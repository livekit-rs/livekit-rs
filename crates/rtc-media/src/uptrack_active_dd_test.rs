use super::*;
use rtc_core::{
    config::VideoBitrateReport,
    model::{TrackCodec, VideoLayerMode},
};
use std::sync::Arc;
use str0m::{media::Frequency, rtp::ExtensionValues};

#[test]
fn active_vp9_dd_mask_controls_layer_availability_and_reported_cost() {
    let start = Instant::now();
    let interval = Duration::from_millis(125);
    let mut up = svc_tests::track(start);
    up.spec.codecs.push(TrackCodec {
        mime_type: "video/VP9".into(),
        layer_mode: VideoLayerMode::MultipleSpatialLayersPerStream,
        ..Default::default()
    });
    up.configure_bitrate_reports(VideoBitrateReport {
        video: [Some(interval), Some(interval), None],
        ..Default::default()
    });
    let mut parser = crate::av1::PacketReceiver::default();
    let mut observe = |up: &mut UpTrack, sequence: u64, descriptor: &[u8], now: Instant| {
        let mut ext = ExtensionValues::default();
        ext.user_values
            .set(crate::av1::RawDescriptor(Arc::from(descriptor)));
        let parsed = parser.parse(&ext, sequence, now, false).unwrap();
        up.on_av1_packet(
            &parsed,
            MediaTime::new(sequence * 3000, Frequency::NINETY_KHZ),
            100,
            128,
            now,
        );
    };

    observe(&mut up, 1, &[0xc0, 0, 1, 0x80, 0x01, 0xba, 0x20], start);
    let report = start + interval;
    assert!(up.advance_bitrate_reports(report));
    assert_eq!(up.available(report), [true, true, false]);
    let upper_rate = up.bitrates()[1];
    assert_eq!(upper_rate, 8192);
    let declared = [100_000, 200_000, 300_000];
    let mut warm = crate::allocator::LayerBitrateCache::default();
    let mut cold = crate::allocator::LayerBitrateCache::default();
    let check_plan = |phase, bitrates, cap| {
        let tracks = [crate::allocator::AllocTrack {
            priority: crate::allocator::PRIORITY_DEFAULT,
            max_spatial: 2,
            bitrates,
            exempt: false,
        }];
        let plan = crate::allocator::plan(Some(100_000), &tracks, 0, true);
        assert_eq!(plan.caps, [cap]);
        assert_eq!(plan.allocated_bps, 8192);
        assert_eq!(crate::allocator::next_probe_budget(&tracks, &plan), None);
        eprintln!(
            "ACTIVE_DD_ALLOC_RUST phase={phase} cap={cap} bps={}",
            plan.allocated_bps
        );
    };
    let active = warm.resolve(&up.sid, up.bitrate_observation(), declared, report);
    assert_eq!(active, [8192, 8192, 0]);
    check_plan("attached", active, 1);

    let shrunk = report + Duration::from_millis(1);
    observe(&mut up, 2, &[0xc0, 0, 2, 0x42], shrunk);
    assert_eq!(up.available(shrunk), [true, false, false], "mask shrank");
    assert_eq!(up.bitrates()[1], 0, "inactive upper cost must be hidden");
    for cache in [&mut warm, &mut cold] {
        let bitrates = cache.resolve(&up.sid, up.bitrate_observation(), declared, shrunk);
        assert_eq!(bitrates, [8192, 0, 0]);
        check_plan("shrunk", bitrates, 0);
    }

    let restored = shrunk + Duration::from_millis(1);
    observe(&mut up, 3, &[0xc0, 0, 3, 0x46], restored);
    assert_eq!(up.available(restored), [true, true, false], "mask restored");
    assert_eq!(up.bitrates()[1], upper_rate, "restore the completed cost");
    for cache in [&mut warm, &mut cold] {
        let bitrates = cache.resolve(&up.sid, up.bitrate_observation(), declared, restored);
        assert_eq!(
            bitrates, active,
            "a completed window remains eligible after the layer is reactivated"
        );
        check_plan("restored", bitrates, 1);
    }

    let idle = restored + Duration::from_secs(2);
    assert_eq!(
        up.available(idle),
        [true, true, false],
        "DD has no idle timeout"
    );
}
