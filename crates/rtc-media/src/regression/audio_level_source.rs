//! Real bound native-frame -> upstream audio-level detector -> engine event.
//! Frames are supplied at the existing post-depacketizer consumer boundary;
//! this is not an authenticated SRTP or official SDK interop claim.
use super::*;
use rtc_core::media_api::{MediaCommand, PublishedTrackSpec};
use rtc_core::model::{TrackKind, TrackSource};
use str0m::format::{Codec, CodecExtra};
use str0m::media::{Frequency, MediaData, MediaTime};

#[test]
fn source_audio_rtp_first_sample_has_zero_age_and_short_windows_emit_unclipped_levels() {
    for interval_ms in [10_u64, 40] {
        let now = Instant::now();
        let mut engine = engine_with_audio_interval(now, Duration::from_millis(interval_ms));
        let tid = transport(&mut engine, PcRole::Publisher, now);
        let sid = TrackSid::from("TR_audio_source");
        engine.handle_command(
            MediaCommand::ExpectTrack {
                transport: tid,
                track: PublishedTrackSpec {
                    sid: sid.clone(),
                    kind: TrackKind::Audio,
                    source: TrackSource::Microphone,
                    cid: "source-audio".into(),
                    backup_codec_policy: Default::default(),
                    layers: vec![],
                    codecs: vec![],
                    stereo: false,
                    disable_dtx: false,
                    disable_red: true,
                    encrypted: false,
                    muted: false,
                },
            },
            now,
        );
        let mut peer = client(now);
        let events = offer(&mut engine, &mut peer, tid, now, |api| {
            api.add_media(MediaKind::Audio, Direction::SendOnly, None, None, None);
        });
        let mid = published_mid(&events);
        engine.handle_command(
            MediaCommand::BindTrack {
                transport: tid,
                mid: mid.to_string(),
                track_sid: sid.clone(),
            },
            now,
        );
        let params = engine.transports[&tid]
            .rtc
            .codec_config()
            .params()
            .iter()
            .find(|param| param.spec().codec == Codec::Opus)
            .copied()
            .unwrap();
        let make_frame = |sequence: u64, rtp_time: u64, at: Instant| {
            let mut extensions = str0m::rtp::ExtensionValues::default();
            extensions.audio_level = Some(0); // RFC6464 zero dBov.
            MediaData {
                mid,
                pt: params.pt(),
                rid: None,
                params,
                time: MediaTime::new(rtp_time, Frequency::FORTY_EIGHT_KHZ),
                network_time: at,
                seq_range: sequence.into()..=sequence.into(),
                contiguous: true,
                data: vec![0xf8, 0xff, 0xfe].into(),
                ext_vals: extensions,
                codec_extra: CodecExtra::None,
                last_sender_info: None,
                audio_start_of_talk_spurt: false,
            }
        };
        let observed = |engine: &mut MediaEngine, at: Instant| {
            engine.events.clear();
            engine.emit_audio_levels(at);
            let levels = engine
                .events
                .iter()
                .find_map(|event| match event {
                    MediaEvent::AudioLevels(levels) => Some(levels),
                    _ => None,
                })
                .expect("bound microphone must emit actual engine AudioLevels");
            assert_eq!(levels.len(), 1);
            (levels[0].level, levels[0].active)
        };
        let mut absent_extension = make_frame(0, 47_040, now);
        absent_extension.ext_vals.audio_level = None;
        engine.on_media_data(tid, absent_extension, now);
        assert_eq!(observed(&mut engine, now), (0.0, false));
        engine.on_media_data(tid, make_frame(1, 48_000, now), now);
        assert_eq!(
            observed(&mut engine, now),
            (0.0, false),
            "first RTP timestamp establishes the baseline, not an invented 20ms"
        );
        let second = now + Duration::from_millis(20);
        engine.on_media_data(tid, make_frame(2, 48_960, second), second);
        let last_at = if interval_ms == 10 {
            let (level, active) = observed(&mut engine, second);
            assert!(active);
            assert!(
                (level - 4.0 / 3.0).abs() < 1e-6,
                "Go returns real activity-weighted EMA above one: {level}"
            );
            second
        } else {
            assert_eq!(observed(&mut engine, second), (0.0, false));
            let third = now + Duration::from_millis(40);
            engine.on_media_data(tid, make_frame(3, 49_920, third), third);
            let (level, active) = observed(&mut engine, third);
            assert!(active);
            assert!((level - 2.0 / 3.0).abs() < 1e-6);
            third
        };
        let previous = observed(&mut engine, last_at);
        // Neither duplicate nor backwards timestamps may spend the same audio
        // duration twice or update its freshness.
        let timestamp = if interval_ms == 10 { 48_960 } else { 49_920 };
        engine.on_media_data(tid, make_frame(4, timestamp, last_at), last_at);
        engine.on_media_data(tid, make_frame(5, 48_000, last_at), last_at);
        assert_eq!(observed(&mut engine, last_at), previous);
        let stale = last_at + Duration::from_millis(2 * interval_ms);
        assert_eq!(observed(&mut engine, stale), (0.0, false));
        engine.handle_command(MediaCommand::CloseTransport(tid), stale);
        assert!(engine.transports.is_empty());
        assert!(
            !engine
                .up_by_mid
                .keys()
                .any(|(transport, _)| *transport == tid)
        );
        // The actual room owns publication lifetime separately from its PC.
        engine.handle_command(MediaCommand::UnpublishTrack { track_sid: sid }, stale);
        assert_eq!(engine.ups.len(), 0);
    }
}
