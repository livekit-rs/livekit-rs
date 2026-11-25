//! `rtc.batch_io.*` reaches the receiver load-balance threshold.
//!
//! Upstream chain, pinned in the Go oracle:
//! * `pkg/service/roommanager.go:546-548` builds
//!   `participant.Params.MediaBatchIOEnabled` from
//!   `RTC.BatchIO.BatchSize > 0 && (ICEPortRangeStart == 0 || ICEPortRangeEnd == 0) && UDPPort.Valid()`.
//! * `pkg/rtc/mediatrack.go:276-286` turns that boolean into the receiver
//!   load-balance threshold: `300` when set, `20` otherwise.
//! * `pkg/rtc/mediatrack.go:417` passes it to `sfu.WithLoadBalanceThreshold`,
//!   which `pkg/sfu/receiver_base.go:240-246` gives to the down-track spreader.
//!
//! The Rust engine keeps the same quantity at its own broadcast-measurement
//! boundary (`forward_timing.rs`): a received frame costs one write per
//! successful subscriber until the cohort has been written to more subscribers
//! than the threshold allows; past that point Go's spreader hands writes to its
//! worker pool instead of measuring them serially. Both branches are observable
//! through the forwarded-latency sample the collector records.
use super::*;
use str0m::media::ForwardingPacket;

/// One admitted original packet, broadcast to `writes` subscribers.
fn cohort(now: Instant, writes: u32) -> crate::forward_timing::ForwardingAdmissions {
    let mut admissions = crate::forward_timing::ForwardingAdmissions::new(&[ForwardingPacket {
        sequence: 1,
        spatial: None,
        temporal: None,
        extension_index: 0,
        received: now,
        out_of_order: false,
        buffered: false,
    }]);
    for _ in 0..writes.min(4_096) {
        admissions.accepted(0, 0, 0);
    }
    if writes > 4_096 {
        // Broadcast to far more subscribers than either threshold allows; the
        // cap, not this fixture, limits the measured cost.
        admissions.set_write_count(0, writes);
    }
    admissions
}

/// Same physical measurement, only the projected batch-IO condition differs:
/// the default threshold caps the measured broadcast at 20 subscriber writes,
/// the batch-IO threshold lets it run to 300. A wiring that ignores
/// `rtc.media_batch_io_enabled` collapses both legs to one number.
#[test]
fn batch_io_condition_selects_the_receiver_load_balance_threshold() {
    for (batch_io, expected_writes) in [(false, 20u32), (true, 300u32)] {
        let now = Instant::now();
        let mut engine = engine(now);
        engine.cfg.rtc.media_batch_io_enabled = batch_io;
        assert_eq!(
            engine.receiver_lb_threshold(),
            if batch_io {
                crate::forward_timing::LB_THRESHOLD_BATCH_IO
            } else {
                crate::forward_timing::LB_THRESHOLD_DEFAULT
            },
            "upstream mediatrack.go:276-286 threshold for MediaBatchIOEnabled={batch_io}"
        );

        let stats = rtc_core::forward_stats::ForwardStats::new(
            100_000_000,
            100_000_000,
            1_000_000_000,
            now,
        )
        .unwrap();
        engine.cfg.rtc.forward_stats = Some(stats.clone());

        // Each subscriber write costs one virtual millisecond in Go's serial
        // forwarder, so the measured transit of a broadcast cohort is capped by
        // the receiver load-balance threshold. These legs use 45 ms and 250 ms:
        // both are above the default cap of 20, while 250 ms stays inside the
        // batch-IO cap of 300. A leg that never hits its cap cannot tell the
        // two thresholds apart, so the discriminating numbers are asserted here.
        let (small_ms, large_ms) = (45u64, 250u64);
        assert!(small_ms > crate::forward_timing::LB_THRESHOLD_DEFAULT as u64);
        assert!(large_ms < crate::forward_timing::LB_THRESHOLD_BATCH_IO as u64);

        // Broadcast to far more subscribers than either threshold allows: the
        // cap, not this fixture, limits the measurement.
        engine.record_forwarding_cohort(
            &cohort(now, u32::MAX),
            now + Duration::from_millis(small_ms),
            now,
        );
        engine.record_forwarding_cohort(
            &cohort(now, u32::MAX),
            now + Duration::from_millis(large_ms),
            now,
        );
        // A third cohort well inside both caps measures exactly its writes.
        engine.record_forwarding_cohort(&cohort(now, 5), now + Duration::from_millis(5), now);
        // An ineligible cohort (out of order) is filtered like Go's
        // `!extPkt.IsBuffered && !extPkt.IsOutOfOrder` gate.
        engine.record_forwarding_cohort(
            &cohort_out_of_order(now),
            now + Duration::from_secs(9),
            now,
        );

        stats.tick(now + Duration::from_millis(100));
        let snapshot = stats.snapshot();
        assert_eq!(snapshot.histogram_count, 3);
        // The first cohort is capped by the configured threshold; the second
        // (250 ms) only reaches the cap in the default branch and otherwise
        // measures its full delay; the third always measures its 5 writes.
        let cap = if batch_io {
            crate::forward_timing::LB_THRESHOLD_BATCH_IO as u64
        } else {
            crate::forward_timing::LB_THRESHOLD_DEFAULT as u64
        };
        let expected_ms = small_ms.min(cap) + large_ms.min(cap) + 5;
        assert_eq!(
            snapshot.histogram_sum_ns,
            1_000_000.0 * expected_ms as f64,
            "broadcast cost must stop at {cap} subscriber writes for batch_io={batch_io}"
        );
    }
}

/// The same oversized cohort flagged out of order: Go excludes buffered and
/// late arrivals from the forwarding sample (`receiver_base.go:980`).
fn cohort_out_of_order(now: Instant) -> crate::forward_timing::ForwardingAdmissions {
    let mut admissions = crate::forward_timing::ForwardingAdmissions::new(&[ForwardingPacket {
        sequence: 2,
        spatial: None,
        temporal: None,
        extension_index: 0,
        received: now,
        out_of_order: true,
        buffered: false,
    }]);
    admissions.accepted(0, 0, 0);
    admissions
}
