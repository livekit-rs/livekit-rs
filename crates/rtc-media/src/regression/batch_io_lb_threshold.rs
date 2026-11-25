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
//! boundary (`forward_timing.rs`): a cohort accumulates one write per
//! successful subscriber per admitted original packet, and once the running
//! total passes the threshold the remaining packets are attributed to Go's
//! spreader worker pool instead of this call's serial path, so they lose their
//! forwarding-latency sample. The threshold is therefore observable as the
//! *number of samples* a cohort produces, not as their size — the packet whose
//! own write count overshoots the budget is the discriminator, which is why the
//! fixture leads with one 45-subscriber broadcast.
use super::*;
use str0m::media::ForwardingPacket;

/// Six admitted originals, each with its own 1 ms transit, the first broadcast
/// to `first_writes` subscribers and the rest to one subscriber each.
fn cohort(now: Instant, first_writes: u32) -> crate::forward_timing::ForwardingAdmissions {
    let items: Vec<ForwardingPacket> = (0..6u32)
        .map(|index| ForwardingPacket {
            sequence: u64::from(index) + 1,
            spatial: None,
            temporal: None,
            extension_index: 0,
            received: now - Duration::from_millis(1),
            out_of_order: false,
            buffered: false,
        })
        .collect();
    let mut admissions = crate::forward_timing::ForwardingAdmissions::new(&items);
    // One `accepted` call is one subscriber that received every packet.
    admissions.accepted(0, 0, 0);
    // The leading packet was also written to `first_writes - 1` further
    // subscribers; a broadcast this wide is the fixture's physical shape, not a
    // loop a regression suite should run.
    admissions.set_write_count(0, first_writes);
    admissions
}

/// Same physical measurement, only the projected batch-IO condition differs.
/// The leading broadcast is 45 writes: above the default threshold of 20, so
/// the serial measurement stops there and only that packet is sampled; inside
/// the batch-IO threshold of 300, so all six packets are sampled. A wiring that
/// ignores `rtc.media_batch_io_enabled` gives both legs the batch-IO count.
#[test]
fn batch_io_condition_selects_the_receiver_load_balance_threshold() {
    for (batch_io, expected_samples) in [(false, 1u64), (true, 6u64)] {
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
        // The discriminating shape: the leading broadcast sits between the two
        // thresholds, so it overshoots one budget and not the other.
        assert!(45 > crate::forward_timing::LB_THRESHOLD_DEFAULT as u32);
        assert!(45 < crate::forward_timing::LB_THRESHOLD_BATCH_IO as u32);

        let stats = rtc_core::forward_stats::ForwardStats::new(
            100_000_000,
            100_000_000,
            1_000_000_000,
            now,
        )
        .unwrap();
        engine.cfg.rtc.forward_stats = Some(stats.clone());

        engine.record_forwarding_cohort(&cohort(now, 45), now, now);
        stats.tick(now + Duration::from_millis(100));
        let snapshot = stats.snapshot();
        assert_eq!(
            snapshot.histogram_count, expected_samples,
            "a 45-subscriber leading broadcast must cost {expected_samples} \
             forwarding samples for batch_io={batch_io}"
        );
        assert_eq!(
            snapshot.histogram_sum_ns,
            1_000_000.0 * expected_samples as f64,
            "each sampled packet carries its own 1 ms transit for batch_io={batch_io}"
        );
    }
}

/// A cohort whose every packet stays inside both budgets samples all of them,
/// so the threshold is a cap and not a floor: this leg keeps the discriminating
/// one from passing on an always-truncating wiring.
#[test]
fn a_small_broadcast_is_sampled_whole_under_either_threshold() {
    for batch_io in [false, true] {
        let now = Instant::now();
        let mut engine = engine(now);
        engine.cfg.rtc.media_batch_io_enabled = batch_io;
        let stats = rtc_core::forward_stats::ForwardStats::new(
            100_000_000,
            100_000_000,
            1_000_000_000,
            now,
        )
        .unwrap();
        engine.cfg.rtc.forward_stats = Some(stats.clone());
        // 2 subscribers x 6 packets = 12 writes, below the 20 budget.
        engine.record_forwarding_cohort(&cohort(now, 2), now, now);
        stats.tick(now + Duration::from_millis(100));
        assert_eq!(
            stats.snapshot().histogram_count,
            6,
            "batch_io={batch_io} cannot drop samples from a cohort that stays \
             inside its own threshold"
        );
    }
}
