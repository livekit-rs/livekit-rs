//! Exact vendored pacing source, including its original private tests.
//! The network scenario is a synthetic wire/token-bucket model, not SRTP or
//! browser decoding. Its latency gate supplements unchanged SDK acceptance.
#![allow(dead_code, unused_imports)]
#[macro_use]
extern crate tracing;
#[macro_export]
macro_rules! log_stat {
    ($($arg:tt)*) => {};
}
#[allow(unexpected_cfgs)]
#[path = "../../../vendor/str0m/src/rtp/id.rs"]
mod native_ids;
mod rtp_ {
    pub(crate) use crate::native_ids::{Mid, MidRid, TwccClusterId};
    pub use str0m::rtp::RtpHeader;
    pub use str0m_proto::{Bitrate, DataSize};
}
#[allow(unused_macros)]
#[path = "../../../vendor/str0m/src/bwe/macros.rs"]
mod native_bwe_macros;
#[path = "../../../vendor/str0m/src/bwe/probe/cluster.rs"]
mod native_cluster;
#[path = "../../../vendor/str0m/src/util/mod.rs"]
mod util;
mod bwe_ {
    pub(crate) use crate::native_bwe_macros::{log_pacer_media_debt, log_pacer_padding_debt};
    pub(crate) use crate::native_cluster::{ProbeClusterConfig, ProbeClusterState, ProbeKind};
}
mod bwe {
    pub(crate) use crate::native_cluster::ProbeKind;
}
#[derive(Default)]
enum Reason {
    #[default]
    NotHappening,
    Pacer(pacer::PacerReason),
}
// Keep the actual upstream module tree: original leaky/null/control/unit tests
// execute unchanged and are reported separately from the new latency scenario.
#[allow(clippy::collapsible_if)]
#[path = "../../../vendor/str0m/src/pacer/mod.rs"]
mod pacer;

use pacer::{Pacer, PacerControl, PacerImpl, QueuePriority, QueueSnapshot, QueueState};
use rtp_::{Bitrate, DataSize, MidRid};
use std::collections::VecDeque;
use std::time::{Duration, Instant};

#[test]
fn normal_media_pacer_stays_within_fixed_four_megabit_queue_budget() {
    for flush_phase_ms in [0, 4] {
        let start = Instant::now();
        let rate = PacerControl::new()
            .calculate(true, Bitrate::mbps(2), false)
            .pacing_rate;
        assert_eq!(
            rate,
            Bitrate::bps(2_200_000),
            "actual production factor is1.1"
        );
        let mut pacer = PacerImpl::leaky_bucket(rate);
        pacer.set_padding_rate(Bitrate::ZERO);
        let midrid = MidRid("media".into(), None);
        let mut queued: VecDeque<Instant> = VecDeque::new();
        let mut network: VecDeque<(Instant, usize)> = VecDeque::new();
        let mut next_frame_us = 0;
        let mut sent = 0;
        let mut sent_bytes = 0;
        let mut forwarded = 0;
        let mut max_delay = Duration::ZERO;
        let mut max_network_bytes = 0;
        let mut max_media_age = Duration::ZERO;
        let mut tokens = 4500.0f64;
        let mut last_flush_us = 0;
        let payload = 1200usize;
        // Explicit synthetic datagram layout:1200 payload+24 RTP/extensions
        // +16 SRTP+28 IPv4/UDP. Not inferred from a desired rate or fake ACKs.
        let datagram = payload + 24 + 16 + 28;
        for tick_us in (0..2_000_000u64).step_by(1000) {
            let now = start + Duration::from_micros(tick_us);
            if tick_us >= next_frame_us {
                // Deterministic84KB keyframe-sized initial queue, then identical
                // 7.2KB interframe-sized queues every33.333ms in both variants.
                let count = if next_frame_us == 0 { 70 } else { 6 };
                queued.extend(std::iter::repeat_n(now, count));
                next_frame_us += 33_333;
            }
            for _ in 0..16 {
                let snapshot = QueueSnapshot {
                    created_at: now,
                    byte_size: queued.len() * payload,
                    packet_count: queued.len() as u32,
                    total_queue_time_origin: queued.iter().map(|at| now.duration_since(*at)).sum(),
                    first_unsent: queued.front().copied(),
                    priority: if queued.is_empty() {
                        QueuePriority::Empty
                    } else {
                        QueuePriority::Media
                    },
                    ..Default::default()
                };
                assert!(
                    pacer
                        .handle_timeout(
                            now,
                            std::iter::once(QueueState {
                                midrid,
                                unpaced: false,
                                use_for_padding: false,
                                snapshot,
                            })
                        )
                        .is_none()
                );
                if let Some((selected, cluster)) = pacer.poll_queue() {
                    assert_eq!(selected, midrid);
                    assert!(cluster.is_none(), "this isolates normal pacing, not probes");
                    let queued_at = queued.pop_front().expect("actual queued payload");
                    max_media_age = max_media_age.max(now.duration_since(queued_at));
                    pacer.register_send(now, DataSize::bytes(payload.try_into().unwrap()), midrid);
                    sent += 1;
                    sent_bytes += payload;
                    network.push_back((now, datagram));
                    max_network_bytes =
                        max_network_bytes.max(network.iter().map(|p| p.1).sum::<usize>());
                }
                if pacer.poll_timeout().0.is_none_or(|at| at > now) {
                    break;
                }
            }
            if tick_us / 1000 >= flush_phase_ms && (tick_us / 1000 - flush_phase_ms) % 5 == 0 {
                tokens = (tokens + (tick_us - last_flush_us) as f64 * 0.5).min(4500.0);
                last_flush_us = tick_us;
                while network.front().is_some_and(|p| p.1 as f64 <= tokens) {
                    let (sent_at, bytes) = network.pop_front().unwrap();
                    tokens -= bytes as f64;
                    max_delay = max_delay.max(now.duration_since(sent_at));
                    forwarded += 1;
                }
            }
        }
        eprintln!(
            "actual native pacer synthetic4M/4500B/5ms-flush model: phase={flush_phase_ms}ms sent={sent} payloadBytes={sent_bytes} forwarded={forwarded} peakNetworkB={max_network_bytes} maxNetworkDelay={max_delay:?} maxMediaQueueAge={max_media_age:?}"
        );
        assert!(
            sent > 100 && forwarded > 100,
            "a silent pacer cannot pass latency"
        );
        assert!(
            sent_bytes >= 400_000,
            "do not improve delay by suppressing throughput"
        );
        assert!(
            max_media_age < Duration::from_millis(500),
            "bounded real queue age"
        );
        assert!(
            max_delay <= Duration::from_millis(5),
            "new low-latency subscenario failed with real pacer: {max_delay:?}; original browser acceptance is independent"
        );
    }
}

fn same_clock_packet_burst(rate: Bitrate, payload: usize, unpaced: bool, rounds: usize) -> usize {
    let start = Instant::now();
    let midrid = MidRid("envelope".into(), None);
    let mut pacer = PacerImpl::leaky_bucket(rate);
    // Avoid accidentally testing queue-drain acceleration at the low rates.
    // One whole packet is still required when the2s budget is sub-packet.
    let mut left = if unpaced {
        500
    } else {
        ((rate * Duration::from_secs(2)).as_bytes_usize() / payload).clamp(1, 500)
    };
    let mut sent = 0;
    for _ in 0..rounds {
        let snapshot = QueueSnapshot {
            created_at: start,
            byte_size: left * payload,
            packet_count: left as u32,
            first_unsent: Some(start),
            priority: QueuePriority::Media,
            ..Default::default()
        };
        pacer.handle_timeout(
            start,
            std::iter::once(QueueState {
                midrid,
                unpaced,
                use_for_padding: false,
                snapshot,
            }),
        );
        let Some((_, cluster)) = pacer.poll_queue() else {
            break;
        };
        assert!(cluster.is_none());
        pacer.register_send(start, DataSize::bytes(payload.try_into().unwrap()), midrid);
        sent += payload;
        left -= 1;
    }
    sent
}

#[test]
fn normal_packet_envelope_bounds_low_rate_and_high_rate_bursts() {
    for rate in [
        Bitrate::bps(1000),
        Bitrate::kbps(10),
        Bitrate::mbps(2),
        Bitrate::mbps(20),
        Bitrate::mbps(200),
    ] {
        let bytes = same_clock_packet_burst(rate, 1200, false, 500);
        let allowance = (rate * Duration::from_millis(5)).min(DataSize::bytes(63_000));
        assert!(bytes > 0, "a packet may overshoot a sub-packet credit");
        assert!(
            DataSize::bytes(bytes.try_into().unwrap()) <= allowance + DataSize::bytes(1200),
            "normal burst must be bounded by5ms or63kB plus one actual packet: {rate:?}/{bytes}"
        );
    }
}

#[test]
fn unpaced_audio_and_high_pps_preserve_bounded_driver_fairness() {
    // Audio priority is intentionally independent of paced video debt. The
    // caller's actual16-round fairness limit still bounds work at one clock.
    assert_eq!(
        same_clock_packet_burst(Bitrate::bps(1000), 1200, true, 16),
        16 * 1200
    );
    let small_packets = same_clock_packet_burst(Bitrate::mbps(20), 16, false, 16);
    assert_eq!(small_packets, 16 * 16);
    assert!(
        DataSize::bytes(small_packets.try_into().unwrap())
            <= Bitrate::mbps(20) * Duration::from_millis(5)
    );
}
