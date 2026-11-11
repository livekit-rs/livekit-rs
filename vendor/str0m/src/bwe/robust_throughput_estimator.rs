// Copyright (c) 2019 The WebRTC project authors. All Rights Reserved.
// Rust adaptation of Google WebRTC RobustThroughputEstimator at commit
// 202acee882062f7ca632472e4810f25938828f6a, under the BSD-style license
// and patent grant in GOOGLE-WEBRTC-LICENSE / GOOGLE-WEBRTC-PATENTS.
// Only default settings are implemented. All input RTP is TWCC-tracked, so
// Google's prior_unacked_data term is zero; lost records never enter this API.
use std::collections::VecDeque;
use std::time::{Duration, Instant};

use crate::Bitrate;
use crate::rtp_::DataSize;

const WINDOW_PACKETS: usize = 20;
const MAX_WINDOW_PACKETS: usize = 500;
const MIN_WINDOW_DURATION: Duration = Duration::from_secs(1);
const MAX_WINDOW_DURATION: Duration = Duration::from_secs(5);
const REQUIRED_PACKETS: usize = 10;
const MAX_REORDERING: Duration = Duration::from_secs(1);
const MIN_RATE_INTERVAL: Duration = Duration::from_millis(1);

#[derive(Clone, Copy)]
struct Sample {
    send_time: Instant,
    receive_time: Instant,
    size: DataSize,
}

pub(super) struct RobustThroughputEstimator {
    window: VecDeque<Sample>,
    latest_discarded_send_time: Option<Instant>,
}

impl RobustThroughputEstimator {
    pub(super) fn new() -> Self {
        Self {
            window: VecDeque::new(),
            latest_discarded_send_time: None,
        }
    }

    /// Process one real, already-deduplicated TWCC acknowledgment batch.
    /// Native TwccSendRegister::apply_report excludes previously ACKed records;
    /// a previously lost packet enters once if later acknowledged as received.
    /// Ordering and trimming match Google's batch API, not per-packet updates.
    pub(super) fn incoming_feedback(
        &mut self,
        packets: impl Iterator<Item = (Instant, Instant, DataSize)>,
    ) {
        for (send_time, receive_time, size) in packets {
            self.window.push_back(Sample {
                send_time,
                receive_time,
                size,
            });
            let mut index = self.window.len() - 1;
            while index > 0 && self.window[index].receive_time < self.window[index - 1].receive_time
            {
                self.window.swap(index, index - 1);
                index -= 1;
            }
            if self.window.back().is_some_and(|latest| {
                latest.receive_time.saturating_duration_since(receive_time) > MAX_REORDERING
            }) {
                // As upstream, discard the offending packet too. No remote
                // timestamp is compared with a local send timestamp.
                self.window.clear();
                self.latest_discarded_send_time = None;
            }
        }
        // Deliberately trim only after the entire batch has been inserted.
        while self.first_packet_outside_window() {
            let discarded = self.window.pop_front().expect("nonempty window");
            self.latest_discarded_send_time = Some(
                self.latest_discarded_send_time
                    .map_or(discarded.send_time, |last| last.max(discarded.send_time)),
            );
        }
    }

    fn first_packet_outside_window(&self) -> bool {
        let (Some(first), Some(last)) = (self.window.front(), self.window.back()) else {
            return false;
        };
        let duration = last.receive_time.duration_since(first.receive_time);
        self.window.len() > MAX_WINDOW_PACKETS
            || duration > MAX_WINDOW_DURATION
            || (self.window.len() > WINDOW_PACKETS && duration > MIN_WINDOW_DURATION)
    }

    pub(super) fn current_estimate(&self) -> Option<Bitrate> {
        if self.window.len() < REQUIRED_PACKETS {
            return None;
        }
        let mut largest_gap = Duration::ZERO;
        let mut second_largest_gap = Duration::ZERO;
        for index in 1..self.window.len() {
            let gap = self.window[index]
                .receive_time
                .duration_since(self.window[index - 1].receive_time);
            if gap > largest_gap {
                second_largest_gap = largest_gap;
                largest_gap = gap;
            } else if gap > second_largest_gap {
                second_largest_gap = gap;
            }
        }
        let mut first_send = None;
        let mut last_send = None;
        let mut last_send_bytes = 0.0;
        let mut send_bytes = 0.0;
        let mut receive_bytes = 0.0;
        let mut valid_send_packets = 0;
        for packet in &self.window {
            let bytes = packet.size.as_bytes_i64() as f64;
            receive_bytes += bytes;
            if self
                .latest_discarded_send_time
                .is_some_and(|discarded| packet.send_time < discarded)
            {
                continue;
            }
            if last_send.is_none_or(|last| packet.send_time > last) {
                last_send = Some(packet.send_time);
                last_send_bytes = bytes;
            }
            first_send = Some(first_send.map_or(packet.send_time, |first: Instant| {
                first.min(packet.send_time)
            }));
            send_bytes += bytes;
            valid_send_packets += 1;
        }
        // Bounds above guarantee these endpoints exist. All receive timestamps
        // are remote-clock values, used only with that same clock domain.
        let first = self.window.front()?;
        let last = self.window.back()?;
        receive_bytes -= first.size.as_bytes_i64() as f64;
        send_bytes -= last_send_bytes;
        let receive_duration = (last.receive_time.duration_since(first.receive_time) - largest_gap
            + second_largest_gap)
            .max(MIN_RATE_INTERVAL);
        let receive_bps = receive_bytes * 8.0 / receive_duration.as_secs_f64();
        if valid_send_packets < REQUIRED_PACKETS {
            return Some(receive_bps.into());
        }
        let send_duration = last_send?
            .duration_since(first_send?)
            .max(MIN_RATE_INTERVAL);
        let send_bps = send_bytes * 8.0 / send_duration.as_secs_f64();
        Some(send_bps.min(receive_bps).into())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn packet(
        at: Instant,
        send_ms: u64,
        receive_ms: u64,
        bytes: i64,
    ) -> (Instant, Instant, DataSize) {
        (
            at + Duration::from_millis(send_ms),
            at + Duration::from_millis(receive_ms),
            DataSize::bytes(bytes),
        )
    }

    fn rate(estimator: &RobustThroughputEstimator) -> f64 {
        estimator.current_estimate().unwrap().as_f64()
    }

    fn close(a: f64, b: f64) {
        assert!((a - b).abs() < 0.001, "{a} != {b}");
    }

    #[test]
    fn default_window_and_required_packet_boundaries_match_upstream() {
        let now = Instant::now();
        let mut e = RobustThroughputEstimator::new();
        e.incoming_feedback((0..9).map(|i| packet(now, i * 10, i * 10, 1000)));
        assert!(e.current_estimate().is_none());
        e.incoming_feedback(std::iter::once(packet(now, 90, 90, 1000)));
        close(rate(&e), 800_000.0);
        let mut boundary = RobustThroughputEstimator::new();
        boundary.incoming_feedback((0..21).map(|i| packet(now, i * 50, i * 50, 1000)));
        assert_eq!(
            boundary.window.len(),
            21,
            "strictly greater than one second is required to trim"
        );
        boundary.incoming_feedback(std::iter::once(packet(now, 1051, 1051, 1000)));
        assert_eq!(boundary.window.len(), 20);
        assert!(boundary.latest_discarded_send_time.is_some());
    }

    #[test]
    fn packet_and_maximum_time_bounds_are_enforced_after_a_batch() {
        let now = Instant::now();
        let mut e = RobustThroughputEstimator::new();
        e.incoming_feedback((0..600).map(|i| packet(now, i, i, 1000)));
        assert_eq!(e.window.len(), MAX_WINDOW_PACKETS);
        let mut sparse = RobustThroughputEstimator::new();
        sparse.incoming_feedback((0..11).map(|i| packet(now, i * 500, i * 500, 1000)));
        assert_eq!(sparse.window.len(), 11, "five-second boundary retained");
        sparse.incoming_feedback(std::iter::once(packet(now, 5001, 5001, 1000)));
        assert_eq!(sparse.window.len(), 11);
        assert_eq!(
            sparse.window.front().unwrap().receive_time,
            now + Duration::from_millis(500)
        );
    }

    #[test]
    fn receive_gap_replacement_cannot_exceed_observed_send_rate() {
        let now = Instant::now();
        let mut e = RobustThroughputEstimator::new();
        e.incoming_feedback(
            (0..20).map(|i| packet(now, i * 10, i * 10 + if i >= 10 { 100 } else { 0 }, 1000)),
        );
        close(rate(&e), 800_000.0);
        let mut receive_limited = RobustThroughputEstimator::new();
        receive_limited.incoming_feedback((0..20).map(|i| packet(now, i * 5, i * 10, 1000)));
        close(rate(&receive_limited), 800_000.0);
    }

    #[test]
    fn timestamp_end_corrections_apply_to_variable_size_packets() {
        let now = Instant::now();
        let mut e = RobustThroughputEstimator::new();
        e.incoming_feedback((0..10).map(|i| {
            packet(
                now,
                i * 10,
                i * 10,
                if i == 0 {
                    2000
                } else if i == 9 {
                    500
                } else {
                    1000
                },
            )
        }));
        close(rate(&e), 8500.0 * 8.0 / 0.09);
    }

    #[test]
    fn moderate_reordering_is_sorted_but_severe_reordering_clears_old_clock_domain() {
        let now = Instant::now();
        let mut e = RobustThroughputEstimator::new();
        e.incoming_feedback((10..20).map(|i| packet(now, i * 10, i * 10, 1000)));
        e.incoming_feedback((0..10).map(|i| packet(now, i * 10, i * 10, 1000)));
        close(rate(&e), 800_000.0);
        e.incoming_feedback(std::iter::once(packet(now, 2000, 2000, 1000)));
        e.incoming_feedback(std::iter::once(packet(now, 0, 0, 1000)));
        assert!(
            e.window.is_empty(),
            "offending reordered packet must not be retained"
        );
        assert!(e.latest_discarded_send_time.is_none());
        assert!(e.current_estimate().is_none());
        e.incoming_feedback((0..10).map(|i| packet(now, 2100 + i * 10, i * 10, 1000)));
        close(rate(&e), 800_000.0);
    }

    #[test]
    fn repeated_receive_timestamps_and_local_send_clock_origin_remain_bounded() {
        let now = Instant::now();
        let mut e = RobustThroughputEstimator::new();
        e.incoming_feedback((0..10).map(|i| packet(now, 100_000 + i * 10, 100, 1000)));
        close(rate(&e), 800_000.0);
    }

    #[test]
    fn old_send_samples_after_window_discard_use_only_receive_rate_when_fewer_than_ten_remain() {
        let now = Instant::now();
        let mut e = RobustThroughputEstimator::new();
        e.incoming_feedback((0..21).map(|i| packet(now, 1000 + i * 100, i * 100, 1000)));
        assert!(e.latest_discarded_send_time.is_some());
        e.incoming_feedback((21..41).map(|i| packet(now, 0, i * 100, 1000)));
        assert_eq!(e.window.len(), 20);
        assert!(
            e.window
                .iter()
                .all(|p| Some(p.send_time) < e.latest_discarded_send_time)
        );
        close(rate(&e), 80_000.0);
    }

    #[test]
    fn sustained_rate_drop_is_not_hidden_by_single_gap_suppression() {
        let now = Instant::now();
        let mut e = RobustThroughputEstimator::new();
        e.incoming_feedback((0..100).map(|i| packet(now, i * 10, i * 10, 1000)));
        close(rate(&e), 800_000.0);
        e.incoming_feedback((100..200).map(|i| packet(now, i * 100, i * 100, 1000)));
        close(rate(&e), 80_000.0);
    }
    #[test]
    fn severe_clock_change_continues_with_the_remainder_of_the_same_batch() {
        let now = Instant::now();
        let mut e = RobustThroughputEstimator::new();
        e.incoming_feedback((0..20).map(|i| packet(now, 3000 + i * 10, 3000 + i * 10, 1000)));
        e.incoming_feedback((0..20).map(|i| packet(now, 3300 + i * 10, i * 10, 1000)));
        assert_eq!(
            e.window.len(),
            19,
            "only the triggering packet is discarded"
        );
        close(rate(&e), 800_000.0);
    }

    #[test]
    fn long_silence_requires_a_new_ten_packet_window_and_does_not_apply_a_floor() {
        let now = Instant::now();
        let mut e = RobustThroughputEstimator::new();
        e.incoming_feedback((0..20).map(|i| packet(now, i * 10, i * 10, 1000)));
        close(rate(&e), 800_000.0);
        e.incoming_feedback((0..5).map(|i| packet(now, 60_000 + i * 100, 60_000 + i * 100, 100)));
        assert_eq!(e.window.len(), 5);
        assert!(e.current_estimate().is_none());
        e.incoming_feedback((5..10).map(|i| packet(now, 60_000 + i * 100, 60_000 + i * 100, 100)));
        close(rate(&e), 8_000.0);
    }

    #[test]
    fn a_large_batch_is_inserted_before_its_single_trim_pass() {
        let now = Instant::now();
        let mut e = RobustThroughputEstimator::new();
        let mut batch: Vec<_> = (0..30).map(|i| packet(now, i * 50, i * 50, 1000)).collect();
        // This within-one-second reordered sample remains inside the complete
        // batch before the original send-time discard watermark is advanced.
        batch.push(packet(now, 5000, 1000, 1000));
        e.incoming_feedback(batch.into_iter());
        assert_eq!(e.window.len(), 22);
        assert!(
            e.window
                .iter()
                .zip(e.window.iter().skip(1))
                .all(|(a, b)| a.receive_time <= b.receive_time)
        );
        assert!(e.latest_discarded_send_time.unwrap() < now + Duration::from_millis(5000));
        assert!(rate(&e).is_finite());
    }
    #[test]
    fn missing_acknowledgments_are_not_counted_as_received_bytes() {
        let now = Instant::now();
        let mut e = RobustThroughputEstimator::new();
        e.incoming_feedback(std::iter::empty());
        assert!(e.current_estimate().is_none());
        // Production excludes lost records before this API. Every other packet
        // of an 800kbps send stream is absent, so actual ACK throughput is400kbps.
        e.incoming_feedback(
            (0..40)
                .step_by(2)
                .map(|i| packet(now, i * 10, i * 10, 1000)),
        );
        close(rate(&e), 400_000.0);
    }
    #[test]
    fn measured_audio_probe_video_step_converges_to_actual_video_rate() {
        let now = Instant::now();
        let mut e = RobustThroughputEstimator::new();
        e.incoming_feedback(std::iter::empty());
        assert!(
            e.current_estimate().is_none(),
            "no ACK means no invented estimate"
        );
        // Synthetic acknowledged packet timing only, not desired-rate inputs:
        // 120 bytes / 20ms =48kbps audio; fixed10ms one-way receive offset.
        e.incoming_feedback((0..50).map(|i| packet(now, i * 20, i * 20 + 10, 120)));
        close(rate(&e), 48_000.0);
        e.incoming_feedback(std::iter::empty());
        close(rate(&e), 48_000.0);
        // Twenty actually acknowledged1000-byte probe packets spaced2ms=4Mbps.
        // The ACK estimator still measures its mixed window, not requested4Mbps.
        e.incoming_feedback((0..20).map(|i| packet(now, 1000 + i * 2, 1010 + i * 2, 1000)));
        assert!(rate(&e) > 48_000.0 && rate(&e) <= 4_000_000.0);
        // 1125bytes / 5ms =1.8Mbps video. After a complete video-only window,
        // old audio/probe packets must not pin throughput to the audio history.
        for i in 0..220 {
            e.incoming_feedback(std::iter::once(packet(
                now,
                1040 + i * 5,
                1050 + i * 5,
                1125,
            )));
        }
        assert!(e.window.iter().all(|p| p.size == DataSize::bytes(1125)));
        close(rate(&e), 1_800_000.0);
        e.incoming_feedback(std::iter::empty());
        close(rate(&e), 1_800_000.0);
    }
}
