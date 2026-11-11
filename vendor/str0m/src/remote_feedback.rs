//! Output-side observations for receiver/interceptor policy. Never a capacity estimate.
use crate::rtp_::Mid;
use std::collections::{HashMap, VecDeque};
use std::time::{Duration, Instant};

/// Actual locally emitted RTP counters for one negotiated sender.
#[derive(Clone, Copy, Debug)]
pub struct RemoteFeedbackStats {
    /// Negotiated media identity. The application must still select its video tracks.
    pub mid: Mid,
    /// Actual primary SSRC of this local sender.
    pub ssrc: u32,
    /// Negotiated retransmission SSRC, when configured.
    pub rtx_ssrc: Option<u32>,
    /// Local sender reset generation, preventing counter/sequence aliasing.
    pub generation: u64,
    /// Cumulative primary non-padding packets (wrapping like Go uint32).
    pub packets_primary: u32,
    /// Cumulative accepted repeat packet-NACK requests, not RTCP message count.
    pub repeated_nacks: u32,
    /// Actual output primary RTP bytes, including RTP header, excluding SRTP tag.
    pub bytes_primary: u64,
    /// Actual output non-padding RTX RTP bytes, including RTP header/OSN.
    pub bytes_rtx: u64,
    /// Actual output non-padding RTX packet count.
    pub packets_rtx: u64,
    /// Actual output padding RTP bytes, excluding SRTP tag.
    pub bytes_padding: u64,
    /// Actual output padding packets.
    pub packets_padding: u64,
    /// Last actual local RTP output; None until output exists.
    pub last_output_at: Option<Instant>,
}
#[derive(Debug)]
struct Entry {
    last_nack: Instant,
    count: u8,
}
#[derive(Debug)]
pub(crate) struct RemoteFeedbackMeter {
    pub stats: RemoteFeedbackStats,
    sent: HashMap<u64, Entry>,
    order: VecDeque<u64>,
    capacity: usize,
}
impl RemoteFeedbackMeter {
    pub fn new(mid: Mid, capacity: usize) -> Self {
        Self {
            stats: RemoteFeedbackStats {
                mid,
                ssrc: 0,
                rtx_ssrc: None,
                generation: 0,
                packets_primary: 0,
                repeated_nacks: 0,
                bytes_primary: 0,
                bytes_rtx: 0,
                packets_rtx: 0,
                bytes_padding: 0,
                packets_padding: 0,
                last_output_at: None,
            },
            sent: HashMap::new(),
            order: VecDeque::new(),
            capacity: capacity.max(1),
        }
    }
    pub fn sent(&mut self, seq: u64, bytes: usize, primary: bool, padding: bool, now: Instant) {
        self.stats.last_output_at = Some(now);
        if padding {
            self.stats.bytes_padding += bytes as u64;
            self.stats.packets_padding += 1;
        } else if primary {
            self.stats.packets_primary = self.stats.packets_primary.wrapping_add(1);
            self.stats.bytes_primary += bytes as u64;
            // Extended sequence identity, not uint16 aliasing across wrap.
            if self.sent.contains_key(&seq) {
                return;
            }
            while self.order.len() >= self.capacity {
                if let Some(old) = self.order.pop_front() {
                    self.sent.remove(&old);
                }
            }
            self.order.push_back(seq);
            self.sent.insert(
                seq,
                Entry {
                    last_nack: now,
                    count: 0,
                },
            );
        } else {
            self.stats.bytes_rtx += bytes as u64;
            self.stats.packets_rtx += 1;
        }
    }
    // Fixed Go sequencer.go:385 + downtrack.go:2489. Call only after actual
    // retransmission-cache lookup succeeded; don't count NACK message length.
    pub fn nack(&mut self, seq: u64, rtt: Duration, now: Instant) -> bool {
        let Some(highest) = self.order.back().copied() else {
            return false;
        };
        if seq > highest || highest - seq >= self.capacity as u64 {
            return false;
        }
        let Some(entry) = self.sent.get_mut(&seq) else {
            return false;
        };
        let minimum_ms = 100_u128.min(rtt.as_millis().saturating_mul(2));
        let Some(elapsed) = now.checked_duration_since(entry.last_nack) else {
            return false;
        };
        if entry.count < 3 && elapsed.as_millis() > minimum_ms {
            entry.count += 1;
            entry.last_nack = now;
            if entry.count > 1 {
                self.stats.repeated_nacks = self.stats.repeated_nacks.wrapping_add(1);
            }
            return true;
        }
        false
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn repeated_packet_not_message_counter_strict_boundary_and_bounded_generation() {
        let now = Instant::now();
        let mid = Mid::new();
        let mut m = RemoteFeedbackMeter::new(mid, 2);
        m.sent(65535, 100, true, false, now);
        for ms in [100, 101, 201, 202, 303, 404] {
            m.nack(
                65535,
                Duration::from_millis(70),
                now + Duration::from_millis(ms),
            );
        }
        assert_eq!(m.stats.repeated_nacks, 2); // first accepted101; repeat202,303; max3.
        m.sent(65536, 100, true, false, now);
        m.sent(65537, 100, true, false, now);
        m.nack(65535, Duration::ZERO, now + Duration::from_secs(2));
        assert_eq!(m.stats.repeated_nacks, 2); // evicted, never resurrected.
        assert_eq!(m.sent.len(), 2);
    }
    #[test]
    fn actual_primary_rtx_padding_are_separate_and_wrapping_is_source_semantic() {
        let now = Instant::now();
        let mut m = RemoteFeedbackMeter::new(Mid::new(), 2);
        m.stats.packets_primary = u32::MAX;
        m.stats.repeated_nacks = u32::MAX;
        m.sent(1, 100, true, false, now);
        m.sent(2, 110, false, false, now);
        m.sent(3, 80, false, true, now);
        assert_eq!(m.stats.packets_primary, 0);
        assert_eq!(m.stats.bytes_primary, 100);
        assert_eq!(m.stats.bytes_rtx, 110);
        assert_eq!(m.stats.bytes_padding, 80);
        m.nack(1, Duration::ZERO, now + Duration::from_millis(1));
        m.nack(1, Duration::ZERO, now + Duration::from_millis(2));
        assert_eq!(m.stats.repeated_nacks, 0);
    }
}
