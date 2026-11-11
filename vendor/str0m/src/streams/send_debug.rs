//! Read-only diagnostics of actual primary RTP emissions. This is deliberately
//! independent of the source transport statistics and remote-BWE counters.
use super::*;
#[derive(Clone, Copy, Debug, Default)]
pub(super) struct PrimaryPacketHistory {
    last: Option<(u64, u64, bool)>,
    previous: Option<(u64, u64, bool)>,
    pub(super) report: Option<SenderInfo>,
}
impl PrimaryPacketHistory {
    pub(super) fn observe(&mut self, seq: u64, timestamp: u32, marker: bool) {
        let timestamp = crate::rtp_::extend_u32(self.last.map(|(_, time, _)| time), timestamp);
        self.previous = self.last;
        self.last = Some((seq, timestamp, marker));
    }
}
impl StreamTx {
    /// Last and preceding packets that the packetizer actually emitted as
    /// regular primary media. RTX and padding do not mutate this history.
    pub fn debug_primary_packets(&self) -> (Option<(u64, u64, bool)>, Option<(u64, u64, bool)>) {
        (self.debug_primary.last, self.debug_primary.previous)
    }
    /// Exactly the last native SR queued by the real RTCP producer. An
    /// unstarted stream has no diagnostic sender report instead of a fake one.
    pub fn debug_sender_report(&self, _now: Instant) -> Option<SenderInfo> {
        self.debug_primary.report
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn observed_primary_packet_history_preserves_wrap_and_real_markers() {
        let mut history = PrimaryPacketHistory::default();
        assert!(history.last.is_none());
        history.observe(65_535, u32::MAX - 1, false);
        history.observe(65_536, 2, true);
        assert_eq!(history.previous, Some((65_535, u32::MAX as u64 - 1, false)));
        assert_eq!(history.last, Some((65_536, 0x1_0000_0002, true)));
    }
}
