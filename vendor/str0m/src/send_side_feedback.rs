//! Explicit, default-off raw boundary for an application-owned estimator.
use std::collections::VecDeque;
use std::time::Instant;

/// Metadata from one actually emitted RTP datagram, after protection/size checks.
#[derive(Clone, Copy, Debug)]
pub struct SendSidePacket {
    /// Local monotonic time used for this actual output.
    pub at: Instant,
    /// Actual RTP sequence number, independent of the transport-wide sequence.
    pub sequence: u16,
    /// Actual sender SSRC (including RTX when this is a retransmission).
    pub ssrc: u32,
    /// RTP header bytes, including actual negotiated extensions.
    pub header_bytes: usize,
    /// RTP payload bytes, excluding RTP padding and SRTP authentication bytes.
    pub payload_bytes: usize,
    /// Whether this media negotiated the transport sequence extension.
    pub twcc_negotiated: bool,
    /// Transport sequence actually encoded in this packet, when present.
    pub transport_sequence: Option<u16>,
}
/// Actual send/authenticated-feedback inputs for a separate estimator owner.
#[derive(Clone)]
pub enum SendSideFeedback {
    /// One real RTP output; contains no media payload.
    Sent(SendSidePacket),
    /// Authenticated original SRTCP datagram; the owner validates the complete compound before delivering any feedback.
    Report {
        /// Actual local authenticated receive time.
        at: Instant,
        /// Original complete RTCP datagram bytes (not native matched records).
        packet: Vec<u8>,
    },
    /// Explicit local observation discontinuity. The owner must retire its
    /// estimator instead of silently continuing with incomplete sent history.
    Discontinuity,
}
impl std::fmt::Debug for SendSideFeedback {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Sent(v) => f.debug_tuple("Sent").field(v).finish(),
            Self::Report { at, packet } => f
                .debug_struct("Report")
                .field("at", at)
                .field("bytes", &packet.len())
                .finish(),
            Self::Discontinuity => f.write_str("Discontinuity"),
        }
    }
}
#[derive(Debug, Default)]
pub(crate) struct SendSideObserver {
    queue: VecDeque<SendSideFeedback>,
    discontinuity: bool,
    queued_bytes: usize,
}
impl SendSideObserver {
    const CAPACITY: usize = 512;
    const BYTE_CAPACITY: usize = 256 * 1024;
    fn bytes(item: &SendSideFeedback) -> usize {
        match item {
            SendSideFeedback::Report { packet, .. } => packet.len(),
            _ => 0,
        }
    }
    pub fn push(&mut self, item: SendSideFeedback) {
        if self.discontinuity {
            return;
        }
        let bytes = Self::bytes(&item);
        if self.queue.len() == Self::CAPACITY || bytes > Self::BYTE_CAPACITY - self.queued_bytes {
            self.queue.clear();
            self.queued_bytes = 0;
            self.queue.push_back(SendSideFeedback::Discontinuity);
            self.discontinuity = true;
        } else {
            self.queued_bytes += bytes;
            self.queue.push_back(item);
        }
    }
    pub fn report(&mut self, at: Instant, raw: &[u8]) {
        if !self.discontinuity {
            self.push(SendSideFeedback::Report {
                at,
                packet: raw.to_vec(),
            });
        }
    }
    pub fn poll(&mut self) -> Option<SendSideFeedback> {
        let item = self.queue.pop_front()?;
        self.queued_bytes -= Self::bytes(&item);
        Some(item)
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn compound_feedback_keeps_original_datagram_and_overflow_retires_once() {
        let now = Instant::now();
        let mut meter = SendSideObserver::default();
        let a = [0x8f, 205, 0, 1, 1, 2, 3, 4];
        let ignored = [0x80, 200, 0, 1, 5, 6, 7, 8];
        let b = [0x8b, 205, 0, 1, 9, 10, 11, 12];
        let bytes = [a.as_slice(), ignored.as_slice(), b.as_slice()].concat();
        meter.report(now, &bytes);
        let Some(SendSideFeedback::Report { at, packet }) = meter.poll() else {
            panic!("original authenticated datagram expected")
        };
        assert_eq!(at, now);
        assert_eq!(packet, bytes);
        assert!(meter.poll().is_none());
        for _ in 0..=SendSideObserver::CAPACITY {
            meter.push(SendSideFeedback::Discontinuity);
        }
        assert!(matches!(
            meter.poll(),
            Some(SendSideFeedback::Discontinuity)
        ));
        assert!(meter.poll().is_none());
        meter.report(now, &a);
        assert!(meter.poll().is_none());
    }
}
