//! Bounded metadata only; never changes TWCC encoding or estimation.
use std::collections::VecDeque;
use std::time::{Duration, Instant};

/// Metadata for server-generated TWCC about incoming RTP, not a capacity proof.
#[derive(Debug, Clone, Copy)]
pub struct BweIncomingTwccObservation {
    /// Report sender SSRC.
    pub sender_ssrc: u32,
    /// Report media SSRC.
    pub media_ssrc: u32,
    /// Low sixteen bits of the first reported transport sequence.
    pub base_seq: u16,
    /// Wire feedback counter (wraps at256).
    pub feedback_count: u8,
    /// Number of reported packet statuses, including missing packets.
    pub status_count: u16,
    /// Actually received statuses in this report.
    pub received_packets: usize,
    /// Missing statuses in this report, not invented received timestamps.
    pub missing_packets: usize,
    /// Earliest actual receipt in this report's packet set.
    pub first_received_at: Instant,
    /// Latest actual receipt in the same set.
    pub last_received_at: Instant,
    /// Actual receive span in local clock units.
    pub actual_span: Duration,
    /// Span reconstructed from the report's real encoded deltas.
    pub encoded_span: Duration,
    /// Largest absolute per-packet reconstruction error.
    pub max_encoding_error: Duration,
    /// Session feedback construction time, absent for standalone encoder calls.
    pub built_at: Option<Instant>,
    /// Session feedback serialization poll time, not kernel/network send time.
    pub emitted_at: Option<Instant>,
    /// Time from oldest actual receipt to serialization poll.
    pub report_age: Option<Duration>,
    /// Maximum oldest-receipt age observed at serialization in this registration.
    pub max_report_age: Duration,
    /// Maximum build-to-serialization queue delay in this registration.
    pub max_pending_age: Duration,
    /// Largest gap between unique receive callbacks in this registration.
    pub max_receiver_gap: Duration,
}

#[derive(Debug, Default)]
pub(super) struct ObservationHistory {
    reports: VecDeque<BweIncomingTwccObservation>,
    last_receipt: Option<Instant>,
    max_receiver_gap: Duration,
    max_report_age: Duration,
    max_pending_age: Duration,
    last_anomaly_log: Option<Instant>,
}
impl ObservationHistory {
    pub(super) fn received(&mut self, at: Instant) {
        if let Some(last) = self.last_receipt {
            self.max_receiver_gap = self
                .max_receiver_gap
                .max(at.saturating_duration_since(last));
        }
        self.last_receipt = Some(self.last_receipt.map_or(at, |last| last.max(at)));
    }
    pub(super) fn push(&mut self, mut observation: BweIncomingTwccObservation) {
        if self.reports.len() == 32 {
            self.reports.pop_front();
        }
        observation.max_receiver_gap = self.max_receiver_gap;
        self.reports.push_back(observation);
    }
    pub(super) fn built(&mut self, base: u16, counter: u8, sender: u32, media: u32, now: Instant) {
        if let Some(o) = self
            .reports
            .iter_mut()
            .rev()
            .find(|o| o.base_seq == base && o.feedback_count == counter)
        {
            o.built_at = Some(now);
            o.sender_ssrc = sender;
            o.media_ssrc = media;
        }
    }
    pub(super) fn emitted(&mut self, base: u16, counter: u8, now: Instant) {
        let Some(o) =
            self.reports.iter_mut().rev().find(|o| {
                o.base_seq == base && o.feedback_count == counter && o.emitted_at.is_none()
            })
        else {
            return;
        };
        o.emitted_at = Some(now);
        let age = now.saturating_duration_since(o.first_received_at);
        o.report_age = Some(age);
        self.max_report_age = self.max_report_age.max(age);
        self.max_pending_age = self.max_pending_age.max(
            o.built_at
                .map_or(Duration::ZERO, |at| now.saturating_duration_since(at)),
        );
        o.max_report_age = self.max_report_age;
        o.max_pending_age = self.max_pending_age;
        o.max_receiver_gap = self.max_receiver_gap;
        let anomalous =
            age > Duration::from_millis(100) || o.max_encoding_error > Duration::from_micros(125);
        if anomalous
            && self
                .last_anomaly_log
                .is_none_or(|at| now.saturating_duration_since(at) >= Duration::from_secs(1))
        {
            self.last_anomaly_log = Some(now);
            tracing::debug!(observation = ?o, "incoming TWCC metadata anomaly (serialization poll, not kernel send)");
        }
    }
    pub(super) fn snapshot(&self) -> Option<BweIncomingTwccObservation> {
        let mut o = *self.reports.back()?;
        o.max_receiver_gap = self.max_receiver_gap;
        o.max_report_age = self.max_report_age;
        o.max_pending_age = self.max_pending_age;
        Some(o)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn observation(at: Instant, counter: u8) -> BweIncomingTwccObservation {
        BweIncomingTwccObservation {
            sender_ssrc: 1,
            media_ssrc: 2,
            base_seq: u16::from(counter),
            feedback_count: counter,
            status_count: 1,
            received_packets: 1,
            missing_packets: 0,
            first_received_at: at,
            last_received_at: at,
            actual_span: Duration::ZERO,
            encoded_span: Duration::ZERO,
            max_encoding_error: Duration::ZERO,
            built_at: None,
            emitted_at: None,
            report_age: None,
            max_report_age: Duration::ZERO,
            max_pending_age: Duration::ZERO,
            max_receiver_gap: Duration::ZERO,
        }
    }
    #[test]
    fn bounded_history_identity_and_duplicate_emit_do_not_refresh_time() {
        let at = Instant::now();
        let mut h = ObservationHistory::default();
        assert!(h.snapshot().is_none());
        for n in 0..100 {
            h.push(observation(at, n));
        }
        assert_eq!(h.reports.len(), 32);
        h.built(99, 99, 3, 4, at + Duration::from_millis(5));
        h.emitted(98, 99, at + Duration::from_millis(10));
        assert!(h.snapshot().unwrap().emitted_at.is_none());
        h.emitted(99, 99, at + Duration::from_millis(15));
        let o = h.snapshot().unwrap();
        assert_eq!(o.report_age, Some(Duration::from_millis(15)));
        assert_eq!(o.max_pending_age, Duration::from_millis(10));
        h.emitted(99, 99, at + Duration::from_secs(2));
        assert_eq!(h.snapshot().unwrap().emitted_at, o.emitted_at);
        assert_eq!(h.snapshot().unwrap().max_report_age, o.max_report_age);
    }
    #[test]
    fn real_callback_gap_is_retained_without_crossing_remote_clock() {
        let at = Instant::now();
        let mut h = ObservationHistory::default();
        h.received(at);
        h.received(at + Duration::from_millis(193));
        h.received(at + Duration::from_millis(194));
        h.push(observation(at, 1));
        assert_eq!(
            h.snapshot().unwrap().max_receiver_gap,
            Duration::from_millis(193)
        );
    }
}
