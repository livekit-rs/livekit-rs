//! Bounded sustained delivery validation, separate from Google short probe math.
//! Only actual unique TWCC acknowledgments for this validation are evidence.
use crate::Bitrate;
use std::collections::{HashMap, HashSet};
use std::time::{Duration, Instant};

/// Lifecycle of a bounded sustained validation train.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BweValidationStatus {
    /// Requested, without any matched acknowledgment yet.
    Pending,
    /// Receiving actual matched feedback, not yet a sufficient proof.
    Running,
    /// Both raw spans cover at least one second and measured rate meets target.
    Passed,
    /// Loss, congestion, resource bound or fixed deadline rejected validation.
    Failed,
    /// Application cancelled this request.
    Cancelled,
}
/// Cumulative raw facts for the entire immutable validation request.
#[derive(Debug, Clone, Copy, Default)]
pub struct BweValidationTotals {
    /// Raw full-request local send span.
    pub send_span: Duration,
    /// Raw full-request remote receive span, never compared with local clocks.
    pub recv_span: Duration,
    /// Actual protected bytes excluding the last sent packet.
    pub send_bytes: u64,
    /// Actual protected bytes excluding the first received packet.
    pub recv_bytes: u64,
    /// Unique actually received packets in the request.
    pub received_packets: u64,
    /// Full-request minimum raw rate once both spans cover one second.
    pub measured_bitrate: Option<Bitrate>,
}
/// Actual packet collection selected as the completed validation proof.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BweValidationProofScope {
    /// One existing credit-delimited measurement segment.
    CurrentSegment,
    /// Every unique matched received packet from this immutable request.
    WholeRequest,
}
/// Completed raw evidence; requested pacing strength is never a measured rate.
#[derive(Debug, Clone, Copy)]
pub struct BweValidationProof {
    /// Which actual packet collection supplied every following field.
    pub scope: BweValidationProofScope,
    /// First actual acknowledged local send in the selected collection.
    pub started_at: Instant,
    /// Raw local send span with all internal gaps retained.
    pub send_span: Duration,
    /// Raw remote receive span with all internal gaps retained.
    pub recv_span: Duration,
    /// Actual protected bytes excluding the final sent packet.
    pub send_bytes: u64,
    /// Actual protected bytes excluding the first received packet.
    pub recv_bytes: u64,
    /// Unique received packets in this selected collection.
    pub received_packets: u64,
    /// Minimum actual protected send and receive rates.
    pub measured_bitrate: Bitrate,
    /// Minimum actual payload rate from exactly the same packets and endpoints.
    pub measured_payload_bitrate: Bitrate,
}
/// Actual encrypted RTP delivery, excluding IP/UDP overhead. Current measurement
/// segment and full-request cumulative facts are deliberately different fields.
#[derive(Debug, Clone, Copy)]
pub struct BweValidationObservation {
    /// Cluster/request identity; never compare results across resets.
    pub id: u64,
    /// Time of latest actual feedback or terminal transition.
    pub at: Instant,
    /// Immutable request time and three-second deadline origin.
    pub requested_at: Instant,
    /// Earliest actual sent packet acknowledged anywhere in the full request.
    pub started_at: Option<Instant>,
    /// Required measured protected bitrate, never a measured result.
    pub target_bitrate: Bitrate,
    /// Immutable protected-byte sending strength, distinct from proof floor.
    pub pacing_bitrate: Bitrate,
    /// Completed selected evidence. Current segment diagnostics below are not
    /// rewritten when an explicit dual request selects the whole collection.
    pub selected_proof: Option<BweValidationProof>,
    /// Request lifecycle; any matching loss permanently fails the request.
    pub status: BweValidationStatus,
    /// Current segment's raw local send span.
    pub send_span: Duration,
    /// Current segment's raw remote receive span.
    pub recv_span: Duration,
    /// Current segment protected bytes excluding its last sent packet.
    pub send_bytes: u64,
    /// Current segment protected bytes excluding its first received packet.
    pub recv_bytes: u64,
    /// Unique received packets across the entire request, including old segments.
    pub received_packets: u64,
    /// Unique reported loss across the entire request, including old segments.
    pub lost_packets: u64,
    /// Current segment minimum raw rate, absent until both spans cover one second.
    pub measured_bitrate: Option<Bitrate>,
    /// Full-request cumulative facts, never erased when a new segment starts.
    pub request_totals: BweValidationTotals,
    /// Current segment's first actual acknowledged local send time.
    pub measurement_started_at: Option<Instant>,
    /// Measurement segment count, initially one; increments only on actual credit loss.
    pub measurement_segments: u32,
    /// Sum of actual positive local service credit discarded by native pacing.
    pub cancelled_local_credit: Duration,
    /// Latest real local-send boundary caused by discarding service credit.
    pub last_credit_discard_at: Option<Instant>,
}
#[derive(Default)]
struct Measurement {
    first_send: Option<Instant>,
    last_send: Option<Instant>,
    first_receive: Option<Instant>,
    last_receive: Option<Instant>,
    last_send_size: u64,
    first_receive_size: u64,
    total_bytes: u64,
    last_send_payload_size: u64,
    first_receive_payload_size: u64,
    total_payload_bytes: u64,
    packets: u64,
}
impl Measurement {
    fn record(&mut self, sent: Instant, received: Instant, bytes: u64, payload: u64) {
        self.first_send = Some(self.first_send.map_or(sent, |v| v.min(sent)));
        if self.last_send.is_none_or(|v| sent >= v) {
            self.last_send = Some(sent);
            self.last_send_size = bytes;
            self.last_send_payload_size = payload;
        }
        if self.first_receive.is_none_or(|v| received <= v) {
            self.first_receive = Some(received);
            self.first_receive_size = bytes;
            self.first_receive_payload_size = payload;
        }
        self.last_receive = Some(self.last_receive.map_or(received, |v| v.max(received)));
        self.total_bytes += bytes;
        self.total_payload_bytes += payload;
        self.packets += 1;
    }
    fn totals(&self) -> BweValidationTotals {
        let send_span = self
            .last_send
            .zip(self.first_send)
            .map_or(Duration::ZERO, |(last, first)| last.duration_since(first));
        let recv_span = self
            .last_receive
            .zip(self.first_receive)
            .map_or(Duration::ZERO, |(last, first)| last.duration_since(first));
        let send_bytes = self.total_bytes.saturating_sub(self.last_send_size);
        let recv_bytes = self.total_bytes.saturating_sub(self.first_receive_size);
        BweValidationTotals {
            send_span,
            recv_span,
            send_bytes,
            recv_bytes,
            received_packets: self.packets,
            measured_bitrate: Self::rate(send_span, recv_span, send_bytes, recv_bytes),
        }
    }
    fn rate(
        send_span: Duration,
        recv_span: Duration,
        send_bytes: u64,
        recv_bytes: u64,
    ) -> Option<Bitrate> {
        if send_span < Duration::from_secs(1) || recv_span < Duration::from_secs(1) {
            return None;
        }
        let rate = (send_bytes as f64 * 8.0 / send_span.as_secs_f64())
            .min(recv_bytes as f64 * 8.0 / recv_span.as_secs_f64());
        (rate > 0.0 && rate.is_finite()).then(|| rate.into())
    }
    fn proof(&self, scope: BweValidationProofScope, floor: Bitrate) -> Option<BweValidationProof> {
        let totals = self.totals();
        let measured = totals.measured_bitrate.filter(|rate| *rate >= floor)?;
        Some(BweValidationProof {
            scope,
            started_at: self.first_send?,
            send_span: totals.send_span,
            recv_span: totals.recv_span,
            send_bytes: totals.send_bytes,
            recv_bytes: totals.recv_bytes,
            received_packets: totals.received_packets,
            measured_bitrate: measured,
            measured_payload_bitrate: self.payload_rate().unwrap_or(Bitrate::ZERO),
        })
    }
    fn payload_rate(&self) -> Option<Bitrate> {
        let totals = self.totals();
        Self::rate(
            totals.send_span,
            totals.recv_span,
            self.total_payload_bytes
                .saturating_sub(self.last_send_payload_size),
            self.total_payload_bytes
                .saturating_sub(self.first_receive_payload_size),
        )
    }
}
pub(super) struct Validation {
    observation: BweValidationObservation,
    seen: HashSet<u64>,
    sent: HashMap<u64, Instant>,
    sent_count: usize,
    request: Measurement,
    segment: Measurement,
    segment_cutoff: Instant,
    segment_first_seq: Option<u64>,
    native_result_taken: bool,
    allow_whole_request: bool,
}
impl Validation {
    pub(super) fn new(id: u64, target: Bitrate, now: Instant) -> Self {
        Self::create(id, target, target, now, false)
    }
    pub(super) fn valid_pacing_pair(floor: Bitrate, pacing: Bitrate) -> bool {
        let p = floor.as_f64();
        let q = pacing.as_f64();
        p.is_finite() && q.is_finite() && p > 0.0 && q >= p && q <= p * 1.2 && q < u64::MAX as f64
    }
    pub(super) fn with_pacing(
        id: u64,
        floor: Bitrate,
        pacing: Bitrate,
        now: Instant,
    ) -> Option<Self> {
        Self::valid_pacing_pair(floor, pacing).then(|| Self::create(id, floor, pacing, now, true))
    }
    fn create(id: u64, target: Bitrate, pacing: Bitrate, now: Instant, whole: bool) -> Self {
        Self {
            observation: BweValidationObservation {
                id,
                at: now,
                requested_at: now,
                started_at: None,
                target_bitrate: target,
                pacing_bitrate: pacing,
                selected_proof: None,
                status: BweValidationStatus::Pending,
                send_span: Duration::ZERO,
                recv_span: Duration::ZERO,
                send_bytes: 0,
                recv_bytes: 0,
                received_packets: 0,
                lost_packets: 0,
                measured_bitrate: None,
                request_totals: Default::default(),
                measurement_started_at: None,
                measurement_segments: 1,
                cancelled_local_credit: Duration::ZERO,
                last_credit_discard_at: None,
            },
            seen: HashSet::new(),
            sent: HashMap::new(),
            sent_count: 0,
            request: Default::default(),
            segment: Default::default(),
            segment_cutoff: now,
            segment_first_seq: None,
            native_result_taken: false,
            allow_whole_request: whole,
        }
    }
    pub(super) fn snapshot(&self) -> BweValidationObservation {
        self.observation
    }
    pub(super) fn active(&self) -> bool {
        matches!(
            self.observation.status,
            BweValidationStatus::Pending | BweValidationStatus::Running
        )
    }
    pub(super) fn terminate(&mut self, status: BweValidationStatus, now: Instant) {
        if self.active() {
            self.observation.status = status;
            self.observation.at = now;
            tracing::debug!(id=self.observation.id, status=?status, requested_at=?self.observation.requested_at,
                started_at=?self.observation.started_at, terminal_at=?now, target_bps=self.observation.target_bitrate.as_u64(),
                send_span_us=self.observation.send_span.as_micros(), recv_span_us=self.observation.recv_span.as_micros(),
                send_bytes=self.observation.send_bytes, recv_bytes=self.observation.recv_bytes,
                received_packets=self.observation.received_packets, lost_packets=self.observation.lost_packets,
                measured_bps=?self.observation.measured_bitrate.map(|v|v.as_f64()),
                measurement_started_at=?self.observation.measurement_started_at, measurement_segments=self.observation.measurement_segments,
                cancelled_local_credit_us=self.observation.cancelled_local_credit.as_micros(),
                last_credit_discard_at=?self.observation.last_credit_discard_at, request_totals=?self.observation.request_totals,
                pacing_bps=self.observation.pacing_bitrate.as_f64(), selected_proof=?self.observation.selected_proof,
                "native sustained validation terminal");
        }
    }
    pub(super) fn timeout(&mut self, now: Instant) {
        if now.saturating_duration_since(self.observation.requested_at) >= Duration::from_secs(3) {
            self.terminate(BweValidationStatus::Failed, now);
        }
    }
    /// Register an actual TWCC-tagged send, never a requested padding size.
    pub(super) fn on_sent(&mut self, id: u64, seq: u64, sent_at: Instant) {
        if !self.active() || id != self.observation.id || sent_at < self.observation.requested_at {
            return;
        }
        if self.sent.contains_key(&seq) || self.seen.contains(&seq) {
            return;
        }
        self.sent_count += 1;
        if self.sent_count > 8192 {
            self.terminate(BweValidationStatus::Failed, sent_at);
            return;
        }
        self.sent.insert(seq, sent_at);
    }
    pub(super) fn on_credit_discard_at_seq(
        &mut self,
        id: u64,
        seq: u64,
        sent_at: Instant,
        discarded: Duration,
    ) {
        let before = self.observation.measurement_segments;
        self.on_credit_discard(id, sent_at, discarded);
        if self.observation.measurement_segments != before {
            self.segment_first_seq = Some(seq);
        }
    }
    /// Only the pacing ledger calls this after a real transmitted packet has
    /// actually discarded positive local credit. Feedback/gaps cannot call it.
    pub(super) fn on_credit_discard(&mut self, id: u64, sent_at: Instant, discarded: Duration) {
        if !self.active()
            || id != self.observation.id
            || discarded.is_zero()
            || sent_at < self.observation.requested_at
            || self
                .observation
                .last_credit_discard_at
                .is_some_and(|last| sent_at <= last)
        {
            return;
        }
        self.timeout(sent_at);
        if !self.active() {
            return;
        }
        self.segment_cutoff = sent_at;
        self.segment = Default::default();
        self.observation.measurement_segments =
            self.observation.measurement_segments.saturating_add(1);
        self.observation.cancelled_local_credit += discarded;
        self.observation.last_credit_discard_at = Some(sent_at);
        self.update_segment();
    }
    fn update_segment(&mut self) {
        let totals = self.segment.totals();
        self.observation.send_span = totals.send_span;
        self.observation.recv_span = totals.recv_span;
        self.observation.send_bytes = totals.send_bytes;
        self.observation.recv_bytes = totals.recv_bytes;
        self.observation.measured_bitrate = totals.measured_bitrate;
        self.observation.measurement_started_at = self.segment.first_send;
    }
    #[cfg(test)]
    pub(super) fn record(
        &mut self,
        seq: u64,
        sent: Instant,
        received: Option<Instant>,
        bytes: usize,
        now: Instant,
    ) {
        self.record_packet(seq, sent, received, bytes, bytes, now);
    }
    pub(super) fn record_packet(
        &mut self,
        seq: u64,
        sent: Instant,
        received: Option<Instant>,
        bytes: usize,
        payload_bytes: usize,
        now: Instant,
    ) {
        self.timeout(now);
        if !self.active() || sent < self.observation.requested_at || !self.seen.insert(seq) {
            return;
        }
        if self.seen.len() > 8192 || payload_bytes > bytes {
            self.terminate(BweValidationStatus::Failed, now);
            return;
        }
        self.sent.remove(&seq);
        self.observation.at = now;
        let Some(received) = received else {
            self.observation.lost_packets += 1;
            self.terminate(BweValidationStatus::Failed, now);
            return;
        };
        self.observation.status = BweValidationStatus::Running;
        self.request
            .record(sent, received, bytes as u64, payload_bytes as u64);
        self.observation.request_totals = self.request.totals();
        self.observation.started_at = self.request.first_send;
        self.observation.received_packets = self.request.packets;
        if sent >= self.segment_cutoff && self.segment_first_seq.is_none_or(|first| seq >= first) {
            self.segment
                .record(sent, received, bytes as u64, payload_bytes as u64);
            self.update_segment();
        }
    }
    pub(super) fn finish_feedback(
        &mut self,
        overuse: bool,
        batch_has_loss: bool,
        now: Instant,
    ) -> Option<Bitrate> {
        if overuse {
            self.terminate(BweValidationStatus::Failed, now);
        }
        self.finish_batch(now);
        if self.observation.status != BweValidationStatus::Passed || self.native_result_taken {
            return None;
        }
        self.native_result_taken = true;
        if overuse || batch_has_loss {
            return None;
        }
        self.observation
            .selected_proof
            .map(|proof| proof.measured_payload_bitrate)
            .filter(|rate| !rate.is_zero())
    }
    pub(super) fn finish_batch(&mut self, now: Instant) {
        self.timeout(now);
        if !self.active() {
            return;
        }
        self.update_segment();
        // A new segment cannot conceal unresolved actual sends in older ones.
        // Current-segment in-flight tails retain the original proof semantics.
        if self.sent.iter().any(|(seq, sent)| {
            *sent < self.segment_cutoff || self.segment_first_seq.is_some_and(|first| *seq < first)
        }) {
            return;
        }
        let selected = self
            .segment
            .proof(
                BweValidationProofScope::CurrentSegment,
                self.observation.target_bitrate,
            )
            .or_else(|| {
                self.allow_whole_request
                    .then(|| {
                        self.request.proof(
                            BweValidationProofScope::WholeRequest,
                            self.observation.target_bitrate,
                        )
                    })
                    .flatten()
            });
        if let Some(proof) = selected {
            self.observation.selected_proof = Some(proof);
            self.terminate(BweValidationStatus::Passed, now);
        }
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn short_burst_and_requested_rate_cannot_pass_sustained_validation() {
        let start = Instant::now();
        let mut v = Validation::new(7, Bitrate::kbps(200), start);
        for i in 0..5 {
            let at = start + Duration::from_millis(i * 4);
            v.record(i, at, Some(at), 1100, at);
        }
        v.finish_batch(start + Duration::from_millis(20));
        assert_eq!(v.snapshot().status, BweValidationStatus::Running);
        assert!(v.snapshot().measured_bitrate.is_none());
        v.timeout(start + Duration::from_secs(3));
        assert_eq!(v.snapshot().status, BweValidationStatus::Failed);
    }
    #[test]
    fn actual_full_raw_window_passes_without_gap_correction_or_fake_target_bytes() {
        let start = Instant::now();
        let remote = start + Duration::from_secs(60);
        let mut v = Validation::new(8, Bitrate::kbps(160), start);
        for i in 0..=100 {
            let sent = start + Duration::from_millis(i * 10);
            let recv = remote + Duration::from_millis(i * 10);
            v.record(i, sent, Some(recv), 200, sent + Duration::from_millis(20));
        }
        v.finish_batch(start + Duration::from_millis(1020));
        let o = v.snapshot();
        assert_eq!(o.status, BweValidationStatus::Passed);
        assert_eq!(o.send_bytes, 20000);
        assert_eq!(o.recv_bytes, 20000);
        assert_eq!(o.measured_bitrate.unwrap(), Bitrate::kbps(160));
    }
    #[test]
    fn slow_receive_gaps_remain_in_denominator_and_do_not_prove_fast_train() {
        let start = Instant::now();
        let remote = start + Duration::from_secs(60);
        let mut v = Validation::new(8, Bitrate::kbps(160), start);
        for i in 0..=100 {
            v.record(
                i,
                start + Duration::from_millis(i * 10),
                Some(remote + Duration::from_millis(i * 20)),
                200,
                start + Duration::from_millis(i * 20),
            );
        }
        v.finish_batch(start + Duration::from_secs(2));
        assert_eq!(v.snapshot().measured_bitrate.unwrap(), Bitrate::kbps(80));
        assert_eq!(v.snapshot().status, BweValidationStatus::Running);
        v.timeout(start + Duration::from_secs(3));
        assert_eq!(v.snapshot().status, BweValidationStatus::Failed);
    }
    #[test]
    fn old_loss_duplicate_and_timer_cannot_refresh_or_extend_fixed_request() {
        let start = Instant::now();
        let mut v = Validation::new(1, Bitrate::kbps(100), start);
        v.record(9, start - Duration::from_millis(1), None, 200, start);
        assert_eq!(v.snapshot().lost_packets, 0);
        v.record(1, start, Some(start), 200, start);
        let before = v.snapshot();
        v.record(1, start, Some(start), 200, start + Duration::from_secs(1));
        assert_eq!(v.snapshot().at, before.at);
        v.timeout(start + Duration::from_secs(2));
        assert_eq!(v.snapshot().at, before.at);
        v.record(
            2,
            start + Duration::from_secs(1),
            None,
            200,
            start + Duration::from_secs(1),
        );
        assert_eq!(v.snapshot().status, BweValidationStatus::Failed);
        assert_eq!(v.snapshot().requested_at, start);
    }
    #[test]
    fn loss_later_in_same_batch_rejects_earlier_full_span_before_publication() {
        let start = Instant::now();
        let mut v = Validation::new(2, Bitrate::kbps(160), start);
        for i in 0..=100 {
            let at = start + Duration::from_millis(i * 10);
            v.record(i, at, Some(at), 200, at);
        }
        assert_ne!(v.snapshot().status, BweValidationStatus::Passed);
        v.record(
            101,
            start + Duration::from_millis(1010),
            None,
            200,
            start + Duration::from_millis(1050),
        );
        v.finish_batch(start + Duration::from_millis(1050));
        assert_eq!(v.snapshot().status, BweValidationStatus::Failed);
        assert_eq!(v.snapshot().lost_packets, 1);
    }
}

#[cfg(test)]
mod native_payload_tests {
    use super::*;
    fn full_window(start: Instant) -> Validation {
        let mut v = Validation::new(42, Bitrate::kbps(80), start);
        for i in 0..=10 {
            let sent = start + Duration::from_millis(i * 100);
            v.record_packet(
                i,
                sent,
                Some(sent + Duration::from_secs(30)),
                1000,
                900,
                sent,
            );
        }
        v
    }
    #[test]
    fn native_input_uses_actual_payload_not_protected_goal_and_only_once() {
        let start = Instant::now();
        let mut v = full_window(start);
        assert_eq!(
            v.finish_feedback(false, false, start + Duration::from_secs(1)),
            Some(Bitrate::kbps(72))
        );
        assert_eq!(v.snapshot().measured_bitrate, Some(Bitrate::kbps(80)));
        assert_eq!(v.snapshot().status, BweValidationStatus::Passed);
        assert!(
            v.finish_feedback(false, false, start + Duration::from_secs(2))
                .is_none()
        );
        assert!(
            v.finish_feedback(true, true, start + Duration::from_secs(2))
                .is_none()
        );
    }
    #[test]
    fn same_batch_loss_cannot_rebase_and_cached_pass_cannot_retry() {
        let start = Instant::now();
        let mut v = full_window(start);
        assert!(
            v.finish_feedback(false, true, start + Duration::from_secs(1))
                .is_none()
        );
        assert_eq!(v.snapshot().status, BweValidationStatus::Passed);
        assert!(
            v.finish_feedback(false, false, start + Duration::from_secs(2))
                .is_none()
        );
    }
    #[test]
    fn current_overuse_rejects_new_proof_before_native_input() {
        let start = Instant::now();
        let mut v = full_window(start);
        assert!(
            v.finish_feedback(true, false, start + Duration::from_secs(1))
                .is_none()
        );
        assert_eq!(v.snapshot().status, BweValidationStatus::Failed);
        assert!(
            v.finish_feedback(false, false, start + Duration::from_secs(2))
                .is_none()
        );
    }
}

#[cfg(test)]
mod credit_segment_tests {
    use super::*;

    fn ack(v: &mut Validation, seq: u64, start: Instant, ms: u64, bytes: usize) {
        let sent = start + Duration::from_millis(ms);
        v.record_packet(
            seq,
            sent,
            Some(sent + Duration::from_secs(20)),
            bytes,
            bytes / 2,
            sent,
        );
    }

    #[test]
    fn local_clip_keeps_whole_request_but_measures_only_new_actual_segment() {
        let start = Instant::now();
        let mut v = Validation::new(7, Bitrate::kbps(80), start);
        ack(&mut v, 0, start, 0, 1000);
        ack(&mut v, 1, start, 100, 1000);
        v.on_credit_discard(
            7,
            start + Duration::from_millis(300),
            Duration::from_millis(40),
        );
        assert_eq!(v.snapshot().measurement_started_at, None);
        // Actual new packet endpoints, not the callback, establish the span.
        for i in 0..=10 {
            ack(&mut v, i + 2, start, 310 + i * 100, 1000);
        }
        let at = start + Duration::from_millis(1310);
        assert_eq!(v.finish_feedback(false, false, at), Some(Bitrate::kbps(40)));
        let result = v.snapshot();
        assert_eq!(result.measurement_segments, 2);
        assert_eq!(
            result.measurement_started_at,
            Some(start + Duration::from_millis(310))
        );
        assert_eq!(result.cancelled_local_credit, Duration::from_millis(40));
        assert_eq!(result.measured_bitrate, Some(Bitrate::kbps(80)));
        assert!(result.request_totals.measured_bitrate.unwrap() < Bitrate::kbps(80));
        assert_eq!(result.received_packets, 13);
        assert!(v.finish_feedback(false, false, at).is_none());
    }

    #[test]
    fn old_unresolved_actual_send_blocks_pass_and_delayed_old_loss_is_permanent() {
        let start = Instant::now();
        let mut v = Validation::new(7, Bitrate::kbps(80), start);
        v.on_sent(7, 99, start);
        v.on_credit_discard(
            7,
            start + Duration::from_millis(300),
            Duration::from_millis(40),
        );
        for i in 0..=10 {
            ack(&mut v, i, start, 300 + i * 100, 1000);
        }
        let at = start + Duration::from_millis(1300);
        assert!(v.finish_feedback(false, false, at).is_none());
        assert_eq!(v.snapshot().status, BweValidationStatus::Running);
        v.record_packet(99, start, None, 1000, 500, at);
        assert!(v.finish_feedback(false, false, at).is_none());
        assert_eq!(v.snapshot().status, BweValidationStatus::Failed);
        assert_eq!(v.snapshot().lost_packets, 1);
        v.on_credit_discard(7, at, Duration::from_millis(10));
        assert_eq!(v.snapshot().measurement_segments, 2);
    }

    #[test]
    fn old_good_ack_closes_old_send_without_polluting_segment_or_duplicate_counts() {
        let start = Instant::now();
        let mut v = Validation::new(7, Bitrate::kbps(80), start);
        v.on_sent(7, 99, start);
        v.on_credit_discard(
            7,
            start + Duration::from_millis(300),
            Duration::from_millis(40),
        );
        for i in 0..=10 {
            ack(&mut v, i, start, 300 + i * 100, 1000);
        }
        let before = v.snapshot();
        let at = start + Duration::from_millis(1400);
        v.record_packet(
            99,
            start,
            Some(start + Duration::from_secs(20)),
            50_000,
            25_000,
            at,
        );
        v.record_packet(99, start, None, 50_000, 25_000, at);
        assert_eq!(v.snapshot().send_bytes, before.send_bytes);
        assert_eq!(v.snapshot().recv_bytes, before.recv_bytes);
        assert_eq!(v.snapshot().received_packets, 12);
        assert_eq!(v.finish_feedback(false, false, at), Some(Bitrate::kbps(40)));
    }

    #[test]
    fn same_instant_actual_sequence_boundary_cannot_hide_old_send_or_loss() {
        let start = Instant::now();
        let mut v = Validation::new(7, Bitrate::kbps(80), start);
        let cut = start + Duration::from_millis(300);
        v.on_sent(7, 1, cut);
        v.on_credit_discard_at_seq(7, 2, cut, Duration::from_millis(40));
        v.on_sent(7, 2, cut);
        for i in 0..=10 {
            ack(&mut v, i + 2, start, 300 + i * 100, 1000);
        }
        let at = start + Duration::from_millis(1300);
        v.finish_batch(at);
        assert_eq!(v.snapshot().status, BweValidationStatus::Running);
        v.record_packet(1, cut, None, 1000, 500, at);
        v.finish_batch(at);
        assert_eq!(v.snapshot().status, BweValidationStatus::Failed);
    }

    #[test]
    fn invalid_or_repeated_clip_cannot_restart_measurement_or_deadline() {
        let start = Instant::now();
        let mut v = Validation::new(7, Bitrate::kbps(80), start);
        v.on_credit_discard(8, start, Duration::from_millis(1));
        v.on_credit_discard(7, start, Duration::ZERO);
        assert_eq!(v.snapshot().measurement_segments, 1);
        for ms in [100, 900, 1700, 2500] {
            let at = start + Duration::from_millis(ms);
            v.on_credit_discard(7, at, Duration::from_millis(40));
            v.on_credit_discard(7, at, Duration::from_millis(40));
            ack(&mut v, ms, start, ms, 1000);
        }
        assert_eq!(v.snapshot().measurement_segments, 5);
        v.timeout(start + Duration::from_secs(3));
        assert_eq!(v.snapshot().status, BweValidationStatus::Failed);
        assert!(v.snapshot().measured_bitrate.is_none());
        assert_eq!(v.snapshot().requested_at, start);
    }

    #[test]
    fn network_gap_never_creates_segment_or_erases_receive_span() {
        let start = Instant::now();
        let mut v = Validation::new(7, Bitrate::kbps(80), start);
        for i in 0..=10 {
            let sent = start + Duration::from_millis(i * 100);
            let recv = sent
                + Duration::from_secs(20)
                + Duration::from_millis(if i >= 5 { 500 } else { 0 });
            v.record_packet(i, sent, Some(recv), 1000, 500, sent);
        }
        v.finish_batch(start + Duration::from_secs(1));
        assert_eq!(v.snapshot().measurement_segments, 1);
        assert_eq!(v.snapshot().recv_span, Duration::from_millis(1500));
        assert!(v.snapshot().measured_bitrate.unwrap() < Bitrate::kbps(80));
        assert_eq!(v.snapshot().status, BweValidationStatus::Running);
    }

    #[test]
    fn clipping_cannot_manufacture_rate_or_forget_global_seen_resource_limit() {
        let start = Instant::now();
        let mut v = Validation::new(7, Bitrate::kbps(80), start);
        v.on_credit_discard(
            7,
            start + Duration::from_millis(300),
            Duration::from_millis(40),
        );
        for i in 0..=10 {
            ack(&mut v, i, start, 300 + i * 100, 100);
        }
        v.finish_batch(start + Duration::from_millis(1300));
        assert_eq!(v.snapshot().measured_bitrate, Some(Bitrate::kbps(8)));
        assert_eq!(v.snapshot().status, BweValidationStatus::Running);
        for i in 100..=8292 {
            v.on_sent(7, i, start + Duration::from_millis(1400));
        }
        assert_eq!(v.snapshot().status, BweValidationStatus::Failed);
    }
}

#[cfg(test)]
mod dual_proof_tests {
    use super::*;

    fn segmented(start: Instant, dual: bool, bytes: usize) -> Validation {
        let mut v = if dual {
            Validation::with_pacing(77, Bitrate::kbps(80), Bitrate::kbps(96), start).unwrap()
        } else {
            Validation::new(77, Bitrate::kbps(80), start)
        };
        for i in 0..=12 {
            let at = start + Duration::from_millis(i * 100);
            if i > 0 && i % 3 == 0 {
                v.on_credit_discard(77, at, Duration::from_millis(20));
            }
            v.record_packet(
                i,
                at,
                Some(at + Duration::from_secs(20)),
                bytes,
                bytes / 2,
                at,
            );
        }
        v
    }

    #[test]
    fn only_explicit_dual_can_select_complete_raw_request_with_short_segments() {
        let start = Instant::now();
        let at = start + Duration::from_millis(1220);
        let mut single = segmented(start, false, 1000);
        assert!(single.finish_feedback(false, false, at).is_none());
        assert_eq!(single.snapshot().status, BweValidationStatus::Running);
        let mut dual = segmented(start, true, 1000);
        assert_eq!(
            dual.finish_feedback(false, false, at),
            Some(Bitrate::kbps(40))
        );
        let o = dual.snapshot();
        let proof = o.selected_proof.unwrap();
        assert_eq!(proof.scope, BweValidationProofScope::WholeRequest);
        assert_eq!(proof.measured_bitrate, Bitrate::kbps(80));
        assert_eq!(proof.measured_payload_bitrate, Bitrate::kbps(40));
        assert_eq!(proof.send_span, Duration::from_millis(1200));
        assert!(o.send_span < Duration::from_secs(1));
        assert!(o.measured_bitrate.is_none());
        assert!(dual.finish_feedback(false, false, at).is_none());
    }

    #[test]
    fn higher_pacing_does_not_replace_actual_insufficient_prefix_or_clear_loss() {
        let start = Instant::now();
        let at = start + Duration::from_millis(1220);
        let mut slow = segmented(start, true, 999);
        assert!(slow.finish_feedback(false, false, at).is_none());
        assert!(slow.snapshot().request_totals.measured_bitrate.unwrap() < Bitrate::kbps(80));
        assert!(slow.snapshot().selected_proof.is_none());
        let mut unresolved = segmented(start, true, 1000);
        unresolved.on_sent(77, 99, start);
        assert!(unresolved.finish_feedback(false, false, at).is_none());
        unresolved.record_packet(99, start, None, 1000, 500, at);
        assert!(unresolved.finish_feedback(false, false, at).is_none());
        assert_eq!(unresolved.snapshot().status, BweValidationStatus::Failed);
        let mut overuse = segmented(start, true, 1000);
        assert!(overuse.finish_feedback(true, false, at).is_none());
        assert_eq!(overuse.snapshot().status, BweValidationStatus::Failed);
    }

    #[test]
    fn pacing_pair_rejects_nonfinite_negative_unrepresentable_and_one_ulp_excess() {
        let start = Instant::now();
        let limit = 100_000.0_f64 * 1.2;
        let above = f64::from_bits(limit.to_bits() + 1);
        for (p, q) in [
            (0.0, 1.0),
            (-1.0, 1.0),
            (f64::NAN, 1.0),
            (1.0, f64::NAN),
            (1.0, f64::INFINITY),
            (100_000.0, 99_999.0),
            (100_000.0, above),
            (u64::MAX as f64, u64::MAX as f64),
        ] {
            assert!(Validation::with_pacing(1, p.into(), q.into(), start).is_none());
        }
        assert!(Validation::with_pacing(1, Bitrate::kbps(100), limit.into(), start).is_some());
    }
}
