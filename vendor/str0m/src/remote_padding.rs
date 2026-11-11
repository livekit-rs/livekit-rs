//! Bounded outgoing padding execution, independent of native bandwidth estimation.
use crate::rtp_::Bitrate;
use std::time::{Duration, Instant};

/// Local execution status. No state is proof of remote receipt or capacity.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PaddingStatus {
    /// Local padding authorization may still produce output.
    Active,
    /// Remaining protected-byte budget is smaller than one conservative packet.
    Completed,
    /// Immutable request deadline was reached.
    Expired,
    /// Owner cancelled this identity or closed the RTP session.
    Cancelled,
    /// Negotiated sending RTX source is no longer usable.
    SourceUnavailable,
    /// Generated packet exceeded the conservative size/budget; no output emitted.
    EncodingLimit,
}
/// Rejected requests do not consume identities or replace existing work.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PaddingRequestError {
    /// Byte, rate or duration limits are invalid.
    InvalidLimits,
    /// An independent request is already active.
    Busy,
    /// Native BWE owns probe/pacing execution on this session.
    NativeBweEnabled,
    /// RTP is closing or SRTP keys are unavailable.
    NotReady,
    /// No negotiated non-stopped sending RTX source is available.
    NoSource,
    /// Monotonic request identities cannot advance without reuse.
    IdentityExhausted,
}
/// Explicit local execution behavior. Neither mode is received-capacity proof.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PaddingMode {
    /// Original byte/rate-paced execution; unchanged by burst requests.
    Paced,
    /// Owner-authorized bounded burst, at most16packets per logical wake.
    Burst,
}
/// Actual local protected RTP output counters, never receiver acknowledgement.
#[derive(Clone, Copy, Debug)]
pub struct PaddingObservation {
    /// Monotonic local execution identity.
    pub id: u64,
    /// Actual selected local scheduling behavior.
    pub mode: PaddingMode,
    /// Local authorization/execution status, never receipt proof.
    pub status: PaddingStatus,
    /// Owner-supplied monotonic request time.
    pub requested_at: Instant,
    /// Fixed local expiry; actual output never extends it.
    pub deadline: Instant,
    /// Maximum total protected RTP bytes authorized for this request.
    pub max_protected_bytes: u64,
    /// Paced-mode local protected-byte rate, not estimated capacity. Burst mode
    /// does not infer a bitrate from the request; this stores the API ceiling only.
    pub rate: Bitrate,
    /// Protected RTP bytes actually produced as local datagram output.
    pub sent_protected_bytes: u64,
    /// Actual local output count, excluding rejected/queued-only work.
    pub sent_packets: u64,
    /// First actual local datagram output, absent until a packet is produced.
    pub first_output_at: Option<Instant>,
    /// Last actual local datagram output, absent if no packet was produced.
    pub last_output_at: Option<Instant>,
}

pub(crate) const MAX_PADDING_PACKET: usize = 512;
#[derive(Debug, Default)]
pub(crate) struct PaddingExecutor {
    next_id: u64,
    observation: Option<PaddingObservation>,
    next_at: Option<Instant>,
    burst_at: Option<(Instant, u8)>,
}
impl PaddingExecutor {
    pub(crate) fn start(
        &mut self,
        bytes: u64,
        rate: Bitrate,
        duration: Duration,
        now: Instant,
    ) -> Result<u64, PaddingRequestError> {
        let bps = rate.as_f64();
        if bytes < MAX_PADDING_PACKET as u64
            || bytes > 16 * 1024 * 1024
            || !bps.is_finite()
            || bps < 1.0
            || bps > 100_000_000.0
            || duration.is_zero()
            || duration > Duration::from_secs(5)
        {
            return Err(PaddingRequestError::InvalidLimits);
        }
        self.expire(now);
        if self
            .observation
            .is_some_and(|o| o.status == PaddingStatus::Active)
        {
            return Err(PaddingRequestError::Busy);
        }
        let deadline = now
            .checked_add(duration)
            .ok_or(PaddingRequestError::InvalidLimits)?;
        let id = self
            .next_id
            .checked_add(1)
            .ok_or(PaddingRequestError::IdentityExhausted)?;
        self.next_id = id;
        self.observation = Some(PaddingObservation {
            id,
            mode: PaddingMode::Paced,
            status: PaddingStatus::Active,
            requested_at: now,
            deadline,
            max_protected_bytes: bytes,
            rate,
            sent_protected_bytes: 0,
            sent_packets: 0,
            first_output_at: None,
            last_output_at: None,
        });
        self.next_at = Some(now);
        Ok(id)
    }
    pub(crate) fn start_burst(
        &mut self,
        bytes: u64,
        duration: Duration,
        now: Instant,
    ) -> Result<u64, PaddingRequestError> {
        let id = self.start(bytes, Bitrate::bps(100_000_000), duration, now)?;
        self.observation.as_mut().unwrap().mode = PaddingMode::Burst;
        if self
            .burst_at
            .is_some_and(|(at, count)| at == now && count >= 16)
        {
            self.next_at = Some(now + Duration::from_millis(1));
        }
        Ok(id)
    }
    pub(crate) fn observation(&self) -> Option<PaddingObservation> {
        self.observation
    }
    pub(crate) fn expire(&mut self, now: Instant) {
        if self
            .observation
            .is_some_and(|o| o.status == PaddingStatus::Active && now >= o.deadline)
        {
            self.stop(PaddingStatus::Expired);
        }
    }
    pub(crate) fn cancel(&mut self, id: u64) -> bool {
        if self
            .observation
            .is_some_and(|o| o.id == id && o.status == PaddingStatus::Active)
        {
            self.stop(PaddingStatus::Cancelled);
            true
        } else {
            false
        }
    }
    pub(crate) fn stop(&mut self, status: PaddingStatus) {
        if let Some(o) = self.observation.as_mut() {
            o.status = status;
        }
        self.next_at = None;
    }
    pub(crate) fn due(&mut self, now: Instant) -> bool {
        self.expire(now);
        self.next_at.is_some_and(|at| now >= at)
    }
    pub(crate) fn remaining(&self) -> u64 {
        self.observation
            .map(|o| o.max_protected_bytes.saturating_sub(o.sent_protected_bytes))
            .unwrap_or(0)
    }
    pub(crate) fn sent(&mut self, now: Instant, bytes: usize) {
        let Some(o) = self.observation.as_mut() else {
            return;
        };
        debug_assert!(
            o.status == PaddingStatus::Active
                && bytes <= MAX_PADDING_PACKET
                && bytes as u64 <= o.max_protected_bytes - o.sent_protected_bytes
        );
        o.sent_protected_bytes += bytes as u64;
        o.sent_packets += 1;
        o.first_output_at.get_or_insert(now);
        o.last_output_at = Some(now);
        let burst_count = if o.mode == PaddingMode::Burst {
            let count = match self.burst_at {
                Some((at, count)) if at == now => count + 1,
                _ => 1,
            };
            self.burst_at = Some((now, count));
            Some(count)
        } else {
            None
        };
        if o.max_protected_bytes - o.sent_protected_bytes < MAX_PADDING_PACKET as u64 {
            self.stop(PaddingStatus::Completed);
        } else if let Some(count) = burst_count {
            self.next_at = Some(if count < 16 {
                now
            } else {
                now + Duration::from_millis(1)
            });
        } else {
            // No stored credit: scheduler stalls never authorize a catch-up burst.
            self.next_at = now.checked_add(Duration::from_nanos(
                (bytes as f64 * 8_000_000_000.0 / o.rate.as_f64())
                    .ceil()
                    .max(1.0) as u64,
            ));
        }
    }
    pub(crate) fn timeout(&self) -> Option<Instant> {
        let o = self.observation?;
        (o.status == PaddingStatus::Active)
            .then(|| self.next_at.unwrap_or(o.deadline).min(o.deadline))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn bounded_execution_expires_cancels_without_reusing_identity_or_fake_delivery() {
        let now = Instant::now();
        let mut p = PaddingExecutor::default();
        let id = p
            .start(2048, Bitrate::bps(100_000), Duration::from_secs(1), now)
            .unwrap();
        assert_eq!(
            p.start(2048, Bitrate::bps(100_000), Duration::from_secs(1), now),
            Err(PaddingRequestError::Busy)
        );
        assert_eq!(p.observation().unwrap().sent_packets, 0);
        assert!(p.cancel(id));
        assert!(p.observation().unwrap().first_output_at.is_none());
        assert!(p.observation().unwrap().last_output_at.is_none());
        assert!(!p.cancel(id));
        let newer = p
            .start(2048, Bitrate::bps(100_000), Duration::from_secs(1), now)
            .unwrap();
        assert!(newer > id);
        assert!(!p.cancel(id));
        assert!(!p.due(now + Duration::from_secs(1)));
        assert_eq!(p.observation().unwrap().status, PaddingStatus::Expired);
        assert_eq!(p.observation().unwrap().sent_protected_bytes, 0);
        assert!(p.observation().unwrap().first_output_at.is_none());
        assert!(p.observation().unwrap().last_output_at.is_none());
    }
    #[test]
    fn actual_output_budget_is_finite_and_stall_credit_is_not_accumulated() {
        let now = Instant::now();
        let mut p = PaddingExecutor::default();
        p.start(1024, Bitrate::bps(80_000), Duration::from_secs(2), now)
            .unwrap();
        assert!(p.due(now));
        p.sent(now, 256);
        assert!(!p.due(now));
        assert_eq!(p.timeout(), Some(now + Duration::from_micros(25_600)));
        let late = now + Duration::from_millis(500);
        assert!(p.due(late));
        p.sent(late, 256);
        assert!(!p.due(late));
        assert_eq!(p.timeout(), Some(late + Duration::from_micros(25_600)));
        p.sent(late + Duration::from_micros(25_600), 256);
        assert_eq!(p.observation().unwrap().status, PaddingStatus::Completed);
        assert_eq!(p.observation().unwrap().sent_protected_bytes, 768);
        assert_eq!(p.remaining(), 256);
        assert_eq!(p.observation().unwrap().first_output_at, Some(now));
        assert_eq!(
            p.observation().unwrap().last_output_at,
            Some(late + Duration::from_micros(25_600))
        );
    }
    #[test]
    fn invalid_limits_leave_current_request_unchanged() {
        let now = Instant::now();
        let mut p = PaddingExecutor::default();
        let id = p
            .start(2048, Bitrate::bps(100_000), Duration::from_secs(1), now)
            .unwrap();
        for (bytes, rate, duration) in [
            (511, 100_000, 1),
            (17 * 1024 * 1024, 100_000, 1),
            (2048, 0, 1),
            (2048, 100_000_001, 1),
            (2048, 100_000, 6),
        ] {
            assert_eq!(
                p.start(
                    bytes,
                    Bitrate::bps(rate),
                    Duration::from_secs(duration),
                    now
                ),
                Err(PaddingRequestError::InvalidLimits)
            );
            assert_eq!(p.observation().unwrap().id, id);
        }
    }
}

#[cfg(test)]
mod burst_tests {
    use super::*;
    #[test]
    fn actual_budget_and_fair_yield_hold_across_same_clock_request_generations() {
        let now = Instant::now();
        let mut e = PaddingExecutor::default();
        for _ in 0..16 {
            let id = e.start_burst(512, Duration::from_secs(1), now).unwrap();
            assert!(e.due(now));
            e.sent(now, 512);
            let o = e.observation().unwrap();
            assert_eq!(o.id, id);
            assert_eq!(o.sent_protected_bytes, 512);
            assert_eq!(o.status, PaddingStatus::Completed);
        }
        e.start_burst(1024, Duration::from_secs(1), now).unwrap();
        assert!(!e.due(now));
        assert!(e.due(now + Duration::from_millis(1)));
        e.sent(now + Duration::from_millis(1), 512);
        assert_eq!(e.observation().unwrap().sent_protected_bytes, 512);
    }
    #[test]
    fn old_paced_api_does_not_gain_burst_credit() {
        let now = Instant::now();
        let mut e = PaddingExecutor::default();
        e.start(4096, Bitrate::bps(100_000), Duration::from_secs(1), now)
            .unwrap();
        e.sent(now, 256);
        assert!(!e.due(now));
        assert_eq!(e.observation().unwrap().mode, PaddingMode::Paced);
    }
}
