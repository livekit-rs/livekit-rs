//! Fixed Participant.UpdateMediaRTT: lifetime state independent of signal RTT.
use super::*;
#[derive(Debug)]
pub(super) struct MediaRtt {
    updated_at: Instant,
    value: u32,
}
impl MediaRtt {
    pub(super) fn new(now: Instant) -> Self {
        Self {
            updated_at: now,
            value: 0,
        }
    }
    fn observe(&mut self, rtt: u32, received_at: Instant) -> bool {
        if rtt == 0
            || self.value == rtt
            || received_at
                .checked_duration_since(self.updated_at)
                .is_none_or(|elapsed| elapsed < Duration::from_secs(5))
        {
            return false;
        }
        self.updated_at = received_at;
        self.value = rtt;
        true
    }
}
impl Room {
    pub(super) fn subscriber_rtt(
        &mut self,
        transport: TransportId,
        track: &TrackSid,
        rtt: u32,
        received_at: Instant,
    ) {
        let Some(participant) = self.participants.values_mut().find(|p| {
            !p.is_disconnected()
                && p.transports.iter().any(|(id, role)| {
                    *id == transport
                        && matches!(
                            role,
                            crate::media_api::PcRole::Subscriber | crate::media_api::PcRole::Single
                        )
                })
        }) else {
            return;
        };
        if participant
            .subscriptions
            .get(track)
            .is_some_and(|s| s.bound && !s.closing)
        {
            if participant.media_rtt.observe(rtt, received_at) {
                participant.fallback.media_rtt(rtt);
            }
        }
    }
    /// One actor turn binds both the response destination SID and media budget.
    pub fn rpc_participant(&self, identity: &Identity) -> Result<(ParticipantSid, u32), OpError> {
        let participant = self
            .by_identity
            .get(identity)
            .and_then(|sid| self.participants.get(sid))
            .filter(|p| !p.is_disconnected())
            .ok_or(OpError::NotFound("participant does not exist"))?;
        Ok((participant.info.sid.clone(), participant.media_rtt.value))
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn five_second_gate_equal_zero_and_reordered_reports_preserve_clock() {
        let now = Instant::now();
        let mut rtt = MediaRtt::new(now);
        rtt.observe(30, now + Duration::from_millis(4999));
        assert_eq!(rtt.value, 0);
        rtt.observe(30, now + Duration::from_secs(5));
        assert_eq!(rtt.value, 30);
        rtt.observe(30, now + Duration::from_secs(11));
        rtt.observe(40, now + Duration::from_secs(11));
        assert_eq!(rtt.value, 40);
        rtt.observe(0, now + Duration::from_secs(20));
        assert_eq!(rtt.value, 40);
        rtt.observe(50, now + Duration::from_secs(16));
        assert_eq!(rtt.value, 50);
        rtt.observe(90, now + Duration::from_secs(15));
        assert_eq!(rtt.value, 50);
        assert_eq!(MediaRtt::new(now + Duration::from_secs(30)).value, 0);
    }
}
