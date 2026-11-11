// SPDX-FileCopyrightText: 2026 The Pion community <https://pion.ly>
// SPDX-License-Identifier: MIT
// Source: fixed Pion SCTP v1.12.0 association.go onRackAfterSACK,
// onRackTimeoutLocked and windowedmin.go. No socket or alternate runtime.
use super::*;

#[derive(Debug, Default)]
pub(super) struct SourceRackDelivery {
    pub newest: Option<(Instant, u32)>,
    pub rtts: Vec<Duration>,
    pub acked: Vec<u32>,
    pub next_measure: u32,
}

#[derive(Debug)]
pub(super) struct SourceRackAck {
    pub delivered_at: Option<Instant>,
    pub highest_delivered_tsn: u32,
    pub reordering_seen: bool,
    pub min_tsn_measure: Option<u32>,
    pub reo: Duration,
    pub min_rtt: Duration,
    pub srtt_ms: f64,
    pub keep_inflated: u32,
    pub deadline: Option<Instant>,
    pub order: VecDeque<u32>,
    window: VecDeque<(Instant, Duration)>,
}
impl Default for SourceRackAck {
    fn default() -> Self {
        Self {
            delivered_at: None,
            highest_delivered_tsn: 0,
            reordering_seen: false,
            min_tsn_measure: None,
            reo: Duration::ZERO,
            min_rtt: Duration::ZERO,
            srtt_ms: 0.0,
            keep_inflated: 0,
            deadline: None,
            order: VecDeque::new(),
            window: VecDeque::new(),
        }
    }
}
impl SourceRackAck {
    #[cfg(test)]
    pub fn window_len(&self) -> usize {
        self.window.len()
    }
    pub fn with_min_tsn(tsn: u32) -> Self {
        Self {
            min_tsn_measure: Some(tsn),
            ..Self::default()
        }
    }
    pub fn remove(&mut self, tsn: u32) {
        self.order.retain(|id| *id != tsn);
    }
    pub fn insert(&mut self, tsn: u32) {
        self.remove(tsn);
        self.order.push_back(tsn);
    }
    fn prune(&mut self, now: Instant) {
        // Source retains a sample exactly30s old (strict Before cutoff).
        while self
            .window
            .front()
            .is_some_and(|(at, _)| now.saturating_duration_since(*at) > Duration::from_secs(30))
        {
            self.window.pop_front();
        }
    }
    pub fn rtt(&mut self, now: Instant, value: Duration) {
        self.prune(now);
        while self.window.back().is_some_and(|(_, old)| *old >= value) {
            self.window.pop_back();
        }
        self.window.push_back((now, value));
        // Retain sub-millisecond source SRTT separately from native integer RTO.
        let rtt_ms = value.as_secs_f64() * 1000.0;
        self.srtt_ms = if self.srtt_ms == 0.0 {
            rtt_ms
        } else {
            (1.0 - 0.125) * self.srtt_ms + 0.125 * rtt_ms
        };
    }
    pub fn srtt_duration(&self) -> Option<Duration> {
        (self.srtt_ms > 0.0).then(|| Duration::from_nanos((self.srtt_ms * 1e6) as u64))
    }
    pub fn observe(
        &mut self,
        delivery: SourceRackDelivery,
        duplicate: bool,
        fast_recovery: bool,
        t3_running: bool,
        now: Instant,
    ) {
        self.min_tsn_measure = Some(delivery.next_measure);
        for tsn in delivery.acked {
            self.remove(tsn);
        }
        for rtt in delivery.rtts {
            self.rtt(now, rtt);
        }
        if let Some((sent, tsn)) = delivery.newest {
            if sna32lt(self.highest_delivered_tsn, tsn) {
                self.highest_delivered_tsn = tsn;
            } else {
                self.reordering_seen = true;
            }
            if self.delivered_at.is_none_or(|old| sent > old) {
                self.delivered_at = Some(sent);
            }
        }
        self.prune(now);
        if let Some((_, minimum)) = self.window.front() {
            if !minimum.is_zero() {
                self.min_rtt = *minimum;
            }
        }
        let base = self.min_rtt / 4; // fixed app never supplies a nonzero floor
        if !self.reordering_seen && (fast_recovery || t3_running) {
            self.reo = Duration::ZERO;
        } else if self.reo.is_zero() && !base.is_zero() {
            self.reo = base;
        }
        if duplicate && !self.min_rtt.is_zero() {
            self.reo = self.reo.saturating_add(base);
            self.keep_inflated = 16;
        }
        if !fast_recovery && self.keep_inflated > 0 {
            self.keep_inflated -= 1;
            if self.keep_inflated == 0 && !self.min_rtt.is_zero() {
                self.reo = base;
            }
        }
        if let Some(srtt) = self.srtt_duration() {
            self.reo = self.reo.min(srtt);
        }
    }
    pub fn arm(&mut self, now: Instant) {
        self.deadline = if !self.order.is_empty() {
            self.delivered_at.and_then(|delivered| {
                let duration = now
                    .saturating_duration_since(delivered)
                    .checked_add(self.reo)?;
                if duration.is_zero() {
                    None
                } else {
                    now.checked_add(duration)
                }
            })
        } else {
            None
        };
    }
}
