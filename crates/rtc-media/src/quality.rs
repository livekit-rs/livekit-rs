//! Connection quality scoring (sfu-media.md §6.2, connectionquality/scorer.go).
//!
//! Loss-only plus layer distance (upstream defaults: RTT/jitter/bitrate scoring off).
//! Inputs come from str0m's 1 s `MediaIngressStats` / `MediaEgressStats`: the
//! per-interval packet delta and the loss fraction carried by the RTCP receiver
//! reports (sent by us for ingress, received from the client for egress).

use std::time::{Duration, Instant};

use rtc_core::model::ConnectionQuality;

pub(crate) const QUALITY_WINDOW: Duration = Duration::from_secs(5);

const MAX_SCORE: f64 = 100.0;
const MIN_SCORE: f64 = 30.0;
const INCREASE_FACTOR: f64 = 0.4;
const DECREASE_FACTOR: f64 = 0.8;
const DISTANCE_WEIGHT: f64 = 35.0;
const LOST_SCORE: f64 = 20.0;
const POOR_SCORE: f64 = 40.0;

/// Packet-loss weight by codec (connectionstats.go:405-430). Opus/RED with FEC
/// (our opus fmtp always carries `useinbandfec=1`) is divided by 1.5.
pub(crate) fn loss_weight(mime: &str) -> f64 {
    let m = mime.to_ascii_lowercase();
    if m == "audio/opus" {
        8.0 / 1.5
    } else if m == "audio/red" {
        4.0 / 1.5
    } else if m.starts_with("video/") {
        10.0
    } else {
        // PCMU/PCMA and unknown audio: no weight upstream (0) would hide loss; use opus.
        8.0
    }
}

pub(crate) struct Scorer {
    weight: f64,
    score: f64,
    // current window
    packets: u64,
    lost: f64,
    layer_distance_sum: f64,
    layer_samples: u32,
    last_packet_at: Option<Instant>,
    last_rtcp_at: Option<Instant>,
    muted: bool,
    paused: bool,
}

impl Scorer {
    pub(crate) fn new(mime: &str) -> Self {
        Self {
            weight: loss_weight(mime),
            score: MAX_SCORE,
            packets: 0,
            lost: 0.0,
            layer_distance_sum: 0.0,
            layer_samples: 0,
            last_packet_at: None,
            last_rtcp_at: None,
            muted: false,
            paused: false,
        }
    }

    pub(crate) fn set_mime(&mut self, mime: &str) {
        self.weight = loss_weight(mime);
    }

    /// One stats sample: `packets` new packets, `loss` fraction (0..=1) if a report
    /// was seen in that interval.
    pub(crate) fn add_sample(&mut self, packets: u64, loss: Option<f32>, now: Instant) {
        self.packets += packets;
        if packets > 0 {
            self.last_packet_at = Some(now);
        }
        if let Some(l) = loss {
            self.last_rtcp_at = Some(now);
            self.lost += l.clamp(0.0, 1.0) as f64 * packets as f64;
        }
    }

    pub(crate) fn mark_rtcp(&mut self, now: Instant) {
        self.last_rtcp_at = Some(now);
    }

    /// Down tracks: how many spatial layers below the wanted one we forward.
    pub(crate) fn add_layer_distance(&mut self, distance: i32) {
        self.layer_distance_sum += distance.max(0) as f64;
        self.layer_samples += 1;
    }

    pub(crate) fn set_muted(&mut self, muted: bool) {
        if muted && !self.muted && self.score > LOST_SCORE {
            // On mute the score jumps to max unless LOST.
            self.score = MAX_SCORE;
        }
        self.muted = muted;
    }

    /// Bandwidth pause: POOR immediately (scorer.go:518-538).
    pub(crate) fn set_paused(&mut self, paused: bool) {
        self.paused = paused;
        if paused {
            self.score = MIN_SCORE;
        }
    }

    /// Close the window and update the score.
    pub(crate) fn update(&mut self, now: Instant) -> f64 {
        let packets = std::mem::take(&mut self.packets);
        let lost = std::mem::take(&mut self.lost);
        let dist_sum = std::mem::take(&mut self.layer_distance_sum);
        let dist_n = std::mem::take(&mut self.layer_samples);
        if self.muted {
            return self.score;
        }
        if self.paused {
            self.score = MIN_SCORE;
            return self.score;
        }
        if packets == 0 {
            let rtcp_stale = self
                .last_rtcp_at
                .is_none_or(|t| now.saturating_duration_since(t) > QUALITY_WINDOW);
            self.score = if rtcp_stale { LOST_SCORE } else { POOR_SCORE };
            return self.score;
        }
        let loss_pct = lost * 100.0 / packets as f64;
        let packet_score = (MAX_SCORE - loss_pct * self.weight).max(0.0);
        let layer_score = if dist_n > 0 {
            (MAX_SCORE - (dist_sum / dist_n as f64) * DISTANCE_WEIGHT).clamp(0.0, MAX_SCORE)
        } else {
            MAX_SCORE
        };
        let raw = packet_score.min(layer_score);
        // Coming back from LOST/POOR-without-packets restarts from the raw score.
        let factor = if raw > self.score {
            INCREASE_FACTOR
        } else {
            DECREASE_FACTOR
        };
        self.score = (factor * raw + (1.0 - factor) * self.score).max(MIN_SCORE);
        self.score
    }

    #[cfg_attr(not(test), allow(dead_code))]
    pub(crate) fn score(&self) -> f64 {
        self.score
    }
}

/// `>80 EXCELLENT, >40 GOOD, >20 POOR, else LOST`.
pub(crate) fn quality_of(score: f64) -> ConnectionQuality {
    if score > 80.0 {
        ConnectionQuality::Excellent
    } else if score > 40.0 {
        ConnectionQuality::Good
    } else if score > 20.0 {
        ConnectionQuality::Poor
    } else {
        ConnectionQuality::Lost
    }
}

/// MOS: `1 + 0.035 s + 7e-6 s (s-60)(100-s)`, clamped to [1, 4.5].
pub(crate) fn mos(score: f64) -> f32 {
    let s = score.clamp(0.0, 100.0);
    let m = 1.0 + 0.035 * s + 0.000007 * s * (s - 60.0) * (100.0 - s);
    m.clamp(1.0, 4.5) as f32
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn thresholds_and_mos() {
        assert_eq!(quality_of(100.0), ConnectionQuality::Excellent);
        assert_eq!(quality_of(80.0), ConnectionQuality::Good);
        assert_eq!(quality_of(40.0), ConnectionQuality::Poor);
        assert_eq!(quality_of(20.0), ConnectionQuality::Lost);
        assert_eq!(mos(100.0), 4.5);
        assert!((mos(0.0) - 1.0).abs() < 1e-6);
        assert!(mos(80.0) > 3.5 && mos(80.0) < 4.5);
    }

    #[test]
    fn loss_lowers_score_fast_and_recovers_slowly() {
        let t0 = Instant::now();
        let mut s = Scorer::new("video/VP8");
        // 5 % loss → packet score 50
        s.add_sample(1000, Some(0.05), t0);
        let v = s.update(t0);
        assert!((v - (0.8 * 50.0 + 0.2 * 100.0)).abs() < 1e-6, "{v}");
        s.add_sample(1000, Some(0.0), t0);
        let w = s.update(t0);
        assert!((w - (0.4 * 100.0 + 0.6 * v)).abs() < 1e-6, "{w}");
    }

    #[test]
    fn floor_and_no_packets() {
        let t0 = Instant::now();
        let mut s = Scorer::new("audio/opus");
        s.add_sample(100, Some(1.0), t0);
        assert_eq!(s.update(t0), MIN_SCORE);
        // no packets, no recent RTCP → LOST
        assert_eq!(s.update(t0 + Duration::from_secs(10)), LOST_SCORE);
        assert_eq!(quality_of(s.score()), ConnectionQuality::Lost);
        // no packets but fresh RTCP → POOR
        s.mark_rtcp(t0 + Duration::from_secs(14));
        assert_eq!(s.update(t0 + Duration::from_secs(15)), POOR_SCORE);
    }

    #[test]
    fn mute_skips_updates() {
        let t0 = Instant::now();
        let mut s = Scorer::new("audio/opus");
        s.add_sample(100, Some(0.5), t0);
        s.update(t0);
        s.set_muted(true);
        assert_eq!(s.update(t0 + QUALITY_WINDOW), MAX_SCORE);
    }

    #[test]
    fn layer_distance_costs_a_level() {
        let t0 = Instant::now();
        let mut s = Scorer::new("video/VP8");
        s.add_sample(500, Some(0.0), t0);
        s.add_layer_distance(1);
        let v = s.update(t0);
        assert!((v - (0.8 * 65.0 + 0.2 * 100.0)).abs() < 1e-6, "{v}");
    }
}
