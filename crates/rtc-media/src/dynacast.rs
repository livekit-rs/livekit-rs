//! Dynacast: committed max subscribed quality per published video track and codec
//! (sfu-media.md §3; dynacastqualityvideo.go + dynacastmanagervideo.go).
//!
//! - aggregation: max over subscribers, OFF subscribers ignored, none → OFF;
//! - first aggregation (or the forced 10 s initial update) is committed even if
//!   unchanged;
//! - upgrades (and adding a codec) commit immediately; downgrades-only are debounced
//!   when `video.dynacast_pause_delay` is positive; returning to the committed quality
//!   leaves the original timer armed, and an immediate commit cancels it;
//! - while the publisher track is muted nothing is emitted; unmute forces an update.
//!
//! Each actual codec receiver owns this state; the engine combines codec updates
//! for the logical published track and migrates demands on codec regression.

use std::time::{Duration, Instant};

use rtc_core::model::{SubscribedCodec, SubscribedQuality, VideoQuality};

/// Forced first update after publish (`initialQualityUpdateWait`).
pub(crate) const INITIAL_UPDATE_WAIT: Duration = Duration::from_secs(10);

pub(crate) struct Dynacast {
    codec: String,
    pause_delay: Duration,
    /// Aggregated max subscribed quality; `None` until the first aggregation.
    aggregated: Option<VideoQuality>,
    committed: Option<VideoQuality>,
    debounce_at: Option<Instant>,
    initial_at: Option<Instant>,
    muted: bool,
}

/// `OFF` compares as "nothing"; otherwise LOW < MEDIUM < HIGH.
fn is_upgrade(from: VideoQuality, to: VideoQuality) -> bool {
    (from == VideoQuality::Off && to != VideoQuality::Off)
        || (from != VideoQuality::Off && to != VideoQuality::Off && from < to)
}

/// Max of subscriber demands, ignoring OFF (none → OFF).
pub(crate) fn aggregate(demands: impl IntoIterator<Item = VideoQuality>) -> VideoQuality {
    let mut max = VideoQuality::Off;
    for q in demands {
        if q == VideoQuality::Off {
            continue;
        }
        if max == VideoQuality::Off || q > max {
            max = q;
        }
    }
    max
}

impl Dynacast {
    pub(crate) fn new(codec: String, pause_delay: Duration, now: Instant, muted: bool) -> Self {
        Self {
            codec,
            pause_delay,
            aggregated: None,
            committed: None,
            debounce_at: None,
            initial_at: Some(now + INITIAL_UPDATE_WAIT),
            muted,
        }
    }

    pub(crate) fn committed_message(&self) -> Option<Vec<SubscribedCodec>> {
        self.committed.map(|quality| message(&self.codec, quality))
    }

    pub(crate) fn set_codec(&mut self, codec: String) {
        self.codec = codec;
    }

    pub(crate) fn next_deadline(&self) -> Option<Instant> {
        match (self.debounce_at, self.initial_at) {
            (Some(a), Some(b)) => Some(a.min(b)),
            (a, b) => a.or(b),
        }
    }

    /// New aggregated demand. Returns the update to send, if any.
    pub(crate) fn set_demand(
        &mut self,
        q: VideoQuality,
        now: Instant,
    ) -> Option<Vec<SubscribedCodec>> {
        if self.aggregated == Some(q) {
            return None;
        }
        self.aggregated = Some(q);
        self.update(false, now)
    }

    /// Timers: the 10 s initial forced update and the downgrade debounce.
    pub(crate) fn tick(&mut self, now: Instant) -> Option<Vec<SubscribedCodec>> {
        if self.initial_at.is_some_and(|t| t <= now) {
            self.initial_at = None;
            if self.aggregated.is_none() {
                self.aggregated = Some(VideoQuality::Off);
            }
            return self.update(true, now);
        }
        if self.debounce_at.is_some_and(|t| t <= now) {
            self.debounce_at = None;
            return self.update(true, now);
        }
        None
    }

    /// Publisher mute state. Unmute forces an update of the current demand.
    pub(crate) fn set_muted(&mut self, muted: bool, now: Instant) -> Option<Vec<SubscribedCodec>> {
        if self.muted == muted {
            return None;
        }
        self.muted = muted;
        if !muted && self.aggregated.is_some() {
            return self.update(true, now);
        }
        None
    }

    fn update(&mut self, force: bool, now: Instant) -> Option<Vec<SubscribedCodec>> {
        let q = self.aggregated?;
        let changed = self.committed != Some(q);
        // Adding the codec (first commit) counts as a change that is not a downgrade.
        let downgrade_only = match self.committed {
            Some(c) => !is_upgrade(c, q),
            None => false,
        };
        if !force {
            if !changed {
                return None;
            }
            if downgrade_only && !self.pause_delay.is_zero() {
                if self.debounce_at.is_none() {
                    self.debounce_at = Some(now + self.pause_delay);
                }
                return None;
            }
        }
        self.debounce_at = None;
        self.committed = Some(q);
        if self.muted {
            return None;
        }
        Some(message(&self.codec, q))
    }
}

/// Always three entries LOW, MEDIUM, HIGH with `enabled = q <= max` (OFF → none).
pub(crate) fn message(codec: &str, max: VideoQuality) -> Vec<SubscribedCodec> {
    let qualities = [VideoQuality::Low, VideoQuality::Medium, VideoQuality::High]
        .into_iter()
        .map(|quality| SubscribedQuality {
            quality,
            enabled: max != VideoQuality::Off && quality <= max,
        })
        .collect();
    vec![SubscribedCodec {
        codec: codec.to_string(),
        qualities,
    }]
}

#[cfg(test)]
mod tests {
    use super::*;
    use VideoQuality::*;

    fn enabled(m: &[SubscribedCodec]) -> Vec<bool> {
        m[0].qualities.iter().map(|q| q.enabled).collect()
    }

    #[test]
    fn aggregation_ignores_off() {
        assert_eq!(aggregate([Low, Off, Medium]), Medium);
        assert_eq!(aggregate([Off, Off]), Off);
        assert_eq!(aggregate(std::iter::empty()), Off);
    }

    #[test]
    fn first_demand_is_immediate_and_downgrades_debounced() {
        let t0 = Instant::now();
        let mut d = Dynacast::new("vp8".into(), Duration::from_secs(5), t0, false);
        let m = d.set_demand(High, t0).expect("first commit");
        assert_eq!(m[0].codec, "vp8");
        assert_eq!(enabled(&m), vec![true, true, true]);
        // downgrade waits
        assert!(d.set_demand(Low, t0 + Duration::from_secs(1)).is_none());
        assert!(d.tick(t0 + Duration::from_secs(5)).is_none());
        let m = d
            .tick(t0 + Duration::from_secs(6))
            .expect("debounced downgrade");
        assert_eq!(enabled(&m), vec![true, false, false]);
        // upgrade immediate
        let m = d.set_demand(Medium, t0 + Duration::from_secs(7)).unwrap();
        assert_eq!(enabled(&m), vec![true, true, false]);
        // the 10 s initial timer still fires once and force-resends the committed value
        let m = d.tick(t0 + INITIAL_UPDATE_WAIT).unwrap();
        assert_eq!(enabled(&m), vec![true, true, false]);
        assert!(d.next_deadline().is_none());
    }

    #[test]
    fn upgrade_cancels_pending_downgrade() {
        let t0 = Instant::now();
        let mut d = Dynacast::new("vp8".into(), Duration::from_secs(5), t0, false);
        d.set_demand(Medium, t0).unwrap();
        assert!(d.set_demand(Off, t0).is_none());
        assert!(d.debounce_at.is_some());
        let m = d.set_demand(High, t0).unwrap();
        assert_eq!(enabled(&m), vec![true, true, true]);
        assert!(d.debounce_at.is_none());
    }

    #[test]
    fn zero_pause_delay_commits_downgrade_without_timer_poll() {
        let now = Instant::now();
        let mut d = Dynacast::new("vp9".into(), Duration::ZERO, now, false);
        d.set_demand(High, now).unwrap();
        let msg = d.set_demand(Off, now).expect("immediate downgrade");
        assert_eq!(enabled(&msg), vec![false, false, false]);
        assert!(d.tick(now).is_none());
    }

    #[test]
    fn returning_to_committed_quality_keeps_pending_downgrade_deadline() {
        let now = Instant::now();
        let delay = Duration::from_secs(5);
        for next_downgrade in [false, true] {
            let mut d = Dynacast::new("vp9".into(), delay, now, false);
            d.set_demand(High, now).unwrap();
            assert!(d.set_demand(Low, now).is_none());
            assert!(d.set_demand(High, now + Duration::from_secs(1)).is_none());
            assert_eq!(d.next_deadline(), Some(now + delay));
            if next_downgrade {
                assert!(d.set_demand(Off, now + Duration::from_secs(2)).is_none());
            }
            assert!(d.tick(now + Duration::from_secs(4)).is_none());
            let msg = d.tick(now + delay).expect("original deadline");
            assert_eq!(enabled(&msg), vec![!next_downgrade; 3]);
        }
    }

    #[test]
    fn initial_forced_update_reports_off_without_subscribers() {
        let t0 = Instant::now();
        let mut d = Dynacast::new("h264".into(), Duration::from_secs(5), t0, false);
        assert!(d.tick(t0 + Duration::from_secs(9)).is_none());
        let m = d.tick(t0 + INITIAL_UPDATE_WAIT).unwrap();
        assert_eq!(enabled(&m), vec![false, false, false]);
    }

    #[test]
    fn muted_suppresses_and_unmute_forces() {
        let t0 = Instant::now();
        let mut d = Dynacast::new("vp8".into(), Duration::from_secs(5), t0, true);
        assert!(d.set_demand(High, t0).is_none());
        let m = d.set_muted(false, t0).unwrap();
        assert_eq!(enabled(&m), vec![true, true, true]);
    }
}
