//! Per-codec demand and track-wide dynacast commit decisions.

use std::time::{Duration, Instant};

use rtc_core::model::{SubscribedCodec, SubscribedQuality, VideoQuality};

pub(crate) const INITIAL_UPDATE_WAIT: Duration = Duration::from_secs(10);

pub(crate) struct Dynacast {
    codec: String,
    pause_delay: Duration,
    aggregated: Option<VideoQuality>,
    initialized: bool,
    committed: Option<VideoQuality>,
    initial_at: Option<Instant>,
    // Go keeps the regressed-to mime's demand entry only once its own quality object
    // reports one, so a codec regression writes HIGH into that entry until the real
    // subscriber aggregate is computed.
    woken: bool,
    muted: bool,
}

#[derive(Default)]
pub(crate) struct Commit {
    pub deadline: Option<Instant>,
    // Synthetic backup entries belong in the payload, not the resend eligibility decision.
    pub snapshot: Vec<SubscribedCodec>,
    pub resend_eligible: bool,
}

impl Commit {
    pub fn update<'a>(
        &mut self,
        codecs: impl Iterator<Item = &'a Dynacast>,
        force: bool,
        now: Instant,
    ) -> bool {
        let mut delay = None;
        let mut changed = false;
        let mut upgrade = false;
        let mut resend_eligible = false;
        for codec in codecs {
            let Some(quality) = codec.aggregated else {
                continue;
            };
            resend_eligible |= quality != VideoQuality::High;
            delay.get_or_insert(codec.pause_delay);
            changed |= codec.committed != Some(quality);
            upgrade |= codec.committed.is_none_or(|old| is_upgrade(old, quality));
        }
        let Some(delay) = delay else {
            self.deadline = None;
            return false;
        };
        if !force {
            if !changed {
                return false;
            }
            if !upgrade && !delay.is_zero() {
                self.deadline.get_or_insert(now + delay);
                return false;
            }
        }
        self.deadline = None;
        self.resend_eligible = resend_eligible;
        true
    }
}

fn is_upgrade(from: VideoQuality, to: VideoQuality) -> bool {
    (from == VideoQuality::Off && to != VideoQuality::Off)
        || (from != VideoQuality::Off && to != VideoQuality::Off && from < to)
}

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
            initialized: false,
            committed: None,
            initial_at: Some(now + INITIAL_UPDATE_WAIT),
            woken: false,
            muted,
        }
    }

    /// Go `OnRegressCodec`: HIGH enters the demand map only when the regressed-to mime has
    /// no entry of its own, which is what tells the publisher to start that codec.
    pub(crate) fn wake_for_regression(&mut self) -> bool {
        if self.aggregated.is_some() {
            return false;
        }
        self.aggregated = Some(VideoQuality::High);
        self.woken = true;
        true
    }

    /// Go `dynacastQualityVideo.Replace` -> `Restart`: the transferred subscriber demands are
    /// not evaluated here, only the initial forced update is re-armed.
    pub(crate) fn reinitialize(&mut self, now: Instant) {
        self.initialized = false;
        self.initial_at = Some(now + INITIAL_UPDATE_WAIT);
    }

    /// The current aggregate is a regression wake-up rather than this codec's own demand.
    pub(crate) fn needs_recompute(&self) -> bool {
        self.woken
    }

    pub(crate) fn committed_message(&self) -> Option<Vec<SubscribedCodec>> {
        self.committed.map(|quality| message(&self.codec, quality))
    }

    pub(crate) fn set_codec(&mut self, codec: String) {
        self.codec = codec;
    }

    pub(crate) fn next_deadline(&self) -> Option<Instant> {
        self.initial_at
    }

    pub(crate) fn restart(&mut self, now: Instant) {
        self.committed = None;
        self.initialized = false;
        self.initial_at = Some(now + INITIAL_UPDATE_WAIT);
    }

    pub(crate) fn set_demand(&mut self, quality: VideoQuality) -> bool {
        if self.initialized && self.aggregated == Some(quality) {
            return false;
        }
        self.initialized = true;
        self.woken = false;
        self.aggregated = Some(quality);
        true
    }

    pub(crate) fn take_initial_update(&mut self, now: Instant) -> bool {
        if !self.initial_at.is_some_and(|at| at <= now) {
            return false;
        }
        self.initial_at = None;
        self.initialized = true;
        if self.woken {
            // Go's forced `updateQualityChange(true)` re-reads the codec's own subscribers, so
            // the injected wake-up must not survive as if it were a reported demand.
            self.woken = false;
            return true;
        }
        self.aggregated.get_or_insert(VideoQuality::Off);
        true
    }

    pub(crate) fn set_muted(&mut self, muted: bool) -> bool {
        if self.muted == muted {
            return false;
        }
        self.muted = muted;
        !muted && self.aggregated.is_some()
    }

    pub(crate) fn commit(&mut self) {
        self.committed = self.aggregated;
    }
}

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

    struct Driver {
        codec: Dynacast,
        shared: Commit,
    }

    impl Driver {
        fn new(delay: Duration, now: Instant, muted: bool) -> Self {
            Self {
                codec: Dynacast::new("vp8".into(), delay, now, muted),
                shared: Commit::default(),
            }
        }

        fn update(&mut self, force: bool, now: Instant) -> Option<Vec<SubscribedCodec>> {
            if !self.shared.update(std::iter::once(&self.codec), force, now) {
                return None;
            }
            self.codec.commit();
            if self.codec.muted {
                None
            } else {
                self.codec.committed_message()
            }
        }

        fn demand(&mut self, quality: VideoQuality, now: Instant) -> Option<Vec<SubscribedCodec>> {
            if !self.codec.set_demand(quality) {
                return None;
            }
            self.update(false, now)
        }

        fn tick(&mut self, now: Instant) -> Option<Vec<SubscribedCodec>> {
            let initial = if self.codec.take_initial_update(now) {
                self.update(false, now)
            } else {
                None
            };
            if self.shared.deadline.is_some_and(|at| at <= now) {
                self.update(true, now)
            } else {
                initial
            }
        }
    }

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
        let now = Instant::now();
        let mut d = Driver::new(Duration::from_secs(5), now, false);
        let msg = d.demand(High, now).unwrap();
        assert_eq!(msg[0].codec, "vp8");
        assert_eq!(enabled(&msg), vec![true; 3]);
        assert!(d.demand(Low, now + Duration::from_secs(1)).is_none());
        assert!(d.tick(now + Duration::from_secs(5)).is_none());
        assert_eq!(
            enabled(&d.tick(now + Duration::from_secs(6)).unwrap()),
            vec![true, false, false]
        );
        assert_eq!(
            enabled(&d.demand(Medium, now + Duration::from_secs(7)).unwrap()),
            vec![true, true, false]
        );
        assert!(d.tick(now + INITIAL_UPDATE_WAIT).is_none());
        assert!(d.codec.next_deadline().is_none() && d.shared.deadline.is_none());
    }

    #[test]
    fn upgrade_cancels_pending_downgrade() {
        let now = Instant::now();
        let mut d = Driver::new(Duration::from_secs(5), now, false);
        d.demand(Medium, now).unwrap();
        assert!(d.demand(Off, now).is_none());
        assert!(d.shared.deadline.is_some());
        assert_eq!(enabled(&d.demand(High, now).unwrap()), vec![true; 3]);
        assert!(d.shared.deadline.is_none());
    }

    #[test]
    fn zero_pause_delay_commits_downgrade_without_timer_poll() {
        let now = Instant::now();
        let mut d = Driver::new(Duration::ZERO, now, false);
        d.demand(High, now).unwrap();
        assert_eq!(enabled(&d.demand(Off, now).unwrap()), vec![false; 3]);
        assert!(d.tick(now).is_none());
    }

    #[test]
    fn returning_to_committed_quality_keeps_pending_downgrade_deadline() {
        let now = Instant::now();
        let delay = Duration::from_secs(5);
        for next_downgrade in [false, true] {
            let mut d = Driver::new(delay, now, false);
            d.demand(High, now).unwrap();
            assert!(d.demand(Low, now).is_none());
            assert!(d.demand(High, now + Duration::from_secs(1)).is_none());
            assert_eq!(d.shared.deadline, Some(now + delay));
            if next_downgrade {
                assert!(d.demand(Off, now + Duration::from_secs(2)).is_none());
            }
            assert!(d.tick(now + Duration::from_secs(4)).is_none());
            assert_eq!(
                enabled(&d.tick(now + delay).unwrap()),
                vec![!next_downgrade; 3]
            );
        }
    }

    #[test]
    fn initial_forced_update_reports_off_without_subscribers() {
        let now = Instant::now();
        let mut d = Driver::new(Duration::from_secs(5), now, false);
        assert!(d.tick(now + Duration::from_secs(9)).is_none());
        assert_eq!(
            enabled(&d.tick(now + INITIAL_UPDATE_WAIT).unwrap()),
            vec![false; 3]
        );
    }

    #[test]
    fn restart_preserves_demand_pause_mute_and_reinitializes_same_quality() {
        let now = Instant::now();
        let delay = Duration::from_secs(1);
        let mut d = Driver::new(delay, now, true);
        assert!(!d.codec.initialized);
        assert!(d.demand(Low, now).is_none());
        assert!(d.codec.initialized);
        assert_eq!(d.codec.committed, Some(Low));
        let restart = now + Duration::from_millis(100);
        d.codec.restart(restart);
        assert_eq!(d.codec.aggregated, Some(Low));
        assert_eq!(d.codec.committed, None);
        assert_eq!(d.codec.pause_delay, delay);
        assert!(d.codec.muted);
        assert!(!d.codec.initialized);
        assert_eq!(d.codec.next_deadline(), Some(restart + INITIAL_UPDATE_WAIT));
        assert!(
            d.codec.set_demand(Low),
            "same demand must reenter the manager"
        );
        assert!(d.codec.initialized);
        assert!(d.update(false, restart).is_none());
        assert_eq!(d.codec.committed, Some(Low));
        assert!(!d.codec.set_demand(Low));
        d.codec.restart(restart);
        assert!(d.codec.take_initial_update(restart + INITIAL_UPDATE_WAIT));
        assert!(d.codec.initialized);
        assert!(!d.codec.set_demand(Low));
    }

    #[test]
    fn regression_wake_publishes_high_and_the_forced_update_replaces_it() {
        let now = Instant::now();
        let delay = Duration::from_secs(1);
        let mut d = Driver::new(delay, now, false);
        assert!(
            d.codec.wake_for_regression(),
            "a codec with no demand of its own can be woken"
        );
        assert_eq!(d.codec.aggregated, Some(High));
        assert_eq!(enabled(&d.update(false, now).unwrap()), vec![true; 3]);
        assert!(
            !d.codec.wake_for_regression(),
            "a second regression must not invent another demand"
        );
        let at = now + INITIAL_UPDATE_WAIT;
        d.codec.reinitialize(at);
        assert_eq!(d.codec.committed, Some(High));
        assert_eq!(d.codec.next_deadline(), Some(at + INITIAL_UPDATE_WAIT));
        assert!(d.codec.take_initial_update(at + INITIAL_UPDATE_WAIT));
        assert!(
            !d.codec.needs_recompute(),
            "the forced update owns the aggregate from now on"
        );
        assert!(
            d.demand(Low, at + INITIAL_UPDATE_WAIT).is_none(),
            "replacing HIGH with a real demand is a debounced downgrade"
        );
        assert_eq!(
            enabled(&d.tick(at + INITIAL_UPDATE_WAIT + delay).unwrap()),
            vec![true, false, false]
        );
        assert!(
            !d.codec.wake_for_regression(),
            "a reported demand blocks the wake-up"
        );
    }

    #[test]
    fn muted_suppresses_and_unmute_forces() {
        let now = Instant::now();
        let mut d = Driver::new(Duration::from_secs(5), now, true);
        assert!(d.demand(High, now).is_none());
        assert_eq!(d.codec.committed, Some(High));
        assert!(d.codec.set_muted(false));
        assert_eq!(enabled(&d.update(true, now).unwrap()), vec![true; 3]);
    }
}
