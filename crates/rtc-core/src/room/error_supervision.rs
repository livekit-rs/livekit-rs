// Source-derived from LiveKit v1.13.10 supervisor/publication_monitor.go and
// participant_supervisor.go. Copyright2023 LiveKit, Inc. Apache-2.0.
//! Actual room-owned publication supervision; explicit clock, no spawned timers.
use super::*;
use crate::session::SubscriptionError;

const PUBLISH_WAIT: Duration = Duration::from_secs(30);
const SUBSCRIPTION_WAIT: Duration = Duration::from_secs(15);
const RECONCILE_INTERVAL: Duration = Duration::from_secs(3);

#[derive(Debug)]
pub(super) struct PublicationMonitor {
    desired: VecDeque<bool>,
    connected: bool,
    muted: bool,
    published: bool,
    unmuted_at: Option<Instant>,
    errored: bool,
}
impl PublicationMonitor {
    pub fn new(connected: bool) -> Self {
        Self {
            desired: VecDeque::new(),
            connected,
            muted: false,
            published: false,
            unmuted_at: None,
            errored: false,
        }
    }
    fn maybe_start(&mut self, now: Instant) {
        if self.connected && !self.muted {
            self.unmuted_at = Some(now);
        }
    }
    fn update(&mut self, now: Instant) {
        let mut popped = false;
        while let Some(start) = self.desired.front().copied() {
            if (start && !self.published) || (!start && self.published) {
                if popped && start {
                    self.unmuted_at = None;
                    self.maybe_start(now);
                }
                return;
            }
            self.desired.pop_front();
            popped = true;
        }
    }
    pub fn pending(&mut self, now: Instant) {
        self.desired.push_back(true);
        self.desired.push_back(false);
        self.update(now);
    }
    pub fn connected(&mut self, now: Instant) {
        self.connected = true;
        self.maybe_start(now);
    }
    pub fn muted(&mut self, muted: bool, now: Instant) {
        self.muted = muted;
        self.maybe_start(now);
    }
    pub fn published(&mut self, value: bool, now: Instant) {
        self.published = value;
        self.update(now);
    }
    pub fn timed_out(&self, now: Instant) -> bool {
        self.desired.front() == Some(&true)
            && !self.muted
            && self
                .unmuted_at
                .is_some_and(|at| now.saturating_duration_since(at) > PUBLISH_WAIT)
    }
    pub fn check(&mut self, now: Instant) -> bool {
        let error = self.timed_out(now);
        let newly = error && !self.errored;
        self.errored = error;
        newly
    }
    pub fn idle(&self) -> bool {
        self.desired.is_empty() && !self.published
    }
}
#[derive(Debug)]
pub(super) struct SubscriptionMonitor {
    pub started: Instant,
    pub failed: Option<SubscriptionError>,
    pub notified_at: Option<Instant>,
}
impl Room {
    pub(super) fn publication_pending(
        &mut self,
        sid: &ParticipantSid,
        track: &TrackSid,
        muted: bool,
        now: Instant,
    ) {
        let connected = self.publisher_connected.contains(sid);
        let watch = self
            .publication_monitors
            .entry((sid.clone(), track.clone()))
            .or_insert_with(|| PublicationMonitor::new(connected));
        watch.pending(now);
        watch.muted(muted, now);
    }
    pub(super) fn publication_observed(
        &mut self,
        sid: &ParticipantSid,
        track: &TrackSid,
        now: Instant,
    ) {
        if let Some(w) = self
            .publication_monitors
            .get_mut(&(sid.clone(), track.clone()))
        {
            w.published(true, now);
        }
    }
    pub(super) fn publication_cleared(
        &mut self,
        sid: &ParticipantSid,
        track: &TrackSid,
        now: Instant,
    ) {
        if let Some(w) = self
            .publication_monitors
            .get_mut(&(sid.clone(), track.clone()))
        {
            w.published(false, now);
        }
    }
    pub(super) fn publication_cancelled(&mut self, sid: &ParticipantSid, track: &TrackSid) {
        let key = (sid.clone(), track.clone());
        if self
            .publication_monitors
            .get(&key)
            .is_some_and(|m| !m.published)
        {
            self.publication_monitors.remove(&key);
        }
    }
    pub(super) fn supervise_errors(&mut self, now: Instant) {
        if now < self.next_error_supervision {
            return;
        }
        self.next_error_supervision = now + Duration::from_secs(1);
        self.publication_monitors
            .retain(|(sid, _), m| self.participants.contains_key(sid) && !m.idle());
        let failed: Vec<_> = self
            .publication_monitors
            .iter_mut()
            .filter_map(|((sid, _), m)| m.check(now).then_some(sid.clone()))
            .collect();
        if self.policy.reconnect_on_publication_error {
            for sid in failed {
                self.full_reconnect(&sid, now);
            }
        }
        let mut errors = Vec::new();
        self.subscription_monitors.retain(|(sid, track), _| {
            self.participants
                .get(sid)
                .is_some_and(|p| p.subscriptions.contains_key(track))
        });
        for ((sid, track), watch) in &mut self.subscription_monitors {
            let Some(p) = self.participants.get(sid) else {
                continue;
            };
            let Some(sub) = p.subscriptions.get(track) else {
                continue;
            };
            // A historical media failure is not the current retry cause after
            // unpublication or authorization changes. Fixed Go reconcile resolves
            // track/permission again before classifying a subscription failure.
            let eligible = p.info.permission.can_subscribe
                && self.track_owner.get(track).is_some_and(|owner| {
                    owner != sid
                        && self.participants.get(owner).is_some_and(|publisher| {
                            publisher.published(track).is_some()
                                && publisher.allows_subscriber(track, sid, &p.info.identity)
                        })
                });
            if !eligible {
                watch.failed = None;
                watch.notified_at = None;
                continue;
            }
            if sub.bound {
                watch.failed = None;
                continue;
            }
            // This slice covers a real prepared-but-unbound downtrack and an
            // already observed fatal candidate. The general reconciliation/
            // missing-track60s retry profile remains a separate OPEN leaf.
            let failed = watch
                .failed
                .filter(|e| *e != SubscriptionError::TrackNotFound);
            let active_start = p.active_at.map(|at| at.max(watch.started));
            let due = failed
                .is_some_and(|_| now.saturating_duration_since(watch.started) > SUBSCRIPTION_WAIT)
                || (sub.prepared
                    && active_start
                        .is_some_and(|at| now.saturating_duration_since(at) > SUBSCRIPTION_WAIT));
            if due
                && watch
                    .notified_at
                    .is_none_or(|at| now.saturating_duration_since(at) >= RECONCILE_INTERVAL)
            {
                watch.notified_at = Some(now);
                errors.push((
                    sid.clone(),
                    track.clone(),
                    failed.unwrap_or(SubscriptionError::Unknown),
                ));
            }
        }
        for (sid, track, error) in errors {
            self.signal(
                &sid,
                SignalEvent::SubscriptionResponse {
                    track_sid: track,
                    error,
                },
            );
            if self.policy.reconnect_on_subscription_error {
                self.full_reconnect(&sid, now);
            }
        }
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn publication_strict_boundary_queue_mute_and_recovery() {
        let at = Instant::now();
        let mut m = PublicationMonitor::new(true);
        m.pending(at);
        m.muted(false, at);
        assert!(!m.check(at + PUBLISH_WAIT));
        assert!(m.check(at + PUBLISH_WAIT + Duration::from_nanos(1)));
        assert!(!m.check(at + Duration::from_secs(40)));
        m.published(true, at + Duration::from_secs(41));
        assert!(!m.check(at + Duration::from_secs(41)));
        m.pending(at + Duration::from_secs(42));
        m.published(false, at + Duration::from_secs(43));
        assert!(!m.check(at + Duration::from_secs(73)));
        assert!(m.check(at + Duration::from_secs(74)));
        m.muted(true, at + Duration::from_secs(75));
        assert!(!m.check(at + Duration::from_secs(90)));
        m.muted(false, at + Duration::from_secs(91));
        assert!(!m.check(at + Duration::from_secs(121)));
        assert!(m.check(at + Duration::from_secs(122)));
    }
}

#[cfg(test)]
mod actual_source_oracle {
    use super::*;
    #[test]
    #[ignore = "requires actual fixed Go PublicationMonitor output normalized with normalize_results.py"]
    fn fixed_go_monitor_clock_vectors() {
        type ClockAction = (u64, &'static str);
        type MonitorCase = (&'static str, bool, &'static [ClockAction]);
        let cases: &[MonitorCase] = &[
            (
                "strict-boundary",
                true,
                &[
                    (0, "pending"),
                    (0, "unmute"),
                    (30000000000, "check"),
                    (30000000001, "check"),
                    (40000000000, "check"),
                ],
            ),
            (
                "not-connected",
                false,
                &[
                    (0, "pending"),
                    (0, "unmute"),
                    (90000000000, "check"),
                    (91000000000, "connect"),
                    (121000000000, "check"),
                    (121000000001, "check"),
                ],
            ),
            (
                "mute-resets-on-unmute",
                true,
                &[
                    (0, "pending"),
                    (0, "mute"),
                    (60000000000, "check"),
                    (61000000000, "unmute"),
                    (91000000000, "check"),
                    (91000000001, "check"),
                ],
            ),
            (
                "actual-track-clears-pending",
                true,
                &[
                    (0, "pending"),
                    (0, "unmute"),
                    (1000000000, "publish"),
                    (100000000000, "check"),
                    (101000000000, "clear"),
                    (200000000000, "check"),
                ],
            ),
            (
                "queued-republish-rearms",
                true,
                &[
                    (0, "pending"),
                    (0, "unmute"),
                    (1000000000, "publish"),
                    (2000000000, "pending"),
                    (3000000000, "clear"),
                    (33000000000, "check"),
                    (33000000001, "check"),
                    (34000000000, "publish"),
                    (100000000000, "check"),
                    (101000000000, "clear"),
                ],
            ),
            (
                "repeated-mute-event",
                true,
                &[
                    (0, "pending"),
                    (0, "unmute"),
                    (20000000000, "unmute"),
                    (50000000000, "check"),
                    (50000000001, "check"),
                ],
            ),
            (
                "disconnected-preserves-unmuted-timestamp-source",
                true,
                &[
                    (0, "pending"),
                    (0, "unmute"),
                    (1000000000, "disconnect"),
                    (30000000001, "check"),
                    (32000000000, "connect"),
                    (62000000000, "check"),
                    (62000000001, "check"),
                ],
            ),
            (
                "publish-before-pending",
                true,
                &[
                    (0, "publish"),
                    (1000000000, "pending"),
                    (1000000000, "unmute"),
                    (40000000000, "check"),
                    (41000000000, "clear"),
                    (100000000000, "check"),
                ],
            ),
        ];
        let result = std::fs::read_to_string(
            std::env::var("RECONNECT_MONITOR_GO_RESULTS").expect("actual fixed Go TSV output"),
        )
        .unwrap();
        let rows: Vec<_> = result
            .lines()
            .map(|s| s.split('\t').collect::<Vec<_>>())
            .collect();
        let mut offset = 0;
        for &(name, connected, actions) in cases {
            let origin = Instant::now();
            let mut m = PublicationMonitor::new(connected);
            for (index, &(at_ns, op)) in actions.iter().enumerate() {
                let now = origin + Duration::from_nanos(at_ns);
                match op {
                    "pending" => m.pending(now),
                    "connect" => m.connected(now),
                    "disconnect" => {
                        m.connected = false;
                        m.maybe_start(now);
                    }
                    "mute" => m.muted(true, now),
                    "unmute" => m.muted(false, now),
                    "publish" => m.published(true, now),
                    "clear" => m.published(false, now),
                    "check" => {}
                    _ => panic!("unknown source action"),
                }
                let row = &rows[offset];
                offset += 1;
                assert_eq!(row.len(), 5);
                assert_eq!(row[0], name);
                assert_eq!(row[1].parse::<usize>().unwrap(), index);
                assert_eq!(row[2].parse::<bool>().unwrap(), m.timed_out(now));
                assert_eq!(row[3].parse::<bool>().unwrap(), m.idle());
                assert_eq!(row[4].parse::<bool>().unwrap(), m.published);
            }
        }
        assert_eq!(offset, rows.len());
        assert_eq!(offset, 51);
    }
}
