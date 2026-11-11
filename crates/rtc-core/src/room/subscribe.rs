//! Subscriptions, subscription permissions and track settings (signaling.md §5).

use std::time::Instant;

use super::participant::Subscription;
use super::*;
use crate::media_api::{DownTrackSpec, MediaCommand};
use crate::session::SubscriptionError;

impl Room {
    /// Subscribe to all tracks of others (auto-subscribe at join/ACTIVE).
    pub(super) fn subscribe_to_existing(&mut self, sid: &ParticipantSid, now: Instant) {
        let wants = self
            .participants
            .get(sid)
            .is_some_and(|p| p.auto_subscribe && p.info.permission.can_subscribe);
        if !wants {
            return;
        }
        let tracks: Vec<TrackSid> = self
            .track_owner
            .iter()
            .filter(|(_, owner)| *owner != sid)
            .map(|(t, _)| t.clone())
            .collect();
        for track in tracks {
            self.subscribe(sid, &track, now);
        }
    }

    /// Request a subscription. Records intent even if the track or permission is
    /// missing, so it resolves when they appear.
    pub(super) fn subscribe(&mut self, sid: &ParticipantSid, track_sid: &TrackSid, now: Instant) {
        if let Some(p) = self.participants.get_mut(sid) {
            // Go setDesired(true) resets the start even for repeated explicit requests.
            let entry = p.subscriptions.entry(track_sid.clone()).or_default();
            if !entry.desired {
                entry.attempt_reported = false;
                entry.event_reported = false;
            }
            entry.desired = true;
            entry.keep_until = None;
            if !entry.requested {
                entry.desired_since = Some(now);
            }
        }
        self.resolve_subscription_with_limit(sid, track_sid, now, true);
    }

    pub(super) fn resolve_subscription(
        &mut self,
        sid: &ParticipantSid,
        track_sid: &TrackSid,
        now: Instant,
    ) {
        self.resolve_subscription_with_limit(sid, track_sid, now, false);
    }

    fn resolve_subscription_with_limit(
        &mut self,
        sid: &ParticipantSid,
        track_sid: &TrackSid,
        now: Instant,
        report_limit: bool,
    ) {
        if let Some(sub) = self
            .participants
            .get_mut(sid)
            .and_then(|p| p.subscriptions.get_mut(track_sid))
        {
            if sub.desired && !sub.closing && !sub.requested && !sub.attempt_reported {
                self.lifecycle.track_subscribe_attempts += 1;
                sub.attempt_reported = true;
            }
        }
        let (subscriber_identity, can_subscribe, already, desired_since) =
            match self.participants.get_mut(sid) {
                Some(p) => {
                    let entry = p.subscriptions.entry(track_sid.clone()).or_default();
                    if !entry.desired || entry.closing {
                        return;
                    }
                    (
                        p.info.identity.clone(),
                        p.info.permission.can_subscribe,
                        entry.requested,
                        entry.desired_since,
                    )
                }
                None => return,
            };
        if already || !can_subscribe {
            return;
        }
        let publisher = self
            .track_owner
            .get(track_sid)
            .filter(|owner| {
                self.participants
                    .get(*owner)
                    .is_some_and(|p| p.published(track_sid).is_some())
            })
            .cloned();
        let Some(publisher) = publisher else {
            let interactive = self
                .participants
                .get(sid)
                .is_some_and(|p| !p.capabilities.one_shot);
            if interactive
                && desired_since.is_some_and(|start| {
                    now.saturating_duration_since(start) > Duration::from_secs(60)
                })
            {
                // The resolver reports a terminal missing-source failure. Go
                // eventSent is set before retirement, so this is not a cancel.
                if let Some(sub) = self
                    .participants
                    .get_mut(sid)
                    .and_then(|p| p.subscriptions.get_mut(track_sid))
                {
                    sub.event_reported = true;
                }
                self.unsubscribe(sid, track_sid, now);
                self.signal(
                    sid,
                    SignalEvent::SubscriptionResponse {
                        track_sid: track_sid.clone(),
                        error: SubscriptionError::TrackNotFound,
                    },
                );
            }
            return;
        };
        if &publisher == sid {
            return;
        }

        let allowed = self
            .participants
            .get(&publisher)
            .is_some_and(|pp| pp.allows_subscriber(track_sid, sid, &subscriber_identity));
        self.report_permission(sid, &publisher, track_sid, allowed);
        if !allowed {
            return;
        }

        if !self.within_subscription_limits(sid, track_sid) {
            // Source periodic retries do not emit a public error for capacity.
            // Retain the existing explicit-request response outside this slice.
            if report_limit {
                self.signal(
                    sid,
                    SignalEvent::SubscriptionResponse {
                        track_sid: track_sid.clone(),
                        error: SubscriptionError::Unknown,
                    },
                );
            }
            return;
        }

        // The subscriber must know the publisher's latest info first.
        let publisher_info = match self.participants.get_mut(&publisher) {
            Some(pp) => pp.snapshot(),
            None => return,
        };
        let seen = self
            .participants
            .get(sid)
            .and_then(|p| p.sent_versions.get(&publisher).copied());
        if seen.is_none_or(|v| v < publisher_info.version) {
            self.send_participant_updates(sid, vec![publisher_info.clone()]);
        }

        let Some(track) = publisher_info
            .tracks
            .iter()
            .find(|t| &t.sid == track_sid)
            .cloned()
        else {
            return;
        };
        let spec = {
            let p = self.participants.get(sid).expect("checked");
            let settings = p.track_settings.get(track_sid).cloned().unwrap_or_else(|| {
                if p.adaptive_stream && track.kind == TrackKind::Video {
                    TrackSettings {
                        quality: VideoQuality::Low,
                        ..Default::default()
                    }
                } else {
                    TrackSettings::default()
                }
            });
            DownTrackSpec {
                subscriber: sid.clone(),
                publisher: publisher.clone(),
                track_sid: track_sid.clone(),
                stream_id: self.stream_id_for(p, &publisher, &track),
                settings,
                adaptive_stream: p.adaptive_stream,
            }
        };
        if let Some(p) = self.participants.get_mut(sid)
            && let Some(sub) = p.subscriptions.get_mut(track_sid)
        {
            sub.requested = true;
            sub.desired_since = None;
        }
        self.subscription_monitors
            .entry((sid.clone(), track_sid.clone()))
            .or_insert(error_supervision::SubscriptionMonitor {
                started: now,
                failed: None,
                notified_at: None,
            });
        self.media(MediaCommand::Subscribe(spec));
        let _ = now;
    }

    /// Fixed subscriptionmanager.reconcileWorker: retry the current resolver cause,
    /// and only abandon missing tracks after strictly more than one minute.
    pub(super) fn reconcile_missing_subscriptions(&mut self, now: Instant) {
        let mut retry = Vec::new();
        for (sid, participant) in &mut self.participants {
            if now < participant.next_subscription_reconcile {
                continue;
            }
            let elapsed = now.saturating_duration_since(participant.next_subscription_reconcile);
            participant.next_subscription_reconcile +=
                Duration::from_secs((elapsed.as_secs() / 3 + 1) * 3);
            if participant.info.state == ParticipantState::Disconnected {
                continue;
            }
            let expired: Vec<_> = participant
                .subscriptions
                .iter()
                .filter(|(_, sub)| {
                    !sub.desired
                        && !sub.closing
                        && !sub.requested
                        && sub.keep_until.is_none_or(|until| now >= until)
                })
                .map(|(track, _)| track.clone())
                .collect();
            for track in expired {
                participant.subscriptions.remove(&track);
                participant.track_settings.remove(&track);
            }
            if participant.capabilities.one_shot {
                continue;
            }
            for (track, sub) in &participant.subscriptions {
                if !sub.requested && sub.desired_since.is_some() {
                    retry.push((sid.clone(), track.clone()));
                }
            }
        }
        for (sid, track) in retry {
            // Resolver preserves source order: CanSubscribe before track existence,
            // track existence before publisher permission and subscription limits.
            self.resolve_subscription(&sid, &track, now);
        }
    }

    pub(super) fn unsubscribe(&mut self, sid: &ParticipantSid, track_sid: &TrackSid, now: Instant) {
        self.subscription_monitors
            .remove(&(sid.clone(), track_sid.clone()));
        let (was_requested, one_shot, changed) = match self.participants.get_mut(sid) {
            Some(p) => {
                let Some(sub) = p.subscriptions.get_mut(track_sid) else {
                    return;
                };
                if sub.desired && !sub.event_reported {
                    self.lifecycle.track_subscribe_cancels += 1;
                    sub.event_reported = true;
                }
                let requested = sub.requested;
                let changed = sub.desired;
                sub.desired = false;
                sub.desired_since = None;
                sub.requested = false;
                sub.bound = false;
                sub.prepared = false;
                sub.closing |= requested;
                (requested, p.capabilities.one_shot, changed)
            }
            None => return,
        };
        if !one_shot && !changed {
            return;
        }
        if was_requested {
            self.media(MediaCommand::Unsubscribe {
                subscriber: sid.clone(),
                track_sid: track_sid.clone(),
            });
        }
        // Fixed unsubscribeSynchronous removes the whole entry immediately.
        if one_shot {
            if let Some(p) = self.participants.get_mut(sid) {
                p.subscriptions.remove(track_sid);
                p.track_settings.remove(track_sid);
            }
        } else {
            self.cleanup_subscription(sid, track_sid, now);
        }
    }

    fn cleanup_subscription(&mut self, sid: &ParticipantSid, track_sid: &TrackSid, now: Instant) {
        if let Some(p) = self.participants.get_mut(sid) {
            let cleanup = p.subscriptions.get(track_sid).is_some_and(|sub| {
                !sub.desired
                    && !sub.requested
                    && !sub.closing
                    && sub.keep_until.is_none_or(|until| now >= until)
            });
            if cleanup {
                p.subscriptions.remove(track_sid);
                p.track_settings.remove(track_sid);
            }
        }
    }

    pub(super) fn subscription_removed(
        &mut self,
        sid: &ParticipantSid,
        track_sid: &TrackSid,
        now: Instant,
    ) {
        let desired = match self
            .participants
            .get_mut(sid)
            .and_then(|p| p.subscriptions.get_mut(track_sid))
        {
            Some(sub) if sub.closing => {
                sub.closing = false;
                sub.desired
            }
            _ => return,
        };
        if desired {
            self.resolve_subscription(sid, track_sid, now);
        } else {
            self.cleanup_subscription(sid, track_sid, now);
        }
    }

    /// First resolution always reports; afterwards only changes (§5).
    fn report_permission(
        &mut self,
        sid: &ParticipantSid,
        publisher: &ParticipantSid,
        track_sid: &TrackSid,
        allowed: bool,
    ) {
        let changed = match self.participants.get_mut(sid) {
            Some(p) => {
                let sub = p
                    .subscriptions
                    .entry(track_sid.clone())
                    .or_insert_with(Subscription::default);
                let changed = sub.reported_allowed != Some(allowed);
                sub.reported_allowed = Some(allowed);
                changed
            }
            None => false,
        };
        if changed {
            self.signal(
                sid,
                SignalEvent::SubscriptionPermissionUpdate {
                    participant_sid: publisher.clone(),
                    track_sid: track_sid.clone(),
                    allowed,
                },
            );
        }
    }

    fn within_subscription_limits(&self, sid: &ParticipantSid, track_sid: &TrackSid) -> bool {
        let Some(p) = self.participants.get(sid) else {
            return false;
        };
        let kind = self
            .track_owner
            .get(track_sid)
            .and_then(|o| self.participants.get(o))
            .and_then(|o| o.published(track_sid))
            .map(|t| t.kind);
        let limit = match kind {
            Some(TrackKind::Video) => self.policy.subscription_limit_video,
            Some(TrackKind::Audio) => self.policy.subscription_limit_audio,
            _ => None,
        };
        let Some(limit) = limit else { return true };
        let count = p
            .subscriptions
            .iter()
            .filter(|(t, s)| {
                s.requested
                    && self
                        .track_owner
                        .get(*t)
                        .and_then(|o| self.participants.get(o))
                        .and_then(|o| o.published(t))
                        .map(|tr| tr.kind)
                        == kind
            })
            .count();
        (count as u32) < limit
    }

    /// Publisher replaced its subscription permission: revoke or grant.
    pub(super) fn set_subscription_permission(
        &mut self,
        publisher: &ParticipantSid,
        perm: SubscriptionPermission,
        now: Instant,
    ) {
        let tracks: Vec<TrackSid> = match self.participants.get_mut(publisher) {
            Some(p) => {
                p.subscription_permission = Some(perm);
                p.info.tracks.iter().map(|t| t.sid.clone()).collect()
            }
            None => return,
        };
        // Resolve sid-only entries to identities (upstream matches by identity).
        let sid_to_identity: Vec<(ParticipantSid, Identity)> = self
            .participants
            .values()
            .map(|p| (p.info.sid.clone(), p.info.identity.clone()))
            .collect();
        if let Some(p) = self.participants.get_mut(publisher)
            && let Some(perm) = p.subscription_permission.as_mut()
        {
            for tp in &mut perm.track_permissions {
                if tp.participant_identity.is_none()
                    && let Some(psid) = &tp.participant_sid
                {
                    tp.participant_identity = sid_to_identity
                        .iter()
                        .find(|(s, _)| s == psid)
                        .map(|(_, i)| i.clone());
                }
            }
        }

        let subscribers: Vec<(ParticipantSid, Identity)> = self
            .participants
            .iter()
            .filter(|(s, _)| *s != publisher)
            .map(|(s, p)| (s.clone(), p.info.identity.clone()))
            .collect();
        for track in tracks {
            for (sub_sid, sub_identity) in &subscribers {
                let wants = self
                    .participants
                    .get(sub_sid)
                    .and_then(|p| p.subscriptions.get(&track).map(|s| s.requested));
                let Some(requested) = wants else { continue };
                let allowed = self
                    .participants
                    .get(publisher)
                    .is_some_and(|pp| pp.allows_subscriber(&track, sub_sid, sub_identity));
                // A publisher-denied subscription has no active bind attempt. A later
                // grant creates a fresh monitor, as source setHasPermission(true) does.
                if !allowed {
                    self.subscription_monitors
                        .remove(&(sub_sid.clone(), track.clone()));
                }
                if requested && !allowed {
                    self.media(MediaCommand::Unsubscribe {
                        subscriber: sub_sid.clone(),
                        track_sid: track.clone(),
                    });
                    if let Some(p) = self.participants.get_mut(sub_sid)
                        && let Some(s) = p.subscriptions.get_mut(&track)
                    {
                        s.requested = false;
                        s.bound = false;
                        s.prepared = false;
                    }
                    self.report_permission(sub_sid, publisher, &track, false);
                } else if !requested && allowed {
                    self.resolve_subscription(sub_sid, &track, now);
                }
            }
        }
        self.reconcile_data_subscriptions();
    }

    /// Re-evaluate everything a participant can no longer subscribe to.
    pub(super) fn revoke_all_subscriptions(&mut self, sid: &ParticipantSid) {
        let tracks: Vec<TrackSid> = match self.participants.get(sid) {
            Some(p) => p
                .subscriptions
                .iter()
                .filter(|(_, s)| s.requested)
                .map(|(t, _)| t.clone())
                .collect(),
            None => return,
        };
        for t in tracks {
            if let Some(sub) = self
                .participants
                .get_mut(sid)
                .and_then(|p| p.subscriptions.get_mut(&t))
            {
                // Permission temporarily suppresses the requested subscription;
                // it does not erase the client's explicit subscription intent.
                sub.requested = false;
                sub.bound = false;
                sub.prepared = false;
            }
            self.media(MediaCommand::Unsubscribe {
                subscriber: sid.clone(),
                track_sid: t,
            });
        }
    }

    pub(super) fn update_track_settings(
        &mut self,
        sid: &ParticipantSid,
        track_sids: Vec<TrackSid>,
        settings: TrackSettings,
        now: Instant,
    ) {
        let mut to_media = Vec::new();
        if let Some(p) = self.participants.get_mut(sid) {
            for t in track_sids {
                let sub = p.subscriptions.entry(t.clone()).or_default();
                if !sub.desired {
                    sub.keep_until = Some(now + Duration::from_secs(60));
                }
                p.track_settings.insert(t.clone(), settings.clone());
                if p.subscriptions
                    .get(&t)
                    .is_some_and(|s| s.requested || s.closing)
                {
                    to_media.push(t);
                }
            }
        }
        for t in to_media {
            self.media(MediaCommand::UpdateTrackSettings {
                subscriber: sid.clone(),
                track_sid: t,
                settings: settings.clone(),
            });
        }
    }

    /// Media bound the down track: TrackSubscribed to the publisher once.
    pub(super) fn subscription_bound(&mut self, subscriber: &ParticipantSid, track_sid: &TrackSid) {
        let hidden = match self.participants.get_mut(subscriber) {
            Some(p) => {
                let Some(s) = p.subscriptions.get_mut(track_sid) else {
                    return;
                };
                if !s.requested || !p.info.permission.can_subscribe {
                    return;
                }
                if !s.bound {
                    self.lifecycle.track_subscribe_success += 1;
                }
                s.event_reported = true;
                // Fixed recordAttempt(true) resets the retry attempt counter;
                // a later real rebind may start another subscription attempt.
                s.attempt_reported = false;
                s.bound = true;
                p.is_hidden()
            }
            None => return,
        };
        if hidden {
            return;
        }
        let Some(publisher) = self.track_owner.get(track_sid).cloned() else {
            return;
        };
        let notify = match self.participants.get_mut(&publisher) {
            Some(pp) => {
                pp.capabilities.request_response
                    && pp.track_subscribed_sent.insert(track_sid.clone())
            }
            None => false,
        };
        if notify {
            self.signal(
                &publisher,
                SignalEvent::TrackSubscribed {
                    track_sid: track_sid.clone(),
                },
            );
        }
    }

    pub(super) fn subscription_failed(
        &mut self,
        subscriber: &ParticipantSid,
        track_sid: &TrackSid,
        error: SubscriptionError,
    ) {
        if let Some(watch) = self
            .subscription_monitors
            .get_mut(&(subscriber.clone(), track_sid.clone()))
        {
            watch.failed = Some(error);
        }
        if let Some(p) = self.participants.get_mut(subscriber)
            && let Some(s) = p.subscriptions.get_mut(track_sid)
        {
            // A real failure response is an outcome, not a user cancellation.
            s.event_reported = true;
            s.requested = false;
            s.bound = false;
            s.prepared = false;
        }
        self.signal(
            subscriber,
            SignalEvent::SubscriptionResponse {
                track_sid: track_sid.clone(),
                error,
            },
        );
    }
}

impl Room {
    /// Source SubscriptionManager.IsTrackNameSubscribed checks a real subscribed-track
    /// object, not final SDP binding. Requiring bound here deadlocks deferred answers.
    pub fn named_subscriptions_prepared(
        &self,
        sid: &ParticipantSid,
        generation: crate::ids::SessionGeneration,
        targets: &[crate::session::NamedSubscription],
    ) -> bool {
        let Some(subscriber) = self
            .participants
            .get(sid)
            .filter(|p| p.generation == generation && !p.is_disconnected())
        else {
            return false;
        };
        targets.iter().all(|target| {
            subscriber
                .subscriptions
                .iter()
                .any(|(track_sid, subscription)| {
                    if !subscription.requested || !subscription.prepared {
                        return false;
                    }
                    self.track_owner
                        .get(track_sid)
                        .and_then(|owner| self.participants.get(owner))
                        .is_some_and(|publisher| {
                            publisher.info.identity == target.publisher
                                && publisher.info.tracks.iter().any(|track| {
                                    track.sid == *track_sid && track.name == target.track_name
                                })
                        })
                })
        })
    }
}
