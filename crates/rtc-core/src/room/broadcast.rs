//! Participant/room updates, speakers and connection quality fan-out
//! (signaling.md §6, sfu-media.md §5–§6).

use std::time::Instant;

use super::*;
use crate::media_api::{AudioLevel, QualitySample};

/// `AudioLevelQuantization`.
const LEVEL_QUANTIZATION: f32 = 8.0;

impl Room {
    /// Deliver participant infos to one recipient with upstream filtering rules.
    pub(super) fn send_participant_updates(
        &mut self,
        recipient: &ParticipantSid,
        infos: Vec<ParticipantInfo>,
    ) {
        let Some(r) = self.participants.get_mut(recipient) else {
            return;
        };
        if r.is_disconnected() {
            return;
        }
        let mut out = Vec::with_capacity(infos.len());
        for info in infos {
            if &info.sid != recipient && info.permission.hidden {
                continue;
            }
            if let Some(seen) = r.sent_versions.get(&info.sid)
                && info.version < *seen
            {
                continue;
            }
            if !r.ready {
                let keep = r
                    .queued_updates
                    .get(&info.sid)
                    .is_none_or(|q| q.version <= info.version);
                if keep {
                    r.queued_updates.insert(info.sid.clone(), info);
                }
                continue;
            }
            r.sent_versions.insert(info.sid.clone(), info.version);
            out.push(info);
        }
        if !out.is_empty() {
            self.signal(recipient, SignalEvent::ParticipantUpdate(out));
        }
    }

    pub(super) fn flush_queued_updates(&mut self, sid: &ParticipantSid) {
        let queued: Vec<ParticipantInfo> = match self.participants.get_mut(sid) {
            Some(p) => std::mem::take(&mut p.queued_updates)
                .into_values()
                .collect(),
            None => return,
        };
        if !queued.is_empty() {
            self.send_participant_updates(sid, queued);
        }
    }

    /// Broadcast a participant's current info (upstream `broadcastParticipantState`).
    /// The participant itself always gets it now; hidden participants only
    /// themselves; others now if `immediate` or the participant publishes,
    /// otherwise at the next batch flush.
    pub(super) fn broadcast_participant(
        &mut self,
        sid: &ParticipantSid,
        immediate: bool,
        _now: Instant,
    ) {
        // These entrypoints represent participant changes, unlike read/batch flush paths.
        self.remember_participant_change(sid);
        let Some(p) = self.participants.get_mut(sid) else {
            return;
        };
        let info = p.snapshot();
        let hidden = p.is_hidden();
        let publisher = p.info.is_publisher;
        self.send_participant_updates(sid, vec![info.clone()]);
        if hidden {
            return;
        }
        if immediate || publisher {
            self.send_to_others(sid, info);
        } else if !self.batch_pending.contains(sid) {
            self.batch_pending.push(sid.clone());
        }
    }

    /// Broadcast to everyone except `sid` without touching `sid`'s own copy.
    pub(super) fn broadcast_to_others(&mut self, sid: &ParticipantSid) {
        // These entrypoints represent participant changes, unlike read/batch flush paths.
        self.remember_participant_change(sid);
        let Some(p) = self.participants.get_mut(sid) else {
            return;
        };
        if p.is_hidden() {
            return;
        }
        let info = p.snapshot();
        self.send_to_others(sid, info);
    }

    fn send_to_others(&mut self, sid: &ParticipantSid, info: ParticipantInfo) {
        // An immediate/publisher send dequeues that participant's pending
        // snapshot (fixed PushAndDequeueUpdates), so the next batch does not
        // repeat a callback already delivered through the immediate path.
        self.batch_pending.retain(|pending| pending != sid);
        let others: Vec<ParticipantSid> = self
            .participants
            .keys()
            .filter(|s| *s != sid)
            .cloned()
            .collect();
        for other in others {
            self.send_participant_updates(&other, vec![info.clone()]);
        }
    }

    pub(super) fn flush_batched_updates(&mut self) {
        let pending = std::mem::take(&mut self.batch_pending);
        let mut infos = Vec::with_capacity(pending.len());
        for sid in pending {
            if let Some(p) = self.participants.get_mut(&sid) {
                if !p.is_hidden() {
                    infos.push(p.snapshot());
                }
            }
        }
        // Fixed changeUpdateWorker sends one batch to every room participant,
        // including sources that already received their immediate self update.
        // Chunk before per-recipient ready/hidden/version filtering, matching
        // SendParticipantUpdates -> ChunkProtoBatch -> SendParticipantUpdate.
        let chunks = self.chunk_participant_updates(infos);
        let recipients: Vec<_> = self.participants.keys().cloned().collect();
        for recipient in recipients {
            for chunk in &chunks {
                self.send_participant_updates(&recipient, chunk.clone());
            }
        }
    }

    pub(super) fn broadcast_room_update(&mut self) {
        let room = self.info();
        let ready: Vec<ParticipantSid> = self
            .participants
            .iter()
            .filter(|(_, p)| p.ready && !p.is_disconnected())
            .map(|(s, _)| s.clone())
            .collect();
        for sid in ready {
            self.signal(&sid, SignalEvent::RoomUpdate(room.clone()));
        }
    }

    /// Active speaker deltas every `audio.update_interval`.
    pub(super) fn handle_audio_levels(&mut self, levels: Vec<AudioLevel>) {
        let mut active: Vec<(ParticipantSid, f32)> = levels
            .into_iter()
            .filter(|l| l.active && self.participants.contains_key(&l.participant))
            .map(|l| {
                (
                    l.participant,
                    (l.level * LEVEL_QUANTIZATION).ceil() / LEVEL_QUANTIZATION,
                )
            })
            .collect();
        active.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));

        let mut changed: Vec<SpeakerInfo> = Vec::new();
        for (sid, level) in &active {
            if self.speakers.get(sid) != Some(level) {
                changed.push(SpeakerInfo {
                    sid: sid.clone(),
                    level: *level,
                    active: true,
                });
            }
        }
        let stopped: Vec<ParticipantSid> = self
            .speakers
            .keys()
            .filter(|s| !active.iter().any(|(a, _)| a == *s))
            .cloned()
            .collect();
        for sid in stopped {
            changed.push(SpeakerInfo {
                sid,
                level: 0.0,
                active: false,
            });
        }
        self.speakers = active.into_iter().collect();
        if changed.is_empty() {
            return;
        }

        self.send_speaker_changes(changed);
    }

    /// Fixed Go Room.sendSpeakerChanges → ParticipantImpl.SendSpeakerUpdate(force=false):
    /// every ready participant that supports speaker changes receives the speakers it is
    /// subscribed to or itself. Go writes the update even when that scoped list is empty, and
    /// the official SDK emits ActiveSpeakersChanged for it, so an empty update is sent too.
    pub(super) fn send_speaker_changes(&mut self, changed: Vec<SpeakerInfo>) {
        let recipients: Vec<ParticipantSid> = self.participants.keys().cloned().collect();
        for r in recipients {
            let filtered: Vec<SpeakerInfo> = {
                let Some(p) = self.participants.get(&r) else {
                    continue;
                };
                if !p.ready || !p.capabilities.speaker_changed {
                    continue;
                }
                changed
                    .iter()
                    .filter(|s| s.sid == r || self.subscribes_to_participant(&r, &s.sid))
                    .cloned()
                    .collect()
            };
            self.signal(&r, SignalEvent::SpeakersChanged(filtered));
        }
    }

    /// Connection quality every 5 s, sent only when some quality enum changed.
    pub(super) fn handle_quality(&mut self, samples: Vec<QualitySample>) {
        let mut any_changed = false;
        let mut current: Vec<ConnectionQualityInfo> = Vec::new();
        for s in samples {
            let Some(p) = self.participants.get_mut(&s.participant) else {
                continue;
            };
            if !p.is_active() {
                continue;
            }
            if p.last_quality != Some(s.quality) {
                any_changed = true;
                p.last_quality = Some(s.quality);
            }
            current.push(ConnectionQualityInfo {
                participant_sid: s.participant,
                quality: s.quality,
                score: s.score,
            });
        }
        if !any_changed {
            return;
        }
        let recipients: Vec<ParticipantSid> = self.participants.keys().cloned().collect();
        for r in recipients {
            let updates: Vec<ConnectionQualityInfo> = {
                let Some(p) = self.participants.get(&r) else {
                    continue;
                };
                if !p.is_active() || !p.ready || !p.capabilities.connection_quality {
                    continue;
                }
                let lost_ok = p.capabilities.connection_quality_lost;
                current
                    .iter()
                    .filter(|q| {
                        q.participant_sid == r
                            || self.subscribes_to_participant(&r, &q.participant_sid)
                    })
                    .map(|q| {
                        let mut q = q.clone();
                        if !lost_ok && q.quality == ConnectionQuality::Lost {
                            q.quality = ConnectionQuality::Poor;
                        }
                        q
                    })
                    .collect()
            };
            if !updates.is_empty() {
                self.signal(&r, SignalEvent::ConnectionQuality(updates));
            }
        }
    }

    /// `subscriber` has a subscription to any track of `publisher`.
    pub(super) fn subscribes_to_participant(
        &self,
        subscriber: &ParticipantSid,
        publisher: &ParticipantSid,
    ) -> bool {
        let Some(s) = self.participants.get(subscriber) else {
            return false;
        };
        s.subscriptions
            .iter()
            .any(|(track, sub)| sub.requested && self.track_owner.get(track) == Some(publisher))
    }
}
