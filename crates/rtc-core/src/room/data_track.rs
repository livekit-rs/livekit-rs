//! Data-track lifecycle and raw-channel routing, matching pinned participant_data_track.go.
use std::collections::BTreeMap;
use std::time::Instant;

use super::participant::DataSubscription;
use super::*;
use crate::session::{DataTrackSubscriptionUpdate, PublishDataTrack, RequestOutcome};

impl Room {
    pub(super) fn publish_data_track(
        &mut self,
        sid: &ParticipantSid,
        request: PublishDataTrack,
        now: Instant,
    ) {
        let Some(p) = self.participants.get(sid) else {
            return;
        };
        let error = if !p.info.permission.can_publish_data || !self.policy.enable_data_tracks {
            Some((
                RequestOutcome::NotAllowed,
                "does not have permission to publish data",
            ))
        } else if request.pub_handle == 0 || request.pub_handle > u16::MAX as u32 {
            Some((
                RequestOutcome::InvalidHandle,
                "handle should be > 0 AND < 65536",
            ))
        } else if request.name.is_empty() || request.name.len() > 256 {
            Some((
                RequestOutcome::InvalidName,
                "name should not be empty and should not exceed 256 characters",
            ))
        } else if !self.valid_data_encoding(request.frame_encoding.as_ref())
            || !self.valid_data_encoding(request.schema.as_ref().and_then(|s| s.encoding.as_ref()))
        {
            Some((
                RequestOutcome::InvalidRequest,
                "encoding identifier is empty or exceeds the maximum length",
            ))
        } else {
            p.info.data_tracks.iter().find_map(|track| {
                if track.pub_handle == request.pub_handle {
                    Some((
                        RequestOutcome::DuplicateHandle,
                        "a data track with same handle already exists",
                    ))
                } else if track.name == request.name {
                    Some((
                        RequestOutcome::DuplicateName,
                        "a data track with same name already exists",
                    ))
                } else {
                    None
                }
            })
        };
        if let Some((outcome, message)) = error {
            self.request_response(sid, request.token, 0, outcome, message);
            return;
        }
        let info = DataTrackInfo {
            sid: TrackSid(crate::ids::new_sid("DTR_")),
            pub_handle: request.pub_handle,
            name: request.name,
            encryption: request.encryption,
            frame_encoding: request.frame_encoding,
            schema: request.schema,
        };
        if let Some(p) = self.participants.get_mut(sid) {
            p.info.data_tracks.push(info.clone());
            p.info.is_publisher = true;
            p.mark_dirty();
        }
        self.signal(sid, SignalEvent::DataTrackPublished(info.clone()));
        self.broadcast_participant(sid, false, now);
        self.schedule_room_update(now);
        let subscribers: Vec<_> = self
            .participants
            .iter()
            .filter(|(other, p)| {
                *other != sid
                    && p.is_active()
                    && p.auto_subscribe_data_track
                    && !p.data_subscriptions.contains_key(&info.sid)
            })
            .map(|(other, _)| other.clone())
            .collect();
        for subscriber in subscribers {
            self.update_data_subscriptions(
                &subscriber,
                vec![DataTrackSubscriptionUpdate {
                    track_sid: info.sid.clone(),
                    subscribe: true,
                    target_fps: None,
                }],
            );
        }
        self.reconcile_data_subscriptions();
    }

    fn valid_data_encoding(&self, encoding: Option<&DataTrackEncoding>) -> bool {
        match encoding {
            Some(DataTrackEncoding::Custom(value)) => {
                !value.is_empty()
                    && (self.policy.max_data_track_custom_encoding_length == 0
                        || i64::try_from(value.len()).is_ok_and(|length| {
                            length <= self.policy.max_data_track_custom_encoding_length
                        }))
            }
            _ => true,
        }
    }

    pub(super) fn unpublish_data_track(
        &mut self,
        sid: &ParticipantSid,
        token: u32,
        pub_handle: u32,
        now: Instant,
    ) {
        // The pinned implementation narrows to uint16 before lookup, including unpublish.
        let handle = pub_handle as u16 as u32;
        let info = self
            .participants
            .get(sid)
            .and_then(|p| p.info.data_tracks.iter().find(|t| t.pub_handle == handle))
            .cloned();
        let Some(info) = info else {
            self.request_response(sid, token, 0, RequestOutcome::NotFound, "");
            return;
        };
        if let Some(p) = self.participants.get_mut(sid) {
            p.info.data_tracks.retain(|t| t.sid != info.sid);
            p.mark_dirty();
        }
        self.signal(sid, SignalEvent::DataTrackUnpublished(info.clone()));
        let affected: Vec<_> = self
            .participants
            .iter_mut()
            .filter_map(|(subscriber, p)| {
                p.data_subscriptions
                    .remove(&info.sid)
                    .map(|_| subscriber.clone())
            })
            .collect();
        for subscriber in affected {
            self.send_data_handles(&subscriber);
        }
        self.broadcast_participant(sid, false, now);
        self.schedule_room_update(now);
    }

    pub(super) fn remove_published_data_tracks(&mut self, sid: &ParticipantSid, now: Instant) {
        let handles: Vec<_> = self
            .participants
            .get(sid)
            .map(|p| p.info.data_tracks.iter().map(|t| t.pub_handle).collect())
            .unwrap_or_default();
        for handle in handles {
            self.unpublish_data_track(sid, 0, handle, now);
        }
    }

    pub(super) fn subscribe_existing_data_tracks(&mut self, sid: &ParticipantSid) {
        if !self
            .participants
            .get(sid)
            .is_some_and(|p| p.auto_subscribe_data_track)
        {
            return;
        }
        let existing = self
            .participants
            .get(sid)
            .map(|p| {
                p.data_subscriptions
                    .keys()
                    .cloned()
                    .collect::<std::collections::HashSet<_>>()
            })
            .unwrap_or_default();
        let tracks: Vec<_> = self
            .participants
            .iter()
            .filter(|(publisher, _)| *publisher != sid)
            .flat_map(|(_, p)| p.info.data_tracks.iter().map(|t| t.sid.clone()))
            .filter(|track| !existing.contains(track))
            .collect();
        self.update_data_subscriptions(
            sid,
            tracks
                .into_iter()
                .map(|track_sid| DataTrackSubscriptionUpdate {
                    track_sid,
                    subscribe: true,
                    target_fps: None,
                })
                .collect(),
        );
    }

    pub(super) fn update_data_subscriptions(
        &mut self,
        sid: &ParticipantSid,
        updates: Vec<DataTrackSubscriptionUpdate>,
    ) {
        let Some(p) = self.participants.get_mut(sid) else {
            return;
        };
        let mut removed = false;
        for update in updates {
            if update.subscribe {
                p.data_subscriptions
                    .entry(update.track_sid)
                    .and_modify(|s| s.target_fps = update.target_fps)
                    .or_insert(DataSubscription {
                        handle: None,
                        target_fps: update.target_fps,
                    });
            } else {
                removed |= p.data_subscriptions.remove(&update.track_sid).is_some();
            }
        }
        self.reconcile_data_subscriptions();
        if removed {
            self.send_data_handles(sid);
        }
    }

    pub(super) fn reconcile_data_subscriptions(&mut self) {
        let candidates: Vec<_> = self
            .participants
            .iter()
            .flat_map(|(subscriber, p)| {
                p.data_subscriptions
                    .keys()
                    .map(|track| (subscriber.clone(), track.clone()))
                    .collect::<Vec<_>>()
            })
            .collect();
        let mut changed = Vec::new();
        for (subscriber, track) in candidates {
            let allowed = self.participants.get(&subscriber).is_some_and(|sp| {
                sp.info.permission.can_subscribe
                    && !sp.is_disconnected()
                    && self.participants.iter().any(|(publisher, pp)| {
                        publisher != &subscriber
                            && !pp.is_disconnected()
                            && pp.info.data_tracks.iter().any(|t| t.sid == track)
                            && pp.allows_subscriber(&track, &subscriber, &sp.info.identity)
                    })
            });
            if let Some(p) = self.participants.get_mut(&subscriber) {
                let bound = p
                    .data_subscriptions
                    .get(&track)
                    .and_then(|s| s.handle)
                    .is_some();
                if allowed && !bound {
                    p.next_data_handle = p.next_data_handle.wrapping_add(1);
                    if p.next_data_handle == 0 {
                        p.next_data_handle = 1;
                    }
                    let handle = p.next_data_handle;
                    if let Some(s) = p.data_subscriptions.get_mut(&track) {
                        s.handle = Some(handle);
                    }
                    changed.push(subscriber);
                } else if !allowed && bound {
                    if let Some(s) = p.data_subscriptions.get_mut(&track) {
                        s.handle = None;
                    }
                    changed.push(subscriber);
                }
            }
        }
        changed.sort();
        changed.dedup();
        for subscriber in changed {
            self.send_data_handles(&subscriber);
        }
    }

    pub(super) fn send_data_handles(&mut self, sid: &ParticipantSid) {
        let mut handles = BTreeMap::new();
        if let Some(p) = self.participants.get(sid) {
            for (track, sub) in &p.data_subscriptions {
                let Some(handle) = sub.handle else { continue };
                if let Some((_, publisher)) = self
                    .participants
                    .iter()
                    .find(|(_, pp)| pp.info.data_tracks.iter().any(|t| &t.sid == track))
                {
                    handles.insert(
                        handle as u32,
                        DataTrackHandleInfo {
                            publisher_identity: publisher.info.identity.clone(),
                            publisher_sid: publisher.info.sid.clone(),
                            track_sid: track.clone(),
                        },
                    );
                }
            }
        }
        self.signal(sid, SignalEvent::DataTrackSubscriberHandles(handles));
    }

    pub(super) fn data_track_received(&mut self, sid: &ParticipantSid, payload: &[u8]) {
        if !valid_packet(payload) {
            return;
        }
        let handle = u16::from_be_bytes([payload[2], payload[3]]) as u32;
        let Some(publisher) = self
            .participants
            .get(sid)
            .filter(|p| !p.is_disconnected() && p.info.permission.can_publish_data)
        else {
            return;
        };
        let Some(track) = publisher
            .info
            .data_tracks
            .iter()
            .find(|t| t.pub_handle == handle)
            .map(|t| t.sid.clone())
        else {
            return;
        };
        let recipients: Vec<_> = self
            .participants
            .iter()
            .filter(|(subscriber, p)| {
                *subscriber != sid
                    && p.is_active()
                    && p.info.permission.can_subscribe
                    && publisher.allows_subscriber(&track, subscriber, &p.info.identity)
            })
            .filter_map(|(subscriber, p)| {
                p.data_subscriptions
                    .get(&track)
                    .and_then(|s| s.handle)
                    .map(|handle| (subscriber.clone(), handle))
            })
            .collect();
        for (to, handle) in recipients {
            let mut packet = payload.to_vec();
            packet[2..4].copy_from_slice(&handle.to_be_bytes());
            self.media(MediaCommand::SendDataTrack {
                to,
                payload: packet,
            });
        }
    }
}

/// Validate the upstream twelve-byte header and each declared extension boundary.
fn valid_packet(packet: &[u8]) -> bool {
    if packet.len() < 12 {
        return false;
    }
    if packet[0] & 4 == 0 {
        return true;
    }
    if packet.len() < 14 {
        return false;
    }
    let end = 12 + (u16::from_be_bytes([packet[12], packet[13]]) as usize + 1) * 4;
    if end > packet.len() {
        return false;
    }
    let mut at = 14;
    while at < end {
        let id = packet[at];
        if id == 0 {
            return true;
        }
        if at + 2 > end {
            return false;
        }
        let size = packet[at + 1] as usize;
        at += 2;
        if at + size > end {
            return false;
        }
        at += size;
    }
    true
}
