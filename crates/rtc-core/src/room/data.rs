//! Data packet routing (signaling.md §7).

use std::time::Instant;

use super::*;
use crate::data::{DataSender, DataVerdict};
use crate::media_api::{DataKind, MediaCommand};

impl Room {
    /// Inbound message from a participant's data channel.
    pub(super) fn data_received(
        &mut self,
        sid: &ParticipantSid,
        kind: DataKind,
        label: &str,
        payload: &[u8],
        now: Instant,
    ) {
        // Go counts each permitted detached data message before protobuf
        // decoding and replay suppression. A dropped/malformed application
        // payload is still traffic, while a denied/disconnected sender is not.
        if !self
            .participants
            .get(sid)
            .is_some_and(|p| !p.is_disconnected() && p.info.permission.can_publish_data)
        {
            return;
        }
        self.data_bytes_in = self.data_bytes_in.wrapping_add(payload.len() as u64);
        self.data_packets_in = self.data_packets_in.wrapping_add(1);
        if label == "_data_track" {
            self.data_track_received(sid, payload);
            return;
        }
        // A consumed RPC response or dropped payload still advances the reliable
        // stream. Replayed envelopes must not repeat adapter side effects.
        let envelope_sequence = if kind == DataKind::Reliable {
            self.data.reliable_sequence(label, payload)
        } else {
            0
        };
        if envelope_sequence > 0 {
            let Some(p) = self.participants.get_mut(sid) else {
                return;
            };
            if p.is_disconnected()
                || !p.info.permission.can_publish_data
                || envelope_sequence <= p.last_reliable_seq
            {
                return;
            }
            p.last_reliable_seq = envelope_sequence;
        }
        let verdict = {
            let Some(p) = self.participants.get(sid) else {
                return;
            };
            if p.is_disconnected() || !p.info.permission.can_publish_data {
                return;
            }
            let sender = DataSender {
                sid: &p.info.sid,
                identity: &p.info.identity,
                hidden: p.is_hidden(),
                kind: p.info.kind,
                is_agent: p.is_agent(),
            };
            self.data.inbound(&sender, kind, label, payload)
        };
        let DataVerdict::Forward {
            payload,
            destination_identities,
            destination_sids,
            sequence,
            nonce,
        } = verdict
        else {
            return;
        };

        if kind == DataKind::Reliable && sequence > 0 && envelope_sequence == 0 {
            let Some(p) = self.participants.get_mut(sid) else {
                return;
            };
            if sequence <= p.last_reliable_seq {
                return;
            }
            p.last_reliable_seq = sequence;
        }
        if let Some(nonce) = nonce.filter(|n| !n.is_empty())
            && !self.admit_nonce(&nonce, now)
        {
            return;
        }
        self.route_data(
            Some(sid),
            kind,
            payload,
            &destination_identities,
            &destination_sids,
            sequence > 0,
        );
    }

    /// Deliver to every eligible participant except the sender.
    pub(super) fn route_data(
        &mut self,
        sender: Option<&ParticipantSid>,
        kind: DataKind,
        payload: Vec<u8>,
        destination_identities: &[Identity],
        destination_sids: &[ParticipantSid],
        sequenced: bool,
    ) {
        let targeted = !destination_identities.is_empty() || !destination_sids.is_empty();
        let recipients: Vec<ParticipantSid> = self
            .participants
            .iter()
            .filter(|(s, p)| {
                if Some(*s) == sender || p.is_disconnected() {
                    return false;
                }
                if targeted
                    && !destination_identities.contains(&p.info.identity)
                    && !destination_sids.contains(&p.info.sid)
                {
                    return false;
                }
                // Only exact RELIABLE may enter the source joining lifecycle.
                p.is_active()
                    || (kind == DataKind::Reliable && (sequenced || !p.can_write_reliable))
            })
            .map(|(s, _)| s.clone())
            .collect();
        let n = recipients.len();
        let mut payload = Some(payload);
        let mut forwarded = Vec::new();
        for (i, to) in recipients.into_iter().enumerate() {
            if kind == DataKind::Reliable && !sequenced {
                let p = self.participants.get_mut(&to).expect("captured recipient");
                if !p.can_write_reliable && !p.joining_replay_in_progress {
                    // Source counts the original encoded message bytes, including
                    // its protobuf envelope. Reaching the inclusive limit is legal.
                    let data = payload.as_ref().expect("remaining owned payload");
                    if data.len() <= 100_000_usize.saturating_sub(p.joining_reliable_bytes) {
                        p.joining_reliable_bytes += data.len();
                        p.joining_reliable.push_back(data.clone());
                    }
                    continue;
                }
                if !p.is_active() {
                    continue;
                }
            }
            // The last recipient takes the buffer instead of a copy.
            let data = if i + 1 == n {
                payload.take().unwrap_or_default()
            } else {
                payload.clone().unwrap_or_default()
            };
            if sender.is_some() {
                forwarded.push(crate::media_api::DataWrite {
                    to,
                    kind,
                    payload: data,
                    expected_transport: None,
                });
            } else {
                self.media(MediaCommand::SendData {
                    to,
                    kind,
                    payload: data,
                });
            }
        }
        if let (Some(source), false) = (sender, forwarded.is_empty()) {
            self.media(MediaCommand::ForwardData {
                source: source.clone(),
                writes: forwarded,
            });
        }
    }
    pub(super) fn primary_transport_established(
        &mut self,
        sid: &ParticipantSid,
        transport: TransportId,
    ) {
        let pending = {
            let Some(p) = self.participants.get_mut(sid) else {
                return;
            };
            if p.is_disconnected()
                || p.primary != transport
                || p.can_write_reliable
                || p.joining_replay_in_progress
            {
                return;
            }
            p.joining_replay_in_progress = true;
            p.joining_reliable_bytes = 0;
            std::mem::take(&mut p.joining_reliable)
        };
        let writes = pending
            .into_iter()
            .map(|payload| crate::media_api::DataWrite {
                to: sid.clone(),
                kind: DataKind::Reliable,
                payload,
                expected_transport: Some(transport),
            })
            .collect();
        self.media(MediaCommand::ReplayJoiningReliable {
            participant: sid.clone(),
            transport,
            writes,
        });
    }

    pub(super) fn joining_reliable_replayed(
        &mut self,
        sid: &ParticipantSid,
        transport: TransportId,
    ) {
        let Some(p) = self.participants.get_mut(sid) else {
            return;
        };
        if p.primary != transport || p.is_disconnected() || !p.joining_replay_in_progress {
            return;
        }
        p.joining_replay_in_progress = false;
        p.can_write_reliable = true;
    }

    /// Room-wide 2 s duplicate suppression of non-empty `UserPacket.nonce`.
    /// Returns false for a duplicate; otherwise records it.
    pub(super) fn admit_nonce(&mut self, nonce: &[u8], now: Instant) -> bool {
        self.expire_nonces(now);
        if self.nonces.iter().any(|(_, n)| n.as_slice() == nonce) {
            return false;
        }
        if self.nonces.len() >= NONCE_CACHE_MAX {
            self.nonces.pop_front();
        }
        self.nonces.push_back((now, nonce.to_vec()));
        true
    }
}
