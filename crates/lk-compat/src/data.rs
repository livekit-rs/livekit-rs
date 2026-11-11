//! Inbound data-channel policy (signaling.md §7, participant.go:2491-2713).
//!
//! The room calls [`LiveKitDataHandler::inbound`] for every message a participant
//! publishes. Enforcement that needs room state stays in the engine:
//! `canPublishData`, the per-publisher reliable sequence check
//! (`sequence <= lastPubReliableSeq` → drop), nonce de-duplication and recipient
//! routing use the values returned in [`DataVerdict::Forward`].

use std::sync::Arc;

use lk_protocol::livekit as pb;
use pb::data_packet::Value;
use prost::Message;
use rtc_core::data::{DataHandler, DataSender, DataVerdict};
use rtc_core::ids::{Identity, ParticipantSid};
use rtc_core::media_api::DataKind;

use crate::convert::data_kind_to_proto;
use crate::rpc::{DataChannelRpcError, RpcOutcome, RpcRegistry};

pub const RELIABLE_LABEL: &str = "_reliable";
pub const LOSSY_LABEL: &str = "_lossy";
pub const DATA_TRACK_LABEL: &str = "_data_track";

pub struct LiveKitDataHandler {
    rpc: Arc<RpcRegistry>,
}

impl LiveKitDataHandler {
    pub fn new(rpc: Arc<RpcRegistry>) -> Self {
        Self { rpc }
    }

    /// Message on a `_reliable`/`_lossy` channel: a `DataPacket`.
    fn labeled(&self, sender: &DataSender<'_>, kind: DataKind, raw: &[u8]) -> DataVerdict {
        let Ok(mut dp) = lk_protocol::wire::decode::<pb::DataPacket>(raw) else {
            return DataVerdict::Drop;
        };

        // Always the real sender SID; the channel kind is the source of truth.
        dp.participant_sid = sender.sid.0.clone();
        dp.kind = data_kind_to_proto(kind);
        let sender_identity = if sender.hidden {
            String::new()
        } else {
            sender.identity.0.clone()
        };

        let mut forward = true;
        let mut override_identity = true;
        let mut nonce = None;
        let on_behalf = sender.is_agent
            && !dp.participant_identity.is_empty()
            && dp.participant_identity != sender.identity.0;

        match dp.value.as_mut() {
            Some(Value::User(u)) => {
                if sender.hidden {
                    u.participant_sid.clear();
                    u.participant_identity.clear();
                } else {
                    u.participant_sid = sender.sid.0.clone();
                    u.participant_identity = sender.identity.0.clone();
                }
                if !dp.destination_identities.is_empty() {
                    u.destination_identities = dp.destination_identities.clone();
                } else {
                    dp.destination_identities = u.destination_identities.clone();
                }
                if !u.nonce.is_empty() {
                    nonce = Some(u.nonce.clone());
                }
            }
            Some(Value::Transcription(_)) => {
                // Only agents may publish transcriptions.
                forward = sender.is_agent;
            }
            Some(Value::ChatMessage(c)) => {
                if on_behalf {
                    override_identity = false;
                    c.generated = true;
                }
            }
            Some(Value::Metrics(_)) => {
                // Metrics go through the metrics path, never forwarded as data.
                forward = false;
            }
            Some(Value::RpcResponse(r)) => {
                let outcome = match r.value.as_ref() {
                    Some(pb::rpc_response::Value::Payload(p)) => {
                        Some(RpcOutcome::Payload(p.clone()))
                    }
                    Some(pb::rpc_response::Value::Error(e)) => {
                        Some(RpcOutcome::Error(DataChannelRpcError {
                            code: e.code,
                            message: e.message.clone(),
                            data: e.data.clone(),
                        }))
                    }
                    // Upstream only matches payload/error responses; compressed
                    // or empty responses are forwarded.
                    _ => None,
                };
                if let Some(outcome) = outcome
                    && self.rpc.handle_response(sender.sid, &r.request_id, outcome)
                {
                    return DataVerdict::Consumed;
                }
            }
            Some(Value::RpcAck(a)) => {
                if self.rpc.handle_ack(sender.sid, &a.request_id) {
                    return DataVerdict::Consumed;
                }
            }
            Some(Value::StreamHeader(h)) if on_behalf => match h.content_header.as_mut() {
                Some(pb::data_stream::header::ContentHeader::TextHeader(t)) => {
                    t.generated = true;
                    override_identity = false;
                }
                _ => override_identity = true,
            },
            // rpc_request, stream chunk/trailer, sip_dtmf, encrypted packets,
            // deprecated speaker updates and unknown payloads: forwarded as-is.
            _ => {}
        }

        if !forward {
            return DataVerdict::Drop;
        }
        if override_identity {
            dp.participant_identity = sender_identity;
        }

        let (destination_identities, destination_sids) = destinations(&dp);
        // Upstream Go re-marshals the (mutated) typed packet with proto.Marshal,
        // which keeps the unknown protobuf fields the decoder skipped. prost
        // drops them, so restore them from the untouched inbound wire before
        // forwarding, at the root and inside retained child payloads.
        let known = dp.encode_to_vec();
        let payload = lk_protocol::wire::preserve_unknown_bytes("livekit.DataPacket", raw, &known)
            .unwrap_or(known);
        DataVerdict::Forward {
            sequence: if kind == DataKind::Reliable {
                dp.sequence
            } else {
                0
            },
            payload,
            destination_identities,
            destination_sids,
            nonce,
        }
    }

    /// Any other label: upstream "unlabeled" data, re-sent to everyone else as
    /// `DataPacket{participant_identity: sender, user: {payload: raw}}`
    /// (transport.go:1539-1571).
    fn unlabeled(&self, sender: &DataSender<'_>, raw: &[u8]) -> DataVerdict {
        let dp = pb::DataPacket {
            participant_identity: sender.identity.0.clone(),
            value: Some(Value::User(pb::UserPacket {
                payload: raw.to_vec(),
                ..Default::default()
            })),
            ..Default::default()
        };
        DataVerdict::Forward {
            payload: dp.encode_to_vec(),
            destination_identities: Vec::new(),
            destination_sids: Vec::new(),
            sequence: 0,
            nonce: None,
        }
    }
}

/// Recipient filters (`BroadcastDataPacketForRoom`): `destination_identities`
/// (already synchronised with the user packet) and legacy `user.destination_sids`.
fn destinations(dp: &pb::DataPacket) -> (Vec<Identity>, Vec<ParticipantSid>) {
    let identities = dp
        .destination_identities
        .iter()
        .map(|s| Identity(s.clone()))
        .collect();
    let sids = match dp.value.as_ref() {
        Some(Value::User(u)) => u
            .destination_sids
            .iter()
            .map(|s| ParticipantSid(s.clone()))
            .collect(),
        _ => Vec::new(),
    };
    (identities, sids)
}

impl DataHandler for LiveKitDataHandler {
    fn reliable_sequence(&self, label: &str, raw: &[u8]) -> u32 {
        match label {
            RELIABLE_LABEL | LOSSY_LABEL | "" => {
                lk_protocol::wire::decode::<pb::DataPacket>(raw).map_or(0, |packet| packet.sequence)
            }
            _ => 0,
        }
    }

    fn inbound(
        &self,
        sender: &DataSender<'_>,
        kind: DataKind,
        label: &str,
        raw: &[u8],
    ) -> DataVerdict {
        match label {
            RELIABLE_LABEL | LOSSY_LABEL | "" => self.labeled(sender, kind, raw),
            // Data-track frames are not DataPacket-framed and are not part of the
            // data-message path.
            DATA_TRACK_LABEL => DataVerdict::Drop,
            _ => self.unlabeled(sender, raw),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rtc_core::model::ParticipantKind;

    struct Who {
        sid: ParticipantSid,
        identity: Identity,
    }

    fn who() -> Who {
        Who {
            sid: ParticipantSid("PA_s".into()),
            identity: Identity("alice".into()),
        }
    }

    fn sender<'a>(w: &'a Who, hidden: bool, agent: bool) -> DataSender<'a> {
        DataSender {
            sid: &w.sid,
            identity: &w.identity,
            hidden,
            kind: if agent {
                ParticipantKind::Agent
            } else {
                ParticipantKind::Standard
            },
            is_agent: agent,
        }
    }

    fn handler() -> (LiveKitDataHandler, Arc<RpcRegistry>) {
        let rpc = Arc::new(RpcRegistry::new());
        (LiveKitDataHandler::new(rpc.clone()), rpc)
    }

    fn forwarded(
        v: DataVerdict,
    ) -> (
        pb::DataPacket,
        Vec<Identity>,
        Vec<ParticipantSid>,
        u32,
        Option<Vec<u8>>,
    ) {
        match v {
            DataVerdict::Forward {
                payload,
                destination_identities,
                destination_sids,
                sequence,
                nonce,
            } => (
                // A forwarding consumer sees the Go-tolerant known projection:
                // retained unknown/wrong-kind bytes are parsed as unrecognized and
                // never block the known fields these assertions check.
                lk_protocol::wire::decode::<pb::DataPacket>(payload.as_slice()).unwrap(),
                destination_identities,
                destination_sids,
                sequence,
                nonce,
            ),
            other => panic!("expected forward, got {other:?}"),
        }
    }

    #[test]
    fn user_packet_rewritten() {
        let (h, _) = handler();
        let w = who();
        let dp = pb::DataPacket {
            kind: 1,
            participant_identity: "spoofed".into(),
            participant_sid: "PA_spoof".into(),
            sequence: 7,
            value: Some(Value::User(pb::UserPacket {
                payload: b"hi".to_vec(),
                participant_identity: "spoofed".into(),
                destination_identities: vec!["bob".into()],
                destination_sids: vec!["PA_b".into()],
                nonce: vec![1; 16],
                ..Default::default()
            })),
            ..Default::default()
        };
        let (out, ids, sids, seq, nonce) = forwarded(h.inbound(
            &sender(&w, false, false),
            DataKind::Reliable,
            RELIABLE_LABEL,
            &dp.encode_to_vec(),
        ));
        assert_eq!(out.participant_sid, "PA_s");
        assert_eq!(out.participant_identity, "alice");
        assert_eq!(out.kind, 0);
        assert_eq!(out.destination_identities, vec!["bob".to_string()]);
        assert_eq!(ids, vec![Identity("bob".into())]);
        assert_eq!(sids, vec![ParticipantSid("PA_b".into())]);
        assert_eq!(seq, 7);
        assert_eq!(nonce, Some(vec![1; 16]));
        match out.value {
            Some(Value::User(u)) => {
                assert_eq!(u.participant_identity, "alice");
                assert_eq!(u.participant_sid, "PA_s");
            }
            _ => panic!(),
        }
    }

    #[test]
    fn hidden_sender_and_lossy_sequence() {
        let (h, _) = handler();
        let w = who();
        let dp = pb::DataPacket {
            sequence: 3,
            value: Some(Value::User(pb::UserPacket::default())),
            ..Default::default()
        };
        let (out, _, _, seq, _) = forwarded(h.inbound(
            &sender(&w, true, false),
            DataKind::Lossy,
            LOSSY_LABEL,
            &dp.encode_to_vec(),
        ));
        assert_eq!(out.participant_identity, "");
        assert_eq!(out.participant_sid, "PA_s");
        assert_eq!(out.kind, 1);
        assert_eq!(seq, 0);
    }

    #[test]
    fn transcription_metrics_garbage() {
        let (h, _) = handler();
        let w = who();
        let t = pb::DataPacket {
            value: Some(Value::Transcription(pb::Transcription::default())),
            ..Default::default()
        }
        .encode_to_vec();
        assert_eq!(
            h.inbound(
                &sender(&w, false, false),
                DataKind::Reliable,
                RELIABLE_LABEL,
                &t
            ),
            DataVerdict::Drop
        );
        assert!(matches!(
            h.inbound(
                &sender(&w, false, true),
                DataKind::Reliable,
                RELIABLE_LABEL,
                &t
            ),
            DataVerdict::Forward { .. }
        ));
        let m = pb::DataPacket {
            value: Some(Value::Metrics(pb::MetricsBatch::default())),
            ..Default::default()
        }
        .encode_to_vec();
        assert_eq!(
            h.inbound(
                &sender(&w, false, true),
                DataKind::Reliable,
                RELIABLE_LABEL,
                &m
            ),
            DataVerdict::Drop
        );
        assert_eq!(
            h.inbound(
                &sender(&w, false, false),
                DataKind::Reliable,
                RELIABLE_LABEL,
                &[0xff, 0xff, 0xff]
            ),
            DataVerdict::Drop
        );
    }

    #[test]
    fn agent_chat_on_behalf_keeps_identity() {
        let (h, _) = handler();
        let w = who();
        let dp = pb::DataPacket {
            participant_identity: "bob".into(),
            value: Some(Value::ChatMessage(pb::ChatMessage::default())),
            ..Default::default()
        };
        let (out, ..) = forwarded(h.inbound(
            &sender(&w, false, true),
            DataKind::Reliable,
            RELIABLE_LABEL,
            &dp.encode_to_vec(),
        ));
        assert_eq!(out.participant_identity, "bob");
        match out.value {
            Some(Value::ChatMessage(c)) => assert!(c.generated),
            _ => panic!(),
        }
        // non-agent cannot speak for others
        let (out, ..) = forwarded(h.inbound(
            &sender(&w, false, false),
            DataKind::Reliable,
            RELIABLE_LABEL,
            &dp.encode_to_vec(),
        ));
        assert_eq!(out.participant_identity, "alice");
    }

    #[test]
    fn rpc_ack_consumed_only_when_pending() {
        let (h, rpc) = handler();
        let w = who();
        let ack = pb::DataPacket {
            value: Some(Value::RpcAck(pb::RpcAck {
                request_id: "r1".into(),
            })),
            ..Default::default()
        }
        .encode_to_vec();
        assert!(matches!(
            h.inbound(
                &sender(&w, false, false),
                DataKind::Reliable,
                RELIABLE_LABEL,
                &ack
            ),
            DataVerdict::Forward { .. }
        ));
        // Register through the public perform path is async; use the response path.
        let resp = pb::DataPacket {
            value: Some(Value::RpcResponse(pb::RpcResponse {
                request_id: "r1".into(),
                value: Some(pb::rpc_response::Value::Payload("ok".into())),
            })),
            ..Default::default()
        }
        .encode_to_vec();
        assert!(matches!(
            h.inbound(
                &sender(&w, false, false),
                DataKind::Reliable,
                RELIABLE_LABEL,
                &resp
            ),
            DataVerdict::Forward { .. }
        ));
        assert_eq!(rpc.pending_count(), 0);
    }

    #[test]
    fn unlabeled_wraps_raw_bytes() {
        let (h, _) = handler();
        let w = who();
        let (out, ids, ..) = forwarded(h.inbound(
            &sender(&w, true, false),
            DataKind::Reliable,
            "custom",
            b"raw",
        ));
        assert!(ids.is_empty());
        assert_eq!(out.participant_identity, "alice");
        match out.value {
            Some(Value::User(u)) => assert_eq!(u.payload, b"raw"),
            _ => panic!(),
        }
        assert_eq!(
            h.inbound(
                &sender(&w, false, false),
                DataKind::Reliable,
                DATA_TRACK_LABEL,
                b"x"
            ),
            DataVerdict::Drop
        );
    }
    #[test]
    fn labeled_legal_wrong_wire_fields_forward_but_invalid_wire_and_utf8_drop() {
        let w = who();
        let sender = sender(&w, false, false);
        let (handler, _) = handler();
        // Actual UserPacket.payload#2 plus wrong-kind known participant_identity#4.
        let wire = [0x12, 3, 0x12, 1, b'x', 0x20, 7, 0x80, 1, 42];
        assert!(pb::DataPacket::decode(wire.as_slice()).is_err());
        assert_eq!(handler.reliable_sequence(RELIABLE_LABEL, &wire), 42);
        let (packet, _, _, sequence, _) =
            forwarded(handler.inbound(&sender, DataKind::Reliable, RELIABLE_LABEL, &wire));
        assert_eq!(sequence, 42);
        assert_eq!(packet.participant_identity, "alice");
        assert!(
            matches!(packet.value,Some(Value::User(user)) if user.payload==b"x" && user.participant_identity=="alice")
        );
        // The forwarded wire retains Go's re-emitted wrong-kind known number
        // (participant_identity#4 carried as varint 7), which prost's typed view
        // skips but the official protobuf-go consumer tolerates as unrecognized.
        let DataVerdict::Forward { payload, .. } =
            handler.inbound(&sender, DataKind::Reliable, RELIABLE_LABEL, &wire)
        else {
            panic!("expected forward");
        };
        assert!(
            payload.ends_with(&[0x20, 7]),
            "wrong-kind known number preserved: {payload:02x?}"
        );
        // Same rule recursively inside UserPacket: wrong-kind identity#5.
        let nested = [0x12, 5, 0x12, 1, b'x', 0x28, 7];
        assert!(matches!(
            handler.inbound(&sender, DataKind::Reliable, RELIABLE_LABEL, &nested),
            DataVerdict::Forward { .. }
        ));
        for invalid in [&[0x12, 2, 0x12][..], &[0x22, 1, 0xff][..]] {
            assert!(matches!(
                handler.inbound(&sender, DataKind::Reliable, RELIABLE_LABEL, invalid),
                DataVerdict::Drop
            ));
        }
    }

    #[test]
    fn unknown_protobuf_fields_survive_forward() {
        let (h, _) = handler();
        let w = who();
        // DataPacket{ User#2{ payload#2='x', unknown100 varint=7 }, unknown400 varint=9 }.
        // Upstream Go re-marshals the mutated packet and keeps these unknowns;
        // prost would drop them, so the forward path must restore them.
        let wire: Vec<u8> = vec![
            0x12, 6, 0x12, 1, b'x', 0x80, 0x06, 0x07, // User + nested unknown100
            0x80, 0x19, 0x09, // root unknown400 varint 9
        ];
        let DataVerdict::Forward { payload, .. } = h.inbound(
            &sender(&w, false, false),
            DataKind::Reliable,
            RELIABLE_LABEL,
            &wire,
        ) else {
            panic!("expected forward");
        };
        // Root unknown400 (tag 0x80,0x19 value 9) is appended after the known tail.
        assert!(
            payload.ends_with(&[0x80, 0x19, 0x09]),
            "root unknown preserved: {payload:02x?}"
        );
        // Nested unknown100 (0x80,0x06,0x07) is restored inside the retained User bytes.
        assert!(
            payload.windows(3).any(|win| win == [0x80, 0x06, 0x07]),
            "nested unknown preserved: {payload:02x?}"
        );
        // Known sender attribution is still applied on top of the preserved wire.
        let dp = pb::DataPacket::decode(payload.as_slice()).unwrap();
        assert_eq!(dp.participant_identity, "alice");
        assert_eq!(dp.participant_sid, "PA_s");
        assert!(matches!(dp.value, Some(Value::User(u)) if u.payload == b"x"));
    }
}
