//! C85-A parity legs for the six `DATA_PACKET` ledger leaves: `rpc_request`,
//! `rpc_ack`, `rpc_response`, `stream_header`, `stream_chunk`, `stream_trailer`.
//!
//! Every expectation is re-derived from the pinned Go oracle (livekit server
//! v1.13.10, commit e5655ac2, `references/livekit-stable`) and driven through the
//! production chain only:
//!
//! ```text
//! MediaEvent::DataReceived
//!   → rtc_core::room::Room::data_received      (crates/rtc-core/src/room/data.rs)
//!   → lk_compat::data::LiveKitDataHandler       (crates/lk-compat/src/data.rs)
//!   → Room::route_data → ForwardData/SendData to the recipients
//! ```
//!
//! The handler type and the registry are the ones `CompatServices::new` builds and
//! `assemble` installs into the real `RoomManager` (`crates/lk-compat/src/state.rs`),
//! so this is the same object graph the launched binary uses. Tier attested here is
//! `component_behavior`; these legs do NOT attest `main_server_consumer` or
//! `official_differential_lifecycle` (those need a launched server plus the official
//! JS SDK, see tests/interop/rpc-lifecycle/README.md).
//!
//! Go ground truth used below:
//! * participant.go:2491 `onReceivedDataMessage` — unparseable packet returns.
//! * participant.go:2550 `handleReceivedDataMessage` — reliable-sequence gate,
//!   `dp.ParticipantSid = p.ID()`, `dp.Kind = kind`, then the per-variant switch.
//!   Each of the six variants begins with a nil-inner-payload `return`.
//! * participant.go:2621 `*DataPacket_RpcRequest` — debug log only, then forwards.
//! * participant.go:2631/2645 `RpcResponse`/`RpcAck` — consume only when
//!   `handleIncomingRpcResponse`/`handleIncomingRpcAck` hits the request-id map.
//! * participant.go:2657 `StreamHeader` — agent-on-behalf marks a *text* content
//!   header `generated` and keeps the claimed identity; every other content header
//!   takes the `default:` arm and is overwritten with the agent's own identity.
//! * participant.go:2673/2678 `StreamChunk`/`StreamTrailer` — nil guard only.

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use std::time::{Duration, Instant};

use lk_protocol::livekit as pb;
use prost::Message;
use rtc_core::config::RoomPolicy;
use rtc_core::ids::{ParticipantSid, ProjectId, RoomKey};
use rtc_core::media_api::{DataKind, MediaCommand, MediaEvent};
use rtc_core::model::{CodecSpec, ParticipantKind};
use rtc_core::permission::{Grants, VideoGrant};
use rtc_core::room::{Effect, Room};
use rtc_core::session::{ClientCapabilities, ClientMeta, JoinParams, SdkKind};

use lk_compat::data::{LOSSY_LABEL, LiveKitDataHandler, RELIABLE_LABEL};
use lk_compat::rpc::{DataChannelRpcError, RpcOutcome, RpcRegistry};

const ROOM: &str = "data-packet-leaves";

fn policy() -> RoomPolicy {
    RoomPolicy {
        transport_fallback: Default::default(),
        enable_data_tracks: true,
        enable_participant_data_blob: false,
        max_data_track_custom_encoding_length: 32,
        max_data_blob_key_length: 256,
        max_data_blob_size: 64_000,
        enabled_codecs: vec![CodecSpec {
            mime: "audio/opus".into(),
            fmtp_line: String::new(),
        }],
        max_participants: 0,
        empty_timeout: Duration::from_secs(300),
        departure_timeout: Duration::from_secs(20),
        enable_remote_unmute: false,
        playout_delay: None,
        sync_streams: false,
        enable_full_room_in_webhooks: false,
        update_batch_target_size: 128 << 10,
        subscription_limit_video: None,
        subscription_limit_audio: None,
        max_participant_name_length: 256,
        max_metadata_size: Some(512 << 10),
        max_attributes_size: Some(64 << 10),
        use_ice_lite: false,
        allow_pause: false,
        reconnect_on_publication_error: false,
        reconnect_on_subscription_error: false,
        reconnect_on_data_channel_error: false,
        region: String::new(),
    }
}

fn join_params(identity: &str, kind: ParticipantKind) -> JoinParams {
    JoinParams {
        room: RoomKey::new(ProjectId::local(), ROOM),
        grants: Grants {
            identity: identity.into(),
            video: VideoGrant {
                room_join: true,
                room: ROOM.into(),
                ..Default::default()
            },
            kind,
            ..Default::default()
        },
        identity: identity.into(),
        name: String::new(),
        metadata: String::new(),
        attributes: BTreeMap::new(),
        client: ClientMeta {
            sdk: SdkKind::Js,
            protocol: 17,
            ..Default::default()
        },
        capabilities: ClientCapabilities {
            ice_tcp: true,
            subscriber_primary: true,
            speaker_changed: true,
            connection_quality: true,
            ice_lite: true,
            unpublish: true,
            fast_start: true,
            disconnected_update: true,
            sync_stream_id: true,
            connection_quality_lost: true,
            identity_based_reconnection: true,
            regions_in_leave: true,
            non_error_signal_response: true,
            request_response: true,
            audio_red: true,
            ..Default::default()
        },
        auto_subscribe: true,
        auto_subscribe_data_track: false,
        adaptive_stream: false,
        single_pc: true,
        disable_ice_lite: false,
        subscriber_allow_pause: None,
        reconnect: false,
        reconnect_reason: 0,
        participant_sid: None,
        add_tracks: vec![],
        publisher_offer: None,
    }
}

fn transport_of(effects: &[Effect], sid: &ParticipantSid) -> rtc_core::ids::TransportId {
    effects
        .iter()
        .find_map(|e| match e {
            Effect::Media(MediaCommand::CreateTransport(spec)) if &spec.participant == sid => {
                Some(spec.id)
            }
            _ => None,
        })
        .expect("single-pc transport created at join")
}

/// Real room state machine driven by the production adapter data handler.
struct Harness {
    room: Room,
    rpc: Arc<RpcRegistry>,
    now: Instant,
    alice: ParticipantSid,
    bob: ParticipantSid,
    carol: ParticipantSid,
}

impl Harness {
    fn new() -> Self {
        let now = Instant::now();
        let rpc = Arc::new(RpcRegistry::new());
        let data = Arc::new(LiveKitDataHandler::new(rpc.clone()));
        let mut room = Room::new(
            RoomKey::new(ProjectId::local(), ROOM),
            Arc::new(policy()),
            data,
            &Default::default(),
            now,
        );
        room.take_effects();
        let mut sids = Vec::new();
        for name in ["alice", "bob", "carol"] {
            sids.push(
                room.join(join_params(name, ParticipantKind::Standard), now)
                    .unwrap()
                    .sid,
            );
        }
        let effects = room.take_effects();
        let mut harness = Self {
            room,
            rpc,
            now,
            alice: sids[0].clone(),
            bob: sids[1].clone(),
            carol: sids[2].clone(),
        };
        for sid in &sids {
            harness.bring_up_pc(sid, transport_of(&effects, sid));
        }
        harness
    }

    /// Join an extra participant of `kind` and bring its peer connection up.
    fn join_agent(&mut self, identity: &str, kind: ParticipantKind) -> ParticipantSid {
        let sid = self
            .room
            .join(join_params(identity, kind), self.now)
            .unwrap()
            .sid;
        let effects = self.room.take_effects();
        self.bring_up_pc(&sid, transport_of(&effects, &sid));
        sid
    }

    /// Replay the engine's peer-connection ready chain for `sid`.
    ///
    /// `can_write_reliable` is not a join-time fact: the engine reports the
    /// primary transport fully established (`signaling.rs:1859`), the room
    /// answers with the buffered reliable batch, and only after the fan-out
    /// drains does the engine report the replay complete
    /// (`data_fanout.rs:149`), which is what opens the reliable write path
    /// (`data.rs:231`). A fresh peer connection has nothing buffered yet, so
    /// the replay must arrive empty; that is what keeps this harness from
    /// silently pre-delivering frames the legs below claim to send.
    fn bring_up_pc(&mut self, sid: &ParticipantSid, transport: rtc_core::ids::TransportId) {
        let now = self.now;
        self.room.handle_media_event(
            MediaEvent::TransportConnected {
                participant: sid.clone(),
                transport,
            },
            now,
        );
        self.room.handle_media_event(
            MediaEvent::PrimaryTransportEstablished {
                participant: sid.clone(),
                transport,
            },
            now,
        );
        let replays: Vec<(ParticipantSid, rtc_core::ids::TransportId, usize)> = self
            .room
            .take_effects()
            .into_iter()
            .filter_map(|effect| match effect {
                Effect::Media(MediaCommand::ReplayJoiningReliable {
                    participant,
                    transport,
                    writes,
                }) => Some((participant, transport, writes.len())),
                _ => None,
            })
            .collect();
        for (participant, replayed_transport, buffered) in replays {
            assert_eq!(buffered, 0, "bring-up must not buffer reliable frames");
            self.room.handle_media_event(
                MediaEvent::JoiningReliableReplayed {
                    participant,
                    transport: replayed_transport,
                },
                now,
            );
        }
        self.take_writes();
    }

    fn inbound(&mut self, sender: &ParticipantSid, kind: DataKind, label: &str, raw: &[u8]) {
        self.room.handle_media_event(
            MediaEvent::DataReceived {
                participant: sender.clone(),
                kind,
                label: label.into(),
                payload: raw.to_vec(),
            },
            self.now,
        );
    }

    /// Data writes the engine actually scheduled for recipients after the last
    /// stimulus, in delivery order.
    fn take_writes(&mut self) -> Vec<(ParticipantSid, Vec<u8>)> {
        let mut out = Vec::new();
        for effect in self.room.take_effects() {
            match effect {
                Effect::Media(MediaCommand::SendData { to, payload, .. }) => {
                    out.push((to, payload))
                }
                Effect::Media(MediaCommand::ForwardData { writes, .. }) => {
                    for w in writes {
                        out.push((w.to, w.payload))
                    }
                }
                _ => {}
            }
        }
        out
    }

    /// Assert the frame reached exactly `expected` (identity order irrelevant) and
    /// return the forwarded packet as a recipient would decode it.
    fn forward_to(&mut self, expected: &[&ParticipantSid]) -> pb::DataPacket {
        let writes = self.take_writes();
        let mut got: Vec<String> = writes.iter().map(|(s, _)| s.0.clone()).collect();
        got.sort();
        let mut want: Vec<String> = expected.iter().map(|s| s.0.clone()).collect();
        want.sort();
        assert_eq!(got, want, "recipient set");
        assert_eq!(writes.len(), expected.len(), "one write per recipient");
        pb::DataPacket::decode(writes[0].1.as_slice()).expect("forwarded packet decodes")
    }

    fn assert_no_forward(&mut self) {
        assert!(self.take_writes().is_empty(), "frame must not be delivered");
    }
}

// ---------------------------------------------------------------------------
// DATA_PACKET:rpc_request — relayed, never consumed by the SFU
// ---------------------------------------------------------------------------

/// Go's `*DataPacket_RpcRequest` branch contains only a debug log, so the request
/// keeps `shouldForwardData = true` and gets the shared sender attribution like any
/// other frame: the SFU relays a peer-to-peer RPC request, it never answers it and
/// never registers pending state for it. A spoofed inner identity/SID is overwritten.
#[test]
fn rpc_request_relays_with_sender_attribution_and_registers_nothing() {
    let mut h = Harness::new();
    let wire = pb::DataPacket {
        participant_identity: "mallory".into(),
        participant_sid: "PA_spoof".into(),
        value: Some(pb::data_packet::Value::RpcRequest(pb::RpcRequest {
            id: "req-1".into(),
            method: "greet".into(),
            payload: "hi".into(),
            response_timeout_ms: 5_000,
            version: 1,
            compressed_payload: vec![],
        })),
        ..Default::default()
    }
    .encode_to_vec();
    h.inbound(&h.alice.clone(), DataKind::Reliable, RELIABLE_LABEL, &wire);
    let out = h.forward_to(&[&h.bob.clone(), &h.carol.clone()]);
    let req = match out.value {
        Some(pb::data_packet::Value::RpcRequest(r)) => r,
        other => panic!("rpc_request frame must survive routing, got {other:?}"),
    };
    assert_eq!(
        (req.id.as_str(), req.method.as_str(), req.payload.as_str()),
        ("req-1", "greet", "hi")
    );
    assert_eq!((req.response_timeout_ms, req.version), (5_000, 1));
    assert_eq!(out.participant_identity, "alice");
    assert_eq!(out.participant_sid, h.alice.0);
    // the channel kind is the source of truth, not the client's claim
    #[expect(deprecated)] // participant.go:2565 stamps dp.Kind from the channel
    let kind = out.kind;
    assert_eq!(kind, 0);
    assert_eq!(
        h.rpc.pending_count(),
        0,
        "observing a request must not register it"
    );
}

/// Targeting half of the leaf: an RPC request carries its recipients on the
/// envelope only, so `destination_identities` must select the recipient set.
#[test]
fn rpc_request_honours_destination_identities() {
    let mut h = Harness::new();
    let wire = pb::DataPacket {
        destination_identities: vec!["carol".into()],
        value: Some(pb::data_packet::Value::RpcRequest(pb::RpcRequest {
            id: "req-t".into(),
            method: "m".into(),
            ..Default::default()
        })),
        ..Default::default()
    }
    .encode_to_vec();
    h.inbound(&h.alice.clone(), DataKind::Reliable, RELIABLE_LABEL, &wire);
    let out = h.forward_to(&[&h.carol.clone()]);
    assert!(matches!(
        out.value,
        Some(pb::data_packet::Value::RpcRequest(_))
    ));
}

// ---------------------------------------------------------------------------
// DATA_PACKET:rpc_ack
// ---------------------------------------------------------------------------

/// Go `handleIncomingRpcAck` looks the request id up in `rpcPendingAcks`; a hit
/// resolves the waiting call and consumes the packet so it is never broadcast.
#[test]
fn rpc_ack_for_a_pending_request_is_consumed_and_resolves_the_ack_waiter() {
    let mut h = Harness::new();
    let (mut ack_rx, _resp_rx) = h.rpc.register("req-ack", h.bob.clone()).unwrap();
    let wire = pb::DataPacket {
        value: Some(pb::data_packet::Value::RpcAck(pb::RpcAck {
            request_id: "req-ack".into(),
        })),
        ..Default::default()
    }
    .encode_to_vec();
    h.inbound(&h.bob.clone(), DataKind::Reliable, RELIABLE_LABEL, &wire);
    h.assert_no_forward();
    assert_eq!(
        ack_rx.try_recv(),
        Ok(()),
        "the ack must resolve the PerformRpc ack timer"
    );
    // Upstream keeps rpcPendingResponses until the response arrives; the Rust
    // entry stays registered (only the response path drains it).
    assert_eq!(h.rpc.pending_count(), 1);
}

/// Go keeps `rpcPendingAcks` on the participant whose `handleReceivedDataMessage`
/// received the frame, so a request id registered for bob is never matched when
/// carol publishes the ack: the lookup misses and the packet is forwarded. The
/// room-level Rust registry stores the bound SID and reaches the same verdict,
/// so this leg pins parity rather than a divergence.
#[test]
fn rpc_ack_from_another_participant_is_not_consumed() {
    let mut h = Harness::new();
    let (mut ack_rx, _resp_rx) = h.rpc.register("req-x", h.bob.clone()).unwrap();
    let wire = pb::DataPacket {
        value: Some(pb::data_packet::Value::RpcAck(pb::RpcAck {
            request_id: "req-x".into(),
        })),
        ..Default::default()
    }
    .encode_to_vec();
    // carol claims the answer to a call that was sent to bob
    h.inbound(&h.carol.clone(), DataKind::Reliable, RELIABLE_LABEL, &wire);
    let out = h.forward_to(&[&h.alice.clone(), &h.bob.clone()]);
    assert!(matches!(out.value, Some(pb::data_packet::Value::RpcAck(_))));
    assert!(
        matches!(
            ack_rx.try_recv(),
            Err(tokio::sync::oneshot::error::TryRecvError::Empty)
        ),
        "a wrong-sender ack must not resolve the pending call"
    );
    assert_eq!(h.rpc.pending_count(), 1);
}

/// Unknown request id → no map hit → `shouldForwardData` stays true, so the frame
/// is relayed unchanged to everyone else and the registry is untouched.
#[test]
fn rpc_ack_unknown_request_id_is_relayed() {
    let mut h = Harness::new();
    let (_ack_rx, _resp_rx) = h.rpc.register("req-held", h.bob.clone()).unwrap();
    let wire = pb::DataPacket {
        value: Some(pb::data_packet::Value::RpcAck(pb::RpcAck {
            request_id: "not-a-server-request".into(),
        })),
        ..Default::default()
    }
    .encode_to_vec();
    h.inbound(&h.alice.clone(), DataKind::Reliable, RELIABLE_LABEL, &wire);
    let out = h.forward_to(&[&h.bob.clone(), &h.carol.clone()]);
    match out.value {
        Some(pb::data_packet::Value::RpcAck(a)) => {
            assert_eq!(a.request_id, "not-a-server-request")
        }
        other => panic!("ack frame must be relayed unchanged, got {other:?}"),
    }
    assert_eq!(
        h.rpc.pending_count(),
        1,
        "an unrelated ack cannot drain the registry"
    );
}

// ---------------------------------------------------------------------------
// DATA_PACKET:rpc_response
// ---------------------------------------------------------------------------

/// Go splits the response into payload / error arms and hands both to
/// `handleIncomingRpcResponse`; a map hit consumes the packet and resolves the call.
#[test]
fn rpc_response_payload_is_consumed_and_delivered() {
    let mut h = Harness::new();
    let (_ack_rx, mut resp_rx) = h.rpc.register("req-p", h.bob.clone()).unwrap();
    let wire = pb::DataPacket {
        value: Some(pb::data_packet::Value::RpcResponse(pb::RpcResponse {
            request_id: "req-p".into(),
            value: Some(pb::rpc_response::Value::Payload("pong".into())),
        })),
        ..Default::default()
    }
    .encode_to_vec();
    h.inbound(&h.bob.clone(), DataKind::Reliable, RELIABLE_LABEL, &wire);
    h.assert_no_forward();
    assert_eq!(resp_rx.try_recv(), Ok(RpcOutcome::Payload("pong".into())));
    assert_eq!(
        h.rpc.pending_count(),
        0,
        "the response drains the pending entry"
    );
}

#[test]
fn rpc_response_error_carries_code_message_data_and_consumes() {
    let mut h = Harness::new();
    let (_ack_rx, mut resp_rx) = h.rpc.register("req-e", h.bob.clone()).unwrap();
    let wire = pb::DataPacket {
        value: Some(pb::data_packet::Value::RpcResponse(pb::RpcResponse {
            request_id: "req-e".into(),
            value: Some(pb::rpc_response::Value::Error(pb::RpcError {
                code: 1500,
                message: "handler failed".into(),
                data: "{\"detail\":1}".into(),
            })),
        })),
        ..Default::default()
    }
    .encode_to_vec();
    h.inbound(&h.bob.clone(), DataKind::Reliable, RELIABLE_LABEL, &wire);
    h.assert_no_forward();
    assert_eq!(
        resp_rx.try_recv(),
        Ok(RpcOutcome::Error(DataChannelRpcError {
            code: 1500,
            message: "handler failed".into(),
            data: "{\"detail\":1}".into(),
        }))
    );
    assert_eq!(h.rpc.pending_count(), 0);
}

/// Go's inner `switch res := rpcResponse.Value.(type)` has no default arm: an unset
/// (or compressed-only) value leaves `shouldForwardData` true, so even a known
/// request id is relayed instead of consumed.
#[test]
fn rpc_response_without_payload_or_error_arm_is_not_consumed() {
    let mut h = Harness::new();
    let (_ack_rx, mut resp_rx) = h.rpc.register("req-n", h.bob.clone()).unwrap();
    let wire = pb::DataPacket {
        value: Some(pb::data_packet::Value::RpcResponse(pb::RpcResponse {
            request_id: "req-n".into(),
            value: None,
        })),
        ..Default::default()
    }
    .encode_to_vec();
    h.inbound(&h.bob.clone(), DataKind::Reliable, RELIABLE_LABEL, &wire);
    let out = h.forward_to(&[&h.alice.clone(), &h.carol.clone()]);
    match out.value {
        Some(pb::data_packet::Value::RpcResponse(r)) => {
            assert_eq!(r.request_id, "req-n");
            assert!(r.value.is_none(), "unset value must pass through unset");
        }
        other => panic!("{other:?}"),
    }
    assert!(
        matches!(
            resp_rx.try_recv(),
            Err(tokio::sync::oneshot::error::TryRecvError::Empty)
        ),
        "an untyped response must not resolve the pending call"
    );
    assert_eq!(h.rpc.pending_count(), 1);
}

#[test]
fn rpc_response_unknown_request_id_is_relayed() {
    let mut h = Harness::new();
    let (_ack_rx, _resp_rx) = h.rpc.register("req-held", h.bob.clone()).unwrap();
    let wire = pb::DataPacket {
        value: Some(pb::data_packet::Value::RpcResponse(pb::RpcResponse {
            request_id: "no-such-request".into(),
            value: Some(pb::rpc_response::Value::Payload("x".into())),
        })),
        ..Default::default()
    }
    .encode_to_vec();
    h.inbound(&h.alice.clone(), DataKind::Reliable, RELIABLE_LABEL, &wire);
    let out = h.forward_to(&[&h.bob.clone(), &h.carol.clone()]);
    match out.value {
        Some(pb::data_packet::Value::RpcResponse(r)) => {
            assert_eq!(r.request_id, "no-such-request");
            assert_eq!(r.value, Some(pb::rpc_response::Value::Payload("x".into())));
        }
        other => panic!("{other:?}"),
    }
    assert_eq!(h.rpc.pending_count(), 1);
}

/// Response before ack: upstream's ack timer and response map are separate, and a
/// response resolves the caller without needing an ack first. Same observable here.
#[test]
fn rpc_response_resolves_both_waiters_when_the_ack_never_arrives() {
    let mut h = Harness::new();
    let (mut ack_rx, mut resp_rx) = h.rpc.register("req-order", h.bob.clone()).unwrap();
    let wire = pb::DataPacket {
        value: Some(pb::data_packet::Value::RpcResponse(pb::RpcResponse {
            request_id: "req-order".into(),
            value: Some(pb::rpc_response::Value::Payload("direct".into())),
        })),
        ..Default::default()
    }
    .encode_to_vec();
    h.inbound(&h.bob.clone(), DataKind::Reliable, RELIABLE_LABEL, &wire);
    h.assert_no_forward();
    assert_eq!(ack_rx.try_recv(), Ok(()));
    assert_eq!(resp_rx.try_recv(), Ok(RpcOutcome::Payload("direct".into())));
    assert_eq!(h.rpc.pending_count(), 0);
}

// ---------------------------------------------------------------------------
// DATA_PACKET:stream_header
// ---------------------------------------------------------------------------

/// Go `StreamHeader`: agent + claimed identity != own identity + TEXT content header
/// ⇒ `Generated = true` and `overrideSenderIdentity = false` (claimed identity kept).
#[test]
fn stream_header_agent_on_behalf_text_keeps_claimed_identity_and_marks_generated() {
    let mut h = Harness::new();
    let agent = h.join_agent("agent1", ParticipantKind::Agent);
    let wire = pb::DataPacket {
        participant_identity: "alice".into(),
        value: Some(pb::data_packet::Value::StreamHeader(
            pb::data_stream::Header {
                stream_id: "s1".into(),
                topic: "chat".into(),
                mime_type: "text/plain".into(),
                content_header: Some(pb::data_stream::header::ContentHeader::TextHeader(
                    pb::data_stream::TextHeader {
                        operation_type: 0,
                        generated: false,
                        ..Default::default()
                    },
                )),
                ..Default::default()
            },
        )),
        ..Default::default()
    }
    .encode_to_vec();
    h.inbound(&agent, DataKind::Reliable, RELIABLE_LABEL, &wire);
    let out = h.forward_to(&[&h.alice.clone(), &h.bob.clone(), &h.carol.clone()]);
    assert_eq!(
        out.participant_identity, "alice",
        "agent publishing on behalf keeps the claimed identity"
    );
    match out.value {
        Some(pb::data_packet::Value::StreamHeader(hdr)) => {
            assert_eq!((hdr.stream_id.as_str(), hdr.topic.as_str()), ("s1", "chat"));
            match hdr.content_header {
                Some(pb::data_stream::header::ContentHeader::TextHeader(t)) => {
                    assert!(
                        t.generated,
                        "agent-authored text header is marked generated"
                    )
                }
                other => panic!("text header expected, got {other:?}"),
            }
        }
        other => panic!("header frame expected, got {other:?}"),
    }
}

/// Measured Go behaviour for the sibling arm: a BYTE content header under the same
/// agent-on-behalf condition takes `default:` ⇒ `overrideSenderIdentity = true`, so
/// the claimed identity is replaced by the agent's own. Not a Rust-only rule.
#[test]
fn stream_header_agent_byte_header_overrides_claimed_identity() {
    let mut h = Harness::new();
    let agent = h.join_agent("agent2", ParticipantKind::Agent);
    let wire = pb::DataPacket {
        participant_identity: "alice".into(),
        value: Some(pb::data_packet::Value::StreamHeader(
            pb::data_stream::Header {
                stream_id: "f1".into(),
                mime_type: "application/octet-stream".into(),
                content_header: Some(pb::data_stream::header::ContentHeader::ByteHeader(
                    pb::data_stream::ByteHeader {
                        name: "f.bin".into(),
                    },
                )),
                ..Default::default()
            },
        )),
        ..Default::default()
    }
    .encode_to_vec();
    h.inbound(&agent, DataKind::Reliable, RELIABLE_LABEL, &wire);
    let out = h.forward_to(&[&h.alice.clone(), &h.bob.clone(), &h.carol.clone()]);
    assert_eq!(
        out.participant_identity, "agent2",
        "Go's default arm overrides the identity for byte headers"
    );
    match out.value {
        Some(pb::data_packet::Value::StreamHeader(hdr)) => match hdr.content_header {
            Some(pb::data_stream::header::ContentHeader::ByteHeader(b)) => {
                assert_eq!(b.name, "f.bin")
            }
            other => panic!("byte header expected, got {other:?}"),
        },
        other => panic!("{other:?}"),
    }
}

/// A plain participant may not speak for anybody else, and the server does not
/// clear a client-set `generated` flag: Go only ever sets it, never resets it.
#[test]
fn stream_header_non_agent_identity_is_overwritten_and_generated_is_left_alone() {
    let mut h = Harness::new();
    let wire = pb::DataPacket {
        participant_identity: "mallory".into(),
        value: Some(pb::data_packet::Value::StreamHeader(
            pb::data_stream::Header {
                stream_id: "s2".into(),
                content_header: Some(pb::data_stream::header::ContentHeader::TextHeader(
                    pb::data_stream::TextHeader {
                        generated: true,
                        ..Default::default()
                    },
                )),
                ..Default::default()
            },
        )),
        ..Default::default()
    }
    .encode_to_vec();
    h.inbound(&h.alice.clone(), DataKind::Reliable, RELIABLE_LABEL, &wire);
    let out = h.forward_to(&[&h.bob.clone(), &h.carol.clone()]);
    assert_eq!(out.participant_identity, "alice");
    assert_eq!(out.participant_sid, h.alice.0);
    match out.value {
        Some(pb::data_packet::Value::StreamHeader(hdr)) => match hdr.content_header {
            Some(pb::data_stream::header::ContentHeader::TextHeader(t)) => {
                assert!(
                    t.generated,
                    "upstream never clears a client-set generated flag"
                )
            }
            other => panic!("{other:?}"),
        },
        other => panic!("{other:?}"),
    }
}

/// An agent whose header carries NO content header (oneof unset) matches neither
/// the text nor the default arm inside the agent condition — Go's inner switch
/// still runs `default:` for `nil`, so the identity is overridden.
#[test]
fn stream_header_agent_without_content_header_overrides_identity() {
    let mut h = Harness::new();
    let agent = h.join_agent("agent3", ParticipantKind::Agent);
    let wire = pb::DataPacket {
        participant_identity: "alice".into(),
        value: Some(pb::data_packet::Value::StreamHeader(
            pb::data_stream::Header {
                stream_id: "s3".into(),
                ..Default::default()
            },
        )),
        ..Default::default()
    }
    .encode_to_vec();
    h.inbound(&agent, DataKind::Reliable, RELIABLE_LABEL, &wire);
    let out = h.forward_to(&[&h.alice.clone(), &h.bob.clone(), &h.carol.clone()]);
    assert_eq!(out.participant_identity, "agent3");
}

// ---------------------------------------------------------------------------
// DATA_PACKET:stream_chunk / stream_trailer
// ---------------------------------------------------------------------------

/// Both branches are a nil guard and nothing else: no authorisation, no ordering,
/// no consumption. Out-of-order and duplicate `chunk_index` values must reach the
/// clients untouched — reassembly is the SDK's job.
#[test]
fn stream_chunk_and_trailer_relay_out_of_order_and_duplicate_indices() {
    let mut h = Harness::new();
    for idx in [2u64, 0, 1, 0] {
        let wire = pb::DataPacket {
            participant_identity: "spoof".into(),
            value: Some(pb::data_packet::Value::StreamChunk(
                pb::data_stream::Chunk {
                    stream_id: "s4".into(),
                    chunk_index: idx,
                    content: format!("c{idx}").into_bytes(),
                    version: 0,
                    ..Default::default()
                },
            )),
            ..Default::default()
        }
        .encode_to_vec();
        h.inbound(&h.alice.clone(), DataKind::Reliable, RELIABLE_LABEL, &wire);
        let out = h.forward_to(&[&h.bob.clone(), &h.carol.clone()]);
        match out.value {
            Some(pb::data_packet::Value::StreamChunk(c)) => {
                assert_eq!((c.stream_id.as_str(), c.chunk_index), ("s4", idx));
                assert_eq!(c.content, format!("c{idx}").into_bytes());
            }
            other => panic!("chunk frame expected, got {other:?}"),
        }
        assert_eq!(out.participant_identity, "alice");
        assert_eq!(out.participant_sid, h.alice.0);
    }

    let mut attributes = HashMap::new();
    attributes.insert("bytes".to_string(), "123".to_string());
    let wire = pb::DataPacket {
        participant_identity: "spoof".into(),
        value: Some(pb::data_packet::Value::StreamTrailer(
            pb::data_stream::Trailer {
                stream_id: "s4".into(),
                reason: String::new(),
                attributes,
            },
        )),
        ..Default::default()
    }
    .encode_to_vec();
    h.inbound(&h.alice.clone(), DataKind::Reliable, RELIABLE_LABEL, &wire);
    let out = h.forward_to(&[&h.bob.clone(), &h.carol.clone()]);
    match out.value {
        Some(pb::data_packet::Value::StreamTrailer(t)) => {
            assert_eq!(t.stream_id, "s4");
            assert_eq!(t.attributes.get("bytes").map(String::as_str), Some("123"));
        }
        other => panic!("trailer frame expected, got {other:?}"),
    }
    assert_eq!(out.participant_identity, "alice");
}

/// Stream frames have no inner destination field, so routing derives purely from
/// the envelope's `destination_identities` for all three of them.
#[test]
fn stream_frames_route_by_destination_identities_only() {
    let mut h = Harness::new();
    let header = pb::DataPacket {
        destination_identities: vec!["carol".into()],
        value: Some(pb::data_packet::Value::StreamHeader(
            pb::data_stream::Header {
                stream_id: "s5".into(),
                content_header: Some(pb::data_stream::header::ContentHeader::ByteHeader(
                    pb::data_stream::ByteHeader { name: "n".into() },
                )),
                ..Default::default()
            },
        )),
        ..Default::default()
    }
    .encode_to_vec();
    h.inbound(
        &h.alice.clone(),
        DataKind::Reliable,
        RELIABLE_LABEL,
        &header,
    );
    h.forward_to(&[&h.carol.clone()]);

    let chunk = pb::DataPacket {
        destination_identities: vec!["bob".into()],
        value: Some(pb::data_packet::Value::StreamChunk(
            pb::data_stream::Chunk {
                stream_id: "s5".into(),
                chunk_index: 0,
                content: b"x".to_vec(),
                ..Default::default()
            },
        )),
        ..Default::default()
    }
    .encode_to_vec();
    h.inbound(&h.alice.clone(), DataKind::Reliable, RELIABLE_LABEL, &chunk);
    h.forward_to(&[&h.bob.clone()]);

    // an unknown destination receives nothing at all
    let trailer = pb::DataPacket {
        destination_identities: vec!["nobody".into()],
        value: Some(pb::data_packet::Value::StreamTrailer(
            pb::data_stream::Trailer {
                stream_id: "s5".into(),
                ..Default::default()
            },
        )),
        ..Default::default()
    }
    .encode_to_vec();
    h.inbound(
        &h.alice.clone(),
        DataKind::Reliable,
        RELIABLE_LABEL,
        &trailer,
    );
    h.assert_no_forward();
}

// ---------------------------------------------------------------------------
// shared gates across all six variants
// ---------------------------------------------------------------------------

/// Go guards each oneof arm with `if payload.RpcRequest == nil { return }`, but
/// protobuf-go allocates the typed message before merging the body, so a
/// zero-length oneof field decodes to an empty non-nil message and the guard is
/// unreachable from the wire. Measured with the pinned
/// protocol v1.52.2-0.20261012065025-9bb708cdb819 on protobuf v1.36.12: fields
/// 10..15 all decode with the oneof set and `typedNil=false`, and a marshalled
/// zero-valued arm produces the same two-byte body. Every variant therefore runs
/// its normal arm, keeps `shouldForwardData = true` and is relayed, so the Rust
/// handler must forward rather than emulate a nil guard.
#[test]
fn empty_oneof_bodies_reach_the_variant_arm_instead_of_a_nil_guard() {
    let mut h = Harness::new();
    for (field, name) in [
        (10u32, "rpc_request"),
        (11, "rpc_ack"),
        (12, "rpc_response"),
        (13, "stream_header"),
        (14, "stream_chunk"),
        (15, "stream_trailer"),
    ] {
        // tag(field, wiretype 2) + length 0
        let wire = vec![((field << 3) | 2) as u8, 0];
        h.inbound(&h.alice.clone(), DataKind::Reliable, RELIABLE_LABEL, &wire);
        let out = h.forward_to(&[&h.bob.clone(), &h.carol.clone()]);
        let arm = match &out.value {
            Some(pb::data_packet::Value::RpcRequest(_)) => "rpc_request",
            Some(pb::data_packet::Value::RpcAck(_)) => "rpc_ack",
            Some(pb::data_packet::Value::RpcResponse(_)) => "rpc_response",
            Some(pb::data_packet::Value::StreamHeader(_)) => "stream_header",
            Some(pb::data_packet::Value::StreamChunk(_)) => "stream_chunk",
            Some(pb::data_packet::Value::StreamTrailer(_)) => "stream_trailer",
            other => panic!("{name}: relayed arm must survive, got {other:?}"),
        };
        assert_eq!(arm, name, "relayed frame keeps its own variant");
        assert_eq!(out.participant_identity, "alice", "sender asserted");
    }
    // Nothing on any arm registers or resolves pending RPC state.
    assert_eq!(h.rpc.pending_count(), 0);
}

/// Go advances `lastPubReliableSeq` before the payload switch, so the sequence gate
/// applies to these frames too: a replayed sequence never re-runs the handler and
/// never re-delivers, while lossy frames ignore the counter entirely.
#[test]
fn reliable_sequence_gate_covers_rpc_and_stream_frames() {
    let mut h = Harness::new();
    let chunk = |seq: u32| {
        pb::DataPacket {
            sequence: seq,
            value: Some(pb::data_packet::Value::StreamChunk(
                pb::data_stream::Chunk {
                    stream_id: "s6".into(),
                    chunk_index: seq as u64,
                    content: b"y".to_vec(),
                    ..Default::default()
                },
            )),
            ..Default::default()
        }
        .encode_to_vec()
    };
    h.inbound(
        &h.alice.clone(),
        DataKind::Reliable,
        RELIABLE_LABEL,
        &chunk(5),
    );
    h.forward_to(&[&h.bob.clone(), &h.carol.clone()]);
    h.inbound(
        &h.alice.clone(),
        DataKind::Reliable,
        RELIABLE_LABEL,
        &chunk(5),
    );
    h.assert_no_forward();
    h.inbound(
        &h.alice.clone(),
        DataKind::Reliable,
        RELIABLE_LABEL,
        &chunk(4),
    );
    h.assert_no_forward();
    h.inbound(
        &h.alice.clone(),
        DataKind::Reliable,
        RELIABLE_LABEL,
        &chunk(6),
    );
    h.forward_to(&[&h.bob.clone(), &h.carol.clone()]);
    // lossy: sequence is ignored, so the same value delivers again
    h.inbound(&h.alice.clone(), DataKind::Lossy, LOSSY_LABEL, &chunk(6));
    h.forward_to(&[&h.bob.clone(), &h.carol.clone()]);
}

/// An ack/response for a pending call must not leak to any recipient even on the
/// lossy channel, matching Go's channel-independent consumption.
#[test]
fn consumed_rpc_frames_do_not_leak_on_the_lossy_channel() {
    let mut h = Harness::new();
    let (_ack_rx, mut resp_rx) = h.rpc.register("req-lossy", h.bob.clone()).unwrap();
    let wire = pb::DataPacket {
        value: Some(pb::data_packet::Value::RpcResponse(pb::RpcResponse {
            request_id: "req-lossy".into(),
            value: Some(pb::rpc_response::Value::Payload("p".into())),
        })),
        ..Default::default()
    }
    .encode_to_vec();
    h.inbound(&h.bob.clone(), DataKind::Lossy, LOSSY_LABEL, &wire);
    h.assert_no_forward();
    assert_eq!(resp_rx.try_recv(), Ok(RpcOutcome::Payload("p".into())));
}
