//! One signal WebSocket: frames ⇄ engine session (signaling.md §1.5, §9, §10).

use std::time::Duration;

use axum::extract::ws::{CloseFrame, Message, Utf8Bytes, WebSocket};
use futures_util::{SinkExt, StreamExt};
use lk_protocol::livekit as pb;
use pb::signal_response::Message as Resp;
use prost::Message as _;
use rtc_core::permission::Grants;
use rtc_core::session::SignalClaimsSnapshot;
use rtc_core::session::{ClientMeta, JoinInfo, ReconnectInfo, SessionOutput, SignalEvent};
use rtc_runtime::SessionHandle;

use crate::convert;
use crate::signal::encode::{EncodeCtx, Inbound, RequestCache, decode_request, encode_signal};
use crate::signal::ice;
use crate::state::AppState;

/// WS control ping frequency.
const WS_PING_INTERVAL: Duration = Duration::from_secs(10);
/// Token refresh cadence.
const REFRESH_INTERVAL: Duration = Duration::from_secs(5 * 60);
/// Write deadline for one frame; a stuck socket ends the session.
const WRITE_TIMEOUT: Duration = Duration::from_secs(5);

pub struct SessionCtx {
    pub state: AppState,
    pub grants: Grants,
    pub client: ClientMeta,
    pub cache: RequestCache,
}

/// Encoding chosen by the client: binary protobuf until a text frame arrives.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Mode {
    Binary,
    Json,
}

/// Follow actual wrapped error causes; only a typed message-size failure
/// authorizes Gorilla's MessageTooBig close, never a text-matched diagnostic.
pub(crate) fn capacity_message_too_long(error: &axum::Error) -> bool {
    let mut current: Option<&(dyn std::error::Error + 'static)> = Some(error);
    for _ in 0..16 {
        let Some(error) = current else { return false };
        if matches!(
            error.downcast_ref::<tungstenite::Error>(),
            Some(tungstenite::Error::Capacity(
                tungstenite::error::CapacityError::MessageTooLong { .. }
            ))
        ) {
            return true;
        }
        current = error.source();
    }
    false
}

/// Decoder failures on compressed payloads are separate from the wire cap:
/// fixed wsprotocol.go's decoded LimitReader returns a normal protocol close.
pub(crate) fn invalid_compressed_frame(error: &axum::Error) -> bool {
    let mut current: Option<&(dyn std::error::Error + 'static)> = Some(error);
    for _ in 0..16 {
        let Some(error) = current else {
            return false;
        };
        if matches!(
            error.downcast_ref::<tungstenite::Error>(),
            Some(
                tungstenite::Error::Capacity(
                    tungstenite::error::CapacityError::DecompressedMessageTooLong { .. }
                ) | tungstenite::Error::Protocol(
                    tungstenite::error::ProtocolError::InvalidCompression
                )
            )
        ) {
            return true;
        }
        current = error.source();
    }
    false
}

/// Preserve the close written by Gorilla for invalid RTC headers, or the
/// normal close written by LiveKit when Gorilla returns a signed length error.
pub(crate) fn protocol_close(error: &axum::Error) -> Option<CloseFrame> {
    let mut current: Option<&(dyn std::error::Error + 'static)> = Some(error);
    for _ in 0..16 {
        let error = current?;
        if let Some(tungstenite::Error::Protocol(protocol)) =
            error.downcast_ref::<tungstenite::Error>()
        {
            use tungstenite::error::ProtocolError;
            match protocol {
                ProtocolError::LivekitRtcProtocolViolation(reason) => {
                    return Some(CloseFrame {
                        code: 1002,
                        reason: Utf8Bytes::from(reason.as_str()),
                    });
                }
                ProtocolError::LivekitRtcReadLengthOverflow => {
                    return Some(CloseFrame {
                        code: 1000,
                        reason: Utf8Bytes::default(),
                    });
                }
                _ => {}
            }
        }
        current = error.source();
    }
    None
}

fn encode_frame(resp: &pb::SignalResponse, mode: Mode) -> Option<Message> {
    match mode {
        Mode::Binary => Some(Message::Binary(resp.encode_to_vec().into())),
        Mode::Json => serde_json::to_string(resp)
            .ok()
            .map(|s| Message::Text(Utf8Bytes::from(s))),
    }
}

pub(crate) fn decode_binary_request(
    bytes: &[u8],
) -> Result<pb::SignalRequest, lk_protocol::wire::WireError> {
    lk_protocol::wire::decode(bytes)
}

pub(crate) fn decode_text_request(text: &str) -> Result<pb::SignalRequest, serde_json::Error> {
    lk_protocol::json::decode_frame(text.as_bytes())
}

impl SessionCtx {
    /// Compose the actual binary Blob response from its source provenance.
    fn compose_frame(
        &mut self,
        ev: SignalEvent,
        sid: &rtc_core::ids::ParticipantSid,
        mode: Mode,
    ) -> Option<Message> {
        let raw = if mode == Mode::Binary {
            super::blob::response_wire(&ev)
        } else {
            None
        };
        let response = self.compose(ev, sid)?;
        raw.map(|wire| Message::Binary(wire.into()))
            .or_else(|| encode_frame(&response, mode))
    }

    pub(crate) fn compose_wire(
        &mut self,
        event: SignalEvent,
        sid: &rtc_core::ids::ParticipantSid,
    ) -> Option<Vec<u8>> {
        let raw = super::blob::response_wire(&event);
        let response = self.compose(event, sid)?;
        Some(raw.unwrap_or_else(|| response.encode_to_vec()))
    }
    /// Compose Join/Reconnect/refresh-token responses; everything else goes
    /// through [`encode_signal`].
    pub fn compose(
        &mut self,
        ev: SignalEvent,
        sid: &rtc_core::ids::ParticipantSid,
    ) -> Option<pb::SignalResponse> {
        let m = match ev {
            SignalEvent::Join(info) => Resp::Join(self.join_response(*info, sid)),
            SignalEvent::Reconnect(info) => Resp::Reconnect(self.reconnect_response(info, sid)),
            SignalEvent::ClaimsChanged(info) => Resp::RefreshToken(self.refresh_token(&info)?),
            other => {
                let ctx = EncodeCtx {
                    client: &self.client,
                };
                return encode_signal(other, &ctx, &mut self.cache);
            }
        };
        Some(pb::SignalResponse { message: Some(m) })
    }

    fn join_response(
        &self,
        info: JoinInfo,
        sid: &rtc_core::ids::ParticipantSid,
    ) -> pb::JoinResponse {
        let cfg = &self.state.config;
        pb::JoinResponse {
            room: Some(convert::room_info_to_proto(&info.room)),
            participant: Some(convert::participant_info_to_proto(&info.participant)),
            other_participants: info
                .other_participants
                .iter()
                .map(convert::participant_info_to_proto)
                .collect(),
            server_version: ice::COMPAT_SERVER_VERSION.to_string(),
            ice_servers: ice::ice_servers_for_preference(
                &self.state,
                &self.grants.api_key,
                sid,
                info.ice_preference == rtc_core::transport_fallback::IcePreference::Tls,
            ),
            subscriber_primary: info.subscriber_primary,
            alternative_url: String::new(),
            client_configuration: ice::client_configuration(
                &self.client,
                info.ice_preference == rtc_core::transport_fallback::IcePreference::Tls,
            ),
            server_region: cfg.region.clone(),
            ping_timeout: info.ping_timeout_s,
            ping_interval: info.ping_interval_s,
            server_info: Some(ice::server_info(cfg)),
            sif_trailer: info.sif_trailer,
            enabled_publish_codecs: info
                .enabled_publish_codecs
                .iter()
                .map(convert::codec_spec_to_proto)
                .collect(),
            fast_publish: info.fast_publish,
        }
    }

    fn reconnect_response(
        &self,
        info: ReconnectInfo,
        sid: &rtc_core::ids::ParticipantSid,
    ) -> pb::ReconnectResponse {
        pb::ReconnectResponse {
            ice_servers: ice::ice_servers_for_preference(
                &self.state,
                &self.grants.api_key,
                sid,
                info.ice_servers_preference == rtc_core::transport_fallback::IcePreference::Tls,
            ),
            client_configuration: ice::client_configuration(
                &self.client,
                info.ice_preference == rtc_core::transport_fallback::IcePreference::Tls,
            ),
            server_info: Some(ice::server_info(&self.state.config)),
            last_message_seq: info.last_message_seq,
        }
    }

    /// Refresh token with current claims (roommanager.go:1156-1191).
    pub(crate) fn refresh_token(&mut self, info: &SignalClaimsSnapshot) -> Option<String> {
        // Keep the stored grants in sync with server-side changes.
        self.grants.name = info.name.clone();
        self.grants.metadata = info.metadata.clone();
        self.grants.attributes = info.attributes.clone();
        // The owner already preserves optional values on metadata/noop changes
        // and materializes them only when SetPermission actually changes state.
        self.grants.video = info.video.clone();
        match self.state.tokens.refresh_token(&self.grants) {
            Ok(t) => Some(t),
            Err(e) => {
                tracing::warn!(error = %e, "could not mint refresh token");
                None
            }
        }
    }
}

/// Drive one upgraded socket until either side ends. `first` is the session's
/// first output (Join, Reconnect or Leave), already received before upgrading.
pub async fn run(
    socket: WebSocket,
    mut handle: SessionHandle,
    first: SessionOutput,
    mut ctx: SessionCtx,
) {
    let manager = ctx.state.manager.clone();
    let sid = handle.participant_sid.clone();
    let admission_state = ctx.state.clone();
    let admission_room = ctx.grants.video.room.clone();
    let admission_identity = handle.identity.clone();
    let admission_sid = sid.clone();
    let admission = async {
        if admission_state.room_control.is_some() {
            admission_state
                .routing_events
                .wait_participant_admitted(&admission_room, &admission_identity.0, &admission_sid.0)
                .await
        } else {
            Ok(())
        }
    };
    tokio::pin!(admission);
    let mut input_admitted = admission_state.room_control.is_none();
    let mut pending_frame = None;
    let (mut tx, mut rx) = socket.split();
    let mut mode = Mode::Binary;
    let mut last_claims: Option<SignalClaimsSnapshot> = None;

    let mut ping = tokio::time::interval(WS_PING_INTERVAL);
    ping.tick().await; // first tick is immediate
    let mut refresh = tokio::time::interval(REFRESH_INTERVAL);
    refresh.tick().await;

    // Write the first message before reading anything.
    let mut closing = false;
    let mut invalid_frame = false;
    let mut message_too_large = false;
    let mut protocol_error_close = None;
    match first {
        SessionOutput::Signal(ev) => {
            if let SignalEvent::ClaimsChanged(info) = &ev {
                last_claims = Some((**info).clone());
            }
            if let Some(frame) = ctx.compose_frame(ev, &sid, mode) {
                if send(&manager, &mut tx, frame).await.is_err() {
                    return;
                }
                manager.record_signal_connected();
            }
        }
        SessionOutput::Close => closing = true,
    }

    while !closing {
        tokio::select! {
            out = handle.output.recv() => {
                match out {
                    Some(SessionOutput::Signal(ev)) => {
                        if let SignalEvent::ClaimsChanged(info) = &ev {
                            last_claims = Some((**info).clone());
                        }
                        let Some(frame) = ctx.compose_frame(ev, &sid, mode) else { continue };
                        if send(&manager, &mut tx, frame).await.is_err() {
                            break;
                        }
                    }
                    Some(SessionOutput::Close) | None => closing = true,
                }
            }
            admitted = &mut admission, if !input_admitted => {
                if let Err(error) = admitted {
                    tracing::warn!(code=%error.code, "initial signal participant admission failed");
                    break;
                }
                input_admitted = true;
            }
            frame = async {
                if input_admitted && pending_frame.is_some() { pending_frame.take() }
                else { rx.next().await }
            }, if input_admitted || pending_frame.is_none() => {
                let Some(frame) = frame else { break };
                if !input_admitted && matches!(&frame, Ok(Message::Binary(_) | Message::Text(_))) {
                    pending_frame = Some(frame);
                    continue;
                }
                let frame = match frame {
                    Ok(frame) => frame,
                    Err(error) => {
                        message_too_large = capacity_message_too_long(&error);
                        invalid_frame=invalid_compressed_frame(&error);
                        protocol_error_close = protocol_close(&error);
                        break;
                    }
                };
                let received_bytes = application_frame_len(&frame);
                let (request, raw_request) = match frame {
                    Message::Binary(b) => {
                        mode = Mode::Binary;
                        match decode_binary_request(b.as_ref()) {
                            Ok(r) => {
                                let keep = matches!(&r.message, Some(pb::signal_request::Message::StoreDataBlobRequest(_) | pb::signal_request::Message::GetDataBlobRequest(_)));
                                (r, keep.then_some(b))
                            },
                            Err(_) => { invalid_frame = true; break; }, // source normal close on undecodable frame
                        }
                    }
                    Message::Text(t) => {
                        mode = Mode::Json;
                        match decode_text_request(t.as_str()) {
                            Ok(r) => (r, None),
                            Err(_) => { invalid_frame = true; break; },
                        }
                    }
                    Message::Close(_) => break,
                    Message::Ping(_) | Message::Pong(_) => continue,
                };
                // Count the original JSON/protobuf application payload only
                // after successful public decoder acceptance, before forwarding.
                manager.record_signaling(received_bytes.unwrap(), false);
                let inbound = if let Some(raw) = raw_request.as_deref() {
                    match super::blob::decode_request(raw) { Ok(value) => value, Err(_) => break }
                } else { decode_request(request, &mut ctx.cache) };
                match inbound {
                    Inbound::Forward(req) => {
                        if !handle.send(req).await {
                            break;
                        }
                    }
                    Inbound::Reply(resp) => {
                        if let Some(frame) = encode_frame(&resp, mode)
                            && send(&manager, &mut tx, frame).await.is_err() {
                                break;
                            }
                    }
                    Inbound::ForwardAndReply(req, resp) => {
                        // rtcservice.go forwards even if the local pong write
                        // failed; only then does this session finish closing.
                        if !reply_then_forward(async {
                            match encode_frame(&resp, mode) {
                                Some(frame) => send(&manager, &mut tx, frame).await.is_ok(),
                                None => true,
                            }
                        }, handle.send(*req)).await { break; }
                    }
                    Inbound::Ignore => {}
                }
            }
            _ = ping.tick() => {
                if send(&manager, &mut tx, Message::Ping(Vec::new().into())).await.is_err() {
                    break;
                }
            }
            _ = refresh.tick() => {
                if let Some(info) = last_claims.clone()
                    && let Some(token) = ctx.refresh_token(&info) {
                        let resp = pb::SignalResponse { message: Some(Resp::RefreshToken(token)) };
                        if let Some(frame) = encode_frame(&resp, mode)
                            && send(&manager, &mut tx, frame).await.is_err() {
                                break;
                            }
                    }
            }
        }
    }

    if closing {
        // Drain anything still queued (e.g. the Leave) then close normally.
        while let Ok(out) = handle.output.try_recv() {
            if let SessionOutput::Signal(ev) = out
                && let Some(frame) = ctx.compose_frame(ev, &sid, mode)
            {
                let _ = send(&manager, &mut tx, frame).await;
            }
        }
    }
    // rtcservice.go447–472 always CloseWithReason("") after a decoder error.
    // Keep core-output draining above distinct from parser-error termination.
    if closing || invalid_frame || message_too_large || protocol_error_close.is_some() {
        let close = Message::Close(Some(protocol_error_close.unwrap_or_else(|| CloseFrame {
            // Gorilla sends close1009 before returning its read-limit error.
            // Keep decoder errors at1000 and unrelated IO/protocol errors intact.
            code: if message_too_large { 1009 } else { 1000 },
            reason: Utf8Bytes::from_static(""),
        })));
        let _ = send(&manager, &mut tx, close).await;
    }
    let _ = tx.close().await;
    // Dropping the handle reports SignalClosed to the room.
    drop(handle);
}

pub(super) fn application_frame_len(frame: &Message) -> Option<usize> {
    match frame {
        Message::Binary(value) => Some(value.len()),
        Message::Text(value) => Some(value.len()),
        _ => None,
    }
}

pub(super) async fn send<S>(
    manager: &rtc_runtime::RoomManager,
    tx: &mut S,
    frame: Message,
) -> Result<(), ()>
where
    S: futures_util::Sink<Message> + Unpin,
{
    let bytes = application_frame_len(&frame);
    match tokio::time::timeout(WRITE_TIMEOUT, tx.send(frame)).await {
        Ok(Ok(())) => {
            if let Some(bytes) = bytes {
                manager.record_signaling(bytes, true);
            }
            Ok(())
        }
        _ => Err(()),
    }
}

#[cfg(test)]
mod binary_signal_regressions {
    use super::*;
    #[test]
    fn known_wrong_wire_does_not_tear_down_an_otherwise_valid_binary_request() {
        // Offer.sdp#2="v", while Offer.type#1 has a legal unknown varint.
        let wire = [0x0a, 5, 0x12, 1, b'v', 0x08, 7];
        assert!(pb::SignalRequest::decode(wire.as_slice()).is_err());
        let actual = decode_binary_request(&wire).unwrap();
        assert!(
            matches!(actual.message,Some(pb::signal_request::Message::Offer(value)) if value.sdp=="v" && value.r#type.is_empty())
        );
        assert!(decode_binary_request(&[0x0a, 3, 0x12, 1, 0xff]).is_err());
        assert!(decode_binary_request(&[0x0a, 2, 0x12]).is_err());
    }
}

#[cfg(test)]
mod livekit_signal_json_oracle {
    use super::*;
    #[test]
    #[ignore = "requires independently executed fixed Go protojson results"]
    fn actual_fixed_livekit_signal_json() {
        let cases: serde_json::Value = serde_json::from_slice(
            &std::fs::read(std::env::var("SIGNAL_JSON_GO_CASES").expect("actual cases")).unwrap(),
        )
        .unwrap();
        let results = std::fs::read_to_string(
            std::env::var("SIGNAL_JSON_GO_RESULTS").expect("actual Go output"),
        )
        .unwrap();
        let rows: Vec<serde_json::Value> = results
            .lines()
            .filter(|s| !s.is_empty())
            .map(|s| serde_json::from_str(s).unwrap())
            .collect();
        let cases = cases.as_array().unwrap();
        assert_eq!(cases.len(), 35);
        assert_eq!(rows.len(), 35);
        for (case, expected) in cases.iter().zip(&rows) {
            assert_eq!(case["id"], expected["id"]);
            let actual = decode_text_request(case["json"].as_str().unwrap());
            assert_eq!(
                actual.is_ok(),
                expected["accepted"].as_bool().unwrap(),
                "case {} actual {:?}",
                case["id"],
                actual
            );
            if let Ok(actual) = actual {
                let wire: Vec<u8> = expected["wire"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .map(|n| n.as_u64().unwrap() as u8)
                    .collect();
                let expected = decode_binary_request(&wire).unwrap();
                assert_eq!(actual, expected, "known semantic projection {}", case["id"]);
            }
        }
    }
}

#[cfg(test)]
mod websocket_capacity_error_tests {
    use super::*;
    #[test]
    fn actual_axum_tungstenite_capacity_cause_is_not_other_protocol_or_io_errors() {
        let capacity = axum::Error::new(tungstenite::Error::Capacity(
            tungstenite::error::CapacityError::MessageTooLong {
                size: 1025,
                max_size: 1024,
            },
        ));
        assert!(capacity_message_too_long(&capacity));
        for error in [
            tungstenite::Error::ConnectionClosed,
            tungstenite::Error::Protocol(
                tungstenite::error::ProtocolError::ResetWithoutClosingHandshake,
            ),
            tungstenite::Error::Io(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "fixture EOF",
            )),
            tungstenite::Error::Capacity(tungstenite::error::CapacityError::TooManyHeaders),
        ] {
            assert!(!capacity_message_too_long(&axum::Error::new(error)));
        }
    }
}

#[cfg(test)]
#[path = "session/claims_snapshot_tests.rs"]
mod claims_snapshot_tests;

#[cfg(test)]
#[path = "session/raw_source_tests.rs"]
mod raw_source_tests;

#[cfg(test)]
#[path = "session/add_track_response_tests.rs"]
mod add_track_response_tests;

#[cfg(test)]
#[path = "session/open_source_tests.rs"]
mod open_source_tests;

#[cfg(test)]
#[path = "session/signaling_telemetry_tests.rs"]
mod signaling_telemetry_tests;

/// Fixed rtcservice.go650-695: attempt local response, then still attempt the
/// original bounded owner sink. A failed response never fakes owner success.
/// The caller retains its existing write timeout, queue and generation fences.
pub(crate) async fn reply_then_forward(
    reply: impl std::future::Future<Output = bool>,
    forward: impl std::future::Future<Output = bool>,
) -> bool {
    let written = reply.await;
    let forwarded = forward.await;
    written && forwarded
}
