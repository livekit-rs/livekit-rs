//! Server-initiated RPC (`RoomService.PerformRpc`, participant.go:4631-4751).
//!
//! The server sends a reliable `DataPacket{participant_identity: <id>,
//! rpc_request{id, method, payload, response_timeout_ms, version: 1}}` to the
//! destination participant and waits for its `rpc_ack` (2 s) and `rpc_response`
//! (the response timeout, default 10 s). Acks and responses from the destination
//! participant whose request id is pending here are consumed by
//! [`crate::data::LiveKitDataHandler`] and routed to [`RpcRegistry`].
//!
//! The public standalone and distributed paths use real typed PSRPC consumers.
//! A captured owner provides genuine RR media RTT for the wire response budget;
//! the server's absolute ACK and response deadlines retain their full budgets.

use std::collections::HashMap;
use std::sync::Mutex;
use std::time::Duration;

use lk_protocol::livekit as pb;
use prost::Message;
use rtc_core::ids::{Identity, ParticipantSid, RoomKey};
use rtc_core::media_api::DataKind;
use rtc_runtime::ApiError;
use tokio::sync::oneshot;

use crate::state::AppState;

/// `DataChannelRpcMaxPayloadBytes`.
pub const MAX_PAYLOAD_BYTES: usize = 15_360;
/// `DataChannelRpcMaxRoundTripLatency`.
pub const ACK_TIMEOUT: Duration = Duration::from_millis(2_000);
/// `DataChannelRpcDefaultResponseTimeout`.
pub const DEFAULT_RESPONSE_TIMEOUT_MS: u32 = 10_000;
/// Bound on concurrently pending server RPCs.
pub const MAX_PENDING: usize = 4_096;

/// Built-in `DataChannelRpcErrorCode`s.
pub mod codes {
    pub const APPLICATION_ERROR: u32 = 1500;
    pub const CONNECTION_TIMEOUT: u32 = 1501;
    pub const RESPONSE_TIMEOUT: u32 = 1502;
    pub const RECIPIENT_DISCONNECTED: u32 = 1503;
    pub const RESPONSE_PAYLOAD_TOO_LARGE: u32 = 1504;
    pub const SEND_FAILED: u32 = 1505;
    pub const UNSUPPORTED_METHOD: u32 = 1400;
    pub const RECIPIENT_NOT_FOUND: u32 = 1401;
    pub const REQUEST_PAYLOAD_TOO_LARGE: u32 = 1402;
    pub const UNSUPPORTED_SERVER: u32 = 1403;
    pub const UNSUPPORTED_VERSION: u32 = 1404;
}

/// psrpc error class an RPC error maps to (`DataChannelRpcError.PsrpcError`).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RpcErrorClass {
    Internal,
    Canceled,
    Unavailable,
    InvalidArgument,
    NotFound,
    Unimplemented,
}

/// `utils.DataChannelRpcError`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DataChannelRpcError {
    pub code: u32,
    pub message: String,
    pub data: String,
}

impl DataChannelRpcError {
    pub fn built_in(code: u32) -> Self {
        let message = match code {
            codes::APPLICATION_ERROR => "Application error in method handler",
            codes::CONNECTION_TIMEOUT => "Connection timeout",
            codes::RESPONSE_TIMEOUT => "Response timeout",
            codes::RECIPIENT_DISCONNECTED => "Recipient disconnected",
            codes::RESPONSE_PAYLOAD_TOO_LARGE => "Response payload too large",
            codes::SEND_FAILED => "Failed to send",
            codes::UNSUPPORTED_METHOD => "Method not supported at destination",
            codes::RECIPIENT_NOT_FOUND => "Recipient not found",
            codes::REQUEST_PAYLOAD_TOO_LARGE => "Request payload too large",
            codes::UNSUPPORTED_SERVER => "RPC not supported by server",
            codes::UNSUPPORTED_VERSION => "Unsupported RPC version",
            _ => "",
        };
        Self {
            code,
            message: message.to_string(),
            data: String::new(),
        }
    }

    /// psrpc code; MalformedRequest → invalid_argument and MalformedResponse →
    /// internal after the Twirp translation.
    pub fn class(&self) -> RpcErrorClass {
        match self.code {
            codes::CONNECTION_TIMEOUT | codes::RESPONSE_TIMEOUT => RpcErrorClass::Canceled,
            codes::RECIPIENT_DISCONNECTED => RpcErrorClass::Unavailable,
            codes::UNSUPPORTED_METHOD | codes::REQUEST_PAYLOAD_TOO_LARGE => {
                RpcErrorClass::InvalidArgument
            }
            codes::RECIPIENT_NOT_FOUND => RpcErrorClass::NotFound,
            codes::UNSUPPORTED_SERVER | codes::UNSUPPORTED_VERSION => RpcErrorClass::Unimplemented,
            _ => RpcErrorClass::Internal,
        }
    }
}

impl std::fmt::Display for DataChannelRpcError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.data.is_empty() {
            write!(f, "RpcError {}: {}", self.code, self.message)
        } else {
            write!(f, "RpcError {}: {}: {}", self.code, self.message, self.data)
        }
    }
}

/// PerformRpc failure.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RpcFailure {
    Rpc(DataChannelRpcError),
    Api(ApiError),
}

/// What the destination answered.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RpcOutcome {
    Payload(String),
    Error(DataChannelRpcError),
}

struct Pending {
    participant: ParticipantSid,
    ack: Option<oneshot::Sender<()>>,
    response: Option<oneshot::Sender<RpcOutcome>>,
}

/// Pending server-initiated RPCs, keyed by request id.
#[derive(Default)]
pub struct RpcRegistry {
    pending: Mutex<HashMap<String, Pending>>,
}

impl RpcRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    pub(crate) fn register(
        &self,
        id: &str,
        participant: ParticipantSid,
    ) -> Option<(oneshot::Receiver<()>, oneshot::Receiver<RpcOutcome>)> {
        let mut map = self.pending.lock().unwrap_or_else(|e| e.into_inner());
        if map.len() >= MAX_PENDING {
            return None;
        }
        let (ack_tx, ack_rx) = oneshot::channel();
        let (resp_tx, resp_rx) = oneshot::channel();
        map.insert(
            id.to_string(),
            Pending {
                participant,
                ack: Some(ack_tx),
                response: Some(resp_tx),
            },
        );
        Some((ack_rx, resp_rx))
    }

    fn remove(&self, id: &str) {
        self.pending
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .remove(id);
    }

    /// `handleIncomingRpcAck`: `true` if `request_id` is a pending server RPC sent
    /// to `sender` (the packet is then consumed).
    pub fn handle_ack(&self, sender: &ParticipantSid, request_id: &str) -> bool {
        let mut map = self.pending.lock().unwrap_or_else(|e| e.into_inner());
        match map.get_mut(request_id) {
            Some(p) if &p.participant == sender => {
                if let Some(tx) = p.ack.take() {
                    let _ = tx.send(());
                }
                true
            }
            _ => false,
        }
    }

    /// `handleIncomingRpcResponse`: resolves (and removes) the pending RPC.
    pub fn handle_response(
        &self,
        sender: &ParticipantSid,
        request_id: &str,
        outcome: RpcOutcome,
    ) -> bool {
        let mut map = self.pending.lock().unwrap_or_else(|e| e.into_inner());
        let matches = map
            .get(request_id)
            .is_some_and(|p| &p.participant == sender);
        if !matches {
            return false;
        }
        if let Some(mut p) = map.remove(request_id) {
            if let Some(tx) = p.ack.take() {
                let _ = tx.send(());
            }
            if let Some(tx) = p.response.take() {
                let _ = tx.send(outcome);
            }
        }
        true
    }

    /// Fixed Participant.Close resolves real accepted RPCs with1503.
    pub(crate) fn disconnect_participant(&self, participant: &ParticipantSid) {
        let mut pending = self.pending.lock().unwrap_or_else(|p| p.into_inner());
        let ids: Vec<_> = pending
            .iter()
            .filter(|(_, value)| &value.participant == participant)
            .map(|(id, _)| id.clone())
            .collect();
        for id in ids {
            if let Some(mut value) = pending.remove(&id) {
                if let Some(ack) = value.ack.take() {
                    let _ = ack.send(());
                }
                if let Some(response) = value.response.take() {
                    let _ = response.send(RpcOutcome::Error(DataChannelRpcError::built_in(
                        codes::RECIPIENT_DISCONNECTED,
                    )));
                }
            }
        }
    }
    pub fn pending_count(&self) -> usize {
        self.pending.lock().unwrap_or_else(|e| e.into_inner()).len()
    }
}

/// Removes the pending entry on every exit path.
struct PendingGuard<'a> {
    registry: &'a RpcRegistry,
    id: String,
}

impl Drop for PendingGuard<'_> {
    fn drop(&mut self) {
        self.registry.remove(&self.id);
    }
}

/// Random RFC 4122 version-4 UUID string (`uuid.NewString()`).
pub fn new_uuid_v4() -> String {
    let mut b = [0u8; 16];
    rtc_core::util::fill_random(&mut b);
    b[6] = (b[6] & 0x0f) | 0x40;
    b[8] = (b[8] & 0x3f) | 0x80;
    let hex: String = b.iter().map(|x| format!("{x:02x}")).collect();
    format!(
        "{}-{}-{}-{}-{}",
        &hex[0..8],
        &hex[8..12],
        &hex[12..16],
        &hex[16..20],
        &hex[20..32]
    )
}

/// Encoded `DataPacket` carrying the RPC request.
pub fn rpc_request_packet(
    id: &str,
    method: &str,
    payload: &str,
    response_timeout_ms: u32,
) -> Vec<u8> {
    pb::DataPacket {
        kind: pb::data_packet::Kind::Reliable as i32,
        participant_identity: id.to_string(),
        value: Some(pb::data_packet::Value::RpcRequest(pb::RpcRequest {
            id: id.to_string(),
            method: method.to_string(),
            payload: payload.to_string(),
            response_timeout_ms,
            version: 1,
            ..Default::default()
        })),
        ..Default::default()
    }
    .encode_to_vec()
}

/// Perform an RPC on `destination` in `room`; returns the response payload.
pub async fn perform_rpc(
    state: &AppState,
    room: &RoomKey,
    destination: &Identity,
    method: &str,
    payload: &str,
    response_timeout_ms: u32,
) -> Result<String, RpcFailure> {
    let info = state
        .manager
        .list_rooms(&room.project, &[room.name.clone()])
        .into_iter()
        .next()
        .ok_or_else(|| RpcFailure::Api(ApiError::Unavailable("no response from servers".into())))?;
    let owner = state
        .manager
        .capture_room_owner(room, &info.sid)
        .map_err(RpcFailure::Api)?;
    perform_rpc_bound(
        state,
        room,
        destination,
        method,
        payload,
        response_timeout_ms,
        Some(&owner),
    )
    .await
}

/// Accepted PSRPC work retains the exact owner actor across name reuse.
pub(crate) async fn perform_rpc_bound(
    state: &AppState,
    room: &RoomKey,
    destination: &Identity,
    method: &str,
    payload: &str,
    response_timeout_ms: u32,
    owner: Option<&rtc_runtime::RoomOwnerHandle>,
) -> Result<String, RpcFailure> {
    perform_rpc_impl(
        &state.rpc,
        Some(state),
        room,
        destination,
        method,
        payload,
        response_timeout_ms,
        owner,
        None,
    )
    .await
}
pub(crate) async fn perform_rpc_owned(
    registry: &RpcRegistry,
    owner: &rtc_runtime::RoomOwnerHandle,
    destination: &Identity,
    method: &str,
    payload: &str,
    response_timeout_ms: u32,
    expected_sid: &ParticipantSid,
) -> Result<String, RpcFailure> {
    perform_rpc_impl(
        registry,
        None,
        owner.key(),
        destination,
        method,
        payload,
        response_timeout_ms,
        Some(owner),
        Some(expected_sid),
    )
    .await
}
async fn perform_rpc_impl(
    registry: &RpcRegistry,
    state: Option<&AppState>,
    room: &RoomKey,
    destination: &Identity,
    method: &str,
    payload: &str,
    response_timeout_ms: u32,
    owner: Option<&rtc_runtime::RoomOwnerHandle>,
    expected_sid: Option<&ParticipantSid>,
) -> Result<String, RpcFailure> {
    let timeout_ms = if response_timeout_ms == 0 {
        DEFAULT_RESPONSE_TIMEOUT_MS
    } else {
        response_timeout_ms
    };
    if payload.len() > MAX_PAYLOAD_BYTES {
        return Err(RpcFailure::Rpc(DataChannelRpcError::built_in(
            codes::REQUEST_PAYLOAD_TOO_LARGE,
        )));
    }
    // Upstream routes to the participant topic; nobody answers for a missing
    // participant ("no response from servers").
    let lookup = match owner {
        Some(owner) => owner.rpc_participant(destination.clone()).await,
        None => {
            let state = state.expect("standalone caller state");
            let info = state.manager.get_participant(room, destination).await;
            match info {
                Ok(info) => Ok((info.sid, 0)),
                Err(error) => Err(error),
            }
        }
    };
    let (participant, media_rtt_ms) = match lookup {
        Ok(participant) => participant,
        Err(ApiError::NotFound(_)) | Err(ApiError::Unavailable(_)) => {
            return Err(RpcFailure::Api(ApiError::Unavailable(
                "no response from servers".into(),
            )));
        }
        Err(e) => return Err(RpcFailure::Api(e)),
    };

    if expected_sid.is_some_and(|expected| expected != &participant) {
        return Err(RpcFailure::Api(ApiError::Unavailable(
            "no response from servers".into(),
        )));
    }
    let id = new_uuid_v4();
    let Some((ack_rx, resp_rx)) = registry.register(&id, participant.clone()) else {
        return Err(RpcFailure::Api(ApiError::Unavailable(
            "too many pending RPC requests".into(),
        )));
    };
    let _guard = PendingGuard {
        registry,
        id: id.clone(),
    };

    let started = tokio::time::Instant::now();
    let packet = rpc_request_packet(&id, method, payload, timeout_ms.wrapping_sub(media_rtt_ms));
    let sending = async {
        match owner {
            Some(owner) => {
                owner
                    .send_data_with_nonce(
                        DataKind::Reliable,
                        packet,
                        Vec::new(),
                        vec![participant.clone()],
                        Vec::new(),
                    )
                    .await
            }
            None => {
                state
                    .expect("standalone caller state")
                    .manager
                    .send_data(
                        room,
                        DataKind::Reliable,
                        packet,
                        vec![destination.clone()],
                        Vec::new(),
                    )
                    .await
            }
        }
    };
    send_with_outcome(
        started,
        sending,
        ack_rx,
        resp_rx,
        ACK_TIMEOUT,
        Duration::from_millis(timeout_ms as u64),
    )
    .await
}

/// Fixed participant.PerformRpc starts its timers before SendDataMessage. A
/// reliable transport waiting for fanout/backpressure cannot postpone either
/// deadline, and a genuine response is observable while that send is pending.
async fn send_with_outcome<SendingFuture>(
    started: tokio::time::Instant,
    sending: SendingFuture,
    ack_rx: oneshot::Receiver<()>,
    resp_rx: oneshot::Receiver<RpcOutcome>,
    ack_timeout: Duration,
    response_timeout: Duration,
) -> Result<String, RpcFailure>
where
    SendingFuture: std::future::Future<Output = Result<(), ApiError>>,
{
    let outcome = wait_for_outcome_at(ack_rx, resp_rx, started, ack_timeout, response_timeout);
    tokio::pin!(outcome, sending);
    tokio::select! {
        biased;
        result = &mut outcome => return result,
        sent = &mut sending => if let Err(error) = sent {
            return Err(RpcFailure::Api(ApiError::Internal(error.to_string())));
        },
    }
    outcome.await
}

/// Compatibility helper for the isolated outcome tests.
#[cfg(test)]
async fn wait_for_outcome(
    ack_rx: oneshot::Receiver<()>,
    resp_rx: oneshot::Receiver<RpcOutcome>,
    ack_timeout: Duration,
    response_timeout: Duration,
) -> Result<String, RpcFailure> {
    wait_for_outcome_at(
        ack_rx,
        resp_rx,
        tokio::time::Instant::now(),
        ack_timeout,
        response_timeout,
    )
    .await
}
async fn wait_for_outcome_at(
    mut ack_rx: oneshot::Receiver<()>,
    mut resp_rx: oneshot::Receiver<RpcOutcome>,
    started: tokio::time::Instant,
    ack_timeout: Duration,
    response_timeout: Duration,
) -> Result<String, RpcFailure> {
    let ack_timer = tokio::time::sleep_until(started + ack_timeout);
    let resp_timer = tokio::time::sleep_until(started + response_timeout);
    tokio::pin!(ack_timer);
    tokio::pin!(resp_timer);
    let mut acked = false;
    loop {
        tokio::select! {
            r = &mut ack_rx, if !acked => {
                // A dropped sender (entry removed) also stops the ack timer; the
                // response branch decides the outcome.
                let _ = r;
                acked = true;
            }
            r = &mut resp_rx => {
                return match r {
                    Ok(RpcOutcome::Payload(p)) => Ok(p),
                    Ok(RpcOutcome::Error(e)) => Err(RpcFailure::Rpc(e)),
                    Err(_) => Err(RpcFailure::Rpc(DataChannelRpcError::built_in(codes::RECIPIENT_DISCONNECTED))),
                };
            }
            _ = &mut ack_timer, if !acked => {
                return Err(RpcFailure::Rpc(DataChannelRpcError::built_in(codes::CONNECTION_TIMEOUT)));
            }
            _ = &mut resp_timer => {
                return Err(RpcFailure::Rpc(DataChannelRpcError::built_in(codes::RESPONSE_TIMEOUT)));
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn uuid_format() {
        let u = new_uuid_v4();
        assert_eq!(u.len(), 36);
        assert_eq!(&u[14..15], "4");
        assert!(matches!(&u[19..20], "8" | "9" | "a" | "b"));
        assert_ne!(u, new_uuid_v4());
    }

    #[test]
    fn error_text_and_class() {
        let e = DataChannelRpcError::built_in(codes::CONNECTION_TIMEOUT);
        assert_eq!(e.to_string(), "RpcError 1501: Connection timeout");
        assert_eq!(e.class(), RpcErrorClass::Canceled);
        let e = DataChannelRpcError {
            code: 1500,
            message: "boom".into(),
            data: "d".into(),
        };
        assert_eq!(e.to_string(), "RpcError 1500: boom: d");
        assert_eq!(e.class(), RpcErrorClass::Internal);
        assert_eq!(
            DataChannelRpcError::built_in(1402).class(),
            RpcErrorClass::InvalidArgument
        );
        assert_eq!(
            DataChannelRpcError {
                code: 7,
                message: String::new(),
                data: String::new()
            }
            .class(),
            RpcErrorClass::Internal
        );
    }

    #[test]
    fn registry_matches_sender() {
        let r = RpcRegistry::new();
        let alice = ParticipantSid("alice".into());
        let bob = ParticipantSid("bob".into());
        let (_a, _b) = r.register("id1", alice.clone()).unwrap();
        assert!(!r.handle_ack(&bob, "id1"));
        assert!(r.handle_ack(&alice, "id1"));
        assert!(!r.handle_response(&alice, "other", RpcOutcome::Payload(String::new())));
        assert!(r.handle_response(&alice, "id1", RpcOutcome::Payload("x".into())));
        assert_eq!(r.pending_count(), 0);
        assert!(!r.handle_ack(&alice, "id1"));
    }

    #[test]
    fn request_packet_decodes() {
        let bytes = rpc_request_packet("id", "m", "p", 500);
        let dp = pb::DataPacket::decode(bytes.as_slice()).unwrap();
        assert_eq!(dp.participant_identity, "id");
        match dp.value {
            Some(pb::data_packet::Value::RpcRequest(r)) => {
                assert_eq!(r.method, "m");
                assert_eq!(r.version, 1);
                assert_eq!(r.response_timeout_ms, 500);
            }
            other => panic!("unexpected {other:?}"),
        }
    }

    fn rt() -> tokio::runtime::Runtime {
        tokio::runtime::Builder::new_current_thread()
            .enable_time()
            .build()
            .unwrap()
    }

    #[test]
    fn timers() {
        rt().block_on(async {
            let r = RpcRegistry::new();
            let (ack, resp) = r.register("a", ParticipantSid("x".into())).unwrap();
            let e = wait_for_outcome(ack, resp, Duration::from_millis(10), Duration::from_secs(5))
                .await;
            assert_eq!(e, Err(RpcFailure::Rpc(DataChannelRpcError::built_in(1501))));

            let (ack, resp) = r.register("b", ParticipantSid("x".into())).unwrap();
            assert!(r.handle_ack(&ParticipantSid("x".into()), "b"));
            let e = wait_for_outcome(
                ack,
                resp,
                Duration::from_millis(10),
                Duration::from_millis(30),
            )
            .await;
            assert_eq!(e, Err(RpcFailure::Rpc(DataChannelRpcError::built_in(1502))));

            let (ack, resp) = r.register("c", ParticipantSid("x".into())).unwrap();
            assert!(r.handle_response(
                &ParticipantSid("x".into()),
                "c",
                RpcOutcome::Payload("ok".into())
            ));
            let ok = wait_for_outcome(
                ack,
                resp,
                Duration::from_millis(10),
                Duration::from_millis(30),
            )
            .await;
            assert_eq!(ok, Ok("ok".into()));
        });
    }

    #[tokio::test]
    async fn pending_guard_cleans_up_timeout_and_aborted_waiter() {
        let registry = std::sync::Arc::new(RpcRegistry::new());
        let result = {
            let (ack, response) = registry
                .register("timeout", ParticipantSid("recipient".into()))
                .unwrap();
            let _guard = PendingGuard {
                registry: &registry,
                id: "timeout".into(),
            };
            wait_for_outcome(
                ack,
                response,
                Duration::from_millis(1),
                Duration::from_secs(5),
            )
            .await
        };
        assert_eq!(
            result,
            Err(RpcFailure::Rpc(DataChannelRpcError::built_in(
                codes::CONNECTION_TIMEOUT
            )))
        );
        assert_eq!(registry.pending_count(), 0);

        let waiting_registry = registry.clone();
        let (ready, registered) = oneshot::channel();
        let task = tokio::spawn(async move {
            let (ack, response) = waiting_registry
                .register("aborted", ParticipantSid("recipient".into()))
                .unwrap();
            let _guard = PendingGuard {
                registry: &waiting_registry,
                id: "aborted".into(),
            };
            ready.send(()).unwrap();
            wait_for_outcome(
                ack,
                response,
                Duration::from_secs(60),
                Duration::from_secs(60),
            )
            .await
        });
        registered.await.unwrap();
        assert_eq!(registry.pending_count(), 1);
        task.abort();
        assert!(task.await.unwrap_err().is_cancelled());
        assert_eq!(registry.pending_count(), 0);
    }
}

#[cfg(test)]
mod send_deadline_tests;
