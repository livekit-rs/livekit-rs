// Copyright 2024 LiveKit, Inc. Apache-2.0; adapted from fixed
// pkg/agent/endpoint/wire/control.go + pkg/service/agentwt.go + pkg/agent/worker.go.
// See CONTRACT.md and SOURCE.json for implementation and verification boundaries.
//! SPEC-0074 C15: the agent-over-WebTransport control plane.
//!
//! After a worker upgrades `/agent` over WebTransport it opens one bidirectional
//! stream and runs the same `WorkerMessage`/`ServerMessage` exchange as the
//! WebSocket path, but framed as a 4-byte big-endian length prefix followed by
//! the marshalled protobuf (Go `wire.WriteControlMessage`/`ReadControlMessage`,
//! bounded by `MaxControlMessageSize = 1 << 20`). This module reproduces Go's
//! `wtSignalConn` over that framing and shares the transport-agnostic control
//! logic: the `HandshakeAgentWorker` Register loop (`agent.RegisterTimeout`) and
//! the `DispatchWorkerSignal` arms re-entered here mirror `pkg/agent/worker.go`
//! exactly, reusing the same [`Registration::from_request`] validation,
//! [`Registry`] bookkeeping and the production-built [`pb::ServerInfo`].
//!
//! Unlike the WebSocket adapter there is no JSON codec, no 10s keepalive ping
//! and no Close-frame ACK: WebTransport tears the session down at the
//! connection/stream level, which the caller (the `livecloud` listener) performs
//! when [`session`] returns, matching Go's deferred `sigConn.Close()`.

use super::{
    registry::{Outbound, REGISTER_TIMEOUT, Registration, Registry, WorkerError, send_message},
    websocket::{WorkerEvent, WorkerHttpState},
};
use lk_protocol::livekit as pb;
use prost::Message as _;
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt},
    sync::mpsc,
    task::JoinSet,
};

/// Go `wire.MaxControlMessageSize`: bound one framed control message so a
/// malformed length prefix cannot make the reader allocate without bound.
const MAX_CONTROL_MESSAGE_SIZE: u32 = 1 << 20;

fn now_ms() -> i64 {
    rtc_core::util::unix_ms()
}

fn now_ns() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos()
        .min(i64::MAX as u128) as i64
}

/// `wire.writeLenPrefixed`: marshal the message, write a 4-byte big-endian
/// length, then the body, as one atomic frame on the control stream.
fn encode_control(message: &pb::ServerMessage) -> Result<Vec<u8>, WorkerError> {
    let body = message.encode_to_vec();
    let len = u32::try_from(body.len()).map_err(|_| WorkerError::Closed)?;
    let mut frame = Vec::with_capacity(4 + body.len());
    frame.extend_from_slice(&len.to_be_bytes());
    frame.extend_from_slice(&body);
    Ok(frame)
}

/// Read exactly `buf.len()` bytes. Returns `Ok(false)` only for a clean
/// end-of-stream with zero bytes read (Go `io.ReadFull` -> `io.EOF`); a partial
/// read is an error (Go `ErrUnexpectedEOF`).
async fn read_full<R: AsyncRead + Unpin>(
    read: &mut R,
    buf: &mut [u8],
) -> Result<bool, WorkerError> {
    let mut filled = 0;
    while filled < buf.len() {
        match read.read(&mut buf[filled..]).await {
            Ok(0) => {
                if filled == 0 {
                    return Ok(false);
                }
                return Err(WorkerError::Closed);
            }
            Ok(n) => filled += n,
            Err(_) => return Err(WorkerError::Closed),
        }
    }
    Ok(true)
}

/// `wire.readLenPrefixed` into a `WorkerMessage`. `Ok(None)` marks a clean
/// stream end at a message boundary (the worker closed the control stream).
async fn read_control<R: AsyncRead + Unpin>(
    read: &mut R,
) -> Result<Option<pb::WorkerMessage>, WorkerError> {
    let mut header = [0u8; 4];
    if !read_full(read, &mut header).await? {
        return Ok(None);
    }
    let len = u32::from_be_bytes(header);
    if len > MAX_CONTROL_MESSAGE_SIZE {
        return Err(WorkerError::InvalidRegistration(format!(
            "message too large: {len} bytes"
        )));
    }
    let mut body = vec![0u8; usize::try_from(len).unwrap_or(usize::MAX)];
    if !read_full(read, &mut body).await? {
        // End of stream mid-body: Go surfaces `io.ErrUnexpectedEOF` and the
        // read loop ends without dispatching a partial message.
        return Err(WorkerError::Closed);
    }
    let message = pb::WorkerMessage::decode(&body[..])
        .map_err(|_| WorkerError::InvalidRegistration("invalid worker message".into()))?;
    Ok(Some(message))
}

/// Abort the writer task when the session ends, mirroring `websocket::WriterTask`.
struct WriterTask(tokio::task::JoinHandle<()>);
impl Drop for WriterTask {
    fn drop(&mut self) {
        self.0.abort();
    }
}

/// Deregister a registered worker exactly once, mirroring `websocket::SessionGuard`.
struct SessionGuard {
    registry: Registry,
    id: String,
    registered: bool,
}
impl SessionGuard {
    fn disconnect(&mut self) -> Vec<String> {
        if !self.registered {
            return Vec::new();
        }
        self.registered = false;
        self.registry.disconnect(&self.id)
    }
}
impl Drop for SessionGuard {
    fn drop(&mut self) {
        self.disconnect();
    }
}

/// Serve one worker's WebTransport control stream: run the bounded Register
/// handshake, then the dispatch loop, writing every `ServerMessage` (register
/// reply, pong, job assignment) length-prefixed onto the stream. `read`/`write`
/// are the two halves of a split [`WtBidiStream`]. Returns when the worker
/// closes the stream, a dispatch fails, or shutdown is signaled; the caller
/// then closes the connection (Go's deferred `sigConn.Close()`).
pub async fn session<R, W>(
    state: WorkerHttpState,
    read: R,
    write: W,
    protocol: i64,
    api_key: String,
) where
    R: AsyncRead + Unpin + Send + 'static,
    W: AsyncWrite + Unpin + Send + 'static,
{
    session_with_endpoints(state, read, write, protocol, api_key, None).await;
}

/// The endpoint opener belongs to this exact WT session, never another worker
/// or a later reconnect. The plain session entry point preserves control-only
/// fixtures and never advertises a data plane without an actual opener.
pub async fn session_with_endpoints<R, W>(
    mut state: WorkerHttpState,
    read: R,
    write: W,
    protocol: i64,
    api_key: String,
    endpoint_opener: Option<super::endpoint::StreamOpener>,
) where
    R: AsyncRead + Unpin + Send + 'static,
    W: AsyncWrite + Unpin + Send + 'static,
{
    if *state.shutdown.borrow() {
        return;
    }
    let (outgoing, mut queue) = mpsc::channel::<Outbound>(64);
    let mut write = write;
    // A single writer owns the send half; every server->worker message goes
    // through the shared `outgoing` channel so writes stay serialized exactly
    // as Go's `wtSignalConn.writeMu` serializes `WriteServerMessage`.
    let mut writer = WriterTask(tokio::spawn(async move {
        while let Some(value) = queue.recv().await {
            let result = match encode_control(&value.message) {
                Ok(frame) => write
                    .write_all(&frame)
                    .await
                    .map_err(|_| WorkerError::Closed),
                Err(e) => Err(e),
            };
            let failed = result.is_err();
            let _ = value.written.send(result);
            if failed {
                break;
            }
        }
    }));
    let mut read = read;
    let id = rtc_core::ids::new_sid("AW_");
    let mut guard = SessionGuard {
        registry: state.registry.clone(),
        id: id.clone(),
        registered: false,
    };

    let result = async {
        // HandshakeAgentWorker: loop until Registered within `RegisterTimeout`.
        let until = tokio::time::Instant::now() + REGISTER_TIMEOUT;
        loop {
            let message = tokio::time::timeout_at(until, read_control(&mut read))
                .await
                .map_err(|_| WorkerError::Closed)??;
            let Some(message) = message else {
                return Ok::<(), WorkerError>(());
            };
            match message.message {
                Some(pb::worker_message::Message::Ping(p)) => {
                    send_message(&outgoing, pong(p.timestamp)).await?
                }
                Some(pb::worker_message::Message::Register(request)) => {
                    let mut registration =
                        Registration::from_request(id.clone(), protocol, &request)?;
                    registration.api_key = api_key.clone();
                    let endpoints = if endpoint_opener.is_some() && !request.endpoints.is_empty() {
                        Some(state.registry.endpoints.negotiate(&request)?)
                    } else {
                        None
                    };
                    let endpoint_settings = endpoints
                        .as_ref()
                        .map(|_| pb::agent_http::AgentEndpointSettings { protocol: 1 });
                    send_message(
                        &outgoing,
                        pb::ServerMessage {
                            message: Some(pb::server_message::Message::Register(
                                pb::RegisterWorkerResponse {
                                    worker_id: id.clone(),
                                    server_info: Some(state.server_info.clone()),
                                    endpoint_settings,
                                },
                            )),
                        },
                    )
                    .await?;
                    state
                        .registry
                        .register_ready(registration.clone(), outgoing.clone())
                        .await?;
                    if let (Some(manifest), Some(opener)) = (endpoints, endpoint_opener.clone()) {
                        state
                            .registry
                            .endpoints
                            .register(&registration, manifest, opener);
                    }
                    guard.registered = true;
                    state
                        .events
                        .send(WorkerEvent::Registered {
                            worker_id: id.clone(),
                        })
                        .await
                        .map_err(|_| WorkerError::Closed)?;
                    break;
                }
                _ => {
                    return Err::<(), WorkerError>(WorkerError::InvalidRegistration(
                        "unknown or unimplemented worker signal before registration".into(),
                    ));
                }
            }
        }

        // handleConnection dispatch loop: unbounded, mirrors DispatchWorkerSignal.
        let mut simulated = JoinSet::new();
        loop {
            let Some(message) = read_control(&mut read).await? else {
                return Ok(());
            };
            match message.message {
                Some(pb::worker_message::Message::Ping(p)) => {
                    send_message(&outgoing, pong(p.timestamp)).await?
                }
                Some(pb::worker_message::Message::UpdateWorker(update)) => {
                    state.registry.update_worker(&id, &update)?
                }
                Some(pb::worker_message::Message::Availability(response)) => {
                    let _ = state.registry.availability(&id, response);
                }
                Some(pb::worker_message::Message::UpdateJob(update)) => {
                    if let Ok(job_state) = state.registry.update_job(&id, &update, now_ns()) {
                        state
                            .events
                            .send(WorkerEvent::JobState {
                                worker_id: id.clone(),
                                job_id: update.job_id,
                                state: job_state,
                            })
                            .await
                            .map_err(|_| WorkerError::Closed)?;
                    }
                }
                Some(pb::worker_message::Message::MigrateJob(_)) => {} // fixed OSS intentionally no-op
                Some(pb::worker_message::Message::SimulateJob(request)) => {
                    while simulated.try_join_next().is_some() {}
                    if simulated.len() >= 256 {
                        return Err(WorkerError::Capacity);
                    }
                    let registry = state.registry.clone();
                    let tokens = state.tokens.clone();
                    let worker = id.clone();
                    let registration = registry.registration(&id).ok_or(WorkerError::Closed)?;
                    let job = pb::Job {
                        id: rtc_core::ids::new_sid("AJ_"),
                        r#type: if request.participant.is_some() { 1 } else { 0 },
                        room: request.room,
                        participant: request.participant,
                        namespace: registration.key.namespace,
                        agent_name: registration.key.agent_name,
                        ..Default::default()
                    };
                    simulated.spawn(async move {
                        let _ = registry
                            .assign(&worker, job, now_ns(), |r, j, a| {
                                super::token::mint_assignment(
                                    &tokens,
                                    &r.api_key,
                                    r,
                                    j,
                                    a,
                                    now_ms() / 1000,
                                )
                            })
                            .await;
                    });
                }
                _ => {
                    return Err::<(), WorkerError>(WorkerError::InvalidRegistration(
                        "unknown or unimplemented worker signal".into(),
                    ));
                }
            }
        }
    };

    tokio::select! {
        _ = &mut writer.0 => {}
        outcome = result => {
            // Go logs the dispatch error and returns false; the observable
            // effect is that the connection is torn down, done by the caller.
            let _ = outcome;
        }
        _ = state.shutdown.changed() => {}
    }

    if guard.registered {
        let jobs = guard.disconnect();
        let _ = state
            .events
            .send(WorkerEvent::Disconnected {
                worker_id: id,
                jobs,
            })
            .await;
    }
}

/// `WorkerPingHandler.HandlePing`: reply with a `WorkerPong` echoing the
/// request timestamp and stamping the server's current time in milliseconds.
fn pong(last_timestamp: i64) -> pb::ServerMessage {
    pb::ServerMessage {
        message: Some(pb::server_message::Message::Pong(pb::WorkerPong {
            last_timestamp,
            timestamp: now_ms(),
        })),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::agent::registry::{Limits, Registry};
    use crate::auth::{KeyStore, TokenMinter};
    use std::sync::Arc;
    use std::time::Duration;
    use tokio::io::{AsyncReadExt, AsyncWriteExt, duplex, split};
    use tokio::sync::{mpsc, watch};

    const MAX: u32 = MAX_CONTROL_MESSAGE_SIZE;

    // The test side mirrors the wire: 4-byte big-endian length + marshalled proto.
    async fn send_worker<W: AsyncWrite + Unpin>(w: &mut W, message: pb::WorkerMessage) {
        let body = message.encode_to_vec();
        let mut frame = Vec::with_capacity(4 + body.len());
        frame.extend_from_slice(&(body.len() as u32).to_be_bytes());
        frame.extend_from_slice(&body);
        w.write_all(&frame).await.unwrap();
        w.flush().await.unwrap();
    }

    async fn recv_server<R: AsyncRead + Unpin>(r: &mut R) -> Option<pb::ServerMessage> {
        let mut header = [0u8; 4];
        match r.read_exact(&mut header).await {
            Ok(_) => {}
            Err(_) => return None,
        }
        let len = usize::try_from(u32::from_be_bytes(header)).unwrap_or(usize::MAX);
        let mut body = vec![0u8; len];
        if r.read_exact(&mut body).await.is_err() {
            return None;
        }
        pb::ServerMessage::decode(&body[..]).ok()
    }

    fn register_request(name: &str) -> pb::WorkerMessage {
        pb::WorkerMessage {
            message: Some(pb::worker_message::Message::Register(
                pb::RegisterWorkerRequest {
                    r#type: 0, // JT_ROOM
                    agent_name: name.to_string(),
                    version: "1.0.0".into(),
                    ..Default::default()
                },
            )),
        }
    }

    fn fixture() -> WorkerHttpState {
        let keys = Arc::new(KeyStore::new(vec![(
            "wt-test".into(),
            "wt-test-secret-0123456789abcdefghij".into(),
        )]));
        let tokens = Arc::new(TokenMinter::new(keys.clone()));
        let (events, mut rx) = mpsc::channel(64);
        tokio::spawn(async move { while rx.recv().await.is_some() {} });
        let (shutdown_tx, shutdown) = watch::channel(false);
        // Mirror production: the `AgentService` holds the shutdown sender for the
        // whole server lifetime, so the session's `shutdown.changed()` stays
        // Pending until a real teardown. Park it on a task here; dropping the
        // sender would resolve `changed()` immediately and tear the session down
        // before any reply is written.
        tokio::spawn(async move {
            let _shutdown_tx = shutdown_tx;
            std::future::pending::<()>().await;
        });
        WorkerHttpState {
            registry: Registry::new(Limits::default()),
            keys,
            tokens,
            server_info: pb::ServerInfo {
                edition: pb::server_info::Edition::Standard as i32,
                version: "1.13.10".into(),
                protocol: 15,
                agent_protocol: 1,
                ..Default::default()
            },
            signal_message_size_limit: 2 << 20,
            events,
            shutdown,
            _keepalive: None,
        }
    }

    /// Drive the session on one duplex half; returns the peer end for the test.
    fn spawn_session(state: WorkerHttpState) -> tokio::io::DuplexStream {
        let (peer, sess) = duplex(1 << 16);
        let (sr, sw) = split(sess);
        tokio::spawn(session(state, sr, sw, 1, "wt-test".into()));
        peer
    }

    #[tokio::test]
    async fn real_endpoint_opener_is_required_for_negotiation_and_disconnect_fences_it() {
        let state = fixture();
        let registry = state.registry.clone();
        let (node, mut peer) = duplex(1 << 16);
        let (read, write) = split(node);
        let opener: super::super::endpoint::StreamOpener = Arc::new(|| {
            Box::pin(async { Err(std::io::Error::other("scripted endpoint open failure")) })
        });
        let task = tokio::spawn(session_with_endpoints(
            state,
            read,
            write,
            1,
            "wt-test".into(),
            Some(opener),
        ));
        send_worker(
            &mut peer,
            pb::WorkerMessage {
                message: Some(pb::worker_message::Message::Register(
                    pb::RegisterWorkerRequest {
                        agent_name: "endpoint-agent".into(),
                        instance_id: "epoch".into(),
                        endpoint_protocol: 99,
                        endpoints: vec![pb::agent_http::AgentEndpoint {
                            path: "/echo".into(),
                            methods: vec!["POST".into()],
                            kind: 0,
                            public: false,
                        }],
                        ..Default::default()
                    },
                )),
            },
        )
        .await;
        let response = recv_server(&mut peer).await.unwrap();
        let id = match response.message.unwrap() {
            pb::server_message::Message::Register(reply) => {
                assert_eq!(reply.endpoint_settings.unwrap().protocol, 1);
                reply.worker_id
            }
            _ => panic!("expected register"),
        };
        // Force another control round trip to establish that register bookkeeping
        // completed after the response writer acknowledged its frame.
        send_worker(
            &mut peer,
            pb::WorkerMessage {
                message: Some(pb::worker_message::Message::Ping(pb::WorkerPing {
                    timestamp: 1,
                })),
            },
        )
        .await;
        assert!(matches!(
            recv_server(&mut peer).await.unwrap().message,
            Some(pb::server_message::Message::Pong(_))
        ));
        assert_eq!(
            registry
                .endpoints
                .spare_streams("wt-test", "endpoint-agent", ""),
            256
        );
        drop(peer);
        task.await.unwrap();
        assert!(registry.registration(&id).is_none());
        assert_eq!(
            registry
                .endpoints
                .spare_streams("wt-test", "endpoint-agent", ""),
            0
        );
    }

    #[tokio::test]
    async fn register_handshake_replies_worker_id_and_server_info() {
        let mut peer = spawn_session(fixture());
        send_worker(&mut peer, register_request("test-agent")).await;
        let response = recv_server(&mut peer).await.expect("register response");
        match response.message {
            Some(pb::server_message::Message::Register(reply)) => {
                assert!(reply.worker_id.starts_with("AW_"), "worker id prefix");
                let info = reply.server_info.expect("server info present");
                assert_eq!(info.version, "1.13.10");
                assert_eq!(info.agent_protocol, 1);
                assert!(reply.endpoint_settings.is_none(), "no endpoint negotiation");
            }
            other => panic!("expected a register response, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn ping_before_and_after_register_replies_pong() {
        let mut peer = spawn_session(fixture());
        // Ping is served during the handshake too (Go embeds WorkerPingHandler).
        send_worker(
            &mut peer,
            pb::WorkerMessage {
                message: Some(pb::worker_message::Message::Ping(pb::WorkerPing {
                    timestamp: 4444,
                })),
            },
        )
        .await;
        let pong = recv_server(&mut peer).await.expect("pre-register pong");
        match pong.message {
            Some(pb::server_message::Message::Pong(p)) => assert_eq!(p.last_timestamp, 4444),
            other => panic!("expected pong, got {other:?}"),
        }
        // Then a real registration followed by another ping/pong round trip.
        send_worker(&mut peer, register_request("test-agent")).await;
        recv_server(&mut peer).await.expect("register response");
        send_worker(
            &mut peer,
            pb::WorkerMessage {
                message: Some(pb::worker_message::Message::Ping(pb::WorkerPing {
                    timestamp: 5555,
                })),
            },
        )
        .await;
        let pong = recv_server(&mut peer).await.expect("post-register pong");
        match pong.message {
            Some(pb::server_message::Message::Pong(p)) => assert_eq!(p.last_timestamp, 5555),
            other => panic!("expected pong, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn non_register_signal_before_registration_ends_the_session() {
        let mut peer = spawn_session(fixture());
        // An UpdateJob before Register: dispatch fails and the connection ends.
        send_worker(
            &mut peer,
            pb::WorkerMessage {
                message: Some(pb::worker_message::Message::UpdateJob(
                    pb::UpdateJobStatus {
                        job_id: "AJ_missing".into(),
                        status: 1,
                        ..Default::default()
                    },
                )),
            },
        )
        .await;
        // No server reply is produced; the stream is torn down.
        assert!(
            recv_server(&mut peer).await.is_none(),
            "a failed handshake writes no response and closes"
        );
    }

    #[tokio::test]
    async fn oversized_frame_length_is_rejected_without_allocating() {
        let mut peer = spawn_session(fixture());
        // A header claiming more than MaxControlMessageSize must be refused
        // before the body is read (Go `message too large`).
        let mut header = Vec::new();
        header.extend_from_slice(&(MAX + 1).to_be_bytes());
        peer.write_all(&header).await.unwrap();
        peer.flush().await.unwrap();
        assert!(
            recv_server(&mut peer).await.is_none(),
            "an oversized length ends the session"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn missing_register_hits_the_handshake_deadline() {
        let mut peer = spawn_session(fixture());
        // Never register: advancing past RegisterTimeout must close the control
        // plane exactly like Go's SetReadDeadline on the registerer.
        tokio::time::advance(REGISTER_TIMEOUT + Duration::from_secs(1)).await;
        // The session task observes the deadline, returns and drops the stream.
        let mut sink = [0u8; 1];
        let read = peer.read(&mut sink).await.unwrap();
        assert_eq!(read, 0, "the deadline closes the stream with no bytes");
    }

    #[tokio::test]
    async fn concurrent_workers_share_one_registry() {
        // Production shares a single registry across every control stream; three
        // concurrent handshakes must each get their own worker id and register.
        let state = fixture();
        let registry = state.registry.clone();
        let mut peers = Vec::new();
        for _ in 0..3 {
            peers.push(spawn_session(state.clone()));
        }
        let mut drivers = Vec::new();
        for (i, mut peer) in peers.into_iter().enumerate() {
            let registry = registry.clone();
            drivers.push(tokio::spawn(async move {
                send_worker(&mut peer, register_request(&format!("agent-{i}"))).await;
                let response = recv_server(&mut peer).await.expect("register response");
                let id = match response.message {
                    Some(pb::server_message::Message::Register(reply)) => reply.worker_id,
                    _ => panic!("expected register response"),
                };
                // Round-trip a ping: the pong is only served from the dispatch
                // loop that begins after `registry.register`, so receiving it
                // proves this worker is already present in the shared registry.
                // Assert while `peer` is still open: dropping it here would let
                // the session EOF-disconnect and remove the worker from the map.
                send_worker(
                    &mut peer,
                    pb::WorkerMessage {
                        message: Some(pb::worker_message::Message::Ping(pb::WorkerPing {
                            timestamp: 777,
                        })),
                    },
                )
                .await;
                recv_server(&mut peer).await.expect("post-register pong");
                assert!(registry.registration(&id).is_some(), "{id} registered");
                (id, peer)
            }));
        }
        let mut ids = Vec::new();
        let mut keepalive = Vec::new();
        for driver in drivers {
            let (id, peer) = driver.await.unwrap();
            ids.push(id);
            keepalive.push(peer);
        }
        // All three registered concurrently with distinct worker ids over the one
        // shared registry (the three sessions never collide on identity or state).
        ids.sort();
        ids.dedup();
        assert_eq!(ids.len(), 3, "three distinct workers registered");
        // Every worker is visible through the service-level registry handle.
        for id in &ids {
            assert!(state.registry.registration(id).is_some(), "{id} registered");
        }
        drop(keepalive);
    }
}
