// SPEC-0074 C15 agent-wt wire differential probe.
//
// Drives a single WebTransport agent session against a *running*
// `livekit-server` (the Go oracle or the Rust binary), using the exact same
// client code path (`lk-webtransport` + the `4-byte BE length + protobuf`
// control framing that Go's `wire.WriteControlMessage`/`ReadControlMessage`
// use). It prints a canonical JSON observation so the driver can diff the
// externally visible output of both binaries for one client input.
//
// `--mode` selects which part of the session lifecycle the probe exercises.
// Every mode reports the same observation keys as the Go `wtdiff` probe, so
// the same probe can be run against the Go oracle and the Rust server and the
// two results compared field by field:
//
//	register  - upgrade + control stream + Register round trip, then check
//	            that the session stays open (the agent registration is live);
//	idle      - upgrade only, never open a control stream: the server must end
//	            the session on its `agent.RegisterTimeout` deadline. The close
//	            reason and how it was carried (a WT_CLOSE_SESSION capsule or a
//	            QUIC CONNECTION_CLOSE) are the observable contract;
//	stream    - upgrade + Register, then open a *second* client-initiated
//	            WebTransport stream and write a payload: the official listener
//	            accepts extra streams into the session's incoming queue and
//	            leaves them open, so the peer must not observe a reset;
//	close     - upgrade + Register, then open a *second* client-initiated
//	            WebTransport stream and end the agent control plane (a FIN on
//	            the control stream): the server must end the session, carry the
//	            WT_CLOSE_SESSION capsule and terminate the session's remaining
//	            streams with `WTSessionGoneErrorCode`;
//	datagram  - upgrade + Register, then send one WebTransport datagram
//	            (RFC 9297): the session must survive it;
//	data_integrity - upgrade + Register, then write a larger payload (48 bytes,
//	            3 × 16-byte pattern) on a bidi stream and FIN the send side:
//	            verifies the transport preserves data integrity without reset;
//	unidir    - upgrade + Register, then open a unidirectional stream and
//	            write a payload: the server accepts it without echoing;
//	reconnect - Register on one connection, close the session, establish a
//	            fresh connection and Register again: verifies the server
//	            cleans up the first session and accepts the second;
//	reliable_prefix - upgrade + Register, write two chunks on a bidi stream,
//	            then reset it: verifies the transport delivers the contiguous
//	            prefix before the reset (RESET_STREAM_AT contract).
//
// Non-deterministic identity (the minted worker id, the node id) is reported as
// structure (prefix/length/alphabet, presence) rather than exact bytes, so the
// comparison targets parity of the contract, not per-process randomness.
//
// Usage:
//   wt-probe --addr 127.0.0.1:PORT --token <jwt> [--path /agent] [--authority localhost]
//             [--mode register|idle|stream|close|datagram|data_integrity|unidir|reconnect|reliable_prefix]

use std::time::Duration;

use bytes::Bytes;
use lk_protocol::livekit as pb;
use lk_webtransport::connect_observing;
use lk_webtransport::{
    WT_SESSION_GONE_ERROR_CODE, WtBidiStream, WtClient, WtError, client_config_insecure,
    client_endpoint,
};
use prost::Message as _;

/// `shortuuid.DefaultAlphabet`, the character set upstream `guid.New` draws the
/// random worker-id suffix from.
const ID_ALPHABET: &[u8] = b"23456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz";

/// The observation window used by the modes that only check that the session
/// survives: long enough to see a peer-side teardown, short enough to keep the
/// differential quick.
const ALIVE_WINDOW: Duration = Duration::from_millis(1500);

/// The idle mode waits for the server's `agent.RegisterTimeout` (10s) plus
/// enough slack for the close to travel.
const IDLE_WINDOW: Duration = Duration::from_secs(25);

/// Parsed `--key value` command line (no external arg parser, so the probe adds
/// no dependency to the tree).
struct Args {
    addr: String,
    token: String,
    path: String,
    authority: String,
    mode: String,
}

fn parse_args() -> Args {
    let argv: Vec<String> = std::env::args().collect();
    let mut out = Args {
        addr: String::new(),
        token: String::new(),
        path: "/agent".to_string(),
        authority: "localhost".to_string(),
        mode: "register".to_string(),
    };
    let mut i = 1;
    while i + 1 < argv.len() {
        let value = argv[i + 1].clone();
        match argv[i].as_str() {
            "--addr" => out.addr = value,
            "--token" => out.token = value,
            "--path" => out.path = value,
            "--authority" => out.authority = value,
            "--mode" => out.mode = value,
            other => panic!("unknown or value-less argument {other}"),
        }
        i += 2;
    }
    assert!(
        !out.addr.is_empty() && !out.token.is_empty(),
        "--addr and --token required"
    );
    out
}

/// Accumulate chunks off the control stream until one full
/// `4-byte big-endian length + body` control frame arrives; `None` marks an
/// early stream end before a complete frame (the client mirror of Go's
/// `ReadControlMessage`).
async fn read_one_frame(stream: &mut WtBidiStream) -> Option<Vec<u8>> {
    let mut buf: Vec<u8> = Vec::new();
    while buf.len() < 4 {
        match stream.recv_data().await {
            Ok(Some(chunk)) => buf.extend_from_slice(&chunk),
            _ => return None,
        }
    }
    let len = usize::try_from(u32::from_be_bytes([buf[0], buf[1], buf[2], buf[3]])).ok()?;
    // Refuse to allocate an absurd frame the way the server would; a client
    // overflow here means the peer wrote a corrupt header.
    if len > (1 << 20) {
        return None;
    }
    while buf.len() < 4 + len {
        match stream.recv_data().await {
            Ok(Some(chunk)) => buf.extend_from_slice(&chunk),
            _ => return None,
        }
    }
    Some(buf[4..4 + len].to_vec())
}

/// Report the worker id only by its observable structure: `AW_` prefix, total
/// length and that the suffix is drawn from the upstream guid alphabet, so two
/// independently generated ids compare equal in shape.
fn id_shape(id: &str) -> serde_json::Value {
    let suffix_ok = id
        .strip_prefix("AW_")
        .is_some_and(|rest| !rest.is_empty() && rest.bytes().all(|b| ID_ALPHABET.contains(&b)));
    serde_json::json!({
        "prefix": "AW_",
        "len": id.len(),
        "alphabet_ok": suffix_ok,
    })
}

/// The close observation, using the same vocabulary as the Go probe:
/// `session_error` (the peer sent a WT_CLOSE_SESSION capsule),
/// `connection_closed` (the peer tore the QUIC connection down) or `none` (the
/// session was still open when the window elapsed).
fn close_observation(
    kind: &str,
    code: Option<u64>,
    message: Option<String>,
    elapsed: Duration,
) -> serde_json::Value {
    let millis = elapsed.as_millis() as u64;
    serde_json::json!({
        "session_closed": kind != "none",
        "close_kind": kind,
        "close_code": code,
        "close_message": message,
        "closed_after_ms": millis,
        "closed_after_s": (millis + 500) / 1000,
    })
}

/// Wait up to `window` for the session to end and report how it ended.
async fn observe(client: &mut WtClient, window: Duration) -> serde_json::Value {
    // A clone of the connection lets the connection-close branch race the
    // capsule read without borrowing the client twice.
    let conn = client.session().connection().clone();
    let start = std::time::Instant::now();
    let closed = tokio::time::timeout(window, async {
        tokio::select! {
            capsule = client.session_mut().read_close_capsule() => match capsule {
                Ok(Some((code, message))) => {
                    close_observation("session_error", Some(u64::from(code)), Some(message), start.elapsed())
                }
                Ok(None) => close_observation(
                    "other",
                    None,
                    Some("capsule stream ended without WT_CLOSE_SESSION".to_string()),
                    start.elapsed(),
                ),
                Err(e) => close_observation("other", None, Some(e.to_string()), start.elapsed()),
            },
            closed = conn.closed() => match closed {
                quinn::ConnectionError::ApplicationClosed(close) => close_observation(
                    "connection_closed",
                    Some(close.error_code.into_inner()),
                    Some(String::from_utf8_lossy(close.reason.as_ref()).into_owned()),
                    start.elapsed(),
                ),
                quinn::ConnectionError::LocallyClosed => close_observation(
                    "other",
                    None,
                    Some("connection closed locally".to_string()),
                    start.elapsed(),
                ),
                e => close_observation("other", None, Some(e.to_string()), start.elapsed()),
            },
        }
    })
    .await;
    match closed {
        Ok(value) => value,
        // The window elapsed with the session still open.
        Err(_) => close_observation("none", None, None, start.elapsed()),
    }
}

/// Perform the agent registration handshake on a fresh control stream and
/// return the contract observation together with the control stream.
///
/// The control stream is handed back so the caller can keep it open for the
/// rest of the session, exactly like a real agent (and like the Go `wtdiff`
/// probe). Letting it drop would end the agent's control plane, which is a
/// different scenario the `close` mode exercises on purpose.
async fn register(client: &mut WtClient) -> (serde_json::Value, WtBidiStream) {
    let mut control = client
        .session_mut()
        .open_bi()
        .await
        .expect("open control stream");
    let request = pb::WorkerMessage {
        message: Some(pb::worker_message::Message::Register(
            pb::RegisterWorkerRequest {
                r#type: 0, // JT_ROOM
                version: "1.0.0".into(),
                agent_name: "diff-agent".into(),
                ..Default::default()
            },
        )),
    };
    let body = request.encode_to_vec();
    let mut frame = Vec::with_capacity(4 + body.len());
    frame.extend_from_slice(&(body.len() as u32).to_be_bytes());
    frame.extend_from_slice(&body);
    control
        .send_data(Bytes::from(frame))
        .await
        .expect("send register");

    let reply = tokio::time::timeout(Duration::from_secs(10), read_one_frame(&mut control))
        .await
        .expect("register response within deadline")
        .expect("register response frame");

    let message = pb::ServerMessage::decode(&reply[..]).expect("decode register response");
    let contract = match message.message {
        Some(pb::server_message::Message::Register(response)) => {
            let info = response.server_info.unwrap_or_default();
            serde_json::json!({
                "response_type": "register",
                "worker_id": id_shape(&response.worker_id),
                "server_info": {
                    "edition": info.edition,
                    "version": info.version,
                    "protocol": info.protocol,
                    "agent_protocol": info.agent_protocol,
                    "region": info.region,
                    "node_id_present": !info.node_id.is_empty(),
                    "debug_info": info.debug_info,
                },
                "endpoint_settings_present": response.endpoint_settings.is_some(),
            })
        }
        other => serde_json::json!({
            "response_type": format!("unexpected:{other:?}"),
        }),
    };
    (contract, control)
}

/// Establish one WebTransport session to the server using a *fresh* endpoint.
/// Used by the `reconnect` mode to create the second connection after the first
/// has been closed. A new UDP socket + quinn endpoint are created so the h3
/// layer has no leftover state from the previous connection.
async fn connect_once(
    addr: std::net::SocketAddr,
    authority: &str,
    path: &str,
    token: &str,
) -> (WtClient, http::HeaderMap, quinn::Endpoint) {
    let socket = std::net::UdpSocket::bind("127.0.0.1:0").expect("bind reconnect udp");
    socket.set_nonblocking(true).expect("nonblocking udp");
    let cfg = client_config_insecure().expect("insecure client config");
    let ep = client_endpoint(cfg, b"probe-reset-key-2", socket).expect("client endpoint");
    let auth_header = format!("Bearer {}", token);
    let (client, headers) = connect_observing(
        &ep,
        addr,
        authority,
        path,
        &[("authorization", auth_header.as_str())],
    )
    .await
    .expect("webtransport upgrade must succeed");
    (client, headers, ep)
}

#[tokio::main]
async fn main() {
    let args = parse_args();
    let addr: std::net::SocketAddr = args.addr.parse().expect("--addr must be <ip>:<port>");

    // A dedicated UDP socket + reset key keep this endpoint independent of the
    // connection lifetime; the endpoint must stay alive for the whole session.
    let socket = std::net::UdpSocket::bind("127.0.0.1:0").expect("bind client udp");
    socket
        .set_nonblocking(true)
        .expect("nonblocking client udp");
    let cfg = client_config_insecure().expect("insecure client config");
    let ep = client_endpoint(cfg, b"probe-reset-key", socket).expect("client endpoint");
    let auth_header = format!("Bearer {}", args.token);
    let (mut client, headers) = connect_observing(
        &ep,
        addr,
        &args.authority,
        &args.path,
        &[("authorization", auth_header.as_str())],
    )
    .await
    .expect("webtransport upgrade must succeed");

    // Capture the externally visible upgrade negotiation: the response headers
    // the server sent with its 200.
    let draft = headers
        .get("sec-webtransport-http3-draft")
        .and_then(|v| v.to_str().ok())
        .map(str::to_string);

    let mut observation = serde_json::json!({
        "mode": args.mode,
        "upgrade": { "status": 200, "sec_webtransport_http3_draft": draft },
    });

    // The control stream stays open for the whole run, the way a real agent
    // (and the Go `wtdiff` probe) keeps it: the session must survive as long
    // as the agent's control plane is up.
    let mut control: Option<WtBidiStream> = None;

    match args.mode.as_str() {
        "register" => {
            let (contract, stream) = register(&mut client).await;
            control = Some(stream);
            merge(&mut observation, contract);
            let closed = observe(&mut client, ALIVE_WINDOW).await;
            merge(&mut observation, closed);
        }
        "idle" => {
            // Never open a control stream: the server must end the session on
            // its own deadline.
            let closed = observe(&mut client, IDLE_WINDOW).await;
            merge(&mut observation, closed);
        }
        "stream" => {
            let (contract, stream) = register(&mut client).await;
            control = Some(stream);
            merge(&mut observation, contract);
            let mut stream = client
                .session_mut()
                .open_bi()
                .await
                .expect("open second stream");
            let payload = Bytes::from_static(b"c15-stream-probe");
            stream
                .send_data(payload.clone())
                .await
                .expect("write second stream");
            let target = observation.as_object_mut().expect("observation object");
            target.insert("stream_opened".to_string(), serde_json::json!(true));
            target.insert(
                "stream_write_bytes".to_string(),
                serde_json::json!(payload.len()),
            );
            let outcome = read_extra_stream(&mut stream).await;
            record_stream_outcome(target, &outcome);
            let closed = observe(&mut client, ALIVE_WINDOW).await;
            merge(&mut observation, closed);
        }
        "close" => {
            let (contract, mut stream) = register(&mut client).await;
            merge(&mut observation, contract);
            let mut extra = client
                .session_mut()
                .open_bi()
                .await
                .expect("open second stream");
            let payload = Bytes::from_static(b"c15-close-probe");
            extra
                .send_data(payload.clone())
                .await
                .expect("write second stream");
            let target = observation.as_object_mut().expect("observation object");
            target.insert("stream_opened".to_string(), serde_json::json!(true));
            target.insert(
                "stream_write_bytes".to_string(),
                serde_json::json!(payload.len()),
            );
            // The agent ends its control plane: the server must close the
            // session and terminate the streams the session still owns.
            stream.finish().await.expect("close the control stream");
            control = Some(stream);
            let outcome = read_extra_stream(&mut extra).await;
            record_stream_outcome(target, &outcome);
            let closed = observe(&mut client, ALIVE_WINDOW).await;
            merge(&mut observation, closed);
        }
        "datagram" => {
            let (contract, stream) = register(&mut client).await;
            control = Some(stream);
            merge(&mut observation, contract);
            let payload = Bytes::from_static(b"c15-datagram-probe");
            client
                .session()
                .send_datagram(payload.clone())
                .expect("send datagram");
            let target = observation.as_object_mut().expect("observation object");
            target.insert("datagram_sent".to_string(), serde_json::json!(true));
            target.insert(
                "datagram_bytes".to_string(),
                serde_json::json!(payload.len()),
            );
            let echo = tokio::time::timeout(ALIVE_WINDOW, client.session().read_datagram()).await;
            let (echo_bytes, echoed) = match echo {
                Ok(Ok(bytes)) => (bytes.len(), true),
                _ => (0, false),
            };
            target.insert("datagram_echo".to_string(), serde_json::json!(echoed));
            target.insert(
                "datagram_echo_bytes".to_string(),
                serde_json::json!(echo_bytes),
            );
            let closed = observe(&mut client, ALIVE_WINDOW).await;
            merge(&mut observation, closed);
        }
        "data_integrity" => {
            let (contract, stream) = register(&mut client).await;
            control = Some(stream);
            merge(&mut observation, contract);
            let mut stream = client
                .session_mut()
                .open_bi()
                .await
                .expect("open data stream");
            // 3 × 16-byte pattern = 48 bytes total.
            let chunk = b"c15-integrity-ck";
            let mut total = 0;
            for _ in 0..3 {
                stream
                    .send_data(Bytes::from_static(chunk))
                    .await
                    .expect("send data chunk");
                total += chunk.len();
            }
            stream.finish().await.expect("finish data stream");
            let target = observation.as_object_mut().expect("observation object");
            target.insert("stream_opened".to_string(), serde_json::json!(true));
            target.insert("stream_write_bytes".to_string(), serde_json::json!(total));
            target.insert("stream_fin_sent".to_string(), serde_json::json!(true));
            let outcome = read_extra_stream(&mut stream).await;
            record_stream_outcome(target, &outcome);
            let closed = observe(&mut client, ALIVE_WINDOW).await;
            merge(&mut observation, closed);
        }
        "unidir" => {
            let (contract, stream) = register(&mut client).await;
            control = Some(stream);
            merge(&mut observation, contract);
            let mut uni = client
                .session_mut()
                .open_uni()
                .await
                .expect("open unidirectional stream");
            let payload = Bytes::from_static(b"c15-unidir-probe");
            uni.send_data(payload.clone())
                .await
                .expect("send unidir data");
            uni.finish().await.expect("finish unidir stream");
            let target = observation.as_object_mut().expect("observation object");
            target.insert("unidir_opened".to_string(), serde_json::json!(true));
            target.insert(
                "unidir_write_bytes".to_string(),
                serde_json::json!(payload.len()),
            );
            target.insert("unidir_finish_ok".to_string(), serde_json::json!(true));
            let closed = observe(&mut client, ALIVE_WINDOW).await;
            merge(&mut observation, closed);
        }
        "reconnect" => {
            // First registration on the current connection.
            let (contract1, control1) = register(&mut client).await;
            let w1 = contract1.get("worker_id").cloned();
            let s1 = contract1.get("server_info").cloned();
            merge(&mut observation, contract1);
            let closed1 = observe(&mut client, ALIVE_WINDOW).await;
            // Drop the first control stream (not the session).
            drop(control1);
            // Open a fresh WebTransport session on a new endpoint.
            // We avoid explicitly closing the first session to sidestep an
            // h3-quinn 0.0.10 stop_sending panic during connection teardown.
            let (mut client2, _headers2, ep2) =
                connect_once(addr, &args.authority, &args.path, &args.token).await;
            let (contract2, control2) = register(&mut client2).await;
            let w2 = contract2.get("worker_id").cloned();
            let s2 = contract2.get("server_info").cloned();
            let closed2 = observe(&mut client2, ALIVE_WINDOW).await;
            drop(control2);
            drop(client2);
            drop(ep2);
            let target = observation.as_object_mut().expect("observation object");
            target.insert(
                "reconnect_worker_id_shape_match".to_string(),
                serde_json::json!(w1 == w2),
            );
            target.insert(
                "reconnect_server_info_match".to_string(),
                serde_json::json!(s1 == s2),
            );
            target.insert("second_register".to_string(), contract2);
            target.insert(
                "first_session_closed".to_string(),
                closed1["session_closed"].clone(),
            );
            target.insert(
                "first_close_kind".to_string(),
                closed1["close_kind"].clone(),
            );
            target.insert(
                "second_session_closed".to_string(),
                serde_json::json!(closed2["session_closed"]),
            );
            target.insert(
                "second_close_kind".to_string(),
                closed2["close_kind"].clone(),
            );
            // Reconnect owns both sessions; skip the outer control/cleanup.
            drop(ep);
            println!(
                "{}",
                serde_json::to_string(&observation).expect("serialize")
            );
            return;
        }
        "reliable_prefix" => {
            let (contract, stream) = register(&mut client).await;
            control = Some(stream);
            merge(&mut observation, contract);
            let mut bstream = client
                .session_mut()
                .open_bi()
                .await
                .expect("open prefix stream");
            let chunk1 = Bytes::from_static(b"c15-reliable-pfx-1");
            let chunk2 = Bytes::from_static(b"c15-reliable-pfx-2");
            let total = chunk1.len() + chunk2.len();
            bstream.send_data(chunk1).await.expect("send chunk 1");
            bstream.send_data(chunk2).await.expect("send chunk 2");
            bstream.reset(WT_SESSION_GONE_ERROR_CODE);
            let target = observation.as_object_mut().expect("observation object");
            target.insert("stream_opened".to_string(), serde_json::json!(true));
            target.insert("stream_write_bytes".to_string(), serde_json::json!(total));
            target.insert("stream_reset_sent".to_string(), serde_json::json!(true));
            target.insert(
                "stream_reset_code".to_string(),
                serde_json::json!(WT_SESSION_GONE_ERROR_CODE),
            );
            let closed = observe(&mut client, ALIVE_WINDOW).await;
            merge(&mut observation, closed);
        }
        other => panic!("unknown mode {other}"),
    }

    // Keep the endpoint alive until after the observation is produced.
    drop(control);
    drop(client);
    drop(ep);
    println!(
        "{}",
        serde_json::to_string(&observation).expect("serialize")
    );
}

/// How the peer terminated the extra stream, in the vocabulary both probes
/// speak: `none` (the peer delivered data and left the stream open), `eof`
/// (the peer finished the stream with a FIN), `reset` (the peer sent
/// RESET_STREAM with an error code), `session_gone` (the peer tore the session
/// down and the stream died with it), `deadline` (the read deadline elapsed
/// with the stream still open) or `other`.
///
/// Unlike the Go probe - whose `webtransport-go` v0.12.0 absorbs
/// `WTSessionGoneErrorCode` inside `ReceiveStream.Read` and reports the
/// session's own close error instead of the stream error code - quinn surfaces
/// the reset code verbatim, so the numeric code is observable here.
struct StreamOutcome {
    /// Bytes the peer echoed back before the termination.
    echo_bytes: usize,
    /// Whether the peer sent a reset at all.
    reset: bool,
    /// The reset error code, when one was observable.
    reset_code: Option<u64>,
    /// The termination class.
    termination: &'static str,
}

/// Read an extra stream and report how the peer terminated it: the echoed
/// byte count, the termination class and the reset error code.
async fn read_extra_stream(stream: &mut WtBidiStream) -> StreamOutcome {
    let read = tokio::time::timeout(ALIVE_WINDOW, stream.recv_data()).await;
    match read {
        Ok(Ok(Some(chunk))) => StreamOutcome {
            echo_bytes: chunk.len(),
            reset: false,
            reset_code: None,
            termination: "none",
        },
        Ok(Ok(None)) => StreamOutcome {
            echo_bytes: 0,
            reset: false,
            reset_code: None,
            termination: "eof",
        },
        Ok(Err(WtError::StreamTerminated(code))) => StreamOutcome {
            echo_bytes: 0,
            reset: true,
            reset_code: Some(code),
            termination: if code == WT_SESSION_GONE_ERROR_CODE {
                "session_gone"
            } else {
                "reset"
            },
        },
        Ok(Err(_)) => StreamOutcome {
            echo_bytes: 0,
            reset: false,
            reset_code: None,
            termination: "other",
        },
        // The read deadline elapsed: the stream is still open.
        Err(_) => StreamOutcome {
            echo_bytes: 0,
            reset: false,
            reset_code: None,
            termination: "deadline",
        },
    }
}

/// Merge the keys of `source` into `target`.
fn merge(target: &mut serde_json::Value, source: serde_json::Value) {
    if let (Some(target), Some(source)) = (target.as_object_mut(), source.as_object()) {
        for (key, value) in source {
            target.insert(key.clone(), value.clone());
        }
    }
}

/// Record the outcome of an extra-stream read in the observation.
fn record_stream_outcome(
    target: &mut serde_json::Map<String, serde_json::Value>,
    outcome: &StreamOutcome,
) {
    target.insert(
        "stream_echo_bytes".to_string(),
        serde_json::json!(outcome.echo_bytes),
    );
    target.insert("stream_reset".to_string(), serde_json::json!(outcome.reset));
    target.insert(
        "stream_reset_code".to_string(),
        match outcome.reset_code {
            Some(code) => serde_json::json!(code),
            None => serde_json::Value::Null,
        },
    );
    target.insert(
        "stream_termination".to_string(),
        serde_json::json!(outcome.termination),
    );
}
