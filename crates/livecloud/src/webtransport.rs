//! SPEC-0074 C15: the production WebTransport listener, wired into the
//! `livekit-server` binary exactly like upstream `pkg/service/webtransport.go`
//! on `quic-go/webtransport-go`.
//!
//! Contract reproduced here (audited against the pinned v1.13.10 reference):
//! - `WebTransportTLS(certFile, keyFile, dev)`: both files -> load the pair;
//!   neither and not `dev` -> fatal
//!   `webtransport requires webtransport.tls_cert_file/tls_key_file`;
//!   neither and `dev` -> an ephemeral ECDSA P-256 self-signed cert
//!   (serial 1, `CN=livekit-webtransport-dev`, `-1h`/`+365d`, SANs
//!   `localhost`/`127.0.0.1`/`::1`) plus a WARN that clients must skip
//!   verification.
//! - One UDP socket per bind address (empty addrs -> all interfaces), datagrams
//!   enabled, the `h3` ALPN; the listener logs `webtransport listener started`
//!   with `addresses`, `webtransport listener stopped` on accept-loop exit and
//!   `webtransport connection stopped` on a per-connection serve error.
//! - Handler chain (`NewWebTransportHandler`): path normalization -> negroni
//!   recovery -> API-key auth -> mux. The only route is `/agent`
//!   (`AgentWTService`); anything else is the mux `404 page not found`.
//! - `/agent` semantics (`agentwt.go`): a missing/foreign grant is
//!   `401 no permissions to access the room`; a request that reaches the route
//!   with an agent grant but is not a WebTransport CONNECT logs
//!   `agent webtransport upgrade failed` and answers implicitly (`200`); a
//!   successful upgrade then waits for the worker control stream bounded by
//!   `agent.RegisterTimeout`, closing the session with reason `no control
//!   stream` when none arrives.
//!
//! The worker data plane (the length-delimited `WorkerMessage`/`ServerMessage`
//! exchange on the control stream) is served by
//! [`lk_compat::agent::wt_control`], which shares the WebSocket `/agent`
//! registry, registration validation and job-effect consumer.

use std::io;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr, UdpSocket};
use std::pin::Pin;
use std::str::FromStr;
use std::task::{Context, Poll};
use std::time::{Duration, SystemTime};

use bytes::Bytes;
use http::{Response, StatusCode, header};
use lk_compat::agent::websocket::WorkerHttpState;
use lk_compat::agent::wt_control;
use lk_compat::auth::{authenticate, extract_token, query_param};
use lk_webtransport::{
    Accepted, ControlReader, ControlWriter, ServerTls, SessionId, WtBidiStream, WtServerConnection,
    WtSession, endpoint, is_webtransport_connect, reject_with_body, server_config, upgrade_connect,
};
use p256::ecdsa::{DerSignature, SigningKey};
use p256::pkcs8::EncodePrivateKey;
use tokio::sync::{mpsc, watch};
use tokio::task::JoinHandle;
use x509_cert::builder::profile::BuilderProfile;
use x509_cert::builder::{Builder, CertificateBuilder};
use x509_cert::certificate::TbsCertificate;
use x509_cert::der::Encode;
use x509_cert::ext::pkix::name::{GeneralName, GeneralNames};
use x509_cert::ext::pkix::{BasicConstraints, KeyUsage, KeyUsages, SubjectAltName};
use x509_cert::ext::{Extension, ToExtension};
use x509_cert::name::Name;
use x509_cert::serial_number::SerialNumber;
use x509_cert::spki::{SubjectPublicKeyInfo, SubjectPublicKeyInfoRef};
use x509_cert::time::Time;
use x509_cert::time::Validity;

use crate::zap::ZapValue;
use rtc_tls::rustls::pki_types::{CertificateDer, PrivateKeyDer, pem::PemObject};

/// Source caller strings match the upstream Go log sites.
const CALLER_STARTED: &str = "pkg/service/webtransport.go:207";
const CALLER_STOPPED: &str = "pkg/service/webtransport.go:242";
const CALLER_CONN_STOPPED: &str = "pkg/service/webtransport.go:247";
const CALLER_DEV_CERT: &str = "pkg/service/webtransport.go:71";
const CALLER_UPGRADE_FAILED: &str = "pkg/service/agentwt.go:82";

/// `rtc.ErrPermissionDenied` (pkg/rtc/errors.go:24): the `/agent` body when the
/// grant is absent or lacks `Video.Agent`.
const PERMISSION_DENIED: &[u8] = b"no permissions to access the room";
/// ServeMux's default not-found body for a path with no registered route.
const NOT_FOUND: &[u8] = b"404 page not found\n";
/// Go `agent.RegisterTimeout` — the bounded wait for the control stream.
pub(crate) const REGISTER_TIMEOUT: Duration = Duration::from_secs(10);

/// A fatal WebTransport setup error; the message is surfaced verbatim as a
/// startup error (like upstream `must.Get`/`return err` before the readiness
/// line).
#[derive(Debug, thiserror::Error)]
#[error("{0}")]
pub struct SetupError(String);

impl SetupError {
    fn text(msg: impl Into<String>) -> Self {
        Self(msg.into())
    }
}

/// Build the listener TLS material (`WebTransportTLS`). The dev self-signed
/// path emits the Go WARN. The pair/leaf/key validation that Go performs in
/// `tls.LoadX509KeyPair` happens here for PEM parsing and, for key/leaf
/// compatibility, when [`bind`] builds the QUIC crypto config.
///
/// # Errors
/// [`SetupError`] reproducing the upstream messages: the missing-file error in
/// non-dev mode, or a certificate/key read/parse failure.
pub fn build_server_tls(
    cert_file: &str,
    key_file: &str,
    dev: bool,
) -> Result<ServerTls, SetupError> {
    if !cert_file.is_empty() && !key_file.is_empty() {
        return load_cert_files(cert_file, key_file);
    }
    if !dev {
        return Err(SetupError::text(
            "webtransport requires webtransport.tls_cert_file/tls_key_file",
        ));
    }
    let (cert_chain, key) = generate_dev_cert()?;
    crate::logging::notify(
        crate::zap::WARN,
        "livekit",
        CALLER_DEV_CERT,
        "webtransport using a generated self-signed cert (dev mode); clients must skip verification",
        &[],
        None,
    );
    Ok(ServerTls::new(cert_chain, key))
}

/// `tls.LoadX509KeyPair` for the file branch: read + PEM-decode the leaf-first
/// chain and a supported private key.
fn load_cert_files(cert_file: &str, key_file: &str) -> Result<ServerTls, SetupError> {
    let cert_pem = std::fs::read(cert_file)
        .map_err(|e| SetupError::text(format!("failed to load certificate: {e}")))?;
    let key_pem = std::fs::read(key_file)
        .map_err(|e| SetupError::text(format!("failed to load private key: {e}")))?;
    let chain = CertificateDer::pem_slice_iter(&cert_pem)
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| SetupError::text(format!("failed to parse certificate chain: {e}")))?;
    if chain.is_empty() {
        return Err(SetupError::text(
            "failed to parse private key: no certificates",
        ));
    }
    let key = PrivateKeyDer::from_pem_slice(&key_pem)
        .map_err(|e| SetupError::text(format!("failed to parse private key: {e}")))?;
    Ok(ServerTls::new(chain, key))
}

/// Bind one UDP socket per address and construct the QUIC endpoint, mirroring
/// the socket/`quic.ListenEarly` half of `WebTransportServer.Listen`. Bind is
/// transactional: a failure closes the sockets bound so far (Go `closeAll`).
///
/// Builds the server crypto config here so a certificate/key mismatch is fatal
/// before the readiness line, like Go surfacing `LoadX509KeyPair`/TLS config
/// errors during `InitializeServer`.
///
/// # Errors
/// [`SetupError`] wrapping the Go-style `net.ListenUDP`/bind failure text.
pub async fn bind(
    tls: &ServerTls,
    hosts: &[String],
    port: u16,
) -> Result<WebTransportListener, SetupError> {
    let token_master = random_bytes(32);
    let reset_key = random_bytes(32);
    let config = server_config(tls, &token_master)
        .map_err(|e| SetupError::text(format!("webtransport tls config: {e}")))?;

    // Empty addrs bind all interfaces (Go: `addrs = []string{""}`).
    let wildcard = [String::new()];
    let hosts = if hosts.is_empty() { &wildcard } else { hosts };

    let mut endpoints = Vec::with_capacity(hosts.len());
    for host in hosts {
        let address = crate::bind_host::resolve(host, port)
            .await
            .map_err(SetupError::text)?;
        let socket = bind_udp_socket(address, port)?;
        let bound = socket
            .local_addr()
            .map_err(|e| SetupError::text(format!("webtransport local addr: {e}")))?;
        socket
            .set_nonblocking(true)
            .map_err(|e| SetupError::text(format!("webtransport nonblocking: {e}")))?;
        let ep = endpoint(config.clone(), &reset_key, socket)
            .map_err(|e| SetupError::text(format!("listen udp {bound}: webtransport bind: {e}")))?;
        endpoints.push((ep, bound));
    }
    // Go `Listen` logs `webtransport listener started` with the bound
    // addresses before the readiness line (webtransport.go:207).
    let addresses: Vec<ZapValue> = endpoints
        .iter()
        .map(|(_, addr)| ZapValue::Str(addr.to_string()))
        .collect();
    crate::logging::notify(
        crate::zap::INFO,
        "livekit",
        CALLER_STARTED,
        "webtransport listener started",
        &[("addresses", ZapValue::Array(addresses))],
        None,
    );
    Ok(WebTransportListener { endpoints })
}

/// `net.ListenUDP("udp", JoinHostPort(host, port))`, dual-stack when the host
/// is unset (mirrors the TCP `listen` helper's `0.0.0.0` fallback).
fn bind_udp_socket(host: Option<SocketAddr>, port: u16) -> Result<UdpSocket, SetupError> {
    let fail = |e: std::io::Error, text: String| {
        SetupError::text(format!(
            "listen udp {text}: bind: {}",
            lk_config::go_errno_text(&e)
        ))
    };
    match host {
        Some(addr) => UdpSocket::bind(addr).map_err(|e| fail(e, addr.to_string())),
        None => {
            let v6 = SocketAddr::new(IpAddr::V6(Ipv6Addr::UNSPECIFIED), port);
            match UdpSocket::bind(v6) {
                Ok(s) => Ok(s),
                Err(e) if e.kind() == std::io::ErrorKind::AddrInUse => {
                    Err(fail(e, format!(":{port}")))
                }
                Err(_) => UdpSocket::bind(SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), port))
                    .map_err(|e| fail(e, format!(":{port}"))),
            }
        }
    }
}

/// A bound WebTransport listener: one QUIC endpoint per address plus its
/// bound socket address. Endpoints are consumed by [`WebTransportListener::spawn`].
pub struct WebTransportListener {
    endpoints: Vec<(quinn::Endpoint, SocketAddr)>,
}

impl WebTransportListener {
    /// The bound local addresses (`addresses` on the started log line). Read
    /// back by the acceptance tests to learn an ephemeral bind port.
    #[must_use]
    #[allow(dead_code)] // consumed by the `#[cfg(test)]` acceptance harness
    pub fn bound_addrs(&self) -> Vec<SocketAddr> {
        self.endpoints.iter().map(|(_, addr)| *addr).collect()
    }

    /// Move each endpoint into its own accept pump task. The Go
    /// `webtransport listener started` line is already emitted by [`bind`]; this
    /// only starts the pumps. Callers push the returned handles into the
    /// shutdown-drained `listeners` set.
    pub fn spawn(
        self,
        worker: WorkerHttpState,
        control_timeout: Duration,
        shutdown: watch::Receiver<bool>,
    ) -> Vec<JoinHandle<()>> {
        self.endpoints
            .into_iter()
            .map(|(ep, _)| {
                let worker = worker.clone();
                let shutdown = shutdown.clone();
                tokio::spawn(accept_pump(ep, worker, control_timeout, shutdown))
            })
            .collect()
    }
}

/// Go `WebTransportServer.accept`: accept QUIC connections and serve each on
/// its own task until the endpoint closes or shutdown is signaled. On shutdown
/// the endpoints close ( CONNECTION_CLOSE ) last, after in-flight connections
/// have been given a chance to observe the GOAWAY-equivalent teardown.
async fn accept_pump(
    ep: quinn::Endpoint,
    worker: WorkerHttpState,
    control_timeout: Duration,
    mut shutdown: watch::Receiver<bool>,
) {
    loop {
        let incoming = tokio::select! {
            biased;
            changed = shutdown.changed() => {
                if changed.is_err() || *shutdown.borrow_and_update() {
                    break;
                }
                continue;
            }
            incoming = ep.accept() => incoming,
        };
        let Some(incoming) = incoming else { break };
        match incoming.await {
            Ok(conn) => {
                let worker = worker.clone();
                tokio::spawn(async move {
                    if let Err(e) = handle_connection(conn, worker, control_timeout).await {
                        crate::logging::notify(
                            crate::zap::INFO,
                            "livekit",
                            CALLER_CONN_STOPPED,
                            "webtransport connection stopped",
                            &[("error", ZapValue::Str(e.to_string()))],
                            None,
                        );
                    }
                });
            }
            Err(e) => {
                // Handshake-level failure; Go's ServeQUICConn swallows these and
                // only logs post-serve errors. Normalize: no log here.
                tracing::debug!("webtransport handshake failed: {e}");
            }
        }
    }
    // Close the endpoint so the CONNECTION_CLOSE reaches peers, then stop.
    ep.close(quinn::VarInt::from_u32(0), b"shutdown");
    crate::logging::notify(
        crate::zap::INFO,
        "livekit",
        CALLER_STOPPED,
        "webtransport listener stopped",
        &[("error", ZapValue::Str("listener closed".into()))],
        None,
    );
}

/// One h3/WebTransport connection: run the mixed request/stream accept pump and
/// forward upgraded sessions' streams to their waiting handlers.
async fn handle_connection(
    conn: quinn::Connection,
    worker: WorkerHttpState,
    control_timeout: Duration,
) -> lk_webtransport::WtResult<()> {
    let mut wt = WtServerConnection::new(conn)
        .await?
        .with_session_uni_routing();
    // session id -> the waiting control-stream handler.
    let mut sessions: std::collections::HashMap<SessionId, mpsc::UnboundedSender<WtBidiStream>> =
        std::collections::HashMap::new();
    let (finished_tx, mut finished_rx) = mpsc::unbounded_channel();
    // The datagram pump needs the bare connection (the h3 layer does not
    // demux QUIC datagrams), so it runs next to the accept pump.
    let datagram_conn = wt.connection().clone();
    loop {
        let item = tokio::select! {
            finished = finished_rx.recv() => {
                if let Some(id) = finished {
                    sessions.remove(&id);
                    wt.drop_session(id);
                }
                continue;
            }
            item = wt.accept() => item,
            datagram = lk_webtransport::accept_datagram(&datagram_conn) => {
                match datagram {
                    // A datagram for a session this connection does not know is
                    // dropped, exactly like quic-go's `receiveDatagrams`.
                    Ok(Some(_)) => continue,
                    Ok(None) => continue,
                    // A malformed datagram closed the connection with
                    // H3_DATAGRAM_ERROR; stop serving it.
                    Err(e) => {
                        tracing::debug!("webtransport datagram error: {e}");
                        break;
                    }
                }
            }
        };
        let item = match item {
            Ok(Some(item)) => item,
            Ok(None) => break,
            Err(e) => return Err(e),
        };
        match item {
            Accepted::Request(req, stream) => {
                route_request(
                    &mut wt,
                    req,
                    *stream,
                    &worker,
                    control_timeout,
                    &mut sessions,
                    &finished_tx,
                )
                .await;
            }
            Accepted::Bidi(session, mut stream) => {
                let code = if let Some(tx) = sessions.get(&session) {
                    if let Err(error) = tx.send(stream) {
                        stream = error.0;
                    } else {
                        continue;
                    }
                    // The session is closed but still registered. Go adds this
                    // stream to the closed session and resets it as owned.
                    // Keep the route until the completion notification so the
                    // same rule applies to every stream in this window.
                    lk_webtransport::WT_SESSION_GONE_ERROR_CODE
                } else {
                    wt.drop_session(session);
                    lk_webtransport::WT_BUFFERED_STREAM_REJECTED_ERROR_CODE
                };
                stream.stop_sending(code);
                stream.reset(code);
            }
        }
    }
    Ok(())
}

/// Reproduce the request-scoped chain (normalize -> auth -> mux -> `/agent`) on
/// one CONNECT/plain request stream.
async fn route_request(
    wt: &mut WtServerConnection,
    req: http::Request<()>,
    mut stream: lk_webtransport::ServerConnect,
    worker: &WorkerHttpState,
    control_timeout: Duration,
    sessions: &mut std::collections::HashMap<SessionId, mpsc::UnboundedSender<WtBidiStream>>,
    finished: &mpsc::UnboundedSender<SessionId>,
) {
    // WithPathNormalization: strip one leading slash on `//...`.
    let mut uri = req.uri().clone();
    if let Some(stripped) = lk_compat::http::strip_double_slash(&uri) {
        uri = stripped;
    }
    let path = uri.path().to_string();
    let query = uri.query().unwrap_or("");

    // APIKeyAuthMiddleware: 401 on a malformed bearer header or an unparseable/
    // unverifiable token; otherwise grants reach the mux (possibly none).
    let headers = req.headers().clone();
    let token = match extract_token(&headers, query) {
        Ok(token) => token,
        Err(e) => {
            answer(&mut stream, e.status(), e.to_string().as_bytes()).await;
            return;
        }
    };
    let grants = match authenticate(&worker.keys, token.as_deref()) {
        Ok(grants) => grants,
        Err(e) => {
            answer(&mut stream, e.status(), e.to_string().as_bytes()).await;
            return;
        }
    };

    // ServeMux: only `/agent` is registered.
    if path != "/agent" {
        answer(&mut stream, StatusCode::NOT_FOUND, NOT_FOUND).await;
        return;
    }

    // AgentWTService: `GetGrants` nil or without `Video.Agent` is 401.
    if !matches!(&grants, Some(g) if g.video.agent) {
        answer(&mut stream, StatusCode::UNAUTHORIZED, PERMISSION_DENIED).await;
        return;
    }

    // UpgradeAgentWebTransport: a non-WebTransport-CONNECT request that reaches
    // here logs the upgrade failure and answers implicitly (200, empty body).
    if !is_webtransport_connect(&req) {
        crate::logging::notify(
            crate::zap::WARN,
            "livekit",
            CALLER_UPGRADE_FAILED,
            "agent webtransport upgrade failed",
            &[],
            None,
        );
        answer(&mut stream, StatusCode::OK, b"").await;
        return;
    }

    // Go reads `protocol` from the request form (defaulting the registration
    // protocol) and carries the authenticated api key into the worker's
    // registration; both are needed once the control stream is accepted, so
    // capture them before `req` is consumed by the upgrade.
    let protocol = query_param(query, "protocol")
        .and_then(|v| v.parse::<i64>().ok())
        .unwrap_or(1);
    let api_key = grants
        .as_ref()
        .map(|g| g.api_key.clone())
        .unwrap_or_default();

    match upgrade_connect(wt, req, stream).await {
        Ok(session) => {
            let (tx, rx) = mpsc::unbounded_channel();
            sessions.insert(session.id(), tx);
            let id = session.id();
            let finished = finished.clone();
            let worker = worker.clone();
            tokio::spawn(async move {
                wait_control_stream(session, rx, control_timeout, worker, protocol, api_key).await;
                let _ = finished.send(id);
            });
        }
        Err(e) => {
            crate::logging::notify(
                crate::zap::WARN,
                "livekit",
                CALLER_UPGRADE_FAILED,
                "agent webtransport upgrade failed",
                &[("error", ZapValue::Str(e.to_string()))],
                None,
            );
        }
    }
}

/// The write half of the agent control stream, terminated with an
/// application error code when it is dropped.
///
/// `webtransport-go` ends a session by closing every stream the session owns
/// (`CancelWrite` + `CancelRead` with `WTSessionGoneErrorCode`) before the
/// `WT_CLOSE_SESSION` capsule is written. Quinn's `SendStream` drop only sends
/// a FIN, so the half is wrapped to carry the session's error code instead of
/// silently closing the stream.
struct ResetOnDrop {
    inner: Box<dyn ControlWriter>,
    code: u64,
}

impl ResetOnDrop {
    fn new(inner: Box<dyn ControlWriter>, code: u64) -> Self {
        Self { inner, code }
    }
}

impl tokio::io::AsyncWrite for ResetOnDrop {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.get_mut().inner).poll_write(cx, buf)
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().inner).poll_flush(cx)
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().inner).poll_shutdown(cx)
    }
}

impl Drop for ResetOnDrop {
    fn drop(&mut self) {
        self.inner.reset(self.code);
    }
}

/// The read half of the agent control stream, terminated with an application
/// error code when it is dropped (Go's `CancelRead`, a QUIC `STOP_SENDING`).
struct StopOnDrop {
    inner: Box<dyn ControlReader>,
    code: u64,
}

impl StopOnDrop {
    fn new(inner: Box<dyn ControlReader>, code: u64) -> Self {
        Self { inner, code }
    }
}

impl tokio::io::AsyncRead for StopOnDrop {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().inner).poll_read(cx, buf)
    }
}

impl Drop for StopOnDrop {
    fn drop(&mut self) {
        self.inner.stop_sending(self.code);
    }
}

/// HTTP exchange stream: cancellation releases both halves, while a clean FIN
/// must remain intact instead of being reset on Drop (endpoint/webtransport.go).
struct EndpointBidi {
    read: Box<dyn ControlReader>,
    write: Box<dyn ControlWriter>,
    finished: bool,
}
impl tokio::io::AsyncRead for EndpointBidi {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().read)
            .poll_read(cx, buf)
            .map_err(
                |error| match lk_webtransport::remote_stream_reset_code(&error) {
                    Some(code) => {
                        io::Error::other(lk_compat::agent::endpoint::RemoteResponseReset(code))
                    }
                    None => error,
                },
            )
    }
}
impl tokio::io::AsyncWrite for EndpointBidi {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.get_mut().write).poll_write(cx, buf)
    }
    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().write).poll_flush(cx)
    }
    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        let result = Pin::new(&mut this.write).poll_shutdown(cx);
        if matches!(result, Poll::Ready(Ok(()))) {
            this.finished = true;
        }
        result
    }
}
// HSR_ABORT is application code zero. webtransport-go v0.12.0
// maps it into the WebTransport stream error range before QUIC sees it.
const ENDPOINT_ABORT_WIRE_CODE: u64 = 0x52e4_a40f_a8db;

impl Drop for EndpointBidi {
    fn drop(&mut self) {
        if !self.finished {
            self.write.reset(ENDPOINT_ABORT_WIRE_CODE);
        }
        self.read.stop_sending(ENDPOINT_ABORT_WIRE_CODE);
    }
}

#[cfg(test)]
static ATTACH_OBSERVERS: std::sync::LazyLock<
    std::sync::Mutex<std::collections::HashMap<SocketAddr, tokio::sync::oneshot::Sender<()>>>,
> = std::sync::LazyLock::new(Default::default);

/// `AcceptAgentControlStream` + `handleConnection`: wait for the worker's first
/// bidi stream within `control_timeout` (Go `agent.RegisterTimeout`). On success
/// the stream becomes the framed `WorkerMessage`/`ServerMessage` control plane
/// ([`wt_control::session`], sharing the WebSocket `/agent` registry); when that
/// session ends only that session is torn down, mirroring Go's deferred
/// `sigConn.Close()` (`CloseWithError(SessionCloseOK, "")`). On timeout with no
/// control stream the session is closed with reason `no control stream`.
async fn wait_control_stream(
    session: WtSession,
    mut rx: mpsc::UnboundedReceiver<WtBidiStream>,
    control_timeout: Duration,
    worker: WorkerHttpState,
    protocol: i64,
    api_key: String,
) {
    let mut session = session;
    // CONNECT capsules are independent of the control bidi stream. A peer may
    // close just this session while leaving control/endpoint streams and the
    // shared QUIC connection open (webtransport-go Session.handleCapsules).
    let accepted = tokio::select! {
        closed = session.read_close_capsule() => {
            rx.close();
            while let Ok(stream) = rx.try_recv() {
                session.streams().register(stream);
            }
            match closed {
                Ok(Some(_)) => session.close_from_peer(),
                _ => session.close_from_peer_read_error(),
            }
            return;
        }
        accepted = tokio::time::timeout(control_timeout, rx.recv()) => accepted,
    };
    match accepted {
        Ok(Some(control)) => {
            let streams = session.streams().clone();
            let open = session.owned_bidi_opener();
            let endpoint_opener: lk_compat::agent::endpoint::StreamOpener =
                std::sync::Arc::new(move || {
                    let opening = open();
                    Box::pin(async move {
                        let stream = opening.await.map_err(|e| io::Error::other(e.to_string()))?;
                        let (write, read) = stream.into_split();
                        Ok(Box::new(EndpointBidi {
                            read,
                            write,
                            finished: false,
                        })
                            as lk_compat::agent::endpoint::StreamIo)
                    })
                });
            let (write, read) = control.into_split();
            // Give the control loop a session-local shutdown signal so it
            // completes its normal disconnect event (including released jobs),
            // rather than only dropping its registry guard on cancellation.
            let mut server_shutdown = worker.shutdown.clone();
            let (session_shutdown, shutdown) = watch::channel(*server_shutdown.borrow());
            let mut worker = worker;
            worker.shutdown = shutdown;
            {
                let control = wt_control::session_with_endpoints(
                    worker,
                    StopOnDrop::new(read, lk_webtransport::WT_SESSION_GONE_ERROR_CODE),
                    ResetOnDrop::new(write, lk_webtransport::WT_SESSION_GONE_ERROR_CODE),
                    protocol,
                    api_key,
                    Some(endpoint_opener),
                );
                tokio::pin!(control);
                #[cfg(test)]
                let remote = session.connection().remote_address();
                let peer_read = {
                    let close = session.read_close_capsule();
                    tokio::pin!(close);
                    loop {
                        tokio::select! {
                            closed = &mut close => break Some(closed),
                            _ = &mut control => break None,
                            _ = server_shutdown.changed() => {
                                let _ = session_shutdown.send(true);
                                (&mut control).await;
                                break None;
                            }
                            stream = rx.recv() => match stream {
                                Some(stream) => {
                                    streams.register(stream);
                                    #[cfg(test)]
                                    if let Some(attached) = ATTACH_OBSERVERS
                                        .lock()
                                        .unwrap()
                                        .remove(&remote)
                                    {
                                        let _ = attached.send(());
                                    }
                                }
                                None => break Some(Ok(None)),
                            },
                        }
                    }
                };
                // Close the transport before waiting for the worker's normal
                // disconnect notification, which can block on its event queue.
                if let Some(closed) = peer_read {
                    rx.close();
                    streams.close();
                    while let Ok(stream) = rx.try_recv() {
                        streams.register(stream);
                    }
                    match closed {
                        Ok(Some(_)) => session.close_from_peer(),
                        _ => session.close_from_peer_read_error(),
                    }
                    let _ = session_shutdown.send(true);
                    control.await;
                    return;
                }
            }
            // Terminate queued streams as well as ones already registered.
            rx.close();
            streams.close();
            while let Ok(stream) = rx.try_recv() {
                streams.register(stream);
            }
            let _ = session
                .close_with_error(lk_webtransport::SESSION_CLOSE_OK, "")
                .await;
        }
        Ok(None) => session.close_from_peer(),
        Err(_elapsed) => {
            let _ = session
                .close_with_error(lk_webtransport::SESSION_CLOSE_OK, "no control stream")
                .await;
        }
    }
}

/// Send a status + `text/plain` body and finish the stream (Go `HandleError`
/// writes the status line plus `err.Error()` bytes).
async fn answer(stream: &mut lk_webtransport::ServerConnect, status: StatusCode, body: &[u8]) {
    let response = Response::builder()
        .status(status)
        .header(header::CONTENT_TYPE, "text/plain; charset=utf-8")
        .body(())
        .expect("status+content-type response");
    let _ = reject_with_body(stream, response, Bytes::copy_from_slice(body)).await;
}

/// Generate the dev-mode self-signed ECDSA P-256 certificate (Go
/// `WebTransportTLS` dev branch): serial 1, `CN=livekit-webtransport-dev`,
/// `-1h`/`+365d`, SANs `localhost` + `127.0.0.1` + `::1`.
fn generate_dev_cert() -> Result<(Vec<CertificateDer<'static>>, PrivateKeyDer<'static>), SetupError>
{
    let signing_key = random_signing_key()?;
    let serial = SerialNumber::new(&[1])
        .map_err(|e| SetupError::text(format!("webtransport dev serial: {e}")))?;

    let now = SystemTime::now();
    let not_before = Time::try_from(now - Duration::from_secs(3600))
        .map_err(|e| SetupError::text(format!("webtransport dev not_before: {e}")))?;
    let not_after = Time::try_from(now + Duration::from_secs(365 * 24 * 3600))
        .map_err(|e| SetupError::text(format!("webtransport dev not_after: {e}")))?;
    let validity = Validity::new(not_before, not_after);

    let subject = Name::from_str("CN=livekit-webtransport-dev")
        .map_err(|e| SetupError::text(format!("webtransport dev subject: {e}")))?;
    let names: GeneralNames = vec![
        GeneralName::DnsName(
            x509_cert::der::asn1::Ia5String::new("localhost")
                .map_err(|e| SetupError::text(format!("webtransport dev san: {e}")))?,
        ),
        GeneralName::from(IpAddr::V4(Ipv4Addr::LOCALHOST)),
        GeneralName::from(IpAddr::V6(Ipv6Addr::LOCALHOST)),
    ];
    let spki = SubjectPublicKeyInfo::from_key(signing_key.verifying_key())
        .map_err(|e| SetupError::text(format!("webtransport dev spki: {e}")))?;

    let profile = WtDevProfile {
        subject,
        names: SubjectAltName(names),
    };
    let cert = CertificateBuilder::new(profile, serial, validity, spki)
        .map_err(|e| SetupError::text(format!("webtransport dev builder: {e}")))?
        .build::<_, DerSignature>(&signing_key)
        .map_err(|e| SetupError::text(format!("webtransport dev cert: {e}")))?;

    let der = cert
        .to_der()
        .map_err(|e| SetupError::text(format!("webtransport dev encode: {e}")))?;
    let key = signing_key
        .to_pkcs8_der()
        .map_err(|e| SetupError::text(format!("webtransport dev key: {e}")))?;
    let private_key = PrivateKeyDer::Pkcs8(key.as_bytes().to_vec().into());
    Ok((vec![CertificateDer::from(der)], private_key))
}

/// Self-signed WebTransport dev endpoint profile: `digitalSignature` key usage
/// and the SAN set the Go template declares.
struct WtDevProfile {
    subject: Name,
    names: SubjectAltName,
}

impl BuilderProfile for WtDevProfile {
    fn get_issuer(&self, subject: &Name) -> Name {
        subject.clone()
    }

    fn get_subject(&self) -> Name {
        self.subject.clone()
    }

    fn build_extensions(
        &self,
        _spk: SubjectPublicKeyInfoRef<'_>,
        _issuer_spk: SubjectPublicKeyInfoRef<'_>,
        tbs: &TbsCertificate,
    ) -> x509_cert::builder::Result<Vec<Extension>> {
        let constraints = BasicConstraints {
            ca: false,
            path_len_constraint: None,
        }
        .to_extension(tbs.subject(), &[])?;
        let mut extensions = vec![constraints];
        extensions.push(
            KeyUsage(KeyUsages::DigitalSignature.into())
                .to_extension(tbs.subject(), &extensions)?,
        );
        extensions.push(self.names.to_extension(tbs.subject(), &extensions)?);
        Ok(extensions)
    }
}

/// `rand.Read` equivalent: fill `buf` from the OS CSPRNG (getrandom).
fn random_bytes(len: usize) -> Vec<u8> {
    let mut buf = vec![0u8; len];
    // getrandom's fill is infallible on supported platforms in practice; a hard
    // failure would make the whole process unable to bind, matching Go's
    // `ecdsa.GenerateKey` error being fatal to startup.
    getrandom::fill(&mut buf).expect("os random source available for webtransport listener");
    buf
}

/// A uniformly random 32-byte P-256 scalar is valid with overwhelming
/// probability; retry the rare out-of-range/zero value (mirrors `rtc-crypto`).
fn random_signing_key() -> Result<SigningKey, SetupError> {
    for _ in 0..8 {
        let bytes = random_bytes(32);
        if let Ok(key) = SigningKey::from_slice(&bytes) {
            return Ok(key);
        }
    }
    Err(SetupError::text(
        "webtransport dev cert: could not derive a valid P-256 scalar",
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use lk_compat::agent::registry::{Limits, Registry};
    use lk_compat::auth::{KeyStore, TokenClaims, TokenMinter};
    use lk_protocol::livekit as pb;
    use lk_webtransport::{
        WtBidiStream, WtClient, client_config, client_config_insecure, client_endpoint,
        connect_with_headers,
    };
    use prost::Message as _;
    use rtc_core::permission::VideoGrant;
    use std::sync::Arc;

    const API_KEY: &str = "APIwebtransportacceptkey";
    const SECRET: &str = "webtransportacceptsecret0123456789abcdefghij";

    fn key_store() -> Arc<KeyStore> {
        Arc::new(KeyStore::new(vec![(
            API_KEY.to_string(),
            SECRET.to_string(),
        )]))
    }

    /// A worker control-plane state for the acceptance pumps: the data plane
    /// shares the WebSocket registry, so the event queue needs a live consumer
    /// (a drain task) for `Registered`/`Disconnected` sends to succeed.
    fn worker_state(keys: Arc<KeyStore>) -> WorkerHttpState {
        let tokens = Arc::new(TokenMinter::new(keys.clone()));
        let (events, mut rx) = mpsc::channel(64);
        tokio::spawn(async move { while rx.recv().await.is_some() {} });
        let (shutdown_tx, shutdown) = watch::channel(false);
        // Mirror the production `AgentService`, which holds the shutdown sender
        // for the server lifetime. Dropping it here would make the session's
        // `shutdown.changed()` resolve at once and cancel the control handshake.
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

    fn mint_agent_token(keys: &Arc<KeyStore>, agent: bool) -> String {
        let minter = TokenMinter::new(keys.clone());
        let claims = TokenClaims {
            identity: "worker".into(),
            video: Some(VideoGrant {
                agent,
                ..Default::default()
            }),
            ..Default::default()
        };
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs() as i64;
        minter
            .mint(API_KEY, &claims, Duration::from_secs(600), now)
            .unwrap()
    }

    fn bound_socket() -> (UdpSocket, SocketAddr) {
        let socket = UdpSocket::bind("127.0.0.1:0").expect("bind udp");
        let addr = socket.local_addr().expect("local addr");
        socket.set_nonblocking(true).expect("nonblocking");
        (socket, addr)
    }

    /// Start a dev-mode listener on loopback:port 0 and return the bound
    /// address plus the pump handles. The `stop_tx` keeps the pumps alive for
    /// the caller's lifetime; it is dropped only at test end.
    async fn start_dev_listener(
        keys: Arc<KeyStore>,
        control_timeout: Duration,
    ) -> (SocketAddr, Vec<JoinHandle<()>>, watch::Sender<bool>) {
        let tls = build_server_tls("", "", true).expect("dev tls");
        let listener = bind(&tls, &["127.0.0.1".to_owned()], 0)
            .await
            .expect("bind");
        let addr = listener.bound_addrs()[0];
        let (stop_tx, stop_rx) = watch::channel(false);
        let handles = listener.spawn(worker_state(keys), control_timeout, stop_rx);
        (addr, handles, stop_tx)
    }

    /// Dial an insecure (dev-cert) client. Returns the client and its endpoint;
    /// dropping the endpoint would stop driving the connection, so the caller
    /// holds both until done (mirrors the `lk-webtransport` acceptance test).
    async fn dial_insecure(
        addr: SocketAddr,
        path: &str,
        headers: &[(&str, &str)],
    ) -> lk_webtransport::WtResult<(WtClient, quinn::Endpoint)> {
        let cfg = client_config_insecure().expect("insecure client config");
        let (socket, _) = bound_socket();
        let ep = client_endpoint(cfg, b"client-reset-key", socket).expect("client endpoint");
        let client = connect_with_headers(&ep, addr, "localhost", path, headers).await?;
        Ok((client, ep))
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn endpoint_stream_drop_sends_wt_abort_and_preserves_clean_fin_on_real_quic() {
        use tokio::io::AsyncWriteExt as _;

        // Fixed webtransport-go v0.12.0 errors.go wire value for application
        // HSR_ABORT (0). Keep the peer expectation independent of the producer.
        const WANT: u64 = 0x52e4_a40f_a8db;
        let (socket, addr) = bound_socket();
        let tls = build_server_tls("", "", true).unwrap();
        let config = server_config(&tls, b"abort-token-key-for-physical-test-12345").unwrap();
        let server = endpoint(config, b"abort-reset-key", socket).unwrap();
        let (release, mut release_rx) = mpsc::channel(1);
        let (server_checked, peer_may_drop) = tokio::sync::oneshot::channel();
        let server_flow = async {
            let incoming = server
                .accept()
                .await
                .expect("actual worker QUIC connection");
            let mut conn = WtServerConnection::new(incoming.await.unwrap())
                .await
                .unwrap();
            let Some(Accepted::Request(req, stream)) = conn.accept().await.unwrap() else {
                panic!("actual WebTransport CONNECT");
            };
            let session = upgrade_connect(&mut conn, req, *stream).await.unwrap();
            let open = session.owned_bidi_opener();
            for finish in [false, true] {
                let (write, read) = open().await.unwrap().into_split();
                let mut stream = EndpointBidi {
                    read,
                    write,
                    finished: false,
                };
                stream.write_all(b"complete-prefix").await.unwrap();
                assert_eq!(
                    release_rx.recv().await,
                    Some(finish),
                    "peer has read the prefix"
                );
                if finish {
                    stream.shutdown().await.unwrap();
                }
                drop(stream);
            }
            // Keep the same CONNECT/QUIC alive until the peer has observed both
            // stream terminations and confirmed the session stayed usable.
            assert_eq!(release_rx.recv().await, Some(true));
            let close_reason = session.connection().close_reason();
            assert!(
                close_reason.is_none(),
                "server connection closed: {close_reason:?}"
            );
            server_checked
                .send(())
                .expect("peer kept its connection alive");
        };
        let peer_flow = async {
            let (mut client, endpoint) = dial_insecure(addr, "/agent", &[]).await.unwrap();
            for finish in [false, true] {
                let (sid, mut stream) = client.accept_bi().await.unwrap();
                assert_eq!(sid, client.session().id());
                let mut prefix = Vec::new();
                while prefix.len() < b"complete-prefix".len() {
                    let bytes = stream
                        .recv_data()
                        .await
                        .unwrap()
                        .expect("actual payload before drop");
                    prefix.extend_from_slice(&bytes);
                }
                assert_eq!(prefix, b"complete-prefix");
                release.send(finish).await.unwrap();
                match (finish, stream.recv_data().await) {
                    (true, Ok(None)) => {}
                    (false, Err(lk_webtransport::WtError::StreamTerminated(WANT))) => {}
                    (finish, result) => panic!("worker RESET/FIN with clean={finish}: {result:?}"),
                }
                // Keep the worker's sending direction open so actual
                // STOP_SENDING is visible through the peer's stream writer.
                loop {
                    match stream.send_data(Bytes::from_static(b"still writing")).await {
                        Ok(()) => tokio::time::sleep(Duration::from_millis(2)).await,
                        Err(lk_webtransport::WtError::StreamTerminated(WANT)) => break,
                        Err(error) => panic!("worker STOP_SENDING code: {error:?}"),
                    }
                }
            }
            assert!(client.session().connection().close_reason().is_none());
            release.send(true).await.unwrap();
            // A queued notification does not mean the server has checked yet.
            // Keep the client and its QUIC endpoint alive until that check.
            peer_may_drop
                .await
                .expect("server checked the live connection");
            drop((client, endpoint));
        };
        tokio::time::timeout(Duration::from_secs(8), async {
            tokio::join!(server_flow, peer_flow);
        })
        .await
        .expect("real WT stream wire reset and clean FIN are required");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn endpoint_remote_reset_drives_http_status_and_retries_on_real_quic() {
        use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};

        async fn register(
            addr: SocketAddr,
            token: &str,
            instance: &str,
        ) -> (WtClient, quinn::Endpoint, WtBidiStream) {
            let (client, endpoint) = dial_insecure(addr, "/agent", &[("authorization", token)])
                .await
                .unwrap();
            let mut control = client.session().open_bi().await.unwrap();
            let register = pb::WorkerMessage {
                message: Some(pb::worker_message::Message::Register(
                    pb::RegisterWorkerRequest {
                        agent_name: "reset-worker".into(),
                        instance_id: instance.into(),
                        endpoint_protocol: 1,
                        endpoints: vec![pb::agent_http::AgentEndpoint {
                            path: "/reset".into(),
                            methods: vec!["GET".into(), "POST".into()],
                            public: true,
                            ..Default::default()
                        }],
                        ..Default::default()
                    },
                )),
            }
            .encode_to_vec();
            let mut frame = (register.len() as u32).to_be_bytes().to_vec();
            frame.extend_from_slice(&register);
            control.send_data(frame.into()).await.unwrap();
            let reply = read_one_frame(&mut control)
                .await
                .expect("real worker registration");
            let message = pb::ServerMessage::decode(reply.as_slice()).unwrap();
            let Some(pb::server_message::Message::Register(reply)) = message.message else {
                panic!("real worker registration response");
            };
            assert_eq!(reply.endpoint_settings.unwrap().protocol, 1);
            (client, endpoint, control)
        }

        async fn incoming(left: &mut WtClient, right: &mut WtClient) -> (usize, WtBidiStream) {
            tokio::select! {
                stream = left.accept_bi() => (0, stream.unwrap().1),
                stream = right.accept_bi() => (1, stream.unwrap().1),
            }
        }

        async fn head(
            stream: &mut WtBidiStream,
            method: &str,
            body: &str,
            request_id: &str,
        ) -> Vec<u8> {
            let mut input = Vec::new();
            loop {
                let chunk = stream
                    .recv_data()
                    .await
                    .unwrap()
                    .expect("worker request bytes");
                input.extend_from_slice(&chunk);
                if input.len() < 4 {
                    continue;
                }
                let size = u32::from_be_bytes(input[..4].try_into().unwrap()) as usize;
                assert!(size <= 64 * 1024);
                if input.len() < 4 + size {
                    continue;
                }
                let rest = &input[4 + size..];
                let Some(end) = rest.windows(4).position(|chunk| chunk == b"\r\n\r\n") else {
                    continue;
                };
                if rest.len() < end + 4 + body.len() {
                    continue;
                }
                let preamble = pb::agent_http::StreamPreamble::decode(&input[4..4 + size]).unwrap();
                assert_eq!(preamble.request_id, request_id);
                assert_eq!(preamble.route, "/reset");
                assert!(rest.starts_with(format!("{method} /reset HTTP/1.1\r\n").as_bytes()));
                assert_eq!(&rest[end + 4..], body.as_bytes());
                return rest.to_vec();
            }
        }

        async fn request(addr: SocketAddr, method: &str, body: &str, id: &str) -> String {
            let mut socket = tokio::net::TcpStream::connect(addr).await.unwrap();
            socket.write_all(format!(
                "{method} /agents/reset-worker/_/reset HTTP/1.1\r\nHost: localhost\r\nX-Request-Id: {id}\r\nConnection: close\r\nContent-Length: {}\r\n\r\n{body}",
                body.len(),
            ).as_bytes()).await.unwrap();
            let mut response = String::new();
            socket.read_to_string(&mut response).await.unwrap();
            response
        }

        const APP_RESET: u64 = 0x52e4_a40f_a8db;
        let keys = key_store();
        let token = format!("Bearer {}", mint_agent_token(&keys, true));
        let mut state = worker_state(keys);
        let (events, mut event_rx) = mpsc::channel(64);
        state.events = events;
        let tls = build_server_tls("", "", true).unwrap();
        let wt = bind(&tls, &["127.0.0.1".to_owned()], 0).await.unwrap();
        let addr = wt.bound_addrs()[0];
        let (wt_stop, wt_shutdown) = watch::channel(false);
        let wt_handles = wt.spawn(state.clone(), Duration::from_secs(30), wt_shutdown);
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let http_addr = listener.local_addr().unwrap();
        let (http_stop, http_shutdown) = tokio::sync::oneshot::channel();
        let app = lk_compat::agent::endpoint::routes(state);
        let http_server = tokio::spawn(async move {
            axum::serve(listener, app)
                .with_graceful_shutdown(async move {
                    let _ = http_shutdown.await;
                })
                .await
                .unwrap();
        });

        tokio::time::timeout(Duration::from_secs(12), async {
            let (mut left, ep_left, _control_left) = register(addr, &token, "reset-left").await;
            assert!(matches!(
                event_rx.recv().await,
                Some(lk_compat::agent::websocket::WorkerEvent::Registered { .. })
            ));
            let (mut right, ep_right, _control_right) = register(addr, &token, "reset-right").await;
            assert!(matches!(
                event_rx.recv().await,
                Some(lk_compat::agent::websocket::WorkerEvent::Registered { .. })
            ));
            // Codes 0/1/2/3/4 are the pinned protocol's ABORT/REFUSED/
            // INTERNAL/TIMEOUT/PROTOCOL; 2 denotes application dispatch.
            for (index, (method, body, code, second_worker, exhaust, status)) in [
                ("POST", "", 1, true, false, 200),
                ("GET", "", 3, true, false, 200),
                ("GET", "", 4, true, false, 200),
                ("GET", "", 0, true, false, 200),
                ("GET", "", 3, true, true, 503),
                ("POST", "", 3, false, false, 504),
                ("GET", "", 2, false, false, 502),
                ("POST", "", 0, false, false, 502),
                ("POST", "body", 1, false, false, 502),
            ].into_iter().enumerate() {
                let request_id = format!("physical-reset-{index}");
                let http = async {
                    let response = request(http_addr, method, body, &request_id).await;
                    assert!(
                        response.starts_with(&format!("HTTP/1.1 {status} ")),
                        "phase {index}: {response}"
                    );
                    response
                };
                let worker = async {
                    let (first_worker, mut stream) = incoming(&mut left, &mut right).await;
                    let first_head = head(&mut stream, method, body, &request_id).await;
                    stream.reset(APP_RESET + code);
                    if second_worker {
                        let (second_worker, mut second) = incoming(&mut left, &mut right).await;
                        assert_ne!(first_worker, second_worker, "retry chose a distinct registration");
                        assert_eq!(head(&mut second, method, body, &request_id).await, first_head,
                            "attempts must carry identical canonical request heads");
                        if exhaust {
                            second.reset(APP_RESET + code);
                        } else {
                            second.send_data(Bytes::from_static(b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nok")).await.unwrap();
                            second.finish().await.unwrap();
                        }
                    }
                };
                let (response, ()) = tokio::join!(http, worker);
                if status == 200 {
                    assert!(response.ends_with("\r\n\r\nok"));
                }
                if exhaust {
                    assert!(response.to_ascii_lowercase().contains("\r\nretry-after: 1\r\n"));
                }
                assert!(tokio::time::timeout(Duration::from_millis(35), incoming(&mut left, &mut right)).await.is_err(),
                    "phase {index}: unexpected additional worker attempt");
                println!("RUST_ENDPOINT_RESPONSE_RESET phase={index} method={method} code={code} attempts={} http={status}", 1 + usize::from(second_worker));
            }
            assert!(left.session().connection().close_reason().is_none());
            assert!(right.session().connection().close_reason().is_none());
            ep_left.close(0u32.into(), b"fixture complete");
            ep_right.close(0u32.into(), b"fixture complete");
        }).await.expect("real HTTP/WT worker campaigns");
        let _ = http_stop.send(());
        wt_stop.send(true).unwrap();
        tokio::time::timeout(Duration::from_secs(5), async {
            http_server.await.unwrap();
            for task in wt_handles {
                task.await.unwrap();
            }
        })
        .await
        .expect("owned HTTP and WebTransport listeners stopped");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn worker_informational_heads_reach_tcp_before_final_actual_wt() {
        use tokio::io::{AsyncBufReadExt as _, AsyncReadExt as _, AsyncWriteExt as _};

        async fn register(
            addr: SocketAddr,
            token: &str,
            instance: &str,
        ) -> (WtClient, quinn::Endpoint, WtBidiStream) {
            let (client, endpoint) = dial_insecure(addr, "/agent", &[("authorization", token)])
                .await
                .unwrap();
            let mut control = client.session().open_bi().await.unwrap();
            let register = pb::WorkerMessage {
                message: Some(pb::worker_message::Message::Register(
                    pb::RegisterWorkerRequest {
                        agent_name: "hints-worker".into(),
                        instance_id: instance.into(),
                        endpoint_protocol: 1,
                        endpoints: vec![pb::agent_http::AgentEndpoint {
                            path: "/hints".into(),
                            methods: vec!["GET".into()],
                            public: true,
                            ..Default::default()
                        }],
                        ..Default::default()
                    },
                )),
            }
            .encode_to_vec();
            let mut frame = (register.len() as u32).to_be_bytes().to_vec();
            frame.extend_from_slice(&register);
            control.send_data(frame.into()).await.unwrap();
            let reply = read_one_frame(&mut control)
                .await
                .expect("real worker registration");
            let message = pb::ServerMessage::decode(reply.as_slice()).unwrap();
            let Some(pb::server_message::Message::Register(reply)) = message.message else {
                panic!("real worker registration response");
            };
            assert_eq!(reply.endpoint_settings.unwrap().protocol, 1);
            (client, endpoint, control)
        }

        async fn incoming(left: &mut WtClient, right: &mut WtClient) -> (usize, WtBidiStream) {
            tokio::select! {
                stream = left.accept_bi() => (0, stream.unwrap().1),
                stream = right.accept_bi() => (1, stream.unwrap().1),
            }
        }

        async fn head(
            stream: &mut WtBidiStream,
            method: &str,
            body: &str,
            request_id: &str,
        ) -> Vec<u8> {
            let mut input = Vec::new();
            loop {
                let chunk = stream
                    .recv_data()
                    .await
                    .unwrap()
                    .expect("worker request bytes");
                input.extend_from_slice(&chunk);
                if input.len() < 4 {
                    continue;
                }
                let size = u32::from_be_bytes(input[..4].try_into().unwrap()) as usize;
                assert!(size <= 64 * 1024);
                if input.len() < 4 + size {
                    continue;
                }
                let rest = &input[4 + size..];
                let Some(end) = rest.windows(4).position(|chunk| chunk == b"\r\n\r\n") else {
                    continue;
                };
                if rest.len() < end + 4 + body.len() {
                    continue;
                }
                let preamble = pb::agent_http::StreamPreamble::decode(&input[4..4 + size]).unwrap();
                assert_eq!(preamble.request_id, request_id);
                assert_eq!(preamble.route, "/hints");
                assert!(rest.starts_with(format!("{method} /hints HTTP/1.1\r\n").as_bytes()));
                assert_eq!(&rest[end + 4..], body.as_bytes());
                return rest.to_vec();
            }
        }

        let keys = key_store();
        let token = format!("Bearer {}", mint_agent_token(&keys, true));
        let mut state = worker_state(keys);
        let (events, mut event_rx) = mpsc::channel(64);
        state.events = events;
        let tls = build_server_tls("", "", true).unwrap();
        let wt = bind(&tls, &["127.0.0.1".to_owned()], 0).await.unwrap();
        let addr = wt.bound_addrs()[0];
        let (wt_stop, wt_shutdown) = watch::channel(false);
        let wt_handles = wt.spawn(state.clone(), Duration::from_secs(30), wt_shutdown);
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let http_addr = listener.local_addr().unwrap();
        let (http_stop, http_shutdown) = tokio::sync::oneshot::channel();
        let app = lk_compat::http::agent_endpoint_router(state);
        let http_server = tokio::spawn(async move {
            axum::serve(listener, app)
                .with_graceful_shutdown(async move {
                    let _ = http_shutdown.await;
                })
                .await
                .unwrap();
        });

        tokio::time::timeout(Duration::from_secs(12), async {
            let (mut left, ep_left, _control_left) = register(addr, &token, "reset-left").await;
            assert!(matches!(
                event_rx.recv().await,
                Some(lk_compat::agent::websocket::WorkerEvent::Registered { .. })
            ));
            let (mut right, ep_right, _control_right) = register(addr, &token, "reset-right").await;
            assert!(matches!(
                event_rx.recv().await,
                Some(lk_compat::agent::websocket::WorkerEvent::Registered { .. })
            ));

            #[derive(Debug, PartialEq, Eq)]
            struct Row {
                label: &'static str,
                informational: Vec<u16>,
                final_status: u16,
                before_release: bool,
                no_retry: bool,
            }

            let mut observed = Vec::new();
            let mut expected = Vec::new();
            for (label, wanted, final_status, gated, reset) in [
                ("gated-order", vec![103, 102], 200, true, false),
                ("eight", vec![103; 8], 200, false, false),
                ("nine", vec![103; 9], 502, false, false),
                ("retry-fenced", vec![103], 504, false, true),
            ] {
                let (seen_first, first_received) = tokio::sync::oneshot::channel();
                let client = async {
                    let mut socket = tokio::net::TcpStream::connect(http_addr).await.unwrap();
                    socket.write_all(format!(
                        "GET /agents/hints-worker/_/hints HTTP/1.1\r\nHost: localhost\r\nX-Request-Id: {label}\r\nOrigin: https://caller.example\r\nConnection: close\r\n\r\n"
                    ).as_bytes()).await.unwrap();
                    let mut reader = tokio::io::BufReader::new(socket);
                    let mut seen_first = Some(seen_first);
                    let mut informational = Vec::new();
                    let status = loop {
                        let mut line = String::new();
                        assert!(reader.read_line(&mut line).await.unwrap() > 0);
                        assert!(line.starts_with("HTTP/1.1 "), "{line:?}");
                        let status = line.split_whitespace().nth(1).unwrap().parse::<u16>().unwrap();
                        let mut headers = String::new();
                        loop {
                            line.clear();
                            assert!(reader.read_line(&mut line).await.unwrap() > 0);
                            if line == "\r\n" { break; }
                            headers.push_str(&line);
                        }
                        let headers = headers.to_ascii_lowercase();
                        if status < 200 {
                            informational.push(status);
                            assert!(headers.contains(&format!("x-hint-index: {}\r\n", informational.len())));
                            assert!(headers.contains("link: </theme.css>; rel=preload\r\n"));
                            assert!(!headers.contains("x-internal-hop:"));
                            assert!(!headers.contains("x-final:"));
                            if gated && informational.len() == 1 {
                                assert!(headers.contains("access-control-allow-origin: https://worker.example\r\n"));
                                assert!(headers.contains("vary: accept-encoding\r\n"));
                                assert!(!headers.contains("https://caller.example"));
                                assert!(!headers.contains("vary: origin\r\n"));
                            } else {
                                assert!(headers.contains("access-control-allow-origin: https://caller.example\r\n"));
                                assert!(headers.contains("vary: origin\r\n"));
                            }
                            assert!(headers.contains("access-control-expose-headers: *\r\n"));
                            if let Some(notify) = seen_first.take() {
                                let _ = notify.send(());
                            }
                        } else {
                            assert!(!headers.contains("x-hint-index:"));
                            assert!(!headers.contains("link:"));
                            assert!(headers.contains("access-control-allow-origin: https://caller.example\r\n"));
                            assert!(headers.contains("vary: origin\r\n"));
                            assert!(!headers.contains("https://worker.example"));
                            if status == 200 { assert!(headers.contains("x-final: yes\r\n")); }
                            break status;
                        }
                    };
                    let mut body = String::new();
                    reader.read_to_string(&mut body).await.unwrap();
                    match status {
                        200 => assert_eq!(body, "ok"),
                        502 => assert_eq!(body, "bad gateway\n"),
                        504 => assert_eq!(body, "gateway timeout\n"),
                        _ => panic!("unexpected HTTP {status}: {body:?}"),
                    }
                    (informational, status)
                };
                let worker = async {
                    let (_, mut stream) = incoming(&mut left, &mut right).await;
                    head(&mut stream, "GET", "", label).await;
                    let mut first_received = Some(first_received);
                    let mut before_release = false;
                    for (i, status) in wanted.iter().enumerate() {
                        let reason = if *status == 102 { "Processing" } else { "Early Hints" };
                        let cors = if gated && i == 0 {
                            "Access-Control-Allow-Origin: https://worker.example\r\nVary: Accept-Encoding\r\n"
                        } else {
                            ""
                        };
                        stream.send_data(format!(
                            "HTTP/1.1 {status} {reason}\r\nLink: </theme.css>; rel=preload\r\nX-Hint-Index: {}\r\nConnection: X-Internal-Hop\r\nX-Internal-Hop: omit\r\n{cors}\r\n",
                            i + 1
                        ).into()).await.unwrap();
                        if (gated || reset) && i == 0 {
                            // Release the worker even on the original broken
                            // implementation, so all four cases report actual
                            // wire rows before the shared assertions run.
                            before_release = matches!(
                                tokio::time::timeout(Duration::from_millis(700), first_received.take().unwrap()).await,
                                Ok(Ok(()))
                            );
                        }
                    }
                    if reset {
                        stream.reset(0x52e4_a40f_a8de);
                    } else if wanted.len() < 9 {
                        stream.send_data(Bytes::from_static(
                            b"HTTP/1.1 200 OK\r\nX-Final: yes\r\nContent-Length: 2\r\n\r\nok"
                        )).await.unwrap();
                        stream.finish().await.unwrap();
                    }
                    // For nine heads the worker deliberately withholds a final
                    // response, so the frontend must send its own terminal error.
                    (stream, before_release)
                };
                let ((informational, status), (_stream, before_release)) =
                    tokio::time::timeout(Duration::from_secs(3), async { tokio::join!(client, worker) })
                        .await.unwrap_or_else(|_| panic!("{label}: raw TCP/WT exchange did not complete"));
                let no_retry = if reset {
                    tokio::time::timeout(Duration::from_millis(150), incoming(&mut left, &mut right))
                        .await.is_err()
                } else { true };
                let row = Row { label, informational, final_status: status, before_release, no_retry };
                println!("RUST_ENDPOINT_INFORMATIONAL {row:?}");
                observed.push(row);
                expected.push(Row {
                    label,
                    informational: wanted,
                    final_status,
                    before_release: gated || reset,
                    no_retry: true,
                });
            }
            assert_eq!(observed, expected, "real TCP informational heads must precede the final response");
            assert!(left.session().connection().close_reason().is_none());
            assert!(right.session().connection().close_reason().is_none());
            ep_left.close(0u32.into(), b"fixture complete");
            ep_right.close(0u32.into(), b"fixture complete");
        }).await.expect("real HTTP/WT worker campaigns");
        let _ = http_stop.send(());
        wt_stop.send(true).unwrap();
        tokio::time::timeout(Duration::from_secs(5), async {
            http_server.await.unwrap();
            for task in wt_handles {
                task.await.unwrap();
            }
        })
        .await
        .expect("owned HTTP and WebTransport listeners stopped");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn failed_chunked_upload_reaches_real_worker_as_peer_gone_completion() {
        use axum::{body::Body, extract::Request, middleware::Next, response::Response};
        use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};

        async fn source_control(mut req: Request, next: Next) -> Response {
            if req.headers().contains_key("x-fixture-source-error") {
                // Control only the source failure, independently of an HTTP
                // client's full TCP disconnect. The actual node front and
                // worker still exchange every byte over their production IO.
                let reason = format!("socket lost\r\n\t\u{7f} {}", "x".repeat(300));
                let parts: [io::Result<Bytes>; 2] = [
                    Ok(Bytes::from_static(b"prefix")),
                    Err(io::Error::other(reason)),
                ];
                *req.body_mut() = Body::from_stream(futures_util::stream::iter(parts));
                req.headers_mut().remove(header::CONTENT_LENGTH);
            }
            next.run(req).await
        }

        let keys = key_store();
        let token = format!("Bearer {}", mint_agent_token(&keys, true));
        let mut state = worker_state(keys);
        let (events, mut event_rx) = mpsc::channel(64);
        state.events = events;
        let tls = build_server_tls("", "", true).unwrap();
        let wt = bind(&tls, &["127.0.0.1".to_owned()], 0).await.unwrap();
        let wt_addr = wt.bound_addrs()[0];
        let (wt_stop, wt_shutdown) = watch::channel(false);
        let wt_handles = wt.spawn(state.clone(), Duration::from_secs(30), wt_shutdown);
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let http_addr = listener.local_addr().unwrap();
        let (http_stop, http_shutdown) = tokio::sync::oneshot::channel();
        let app = lk_compat::agent::endpoint::routes(state)
            .layer(axum::middleware::from_fn(source_control));
        let http_server = tokio::spawn(async move {
            axum::serve(listener, app)
                .with_graceful_shutdown(async move {
                    let _ = http_shutdown.await;
                })
                .await
                .unwrap();
        });

        tokio::time::timeout(Duration::from_secs(12), async {
            let (mut worker, ep) = dial_insecure(wt_addr, "/agent", &[("authorization", &token)]).await.unwrap();
            let mut control = worker.session().open_bi().await.unwrap();
            let register = pb::WorkerMessage {
                message: Some(pb::worker_message::Message::Register(pb::RegisterWorkerRequest {
                    agent_name: "upload-worker".into(),
                    instance_id: "upload-instance".into(),
                    endpoint_protocol: 1,
                    endpoints: vec![pb::agent_http::AgentEndpoint {
                        path: "/upload".into(),
                        methods: vec!["POST".into()],
                        public: true,
                        ..Default::default()
                    }],
                    ..Default::default()
                })),
            }.encode_to_vec();
            let mut frame = (register.len() as u32).to_be_bytes().to_vec();
            frame.extend_from_slice(&register);
            control.send_data(frame.into()).await.unwrap();
            let response = read_one_frame(&mut control).await.expect("real worker registration");
            let response = pb::ServerMessage::decode(response.as_slice()).unwrap();
            let Some(pb::server_message::Message::Register(response)) = response.message else {
                panic!("real worker registration reply");
            };
            assert_eq!(response.endpoint_settings.unwrap().protocol, 1);
            assert!(matches!(event_rx.recv().await, Some(lk_compat::agent::websocket::WorkerEvent::Registered { .. })));

            for (case, header_and_body) in [
                ("source-error", "Content-Length: 0\r\nX-Fixture-Source-Error: yes\r\n\r\n"),
                ("clean", "Transfer-Encoding: chunked\r\n\r\n6\r\nprefix\r\n0\r\n\r\n"),
                ("forged", "Transfer-Encoding: chunked\r\nTrailer: X-Lk-Completion, X-Lk-Error, App-Trailer\r\n\r\n6\r\nprefix\r\n0\r\nX-Lk-Completion: timeout\r\nX-Lk-Error: forged\r\nApp-Trailer: ignored\r\n\r\n"),
            ] {
                let client = async {
                    let mut io = tokio::net::TcpStream::connect(http_addr).await.unwrap();
                    io.write_all(format!(
                        "POST /agents/upload-worker/_/upload HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\nX-Request-Id: {case}\r\n{header_and_body}",
                    ).as_bytes()).await.unwrap();
                    let mut output = String::new();
                    io.read_to_string(&mut output).await.unwrap();
                    assert!(output.starts_with("HTTP/1.1 200 "), "case {case}: {output}");
                    assert!(output.ends_with("\r\n\r\nok"), "case {case}: {output}");
                };
                let endpoint = async {
                    let (sid, mut stream) = worker.accept_bi().await.unwrap();
                    assert_eq!(sid, worker.session().id());
                    let mut input = Vec::new();
                    loop {
                        let bytes = stream.recv_data().await
                            .unwrap_or_else(|error| panic!("case {case}: worker lost upload framing: {error}"))
                            .unwrap_or_else(|| panic!("case {case}: worker got early EOF"));
                        input.extend_from_slice(&bytes);
                        if input.len() < 4 { continue; }
                        let size = u32::from_be_bytes(input[..4].try_into().unwrap()) as usize;
                        assert!(size <= 64 * 1024);
                        if input.len() < 4 + size { continue; }
                        let rest = &input[4 + size..];
                        let Some(head_end) = rest.windows(4).position(|bytes| bytes == b"\r\n\r\n") else { continue; };
                        let body = &rest[head_end + 4..];
                        if !body.ends_with(b"\r\n\r\n") { continue; }
                        let preamble = pb::agent_http::StreamPreamble::decode(&input[4..4+size]).unwrap();
                        assert_eq!(preamble.route, "/upload");
                        assert_eq!(preamble.request_id, case);
                        let head = std::str::from_utf8(&rest[..head_end]).unwrap().to_ascii_lowercase();
                        assert!(head.starts_with("post /upload http/1.1\r\n"));
                        assert!(head.contains("\r\ntransfer-encoding: chunked"));
                        assert!(body.starts_with(b"6\r\nprefix\r\n0\r\n"));
                        let trailers = std::str::from_utf8(&body[b"6\r\nprefix\r\n0\r\n".len()..]).unwrap();
                        if case == "source-error" {
                            let mut fields = trailers.split("\r\n").filter(|line| !line.is_empty()).map(|line| {
                                let (name, value) = line.split_once(": ").unwrap();
                                (name.to_ascii_lowercase(), value)
                            }).collect::<std::collections::BTreeMap<_,_>>();
                            assert_eq!(fields.remove("x-lk-completion"), Some("peer_gone"));
                            let reason = fields.remove("x-lk-error").expect("actual source error reason");
                            assert_eq!(reason.len(), 256);
                            assert!(reason.starts_with("socket lost     "));
                            assert!(reason.bytes().all(|byte| !byte.is_ascii_control()));
                            assert!(fields.is_empty());
                        } else {
                            assert_eq!(trailers, "\r\n", "healthy upload has no worker completion claim");
                        }
                        stream.send_data(Bytes::from_static(b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nok")).await.unwrap();
                        stream.finish().await.unwrap();
                        break;
                    }
                };
                tokio::join!(client, endpoint);
                println!("RUST_ENDPOINT_UPLOAD_COMPLETION case={case} prefix=prefix completion={}", if case=="source-error" {"peer_gone"} else {"empty"});
            }
            assert!(worker.session().connection().close_reason().is_none());
            ep.close(0u32.into(), b"fixture complete");
        }).await.expect("real HTTP and WT upload exchanges");
        let _ = http_stop.send(());
        wt_stop.send(true).unwrap();
        tokio::time::timeout(Duration::from_secs(5), async {
            http_server.await.unwrap();
            for task in wt_handles {
                task.await.unwrap();
            }
        })
        .await
        .expect("owned HTTP and WT listeners stopped");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn early_worker_head_and_reset_survive_flow_blocked_wt_preamble() {
        use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};

        // The actual worker cannot accept all of a 128-byte request ID before
        // it reads. Its WT header and prefix are at most one window apart.
        const WINDOW: usize = 64;

        async fn stage<T>(name: &str, future: impl std::future::Future<Output = T>) -> T {
            eprintln!("WT_PREAMBLE_STAGE begin={name}");
            let result = tokio::time::timeout(Duration::from_secs(3), future)
                .await
                .unwrap_or_else(|_| panic!("WT preamble fixture timed out at {name}"));
            eprintln!("WT_PREAMBLE_STAGE done={name}");
            result
        }
        let keys = key_store();
        let token = format!("Bearer {}", mint_agent_token(&keys, true));
        let mut state = worker_state(keys);
        let (events, mut event_rx) = mpsc::channel(64);
        state.events = events;
        let tls = build_server_tls("", "", true).unwrap();
        let wt = bind(&tls, &["127.0.0.1".to_owned()], 0).await.unwrap();
        let wt_addr = wt.bound_addrs()[0];
        let (wt_stop, wt_shutdown) = watch::channel(false);
        let wt_handles = wt.spawn(state.clone(), Duration::from_secs(30), wt_shutdown);
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let http_addr = listener.local_addr().unwrap();
        let (http_stop, http_shutdown) = tokio::sync::oneshot::channel();
        let app = lk_compat::agent::endpoint::routes(state);
        let http_server = tokio::spawn(async move {
            axum::serve(listener, app)
                .with_graceful_shutdown(async move {
                    let _ = http_shutdown.await;
                })
                .await
                .unwrap();
        });

        tokio::time::timeout(Duration::from_secs(12), async {
            let mut config = client_config_insecure().unwrap();
            let mut transport = quinn::TransportConfig::default();
            transport.datagram_receive_buffer_size(Some(128));
            transport.stream_receive_window((WINDOW as u32).into());
            config.transport_config(Arc::new(transport));
            let (socket, _) = bound_socket();
            let ep = client_endpoint(config, b"small-window-reset-key", socket).unwrap();
            let mut worker = stage("WT CONNECT", connect_with_headers(&ep, wt_addr, "localhost", "/agent", &[("authorization", &token)])).await.unwrap();
            let mut control = stage("WT control open", worker.session().open_bi()).await.unwrap();
            let register = pb::WorkerMessage {
                message: Some(pb::worker_message::Message::Register(pb::RegisterWorkerRequest {
                    agent_name: "preamble-worker".into(),
                    instance_id: "preamble-instance".into(),
                    endpoint_protocol: 1,
                    endpoints: vec![pb::agent_http::AgentEndpoint {
                        path: "/early".into(),
                        methods: vec!["POST".into()],
                        public: true,
                        ..Default::default()
                    }],
                    ..Default::default()
                })),
            }.encode_to_vec();
            let mut frame = (register.len() as u32).to_be_bytes().to_vec();
            frame.extend_from_slice(&register);
            stage("worker registration write", control.send_data(frame.into())).await.unwrap();
            let reply = stage("worker registration response", read_one_frame(&mut control)).await.expect("real worker registration");
            let reply = pb::ServerMessage::decode(reply.as_slice()).unwrap();
            let Some(pb::server_message::Message::Register(reply)) = reply.message else {
                panic!("real worker registration response");
            };
            assert_eq!(reply.endpoint_settings.unwrap().protocol, 1);
            assert!(matches!(stage("worker registration published", event_rx.recv()).await, Some(lk_compat::agent::websocket::WorkerEvent::Registered { .. })));

            for (label, reset, status) in [("early-final", false, 200), ("early-timeout", true, 504)] {
                eprintln!("WT_PREAMBLE_STAGE begin={label}");
                let blocked_before = worker.session().connection().stats().frame_rx.stream_data_blocked;
                let request_id = format!("{label}{}", "x".repeat(128 - label.len()));
                let client = async {
                    let mut io = tokio::net::TcpStream::connect(http_addr).await.unwrap();
                    io.write_all(format!(
                        "POST /agents/preamble-worker/_/early HTTP/1.1\r\nHost: localhost\r\nX-Request-Id: {request_id}\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
                    ).as_bytes()).await.unwrap();
                    let mut response = String::new();
                    io.read_to_string(&mut response).await.unwrap();
                    eprintln!("WT_PREAMBLE_STAGE HTTP={label} status={}", response.lines().next().unwrap_or_default());
                    assert!(response.starts_with(&format!("HTTP/1.1 {status} ")), "{label}: {response}");
                    assert!(response.ends_with(if reset { "\r\n\r\ngateway timeout\n" } else { "\r\n\r\nok" }));
                };
                let endpoint = async {
                    let (sid, mut stream) = worker.accept_bi().await.unwrap();
                    assert_eq!(sid, worker.session().id());
                    eprintln!("WT_PREAMBLE_STAGE accepted={label}");
                    let mut bytes = Vec::new();
                    while bytes.len() < 4 {
                        bytes.extend_from_slice(&stream.recv_data().await.unwrap().expect("partial preamble"));
                    }
                    let declared = u32::from_be_bytes(bytes[..4].try_into().unwrap()) as usize;
                    assert!(declared >= request_id.len());
                    eprintln!("WT_PREAMBLE_STAGE prefix={label} declared={declared} consumed={}", bytes.len());
                    assert!(bytes.len() < 4 + declared && 4 + declared > WINDOW * 2,
                        "worker has not consumed the whole preamble");
                    // The advertised window is genuinely exhausted. No other
                    // test stream writes to the worker after registration.
                    tokio::time::timeout(Duration::from_secs(2), async {
                        while worker.session().connection().stats().frame_rx.stream_data_blocked <= blocked_before {
                            tokio::time::sleep(Duration::from_millis(2)).await;
                        }
                    }).await.expect("actual STREAM_DATA_BLOCKED before worker responds");
                    eprintln!("WT_PREAMBLE_STAGE flow_blocked={label}");
                    if reset {
                        stream.reset(0x52e4_a40f_a8de); // mapped worker HSR_TIMEOUT = 3
                    } else {
                        stream.send_data(Bytes::from_static(b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nok")).await.unwrap();
                        stream.finish().await.unwrap();
                    }
                    // Own this half until the caller actually reads a terminal
                    // response, keeping request flow control blocked.
                    stream
                };
                let (_, _stream) = tokio::time::timeout(Duration::from_secs(3), async {
                    tokio::join!(client, endpoint)
                }).await.unwrap_or_else(|_| panic!("{label}: HTTP was blocked on the incomplete WT preamble"));
                println!("RUST_ENDPOINT_EARLY_PREAMBLE case={label} flow_blocked=true http={status}");
            }
            assert!(worker.session().connection().close_reason().is_none());
            ep.close(0u32.into(), b"fixture complete");
        }).await.expect("real early-response campaigns");
        let _ = http_stop.send(());
        wt_stop.send(true).unwrap();
        tokio::time::timeout(Duration::from_secs(5), async {
            http_server.await.unwrap();
            for task in wt_handles {
                task.await.unwrap();
            }
        })
        .await
        .expect("owned HTTP and WT listeners stopped");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn worker_expect_continue_respects_wt_request_head_actual() {
        use tokio::io::{AsyncBufReadExt as _, AsyncReadExt as _, AsyncWriteExt as _};

        // The actual worker cannot accept all of a 128-byte request ID before
        // it reads. Its WT header and prefix are at most one window apart.
        const WINDOW: usize = 64;

        async fn stage<T>(name: &str, future: impl std::future::Future<Output = T>) -> T {
            eprintln!("WT_EXPECT_STAGE begin={name}");
            let result = tokio::time::timeout(Duration::from_secs(3), future)
                .await
                .unwrap_or_else(|_| panic!("WT Expect fixture timed out at {name}"));
            eprintln!("WT_EXPECT_STAGE done={name}");
            result
        }

        async fn read_head(tcp: &mut tokio::io::BufReader<tokio::net::TcpStream>) -> (u16, bool) {
            let mut line = String::new();
            assert!(tcp.read_line(&mut line).await.unwrap() > 0);
            assert!(line.starts_with("HTTP/1.1 "), "{line:?}");
            let status = line
                .split_whitespace()
                .nth(1)
                .unwrap()
                .parse::<u16>()
                .unwrap();
            let mut marker = false;
            loop {
                line.clear();
                assert!(tcp.read_line(&mut line).await.unwrap() > 0);
                if line == "\r\n" {
                    break;
                }
                marker |= line.eq_ignore_ascii_case("x-worker-continue: yes\r\n");
            }
            (status, marker)
        }

        let keys = key_store();
        let token = format!("Bearer {}", mint_agent_token(&keys, true));
        let mut state = worker_state(keys);
        let (events, mut event_rx) = mpsc::channel(64);
        state.events = events;
        let tls = build_server_tls("", "", true).unwrap();
        let wt = bind(&tls, &["127.0.0.1".to_owned()], 0).await.unwrap();
        let wt_addr = wt.bound_addrs()[0];
        let (wt_stop, wt_shutdown) = watch::channel(false);
        let wt_handles = wt.spawn(state.clone(), Duration::from_secs(30), wt_shutdown);
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let http_addr = listener.local_addr().unwrap();
        let (http_stop, http_shutdown) = tokio::sync::oneshot::channel();
        let app = lk_compat::agent::endpoint::routes(state);
        let http_server = tokio::spawn(async move {
            axum::serve(listener, app)
                .with_graceful_shutdown(async move {
                    let _ = http_shutdown.await;
                })
                .await
                .unwrap();
        });

        tokio::time::timeout(Duration::from_secs(12), async {
            let mut config = client_config_insecure().unwrap();
            let mut transport = quinn::TransportConfig::default();
            transport.datagram_receive_buffer_size(Some(128));
            transport.stream_receive_window((WINDOW as u32).into());
            config.transport_config(Arc::new(transport));
            let (socket, _) = bound_socket();
            let ep = client_endpoint(config, b"expect-small-window-reset-key", socket).unwrap();
            let mut worker = stage("WT CONNECT", connect_with_headers(&ep, wt_addr, "localhost", "/agent", &[("authorization", &token)])).await.unwrap();
            let mut control = stage("WT control open", worker.session().open_bi()).await.unwrap();
            let register = pb::WorkerMessage {
                message: Some(pb::worker_message::Message::Register(pb::RegisterWorkerRequest {
                    agent_name: "expect-worker".into(),
                    instance_id: "expect-instance".into(),
                    endpoint_protocol: 1,
                    endpoints: vec![pb::agent_http::AgentEndpoint {
                        path: "/expect".into(),
                        methods: vec!["POST".into()],
                        public: true,
                        ..Default::default()
                    }],
                    ..Default::default()
                })),
            }.encode_to_vec();
            let mut frame = (register.len() as u32).to_be_bytes().to_vec();
            frame.extend_from_slice(&register);
            stage("worker registration write", control.send_data(frame.into())).await.unwrap();
            let reply = stage("worker registration response", read_one_frame(&mut control)).await.expect("real worker registration");
            let reply = pb::ServerMessage::decode(reply.as_slice()).unwrap();
            let Some(pb::server_message::Message::Register(reply)) = reply.message else {
                panic!("real worker registration response");
            };
            assert_eq!(reply.endpoint_settings.unwrap().protocol, 1);
            assert!(matches!(stage("worker registration published", event_rx.recv()).await, Some(lk_compat::agent::websocket::WorkerEvent::Registered { .. })));


            let mut observed = Vec::new();
            for (label, explicit, large) in [
                ("small-automatic", false, false),
                ("small-worker", true, false),
                ("large-automatic", false, true),
                ("large-worker", true, true),
            ] {
                let id = format!("{label}{}", "x".repeat(128-label.len()));
                let blocked_before = worker.session().connection().stats().frame_rx.stream_data_blocked;
                let mut socket = tokio::net::TcpStream::connect(http_addr).await.unwrap();
                let padding = if large {
                    format!("X-Expect-Pad: {}\r\n", "p".repeat(8192))
                } else {
                    String::new()
                };
                socket.write_all(format!(
                    "POST /agents/expect-worker/_/expect HTTP/1.1\r\nHost: localhost\r\nX-Request-Id: {id}\r\nExpect: 100-continue\r\nContent-Length: 4\r\nConnection: close\r\n{padding}\r\n"
                ).as_bytes()).await.unwrap();
                let mut tcp = tokio::io::BufReader::new(socket);
                let (sid, mut stream) = stage("worker attempt open", worker.accept_bi()).await.unwrap();
                assert_eq!(sid, worker.session().id());
                let mut received = Vec::new();
                while received.len() < 4 {
                    received.extend_from_slice(&stage("partial WT preamble", stream.recv_data()).await.unwrap().unwrap());
                }
                let declared = u32::from_be_bytes(received[..4].try_into().unwrap()) as usize;
                assert!(declared >= id.len() && declared + 4 > 2 * WINDOW);
                assert!(received.len() < declared + 4);
                stage("actual request direction STREAM_DATA_BLOCKED", async {
                    while worker.session().connection().stats().frame_rx.stream_data_blocked <= blocked_before {
                        tokio::time::sleep(Duration::from_millis(2)).await;
                    }
                }).await;

                let silent = match tokio::time::timeout(Duration::from_millis(150), tcp.fill_buf()).await {
                    Err(_) => true,
                    Ok(Ok(bytes)) => {
                        assert!(!bytes.is_empty(), "caller closed while WT request was blocked");
                        false
                    }
                    Ok(Err(error)) => panic!("caller socket: {error}"),
                };
                let mut first = None;
                if explicit {
                    stream.send_data(Bytes::from_static(
                        b"HTTP/1.1 100 Continue\r\nX-Worker-Continue: yes\r\n\r\n"
                    )).await.unwrap();
                    first = Some(stage("worker 100 before more WT request credit", read_head(&mut tcp)).await);
                }

                let head_end = stage("actual canonical request head", async {
                    loop {
                        if received.len() >= 4 + declared {
                            if let Some(end) = received[4+declared..].windows(4).position(|w| w == b"\r\n\r\n") {
                                break 4 + declared + end + 4;
                            }
                        }
                        received.extend_from_slice(&stream.recv_data().await.unwrap().expect("worker request bytes"));
                    }
                }).await;
                let preamble = pb::agent_http::StreamPreamble::decode(&received[4..4+declared]).unwrap();
                assert_eq!(preamble.request_id, id);
                assert_eq!(preamble.route, "/expect");
                let canonical = std::str::from_utf8(&received[4+declared..head_end]).unwrap().to_ascii_lowercase();
                assert!(canonical.starts_with("post /expect http/1.1\r\n"));
                assert!(canonical.contains("\r\ncontent-length: 4\r\n"));
                assert!(!canonical.contains("\r\nexpect:"));
                if large {
                    assert!(canonical.contains(&format!("\r\nx-expect-pad: {}\r\n", "p".repeat(8192))));
                }
                assert_eq!(received.len(), head_end, "caller did not send body before seeing 100");
                let first = match first {
                    Some(first) => first,
                    None => stage("automatic 100 after WT request head", read_head(&mut tcp)).await,
                };
                assert_eq!(first.0, 100);
                tcp.get_mut().write_all(b"body").await.unwrap();
                while received.len() < head_end + 4 {
                    received.extend_from_slice(&stage("real caller body at worker", stream.recv_data()).await.unwrap().unwrap());
                }
                assert_eq!(&received[head_end..], b"body");
                stream.send_data(Bytes::from_static(b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nok")).await.unwrap();
                stream.finish().await.unwrap();
                let mut continues = vec![first.1];
                loop {
                    let (status, marker) = stage("remaining caller heads", read_head(&mut tcp)).await;
                    if status == 200 {
                        break;
                    }
                    assert_eq!(status, 100, "unexpected caller status");
                    continues.push(marker);
                }
                let mut final_body = String::new();
                tcp.read_to_string(&mut final_body).await.unwrap();
                assert_eq!(final_body, "ok");
                println!("RUST_ENDPOINT_EXPECT case={label} declared={declared} flow_blocked=true silent_while_blocked={silent} worker_markers={continues:?} expect_stripped=true body=body http=200");
                observed.push((silent, continues));
            }
            assert_eq!(
                observed,
                [
                    (false, vec![false]),
                    (false, vec![false, true]),
                    (true, vec![false]),
                    (true, vec![true]),
                ],
                "pinned Go 100-continue sequence with a short buffered or large blocked worker head"
            );
            assert!(worker.session().connection().close_reason().is_none());
            ep.close(0u32.into(), b"fixture complete");
        }).await.expect("real early-response campaigns");
        let _ = http_stop.send(());
        wt_stop.send(true).unwrap();
        tokio::time::timeout(Duration::from_secs(5), async {
            http_server.await.unwrap();
            for task in wt_handles {
                task.await.unwrap();
            }
        })
        .await
        .expect("owned HTTP and WT listeners stopped");
    }

    /// Accumulate chunks off a worker control stream until one full
    /// `4-byte big-endian length + body` control frame has arrived (the client
    /// mirror of Go's `ReadControlMessage`); `None` marks an early stream end.
    async fn read_one_frame(stream: &mut WtBidiStream) -> Option<Vec<u8>> {
        let mut buf: Vec<u8> = Vec::new();
        while buf.len() < 4 {
            match stream.recv_data().await {
                Ok(Some(chunk)) => buf.extend_from_slice(&chunk),
                _ => return None,
            }
        }
        let len = usize::try_from(u32::from_be_bytes([buf[0], buf[1], buf[2], buf[3]])).ok()?;
        while buf.len() < 4 + len {
            match stream.recv_data().await {
                Ok(Some(chunk)) => buf.extend_from_slice(&chunk),
                _ => return None,
            }
        }
        Some(buf[4..4 + len].to_vec())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn agent_control_stream_register_round_trip() {
        // End-to-end over real UDP/QUIC/h3: once the control stream is accepted
        // it is split and handed to `wt_control::session`, so a length-prefixed
        // `Register` request must come back as a length-prefixed
        // `RegisterWorkerResponse` carrying the minted worker id and the
        // production-built server info. This is the integration the in-crate
        // duplex unit tests cannot cover: the live `into_split` halves over an
        // actual WebTransport stream.
        let keys = key_store();
        let token = format!("Bearer {}", mint_agent_token(&keys, true));
        let (addr, _handles, _stop) = start_dev_listener(keys, Duration::from_secs(30)).await;
        let (mut client, _ep) = dial_insecure(addr, "/agent", &[("authorization", token.as_str())])
            .await
            .expect("agent upgrade");
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
                    agent_name: "e2e-agent".into(),
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
        let reply = tokio::time::timeout(Duration::from_secs(5), read_one_frame(&mut control))
            .await
            .expect("register response within deadline")
            .expect("register response frame");
        let message = pb::ServerMessage::decode(&reply[..]).expect("decode register response");
        match message.message {
            Some(pb::server_message::Message::Register(response)) => {
                assert!(response.worker_id.starts_with("AW_"), "worker id prefix");
                let info = response.server_info.expect("server info present");
                assert_eq!(info.version, "1.13.10");
                assert_eq!(info.agent_protocol, 1);
                assert!(
                    response.endpoint_settings.is_none(),
                    "agent HTTP endpoint not negotiated"
                );
            }
            other => panic!("expected a register response, got {other:?}"),
        }
        drop(control);
    }

    /// A capsule-only close must cancel the worker even though its control
    /// stream has no EOF. Also exercise fragmented framing, a live sibling
    /// session and a new CONNECT on the same QUIC connection.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn peer_capsule_unregisters_worker_and_resets_streams_without_closing_quic() {
        let keys = key_store();
        let token = format!("Bearer {}", mint_agent_token(&keys, true));
        let mut worker = worker_state(keys);
        let (events, mut event_rx) = mpsc::channel(1);
        worker.events = events.clone();
        let registry = worker.registry.clone();
        let tls = build_server_tls("", "", true).unwrap();
        let listener = bind(&tls, &["127.0.0.1".to_owned()], 0).await.unwrap();
        let addr = listener.bound_addrs()[0];
        let (stop, shutdown) = watch::channel(false);
        let handles = listener.spawn(worker, Duration::from_secs(30), shutdown);
        let (mut client, ep) = dial_insecure(addr, "/agent", &[("authorization", &token)])
            .await
            .unwrap();
        // Begin a capsule before control acceptance. Cancellation of the
        // pre-control reader must retain its partial bytes for the next reader.
        client
            .session_mut()
            .send(Bytes::from_static(&[0x68]))
            .await
            .unwrap();
        let mut control = client.session().open_bi().await.unwrap();
        let request = pb::WorkerMessage {
            message: Some(pb::worker_message::Message::Register(
                pb::RegisterWorkerRequest {
                    r#type: 0,
                    version: "1.0.0".into(),
                    agent_name: "capsule-close".into(),
                    ..Default::default()
                },
            )),
        };
        let body = request.encode_to_vec();
        let mut frame = (body.len() as u32).to_be_bytes().to_vec();
        frame.extend_from_slice(&body);
        let frame = Bytes::from(frame);
        control.send_data(frame.clone()).await.unwrap();
        let reply = tokio::time::timeout(Duration::from_secs(5), read_one_frame(&mut control))
            .await
            .unwrap()
            .unwrap();
        let response = pb::ServerMessage::decode(&reply[..]).unwrap();
        let id = match response.message {
            Some(pb::server_message::Message::Register(response)) => response.worker_id,
            other => panic!("expected register, got {other:?}"),
        };
        assert!(registry.registration(&id).is_some());
        assert!(matches!(event_rx.recv().await,
            Some(lk_compat::agent::websocket::WorkerEvent::Registered { worker_id }) if worker_id == id));
        let mut sibling = client
            .open_session("localhost", "/agent", &[("authorization", &token)])
            .await
            .unwrap();
        let mut sibling_control = sibling.open_bi().await.unwrap();
        sibling_control.send_data(frame).await.unwrap();
        let reply =
            tokio::time::timeout(Duration::from_secs(5), read_one_frame(&mut sibling_control))
                .await
                .unwrap()
                .unwrap();
        let sibling_id = match pb::ServerMessage::decode(&reply[..]).unwrap().message {
            Some(pb::server_message::Message::Register(response)) => response.worker_id,
            other => panic!("expected sibling register, got {other:?}"),
        };
        assert_ne!(id, sibling_id);
        assert!(matches!(event_rx.recv().await,
            Some(lk_compat::agent::websocket::WorkerEvent::Registered { worker_id }) if worker_id == sibling_id));
        let (attached, wait_attached) = tokio::sync::oneshot::channel();
        let peer = ep.local_addr().unwrap();
        assert!(
            ATTACH_OBSERVERS
                .lock()
                .unwrap()
                .insert(peer, attached)
                .is_none()
        );
        let mut extra = client.session().open_bi().await.unwrap();
        extra
            .send_data(Bytes::from_static(b"endpoint-still-open"))
            .await
            .unwrap();
        // A successful local write does not prove the peer has attached this
        // stream. Observe the actual registration before testing active close.
        let attached_result = tokio::time::timeout(Duration::from_secs(5), wait_attached).await;
        ATTACH_OBSERVERS.lock().unwrap().remove(&peer);
        attached_result
            .expect("extra stream attachment deadline")
            .expect("extra stream attachment receipt");
        // Hold worker teardown after CONNECT is closed but before its route
        // is removed by the connection pump.
        events
            .send(lk_compat::agent::websocket::WorkerEvent::Registered {
                worker_id: "fixture-event-queue-barrier".into(),
            })
            .await
            .unwrap();
        // WT_CLOSE_SESSION (0x2843), payload length 4, application code 7.
        // Keep both CONNECT and control send halves open after these writes.
        client
            .session_mut()
            .send(Bytes::from_static(&[0x43, 4, 0, 0, 0, 7]))
            .await
            .unwrap();
        tokio::time::timeout(Duration::from_secs(5), async {
            while registry.registration(&id).is_some() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("peer capsule must release the worker registration");
        tokio::time::timeout(Duration::from_secs(5), async {
            for _ in 0..2 {
                let mut closing = client.session().open_bi().await.unwrap();
                closing
                    .send_data(Bytes::from_static(b"closing-session-stream"))
                    .await
                    .unwrap();
                let error = closing
                    .recv_data()
                    .await
                    .expect_err("stream on closing session must reset");
                assert!(
                    matches!(error, lk_webtransport::WtError::StreamTerminated(code)
                    if code == lk_webtransport::WT_SESSION_GONE_ERROR_CODE),
                    "{error:?}"
                );
            }
        })
        .await
        .expect("closing-session stream reset deadline");
        assert!(matches!(event_rx.recv().await,
            Some(lk_compat::agent::websocket::WorkerEvent::Registered { worker_id })
                if worker_id == "fixture-event-queue-barrier"));
        let event = tokio::time::timeout(Duration::from_secs(5), event_rx.recv())
            .await
            .expect("normal disconnect event deadline");
        assert!(matches!(event,
            Some(lk_compat::agent::websocket::WorkerEvent::Disconnected { worker_id, jobs })
                if worker_id == id && jobs.is_empty()));
        assert!(registry.registration(&sibling_id).is_some());
        let ping = pb::WorkerMessage {
            message: Some(pb::worker_message::Message::Ping(pb::WorkerPing {
                timestamp: 4201,
                ..Default::default()
            })),
        }
        .encode_to_vec();
        let mut frame = (ping.len() as u32).to_be_bytes().to_vec();
        frame.extend_from_slice(&ping);
        sibling_control.send_data(Bytes::from(frame)).await.unwrap();
        let reply =
            tokio::time::timeout(Duration::from_secs(5), read_one_frame(&mut sibling_control))
                .await
                .unwrap()
                .unwrap();
        assert!(
            matches!(pb::ServerMessage::decode(&reply[..]).unwrap().message,
            Some(pb::server_message::Message::Pong(pong)) if pong.last_timestamp == 4201)
        );
        for stream in [&mut control, &mut extra] {
            let error = tokio::time::timeout(Duration::from_secs(5), stream.recv_data())
                .await
                .expect("session stream reset deadline")
                .expect_err("session stream must reset");
            assert!(
                matches!(error, lk_webtransport::WtError::StreamTerminated(code)
                if code == lk_webtransport::WT_SESSION_GONE_ERROR_CODE),
                "{error:?}"
            );
        }
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                let mut late = client.session().open_bi().await.unwrap();
                late.send_data(Bytes::from_static(b"late-closed-session-stream"))
                    .await
                    .unwrap();
                let error = late.recv_data().await.expect_err("late stream must reset");
                match error {
                    lk_webtransport::WtError::StreamTerminated(
                        lk_webtransport::WT_BUFFERED_STREAM_REJECTED_ERROR_CODE,
                    ) => break,
                    // The disconnect notification can finish before the
                    // connection pump consumes the route retirement event.
                    lk_webtransport::WtError::StreamTerminated(
                        lk_webtransport::WT_SESSION_GONE_ERROR_CODE,
                    ) => continue,
                    other => panic!("unexpected late stream error: {other:?}"),
                }
            }
        })
        .await
        .expect("session route retirement deadline");
        let conn = client.session().connection().clone();
        assert!(
            conn.close_reason().is_none(),
            "session close must keep QUIC alive"
        );
        let mut next = tokio::time::timeout(
            Duration::from_secs(5),
            client.open_session("localhost", "/agent", &[("authorization", &token)]),
        )
        .await
        .expect("second CONNECT deadline")
        .unwrap();
        assert_ne!(next.flow_id(), 0);
        assert_eq!(
            next.id(),
            SessionId::try_from(next.flow_id() * 4).unwrap(),
            "WT stream headers use the full CONNECT ID; datagrams use the quarter ID"
        );
        let mut next_control = next.open_bi().await.unwrap();
        let body = pb::WorkerMessage {
            message: Some(pb::worker_message::Message::Register(
                pb::RegisterWorkerRequest {
                    agent_name: "after-capsule-close".into(),
                    ..Default::default()
                },
            )),
        }
        .encode_to_vec();
        let mut frame = (body.len() as u32).to_be_bytes().to_vec();
        frame.extend_from_slice(&body);
        next_control.send_data(Bytes::from(frame)).await.unwrap();
        let reply = tokio::time::timeout(Duration::from_secs(5), read_one_frame(&mut next_control))
            .await
            .unwrap()
            .unwrap();
        assert!(matches!(
            pb::ServerMessage::decode(&reply[..]).unwrap().message,
            Some(pb::server_message::Message::Register(_))
        ));
        next.close_with_error(0, "").await.unwrap();
        sibling.close_with_error(0, "").await.unwrap();
        stop.send(true).unwrap();
        ep.close(quinn::VarInt::from_u32(0), b"test complete");
        for handle in handles {
            handle.await.unwrap();
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn connect_agent_with_valid_grant_upgrades() {
        let keys = key_store();
        let token = format!("Bearer {}", mint_agent_token(&keys, true));
        // A generous control timeout so the upgrade path is exercised, not the
        // no-control-stream teardown.
        let (addr, _handles, _stop) = start_dev_listener(keys, Duration::from_secs(30)).await;
        let (client, _ep) = dial_insecure(addr, "/agent", &[("authorization", token.as_str())])
            .await
            .expect("agent CONNECT upgrades with a valid agent grant");
        // The worker's control stream is accepted by the pump.
        let control = client
            .session()
            .open_bi()
            .await
            .expect("open control stream");
        drop(control);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn no_token_agent_route_is_401() {
        let keys = key_store();
        let (addr, _handles, _stop) = start_dev_listener(keys, Duration::from_secs(5)).await;
        let err = match dial_insecure(addr, "/agent", &[]).await {
            Err(e) => e,
            Ok((_c, ep)) => {
                drop(ep);
                panic!("no token must not upgrade")
            }
        };
        assert!(
            err.to_string().contains("status 401"),
            "expected 401, got {err}"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn non_agent_grant_is_401() {
        let keys = key_store();
        let token = format!("Bearer {}", mint_agent_token(&keys, false));
        let (addr, _handles, _stop) = start_dev_listener(keys, Duration::from_secs(5)).await;
        let err = match dial_insecure(addr, "/agent", &[("authorization", token.as_str())]).await {
            Err(e) => e,
            Ok((_c, ep)) => {
                drop(ep);
                panic!("a grant without Video.Agent must not upgrade")
            }
        };
        assert!(
            err.to_string().contains("status 401"),
            "expected 401, got {err}"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn malformed_authorization_header_is_401() {
        let keys = key_store();
        let (addr, _handles, _stop) = start_dev_listener(keys, Duration::from_secs(5)).await;
        let err = match dial_insecure(addr, "/agent", &[("authorization", "Token notbearer")]).await
        {
            Err(e) => e,
            Ok((_c, ep)) => {
                drop(ep);
                panic!("a non-Bearer header is rejected before routing")
            }
        };
        assert!(
            err.to_string().contains("status 401"),
            "expected 401, got {err}"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn unknown_path_is_404() {
        let keys = key_store();
        let token = format!("Bearer {}", mint_agent_token(&keys, true));
        let (addr, _handles, _stop) = start_dev_listener(keys, Duration::from_secs(5)).await;
        let err = match dial_insecure(addr, "/wtp/rtc", &[("authorization", token.as_str())]).await
        {
            Err(e) => e,
            Ok((_c, ep)) => {
                drop(ep);
                panic!("only /agent is routed on the WebTransport listener")
            }
        };
        assert!(
            err.to_string().contains("status 404"),
            "expected 404, got {err}"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn missing_control_stream_closes_the_session_with_a_capsule() {
        let keys = key_store();
        let token = format!("Bearer {}", mint_agent_token(&keys, true));
        // Short bounded wait so the no-control-stream teardown is observable.
        let (addr, _handles, _stop) = start_dev_listener(keys, Duration::from_millis(300)).await;
        let (mut client, _ep) = dial_insecure(addr, "/agent", &[("authorization", token.as_str())])
            .await
            .expect("upgrade succeeds before the control-stream deadline");
        // Never open a control stream: the server ends the session with Go's
        // reason, carried by a WT_CLOSE_SESSION capsule on the CONNECT stream
        // (the QUIC connection itself stays open, like Go's CloseWithError).
        let closed = tokio::time::timeout(
            Duration::from_secs(5),
            client.session_mut().read_close_capsule(),
        )
        .await
        .expect("session closes after the control timeout")
        .expect("capsule stream readable");
        assert_eq!(
            closed,
            Some((
                lk_webtransport::SESSION_CLOSE_OK,
                "no control stream".to_string()
            ))
        );
    }

    #[test]
    fn tls_requires_files_when_not_dev() {
        let err = match build_server_tls("", "", false) {
            Err(e) => e,
            Ok(_) => panic!("missing cert pair is fatal in non-dev mode"),
        };
        assert_eq!(
            err.to_string(),
            "webtransport requires webtransport.tls_cert_file/tls_key_file"
        );
    }

    #[test]
    fn tls_reports_missing_cert_files_when_configured() {
        let err = match build_server_tls(
            "/nonexistent/livekit-wt-cert.pem",
            "/nonexistent/livekit-wt-key.pem",
            false,
        ) {
            Err(e) => e,
            Ok(_) => panic!("an unreadable cert file is fatal"),
        };
        assert!(err.to_string().contains("failed to load certificate"));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn cert_file_branch_handshakes_with_trusted_root() {
        // Load the pinned RSA-root -> intermediate -> P-256 `localhost` leaf
        // through the file branch, then complete a real (verified) handshake.
        const ROOT: &[u8] = include_bytes!("../../rtc-tls/tests/fixtures/expanded/root-rsa.pem");
        const LEAF: &[u8] = include_bytes!("../../rtc-tls/tests/fixtures/expanded/p256.pem");
        const KEY: &[u8] = include_bytes!("../../rtc-tls/tests/fixtures/expanded/p256-key.pem");

        let dir = std::env::temp_dir();
        let cert_path = dir.join("livekit-wt-accept-leaf.pem");
        let key_path = dir.join("livekit-wt-accept-key.pem");
        std::fs::write(&cert_path, LEAF).expect("write cert");
        std::fs::write(&key_path, KEY).expect("write key");

        let keys = key_store();
        let tls = build_server_tls(
            cert_path.to_str().unwrap(),
            key_path.to_str().unwrap(),
            false,
        )
        .expect("cert files load in non-dev mode");
        let listener = bind(&tls, &["127.0.0.1".to_owned()], 0)
            .await
            .expect("bind");
        let addr = listener.bound_addrs()[0];
        let (stop_tx, stop_rx) = watch::channel(false);
        let _handles = listener.spawn(worker_state(keys.clone()), Duration::from_secs(30), stop_rx);

        let token = format!("Bearer {}", mint_agent_token(&keys, true));
        let mut roots = rtc_tls::rustls::RootCertStore::empty();
        for c in rtc_tls::rustls::pki_types::CertificateDer::pem_slice_iter(ROOT) {
            roots.add(c.expect("root")).expect("anchor");
        }
        let cfg = client_config(roots).expect("trusted client config");
        let (socket, _) = bound_socket();
        let ep = client_endpoint(cfg, b"client-reset-key", socket).expect("endpoint");
        let client = connect_with_headers(
            &ep,
            addr,
            "localhost",
            "/agent",
            &[("authorization", token.as_str())],
        )
        .await
        .expect("verified handshake against the pinned root");
        drop(client);
        drop(stop_tx);

        let _ = std::fs::remove_file(&cert_path);
        let _ = std::fs::remove_file(&key_path);
    }
}
