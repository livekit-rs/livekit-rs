// Server-side WebTransport wiring: pure-Rust crypto -> Quinn async endpoint ->
// h3/HTTP/3 -> WebTransport sessions, mirroring the upstream Go contract
// (`pkg/service/webtransport.go` + `quic-go/webtransport-go` v0.12.0).
//
// Like the Go listener, one UDP socket is bound per address with datagrams
// enabled and the `h3` ALPN; every QUIC connection carries an h3 server
// connection whose SETTINGS advertise `enable_webtransport`,
// `enable_extended_connect`, `enable_datagram` and
// `max_webtransport_sessions`. MIXED ROUTING NOTE: client-initiated bidi
// streams carry either an HTTP/3 request (CONNECT or plain) or a WebTransport
// stream header (`0x41 || session`), so a single accept pump
// (`poll_accept_request_stream` + first-frame peek) classifies them, exactly
// like the official `h3-webtransport` crate: WT headers yield the whole raw
// stream, anything else is handed back to h3's request resolver.
// Unidirectional WT streams are demuxed by h3's `0x54` handling into
// `accepted_streams_mut().wt_uni_streams`.

use std::collections::{HashMap, VecDeque};
use std::future::poll_fn;
use std::sync::Arc;
use std::task::Poll;
use std::time::Duration;

use bytes::Bytes;
use h3::frame::FrameStream;
use h3::proto::frame::Frame;
use h3::stream::BufRecvStream;
use h3::webtransport::SessionId;
use rtc_tls::quinn::{QuinnServerConfig, ResetKey, TokenKey};
use rustls_pki_types::{CertificateDer, PrivateKeyDer};
use tokio::task::JoinSet;
use tokio::time::Instant;

use crate::session::{
    H3Conn, Result, ServerConnect, WtBidiStream, WtError, WtRecvStream, WtSession,
    decode_quic_varint,
};

/// ALPN advertised by the WebTransport listener (Go: `http3.NextProtoH3`).
pub const H3_ALPN: &[u8] = b"h3";

/// `SETTINGS_MAX_FIELD_SECTION_SIZE` the Go listener advertises: quic-go takes it
/// from `http3.Server.maxHeaderBytes()`, which falls back to Go's
/// `http.DefaultMaxHeaderBytes` because LiveKit never sets `MaxHeaderBytes`.
const GO_MAX_HEADER_BYTES: u64 = 1 << 20;
/// The largest value a QUIC variable integer can carry (`2**62 - 1`), which is
/// what `ConfigureHTTP3Server` puts in `SETTINGS_WT_MAX_SESSIONS`.
const WT_MAX_SESSIONS: u64 = (1 << 62) - 1;

/// `H3_DATAGRAM_ERROR` (RFC 9114 §8.1): the connection error a peer raises
/// when a datagram cannot be parsed as an HTTP datagram.
const H3_DATAGRAM_ERROR: u32 = 0x33;

/// `maxQuarterStreamID` (quic-go `http3/conn.go`): the largest quarter stream
/// ID a datagram may carry.
const MAX_QUARTER_STREAM_ID: u64 = (1 << 60) - 1;

/// Raw QUIC stream code used to reject a stream for a recently closed or
/// expired WebTransport session (webtransport-go v0.12.0 errors.go).
pub const WT_BUFFERED_STREAM_REJECTED_ERROR_CODE: u64 = 0x3994_bd84;

const RECENTLY_CLOSED_SESSIONS: usize = 16;
const STREAM_REORDERING_TIMEOUT: Duration = Duration::from_secs(5);

/// TLS material plus transport knobs for a WebTransport listener.
pub struct ServerTls {
    /// Certificate chain presented to clients.
    pub cert_chain: Vec<CertificateDer<'static>>,
    /// Leaf private key.
    pub key: PrivateKeyDer<'static>,
    /// Datagram receive queue depth; `Some` advertises `max_datagram_frame_size`
    /// (Go: `quicConf.EnableDatagrams = true`).
    pub datagram_queue: Option<usize>,
    /// Whether PLPMTUD discovery runs (Quinn default: enabled).
    pub mtu_discovery: bool,
}

impl ServerTls {
    /// Default server knobs matching the Go listener.
    #[must_use]
    pub fn new(cert_chain: Vec<CertificateDer<'static>>, key: PrivateKeyDer<'static>) -> Self {
        Self {
            cert_chain,
            key,
            datagram_queue: Some(128),
            mtu_discovery: true,
        }
    }
}

/// Build the Quinn `ServerConfig` over the `rtc-tls` pure-Rust QUIC crypto
/// adapter (custom provider, no ring/aws-lc).
///
/// # Errors
/// Returns a rustls error if the certificate/key pair is invalid.
pub fn server_config(tls: &ServerTls, token_master: &[u8]) -> Result<quinn::ServerConfig> {
    let crypto = Arc::new(
        QuinnServerConfig::new(
            tls.cert_chain.clone(),
            tls.key.clone_key(),
            vec![H3_ALPN.to_vec()],
        )
        .map_err(|e| WtError::Protocol(format!("server crypto config: {e}")))?
        // webtransport-go@v0.12.0/server.go forces
        // `quicConf.EnableStreamResetPartialDelivery = true`, and its client won't
        // dial a server that omits the parameter, so the parity knob is not
        // configurable here.
        .with_reset_stream_at(),
    );
    let mut config = quinn::ServerConfig::new(crypto, Arc::new(TokenKey::new(token_master)));
    let mut transport = quinn::TransportConfig::default();
    transport.datagram_receive_buffer_size(tls.datagram_queue);
    if !tls.mtu_discovery {
        transport.mtu_discovery_config(None);
    }
    config.transport_config(Arc::new(transport));
    Ok(config)
}

/// Bind a server endpoint on `socket`, mirroring
/// `quic.ListenEarly(udp, tlsConf, quicConf)` for our accept-only use.
///
/// # Errors
/// Propagates socket/IO errors from the Quinn endpoint construction.
pub fn endpoint(
    config: quinn::ServerConfig,
    reset_key: &[u8],
    socket: std::net::UdpSocket,
) -> std::io::Result<quinn::Endpoint> {
    let endpoint_config = quinn::EndpointConfig::new(Arc::new(ResetKey::new(reset_key)));
    quinn::Endpoint::new(
        endpoint_config,
        Some(config),
        socket,
        Arc::new(quinn::TokioRuntime),
    )
}

/// One accepted item from the mixed request/WT bidi accept pump.
pub enum Accepted {
    /// Client-initiated WebTransport stream belonging to a known session.
    Bidi(SessionId, WtBidiStream),
    /// Plain HTTP/3 request (typically the CONNECT opening a session); the
    /// caller answers or rejects it on `ServerConnect`. Boxed because h3
    /// request streams carry the whole framing state machine.
    Request(http::Request<()>, Box<ServerConnect>),
}

enum Incoming<Bidi, Uni> {
    Request(Bidi),
    Uni(Uni),
}

struct PendingSessionStreams {
    deadline: Instant,
    streams: Vec<WtBidiStream>,
    uni: Vec<WtRecvStream>,
}

/// One h3 server connection plus WebTransport session routing.
pub struct WtServerConnection {
    conn: quinn::Connection,
    h3: h3::server::Connection<H3Conn, Bytes>,
    sessions: Vec<SessionId>,
    recently_closed: VecDeque<SessionId>,
    pending: HashMap<SessionId, PendingSessionStreams>,
    ready: VecDeque<(SessionId, WtBidiStream)>,
    classifying: JoinSet<Result<Option<Accepted>>>,
    bidi_done: bool,
    route_session_uni: bool,
    owned_uni: HashMap<SessionId, Vec<WtRecvStream>>,
}

impl WtServerConnection {
    /// Handshake the h3 server connection over an accepted QUIC connection,
    /// advertising the WebTransport settings block (Go:
    /// `ConfigureHTTP3Server`; h3 0.0.8 emits the draft-15 setting IDs).
    ///
    /// # Errors
    /// Propagates h3 connection (SETTINGS/stream) establishment errors.
    pub async fn new(conn: quinn::Connection) -> Result<Self> {
        // `ConfigureHTTP3Server` (webtransport-go@v0.12.0/server.go) plus quic-go's
        // `settingsFrame` produce exactly this block on the Go listener: the draft-06
        // and final WebTransport enable settings, `SETTINGS_WT_MAX_SESSIONS` at the
        // variable-integer maximum, `SETTINGS_MAX_FIELD_SECTION_SIZE` at Go's
        // `http.DefaultMaxHeaderBytes` (`MaxHeaderBytes` is never set upstream),
        // Extended CONNECT and HTTP/3 datagrams. quic-go sends no reserved/grease
        // setting, so neither do we.
        let h3 = h3::server::builder()
            .max_field_section_size(GO_MAX_HEADER_BYTES)
            .enable_webtransport(true)
            .enable_extended_connect(true)
            .enable_datagram(true)
            .max_webtransport_sessions(WT_MAX_SESSIONS)
            .send_grease(false)
            .build(h3_quinn::Connection::new(conn.clone()))
            .await?;
        Ok(Self {
            conn,
            h3,
            sessions: Vec::new(),
            recently_closed: VecDeque::new(),
            pending: HashMap::new(),
            ready: VecDeque::new(),
            classifying: JoinSet::new(),
            bidi_done: false,
            route_session_uni: false,
            owned_uni: HashMap::new(),
        })
    }

    /// The underlying QUIC connection.
    #[must_use]
    pub fn connection(&self) -> &quinn::Connection {
        &self.conn
    }

    /// Let the connection own incoming uni streams for an Agent service that
    /// has no uni consumer. Applications that read uni use `accept_uni` instead.
    #[must_use]
    pub fn with_session_uni_routing(mut self) -> Self {
        self.route_session_uni = true;
        self
    }

    /// Accept a client-initiated HTTP/3 request or WebTransport bidi stream.
    ///
    /// A WT stream for a not-yet-established session waits for CONNECT for up
    /// to five seconds after its most recent arrival. A recently closed
    /// session rejects new streams immediately. Stream classification tasks
    /// are connection-owned, so cancellation by another input cannot discard
    /// a partially received header or block a later complete CONNECT.
    ///
    /// # Errors
    /// Propagates h3/QUIC connection errors and request-resolution errors.
    pub async fn accept(&mut self) -> Result<Option<Accepted>> {
        loop {
            if let Some((session, stream)) = self.ready.pop_front() {
                return Ok(Some(Accepted::Bidi(session, stream)));
            }
            if self.bidi_done && self.classifying.is_empty() {
                return Ok(None);
            }
            let expiry = self.pending.values().map(|entry| entry.deadline).min();
            tokio::select! {
                _ = async {
                    if let Some(deadline) = expiry {
                        tokio::time::sleep_until(deadline).await;
                    }
                }, if expiry.is_some() => self.expire_pending(),
                parsed = self.classifying.join_next(), if !self.classifying.is_empty() => {
                    let parsed = parsed.expect("nonempty stream tasks")
                        .map_err(|error| WtError::Protocol(format!("classify WT stream: {error}")))??;
                    match parsed {
                        Some(Accepted::Bidi(session, stream)) => {
                            self.validate_wire_session_id(session)?;
                            if self.has_session(session) {
                                return Ok(Some(Accepted::Bidi(session, stream)));
                            }
                            self.buffer_or_reject(session, stream);
                        }
                        Some(request) => return Ok(Some(request)),
                        None => {}
                    }
                }
                ingress = poll_fn(|cx| {
                    if self.route_session_uni {
                        self.h3.inner.poll_accept_recv(cx)?;
                        if let Some((id, stream)) =
                            self.h3.inner.accepted_streams_mut().wt_uni_streams.pop()
                        {
                            return Poll::Ready(Ok(Incoming::Uni((id, stream))));
                        }
                    }
                    self.h3.poll_accept_request_stream(cx).map(|r| r.map(Incoming::Request))
                }), if !self.bidi_done => {
                    match ingress? {
                        Incoming::Uni((session, stream)) => {
                            self.validate_wire_session_id(session)?;
                            self.route_uni(session, WtRecvStream::from_raw(stream));
                        }
                        Incoming::Request(Some(bidi)) => {
                            let resolver = self.h3.create_resolver(FrameStream::new(BufRecvStream::new(bidi)));
                            self.classifying.spawn(classify_bidi(resolver));
                        }
                        Incoming::Request(None) => self.bidi_done = true,
                    }
                }
            }
        }
    }

    fn validate_wire_session_id(&self, session: SessionId) -> Result<()> {
        if session.is_client_initiated_bidi() {
            return Ok(());
        }
        // Fixed webtransport-go closes the QUIC connection with H3_ID_ERROR
        // for a stream ID that cannot identify a client CONNECT.
        self.conn.close(quinn::VarInt::from_u32(0x108), b"");
        Err(WtError::Protocol(
            "invalid WebTransport CONNECT stream ID".into(),
        ))
    }

    fn buffer_or_reject(&mut self, session: SessionId, stream: WtBidiStream) {
        if self.recently_closed.contains(&session) {
            reject_bidi(stream, WT_BUFFERED_STREAM_REJECTED_ERROR_CODE);
            return;
        }
        self.pending_entry(session).streams.push(stream);
    }

    fn route_uni(&mut self, session: SessionId, mut stream: WtRecvStream) {
        if self.has_session(session) {
            self.owned_uni.entry(session).or_default().push(stream);
        } else if self.recently_closed.contains(&session) {
            stream.stop_sending(WT_BUFFERED_STREAM_REJECTED_ERROR_CODE);
        } else {
            self.pending_entry(session).uni.push(stream);
        }
    }

    fn pending_entry(&mut self, session: SessionId) -> &mut PendingSessionStreams {
        let deadline = Instant::now() + STREAM_REORDERING_TIMEOUT;
        let entry = self
            .pending
            .entry(session)
            .or_insert_with(|| PendingSessionStreams {
                deadline,
                streams: Vec::new(),
                uni: Vec::new(),
            });
        entry.deadline = deadline;
        entry
    }

    fn expire_pending(&mut self) {
        let now = Instant::now();
        self.pending.retain(|_, entry| {
            if entry.deadline > now {
                return true;
            }
            for stream in entry.streams.drain(..) {
                reject_bidi(stream, WT_BUFFERED_STREAM_REJECTED_ERROR_CODE);
            }
            for mut stream in entry.uni.drain(..) {
                stream.stop_sending(WT_BUFFERED_STREAM_REJECTED_ERROR_CODE);
            }
            false
        });
    }

    /// Drive h3 stream demuxing and take the next unidirectional WT stream,
    /// whose `0x54 || session` header was already consumed internally.
    ///
    /// # Errors
    /// Propagates h3 connection errors from the receive pump.
    pub async fn accept_uni(&mut self) -> Result<Option<(SessionId, WtRecvStream)>> {
        poll_fn(|cx| {
            self.h3.inner.poll_accept_recv(cx)?;
            if let Some((id, stream)) = self.h3.inner.accepted_streams_mut().wt_uni_streams.pop() {
                return Poll::Ready(Ok(Some((id, WtRecvStream::from_raw(stream)))));
            }
            Poll::Pending
        })
        .await
    }

    /// Register a session ID (after a successful upgrade) so its streams route.
    pub fn add_session(&mut self, session: SessionId) {
        if self.has_session(session) {
            return;
        }
        self.sessions.push(session);
        if let Some(pending) = self.pending.remove(&session) {
            self.ready
                .extend(pending.streams.into_iter().map(|stream| (session, stream)));
            if !pending.uni.is_empty() {
                self.owned_uni.insert(session, pending.uni);
            }
        }
    }

    /// Forget a session (e.g. after its CONNECT stream closes) so later
    /// streams for it are refused.
    pub fn drop_session(&mut self, session: SessionId) {
        let Some(position) = self.sessions.iter().position(|id| *id == session) else {
            return;
        };
        self.sessions.swap_remove(position);
        if let Some(streams) = self.owned_uni.remove(&session) {
            for mut stream in streams {
                stream.stop_sending(crate::session::WT_SESSION_GONE_ERROR_CODE);
            }
        }
        if self.recently_closed.len() == RECENTLY_CLOSED_SESSIONS {
            self.recently_closed.pop_front();
        }
        self.recently_closed.push_back(session);
        let mut remaining = VecDeque::new();
        for (id, stream) in self.ready.drain(..) {
            if id == session {
                reject_bidi(stream, crate::session::WT_SESSION_GONE_ERROR_CODE);
            } else {
                remaining.push_back((id, stream));
            }
        }
        self.ready = remaining;
    }

    /// Whether `session` was upgraded on this connection.
    #[must_use]
    pub fn has_session(&self, session: SessionId) -> bool {
        self.sessions.contains(&session)
    }
}

async fn classify_bidi(
    mut resolver: h3::server::RequestResolver<H3Conn, Bytes>,
) -> Result<Option<Accepted>> {
    let frame = poll_fn(|cx| resolver.frame_stream.poll_next(cx)).await;
    match frame {
        // Terminating a single bidi stream does not end the connection.
        Err(h3::frame::FrameStreamError::Quic(
            h3::quic::StreamErrorIncoming::StreamTerminated { .. }
            | h3::quic::StreamErrorIncoming::Unknown(_),
        )) => Ok(None),
        Ok(Some(Frame::WebTransportStream(session))) => {
            let raw = resolver.frame_stream.into_inner();
            Ok(Some(Accepted::Bidi(session, WtBidiStream::from_raw(raw))))
        }
        other => {
            let (request, connect) = resolver.accept_with_frame(other)?.resolve().await?;
            Ok(Some(Accepted::Request(request, Box::new(connect))))
        }
    }
}

fn reject_bidi(mut stream: WtBidiStream, code: u64) {
    stream.stop_sending(code);
    stream.reset(code);
}

/// Read the next QUIC datagram and decode its RFC 9297 quarter-stream-ID
/// prefix, returning the session's quarter stream ID and the payload.
///
/// A datagram whose prefix cannot be parsed, or whose quarter stream ID is out
/// of range, closes the connection with `H3_DATAGRAM_ERROR`, exactly like
/// quic-go's `rawConn.receiveDatagrams`.
///
/// # Errors
/// Propagates QUIC connection errors and reports a malformed datagram.
pub async fn accept_datagram(conn: &quinn::Connection) -> Result<Option<(u64, Bytes)>> {
    let raw = match conn.read_datagram().await {
        Ok(raw) => raw,
        Err(e) => return Err(WtError::Quic(e.to_string())),
    };
    let Some((quarter_stream_id, payload)) = decode_quic_varint(raw) else {
        conn.close(quinn::VarInt::from_u32(H3_DATAGRAM_ERROR), b"");
        return Err(WtError::Protocol(
            "could not read quarter stream id from datagram".into(),
        ));
    };
    if quarter_stream_id > MAX_QUARTER_STREAM_ID {
        conn.close(quinn::VarInt::from_u32(H3_DATAGRAM_ERROR), b"");
        return Err(WtError::Protocol(format!(
            "invalid quarter stream id {quarter_stream_id}"
        )));
    }
    Ok(Some((quarter_stream_id, payload)))
}

/// Upgrade an Extended CONNECT request into a WebTransport session.
///
/// Validates `:method=CONNECT` with a WebTransport `:protocol` token, answers
/// `200 OK` and keeps the whole request stream as the session channel,
/// registering the session ID on `conn` for stream routing.
///
/// The reply carries no extra headers: the official v1.13.10 listener
/// (`webtransport-go` v0.12.0 `server.go`) only writes `200 OK`, and it never
/// answers `WT-Protocol` either, because LiveKit leaves `ApplicationProtocols`
/// empty (`pkg/service/webtransport.go: NewWebTransportServer`), which makes
/// `selectProtocol` always return the empty string. Emitting the old
/// `sec-webtransport-http3-draft: draft02` echo would diverge from that wire
/// contract while this listener speaks the final (draft-15) protocol.
///
/// # Errors
/// Returns a protocol error for non-CONNECT/non-webtransport requests, which
/// the caller should reject with `4xx` per the contract.
pub async fn upgrade_connect(
    conn: &mut WtServerConnection,
    req: http::Request<()>,
    mut stream: ServerConnect,
) -> Result<WtSession> {
    if req.method() != http::Method::CONNECT {
        return Err(WtError::Protocol(format!(
            "expected CONNECT request, got {}",
            req.method()
        )));
    }
    if !is_webtransport_protocol(&req) {
        return Err(WtError::Protocol(
            "expected :protocol=webtransport or webtransport-h3".into(),
        ));
    }
    let response = http::Response::builder()
        .status(http::StatusCode::OK)
        .body(())
        .expect("status-only response");
    stream.send_response(response).await?;
    let session = WtSession::from_server(conn.connection().clone(), stream);
    conn.add_session(session.id());
    Ok(session)
}

/// Whether `req` carries a WebTransport `:protocol` token.
///
/// draft-ietf-webtrans-http3-15 registered `webtransport-h3` as the value and
/// `webtransport-go` v0.12.0 sends exactly that (`protocol.go: protocolHeader`),
/// while its server still accepts the older `webtransport` token
/// (`isWebTransportProtocol`). Both therefore denote a session request here.
fn is_webtransport_protocol(req: &http::Request<()>) -> bool {
    req.extensions()
        .get::<h3::ext::Protocol>()
        .is_some_and(h3::ext::Protocol::is_webtransport)
}

/// Whether `req` is an Extended CONNECT asking for the WebTransport protocol
/// (the shape [`upgrade_connect`] accepts). Lets a router decide up front
/// whether to attempt an upgrade or answer like the Go handler does when the
/// request reaches an upgrade route but is not a WebTransport CONNECT.
#[must_use]
pub fn is_webtransport_connect(req: &http::Request<()>) -> bool {
    req.method() == http::Method::CONNECT && is_webtransport_protocol(req)
}

/// Reject a request with `status` and close its stream.
///
/// # Errors
/// Propagates stream errors.
pub async fn reject(stream: &mut ServerConnect, status: http::StatusCode) -> Result<()> {
    stream
        .send_response(
            http::Response::builder()
                .status(status)
                .body(())
                .expect("status-only response"),
        )
        .await
        .map_err(WtError::Stream)?;
    stream.finish().await.map_err(Into::into)
}

/// Reject a request with a full response (status + headers) and a DATA body,
/// mirroring Go's `HandleError` (status line plus `err.Error()` bytes).
///
/// # Errors
/// Propagates stream errors.
pub async fn reject_with_body(
    stream: &mut ServerConnect,
    response: http::Response<()>,
    body: Bytes,
) -> Result<()> {
    stream.send_response(response).await?;
    stream.send_data(body).await?;
    stream.finish().await.map_err(Into::into)
}

/// Convenience: bind + accept QUIC connections and run `handler` per
/// connection until the endpoint closes.
///
/// # Errors
/// Propagates endpoint construction/handshake errors.
pub async fn serve<F, Fut>(
    socket: std::net::UdpSocket,
    config: quinn::ServerConfig,
    reset_key: &[u8],
    handler: F,
) -> Result<()>
where
    F: Fn(WtServerConnection) -> Fut + Send + 'static,
    Fut: std::future::Future<Output = ()> + Send + 'static,
{
    let ep =
        endpoint(config, reset_key, socket).map_err(|e| WtError::Protocol(format!("bind: {e}")))?;
    while let Some(incoming) = ep.accept().await {
        match incoming.await {
            Ok(conn) => match WtServerConnection::new(conn).await {
                Ok(server) => {
                    tokio::spawn(handler(server));
                }
                Err(e) => {
                    eprintln!("webtransport: h3 handshake failed: {e}");
                }
            },
            Err(e) => {
                eprintln!("webtransport: quic handshake failed: {e}");
            }
        }
    }
    Ok(())
}
