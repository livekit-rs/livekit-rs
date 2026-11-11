// WebTransport session layer shared by the server and the client.
//
// Wire contract (verified against `webtransport-go` and the official
// `h3-webtransport` crate): a WebTransport stream carries a bare varint
// header (`0x41 || session id` bidirectional, `0x54 || session id`
// unidirectional) and everything after the header is raw stream payload —
// *no* HTTP/3 DATA framing. So payload transfer goes through the raw
// `quic::SendStreamUnframed`/`quic::RecvStream` surfaces of
// `h3::stream::BufRecvStream` over whole h3-quinn streams (never `split`,
// whose halves are framed request wrappers). The session itself owns the
// CONNECT request stream for both directions.

use std::future::poll_fn;
use std::io;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};

use bytes::{Buf, Bytes, BytesMut};
use h3::error::Code;
use h3::quic::{
    self, OpenStreams as _, RecvStream as _, SendStream as _, SendStreamUnframed as _, WriteBuf,
};
use h3::stream::{BidiStreamHeader, BufRecvStream, UniStreamHeader};
use h3::webtransport::SessionId;

/// h3's Quinn binding (connection adapter over `quinn::Connection`).
pub type H3Conn = h3_quinn::Connection;
/// Client/server-initiated bidirectional QUIC stream in raw byte form.
pub type H3Bidi = h3_quinn::BidiStream<Bytes>;
/// Unidirectional send half in raw byte form.
pub type H3Send = h3_quinn::SendStream<Bytes>;
/// Unidirectional receive half.
pub type H3Recv = h3_quinn::RecvStream;
/// Whole bidi stream with buffering (`BufRecvStream` implements the raw
/// unframed send plus framed receive traits over one owned stream).
pub type BufBidi = BufRecvStream<H3Bidi, Bytes>;
/// Buffered unidirectional send stream.
pub type BufSend = BufRecvStream<H3Send, Bytes>;
/// Buffered unidirectional receive stream.
pub type BufRecv = BufRecvStream<H3Recv, Bytes>;
/// Server side of an upgraded CONNECT request stream.
pub type ServerConnect = h3::server::RequestStream<H3Bidi, Bytes>;
/// Client side of an upgraded CONNECT request stream.
pub type ClientConnect = h3::client::RequestStream<H3Bidi, Bytes>;

/// Errors surfaced by the WebTransport layer.
#[derive(Debug, thiserror::Error)]
pub enum WtError {
    /// Fatal h3 connection error.
    #[error("h3 connection error: {0}")]
    Connection(#[from] h3::error::ConnectionError),
    /// h3 stream-level error.
    #[error("h3 stream error: {0}")]
    Stream(#[from] h3::error::StreamError),
    /// Quinn transport error.
    #[error("quic error: {0}")]
    Quic(String),
    /// The peer reset the stream or stopped sending on it, carrying the
    /// application error code it used.
    #[error("wt stream terminated with code {0}")]
    StreamTerminated(u64),
    /// Protocol violation or invalid state.
    #[error("webtransport protocol error: {0}")]
    Protocol(String),
}

/// Crate result alias.
pub type Result<T, E = WtError> = std::result::Result<T, E>;

/// The `WT_CLOSE_SESSION` capsule type (draft-ietf-webtrans-http3 §5.3), the
/// only capsule `webtransport-go` v0.12.0 uses to end a session.
pub const CLOSE_SESSION_CAPSULE_TYPE: u64 = 0x2843;

/// `WTSessionGoneErrorCode` (webtransport-go `errors.go`): the stream error
/// code a peer sends on the CONNECT stream once the session is gone.
pub const WT_SESSION_GONE_ERROR_CODE: u64 = 0x170d7b68;

/// `SessionCloseOK` (`pkg/agent/endpoint/wire/protocol.go`): the application
/// close code for a normal session termination.
pub const SESSION_CLOSE_OK: u32 = 0;

/// Decode a remote stream reset carried by a split WT stream's I/O error.
/// Connection errors and codes outside the WebTransport range have no
/// application outcome. The reserved HTTP/3 code points are skipped.
pub fn remote_stream_reset_code(error: &io::Error) -> Option<u32> {
    let quic::StreamErrorIncoming::StreamTerminated { error_code } = error
        .get_ref()?
        .downcast_ref::<quic::StreamErrorIncoming>(
    )?
    else {
        return None;
    };
    let code = *error_code;
    const FIRST: u64 = 0x52e4_a40f_a8db;
    const LAST: u64 = 0x52e5_ac98_3162;
    if !(FIRST..=LAST).contains(&code) || (code - 0x21) % 0x1f == 0 {
        return None;
    }
    let offset = code - FIRST;
    u32::try_from(offset - offset / 0x1f).ok()
}

/// Map a raw (unframed) stream error from the h3 quic layer.
fn incoming_err(e: quic::StreamErrorIncoming) -> WtError {
    match e {
        quic::StreamErrorIncoming::StreamTerminated { error_code } => {
            WtError::StreamTerminated(error_code)
        }
        other => WtError::Protocol(format!("wt stream error: {other:?}")),
    }
}

/// Write the `type || session id` varint header of a newly opened WT stream
/// raw onto the wire, driving `poll_send` until fully consumed.
async fn write_bidi_header(stream: &mut BufBidi, session: SessionId) -> Result<()> {
    let mut header: WriteBuf<&'static [u8]> =
        WriteBuf::from(BidiStreamHeader::WebTransportBidi(session));
    while header.has_remaining() {
        poll_fn(|cx| stream.poll_send(cx, &mut header))
            .await
            .map_err(incoming_err)?;
    }
    Ok(())
}

/// Unidirectional variant of [`write_bidi_header`].
async fn write_uni_header(stream: &mut BufSend, session: SessionId) -> Result<()> {
    let mut header: WriteBuf<&'static [u8]> =
        WriteBuf::from(UniStreamHeader::WebTransportUni(session));
    while header.has_remaining() {
        poll_fn(|cx| stream.poll_send(cx, &mut header))
            .await
            .map_err(incoming_err)?;
    }
    Ok(())
}

/// Open a client/server-initiated WT bidirectional stream on the raw QUIC
/// connection and write its `0x41 || session` header.
async fn open_wt_bidi(conn: &quinn::Connection, session: SessionId) -> Result<WtBidiStream> {
    let mut h3_conn = H3Conn::new(conn.clone());
    let bidi = poll_fn(|cx| h3_conn.poll_open_bidi(cx))
        .await
        .map_err(|e| WtError::Protocol(format!("open bidi: {e:?}")))?;
    // An owned opener can be cancelled while flow control blocks this header.
    // Keep both halves fenced even before a WtBidiStream reaches its caller.
    struct PendingBidi(Option<WtBidiStream>);
    impl Drop for PendingBidi {
        fn drop(&mut self) {
            if let Some(stream) = &mut self.0 {
                terminate_bidi(stream);
            }
        }
    }
    let mut pending = PendingBidi(Some(WtBidiStream {
        stream: BufRecvStream::new(bidi),
    }));
    write_bidi_header(
        &mut pending.0.as_mut().expect("pending stream").stream,
        session,
    )
    .await?;
    Ok(pending.0.take().expect("opened stream"))
}

/// Open a WT unidirectional stream with its `0x54 || session` header.
async fn open_wt_uni(conn: &quinn::Connection, session: SessionId) -> Result<WtSendOnlyStream> {
    let mut h3_conn = H3Conn::new(conn.clone());
    let send = poll_fn(|cx| h3_conn.poll_open_send(cx))
        .await
        .map_err(|e| WtError::Protocol(format!("open uni: {e:?}")))?;
    let mut stream = BufRecvStream::new(send);
    write_uni_header(&mut stream, session).await?;
    Ok(WtSendOnlyStream { stream })
}

/// A WebTransport bidirectional stream carrying raw payload after its header.
pub struct WtBidiStream {
    stream: BufBidi,
}

impl WtBidiStream {
    /// Wrap an already header-validated whole bidi stream (accepted side).
    pub(crate) fn from_raw(stream: BufBidi) -> Self {
        Self { stream }
    }

    /// Append raw payload bytes to the stream.
    ///
    /// # Errors
    /// Propagates QUIC stream errors.
    pub async fn send_data(&mut self, data: Bytes) -> Result<()> {
        let mut buf = data;
        while buf.has_remaining() {
            poll_fn(|cx| self.stream.poll_send(cx, &mut buf))
                .await
                .map_err(incoming_err)?;
        }
        Ok(())
    }

    /// Read the next payload chunk, `None` at final stream size.
    ///
    /// # Errors
    /// Propagates QUIC stream errors.
    pub async fn recv_data(&mut self) -> Result<Option<Bytes>> {
        poll_fn(|cx| self.stream.poll_data(cx))
            .await
            .map_err(incoming_err)
    }

    /// Split the bidi stream into independent send and receive halves, boxed
    /// as `tokio` `AsyncWrite`/`AsyncRead`. The agent-wt control plane drives
    /// the two halves from separate tasks (a reader loop and a writer that
    /// drains job-assignment frames), which the unsplit `&mut self` API cannot
    /// do. Payload stays raw/unframed: the length-prefixed protobuf control
    /// encoding is applied by the caller, matching Go's `wire.WriteControlMessage`
    /// /`ReadControlMessage` over one `webtransport.Stream`.
    ///
    /// The halves keep the ability to terminate the stream with an
    /// application error code ([`ControlWriter::reset`] /
    /// [`ControlReader::stop_sending`]), which is how a session ends the
    /// streams it owns.
    #[must_use]
    pub fn into_split(self) -> (Box<dyn ControlWriter>, Box<dyn ControlReader>) {
        use h3::quic::BidiStream as _;
        let (send, recv) = self.stream.split();
        (
            Box::new(SplitWriter { inner: send }),
            Box::new(SplitReader { inner: recv }),
        )
    }

    /// Finish sending (final SIZE/close of the send side).
    ///
    /// # Errors
    /// Propagates QUIC stream errors.
    pub async fn finish(&mut self) -> Result<()> {
        poll_fn(|cx| self.stream.poll_finish(cx))
            .await
            .map_err(incoming_err)
    }

    /// Abort the send side with an application error code.
    pub fn reset(&mut self, error_code: u64) {
        self.stream.reset(error_code);
    }

    /// Signal the peer to stop the receive side with an application error code.
    pub fn stop_sending(&mut self, error_code: u64) {
        self.stream.stop_sending(error_code);
    }
}

/// The send half of a WebTransport stream, which can be terminated with an
/// application error code like Go's `webtransport.Stream.CancelWrite`.
pub trait ControlWriter: tokio::io::AsyncWrite + Unpin + Send + 'static {
    /// Terminate the send side with an application error code (a QUIC
    /// `RESET_STREAM`).
    fn reset(&mut self, error_code: u64);
}

/// The receive half of a WebTransport stream, which can be terminated with an
/// application error code like Go's `webtransport.Stream.CancelRead`.
pub trait ControlReader: tokio::io::AsyncRead + Unpin + Send + 'static {
    /// Tell the peer to stop sending with an application error code (a QUIC
    /// `STOP_SENDING`).
    fn stop_sending(&mut self, error_code: u64);
}

/// The write half of a split WebTransport stream.
struct SplitWriter {
    inner: <BufBidi as h3::quic::BidiStream<Bytes>>::SendStream,
}

/// The read half of a split WebTransport stream.
struct SplitReader {
    inner: <BufBidi as h3::quic::BidiStream<Bytes>>::RecvStream,
}

impl tokio::io::AsyncWrite for SplitWriter {
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

impl ControlWriter for SplitWriter {
    fn reset(&mut self, error_code: u64) {
        use h3::quic::SendStream as _;
        self.inner.reset(error_code);
    }
}

impl tokio::io::AsyncRead for SplitReader {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().inner).poll_read(cx, buf)
    }
}

impl ControlReader for SplitReader {
    fn stop_sending(&mut self, error_code: u64) {
        use h3::quic::RecvStream as _;
        self.inner.stop_sending(error_code);
    }
}

/// The streams that belong to one WebTransport session.
///
/// `webtransport-go` ends a session by terminating every stream it owns
/// (`CancelWrite`/`CancelRead` with `WTSessionGoneErrorCode`), and it also
/// terminates a stream that arrives after the session is already gone
/// (`incomingStreamsMap.AddStream`). The server side registers the streams it
/// accepted here so that ending the session reproduces both behaviours on the
/// wire instead of silently dropping the streams.
#[derive(Clone, Default)]
pub struct SessionStreams {
    inner: Arc<Mutex<SessionStreamsInner>>,
    closed: Arc<tokio::sync::Notify>,
}

/// The mutable state of a [`SessionStreams`] registry.
#[derive(Default)]
struct SessionStreamsInner {
    closed: bool,
    bidi: Vec<WtBidiStream>,
}

impl SessionStreams {
    /// Register an accepted stream. A stream that arrives after the session
    /// was closed is terminated right away, like `webtransport-go`'s
    /// `incomingStreamsMap.AddStream`.
    pub fn register(&self, stream: WtBidiStream) {
        let mut inner = self.inner.lock().expect("session streams poisoned");
        if inner.closed {
            drop(inner);
            let mut stream = stream;
            terminate_bidi(&mut stream);
            return;
        }
        inner.bidi.push(stream);
    }

    /// Terminate every registered stream with `WTSessionGoneErrorCode` and
    /// refuse the ones that arrive later.
    pub fn close(&self) {
        let mut inner = self.inner.lock().expect("session streams poisoned");
        if inner.closed {
            return;
        }
        inner.closed = true;
        let streams = std::mem::take(&mut inner.bidi);
        drop(inner);
        self.closed.notify_waiters();
        for mut stream in streams {
            terminate_bidi(&mut stream);
        }
    }
}

/// Terminate one bidirectional stream the way `webtransport-go` ends a
/// session's streams: `CancelWrite` plus `CancelRead` with
/// `WTSessionGoneErrorCode`.
fn terminate_bidi(stream: &mut WtBidiStream) {
    stream.stop_sending(WT_SESSION_GONE_ERROR_CODE);
    stream.reset(WT_SESSION_GONE_ERROR_CODE);
}

/// A server-to-client (or client-to-server) WT unidirectional send stream.
pub struct WtSendOnlyStream {
    stream: BufSend,
}

impl WtSendOnlyStream {
    /// Wrap a header-written unidirectional send stream.
    #[must_use]
    pub fn from_raw(stream: BufSend) -> Self {
        Self { stream }
    }

    /// Append raw payload bytes to the stream.
    ///
    /// # Errors
    /// Propagates QUIC stream errors.
    pub async fn send_data(&mut self, data: Bytes) -> Result<()> {
        let mut buf = data;
        while buf.has_remaining() {
            poll_fn(|cx| self.stream.poll_send(cx, &mut buf))
                .await
                .map_err(incoming_err)?;
        }
        Ok(())
    }

    /// Finish sending (close of the stream).
    ///
    /// # Errors
    /// Propagates QUIC stream errors.
    pub async fn finish(&mut self) -> Result<()> {
        poll_fn(|cx| self.stream.poll_finish(cx))
            .await
            .map_err(incoming_err)
    }

    /// Abort the stream with an application error code.
    pub fn reset(&mut self, error_code: u64) {
        self.stream.reset(error_code);
    }
}

/// The receive side of a WT unidirectional stream (accepted or split).
pub struct WtRecvStream {
    stream: BufRecv,
}

impl WtRecvStream {
    /// Wrap a demuxed WT unidirectional receive stream (header consumed).
    pub(crate) fn from_raw(stream: BufRecv) -> Self {
        Self { stream }
    }

    /// Read the next payload chunk, `None` at final stream size.
    ///
    /// # Errors
    /// Propagates QUIC stream errors.
    pub async fn recv_data(&mut self) -> Result<Option<Bytes>> {
        poll_fn(|cx| self.stream.poll_data(cx))
            .await
            .map_err(incoming_err)
    }

    /// Signal the peer to stop sending with an application error code.
    pub fn stop_sending(&mut self, error_code: u64) {
        self.stream.stop_sending(error_code);
    }
}

/// The CONNECT request stream backing a session, role-tagged.
pub enum ConnectStream {
    /// Server side of the upgraded CONNECT.
    Server(Box<ServerConnect>),
    /// Client side of the upgraded CONNECT.
    Client(Box<ClientConnect>),
}

impl ConnectStream {
    /// Send raw session-level payload on the CONNECT stream.
    ///
    /// # Errors
    /// Propagates h3 stream errors.
    pub async fn send_data(&mut self, data: Bytes) -> Result<()> {
        match self {
            Self::Server(stream) => stream.send_data(data).await.map_err(Into::into),
            Self::Client(stream) => stream.send_data(data).await.map_err(Into::into),
        }
    }

    /// Receive the next session-level payload chunk, normalized to `Bytes`.
    ///
    /// # Errors
    /// Propagates h3 stream errors.
    pub async fn recv_data(&mut self) -> Result<Option<Bytes>> {
        let chunk = match self {
            Self::Server(stream) => stream
                .recv_data()
                .await
                .map(|opt| opt.map(|mut b| b.copy_to_bytes(b.remaining()))),
            Self::Client(stream) => stream
                .recv_data()
                .await
                .map(|opt| opt.map(|mut b| b.copy_to_bytes(b.remaining()))),
        }?;
        Ok(chunk)
    }

    /// Finish the send side of the CONNECT stream (a QUIC FIN), like Go's
    /// `str.Close()` in `closeSessionStream`.
    ///
    /// # Errors
    /// Propagates h3 stream errors.
    pub async fn finish(&mut self) -> Result<()> {
        match self {
            Self::Server(stream) => stream.finish().await.map_err(Into::into),
            Self::Client(stream) => stream.finish().await.map_err(Into::into),
        }
    }

    /// Abort the CONNECT send half without ending the shared connection.
    pub fn reset(&mut self, error_code: u64) {
        let code = Code::from(error_code);
        match self {
            Self::Server(stream) => stream.stop_stream(code),
            Self::Client(stream) => stream.stop_stream(code),
        }
    }

    /// Tell the peer to stop sending on the CONNECT stream (a QUIC
    /// STOP_SENDING), like Go's `str.CancelRead(WTSessionGoneErrorCode)`.
    pub fn stop_sending(&mut self, error_code: u64) {
        let code = Code::from(error_code);
        match self {
            Self::Server(stream) => stream.stop_sending(code),
            Self::Client(stream) => stream.stop_sending(code),
        }
    }
}

/// One WebTransport session: the upgraded CONNECT channel plus the QUIC
/// connection used to open further WT streams and exchange datagrams.
pub struct WtSession {
    conn: quinn::Connection,
    session: SessionId,
    /// The CONNECT request's stream index, i.e. the "quarter stream ID" that
    /// prefixes every WebTransport datagram payload (RFC 9297 §2.1). quic-go's
    /// `http3` layer writes `varint(stream_id / 4)`, which for a client-
    /// initiated CONNECT stream is exactly this index; the peer parses the
    /// prefix to route the datagram to its session and closes the connection
    /// with `H3_DATAGRAM_ERROR` when it is missing.
    flow_id: u64,
    connect: ConnectStream,
    close_capsules: CloseCapsules,
    /// The streams this session owns. Ending the session terminates them, the
    /// way `webtransport-go` ends the streams of a session it closes.
    streams: SessionStreams,
}

impl WtSession {
    /// Bind a server session from the answered CONNECT request stream.
    pub fn from_server(conn: quinn::Connection, stream: ServerConnect) -> Self {
        let id = stream.send_id();
        let session = SessionId::try_from(id.into_inner()).expect("valid CONNECT stream id");
        let flow_id = id.index();
        Self {
            conn,
            session,
            flow_id,
            connect: ConnectStream::Server(Box::new(stream)),
            close_capsules: CloseCapsules::default(),
            streams: SessionStreams::default(),
        }
    }

    /// Bind a client session from a CONNECT request stream that got `200`.
    pub fn from_client(conn: quinn::Connection, stream: ClientConnect) -> Self {
        let id = stream.id();
        let session = SessionId::try_from(id.into_inner()).expect("valid CONNECT stream id");
        let flow_id = id.index();
        Self {
            conn,
            session,
            flow_id,
            connect: ConnectStream::Client(Box::new(stream)),
            close_capsules: CloseCapsules::default(),
            streams: SessionStreams::default(),
        }
    }

    /// The session id (the CONNECT stream id) identifying this session in
    /// WT stream headers.
    #[must_use]
    pub fn id(&self) -> SessionId {
        self.session
    }

    /// The underlying QUIC connection.
    #[must_use]
    pub fn connection(&self) -> &quinn::Connection {
        &self.conn
    }

    /// The session's "quarter stream ID" (RFC 9297 §2.1): the value every
    /// WebTransport datagram of this session is prefixed with.
    #[must_use]
    pub fn flow_id(&self) -> u64 {
        self.flow_id
    }

    /// The registry of the streams this session owns. Registering a stream
    /// there makes the session terminate it when the session ends.
    #[must_use]
    pub fn streams(&self) -> &SessionStreams {
        &self.streams
    }

    /// End the session the way the official listener does: a `WT_CLOSE_SESSION`
    /// capsule on the CONNECT stream carrying the application close code and
    /// the reason, followed by `STOP_SENDING(WTSessionGoneErrorCode)` and a FIN
    /// on that stream (Go's `closeSessionStream`).
    ///
    /// The streams the session owns are terminated with
    /// `WTSessionGoneErrorCode` first, exactly like `webtransport-go`'s
    /// `Session.closeWithError`, which closes every stream of the session
    /// before it writes the close capsule.
    ///
    /// The QUIC connection itself stays open, exactly like Go's
    /// `Session.CloseWithError`, which only tears the session down.
    ///
    /// # Errors
    /// Propagates h3 stream errors.
    pub async fn close_with_error(&mut self, code: u32, message: &str) -> Result<()> {
        self.streams.close();
        self.connect
            .send_data(encode_close_session_capsule(code, message))
            .await?;
        self.connect.stop_sending(WT_SESSION_GONE_ERROR_CODE);
        self.connect.finish().await?;
        Ok(())
    }

    /// Finish teardown initiated by a valid peer close capsule.
    /// Unlike a local close, webtransport-go does not send a reply capsule:
    /// it cancels both CONNECT halves and every owned stream, keeping QUIC up.
    pub fn close_from_peer(&mut self) {
        self.abort_connect(WT_SESSION_GONE_ERROR_CODE);
    }

    /// Finish teardown after the peer closes or malforms the capsule stream.
    /// Go uses H3_DATAGRAM_ERROR only on CONNECT; owned streams still see
    /// WT_SESSION_GONE.
    pub fn close_from_peer_read_error(&mut self) {
        self.abort_connect(Code::H3_DATAGRAM_ERROR.value());
    }

    fn abort_connect(&mut self, code: u64) {
        self.streams.close();
        self.connect.stop_sending(code);
        self.connect.reset(code);
    }

    /// Read the CONNECT stream's capsule stream until a `WT_CLOSE_SESSION`
    /// capsule arrives, returning its close code and message.
    ///
    /// `Ok(None)` means the peer ended the stream without closing the
    /// session (e.g. it tore the whole connection down).
    ///
    /// # Errors
    /// Propagates h3 stream errors and rejects a malformed close capsule.
    pub async fn read_close_capsule(&mut self) -> Result<Option<(u32, String)>> {
        loop {
            if let Some(close) = self.close_capsules.next()? {
                return Ok(Some(close));
            }
            match self.connect.recv_data().await? {
                Some(chunk) => self.close_capsules.input.extend_from_slice(&chunk),
                None => return self.close_capsules.finish(),
            }
        }
    }

    /// Send session-level payload on the CONNECT stream.
    ///
    /// # Errors
    /// Propagates h3 stream errors.
    pub async fn send(&mut self, data: Bytes) -> Result<()> {
        self.connect.send_data(data).await
    }

    /// Receive session-level payload from the CONNECT stream.
    ///
    /// # Errors
    /// Propagates h3 stream errors.
    pub async fn recv(&mut self) -> Result<Option<Bytes>> {
        self.connect.recv_data().await
    }

    /// A session-bound opener that can coexist with the CONNECT capsule reader.
    /// Both queued and completed opens are fenced by the original session's
    /// close state; cloning the QUIC connection never bypasses session closure.
    pub fn owned_bidi_opener(
        &self,
    ) -> impl Fn() -> Pin<Box<dyn std::future::Future<Output = Result<WtBidiStream>> + Send>>
    + Clone
    + Send
    + Sync
    + 'static
    + use<> {
        let conn = self.conn.clone();
        let session = self.session;
        let streams = self.streams.clone();
        move || {
            let conn = conn.clone();
            let streams = streams.clone();
            Box::pin(async move {
                let notified = streams.closed.notified();
                tokio::pin!(notified);
                notified.as_mut().enable();
                if streams
                    .inner
                    .lock()
                    .expect("session streams poisoned")
                    .closed
                {
                    return Err(WtError::Protocol("session closed".into()));
                }
                let mut stream = tokio::select! {
                    stream = open_wt_bidi(&conn, session) => stream?,
                    _ = &mut notified => return Err(WtError::Protocol("session closed".into())),
                };
                if streams
                    .inner
                    .lock()
                    .expect("session streams poisoned")
                    .closed
                {
                    terminate_bidi(&mut stream);
                    return Err(WtError::Protocol("session closed".into()));
                }
                Ok(stream)
            })
        }
    }

    /// Open a new WT bidirectional stream belonging to this session.
    ///
    /// # Errors
    /// Propagates QUIC/h3 stream errors.
    pub async fn open_bi(&self) -> Result<WtBidiStream> {
        open_wt_bidi(&self.conn, self.session).await
    }

    /// Open a new WT unidirectional send stream for this session.
    ///
    /// # Errors
    /// Propagates QUIC/h3 stream errors.
    pub async fn open_uni(&self) -> Result<WtSendOnlyStream> {
        open_wt_uni(&self.conn, self.session).await
    }

    /// Send an unreliable WebTransport datagram (RFC 9297: the raw QUIC
    /// application datagram carries the WT payload).
    ///
    /// The payload is prefixed with the session's quarter stream ID, exactly
    /// like `quic-go`'s `http3` layer does, so the peer can route the datagram
    /// to the session it belongs to.
    ///
    /// # Errors
    /// Reports unsupported/disabled datagrams or oversized payloads.
    pub fn send_datagram(&self, data: Bytes) -> Result<()> {
        let mut framed = BytesMut::with_capacity(data.len() + 8);
        encode_quic_varint(&mut framed, self.flow_id);
        framed.extend_from_slice(&data);
        self.conn
            .send_datagram(framed.freeze())
            .map_err(|e| WtError::Quic(e.to_string()))
    }

    /// Await the next inbound WT datagram, with the RFC 9297 flow-id prefix
    /// stripped.
    ///
    /// # Errors
    /// Propagates connection loss and rejects datagrams addressed to another
    /// session.
    pub async fn read_datagram(&self) -> Result<Bytes> {
        let raw = self
            .conn
            .read_datagram()
            .await
            .map_err(|e| WtError::Quic(e.to_string()))?;
        let (flow_id, payload) = decode_quic_varint(raw)
            .ok_or_else(|| WtError::Protocol("malformed datagram prefix".into()))?;
        if flow_id != self.flow_id {
            return Err(WtError::Protocol(format!(
                "datagram for foreign session ({flow_id} != {})",
                self.flow_id
            )));
        }
        Ok(payload)
    }
}

/// Encode `value` as a QUIC variable-length integer (RFC 9000 §16).
fn encode_quic_varint(buf: &mut BytesMut, value: u64) {
    let varint =
        h3::proto::varint::VarInt::from_u64(value).expect("quarter stream id fits in a varint");
    varint.encode(buf);
}

/// Decode a QUIC variable-length integer prefix, returning it with the
/// remaining bytes.
pub(crate) fn decode_quic_varint(mut buf: Bytes) -> Option<(u64, Bytes)> {
    let (value, len) = decode_quic_varint_slice(&buf)?;
    buf.advance(len);
    Some((value, buf))
}

/// Decode a QUIC variable-length integer from the front of `buf`, returning
/// the value and the number of bytes it occupied.
fn decode_quic_varint_slice(buf: &[u8]) -> Option<(u64, usize)> {
    let first = *buf.first()?;
    let len = 1usize << (first >> 6);
    if buf.len() < len {
        return None;
    }
    let mut value = u64::from(first & 0x3f);
    for byte in &buf[1..len] {
        value = (value << 8) | u64::from(*byte);
    }
    Some((value, len))
}

/// Encode a `WT_CLOSE_SESSION` capsule: the capsule type, the payload length,
/// then the 4-byte big-endian application close code and the UTF-8 message
/// (webtransport-go `capsule.go`).
fn encode_close_session_capsule(code: u32, message: &str) -> Bytes {
    let mut end = message.len().min(MAX_CLOSE_REASON);
    while !message.is_char_boundary(end) {
        end -= 1;
    }
    let message = &message[..end];
    let mut payload = BytesMut::with_capacity(4 + message.len());
    payload.extend_from_slice(&code.to_be_bytes());
    payload.extend_from_slice(message.as_bytes());
    let mut out = BytesMut::with_capacity(16 + payload.len());
    encode_quic_varint(&mut out, CLOSE_SESSION_CAPSULE_TYPE);
    encode_quic_varint(&mut out, payload.len() as u64);
    out.extend_from_slice(&payload);
    out.freeze()
}

const MAX_CLOSE_REASON: usize = 1024;
type CloseReason = (u32, String);

/// Capsule progress belongs to the session so dropping a pending read (for
/// example, when the control stream arrives) cannot lose the framing state.
#[derive(Default)]
struct CloseCapsules {
    input: BytesMut,
    body: Option<CapsuleBody>,
}

struct CapsuleBody {
    remaining: u64,
    payload: CapsulePayload,
}

enum CapsulePayload {
    Close(BytesMut),
    FlowControl {
        prefix: BytesMut,
        stream_limit: bool,
    },
    Unknown,
}

impl CloseCapsules {
    fn next(&mut self) -> Result<Option<CloseReason>> {
        loop {
            if self.body.is_none() {
                let Some((kind, type_len)) = decode_quic_varint_slice(&self.input) else {
                    return Ok(None);
                };
                let Some((length, length_len)) = decode_quic_varint_slice(&self.input[type_len..])
                else {
                    return Ok(None);
                };
                let payload = match kind {
                    CLOSE_SESSION_CAPSULE_TYPE => CapsulePayload::Close(BytesMut::new()),
                    // These two stream-data capsules are exclusive to HTTP/2.
                    0x190b4d3e | 0x190b4d42 => {
                        return Err(WtError::Protocol(
                            "HTTP/2-only WebTransport flow control capsule".into(),
                        ));
                    }
                    // No session flow control is negotiated here. Go still
                    // validates these known capsules before ignoring them.
                    0x190b4d3d | 0x190b4d3f..=0x190b4d41 | 0x190b4d43..=0x190b4d44 => {
                        CapsulePayload::FlowControl {
                            prefix: BytesMut::new(),
                            stream_limit: matches!(
                                kind,
                                0x190b4d3f | 0x190b4d40 | 0x190b4d43 | 0x190b4d44
                            ),
                        }
                    }
                    _ => CapsulePayload::Unknown,
                };
                self.input.advance(type_len + length_len);
                self.body = Some(CapsuleBody {
                    remaining: length,
                    payload,
                });
            }
            let body = self.body.as_mut().expect("capsule header parsed");
            let available = body.remaining.min(self.input.len() as u64) as usize;
            match &mut body.payload {
                CapsulePayload::Close(close) => {
                    let keep = available.min(4 + MAX_CLOSE_REASON - close.len());
                    close.extend_from_slice(&self.input[..keep]);
                }
                CapsulePayload::FlowControl {
                    prefix,
                    stream_limit,
                } => {
                    let length = body.remaining + prefix.len() as u64;
                    let keep = available.min(8 - prefix.len());
                    prefix.extend_from_slice(&self.input[..keep]);
                    let malformed = match decode_quic_varint_slice(prefix) {
                        Some((value, consumed)) => {
                            length != consumed as u64 || (*stream_limit && value > 1 << 60)
                        }
                        None => body.remaining == available as u64,
                    };
                    if malformed {
                        return Err(WtError::Protocol(
                            "invalid WebTransport flow control capsule".into(),
                        ));
                    }
                }
                CapsulePayload::Unknown => {}
            }
            self.input.advance(available);
            body.remaining -= available as u64;
            if body.remaining != 0 {
                return Ok(None);
            }
            let body = self.body.take().expect("capsule body completed");
            if let CapsulePayload::Close(mut close) = body.payload {
                if close.len() < 4 {
                    return Err(WtError::Protocol(
                        "WT_CLOSE_SESSION capsule shorter than its error code".into(),
                    ));
                }
                let code = close.get_u32();
                return Ok(Some((code, String::from_utf8_lossy(&close).into_owned())));
            }
        }
    }

    fn finish(&self) -> Result<Option<CloseReason>> {
        if self.body.is_some() || !self.input.is_empty() {
            return Err(WtError::Protocol("incomplete WebTransport capsule".into()));
        }
        Ok(None)
    }
}

#[cfg(test)]
mod capsule_tests {
    use super::*;

    #[test]
    fn skips_unknown_body_and_caps_close_reason_across_chunks() {
        let mut reader = CloseCapsules::default();
        encode_quic_varint(&mut reader.input, 63);
        encode_quic_varint(&mut reader.input, 65_536);
        for _ in 0..64 {
            reader.input.extend_from_slice(&[0x43; 1024]);
            assert!(reader.next().unwrap().is_none());
            assert!(reader.input.is_empty());
        }
        assert!(reader.body.is_none());
        encode_quic_varint(&mut reader.input, CLOSE_SESSION_CAPSULE_TYPE);
        encode_quic_varint(&mut reader.input, 2048);
        reader.input.extend_from_slice(&7_u32.to_be_bytes());
        reader.input.extend_from_slice(&[b'a'; 1500]);
        assert!(reader.next().unwrap().is_none());
        let CapsulePayload::Close(reason) = &reader.body.as_ref().unwrap().payload else {
            panic!("expected close reason");
        };
        assert_eq!(reason.len(), 1028);
        assert!(reader.finish().is_err());
        reader.input.extend_from_slice(&[b'b'; 544]);
        assert_eq!(reader.next().unwrap(), Some((7, "a".repeat(1024))));
        assert!(reader.finish().unwrap().is_none());
    }

    #[test]
    fn outgoing_reason_ends_at_utf8_boundary() {
        let message = format!("{}éx", "a".repeat(1023));
        let mut reader = CloseCapsules {
            input: BytesMut::from(encode_close_session_capsule(23, &message).as_ref()),
            ..Default::default()
        };
        assert_eq!(reader.next().unwrap(), Some((23, "a".repeat(1023))));
        assert!(reader.finish().unwrap().is_none());
    }

    #[test]
    fn validates_flow_control_capsules_even_when_flow_control_is_disabled() {
        let mut reader = CloseCapsules::default();
        encode_quic_varint(&mut reader.input, 0x190b4d3f);
        encode_quic_varint(&mut reader.input, 2);
        reader.input.extend_from_slice(&[0x40]);
        assert!(reader.next().unwrap().is_none());
        reader.input.extend_from_slice(&[0x40]);
        assert!(reader.next().unwrap().is_none());
        assert!(reader.finish().unwrap().is_none());

        // Once the first varint is complete, the declared extra byte alone is
        // enough to reject the capsule; the peer need not send the tail.
        encode_quic_varint(&mut reader.input, 0x190b4d3f);
        encode_quic_varint(&mut reader.input, 3);
        reader.input.extend_from_slice(&[0x40, 0x40]);
        assert!(reader.next().is_err());
    }
}
