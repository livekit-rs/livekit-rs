// Client-side WebTransport dialing over the pure-Rust stack.
//
// Mirrors what `webtransport-go`'s `ClientConn.dial` does: open a client-
// initiated bidi stream, send the HEADERS frame of an Extended CONNECT
// (`:method=CONNECT`, `:protocol=webtransport`, plus `:scheme`/`:authority`/
// `:path`), read the `200 OK` response, and keep the whole stream as the
// session's CONNECT channel. Server-initiated bidi streams are accepted
// through the h3 connection internals and classified by their first frame
// (`0x41 || session` WT header, checked against our session); unidirectional
// server streams arrive pre-demuxed in h3's `wt_uni_streams` queue. Used by
// the acceptance tests (and future SDK-facing plumbing) to drive a real
// handshake over UDP exactly the way the Go client does.

use std::future::poll_fn;
use std::sync::Arc;
use std::task::Poll;

use bytes::Bytes;
use h3::ext::Protocol;
use h3::frame::FrameStream;
use h3::proto::frame::Frame;
use h3::quic;
use h3::stream::BufRecvStream;
use h3::webtransport::SessionId;
use rtc_tls::quinn::{QuinnClientConfig, ResetKey};
use rtc_tls::rustls::client::danger::{
    HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier,
};
use rtc_tls::rustls::pki_types::{CertificateDer, ServerName, UnixTime};
use rtc_tls::rustls::{
    DigitallySignedStruct as Dss, Error as RustlsError, RootCertStore, SignatureScheme,
};

use crate::session::{
    ClientConnect, H3Conn, Result, WtBidiStream, WtError, WtRecvStream, WtSession,
};

/// h3 client connection driver over the Quinn binding.
type H3Client = h3::client::Connection<H3Conn, Bytes>;
/// h3 request dispatcher (kept for follow-up CONNECTs / stream opening).
type H3SendRequest =
    h3::client::SendRequest<<H3Conn as quic::Connection<Bytes>>::OpenStreams, Bytes>;

/// Build the pure-Rust Quinn `ClientConfig` trusting `roots` with the `h3`
/// ALPN (production path; verification uses the WebPkiVerifiers of `rtc-tls`).
///
/// # Errors
/// Returns a rustls error if the trust anchors are invalid.
pub fn client_config(roots: RootCertStore) -> Result<quinn::ClientConfig> {
    let crypto = QuinnClientConfig::with_root_certificates(roots, vec![b"h3".to_vec()])
        .map_err(|e| WtError::Protocol(format!("client crypto config: {e}")))?;
    Ok(finish_config(crypto))
}

/// As [`client_config`], but with the test-only accept-any verifier, matching
/// the Go dev mode where clients skip verification of the self-signed
/// listener cert.
///
/// # Errors
/// Returns a rustls error if the custom verifier setup fails.
pub fn client_config_insecure() -> Result<quinn::ClientConfig> {
    let crypto =
        QuinnClientConfig::dangerous_with_verifier(Arc::new(AllowAnyCert), vec![b"h3".to_vec()])
            .map_err(|e| WtError::Protocol(format!("client crypto config: {e}")))?;
    Ok(finish_config(crypto))
}

fn finish_config(crypto: QuinnClientConfig) -> quinn::ClientConfig {
    // quic-go turns on stream reset partial delivery for every WebTransport endpoint
    // (`ConfigureHTTP3Server` sets `EnableStreamResetPartialDelivery`), and
    // `webtransport-go` v0.12.0 refuses to open a session against a peer that omits the
    // `reset_stream_at` transport parameter. The vendored quinn-proto decodes
    // `RESET_STREAM_AT` and delivers its reliable prefix, so this promise is backed by a
    // real capability rather than an advertisement alone.
    let mut config = quinn::ClientConfig::new(Arc::new(crypto.with_reset_stream_at()));
    let mut transport = quinn::TransportConfig::default();
    transport.datagram_receive_buffer_size(Some(128));
    config.transport_config(Arc::new(transport));
    config
}

/// Bind a client endpoint on `socket` with our pure-Rust `client_config`.
///
/// # Errors
/// Propagates socket/IO errors from the Quinn endpoint construction.
pub fn endpoint(
    config: quinn::ClientConfig,
    reset_key: &[u8],
    socket: std::net::UdpSocket,
) -> std::io::Result<quinn::Endpoint> {
    let endpoint_config = quinn::EndpointConfig::new(Arc::new(ResetKey::new(reset_key)));
    quinn::Endpoint::new(endpoint_config, None, socket, Arc::new(quinn::TokioRuntime)).map(
        |mut ep| {
            ep.set_default_client_config(config);
            ep
        },
    )
}

/// A certificate verifier that trusts any end entity (test/dev only).
#[derive(Debug)]
pub struct AllowAnyCert;

impl ServerCertVerifier for AllowAnyCert {
    fn verify_server_cert(
        &self,
        _end_entity: &CertificateDer<'_>,
        _intermediates: &[CertificateDer<'_>],
        _server_name: &ServerName<'_>,
        _ocsp_response: &[u8],
        _now: UnixTime,
    ) -> std::result::Result<ServerCertVerified, RustlsError> {
        Ok(ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        _message: &[u8],
        _cert: &CertificateDer<'_>,
        _dss: &Dss,
    ) -> std::result::Result<HandshakeSignatureValid, RustlsError> {
        Ok(HandshakeSignatureValid::assertion())
    }

    fn verify_tls13_signature(
        &self,
        _message: &[u8],
        _cert: &CertificateDer<'_>,
        _dss: &Dss,
    ) -> std::result::Result<HandshakeSignatureValid, RustlsError> {
        Ok(HandshakeSignatureValid::assertion())
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        use SignatureScheme::{
            ECDSA_NISTP256_SHA256, ECDSA_NISTP384_SHA384, ED25519, RSA_PKCS1_SHA256, RSA_PSS_SHA256,
        };
        vec![
            ECDSA_NISTP256_SHA256,
            ECDSA_NISTP384_SHA384,
            ED25519,
            RSA_PKCS1_SHA256,
            RSA_PSS_SHA256,
        ]
    }
}

/// A dialed WebTransport session plus the h3 machinery backing further
/// requests and server-initiated stream acceptance.
pub struct WtClient {
    session: WtSession,
    h3: H3Client,
    send_req: H3SendRequest,
}

impl WtClient {
    /// The upgraded session (streams, datagrams, CONNECT channel).
    #[must_use]
    pub fn session(&self) -> &WtSession {
        &self.session
    }

    /// Mutable access to the session (payload on the CONNECT channel).
    pub fn session_mut(&mut self) -> &mut WtSession {
        &mut self.session
    }

    /// The h3 request dispatcher for follow-up requests on this connection.
    pub fn request_sender(&mut self) -> &mut H3SendRequest {
        &mut self.send_req
    }

    /// Upgrade another independent session on the existing QUIC connection.
    ///
    /// # Errors
    /// Propagates request/response errors and rejects a non-200 upgrade.
    pub async fn open_session(
        &mut self,
        authority: &str,
        path: &str,
        headers: &[(&str, &str)],
    ) -> Result<WtSession> {
        let mut stream = self
            .send_req
            .send_request(connect_request(authority, path, headers))
            .await?;
        let response = stream.recv_response().await?;
        if response.status() != http::StatusCode::OK {
            return Err(WtError::Protocol(format!(
                "webtransport upgrade failed with status {}",
                response.status()
            )));
        }
        Ok(WtSession::from_client(
            self.session.connection().clone(),
            stream,
        ))
    }

    /// Accept a server-initiated WT bidirectional stream, validating the
    /// `0x41 || session` header against this client's session.
    ///
    /// # Errors
    /// Propagates h3/QUIC errors; non-WT or foreign-session streams are
    /// protocol errors.
    pub async fn accept_bi(&mut self) -> Result<(SessionId, WtBidiStream)> {
        let bidi = poll_fn(|cx| self.h3.inner.poll_accept_bi(cx)).await?;
        let mut framed = FrameStream::new(BufRecvStream::new(bidi));
        match poll_fn(|cx| framed.poll_next(cx)).await {
            Ok(Some(Frame::WebTransportStream(session))) => {
                if session != self.session.id() {
                    return Err(WtError::Protocol(format!(
                        "webtransport stream for foreign session ({session:?})"
                    )));
                }
                Ok((session, WtBidiStream::from_raw(framed.into_inner())))
            }
            Ok(None) => Err(WtError::Protocol(
                "server-initiated bidi ended before first frame".into(),
            )),
            Ok(Some(other)) => Err(WtError::Protocol(format!(
                "unexpected first frame on server-initiated bidi: {other:?}"
            ))),
            Err(e) => Err(WtError::Protocol(format!("bad first frame: {e:?}"))),
        }
    }

    /// Drive h3 demuxing and take the next server-initiated WT unidirectional
    /// stream (`0x54` header already consumed by h3).
    ///
    /// # Errors
    /// Propagates h3 connection errors from the receive pump.
    pub async fn accept_uni(&mut self) -> Result<Option<(SessionId, WtRecvStream)>> {
        poll_fn(|cx| {
            self.h3.inner.poll_accept_recv(cx)?;
            if let Some((id, stream)) = self.h3.inner.accepted_streams_mut().wt_uni_streams.pop() {
                if id != self.session.id() {
                    return Poll::Ready(Err(WtError::Protocol(format!(
                        "webtransport uni stream for foreign session ({id:?})"
                    ))));
                }
                return Poll::Ready(Ok(Some((id, WtRecvStream::from_raw(stream)))));
            }
            Poll::Pending
        })
        .await
    }
}

/// Dial a WebTransport session at `addr` for SNI/`authority` and `path`,
/// completing the Extended CONNECT upgrade.
///
/// # Errors
/// Propagates QUIC handshake, h3 and upgrade errors; a non-`200` response is
/// reported as a protocol error with the status code.
pub async fn connect(
    endpoint: &quinn::Endpoint,
    addr: std::net::SocketAddr,
    authority: &str,
    path: &str,
) -> Result<WtClient> {
    connect_with_headers(endpoint, addr, authority, path, &[]).await
}

/// [`connect`] with request headers on the CONNECT (the Go server's auth
/// middleware reads `Authorization`/`access_token` from the request itself).
///
/// # Errors
/// Propagates QUIC handshake, h3 and upgrade errors; a non-`200` response is
/// reported as a protocol error with the status code.
pub async fn connect_with_headers(
    endpoint: &quinn::Endpoint,
    addr: std::net::SocketAddr,
    authority: &str,
    path: &str,
    headers: &[(&str, &str)],
) -> Result<WtClient> {
    let (client, _) = connect_observing(endpoint, addr, authority, path, headers).await?;
    Ok(client)
}

/// [`connect_with_headers`] that also surfaces the upgrade response header
/// map. The Extended CONNECT reply carries the server's WebTransport
/// negotiation (status plus, in draft02 mode, `sec-webtransport-http3-draft`);
/// exposing it lets the SPEC-0074 C15 differential compare a single client's
/// observed upgrade headers against the Go oracle and the Rust server
/// byte-for-byte.
///
/// # Errors
/// Propagates QUIC handshake, h3 and upgrade errors; a non-`200` response is
/// reported as a protocol error with the status code.
pub async fn connect_observing(
    endpoint: &quinn::Endpoint,
    addr: std::net::SocketAddr,
    authority: &str,
    path: &str,
    headers: &[(&str, &str)],
) -> Result<(WtClient, http::HeaderMap)> {
    let connecting = endpoint
        .connect(addr, authority)
        .map_err(|e| WtError::Protocol(format!("quic connect: {e}")))?;
    let conn = connecting
        .await
        .map_err(|e| WtError::Protocol(format!("quic handshake: {e}")))?;
    let (mut h3, mut send_req) = h3::client::builder()
        .enable_extended_connect(true)
        .enable_datagram(true)
        .send_grease(true)
        .build(h3_quinn::Connection::new(conn.clone()))
        .await?;
    // h3 0.0.8's client builder exposes no `enable_webtransport` knob; patch
    // the connection config through the third-party backend surface so h3's
    // unidirectional demux buffers `0x54` streams into `wt_uni_streams`
    // instead of dropping them (the h3 server builder does this natively).
    // Written as `Into::into` because `h3::config` is a private module; the
    // assignment target pins the concrete type.
    let mut wt_settings = h3::proto::frame::Settings::default();
    wt_settings
        .insert(h3::proto::frame::SettingId::ENABLE_WEBTRANSPORT, 1)
        .map_err(|e| WtError::Protocol(format!("wt setting: {e:?}")))?;
    wt_settings
        .insert(h3::proto::frame::SettingId::ENABLE_CONNECT_PROTOCOL, 1)
        .map_err(|e| WtError::Protocol(format!("connect setting: {e:?}")))?;
    wt_settings
        .insert(h3::proto::frame::SettingId::H3_DATAGRAM, 1)
        .map_err(|e| WtError::Protocol(format!("datagram setting: {e:?}")))?;
    h3.inner.config.settings = Into::into(&wt_settings);
    let mut stream: ClientConnect = send_req
        .send_request(connect_request(authority, path, headers))
        .await?;
    let resp = stream.recv_response().await?;
    let upgrade_headers = resp.headers().clone();
    if resp.status() != http::StatusCode::OK {
        return Err(WtError::Protocol(format!(
            "webtransport upgrade failed with status {}",
            resp.status()
        )));
    }
    let session = WtSession::from_client(conn, stream);
    Ok((
        WtClient {
            session,
            h3,
            send_req,
        },
        upgrade_headers,
    ))
}

/// The Extended CONNECT request per the Go client: method CONNECT, no body, and
/// the WebTransport `:protocol` token carried as the h3 `Protocol` extension.
/// `webtransport-go` v0.12.0 sends the draft-15 `webtransport-h3` value
/// (`protocol.go: protocolHeader`), so this dialer sends the same one.
fn connect_request(authority: &str, path: &str, headers: &[(&str, &str)]) -> http::Request<()> {
    let mut uri = String::new();
    let _ = std::fmt::write(&mut uri, format_args!("https://{authority}{path}"));
    let mut builder = http::Request::builder()
        .method(http::Method::CONNECT)
        .uri(uri);
    for (name, value) in headers {
        builder = builder.header(*name, *value);
    }
    let mut req = builder.body(()).expect("connect request");
    req.extensions_mut().insert(Protocol::WEB_TRANSPORT_H3);
    req
}
