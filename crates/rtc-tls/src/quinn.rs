//! Quinn `crypto` adapter over the pure-Rust TLS/QUIC stack (SPEC-0074 C15).
//!
//! Quinn's transport state machine is crypto-agnostic: it drives a TLS session
//! and packet/header protection through the public traits in
//! [`quinn_proto::crypto`]. Quinn's own implementation of those traits lives in
//! `quinn-proto::crypto::rustls` and is compiled only under the `rustls-ring` /
//! `rustls-aws-lc-rs` features, i.e. it always reaches for `ring`/`aws-lc`. We
//! disable those features and implement the traits here instead, reusing the
//! exact same `rustls::quic` key schedule that [super::quic] already bridges to
//! the RFC 9001-verified [`lk_quic_crypto`] core.
//!
//! The result is a fully self-contained QUIC crypto provider in pure Rust:
//!
//! * [`TlsSession`] implements [`quinn_proto::crypto::Session`] by wrapping a
//!   `rustls::quic::Connection`. rustls owns the TLS 1.3 handshake and the QUIC
//!   key schedule (Initial salts, `HKDF-Expand-Label` with the `quic key`/
//!   `quic iv`/`quic hp`/`quic ku` labels) through the AES-128-GCM suite's
//!   `quic` algorithm — [super::quic::QuicAlgorithm] — which delegates every
//!   AEAD/header-protection primitive to `lk_quic_crypto`.
//! * [`QuinnClientConfig`]/[`QuinnServerConfig`] implement the corresponding
//!   config traits and build the session and Initial keys from that suite.
//! * Retry integrity ([`quinn_proto::crypto::ServerConfig::retry_tag`] and
//!   [`quinn_proto::crypto::Session::is_valid_retry`]) uses the fixed RFC 9001
//!   §5.8 key/nonce via [`lk_quic_crypto::retry_tag`] — no `ring`.
//! * Endpoint-level stateless keys ([`ResetKey`] for `HmacKey`, [`TokenKey`] for
//!   `HandshakeTokenKey`/`AeadKey`) use the RustCrypto `hmac`/`sha2`/`aes-gcm`
//!   crates already vendored in the workspace.
//!
//! This module is gated behind the `quinn` cargo feature so the widely depended
//! on default `rtc-tls` graph stays free of the QUIC transport crate.

use std::{any::Any, io, sync::Arc};

use aes_gcm::aead::Aead;
use bytes::BytesMut;
use hmac::{Hmac, Mac};
use quinn_proto::{
    ConnectError, ConnectionId, Side as QuinnSide, TransportError, TransportErrorCode,
    crypto::{
        self, AeadKey, CryptoError, ExportKeyingMaterialError, HandshakeTokenKey, HeaderKey,
        HmacKey, KeyPair, Keys as QuinnKeys, PacketKey as QuinnPacketKey, UnsupportedVersion,
    },
    transport_parameters::TransportParameters,
};
use rustls::{
    CipherSuite, RootCertStore,
    client::ClientConfig as RustlsClientConfig,
    crypto::CryptoProvider,
    pki_types::{CertificateDer, PrivateKeyDer, ServerName},
    quic::{
        ClientConnection, Connection, HeaderProtectionKey, KeyChange, PacketKey, Secrets,
        ServerConnection, Suite, Version,
    },
    server::ServerConfig as RustlsServerConfig,
};
use sha2::Sha256;

type HmacSha256 = Hmac<Sha256>;

/// RFC 9001 §5.8 Retry integrity key/nonce (AES-128-GCM), version 1, are
/// applied internally by [`lk_quic_crypto::retry_tag`]. Only the draft-era
/// constants, unused by our version-1 sessions, are spelled out here.
const RETRY_INTEGRITY_KEY_DRAFT: [u8; 16] = [
    0xcc, 0xce, 0x18, 0x7e, 0xd0, 0x9a, 0x09, 0xd0, 0x57, 0x28, 0x15, 0x5a, 0x6c, 0xb9, 0x6b, 0xe1,
];
const RETRY_INTEGRITY_NONCE_DRAFT: [u8; 12] = [
    0xe5, 0x49, 0x30, 0xf9, 0x7f, 0x21, 0x36, 0xf0, 0x53, 0x0a, 0x8c, 0x1c,
];

/// Build the pure-Rust provider and locate the AES-128-GCM TLS 1.3 suite that
/// carries our QUIC [`super::quic::QuicAlgorithm`] as its `quic` hook.
fn quic_provider() -> Arc<CryptoProvider> {
    Arc::new(crate::crypto_provider())
}

/// The QUIC suite (`Suite`) for `TLS13_AES_128_GCM_SHA256` in `provider`, or
/// `None` when the provider offers no AES-128-GCM TLS 1.3 suite with a QUIC
/// algorithm. This mirrors Quinn's own `initial_suite_from_provider`.
fn initial_suite_from_provider(provider: &Arc<CryptoProvider>) -> Option<Suite> {
    let suite = provider
        .cipher_suites
        .iter()
        .find_map(|cs| match (cs.suite(), cs.tls13()) {
            (CipherSuite::TLS13_AES_128_GCM_SHA256, Some(s)) => Some(s),
            _ => None,
        })?;
    // The AES-128-GCM TLS 1.3 suite carries our QUIC algorithm in `quic`
    // (grafted in [`crate::provider`]); without it there is no QUIC suite.
    let quic = suite.quic?;
    Some(Suite { suite, quic })
}

fn interpret_version(version: u32) -> Result<Version, UnsupportedVersion> {
    match version {
        0xff00_001d..=0xff00_0020 => Ok(Version::V1Draft),
        0x0000_0001 | 0xff00_0021..=0xff00_0022 => Ok(Version::V1),
        _ => Err(UnsupportedVersion),
    }
}

/// `reset_stream_at` (QUIC Stream Resets with Partial Delivery, the
/// draft-ietf-quic-reliable-stream-reset codepoint quic-go v0.61 and therefore
/// the pinned LiveKit Go server use): a transport parameter whose *identifier*
/// is 0x1d and whose value is empty.
///
/// Quinn 0.11.19 has no field for it, so it never reaches the wire. webtransport-go
/// v0.12.0 refuses a session whose server did not advertise it
/// (`client_conn.go`: "server didn't enable QUIC stream reset partial delivery"),
/// and `webtransport.Server` forces the parameter on for its own listener, so a
/// LiveKit-compatible WebTransport listener has to carry it too.
const RESET_STREAM_AT_PARAMETER_ID: u64 = 0x1d;

/// Serialise Quinn's transport parameters, optionally appending the
/// zero-length `reset_stream_at` parameter. Quinn writes `id`/`length`/`value`
/// triples and QUIC does not order them, so appending is valid; the byte
/// pattern matches quic-go's `quicvarint.Append(b, 0x1d)` +
/// `quicvarint.Append(b, 0)`, and a receiver that does not know the parameter
/// skips it by length (RFC 9000 §7.3, which Quinn's own reader does).
fn to_vec(params: &TransportParameters, reset_stream_at: bool) -> Vec<u8> {
    let mut bytes = Vec::new();
    params.write(&mut bytes);
    if reset_stream_at {
        // Both identifiers fit in the single-byte QUIC variable-integer prefix.
        bytes.push(RESET_STREAM_AT_PARAMETER_ID as u8);
        bytes.push(0);
    }
    bytes
}

fn rustls_side(side: QuinnSide) -> rustls::Side {
    match side {
        QuinnSide::Client => rustls::Side::Client,
        QuinnSide::Server => rustls::Side::Server,
    }
}

/// Wrap a `rustls::quic::Keys` (whose packet/header keys already delegate to
/// `lk_quic_crypto`) into Quinn's `crypto::Keys` via the local newtypes below.
fn wrap_keys(keys: rustls::quic::Keys) -> QuinnKeys {
    QuinnKeys {
        header: KeyPair {
            local: Box::new(HeaderKeyWrap(keys.local.header)),
            remote: Box::new(HeaderKeyWrap(keys.remote.header)),
        },
        packet: KeyPair {
            local: Box::new(PacketKeyWrap(keys.local.packet)),
            remote: Box::new(PacketKeyWrap(keys.remote.packet)),
        },
    }
}

fn initial_keys(
    version: Version,
    dst_cid: ConnectionId,
    side: QuinnSide,
    suite: &Suite,
) -> QuinnKeys {
    wrap_keys(suite.keys(&dst_cid, rustls_side(side), version))
}

// --- Packet / header key newtypes -------------------------------------------

/// Quinn `PacketKey` over a `rustls::quic::PacketKey` (delegating to
/// `lk_quic_crypto`). Reimplements the `impl` Quinn gates behind `rustls-ring`.
struct PacketKeyWrap(Box<dyn PacketKey>);

impl QuinnPacketKey for PacketKeyWrap {
    fn encrypt(&self, packet: u64, buf: &mut [u8], header_len: usize) {
        let (header, payload_tag) = buf.split_at_mut(header_len);
        let (payload, tag_storage) = payload_tag.split_at_mut(payload_tag.len() - self.0.tag_len());
        let tag = self
            .0
            .encrypt_in_place(packet, &*header, payload)
            .expect("AEAD encrypt");
        tag_storage.copy_from_slice(tag.as_ref());
    }

    fn decrypt(
        &self,
        packet: u64,
        header: &[u8],
        payload: &mut BytesMut,
    ) -> Result<(), CryptoError> {
        let plain = self
            .0
            .decrypt_in_place(packet, header, payload.as_mut())
            .map_err(|_| CryptoError)?;
        let plain_len = plain.len();
        payload.truncate(plain_len);
        Ok(())
    }

    fn tag_len(&self) -> usize {
        self.0.tag_len()
    }

    fn confidentiality_limit(&self) -> u64 {
        self.0.confidentiality_limit()
    }

    fn integrity_limit(&self) -> u64 {
        self.0.integrity_limit()
    }
}

/// Quinn `HeaderKey` over a `rustls::quic::HeaderProtectionKey`.
///
/// Quinn's `crypto::HeaderKey` hands over the whole packet plus `pn_offset`, the
/// position of the *packet-number field* (quinn-proto `packet.rs:493` sets
/// `pn_pos = header_len - pn_len`, and the receive path reads the length back
/// from the unprotected byte). The buffer always extends four bytes past
/// `pn_offset` before the 16-byte sample, so those four bytes are the maximum
/// region the upstream adapter ever masks.
///
/// RFC 9001 §5.4.1 protects only the first byte's low bits and the actual
/// packet-number bytes. Quinn supplies a four-byte maximum region; the rustls
/// HeaderProtectionKey implementation must leave any following ciphertext
/// untouched. Our original AES128 provider XORed the entire supplied slice,
/// violating that contract. Historical quic-go differential tests exposed the
/// mismatch and a wrapper width probe worked around it. All provider algorithms
/// now honor the contract directly, so decryption uses one ordinary call.
struct HeaderKeyWrap(Box<dyn HeaderProtectionKey>);

/// Length of the packet-number field, given the *unprotected* first byte: the
/// two low bits are the width tag minus one (RFC 9000 §17.2, quinn-proto
/// `PacketNumber::decode_len`). The first byte is masked separately, so this is
/// also the width of the header-protection mask.
///
/// On receive those bits are themselves covered by header protection, so the
/// caller unprotects the first byte before asking.
fn header_protection_width(first_byte: u8) -> usize {
    (first_byte & 0x03) as usize + 1
}

impl HeaderKey for HeaderKeyWrap {
    fn decrypt(&self, pn_offset: usize, packet: &mut [u8]) {
        let (header, sample) = packet.split_at_mut(pn_offset + 4);
        let (first, rest) = header.split_at_mut(1);
        let sample_len = self.0.sample_len();
        // RFC 9000 §17.2 lays the packet number out from `pn_offset`, so that is
        // where the masked bytes start; upstream's four-byte span overshoots it.
        let pn_field = &mut rest[pn_offset - 1..];
        // rustls's algorithm unmasks the first byte before determining the
        // actual PN width, leaving any following ciphertext untouched. Supply
        // the maximum four-byte region in one call: a two-call empty-PN probe
        // is unnecessary and would bypass input-length validation.
        self.0
            .decrypt_in_place(&sample[..sample_len], &mut first[0], pn_field)
            .expect("header protection");
    }

    fn encrypt(&self, pn_offset: usize, packet: &mut [u8]) {
        let (header, sample) = packet.split_at_mut(pn_offset + 4);
        let (first, rest) = header.split_at_mut(1);
        let sample_len = self.0.sample_len();
        let pn_field = &mut rest[pn_offset - 1..];
        // On the send side the first byte still carries the packet-number length
        // in the clear, so the width is readable before protecting anything.
        let width = header_protection_width(first[0]).min(pn_field.len());
        self.0
            .encrypt_in_place(&sample[..sample_len], &mut first[0], &mut pn_field[..width])
            .expect("header protection");
    }

    fn sample_size(&self) -> usize {
        self.0.sample_len()
    }
}

// --- Session ----------------------------------------------------------------

/// A Quinn crypto session backed by a `rustls::quic` TLS 1.3 connection.
pub struct TlsSession {
    version: Version,
    got_handshake_data: bool,
    next_secrets: Option<Secrets>,
    inner: Connection,
    suite: Suite,
}

impl TlsSession {
    fn side(&self) -> QuinnSide {
        match self.inner {
            Connection::Client(_) => QuinnSide::Client,
            Connection::Server(_) => QuinnSide::Server,
        }
    }
}

/// Authentication data surfaced by [`TlsSession::handshake_data`].
pub struct HandshakeData {
    /// The negotiated application protocol (ALPN), if any.
    pub protocol: Option<Vec<u8>>,
    /// The SNI server name the client requested, if any (server side only).
    pub server_name: Option<String>,
}

impl crypto::Session for TlsSession {
    fn initial_keys(&self, dst_cid: &ConnectionId, side: QuinnSide) -> QuinnKeys {
        initial_keys(self.version, *dst_cid, side, &self.suite)
    }

    fn handshake_data(&self) -> Option<Box<dyn Any>> {
        if !self.got_handshake_data {
            return None;
        }
        Some(Box::new(HandshakeData {
            protocol: self.inner.alpn_protocol().map(|x| x.into()),
            server_name: match self.inner {
                Connection::Client(_) => None,
                Connection::Server(ref session) => session.server_name().map(|x| x.into()),
            },
        }))
    }

    fn peer_identity(&self) -> Option<Box<dyn Any>> {
        self.inner.peer_certificates().map(|v| -> Box<dyn Any> {
            Box::new(
                v.iter()
                    .map(|c| c.clone().into_owned())
                    .collect::<Vec<CertificateDer<'static>>>(),
            )
        })
    }

    fn early_crypto(&self) -> Option<(Box<dyn HeaderKey>, Box<dyn QuinnPacketKey>)> {
        let keys = self.inner.zero_rtt_keys()?;
        Some((
            Box::new(HeaderKeyWrap(keys.header)),
            Box::new(PacketKeyWrap(keys.packet)),
        ))
    }

    fn early_data_accepted(&self) -> Option<bool> {
        match self.inner {
            Connection::Client(ref session) => Some(session.is_early_data_accepted()),
            _ => None,
        }
    }

    fn is_handshaking(&self) -> bool {
        self.inner.is_handshaking()
    }

    fn read_handshake(&mut self, buf: &[u8]) -> Result<bool, TransportError> {
        self.inner.read_hs(buf).map_err(|e| {
            if let Some(alert) = self.inner.alert() {
                TransportError {
                    code: TransportErrorCode::crypto(alert.into()),
                    frame: None,
                    reason: e.to_string(),
                }
            } else {
                TransportError {
                    code: TransportErrorCode::PROTOCOL_VIOLATION,
                    frame: None,
                    reason: format!("TLS error: {e}"),
                }
            }
        })?;
        if !self.got_handshake_data {
            // Match Quinn: signal handshake-data readiness once ALPN or a server
            // name is available, or once the handshake has completed.
            let have_server_name = match self.inner {
                Connection::Client(_) => false,
                Connection::Server(ref session) => session.server_name().is_some(),
            };
            if self.inner.alpn_protocol().is_some() || have_server_name || !self.is_handshaking() {
                self.got_handshake_data = true;
                return Ok(true);
            }
        }
        Ok(false)
    }

    fn transport_parameters(&self) -> Result<Option<TransportParameters>, TransportError> {
        match self.inner.quic_transport_parameters() {
            None => Ok(None),
            Some(buf) => match TransportParameters::read(self.side(), &mut io::Cursor::new(buf)) {
                Ok(params) => Ok(Some(params)),
                Err(e) => Err(e.into()),
            },
        }
    }

    fn write_handshake(&mut self, buf: &mut Vec<u8>) -> Option<QuinnKeys> {
        let keys = match self.inner.write_hs(buf)? {
            KeyChange::Handshake { keys } => keys,
            KeyChange::OneRtt { keys, next } => {
                self.next_secrets = Some(next);
                keys
            }
        };
        Some(wrap_keys(keys))
    }

    fn next_1rtt_keys(&mut self) -> Option<KeyPair<Box<dyn QuinnPacketKey>>> {
        let secrets = self.next_secrets.as_mut()?;
        let keys = secrets.next_packet_keys();
        Some(KeyPair {
            local: Box::new(PacketKeyWrap(keys.local)),
            remote: Box::new(PacketKeyWrap(keys.remote)),
        })
    }

    fn is_valid_retry(&self, orig_dst_cid: &ConnectionId, header: &[u8], payload: &[u8]) -> bool {
        let tag_start = match payload.len().checked_sub(16) {
            Some(x) => x,
            None => return false,
        };
        // Reconstruct the Retry Pseudo-Packet body (everything but the trailing
        // 16-byte tag): header || payload[..tag_start]. lk_quic_crypto prepends
        // the `odcid_len || odcid` prefix internally.
        let mut without_tag = Vec::with_capacity(header.len() + tag_start);
        without_tag.extend_from_slice(header);
        without_tag.extend_from_slice(&payload[..tag_start]);
        match lk_quic_crypto::retry_tag(orig_dst_cid, &without_tag) {
            // Constant-time-ish compare; a wrong tag simply fails the equality.
            Ok(expected) => expected[..] == payload[tag_start..],
            Err(_) => false,
        }
    }

    fn export_keying_material(
        &self,
        output: &mut [u8],
        label: &[u8],
        context: &[u8],
    ) -> Result<(), ExportKeyingMaterialError> {
        self.inner
            .export_keying_material(output, label, Some(context))
            .map_err(|_| ExportKeyingMaterialError)?;
        Ok(())
    }
}

// --- Client config ----------------------------------------------------------

/// A Quinn `crypto::ClientConfig` over the pure-Rust provider.
pub struct QuinnClientConfig {
    inner: Arc<RustlsClientConfig>,
    initial: Suite,
    reset_stream_at: bool,
}

impl QuinnClientConfig {
    /// Build a client crypto config trusting `roots` and offering `alpn`.
    pub fn with_root_certificates(
        roots: RootCertStore,
        alpn: Vec<Vec<u8>>,
    ) -> Result<Self, rustls::Error> {
        let provider = quic_provider();
        let mut inner = RustlsClientConfig::builder_with_provider(provider)
            .with_protocol_versions(&[&rustls::version::TLS13])
            .map_err(|_| rustls::Error::General("TLS 1.3 unsupported".into()))?
            .with_go_root_certificates(roots)
            .with_no_client_auth();
        inner.alpn_protocols = alpn;
        let initial = initial_suite_from_provider(inner.crypto_provider())
            .ok_or(rustls::Error::General("no initial QUIC suite".into()))?;
        Ok(Self {
            inner: Arc::new(inner),
            initial,
            reset_stream_at: false,
        })
    }

    /// Build a client crypto config that defers certificate validation to a
    /// custom `verifier` (test/dev only: e.g. trusting a self-signed listener
    /// certificate without shipping its trust anchor).
    ///
    /// # Errors
    /// Returns a rustls error if TLS 1.3 or the QUIC initial suite is
    /// unavailable in the provider.
    pub fn dangerous_with_verifier(
        verifier: Arc<dyn rustls::client::danger::ServerCertVerifier>,
        alpn: Vec<Vec<u8>>,
    ) -> Result<Self, rustls::Error> {
        let provider = quic_provider();
        let mut inner = RustlsClientConfig::builder_with_provider(provider)
            .with_protocol_versions(&[&rustls::version::TLS13])
            .map_err(|_| rustls::Error::General("TLS 1.3 unsupported".into()))?
            .dangerous()
            .with_custom_certificate_verifier(verifier)
            .with_no_client_auth();
        inner.alpn_protocols = alpn;
        let initial = initial_suite_from_provider(inner.crypto_provider())
            .ok_or(rustls::Error::General("no initial QUIC suite".into()))?;
        Ok(Self {
            inner: Arc::new(inner),
            initial,
            reset_stream_at: false,
        })
    }

    /// Advertise the `reset_stream_at` transport parameter (QUIC Stream Resets
    /// with Partial Delivery).
    ///
    /// webtransport-go v0.12.0 needs it in both directions — its client won't
    /// open a session against a server that omits it, and its `Client` refuses to
    /// dial unless `QUICConfig.EnableStreamResetPartialDelivery` was set — so a
    /// WebTransport dialer has to reach the same wire state as the Go one.
    #[must_use]
    pub fn with_reset_stream_at(mut self) -> Self {
        self.reset_stream_at = true;
        self
    }
}

impl crypto::ClientConfig for QuinnClientConfig {
    fn start_session(
        self: Arc<Self>,
        version: u32,
        server_name: &str,
        params: &TransportParameters,
    ) -> Result<Box<dyn crypto::Session>, ConnectError> {
        let version = interpret_version(version)?;
        Ok(Box::new(TlsSession {
            version,
            got_handshake_data: false,
            next_secrets: None,
            inner: Connection::Client(
                ClientConnection::new(
                    self.inner.clone(),
                    version,
                    ServerName::try_from(server_name)
                        .map_err(|_| ConnectError::InvalidServerName(server_name.into()))?
                        .to_owned(),
                    to_vec(params, self.reset_stream_at),
                )
                .expect("client QUIC connection"),
            ),
            suite: self.initial,
        }))
    }
}

// --- Server config ----------------------------------------------------------

/// A Quinn `crypto::ServerConfig` over the pure-Rust provider.
pub struct QuinnServerConfig {
    inner: Arc<RustlsServerConfig>,
    initial: Suite,
    reset_stream_at: bool,
}

impl QuinnServerConfig {
    /// Build a server crypto config presenting `cert_chain`/`key` and requiring
    /// one of the `alpn` protocols.
    pub fn new(
        cert_chain: Vec<CertificateDer<'static>>,
        key: PrivateKeyDer<'static>,
        alpn: Vec<Vec<u8>>,
    ) -> Result<Self, rustls::Error> {
        let provider = quic_provider();
        let mut inner = RustlsServerConfig::builder_with_provider(provider)
            .with_protocol_versions(&[&rustls::version::TLS13])
            .map_err(|_| rustls::Error::General("TLS 1.3 unsupported".into()))?
            .with_no_client_auth()
            .with_single_cert(cert_chain, key)?;
        inner.alpn_protocols = alpn;
        // QUIC prohibits early data sizes other than 0 or u32::MAX; we accept 0.
        inner.max_early_data_size = 0;
        let initial = initial_suite_from_provider(inner.crypto_provider())
            .ok_or(rustls::Error::General("no initial QUIC suite".into()))?;
        Ok(Self {
            inner: Arc::new(inner),
            initial,
            reset_stream_at: false,
        })
    }

    /// Advertise the `reset_stream_at` transport parameter; see
    /// [`QuinnClientConfig::with_reset_stream_at`]. `webtransport.Server` forces
    /// the equivalent flag on for the Go listener
    /// (`webtransport-go@v0.12.0/server.go`), so this WebTransport listener has to
    /// do the same to be reachable by that client.
    #[must_use]
    pub fn with_reset_stream_at(mut self) -> Self {
        self.reset_stream_at = true;
        self
    }
}

impl crypto::ServerConfig for QuinnServerConfig {
    fn start_session(
        self: Arc<Self>,
        version: u32,
        params: &TransportParameters,
    ) -> Box<dyn crypto::Session> {
        // Safe: start_session is never called if initial_keys rejected version.
        let version = interpret_version(version).unwrap();
        Box::new(TlsSession {
            version,
            got_handshake_data: false,
            next_secrets: None,
            inner: Connection::Server(
                ServerConnection::new(
                    self.inner.clone(),
                    version,
                    to_vec(params, self.reset_stream_at),
                )
                .expect("server QUIC connection"),
            ),
            suite: self.initial,
        })
    }

    fn initial_keys(
        &self,
        version: u32,
        dst_cid: &ConnectionId,
    ) -> Result<QuinnKeys, UnsupportedVersion> {
        let version = interpret_version(version)?;
        Ok(initial_keys(
            version,
            *dst_cid,
            QuinnSide::Server,
            &self.initial,
        ))
    }

    fn retry_tag(&self, version: u32, orig_dst_cid: &ConnectionId, packet: &[u8]) -> [u8; 16] {
        let version = interpret_version(version).unwrap();
        // `packet` is the Retry packet with the trailing tag removed; lk's
        // version-1 helper uses the fixed §5.8 key/nonce and prepends the
        // odcid prefix. For the draft version, fall back to a manual seal.
        match version {
            Version::V1 => {
                lk_quic_crypto::retry_tag(orig_dst_cid, packet).expect("AES-128-GCM retry tag")
            }
            _ => {
                let key = &RETRY_INTEGRITY_KEY_DRAFT;
                let nonce_bytes = RETRY_INTEGRITY_NONCE_DRAFT;
                // Draft path is never reached by our version-1 sessions; compute
                // via the same AES-128-GCM primitive for completeness.
                let cipher = <aes_gcm::Aes128Gcm as aes_gcm::KeyInit>::new_from_slice(key)
                    .expect("16-byte key");
                let nonce = aes_gcm::aead::generic_array::GenericArray::from_slice(&nonce_bytes);
                let mut aad = Vec::with_capacity(packet.len() + orig_dst_cid.len() + 1);
                aad.push(orig_dst_cid.len() as u8);
                aad.extend_from_slice(orig_dst_cid);
                aad.extend_from_slice(packet);
                let out = cipher
                    .encrypt(
                        nonce,
                        aes_gcm::aead::Payload {
                            aad: &aad,
                            msg: b"",
                        },
                    )
                    .expect("draft retry seal");
                let mut result = [0u8; 16];
                result.copy_from_slice(&out);
                result
            }
        }
    }
}

// --- Endpoint-level stateless keys ------------------------------------------

/// HMAC-SHA256 key used by Quinn for stateless reset verification.
pub struct ResetKey {
    key: Vec<u8>,
}

impl ResetKey {
    /// Create a reset key from arbitrary secret bytes.
    pub fn new(key: &[u8]) -> Self {
        Self { key: key.to_vec() }
    }
}

impl HmacKey for ResetKey {
    fn sign(&self, data: &[u8], signature_out: &mut [u8]) {
        let mut mac = HmacSha256::new_from_slice(&self.key).expect("HMAC accepts any key length");
        mac.update(data);
        signature_out.copy_from_slice(&mac.finalize().into_bytes());
    }

    fn signature_len(&self) -> usize {
        32
    }

    fn verify(&self, data: &[u8], signature: &[u8]) -> Result<(), CryptoError> {
        let mut mac = HmacSha256::new_from_slice(&self.key).expect("HMAC accepts any key length");
        mac.update(data);
        mac.verify_slice(signature).map_err(|_| CryptoError)
    }
}

/// Handshake token key: an HKDF-SHA256 PRK used to derive per-token AES-256-GCM
/// keys via `aead_from_hkdf` (RFC 5869, matching Quinn's ring construction).
pub struct TokenKey {
    prk: [u8; 32],
}

impl TokenKey {
    /// HKDF-Extract the master secret with an empty salt into a PRK.
    pub fn new(master: &[u8]) -> Self {
        // Empty salt => HMAC key of HashLen zero bytes (RFC 5869 §2.2).
        let mut mac = HmacSha256::new_from_slice(&[0u8; 32]).expect("32-byte key");
        mac.update(master);
        let mut prk = [0u8; 32];
        prk.copy_from_slice(&mac.finalize().into_bytes());
        Self { prk }
    }
}

impl HandshakeTokenKey for TokenKey {
    fn aead_from_hkdf(&self, random_bytes: &[u8]) -> Box<dyn AeadKey> {
        // HKDF-Expand to 32 bytes: T(1) = HMAC(PRK, info || 0x01) is a single
        // SHA-256 block, exactly the AES-256 key length.
        let mut mac = HmacSha256::new_from_slice(&self.prk).expect("32-byte PRK");
        mac.update(random_bytes);
        mac.update(&[0x01]);
        let okm = mac.finalize().into_bytes();
        let cipher =
            <aes_gcm::Aes256Gcm as aes_gcm::KeyInit>::new_from_slice(&okm).expect("32-byte key");
        Box::new(TokenAeadKey { cipher })
    }
}

struct TokenAeadKey {
    cipher: aes_gcm::Aes256Gcm,
}

impl AeadKey for TokenAeadKey {
    fn seal(&self, data: &mut Vec<u8>, additional_data: &[u8]) -> Result<(), CryptoError> {
        let nonce = aes_gcm::aead::generic_array::GenericArray::from_slice(&[0u8; 12]);
        let sealed = self
            .cipher
            .encrypt(
                nonce,
                aes_gcm::aead::Payload {
                    aad: additional_data,
                    msg: &data[..],
                },
            )
            .map_err(|_| CryptoError)?;
        // `sealed` is ciphertext || tag with len == data.len() + 16.
        *data = sealed;
        Ok(())
    }

    fn open<'a>(
        &self,
        data: &'a mut [u8],
        additional_data: &[u8],
    ) -> Result<&'a mut [u8], CryptoError> {
        let nonce = aes_gcm::aead::generic_array::GenericArray::from_slice(&[0u8; 12]);
        let plain = self
            .cipher
            .decrypt(
                nonce,
                aes_gcm::aead::Payload {
                    aad: additional_data,
                    msg: &data[..],
                },
            )
            .map_err(|_| CryptoError)?;
        let len = plain.len();
        data[..len].copy_from_slice(&plain);
        Ok(&mut data[..len])
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A minimal decodable parameter block: one integer parameter
    /// (`initial_max_data`, identifier 0x04) with a one-byte value.
    const ONE_PARAM: &[u8] = &[0x04, 0x01, 0x10];

    fn decode(bytes: &[u8]) -> TransportParameters {
        TransportParameters::read(QuinnSide::Server, &mut &bytes[..])
            .expect("transport parameters must decode")
    }

    #[test]
    fn reset_stream_at_is_appended_as_an_empty_parameter() {
        let params = decode(ONE_PARAM);
        let plain = to_vec(&params, false);
        let advertised = to_vec(&params, true);
        // Whatever Quinn wrote stays byte-identical.
        assert_eq!(&advertised[..plain.len()], &plain[..]);
        // The addition is quic-go's exact encoding: identifier 0x1d, length 0.
        assert_eq!(&advertised[plain.len()..], &[0x1d, 0x00]);
    }

    #[test]
    fn reset_stream_at_survives_a_peer_that_does_not_know_it() {
        // RFC 9000 §7.3 makes an unknown parameter ignorable, and Quinn does skip
        // it by length -- so advertising `reset_stream_at` cannot break a
        // Rust-to-Rust connection, and a Go peer's own copy is tolerated on the
        // way in as well.
        let advertised = to_vec(&decode(ONE_PARAM), true);
        assert_eq!(decode(&advertised), decode(ONE_PARAM));
    }
}
