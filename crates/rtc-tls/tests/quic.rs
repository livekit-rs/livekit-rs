//! End-to-end TLS 1.3-over-QUIC handshake driven through the pure-Rust
//! provider's QUIC `Algorithm` (which delegates to the RFC 9001-verified
//! [`lk_quic_crypto`] core). No `ring`/`aws-lc`.
//!
//! This exercises the whole TLS-secret -> QUIC-key bridge that a Quinn
//! `crypto::Session` needs: the Initial key schedule, a completed TLS 1.3
//! handshake with Handshake and 1-RTT key changes, cross-endpoint AEAD and
//! header-protection round trips, key-update derivation, and an external
//! consistency check against `lk_quic_crypto`'s independent key derivation.

use std::sync::Arc;

use lk_quic_crypto::{AEAD_TAG_LEN, HP_LEN};
use rtc_tls::rustls::{
    self, CipherSuite, RootCertStore,
    client::ClientConfig,
    pki_types::{CertificateDer, PrivateKeyDer, ServerName, pem::PemObject},
    quic::{ClientConnection, KeyChange, Keys, Secrets, ServerConnection, Suite, Version},
    server::ServerConfig,
};

// Long-validity public test fixtures (see fixtures/expanded/README.md): an
// RSA root signs an intermediate, which signs the P-256 `localhost` leaf.
const ROOT: &[u8] = include_bytes!("fixtures/expanded/root-rsa.pem");
const LEAF: &[u8] = include_bytes!("fixtures/expanded/p256.pem");
const INTERMEDIATE: &[u8] = include_bytes!("fixtures/expanded/intermediate.pem");
const LEAF_KEY: &[u8] = include_bytes!("fixtures/expanded/p256-key.pem");

/// Extract the [`Keys`] (and any post-handshake `next` secrets) from a
/// rustls QUIC key change.
fn take_change(change: KeyChange) -> (Keys, Option<Secrets>) {
    match change {
        KeyChange::Handshake { keys } => (keys, None),
        KeyChange::OneRtt { keys, next } => (keys, Some(next)),
    }
}

fn quic_aes128_suite() -> Suite {
    let mut provider = rtc_tls::crypto_provider();
    provider
        .cipher_suites
        .retain(|cs| cs.suite() == CipherSuite::TLS13_AES_128_GCM_SHA256);
    // Sanity: the retained TLS 1.3 suite carries our QUIC algorithm.
    let tls13 = provider
        .cipher_suites
        .iter()
        .find_map(|cs| cs.tls13())
        .expect("AES-128-GCM TLS 1.3 suite present");
    let quic = tls13
        .quic
        .expect("AES-128-GCM suite advertises a QUIC algorithm");
    Suite { suite: tls13, quic }
}

fn configs(suite: CipherSuite) -> (Arc<ClientConfig>, Arc<ServerConfig>) {
    let mut provider = rtc_tls::crypto_provider();
    provider.cipher_suites.retain(|cs| cs.suite() == suite);
    // Keep a single key exchange so negotiation is deterministic.
    provider
        .kx_groups
        .retain(|g| g.name() == rustls::NamedGroup::X25519);

    let server = ServerConfig::builder_with_provider(Arc::new(provider.clone()))
        .with_protocol_versions(&[&rustls::version::TLS13])
        .unwrap()
        .with_no_client_auth()
        .with_single_cert(
            vec![
                CertificateDer::from_pem_slice(LEAF).unwrap(),
                CertificateDer::from_pem_slice(INTERMEDIATE).unwrap(),
            ],
            PrivateKeyDer::from_pem_slice(LEAF_KEY).unwrap(),
        )
        .unwrap();

    let mut roots = RootCertStore::empty();
    roots
        .add(CertificateDer::from_pem_slice(ROOT).unwrap())
        .unwrap();
    let client = ClientConfig::builder_with_provider(Arc::new(provider))
        .with_protocol_versions(&[&rustls::version::TLS13])
        .unwrap()
        .with_root_certificates(roots)
        .with_no_client_auth();

    (Arc::new(client), Arc::new(server))
}

/// Run a full TLS 1.3-over-QUIC handshake to completion, returning the final
/// 1-RTT [`Keys`] for each side plus the client's post-handshake `next`
/// secrets used for key updates.
fn complete_handshake() -> (Keys, Keys, Secrets) {
    let (client, server, next, _) =
        complete_handshake_with_suite(CipherSuite::TLS13_AES_128_GCM_SHA256);
    (client, server, next)
}

fn complete_handshake_with_suite(suite: CipherSuite) -> (Keys, Keys, Secrets, Secrets) {
    let (client_cfg, server_cfg) = configs(suite);
    let params = b"quic-transport-parameters-under-test".to_vec();

    let mut client = ClientConnection::new(
        client_cfg,
        Version::V1,
        ServerName::try_from("localhost").unwrap().to_owned(),
        params.clone(),
    )
    .expect("client QUIC connection");
    let mut server = ServerConnection::new(server_cfg, Version::V1, params.clone())
        .expect("server QUIC connection");

    let mut client_keys: Option<Keys> = None;
    let mut client_next: Option<Secrets> = None;
    let mut server_keys: Option<Keys> = None;
    let mut server_next: Option<Secrets> = None;

    let mut buf = Vec::new();
    // Pump CRYPTO-frame bytes until neither side produces more output. rustls
    // emits the ClientHello, then the Server flight (-> Handshake keys), then
    // the client Finished (-> 1-RTT keys), then post-handshake session tickets.
    loop {
        let mut progressed = false;

        buf.clear();
        if let Some(change) = client.write_hs(&mut buf) {
            let (keys, next) = take_change(change);
            client_keys = Some(keys);
            client_next = next.or(client_next);
        }
        if !buf.is_empty() {
            server.read_hs(&buf).expect("server reads client CRYPTO");
            progressed = true;
        }

        buf.clear();
        if let Some(change) = server.write_hs(&mut buf) {
            let (keys, next) = take_change(change);
            server_keys = Some(keys);
            server_next = next.or(server_next);
        }
        if !buf.is_empty() {
            client.read_hs(&buf).expect("client reads server CRYPTO");
            progressed = true;
        }

        if !progressed {
            break;
        }
    }

    assert_eq!(client.negotiated_cipher_suite().unwrap().suite(), suite);
    assert_eq!(server.negotiated_cipher_suite().unwrap().suite(), suite);
    assert!(!client.is_handshaking(), "client handshake did not finish");
    assert!(!server.is_handshaking(), "server handshake did not finish");
    // Both peers must have surfaced the other's transport parameters.
    assert_eq!(
        client.quic_transport_parameters(),
        Some(&params[..]),
        "client did not receive server transport parameters"
    );
    assert_eq!(
        server.quic_transport_parameters(),
        Some(&params[..]),
        "server did not receive client transport parameters"
    );

    (
        client_keys.expect("client 1-RTT keys"),
        server_keys.expect("server 1-RTT keys"),
        client_next.expect("client post-handshake secrets for key updates"),
        server_next.expect("server post-handshake secrets for key updates"),
    )
}

#[test]
fn tls13_over_quic_1rtt_aead_round_trips_across_endpoints() {
    let (client, server, _next) = complete_handshake();

    // client -> server application data: the client's local key must be
    // decryptable by the server's remote key (same traffic secret).
    let header = b"1-rtt-client-header".to_vec();
    let plaintext = b"hello from the client over 1-RTT".to_vec();
    let mut payload = plaintext.clone();
    assert_eq!(client.local.packet.tag_len(), AEAD_TAG_LEN);
    let tag = client
        .local
        .packet
        .encrypt_in_place(7, &header, &mut payload)
        .expect("client 1-RTT encrypt");
    let mut wire = payload.clone();
    wire.extend_from_slice(tag.as_ref());
    let opened = server
        .remote
        .packet
        .decrypt_in_place(7, &header, &mut wire)
        .expect("server 1-RTT decrypt");
    assert_eq!(opened, plaintext);

    // server -> client application data.
    let header = b"1-rtt-server-header".to_vec();
    let plaintext = b"reply from the server over 1-RTT".to_vec();
    let mut payload = plaintext.clone();
    let tag = server
        .local
        .packet
        .encrypt_in_place(11, &header, &mut payload)
        .expect("server 1-RTT encrypt");
    let mut wire = payload.clone();
    wire.extend_from_slice(tag.as_ref());
    let opened = client
        .remote
        .packet
        .decrypt_in_place(11, &header, &mut wire)
        .expect("client 1-RTT decrypt");
    assert_eq!(opened, plaintext);

    // A flipped tag bit must fail authentication (negative).
    *wire.last_mut().unwrap() ^= 1;
    assert!(
        client
            .remote
            .packet
            .decrypt_in_place(11, &header, &mut wire)
            .is_err(),
        "tampered 1-RTT tag was accepted"
    );
}

#[test]
fn tls13_over_quic_header_protection_round_trips_across_endpoints() {
    let (client, server, _next) = complete_handshake();

    let sample = [0xA5u8; 16];
    assert_eq!(client.local.header.sample_len(), 16);

    // Long header: only the low 4 bits of the first byte are protected
    // (RFC 9001 5.4.1), so the 0xf0 high nibble must survive, and the peer's
    // remote header key must fully restore the original bytes.
    let original_first = 0xc3u8;
    let original_pn = [0x00u8, 0x00, 0x12, 0x34];
    let mut first = original_first;
    let mut pn = original_pn;
    client
        .local
        .header
        .encrypt_in_place(&sample, &mut first, &mut pn)
        .expect("client HP protect");
    assert_eq!(
        first & 0xf0,
        original_first & 0xf0,
        "long-header high bits altered"
    );
    server
        .remote
        .header
        .decrypt_in_place(&sample, &mut first, &mut pn)
        .expect("server HP unprotect");
    assert_eq!(first, original_first, "HP did not restore first byte");
    assert_eq!(pn, original_pn, "HP did not restore packet number");
}

#[test]
fn tls13_over_quic_key_update_produces_working_packet_keys() {
    let (_client, _server, mut next) = complete_handshake();

    // Derive the next 1-RTT key pair from the stored post-handshake secrets.
    let updated = next.next_packet_keys();
    assert_eq!(updated.local.tag_len(), AEAD_TAG_LEN);

    let plaintext = b"data protected by an updated 1-RTT key".to_vec();
    let mut payload = plaintext.clone();
    let tag = updated
        .local
        .encrypt_in_place(42, b"ku-header", &mut payload)
        .expect("updated key encrypt");
    let mut wire = payload.clone();
    wire.extend_from_slice(tag.as_ref());
    let opened = updated
        .local
        .decrypt_in_place(42, b"ku-header", &mut wire)
        .expect("updated key decrypt");
    assert_eq!(opened, plaintext);
}

#[test]
fn quic_initial_key_schedule_matches_independent_rfc9001_core() {
    // External consistency: the rustls-driven Initial derivation (through the
    // QUIC `Algorithm` bridge) must agree byte-for-byte with `lk_quic_crypto`'s
    // independently RFC 9001-KAT-verified Initial key schedule.
    let suite = quic_aes128_suite();
    let dst_cid = [0x83, 0x94, 0xc8, 0xf0, 0x3e, 0x51, 0x57, 0x08];
    let rustls_initial = suite.keys(&dst_cid, rustls::Side::Client, Version::V1);
    let (core_client, _core_server) = lk_quic_crypto::initial_keys(&dst_cid);

    // AEAD: seal with the core's client Initial key, open through the rustls
    // Initial key. Both must hold the identical `client in` key/IV.
    let header = b"initial-packet-header";
    let plaintext = b"an initial-protected CRYPTO frame";
    let sealed = lk_quic_crypto::aead_seal(&core_client.key, &core_client.iv, 2, header, plaintext)
        .expect("core initial seal");
    let mut wire = sealed;
    let opened = rustls_initial
        .local
        .packet
        .decrypt_in_place(2, header, &mut wire)
        .expect("rustls initial open must accept core-sealed data");
    assert_eq!(
        opened, plaintext,
        "rustls Initial AEAD key != core Initial key"
    );

    // Header protection: apply the rustls Initial HP key and check it matches
    // the mask produced by the core's independent `hp` key (long-header 0x0f).
    let sample = [0x77u8; 16];
    let expected = lk_quic_crypto::hp_mask(&core_client.hp, &sample).expect("core hp mask");
    let mut first = 0xc3u8;
    let mut pn = [0x00u8, 0x00, 0x0a, 0x0b];
    rustls_initial
        .local
        .header
        .encrypt_in_place(&sample, &mut first, &mut pn)
        .expect("rustls initial HP");
    assert_eq!(
        first,
        0xc3 ^ (expected[0] & 0x0f),
        "rustls Initial hp key != core hp"
    );
    let restored = [
        pn[0] ^ expected[1],
        pn[1] ^ expected[2],
        pn[2] ^ expected[3],
        pn[3] ^ expected[4],
    ];
    assert_eq!(
        restored,
        [0x00, 0x00, 0x0a, 0x0b],
        "rustls Initial HP PN mask != core"
    );

    assert_eq!(core_client.hp.len(), HP_LEN);
}

#[test]
fn all_quic_suites_forced_handshake_packet_header_and_key_update() {
    for suite in [
        CipherSuite::TLS13_AES_128_GCM_SHA256,
        CipherSuite::TLS13_AES_256_GCM_SHA384,
        CipherSuite::TLS13_CHACHA20_POLY1305_SHA256,
    ] {
        let (client, server, mut next, mut peer_next) = complete_handshake_with_suite(suite);
        for (send, recv) in [
            (&client.local, &server.remote),
            (&server.local, &client.remote),
        ] {
            let mut payload = b"forced suite authenticated payload".to_vec();
            let plain = payload.clone();
            let tag = send
                .packet
                .encrypt_in_place(91, b"header", &mut payload)
                .unwrap();
            payload.extend_from_slice(tag.as_ref());
            let mut bad = payload.clone();
            *bad.last_mut().unwrap() ^= 1;
            assert!(
                recv.packet
                    .decrypt_in_place(91, b"header", &mut bad)
                    .is_err()
            );
            assert_eq!(
                recv.packet
                    .decrypt_in_place(91, b"header", &mut payload)
                    .unwrap(),
                plain
            );
            for form in [0x40, 0xc0] {
                for len in 1..=4 {
                    let original_first = form | (len - 1) as u8;
                    let original_pn = [0x12, 0x34, 0x56, 0x78];
                    let mut first = original_first;
                    let mut pn = original_pn;
                    send.header
                        .encrypt_in_place(&[0xa5; 16], &mut first, &mut pn)
                        .unwrap();
                    assert_eq!(&pn[len..], &original_pn[len..]);
                    recv.header
                        .decrypt_in_place(&[0xa5; 16], &mut first, &mut pn)
                        .unwrap();
                    assert_eq!((first, pn), (original_first, original_pn));
                }
            }
        }
        let updated = next.next_packet_keys();
        let peer_updated = peer_next.next_packet_keys();
        for (send, receive, old_receive) in [
            (&updated.local, &peer_updated.remote, &server.remote.packet),
            (&peer_updated.local, &updated.remote, &client.remote.packet),
        ] {
            let mut payload = b"updated suite keys".to_vec();
            let plain = payload.clone();
            let tag = send.encrypt_in_place(92, b"header", &mut payload).unwrap();
            payload.extend_from_slice(tag.as_ref());
            let mut stale = payload.clone();
            assert!(
                old_receive
                    .decrypt_in_place(92, b"header", &mut stale)
                    .is_err()
            );
            assert_eq!(
                receive
                    .decrypt_in_place(92, b"header", &mut payload)
                    .unwrap(),
                plain
            );
        }
    }
}

// --- W03 reachability: the forced-suite tests above restrict BOTH ends to one
// suite. These keep the server's full, unmodified live provider (AES-128-GCM
// listed first) and prove a peer that only offers ChaCha20-Poly1305 — e.g.
// quic-go on a host without AES hardware — still negotiates ChaCha QUIC. This
// refutes the historical "only AES-128-GCM is negotiated" claim.

fn full_provider_server_config() -> Arc<ServerConfig> {
    Arc::new(
        ServerConfig::builder_with_provider(Arc::new(rtc_tls::crypto_provider()))
            .with_protocol_versions(&[&rustls::version::TLS13])
            .unwrap()
            .with_no_client_auth()
            .with_single_cert(
                vec![
                    CertificateDer::from_pem_slice(LEAF).unwrap(),
                    CertificateDer::from_pem_slice(INTERMEDIATE).unwrap(),
                ],
                PrivateKeyDer::from_pem_slice(LEAF_KEY).unwrap(),
            )
            .unwrap(),
    )
}

fn client_config_retaining(suite: CipherSuite) -> Arc<ClientConfig> {
    let mut provider = rtc_tls::crypto_provider();
    provider.cipher_suites.retain(|cs| cs.suite() == suite);
    provider
        .kx_groups
        .retain(|g| g.name() == rustls::NamedGroup::X25519);
    let mut roots = RootCertStore::empty();
    roots
        .add(CertificateDer::from_pem_slice(ROOT).unwrap())
        .unwrap();
    Arc::new(
        ClientConfig::builder_with_provider(Arc::new(provider))
            .with_protocol_versions(&[&rustls::version::TLS13])
            .unwrap()
            .with_root_certificates(roots)
            .with_no_client_auth(),
    )
}

/// Drive a QUIC handshake, propagating rustls errors so a rejected negotiation
/// can be asserted rather than panicking. Returns the 1-RTT keys and the suite
/// both ends agreed on.
fn pump_quic(
    client_cfg: Arc<ClientConfig>,
    server_cfg: Arc<ServerConfig>,
) -> Result<(Keys, Keys, CipherSuite), rustls::Error> {
    let params = b"quic-transport-parameters-under-test".to_vec();
    let mut client = ClientConnection::new(
        client_cfg,
        Version::V1,
        ServerName::try_from("localhost").unwrap().to_owned(),
        params.clone(),
    )?;
    let mut server = ServerConnection::new(server_cfg, Version::V1, params.clone())?;
    let mut client_keys: Option<Keys> = None;
    let mut server_keys: Option<Keys> = None;
    let mut buf = Vec::new();
    loop {
        let mut progressed = false;
        buf.clear();
        if let Some(change) = client.write_hs(&mut buf) {
            client_keys = Some(take_change(change).0);
        }
        if !buf.is_empty() {
            server.read_hs(&buf)?;
            progressed = true;
        }
        buf.clear();
        if let Some(change) = server.write_hs(&mut buf) {
            server_keys = Some(take_change(change).0);
        }
        if !buf.is_empty() {
            client.read_hs(&buf)?;
            progressed = true;
        }
        if !progressed {
            break;
        }
    }
    if client.is_handshaking() || server.is_handshaking() {
        return Err(rustls::Error::General("handshake did not complete".into()));
    }
    let negotiated = client.negotiated_cipher_suite().unwrap().suite();
    assert_eq!(
        server.negotiated_cipher_suite().unwrap().suite(),
        negotiated
    );
    Ok((
        client_keys.expect("client 1-RTT keys"),
        server_keys.expect("server 1-RTT keys"),
        negotiated,
    ))
}

#[test]
fn live_full_provider_negotiates_chacha_quic_for_a_chacha_only_peer() {
    let (client_keys, server_keys, negotiated) = pump_quic(
        client_config_retaining(CipherSuite::TLS13_CHACHA20_POLY1305_SHA256),
        full_provider_server_config(),
    )
    .expect("a ChaCha-only peer must negotiate ChaCha QUIC against the live provider");
    assert_eq!(negotiated, CipherSuite::TLS13_CHACHA20_POLY1305_SHA256);

    // The negotiated ChaCha QUIC path must actually protect traffic across ends.
    let mut payload = b"chacha 1-rtt over the live provider".to_vec();
    let plain = payload.clone();
    let tag = client_keys
        .local
        .packet
        .encrypt_in_place(13, b"h", &mut payload)
        .unwrap();
    payload.extend_from_slice(tag.as_ref());
    assert_eq!(
        server_keys
            .remote
            .packet
            .decrypt_in_place(13, b"h", &mut payload)
            .unwrap(),
        plain
    );
    let mut first = 0xc3u8;
    let mut pn = [0x00u8, 0x00, 0x12, 0x34];
    client_keys
        .local
        .header
        .encrypt_in_place(&[0xa5; 16], &mut first, &mut pn)
        .unwrap();
    server_keys
        .remote
        .header
        .decrypt_in_place(&[0xa5; 16], &mut first, &mut pn)
        .unwrap();
    assert_eq!((first, pn), (0xc3, [0x00, 0x00, 0x12, 0x34]));
}

#[test]
fn chacha_only_peer_fails_when_the_server_drops_chacha() {
    // Positive control: the negotiation above is real, not a silent fallback. A
    // server keeping only AES-128-GCM shares no QUIC suite with a ChaCha-only
    // client, so the handshake must fail rather than complete on another suite.
    let mut provider = rtc_tls::crypto_provider();
    provider
        .cipher_suites
        .retain(|cs| cs.suite() == CipherSuite::TLS13_AES_128_GCM_SHA256);
    let server = Arc::new(
        ServerConfig::builder_with_provider(Arc::new(provider))
            .with_protocol_versions(&[&rustls::version::TLS13])
            .unwrap()
            .with_no_client_auth()
            .with_single_cert(
                vec![
                    CertificateDer::from_pem_slice(LEAF).unwrap(),
                    CertificateDer::from_pem_slice(INTERMEDIATE).unwrap(),
                ],
                PrivateKeyDer::from_pem_slice(LEAF_KEY).unwrap(),
            )
            .unwrap(),
    );
    assert!(
        pump_quic(
            client_config_retaining(CipherSuite::TLS13_CHACHA20_POLY1305_SHA256),
            server,
        )
        .is_err(),
        "a ChaCha-only peer must not fall back to a server without ChaCha"
    );
}
