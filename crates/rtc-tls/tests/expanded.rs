use rtc_tls::{rustls, tokio_rustls};
use rustls::pki_types::{CertificateDer, PrivateKeyDer, ServerName, pem::PemObject};
use std::{sync::Arc, time::Duration};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
const ROOT: &[u8] = include_bytes!("fixtures/expanded/root-rsa.pem");
const RSA_CERT: &[u8] = include_bytes!("fixtures/expanded/rsa.pem");
const RSA_KEY: &[u8] = include_bytes!("fixtures/expanded/rsa-key.pem");
const KEYS: &[(&str, &[u8], &[u8])] = &[
    ("RSA", RSA_CERT, RSA_KEY),
    (
        "P256",
        include_bytes!("fixtures/expanded/p256.pem"),
        include_bytes!("fixtures/expanded/p256-key.pem"),
    ),
    (
        "P384",
        include_bytes!("fixtures/expanded/p384.pem"),
        include_bytes!("fixtures/expanded/p384-key.pem"),
    ),
    (
        "P521",
        include_bytes!("fixtures/expanded/p521.pem"),
        include_bytes!("fixtures/expanded/p521-key.pem"),
    ),
    (
        "Ed25519",
        include_bytes!("fixtures/expanded/ed25519.pem"),
        include_bytes!("fixtures/expanded/ed25519-key.pem"),
    ),
];
fn roots(pem: &[u8]) -> rustls::RootCertStore {
    let mut roots = rustls::RootCertStore::empty();
    roots
        .add(CertificateDer::from_pem_slice(pem).unwrap())
        .unwrap();
    roots
}
async fn exchange(
    mut server: rustls::ServerConfig,
    mut client: rustls::ClientConfig,
) -> Result<(), String> {
    // Bound records to exercise AEAD framing across more than one TLS record.
    server.max_fragment_size = Some(128);
    client.max_fragment_size = Some(128);
    let (a, b) = tokio::io::duplex(65536);
    let acceptor = tokio_rustls::TlsAcceptor::from(Arc::new(server));
    let connector = tokio_rustls::TlsConnector::from(Arc::new(client));
    let (server, client) = tokio::join!(
        acceptor.accept(a),
        connector.connect(ServerName::try_from("localhost").unwrap(), b)
    );
    let mut server = server.map_err(|e| e.to_string())?;
    let mut client = client.map_err(|e| e.to_string())?;
    let expected = vec![123; 4096];
    let send = async {
        client.write_all(&expected).await?;
        client.shutdown().await
    };
    let recv = async {
        let mut received = Vec::new();
        server.read_to_end(&mut received).await?;
        assert_eq!(received, expected);
        Ok::<_, std::io::Error>(())
    };
    let (a, b) = tokio::join!(send, recv);
    a.map_err(|e| e.to_string())?;
    b.map_err(|e| e.to_string())?;
    Ok(())
}
#[tokio::test]
async fn tls12_tls13_all_aeads_key_exchanges_and_endpoint_keys() {
    use rustls::{CipherSuite as C, NamedGroup as G};
    for &(name, cert, key) in KEYS {
        for group in [G::X25519, G::secp256r1, G::secp384r1] {
            for tls13 in [true, false] {
                let suites = if tls13 {
                    vec![
                        C::TLS13_AES_128_GCM_SHA256,
                        C::TLS13_AES_256_GCM_SHA384,
                        C::TLS13_CHACHA20_POLY1305_SHA256,
                    ]
                } else if name == "RSA" {
                    vec![
                        C::TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256,
                        C::TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384,
                        C::TLS_ECDHE_RSA_WITH_CHACHA20_POLY1305_SHA256,
                    ]
                } else {
                    vec![
                        C::TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256,
                        C::TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384,
                        C::TLS_ECDHE_ECDSA_WITH_CHACHA20_POLY1305_SHA256,
                    ]
                };
                for suite in suites {
                    let mut provider = rtc_tls::crypto_provider();
                    provider.cipher_suites.retain(|s| s.suite() == suite);
                    provider.kx_groups.retain(|g| g.name() == group);
                    let versions = if tls13 {
                        vec![&rustls::version::TLS13]
                    } else {
                        vec![&rustls::version::TLS12]
                    };
                    let server =
                        rustls::ServerConfig::builder_with_provider(Arc::new(provider.clone()))
                            .with_protocol_versions(&versions)
                            .unwrap()
                            .with_no_client_auth()
                            .with_single_cert(
                                vec![CertificateDer::from_pem_slice(cert).unwrap()],
                                PrivateKeyDer::from_pem_slice(key).unwrap(),
                            )
                            .unwrap();
                    let client = rustls::ClientConfig::builder_with_provider(Arc::new(provider))
                        .with_protocol_versions(&versions)
                        .unwrap()
                        .with_root_certificates(roots(ROOT))
                        .with_no_client_auth();
                    let result =
                        tokio::time::timeout(Duration::from_secs(10), exchange(server, client))
                            .await;
                    assert!(
                        matches!(result, Ok(Ok(()))),
                        "{name}/{suite:?}/{group:?}: {result:?}"
                    );
                }
            }
        }
    }
}
#[test]
fn tls12_sha512_ecdsa_uses_leaf_curve_without_weakening_tls13() {
    use p256::ecdsa::signature::hazmat::PrehashSigner;
    use p256::pkcs8::DecodePrivateKey;
    use rustls::crypto::{verify_tls12_signature, verify_tls13_signature};
    use rustls::internal::msgs::codec::Codec;
    use rustls::{DigitallySignedStruct, SignatureScheme};
    use sha2::{Digest, Sha512};

    let message = b"ECDSA SHA-512 handshake input";
    let prehash = Sha512::digest(message);
    let algorithms = rtc_tls::crypto_provider().signature_verification_algorithms;

    let (_, p256_cert, p256_key) = KEYS[1];
    let (_, p384_cert, p384_key) = KEYS[2];
    let PrivateKeyDer::Pkcs8(p256_key) = PrivateKeyDer::from_pem_slice(p256_key).unwrap() else {
        panic!("P-256 fixture must contain a PKCS#8 key");
    };
    let PrivateKeyDer::Pkcs8(p384_key) = PrivateKeyDer::from_pem_slice(p384_key).unwrap() else {
        panic!("P-384 fixture must contain a PKCS#8 key");
    };
    let p256_sig: p256::ecdsa::DerSignature =
        p256::ecdsa::SigningKey::from_pkcs8_der(p256_key.secret_pkcs8_der())
            .unwrap()
            .sign_prehash(&prehash)
            .unwrap();
    let p384_sig: p384::ecdsa::DerSignature =
        p384::ecdsa::SigningKey::from_pkcs8_der(p384_key.secret_pkcs8_der())
            .unwrap()
            .sign_prehash(&prehash)
            .unwrap();

    for (cert, sig) in [
        (p256_cert, p256_sig.as_bytes()),
        (p384_cert, p384_sig.as_bytes()),
    ] {
        let cert = CertificateDer::from_pem_slice(cert).unwrap();
        // 0x0603 selects SHA-512/ECDSA in TLS 1.2 and P-521/SHA-512 in TLS 1.3.
        let mut encoded = u16::from(SignatureScheme::ECDSA_NISTP521_SHA512)
            .to_be_bytes()
            .to_vec();
        encoded.extend_from_slice(&u16::try_from(sig.len()).unwrap().to_be_bytes());
        encoded.extend_from_slice(sig);
        let signed = DigitallySignedStruct::read_bytes(&encoded).unwrap();
        verify_tls12_signature(message, &cert, &signed, &algorithms).unwrap();
        assert!(verify_tls12_signature(b"tampered", &cert, &signed, &algorithms).is_err());
        assert!(verify_tls13_signature(message, &cert, &signed, &algorithms).is_err());
    }
}

#[test]
fn rsa_pkcs1_and_pkcs8_signatures_match_independent_openssl_vectors() {
    use rustls::SignatureScheme as S;
    let provider = rtc_tls::crypto_provider();
    for key in [
        RSA_KEY,
        include_bytes!("fixtures/expanded/rsa-pkcs1-key.pem").as_ref(),
    ] {
        let key = provider
            .key_provider
            .load_private_key(PrivateKeyDer::from_pem_slice(key).unwrap())
            .unwrap();
        for (scheme, signature) in [
            (
                S::RSA_PKCS1_SHA256,
                include_bytes!("fixtures/expanded/rsa-pkcs1-sha256.sig").as_ref(),
            ),
            (
                S::RSA_PKCS1_SHA384,
                include_bytes!("fixtures/expanded/rsa-pkcs1-sha384.sig").as_ref(),
            ),
            (
                S::RSA_PKCS1_SHA512,
                include_bytes!("fixtures/expanded/rsa-pkcs1-sha512.sig").as_ref(),
            ),
        ] {
            assert_eq!(
                key.choose_scheme(&[scheme])
                    .unwrap()
                    .sign(include_bytes!("fixtures/expanded/rsa-message.bin"))
                    .unwrap(),
                signature
            );
        }
    }
}
#[test]
fn independent_rsa_vectors_verify_and_tampered_signatures_messages_and_keys_fail() {
    use rustls::SignatureScheme as S;
    let provider = rtc_tls::crypto_provider();
    let public = include_bytes!("fixtures/expanded/rsa-public.der");
    let message = include_bytes!("fixtures/expanded/rsa-message.bin");
    for (scheme, sig) in [
        (
            S::RSA_PKCS1_SHA256,
            include_bytes!("fixtures/expanded/rsa-pkcs1-sha256.sig").as_ref(),
        ),
        (
            S::RSA_PKCS1_SHA384,
            include_bytes!("fixtures/expanded/rsa-pkcs1-sha384.sig").as_ref(),
        ),
        (
            S::RSA_PKCS1_SHA512,
            include_bytes!("fixtures/expanded/rsa-pkcs1-sha512.sig").as_ref(),
        ),
        (
            S::RSA_PSS_SHA256,
            include_bytes!("fixtures/expanded/rsa-pss-sha256.sig").as_ref(),
        ),
        (
            S::RSA_PSS_SHA384,
            include_bytes!("fixtures/expanded/rsa-pss-sha384.sig").as_ref(),
        ),
        (
            S::RSA_PSS_SHA512,
            include_bytes!("fixtures/expanded/rsa-pss-sha512.sig").as_ref(),
        ),
    ] {
        let verifier = provider
            .signature_verification_algorithms
            .mapping
            .iter()
            .find(|(s, _)| *s == scheme)
            .unwrap()
            .1[0];
        verifier.verify_signature(public, message, sig).unwrap();
        let mut changed = sig.to_vec();
        changed[0] ^= 1;
        assert!(
            verifier
                .verify_signature(public, message, &changed)
                .is_err()
        );
        assert!(
            verifier
                .verify_signature(public, b"different", sig)
                .is_err()
        );
        assert!(
            verifier
                .verify_signature(&public[..public.len() - 1], message, sig)
                .is_err()
        );
    }
}
#[tokio::test]
async fn ed25519_signed_issuer_and_tampered_rsa_chain_are_checked() {
    let key = include_bytes!("fixtures/expanded/p256-key.pem");
    let cert = include_bytes!("fixtures/expanded/p256-ed-chain.pem");
    let server = rtc_tls::server_config_from_pem(cert, key).unwrap();
    let client = rtc_tls::client_config_with_roots(roots(include_bytes!(
        "fixtures/expanded/root-ed25519.pem"
    )))
    .unwrap();
    exchange(server, client).await.unwrap();
    let mut cert = CertificateDer::from_pem_slice(RSA_CERT).unwrap().to_vec();
    *cert.last_mut().unwrap() ^= 1;
    let server = rustls::ServerConfig::builder_with_provider(Arc::new(rtc_tls::crypto_provider()))
        .with_protocol_versions(&[&rustls::version::TLS13])
        .unwrap()
        .with_no_client_auth()
        .with_single_cert(
            vec![CertificateDer::from(cert)],
            PrivateKeyDer::from_pem_slice(RSA_KEY).unwrap(),
        )
        .unwrap();
    assert!(
        exchange(
            server,
            rtc_tls::client_config_with_roots(roots(ROOT)).unwrap()
        )
        .await
        .is_err()
    );
}
#[test]
fn expanded_private_keys_match_leaf_and_rsa_private_exponent_corruption_is_rejected() {
    for &(name, cert, key) in KEYS {
        rtc_tls::server_config_from_pem(cert, key).unwrap_or_else(|e| panic!("{name}: {e}"));
    }
    for (cert, key) in [
        (
            include_bytes!("fixtures/expanded/p384.pem").as_ref(),
            include_bytes!("fixtures/expanded/p384-sec1-key.pem").as_ref(),
        ),
        (
            include_bytes!("fixtures/expanded/p521.pem").as_ref(),
            include_bytes!("fixtures/expanded/p521-sec1-key.pem").as_ref(),
        ),
    ] {
        rtc_tls::server_config_from_pem(cert, key).unwrap();
    }
    assert!(
        rtc_tls::server_config_from_pem(
            RSA_CERT,
            include_bytes!("fixtures/expanded/ed25519-key.pem")
        )
        .is_err()
    );
    let key = PrivateKeyDer::from_pem_slice(include_bytes!("fixtures/expanded/rsa-pkcs1-key.pem"))
        .unwrap();
    let mut der = key.secret_der().to_vec();
    use rsa::pkcs8::der::Decode;
    let parsed = rsa::pkcs1::RsaPrivateKey::from_der(&der).unwrap();
    let offset = parsed.private_exponent.as_bytes().as_ptr() as usize - der.as_ptr() as usize;
    der[offset + 8] ^= 1;
    assert!(
        rtc_tls::crypto_provider()
            .key_provider
            .load_private_key(rustls::pki_types::PrivatePkcs1KeyDer::from(der).into())
            .is_err()
    );
}
#[test]
fn low_order_x25519_key_shares_and_static_rsa_suites_are_unavailable() {
    let provider = rtc_tls::crypto_provider();
    let x = provider
        .kx_groups
        .iter()
        .find(|g| g.name() == rustls::NamedGroup::X25519)
        .unwrap();
    for share in [vec![0; 32], vec![1], {
        let mut p = vec![0; 32];
        p[0] = 1;
        p
    }] {
        assert!(x.start().unwrap().complete(&share).is_err());
    }
    assert!(
        provider
            .cipher_suites
            .iter()
            .all(|s| !format!("{:?}", s.suite()).starts_with("TLS_RSA_"))
    );
}

#[tokio::test]
async fn root_intermediate_leaf_fullchain_verifies_and_missing_or_wrong_order_fails() {
    let key = include_bytes!("fixtures/expanded/p256-key.pem");
    let fullchain = include_bytes!("fixtures/expanded/fullchain.pem");
    let trust = include_bytes!("fixtures/expanded/chain-root.pem");
    let server = rtc_tls::server_config_from_pem(fullchain, key).unwrap();
    exchange(
        server,
        rtc_tls::client_config_with_roots(roots(trust)).unwrap(),
    )
    .await
    .unwrap();
    let leaf = include_bytes!("fixtures/expanded/chain-leaf.pem");
    let server = rtc_tls::server_config_from_pem(leaf, key).unwrap();
    assert!(
        exchange(
            server,
            rtc_tls::client_config_with_roots(roots(trust)).unwrap()
        )
        .await
        .is_err()
    );
    let wrong = [
        include_bytes!("fixtures/expanded/intermediate.pem").as_ref(),
        leaf.as_ref(),
    ]
    .concat();
    assert!(rtc_tls::server_config_from_pem(&wrong, key).is_err());
}

/// Wall-clock ceiling for one OpenSSL client/server cell, covering the accept leg.
const CELL_TIMEOUT: Duration = Duration::from_secs(15);

/// Grace given to the server task once the client process has already exited.
const SERVER_GRACE: Duration = Duration::from_secs(3);

/// Await both legs of one cell, naming the cell and the surviving evidence when a leg
/// cannot produce an outcome.
///
/// Bounding only the client leg is not enough. When the client process exits before the
/// accept leg can complete - a build that rejects the cipher or group string does exactly
/// that - the server task stays pending forever, and an unbounded `server.await` stalls
/// the whole matrix in silence. Run 45691070092 spent 44 of the job's 45 minutes on this
/// step, printing only "has been running for over 60 seconds", and the teardown listed no
/// OpenSSL process among the orphans it killed, so the client had already been gone for the
/// entire stall.
///
/// The legs carry a separate deadline because they carry different evidence. A client that
/// never returns is a hung process; a client that returns while the server leg is still
/// pending refused the stimulus, and its exit status and stderr are the only record of why.
/// Run 45700623620 is the first bounded verdict: `P256/TLS_AES_128_GCM_SHA256/P-256: no
/// outcome within 15s` in 15.50s, which names the cell but not the refusal.
async fn await_cell(
    label: &str,
    client: tokio::task::JoinHandle<std::process::Output>,
    server: tokio::task::JoinHandle<()>,
) -> Result<std::process::Output, String> {
    let client_leg = async move {
        client
            .await
            .map_err(|e| format!("{label}: client task failed: {e}"))
    };
    let out = match tokio::time::timeout(CELL_TIMEOUT, client_leg).await {
        Ok(Ok(out)) => out,
        Ok(Err(e)) => return Err(e),
        Err(_) => {
            return Err(format!(
                "{label}: the client produced no outcome within {}s",
                CELL_TIMEOUT.as_secs()
            ));
        }
    };
    let server_leg = async move {
        server
            .await
            .map_err(|e| format!("{label}: server task failed: {e}"))
    };
    match tokio::time::timeout(SERVER_GRACE, server_leg).await {
        Ok(Ok(())) => Ok(out),
        Ok(Err(e)) => Err(e),
        Err(_) => Err(format!(
            "{label}: the client exited with {} but the server leg never completed; stderr: {}",
            out.status,
            stderr_tail(&out.stderr)
        )),
    }
}

/// The tail of a client's stderr, trimmed, so a refusal stays quotable in one line.
fn stderr_tail(stderr: &[u8]) -> String {
    const KEEP: usize = 400;
    let text = String::from_utf8_lossy(stderr);
    if text.len() <= KEEP {
        return text.trim().to_string();
    }
    let mut start = text.len() - KEEP;
    while !text.is_char_boundary(start) {
        start += 1;
    }
    text[start..].trim().to_string()
}

#[tokio::test]
#[ignore = "requires independent OpenSSL 3 via RTC_TLS_OPENSSL"]
async fn openssl_client_verifies_rsa_ecdsa_ed25519_all_ciphers_and_key_exchanges() {
    use std::io::Write;
    use std::process::{Command, Stdio};
    let openssl = std::env::var_os("RTC_TLS_OPENSSL").expect("RTC_TLS_OPENSSL");
    let ca = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures/expanded/root-rsa.pem");
    for (ki, &(name, cert, key)) in KEYS.iter().enumerate() {
        for (si, (tls13, alg)) in [
            (true, "TLS_AES_128_GCM_SHA256"),
            (true, "TLS_AES_256_GCM_SHA384"),
            (true, "TLS_CHACHA20_POLY1305_SHA256"),
            (false, "AES128-GCM-SHA256"),
            (false, "AES256-GCM-SHA384"),
            (false, "CHACHA20-POLY1305"),
        ]
        .into_iter()
        .enumerate()
        {
            let (group, group_name) = [
                (rustls::NamedGroup::X25519, "X25519"),
                (rustls::NamedGroup::secp256r1, "P-256"),
                (rustls::NamedGroup::secp384r1, "P-384"),
            ][(ki + si) % 3];
            let mut provider = rtc_tls::crypto_provider();
            provider.kx_groups.retain(|g| g.name() == group);
            let config = rustls::ServerConfig::builder_with_provider(Arc::new(provider))
                .with_protocol_versions(&[&rustls::version::TLS13, &rustls::version::TLS12])
                .unwrap()
                .with_no_client_auth()
                .with_single_cert(
                    vec![CertificateDer::from_pem_slice(cert).unwrap()],
                    PrivateKeyDer::from_pem_slice(key).unwrap(),
                )
                .unwrap();
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let addr = listener.local_addr().unwrap();
            let server = tokio::spawn(async move {
                let (tcp, _) = listener.accept().await.unwrap();
                let mut tls = tokio_rustls::TlsAcceptor::from(Arc::new(config))
                    .accept(tcp)
                    .await
                    .unwrap();
                assert_eq!(
                    tls.get_ref().1.protocol_version(),
                    Some(if tls13 {
                        rustls::ProtocolVersion::TLSv1_3
                    } else {
                        rustls::ProtocolVersion::TLSv1_2
                    })
                );
                assert_eq!(
                    tls.get_ref()
                        .1
                        .negotiated_key_exchange_group()
                        .unwrap()
                        .name(),
                    group
                );
                let mut buf = [0; 4];
                tls.read_exact(&mut buf).await.unwrap();
                assert_eq!(&buf, b"ping");
                tls.write_all(b"pong").await.unwrap();
                tls.shutdown().await.unwrap();
            });
            let openssl = openssl.clone();
            let ca = ca.clone();
            let cipher = if tls13 {
                alg.to_string()
            } else {
                format!(
                    "ECDHE-{}-{alg}",
                    if name == "RSA" { "RSA" } else { "ECDSA" }
                )
            };
            // The client must offer the group the server keeps, and the leaf's own curve for
            // the ECDSA keys. OpenSSL 3.0 refuses a group list with a repeated entry
            // ("Call to SSL_CONF_cmd(-groups, P-256:P-256) failed") and exits before
            // connecting, while 3.6 and 4.1 accept it; the rotation makes (P256, si 0) and
            // (P384, si 1) land on their own curve, so the list is built only when the two
            // differ.
            let key_curve = match name {
                "P256" => "P-256",
                "P384" => "P-384",
                "P521" => "P-521",
                _ => "",
            };
            let group_list = if key_curve.is_empty() || key_curve == group_name {
                group_name.to_string()
            } else {
                format!("{group_name}:{key_curve}")
            };
            let client = tokio::task::spawn_blocking(move || {
                let mut c = Command::new(openssl);
                c.args([
                    "s_client",
                    "-quiet",
                    "-ign_eof",
                    "-verify_return_error",
                    "-connect",
                    &addr.to_string(),
                    "-servername",
                    "localhost",
                    "-verify_hostname",
                    "localhost",
                    "-CAfile",
                    ca.to_str().unwrap(),
                    "-groups",
                    &group_list,
                ]);
                c.arg(if tls13 { "-tls1_3" } else { "-tls1_2" })
                    .arg(if tls13 { "-ciphersuites" } else { "-cipher" })
                    .arg(cipher);
                let mut child = c
                    .stdin(Stdio::piped())
                    .stdout(Stdio::piped())
                    .stderr(Stdio::piped())
                    .spawn()
                    .unwrap();
                child.stdin.take().unwrap().write_all(b"ping").unwrap();
                child.wait_with_output().unwrap()
            });
            let out = await_cell(&format!("{name}/{alg}/{group_name}"), client, server)
                .await
                .unwrap_or_else(|e| panic!("{e}"));
            assert!(
                out.status.success(),
                "{name}/{alg}/{group_name}: {}",
                String::from_utf8_lossy(&out.stderr)
            );
            assert_eq!(out.stdout, b"pong");
        }
    }
}

/// The stall shape the CI hit: the client process is gone while the accept leg is still
/// being awaited, and only the client leg carries a deadline. An option every OpenSSL
/// build rejects reproduces it without depending on any particular cipher support.
///
/// The outer deadline here is longer than the per-cell one, so an `await_cell` that
/// leaves the accept leg unbounded fails this test rather than hanging it.
#[tokio::test]
#[ignore = "requires independent OpenSSL 3 via RTC_TLS_OPENSSL"]
async fn openssl_cell_without_a_connection_reports_a_named_stall() {
    use std::process::{Command, Stdio};
    let openssl = std::env::var_os("RTC_TLS_OPENSSL").expect("RTC_TLS_OPENSSL");
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        let (_tcp, _) = listener.accept().await.unwrap();
    });
    let connect = addr.to_string();
    let client = tokio::task::spawn_blocking(move || {
        Command::new(openssl)
            .args(["s_client", "-rtc-tls-no-such-option", "-connect", &connect])
            .stdin(Stdio::null())
            .output()
            .unwrap()
    });
    let label = "RSA/AES128-GCM-SHA256/X25519";
    let outer = CELL_TIMEOUT + SERVER_GRACE + Duration::from_secs(10);
    let outcome = tokio::time::timeout(outer, await_cell(label, client, server))
        .await
        .expect("the accept leg must be bounded by the cell deadline");
    let err = outcome.expect_err("a client that never connects must fail its cell, not hang it");
    assert!(err.contains(label), "the failure must name the cell: {err}");
    assert!(
        err.contains("server leg never completed"),
        "the failure must say which leg stalled: {err}"
    );
    assert!(
        err.contains("exit status"),
        "the failure must carry the client verdict that explains the stall: {err}"
    );
}

#[tokio::test]
#[ignore = "requires independent OpenSSL 3 server via RTC_TLS_OPENSSL"]
async fn openssl_rsa_server_is_verified_by_rust_https_client_tls12_and_tls13() {
    use std::process::{Command, Stdio};
    let openssl = std::env::var_os("RTC_TLS_OPENSSL").expect("RTC_TLS_OPENSSL");
    let fixture = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/expanded");
    for (version, cipher) in [
        ("-tls1_2", "ECDHE-RSA-AES128-GCM-SHA256"),
        ("-tls1_3", "TLS_AES_256_GCM_SHA384"),
    ] {
        let reserve = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = reserve.local_addr().unwrap();
        drop(reserve);
        let mut child = Command::new(&openssl)
            .args([
                "s_server",
                "-ign_eof",
                "-accept",
                &addr.to_string(),
                "-cert",
                fixture.join("rsa.pem").to_str().unwrap(),
                "-key",
                fixture.join("rsa-key.pem").to_str().unwrap(),
                version,
                if version == "-tls1_3" {
                    "-ciphersuites"
                } else {
                    "-cipher"
                },
                cipher,
            ])
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .spawn()
            .unwrap();
        struct Stop(std::process::Child);
        impl Drop for Stop {
            fn drop(&mut self) {
                let _ = self.0.kill();
                let _ = self.0.wait();
            }
        }
        // Read readiness without opening a sacrificial TCP connection: s_server
        // may otherwise consume the queued stdin response on that failed handshake.
        let stdout = child.stdout.take().unwrap();
        let (ready, wait_ready) = tokio::sync::oneshot::channel();
        let (request_seen, wait_request) = tokio::sync::oneshot::channel();
        let drain = tokio::task::spawn_blocking(move || {
            use std::io::BufRead;
            let mut ready = Some(ready);
            let mut request_seen = Some(request_seen);
            for line in std::io::BufReader::new(stdout).lines() {
                let Ok(line) = line else { break };
                if line.starts_with("POST ")
                    && let Some(seen) = request_seen.take()
                {
                    let _ = seen.send(());
                }
                if line.starts_with("ACCEPT")
                    && let Some(ready) = ready.take()
                {
                    let _ = ready.send(());
                }
            }
        });
        tokio::time::timeout(Duration::from_secs(5), wait_ready)
            .await
            .unwrap()
            .unwrap();
        assert!(child.try_wait().unwrap().is_none());
        let mut response_stdin = child.stdin.take().unwrap();
        let stop = Stop(child);
        let good = rtc_tls::http::HttpsClient::with_config(
            rtc_tls::client_config_with_roots(roots(ROOT)).unwrap(),
            Duration::from_secs(2),
            Duration::from_secs(5),
        );
        let endpoint = format!("https://localhost:{}/", addr.port());
        let request = good.post(&endpoint, hyper::HeaderMap::new(), bytes::Bytes::new());
        let respond = async {
            tokio::time::timeout(Duration::from_secs(5), wait_request)
                .await
                .unwrap()
                .unwrap();
            std::io::Write::write_all(
                &mut response_stdin,
                b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
            )
            .unwrap();
        };
        let (response, ()) = tokio::join!(request, respond);
        assert_eq!(response.unwrap().0, 200);
        let untrusted = rtc_tls::http::HttpsClient::with_config(
            rtc_tls::client_config_with_roots(rustls::RootCertStore::empty()).unwrap(),
            Duration::from_secs(2),
            Duration::from_secs(5),
        );
        assert!(
            untrusted
                .post(
                    &format!("https://localhost:{}/", addr.port()),
                    hyper::HeaderMap::new(),
                    bytes::Bytes::new()
                )
                .await
                .is_err()
        );
        let tcp = tokio::net::TcpStream::connect(addr).await.unwrap();
        assert!(
            tokio_rustls::TlsConnector::from(Arc::new(
                rtc_tls::client_config_with_roots(roots(ROOT)).unwrap()
            ))
            .connect(ServerName::try_from("wrong.example").unwrap(), tcp)
            .await
            .is_err()
        );
        drop(stop);
        drain.await.unwrap();
    }
}

#[test]
fn non_byte_aligned_rsa_2049_repeated_blinded_signatures_verify() {
    use rsa::pkcs8::DecodePublicKey;
    use rsa::signature::Verifier;
    let provider = rtc_tls::crypto_provider();
    let key = provider
        .key_provider
        .load_private_key(
            PrivateKeyDer::from_pem_slice(include_bytes!("fixtures/expanded/rsa-2049-key.pem"))
                .unwrap(),
        )
        .unwrap();
    let public =
        rsa::RsaPublicKey::from_public_key_der(key.public_key().unwrap().as_ref()).unwrap();
    use rsa::traits::PublicKeyParts;
    assert_eq!(public.n().bits(), 2049);
    for _ in 0..32 {
        let message = b"non-byte-aligned RSA signature regression";
        for scheme in [
            rustls::SignatureScheme::RSA_PKCS1_SHA256,
            rustls::SignatureScheme::RSA_PSS_SHA256,
        ] {
            let sig = key.choose_scheme(&[scheme]).unwrap().sign(message).unwrap();
            assert_eq!(sig.len(), 257);
            if scheme == rustls::SignatureScheme::RSA_PKCS1_SHA256 {
                rsa::pkcs1v15::VerifyingKey::<sha2::Sha256>::new(public.clone())
                    .verify(
                        message,
                        &rsa::pkcs1v15::Signature::try_from(sig.as_slice()).unwrap(),
                    )
                    .unwrap();
            } else {
                rsa::pss::VerifyingKey::<sha2::Sha256>::new(public.clone())
                    .verify(
                        message,
                        &rsa::pss::Signature::try_from(sig.as_slice()).unwrap(),
                    )
                    .unwrap();
            }
        }
    }
}

#[test]
#[ignore = "requires independent OpenSSL 3 via RTC_TLS_OPENSSL"]
fn openssl_verifies_non_byte_aligned_rsa_signatures_and_rejects_tampering() {
    let openssl = std::env::var_os("RTC_TLS_OPENSSL").expect("RTC_TLS_OPENSSL");
    let fixtures = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/expanded");
    let provider = rtc_tls::crypto_provider();
    let key = provider
        .key_provider
        .load_private_key(
            PrivateKeyDer::from_pem_slice(include_bytes!("fixtures/expanded/rsa-2049-key.pem"))
                .unwrap(),
        )
        .unwrap();
    let tmp = std::env::temp_dir().join(format!("livekit-rs-rsa-2049-{}", std::process::id()));
    std::fs::create_dir_all(&tmp).unwrap();
    struct Cleanup(std::path::PathBuf);
    impl Drop for Cleanup {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }
    let _cleanup = Cleanup(tmp.clone());
    use rustls::SignatureScheme as S;
    for (scheme, hash, padding) in [
        (S::RSA_PKCS1_SHA256, "-sha256", "pkcs1"),
        (S::RSA_PKCS1_SHA384, "-sha384", "pkcs1"),
        (S::RSA_PKCS1_SHA512, "-sha512", "pkcs1"),
        (S::RSA_PSS_SHA256, "-sha256", "pss"),
        (S::RSA_PSS_SHA384, "-sha384", "pss"),
        (S::RSA_PSS_SHA512, "-sha512", "pss"),
    ] {
        let mut signature = key
            .choose_scheme(&[scheme])
            .unwrap()
            .sign(include_bytes!("fixtures/expanded/rsa-message.bin"))
            .unwrap();
        for corrupt in [false, true] {
            if corrupt {
                signature[0] ^= 1;
            }
            let file = tmp.join("signature.bin");
            std::fs::write(&file, &signature).unwrap();
            let mut command = std::process::Command::new(&openssl);
            command
                .arg("dgst")
                .arg(hash)
                .arg("-verify")
                .arg(fixtures.join("rsa-2049-public.pem"))
                .arg("-signature")
                .arg(file)
                .arg("-sigopt")
                .arg(format!("rsa_padding_mode:{padding}"));
            if padding == "pss" {
                command.args(["-sigopt", "rsa_pss_saltlen:digest"]);
            }
            let result = command
                .arg(fixtures.join("rsa-message.bin"))
                .output()
                .unwrap();
            assert_eq!(
                result.status.success(),
                !corrupt,
                "{scheme:?}: {}",
                String::from_utf8_lossy(&result.stderr)
            );
        }
    }
}
