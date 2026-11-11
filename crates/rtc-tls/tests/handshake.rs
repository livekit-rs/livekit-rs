use rtc_tls::{rustls, tokio_rustls};
use rustls::pki_types::{CertificateDer, ServerName, pem::PemObject};
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

const CERT: &[u8] = include_bytes!("fixtures/server.pem");
const KEY: &[u8] = include_bytes!("fixtures/server-key.pem");
const SEC1_KEY: &[u8] = include_bytes!("fixtures/server-sec1-key.pem");
fn roots() -> rustls::RootCertStore {
    let mut roots = rustls::RootCertStore::empty();
    roots
        .add(CertificateDer::from_pem_slice(include_bytes!("fixtures/root.pem")).unwrap())
        .unwrap();
    roots
}

async fn exchange(name: &'static str, roots: rustls::RootCertStore) -> std::io::Result<()> {
    exchange_config(name, rtc_tls::client_config_with_roots(roots).unwrap()).await
}

async fn exchange_config(name: &'static str, client: rustls::ClientConfig) -> std::io::Result<()> {
    exchange_config_key(name, client, KEY).await
}

async fn exchange_config_key(
    name: &'static str,
    client: rustls::ClientConfig,
    key: &[u8],
) -> std::io::Result<()> {
    let server = Arc::new(rtc_tls::server_config_from_pem(CERT, key).unwrap());
    let client = Arc::new(client);
    let (a, b) = tokio::io::duplex(65536);
    let task = tokio::spawn(async move {
        let mut tls = tokio_rustls::TlsAcceptor::from(server).accept(a).await?;
        assert_eq!(
            tls.get_ref().1.protocol_version(),
            Some(rustls::ProtocolVersion::TLSv1_3)
        );
        let mut data = [0; 4];
        tls.read_exact(&mut data).await?;
        assert_eq!(&data, b"ping");
        tls.write_all(b"pong").await?;
        tls.get_mut().1.refresh_traffic_keys().unwrap();
        let mut next = [0; 4];
        tls.read_exact(&mut next).await?;
        assert_eq!(&next, b"next");
        tls.write_all(b"done").await?;
        tls.shutdown().await?;
        let mut remaining = Vec::new();
        tls.read_to_end(&mut remaining).await?;
        Ok::<_, std::io::Error>(())
    });
    let result = async {
        let mut tls = tokio_rustls::TlsConnector::from(client)
            .connect(ServerName::try_from(name).unwrap(), b)
            .await?;
        tls.write_all(b"ping").await?;
        let mut reply = [0; 4];
        tls.read_exact(&mut reply).await?;
        assert_eq!(&reply, b"pong");
        tls.get_mut().1.refresh_traffic_keys().unwrap();
        tls.write_all(b"next").await?;
        tls.read_exact(&mut reply).await?;
        assert_eq!(&reply, b"done");
        tls.shutdown().await?;
        let mut remaining = Vec::new();
        tls.read_to_end(&mut remaining).await?;
        Ok::<_, std::io::Error>(())
    }
    .await;
    if result.is_err() {
        task.abort();
    } else {
        task.await.unwrap()?;
    }
    result
}

#[tokio::test]
async fn verified_tls13_handshake_and_bidirectional_data() {
    tokio::time::timeout(
        std::time::Duration::from_secs(5),
        exchange("localhost", roots()),
    )
    .await
    .unwrap()
    .unwrap();
}
#[tokio::test]
async fn hostname_and_trust_are_verified() {
    assert!(exchange("wrong.example", roots()).await.is_err());
    assert!(
        exchange("localhost", rustls::RootCertStore::empty())
            .await
            .is_err()
    );
}
#[test]
fn mismatched_private_key_is_rejected_before_listen() {
    assert!(
        rtc_tls::server_config_from_pem(CERT, include_bytes!("fixtures/mismatch-key.pem")).is_err()
    );
}
#[test]
fn empty_and_unsupported_private_keys_are_rejected() {
    assert!(rtc_tls::server_config_from_pem(b"", KEY).is_err());
    assert!(
        rtc_tls::server_config_from_pem(
            CERT,
            b"-----BEGIN RSA PRIVATE KEY-----\nAA==\n-----END RSA PRIVATE KEY-----\n"
        )
        .is_err()
    );
}

#[tokio::test]
#[ignore = "requires an independent OpenSSL 3 client via RTC_TLS_OPENSSL"]
async fn independent_openssl_client_negotiates_tls13_and_verifies_certificate() {
    let openssl = std::env::var_os("RTC_TLS_OPENSSL").expect("set RTC_TLS_OPENSSL");
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let config = Arc::new(rtc_tls::server_config_from_pem(CERT, SEC1_KEY).unwrap());
    let server = tokio::spawn(async move {
        let (tcp, _) = listener.accept().await.unwrap();
        let mut tls = tokio_rustls::TlsAcceptor::from(config)
            .accept(tcp)
            .await
            .unwrap();
        let mut ping = [0; 4];
        tls.read_exact(&mut ping).await.unwrap();
        assert_eq!(&ping, b"ping");
        tls.write_all(b"pong").await.unwrap();
        tls.shutdown().await.unwrap();
    });
    let ca = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/root.pem");
    let output = tokio::task::spawn_blocking(move || {
        use std::io::Write;
        use std::process::{Command, Stdio};
        let mut child = Command::new(openssl)
            .args([
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
                "-tls1_3",
                "-groups",
                "P-256",
                "-ciphersuites",
                "TLS_AES_128_GCM_SHA256",
            ])
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap();
        child.stdin.take().unwrap().write_all(b"ping").unwrap();
        child.wait_with_output().unwrap()
    });
    let output = tokio::time::timeout(std::time::Duration::from_secs(10), output)
        .await
        .unwrap()
        .unwrap();
    server.await.unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert_eq!(output.stdout, b"pong");
}

#[derive(Debug)]
struct AfterCertificateExpiry;
impl rustls::time_provider::TimeProvider for AfterCertificateExpiry {
    fn current_time(&self) -> Option<rustls::pki_types::UnixTime> {
        Some(rustls::pki_types::UnixTime::since_unix_epoch(
            std::time::Duration::from_secs(2_500_000_000),
        ))
    }
}
#[tokio::test]
async fn expired_certificates_are_rejected() {
    let mut config = rtc_tls::client_config_with_roots(roots()).unwrap();
    config.time_provider = Arc::new(AfterCertificateExpiry);
    assert!(exchange_config("localhost", config).await.is_err());
}
#[test]
fn explicit_ca_file_is_loaded_and_missing_file_is_an_error() {
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/root.pem");
    assert!(rtc_tls::client_config_with_ca_file(&root).is_ok());
    assert!(rtc_tls::client_config_with_ca_file(&root.with_extension("absent")).is_err());
}

async fn chain_handshake(cert: Vec<u8>, root: &[u8]) -> Result<(), String> {
    let key = rustls::pki_types::PrivateKeyDer::from_pem_slice(KEY).unwrap();
    let server = rustls::ServerConfig::builder_with_provider(Arc::new(rtc_tls::crypto_provider()))
        .with_protocol_versions(&[&rustls::version::TLS13])
        .unwrap()
        .with_no_client_auth()
        .with_single_cert(vec![CertificateDer::from(cert)], key)
        .unwrap();
    let mut roots = rustls::RootCertStore::empty();
    roots
        .add(CertificateDer::from_pem_slice(root).unwrap())
        .unwrap();
    let client = rtc_tls::client_config_with_roots(roots).unwrap();
    let (a, b) = tokio::io::duplex(65536);
    let acceptor = tokio_rustls::TlsAcceptor::from(Arc::new(server));
    let connector = tokio_rustls::TlsConnector::from(Arc::new(client));
    let (server, client) = tokio::join!(
        acceptor.accept(a),
        connector.connect(ServerName::try_from("localhost").unwrap(), b)
    );
    server.map_err(|e| e.to_string())?;
    client.map_err(|e| e.to_string())?;
    Ok(())
}
#[tokio::test]
async fn p384_sha384_issuer_chain_is_verified_with_p256_endpoint_key() {
    let cert =
        CertificateDer::from_pem_slice(include_bytes!("fixtures/p384-chain-server.pem")).unwrap();
    tokio::time::timeout(
        std::time::Duration::from_secs(5),
        chain_handshake(cert.to_vec(), include_bytes!("fixtures/p384-root.pem")),
    )
    .await
    .unwrap()
    .unwrap();
}
#[tokio::test]
async fn tampered_certificate_signature_is_rejected() {
    let mut cert = CertificateDer::from_pem_slice(CERT).unwrap().to_vec();
    *cert.last_mut().unwrap() ^= 1;
    assert!(
        tokio::time::timeout(
            std::time::Duration::from_secs(5),
            chain_handshake(cert, include_bytes!("fixtures/root.pem"))
        )
        .await
        .unwrap()
        .is_err()
    );
}

#[tokio::test]
async fn sec1_key_supports_verified_tls13_and_bidirectional_data() {
    tokio::time::timeout(
        std::time::Duration::from_secs(5),
        exchange_config_key(
            "localhost",
            rtc_tls::client_config_with_roots(roots()).unwrap(),
            SEC1_KEY,
        ),
    )
    .await
    .unwrap()
    .unwrap();
}

#[test]
fn sec1_and_pkcs8_encodings_produce_the_same_public_key_and_signature() {
    use rustls::pki_types::PrivateKeyDer;
    let provider = rtc_tls::crypto_provider();
    let pkcs8 = provider
        .key_provider
        .load_private_key(PrivateKeyDer::from_pem_slice(KEY).unwrap())
        .unwrap();
    let sec1 = provider
        .key_provider
        .load_private_key(PrivateKeyDer::from_pem_slice(SEC1_KEY).unwrap())
        .unwrap();
    assert_eq!(pkcs8.public_key(), sec1.public_key());
    let schemes = [rustls::SignatureScheme::ECDSA_NISTP256_SHA256];
    let message = b"SEC1 and PKCS8 identify the same TLS signing key";
    assert_eq!(
        pkcs8
            .choose_scheme(&schemes)
            .unwrap()
            .sign(message)
            .unwrap(),
        sec1.choose_scheme(&schemes).unwrap().sign(message).unwrap()
    );
}

#[test]
fn sec1_rejects_other_curves_malformed_der_and_inconsistent_public_key() {
    use rustls::pki_types::{PrivateKeyDer, PrivateSec1KeyDer};
    let provider = rtc_tls::crypto_provider();
    let p384 = PrivateKeyDer::from_pem_slice(include_bytes!("fixtures/p384-sec1-key.pem")).unwrap();
    assert!(provider.key_provider.load_private_key(p384).is_ok());
    assert!(
        rtc_tls::server_config_from_pem(CERT, include_bytes!("fixtures/p384-sec1-key.pem"))
            .is_err()
    );
    let key = PrivateKeyDer::from_pem_slice(SEC1_KEY).unwrap();
    let der = key.secret_der();
    for bad in [vec![0x30, 0x00], der[..der.len() - 1].to_vec(), {
        let mut changed = der.to_vec();
        *changed.last_mut().unwrap() ^= 1;
        changed
    }] {
        assert!(
            provider
                .key_provider
                .load_private_key(PrivateKeyDer::Sec1(PrivateSec1KeyDer::from(bad)))
                .is_err()
        );
    }
}
