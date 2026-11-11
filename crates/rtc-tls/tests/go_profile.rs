//! Full-verification Go role profile. No listening socket is needed for these
//! source-level verdict controls; actual TLS consumers/alerts have a separate
//! external owned-runner gate.
use rtc_tls::rustls;
use rustls::client::{WebPkiServerVerifier, danger::ServerCertVerifier};
use rustls::pki_types::{CertificateDer, ServerName, UnixTime, pem::PemObject};
use std::{sync::Arc, time::Duration};
const CA_LEAF: &[u8] = include_bytes!("fixtures/go-profile/ca-leaf.pem");
fn certificate(pem: &[u8]) -> CertificateDer<'static> {
    CertificateDer::from_pem_slice(pem).unwrap().into_owned()
}
fn roots(pem: &[u8]) -> rustls::RootCertStore {
    let mut roots = rustls::RootCertStore::empty();
    roots.add(certificate(pem)).unwrap();
    roots
}
fn verifier(pem: &[u8], go: bool) -> Arc<WebPkiServerVerifier> {
    let builder = WebPkiServerVerifier::builder_with_provider(
        Arc::new(roots(pem)),
        Arc::new(rtc_tls::crypto_provider()),
    );
    if go {
        builder.with_go_certificate_policy().build().unwrap()
    } else {
        builder.build().unwrap()
    }
}
fn now() -> UnixTime {
    UnixTime::since_unix_epoch(Duration::from_secs(1_830_297_600))
}
fn verify(
    verifier: &dyn ServerCertVerifier,
    leaf: &CertificateDer<'_>,
    chain: &[CertificateDer<'_>],
    name: &str,
    time: UnixTime,
) -> Result<(), rustls::Error> {
    verifier
        .verify_server_cert(
            leaf,
            chain,
            &ServerName::try_from(name.to_owned()).unwrap(),
            &[],
            time,
        )
        .map(|_| ())
}
#[test]
fn go_role_profile_accepts_trusted_ca_leaf_but_default_webpki_remains_strict() {
    let leaf = certificate(CA_LEAF);
    let strict = verifier(CA_LEAF, false);
    let error = verify(strict.as_ref(), &leaf, &[], "localhost", now()).unwrap_err();
    assert!(error.to_string().contains("CaUsedAsEndEntity"), "{error:?}");
    assert_eq!(strict.certificate_verification_alert(), None);
    let go = verifier(CA_LEAF, true);
    verify(go.as_ref(), &leaf, &[], "localhost", now()).unwrap();
    assert_eq!(
        go.certificate_verification_alert(),
        Some(rustls::AlertDescription::BadCertificate)
    );
}
#[test]
fn go_role_profile_keeps_signature_name_time_purpose_issuer_and_constraints_checks() {
    let go = verifier(CA_LEAF, true);
    let leaf = certificate(CA_LEAF);
    assert!(verify(go.as_ref(), &leaf, &[], "wrong.example", now()).is_err());
    assert!(
        verify(
            go.as_ref(),
            &leaf,
            &[],
            "localhost",
            UnixTime::since_unix_epoch(Duration::from_secs(4_102_444_800))
        )
        .is_err()
    );
    let mut corrupt = leaf.as_ref().to_vec();
    *corrupt.last_mut().unwrap() ^= 1;
    assert!(
        verify(
            go.as_ref(),
            &CertificateDer::from(corrupt),
            &[],
            "localhost",
            now()
        )
        .is_err()
    );
    let unrelated = verifier(include_bytes!("fixtures/root.pem"), true);
    assert!(verify(unrelated.as_ref(), &leaf, &[], "localhost", now()).is_err());
    let client_only = include_bytes!("fixtures/go-profile/ca-leaf-client-only.pem");
    assert!(
        verify(
            verifier(client_only, true).as_ref(),
            &certificate(client_only),
            &[],
            "localhost",
            now()
        )
        .is_err()
    );
    assert!(
        verify(
            go.as_ref(),
            &certificate(include_bytes!("fixtures/go-profile/through-nonca.pem")),
            &[certificate(include_bytes!(
                "fixtures/go-profile/nonca-issuer.pem"
            ))],
            "localhost",
            now()
        )
        .is_err()
    );
    assert!(
        verify(
            go.as_ref(),
            &certificate(include_bytes!(
                "fixtures/go-profile/outside-name-constraint.pem"
            )),
            &[certificate(include_bytes!(
                "fixtures/go-profile/constrained-issuer.pem"
            ))],
            "localhost",
            now()
        )
        .is_err()
    );
}

#[derive(Debug)]
struct FixedTime(UnixTime);
impl rustls::time_provider::TimeProvider for FixedTime {
    fn current_time(&self) -> Option<UnixTime> {
        Some(self.0)
    }
}
#[tokio::test]
#[ignore = "requires owned external TLS peer from tls-go-certificate-profile/run-owned.py"]
async fn external_go_profile_client_consumer() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let address = std::env::var("C07_ADDRESS").expect("owned peer address");
    let ca = std::fs::read(std::env::var("C07_CA").expect("CA input")).unwrap();
    let name = std::env::var("C07_NAME").unwrap();
    let mode = std::env::var("C07_PROFILE_MODE").unwrap();
    let expected = std::env::var("C07_EXPECT_ACCEPT").unwrap() == "true";
    let mut client = if mode == "go" {
        rtc_tls::verified_client_config(Some(&ca), None, None).unwrap()
    } else {
        rustls::ClientConfig::builder_with_provider(Arc::new(rtc_tls::crypto_provider()))
            .with_protocol_versions(&[&rustls::version::TLS13, &rustls::version::TLS12])
            .unwrap()
            .with_root_certificates(roots(&ca))
            .with_no_client_auth()
    };
    let seconds = std::env::var("C07_TIME").unwrap().parse::<u64>().unwrap();
    client.time_provider = Arc::new(FixedTime(UnixTime::since_unix_epoch(Duration::from_secs(
        seconds,
    ))));
    let result = tokio::time::timeout(Duration::from_secs(10), async {
        let tcp = tokio::net::TcpStream::connect(address).await?;
        let mut tls = rtc_tls::tokio_rustls::TlsConnector::from(Arc::new(client))
            .connect(ServerName::try_from(name).unwrap(), tcp)
            .await?;
        tls.write_all(b"ping").await?;
        let mut reply = [0; 4];
        tls.read_exact(&mut reply).await?;
        assert_eq!(&reply, b"pong");
        // Standard Go witness defers Close and ignores its teardown error.
        // The verified handshake plus actual ping/pong already established the
        // verdict. Keep any teardown failure separate and observable.
        if let Err(error) = tls.shutdown().await {
            let encoded = error
                .to_string()
                .as_bytes()
                .iter()
                .map(|value| format!("{value:02x}"))
                .collect::<String>();
            println!("C07_CLOSE_ERROR:{encoded}");
        }
        Ok::<_, std::io::Error>(())
    })
    .await
    .expect("bounded actual TLS consumer");
    let accepted = result.is_ok();
    let error = result
        .err()
        .map(|error| error.to_string())
        .unwrap_or_default();
    let encoded = error
        .as_bytes()
        .iter()
        .map(|value| format!("{value:02x}"))
        .collect::<String>();
    println!(
        "C07_RESULT:{}:{}",
        if accepted { "accepted" } else { "rejected" },
        encoded
    );
    assert_eq!(accepted, expected, "actual {mode} verification: {error}");
}
