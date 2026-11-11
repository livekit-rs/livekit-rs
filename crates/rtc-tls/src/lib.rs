//! Explicit RustCrypto TLS 1.2/1.3 configuration for TURN, HTTPS and WSS.
//!
//! AEAD suites: AES-GCM and ChaCha20-Poly1305. Key exchange: X25519/P256/P384.
//! RSA, ECDSA P256/P384/P521 and Ed25519 keys/certificates are supported within
//! the tested matrix. RSA private signatures use independent stable arithmetic;
//! the rsa dependency remains public-only. WebPKI verifies every peer chain.

mod expanded;
mod expanded_aead;
pub mod http;
mod provider;
mod quic;
#[cfg(feature = "quinn")]
pub mod quinn;
mod roots;
mod rsa_sign;
mod rsa_verify;

use std::path::Path;
use std::sync::Arc;

use rustls::RootCertStore;
use rustls_pki_types::pem::PemObject;
use rustls_pki_types::{CertificateDer, PrivateKeyDer};

pub use provider::crypto_provider;
pub use rustls;
pub use tokio_rustls;

#[derive(Debug, thiserror::Error)]
pub enum TlsError {
    #[error("could not read TLS certificate/key: {0}")]
    Io(#[from] std::io::Error),
    #[error("invalid TLS PEM: {0}")]
    Pem(String),
    #[error("TLS configuration error: {0}")]
    Config(#[from] rustls::Error),
}

/// Construct a verified client with the configured OS/PEM trust-anchor sources.
/// Unsupported signature algorithms fail closed; no dangerous verifier is used.
pub fn client_config() -> Result<rustls::ClientConfig, TlsError> {
    client_config_with_roots(roots::load()?)
}

fn mozilla_roots() -> RootCertStore {
    RootCertStore::from_iter(webpki_roots::TLS_SERVER_ROOTS.iter().cloned())
}

fn append_roots_from_file(
    mut roots: RootCertStore,
    path: &Path,
) -> Result<RootCertStore, TlsError> {
    let pem = std::fs::read(path)?;
    let certificates = CertificateDer::pem_slice_iter(&pem)
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| TlsError::Pem(e.to_string()))?;
    if certificates.is_empty() {
        return Err(TlsError::Pem(format!(
            "SSL_CERT_FILE {} contains no certificates",
            path.display()
        )));
    }
    for certificate in certificates {
        roots.add(certificate)?;
    }
    Ok(roots)
}

/// Explicit CA-file plus frozen Mozilla roots, useful for embedding/tests.
pub fn client_config_with_ca_file(path: &Path) -> Result<rustls::ClientConfig, TlsError> {
    client_config_with_roots(append_roots_from_file(mozilla_roots(), path)?)
}

pub fn client_config_with_roots(roots: RootCertStore) -> Result<rustls::ClientConfig, TlsError> {
    Ok(
        rustls::ClientConfig::builder_with_provider(Arc::new(crypto_provider()))
            .with_protocol_versions(&[&rustls::version::TLS13, &rustls::version::TLS12])?
            .with_go_root_certificates(roots)
            .with_no_client_auth(),
    )
}

/// Verified client from an explicit PEM root bundle and an optional mTLS client
/// key pair. `roots_pem` must contain at least one parsable certificate when
/// `Some`; `None` uses the system trust anchors, matching Go's `tls.Config`
/// with an empty `RootCAs`. A present-but-empty client pair fails closed via
/// rustls, mirroring Go's `tls.LoadX509KeyPair`. Used by the SPEC-0074 C10
/// Redis TLS path, where `xtls.ServerName` requires a client whose
/// SNI/verification name is set independently of the dial address (redis-rs
/// binds SNI to the address host).
pub fn verified_client_config(
    roots_pem: Option<&[u8]>,
    client_cert_pem: Option<&[u8]>,
    client_key_pem: Option<&[u8]>,
) -> Result<rustls::ClientConfig, TlsError> {
    let roots = match roots_pem {
        Some(pem) => {
            let mut roots = RootCertStore::empty();
            let mut count = 0;
            for certificate in CertificateDer::pem_slice_iter(pem) {
                roots.add(certificate.map_err(|e| TlsError::Pem(e.to_string()))?)?;
                count += 1;
            }
            if count == 0 {
                return Err(TlsError::Pem(
                    "no certificates found in the provided CA bundle".into(),
                ));
            }
            roots
        }
        None => roots::load()?,
    };
    let builder = rustls::ClientConfig::builder_with_provider(Arc::new(crypto_provider()))
        .with_protocol_versions(&[&rustls::version::TLS13, &rustls::version::TLS12])?;
    if let (Some(cert), Some(key)) = (client_cert_pem, client_key_pem) {
        let certs = CertificateDer::pem_slice_iter(cert)
            .collect::<Result<Vec<_>, _>>()
            .map_err(|e| TlsError::Pem(e.to_string()))?;
        if certs.is_empty() {
            return Err(TlsError::Pem("client certificate chain is empty".into()));
        }
        let key = PrivateKeyDer::from_pem_slice(key).map_err(|e| TlsError::Pem(e.to_string()))?;
        Ok(builder
            .with_go_root_certificates(roots)
            .with_client_auth_cert(certs, key)?)
    } else {
        Ok(builder
            .with_go_root_certificates(roots)
            .with_no_client_auth())
    }
}

/// `xtls.Config.Insecure` → Go's `tls.Config{InsecureSkipVerify: true}`: a real
/// TLS 1.2/1.3 handshake whose peer certificate is deliberately not verified, and
/// whose client certificate (when configured) is still sent. Only a configuration
/// that asks for it reaches this; [`verified_client_config`] never uses the
/// skip-verify peer.
pub fn insecure_client_config(
    client_cert_pem: Option<&[u8]>,
    client_key_pem: Option<&[u8]>,
) -> Result<rustls::ClientConfig, TlsError> {
    let builder = rustls::ClientConfig::builder_with_provider(Arc::new(crypto_provider()))
        .with_protocol_versions(&[&rustls::version::TLS13, &rustls::version::TLS12])?
        .dangerous()
        .with_custom_certificate_verifier(Arc::new(SkipPeerVerify));
    with_optional_client_cert(builder, client_cert_pem, client_key_pem)
}

/// Attaches the `xtls` client key pair when both halves are present, matching
/// `tls.LoadX509KeyPair` (a cert without a key is not a usable pair, so Go sends
/// no client certificate and the server decides whether that is fatal).
fn with_optional_client_cert(
    builder: rustls::ConfigBuilder<rustls::ClientConfig, rustls::client::WantsClientCert>,
    client_cert_pem: Option<&[u8]>,
    client_key_pem: Option<&[u8]>,
) -> Result<rustls::ClientConfig, TlsError> {
    if let (Some(cert), Some(key)) = (client_cert_pem, client_key_pem) {
        let certs = CertificateDer::pem_slice_iter(cert)
            .collect::<Result<Vec<_>, _>>()
            .map_err(|e| TlsError::Pem(e.to_string()))?;
        if certs.is_empty() {
            return Err(TlsError::Pem("client certificate chain is empty".into()));
        }
        let key = PrivateKeyDer::from_pem_slice(key).map_err(|e| TlsError::Pem(e.to_string()))?;
        return Ok(builder.with_client_auth_cert(certs, key)?);
    }
    Ok(builder.with_no_client_auth())
}

/// The verifier behind [`insecure_client_config`]; performs no WebPKI work.
#[derive(Debug)]
struct SkipPeerVerify;

impl rustls::client::danger::ServerCertVerifier for SkipPeerVerify {
    fn verify_server_cert(
        &self,
        _end_entity: &CertificateDer<'_>,
        _intermediates: &[CertificateDer<'_>],
        _server_name: &rustls::pki_types::ServerName<'_>,
        _ocsp_response: &[u8],
        _now: rustls::pki_types::UnixTime,
    ) -> Result<rustls::client::danger::ServerCertVerified, rustls::Error> {
        Ok(rustls::client::danger::ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        _message: &[u8],
        _cert: &CertificateDer<'_>,
        _dss: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        Ok(rustls::client::danger::HandshakeSignatureValid::assertion())
    }

    fn verify_tls13_signature(
        &self,
        _message: &[u8],
        _cert: &CertificateDer<'_>,
        _dss: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        Ok(rustls::client::danger::HandshakeSignatureValid::assertion())
    }

    fn supported_verify_schemes(&self) -> Vec<rustls::SignatureScheme> {
        crypto_provider()
            .signature_verification_algorithms
            .supported_schemes()
    }
}

/// Load a leaf-first PEM chain and a supported PKCS#1/PKCS#8/SEC1 private key.
/// rustls checks key/leaf compatibility before a listener can be started.
pub fn server_config(cert: &Path, key: &Path) -> Result<rustls::ServerConfig, TlsError> {
    let key = zeroize::Zeroizing::new(std::fs::read(key)?);
    server_config_from_pem(&std::fs::read(cert)?, &key)
}

pub fn server_config_from_pem(cert: &[u8], key: &[u8]) -> Result<rustls::ServerConfig, TlsError> {
    let chain = CertificateDer::pem_slice_iter(cert)
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| TlsError::Pem(e.to_string()))?;
    if chain.is_empty() {
        return Err(TlsError::Pem("certificate chain is empty".into()));
    }
    let key = PrivateKeyDer::from_pem_slice(key).map_err(|e| TlsError::Pem(e.to_string()))?;
    Ok(
        rustls::ServerConfig::builder_with_provider(Arc::new(crypto_provider()))
            .with_protocol_versions(&[&rustls::version::TLS13, &rustls::version::TLS12])?
            .with_no_client_auth()
            .with_single_cert(chain, key)?,
    )
}
