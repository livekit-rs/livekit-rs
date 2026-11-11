//! Self-signed ECDSA P-256 certificate generation for DTLS, pure Rust.
//!
//! Replaces `dimpl::certificate::generate_self_signed_certificate`, which needs the
//! `rcgen` feature and therefore aws-lc-rs. WebRTC peers authenticate the certificate
//! through the SDP fingerprint. The certificate still needs digitalSignature key
//! usage for browser DTLS authentication; it is an endpoint certificate, not a CA.

use std::str::FromStr;
use std::time::Duration;

use p256::ecdsa::{DerSignature, SigningKey};
use p256::pkcs8::EncodePrivateKey;
use x509_cert::builder::profile::BuilderProfile;
use x509_cert::builder::{Builder, CertificateBuilder};
use x509_cert::certificate::TbsCertificate;
use x509_cert::der::Encode;
use x509_cert::ext::pkix::{BasicConstraints, KeyUsage, KeyUsages};
use x509_cert::ext::{Extension, ToExtension};
use x509_cert::name::Name;
use x509_cert::serial_number::SerialNumber;
use x509_cert::spki::{SubjectPublicKeyInfo, SubjectPublicKeyInfoRef};
use x509_cert::time::Validity;

/// Self-signed WebRTC endpoint profile. CA/Browser Forum root profiles grant
/// certificate-signing permission instead of the digital signatures DTLS needs.
struct DtlsEndpointProfile {
    subject: Name,
}

impl BuilderProfile for DtlsEndpointProfile {
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
        Ok(extensions)
    }
}

/// DER encoded certificate and PKCS#8 private key.
#[derive(Clone)]
pub struct GeneratedCert {
    pub certificate: Vec<u8>,
    pub private_key: Vec<u8>,
}

impl std::fmt::Debug for GeneratedCert {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GeneratedCert")
            .field("certificate_len", &self.certificate.len())
            .finish_non_exhaustive()
    }
}

#[derive(Debug, thiserror::Error)]
pub enum CertError {
    #[error("os random source failed: {0}")]
    Random(String),
    #[error("certificate encoding failed: {0}")]
    Encode(String),
}

const VALIDITY: Duration = Duration::from_secs(365 * 24 * 3600);
const KEY_ATTEMPTS: usize = 8;

/// Generate a fresh self-signed P-256 certificate valid for one year.
pub fn generate_self_signed() -> Result<GeneratedCert, CertError> {
    let signing_key = random_signing_key()?;

    // Serial must be unique across processes and machines (Firefox, str0m#517),
    // positive and non-zero in its first octet.
    let mut serial = [0u8; 16];
    fill_random(&mut serial)?;
    serial[0] = (serial[0] & 0x7f) | 0x01;
    let serial = SerialNumber::new(&serial).map_err(enc)?;

    let validity = Validity::from_now(VALIDITY).map_err(enc)?;
    let subject = Name::from_str("CN=livecloud DTLS,O=livekit-rs,C=US").map_err(enc)?;
    let spki = SubjectPublicKeyInfo::from_key(signing_key.verifying_key()).map_err(enc)?;
    let profile = DtlsEndpointProfile { subject };

    let builder = CertificateBuilder::new(profile, serial, validity, spki).map_err(enc)?;
    let cert = builder
        .build::<_, DerSignature>(&signing_key)
        .map_err(enc)?;

    let certificate = cert.to_der().map_err(enc)?;
    let private_key = signing_key.to_pkcs8_der().map_err(enc)?.as_bytes().to_vec();

    Ok(GeneratedCert {
        certificate,
        private_key,
    })
}

fn random_signing_key() -> Result<SigningKey, CertError> {
    // A uniformly random 32-byte string is a valid scalar with overwhelming
    // probability; retry the rare out-of-range or zero value.
    for _ in 0..KEY_ATTEMPTS {
        let mut bytes = [0u8; 32];
        fill_random(&mut bytes)?;
        if let Ok(key) = SigningKey::from_slice(&bytes) {
            return Ok(key);
        }
    }
    Err(CertError::Random(
        "could not derive a valid P-256 scalar".into(),
    ))
}

fn fill_random(buf: &mut [u8]) -> Result<(), CertError> {
    getrandom::fill(buf).map_err(|e| CertError::Random(e.to_string()))
}

fn enc<E: std::fmt::Display>(e: E) -> CertError {
    CertError::Encode(e.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn generates_distinct_der_certificates() {
        let a = generate_self_signed().unwrap();
        let b = generate_self_signed().unwrap();
        assert!(a.certificate.len() > 100);
        assert_ne!(a.certificate, b.certificate);
        assert_ne!(a.private_key, b.private_key);
    }

    #[test]
    fn dtls_certificate_allows_endpoint_signatures_and_cannot_sign_certificates() {
        use x509_cert::der::Decode;
        let generated = generate_self_signed().unwrap();
        let certificate = x509_cert::Certificate::from_der(&generated.certificate).unwrap();
        let tbs = certificate.tbs_certificate();
        let (critical, usage) = tbs.get_extension::<KeyUsage>().unwrap().unwrap();
        assert!(critical);
        assert!(
            usage.digital_signature(),
            "browser DTLS rejects KEY_USAGE_BIT_INCORRECT otherwise"
        );
        assert!(!usage.key_cert_sign());
        assert!(!usage.crl_sign());
        let (_, constraints) = tbs.get_extension::<BasicConstraints>().unwrap().unwrap();
        assert!(!constraints.ca);
        assert_eq!(constraints.path_len_constraint, None);
    }

    #[test]
    fn private_key_is_loadable_by_dimpl_rust_crypto() {
        let c = generate_self_signed().unwrap();
        let cert = dimpl::DtlsCertificate {
            certificate: c.certificate,
            private_key: c.private_key,
        };
        let config = std::sync::Arc::new(dimpl::Config::builder().build().unwrap());
        // Construction parses the private key; a parse failure would panic/err here.
        let _ = dimpl::Dtls::new_12(config, cert, std::time::Instant::now());
    }
}
