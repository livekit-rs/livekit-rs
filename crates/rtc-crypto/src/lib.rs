//! Pure-Rust crypto provider for str0m (TASK-CRYPTO-01).
//!
//! DTLS via dimpl with only its `rust-crypto` feature, SRTP/HMAC/SHA via RustCrypto,
//! and certificates generated with p256 + x509-cert instead of rcgen. Nothing here
//! enables ring, aws-lc-rs or OpenSSL; the production feature tree must be checked
//! with `cargo tree -e features` before release (DEP-06).

pub mod cert;
mod dtls;
mod sha1;
mod sha256;
mod srtp;

use dtls::RustCryptoDtlsProvider;
use sha1::RustCryptoSha1HmacProvider;
use sha256::RustCryptoSha256Provider;
use srtp::RustCryptoSrtpProvider;
use str0m_proto::crypto::CryptoProvider;
use str0m_proto::crypto::dtls::DtlsCert;

/// Build the crypto provider. Pass it to `RtcConfig::set_crypto_provider`.
pub fn provider() -> CryptoProvider {
    static SRTP: RustCryptoSrtpProvider = RustCryptoSrtpProvider;
    static SHA1_HMAC: RustCryptoSha1HmacProvider = RustCryptoSha1HmacProvider;
    static SHA256: RustCryptoSha256Provider = RustCryptoSha256Provider;
    static DTLS: RustCryptoDtlsProvider = RustCryptoDtlsProvider;

    CryptoProvider {
        srtp_provider: &SRTP,
        sha1_hmac_provider: &SHA1_HMAC,
        sha256_provider: &SHA256,
        dtls_provider: &DTLS,
    }
}

/// Generate a DTLS certificate for `RtcConfig::set_dtls_cert`.
pub fn dtls_cert() -> Result<DtlsCert, cert::CertError> {
    let c = cert::generate_self_signed()?;
    Ok(DtlsCert {
        certificate: c.certificate,
        private_key: c.private_key,
    })
}
