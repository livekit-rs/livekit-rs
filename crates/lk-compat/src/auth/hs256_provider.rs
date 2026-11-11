//! HS256-only jsonwebtoken provider backed by the existing RustCrypto HMAC/SHA2
//! dependencies. Keeping the public provider boundary avoids pulling unused RSA,
//! ECDSA and EdDSA implementations into an HMAC-only access-token signer.

use std::sync::OnceLock;

use hmac::{Hmac, Mac};
use jsonwebtoken::crypto::{CryptoProvider, JwtSigner, JwtVerifier, KeyUtils};
use jsonwebtoken::errors::{ErrorKind, Result};
use jsonwebtoken::signature::{self, Signer, Verifier};
use jsonwebtoken::{Algorithm, AlgorithmFamily, DecodingKey, EncodingKey};
use sha2::Sha256;

struct Hs256(Hmac<Sha256>);

impl Signer<Vec<u8>> for Hs256 {
    fn try_sign(&self, message: &[u8]) -> std::result::Result<Vec<u8>, signature::Error> {
        let mut mac = self.0.clone();
        mac.update(message);
        Ok(mac.finalize().into_bytes().to_vec())
    }
}

impl JwtSigner for Hs256 {
    fn algorithm(&self) -> Algorithm {
        Algorithm::HS256
    }
}

impl Verifier<Vec<u8>> for Hs256 {
    fn verify(
        &self,
        message: &[u8],
        signature: &Vec<u8>,
    ) -> std::result::Result<(), signature::Error> {
        let mut mac = self.0.clone();
        mac.update(message);
        // RustCrypto performs the authentication-tag comparison in constant time.
        mac.verify_slice(signature)
            .map_err(signature::Error::from_source)
    }
}

impl JwtVerifier for Hs256 {
    fn algorithm(&self) -> Algorithm {
        Algorithm::HS256
    }
}

fn signer(algorithm: &Algorithm, key: &EncodingKey) -> Result<Box<dyn JwtSigner>> {
    if *algorithm != Algorithm::HS256 {
        return Err(ErrorKind::InvalidAlgorithm.into());
    }
    if key.family() != AlgorithmFamily::Hmac {
        return Err(ErrorKind::InvalidKeyFormat.into());
    }
    let mac =
        Hmac::<Sha256>::new_from_slice(key.as_bytes()).map_err(|_| ErrorKind::InvalidKeyFormat)?;
    Ok(Box::new(Hs256(mac)))
}

fn verifier(algorithm: &Algorithm, key: &DecodingKey) -> Result<Box<dyn JwtVerifier>> {
    if *algorithm != Algorithm::HS256 {
        return Err(ErrorKind::InvalidAlgorithm.into());
    }
    if key.family() != AlgorithmFamily::Hmac {
        return Err(ErrorKind::InvalidKeyFormat.into());
    }
    let mac = Hmac::<Sha256>::new_from_slice(key.try_get_as_bytes()?)
        .map_err(|_| ErrorKind::InvalidKeyFormat)?;
    Ok(Box::new(Hs256(mac)))
}

static PROVIDER: CryptoProvider = CryptoProvider {
    signer_factory: signer,
    verifier_factory: verifier,
    // This server signs shared-secret tokens; asymmetric key extraction/JWK
    // digests are outside this provider. Return errors instead of dummy panics.
    key_utils: KeyUtils {
        rsa_pub_components_from_private_key: |_| Err(ErrorKind::InvalidAlgorithm.into()),
        rsa_pub_components_from_public_key: |_| Err(ErrorKind::InvalidAlgorithm.into()),
        ec_pub_components_from_private_key: |_, _| Err(ErrorKind::InvalidAlgorithm.into()),
        ed_pub_components_from_private_key: |_, _| Err(ErrorKind::InvalidAlgorithm.into()),
        compute_digest: |_, _| Err(ErrorKind::InvalidAlgorithm.into()),
    },
};

/// Initialize lazily at the signing boundary so callers and isolated unit tests
/// need no startup hook. jsonwebtoken's global provider cannot be replaced. If a
/// host installed another provider first, fail explicitly rather than signing
/// through an unreviewed implementation or triggering jsonwebtoken's dummy panic.
pub(super) fn ensure_installed() -> Result<()> {
    static INSTALLED: OnceLock<bool> = OnceLock::new();
    if *INSTALLED.get_or_init(|| PROVIDER.install_default().is_ok()) {
        Ok(())
    } else {
        Err(ErrorKind::Provider(
            "LiveKit HS256 provider cannot be installed: a jsonwebtoken provider was already selected".into(),
        ).into())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rfc4231_hmac_sha256_known_answer_and_verification() {
        // RFC 4231 section 4.2, test case 1 (HMAC-SHA-256).
        let key = [0x0b; 20];
        let sign = signer(&Algorithm::HS256, &EncodingKey::from_secret(&key)).unwrap();
        let verify = verifier(&Algorithm::HS256, &DecodingKey::from_secret(&key)).unwrap();
        let expected = [
            0xb0, 0x34, 0x4c, 0x61, 0xd8, 0xdb, 0x38, 0x53, 0x5c, 0xa8, 0xaf, 0xce, 0xaf, 0x0b,
            0xf1, 0x2b, 0x88, 0x1d, 0xc2, 0x00, 0xc9, 0x83, 0x3d, 0xa7, 0x26, 0xe9, 0x37, 0x6c,
            0x2e, 0x32, 0xcf, 0xf7,
        ]
        .to_vec();
        assert_eq!(sign.algorithm(), Algorithm::HS256);
        assert_eq!(verify.algorithm(), Algorithm::HS256);
        assert_eq!(sign.try_sign(b"Hi There").unwrap(), expected);
        assert!(verify.verify(b"Hi There", &expected).is_ok());
        assert!(verify.verify(b"Hi there", &expected).is_err());
        assert!(
            verify
                .verify(b"Hi There", &expected[..31].to_vec())
                .is_err()
        );
    }

    #[test]
    fn non_hs256_algorithms_and_wrong_key_families_are_rejected() {
        for algorithm in [
            Algorithm::HS384,
            Algorithm::HS512,
            Algorithm::RS256,
            Algorithm::RS384,
            Algorithm::RS512,
            Algorithm::PS256,
            Algorithm::PS384,
            Algorithm::PS512,
            Algorithm::ES256,
            Algorithm::ES384,
            Algorithm::EdDSA,
        ] {
            assert!(matches!(
                signer(&algorithm, &EncodingKey::from_secret(b"secret"))
                    .err()
                    .unwrap()
                    .kind(),
                ErrorKind::InvalidAlgorithm
            ));
            assert!(matches!(
                verifier(&algorithm, &DecodingKey::from_secret(b"secret"))
                    .err()
                    .unwrap()
                    .kind(),
                ErrorKind::InvalidAlgorithm
            ));
        }
        assert!(matches!(
            signer(&Algorithm::HS256, &EncodingKey::from_rsa_der(b"der"))
                .err()
                .unwrap()
                .kind(),
            ErrorKind::InvalidKeyFormat
        ));
        assert!(matches!(
            verifier(&Algorithm::HS256, &DecodingKey::from_rsa_der(b"der"))
                .err()
                .unwrap()
                .kind(),
            ErrorKind::InvalidKeyFormat
        ));
        assert!((PROVIDER.key_utils.rsa_pub_components_from_private_key)(b"der").is_err());
        assert!((PROVIDER.key_utils.rsa_pub_components_from_public_key)(b"der").is_err());
        assert!(
            (PROVIDER.key_utils.ec_pub_components_from_private_key)(b"der", Algorithm::ES256)
                .is_err()
        );
        assert!(
            (PROVIDER.key_utils.ed_pub_components_from_private_key)(
                b"der",
                &jsonwebtoken::jwk::EllipticCurve::Ed25519
            )
            .is_err()
        );
        assert!(
            (PROVIDER.key_utils.compute_digest)(b"data", jsonwebtoken::jwk::ThumbprintHash::SHA256)
                .is_err()
        );
    }

    #[test]
    fn concurrent_initialization_and_jwt_signing_are_idempotent() {
        let workers: Vec<_> = (0..16).map(|_| std::thread::spawn(|| {
            ensure_installed().unwrap();
            let signed = jsonwebtoken::crypto::sign(
                b"eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiIxMjM0NTY3ODkwIiwibmFtZSI6IkpvaG4gRG9lIiwiaWF0IjoxNTE2MjM5MDIyfQ",
                &EncodingKey::from_secret(b"your-256-bit-secret"), Algorithm::HS256,
            ).unwrap();
            assert_eq!(signed, "SflKxwRJSMeKKF2QT4fwpMeJf36POk6yJV_adQssw5c");
        })).collect();
        for worker in workers {
            worker.join().unwrap();
        }
    }

    #[test]
    fn global_provider_conflict_is_an_explicit_error() {
        // Process globals cannot be reset, so exercise the conflict branch in a
        // fresh test process without contaminating the normal auth tests.
        const CHILD_ENV: &str = "LIVECLOUD_TEST_JWT_PROVIDER_CONFLICT";
        if std::env::var_os(CHILD_ENV).is_some() {
            Box::leak(Box::new(PROVIDER.clone()))
                .install_default()
                .unwrap();
            for _ in 0..2 {
                assert!(
                    matches!(ensure_installed().unwrap_err().kind(), ErrorKind::Provider(message) if message.contains("already selected"))
                );
            }
            return;
        }
        let result = std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "auth::hs256_provider::tests::global_provider_conflict_is_an_explicit_error",
                "--nocapture",
            ])
            .env(CHILD_ENV, "1")
            .output()
            .unwrap();
        assert!(
            result.status.success(),
            "{}",
            String::from_utf8_lossy(&result.stderr)
        );
    }
}
