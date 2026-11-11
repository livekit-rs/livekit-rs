//! Expanded stable RustCrypto adapters. Interface patterns derive from
//! RustCrypto/rustls-rustcrypto 70f76c039e587192688af18a80d5d6435dedaf22.
//! The rsa dependency is public-only in rsa_verify.rs; RSA private TLS
//! signing is an independent stable-arithmetic adapter in rsa_sign.rs.
use crate::expanded_aead::{chacha20, gcm};
use hmac::Mac;
use p256::ecdsa::signature::Signer as _;
use p256::pkcs8::{DecodePrivateKey, EncodePublicKey};
use rustls::crypto::{self, hash};
use rustls::{CipherSuite, SignatureScheme, SupportedCipherSuite};
use rustls_pki_types::{PrivateKeyDer, SubjectPublicKeyInfoDer};
use sha2::{Digest, Sha384, Sha512};
use std::sync::Arc;

struct Hash384;
struct Context384(Sha384);
impl hash::Hash for Hash384 {
    fn start(&self) -> Box<dyn hash::Context> {
        Box::new(Context384(Sha384::new()))
    }
    fn hash(&self, data: &[u8]) -> hash::Output {
        hash::Output::new(&Sha384::digest(data))
    }
    fn output_len(&self) -> usize {
        48
    }
    fn algorithm(&self) -> hash::HashAlgorithm {
        hash::HashAlgorithm::SHA384
    }
}
impl hash::Context for Context384 {
    fn fork_finish(&self) -> hash::Output {
        hash::Output::new(&self.0.clone().finalize())
    }
    fn fork(&self) -> Box<dyn hash::Context> {
        Box::new(Self(self.0.clone()))
    }
    fn finish(self: Box<Self>) -> hash::Output {
        hash::Output::new(&self.0.finalize())
    }
    fn update(&mut self, data: &[u8]) {
        self.0.update(data)
    }
}
struct Hmac384;
struct Hmac384Key(hmac::Hmac<Sha384>);
impl crypto::hmac::Hmac for Hmac384 {
    fn with_key(&self, key: &[u8]) -> Box<dyn crypto::hmac::Key> {
        Box::new(Hmac384Key(
            <hmac::Hmac<Sha384> as Mac>::new_from_slice(key).expect("HMAC accepts all lengths"),
        ))
    }
    fn hash_output_len(&self) -> usize {
        48
    }
}
impl crypto::hmac::Key for Hmac384Key {
    fn sign_concat(&self, first: &[u8], middle: &[&[u8]], last: &[u8]) -> crypto::hmac::Tag {
        let mut m = self.0.clone();
        m.update(first);
        for x in middle {
            m.update(x)
        }
        m.update(last);
        crypto::hmac::Tag::new(&m.finalize().into_bytes())
    }
    fn tag_len(&self) -> usize {
        48
    }
}
pub const AES256: SupportedCipherSuite = SupportedCipherSuite::Tls13(&rustls::Tls13CipherSuite {
    common: crypto::CipherSuiteCommon {
        suite: CipherSuite::TLS13_AES_256_GCM_SHA384,
        hash_provider: &Hash384,
        confidentiality_limit: 1 << 23,
    },
    hkdf_provider: &crypto::tls13::HkdfUsingHmac(&Hmac384),
    aead_alg: &gcm::Tls13Aes256Gcm,
    quic: Some(&crate::quic::ExpandedQuicAlgorithm::Aes256),
});
pub const CHACHA: SupportedCipherSuite = SupportedCipherSuite::Tls13(&rustls::Tls13CipherSuite {
    common: crypto::CipherSuiteCommon {
        suite: CipherSuite::TLS13_CHACHA20_POLY1305_SHA256,
        hash_provider: &super::provider::Hash256,
        confidentiality_limit: u64::MAX,
    },
    hkdf_provider: &crypto::tls13::HkdfUsingHmac(&super::provider::Hmac256),
    aead_alg: &chacha20::Chacha20Poly1305,
    quic: Some(&crate::quic::ExpandedQuicAlgorithm::ChaCha20),
});
const EC_SCHEMES: &[SignatureScheme] = &[
    SignatureScheme::ECDSA_NISTP256_SHA256,
    SignatureScheme::ECDSA_NISTP384_SHA384,
    SignatureScheme::ECDSA_NISTP521_SHA512,
    SignatureScheme::ED25519,
];
const RSA_SCHEMES: &[SignatureScheme] = &[
    SignatureScheme::RSA_PSS_SHA256,
    SignatureScheme::RSA_PSS_SHA384,
    SignatureScheme::RSA_PSS_SHA512,
    SignatureScheme::RSA_PKCS1_SHA256,
    SignatureScheme::RSA_PKCS1_SHA384,
    SignatureScheme::RSA_PKCS1_SHA512,
];
macro_rules! suite12 {
    ($name:ident,$suite:ident,$hash:expr,$hmac:expr,$aead:expr,$sign:expr,$limit:expr) => {
        const $name: SupportedCipherSuite =
            SupportedCipherSuite::Tls12(&rustls::Tls12CipherSuite {
                common: crypto::CipherSuiteCommon {
                    suite: CipherSuite::$suite,
                    hash_provider: $hash,
                    confidentiality_limit: $limit,
                },
                kx: crypto::KeyExchangeAlgorithm::ECDHE,
                sign: $sign,
                aead_alg: $aead,
                prf_provider: &crypto::tls12::PrfUsingHmac($hmac),
            });
    };
}
suite12!(
    EC128,
    TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256,
    &super::provider::Hash256,
    &super::provider::Hmac256,
    &gcm::Tls12Aes128Gcm,
    EC_SCHEMES,
    1 << 23
);
suite12!(
    EC256,
    TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384,
    &Hash384,
    &Hmac384,
    &gcm::Tls12Aes256Gcm,
    EC_SCHEMES,
    1 << 23
);
suite12!(
    ECC20,
    TLS_ECDHE_ECDSA_WITH_CHACHA20_POLY1305_SHA256,
    &super::provider::Hash256,
    &super::provider::Hmac256,
    &chacha20::Chacha20Poly1305,
    EC_SCHEMES,
    u64::MAX
);
suite12!(
    RSA128,
    TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256,
    &super::provider::Hash256,
    &super::provider::Hmac256,
    &gcm::Tls12Aes128Gcm,
    RSA_SCHEMES,
    1 << 23
);
suite12!(
    RSA256,
    TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384,
    &Hash384,
    &Hmac384,
    &gcm::Tls12Aes256Gcm,
    RSA_SCHEMES,
    1 << 23
);
suite12!(
    RSAC20,
    TLS_ECDHE_RSA_WITH_CHACHA20_POLY1305_SHA256,
    &super::provider::Hash256,
    &super::provider::Hmac256,
    &chacha20::Chacha20Poly1305,
    RSA_SCHEMES,
    u64::MAX
);
pub const TLS12: &[SupportedCipherSuite] = &[EC128, EC256, ECC20, RSA128, RSA256, RSAC20];

#[derive(Debug)]
pub struct X25519;
struct XExchange {
    secret: x25519_dalek::StaticSecret,
    public: x25519_dalek::PublicKey,
}
impl crypto::SupportedKxGroup for X25519 {
    fn name(&self) -> rustls::NamedGroup {
        rustls::NamedGroup::X25519
    }
    fn start(&self) -> Result<Box<dyn crypto::ActiveKeyExchange>, rustls::Error> {
        let mut bytes = [0; 32];
        getrandom::fill(&mut bytes).map_err(|_| rustls::Error::FailedToGetRandomBytes)?;
        let secret = x25519_dalek::StaticSecret::from(bytes);
        p256::elliptic_curve::zeroize::Zeroize::zeroize(&mut bytes);
        let public = x25519_dalek::PublicKey::from(&secret);
        Ok(Box::new(XExchange { secret, public }))
    }
}
impl crypto::ActiveKeyExchange for XExchange {
    fn pub_key(&self) -> &[u8] {
        self.public.as_bytes()
    }
    fn group(&self) -> rustls::NamedGroup {
        rustls::NamedGroup::X25519
    }
    fn complete(self: Box<Self>, peer: &[u8]) -> Result<crypto::SharedSecret, rustls::Error> {
        let bytes: [u8; 32] = peer
            .try_into()
            .map_err(|_| rustls::Error::from(rustls::PeerMisbehaved::InvalidKeyShare))?;
        let secret = self
            .secret
            .diffie_hellman(&x25519_dalek::PublicKey::from(bytes));
        if !secret.was_contributory() {
            return Err(rustls::PeerMisbehaved::InvalidKeyShare.into());
        }
        Ok(secret.as_bytes().as_slice().into())
    }
}
#[derive(Debug)]
pub struct P384;
struct P384Exchange {
    secret: p384::SecretKey,
    public: Vec<u8>,
}
impl crypto::SupportedKxGroup for P384 {
    fn name(&self) -> rustls::NamedGroup {
        rustls::NamedGroup::secp384r1
    }
    fn start(&self) -> Result<Box<dyn crypto::ActiveKeyExchange>, rustls::Error> {
        for _ in 0..8 {
            let mut bytes = [0; 48];
            getrandom::fill(&mut bytes).map_err(|_| rustls::Error::FailedToGetRandomBytes)?;
            let key = p384::SecretKey::from_slice(&bytes);
            p256::elliptic_curve::zeroize::Zeroize::zeroize(&mut bytes);
            if let Ok(secret) = key {
                let public = secret.public_key().to_sec1_bytes().to_vec();
                return Ok(Box::new(P384Exchange { secret, public }));
            }
        }
        Err(rustls::Error::FailedToGetRandomBytes)
    }
}
impl crypto::ActiveKeyExchange for P384Exchange {
    fn pub_key(&self) -> &[u8] {
        &self.public
    }
    fn group(&self) -> rustls::NamedGroup {
        rustls::NamedGroup::secp384r1
    }
    fn complete(self: Box<Self>, peer: &[u8]) -> Result<crypto::SharedSecret, rustls::Error> {
        let peer = p384::PublicKey::from_sec1_bytes(peer)
            .map_err(|_| rustls::Error::from(rustls::PeerMisbehaved::InvalidKeyShare))?;
        let secret = p384::ecdh::diffie_hellman(self.secret.to_nonzero_scalar(), peer.as_affine());
        Ok(secret.raw_secret_bytes().as_slice().into())
    }
}
#[derive(Clone, Debug)]
enum Key {
    P384(p384::ecdsa::SigningKey),
    P521(p521::ecdsa::SigningKey),
    Ed25519(ed25519_dalek::SigningKey),
}
#[derive(Debug)]
struct KeySigner {
    key: Key,
    spki: Vec<u8>,
}
#[derive(Debug)]
struct Signer(Key);
impl Key {
    fn scheme(&self) -> SignatureScheme {
        match self {
            Self::P384(_) => SignatureScheme::ECDSA_NISTP384_SHA384,
            Self::P521(_) => SignatureScheme::ECDSA_NISTP521_SHA512,
            Self::Ed25519(_) => SignatureScheme::ED25519,
        }
    }
}
pub fn load_key(key: &PrivateKeyDer<'_>) -> Option<Arc<dyn rustls::sign::SigningKey>> {
    let signing = match key {
        PrivateKeyDer::Pkcs8(k) => {
            if let Ok(key) = p384::ecdsa::SigningKey::from_pkcs8_der(k.secret_pkcs8_der()) {
                Key::P384(key)
            } else if let Ok(key) = p521::ecdsa::SigningKey::from_pkcs8_der(k.secret_pkcs8_der()) {
                Key::P521(key)
            } else if let Ok(key) = ed25519_dalek::SigningKey::from_pkcs8_der(k.secret_pkcs8_der())
            {
                Key::Ed25519(key)
            } else {
                return None;
            }
        }
        PrivateKeyDer::Sec1(k) => {
            if let Ok(secret) = p384::SecretKey::from_sec1_der(k.secret_sec1_der()) {
                Key::P384(p384::ecdsa::SigningKey::from(secret))
            } else {
                Key::P521(p521::ecdsa::SigningKey::from(
                    p521::SecretKey::from_sec1_der(k.secret_sec1_der()).ok()?,
                ))
            }
        }
        _ => return None,
    };
    let spki = match &signing {
        Key::P384(k) => k
            .verifying_key()
            .to_public_key_der()
            .ok()?
            .as_bytes()
            .to_vec(),
        Key::P521(k) => k
            .verifying_key()
            .to_public_key_der()
            .ok()?
            .as_bytes()
            .to_vec(),
        Key::Ed25519(k) => k
            .verifying_key()
            .to_public_key_der()
            .ok()?
            .as_bytes()
            .to_vec(),
    };
    Some(Arc::new(KeySigner { key: signing, spki }))
}
impl rustls::sign::SigningKey for KeySigner {
    fn choose_scheme(&self, offered: &[SignatureScheme]) -> Option<Box<dyn rustls::sign::Signer>> {
        offered
            .contains(&self.key.scheme())
            .then(|| Box::new(Signer(self.key.clone())) as Box<dyn rustls::sign::Signer>)
    }
    fn algorithm(&self) -> rustls::SignatureAlgorithm {
        match self.key {
            Key::P384(_) | Key::P521(_) => rustls::SignatureAlgorithm::ECDSA,
            Key::Ed25519(_) => rustls::SignatureAlgorithm::ED25519,
        }
    }
    fn public_key(&self) -> Option<SubjectPublicKeyInfoDer<'_>> {
        Some(self.spki.as_slice().into())
    }
}
impl rustls::sign::Signer for Signer {
    fn sign(&self, message: &[u8]) -> Result<Vec<u8>, rustls::Error> {
        match &self.0 {
            Key::P384(key) => {
                let sig: p384::ecdsa::DerSignature = key
                    .try_sign(message)
                    .map_err(|_| rustls::Error::General("P384 signing failed".into()))?;
                Ok(sig.as_bytes().to_vec())
            }
            Key::P521(key) => {
                let sig: p521::ecdsa::DerSignature = key
                    .try_sign(message)
                    .map_err(|_| rustls::Error::General("P521 signing failed".into()))?;
                Ok(sig.as_bytes().to_vec())
            }
            Key::Ed25519(key) => {
                let sig: ed25519_dalek::Signature = key
                    .try_sign(message)
                    .map_err(|_| rustls::Error::General("Ed25519 signing failed".into()))?;
                Ok(sig.to_bytes().to_vec())
            }
        }
    }
    fn scheme(&self) -> SignatureScheme {
        self.0.scheme()
    }
}
#[derive(Debug)]
pub struct Ed25519Verifier;
impl rustls_pki_types::SignatureVerificationAlgorithm for Ed25519Verifier {
    fn public_key_alg_id(&self) -> rustls_pki_types::AlgorithmIdentifier {
        rustls_pki_types::alg_id::ED25519
    }
    fn signature_alg_id(&self) -> rustls_pki_types::AlgorithmIdentifier {
        rustls_pki_types::alg_id::ED25519
    }
    fn verify_signature(
        &self,
        public: &[u8],
        message: &[u8],
        signature: &[u8],
    ) -> Result<(), rustls_pki_types::InvalidSignature> {
        let key = ed25519_dalek::VerifyingKey::from_bytes(
            public
                .try_into()
                .map_err(|_| rustls_pki_types::InvalidSignature)?,
        )
        .map_err(|_| rustls_pki_types::InvalidSignature)?;
        let sig = ed25519_dalek::Signature::from_slice(signature)
            .map_err(|_| rustls_pki_types::InvalidSignature)?;
        key.verify_strict(message, &sig)
            .map_err(|_| rustls_pki_types::InvalidSignature)
    }
}

#[derive(Debug)]
pub struct P521Verifier;
impl rustls_pki_types::SignatureVerificationAlgorithm for P521Verifier {
    fn public_key_alg_id(&self) -> rustls_pki_types::AlgorithmIdentifier {
        rustls_pki_types::alg_id::ECDSA_P521
    }
    fn signature_alg_id(&self) -> rustls_pki_types::AlgorithmIdentifier {
        rustls_pki_types::alg_id::ECDSA_SHA512
    }
    fn verify_signature(
        &self,
        public: &[u8],
        message: &[u8],
        signature: &[u8],
    ) -> Result<(), rustls_pki_types::InvalidSignature> {
        use p256::ecdsa::signature::hazmat::PrehashVerifier;
        let key = p521::ecdsa::VerifyingKey::from_sec1_bytes(public)
            .map_err(|_| rustls_pki_types::InvalidSignature)?;
        let sig = p521::ecdsa::DerSignature::from_bytes(signature)
            .map_err(|_| rustls_pki_types::InvalidSignature)?;
        key.verify_prehash(&Sha512::digest(message), &sig)
            .map_err(|_| rustls_pki_types::InvalidSignature)
    }
}
