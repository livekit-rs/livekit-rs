//! Provider adapters follow RustCrypto/rustls-rustcrypto commit
//! 70f76c039e587192688af18a80d5d6435dedaf22 (MIT OR Apache-2.0), narrowed to
//! stable primitives and TLS 1.3. Protocol framing/HKDF/nonce rules come from
//! rustls; all cryptographic operations come from the existing RustCrypto crates.

use std::sync::Arc;

use aes_gcm::aead::{AeadInPlace, KeyInit};
use hmac::Mac;
use p256::ecdsa::signature::{Signer as _, hazmat::PrehashVerifier};
use p256::pkcs8::{DecodePrivateKey, EncodePublicKey};
use rustls::crypto::{self, cipher, hash};
use rustls::{CipherSuite, SignatureAlgorithm, SignatureScheme, SupportedCipherSuite};
use rustls_pki_types::{
    AlgorithmIdentifier, InvalidSignature, PrivateKeyDer, SignatureVerificationAlgorithm,
    SubjectPublicKeyInfoDer, alg_id,
};
use sha2::{Digest, Sha256, Sha384, Sha512};

#[derive(Debug)]
struct Primitives;

pub fn crypto_provider() -> crypto::CryptoProvider {
    crypto::CryptoProvider {
        cipher_suites: [
            vec![AES128_GCM, crate::expanded::AES256, crate::expanded::CHACHA],
            crate::expanded::TLS12.to_vec(),
        ]
        .concat(),
        kx_groups: vec![&crate::expanded::X25519, &P256_KX, &crate::expanded::P384],
        signature_verification_algorithms: VERIFY_ALGORITHMS,
        secure_random: &Primitives,
        key_provider: &Primitives,
    }
}

impl crypto::SecureRandom for Primitives {
    fn fill(&self, bytes: &mut [u8]) -> Result<(), crypto::GetRandomFailed> {
        getrandom::fill(bytes).map_err(|_| crypto::GetRandomFailed)
    }
}

impl crypto::KeyProvider for Primitives {
    fn load_private_key(
        &self,
        key: PrivateKeyDer<'static>,
    ) -> Result<Arc<dyn rustls::sign::SigningKey>, rustls::Error> {
        let key = zeroize::Zeroizing::new(key);
        if let Some(signing) = crate::rsa_sign::load(&key)? {
            return Ok(signing);
        }
        if let Some(signing) = crate::expanded::load_key(&key) {
            return Ok(signing);
        }
        let invalid = || {
            rustls::Error::General(
                "TLS requires a valid RSA PKCS1/PKCS8, ECDSA P256/P384 or Ed25519 private key"
                    .into(),
            )
        };
        let signing = match &*key {
            PrivateKeyDer::Pkcs8(key) => {
                p256::ecdsa::SigningKey::from_pkcs8_der(key.secret_pkcs8_der())
                    .map_err(|_| invalid())?
            }
            PrivateKeyDer::Sec1(key) => {
                // RustCrypto validates any named-curve OID and supplied public key
                // against P-256, in addition to DER and private-scalar validation.
                let secret =
                    p256::SecretKey::from_sec1_der(key.secret_sec1_der()).map_err(|_| invalid())?;
                p256::ecdsa::SigningKey::from(secret)
            }
            _ => return Err(invalid()),
        };
        let spki = signing
            .verifying_key()
            .to_public_key_der()
            .map_err(|e| rustls::Error::General(e.to_string()))?
            .as_bytes()
            .to_vec();
        Ok(Arc::new(SigningKey { signing, spki }))
    }
}

#[derive(Debug)]
struct SigningKey {
    signing: p256::ecdsa::SigningKey,
    spki: Vec<u8>,
}
impl rustls::sign::SigningKey for SigningKey {
    fn choose_scheme(&self, offered: &[SignatureScheme]) -> Option<Box<dyn rustls::sign::Signer>> {
        offered
            .contains(&SignatureScheme::ECDSA_NISTP256_SHA256)
            .then(|| {
                Box::new(SignatureSigner(self.signing.clone())) as Box<dyn rustls::sign::Signer>
            })
    }
    fn algorithm(&self) -> SignatureAlgorithm {
        SignatureAlgorithm::ECDSA
    }
    fn public_key(&self) -> Option<SubjectPublicKeyInfoDer<'_>> {
        Some(self.spki.as_slice().into())
    }
}
#[derive(Debug)]
struct SignatureSigner(p256::ecdsa::SigningKey);
impl rustls::sign::Signer for SignatureSigner {
    fn sign(&self, message: &[u8]) -> Result<Vec<u8>, rustls::Error> {
        let signature: p256::ecdsa::DerSignature = self
            .0
            .try_sign(message)
            .map_err(|_| rustls::Error::General("ECDSA signing failed".into()))?;
        Ok(signature.as_bytes().to_vec())
    }
    fn scheme(&self) -> SignatureScheme {
        SignatureScheme::ECDSA_NISTP256_SHA256
    }
}

pub(crate) struct Hash256;
struct HashContext(Sha256);
impl hash::Hash for Hash256 {
    fn start(&self) -> Box<dyn hash::Context> {
        Box::new(HashContext(Sha256::new()))
    }
    fn hash(&self, data: &[u8]) -> hash::Output {
        hash::Output::new(&Sha256::digest(data))
    }
    fn output_len(&self) -> usize {
        32
    }
    fn algorithm(&self) -> hash::HashAlgorithm {
        hash::HashAlgorithm::SHA256
    }
}
impl hash::Context for HashContext {
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
pub(crate) struct Hmac256;
struct HmacKey(hmac::Hmac<Sha256>);
impl crypto::hmac::Hmac for Hmac256 {
    fn with_key(&self, key: &[u8]) -> Box<dyn crypto::hmac::Key> {
        // HMAC accepts every key length by construction.
        Box::new(HmacKey(
            <hmac::Hmac<Sha256> as Mac>::new_from_slice(key)
                .expect("HMAC accepts arbitrary key lengths"),
        ))
    }
    fn hash_output_len(&self) -> usize {
        32
    }
}
impl crypto::hmac::Key for HmacKey {
    fn sign_concat(&self, first: &[u8], middle: &[&[u8]], last: &[u8]) -> crypto::hmac::Tag {
        let mut mac = self.0.clone();
        mac.update(first);
        for part in middle {
            mac.update(part);
        }
        mac.update(last);
        crypto::hmac::Tag::new(&mac.finalize().into_bytes())
    }
    fn tag_len(&self) -> usize {
        32
    }
}

#[derive(Debug)]
struct P256Exchange;
static P256_KX: P256Exchange = P256Exchange;
impl crypto::SupportedKxGroup for P256Exchange {
    fn name(&self) -> rustls::NamedGroup {
        rustls::NamedGroup::secp256r1
    }
    fn start(&self) -> Result<Box<dyn crypto::ActiveKeyExchange>, rustls::Error> {
        for _ in 0..8 {
            let mut bytes = [0; 32];
            getrandom::fill(&mut bytes).map_err(|_| rustls::Error::FailedToGetRandomBytes)?;
            let key = p256::SecretKey::from_slice(&bytes);
            p256::elliptic_curve::zeroize::Zeroize::zeroize(&mut bytes);
            if let Ok(secret) = key {
                let public = secret.public_key().to_sec1_bytes().to_vec();
                return Ok(Box::new(Exchange { secret, public }));
            }
        }
        Err(rustls::Error::FailedToGetRandomBytes)
    }
}
struct Exchange {
    secret: p256::SecretKey,
    public: Vec<u8>,
}
impl crypto::ActiveKeyExchange for Exchange {
    fn pub_key(&self) -> &[u8] {
        &self.public
    }
    fn group(&self) -> rustls::NamedGroup {
        rustls::NamedGroup::secp256r1
    }
    fn complete(self: Box<Self>, peer: &[u8]) -> Result<crypto::SharedSecret, rustls::Error> {
        let peer = p256::PublicKey::from_sec1_bytes(peer)
            .map_err(|_| rustls::Error::from(rustls::PeerMisbehaved::InvalidKeyShare))?;
        let secret = p256::ecdh::diffie_hellman(self.secret.to_nonzero_scalar(), peer.as_affine());
        Ok(secret.raw_secret_bytes().as_slice().into())
    }
}

const AES128_GCM: SupportedCipherSuite = SupportedCipherSuite::Tls13(&rustls::Tls13CipherSuite {
    common: crypto::CipherSuiteCommon {
        suite: CipherSuite::TLS13_AES_128_GCM_SHA256,
        hash_provider: &Hash256,
        confidentiality_limit: 1 << 23,
    },
    hkdf_provider: &crypto::tls13::HkdfUsingHmac(&Hmac256),
    aead_alg: &Aes128,
    // QUIC packet/header protection delegates to the RFC 9001-verified
    // lk-quic-crypto core (see `crate::quic`); this also makes the provider
    // advertise `Protocol::Quic` support. AES-128-GCM is listed first so it is
    // preferred, but the expanded AES-256-GCM and ChaCha20-Poly1305 suites also
    // carry QUIC algorithms, so a peer that only offers ChaCha still negotiates
    // ChaCha QUIC (see tests/quic.rs).
    quic: Some(&crate::quic::QuicAlgorithm),
});
struct Aes128;
struct RecordCipher(aes_gcm::Aes128Gcm, cipher::Iv);
impl cipher::Tls13AeadAlgorithm for Aes128 {
    fn encrypter(&self, key: cipher::AeadKey, iv: cipher::Iv) -> Box<dyn cipher::MessageEncrypter> {
        Box::new(RecordCipher(
            aes_gcm::Aes128Gcm::new_from_slice(key.as_ref())
                .expect("rustls supplies an AES-128 key"),
            iv,
        ))
    }
    fn decrypter(&self, key: cipher::AeadKey, iv: cipher::Iv) -> Box<dyn cipher::MessageDecrypter> {
        Box::new(RecordCipher(
            aes_gcm::Aes128Gcm::new_from_slice(key.as_ref())
                .expect("rustls supplies an AES-128 key"),
            iv,
        ))
    }
    fn key_len(&self) -> usize {
        16
    }
    fn extract_keys(
        &self,
        key: cipher::AeadKey,
        iv: cipher::Iv,
    ) -> Result<rustls::ConnectionTrafficSecrets, cipher::UnsupportedOperationError> {
        Ok(rustls::ConnectionTrafficSecrets::Aes128Gcm { key, iv })
    }
}
impl cipher::MessageEncrypter for RecordCipher {
    fn encrypt(
        &mut self,
        message: cipher::OutboundPlainMessage<'_>,
        seq: u64,
    ) -> Result<cipher::OutboundOpaqueMessage, rustls::Error> {
        let mut bytes = message.payload.to_vec();
        bytes.push(u8::from(message.typ));
        let nonce = cipher::Nonce::new(&self.1, seq).0;
        let aad = cipher::make_tls13_aad(bytes.len() + 16);
        self.0
            .encrypt_in_place((&nonce).into(), &aad, &mut bytes)
            .map_err(|_| rustls::Error::EncryptError)?;
        let mut payload = cipher::PrefixedPayload::with_capacity(bytes.len());
        payload.extend_from_slice(&bytes);
        Ok(cipher::OutboundOpaqueMessage::new(
            rustls::ContentType::ApplicationData,
            rustls::ProtocolVersion::TLSv1_2,
            payload,
        ))
    }
    fn encrypted_payload_len(&self, payload_len: usize) -> usize {
        payload_len + 17
    }
}
impl cipher::MessageDecrypter for RecordCipher {
    fn decrypt<'a>(
        &mut self,
        mut message: cipher::InboundOpaqueMessage<'a>,
        seq: u64,
    ) -> Result<cipher::InboundPlainMessage<'a>, rustls::Error> {
        if message.payload.len() < 17 {
            return Err(rustls::Error::DecryptError);
        }
        let end = message.payload.len() - 16;
        let tag = aes_gcm::Tag::clone_from_slice(&message.payload[end..]);
        let nonce = cipher::Nonce::new(&self.1, seq).0;
        let aad = cipher::make_tls13_aad(message.payload.len());
        self.0
            .decrypt_in_place_detached((&nonce).into(), &aad, &mut message.payload[..end], &tag)
            .map_err(|_| rustls::Error::DecryptError)?;
        message.payload.truncate(end);
        message.into_tls13_unpadded_message()
    }
}

#[derive(Debug)]
enum EcdsaHash {
    Sha256,
    Sha384,
    Sha512,
}

#[derive(Debug)]
struct EcdsaVerifier {
    p384: bool,
    hash: EcdsaHash,
}
impl SignatureVerificationAlgorithm for EcdsaVerifier {
    fn public_key_alg_id(&self) -> AlgorithmIdentifier {
        if self.p384 {
            alg_id::ECDSA_P384
        } else {
            alg_id::ECDSA_P256
        }
    }
    fn signature_alg_id(&self) -> AlgorithmIdentifier {
        match self.hash {
            EcdsaHash::Sha256 => alg_id::ECDSA_SHA256,
            EcdsaHash::Sha384 => alg_id::ECDSA_SHA384,
            EcdsaHash::Sha512 => alg_id::ECDSA_SHA512,
        }
    }
    fn verify_signature(
        &self,
        public_key: &[u8],
        message: &[u8],
        signature: &[u8],
    ) -> Result<(), InvalidSignature> {
        let digest = match self.hash {
            EcdsaHash::Sha256 => Sha256::digest(message).to_vec(),
            EcdsaHash::Sha384 => Sha384::digest(message).to_vec(),
            EcdsaHash::Sha512 => Sha512::digest(message).to_vec(),
        };
        if self.p384 {
            let key = p384::ecdsa::VerifyingKey::from_sec1_bytes(public_key)
                .map_err(|_| InvalidSignature)?;
            let signature =
                p384::ecdsa::DerSignature::from_bytes(signature).map_err(|_| InvalidSignature)?;
            key.verify_prehash(&digest, &signature)
                .map_err(|_| InvalidSignature)
        } else {
            let key = p256::ecdsa::VerifyingKey::from_sec1_bytes(public_key)
                .map_err(|_| InvalidSignature)?;
            let signature =
                p256::ecdsa::DerSignature::from_bytes(signature).map_err(|_| InvalidSignature)?;
            key.verify_prehash(&digest, &signature)
                .map_err(|_| InvalidSignature)
        }
    }
}
static P256_SHA256: EcdsaVerifier = EcdsaVerifier {
    p384: false,
    hash: EcdsaHash::Sha256,
};
static P256_SHA384: EcdsaVerifier = EcdsaVerifier {
    p384: false,
    hash: EcdsaHash::Sha384,
};
static P384_SHA256: EcdsaVerifier = EcdsaVerifier {
    p384: true,
    hash: EcdsaHash::Sha256,
};
static P384_SHA384: EcdsaVerifier = EcdsaVerifier {
    p384: true,
    hash: EcdsaHash::Sha384,
};
static P256_SHA512: EcdsaVerifier = EcdsaVerifier {
    p384: false,
    hash: EcdsaHash::Sha512,
};
static P384_SHA512: EcdsaVerifier = EcdsaVerifier {
    p384: true,
    hash: EcdsaHash::Sha512,
};
const VERIFY_ALGORITHMS: crypto::WebPkiSupportedAlgorithms = crypto::WebPkiSupportedAlgorithms {
    all: &[
        &P256_SHA256,
        &P256_SHA384,
        &P384_SHA256,
        &P384_SHA384,
        &crate::expanded::Ed25519Verifier,
        &crate::expanded::P521Verifier,
        &crate::rsa_verify::Pkcs256,
        &crate::rsa_verify::Pkcs384,
        &crate::rsa_verify::Pkcs512,
        &crate::rsa_verify::Pss256,
        &crate::rsa_verify::Pss384,
        &crate::rsa_verify::Pss512,
    ],
    mapping: &[
        (
            SignatureScheme::ED25519,
            &[&crate::expanded::Ed25519Verifier],
        ),
        (
            SignatureScheme::ECDSA_NISTP521_SHA512,
            // TLS 1.3 selects the first verifier and requires a P-521 key.
            // TLS 1.2 selects SHA-512 independently of the certificate curve.
            &[&crate::expanded::P521Verifier, &P256_SHA512, &P384_SHA512],
        ),
        (
            SignatureScheme::RSA_PKCS1_SHA256,
            &[&crate::rsa_verify::Pkcs256],
        ),
        (
            SignatureScheme::RSA_PKCS1_SHA384,
            &[&crate::rsa_verify::Pkcs384],
        ),
        (
            SignatureScheme::RSA_PKCS1_SHA512,
            &[&crate::rsa_verify::Pkcs512],
        ),
        (
            SignatureScheme::RSA_PSS_SHA256,
            &[&crate::rsa_verify::Pss256],
        ),
        (
            SignatureScheme::RSA_PSS_SHA384,
            &[&crate::rsa_verify::Pss384],
        ),
        (
            SignatureScheme::RSA_PSS_SHA512,
            &[&crate::rsa_verify::Pss512],
        ),
        (
            SignatureScheme::ECDSA_NISTP256_SHA256,
            &[&P256_SHA256, &P384_SHA256],
        ),
        (
            SignatureScheme::ECDSA_NISTP384_SHA384,
            &[&P384_SHA384, &P256_SHA384],
        ),
    ],
};

#[cfg(test)]
mod tests {
    use super::*;
    use cipher::{MessageDecrypter, MessageEncrypter};

    #[test]
    fn tls13_records_reject_tampering_truncation_and_wrong_sequence() {
        let key = [7u8; 16];
        let iv = [9u8; 12];
        let record = || {
            let mut enc = RecordCipher(
                aes_gcm::Aes128Gcm::new_from_slice(&key).unwrap(),
                cipher::Iv::from(iv),
            );
            enc.encrypt(
                cipher::OutboundPlainMessage {
                    typ: rustls::ContentType::ApplicationData,
                    version: rustls::ProtocolVersion::TLSv1_3,
                    payload: cipher::OutboundChunks::Single(b"payload"),
                },
                3,
            )
            .unwrap()
            .payload
            .as_ref()
            .to_vec()
        };
        for mutation in 0..3 {
            let mut payload = record();
            let seq = if mutation == 2 { 4 } else { 3 };
            if mutation == 0 {
                payload[0] ^= 1;
            }
            if mutation == 1 {
                payload.truncate(payload.len() - 1);
            }
            let mut dec = RecordCipher(
                aes_gcm::Aes128Gcm::new_from_slice(&key).unwrap(),
                cipher::Iv::from(iv),
            );
            assert!(
                dec.decrypt(
                    cipher::InboundOpaqueMessage::new(
                        rustls::ContentType::ApplicationData,
                        rustls::ProtocolVersion::TLSv1_2,
                        &mut payload,
                    ),
                    seq
                )
                .is_err()
            );
        }
    }

    #[test]
    fn invalid_p256_key_share_and_unsupported_signatures_are_rejected() {
        let exchange = crypto::SupportedKxGroup::start(&P256_KX).unwrap();
        assert!(exchange.complete(&[0u8; 65]).is_err());
        assert!(
            !VERIFY_ALGORITHMS
                .supported_schemes()
                .contains(&SignatureScheme::RSA_PKCS1_SHA1)
        );
    }
}
