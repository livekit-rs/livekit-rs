//! Public-only RSA signature verification. This module never loads, constructs,
//! signs with, or decrypts with an RSA private key. See RSA-PUBLIC-ONLY.md.
//! Adapters follow RustCrypto/rustls-rustcrypto's public verifier patterns.
use rsa::pkcs8::der::Decode;
use rsa::signature::Verifier as _;
use rsa::traits::PublicKeyParts;
use rustls_pki_types::{
    AlgorithmIdentifier, InvalidSignature, SignatureVerificationAlgorithm, alg_id,
};
use sha2::{Sha256, Sha384, Sha512};
macro_rules! verifier {
    ($name:ident,$oid:ident,$key:ty,$sig:ty) => {
        #[derive(Debug)]
        pub struct $name;
        impl SignatureVerificationAlgorithm for $name {
            fn public_key_alg_id(&self) -> AlgorithmIdentifier {
                alg_id::RSA_ENCRYPTION
            }
            fn signature_alg_id(&self) -> AlgorithmIdentifier {
                alg_id::$oid
            }
            fn verify_signature(
                &self,
                public: &[u8],
                message: &[u8],
                signature: &[u8],
            ) -> Result<(), InvalidSignature> {
                let parsed =
                    rsa::pkcs1::RsaPublicKey::from_der(public).map_err(|_| InvalidSignature)?;
                let key = rsa::RsaPublicKey::new_with_max_size(
                    rsa::BigUint::from_bytes_be(parsed.modulus.as_bytes()),
                    rsa::BigUint::from_bytes_be(parsed.public_exponent.as_bytes()),
                    8192,
                )
                .map_err(|_| InvalidSignature)?;
                // Bound public verification work and reject weak endpoint/issuer keys.
                if !(1024..=8192).contains(&key.n().bits()) || key.e().bits() > 32 {
                    return Err(InvalidSignature);
                }
                let signature = <$sig>::try_from(signature).map_err(|_| InvalidSignature)?;
                <$key>::new(key)
                    .verify(message, &signature)
                    .map_err(|_| InvalidSignature)
            }
        }
    };
}
verifier!(
    Pkcs256,
    RSA_PKCS1_SHA256,
    rsa::pkcs1v15::VerifyingKey<Sha256>,
    rsa::pkcs1v15::Signature
);
verifier!(
    Pkcs384,
    RSA_PKCS1_SHA384,
    rsa::pkcs1v15::VerifyingKey<Sha384>,
    rsa::pkcs1v15::Signature
);
verifier!(
    Pkcs512,
    RSA_PKCS1_SHA512,
    rsa::pkcs1v15::VerifyingKey<Sha512>,
    rsa::pkcs1v15::Signature
);
verifier!(
    Pss256,
    RSA_PSS_SHA256,
    rsa::pss::VerifyingKey<Sha256>,
    rsa::pss::Signature
);
verifier!(
    Pss384,
    RSA_PSS_SHA384,
    rsa::pss::VerifyingKey<Sha384>,
    rsa::pss::Signature
);
verifier!(
    Pss512,
    RSA_PSS_SHA512,
    rsa::pss::VerifyingKey<Sha512>,
    rsa::pss::Signature
);
