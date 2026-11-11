//! Signature-only RSA adapter over stable crypto-bigint. Encoding/blinding
//! patterns derive from RustCrypto/RSA 0.10.0-rc.19 (MIT OR Apache-2.0).
//! That release is reference source, NOT a production dependency.
//! No CRT, encryption-padding decoder, RSA decryption API or static-RSA KX.
use crypto_bigint::{
    BoxedUint, ConcatenatingMul, NonZero, Odd, Resize,
    modular::{BoxedMontyForm, BoxedMontyParams},
};
use rsa::{pkcs8::der::Decode, traits::PublicKeyParts};
use rustls::{SignatureAlgorithm, SignatureScheme};
use rustls_pki_types::{PrivateKeyDer, SubjectPublicKeyInfoDer};
use sha2::{Digest, Sha256, Sha384, Sha512};
use std::{fmt, sync::Arc};
use zeroize::{Zeroize, Zeroizing};

pub struct Key {
    inner: Arc<Inner>,
    spki: Vec<u8>,
}
struct Inner {
    d: Zeroizing<BoxedUint>,
    e: BoxedUint,
    params: BoxedMontyParams,
    bits: u32,
    bytes: usize,
    n_bits: usize,
}
impl fmt::Debug for Key {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("RsaSigningKey([REDACTED])")
    }
}
struct Signer {
    inner: Arc<Inner>,
    scheme: SignatureScheme,
}
impl fmt::Debug for Signer {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("RsaSigner([REDACTED])")
    }
}
fn error() -> rustls::Error {
    rustls::Error::General("invalid RSA signing key or signature operation".into())
}
const SCHEMES: &[SignatureScheme] = &[
    SignatureScheme::RSA_PSS_SHA512,
    SignatureScheme::RSA_PSS_SHA384,
    SignatureScheme::RSA_PSS_SHA256,
    SignatureScheme::RSA_PKCS1_SHA512,
    SignatureScheme::RSA_PKCS1_SHA384,
    SignatureScheme::RSA_PKCS1_SHA256,
];
pub fn load(
    key: &PrivateKeyDer<'_>,
) -> Result<Option<Arc<dyn rustls::sign::SigningKey>>, rustls::Error> {
    let der = match key {
        PrivateKeyDer::Pkcs1(x) => x.secret_pkcs1_der(),
        PrivateKeyDer::Pkcs8(x) => {
            let info =
                rsa::pkcs8::PrivateKeyInfo::from_der(x.secret_pkcs8_der()).map_err(|_| error())?;
            if info.algorithm.oid != rsa::pkcs1::ALGORITHM_OID {
                return Ok(None);
            }
            info.private_key
        }
        _ => return Ok(None),
    };
    // DER parsing is local initialization. No rsa crate private arithmetic is used.
    let parsed = rsa::pkcs1::RsaPrivateKey::from_der(der).map_err(|_| error())?;
    let n_bytes = parsed.modulus.as_bytes();
    if !(128..=1024).contains(&n_bytes.len()) {
        return Err(error());
    }
    let bits = (n_bytes.len() * 8) as u32;
    let n = BoxedUint::from_be_slice(n_bytes, bits).map_err(|_| error())?;
    let e =
        BoxedUint::from_be_slice(parsed.public_exponent.as_bytes(), bits).map_err(|_| error())?;
    let d = Zeroizing::new(
        BoxedUint::from_be_slice(parsed.private_exponent.as_bytes(), bits).map_err(|_| error())?,
    );
    if *d == BoxedUint::zero_with_precision(bits) || *d >= n {
        return Err(error());
    }
    // Go-style local parameter validation: product(primes)=n and ed=1 mod p-1.
    // CRT values are never used for signing. Multi-prime PKCS1 remains supported.
    let mut primes = vec![parsed.prime1.as_bytes(), parsed.prime2.as_bytes()];
    if let Some(extra) = &parsed.other_prime_infos {
        for part in extra {
            primes.push(part.prime.as_bytes())
        }
    }
    let one = BoxedUint::one_with_precision(bits);
    let mut product = Zeroizing::new(one.clone());
    let ed = Zeroizing::new(e.concatenating_mul(&*d));
    for prime in primes {
        let prime = Zeroizing::new(BoxedUint::from_be_slice(prime, bits).map_err(|_| error())?);
        if *prime <= one {
            return Err(error());
        }
        let next = Zeroizing::new(
            Option::<BoxedUint>::from(product.checked_mul(&*prime)).ok_or_else(error)?,
        );
        product.zeroize();
        *product = (*next).clone();
        let pm = Zeroizing::new(prime.wrapping_sub(&one));
        let modulus = Zeroizing::new(
            Option::<NonZero<BoxedUint>>::from(NonZero::new(
                (&*pm).resize_unchecked(ed.bits_precision()),
            ))
            .ok_or_else(error)?,
        );
        let remainder = Zeroizing::new(ed.rem(&modulus));
        if *remainder != BoxedUint::one_with_precision(ed.bits_precision()) {
            return Err(error());
        }
    }
    if *product != n {
        return Err(error());
    }
    let odd = Option::<Odd<BoxedUint>>::from(Odd::new(n)).ok_or_else(error)?;
    let params = BoxedMontyParams::new(odd);
    let public = rsa::RsaPublicKey::new_with_max_size(
        rsa::BigUint::from_bytes_be(n_bytes),
        rsa::BigUint::from_bytes_be(parsed.public_exponent.as_bytes()),
        8192,
    )
    .map_err(|_| error())?;
    if public.n().bits() < 1024 || public.e().bits() > 32 {
        return Err(error());
    }
    use rsa::pkcs8::EncodePublicKey;
    let spki = public
        .to_public_key_der()
        .map_err(|_| error())?
        .as_bytes()
        .to_vec();
    let inner = Arc::new(Inner {
        d,
        e,
        params,
        bits,
        bytes: n_bytes.len(),
        n_bits: public.n().bits(),
    });
    // Refuse an inconsistent d during loading, before exposing a listener.
    let mut probe = Zeroizing::new(vec![0; inner.bytes]);
    *probe.last_mut().ok_or_else(error)? = 42;
    private_signature_primitive(&inner, &probe)?;
    Ok(Some(Arc::new(Key { inner, spki })))
}
impl rustls::sign::SigningKey for Key {
    fn choose_scheme(&self, offered: &[SignatureScheme]) -> Option<Box<dyn rustls::sign::Signer>> {
        SCHEMES
            .iter()
            .find(|s| {
                let h_len = match s {
                    SignatureScheme::RSA_PSS_SHA512 => 64,
                    SignatureScheme::RSA_PSS_SHA384 => 48,
                    _ => 32,
                };
                let pss = matches!(
                    s,
                    SignatureScheme::RSA_PSS_SHA256
                        | SignatureScheme::RSA_PSS_SHA384
                        | SignatureScheme::RSA_PSS_SHA512
                );
                offered.contains(s)
                    && (!pss || (self.inner.n_bits - 1).div_ceil(8) >= 2 * h_len + 2)
            })
            .map(|scheme| {
                Box::new(Signer {
                    inner: self.inner.clone(),
                    scheme: *scheme,
                }) as Box<dyn rustls::sign::Signer>
            })
    }
    fn algorithm(&self) -> SignatureAlgorithm {
        SignatureAlgorithm::RSA
    }
    fn public_key(&self) -> Option<SubjectPublicKeyInfoDer<'_>> {
        Some(self.spki.as_slice().into())
    }
}
impl rustls::sign::Signer for Signer {
    fn scheme(&self) -> SignatureScheme {
        self.scheme
    }
    fn sign(&self, message: &[u8]) -> Result<Vec<u8>, rustls::Error> {
        let encoded = match self.scheme {
            SignatureScheme::RSA_PKCS1_SHA256 => pkcs_encode(
                &rsa::Pkcs1v15Sign::new::<Sha256>().prefix,
                &Sha256::digest(message),
                self.inner.bytes,
            )?,
            SignatureScheme::RSA_PKCS1_SHA384 => pkcs_encode(
                &rsa::Pkcs1v15Sign::new::<Sha384>().prefix,
                &Sha384::digest(message),
                self.inner.bytes,
            )?,
            SignatureScheme::RSA_PKCS1_SHA512 => pkcs_encode(
                &rsa::Pkcs1v15Sign::new::<Sha512>().prefix,
                &Sha512::digest(message),
                self.inner.bytes,
            )?,
            SignatureScheme::RSA_PSS_SHA256 => {
                pss_encode::<Sha256>(message, self.inner.n_bits - 1)?
            }
            SignatureScheme::RSA_PSS_SHA384 => {
                pss_encode::<Sha384>(message, self.inner.n_bits - 1)?
            }
            SignatureScheme::RSA_PSS_SHA512 => {
                pss_encode::<Sha512>(message, self.inner.n_bits - 1)?
            }
            _ => return Err(error()),
        };
        private_signature_primitive(&self.inner, &encoded)
    }
}
fn private_signature_primitive(key: &Inner, encoded: &[u8]) -> Result<Vec<u8>, rustls::Error> {
    private_signature_with_rng(key, encoded, getrandom::fill)
}
fn private_signature_with_rng(
    key: &Inner,
    encoded: &[u8],
    mut fill: impl FnMut(&mut [u8]) -> Result<(), getrandom::Error>,
) -> Result<Vec<u8>, rustls::Error> {
    let m = Zeroizing::new(BoxedUint::from_be_slice(encoded, key.bits).map_err(|_| error())?);
    if *m >= *key.params.modulus().as_ref() {
        return Err(error());
    }
    // Blinding is mandatory; entropy failure aborts rather than falling back.
    let mut random = Zeroizing::new(vec![0; key.bytes]);
    for _ in 0..64 {
        fill(&mut random).map_err(|_| rustls::Error::FailedToGetRandomBytes)?;
        // Sample at the public modulus bit length, not the rounded byte width.
        // Without this mask a 2049-bit modulus would reject ~255/256 draws.
        random[0] &= 0xff >> (key.bytes * 8 - key.n_bits);
        let r = Zeroizing::new(BoxedUint::from_be_slice(&random, key.bits).map_err(|_| error())?);
        if *r >= *key.params.modulus().as_ref() {
            continue;
        }
        let Some(inv) = Option::<BoxedUint>::from(r.invert_odd_mod(key.params.modulus())) else {
            continue;
        };
        let inv = Zeroizing::new(inv);
        let r_m = Zeroizing::new(BoxedMontyForm::new((*r).clone(), &key.params));
        // e and its bit length are public. Only d uses full fixed public width.
        let r_e = Zeroizing::new(r_m.pow_bounded_exp(&key.e, key.e.bits_vartime()));
        let message = Zeroizing::new(BoxedMontyForm::new((*m).clone(), &key.params));
        let blinded = Zeroizing::new(&*message * &*r_e);
        let powered = Zeroizing::new(blinded.pow(&key.d));
        let unblinder = Zeroizing::new(BoxedMontyForm::new((*inv).clone(), &key.params));
        let signature = Zeroizing::new((&*powered * &*unblinder).retrieve());
        // Verify before releasing any result, including all fault/error paths.
        let public_check = BoxedMontyForm::new((*signature).clone(), &key.params)
            .pow_bounded_exp(&key.e, key.e.bits_vartime())
            .retrieve();
        if public_check != *m {
            return Err(error());
        }
        let bytes = signature.to_be_bytes();
        return Ok(bytes[bytes.len() - key.bytes..].to_vec());
    }
    Err(error())
}
// Narrow copies of the upstream RSA signing encoders; no unpadding routines.
fn pkcs_encode(
    prefix: &[u8],
    hash: &[u8],
    len: usize,
) -> Result<Zeroizing<Vec<u8>>, rustls::Error> {
    let t_len = prefix.len() + hash.len();
    if len < t_len + 11 {
        return Err(error());
    }
    let mut em = Zeroizing::new(vec![0xff; len]);
    em[0] = 0;
    em[1] = 1;
    em[len - t_len - 1] = 0;
    em[len - t_len..len - hash.len()].copy_from_slice(prefix);
    em[len - hash.len()..].copy_from_slice(hash);
    Ok(em)
}
fn pss_encode<D: Digest>(
    message: &[u8],
    em_bits: usize,
) -> Result<Zeroizing<Vec<u8>>, rustls::Error> {
    let m_hash = D::digest(message);
    let h_len = m_hash.len();
    let em_len = em_bits.div_ceil(8);
    if em_len < 2 * h_len + 2 {
        return Err(error());
    }
    let mut salt = Zeroizing::new(vec![0; h_len]);
    getrandom::fill(&mut salt).map_err(|_| rustls::Error::FailedToGetRandomBytes)?;
    let mut h = D::new();
    h.update([0; 8]);
    h.update(&m_hash);
    h.update(&salt);
    let hashed = h.finalize();
    let mut em = Zeroizing::new(vec![0; em_len]);
    let db_len = em_len - h_len - 1;
    em[db_len - h_len - 1] = 1;
    em[db_len - h_len..db_len].copy_from_slice(&salt);
    // MGF1 XOR, counter and lengths are public and bounded by the RSA key size.
    for (counter, chunk) in em[..db_len].chunks_mut(h_len).enumerate() {
        let mut mgf = D::new();
        mgf.update(&hashed);
        mgf.update((counter as u32).to_be_bytes());
        for (x, mask) in chunk.iter_mut().zip(mgf.finalize()) {
            *x ^= mask
        }
    }
    em[0] &= 0xff >> (8 * em_len - em_bits);
    em[db_len..em_len - 1].copy_from_slice(&hashed);
    em[em_len - 1] = 0xbc;
    Ok(em)
}

#[cfg(test)]
mod tests {
    use super::*;
    fn fixture() -> Inner {
        use rustls_pki_types::pem::PemObject;
        let key = PrivateKeyDer::from_pem_slice(include_bytes!(
            "../tests/fixtures/expanded/rsa-pkcs1-key.pem"
        ))
        .unwrap();
        let parsed = rsa::pkcs1::RsaPrivateKey::from_der(key.secret_der()).unwrap();
        let bits = (parsed.modulus.as_bytes().len() * 8) as u32;
        let n = BoxedUint::from_be_slice(parsed.modulus.as_bytes(), bits).unwrap();
        Inner {
            d: Zeroizing::new(
                BoxedUint::from_be_slice(parsed.private_exponent.as_bytes(), bits).unwrap(),
            ),
            e: BoxedUint::from_be_slice(parsed.public_exponent.as_bytes(), bits).unwrap(),
            params: BoxedMontyParams::new(Option::<Odd<BoxedUint>>::from(Odd::new(n)).unwrap()),
            bits,
            bytes: bits as usize / 8,
            n_bits: bits as usize,
        }
    }
    #[test]
    fn rsa_signing_requires_entropy_and_contributory_blinding_without_fallback() {
        let key = fixture();
        let mut em = vec![0; key.bytes];
        *em.last_mut().unwrap() = 42;
        assert!(
            private_signature_with_rng(&key, &em, |_| Err(getrandom::Error::UNSUPPORTED)).is_err()
        );
        let mut calls = 0;
        assert!(
            private_signature_with_rng(&key, &em, |b| {
                calls += 1;
                b.fill(0);
                Ok(())
            })
            .is_err()
        );
        assert_eq!(calls, 64);
    }
    #[test]
    fn rsa_fault_check_rejects_corrupted_private_exponent_instead_of_returning_signature() {
        let mut key = fixture();
        *key.d = BoxedUint::one_with_precision(key.bits);
        let mut em = vec![0; key.bytes];
        *em.last_mut().unwrap() = 42;
        assert!(private_signature_primitive(&key, &em).is_err());
    }
}
