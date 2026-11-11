//! rustls QUIC key-schedule bridge.
//!
//! rustls owns the TLS 1.3 handshake state machine and the QUIC key schedule
//! bookkeeping (initial salts, `HKDF-Expand-Label` over the traffic secrets with
//! the `quic key`/`quic iv`/`quic hp`/`quic ku` labels). Those operations run
//! through the suite's `hkdf_provider`, which is the existing RustCrypto HMAC in
//! [`crate::provider`]. The actual AEAD packet protection and AES-based header
//! protection for AES128 are delegated to the independently RFC 9001-verified
//! [`lk_quic_crypto`] core via the [`rustls::quic::Algorithm`] hook below.
//!
//! This keeps every cryptographic primitive in pure Rust (RustCrypto): no
//! `ring`/`aws-lc` is reachable through the QUIC path. The AES-128-GCM TLS 1.3
//! suite carries `quic: Some(&QuicAlgorithm)` so rustls advertises QUIC support
//! (`CryptoProvider::supports_protocol(Protocol::Quic)`).

use rustls::crypto::cipher::{AeadKey, Iv};
use rustls::quic::{HeaderProtectionKey, PacketKey, Tag};
use rustls::{Error, quic};

/// Confidentiality/integrity limits for AES-128-GCM QUIC packets (RFC 9001 §6.1
/// table): `2^23` protected packets before a key rotation, `2^52` failed
/// decryptions before the connection is abandoned.
const AEAD_CONFIDENTIALITY_LIMIT: u64 = 1 << 23;
const AEAD_INTEGRITY_LIMIT: u64 = 1 << 52;

/// QUIC header/packet protection algorithm backed by [`lk_quic_crypto`].
pub(crate) struct QuicAlgorithm;

impl quic::Algorithm for QuicAlgorithm {
    fn packet_key(&self, key: AeadKey, iv: Iv) -> Box<dyn PacketKey> {
        let mut k = [0u8; lk_quic_crypto::KEY_LEN];
        let mut n = [0u8; lk_quic_crypto::IV_LEN];
        k.copy_from_slice(key.as_ref());
        n.copy_from_slice(iv.as_ref());
        Box::new(QuicPacketKey { key: k, iv: n })
    }

    fn header_protection_key(&self, key: AeadKey) -> Box<dyn HeaderProtectionKey> {
        let mut k = [0u8; lk_quic_crypto::HP_LEN];
        k.copy_from_slice(key.as_ref());
        Box::new(QuicHeaderKey { key: k })
    }

    fn aead_key_len(&self) -> usize {
        lk_quic_crypto::KEY_LEN
    }

    fn fips(&self) -> bool {
        false
    }
}

/// AEAD packet protection (`AES-128-GCM`) via [`lk_quic_crypto::aead_seal`]/
/// [`lk_quic_crypto::aead_open`].
struct QuicPacketKey {
    key: [u8; lk_quic_crypto::KEY_LEN],
    iv: [u8; lk_quic_crypto::IV_LEN],
}

impl PacketKey for QuicPacketKey {
    fn encrypt_in_place(
        &self,
        packet_number: u64,
        header: &[u8],
        payload: &mut [u8],
    ) -> Result<Tag, Error> {
        // lk-quic-crypto returns `ciphertext || tag`; rustls hands us the
        // plaintext buffer and wants the tag returned separately.
        let sealed = lk_quic_crypto::aead_seal(&self.key, &self.iv, packet_number, header, payload)
            .map_err(|_| Error::EncryptError)?;
        let (ct, tag) = sealed.split_at(payload.len());
        payload.copy_from_slice(ct);
        Ok(Tag::from(tag))
    }

    fn decrypt_in_place<'a>(
        &self,
        packet_number: u64,
        header: &[u8],
        payload: &'a mut [u8],
    ) -> Result<&'a [u8], Error> {
        // `payload` is `ciphertext || tag` (in place); lk-quic-crypto copies the
        // plaintext out, so write it back to the front of the buffer.
        let plain = lk_quic_crypto::aead_open(&self.key, &self.iv, packet_number, header, payload)
            .map_err(|_| Error::DecryptError)?;
        let len = plain.len();
        payload[..len].copy_from_slice(&plain);
        Ok(&payload[..len])
    }

    fn tag_len(&self) -> usize {
        lk_quic_crypto::AEAD_TAG_LEN
    }

    fn confidentiality_limit(&self) -> u64 {
        AEAD_CONFIDENTIALITY_LIMIT
    }

    fn integrity_limit(&self) -> u64 {
        AEAD_INTEGRITY_LIMIT
    }
}

/// AES-based header protection (RFC 9001 §5.4) via [`lk_quic_crypto`].
///
/// rustls' lower-level interface hands us the 16-byte sample, the first header
/// byte and the packet-number slice separately, so the long/short distinction is
/// read back from the unprotected `0x80` fixed bit of the first byte.
struct QuicHeaderKey {
    key: [u8; lk_quic_crypto::HP_LEN],
}

impl QuicHeaderKey {
    fn apply(
        &self,
        sample: &[u8],
        first: &mut u8,
        packet_number: &mut [u8],
        masked: bool,
    ) -> Result<(), Error> {
        if sample.len() != 16 {
            return Err(Error::General(
                "invalid header protection sample length".into(),
            ));
        }
        let mask = lk_quic_crypto::hp_mask(&self.key, sample)
            .map_err(|_| Error::General("invalid header protection sample length".into()))?;
        apply_header_mask(&mask, first, packet_number, masked)
    }
}

impl HeaderProtectionKey for QuicHeaderKey {
    fn encrypt_in_place(
        &self,
        sample: &[u8],
        first: &mut u8,
        packet_number: &mut [u8],
    ) -> Result<(), Error> {
        self.apply(sample, first, packet_number, false)
    }

    fn decrypt_in_place(
        &self,
        sample: &[u8],
        first: &mut u8,
        packet_number: &mut [u8],
    ) -> Result<(), Error> {
        // Header protection is a pure XOR mask: protect and unprotect are the
        // same operation (RFC 9001 §5.4.2).
        self.apply(sample, first, packet_number, true)
    }

    fn sample_len(&self) -> usize {
        16
    }
}

/// RFC 9001 §5.4.1 / rustls HeaderProtectionKey: decrypt callers can supply
/// four bytes including payload after a shorter packet number. Only the
/// decoded PN length is XORed; validation must not partially alter input.
fn apply_header_mask(
    mask: &[u8; 5],
    first: &mut u8,
    packet_number: &mut [u8],
    masked: bool,
) -> Result<(), Error> {
    if packet_number.len() > 4 {
        return Err(Error::General("packet number too long".into()));
    }
    let bits = lk_quic_crypto::hp_first_byte_mask(*first & 0x80 != 0);
    let plain_first = if masked {
        *first ^ (mask[0] & bits)
    } else {
        *first
    };
    let pn_len = usize::from(plain_first & 3) + 1;
    if packet_number.len() < pn_len {
        return Err(Error::General("packet number truncated".into()));
    }
    *first ^= mask[0] & bits;
    for (byte, mask) in packet_number.iter_mut().take(pn_len).zip(&mask[1..]) {
        *byte ^= mask;
    }
    Ok(())
}

/// The remaining TLS 1.3 suites use the same RustCrypto primitives as the
/// TCP TLS adapters. Initial keys remain AES128 regardless of negotiated suite.
pub(crate) enum ExpandedQuicAlgorithm {
    Aes256,
    ChaCha20,
}

impl quic::Algorithm for ExpandedQuicAlgorithm {
    fn packet_key(&self, key: AeadKey, iv: Iv) -> Box<dyn PacketKey> {
        use chacha20poly1305::KeyInit;
        let cipher = match self {
            Self::Aes256 => ExpandedCipher::Aes256(
                aes_gcm_next::Aes256Gcm::new_from_slice(key.as_ref()).expect("AES256 key length"),
            ),
            Self::ChaCha20 => ExpandedCipher::ChaCha20(
                chacha20poly1305::ChaCha20Poly1305::new_from_slice(key.as_ref())
                    .expect("ChaCha20 key length"),
            ),
        };
        Box::new(ExpandedPacketKey { cipher, iv })
    }
    fn header_protection_key(&self, key: AeadKey) -> Box<dyn HeaderProtectionKey> {
        use aes_gcm::aes::cipher::KeyInit;
        let key = match self {
            Self::Aes256 => ExpandedHeaderKey::Aes256(
                aes_gcm::aes::Aes256::new_from_slice(key.as_ref()).expect("AES256 HP key length"),
            ),
            Self::ChaCha20 => ExpandedHeaderKey::ChaCha20(
                key.as_ref().try_into().expect("ChaCha20 HP key length"),
            ),
        };
        Box::new(key)
    }
    fn aead_key_len(&self) -> usize {
        32
    }
    fn fips(&self) -> bool {
        false
    }
}

enum ExpandedCipher {
    Aes256(aes_gcm_next::Aes256Gcm),
    ChaCha20(chacha20poly1305::ChaCha20Poly1305),
}
struct ExpandedPacketKey {
    cipher: ExpandedCipher,
    iv: Iv,
}
impl PacketKey for ExpandedPacketKey {
    fn encrypt_in_place(
        &self,
        packet_number: u64,
        header: &[u8],
        payload: &mut [u8],
    ) -> Result<Tag, Error> {
        use aead::AeadInOut;
        let nonce = rustls::crypto::cipher::Nonce::new(&self.iv, packet_number)
            .0
            .into();
        let tag = match &self.cipher {
            ExpandedCipher::Aes256(cipher) => {
                cipher.encrypt_inout_detached(&nonce, header, payload.into())
            }
            ExpandedCipher::ChaCha20(cipher) => {
                cipher.encrypt_inout_detached(&nonce, header, payload.into())
            }
        }
        .map_err(|_| Error::EncryptError)?;
        Ok(Tag::from(tag.as_slice()))
    }
    fn decrypt_in_place<'a>(
        &self,
        packet_number: u64,
        header: &[u8],
        payload: &'a mut [u8],
    ) -> Result<&'a [u8], Error> {
        use aead::AeadInOut;
        let len = payload.len().checked_sub(16).ok_or(Error::DecryptError)?;
        let (ciphertext, tag) = payload.split_at_mut(len);
        let nonce = rustls::crypto::cipher::Nonce::new(&self.iv, packet_number)
            .0
            .into();
        let tag = <[u8; 16]>::try_from(&*tag)
            .map_err(|_| Error::DecryptError)?
            .into();
        match &self.cipher {
            ExpandedCipher::Aes256(cipher) => {
                cipher.decrypt_inout_detached(&nonce, header, (&mut *ciphertext).into(), &tag)
            }
            ExpandedCipher::ChaCha20(cipher) => {
                cipher.decrypt_inout_detached(&nonce, header, (&mut *ciphertext).into(), &tag)
            }
        }
        .map_err(|_| Error::DecryptError)?;
        Ok(ciphertext)
    }
    fn tag_len(&self) -> usize {
        16
    }
    fn confidentiality_limit(&self) -> u64 {
        match self.cipher {
            ExpandedCipher::Aes256(_) => AEAD_CONFIDENTIALITY_LIMIT,
            ExpandedCipher::ChaCha20(_) => u64::MAX,
        }
    }
    fn integrity_limit(&self) -> u64 {
        match self.cipher {
            ExpandedCipher::Aes256(_) => AEAD_INTEGRITY_LIMIT,
            ExpandedCipher::ChaCha20(_) => 1 << 36,
        }
    }
}

enum ExpandedHeaderKey {
    Aes256(aes_gcm::aes::Aes256),
    ChaCha20([u8; 32]),
}
impl ExpandedHeaderKey {
    fn apply(
        &self,
        sample: &[u8],
        first: &mut u8,
        pn: &mut [u8],
        masked: bool,
    ) -> Result<(), Error> {
        if sample.len() != 16 {
            return Err(Error::General(
                "invalid header protection sample length".into(),
            ));
        }
        let mut mask = [0u8; 5];
        match self {
            Self::Aes256(cipher) => {
                use aes_gcm::aes::cipher::{BlockEncrypt, generic_array::GenericArray};
                let mut block = GenericArray::clone_from_slice(sample);
                cipher.encrypt_block(&mut block);
                mask.copy_from_slice(&block[..5]);
            }
            Self::ChaCha20(key) => {
                use chacha20::cipher::{KeyIvInit, StreamCipherCore};
                // Header protection generates exactly one independent block;
                // all 32-bit sample counters, including 0xffffffff, are valid.
                // The streaming wrapper intentionally reserves the last block
                // to prevent stream reuse, so use RustCrypto's block core here.
                let mut cipher = chacha20::ChaChaCore::<chacha20::R20, chacha20::variants::Ietf>::new_from_slices(key, &sample[4..])
                    .map_err(|_| Error::General("invalid ChaCha20 HP key".into()))?;
                let counter = u32::from_le_bytes(sample[..4].try_into().expect("4-byte counter"));
                cipher.set_block_pos(counter);
                let mut block = Default::default();
                cipher.write_keystream_block(&mut block);
                mask.copy_from_slice(&block[..5]);
            }
        }
        apply_header_mask(&mask, first, pn, masked)
    }
}
impl HeaderProtectionKey for ExpandedHeaderKey {
    fn encrypt_in_place(&self, sample: &[u8], first: &mut u8, pn: &mut [u8]) -> Result<(), Error> {
        self.apply(sample, first, pn, false)
    }
    fn decrypt_in_place(&self, sample: &[u8], first: &mut u8, pn: &mut [u8]) -> Result<(), Error> {
        self.apply(sample, first, pn, true)
    }
    fn sample_len(&self) -> usize {
        16
    }
}

#[cfg(test)]
mod expanded_tests {
    use super::*;
    use rustls::quic::Algorithm;
    fn hex<const N: usize>(text: &str) -> [u8; N] {
        assert_eq!(text.len(), N * 2);
        std::array::from_fn(|i| u8::from_str_radix(&text[i * 2..i * 2 + 2], 16).unwrap())
    }
    #[test]
    fn rfc9001_a5_chacha_short_packet_known_answer() {
        // https://www.rfc-editor.org/rfc/rfc9001.html#appendix-A.5
        let algorithm = ExpandedQuicAlgorithm::ChaCha20;
        let key = algorithm.packet_key(
            AeadKey::from(hex::<32>(
                "c6d98ff3441c3fe1b2182094f69caa2ed4b716b65488960a7a984979fb23e1c8",
            )),
            Iv::new(hex("e0459b3474bdd0e44a41c144")),
        );
        let hp = algorithm.header_protection_key(AeadKey::from(hex::<32>(
            "25a282b9e82f06f21f488917a4fc8f1b73573685608597d0efcb076b0ab7a7a4",
        )));
        let mut payload = [1];
        let tag = key
            .encrypt_in_place(654360564, &hex::<4>("4200bff4"), &mut payload)
            .unwrap();
        let mut ciphertext = payload.to_vec();
        ciphertext.extend_from_slice(tag.as_ref());
        assert_eq!(ciphertext, hex::<17>("655e5cd55c41f69080575d7999c25a5bfb"));
        let sample = hex::<16>("5e5cd55c41f69080575d7999c25a5bfb");
        let mut first = 0x42;
        // A fourth byte supplied for decryption belongs to ciphertext, not PN.
        let mut pn = [0x00, 0xbf, 0xf4, 0x65];
        hp.encrypt_in_place(&sample, &mut first, &mut pn).unwrap();
        assert_eq!(first, 0x4c);
        assert_eq!(pn, [0xfe, 0x41, 0x89, 0x65]);
        hp.decrypt_in_place(&sample, &mut first, &mut pn).unwrap();
        assert_eq!(first, 0x42);
        assert_eq!(pn, [0, 0xbf, 0xf4, 0x65]);
        assert_eq!(
            key.decrypt_in_place(654360564, &hex::<4>("4200bff4"), &mut ciphertext)
                .unwrap(),
            [1]
        );
        let mut bad = hex::<17>("655e5cd55c41f69080575d7999c25a5bfa");
        assert!(
            key.decrypt_in_place(654360564, &hex::<4>("4200bff4"), &mut bad)
                .is_err()
        );
        assert_eq!(key.integrity_limit(), 1 << 36);
    }
    #[test]
    fn aes256_zero_key_packet_and_header_known_answers() {
        // AES-256-GCM: 256-bit zero key, 96-bit zero IV, one zero block,
        // empty AAD. Header protection separately uses AES-256-ECB(zero block).
        let algorithm = ExpandedQuicAlgorithm::Aes256;
        let key = algorithm.packet_key(AeadKey::from([0; 32]), Iv::new([0; 12]));
        let mut payload = [0; 16];
        let tag = key.encrypt_in_place(0, &[], &mut payload).unwrap();
        assert_eq!(payload, hex::<16>("cea7403d4d606b6e074ec5d3baf39d18"));
        assert_eq!(tag.as_ref(), &hex::<16>("d0d1c8a799996bf0265b98b5d48ab919"));
        let hp = algorithm.header_protection_key(AeadKey::from([0; 32]));
        let mut first = 0xc3;
        let mut pn = [0; 4];
        hp.encrypt_in_place(&[0; 16], &mut first, &mut pn).unwrap();
        assert_eq!(first, 0xcf);
        assert_eq!(pn, hex::<4>("95c078a2"));
    }
    #[test]
    fn chacha_header_protection_accepts_maximum_sample_counter() {
        let hp = ExpandedQuicAlgorithm::ChaCha20.header_protection_key(AeadKey::from([0; 32]));
        let mut sample = [0; 16];
        sample[..4].fill(0xff);
        let mut first = 0x43;
        let mut pn = [0x12, 0x34, 0x56, 0x78];
        hp.encrypt_in_place(&sample, &mut first, &mut pn).unwrap();
        hp.decrypt_in_place(&sample, &mut first, &mut pn).unwrap();
        assert_eq!((first, pn), (0x43, [0x12, 0x34, 0x56, 0x78]));
    }

    #[test]
    fn header_mask_changes_only_actual_packet_number_bytes() {
        // Exercise the shared AES128/AES256/ChaCha application rule independently
        // of the primitive. 1/2/3/4-byte PN, short/long headers, padded slices.
        let mask = [0xae, 0xfe, 0xfe, 0x7d, 0x03];
        for form in [0x40, 0xc0] {
            for len in 1..=4 {
                let original_first = form | (len - 1) as u8;
                let original = [0x12, 0x34, 0x56, 0x78];
                let mut first = original_first;
                let mut pn = original;
                apply_header_mask(&mask, &mut first, &mut pn, false).unwrap();
                assert_eq!(&pn[len..], &original[len..]);
                apply_header_mask(&mask, &mut first, &mut pn, true).unwrap();
                assert_eq!((first, pn), (original_first, original));
            }
        }
        let mut first = 0xc3;
        let mut pn = [0x12; 3];
        assert!(apply_header_mask(&mask, &mut first, &mut pn, false).is_err());
        assert_eq!((first, pn), (0xc3, [0x12; 3]));
    }
}
