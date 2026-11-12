//! Pure-Rust (RustCrypto) QUIC packet/header protection per RFC 9001.
//!
//! This module is the crypto core for the WebTransport (HTTP/3) adapter
//! (SPEC-0074 C15). It is intentionally free of `ring` / `aws-lc-rs` /
//! `openssl` — every primitive is a RustCrypto crate already vendored in the
//! workspace (`aes`, `aes-gcm`, `hmac`, `sha2`, `subtle`), so `cargo deny`
//! stays green with no exception. This core covers the mandatory-to-implement
//! `AEAD_AES_128_GCM` suite. The ChaCha20-Poly1305 suite and the quinn
//! `crypto::{Session,PacketKey,HeaderKey,...}` trait glue are implemented in
//! `rtc-tls` (`quic::ExpandedQuicAlgorithm`, verified by the RFC 9001 A.5 ChaCha
//! known-answer test, and `quinn::TlsSession`), which the live `lk-webtransport`
//! client/server configs use (SPEC-0074 C15).
//!
//! Test coverage: the primitives (key schedule, AEAD packet protection, AES-ECB
//! header mask, Retry, key update) and the *complete* Appendix A.2 Client Initial
//! and A.3 Server Initial packets are verified as known-answer tests against the
//! published RFC 9001 bytes (unprotected header + CRYPTO/PADDING payload + full
//! protected output). Those whole-packet vectors pin the `pn_offset + 4`
//! header-protection sample offset for both 4-byte (A.2) and 2-byte (A.3)
//! packet-number encodings. The quinn `crypto::Session` glue (TLS 1.3 handshake +
//! transport parameters + key handoff) and the ChaCha20-Poly1305 suite are
//! implemented in `rtc-tls` and reach the live WebTransport endpoint; this core
//! remains the AES-128-GCM primitive layer they wrap (SPEC-0074 C15).

use aes::Aes128;
use aes::cipher::{BlockEncrypt, KeyInit as CipherKeyInit, generic_array::GenericArray};
use aes_gcm::aead::{Aead, Payload};
use aes_gcm::{Aes128Gcm, Nonce as AesGcmNonce};
use hmac::{Hmac, Mac};
use sha2::Sha256;

/// Suite-wide constants for the AES-128-GCM / SHA-256 QUIC version 1.
pub const KEY_LEN: usize = 16;
pub const IV_LEN: usize = 12;
pub const HP_LEN: usize = 16;
pub const AEAD_TAG_LEN: usize = 16;
/// RFC 9001 §5.2 initial salt for version 1.
pub const INITIAL_SALT_V1: [u8; 20] = [
    0x38, 0x76, 0x2c, 0xf7, 0xf5, 0x59, 0x34, 0xb3, 0x4d, 0x17, 0x9a, 0xe6, 0xa4, 0xc8, 0x0c, 0xad,
    0xcc, 0xbb, 0x7f, 0x0a,
];
/// RFC 9001 §5.8 Retry integrity key/nonce for version 1 (fixed).
pub const RETRY_KEY_V1: [u8; 16] = [
    0xbe, 0x0c, 0x69, 0x0b, 0x9f, 0x66, 0x57, 0x5a, 0x1d, 0x76, 0x6b, 0x54, 0xe3, 0x68, 0xc8, 0x4e,
];
pub const RETRY_NONCE_V1: [u8; 12] = [
    0x46, 0x15, 0x99, 0xd3, 0x5d, 0x63, 0x2b, 0xf2, 0x23, 0x98, 0x25, 0xbb,
];

/// Error type for the protection primitives.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CryptoError {
    /// AEAD open failed (tag mismatch / tampered ciphertext / wrong key).
    OpenFailed,
    /// Caller supplied a key/IV of the wrong length for this suite.
    InvalidLength,
}

type HmacSha256 = Hmac<Sha256>;

// --- HKDF (RFC 5869) + TLS 1.3 HKDF-Expand-Label, over hmac+sha2 -------------

fn hkdf_extract(salt: &[u8], ikm: &[u8]) -> [u8; 32] {
    let mut mac = <HmacSha256 as Mac>::new_from_slice(salt).expect("HMAC accepts any key length");
    mac.update(ikm);
    let mut out = [0u8; 32];
    out.copy_from_slice(&mac.finalize().into_bytes());
    out
}

fn hkdf_expand(prk: &[u8; 32], info: &[u8], len: usize) -> Vec<u8> {
    // RFC 5869 §2.3: L <= 255 * HashLen. Refuse (empty) rather than overflow the
    // single-byte expand counter past 255 blocks.
    if len > 255 * 32 {
        return Vec::new();
    }
    let mut out = Vec::with_capacity(len);
    let mut t: Vec<u8> = Vec::new();
    for counter in 1u8..=255u8 {
        if out.len() >= len {
            break;
        }
        let mut mac =
            <HmacSha256 as Mac>::new_from_slice(prk).expect("HMAC accepts any key length");
        mac.update(&t);
        mac.update(info);
        mac.update(&[counter]);
        let block = mac.finalize().into_bytes();
        out.extend_from_slice(&block);
        t = block.to_vec();
    }
    out.truncate(len);
    out
}

/// TLS 1.3 `HKDF-Expand-Label` (RFC 8446 §7.1) used by QUIC key derivation.
///
/// Returns an empty `Vec` for inputs that cannot be encoded into the
/// `HkdfLabel` (label/context longer than 255 bytes, or a requested length
/// above the `uint16`/`255*HashLen` caps) instead of silently truncating.
pub fn hkdf_expand_label(secret: &[u8; 32], label: &str, context: &[u8], len: usize) -> Vec<u8> {
    // HkdfLabel { uint16 length; opaque label<7..255> /* "tls13 "+label */;
    //             opaque context<0..255>; }
    let full_label = format!("tls13 {label}");
    if full_label.len() > 255 || context.len() > 255 || len > u16::MAX as usize || len > 255 * 32 {
        return Vec::new();
    }
    let mut info = Vec::with_capacity(3 + full_label.len() + context.len() + 1);
    info.extend_from_slice(&(len as u16).to_be_bytes());
    info.push(full_label.len() as u8);
    info.extend_from_slice(full_label.as_bytes());
    info.push(context.len() as u8);
    info.extend_from_slice(context);
    hkdf_expand(secret, &info, len)
}

/// Derived packet keys for one (direction, encryption-level).
#[derive(Clone, Copy, PartialEq, Eq)]
pub struct PacketKeys {
    pub key: [u8; KEY_LEN],
    pub iv: [u8; IV_LEN],
    pub hp: [u8; HP_LEN],
}

impl PacketKeys {
    fn from_secret(secret: &[u8; 32]) -> Self {
        let mut key = [0u8; KEY_LEN];
        let mut iv = [0u8; IV_LEN];
        let mut hp = [0u8; HP_LEN];
        key.copy_from_slice(&hkdf_expand_label(secret, "quic key", b"", KEY_LEN));
        iv.copy_from_slice(&hkdf_expand_label(secret, "quic iv", b"", IV_LEN));
        hp.copy_from_slice(&hkdf_expand_label(secret, "quic hp", b"", HP_LEN));
        Self { key, iv, hp }
    }
}

/// RFC 9001 §5.2 Initial secrets from the client's Destination Connection ID.
pub fn initial_keys(dst_cid: &[u8]) -> (PacketKeys, PacketKeys) {
    let initial_secret = hkdf_extract(&INITIAL_SALT_V1, dst_cid);
    let client = hkdf_expand_label(&initial_secret, "client in", b"", 32);
    let server = hkdf_expand_label(&initial_secret, "server in", b"", 32);
    let mut c = [0u8; 32];
    let mut s = [0u8; 32];
    c.copy_from_slice(&client);
    s.copy_from_slice(&server);
    (PacketKeys::from_secret(&c), PacketKeys::from_secret(&s))
}

/// Traffic keys from a raw TLS 1.3 `Secrets::export` secret (Handshake/Application).
pub fn keys_from_secret(secret: &[u8; 32]) -> PacketKeys {
    PacketKeys::from_secret(secret)
}

/// RFC 9001 §6.1: the secret used after a key update is
/// `HKDF-Expand-Label(secret, "quic ku", "", 32)`.
pub fn updated_secret(secret: &[u8; 32]) -> [u8; 32] {
    let mut out = [0u8; 32];
    out.copy_from_slice(&hkdf_expand_label(secret, "quic ku", b"", 32));
    out
}

// --- AEAD packet protection (RFC 9001 §5.3) ---------------------------------

/// nonce = IV with the packet number XOR-ed into its last 8 bytes (as 64-bit BE).
pub fn derive_nonce(iv: &[u8; IV_LEN], pn: u64) -> [u8; IV_LEN] {
    let mut n = *iv;
    let p = pn.to_be_bytes();
    for (dst, src) in n.iter_mut().skip(IV_LEN - 8).zip(p.iter()) {
        *dst ^= *src;
    }
    n
}

/// Encrypt `payload`, returning `ciphertext||tag`. `header` is the AEAD AAD
/// (the packet header up to and including the plaintext packet number).
pub fn aead_seal(
    key: &[u8; KEY_LEN],
    iv: &[u8; IV_LEN],
    pn: u64,
    header: &[u8],
    payload: &[u8],
) -> Result<Vec<u8>, CryptoError> {
    let cipher = Aes128Gcm::new_from_slice(key).map_err(|_| CryptoError::InvalidLength)?;
    let nonce = derive_nonce(iv, pn);
    let ng = AesGcmNonce::clone_from_slice(&nonce);
    cipher
        .encrypt(
            &ng,
            Payload {
                aad: header,
                msg: payload,
            },
        )
        .map_err(|_| CryptoError::OpenFailed)
}

/// Decrypt `ciphertext||tag`, returning the plaintext.
pub fn aead_open(
    key: &[u8; KEY_LEN],
    iv: &[u8; IV_LEN],
    pn: u64,
    header: &[u8],
    ciphertext: &[u8],
) -> Result<Vec<u8>, CryptoError> {
    if ciphertext.len() < AEAD_TAG_LEN {
        return Err(CryptoError::OpenFailed);
    }
    let cipher = Aes128Gcm::new_from_slice(key).map_err(|_| CryptoError::InvalidLength)?;
    let nonce = derive_nonce(iv, pn);
    let ng = AesGcmNonce::clone_from_slice(&nonce);
    cipher
        .decrypt(
            &ng,
            Payload {
                aad: header,
                msg: ciphertext,
            },
        )
        .map_err(|_| CryptoError::OpenFailed)
}

// --- Header protection (RFC 9001 §5.4, AES-based mask) ----------------------

/// 5-byte header-protection mask = `AES-ECB(hp_key, sample[0..16])`.
pub fn hp_mask(hp_key: &[u8; HP_LEN], sample: &[u8]) -> Result<[u8; 5], CryptoError> {
    if sample.len() < 16 {
        return Err(CryptoError::InvalidLength);
    }
    let cipher = Aes128::new_from_slice(hp_key).map_err(|_| CryptoError::InvalidLength)?;
    let mut blk = GenericArray::clone_from_slice(&sample[..16]);
    cipher.encrypt_block(&mut blk);
    let mut mask = [0u8; 5];
    mask.copy_from_slice(&blk[..5]);
    Ok(mask)
}

/// Low bits of `mask[0]` for first-byte XOR: `0x0f` for a long header (4 packet
/// number/length bits), `0x1f` for a short header (adds the key-phase bit).
pub fn hp_first_byte_mask(long_header: bool) -> u8 {
    if long_header { 0x0f } else { 0x1f }
}

/// XOR `packet[pn_offset..pn_offset+pn_len]` with `mask[1..1+pn_len]`.
/// Applied identically for protect and unprotect (XOR is its own inverse).
pub fn hp_xor_packet_number(packet: &mut [u8], pn_offset: usize, pn_len: usize, mask: &[u8; 5]) {
    for i in 0..pn_len {
        packet[pn_offset + i] ^= mask[1 + i];
    }
}

/// Apply header protection to a packet in place: `first_byte ^= mask[0] & bits`,
/// then XOR the packet-number bytes with `mask[1..]`.
///
/// The ciphertext sample is taken at `pn_offset + 4` for 16 bytes, independent
/// of `pn_len` (RFC 9001 §5.4.2). Malformed inputs (empty packet, `pn_len`
/// outside 1..=4, or an offset/sample that runs past the end) return
/// [`CryptoError`] rather than panicking.
pub fn hp_apply(
    packet: &mut [u8],
    pn_offset: usize,
    pn_len: usize,
    long_header: bool,
    hp_key: &[u8; HP_LEN],
) -> Result<(), CryptoError> {
    if pn_len == 0 || pn_len > 4 || packet.is_empty() {
        return Err(CryptoError::InvalidLength);
    }
    let pn_end = pn_offset
        .checked_add(pn_len)
        .ok_or(CryptoError::InvalidLength)?;
    if pn_offset >= packet.len() || pn_end > packet.len() {
        return Err(CryptoError::InvalidLength);
    }
    let sample_offset = pn_offset + 4;
    let sample_end = sample_offset
        .checked_add(16)
        .ok_or(CryptoError::InvalidLength)?;
    if sample_end > packet.len() {
        return Err(CryptoError::InvalidLength);
    }
    let mask = hp_mask(hp_key, &packet[sample_offset..sample_end])?;
    packet[0] ^= mask[0] & hp_first_byte_mask(long_header);
    hp_xor_packet_number(packet, pn_offset, pn_len, &mask);
    Ok(())
}

// --- Retry integrity (RFC 9001 §5.8) -----------------------------------------

/// Compute the 16-byte Retry Integrity Tag over the Retry Pseudo-Packet.
/// `retry_without_tag` is the transmitted Retry packet with the trailing 16-byte
/// tag removed (it starts with the `0xff` long-header byte + version).
pub fn retry_tag(odcid: &[u8], retry_without_tag: &[u8]) -> Result<[u8; 16], CryptoError> {
    let cipher =
        Aes128Gcm::new_from_slice(&RETRY_KEY_V1).map_err(|_| CryptoError::InvalidLength)?;
    let nonce = AesGcmNonce::clone_from_slice(&RETRY_NONCE_V1);
    let mut aad = Vec::with_capacity(retry_without_tag.len() + odcid.len() + 1);
    aad.push(odcid.len() as u8);
    aad.extend_from_slice(odcid);
    aad.extend_from_slice(retry_without_tag);
    let tag = cipher
        .encrypt(
            &nonce,
            Payload {
                aad: &aad,
                msg: b"",
            },
        )
        .map_err(|_| CryptoError::OpenFailed)?;
    let mut out = [0u8; 16];
    // With empty plaintext, the AEAD output is exactly the 16-byte tag.
    if tag.len() != 16 {
        return Err(CryptoError::OpenFailed);
    }
    out.copy_from_slice(&tag);
    Ok(out)
}

#[cfg(test)]
#[path = "tests.rs"]
mod tests;
/// RFC 9001 Appendix A whole-packet vectors. Public so downstream adapters
/// (the Quinn `crypto::HeaderKey`/`PacketKey` newtypes in `rtc-tls`) can pin
/// their wire behaviour against the same published bytes instead of a
/// same-stack round trip, which is symmetric and therefore blind to a
/// protection-width deviation.
pub mod vectors;
