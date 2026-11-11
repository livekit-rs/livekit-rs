//! Stateless nonces: an HMAC-SHA256 signed timestamp under a per-process
//! random key. A nonce is valid for [`NONCE_LIFETIME_SECS`]; afterwards (or
//! when it fails verification) the server answers 438 Stale Nonce.

use std::time::{SystemTime, UNIX_EPOCH};

use hmac::{Hmac, Mac};
use sha2::Sha256;
use subtle::ConstantTimeEq;

type HmacSha256 = Hmac<Sha256>;

/// Nonce validity window.
pub(crate) const NONCE_LIFETIME_SECS: u64 = 600;
/// Tolerated clock skew for nonces issued "in the future".
const MAX_FUTURE_SKEW_SECS: u64 = 5;
const TAG_LEN: usize = 8;
/// 16 hex chars timestamp + 16 hex chars tag.
const NONCE_LEN: usize = 16 + TAG_LEN * 2;

pub(crate) struct NonceManager {
    key: [u8; 32],
}

impl NonceManager {
    pub(crate) fn new() -> NonceManager {
        let mut key = [0u8; 32];
        rand::fill(&mut key[..]);
        NonceManager { key }
    }

    #[cfg(test)]
    fn with_key(key: [u8; 32]) -> NonceManager {
        NonceManager { key }
    }

    fn tag(&self, ts: u64) -> [u8; TAG_LEN] {
        let mut mac =
            HmacSha256::new_from_slice(&self.key).expect("HMAC accepts keys of any length");
        mac.update(b"rtc-turn-nonce");
        mac.update(&ts.to_be_bytes());
        let full = mac.finalize().into_bytes();
        let mut out = [0u8; TAG_LEN];
        out.copy_from_slice(&full[..TAG_LEN]);
        out
    }

    /// Issues a fresh nonce.
    pub(crate) fn generate(&self) -> String {
        self.generate_at(now_secs())
    }

    pub(crate) fn generate_at(&self, now: u64) -> String {
        let mut s = String::with_capacity(NONCE_LEN);
        push_hex(&mut s, &now.to_be_bytes());
        push_hex(&mut s, &self.tag(now));
        s
    }

    /// Returns true when the nonce was issued by this server and is not stale.
    pub(crate) fn validate(&self, nonce: &[u8]) -> bool {
        self.validate_at(nonce, now_secs())
    }

    pub(crate) fn validate_at(&self, nonce: &[u8], now: u64) -> bool {
        if nonce.len() != NONCE_LEN {
            return false;
        }
        let mut ts_bytes = [0u8; 8];
        let mut tag = [0u8; TAG_LEN];
        if !parse_hex(&nonce[..16], &mut ts_bytes) || !parse_hex(&nonce[16..], &mut tag) {
            return false;
        }
        let ts = u64::from_be_bytes(ts_bytes);
        let expected = self.tag(ts);
        let valid_tag: bool = expected[..].ct_eq(&tag[..]).into();
        if !valid_tag {
            return false;
        }
        ts <= now.saturating_add(MAX_FUTURE_SKEW_SECS)
            && now.saturating_sub(ts) <= NONCE_LIFETIME_SECS
    }
}

fn now_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

fn push_hex(s: &mut String, bytes: &[u8]) {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    for b in bytes {
        s.push(HEX[(b >> 4) as usize] as char);
        s.push(HEX[(b & 0x0f) as usize] as char);
    }
}

fn parse_hex(src: &[u8], out: &mut [u8]) -> bool {
    if src.len() != out.len() * 2 {
        return false;
    }
    fn nib(c: u8) -> Option<u8> {
        match c {
            b'0'..=b'9' => Some(c - b'0'),
            b'a'..=b'f' => Some(c - b'a' + 10),
            _ => None,
        }
    }
    for (i, o) in out.iter_mut().enumerate() {
        let (Some(h), Some(l)) = (nib(src[2 * i]), nib(src[2 * i + 1])) else {
            return false;
        };
        *o = (h << 4) | l;
    }
    true
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fresh_nonce_is_valid() {
        let m = NonceManager::with_key([1u8; 32]);
        let n = m.generate_at(1_000_000);
        assert_eq!(n.len(), NONCE_LEN);
        assert!(m.validate_at(n.as_bytes(), 1_000_000));
        assert!(m.validate_at(n.as_bytes(), 1_000_000 + NONCE_LIFETIME_SECS));
    }

    #[test]
    fn expired_nonce_is_stale() {
        let m = NonceManager::with_key([1u8; 32]);
        let n = m.generate_at(1_000_000);
        assert!(!m.validate_at(n.as_bytes(), 1_000_000 + NONCE_LIFETIME_SECS + 1));
    }

    #[test]
    fn future_nonce_rejected() {
        let m = NonceManager::with_key([1u8; 32]);
        let n = m.generate_at(1_000_100);
        assert!(!m.validate_at(n.as_bytes(), 1_000_000));
    }

    #[test]
    fn foreign_or_tampered_nonce_rejected() {
        let a = NonceManager::with_key([1u8; 32]);
        let b = NonceManager::with_key([2u8; 32]);
        let n = a.generate_at(1_000_000);
        assert!(!b.validate_at(n.as_bytes(), 1_000_000));

        // Shift the timestamp while keeping the tag: must fail.
        let mut t = n.clone().into_bytes();
        t[15] = if t[15] == b'0' { b'1' } else { b'0' };
        assert!(!a.validate_at(&t, 1_000_000));

        assert!(!a.validate_at(b"short", 1_000_000));
        assert!(!a.validate_at(&[b'z'; NONCE_LEN], 1_000_000));
    }

    #[test]
    fn real_clock_round_trip() {
        let m = NonceManager::new();
        let n = m.generate();
        assert!(m.validate(n.as_bytes()));
    }
}
