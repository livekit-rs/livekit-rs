//! Small shared helpers.

use std::time::{SystemTime, UNIX_EPOCH};

/// Fill `buf` from the OS random source. Failure of the OS RNG is unrecoverable
/// for id and credential generation, so it aborts loudly.
pub fn fill_random(buf: &mut [u8]) {
    getrandom::fill(buf).expect("OS random source unavailable");
}

const BASE62: &[u8; 62] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";

/// `encoder.get6bits`: the 6 bits ending at bit position `pos` (counted from the
/// start of `src`, MSB first), reading the input from its end.
fn get6bits(src: &[u8], pos: usize) -> u8 {
    let mut r = pos & 7;
    let mut i = pos >> 3;
    if r == 0 {
        i -= 1;
        r = 8;
    }
    let mut b = src[i] >> (8 - r);
    if r < 6 && i > 0 {
        b |= src[i - 1] << r;
    }
    b & 0x3F
}

/// jxskiss/base62 `Encode` (v2 encoder).
pub fn base62_encode(src: &[u8]) -> String {
    let mut out = String::with_capacity(src.len() * 9 / 5 + 1);
    let mut pos = src.len() * 8;
    while pos > 0 {
        let mut size = 6;
        let mut b = get6bits(src, pos);
        if b & 0x1E == 0x1E {
            if pos > 6 || b > 0x1F {
                size = 5;
            }
            b &= 0x1F;
        }
        out.push(BASE62[b as usize] as char);
        pos = pos.saturating_sub(size);
    }
    out
}

/// protocol `utils.RandomSecret`: base62 of 32 random bytes.
pub fn random_secret() -> String {
    let mut buf = [0u8; 32];
    fill_random(&mut buf);
    base62_encode(&buf)
}

pub fn random_u64() -> u64 {
    let mut b = [0u8; 8];
    fill_random(&mut b);
    u64::from_le_bytes(b)
}

/// Unix time in milliseconds.
pub fn unix_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

/// Unix time in seconds.
pub fn unix_s() -> i64 {
    unix_ms() / 1000
}

/// Unix time in nanoseconds (used by upstream `TimedVersion`).
pub fn unix_ns() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos() as i64)
        .unwrap_or(0)
}
