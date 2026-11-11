//! Embedded-TURN credentials (`pkg/service/turn.go`, `TURNAuthHandler`):
//!
//! * username = base62(`"<apiKey>|<participantSid>|<expiry unix s>"`)
//! * password = base62(sha256(`"<secret>|<participantSid>|<expiry>"`))
//! * long-term key = MD5(`username:realm:password`) ([`rtc_turn::long_term_key`])
//!
//! Expiry is only enforced for Allocate; Refresh/CreatePermission/ChannelBind/
//! Send keep working past it so long sessions survive.
//!
//! base62 is a port of `github.com/jxskiss/base62` v1.1.0 (`StdEncoding`,
//! alphabet `A-Za-z0-9`, the v2 bit-packing encoder with the 5-bit "compact"
//! symbols 30/31) rather than a big-integer encoding, so usernames and passwords
//! are byte-identical to upstream. Only internal consistency is required (the
//! client treats both as opaque), and a faithful port gives that plus wire parity.

use std::sync::Arc;
use std::time::Duration;

use rtc_core::ids::ParticipantSid;
use rtc_turn::{AuthHandler, AuthRequest, Method, UnixAuthRequest};
use sha2::{Digest, Sha256};

use crate::auth::KeyStore;

/// `config.DefaultTURNTTLSeconds`.
pub const DEFAULT_TTL_SECS: u64 = 300;
/// `config.TURNMaxTTLSeconds`.
pub const MAX_TTL_SECS: u64 = 24 * 60 * 60;

pub struct TurnAuth {
    keys: Arc<KeyStore>,
}

impl TurnAuth {
    pub fn new(keys: Arc<KeyStore>) -> Self {
        Self { keys }
    }

    /// `CreateUsername` + `CreatePassword` → `(username, password)`. `ttl` is
    /// clamped like `ClampTURNTTLSeconds` (0 → 300 s, > 24 h → 24 h). The API key
    /// is the one the participant authenticated with, so it is always known; if
    /// it is not, the password is empty (authentication will fail) and a warning
    /// is logged. Use [`TurnAuth::create_credentials_at`] to observe that case.
    pub fn create_credentials(
        &self,
        api_key: &str,
        participant_sid: &ParticipantSid,
        ttl: Duration,
    ) -> (String, String) {
        match self.create_credentials_at(api_key, participant_sid, ttl, rtc_core::util::unix_s()) {
            Some(c) => c,
            None => {
                tracing::warn!(api_key, "could not create TURN password: unknown API key");
                let username = self
                    .username_at(api_key, participant_sid, ttl, rtc_core::util::unix_s())
                    .0;
                (username, String::new())
            }
        }
    }

    fn username_at(
        &self,
        api_key: &str,
        participant_sid: &ParticipantSid,
        ttl: Duration,
        now_s: i64,
    ) -> (String, i64) {
        let ttl = match ttl.as_secs() {
            0 => DEFAULT_TTL_SECS,
            t if t > MAX_TTL_SECS => MAX_TTL_SECS,
            t => t,
        };
        let expiry = now_s + ttl as i64;
        (
            encode(format!("{api_key}|{}|{expiry}", participant_sid.as_str()).as_bytes()),
            expiry,
        )
    }

    pub fn create_credentials_at(
        &self,
        api_key: &str,
        participant_sid: &ParticipantSid,
        ttl: Duration,
        now_s: i64,
    ) -> Option<(String, String)> {
        let (username, expiry) = self.username_at(api_key, participant_sid, ttl, now_s);
        let password = self.password(api_key, participant_sid.as_str(), expiry)?;
        Some((username, password))
    }

    fn password(&self, api_key: &str, pid: &str, expiry: i64) -> Option<String> {
        let secret = self.keys.secret(api_key)?;
        let sum = Sha256::digest(format!("{secret}|{pid}|{expiry}").as_bytes());
        Some(encode(&sum[..]))
    }

    /// `HandleAuth` at `now_s`.
    pub fn authenticate_at(&self, req: &AuthRequest<'_>, now_s: i64) -> Option<(String, [u8; 16])> {
        self.authenticate_credentials_at(req.username, req.realm, req.method, now_s)
    }

    fn authenticate_credentials_at(
        &self,
        username: &str,
        realm: &str,
        method: Method,
        now_s: i64,
    ) -> Option<(String, [u8; 16])> {
        let decoded = decode(username)?;
        let decoded = String::from_utf8_lossy(&decoded);
        let parts: Vec<&str> = decoded.split('|').collect();
        if parts.len() != 3 {
            return None;
        }
        let expiry: i64 = parts[2].parse().ok()?;
        if expiry == 0 {
            return None;
        }
        if now_s > expiry && method == Method::Allocate {
            tracing::info!(participant = parts[1], expiry, "TURN credential expired");
            return None;
        }
        let password = self.password(parts[0], parts[1], expiry)?;
        // Upstream hashes with the fixed realm "livekit"; the TURN server passes
        // the realm it challenged with, which is the same value when configured
        // like upstream.
        Some((
            parts[1].to_string(),
            rtc_turn::long_term_key(username, realm, &password),
        ))
    }
}

impl AuthHandler for TurnAuth {
    fn authenticate(&self, req: &AuthRequest<'_>) -> Option<(String, [u8; 16])> {
        self.authenticate_at(req, rtc_core::util::unix_s())
    }

    fn authenticate_unix(&self, req: &UnixAuthRequest<'_>) -> Option<(String, [u8; 16])> {
        self.authenticate_credentials_at(
            req.username,
            req.realm,
            req.method,
            rtc_core::util::unix_s(),
        )
    }
}

// ------------------------------------------------------------------ base62

#[cfg(test)]
const ALPHABET: &[u8; 62] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
const COMPACT_MASK: u8 = 0x1E;

/// jxskiss/base62 `Encode` (v2 encoder).
pub use rtc_core::util::base62_encode as encode;

fn decode_symbol(c: u8) -> Option<u8> {
    match c {
        b'A'..=b'Z' => Some(c - b'A'),
        b'a'..=b'z' => Some(c - b'a' + 26),
        b'0'..=b'9' => Some(c - b'0' + 52),
        _ => None,
    }
}

/// jxskiss/base62 `Decode`; `None` on an illegal character.
pub fn decode(src: &str) -> Option<Vec<u8>> {
    let bytes = src.as_bytes();
    // Bytes are produced least-significant first; reversed at the end.
    let mut out = Vec::with_capacity(bytes.len() * 6 / 8 + 1);
    let mut pos: u32 = 0;
    let mut acc: u32 = 0;
    for (i, &c) in bytes.iter().enumerate() {
        let x = decode_symbol(c)?;
        acc |= (x as u32) << pos;
        if i == bytes.len() - 1 {
            pos += 8 - x.leading_zeros(); // bits.Len8(x)
        } else if x & COMPACT_MASK == COMPACT_MASK {
            pos += 5;
        } else {
            pos += 6;
        }
        if pos >= 8 {
            out.push(acc as u8);
            pos %= 8;
            acc >>= 8;
        }
    }
    if pos > 0 {
        out.push(acc as u8);
    }
    out.reverse();
    Some(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::SocketAddr;

    #[test]
    fn base62_round_trip() {
        let samples: Vec<Vec<u8>> = vec![
            b"".to_vec(),
            b"a".to_vec(),
            b"APIkey|PA_abcdefghijkl|1700000000".to_vec(),
            (0u8..=255).collect(),
            vec![0xff; 33],
            vec![0x00; 7],
            vec![0x78, 0x7c, 0x3e, 0x1f],
        ];
        for s in samples {
            let e = encode(&s);
            assert!(e.bytes().all(|c| ALPHABET.contains(&c)));
            assert_eq!(decode(&e).unwrap(), s, "round trip of {s:?} via {e}");
        }
        assert!(decode("ab-c").is_none());
    }

    #[test]
    fn base62_round_trip_random() {
        for len in 1..64 {
            let mut buf = vec![0u8; len];
            rtc_core::util::fill_random(&mut buf);
            assert_eq!(decode(&encode(&buf)).unwrap(), buf);
        }
    }

    fn auth() -> TurnAuth {
        TurnAuth::new(Arc::new(KeyStore::new(vec![(
            "key".into(),
            "secret".into(),
        )])))
    }

    fn req<'a>(username: &'a str, method: Method) -> AuthRequest<'a> {
        AuthRequest {
            username,
            realm: "livekit",
            method,
            src: SocketAddr::from(([127, 0, 0, 1], 5000)),
        }
    }

    #[test]
    fn credentials_verify_and_expiry_only_on_allocate() {
        let a = auth();
        let pid = ParticipantSid("PA_x".into());
        let (user, pass) = a
            .create_credentials_at("key", &pid, Duration::from_secs(60), 1_000)
            .unwrap();
        let (uid, key) = a
            .authenticate_at(&req(&user, Method::Allocate), 1_030)
            .unwrap();
        assert_eq!(uid, "PA_x");
        assert_eq!(key, rtc_turn::long_term_key(&user, "livekit", &pass));
        assert!(
            a.authenticate_at(&req(&user, Method::Allocate), 1_061)
                .is_none()
        );
        assert!(
            a.authenticate_at(&req(&user, Method::Refresh), 5_000)
                .is_some()
        );
        assert!(
            a.create_credentials_at("nokey", &pid, Duration::ZERO, 0)
                .is_none()
        );
        assert!(
            a.authenticate_at(&req("garbage!", Method::Allocate), 0)
                .is_none()
        );
    }

    #[test]
    fn ttl_clamp() {
        let a = auth();
        let pid = ParticipantSid("PA_x".into());
        let (user, _) = a
            .create_credentials_at("key", &pid, Duration::ZERO, 0)
            .unwrap();
        let decoded = String::from_utf8(decode(&user).unwrap()).unwrap();
        assert_eq!(decoded, "key|PA_x|300");
        let (user, _) = a
            .create_credentials_at("key", &pid, Duration::from_secs(1 << 30), 0)
            .unwrap();
        let decoded = String::from_utf8(decode(&user).unwrap()).unwrap();
        assert_eq!(decoded, "key|PA_x|86400");
    }
}
