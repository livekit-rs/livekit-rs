//! Long-term credential authentication hooks (RFC 8489 §9.2).

use std::net::SocketAddr;

use md5::{Digest, Md5};

use crate::stun::Method;

/// Information about an authenticated TURN request handed to [`AuthHandler`].
#[derive(Debug, Clone, Copy)]
pub struct AuthRequest<'a> {
    pub username: &'a str,
    pub realm: &'a str,
    pub method: Method,
    pub src: SocketAddr,
}

/// Transport family declared by a trusted AF_UNIX PROXY v2 header.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum UnixProxyTransport {
    Stream,
    Datagram,
}

/// Logical source and destination names supplied by a trusted proxy.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct UnixProxyAddress {
    pub transport: UnixProxyTransport,
    pub source: Box<[u8]>,
    pub destination: Box<[u8]>,
}

/// An authenticated request on a connection with an AF_UNIX PROXY address.
#[derive(Debug, Clone, Copy)]
pub struct UnixAuthRequest<'a> {
    pub username: &'a str,
    pub realm: &'a str,
    pub method: Method,
    pub src: &'a UnixProxyAddress,
}

/// Credential lookup for long-term authentication.
///
/// Mirrors LiveKit's `TURNAuthHandler.HandleAuth`: implementations are
/// expected to reject an expired credential only when `req.method` is
/// [`Method::Allocate`], so existing allocations can keep refreshing,
/// creating permissions and binding channels past the credential TTL.
pub trait AuthHandler: Send + Sync + 'static {
    /// Returns (user_id used for quota, long-term key = MD5("username:realm:password")).
    fn authenticate(&self, req: &AuthRequest<'_>) -> Option<(String, [u8; 16])>;

    /// Unix addresses must be handled explicitly. An IP-based policy cannot
    /// safely authorize them by substituting the proxy's TCP peer address.
    fn authenticate_unix(&self, _req: &UnixAuthRequest<'_>) -> Option<(String, [u8; 16])> {
        None
    }
}

/// Computes the long-term credential key `MD5(username ":" realm ":" password)`.
pub fn long_term_key(username: &str, realm: &str, password: &str) -> [u8; 16] {
    let mut h = Md5::new();
    h.update(username.as_bytes());
    h.update(b":");
    h.update(realm.as_bytes());
    h.update(b":");
    h.update(password.as_bytes());
    h.finalize().into()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn long_term_key_known_value() {
        // MD5("user:realm:pass")
        let expected: [u8; 16] = [
            0x84, 0x93, 0xfb, 0xc5, 0x3b, 0xa5, 0x82, 0xfb, 0x4c, 0x04, 0x4c, 0x45, 0x6b, 0xdc,
            0x40, 0xeb,
        ];
        assert_eq!(long_term_key("user", "realm", "pass"), expected);
    }

    #[test]
    fn long_term_key_differs_per_realm() {
        assert_ne!(
            long_term_key("user", "livekit", "pass"),
            long_term_key("user", "realm", "pass")
        );
    }
}
