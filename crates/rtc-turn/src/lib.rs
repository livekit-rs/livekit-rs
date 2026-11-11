//! `rtc-turn`: an embedded TURN server for livekit-rs, modelled on LiveKit's
//! embedded pion/turn server (`pkg/service/turn.go`).
//!
//! # Implemented
//!
//! * STUN codec (RFC 5389/8489): header, magic cookie, transaction id,
//!   attribute padding, MESSAGE-INTEGRITY (HMAC-SHA1, constant-time verify),
//!   FINGERPRINT, ERROR-CODE, UNKNOWN-ATTRIBUTES (420 for unknown
//!   comprehension-required attributes), XOR-*-ADDRESS, etc.
//! * Binding requests answered with XOR-MAPPED-ADDRESS without authentication.
//! * Long-term credentials: 401 + REALM/NONCE challenge, 438 on stale or
//!   foreign nonce. Nonces are stateless HMAC-SHA256 signed timestamps valid
//!   for 10 minutes. Credential lookup is delegated to [`AuthHandler`]
//!   (LiveKit semantics: expired credentials are only rejected for Allocate).
//! * TURN (RFC 8656 / RFC 5766 subset), UDP relays only:
//!   Allocate (cached response for retransmits, 437 on mismatch, 442 for
//!   non-UDP REQUESTED-TRANSPORT, 440 for unsupported family, 486 per-user
//!   quota, 508 on capacity / port exhaustion after 50 attempts),
//!   Refresh (lifetime 0 deletes), CreatePermission (300 s), ChannelBind
//!   (0x4000..=0x7FFE, 600 s, rebind conflicts => 400), Send/Data indications
//!   and ChannelData in both directions.
//! * Peer policy mirroring LiveKit's `isRestrictedPeerIP`: restricted peers
//!   (loopback, RFC 1918, link-local, CGNAT, multicast, ULA, NAT64, ...) are
//!   denied (403) unless in `allow_restricted_peer_cidrs`; `deny_peer_cidrs`
//!   always wins.
//! * UDP, TCP and TLS (via a caller-provided [`StreamAcceptor`]) listeners.
//!   Stream allocations are bound to their connection and deleted on close.
//!
//! # Not supported
//!
//! * TCP relay allocations (RFC 6062) — rejected with 442.
//! * EVEN-PORT with the R (reserve) bit and RESERVATION-TOKEN — rejected
//!   (508, or 400 when combined with conflicting attributes). EVEN-PORT
//!   without R is honoured.
//! * Dual allocations (ADDITIONAL-ADDRESS-FAMILY is ignored), DONT-FRAGMENT
//!   (accepted, not enforced), MESSAGE-INTEGRITY-SHA256 / PASSWORD-ALGORITHM
//!   (ignored; HMAC-SHA1 is required), USERHASH, ALTERNATE-SERVER.
//! * Mobility (RFC 8016).
//!
//! The PROXY protocol (v1/v2) is parsed on TCP listeners when trusted proxy
//! CIDRs are configured (see [`TurnServerConfig::proxy_trusted_cidrs`]).

pub mod stun;

mod allocation;
mod auth;
mod nonce;
mod policy;
mod proxy;
mod server;
mod tcp;

pub use auth::{
    AuthHandler, AuthRequest, UnixAuthRequest, UnixProxyAddress, UnixProxyTransport, long_term_key,
};
pub use policy::LocalRelayPolicy;
pub use policy::is_restricted_peer_ip;
pub use server::{ListenerSpec, TurnError, TurnHandle, TurnServer, TurnServerConfig};
pub use stun::Method;
pub use tcp::{AcceptFuture, AsyncStream, StreamAcceptor};
