//! Network-facing types shared with the runtime's socket layer.

use std::net::SocketAddr;
use std::time::Instant;

use rtc_core::ids::TransportId;
use str0m::net::Protocol;

/// Transport protocol of a datagram or candidate. TCP means RFC 4571 framing has
/// already been removed by the socket layer.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Proto {
    Udp,
    Tcp,
}

impl Proto {
    pub(crate) fn to_str0m(self) -> Protocol {
        match self {
            Proto::Udp => Protocol::Udp,
            Proto::Tcp => Protocol::Tcp,
        }
    }

    pub(crate) fn from_str0m(p: Protocol) -> Proto {
        match p {
            Protocol::Udp => Proto::Udp,
            // SSL-TCP and TLS only appear for relayed candidates, which this server
            // never creates; they are TCP framed on our side regardless.
            Protocol::Tcp | Protocol::SslTcp | Protocol::Tls => Proto::Tcp,
        }
    }
}

/// One inbound datagram, already routed to a transport by the socket layer.
pub struct Datagram<'a> {
    pub transport: TransportId,
    pub proto: Proto,
    pub source: SocketAddr,
    pub destination: SocketAddr,
    pub data: &'a [u8],
    pub received_at: Instant,
}

/// Sink for outbound datagrams. Must not block (the runtime queues).
pub trait PacketOut: Send {
    /// Capture an output capability for this exact transport registration.
    /// Legacy/test sinks may keep the default shared output path. Runtime
    /// implementations bind a generation once, never infer it per packet.
    fn for_transport(&self, _transport: TransportId) -> Option<Box<dyn PacketOut>> {
        None
    }

    fn send(&self, proto: Proto, source: SocketAddr, destination: SocketAddr, data: &[u8]);
}

/// A local candidate the socket layer listens on. TCP candidates are passive.
#[derive(Clone, Copy, Debug)]
pub struct LocalCandidate {
    pub proto: Proto,
    /// Address signaled to the unchanged remote SDK.
    pub addr: SocketAddr,
    /// Real bound local socket; ICE pair and output source identity.
    pub base: SocketAddr,
}

/// Local ICE credentials of one transport.
#[derive(Clone, Debug)]
pub struct IceCredentials {
    pub ufrag: String,
    pub pwd: String,
}

/// `ice-char = ALPHA / DIGIT / "+" / "/"` (RFC 8839 §5.4). 64 symbols, so masking a
/// random byte with 63 is uniform.
const ICE_CHARS: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";

const UFRAG_LEN: usize = 8;
const PWD_LEN: usize = 24;

impl IceCredentials {
    /// 8-char ufrag and 24-char password from the ICE character set.
    pub fn random() -> Self {
        Self {
            ufrag: random_ice_string(UFRAG_LEN),
            pwd: random_ice_string(PWD_LEN),
        }
    }
}

fn random_ice_string(len: usize) -> String {
    let mut buf = [0u8; 32];
    let buf = &mut buf[..len];
    rtc_core::util::fill_random(buf);
    buf.iter()
        .map(|b| ICE_CHARS[(b & 63) as usize] as char)
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn random_credentials_have_expected_shape() {
        let c = IceCredentials::random();
        assert_eq!(c.ufrag.len(), UFRAG_LEN);
        assert_eq!(c.pwd.len(), PWD_LEN);
        assert!(
            c.ufrag
                .bytes()
                .chain(c.pwd.bytes())
                .all(|b| ICE_CHARS.contains(&b))
        );
        let d = IceCredentials::random();
        assert_ne!(c.pwd, d.pwd);
    }

    #[test]
    fn proto_round_trip() {
        assert_eq!(Proto::from_str0m(Proto::Udp.to_str0m()), Proto::Udp);
        assert_eq!(Proto::from_str0m(Proto::Tcp.to_str0m()), Proto::Tcp);
        assert_eq!(Proto::from_str0m(Protocol::SslTcp), Proto::Tcp);
    }
}
