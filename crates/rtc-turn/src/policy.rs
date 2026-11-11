//! Peer permission policy, mirroring LiveKit's `isRestrictedPeerIP` and the
//! `allow_restricted_peer_cidrs` / `deny_peer_cidrs` permission handler.

use std::collections::HashSet;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};

/// Embedded TURN limits outbound datagrams to this host to its UDP media ports.
/// The separate IP permission policy still applies to every peer.
#[derive(Debug, Clone)]
pub struct LocalRelayPolicy {
    local_ips: HashSet<IpAddr>,
    media_ports: Vec<(u16, u16)>,
}

impl LocalRelayPolicy {
    /// Snapshots interface and node addresses and inclusive media port ranges.
    pub fn new(local_ips: impl IntoIterator<Item = IpAddr>, media_ports: Vec<(u16, u16)>) -> Self {
        Self {
            local_ips: local_ips.into_iter().map(unmap).collect(),
            media_ports,
        }
    }

    /// Applies the local port restriction after the caller's permission checks.
    pub fn allows(&self, peer: SocketAddr) -> bool {
        !self.local_ips.contains(&unmap(peer.ip()))
            || self
                .media_ports
                .iter()
                .any(|&(start, end)| (start..=end).contains(&peer.port()))
    }
}

/// A parsed CIDR block. Constructed via [`Cidr::new`], which validates the
/// prefix length and masks host bits.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Cidr {
    addr: IpAddr,
    prefix: u8,
}

impl Cidr {
    pub(crate) fn new(addr: IpAddr, prefix: u8) -> Option<Cidr> {
        // An IPv4-mapped IPv6 network with prefix >= 96 is treated as IPv4,
        // matching Go's net.IPNet semantics for mapped addresses.
        let (addr, prefix) = match addr {
            IpAddr::V6(v6) => match v6.to_ipv4_mapped() {
                Some(v4) if (96..=128).contains(&prefix) => (IpAddr::V4(v4), prefix - 96),
                _ => (addr, prefix),
            },
            v4 => (v4, prefix),
        };
        match addr {
            IpAddr::V4(a) if prefix <= 32 => Some(Cidr {
                addr: IpAddr::V4(Ipv4Addr::from(u32::from(a) & mask32(prefix))),
                prefix,
            }),
            IpAddr::V6(a) if prefix <= 128 => Some(Cidr {
                addr: IpAddr::V6(Ipv6Addr::from(u128::from(a) & mask128(prefix))),
                prefix,
            }),
            _ => None,
        }
    }

    pub(crate) fn contains(&self, ip: IpAddr) -> bool {
        match (self.addr, unmap(ip)) {
            (IpAddr::V4(net), IpAddr::V4(ip)) => {
                u32::from(ip) & mask32(self.prefix) == u32::from(net)
            }
            (IpAddr::V6(net), IpAddr::V6(ip)) => {
                u128::from(ip) & mask128(self.prefix) == u128::from(net)
            }
            _ => false,
        }
    }
}

fn mask32(prefix: u8) -> u32 {
    if prefix == 0 {
        0
    } else {
        u32::MAX << (32 - prefix as u32)
    }
}

fn mask128(prefix: u8) -> u128 {
    if prefix == 0 {
        0
    } else {
        u128::MAX << (128 - prefix as u32)
    }
}

/// Converts IPv4-mapped IPv6 addresses (::ffff:a.b.c.d) to IPv4.
pub(crate) fn unmap(ip: IpAddr) -> IpAddr {
    match ip {
        IpAddr::V6(v6) => match v6.to_ipv4_mapped() {
            Some(v4) => IpAddr::V4(v4),
            None => ip,
        },
        v4 => v4,
    }
}

fn in_v4(ip: Ipv4Addr, net: [u8; 4], prefix: u8) -> bool {
    u32::from(ip) & mask32(prefix) == u32::from(Ipv4Addr::from(net)) & mask32(prefix)
}

fn in_v6(ip: Ipv6Addr, net: Ipv6Addr, prefix: u8) -> bool {
    u128::from(ip) & mask128(prefix) == u128::from(net) & mask128(prefix)
}

/// Returns true for peer addresses a TURN client must not reach by default:
/// loopback, link-local (unicast + multicast), multicast, private (RFC 1918,
/// IPv6 ULA fc00::/7), unspecified, plus 0.0.0.0/8, 100.64.0.0/10 (CGNAT),
/// 198.18.0.0/15 (benchmarking), 240.0.0.0/4 (reserved + broadcast),
/// 64:ff9b::/96 and 64:ff9b:1::/48 (NAT64).
///
/// Equivalent to LiveKit's `isRestrictedPeerIP` (Go `net.IP` predicates +
/// `extraRestrictedPeerPrefixes`). IPv4-mapped IPv6 addresses are classified
/// as their IPv4 address.
pub fn is_restricted_peer_ip(ip: IpAddr) -> bool {
    match unmap(ip) {
        IpAddr::V4(v4) => {
            v4.is_loopback()                         // 127.0.0.0/8
                || in_v4(v4, [169, 254, 0, 0], 16)   // link-local unicast
                || in_v4(v4, [224, 0, 0, 0], 4)      // multicast (incl. link-local multicast)
                || in_v4(v4, [10, 0, 0, 0], 8)       // private
                || in_v4(v4, [172, 16, 0, 0], 12)
                || in_v4(v4, [192, 168, 0, 0], 16)
                || v4.is_unspecified()
                || in_v4(v4, [0, 0, 0, 0], 8)        // "this" network
                || in_v4(v4, [100, 64, 0, 0], 10)    // CGNAT
                || in_v4(v4, [198, 18, 0, 0], 15)    // benchmarking
                || in_v4(v4, [240, 0, 0, 0], 4) // reserved, includes broadcast
        }
        IpAddr::V6(v6) => {
            v6.is_loopback()
                || in_v6(v6, Ipv6Addr::new(0xfe80, 0, 0, 0, 0, 0, 0, 0), 10) // link-local unicast
                || in_v6(v6, Ipv6Addr::new(0xff00, 0, 0, 0, 0, 0, 0, 0), 8)  // multicast
                || in_v6(v6, Ipv6Addr::new(0xfc00, 0, 0, 0, 0, 0, 0, 0), 7)  // ULA (Go IsPrivate)
                || v6.is_unspecified()
                || in_v6(v6, Ipv6Addr::new(0x64, 0xff9b, 0, 0, 0, 0, 0, 0), 96) // NAT64
                || in_v6(v6, Ipv6Addr::new(0x64, 0xff9b, 1, 0, 0, 0, 0, 0), 48) // local-use NAT64
        }
    }
}

/// The permission decision applied to CreatePermission / ChannelBind peers.
#[derive(Debug, Clone, Default)]
pub(crate) struct PeerPolicy {
    allow_restricted: Vec<Cidr>,
    deny: Vec<Cidr>,
}

impl PeerPolicy {
    pub(crate) fn new(allow_restricted: Vec<Cidr>, deny: Vec<Cidr>) -> PeerPolicy {
        PeerPolicy {
            allow_restricted,
            deny,
        }
    }

    /// Restricted IPs are denied unless in the allow list; the deny list
    /// always wins.
    pub(crate) fn permits(&self, peer: IpAddr) -> bool {
        let peer = unmap(peer);
        if is_restricted_peer_ip(peer) && !self.allow_restricted.iter().any(|c| c.contains(peer)) {
            return false;
        }
        !self.deny.iter().any(|c| c.contains(peer))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ip(s: &str) -> IpAddr {
        s.parse().unwrap()
    }

    #[test]
    fn restricted_classification() {
        for s in [
            "127.0.0.1",
            "127.255.0.1",
            "10.1.2.3",
            "172.16.0.1",
            "172.31.255.255",
            "192.168.1.1",
            "169.254.169.254",
            "224.0.0.1",
            "239.255.255.250",
            "0.0.0.0",
            "0.1.2.3",
            "100.64.0.1",
            "100.127.255.255",
            "198.18.0.1",
            "198.19.255.255",
            "240.0.0.1",
            "255.255.255.255",
            "::1",
            "::",
            "fe80::1",
            "ff02::1",
            "ff0e::1",
            "fc00::1",
            "fd12:3456::1",
            "64:ff9b::a00:1",
            "64:ff9b:1::1",
            "::ffff:127.0.0.1",
            "::ffff:10.0.0.1",
        ] {
            assert!(is_restricted_peer_ip(ip(s)), "{s} should be restricted");
        }
    }

    #[test]
    fn public_not_restricted() {
        for s in [
            "8.8.8.8",
            "1.1.1.1",
            "172.32.0.1",
            "100.128.0.1",
            "198.20.0.1",
            "203.0.113.10",
            "2001:4860:4860::8888",
            "2606:4700::1111",
            "::ffff:8.8.8.8",
            "64:ff9b:2::1",
        ] {
            assert!(
                !is_restricted_peer_ip(ip(s)),
                "{s} should not be restricted"
            );
        }
    }

    #[test]
    fn cidr_parse_and_contains() {
        let c = Cidr::new(ip("10.1.2.3"), 8).unwrap();
        assert!(c.contains(ip("10.200.0.1")));
        assert!(c.contains(ip("::ffff:10.200.0.1")));
        assert!(!c.contains(ip("11.0.0.1")));
        assert!(Cidr::new(ip("10.0.0.0"), 33).is_none());
        assert!(Cidr::new(ip("::"), 129).is_none());
        let all = Cidr::new(ip("0.0.0.0"), 0).unwrap();
        assert!(all.contains(ip("1.2.3.4")));
        assert!(!all.contains(ip("2001:db8::1")));
        let mapped = Cidr::new(ip("::ffff:192.168.0.0"), 112).unwrap();
        assert!(mapped.contains(ip("192.168.4.4")));
        let v6 = Cidr::new(ip("fd00::"), 8).unwrap();
        assert!(v6.contains(ip("fd12::1")));
        assert!(!v6.contains(ip("fe80::1")));
    }

    #[test]
    fn policy_allow_and_deny() {
        let p = PeerPolicy::default();
        assert!(!p.permits(ip("10.0.0.1")));
        assert!(p.permits(ip("8.8.8.8")));

        let p = PeerPolicy::new(
            vec![Cidr::new(ip("10.0.0.0"), 8).unwrap()],
            vec![
                Cidr::new(ip("10.9.0.0"), 16).unwrap(),
                Cidr::new(ip("8.8.8.0"), 24).unwrap(),
            ],
        );
        assert!(p.permits(ip("10.0.0.1")), "allowed restricted");
        assert!(!p.permits(ip("10.9.1.1")), "deny wins over allow");
        assert!(!p.permits(ip("8.8.8.8")), "deny applies to public IPs");
        assert!(p.permits(ip("1.1.1.1")));
        assert!(
            !p.permits(ip("192.168.0.1")),
            "restricted, not in allow list"
        );
    }
}
