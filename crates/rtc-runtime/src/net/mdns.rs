//! Remote mDNS ICE candidates (`rtc.use_mdns`).
//!
//! Upstream disables pion's mDNS engine when the setting is off
//! (`mediatransportutil/rtcconfig/webrtc_config.go:85-87`,
//! `SetICEMulticastDNSMode(ice.MulticastDNSModeDisabled)`) and drops remote
//! `.local`/`.invalid` candidates at the peer-connection boundary
//! (`pkg/rtc/transport.go:2494` trickle, `:2566` SDP filtering). With the
//! setting on pion runs in `QueryOnly` mode: local host candidates keep their
//! IPs, while remote `.local` candidates are resolved with an mDNS query before
//! they are used (`livekit-ice/agent.go:1048-1068`, `mdns.go`). str0m has no
//! mDNS support, so this module performs that query itself and rewrites the
//! candidate address to the resolved IP before the engine sees it.

use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};
use std::time::Duration;

use tokio::net::UdpSocket;
use tokio::time::Instant;

/// mDNS group/port for IPv4 (RFC 6762 §3).
pub(crate) const GROUP_V4: SocketAddr =
    SocketAddr::new(IpAddr::V4(Ipv4Addr::new(224, 0, 0, 251)), 5353);
/// mDNS group/port for IPv6.
pub(crate) const GROUP_V6: SocketAddr = SocketAddr::new(
    IpAddr::V6(Ipv6Addr::new(0xff02, 0, 0, 0, 0, 0, 0, 0xfb)),
    5353,
);
/// pion bounds the query with `stunGatherTimeout`, whose default is 5s.
pub(crate) const QUERY_TIMEOUT: Duration = Duration::from_secs(5);
/// Where a query is sent in production. Tests point the same list at a
/// unicast responder so resolution does not depend on the local network.
pub(crate) const GROUPS: [SocketAddr; 2] = [GROUP_V4, GROUP_V6];
/// How long one socket waits for a reply before the query is re-sent.
const WAIT_SLICE: Duration = Duration::from_millis(250);

/// Which flavour of mDNS hostname a candidate carries.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum MdnsKind {
    /// `*.local`: resolvable through multicast DNS.
    Local,
    /// `*.invalid`: never resolvable (RFC 2606), pion keeps it and the
    /// connectivity check simply never succeeds.
    Invalid,
}

/// Upstream `IsICECandidateMDNS`: a `.local` or `.invalid` suffix. The check is
/// case sensitive, exactly like Go's `strings.HasSuffix`.
pub(crate) fn classify(address: &str) -> Option<MdnsKind> {
    if address.ends_with(".local") {
        Some(MdnsKind::Local)
    } else if address.ends_with(".invalid") {
        Some(MdnsKind::Invalid)
    } else {
        None
    }
}

/// The address field of an ICE candidate line (foundation/component/proto/
/// priority/address/port/typ), tolerating the `a=` and `candidate:` prefixes.
pub(crate) fn candidate_address(line: &str) -> Option<&str> {
    let rest = line.strip_prefix("a=").unwrap_or(line);
    let rest = rest.strip_prefix("candidate:").unwrap_or(rest);
    rest.split_whitespace().nth(4)
}

/// Replace the address field, preserving every other byte of the line.
pub(crate) fn rewrite_candidate_address(line: &str, address: &str) -> Option<String> {
    let (prefix, rest) = match line.strip_prefix("a=candidate:") {
        Some(rest) => ("a=candidate:", rest),
        None => match line.strip_prefix("candidate:") {
            Some(rest) => ("candidate:", rest),
            None => ("", line),
        },
    };
    let mut fields: Vec<&str> = rest.split_whitespace().collect();
    if fields.len() < 6 {
        return None;
    }
    fields[4] = address;
    Some(format!("{prefix}{}", fields.join(" ")))
}

/// One `a=candidate:` line removed from a remote description and kept for mDNS
/// resolution, together with the media section it belongs to so the resolved
/// candidate can be re-injected as trickle for the right transport.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct ExtractedCandidate {
    /// Candidate line with the `candidate:` prefix (trickle form).
    pub line: String,
    pub mid: Option<String>,
    pub mline: Option<u32>,
}

/// Remove every mDNS candidate from a remote description.
///
/// Upstream filters these out of the SDP (`transport.go:2566`) and adds the
/// accepted ones through `AddRemoteCandidate`, so stripping them here and
/// re-injecting them as trickle after resolution is the same code path.
pub(crate) fn split_sdp_mdns_candidates(sdp: &str) -> (String, Vec<ExtractedCandidate>) {
    let mut kept = String::with_capacity(sdp.len());
    let mut extracted = Vec::new();
    let mut mid: Option<String> = None;
    let mut mline: u32 = 0;
    let mut seen_media = false;
    for line in sdp.split_inclusive('\n') {
        let trimmed = line.trim_end_matches(['\r', '\n']);
        if trimmed.starts_with("m=") {
            if seen_media {
                mline += 1;
            }
            seen_media = true;
            mid = None;
        } else if let Some(value) = trimmed.strip_prefix("a=mid:") {
            mid = Some(value.to_string());
        } else if trimmed.starts_with("a=candidate:")
            && candidate_address(trimmed).is_some_and(|a| classify(a).is_some())
        {
            extracted.push(ExtractedCandidate {
                line: trimmed.replacen("a=candidate:", "candidate:", 1),
                mid: mid.clone(),
                mline: seen_media.then_some(mline),
            });
            continue;
        }
        kept.push_str(line);
    }
    (kept, extracted)
}

/// Resolve a `.local` name through multicast DNS against `targets` (the mDNS
/// groups in production, a unicast responder in tests).
pub(crate) async fn resolve_via(
    name: &str,
    timeout: Duration,
    targets: &[SocketAddr],
) -> Option<IpAddr> {
    let query = name.trim_end_matches('.').to_ascii_lowercase();
    if query.is_empty() {
        return None;
    }
    let deadline = Instant::now() + timeout;
    let mut sockets = Vec::new();
    for target in targets {
        let bind: SocketAddr = if target.is_ipv4() {
            SocketAddr::from((Ipv4Addr::UNSPECIFIED, 0))
        } else {
            SocketAddr::from((Ipv6Addr::UNSPECIFIED, 0))
        };
        if let Ok(socket) = UdpSocket::bind(bind).await {
            sockets.push((socket, *target));
        }
    }
    if sockets.is_empty() {
        return None;
    }
    let mut buf = [0u8; 1500];
    while Instant::now() < deadline {
        for (socket, target, record) in sockets
            .iter()
            .map(|(socket, target)| (socket, target, record_type(target)))
        {
            // A unicast-response query, like every mDNS resolver that does not
            // own port 5353: responders answer to the querier's source port.
            let packet = query_packet(&query, record);
            if socket.send_to(&packet, *target).await.is_err() {
                continue;
            }
            let wait = (deadline - Instant::now()).min(WAIT_SLICE);
            if wait.is_zero() {
                return None;
            }
            let slice = tokio::time::timeout(wait, socket.recv_from(&mut buf)).await;
            if let Ok(Ok((len, _))) = slice
                && let Some(ip) = response_ip(&buf[..len], &query)
            {
                return Some(ip);
            }
        }
    }
    None
}

fn record_type(target: &SocketAddr) -> RecordType {
    if target.is_ipv4() {
        RecordType::A
    } else {
        RecordType::Aaaa
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum RecordType {
    A,
    Aaaa,
}

impl RecordType {
    fn code(self) -> u16 {
        match self {
            RecordType::A => 1,
            RecordType::Aaaa => 28,
        }
    }
}

/// One-question mDNS query with the unicast-response bit set in QCLASS.
fn query_packet(name: &str, record: RecordType) -> Vec<u8> {
    let mut packet = Vec::with_capacity(64);
    packet.extend_from_slice(&0u16.to_be_bytes()); // id: unused in mDNS
    packet.extend_from_slice(&0u16.to_be_bytes()); // flags: standard query
    packet.extend_from_slice(&1u16.to_be_bytes()); // qdcount
    packet.extend_from_slice(&[0, 0, 0, 0]); // ancount, nscount
    packet.extend_from_slice(&[0, 0]); // arcount
    for label in name.split('.').filter(|label| !label.is_empty()) {
        let bytes = label.as_bytes();
        let len = bytes.len().min(63);
        packet.push(len as u8);
        packet.extend_from_slice(&bytes[..len]);
    }
    packet.push(0);
    packet.extend_from_slice(&record.code().to_be_bytes());
    packet.extend_from_slice(&0x8001u16.to_be_bytes()); // IN + QU
    packet
}

/// First A/AAAA answer whose owner name matches `name`.
fn response_ip(packet: &[u8], name: &str) -> Option<IpAddr> {
    if packet.len() < 12 {
        return None;
    }
    let questions = u16::from_be_bytes([packet[4], packet[5]]) as usize;
    let answers = u16::from_be_bytes([packet[6], packet[7]]) as usize;
    let mut offset = 12;
    for _ in 0..questions {
        let (_, next) = read_name(packet, offset)?;
        offset = next.checked_add(4)?; // qtype + qclass
        if offset > packet.len() {
            return None;
        }
    }
    for _ in 0..answers {
        let (owner, next) = read_name(packet, offset)?;
        offset = next;
        if offset + 10 > packet.len() {
            return None;
        }
        let record = u16::from_be_bytes([packet[offset], packet[offset + 1]]);
        let length = u16::from_be_bytes([packet[offset + 8], packet[offset + 9]]) as usize;
        offset += 10;
        let end = offset.checked_add(length)?;
        if end > packet.len() {
            return None;
        }
        if owner.eq_ignore_ascii_case(name) {
            let rdata = &packet[offset..end];
            if record == 1 && rdata.len() == 4 {
                return Some(IpAddr::from([rdata[0], rdata[1], rdata[2], rdata[3]]));
            }
            if record == 28 && rdata.len() == 16 {
                let mut octets = [0u8; 16];
                octets.copy_from_slice(rdata);
                return Some(IpAddr::V6(Ipv6Addr::from(octets)));
            }
        }
        offset = end;
    }
    None
}

/// DNS name decoder with compression-pointer support.
fn read_name(packet: &[u8], start: usize) -> Option<(String, usize)> {
    let mut labels = Vec::new();
    let mut offset = start;
    let mut next = None;
    let mut jumps = 0;
    loop {
        let len = *packet.get(offset)?;
        match len & 0xc0 {
            0x00 => {
                if len == 0 {
                    offset += 1;
                    break;
                }
                let from = offset + 1;
                let to = from.checked_add(len as usize)?;
                let label = packet.get(from..to)?;
                labels.push(String::from_utf8_lossy(label).into_owned());
                offset = to;
            }
            0xc0 => {
                let low = *packet.get(offset + 1)?;
                let pointer = (((len & 0x3f) as usize) << 8) | low as usize;
                next.get_or_insert(offset + 2);
                jumps += 1;
                if jumps > 16 || pointer >= packet.len() {
                    return None;
                }
                offset = pointer;
            }
            _ => return None,
        }
    }
    Some((labels.join("."), next.unwrap_or(offset)))
}

#[cfg(test)]
pub(crate) mod test_support {
    use super::*;
    use std::sync::{Arc, atomic::AtomicUsize};

    pub(crate) fn responder_answer(name: &str, ip: IpAddr) -> Vec<u8> {
        let mut packet = Vec::new();
        packet.extend_from_slice(&[0, 0]); // id
        packet.extend_from_slice(&0x8400u16.to_be_bytes()); // response + AA
        packet.extend_from_slice(&0u16.to_be_bytes()); // qdcount
        packet.extend_from_slice(&1u16.to_be_bytes()); // ancount
        packet.extend_from_slice(&[0, 0, 0, 0]);
        let mut labels = Vec::new();
        for label in name.split('.').filter(|l| !l.is_empty()) {
            labels.push(label.len() as u8);
            labels.extend_from_slice(label.as_bytes());
        }
        labels.push(0);
        packet.extend_from_slice(&labels);
        let (record, rdata) = match ip {
            IpAddr::V4(v4) => (1u16, v4.octets().to_vec()),
            IpAddr::V6(v6) => (28u16, v6.octets().to_vec()),
        };
        packet.extend_from_slice(&record.to_be_bytes());
        packet.extend_from_slice(&0x8001u16.to_be_bytes()); // IN + cache flush
        packet.extend_from_slice(&120u32.to_be_bytes()); // ttl
        packet.extend_from_slice(&(rdata.len() as u16).to_be_bytes());
        packet.extend_from_slice(&rdata);
        packet
    }

    /// A real unicast responder standing in for an mDNS responder: it receives
    /// the query on a plain UDP socket and replies to the querier.
    pub(crate) async fn responder(name: &'static str, ip: IpAddr) -> SocketAddr {
        responder_with_counter(name, ip).await.0
    }

    /// [`responder`] that also counts the queries it answered.
    pub(crate) async fn responder_with_counter(
        name: &'static str,
        ip: IpAddr,
    ) -> (SocketAddr, Arc<AtomicUsize>) {
        let socket = UdpSocket::bind((Ipv4Addr::LOCALHOST, 0)).await.unwrap();
        let address = socket.local_addr().unwrap();
        let seen = Arc::new(AtomicUsize::new(0));
        let count = seen.clone();
        tokio::spawn(async move {
            let mut buf = [0u8; 1500];
            loop {
                let Ok((_, from)) = socket.recv_from(&mut buf).await else {
                    return;
                };
                count.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                if socket
                    .send_to(&responder_answer(name, ip), from)
                    .await
                    .is_err()
                {
                    return;
                }
            }
        });
        (address, seen)
    }

    /// A bound socket that counts queries but never answers: it makes "no
    /// query was sent" observable.
    pub(crate) async fn silent_spy() -> (SocketAddr, Arc<AtomicUsize>) {
        let socket = UdpSocket::bind((Ipv4Addr::LOCALHOST, 0)).await.unwrap();
        let address = socket.local_addr().unwrap();
        let seen = Arc::new(AtomicUsize::new(0));
        let count = seen.clone();
        tokio::spawn(async move {
            let mut buf = [0u8; 1500];
            while socket.recv_from(&mut buf).await.is_ok() {
                count.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            }
        });
        (address, seen)
    }
}

#[cfg(test)]
mod tests {
    use super::test_support::responder;
    use super::*;

    #[test]
    fn classifies_only_local_and_invalid_suffixes() {
        assert_eq!(classify("abcd-1234.local"), Some(MdnsKind::Local));
        assert_eq!(classify("abcd-1234.invalid"), Some(MdnsKind::Invalid));
        assert_eq!(classify("192.0.2.10"), None);
        // Go's strings.HasSuffix is case sensitive.
        assert_eq!(classify("ABCD.LOCAL"), None);
    }

    #[test]
    fn reads_and_rewrites_the_candidate_address_field() {
        let line = "candidate:1 1 udp 2130706431 abcd-1234.local 54321 typ host generation 0";
        assert_eq!(candidate_address(line), Some("abcd-1234.local"));
        assert_eq!(
            candidate_address("a=candidate:2 1 tcp 1518280447 192.0.2.10 9 typ host"),
            Some("192.0.2.10")
        );
        assert_eq!(
            rewrite_candidate_address(line, "192.0.2.10").unwrap(),
            "candidate:1 1 udp 2130706431 192.0.2.10 54321 typ host generation 0"
        );
        assert_eq!(
            rewrite_candidate_address("a=candidate:2 1 udp 1 abcd.local 9 typ host", "192.0.2.11")
                .unwrap(),
            "a=candidate:2 1 udp 1 192.0.2.11 9 typ host"
        );
    }

    #[test]
    fn strips_mdns_candidates_and_keeps_the_media_section_context() {
        let sdp = "v=0\r\n\
                   m=audio 9 UDP/TLS/RTP/SAVPF 111\r\n\
                   a=mid:0\r\n\
                   a=candidate:1 1 udp 2130706431 192.0.2.10 5000 typ host\r\n\
                   a=candidate:2 1 udp 2130706431 abcd-1234.local 5001 typ host\r\n\
                   a=candidate:3 1 udp 2130706431 abcd-1234.invalid 5002 typ host\r\n\
                   m=video 9 UDP/TLS/RTP/SAVPF 96\r\n\
                   a=mid:1\r\n\
                   a=candidate:4 1 udp 2130706431 efgh-5678.local 5003 typ host\r\n";
        let (kept, extracted) = split_sdp_mdns_candidates(sdp);
        assert!(kept.contains("192.0.2.10 5000"));
        assert!(!kept.contains(".local"));
        assert!(!kept.contains(".invalid"));
        assert_eq!(extracted.len(), 3);
        assert_eq!(extracted[0].mid.as_deref(), Some("0"));
        assert_eq!(extracted[0].mline, Some(0));
        assert!(extracted[0].line.starts_with("candidate:2 "));
        assert_eq!(extracted[1].mid.as_deref(), Some("0"));
        assert_eq!(extracted[2].mid.as_deref(), Some("1"));
        assert_eq!(extracted[2].mline, Some(1));
    }

    #[test]
    fn sdp_without_mdns_candidates_is_unchanged() {
        let sdp = "v=0\r\nm=audio 9 UDP/TLS/RTP/SAVPF 111\r\na=mid:0\r\n";
        let (kept, extracted) = split_sdp_mdns_candidates(sdp);
        assert_eq!(kept, sdp);
        assert!(extracted.is_empty());
    }

    #[test]
    fn query_packet_encodes_a_single_unicast_response_question() {
        let packet = query_packet("abcd-1234.local", RecordType::Aaaa);
        assert_eq!(&packet[0..2], &[0, 0]);
        assert_eq!(&packet[4..6], &[0, 1], "one question");
        let qname_end = packet[12..].iter().position(|b| *b == 0).unwrap() + 12 + 1;
        assert_eq!(&packet[12..qname_end], b"\x09abcd-1234\x05local\x00");
        assert_eq!(
            &packet[qname_end..qname_end + 2],
            &28u16.to_be_bytes(),
            "AAAA qtype"
        );
        assert_eq!(
            &packet[qname_end + 2..qname_end + 4],
            &0x8001u16.to_be_bytes(),
            "IN with QU bit"
        );
    }

    #[test]
    fn parses_answers_with_compressed_owner_names() {
        // Question holds the name; the answer owner is a pointer back to it.
        let mut packet = Vec::new();
        packet.extend_from_slice(&[0, 0]);
        packet.extend_from_slice(&0x8400u16.to_be_bytes());
        packet.extend_from_slice(&1u16.to_be_bytes()); // qdcount
        packet.extend_from_slice(&1u16.to_be_bytes()); // ancount
        packet.extend_from_slice(&[0, 0, 0, 0]);
        packet.extend_from_slice(b"\x04abcd\x05local\x00");
        packet.extend_from_slice(&1u16.to_be_bytes()); // A
        packet.extend_from_slice(&1u16.to_be_bytes()); // IN
        packet.extend_from_slice(&[0xc0, 0x0c]); // owner: pointer to offset 12
        packet.extend_from_slice(&1u16.to_be_bytes());
        packet.extend_from_slice(&0x8001u16.to_be_bytes());
        packet.extend_from_slice(&120u32.to_be_bytes());
        packet.extend_from_slice(&4u16.to_be_bytes());
        packet.extend_from_slice(&[192, 0, 2, 7]);
        assert_eq!(
            response_ip(&packet, "abcd.local"),
            Some(IpAddr::V4(Ipv4Addr::new(192, 0, 2, 7)))
        );
        assert_eq!(response_ip(&packet, "other.local"), None);
    }

    #[tokio::test]
    async fn resolves_a_name_through_a_real_responder_socket() {
        let ip = IpAddr::V4(Ipv4Addr::new(192, 0, 2, 42));
        let target = responder("abcd-1234.local", ip).await;
        assert_eq!(
            resolve_via("abcd-1234.local", Duration::from_secs(2), &[target]).await,
            Some(ip)
        );
    }

    #[tokio::test]
    async fn resolution_returns_none_when_no_responder_answers() {
        // A bound but silent socket: queries are sent, nothing replies.
        let silent = UdpSocket::bind((Ipv4Addr::LOCALHOST, 0)).await.unwrap();
        let target = silent.local_addr().unwrap();
        assert_eq!(
            resolve_via("abcd-1234.local", Duration::from_millis(600), &[target]).await,
            None
        );
    }

    #[tokio::test]
    async fn resolution_of_an_empty_name_does_not_query() {
        assert_eq!(
            resolve_via(".", Duration::from_millis(100), &[GROUP_V4]).await,
            None
        );
    }
}
