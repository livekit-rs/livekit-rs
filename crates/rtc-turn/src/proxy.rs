//! PROXY protocol (HAProxy v1 text and v2 binary) header parsing, modelled on
//! `github.com/pires/go-proxyproto` as used by LiveKit's
//! `newTURNTCPListener` (`pkg/service/turn.go`). The header is read from the
//! raw TCP stream *before* TLS so the TLS layer and TURN see the real client
//! address the trusted proxy forwarded.
//!
//! Only the source address is extracted (it becomes the connection's client
//! address); the destination fields are validated for shape but not otherwise
//! used, matching how LiveKit consumes `RemoteAddr()`.

use std::io;
use std::net::{IpAddr, SocketAddr};

use tokio::io::{AsyncRead, AsyncReadExt};
use tokio::net::TcpStream;

use crate::auth::{UnixProxyAddress, UnixProxyTransport};
use crate::policy::Cidr;

/// HAProxy PROXY protocol v2 12-byte signature (`\r\n\r\n\0\r\nQUIT\n`).
const V2_SIGNATURE: [u8; 12] = [
    0x0D, 0x0A, 0x0D, 0x0A, 0x00, 0x0D, 0x0A, 0x51, 0x55, 0x49, 0x54, 0x0A,
];
/// Maximum v1 wire length, including CRLF.
const V1_MAX_LEN: usize = 107;
const V1_SIGNATURE: &[u8; 5] = b"PROXY";
/// The fixed Go server uses go-proxyproto's default limit for v2 addresses and TLVs.
const V2_MAX_ADDR_LEN: usize = 4096;

/// v2 command octet low nibble.
const V2_CMD_LOCAL: u8 = 0x00;
const V2_CMD_PROXY: u8 = 0x01;
/// v2 address family octet high nibble.
const V2_FAMILY_UNSPEC: u8 = 0x00;
const V2_FAMILY_INET: u8 = 0x01;
const V2_FAMILY_INET6: u8 = 0x02;
const V2_FAMILY_UNIX: u8 = 0x03;
const UNIX_NAME_LEN: usize = 108;

#[derive(Debug, PartialEq, Eq)]
pub(crate) enum ProxySource {
    Direct,
    Inet(SocketAddr),
    Unix(UnixProxyAddress),
}

/// Returns `true` when `ip` falls inside any of the trusted CIDRs. Malformed
/// entries were rejected at startup, so a `Cidr` that fails to build is
/// skipped rather than matching.
pub(crate) fn is_trusted_proxy(cidrs: &[(IpAddr, u8)], ip: IpAddr) -> bool {
    cidrs
        .iter()
        .any(|(base, bits)| Cidr::new(*base, *bits).is_some_and(|c| c.contains(ip)))
}

/// Read one PROXY protocol header from `stream`, returning the forwarded client
/// address. `Direct` keeps the actual TCP peer address for LOCAL or UNKNOWN.
/// Any parse error aborts the connection.
pub(crate) async fn read_header(stream: &mut TcpStream) -> io::Result<ProxySource> {
    let mut first = [0u8; 1];
    stream.read_exact(&mut first).await?;
    match first[0] {
        // v2 signature begins with CR LF.
        0x0D => read_v2(stream).await,
        // v1 line begins with "PROXY".
        b'P' => read_v1(stream)
            .await
            .map(|addr| addr.map_or(ProxySource::Direct, ProxySource::Inet)),
        _ => Err(invalid(
            "stream does not begin with a PROXY protocol header",
        )),
    }
}

fn invalid(msg: &str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, msg.to_string())
}

fn parse_v1_port(value: &str, error: &str) -> io::Result<u16> {
    if !value.bytes().all(|b| b.is_ascii_digit()) || (value.len() > 1 && value.starts_with('0')) {
        return Err(invalid(error));
    }
    value.parse().map_err(|_| invalid(error))
}

/// Go refills its buffer until the signature is available, then requires the
/// rest of the v1 line to be already buffered. Peek so a coalesced TLS
/// ClientHello or TURN frame remains in the stream.
async fn read_v1(stream: &mut TcpStream) -> io::Result<Option<SocketAddr>> {
    let mut line = [0; V1_MAX_LEN];
    line[0] = b'P';
    let mut consumed = 1;
    loop {
        let available = consumed + stream.peek(&mut line[consumed..]).await?;
        if available == consumed {
            return Err(invalid("incomplete PROXY v1 header"));
        }
        if available < V1_SIGNATURE.len() {
            stream.read_exact(&mut line[consumed..available]).await?;
            consumed = available;
            continue;
        }
        if !line.starts_with(V1_SIGNATURE) {
            return Err(invalid("PROXY v1 header missing signature"));
        }
        let len = line[..available]
            .iter()
            .position(|&byte| byte == b'\n')
            .map(|index| index + 1)
            .ok_or_else(|| invalid("incomplete or oversized PROXY v1 header"))?;
        let address = parse_v1(&line[..len])?;
        stream.read_exact(&mut line[consumed..len]).await?;
        return Ok(address);
    }
}

fn parse_v1(line: &[u8]) -> io::Result<Option<SocketAddr>> {
    let body = line
        .strip_suffix(b"\r\n")
        .ok_or_else(|| invalid("PROXY v1 header must end in CRLF"))?;
    // UNKNOWN does not carry addresses. Its optional suffix is opaque.
    let mut fields = body.splitn(3, |&byte| byte == b' ');
    if fields.next() == Some(&b"PROXY"[..]) && fields.next() == Some(&b"UNKNOWN"[..]) {
        return Ok(None);
    }
    let text =
        std::str::from_utf8(body).map_err(|_| invalid("invalid UTF-8 in PROXY v1 header"))?;
    let mut it = text.split(' ');
    if it.next() != Some("PROXY") {
        return Err(invalid("PROXY v1 header missing PROXY token"));
    }
    let family = it
        .next()
        .ok_or_else(|| invalid("PROXY v1 header truncated"))?;
    let want_v6 = match family {
        "TCP4" => false,
        "TCP6" => true,
        _ => return Err(invalid("PROXY v1 header has an unknown family")),
    };
    let src = it
        .next()
        .ok_or_else(|| invalid("PROXY v1 header missing source address"))?;
    let dst = it
        .next()
        .ok_or_else(|| invalid("PROXY v1 header missing destination address"))?;
    let sport = it
        .next()
        .ok_or_else(|| invalid("PROXY v1 header missing source port"))?;
    let dport = it
        .next()
        .ok_or_else(|| invalid("PROXY v1 header missing destination port"))?;
    let sport = parse_v1_port(sport, "PROXY v1 header has an invalid source port")?;
    parse_v1_port(dport, "PROXY v1 header has an invalid destination port")?;
    // Upstream parses exactly six fields separated by single ASCII spaces for TCP4/TCP6.
    if it.next().is_some() {
        return Err(invalid("PROXY v1 header has too many fields"));
    }
    // Consume the family mismatch: the address version must agree with the token.
    let src_ip: IpAddr = src
        .parse()
        .map_err(|_| invalid("PROXY v1 header has an invalid source IP"))?;
    let dst_ip: IpAddr = dst
        .parse()
        .map_err(|_| invalid("PROXY v1 header has an invalid destination IP"))?;
    if src_ip.is_ipv6() != want_v6 || dst_ip.is_ipv6() != want_v6 {
        return Err(invalid("PROXY v1 header family does not match addresses"));
    }
    Ok(Some(SocketAddr::new(src_ip, sport)))
}

/// v2: the first CR was consumed; read the remaining 11 signature bytes, the
/// version/command and family octets, the length, then the address block.
async fn read_v2<R: AsyncRead + Unpin>(stream: &mut R) -> io::Result<ProxySource> {
    let mut rest = [0u8; 11];
    stream.read_exact(&mut rest).await?;
    if rest != V2_SIGNATURE[1..] {
        return Err(invalid("PROXY v2 signature mismatch"));
    }
    let mut hdr = [0u8; 4];
    stream.read_exact(&mut hdr).await?;
    let (ver_cmd, fam_tr) = (hdr[0], hdr[1]);
    if ver_cmd >> 4 != 2 {
        return Err(invalid("PROXY v2 header has an unexpected version"));
    }
    let cmd = ver_cmd & 0x0F;
    if cmd != V2_CMD_LOCAL && cmd != V2_CMD_PROXY {
        return Err(invalid("PROXY v2 header has an unknown command"));
    }
    // Go validates the complete family/transport byte, even for LOCAL.
    if !matches!(fam_tr, 0x00 | 0x11 | 0x12 | 0x21 | 0x22 | 0x31 | 0x32)
        || (cmd == V2_CMD_PROXY && fam_tr == 0x00)
    {
        return Err(invalid(
            "PROXY v2 header has an unsupported family or transport",
        ));
    }
    let len = u16::from_be_bytes([hdr[2], hdr[3]]) as usize;
    if len > V2_MAX_ADDR_LEN {
        return Err(invalid("PROXY v2 header address block is too large"));
    }
    let mut block = vec![0u8; len];
    stream.read_exact(&mut block).await?;
    // LOCAL consumes the declared payload but always retains the actual
    // peer, including when the payload contains an informational address.
    if cmd == V2_CMD_LOCAL {
        return Ok(ProxySource::Direct);
    }

    let family = fam_tr >> 4;
    match family {
        V2_FAMILY_UNSPEC => Ok(ProxySource::Direct),
        V2_FAMILY_INET => {
            if len < 12 {
                return Err(invalid("PROXY v2 INET address block is too short"));
            }
            let src = IpAddr::from(<[u8; 4]>::try_from(&block[0..4]).unwrap());
            let sport = u16::from_be_bytes([block[8], block[9]]);
            Ok(ProxySource::Inet(SocketAddr::new(src, sport)))
        }
        V2_FAMILY_INET6 => {
            if len < 36 {
                return Err(invalid("PROXY v2 INET6 address block is too short"));
            }
            let src = IpAddr::from(<[u8; 16]>::try_from(&block[0..16]).unwrap());
            let sport = u16::from_be_bytes([block[32], block[33]]);
            Ok(ProxySource::Inet(SocketAddr::new(src, sport)))
        }
        V2_FAMILY_UNIX => {
            if len < 2 * UNIX_NAME_LEN {
                return Err(invalid("PROXY v2 UNIX address block is too short"));
            }
            let name = |bytes: &[u8]| {
                let end = bytes
                    .iter()
                    .position(|&byte| byte == 0)
                    .unwrap_or(bytes.len());
                Box::from(&bytes[..end])
            };
            Ok(ProxySource::Unix(UnixProxyAddress {
                transport: if fam_tr == 0x31 {
                    UnixProxyTransport::Stream
                } else {
                    UnixProxyTransport::Datagram
                },
                source: name(&block[..UNIX_NAME_LEN]),
                destination: name(&block[UNIX_NAME_LEN..2 * UNIX_NAME_LEN]),
            }))
        }
        _ => Err(invalid("PROXY v2 header has an unsupported address family")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::{Ipv4Addr, Ipv6Addr};

    fn v1_line(s: &str) -> Vec<u8> {
        let mut b = s.as_bytes().to_vec();
        b.extend_from_slice(b"\r\n");
        b
    }

    #[tokio::test]
    async fn v2_local_ignores_address_and_still_consumes_declared_bytes() {
        for block in [&[][..], &[203, 0, 113, 9, 127, 0, 0, 1, 0, 12, 0, 13][..]] {
            let mut bytes = V2_SIGNATURE.to_vec();
            bytes.extend_from_slice(&[0x20, 0x11, 0, block.len() as u8]);
            bytes.extend_from_slice(block);
            bytes.extend_from_slice(b"TLS");
            let mut stream = &bytes[1..];
            assert_eq!(read_v2(&mut stream).await.unwrap(), ProxySource::Direct);
            assert_eq!(stream, b"TLS");
        }
    }

    #[tokio::test]
    async fn v2_rejects_invalid_transport_and_proxy_unspec() {
        for family in [0x13, 0x00] {
            let mut bytes = V2_SIGNATURE.to_vec();
            bytes.extend_from_slice(&[0x21, family, 0, 0]);
            assert!(read_v2(&mut &bytes[1..]).await.is_err());
        }
    }

    #[test]
    fn v1_tcp4_and_tcp6() {
        let r = v1_line("PROXY TCP4 203.0.113.9 198.51.100.1 55000 443");
        let got = parse_v1(&r).unwrap();
        assert_eq!(
            got,
            Some(SocketAddr::new(
                IpAddr::V4(Ipv4Addr::new(203, 0, 113, 9)),
                55000
            ))
        );

        let r = v1_line("PROXY TCP6 2001:db8::1 2001:db8::2 1234 443");
        let got = parse_v1(&r).unwrap();
        assert_eq!(
            got,
            Some(SocketAddr::new(
                IpAddr::V6(Ipv6Addr::new(0x2001, 0xdb8, 0, 0, 0, 0, 0, 1)),
                1234
            ))
        );
    }

    #[test]
    fn v1_unknown_keeps_peer() {
        let r = v1_line("PROXY UNKNOWN");
        assert_eq!(parse_v1(&r).unwrap(), None);
        assert_eq!(
            parse_v1(b"PROXY UNKNOWN \xff\xfe\x00 opaque\r\n").unwrap(),
            None
        );
    }

    #[test]
    fn v1_family_mismatch_rejected() {
        let r = v1_line("PROXY TCP4 2001:db8::1 2001:db8::2 1 2");
        assert!(parse_v1(&r).is_err());
    }

    #[test]
    fn v1_extra_fields_and_bad_port_rejected() {
        // A non-numeric destination port is invalid (upstream parses it as uint16).
        let r = v1_line("PROXY TCP4 1.2.3.4 5.6.7.8 1 abc");
        assert!(parse_v1(&r).is_err());
        // More than the six TCP4/TCP6 fields is rejected.
        let r = v1_line("PROXY TCP4 1.2.3.4 5.6.7.8 1 2 3");
        assert!(parse_v1(&r).is_err());
        // But UNKNOWN tolerates the padding fields proxies sometimes send.
        let r = v1_line("PROXY UNKNOWN ffff0000000000000000000000000000 0 0");
        assert_eq!(parse_v1(&r).unwrap(), None);
    }

    #[tokio::test]
    async fn v2_inet_and_inet6_and_unspec() {
        // v2 INET PROXY: src 203.0.113.9:55000, dst 198.51.100.1:443.
        let mut b: Vec<u8> = V2_SIGNATURE.to_vec();
        b.push(0x21); // ver 2 | cmd PROXY
        b.push(0x11); // family INET | transport STREAM
        b.extend_from_slice(&12u16.to_be_bytes());
        b.extend_from_slice(&[203, 0, 113, 9]);
        b.extend_from_slice(&[198, 51, 100, 1]);
        b.extend_from_slice(&55000u16.to_be_bytes());
        b.extend_from_slice(&443u16.to_be_bytes());
        let got = read_v2(&mut &b[1..]).await.unwrap();
        assert_eq!(
            got,
            ProxySource::Inet(SocketAddr::new(
                IpAddr::V4(Ipv4Addr::new(203, 0, 113, 9)),
                55000
            ))
        );

        // v2 INET6 LOCAL with a zero-length block behaves like UNSPEC (no override).
        let mut b: Vec<u8> = V2_SIGNATURE.to_vec();
        b.push(0x20); // ver 2 | cmd LOCAL
        b.push(0x00); // AF_UNSPEC
        b.extend_from_slice(&0u16.to_be_bytes());
        assert_eq!(read_v2(&mut &b[1..]).await.unwrap(), ProxySource::Direct);
    }

    #[tokio::test]
    async fn v2_unix_preserves_opaque_names_and_transport() {
        for (family, transport) in [
            (0x31, UnixProxyTransport::Stream),
            (0x32, UnixProxyTransport::Datagram),
        ] {
            let mut header = V2_SIGNATURE.to_vec();
            header.extend_from_slice(&[0x21, family, 0, 216]);
            let mut addresses = [0; 216];
            addresses[..4].copy_from_slice(b"a\xff\0b");
            addresses[108..].fill(b'd');
            header.extend_from_slice(&addresses);
            header.extend_from_slice(b"TLS");
            let mut wire = &header[1..];
            assert_eq!(
                read_v2(&mut wire).await.unwrap(),
                ProxySource::Unix(UnixProxyAddress {
                    transport,
                    source: Box::from(&b"a\xff"[..]),
                    destination: Box::from(&addresses[108..]),
                }),
            );
            assert_eq!(wire, b"TLS");
        }
    }

    #[tokio::test]
    async fn non_proxy_stream_rejected() {
        // A raw TLS ClientHello byte (0x16) is neither v1 nor v2.
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let mut sender = TcpStream::connect(listener.local_addr().unwrap())
            .await
            .unwrap();
        let (mut receiver, _) = listener.accept().await.unwrap();
        tokio::io::AsyncWriteExt::write_all(&mut sender, &[0x16, 0x03, 0x01])
            .await
            .unwrap();
        assert!(read_header(&mut receiver).await.is_err());
    }

    #[test]
    fn trusted_membership() {
        let cidrs = vec![
            (IpAddr::V4(Ipv4Addr::new(127, 0, 0, 0)), 8),
            (IpAddr::V6(Ipv6Addr::LOCALHOST), 128),
        ];
        assert!(is_trusted_proxy(&cidrs, IpAddr::V4(Ipv4Addr::LOCALHOST)));
        assert!(is_trusted_proxy(&cidrs, IpAddr::V6(Ipv6Addr::LOCALHOST)));
        assert!(!is_trusted_proxy(
            &cidrs,
            IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1))
        ));
    }
}
