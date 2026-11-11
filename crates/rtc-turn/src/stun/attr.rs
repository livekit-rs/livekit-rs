//! STUN/TURN attribute types and value codecs.

use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};

use super::{MAGIC_COOKIE, StunError};

pub const MAPPED_ADDRESS: u16 = 0x0001;
pub const USERNAME: u16 = 0x0006;
pub const MESSAGE_INTEGRITY: u16 = 0x0008;
pub const ERROR_CODE: u16 = 0x0009;
pub const UNKNOWN_ATTRIBUTES: u16 = 0x000A;
pub const CHANNEL_NUMBER: u16 = 0x000C;
pub const LIFETIME: u16 = 0x000D;
pub const XOR_PEER_ADDRESS: u16 = 0x0012;
pub const DATA: u16 = 0x0013;
pub const REALM: u16 = 0x0014;
pub const NONCE: u16 = 0x0015;
pub const XOR_RELAYED_ADDRESS: u16 = 0x0016;
pub const REQUESTED_ADDRESS_FAMILY: u16 = 0x0017;
pub const EVEN_PORT: u16 = 0x0018;
pub const REQUESTED_TRANSPORT: u16 = 0x0019;
pub const DONT_FRAGMENT: u16 = 0x001A;
pub const MESSAGE_INTEGRITY_SHA256: u16 = 0x001C;
pub const PASSWORD_ALGORITHM: u16 = 0x001D;
pub const USERHASH: u16 = 0x001E;
pub const XOR_MAPPED_ADDRESS: u16 = 0x0020;
pub const RESERVATION_TOKEN: u16 = 0x0022;
pub const PRIORITY: u16 = 0x0024;
pub const USE_CANDIDATE: u16 = 0x0025;
pub const ADDITIONAL_ADDRESS_FAMILY: u16 = 0x8000;
pub const PASSWORD_ALGORITHMS: u16 = 0x8002;
pub const ALTERNATE_DOMAIN: u16 = 0x8003;
pub const SOFTWARE: u16 = 0x8022;
pub const ALTERNATE_SERVER: u16 = 0x8023;
pub const FINGERPRINT: u16 = 0x8028;
pub const ICE_CONTROLLED: u16 = 0x8029;
pub const ICE_CONTROLLING: u16 = 0x802A;

/// IANA protocol number for UDP, the only REQUESTED-TRANSPORT supported.
pub const PROTO_UDP: u8 = 17;

/// Address family values used by XOR-*-ADDRESS and REQUESTED-ADDRESS-FAMILY.
pub const FAMILY_IPV4: u8 = 0x01;
pub const FAMILY_IPV6: u8 = 0x02;

/// Comprehension-required attributes (0x0000..=0x7FFF) this implementation
/// understands. Others in a TURN request trigger 420 Unknown Attribute.
/// EVEN-PORT and RESERVATION-TOKEN are "understood" in the sense that the
/// server explicitly rejects/handles them with a proper error code.
pub fn is_known_comprehension_required(t: u16) -> bool {
    matches!(
        t,
        MAPPED_ADDRESS
            | USERNAME
            | MESSAGE_INTEGRITY
            | ERROR_CODE
            | UNKNOWN_ATTRIBUTES
            | CHANNEL_NUMBER
            | LIFETIME
            | XOR_PEER_ADDRESS
            | DATA
            | REALM
            | NONCE
            | XOR_RELAYED_ADDRESS
            | REQUESTED_ADDRESS_FAMILY
            | EVEN_PORT
            | REQUESTED_TRANSPORT
            | DONT_FRAGMENT
            | MESSAGE_INTEGRITY_SHA256
            | PASSWORD_ALGORITHM
            | USERHASH
            | XOR_MAPPED_ADDRESS
            | RESERVATION_TOKEN
            | PRIORITY
            | USE_CANDIDATE
    )
}

/// Returns true for comprehension-required attribute types.
pub const fn is_comprehension_required(t: u16) -> bool {
    t < 0x8000
}

/// Encodes an XOR-*-ADDRESS value (RFC 5389 §15.2).
pub fn encode_xor_address(addr: SocketAddr, transaction_id: &[u8; 12]) -> Vec<u8> {
    let port = addr.port() ^ (MAGIC_COOKIE >> 16) as u16;
    let cookie = MAGIC_COOKIE.to_be_bytes();
    match addr.ip() {
        IpAddr::V4(ip) => {
            let mut out = Vec::with_capacity(8);
            out.push(0);
            out.push(FAMILY_IPV4);
            out.extend_from_slice(&port.to_be_bytes());
            let o = ip.octets();
            for i in 0..4 {
                out.push(o[i] ^ cookie[i]);
            }
            out
        }
        IpAddr::V6(ip) => {
            let mut out = Vec::with_capacity(20);
            out.push(0);
            out.push(FAMILY_IPV6);
            out.extend_from_slice(&port.to_be_bytes());
            let o = ip.octets();
            for i in 0..16 {
                let k = if i < 4 {
                    cookie[i]
                } else {
                    transaction_id[i - 4]
                };
                out.push(o[i] ^ k);
            }
            out
        }
    }
}

/// Decodes an XOR-*-ADDRESS value.
pub fn decode_xor_address(
    v: &[u8],
    transaction_id: &[u8; 12],
    typ: u16,
) -> Result<SocketAddr, StunError> {
    decode_xor_address_with(v, transaction_id, typ, false)
}

/// Pion's TURN peer address decoder requires a full family field and
/// zero-fills missing address octets after decoding the supplied prefix.
pub(crate) fn decode_turn_peer_address(
    v: &[u8],
    transaction_id: &[u8; 12],
) -> Result<SocketAddr, StunError> {
    decode_xor_address_with(v, transaction_id, XOR_PEER_ADDRESS, true)
}

fn decode_xor_address_with(
    v: &[u8],
    transaction_id: &[u8; 12],
    typ: u16,
    turn_peer: bool,
) -> Result<SocketAddr, StunError> {
    if v.len() <= 4 || (turn_peer && v[0] != 0) {
        return Err(StunError::BadAttribute(typ));
    }
    let port = u16::from_be_bytes([v[2], v[3]]) ^ (MAGIC_COOKIE >> 16) as u16;
    let cookie = MAGIC_COOKIE.to_be_bytes();
    let fits = |len| v.len() == len || (turn_peer && v.len() < len);
    match v[1] {
        FAMILY_IPV4 if fits(8) => {
            let mut o = [0u8; 4];
            for (i, &encoded) in v[4..].iter().enumerate() {
                o[i] = encoded ^ cookie[i];
            }
            Ok(SocketAddr::new(IpAddr::V4(Ipv4Addr::from(o)), port))
        }
        FAMILY_IPV6 if fits(20) => {
            let mut o = [0u8; 16];
            for (i, &encoded) in v[4..].iter().enumerate() {
                let k = if i < 4 {
                    cookie[i]
                } else {
                    transaction_id[i - 4]
                };
                o[i] = encoded ^ k;
            }
            Ok(SocketAddr::new(IpAddr::V6(Ipv6Addr::from(o)), port))
        }
        _ => Err(StunError::BadAttribute(typ)),
    }
}

/// Encodes an ERROR-CODE value (RFC 5389 §15.6).
pub fn encode_error_code(code: u16, reason: &str) -> Vec<u8> {
    let mut out = Vec::with_capacity(4 + reason.len());
    out.extend_from_slice(&[0, 0, ((code / 100) & 0x07) as u8, (code % 100) as u8]);
    out.extend_from_slice(reason.as_bytes());
    out
}

/// Decodes an ERROR-CODE value into (code, reason).
pub fn decode_error_code(v: &[u8]) -> Result<(u16, String), StunError> {
    if v.len() < 4 {
        return Err(StunError::BadAttribute(ERROR_CODE));
    }
    let code = (v[2] & 0x07) as u16 * 100 + v[3] as u16;
    let reason = String::from_utf8_lossy(&v[4..]).into_owned();
    Ok((code, reason))
}

/// Encodes UNKNOWN-ATTRIBUTES.
pub fn encode_unknown_attributes(types: &[u16]) -> Vec<u8> {
    let mut out = Vec::with_capacity(types.len() * 2);
    for t in types {
        out.extend_from_slice(&t.to_be_bytes());
    }
    out
}

/// Decodes UNKNOWN-ATTRIBUTES.
pub fn decode_unknown_attributes(v: &[u8]) -> Vec<u16> {
    v.as_chunks::<2>()
        .0
        .iter()
        .map(|c| u16::from_be_bytes(*c))
        .collect()
}

/// Decodes a 4-byte big endian u32 attribute (LIFETIME, ...).
pub fn decode_u32(v: &[u8], typ: u16) -> Result<u32, StunError> {
    let b: [u8; 4] = v.try_into().map_err(|_| StunError::BadAttribute(typ))?;
    Ok(u32::from_be_bytes(b))
}

/// Encodes REQUESTED-TRANSPORT.
pub fn encode_requested_transport(protocol: u8) -> [u8; 4] {
    [protocol, 0, 0, 0]
}

/// Encodes CHANNEL-NUMBER.
pub fn encode_channel_number(n: u16) -> [u8; 4] {
    let b = n.to_be_bytes();
    [b[0], b[1], 0, 0]
}

/// Decodes CHANNEL-NUMBER.
pub fn decode_channel_number(v: &[u8]) -> Result<u16, StunError> {
    if v.len() != 4 {
        return Err(StunError::BadAttribute(CHANNEL_NUMBER));
    }
    Ok(u16::from_be_bytes([v[0], v[1]]))
}

/// Standard reason phrase for the error codes this server emits.
pub fn reason_phrase(code: u16) -> &'static str {
    match code {
        300 => "Try Alternate",
        400 => "Bad Request",
        401 => "Unauthorized",
        403 => "Forbidden",
        420 => "Unknown Attribute",
        437 => "Allocation Mismatch",
        438 => "Stale Nonce",
        440 => "Address Family not Supported",
        441 => "Wrong Credentials",
        442 => "Unsupported Transport Protocol",
        443 => "Peer Address Family Mismatch",
        486 => "Allocation Quota Reached",
        500 => "Server Error",
        508 => "Insufficient Capacity",
        _ => "Error",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const TXID: [u8; 12] = [
        0xb7, 0xe7, 0xa7, 0x01, 0xbc, 0x34, 0xd6, 0x86, 0xfa, 0x87, 0xdf, 0xae,
    ];

    #[test]
    fn xor_address_rfc5769_ipv4() {
        // RFC 5769 §2.2: 192.0.2.1:32853
        let v = [0x00, 0x01, 0xa1, 0x47, 0xe1, 0x12, 0xa6, 0x43];
        let addr = decode_xor_address(&v, &TXID, XOR_MAPPED_ADDRESS).unwrap();
        assert_eq!(addr, "192.0.2.1:32853".parse().unwrap());
        assert_eq!(encode_xor_address(addr, &TXID), v.to_vec());
    }

    #[test]
    fn xor_address_rfc5769_ipv6() {
        // RFC 5769 §2.3: [2001:db8:1234:5678:11:2233:4455:6677]:32853
        let v = [
            0x00, 0x02, 0xa1, 0x47, 0x01, 0x13, 0xa9, 0xfa, 0xa5, 0xd3, 0xf1, 0x79, 0xbc, 0x25,
            0xf4, 0xb5, 0xbe, 0xd2, 0xb9, 0xd9,
        ];
        let addr = decode_xor_address(&v, &TXID, XOR_MAPPED_ADDRESS).unwrap();
        assert_eq!(
            addr,
            "[2001:db8:1234:5678:11:2233:4455:6677]:32853"
                .parse()
                .unwrap()
        );
        assert_eq!(encode_xor_address(addr, &TXID), v.to_vec());
    }

    #[test]
    fn error_code_round_trip() {
        let v = encode_error_code(438, "Stale Nonce");
        assert_eq!(&v[..4], &[0, 0, 4, 38]);
        assert_eq!(
            decode_error_code(&v).unwrap(),
            (438, "Stale Nonce".to_string())
        );
    }

    #[test]
    fn unknown_attributes_round_trip() {
        let v = encode_unknown_attributes(&[0x0031, 0x7000]);
        assert_eq!(decode_unknown_attributes(&v), vec![0x0031, 0x7000]);
    }
}
