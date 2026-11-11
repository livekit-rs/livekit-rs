//! STUN message decoding and encoding.

use std::net::SocketAddr;

use hmac::{Hmac, Mac};
use sha1::Sha1;
use subtle::ConstantTimeEq;

use super::attr;
use super::{
    Class, FINGERPRINT_XOR, HEADER_LEN, MAGIC_COOKIE, Method, StunError, message_type, pad4,
    split_message_type,
};

type HmacSha1 = Hmac<Sha1>;

const INTEGRITY_LEN: usize = 20;
const INTEGRITY_ATTR_LEN: usize = 4 + INTEGRITY_LEN;
const FINGERPRINT_ATTR_LEN: usize = 8;

#[derive(Debug, Clone, Copy)]
struct RawAttr {
    typ: u16,
    /// Offset of the value within `Message::raw`.
    start: usize,
    len: usize,
}

/// A decoded STUN message. Holds a copy of the raw bytes so that
/// MESSAGE-INTEGRITY can be verified once the key is known.
#[derive(Debug, Clone)]
pub struct Message {
    method: Method,
    class: Class,
    transaction_id: [u8; 12],
    raw: Vec<u8>,
    attrs: Vec<RawAttr>,
    /// Offset of the MESSAGE-INTEGRITY attribute header, when present.
    integrity_offset: Option<usize>,
    has_fingerprint: bool,
}

impl Message {
    /// Decodes a STUN message from `buf`. Bytes after the length announced in
    /// the header are ignored. Attributes following MESSAGE-INTEGRITY (other
    /// than FINGERPRINT) are ignored as required by RFC 5389 §15.4. A present
    /// FINGERPRINT is validated and must be the last attribute.
    pub fn decode(buf: &[u8]) -> Result<Message, StunError> {
        Self::decode_inner(buf, false)
    }

    /// Go treats Send indications as allocation-scoped data. Their optional
    /// authentication metadata does not affect TLV decoding or authorization.
    pub(crate) fn decode_client(buf: &[u8]) -> Result<Message, StunError> {
        Self::decode_inner(buf, true)
    }

    fn decode_inner(buf: &[u8], turn_client: bool) -> Result<Message, StunError> {
        if buf.len() < HEADER_LEN {
            return Err(StunError::TooShort);
        }
        if buf[0] & 0xC0 != 0 {
            return Err(StunError::NotStun);
        }
        if u32::from_be_bytes([buf[4], buf[5], buf[6], buf[7]]) != MAGIC_COOKIE {
            return Err(StunError::NotStun);
        }
        let typ = u16::from_be_bytes([buf[0], buf[1]]);
        let len = u16::from_be_bytes([buf[2], buf[3]]) as usize;
        if !len.is_multiple_of(4) || HEADER_LEN + len > buf.len() {
            return Err(StunError::BadLength);
        }
        let (code, class) = split_message_type(typ);
        let method = Method::from_code(code).ok_or(StunError::UnknownMethod(code))?;
        let ignore_auth_metadata =
            turn_client && method == Method::Send && class == Class::Indication;
        let mut transaction_id = [0u8; 12];
        transaction_id.copy_from_slice(&buf[8..20]);

        let raw = buf[..HEADER_LEN + len].to_vec();
        let end = raw.len();
        let mut attrs = Vec::new();
        let mut integrity_offset = None;
        let mut has_fingerprint = false;
        let mut off = HEADER_LEN;
        while off < end {
            if has_fingerprint {
                // FINGERPRINT must be last.
                return Err(StunError::BadAttribute(attr::FINGERPRINT));
            }
            if off + 4 > end {
                return Err(StunError::BadLength);
            }
            let at = u16::from_be_bytes([raw[off], raw[off + 1]]);
            let alen = u16::from_be_bytes([raw[off + 2], raw[off + 3]]) as usize;
            let vstart = off + 4;
            if vstart + pad4(alen) > end {
                return Err(StunError::BadAttribute(at));
            }
            match at {
                attr::MESSAGE_INTEGRITY | attr::FINGERPRINT if ignore_auth_metadata => {}
                attr::MESSAGE_INTEGRITY if integrity_offset.is_none() => {
                    if alen != INTEGRITY_LEN {
                        return Err(StunError::BadAttribute(at));
                    }
                    integrity_offset = Some(off);
                }
                attr::FINGERPRINT => {
                    if alen != 4 {
                        return Err(StunError::BadAttribute(at));
                    }
                    let mut tmp = raw[..off].to_vec();
                    let adj = (off + FINGERPRINT_ATTR_LEN - HEADER_LEN) as u16;
                    tmp[2..4].copy_from_slice(&adj.to_be_bytes());
                    let expected = crc32fast::hash(&tmp) ^ FINGERPRINT_XOR;
                    let got = u32::from_be_bytes([
                        raw[vstart],
                        raw[vstart + 1],
                        raw[vstart + 2],
                        raw[vstart + 3],
                    ]);
                    if expected != got {
                        return Err(StunError::FingerprintMismatch);
                    }
                    has_fingerprint = true;
                }
                _ if integrity_offset.is_some() => {
                    // Ignored per RFC 5389 §15.4 (includes MESSAGE-INTEGRITY-SHA256).
                }
                _ => attrs.push(RawAttr {
                    typ: at,
                    start: vstart,
                    len: alen,
                }),
            }
            off = vstart + pad4(alen);
        }

        Ok(Message {
            method,
            class,
            transaction_id,
            raw,
            attrs,
            integrity_offset,
            has_fingerprint,
        })
    }

    pub fn method(&self) -> Method {
        self.method
    }

    pub fn class(&self) -> Class {
        self.class
    }

    pub fn transaction_id(&self) -> &[u8; 12] {
        &self.transaction_id
    }

    /// Raw encoded bytes of the message.
    pub fn raw(&self) -> &[u8] {
        &self.raw
    }

    /// Whether a (validated) FINGERPRINT attribute was present.
    pub fn has_fingerprint(&self) -> bool {
        self.has_fingerprint
    }

    /// Whether MESSAGE-INTEGRITY (HMAC-SHA1) is present.
    pub fn has_integrity(&self) -> bool {
        self.integrity_offset.is_some()
    }

    /// Value of the first attribute of type `typ` (preceding MESSAGE-INTEGRITY).
    pub fn get(&self, typ: u16) -> Option<&[u8]> {
        self.attrs
            .iter()
            .find(|a| a.typ == typ)
            .map(|a| &self.raw[a.start..a.start + a.len])
    }

    /// Values of all attributes of type `typ`.
    pub fn get_all(&self, typ: u16) -> impl Iterator<Item = &[u8]> + '_ {
        self.attrs
            .iter()
            .filter(move |a| a.typ == typ)
            .map(|a| &self.raw[a.start..a.start + a.len])
    }

    /// Whether an attribute of type `typ` is present.
    pub fn contains(&self, typ: u16) -> bool {
        self.attrs.iter().any(|a| a.typ == typ)
    }

    /// Attribute types in order of appearance (excluding MESSAGE-INTEGRITY
    /// and FINGERPRINT).
    pub fn attribute_types(&self) -> impl Iterator<Item = u16> + '_ {
        self.attrs.iter().map(|a| a.typ)
    }

    /// Comprehension-required attribute types not understood by this server.
    pub fn unknown_comprehension_required(&self) -> Vec<u16> {
        let mut out: Vec<u16> = self
            .attribute_types()
            .filter(|t| {
                attr::is_comprehension_required(*t) && !attr::is_known_comprehension_required(*t)
            })
            .collect();
        out.dedup();
        out
    }

    /// Verifies MESSAGE-INTEGRITY with `key` in constant time. Returns false
    /// when the attribute is absent.
    pub fn verify_integrity(&self, key: &[u8]) -> bool {
        let Some(off) = self.integrity_offset else {
            return false;
        };
        let mut header = [0u8; HEADER_LEN];
        header.copy_from_slice(&self.raw[..HEADER_LEN]);
        let adj = (off + INTEGRITY_ATTR_LEN - HEADER_LEN) as u16;
        header[2..4].copy_from_slice(&adj.to_be_bytes());
        let mut mac = HmacSha1::new_from_slice(key).expect("HMAC accepts keys of any length");
        mac.update(&header);
        mac.update(&self.raw[HEADER_LEN..off]);
        let tag = mac.finalize().into_bytes();
        let got = &self.raw[off + 4..off + INTEGRITY_ATTR_LEN];
        bool::from(tag.as_slice().ct_eq(got))
    }

    fn get_str(&self, typ: u16) -> Option<Result<&str, StunError>> {
        self.get(typ)
            .map(|v| std::str::from_utf8(v).map_err(|_| StunError::BadAttribute(typ)))
    }

    pub fn username(&self) -> Option<Result<&str, StunError>> {
        self.get_str(attr::USERNAME)
    }

    pub fn realm(&self) -> Option<Result<&str, StunError>> {
        self.get_str(attr::REALM)
    }

    pub fn nonce(&self) -> Option<&[u8]> {
        self.get(attr::NONCE)
    }

    pub fn software(&self) -> Option<Result<&str, StunError>> {
        self.get_str(attr::SOFTWARE)
    }

    pub fn lifetime(&self) -> Option<Result<u32, StunError>> {
        self.get(attr::LIFETIME)
            .map(|v| attr::decode_u32(v, attr::LIFETIME))
    }

    /// Decodes the first XOR-*-ADDRESS attribute of type `typ`.
    pub fn xor_address(&self, typ: u16) -> Option<Result<SocketAddr, StunError>> {
        self.get(typ)
            .map(|v| attr::decode_xor_address(v, &self.transaction_id, typ))
    }

    /// Decodes all XOR-PEER-ADDRESS attributes.
    pub fn xor_peer_addresses(&self) -> Result<Vec<SocketAddr>, StunError> {
        self.get_all(attr::XOR_PEER_ADDRESS)
            .map(|v| attr::decode_xor_address(v, &self.transaction_id, attr::XOR_PEER_ADDRESS))
            .collect()
    }

    pub fn error_code(&self) -> Option<Result<(u16, String), StunError>> {
        self.get(attr::ERROR_CODE).map(attr::decode_error_code)
    }

    /// REQUESTED-TRANSPORT protocol number.
    pub fn requested_transport(&self) -> Option<Result<u8, StunError>> {
        self.get(attr::REQUESTED_TRANSPORT).map(|v| {
            if v.len() != 4 {
                Err(StunError::BadAttribute(attr::REQUESTED_TRANSPORT))
            } else {
                Ok(v[0])
            }
        })
    }

    /// REQUESTED-ADDRESS-FAMILY family byte.
    pub fn requested_address_family(&self) -> Option<Result<u8, StunError>> {
        self.get(attr::REQUESTED_ADDRESS_FAMILY).map(|v| {
            if v.len() != 4 {
                Err(StunError::BadAttribute(attr::REQUESTED_ADDRESS_FAMILY))
            } else {
                Ok(v[0])
            }
        })
    }

    /// EVEN-PORT: returns the R (reserve) bit.
    pub fn even_port(&self) -> Option<Result<bool, StunError>> {
        self.get(attr::EVEN_PORT).map(|v| {
            if v.is_empty() {
                Err(StunError::BadAttribute(attr::EVEN_PORT))
            } else {
                Ok(v[0] & 0x80 != 0)
            }
        })
    }

    pub fn channel_number(&self) -> Option<Result<u16, StunError>> {
        self.get(attr::CHANNEL_NUMBER)
            .map(attr::decode_channel_number)
    }

    pub fn data(&self) -> Option<&[u8]> {
        self.get(attr::DATA)
    }
}

/// Incremental STUN message encoder.
#[derive(Debug, Clone)]
pub struct MessageBuilder {
    buf: Vec<u8>,
}

impl MessageBuilder {
    pub fn new(method: Method, class: Class, transaction_id: [u8; 12]) -> MessageBuilder {
        let mut buf = Vec::with_capacity(128);
        buf.extend_from_slice(&message_type(method, class).to_be_bytes());
        buf.extend_from_slice(&[0, 0]);
        buf.extend_from_slice(&MAGIC_COOKIE.to_be_bytes());
        buf.extend_from_slice(&transaction_id);
        MessageBuilder { buf }
    }

    /// Creates a builder for a response of `class` to `req`.
    pub fn response_to(req: &Message, class: Class) -> MessageBuilder {
        MessageBuilder::new(req.method(), class, *req.transaction_id())
    }

    fn transaction_id(&self) -> [u8; 12] {
        let mut t = [0u8; 12];
        t.copy_from_slice(&self.buf[8..20]);
        t
    }

    fn set_len(&mut self, len: usize) {
        let l = len as u16;
        self.buf[2..4].copy_from_slice(&l.to_be_bytes());
    }

    /// Appends a raw attribute with zero padding.
    pub fn add(&mut self, typ: u16, value: &[u8]) -> &mut Self {
        debug_assert!(value.len() <= u16::MAX as usize);
        self.buf.extend_from_slice(&typ.to_be_bytes());
        self.buf
            .extend_from_slice(&(value.len() as u16).to_be_bytes());
        self.buf.extend_from_slice(value);
        let padded = self.buf.len() + (pad4(value.len()) - value.len());
        self.buf.resize(padded, 0);
        let l = self.buf.len() - HEADER_LEN;
        self.set_len(l);
        self
    }

    pub fn add_xor_address(&mut self, typ: u16, addr: SocketAddr) -> &mut Self {
        let v = attr::encode_xor_address(addr, &self.transaction_id());
        self.add(typ, &v)
    }

    pub fn add_error(&mut self, code: u16) -> &mut Self {
        let v = attr::encode_error_code(code, attr::reason_phrase(code));
        self.add(attr::ERROR_CODE, &v)
    }

    pub fn add_u32(&mut self, typ: u16, value: u32) -> &mut Self {
        self.add(typ, &value.to_be_bytes())
    }

    /// Finishes the message, optionally appending MESSAGE-INTEGRITY computed
    /// with `integrity_key` and then FINGERPRINT.
    pub fn build(mut self, integrity_key: Option<&[u8]>, fingerprint: bool) -> Vec<u8> {
        if let Some(key) = integrity_key {
            let l = self.buf.len() - HEADER_LEN + INTEGRITY_ATTR_LEN;
            self.set_len(l);
            let mut mac = HmacSha1::new_from_slice(key).expect("HMAC accepts keys of any length");
            mac.update(&self.buf);
            let tag = mac.finalize().into_bytes();
            self.buf
                .extend_from_slice(&attr::MESSAGE_INTEGRITY.to_be_bytes());
            self.buf
                .extend_from_slice(&(INTEGRITY_LEN as u16).to_be_bytes());
            self.buf.extend_from_slice(tag.as_slice());
        }
        if fingerprint {
            let l = self.buf.len() - HEADER_LEN + FINGERPRINT_ATTR_LEN;
            self.set_len(l);
            let crc = crc32fast::hash(&self.buf) ^ FINGERPRINT_XOR;
            self.buf.extend_from_slice(&attr::FINGERPRINT.to_be_bytes());
            self.buf.extend_from_slice(&4u16.to_be_bytes());
            self.buf.extend_from_slice(&crc.to_be_bytes());
        }
        self.buf
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// RFC 5769 §2.1 sample request (short-term credentials).
    const RFC5769_REQUEST: [u8; 108] = [
        0x00, 0x01, 0x00, 0x58, 0x21, 0x12, 0xa4, 0x42, 0xb7, 0xe7, 0xa7, 0x01, 0xbc, 0x34, 0xd6,
        0x86, 0xfa, 0x87, 0xdf, 0xae, 0x80, 0x22, 0x00, 0x10, 0x53, 0x54, 0x55, 0x4e, 0x20, 0x74,
        0x65, 0x73, 0x74, 0x20, 0x63, 0x6c, 0x69, 0x65, 0x6e, 0x74, 0x00, 0x24, 0x00, 0x04, 0x6e,
        0x00, 0x01, 0xff, 0x80, 0x29, 0x00, 0x08, 0x93, 0x2f, 0xf9, 0xb1, 0x51, 0x26, 0x3b, 0x36,
        0x00, 0x06, 0x00, 0x09, 0x65, 0x76, 0x74, 0x6a, 0x3a, 0x68, 0x36, 0x76, 0x59, 0x20, 0x20,
        0x20, 0x00, 0x08, 0x00, 0x14, 0x9a, 0xea, 0xa7, 0x0c, 0xbf, 0xd8, 0xcb, 0x56, 0x78, 0x1e,
        0xf2, 0xb5, 0xb2, 0xd3, 0xf2, 0x49, 0xc1, 0xb5, 0x71, 0xa2, 0x80, 0x28, 0x00, 0x04, 0xe5,
        0x7a, 0x3b, 0xcf,
    ];

    /// RFC 5769 §2.2 sample IPv4 response.
    const RFC5769_RESPONSE_V4: [u8; 80] = [
        0x01, 0x01, 0x00, 0x3c, 0x21, 0x12, 0xa4, 0x42, 0xb7, 0xe7, 0xa7, 0x01, 0xbc, 0x34, 0xd6,
        0x86, 0xfa, 0x87, 0xdf, 0xae, 0x80, 0x22, 0x00, 0x0b, 0x74, 0x65, 0x73, 0x74, 0x20, 0x76,
        0x65, 0x63, 0x74, 0x6f, 0x72, 0x20, 0x00, 0x20, 0x00, 0x08, 0x00, 0x01, 0xa1, 0x47, 0xe1,
        0x12, 0xa6, 0x43, 0x00, 0x08, 0x00, 0x14, 0x2b, 0x91, 0xf5, 0x99, 0xfd, 0x9e, 0x90, 0xc3,
        0x8c, 0x74, 0x89, 0xf9, 0x2a, 0xf9, 0xba, 0x53, 0xf0, 0x6b, 0xe7, 0xd7, 0x80, 0x28, 0x00,
        0x04, 0xc0, 0x7d, 0x4c, 0x96,
    ];

    const RFC5769_PASSWORD: &[u8] = b"VOkJxbRl1RmTxUk/WvJxBt";

    #[test]
    fn rfc5769_request_integrity_and_fingerprint() {
        let m = Message::decode(&RFC5769_REQUEST).expect("decode");
        assert_eq!(m.method(), Method::Binding);
        assert_eq!(m.class(), Class::Request);
        assert!(m.has_fingerprint());
        assert!(m.has_integrity());
        assert_eq!(m.username().unwrap().unwrap(), "evtj:h6vY");
        assert_eq!(m.software().unwrap().unwrap(), "STUN test client");
        assert!(m.verify_integrity(RFC5769_PASSWORD));
        assert!(!m.verify_integrity(b"wrong password"));
    }

    #[test]
    fn rfc5769_request_fingerprint_tamper_detected() {
        let mut bad = RFC5769_REQUEST;
        bad[107] ^= 0x01;
        assert_eq!(
            Message::decode(&bad).unwrap_err(),
            StunError::FingerprintMismatch
        );
    }

    #[test]
    fn rfc5769_response_ipv4() {
        let m = Message::decode(&RFC5769_RESPONSE_V4).expect("decode");
        assert_eq!(m.class(), Class::Success);
        assert!(m.verify_integrity(RFC5769_PASSWORD));
        let addr = m.xor_address(attr::XOR_MAPPED_ADDRESS).unwrap().unwrap();
        assert_eq!(addr, "192.0.2.1:32853".parse().unwrap());
    }

    #[test]
    fn rfc5769_response_reencode_matches() {
        // Re-encoding the same attributes must yield the identical bytes,
        // proving our MESSAGE-INTEGRITY and FINGERPRINT computation.
        let txid: [u8; 12] = RFC5769_RESPONSE_V4[8..20].try_into().unwrap();
        let mut b = MessageBuilder::new(Method::Binding, Class::Success, txid);
        b.add(attr::SOFTWARE, b"test vector");
        b.add_xor_address(attr::XOR_MAPPED_ADDRESS, "192.0.2.1:32853".parse().unwrap());
        // The RFC vector pads SOFTWARE with 0x20 where we pad with zero;
        // patch the single padding byte so the output must be bit-identical.
        assert_eq!(b.buf[35], 0);
        b.buf[35] = 0x20;
        let out = b.build(Some(RFC5769_PASSWORD), true);
        assert_eq!(out.as_slice(), &RFC5769_RESPONSE_V4[..]);
    }

    #[test]
    fn rfc5769_request_reencode_matches() {
        let txid: [u8; 12] = RFC5769_REQUEST[8..20].try_into().unwrap();
        let mut b = MessageBuilder::new(Method::Binding, Class::Request, txid);
        b.add(attr::SOFTWARE, b"STUN test client")
            .add(attr::PRIORITY, &[0x6e, 0x00, 0x01, 0xff])
            .add(
                attr::ICE_CONTROLLED,
                &[0x93, 0x2f, 0xf9, 0xb1, 0x51, 0x26, 0x3b, 0x36],
            )
            .add(attr::USERNAME, b"evtj:h6vY");
        // RFC vector uses 0x20 as USERNAME padding.
        b.buf[73..76].copy_from_slice(b"   ");
        let out = b.build(Some(RFC5769_PASSWORD), true);
        assert_eq!(out.as_slice(), &RFC5769_REQUEST[..]);
    }

    #[test]
    fn encode_decode_round_trip() {
        let txid = [7u8; 12];
        let peer: SocketAddr = "203.0.113.5:4000".parse().unwrap();
        let peer6: SocketAddr = "[2001:db8::1]:5000".parse().unwrap();
        let mut b = MessageBuilder::new(Method::CreatePermission, Class::Request, txid);
        b.add(attr::USERNAME, b"alice")
            .add(attr::REALM, b"livekit")
            .add(attr::NONCE, b"abcdef")
            .add_xor_address(attr::XOR_PEER_ADDRESS, peer)
            .add_xor_address(attr::XOR_PEER_ADDRESS, peer6)
            .add_u32(attr::LIFETIME, 600)
            .add(
                attr::REQUESTED_TRANSPORT,
                &attr::encode_requested_transport(attr::PROTO_UDP),
            )
            .add(attr::CHANNEL_NUMBER, &attr::encode_channel_number(0x4001))
            .add(attr::DATA, b"xyz");
        let key = [1u8; 16];
        let out = b.build(Some(&key), true);
        assert_eq!(out.len() % 4, 0);

        let m = Message::decode(&out).unwrap();
        assert_eq!(m.method(), Method::CreatePermission);
        assert_eq!(m.class(), Class::Request);
        assert_eq!(m.transaction_id(), &txid);
        assert_eq!(m.username().unwrap().unwrap(), "alice");
        assert_eq!(m.realm().unwrap().unwrap(), "livekit");
        assert_eq!(m.nonce().unwrap(), b"abcdef");
        assert_eq!(m.xor_peer_addresses().unwrap(), vec![peer, peer6]);
        assert_eq!(m.lifetime().unwrap().unwrap(), 600);
        assert_eq!(m.requested_transport().unwrap().unwrap(), 17);
        assert_eq!(m.channel_number().unwrap().unwrap(), 0x4001);
        assert_eq!(m.data().unwrap(), b"xyz");
        assert!(m.has_fingerprint());
        assert!(m.verify_integrity(&key));
        assert!(!m.verify_integrity(&[2u8; 16]));
    }

    #[test]
    fn error_response_round_trip() {
        let mut b = MessageBuilder::new(Method::Allocate, Class::Error, [3u8; 12]);
        b.add_error(401)
            .add(attr::REALM, b"livekit")
            .add(attr::NONCE, b"n");
        let out = b.build(None, true);
        let m = Message::decode(&out).unwrap();
        assert_eq!(m.class(), Class::Error);
        assert_eq!(
            m.error_code().unwrap().unwrap(),
            (401, "Unauthorized".to_string())
        );
        assert!(!m.has_integrity());
    }

    #[test]
    fn attributes_after_integrity_are_ignored() {
        let mut b = MessageBuilder::new(Method::Refresh, Class::Request, [9u8; 12]);
        b.add(attr::USERNAME, b"bob");
        let mut out = b.build(Some(b"k"), false);
        // Append a LIFETIME after MESSAGE-INTEGRITY and fix up the length.
        out.extend_from_slice(&attr::LIFETIME.to_be_bytes());
        out.extend_from_slice(&4u16.to_be_bytes());
        out.extend_from_slice(&0u32.to_be_bytes());
        let l = (out.len() - HEADER_LEN) as u16;
        out[2..4].copy_from_slice(&l.to_be_bytes());
        let m = Message::decode(&out).unwrap();
        assert!(m.lifetime().is_none());
        assert!(m.verify_integrity(b"k"));
    }

    #[test]
    fn turn_send_indication_ignores_auth_metadata_without_relaxing_other_messages() {
        let mut builder = MessageBuilder::new(Method::Send, Class::Indication, [9u8; 12]);
        builder
            .add(attr::MESSAGE_INTEGRITY, &[0; INTEGRITY_LEN])
            .add(attr::DATA, b"payload");
        let mut wire = builder.build(None, true);
        *wire.last_mut().unwrap() ^= 1;
        assert_eq!(
            Message::decode(&wire).unwrap_err(),
            StunError::FingerprintMismatch
        );
        let message = Message::decode_client(&wire).unwrap();
        assert_eq!(message.data(), Some(b"payload".as_slice()));
        assert!(!message.has_integrity());
        assert!(!message.has_fingerprint());
        wire[..2].copy_from_slice(&message_type(Method::Send, Class::Request).to_be_bytes());
        assert_eq!(
            Message::decode_client(&wire).unwrap_err(),
            StunError::FingerprintMismatch
        );
        wire[..2].copy_from_slice(&message_type(Method::Send, Class::Indication).to_be_bytes());
        wire.pop();
        assert_eq!(
            Message::decode_client(&wire).unwrap_err(),
            StunError::BadLength
        );
    }

    #[test]
    fn decode_rejects_garbage() {
        assert_eq!(
            Message::decode(&[0u8; 10]).unwrap_err(),
            StunError::TooShort
        );
        let mut bad_cookie = RFC5769_REQUEST;
        bad_cookie[4] = 0;
        assert_eq!(
            Message::decode(&bad_cookie).unwrap_err(),
            StunError::NotStun
        );
        let mut bad_len = RFC5769_REQUEST;
        bad_len[3] = 0xFC;
        assert_eq!(Message::decode(&bad_len).unwrap_err(), StunError::BadLength);
    }

    #[test]
    fn unknown_comprehension_required_detected() {
        let mut b = MessageBuilder::new(Method::Allocate, Class::Request, [1u8; 12]);
        b.add(0x0031, b"x").add(0x8077, b"optional");
        let m = Message::decode(&b.build(None, false)).unwrap();
        assert_eq!(m.unknown_comprehension_required(), vec![0x0031]);
    }
}
