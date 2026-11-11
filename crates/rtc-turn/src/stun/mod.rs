//! Minimal STUN (RFC 5389 / RFC 8489) and TURN (RFC 8656) message codec.
//!
//! * [`Message`] is a decoded, borrowed-free view of a STUN message that keeps
//!   the raw bytes so MESSAGE-INTEGRITY can be verified later (once the key is
//!   known from the auth handler).
//! * [`MessageBuilder`] encodes messages, optionally appending
//!   MESSAGE-INTEGRITY (HMAC-SHA1) and FINGERPRINT (CRC-32).
//! * [`ChannelData`] handles the TURN ChannelData framing.

pub mod attr;
mod message;

pub use message::{Message, MessageBuilder};

/// STUN magic cookie (RFC 5389 §6).
pub const MAGIC_COOKIE: u32 = 0x2112_A442;
/// Size of the fixed STUN header.
pub const HEADER_LEN: usize = 20;
/// Size of the ChannelData header.
pub const CHANNEL_DATA_HEADER_LEN: usize = 4;
/// XOR constant applied to the CRC-32 of FINGERPRINT.
pub const FINGERPRINT_XOR: u32 = 0x5354_554E;

/// Lowest valid TURN channel number.
pub const CHANNEL_MIN: u16 = 0x4000;
/// Highest valid TURN channel number (RFC 5766; 0x7FFF is reserved).
pub const CHANNEL_MAX: u16 = 0x7FFE;

/// STUN/TURN methods understood by this crate.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Method {
    Binding,
    Allocate,
    Refresh,
    Send,
    Data,
    CreatePermission,
    ChannelBind,
}

impl Method {
    /// The 12-bit method number.
    pub const fn code(self) -> u16 {
        match self {
            Method::Binding => 0x001,
            Method::Allocate => 0x003,
            Method::Refresh => 0x004,
            Method::Send => 0x006,
            Method::Data => 0x007,
            Method::CreatePermission => 0x008,
            Method::ChannelBind => 0x009,
        }
    }

    /// Maps a 12-bit method number to a [`Method`].
    pub const fn from_code(code: u16) -> Option<Method> {
        Some(match code {
            0x001 => Method::Binding,
            0x003 => Method::Allocate,
            0x004 => Method::Refresh,
            0x006 => Method::Send,
            0x007 => Method::Data,
            0x008 => Method::CreatePermission,
            0x009 => Method::ChannelBind,
            _ => return None,
        })
    }
}

impl std::fmt::Display for Method {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let s = match self {
            Method::Binding => "Binding",
            Method::Allocate => "Allocate",
            Method::Refresh => "Refresh",
            Method::Send => "Send",
            Method::Data => "Data",
            Method::CreatePermission => "CreatePermission",
            Method::ChannelBind => "ChannelBind",
        };
        f.write_str(s)
    }
}

/// STUN message class.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Class {
    Request,
    Indication,
    Success,
    Error,
}

impl Class {
    const fn bits(self) -> u16 {
        match self {
            Class::Request => 0b00,
            Class::Indication => 0b01,
            Class::Success => 0b10,
            Class::Error => 0b11,
        }
    }

    const fn from_bits(bits: u16) -> Class {
        match bits & 0b11 {
            0b00 => Class::Request,
            0b01 => Class::Indication,
            0b10 => Class::Success,
            _ => Class::Error,
        }
    }
}

/// Encodes method + class into the 14-bit STUN message type (RFC 5389 §6).
pub const fn message_type(method: Method, class: Class) -> u16 {
    let m = method.code();
    let c = class.bits();
    (m & 0x000F) | ((m & 0x0070) << 1) | ((m & 0x0F80) << 2) | ((c & 0b01) << 4) | ((c & 0b10) << 7)
}

/// Splits a STUN message type into (method number, class).
pub const fn split_message_type(t: u16) -> (u16, Class) {
    let m = (t & 0x000F) | ((t & 0x00E0) >> 1) | ((t & 0x3E00) >> 2);
    let c = ((t >> 4) & 0b01) | ((t >> 7) & 0b10);
    (m, Class::from_bits(c))
}

/// Codec errors.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum StunError {
    #[error("buffer too short")]
    TooShort,
    #[error("not a STUN message")]
    NotStun,
    #[error("unknown STUN method {0:#05x}")]
    UnknownMethod(u16),
    #[error("malformed message length")]
    BadLength,
    #[error("malformed attribute {0:#06x}")]
    BadAttribute(u16),
    #[error("FINGERPRINT mismatch")]
    FingerprintMismatch,
    #[error("invalid ChannelData")]
    BadChannelData,
}

/// Returns `true` when the first byte indicates a STUN message (top two bits 0b00).
pub fn is_stun(buf: &[u8]) -> bool {
    buf.len() >= HEADER_LEN
        && buf[0] & 0xC0 == 0
        && u32::from_be_bytes([buf[4], buf[5], buf[6], buf[7]]) == MAGIC_COOKIE
}

/// Returns `true` when the first byte indicates ChannelData (0x40..=0x7F).
pub fn is_channel_data(buf: &[u8]) -> bool {
    buf.len() >= CHANNEL_DATA_HEADER_LEN && (0x40..=0x7F).contains(&buf[0])
}

/// Rounds `n` up to a multiple of 4.
pub const fn pad4(n: usize) -> usize {
    (n + 3) & !3
}

/// For stream transports: given the start of the buffered bytes, returns the
/// total length of the first frame (STUN message or padded ChannelData), or
/// `Ok(None)` if more bytes are needed to tell. Errors mean the stream is not
/// speaking STUN/TURN and should be closed.
pub fn stream_frame_len(buf: &[u8]) -> Result<Option<usize>, StunError> {
    if buf.len() < 4 {
        return Ok(None);
    }
    let len = u16::from_be_bytes([buf[2], buf[3]]) as usize;
    match buf[0] & 0xC0 {
        0x00 => {
            if !len.is_multiple_of(4) {
                return Err(StunError::BadLength);
            }
            Ok(Some(HEADER_LEN + len))
        }
        0x40 => Ok(Some(CHANNEL_DATA_HEADER_LEN + pad4(len))),
        _ => Err(StunError::NotStun),
    }
}

/// A decoded ChannelData message (RFC 8656 §12.4).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ChannelData<'a> {
    pub number: u16,
    pub data: &'a [u8],
}

impl<'a> ChannelData<'a> {
    /// Decodes ChannelData. Trailing padding (present on stream transports)
    /// is accepted and ignored.
    pub fn decode(buf: &'a [u8]) -> Result<ChannelData<'a>, StunError> {
        if buf.len() < CHANNEL_DATA_HEADER_LEN {
            return Err(StunError::TooShort);
        }
        let number = u16::from_be_bytes([buf[0], buf[1]]);
        if !(CHANNEL_MIN..=0x7FFF).contains(&number) {
            return Err(StunError::BadChannelData);
        }
        let len = u16::from_be_bytes([buf[2], buf[3]]) as usize;
        let end = CHANNEL_DATA_HEADER_LEN + len;
        if buf.len() < end {
            return Err(StunError::BadChannelData);
        }
        Ok(ChannelData {
            number,
            data: &buf[CHANNEL_DATA_HEADER_LEN..end],
        })
    }

    /// Encodes ChannelData into a new buffer; `pad` appends zero padding to a
    /// multiple of 4 bytes (mandatory over TCP/TLS, optional over UDP).
    pub fn encode(number: u16, data: &[u8], pad: bool) -> Vec<u8> {
        let total = if pad {
            CHANNEL_DATA_HEADER_LEN + pad4(data.len())
        } else {
            CHANNEL_DATA_HEADER_LEN + data.len()
        };
        let mut out = Vec::with_capacity(total);
        out.extend_from_slice(&number.to_be_bytes());
        out.extend_from_slice(&(data.len() as u16).to_be_bytes());
        out.extend_from_slice(data);
        out.resize(total, 0);
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn message_type_round_trip() {
        for m in [
            Method::Binding,
            Method::Allocate,
            Method::Refresh,
            Method::Send,
            Method::Data,
            Method::CreatePermission,
            Method::ChannelBind,
        ] {
            for c in [
                Class::Request,
                Class::Indication,
                Class::Success,
                Class::Error,
            ] {
                let t = message_type(m, c);
                let (code, class) = split_message_type(t);
                assert_eq!(Method::from_code(code), Some(m));
                assert_eq!(class, c);
            }
        }
        // Well known values.
        assert_eq!(message_type(Method::Binding, Class::Request), 0x0001);
        assert_eq!(message_type(Method::Binding, Class::Success), 0x0101);
        assert_eq!(message_type(Method::Allocate, Class::Error), 0x0113);
        assert_eq!(message_type(Method::Send, Class::Indication), 0x0016);
        assert_eq!(message_type(Method::Data, Class::Indication), 0x0017);
    }

    #[test]
    fn channel_data_round_trip_unpadded() {
        let enc = ChannelData::encode(0x4001, b"hello", false);
        assert_eq!(enc.len(), 9);
        assert_eq!(&enc[..4], &[0x40, 0x01, 0x00, 0x05]);
        assert!(is_channel_data(&enc));
        let cd = ChannelData::decode(&enc).unwrap();
        assert_eq!(cd.number, 0x4001);
        assert_eq!(cd.data, b"hello");
    }

    #[test]
    fn channel_data_padded_for_stream() {
        let enc = ChannelData::encode(0x7FFE, b"hello", true);
        assert_eq!(enc.len(), 12);
        assert_eq!(&enc[9..], &[0, 0, 0]);
        assert_eq!(stream_frame_len(&enc), Ok(Some(12)));
        let cd = ChannelData::decode(&enc).unwrap();
        assert_eq!(cd.data, b"hello");
    }

    #[test]
    fn channel_data_rejects_bad_input() {
        assert_eq!(
            ChannelData::decode(&[0x40, 0x00, 0x00]),
            Err(StunError::TooShort)
        );
        // length says 8 but only 2 bytes present
        assert_eq!(
            ChannelData::decode(&[0x40, 0x00, 0x00, 0x08, 1, 2]),
            Err(StunError::BadChannelData)
        );
        // not in channel range
        assert_eq!(
            ChannelData::decode(&[0x80, 0x00, 0x00, 0x00]),
            Err(StunError::BadChannelData)
        );
    }

    #[test]
    fn stream_frame_len_stun_and_invalid() {
        // STUN header claiming 8 bytes of attributes.
        let hdr = [0x00, 0x01, 0x00, 0x08];
        assert_eq!(stream_frame_len(&hdr), Ok(Some(28)));
        assert_eq!(stream_frame_len(&[0x00, 0x01, 0x00]), Ok(None));
        assert_eq!(
            stream_frame_len(&[0x00, 0x01, 0x00, 0x07]),
            Err(StunError::BadLength)
        );
        assert_eq!(
            stream_frame_len(&[0xC0, 0x01, 0x00, 0x00]),
            Err(StunError::NotStun)
        );
    }
}
