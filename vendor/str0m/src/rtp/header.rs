#![allow(clippy::unusual_byte_groupings)]

use super::ext::{ExtensionMap, ExtensionValues, ExtensionsForm};
use super::{MAX_BLANK_PADDING_PAYLOAD_SIZE, Pt, SeqNo, Ssrc};

/// Parsed header from an RTP packet.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RtpHeader {
    /// Always 2
    pub version: u8,
    /// Whether the RTP packet has padding to be an equal of 4 bytes.
    pub has_padding: bool,
    /// RTP packet has "RTP header extensions".
    pub has_extension: bool,
    /// Number of contributing source identifiers.
    pub csrc_count: usize,
    /// For video, this marker signifies the end of a series of packets that
    /// together form a single video frame.
    /// For audio, it marks the beginning of a talkspurt, which is a burst of
    /// audio packets.
    pub marker: bool,
    /// Type of payload being carried. What this correlates to is sent in the SDP.
    pub payload_type: Pt,
    /// Sequence number increasing by 1 for each RTP packet.
    pub sequence_number: u16,
    /// Timestamp in media time for the RTP packet. What the media time base is depends
    /// on the codec.
    pub timestamp: u32,
    /// Sender source identifier.
    pub ssrc: Ssrc,
    /// Contributing source identifiers (CSRC). Up to 15 values per RFC 3550.
    pub csrc: [u32; 15],
    /// The extension values parsed using the mapping via SDP.
    pub ext_vals: ExtensionValues,
    /// Length of header.
    pub header_len: usize,
}

impl RtpHeader {
    pub(crate) fn write_to(&self, buf: &mut [u8], exts: &ExtensionMap) -> usize {
        assert!(self.csrc_count <= 15, "CSRC count must be <= 15");
        buf[0] = 0b10_0_0_0000
            | if self.has_padding { 1 << 5 } else { 0 }
            | if self.has_extension { 1 << 4 } else { 0 }
            | (self.csrc_count as u8);

        assert!(*self.payload_type <= 127);
        buf[1] = *self.payload_type & 0b0111_1111 | if self.marker { 1 << 7 } else { 0 };

        buf[2..4].copy_from_slice(&self.sequence_number.to_be_bytes());
        buf[4..8].copy_from_slice(&self.timestamp.to_be_bytes());
        buf[8..12].copy_from_slice(&self.ssrc.to_be_bytes());

        let csrc_len = self.csrc_count * 4;
        for i in 0..self.csrc_count {
            let offset = 12 + i * 4;
            buf[offset..offset + 4].copy_from_slice(&self.csrc[i].to_be_bytes());
        }

        let ext_start = 12 + csrc_len;
        let exts_form = exts.form(&self.ext_vals);
        buf[ext_start..ext_start + 2].copy_from_slice(&exts_form.serialize());

        let ext_buf = &mut buf[ext_start + 4..];
        let mut ext_len = exts.write_to(ext_buf, &self.ext_vals, exts_form);

        let pad = 4 - ext_len % 4;
        if pad < 4 {
            ext_len += pad;
            for i in 0..pad {
                ext_buf[ext_len - i - 1] = 0;
            }
        }

        let bede_len = (ext_len / 4) as u16;
        buf[ext_start + 2..ext_start + 4].copy_from_slice(&bede_len.to_be_bytes());

        ext_start + 4 + ext_len
    }

    fn do_pad(buf: &mut [u8], from: usize, pad: usize) {
        assert!(pad <= 255);

        // set the padding bit
        buf[0] |= 0b00_1_0_0000;

        for i in 0..pad {
            buf[from + i] = 0;
        }

        buf[from + pad - 1] = pad as u8;
    }

    /// Write a packet consisting entirely of padding and write.
    pub fn create_padding_packet(
        buf: &mut [u8],
        header_len: usize,
        pad_len: u8,
        block_size: usize,
    ) -> usize {
        if pad_len == 0 {
            warn!("Not generating padding packet with zero length");
            return 0;
        }

        let rounded_len = if pad_len as usize % block_size == 0 {
            pad_len as usize
        } else {
            ((pad_len as usize / block_size) + 1) * block_size
        }
        .min(MAX_BLANK_PADDING_PAYLOAD_SIZE);

        Self::do_pad(buf, header_len, rounded_len);

        rounded_len
    }

    /// Returns `buf` with any trailing SRTP padding removed, or `None` if the
    /// trailing padding length is invalid.
    ///
    /// Fixed livekit/webrtc-pion parity: `rtp.Packet.Unmarshal` rejects a set
    /// padding bit with no trailing byte (`errTooSmall`) and a zero pad count
    /// (`errInvalidRTPPadding`). Both must drop the whole packet rather than
    /// publish an empty/zero-padded payload as media.
    pub(crate) fn unpad_payload(buf: &[u8]) -> Option<&[u8]> {
        if buf.is_empty() {
            return None;
        }
        let pad_len = buf[buf.len() - 1] as usize;
        if pad_len == 0 {
            return None;
        }
        let unpadded_len = buf.len().checked_sub(pad_len)?;
        Some(&buf[..unpadded_len])
    }

    pub(crate) fn parse(buf: &[u8], exts: &ExtensionMap) -> Option<RtpHeader> {
        let orig_len = buf.len();
        if buf.len() < 12 {
            trace!("RTP header too short < 12: {}", buf.len());
            return None;
        }

        let version = (buf[0] & 0b1100_0000) >> 6;
        if version != 2 {
            trace!("RTP version is not 2");
            return None;
        }
        let has_padding = buf[0] & 0b0010_0000 > 0;
        let has_extension = buf[0] & 0b0001_0000 > 0;
        let csrc_count = (buf[0] & 0b0000_1111) as usize;
        let marker = buf[1] & 0b1000_0000 > 0;
        let payload_type = (buf[1] & 0b0111_1111).into();
        let sequence_number = u16::from_be_bytes([buf[2], buf[3]]);

        let timestamp = u32::from_be_bytes([buf[4], buf[5], buf[6], buf[7]]);

        let ssrc = u32::from_be_bytes([buf[8], buf[9], buf[10], buf[11]]);

        let buf: &[u8] = &buf[12..];

        let csrc_len = 4 * csrc_count;
        if buf.len() < csrc_len {
            trace!("RTP header invalid, not enough csrc");
            return None;
        }

        let mut csrc = [0_u32; 15];
        for (i, item) in csrc.iter_mut().enumerate().take(csrc_count) {
            let offset = i * 4;
            *item = u32::from_be_bytes([
                buf[offset],
                buf[offset + 1],
                buf[offset + 2],
                buf[offset + 3],
            ]);
        }

        let buf: &[u8] = &buf[csrc_len..];

        let mut ext = ExtensionValues {
            ..Default::default()
        };

        let rest = if !has_extension {
            buf
        } else {
            if buf.len() < 4 {
                trace!("RTP bad header extension");
                return None;
            }

            let Some(exts_form) = ExtensionsForm::parse([buf[0], buf[1]]) else {
                trace!(
                    "Ignoring unknown RTP header extensions form: {:?}",
                    u16::from_be_bytes([buf[0], buf[1]])
                );
                return None;
            };
            let ext_words = u16::from_be_bytes([buf[2], buf[3]]);
            let ext_len = ext_words as usize * 4;

            let buf: &[u8] = &buf[4..];

            if buf.len() < ext_len {
                trace!("RTP ext len larger than header {} > {}", buf.len(), ext_len);
                return None;
            }

            exts.parse(&buf[..ext_len], exts_form, &mut ext);
            &buf[ext_len..]
        };

        let header_len = orig_len - rest.len();

        let ret = RtpHeader {
            version,
            has_padding,
            has_extension,
            csrc_count,
            marker,
            payload_type,
            sequence_number,
            timestamp,
            ssrc: ssrc.into(),
            csrc,
            ext_vals: ext,
            header_len,
        };

        Some(ret)
    }

    /// For RTX the original sequence number is inserted before the RTP payload.
    pub fn read_original_sequence_number(buf: &[u8], seq_no: &mut u16) -> usize {
        if buf.len() < 2 {
            // This is not an RTX packet, probably a padding packet
            return 0;
        }
        *seq_no = u16::from_be_bytes([buf[0], buf[1]]);
        2
    }

    /// For RTX the original sequence number is inserted before the RTP payload.
    pub fn write_original_sequence_number(buf: &mut [u8], seq_no: SeqNo) -> usize {
        let seq_u16 = (*seq_no) as u16;
        buf[0..2].copy_from_slice(&seq_u16.to_be_bytes());
        2
    }

    /// Sequencer number of this RTP header given the previous number.
    ///
    /// The logic detects wrap-arounds of the 16-bit RTP sequence number.
    pub fn sequence_number(&self, previous: Option<SeqNo>) -> SeqNo {
        let e_seq = extend_u16(previous.map(|v| *v), self.sequence_number);
        e_seq.into()
    }
}

macro_rules! mk_extend {
    ($id:ident, $t:ty, $bits:expr) => {
        /// "extend" a less than 64 bit sequence number into a 64 bit by
        /// using the knowledge of the previous such sequence number.
        pub fn $id(prev_ext_seq: Option<u64>, seq: $t) -> u64 {
            const MAX: u64 = 2_u64.pow($bits); // u16: 65_536;
            const HALF: u64 = MAX / 2; // u16: 32_768
            const BITS: usize = $bits;
            const VAL_MASK: u64 = ((1 << BITS) - 1);
            const ROC_MASK: i64 = (u64::MAX >> BITS) as i64;

            // We define the index of the SRTP packet corresponding to a given
            // ROC and RTP sequence number to be the 48-bit quantity
            //       i = 2^16 * ROC + SEQ.
            //
            // https://tools.ietf.org/html/rfc3711#appendix-A
            //

            // Mask seq to the N bits we are considering.
            // As an example, this allows us to handle the wrap-around of 7 bit
            // values while storing them in u8
            let seq = seq as u64 & VAL_MASK;

            if prev_ext_seq.is_none() {
                // No wrap-around so far.
                return seq;
            }

            let prev_index = prev_ext_seq.unwrap();
            let roc = (prev_index >> BITS) as i64; // how many wrap-arounds.
            let prev_seq = prev_index & (MAX - 1); // u16: 0xffff

            let v = if prev_seq < HALF {
                if seq > HALF + prev_seq {
                    (roc - 1) & ROC_MASK
                } else {
                    roc
                }
            } else if prev_seq > seq + HALF {
                (roc + 1) & ROC_MASK
            } else {
                roc
            };

            if v < 0 {
                return 0;
            }

            (v as u64) * MAX + seq
        }
    };
}

mk_extend!(extend_u8, u8, 8);
mk_extend!(extend_u16, u16, 16);
mk_extend!(extend_u32, u32, 32);

// we 'host' 7 bits in u8 but we ignore the most significant one
mk_extend!(extend_u7, u8, 7);
mk_extend!(extend_u15, u16, 15);

impl Default for RtpHeader {
    fn default() -> Self {
        Self {
            version: 2,
            has_padding: false,
            has_extension: true,
            csrc_count: 0,
            marker: false,
            payload_type: 1.into(),
            sequence_number: 0,
            timestamp: 0,
            ssrc: 0.into(),
            csrc: [0; 15],
            ext_vals: ExtensionValues::default(),
            header_len: 16,
        }
    }
}

#[cfg(test)]
mod test {
    use crate::util::already_happened;
    use crate::{io::DATAGRAM_MAX_PACKET_SIZE, rtp_::Extension};
    use std::time::Duration;

    use super::*;

    #[test]
    fn extend_u16_wrap_around() {
        assert_eq!(extend_u16(None, 0), 0);
        assert_eq!(extend_u16(Some(0), 1), 1);
        assert_eq!(extend_u16(Some(65_535), 0), 65_536);
        assert_eq!(extend_u16(Some(65_500), 2), 65_538);
        assert_eq!(extend_u16(Some(2), 1), 1);
        assert_eq!(extend_u16(Some(65_538), 1), 65_537);
        assert_eq!(extend_u16(Some(3), 3), 3);
        assert_eq!(extend_u16(Some(65_500), 65_500), 65_500);
    }

    #[test]
    fn extend_u16_with_0_prev() {
        // This tests going backwards from previous 0. This should wrap
        // around "backwards" making a ridiculous number.
        let seq = u16::MAX / 2 + 2;
        let expected = u64::MAX - (u16::MAX - seq) as u64;
        assert_eq!(extend_u16(Some(0), seq), expected);
    }

    #[test]
    fn extend_u32_wrap_around() {
        const U32MAX: u64 = u32::MAX as u64 + 1;
        assert_eq!(extend_u32(None, 0), 0);
        assert_eq!(extend_u32(Some(0), 1), 1);
        assert_eq!(extend_u32(Some(U32MAX - 1), 0), U32MAX);
        assert_eq!(extend_u32(Some(U32MAX - 32), 2), U32MAX + 2);
        assert_eq!(extend_u32(Some(2), 1), 1);
        assert_eq!(extend_u32(Some(U32MAX + 2), 1), U32MAX + 1);
        assert_eq!(extend_u32(Some(3), 3), 3);
        assert_eq!(
            extend_u32(Some(U32MAX - 32), (U32MAX - 32) as u32),
            U32MAX - 32
        );
    }

    #[test]
    fn extend_u32_with_0_prev() {
        // This tests going backwards from previous 0. This should wrap
        // around "backwards" making a ridiculous number.
        let seq = u32::MAX / 2 + 2;
        let expected = u64::MAX - (u32::MAX - seq) as u64;
        assert_eq!(extend_u32(Some(0), seq), expected);
    }

    #[test]
    fn extend_u7_wrap_around() {
        let max = 2_u64.pow(7) - 1;
        let half = max / 2;
        assert_eq!(extend_u7(None, 0), 0);
        assert_eq!(extend_u7(None, 1), 1);

        // most significant bit gets ignored
        assert_eq!(extend_u7(None, max as u8), max);
        assert_eq!(extend_u7(None, (max + 1) as u8), 0);

        assert_eq!(extend_u7(Some(0), half as u8), half);
        assert_eq!(extend_u7(Some(0), (half + 1) as u8), half + 1);

        let seq = max / 2 + 2;
        let expected = u64::MAX - (max - seq);
        assert_eq!(extend_u7(Some(0), seq as u8), expected);
    }

    #[test]
    fn extend_u15_wrap_around() {
        let max = 2_u64.pow(15) - 1;
        let half = max / 2;
        assert_eq!(extend_u15(None, 0), 0);
        assert_eq!(extend_u15(None, 1), 1);

        assert_eq!(extend_u15(None, max as u16), max);
        assert_eq!(extend_u15(None, (max + 1) as u16), 0);

        assert_eq!(extend_u15(Some(0), half as u16), half);
        assert_eq!(extend_u15(Some(0), (half + 1) as u16), half + 1);

        let seq = max / 2 + 2;
        let expected = u64::MAX - (max - seq);
        assert_eq!(extend_u15(Some(0), seq as u16), expected);
    }

    #[test]
    fn test_generate_one_length_padding_packet() {
        let mut buf = vec![6; 255];
        RtpHeader::create_padding_packet(&mut buf, 10, 1, 16);

        let mut expected = vec![0; 16];
        expected[15] = 16;
        assert_eq!(&buf[10..26], &expected);
    }

    // version: 2,
    // has_padding: false,
    // has_extension: true,
    // marker: false,
    // payload_type: 1.into(),
    // sequence_number: 0,
    // timestamp: 0,
    // ssrc: 0.into(),
    // ext_vals: ExtensionValues::default(),
    // header_len: 16,

    #[test]
    fn test_write_rtp_headers() {
        fn mk_header(seq: u16, ts: u32, level: i8, marker: bool, exts: &ExtensionMap) -> Vec<u8> {
            let header = RtpHeader {
                payload_type: 33.into(),
                sequence_number: seq,
                timestamp: ts,
                ssrc: 44.into(),
                marker,
                ext_vals: ExtensionValues {
                    audio_level: Some(level),
                    voice_activity: Some(false),
                    ..Default::default()
                },
                ..Default::default()
            };
            let mut buf = vec![0; DATAGRAM_MAX_PACKET_SIZE];
            let n = header.write_to(&mut buf[..], exts);
            buf.truncate(n);

            buf
        }

        let mut exts = ExtensionMap::empty();
        // The expected bytes below carry the level with the V bit CLEAR, which
        // is what `voice_activity: Some(false)` above asks for. They used to
        // have bit 7 set (170 = 0xAA rather than 42 = 0x2A) because the
        // serializer let the sign of the negative level spill into that bit.
        exts.set(3, Extension::AudioLevel);

        let buf1 = mk_header(47_000, 10_000, -42, false, &exts);
        let buf2 = mk_header(47_001, 12_000, -43, true, &exts);
        let buf3 = mk_header(47_002, 14_000, -44, false, &exts);

        let p1 = &[
            144, 33, 183, 152, 0, 0, 39, 16, 0, 0, 0, 44, 0xBE, 0xDE, 0, 1, 48, 42, 0, 0,
        ];
        let p2 = &[
            144, 161, 183, 153, 0, 0, 46, 224, 0, 0, 0, 44, 0xBE, 0xDE, 0, 1, 48, 43, 0, 0,
        ];
        let p3 = &[
            144, 33, 183, 154, 0, 0, 54, 176, 0, 0, 0, 44, 0xBE, 0xDE, 0, 1, 48, 44, 0, 0,
        ];

        assert_eq!(&buf1, p1);
        assert_eq!(&buf2, p2);
        assert_eq!(&buf3, p3);
    }

    #[test]
    fn test_write_rtp_headers_two_byte_form() {
        fn mk_header(seq: u16, ts: u32, level: i8, marker: bool, exts: &ExtensionMap) -> Vec<u8> {
            let header = RtpHeader {
                payload_type: 33.into(),
                sequence_number: seq,
                timestamp: ts,
                ssrc: 44.into(),
                marker,
                ext_vals: ExtensionValues {
                    audio_level: Some(level),
                    voice_activity: Some(false),
                    ..Default::default()
                },
                ..Default::default()
            };
            let mut buf = vec![0; DATAGRAM_MAX_PACKET_SIZE];
            let n = header.write_to(&mut buf[..], exts);
            buf.truncate(n);

            buf
        }

        let mut exts = ExtensionMap::empty();
        // Same correction as above: the V bit is clear because the header
        // asks for `voice_activity: Some(false)`.
        // An ID larger than 14 forces the 2-byte header extension form
        exts.set(15, Extension::AudioLevel);

        let buf1 = mk_header(47_000, 10_000, -42, false, &exts);
        let buf2 = mk_header(47_001, 12_000, -43, true, &exts);
        let buf3 = mk_header(47_002, 14_000, -44, false, &exts);

        let p1 = &[
            144, 33, 183, 152, 0, 0, 39, 16, 0, 0, 0, 44, 0x10, 0x00, 0, 1, 15, 1, 42, 0,
        ];
        let p2 = &[
            144, 161, 183, 153, 0, 0, 46, 224, 0, 0, 0, 44, 0x10, 0x00, 0, 1, 15, 1, 43, 0,
        ];
        let p3 = &[
            144, 33, 183, 154, 0, 0, 54, 176, 0, 0, 0, 44, 0x10, 0x00, 0, 1, 15, 1, 44, 0,
        ];

        assert_eq!(&buf1, p1);
        assert_eq!(&buf2, p2);
        assert_eq!(&buf3, p3);
    }

    #[test]
    fn test_parse_rtp_headers() {
        let exts = ExtensionMap::standard();

        let hb1 = [
            176, 111, 183, 152, 0, 0, 39, 16, 46, 87, 21, 249, 0xBE, 0xDE, 0, 4, 16, 170, 34, 254,
            32, 106, 49, 0, 0, 66, 120, 89, 106, 0, 0, 0, 1, 2, 3, 4, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 12,
        ];

        let hb2 = [
            176, 111, 183, 153, 0, 0, 46, 224, 46, 87, 21, 249, 0xBE, 0xDE, 0, 4, 16, 171, 34, 254,
            134, 208, 49, 0, 2, 66, 120, 89, 106, 0, 0, 0, 5, 6, 7, 8, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 12,
        ];
        let hb3 = [
            176, 111, 183, 154, 0, 0, 54, 176, 46, 87, 21, 249, 0xBE, 0xDE, 0, 4, 16, 172, 34, 254,
            32, 106, 49, 0, 1, 66, 120, 89, 106, 0, 0, 0, 9, 10, 11, 12, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 12,
        ];

        let h1 = RtpHeader::parse(&hb1, &exts).unwrap();
        let abs1 = already_happened() + Duration::from_micros(63531654);

        assert_eq!(
            h1,
            RtpHeader {
                version: 2,
                has_padding: true,
                has_extension: true,
                csrc_count: 0,
                marker: false,
                payload_type: 111.into(),
                sequence_number: 47000,
                timestamp: 10000,
                ssrc: 777459193.into(),
                csrc: [0; 15],
                ext_vals: ExtensionValues {
                    mid: Some("xYj".into()),
                    abs_send_time: Some(abs1),
                    voice_activity: Some(true),
                    audio_level: Some(-42),
                    transport_cc: Some(0),
                    ..Default::default()
                },
                header_len: 32
            }
        );

        let h2 = RtpHeader::parse(&hb2, &exts).unwrap();
        let abs2 = already_happened() + Duration::from_micros(63631652);

        assert_eq!(
            h2,
            RtpHeader {
                version: 2,
                has_padding: true,
                has_extension: true,
                csrc_count: 0,
                marker: false,
                payload_type: 111.into(),
                sequence_number: 47001,
                timestamp: 12000,
                ssrc: 777459193.into(),
                csrc: [0; 15],
                ext_vals: ExtensionValues {
                    mid: Some("xYj".into()),
                    abs_send_time: Some(abs2),
                    voice_activity: Some(true),
                    audio_level: Some(-43),
                    transport_cc: Some(2),
                    ..Default::default()
                },
                header_len: 32
            }
        );

        let h3 = RtpHeader::parse(&hb3, &exts).unwrap();
        let abs3 = already_happened() + Duration::from_micros(63531654);

        assert_eq!(
            h3,
            RtpHeader {
                version: 2,
                has_padding: true,
                has_extension: true,
                csrc_count: 0,
                marker: false,
                payload_type: 111.into(),
                sequence_number: 47002,
                timestamp: 14000,
                ssrc: 777459193.into(),
                csrc: [0; 15],
                ext_vals: ExtensionValues {
                    mid: Some("xYj".into()),
                    abs_send_time: Some(abs3),
                    voice_activity: Some(true),
                    audio_level: Some(-44),
                    transport_cc: Some(1),
                    ..Default::default()
                },
                header_len: 32
            }
        );
    }

    #[test]
    fn test_parse_rtp_headers_two_byte_form() {
        let exts = ExtensionMap::standard();

        let hb1 = [
            176, 111, 183, 152, 0, 0, 39, 16, 46, 87, 21, 249, 0x10, 0x01, 0, 5, 1, 1, 170, 2, 3,
            254, 32, 106, 3, 2, 0, 0, 4, 3, 120, 89, 106, 0, 0, 0, 1, 2, 3, 4, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 12,
        ];

        let hb2 = [
            176, 111, 183, 153, 0, 0, 46, 224, 46, 87, 21, 249, 0x10, 0x02, 0, 5, 1, 1, 171, 2, 3,
            254, 134, 208, 3, 2, 0, 2, 4, 3, 120, 89, 106, 0, 0, 0, 5, 6, 7, 8, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 12,
        ];
        let hb3 = [
            176, 111, 183, 154, 0, 0, 54, 176, 46, 87, 21, 249, 0x10, 0x03, 0, 5, 1, 1, 172, 2, 3,
            254, 32, 106, 3, 2, 0, 1, 4, 3, 120, 89, 106, 0, 0, 0, 9, 10, 11, 12, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 12,
        ];

        let h1 = RtpHeader::parse(&hb1, &exts).unwrap();
        let abs1 = already_happened() + Duration::from_micros(63531654);

        assert_eq!(
            h1,
            RtpHeader {
                version: 2,
                has_padding: true,
                has_extension: true,
                csrc_count: 0,
                marker: false,
                payload_type: 111.into(),
                sequence_number: 47000,
                timestamp: 10000,
                ssrc: 777459193.into(),
                csrc: [0; 15],
                ext_vals: ExtensionValues {
                    mid: Some("xYj".into()),
                    abs_send_time: Some(abs1),
                    voice_activity: Some(true),
                    audio_level: Some(-42),
                    transport_cc: Some(0),
                    ..Default::default()
                },
                header_len: 36
            }
        );

        let h2 = RtpHeader::parse(&hb2, &exts).unwrap();
        let abs2 = already_happened() + Duration::from_micros(63631652);

        assert_eq!(
            h2,
            RtpHeader {
                version: 2,
                has_padding: true,
                has_extension: true,
                csrc_count: 0,
                marker: false,
                payload_type: 111.into(),
                sequence_number: 47001,
                timestamp: 12000,
                ssrc: 777459193.into(),
                csrc: [0; 15],
                ext_vals: ExtensionValues {
                    mid: Some("xYj".into()),
                    abs_send_time: Some(abs2),
                    voice_activity: Some(true),
                    audio_level: Some(-43),
                    transport_cc: Some(2),
                    ..Default::default()
                },
                header_len: 36
            }
        );

        let h3 = RtpHeader::parse(&hb3, &exts).unwrap();
        let abs3 = already_happened() + Duration::from_micros(63531654);

        assert_eq!(
            h3,
            RtpHeader {
                version: 2,
                has_padding: true,
                has_extension: true,
                csrc_count: 0,
                marker: false,
                payload_type: 111.into(),
                sequence_number: 47002,
                timestamp: 14000,
                ssrc: 777459193.into(),
                csrc: [0; 15],
                ext_vals: ExtensionValues {
                    mid: Some("xYj".into()),
                    abs_send_time: Some(abs3),
                    voice_activity: Some(true),
                    audio_level: Some(-44),
                    transport_cc: Some(1),
                    ..Default::default()
                },
                header_len: 36
            }
        );
    }

    #[test]
    fn test_write_rtp_headers_with_csrc() {
        fn mk_header(csrc_count: usize, csrc: [u32; 15], exts: &ExtensionMap) -> Vec<u8> {
            let header = RtpHeader {
                csrc_count,
                payload_type: 33.into(),
                sequence_number: 47_000,
                timestamp: 10_000,
                ssrc: 44.into(),
                csrc,
                ext_vals: ExtensionValues {
                    audio_level: Some(-42),
                    voice_activity: Some(true),
                    ..Default::default()
                },
                ..Default::default()
            };
            let mut buf = vec![0; DATAGRAM_MAX_PACKET_SIZE];
            let n = header.write_to(&mut buf[..], exts);
            buf.truncate(n);
            buf
        }

        let mut exts = ExtensionMap::empty();
        exts.set(3, Extension::AudioLevel);

        // 3 CSRC entries, one-byte extension form
        let buf1 = mk_header(
            3,
            [
                100,
                0x12_34_56_78,
                0xFFFF_FFFF,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
            ],
            &exts,
        );
        let p1 = &[
            147, 33, 183, 152, 0, 0, 39, 16, 0, 0, 0, 44, // fixed header (CC=3)
            0, 0, 0, 100, 0x12, 0x34, 0x56, 0x78, 0xFF, 0xFF, 0xFF, 0xFF, // CSRC values
            0xBE, 0xDE, 0, 1, 48, 170, 0, 0, // extension
        ];
        assert_eq!(&buf1, p1);

        // 15 CSRC entries (maximum)
        let buf2 = mk_header(
            15,
            [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15],
            &exts,
        );
        let p2 = &[
            159, 33, 183, 152, 0, 0, 39, 16, 0, 0, 0, 44, // fixed header (CC=15)
            0, 0, 0, 1, 0, 0, 0, 2, 0, 0, 0, 3, 0, 0, 0, 4, // CSRC 1-4
            0, 0, 0, 5, 0, 0, 0, 6, 0, 0, 0, 7, 0, 0, 0, 8, // CSRC 5-8
            0, 0, 0, 9, 0, 0, 0, 10, 0, 0, 0, 11, 0, 0, 0, 12, // CSRC 9-12
            0, 0, 0, 13, 0, 0, 0, 14, 0, 0, 0, 15, // CSRC 13-15
            0xBE, 0xDE, 0, 1, 48, 170, 0, 0, // extension
        ];
        assert_eq!(&buf2, p2);
    }

    #[test]
    fn test_parse_rtp_headers_with_csrc() {
        let mut exts = ExtensionMap::empty();
        exts.set(3, Extension::AudioLevel);

        // 3 CSRC entries with one-byte extension form (produced by test_write_rtp_headers_with_csrc)
        let buf1 = [
            147, 33, 183, 152, 0, 0, 39, 16, 0, 0, 0, 44, 0, 0, 0, 100, 0x12, 0x34, 0x56, 0x78,
            0xFF, 0xFF, 0xFF, 0xFF, 0xBE, 0xDE, 0, 1, 48, 170, 0, 0,
        ];

        let h1 = RtpHeader::parse(&buf1, &exts).unwrap();

        assert_eq!(
            h1,
            RtpHeader {
                version: 2,
                has_padding: false,
                has_extension: true,
                csrc_count: 3,
                marker: false,
                payload_type: 33.into(),
                sequence_number: 47_000,
                timestamp: 10_000,
                ssrc: 44.into(),
                csrc: [
                    100,
                    0x12_34_56_78,
                    0xFFFF_FFFF,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0
                ],
                ext_vals: ExtensionValues {
                    audio_level: Some(-42),
                    voice_activity: Some(true),
                    ..Default::default()
                },
                header_len: 32,
            }
        );

        // 15 CSRC entries (maximum)
        let buf2 = [
            159, 33, 183, 152, 0, 0, 39, 16, 0, 0, 0, 44, // fixed header (CC=15)
            0, 0, 0, 1, 0, 0, 0, 2, 0, 0, 0, 3, 0, 0, 0, 4, // CSRC 1-4
            0, 0, 0, 5, 0, 0, 0, 6, 0, 0, 0, 7, 0, 0, 0, 8, // CSRC 5-8
            0, 0, 0, 9, 0, 0, 0, 10, 0, 0, 0, 11, 0, 0, 0, 12, // CSRC 9-12
            0, 0, 0, 13, 0, 0, 0, 14, 0, 0, 0, 15, // CSRC 13-15
            0xBE, 0xDE, 0, 1, 48, 170, 0, 0, // extension
        ];

        let h2 = RtpHeader::parse(&buf2, &exts).unwrap();

        assert_eq!(
            h2,
            RtpHeader {
                version: 2,
                has_padding: false,
                has_extension: true,
                csrc_count: 15,
                marker: false,
                payload_type: 33.into(),
                sequence_number: 47_000,
                timestamp: 10_000,
                ssrc: 44.into(),
                csrc: [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15],
                ext_vals: ExtensionValues {
                    audio_level: Some(-42),
                    voice_activity: Some(true),
                    ..Default::default()
                },
                header_len: 80,
            }
        );
    }

    #[test]
    fn truncate_off_srtp_padding() {
        let truncate = |payload: Vec<u8>| -> Result<Vec<u8>, ()> {
            match RtpHeader::unpad_payload(&payload) {
                Some(unpadded) => Ok(unpadded.to_vec()),
                None => Err(()),
            }
        };

        // A zero pad count is invalid (Pion errInvalidRTPPadding), not media.
        assert_eq!(Err(()), truncate(vec![1, 2, 3, 4, 0]));
        assert_eq!(Ok(vec![1, 2, 3, 4]), truncate(vec![1, 2, 3, 4, 1]));
        assert_eq!(Ok(vec![1, 2, 3]), truncate(vec![1, 2, 3, 4, 2]));
        assert_eq!(Ok(vec![1, 2]), truncate(vec![1, 2, 3, 4, 3]));
        assert_eq!(Ok(vec![1]), truncate(vec![1, 2, 3, 4, 4]));
        assert_eq!(Ok(vec![]), truncate(vec![1, 2, 3, 4, 5]));
        assert_eq!(Err(()), truncate(vec![1, 2, 3, 4, 6]));
        assert_eq!(Err(()), truncate(vec![1, 2, 3, 4, 255]));
        assert_eq!(Err(()), truncate(vec![0]));
        assert_eq!(Ok(vec![]), truncate(vec![1]));
        // A set padding bit with no trailing byte is invalid (Pion errTooSmall).
        assert_eq!(Err(()), truncate(vec![]));
    }

    /// Frozen `tests/oracles/pion-rtp-padding/cases.jsonl` decision rows. Every
    /// field is what the corresponding fixed Go parser reports, so a drift in
    /// either parser fails this table before any paired run is needed.
    #[derive(Clone, Copy)]
    struct Admission {
        case: &'static str,
        bytes_hex: &'static str,
        accepted: bool,
        payload_hex: &'static str,
        padding_only: bool,
        padding_bit: bool,
        padding_size: usize,
    }

    const FROZEN_PADDING_ADMISSION: [Admission; 13] = [
        Admission {
            case: "pad_no_tail_byte",
            bytes_hex: "a06003e8000186a000067532",
            accepted: false,
            payload_hex: "",
            padding_only: false,
            padding_bit: true,
            padding_size: 0,
        },
        Admission {
            case: "pad_zero_count",
            bytes_hex: "a06003e8000186a0000675321001000000",
            accepted: false,
            payload_hex: "",
            padding_only: false,
            padding_bit: true,
            padding_size: 0,
        },
        Admission {
            case: "pad_count_exceeds_data",
            bytes_hex: "a06003e8000186a00006753203",
            accepted: false,
            payload_hex: "",
            padding_only: false,
            padding_bit: true,
            padding_size: 3,
        },
        Admission {
            case: "pad_count_equals_data",
            bytes_hex: "a06003e8000186a0000675320202",
            accepted: true,
            payload_hex: "",
            padding_only: true,
            padding_bit: true,
            padding_size: 2,
        },
        Admission {
            case: "pad_only_single_byte",
            bytes_hex: "a06003e8000186a00006753201",
            accepted: true,
            payload_hex: "",
            padding_only: true,
            padding_bit: true,
            padding_size: 1,
        },
        Admission {
            case: "pad_full_255",
            bytes_hex: concat!(
                "a06003e8000186a000067532",
                "0000000000000000000000000000000000000000",
                "0000000000000000000000000000000000000000",
                "0000000000000000000000000000000000000000",
                "0000000000000000000000000000000000000000",
                "0000000000000000000000000000000000000000",
                "0000000000000000000000000000000000000000",
                "0000000000000000000000000000000000000000",
                "0000000000000000000000000000000000000000",
                "0000000000000000000000000000000000000000",
                "0000000000000000000000000000000000000000",
                "0000000000000000000000000000000000000000",
                "0000000000000000000000000000000000000000",
                "0000000000000000000000000000ff"
            ),
            accepted: true,
            payload_hex: "",
            padding_only: true,
            padding_bit: true,
            padding_size: 255,
        },
        Admission {
            case: "pad_blank_before_count",
            bytes_hex: "a06003e8000186a000067532000002",
            accepted: true,
            payload_hex: "00",
            padding_only: false,
            padding_bit: true,
            padding_size: 2,
        },
        Admission {
            case: "pad_unaligned_block",
            bytes_hex: "a06003e8000186a00006753210010003",
            accepted: true,
            payload_hex: "10",
            padding_only: false,
            padding_bit: true,
            padding_size: 3,
        },
        Admission {
            case: "payload_with_padding",
            bytes_hex: "a06003e8000186a0000675321001000001",
            accepted: true,
            payload_hex: "10010000",
            padding_only: false,
            padding_bit: true,
            padding_size: 1,
        },
        Admission {
            case: "no_padding_empty_payload",
            bytes_hex: "806003e8000186a000067532",
            accepted: true,
            payload_hex: "",
            padding_only: false,
            padding_bit: false,
            padding_size: 0,
        },
        Admission {
            case: "no_padding_real_payload",
            bytes_hex: "806003e8000186a0000675321001000000",
            accepted: true,
            payload_hex: "1001000000",
            padding_only: false,
            padding_bit: false,
            padding_size: 0,
        },
        Admission {
            case: "truncated_header",
            bytes_hex: "a06003e8000186",
            accepted: false,
            payload_hex: "",
            padding_only: false,
            padding_bit: true,
            padding_size: 0,
        },
        Admission {
            case: "header_below_four_bytes",
            bytes_hex: "a0",
            accepted: false,
            payload_hex: "",
            padding_only: false,
            padding_bit: false,
            padding_size: 0,
        },
    ];

    fn hex_bytes(text: &str) -> Vec<u8> {
        assert!(text.len() % 2 == 0, "hex vector has odd length");
        (0..text.len() / 2)
            .map(|i| u8::from_str_radix(&text[2 * i..2 * i + 2], 16).expect("hex byte"))
            .collect()
    }

    fn hex_string(bytes: &[u8]) -> String {
        bytes.iter().map(|b| format!("{b:02x}")).collect()
    }

    /// Everything one parser reports for a vector: the admission decision plus
    /// the padding state the receiver keeps afterwards. `padding_bit` and
    /// `padding_size` are Pion struct state; Pion leaves them partially
    /// populated even for a packet it rejects, which is why the table freezes
    /// them separately from the verdict.
    #[derive(Clone, PartialEq, Eq, Debug)]
    struct Observation {
        accepted: bool,
        payload_hex: String,
        padding_only: bool,
        padding_bit: bool,
        padding_size: usize,
    }

    impl From<&Admission> for Observation {
        fn from(row: &Admission) -> Observation {
            Observation {
                accepted: row.accepted,
                payload_hex: row.payload_hex.to_string(),
                padding_only: row.padding_only,
                padding_bit: row.padding_bit,
                padding_size: row.padding_size,
            }
        }
    }

    /// The verdict str0m can actually reproduce: survive or not, which payload
    /// bytes reach the receive path, and whether the packet is padding-only.
    #[derive(Clone, PartialEq, Eq, Debug)]
    struct Decision {
        accepted: bool,
        payload_hex: String,
        padding_only: bool,
    }

    fn decision_of(observation: &Observation) -> Decision {
        Decision {
            accepted: observation.accepted,
            payload_hex: observation.payload_hex.clone(),
            padding_only: observation.padding_only,
        }
    }

    /// Native verdict, computed the way `Session::handle_rtp_packet` does:
    /// header parse, then unpadding only when the padding bit is set.
    fn native_decision(bytes: &[u8]) -> Decision {
        let reject = || Decision {
            accepted: false,
            payload_hex: String::new(),
            padding_only: false,
        };
        let exts = ExtensionMap::empty();
        let Some(header) = RtpHeader::parse(bytes, &exts) else {
            return reject();
        };
        let rest = &bytes[header.header_len..];
        let payload = if header.has_padding {
            match RtpHeader::unpad_payload(rest) {
                Some(unpadded) => unpadded,
                None => return reject(),
            }
        } else {
            rest
        };
        Decision {
            accepted: true,
            payload_hex: hex_string(payload),
            padding_only: header.has_padding && payload.is_empty(),
        }
    }

    fn observation_of(row: &serde_json::Value) -> Observation {
        Observation {
            accepted: row["accepted"].as_bool().expect("accepted is a Go bool"),
            payload_hex: row["payload_hex"]
                .as_str()
                .expect("payload_hex is a Go string")
                .to_string(),
            padding_only: row["padding_only"]
                .as_bool()
                .expect("padding_only is a Go bool"),
            padding_bit: row["padding_bit"]
                .as_bool()
                .expect("padding_bit is a Go bool"),
            padding_size: usize::try_from(
                row["padding_size"]
                    .as_u64()
                    .expect("padding_size is a Go int"),
            )
            .expect("padding_size fits usize"),
        }
    }

    /// One formatter for every comparison so no field can be dropped silently.
    fn check_drift(case: &str, fields: Vec<(&str, String, String)>) -> Result<(), String> {
        let drift = fields
            .into_iter()
            .filter(|(_, expected, actual)| expected != actual)
            .map(|(field, expected, actual)| {
                format!("{field}: frozen {expected} != actual {actual}")
            })
            .collect::<Vec<_>>();
        if drift.is_empty() {
            Ok(())
        } else {
            Err(format!("{case}: {}", drift.join("; ")))
        }
    }

    fn compare_observation(
        case: &str,
        frozen: &Observation,
        actual: &Observation,
    ) -> Result<(), String> {
        check_drift(
            case,
            vec![
                (
                    "accepted",
                    frozen.accepted.to_string(),
                    actual.accepted.to_string(),
                ),
                (
                    "payload_hex",
                    frozen.payload_hex.clone(),
                    actual.payload_hex.clone(),
                ),
                (
                    "padding_only",
                    frozen.padding_only.to_string(),
                    actual.padding_only.to_string(),
                ),
                (
                    "padding_bit",
                    frozen.padding_bit.to_string(),
                    actual.padding_bit.to_string(),
                ),
                (
                    "padding_size",
                    frozen.padding_size.to_string(),
                    actual.padding_size.to_string(),
                ),
            ],
        )
    }

    fn compare_native(
        case: &str,
        reference: &Observation,
        native: &Decision,
    ) -> Result<(), String> {
        let expected = decision_of(reference);
        check_drift(
            case,
            vec![
                (
                    "accepted",
                    expected.accepted.to_string(),
                    native.accepted.to_string(),
                ),
                (
                    "payload_hex",
                    expected.payload_hex.clone(),
                    native.payload_hex.clone(),
                ),
                (
                    "padding_only",
                    expected.padding_only.to_string(),
                    native.padding_only.to_string(),
                ),
            ],
        )
    }

    fn frozen_vector(case: &str) -> serde_json::Value {
        include_str!("../../../../tests/oracles/pion-rtp-padding/cases.jsonl")
            .lines()
            .filter(|line| !line.trim().is_empty())
            .map(|line| -> serde_json::Value {
                serde_json::from_str(line).expect("padding vector")
            })
            .find(|vector| vector["case"] == serde_json::json!(case))
            .unwrap_or_else(|| panic!("committed padding vectors are missing {case}"))
    }

    /// The Go `error_text` must agree with the acceptance it accompanies,
    /// otherwise a row could report a rejection with an empty reason and match.
    fn error_agrees_with_acceptance(row: &serde_json::Value) -> Result<(), String> {
        let accepted = row["accepted"].as_bool().expect("accepted is a Go bool");
        let error_text = row["error_text"]
            .as_str()
            .expect("error_text is a Go string");
        if error_text.is_empty() == accepted {
            Ok(())
        } else {
            Err(format!(
                "error_text {error_text:?} disagrees with accepted {accepted}"
            ))
        }
    }

    #[test]
    fn frozen_rtp_padding_admission_table_matches_native_parser() {
        for row in FROZEN_PADDING_ADMISSION.iter() {
            let vector = frozen_vector(row.case);
            assert_eq!(
                vector["bytes_hex"],
                serde_json::json!(row.bytes_hex),
                "{}: table hex drifted from the committed vector",
                row.case
            );
            let bytes = hex_bytes(row.bytes_hex);
            assert_eq!(
                hex_string(&bytes),
                row.bytes_hex,
                "{}: committed hex vector is not canonical",
                row.case
            );
            let frozen = Observation::from(row);
            assert_eq!(
                Ok(()),
                compare_observation(row.case, &frozen, &observation_of(&vector["expected"])),
                "{}: table drifted from the committed vector",
                row.case
            );
            assert_eq!(
                Ok(()),
                compare_native(row.case, &frozen, &native_decision(&bytes)),
                "{}: native parser disagrees with the frozen decision",
                row.case
            );
        }

        // Every compared field must be able to fail, otherwise the table is a
        // tautology instead of a check.
        let row = FROZEN_PADDING_ADMISSION
            .iter()
            .find(|r| r.case == "pad_zero_count")
            .expect("pad_zero_count row");
        let frozen = Observation::from(row);
        for (field, value) in [
            ("accepted", serde_json::json!(true)),
            ("payload_hex", serde_json::json!("1001000000")),
            ("padding_only", serde_json::json!(true)),
            ("padding_bit", serde_json::json!(false)),
            ("padding_size", serde_json::json!(7)),
        ] {
            let mut vector = frozen_vector(row.case);
            vector["expected"][field] = value;
            assert!(
                compare_observation(row.case, &frozen, &observation_of(&vector["expected"]))
                    .is_err(),
                "vector mutation {}.{field} was not detected",
                row.case
            );
        }
        assert!(
            compare_native(
                row.case,
                &frozen,
                &Decision {
                    accepted: true,
                    payload_hex: String::new(),
                    padding_only: true,
                }
            )
            .is_err(),
            "a native parser that accepted a rejected packet was not detected"
        );
    }

    #[test]
    #[ignore = "requires actual fixed-Pion rtp.Packet.Unmarshal observer rows"]
    fn original_go_rtp_padding_admission_rows_match_native_parser() {
        let read_rows = |path: String, prefix: &str| -> Vec<serde_json::Value> {
            std::fs::read_to_string(path)
                .expect("observer file")
                .lines()
                .filter(|line| !line.trim().is_empty())
                .filter_map(|line| line.strip_prefix(prefix))
                .map(|line| -> serde_json::Value {
                    serde_json::from_str(line).expect("observer row")
                })
                .collect()
        };
        let vectors = read_rows(
            std::env::var("PION_RTP_PADDING_GO_CASES").expect("fixed-Pion observer cases"),
            "",
        );
        let rows = read_rows(
            std::env::var("PION_RTP_PADDING_GO_RESULTS").expect("fixed-Pion observer results"),
            "SOURCE_PION_RTP_PADDING ",
        );
        assert_eq!(
            vectors.len(),
            FROZEN_PADDING_ADMISSION.len(),
            "one committed vector per frozen row"
        );
        assert_eq!(
            rows.len(),
            FROZEN_PADDING_ADMISSION.len(),
            "one actual observer row per vector"
        );

        let mut seen = std::collections::HashSet::new();
        for row in &rows {
            let case = row["case"].as_str().expect("actual row case");
            assert!(seen.insert(case), "repeated actual row: {case}");
            let table = FROZEN_PADDING_ADMISSION
                .iter()
                .find(|r| r.case == case)
                .unwrap_or_else(|| panic!("unexpected actual row: {case}"));
            let vector = vectors
                .iter()
                .find(|v| v["case"] == serde_json::json!(case))
                .unwrap_or_else(|| panic!("observer input is missing {case}"));
            assert_eq!(
                row["bytes_hex"],
                serde_json::json!(table.bytes_hex),
                "{case}: observer input drifted from the frozen table"
            );
            assert_eq!(
                row["bytes_hex"], vector["bytes_hex"],
                "{case}: actual row does not match the vector it was given"
            );
            assert_eq!(
                Ok(()),
                error_agrees_with_acceptance(row),
                "{case}: actual Pion rejection reason"
            );
            let actual = observation_of(row);
            assert_eq!(
                Ok(()),
                compare_observation(case, &Observation::from(table), &actual),
                "{case}: actual fixed-Pion verdict drifted from the frozen table"
            );
            assert_eq!(
                Ok(()),
                compare_native(case, &actual, &native_decision(&hex_bytes(table.bytes_hex))),
                "{case}: native parser drifted from the actual fixed-Pion verdict"
            );
        }

        // Differential-level falsifiability: the Go↔table↔native comparison must
        // reject a Pion row that only looks equivalent.
        for (case, field, value) in [
            ("pad_zero_count", "accepted", serde_json::json!(true)),
            ("pad_zero_count", "padding_size", serde_json::json!(1)),
            ("payload_with_padding", "payload_hex", serde_json::json!("")),
            ("payload_with_padding", "padding_size", serde_json::json!(0)),
        ] {
            let table = FROZEN_PADDING_ADMISSION
                .iter()
                .find(|r| r.case == case)
                .expect("mutation row");
            let row = rows
                .iter()
                .find(|r| r["case"] == serde_json::json!(case))
                .expect("mutation actual row");
            let mut broken = row.clone();
            broken[field] = value;
            assert!(
                compare_observation(case, &Observation::from(table), &observation_of(&broken))
                    .is_err(),
                "actual-row mutation {case}.{field} was not detected"
            );
        }
    }
}
