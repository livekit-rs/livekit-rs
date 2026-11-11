// SPDX-FileCopyrightText: 2026 The Pion community <https://pion.ly>
// SPDX-License-Identifier: MIT
//! Transactional RTCP admission from Pion rtcp v1.2.19 packet.go and its typed
//! Unmarshal implementations. The interceptor parses the entire datagram before
//! WriteRTCP sees any packet. Unknown packet types retain RawPacket admission;
//! this is deliberately not a stricter generic RFC padding validator.
use super::{FeedbackError, Rfc8888Report, TwccReport, parse_rfc8888, parse_twcc};

/// Feedback selected only after every packet in the datagram has parsed.
#[derive(Clone, Debug)]
pub enum CompoundFeedback {
    Twcc(TwccReport),
    Rfc8888(Rfc8888Report),
}
/// Packet count includes valid non-feedback and unknown RawPacket packets.
#[derive(Clone, Debug)]
pub struct CompoundReport {
    pub packet_count: usize,
    pub feedback: Vec<CompoundFeedback>,
}
fn error(message: &'static str) -> FeedbackError {
    FeedbackError::Wire(message)
}
fn short() -> FeedbackError {
    error("rtcp: packet too short")
}
fn require_size(raw: &[u8], minimum: usize) -> Result<(), FeedbackError> {
    if raw.len() < minimum {
        Err(short())
    } else {
        Ok(())
    }
}
fn sdes(raw: &[u8], count: usize) -> Result<(), FeedbackError> {
    let mut at = 4;
    let mut chunks = 0;
    while at < raw.len() {
        require_size(&raw[at..], 5)?;
        let start = at;
        at += 4;
        loop {
            if at == raw.len() {
                return Err(short());
            }
            if raw[at] == 0 {
                at += 1;
                break;
            }
            require_size(&raw[at..], 2)?;
            let next = at + 2 + usize::from(raw[at + 1]);
            if next > raw.len() {
                return Err(short());
            }
            at = next;
        }
        // Source Chunk.Unmarshal returns at End, and Chunk.len rounds up the
        // decoded items, without inspecting the padding bytes' contents.
        at = start + (at - start).next_multiple_of(4);
        chunks += 1;
    }
    if chunks != count {
        return Err(error("rtcp: invalid header"));
    }
    Ok(())
}
fn xr(raw: &[u8]) -> Result<(), FeedbackError> {
    let wrong = || error("rtcp: wrong marshal size");
    if raw.len() < 8 {
        return Err(wrong());
    }
    let mut at = 8;
    while at < raw.len() {
        if raw.len() - at < 4 {
            return Err(wrong());
        }
        // Source packetBuffer.split clamps an overlong block at packet end.
        let declared = (usize::from(u16::from_be_bytes([raw[at + 2], raw[at + 3]])) + 1) * 4;
        let size = declared.min(raw.len() - at);
        let (minimum, stride) = match raw[at] {
            1 | 2 => (12, 2),
            3 => (12, 4),
            4 => (12, 0),
            5 => (4, 12),
            6 => (40, 0),
            7 => (36, 0),
            _ => (4, 1),
        };
        if size < minimum || (stride != 0 && (size - minimum) % stride != 0) {
            return Err(wrong());
        }
        at += size;
    }
    Ok(())
}
fn validate_other(raw: &[u8]) -> Result<(), FeedbackError> {
    let count = usize::from(raw[0] & 31);
    match raw[1] {
        200 => require_size(raw, 28 + 24 * count)?,
        201 => {
            require_size(raw, 8)?;
            let mut at = 8;
            let mut reports = 0;
            while at < raw.len() && reports < count {
                require_size(&raw[at..], 24)?;
                at += 24;
                reports += 1;
            }
            if reports != count {
                return Err(error("rtcp: invalid header"));
            }
        }
        202 => sdes(raw, count)?,
        203 => {
            let reason = 4 + count * 4;
            require_size(raw, reason)?;
            if reason < raw.len() {
                require_size(raw, reason + 1 + usize::from(raw[reason]))?;
            }
        }
        204 => {
            require_size(raw, 12)?;
            if raw[0] & 32 != 0 && usize::from(raw[raw.len() - 1]) > raw.len() - 12 {
                return Err(error("rtcp: invalid padding value"));
            }
        }
        205 if count == 1 => {
            require_size(raw, 8)?;
            if raw.len() - 4 <= 8 {
                return Err(error("rtcp: invalid packet length"));
            }
        }
        205 if count == 5 => require_size(raw, 12)?,
        206 if count == 1 || count == 2 => require_size(raw, 12)?,
        206 if count == 4 => {
            require_size(raw, 8)?;
            if raw.len() - 4 <= 8 || !(raw.len() - 4).is_multiple_of(8) {
                return Err(error("rtcp: invalid packet length"));
            }
        }
        206 if count == 15 => {
            require_size(raw, 20)?;
            if raw[0] & 32 != 0 {
                return Err(error("rtcp: invalid padding value expected(0) actual(1)"));
            }
            // Source REMB performs this expression in uint16, unlike the
            // compound dispatcher's int length. Preserve its wrap boundary.
            let size = u16::from_be_bytes([raw[2], raw[3]])
                .wrapping_add(1)
                .wrapping_mul(4) as usize;
            if size < 20 {
                return Err(error("rtcp: header length is too small"));
            }
            if raw[8..12] != [0; 4] {
                return Err(error("rtcp: media SSRC must be 0"));
            }
            if &raw[12..16] != b"REMB" {
                return Err(error("missing REMB identifier"));
            }
            if size != 20 + 4 * usize::from(raw[16]) {
                return Err(error("SSRC num and length do not match"));
            }
        }
        207 => xr(raw)?,
        // RawPacket validates only the header, already checked by dispatcher.
        _ => {}
    }
    Ok(())
}
/// Whole-datagram predicate for an authenticated native RTCP boundary. Used
/// by LiveKit assembly in every mode/direction, not only Pion estimator mode.
pub fn admit_compound(raw: &[u8]) -> bool {
    parse_compound(raw).is_ok()
}

/// Parse the whole datagram before returning selected feedback. No adapter or
/// estimator state is touched on any malformed later packet. The input must be
/// an authenticated, decrypted RTCP datagram, not concatenated SRTP packets.
pub fn parse_compound(raw: &[u8]) -> Result<CompoundReport, FeedbackError> {
    let mut at = 0;
    let mut packet_count = 0;
    let mut feedback = Vec::new();
    while at < raw.len() {
        require_size(&raw[at..], 4)?;
        if raw[at] >> 6 != 2 {
            return Err(error("rtcp: invalid packet version"));
        }
        let size = 4 * (usize::from(u16::from_be_bytes([raw[at + 2], raw[at + 3]])) + 1);
        if size > raw.len() - at {
            return Err(short());
        }
        let packet = &raw[at..at + size];
        match (packet[1], packet[0] & 31) {
            (205, 15) => feedback.push(CompoundFeedback::Twcc(parse_twcc(packet)?)),
            (205, 11) => feedback.push(CompoundFeedback::Rfc8888(parse_rfc8888(packet)?)),
            _ => validate_other(packet)?,
        }
        packet_count += 1;
        at += size;
    }
    if packet_count == 0 {
        return Err(error("rtcp: invalid header"));
    }
    Ok(CompoundReport {
        packet_count,
        feedback,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    fn twcc() -> Vec<u8> {
        vec![
            0x8f, 205, 0, 5, 0, 0, 0, 1, 0, 0, 0, 2, 0, 1, 0, 1, 0, 0, 1, 0, 0x20, 1, 0, 0,
        ]
    }
    #[test]
    fn later_malformed_packet_rejects_whole_compound_before_delivery() {
        let good = twcc();
        assert_eq!(parse_compound(&good).unwrap().feedback.len(), 1);
        for suffix in [
            &[0][..],
            &[0x80, 200, 0, 1, 0, 0, 0, 1][..],
            &[0x81, 205, 0, 1, 0, 0, 0, 1][..],
        ] {
            let mut compound = good.clone();
            compound.extend_from_slice(suffix);
            assert!(parse_compound(&compound).is_err());
        }
        let mut compound = vec![0x80, 210, 0, 0]; // unknown type stays valid RawPacket
        compound.extend_from_slice(&good);
        let parsed = parse_compound(&compound).unwrap();
        assert_eq!(parsed.packet_count, 2);
        assert_eq!(parsed.feedback.len(), 1);
    }
    fn compare_actual(input: &str, output: &str, count: usize) {
        let inputs: Vec<serde_json::Value> = input
            .lines()
            .map(|s| serde_json::from_str(s).unwrap())
            .collect();
        let rows: Vec<serde_json::Value> = output
            .lines()
            .map(|s| serde_json::from_str(s).unwrap())
            .collect();
        assert_eq!(inputs.len(), count);
        assert_eq!(inputs.len(), rows.len());
        for (case, row) in inputs.iter().zip(&rows) {
            assert_eq!(case["id"], row["id"]);
            let hex = case["raw_hex"].as_str().unwrap();
            let raw: Vec<u8> = (0..hex.len())
                .step_by(2)
                .map(|i| u8::from_str_radix(&hex[i..i + 2], 16).unwrap())
                .collect();
            let result = parse_compound(&raw);
            assert_eq!(
                result.is_ok(),
                row["accepted"].as_bool().unwrap(),
                "{}",
                case["id"]
            );
            match result {
                Ok(parsed) => assert_eq!(
                    parsed.packet_count as u64,
                    row["packet_count"].as_u64().unwrap(),
                    "{}",
                    case["id"]
                ),
                Err(FeedbackError::Wire(e)) => {
                    assert_eq!(e, row["error"].as_str().unwrap(), "{}", case["id"])
                }
                Err(other) => panic!("unexpected {other:?}"),
            }
        }
    }
    #[test]
    #[ignore = "requires original fourteen-case actual fixed Go output"]
    fn actual_compound_admission_fourteen() {
        let input = std::fs::read_to_string(std::env::var("PION_COMPOUND_INPUT").unwrap()).unwrap();
        let output =
            std::fs::read_to_string(std::env::var("PION_COMPOUND_RESULTS").unwrap()).unwrap();
        compare_actual(&input, &output, 14);
    }
    #[test]
    #[ignore = "requires additional 234 typed-boundary actual fixed Go output"]
    fn actual_compound_admission_typed_boundaries() {
        let input =
            std::fs::read_to_string(std::env::var("PION_COMPOUND_EDGE_INPUT").unwrap()).unwrap();
        let output =
            std::fs::read_to_string(std::env::var("PION_COMPOUND_EDGE_RESULTS").unwrap()).unwrap();
        compare_actual(&input, &output, 234);
    }
}
