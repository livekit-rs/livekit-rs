//! Descriptor-directed fixed-Go binary request decoding.
//!
//! The pinned generated-Go decoder treats an unexpected wire kind on a known
//! field number as unknown (internal/impl/codec_gen.go and decode.go). Prost
//! rejects it instead. Filter only framing/unknown fields, then let the real
//! generated decoder validate all retained known scalar values and UTF-8.
//! Unknown bytes are intentionally discarded in the typed projection. A caller
//! needing lossless storage must retain a separate request-owned provenance tree.
use prost::{Message, Name};
use prost_reflect::{FieldDescriptor, Kind, MessageDescriptor};
use std::{fmt, ops::Range};

#[derive(Debug)]
pub enum WireError {
    DescriptorMissing(String),
    Framing(&'static str),
    KnownValue(prost::DecodeError),
}
impl fmt::Display for WireError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::DescriptorMissing(name) => write!(f, "protobuf descriptor missing: {name}"),
            Self::Framing(message) => write!(f, "invalid protobuf framing: {message}"),
            Self::KnownValue(error) => error.fmt(f),
        }
    }
}
impl std::error::Error for WireError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        if let Self::KnownValue(error) = self {
            Some(error)
        } else {
            None
        }
    }
}
type Result<T> = std::result::Result<T, WireError>;

/// Decode a pinned generated message using Go's known-number/wrong-kind rule.
/// Known values, optional presence, oneof order and repeated merge semantics are
/// still decoded by `T`; this function never synthesizes field values.
pub fn decode<T: Message + Name + Default>(body: &[u8]) -> Result<T> {
    T::decode(filter_known(&T::full_name(), body)?.as_slice()).map_err(WireError::KnownValue)
}

/// Return known-field wire with legal unknown fields omitted at every message
/// boundary. Unknown length-delimited values stay opaque; only their framing is
/// checked. The recursion limit matches the generated prost boundary (100).
pub fn filter_known(name: &str, body: &[u8]) -> Result<Vec<u8>> {
    let descriptor = crate::json::descriptor_pool()
        .get_message_by_name(name)
        .ok_or_else(|| WireError::DescriptorMissing(name.to_owned()))?;
    filter(&descriptor, body, 100)
}

/// Re-encode a generated typed projection (`mutated`) while restoring every
/// protobuf field that the typed decode legitimately discarded as unknown at
/// the root and inside retained child messages, matching upstream Go's
/// `proto.Unmarshal`/`proto.Marshal` round trip (which keeps `XXX_unrecognized`
/// bytes and re-emits them). Unknown occurrences include field numbers absent
/// from the descriptor and known numbers carried on a wrong wire type (Go's
/// "unknown" rule). Child messages are matched to their original occurrence in
/// order, so repeated/oneof ordering is preserved; a child dropped from the
/// typed output also loses its unknowns, exactly as Go omits an absent message.
/// Field order within a message is protobuf-irrelevant, so restored unknowns are
/// appended after the retained known fields.
pub fn preserve_unknown_bytes(name: &str, original: &[u8], mutated: &[u8]) -> Result<Vec<u8>> {
    let descriptor = crate::json::descriptor_pool()
        .get_message_by_name(name)
        .ok_or_else(|| WireError::DescriptorMissing(name.to_owned()))?;
    merge(&descriptor, original, mutated, 100)
}

/// Fixed internal PSRPC schema only; kept separate from public JSON dialects.
/// Original envelopes remain with the caller, this is validation/projection only.
pub fn filter_known_psrpc(name: &str, body: &[u8]) -> Result<Vec<u8>> {
    filter(&psrpc_descriptor(name)?, body, 100)
}
fn psrpc_descriptor(name: &str) -> Result<MessageDescriptor> {
    static POOL: std::sync::OnceLock<prost_reflect::DescriptorPool> = std::sync::OnceLock::new();
    let pool = POOL.get_or_init(|| {
        prost_reflect::DescriptorPool::decode(
            include_bytes!(concat!(env!("OUT_DIR"), "/psrpc-descriptors.bin")).as_slice(),
        )
        .expect("build-generated fixed PSRPC descriptors")
    });
    pool.get_message_by_name(name)
        .ok_or_else(|| WireError::DescriptorMissing(name.to_owned()))
}
/// Root projection for a source-compatible PSRPC consumer that independently
/// preserves child wires (notably Response Any provenance). Accepted children
/// remain byte-for-byte original, including duplicate occurrences/default clears.
/// Full recursive admission remains the caller's normal typed forward boundary.
pub fn filter_known_psrpc_root(name: &str, body: &[u8]) -> Result<Vec<u8>> {
    let descriptor = psrpc_descriptor(name)?;
    let mut output = Vec::with_capacity(body.len());
    let mut at = 0;
    while at < body.len() {
        let start = at;
        let tag = varint(body, &mut at)?;
        let number = field_number(tag)?;
        let wire = (tag & 7) as u8;
        let _ = skip(body, &mut at, number, wire, 100)?;
        if descriptor
            .get_field(number)
            .is_some_and(|field| accepted(&field, wire))
        {
            output.extend_from_slice(&body[start..at]);
        }
    }
    Ok(output)
}

fn accepted(field: &FieldDescriptor, wire: u8) -> bool {
    if field.is_group() {
        return wire == 3;
    }
    let scalar = match field.kind() {
        Kind::Double | Kind::Fixed64 | Kind::Sfixed64 => 1,
        Kind::Float | Kind::Fixed32 | Kind::Sfixed32 => 5,
        Kind::String | Kind::Bytes | Kind::Message(_) => 2,
        _ => 0,
    };
    wire == scalar || (wire == 2 && field.is_list() && scalar != 2)
}

fn filter(descriptor: &MessageDescriptor, body: &[u8], depth: usize) -> Result<Vec<u8>> {
    if depth == 0 {
        return Err(WireError::Framing("message recursion limit"));
    }
    let mut output = Vec::with_capacity(body.len());
    let mut at = 0;
    while at < body.len() {
        let start = at;
        let tag = varint(body, &mut at)?;
        let number = field_number(tag)?;
        let wire = (tag & 7) as u8;
        let payload = skip(body, &mut at, number, wire, depth)?;
        let Some(field) = descriptor
            .get_field(number)
            .filter(|field| accepted(field, wire))
        else {
            continue;
        };
        if let Kind::Message(child) = field.kind() {
            let payload = payload.ok_or(WireError::Framing("missing message payload"))?;
            let filtered = filter(&child, &body[payload], depth - 1)?;
            put_varint(&mut output, (u64::from(number) << 3) | u64::from(wire));
            if wire == 2 {
                put_varint(&mut output, filtered.len() as u64);
            }
            output.extend(filtered);
            if wire == 3 {
                put_varint(&mut output, (u64::from(number) << 3) | 4);
            }
        } else {
            // Keep exact known scalar / string / packed bytes, including value
            // varints. Generated prost performs semantic and UTF-8 validation.
            output.extend_from_slice(&body[start..at]);
        }
    }
    Ok(output)
}

/// Recursive body of [`preserve_unknown_bytes`]. `original` is the untouched
/// inbound wire (source of discarded unknowns); `mutated` is the typed
/// re-encode. Returns `mutated` with retained child messages recursively merged
/// and this level's unknown occurrences appended.
fn merge(
    desc: &MessageDescriptor,
    original: &[u8],
    mutated: &[u8],
    depth: usize,
) -> Result<Vec<u8>> {
    if depth == 0 {
        return Err(WireError::Framing("message recursion limit"));
    }
    // Pass 1 over the original: collect this level's unknown occurrences and
    // queue the payload range of each retained child-message occurrence.
    let mut unknown_tail: Vec<u8> = Vec::new();
    let mut orig_children: std::collections::HashMap<
        u32,
        std::collections::VecDeque<Range<usize>>,
    > = std::collections::HashMap::new();
    let mut at = 0;
    while at < original.len() {
        let start = at;
        let tag = varint(original, &mut at)?;
        let number = field_number(tag)?;
        let wire = (tag & 7) as u8;
        let payload = skip(original, &mut at, number, wire, depth)?;
        let field = desc.get_field(number).filter(|f| accepted(f, wire));
        match field {
            // A retained child message may itself carry discarded unknowns.
            Some(f) if wire == 2 && matches!(f.kind(), Kind::Message(_)) => {
                if let Some(range) = payload {
                    orig_children.entry(number).or_default().push_back(range);
                }
            }
            // Any other accepted known field needs no restoration here.
            Some(_) => {}
            // Unknown number or known-number-wrong-kind: keep verbatim.
            None => unknown_tail.extend_from_slice(&original[start..at]),
        }
    }
    // Pass 2 over the typed re-encode: rewrite retained child messages with
    // their merged bodies and copy every other field verbatim.
    let mut out: Vec<u8> = Vec::with_capacity(mutated.len() + unknown_tail.len());
    let mut at = 0;
    while at < mutated.len() {
        let start = at;
        let tag = varint(mutated, &mut at)?;
        let number = field_number(tag)?;
        let wire = (tag & 7) as u8;
        let payload = skip(mutated, &mut at, number, wire, depth)?;
        let end = at;
        let child = if wire == 2 {
            match desc.get_field(number).map(|f| f.kind()) {
                Some(Kind::Message(child)) => Some(child),
                _ => None,
            }
        } else {
            None
        };
        match (child, payload) {
            (Some(child), Some(range)) => {
                out.extend_from_slice(&mutated[start..start + varint_len(tag)]);
                let merged = match orig_children.get_mut(&number).and_then(|q| q.pop_front()) {
                    Some(orig_range) => {
                        merge(&child, &original[orig_range], &mutated[range], depth - 1)?
                    }
                    None => mutated[range].to_vec(),
                };
                put_varint(&mut out, merged.len() as u64);
                out.extend(merged);
            }
            _ => out.extend_from_slice(&mutated[start..end]),
        }
    }
    out.extend(unknown_tail);
    Ok(out)
}

/// Byte length of a canonical varint already validated by [`varint`].
fn varint_len(value: u64) -> usize {
    let mut v = value >> 7;
    let mut len = 1;
    while v != 0 {
        v >>= 7;
        len += 1;
    }
    len
}

fn field_number(tag: u64) -> Result<u32> {
    let number = tag >> 3;
    if number == 0 || number > 0x1fff_ffff {
        return Err(WireError::Framing("invalid field number"));
    }
    Ok(number as u32)
}
fn varint(bytes: &[u8], at: &mut usize) -> Result<u64> {
    let mut value = 0;
    for shift in (0..70).step_by(7) {
        let b = *bytes
            .get(*at)
            .ok_or(WireError::Framing("truncated varint"))?;
        *at += 1;
        if shift == 63 && b > 1 {
            return Err(WireError::Framing("invalid varint"));
        }
        value |= u64::from(b & 127) << shift;
        if b < 128 {
            return Ok(value);
        }
    }
    Err(WireError::Framing("invalid varint"))
}
fn advance(bytes: &[u8], at: &mut usize, count: usize) -> Result<()> {
    *at = at
        .checked_add(count)
        .filter(|end| *end <= bytes.len())
        .ok_or(WireError::Framing("truncated field"))?;
    Ok(())
}
fn skip(
    bytes: &[u8],
    at: &mut usize,
    number: u32,
    wire: u8,
    depth: usize,
) -> Result<Option<Range<usize>>> {
    match wire {
        0 => {
            varint(bytes, at)?;
        }
        1 => advance(bytes, at, 8)?,
        2 => {
            let size = usize::try_from(varint(bytes, at)?)
                .map_err(|_| WireError::Framing("length overflow"))?;
            let begin = *at;
            advance(bytes, at, size)?;
            return Ok(Some(begin..*at));
        }
        3 => {
            if depth == 0 {
                return Err(WireError::Framing("group recursion limit"));
            }
            let begin = *at;
            loop {
                let end = *at;
                let tag = varint(bytes, at)?;
                let child = field_number(tag)?;
                let kind = (tag & 7) as u8;
                if kind == 4 {
                    if child != number {
                        return Err(WireError::Framing("mismatched end group"));
                    }
                    return Ok(Some(begin..end));
                }
                skip(bytes, at, child, kind, depth - 1)?;
            }
        }
        5 => advance(bytes, at, 4)?,
        _ => return Err(WireError::Framing("invalid wire type")),
    }
    Ok(None)
}
fn put_varint(output: &mut Vec<u8>, mut value: u64) {
    while value >= 128 {
        output.push((value as u8 & 127) | 128);
        value >>= 7;
    }
    output.push(value as u8);
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::livekit as pb;
    #[test]
    fn known_number_wrong_kind_is_unknown_not_a_known_value() {
        let wire = [0x12, 1, b'N', 0x10, 7, 0xa0, 6, 0x81, 0];
        assert!(pb::SipInboundTrunkInfo::decode(wire.as_slice()).is_err());
        let actual = decode::<pb::SipInboundTrunkInfo>(&wire).unwrap();
        assert_eq!(actual.name, "N");
        assert_eq!(actual.encode_to_vec(), vec![0x12, 1, b'N']);
        assert_eq!(
            decode::<pb::SipInboundTrunkInfo>(&[0x10, 7]).unwrap().name,
            ""
        );
    }
    #[test]
    fn nested_wrong_kind_and_true_invalid_utf8_are_distinct() {
        // SIPInboundTrunkInfo.media#20 contains MediaConfig.codecs#2 wrong varint.
        let actual = decode::<pb::SipInboundTrunkInfo>(&[0xa2, 1, 2, 0x10, 7]).unwrap();
        assert!(actual.media.unwrap().codecs.is_empty());
        assert!(decode::<pb::SipInboundTrunkInfo>(&[0x12, 1, 0xff]).is_err());
        // Unknown bytes do not acquire a known-string UTF-8 contract.
        assert!(decode::<pb::SipInboundTrunkInfo>(&[0xa2, 6, 1, 0xff]).is_ok());
    }
    #[test]
    fn map_default_semantics_and_valid_values_survive_filtered_entries() {
        // headers#9 entry key#1 wrong varint is discarded => default empty key.
        let actual = decode::<pb::SipInboundTrunkInfo>(&[0x4a, 5, 8, 7, 0x12, 1, b'v']).unwrap();
        assert_eq!(actual.headers.get("").map(String::as_str), Some("v"));
        // Known UTF-8 key still validates, even when its value is empty.
        assert!(decode::<pb::SipInboundTrunkInfo>(&[0x4a, 3, 0x0a, 1, 0xff]).is_err());
    }
    #[test]
    fn malformed_unknown_framing_is_never_silently_accepted() {
        for wire in [
            &[0xa0, 6, 0x80][..],
            &[0xa2, 6, 2, 1][..],
            &[0xa3, 6, 0xac, 6][..],
            &[0][..],
            &[0xa6, 6][..],
        ] {
            assert!(
                decode::<pb::SipInboundTrunkInfo>(wire).is_err(),
                "{wire:02x?}"
            );
        }
    }
    #[test]
    fn repeated_parent_oneof_and_optional_presence_are_not_reconstructed() {
        // Info.rule#2 Direct#1 then another rule Individual#2 then Direct#1.
        let actual = decode::<pb::SipDispatchRuleInfo>(&[
            0x12, 5, 0x0a, 3, 0xa0, 6, 7, 0x12, 4, 0x12, 0, 0x0a, 0,
        ])
        .unwrap();
        assert!(matches!(
            actual.rule.unwrap().rule,
            Some(pb::sip_dispatch_rule::Rule::DispatchRuleDirect(_))
        ));
        // Field1 is ordinary only_listed_codecs=false, not optional encryption.
        let media = decode::<pb::SipMediaConfig>(&[8, 0]).unwrap();
        assert!(!media.only_listed_codecs);
        assert_eq!(media.encryption, None);
        // Fixed schema encryption#3 must retain an explicitly present zero.
        let media = decode::<pb::SipMediaConfig>(&[0x18, 0]).unwrap();
        assert_eq!(media.encryption, Some(0));
        assert_eq!(media.encode_to_vec(), vec![0x18, 0]);
    }
}

#[cfg(test)]
mod preserve_tests {
    use super::*;
    use crate::livekit as pb;
    use prost_reflect::{DynamicMessage, ReflectMessage, Value};

    fn count_unknown(name: &str, body: &[u8]) -> usize {
        let dyn_msg = crate::json::decode_wire_dynamic(name, body).unwrap();
        fn walk(m: &DynamicMessage) -> usize {
            let mut n = m.unknown_fields().count();
            for field in m.descriptor().fields() {
                if !matches!(field.kind(), Kind::Message(_)) {
                    continue;
                }
                if let Some(v) = m.get_field_by_number(field.number()) {
                    match &*v {
                        Value::Message(c) => n += walk(c),
                        Value::List(l) => {
                            for e in l {
                                if let Value::Message(c) = e {
                                    n += walk(c);
                                }
                            }
                        }
                        _ => {}
                    }
                }
            }
            n
        }
        walk(&dyn_msg)
    }

    #[test]
    fn typed_projection_drops_unknown_but_preserve_restores_root_and_nested() {
        // DataPacket{ User#2{ payload#2='x', unknown100 varint=42 }, unknown200 bytes }.
        let original: Vec<u8> = vec![
            0x12, 6, 0x12, 1, b'x', 0x80, 0x06, 0x2a, // User + nested unknown100
            0xc2, 0x0c, 0x02, 0xff, 0xfe, // root unknown200 length-delimited
        ];
        // Known-only projection and typed re-encode carry no unknowns.
        let known = filter_known("livekit.DataPacket", &original).unwrap();
        assert_eq!(count_unknown("livekit.DataPacket", &known), 0);
        let typed = decode::<pb::DataPacket>(&original).unwrap().encode_to_vec();
        assert_eq!(count_unknown("livekit.DataPacket", &typed), 0);
        // Preservation restores both the root and the nested occurrence.
        let preserved = preserve_unknown_bytes("livekit.DataPacket", &original, &typed).unwrap();
        assert_eq!(count_unknown("livekit.DataPacket", &preserved), 2);
        // Known payload value survives and framing stays valid for typed consumers.
        let back = pb::DataPacket::decode(preserved.as_slice()).unwrap();
        assert!(matches!(back.value, Some(pb::data_packet::Value::User(u)) if u.payload == b"x"));
    }

    #[test]
    fn preserve_is_identity_when_no_unknowns_present() {
        let original: Vec<u8> = vec![0x12, 3, 0x12, 1, b'y']; // User{payload 'y'}
        let typed = decode::<pb::DataPacket>(&original).unwrap().encode_to_vec();
        let preserved = preserve_unknown_bytes("livekit.DataPacket", &original, &typed).unwrap();
        assert_eq!(count_unknown("livekit.DataPacket", &original), 0);
        assert_eq!(count_unknown("livekit.DataPacket", &preserved), 0);
        assert_eq!(typed, preserved, "no-unknown forward is byte-stable");
    }

    #[test]
    fn known_number_wrong_kind_is_restored_as_unknown() {
        // participant_identity#4 declared string (wire2) but sent as varint 7 (Go unknown).
        let original: Vec<u8> = vec![0x12, 3, 0x12, 1, b'x', 0x20, 0x07];
        let typed = decode::<pb::DataPacket>(&original).unwrap().encode_to_vec();
        // The typed projection itself carries no field4 (the wrong kind is dropped).
        assert!(!typed.windows(2).any(|w| w == [0x20, 0x07]));
        let preserved = preserve_unknown_bytes("livekit.DataPacket", &original, &typed).unwrap();
        // Go re-marshals and re-emits the wrong-kind known number verbatim, so the
        // forward must retain its original bytes as an appended unknown occurrence.
        assert!(
            preserved.ends_with(&[0x20, 0x07]),
            "wrong-kind known number preserved: {preserved:02x?}"
        );
        assert_eq!(preserved.len(), typed.len() + 2);
    }
}

#[cfg(test)]
mod official_oracle_tests {
    use super::*;
    use crate::livekit as pb;
    use base64::Engine;
    use prost_reflect::DynamicMessage;
    fn project(name: &str, body: &[u8]) -> Result<Vec<u8>> {
        match name {
            "livekit.SIPInboundTrunkInfo" => {
                decode::<pb::SipInboundTrunkInfo>(body).map(|v| v.encode_to_vec())
            }
            "livekit.SIPDispatchRuleInfo" => {
                decode::<pb::SipDispatchRuleInfo>(body).map(|v| v.encode_to_vec())
            }
            "livekit.SIPMediaConfig" => {
                decode::<pb::SipMediaConfig>(body).map(|v| v.encode_to_vec())
            }
            "livekit.SIPCallInfo" => decode::<pb::SipCallInfo>(body).map(|v| v.encode_to_vec()),
            "livekit.WorkerMessage" => decode::<pb::WorkerMessage>(body).map(|v| v.encode_to_vec()),
            _ => panic!("fixture names must have a real generated typed projection"),
        }
    }
    #[test]
    #[ignore = "requires actual Go results via WIRE_GO_ORACLE_RESULTS and WIRE_GO_ORACLE_CASES"]
    fn fixed_go_generated_binary_decoder_vectors() {
        let cases =
            std::fs::read_to_string(std::env::var("WIRE_GO_ORACLE_CASES").unwrap()).unwrap();
        let expected =
            std::fs::read_to_string(std::env::var("WIRE_GO_ORACLE_RESULTS").unwrap()).unwrap();
        let expected = expected
            .lines()
            .map(|line| serde_json::from_str::<serde_json::Value>(line).unwrap())
            .collect::<Vec<_>>();
        let mut count = 0;
        for line in cases.lines() {
            let row: serde_json::Value = serde_json::from_str(line).unwrap();
            let name = row["type"].as_str().unwrap();
            let hex = row["hex"].as_str().unwrap();
            let body = hex
                .as_bytes()
                .chunks_exact(2)
                .map(|pair| u8::from_str_radix(std::str::from_utf8(pair).unwrap(), 16).unwrap())
                .collect::<Vec<_>>();
            let actual = project(name, &body);
            let expected = expected
                .iter()
                .find(|v| v["name"] == row["name"])
                .expect("missing actual Go result");
            assert_eq!(
                actual.is_ok(),
                expected["accepted"].as_bool().unwrap(),
                "acceptance {}",
                row["name"]
            );
            if let Ok(actual) = actual {
                let known = base64::engine::general_purpose::STANDARD
                    .decode(expected["known_wire_base64"].as_str().unwrap())
                    .unwrap();
                let descriptor = crate::json::descriptor_pool()
                    .get_message_by_name(name)
                    .unwrap();
                let a = DynamicMessage::decode(descriptor.clone(), actual.as_slice()).unwrap();
                let b = DynamicMessage::decode(descriptor, known.as_slice()).unwrap();
                assert_eq!(a, b, "typed fields and presence {}", row["name"]);
            }
            count += 1;
        }
        assert_eq!(count, 24);
    }
}
