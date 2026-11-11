//! Descriptor-bound request ProtoJSON. Responses retain their existing dialect.
//!
//! Raw JSON values preserve duplicate keys and integer lexical precision. The
//! descriptor pass enforces Google's seen-field rule and exact integer parsing;
//! prost-reflect owns message, enum, oneof and WKT decoding afterwards.

use std::collections::BTreeSet;
use std::fmt;
use std::sync::OnceLock;

use prost::Message;
use prost_reflect::{DescriptorPool, DynamicMessage, Kind, MessageDescriptor};
use serde::de::{MapAccess, Visitor};
use serde::{Deserialize, Deserializer};
use serde_json::value::RawValue;

type Result<T> = std::result::Result<T, serde_json::Error>;
fn error(message: impl fmt::Display) -> serde_json::Error {
    <serde_json::Error as serde::de::Error>::custom(message)
}

fn pool() -> &'static DescriptorPool {
    static POOL: OnceLock<DescriptorPool> = OnceLock::new();
    POOL.get_or_init(|| {
        DescriptorPool::decode(
            include_bytes!(concat!(env!("OUT_DIR"), "/livekit-descriptors.bin")).as_slice(),
        )
        .expect("build-generated pinned descriptors must be valid")
    })
}

/// Immutable descriptor pool generated from the pinned schema inputs.
pub fn descriptor_pool() -> &'static DescriptorPool {
    pool()
}

/// Decode protobuf without discarding unknown fields. Callers keep this dynamic
/// value request-local when read/modify/write needs forward-compatible fidelity.
pub fn decode_wire_dynamic(name: &str, body: &[u8]) -> Result<DynamicMessage> {
    let descriptor = pool()
        .get_message_by_name(name)
        .ok_or_else(|| error("unknown wire descriptor"))?;
    DynamicMessage::decode(descriptor, body).map_err(error)
}

/// Original Twirp response dialect for an already-preserved wire message.
pub fn encode_twirp_dynamic(message: &DynamicMessage) -> Result<Vec<u8>> {
    use prost_reflect::ReflectMessage;
    let name = message.descriptor().full_name().to_owned();
    let mut value = response::reflect_value(&name, &message.encode_to_vec())?;
    response::complete(&name, &mut value)?;
    serde_json::to_vec(&value)
}

/// Default ProtoJSON (camelCase, omit unpopulated), including unknown enum values.
/// Worker/signal WebSockets use this dialect; Twirp responses keep their distinct
/// EmitUnpopulated + original-name serializer.
pub fn encode_default<T: prost::Message + prost::Name>(message: &T) -> Result<Vec<u8>> {
    let descriptor = pool()
        .get_message_by_name(&T::full_name())
        .ok_or_else(|| error("unknown response descriptor"))?;
    let value =
        prost_reflect::DynamicMessage::decode(descriptor, message.encode_to_vec().as_slice())
            .map_err(error)?;
    let mut serializer = serde_json::Serializer::new(Vec::new());
    value
        .serialize_with_options(
            &mut serializer,
            &prost_reflect::SerializeOptions::new()
                .use_proto_field_name(false)
                .skip_default_fields(true),
        )
        .map_err(error)?;
    Ok(serializer.into_inner())
}

/// Decode the first JSON value into a generated LiveKit request. Trailing bytes
/// remain ignored, matching the generated upstream Twirp json.Decoder boundary.
pub fn decode<T: prost::Message + prost::Name + Default>(body: &[u8]) -> Result<T> {
    let message = decode_dynamic(&T::full_name(), body)?;
    T::decode(message.encode_to_vec().as_slice()).map_err(error)
}

/// Decode using a fully qualified pinned message name, useful for independent
/// typed oracle tests. Unknown descriptors are errors, never untyped fallback.
pub fn decode_dynamic(name: &str, body: &[u8]) -> Result<DynamicMessage> {
    let descriptor = pool()
        .get_message_by_name(name)
        .ok_or_else(|| error("unknown request descriptor"))?;
    let mut input = serde_json::Deserializer::from_slice(body);
    let raw = Box::<RawValue>::deserialize(&mut input)?;
    validate_string_tokens(raw.get())?;
    let normalized = normalize_message(&descriptor, raw.get(), 0)?;
    builder::message(descriptor, &normalized, 0)
}

struct Object(Vec<(String, Box<RawValue>)>);
impl<'de> Deserialize<'de> for Object {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> std::result::Result<Self, D::Error> {
        struct ObjectVisitor;
        impl<'de> Visitor<'de> for ObjectVisitor {
            type Value = Object;
            fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str("a message object")
            }
            fn visit_map<A: MapAccess<'de>>(
                self,
                mut map: A,
            ) -> std::result::Result<Object, A::Error> {
                let mut fields = Vec::new();
                while let Some(pair) = map.next_entry()? {
                    fields.push(pair);
                }
                Ok(Object(fields))
            }
        }
        deserializer.deserialize_map(ObjectVisitor)
    }
}

fn object(fields: impl IntoIterator<Item = (String, String)>) -> Result<String> {
    let mut output = String::from("{");
    for (index, (name, value)) in fields.into_iter().enumerate() {
        if index != 0 {
            output.push(',');
        }
        output.push_str(&serde_json::to_string(&name)?);
        output.push(':');
        output.push_str(&value);
    }
    output.push('}');
    Ok(output)
}

fn normalize_message(desc: &MessageDescriptor, raw: &str, depth: usize) -> Result<String> {
    if depth > 100 {
        return Err(error("message recursion limit"));
    }
    // WKT JSON shapes are not ordinary message objects. Their grammar and
    // ranges are decoded by the mature reflection implementation.
    if desc.full_name() == "google.protobuf.Duration" {
        return duration(raw);
    }
    if desc.full_name().starts_with("google.protobuf.") {
        return Ok(raw.to_owned());
    }
    let Object(fields) = serde_json::from_str(raw)?;
    let mut seen = BTreeSet::new();
    let mut normalized = Vec::with_capacity(fields.len());
    for (key, value) in fields {
        let Some(field) = desc
            .get_field_by_json_name(&key)
            .or_else(|| desc.get_field_by_name(&key))
        else {
            normalized.push((key, value.get().to_owned()));
            continue;
        };
        // Mark presence BEFORE null or unknown-symbolic-enum skipping.
        if !seen.insert(field.number()) {
            return Err(error(format!("duplicate field {key}")));
        }
        let value = if value.get() == "null" {
            "null".to_owned()
        } else if field.is_map() {
            let Kind::Message(entry) = field.kind() else {
                return Err(error("invalid map descriptor"));
            };
            let kind = entry
                .get_field(2)
                .ok_or_else(|| error("invalid map value descriptor"))?
                .kind();
            let Object(entries) = serde_json::from_str(value.get())?;
            object(
                entries
                    .into_iter()
                    .map(|(key, value)| Ok((key, normalize_kind(&kind, value.get(), depth + 1)?)))
                    .collect::<Result<Vec<_>>>()?,
            )?
        } else if field.is_list() {
            let values: Vec<Box<RawValue>> = serde_json::from_str(value.get())?;
            let values = values
                .into_iter()
                .map(|value| normalize_kind(&field.kind(), value.get(), depth + 1))
                .collect::<Result<Vec<_>>>()?;
            format!("[{}]", values.join(","))
        } else {
            normalize_kind(&field.kind(), value.get(), depth + 1)?
        };
        normalized.push((key, value));
    }
    object(normalized)
}

fn normalize_kind(kind: &Kind, raw: &str, depth: usize) -> Result<String> {
    match kind {
        Kind::Int32 | Kind::Sint32 | Kind::Sfixed32 => integer(raw, true, 32),
        Kind::Int64 | Kind::Sint64 | Kind::Sfixed64 => integer(raw, true, 64),
        Kind::Uint32 | Kind::Fixed32 => integer(raw, false, 32),
        Kind::Uint64 | Kind::Fixed64 => integer(raw, false, 64),
        Kind::Enum(_) if !raw.starts_with('"') && raw != "null" => {
            // Reflection's enum numeric visitor expects an integer JSON token.
            let quoted = integer(raw, true, 32)?;
            serde_json::from_str::<String>(&quoted)
        }
        Kind::Message(desc) if raw != "null" => normalize_message(desc, raw, depth),
        _ => Ok(raw.to_owned()),
    }
}

/// Exact decimal/exponent integer normalization: never convert through f64.
fn integer(raw: &str, signed: bool, bits: u32) -> Result<String> {
    let token = if raw.starts_with('"') {
        serde_json::from_str::<String>(raw)?
    } else {
        raw.to_owned()
    };
    let bytes = token.as_bytes();
    if bytes.is_empty() {
        return Err(error("empty integer"));
    }
    let negative = bytes[0] == b'-';
    let start = usize::from(negative);
    let mut at = start;
    while at < bytes.len() && bytes[at].is_ascii_digit() {
        at += 1;
    }
    if at == start || (at - start > 1 && bytes[start] == b'0') {
        return Err(error("invalid integer syntax"));
    }
    let mut digits = token[start..at].to_owned();
    let mut scale: i64 = 0;
    if bytes.get(at) == Some(&b'.') {
        at += 1;
        let first = at;
        while at < bytes.len() && bytes[at].is_ascii_digit() {
            at += 1;
        }
        if at == first {
            return Err(error("invalid decimal syntax"));
        }
        digits.push_str(&token[first..at]);
        scale = -i64::try_from(at - first).map_err(error)?;
    }
    if matches!(bytes.get(at), Some(b'e' | b'E')) {
        at += 1;
        let first = at;
        if matches!(bytes.get(at), Some(b'+' | b'-')) {
            at += 1;
        }
        let exp_digits = at;
        while at < bytes.len() && bytes[at].is_ascii_digit() {
            at += 1;
        }
        if at == exp_digits {
            return Err(error("invalid exponent"));
        }
        let exponent = if digits.bytes().all(|value| value == b'0') {
            0
        } else {
            token[first..at].parse::<i32>().map_err(error)?
        };
        scale = scale
            .checked_add(i64::from(exponent))
            .ok_or_else(|| error("exponent overflow"))?;
    }
    if at != bytes.len() {
        return Err(error("invalid integer syntax"));
    }
    // Zero can be normalized without expanding even extreme finite exponents.
    if digits.bytes().all(|value| value == b'0') {
        return Ok("\"0\"".into());
    }
    // Leading fractional zeroes affect scale, not integer magnitude or range.
    digits = digits.trim_start_matches('0').to_owned();
    if scale < 0 {
        let remove = usize::try_from(-scale).map_err(error)?;
        if remove > digits.len()
            || !digits[digits.len() - remove..]
                .bytes()
                .all(|value| value == b'0')
        {
            return Err(error("fractional integer"));
        }
        digits.truncate(digits.len() - remove);
    } else {
        let append = usize::try_from(scale).map_err(error)?;
        if append > 20 || digits.len().saturating_add(append) > 40 {
            return Err(error("integer out of range"));
        }
        digits.extend(std::iter::repeat_n('0', append));
    }
    let digits = digits.trim_start_matches('0');
    let canonical = format!("{}{}", if negative { "-" } else { "" }, digits);
    if signed {
        let value = canonical.parse::<i64>().map_err(error)?;
        if bits == 32 {
            i32::try_from(value).map_err(error)?;
        }
    } else {
        let value = canonical.parse::<u64>().map_err(error)?;
        if bits == 32 {
            u32::try_from(value).map_err(error)?;
        }
    }
    serde_json::to_string(&canonical)
}

// Google's Duration accepts +1s, .5s and 1.s, but never >9 fractional
// digits. Normalize only that grammar; reflection still enforces WKT ranges.
fn duration(raw: &str) -> Result<String> {
    let value: String = serde_json::from_str(raw)?;
    let value = value
        .strip_suffix('s')
        .ok_or_else(|| error("missing duration suffix"))?;
    let (sign, value) = match value.as_bytes().first() {
        Some(b'-') => ("-", &value[1..]),
        Some(b'+') => ("", &value[1..]),
        _ => ("", value),
    };
    let (whole, fraction) = value
        .split_once('.')
        .map_or((value, None), |(whole, fraction)| (whole, Some(fraction)));
    if (whole.is_empty() && fraction.is_none())
        || !whole.bytes().all(|v| v.is_ascii_digit())
        || (whole.len() > 1 && whole.starts_with('0'))
    {
        return Err(error("invalid duration seconds"));
    }
    let fraction = match fraction {
        Some(value) if value.len() <= 9 && value.bytes().all(|v| v.is_ascii_digit()) => value,
        Some(_) => return Err(error("invalid duration fraction")),
        None => "",
    };
    if whole.is_empty() && fraction.is_empty() {
        return Err(error("empty duration"));
    }
    let whole = if whole.is_empty() { "0" } else { whole };
    let canonical = if fraction.is_empty() {
        format!("{sign}{whole}s")
    } else {
        format!("{sign}{whole}.{fraction}s")
    };
    serde_json::to_string(&canonical)
}

#[cfg(test)]
mod tests;

// Ignored unknown strings still obey Google's UTF-8/surrogate grammar. RawValue
// validates JSON structure without decoding ignored string escape sequences.
fn validate_string_tokens(raw: &str) -> Result<()> {
    let bytes = raw.as_bytes();
    let mut index = 0;
    while index < bytes.len() {
        if bytes[index] != b'"' {
            index += 1;
            continue;
        }
        let start = index;
        index += 1;
        while index < bytes.len() {
            if bytes[index] == b'\\' {
                index += 2;
            } else if bytes[index] == b'"' {
                index += 1;
                break;
            } else {
                index += 1;
            }
        }
        let _: String = serde_json::from_str(&raw[start..index])?;
    }
    Ok(())
}
mod builder;
pub mod diagnostic;

pub(crate) mod response;

pub mod wire_error;

/// Decode a complete WebSocket JSON frame using LiveKit's permissive wrapper:
/// unknown fields/enum names are discarded, but a second JSON value or trailing
/// non-whitespace bytes are errors. Twirp's first-value decoder stays separate.
pub fn decode_frame<T: prost::Message + prost::Name + Default>(body: &[u8]) -> Result<T> {
    let mut input = serde_json::Deserializer::from_slice(body);
    let _ = Box::<RawValue>::deserialize(&mut input)?;
    input.end()?;
    decode(body)
}
