//! Narrow descriptor builder for recursive messages and Any. Scalar/WKT wire
//! values are decoded by prost-reflect; no generated field-name tables exist.
use std::collections::HashMap;

use prost::Message;
use prost_reflect::{DynamicMessage, Kind, MapKey, MessageDescriptor, Value};

use super::{Object, Result, error, object, pool};

pub(super) fn message(desc: MessageDescriptor, raw: &str, depth: usize) -> Result<DynamicMessage> {
    if depth > 100 {
        return Err(error("message recursion limit"));
    }
    match desc.full_name() {
        "google.protobuf.Any" => return any(desc, raw, depth),
        "google.protobuf.Timestamp" => return timestamp(desc, raw),
        "google.protobuf.FieldMask" => {
            let value: String = serde_json::from_str(raw)?;
            let value = value.trim();
            if !value.is_empty() {
                for path in value.split(',') {
                    if path.contains('_')
                        || path.split('.').any(|part| {
                            let mut bytes = part.bytes();
                            !bytes
                                .next()
                                .is_some_and(|v| v.is_ascii_alphabetic() || v == b'_')
                                || !bytes.all(|v| v.is_ascii_alphanumeric() || v == b'_')
                        })
                    {
                        return Err(error("invalid FieldMask path"));
                    }
                }
            }
            return reflect(desc, &serde_json::to_string(value)?);
        }
        "google.protobuf.Struct" => {
            let Object(fields) = serde_json::from_str(raw)?;
            let mut seen = std::collections::BTreeSet::new();
            for (key, _) in &fields {
                if !seen.insert(key) {
                    return Err(error("duplicate Struct key"));
                }
            }
        }
        _ => {}
    }
    if desc.full_name().starts_with("google.protobuf.") {
        return reflect(desc, raw);
    }
    let Object(fields) = serde_json::from_str(raw)?;
    let mut output = DynamicMessage::new(desc.clone());
    for (key, raw) in fields {
        let Some(field) = desc
            .get_field_by_json_name(&key)
            .or_else(|| desc.get_field_by_name(&key))
        else {
            continue;
        };
        if raw.get() == "null" {
            continue;
        }
        let value = if field.is_map() {
            let Kind::Message(entry) = field.kind() else {
                return Err(error("invalid map"));
            };
            let key_kind = entry
                .get_field(1)
                .ok_or_else(|| error("missing map key"))?
                .kind();
            let value_field = entry
                .get_field(2)
                .ok_or_else(|| error("missing map value"))?;
            let Object(entries) = serde_json::from_str(raw.get())?;
            let mut map = HashMap::new();
            for (key, raw) in entries {
                let key = map_key(&key_kind, &key)?;
                if map.contains_key(&key) {
                    return Err(error("duplicate semantic map key"));
                }
                if raw.get() == "null" {
                    // ProtoJSON's ordinary nullable FIELD shortcut is not
                    // applied to map values. Only Value/NullValue accept null.
                    let accepts_null = match value_field.kind() {
                        Kind::Message(inner) => inner.full_name() == "google.protobuf.Value",
                        Kind::Enum(inner) => inner.full_name() == "google.protobuf.NullValue",
                        _ => false,
                    };
                    if !accepts_null {
                        return Err(error("invalid null map value"));
                    }
                }
                if unknown_enum(&value_field.kind(), raw.get())? {
                    continue;
                }
                let value = match value_field.kind() {
                    Kind::Message(inner) => Value::Message(message(inner, raw.get(), depth + 1)?),
                    _ => {
                        let temp = reflect(
                            entry.clone(),
                            &object([(String::from("value"), raw.get().to_owned())])?,
                        )?;
                        temp.get_field(&value_field).into_owned()
                    }
                };
                map.insert(key, value);
            }
            Value::Map(map)
        } else if let Kind::Message(inner) = field.kind() {
            if field.is_list() {
                let values: Vec<Box<serde_json::value::RawValue>> =
                    serde_json::from_str(raw.get())?;
                Value::List(
                    values
                        .into_iter()
                        .map(|raw| message(inner.clone(), raw.get(), depth + 1).map(Value::Message))
                        .collect::<Result<Vec<_>>>()?,
                )
            } else {
                Value::Message(message(inner, raw.get(), depth + 1)?)
            }
        } else {
            if unknown_enum(&field.kind(), raw.get())? {
                continue;
            }
            let temp = reflect(desc.clone(), &object([(key, raw.get().to_owned())])?)?;
            temp.get_field(&field).into_owned()
        };
        if let Some(oneof) = field.containing_oneof() {
            if oneof.fields().any(|other| output.has_field(&other)) {
                return Err(error("multiple non-null oneof fields"));
            }
        }
        output.set_field(&field, value);
    }
    Ok(output)
}

fn reflect(desc: MessageDescriptor, raw: &str) -> Result<DynamicMessage> {
    DynamicMessage::deserialize_with_options(
        desc,
        &mut serde_json::Deserializer::from_str(raw),
        &prost_reflect::DeserializeOptions::new().deny_unknown_fields(false),
    )
}
pub(super) fn unknown_enum(kind: &Kind, raw: &str) -> Result<bool> {
    if let Kind::Enum(desc) = kind {
        if raw.starts_with('"') {
            let name: String = serde_json::from_str(raw)?;
            return Ok(desc.get_value_by_name(&name).is_none());
        }
    }
    Ok(false)
}
pub(super) fn map_key(kind: &Kind, key: &str) -> Result<MapKey> {
    Ok(match kind {
        Kind::String => MapKey::String(key.into()),
        Kind::Bool => MapKey::Bool(match key {
            "true" => true,
            "false" => false,
            _ => return Err(error("invalid bool map key")),
        }),
        Kind::Int32 | Kind::Sint32 | Kind::Sfixed32 => MapKey::I32(key.parse().map_err(error)?),
        Kind::Int64 | Kind::Sint64 | Kind::Sfixed64 => MapKey::I64(key.parse().map_err(error)?),
        Kind::Uint32 | Kind::Fixed32 => MapKey::U32(key.parse().map_err(error)?),
        Kind::Uint64 | Kind::Fixed64 => MapKey::U64(key.parse().map_err(error)?),
        _ => return Err(error("invalid map key kind")),
    })
}
fn any(desc: MessageDescriptor, raw: &str, depth: usize) -> Result<DynamicMessage> {
    let Object(fields) = serde_json::from_str(raw)?;
    let mut output = DynamicMessage::new(desc.clone());
    if fields.is_empty() {
        return Ok(output);
    }
    let mut url = None;
    for (key, raw) in &fields {
        if key == "@type" {
            if url.is_some() {
                return Err(error("duplicate @type"));
            }
            let value: String = serde_json::from_str(raw.get())?;
            if value.is_empty() {
                return Err(error("empty Any type URL"));
            }
            url = Some(value);
        }
    }
    let url = url.ok_or_else(|| error("missing Any type URL"))?;
    let name = url
        .rsplit('/')
        .next()
        .ok_or_else(|| error("invalid Any URL"))?;
    let payload_desc = pool()
        .get_message_by_name(name)
        .ok_or_else(|| error("unregistered Any payload"))?;
    let payload_fields = fields
        .into_iter()
        .filter(|(key, _)| key != "@type")
        .collect::<Vec<_>>();
    let raw = if name.starts_with("google.protobuf.") {
        let mut value = None;
        for (key, raw) in payload_fields {
            if key == "value" {
                if value.is_some() {
                    return Err(error("duplicate Any value"));
                }
                value = Some(raw.get().to_owned());
            }
        }
        value.ok_or_else(|| error("missing Any WKT value"))?
    } else {
        object(
            payload_fields
                .into_iter()
                .map(|(key, raw)| (key, raw.get().to_owned())),
        )?
    };
    let normalized = super::normalize_message(&payload_desc, &raw, depth + 1)?;
    let payload = message(payload_desc, &normalized, depth + 1)?;
    output.set_field(&desc.get_field(1).unwrap(), Value::String(url));
    output.set_field(
        &desc.get_field(2).unwrap(),
        Value::Bytes(payload.encode_to_vec().into()),
    );
    Ok(output)
}
fn timestamp(desc: MessageDescriptor, raw: &str) -> Result<DynamicMessage> {
    let mut value: String = serde_json::from_str(raw)?;
    if value.len() < 20
        || value.as_bytes().get(10) != Some(&b'T')
        || value.as_bytes().get(19).is_none()
    {
        return Err(error("invalid Timestamp grammar"));
    }
    if value.get(17..19).is_some_and(|seconds| seconds >= "60") {
        return Err(error("invalid Timestamp seconds"));
    }
    let zone = value
        .rfind(['Z', '+', '-'])
        .ok_or_else(|| error("missing Timestamp zone"))?;
    if zone < 19 {
        return Err(error("missing Timestamp zone"));
    }
    if let Some(dot) = value.find('.') {
        if dot != 19 || zone <= dot || zone - dot > 10 {
            return Err(error("invalid Timestamp precision"));
        }
    }
    let adjust = if value.ends_with("+24:00") {
        -3600
    } else if value.ends_with("-24:00") {
        3600
    } else {
        0
    };
    if adjust != 0 {
        let length = value.len();
        value.replace_range(length - 5..length - 3, "23");
    }
    let mut output = reflect(desc.clone(), &serde_json::to_string(&value)?)?;
    if adjust != 0 {
        let field = desc.get_field(1).unwrap();
        let Value::I64(seconds) = output.get_field(&field).into_owned() else {
            return Err(error("invalid Timestamp descriptor"));
        };
        let seconds = seconds + adjust;
        if !(-62_135_596_800..=253_402_300_799).contains(&seconds) {
            return Err(error("Timestamp out of range"));
        }
        output.set_field(&field, Value::I64(seconds));
    }
    Ok(output)
}
