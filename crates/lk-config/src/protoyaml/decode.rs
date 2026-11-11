//! Descriptor traversal adapted from buf.build/go/protoyaml v0.7.0 decode.go.
//! Copyright 2023-2026 Buf Technologies, Inc. Licensed under Apache-2.0.
use super::node::{Node, Value};
use prost_reflect::{FieldDescriptor, Kind, MessageDescriptor};
use serde_saphyr::granit_parser::ScalarStyle;
use std::collections::HashMap;

pub(super) struct Decoder<'a> {
    pub errors: Vec<String>,
    pub source: &'a str,
}
fn quote(value: &str) -> String {
    serde_json::to_string(value).expect("string serialization")
}
fn choices(values: impl IntoIterator<Item = String>) -> String {
    let mut names: Vec<_> = values.into_iter().take(7).collect();
    if names.len() == 7 {
        names.push("...".into());
    }
    format!("[{}]", names.join(" "))
}

fn number(node: &mut Node, value: String) {
    node.value = Value::Scalar(value, ScalarStyle::Plain, "!!int".into());
    node.anchor.clear();
    node.explicit_tag = None;
}
fn string(node: &mut Node, value: String) {
    node.value = Value::Scalar(value, ScalarStyle::DoubleQuoted, "!!str".into());
    node.anchor.clear();
    node.explicit_tag = None;
}
impl Decoder<'_> {
    fn error(&mut self, node: &Node, message: impl AsRef<str>) {
        let mut text = format!(":{}:{} {}\n", node.line, node.column, message.as_ref());
        if let Some(line) = self
            .source
            .lines()
            .nth(node.line.saturating_sub(1))
            .filter(|s| !s.is_empty())
        {
            text.push_str(&format!(
                "{:4} | {}\n{:4} | {}^\n",
                node.line,
                line,
                node.line,
                ".".repeat(node.column.saturating_sub(1))
            ));
        }
        self.errors.push(text);
    }
    fn kind(&mut self, node: &Node, expected: &str) -> bool {
        if node.kind() == expected {
            true
        } else {
            self.error(node, format!("expected {expected}, got {}", node.kind()));
            false
        }
    }
    pub fn message(&mut self, node: &Node, descriptor: &MessageDescriptor) -> Node {
        self.message_with(node, descriptor, None)
    }
    fn message_with(
        &mut self,
        node: &Node,
        descriptor: &MessageDescriptor,
        previous: Option<&Node>,
    ) -> Node {
        let mut out = node.clone();
        out.anchor.clear();
        if node.null() {
            if let Some(previous) = previous {
                return previous.clone();
            }
            out.empty_map();
            return out;
        }
        let Value::Map(entries) = &node.value else {
            self.error(
                node,
                format!(
                    "expected fields for {}, got {}",
                    descriptor.full_name(),
                    node.kind()
                ),
            );
            out.empty_map();
            return out;
        };
        let mut fields: Vec<(Node, Node)> = previous
            .and_then(|n| match &n.value {
                Value::Map(v) => Some(v.clone()),
                _ => None,
            })
            .unwrap_or_default();
        let mut oneofs: HashMap<String, String> = HashMap::new();
        for (key, _) in &fields {
            if let Some(field) = descriptor.get_field_by_name(key.text())
                && let Some(oneof) = field.containing_oneof().filter(|v| !v.is_synthetic())
            {
                oneofs.insert(oneof.name().to_owned(), field.name().to_owned());
            }
        }
        for (key, value) in entries {
            if !self.kind(key, "scalar") {
                continue;
            }
            let field = descriptor
                .get_field_by_json_name(key.text())
                .or_else(|| descriptor.get_field_by_name(key.text()))
                .or_else(|| {
                    key.text()
                        .parse::<u32>()
                        .ok()
                        .and_then(|n| descriptor.get_field(n))
                });
            let Some(field) = field else {
                let names = descriptor
                    .descriptor_proto()
                    .field
                    .iter()
                    .filter_map(|f| f.name.clone());
                self.error(
                    key,
                    format!(
                        "unknown field {}, expected one of {}",
                        quote(key.text()),
                        choices(names)
                    ),
                );
                continue;
            };
            if let Some(oneof) = field.containing_oneof().filter(|v| !v.is_synthetic()) {
                if let Some(previous) = oneofs.get(oneof.name()) {
                    self.error(
                        value,
                        format!("field {previous} is already set for oneof {}", oneof.name()),
                    );
                    continue;
                }
                oneofs.insert(oneof.name().to_owned(), field.name().to_owned());
            }
            let mut canonical_key = key.clone();
            string(&mut canonical_key, field.name().into());
            let prior = fields
                .iter()
                .find(|(k, _)| k.text() == field.name())
                .map(|(_, v)| v);
            let canonical = if !field.is_list() && !field.is_map() {
                if let Kind::Message(child) = field.kind() {
                    self.message_with(value, &child, prior)
                } else {
                    self.field(value, &field)
                }
            } else {
                self.field(value, &field)
            };
            // Go Mutable(field).List() does not allocate the generated slice
            // until Append. An empty occurrence must not create Some(empty)
            // in the compatibility DTO, or erase an earlier nonempty list.
            if field.is_list() && matches!(&canonical.value, Value::Seq(items) if items.is_empty())
            {
                continue;
            }
            if let Some((_, previous)) = fields.iter_mut().find(|(k, _)| k.text() == field.name()) {
                if !field.is_list() && !field.is_map() && matches!(field.kind(), Kind::Message(_)) {
                    *previous = canonical;
                } else {
                    merge(previous, canonical, field.is_list(), field.is_map());
                }
            } else {
                fields.push((canonical_key, canonical));
            }
        }
        out.value = Value::Map(fields);
        out
    }
    fn field(&mut self, node: &Node, field: &FieldDescriptor) -> Node {
        if field.is_map() {
            let mut out = node.clone();
            out.anchor.clear();
            if !self.kind(node, "mapping") {
                out.empty_map();
                return out;
            }
            let Kind::Message(entry) = field.kind() else {
                unreachable!()
            };
            let key_field = entry.get_field(1).expect("map key");
            let value_field = entry.get_field(2).expect("map value");
            let Value::Map(items) = &node.value else {
                unreachable!()
            };
            let mut normalized: Vec<(Node, Node)> = Vec::new();
            for (key, value) in items {
                let k = self.scalar(key, &key_field, true);
                let v = self.single(value, &value_field);
                if let Some((_, previous)) = normalized
                    .iter_mut()
                    .find(|(old, _)| old.text() == k.text())
                {
                    *previous = v;
                } else {
                    normalized.push((k, v));
                }
            }
            out.value = Value::Map(normalized);
            return out;
        }
        if field.is_list() {
            let mut out = node.clone();
            out.anchor.clear();
            if !self.kind(node, "sequence") {
                out.value = Value::Seq(Vec::new());
                return out;
            }
            let Value::Seq(items) = &node.value else {
                unreachable!()
            };
            out.value = Value::Seq(items.iter().map(|v| self.single(v, field)).collect());
            return out;
        }
        self.single(node, field)
    }
    fn single(&mut self, node: &Node, field: &FieldDescriptor) -> Node {
        if let Kind::Message(sub) = field.kind() {
            self.message(node, &sub)
        } else {
            self.scalar(node, field, false)
        }
    }
    fn scalar(&mut self, node: &Node, field: &FieldDescriptor, for_key: bool) -> Node {
        let mut out = node.clone();
        out.anchor.clear();
        if !self.kind(node, "scalar") {
            return out;
        }
        let value = node.text();
        match field.kind() {
            Kind::String => string(&mut out, value.into()),
            Kind::Bool => {
                if value != "true" && value != "false" {
                    self.error(node, format!("expected bool, got {}", quote(value)));
                } else if !for_key && !matches!(&node.value,Value::Scalar(_,_,tag) if tag=="!!bool")
                {
                    let tag = match &node.value {
                        Value::Scalar(_, _, tag) => tag.as_str(),
                        _ => "",
                    };
                    self.error(node, format!("expected tag !!bool, got {tag}"));
                }
                out.value = Value::Scalar(value.into(), ScalarStyle::Plain, "!!bool".into());
                out.anchor.clear();
                out.explicit_tag = None;
            }
            Kind::Enum(en) => {
                let names = || {
                    choices(
                        en.enum_descriptor_proto()
                            .value
                            .iter()
                            .filter_map(|v| v.name.clone()),
                    )
                };
                if let Some(v) = en.get_value_by_name(value) {
                    number(&mut out, v.number().to_string());
                } else if let Ok(v) = integer(value, false) {
                    if let Ok(v) = i32::try_from(v) {
                        number(&mut out, v.to_string());
                    } else {
                        self.error(
                            node,
                            format!(
                                "expected int32 for {}, got int64, expected one of {}",
                                field.full_name(),
                                names()
                            ),
                        );
                    }
                } else {
                    self.error(
                        node,
                        format!(
                            "unknown enum value {}, expected one of {}",
                            quote(value),
                            names()
                        ),
                    );
                }
            }
            Kind::Int32
            | Kind::Sint32
            | Kind::Sfixed32
            | Kind::Int64
            | Kind::Sint64
            | Kind::Sfixed64
            | Kind::Uint32
            | Kind::Fixed32
            | Kind::Uint64
            | Kind::Fixed64 => match integer(value, true) {
                Ok(n) => {
                    let (min, max) = match field.kind() {
                        Kind::Int32 | Kind::Sint32 | Kind::Sfixed32 => {
                            (i32::MIN as i128, i32::MAX as i128)
                        }
                        Kind::Uint32 | Kind::Fixed32 => (0, u32::MAX as i128),
                        Kind::Uint64 | Kind::Fixed64 => (0, u64::MAX as i128),
                        _ => (i64::MIN as i128, i64::MAX as i128),
                    };
                    if n < min {
                        self.error(node, format!("integer is too small: < {min}"));
                    } else if n > max {
                        self.error(node, format!("integer is too large: > {max}"));
                    }
                    number(&mut out, n.to_string());
                }
                Err(e) => self.error(node, format!("invalid integer: {e}")),
            },
            Kind::Double | Kind::Float => {
                match super::float::parse(value, matches!(field.kind(), Kind::Float)) {
                    Ok(v) => {
                        out.value = Value::Float(v.to_bits());
                        out.anchor.clear();
                        out.explicit_tag = None;
                    }
                    Err(error) => self.error(
                        node,
                        format!(
                            "invalid float: strconv.ParseFloat: parsing {}: {}",
                            quote(value),
                            error.message()
                        ),
                    ),
                }
            }
            Kind::Bytes => {
                string(&mut out, value.into());
            }
            Kind::Message(_) => unreachable!(),
        }
        out
    }
}
fn merge(previous: &mut Node, new: Node, list: bool, message: bool) {
    if list {
        if let (Value::Seq(old), Value::Seq(new)) = (&mut previous.value, new.value) {
            old.extend(new);
        }
        return;
    }
    if message && let (Value::Map(old), Value::Map(new_fields)) = (&mut previous.value, &new.value)
    {
        for (key, value) in new_fields {
            if let Some((_, old_value)) = old.iter_mut().find(|(k, _)| k.text() == key.text()) {
                *old_value = value.clone();
            } else {
                old.push((key.clone(), value.clone()));
            }
        }
        return;
    }
    *previous = new;
}
// Exact integer path stays integer; float notation is only the source fallback
// and is bounded below 2^53 before accepting an integral value.
fn integer(value: &str, allow_bytes: bool) -> Result<i128, String> {
    let (negative, raw) = value
        .strip_prefix('-')
        .map_or((false, value), |s| (true, s));
    let (radix, digits) = if raw.starts_with("0x") || raw.starts_with("0X") {
        (16, &raw[2..])
    } else if raw.starts_with("0o") || raw.starts_with("0O") {
        (8, &raw[2..])
    } else if raw.starts_with("0b") || raw.starts_with("0B") {
        (2, &raw[2..])
    } else {
        (10, raw)
    };
    if let Ok(n) = u64::from_str_radix(digits, radix) {
        return Ok(if negative { -(n as i128) } else { n as i128 });
    }
    if let Ok(n) = raw.parse::<f64>() {
        if n.is_finite() && n >= 0.0 && n.fract() == 0.0 && n < (1u64 << 53) as f64 {
            return Ok(if negative { -(n as i128) } else { n as i128 });
        }
        if n.is_finite() && n >= 0.0 {
            return Err("precision loss".into());
        }
    }
    if allow_bytes
        && (raw.starts_with('.') || raw.as_bytes().first().is_some_and(u8::is_ascii_digit))
    {
        let total = byte_literal(raw)?;
        return Ok(if negative {
            -(total as i128)
        } else {
            total as i128
        });
    }
    Err("invalid number, expected digit".into())
}

// Fixed protoyaml parseNumberWithUnit + leadingFrac semantics. The source
// consumes every decimal digit but stops extending numerator AND decimal scale
// after the next digit would exceed 2^63. Retaining only that source prefix is
// deliberate; interpreting the whole tail as a new exact rational would differ.
fn byte_literal(raw: &str) -> Result<u64, String> {
    let bytes = raw.as_bytes();
    let mut at = 0usize;
    let mut whole = 0u64;
    while let Some(digit) = bytes.get(at).filter(|c| c.is_ascii_digit()) {
        // Match fixed leadingInt's uint64 arithmetic and overflow predicate.
        let next = whole.wrapping_mul(10).wrapping_add((*digit - b'0') as u64);
        if next < whole {
            return Err("integer overflow".into());
        }
        whole = next;
        at += 1;
    }
    let pre = at != 0;
    let mut fraction = 0u64;
    let mut scale_digits = 0usize;
    let mut post = false;
    if bytes.get(at) == Some(&b'.') {
        at += 1;
        let mut overflow = false;
        while let Some(digit) = bytes.get(at).filter(|c| c.is_ascii_digit()) {
            post = true;
            if !overflow {
                if fraction > (i64::MAX as u64) / 10 {
                    overflow = true;
                } else {
                    let next = fraction * 10 + (*digit - b'0') as u64;
                    if next > 1u64 << 63 {
                        overflow = true;
                    } else {
                        fraction = next;
                        scale_digits += 1;
                    }
                }
            }
            at += 1;
        }
    }
    if !pre && !post {
        return Err("invalid number, expected digit".into());
    }
    let unit_start = at;
    while let Some(c) = bytes.get(at) {
        if *c == b'.' || *c == b'-' || c.is_ascii_digit() {
            break;
        }
        at += 1;
    }
    const UNITS: [(&str, u128); 12] = [
        ("k", 1000),
        ("Ki", 1 << 10),
        ("M", 1_000_000),
        ("Mi", 1 << 20),
        ("G", 1_000_000_000),
        ("Gi", 1 << 30),
        ("T", 1_000_000_000_000),
        ("Ti", 1 << 40),
        ("P", 1_000_000_000_000_000),
        ("Pi", 1 << 50),
        ("E", 1_000_000_000_000_000_000),
        ("Ei", 1 << 60),
    ];
    let Some((_, unit)) = UNITS.iter().find(|(u, _)| *u == &raw[unit_start..at]) else {
        return Err(
            "invalid bytes: unknown unit, expected one of [k M G T P E Ki Mi Gi Ti Pi Ei]".into(),
        );
    };
    if at != bytes.len() {
        // parseUintLiteral returns its original ParseUint error when parseBytes
        // succeeded but left unconsumed input. No suffix is silently ignored.
        return Err(format!(
            "strconv.ParseUint: parsing {}: invalid syntax",
            quote(raw)
        ));
    }
    // u64 whole × unit<=2^60 and fraction<=2^63 × unit<=2^60
    // both fit u128. Repeated division is exact floor(product/10^digits).
    // Once zero, any further (possibly arbitrarily many) zeros remain zero;
    // there is no fixed exponent limit and no allocated big-integer dependency.
    let mut fractional_bytes = (fraction as u128) * unit;
    for _ in 0..scale_digits {
        fractional_bytes /= 10;
        if fractional_bytes == 0 {
            break;
        }
    }
    let total = (whole as u128) * unit + fractional_bytes;
    u64::try_from(total).map_err(|_| "integer is too large".into())
}

#[cfg(test)]
mod byte_literal_source_tests {
    use super::{byte_literal, integer};

    #[test]
    fn unit_requires_a_real_decimal_digit() {
        for input in [".k", ".Ki"] {
            assert_eq!(
                integer(input, true).unwrap_err(),
                "invalid number, expected digit"
            );
        }
        assert_eq!(integer(".1k", true).unwrap(), 100);
        assert_eq!(integer("1.k", true).unwrap(), 1000);
    }
    #[test]
    fn arbitrarily_long_zero_fraction_is_consumed_without_exponent_overflow() {
        for length in [40, 128, 4096] {
            assert_eq!(
                integer(&format!("1.{}k", "0".repeat(length)), true).unwrap(),
                1000
            );
            assert_eq!(
                integer(&format!("0.{}Ki", "0".repeat(length)), true).unwrap(),
                0
            );
            assert_eq!(
                integer(&format!("-1.{}k", "0".repeat(length)), true).unwrap(),
                -1000
            );
        }
    }
    #[test]
    fn fraction_overflow_stops_source_prefix_but_still_consumes_tail() {
        assert_eq!(
            byte_literal("0.123456789012345678901234567890k").unwrap(),
            123
        );
        assert_eq!(
            byte_literal("0.999999999999999999999999999999k").unwrap(),
            999
        );
        assert_eq!(
            byte_literal("0.0000000000000000000000000000000000000001Ei").unwrap(),
            0
        );
    }
    #[test]
    fn source_floor_and_unsigned_range_are_not_rounded_up_or_saturated() {
        assert_eq!(byte_literal("0.0001k").unwrap(), 0);
        assert_eq!(byte_literal("15Ei").unwrap(), 15u64 << 60);
        assert_eq!(byte_literal("16Ei").unwrap_err(), "integer is too large");
        assert!(byte_literal("1k2").unwrap_err().contains("invalid syntax"));
        assert!(byte_literal("1..k").unwrap_err().contains("unknown unit"));
    }
}
