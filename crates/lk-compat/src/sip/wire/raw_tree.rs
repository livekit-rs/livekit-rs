//! Exact fixed generated-Go unknown value bytes, independent of semantic reflection.
//! Go canonicalizes the leading unknown tag, retaining the complete value span.
use prost::Message;
use prost_reflect::{
    DynamicMessage, FieldDescriptor, Kind, MapKey, MessageDescriptor, ReflectMessage, Value,
};
use std::collections::BTreeMap;
use std::ops::Range;

#[derive(Clone, Debug, Default)]
pub(super) struct Tree {
    unknown: Vec<u8>,
    children: BTreeMap<u32, Children>,
    oneofs: BTreeMap<String, u32>,
}
#[derive(Clone, Debug)]
enum Children {
    Singular(Box<Tree>),
    Repeated(Vec<Tree>),
    Map(BTreeMap<MapKey, Tree>),
}
pub(super) type Result<T> = std::result::Result<T, &'static str>;
impl Tree {
    pub(super) fn unknown_wire(&self) -> &[u8] {
        &self.unknown
    }
    pub(super) fn parse(desc: &MessageDescriptor, bytes: &[u8]) -> Result<Self> {
        Self::parse_depth(desc, bytes, 100)
    }
    fn parse_depth(desc: &MessageDescriptor, bytes: &[u8], depth: usize) -> Result<Self> {
        let mut out = Self::default();
        out.parse_in_place(desc, bytes, depth)?;
        Ok(out)
    }
    // Singular protobuf messages merge in wire order. Apply every occurrence to
    // the existing subtree: a later occurrence can switch a oneof away and back,
    // which must discard the earlier branch even when its final tag is unchanged.
    fn parse_in_place(
        &mut self,
        desc: &MessageDescriptor,
        bytes: &[u8],
        depth: usize,
    ) -> Result<()> {
        if depth == 0 {
            return Err("protobuf provenance recursion limit");
        }
        let out = self;
        let mut at = 0;
        while at < bytes.len() {
            let f = scan(bytes, &mut at, depth)?;
            let descriptor = desc.get_field(f.number);
            let known = descriptor.as_ref().is_some_and(|d| wire_matches(d, f.wire));
            if !known {
                put_varint(
                    &mut out.unknown,
                    (u64::from(f.number) << 3) | u64::from(f.wire),
                );
                out.unknown.extend_from_slice(&bytes[f.value_start..f.end]);
                continue;
            }
            let field = descriptor.unwrap();
            if let Some(oneof) = field.containing_oneof() {
                let name = oneof.full_name().to_owned();
                if let Some(old) = out.oneofs.insert(name, f.number)
                    && old != f.number
                {
                    out.children.remove(&old);
                }
            }
            let Kind::Message(message) = field.kind() else {
                continue;
            };
            let Some(payload) = f.payload else { continue };
            let payload = &bytes[payload];
            if field.is_map() {
                let known_entry = lk_protocol::wire::filter_known(message.full_name(), payload)
                    .map_err(|_| "invalid map known projection")?;
                let entry = DynamicMessage::decode(message.clone(), known_entry.as_slice())
                    .map_err(|_| "invalid map provenance")?;
                let key = map_key(
                    entry
                        .get_field_by_number(1)
                        .ok_or("missing map key descriptor")?
                        .as_ref(),
                )
                .ok_or("unsupported map key")?;
                let value_desc = message.get_field(2).ok_or("missing map value descriptor")?;
                if matches!(value_desc.kind(), Kind::Message(_)) {
                    let mut entry_tree = Self::parse_depth(&message, payload, depth - 1)?;
                    let child = match entry_tree.children.remove(&2) {
                        Some(Children::Singular(v)) => *v,
                        _ => Self::default(),
                    };
                    let children = out
                        .children
                        .entry(f.number)
                        .or_insert_with(|| Children::Map(BTreeMap::new()));
                    if let Children::Map(map) = children {
                        map.insert(key, child);
                    } else {
                        return Err("map provenance shape mismatch");
                    }
                }
            } else if field.is_list() {
                let child = Self::parse_depth(&message, payload, depth - 1)?;
                let children = out
                    .children
                    .entry(f.number)
                    .or_insert_with(|| Children::Repeated(Vec::new()));
                if let Children::Repeated(values) = children {
                    values.push(child);
                } else {
                    return Err("repeated provenance shape mismatch");
                }
            } else {
                match out.children.entry(f.number) {
                    std::collections::btree_map::Entry::Vacant(e) => {
                        let child = Self::parse_depth(&message, payload, depth - 1)?;
                        e.insert(Children::Singular(Box::new(child)));
                    }
                    std::collections::btree_map::Entry::Occupied(mut e) => {
                        if let Children::Singular(old) = e.get_mut() {
                            old.parse_in_place(&message, payload, depth - 1)?;
                        } else {
                            return Err("singular provenance shape mismatch");
                        }
                    }
                }
            }
        }
        Ok(())
    }
    pub(super) fn nested(&self, field: u32) -> Self {
        match self.children.get(&field) {
            Some(Children::Singular(v)) => (**v).clone(),
            _ => Self::default(),
        }
    }
    pub(super) fn repeated(&self, field: u32) -> Vec<Self> {
        match self.children.get(&field) {
            Some(Children::Repeated(values)) => values.clone(),
            _ => Vec::new(),
        }
    }
    pub(super) fn replace_nested(&mut self, field: u32, value: Option<Self>) {
        if let Some(value) = value {
            self.children
                .insert(field, Children::Singular(Box::new(value)));
        } else {
            self.children.remove(&field);
        }
    }
    pub(super) fn replace_repeated(&mut self, field: u32, values: Vec<Self>) {
        self.children.insert(field, Children::Repeated(values));
    }
    pub(super) fn retain_for(&mut self, message: &DynamicMessage) {
        self.children.retain(|number, children| {
            let Some(field) = message.descriptor().get_field(*number) else {
                return false;
            };
            if !message.has_field(&field) {
                return false;
            }
            match (children, message.get_field(&field).as_ref()) {
                (Children::Singular(tree), Value::Message(value)) => tree.retain_for(value),
                (Children::Repeated(trees), Value::List(values)) => {
                    trees.truncate(values.len());
                    for (tree, value) in trees.iter_mut().zip(values) {
                        if let Value::Message(value) = value {
                            tree.retain_for(value)
                        }
                    }
                }
                (Children::Map(trees), Value::Map(values)) => {
                    trees.retain(|key, tree| {
                        if let Some(Value::Message(value)) = values.get(key) {
                            tree.retain_for(value);
                            true
                        } else {
                            false
                        }
                    });
                }
                _ => return false,
            }
            true
        });
    }
    pub(super) fn encode(&self, message: &DynamicMessage) -> Vec<u8> {
        self.encode_depth(message, 100)
    }
    fn encode_depth(&self, message: &DynamicMessage, depth: usize) -> Vec<u8> {
        assert!(depth > 0, "validated message recursion bound");
        let mut primitive = message.clone();
        primitive.take_unknown_fields().for_each(drop);
        let mut nested = Vec::new();
        for (field, value) in message.fields() {
            let Kind::Message(entry) = field.kind() else {
                continue;
            };
            primitive.clear_field(&field);
            let number = field.number();
            if field.is_map() {
                let Value::Map(values) = value else {
                    unreachable!()
                };
                for (key, value) in values {
                    let mut wire = DynamicMessage::new(entry.clone());
                    wire.set_field_by_number(1, key_value(key));
                    if let Value::Message(value) = value {
                        let tree = match self.children.get(&number) {
                            Some(Children::Map(trees)) => trees.get(key),
                            _ => None,
                        };
                        let payload = tree
                            .cloned()
                            .unwrap_or_default()
                            .encode_depth(value, depth - 1);
                        let mut entry_bytes = wire.encode_to_vec();
                        put_message(&mut entry_bytes, 2, &payload, false);
                        put_message(&mut nested, number, &entry_bytes, false);
                    } else {
                        wire.set_field_by_number(2, value.clone());
                        put_message(&mut nested, number, &wire.encode_to_vec(), false);
                    }
                }
            } else if field.is_list() {
                let Value::List(values) = value else {
                    unreachable!()
                };
                for (i, value) in values.iter().enumerate() {
                    let Value::Message(value) = value else {
                        unreachable!()
                    };
                    let tree = match self.children.get(&number) {
                        Some(Children::Repeated(trees)) => trees.get(i),
                        _ => None,
                    };
                    let bytes = tree
                        .cloned()
                        .unwrap_or_default()
                        .encode_depth(value, depth - 1);
                    put_message(&mut nested, number, &bytes, field.is_group());
                }
            } else {
                let Value::Message(value) = value else {
                    unreachable!()
                };
                let bytes = self.nested(number).encode_depth(value, depth - 1);
                put_message(&mut nested, number, &bytes, field.is_group());
            }
        }
        let mut output = primitive.encode_to_vec();
        output.extend(nested);
        output.extend_from_slice(&self.unknown);
        output
    }
}
fn key_value(key: &MapKey) -> Value {
    match key {
        MapKey::Bool(v) => Value::Bool(*v),
        MapKey::I32(v) => Value::I32(*v),
        MapKey::I64(v) => Value::I64(*v),
        MapKey::U32(v) => Value::U32(*v),
        MapKey::U64(v) => Value::U64(*v),
        MapKey::String(v) => Value::String(v.clone()),
    }
}
fn map_key(value: &Value) -> Option<MapKey> {
    Some(match value {
        Value::Bool(v) => MapKey::Bool(*v),
        Value::I32(v) => MapKey::I32(*v),
        Value::I64(v) => MapKey::I64(*v),
        Value::U32(v) => MapKey::U32(*v),
        Value::U64(v) => MapKey::U64(*v),
        Value::String(v) => MapKey::String(v.clone()),
        _ => return None,
    })
}
fn wire_matches(field: &FieldDescriptor, wire: u8) -> bool {
    if field.is_group() {
        return wire == 3;
    }
    if field.is_list()
        && !matches!(field.kind(), Kind::String | Kind::Bytes | Kind::Message(_))
        && wire == 2
    {
        return true;
    }
    wire == match field.kind() {
        Kind::Double | Kind::Fixed64 | Kind::Sfixed64 => 1,
        Kind::Float | Kind::Fixed32 | Kind::Sfixed32 => 5,
        Kind::String | Kind::Bytes | Kind::Message(_) => 2,
        _ => 0,
    }
}
struct Field {
    number: u32,
    wire: u8,
    value_start: usize,
    end: usize,
    payload: Option<Range<usize>>,
}
fn scan(bytes: &[u8], at: &mut usize, depth: usize) -> Result<Field> {
    let key = read_varint(bytes, at)?;
    let number = key >> 3;
    let wire = (key & 7) as u8;
    if number == 0 || number > 0x1fff_ffff {
        return Err("invalid field number");
    }
    let value_start = *at;
    let payload = skip(bytes, at, number as u32, wire, depth)?;
    Ok(Field {
        number: number as u32,
        wire,
        value_start,
        end: *at,
        payload,
    })
}
fn read_varint(bytes: &[u8], at: &mut usize) -> Result<u64> {
    let mut n = 0;
    for shift in (0..70).step_by(7) {
        let b = *bytes.get(*at).ok_or("truncated varint")?;
        *at += 1;
        if shift == 63 && b > 1 {
            return Err("invalid varint");
        }
        n |= u64::from(b & 127) << shift;
        if b < 128 {
            return Ok(n);
        }
    }
    Err("invalid varint")
}
fn advance(bytes: &[u8], at: &mut usize, n: usize) -> Result<()> {
    *at = at
        .checked_add(n)
        .filter(|v| *v <= bytes.len())
        .ok_or("truncated field")?;
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
            read_varint(bytes, at)?;
        }
        1 => advance(bytes, at, 8)?,
        2 => {
            let len = usize::try_from(read_varint(bytes, at)?).map_err(|_| "invalid length")?;
            let start = *at;
            advance(bytes, at, len)?;
            return Ok(Some(start..*at));
        }
        3 => {
            if depth == 0 {
                return Err("group recursion limit");
            }
            let start = *at;
            loop {
                let end_start = *at;
                let key = read_varint(bytes, at)?;
                let tag = key >> 3;
                let kind = (key & 7) as u8;
                if tag == 0 || tag > 0x1fff_ffff {
                    return Err("invalid group tag");
                }
                if kind == 4 {
                    if tag != u64::from(number) {
                        return Err("mismatched group");
                    }
                    return Ok(Some(start..end_start));
                }
                skip(bytes, at, tag as u32, kind, depth - 1)?;
            }
        }
        5 => advance(bytes, at, 4)?,
        _ => return Err("invalid wire type"),
    }
    Ok(None)
}
fn put_varint(out: &mut Vec<u8>, mut n: u64) {
    while n >= 128 {
        out.push((n as u8 & 127) | 128);
        n >>= 7;
    }
    out.push(n as u8)
}
fn put_message(out: &mut Vec<u8>, number: u32, bytes: &[u8], group: bool) {
    put_varint(out, (u64::from(number) << 3) | if group { 3 } else { 2 });
    if !group {
        put_varint(out, bytes.len() as u64);
    }
    out.extend_from_slice(bytes);
    if group {
        put_varint(out, (u64::from(number) << 3) | 4);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sip::wire::PreservedMessage;
    use lk_protocol::livekit as pb;
    use prost::Name;
    #[test]
    fn fixed_go_unknown_tag_canonicalization_does_not_canonicalize_values_or_reorder() {
        let vectors = [
            (vec![0xa0, 6, 0x81, 0], vec![0xa0, 6, 0x81, 0]),
            (vec![0xa0, 0x86, 0, 7], vec![0xa0, 6, 7]),
            (vec![0xba, 6, 0x81, 0, b'X'], vec![0xba, 6, 0x81, 0, b'X']),
            (
                vec![0xb0, 6, 3, 0xa0, 6, 1, 0xb0, 6, 4, 0xa8, 6, 2, 0xa0, 6, 5],
                vec![0xb0, 6, 3, 0xa0, 6, 1, 0xb0, 6, 4, 0xa8, 6, 2, 0xa0, 6, 5],
            ),
            (
                vec![0xc3, 6, 0x10, 0x81, 0, 0xc4, 6],
                vec![0xc3, 6, 0x10, 0x81, 0, 0xc4, 6],
            ),
        ];
        for (input, expected) in vectors {
            let mut bytes = pb::SipInboundTrunkInfo {
                numbers: vec!["+123".into()],
                ..Default::default()
            }
            .encode_to_vec();
            bytes.extend(input);
            let source =
                PreservedMessage::decode(&pb::SipInboundTrunkInfo::full_name(), &bytes).unwrap();
            let mut known: pb::SipInboundTrunkInfo = source.project();
            known.name = "patch".into();
            known.numbers.clear();
            let output = source.overlay(&known).encode_to_vec();
            assert!(output.ends_with(&expected));
            let known = pb::SipInboundTrunkInfo::decode(output.as_slice()).unwrap();
            assert_eq!(known.name, "patch");
            assert!(known.numbers.is_empty());
        }
    }
    #[test]
    fn duplicate_known_singular_message_merges_unknown_subtrees_in_wire_order() {
        let mut a = pb::SipMediaConfig {
            encryption: Some(1),
            ..Default::default()
        }
        .encode_to_vec();
        a.extend([0xa0, 6, 0x81, 0]);
        let mut b = pb::SipMediaConfig {
            encryption: Some(2),
            ..Default::default()
        }
        .encode_to_vec();
        b.extend([0xa0, 6, 0x82, 0]);
        let mut input = Vec::new();
        put_message(&mut input, 20, &a, false);
        put_message(&mut input, 20, &b, false);
        let wire = PreservedMessage::decode(&pb::SipInboundTrunkInfo::full_name(), &input).unwrap();
        let child = wire.nested("media").unwrap();
        let encoded = child.encode_to_vec();
        assert!(encoded.ends_with(&[0xa0, 6, 0x81, 0, 0xa0, 6, 0x82, 0]));
        assert_eq!(child.project::<pb::SipMediaConfig>().encryption, Some(2));
    }
    #[test]
    fn duplicate_map_key_is_last_wins_but_duplicate_value_messages_merge_and_entry_unknown_drops() {
        let child = |identity: &str, marker: u8| {
            let mut value = Vec::new();
            put_message(&mut value, 1, identity.as_bytes(), false);
            value.extend([0xa0, 6, marker | 128, 0]);
            value
        };
        let a = child("old", 1);
        let b = child("first-in-last-entry", 2);
        let c = child("last", 3);
        let mut entry1 = vec![8, 7];
        put_message(&mut entry1, 2, &a, false);
        let mut entry2 = vec![8, 7];
        put_message(&mut entry2, 2, &b, false);
        put_message(&mut entry2, 2, &c, false);
        entry2.extend([0xa8, 8, 40]); // unknown entry field133
        let mut input = Vec::new();
        put_message(&mut input, 1, &entry1, false);
        put_message(&mut input, 1, &entry2, false);
        let wire =
            PreservedMessage::decode(&pb::DataTrackSubscriberHandles::full_name(), &input).unwrap();
        let bytes = wire.encode_to_vec();
        assert!(!bytes.windows(4).any(|v| v == [0xa0, 6, 0x81, 0]));
        assert!(!bytes.windows(3).any(|v| v == [0xa8, 8, 40]));
        assert!(
            bytes
                .windows(8)
                .any(|v| v == [0xa0, 6, 0x82, 0, 0xa0, 6, 0x83, 0])
        );
        let known = pb::DataTrackSubscriberHandles::decode(bytes.as_slice()).unwrap();
        assert_eq!(known.sub_handles.len(), 1);
        assert_eq!(
            known.sub_handles.get(&7).unwrap().publisher_identity,
            "last"
        );
    }
    #[test]
    fn oneof_replacement_cannot_retain_a_hidden_old_raw_tree() {
        let direct = pb::SipDispatchRuleDirect {
            room_name: "old".into(),
            ..Default::default()
        };
        let mut raw = direct.encode_to_vec();
        raw.extend([0xa0, 6, 0x81, 0]);
        let child =
            PreservedMessage::decode(&pb::SipDispatchRuleDirect::full_name(), &raw).unwrap();
        let mut original = PreservedMessage::from_message(&pb::SipDispatchRule {
            rule: Some(pb::sip_dispatch_rule::Rule::DispatchRuleDirect(
                direct.clone(),
            )),
        });
        original.replace_nested("dispatch_rule_direct", Some(&child));
        let changed = pb::SipDispatchRule {
            rule: Some(pb::sip_dispatch_rule::Rule::DispatchRuleIndividual(
                pb::SipDispatchRuleIndividual {
                    room_prefix: "new".into(),
                    ..Default::default()
                },
            )),
        };
        let changed = original.overlay(&changed);
        let back = changed.overlay(&pb::SipDispatchRule {
            rule: Some(pb::sip_dispatch_rule::Rule::DispatchRuleDirect(direct)),
        });
        assert!(
            !back
                .encode_to_vec()
                .windows(4)
                .any(|v| v == [0xa0, 6, 0x81, 0])
        );
    }
    #[test]
    fn duplicate_singular_message_switch_away_and_back_discards_earlier_oneof_child() {
        // Fixed Go generated decoder: Direct(unknown100=7), then a second rule
        // message that switches Individual(empty) -> Direct(empty).
        let bytes = [0x12, 5, 0x0a, 3, 0xa0, 6, 7, 0x12, 4, 0x12, 0, 0x0a, 0];
        let wire = PreservedMessage::decode(&pb::SipDispatchRuleInfo::full_name(), &bytes).unwrap();
        let direct = wire
            .nested("rule")
            .unwrap()
            .nested("dispatch_rule_direct")
            .unwrap();
        assert_eq!(
            direct.encode_to_vec(),
            Vec::<u8>::new(),
            "sequential oneof switch must discard old child unknowns"
        );
    }
}
