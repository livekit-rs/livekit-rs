//! Request-owned protobuf provenance. Known fields are replaced, never merged
//! back from old wire bytes; unknowns follow the exact clone source of Go Apply.
use prost::{Message, Name};
use prost_reflect::{DynamicMessage, ReflectMessage, Value};
mod raw_tree;

#[derive(Debug, thiserror::Error)]
pub enum WireError {
    #[error("protobuf provenance decode failed")]
    Decode(#[from] prost::DecodeError),
    #[error("protobuf known projection failed")]
    KnownProjection(#[from] lk_protocol::wire::WireError),
    #[error("protobuf provenance traversal failed: {0}")]
    Traversal(&'static str),
}

#[derive(Clone, Debug)]
pub struct PreservedMessage {
    inner: DynamicMessage,
    raw: raw_tree::Tree,
}
impl PreservedMessage {
    pub fn decode(name: &str, bytes: &[u8]) -> Result<Self, WireError> {
        let descriptor = lk_protocol::json::descriptor_pool()
            .get_message_by_name(name)
            .expect("fixed generated SIP descriptor must exist");
        // Prost/reflection reject a legal Go unknown field when its number
        // collides with a known field of a different wire kind. The semantic
        // view contains known fields only; the raw tree owns every unknown.
        let known = lk_protocol::wire::filter_known(name, bytes)?;
        let inner = DynamicMessage::decode(descriptor.clone(), known.as_slice())?;
        let raw = raw_tree::Tree::parse(&descriptor, bytes).map_err(WireError::Traversal)?;
        Ok(Self { inner, raw })
    }
    pub fn from_message<T: Message + Name>(value: &T) -> Self {
        Self::decode(&T::full_name(), &value.encode_to_vec())
            .expect("fixed generated message descriptor must exist")
    }
    /// Unknown values at this message level only; child ownership stays nested.
    pub fn unknown_wire(&self) -> &[u8] {
        self.raw.unknown_wire()
    }
    pub fn as_dynamic(&self) -> &DynamicMessage {
        &self.inner
    }
    pub fn project<T: Message + Name + Default>(&self) -> T {
        lk_protocol::wire::decode(self.inner.encode_to_vec().as_slice())
            .expect("matching generated projection")
    }
    #[cfg(test)]
    pub(crate) fn test_unknown_fields_count(&self) -> usize {
        // These tests use independent unknown tags, so ordinary reflection can
        // inspect the actual emitted bytes. Do not inspect the known-only view.
        DynamicMessage::decode(self.inner.descriptor(), self.encode_to_vec().as_slice())
            .expect("test marker protobuf")
            .unknown_fields()
            .count()
    }
    pub fn nested(&self, field: &str) -> Option<Self> {
        let descriptor = self.inner.descriptor().get_field_by_name(field)?;
        if !self.inner.has_field(&descriptor) {
            return None;
        }
        match self.inner.get_field(&descriptor).as_ref() {
            Value::Message(inner) => Some(Self {
                inner: inner.clone(),
                raw: self.raw.nested(descriptor.number()),
            }),
            _ => None,
        }
    }
    /// Clone a repeated-message cohort with its corresponding per-element
    /// unknown bytes. Missing, scalar, map and singular fields return no values.
    pub fn repeated(&self, field: &str) -> Vec<Self> {
        let Some(descriptor) = self.inner.descriptor().get_field_by_name(field) else {
            return Vec::new();
        };
        if !descriptor.is_list() || descriptor.is_map() {
            return Vec::new();
        }
        let provenance = self.raw.repeated(descriptor.number());
        match self.inner.get_field(&descriptor).as_ref() {
            Value::List(values) => values
                .iter()
                .enumerate()
                .filter_map(|(index, value)| match value {
                    Value::Message(inner) => Some(Self {
                        inner: inner.clone(),
                        raw: provenance.get(index).cloned().unwrap_or_default(),
                    }),
                    _ => None,
                })
                .collect(),
            _ => Vec::new(),
        }
    }
    pub fn replace_nested(&mut self, field: &str, value: Option<&Self>) {
        let number = self
            .inner
            .descriptor()
            .get_field_by_name(field)
            .expect("fixed child field")
            .number();
        self.raw
            .replace_nested(number, value.map(|v| v.raw.clone()));
        if let Some(value) = value {
            self.inner
                .set_field_by_name(field, Value::Message(value.inner.clone()));
        } else {
            self.inner.clear_field_by_name(field);
        }
    }
    pub fn replace_repeated(&mut self, field: &str, values: impl IntoIterator<Item = Self>) {
        let number = self
            .inner
            .descriptor()
            .get_field_by_name(field)
            .expect("fixed repeated child field")
            .number();
        let values: Vec<_> = values.into_iter().collect();
        self.raw
            .replace_repeated(number, values.iter().map(|v| v.raw.clone()).collect());
        self.inner.set_field_by_name(
            field,
            Value::List(
                values
                    .into_iter()
                    .map(|v| Value::Message(v.inner))
                    .collect(),
            ),
        );
    }
    /// Overlay all known typed values while retaining unknowns from this clone
    /// source. Callers explicitly replace subtree provenance before this call.
    /// This recursive walk is bounded to prost's 100 nested-message levels.
    pub fn overlay<T: Message + Name>(&self, updated: &T) -> Self {
        let typed = Self::from_message(updated);
        let inner = overlay(&self.inner, &typed.inner, 100);
        let mut raw = self.raw.clone();
        raw.retain_for(&inner);
        Self { inner, raw }
    }
    pub fn encode_to_vec(&self) -> Vec<u8> {
        self.raw.encode(&self.inner)
    }
}
fn overlay(old: &DynamicMessage, new: &DynamicMessage, depth: usize) -> DynamicMessage {
    assert!(
        depth > 0,
        "decoded provenance must obey prost recursion bound"
    );
    assert_eq!(
        old.descriptor(),
        new.descriptor(),
        "provenance descriptor mismatch"
    );
    let mut result = old.clone();
    for field in new.descriptor().fields() {
        result.clear_field(&field);
        if new.has_field(&field) {
            let value = new.get_field(&field).into_owned();
            let value = if old.has_field(&field) {
                overlay_value(old.get_field(&field).as_ref(), value, depth - 1)
            } else {
                value
            };
            result.set_field(&field, value);
        }
    }
    result
}
fn overlay_value(old: &Value, new: Value, depth: usize) -> Value {
    match (old, new) {
        (Value::Message(old), Value::Message(new)) => Value::Message(overlay(old, &new, depth)),
        (Value::List(old), Value::List(new)) => Value::List(
            new.into_iter()
                .enumerate()
                .map(|(i, v)| {
                    old.get(i)
                        .map_or_else(|| v.clone(), |o| overlay_value(o, v.clone(), depth))
                })
                .collect(),
        ),
        (Value::Map(old), Value::Map(new)) => Value::Map(
            new.into_iter()
                .map(|(k, v)| {
                    let v = old
                        .get(&k)
                        .map_or_else(|| v.clone(), |o| overlay_value(o, v.clone(), depth));
                    (k, v)
                })
                .collect(),
        ),
        (_, new) => new,
    }
}

#[derive(Clone, Debug)]
pub struct Stored<T> {
    pub value: T,
    pub wire: PreservedMessage,
}
impl<T> std::ops::Deref for Stored<T> {
    type Target = T;
    fn deref(&self) -> &T {
        &self.value
    }
}
impl<T: Message + Name> Stored<T> {
    pub fn typed(value: T) -> Self {
        let wire = PreservedMessage::from_message(&value);
        Self { value, wire }
    }
    pub fn with_wire(value: T, source: PreservedMessage) -> Self {
        let wire = source.overlay(&value);
        Self { value, wire }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use lk_protocol::livekit as pb;
    fn seeded() -> PreservedMessage {
        // root field100=7; media field101=9.
        let mut value = pb::SipInboundTrunkInfo {
            sip_trunk_id: "ST_fixture".into(),
            numbers: vec!["+123".into()],
            media: Some(Default::default()),
            ..Default::default()
        }
        .encode_to_vec();
        value.extend([0xa0, 6, 7]);
        let mut wire =
            PreservedMessage::decode(&pb::SipInboundTrunkInfo::full_name(), &value).unwrap();
        let mut media = pb::SipMediaConfig::default().encode_to_vec();
        media.extend([0xa8, 6, 9]);
        wire.replace_nested(
            "media",
            Some(&PreservedMessage::decode(&pb::SipMediaConfig::full_name(), &media).unwrap()),
        );
        wire
    }
    #[test]
    fn unrelated_patch_preserves_root_and_child_without_resurrecting_cleared_values() {
        let source = seeded();
        let mut typed: pb::SipInboundTrunkInfo = source.project();
        typed.name = "updated".into();
        typed.numbers.clear();
        let actual = source.overlay(&typed);
        assert_eq!(actual.test_unknown_fields_count(), 1);
        assert_eq!(
            actual.nested("media").unwrap().test_unknown_fields_count(),
            1
        );
        assert!(
            actual
                .project::<pb::SipInboundTrunkInfo>()
                .numbers
                .is_empty()
        );
    }
    #[test]
    fn replacement_provenance_is_explicit_not_old_unknown_merge() {
        let old = seeded();
        let replacement = pb::SipInboundTrunkInfo {
            numbers: vec!["+456".into()],
            ..Default::default()
        };
        let mut replacement = replacement;
        replacement.sip_trunk_id = old.project::<pb::SipInboundTrunkInfo>().sip_trunk_id;
        let actual = PreservedMessage::from_message(&replacement).overlay(&replacement);
        assert_eq!(actual.test_unknown_fields_count(), 0);
        assert!(actual.nested("media").is_none());
        let mut source = old.clone();
        source.replace_nested(
            "media",
            Some(&PreservedMessage::from_message(
                &pb::SipMediaConfig::default(),
            )),
        );
        let typed: pb::SipInboundTrunkInfo = source.project();
        let actual = source.overlay(&typed);
        assert_eq!(actual.test_unknown_fields_count(), 1);
        assert_eq!(
            actual.nested("media").unwrap().test_unknown_fields_count(),
            0
        );
    }
}

#[cfg(test)]
mod provenance_tests {
    use super::*;
    use lk_protocol::livekit as pb;
    fn unknown<T: Message + Name>(value: &T, n: u8) -> PreservedMessage {
        let mut bytes = value.encode_to_vec();
        bytes.extend([0xa0, 6, n]);
        PreservedMessage::decode(&T::full_name(), &bytes).unwrap()
    }
    #[test]
    fn list_get_and_new_id_preserve_unknown_not_request_envelope() {
        let info = pb::SipOutboundTrunkInfo {
            address: "example.invalid".into(),
            numbers: vec!["+123".into()],
            ..Default::default()
        };
        let source = unknown(&info, 7);
        let mut assigned = info;
        assigned.sip_trunk_id = "ST_fixture".into();
        let record = Stored::with_wire(assigned, source);
        let mut get = PreservedMessage::from_message(&pb::GetSipOutboundTrunkResponse::default());
        get.replace_nested("trunk", Some(&record.wire));
        assert_eq!(get.nested("trunk").unwrap().test_unknown_fields_count(), 1);
        let mut list = PreservedMessage::from_message(&pb::ListSipOutboundTrunkResponse::default());
        list.replace_repeated("items", [record.wire]);
        let actual_list =
            DynamicMessage::decode(list.inner.descriptor(), list.encode_to_vec().as_slice())
                .unwrap();
        let Value::List(values) = actual_list.get_field_by_name("items").unwrap().into_owned()
        else {
            panic!("list")
        };
        let Value::Message(item) = &values[0] else {
            panic!("message")
        };
        assert_eq!(item.unknown_fields().count(), 1);
        assert_eq!(
            item.get_field_by_name("sip_trunk_id").unwrap().as_ref(),
            &Value::String("ST_fixture".into())
        );
    }
    #[test]
    fn nested_oneof_unknown_retained_only_for_surviving_message_nodes() {
        let direct = pb::SipDispatchRuleDirect {
            room_name: "room".into(),
            ..Default::default()
        };
        let mut rule = PreservedMessage::from_message(&pb::SipDispatchRule {
            rule: Some(pb::sip_dispatch_rule::Rule::DispatchRuleDirect(
                direct.clone(),
            )),
        });
        rule.replace_nested("dispatch_rule_direct", Some(&unknown(&direct, 9)));
        let mut typed: pb::SipDispatchRule = rule.project();
        if let Some(pb::sip_dispatch_rule::Rule::DispatchRuleDirect(v)) = typed.rule.as_mut() {
            v.room_name.clear();
        }
        let kept = rule.overlay(&typed);
        assert_eq!(
            kept.nested("dispatch_rule_direct")
                .unwrap()
                .test_unknown_fields_count(),
            1
        );
        let switched = pb::SipDispatchRule {
            rule: Some(pb::sip_dispatch_rule::Rule::DispatchRuleIndividual(
                pb::SipDispatchRuleIndividual {
                    room_prefix: "prefix".into(),
                    ..Default::default()
                },
            )),
        };
        let actual = rule.overlay(&switched);
        assert!(actual.nested("dispatch_rule_direct").is_none());
        assert_eq!(
            actual
                .nested("dispatch_rule_individual")
                .unwrap()
                .test_unknown_fields_count(),
            0
        );
    }
    #[test]
    fn legacy_conversions_create_fresh_provenance_instead_of_copying_unknown() {
        let legacy = pb::SipTrunkInfo {
            sip_trunk_id: "ST_fixture".into(),
            outbound_number: "+123".into(),
            ..Default::default()
        };
        let wire = unknown(&legacy, 7);
        assert_eq!(wire.test_unknown_fields_count(), 1);
        let converted = crate::sip::catalog::legacy_as_inbound(&wire.project()).unwrap();
        let fresh = Stored::typed(converted);
        assert_eq!(fresh.wire.test_unknown_fields_count(), 0);
    }
}

#[cfg(test)]
mod wrong_wire_tests {
    use super::*;
    use lk_protocol::livekit as pb;
    #[test]
    fn legal_known_number_wrong_wire_remains_unknown_on_root_and_nested_media() {
        // name#2 and media.codecs#2 are strings/messages, not varints. Go accepts
        // both as unknown. Keep literal bytes through projection and update.
        let input = [0x10, 7, 0xa2, 1, 2, 0x10, 7];
        let source =
            PreservedMessage::decode(&pb::SipInboundTrunkInfo::full_name(), &input).unwrap();
        let mut typed: pb::SipInboundTrunkInfo = source.project();
        assert!(typed.name.is_empty());
        assert!(typed.media.as_ref().unwrap().codecs.is_empty());
        assert_eq!(source.as_dynamic().unknown_fields().count(), 0);
        assert_eq!(source.nested("media").unwrap().encode_to_vec(), [0x10, 7]);
        typed.name = "updated".into();
        let updated = source.overlay(&typed);
        assert!(updated.encode_to_vec().ends_with(&[0x10, 7]));
        assert_eq!(updated.nested("media").unwrap().encode_to_vec(), [0x10, 7]);
        assert_eq!(updated.project::<pb::SipInboundTrunkInfo>().name, "updated");
    }
}

#[cfg(test)]
mod repeated_provenance_tests {
    use super::*;
    use lk_protocol::livekit as pb;
    #[test]
    fn repeated_children_keep_distinct_unknowns_and_replace_without_index_resurrection() {
        // Two actual SIPCodec messages with different future scalar markers.
        let source = PreservedMessage::decode(
            &pb::SipMediaConfig::full_name(),
            &[
                0x12, 6, 0x0a, 1, b'a', 0xa0, 6, 7, 0x12, 6, 0x0a, 1, b'b', 0xa0, 6, 8,
            ],
        )
        .unwrap();
        let values = source.repeated("codecs");
        assert_eq!(values.len(), 2);
        assert_eq!(values[0].project::<pb::SipCodec>().name, "a");
        assert!(values[0].encode_to_vec().ends_with(&[0xa0, 6, 7]));
        assert!(values[1].encode_to_vec().ends_with(&[0xa0, 6, 8]));
        assert!(source.repeated("encryption").is_empty());
        assert!(source.repeated("missing").is_empty());
        let mut destination = PreservedMessage::from_message(&pb::SipMediaConfig::default());
        destination.replace_repeated("codecs", [values[1].clone()]);
        let known: pb::SipMediaConfig = destination.project();
        let replaced = destination.overlay(&known);
        assert_eq!(replaced.repeated("codecs").len(), 1);
        assert!(
            replaced.repeated("codecs")[0]
                .encode_to_vec()
                .ends_with(&[0xa0, 6, 8])
        );
        destination.replace_repeated(
            "codecs",
            [PreservedMessage::from_message(&pb::SipCodec {
                name: "fresh".into(),
                ..Default::default()
            })],
        );
        assert_eq!(
            destination.repeated("codecs")[0].test_unknown_fields_count(),
            0
        );
    }
}
