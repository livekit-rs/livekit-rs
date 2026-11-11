//! Complete the generated response dialect's missing EmitUnpopulated message
//! nulls. Scalar/enum/64-bit formatting stays with the existing pbjson twin.
use prost_reflect::{Kind, MessageDescriptor};
use serde_json::Value;

use super::{Result, error, pool};

pub(crate) fn reflect_value(name: &str, bytes: &[u8]) -> Result<Value> {
    let descriptor = pool()
        .get_message_by_name(name)
        .ok_or_else(|| error("unknown response descriptor"))?;
    let message = prost_reflect::DynamicMessage::decode(descriptor, bytes).map_err(error)?;
    let mut serializer = serde_json::Serializer::new(Vec::new());
    message
        .serialize_with_options(
            &mut serializer,
            &prost_reflect::SerializeOptions::new()
                .use_proto_field_name(true)
                .skip_default_fields(false),
        )
        .map_err(error)?;
    serde_json::from_slice(&serializer.into_inner())
}

pub(crate) fn complete(name: &str, value: &mut Value) -> Result<()> {
    let descriptor = pool()
        .get_message_by_name(name)
        .ok_or_else(|| error("unknown response descriptor"))?;
    message(&descriptor, value, 0)
}
fn message(desc: &MessageDescriptor, value: &mut Value, depth: usize) -> Result<()> {
    if depth > 100 {
        return Err(error("response recursion limit"));
    }
    // Canonical WKT JSON is not its protobuf field object representation.
    if desc.full_name().starts_with("google.protobuf.") {
        return Ok(());
    }
    let Some(object) = value.as_object_mut() else {
        return Ok(());
    };
    for field in desc.fields() {
        let Kind::Message(inner) = field.kind() else {
            continue;
        };
        if field.is_map() {
            if let Some(values) = object.get_mut(field.name()).and_then(Value::as_object_mut) {
                if let Some(value_field) = inner.get_field(2) {
                    if let Kind::Message(value_desc) = value_field.kind() {
                        for value in values.values_mut() {
                            message(&value_desc, value, depth + 1)?;
                        }
                    }
                }
            }
        } else if field.is_list() {
            if let Some(values) = object.get_mut(field.name()).and_then(Value::as_array_mut) {
                for value in values {
                    message(&inner, value, depth + 1)?;
                }
            }
        } else if let Some(value) = object.get_mut(field.name()) {
            message(&inner, value, depth + 1)?;
        } else if field.containing_oneof().is_none() {
            // Go EmitUnpopulated emits null only for absent ordinary message
            // fields, never unset oneofs (including synthetic optional oneofs).
            object.insert(field.name().to_owned(), Value::Null);
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use crate::{livekit as pb, twirp_json::livekit as tj};
    #[test]
    fn ordinary_absent_messages_are_null_without_optional_or_oneof_fabrication() {
        let bytes =
            crate::to_twirp_json::<_, tj::SipInboundTrunkInfo>(&pb::SipInboundTrunkInfo::default())
                .unwrap();
        let value: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        for name in [
            "ringing_timeout",
            "max_call_duration",
            "media",
            "created_at",
            "updated_at",
        ] {
            assert_eq!(value.get(name), Some(&serde_json::Value::Null), "{name}");
        }
        let bytes = crate::to_twirp_json::<_, tj::CreateIngressRequest>(
            &pb::CreateIngressRequest::default(),
        )
        .unwrap();
        let value: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        assert!(!value.as_object().unwrap().contains_key("enabled"));
        assert!(
            !value
                .as_object()
                .unwrap()
                .contains_key("enable_transcoding")
        );
        assert_eq!(value.get("audio"), Some(&serde_json::Value::Null));
        assert_eq!(value.get("video"), Some(&serde_json::Value::Null));
        let bytes =
            crate::to_twirp_json::<_, tj::StartEgressRequest>(&pb::StartEgressRequest::default())
                .unwrap();
        let value: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        for name in ["template", "web", "media", "preset", "advanced"] {
            assert!(!value.as_object().unwrap().contains_key(name), "{name}");
        }
    }
    #[test]
    fn nested_lists_and_present_wkt_keep_real_presence_and_scalars() {
        let response = pb::ListSipInboundTrunkResponse {
            items: vec![pb::SipInboundTrunkInfo {
                ringing_timeout: Some(crate::well_known::Duration {
                    seconds: 1,
                    nanos: 500_000_000,
                }),
                ..Default::default()
            }],
        };
        let bytes = crate::to_twirp_json::<_, tj::ListSipInboundTrunkResponse>(&response).unwrap();
        let value: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(value["items"][0]["ringing_timeout"], "1.500s");
        assert_eq!(
            value["items"][0].get("media"),
            Some(&serde_json::Value::Null)
        );
        assert_eq!(value["items"][0]["sip_trunk_id"], "");
        let get = pb::GetSipInboundTrunkResponse { trunk: None };
        let bytes = crate::to_twirp_json::<_, tj::GetSipInboundTrunkResponse>(&get).unwrap();
        assert_eq!(
            serde_json::from_slice::<serde_json::Value>(&bytes).unwrap(),
            serde_json::json!({"trunk":null})
        );
    }
    #[test]
    fn unknown_enum_response_values_are_numbers_without_losing_known_names_or_nulls() {
        let info = pb::SipInboundTrunkInfo {
            include_headers: 999,
            headers: std::collections::HashMap::from([("X-Fixture".into(), "v".into())]),
            ..Default::default()
        };
        let bytes = crate::to_twirp_json::<_, tj::SipInboundTrunkInfo>(&info).unwrap();
        let value: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(value["include_headers"], 999);
        assert_eq!(value["headers"]["X-Fixture"], "v");
        assert_eq!(value["media_encryption"], "SIP_MEDIA_ENCRYPT_DISABLE");
        assert_eq!(value.get("ringing_timeout"), Some(&serde_json::Value::Null));
        for number in [i32::MIN, i32::MAX] {
            let info = pb::SipInboundTrunkInfo {
                include_headers: number,
                ..Default::default()
            };
            let bytes = crate::to_twirp_json::<_, tj::SipInboundTrunkInfo>(&info).unwrap();
            assert_eq!(
                serde_json::from_slice::<serde_json::Value>(&bytes).unwrap()["include_headers"],
                number
            );
        }
        let list = pb::ListSipInboundTrunkResponse {
            items: vec![
                info,
                pb::SipInboundTrunkInfo {
                    include_headers: 2,
                    ..Default::default()
                },
            ],
        };
        let bytes = crate::to_twirp_json::<_, tj::ListSipInboundTrunkResponse>(&list).unwrap();
        let value: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(value["items"][0]["include_headers"], 999);
        assert_eq!(value["items"][1]["include_headers"], "SIP_ALL_HEADERS");
        assert_eq!(
            value["items"][1].get("created_at"),
            Some(&serde_json::Value::Null)
        );
        let call = pb::SipCallInfo {
            enabled_features: vec![1, 999, -1],
            ..Default::default()
        };
        let bytes = crate::to_twirp_json::<_, tj::SipCallInfo>(&call).unwrap();
        assert_eq!(
            serde_json::from_slice::<serde_json::Value>(&bytes).unwrap()["enabled_features"],
            serde_json::json!(["KRISP_ENABLED", 999, -1])
        );
        let request = pb::CreateIngressRequest {
            input_type: 999,
            enabled: Some(false),
            ..Default::default()
        };
        let bytes = crate::to_twirp_json::<_, tj::CreateIngressRequest>(&request).unwrap();
        let value: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(value["enabled"], false);
        assert_eq!(value["input_type"], 999);
        assert!(
            !value
                .as_object()
                .unwrap()
                .contains_key("enable_transcoding")
        );
    }
    #[test]
    fn actual_go_unknown_enum_response_oracle_matches_shared_encoder() {
        use base64::Engine as _;
        use prost::Message as _;
        let oracle:serde_json::Value=serde_json::from_str(include_str!(concat!(env!("CARGO_MANIFEST_DIR"),"/../../tests/fixtures/upstream/evidence/2026-10-25/f04-protojson-response-enum-official-oracle.json"))).unwrap();
        let cases = oracle["cases"].as_array().unwrap();
        assert_eq!(cases.len(), 7);
        for case in cases {
            assert_eq!(case["decoded"], true);
            let wire = base64::engine::general_purpose::STANDARD
                .decode(case["wire_base64"].as_str().unwrap())
                .unwrap();
            macro_rules! encoded {
                ($a:ty,$b:ty) => {
                    crate::to_twirp_json::<$a, $b>(&<$a>::decode(wire.as_slice()).unwrap()).unwrap()
                };
            }
            let bytes = match case["type"].as_str().unwrap() {
                "livekit.SIPInboundTrunkInfo" => {
                    encoded!(pb::SipInboundTrunkInfo, tj::SipInboundTrunkInfo)
                }
                "livekit.ListSIPInboundTrunkResponse" => encoded!(
                    pb::ListSipInboundTrunkResponse,
                    tj::ListSipInboundTrunkResponse
                ),
                "livekit.SIPCallInfo" => encoded!(pb::SipCallInfo, tj::SipCallInfo),
                "livekit.CreateIngressRequest" => {
                    encoded!(pb::CreateIngressRequest, tj::CreateIngressRequest)
                }
                _ => panic!("unhandled actual oracle response type"),
            };
            let value: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
            assert_eq!(value, case["message"], "{}", case["case"]);
        }
    }
}

#[cfg(test)]
mod preserved_wire_tests {
    use crate::livekit as pb;
    use prost::{Message, Name};

    #[test]
    fn preserved_wire_keeps_unknown_binary_and_original_json_dialects() {
        let original = pb::SipOutboundTrunkInfo {
            sip_trunk_id: "ST_preserved".into(),
            transport: 999,
            ..Default::default()
        };
        let mut bytes = original.encode_to_vec();
        // Unknown field100, varint42. Generated prost decoding would discard it.
        bytes.extend_from_slice(&[0xa0, 0x06, 0x2a]);
        let dynamic =
            crate::json::decode_wire_dynamic(&pb::SipOutboundTrunkInfo::full_name(), &bytes)
                .unwrap();
        assert_eq!(dynamic.unknown_fields().count(), 1);
        let roundtrip = dynamic.encode_to_vec();
        let reparsed =
            crate::json::decode_wire_dynamic(&pb::SipOutboundTrunkInfo::full_name(), &roundtrip)
                .unwrap();
        assert_eq!(reparsed.unknown_fields().count(), 1);
        assert_eq!(
            pb::SipOutboundTrunkInfo::decode(roundtrip.as_slice()).unwrap(),
            original
        );
        let json: serde_json::Value =
            serde_json::from_slice(&crate::json::encode_twirp_dynamic(&dynamic).unwrap()).unwrap();
        assert_eq!(json["transport"], 999);
        assert_eq!(json["media"], serde_json::Value::Null);
        assert_eq!(json["sip_trunk_id"], "ST_preserved");
        assert!(json.get("100").is_none());
        let default_json: serde_json::Value =
            serde_json::from_slice(&crate::json::encode_default(&original).unwrap()).unwrap();
        assert_eq!(default_json["transport"], 999);
        assert_eq!(default_json["sipTrunkId"], "ST_preserved");
        assert!(default_json.get("media").is_none());
    }
}
