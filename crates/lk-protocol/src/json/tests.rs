use super::*;
use crate::livekit as pb;
use base64::Engine as _;

#[test]
fn actual_official_go_twenty_two_vectors_match_typed_semantics() {
    let inputs: serde_json::Value = serde_json::from_str(include_str!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../tests/oracles/protojson/cases.json"
    )))
    .unwrap();
    let oracle: serde_json::Value = serde_json::from_str(include_str!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../tests/fixtures/upstream/evidence/2026-10-25/f03-protojson-primary-wire-official-oracle.json"
    )))
    .unwrap();
    assert_eq!(inputs["cases"].as_array().unwrap().len(), 22);
    assert_eq!(oracle["cases"].as_array().unwrap().len(), 22);
    for (input, expected) in inputs["cases"]
        .as_array()
        .unwrap()
        .iter()
        .zip(oracle["cases"].as_array().unwrap())
    {
        assert_eq!(input["case"], expected["case"]);
        assert_eq!(input["type"], expected["type"]);
        let result = decode_dynamic(
            input["type"].as_str().unwrap(),
            input["body"].as_str().unwrap().as_bytes(),
        );
        assert_eq!(
            result.is_ok(),
            expected["decoded"].as_bool().unwrap(),
            "{}: actual decoded={} error={:?}",
            input["case"],
            result.is_ok(),
            result.as_ref().err()
        );
        if let Ok(message) = result {
            assert!(
                wire_canonical_message(&message, expected) == official_wire_message(expected),
                "typed protobuf fields/values/presence differ: {}",
                input["case"]
            );
        }
    }
}

#[test]
fn integer_normalizer_retains_all_64_bits_and_rejects_fraction_and_overflow() {
    for (raw, expected) in [
        ("9007199254740993", "9007199254740993"),
        ("9.007199254740993e15", "9007199254740993"),
        ("9223372036854775807", "9223372036854775807"),
        ("-9223372036854775808", "-9223372036854775808"),
        ("10e-1", "1"),
        ("1.000", "1"),
    ] {
        assert_eq!(integer(raw, true, 64).unwrap(), format!("\"{expected}\""));
    }
    assert_eq!(
        integer("18446744073709551615", false, 64).unwrap(),
        "\"18446744073709551615\""
    );
    for raw in [
        "9223372036854775808",
        "-9223372036854775809",
        "1e-1",
        "1.0001",
        "01",
        "+1",
        " 1",
        "1e9999999999",
    ] {
        assert!(integer(raw, true, 64).is_err(), "{raw}");
    }
    assert!(integer("18446744073709551616", false, 64).is_err());
    let room: pb::Room = decode(br#"{"creationTime":9.007199254740993e15}"#).unwrap();
    assert_eq!(room.creation_time, 9_007_199_254_740_993);
}

#[test]
fn generated_request_bridge_preserves_unknown_enum_and_null_presence() {
    let request: pb::CreateIngressRequest =
        decode(br#"{"inputType":12345,"enabled":null}"#).unwrap();
    assert_eq!(request.input_type, 12345);
    assert_eq!(request.enabled, None);
    let request: pb::CreateIngressRequest =
        decode(br#"{"inputType":"FUTURE_INPUT","enabled":false}"#).unwrap();
    assert_eq!(request.input_type, 0);
    assert_eq!(request.enabled, Some(false));
    let request: pb::CreateRoomRequest =
        decode(br#"{"name":null,"future":1,"future":2} ignored"#).unwrap();
    assert!(request.name.is_empty());
    for bytes in [
        &br#"{"name":null,"name":"x"}"#[..],
        &br#"{"emptyTimeout":null,"empty_timeout":2}"#[..],
    ] {
        assert!(decode::<pb::CreateRoomRequest>(bytes).is_err());
    }
    assert!(decode_dynamic("livekit.UnknownRequest", b"{}").is_err());
    assert!(decode::<pb::CreateRoomRequest>(b"null").is_err());
}

#[test]
fn actual_official_go_residual_forty_nine_vectors_match_typed_semantics() {
    let inputs: serde_json::Value = serde_json::from_str(include_str!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../tests/oracles/protojson/residual-cases.json"
    )))
    .unwrap();
    let oracle: serde_json::Value = serde_json::from_str(include_str!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../tests/fixtures/upstream/evidence/2026-10-25/f03-protojson-residual-wire-official-oracle.json"
    )))
    .unwrap();
    let inputs = inputs["cases"].as_array().unwrap();
    let oracle = oracle["cases"].as_array().unwrap();
    assert_eq!(inputs.len(), 49);
    assert_eq!(oracle.len(), 49);
    let mut mismatches = Vec::new();
    for (input, expected) in inputs.iter().zip(oracle) {
        assert_eq!(input["case"], expected["case"]);
        assert_eq!(input["type"], expected["type"]);
        let body = match (input.get("body"), input.get("body_base64")) {
            (Some(value), None) => value.as_str().unwrap().as_bytes().to_vec(),
            (None, Some(value)) => base64::engine::general_purpose::STANDARD
                .decode(value.as_str().unwrap())
                .unwrap(),
            _ => panic!("fixture must have exactly one body representation"),
        };
        let result = decode_dynamic(input["type"].as_str().unwrap(), &body);
        if result.is_ok() != expected["decoded"].as_bool().unwrap() {
            mismatches.push(format!(
                "{}: wanted decoded={}, actual decoded={} error={:?}",
                input["case"],
                expected["decoded"],
                result.is_ok(),
                result.as_ref().err()
            ));
            continue;
        }
        if let Ok(message) = result {
            if wire_canonical_message(&message, expected) != official_wire_message(expected) {
                mismatches.push(format!(
                    "{}: actual typed protobuf fields/values/presence differ from Go wire",
                    input["case"]
                ));
            }
        }
    }
    assert!(
        mismatches.is_empty(),
        "{} of 49 actual oracle cases differ:\n{}",
        mismatches.len(),
        mismatches.join("\n")
    );
}

// Match the actual public generated-type bridge: default proto3 scalar storage
// is not presence, while genuine optional/oneof presence remains on the wire.
// The separate 24-zero and optional-false tests verify this boundary directly.
fn wire_canonical_message(actual: &DynamicMessage, expected: &serde_json::Value) -> DynamicMessage {
    let descriptor = pool()
        .get_message_by_name(expected["type"].as_str().unwrap())
        .unwrap();
    DynamicMessage::decode(descriptor, actual.encode_to_vec().as_slice()).unwrap()
}

// These bytes must be generated by the actual pinned Go proto.Marshal call.
// Re-decoding into the same descriptor compares typed values and presence,
// independently of map serialization order or JSON's 1 versus 1.0 rendering.
fn official_wire_message(expected: &serde_json::Value) -> DynamicMessage {
    let bytes = base64::engine::general_purpose::STANDARD
        .decode(
            expected["wire_base64"]
                .as_str()
                .expect("actual Go wire is required; no JSON fallback"),
        )
        .unwrap();
    let descriptor = pool()
        .get_message_by_name(expected["type"].as_str().unwrap())
        .unwrap();
    DynamicMessage::decode(descriptor, bytes.as_slice()).unwrap()
}

#[test]
fn actual_official_go_numeric_two_hundred_forty_six_vectors_match_typed_wire() {
    let inputs: serde_json::Value = serde_json::from_str(include_str!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../tests/oracles/protojson/numeric-cases.json"
    )))
    .unwrap();
    let oracle: serde_json::Value = serde_json::from_str(include_str!(concat!(
        env!("CARGO_MANIFEST_DIR"), "/../../tests/fixtures/upstream/evidence/2026-10-25/f03-protojson-numeric-wire-official-oracle.json"))).unwrap();
    let inputs = inputs["cases"].as_array().unwrap();
    let oracle = oracle["cases"].as_array().unwrap();
    assert_eq!(inputs.len(), 246);
    assert_eq!(oracle.len(), 246);
    let mut mismatches = Vec::new();
    for (input, expected) in inputs.iter().zip(oracle) {
        assert_eq!(input["case"], expected["case"]);
        assert_eq!(input["type"], expected["type"]);
        let result = decode_dynamic(
            input["type"].as_str().unwrap(),
            input["body"].as_str().unwrap().as_bytes(),
        );
        if result.is_ok() != expected["decoded"].as_bool().unwrap() {
            mismatches.push(format!(
                "{}: wanted decoded={}, actual decoded={} error={:?}",
                input["case"],
                expected["decoded"],
                result.is_ok(),
                result.as_ref().err()
            ));
            continue;
        }
        if let Ok(message) = result {
            if wire_canonical_message(&message, expected) != official_wire_message(expected) {
                mismatches.push(format!(
                    "{}: actual typed protobuf fields/values/presence differ from Go wire",
                    input["case"]
                ));
            }
        }
    }
    assert!(
        mismatches.is_empty(),
        "{} of 246 actual numeric oracle cases differ:\n{}",
        mismatches.len(),
        mismatches.join("\n")
    );
}

#[test]
fn actual_numeric_zero_wire_and_generated_production_bridge_preserve_defaults() {
    let inputs: serde_json::Value = serde_json::from_str(include_str!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../tests/oracles/protojson/numeric-cases.json"
    )))
    .unwrap();
    let oracle: serde_json::Value = serde_json::from_str(include_str!(concat!(
        env!("CARGO_MANIFEST_DIR"), "/../../tests/fixtures/upstream/evidence/2026-10-25/f03-protojson-numeric-wire-official-oracle.json"))).unwrap();
    let mut checked = 0;
    for (input, expected) in inputs["cases"]
        .as_array()
        .unwrap()
        .iter()
        .zip(oracle["cases"].as_array().unwrap())
    {
        assert_eq!(input["case"], expected["case"]);
        if expected.get("wire_base64").and_then(|value| value.as_str()) != Some("") {
            continue;
        }
        let name = input["type"].as_str().unwrap();
        let body = input["body"].as_str().unwrap().as_bytes();
        let dynamic = decode_dynamic(name, body).unwrap();
        let actual_dynamic_wire = dynamic.encode_to_vec();
        assert!(
            actual_dynamic_wire.is_empty(),
            "actual dynamic encoding must equal Go's empty wire: {}",
            input["case"]
        );
        // This is the actual public production decoder, not merely an
        // internal DynamicMessage or expected JSON reconstruction.
        let bridge_wire = match name {
            "livekit.CreateRoomRequest" => {
                let value: pb::CreateRoomRequest = decode(body).unwrap();
                assert_eq!(value.empty_timeout, 0);
                value.encode_to_vec()
            }
            "livekit.Room" => {
                let value: pb::Room = decode(body).unwrap();
                assert_eq!(value.creation_time, 0);
                value.encode_to_vec()
            }
            "livekit.RTPStats" => {
                let value: pb::RtpStats = decode(body).unwrap();
                assert_eq!(value.bytes, 0);
                value.encode_to_vec()
            }
            _ => panic!("unexpected numeric fixture type"),
        };
        assert!(
            bridge_wire.is_empty(),
            "generated production bridge must equal actual Go wire: {}",
            input["case"]
        );
        let canonical = DynamicMessage::decode(
            pool().get_message_by_name(name).unwrap(),
            actual_dynamic_wire.as_slice(),
        )
        .unwrap();
        assert!(
            canonical == official_wire_message(expected),
            "wire-decoded fields/presence must match Go: {}",
            input["case"]
        );
        checked += 1;
    }
    assert_eq!(
        checked, 24,
        "all three real scalar types and eight zero spellings must be covered"
    );
}

#[test]
fn generated_production_bridge_keeps_explicit_optional_false_presence_on_wire() {
    let explicit: pb::CreateIngressRequest = decode(br#"{"enabled":false}"#).unwrap();
    let absent: pb::CreateIngressRequest = decode(b"{}").unwrap();
    assert_eq!(explicit.enabled, Some(false));
    assert_eq!(absent.enabled, None);
    assert!(!explicit.encode_to_vec().is_empty());
    assert!(absent.encode_to_vec().is_empty());
    let name = "livekit.CreateIngressRequest";
    let descriptor = pool().get_message_by_name(name).unwrap();
    let explicit =
        DynamicMessage::decode(descriptor.clone(), explicit.encode_to_vec().as_slice()).unwrap();
    let absent = DynamicMessage::decode(descriptor, absent.encode_to_vec().as_slice()).unwrap();
    assert_ne!(
        explicit.has_field_by_name("enabled"),
        absent.has_field_by_name("enabled")
    );
    assert!(
        explicit != absent,
        "wire canonicalization must not erase genuine optional presence"
    );
}

#[test]
fn websocket_wrapper_unknown_fields_and_full_frame_are_distinct_from_twirp() {
    let message: pb::SignalRequest =
        super::decode_frame(br#"{"future":1,"pingReq":{"timestamp":9.91e2,"future":2}}"#).unwrap();
    assert!(
        matches!(message.message,Some(pb::signal_request::Message::PingReq(p)) if p.timestamp==991)
    );
    assert!(super::decode_frame::<pb::SignalRequest>(br#"{"pingReq":{}} {}"#).is_err());
    assert!(
        decode::<pb::SignalRequest>(br#"{"pingReq":{}} {}"#).is_ok(),
        "Twirp existing first-value API remains unchanged"
    );
    for input in [
        br#"{"updateMetadata":{"attributes":{"x":null}}}"#.as_slice(),
        br#"{"pingReq":null,"ping_req":{}}"#.as_slice(),
    ] {
        assert!(super::decode_frame::<pb::SignalRequest>(input).is_err());
    }
}
