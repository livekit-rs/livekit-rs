//! Actual fixed-Go validator/mapper comparisons. This is component evidence,
//! separate from SIPInternal worker/RPC/phone/media acceptance. JSONL values
//! come from tests/oracles/sip-gateway/main.go, never Rust-generated expected data.
use super::*;
use base64::{Engine as _, engine::general_purpose::STANDARD};
use prost::{Message, Name};
use serde_json::Value;
use std::collections::HashSet;

fn parse<T: Message + Name + Default>(value: &Value) -> T {
    lk_protocol::json::decode(&serde_json::to_vec(value).unwrap())
        .expect("fixed oracle fixture must decode through official descriptor")
}
fn known_json<T: Message + Name>(message: &T) -> Value {
    serde_json::from_slice(&lk_protocol::json::encode_default(message).unwrap()).unwrap()
}
fn compare_message<T: Message + Name + Default>(message: &T, expected: &Value, id: &str) {
    assert!(
        expected.get("error").is_none(),
        "{id}: Go returned an error, Rust succeeded: {expected}"
    );
    assert_eq!(
        expected["type"].as_str(),
        Some(T::full_name().as_str()),
        "{id}: message type"
    );
    let wire = STANDARD
        .decode(
            expected["wire_base64"]
                .as_str()
                .expect("actual Go response wire"),
        )
        .unwrap();
    let expected_message: T =
        lk_protocol::wire::decode(&wire).expect("actual Go protobuf known projection");
    // Descriptor-bound official ProtoJSON handles numeric unknown enums and
    // avoids depending on Go's randomized protobuf map serialization order.
    assert_eq!(
        known_json(message),
        known_json(&expected_message),
        "{id}: complete known message"
    );
}
fn compare_error(error: RpcError, expected: &Value, id: &str) {
    assert_eq!(
        error.message,
        expected["error"]
            .as_str()
            .expect("Go succeeded while Rust failed"),
        "{id}: validation error"
    );
    assert!(
        expected.get("wire_base64").is_none(),
        "{id}: errored Go row unexpectedly contained success wire"
    );
}
#[test]
#[ignore = "requires actual fixed Go716 SIP gateway results and source cases"]
fn all_716_actual_go_validator_mapper_and_country_rows_match() {
    let cases_path = std::env::var("SIP_GATEWAY_GO_CASES").expect("fixed oracle cases path");
    let results_path =
        std::env::var("SIP_GATEWAY_GO_RESULTS").expect("actual Go JSONL output path");
    let cases: Vec<Value> = serde_json::from_slice(&std::fs::read(cases_path).unwrap()).unwrap();
    let text = std::fs::read_to_string(results_path).unwrap();
    let results: Vec<Value> = text
        .lines()
        .filter(|s| !s.trim().is_empty())
        .map(|s| serde_json::from_str(s).expect("actual Go JSONL row"))
        .collect();
    assert_eq!(cases.len(), 716);
    assert_eq!(results.len(), 716);
    let mut ids = HashSet::new();
    let (mut create_count, mut transfer_count, mut map_count, mut country_count) = (0, 0, 0, 0);
    let mut country_edges = HashSet::new();
    for (case, expected) in cases.iter().zip(&results) {
        let id = case["id"].as_str().unwrap();
        assert!(ids.insert(id), "duplicate case {id}");
        assert_eq!(
            expected["id"].as_str(),
            Some(id),
            "actual oracle row order/identity"
        );
        if let Some(code) = id.strip_prefix("country-") {
            if code.len() == 2 && code.bytes().all(|b| b.is_ascii_uppercase()) {
                country_count += 1;
            } else {
                assert!(country_edges.insert(code), "duplicate country edge {code}");
            }
        }
        match case["op"].as_str().unwrap() {
            "create" => {
                create_count += 1;
                let mut request: pb::CreateSipParticipantRequest = parse(&case["request"]);
                upgrade(&mut request);
                match validation::create(&request) {
                    Ok(()) => compare_message(&request, expected, id),
                    Err(e) => compare_error(e, expected, id),
                }
            }
            "transfer" => {
                transfer_count += 1;
                let mut request: pb::TransferSipParticipantRequest = parse(&case["request"]);
                match validation::transfer(&mut request) {
                    Ok(()) => compare_message(&request, expected, id),
                    Err(e) => compare_error(e, expected, id),
                }
            }
            "map" => {
                map_count += 1;
                let mut request: pb::CreateSipParticipantRequest = parse(&case["request"]);
                // Go's NewCreate... invokes Upgrade; production create_wire
                // performs this same step before the Rust mapper.
                upgrade(&mut request);
                let request = WireValue::from_message(request);
                let trunk: Option<Stored<pb::SipOutboundTrunkInfo>> = case
                    .get("trunk")
                    .filter(|v| !v.is_null())
                    .map(|v| Stored::typed(parse(v)));
                // This is the explicit protocol helper context in the actual
                // Go oracle, distinct from service-level FromHost selection.
                match map_create_with_hostname(
                    &request,
                    trunk.as_ref(),
                    "SCL_fixture".into(),
                    "from.fixture",
                    |count| {
                        assert_eq!(
                            count, 1,
                            "fixed mapper vector must not hide random multi-number choice"
                        );
                        Ok(0)
                    },
                ) {
                    Ok(value) => compare_message(&value.message, expected, id),
                    Err(e) => compare_error(e, expected, id),
                }
            }
            other => panic!("unsupported actual oracle op {other}"),
        }
    }
    assert_eq!(
        (create_count, transfer_count, map_count, country_count),
        (704, 9, 3, 676)
    );
    assert_eq!(country_edges, HashSet::from(["us", "USA", "001"]));
}

/// Packet provenance is deliberately not inferred from a known-JSON oracle.
/// This independent byte-level test verifies copied duration children and that
/// unrelated root unknown fields do not migrate across public/internal types.
#[test]
fn actual_mapper_context_copies_duration_unknowns_not_public_root_unknowns() {
    let request = pb::CreateSipParticipantRequest {
        sip_number: "123".into(),
        sip_call_to: "456".into(),
        room_name: "room".into(),
        ringing_timeout: Some(pbjson_types::Duration {
            seconds: 5,
            nanos: 0,
        }),
        ..Default::default()
    };
    let marker = [0xc0, 0xa3, 0x09, 0x2a];
    let mut raw = request.encode_to_vec();
    raw.extend(marker);
    let mut value = WireValue::<pb::CreateSipParticipantRequest>::decode(&raw).unwrap();
    let mut duration = request.ringing_timeout.unwrap().encode_to_vec();
    duration.extend(marker);
    let child = PreservedMessage::decode("google.protobuf.Duration", &duration).unwrap();
    value.wire.replace_nested("ringing_timeout", Some(&child));
    let out = map_create_with_hostname(&value, None, "SCL_fixture".into(), "from.fixture", |_| {
        panic!("explicit source number")
    })
    .unwrap();
    assert_eq!(out.message.hostname, "from.fixture");
    assert!(
        out.wire
            .nested("ringing_timeout")
            .unwrap()
            .encode_to_vec()
            .ends_with(&marker)
    );
    let mut projected = out.wire.clone();
    projected.replace_nested("ringing_timeout", None);
    let mut known = out.message;
    known.ringing_timeout = None;
    assert!(
        !projected
            .overlay(&known)
            .encode_to_vec()
            .windows(marker.len())
            .any(|bytes| bytes == marker)
    );
}
