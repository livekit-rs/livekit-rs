use super::*;
use crate::service_bus::channels;
use base64::{Engine, engine::general_purpose::STANDARD};
use serde_json::{Value, json};

const LIMITS: WireLimits = WireLimits {
    encoded_bytes: 1024 * 1024,
    decompressed_bytes: 1024 * 1024,
};
fn wrap(value: Vec<u8>, compression: i32) -> Vec<u8> {
    pb::Msg {
        type_url: "type.googleapis.com/internal.Request".into(),
        value,
        compression,
        channel: "Method.REQ".into(),
    }
    .encode_to_vec()
}
fn channel_json(channel: channels::Channel) -> Value {
    json!({"legacy":channel.legacy,"server":channel.server,"local":channel.local})
}
fn compare_message<T: BusMessage + PartialEq + std::fmt::Debug>(item: &Value) -> String {
    let bytes = STANDARD
        .decode(item["wire_base64"].as_str().unwrap())
        .unwrap();
    let expected = T::decode(
        STANDARD
            .decode(item["payload_base64"].as_str().unwrap())
            .unwrap()
            .as_slice(),
    )
    .unwrap();
    let decoded = decode::<T>(&bytes, LIMITS).unwrap();
    assert_eq!(decoded.message, expected);
    assert_eq!(decoded.local_channel, item["local"].as_str().unwrap());
    assert_eq!(
        decoded.compression,
        item["compression"].as_i64().unwrap() as i32
    );
    let policy = CompressionPolicy {
        quality: if decoded.compression == 1 { 6 } else { 0 },
        threshold: 0,
    };
    STANDARD.encode(encode(&decoded.message, &decoded.local_channel, policy, LIMITS).unwrap())
}

#[test]
fn actual_go_bus_fixtures_and_channel_helpers_are_consumed_and_rust_outputs_can_be_exported() {
    let fixture_path =
        std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("src/service_bus/go-fixtures.json");
    let mut fixture: Value = serde_json::from_slice(
        &std::fs::read(fixture_path)
            .expect("Generate the real pinned Go fixture first; see tests/oracles/psrpc/README.md"),
    )
    .unwrap();
    assert_eq!(
        fixture["source_commit"],
        "e9701ee1b5a981cfa3c7c69503d4775ddff13729"
    );
    let cases = fixture["channels"].as_array_mut().unwrap();
    assert_eq!(cases.len(), 48);
    for item in cases {
        let service = item["service"].as_str().unwrap().to_owned();
        let method = item["method"].as_str().unwrap().to_owned();
        let client = item["client"].as_str().unwrap().to_owned();
        let topics: Vec<String> = item["topics"]
            .as_array()
            .map(|items| {
                items
                    .iter()
                    .map(|v| v.as_str().unwrap().to_owned())
                    .collect()
            })
            .unwrap_or_default();
        let refs: Vec<_> = topics.iter().map(String::as_str).collect();
        let values = [
            (
                "request",
                channel_json(channels::request(
                    &service,
                    &method,
                    &refs,
                    item["queue"].as_bool().unwrap(),
                )),
            ),
            (
                "claim_response",
                channel_json(channels::claim_response(&service, &method, &refs)),
            ),
            (
                "stream_server",
                channel_json(channels::stream_server(&service, &method, &refs)),
            ),
            (
                "response",
                channel_json(channels::response(&service, &client)),
            ),
            (
                "claim_request",
                channel_json(channels::claim_request(&service, &client)),
            ),
            (
                "stream_client",
                channel_json(channels::stream_client(&service, &client)),
            ),
            ("handler_key", json!(channels::handler_key(&method, &refs))),
        ];
        for (key, actual) in values {
            assert_eq!(item[key], actual, "channel {key}");
            item[key] = actual;
        }
    }
    let messages = fixture["messages"].as_array_mut().unwrap();
    assert_eq!(messages.len(), 9);
    for item in messages {
        let encoded = match item["type"].as_str().unwrap() {
            "internal.Request" => compare_message::<pb::Request>(item),
            "internal.Response" => compare_message::<pb::Response>(item),
            "internal.ClaimRequest" => compare_message::<pb::ClaimRequest>(item),
            "internal.ClaimResponse" => compare_message::<pb::ClaimResponse>(item),
            "internal.Stream" => compare_message::<pb::Stream>(item),
            other => panic!("unexpected real oracle type {other}"),
        };
        item["wire_base64"] = json!(encoded);
    }
    if let Some(path) = std::env::var_os("PSRPC_RUST_FIXTURE_OUT") {
        std::fs::write(path, serde_json::to_vec_pretty(&fixture).unwrap()).unwrap();
    }
}

#[test]
fn envelope_and_plain_payload_limits_have_exact_boundaries() {
    let message = pb::Request {
        raw_request: vec![7; 512],
        ..Default::default()
    };
    let encoded = encode(&message, "REQ", CompressionPolicy::default(), LIMITS).unwrap();
    let exact = WireLimits {
        encoded_bytes: encoded.len(),
        decompressed_bytes: message.encoded_len(),
    };
    assert_eq!(
        decode::<pb::Request>(&encoded, exact).unwrap().message,
        message
    );
    assert!(matches!(
        decode::<pb::Request>(
            &encoded,
            WireLimits {
                encoded_bytes: encoded.len() - 1,
                ..exact
            }
        ),
        Err(WireError::EncodedLimit(_))
    ));
    assert!(matches!(
        decode::<pb::Request>(
            &encoded,
            WireLimits {
                decompressed_bytes: message.encoded_len() - 1,
                ..exact
            }
        ),
        Err(WireError::DecodedLimit(_))
    ));
    assert!(matches!(
        encode(
            &message,
            "REQ",
            CompressionPolicy::default(),
            WireLimits {
                decompressed_bytes: 511,
                ..LIMITS
            }
        ),
        Err(WireError::DecodedLimit(_))
    ));
    assert!(matches!(
        encode(
            &message,
            "REQ",
            CompressionPolicy::default(),
            WireLimits {
                encoded_bytes: 1,
                ..LIMITS
            }
        ),
        Err(WireError::EncodedLimit(_))
    ));
}

#[test]
fn gzip_amplification_truncation_crc_and_unknown_compression_fail() {
    let message = pb::Request {
        raw_request: vec![42; 100_000],
        ..Default::default()
    };
    let encoded = encode(
        &message,
        "",
        CompressionPolicy {
            quality: 6,
            threshold: 0,
        },
        LIMITS,
    )
    .unwrap();
    let mut envelope = pb::Msg::decode(encoded.as_slice()).unwrap();
    assert_eq!(envelope.compression, 1);
    assert!(encoded.len() < 2000);
    assert!(matches!(
        decode::<pb::Request>(
            &encoded,
            WireLimits {
                decompressed_bytes: 4096,
                ..LIMITS
            }
        ),
        Err(WireError::DecodedLimit(4096))
    ));
    assert_eq!(
        decode::<pb::Request>(&encoded, LIMITS).unwrap().message,
        message
    );
    envelope.value.pop();
    assert!(matches!(
        decode::<pb::Request>(&envelope.encode_to_vec(), LIMITS),
        Err(WireError::Gzip(_))
    ));
    envelope = pb::Msg::decode(encoded.as_slice()).unwrap();
    let crc = envelope.value.len() - 8;
    envelope.value[crc] ^= 1;
    assert!(matches!(
        decode::<pb::Request>(&envelope.encode_to_vec(), LIMITS),
        Err(WireError::Gzip(_))
    ));
    envelope.compression = 77;
    assert!(matches!(
        decode::<pb::Request>(&envelope.encode_to_vec(), LIMITS),
        Err(WireError::Compression(77))
    ));
}

#[test]
fn multistream_gzip_uses_the_same_bounded_plain_message() {
    let message = pb::Request {
        raw_request: vec![8; 1024],
        ..Default::default()
    };
    let plain = message.encode_to_vec();
    let mut members = Vec::new();
    for half in plain.chunks(plain.len() / 2) {
        let mut writer = GzEncoder::new(Vec::new(), Compression::default());
        writer.write_all(half).unwrap();
        members.extend(writer.finish().unwrap());
    }
    assert_eq!(
        decode::<pb::Request>(&wrap(members.clone(), 1), LIMITS)
            .unwrap()
            .message,
        message
    );
    assert!(matches!(
        decode::<pb::Request>(
            &wrap(members, 1),
            WireLimits {
                decompressed_bytes: 1000,
                ..LIMITS
            }
        ),
        Err(WireError::DecodedLimit(1000))
    ));
}

#[test]
fn publishing_threshold_disabled_and_incompressible_policies_match_contract() {
    let message = pb::Request {
        raw_request: vec![0; 1024],
        ..Default::default()
    };
    for policy in [
        CompressionPolicy::default(),
        CompressionPolicy {
            quality: -1,
            threshold: 0,
        },
        CompressionPolicy {
            quality: 6,
            threshold: 2048,
        },
    ] {
        let bytes = encode(&message, "", policy, LIMITS).unwrap();
        assert_eq!(pb::Msg::decode(bytes.as_slice()).unwrap().compression, 0);
    }
    let bytes = encode(
        &message,
        "",
        CompressionPolicy {
            quality: 99,
            threshold: 0,
        },
        LIMITS,
    )
    .unwrap();
    assert_eq!(pb::Msg::decode(bytes.as_slice()).unwrap().compression, 1);
    // A short message is larger as a gzip member; upstream retains plain bytes.
    let bytes = encode(
        &pb::Request::default(),
        "",
        CompressionPolicy {
            quality: 6,
            threshold: 0,
        },
        LIMITS,
    )
    .unwrap();
    assert_eq!(pb::Msg::decode(bytes.as_slice()).unwrap().compression, 0);
}

#[test]
fn type_resolution_malformed_data_and_raw_payload_execution_are_distinct() {
    assert!(matches!(
        decode::<pb::Request>(&[0xff], LIMITS),
        Err(WireError::Envelope(_))
    ));
    assert!(matches!(
        decode::<pb::Request>(&wrap(vec![0xff], 0), LIMITS),
        Err(WireError::Payload(_))
    ));
    assert!(matches!(
        decode::<pb::Response>(&wrap(vec![], 0), LIMITS),
        Err(WireError::TypeMismatch { .. })
    ));
    let mut envelope = pb::Msg::decode(wrap(vec![], 0).as_slice()).unwrap();
    envelope.type_url = "custom.prefix/internal.Request".into();
    assert_eq!(
        decode::<pb::Request>(&envelope.encode_to_vec(), LIMITS)
            .unwrap()
            .message,
        pb::Request::default()
    );
    let raw = pb::ClaimResponse {
        request_id: "raw-request".into(),
        server_id: "real-server".into(),
    };
    let legacy = pb::ClaimResponse {
        request_id: "legacy-any".into(),
        ..Default::default()
    };
    let request = pb::Request {
        request: Some(pbjson_types::Any {
            type_url: "type.googleapis.com/internal.ClaimResponse".into(),
            value: legacy.encode_to_vec().into(),
        }),
        raw_request: raw.encode_to_vec(),
        ..Default::default()
    };
    assert_eq!(
        request_payload::<pb::ClaimResponse>(&request, 1024).unwrap(),
        raw
    );
    assert_eq!(
        request_payload::<pb::ClaimResponse>(
            &pb::Request {
                raw_request: vec![],
                ..request
            },
            1024
        )
        .unwrap(),
        pb::ClaimResponse::default()
    );
    assert!(matches!(
        request_payload::<pb::Request>(
            &pb::Request {
                raw_request: vec![0xff],
                ..Default::default()
            },
            1024
        ),
        Err(WireError::Payload(_))
    ));
    let response = pb::Response {
        raw_response: raw.encode_to_vec(),
        ..Default::default()
    };
    assert_eq!(
        response_payload::<pb::ClaimResponse>(&response, 1024).unwrap(),
        raw
    );
    assert!(matches!(
        response_payload::<pb::ClaimResponse>(&response, 1),
        Err(WireError::DecodedLimit(1))
    ));
}
