//! Wire/JSON contracts shared by the two generated schema dialects.
#![allow(deprecated)]
use lk_protocol::{livekit as pb, twirp_json::livekit as twirp};
use prost::Message;
use serde_json::json;

#[test]
fn both_dialects_share_binary_wire_and_preserve_oneof_and_optional_fields() {
    let packet = pb::DataPacket {
        kind: pb::data_packet::Kind::Lossy as i32,
        participant_identity: "alice".into(),
        destination_identities: vec!["bob".into()],
        sequence: 42,
        value: Some(pb::data_packet::Value::User(pb::UserPacket {
            payload: vec![0, 1, 254, 255],
            topic: Some(String::new()),
            start_time: Some(u64::MAX),
            nonce: vec![4, 5, 6],
            ..Default::default()
        })),
        ..Default::default()
    };
    let wire = packet.encode_to_vec();
    let twin = twirp::DataPacket::decode(wire.as_slice()).unwrap();
    assert_eq!(twin.encode_to_vec(), wire);
    assert_eq!(
        pb::DataPacket::decode(twin.encode_to_vec().as_slice()).unwrap(),
        packet
    );
    let camel = serde_json::to_value(&packet).unwrap();
    let snake = serde_json::to_value(&twin).unwrap();
    assert_eq!(camel["participantIdentity"], "alice");
    assert_eq!(snake["participant_identity"], "alice");
    assert_eq!(camel["kind"], "LOSSY");
    assert_eq!(camel["user"]["payload"], "AAH+/w==");
    assert_eq!(camel["user"]["startTime"], u64::MAX.to_string());
    assert_eq!(snake["user"]["start_time"], u64::MAX.to_string());
    // proto3 optional empty strings are present, not dropped as defaults.
    assert_eq!(camel["user"]["topic"], "");
}

#[test]
fn int64_values_remain_lossless_strings_and_defaults_match_each_dialect() {
    let room = pb::Room {
        creation_time: i64::MAX,
        creation_time_ms: i64::MIN,
        ..Default::default()
    };
    let camel = serde_json::to_value(&room).unwrap();
    assert_eq!(camel["creationTime"], i64::MAX.to_string());
    assert_eq!(camel["creationTimeMs"], i64::MIN.to_string());
    assert!(camel.get("name").is_none());
    let encoded = lk_protocol::to_twirp_json::<_, twirp::Room>(&room).unwrap();
    let snake: serde_json::Value = serde_json::from_slice(&encoded).unwrap();
    assert_eq!(snake["creation_time"], i64::MAX.to_string());
    assert_eq!(snake["name"], "");
    assert_eq!(snake["num_participants"], 0);
    assert_eq!(snake["enabled_codecs"], json!([]));
}

#[test]
fn both_input_name_styles_and_unknown_fields_are_accepted() {
    for input in [
        json!({"creationTime": "9007199254740993", "numParticipants": 2, "futureField": true}),
        json!({"creation_time": "9007199254740993", "num_participants": 2, "futureField": true}),
    ] {
        let room: pb::Room = serde_json::from_value(input.clone()).unwrap();
        let twin: twirp::Room = serde_json::from_value(input).unwrap();
        assert_eq!(room.creation_time, 9_007_199_254_740_993);
        assert_eq!(room.num_participants, 2);
        assert_eq!(room.encode_to_vec(), twin.encode_to_vec());
    }
}

#[test]
fn enum_name_and_number_inputs_and_unknown_wire_fields_are_supported() {
    for kind in [json!("LOSSY"), json!(1)] {
        let packet: pb::DataPacket = serde_json::from_value(json!({"kind": kind})).unwrap();
        assert_eq!(packet.kind, pb::data_packet::Kind::Lossy as i32);
    }
    // Field 100 varint=1 is not in this pinned schema and is safely skipped.
    let packet = pb::DataPacket::decode(&[0xa0, 0x06, 0x01][..]).unwrap();
    assert_eq!(packet, pb::DataPacket::default());
}

#[test]
fn mismatched_dialect_conversion_returns_an_error_instead_of_empty_defaults() {
    let room = pb::Room {
        name: "not a protobuf message".into(),
        ..Default::default()
    };
    assert!(lk_protocol::to_twirp_json::<_, twirp::DataPacket>(&room).is_err());
}
