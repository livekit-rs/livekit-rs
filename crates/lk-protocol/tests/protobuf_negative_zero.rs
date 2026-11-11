//! Full generated wire + descriptor + response/request bridge regressions.
use lk_protocol::livekit::EncodingOptions;
use prost::Message;
use prost_reflect::{DynamicMessage, Kind, Value};
#[derive(Clone, PartialEq, Message)]
struct FloatContainers {
    #[prost(float, tag = "1")]
    single: f32,
    #[prost(double, tag = "2")]
    double: f64,
    #[prost(float, repeated, tag = "3")]
    repeated: Vec<f32>,
    #[prost(btree_map = "string, double", tag = "4")]
    map: std::collections::BTreeMap<String, f64>,
    #[prost(btree_map = "string, float", tag = "5")]
    single_map: std::collections::BTreeMap<String, f32>,
    #[prost(double, optional, tag = "6")]
    optional: Option<f64>,
}
#[test]
fn generated_negative_zero_and_positive_default_survive_all_wire_containers() {
    let message = FloatContainers {
        single: -0.0,
        double: -0.0,
        repeated: vec![-0.0, 0.0],
        map: [("negative".into(), -0.0), ("positive".into(), 0.0)].into(),
        single_map: [("negative".into(), -0.0), ("positive".into(), 0.0)].into(),
        optional: Some(-0.0),
    };
    let wire = message.encode_to_vec();
    assert_eq!(wire.len(), message.encoded_len());
    let decoded = FloatContainers::decode(wire.as_slice()).unwrap();
    assert_eq!(decoded.single.to_bits(), (-0.0f32).to_bits());
    assert_eq!(decoded.double.to_bits(), (-0.0f64).to_bits());
    assert_eq!(decoded.optional.unwrap().to_bits(), (-0.0f64).to_bits());
    assert_eq!(
        decoded
            .repeated
            .iter()
            .map(|f| f.to_bits())
            .collect::<Vec<_>>(),
        vec![(-0.0f32).to_bits(), 0.0f32.to_bits()]
    );
    assert_eq!(decoded.map["negative"].to_bits(), (-0.0f64).to_bits());
    assert_eq!(decoded.map["positive"].to_bits(), 0.0f64.to_bits());
    assert_eq!(
        decoded.single_map["negative"].to_bits(),
        (-0.0f32).to_bits()
    );
    assert_eq!(decoded.single_map["positive"].to_bits(), 0.0f32.to_bits());
    assert!(
        FloatContainers::default().encode_to_vec().is_empty(),
        "ordinary +0 and absent optional fields remain defaults"
    );
    let positive = FloatContainers {
        optional: Some(0.0),
        ..Default::default()
    };
    assert!(
        !positive.encode_to_vec().is_empty(),
        "explicit optional +0 presence must remain"
    );
}
#[test]
fn dynamic_negative_zero_is_not_implicit_default_and_wire_length_matches() {
    assert!(!Value::F32(-0.0).is_default(&Kind::Float));
    assert!(Value::F32(0.0).is_default(&Kind::Float));
    assert!(!Value::F64(-0.0).is_default(&Kind::Double));
    assert!(Value::F64(0.0).is_default(&Kind::Double));
    assert!(!Value::F64(f64::NAN).is_default(&Kind::Double));
    let descriptor = lk_protocol::json::descriptor_pool()
        .get_message_by_name("livekit.EncodingOptions")
        .unwrap();
    let field = descriptor.get_field_by_name("key_frame_interval").unwrap();
    let mut message = DynamicMessage::new(descriptor.clone());
    message.set_field(&field, Value::F64(-0.0));
    assert!(message.has_field(&field));
    let wire = message.encode_to_vec();
    assert_eq!(wire.len(), message.encoded_len());
    assert_eq!(wire, vec![0x51, 0, 0, 0, 0, 0, 0, 0, 0x80]);
    let read = DynamicMessage::decode(descriptor, wire.as_slice()).unwrap();
    assert_eq!(
        read.get_field(&field).as_f64().unwrap().to_bits(),
        (-0.0f64).to_bits()
    );
}
#[test]
fn public_generated_request_and_both_response_dialects_keep_negative_zero() {
    let message =
        lk_protocol::json::decode::<EncodingOptions>(br#"{"key_frame_interval":-0.0}"#).unwrap();
    assert_eq!(message.key_frame_interval.to_bits(), (-0.0f64).to_bits());
    let wire = message.encode_to_vec();
    assert_eq!(wire, vec![0x51, 0, 0, 0, 0, 0, 0, 0, 0x80]);
    let decoded = EncodingOptions::decode(wire.as_slice()).unwrap();
    assert_eq!(decoded.key_frame_interval.to_bits(), (-0.0f64).to_bits());
    for body in [
        lk_protocol::json::encode_default(&message).unwrap(),
        lk_protocol::to_twirp_json::<
            EncodingOptions,
            lk_protocol::twirp_json::livekit::EncodingOptions,
        >(&message)
        .unwrap(),
    ] {
        let value: serde_json::Value = serde_json::from_slice(&body).unwrap();
        let number = value
            .get("keyFrameInterval")
            .or_else(|| value.get("key_frame_interval"))
            .unwrap();
        assert_eq!(number.as_f64().unwrap().to_bits(), (-0.0f64).to_bits());
        let read = lk_protocol::json::decode::<EncodingOptions>(&body).unwrap();
        assert_eq!(read.key_frame_interval.to_bits(), (-0.0f64).to_bits());
    }
}
#[test]
#[ignore = "requires actual fixed Google protobuf container wire output"]
fn actual_google_container_wire_matches_generated_encoder() {
    use base64::Engine as _;
    let rows = std::fs::read_to_string(
        std::env::var("GO_NEGATIVE_ZERO_CONTAINER_RESULTS").expect("actual Go output required"),
    )
    .unwrap();
    let mut count = 0;
    for line in rows.lines() {
        let row: serde_json::Value = serde_json::from_str(line).unwrap();
        let double =
            f64::from_bits(u64::from_str_radix(row["double_bits"].as_str().unwrap(), 16).unwrap());
        let single =
            f32::from_bits(u32::from_str_radix(row["single_bits"].as_str().unwrap(), 16).unwrap());
        let mut message = FloatContainers {
            single,
            double,
            ..Default::default()
        };
        if row["optional"].as_bool().unwrap() {
            message.optional = Some(double)
        }
        if row["repeated"].as_bool().unwrap() {
            message.repeated = vec![single, 0.0]
        }
        if row["map"].as_bool().unwrap() {
            message.map = [("negative".into(), double), ("positive".into(), 0.0)].into();
            message.single_map = [("negative".into(), single), ("positive".into(), 0.0)].into();
        }
        let actual = message.encode_to_vec();
        let expected = base64::engine::general_purpose::STANDARD
            .decode(row["wire_base64"].as_str().unwrap())
            .unwrap();
        assert_eq!(actual, expected, "actual Google wire {}", row["name"]);
        assert_eq!(actual.len(), message.encoded_len());
        count += 1;
    }
    assert_eq!(count, 4, "all real source container cases retained");
}
