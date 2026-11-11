//! Actual Go double consumers, including the public request-to-generated bridge.
//! Root supplies frozen raw source inputs/results. No float tolerance is used.
use lk_protocol::livekit::EncodingOptions;
#[test]
#[ignore = "requires actual fixed Go double-consumer output"]
fn actual_go_json_double_bits_survive_public_generated_request_bridge() {
    let path = std::env::var("GO_JSON_DOUBLE_RESULTS").expect("actual Go results required");
    let rows = std::fs::read_to_string(path).unwrap();
    let mut count = 0;
    for line in rows.lines() {
        let row: serde_json::Value = serde_json::from_str(line).unwrap();
        let name = row["name"].as_str().unwrap();
        let lexeme = row["lexeme"].as_str().unwrap();
        if let Some(bits) = row["strconv_bits"].as_str() {
            assert_eq!(
                format!("{:016x}", lexeme.parse::<f64>().unwrap().to_bits()),
                bits,
                "{name}: std decimal source parse"
            );
        }
        let value = serde_json::from_str::<serde_json::Value>(lexeme);
        match row["json_bits"].as_str() {
            Some(bits) => assert_eq!(
                format!("{:016x}", value.unwrap().as_f64().unwrap().to_bits()),
                bits,
                "{name}: actual production serde_json float bits"
            ),
            None => assert!(value.is_err(), "{name}: actual Go encoding/json rejected"),
        }
        let body = format!("{{\"key_frame_interval\":{lexeme}}}");
        let typed = lk_protocol::json::decode::<EncodingOptions>(body.as_bytes());
        match row["protojson_bits"].as_str() {
            Some(bits) => assert_eq!(
                format!("{:016x}", typed.unwrap().key_frame_interval.to_bits()),
                bits,
                "{name}: real public ProtoJSON to generated double bits"
            ),
            None => assert!(typed.is_err(), "{name}: actual Go ProtoJSON rejected"),
        }
        count += 1;
    }
    assert_eq!(count, 20, "all actual raw cases must be checked");
}

/// Separate public-consumer gate so a generic Value failure cannot prevent the
/// actual generated request bridge from being observed on the old dependency.
#[test]
#[ignore = "requires actual fixed Go double-consumer output"]
fn actual_go_generated_double_bridge_independently_matches_bits() {
    let path = std::env::var("GO_JSON_DOUBLE_RESULTS").expect("actual Go results required");
    let rows = std::fs::read_to_string(path).unwrap();
    let mut count = 0;
    for line in rows.lines() {
        let row: serde_json::Value = serde_json::from_str(line).unwrap();
        let lexeme = row["lexeme"].as_str().unwrap();
        let body = format!("{{\"key_frame_interval\":{lexeme}}}");
        let typed = lk_protocol::json::decode::<EncodingOptions>(body.as_bytes());
        if let Some(bits) = row["protojson_bits"].as_str() {
            assert_eq!(
                format!("{:016x}", typed.unwrap().key_frame_interval.to_bits()),
                bits,
                "{}: actual generated request double consumer",
                row["name"]
            );
        } else {
            assert!(
                typed.is_err(),
                "{}: fixed Go ProtoJSON rejects",
                row["name"]
            );
        }
        count += 1;
    }
    assert_eq!(count, 20, "all original source cases must execute");
}
