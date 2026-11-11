use super::*;
use serde_json::Value;
fn parse<T: prost::Message + prost::Name + Default>(v: &Value) -> T {
    lk_protocol::json::decode(&serde_json::to_vec(v).unwrap()).unwrap()
}
#[test]
fn actual_fixed_go_preflight_and_apply_error_codes() {
    let inputs: Vec<Value> = serde_json::from_str(include_str!("code-vectors.json")).unwrap();
    let expected: Vec<Value> = serde_json::from_str(include_str!("code-results.json")).unwrap();
    assert_eq!(inputs.len(), expected.len());
    for (v, e) in inputs.iter().zip(&expected) {
        let name = v["name"].as_str().unwrap();
        assert_eq!(e["name"].as_str(), Some(name));
        let failure = if v["op"] == "inbound" {
            apply_inbound_request(&parse(&v["request"]), &parse(&v["existing"])).unwrap_err()
        } else {
            apply_outbound_request(&parse(&v["request"]), &parse(&v["existing"])).unwrap_err()
        };
        let code = if failure.invalid_argument {
            "invalid_argument"
        } else {
            "unknown"
        };
        assert_eq!(
            code,
            e["code"].as_str().unwrap(),
            "{name}: typed error origin"
        );
        assert_eq!(
            failure.message,
            e["message"].as_str().unwrap(),
            "{name}: error message"
        );
    }
}
