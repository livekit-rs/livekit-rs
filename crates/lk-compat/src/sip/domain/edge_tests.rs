use super::*;
use serde_json::Value;

#[test]
fn fixed_go_quote_and_network_parser_edges() {
    let vectors: Vec<Value> = serde_json::from_str(include_str!("edge-vectors.json")).unwrap();
    let results: Vec<Value> = serde_json::from_str(include_str!("edge-results.json")).unwrap();
    assert_eq!(vectors.len(), results.len());
    for (v, r) in vectors.iter().zip(&results) {
        let name = v["name"].as_str().unwrap();
        assert_eq!(r["name"].as_str(), Some(name));
        if v["op"] == "quote" {
            assert_eq!(
                super::matching::quote(v["value"].as_str().unwrap()),
                r["quoted"].as_str().unwrap(),
                "{name}"
            );
        } else {
            let masks: Vec<String> = serde_json::from_value(v["masks"].clone()).unwrap();
            assert_eq!(
                match_address_masks(
                    v["addr"].as_str().unwrap(),
                    v["host"].as_str().unwrap(),
                    &masks
                ),
                r["matches"].as_bool().unwrap(),
                "{name}"
            );
        }
    }
}
