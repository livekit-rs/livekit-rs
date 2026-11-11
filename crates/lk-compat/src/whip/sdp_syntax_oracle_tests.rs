//! Actual fixed Pion wrapper parser oracle, independent of native str0m.
#[test]
#[ignore = "requires actual fixed Pion SessionDescription.Unmarshal oracle"]
fn all_45_actual_go_sdp_grammar_rows_match_before_rtc_semantics() {
    assert_rows("WHIP_SDP_GO_CASES", "WHIP_SDP_GO_RESULTS", 45);
}
#[test]
#[ignore = "requires independently executed fixed Pion optional field oracle"]
fn all_13_actual_go_sdp_optional_field_rows_match() {
    assert_rows(
        "WHIP_SDP_OPTIONAL_GO_CASES",
        "WHIP_SDP_OPTIONAL_GO_RESULTS",
        13,
    );
}
fn assert_rows(cases_env: &str, results_env: &str, count: usize) {
    let cases: Vec<serde_json::Value> =
        serde_json::from_slice(&std::fs::read(std::env::var(cases_env).expect("cases")).unwrap())
            .unwrap();
    let text =
        std::fs::read_to_string(std::env::var(results_env).expect("actual Go JSONL")).unwrap();
    let rows: Vec<serde_json::Value> = text
        .lines()
        .map(|s| serde_json::from_str(s).unwrap())
        .collect();
    assert_eq!(cases.len(), count);
    assert_eq!(rows.len(), count);
    for (case, row) in cases.iter().zip(rows.iter()) {
        assert_eq!(case["id"], row["id"]);
        let result = super::sdp_syntax::validate(case["sdp"].as_str().unwrap().as_bytes());
        assert_eq!(
            result.is_ok(),
            row["accepted"].as_bool().unwrap(),
            "{}",
            case["id"]
        );
        assert_eq!(
            result.err().unwrap_or_default(),
            row["error"].as_str().unwrap(),
            "{}",
            case["id"]
        );
    }
}
