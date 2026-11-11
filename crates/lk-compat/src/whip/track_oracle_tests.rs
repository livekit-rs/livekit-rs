//! Actual fixed-Go source-method vectors, separate from real full RTC acceptance.
use super::offer::synthesize;
use base64::Engine as _;
#[test]
#[ignore = "coordinator executes fixed source Go collector oracle first"]
fn fourteen_actual_go_synthesized_track_rows_match_full_neutral_projection() {
    let cases: Vec<serde_json::Value> = serde_json::from_slice(
        &std::fs::read(std::env::var("WHIP_TRACK_GO_CASES").expect("cases")).unwrap(),
    )
    .unwrap();
    let text =
        std::fs::read_to_string(std::env::var("WHIP_TRACK_GO_RESULTS").expect("actual Go JSONL"))
            .unwrap();
    let rows: Vec<serde_json::Value> = text
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect();
    assert_eq!(cases.len(), 14);
    assert_eq!(rows.len(), 14);
    for (case, row) in cases.iter().zip(rows.iter()) {
        assert_eq!(case["id"], row["id"]);
        // The actual source first parses SDP, then invokes the private method.
        // Its EqualFold media-kind branch does not bypass the stricter parser.
        match super::sdp_syntax::validate_sdp_only(case["sdp"].as_str().unwrap().as_bytes()) {
            Err(error) => {
                assert_eq!(
                    error,
                    row["error"].as_str().expect("actual Go parse error"),
                    "{}",
                    case["id"]
                );
                assert!(
                    row.get("tracks").is_none(),
                    "source parser failure must not synthesize tracks"
                );
                continue;
            }
            Ok(()) => assert!(row.get("error").is_none(), "{}", row),
        }
        let mut actual = synthesize(case["sdp"].as_str().unwrap());
        let expected = row["tracks"].as_array().unwrap();
        assert_eq!(actual.len(), expected.len());
        for (index, encoded) in expected.iter().enumerate() {
            let bytes = base64::engine::general_purpose::STANDARD
                .decode(encoded.as_str().unwrap())
                .unwrap();
            let request: lk_protocol::livekit::AddTrackRequest =
                lk_protocol::wire::decode(&bytes).unwrap();
            let mut expected =
                crate::signal::params::add_track_from_proto(&request, index as u32 + 1);
            if case["random_cid"] == true {
                for cid in [&actual[index].cid, &expected.cid] {
                    assert!(cid.starts_with("TR_"));
                    assert_eq!(cid.len(), 15);
                    assert!(cid[3..].bytes().all(|b| {
                        b"23456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz".contains(&b)
                    }));
                }
                // Two independently random source IDs are compared by their exact public domain,
                // never claimed byte-identical. All other known fields still compare exactly.
                actual[index].cid = "TR_RANDOM".into();
                expected.cid = "TR_RANDOM".into();
            }
            assert_eq!(actual[index], expected, "{}", case["id"]);
        }
    }
}
