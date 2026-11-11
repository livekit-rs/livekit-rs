//! Actual source-extracted Go UpdateVideoSize mutation versus neutral model.
use base64::Engine as _;
#[test]
#[ignore = "requires actual fixed source video dimensions Go oracle"]
fn all_nine_actual_go_track_video_size_mutations_match() {
    let cases: Vec<serde_json::Value> = serde_json::from_slice(
        &std::fs::read(std::env::var("WHIP_VIDEO_GO_CASES").expect("cases")).unwrap(),
    )
    .unwrap();
    let text =
        std::fs::read_to_string(std::env::var("WHIP_VIDEO_GO_RESULTS").expect("actual Go JSONL"))
            .unwrap();
    let rows: Vec<serde_json::Value> = text
        .lines()
        .map(|s| serde_json::from_str(s).unwrap())
        .collect();
    assert_eq!(cases.len(), 9);
    assert_eq!(rows.len(), 9);
    let decode = |v: &serde_json::Value| {
        let bytes = base64::engine::general_purpose::STANDARD
            .decode(v.as_str().unwrap())
            .unwrap();
        lk_protocol::wire::decode::<lk_protocol::livekit::TrackInfo>(&bytes).unwrap()
    };
    for (case, row) in cases.iter().zip(rows.iter()) {
        assert_eq!(case["id"], row["id"]);
        let mut actual = crate::convert::track_info_from_proto(&decode(&case["track"]));
        let sizes: Vec<_> = case["sizes"]
            .as_array()
            .unwrap()
            .iter()
            .map(|s| (s[0].as_u64().unwrap() as u32, s[1].as_u64().unwrap() as u32))
            .collect();
        let changed = actual.apply_observed_video_sizes(case["mime"].as_str().unwrap(), &sizes);
        assert_eq!(changed, row["changed"].as_bool().unwrap());
        assert_eq!(
            crate::convert::track_info_to_proto(&actual),
            decode(&row["track"]),
            "{}",
            case["id"]
        );
    }
}
