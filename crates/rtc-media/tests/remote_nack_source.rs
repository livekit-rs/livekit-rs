//! Exact actual new vendor meter, independently compared to fixed Go sequencer eligibility.
mod rtp_ {
    pub use str0m::media::Mid;
}
#[allow(dead_code)]
#[path = "../../../vendor/str0m/src/remote_feedback.rs"]
mod exact_meter;
use std::time::{Duration, Instant};
#[test]
#[ignore = "requires actual Go source result REMOTE_NACK_ORACLE"]
fn fixed_go_sequencer_eligibility_not_rtcp_message_count() {
    let actual = std::fs::read_to_string(
        std::env::var("REMOTE_NACK_ORACLE").expect("actual Go oracle required"),
    )
    .unwrap();
    let rows: Vec<serde_json::Value> = actual
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect();
    let cases: serde_json::Value = serde_json::from_str(include_str!(
        "../../../tests/oracles/feedback-runtime/sequencer-cases.json"
    ))
    .unwrap();
    let mut offset = 0;
    for case in cases.as_array().unwrap() {
        let mut meter = exact_meter::RemoteFeedbackMeter::new(
            str0m::media::Mid::new(),
            case["Capacity"].as_u64().unwrap() as usize,
        );
        let origin = Instant::now();
        for event in case["Events"].as_array().unwrap() {
            let row = &rows[offset];
            offset += 1;
            assert_eq!(row["case"], case["Name"]);
            assert_eq!(row["kind"], event["Kind"]);
            let now = origin + Duration::from_millis(event["MS"].as_u64().unwrap());
            let seq = event["Seq"].as_u64().unwrap();
            let accepted = if event["Kind"] == "send" {
                meter.sent(seq, 100, true, false, now);
                0
            } else {
                u64::from(meter.nack(
                    seq,
                    Duration::from_millis(case["RTT"].as_u64().unwrap()),
                    now,
                ))
            };
            assert_eq!(accepted, row["accepted"].as_u64().unwrap());
            assert_eq!(
                u64::from(meter.stats.repeated_nacks),
                row["repeated"].as_u64().unwrap()
            );
        }
    }
    assert_eq!(offset, rows.len());
}
