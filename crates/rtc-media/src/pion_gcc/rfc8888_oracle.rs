//! Additive actual RFC8888 adapter/wire/reference source comparison.
use super::*;
use serde_json::Value;
fn rows(key: &str) -> Vec<Value> {
    std::fs::read_to_string(std::env::var(key).expect("actual frozen source path required"))
        .unwrap()
        .lines()
        .map(|s| serde_json::from_str(s).unwrap())
        .collect()
}
fn unhex(s: &str) -> Vec<u8> {
    assert_eq!(s.len() % 2, 0);
    (0..s.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&s[i..i + 2], 16).unwrap())
        .collect()
}
const UNIX_ORIGIN: i128 = 62_135_596_800i128 * 1_000_000_000;
#[test]
#[ignore = "requires actual RFC8888 reference source outputs"]
fn actual_rfc8888_wire_adapter_and_report_reference() {
    let inputs = rows("PION_RFC8888_INPUT");
    let outputs = rows("PION_RFC8888_RESULTS");
    assert_eq!(inputs.len(), 7);
    assert_eq!(outputs.len(), 7);
    for (case, row) in inputs.iter().zip(&outputs) {
        assert_eq!(case["name"], row["case"]);
        let wire = unhex(row["wire_hex"].as_str().unwrap());
        let report = parse_rfc8888(&wire).unwrap();
        assert_eq!(
            u64::from(report.timestamp),
            case["report_timestamp"].as_u64().unwrap()
        );
        let reference = rfc8888_report_time(report.timestamp);
        assert_eq!(
            reference - UNIX_ORIGIN,
            i128::from(row["report_unix_ns"].as_i64().unwrap())
        );
        let now = UNIX_ORIGIN + i128::from(row["now_unix_ns"].as_i64().unwrap());
        let mut adapter = Adapter::new();
        for sent in row["actual_sent"].as_array().unwrap() {
            adapter.sent(Ack {
                sequence: sent["seq"].as_u64().unwrap() as u16,
                ssrc: sent["ssrc"].as_u64().unwrap() as u32,
                size: sent["size"].as_i64().unwrap(),
                departure: Some(
                    UNIX_ORIGIN + i128::from(sent["departure_unix_ns"].as_i64().unwrap()),
                ),
                ..Ack::default()
            });
        }
        let acks = adapter.rfc8888(reference, &report.blocks);
        let source = row["acks"].as_array().unwrap();
        assert_eq!(acks.len(), source.len());
        for (a, b) in acks.iter().zip(source) {
            assert_eq!(u64::from(a.sequence), b["seq"].as_u64().unwrap());
            assert_eq!(u64::from(a.ssrc), b["ssrc"].as_u64().unwrap());
            assert_eq!(a.size, b["size"].as_i64().unwrap());
            assert_eq!(
                a.departure.unwrap() - UNIX_ORIGIN,
                i128::from(b["departure_unix_ns"].as_i64().unwrap())
            );
            assert_eq!(
                a.arrival.map(|v| v - UNIX_ORIGIN),
                b["arrival_unix_ns"].as_i64().map(i128::from)
            );
            assert_eq!(u64::from(a.ecn), b["ecn"].as_u64().unwrap());
        }
        let mut e = Estimator::new(now);
        e.delay.rate.rtt = 987654321;
        e.feedback_timed(
            &acks,
            FeedbackTime::Rfc8888 {
                report_time: reference,
            },
            now,
        )
        .unwrap();
        assert_eq!(
            e.latest_rtt(),
            row["rtt_ns"].as_i64().unwrap(),
            "{}",
            case["name"]
        );
    }
}
