use super::*;
use serde_json::{Value, json};
fn unhex(s: &str) -> Vec<u8> {
    assert_eq!(s.len() % 2, 0);
    (0..s.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&s[i..i + 2], 16).unwrap())
        .collect()
}
#[test]
#[ignore = "requires actual fixed Pion sent-history/wire oracle output"]
fn actual_pion_sent_and_rtcp_wire_adapter_match_all_source_observations() {
    let raw = std::fs::read_to_string(
        std::env::var("PION_ADAPTER_INPUT").expect("frozen actual raw cases required"),
    )
    .unwrap();
    let expected = std::fs::read_to_string(
        std::env::var("PION_ADAPTER_RESULTS").expect("actual source output required"),
    )
    .unwrap();
    let rows: Vec<Value> = expected
        .lines()
        .map(|s| serde_json::from_str(s).unwrap())
        .collect();
    let mut pos = 0;
    const ORIGIN: Timestamp = (62_135_596_800i128 + 1000) * 1_000_000_000;
    for line in raw.lines() {
        let case: Value = serde_json::from_str(line).unwrap();
        let mut adapter = Adapter::new();
        for (index, wire) in case["reports_hex"].as_array().unwrap().iter().enumerate() {
            let row = &rows[pos];
            pos += 1;
            assert_eq!(row["case"], case["name"]);
            assert_eq!(row["index"], index);
            if index == 0 {
                for sent in row["actual_sent"].as_array().unwrap() {
                    adapter.sent(Ack {
                        sequence: sent["seq"].as_u64().unwrap() as u16,
                        ssrc: sent["ssrc"].as_u64().unwrap() as u32,
                        size: sent["size"].as_i64().unwrap(),
                        departure: Some(
                            ORIGIN + i128::from(sent["departure_ns"].as_i64().unwrap()),
                        ),
                        ..Ack::default()
                    });
                }
            }
            let parsed = parse_twcc(&unhex(wire.as_str().unwrap()));
            if let Some(error) = row["wire_error"].as_str() {
                assert_eq!(
                    parsed.unwrap_err(),
                    FeedbackError::Wire(error_for_source(error))
                );
                continue;
            }
            let report = parsed.expect("actual Go parsed this identical RTCP wire");
            assert_eq!(u64::from(report.base), row["base"].as_u64().unwrap());
            assert_eq!(
                u64::from(report.reference),
                row["reference"].as_u64().unwrap()
            );
            assert_eq!(json!(report.deltas_us), row["deltas_us"]);
            let chunks: Vec<_> = report
                .chunks
                .iter()
                .map(|c| match c {
                    Chunk::Run { symbol, length } => {
                        json!({"kind":"run","symbol":symbol,"length":length})
                    }
                    Chunk::Vector(symbols) => json!({"kind":"vector","symbols":symbols}),
                })
                .collect();
            assert_eq!(json!(chunks), row["chunks"]);
            let out = adapter.twcc(
                report.base,
                report.reference,
                &report.chunks,
                &report.deltas_us,
            );
            if row["adapter_error"].as_str().is_some() {
                assert_eq!(out, Err(FeedbackError::InvalidFeedback));
                continue;
            }
            let values:Vec<_>=out.unwrap().into_iter().map(|a|json!({"seq":a.sequence,"ssrc":a.ssrc,"size":a.size,"departure_ns":a.departure.map(|d|(d-ORIGIN)as i64),"arrival_ns":a.arrival.map(|v|v as i64)})).collect();
            assert_eq!(
                json!(values),
                row["acks"],
                "actual adapter {} index{index}",
                row["case"]
            );
        }
    }
    assert_eq!(pos, rows.len());
    assert_eq!(
        pos, 12,
        "all source observations, including malformed wire, remain mandatory"
    );
}
fn error_for_source(error: &str) -> &'static str {
    match error {
        "rtcp: packet too short" => "rtcp: packet too short",
        "rtcp: invalid packet version" => "rtcp: invalid packet version",
        "rtcp: wrong packet type" => "rtcp: wrong packet type",
        _ => panic!("unimplemented actual source wire error {error}"),
    }
}
