//! Additive internal-state source observations, all comparisons exact bits.
use super::*;
use serde_json::{Value, json};
fn read(name: &str) -> Vec<Value> {
    std::fs::read_to_string(std::env::var(name).expect("explicit frozen source path"))
        .unwrap()
        .lines()
        .map(|s| serde_json::from_str(s).unwrap())
        .collect()
}
#[test]
#[ignore = "requires actual supplemental Kalman/EMA source observations"]
fn actual_supplemental_kalman_ema_bits() {
    let inputs = read("PION_NUMERIC_SOURCE_INPUT");
    let outputs = read("PION_NUMERIC_SOURCE_RESULTS");
    let mut pos = 0;
    for case in inputs {
        let mut k = Kalman::new();
        k.uncertainty = case["initial_uncertainty"].as_f64().unwrap_or(0.0);
        let mut ema = Ema::default();
        for (index, value) in case["values"].as_array().unwrap().iter().enumerate() {
            let row = &outputs[pos];
            pos += 1;
            assert_eq!(row["case"], case["name"]);
            assert_eq!(row["index"], json!(index));
            let bits = |key: &str, actual: f64| {
                assert_eq!(
                    format!("{:016x}", actual.to_bits()),
                    row[key].as_str().unwrap(),
                    "{} {index} {key}",
                    case["name"]
                );
            };
            match case["stage"].as_str().unwrap() {
                "kalman" => {
                    assert_eq!(
                        k.update(value.as_i64().unwrap()),
                        row["estimate_ns"].as_i64().unwrap()
                    );
                    bits("error_bits", k.error);
                    bits("gain_bits", k.gain);
                    bits("uncertainty_bits", k.uncertainty);
                }
                "ema" => {
                    ema.update(value.as_i64().unwrap() as f64);
                    bits("mean_bits", ema.mean);
                    bits("variance_bits", ema.variance);
                    bits("deviation_bits", ema.deviation);
                }
                _ => panic!("unknown source stage"),
            }
        }
    }
    assert_eq!(pos, outputs.len());
    assert_eq!(pos, 34);
}
#[test]
#[ignore = "requires actual independent supplemental source observations"]
fn actual_independent_large_range_and_report_reference() {
    use super::super::{Ack, Adapter, Estimator, FeedbackTime, rfc8888_report_time};
    let inputs = read("PION_GCC_EXTRA_INPUT");
    let outputs = read("PION_GCC_EXTRA_RESULTS");
    let mut pos = 0;
    const ORIGIN: i128 = (62_135_596_800i128 + 1000) * 1_000_000_000;
    for case in inputs {
        let mut k = Kalman::new();
        let mut ema = Ema::default();
        let mut th = Threshold::new();
        for (index, event) in case["events"].as_array().unwrap().iter().enumerate() {
            let row = &outputs[pos];
            pos += 1;
            assert_eq!(row["case"], case["name"]);
            assert_eq!(row["index"], json!(index));
            let i = |key: &str| event[key].as_i64().unwrap_or(0);
            let bits = |key: &str, actual: f64| {
                assert_eq!(
                    actual.to_bits(),
                    row[key].as_u64().unwrap(),
                    "{} {index} {key}",
                    case["name"]
                );
            };
            match case["stage"].as_str().unwrap() {
                "kalman" => {
                    assert_eq!(
                        k.update(i("measurement_ns")),
                        row["estimate_ns"].as_i64().unwrap()
                    );
                    bits("error_bits", k.error);
                    bits("uncertainty_bits", k.uncertainty);
                    bits("gain_bits", k.gain);
                }
                "ema" => {
                    ema.update(i("received") as f64);
                    bits("mean_bits", ema.mean);
                    bits("variance_bits", ema.variance);
                    bits("deviation_bits", ema.deviation);
                }
                "threshold" => {
                    let (u, s, t) = th.compare(i("estimate_ns"), i128::from(i("at_ns")));
                    assert_eq!(json!(u as i64), row["usage"]);
                    assert_eq!(json!(s), row["scaled_ns"]);
                    assert_eq!(json!(t), row["threshold_ns"]);
                    assert_eq!(json!(th.threshold), row["next_threshold_ns"]);
                }
                "rfc8888" => {
                    let mut adapter = Adapter::new();
                    adapter.sent(Ack {
                        sequence: 7,
                        ssrc: 1,
                        size: 135,
                        departure: Some(ORIGIN + i128::from(i("departure_ns"))),
                        ..Ack::default()
                    });
                    let reference =
                        rfc8888_report_time(row["report_timestamp"].as_u64().unwrap() as u32);
                    let acks =
                        adapter.rfc8888(reference, &[(1, 7, vec![(true, i("offset") as u16, 0)])]);
                    let mut e = Estimator::new(ORIGIN);
                    e.feedback_timed(
                        &acks,
                        FeedbackTime::Rfc8888 {
                            report_time: reference,
                        },
                        ORIGIN + i128::from(i("at_ns")),
                    )
                    .unwrap();
                    assert_eq!(e.latest_rtt(), row["rtt_ns"].as_i64().unwrap());
                }
                _ => panic!("unknown source stage"),
            }
        }
    }
    assert_eq!(pos, outputs.len());
    assert_eq!(pos, 12);
}
