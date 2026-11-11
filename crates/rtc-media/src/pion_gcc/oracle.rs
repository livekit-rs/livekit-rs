//! Actual pinned Go stage-output comparison. No hand-authored expected rates.
use super::*;
use serde_json::{Value, json};
fn i(v: &Value, key: &str) -> i64 {
    v[key].as_i64().unwrap_or(0)
}
fn usage(v: i64) -> Usage {
    match v {
        0 => Usage::Over,
        1 => Usage::Under,
        2 => Usage::Normal,
        _ => panic!("unknown actual source usage"),
    }
}
fn stats(s: Stats) -> Value {
    json!({"Measurement":s.measurement,"Estimate":s.estimate,"Threshold":s.threshold,"LastReceiveDelta":s.receive_delta,"Usage":s.usage as i64,"State":s.state as i64,"TargetBitrate":s.target})
}
#[test]
#[ignore = "requires actual coordinator fixed Pion stage source output"]
fn actual_pion_stage_outputs_match_original_source() {
    let input = std::fs::read_to_string(
        std::env::var("PION_GCC_STAGE_INPUT").expect("frozen raw source input required"),
    )
    .unwrap();
    let actual = std::fs::read_to_string(
        std::env::var("PION_GCC_STAGE_RESULTS").expect("actual Go result required"),
    )
    .unwrap();
    let outputs: Vec<Value> = actual
        .lines()
        .map(|s| serde_json::from_str(s).unwrap())
        .collect();
    let mut pos = 0;
    for line in input.lines() {
        let c: Value = serde_json::from_str(line).unwrap();
        let stage = c["stage"].as_str().unwrap();
        let mut k = delay::Kalman::new();
        let mut th = delay::Threshold::new();
        let mut over = delay::Overuse::new(0);
        let mut rate = delay::Rate::new(1_000_000, 5_000, 50_000_000);
        let mut loss = Loss::new(1_000_000);
        let mut groups = delay::Groups::new();
        let mut received = delay::Received::new();
        let mut group_output = vec![];
        let mut received_output = vec![];
        let events = c["events"].as_array().unwrap();
        for (index, e) in events.iter().enumerate() {
            let now = i128::from(i(e, "at_ns"));
            let ds = Stats {
                measurement: i(e, "measurement_ns"),
                estimate: i(e, "estimate_ns"),
                receive_delta: i(e, "receive_delta_ns"),
                usage: usage(i(e, "usage")),
                ..Stats::default()
            };
            let mut observations: Vec<(&str, Value)> = vec![];
            match stage {
                "kalman" => {
                    observations.push(("estimate_ns", json!(k.update(ds.measurement))));
                    observations.push(("error", json!(k.error)));
                    observations.push(("uncertainty", json!(k.uncertainty)));
                    observations.push(("gain", json!(k.gain)));
                }
                "threshold" => {
                    let (u, estimate, threshold) = th.compare(ds.estimate, now);
                    observations.extend([
                        ("usage", json!(u as i64)),
                        ("estimate_ns", json!(estimate)),
                        ("threshold_ns", json!(threshold)),
                        ("next_threshold_ns", json!(th.threshold)),
                    ]);
                }
                "overuse" => {
                    let s = over.update(ds, now);
                    observations.extend([
                        ("stats", stats(s)),
                        ("count", json!(over.count)),
                        ("increasing_ns", json!(over.increasing)),
                    ]);
                }
                "rate" => {
                    rate.received = i(e, "received");
                    rate.rtt = i(e, "rtt_ns");
                    let out = rate.update(ds, now);
                    observations.extend([
                        ("emitted", json!(out.is_some())),
                        ("target", json!(rate.target)),
                        ("stored_stats", stats(rate.last_stats)),
                    ]);
                    if let Some(s) = out {
                        observations.push(("stats", stats(s)));
                    }
                }
                "loss" => {
                    let lost = i(e, "Lost");
                    let batch: Vec<_> = (0..i(e, "Total"))
                        .map(|j| Ack {
                            arrival: (j >= lost).then_some(now + 1),
                            ..Ack::default()
                        })
                        .collect();
                    loss.update(&batch, now);
                    let target = loss.estimate(i(e, "wanted"));
                    observations.extend([
                        (
                            "stats",
                            json!({"TargetBitrate":target,"AverageLoss":loss.average}),
                        ),
                        ("bitrate", json!(loss.bitrate)),
                    ]);
                }
                "groups" | "received" => {
                    let a = Ack {
                        departure: e["departure_ns"].as_i64().map(i128::from),
                        arrival: e["arrival_ns"].as_i64().map(i128::from),
                        size: i(e, "size"),
                        ..Ack::default()
                    };
                    if stage == "groups" {
                        if let Some(g) = groups.push(a) {
                            group_output.push(json!({"departure_ns":g.departure,"arrival_ns":g.arrival,"packets":g.packets}));
                        }
                    } else if let Some(v) = received.push(a) {
                        received_output.push(v)
                    };
                    if index + 1 != events.len() {
                        continue;
                    }
                    observations.push(if stage == "groups" {
                        ("groups", json!(group_output))
                    } else {
                        ("rates", json!(received_output))
                    });
                }
                _ => panic!("unknown frozen source stage"),
            }
            let expected = &outputs[pos];
            pos += 1;
            assert_eq!(expected["case"], c["name"]);
            assert_eq!(expected["index"], index);
            for (key, value) in observations {
                if matches!(key, "error" | "gain" | "uncertainty") {
                    assert_eq!(
                        value.as_f64().unwrap().to_bits(),
                        expected[key].as_f64().unwrap().to_bits(),
                        "actual source f64 case={} index={index} field={key}",
                        c["name"]
                    );
                } else if stage == "loss" && key == "stats" {
                    assert_eq!(value["TargetBitrate"], expected[key]["TargetBitrate"]);
                    assert_eq!(
                        value["AverageLoss"].as_f64().unwrap().to_bits(),
                        expected[key]["AverageLoss"].as_f64().unwrap().to_bits(),
                        "actual source loss f64 case={} index={index}",
                        c["name"]
                    );
                } else {
                    assert_eq!(
                        &value, &expected[key],
                        "actual source case={} index={index} field={key}",
                        c["name"]
                    );
                }
            }
        }
    }
    assert_eq!(
        pos,
        outputs.len(),
        "every actual source observation must be compared"
    );
    assert_eq!(pos, 404);
}
