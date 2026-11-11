//! Consumes actual fixed Go process output. Never generates its own oracle.
use super::*;
use serde_json::Value;
#[test]
#[ignore = "coordinator must first run fixed Go oracle and supply its actual output and vectors"]
fn actual_fixed_go_selector_corpus() {
    let vectors: Vec<Value> = serde_json::from_slice(
        &std::fs::read(std::env::var("NODE_SELECTOR_VECTORS").expect("actual input required"))
            .unwrap(),
    )
    .unwrap();
    let results: Vec<Value> = serde_json::from_slice(
        &std::fs::read(
            std::env::var("NODE_SELECTOR_GO_RESULTS").expect("actual Go output required"),
        )
        .unwrap(),
    )
    .unwrap();
    compare_source_rows(vectors, results, 26, false);
}

#[test]
#[ignore = "requires additive actual fixed Go source-enum30 corpus output"]
fn actual_fixed_go_source_enum_selector_corpus() {
    let read = |name: &str| -> Vec<Value> {
        serde_json::from_slice(
            &std::fs::read(std::env::var(name).expect("actual source input/output required"))
                .unwrap(),
        )
        .unwrap()
    };
    compare_source_rows(
        read("NODE_SELECTOR_V2_VECTORS"),
        read("NODE_SELECTOR_V2_GO_RESULTS"),
        30,
        true,
    );
}

fn compare_source_rows(
    vectors: Vec<Value>,
    results: Vec<Value>,
    expected_count: usize,
    require_enums: bool,
) {
    assert_eq!(vectors.len(), expected_count);
    assert_eq!(results.len(), vectors.len());
    let source_ordinary = results.iter().filter(|r| r["status"] != "PANIC").count();
    let source_panics = results.len() - source_ordinary;
    let mut observed = 0;
    let mut panics = 0;
    for (c, r) in vectors.iter().zip(results) {
        assert_eq!(c["name"], r["name"]);
        assert_eq!(
            r["clock_boundary_ambiguous"], false,
            "source clock changed during fixture"
        );
        let text = |k: &str| c[k].as_str().unwrap_or_default().to_owned();
        let f = |k: &str| c[k].as_f64().unwrap_or_default() as f32;
        let cfg = Config {
            kind: text("kind"),
            sort_by: text("sort_by"),
            algorithm: text("algorithm"),
            cpu_limit: f("cpu_limit"),
            sysload_limit: f("sysload_limit"),
            region: text("region"),
            regions: c["regions"]
                .as_array()
                .map(|rs| {
                    rs.iter()
                        .map(|v| Region {
                            name: v["name"].as_str().unwrap().into(),
                            lat: v["lat"].as_f64().unwrap(),
                            lon: v["lon"].as_f64().unwrap(),
                        })
                        .collect()
                })
                .unwrap_or_default(),
            track_limit: c["track_limit"].as_i64().unwrap_or(0) as i32,
            bytes_limit: f("bytes_limit"),
        };
        let mut nodes: Vec<pb::Node> = c["nodes"]
            .as_array()
            .unwrap()
            .iter()
            .map(|v| lk_protocol::json::decode(&serde_json::to_vec(v).unwrap()).unwrap())
            .collect();
        for (i, node) in nodes.iter_mut().enumerate() {
            if let Some(s) = &mut node.stats {
                s.updated_at = 1000 - c["ages"][i].as_i64().unwrap_or(0);
            }
        }
        if require_enums {
            for (name, value) in [
                ("STARTING_UP", pb::NodeState::StartingUp as i32),
                ("SERVING", pb::NodeState::Serving as i32),
                ("SHUTTING_DOWN", pb::NodeState::ShuttingDown as i32),
                ("SUSPENDED", pb::NodeState::Suspended as i32),
            ] {
                assert_eq!(
                    r["source_node_state_enums"][name].as_i64().unwrap(),
                    i64::from(value)
                );
            }
        }
        // Actual Go calls LimitsReached before selector construction, including
        // nil-stats and bad-config rows. Test that independent consumer as well.
        let limits_owner = ConfiguredSelector {
            config: cfg.clone(),
            distances: Default::default(),
        };
        let limits: Vec<_> = nodes.iter().map(|n| limits_owner.at_capacity(n)).collect();
        assert_eq!(serde_json::to_value(limits).unwrap(), r["limits"]);
        let selector = ConfiguredSelector::new(cfg);
        match r["status"].as_str().unwrap() {
            "PASS" => {
                let selector = selector.unwrap();
                assert_eq!(
                    selector.select_at(&nodes, 1000, &mut |_| 0).unwrap(),
                    r["selected_id"].as_str().unwrap()
                );
                let limits: Vec<_> = nodes.iter().map(|n| selector.at_capacity(n)).collect();
                assert_eq!(serde_json::to_value(limits).unwrap(), r["limits"]);
                observed += 1;
            }
            "ERROR" => {
                let actual = selector.and_then(|s| s.select_at(&nodes, 1000, &mut |_| 0));
                assert_eq!(
                    actual.unwrap_err().to_string(),
                    r["error"].as_str().unwrap()
                );
                observed += 1;
            }
            "PANIC" => {
                panics += 1;
                assert!(
                    selector
                        .unwrap()
                        .select_at(&nodes, 1000, &mut |_| 0)
                        .is_err()
                ); /* Deliberately do not call this runtime panic parity: source fatal-vs-error HTTP mapping remains OPEN. */
            }
            other => panic!("unknown actual Go oracle status {other}"),
        }
    }
    assert_eq!(
        observed, source_ordinary,
        "every actual ordinary source row compared"
    );
    assert_eq!(
        panics, source_panics,
        "source panic rows separately accounted; no HTTP parity claim"
    );
    assert_eq!(observed + panics, expected_count);
}
