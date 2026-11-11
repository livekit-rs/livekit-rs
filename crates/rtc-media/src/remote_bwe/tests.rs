use super::*;
use rtc_core::config::{PolicyDuration, RemoteBweConfig};
use std::time::{Duration, Instant};

fn config(preset: &str) -> RemoteBweConfig {
    let mut c = RemoteBweConfig::default();
    if preset == "fast" {
        c.channel_observer_non_probe.estimate.required_samples = 3;
        c.channel_observer_non_probe.estimate.required_samples_min = 3;
        c.channel_observer_non_probe.estimate.collapse_threshold = PolicyDuration::ZERO;
    }
    if preset == "negative" {
        c.channel_observer_non_probe.nack.window_min_duration = PolicyDuration::from_nanos(-1);
        c.channel_observer_non_probe.nack.window_max_duration = PolicyDuration::from_nanos(-1);
        c.channel_observer_probe.estimate.collapse_threshold = PolicyDuration::from_nanos(-1);
        c.channel_observer_probe.estimate.validity_window = PolicyDuration::from_nanos(-1);
        c.probe_controller.settle_wait_min = PolicyDuration::from_millis(-2);
        c.probe_controller.settle_wait_max = PolicyDuration::from_millis(-1);
        c.probe_controller.probe_regulator.base_interval = PolicyDuration::from_millis(-3);
        c.probe_controller.probe_regulator.min_duration = PolicyDuration::from_millis(-200);
    }
    c
}
fn signal_code(value: ProbeSignal) -> i32 {
    match value {
        ProbeSignal::Inconclusive => 0,
        ProbeSignal::Congesting => 1,
        ProbeSignal::NotCongesting => 2,
    }
}
fn state_code(value: CongestionState) -> i32 {
    match value {
        CongestionState::None => 0,
        CongestionState::Congested => 2,
    }
}

#[test]
fn fixed_actual_go_source_golden_matches_each_policy_transition() {
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("src/remote_bwe/golden.tsv");
    let golden = std::fs::read_to_string(path).expect(
        "coordinator must generate golden.tsv by running the pinned actual-Go-source harness first",
    );
    let mut expected = golden.lines().filter(|line| !line.starts_with('#'));
    let start = Instant::now();
    let mut case = "";
    let mut controller = RemoteController::new(RemoteBweConfig::default(), start);
    for line in include_str!("operations.csv")
        .lines()
        .filter(|line| !line.starts_with('#') && !line.is_empty())
    {
        let f: Vec<_> = line.split(',').collect();
        if f[0] != case {
            case = f[0];
            controller = RemoteController::new(config(f[1]), start);
        }
        let at: u64 = f[2].parse().unwrap();
        let now = start + Duration::from_nanos(at);
        let a: i64 = f[4].parse().unwrap();
        let b: i64 = f[5].parse().unwrap();
        let c: i64 = f[6].parse().unwrap();
        let d: i64 = f[7].parse().unwrap();
        let mut changed = false;
        let mut finalized = false;
        let mut signal = -1;
        let mut result_bytes = 0;
        match f[3] {
            "remb" => changed = controller.on_remb(a, b, c as u32, d as u32, now).is_some(),
            "start" => controller.start_probe(
                a as u32,
                ProbeGoal {
                    desired_bps: b,
                    expected_usage_bps: c,
                    duration: controller.probe_duration(),
                    ..Default::default()
                },
                now,
            ),
            "done" => controller.finish_probe(
                a as u32,
                ProbeResult {
                    bytes_probe: b,
                    is_completed: true,
                    ..Default::default()
                },
                now,
            ),
            "rtt" => controller.update_rtt(a as f64 / 1_000_000_000.0),
            "reset" => controller.reset(now),
            "finalize" => {
                if let Some(out) = controller.finalize(now) {
                    finalized = true;
                    signal = signal_code(out.signal);
                    result_bytes = out.result.bytes_probe;
                }
            }
            "poll" => {}
            other => panic!("unknown operation {other}"),
        }
        let actual = format!(
            "{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}",
            case,
            at,
            controller.capacity(),
            state_code(controller.state()),
            controller.goal_reached(),
            controller.can_probe(now),
            controller.probe_duration().as_nanos_i64(),
            changed,
            finalized,
            signal,
            result_bytes
        );
        assert_eq!(
            actual,
            expected.next().expect("missing Go output row"),
            "operation {line}"
        );
    }
    assert!(expected.next().is_none(), "unexpected additional Go output");
}

#[test]
fn optimistic_initial_is_not_a_received_estimate_and_thresholds_remain_strict() {
    let now = Instant::now();
    let mut c = RemoteController::new(config("fast"), now);
    assert_eq!(c.capacity(), 100_000_000);
    assert!(!c.can_probe(now));
    assert!(c.can_probe(now + Duration::from_nanos(1)));
    assert!(c.on_remb(1_000_000, 500_000, 100, 0, now).is_none());
    assert!(c.on_remb(900_000, 500_000, 100, 0, now).is_none());
    assert!(c.on_remb(800_000, 500_000, 100, 0, now).is_none());
    assert_eq!(
        c.capacity(),
        100_000_000,
        "above expected usage threshold must not commit"
    );
}
#[test]
fn repeated_nacks_start_window_and_exact_minimum_is_not_triggered() {
    let now = Instant::now();
    let mut c = RemoteController::new(RemoteBweConfig::default(), now);
    c.on_remb(2_000_000, 1_000_000, 100, 100, now);
    assert!(
        c.on_remb(2_000_000, 1_000_000, 100, 0, now + Duration::from_secs(2))
            .is_none()
    );
    let changed = c
        .on_remb(
            2_000_000,
            1_000_000,
            100,
            0,
            now + Duration::from_secs(2) + Duration::from_nanos(1),
        )
        .unwrap();
    assert_eq!(changed.to, CongestionState::Congested);
    assert_eq!(c.capacity(), 866_666);
}
