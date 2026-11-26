//! C98-A config_projection legs for the 94 `generated_flags:--rtc.congestion_control.*`
//! rows the ledger left NOT_RUN.
//!
//! Tier 2 only (SPEC-0075 §2): every leg drives the public loading API
//! (`parse_with_probe`) with a generated CLI flag and asserts on the **projected**
//! `rtc_core::config::CongestionControl`, never on the parsed tree. The projection sites
//! are crates/lk-config/src/normalize.rs:25-234 (project_stream_allocator_config,
//! project_remote_bwe_config, project_channel_observer_config,
//! project_trend_detector_config, project_nack_tracker_config,
//! project_probe_controller_config, project_probe_regulator_config,
//! project_send_side_bwe_config, project_congestion_detector_config,
//! project_packet_group_config, project_probe_packet_group_config,
//! project_probe_signal_config, project_weighted_loss_config,
//! project_congestion_signal_config, project_congestion_control).
//!
//! Go oracle: references/livekit-stable @ e5655ac (v1.13.10). The flag set is what
//! GenerateCLIFlags (pkg/config/config.go:943-1050) produces by reflecting over
//! CongestionControlConfig (pkg/config/config.go:218-231) and the structs it embeds:
//! streamallocator.StreamAllocatorConfig
//! (pkg/sfu/streamallocator/streamallocator.go:190-199),
//! remotebwe.RemoteBWEConfig (pkg/sfu/bwe/remotebwe/remote_bwe.go:31-37),
//! sendsidebwe.SendSideBWEConfig (pkg/sfu/bwe/sendsidebwe/send_side_bwe.go:57-59) and
//! sendsidebwe.CongestionDetectorConfig
//! (pkg/sfu/bwe/sendsidebwe/congestion_detector.go:437-468).
//!
//! Each leg also compares a sibling leaf across the two runs, so a projection that has
//! the right shape but reads the wrong field cannot pass either.

use lk_config::{
    EntryAction, Env, ProgramInfo, generated::specs, netdetect::NetProbe, parse_with_probe,
};
use rtc_core::config::CongestionControl;
use std::{ffi::OsString, net::IpAddr, time::Duration};

struct NoNetwork;
impl NetProbe for NoNetwork {
    fn local_addresses(&self) -> Vec<IpAddr> {
        vec!["127.0.0.1".parse().expect("fixture loopback")]
    }
    fn external_ip(&self, _: &[String], _: bool) -> Result<IpAddr, String> {
        panic!("fixture must not discover external IP")
    }
    fn sleep(&self, _: Duration) {
        panic!("fixture must not retry networking")
    }
}

fn parse(args: &[&str], env: &Env) -> Box<lk_config::LoadedConfig> {
    let info = ProgramInfo {
        binary_name: "livecloud".into(),
        product_version: "fixture".into(),
        compat_version: "1.13.10",
    };
    let mut argv: Vec<OsString> = ["livecloud", "--dev"]
        .into_iter()
        .map(OsString::from)
        .collect();
    argv.extend(args.iter().map(OsString::from));
    let result = match parse_with_probe(argv, env, &info, &NoNetwork) {
        Ok(r) => r,
        Err(e) => panic!("parse_with_probe error: {}", e),
    };
    match result {
        EntryAction::RunServer(config) => config,
        other => panic!("expected RunServer, got other entry action: {other:?}"),
    }
}

/// Projected field accessor: the load path is what this tier tests, so a non-RunServer
/// outcome is a fixture failure, never a skipped leg.
fn cc(args: &[&str], env: &Env) -> CongestionControl {
    parse(args, env).server.rtc.congestion_control.clone()
}

/// The generated flag surface itself. Go builds it by reflection over the same struct
/// tree this port walks, so a flag that vanishes here is a flag the binary stopped
/// accepting -- a parity break even if every other leg still passes.
#[test]
fn cc_generated_flag_table_matches_the_go_walk() {
    const EXPECTED: &[&str] = &[
        "rtc.congestion_control.allow_pause",
        "rtc.congestion_control.enabled",
        "rtc.congestion_control.remote_bwe.channel_observer_non_probe.estimate.collapse_threshold",
        "rtc.congestion_control.remote_bwe.channel_observer_non_probe.estimate.downward_trend_max_wait",
        "rtc.congestion_control.remote_bwe.channel_observer_non_probe.estimate.downward_trend_threshold",
        "rtc.congestion_control.remote_bwe.channel_observer_non_probe.estimate.required_samples",
        "rtc.congestion_control.remote_bwe.channel_observer_non_probe.estimate.required_samples_min",
        "rtc.congestion_control.remote_bwe.channel_observer_non_probe.estimate.validity_window",
        "rtc.congestion_control.remote_bwe.channel_observer_non_probe.nack.ratio_threshold",
        "rtc.congestion_control.remote_bwe.channel_observer_non_probe.nack.window_max_duration",
        "rtc.congestion_control.remote_bwe.channel_observer_non_probe.nack.window_min_duration",
        "rtc.congestion_control.remote_bwe.channel_observer_probe.estimate.collapse_threshold",
        "rtc.congestion_control.remote_bwe.channel_observer_probe.estimate.downward_trend_max_wait",
        "rtc.congestion_control.remote_bwe.channel_observer_probe.estimate.downward_trend_threshold",
        "rtc.congestion_control.remote_bwe.channel_observer_probe.estimate.required_samples",
        "rtc.congestion_control.remote_bwe.channel_observer_probe.estimate.required_samples_min",
        "rtc.congestion_control.remote_bwe.channel_observer_probe.estimate.validity_window",
        "rtc.congestion_control.remote_bwe.channel_observer_probe.nack.ratio_threshold",
        "rtc.congestion_control.remote_bwe.channel_observer_probe.nack.window_max_duration",
        "rtc.congestion_control.remote_bwe.channel_observer_probe.nack.window_min_duration",
        "rtc.congestion_control.remote_bwe.expected_usage_threshold",
        "rtc.congestion_control.remote_bwe.nack_ratio_attenuator",
        "rtc.congestion_control.remote_bwe.probe_controller.probe_regulator.backoff_factor",
        "rtc.congestion_control.remote_bwe.probe_controller.probe_regulator.base_interval",
        "rtc.congestion_control.remote_bwe.probe_controller.probe_regulator.duration_increase_factor",
        "rtc.congestion_control.remote_bwe.probe_controller.probe_regulator.max_duration",
        "rtc.congestion_control.remote_bwe.probe_controller.probe_regulator.max_interval",
        "rtc.congestion_control.remote_bwe.probe_controller.probe_regulator.min_duration",
        "rtc.congestion_control.remote_bwe.probe_controller.settle_wait_max",
        "rtc.congestion_control.remote_bwe.probe_controller.settle_wait_min",
        "rtc.congestion_control.remote_bwe.probe_controller.settle_wait_num_rtt",
        "rtc.congestion_control.send_side_bwe.congestion_detector.congested_ctr_epsilon",
        "rtc.congestion_control.send_side_bwe.congestion_detector.congested_ctr_trend.collapse_threshold",
        "rtc.congestion_control.send_side_bwe.congestion_detector.congested_ctr_trend.downward_trend_max_wait",
        "rtc.congestion_control.send_side_bwe.congestion_detector.congested_ctr_trend.downward_trend_threshold",
        "rtc.congestion_control.send_side_bwe.congestion_detector.congested_ctr_trend.required_samples",
        "rtc.congestion_control.send_side_bwe.congestion_detector.congested_ctr_trend.required_samples_min",
        "rtc.congestion_control.send_side_bwe.congestion_detector.congested_ctr_trend.validity_window",
        "rtc.congestion_control.send_side_bwe.congestion_detector.congested_packet_group.max_window_duration",
        "rtc.congestion_control.send_side_bwe.congestion_detector.congested_packet_group.min_packets",
        "rtc.congestion_control.send_side_bwe.congestion_detector.dqr_max_delay",
        "rtc.congestion_control.send_side_bwe.congestion_detector.dqr_max_weighted_loss",
        "rtc.congestion_control.send_side_bwe.congestion_detector.estimaton_window_duration",
        "rtc.congestion_control.send_side_bwe.congestion_detector.jqr_min_delay",
        "rtc.congestion_control.send_side_bwe.congestion_detector.jqr_min_trend_coefficient",
        "rtc.congestion_control.send_side_bwe.congestion_detector.jqr_min_weighted_loss",
        "rtc.congestion_control.send_side_bwe.congestion_detector.loss_congested_dqr.min_duration",
        "rtc.congestion_control.send_side_bwe.congestion_detector.loss_congested_dqr.min_number_of_groups",
        "rtc.congestion_control.send_side_bwe.congestion_detector.loss_congested_jqr.min_duration",
        "rtc.congestion_control.send_side_bwe.congestion_detector.loss_congested_jqr.min_number_of_groups",
        "rtc.congestion_control.send_side_bwe.congestion_detector.loss_early_warning_dqr.min_duration",
        "rtc.congestion_control.send_side_bwe.congestion_detector.loss_early_warning_dqr.min_number_of_groups",
        "rtc.congestion_control.send_side_bwe.congestion_detector.loss_early_warning_jqr.min_duration",
        "rtc.congestion_control.send_side_bwe.congestion_detector.loss_early_warning_jqr.min_number_of_groups",
        "rtc.congestion_control.send_side_bwe.congestion_detector.packet_group.max_window_duration",
        "rtc.congestion_control.send_side_bwe.congestion_detector.packet_group.min_packets",
        "rtc.congestion_control.send_side_bwe.congestion_detector.packet_group_max_age",
        "rtc.congestion_control.send_side_bwe.congestion_detector.probe_packet_group.packet_group.max_window_duration",
        "rtc.congestion_control.send_side_bwe.congestion_detector.probe_packet_group.packet_group.min_packets",
        "rtc.congestion_control.send_side_bwe.congestion_detector.probe_packet_group.settle_wait_max",
        "rtc.congestion_control.send_side_bwe.congestion_detector.probe_packet_group.settle_wait_min",
        "rtc.congestion_control.send_side_bwe.congestion_detector.probe_packet_group.settle_wait_num_rtt",
        "rtc.congestion_control.send_side_bwe.congestion_detector.probe_regulator.backoff_factor",
        "rtc.congestion_control.send_side_bwe.congestion_detector.probe_regulator.base_interval",
        "rtc.congestion_control.send_side_bwe.congestion_detector.probe_regulator.duration_increase_factor",
        "rtc.congestion_control.send_side_bwe.congestion_detector.probe_regulator.max_duration",
        "rtc.congestion_control.send_side_bwe.congestion_detector.probe_regulator.max_interval",
        "rtc.congestion_control.send_side_bwe.congestion_detector.probe_regulator.min_duration",
        "rtc.congestion_control.send_side_bwe.congestion_detector.probe_signal.dqr_max_delay",
        "rtc.congestion_control.send_side_bwe.congestion_detector.probe_signal.dqr_max_weighted_loss",
        "rtc.congestion_control.send_side_bwe.congestion_detector.probe_signal.jqr_min_delay",
        "rtc.congestion_control.send_side_bwe.congestion_detector.probe_signal.jqr_min_weighted_loss",
        "rtc.congestion_control.send_side_bwe.congestion_detector.probe_signal.min_bytes_ratio",
        "rtc.congestion_control.send_side_bwe.congestion_detector.probe_signal.min_duration_ratio",
        "rtc.congestion_control.send_side_bwe.congestion_detector.probe_signal.weighted_loss.base_duration",
        "rtc.congestion_control.send_side_bwe.congestion_detector.probe_signal.weighted_loss.base_pps",
        "rtc.congestion_control.send_side_bwe.congestion_detector.probe_signal.weighted_loss.loss_penalty_factor",
        "rtc.congestion_control.send_side_bwe.congestion_detector.probe_signal.weighted_loss.min_duration_for_loss_validity",
        "rtc.congestion_control.send_side_bwe.congestion_detector.queuing_delay_congested_dqr.min_duration",
        "rtc.congestion_control.send_side_bwe.congestion_detector.queuing_delay_congested_dqr.min_number_of_groups",
        "rtc.congestion_control.send_side_bwe.congestion_detector.queuing_delay_congested_jqr.min_duration",
        "rtc.congestion_control.send_side_bwe.congestion_detector.queuing_delay_congested_jqr.min_number_of_groups",
        "rtc.congestion_control.send_side_bwe.congestion_detector.queuing_delay_early_warning_dqr.min_duration",
        "rtc.congestion_control.send_side_bwe.congestion_detector.queuing_delay_early_warning_dqr.min_number_of_groups",
        "rtc.congestion_control.send_side_bwe.congestion_detector.queuing_delay_early_warning_jqr.min_duration",
        "rtc.congestion_control.send_side_bwe.congestion_detector.queuing_delay_early_warning_jqr.min_number_of_groups",
        "rtc.congestion_control.send_side_bwe.congestion_detector.weighted_loss.base_duration",
        "rtc.congestion_control.send_side_bwe.congestion_detector.weighted_loss.base_pps",
        "rtc.congestion_control.send_side_bwe.congestion_detector.weighted_loss.loss_penalty_factor",
        "rtc.congestion_control.send_side_bwe.congestion_detector.weighted_loss.min_duration_for_loss_validity",
        "rtc.congestion_control.send_side_bwe_pacer",
        "rtc.congestion_control.stream_allocator.disable_etimation_unmanaged_tracks",
        "rtc.congestion_control.stream_allocator.min_channel_capacity",
        "rtc.congestion_control.stream_allocator.paused_min_wait",
        "rtc.congestion_control.stream_allocator.probe_min_bps",
        "rtc.congestion_control.stream_allocator.probe_mode",
        "rtc.congestion_control.stream_allocator.probe_overage_pct",
        "rtc.congestion_control.use_send_side_bwe",
        "rtc.congestion_control.use_send_side_bwe_interceptor",
    ];
    let mut got: Vec<&str> = specs()
        .iter()
        .filter(|s| s.path.starts_with("rtc.congestion_control."))
        .map(|s| s.path.as_str())
        .collect();
    got.sort_unstable();
    let want: Vec<&str> = EXPECTED.to_vec();
    assert_eq!(want.len(), 99, "congestion_control generated flag count");
    assert_eq!(want, got, "generated flag table drifted from the Go walk");
}

#[test]
fn cc_flag_remote_bwe_channel_observer_non_probe_estimate_collapse_threshold() {
    // --rtc.congestion_control.remote_bwe.channel_observer_non_probe.estimate.collapse_threshold: Go default '500ms'
    let base = cc(&[], &Env::default());
    let got = cc(
        &[
            "--rtc.congestion_control.remote_bwe.channel_observer_non_probe.estimate.collapse_threshold=1000000001ns",
        ],
        &Env::default(),
    );
    assert_eq!(
        1000000001_i64,
        got.remote_bwe
            .channel_observer_non_probe
            .estimate
            .collapse_threshold
            .as_nanos_i64(),
        "C98A-01 --rtc.congestion_control.remote_bwe.channel_observer_non_probe.estimate.collapse_threshold=1000000001ns must project the Duration leaf"
    );
    assert_eq!(
        base.remote_bwe
            .channel_observer_non_probe
            .estimate
            .downward_trend_max_wait
            .as_nanos_i64(),
        got.remote_bwe
            .channel_observer_non_probe
            .estimate
            .downward_trend_max_wait
            .as_nanos_i64(),
        "C98A-01 --rtc.congestion_control.remote_bwe.channel_observer_non_probe.estimate.collapse_threshold must not disturb rtc.congestion_control.remote_bwe.channel_observer_non_probe.estimate.downward_trend_max_wait"
    );
}

#[test]
fn cc_flag_remote_bwe_channel_observer_non_probe_estimate_downward_trend_max_wait() {
    // --rtc.congestion_control.remote_bwe.channel_observer_non_probe.estimate.downward_trend_max_wait: Go default '5s'
    let base = cc(&[], &Env::default());
    let got = cc(
        &[
            "--rtc.congestion_control.remote_bwe.channel_observer_non_probe.estimate.downward_trend_max_wait=2000000008ns",
        ],
        &Env::default(),
    );
    assert_eq!(
        2000000008_i64,
        got.remote_bwe
            .channel_observer_non_probe
            .estimate
            .downward_trend_max_wait
            .as_nanos_i64(),
        "C98A-02 --rtc.congestion_control.remote_bwe.channel_observer_non_probe.estimate.downward_trend_max_wait=2000000008ns must project the Duration leaf"
    );
    assert_eq!(
        base.remote_bwe
            .channel_observer_non_probe
            .estimate
            .collapse_threshold
            .as_nanos_i64(),
        got.remote_bwe
            .channel_observer_non_probe
            .estimate
            .collapse_threshold
            .as_nanos_i64(),
        "C98A-02 --rtc.congestion_control.remote_bwe.channel_observer_non_probe.estimate.downward_trend_max_wait must not disturb rtc.congestion_control.remote_bwe.channel_observer_non_probe.estimate.collapse_threshold"
    );
}

#[test]
fn cc_flag_remote_bwe_channel_observer_non_probe_estimate_downward_trend_threshold() {
    // --rtc.congestion_control.remote_bwe.channel_observer_non_probe.estimate.downward_trend_threshold: Go default '-0.6'
    let base = cc(&[], &Env::default());
    let got = cc(
        &[
            "--rtc.congestion_control.remote_bwe.channel_observer_non_probe.estimate.downward_trend_threshold=0.03125",
        ],
        &Env::default(),
    );
    assert_eq!(
        0.03125_f64,
        got.remote_bwe
            .channel_observer_non_probe
            .estimate
            .downward_trend_threshold,
        "C98A-03 --rtc.congestion_control.remote_bwe.channel_observer_non_probe.estimate.downward_trend_threshold=0.03125 must project the Float leaf"
    );
    assert_eq!(
        base.remote_bwe
            .channel_observer_non_probe
            .estimate
            .collapse_threshold
            .as_nanos_i64(),
        got.remote_bwe
            .channel_observer_non_probe
            .estimate
            .collapse_threshold
            .as_nanos_i64(),
        "C98A-03 --rtc.congestion_control.remote_bwe.channel_observer_non_probe.estimate.downward_trend_threshold must not disturb rtc.congestion_control.remote_bwe.channel_observer_non_probe.estimate.collapse_threshold"
    );
}

#[test]
fn cc_flag_remote_bwe_channel_observer_non_probe_estimate_required_samples() {
    // --rtc.congestion_control.remote_bwe.channel_observer_non_probe.estimate.required_samples: Go default '12'
    let base = cc(&[], &Env::default());
    let got = cc(
        &[
            "--rtc.congestion_control.remote_bwe.channel_observer_non_probe.estimate.required_samples=100003",
        ],
        &Env::default(),
    );
    assert_eq!(
        100003_i64,
        got.remote_bwe
            .channel_observer_non_probe
            .estimate
            .required_samples,
        "C98A-04 --rtc.congestion_control.remote_bwe.channel_observer_non_probe.estimate.required_samples=100003 must project the Int leaf"
    );
    assert_eq!(
        base.remote_bwe
            .channel_observer_non_probe
            .estimate
            .collapse_threshold
            .as_nanos_i64(),
        got.remote_bwe
            .channel_observer_non_probe
            .estimate
            .collapse_threshold
            .as_nanos_i64(),
        "C98A-04 --rtc.congestion_control.remote_bwe.channel_observer_non_probe.estimate.required_samples must not disturb rtc.congestion_control.remote_bwe.channel_observer_non_probe.estimate.collapse_threshold"
    );
}

#[test]
fn cc_flag_remote_bwe_channel_observer_non_probe_estimate_required_samples_min() {
    // --rtc.congestion_control.remote_bwe.channel_observer_non_probe.estimate.required_samples_min: Go default '8'
    let base = cc(&[], &Env::default());
    let got = cc(
        &[
            "--rtc.congestion_control.remote_bwe.channel_observer_non_probe.estimate.required_samples_min=100004",
        ],
        &Env::default(),
    );
    assert_eq!(
        100004_i64,
        got.remote_bwe
            .channel_observer_non_probe
            .estimate
            .required_samples_min,
        "C98A-05 --rtc.congestion_control.remote_bwe.channel_observer_non_probe.estimate.required_samples_min=100004 must project the Int leaf"
    );
    assert_eq!(
        base.remote_bwe
            .channel_observer_non_probe
            .estimate
            .collapse_threshold
            .as_nanos_i64(),
        got.remote_bwe
            .channel_observer_non_probe
            .estimate
            .collapse_threshold
            .as_nanos_i64(),
        "C98A-05 --rtc.congestion_control.remote_bwe.channel_observer_non_probe.estimate.required_samples_min must not disturb rtc.congestion_control.remote_bwe.channel_observer_non_probe.estimate.collapse_threshold"
    );
}

#[test]
fn cc_flag_remote_bwe_channel_observer_non_probe_estimate_validity_window() {
    // --rtc.congestion_control.remote_bwe.channel_observer_non_probe.estimate.validity_window: Go default '10s'
    let base = cc(&[], &Env::default());
    let got = cc(
        &[
            "--rtc.congestion_control.remote_bwe.channel_observer_non_probe.estimate.validity_window=6000000036ns",
        ],
        &Env::default(),
    );
    assert_eq!(
        6000000036_i64,
        got.remote_bwe
            .channel_observer_non_probe
            .estimate
            .validity_window
            .as_nanos_i64(),
        "C98A-06 --rtc.congestion_control.remote_bwe.channel_observer_non_probe.estimate.validity_window=6000000036ns must project the Duration leaf"
    );
    assert_eq!(
        base.remote_bwe
            .channel_observer_non_probe
            .estimate
            .collapse_threshold
            .as_nanos_i64(),
        got.remote_bwe
            .channel_observer_non_probe
            .estimate
            .collapse_threshold
            .as_nanos_i64(),
        "C98A-06 --rtc.congestion_control.remote_bwe.channel_observer_non_probe.estimate.validity_window must not disturb rtc.congestion_control.remote_bwe.channel_observer_non_probe.estimate.collapse_threshold"
    );
}

#[test]
fn cc_flag_remote_bwe_channel_observer_non_probe_nack_ratio_threshold() {
    // --rtc.congestion_control.remote_bwe.channel_observer_non_probe.nack.ratio_threshold: Go default '0.08'
    let base = cc(&[], &Env::default());
    let got = cc(
        &[
            "--rtc.congestion_control.remote_bwe.channel_observer_non_probe.nack.ratio_threshold=0.09375",
        ],
        &Env::default(),
    );
    assert_eq!(
        0.09375_f64,
        got.remote_bwe
            .channel_observer_non_probe
            .nack
            .ratio_threshold,
        "C98A-07 --rtc.congestion_control.remote_bwe.channel_observer_non_probe.nack.ratio_threshold=0.09375 must project the Float leaf"
    );
    assert_eq!(
        base.remote_bwe
            .channel_observer_non_probe
            .nack
            .window_max_duration
            .as_nanos_i64(),
        got.remote_bwe
            .channel_observer_non_probe
            .nack
            .window_max_duration
            .as_nanos_i64(),
        "C98A-07 --rtc.congestion_control.remote_bwe.channel_observer_non_probe.nack.ratio_threshold must not disturb rtc.congestion_control.remote_bwe.channel_observer_non_probe.nack.window_max_duration"
    );
}

#[test]
fn cc_flag_remote_bwe_channel_observer_non_probe_nack_window_max_duration() {
    // --rtc.congestion_control.remote_bwe.channel_observer_non_probe.nack.window_max_duration: Go default '3s'
    let base = cc(&[], &Env::default());
    let got = cc(
        &[
            "--rtc.congestion_control.remote_bwe.channel_observer_non_probe.nack.window_max_duration=8000000050ns",
        ],
        &Env::default(),
    );
    assert_eq!(
        8000000050_i64,
        got.remote_bwe
            .channel_observer_non_probe
            .nack
            .window_max_duration
            .as_nanos_i64(),
        "C98A-08 --rtc.congestion_control.remote_bwe.channel_observer_non_probe.nack.window_max_duration=8000000050ns must project the Duration leaf"
    );
    assert_eq!(
        base.remote_bwe
            .channel_observer_non_probe
            .nack
            .ratio_threshold,
        got.remote_bwe
            .channel_observer_non_probe
            .nack
            .ratio_threshold,
        "C98A-08 --rtc.congestion_control.remote_bwe.channel_observer_non_probe.nack.window_max_duration must not disturb rtc.congestion_control.remote_bwe.channel_observer_non_probe.nack.ratio_threshold"
    );
}

#[test]
fn cc_flag_remote_bwe_channel_observer_non_probe_nack_window_min_duration() {
    // --rtc.congestion_control.remote_bwe.channel_observer_non_probe.nack.window_min_duration: Go default '2s'
    let base = cc(&[], &Env::default());
    let got = cc(
        &[
            "--rtc.congestion_control.remote_bwe.channel_observer_non_probe.nack.window_min_duration=9000000057ns",
        ],
        &Env::default(),
    );
    assert_eq!(
        9000000057_i64,
        got.remote_bwe
            .channel_observer_non_probe
            .nack
            .window_min_duration
            .as_nanos_i64(),
        "C98A-09 --rtc.congestion_control.remote_bwe.channel_observer_non_probe.nack.window_min_duration=9000000057ns must project the Duration leaf"
    );
    assert_eq!(
        base.remote_bwe
            .channel_observer_non_probe
            .nack
            .ratio_threshold,
        got.remote_bwe
            .channel_observer_non_probe
            .nack
            .ratio_threshold,
        "C98A-09 --rtc.congestion_control.remote_bwe.channel_observer_non_probe.nack.window_min_duration must not disturb rtc.congestion_control.remote_bwe.channel_observer_non_probe.nack.ratio_threshold"
    );
}

#[test]
fn cc_flag_remote_bwe_channel_observer_probe_estimate_collapse_threshold() {
    // --rtc.congestion_control.remote_bwe.channel_observer_probe.estimate.collapse_threshold: Go default '0s'
    let base = cc(&[], &Env::default());
    let got = cc(
        &[
            "--rtc.congestion_control.remote_bwe.channel_observer_probe.estimate.collapse_threshold=10000000064ns",
        ],
        &Env::default(),
    );
    assert_eq!(
        10000000064_i64,
        got.remote_bwe
            .channel_observer_probe
            .estimate
            .collapse_threshold
            .as_nanos_i64(),
        "C98A-10 --rtc.congestion_control.remote_bwe.channel_observer_probe.estimate.collapse_threshold=10000000064ns must project the Duration leaf"
    );
    assert_eq!(
        base.remote_bwe
            .channel_observer_probe
            .estimate
            .downward_trend_max_wait
            .as_nanos_i64(),
        got.remote_bwe
            .channel_observer_probe
            .estimate
            .downward_trend_max_wait
            .as_nanos_i64(),
        "C98A-10 --rtc.congestion_control.remote_bwe.channel_observer_probe.estimate.collapse_threshold must not disturb rtc.congestion_control.remote_bwe.channel_observer_probe.estimate.downward_trend_max_wait"
    );
}

#[test]
fn cc_flag_remote_bwe_channel_observer_probe_estimate_downward_trend_max_wait() {
    // --rtc.congestion_control.remote_bwe.channel_observer_probe.estimate.downward_trend_max_wait: Go default '5s'
    let base = cc(&[], &Env::default());
    let got = cc(
        &[
            "--rtc.congestion_control.remote_bwe.channel_observer_probe.estimate.downward_trend_max_wait=11000000071ns",
        ],
        &Env::default(),
    );
    assert_eq!(
        11000000071_i64,
        got.remote_bwe
            .channel_observer_probe
            .estimate
            .downward_trend_max_wait
            .as_nanos_i64(),
        "C98A-11 --rtc.congestion_control.remote_bwe.channel_observer_probe.estimate.downward_trend_max_wait=11000000071ns must project the Duration leaf"
    );
    assert_eq!(
        base.remote_bwe
            .channel_observer_probe
            .estimate
            .collapse_threshold
            .as_nanos_i64(),
        got.remote_bwe
            .channel_observer_probe
            .estimate
            .collapse_threshold
            .as_nanos_i64(),
        "C98A-11 --rtc.congestion_control.remote_bwe.channel_observer_probe.estimate.downward_trend_max_wait must not disturb rtc.congestion_control.remote_bwe.channel_observer_probe.estimate.collapse_threshold"
    );
}

#[test]
fn cc_flag_remote_bwe_channel_observer_probe_estimate_downward_trend_threshold() {
    // --rtc.congestion_control.remote_bwe.channel_observer_probe.estimate.downward_trend_threshold: Go default '0'
    let base = cc(&[], &Env::default());
    let got = cc(
        &[
            "--rtc.congestion_control.remote_bwe.channel_observer_probe.estimate.downward_trend_threshold=0.171875",
        ],
        &Env::default(),
    );
    assert_eq!(
        0.171875_f64,
        got.remote_bwe
            .channel_observer_probe
            .estimate
            .downward_trend_threshold,
        "C98A-12 --rtc.congestion_control.remote_bwe.channel_observer_probe.estimate.downward_trend_threshold=0.171875 must project the Float leaf"
    );
    assert_eq!(
        base.remote_bwe
            .channel_observer_probe
            .estimate
            .collapse_threshold
            .as_nanos_i64(),
        got.remote_bwe
            .channel_observer_probe
            .estimate
            .collapse_threshold
            .as_nanos_i64(),
        "C98A-12 --rtc.congestion_control.remote_bwe.channel_observer_probe.estimate.downward_trend_threshold must not disturb rtc.congestion_control.remote_bwe.channel_observer_probe.estimate.collapse_threshold"
    );
}

#[test]
fn cc_flag_remote_bwe_channel_observer_probe_estimate_required_samples() {
    // --rtc.congestion_control.remote_bwe.channel_observer_probe.estimate.required_samples: Go default '3'
    let base = cc(&[], &Env::default());
    let got = cc(
        &[
            "--rtc.congestion_control.remote_bwe.channel_observer_probe.estimate.required_samples=100012",
        ],
        &Env::default(),
    );
    assert_eq!(
        100012_i64,
        got.remote_bwe
            .channel_observer_probe
            .estimate
            .required_samples,
        "C98A-13 --rtc.congestion_control.remote_bwe.channel_observer_probe.estimate.required_samples=100012 must project the Int leaf"
    );
    assert_eq!(
        base.remote_bwe
            .channel_observer_probe
            .estimate
            .collapse_threshold
            .as_nanos_i64(),
        got.remote_bwe
            .channel_observer_probe
            .estimate
            .collapse_threshold
            .as_nanos_i64(),
        "C98A-13 --rtc.congestion_control.remote_bwe.channel_observer_probe.estimate.required_samples must not disturb rtc.congestion_control.remote_bwe.channel_observer_probe.estimate.collapse_threshold"
    );
}

#[test]
fn cc_flag_remote_bwe_channel_observer_probe_estimate_required_samples_min() {
    // --rtc.congestion_control.remote_bwe.channel_observer_probe.estimate.required_samples_min: Go default '3'
    let base = cc(&[], &Env::default());
    let got = cc(
        &[
            "--rtc.congestion_control.remote_bwe.channel_observer_probe.estimate.required_samples_min=100013",
        ],
        &Env::default(),
    );
    assert_eq!(
        100013_i64,
        got.remote_bwe
            .channel_observer_probe
            .estimate
            .required_samples_min,
        "C98A-14 --rtc.congestion_control.remote_bwe.channel_observer_probe.estimate.required_samples_min=100013 must project the Int leaf"
    );
    assert_eq!(
        base.remote_bwe
            .channel_observer_probe
            .estimate
            .collapse_threshold
            .as_nanos_i64(),
        got.remote_bwe
            .channel_observer_probe
            .estimate
            .collapse_threshold
            .as_nanos_i64(),
        "C98A-14 --rtc.congestion_control.remote_bwe.channel_observer_probe.estimate.required_samples_min must not disturb rtc.congestion_control.remote_bwe.channel_observer_probe.estimate.collapse_threshold"
    );
}

#[test]
fn cc_flag_remote_bwe_channel_observer_probe_estimate_validity_window() {
    // --rtc.congestion_control.remote_bwe.channel_observer_probe.estimate.validity_window: Go default '10s'
    let base = cc(&[], &Env::default());
    let got = cc(
        &[
            "--rtc.congestion_control.remote_bwe.channel_observer_probe.estimate.validity_window=15000000099ns",
        ],
        &Env::default(),
    );
    assert_eq!(
        15000000099_i64,
        got.remote_bwe
            .channel_observer_probe
            .estimate
            .validity_window
            .as_nanos_i64(),
        "C98A-15 --rtc.congestion_control.remote_bwe.channel_observer_probe.estimate.validity_window=15000000099ns must project the Duration leaf"
    );
    assert_eq!(
        base.remote_bwe
            .channel_observer_probe
            .estimate
            .collapse_threshold
            .as_nanos_i64(),
        got.remote_bwe
            .channel_observer_probe
            .estimate
            .collapse_threshold
            .as_nanos_i64(),
        "C98A-15 --rtc.congestion_control.remote_bwe.channel_observer_probe.estimate.validity_window must not disturb rtc.congestion_control.remote_bwe.channel_observer_probe.estimate.collapse_threshold"
    );
}

#[test]
fn cc_flag_remote_bwe_channel_observer_probe_nack_ratio_threshold() {
    // --rtc.congestion_control.remote_bwe.channel_observer_probe.nack.ratio_threshold: Go default '0.04'
    let base = cc(&[], &Env::default());
    let got = cc(
        &[
            "--rtc.congestion_control.remote_bwe.channel_observer_probe.nack.ratio_threshold=0.234375",
        ],
        &Env::default(),
    );
    assert_eq!(
        0.234375_f64, got.remote_bwe.channel_observer_probe.nack.ratio_threshold,
        "C98A-16 --rtc.congestion_control.remote_bwe.channel_observer_probe.nack.ratio_threshold=0.234375 must project the Float leaf"
    );
    assert_eq!(
        base.remote_bwe
            .channel_observer_probe
            .nack
            .window_max_duration
            .as_nanos_i64(),
        got.remote_bwe
            .channel_observer_probe
            .nack
            .window_max_duration
            .as_nanos_i64(),
        "C98A-16 --rtc.congestion_control.remote_bwe.channel_observer_probe.nack.ratio_threshold must not disturb rtc.congestion_control.remote_bwe.channel_observer_probe.nack.window_max_duration"
    );
}

#[test]
fn cc_flag_remote_bwe_channel_observer_probe_nack_window_max_duration() {
    // --rtc.congestion_control.remote_bwe.channel_observer_probe.nack.window_max_duration: Go default '1s'
    let base = cc(&[], &Env::default());
    let got = cc(
        &[
            "--rtc.congestion_control.remote_bwe.channel_observer_probe.nack.window_max_duration=17000000113ns",
        ],
        &Env::default(),
    );
    assert_eq!(
        17000000113_i64,
        got.remote_bwe
            .channel_observer_probe
            .nack
            .window_max_duration
            .as_nanos_i64(),
        "C98A-17 --rtc.congestion_control.remote_bwe.channel_observer_probe.nack.window_max_duration=17000000113ns must project the Duration leaf"
    );
    assert_eq!(
        base.remote_bwe.channel_observer_probe.nack.ratio_threshold,
        got.remote_bwe.channel_observer_probe.nack.ratio_threshold,
        "C98A-17 --rtc.congestion_control.remote_bwe.channel_observer_probe.nack.window_max_duration must not disturb rtc.congestion_control.remote_bwe.channel_observer_probe.nack.ratio_threshold"
    );
}

#[test]
fn cc_flag_remote_bwe_channel_observer_probe_nack_window_min_duration() {
    // --rtc.congestion_control.remote_bwe.channel_observer_probe.nack.window_min_duration: Go default '500ms'
    let base = cc(&[], &Env::default());
    let got = cc(
        &[
            "--rtc.congestion_control.remote_bwe.channel_observer_probe.nack.window_min_duration=18000000120ns",
        ],
        &Env::default(),
    );
    assert_eq!(
        18000000120_i64,
        got.remote_bwe
            .channel_observer_probe
            .nack
            .window_min_duration
            .as_nanos_i64(),
        "C98A-18 --rtc.congestion_control.remote_bwe.channel_observer_probe.nack.window_min_duration=18000000120ns must project the Duration leaf"
    );
    assert_eq!(
        base.remote_bwe.channel_observer_probe.nack.ratio_threshold,
        got.remote_bwe.channel_observer_probe.nack.ratio_threshold,
        "C98A-18 --rtc.congestion_control.remote_bwe.channel_observer_probe.nack.window_min_duration must not disturb rtc.congestion_control.remote_bwe.channel_observer_probe.nack.ratio_threshold"
    );
}

#[test]
fn cc_flag_remote_bwe_expected_usage_threshold() {
    // --rtc.congestion_control.remote_bwe.expected_usage_threshold: Go default '0.95'
    let base = cc(&[], &Env::default());
    let got = cc(
        &["--rtc.congestion_control.remote_bwe.expected_usage_threshold=0.28125"],
        &Env::default(),
    );
    assert_eq!(
        0.28125_f64, got.remote_bwe.expected_usage_threshold,
        "C98A-19 --rtc.congestion_control.remote_bwe.expected_usage_threshold=0.28125 must project the Float leaf"
    );
    assert_eq!(
        base.remote_bwe.nack_ratio_attenuator, got.remote_bwe.nack_ratio_attenuator,
        "C98A-19 --rtc.congestion_control.remote_bwe.expected_usage_threshold must not disturb rtc.congestion_control.remote_bwe.nack_ratio_attenuator"
    );
}

#[test]
fn cc_flag_remote_bwe_nack_ratio_attenuator() {
    // --rtc.congestion_control.remote_bwe.nack_ratio_attenuator: Go default '0.4'
    let base = cc(&[], &Env::default());
    let got = cc(
        &["--rtc.congestion_control.remote_bwe.nack_ratio_attenuator=0.296875"],
        &Env::default(),
    );
    assert_eq!(
        0.296875_f64, got.remote_bwe.nack_ratio_attenuator,
        "C98A-20 --rtc.congestion_control.remote_bwe.nack_ratio_attenuator=0.296875 must project the Float leaf"
    );
    assert_eq!(
        base.remote_bwe.expected_usage_threshold, got.remote_bwe.expected_usage_threshold,
        "C98A-20 --rtc.congestion_control.remote_bwe.nack_ratio_attenuator must not disturb rtc.congestion_control.remote_bwe.expected_usage_threshold"
    );
}

#[test]
fn cc_flag_remote_bwe_probe_controller_probe_regulator_backoff_factor() {
    // --rtc.congestion_control.remote_bwe.probe_controller.probe_regulator.backoff_factor: Go default '1.5'
    let base = cc(&[], &Env::default());
    let got = cc(
        &[
            "--rtc.congestion_control.remote_bwe.probe_controller.probe_regulator.backoff_factor=0.3125",
        ],
        &Env::default(),
    );
    assert_eq!(
        0.3125_f64,
        got.remote_bwe
            .probe_controller
            .probe_regulator
            .backoff_factor,
        "C98A-21 --rtc.congestion_control.remote_bwe.probe_controller.probe_regulator.backoff_factor=0.3125 must project the Float leaf"
    );
    assert_eq!(
        base.remote_bwe
            .probe_controller
            .probe_regulator
            .base_interval
            .as_nanos_i64(),
        got.remote_bwe
            .probe_controller
            .probe_regulator
            .base_interval
            .as_nanos_i64(),
        "C98A-21 --rtc.congestion_control.remote_bwe.probe_controller.probe_regulator.backoff_factor must not disturb rtc.congestion_control.remote_bwe.probe_controller.probe_regulator.base_interval"
    );
}

#[test]
fn cc_flag_remote_bwe_probe_controller_probe_regulator_base_interval() {
    // --rtc.congestion_control.remote_bwe.probe_controller.probe_regulator.base_interval: Go default '3s'
    let base = cc(&[], &Env::default());
    let got = cc(
        &[
            "--rtc.congestion_control.remote_bwe.probe_controller.probe_regulator.base_interval=22000000148ns",
        ],
        &Env::default(),
    );
    assert_eq!(
        22000000148_i64,
        got.remote_bwe
            .probe_controller
            .probe_regulator
            .base_interval
            .as_nanos_i64(),
        "C98A-22 --rtc.congestion_control.remote_bwe.probe_controller.probe_regulator.base_interval=22000000148ns must project the Duration leaf"
    );
    assert_eq!(
        base.remote_bwe
            .probe_controller
            .probe_regulator
            .backoff_factor,
        got.remote_bwe
            .probe_controller
            .probe_regulator
            .backoff_factor,
        "C98A-22 --rtc.congestion_control.remote_bwe.probe_controller.probe_regulator.base_interval must not disturb rtc.congestion_control.remote_bwe.probe_controller.probe_regulator.backoff_factor"
    );
}

#[test]
fn cc_flag_remote_bwe_probe_controller_probe_regulator_duration_increase_factor() {
    // --rtc.congestion_control.remote_bwe.probe_controller.probe_regulator.duration_increase_factor: Go default '1.5'
    let base = cc(&[], &Env::default());
    let got = cc(
        &[
            "--rtc.congestion_control.remote_bwe.probe_controller.probe_regulator.duration_increase_factor=0.34375",
        ],
        &Env::default(),
    );
    assert_eq!(
        0.34375_f64,
        got.remote_bwe
            .probe_controller
            .probe_regulator
            .duration_increase_factor,
        "C98A-23 --rtc.congestion_control.remote_bwe.probe_controller.probe_regulator.duration_increase_factor=0.34375 must project the Float leaf"
    );
    assert_eq!(
        base.remote_bwe
            .probe_controller
            .probe_regulator
            .backoff_factor,
        got.remote_bwe
            .probe_controller
            .probe_regulator
            .backoff_factor,
        "C98A-23 --rtc.congestion_control.remote_bwe.probe_controller.probe_regulator.duration_increase_factor must not disturb rtc.congestion_control.remote_bwe.probe_controller.probe_regulator.backoff_factor"
    );
}

#[test]
fn cc_flag_remote_bwe_probe_controller_probe_regulator_max_duration() {
    // --rtc.congestion_control.remote_bwe.probe_controller.probe_regulator.max_duration: Go default '20s'
    let base = cc(&[], &Env::default());
    let got = cc(
        &[
            "--rtc.congestion_control.remote_bwe.probe_controller.probe_regulator.max_duration=24000000162ns",
        ],
        &Env::default(),
    );
    assert_eq!(
        24000000162_i64,
        got.remote_bwe
            .probe_controller
            .probe_regulator
            .max_duration
            .as_nanos_i64(),
        "C98A-24 --rtc.congestion_control.remote_bwe.probe_controller.probe_regulator.max_duration=24000000162ns must project the Duration leaf"
    );
    assert_eq!(
        base.remote_bwe
            .probe_controller
            .probe_regulator
            .backoff_factor,
        got.remote_bwe
            .probe_controller
            .probe_regulator
            .backoff_factor,
        "C98A-24 --rtc.congestion_control.remote_bwe.probe_controller.probe_regulator.max_duration must not disturb rtc.congestion_control.remote_bwe.probe_controller.probe_regulator.backoff_factor"
    );
}

#[test]
fn cc_flag_remote_bwe_probe_controller_probe_regulator_max_interval() {
    // --rtc.congestion_control.remote_bwe.probe_controller.probe_regulator.max_interval: Go default '2m0s'
    let base = cc(&[], &Env::default());
    let got = cc(
        &[
            "--rtc.congestion_control.remote_bwe.probe_controller.probe_regulator.max_interval=25000000169ns",
        ],
        &Env::default(),
    );
    assert_eq!(
        25000000169_i64,
        got.remote_bwe
            .probe_controller
            .probe_regulator
            .max_interval
            .as_nanos_i64(),
        "C98A-25 --rtc.congestion_control.remote_bwe.probe_controller.probe_regulator.max_interval=25000000169ns must project the Duration leaf"
    );
    assert_eq!(
        base.remote_bwe
            .probe_controller
            .probe_regulator
            .backoff_factor,
        got.remote_bwe
            .probe_controller
            .probe_regulator
            .backoff_factor,
        "C98A-25 --rtc.congestion_control.remote_bwe.probe_controller.probe_regulator.max_interval must not disturb rtc.congestion_control.remote_bwe.probe_controller.probe_regulator.backoff_factor"
    );
}

#[test]
fn cc_flag_remote_bwe_probe_controller_probe_regulator_min_duration() {
    // --rtc.congestion_control.remote_bwe.probe_controller.probe_regulator.min_duration: Go default '200ms'
    let base = cc(&[], &Env::default());
    let got = cc(
        &[
            "--rtc.congestion_control.remote_bwe.probe_controller.probe_regulator.min_duration=26000000176ns",
        ],
        &Env::default(),
    );
    assert_eq!(
        26000000176_i64,
        got.remote_bwe
            .probe_controller
            .probe_regulator
            .min_duration
            .as_nanos_i64(),
        "C98A-26 --rtc.congestion_control.remote_bwe.probe_controller.probe_regulator.min_duration=26000000176ns must project the Duration leaf"
    );
    assert_eq!(
        base.remote_bwe
            .probe_controller
            .probe_regulator
            .backoff_factor,
        got.remote_bwe
            .probe_controller
            .probe_regulator
            .backoff_factor,
        "C98A-26 --rtc.congestion_control.remote_bwe.probe_controller.probe_regulator.min_duration must not disturb rtc.congestion_control.remote_bwe.probe_controller.probe_regulator.backoff_factor"
    );
}

#[test]
fn cc_flag_remote_bwe_probe_controller_settle_wait_max() {
    // --rtc.congestion_control.remote_bwe.probe_controller.settle_wait_max: Go default '5s'
    let base = cc(&[], &Env::default());
    let got = cc(
        &["--rtc.congestion_control.remote_bwe.probe_controller.settle_wait_max=27000000183ns"],
        &Env::default(),
    );
    assert_eq!(
        27000000183_i64,
        got.remote_bwe
            .probe_controller
            .settle_wait_max
            .as_nanos_i64(),
        "C98A-27 --rtc.congestion_control.remote_bwe.probe_controller.settle_wait_max=27000000183ns must project the Duration leaf"
    );
    assert_eq!(
        base.remote_bwe
            .probe_controller
            .settle_wait_min
            .as_nanos_i64(),
        got.remote_bwe
            .probe_controller
            .settle_wait_min
            .as_nanos_i64(),
        "C98A-27 --rtc.congestion_control.remote_bwe.probe_controller.settle_wait_max must not disturb rtc.congestion_control.remote_bwe.probe_controller.settle_wait_min"
    );
}

#[test]
fn cc_flag_remote_bwe_probe_controller_settle_wait_min() {
    // --rtc.congestion_control.remote_bwe.probe_controller.settle_wait_min: Go default '250ms'
    let base = cc(&[], &Env::default());
    let got = cc(
        &["--rtc.congestion_control.remote_bwe.probe_controller.settle_wait_min=28000000190ns"],
        &Env::default(),
    );
    assert_eq!(
        28000000190_i64,
        got.remote_bwe
            .probe_controller
            .settle_wait_min
            .as_nanos_i64(),
        "C98A-28 --rtc.congestion_control.remote_bwe.probe_controller.settle_wait_min=28000000190ns must project the Duration leaf"
    );
    assert_eq!(
        base.remote_bwe
            .probe_controller
            .settle_wait_max
            .as_nanos_i64(),
        got.remote_bwe
            .probe_controller
            .settle_wait_max
            .as_nanos_i64(),
        "C98A-28 --rtc.congestion_control.remote_bwe.probe_controller.settle_wait_min must not disturb rtc.congestion_control.remote_bwe.probe_controller.settle_wait_max"
    );
}

#[test]
fn cc_flag_remote_bwe_probe_controller_settle_wait_num_rtt() {
    // --rtc.congestion_control.remote_bwe.probe_controller.settle_wait_num_rtt: Go default '5'
    let base = cc(&[], &Env::default());
    let got = cc(
        &["--rtc.congestion_control.remote_bwe.probe_controller.settle_wait_num_rtt=35"],
        &Env::default(),
    );
    assert_eq!(
        35_u32, got.remote_bwe.probe_controller.settle_wait_num_rtt,
        "C98A-29 --rtc.congestion_control.remote_bwe.probe_controller.settle_wait_num_rtt=35 must project the Uint leaf"
    );
    assert_eq!(
        base.remote_bwe
            .probe_controller
            .settle_wait_max
            .as_nanos_i64(),
        got.remote_bwe
            .probe_controller
            .settle_wait_max
            .as_nanos_i64(),
        "C98A-29 --rtc.congestion_control.remote_bwe.probe_controller.settle_wait_num_rtt must not disturb rtc.congestion_control.remote_bwe.probe_controller.settle_wait_max"
    );
}

#[test]
fn cc_flag_send_side_bwe_congestion_detector_congested_ctr_epsilon() {
    // --rtc.congestion_control.send_side_bwe.congestion_detector.congested_ctr_epsilon: Go default '0.05'
    let base = cc(&[], &Env::default());
    let got = cc(
        &[
            "--rtc.congestion_control.send_side_bwe.congestion_detector.congested_ctr_epsilon=0.453125",
        ],
        &Env::default(),
    );
    assert_eq!(
        0.453125_f64, got.send_side_bwe.congestion_detector.congested_ctr_epsilon,
        "C98A-30 --rtc.congestion_control.send_side_bwe.congestion_detector.congested_ctr_epsilon=0.453125 must project the Float leaf"
    );
    assert_eq!(
        base.send_side_bwe
            .congestion_detector
            .dqr_max_delay
            .as_nanos_i64(),
        got.send_side_bwe
            .congestion_detector
            .dqr_max_delay
            .as_nanos_i64(),
        "C98A-30 --rtc.congestion_control.send_side_bwe.congestion_detector.congested_ctr_epsilon must not disturb rtc.congestion_control.send_side_bwe.congestion_detector.dqr_max_delay"
    );
}

#[test]
fn cc_flag_send_side_bwe_congestion_detector_congested_ctr_trend_collapse_threshold() {
    // --rtc.congestion_control.send_side_bwe.congestion_detector.congested_ctr_trend.collapse_threshold: Go default '500ms'
    let base = cc(&[], &Env::default());
    let got = cc(
        &[
            "--rtc.congestion_control.send_side_bwe.congestion_detector.congested_ctr_trend.collapse_threshold=31000000211ns",
        ],
        &Env::default(),
    );
    assert_eq!(
        31000000211_i64,
        got.send_side_bwe
            .congestion_detector
            .congested_ctr_trend
            .collapse_threshold
            .as_nanos_i64(),
        "C98A-31 --rtc.congestion_control.send_side_bwe.congestion_detector.congested_ctr_trend.collapse_threshold=31000000211ns must project the Duration leaf"
    );
    assert_eq!(
        base.send_side_bwe
            .congestion_detector
            .congested_ctr_trend
            .downward_trend_max_wait
            .as_nanos_i64(),
        got.send_side_bwe
            .congestion_detector
            .congested_ctr_trend
            .downward_trend_max_wait
            .as_nanos_i64(),
        "C98A-31 --rtc.congestion_control.send_side_bwe.congestion_detector.congested_ctr_trend.collapse_threshold must not disturb rtc.congestion_control.send_side_bwe.congestion_detector.congested_ctr_trend.downward_trend_max_wait"
    );
}

#[test]
fn cc_flag_send_side_bwe_congestion_detector_congested_ctr_trend_downward_trend_max_wait() {
    // --rtc.congestion_control.send_side_bwe.congestion_detector.congested_ctr_trend.downward_trend_max_wait: Go default '2s'
    let base = cc(&[], &Env::default());
    let got = cc(
        &[
            "--rtc.congestion_control.send_side_bwe.congestion_detector.congested_ctr_trend.downward_trend_max_wait=32000000218ns",
        ],
        &Env::default(),
    );
    assert_eq!(
        32000000218_i64,
        got.send_side_bwe
            .congestion_detector
            .congested_ctr_trend
            .downward_trend_max_wait
            .as_nanos_i64(),
        "C98A-32 --rtc.congestion_control.send_side_bwe.congestion_detector.congested_ctr_trend.downward_trend_max_wait=32000000218ns must project the Duration leaf"
    );
    assert_eq!(
        base.send_side_bwe
            .congestion_detector
            .congested_ctr_trend
            .collapse_threshold
            .as_nanos_i64(),
        got.send_side_bwe
            .congestion_detector
            .congested_ctr_trend
            .collapse_threshold
            .as_nanos_i64(),
        "C98A-32 --rtc.congestion_control.send_side_bwe.congestion_detector.congested_ctr_trend.downward_trend_max_wait must not disturb rtc.congestion_control.send_side_bwe.congestion_detector.congested_ctr_trend.collapse_threshold"
    );
}

#[test]
fn cc_flag_send_side_bwe_congestion_detector_congested_ctr_trend_downward_trend_threshold() {
    // --rtc.congestion_control.send_side_bwe.congestion_detector.congested_ctr_trend.downward_trend_threshold: Go default '-0.5'
    let base = cc(&[], &Env::default());
    let got = cc(
        &[
            "--rtc.congestion_control.send_side_bwe.congestion_detector.congested_ctr_trend.downward_trend_threshold=0.5",
        ],
        &Env::default(),
    );
    assert_eq!(
        0.5_f64,
        got.send_side_bwe
            .congestion_detector
            .congested_ctr_trend
            .downward_trend_threshold,
        "C98A-33 --rtc.congestion_control.send_side_bwe.congestion_detector.congested_ctr_trend.downward_trend_threshold=0.5 must project the Float leaf"
    );
    assert_eq!(
        base.send_side_bwe
            .congestion_detector
            .congested_ctr_trend
            .collapse_threshold
            .as_nanos_i64(),
        got.send_side_bwe
            .congestion_detector
            .congested_ctr_trend
            .collapse_threshold
            .as_nanos_i64(),
        "C98A-33 --rtc.congestion_control.send_side_bwe.congestion_detector.congested_ctr_trend.downward_trend_threshold must not disturb rtc.congestion_control.send_side_bwe.congestion_detector.congested_ctr_trend.collapse_threshold"
    );
}

#[test]
fn cc_flag_send_side_bwe_congestion_detector_congested_ctr_trend_required_samples() {
    // --rtc.congestion_control.send_side_bwe.congestion_detector.congested_ctr_trend.required_samples: Go default '4'
    let base = cc(&[], &Env::default());
    let got = cc(
        &[
            "--rtc.congestion_control.send_side_bwe.congestion_detector.congested_ctr_trend.required_samples=100033",
        ],
        &Env::default(),
    );
    assert_eq!(
        100033_i64,
        got.send_side_bwe
            .congestion_detector
            .congested_ctr_trend
            .required_samples,
        "C98A-34 --rtc.congestion_control.send_side_bwe.congestion_detector.congested_ctr_trend.required_samples=100033 must project the Int leaf"
    );
    assert_eq!(
        base.send_side_bwe
            .congestion_detector
            .congested_ctr_trend
            .collapse_threshold
            .as_nanos_i64(),
        got.send_side_bwe
            .congestion_detector
            .congested_ctr_trend
            .collapse_threshold
            .as_nanos_i64(),
        "C98A-34 --rtc.congestion_control.send_side_bwe.congestion_detector.congested_ctr_trend.required_samples must not disturb rtc.congestion_control.send_side_bwe.congestion_detector.congested_ctr_trend.collapse_threshold"
    );
}

#[test]
fn cc_flag_send_side_bwe_congestion_detector_congested_ctr_trend_required_samples_min() {
    // --rtc.congestion_control.send_side_bwe.congestion_detector.congested_ctr_trend.required_samples_min: Go default '2'
    let base = cc(&[], &Env::default());
    let got = cc(
        &[
            "--rtc.congestion_control.send_side_bwe.congestion_detector.congested_ctr_trend.required_samples_min=100034",
        ],
        &Env::default(),
    );
    assert_eq!(
        100034_i64,
        got.send_side_bwe
            .congestion_detector
            .congested_ctr_trend
            .required_samples_min,
        "C98A-35 --rtc.congestion_control.send_side_bwe.congestion_detector.congested_ctr_trend.required_samples_min=100034 must project the Int leaf"
    );
    assert_eq!(
        base.send_side_bwe
            .congestion_detector
            .congested_ctr_trend
            .collapse_threshold
            .as_nanos_i64(),
        got.send_side_bwe
            .congestion_detector
            .congested_ctr_trend
            .collapse_threshold
            .as_nanos_i64(),
        "C98A-35 --rtc.congestion_control.send_side_bwe.congestion_detector.congested_ctr_trend.required_samples_min must not disturb rtc.congestion_control.send_side_bwe.congestion_detector.congested_ctr_trend.collapse_threshold"
    );
}

#[test]
fn cc_flag_send_side_bwe_congestion_detector_congested_ctr_trend_validity_window() {
    // --rtc.congestion_control.send_side_bwe.congestion_detector.congested_ctr_trend.validity_window: Go default '10s'
    let base = cc(&[], &Env::default());
    let got = cc(
        &[
            "--rtc.congestion_control.send_side_bwe.congestion_detector.congested_ctr_trend.validity_window=36000000246ns",
        ],
        &Env::default(),
    );
    assert_eq!(
        36000000246_i64,
        got.send_side_bwe
            .congestion_detector
            .congested_ctr_trend
            .validity_window
            .as_nanos_i64(),
        "C98A-36 --rtc.congestion_control.send_side_bwe.congestion_detector.congested_ctr_trend.validity_window=36000000246ns must project the Duration leaf"
    );
    assert_eq!(
        base.send_side_bwe
            .congestion_detector
            .congested_ctr_trend
            .collapse_threshold
            .as_nanos_i64(),
        got.send_side_bwe
            .congestion_detector
            .congested_ctr_trend
            .collapse_threshold
            .as_nanos_i64(),
        "C98A-36 --rtc.congestion_control.send_side_bwe.congestion_detector.congested_ctr_trend.validity_window must not disturb rtc.congestion_control.send_side_bwe.congestion_detector.congested_ctr_trend.collapse_threshold"
    );
}

#[test]
fn cc_flag_send_side_bwe_congestion_detector_congested_packet_group_max_window_duration() {
    // --rtc.congestion_control.send_side_bwe.congestion_detector.congested_packet_group.max_window_duration: Go default '150ms'
    let base = cc(&[], &Env::default());
    let got = cc(
        &[
            "--rtc.congestion_control.send_side_bwe.congestion_detector.congested_packet_group.max_window_duration=37000000253ns",
        ],
        &Env::default(),
    );
    assert_eq!(
        37000000253_i64,
        got.send_side_bwe
            .congestion_detector
            .congested_packet_group
            .max_window_duration
            .as_nanos_i64(),
        "C98A-37 --rtc.congestion_control.send_side_bwe.congestion_detector.congested_packet_group.max_window_duration=37000000253ns must project the Duration leaf"
    );
    assert_eq!(
        base.send_side_bwe
            .congestion_detector
            .congested_packet_group
            .min_packets,
        got.send_side_bwe
            .congestion_detector
            .congested_packet_group
            .min_packets,
        "C98A-37 --rtc.congestion_control.send_side_bwe.congestion_detector.congested_packet_group.max_window_duration must not disturb rtc.congestion_control.send_side_bwe.congestion_detector.congested_packet_group.min_packets"
    );
}

#[test]
fn cc_flag_send_side_bwe_congestion_detector_congested_packet_group_min_packets() {
    // --rtc.congestion_control.send_side_bwe.congestion_detector.congested_packet_group.min_packets: Go default '20'
    let base = cc(&[], &Env::default());
    let got = cc(
        &[
            "--rtc.congestion_control.send_side_bwe.congestion_detector.congested_packet_group.min_packets=100037",
        ],
        &Env::default(),
    );
    assert_eq!(
        100037_i64,
        got.send_side_bwe
            .congestion_detector
            .congested_packet_group
            .min_packets,
        "C98A-38 --rtc.congestion_control.send_side_bwe.congestion_detector.congested_packet_group.min_packets=100037 must project the Int leaf"
    );
    assert_eq!(
        base.send_side_bwe
            .congestion_detector
            .congested_packet_group
            .max_window_duration
            .as_nanos_i64(),
        got.send_side_bwe
            .congestion_detector
            .congested_packet_group
            .max_window_duration
            .as_nanos_i64(),
        "C98A-38 --rtc.congestion_control.send_side_bwe.congestion_detector.congested_packet_group.min_packets must not disturb rtc.congestion_control.send_side_bwe.congestion_detector.congested_packet_group.max_window_duration"
    );
}

#[test]
fn cc_flag_send_side_bwe_congestion_detector_dqr_max_delay() {
    // --rtc.congestion_control.send_side_bwe.congestion_detector.dqr_max_delay: Go default '20ms'
    let base = cc(&[], &Env::default());
    let got = cc(
        &["--rtc.congestion_control.send_side_bwe.congestion_detector.dqr_max_delay=39000000267ns"],
        &Env::default(),
    );
    assert_eq!(
        39000000267_i64,
        got.send_side_bwe
            .congestion_detector
            .dqr_max_delay
            .as_nanos_i64(),
        "C98A-39 --rtc.congestion_control.send_side_bwe.congestion_detector.dqr_max_delay=39000000267ns must project the Duration leaf"
    );
    assert_eq!(
        base.send_side_bwe.congestion_detector.congested_ctr_epsilon,
        got.send_side_bwe.congestion_detector.congested_ctr_epsilon,
        "C98A-39 --rtc.congestion_control.send_side_bwe.congestion_detector.dqr_max_delay must not disturb rtc.congestion_control.send_side_bwe.congestion_detector.congested_ctr_epsilon"
    );
}

#[test]
fn cc_flag_send_side_bwe_congestion_detector_dqr_max_weighted_loss() {
    // --rtc.congestion_control.send_side_bwe.congestion_detector.dqr_max_weighted_loss: Go default '0.1'
    let base = cc(&[], &Env::default());
    let got = cc(
        &[
            "--rtc.congestion_control.send_side_bwe.congestion_detector.dqr_max_weighted_loss=0.609375",
        ],
        &Env::default(),
    );
    assert_eq!(
        0.609375_f64, got.send_side_bwe.congestion_detector.dqr_max_weighted_loss,
        "C98A-40 --rtc.congestion_control.send_side_bwe.congestion_detector.dqr_max_weighted_loss=0.609375 must project the Float leaf"
    );
    assert_eq!(
        base.send_side_bwe.congestion_detector.congested_ctr_epsilon,
        got.send_side_bwe.congestion_detector.congested_ctr_epsilon,
        "C98A-40 --rtc.congestion_control.send_side_bwe.congestion_detector.dqr_max_weighted_loss must not disturb rtc.congestion_control.send_side_bwe.congestion_detector.congested_ctr_epsilon"
    );
}

#[test]
fn cc_flag_send_side_bwe_congestion_detector_estimaton_window_duration() {
    // --rtc.congestion_control.send_side_bwe.congestion_detector.estimaton_window_duration: Go default '1s'
    let base = cc(&[], &Env::default());
    let got = cc(
        &[
            "--rtc.congestion_control.send_side_bwe.congestion_detector.estimaton_window_duration=41000000281ns",
        ],
        &Env::default(),
    );
    assert_eq!(
        41000000281_i64,
        got.send_side_bwe
            .congestion_detector
            .estimation_window_duration
            .as_nanos_i64(),
        "C98A-41 --rtc.congestion_control.send_side_bwe.congestion_detector.estimaton_window_duration=41000000281ns must project the Duration leaf"
    );
    assert_eq!(
        base.send_side_bwe.congestion_detector.congested_ctr_epsilon,
        got.send_side_bwe.congestion_detector.congested_ctr_epsilon,
        "C98A-41 --rtc.congestion_control.send_side_bwe.congestion_detector.estimaton_window_duration must not disturb rtc.congestion_control.send_side_bwe.congestion_detector.congested_ctr_epsilon"
    );
}

#[test]
fn cc_flag_send_side_bwe_congestion_detector_jqr_min_delay() {
    // --rtc.congestion_control.send_side_bwe.congestion_detector.jqr_min_delay: Go default '50ms'
    let base = cc(&[], &Env::default());
    let got = cc(
        &["--rtc.congestion_control.send_side_bwe.congestion_detector.jqr_min_delay=42000000288ns"],
        &Env::default(),
    );
    assert_eq!(
        42000000288_i64,
        got.send_side_bwe
            .congestion_detector
            .jqr_min_delay
            .as_nanos_i64(),
        "C98A-42 --rtc.congestion_control.send_side_bwe.congestion_detector.jqr_min_delay=42000000288ns must project the Duration leaf"
    );
    assert_eq!(
        base.send_side_bwe.congestion_detector.congested_ctr_epsilon,
        got.send_side_bwe.congestion_detector.congested_ctr_epsilon,
        "C98A-42 --rtc.congestion_control.send_side_bwe.congestion_detector.jqr_min_delay must not disturb rtc.congestion_control.send_side_bwe.congestion_detector.congested_ctr_epsilon"
    );
}

#[test]
fn cc_flag_send_side_bwe_congestion_detector_jqr_min_trend_coefficient() {
    // --rtc.congestion_control.send_side_bwe.congestion_detector.jqr_min_trend_coefficient: Go default '0.8'
    let base = cc(&[], &Env::default());
    let got = cc(
        &[
            "--rtc.congestion_control.send_side_bwe.congestion_detector.jqr_min_trend_coefficient=0.65625",
        ],
        &Env::default(),
    );
    assert_eq!(
        0.65625_f64,
        got.send_side_bwe
            .congestion_detector
            .jqr_min_trend_coefficient,
        "C98A-43 --rtc.congestion_control.send_side_bwe.congestion_detector.jqr_min_trend_coefficient=0.65625 must project the Float leaf"
    );
    assert_eq!(
        base.send_side_bwe.congestion_detector.congested_ctr_epsilon,
        got.send_side_bwe.congestion_detector.congested_ctr_epsilon,
        "C98A-43 --rtc.congestion_control.send_side_bwe.congestion_detector.jqr_min_trend_coefficient must not disturb rtc.congestion_control.send_side_bwe.congestion_detector.congested_ctr_epsilon"
    );
}

#[test]
fn cc_flag_send_side_bwe_congestion_detector_jqr_min_weighted_loss() {
    // --rtc.congestion_control.send_side_bwe.congestion_detector.jqr_min_weighted_loss: Go default '0.25'
    let base = cc(&[], &Env::default());
    let got = cc(
        &[
            "--rtc.congestion_control.send_side_bwe.congestion_detector.jqr_min_weighted_loss=0.671875",
        ],
        &Env::default(),
    );
    assert_eq!(
        0.671875_f64, got.send_side_bwe.congestion_detector.jqr_min_weighted_loss,
        "C98A-44 --rtc.congestion_control.send_side_bwe.congestion_detector.jqr_min_weighted_loss=0.671875 must project the Float leaf"
    );
    assert_eq!(
        base.send_side_bwe.congestion_detector.congested_ctr_epsilon,
        got.send_side_bwe.congestion_detector.congested_ctr_epsilon,
        "C98A-44 --rtc.congestion_control.send_side_bwe.congestion_detector.jqr_min_weighted_loss must not disturb rtc.congestion_control.send_side_bwe.congestion_detector.congested_ctr_epsilon"
    );
}

#[test]
fn cc_flag_send_side_bwe_congestion_detector_loss_congested_dqr_min_duration() {
    // --rtc.congestion_control.send_side_bwe.congestion_detector.loss_congested_dqr.min_duration: Go default '600ms'
    let base = cc(&[], &Env::default());
    let got = cc(
        &[
            "--rtc.congestion_control.send_side_bwe.congestion_detector.loss_congested_dqr.min_duration=45000000309ns",
        ],
        &Env::default(),
    );
    assert_eq!(
        45000000309_i64,
        got.send_side_bwe
            .congestion_detector
            .loss_congested_dqr
            .min_duration
            .as_nanos_i64(),
        "C98A-45 --rtc.congestion_control.send_side_bwe.congestion_detector.loss_congested_dqr.min_duration=45000000309ns must project the Duration leaf"
    );
    assert_eq!(
        base.send_side_bwe
            .congestion_detector
            .loss_congested_dqr
            .min_number_of_groups,
        got.send_side_bwe
            .congestion_detector
            .loss_congested_dqr
            .min_number_of_groups,
        "C98A-45 --rtc.congestion_control.send_side_bwe.congestion_detector.loss_congested_dqr.min_duration must not disturb rtc.congestion_control.send_side_bwe.congestion_detector.loss_congested_dqr.min_number_of_groups"
    );
}

#[test]
fn cc_flag_send_side_bwe_congestion_detector_loss_congested_dqr_min_number_of_groups() {
    // --rtc.congestion_control.send_side_bwe.congestion_detector.loss_congested_dqr.min_number_of_groups: Go default '6'
    let base = cc(&[], &Env::default());
    let got = cc(
        &[
            "--rtc.congestion_control.send_side_bwe.congestion_detector.loss_congested_dqr.min_number_of_groups=100045",
        ],
        &Env::default(),
    );
    assert_eq!(
        100045_i64,
        got.send_side_bwe
            .congestion_detector
            .loss_congested_dqr
            .min_number_of_groups,
        "C98A-46 --rtc.congestion_control.send_side_bwe.congestion_detector.loss_congested_dqr.min_number_of_groups=100045 must project the Int leaf"
    );
    assert_eq!(
        base.send_side_bwe
            .congestion_detector
            .loss_congested_dqr
            .min_duration
            .as_nanos_i64(),
        got.send_side_bwe
            .congestion_detector
            .loss_congested_dqr
            .min_duration
            .as_nanos_i64(),
        "C98A-46 --rtc.congestion_control.send_side_bwe.congestion_detector.loss_congested_dqr.min_number_of_groups must not disturb rtc.congestion_control.send_side_bwe.congestion_detector.loss_congested_dqr.min_duration"
    );
}

#[test]
fn cc_flag_send_side_bwe_congestion_detector_loss_congested_jqr_min_duration() {
    // --rtc.congestion_control.send_side_bwe.congestion_detector.loss_congested_jqr.min_duration: Go default '600ms'
    let base = cc(&[], &Env::default());
    let got = cc(
        &[
            "--rtc.congestion_control.send_side_bwe.congestion_detector.loss_congested_jqr.min_duration=47000000323ns",
        ],
        &Env::default(),
    );
    assert_eq!(
        47000000323_i64,
        got.send_side_bwe
            .congestion_detector
            .loss_congested_jqr
            .min_duration
            .as_nanos_i64(),
        "C98A-47 --rtc.congestion_control.send_side_bwe.congestion_detector.loss_congested_jqr.min_duration=47000000323ns must project the Duration leaf"
    );
    assert_eq!(
        base.send_side_bwe
            .congestion_detector
            .loss_congested_jqr
            .min_number_of_groups,
        got.send_side_bwe
            .congestion_detector
            .loss_congested_jqr
            .min_number_of_groups,
        "C98A-47 --rtc.congestion_control.send_side_bwe.congestion_detector.loss_congested_jqr.min_duration must not disturb rtc.congestion_control.send_side_bwe.congestion_detector.loss_congested_jqr.min_number_of_groups"
    );
}

#[test]
fn cc_flag_send_side_bwe_congestion_detector_loss_congested_jqr_min_number_of_groups() {
    // --rtc.congestion_control.send_side_bwe.congestion_detector.loss_congested_jqr.min_number_of_groups: Go default '6'
    let base = cc(&[], &Env::default());
    let got = cc(
        &[
            "--rtc.congestion_control.send_side_bwe.congestion_detector.loss_congested_jqr.min_number_of_groups=100047",
        ],
        &Env::default(),
    );
    assert_eq!(
        100047_i64,
        got.send_side_bwe
            .congestion_detector
            .loss_congested_jqr
            .min_number_of_groups,
        "C98A-48 --rtc.congestion_control.send_side_bwe.congestion_detector.loss_congested_jqr.min_number_of_groups=100047 must project the Int leaf"
    );
    assert_eq!(
        base.send_side_bwe
            .congestion_detector
            .loss_congested_jqr
            .min_duration
            .as_nanos_i64(),
        got.send_side_bwe
            .congestion_detector
            .loss_congested_jqr
            .min_duration
            .as_nanos_i64(),
        "C98A-48 --rtc.congestion_control.send_side_bwe.congestion_detector.loss_congested_jqr.min_number_of_groups must not disturb rtc.congestion_control.send_side_bwe.congestion_detector.loss_congested_jqr.min_duration"
    );
}

#[test]
fn cc_flag_send_side_bwe_congestion_detector_loss_early_warning_dqr_min_duration() {
    // --rtc.congestion_control.send_side_bwe.congestion_detector.loss_early_warning_dqr.min_duration: Go default '400ms'
    let base = cc(&[], &Env::default());
    let got = cc(
        &[
            "--rtc.congestion_control.send_side_bwe.congestion_detector.loss_early_warning_dqr.min_duration=49000000337ns",
        ],
        &Env::default(),
    );
    assert_eq!(
        49000000337_i64,
        got.send_side_bwe
            .congestion_detector
            .loss_early_warning_dqr
            .min_duration
            .as_nanos_i64(),
        "C98A-49 --rtc.congestion_control.send_side_bwe.congestion_detector.loss_early_warning_dqr.min_duration=49000000337ns must project the Duration leaf"
    );
    assert_eq!(
        base.send_side_bwe
            .congestion_detector
            .loss_early_warning_dqr
            .min_number_of_groups,
        got.send_side_bwe
            .congestion_detector
            .loss_early_warning_dqr
            .min_number_of_groups,
        "C98A-49 --rtc.congestion_control.send_side_bwe.congestion_detector.loss_early_warning_dqr.min_duration must not disturb rtc.congestion_control.send_side_bwe.congestion_detector.loss_early_warning_dqr.min_number_of_groups"
    );
}

#[test]
fn cc_flag_send_side_bwe_congestion_detector_loss_early_warning_dqr_min_number_of_groups() {
    // --rtc.congestion_control.send_side_bwe.congestion_detector.loss_early_warning_dqr.min_number_of_groups: Go default '4'
    let base = cc(&[], &Env::default());
    let got = cc(
        &[
            "--rtc.congestion_control.send_side_bwe.congestion_detector.loss_early_warning_dqr.min_number_of_groups=100049",
        ],
        &Env::default(),
    );
    assert_eq!(
        100049_i64,
        got.send_side_bwe
            .congestion_detector
            .loss_early_warning_dqr
            .min_number_of_groups,
        "C98A-50 --rtc.congestion_control.send_side_bwe.congestion_detector.loss_early_warning_dqr.min_number_of_groups=100049 must project the Int leaf"
    );
    assert_eq!(
        base.send_side_bwe
            .congestion_detector
            .loss_early_warning_dqr
            .min_duration
            .as_nanos_i64(),
        got.send_side_bwe
            .congestion_detector
            .loss_early_warning_dqr
            .min_duration
            .as_nanos_i64(),
        "C98A-50 --rtc.congestion_control.send_side_bwe.congestion_detector.loss_early_warning_dqr.min_number_of_groups must not disturb rtc.congestion_control.send_side_bwe.congestion_detector.loss_early_warning_dqr.min_duration"
    );
}

#[test]
fn cc_flag_send_side_bwe_congestion_detector_loss_early_warning_jqr_min_duration() {
    // --rtc.congestion_control.send_side_bwe.congestion_detector.loss_early_warning_jqr.min_duration: Go default '300ms'
    let base = cc(&[], &Env::default());
    let got = cc(
        &[
            "--rtc.congestion_control.send_side_bwe.congestion_detector.loss_early_warning_jqr.min_duration=51000000351ns",
        ],
        &Env::default(),
    );
    assert_eq!(
        51000000351_i64,
        got.send_side_bwe
            .congestion_detector
            .loss_early_warning_jqr
            .min_duration
            .as_nanos_i64(),
        "C98A-51 --rtc.congestion_control.send_side_bwe.congestion_detector.loss_early_warning_jqr.min_duration=51000000351ns must project the Duration leaf"
    );
    assert_eq!(
        base.send_side_bwe
            .congestion_detector
            .loss_early_warning_jqr
            .min_number_of_groups,
        got.send_side_bwe
            .congestion_detector
            .loss_early_warning_jqr
            .min_number_of_groups,
        "C98A-51 --rtc.congestion_control.send_side_bwe.congestion_detector.loss_early_warning_jqr.min_duration must not disturb rtc.congestion_control.send_side_bwe.congestion_detector.loss_early_warning_jqr.min_number_of_groups"
    );
}

#[test]
fn cc_flag_send_side_bwe_congestion_detector_loss_early_warning_jqr_min_number_of_groups() {
    // --rtc.congestion_control.send_side_bwe.congestion_detector.loss_early_warning_jqr.min_number_of_groups: Go default '3'
    let base = cc(&[], &Env::default());
    let got = cc(
        &[
            "--rtc.congestion_control.send_side_bwe.congestion_detector.loss_early_warning_jqr.min_number_of_groups=100051",
        ],
        &Env::default(),
    );
    assert_eq!(
        100051_i64,
        got.send_side_bwe
            .congestion_detector
            .loss_early_warning_jqr
            .min_number_of_groups,
        "C98A-52 --rtc.congestion_control.send_side_bwe.congestion_detector.loss_early_warning_jqr.min_number_of_groups=100051 must project the Int leaf"
    );
    assert_eq!(
        base.send_side_bwe
            .congestion_detector
            .loss_early_warning_jqr
            .min_duration
            .as_nanos_i64(),
        got.send_side_bwe
            .congestion_detector
            .loss_early_warning_jqr
            .min_duration
            .as_nanos_i64(),
        "C98A-52 --rtc.congestion_control.send_side_bwe.congestion_detector.loss_early_warning_jqr.min_number_of_groups must not disturb rtc.congestion_control.send_side_bwe.congestion_detector.loss_early_warning_jqr.min_duration"
    );
}

#[test]
fn cc_flag_send_side_bwe_congestion_detector_packet_group_max_window_duration() {
    // --rtc.congestion_control.send_side_bwe.congestion_detector.packet_group.max_window_duration: Go default '500ms'
    let base = cc(&[], &Env::default());
    let got = cc(
        &[
            "--rtc.congestion_control.send_side_bwe.congestion_detector.packet_group.max_window_duration=53000000365ns",
        ],
        &Env::default(),
    );
    assert_eq!(
        53000000365_i64,
        got.send_side_bwe
            .congestion_detector
            .packet_group
            .max_window_duration
            .as_nanos_i64(),
        "C98A-53 --rtc.congestion_control.send_side_bwe.congestion_detector.packet_group.max_window_duration=53000000365ns must project the Duration leaf"
    );
    assert_eq!(
        base.send_side_bwe
            .congestion_detector
            .packet_group
            .min_packets,
        got.send_side_bwe
            .congestion_detector
            .packet_group
            .min_packets,
        "C98A-53 --rtc.congestion_control.send_side_bwe.congestion_detector.packet_group.max_window_duration must not disturb rtc.congestion_control.send_side_bwe.congestion_detector.packet_group.min_packets"
    );
}

#[test]
fn cc_flag_send_side_bwe_congestion_detector_packet_group_min_packets() {
    // --rtc.congestion_control.send_side_bwe.congestion_detector.packet_group.min_packets: Go default '30'
    let base = cc(&[], &Env::default());
    let got = cc(
        &[
            "--rtc.congestion_control.send_side_bwe.congestion_detector.packet_group.min_packets=100053",
        ],
        &Env::default(),
    );
    assert_eq!(
        100053_i64,
        got.send_side_bwe
            .congestion_detector
            .packet_group
            .min_packets,
        "C98A-54 --rtc.congestion_control.send_side_bwe.congestion_detector.packet_group.min_packets=100053 must project the Int leaf"
    );
    assert_eq!(
        base.send_side_bwe
            .congestion_detector
            .packet_group
            .max_window_duration
            .as_nanos_i64(),
        got.send_side_bwe
            .congestion_detector
            .packet_group
            .max_window_duration
            .as_nanos_i64(),
        "C98A-54 --rtc.congestion_control.send_side_bwe.congestion_detector.packet_group.min_packets must not disturb rtc.congestion_control.send_side_bwe.congestion_detector.packet_group.max_window_duration"
    );
}

#[test]
fn cc_flag_send_side_bwe_congestion_detector_packet_group_max_age() {
    // --rtc.congestion_control.send_side_bwe.congestion_detector.packet_group_max_age: Go default '10s'
    let base = cc(&[], &Env::default());
    let got = cc(
        &[
            "--rtc.congestion_control.send_side_bwe.congestion_detector.packet_group_max_age=55000000379ns",
        ],
        &Env::default(),
    );
    assert_eq!(
        55000000379_i64,
        got.send_side_bwe
            .congestion_detector
            .packet_group_max_age
            .as_nanos_i64(),
        "C98A-55 --rtc.congestion_control.send_side_bwe.congestion_detector.packet_group_max_age=55000000379ns must project the Duration leaf"
    );
    assert_eq!(
        base.send_side_bwe.congestion_detector.congested_ctr_epsilon,
        got.send_side_bwe.congestion_detector.congested_ctr_epsilon,
        "C98A-55 --rtc.congestion_control.send_side_bwe.congestion_detector.packet_group_max_age must not disturb rtc.congestion_control.send_side_bwe.congestion_detector.congested_ctr_epsilon"
    );
}

#[test]
fn cc_flag_send_side_bwe_congestion_detector_probe_packet_group_packet_group_max_window_duration() {
    // --rtc.congestion_control.send_side_bwe.congestion_detector.probe_packet_group.packet_group.max_window_duration: Go default '1m0s'
    let base = cc(&[], &Env::default());
    let got = cc(
        &[
            "--rtc.congestion_control.send_side_bwe.congestion_detector.probe_packet_group.packet_group.max_window_duration=56000000386ns",
        ],
        &Env::default(),
    );
    assert_eq!(
        56000000386_i64,
        got.send_side_bwe
            .congestion_detector
            .probe_packet_group
            .packet_group
            .max_window_duration
            .as_nanos_i64(),
        "C98A-56 --rtc.congestion_control.send_side_bwe.congestion_detector.probe_packet_group.packet_group.max_window_duration=56000000386ns must project the Duration leaf"
    );
    assert_eq!(
        base.send_side_bwe
            .congestion_detector
            .probe_packet_group
            .packet_group
            .min_packets,
        got.send_side_bwe
            .congestion_detector
            .probe_packet_group
            .packet_group
            .min_packets,
        "C98A-56 --rtc.congestion_control.send_side_bwe.congestion_detector.probe_packet_group.packet_group.max_window_duration must not disturb rtc.congestion_control.send_side_bwe.congestion_detector.probe_packet_group.packet_group.min_packets"
    );
}

#[test]
fn cc_flag_send_side_bwe_congestion_detector_probe_packet_group_packet_group_min_packets() {
    // --rtc.congestion_control.send_side_bwe.congestion_detector.probe_packet_group.packet_group.min_packets: Go default '16384'
    let base = cc(&[], &Env::default());
    let got = cc(
        &[
            "--rtc.congestion_control.send_side_bwe.congestion_detector.probe_packet_group.packet_group.min_packets=100056",
        ],
        &Env::default(),
    );
    assert_eq!(
        100056_i64,
        got.send_side_bwe
            .congestion_detector
            .probe_packet_group
            .packet_group
            .min_packets,
        "C98A-57 --rtc.congestion_control.send_side_bwe.congestion_detector.probe_packet_group.packet_group.min_packets=100056 must project the Int leaf"
    );
    assert_eq!(
        base.send_side_bwe
            .congestion_detector
            .probe_packet_group
            .packet_group
            .max_window_duration
            .as_nanos_i64(),
        got.send_side_bwe
            .congestion_detector
            .probe_packet_group
            .packet_group
            .max_window_duration
            .as_nanos_i64(),
        "C98A-57 --rtc.congestion_control.send_side_bwe.congestion_detector.probe_packet_group.packet_group.min_packets must not disturb rtc.congestion_control.send_side_bwe.congestion_detector.probe_packet_group.packet_group.max_window_duration"
    );
}

#[test]
fn cc_flag_send_side_bwe_congestion_detector_probe_packet_group_settle_wait_max() {
    // --rtc.congestion_control.send_side_bwe.congestion_detector.probe_packet_group.settle_wait_max: Go default '5s'
    let base = cc(&[], &Env::default());
    let got = cc(
        &[
            "--rtc.congestion_control.send_side_bwe.congestion_detector.probe_packet_group.settle_wait_max=58000000400ns",
        ],
        &Env::default(),
    );
    assert_eq!(
        58000000400_i64,
        got.send_side_bwe
            .congestion_detector
            .probe_packet_group
            .settle_wait_max
            .as_nanos_i64(),
        "C98A-58 --rtc.congestion_control.send_side_bwe.congestion_detector.probe_packet_group.settle_wait_max=58000000400ns must project the Duration leaf"
    );
    assert_eq!(
        base.send_side_bwe
            .congestion_detector
            .probe_packet_group
            .settle_wait_min
            .as_nanos_i64(),
        got.send_side_bwe
            .congestion_detector
            .probe_packet_group
            .settle_wait_min
            .as_nanos_i64(),
        "C98A-58 --rtc.congestion_control.send_side_bwe.congestion_detector.probe_packet_group.settle_wait_max must not disturb rtc.congestion_control.send_side_bwe.congestion_detector.probe_packet_group.settle_wait_min"
    );
}

#[test]
fn cc_flag_send_side_bwe_congestion_detector_probe_packet_group_settle_wait_min() {
    // --rtc.congestion_control.send_side_bwe.congestion_detector.probe_packet_group.settle_wait_min: Go default '250ms'
    let base = cc(&[], &Env::default());
    let got = cc(
        &[
            "--rtc.congestion_control.send_side_bwe.congestion_detector.probe_packet_group.settle_wait_min=59000000407ns",
        ],
        &Env::default(),
    );
    assert_eq!(
        59000000407_i64,
        got.send_side_bwe
            .congestion_detector
            .probe_packet_group
            .settle_wait_min
            .as_nanos_i64(),
        "C98A-59 --rtc.congestion_control.send_side_bwe.congestion_detector.probe_packet_group.settle_wait_min=59000000407ns must project the Duration leaf"
    );
    assert_eq!(
        base.send_side_bwe
            .congestion_detector
            .probe_packet_group
            .settle_wait_max
            .as_nanos_i64(),
        got.send_side_bwe
            .congestion_detector
            .probe_packet_group
            .settle_wait_max
            .as_nanos_i64(),
        "C98A-59 --rtc.congestion_control.send_side_bwe.congestion_detector.probe_packet_group.settle_wait_min must not disturb rtc.congestion_control.send_side_bwe.congestion_detector.probe_packet_group.settle_wait_max"
    );
}

#[test]
fn cc_flag_send_side_bwe_congestion_detector_probe_packet_group_settle_wait_num_rtt() {
    // --rtc.congestion_control.send_side_bwe.congestion_detector.probe_packet_group.settle_wait_num_rtt: Go default '5'
    let base = cc(&[], &Env::default());
    let got = cc(
        &[
            "--rtc.congestion_control.send_side_bwe.congestion_detector.probe_packet_group.settle_wait_num_rtt=66",
        ],
        &Env::default(),
    );
    assert_eq!(
        66_u32,
        got.send_side_bwe
            .congestion_detector
            .probe_packet_group
            .settle_wait_num_rtt,
        "C98A-60 --rtc.congestion_control.send_side_bwe.congestion_detector.probe_packet_group.settle_wait_num_rtt=66 must project the Uint leaf"
    );
    assert_eq!(
        base.send_side_bwe
            .congestion_detector
            .probe_packet_group
            .settle_wait_max
            .as_nanos_i64(),
        got.send_side_bwe
            .congestion_detector
            .probe_packet_group
            .settle_wait_max
            .as_nanos_i64(),
        "C98A-60 --rtc.congestion_control.send_side_bwe.congestion_detector.probe_packet_group.settle_wait_num_rtt must not disturb rtc.congestion_control.send_side_bwe.congestion_detector.probe_packet_group.settle_wait_max"
    );
}

#[test]
fn cc_flag_send_side_bwe_congestion_detector_probe_regulator_backoff_factor() {
    // --rtc.congestion_control.send_side_bwe.congestion_detector.probe_regulator.backoff_factor: Go default '1.5'
    let base = cc(&[], &Env::default());
    let got = cc(
        &[
            "--rtc.congestion_control.send_side_bwe.congestion_detector.probe_regulator.backoff_factor=0.9375",
        ],
        &Env::default(),
    );
    assert_eq!(
        0.9375_f64,
        got.send_side_bwe
            .congestion_detector
            .probe_regulator
            .backoff_factor,
        "C98A-61 --rtc.congestion_control.send_side_bwe.congestion_detector.probe_regulator.backoff_factor=0.9375 must project the Float leaf"
    );
    assert_eq!(
        base.send_side_bwe
            .congestion_detector
            .probe_regulator
            .base_interval
            .as_nanos_i64(),
        got.send_side_bwe
            .congestion_detector
            .probe_regulator
            .base_interval
            .as_nanos_i64(),
        "C98A-61 --rtc.congestion_control.send_side_bwe.congestion_detector.probe_regulator.backoff_factor must not disturb rtc.congestion_control.send_side_bwe.congestion_detector.probe_regulator.base_interval"
    );
}

#[test]
fn cc_flag_send_side_bwe_congestion_detector_probe_regulator_base_interval() {
    // --rtc.congestion_control.send_side_bwe.congestion_detector.probe_regulator.base_interval: Go default '3s'
    let base = cc(&[], &Env::default());
    let got = cc(
        &[
            "--rtc.congestion_control.send_side_bwe.congestion_detector.probe_regulator.base_interval=62000000428ns",
        ],
        &Env::default(),
    );
    assert_eq!(
        62000000428_i64,
        got.send_side_bwe
            .congestion_detector
            .probe_regulator
            .base_interval
            .as_nanos_i64(),
        "C98A-62 --rtc.congestion_control.send_side_bwe.congestion_detector.probe_regulator.base_interval=62000000428ns must project the Duration leaf"
    );
    assert_eq!(
        base.send_side_bwe
            .congestion_detector
            .probe_regulator
            .backoff_factor,
        got.send_side_bwe
            .congestion_detector
            .probe_regulator
            .backoff_factor,
        "C98A-62 --rtc.congestion_control.send_side_bwe.congestion_detector.probe_regulator.base_interval must not disturb rtc.congestion_control.send_side_bwe.congestion_detector.probe_regulator.backoff_factor"
    );
}

#[test]
fn cc_flag_send_side_bwe_congestion_detector_probe_regulator_duration_increase_factor() {
    // --rtc.congestion_control.send_side_bwe.congestion_detector.probe_regulator.duration_increase_factor: Go default '1.5'
    let base = cc(&[], &Env::default());
    let got = cc(
        &[
            "--rtc.congestion_control.send_side_bwe.congestion_detector.probe_regulator.duration_increase_factor=0.96875",
        ],
        &Env::default(),
    );
    assert_eq!(
        0.96875_f64,
        got.send_side_bwe
            .congestion_detector
            .probe_regulator
            .duration_increase_factor,
        "C98A-63 --rtc.congestion_control.send_side_bwe.congestion_detector.probe_regulator.duration_increase_factor=0.96875 must project the Float leaf"
    );
    assert_eq!(
        base.send_side_bwe
            .congestion_detector
            .probe_regulator
            .backoff_factor,
        got.send_side_bwe
            .congestion_detector
            .probe_regulator
            .backoff_factor,
        "C98A-63 --rtc.congestion_control.send_side_bwe.congestion_detector.probe_regulator.duration_increase_factor must not disturb rtc.congestion_control.send_side_bwe.congestion_detector.probe_regulator.backoff_factor"
    );
}

#[test]
fn cc_flag_send_side_bwe_congestion_detector_probe_regulator_max_duration() {
    // --rtc.congestion_control.send_side_bwe.congestion_detector.probe_regulator.max_duration: Go default '20s'
    let base = cc(&[], &Env::default());
    let got = cc(
        &[
            "--rtc.congestion_control.send_side_bwe.congestion_detector.probe_regulator.max_duration=64000000442ns",
        ],
        &Env::default(),
    );
    assert_eq!(
        64000000442_i64,
        got.send_side_bwe
            .congestion_detector
            .probe_regulator
            .max_duration
            .as_nanos_i64(),
        "C98A-64 --rtc.congestion_control.send_side_bwe.congestion_detector.probe_regulator.max_duration=64000000442ns must project the Duration leaf"
    );
    assert_eq!(
        base.send_side_bwe
            .congestion_detector
            .probe_regulator
            .backoff_factor,
        got.send_side_bwe
            .congestion_detector
            .probe_regulator
            .backoff_factor,
        "C98A-64 --rtc.congestion_control.send_side_bwe.congestion_detector.probe_regulator.max_duration must not disturb rtc.congestion_control.send_side_bwe.congestion_detector.probe_regulator.backoff_factor"
    );
}

#[test]
fn cc_flag_send_side_bwe_congestion_detector_probe_regulator_max_interval() {
    // --rtc.congestion_control.send_side_bwe.congestion_detector.probe_regulator.max_interval: Go default '2m0s'
    let base = cc(&[], &Env::default());
    let got = cc(
        &[
            "--rtc.congestion_control.send_side_bwe.congestion_detector.probe_regulator.max_interval=65000000449ns",
        ],
        &Env::default(),
    );
    assert_eq!(
        65000000449_i64,
        got.send_side_bwe
            .congestion_detector
            .probe_regulator
            .max_interval
            .as_nanos_i64(),
        "C98A-65 --rtc.congestion_control.send_side_bwe.congestion_detector.probe_regulator.max_interval=65000000449ns must project the Duration leaf"
    );
    assert_eq!(
        base.send_side_bwe
            .congestion_detector
            .probe_regulator
            .backoff_factor,
        got.send_side_bwe
            .congestion_detector
            .probe_regulator
            .backoff_factor,
        "C98A-65 --rtc.congestion_control.send_side_bwe.congestion_detector.probe_regulator.max_interval must not disturb rtc.congestion_control.send_side_bwe.congestion_detector.probe_regulator.backoff_factor"
    );
}

#[test]
fn cc_flag_send_side_bwe_congestion_detector_probe_regulator_min_duration() {
    // --rtc.congestion_control.send_side_bwe.congestion_detector.probe_regulator.min_duration: Go default '200ms'
    let base = cc(&[], &Env::default());
    let got = cc(
        &[
            "--rtc.congestion_control.send_side_bwe.congestion_detector.probe_regulator.min_duration=66000000456ns",
        ],
        &Env::default(),
    );
    assert_eq!(
        66000000456_i64,
        got.send_side_bwe
            .congestion_detector
            .probe_regulator
            .min_duration
            .as_nanos_i64(),
        "C98A-66 --rtc.congestion_control.send_side_bwe.congestion_detector.probe_regulator.min_duration=66000000456ns must project the Duration leaf"
    );
    assert_eq!(
        base.send_side_bwe
            .congestion_detector
            .probe_regulator
            .backoff_factor,
        got.send_side_bwe
            .congestion_detector
            .probe_regulator
            .backoff_factor,
        "C98A-66 --rtc.congestion_control.send_side_bwe.congestion_detector.probe_regulator.min_duration must not disturb rtc.congestion_control.send_side_bwe.congestion_detector.probe_regulator.backoff_factor"
    );
}

#[test]
fn cc_flag_send_side_bwe_congestion_detector_probe_signal_dqr_max_delay() {
    // --rtc.congestion_control.send_side_bwe.congestion_detector.probe_signal.dqr_max_delay: Go default '20ms'
    let base = cc(&[], &Env::default());
    let got = cc(
        &[
            "--rtc.congestion_control.send_side_bwe.congestion_detector.probe_signal.dqr_max_delay=67000000463ns",
        ],
        &Env::default(),
    );
    assert_eq!(
        67000000463_i64,
        got.send_side_bwe
            .congestion_detector
            .probe_signal
            .dqr_max_delay
            .as_nanos_i64(),
        "C98A-67 --rtc.congestion_control.send_side_bwe.congestion_detector.probe_signal.dqr_max_delay=67000000463ns must project the Duration leaf"
    );
    assert_eq!(
        base.send_side_bwe
            .congestion_detector
            .probe_signal
            .dqr_max_weighted_loss,
        got.send_side_bwe
            .congestion_detector
            .probe_signal
            .dqr_max_weighted_loss,
        "C98A-67 --rtc.congestion_control.send_side_bwe.congestion_detector.probe_signal.dqr_max_delay must not disturb rtc.congestion_control.send_side_bwe.congestion_detector.probe_signal.dqr_max_weighted_loss"
    );
}

#[test]
fn cc_flag_send_side_bwe_congestion_detector_probe_signal_dqr_max_weighted_loss() {
    // --rtc.congestion_control.send_side_bwe.congestion_detector.probe_signal.dqr_max_weighted_loss: Go default '0.1'
    let base = cc(&[], &Env::default());
    let got = cc(
        &[
            "--rtc.congestion_control.send_side_bwe.congestion_detector.probe_signal.dqr_max_weighted_loss=1.046875",
        ],
        &Env::default(),
    );
    assert_eq!(
        1.046875_f64,
        got.send_side_bwe
            .congestion_detector
            .probe_signal
            .dqr_max_weighted_loss,
        "C98A-68 --rtc.congestion_control.send_side_bwe.congestion_detector.probe_signal.dqr_max_weighted_loss=1.046875 must project the Float leaf"
    );
    assert_eq!(
        base.send_side_bwe
            .congestion_detector
            .probe_signal
            .dqr_max_delay
            .as_nanos_i64(),
        got.send_side_bwe
            .congestion_detector
            .probe_signal
            .dqr_max_delay
            .as_nanos_i64(),
        "C98A-68 --rtc.congestion_control.send_side_bwe.congestion_detector.probe_signal.dqr_max_weighted_loss must not disturb rtc.congestion_control.send_side_bwe.congestion_detector.probe_signal.dqr_max_delay"
    );
}

#[test]
fn cc_flag_send_side_bwe_congestion_detector_probe_signal_jqr_min_delay() {
    // --rtc.congestion_control.send_side_bwe.congestion_detector.probe_signal.jqr_min_delay: Go default '50ms'
    let base = cc(&[], &Env::default());
    let got = cc(
        &[
            "--rtc.congestion_control.send_side_bwe.congestion_detector.probe_signal.jqr_min_delay=69000000477ns",
        ],
        &Env::default(),
    );
    assert_eq!(
        69000000477_i64,
        got.send_side_bwe
            .congestion_detector
            .probe_signal
            .jqr_min_delay
            .as_nanos_i64(),
        "C98A-69 --rtc.congestion_control.send_side_bwe.congestion_detector.probe_signal.jqr_min_delay=69000000477ns must project the Duration leaf"
    );
    assert_eq!(
        base.send_side_bwe
            .congestion_detector
            .probe_signal
            .dqr_max_delay
            .as_nanos_i64(),
        got.send_side_bwe
            .congestion_detector
            .probe_signal
            .dqr_max_delay
            .as_nanos_i64(),
        "C98A-69 --rtc.congestion_control.send_side_bwe.congestion_detector.probe_signal.jqr_min_delay must not disturb rtc.congestion_control.send_side_bwe.congestion_detector.probe_signal.dqr_max_delay"
    );
}

#[test]
fn cc_flag_send_side_bwe_congestion_detector_probe_signal_jqr_min_weighted_loss() {
    // --rtc.congestion_control.send_side_bwe.congestion_detector.probe_signal.jqr_min_weighted_loss: Go default '0.25'
    let base = cc(&[], &Env::default());
    let got = cc(
        &[
            "--rtc.congestion_control.send_side_bwe.congestion_detector.probe_signal.jqr_min_weighted_loss=1.078125",
        ],
        &Env::default(),
    );
    assert_eq!(
        1.078125_f64,
        got.send_side_bwe
            .congestion_detector
            .probe_signal
            .jqr_min_weighted_loss,
        "C98A-70 --rtc.congestion_control.send_side_bwe.congestion_detector.probe_signal.jqr_min_weighted_loss=1.078125 must project the Float leaf"
    );
    assert_eq!(
        base.send_side_bwe
            .congestion_detector
            .probe_signal
            .dqr_max_delay
            .as_nanos_i64(),
        got.send_side_bwe
            .congestion_detector
            .probe_signal
            .dqr_max_delay
            .as_nanos_i64(),
        "C98A-70 --rtc.congestion_control.send_side_bwe.congestion_detector.probe_signal.jqr_min_weighted_loss must not disturb rtc.congestion_control.send_side_bwe.congestion_detector.probe_signal.dqr_max_delay"
    );
}

#[test]
fn cc_flag_send_side_bwe_congestion_detector_probe_signal_min_bytes_ratio() {
    // --rtc.congestion_control.send_side_bwe.congestion_detector.probe_signal.min_bytes_ratio: Go default '0.5'
    let base = cc(&[], &Env::default());
    let got = cc(
        &[
            "--rtc.congestion_control.send_side_bwe.congestion_detector.probe_signal.min_bytes_ratio=1.09375",
        ],
        &Env::default(),
    );
    assert_eq!(
        1.09375_f64,
        got.send_side_bwe
            .congestion_detector
            .probe_signal
            .min_bytes_ratio,
        "C98A-71 --rtc.congestion_control.send_side_bwe.congestion_detector.probe_signal.min_bytes_ratio=1.09375 must project the Float leaf"
    );
    assert_eq!(
        base.send_side_bwe
            .congestion_detector
            .probe_signal
            .dqr_max_delay
            .as_nanos_i64(),
        got.send_side_bwe
            .congestion_detector
            .probe_signal
            .dqr_max_delay
            .as_nanos_i64(),
        "C98A-71 --rtc.congestion_control.send_side_bwe.congestion_detector.probe_signal.min_bytes_ratio must not disturb rtc.congestion_control.send_side_bwe.congestion_detector.probe_signal.dqr_max_delay"
    );
}

#[test]
fn cc_flag_send_side_bwe_congestion_detector_probe_signal_min_duration_ratio() {
    // --rtc.congestion_control.send_side_bwe.congestion_detector.probe_signal.min_duration_ratio: Go default '0.5'
    let base = cc(&[], &Env::default());
    let got = cc(
        &[
            "--rtc.congestion_control.send_side_bwe.congestion_detector.probe_signal.min_duration_ratio=1.109375",
        ],
        &Env::default(),
    );
    assert_eq!(
        1.109375_f64,
        got.send_side_bwe
            .congestion_detector
            .probe_signal
            .min_duration_ratio,
        "C98A-72 --rtc.congestion_control.send_side_bwe.congestion_detector.probe_signal.min_duration_ratio=1.109375 must project the Float leaf"
    );
    assert_eq!(
        base.send_side_bwe
            .congestion_detector
            .probe_signal
            .dqr_max_delay
            .as_nanos_i64(),
        got.send_side_bwe
            .congestion_detector
            .probe_signal
            .dqr_max_delay
            .as_nanos_i64(),
        "C98A-72 --rtc.congestion_control.send_side_bwe.congestion_detector.probe_signal.min_duration_ratio must not disturb rtc.congestion_control.send_side_bwe.congestion_detector.probe_signal.dqr_max_delay"
    );
}

#[test]
fn cc_flag_send_side_bwe_congestion_detector_probe_signal_weighted_loss_base_duration() {
    // --rtc.congestion_control.send_side_bwe.congestion_detector.probe_signal.weighted_loss.base_duration: Go default '500ms'
    let base = cc(&[], &Env::default());
    let got = cc(
        &[
            "--rtc.congestion_control.send_side_bwe.congestion_detector.probe_signal.weighted_loss.base_duration=73000000505ns",
        ],
        &Env::default(),
    );
    assert_eq!(
        73000000505_i64,
        got.send_side_bwe
            .congestion_detector
            .probe_signal
            .weighted_loss
            .base_duration
            .as_nanos_i64(),
        "C98A-73 --rtc.congestion_control.send_side_bwe.congestion_detector.probe_signal.weighted_loss.base_duration=73000000505ns must project the Duration leaf"
    );
    assert_eq!(
        base.send_side_bwe
            .congestion_detector
            .probe_signal
            .weighted_loss
            .base_pps,
        got.send_side_bwe
            .congestion_detector
            .probe_signal
            .weighted_loss
            .base_pps,
        "C98A-73 --rtc.congestion_control.send_side_bwe.congestion_detector.probe_signal.weighted_loss.base_duration must not disturb rtc.congestion_control.send_side_bwe.congestion_detector.probe_signal.weighted_loss.base_pps"
    );
}

#[test]
fn cc_flag_send_side_bwe_congestion_detector_probe_signal_weighted_loss_base_pps() {
    // --rtc.congestion_control.send_side_bwe.congestion_detector.probe_signal.weighted_loss.base_pps: Go default '30'
    let base = cc(&[], &Env::default());
    let got = cc(
        &[
            "--rtc.congestion_control.send_side_bwe.congestion_detector.probe_signal.weighted_loss.base_pps=100073",
        ],
        &Env::default(),
    );
    assert_eq!(
        100073_i64,
        got.send_side_bwe
            .congestion_detector
            .probe_signal
            .weighted_loss
            .base_pps,
        "C98A-74 --rtc.congestion_control.send_side_bwe.congestion_detector.probe_signal.weighted_loss.base_pps=100073 must project the Int leaf"
    );
    assert_eq!(
        base.send_side_bwe
            .congestion_detector
            .probe_signal
            .weighted_loss
            .base_duration
            .as_nanos_i64(),
        got.send_side_bwe
            .congestion_detector
            .probe_signal
            .weighted_loss
            .base_duration
            .as_nanos_i64(),
        "C98A-74 --rtc.congestion_control.send_side_bwe.congestion_detector.probe_signal.weighted_loss.base_pps must not disturb rtc.congestion_control.send_side_bwe.congestion_detector.probe_signal.weighted_loss.base_duration"
    );
}

#[test]
fn cc_flag_send_side_bwe_congestion_detector_probe_signal_weighted_loss_loss_penalty_factor() {
    // --rtc.congestion_control.send_side_bwe.congestion_detector.probe_signal.weighted_loss.loss_penalty_factor: Go default '0.25'
    let base = cc(&[], &Env::default());
    let got = cc(
        &[
            "--rtc.congestion_control.send_side_bwe.congestion_detector.probe_signal.weighted_loss.loss_penalty_factor=1.15625",
        ],
        &Env::default(),
    );
    assert_eq!(
        1.15625_f64,
        got.send_side_bwe
            .congestion_detector
            .probe_signal
            .weighted_loss
            .loss_penalty_factor,
        "C98A-75 --rtc.congestion_control.send_side_bwe.congestion_detector.probe_signal.weighted_loss.loss_penalty_factor=1.15625 must project the Float leaf"
    );
    assert_eq!(
        base.send_side_bwe
            .congestion_detector
            .probe_signal
            .weighted_loss
            .base_duration
            .as_nanos_i64(),
        got.send_side_bwe
            .congestion_detector
            .probe_signal
            .weighted_loss
            .base_duration
            .as_nanos_i64(),
        "C98A-75 --rtc.congestion_control.send_side_bwe.congestion_detector.probe_signal.weighted_loss.loss_penalty_factor must not disturb rtc.congestion_control.send_side_bwe.congestion_detector.probe_signal.weighted_loss.base_duration"
    );
}

#[test]
fn cc_flag_send_side_bwe_congestion_detector_probe_signal_weighted_loss_min_duration_for_loss_validity()
 {
    // --rtc.congestion_control.send_side_bwe.congestion_detector.probe_signal.weighted_loss.min_duration_for_loss_validity: Go default '100ms'
    let base = cc(&[], &Env::default());
    let got = cc(
        &[
            "--rtc.congestion_control.send_side_bwe.congestion_detector.probe_signal.weighted_loss.min_duration_for_loss_validity=76000000526ns",
        ],
        &Env::default(),
    );
    assert_eq!(
        76000000526_i64,
        got.send_side_bwe
            .congestion_detector
            .probe_signal
            .weighted_loss
            .min_duration_for_loss_validity
            .as_nanos_i64(),
        "C98A-76 --rtc.congestion_control.send_side_bwe.congestion_detector.probe_signal.weighted_loss.min_duration_for_loss_validity=76000000526ns must project the Duration leaf"
    );
    assert_eq!(
        base.send_side_bwe
            .congestion_detector
            .probe_signal
            .weighted_loss
            .base_duration
            .as_nanos_i64(),
        got.send_side_bwe
            .congestion_detector
            .probe_signal
            .weighted_loss
            .base_duration
            .as_nanos_i64(),
        "C98A-76 --rtc.congestion_control.send_side_bwe.congestion_detector.probe_signal.weighted_loss.min_duration_for_loss_validity must not disturb rtc.congestion_control.send_side_bwe.congestion_detector.probe_signal.weighted_loss.base_duration"
    );
}

#[test]
fn cc_flag_send_side_bwe_congestion_detector_queuing_delay_congested_dqr_min_duration() {
    // --rtc.congestion_control.send_side_bwe.congestion_detector.queuing_delay_congested_dqr.min_duration: Go default '500ms'
    let base = cc(&[], &Env::default());
    let got = cc(
        &[
            "--rtc.congestion_control.send_side_bwe.congestion_detector.queuing_delay_congested_dqr.min_duration=77000000533ns",
        ],
        &Env::default(),
    );
    assert_eq!(
        77000000533_i64,
        got.send_side_bwe
            .congestion_detector
            .queuing_delay_congested_dqr
            .min_duration
            .as_nanos_i64(),
        "C98A-77 --rtc.congestion_control.send_side_bwe.congestion_detector.queuing_delay_congested_dqr.min_duration=77000000533ns must project the Duration leaf"
    );
    assert_eq!(
        base.send_side_bwe
            .congestion_detector
            .queuing_delay_congested_dqr
            .min_number_of_groups,
        got.send_side_bwe
            .congestion_detector
            .queuing_delay_congested_dqr
            .min_number_of_groups,
        "C98A-77 --rtc.congestion_control.send_side_bwe.congestion_detector.queuing_delay_congested_dqr.min_duration must not disturb rtc.congestion_control.send_side_bwe.congestion_detector.queuing_delay_congested_dqr.min_number_of_groups"
    );
}

#[test]
fn cc_flag_send_side_bwe_congestion_detector_queuing_delay_congested_dqr_min_number_of_groups() {
    // --rtc.congestion_control.send_side_bwe.congestion_detector.queuing_delay_congested_dqr.min_number_of_groups: Go default '5'
    let base = cc(&[], &Env::default());
    let got = cc(
        &[
            "--rtc.congestion_control.send_side_bwe.congestion_detector.queuing_delay_congested_dqr.min_number_of_groups=100077",
        ],
        &Env::default(),
    );
    assert_eq!(
        100077_i64,
        got.send_side_bwe
            .congestion_detector
            .queuing_delay_congested_dqr
            .min_number_of_groups,
        "C98A-78 --rtc.congestion_control.send_side_bwe.congestion_detector.queuing_delay_congested_dqr.min_number_of_groups=100077 must project the Int leaf"
    );
    assert_eq!(
        base.send_side_bwe
            .congestion_detector
            .queuing_delay_congested_dqr
            .min_duration
            .as_nanos_i64(),
        got.send_side_bwe
            .congestion_detector
            .queuing_delay_congested_dqr
            .min_duration
            .as_nanos_i64(),
        "C98A-78 --rtc.congestion_control.send_side_bwe.congestion_detector.queuing_delay_congested_dqr.min_number_of_groups must not disturb rtc.congestion_control.send_side_bwe.congestion_detector.queuing_delay_congested_dqr.min_duration"
    );
}

#[test]
fn cc_flag_send_side_bwe_congestion_detector_queuing_delay_congested_jqr_min_duration() {
    // --rtc.congestion_control.send_side_bwe.congestion_detector.queuing_delay_congested_jqr.min_duration: Go default '400ms'
    let base = cc(&[], &Env::default());
    let got = cc(
        &[
            "--rtc.congestion_control.send_side_bwe.congestion_detector.queuing_delay_congested_jqr.min_duration=79000000547ns",
        ],
        &Env::default(),
    );
    assert_eq!(
        79000000547_i64,
        got.send_side_bwe
            .congestion_detector
            .queuing_delay_congested_jqr
            .min_duration
            .as_nanos_i64(),
        "C98A-79 --rtc.congestion_control.send_side_bwe.congestion_detector.queuing_delay_congested_jqr.min_duration=79000000547ns must project the Duration leaf"
    );
    assert_eq!(
        base.send_side_bwe
            .congestion_detector
            .queuing_delay_congested_jqr
            .min_number_of_groups,
        got.send_side_bwe
            .congestion_detector
            .queuing_delay_congested_jqr
            .min_number_of_groups,
        "C98A-79 --rtc.congestion_control.send_side_bwe.congestion_detector.queuing_delay_congested_jqr.min_duration must not disturb rtc.congestion_control.send_side_bwe.congestion_detector.queuing_delay_congested_jqr.min_number_of_groups"
    );
}

#[test]
fn cc_flag_send_side_bwe_congestion_detector_queuing_delay_congested_jqr_min_number_of_groups() {
    // --rtc.congestion_control.send_side_bwe.congestion_detector.queuing_delay_congested_jqr.min_number_of_groups: Go default '4'
    let base = cc(&[], &Env::default());
    let got = cc(
        &[
            "--rtc.congestion_control.send_side_bwe.congestion_detector.queuing_delay_congested_jqr.min_number_of_groups=100079",
        ],
        &Env::default(),
    );
    assert_eq!(
        100079_i64,
        got.send_side_bwe
            .congestion_detector
            .queuing_delay_congested_jqr
            .min_number_of_groups,
        "C98A-80 --rtc.congestion_control.send_side_bwe.congestion_detector.queuing_delay_congested_jqr.min_number_of_groups=100079 must project the Int leaf"
    );
    assert_eq!(
        base.send_side_bwe
            .congestion_detector
            .queuing_delay_congested_jqr
            .min_duration
            .as_nanos_i64(),
        got.send_side_bwe
            .congestion_detector
            .queuing_delay_congested_jqr
            .min_duration
            .as_nanos_i64(),
        "C98A-80 --rtc.congestion_control.send_side_bwe.congestion_detector.queuing_delay_congested_jqr.min_number_of_groups must not disturb rtc.congestion_control.send_side_bwe.congestion_detector.queuing_delay_congested_jqr.min_duration"
    );
}

#[test]
fn cc_flag_send_side_bwe_congestion_detector_queuing_delay_early_warning_dqr_min_duration() {
    // --rtc.congestion_control.send_side_bwe.congestion_detector.queuing_delay_early_warning_dqr.min_duration: Go default '300ms'
    let base = cc(&[], &Env::default());
    let got = cc(
        &[
            "--rtc.congestion_control.send_side_bwe.congestion_detector.queuing_delay_early_warning_dqr.min_duration=81000000561ns",
        ],
        &Env::default(),
    );
    assert_eq!(
        81000000561_i64,
        got.send_side_bwe
            .congestion_detector
            .queuing_delay_early_warning_dqr
            .min_duration
            .as_nanos_i64(),
        "C98A-81 --rtc.congestion_control.send_side_bwe.congestion_detector.queuing_delay_early_warning_dqr.min_duration=81000000561ns must project the Duration leaf"
    );
    assert_eq!(
        base.send_side_bwe
            .congestion_detector
            .queuing_delay_early_warning_dqr
            .min_number_of_groups,
        got.send_side_bwe
            .congestion_detector
            .queuing_delay_early_warning_dqr
            .min_number_of_groups,
        "C98A-81 --rtc.congestion_control.send_side_bwe.congestion_detector.queuing_delay_early_warning_dqr.min_duration must not disturb rtc.congestion_control.send_side_bwe.congestion_detector.queuing_delay_early_warning_dqr.min_number_of_groups"
    );
}

#[test]
fn cc_flag_send_side_bwe_congestion_detector_queuing_delay_early_warning_dqr_min_number_of_groups()
{
    // --rtc.congestion_control.send_side_bwe.congestion_detector.queuing_delay_early_warning_dqr.min_number_of_groups: Go default '3'
    let base = cc(&[], &Env::default());
    let got = cc(
        &[
            "--rtc.congestion_control.send_side_bwe.congestion_detector.queuing_delay_early_warning_dqr.min_number_of_groups=100081",
        ],
        &Env::default(),
    );
    assert_eq!(
        100081_i64,
        got.send_side_bwe
            .congestion_detector
            .queuing_delay_early_warning_dqr
            .min_number_of_groups,
        "C98A-82 --rtc.congestion_control.send_side_bwe.congestion_detector.queuing_delay_early_warning_dqr.min_number_of_groups=100081 must project the Int leaf"
    );
    assert_eq!(
        base.send_side_bwe
            .congestion_detector
            .queuing_delay_early_warning_dqr
            .min_duration
            .as_nanos_i64(),
        got.send_side_bwe
            .congestion_detector
            .queuing_delay_early_warning_dqr
            .min_duration
            .as_nanos_i64(),
        "C98A-82 --rtc.congestion_control.send_side_bwe.congestion_detector.queuing_delay_early_warning_dqr.min_number_of_groups must not disturb rtc.congestion_control.send_side_bwe.congestion_detector.queuing_delay_early_warning_dqr.min_duration"
    );
}

#[test]
fn cc_flag_send_side_bwe_congestion_detector_queuing_delay_early_warning_jqr_min_duration() {
    // --rtc.congestion_control.send_side_bwe.congestion_detector.queuing_delay_early_warning_jqr.min_duration: Go default '200ms'
    let base = cc(&[], &Env::default());
    let got = cc(
        &[
            "--rtc.congestion_control.send_side_bwe.congestion_detector.queuing_delay_early_warning_jqr.min_duration=83000000575ns",
        ],
        &Env::default(),
    );
    assert_eq!(
        83000000575_i64,
        got.send_side_bwe
            .congestion_detector
            .queuing_delay_early_warning_jqr
            .min_duration
            .as_nanos_i64(),
        "C98A-83 --rtc.congestion_control.send_side_bwe.congestion_detector.queuing_delay_early_warning_jqr.min_duration=83000000575ns must project the Duration leaf"
    );
    assert_eq!(
        base.send_side_bwe
            .congestion_detector
            .queuing_delay_early_warning_jqr
            .min_number_of_groups,
        got.send_side_bwe
            .congestion_detector
            .queuing_delay_early_warning_jqr
            .min_number_of_groups,
        "C98A-83 --rtc.congestion_control.send_side_bwe.congestion_detector.queuing_delay_early_warning_jqr.min_duration must not disturb rtc.congestion_control.send_side_bwe.congestion_detector.queuing_delay_early_warning_jqr.min_number_of_groups"
    );
}

#[test]
fn cc_flag_send_side_bwe_congestion_detector_queuing_delay_early_warning_jqr_min_number_of_groups()
{
    // --rtc.congestion_control.send_side_bwe.congestion_detector.queuing_delay_early_warning_jqr.min_number_of_groups: Go default '2'
    let base = cc(&[], &Env::default());
    let got = cc(
        &[
            "--rtc.congestion_control.send_side_bwe.congestion_detector.queuing_delay_early_warning_jqr.min_number_of_groups=100083",
        ],
        &Env::default(),
    );
    assert_eq!(
        100083_i64,
        got.send_side_bwe
            .congestion_detector
            .queuing_delay_early_warning_jqr
            .min_number_of_groups,
        "C98A-84 --rtc.congestion_control.send_side_bwe.congestion_detector.queuing_delay_early_warning_jqr.min_number_of_groups=100083 must project the Int leaf"
    );
    assert_eq!(
        base.send_side_bwe
            .congestion_detector
            .queuing_delay_early_warning_jqr
            .min_duration
            .as_nanos_i64(),
        got.send_side_bwe
            .congestion_detector
            .queuing_delay_early_warning_jqr
            .min_duration
            .as_nanos_i64(),
        "C98A-84 --rtc.congestion_control.send_side_bwe.congestion_detector.queuing_delay_early_warning_jqr.min_number_of_groups must not disturb rtc.congestion_control.send_side_bwe.congestion_detector.queuing_delay_early_warning_jqr.min_duration"
    );
}

#[test]
fn cc_flag_send_side_bwe_congestion_detector_weighted_loss_base_duration() {
    // --rtc.congestion_control.send_side_bwe.congestion_detector.weighted_loss.base_duration: Go default '500ms'
    let base = cc(&[], &Env::default());
    let got = cc(
        &[
            "--rtc.congestion_control.send_side_bwe.congestion_detector.weighted_loss.base_duration=85000000589ns",
        ],
        &Env::default(),
    );
    assert_eq!(
        85000000589_i64,
        got.send_side_bwe
            .congestion_detector
            .weighted_loss
            .base_duration
            .as_nanos_i64(),
        "C98A-85 --rtc.congestion_control.send_side_bwe.congestion_detector.weighted_loss.base_duration=85000000589ns must project the Duration leaf"
    );
    assert_eq!(
        base.send_side_bwe
            .congestion_detector
            .weighted_loss
            .base_pps,
        got.send_side_bwe.congestion_detector.weighted_loss.base_pps,
        "C98A-85 --rtc.congestion_control.send_side_bwe.congestion_detector.weighted_loss.base_duration must not disturb rtc.congestion_control.send_side_bwe.congestion_detector.weighted_loss.base_pps"
    );
}

#[test]
fn cc_flag_send_side_bwe_congestion_detector_weighted_loss_base_pps() {
    // --rtc.congestion_control.send_side_bwe.congestion_detector.weighted_loss.base_pps: Go default '30'
    let base = cc(&[], &Env::default());
    let got = cc(
        &[
            "--rtc.congestion_control.send_side_bwe.congestion_detector.weighted_loss.base_pps=100085",
        ],
        &Env::default(),
    );
    assert_eq!(
        100085_i64, got.send_side_bwe.congestion_detector.weighted_loss.base_pps,
        "C98A-86 --rtc.congestion_control.send_side_bwe.congestion_detector.weighted_loss.base_pps=100085 must project the Int leaf"
    );
    assert_eq!(
        base.send_side_bwe
            .congestion_detector
            .weighted_loss
            .base_duration
            .as_nanos_i64(),
        got.send_side_bwe
            .congestion_detector
            .weighted_loss
            .base_duration
            .as_nanos_i64(),
        "C98A-86 --rtc.congestion_control.send_side_bwe.congestion_detector.weighted_loss.base_pps must not disturb rtc.congestion_control.send_side_bwe.congestion_detector.weighted_loss.base_duration"
    );
}

#[test]
fn cc_flag_send_side_bwe_congestion_detector_weighted_loss_loss_penalty_factor() {
    // --rtc.congestion_control.send_side_bwe.congestion_detector.weighted_loss.loss_penalty_factor: Go default '0.25'
    let base = cc(&[], &Env::default());
    let got = cc(
        &[
            "--rtc.congestion_control.send_side_bwe.congestion_detector.weighted_loss.loss_penalty_factor=1.34375",
        ],
        &Env::default(),
    );
    assert_eq!(
        1.34375_f64,
        got.send_side_bwe
            .congestion_detector
            .weighted_loss
            .loss_penalty_factor,
        "C98A-87 --rtc.congestion_control.send_side_bwe.congestion_detector.weighted_loss.loss_penalty_factor=1.34375 must project the Float leaf"
    );
    assert_eq!(
        base.send_side_bwe
            .congestion_detector
            .weighted_loss
            .base_duration
            .as_nanos_i64(),
        got.send_side_bwe
            .congestion_detector
            .weighted_loss
            .base_duration
            .as_nanos_i64(),
        "C98A-87 --rtc.congestion_control.send_side_bwe.congestion_detector.weighted_loss.loss_penalty_factor must not disturb rtc.congestion_control.send_side_bwe.congestion_detector.weighted_loss.base_duration"
    );
}

#[test]
fn cc_flag_send_side_bwe_congestion_detector_weighted_loss_min_duration_for_loss_validity() {
    // --rtc.congestion_control.send_side_bwe.congestion_detector.weighted_loss.min_duration_for_loss_validity: Go default '100ms'
    let base = cc(&[], &Env::default());
    let got = cc(
        &[
            "--rtc.congestion_control.send_side_bwe.congestion_detector.weighted_loss.min_duration_for_loss_validity=88000000610ns",
        ],
        &Env::default(),
    );
    assert_eq!(
        88000000610_i64,
        got.send_side_bwe
            .congestion_detector
            .weighted_loss
            .min_duration_for_loss_validity
            .as_nanos_i64(),
        "C98A-88 --rtc.congestion_control.send_side_bwe.congestion_detector.weighted_loss.min_duration_for_loss_validity=88000000610ns must project the Duration leaf"
    );
    assert_eq!(
        base.send_side_bwe
            .congestion_detector
            .weighted_loss
            .base_duration
            .as_nanos_i64(),
        got.send_side_bwe
            .congestion_detector
            .weighted_loss
            .base_duration
            .as_nanos_i64(),
        "C98A-88 --rtc.congestion_control.send_side_bwe.congestion_detector.weighted_loss.min_duration_for_loss_validity must not disturb rtc.congestion_control.send_side_bwe.congestion_detector.weighted_loss.base_duration"
    );
}

#[test]
fn cc_flag_stream_allocator_disable_etimation_unmanaged_tracks() {
    // --rtc.congestion_control.stream_allocator.disable_etimation_unmanaged_tracks: Go default 'false'
    let base = cc(&[], &Env::default());
    let got = cc(
        &["--rtc.congestion_control.stream_allocator.disable_etimation_unmanaged_tracks=true"],
        &Env::default(),
    );
    assert!(
        got.stream_allocator.disable_estimation_unmanaged_tracks,
        "C98A-89 --rtc.congestion_control.stream_allocator.disable_etimation_unmanaged_tracks=true must project the Bool leaf"
    );
    assert_eq!(
        base.stream_allocator.min_channel_capacity, got.stream_allocator.min_channel_capacity,
        "C98A-89 --rtc.congestion_control.stream_allocator.disable_etimation_unmanaged_tracks must not disturb rtc.congestion_control.stream_allocator.min_channel_capacity"
    );
}

#[test]
fn cc_flag_stream_allocator_min_channel_capacity() {
    // --rtc.congestion_control.stream_allocator.min_channel_capacity: Go default '0'
    let base = cc(&[], &Env::default());
    let got = cc(
        &["--rtc.congestion_control.stream_allocator.min_channel_capacity=100089"],
        &Env::default(),
    );
    assert_eq!(
        100089_i64, got.stream_allocator.min_channel_capacity,
        "C98A-90 --rtc.congestion_control.stream_allocator.min_channel_capacity=100089 must project the Int leaf"
    );
    assert_eq!(
        base.stream_allocator.disable_estimation_unmanaged_tracks,
        got.stream_allocator.disable_estimation_unmanaged_tracks,
        "C98A-90 --rtc.congestion_control.stream_allocator.min_channel_capacity must not disturb rtc.congestion_control.stream_allocator.disable_etimation_unmanaged_tracks"
    );
}

#[test]
fn cc_flag_stream_allocator_paused_min_wait() {
    // --rtc.congestion_control.stream_allocator.paused_min_wait: Go default '5s'
    let base = cc(&[], &Env::default());
    let got = cc(
        &["--rtc.congestion_control.stream_allocator.paused_min_wait=91000000631ns"],
        &Env::default(),
    );
    assert_eq!(
        91000000631_i64,
        got.stream_allocator.paused_min_wait.as_nanos_i64(),
        "C98A-91 --rtc.congestion_control.stream_allocator.paused_min_wait=91000000631ns must project the Duration leaf"
    );
    assert_eq!(
        base.stream_allocator.disable_estimation_unmanaged_tracks,
        got.stream_allocator.disable_estimation_unmanaged_tracks,
        "C98A-91 --rtc.congestion_control.stream_allocator.paused_min_wait must not disturb rtc.congestion_control.stream_allocator.disable_etimation_unmanaged_tracks"
    );
}

#[test]
fn cc_flag_stream_allocator_probe_min_bps() {
    // --rtc.congestion_control.stream_allocator.probe_min_bps: Go default '200000'
    let base = cc(&[], &Env::default());
    let got = cc(
        &["--rtc.congestion_control.stream_allocator.probe_min_bps=100091"],
        &Env::default(),
    );
    assert_eq!(
        100091_i64, got.stream_allocator.probe_min_bps,
        "C98A-92 --rtc.congestion_control.stream_allocator.probe_min_bps=100091 must project the Int leaf"
    );
    assert_eq!(
        base.stream_allocator.disable_estimation_unmanaged_tracks,
        got.stream_allocator.disable_estimation_unmanaged_tracks,
        "C98A-92 --rtc.congestion_control.stream_allocator.probe_min_bps must not disturb rtc.congestion_control.stream_allocator.disable_etimation_unmanaged_tracks"
    );
}

#[test]
fn cc_flag_stream_allocator_probe_mode() {
    // --rtc.congestion_control.stream_allocator.probe_mode: Go default 'padding'
    let base = cc(&[], &Env::default());
    let got = cc(
        &["--rtc.congestion_control.stream_allocator.probe_mode=media"],
        &Env::default(),
    );
    assert_eq!(
        "media",
        got.stream_allocator.probe_mode.as_str(),
        "C98A-93 --rtc.congestion_control.stream_allocator.probe_mode=media must project the Str leaf"
    );
    assert_eq!(
        base.stream_allocator.disable_estimation_unmanaged_tracks,
        got.stream_allocator.disable_estimation_unmanaged_tracks,
        "C98A-93 --rtc.congestion_control.stream_allocator.probe_mode must not disturb rtc.congestion_control.stream_allocator.disable_etimation_unmanaged_tracks"
    );
}

#[test]
fn cc_flag_stream_allocator_probe_overage_pct() {
    // --rtc.congestion_control.stream_allocator.probe_overage_pct: Go default '120'
    let base = cc(&[], &Env::default());
    let got = cc(
        &["--rtc.congestion_control.stream_allocator.probe_overage_pct=100093"],
        &Env::default(),
    );
    assert_eq!(
        100093_i64, got.stream_allocator.probe_overage_pct,
        "C98A-94 --rtc.congestion_control.stream_allocator.probe_overage_pct=100093 must project the Int leaf"
    );
    assert_eq!(
        base.stream_allocator.disable_estimation_unmanaged_tracks,
        got.stream_allocator.disable_estimation_unmanaged_tracks,
        "C98A-94 --rtc.congestion_control.stream_allocator.probe_overage_pct must not disturb rtc.congestion_control.stream_allocator.disable_etimation_unmanaged_tracks"
    );
}
