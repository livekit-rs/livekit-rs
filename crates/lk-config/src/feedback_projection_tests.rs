//! Real public-loader tests for the feedback policy projection. These do not
//! claim the media engine has implemented the selected strategy.

use std::ffi::OsString;
use std::time::Duration;

use rtc_core::config::{CongestionControl, FeedbackMode, PolicyDuration, SendSidePacer};

use crate::netdetect::tests::FakeProbe;
use crate::{EntryAction, Env, LoadedConfig, ProgramInfo, parse_with_probe};

fn loaded(name: &str, flags: &[&str], env: &Env) -> LoadedConfig {
    let argv = std::iter::once(name)
        .chain(std::iter::once("--dev"))
        .chain(flags.iter().copied())
        .map(OsString::from)
        .collect();
    let info = ProgramInfo {
        binary_name: name.into(),
        product_version: "0.1.0".into(),
        compat_version: "1.13.10",
    };
    match parse_with_probe(argv, env, &info, &FakeProbe::local("10.0.0.5")).unwrap() {
        EntryAction::RunServer(config) => *config,
        other => panic!("expected full server configuration, got {other:?}"),
    }
}

#[test]
fn defaults_preserve_fixed_observer_context_and_separate_allocation_toggle() {
    let config = loaded("livekit-server", &[], &Env::default());
    let control = &config.server.rtc.congestion_control;
    assert_eq!(control, &CongestionControl::default());
    assert!(control.enabled);
    assert_eq!(
        control.effective_feedback_mode(),
        FeedbackMode::ReceiverEstimate
    );
    assert_eq!(control.send_side_bwe_pacer, SendSidePacer::NoQueue);
    let remote = &control.remote_bwe;
    assert_eq!(remote.nack_ratio_attenuator, 0.4);
    assert_eq!(remote.expected_usage_threshold, 0.95);
    assert_eq!(remote.channel_observer_probe.estimate.required_samples, 3);
    assert_eq!(
        remote.channel_observer_non_probe.estimate.required_samples,
        12
    );
    assert_eq!(
        remote
            .channel_observer_non_probe
            .estimate
            .required_samples_min,
        8
    );
    assert_eq!(
        remote
            .channel_observer_non_probe
            .estimate
            .downward_trend_threshold,
        -0.6
    );
    assert_eq!(remote.channel_observer_probe.nack.ratio_threshold, 0.04);
    assert_eq!(remote.channel_observer_non_probe.nack.ratio_threshold, 0.08);
    assert_eq!(
        remote.channel_observer_probe.nack.window_min_duration,
        PolicyDuration::from_millis(500)
    );
    assert_eq!(
        remote.channel_observer_non_probe.nack.window_min_duration,
        PolicyDuration::from_secs(2)
    );
    assert_eq!(
        remote.probe_controller.settle_wait_min,
        PolicyDuration::from_millis(250)
    );
    assert_eq!(
        remote.probe_controller.settle_wait_max,
        PolicyDuration::from_secs(5)
    );
    assert_eq!(
        remote.probe_controller.probe_regulator.max_interval,
        PolicyDuration::from_secs(120)
    );
}

#[test]
fn both_binary_names_keep_both_flags_and_native_precedence_when_disabled() {
    for name in ["livekit-server", "livecloud"] {
        for enabled in [true, false] {
            for (native, interceptor, mode) in [
                (false, false, FeedbackMode::ReceiverEstimate),
                (false, true, FeedbackMode::InterceptorEstimate),
                (true, false, FeedbackMode::NativeSendSide),
                (true, true, FeedbackMode::NativeSendSide),
            ] {
                let yaml = format!(
                    "rtc:\n  congestion_control:\n    enabled: {enabled}\n    use_send_side_bwe: {native}\n    use_send_side_bwe_interceptor: {interceptor}\n"
                );
                let config = loaded(name, &["--config-body", &yaml], &Env::default());
                let control = &config.server.rtc.congestion_control;
                assert_eq!(control.enabled, enabled);
                assert_eq!(control.use_send_side_bwe, native);
                assert_eq!(control.use_send_side_bwe_interceptor, interceptor);
                assert_eq!(control.effective_feedback_mode(), mode);
            }
        }
    }
}

#[test]
fn actual_cli_env_yaml_precedence_projects_false_instead_of_defaulting_true() {
    let yaml = "rtc:\n  congestion_control:\n    enabled: true\n    use_send_side_bwe: false\n    use_send_side_bwe_interceptor: true\n    remote_bwe:\n      expected_usage_threshold: 0.71\n";
    let environment = Env::from_pairs([
        ("LIVEKIT_RTC_CONGESTION_CONTROL_ENABLED", "false"),
        ("LIVEKIT_RTC_CONGESTION_CONTROL_USE_SEND_SIDE_BWE", "true"),
        (
            "LIVEKIT_RTC_CONGESTION_CONTROL_USE_SEND_SIDE_BWE_INTERCEPTOR",
            "false",
        ),
        (
            "LIVEKIT_RTC_CONGESTION_CONTROL_REMOTE_BWE_EXPECTED_USAGE_THRESHOLD",
            "0.82",
        ),
    ]);
    let env_config = loaded("livecloud", &["--config-body", yaml], &environment);
    assert!(!env_config.server.rtc.congestion_control.enabled);
    assert_eq!(
        env_config
            .server
            .rtc
            .congestion_control
            .effective_feedback_mode(),
        FeedbackMode::NativeSendSide
    );
    assert_eq!(
        env_config
            .server
            .rtc
            .congestion_control
            .remote_bwe
            .expected_usage_threshold,
        0.82
    );
    let cli_config = loaded(
        "livecloud",
        &[
            "--config-body",
            yaml,
            "--rtc.congestion_control.use_send_side_bwe=false",
            "--rtc.congestion_control.use_send_side_bwe_interceptor=true",
            "--rtc.congestion_control.remote_bwe.expected_usage_threshold=0.93",
        ],
        &environment,
    );
    let control = &cli_config.server.rtc.congestion_control;
    assert!(!control.enabled);
    assert!(!control.use_send_side_bwe);
    assert!(control.use_send_side_bwe_interceptor);
    assert_eq!(
        control.effective_feedback_mode(),
        FeedbackMode::InterceptorEstimate
    );
    assert_eq!(control.remote_bwe.expected_usage_threshold, 0.93);
}

#[test]
fn nested_observers_regulator_and_signed_nanoseconds_survive_real_yaml_loading() {
    let yaml = r#"
rtc:
  congestion_control:
    stream_allocator:
      min_channel_capacity: -11
      disable_etimation_unmanaged_tracks: true
      probe_mode: media
      probe_overage_pct: 131
      probe_min_bps: 410000
      paused_min_wait: -13ns
    remote_bwe:
      nack_ratio_attenuator: 0.23
      expected_usage_threshold: 0.91
      channel_observer_probe:
        estimate:
          required_samples: -3
          required_samples_min: 0
          downward_trend_threshold: -0.73
          downward_trend_max_wait: -1ns
          collapse_threshold: 0s
          validity_window: 1250us
        nack:
          window_min_duration: -19ns
          window_max_duration: 3ns
          ratio_threshold: 0.12
      channel_observer_non_probe:
        estimate:
          required_samples: 21
        nack:
          ratio_threshold: 0.17
      probe_controller:
        settle_wait_num_rtt: 7
        settle_wait_min: -2ns
        settle_wait_max: 1111ns
        probe_regulator:
          base_interval: 2222ns
          backoff_factor: 1.75
          max_interval: 90s
          min_duration: -23ns
          max_duration: 700ms
          duration_increase_factor: 1.3
"#;
    let parsed = loaded("livecloud", &["--config-body", yaml], &Env::default());
    let control = &parsed.server.rtc.congestion_control;
    let allocator = &control.stream_allocator;
    assert_eq!(allocator.min_channel_capacity, -11);
    assert!(allocator.disable_estimation_unmanaged_tracks);
    assert_eq!(allocator.probe_mode, "media");
    assert_eq!(allocator.probe_overage_pct, 131);
    assert_eq!(allocator.probe_min_bps, 410_000);
    assert_eq!(allocator.paused_min_wait.as_nanos_i64(), -13);
    let remote = &control.remote_bwe;
    assert_eq!(remote.nack_ratio_attenuator, 0.23);
    assert_eq!(remote.expected_usage_threshold, 0.91);
    let trend = &remote.channel_observer_probe.estimate;
    assert_eq!(trend.required_samples, -3);
    assert_eq!(trend.required_samples_min, 0);
    assert_eq!(trend.downward_trend_threshold, -0.73);
    assert_eq!(trend.downward_trend_max_wait.as_nanos_i64(), -1);
    assert_eq!(trend.collapse_threshold.as_nanos_i64(), 0);
    assert_eq!(trend.validity_window.as_nanos_i64(), 1_250_000);
    assert_eq!(trend.downward_trend_max_wait.nonnegative_to_std(), None);
    assert_eq!(
        trend.collapse_threshold.nonnegative_to_std(),
        Some(Duration::ZERO)
    );
    assert_eq!(
        trend.validity_window.nonnegative_to_std(),
        Some(Duration::from_micros(1250))
    );
    assert_eq!(
        remote
            .channel_observer_probe
            .nack
            .window_min_duration
            .as_nanos_i64(),
        -19
    );
    assert_eq!(
        remote
            .channel_observer_probe
            .nack
            .window_max_duration
            .as_nanos_i64(),
        3
    );
    assert_eq!(remote.channel_observer_probe.nack.ratio_threshold, 0.12);
    assert_eq!(
        remote.channel_observer_non_probe.estimate.required_samples,
        21
    );
    assert_eq!(remote.channel_observer_non_probe.nack.ratio_threshold, 0.17);
    let probe = &remote.probe_controller;
    assert_eq!(probe.settle_wait_num_rtt, 7);
    assert_eq!(probe.settle_wait_min.as_nanos_i64(), -2);
    assert_eq!(probe.settle_wait_max.as_nanos_i64(), 1111);
    assert_eq!(probe.probe_regulator.base_interval.as_nanos_i64(), 2222);
    assert_eq!(probe.probe_regulator.backoff_factor, 1.75);
    assert_eq!(
        probe.probe_regulator.max_interval,
        PolicyDuration::from_secs(90)
    );
    assert_eq!(probe.probe_regulator.min_duration.as_nanos_i64(), -23);
    assert_eq!(
        probe.probe_regulator.max_duration,
        PolicyDuration::from_millis(700)
    );
    assert_eq!(probe.probe_regulator.duration_increase_factor, 1.3);
    // Raw signed duration remains available and exactly agrees with the neutral value.
    assert_eq!(
        parsed
            .official
            .rtc
            .congestion_control
            .remote_bwe
            .channel_observer_probe
            .estimate
            .downward_trend_max_wait
            .nanos(),
        -1
    );
}

#[test]
fn native_nested_tuning_and_real_typo_keys_are_preserved_without_activation() {
    let yaml = r#"
rtc:
  congestion_control:
    send_side_bwe:
      congestion_detector:
        packet_group:
          min_packets: 31
          max_window_duration: 501ms
        packet_group_max_age: -7ns
        probe_packet_group:
          packet_group:
            min_packets: 99
            max_window_duration: 3s
          settle_wait_num_rtt: 9
        probe_signal:
          min_bytes_ratio: 0.67
          weighted_loss:
            base_pps: 49
        weighted_loss:
          loss_penalty_factor: 0.13
        queuing_delay_congested_jqr:
          min_number_of_groups: 23
          min_duration: -4ns
        congested_ctr_trend:
          collapse_threshold: -9ns
        congested_packet_group:
          min_packets: 18
        estimaton_window_duration: -5ns
"#;
    let parsed = loaded("livekit-server", &["--config-body", yaml], &Env::default());
    let control = &parsed.server.rtc.congestion_control;
    assert_eq!(
        control.effective_feedback_mode(),
        FeedbackMode::ReceiverEstimate
    );
    let detector = &control.send_side_bwe.congestion_detector;
    assert_eq!(detector.packet_group.min_packets, 31);
    assert_eq!(
        detector.packet_group.max_window_duration,
        PolicyDuration::from_millis(501)
    );
    assert_eq!(detector.packet_group_max_age.as_nanos_i64(), -7);
    assert_eq!(detector.probe_packet_group.packet_group.min_packets, 99);
    assert_eq!(
        detector.probe_packet_group.packet_group.max_window_duration,
        PolicyDuration::from_secs(3)
    );
    assert_eq!(detector.probe_packet_group.settle_wait_num_rtt, 9);
    assert_eq!(detector.probe_signal.min_bytes_ratio, 0.67);
    assert_eq!(detector.probe_signal.weighted_loss.base_pps, 49);
    assert_eq!(detector.weighted_loss.loss_penalty_factor, 0.13);
    assert_eq!(
        detector.queuing_delay_congested_jqr.min_number_of_groups,
        23
    );
    assert_eq!(
        detector
            .queuing_delay_congested_jqr
            .min_duration
            .as_nanos_i64(),
        -4
    );
    assert_eq!(
        detector
            .congested_ctr_trend
            .collapse_threshold
            .as_nanos_i64(),
        -9
    );
    assert_eq!(detector.congested_packet_group.min_packets, 18);
    assert_eq!(detector.estimation_window_duration.as_nanos_i64(), -5);
}

#[test]
fn pacer_raw_choices_are_not_collapsed_by_config_projection() {
    for (raw, expected) in [
        ("", SendSidePacer::Unspecified),
        ("pass-through", SendSidePacer::PassThrough),
        ("no-queue", SendSidePacer::NoQueue),
        ("leaky-bucket", SendSidePacer::LeakyBucket),
    ] {
        let arg = format!("--rtc.congestion_control.send_side_bwe_pacer={raw}");
        let parsed = loaded("livecloud", &[&arg], &Env::default());
        assert_eq!(
            parsed.server.rtc.congestion_control.send_side_bwe_pacer,
            expected
        );
        assert_eq!(
            parsed.official.rtc.congestion_control.send_side_bwe_pacer,
            raw
        );
    }
}

#[test]
fn signed_duration_env_and_cli_keep_zero_distinct_from_negative_yaml() {
    let yaml = "rtc:\n  congestion_control:\n    remote_bwe:\n      channel_observer_non_probe:\n        estimate:\n          collapse_threshold: -17ns\n";
    let env = Env::from_pairs([(
        "LIVEKIT_RTC_CONGESTION_CONTROL_REMOTE_BWE_CHANNEL_OBSERVER_NON_PROBE_ESTIMATE_COLLAPSE_THRESHOLD",
        "0s",
    )]);
    let environment = loaded("livecloud", &["--config-body", yaml], &env);
    assert_eq!(
        environment
            .server
            .rtc
            .congestion_control
            .remote_bwe
            .channel_observer_non_probe
            .estimate
            .collapse_threshold
            .as_nanos_i64(),
        0
    );
    let cli = loaded(
        "livecloud",
        &[
            "--config-body",
            yaml,
            "--rtc.congestion_control.remote_bwe.channel_observer_non_probe.estimate.collapse_threshold=-19ns",
        ],
        &env,
    );
    assert_eq!(
        cli.server
            .rtc
            .congestion_control
            .remote_bwe
            .channel_observer_non_probe
            .estimate
            .collapse_threshold
            .as_nanos_i64(),
        -19
    );
    assert_eq!(
        cli.official
            .rtc
            .congestion_control
            .remote_bwe
            .channel_observer_non_probe
            .estimate
            .collapse_threshold
            .nanos(),
        -19
    );
}

#[test]
fn tcp_fallback_three_inputs_feed_actual_room_policy_with_signed_threshold_and_precedence() {
    let yaml = "rtc:\n  allow_tcp_fallback: false\n  tcp_fallback_rtt_threshold: -7\n  allow_udp_unstable_fallback: true\n";
    let default = loaded("livecloud", &[], &Env::default());
    assert!(
        rtc_core::config::RoomPolicy::from(&default.server)
            .transport_fallback
            .allow_tcp
    );
    let only_yaml = loaded("livecloud", &["--config-body", yaml], &Env::default());
    let policy = rtc_core::config::RoomPolicy::from(&only_yaml.server).transport_fallback;
    assert!(!policy.allow_tcp);
    assert_eq!(policy.rtt_threshold_ms, -7);
    assert!(policy.allow_udp_unstable);
    let environment = Env::from_pairs([
        ("LIVEKIT_RTC_ALLOW_TCP_FALLBACK", "true"),
        ("LIVEKIT_RTC_TCP_FALLBACK_RTT_THRESHOLD", "93"),
        ("LIVEKIT_RTC_ALLOW_UDP_UNSTABLE_FALLBACK", "false"),
    ]);
    let from_env = loaded("livekit-server", &["--config-body", yaml], &environment);
    let policy = rtc_core::config::RoomPolicy::from(&from_env.server).transport_fallback;
    assert!(policy.allow_tcp);
    assert_eq!(policy.rtt_threshold_ms, 93);
    assert!(!policy.allow_udp_unstable);
    let from_cli = loaded(
        "livecloud",
        &[
            "--config-body",
            yaml,
            "--rtc.allow_tcp_fallback=false",
            "--rtc.tcp_fallback_rtt_threshold=-11",
            "--rtc.allow_udp_unstable_fallback=true",
        ],
        &environment,
    );
    let policy = rtc_core::config::RoomPolicy::from(&from_cli.server).transport_fallback;
    assert!(!policy.allow_tcp);
    assert_eq!(policy.rtt_threshold_ms, -11);
    assert!(policy.allow_udp_unstable);
}
