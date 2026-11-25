//! C96-rtc config_projection legs for the nine `rtc.*` leaves of this lane.
//! Every leg drives YAML / ENV / CLI through the public lk-config loading API
//! (`parse_with_probe`) and asserts the value that lands in the projected
//! `ServerConfig.rtc` the media engine consumes, not merely the raw parsed tree.
//!
//! Go oracle: references/livekit-stable @ e5655ac (v1.13.10).
//! Production consumers of the projected fields:
//! - rtc-core/src/config.rs:111-113 (RoomPolicy::from copies the three reconnect
//!   bools) -> rtc-core/src/room/error_supervision.rs:154 and :225,
//!   rtc-core/src/room/media_events.rs:80 (full_reconnect gates).
//! - rtc-media/src/signaling.rs:1818 (data_track_target_latency), :1830-1835
//!   (slow threshold / max buffered / lossy target) -> ordinary_data_writer.rs:97,
//!   data_track_writer.rs:113/:122.
//! - rtc-media/src/engine.rs:466 (enable_rtp_stream_restart_detection).
//! Not projected at all: `rtc.strict_acks` (see its test for the finding).
use lk_config::{EntryAction, Env, ProgramInfo, netdetect::NetProbe, parse_with_probe};
use rtc_core::config::PolicyDuration;
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

/// Projected field accessor: the load path is what this tier tests, so a
/// non-RunServer outcome is a fixture failure, never a skipped leg.
fn rtc(args: &[&str], env: &Env) -> rtc_core::config::RtcConfig {
    parse(args, env).server.rtc.clone()
}

// ---------------------------------------------------------------------------
// YAML:rtc.reconnect_on_publication_error / _subscription_error /
// _data_channel_error + CONFIG_CONSUMPTION:rtc.reconnect_on_*
//
// Upstream models all three as *bool (config.go:171,174,177) and resolves
// absent -> false in code (roommanager.go:456-474), then hands the resolved
// bool to the participant params (roommanager.go:524-526) where it gates a
// forced full reconnect (participant.go:4093, :4110, :4248). The port projects
// Option<bool> -> bool with unwrap_or(false) (normalize.rs:644-646), which is
// the same default but collapses absent-vs-explicit-false.
// ---------------------------------------------------------------------------

#[test]
fn rtc_reconnect_error_bools_yaml_env_cli_projection_and_precedence() {
    // Absent: upstream default is false (roommanager.go:456/462/468).
    let base = rtc(&[], &Env::default());
    assert!(!base.reconnect_on_publication_error);
    assert!(!base.reconnect_on_subscription_error);
    assert!(!base.reconnect_on_data_channel_error);

    // YAML true on each key independently: the projection carries it.
    let yaml = "rtc:\n  reconnect_on_publication_error: true\n";
    let cfg = rtc(&["--config-body", yaml], &Env::default());
    assert!(cfg.reconnect_on_publication_error);
    assert!(!cfg.reconnect_on_subscription_error);
    assert!(!cfg.reconnect_on_data_channel_error);

    let yaml = "rtc:\n  reconnect_on_subscription_error: true\n";
    let cfg = rtc(&["--config-body", yaml], &Env::default());
    assert!(cfg.reconnect_on_subscription_error);
    assert!(!cfg.reconnect_on_publication_error);

    let yaml = "rtc:\n  reconnect_on_data_channel_error: true\n";
    let cfg = rtc(&["--config-body", yaml], &Env::default());
    assert!(cfg.reconnect_on_data_channel_error);
    assert!(!cfg.reconnect_on_publication_error);

    // Explicit false stays false and is distinguishable from absent only at the
    // raw layer: official keeps Option<bool>, the projection flattens it.
    let yaml = "rtc:\n  reconnect_on_publication_error: false\n";
    let off = parse(&["--config-body", yaml], &Env::default());
    assert_eq!(off.official.rtc.reconnect_on_publication_error, Some(false));
    assert!(!off.server.rtc.reconnect_on_publication_error);
    // ... while an absent key is None upstream (the *nil* pointer roommanager
    // checks) and the port's own default representation.
    let absent = parse(&[], &Env::default());
    assert_eq!(absent.official.rtc.reconnect_on_publication_error, None);
    assert_eq!(
        absent.server.rtc.reconnect_on_publication_error,
        off.server.rtc.reconnect_on_publication_error,
        "unwrap_or(false) makes explicit-false and absent identical downstream"
    );

    // ENV surface exists for every generated leaf path.
    let env = Env::from_pairs([
        ("LIVEKIT_RTC_RECONNECT_ON_PUBLICATION_ERROR", "true"),
        ("LIVEKIT_RTC_RECONNECT_ON_SUBSCRIPTION_ERROR", "true"),
        ("LIVEKIT_RTC_RECONNECT_ON_DATA_CHANNEL_ERROR", "true"),
    ]);
    let cfg = rtc(&[], &env);
    assert!(cfg.reconnect_on_publication_error);
    assert!(cfg.reconnect_on_subscription_error);
    assert!(cfg.reconnect_on_data_channel_error);

    // ENV false overrides the false default without changing the neighbours.
    let env_off = Env::from_pairs([("LIVEKIT_RTC_RECONNECT_ON_DATA_CHANNEL_ERROR", "false")]);
    let cfg = rtc(&[], &env_off);
    assert!(!cfg.reconnect_on_data_channel_error);

    // CLI surface (--rtc.<path>) reaches the projection in both directions.
    let cfg = rtc(
        &[
            "--rtc.reconnect_on_publication_error=true",
            "--rtc.reconnect_on_subscription_error=true",
            "--rtc.reconnect_on_data_channel_error=true",
        ],
        &Env::default(),
    );
    assert!(cfg.reconnect_on_publication_error);
    assert!(cfg.reconnect_on_subscription_error);
    assert!(cfg.reconnect_on_data_channel_error);

    // CLI beats ENV: ENV true, CLI false -> false downstream.
    let cfg = rtc(&["--rtc.reconnect_on_publication_error=false"], &env);
    assert!(!cfg.reconnect_on_publication_error);
    // CLI false beats ENV false-then-true on the data-channel key too.
    let cfg = rtc(
        &["--rtc.reconnect_on_data_channel_error=false"],
        &Env::from_pairs([("LIVEKIT_RTC_RECONNECT_ON_DATA_CHANNEL_ERROR", "true")]),
    );
    assert!(!cfg.reconnect_on_data_channel_error);

    // ENV beats YAML: YAML false overridden by ENV true.
    let yaml = "rtc:\n  reconnect_on_subscription_error: false\n";
    let cfg = parse(
        &["--config-body", yaml],
        &Env::from_pairs([("LIVEKIT_RTC_RECONNECT_ON_SUBSCRIPTION_ERROR", "true")]),
    );
    assert!(cfg.server.rtc.reconnect_on_subscription_error);
    // CLI beats YAML.
    let cfg = rtc(
        &[
            "--config-body",
            "rtc:\n  reconnect_on_subscription_error: true\n",
            "--rtc.reconnect_on_subscription_error=false",
        ],
        &Env::default(),
    );
    assert!(!cfg.reconnect_on_subscription_error);

    // Boundary: the YAML 1.1 boolean spellings Go's yaml.v3 accepts for a typed
    // bool must project true, not silently fall back to the false default.
    for (key, spelling) in [
        ("reconnect_on_publication_error", "yes"),
        ("reconnect_on_subscription_error", "on"),
        ("reconnect_on_data_channel_error", "True"),
    ] {
        let yaml = format!("rtc:\n  {key}: {spelling}\n");
        let cfg = rtc(&["--config-body", &yaml], &Env::default());
        match key {
            "reconnect_on_publication_error" => assert!(cfg.reconnect_on_publication_error),
            "reconnect_on_subscription_error" => assert!(cfg.reconnect_on_subscription_error),
            _ => assert!(cfg.reconnect_on_data_channel_error),
        }
    }
}

// ---------------------------------------------------------------------------
// YAML:rtc.data_channel_max_buffered_amount + CONFIG_CONSUMPTION id
//
// Upstream uint64 (config.go:180), zero default, no DefaultConfig entry; it is
// consumed only when DatachannelSlowThreshold == 0
// (transport.go:1592 -> ErrDataChannelBufferFull). Port: u64 copied verbatim
// (normalize.rs:621) into signaling.rs:1831 -> ordinary_data_writer.rs:97.
// ---------------------------------------------------------------------------

#[test]
fn rtc_data_channel_max_buffered_amount_yaml_env_cli_projection_and_precedence() {
    // Absent: upstream leaves it zero-valued (no DefaultConfig entry for it,
    // config.go:569-591); a zero value disables the buffered-amount reject.
    let base = rtc(&[], &Env::default());
    assert_eq!(base.data_channel_max_buffered_amount, 0);

    // YAML explicit 0 is the same number as absent, but explicit nonzero moves
    // the projection; type is u64, not i64/usize.
    let cfg = rtc(
        &[
            "--config-body",
            "rtc:\n  data_channel_max_buffered_amount: 0\n",
        ],
        &Env::default(),
    );
    assert_eq!(cfg.data_channel_max_buffered_amount, 0u64);
    let cfg = rtc(
        &[
            "--config-body",
            "rtc:\n  data_channel_max_buffered_amount: 65536\n",
        ],
        &Env::default(),
    );
    assert_eq!(cfg.data_channel_max_buffered_amount, 65536u64);
    assert_ne!(
        cfg.data_channel_max_buffered_amount,
        base.data_channel_max_buffered_amount
    );

    // Boundary: u64 range survives the projection (an i64 copy would fail here).
    let huge = "18446744073709551615";
    let yaml = format!("rtc:\n  data_channel_max_buffered_amount: {huge}\n");
    let cfg = rtc(&["--config-body", &yaml], &Env::default());
    assert_eq!(cfg.data_channel_max_buffered_amount, u64::MAX);

    // Negative stimulus: Go's yaml.v3 rejects it for a uint64 target, so the
    // load must fail rather than wrap or clamp to zero.
    let neg_err = std::panic::catch_unwind(|| {
        rtc(
            &[
                "--config-body",
                "rtc:\n  data_channel_max_buffered_amount: -1\n",
            ],
            &Env::default(),
        )
    });
    assert!(
        neg_err.is_err(),
        "a negative uint64 buffered amount must not project"
    );

    // ENV surface.
    let env = Env::from_pairs([("LIVEKIT_RTC_DATA_CHANNEL_MAX_BUFFERED_AMOUNT", "8192")]);
    let cfg = rtc(&[], &env);
    assert_eq!(cfg.data_channel_max_buffered_amount, 8192);

    // CLI surface, plus CLI-beats-ENV and ENV-beats-YAML precedence.
    let cfg = rtc(&["--rtc.data_channel_max_buffered_amount=4096"], &env);
    assert_eq!(cfg.data_channel_max_buffered_amount, 4096);
    let yaml = "rtc:\n  data_channel_max_buffered_amount: 1024\n";
    let cfg = parse(&["--config-body", yaml], &env);
    assert_eq!(cfg.server.rtc.data_channel_max_buffered_amount, 8192);
    let cfg = rtc(
        &[
            "--config-body",
            yaml,
            "--rtc.data_channel_max_buffered_amount=2048",
        ],
        &Env::default(),
    );
    assert_eq!(cfg.data_channel_max_buffered_amount, 2048);
}

// ---------------------------------------------------------------------------
// YAML:rtc.datachannel_slow_threshold + CONFIG_CONSUMPTION id
//
// Upstream int (config.go:184), zero default, no DefaultConfig entry. Consumed
// three ways: >0 enables Pion block-writes (transport.go:375), it is handed to
// the reliable writer (transport.go:934/952/1395) and ==0 gates the
// DataChannelMaxBufferedAmount reject (transport.go:1592). Signed int -> i64
// copy (normalize.rs:620).
// ---------------------------------------------------------------------------

#[test]
fn rtc_datachannel_slow_threshold_yaml_env_cli_projection_and_precedence() {
    let base = rtc(&[], &Env::default());
    assert_eq!(base.data_channel_slow_threshold, 0);

    let cfg = rtc(
        &[
            "--config-body",
            "rtc:\n  datachannel_slow_threshold: 1048576\n",
        ],
        &Env::default(),
    );
    assert_eq!(cfg.data_channel_slow_threshold, 1_048_576i64);
    assert_ne!(
        cfg.data_channel_slow_threshold,
        base.data_channel_slow_threshold
    );

    // Boundary: negative is representable upstream (int) and the port keeps the
    // sign, unlike a duration->unsigned conversion.
    let cfg = rtc(
        &["--config-body", "rtc:\n  datachannel_slow_threshold: -5\n"],
        &Env::default(),
    );
    assert_eq!(cfg.data_channel_slow_threshold, -5);

    // Boundary: explicit 0 equals the default numerically but must still be a
    // successful load (it is the "threshold disabled" branch of transport.go:375).
    let cfg = rtc(
        &["--config-body", "rtc:\n  datachannel_slow_threshold: 0\n"],
        &Env::default(),
    );
    assert_eq!(cfg.data_channel_slow_threshold, 0);

    // A huge positive duration-shaped integer stays an integer, not a time. The
    // leaf is typed `int` upstream (config.go:184), so the generated flag surface
    // carries int64 text exactly; i64::MAX itself is refused by the CLI layer as
    // out of range for that int64 flag value.
    let cfg = rtc(
        &[
            "--config-body",
            "rtc:\n  datachannel_slow_threshold: 9223372036854775807\n",
        ],
        &Env::default(),
    );
    assert_eq!(cfg.data_channel_slow_threshold, i64::MAX);
    // The flag surface accepts the full int64 range (measured, not assumed).
    let cfg = rtc(
        &["--rtc.datachannel_slow_threshold=9223372036854775807"],
        &Env::default(),
    );
    assert_eq!(cfg.data_channel_slow_threshold, i64::MAX);

    let env = Env::from_pairs([("LIVEKIT_RTC_DATACHANNEL_SLOW_THRESHOLD", "150000")]);
    let cfg = rtc(&[], &env);
    assert_eq!(cfg.data_channel_slow_threshold, 150_000);
    let cfg = rtc(&["--rtc.datachannel_slow_threshold=-1"], &env);
    assert_eq!(cfg.data_channel_slow_threshold, -1);
    let yaml = "rtc:\n  datachannel_slow_threshold: 777\n";
    let cfg = parse(&["--config-body", yaml], &env);
    assert_eq!(cfg.server.rtc.data_channel_slow_threshold, 150_000);
    let cfg = rtc(
        &[
            "--config-body",
            yaml,
            "--rtc.datachannel_slow_threshold=888",
        ],
        &Env::default(),
    );
    assert_eq!(cfg.data_channel_slow_threshold, 888);
}

// ---------------------------------------------------------------------------
// YAML:rtc.datachannel_lossy_target_latency + CONFIG_CONSUMPTION id
//
// Upstream time.Duration (config.go:187), zero default (no DefaultConfig entry);
// given to the lossy writer (transport.go:959, :1335). The port converts the
// parsed GoDuration nanos into a signed PolicyDuration (normalize.rs:622-624),
// so the unit claim under test is "nanoseconds preserved exactly".
// ---------------------------------------------------------------------------

#[test]
fn rtc_datachannel_lossy_target_latency_yaml_env_cli_projection_and_precedence() {
    let base = rtc(&[], &Env::default());
    assert_eq!(base.data_channel_lossy_target_latency, PolicyDuration::ZERO);

    // 1500ms: nanosecond fidelity, not whole-second truncation.
    let cfg = rtc(
        &[
            "--config-body",
            "rtc:\n  datachannel_lossy_target_latency: 1500ms\n",
        ],
        &Env::default(),
    );
    assert_eq!(
        cfg.data_channel_lossy_target_latency,
        PolicyDuration::from_millis(1500)
    );
    assert_eq!(
        cfg.data_channel_lossy_target_latency.as_nanos_i64(),
        1_500_000_000
    );
    assert_ne!(
        cfg.data_channel_lossy_target_latency,
        base.data_channel_lossy_target_latency
    );

    // Sub-microsecond unit: 250us must not round to 0 or to 1ms.
    let cfg = rtc(
        &[
            "--config-body",
            "rtc:\n  datachannel_lossy_target_latency: 250us\n",
        ],
        &Env::default(),
    );
    assert_eq!(
        cfg.data_channel_lossy_target_latency.as_nanos_i64(),
        250_000
    );
    assert_eq!(cfg.data_channel_lossy_target_latency.as_millis_i64(), 0);

    // Huge duration boundary: hours-scale stays exact.
    let cfg = rtc(
        &[
            "--config-body",
            "rtc:\n  datachannel_lossy_target_latency: 25h\n",
        ],
        &Env::default(),
    );
    assert_eq!(
        cfg.data_channel_lossy_target_latency,
        PolicyDuration::from_nanos(25 * 3_600_000_000_000)
    );

    // Negative duration boundary: signed source is preserved (Go allows it and
    // the port documents negative/zero as distinct).
    let cfg = rtc(
        &[
            "--config-body",
            "rtc:\n  datachannel_lossy_target_latency: -3s\n",
        ],
        &Env::default(),
    );
    assert_eq!(
        cfg.data_channel_lossy_target_latency.as_nanos_i64(),
        -3_000_000_000
    );

    // Boundary: the disable switch upstream documents (config.go:190-193) is the
    // zero Duration, spelled "0s" because a bare !!int is not a time.Duration --
    // same as Go's yaml.v3, which rejects `0` with
    // "cannot unmarshal !!int `0` into time.Duration". It must stay exactly 0 and
    // must not snap back to the 100ms default.
    let cfg = rtc(
        &[
            "--config-body",
            "rtc:\n  datachannel_lossy_target_latency: 0s\n",
        ],
        &Env::default(),
    );
    assert_eq!(cfg.data_channel_lossy_target_latency, PolicyDuration::ZERO);
    assert_ne!(
        cfg.data_channel_lossy_target_latency,
        PolicyDuration::from_millis(100),
        "explicit zero is not the data-track default"
    );
    let bare_int = std::panic::catch_unwind(|| {
        rtc(
            &[
                "--config-body",
                "rtc:\n  datachannel_lossy_target_latency: 0\n",
            ],
            &Env::default(),
        )
    });
    assert!(
        bare_int.is_err(),
        "a bare integer must not parse as a time.Duration"
    );

    let env = Env::from_pairs([("LIVEKIT_RTC_DATACHANNEL_LOSSY_TARGET_LATENCY", "4s")]);
    let cfg = rtc(&[], &env);
    assert_eq!(
        cfg.data_channel_lossy_target_latency,
        PolicyDuration::from_secs(4)
    );
    let cfg = rtc(&["--rtc.datachannel_lossy_target_latency=6s"], &env);
    assert_eq!(
        cfg.data_channel_lossy_target_latency,
        PolicyDuration::from_secs(6)
    );
    let yaml = "rtc:\n  datachannel_lossy_target_latency: 1s\n";
    let cfg = parse(&["--config-body", yaml], &env);
    assert_eq!(
        cfg.server.rtc.data_channel_lossy_target_latency,
        PolicyDuration::from_secs(4)
    );
    let cfg = rtc(
        &[
            "--config-body",
            yaml,
            "--rtc.datachannel_lossy_target_latency=2s",
        ],
        &Env::default(),
    );
    assert_eq!(
        cfg.data_channel_lossy_target_latency,
        PolicyDuration::from_secs(2)
    );
}

// ---------------------------------------------------------------------------
// YAML:rtc.datachannel_data_track_target_latency + CONFIG_CONSUMPTION id
//
// Upstream time.Duration whose only non-zero default in this family is set in
// code: DefaultConfig.RTC.DatachannelDataTrackTargetLatency = 100ms
// (config.go:581). Consumed by the data-track unreliable writer
// (transport.go:945, :1337). Port: normalize.rs:625-627.
// ---------------------------------------------------------------------------

#[test]
fn rtc_datachannel_data_track_target_latency_yaml_env_cli_projection_and_precedence() {
    // Absent: the code-applied 100ms default must reach the projection.
    let base = rtc(&[], &Env::default());
    assert_eq!(
        base.data_track_target_latency,
        PolicyDuration::from_millis(100)
    );
    assert_eq!(base.data_track_target_latency.as_nanos_i64(), 100_000_000);

    let cfg = rtc(
        &[
            "--config-body",
            "rtc:\n  datachannel_data_track_target_latency: 250ms\n",
        ],
        &Env::default(),
    );
    assert_eq!(
        cfg.data_track_target_latency,
        PolicyDuration::from_millis(250)
    );
    assert_ne!(
        cfg.data_track_target_latency,
        base.data_track_target_latency
    );

    // Boundary: explicit 0s disables buffering control upstream (config.go:190-
    // 193 comment) and must stay exactly 0, not snap back to the 100ms default.
    let cfg = rtc(
        &[
            "--config-body",
            "rtc:\n  datachannel_data_track_target_latency: 0s\n",
        ],
        &Env::default(),
    );
    assert_eq!(cfg.data_track_target_latency, PolicyDuration::ZERO);
    assert_ne!(
        cfg.data_track_target_latency, base.data_track_target_latency,
        "explicit 0s must override the code-applied 100ms default"
    );

    // Sub-millisecond unit fidelity.
    let cfg = rtc(
        &[
            "--config-body",
            "rtc:\n  datachannel_data_track_target_latency: 750us\n",
        ],
        &Env::default(),
    );
    assert_eq!(cfg.data_track_target_latency.as_nanos_i64(), 750_000);

    // Huge duration.
    let cfg = rtc(
        &[
            "--config-body",
            "rtc:\n  datachannel_data_track_target_latency: 72h\n",
        ],
        &Env::default(),
    );
    assert_eq!(
        cfg.data_track_target_latency,
        PolicyDuration::from_nanos(72 * 3_600_000_000_000)
    );

    let env = Env::from_pairs([("LIVEKIT_RTC_DATACHANNEL_DATA_TRACK_TARGET_LATENCY", "30s")]);
    let cfg = rtc(&[], &env);
    assert_eq!(cfg.data_track_target_latency, PolicyDuration::from_secs(30));
    let cfg = rtc(&["--rtc.datachannel_data_track_target_latency=45s"], &env);
    assert_eq!(cfg.data_track_target_latency, PolicyDuration::from_secs(45));
    let yaml = "rtc:\n  datachannel_data_track_target_latency: 5s\n";
    let cfg = parse(&["--config-body", yaml], &env);
    assert_eq!(
        cfg.server.rtc.data_track_target_latency,
        PolicyDuration::from_secs(30)
    );
    let cfg = rtc(
        &[
            "--config-body",
            yaml,
            "--rtc.datachannel_data_track_target_latency=6s",
        ],
        &Env::default(),
    );
    assert_eq!(cfg.data_track_target_latency, PolicyDuration::from_secs(6));
}

// ---------------------------------------------------------------------------
// YAML:rtc.enable_rtp_stream_restart_detection + CONFIG_CONSUMPTION id
//
// Upstream bool (config.go:198), false default (no DefaultConfig entry), passed
// through roommanager.go:544 -> participant.go:3718 -> mediatrack.go:419
// (sfu.WithEnableRTPStreamRestartDetection). Port: bool copy (normalize.rs:619)
// consumed at rtc-media/src/engine.rs:466.
// ---------------------------------------------------------------------------

#[test]
fn rtc_enable_rtp_stream_restart_detection_yaml_env_cli_projection_and_precedence() {
    let base = rtc(&[], &Env::default());
    assert!(!base.enable_rtp_stream_restart_detection);

    let cfg = rtc(
        &[
            "--config-body",
            "rtc:\n  enable_rtp_stream_restart_detection: true\n",
        ],
        &Env::default(),
    );
    assert!(cfg.enable_rtp_stream_restart_detection);
    assert_ne!(
        cfg.enable_rtp_stream_restart_detection,
        base.enable_rtp_stream_restart_detection
    );

    // Boundary: explicit false is not treated as "unset -> enable something".
    let cfg = rtc(
        &[
            "--config-body",
            "rtc:\n  enable_rtp_stream_restart_detection: false\n",
        ],
        &Env::default(),
    );
    assert!(!cfg.enable_rtp_stream_restart_detection);

    // YAML 1.1 boolean spellings accepted for a typed bool target upstream.
    for spelling in ["yes", "on", "True"] {
        let yaml = format!("rtc:\n  enable_rtp_stream_restart_detection: {spelling}\n");
        assert!(
            rtc(&["--config-body", &yaml], &Env::default()).enable_rtp_stream_restart_detection,
            "{spelling} must project true"
        );
    }

    let env = Env::from_pairs([("LIVEKIT_RTC_ENABLE_RTP_STREAM_RESTART_DETECTION", "true")]);
    assert!(rtc(&[], &env).enable_rtp_stream_restart_detection);
    let env_off = Env::from_pairs([("LIVEKIT_RTC_ENABLE_RTP_STREAM_RESTART_DETECTION", "false")]);
    assert!(!rtc(&[], &env_off).enable_rtp_stream_restart_detection);

    let cfg = rtc(
        &["--rtc.enable_rtp_stream_restart_detection=true"],
        &Env::default(),
    );
    assert!(cfg.enable_rtp_stream_restart_detection);
    // CLI beats ENV in both directions.
    assert!(
        !rtc(&["--rtc.enable_rtp_stream_restart_detection=false"], &env)
            .enable_rtp_stream_restart_detection
    );
    assert!(
        rtc(
            &["--rtc.enable_rtp_stream_restart_detection=true"],
            &env_off
        )
        .enable_rtp_stream_restart_detection
    );
    // ENV beats YAML; CLI beats YAML.
    let yaml = "rtc:\n  enable_rtp_stream_restart_detection: false\n";
    assert!(
        parse(&["--config-body", yaml], &env)
            .server
            .rtc
            .enable_rtp_stream_restart_detection
    );
    assert!(
        !rtc(
            &[
                "--config-body",
                yaml,
                "--rtc.enable_rtp_stream_restart_detection=false"
            ],
            &env
        )
        .enable_rtp_stream_restart_detection
    );
}

// ---------------------------------------------------------------------------
// YAML:rtc.strict_acks -- NOT_PROJECTED finding.
//
// Upstream declares it Deprecated (config.go:140-141) and, in this pinned tree,
// nothing reads it: `grep -rn StrictACKs references/livekit-stable` returns the
// struct tag only. The port parses it (official.rs:321) and exposes the flag
// surface, but there is no line in normalize.rs that copies it into
// ServerConfig, so no consumer can ever see it. These legs pin that contract:
// the raw value follows YAML/ENV/CLI precedence, and the projection never
// changes whatever else in rtc is configured.
// ---------------------------------------------------------------------------

#[test]
fn rtc_strict_acks_is_parsed_but_not_projected() {
    // Absent: upstream zero value (bool, no DefaultConfig entry).
    let base = parse(&[], &Env::default());
    assert!(!base.official.rtc.strict_acks);

    // YAML true reaches the raw tree...
    let cfg = parse(
        &["--config-body", "rtc:\n  strict_acks: true\n"],
        &Env::default(),
    );
    assert!(cfg.official.rtc.strict_acks);
    // ...and nothing appears in the projected config: flipping strict_acks must
    // leave the whole projected RtcConfig identical, which is the measurable
    // form of "not projected".
    let off_raw = base.official.rtc.clone();
    let off_proj = base.server.rtc.clone();
    let on_raw = cfg.official.rtc.clone();
    let on_proj = cfg.server.rtc.clone();
    assert_ne!(
        on_raw.strict_acks, off_raw.strict_acks,
        "raw parse must follow the YAML value"
    );
    assert_eq!(
        on_proj.reconnect_on_publication_error,
        off_proj.reconnect_on_publication_error
    );
    assert_eq!(
        on_proj.data_channel_max_buffered_amount,
        off_proj.data_channel_max_buffered_amount
    );
    assert_eq!(
        on_proj.data_channel_slow_threshold,
        off_proj.data_channel_slow_threshold
    );
    assert_eq!(
        on_proj.data_track_target_latency,
        off_proj.data_track_target_latency
    );
    assert_eq!(
        on_proj.enable_rtp_stream_restart_detection,
        off_proj.enable_rtp_stream_restart_detection
    );
    // There is no strict_acks-shaped field at all in the projected struct: a
    // sibling flag that does project proves the comparison is meaningful.
    let sibling = parse(
        &[
            "--config-body",
            "rtc:\n  strict_acks: true\n  use_ice_lite: true\n",
        ],
        &Env::default(),
    );
    assert!(sibling.server.rtc.use_ice_lite);
    assert!(sibling.official.rtc.strict_acks);

    // ENV / CLI surfaces exist for the raw leaf (generated flag table).
    let env = Env::from_pairs([("LIVEKIT_RTC_STRICT_ACKS", "true")]);
    assert!(parse(&[], &env).official.rtc.strict_acks);
    assert!(
        !parse(&[], &env).server.rtc.reconnect_on_publication_error,
        "strict_acks ENV must not leak into any projected field"
    );
    let cfg = parse(&["--rtc.strict_acks=true"], &Env::default());
    assert!(cfg.official.rtc.strict_acks);
    // CLI beats YAML in the false direction on the raw leaf.
    let cfg = parse(
        &[
            "--config-body",
            "rtc:\n  strict_acks: true\n",
            "--rtc.strict_acks=false",
        ],
        &Env::default(),
    );
    assert!(!cfg.official.rtc.strict_acks);

    // !!null-tagged mapping: an own field of RTCConfig decodes (unlike an
    // inline embedded field, which panics upstream).
    let cfg = parse(
        &["--config-body", "rtc: !!null {strict_acks: true}\n"],
        &Env::default(),
    );
    assert!(cfg.official.rtc.strict_acks);
    // The port models it as a plain bool, so there is no Option/nil distinction
    // to project: explicit false and absent are one value upstream too.
    let cfg = parse(
        &["--config-body", "rtc:\n  strict_acks: false\n"],
        &Env::default(),
    );
    assert!(!cfg.official.rtc.strict_acks);
}
