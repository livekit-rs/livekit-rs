//! Config leaf parsing for remaining IMPLEMENTED_UNVERIFIED items.
//! Pure config parse + normalize + policy projection. No server or network.
use lk_config::{EntryAction, Env, ProgramInfo, netdetect::NetProbe, parse_with_probe};
use std::{ffi::OsString, net::IpAddr, time::Duration};

struct NoNetwork;
impl NetProbe for NoNetwork {
    fn local_addresses(&self) -> Vec<IpAddr> {
        vec!["127.0.0.1".parse().unwrap()]
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
        EntryAction::PrintHelp {
            text, exit_code, ..
        } => {
            panic!("got PrintHelp(exit={}): {}", exit_code, text)
        }
        _ => panic!("expected RunServer, got other variant"),
    }
}

// ---------------------------------------------------------------------------
// rtc.require_ipv4
// ---------------------------------------------------------------------------

#[test]
fn rtc_require_ipv4_yaml_cli_env_parse() {
    // Default: false
    let config = parse(&[], &Env::default());
    assert!(
        !config.official.rtc.require_ipv4,
        "rtc.require_ipv4 defaults to false"
    );

    // YAML: rtc.require_ipv4: true
    let config = parse(
        &["--config-body", "rtc:\n  require_ipv4: true\n"],
        &Env::default(),
    );
    assert!(
        config.official.rtc.require_ipv4,
        "YAML rtc.require_ipv4=true must be parsed"
    );

    // CLI: --rtc.require_ipv4=true
    let config = parse(&["--rtc.require_ipv4=true"], &Env::default());
    assert!(
        config.official.rtc.require_ipv4,
        "CLI --rtc.require_ipv4=true must be parsed"
    );

    // ENV: LIVEKIT_RTC_REQUIRE_IPV4=true
    let env = Env::from_pairs([("LIVEKIT_RTC_REQUIRE_IPV4", "true")]);
    let config = parse(&[], &env);
    assert!(
        config.official.rtc.require_ipv4,
        "ENV LIVEKIT_RTC_REQUIRE_IPV4=true must be parsed"
    );
}

// ---------------------------------------------------------------------------
// audio.active_level / min_percentile / smooth_intervals
// ---------------------------------------------------------------------------

#[test]
fn audio_config_yaml_cli_env_parse_and_project() {
    // Defaults match Go: active_level=35, min_percentile=40, smooth_intervals=2
    let config = parse(&[], &Env::default());
    assert_eq!(config.official.audio.active_level, 35);
    assert_eq!(config.official.audio.min_percentile, 40);
    assert_eq!(config.official.audio.smooth_intervals, 2);
    assert_eq!(config.server.audio.active_level, 35);
    assert_eq!(config.server.audio.min_percentile, 40);
    assert_eq!(config.server.audio.smooth_intervals, 2);

    // YAML
    let yaml = "audio:\n  active_level: 50\n  min_percentile: 60\n  smooth_intervals: 4\n";
    let config = parse(&["--config-body", yaml], &Env::default());
    assert_eq!(config.official.audio.active_level, 50);
    assert_eq!(config.official.audio.min_percentile, 60);
    assert_eq!(config.official.audio.smooth_intervals, 4);
    assert_eq!(config.server.audio.active_level, 50);
    assert_eq!(config.server.audio.min_percentile, 60);
    assert_eq!(config.server.audio.smooth_intervals, 4);

    // CLI
    let config = parse(
        &[
            "--audio.active_level=80",
            "--audio.min_percentile=90",
            "--audio.smooth_intervals=6",
        ],
        &Env::default(),
    );
    assert_eq!(config.server.audio.active_level, 80);
    assert_eq!(config.server.audio.min_percentile, 90);
    assert_eq!(config.server.audio.smooth_intervals, 6);

    // ENV
    let env = Env::from_pairs([
        ("LIVEKIT_AUDIO_ACTIVE_LEVEL", "100"),
        ("LIVEKIT_AUDIO_MIN_PERCENTILE", "75"),
        ("LIVEKIT_AUDIO_SMOOTH_INTERVALS", "3"),
    ]);
    let config = parse(&[], &env);
    assert_eq!(config.server.audio.active_level, 100);
    assert_eq!(config.server.audio.min_percentile, 75);
    assert_eq!(config.server.audio.smooth_intervals, 3);
}

// ---------------------------------------------------------------------------
// room.playout_delay.enabled
// ---------------------------------------------------------------------------

#[test]
fn room_playout_delay_enabled_yaml_and_projection() {
    // Default: enabled=false → playout_delay projects to None
    let config = parse(&[], &Env::default());
    assert!(!config.official.room.playout_delay.enabled);
    assert_eq!(config.server.room.playout_delay, None);

    // YAML: enabled=true with min/max → projects to Some((min, max))
    let yaml = "room:\n  playout_delay:\n    enabled: true\n    min: 100\n    max: 400\n";
    let config = parse(&["--config-body", yaml], &Env::default());
    assert!(config.official.room.playout_delay.enabled);
    assert_eq!(config.official.room.playout_delay.min, 100);
    assert_eq!(config.official.room.playout_delay.max, 400);
    assert_eq!(config.server.room.playout_delay, Some((100, 400)));

    // YAML: enabled=false with min/max → projects to None (enabled gates it)
    let yaml = "room:\n  playout_delay:\n    enabled: false\n    min: 100\n    max: 400\n";
    let config = parse(&["--config-body", yaml], &Env::default());
    assert!(!config.official.room.playout_delay.enabled);
    assert_eq!(config.server.room.playout_delay, None);

    // CLI: --room.playout_delay.enabled=true
    let config = parse(
        &[
            "--room.playout_delay.enabled=true",
            "--room.playout_delay.min=200",
            "--room.playout_delay.max=500",
        ],
        &Env::default(),
    );
    assert!(config.official.room.playout_delay.enabled);
    assert_eq!(config.server.room.playout_delay, Some((200, 500)));
}

// ---------------------------------------------------------------------------
// room.sync_streams
// ---------------------------------------------------------------------------

#[test]
fn room_sync_streams_yaml_cli_env_parse_and_project() {
    // Default: false
    let config = parse(&[], &Env::default());
    assert!(!config.server.room.sync_streams);

    // YAML
    let config = parse(
        &["--config-body", "room:\n  sync_streams: true\n"],
        &Env::default(),
    );
    assert!(config.server.room.sync_streams);

    // CLI
    let config = parse(&["--room.sync_streams=true"], &Env::default());
    assert!(config.server.room.sync_streams);

    // ENV
    let env = Env::from_pairs([("LIVEKIT_ROOM_SYNC_STREAMS", "true")]);
    let config = parse(&[], &env);
    assert!(config.server.room.sync_streams);
}

// ---------------------------------------------------------------------------
// room.enable_full_room_in_webhooks
// ---------------------------------------------------------------------------

#[test]
fn room_enable_full_room_in_webhooks_yaml_cli_env_parse_and_project() {
    // Default: false
    let config = parse(&[], &Env::default());
    assert!(!config.server.room.enable_full_room_in_webhooks);

    // YAML
    let yaml = "room:\n  enable_full_room_in_webhooks: true\n";
    let config = parse(&["--config-body", yaml], &Env::default());
    assert!(config.server.room.enable_full_room_in_webhooks);

    // CLI
    let config = parse(
        &["--room.enable_full_room_in_webhooks=true"],
        &Env::default(),
    );
    assert!(config.server.room.enable_full_room_in_webhooks);

    // ENV
    let env = Env::from_pairs([("LIVEKIT_ROOM_ENABLE_FULL_ROOM_IN_WEBHOOKS", "true")]);
    let config = parse(&[], &env);
    assert!(config.server.room.enable_full_room_in_webhooks);
}

// ---------------------------------------------------------------------------
// room.room_configurations.{key}.sync_streams
// ---------------------------------------------------------------------------

#[test]
fn room_configurations_sync_streams_yaml_parse_and_preset_projection() {
    // YAML with sync_streams: true in a named preset
    let yaml = "room:\n  room_configurations:\n    mypreset:\n      sync_streams: true\n";
    let config = parse(&["--config-body", yaml], &Env::default());

    // Raw official config: the preset exists with sync_streams=true
    let raw_preset = config
        .official
        .room
        .room_configurations
        .get("mypreset")
        .expect("preset must exist")
        .as_ref()
        .expect("preset must not be nil");
    assert!(raw_preset.sync_streams);

    // Normalized projection: preset sync_streams=true → Some(true)
    let projected = config.server.room.presets.get("mypreset").unwrap();
    let projected = projected.as_ref().unwrap();
    assert_eq!(projected.sync_streams, Some(true));

    // sync_streams: false (default) → None in projection
    let yaml2 = "room:\n  room_configurations:\n    default_preset:\n      max_participants: 10\n";
    let config2 = parse(&["--config-body", yaml2], &Env::default());
    let projected2 = config2.server.room.presets.get("default_preset").unwrap();
    let projected2 = projected2.as_ref().unwrap();
    assert_eq!(projected2.sync_streams, None);
}

// ---------------------------------------------------------------------------
// room.room_configurations.{key}.agents
// ---------------------------------------------------------------------------

#[test]
fn room_configurations_agents_yaml_parse_and_preset_projection() {
    let yaml = concat!(
        "room:\n",
        "  room_configurations:\n",
        "    agentpreset:\n",
        "      agents:\n",
        "        - agent_name: assistant\n",
        "          metadata: context\n",
        "          restart_policy: 1\n",
        "          deployment: prod\n",
        "          attributes:\n",
        "            k: v\n",
    );
    let config = parse(&["--config-body", yaml], &Env::default());

    // Raw official config
    let raw_preset = config
        .official
        .room
        .room_configurations
        .get("agentpreset")
        .expect("preset must exist")
        .as_ref()
        .expect("preset must not be nil");
    let agents = raw_preset
        .agents
        .0
        .as_ref()
        .expect("agents list must exist");
    assert_eq!(agents.len(), 1);
    let agent = agents[0].as_ref().expect("agent must not be nil");
    assert_eq!(agent.agent_name, "assistant");
    assert_eq!(agent.metadata, "context");
    assert_eq!(agent.restart_policy, 1);
    assert_eq!(agent.deployment, "prod");
    assert_eq!(agent.attributes.get("k").map(String::as_str), Some("v"));

    // Normalized projection
    let projected = config.server.room.presets.get("agentpreset").unwrap();
    let projected = projected.as_ref().unwrap();
    let dispatches = projected.agent_dispatches.as_ref().expect("must project");
    assert_eq!(dispatches.len(), 1);
    assert_eq!(dispatches[0].agent_name, "assistant");
    assert_eq!(dispatches[0].metadata, "context");
    assert_eq!(dispatches[0].restart_policy, 1);
    assert_eq!(dispatches[0].deployment, "prod");
    assert_eq!(
        dispatches[0].attributes.get("k").map(String::as_str),
        Some("v")
    );
}

// ---------------------------------------------------------------------------
// limit.max_data_track_custom_encoding_length
// ---------------------------------------------------------------------------

#[test]
fn limit_max_data_track_custom_encoding_length_yaml_cli_env_parse_and_project() {
    // Default: 32 (matching Go DefaultConfig.Limit.MaxDataTrackCustomEncodingLength)
    let config = parse(&[], &Env::default());
    assert_eq!(
        config.official.limit.max_data_track_custom_encoding_length,
        32
    );
    assert_eq!(
        config.server.limits.max_data_track_custom_encoding_length,
        32
    );

    // YAML
    let yaml = "limit:\n  max_data_track_custom_encoding_length: 64\n";
    let config = parse(&["--config-body", yaml], &Env::default());
    assert_eq!(
        config.server.limits.max_data_track_custom_encoding_length,
        64
    );

    // CLI
    let config = parse(
        &["--limit.max_data_track_custom_encoding_length=128"],
        &Env::default(),
    );
    assert_eq!(
        config.server.limits.max_data_track_custom_encoding_length,
        128
    );

    // ENV
    let env = Env::from_pairs([("LIVEKIT_LIMIT_MAX_DATA_TRACK_CUSTOM_ENCODING_LENGTH", "256")]);
    let config = parse(&[], &env);
    assert_eq!(
        config.server.limits.max_data_track_custom_encoding_length,
        256
    );

    // RoomPolicy projection
    let policy = rtc_core::config::RoomPolicy::from(&config.server);
    assert_eq!(policy.max_data_track_custom_encoding_length, 256);
}

// ---------------------------------------------------------------------------
// webtransport (port, tls_cert_file, tls_key_file)
// ---------------------------------------------------------------------------

#[test]
fn webtransport_yaml_cli_env_parse_and_project() {
    // Defaults
    let config = parse(&[], &Env::default());
    assert_eq!(config.official.webtransport.port, 0);
    assert_eq!(config.official.webtransport.tls_cert_file, "");
    assert_eq!(config.official.webtransport.tls_key_file, "");

    // YAML
    let config = parse(
        &[
            "--config-body",
            "webtransport:\n  port: 4443\n  tls_cert_file: /cert.pem\n  tls_key_file: /key.pem\n",
        ],
        &Env::default(),
    );
    assert_eq!(config.official.webtransport.port, 4443);
    assert_eq!(config.official.webtransport.tls_cert_file, "/cert.pem");
    assert_eq!(config.official.webtransport.tls_key_file, "/key.pem");

    // CLI
    let config = parse(
        &[
            "--webtransport.port=4443",
            "--webtransport.tls_cert_file=/c.pem",
            "--webtransport.tls_key_file=/k.pem",
        ],
        &Env::default(),
    );
    assert_eq!(config.official.webtransport.port, 4443);
    assert_eq!(config.official.webtransport.tls_cert_file, "/c.pem");
    assert_eq!(config.official.webtransport.tls_key_file, "/k.pem");

    // ENV
    let env = Env::from_pairs([
        ("LIVEKIT_WEBTRANSPORT_PORT", "4443"),
        ("LIVEKIT_WEBTRANSPORT_TLS_CERT_FILE", "/e.pem"),
        ("LIVEKIT_WEBTRANSPORT_TLS_KEY_FILE", "/f.pem"),
    ]);
    let config = parse(&[], &env);
    assert_eq!(config.official.webtransport.port, 4443);
    assert_eq!(config.official.webtransport.tls_cert_file, "/e.pem");
    assert_eq!(config.official.webtransport.tls_key_file, "/f.pem");
}

// ---------------------------------------------------------------------------
// rtc.udp_port (start/end)
// ---------------------------------------------------------------------------

#[test]
fn rtc_udp_port_yaml_parse() {
    // YAML scalar (single port: start=port, end=0)
    let config = parse(
        &["--config-body", "rtc:\n  udp_port: 10000\n"],
        &Env::default(),
    );
    assert_eq!(config.official.rtc.udp_port.start, 10000);
    assert_eq!(config.official.rtc.udp_port.end, 0);

    // YAML range
    let config = parse(
        &["--config-body", "rtc:\n  udp_port: \"10000-20000\"\n"],
        &Env::default(),
    );
    assert_eq!(config.official.rtc.udp_port.start, 10000);
    assert_eq!(config.official.rtc.udp_port.end, 20000);
}

// ---------------------------------------------------------------------------
// rtc.interfaces (includes/excludes)
// ---------------------------------------------------------------------------

#[test]
fn rtc_interfaces_yaml_parse() {
    // YAML
    let config = parse(
        &[
            "--config-body",
            "rtc:\n  interfaces:\n    includes:\n      - eth0\n      - wlan0\n    excludes:\n      - docker0\n",
        ],
        &Env::default(),
    );
    assert_eq!(
        config.official.rtc.interfaces.includes,
        vec!["eth0", "wlan0"]
    );
    assert_eq!(config.official.rtc.interfaces.excludes, vec!["docker0"]);
}

// ---------------------------------------------------------------------------
// rtc.ips (includes/excludes)
// ---------------------------------------------------------------------------

#[test]
fn rtc_ips_yaml_parse() {
    // YAML
    let config = parse(
        &[
            "--config-body",
            "rtc:\n  ips:\n    includes:\n      - 10.0.0.0/8\n    excludes:\n      - 192.168.0.0/16\n",
        ],
        &Env::default(),
    );
    assert_eq!(config.official.rtc.ips.includes, vec!["10.0.0.0/8"]);
    assert_eq!(config.official.rtc.ips.excludes, vec!["192.168.0.0/16"]);
}

// ---------------------------------------------------------------------------
// rtc.enable_loopback_candidate
// ---------------------------------------------------------------------------

#[test]
fn rtc_enable_loopback_candidate_yaml_cli_env_parse() {
    // Default
    let config = parse(&[], &Env::default());
    assert!(!config.official.rtc.enable_loopback_candidate);

    // YAML
    let config = parse(
        &["--config-body", "rtc:\n  enable_loopback_candidate: true\n"],
        &Env::default(),
    );
    assert!(config.official.rtc.enable_loopback_candidate);

    // CLI
    let config = parse(&["--rtc.enable_loopback_candidate=true"], &Env::default());
    assert!(config.official.rtc.enable_loopback_candidate);

    // ENV
    let env = Env::from_pairs([("LIVEKIT_RTC_ENABLE_LOOPBACK_CANDIDATE", "true")]);
    let config = parse(&[], &env);
    assert!(config.official.rtc.enable_loopback_candidate);
}

// ---------------------------------------------------------------------------
// ingress (rtmp_base_url, whip_base_url)
// ---------------------------------------------------------------------------

#[test]
fn ingress_rtmp_whip_base_url_yaml_cli_env_parse() {
    // YAML
    let config = parse(
        &[
            "--config-body",
            "ingress:\n  rtmp_base_url: rtmp://live.example.com\n  whip_base_url: https://whip.example.com\n",
        ],
        &Env::default(),
    );
    assert_eq!(
        config.official.ingress.rtmp_base_url,
        "rtmp://live.example.com"
    );
    assert_eq!(
        config.official.ingress.whip_base_url,
        "https://whip.example.com"
    );

    // CLI
    let config = parse(
        &[
            "--ingress.rtmp_base_url=rtmp://x.com",
            "--ingress.whip_base_url=https://w.com",
        ],
        &Env::default(),
    );
    assert_eq!(config.official.ingress.rtmp_base_url, "rtmp://x.com");
    assert_eq!(config.official.ingress.whip_base_url, "https://w.com");

    // ENV
    let env = Env::from_pairs([
        ("LIVEKIT_INGRESS_RTMP_BASE_URL", "rtmp://e.com"),
        ("LIVEKIT_INGRESS_WHIP_BASE_URL", "https://e.com"),
    ]);
    let config = parse(&[], &env);
    assert_eq!(config.official.ingress.rtmp_base_url, "rtmp://e.com");
    assert_eq!(config.official.ingress.whip_base_url, "https://e.com");
}

// ---------------------------------------------------------------------------
// limit.signal_message_size_limit
// ---------------------------------------------------------------------------

#[test]
fn limit_signal_message_size_limit_yaml_cli_env_parse() {
    // YAML
    let config = parse(
        &[
            "--config-body",
            "limit:\n  signal_message_size_limit: 8192\n",
        ],
        &Env::default(),
    );
    assert_eq!(config.official.limit.signal_message_size_limit, 8192);

    // CLI
    let config = parse(
        &["--limit.signal_message_size_limit=16384"],
        &Env::default(),
    );
    assert_eq!(config.official.limit.signal_message_size_limit, 16384);

    // ENV
    let env = Env::from_pairs([("LIVEKIT_LIMIT_SIGNAL_MESSAGE_SIZE_LIMIT", "32768")]);
    let config = parse(&[], &env);
    assert_eq!(config.official.limit.signal_message_size_limit, 32768);
}

// ---------------------------------------------------------------------------
// node_selector (kind, sort_by, algorithm, cpu_load_limit, sysload_limit, regions)
// ---------------------------------------------------------------------------

#[test]
fn node_selector_yaml_cli_env_parse() {
    // Defaults
    let config = parse(&[], &Env::default());
    assert_eq!(config.official.node_selector.kind, "any");
    assert_eq!(config.official.node_selector.sort_by, "random");
    assert_eq!(config.official.node_selector.algorithm, "lowest");
    assert!((config.official.node_selector.cpu_load_limit - 0.9).abs() < 0.01);
    assert!((config.official.node_selector.sysload_limit - 0.9).abs() < 0.01);
    assert!(config.official.node_selector.regions.is_empty());

    // YAML
    let config = parse(
        &[
            "--config-body",
            "node_selector:\n  kind: cpuload\n  sort_by: cpu\n  algorithm: packed\n  cpu_load_limit: 0.8\n  sysload_limit: 0.7\n  regions:\n    - name: us-west\n      lat: 37.7749\n      lon: -122.4194\n",
        ],
        &Env::default(),
    );
    assert_eq!(config.official.node_selector.kind, "cpuload");
    assert_eq!(config.official.node_selector.sort_by, "cpu");
    assert_eq!(config.official.node_selector.algorithm, "packed");
    assert!((config.official.node_selector.cpu_load_limit - 0.8).abs() < 0.01);
    assert!((config.official.node_selector.sysload_limit - 0.7).abs() < 0.01);
    assert_eq!(config.official.node_selector.regions.len(), 1);
    assert_eq!(config.official.node_selector.regions[0].name, "us-west");
    assert!((config.official.node_selector.regions[0].lat - 37.7749).abs() < 0.001);
    assert!((config.official.node_selector.regions[0].lon - (-122.4194)).abs() < 0.001);

    // CLI
    let config = parse(
        &[
            "--node_selector.kind=random",
            "--node_selector.sort_by=random",
            "--node_selector.algorithm=lowest",
            "--node_selector.cpu_load_limit=0.5",
            "--node_selector.sysload_limit=0.6",
        ],
        &Env::default(),
    );
    assert_eq!(config.official.node_selector.kind, "random");
    assert_eq!(config.official.node_selector.sort_by, "random");
    assert_eq!(config.official.node_selector.algorithm, "lowest");
    assert!((config.official.node_selector.cpu_load_limit - 0.5).abs() < 0.01);
    assert!((config.official.node_selector.sysload_limit - 0.6).abs() < 0.01);
}

// ---------------------------------------------------------------------------
// psrpc (max_attempts, timeout, backoff, buffer_size, compression)
// ---------------------------------------------------------------------------

#[test]
fn psrpc_yaml_cli_env_parse() {
    // Defaults
    let config = parse(&[], &Env::default());
    assert_eq!(config.official.psrpc.max_attempts, 3);
    assert_eq!(config.official.psrpc.buffer_size, 1000);
    assert_eq!(config.official.psrpc.compression.quality, 0);
    assert_eq!(config.official.psrpc.compression.threshold, 1024);
    assert_eq!(config.official.psrpc.compression.max_decompressed_size, 0);

    // YAML
    let config = parse(
        &[
            "--config-body",
            "psrpc:\n  max_attempts: 5\n  buffer_size: 2000\n  compression:\n    quality: 3\n    threshold: 4096\n    max_decompressed_size: 1048576\n",
        ],
        &Env::default(),
    );
    assert_eq!(config.official.psrpc.max_attempts, 5);
    assert_eq!(config.official.psrpc.buffer_size, 2000);
    assert_eq!(config.official.psrpc.compression.quality, 3);
    assert_eq!(config.official.psrpc.compression.threshold, 4096);
    assert_eq!(
        config.official.psrpc.compression.max_decompressed_size,
        1048576
    );

    // CLI
    let config = parse(
        &[
            "--psrpc.max_attempts=10",
            "--psrpc.buffer_size=5000",
            "--psrpc.compression.quality=6",
            "--psrpc.compression.threshold=8192",
            "--psrpc.compression.max_decompressed_size=2097152",
        ],
        &Env::default(),
    );
    assert_eq!(config.official.psrpc.max_attempts, 10);
    assert_eq!(config.official.psrpc.buffer_size, 5000);
    assert_eq!(config.official.psrpc.compression.quality, 6);
    assert_eq!(config.official.psrpc.compression.threshold, 8192);
    assert_eq!(
        config.official.psrpc.compression.max_decompressed_size,
        2097152
    );
}

// ---------------------------------------------------------------------------
// rtc.reconnect_on_* and rtc.datachannel_data_track_target_latency
// ---------------------------------------------------------------------------

#[test]
fn rtc_reconnect_and_datachannel_latency_yaml_parse() {
    // Defaults
    let config = parse(&[], &Env::default());
    assert_eq!(config.official.rtc.reconnect_on_publication_error, None);
    assert_eq!(config.official.rtc.reconnect_on_subscription_error, None);
    assert_eq!(config.official.rtc.reconnect_on_data_channel_error, None);

    // YAML
    let config = parse(
        &[
            "--config-body",
            "rtc:\n  reconnect_on_publication_error: true\n  reconnect_on_subscription_error: false\n  reconnect_on_data_channel_error: true\n  datachannel_data_track_target_latency: 200ms\n",
        ],
        &Env::default(),
    );
    assert_eq!(
        config.official.rtc.reconnect_on_publication_error,
        Some(true)
    );
    assert_eq!(
        config.official.rtc.reconnect_on_subscription_error,
        Some(false)
    );
    assert_eq!(
        config.official.rtc.reconnect_on_data_channel_error,
        Some(true)
    );
}

// ---------------------------------------------------------------------------
// room.create_room_timeout and room.create_room_attempts
// ---------------------------------------------------------------------------

#[test]
fn room_create_timeout_and_attempts_yaml_cli_parse() {
    // Defaults
    let config = parse(&[], &Env::default());
    assert_eq!(
        config.official.room.create_room_timeout,
        lk_config::GoDuration::from_secs(10)
    );
    assert_eq!(config.official.room.create_room_attempts, 3);

    // YAML
    let config = parse(
        &[
            "--config-body",
            "room:\n  create_room_timeout: 30s\n  create_room_attempts: 5\n",
        ],
        &Env::default(),
    );
    assert_eq!(
        config.official.room.create_room_timeout,
        lk_config::GoDuration::from_secs(30)
    );
    assert_eq!(config.official.room.create_room_attempts, 5);

    // CLI
    let config = parse(
        &[
            "--room.create_room_timeout=1m",
            "--room.create_room_attempts=10",
        ],
        &Env::default(),
    );
    assert_eq!(
        config.official.room.create_room_timeout,
        lk_config::GoDuration::from_secs(60)
    );
    assert_eq!(config.official.room.create_room_attempts, 10);
}

// ---------------------------------------------------------------------------
// limit.max_room_name_length / max_participant_name_length / max_participant_identity_length
// ---------------------------------------------------------------------------

#[test]
fn limit_name_lengths_yaml_cli_parse() {
    // Defaults
    let config = parse(&[], &Env::default());
    assert_eq!(config.official.limit.max_room_name_length, 256);
    assert_eq!(config.official.limit.max_participant_identity_length, 256);

    // YAML
    let config = parse(
        &[
            "--config-body",
            "limit:\n  max_room_name_length: 512\n  max_participant_identity_length: 128\n  max_participant_name_length: 64\n",
        ],
        &Env::default(),
    );
    assert_eq!(config.official.limit.max_room_name_length, 512);
    assert_eq!(config.official.limit.max_participant_identity_length, 128);
    assert_eq!(config.official.limit.max_participant_name_length, 64);

    // CLI
    let config = parse(
        &[
            "--limit.max_room_name_length=1024",
            "--limit.max_participant_identity_length=200",
            "--limit.max_participant_name_length=100",
        ],
        &Env::default(),
    );
    assert_eq!(config.official.limit.max_room_name_length, 1024);
    assert_eq!(config.official.limit.max_participant_identity_length, 200);
    assert_eq!(config.official.limit.max_participant_name_length, 100);
}

#[test]
fn limit_max_attributes_size_yaml_cli_env_parse_and_project() {
    // Default 64*1024 (Go DefaultConfig.Limit.MaxAttributesSize). The generated
    // integer flag --limit.max_attributes_size projects into this same typed leaf.
    let config = parse(&[], &Env::default());
    assert_eq!(config.official.limit.max_attributes_size, 65536);
    assert_eq!(config.server.limits.max_attributes_size, Some(65536));

    // YAML
    let config = parse(
        &["--config-body", "limit:\n  max_attributes_size: 10\n"],
        &Env::default(),
    );
    assert_eq!(config.official.limit.max_attributes_size, 10);
    assert_eq!(config.server.limits.max_attributes_size, Some(10));

    // CLI: the generated integer flag input path.
    let config = parse(&["--limit.max_attributes_size=10"], &Env::default());
    assert_eq!(config.official.limit.max_attributes_size, 10);
    assert_eq!(config.server.limits.max_attributes_size, Some(10));

    // ENV
    let env = Env::from_pairs([("LIVEKIT_LIMIT_MAX_ATTRIBUTES_SIZE", "256")]);
    let config = parse(&[], &env);
    assert_eq!(config.official.limit.max_attributes_size, 256);
    assert_eq!(config.server.limits.max_attributes_size, Some(256));

    // CLI overrides YAML; zero means unlimited (None), matching Go CheckAttributesSize.
    let config = parse(
        &[
            "--config-body",
            "limit:\n  max_attributes_size: 10\n",
            "--limit.max_attributes_size=0",
        ],
        &Env::default(),
    );
    assert_eq!(config.official.limit.max_attributes_size, 0);
    assert_eq!(config.server.limits.max_attributes_size, None);
}

// ---------------------------------------------------------------------------
// logging sample leaves: YAML and ENV projection (CLI is covered separately)
// ---------------------------------------------------------------------------

#[test]
fn logging_sample_yaml_env_projection() {
    // Defaults: sampling off, every window zero. No input may fabricate a sampler.
    let config = parse(&[], &Env::default());
    assert!(!config.official.logging.sample);
    assert_eq!(config.official.logging.sample_initial, 0);
    assert_eq!(config.official.logging.sample_interval, 0);
    assert_eq!(config.official.logging.item_sample_seconds, 0);
    assert_eq!(config.official.logging.item_sample_initial, 0);
    assert_eq!(config.official.logging.item_sample_interval, 0);
    assert!(!config.server.logging.sample);
    assert_eq!(config.server.logging.item_sample_seconds, 0);

    // YAML: each leaf must reach the official tree AND survive normalization.
    let config = parse(
        &[
            "--config-body",
            "logging:\n  sample: true\n  sample_initial: 50\n  sample_interval: 30\n  item_sample_initial: 10\n  item_sample_interval: 60\n  item_sample_seconds: 120\n",
        ],
        &Env::default(),
    );
    assert!(config.official.logging.sample);
    assert_eq!(config.official.logging.sample_initial, 50);
    assert_eq!(config.official.logging.sample_interval, 30);
    assert_eq!(config.official.logging.item_sample_initial, 10);
    assert_eq!(config.official.logging.item_sample_interval, 60);
    assert_eq!(config.official.logging.item_sample_seconds, 120);
    let effective = &config.server.logging;
    assert!(effective.sample);
    assert_eq!(effective.sample_initial, 50);
    assert_eq!(effective.sample_interval, 30);
    assert_eq!(effective.item_sample_initial, 10);
    assert_eq!(effective.item_sample_interval, 60);
    assert_eq!(effective.item_sample_seconds, 120);

    // ENV: the same leaves via LIVEKIT_LOGGING_* must project identically.
    let env = Env::from_pairs([
        ("LIVEKIT_LOGGING_SAMPLE", "true"),
        ("LIVEKIT_LOGGING_SAMPLE_INITIAL", "50"),
        ("LIVEKIT_LOGGING_SAMPLE_INTERVAL", "30"),
        ("LIVEKIT_LOGGING_ITEM_SAMPLE_INITIAL", "10"),
        ("LIVEKIT_LOGGING_ITEM_SAMPLE_INTERVAL", "60"),
        ("LIVEKIT_LOGGING_ITEM_SAMPLE_SECONDS", "120"),
    ]);
    let config = parse(&[], &env);
    assert!(config.official.logging.sample);
    assert_eq!(config.official.logging.sample_initial, 50);
    assert_eq!(config.official.logging.sample_interval, 30);
    assert_eq!(config.official.logging.item_sample_initial, 10);
    assert_eq!(config.official.logging.item_sample_interval, 60);
    assert_eq!(config.official.logging.item_sample_seconds, 120);
    let effective = &config.server.logging;
    assert!(effective.sample);
    assert_eq!(effective.sample_initial, 50);
    assert_eq!(effective.sample_interval, 30);
    assert_eq!(effective.item_sample_initial, 10);
    assert_eq!(effective.item_sample_interval, 60);
    assert_eq!(effective.item_sample_seconds, 120);

    // Signed windows are preserved, not clamped: the fixed Go sampler accepts
    // negative item_sample_initial and the room opt-in is independent of sample.
    let config = parse(
        &[
            "--config-body",
            "logging:\n  sample: false\n  item_sample_seconds: -1\n  item_sample_initial: -3\n",
        ],
        &Env::default(),
    );
    assert!(!config.server.logging.sample);
    assert_eq!(config.server.logging.item_sample_seconds, -1);
    assert_eq!(config.server.logging.item_sample_initial, -3);
}

// ---------------------------------------------------------------------------
// generated_flags CLI input path coverage for flags matching VERIFIED YAML
// ---------------------------------------------------------------------------

#[test]
fn generated_flags_cli_coverage_audio_logging_redis_room_rtc() {
    // audio.* CLI flags (active_level: u8, min_percentile: u8, smooth_intervals: u32)
    let config = parse(
        &[
            "--audio.active_level=15",
            "--audio.min_percentile=85",
            "--audio.smooth_intervals=5",
            "--audio.active_red_encoding=true",
            "--audio.enable_loss_proxying=true",
            "--audio.update_interval=200",
        ],
        &Env::default(),
    );
    assert_eq!(config.official.audio.active_level, 15);
    assert_eq!(config.official.audio.min_percentile, 85);
    assert_eq!(config.official.audio.smooth_intervals, 5);
    assert!(config.official.audio.active_red_encoding);
    assert!(config.official.audio.enable_loss_proxying);
    assert_eq!(config.official.audio.update_interval, 200);

    // logging.* CLI flags (sample: bool, intervals: i64, pion_level: String)
    let config = parse(
        &[
            "--logging.sample=true",
            "--logging.sample_initial=50",
            "--logging.sample_interval=30",
            "--logging.item_sample_initial=10",
            "--logging.item_sample_interval=60",
            "--logging.item_sample_seconds=120",
            "--logging.pion_level=warn",
        ],
        &Env::default(),
    );
    assert!(config.official.logging.sample);
    assert_eq!(config.official.logging.sample_initial, 50);
    assert_eq!(config.official.logging.sample_interval, 30);
    assert_eq!(config.official.logging.item_sample_initial, 10);
    assert_eq!(config.official.logging.item_sample_interval, 60);
    assert_eq!(config.official.logging.item_sample_seconds, 120);
    assert_eq!(config.official.logging.pion_level, "warn");
    // A parse-only assertion would miss dropping fields during normalization.
    let effective = &config.server.logging;
    assert!(effective.sample);
    assert_eq!(effective.sample_initial, 50);
    assert_eq!(effective.sample_interval, 30);
    assert_eq!(effective.item_sample_initial, 10);
    assert_eq!(effective.item_sample_interval, 60);
    assert_eq!(effective.item_sample_seconds, 120);

    // redis.* CLI flags (string/i64/bool fields)
    let config = parse(
        &[
            "--redis.address=localhost:6379",
            "--redis.db=3",
            "--redis.username=testuser",
            "--redis.password=testpass",
            "--redis.pool_size=25",
            "--redis.dial_timeout=10",
            "--redis.read_timeout=5",
            "--redis.write_timeout=5",
            "--redis.use_tls=true",
            "--redis.max_redirects=3",
        ],
        &Env::default(),
    );
    assert_eq!(config.official.redis.address, "localhost:6379");
    assert_eq!(config.official.redis.db, 3);
    assert_eq!(config.official.redis.username, "testuser");
    assert_eq!(config.official.redis.password, "testpass");
    assert_eq!(config.official.redis.pool_size, 25);
    assert_eq!(config.official.redis.dial_timeout, 10);
    assert_eq!(config.official.redis.read_timeout, 5);
    assert_eq!(config.official.redis.write_timeout, 5);
    assert!(config.official.redis.use_tls);
    assert_eq!(config.official.redis.max_redirects, Some(3));

    // redis.pool_timeout is GoDuration
    let config = parse(&["--redis.pool_timeout=5s"], &Env::default());
    assert_eq!(
        config.official.redis.pool_timeout.to_std(),
        Duration::from_secs(5)
    );

    // room.* CLI flags
    let config = parse(
        &[
            "--room.max_metadata_size=1024",
            "--room.max_room_name_length=512",
            "--room.max_participant_identity_length=256",
            "--room.playout_delay.enabled=true",
            "--room.playout_delay.min=100",
            "--room.playout_delay.max=2000",
        ],
        &Env::default(),
    );
    assert_eq!(config.official.room.max_metadata_size, 1024);
    assert_eq!(config.official.room.max_room_name_length, 512);
    assert_eq!(config.official.room.max_participant_identity_length, 256);
    assert!(config.official.room.playout_delay.enabled);
    assert_eq!(config.official.room.playout_delay.min, 100);
    assert_eq!(config.official.room.playout_delay.max, 2000);
}

// ---------------------------------------------------------------------------
// base_flags CLI input path coverage
// ---------------------------------------------------------------------------

#[test]
fn base_flags_cli_parse_and_precedence() {
    // --region sets config.official.region
    let config = parse(&["--region=us-west-2"], &Env::default());
    assert_eq!(config.official.region, "us-west-2");

    // LIVEKIT_REGION env var (used when CLI flag absent)
    let env = Env::from_pairs([("LIVEKIT_REGION", "eu-west-1")]);
    let config = parse(&[], &env);
    assert_eq!(config.official.region, "eu-west-1");

    // CLI beats env
    let env = Env::from_pairs([("LIVEKIT_REGION", "eu-west-1")]);
    let config = parse(&["--region=ap-south-1"], &env);
    assert_eq!(config.official.region, "ap-south-1");

    // --redis-host sets config.official.redis.address
    let config = parse(&["--redis-host=redis.example.com:6379"], &Env::default());
    assert_eq!(config.official.redis.address, "redis.example.com:6379");

    // REDIS_HOST env var
    let env = Env::from_pairs([("REDIS_HOST", "redis-env:6379")]);
    let config = parse(&[], &env);
    assert_eq!(config.official.redis.address, "redis-env:6379");

    // --redis-password sets config.official.redis.password
    let config = parse(&["--redis-password=secret123"], &Env::default());
    assert_eq!(config.official.redis.password, "secret123");

    // REDIS_PASSWORD env var
    let env = Env::from_pairs([("REDIS_PASSWORD", "env-secret")]);
    let config = parse(&[], &env);
    assert_eq!(config.official.redis.password, "env-secret");

    // --key-file requires actual file; tested via dev-mode placeholder keys.
    // Verify the flag is accepted by the CLI parser (base flag mapping).

    // --bind sets config.official.bind_addresses
    let config = parse(&["--bind=10.0.0.1", "--bind=10.0.0.2"], &Env::default());
    assert_eq!(
        config.official.bind_addresses,
        Some(vec!["10.0.0.1".to_string(), "10.0.0.2".to_string()])
    );

    // --bind with comma separation
    let config = parse(&["--bind=10.0.0.1,10.0.0.2"], &Env::default());
    assert_eq!(
        config.official.bind_addresses,
        Some(vec!["10.0.0.1".to_string(), "10.0.0.2".to_string()])
    );

    // --keys sets config.official.keys (format: "key: secret")
    let config = parse(&["--keys=testkey: testsecret"], &Env::default());
    assert_eq!(config.official.keys.0.len(), 1);
    assert_eq!(config.official.keys.0[0].0, "testkey");
    assert_eq!(config.official.keys.0[0].1, "testsecret");

    // LIVEKIT_KEYS env var
    let env = Env::from_pairs([("LIVEKIT_KEYS", "envkey: envsecret")]);
    let config = parse(&[], &env);
    assert_eq!(config.official.keys.0.len(), 1);
    assert_eq!(config.official.keys.0[0].0, "envkey");
    assert_eq!(config.official.keys.0[0].1, "envsecret");

    // --turn-cert and --turn-key set config.official.turn.cert_file/key_file
    let config = parse(
        &[
            "--turn-cert=/path/to/cert.pem",
            "--turn-key=/path/to/key.pem",
        ],
        &Env::default(),
    );
    assert_eq!(config.official.turn.cert_file, "/path/to/cert.pem");
    assert_eq!(config.official.turn.key_file, "/path/to/key.pem");

    // --disable-strict-config allows unknown YAML fields
    let config = parse(
        &[
            "--disable-strict-config",
            "--config-body",
            "unknown_field: value\n",
        ],
        &Env::default(),
    );
    // Should not error with strict disabled
    assert!(config.official.development); // --dev is also set

    // Without --disable-strict-config, unknown YAML fields error
    let info = ProgramInfo {
        binary_name: "livecloud".into(),
        product_version: "fixture".into(),
        compat_version: "1.13.10",
    };
    let argv: Vec<OsString> = [
        "livecloud",
        "--dev",
        "--config-body",
        "totally_unknown_field: 42\n",
    ]
    .into_iter()
    .map(OsString::from)
    .collect();
    let result = parse_with_probe(argv, &Env::default(), &info, &NoNetwork);
    assert!(result.is_err(), "strict config must reject unknown fields");
}

// ---------------------------------------------------------------------------
// Broad YAML parsing coverage for remaining OPEN_AUDIT paths
// ---------------------------------------------------------------------------

#[test]
fn yaml_broad_coverage_port_turn_webhook_rtc_signal_agents_api() {
    let yaml = "\
port: 7881
development: true
log_level: debug
enable_data_tracks: false
region: test-region
redis:
  address: redis:6379
turn:
  enabled: true
  udp_port: 3478
  relay_range_start: 10000
  relay_range_end: 20000
  external_tls: true
  ttl_seconds: 600
  per_user_relay_allocation_limit: 24
  allow_restricted_peer_cidrs:
    - 10.0.0.0/8
  deny_peer_cidrs:
    - 192.168.0.0/16
webhook:
  urls:
    - http://example.com/hook
  api_key: webhook-secret
  url_notifier:
    num_workers: 20
    queue_size: 500
  resource_url_notifier:
    max_age: 1m
    max_depth: 100
  filter_params:
    include_events:
      - room_started
    exclude_events:
      - participant_joined
rtc:
  tcp_port: 9000
  port_range_start: 10000
  port_range_end: 20000
  node_ip:
    ipv4: 127.0.0.1
  use_external_ip: false
  advertise_internal_ip: true
  use_ice_lite: true
  packet_buffer_size: 1000
  packet_buffer_size_video: 1000
  packet_buffer_size_audio: 400
  allow_tcp_fallback: true
  tcp_fallback_rtt_threshold: 5
  data_channel_max_buffered_amount: 65536
  datachannel_slow_threshold: 1024
  enable_rtp_stream_restart_detection: true
  congestion_control:
    enabled: true
    allow_pause: true
    send_side_bwe_pacer: pass-through
  pli_throttle:
    low_quality: 200ms
    mid_quality: 500ms
    high_quality: 1s
  forward_stats:
    summary_interval: 10s
    report_interval: 30s
    report_window: 1m
  stun_servers:
    - stun:stun1.example.com:3478
  batch_io:
    batch_size: 64
    max_flush_interval: 500us
video:
  dynacast_pause_delay: 10s
  codec_regression_threshold: 3
  stream_tracker_manager:
    video:
      stream_tracker_type: packet
    screenshare:
      stream_tracker_type: frame
signal_relay:
  retry_timeout: 10s
  min_retry_interval: 1s
  max_retry_interval: 8s
  stream_buffer_size: 2000
  connect_attempts: 5
agents:
  enable_user_data_recording: true
  enable_user_data_redaction: true
  target_load: 0.85
  endpoints:
    disabled: true
    max_streams: 100
    max_tunnels: 50
    tunnel_drain_timeout: 30s
api:
  execution_timeout: 5s
  check_interval: 200ms
  max_check_interval: 600s
  enable_psrpc_for_get_list_participants: true
trace:
  jaeger_url: http://jaeger:14268/api/traces
node_stats:
  stats_update_interval: 5s
  stats_rate_measurement_intervals: [10s, 30s]
  stats_max_delay: 60s
metric:
  timestamper_config:
    one_way_delay_estimator_min_interval: 100ms
    one_way_delay_estimator_max_batch: 50
  collector:
    sampling_interval_ms: 500
    batch_interval_ms: 2000
  reporter:
    reporting_interval_ms: 5000
limit:
  num_tracks: 100
  bytes_per_sec: 1048576.0
  agent_signal_message_size_limit: 4194304
ingress:
  enable_udp_url_pull: true
";
    let config = parse(&["--config-body", yaml], &Env::default());

    // Top-level fields
    assert_eq!(config.official.port, 7881);
    assert!(config.official.development);
    assert_eq!(config.official.log_level, "debug");
    assert!(!config.official.enable_data_tracks);
    assert_eq!(config.official.region, "test-region");

    // Redis
    assert_eq!(config.official.redis.address, "redis:6379");

    // TURN
    assert!(config.official.turn.enabled);
    assert_eq!(config.official.turn.udp_port, 3478);
    assert_eq!(config.official.turn.relay_range_start, 10000);
    assert_eq!(config.official.turn.relay_range_end, 20000);
    assert!(config.official.turn.external_tls);
    assert_eq!(config.official.turn.ttl_seconds, 600);
    assert_eq!(config.official.turn.per_user_relay_allocation_limit, 24);
    assert_eq!(
        config.official.turn.allow_restricted_peer_cidrs,
        vec!["10.0.0.0/8"]
    );
    assert_eq!(config.official.turn.deny_peer_cidrs, vec!["192.168.0.0/16"]);

    // Webhook
    assert_eq!(
        config.official.webhook.urls,
        vec!["http://example.com/hook"]
    );
    assert_eq!(config.official.webhook.api_key, "webhook-secret");
    assert_eq!(config.official.webhook.url_notifier.num_workers, 20);
    assert_eq!(config.official.webhook.url_notifier.queue_size, 500);
    assert_eq!(
        config.official.webhook.resource_url_notifier.max_age,
        GoDuration::from_secs(60)
    );
    assert_eq!(config.official.webhook.resource_url_notifier.max_depth, 100);
    assert_eq!(
        config.official.webhook.filter_params.include_events,
        vec!["room_started"]
    );
    assert_eq!(
        config.official.webhook.filter_params.exclude_events,
        vec!["participant_joined"]
    );

    // RTC
    assert_eq!(config.official.rtc.tcp_port, 9000);
    assert_eq!(config.official.rtc.port_range_start, 10000);
    assert_eq!(config.official.rtc.port_range_end, 20000);
    assert!(config.official.rtc.advertise_internal_ip);
    assert!(config.official.rtc.use_ice_lite);
    assert_eq!(config.official.rtc.packet_buffer_size, 1000);
    assert_eq!(config.official.rtc.packet_buffer_size_video, 1000);
    assert_eq!(config.official.rtc.packet_buffer_size_audio, 400);
    assert_eq!(config.official.rtc.allow_tcp_fallback, Some(true));
    assert_eq!(config.official.rtc.tcp_fallback_rtt_threshold, 5);
    assert_eq!(config.official.rtc.data_channel_max_buffered_amount, 65536);
    assert_eq!(config.official.rtc.datachannel_slow_threshold, 1024);
    assert!(config.official.rtc.enable_rtp_stream_restart_detection);
    assert!(config.official.rtc.congestion_control.enabled);
    assert!(config.official.rtc.congestion_control.allow_pause);
    assert_eq!(
        config.official.rtc.stun_servers,
        vec!["stun:stun1.example.com:3478"]
    );

    // Video
    assert_eq!(
        config.official.video.dynacast_pause_delay,
        GoDuration::from_secs(10)
    );
    assert_eq!(config.official.video.codec_regression_threshold, 3);
    assert_eq!(
        config
            .official
            .video
            .stream_tracker_manager
            .video
            .stream_tracker_type,
        "packet"
    );
    assert_eq!(
        config
            .official
            .video
            .stream_tracker_manager
            .screenshare
            .stream_tracker_type,
        "frame"
    );

    // Signal relay
    assert_eq!(
        config.official.signal_relay.retry_timeout,
        GoDuration::from_secs(10)
    );
    assert_eq!(
        config.official.signal_relay.min_retry_interval,
        GoDuration::from_millis(1000)
    );
    assert_eq!(
        config.official.signal_relay.max_retry_interval,
        GoDuration::from_secs(8)
    );
    assert_eq!(config.official.signal_relay.stream_buffer_size, 2000);
    assert_eq!(config.official.signal_relay.connect_attempts, 5);

    // Agents
    assert!(config.official.agents.enable_user_data_recording);
    assert!(config.official.agents.enable_user_data_redaction);
    assert!((config.official.agents.target_load - 0.85).abs() < 0.01);
    assert!(config.official.agents.endpoints.disabled);
    assert_eq!(config.official.agents.endpoints.max_streams, 100);
    assert_eq!(config.official.agents.endpoints.max_tunnels, 50);
    assert_eq!(
        config.official.agents.endpoints.tunnel_drain_timeout,
        GoDuration::from_secs(30)
    );

    // API
    assert_eq!(
        config.official.api.execution_timeout,
        GoDuration::from_secs(5)
    );
    assert_eq!(
        config.official.api.check_interval,
        GoDuration::from_millis(200)
    );
    assert_eq!(
        config.official.api.max_check_interval,
        GoDuration::from_secs(600)
    );
    assert!(config.official.api.enable_psrpc_for_get_list_participants);

    // Trace
    assert_eq!(
        config.official.trace.jaeger_url,
        "http://jaeger:14268/api/traces"
    );

    // Node stats
    assert_eq!(
        config.official.node_stats.stats_update_interval,
        GoDuration::from_secs(5)
    );
    assert_eq!(
        config.official.node_stats.stats_rate_measurement_intervals,
        vec![GoDuration::from_secs(10), GoDuration::from_secs(30)]
    );
    assert_eq!(
        config.official.node_stats.stats_max_delay,
        GoDuration::from_secs(60)
    );

    // Metric
    assert_eq!(config.official.metric.collector.sampling_interval_ms, 500);
    assert_eq!(config.official.metric.collector.batch_interval_ms, 2000);
    assert_eq!(config.official.metric.reporter.reporting_interval_ms, 5000);

    // Limit
    assert_eq!(config.official.limit.num_tracks, 100);
    assert!((config.official.limit.bytes_per_sec - 1048576.0).abs() < 1.0);
    assert_eq!(
        config.official.limit.agent_signal_message_size_limit,
        4194304
    );

    // Ingress
    assert!(config.official.ingress.enable_udp_url_pull);
}

use lk_config::GoDuration;

// ---------------------------------------------------------------------------
// Broad generated_flags CLI coverage for remaining OPEN_AUDIT paths
// ---------------------------------------------------------------------------

#[test]
fn generated_flags_cli_broad_coverage_turn_webhook_video_signal_agents_api() {
    // port + top-level flags
    let config = parse(
        &[
            "--port=7881",
            "--development=true",
            "--log_level=debug",
            "--enable_data_tracks=false",
        ],
        &Env::default(),
    );
    assert_eq!(config.official.port, 7881);
    assert!(config.official.development);
    assert_eq!(config.official.log_level, "debug");
    assert!(!config.official.enable_data_tracks);

    // TURN generated flags
    let config = parse(
        &[
            "--turn.enabled=true",
            "--turn.udp_port=3478",
            "--turn.relay_range_start=10000",
            "--turn.relay_range_end=20000",
            "--turn.external_tls=true",
            "--turn.ttl_seconds=600",
            "--turn.per_user_relay_allocation_limit=24",
        ],
        &Env::default(),
    );
    assert!(config.official.turn.enabled);
    assert_eq!(config.official.turn.udp_port, 3478);
    assert_eq!(config.official.turn.relay_range_start, 10000);
    assert_eq!(config.official.turn.relay_range_end, 20000);
    assert!(config.official.turn.external_tls);
    assert_eq!(config.official.turn.ttl_seconds, 600);
    assert_eq!(config.official.turn.per_user_relay_allocation_limit, 24);

    // Webhook generated flags
    let config = parse(
        &[
            "--webhook.api_key=test-key",
            "--webhook.url_notifier.num_workers=20",
            "--webhook.url_notifier.queue_size=500",
            "--webhook.resource_url_notifier.max_depth=100",
        ],
        &Env::default(),
    );
    assert_eq!(config.official.webhook.api_key, "test-key");
    assert_eq!(config.official.webhook.url_notifier.num_workers, 20);
    assert_eq!(config.official.webhook.url_notifier.queue_size, 500);
    assert_eq!(config.official.webhook.resource_url_notifier.max_depth, 100);

    // Video generated flags
    let config = parse(
        &[
            "--video.codec_regression_threshold=3",
            "--video.stream_tracker_manager.video.stream_tracker_type=packet",
            "--video.stream_tracker_manager.screenshare.stream_tracker_type=frame",
        ],
        &Env::default(),
    );
    assert_eq!(config.official.video.codec_regression_threshold, 3);
    assert_eq!(
        config
            .official
            .video
            .stream_tracker_manager
            .video
            .stream_tracker_type,
        "packet"
    );
    assert_eq!(
        config
            .official
            .video
            .stream_tracker_manager
            .screenshare
            .stream_tracker_type,
        "frame"
    );

    // Signal relay generated flags
    let config = parse(
        &[
            "--signal_relay.stream_buffer_size=2000",
            "--signal_relay.connect_attempts=5",
        ],
        &Env::default(),
    );
    assert_eq!(config.official.signal_relay.stream_buffer_size, 2000);
    assert_eq!(config.official.signal_relay.connect_attempts, 5);

    // Agents generated flags
    let config = parse(
        &[
            "--agents.enable_user_data_recording=true",
            "--agents.enable_user_data_redaction=true",
            "--agents.target_load=0.85",
            "--agents.endpoints.disabled=true",
            "--agents.endpoints.max_streams=100",
            "--agents.endpoints.max_tunnels=50",
        ],
        &Env::default(),
    );
    assert!(config.official.agents.enable_user_data_recording);
    assert!(config.official.agents.enable_user_data_redaction);
    assert!((config.official.agents.target_load - 0.85).abs() < 0.01);
    assert!(config.official.agents.endpoints.disabled);
    assert_eq!(config.official.agents.endpoints.max_streams, 100);
    assert_eq!(config.official.agents.endpoints.max_tunnels, 50);

    // API generated flags
    let config = parse(
        &["--api.enable_psrpc_for_get_list_participants=true"],
        &Env::default(),
    );
    assert!(config.official.api.enable_psrpc_for_get_list_participants);

    // Trace generated flags
    let config = parse(
        &["--trace.jaeger_url=http://jaeger:14268/api/traces"],
        &Env::default(),
    );
    assert_eq!(
        config.official.trace.jaeger_url,
        "http://jaeger:14268/api/traces"
    );

    // Metric generated flags
    let config = parse(
        &[
            "--metric.collector.sampling_interval_ms=500",
            "--metric.collector.batch_interval_ms=2000",
            "--metric.reporter.reporting_interval_ms=5000",
        ],
        &Env::default(),
    );
    assert_eq!(config.official.metric.collector.sampling_interval_ms, 500);
    assert_eq!(config.official.metric.collector.batch_interval_ms, 2000);
    assert_eq!(config.official.metric.reporter.reporting_interval_ms, 5000);

    // Limit generated flags
    let config = parse(
        &[
            "--limit.num_tracks=100",
            "--limit.agent_signal_message_size_limit=4194304",
        ],
        &Env::default(),
    );
    assert_eq!(config.official.limit.num_tracks, 100);
    assert_eq!(
        config.official.limit.agent_signal_message_size_limit,
        4194304
    );

    // RTC generated flags
    let config = parse(
        &[
            "--rtc.packet_buffer_size=1000",
            "--rtc.packet_buffer_size_video=1000",
            "--rtc.packet_buffer_size_audio=400",
            "--rtc.tcp_fallback_rtt_threshold=5",
            "--rtc.data_channel_max_buffered_amount=65536",
            "--rtc.datachannel_slow_threshold=1024",
            "--rtc.enable_rtp_stream_restart_detection=true",
            "--rtc.congestion_control.send_side_bwe_pacer=pass-through",
        ],
        &Env::default(),
    );
    assert_eq!(config.official.rtc.packet_buffer_size, 1000);
    assert_eq!(config.official.rtc.packet_buffer_size_video, 1000);
    assert_eq!(config.official.rtc.packet_buffer_size_audio, 400);
    assert_eq!(config.official.rtc.tcp_fallback_rtt_threshold, 5);
    assert_eq!(config.official.rtc.data_channel_max_buffered_amount, 65536);
    assert_eq!(config.official.rtc.datachannel_slow_threshold, 1024);
    assert!(config.official.rtc.enable_rtp_stream_restart_detection);
    assert_eq!(
        config.official.rtc.congestion_control.send_side_bwe_pacer,
        "pass-through"
    );

    // Ingress generated flags
    let config = parse(&["--ingress.enable_udp_url_pull=true"], &Env::default());
    assert!(config.official.ingress.enable_udp_url_pull);

    // Room generated flags
    let config = parse(&["--room.update_batch_target_size=256"], &Env::default());
    assert_eq!(config.official.room.update_batch_target_size, 256);
}

// ---------------------------------------------------------------------------
// Additional generated_flags CLI coverage: prometheus, debug_handler, rtc
// ---------------------------------------------------------------------------

#[test]
fn generated_flags_cli_coverage_prometheus_debug_rtc_extra() {
    // Prometheus flags
    let config = parse(
        &[
            "--prometheus_port=9090",
            "--prometheus.username=admin",
            "--prometheus.password=secret",
            "--prometheus.port=9091",
        ],
        &Env::default(),
    );
    assert_eq!(config.official.prometheus_port, 9090);
    assert_eq!(config.official.prometheus.username, "admin");
    assert_eq!(config.official.prometheus.password, "secret");
    assert_eq!(config.official.prometheus.port, 9091);

    // Debug handler
    let config = parse(&["--debug_handler.port=6060"], &Env::default());
    assert_eq!(config.official.debug_handler.port, 6060);

    // RTC simple flags
    let config = parse(
        &[
            "--rtc.tcp_port=9000",
            "--rtc.port_range_start=10000",
            "--rtc.port_range_end=20000",
            "--rtc.use_ice_lite=true",
            "--rtc.skip_external_ip_validation=true",
            "--rtc.advertise_internal_ip=true",
            "--rtc.strict_acks=true",
            "--rtc.allow_udp_unstable_fallback=true",
            "--rtc.force_tcp=true",
            "--rtc.enable_warp=true",
        ],
        &Env::default(),
    );
    assert_eq!(config.official.rtc.tcp_port, 9000);
    assert_eq!(config.official.rtc.port_range_start, 10000);
    assert_eq!(config.official.rtc.port_range_end, 20000);
    assert!(config.official.rtc.use_ice_lite);
    assert!(config.official.rtc.skip_external_ip_validation);
    assert!(config.official.rtc.advertise_internal_ip);
    assert!(config.official.rtc.strict_acks);
    assert!(config.official.rtc.allow_udp_unstable_fallback);
    assert!(config.official.rtc.force_tcp);
    assert!(config.official.rtc.enable_warp);

    // RTC GoDuration flags
    let config = parse(
        &[
            "--rtc.host_acceptance_min_wait=100ms",
            "--rtc.srflx_acceptance_min_wait=200ms",
            "--rtc.prflx_acceptance_min_wait=300ms",
            "--rtc.relay_acceptance_min_wait=400ms",
        ],
        &Env::default(),
    );
    assert_eq!(
        config.official.rtc.host_acceptance_min_wait,
        GoDuration::from_millis(100)
    );
    assert_eq!(
        config.official.rtc.srflx_acceptance_min_wait,
        GoDuration::from_millis(200)
    );
    assert_eq!(
        config.official.rtc.prflx_acceptance_min_wait,
        GoDuration::from_millis(300)
    );
    assert_eq!(
        config.official.rtc.relay_acceptance_min_wait,
        GoDuration::from_millis(400)
    );

    // RTC congestion control
    let config = parse(
        &[
            "--rtc.congestion_control.enabled=true",
            "--rtc.congestion_control.allow_pause=true",
        ],
        &Env::default(),
    );
    assert!(config.official.rtc.congestion_control.enabled);
    assert!(config.official.rtc.congestion_control.allow_pause);

    // TURN domain + cert paths (no TLS port to avoid domain validation complexity)
    let config = parse(
        &[
            "--turn.domain=turn.example.com",
            "--turn.cert_file=/etc/turn/cert.pem",
            "--turn.key_file=/etc/turn/key.pem",
        ],
        &Env::default(),
    );
    assert_eq!(config.official.turn.domain, "turn.example.com");
    assert_eq!(config.official.turn.cert_file, "/etc/turn/cert.pem");
    assert_eq!(config.official.turn.key_file, "/etc/turn/key.pem");

    // STUN servers: Vec<String> has no generated CLI flag; YAML tested above.

    // Node stats GoDuration flags
    let config = parse(
        &[
            "--node_stats.stats_update_interval=5s",
            "--node_stats.stats_max_delay=60s",
        ],
        &Env::default(),
    );
    assert_eq!(
        config.official.node_stats.stats_update_interval,
        GoDuration::from_secs(5)
    );
    assert_eq!(
        config.official.node_stats.stats_max_delay,
        GoDuration::from_secs(60)
    );

    // API GoDuration flags
    let config = parse(
        &[
            "--api.execution_timeout=5s",
            "--api.check_interval=200ms",
            "--api.max_check_interval=600s",
        ],
        &Env::default(),
    );
    assert_eq!(
        config.official.api.execution_timeout,
        GoDuration::from_secs(5)
    );
    assert_eq!(
        config.official.api.check_interval,
        GoDuration::from_millis(200)
    );
    assert_eq!(
        config.official.api.max_check_interval,
        GoDuration::from_secs(600)
    );

    // Signal relay GoDuration flags
    let config = parse(
        &[
            "--signal_relay.retry_timeout=10s",
            "--signal_relay.min_retry_interval=1s",
            "--signal_relay.max_retry_interval=8s",
        ],
        &Env::default(),
    );
    assert_eq!(
        config.official.signal_relay.retry_timeout,
        GoDuration::from_secs(10)
    );
    assert_eq!(
        config.official.signal_relay.min_retry_interval,
        GoDuration::from_millis(1000)
    );
    assert_eq!(
        config.official.signal_relay.max_retry_interval,
        GoDuration::from_secs(8)
    );

    // Video GoDuration flag
    let config = parse(&["--video.dynacast_pause_delay=10s"], &Env::default());
    assert_eq!(
        config.official.video.dynacast_pause_delay,
        GoDuration::from_secs(10)
    );

    // Limit float flag
    let config = parse(&["--limit.bytes_per_sec=1048576.0"], &Env::default());
    assert!((config.official.limit.bytes_per_sec - 1048576.0).abs() < 1.0);

    // Metric timestamper GoDuration flags
    let config = parse(
        &[
            "--metric.timestamper_config.one_way_delay_estimator_min_interval=100ms",
            "--metric.timestamper_config.one_way_delay_estimator_max_batch=50",
        ],
        &Env::default(),
    );
    assert_eq!(
        config
            .official
            .metric
            .timestamper_config
            .one_way_delay_estimator_min_interval,
        GoDuration::from_millis(100)
    );
    assert_eq!(
        config
            .official
            .metric
            .timestamper_config
            .one_way_delay_estimator_max_batch,
        50
    );

    // Agents endpoints GoDuration flag
    let config = parse(
        &["--agents.endpoints.tunnel_drain_timeout=30s"],
        &Env::default(),
    );
    assert_eq!(
        config.official.agents.endpoints.tunnel_drain_timeout,
        GoDuration::from_secs(30)
    );

    // Webhook resource_url_notifier GoDuration flag
    let config = parse(
        &["--webhook.resource_url_notifier.max_age=1m"],
        &Env::default(),
    );
    assert_eq!(
        config.official.webhook.resource_url_notifier.max_age,
        GoDuration::from_secs(60)
    );

    // RTC pli_throttle GoDuration flags
    let config = parse(
        &[
            "--rtc.pli_throttle.low_quality=200ms",
            "--rtc.pli_throttle.mid_quality=500ms",
            "--rtc.pli_throttle.high_quality=1s",
        ],
        &Env::default(),
    );
    assert_eq!(
        config.official.rtc.pli_throttle.low_quality,
        GoDuration::from_millis(200)
    );
    assert_eq!(
        config.official.rtc.pli_throttle.mid_quality,
        GoDuration::from_millis(500)
    );
    assert_eq!(
        config.official.rtc.pli_throttle.high_quality,
        GoDuration::from_secs(1)
    );

    // RTC forward_stats GoDuration flags
    let config = parse(
        &[
            "--rtc.forward_stats.summary_interval=10s",
            "--rtc.forward_stats.report_interval=30s",
            "--rtc.forward_stats.report_window=1m",
        ],
        &Env::default(),
    );
    assert_eq!(
        config.official.rtc.forward_stats.summary_interval,
        GoDuration::from_secs(10)
    );
    assert_eq!(
        config.official.rtc.forward_stats.report_interval,
        GoDuration::from_secs(30)
    );
    assert_eq!(
        config.official.rtc.forward_stats.report_window,
        GoDuration::from_secs(60)
    );

    // RTC batch_io flags
    let config = parse(
        &[
            "--rtc.batch_io.batch_size=64",
            "--rtc.batch_io.max_flush_interval=1ms",
        ],
        &Env::default(),
    );
    assert_eq!(config.official.rtc.batch_io.batch_size, 64);
    assert_eq!(
        config.official.rtc.batch_io.max_flush_interval,
        GoDuration::from_millis(1)
    );

    // RTC datachannel GoDuration flag
    let config = parse(
        &["--rtc.datachannel_lossy_target_latency=50ms"],
        &Env::default(),
    );
    assert_eq!(
        config.official.rtc.datachannel_lossy_target_latency,
        GoDuration::from_millis(50)
    );

    // Webhook URLs: Vec<String> has no generated CLI flag; YAML tested above.

    // TURN bind_addresses: Vec<String> has no generated CLI flag; YAML tested above.
}

// ---------------------------------------------------------------------------
// Final generated_flags sweep: psrpc, ingress, turn, rtc non-CC, congestion control
// ---------------------------------------------------------------------------

#[test]
fn generated_flags_cli_coverage_psrpc_ingress_rtc_cc() {
    // PSRPC flags
    let config = parse(
        &[
            "--psrpc.max_attempts=5",
            "--psrpc.timeout=10s",
            "--psrpc.backoff=1s",
            "--psrpc.buffer_size=2000",
            "--psrpc.compression.quality=3",
            "--psrpc.compression.threshold=2048",
            "--psrpc.compression.max_decompressed_size=10000",
        ],
        &Env::default(),
    );
    assert_eq!(config.official.psrpc.max_attempts, 5);
    assert_eq!(config.official.psrpc.timeout, GoDuration::from_secs(10));
    assert_eq!(config.official.psrpc.backoff, GoDuration::from_secs(1));
    assert_eq!(config.official.psrpc.buffer_size, 2000);
    assert_eq!(config.official.psrpc.compression.quality, 3);
    assert_eq!(config.official.psrpc.compression.threshold, 2048);
    assert_eq!(
        config.official.psrpc.compression.max_decompressed_size,
        10000
    );

    // Ingress flags
    let config = parse(
        &[
            "--ingress.rtmp_base_url=rtmp://localhost:1935",
            "--ingress.whip_base_url=http://localhost:8080",
        ],
        &Env::default(),
    );
    assert_eq!(
        config.official.ingress.rtmp_base_url,
        "rtmp://localhost:1935"
    );
    assert_eq!(
        config.official.ingress.whip_base_url,
        "http://localhost:8080"
    );

    // TURN tls_port
    let config = parse(&["--turn.tls_port=5349"], &Env::default());
    assert_eq!(config.official.turn.tls_port, 5349);

    // RTC non-CC flags
    let config = parse(
        &[
            "--rtc.udp_port.start=10000",
            "--rtc.udp_port.end=20000",
            "--rtc.node_ip.ipv4=10.0.0.1",
            "--rtc.node_ip.ipv6=::1",
            "--rtc.use_external_ip=false",
            "--rtc.allow_tcp_fallback=true",
        ],
        &Env::default(),
    );
    assert_eq!(config.official.rtc.udp_port.start, 10000);
    assert_eq!(config.official.rtc.udp_port.end, 20000);
    assert_eq!(config.official.rtc.node_ip.v4, "10.0.0.1");
    assert_eq!(config.official.rtc.node_ip.v6, "::1");
    assert!(!config.official.rtc.use_external_ip);
    assert_eq!(config.official.rtc.allow_tcp_fallback, Some(true));

    // Congestion control - stream allocator
    let config = parse(
        &[
            "--rtc.congestion_control.stream_allocator.min_channel_capacity=5",
            "--rtc.congestion_control.stream_allocator.disable_etimation_unmanaged_tracks=true",
            "--rtc.congestion_control.stream_allocator.probe_mode=aggregate",
            "--rtc.congestion_control.stream_allocator.probe_overage_pct=150",
            "--rtc.congestion_control.stream_allocator.probe_min_bps=100000",
            "--rtc.congestion_control.stream_allocator.paused_min_wait=10s",
        ],
        &Env::default(),
    );
    assert_eq!(
        config
            .official
            .rtc
            .congestion_control
            .stream_allocator
            .min_channel_capacity,
        5
    );
    assert!(
        config
            .official
            .rtc
            .congestion_control
            .stream_allocator
            .disable_etimation_unmanaged_tracks
    );
    assert_eq!(
        config
            .official
            .rtc
            .congestion_control
            .stream_allocator
            .probe_mode,
        "aggregate"
    );
    assert_eq!(
        config
            .official
            .rtc
            .congestion_control
            .stream_allocator
            .probe_overage_pct,
        150
    );
    assert_eq!(
        config
            .official
            .rtc
            .congestion_control
            .stream_allocator
            .probe_min_bps,
        100000
    );
    assert_eq!(
        config
            .official
            .rtc
            .congestion_control
            .stream_allocator
            .paused_min_wait,
        GoDuration::from_secs(10)
    );

    // Congestion control - remote BWE
    let config = parse(
        &[
            "--rtc.congestion_control.remote_bwe.nack_ratio_attenuator=0.5",
            "--rtc.congestion_control.remote_bwe.expected_usage_threshold=0.8",
        ],
        &Env::default(),
    );
    assert!(
        (config
            .official
            .rtc
            .congestion_control
            .remote_bwe
            .nack_ratio_attenuator
            - 0.5)
            .abs()
            < 0.01
    );
    assert!(
        (config
            .official
            .rtc
            .congestion_control
            .remote_bwe
            .expected_usage_threshold
            - 0.8)
            .abs()
            < 0.01
    );

    // Congestion control - remote BWE channel observer probe
    let config = parse(
        &[
            "--rtc.congestion_control.remote_bwe.channel_observer_probe.estimate.required_samples=5",
            "--rtc.congestion_control.remote_bwe.channel_observer_probe.estimate.required_samples_min=3",
            "--rtc.congestion_control.remote_bwe.channel_observer_probe.estimate.downward_trend_threshold=10.0",
            "--rtc.congestion_control.remote_bwe.channel_observer_probe.estimate.downward_trend_max_wait=30s",
            "--rtc.congestion_control.remote_bwe.channel_observer_probe.estimate.collapse_threshold=100ms",
            "--rtc.congestion_control.remote_bwe.channel_observer_probe.estimate.validity_window=1m",
        ],
        &Env::default(),
    );
    let cop = &config
        .official
        .rtc
        .congestion_control
        .remote_bwe
        .channel_observer_probe;
    assert_eq!(cop.estimate.required_samples, 5);
    assert_eq!(cop.estimate.required_samples_min, 3);
    assert!((cop.estimate.downward_trend_threshold - 10.0).abs() < 0.01);
    assert_eq!(
        cop.estimate.downward_trend_max_wait,
        GoDuration::from_secs(30)
    );
    assert_eq!(
        cop.estimate.collapse_threshold,
        GoDuration::from_millis(100)
    );
    assert_eq!(cop.estimate.validity_window, GoDuration::from_secs(60));

    // Congestion control - remote BWE channel observer probe NACK
    let config = parse(
        &[
            "--rtc.congestion_control.remote_bwe.channel_observer_probe.nack.window_min_duration=1s",
            "--rtc.congestion_control.remote_bwe.channel_observer_probe.nack.window_max_duration=5s",
            "--rtc.congestion_control.remote_bwe.channel_observer_probe.nack.ratio_threshold=0.1",
        ],
        &Env::default(),
    );
    let cop = &config
        .official
        .rtc
        .congestion_control
        .remote_bwe
        .channel_observer_probe;
    assert_eq!(cop.nack.window_min_duration, GoDuration::from_secs(1));
    assert_eq!(cop.nack.window_max_duration, GoDuration::from_secs(5));
    assert!((cop.nack.ratio_threshold - 0.1).abs() < 0.01);

    // Congestion control - remote BWE channel observer non-probe
    let config = parse(
        &[
            "--rtc.congestion_control.remote_bwe.channel_observer_non_probe.estimate.required_samples=15",
            "--rtc.congestion_control.remote_bwe.channel_observer_non_probe.estimate.required_samples_min=10",
            "--rtc.congestion_control.remote_bwe.channel_observer_non_probe.estimate.downward_trend_threshold=20.0",
            "--rtc.congestion_control.remote_bwe.channel_observer_non_probe.estimate.downward_trend_max_wait=1m",
            "--rtc.congestion_control.remote_bwe.channel_observer_non_probe.estimate.collapse_threshold=200ms",
            "--rtc.congestion_control.remote_bwe.channel_observer_non_probe.estimate.validity_window=2m",
            "--rtc.congestion_control.remote_bwe.channel_observer_non_probe.nack.window_min_duration=3s",
            "--rtc.congestion_control.remote_bwe.channel_observer_non_probe.nack.window_max_duration=10s",
            "--rtc.congestion_control.remote_bwe.channel_observer_non_probe.nack.ratio_threshold=0.15",
        ],
        &Env::default(),
    );
    let conp = &config
        .official
        .rtc
        .congestion_control
        .remote_bwe
        .channel_observer_non_probe;
    assert_eq!(conp.estimate.required_samples, 15);
    assert_eq!(conp.estimate.required_samples_min, 10);
    assert!((conp.estimate.downward_trend_threshold - 20.0).abs() < 0.01);
    assert_eq!(
        conp.estimate.downward_trend_max_wait,
        GoDuration::from_secs(60)
    );
    assert_eq!(
        conp.estimate.collapse_threshold,
        GoDuration::from_millis(200)
    );
    assert_eq!(conp.estimate.validity_window, GoDuration::from_secs(120));
    assert_eq!(conp.nack.window_min_duration, GoDuration::from_secs(3));
    assert_eq!(conp.nack.window_max_duration, GoDuration::from_secs(10));
    assert!((conp.nack.ratio_threshold - 0.15).abs() < 0.01);

    // Congestion control - remote BWE probe controller
    let config = parse(
        &[
            "--rtc.congestion_control.remote_bwe.probe_controller.probe_regulator.base_interval=500ms",
            "--rtc.congestion_control.remote_bwe.probe_controller.probe_regulator.backoff_factor=0.5",
            "--rtc.congestion_control.remote_bwe.probe_controller.probe_regulator.max_interval=30s",
            "--rtc.congestion_control.remote_bwe.probe_controller.probe_regulator.min_duration=10s",
            "--rtc.congestion_control.remote_bwe.probe_controller.probe_regulator.max_duration=1m",
            "--rtc.congestion_control.remote_bwe.probe_controller.probe_regulator.duration_increase_factor=2.0",
            "--rtc.congestion_control.remote_bwe.probe_controller.settle_wait_num_rtt=3",
            "--rtc.congestion_control.remote_bwe.probe_controller.settle_wait_min=1s",
            "--rtc.congestion_control.remote_bwe.probe_controller.settle_wait_max=10s",
        ],
        &Env::default(),
    );
    let pc = &config
        .official
        .rtc
        .congestion_control
        .remote_bwe
        .probe_controller;
    assert_eq!(
        pc.probe_regulator.base_interval,
        GoDuration::from_millis(500)
    );
    assert!((pc.probe_regulator.backoff_factor - 0.5).abs() < 0.01);
    assert_eq!(pc.probe_regulator.max_interval, GoDuration::from_secs(30));
    assert_eq!(pc.probe_regulator.min_duration, GoDuration::from_secs(10));
    assert_eq!(pc.probe_regulator.max_duration, GoDuration::from_secs(60));
    assert!((pc.probe_regulator.duration_increase_factor - 2.0).abs() < 0.01);
    assert_eq!(pc.settle_wait_num_rtt, 3);
    assert_eq!(pc.settle_wait_min, GoDuration::from_secs(1));
    assert_eq!(pc.settle_wait_max, GoDuration::from_secs(10));

    // Congestion control - send side BWE congestion detector
    let config = parse(
        &[
            "--rtc.congestion_control.send_side_bwe.congestion_detector.packet_group.min_packets=50",
            "--rtc.congestion_control.send_side_bwe.congestion_detector.packet_group.max_window_duration=2s",
            "--rtc.congestion_control.send_side_bwe.congestion_detector.packet_group_max_age=30s",
            "--rtc.congestion_control.send_side_bwe.congestion_detector.jqr_min_delay=100ms",
            "--rtc.congestion_control.send_side_bwe.congestion_detector.jqr_min_trend_coefficient=1.5",
            "--rtc.congestion_control.send_side_bwe.congestion_detector.dqr_max_delay=50ms",
            "--rtc.congestion_control.send_side_bwe.congestion_detector.jqr_min_weighted_loss=0.5",
            "--rtc.congestion_control.send_side_bwe.congestion_detector.dqr_max_weighted_loss=0.2",
            "--rtc.congestion_control.send_side_bwe.congestion_detector.congested_ctr_epsilon=0.01",
            "--rtc.congestion_control.send_side_bwe.congestion_detector.estimaton_window_duration=5s",
        ],
        &Env::default(),
    );
    let cd = &config
        .official
        .rtc
        .congestion_control
        .send_side_bwe
        .congestion_detector;
    assert_eq!(cd.packet_group.min_packets, 50);
    assert_eq!(
        cd.packet_group.max_window_duration,
        GoDuration::from_secs(2)
    );
    assert_eq!(cd.packet_group_max_age, GoDuration::from_secs(30));
    assert_eq!(cd.jqr_min_delay, GoDuration::from_millis(100));
    assert!((cd.jqr_min_trend_coefficient - 1.5).abs() < 0.01);
    assert_eq!(cd.dqr_max_delay, GoDuration::from_millis(50));
    assert!((cd.jqr_min_weighted_loss - 0.5).abs() < 0.01);
    assert!((cd.dqr_max_weighted_loss - 0.2).abs() < 0.01);
    assert!((cd.congested_ctr_epsilon - 0.01).abs() < 0.001);
    assert_eq!(cd.estimaton_window_duration, GoDuration::from_secs(5));

    // Congestion control - send side BWE weighted loss
    let config = parse(
        &[
            "--rtc.congestion_control.send_side_bwe.congestion_detector.weighted_loss.min_duration_for_loss_validity=1s",
            "--rtc.congestion_control.send_side_bwe.congestion_detector.weighted_loss.base_duration=500ms",
            "--rtc.congestion_control.send_side_bwe.congestion_detector.weighted_loss.base_pps=100",
            "--rtc.congestion_control.send_side_bwe.congestion_detector.weighted_loss.loss_penalty_factor=0.5",
        ],
        &Env::default(),
    );
    let cd = &config
        .official
        .rtc
        .congestion_control
        .send_side_bwe
        .congestion_detector;
    let wl = &cd.weighted_loss;
    assert_eq!(wl.min_duration_for_loss_validity, GoDuration::from_secs(1));
    assert_eq!(wl.base_duration, GoDuration::from_millis(500));
    assert_eq!(wl.base_pps, 100);
    assert!((wl.loss_penalty_factor - 0.5).abs() < 0.01);

    // Congestion control - send side BWE probe signal
    let config = parse(
        &[
            "--rtc.congestion_control.send_side_bwe.congestion_detector.probe_signal.min_bytes_ratio=0.1",
            "--rtc.congestion_control.send_side_bwe.congestion_detector.probe_signal.min_duration_ratio=0.2",
            "--rtc.congestion_control.send_side_bwe.congestion_detector.probe_signal.jqr_min_delay=100ms",
            "--rtc.congestion_control.send_side_bwe.congestion_detector.probe_signal.dqr_max_delay=50ms",
        ],
        &Env::default(),
    );
    let cd = &config
        .official
        .rtc
        .congestion_control
        .send_side_bwe
        .congestion_detector;
    let ps = &cd.probe_signal;
    assert!((ps.min_bytes_ratio - 0.1).abs() < 0.01);
    assert!((ps.min_duration_ratio - 0.2).abs() < 0.01);
    assert_eq!(ps.jqr_min_delay, GoDuration::from_millis(100));
    assert_eq!(ps.dqr_max_delay, GoDuration::from_millis(50));
}

// ---------------------------------------------------------------------------
// Final sweep: deep CC sub-paths, YAML turn_servers/prometheus/debug_handler,
// base_flags --node-ip/--udp-port
// ---------------------------------------------------------------------------

#[test]
fn generated_flags_deep_cc_probe_packet_group_regulator_signal() {
    // probe_packet_group.packet_group (2)
    let config = parse(
        &[
            "--rtc.congestion_control.send_side_bwe.congestion_detector.probe_packet_group.packet_group.min_packets=100",
            "--rtc.congestion_control.send_side_bwe.congestion_detector.probe_packet_group.packet_group.max_window_duration=5s",
        ],
        &Env::default(),
    );
    let ppg = &config
        .official
        .rtc
        .congestion_control
        .send_side_bwe
        .congestion_detector
        .probe_packet_group;
    assert_eq!(ppg.packet_group.min_packets, 100);
    assert_eq!(
        ppg.packet_group.max_window_duration,
        GoDuration::from_secs(5)
    );

    // probe_packet_group.settle_wait (3)
    let config = parse(
        &[
            "--rtc.congestion_control.send_side_bwe.congestion_detector.probe_packet_group.settle_wait_num_rtt=7",
            "--rtc.congestion_control.send_side_bwe.congestion_detector.probe_packet_group.settle_wait_min=500ms",
            "--rtc.congestion_control.send_side_bwe.congestion_detector.probe_packet_group.settle_wait_max=20s",
        ],
        &Env::default(),
    );
    let ppg = &config
        .official
        .rtc
        .congestion_control
        .send_side_bwe
        .congestion_detector
        .probe_packet_group;
    assert_eq!(ppg.settle_wait_num_rtt, 7);
    assert_eq!(ppg.settle_wait_min, GoDuration::from_millis(500));
    assert_eq!(ppg.settle_wait_max, GoDuration::from_secs(20));

    // probe_regulator (6)
    let config = parse(
        &[
            "--rtc.congestion_control.send_side_bwe.congestion_detector.probe_regulator.base_interval=1s",
            "--rtc.congestion_control.send_side_bwe.congestion_detector.probe_regulator.backoff_factor=1.5",
            "--rtc.congestion_control.send_side_bwe.congestion_detector.probe_regulator.max_interval=1m",
            "--rtc.congestion_control.send_side_bwe.congestion_detector.probe_regulator.min_duration=5s",
            "--rtc.congestion_control.send_side_bwe.congestion_detector.probe_regulator.max_duration=2m",
            "--rtc.congestion_control.send_side_bwe.congestion_detector.probe_regulator.duration_increase_factor=3.0",
        ],
        &Env::default(),
    );
    let pr = &config
        .official
        .rtc
        .congestion_control
        .send_side_bwe
        .congestion_detector
        .probe_regulator;
    assert_eq!(pr.base_interval, GoDuration::from_secs(1));
    assert!((pr.backoff_factor - 1.5).abs() < 0.01);
    assert_eq!(pr.max_interval, GoDuration::from_secs(60));
    assert_eq!(pr.min_duration, GoDuration::from_secs(5));
    assert_eq!(pr.max_duration, GoDuration::from_secs(120));
    assert!((pr.duration_increase_factor - 3.0).abs() < 0.01);

    // probe_signal.weighted_loss (4)
    let config = parse(
        &[
            "--rtc.congestion_control.send_side_bwe.congestion_detector.probe_signal.weighted_loss.min_duration_for_loss_validity=2s",
            "--rtc.congestion_control.send_side_bwe.congestion_detector.probe_signal.weighted_loss.base_duration=1s",
            "--rtc.congestion_control.send_side_bwe.congestion_detector.probe_signal.weighted_loss.base_pps=200",
            "--rtc.congestion_control.send_side_bwe.congestion_detector.probe_signal.weighted_loss.loss_penalty_factor=1.5",
        ],
        &Env::default(),
    );
    let pswl = &config
        .official
        .rtc
        .congestion_control
        .send_side_bwe
        .congestion_detector
        .probe_signal
        .weighted_loss;
    assert_eq!(
        pswl.min_duration_for_loss_validity,
        GoDuration::from_secs(2)
    );
    assert_eq!(pswl.base_duration, GoDuration::from_secs(1));
    assert_eq!(pswl.base_pps, 200);
    assert!((pswl.loss_penalty_factor - 1.5).abs() < 0.01);

    // probe_signal.jqr/dqr weighted_loss (2)
    let config = parse(
        &[
            "--rtc.congestion_control.send_side_bwe.congestion_detector.probe_signal.jqr_min_weighted_loss=0.3",
            "--rtc.congestion_control.send_side_bwe.congestion_detector.probe_signal.dqr_max_weighted_loss=0.15",
        ],
        &Env::default(),
    );
    let ps = &config
        .official
        .rtc
        .congestion_control
        .send_side_bwe
        .congestion_detector
        .probe_signal;
    assert!((ps.jqr_min_weighted_loss - 0.3).abs() < 0.01);
    assert!((ps.dqr_max_weighted_loss - 0.15).abs() < 0.01);
}

#[test]
fn generated_flags_deep_cc_early_warning_congested_signals() {
    // queuing_delay_early_warning_jqr/dqr (4)
    let config = parse(
        &[
            "--rtc.congestion_control.send_side_bwe.congestion_detector.queuing_delay_early_warning_jqr.min_number_of_groups=3",
            "--rtc.congestion_control.send_side_bwe.congestion_detector.queuing_delay_early_warning_jqr.min_duration=500ms",
            "--rtc.congestion_control.send_side_bwe.congestion_detector.queuing_delay_early_warning_dqr.min_number_of_groups=4",
            "--rtc.congestion_control.send_side_bwe.congestion_detector.queuing_delay_early_warning_dqr.min_duration=1s",
        ],
        &Env::default(),
    );
    let cd = &config
        .official
        .rtc
        .congestion_control
        .send_side_bwe
        .congestion_detector;
    assert_eq!(cd.queuing_delay_early_warning_jqr.min_number_of_groups, 3);
    assert_eq!(
        cd.queuing_delay_early_warning_jqr.min_duration,
        GoDuration::from_millis(500)
    );
    assert_eq!(cd.queuing_delay_early_warning_dqr.min_number_of_groups, 4);
    assert_eq!(
        cd.queuing_delay_early_warning_dqr.min_duration,
        GoDuration::from_secs(1)
    );

    // loss_early_warning_jqr/dqr (4)
    let config = parse(
        &[
            "--rtc.congestion_control.send_side_bwe.congestion_detector.loss_early_warning_jqr.min_number_of_groups=5",
            "--rtc.congestion_control.send_side_bwe.congestion_detector.loss_early_warning_jqr.min_duration=2s",
            "--rtc.congestion_control.send_side_bwe.congestion_detector.loss_early_warning_dqr.min_number_of_groups=6",
            "--rtc.congestion_control.send_side_bwe.congestion_detector.loss_early_warning_dqr.min_duration=3s",
        ],
        &Env::default(),
    );
    let cd = &config
        .official
        .rtc
        .congestion_control
        .send_side_bwe
        .congestion_detector;
    assert_eq!(cd.loss_early_warning_jqr.min_number_of_groups, 5);
    assert_eq!(
        cd.loss_early_warning_jqr.min_duration,
        GoDuration::from_secs(2)
    );
    assert_eq!(cd.loss_early_warning_dqr.min_number_of_groups, 6);
    assert_eq!(
        cd.loss_early_warning_dqr.min_duration,
        GoDuration::from_secs(3)
    );

    // queuing_delay_congested_jqr/dqr (4)
    let config = parse(
        &[
            "--rtc.congestion_control.send_side_bwe.congestion_detector.queuing_delay_congested_jqr.min_number_of_groups=7",
            "--rtc.congestion_control.send_side_bwe.congestion_detector.queuing_delay_congested_jqr.min_duration=4s",
            "--rtc.congestion_control.send_side_bwe.congestion_detector.queuing_delay_congested_dqr.min_number_of_groups=8",
            "--rtc.congestion_control.send_side_bwe.congestion_detector.queuing_delay_congested_dqr.min_duration=5s",
        ],
        &Env::default(),
    );
    let cd = &config
        .official
        .rtc
        .congestion_control
        .send_side_bwe
        .congestion_detector;
    assert_eq!(cd.queuing_delay_congested_jqr.min_number_of_groups, 7);
    assert_eq!(
        cd.queuing_delay_congested_jqr.min_duration,
        GoDuration::from_secs(4)
    );
    assert_eq!(cd.queuing_delay_congested_dqr.min_number_of_groups, 8);
    assert_eq!(
        cd.queuing_delay_congested_dqr.min_duration,
        GoDuration::from_secs(5)
    );

    // loss_congested_jqr/dqr (4)
    let config = parse(
        &[
            "--rtc.congestion_control.send_side_bwe.congestion_detector.loss_congested_jqr.min_number_of_groups=9",
            "--rtc.congestion_control.send_side_bwe.congestion_detector.loss_congested_jqr.min_duration=6s",
            "--rtc.congestion_control.send_side_bwe.congestion_detector.loss_congested_dqr.min_number_of_groups=10",
            "--rtc.congestion_control.send_side_bwe.congestion_detector.loss_congested_dqr.min_duration=7s",
        ],
        &Env::default(),
    );
    let cd = &config
        .official
        .rtc
        .congestion_control
        .send_side_bwe
        .congestion_detector;
    assert_eq!(cd.loss_congested_jqr.min_number_of_groups, 9);
    assert_eq!(cd.loss_congested_jqr.min_duration, GoDuration::from_secs(6));
    assert_eq!(cd.loss_congested_dqr.min_number_of_groups, 10);
    assert_eq!(cd.loss_congested_dqr.min_duration, GoDuration::from_secs(7));
}

#[test]
fn generated_flags_deep_cc_congested_ctr_trend_and_packet_group() {
    // congested_ctr_trend (6) - same type pattern as TrendDetectorConfig
    let config = parse(
        &[
            "--rtc.congestion_control.send_side_bwe.congestion_detector.congested_ctr_trend.required_samples=10",
            "--rtc.congestion_control.send_side_bwe.congestion_detector.congested_ctr_trend.required_samples_min=5",
            "--rtc.congestion_control.send_side_bwe.congestion_detector.congested_ctr_trend.downward_trend_threshold=2.5",
            "--rtc.congestion_control.send_side_bwe.congestion_detector.congested_ctr_trend.downward_trend_max_wait=15s",
            "--rtc.congestion_control.send_side_bwe.congestion_detector.congested_ctr_trend.collapse_threshold=300ms",
            "--rtc.congestion_control.send_side_bwe.congestion_detector.congested_ctr_trend.validity_window=30s",
        ],
        &Env::default(),
    );
    let ct = &config
        .official
        .rtc
        .congestion_control
        .send_side_bwe
        .congestion_detector
        .congested_ctr_trend;
    assert_eq!(ct.required_samples, 10);
    assert_eq!(ct.required_samples_min, 5);
    assert!((ct.downward_trend_threshold - 2.5).abs() < 0.01);
    assert_eq!(ct.downward_trend_max_wait, GoDuration::from_secs(15));
    assert_eq!(ct.collapse_threshold, GoDuration::from_millis(300));
    assert_eq!(ct.validity_window, GoDuration::from_secs(30));

    // congested_packet_group (2)
    let config = parse(
        &[
            "--rtc.congestion_control.send_side_bwe.congestion_detector.congested_packet_group.min_packets=30",
            "--rtc.congestion_control.send_side_bwe.congestion_detector.congested_packet_group.max_window_duration=3s",
        ],
        &Env::default(),
    );
    let cpg = &config
        .official
        .rtc
        .congestion_control
        .send_side_bwe
        .congestion_detector
        .congested_packet_group;
    assert_eq!(cpg.min_packets, 30);
    assert_eq!(cpg.max_window_duration, GoDuration::from_secs(3));
}

#[test]
fn yaml_turn_servers_prometheus_debug_handler_coverage() {
    // prometheus (3 fields)
    let config = parse(
        &[
            "--config-body",
            "prometheus:\n  port: 9090\n  username: admin\n  password: secret\n",
        ],
        &Env::default(),
    );
    assert_eq!(config.official.prometheus.port, 9090);
    assert_eq!(config.official.prometheus.username, "admin");
    assert_eq!(config.official.prometheus.password, "secret");

    // debug_handler (1 field)
    let config = parse(
        &["--config-body", "debug_handler:\n  port: 6666\n"],
        &Env::default(),
    );
    assert_eq!(config.official.debug_handler.port, 6666);

    // rtc.turn_servers (array of TurnServer)
    let config = parse(
        &[
            "--config-body",
            "rtc:\n  turn_servers:\n    - host: turn.example.com\n      port: 3478\n      protocol: udp\n      username: testuser\n      credential: testcred\n      secret: testsecret\n      ttl: 3600\n",
        ],
        &Env::default(),
    );
    assert_eq!(config.official.rtc.turn_servers.len(), 1);
    let ts = &config.official.rtc.turn_servers[0];
    assert_eq!(ts.host, "turn.example.com");
    assert_eq!(ts.port, 3478);
    assert_eq!(ts.protocol, "udp");
    assert_eq!(ts.username, "testuser");
    assert_eq!(ts.credential, "testcred");
    assert_eq!(ts.secret, "testsecret");
    assert_eq!(ts.ttl, 3600);
}

#[test]
fn base_flags_node_ip_and_udp_port() {
    // --node-ip sets rtc.node_ip via UnmarshalString
    let config = parse(&["--node-ip=10.0.0.5"], &Env::default());
    assert_eq!(config.official.rtc.node_ip.v4, "10.0.0.5");

    // --udp-port sets rtc.udp_port via UnmarshalString (scalar)
    let config = parse(&["--udp-port=10000"], &Env::default());
    assert_eq!(config.official.rtc.udp_port.start, 10000);
    assert_eq!(config.official.rtc.udp_port.end, 0);

    // --udp-port with range
    let config = parse(&["--udp-port=10000-20000"], &Env::default());
    assert_eq!(config.official.rtc.udp_port.start, 10000);
    assert_eq!(config.official.rtc.udp_port.end, 20000);

    // --config with YAML file path (use --config-body as proxy since --config needs a file)
    // Verify --config-body is accepted and parsed
    let config = parse(&["--config-body", "region: us-west-2\n"], &Env::default());
    assert_eq!(config.official.region, "us-west-2");
}

// ---------------------------------------------------------------------------
// YAML room_configurations.{key}.name + CC parent struct keys
// ---------------------------------------------------------------------------

#[test]
fn yaml_room_configurations_name_and_cc_parent_structs() {
    // room.room_configurations.{key}.name
    let config = parse(
        &[
            "--config-body",
            "room:\n  room_configurations:\n    myroom:\n      name: Test Room\n      empty_timeout: 600\n      max_participants: 50\n",
        ],
        &Env::default(),
    );
    let rc = config.official.room.room_configurations.get("myroom");
    assert!(rc.is_some(), "myroom configuration must exist");
    let rc = rc.unwrap().as_ref().unwrap();
    assert_eq!(rc.name, "Test Room");
    assert_eq!(rc.empty_timeout, 600);
    assert_eq!(rc.max_participants, 50);

    // Verify CC parent struct keys exist by parsing their children
    // stream_allocator
    let config = parse(
        &[
            "--config-body",
            "rtc:\n  congestion_control:\n    stream_allocator:\n      min_channel_capacity: 8\n",
        ],
        &Env::default(),
    );
    assert_eq!(
        config
            .official
            .rtc
            .congestion_control
            .stream_allocator
            .min_channel_capacity,
        8
    );

    // remote_bwe
    let config = parse(
        &[
            "--config-body",
            "rtc:\n  congestion_control:\n    remote_bwe:\n      nack_ratio_attenuator: 0.3\n",
        ],
        &Env::default(),
    );
    assert!(
        (config
            .official
            .rtc
            .congestion_control
            .remote_bwe
            .nack_ratio_attenuator
            - 0.3)
            .abs()
            < 0.01
    );

    // send_side_bwe
    let config = parse(
        &[
            "--config-body",
            "rtc:\n  congestion_control:\n    send_side_bwe:\n      congestion_detector:\n        congested_ctr_epsilon: 0.02\n",
        ],
        &Env::default(),
    );
    assert!(
        (config
            .official
            .rtc
            .congestion_control
            .send_side_bwe
            .congestion_detector
            .congested_ctr_epsilon
            - 0.02)
            .abs()
            < 0.001
    );
}

// ---------------------------------------------------------------------------
// Remaining YAML/generated_flags leaf coverage
// ---------------------------------------------------------------------------

#[test]
fn yaml_remaining_rtc_limit_api_coverage() {
    // rtc.use_stun_port_as_ice
    let config = parse(
        &["--config-body", "rtc:\n  use_stun_port_as_ice: true\n"],
        &Env::default(),
    );
    assert!(config.official.rtc.use_stun_port_as_ice);

    // rtc.external_ip_only
    let config = parse(
        &["--config-body", "rtc:\n  external_ip_only: true\n"],
        &Env::default(),
    );
    assert!(config.official.rtc.external_ip_only);

    // limit.max_api_request_body_size
    let config = parse(&["--limit.max_api_request_body_size=4096"], &Env::default());
    assert_eq!(config.official.limit.max_api_request_body_size, 4096);

    // limit.signal_message_size_limit (YAML path)
    let config = parse(
        &[
            "--config-body",
            "limit:\n  signal_message_size_limit: 8192\n",
        ],
        &Env::default(),
    );
    assert_eq!(config.official.limit.signal_message_size_limit, 8192);
}

// ---------------------------------------------------------------------------
// YAML remaining: video stream_tracker, room_configurations extras, turn secret_file
// ---------------------------------------------------------------------------

#[test]
fn yaml_video_stream_tracker_manager_and_room_config_extras() {
    // video.stream_tracker_manager.video.packet_tracker.{key}.*
    let config = parse(
        &[
            "--config-body",
            "video:\n  stream_tracker_manager:\n    video:\n      bitrate_report_interval:\n        0: 2s\n      packet_tracker:\n        0:\n          samples_required: 10\n          cycles_required: 5\n          cycle_duration: 1s\n      frame_tracker:\n        0:\n          min_fps: 3.0\n",
        ],
        &Env::default(),
    );
    let stm = &config.official.video.stream_tracker_manager;
    assert_eq!(
        stm.video.bitrate_report_interval[&0],
        GoDuration::from_secs(2)
    );
    assert_eq!(stm.video.packet_tracker[&0].samples_required, 10);
    assert_eq!(stm.video.packet_tracker[&0].cycles_required, 5);
    assert_eq!(
        stm.video.packet_tracker[&0].cycle_duration,
        GoDuration::from_secs(1)
    );
    assert!((stm.video.frame_tracker[&0].min_fps - 3.0).abs() < 0.01);
    // The runtime packet trackers consume the merged map: layer 0 overridden,
    // layers 1 and 2 keep the official defaults.
    let trackers = config.server.video.stream_trackers.video;
    assert_eq!(
        trackers[0],
        Some(rtc_core::config::StreamTrackerSpec::Packet(
            rtc_core::config::PacketTrackerSpec {
                samples_required: 10,
                cycles_required: 5,
                cycle_duration: std::time::Duration::from_secs(1),
            }
        ))
    );
    assert_eq!(
        trackers[1],
        Some(rtc_core::config::StreamTrackerSpec::Packet(
            rtc_core::config::PacketTrackerSpec {
                samples_required: 5,
                cycles_required: 20,
                cycle_duration: std::time::Duration::from_millis(500),
            }
        ))
    );
    assert_eq!(
        config.server.video.stream_trackers.screenshare,
        rtc_core::config::VideoStreamTrackers::default().screenshare
    );

    // video.stream_tracker_manager.screenshare
    let config = parse(
        &[
            "--config-body",
            "video:\n  stream_tracker_manager:\n    screenshare:\n      stream_tracker_type: video\n      bitrate_report_interval:\n        0: 5s\n      packet_tracker:\n        0:\n          samples_required: 2\n          cycles_required: 3\n          cycle_duration: 4s\n      frame_tracker:\n        0:\n          min_fps: 1.0\n",
        ],
        &Env::default(),
    );
    let ss = &config.official.video.stream_tracker_manager.screenshare;
    assert_eq!(ss.stream_tracker_type, "video");
    assert_eq!(ss.bitrate_report_interval[&0], GoDuration::from_secs(5));
    assert_eq!(ss.packet_tracker[&0].samples_required, 2);
    assert_eq!(ss.packet_tracker[&0].cycles_required, 3);
    assert_eq!(
        ss.packet_tracker[&0].cycle_duration,
        GoDuration::from_secs(4)
    );
    assert!((ss.frame_tracker[&0].min_fps - 1.0).abs() < 0.01);
    assert_eq!(config.server.video.stream_trackers.screenshare, [None; 3]);

    let config = parse(
        &[
            "--config-body",
            "video:\n  stream_tracker_manager:\n    video:\n      stream_tracker_type: frame\n      frame_tracker:\n        0:\n          min_fps: 3.0\n    screenshare:\n      stream_tracker_type: frame\n",
        ],
        &Env::default(),
    );
    use rtc_core::config::StreamTrackerSpec;
    assert_eq!(
        config.server.video.stream_trackers.video,
        [
            Some(StreamTrackerSpec::Frame { min_fps: 3.0 }),
            Some(StreamTrackerSpec::Frame { min_fps: 5.0 }),
            Some(StreamTrackerSpec::Frame { min_fps: 5.0 }),
        ]
    );
    assert_eq!(
        config.server.video.stream_trackers.screenshare,
        [Some(StreamTrackerSpec::Frame { min_fps: 0.5 }); 3],
    );

    // room.enabled_codecs[].fmtp_line
    let config = parse(
        &[
            "--config-body",
            "room:\n  enabled_codecs:\n    - mime: video/VP8\n      fmtp_line: profile-id=0\n",
        ],
        &Env::default(),
    );
    assert_eq!(config.official.room.enabled_codecs.len(), 1);
    assert_eq!(config.official.room.enabled_codecs[0].mime, "video/VP8");
    assert_eq!(
        config.official.room.enabled_codecs[0].fmtp_line,
        "profile-id=0"
    );

    // room.room_configurations.{key}.egress/minplayoutdelay/maxplayoutdelay/tags
    let config = parse(
        &[
            "--config-body",
            "room:\n  room_configurations:\n    testroom:\n      min_playout_delay: 100\n      max_playout_delay: 2000\n      sync_streams: true\n      tags:\n        env: prod\n",
        ],
        &Env::default(),
    );
    let rc = config
        .official
        .room
        .room_configurations
        .get("testroom")
        .unwrap()
        .as_ref()
        .unwrap();
    assert_eq!(rc.min_playout_delay, 100);
    assert_eq!(rc.max_playout_delay, 2000);
    assert!(rc.sync_streams);
    assert_eq!(rc.tags.get("env").unwrap(), "prod");

    // rtc.turn_servers[].secret_file - requires actual file (parse pipeline stats it)
    // Test with a temp file
    let dir = std::env::temp_dir().join("lk_test_turn_secret");
    std::fs::create_dir_all(&dir).unwrap();
    let secret_path = dir.join("secret.key");
    std::fs::write(&secret_path, "test-secret").unwrap();
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&secret_path, std::fs::Permissions::from_mode(0o600)).unwrap();
    }
    let yaml = format!(
        "rtc:\n  turn_servers:\n    - host: turn1.example.com\n      port: 3478\n      protocol: udp\n      secret_file: {}\n      ttl: 7200\n",
        secret_path.display()
    );
    let config = parse(&["--config-body", &yaml], &Env::default());
    assert_eq!(config.official.rtc.turn_servers.len(), 1);
    assert_eq!(
        config.official.rtc.turn_servers[0].secret_file,
        secret_path.to_str().unwrap()
    );
    assert_eq!(config.official.rtc.turn_servers[0].ttl, 7200);
    let _ = std::fs::remove_dir_all(&dir);
}

// ---------------------------------------------------------------------------
// CONFIG_CONSUMPTION: verify parsed values propagate to normalized ServerConfig
// ---------------------------------------------------------------------------

#[test]
fn external_turn_protocol_uses_go_tcp_fallback() {
    use rtc_core::config::IceServerProtocol::{Tcp, Tls, Udp};

    for (protocol, expected) in [
        ("", Tcp),
        ("      protocol: unknown\n", Tcp),
        ("      protocol: udp\n", Udp),
        ("      protocol: tcp\n", Tcp),
        ("      protocol: tls\n", Tls),
    ] {
        let yaml = format!(
            "rtc:\n  turn_servers:\n    - host: localhost\n      port: 3478\n      secret: test-secret\n{protocol}"
        );
        let config = parse(&["--config-body", &yaml], &Env::default());
        assert_eq!(config.server.rtc.turn_servers[0].protocol, expected);
    }
}

#[test]
fn config_consumption_audio_room_limits_propagate_to_server_config() {
    // 1. audio.active_level / min_percentile / smooth_intervals via CLI
    let config = parse(
        &[
            "--audio.active_level=80",
            "--audio.min_percentile=90",
            "--audio.smooth_intervals=6",
        ],
        &Env::default(),
    );
    // Official values
    assert_eq!(config.official.audio.active_level, 80);
    assert_eq!(config.official.audio.min_percentile, 90);
    assert_eq!(config.official.audio.smooth_intervals, 6);
    // Normalized server config propagation
    assert_eq!(config.server.audio.active_level, 80);
    assert_eq!(config.server.audio.min_percentile, 90);
    assert_eq!(config.server.audio.smooth_intervals, 6);

    // 2. audio.active_level / min_percentile / smooth_intervals via YAML
    let config = parse(
        &[
            "--config-body",
            "audio:\n  active_level: 50\n  min_percentile: 60\n  smooth_intervals: 4\n",
        ],
        &Env::default(),
    );
    assert_eq!(config.server.audio.active_level, 50);
    assert_eq!(config.server.audio.min_percentile, 60);
    assert_eq!(config.server.audio.smooth_intervals, 4);

    // 3. room.playout_delay.enabled → server.room.playout_delay projection
    let config = parse(
        &[
            "--config-body",
            "room:\n  playout_delay:\n    enabled: true\n    min: 100\n    max: 400\n",
        ],
        &Env::default(),
    );
    assert!(config.official.room.playout_delay.enabled);
    assert_eq!(config.server.room.playout_delay, Some((100, 400)));

    // playout_delay.enabled=false → None projection
    let config = parse(
        &[
            "--config-body",
            "room:\n  playout_delay:\n    enabled: false\n    min: 100\n    max: 400\n",
        ],
        &Env::default(),
    );
    assert!(!config.official.room.playout_delay.enabled);
    assert_eq!(config.server.room.playout_delay, None);

    // 4-7. Combined: limit flags → server.limits + RoomPolicy derivation
    let config = parse(
        &[
            "--limit.max_metadata_size=1024",
            "--limit.max_room_name_length=512",
            "--limit.max_participant_identity_length=128",
            "--limit.max_participant_name_length=64",
        ],
        &Env::default(),
    );
    assert_eq!(config.server.limits.max_metadata_size, Some(1024));
    assert_eq!(config.server.limits.max_room_name_length, 512);
    assert_eq!(config.server.limits.max_participant_identity_length, 128);
    assert_eq!(config.server.limits.max_participant_name_length, 64);

    // RoomPolicy derivation from ServerConfig
    let policy = rtc_core::config::RoomPolicy::from(&config.server);
    assert_eq!(policy.max_metadata_size, Some(1024));
    assert_eq!(policy.max_participant_name_length, 64);
}

#[test]
fn participant_update_batch_signed_target_survives_yaml_cli_env_to_room_policy() {
    for target in [-1i64, 0, 1, 131072] {
        let value = target.to_string();
        for config in [
            parse(
                &[
                    "--config-body",
                    &format!("room:\n  update_batch_target_size: {target}\n"),
                ],
                &Env::default(),
            ),
            parse(
                &[&format!("--room.update_batch_target_size={target}")],
                &Env::default(),
            ),
            parse(
                &[],
                &Env::from_pairs([("LIVEKIT_ROOM_UPDATE_BATCH_TARGET_SIZE", value.as_str())]),
            ),
        ] {
            assert_eq!(config.official.room.update_batch_target_size, target);
            assert_eq!(config.server.room.update_batch_target_size, target);
            assert_eq!(
                rtc_core::config::RoomPolicy::from(&config.server).update_batch_target_size,
                target
            );
        }
    }
}

#[test]
fn codec_regression_threshold_reaches_runtime_policy_without_clamping_zero_or_negative() {
    assert_eq!(
        parse(&[], &Env::default())
            .server
            .video
            .codec_regression_threshold,
        5
    );
    for value in [i64::MIN, -1, 0, 4, 5, i64::MAX] {
        let cli = format!("--video.codec_regression_threshold={value}");
        assert_eq!(
            parse(&[&cli], &Env::default())
                .server
                .video
                .codec_regression_threshold,
            value
        );
        let yaml = format!("video:\n  codec_regression_threshold: {value}\n");
        assert_eq!(
            parse(&["--config-body", &yaml], &Env::default())
                .server
                .video
                .codec_regression_threshold,
            value
        );
        let env_value = value.to_string();
        let env = Env::from_pairs([(
            "LIVEKIT_VIDEO_CODEC_REGRESSION_THRESHOLD",
            env_value.as_str(),
        )]);
        assert_eq!(
            parse(&[], &env).server.video.codec_regression_threshold,
            value
        );
    }
}
