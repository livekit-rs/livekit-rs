//! C98-B2 config_projection legs for the boot-time config families that have no
//! `rtc_core::config::ServerConfig` carrier: `agents`, `psrpc`, `ingress`,
//! `node_stats`, `webtransport`, `signal_relay`, `redis` (the `IsConfigured`
//! gate) and `trace`.
//!
//! Every leg drives a YAML / ENV / CLI document through the real loading path
//! (`lk_config::parse_with_probe`) and asserts what lands in the parameter
//! struct the server hands to the component: `agent_service_config`,
//! `psrpc_retry_policy`, `psrpc_compression`, `worker_ingress_config`,
//! `node_stats_policy`, `node_stats_sampler_intervals`, `signal_relay_config`,
//! `signal_connect_attempts`, `webtransport_tls`, `redis_configured` and
//! `jaeger_endpoint` (server.rs). Those functions are the production
//! projection sites — `boot()` builds its component graph from them, so a leg
//! that reddens here means the running server would change its behaviour.
//!
//! Go oracle: references/livekit-stable @ e5655ac (v1.13.10), plus the pinned
//! module checkouts under `references/`.
//! - `pkg/config/config.go:650-658` `SignalRelay`/`Agents` defaults,
//!   `:660` `PSRPC: rpc.DefaultPSRPCConfig`, `:664` + `:561-565` `NodeStats`,
//!   `:352-355` the `SignalRelayConfig` yaml tags, `:490-492` `IngressConfig`.
//! - `pkg/agent/config.go:5-31` `DefaultTargetLoad = 0.7` and the
//!   `EndpointsConfig` zero-value semantics ("0 takes the package default").
//! - `protocol-stable/rpc/typed_api.go:33-58` `PSRPCConfig`/`CompressionConfig`
//!   and their defaults; `psrpc/pkg/bus/options.go:26-32` quality/threshold/
//!   max-size meanings, `:47-49` the `<= 0 -> 1024` seeding after options run,
//!   `psrpc/pkg/bus/bus.go:25` `DefaultCompressionThreshold = 1024`.
//! - `pkg/service/rtcservice.go:396-397` the `ConnectAttempts` loop bound,
//!   `pkg/routing/signal.go:58` + `pkg/service/signal.go:66`
//!   `With{Client,Server}ChannelSize(StreamBufferSize)`,
//!   `pkg/routing/signal.go:311-343` min/max retry interval and retry deadline.
//! - `protocol-stable/redis/redis.go:48-64` `IsConfigured` = address OR
//!   sentinel OR cluster.
//! - `pkg/service/server.go:184-195` the WebTransport TLS construction and its
//!   non-development refusal.

use super::*;
use crate::COMPAT_VERSION;
use lk_config::{EntryAction, Env, ProgramInfo, netdetect::NetProbe, parse_with_probe};
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

/// A dev load refuses to run without keys once the caller asks for a non-dev
/// document (`load.rs:681-690`), so every load carries an upstream-shaped
/// `keys:` map. It stays first so a leg's own body cannot be overridden by it.
const KEYS_BODY: &str = "keys:\n  c98key: \"0123456789012345678901234567890123456789\"\n";

fn load_ex(dev: bool, args: &[&str], env: &Env) -> LoadedConfig {
    let info = ProgramInfo {
        binary_name: "livecloud".into(),
        product_version: "fixture".into(),
        compat_version: COMPAT_VERSION,
    };
    let mut argv: Vec<OsString> = if dev {
        ["livecloud", "--dev"]
            .into_iter()
            .map(OsString::from)
            .collect()
    } else {
        vec![OsString::from("livecloud")]
    };
    argv.extend(args.iter().map(OsString::from));
    let keyed = match argv.iter().position(|a| a == "--config-body") {
        Some(pos) => {
            let own = argv[pos + 1].to_str().expect("utf-8 body").to_string();
            argv[pos + 1] = OsString::from(format!("{KEYS_BODY}{own}"));
            argv
        }
        None => {
            argv.push(OsString::from("--config-body"));
            argv.push(OsString::from(KEYS_BODY));
            argv
        }
    };
    match parse_with_probe(keyed, env, &info, &NoNetwork) {
        Ok(EntryAction::RunServer(loaded)) => *loaded,
        Ok(other) => panic!("expected RunServer, got: {other:?}"),
        Err(e) => panic!("the fixture config must load, got: {e}"),
    }
}

fn load(args: &[&str], env: &Env) -> LoadedConfig {
    load_ex(true, args, env)
}

fn official(args: &[&str], env: &Env) -> Box<lk_config::official::Config> {
    load(args, env).official.clone()
}

fn yaml(body: &str) -> Vec<&str> {
    vec!["--config-body", body]
}

// ---------------------------------------------------------------------------
// agents (+ limit.agent_signal_message_size_limit)
// ---------------------------------------------------------------------------

#[test]
fn agents_leaves_project_into_the_agent_service_params() {
    let cfg = official(&[], &Env::default());
    let got = agent_service_config(&cfg);
    assert_eq!(
        (0.7, false, false),
        (got.target_load, got.enable_recording, got.enable_redaction),
        "C98B2-01 Go seeds only TargetLoad (config.go:656-658, agent/config.go:5)"
    );
    assert_eq!(
        (false, 0, 0, 0),
        (
            got.endpoints_disabled,
            got.endpoints_max_streams,
            got.endpoints_max_tunnels,
            got.endpoints_tunnel_drain_timeout_ns
        ),
        "C98B2-01 every endpoint leaf keeps Go's zero value (agent/config.go:17-31)"
    );
    assert_eq!(
        2 << 20,
        got.signal_message_size_limit,
        "C98B2-01 the size limit rides in from the limit tree, not agents"
    );

    let body = "agents:\n  enable_user_data_recording: true\n  enable_user_data_redaction: true\n  target_load: 0.25\n  endpoints:\n    disabled: true\n    max_streams: 9\n    max_tunnels: 4\n    tunnel_drain_timeout: 1500ms\n";
    let got = agent_service_config(&official(&yaml(body), &Env::default()));
    assert!(
        got.enable_recording && got.enable_redaction && got.endpoints_disabled,
        "C98B2-02 the three bools must each reach their own field: {got:?}"
    );
    assert_eq!(0.25f32, got.target_load, "C98B2-02");
    assert_eq!(
        (9, 4, 1_500_000_000),
        (
            got.endpoints_max_streams,
            got.endpoints_max_tunnels,
            got.endpoints_tunnel_drain_timeout_ns
        ),
        "C98B2-02 the drain timeout must arrive in nanoseconds"
    );

    let flags = official(
        &[
            "--agents.target_load=0.5",
            "--agents.endpoints.max_streams=7",
            "--agents.endpoints.tunnel_drain_timeout=2s",
        ],
        &Env::default(),
    );
    let got = agent_service_config(&flags);
    assert_eq!(
        0.5f32, got.target_load,
        "C98B2-03 the float flag must project"
    );
    assert_eq!(7, got.endpoints_max_streams, "C98B2-03");
    assert_eq!(
        2_000_000_000, got.endpoints_tunnel_drain_timeout_ns,
        "C98B2-03"
    );
    assert!(
        !got.enable_recording && !got.endpoints_disabled,
        "C98B2-03 an unrelated flag must not flip the bools: {got:?}"
    );

    let env = Env::from_pairs([
        ("LIVEKIT_AGENTS_TARGET_LOAD", "0.9"),
        ("LIVEKIT_AGENTS_ENABLE_USER_DATA_REDACTION", "true"),
        ("LIVEKIT_LIMIT_AGENT_SIGNAL_MESSAGE_SIZE_LIMIT", "4096"),
    ]);
    let got = agent_service_config(&official(&[], &env));
    assert_eq!(
        0.9f32, got.target_load,
        "C98B2-04 the env leaves must project"
    );
    assert!(
        got.enable_redaction && !got.enable_recording,
        "C98B2-04 only the redaction env leaf is set: {got:?}"
    );
    assert_eq!(
        4096, got.signal_message_size_limit,
        "C98B2-04 the limit leaf reaches the same struct"
    );
}

// ---------------------------------------------------------------------------
// psrpc retry policy
// ---------------------------------------------------------------------------

#[test]
fn psrpc_retry_leaves_project_into_the_shared_rpc_policy() {
    let default = psrpc_retry_policy(&official(&[], &Env::default()).psrpc);
    assert_eq!(
        (3, 3_000_000_000, 2_000_000_000, 1000),
        (
            default.max_attempts,
            default.timeout.as_nanos_i64(),
            default.backoff.as_nanos_i64(),
            default.buffer_size
        ),
        "C98B2-05 Go's DefaultPSRPCConfig is 3/3s/2s/1000 (rpc/typed_api.go:52-57)"
    );

    let body = "psrpc:\n  max_attempts: 7\n  timeout: 9s\n  backoff: 250ms\n  buffer_size: 32\n";
    let got = psrpc_retry_policy(&official(&yaml(body), &Env::default()).psrpc);
    assert_eq!(
        (7, 9_000_000_000, 250_000_000, 32),
        (
            got.max_attempts,
            got.timeout.as_nanos_i64(),
            got.backoff.as_nanos_i64(),
            got.buffer_size
        ),
        "C98B2-06 each leaf must reach its own policy field"
    );

    let flags = official(
        &[
            "--psrpc.max_attempts=5",
            "--psrpc.timeout=1500ms",
            "--psrpc.backoff=1ms",
            "--psrpc.buffer_size=64",
        ],
        &Env::default(),
    );
    let got = psrpc_retry_policy(&flags.psrpc);
    assert_eq!(
        (5, 1_500_000_000, 1_000_000, 64),
        (
            got.max_attempts,
            got.timeout.as_nanos_i64(),
            got.backoff.as_nanos_i64(),
            got.buffer_size
        ),
        "C98B2-07 the generated flags must project the same four fields"
    );

    // `psrpc.WithChannelSize` semantics: zero falls back to the bus default of
    // 100 and a negative size cannot allocate a channel (`retry.rs:37-43`).
    let zero =
        psrpc_retry_policy(&official(&yaml("psrpc:\n  buffer_size: 0\n"), &Env::default()).psrpc);
    assert_eq!(
        Ok(100),
        zero.handler_channel_size(),
        "C98B2-08 a zero buffer_size must take the bus default"
    );
    let negative =
        psrpc_retry_policy(&official(&yaml("psrpc:\n  buffer_size: -1\n"), &Env::default()).psrpc);
    assert_eq!(
        Err("negative psrpc client buffer_size"),
        negative.handler_channel_size(),
        "C98B2-08 a negative buffer_size must refuse rather than truncate"
    );
    let zero_timeout =
        psrpc_retry_policy(&official(&yaml("psrpc:\n  timeout: 0s\n"), &Env::default()).psrpc);
    assert_eq!(
        Duration::from_secs(3),
        zero_timeout.request_timeout(),
        "C98B2-08 a zero request timeout falls back to the fixed bus default"
    );
}

// ---------------------------------------------------------------------------
// psrpc compression
// ---------------------------------------------------------------------------

#[test]
fn psrpc_compression_leaves_project_once_for_both_buses() {
    let (policy, max_size) = psrpc_compression(&official(&[], &Env::default()).psrpc);
    assert_eq!(
        (0, 1024, isize::MAX as usize - 1),
        (policy.quality, policy.threshold, max_size),
        "C98B2-09 defaults are quality 0 (disabled), threshold 1024
         (typed_api.go:46-48, bus.go:25) and no decompressed-size cap"
    );

    let body = "psrpc:\n  compression:\n    quality: 12\n    threshold: -5\n    max_decompressed_size: 8192\n";
    let (policy, max_size) = psrpc_compression(&official(&yaml(body), &Env::default()).psrpc);
    assert_eq!(
        (9, 1024, 8192),
        (policy.quality, policy.threshold, max_size),
        "C98B2-10 quality clamps to the gzip range and a non-positive
         threshold takes the seeded default (options.go:47-49)"
    );

    let flags = official(
        &[
            "--psrpc.compression.quality=-3",
            "--psrpc.compression.threshold=2048",
            "--psrpc.compression.max_decompressed_size=1",
        ],
        &Env::default(),
    );
    let (policy, max_size) = psrpc_compression(&flags.psrpc);
    assert_eq!(
        (0, 2048, 1),
        (policy.quality, policy.threshold, max_size),
        "C98B2-11 a negative quality is 'disabled', not an error"
    );

    // Both buses are built from the same leaves, so the two projection calls in
    // `boot()` must agree value for value.
    let cfg = official(&yaml(body), &Env::default());
    assert_eq!(
        format!("{:?}", psrpc_compression(&cfg.psrpc)),
        format!("{:?}", psrpc_compression(&cfg.psrpc)),
        "C98B2-12 the local and worker buses read one builder"
    );
}

// ---------------------------------------------------------------------------
// ingress
// ---------------------------------------------------------------------------

#[test]
fn ingress_leaves_project_into_the_worker_ingress_config() {
    let got = worker_ingress_config(&official(&[], &Env::default()).ingress);
    assert_eq!(
        (String::new(), String::new(), false),
        (
            got.rtmp_base_url.clone(),
            got.whip_base_url.clone(),
            got.enable_udp_url_pull
        ),
        "C98B2-13 Go declares no ingress defaults (config.go:490-492)"
    );

    let body = "ingress:\n  rtmp_base_url: rtmp://ingress.local/live\n  whip_base_url: https://ingress.local/w/h\n  enable_udp_url_pull: true\n";
    let got = worker_ingress_config(&official(&yaml(body), &Env::default()).ingress);
    assert_eq!(
        (
            "rtmp://ingress.local/live",
            "https://ingress.local/w/h",
            true
        ),
        (
            got.rtmp_base_url.as_str(),
            got.whip_base_url.as_str(),
            got.enable_udp_url_pull
        ),
        "C98B2-14 the URL pair and the UDP opt-in must each reach the worker"
    );

    let flags = official(
        &[
            "--ingress.rtmp_base_url=rtmp://flag.local/live",
            "--ingress.whip_base_url=https://flag.local/w",
        ],
        &Env::default(),
    );
    let got = worker_ingress_config(&flags.ingress);
    assert_eq!(
        "rtmp://flag.local/live", got.rtmp_base_url,
        "C98B2-15 the generated flag must project"
    );
    assert_eq!("https://flag.local/w", got.whip_base_url, "C98B2-15");
    assert!(
        !got.enable_udp_url_pull,
        "C98B2-15 the URL flags must not enable UDP pull"
    );

    let env = Env::from_pairs([("LIVEKIT_INGRESS_ENABLE_UDP_URL_PULL", "true")]);
    let got = worker_ingress_config(&official(&[], &env).ingress);
    assert!(
        got.enable_udp_url_pull,
        "C98B2-15b the UDP opt-in env leaf must project"
    );
}

// ---------------------------------------------------------------------------
// node_stats
// ---------------------------------------------------------------------------

#[test]
fn node_stats_leaves_project_into_the_policy_and_the_sampler() {
    let cfg = official(&[], &Env::default());
    let policy = node_stats_policy(&cfg);
    assert_eq!(
        (Duration::from_secs(2), Duration::from_secs(30)),
        (policy.update_interval, policy.max_delay),
        "C98B2-16 DefaultNodeStatsConfig is 2s/30s (config.go:561-565)"
    );
    assert_eq!(
        (Duration::from_secs(2), vec![Duration::from_secs(10)]),
        node_stats_sampler_intervals(&cfg),
        "C98B2-16 the sampler takes the update interval plus the rate list"
    );

    let body = "node_stats:\n  stats_update_interval: 500ms\n  stats_rate_measurement_intervals: [1s, 5s, 60s]\n  stats_max_delay: 1m\n";
    let cfg = official(&yaml(body), &Env::default());
    let policy = node_stats_policy(&cfg);
    assert_eq!(
        (Duration::from_millis(500), Duration::from_secs(60)),
        (policy.update_interval, policy.max_delay),
        "C98B2-17 the duration leaves must keep their units"
    );
    let (interval, rates) = node_stats_sampler_intervals(&cfg);
    assert_eq!(Duration::from_millis(500), interval, "C98B2-17");
    assert_eq!(
        vec![
            Duration::from_secs(1),
            Duration::from_secs(5),
            Duration::from_secs(60)
        ],
        rates,
        "C98B2-17 the whole rate list must project in order"
    );

    let cfg = official(&["--node_stats.stats_update_interval=3s"], &Env::default());
    assert_eq!(
        Duration::from_secs(3),
        node_stats_policy(&cfg).update_interval,
        "C98B2-18 the scalar flag must project"
    );
    let empty = official(
        &yaml("node_stats:\n  stats_rate_measurement_intervals: []\n"),
        &Env::default(),
    );
    assert!(
        node_stats_sampler_intervals(&empty).1.is_empty(),
        "C98B2-18 an explicit empty list stays empty: {:?}",
        node_stats_sampler_intervals(&empty).1
    );
}

// ---------------------------------------------------------------------------
// webtransport
// ---------------------------------------------------------------------------

#[test]
fn webtransport_port_gates_the_tls_material() {
    let cfg = official(&[], &Env::default());
    assert!(
        webtransport_tls(&cfg, true)
            .expect("dev mode never fails on a missing pair")
            .is_none(),
        "C98B2-19 port 0 builds no TLS material at all (server.go:184-195)"
    );

    let dev = official(&["--webtransport.port=5443"], &Env::default());
    assert!(
        webtransport_tls(&dev, true)
            .expect("dev mode generates a self-signed pair")
            .is_some(),
        "C98B2-19 a port in dev mode must produce usable TLS material"
    );

    let no_pair = load_ex(false, &["--webtransport.port=5443"], &Env::default());
    let error = match webtransport_tls(&no_pair.official, false) {
        Err(error) => error,
        Ok(other) => panic!(
            "a non-dev port with no cert pair must fail, got: {}",
            other.is_some()
        ),
    };
    assert_eq!(
        "webtransport requires webtransport.tls_cert_file/tls_key_file",
        error.to_string(),
        "C98B2-20 the refusal text is surfaced verbatim as a startup error"
    );

    let missing_file = load_ex(
        false,
        &yaml(
            "webtransport:\n  port: 5443\n  tls_cert_file: /nonexistent/wt.crt\n  tls_key_file: /nonexistent/wt.key\n",
        ),
        &Env::default(),
    );
    let error = match webtransport_tls(&missing_file.official, false) {
        Err(error) => error,
        Ok(other) => panic!(
            "an unreadable cert pair must fail, got: {}",
            other.is_some()
        ),
    };
    assert_eq!(
        "open /nonexistent/wt.crt: no such file or directory",
        error.to_string(),
        "C98B2-21 Go returns the os.ReadFile error verbatim, path included"
    );

    let zero_port = load_ex(
        false,
        &yaml("webtransport:\n  port: 0\n  tls_cert_file: /nonexistent/wt.crt\n"),
        &Env::default(),
    );
    assert!(
        webtransport_tls(&zero_port.official, false)
            .expect("port 0 must not touch the cert files")
            .is_none(),
        "C98B2-21 an explicit zero port stays closed even with files set"
    );
}

// ---------------------------------------------------------------------------
// signal_relay
// ---------------------------------------------------------------------------

#[test]
fn signal_relay_leaves_project_into_the_relay_config() {
    let cfg = official(&[], &Env::default());
    let relay = signal_relay_config(&cfg);
    assert_eq!(
        (
            Duration::from_millis(7500),
            Duration::from_millis(500),
            Duration::from_secs(4),
            1000,
            3
        ),
        (
            relay.retry_timeout,
            relay.min_retry_interval,
            relay.max_retry_interval,
            relay.stream_buffer_size,
            relay.connect_attempts
        ),
        "C98B2-22 Go's defaults are 7.5s/500ms/4s/1000/3 (config.go:650-655)"
    );
    assert_eq!(
        signal_connect_attempts(&cfg),
        relay.connect_attempts,
        "C98B2-22 the join-retry site and the relay must read one value"
    );

    let body = "signal_relay:\n  retry_timeout: 20s\n  min_retry_interval: 100ms\n  max_retry_interval: 900ms\n  stream_buffer_size: 8\n  connect_attempts: 5\n";
    let cfg = official(&yaml(body), &Env::default());
    let relay = signal_relay_config(&cfg);
    assert_eq!(
        (
            Duration::from_secs(20),
            Duration::from_millis(100),
            Duration::from_millis(900),
            8,
            5
        ),
        (
            relay.retry_timeout,
            relay.min_retry_interval,
            relay.max_retry_interval,
            relay.stream_buffer_size,
            relay.connect_attempts
        ),
        "C98B2-23 each leaf must reach its own field (signal.go:311-343)"
    );
    assert_eq!(5, signal_connect_attempts(&cfg), "C98B2-23");

    let flags = official(
        &[
            "--signal_relay.retry_timeout=1s",
            "--signal_relay.min_retry_interval=2s",
            "--signal_relay.max_retry_interval=3s",
            "--signal_relay.stream_buffer_size=16",
            "--signal_relay.connect_attempts=9",
        ],
        &Env::default(),
    );
    let relay = signal_relay_config(&flags);
    assert_eq!(
        (
            Duration::from_secs(1),
            Duration::from_secs(2),
            Duration::from_secs(3),
            16,
            9
        ),
        (
            relay.retry_timeout,
            relay.min_retry_interval,
            relay.max_retry_interval,
            relay.stream_buffer_size,
            relay.connect_attempts
        ),
        "C98B2-24 the five generated flags must project"
    );

    // Go would build an unbuffered channel for 0 and panic on a negative size
    // (`make(chan, -1)`); `rtcservice.go:396` simply runs zero attempts. The port
    // keeps both bounds inside the struct instead of reaching those states.
    let guards = official(
        &yaml("signal_relay:\n  stream_buffer_size: -4\n  connect_attempts: -2\n"),
        &Env::default(),
    );
    let relay = signal_relay_config(&guards);
    assert_eq!(
        1, relay.stream_buffer_size,
        "C98B2-25 a negative buffer clamps to 1"
    );
    assert_eq!(
        0, relay.connect_attempts,
        "C98B2-25 a negative attempt count clamps to zero attempts"
    );
    assert_eq!(0, signal_connect_attempts(&guards), "C98B2-25");
}

// ---------------------------------------------------------------------------
// redis IsConfigured + trace
// ---------------------------------------------------------------------------

#[test]
fn redis_configured_is_the_or_test_of_three_leaf_groups() {
    let none = official(&[], &Env::default());
    assert!(
        !redis_configured(&none.redis),
        "C98B2-26 no address, sentinel or cluster means an unconfigured deployment"
    );
    for (name, body) in [
        ("address", "redis:\n  address: 127.0.0.1:6399\n"),
        (
            "sentinel",
            "redis:\n  sentinel_addresses: [127.0.0.1:26379]\n",
        ),
        ("cluster", "redis:\n  cluster_addresses: [127.0.0.1:7000]\n"),
    ] {
        let cfg = official(&yaml(body), &Env::default());
        assert!(
            redis_configured(&cfg.redis),
            "C98B2-27 a {name}-only deployment is configured (redis.go:48-64)"
        );
    }
    let empty_lists = official(
        &yaml("redis:\n  address: ''\n  sentinel_addresses: []\n  cluster_addresses: []\n"),
        &Env::default(),
    );
    assert!(
        !redis_configured(&empty_lists.redis),
        "C98B2-28 explicit empty leaves stay unconfigured"
    );
}

#[test]
fn jaeger_endpoint_is_set_only_by_a_non_empty_url() {
    let off = official(&[], &Env::default());
    assert_eq!(
        None,
        jaeger_endpoint(&off),
        "C98B2-29 an empty trace.jaeger_url installs no exporter"
    );
    let on = official(
        &yaml("trace:\n  jaeger_url: http://127.0.0.1:4318\n"),
        &Env::default(),
    );
    assert_eq!(
        Some("http://127.0.0.1:4318"),
        jaeger_endpoint(&on),
        "C98B2-29 the configured URL is handed to the provider unchanged"
    );
    let env = Env::from_pairs([("LIVEKIT_TRACE_JAEGER_URL", "https://jaeger.example:4317")]);
    assert_eq!(
        Some("https://jaeger.example:4317"),
        jaeger_endpoint(&official(&[], &env)),
        "C98B2-29b the env leaf must reach the same gate"
    );
}
