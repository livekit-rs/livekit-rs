//! C98-C config_projection legs for the `rtc` / `room` / `audio` / `video` / `turn` / `limit` /
//! `api` / `key_file` leaves that have a real `rtc_core::config` carrier.
//!
//! Tier 2 only (SPEC-0075 §2): every leg drives the public loading API (`parse_with_probe`) and
//! asserts the **projected** `ServerConfig` that the server consumes (`normalize.rs:419-880`),
//! never the parsed official tree, and never only the absence of an error. Each leg sets one
//! leaf to a value the default cannot already have and pins a sibling leaf in the same struct so
//! a crosswire projection reddens the leg.
//!
//! Go oracle: references/livekit-stable @ e5655ac (v1.13.10).
//! - `pkg/config/config.go:120-215` (RTCConfig), :236-260 (RoomConfig), :262-283 (Audio/Video),
//!   :296-320 (TURNConfig), :186-215 (LimitConfig), :325-340 (APIConfig).
//! - `pkg/config/config.go:943-1050` GenerateCLIFlags: a scalar leaf gets a dotted-path flag and
//!   a `LIVEKIT_*` env; maps/slices get neither, which is why `webhook.filter_params`,
//!   `node_stats.stats_rate_measurement_intervals` and `video.stream_tracker_manager.*`
//!   (maps) are pinned as YAML-only below.

use lk_config::{
    EntryAction, Env, ProgramInfo, generated::specs, netdetect::NetProbe, parse_with_probe,
};
use rtc_core::config::{ServerConfig, UdpMode};
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

/// `Mode::Run` needs keys (load.rs:689); `--dev` takes the upstream placeholder-key path
/// (load.rs:363) so no leg depends on a key file on disk.
/// The same fixture with a resolvable external IP, for the leaves whose projection depends on
/// upstream's STUN discovery path (`rtc.use_external_ip`).
struct WithExternal;
impl NetProbe for WithExternal {
    fn local_addresses(&self) -> Vec<IpAddr> {
        vec!["127.0.0.1".parse().expect("fixture loopback")]
    }
    fn external_ip(&self, _: &[String], _: bool) -> Result<IpAddr, String> {
        Ok("203.0.113.99".parse().expect("fixture external"))
    }
    fn sleep(&self, _: Duration) {
        panic!("fixture must not retry networking")
    }
}

fn load(args: &[&str], env: &Env) -> ServerConfig {
    parse(args, env, &NoNetwork)
}

fn load_ext(args: &[&str], env: &Env) -> ServerConfig {
    parse(args, env, &WithExternal)
}

fn parse(args: &[&str], env: &Env, probe: &dyn NetProbe) -> ServerConfig {
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
    match parse_with_probe(argv, env, &info, probe) {
        Ok(EntryAction::RunServer(loaded)) => loaded.server,
        Ok(other) => panic!("expected RunServer, got: {other:?}"),
        Err(e) => panic!("parse_with_probe error: {e}"),
    }
}

/// The verbatim usage/error text for the arms whose correct outcome is not a server config.
fn outcome_text(args: &[&str]) -> String {
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
    match parse_with_probe(argv, &Env::default(), &info, &NoNetwork) {
        Ok(EntryAction::RunServer(_)) => panic!("this arm must not produce a server config"),
        Ok(EntryAction::PrintHelp { text, .. }) => text,
        Ok(other) => format!("{other:?}"),
        Err(e) => e.to_string(),
    }
}

fn warnings(args: &[&str], env: &Env) -> Vec<String> {
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
    match parse_with_probe(argv, env, &info, &NoNetwork) {
        Ok(EntryAction::RunServer(loaded)) => loaded.warnings,
        Ok(other) => panic!("expected RunServer, got: {other:?}"),
        Err(e) => panic!("parse_with_probe error: {e}"),
    }
}

fn yaml(body: &str) -> Vec<&str> {
    vec!["--config-body", body]
}

// ---------------------------------------------------------------------------
// generated_flags:--rtc.udp_port.start / .end  (YAML:rtc.udp_port.{start,end})
//
// `rtc.udp_port` is a struct of two Int leaves, so Go gives each a flag. The projection is
// `RtcConfig::udp` (normalize.rs:440-478): a valid range becomes `UdpMode::Range`, otherwise the
// mux ports are used, and the range is what re-enables `media_batch_io_enabled`.
// ---------------------------------------------------------------------------

#[test]
fn rtc_udp_port_range_projects_into_the_udp_mode_and_gates_batch_io() {
    let base = load(&[], &Env::default());
    assert!(
        matches!(base.rtc.udp, UdpMode::Mux { .. }),
        "C98C-01 the default has no ephemeral range: {:?}",
        base.rtc.udp
    );
    assert!(
        !base.rtc.media_batch_io_enabled,
        "C98C-01 batch IO is off by default because the batch size is 0 (normalize.rs:475-478)"
    );

    let mut args = yaml("rtc:\n  udp_port: \"50000-50010\"\n");
    args.push("--rtc.batch_io.batch_size=4");
    let got = load(&args, &Env::default());
    // `rtc.udp_port` is the mux range (normalize.rs:447-456); the legacy
    // `rtc.port_range_*` pair is what becomes `UdpMode::Range`.
    match &got.rtc.udp {
        UdpMode::Mux { ports } => {
            assert_eq!(
                Some(&50000),
                ports.first(),
                "C98C-02 the mux list must start at the configured port: {ports:?}"
            );
            assert!(
                (1..=11).contains(&ports.len()),
                "C98C-02 the list is capped by the CPU count, got {ports:?}"
            );
            assert_eq!(
                (50000..50000 + ports.len() as u16).collect::<Vec<_>>(),
                *ports,
                "C98C-02 the mux list must be the consecutive range from start"
            );
        }
        other => panic!("C98C-02 rtc.udp_port must project as UdpMode::Mux, got {other:?}"),
    }
    assert!(
        got.rtc.media_batch_io_enabled,
        "C98C-02 a range plus batch size enables batch IO: {:?}",
        got.rtc.udp
    );

    let flags = load(
        &["--rtc.udp_port.start=51000", "--rtc.udp_port.end=51005"],
        &Env::default(),
    );
    match &flags.rtc.udp {
        UdpMode::Mux { ports } => assert_eq!(
            vec![51000, 51001, 51002, 51003, 51004, 51005],
            *ports,
            "C98C-03 both flags must project into the mux list"
        ),
        other => panic!("C98C-03 the flag pair must project as UdpMode::Mux, got {other:?}"),
    }

    let one_sided = load(&["--rtc.udp_port.start=51000"], &Env::default());
    match &one_sided.rtc.udp {
        UdpMode::Mux { ports } => assert_eq!(
            vec![51000],
            *ports,
            "C98C-04 an end of 0 iterates as start..=start (ports.rs:63-69), got {ports:?}"
        ),
        other => panic!("C98C-04 a start alone is still a valid range, got {other:?}"),
    }
    assert!(
        !one_sided.rtc.media_batch_io_enabled,
        "C98C-04 batch IO needs an explicit batch size, not just a range"
    );
    let legacy = load(
        &["--rtc.port_range_start=52000", "--rtc.port_range_end=52004"],
        &Env::default(),
    );
    match &legacy.rtc.udp {
        UdpMode::Range { start, end } => {
            assert_eq!(
                (52000, 52004),
                (*start, *end),
                "C98C-05 the legacy pair is the range mode"
            )
        }
        other => panic!("C98C-05 rtc.port_range_* must project as UdpMode::Range, got {other:?}"),
    }
    assert!(
        !legacy.rtc.media_batch_io_enabled,
        "C98C-05 an ephemeral ICE range disables batch IO (normalize.rs:471-478)"
    );
    // The third Go condition is `UDPPort.Valid()` (`roommanager.go:546-548`). A
    // start-only legacy range keeps step 6 from defaulting `rtc.udp_port`
    // (load.rs:196-204 tests `port_range_start == 0`), so no mux port is left to
    // batch over: the mode is Disabled and a positive batch size must not
    // enable it.
    let start_only_args = [
        "--rtc.port_range_start=52000",
        "--rtc.batch_io.batch_size=4",
    ];
    let start_only = load(&start_only_args, &Env::default());
    assert!(
        matches!(start_only.rtc.udp, UdpMode::Disabled),
        "C98C-05b a start-only legacy range leaves no mux ports: {:?}",
        start_only.rtc.udp
    );
    assert!(
        !start_only.rtc.media_batch_io_enabled,
        "C98C-05c batch IO also requires a valid rtc.udp_port, not only a batch size"
    );
    assert!(
        warnings(&["--rtc.port_range_start=52000"], &Env::default())
            .iter()
            .any(|w| w.contains("WebRTC over UDP is disabled")),
        "C98C-05d the disabled UDP mode must warn"
    );
}

// ---------------------------------------------------------------------------
// generated_flags:--rtc.use_stun_port_as_ice / --rtc.use_external_ip /
// --rtc.external_ip_only / --rtc.enable_loopback_candidate / --rtc.use_mdns / --rtc.enable_warp
// ---------------------------------------------------------------------------

#[test]
fn rtc_bool_flags_project_into_their_own_rtc_slot() {
    let base = load(&[], &Env::default());
    for (name, val) in [
        ("use_stun_port_as_ice", base.rtc.use_stun_port_as_ice),
        ("use_external_ip", base.rtc.use_external_ip),
        ("external_ip_only", base.rtc.external_ip_only),
        (
            "enable_loopback_candidate",
            base.rtc.enable_loopback_candidate,
        ),
        ("use_mdns", base.rtc.use_mdns),
        ("enable_warp", base.rtc.enable_warp),
    ] {
        assert!(
            !val,
            "C98C-05 the Go default of rtc.{name} is false, got {val}"
        );
    }

    for (flag, read) in [
        (
            "--rtc.use_stun_port_as_ice=true",
            "use_stun_port_as_ice" as &str,
        ),
        ("--rtc.external_ip_only=true", "external_ip_only"),
        (
            "--rtc.enable_loopback_candidate=true",
            "enable_loopback_candidate",
        ),
        ("--rtc.use_mdns=true", "use_mdns"),
        ("--rtc.enable_warp=true", "enable_warp"),
    ] {
        let got = load(&[flag], &Env::default());
        let value = match read {
            "use_stun_port_as_ice" => got.rtc.use_stun_port_as_ice,
            "use_external_ip" => got.rtc.use_external_ip,
            "external_ip_only" => got.rtc.external_ip_only,
            "enable_loopback_candidate" => got.rtc.enable_loopback_candidate,
            "use_mdns" => got.rtc.use_mdns,
            "enable_warp" => got.rtc.enable_warp,
            other => unreachable!("{other}"),
        };
        assert!(value, "C98C-06 {flag} must project rtc.{read}, got false");
        let others: Vec<(&str, bool)> = vec![
            ("use_stun_port_as_ice", got.rtc.use_stun_port_as_ice),
            ("use_external_ip", got.rtc.use_external_ip),
            ("external_ip_only", got.rtc.external_ip_only),
            (
                "enable_loopback_candidate",
                got.rtc.enable_loopback_candidate,
            ),
            ("use_mdns", got.rtc.use_mdns),
            ("enable_warp", got.rtc.enable_warp),
        ]
        .into_iter()
        .filter(|(n, _)| *n != read)
        .filter(|(_, v)| *v)
        .collect();
        assert!(
            others.is_empty(),
            "C98C-06 {flag} must not set the sibling bools, also true: {others:?}"
        );
    }

    let ext = load_ext(&["--rtc.use_external_ip=true"], &Env::default());
    assert!(
        ext.rtc.use_external_ip,
        "C98C-06 the external-IP flag projects"
    );

    let env = load(
        &[],
        &Env::from_pairs([
            ("LIVEKIT_RTC_USE_MDNS", "true"),
            ("LIVEKIT_RTC_ENABLE_WARP", "true"),
        ]),
    );
    assert!(
        env.rtc.use_mdns && env.rtc.enable_warp,
        "C98C-07 the generated env names must project"
    );
    assert!(
        !env.rtc.use_external_ip && !env.rtc.external_ip_only,
        "C98C-07 env must not leak across bool leaves"
    );
}

#[test]
fn rtc_use_external_ip_also_drives_the_node_ip_mapping_gate() {
    let base = load_ext(&[], &Env::default());
    assert!(
        !base.rtc.use_node_ip_mapping,
        "C98C-08 no node IP means no 1:1 NAT mapping"
    );
    let got = load_ext(
        &[
            "--rtc.node_ip.ipv4=203.0.113.7",
            "--rtc.use_external_ip=true",
        ],
        &Env::default(),
    );
    assert!(
        got.rtc.use_node_ip_mapping,
        "C98C-08 normalize.rs:593-595 gates the mapping on a node IP plus use_external_ip"
    );
    let ip_only = load_ext(&["--rtc.node_ip.ipv4=203.0.113.7"], &Env::default());
    assert!(
        ip_only.rtc.use_node_ip_mapping,
        "C98C-09 an explicitly configured node IP enables the mapping on its own"
    );
}

// ---------------------------------------------------------------------------
// generated_flags:--rtc.node_ip.ipv4 / .ipv6
// ---------------------------------------------------------------------------

#[test]
fn rtc_node_ip_leaves_project_per_family_and_reject_the_wrong_family() {
    let base = load(&[], &Env::default());
    assert_eq!(
        Some(std::net::Ipv4Addr::LOCALHOST),
        base.rtc.node_ip.v4,
        "C98C-10 with no leaf set, the auto-discovered loopback fills v4 (normalize.rs:490-498)"
    );
    assert_eq!(None, base.rtc.node_ip.v6, "C98C-10 no v6 by default");

    let got = load(
        &[
            "--rtc.node_ip.ipv4=203.0.113.7",
            "--rtc.node_ip.ipv6=2001:db8::1",
        ],
        &Env::default(),
    );
    assert_eq!(
        Some(
            "203.0.113.7"
                .parse::<std::net::Ipv4Addr>()
                .expect("fixture v4")
        ),
        got.rtc.node_ip.v4,
        "C98C-11 the v4 leaf must project an Ipv4Addr"
    );
    assert_eq!(
        Some(
            "2001:db8::1"
                .parse::<std::net::Ipv6Addr>()
                .expect("fixture v6")
        ),
        got.rtc.node_ip.v6,
        "C98C-11 the v6 leaf must project an Ipv6Addr"
    );

    let swapped = load(&["--rtc.node_ip.ipv4=2001:db8::1"], &Env::default());
    assert_eq!(
        None, swapped.rtc.node_ip.v4,
        "C98C-12 a v6 in the v4 leaf is dropped, not coerced (normalize.rs:490-498)"
    );
    // Go classifies a node IP by `ip.To4() != nil` (`rtcconfig/config.go:249`),
    // so an IPv4-mapped literal belongs to the v4 slot. The YAML/base path
    // already normalises it there (`ports.rs:112-140`); a dotted
    // `rtc.node_ip.ipv6` flag bypasses that unmarshal, so the projection must
    // repeat the classification instead of dropping the address.
    let mapped = load(&["--rtc.node_ip.ipv6=::ffff:198.51.100.7"], &Env::default());
    assert_eq!(
        Some(
            "198.51.100.7"
                .parse::<std::net::Ipv4Addr>()
                .expect("fixture mapped v4")
        ),
        mapped.rtc.node_ip.v4,
        "C98C-12b an IPv4-mapped v6 leaf must project into the v4 slot"
    );
    assert_eq!(
        None, mapped.rtc.node_ip.v6,
        "C98C-12b the mapped literal must not also become a v6 rule: {:?}",
        mapped.rtc.node_ip.v6
    );
    let conflict = load(
        &[
            "--rtc.node_ip.ipv4=203.0.113.7",
            "--rtc.node_ip.ipv6=::ffff:198.51.100.7",
        ],
        &Env::default(),
    );
    assert_eq!(
        Some(
            "203.0.113.7"
                .parse::<std::net::Ipv4Addr>()
                .expect("fixture v4")
        ),
        conflict.rtc.node_ip.v4,
        "C98C-12c an explicit v4 leaf keeps its slot against a mapped v6"
    );
    assert_eq!(
        None, conflict.rtc.node_ip.v6,
        "C98C-12c the mapped v6 must not claim the v6 slot"
    );
    let yaml_mapped = load(
        &yaml("rtc:\n  node_ip: \"::ffff:198.51.100.8\"\n"),
        &Env::default(),
    );
    assert_eq!(
        Some(
            "198.51.100.8"
                .parse::<std::net::Ipv4Addr>()
                .expect("fixture yaml mapped v4")
        ),
        yaml_mapped.rtc.node_ip.v4,
        "C98C-12d the YAML scalar path normalises a mapped literal into v4 like Go"
    );
    assert_eq!(
        None, yaml_mapped.rtc.node_ip.v6,
        "C98C-12d the YAML scalar path leaves v6 empty"
    );
    let env = load(
        &[],
        &Env::from_pairs([("LIVEKIT_RTC_NODE_IP_IPV4", "198.51.100.9")]),
    );
    assert_eq!(
        Some(
            "198.51.100.9"
                .parse::<std::net::Ipv4Addr>()
                .expect("fixture env v4")
        ),
        env.rtc.node_ip.v4,
        "C98C-13 the env leaf must project the same way as the flag"
    );
}

// ---------------------------------------------------------------------------
// generated_flags:--rtc.sctp_min_cwnd / .sctp_fast_rtx_wnd / .sctp_cwnd_ca_step /
// --rtc.datachannel_data_track_target_latency
// ---------------------------------------------------------------------------

#[test]
fn rtc_sctp_scalars_project_into_the_sctp_struct_without_crosswire() {
    let base = load(&[], &Env::default());
    assert_eq!(
        (0, 0, 0),
        (
            base.rtc.sctp.min_cwnd,
            base.rtc.sctp.fast_rtx_wnd,
            base.rtc.sctp.cwnd_ca_step
        ),
        "C98C-14 Go defaults are zero"
    );

    let got = load(
        &[
            "--rtc.sctp_min_cwnd=11",
            "--rtc.sctp_fast_rtx_wnd=12",
            "--rtc.sctp_cwnd_ca_step=13",
        ],
        &Env::default(),
    );
    assert_eq!(11, got.rtc.sctp.min_cwnd, "C98C-15 min_cwnd");
    assert_eq!(12, got.rtc.sctp.fast_rtx_wnd, "C98C-15 fast_rtx_wnd");
    assert_eq!(13, got.rtc.sctp.cwnd_ca_step, "C98C-15 cwnd_ca_step");

    let single = load(&["--rtc.sctp_fast_rtx_wnd=21"], &Env::default());
    assert_eq!(
        (0, 21, 0),
        (
            single.rtc.sctp.min_cwnd,
            single.rtc.sctp.fast_rtx_wnd,
            single.rtc.sctp.cwnd_ca_step
        ),
        "C98C-16 one flag must not move the other two"
    );
    let env = load(
        &[],
        &Env::from_pairs([("LIVEKIT_RTC_SCTP_CWND_CA_STEP", "31")]),
    );
    assert_eq!(31, env.rtc.sctp.cwnd_ca_step, "C98C-17 env must project");
}

#[test]
fn rtc_datachannel_data_track_target_latency_projects_as_a_policy_duration() {
    let base = load(&[], &Env::default());
    assert_eq!(
        100_000_000,
        base.rtc.data_track_target_latency.as_nanos_i64(),
        "C98C-18 the Go default is 100ms"
    );
    let got = load(
        &["--rtc.datachannel_data_track_target_latency=1500ms"],
        &Env::default(),
    );
    assert_eq!(
        1_500_000_000,
        got.rtc.data_track_target_latency.as_nanos_i64(),
        "C98C-19 the duration flag must project into the policy duration"
    );
    assert_eq!(
        base.rtc.data_channel_lossy_target_latency.as_nanos_i64(),
        got.rtc.data_channel_lossy_target_latency.as_nanos_i64(),
        "C98C-19 the sibling lossy latency must not move"
    );
    let env = load(
        &[],
        &Env::from_pairs([("LIVEKIT_RTC_DATACHANNEL_DATA_TRACK_TARGET_LATENCY", "250ms")]),
    );
    assert_eq!(
        250_000_000,
        env.rtc.data_track_target_latency.as_nanos_i64(),
        "C98C-20 env must project"
    );
}

// ---------------------------------------------------------------------------
// generated_flags:--rtc.reconnect_on_{publication,subscription,data_channel}_error
//
// These are `Option<bool>` in the official tree (the Go flag has no DefaultText at all, which is
// why specs() shows an empty default) and the projection flattens None -> false.
// ---------------------------------------------------------------------------

#[test]
fn rtc_reconnect_flags_project_per_error_kind() {
    let base = load(&[], &Env::default());
    assert!(
        !base.rtc.reconnect_on_publication_error
            && !base.rtc.reconnect_on_subscription_error
            && !base.rtc.reconnect_on_data_channel_error,
        "C98C-21 unset means no reconnect for any kind"
    );

    let one = load(
        &["--rtc.reconnect_on_subscription_error=true"],
        &Env::default(),
    );
    assert!(
        one.rtc.reconnect_on_subscription_error,
        "C98C-22 the flagged kind must project"
    );
    assert!(
        !one.rtc.reconnect_on_publication_error && !one.rtc.reconnect_on_data_channel_error,
        "C98C-22 the other two kinds must stay off"
    );

    let explicit_false = load(
        &[
            "--rtc.reconnect_on_publication_error=true",
            "--rtc.reconnect_on_publication_error=false",
        ],
        &Env::default(),
    );
    assert!(
        !explicit_false.rtc.reconnect_on_publication_error,
        "C98C-23 the last CLI value wins, it does not OR into true"
    );

    let mixed = load(
        &[
            "--rtc.reconnect_on_publication_error=true",
            "--rtc.reconnect_on_data_channel_error=false",
        ],
        &Env::default(),
    );
    assert!(
        mixed.rtc.reconnect_on_publication_error && !mixed.rtc.reconnect_on_data_channel_error,
        "C98C-24 each kind keeps its own value: {:?}",
        (
            mixed.rtc.reconnect_on_publication_error,
            mixed.rtc.reconnect_on_data_channel_error
        )
    );
    let env = load(
        &[],
        &Env::from_pairs([("LIVEKIT_RTC_RECONNECT_ON_DATA_CHANNEL_ERROR", "true")]),
    );
    assert!(
        env.rtc.reconnect_on_data_channel_error,
        "C98C-25 env must project the data-channel kind"
    );
    let yaml_on = load(
        &yaml("rtc:\n  reconnect_on_publication_error: true\n"),
        &Env::default(),
    );
    assert!(
        yaml_on.rtc.reconnect_on_publication_error,
        "C98C-26 YAML must project the same field"
    );
}

// ---------------------------------------------------------------------------
// YAML:rtc (section parent)
// ---------------------------------------------------------------------------

#[test]
fn yaml_rtc_section_projects_each_leaf_into_its_own_carrier() {
    let body = "rtc:\n  use_ice_lite: true\n  use_external_ip: true\n  use_mdns: true\n  external_ip_only: true\n  enable_loopback_candidate: true\n  enable_warp: true\n  use_stun_port_as_ice: true\n  sctp_min_cwnd: 3\n  datachannel_data_track_target_latency: 900ms\n";
    let got = load_ext(&yaml(body), &Env::default());
    assert!(got.rtc.use_ice_lite, "C98C-27 use_ice_lite");
    assert!(got.rtc.use_external_ip, "C98C-27 use_external_ip");
    assert!(got.rtc.use_mdns, "C98C-27 use_mdns");
    assert!(got.rtc.external_ip_only, "C98C-27 external_ip_only");
    assert!(
        got.rtc.enable_loopback_candidate,
        "C98C-27 enable_loopback_candidate"
    );
    assert!(got.rtc.enable_warp, "C98C-27 enable_warp");
    assert!(got.rtc.use_stun_port_as_ice, "C98C-27 use_stun_port_as_ice");
    assert_eq!(3, got.rtc.sctp.min_cwnd, "C98C-27 sctp_min_cwnd");
    assert_eq!(
        900_000_000,
        got.rtc.data_track_target_latency.as_nanos_i64(),
        "C98C-27 duration inside the section must project"
    );
    assert_eq!(
        0, got.rtc.sctp.fast_rtx_wnd,
        "C98C-28 an absent sibling keeps its default inside one section"
    );
    let flag_over_yaml = load(
        &[
            "--config-body",
            "rtc:\n  use_mdns: true\n",
            "--rtc.use_mdns=false",
        ],
        &Env::default(),
    );
    assert!(
        !flag_over_yaml.rtc.use_mdns,
        "C98C-29 CLI false must beat YAML true rather than merge into it"
    );
}

// ---------------------------------------------------------------------------
// generated_flags:--room.* / YAML:room
// ---------------------------------------------------------------------------

#[test]
fn room_leaf_flags_project_into_the_room_defaults_carrier() {
    let base = load(&[], &Env::default());
    assert_eq!(0, base.room.max_participants, "C98C-30 Go default");
    assert_eq!(3, base.room.create_room_attempts, "C98C-30 Go default");
    assert_eq!(
        10_000_000_000,
        base.room.create_room_timeout.as_nanos_i64(),
        "C98C-30 Go default 10s"
    );
    assert!(
        base.room.auto_create,
        "C98C-30 dev mode turns auto_create on"
    );

    let got = load(
        &[
            "--room.max_participants=1001",
            "--room.create_room_attempts=7",
            "--room.create_room_timeout=4500ms",
        ],
        &Env::default(),
    );
    assert_eq!(1001, got.room.max_participants, "C98C-31");
    assert_eq!(7, got.room.create_room_attempts, "C98C-31");
    assert_eq!(
        4_500_000_000,
        got.room.create_room_timeout.as_nanos_i64(),
        "C98C-31"
    );
    assert_eq!(
        base.room.empty_timeout, got.room.empty_timeout,
        "C98C-32 an untouched room leaf must not move"
    );

    let one = load(&["--room.create_room_attempts=9"], &Env::default());
    assert_eq!(9, one.room.create_room_attempts, "C98C-33");
    assert_eq!(
        0, one.room.max_participants,
        "C98C-33 one flag must not move max_participants"
    );
    assert_eq!(
        10_000_000_000,
        one.room.create_room_timeout.as_nanos_i64(),
        "C98C-33 one flag must not move the timeout"
    );

    let env = load(
        &[],
        &Env::from_pairs([
            ("LIVEKIT_ROOM_MAX_PARTICIPANTS", "2002"),
            ("LIVEKIT_ROOM_CREATE_ROOM_TIMEOUT", "1500ms"),
        ]),
    );
    assert_eq!(2002, env.room.max_participants, "C98C-34 env must project");
    assert_eq!(
        1_500_000_000,
        env.room.create_room_timeout.as_nanos_i64(),
        "C98C-34 env duration must project"
    );
}

#[test]
fn room_section_projects_scalar_and_derived_limit_leaves() {
    let body = "room:\n  max_participants: 45\n  empty_timeout: 60\n  departure_timeout: 20\n  max_metadata_size: 4096\n  max_participant_identity_length: 128\n  enabled_codecs:\n    - mime: audio/opus\n      fmtp_line: minptime=10\n";
    let got = load(&yaml(body), &Env::default());
    assert_eq!(45, got.room.max_participants, "C98C-35");
    assert_eq!(Duration::from_secs(60), got.room.empty_timeout, "C98C-35");
    assert_eq!(
        Duration::from_secs(20),
        got.room.departure_timeout,
        "C98C-35"
    );
    assert_eq!(
        vec![("audio/opus".to_string(), "minptime=10".to_string())],
        got.room
            .enabled_codecs
            .iter()
            .map(|c| (c.mime.clone(), c.fmtp_line.clone()))
            .collect::<Vec<_>>(),
        "C98C-35 the codec spec pair must project both fields"
    );
    assert_eq!(
        Some(4096),
        got.limits.max_metadata_size,
        "C98C-36 room.max_metadata_size is copied into limits (load.rs:665-673)"
    );
    assert_eq!(
        128, got.limits.max_participant_identity_length,
        "C98C-36 room.max_participant_identity_length is copied into limits"
    );
}

// ---------------------------------------------------------------------------
// generated_flags:--limit.* / YAML:limit
// ---------------------------------------------------------------------------

#[test]
fn limit_leaf_flags_project_into_the_limits_carrier() {
    let base = load(&[], &Env::default());
    assert_eq!(256, base.limits.max_data_blob_key_length, "C98C-37");
    assert_eq!(64000, base.limits.max_data_blob_size, "C98C-37");
    assert_eq!(
        Some(2_097_152),
        base.limits.signal_message_size_limit,
        "C98C-37 2 MiB"
    );
    assert_eq!(Some(524_288), base.limits.max_metadata_size, "C98C-37");

    let got = load(
        &[
            "--limit.max_data_blob_key_length=1001",
            "--limit.max_data_blobs_size=1002",
            "--limit.signal_message_size_limit=1003",
        ],
        &Env::default(),
    );
    assert_eq!(1001, got.limits.max_data_blob_key_length, "C98C-38");
    assert_eq!(1002, got.limits.max_data_blob_size, "C98C-38");
    assert_eq!(
        Some(1003),
        got.limits.signal_message_size_limit,
        "C98C-38 the nonzero_usize wrapper must keep the value"
    );
    assert_eq!(
        Some(524_288),
        got.limits.max_metadata_size,
        "C98C-38 untouched limit must not move"
    );

    let zero = load(&["--limit.signal_message_size_limit=0"], &Env::default());
    assert_eq!(
        None, zero.limits.signal_message_size_limit,
        "C98C-39 zero means unlimited, so the carrier is None"
    );

    let api = load(&["--limit.max_api_request_body_size=1004"], &Env::default());
    assert_eq!(
        1004, api.http.max_request_body,
        "C98C-40 the API body limit lands on the HTTP carrier (normalize.rs:437)"
    );
    let env = load(
        &[],
        &Env::from_pairs([
            ("LIVEKIT_LIMIT_MAX_DATA_BLOBS_SIZE", "1005"),
            ("LIVEKIT_LIMIT_MAX_API_REQUEST_BODY_SIZE", "1006"),
        ]),
    );
    assert_eq!(
        1005, env.limits.max_data_blob_size,
        "C98C-41 env must project"
    );
    assert_eq!(1006, env.http.max_request_body, "C98C-41 env must project");
}

#[test]
fn limit_section_projects_metadata_and_legacy_room_copies() {
    let body = "limit:\n  max_metadata_size: 99\n  max_data_blob_key_length: 101\n  max_data_blobs_size: 102\n  signal_message_size_limit: 103\n  max_api_request_body_size: 104\n";
    let yaml_form = load(&yaml(body), &Env::default());
    assert_eq!(Some(99), yaml_form.limits.max_metadata_size, "C98C-42");
    assert_eq!(101, yaml_form.limits.max_data_blob_key_length, "C98C-42");
    assert_eq!(102, yaml_form.limits.max_data_blob_size, "C98C-42");
    assert_eq!(
        Some(103),
        yaml_form.limits.signal_message_size_limit,
        "C98C-42"
    );
    assert_eq!(104, yaml_form.http.max_request_body, "C98C-42");
    let flag_form = load(
        &[
            "--limit.max_metadata_size=99",
            "--limit.max_data_blob_key_length=101",
            "--limit.max_data_blobs_size=102",
            "--limit.signal_message_size_limit=103",
            "--limit.max_api_request_body_size=104",
        ],
        &Env::default(),
    );
    assert_eq!(
        yaml_form.limits.max_metadata_size, flag_form.limits.max_metadata_size,
        "C98C-43 the same leaf must project identically through YAML and CLI"
    );
    assert_eq!(
        yaml_form.limits.max_data_blob_key_length, flag_form.limits.max_data_blob_key_length,
        "C98C-43"
    );
    assert_eq!(
        yaml_form.limits.max_data_blob_size, flag_form.limits.max_data_blob_size,
        "C98C-43"
    );
    assert_eq!(
        yaml_form.limits.signal_message_size_limit, flag_form.limits.signal_message_size_limit,
        "C98C-43"
    );
    assert_eq!(
        yaml_form.http.max_request_body, flag_form.http.max_request_body,
        "C98C-43"
    );
}

#[test]
fn limit_negative_values_follow_their_declared_kind() {
    // `max_data_blob_key_length` is an Int leaf, so a negative reaches the signed carrier;
    // `max_metadata_size` is a Uint leaf, so the same text must fail with the upstream
    // ParseUint message instead of silently becoming a value.
    let got = load(&["--limit.max_data_blob_key_length=-1"], &Env::default());
    assert_eq!(
        -1, got.limits.max_data_blob_key_length,
        "C98C-44 the signed leaf keeps the negative for the predicate to reject with"
    );
    let text = outcome_text(&["--limit.max_metadata_size=-2"]);
    assert!(
        text.contains(
            "invalid value \"-2\" for flag -limit.max_metadata_size: \
         strconv.ParseUint: parsing \"-2\": invalid syntax"
        ),
        "C98C-45 the Uint leaf must reject a negative with the Go message, got: {text}"
    );
    let zero = load(&["--limit.max_data_blob_key_length=0"], &Env::default());
    assert_eq!(
        0, zero.limits.max_data_blob_key_length,
        "C98C-46 zero is the unlimited sentinel for this leaf"
    );
}

// ---------------------------------------------------------------------------
// generated_flags:--audio.* / YAML:audio
// ---------------------------------------------------------------------------

#[test]
fn audio_leaf_flags_project_into_the_audio_carrier() {
    let base = load(&[], &Env::default());
    assert_eq!(
        Duration::from_millis(400),
        base.audio.update_interval,
        "C98C-45 the Go default is 400ms"
    );
    assert!(!base.audio.active_red_encoding, "C98C-45 Go default");

    let got = load(
        &[
            "--audio.update_interval=1500",
            "--audio.active_red_encoding=true",
        ],
        &Env::default(),
    );
    assert_eq!(
        Duration::from_millis(1500),
        got.audio.update_interval,
        "C98C-46 the Uint leaf is milliseconds, not a duration string"
    );
    assert!(got.audio.active_red_encoding, "C98C-46");
    assert_eq!(
        base.audio.active_level, got.audio.active_level,
        "C98C-47 an untouched audio leaf must not move"
    );
    assert_eq!(
        base.audio.smooth_intervals, got.audio.smooth_intervals,
        "C98C-47 an untouched audio leaf must not move"
    );

    let env = load(
        &[],
        &Env::from_pairs([
            ("LIVEKIT_AUDIO_UPDATE_INTERVAL", "250"),
            ("LIVEKIT_AUDIO_ACTIVE_RED_ENCODING", "true"),
        ]),
    );
    assert_eq!(
        Duration::from_millis(250),
        env.audio.update_interval,
        "C98C-48 env must project"
    );
    assert!(env.audio.active_red_encoding, "C98C-48 env must project");

    let yaml_form = load(
        &yaml("audio:\n  update_interval: 321\n  active_red_encoding: true\n"),
        &Env::default(),
    );
    assert_eq!(
        Duration::from_millis(321),
        yaml_form.audio.update_interval,
        "C98C-49 YAML must project the same field the flag does"
    );
    assert!(yaml_form.audio.active_red_encoding, "C98C-49");
}

#[test]
fn audio_section_parent_projects_every_scalar_into_its_own_slot() {
    let body = "audio:\n  active_level: 41\n  min_percentile: 42\n  update_interval: 43\n  smooth_intervals: 44\n  active_red_encoding: true\n";
    let got = load(&yaml(body), &Env::default());
    assert_eq!(41, got.audio.active_level, "C98C-50");
    assert_eq!(42, got.audio.min_percentile, "C98C-50");
    assert_eq!(
        Duration::from_millis(43),
        got.audio.update_interval,
        "C98C-50"
    );
    assert_eq!(44, got.audio.smooth_intervals, "C98C-50");
    assert!(got.audio.active_red_encoding, "C98C-50");
}

// ---------------------------------------------------------------------------
// YAML:video / YAML:video.stream_tracker_manager*
//
// `stream_tracker_manager` holds a mode string plus two int-keyed maps, so the Go
// flag walk (config.go:943-1050) gives the maps and the mode no CLI surface; the
// negative arms below pin that. The projection turns each configured layer into a
// `StreamTrackerSpec` (normalize.rs:391-420).
// ---------------------------------------------------------------------------

#[test]
fn video_scalar_leaves_project_into_the_video_carrier() {
    let base = load(&[], &Env::default());
    assert_eq!(
        5, base.video.codec_regression_threshold,
        "C98C-51 Go default"
    );
    assert_eq!(
        Duration::from_secs(5),
        base.video.dynacast_pause_delay,
        "C98C-51 Go default 5s"
    );

    let got = load(
        &[
            "--video.codec_regression_threshold=11",
            "--video.dynacast_pause_delay=1500ms",
        ],
        &Env::default(),
    );
    assert_eq!(11, got.video.codec_regression_threshold, "C98C-52");
    assert_eq!(
        Duration::from_millis(1500),
        got.video.dynacast_pause_delay,
        "C98C-52"
    );

    let one = load(&["--video.dynacast_pause_delay=2100ms"], &Env::default());
    assert_eq!(
        5, one.video.codec_regression_threshold,
        "C98C-53 one flag must not move the sibling video leaf"
    );
    let env = load(
        &[],
        &Env::from_pairs([
            ("LIVEKIT_VIDEO_CODEC_REGRESSION_THRESHOLD", "13"),
            ("LIVEKIT_VIDEO_DYNACAST_PAUSE_DELAY", "9s"),
        ]),
    );
    assert_eq!(13, env.video.codec_regression_threshold, "C98C-54");
    assert_eq!(
        Duration::from_secs(9),
        env.video.dynacast_pause_delay,
        "C98C-54"
    );
}

#[test]
fn video_stream_tracker_manager_packet_tracker_map_projects_per_layer_and_mode() {
    let body = "video:\n  stream_tracker_manager:\n    video:\n      stream_tracker_type: packet\n      packet_tracker:\n        0:\n          samples_required: 6\n          cycles_required: 7\n          cycle_duration: 8ms\n        2:\n          samples_required: 9\n          cycles_required: 10\n          cycle_duration: 11ms\n";
    let got = load(&yaml(body), &Env::default());
    use rtc_core::config::{StreamTrackerSpec::Packet, VideoStreamTrackers};
    let VideoStreamTrackers { video, .. } = got.video.stream_trackers;
    let layer0 = match video[0] {
        Some(Packet(p)) => p,
        other => panic!("C98C-55 layer 0 must project the packet spec, got {other:?}"),
    };
    assert_eq!(6, layer0.samples_required, "C98C-55");
    assert_eq!(7, layer0.cycles_required, "C98C-55");
    assert_eq!(Duration::from_millis(8), layer0.cycle_duration, "C98C-55");
    let layer2 = match video[2] {
        Some(Packet(p)) => p,
        other => panic!("C98C-56 layer 2 must project its own spec, got {other:?}"),
    };
    assert_eq!(
        9, layer2.samples_required,
        "C98C-56 layer 2 must not copy layer 0"
    );
    assert_eq!(10, layer2.cycles_required, "C98C-56");
    assert_eq!(Duration::from_millis(11), layer2.cycle_duration, "C98C-56");
    assert_eq!(
        Some(Packet(rtc_core::config::PacketTrackerSpec {
            samples_required: 5,
            cycles_required: 20,
            cycle_duration: Duration::from_millis(500),
        })),
        video[1],
        "C98C-57 an unconfigured layer takes the fixed Go packet default, not a neighbour's value"
    );
    let screenshare_defaults = [Some(rtc_core::config::StreamTrackerSpec::Packet(
        rtc_core::config::PacketTrackerSpec {
            samples_required: 1,
            cycles_required: 1,
            cycle_duration: Duration::from_secs(2),
        },
    )); 3];
    assert_eq!(
        screenshare_defaults, got.video.stream_trackers.screenshare,
        "C98C-58 an untouched screenshare section still carries the fixed Go screenshare packet
         defaults, which are not the video ones"
    );

    let screenshare = load(
        &yaml(
            "video:\n  stream_tracker_manager:\n    screenshare:\n      stream_tracker_type: packet\n      packet_tracker:\n        1:\n          samples_required: 21\n          cycles_required: 22\n          cycle_duration: 23ms\n",
        ),
        &Env::default(),
    );
    let layer1 = match screenshare.video.stream_trackers.screenshare[1] {
        Some(Packet(p)) => p,
        other => panic!("C98C-59 screenshare layer 1 must project, got {other:?}"),
    };
    assert_eq!(21, layer1.samples_required, "C98C-59");
    assert_eq!(22, layer1.cycles_required, "C98C-59");
    assert_eq!(Duration::from_millis(23), layer1.cycle_duration, "C98C-59");
    assert_eq!(
        [
            Some(rtc_core::config::StreamTrackerSpec::Packet(
                rtc_core::config::PacketTrackerSpec {
                    samples_required: 1,
                    cycles_required: 4,
                    cycle_duration: Duration::from_millis(500),
                }
            )),
            Some(rtc_core::config::StreamTrackerSpec::Packet(
                rtc_core::config::PacketTrackerSpec {
                    samples_required: 5,
                    cycles_required: 20,
                    cycle_duration: Duration::from_millis(500),
                }
            )),
            Some(rtc_core::config::StreamTrackerSpec::Packet(
                rtc_core::config::PacketTrackerSpec {
                    samples_required: 5,
                    cycles_required: 20,
                    cycle_duration: Duration::from_millis(500),
                }
            )),
        ],
        screenshare.video.stream_trackers.video,
        "C98C-60 a screenshare-only map must leave the video side on its own per-layer defaults,
         where layer 0 (1/4/500ms) differs from layers 1-2 (5/20/500ms)"
    );

    let frame = load(
        &yaml(
            "video:\n  stream_tracker_manager:\n    video:\n      stream_tracker_type: frame\n      frame_tracker:\n        0:\n          min_fps: 2.5\n",
        ),
        &Env::default(),
    );
    match frame.video.stream_trackers.video[0] {
        Some(rtc_core::config::StreamTrackerSpec::Frame { min_fps }) => {
            assert_eq!(2.5, min_fps, "C98C-61 the frame mode must project min_fps")
        }
        other => panic!("C98C-61 frame mode must not project a packet spec: {other:?}"),
    }

    for absent_path in [
        "video.stream_tracker_manager",
        "video.stream_tracker_manager.video",
        "video.stream_tracker_manager.video.packet_tracker",
        "video.stream_tracker_manager.video.packet_tracker.0",
        "video.stream_tracker_manager.screenshare.packet_tracker",
        "video.stream_tracker_manager.video.frame_tracker",
    ] {
        assert!(
            specs().iter().all(|s| s.path != absent_path),
            "C98C-62 a struct/map container must not gain a flag: {absent_path}"
        );
    }
    for present_path in [
        "video.stream_tracker_manager.video.stream_tracker_type",
        "video.stream_tracker_manager.screenshare.stream_tracker_type",
    ] {
        assert!(
            specs().iter().any(|s| s.path == present_path),
            "C98C-62 the scalar mode leaf inside the struct must gain a flag: {present_path}"
        );
    }
}

#[test]
fn video_bitrate_report_intervals_project_per_family_and_layer() {
    let base = load(&[], &Env::default());
    assert_eq!(
        [Some(Duration::from_secs(1)); 3],
        base.video.bitrate_report.video,
        "C98C-80 the Go video bitrate-report default is 1s on every layer"
    );
    assert_eq!(
        [Some(Duration::from_secs(4)); 3],
        base.video.bitrate_report.screenshare,
        "C98C-80 the screenshare side defaults to 4s, which is not the video value"
    );

    let got = load(
        &yaml(
            "video:\n  stream_tracker_manager:\n    video:\n      bitrate_report_interval: {0: 125ms, 2: 450ms}\n    screenshare:\n      bitrate_report_interval: {0: 999ms}\n",
        ),
        &Env::default(),
    );
    assert_eq!(
        [
            Some(Duration::from_millis(125)),
            Some(Duration::from_secs(1)),
            Some(Duration::from_millis(450)),
        ],
        got.video.bitrate_report.video,
        "C98C-81 each layer keeps its own interval and an unset layer keeps the default"
    );
    assert_eq!(
        [
            Some(Duration::from_millis(999)),
            Some(Duration::from_secs(4)),
            Some(Duration::from_secs(4)),
        ],
        got.video.bitrate_report.screenshare,
        "C98C-81 the screenshare side must read its own map, not the video one"
    );

    let zero = load(
        &yaml(
            "video:\n  stream_tracker_manager:\n    video:\n      bitrate_report_interval: {1: 0s}\n",
        ),
        &Env::default(),
    );
    assert_eq!(
        [
            Some(Duration::from_secs(1)),
            None,
            Some(Duration::from_secs(1)),
        ],
        zero.video.bitrate_report.video,
        "C98C-82 a zero interval disables reporting for that layer only (normalize.rs:380-388)"
    );
}

#[test]
fn video_section_parent_projects_scalars_and_trackers_together() {
    let body = "video:\n  codec_regression_threshold: 6\n  dynacast_pause_delay: 1s\n  stream_tracker_manager:\n    video:\n      stream_tracker_type: packet\n      packet_tracker:\n        0:\n          samples_required: 1\n          cycles_required: 2\n          cycle_duration: 3ms\n";
    let got = load(&yaml(body), &Env::default());
    assert_eq!(6, got.video.codec_regression_threshold, "C98C-63");
    assert_eq!(
        Duration::from_secs(1),
        got.video.dynacast_pause_delay,
        "C98C-63"
    );
    use rtc_core::config::StreamTrackerSpec::Packet;
    match got.video.stream_trackers.video[0] {
        Some(Packet(p)) => assert_eq!(
            (1, 2, Duration::from_millis(3)),
            (p.samples_required, p.cycles_required, p.cycle_duration),
            "C98C-63 the tracker inside the same section must project too"
        ),
        other => panic!("C98C-63 expected a packet tracker, got {other:?}"),
    }
    assert_eq!(
        5,
        load(
            &yaml("video:\n  codec_regression_threshold: 6\n"),
            &Env::default()
        )
        .video
        .dynacast_pause_delay
        .as_secs(),
        "C98C-64 a scalar in the section must not disturb the absent sibling"
    );
}

// ---------------------------------------------------------------------------
// generated_flags:--turn.tls_port / --turn.proxy_protocol / YAML:turn
// ---------------------------------------------------------------------------

#[test]
fn turn_leaf_flags_project_only_while_turn_is_enabled() {
    let base = load(&[], &Env::default());
    assert!(!base.turn.enabled, "C98C-65 TURN is off by default");
    assert_eq!(
        None, base.turn.tls_port,
        "C98C-65 an off server has no ports projected"
    );
    assert!(!base.turn.proxy_protocol, "C98C-65 Go default");

    let got = load(
        &["--turn.enabled=true", "--turn.tls_port=4433"],
        &Env::default(),
    );
    assert_eq!(
        Some(4433),
        got.turn.tls_port,
        "C98C-66 the port projects once TURN is enabled"
    );

    let off = load(&["--turn.tls_port=4433"], &Env::default());
    assert_eq!(
        None, off.turn.tls_port,
        "C98C-67 normalize.rs:714-721 suppresses the port while TURN is disabled"
    );

    // `proxy_protocol_trusted_cidrs` is a slice, so Go's walk (config.go:943-1050) gives it no
    // flag at all; YAML only. Upstream defaults it to 127.0.0.0/8 + ::1/128 (config.go:639).
    let with_tls = load(
        &[
            "--turn.enabled=true",
            "--turn.tls_port=4433",
            "--turn.proxy_protocol=true",
            "--config-body",
            "turn:\n  proxy_protocol_trusted_cidrs:\n    - 127.0.0.0/9\n",
        ],
        &Env::default(),
    );
    assert!(with_tls.turn.proxy_protocol, "C98C-68");
    assert_eq!(
        Some(4433),
        with_tls.turn.tls_port,
        "C98C-68 the TLS port must project beside the flag"
    );
    assert_eq!(
        vec![("127.0.0.0".parse::<IpAddr>().expect("fixture cidr"), 9u8)],
        with_tls.turn.proxy_trusted_cidrs,
        "C98C-68 the trusted CIDR must project once the listener exists"
    );

    let no_tls = load(
        &[
            "--turn.proxy_protocol=true",
            "--config-body",
            "turn:\n  proxy_protocol_trusted_cidrs:\n    - 127.0.0.0/9\n",
        ],
        &Env::default(),
    );
    assert!(
        no_tls.turn.proxy_trusted_cidrs.is_empty(),
        "C98C-68b normalize.rs:728 parses the CIDRs only for an enabled TURN TLS listener, so a
         configured list stays out of the carrier otherwise: {:?}",
        no_tls.turn.proxy_trusted_cidrs
    );

    let no_cidr = outcome_text(&[
        "--turn.enabled=true",
        "--turn.tls_port=4433",
        "--turn.proxy_protocol=true",
        "--config-body",
        "turn:\n  proxy_protocol_trusted_cidrs: []\n",
    ]);
    assert!(
        no_cidr.contains(
            "turn.proxy_protocol requires at least one entry in turn.proxy_protocol_trusted_cidrs"
        ),
        "C98C-69 an empty trusted list with proxy protocol on must fail with the upstream text, got: {no_cidr}"
    );
    let alone = load(&["--turn.proxy_protocol=true"], &Env::default());
    assert!(
        alone.turn.proxy_protocol,
        "C98C-69 the bool projects even with TURN off (it is copied unconditionally)"
    );
    let env = load(
        &[],
        &Env::from_pairs([
            ("LIVEKIT_TURN_ENABLED", "true"),
            ("LIVEKIT_TURN_TLS_PORT", "4499"),
        ]),
    );
    assert_eq!(Some(4499), env.turn.tls_port, "C98C-70 env must project");
}

#[test]
fn turn_section_parent_projects_scalars_lists_and_the_derived_ports() {
    let body = "turn:\n  enabled: true\n  domain: turn.example.test\n  tls_port: 4434\n  udp_port: 4435\n  relay_range_start: 40010\n  relay_range_end: 40020\n  ttl_seconds: 900\n  per_user_relay_allocation_limit: 5\n  bind_addresses:\n    - 127.0.0.1\n";
    let got = load(&yaml(body), &Env::default());
    assert!(got.turn.enabled, "C98C-71");
    assert_eq!("turn.example.test", got.turn.domain, "C98C-71");
    assert_eq!(Some(4434), got.turn.tls_port, "C98C-71");
    assert_eq!(Some(4435), got.turn.udp_port, "C98C-71");
    assert_eq!((40010, 40020), got.turn.relay_range, "C98C-71");
    assert_eq!(Duration::from_secs(900), got.turn.ttl, "C98C-71");
    assert_eq!(5, got.turn.per_user_allocation_limit, "C98C-71");
    assert_eq!(
        vec!["127.0.0.1".parse::<IpAddr>().expect("fixture bind")],
        got.turn.bind_addresses,
        "C98C-71 the bind list projects as parsed addresses"
    );
    let clamped = load(
        &yaml("turn:\n  enabled: true\n  ttl_seconds: 999999\n"),
        &Env::default(),
    );
    assert_eq!(
        Duration::from_secs(24 * 60 * 60),
        clamped.turn.ttl,
        "C98C-72 the TTL clamp (load.rs) must survive into the carrier"
    );
}

// ---------------------------------------------------------------------------
// YAML:api / YAML:enable_participant_data_blob / generated_flags:--key_file
// ---------------------------------------------------------------------------

#[test]
fn api_and_blob_leaves_project_onto_the_server_top_level() {
    let base = load(&[], &Env::default());
    assert!(
        !base.enable_psrpc_for_get_list_participants,
        "C98C-73 Go default off"
    );
    assert!(!base.enable_participant_data_blob, "C98C-73 Go default off");

    let got = load(
        &[
            "--api.enable_psrpc_for_get_list_participants=true",
            "--enable_participant_data_blob=true",
        ],
        &Env::default(),
    );
    assert!(
        got.enable_psrpc_for_get_list_participants,
        "C98C-74 the API leaf must reach the server flag"
    );
    assert!(
        got.enable_participant_data_blob,
        "C98C-74 the top-level bool must reach the server flag"
    );
    assert!(
        !base.enable_data_tracks || got.enable_data_tracks == base.enable_data_tracks,
        "C98C-75 the blob leaf must not disturb the data-tracks flag"
    );

    let env = load(
        &[],
        &Env::from_pairs([
            ("LIVEKIT_API_ENABLE_PSRPC_FOR_GET_LIST_PARTICIPANTS", "true"),
            ("LIVEKIT_ENABLE_PARTICIPANT_DATA_BLOB", "true"),
        ]),
    );
    assert!(
        env.enable_psrpc_for_get_list_participants && env.enable_participant_data_blob,
        "C98C-76 env must project both"
    );
    let yaml_form = load(
        &yaml(
            "api:\n  enable_psrpc_for_get_list_participants: true\nenable_participant_data_blob: true\n",
        ),
        &Env::default(),
    );
    assert!(
        yaml_form.enable_psrpc_for_get_list_participants && yaml_form.enable_participant_data_blob,
        "C98C-77 YAML must project both"
    );
}

#[test]
fn key_file_leaf_replaces_the_inline_keys_it_overrides() {
    let dir = std::env::temp_dir().join(format!("c98c-keyfile-{}", std::process::id()));
    std::fs::create_dir_all(&dir).expect("fixture dir");
    let path = dir.join("keys.yaml");
    std::fs::write(&path, "keyOne: 0123456789012345678901234567890123456789\n")
        .expect("fixture key file");
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600))
            .expect("fixture key file mode");
    }
    let flag = format!("--key_file={}", path.display());
    let got = load(&[flag.as_str()], &Env::default());
    assert_eq!(
        vec![(
            "keyOne".to_string(),
            "0123456789012345678901234567890123456789".to_string()
        )],
        got.keys.clone(),
        "C98C-78 the key file replaces the dev placeholder keys, it does not merge into them"
    );
    let env = load(
        &[],
        &Env::from_pairs([("LIVEKIT_KEY_FILE", path.to_str().expect("utf-8 path"))]),
    );
    assert_eq!(
        vec!["keyOne".to_string()],
        env.keys.iter().map(|(k, _)| k.clone()).collect::<Vec<_>>(),
        "C98C-79 the env form must reach the same carrier"
    );
    std::fs::remove_dir_all(&dir).expect("fixture cleanup");
}
