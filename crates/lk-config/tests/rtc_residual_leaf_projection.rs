//! C97-a config_projection legs for the eleven `rtc.*` leaves C96 left NOT_RUN.
//!
//! Same tier and same harness rule as `rtc_leaf_projection.rs`: every leg drives
//! YAML / ENV / CLI through the public loading API (`parse_with_probe`) and
//! asserts on the *projected* `rtc_core::config::RtcConfig` (plus the raw
//! official tree only to prove a precedence source), never on the parsed tree
//! alone. The rtc projection site is `crates/lk-config/src/normalize.rs:575-647`
//! with the `forward_stats` block at :555-574 and the TURN list at :510-537; the
//! `secret_file` read happens earlier, in `load.rs:384-421` (`LoadTURNSecrets`).
//!
//! Go oracle: references/livekit-stable @ e5655ac (v1.13.10). Every "upstream
//! does X" claim below names the pinned file:line that was grepped.
use lk_config::{EntryAction, Env, ProgramInfo, netdetect::NetProbe, parse_with_probe};
use rtc_core::config::IceServerProtocol;
use std::{
    ffi::OsString,
    io::Write,
    net::IpAddr,
    os::unix::fs::PermissionsExt,
    path::PathBuf,
    time::{Duration, Instant},
};

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

/// Run the real loading path and hand back whatever it printed instead of
/// starting the server (usage errors, help text, ...). A leg that expects the
/// CLI to refuse an unusable spelling must assert on this text.
fn entry_action_text(args: &[&str], env: &Env) -> String {
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
        Ok(EntryAction::PrintHelp { text, .. }) => text,
        Ok(other) => panic!("expected a printed-action entry, got {other:?}"),
        Err(e) => format!("parse error: {e}"),
    }
}

/// A unique, self-cleaning temporary path created with `O_EXCL`, so two legs or
/// two runs can never share or overwrite each other's fixture.
fn temp_path(tag: &str) -> PathBuf {
    static SEQ: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);
    let seq = SEQ.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    loop {
        let candidate = std::env::temp_dir().join(format!(
            "c97a-{tag}-{}-{}-{seq}.txt",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("clock after epoch")
                .as_nanos()
        ));
        match std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&candidate)
        {
            Ok(_) => return candidate,
            Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => continue,
            Err(e) => panic!("cannot reserve fixture path: {e}"),
        }
    }
}

/// Write a secret file with an exact mode, returning its path.
fn write_secret(path: &PathBuf, bytes: &[u8], mode: u32) {
    let mut f = std::fs::File::options()
        .write(true)
        .truncate(true)
        .open(path)
        .expect("open reserved fixture secret");
    f.write_all(bytes).expect("write fixture secret");
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(mode))
        .expect("chmod fixture secret");
}

// ---------------------------------------------------------------------------
// YAML:rtc.skip_external_ip_validation
//
// Upstream declares it in the embedded `rtcconfig.RTCConfig`
// (mediatransportutil pkg/rtcconfig/config.go:55, read at webrtc_config.go:330) and
// reads it exactly once, as the hairpin-validation switch handed to the
// external-IP discovery. In this port the projected field is consumed at
// crates/livecloud/src/interfaces.rs:253 (`external_mappings` ->
// `nat1to1::discover_system(..., rtc.skip_external_ip_validation)`).
// normalize.rs:603 copies the leaf verbatim -- no default rewrite, no negation.
// ---------------------------------------------------------------------------

#[test]
fn rtc_skip_external_ip_validation_yaml_env_cli_projection_and_precedence() {
    // Absent: Go zero value (bool, no DefaultConfig entry for it --
    // config.go:569-591 lists no skip flag).
    let base = rtc(&[], &Env::default());
    assert!(!base.skip_external_ip_validation);

    // YAML true projects true and does not move any sibling boolean.
    let cfg = rtc(
        &[
            "--config-body",
            "rtc:\n  skip_external_ip_validation: true\n",
        ],
        &Env::default(),
    );
    assert!(cfg.skip_external_ip_validation);
    assert!(!cfg.use_external_ip, "sibling stays untouched");
    assert!(!cfg.external_ip_only, "sibling stays untouched");
    assert_ne!(
        cfg.skip_external_ip_validation,
        base.skip_external_ip_validation
    );

    // Explicit false is the same downstream value as absent (plain bool upstream,
    // no Option/nil distinction), but the load must still succeed.
    let cfg = rtc(
        &[
            "--config-body",
            "rtc:\n  skip_external_ip_validation: false\n",
        ],
        &Env::default(),
    );
    assert!(!cfg.skip_external_ip_validation);

    // YAML 1.1 boolean spellings for a typed bool target must project true rather
    // than silently fall back to the false default.
    for spelling in ["yes", "on", "True"] {
        let yaml = format!("rtc:\n  skip_external_ip_validation: {spelling}\n");
        assert!(
            rtc(&["--config-body", &yaml], &Env::default()).skip_external_ip_validation,
            "{spelling} must project true"
        );
    }

    // ENV surface, both directions.
    let env_on = Env::from_pairs([("LIVEKIT_RTC_SKIP_EXTERNAL_IP_VALIDATION", "true")]);
    assert!(rtc(&[], &env_on).skip_external_ip_validation);
    let env_off = Env::from_pairs([("LIVEKIT_RTC_SKIP_EXTERNAL_IP_VALIDATION", "false")]);
    assert!(!rtc(&[], &env_off).skip_external_ip_validation);

    // CLI surface, both directions.
    assert!(
        rtc(&["--rtc.skip_external_ip_validation=true"], &Env::default())
            .skip_external_ip_validation
    );
    assert!(
        !rtc(
            &["--rtc.skip_external_ip_validation=false"],
            &Env::from_pairs([("LIVEKIT_RTC_SKIP_EXTERNAL_IP_VALIDATION", "true")])
        )
        .skip_external_ip_validation,
        "CLI false must beat ENV true"
    );
    // CLI beats YAML.
    assert!(
        !rtc(
            &[
                "--config-body",
                "rtc:\n  skip_external_ip_validation: true\n",
                "--rtc.skip_external_ip_validation=false"
            ],
            &Env::default()
        )
        .skip_external_ip_validation
    );
    // ENV beats YAML.
    let cfg = parse(
        &[
            "--config-body",
            "rtc:\n  skip_external_ip_validation: false\n",
        ],
        &env_on,
    );
    assert!(cfg.server.rtc.skip_external_ip_validation);
}

// ---------------------------------------------------------------------------
// YAML:rtc.interfaces (structural parent)
//
// The parent's honest claim is the container conversion, not its two scalars:
// `rtcconfig.InterfacesConfig{Includes,Excludes}` (two independent regex lists)
// becomes one projected `Filter<String>` whose includes/excludes stay separate
// vectors. normalize.rs:608-611 clones both halves verbatim. The children
// interfaces.includes / interfaces.excludes are already MET elsewhere (C30),
// so this leg deliberately asserts only the pairing/no-crossing property plus
// the parent's own empty-vs-populated shape.
// ---------------------------------------------------------------------------

#[test]
fn rtc_interfaces_parent_projects_both_halves_as_one_filter() {
    // Absent parent: both halves empty, never a default deny-all list.
    let base = rtc(&[], &Env::default());
    assert!(base.interfaces.includes.is_empty());
    assert!(base.interfaces.excludes.is_empty());

    // Only includes set: the exclude half stays empty (a projection that merged
    // or mirrored the two lists would fail here).
    let yaml = "rtc:\n  interfaces:\n    includes: [\"^eth$\"]\n";
    let cfg = rtc(&["--config-body", yaml], &Env::default());
    assert_eq!(cfg.interfaces.includes, vec!["^eth$".to_string()]);
    assert!(
        cfg.interfaces.excludes.is_empty(),
        "includes must not leak into excludes"
    );

    // Only excludes set: symmetric guard.
    let yaml = "rtc:\n  interfaces:\n    excludes: [\"^lo$\"]\n";
    let cfg = rtc(&["--config-body", yaml], &Env::default());
    assert!(cfg.interfaces.includes.is_empty());
    assert_eq!(cfg.interfaces.excludes, vec!["^lo$".to_string()]);

    // Both halves in one parent mapping, order preserved per half, duplicates
    // kept verbatim (no dedup upstream either: it is a plain []string copy).
    let yaml = "rtc:\n  interfaces:\n    includes: [\"a\", \"b\", \"a\"]\n    excludes: [\"z\"]\n";
    let cfg = rtc(&["--config-body", yaml], &Env::default());
    assert_eq!(
        cfg.interfaces.includes,
        vec!["a".to_string(), "b".to_string(), "a".to_string()]
    );
    assert_eq!(cfg.interfaces.excludes, vec!["z".to_string()]);

    // No ENV / CLI surface exists for this parent, and that is the upstream
    // contract, not a port gap: `GenerateCLIFlags` skips reflect.Slice fields
    // (`config.go:943-1050`, `case reflect.Slice: continue`), so neither
    // `LIVEKIT_RTC_INTERFACES*` nor a dotted `--rtc.interfaces.*` flag is ever
    // registered. Measured on this port: the unset flag aborts the parse with
    // urfave's own `flag provided but not defined: -rtc.interfaces.includes`
    // usage error (cli.rs:564), and the unset env names are simply inert -- the
    // projection stays exactly at the YAML value. Both arms fail if the port
    // ever grows a slice surface silently.
    let env = Env::from_pairs([
        ("LIVEKIT_RTC_INTERFACES", "^env$"),
        ("LIVEKIT_RTC_INTERFACES_INCLUDES", "^env$"),
        ("LIVEKIT_RTC_INTERFACES_INCLUDES_0", "^env$"),
    ]);
    let cfg = rtc(&["--config-body", yaml], &env);
    assert_eq!(
        cfg.interfaces.includes,
        vec!["a".to_string(), "b".to_string(), "a".to_string()],
        "slice parents have no env surface (generated.rs:1-13) -- YAML must survive"
    );
    assert_eq!(cfg.interfaces.excludes, vec!["z".to_string()]);
    let rejected = entry_action_text(
        &["--config-body", yaml, "--rtc.interfaces.includes=^cli$"],
        &env,
    );
    assert!(
        rejected
            .contains("Incorrect Usage: flag provided but not defined: -rtc.interfaces.includes"),
        "measured rejection was: {rejected}"
    );
}

// ---------------------------------------------------------------------------
// YAML:rtc.ips (structural parent)
//
// The parent's real claim is the type change it owns: `rtcconfig.IPsConfig`
// holds two []string CIDR lists, and the projection converts each element with
// `net.ParseCIDR` semantics into `(network_ip, prefix_len)` while keeping the
// halves apart (normalize.rs:612-615 via `cidrs()` at :316-318). The scalar children
// ips.includes / ips.excludes are MET elsewhere (C28/C29); this leg asserts the
// container-level conversion: masking, family mixing, per-half independence and
// the documented skip-malformed behaviour.
// ---------------------------------------------------------------------------

#[test]
fn rtc_ips_parent_projects_cidr_pairs_per_half() {
    let v4 = IpAddr::V4;
    // Absent parent: no filters at all.
    let base = rtc(&[], &Env::default());
    assert!(base.ips.includes.is_empty() && base.ips.excludes.is_empty());

    // Unmasked host addresses are masked to their network, exactly like Go's
    // net.ParseCIDR return value (the third element proves /16 arithmetic).
    let yaml = "rtc:\n  ips:\n    includes: [\"10.1.2.3/8\", \"192.168.5.7/16\"]\n";
    let cfg = rtc(&["--config-body", yaml], &Env::default());
    assert_eq!(
        cfg.ips.includes,
        vec![
            (v4("10.0.0.0".parse().unwrap()), 8u8),
            (v4("192.168.0.0".parse().unwrap()), 16u8),
        ]
    );
    assert!(cfg.ips.excludes.is_empty(), "halves must not cross");

    // Mixed families and boundary prefixes survive the pair conversion.
    let yaml = "rtc:\n  ips:\n    includes: [\"fd00::/64\", \"203.0.113.0/24\", \"0.0.0.0/0\"]\n";
    let cfg = rtc(&["--config-body", yaml], &Env::default());
    assert_eq!(
        cfg.ips.includes,
        vec![
            ("fd00::".parse::<IpAddr>().unwrap(), 64u8),
            (v4("203.0.113.0".parse().unwrap()), 24u8),
            (v4("0.0.0.0".parse().unwrap()), 0u8),
        ]
    );

    // Per-half independence: excludes carries the *same text* and must convert
    // identically but stay in its own vector.
    let yaml = "rtc:\n  ips:\n    includes: [\"10.0.0.0/8\"]\n    excludes: [\"10.1.0.0/16\"]\n";
    let cfg = rtc(&["--config-body", yaml], &Env::default());
    assert_eq!(
        cfg.ips.includes,
        vec![(v4("10.0.0.0".parse().unwrap()), 8u8)]
    );
    assert_eq!(
        cfg.ips.excludes,
        vec![(v4("10.1.0.0".parse().unwrap()), 16u8)]
    );

    // The parent owns the malformed-entry policy: normalization skips an entry
    // `net.ParseCIDR` rejects instead of failing the load (normalize.rs:306-318
    // comment: `check_ip_filters` refuses startup at the wiring boundary). So a
    // good neighbour survives next to the dropped one, and a wholly bad list
    // yields an empty filter -- never a partial parse of the bad text.
    let yaml = "rtc:\n  ips:\n    includes: [\"not-a-cidr\", \"172.16.0.0/12\"]\n";
    let cfg = rtc(&["--config-body", yaml], &Env::default());
    assert_eq!(
        cfg.ips.includes,
        vec![(v4("172.16.0.0".parse().unwrap()), 12u8)]
    );
    let yaml = "rtc:\n  ips:\n    includes: [\"10.0.0.0/33\", \"::1/129\", \"10.0.0.0\"]\n";
    let cfg = rtc(&["--config-body", yaml], &Env::default());
    assert!(
        cfg.ips.includes.is_empty(),
        "out-of-range prefix lengths and bare IPs are not CIDRs"
    );

    // No ENV / CLI surface for this parent either (same slice rule as
    // `rtc.interfaces`: `config.go:943-1050` skips reflect.Slice, and the port's
    // generated table documents the exclusion at generated.rs:1-13 with its own
    // regression assert at :406). The CIDR conversion above is therefore only
    // reachable through YAML -- which is exactly what the ledger row claims.
    let env = Env::from_pairs([
        ("LIVEKIT_RTC_IPS", "172.20.0.0/16"),
        ("LIVEKIT_RTC_IPS_INCLUDES", "172.20.0.0/16"),
    ]);
    let yaml = "rtc:\n  ips:\n    includes: [\"10.0.0.0/8\"]\n    excludes: [\"192.168.0.0/16\"]\n";
    let cfg = rtc(&["--config-body", yaml], &env);
    assert_eq!(
        cfg.ips.includes,
        vec![(v4("10.0.0.0".parse().unwrap()), 8u8)],
        "the YAML value must survive an unusable env spelling untouched"
    );
    assert_eq!(
        cfg.ips.excludes,
        vec![(v4("192.168.0.0".parse().unwrap()), 16u8)]
    );
    let rejected = entry_action_text(
        &["--config-body", yaml, "--rtc.ips.includes=10.7.0.0/16"],
        &env,
    );
    assert!(
        rejected.contains("Incorrect Usage: flag provided but not defined: -rtc.ips.includes"),
        "measured rejection was: {rejected}"
    );
}

// ---------------------------------------------------------------------------
// YAML:rtc.turn_servers (structural parent)
//
// The parent's claim is the list conversion, which no single child can make:
// `[]config.TURNServer` (config.go:133, element struct at :201-216) becomes
// `Vec<ExternalTurnServer>` (normalize.rs:510-537) with three rewrites that only
// exist at the container level -- protocol string -> enum with a TCP fallback for
// anything unrecognized, `ttl` seconds -> `Duration` with the 0 -> 14400
// (DefaultExternalTURNTTLSeconds, config.go:61) and negative -> 300
// (DefaultTURNTTLSeconds, config.go:57) switches, and inline `secret` ->
// `Option<Vec<u8>>` after trimming (blank == unset, matching LoadTURNSecrets'
// trim-first rule at config.go:857-861). Order and length are preserved.
// ---------------------------------------------------------------------------

#[test]
fn rtc_turn_servers_parent_projects_list_protocol_enum_and_ttl_rules() {
    // Absent parent: empty list, not one default element.
    let base = rtc(&[], &Env::default());
    assert!(base.turn_servers.is_empty());

    // Two elements keep their order; each half of the per-element table is
    // exercised on a different index so a swapped/copied projection fails.
    let yaml = "rtc:\n  turn_servers:\n  \
                - host: a.example\n    port: 3478\n    protocol: udp\n    username: u1\n    credential: c1\n    ttl: 900\n  \
                - host: b.example\n    port: 5349\n    protocol: tls\n    secret: '  s3cret-with-spaces  '\n";
    let cfg = rtc(&["--config-body", yaml], &Env::default());
    assert_eq!(cfg.turn_servers.len(), 2);
    let a = &cfg.turn_servers[0];
    let b = &cfg.turn_servers[1];
    assert_eq!((a.host.as_str(), a.port), ("a.example", 3478i64));
    assert_eq!(a.protocol, IceServerProtocol::Udp);
    assert_eq!((a.username.as_str(), a.credential.as_str()), ("u1", "c1"));
    assert_eq!(a.secret, None, "no secret on the static-auth element");
    assert_eq!(a.ttl, Duration::from_secs(900));
    assert_eq!((b.host.as_str(), b.port), ("b.example", 5349i64));
    assert_eq!(b.protocol, IceServerProtocol::Tls);
    assert_eq!(
        b.secret.as_deref(),
        Some(&b"s3cret-with-spaces"[..]),
        "inline secret is trimmed before it becomes bytes"
    );
    // ttl absent -> the external-TURN default, not zero and not the 5m fallback.
    assert_eq!(b.ttl, Duration::from_secs(14_400));

    // Boundary: an explicit ttl of 0 is the documented "use the default" spelling
    // (config.go:928-929 comment), so it must be 14400, identical to absent.
    let yaml = "rtc:\n  turn_servers:\n  - host: z\n    ttl: 0\n    secret: abc\n";
    let cfg = rtc(&["--config-body", yaml], &Env::default());
    assert_eq!(cfg.turn_servers[0].ttl, Duration::from_secs(14_400));

    // Boundary: a negative ttl falls back to the 5-minute default
    // (ClampTURNTTLSeconds, config.go:904-913 -> DEFAULT_TURN_TTL).
    let yaml = "rtc:\n  turn_servers:\n  - host: z\n    ttl: -1\n    secret: abc\n";
    let cfg = rtc(&["--config-body", yaml], &Env::default());
    assert_eq!(cfg.turn_servers[0].ttl, Duration::from_secs(300));

    // Boundary: an above-max ttl is clamped by NormalizeTURNTTLs
    // (config.go:927-939, TURNMaxTTLSeconds = 86400) *before* projection, so the
    // effective duration is the clamp, not the configured number.
    let yaml = "rtc:\n  turn_servers:\n  - host: z\n    ttl: 999999\n    secret: abc\n";
    let cfg = parse(&["--config-body", yaml], &Env::default());
    assert_eq!(cfg.official.rtc.turn_servers[0].ttl, 86_400);
    assert_eq!(
        cfg.server.rtc.turn_servers[0].ttl,
        Duration::from_secs(86_400)
    );

    // Any unrecognized protocol string lands on TCP, which is what Go's room
    // manager does for everything that is not "udp"/"tls" (normalize.rs:512-517).
    // The match is byte-exact lowercase -- `"UDP"` is *not* `"udp"` upstream, so it
    // must fall through to Tcp; asserting that here keeps the port from silently
    // growing a case-insensitive arm.
    for spelling in ["", "TCP", "quic", "UDP"] {
        let yaml = format!(
            "rtc:\n  turn_servers:\n  - host: p{spelling}\n    protocol: \"{spelling}\"\n    secret: abc\n"
        );
        let cfg = rtc(&["--config-body", &yaml], &Env::default());
        let expected = match spelling {
            "tls" => IceServerProtocol::Tls,
            _ => IceServerProtocol::Tcp,
        };
        assert_eq!(
            cfg.turn_servers[0].protocol, expected,
            "protocol {spelling:?} must project to {expected:?}"
        );
    }

    // A blank-but-present inline secret counts as unset (trim-first rule), so the
    // element projects secret: None; with no username/credential the credential
    // union check then refuses the load (config.go:890-895).
    let yaml = "rtc:\n  turn_servers:\n  - host: q\n    secret: '   '\n";
    let err = std::panic::catch_unwind(|| rtc(&["--config-body", yaml], &Env::default()));
    assert!(
        err.is_err(),
        "a whitespace-only secret with no static credentials is not a usable TURN server"
    );
    let yaml = "rtc:\n  turn_servers:\n  - host: q\n    secret: '   '\n    username: u\n    credential: c\n";
    let cfg = rtc(&["--config-body", yaml], &Env::default());
    assert_eq!(cfg.turn_servers[0].secret, None);
    assert_eq!((cfg.turn_servers[0].username.as_str()), "u");

    // No surface for this parent either: `turn_servers` is a []TURNServer, so
    // `GenerateCLIFlags` (`config.go:943-1050`, `case reflect.Slice: continue`)
    // registers neither a flag nor a `LIVEKIT_RTC_TURN_SERVERS*` env var, and the
    // port mirrors that table (generated.rs:1-13). Measured: the unset flag aborts
    // with urfave's usage error and the unset env names are inert, so the list
    // conversion above is reachable only through YAML.
    let env = Env::from_pairs([
        (
            "LIVEKIT_RTC_TURN_SERVERS",
            "host=env.example,port=3478,protocol=tls",
        ),
        ("LIVEKIT_RTC_TURN_SERVERS_SECRET", "envsecret"),
        ("LIVEKIT_RTC_TURN_SERVERS_0_HOST", "env.example"),
    ]);
    let cfg = rtc(&["--config-body", yaml], &env);
    assert_eq!(cfg.turn_servers.len(), 1, "env must not add elements");
    assert_eq!(
        cfg.turn_servers[0].host, "q",
        "the YAML list must survive an unusable env spelling untouched"
    );
    let rejected = entry_action_text(
        &["--rtc.turn_servers.0.host=cli.example"],
        &Env::from_pairs([
            ("LIVEKIT_RTC_TURN_SERVERS", "host=env.example"),
            ("LIVEKIT_RTC_TURN_SERVERS_SECRET", "envsecret"),
        ]),
    );
    assert!(
        rejected
            .contains("Incorrect Usage: flag provided but not defined: -rtc.turn_servers.0.host"),
        "measured rejection was: {rejected}"
    );
}

// ---------------------------------------------------------------------------
// YAML:rtc.turn_servers[].secret_file
//
// Upstream reader: `LoadTURNSecrets` (config.go:857-898) -- stat, others-perm
// check (ErrTURNSecretFileIncorrectPermission, config.go:72 + the 0o007 filter at
// :858/:872), read, Go-strconv-style trim, empty-file rejection (:881), and the
// both-set warning where the *inline* secret wins (:863-866). The port mirrors it
// in load.rs:384-421 + keys.rs:113-138, and the value then reaches the projection
// only through `ExternalTurnServer.secret` (normalize.rs:530-534). So the honest
// projection assertion is on the loaded secret bytes, not on the file path.
// ---------------------------------------------------------------------------

#[test]
fn rtc_turn_server_secret_file_projection_reads_and_priorities() {
    let path = temp_path("secret");
    write_secret(&path, b"  from-file-secret\n", 0o600);
    let yaml_body = format!(
        "rtc:\n  turn_servers:\n  - host: sf\n    secret_file: {}\n",
        path.display()
    );

    // The file content -- trimmed of leading/trailing space -- is what projects.
    let cfg = rtc(&["--config-body", &yaml_body], &Env::default());
    assert_eq!(
        cfg.turn_servers[0].secret.as_deref(),
        Some(&b"from-file-secret"[..]),
        "secret_file content must be read, trimmed and projected"
    );
    assert_eq!(cfg.turn_servers[0].host, "sf");

    // Both set: the trimmed inline secret wins and the file is not used
    // (config.go:863-866). Assert the exact winner, not merely "some secret".
    let both = format!(
        "rtc:\n  turn_servers:\n  - host: sf\n    secret_file: {}\n    secret: ' inline-wins '\n",
        path.display()
    );
    let cfg = rtc(&["--config-body", &both], &Env::default());
    assert_eq!(
        cfg.turn_servers[0].secret.as_deref(),
        Some(&b"inline-wins"[..])
    );

    // A whitespace-only inline secret falls back to the file (trim-first rule,
    // config.go:859-861), which distinguishes this port from "any non-empty
    // inline string wins".
    let blank_inline = format!(
        "rtc:\n  turn_servers:\n  - host: sf\n    secret_file: {}\n    secret: '   '\n",
        path.display()
    );
    let cfg = rtc(&["--config-body", &blank_inline], &Env::default());
    assert_eq!(
        cfg.turn_servers[0].secret.as_deref(),
        Some(&b"from-file-secret"[..])
    );

    // Permission gate: others-readable is refused before the bytes can project
    // (config.go:872-874).
    std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o644)).unwrap();
    let err = std::panic::catch_unwind(|| rtc(&["--config-body", &yaml_body], &Env::default()));
    assert!(
        err.is_err(),
        "an others-readable secret file must not project its contents"
    );
    std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600)).unwrap();

    // Empty-after-trim file is an error, not a silent secret: None (config.go:879-882).
    let empty = temp_path("secret-empty");
    write_secret(&empty, b"\t \n", 0o600);
    let empty_body = format!(
        "rtc:\n  turn_servers:\n  - host: sf\n    secret_file: {}\n",
        empty.display()
    );
    let err = std::panic::catch_unwind(|| rtc(&["--config-body", &empty_body], &Env::default()));
    assert!(
        err.is_err(),
        "a blank secret file is rejected, not projected"
    );

    // Missing file: stat error propagates (config.go:868-870).
    let missing = temp_path("secret-missing");
    let missing_body = format!(
        "rtc:\n  turn_servers:\n  - host: sf\n    secret_file: {}\n",
        missing.display()
    );
    let err = std::panic::catch_unwind(|| rtc(&["--config-body", &missing_body], &Env::default()));
    assert!(err.is_err(), "a nonexistent secret file must fail the load");

    // The only source that can supply this leaf is YAML -- `turn_servers` is a
    // slice, so upstream registers no flag/env for it or its elements
    // (`config.go:943-1050`). Measured: the dotted env names are inert and the
    // dotted flag aborts the parse with urfave's usage error.
    let env = Env::from_pairs([("LIVEKIT_RTC_TURN_SERVERS_SECRET_FILE", "/dev/null")]);
    let cfg = rtc(&["--config-body", &yaml_body], &env);
    assert_eq!(
        cfg.turn_servers[0].secret.as_deref(),
        Some(&b"from-file-secret"[..]),
        "an unregistered env name must not redirect the file read"
    );
    let rejected = entry_action_text(
        &[
            "--config-body",
            &yaml_body,
            "--rtc.turn_servers.0.secret_file=/dev/null",
        ],
        &Env::default(),
    );
    assert!(
        rejected.contains(
            "Incorrect Usage: flag provided but not defined: -rtc.turn_servers.0.secret_file"
        ),
        "measured rejection was: {rejected}"
    );

    // Binary (non-UTF-8) secret bytes: the YAML view is text-only, so the loader
    // clears the inline string and hands the raw bytes to the projection through
    // `binary_turn_secrets` (load.rs:398-407 + load.rs:444-446). This is the one
    // place where official (`secret: String`) and projected (`secret: Vec<u8>`)
    // legitimately differ, so assert both sides of it.
    let binary = temp_path("secret-binary");
    write_secret(&binary, b"\xff\xfe\x01raw-bytes\n", 0o600);
    let binary_body = format!(
        "rtc:\n  turn_servers:\n  - host: bin\n    secret_file: {}\n",
        binary.display()
    );
    let loaded = parse(&["--config-body", &binary_body], &Env::default());
    assert_eq!(
        loaded.official.rtc.turn_servers[0].secret, "",
        "invalid UTF-8 cannot live in the text tree"
    );
    assert_eq!(
        loaded.server.rtc.turn_servers[0].secret.as_deref(),
        Some(&b"\xff\xfe\x01raw-bytes"[..]),
        "the trimmed raw bytes must still reach the projected runtime config"
    );

    let _ = std::fs::remove_file(&path);
    let _ = std::fs::remove_file(&empty);
    let _ = std::fs::remove_file(&binary);
}

// ---------------------------------------------------------------------------
// YAML:rtc.pli_throttle (structural parent)
//
// Parent of three duration leaves (already MET individually). Its own claim is
// the container conversion: `sfu.PLIThrottleConfig{low,mid,high quality}`
// (config.go:151) -> `PliThrottle{low,mid,high}` with a per-field duration->
// std::Duration `to_std()` and the upstream defaults 500ms/1s/1s
// (sfu.PLIThrottleConfig default, mirrored at official.rs:368-372). A projection
// that crossed two of the three fields, or converted one to the wrong unit, is
// only visible at the parent level.
// ---------------------------------------------------------------------------

#[test]
fn rtc_pli_throttle_parent_projects_three_distinct_durations() {
    // Absent: the upstream defaults must appear, each different so a swap is
    // observable.
    let base = rtc(&[], &Env::default());
    assert_eq!(base.pli_throttle.low, Duration::from_millis(500));
    assert_eq!(base.pli_throttle.mid, Duration::from_secs(1));
    assert_eq!(base.pli_throttle.high, Duration::from_secs(1));

    // Three mutually distinct, non-round values -> each field lands in its own
    // slot; a mis-ordered or shared projection cannot pass.
    let yaml = "rtc:\n  pli_throttle:\n    low_quality: 123ms\n    mid_quality: 2s\n    high_quality: 750us\n";
    let cfg = rtc(&["--config-body", yaml], &Env::default());
    assert_eq!(cfg.pli_throttle.low, Duration::from_millis(123));
    assert_eq!(cfg.pli_throttle.mid, Duration::from_secs(2));
    assert_eq!(cfg.pli_throttle.high, Duration::from_micros(750));
    assert_ne!(cfg.pli_throttle.low, base.pli_throttle.low);
    assert_ne!(cfg.pli_throttle.high, cfg.pli_throttle.mid);

    // Sub-default precision: 499ms differs from the 500ms default by one
    // millisecond, so a "default when small" projection reddens here.
    let yaml = "rtc:\n  pli_throttle:\n    low_quality: 499ms\n";
    let cfg = rtc(&["--config-body", yaml], &Env::default());
    assert_eq!(cfg.pli_throttle.low, Duration::from_millis(499));
    assert_eq!(
        cfg.pli_throttle.mid, base.pli_throttle.mid,
        "unset siblings keep their defaults"
    );

    // Zero is a legal configured value (it disables throttling) and must not
    // snap back to the default. A bare integer is not a time.Duration upstream,
    // hence the "0s" spelling -- same acceptance set the pinned Go has
    // ("cannot unmarshal !!int `500` into time.Duration").
    let yaml =
        "rtc:\n  pli_throttle:\n    low_quality: 0s\n    mid_quality: 0s\n    high_quality: 0s\n";
    let cfg = rtc(&["--config-body", yaml], &Env::default());
    assert_eq!(
        cfg.pli_throttle.low,
        Duration::ZERO,
        "explicit zero is not the 500ms default"
    );
    assert_eq!(cfg.pli_throttle.mid, Duration::ZERO);
    assert_eq!(cfg.pli_throttle.high, Duration::ZERO);
    let bad = std::panic::catch_unwind(|| {
        rtc(
            &[
                "--config-body",
                "rtc:\n  pli_throttle:\n    low_quality: 500\n",
            ],
            &Env::default(),
        )
    });
    assert!(bad.is_err(), "a bare integer is not a time.Duration");

    // Parent-level precedence: ENV replaces one field only, CLI replaces another,
    // and the untouched third still comes from YAML.
    let yaml = "rtc:\n  pli_throttle:\n    low_quality: 10ms\n    mid_quality: 20ms\n    high_quality: 30ms\n";
    let env = Env::from_pairs([("LIVEKIT_RTC_PLI_THROTTLE_MID_QUALITY", "44ms")]);
    let cfg = rtc(
        &[
            "--config-body",
            yaml,
            "--rtc.pli_throttle.high_quality=55ms",
        ],
        &env,
    );
    assert_eq!(cfg.pli_throttle.low, Duration::from_millis(10), "YAML");
    assert_eq!(cfg.pli_throttle.mid, Duration::from_millis(44), "ENV");
    assert_eq!(cfg.pli_throttle.high, Duration::from_millis(55), "CLI");
}

// ---------------------------------------------------------------------------
// YAML:rtc.congestion_control (structural parent)
//
// Parent of the CC family (its scalars are MET elsewhere). Its own claim is the
// container conversion at normalize.rs:208-234: the pacer *string* becomes the
// `SendSidePacer` enum with an explicit reject for unknown spellings (mirroring
// CongestionControlConfig.Validate, config.go:234-247, reached from
// config.go:700), the four booleans keep their individual identities against the
// upstream defaults enabled=true / allow_pause=false / use_send_side_bwe=false /
// use_send_side_bwe_interceptor=false (config.go:582-590, official.rs:380-389),
// and the nested allocator/remote_bwe/send_side_bwe sub-configs travel with it.
// ---------------------------------------------------------------------------

#[test]
fn rtc_congestion_control_parent_projects_pacer_enum_and_flags() {
    use rtc_core::config::SendSidePacer;
    // Absent: upstream defaults, including the code-applied "no-queue" pacer
    // (config.go:589) rather than the empty string.
    let base = rtc(&[], &Env::default());
    assert!(base.congestion_control.enabled);
    assert!(!base.congestion_control.allow_pause);
    assert!(!base.congestion_control.use_send_side_bwe);
    assert!(!base.congestion_control.use_send_side_bwe_interceptor);
    assert_eq!(
        base.congestion_control.send_side_bwe_pacer,
        SendSidePacer::NoQueue
    );

    // All four flags flipped together plus a legal non-default pacer: every one
    // must arrive in its own slot (a collapsed or swapped projection fails).
    let yaml = "rtc:\n  congestion_control:\n    enabled: false\n    allow_pause: true\n    use_send_side_bwe: true\n    use_send_side_bwe_interceptor: true\n    send_side_bwe_pacer: leaky-bucket\n";
    let cfg = rtc(&["--config-body", yaml], &Env::default());
    assert!(!cfg.congestion_control.enabled);
    assert!(cfg.congestion_control.allow_pause);
    assert!(cfg.congestion_control.use_send_side_bwe);
    assert!(cfg.congestion_control.use_send_side_bwe_interceptor);
    assert_eq!(
        cfg.congestion_control.send_side_bwe_pacer,
        SendSidePacer::LeakyBucket
    );
    // The derived feedback mode follows the *projected* enum/flags, proving the
    // consumer sees the parent's conversion rather than the raw strings.
    assert_eq!(
        cfg.congestion_control.effective_feedback_mode(),
        rtc_core::config::FeedbackMode::NativeSendSide
    );

    // Each remaining pacer spelling maps to its own variant; "" is the
    // Unspecified case (normalize.rs:213).
    for (spelling, want) in [
        ("pass-through", SendSidePacer::PassThrough),
        ("no-queue", SendSidePacer::NoQueue),
    ] {
        let y = format!("rtc:\n  congestion_control:\n    send_side_bwe_pacer: {spelling}\n");
        assert_eq!(
            rtc(&["--config-body", &y], &Env::default())
                .congestion_control
                .send_side_bwe_pacer,
            want,
            "pacer {spelling}"
        );
    }
    let y = "rtc:\n  congestion_control:\n    send_side_bwe_pacer: \"\"\n";
    assert_eq!(
        rtc(&["--config-body", y], &Env::default())
            .congestion_control
            .send_side_bwe_pacer,
        SendSidePacer::Unspecified
    );

    // An unknown pacer must fail closed with the path named (Validate's contract,
    // config.go:234-247): not silently fall back to the default.
    let err = std::panic::catch_unwind(|| {
        rtc(
            &[
                "--config-body",
                "rtc:\n  congestion_control:\n    send_side_bwe_pacer: turbo-queue\n",
            ],
            &Env::default(),
        )
    });
    assert!(
        err.is_err(),
        "an unknown send_side_bwe_pacer must reject the load"
    );

    // Nested children travel with the parent: one deliberate non-default in each
    // of the three sub-structs proves the parent does not rebuild them from
    // defaults.
    let yaml = "rtc:\n  congestion_control:\n    stream_allocator:\n      min_channel_capacity: 123456\n    remote_bwe:\n      nack_ratio_attenuator: 0.25\n    send_side_bwe:\n      congestion_detector:\n        jqr_min_delay: 77ms\n";
    let cfg = rtc(&["--config-body", yaml], &Env::default());
    assert_eq!(
        cfg.congestion_control.stream_allocator.min_channel_capacity,
        123_456
    );
    assert_eq!(
        cfg.congestion_control.remote_bwe.nack_ratio_attenuator,
        0.25
    );
    assert_eq!(
        cfg.congestion_control
            .send_side_bwe
            .congestion_detector
            .jqr_min_delay
            .as_millis_i64(),
        77
    );

    // Parent-level precedence across sources on two different children.
    let env = Env::from_pairs([(
        "LIVEKIT_RTC_CONGESTION_CONTROL_STREAM_ALLOCATOR_MIN_CHANNEL_CAPACITY",
        "999",
    )]);
    let cfg = parse(&["--config-body", yaml], &env);
    assert_eq!(
        cfg.server
            .rtc
            .congestion_control
            .stream_allocator
            .min_channel_capacity,
        999,
        "ENV beats YAML inside the parent"
    );
    assert_eq!(
        cfg.server
            .rtc
            .congestion_control
            .remote_bwe
            .nack_ratio_attenuator,
        0.25,
        "untouched sibling keeps the YAML value"
    );
    let cfg = rtc(
        &[
            "--config-body",
            yaml,
            "--rtc.congestion_control.enabled=false",
            "--rtc.congestion_control.remote_bwe.nack_ratio_attenuator=0.5",
        ],
        &env,
    );
    assert!(!cfg.congestion_control.enabled, "CLI beats the YAML true");
    assert_eq!(cfg.congestion_control.remote_bwe.nack_ratio_attenuator, 0.5);
    assert_eq!(
        cfg.congestion_control.stream_allocator.min_channel_capacity,
        999
    );
}

// ---------------------------------------------------------------------------
// YAML:rtc.forward_stats (structural parent)
//
// Parent of the three interval leaves. Its own claim is the activation rule the
// container encodes: upstream builds the collector only when *all three*
// durations are nonzero (`wire_gen.go:346-350` / `wire.go:292-296`, mirrored at
// normalize.rs:555-574), i.e. the projection is `Option<Arc<ForwardStats>>` and
// any zero means None. That is a parent-level decision no single child leg makes.
// ---------------------------------------------------------------------------

#[test]
fn rtc_forward_stats_parent_activation_rule() {
    // Absent: all three zero upstream (config.go:534-538 has no DefaultConfig
    // entry) -> no collector at all.
    let base = rtc(&[], &Env::default());
    assert!(base.forward_stats.is_none());

    // All three nonzero -> exactly one collector exists, and it is the *same* Arc
    // the engine later reads (clone shares the allocation).
    let yaml = "rtc:\n  forward_stats:\n    summary_interval: 100ms\n    report_interval: 1s\n    report_window: 5s\n";
    let cfg = rtc(&["--config-body", yaml], &Env::default());
    let stats = cfg
        .forward_stats
        .clone()
        .expect("three nonzero intervals must activate the collector");
    assert!(std::sync::Arc::ptr_eq(
        &stats,
        &cfg.forward_stats.clone().unwrap()
    ));

    // One-zero-per-position matrix: each position alone is enough to disable it,
    // which is the whole parent claim. Values are otherwise nonzero, so a
    // projection that ORs instead of ANDs the check reddens on every row.
    let rows = [
        ("summary_interval", "0s", "111ms", "5s"),
        ("report_interval", "100ms", "0s", "5s"),
        ("report_window", "100ms", "1s", "0s"),
    ];
    for (zero_key, a, b, c) in rows {
        let y = format!(
            "rtc:\n  forward_stats:\n    summary_interval: {a}\n    report_interval: {b}\n    report_window: {c}\n"
        );
        let cfg = rtc(&["--config-body", &y], &Env::default());
        assert!(
            cfg.forward_stats.is_none(),
            "{zero_key}: 0 must deactivate the collector (wire_gen.go:347)"
        );
    }

    // Precedence at the parent level: YAML sets all three, ENV zeroes one -> the
    // collector disappears even though two leaves came from YAML.
    let env = Env::from_pairs([("LIVEKIT_RTC_FORWARD_STATS_REPORT_WINDOW", "0s")]);
    let cfg = parse(&["--config-body", yaml], &env);
    assert!(
        cfg.server.rtc.forward_stats.is_none(),
        "ENV zero must remove the parent's collector"
    );
    // ... and the CLI can bring it back with a nonzero window.
    let cfg = rtc(
        &[
            "--config-body",
            yaml,
            "--rtc.forward_stats.report_window=6s",
        ],
        &env,
    );
    assert!(cfg.forward_stats.is_some());
}

// ---------------------------------------------------------------------------
// YAML:rtc.forward_stats.summary_interval
//
// Leaf of the activated collector. The only outward handle ForwardStats exposes
// for the summary tick is its deadline arithmetic (`next_deadline()` /
// `tick()`), so the leg asserts the real object's timing phase, which is derived
// from the projected summary duration (normalize.rs:566-571 ->
// rtc-core/src/forward_stats.rs:166-203). 123ms is deliberately not a round
// second, not a multiple of the report interval and differs from every sibling.
// ---------------------------------------------------------------------------

#[test]
fn rtc_forward_stats_summary_interval_leaf_projection() {
    let yaml = "rtc:\n  forward_stats:\n    summary_interval: 123ms\n    report_interval: 7s\n    report_window: 21s\n";
    let cfg = rtc(&["--config-body", yaml], &Env::default());
    let stats = cfg
        .forward_stats
        .expect("summary interval projects a collector");
    // Phase: origin(now) + summary_interval, i.e. exactly 123 ms of nanoseconds,
    // not 123s, not 123000us rounded to something else, not the report period.
    let first = stats.next_deadline();
    let elapsed_ms = first.saturating_duration_since(Instant::now()).as_millis();
    assert!(
        (120..=123).contains(&elapsed_ms),
        "summary deadline must be ~123ms out, got {elapsed_ms}ms"
    );

    // Behaviour: one summary flush per summary tick, and the rolling p95-ish
    // report uses it. Feed samples, advance past two summary deadlines and watch
    // the cumulative histogram count grow by exactly the sample count each flush.
    let t0 = Instant::now();
    stats.record_forward_samples(t0, [t0, t0]);
    stats.tick(t0 + Duration::from_millis(124));
    assert_eq!(stats.snapshot().histogram_count, 2);
    stats.record_forward_samples(t0, [t0]);
    stats.tick(t0 + Duration::from_millis(248));
    assert_eq!(stats.snapshot().histogram_count, 3);
    // Before the next summary deadline nothing more is flushed.
    stats.record_forward_samples(t0, [t0]);
    stats.tick(t0 + Duration::from_millis(250));
    assert_eq!(
        stats.snapshot().histogram_count,
        3,
        "a tick inside the summary span must not flush"
    );

    // ENV / CLI surfaces change the projected timing, not just the raw tree.
    let env = Env::from_pairs([("LIVEKIT_RTC_FORWARD_STATS_SUMMARY_INTERVAL", "450ms")]);
    let cfg = parse(&["--config-body", yaml], &env);
    let s = cfg
        .server
        .rtc
        .forward_stats
        .as_ref()
        .expect("ENV keeps the collector active");
    let ms = s
        .next_deadline()
        .saturating_duration_since(Instant::now())
        .as_millis();
    assert!(
        (447..=450).contains(&ms),
        "ENV summary interval must project, got {ms}ms"
    );
    let cfg = rtc(
        &[
            "--config-body",
            yaml,
            "--rtc.forward_stats.summary_interval=67ms",
        ],
        &env,
    );
    let s = cfg.forward_stats.as_ref().unwrap();
    let ms = s
        .next_deadline()
        .saturating_duration_since(Instant::now())
        .as_millis();
    assert!(
        (64..=67).contains(&ms),
        "CLI must beat ENV on the summary interval, got {ms}ms"
    );
}

// ---------------------------------------------------------------------------
// YAML:rtc.forward_stats.report_interval
//
// Second leaf of the collector. The report tick is the only thing that publishes
// `output.latency_ns` / `dropped_samples` (rtc-core/src/forward_stats.rs:274-297),
// so the leg asserts that a report tick happened exactly at the configured
// distance and at no earlier summary tick. 7s vs a 123ms summary makes the two
// periods unmistakable.
// ---------------------------------------------------------------------------

#[test]
fn rtc_forward_stats_report_interval_leaf_projection() {
    let yaml = "rtc:\n  forward_stats:\n    summary_interval: 123ms\n    report_interval: 7s\n    report_window: 21s\n";
    let cfg = rtc(&["--config-body", yaml], &Env::default());
    let stats = cfg
        .forward_stats
        .clone()
        .expect("report interval projects a collector");
    let t0 = Instant::now();
    // The sample must carry a *nonzero* forwarding latency: the published value is
    // the ring p90 of recorded latencies, and `left == arrival` would record 0 ns,
    // which percentile() cannot distinguish from "no samples" (both give 0).
    stats.record_forward_samples(t0, [t0 - Duration::from_millis(3)]);
    // Summary ticks do not publish a report.
    stats.tick(t0 + Duration::from_millis(124));
    stats.tick(t0 + Duration::from_millis(370));
    assert_eq!(
        stats.snapshot().latency_ns,
        0,
        "no report may be published before the report interval elapses"
    );
    // Exactly at the report deadline the output is published, and it is the
    // recorded 3ms latency -- proof the reported number came from this collector.
    stats.tick(t0 + Duration::from_secs(7));
    assert_eq!(
        stats.snapshot().latency_ns,
        3_000_000,
        "the 7s report tick must publish the summary"
    );

    // ENV changes the published cadence: with a 1s report interval a tick at 1s
    // publishes, whereas the YAML 7s would not have.
    let env = Env::from_pairs([("LIVEKIT_RTC_FORWARD_STATS_REPORT_INTERVAL", "1s")]);
    let cfg = parse(&["--config-body", yaml], &env);
    let stats = cfg
        .server
        .rtc
        .forward_stats
        .as_ref()
        .expect("collector stays active");
    let t1 = Instant::now();
    stats.record_forward_samples(t1, [t1 - Duration::from_millis(3)]);
    stats.tick(t1 + Duration::from_millis(500));
    assert_eq!(
        stats.snapshot().latency_ns,
        0,
        "ENV 1s report must not publish at 500ms"
    );
    stats.tick(t1 + Duration::from_secs(1));
    assert_ne!(
        stats.snapshot().latency_ns,
        0,
        "ENV report interval must drive the publish tick"
    );

    // CLI beats ENV: 2s report interval publishes at 2s and not at 1s.
    let cfg = rtc(
        &[
            "--config-body",
            yaml,
            "--rtc.forward_stats.report_interval=2s",
        ],
        &env,
    );
    let stats = cfg.forward_stats.as_ref().unwrap();
    let t2 = Instant::now();
    stats.record_forward_samples(t2, [t2 - Duration::from_millis(3)]);
    stats.tick(t2 + Duration::from_secs(1));
    assert_eq!(stats.snapshot().latency_ns, 0, "CLI 2s must not fire at 1s");
    stats.tick(t2 + Duration::from_secs(2));
    assert_eq!(
        stats.snapshot().latency_ns,
        3_000_000,
        "CLI report interval must reach the collector"
    );
}

// ---------------------------------------------------------------------------
// YAML:rtc.forward_stats.report_window
//
// Third leaf. The window is the only one of the three whose projection becomes a
// *count*: `ForwardStats::new` sizes the summary ring with ceil(window / summary)
// (rtc-core/src/forward_stats.rs:175) and `summarize` merges
// min(len, ceil(window/summary)) slots (:253-266), so the retention depth is what a
// wrong-unit projection moves. The leg below drives one collector per source (YAML
// 500ms, ENV 2s, CLI 100ms) with identical traffic and asserts three different p90
// answers, which only a capacity difference can produce.
// ---------------------------------------------------------------------------

#[test]
fn rtc_forward_stats_report_window_leaf_projection() {
    // Stimulus: six one-sample flushes 100ms apart carrying 9,8,7,6,5,4ms. The
    // latencies are deliberately never 0ms, because a ring holding a 0ms sample and
    // an empty ring both answer 0 and would make the capacity claims ambiguous.
    // Every expected value was derived from the bucket-percentile arithmetic at
    // forward_stats.rs:103-120 before being asserted.
    let yaml = "rtc:\n  forward_stats:\n    summary_interval: 100ms\n    report_interval: 1s\n    report_window: 500ms\n";
    let cfg = rtc(&["--config-body", yaml], &Env::default());
    let stats = cfg
        .forward_stats
        .clone()
        .expect("window projects a collector");
    let t0 = Instant::now();
    for i in 1..=6u32 {
        let left = t0 + Duration::from_millis(i as u64 * 100 + 50);
        stats.record_forward_samples(left, [left - Duration::from_millis((10 - i) as u64)]);
        stats.tick(left);
    }
    // ceil(500/100) = 5 slots, so the oldest flush (9ms) is evicted and the retained
    // set is {8,7,6,5,4}ms. Target is 0.9*5 = 4.5; the walk reaches the top bucket,
    // which holds exactly one sample, and returns 8ms.
    assert_eq!(
        stats.get_stats_ns(0),
        8_000_000,
        "500ms window over 100ms summaries must retain exactly 5 slots"
    );
    // A sub-summary request rounds up to whole summary spans and never below one slot
    // (forward_stats.rs:257-258), so 1ms selects the newest span only: 4ms, neither
    // the 5-slot answer nor the 0 that a zero-slot clamp would give.
    assert_eq!(
        stats.get_stats_ns(1_000_000),
        4_000_000,
        "the window request must clamp to one slot, not zero"
    );
    // The cumulative histogram is unaffected by the window: it counts every flush.
    assert_eq!(stats.snapshot().histogram_count, 6);

    // Same traffic with a 2s ENV window: ceil(2000/100) = 20 slots, nothing evicted,
    // so the oldest flush survives and target 0.9*6 = 5.4 walks to 9ms.
    let env = Env::from_pairs([("LIVEKIT_RTC_FORWARD_STATS_REPORT_WINDOW", "2s")]);
    let cfg = parse(&["--config-body", yaml], &env);
    let wide = cfg
        .server
        .rtc
        .forward_stats
        .as_ref()
        .expect("nonzero window keeps the collector");
    let t1 = Instant::now();
    for i in 1..=6u32 {
        let left = t1 + Duration::from_millis(i as u64 * 100 + 50);
        wide.record_forward_samples(left, [left - Duration::from_millis((10 - i) as u64)]);
        wide.tick(left);
    }
    assert_eq!(
        wide.get_stats_ns(0),
        9_000_000,
        "a 2s window must retain the oldest summary and move the p90"
    );

    // CLI beats ENV on this leaf alone: the ring collapses to one slot while the
    // sibling leaves keep their YAML values.
    let cfg = rtc(
        &[
            "--config-body",
            yaml,
            "--rtc.forward_stats.report_window=100ms",
        ],
        &env,
    );
    let narrow = cfg
        .forward_stats
        .as_ref()
        .expect("CLI window keeps the collector active");
    let t2 = Instant::now();
    for i in 1..=6u32 {
        let left = t2 + Duration::from_millis(i as u64 * 100 + 50);
        narrow.record_forward_samples(left, [left - Duration::from_millis((10 - i) as u64)]);
        narrow.tick(left);
    }
    // ceil(100/100) = 1: only the newest flush is in the window, so identical traffic
    // answers 4ms where the 5-slot ring answered 8ms and the 20-slot ring 9ms.
    assert_eq!(
        narrow.get_stats_ns(0),
        4_000_000,
        "CLI 100ms window must retain exactly one summary"
    );
    // One further summary tick with nothing recorded replaces that single slot with an
    // empty summary, so the window answer drops to 0 while the histogram still counts
    // six. The same tick on the 20-slot collector keeps the older summaries and still
    // answers 9ms -- the ring capacity alone decides the published number, which is
    // what /metrics' livekit_forward_latency gauge reads (livecloud/src/metrics.rs:198).
    narrow.tick(t2 + Duration::from_millis(750));
    assert_eq!(
        narrow.get_stats_ns(0),
        0,
        "a one-slot ring must be overwritten by the empty summary"
    );
    assert_eq!(
        narrow.snapshot().histogram_count,
        6,
        "eviction must not rewrite the cumulative histogram"
    );
    wide.tick(t1 + Duration::from_millis(750));
    assert_eq!(
        wide.get_stats_ns(0),
        9_000_000,
        "a 20-slot ring retains what the one-slot ring dropped"
    );
    // The due report tick publishes the wide ring's answer, proving the window feeds
    // the reported gauge and not only the on-demand query.
    wide.tick(t1 + Duration::from_millis(1050));
    assert_eq!(
        wide.snapshot().latency_ns,
        9_000_000,
        "the report tick must publish the 20-slot retention"
    );
    // The window override must not disturb its siblings: `next_deadline` is the earlier
    // of the summary and report deadlines (forward_stats.rs:311-314), and both stay on
    // the grid anchored one interval after construction — summary steps of 100ms land
    // the next one at t2+800ms, never a full second late.
    let nd = narrow.next_deadline();
    assert!(
        nd > t2 + Duration::from_millis(750) && nd <= t2 + Duration::from_millis(800),
        "summary_interval must stay on its 100ms grid through a window-only override, got {nd:?}"
    );
}
