//! C98-B1 config_projection legs for the nine `logging.*` / `prometheus.*` rows.
//!
//! Tier 2 only (SPEC-0075 §2): each leg drives the public loading API
//! (`parse_with_probe`) through YAML / ENV / CLI and asserts on the **projected**
//! `rtc_core::config::{LoggingConfig, PrometheusConfig}` (normalize.rs:813-857), plus the
//! load-stage rules that feed them (`load.rs:649-662` for `log_level`, dev-level and
//! `pion_level`), never on the parsed tree alone.
//!
//! Go oracle: references/livekit-stable @ e5655ac (v1.13.10). Generated flag/env surface:
//! pkg/config/config.go:943-1050 -- maps and slices get no flag, which is why
//! `logging.component_levels` has no `--logging.component_levels` and no
//! `LIVEKIT_LOGGING_COMPONENT_LEVELS`; both negative arms are pinned below.

use lk_config::{
    EntryAction, Env, ProgramInfo, generated::specs, netdetect::NetProbe, parse_with_probe,
};
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

/// `Mode::Run` refuses to start without keys (load.rs:689), so every leg is loaded with an
/// upstream-shaped `keys:` map. The secret is 40 chars to keep the short-secret notice out of
/// the warning asserts, and it stays first so a leg's own body cannot override it by order.
const KEYS_BODY: &str = "keys:\n  c98key: \"0123456789012345678901234567890123456789\"\n";

fn load(args: &[&str], env: &Env) -> Box<lk_config::LoadedConfig> {
    let info = ProgramInfo {
        binary_name: "livecloud".into(),
        product_version: "fixture".into(),
        compat_version: "1.13.10",
    };
    let mut argv: Vec<OsString> = ["livecloud"].into_iter().map(OsString::from).collect();
    argv.extend(args.iter().map(OsString::from));
    let keyed;
    let argv = match argv.iter().position(|a| a == "--config-body") {
        Some(pos) => {
            let own = argv[pos + 1].to_str().expect("utf-8 body").to_string();
            argv[pos + 1] = OsString::from(format!("{KEYS_BODY}{own}"));
            argv
        }
        None => {
            keyed = [OsString::from("--config-body"), OsString::from(KEYS_BODY)];
            argv.extend(keyed);
            argv
        }
    };
    match parse_with_probe(argv, env, &info, &NoNetwork) {
        Ok(EntryAction::RunServer(config)) => config,
        Ok(other) => panic!("expected RunServer, got other entry action: {other:?}"),
        Err(e) => panic!("parse_with_probe error: {e}"),
    }
}

fn logging(args: &[&str], env: &Env) -> rtc_core::config::LoggingConfig {
    load(args, env).server.logging.clone()
}

fn prometheus(args: &[&str], env: &Env) -> rtc_core::config::PrometheusConfig {
    load(args, env).server.prometheus.clone()
}

fn body(yaml: &str) -> Vec<String> {
    load(&["--config-body", yaml], &Env::default())
        .warnings
        .clone()
}

// ---------------------------------------------------------------------------
// YAML:logging.json
// ---------------------------------------------------------------------------

#[test]
fn logging_json_projects_from_all_three_sources_in_precedence_order() {
    let base = logging(&["--dev"], &Env::default());
    assert!(!base.json, "C98B1-01 Go default is false");

    let yaml = logging(
        &["--config-body", "logging:\n  json: true\n"],
        &Env::default(),
    );
    assert!(yaml.json, "C98B1-02 YAML logging.json must project");

    let env_on = logging(
        &["--dev"],
        &Env::from_pairs([("LIVEKIT_LOGGING_JSON", "true")]),
    );
    assert!(env_on.json, "C98B1-03 LIVEKIT_LOGGING_JSON must project");

    let cli = logging(
        &["--dev", "--logging.json=true"],
        &Env::from_pairs([("LIVEKIT_LOGGING_JSON", "false")]),
    );
    assert!(cli.json, "C98B1-04 CLI must beat the environment value");

    let over = logging(
        &[
            "--config-body",
            "logging:\n  json: true\n",
            "--logging.json=false",
        ],
        &Env::default(),
    );
    assert!(
        !over.json,
        "C98B1-05 CLI false must beat YAML true, not merge into it"
    );
}

// ---------------------------------------------------------------------------
// YAML:logging.level
//
// normalize.rs:814-824 lower-cases, maps empty to "info" and falls back to "info" with a
// warning for anything outside LOG_LEVELS. load.rs:650-654 lets the top-level `log_level`
// win outright and turns an empty level into "debug" in development mode.
// ---------------------------------------------------------------------------

#[test]
fn logging_level_is_lower_cased_before_it_projects() {
    let got = logging(
        &["--config-body", "logging:\n  level: WARN\n"],
        &Env::default(),
    );
    assert_eq!(
        "warn", got.level,
        "C98B1-06 the projected level is the lower-cased one"
    );
    assert!(
        body("logging:\n  level: WARN\n").is_empty(),
        "C98B1-06 a valid level must not warn: {:?}",
        body("logging:\n  level: WARN\n")
    );
}

#[test]
fn logging_level_unknown_value_projects_as_info_with_the_upstream_warning() {
    let got = logging(
        &["--config-body", "logging:\n  level: verbose\n"],
        &Env::default(),
    );
    assert_eq!(
        "info", got.level,
        "C98B1-07 an out-of-set level projects as info, not verbose"
    );
    assert_eq!(
        vec!["logging.level \"verbose\" is not a valid level, using \"info\"".to_string()],
        got_warnings("logging:\n  level: verbose\n"),
        "C98B1-07 the fallback must be announced"
    );
}

fn got_warnings(yaml: &str) -> Vec<String> {
    load(&["--config-body", yaml], &Env::default()).warnings
}

#[test]
fn logging_level_empty_dev_and_log_level_rules_project() {
    let empty_prod = logging(
        &["--config-body", "logging:\n  level: \"\"\n"],
        &Env::default(),
    );
    assert_eq!(
        "info", empty_prod.level,
        "C98B1-08 an empty level is info outside development mode"
    );

    let empty_dev = logging(
        &["--dev", "--config-body", "logging:\n  level: \"\"\n"],
        &Env::default(),
    );
    assert_eq!(
        "debug", empty_dev.level,
        "C98B1-09 development mode turns the empty level into debug (load.rs:653)"
    );

    let forced = logging(
        &[
            "--config-body",
            "log_level: debug\nlogging:\n  level: warn\n",
        ],
        &Env::default(),
    );
    assert_eq!(
        "debug", forced.level,
        "C98B1-10 the top-level log_level wins over logging.level (load.rs:650)"
    );

    let env_level = logging(&[], &Env::from_pairs([("LIVEKIT_LOGGING_LEVEL", "ERROR")]));
    assert_eq!(
        "error", env_level.level,
        "C98B1-11 the generated env value is lower-cased on the way in"
    );
}

// ---------------------------------------------------------------------------
// YAML:logging.component_levels
//
// A map, so Go's GenerateCLIFlags (config.go:943-1050) gives it neither a flag nor an env
// var. The two negative arms below pin that fact instead of leaving it assumed.
// ---------------------------------------------------------------------------

#[test]
fn logging_component_levels_projects_every_entry_and_has_no_env_or_cli_surface() {
    let yaml = "logging:\n  component_levels:\n    zrouter: debug\n    asignal: trace\n";
    let got = logging(&["--config-body", yaml], &Env::default());
    assert_eq!(
        4,
        got.component_levels.len(),
        "C98B1-12 the two operator entries plus the two pion_level seeds must survive: {:?}",
        got.component_levels
    );
    assert_eq!(
        "debug", got.component_levels["zrouter"],
        "C98B1-12 per-key value must project verbatim"
    );
    assert_eq!(
        "trace", got.component_levels["asignal"],
        "C98B1-12 the second key must not take the first value"
    );
    let keys: Vec<&str> = got.component_levels.keys().map(String::as_str).collect();
    assert_eq!(
        vec!["asignal", "pion", "transport.pion", "zrouter"],
        keys,
        "C98B1-12 the projected map is keyed by the configured names and the pion seeds"
    );

    let absent = logging(&["--dev"], &Env::default());
    assert_eq!(
        vec!["pion", "transport.pion"],
        absent
            .component_levels
            .keys()
            .map(String::as_str)
            .collect::<Vec<_>>(),
        "C98B1-13 an absent map still carries the pion_level seeds: {:?}",
        absent.component_levels
    );
    assert_eq!(
        "error", absent.component_levels["pion"],
        "C98B1-13 DefaultConfig.Logging.PionLevel is \"error\" (config.go:634, fanned out at \
         config.go:731-736 / load.rs:656-662), so even the bare map is not empty"
    );

    let flag = "--logging.component_levels=zrouter=debug";
    let text = entry_action_text(&[flag]);
    assert!(
        text.contains("unexpected flag") || text.contains("error") || text.contains("Usage"),
        "C98B1-14 the map must not gain a CLI surface, got: {text}"
    );
    let env_text = logging(
        &["--dev"],
        &Env::from_pairs([("LIVEKIT_LOGGING_COMPONENT_LEVELS", "zrouter=debug")]),
    );
    assert_eq!(
        vec!["pion", "transport.pion"],
        env_text
            .component_levels
            .keys()
            .map(String::as_str)
            .collect::<Vec<_>>(),
        "C98B1-15 the env name must stay inert for the map: {:?}",
        env_text.component_levels
    );
    assert!(
        specs().iter().all(|s| s.path != "logging.component_levels"),
        "C98B1-14 the generated table must not carry a map leaf"
    );
}

fn entry_action_text(args: &[&str]) -> String {
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
        Ok(EntryAction::PrintHelp { text, .. }) => text,
        Ok(other) => panic!("expected a printed-action entry, got {other:?}"),
        Err(e) => format!("parse error: {e}"),
    }
}

// ---------------------------------------------------------------------------
// YAML:logging.pion_level
//
// load.rs:656-662 fans the value out into two component_levels entries
// (`transport.pion` and `pion`), overwriting anything the operator wrote for `pion`.
// ---------------------------------------------------------------------------

#[test]
fn logging_pion_level_seeds_both_pion_component_entries() {
    let got = logging(
        &["--config-body", "logging:\n  pion_level: warn\n"],
        &Env::default(),
    );
    assert_eq!(
        "warn", got.component_levels["transport.pion"],
        "C98B1-16 pion_level must reach transport.pion"
    );
    assert_eq!(
        "warn", got.component_levels["pion"],
        "C98B1-16 pion_level must reach pion"
    );
    assert_eq!(
        2,
        got.component_levels.len(),
        "C98B1-16 pion_level must not invent other entries: {:?}",
        got.component_levels
    );
}

#[test]
fn logging_pion_level_overwrites_an_operator_pion_entry_and_empty_leaves_it() {
    let over = logging(
        &[
            "--config-body",
            "logging:\n  pion_level: error\n  component_levels:\n    pion: debug\n",
        ],
        &Env::default(),
    );
    assert_eq!(
        "error", over.component_levels["pion"],
        "C98B1-17 pion_level is applied after the map, so it wins"
    );
    assert_eq!(
        "error", over.component_levels["transport.pion"],
        "C98B1-17 the fan-out is unconditional once pion_level is set"
    );

    let empty = logging(
        &[
            "--config-body",
            "logging:\n  pion_level: \"\"\n  component_levels:\n    pion: debug\n",
        ],
        &Env::default(),
    );
    assert_eq!(
        "debug", empty.component_levels["pion"],
        "C98B1-18 an empty pion_level must not insert anything"
    );
    assert!(
        !empty.component_levels.contains_key("transport.pion"),
        "C98B1-18 an empty pion_level must not create transport.pion: {:?}",
        empty.component_levels
    );
}

#[test]
fn logging_pion_level_projects_from_env_and_cli() {
    let env = logging(
        &["--dev"],
        &Env::from_pairs([("LIVEKIT_LOGGING_PION_LEVEL", "info")]),
    );
    assert_eq!(
        "info", env.component_levels["pion"],
        "C98B1-19 LIVEKIT_LOGGING_PION_LEVEL must reach the map"
    );
    let cli = logging(&["--dev", "--logging.pion_level=debug"], &Env::default());
    assert_eq!(
        "debug", cli.component_levels["transport.pion"],
        "C98B1-20 --logging.pion_level must reach transport.pion"
    );
}

// ---------------------------------------------------------------------------
// YAML:logging (section parent)
// ---------------------------------------------------------------------------

#[test]
fn logging_parent_projects_each_field_into_its_own_slot() {
    let yaml = "logging:\n  json: true\n  level: warn\n  sample: false\n  sample_initial: 7\n  sample_interval: 8\n  item_sample_seconds: 9\n  item_sample_initial: 10\n  item_sample_interval: 11\n  pion_level: error\n  component_levels:\n    router: trace\n";
    let got = logging(&["--config-body", yaml], &Env::default());
    assert!(got.json, "C98B1-21 json");
    assert_eq!("warn", got.level, "C98B1-21 level");
    assert!(!got.sample, "C98B1-21 sample");
    assert_eq!(7, got.sample_initial, "C98B1-21 sample_initial");
    assert_eq!(8, got.sample_interval, "C98B1-21 sample_interval");
    assert_eq!(9, got.item_sample_seconds, "C98B1-21 item_sample_seconds");
    assert_eq!(10, got.item_sample_initial, "C98B1-21 item_sample_initial");
    assert_eq!(
        11, got.item_sample_interval,
        "C98B1-21 item_sample_interval"
    );
    assert_eq!(
        "trace", got.component_levels["router"],
        "C98B1-21 the operator map entry survives"
    );
    assert_eq!(
        "error", got.component_levels["pion"],
        "C98B1-21 pion_level still fans out beside an explicit map"
    );
    assert_eq!(
        3,
        got.component_levels.len(),
        "C98B1-21 exactly router + pion + transport.pion: {:?}",
        got.component_levels
    );

    let absent = logging(
        &["--config-body", "rtc:\n  use_ice_lite: true\n"],
        &Env::default(),
    );
    let defaults = rtc_core::config::LoggingConfig::default();
    assert_eq!(
        "", defaults.level,
        "C98B1-22 the struct default is the empty Go string, so the projection below cannot be \
         explained by \"it just used the default\""
    );
    assert_eq!(
        "info", absent.level,
        "C98B1-22 an absent logging block is normalised to \"info\" at normalize.rs:814-816"
    );
    assert_eq!(
        defaults.json, absent.json,
        "C98B1-22 an absent logging block keeps json off"
    );
    assert_eq!(
        vec!["pion", "transport.pion"],
        absent
            .component_levels
            .keys()
            .map(String::as_str)
            .collect::<Vec<_>>(),
        "C98B1-22 an absent logging block invents no component entries beyond the pion seeds: {:?}",
        absent.component_levels
    );
}

// ---------------------------------------------------------------------------
// YAML:prometheus.port
//
// normalize.rs:837-847: the deprecated top-level `prometheus_port` wins when nonzero and
// warns; `opt_port` maps 0 to None and rejects anything outside a uint16.
// ---------------------------------------------------------------------------

#[test]
fn prometheus_port_projects_zero_as_none_and_rejects_out_of_range() {
    let base = prometheus(&["--dev"], &Env::default());
    assert_eq!(None, base.port, "C98B1-23 the default port is unset");

    let set = prometheus(
        &["--config-body", "prometheus:\n  port: 9090\n"],
        &Env::default(),
    );
    assert_eq!(Some(9090), set.port, "C98B1-23 YAML port must project");

    let env = prometheus(
        &["--dev"],
        &Env::from_pairs([("LIVEKIT_PROMETHEUS_PORT", "9091")]),
    );
    assert_eq!(Some(9091), env.port, "C98B1-24 env port must project");

    let cli = prometheus(&["--dev", "--prometheus.port=9092"], &Env::default());
    assert_eq!(Some(9092), cli.port, "C98B1-25 CLI port must project");

    // `prometheus_port` and `prometheus.port` are two leaves that share one env name
    // (env_name maps both '.' and '_' to '_'), so the environment sets the deprecated top-level
    // leaf too, and normalize.rs:838-843 lets that one win over the CLI's nested value.
    let collision = prometheus(
        &["--dev", "--prometheus.port=9092"],
        &Env::from_pairs([("LIVEKIT_PROMETHEUS_PORT", "9091")]),
    );
    assert_eq!(
        Some(9091),
        collision.port,
        "C98B1-25b LIVEKIT_PROMETHEUS_PORT feeds both leaves and the deprecated one wins"
    );
    assert!(
        load(
            &["--dev", "--prometheus.port=9092"],
            &Env::from_pairs([("LIVEKIT_PROMETHEUS_PORT", "9091")]),
        )
        .warnings
        .iter()
        .any(|w| w.contains("prometheus_port is deprecated")),
        "C98B1-25c taking over through the deprecated leaf must emit the upstream deprecation \
         warning, not happen silently"
    );

    let deprecated = prometheus(
        &["--config-body", "prometheus_port: 9093\n"],
        &Env::default(),
    );
    assert_eq!(
        Some(9093),
        deprecated.port,
        "C98B1-26 the deprecated top-level port must take over"
    );
    assert_eq!(
        vec!["prometheus_port is deprecated, please switch to prometheus.port instead".to_string()],
        deprecated_warnings(),
        "C98B1-26 the takeover must be announced"
    );

    let both = prometheus(
        &[
            "--config-body",
            "prometheus_port: 9093\nprometheus:\n  port: 9090\n",
        ],
        &Env::default(),
    );
    assert_eq!(
        Some(9093),
        both.port,
        "C98B1-27 the deprecated value wins over the section value"
    );
}

fn deprecated_warnings() -> Vec<String> {
    load(
        &["--config-body", "prometheus_port: 9093\n"],
        &Env::default(),
    )
    .warnings
}

#[test]
fn prometheus_port_out_of_range_and_zero_stay_honest() {
    let zero = prometheus(
        &["--config-body", "prometheus:\n  port: 0\n"],
        &Env::default(),
    );
    assert_eq!(
        None, zero.port,
        "C98B1-28 port 0 means disabled, not port 0"
    );

    let err = parse_with_probe(
        [
            "livecloud",
            "--dev",
            "--config-body",
            "prometheus:\n  port: 70000\n",
        ]
        .iter()
        .map(OsString::from)
        .collect(),
        &Env::default(),
        &ProgramInfo {
            binary_name: "livecloud".into(),
            product_version: "fixture".into(),
            compat_version: "1.13.10",
        },
        &NoNetwork,
    );
    let text = match err {
        Ok(_) => "accepted".to_string(),
        Err(e) => format!("{e}"),
    };
    assert!(
        text.contains("prometheus.port"),
        "C98B1-29 an out-of-range port must name the key: {text}"
    );
}

// ---------------------------------------------------------------------------
// YAML:prometheus.username / YAML:prometheus.password
//
// normalize.rs:847-856: empty string -> None; a set value -> Some(value).
// ---------------------------------------------------------------------------

#[test]
fn prometheus_username_empty_projects_as_none() {
    let empty = prometheus(
        &["--config-body", "prometheus:\n  username: \"\"\n"],
        &Env::default(),
    );
    assert_eq!(None, empty.username, "C98B1-30 empty username is None");

    let set = prometheus(
        &["--config-body", "prometheus:\n  username: prom\n"],
        &Env::default(),
    );
    assert_eq!(
        Some("prom".to_string()),
        set.username,
        "C98B1-30 a set username must project"
    );

    let cli = prometheus(&["--dev", "--prometheus.username=cliprom"], &Env::default());
    assert_eq!(
        Some("cliprom".to_string()),
        cli.username,
        "C98B1-31 the generated CLI flag must reach the same slot"
    );
    assert_eq!(
        None, cli.password,
        "C98B1-31 setting the username must not invent a password"
    );
}

#[test]
fn prometheus_password_empty_projects_as_none_and_never_leaks_into_warnings() {
    let empty = prometheus(
        &["--config-body", "prometheus:\n  password: \"\"\n"],
        &Env::default(),
    );
    assert_eq!(None, empty.password, "C98B1-32 empty password is None");

    let loaded = load(
        &["--config-body", "prometheus:\n  password: s3cret\n"],
        &Env::default(),
    );
    assert_eq!(
        Some("s3cret".to_string()),
        loaded.server.prometheus.password,
        "C98B1-32 a set password must project"
    );
    let echoed: Vec<&String> = loaded
        .warnings
        .iter()
        .chain(loaded.notices.iter())
        .filter(|m| m.contains("s3cret"))
        .collect();
    assert!(
        echoed.is_empty(),
        "C98B1-33 the password must not be echoed: {echoed:?}"
    );

    let env = prometheus(
        &["--dev"],
        &Env::from_pairs([("LIVEKIT_PROMETHEUS_PASSWORD", "ep")]),
    );
    assert_eq!(
        Some("ep".to_string()),
        env.password,
        "C98B1-34 the env password must project"
    );
    assert_eq!(
        None, env.username,
        "C98B1-34 and must not touch the username"
    );
}

// ---------------------------------------------------------------------------
// YAML:prometheus (section parent)
// ---------------------------------------------------------------------------

#[test]
fn prometheus_parent_projects_the_three_fields_without_cross_talk() {
    let absent = prometheus(
        &["--config-body", "rtc:\n  use_ice_lite: true\n"],
        &Env::default(),
    );
    assert_eq!(None, absent.port, "C98B1-35 an absent section is all-None");
    assert_eq!(
        None, absent.username,
        "C98B1-35 an absent section is all-None"
    );
    assert_eq!(
        None, absent.password,
        "C98B1-35 an absent section is all-None"
    );

    let full = prometheus(
        &[
            "--config-body",
            "prometheus:\n  port: 9090\n  username: u1\n  password: p1\n",
        ],
        &Env::default(),
    );
    assert_eq!(Some(9090), full.port, "C98B1-36 port slot");
    assert_eq!(
        Some("u1".to_string()),
        full.username,
        "C98B1-36 username slot"
    );
    assert_eq!(
        Some("p1".to_string()),
        full.password,
        "C98B1-36 password slot"
    );

    let partial = prometheus(
        &["--config-body", "prometheus:\n  username: u1\n"],
        &Env::default(),
    );
    assert_eq!(
        None, partial.port,
        "C98B1-37 a partial section keeps the others None"
    );
    assert_eq!(
        None, partial.password,
        "C98B1-37 a partial section keeps the others None"
    );
    assert_eq!(
        Some("u1".to_string()),
        partial.username,
        "C98B1-37 only the set field lands"
    );
}
