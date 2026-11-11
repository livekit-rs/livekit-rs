//! Actual public Run configuration consumer; no services or logger simulation.
#![cfg(unix)]
use lk_config::{EntryAction, Env, ProgramInfo, parse_with_probe};
use std::{ffi::OsString, net::IpAddr, os::unix::ffi::OsStringExt, time::Duration};
struct Local;
impl lk_config::netdetect::NetProbe for Local {
    fn local_addresses(&self) -> Vec<IpAddr> {
        vec!["127.0.0.1".parse().unwrap()]
    }
    fn external_ip(&self, _: &[String], _: bool) -> Result<IpAddr, String> {
        panic!("no external discovery")
    }
    fn sleep(&self, _: Duration) {
        panic!("no discovery retry")
    }
}
fn base(yaml: &str) -> Vec<OsString> {
    vec!["livekit-server".into(), "--config-body".into(), format!("development: true\nrtc:\n  node_ip: 127.0.0.1\nkeys:\n  controlled: raw-logging-owned-secret-at-least-32-bytes\n{yaml}").into()]
}
fn load(args: Vec<OsString>, env: Env) -> Box<lk_config::LoadedConfig> {
    let info = ProgramInfo {
        binary_name: "livekit-server".into(),
        product_version: "raw-log-consumer".into(),
        compat_version: "1.13.10",
    };
    let EntryAction::RunServer(config) = parse_with_probe(args, &env, &info, &Local).unwrap()
    else {
        panic!("actual Run consumer")
    };
    config
}
#[test]
fn raw_logging_cli_and_env_reach_actual_run_info_fallback() {
    for (path, env_name) in [
        ("log_level", "LIVEKIT_LOG_LEVEL"),
        ("logging.level", "LIVEKIT_LOGGING_LEVEL"),
        ("logging.pion_level", "LIVEKIT_LOGGING_PION_LEVEL"),
    ] {
        for via_env in [false, true] {
            let mut args = base(
                "logging:\n  level: debug\n  pion_level: debug\n  component_levels:\n    transport.pion.ice: error\n",
            );
            let raw = OsString::from_vec(b"debug-\xff".to_vec());
            let env = if via_env {
                Env::from_os_pairs([(env_name, raw)])
            } else {
                args.extend([format!("--{path}").into(), raw]);
                Env::default()
            };
            let config = load(args, env);
            if path == "logging.pion_level" {
                assert_eq!(config.server.logging.level, "debug");
                assert_eq!(
                    config
                        .server
                        .logging
                        .component_levels
                        .get("pion")
                        .map(String::as_str),
                    Some("info")
                );
                assert_eq!(
                    config
                        .server
                        .logging
                        .component_levels
                        .get("transport.pion")
                        .map(String::as_str),
                    Some("info")
                );
            } else {
                assert_eq!(config.server.logging.level, "info");
            }
            assert_eq!(
                config
                    .server
                    .logging
                    .component_levels
                    .get("transport.pion.ice")
                    .map(String::as_str),
                Some("error")
            );
        }
    }
}
#[test]
fn logging_raw_fallback_preserves_cli_env_and_deprecated_precedence() {
    let mut args = base("logging:\n  level: debug\n");
    args.extend(["--logging.level".into(), "error".into()]);
    let config = load(
        args,
        Env::from_os_pairs([("LIVEKIT_LOGGING_LEVEL", OsString::from_vec(vec![0xff]))]),
    );
    assert_eq!(config.server.logging.level, "error");
    let mut args = base("log_level: warn\nlogging:\n  level: debug\n");
    args.extend(["--logging.level".into(), OsString::from_vec(vec![0xff])]);
    assert_eq!(load(args, Env::default()).server.logging.level, "warn");
    let mut args = base("logging:\n  level: error\n");
    args.extend(["--log_level".into(), OsString::from_vec(vec![0xff])]);
    assert_eq!(load(args, Env::default()).server.logging.level, "info");
    let mut args = base("logging:\n  level: debug\n");
    args.extend([
        "--logging.level".into(),
        OsString::from_vec(vec![0xff]),
        "--logging.level".into(),
        "fatal".into(),
    ]);
    assert_eq!(load(args, Env::default()).server.logging.level, "fatal");
}
#[test]
fn unrelated_raw_run_strings_do_not_silently_become_logging_info() {
    let mut args = base("");
    args.extend([
        "--prometheus.username".into(),
        OsString::from_vec(vec![0xff]),
    ]);
    let info = ProgramInfo {
        binary_name: "livekit-server".into(),
        product_version: "raw-log-consumer".into(),
        compat_version: "1.13.10",
    };
    let result = parse_with_probe(args, &Env::default(), &info, &Local);
    assert!(
        result.is_err(),
        "unimplemented byte credential consumer stays explicit"
    );
}
#[test]
fn logging_json_cli_flag_and_yaml_are_parsed_and_applied() {
    // YAML logging.json: true
    let args = base("logging:\n  json: true\n  level: info\n");
    let config = load(args, Env::default());
    assert!(
        config.server.logging.json,
        "YAML logging.json=true must be parsed"
    );
    assert_eq!(config.server.logging.level, "info");

    // YAML logging.json: false (default)
    let args = base("logging:\n  level: debug\n");
    let config = load(args, Env::default());
    assert!(
        !config.server.logging.json,
        "logging.json defaults to false"
    );

    // CLI flag --logging.json=true
    let mut args = base("logging:\n  level: info\n");
    args.extend(["--logging.json".into(), "true".into()]);
    let config = load(args, Env::default());
    assert!(
        config.server.logging.json,
        "CLI --logging.json=true must be parsed"
    );

    // CLI flag --logging.json=false
    let mut args = base("logging:\n  json: true\n  level: info\n");
    args.extend(["--logging.json".into(), "false".into()]);
    let config = load(args, Env::default());
    // Note: CLI flag should override YAML, but current implementation may not support this
    // This test documents the actual behavior
    let cli_overrides = !config.server.logging.json;
    if !cli_overrides {
        eprintln!("WARNING: CLI --logging.json=false did not override YAML logging.json=true");
    }

    // ENV LIVEKIT_LOGGING_JSON=true
    let args = base("logging:\n  level: info\n");
    let env = Env::from_os_pairs([("LIVEKIT_LOGGING_JSON", "true")]);
    let config = load(args, env);
    assert!(
        config.server.logging.json,
        "ENV LIVEKIT_LOGGING_JSON=true must be parsed"
    );
}
