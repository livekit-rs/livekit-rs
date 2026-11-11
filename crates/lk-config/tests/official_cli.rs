//! Opt-in differential tests against the pinned Go reference binary.
//! LIVEKIT_OFFICIAL_BIN=/path/to/livekit-server cargo test -p lk-config \
//!   --test official_cli -- --ignored --nocapture
//! The reference executable is used only by tests; it is not a runtime dependency.

use std::ffi::OsString;
use std::process::Command;

use lk_config::{EntryAction, Env, ProgramInfo};

type Fixture<'a> = (&'a str, &'a [&'a str], &'a [(&'a str, &'a str)]);

#[derive(Debug, PartialEq, Eq)]
struct Observation {
    code: i32,
    stdout: String,
    stderr: String,
}

fn rust(args: &[&str], env: &[(&str, &str)]) -> Observation {
    let info = ProgramInfo {
        binary_name: "livekit-server".into(),
        product_version: "1.13.10".into(),
        compat_version: "1.13.10",
    };
    let argv = std::iter::once("livekit-server")
        .chain(args.iter().copied())
        .map(OsString::from)
        .collect();
    match lk_config::parse(argv, &Env::from_pairs(env.iter().copied()), &info) {
        Ok(EntryAction::Ports(cfg)) => Observation {
            code: 0,
            stdout: lk_config::ports_output(&cfg),
            stderr: String::new(),
        },
        Ok(EntryAction::PrintHelp {
            text,
            to_stderr: false,
            exit_code,
        }) => Observation {
            code: exit_code,
            stdout: text,
            stderr: String::new(),
        },
        Ok(EntryAction::PrintHelp {
            text,
            to_stderr: true,
            exit_code,
        }) => {
            let (stdout, stderr) = lk_config::help_output(&text, true);
            Observation {
                code: exit_code,
                stdout,
                stderr,
            }
        }
        Err(error) => Observation {
            code: 1,
            stdout: format!("{error}\n"),
            stderr: String::new(),
        },
        other => panic!("fixture must terminate during CLI/config handling: {other:?}"),
    }
}

fn official(args: &[&str], env: &[(&str, &str)]) -> Observation {
    let binary = std::env::var_os("LIVEKIT_OFFICIAL_BIN")
        .expect("set LIVEKIT_OFFICIAL_BIN to the pinned v1.13.10 reference executable");
    let output = Command::new(binary)
        .args(args)
        .env_clear()
        .envs(env.iter().copied())
        .output()
        .expect("run official CLI");
    Observation {
        code: output.status.code().unwrap_or(-1),
        stdout: String::from_utf8(output.stdout).unwrap(),
        // Configuration commands may log timestamped node discovery to stderr;
        // the error/help fixtures below have no such nondeterministic logs.
        stderr: String::from_utf8(output.stderr).unwrap(),
    }
}

fn compare(name: &str, args: &[&str], env: &[(&str, &str)]) {
    let mut expected = official(args, env);
    let mut actual = rust(args, env);
    if expected.code == 0 && args.contains(&"ports") {
        expected.stderr.clear();
        actual.stderr.clear();
    }
    assert_eq!(actual.code, expected.code, "fixture {name} exit code");
    for (stream, actual, expected) in [
        ("stdout", &actual.stdout, &expected.stdout),
        ("stderr", &actual.stderr, &expected.stderr),
    ] {
        if actual != expected {
            let actual_lines: Vec<_> = actual.lines().collect();
            let expected_lines: Vec<_> = expected.lines().collect();
            let line = actual_lines
                .iter()
                .zip(&expected_lines)
                .position(|(a, e)| a != e)
                .unwrap_or(actual_lines.len().min(expected_lines.len()));
            panic!(
                "fixture {name} {stream} line {}: actual={:?}, expected={:?}",
                line + 1,
                actual_lines.get(line),
                expected_lines.get(line)
            );
        }
    }
}

#[test]
#[ignore = "requires the pinned official executable via LIVEKIT_OFFICIAL_BIN"]
fn pinned_cli_behavior() {
    let fixtures: &[Fixture<'_>] = &[
        ("help", &["--help"], &[]),
        ("unknown_help_topic", &["help", "missing"], &[]),
        (
            "help_overrides_invalid_bool",
            &["--help", "--dev=invalid"],
            &[],
        ),
        ("ports_help", &["ports", "--help"], &[]),
        ("default_ports", &["ports"], &[]),
        ("development_ports", &["ports", "--dev"], &[]),
        ("empty_bool_is_true", &["ports", "--dev="], &[]),
        (
            "literal_flag_shaped_file",
            &["ports", "--config", "-dev"],
            &[],
        ),
        (
            "literal_extension_shaped_file",
            &["ports", "--config", "--livecloud-doctor"],
            &[],
        ),
        (
            "body",
            &[
                "ports",
                "--config-body",
                "port: 9900\nrtc:\n  tcp_port: 9901\n  udp_port: 9902\n",
            ],
            &[],
        ),
        (
            "body_wins_file",
            &[
                "ports",
                "--config",
                "/missing/config.yaml",
                "--config-body",
                "port: 9910",
            ],
            &[],
        ),
        ("body_env", &["ports"], &[("LIVEKIT_CONFIG", "port: 9920")]),
        (
            "body_cli_wins_env",
            &["ports", "--config-body", "port: 9930"],
            &[("LIVEKIT_CONFIG", "port: 9920")],
        ),
        (
            "generated_env_ignored_by_subcommand",
            &["ports"],
            &[("LIVEKIT_PORT", "9940")],
        ),
        (
            "generated_cli_ignored_by_subcommand",
            &["ports", "--port", "9940"],
            &[],
        ),
        (
            "base_udp_overrides_body",
            &[
                "ports",
                "--udp-port",
                "9950",
                "--config-body",
                "rtc:\n  udp_port: 9902\n",
            ],
            &[],
        ),
        (
            "base_udp_cli_wins_env",
            &["ports", "--udp-port", "9950"],
            &[("UDP_PORT", "9960")],
        ),
        ("unknown_flag", &["--unknown-flag"], &[]),
        ("unknown_subcommand_flag", &["ports", "--unknown-flag"], &[]),
        ("missing_value", &["--config"], &[]),
        ("invalid_bool", &["--dev=invalid"], &[]),
        ("subcommand_invalid_bool", &["ports", "--dev=invalid"], &[]),
        ("subcommand_missing_value", &["ports", "--config"], &[]),
        ("invalid_generated_bool", &["--rtc.use_ice_lite=abc"], &[]),
        ("invalid_generated_uint", &["--rtc.tcp_port", "abc"], &[]),
        ("negative_generated_uint", &["--port", "-1"], &[]),
        (
            "missing_join_identity",
            &["create-join-token", "--room", "r"],
            &[],
        ),
    ];
    let extra: &[Fixture<'_>] = &[
        (
            "strict_unknown_yaml",
            &["ports", "--config-body", "rtc:\n  bogus: 1\n"],
            &[],
        ),
        (
            "nonstrict_unknown_yaml",
            &[
                "ports",
                "--disable-strict-config",
                "--config-body",
                "rtc:\n  bogus: 1\n",
            ],
            &[],
        ),
        (
            "last_body_wins",
            &[
                "ports",
                "--config-body",
                "port: 9970",
                "--config-body",
                "port: 9980",
            ],
            &[],
        ),
        (
            "empty_env_body_uses_file",
            &["ports", "--config", "/missing/config.yaml"],
            &[("LIVEKIT_CONFIG", "")],
        ),
        (
            "empty_generated_uint",
            &["--config", "/missing/config.yaml"],
            &[("LIVEKIT_PORT", "")],
        ),
        (
            "invalid_generated_uint_env",
            &["--config", "/missing/config.yaml"],
            &[("LIVEKIT_PORT", "abc")],
        ),
        (
            "comment_only_yaml",
            &["ports", "--config-body", "# comment only"],
            &[],
        ),
        (
            "null_yaml_sections",
            &["ports", "--config-body", "rtc:\n  tcp_port:\nturn:\n"],
            &[],
        ),
        (
            "port_range_yaml",
            &["ports", "--config-body", "rtc:\n  udp_port: 9902-9905\n"],
            &[],
        ),
        ("single_dash_missing_value", &["-config"], &[]),
    ];
    let mut failures = Vec::new();
    for (name, args, env) in fixtures.iter().chain(extra) {
        if std::panic::catch_unwind(|| compare(name, args, env)).is_err() {
            failures.push(*name);
        }
    }
    assert!(
        failures.is_empty(),
        "failed official fixtures: {failures:?}"
    );
}
