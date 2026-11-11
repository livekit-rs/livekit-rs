//! LiveKit Server v1.13.10 compatible CLI / YAML / ENV handling (spec 0001 §6,
//! impl-notes `config-cli.md`).
//!
//! [`parse`] turns argv + environment into an [`EntryAction`]; the binary then
//! prints / runs it. All configuration-loading commands come back with a fully
//! post-processed [`LoadedConfig`] whose `server` field is the normalized
//! `rtc_core::config::ServerConfig`.
//!
//! Module map:
//! - `cli`: clap builder, base flags, subcommands, value collection
//! - `generated`: the single table of generated `--a.b.c` / `LIVEKIT_A_B_C` flags
//! - `official`: serde tree of the official YAML + upstream defaults
//! - `duration`, `strconv`, `ports`: Go parsing rules
//! - `load`: the pipeline; `keys`, `netdetect`: key files and node-IP discovery
//! - `unsupported`: CLI-C08 table; `normalize`: official → `ServerConfig`
//! - `output`, `help`: byte-exact command output and help text

use std::ffi::OsString;
use std::path::PathBuf;

mod cli;
mod duration;
mod env;
mod error;
pub mod generated;
mod help;
mod keys;
mod load;
pub mod nat1to1;
pub mod netdetect;
mod normalize;
#[cfg(test)]
mod observer;
pub mod official;
mod output;
pub mod ports;
mod protoyaml;
mod strconv;
mod unsupported;
mod yamlv3;

pub use duration::{GoDuration, format_go_duration, parse_go_duration};
pub use env::Env;
pub use error::ConfigError;
pub use keys::go_errno_text;
pub use output::{generate_keys_output, help_output, ports_output};

/// What the binary should do.
#[derive(Debug)]
pub enum EntryAction {
    RunServer(Box<LoadedConfig>),
    /// Print [`generate_keys_output`] to stdout, exit 0.
    GenerateKeys,
    /// Print [`ports_output`] to stdout, exit 0.
    Ports(Box<CommandConfig>),
    ListNodes(Box<CommandConfig>),
    CreateJoinToken {
        config: Box<CommandConfig>,
        room: String,
        identity: String,
        recorder: bool,
    },
    /// `--help`, `help`, `help-verbose` (stdout, exit 0) and usage errors
    /// (`Incorrect Usage: ...` on stderr; help and bare error on stdout, exit 1).
    /// Use [`help_output`] to render the action to its actual output streams.
    PrintHelp {
        text: String,
        to_stderr: bool,
        exit_code: i32,
    },
    /// Exact non-Unicode usage output, emitted only after pure parsing finishes.
    PrintBytes {
        stdout: Vec<u8>,
        stderr: Vec<u8>,
        exit_code: i32,
    },
    /// `--version` / `-v` line (stdout, exit 0).
    PrintVersion(String),
}

/// Identity of the running binary (`livecloud` or `livekit-server`).
#[derive(Clone, Debug)]
pub struct ProgramInfo {
    /// Displayed command name; parsing is identical for every name.
    pub binary_name: String,
    /// Real product/build version shown by `--version`.
    pub product_version: String,
    /// Upstream version this build is compatible with (`"1.13.10"`).
    pub compat_version: &'static str,
}

/// Structured field carried alongside a startup log message (zap key/value,
/// SPEC-0074 C32): the pipeline is string-based, but upstream renders some of
/// these lines with real zap fields, e.g. `{"apiKey": "devkey"}`.
#[derive(Clone, Debug)]
pub struct NoticeExtra {
    pub message: String,
    pub field: &'static str,
    pub value: String,
}

/// Configuration after the full upstream pipeline.
#[derive(Clone, Debug)]
pub struct LoadedConfig {
    pub server: rtc_core::config::ServerConfig,
    /// Already loaded into `server.keys` (for `RunServer`); kept for messages.
    pub key_file: Option<PathBuf>,
    /// Warnings to log once the logger exists. Upstream emits most of these
    /// before its logger is initialised and so loses them; we keep them.
    pub warnings: Vec<String>,
    /// Warnings that upstream renders with a zap field (secret-too-short).
    pub warning_extras: Vec<NoticeExtra>,
    /// Info-level lines upstream logs while loading (`starting in development
    /// mode`, `no keys provided, using placeholder keys`).
    pub notices: Vec<String>,
    /// `--cpuprofile`; real CPU samples written by server lifecycle.
    pub cpu_profile: Option<PathBuf>,
    /// `--memprofile`; real allocation samples written at server termination.
    pub mem_profile: Option<PathBuf>,
    /// The node IP was discovered rather than configured.
    pub node_ip_auto_generated: bool,
    /// The post-processed official tree, for values `ServerConfig` does not
    /// carry (e.g. the raw `rtc.udp_port` used by `ports`, TURN proxy settings,
    /// webhook worker tuning, `room.create_room_timeout`, ...).
    pub official: Box<official::Config>,
}

/// Source getConfig output for non-server actions. Deliberately has no
/// ServerConfig: printing ports, signing a token and listing nodes don't bind
/// media/listener resources and cannot require their narrowed runtime types.
#[derive(Clone, Debug)]
pub struct CommandConfig {
    pub key_file: Option<PathBuf>,
    pub warnings: Vec<String>,
    pub warning_extras: Vec<NoticeExtra>,
    pub notices: Vec<String>,
    pub cpu_profile: Option<PathBuf>,
    pub mem_profile: Option<PathBuf>,
    pub node_ip_auto_generated: bool,
    pub node_id: String,
    pub(crate) extension: Option<rtc_core::config::ExtensionConfig>,
    pub(crate) binary_turn_secrets: Vec<(usize, Vec<u8>)>,
    pub official: Box<official::Config>,
}

/// Exact postprocessed upstream tree; does not imply server-resource validity.
pub trait OfficialConfigView {
    fn official_config(&self) -> &official::Config;
    fn command_node_id(&self) -> &str;
}
impl OfficialConfigView for LoadedConfig {
    fn official_config(&self) -> &official::Config {
        &self.official
    }
    fn command_node_id(&self) -> &str {
        &self.server.node_id
    }
}
impl OfficialConfigView for CommandConfig {
    fn official_config(&self) -> &official::Config {
        &self.official
    }
    fn command_node_id(&self) -> &str {
        &self.node_id
    }
}
impl<T: OfficialConfigView + ?Sized> OfficialConfigView for Box<T> {
    fn official_config(&self) -> &official::Config {
        (**self).official_config()
    }
    fn command_node_id(&self) -> &str {
        (**self).command_node_id()
    }
}

/// Parses `args` (including `argv[0]`) against `env`.
pub fn parse(
    args: Vec<OsString>,
    env: &Env,
    info: &ProgramInfo,
) -> Result<EntryAction, ConfigError> {
    parse_with_probe(args, env, info, &netdetect::SystemProbe)
}

/// [`parse`] with an injectable network probe (node-IP discovery), for tests.
pub fn parse_with_probe(
    args: Vec<OsString>,
    env: &Env,
    info: &ProgramInfo,
    probe: &dyn netdetect::NetProbe,
) -> Result<EntryAction, ConfigError> {
    let inv = match cli::parse_args(args, env, info)? {
        cli::Parsed::Early(action) => return Ok(action),
        cli::Parsed::Invocation(inv) => inv,
    };
    match &inv.command {
        cli::Command::Run => {
            let cfg = load::load(&inv, env, load::Mode::Run, probe)?;
            Ok(EntryAction::RunServer(Box::new(cfg)))
        }
        cli::Command::GenerateKeys => Ok(EntryAction::GenerateKeys),
        cli::Command::Ports => Ok(EntryAction::Ports(Box::new(load::load_command(
            &inv,
            env,
            load::Mode::Ports,
            probe,
        )?))),
        cli::Command::ListNodes => Ok(EntryAction::ListNodes(Box::new(load::load_command(
            &inv,
            env,
            load::Mode::ListNodes,
            probe,
        )?))),
        cli::Command::CreateJoinToken {
            room,
            identity,
            recorder,
        } => {
            let config = Box::new(load::load_command(
                &inv,
                env,
                load::Mode::CreateJoinToken,
                probe,
            )?);
            Ok(EntryAction::CreateJoinToken {
                config,
                room: room.clone(),
                identity: identity.clone(),
                recorder: *recorder,
            })
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::netdetect::tests::FakeProbe;

    fn info(name: &str) -> ProgramInfo {
        ProgramInfo {
            binary_name: name.into(),
            product_version: "0.1.0".into(),
            compat_version: "1.13.10",
        }
    }

    fn run(name: &str, a: &[&str], env: &Env) -> Result<EntryAction, ConfigError> {
        let args = std::iter::once(name)
            .chain(a.iter().copied())
            .map(OsString::from)
            .collect();
        parse_with_probe(args, env, &info(name), &FakeProbe::local("10.0.0.5"))
    }

    #[test]
    fn signed_identity_limit_preserves_yaml_env_cli_and_legacy_override() {
        for (body, env, flags, expected) in [
            (
                "limit: {max_participant_identity_length: -1}",
                Env::default(),
                vec![],
                -1,
            ),
            (
                "limit: {max_participant_identity_length: 0}",
                Env::default(),
                vec![],
                0,
            ),
            (
                "",
                Env::from_pairs([("LIVEKIT_LIMIT_MAX_PARTICIPANT_IDENTITY_LENGTH", "-2")]),
                vec![],
                -2,
            ),
            (
                "",
                Env::default(),
                vec!["--limit.max_participant_identity_length=-9223372036854775808"],
                i64::MIN,
            ),
            (
                "limit: {max_participant_identity_length: 4}\nroom: {max_participant_identity_length: -1}",
                Env::default(),
                vec![],
                -1,
            ),
        ] {
            let mut args = vec!["--dev", "--config-body", body];
            args.extend(flags);
            let EntryAction::RunServer(config) = run("livekit-server", &args, &env).unwrap() else {
                panic!("expected server")
            };
            assert_eq!(
                config.server.limits.max_participant_identity_length,
                expected
            );
        }
    }

    #[test]
    fn playout_delay_uses_fixed_go_uint32_cast_vectors_without_clamping() {
        let vectors = include_str!(
            "../../../tests/fixtures/upstream/evidence/2026-10-25/f02b-oracle/playout.tsv"
        );
        for line in vectors
            .lines()
            .filter(|line| !line.starts_with('#') && !line.is_empty())
        {
            let fields: Vec<_> = line.split_whitespace().collect();
            let expected: u32 = fields[1].parse().unwrap();
            let yaml = format!(
                "room:\n  playout_delay:\n    enabled: true\n    min: {}\n    max: {}\n",
                fields[0], fields[0]
            );
            let EntryAction::RunServer(config) = run(
                "livekit-server",
                &["--dev", "--config-body", &yaml],
                &Env::default(),
            )
            .unwrap() else {
                panic!("expected server")
            };
            assert_eq!(
                config.server.room.playout_delay,
                Some((expected, expected)),
                "{line}"
            );
        }
        let EntryAction::RunServer(config) = run(
            "livekit-server",
            &[
                "--dev",
                "--config-body",
                "room: {playout_delay: {enabled: false, min: -1, max: -2}}",
            ],
            &Env::default(),
        )
        .unwrap() else {
            panic!("expected server")
        };
        assert_eq!(config.server.room.playout_delay, None);
    }

    #[test]
    fn custom_data_encoding_limit_preserves_defaults_zero_negative_and_precedence() {
        let load = |flags: &[&str], env: &Env| {
            let mut argv = vec!["--dev"];
            argv.extend_from_slice(flags);
            let EntryAction::RunServer(config) = run("livekit-server", &argv, env).unwrap() else {
                panic!("expected server")
            };
            config
        };
        assert_eq!(
            load(&[], &Env::default())
                .server
                .limits
                .max_data_track_custom_encoding_length,
            32
        );
        for value in [-1_i64, 0, 64] {
            let yaml = format!("limit:\n  max_data_track_custom_encoding_length: {value}\n");
            let config = load(&["--config-body", &yaml], &Env::default());
            assert_eq!(
                config.official.limit.max_data_track_custom_encoding_length,
                value
            );
            assert_eq!(
                config.server.limits.max_data_track_custom_encoding_length,
                value
            );
        }
        let env = Env::from_pairs([("LIVEKIT_LIMIT_MAX_DATA_TRACK_CUSTOM_ENCODING_LENGTH", "48")]);
        assert_eq!(
            load(
                &[
                    "--config-body",
                    "limit: {max_data_track_custom_encoding_length: 64}"
                ],
                &env
            )
            .server
            .limits
            .max_data_track_custom_encoding_length,
            48
        );
        assert_eq!(
            load(
                &[
                    "--config-body",
                    "limit: {max_data_track_custom_encoding_length: 64}",
                    "--limit.max_data_track_custom_encoding_length=-2"
                ],
                &env
            )
            .server
            .limits
            .max_data_track_custom_encoding_length,
            -2
        );
    }

    #[test]
    fn public_loader_empty_cli_bool_differs_from_empty_env() {
        let env = Env::from_pairs([("LIVEKIT_RTC_CONGESTION_CONTROL_ENABLED", "")]);
        for (extra, expected) in [
            (&[][..], false),
            (&["--rtc.congestion_control.enabled="][..], true),
        ] {
            let mut flags = vec!["--dev="];
            flags.extend_from_slice(extra);
            let EntryAction::RunServer(config) = run("livekit-server", &flags, &env).unwrap()
            else {
                panic!("expected server")
            };
            assert!(config.official.development);
            assert_eq!(config.server.rtc.congestion_control.enabled, expected);
        }
    }

    #[test]
    fn public_loader_preserves_literal_flag_values_and_cli_precedence() {
        let env = Env::from_pairs([("LIVEKIT_REGION", "env-region")]);
        for name in ["livecloud", "livekit-server"] {
            for region in ["-dev", "-config", "--livecloud-https-port=9999"] {
                let action = run(
                    name,
                    &["--dev", "--region", region, "--turn.domain", "-config"],
                    &env,
                )
                .unwrap();
                let EntryAction::RunServer(config) = action else {
                    panic!("expected server")
                };
                assert_eq!(config.server.region, region);
                assert_eq!(config.official.region, region);
                assert_eq!(config.official.turn.domain, "-config");
                assert!(config.server.extension.is_none());
            }
        }
    }

    #[test]
    fn both_names_behave_identically() {
        for name in ["livecloud", "livekit-server"] {
            match run(name, &["--dev"], &Env::default()).unwrap() {
                EntryAction::RunServer(c) => assert_eq!(c.server.keys[0].0, "devkey"),
                other => panic!("{other:?}"),
            }
            match run(name, &["--help"], &Env::default()).unwrap() {
                EntryAction::PrintHelp { text, .. } => {
                    assert!(text.contains(&format!("{name} - High performance WebRTC server")))
                }
                other => panic!("{other:?}"),
            }
        }
    }

    #[test]
    fn profiles_are_real_run_options_but_do_not_affect_non_server_commands() {
        let EntryAction::RunServer(config) = run(
            "livecloud",
            &[
                "--dev",
                "--cpuprofile",
                "cpu.out",
                "--memprofile",
                "heap.out",
            ],
            &Env::default(),
        )
        .unwrap() else {
            panic!("real server action required");
        };
        assert_eq!(
            config.cpu_profile.as_deref(),
            Some(std::path::Path::new("cpu.out"))
        );
        assert_eq!(
            config.mem_profile.as_deref(),
            Some(std::path::Path::new("heap.out"))
        );
        assert!(matches!(
            run(
                "livecloud",
                &["generate-keys", "--cpuprofile", "cpu.out"],
                &Env::default()
            )
            .unwrap(),
            EntryAction::GenerateKeys
        ));
        let EntryAction::Ports(_) = run(
            "livecloud",
            &["ports", "--dev", "--memprofile", "heap.out"],
            &Env::default(),
        )
        .unwrap() else {
            panic!("ports remains a side-effect-free command");
        };
        let EntryAction::RunServer(config) = run(
            "livecloud",
            &["--dev", "--cpuprofile=", "--memprofile="],
            &Env::default(),
        )
        .unwrap() else {
            panic!()
        };
        assert!(config.cpu_profile.is_none() && config.mem_profile.is_none());
    }

    #[test]
    fn ports_ignores_generated_env() {
        let env = Env::from_pairs([("LIVEKIT_RTC_TCP_PORT", "9999")]);
        match run("livecloud", &["ports"], &env).unwrap() {
            EntryAction::Ports(c) => assert!(ports_output(&c).contains("7881 - ICE/TCP")),
            other => panic!("{other:?}"),
        }
        match run("livecloud", &["--dev"], &env).unwrap() {
            EntryAction::RunServer(c) => assert_eq!(c.server.rtc.tcp_port, Some(9999)),
            other => panic!("{other:?}"),
        }
    }

    #[test]
    fn generate_keys_needs_no_config() {
        assert!(matches!(
            run(
                "livecloud",
                &["generate-keys", "--config", "/nope.yaml"],
                &Env::default()
            )
            .unwrap(),
            EntryAction::GenerateKeys
        ));
    }
}

#[cfg(test)]
mod feedback_projection_tests;

#[cfg(test)]
mod signed_name_projection_tests {
    use super::*;
    #[test]
    fn actual_public_parser_keeps_negative_zero_and_utf8_name_limits() {
        let info = ProgramInfo {
            binary_name: "livekit-server".into(),
            product_version: "test".into(),
            compat_version: "1.13.10",
        };
        for (body, env, extra, expected_room, expected_name) in [
            (
                "limit: {max_room_name_length: -1, max_participant_name_length: -2}",
                Env::default(),
                vec![],
                -1,
                -2,
            ),
            (
                "limit: {max_room_name_length: 0, max_participant_name_length: 0}",
                Env::default(),
                vec![],
                0,
                0,
            ),
            (
                "",
                Env::from_pairs([
                    ("LIVEKIT_LIMIT_MAX_ROOM_NAME_LENGTH", "-3"),
                    ("LIVEKIT_LIMIT_MAX_PARTICIPANT_NAME_LENGTH", "-4"),
                ]),
                vec![],
                -3,
                -4,
            ),
            (
                "limit: {max_room_name_length: 99, max_participant_name_length: 99}",
                Env::from_pairs([
                    ("LIVEKIT_LIMIT_MAX_ROOM_NAME_LENGTH", "2"),
                    ("LIVEKIT_LIMIT_MAX_PARTICIPANT_NAME_LENGTH", "3"),
                ]),
                vec![
                    "--limit.max_room_name_length=-9223372036854775808",
                    "--limit.max_participant_name_length=9223372036854775807",
                ],
                i64::MIN,
                i64::MAX,
            ),
            (
                "limit: {max_room_name_length: 4}
room: {max_room_name_length: -1}",
                Env::default(),
                vec![],
                -1,
                256,
            ),
        ] {
            let mut args = vec![
                "livekit-server",
                "--dev",
                "--node-ip",
                "127.0.0.1",
                "--config-body",
                body,
            ];
            args.extend(extra);
            let result = parse_with_probe(
                args.into_iter().map(Into::into).collect(),
                &env,
                &info,
                &crate::netdetect::tests::FakeProbe::local("127.0.0.1"),
            )
            .unwrap();
            let EntryAction::RunServer(loaded) = result else {
                panic!("server action")
            };
            assert_eq!(loaded.server.limits.max_room_name_length, expected_room);
            assert_eq!(
                loaded.server.limits.max_participant_name_length,
                expected_name
            );
            let policy = rtc_core::config::RoomPolicy::from(&loaded.server);
            assert_eq!(policy.max_participant_name_length, expected_name);
        }
    }
}

#[cfg(test)]
mod command_runtime_separation_tests {
    use super::*;
    use crate::netdetect::tests::FakeProbe;
    fn parse_case(command: &str, tail: &str) -> Result<EntryAction, ConfigError> {
        let body = format!(
            "keys: {{APIowned: owned-secret-for-command-only-not-production}}\nrtc:\n  node_ip: 127.0.0.1\n{tail}"
        );
        let mut args: Vec<OsString> = vec!["livecloud".into(), "--config-body".into(), body.into()];
        if !command.is_empty() {
            args.push(command.into());
        }
        if command == "create-join-token" {
            args.extend(
                ["--room", "room", "--identity", "person"]
                    .into_iter()
                    .map(Into::into),
            );
        }
        parse_with_probe(
            args,
            &Env::default(),
            &ProgramInfo {
                binary_name: "livecloud".into(),
                product_version: "test".into(),
                compat_version: "1.13.10",
            },
            &FakeProbe::local("127.0.0.1"),
        )
    }
    #[test]
    fn public_ports_does_not_construct_unrelated_runtime_listeners() {
        for tail in [
            "port: 70000\n",
            "bind_addresses: [localhost]\n",
            "turn: {enabled: false, udp_port: 70000}\n",
            "turn: {enabled: false, deny_peer_cidrs: [invalid]}\n",
        ] {
            let EntryAction::Ports(cfg) = parse_case("ports", tail)
                .expect("original command consumes official config, not resource normalization")
            else {
                panic!("real ports action required")
            };
            let output = ports_output(&cfg);
            assert!(output.starts_with(if tail.starts_with("port: 70000") {
                "TCP Ports\n70000 - HTTP service\n"
            } else {
                "TCP Ports\n7880 - HTTP service\n"
            }));
        }
    }
    #[test]
    fn public_token_and_local_node_actions_keep_official_data_without_runtime_projection() {
        for command in ["create-join-token", "list-nodes"] {
            let action =
                parse_case(command, "port: 70000\n").expect("no main HTTP listener in command");
            let official = match action {
                EntryAction::CreateJoinToken { config, .. } => config.official,
                EntryAction::ListNodes(config) => config.official,
                _ => panic!("actual command action"),
            };
            assert_eq!(official.port, 70000);
            assert_eq!(
                official.keys.0,
                vec![(
                    "APIowned".into(),
                    "owned-secret-for-command-only-not-production".into()
                )]
            );
        }
    }
    #[test]
    fn real_run_still_rejects_port_that_cannot_be_bound() {
        let error = match parse_case("", "port: 70000\n") {
            Err(e) => e.to_string(),
            Ok(_) => panic!("Run cannot skip actual runtime port validation"),
        };
        assert!(error.contains("70000 is not a valid port"), "{error}");
    }

    // SPEC-0074 C30: a dotted generated flag writes the raw text straight into
    // the `rtc.node_ip` struct leaf, bypassing `NodeIP.UnmarshalString`; the
    // original accepts it and starts, deferring the decision to the ICE gatherer
    // (an invalid literal never matches a rewrite rule → ignored). Rust must not
    // fail early on `ParseIP`, yet a value that *is* a valid IP must still reach
    // the real 1:1 NAT mapping. Covers the flag form and the env form.
    #[test]
    fn generated_raw_node_ip_is_deferred_not_early_rejected() {
        use std::net::Ipv4Addr;
        let parse = |args: &[&str], env: &Env| {
            let argv: Vec<OsString> = std::iter::once("livekit-server")
                .chain(args.iter().copied())
                .map(OsString::from)
                .collect();
            parse_with_probe(
                argv,
                env,
                &ProgramInfo {
                    binary_name: "livekit-server".into(),
                    product_version: "0.1.0".into(),
                    compat_version: "1.13.10",
                },
                &FakeProbe::local("10.0.0.5"),
            )
        };
        // Invalid literal (CLI flag form): accepted, ignored, server starts.
        let EntryAction::RunServer(c) =
            parse(&["--dev", "--rtc.node_ip.ipv4=not-an-ip"], &Env::default())
                .expect("raw invalid node_ip must not fail the load")
        else {
            panic!("real Run consumer required")
        };
        assert!(
            c.server.rtc.node_ip.v4.is_none(),
            "invalid node_ip must be ignored"
        );
        assert!(
            !c.node_ip_auto_generated,
            "non-empty node_ip suppresses discovery"
        );
        // The raw non-empty value still enables the mapping consumer exactly as
        // upstream (`!IsEmpty()`), but the ignored IP means no candidate is
        // rewritten — matching the never-matching rewrite rule.
        assert!(c.server.rtc.use_node_ip_mapping);

        // Invalid literal (environment form): same accept/ignore behavior.
        let EntryAction::RunServer(c) = parse(
            &["--dev"],
            &Env::from_pairs([("LIVEKIT_RTC_NODE_IP_IPV4", "not-an-ip")]),
        )
        .expect("raw invalid node_ip env must not fail the load") else {
            panic!("real Run consumer required")
        };
        assert!(c.server.rtc.node_ip.v4.is_none());

        // Valid IP: the same accepted path must apply the real mapping.
        let EntryAction::RunServer(c) = parse(
            &["--dev", "--rtc.node_ip.ipv4=203.0.113.7"],
            &Env::default(),
        )
        .expect("valid node_ip must start") else {
            panic!("real Run consumer required")
        };
        assert_eq!(c.server.rtc.node_ip.v4, Some(Ipv4Addr::new(203, 0, 113, 7)));
        assert!(c.server.rtc.use_node_ip_mapping);
        assert!(!c.node_ip_auto_generated);
    }
}
