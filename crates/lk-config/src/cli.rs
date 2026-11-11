//! Command line (`LK/cmd/server/main.go:42-188`, impl-notes §1) on clap's builder
//! API. clap only tokenizes: every flag value is taken as a string and parsed
//! with the Go rules afterwards so error texts and env semantics match urfave/cli:
//!
//! - every flag is persistent (accepted after a subcommand name): `global(true)`;
//! - a CLI value beats the env var of the same flag; an env var that is set but
//!   empty counts as set;
//! - bool flags take an optional `=value` (`--dev`, `--dev=false`), never a
//!   separate argument;
//! - repeating a flag keeps the last value (`--bind` accumulates);
//! - Go-style single-dash long flags (`-config x`) are accepted;
//! - unknown positional arguments start the server (urfave runs the root action).
//!
//! Help/usage text is rendered by `help.rs` in urfave's layout, not by clap.

use std::ffi::OsString;

use clap::builder::ValueParser;
use clap::error::{ContextKind, ContextValue, ErrorKind};
use clap::{Arg, ArgAction, ArgMatches};

use crate::env::Env;
use crate::error::ConfigError;
use crate::generated::{LeafSpec, Value, parse_env_value, parse_value, specs};
use crate::help;
use crate::strconv::parse_bool;
#[path = "cli_quote.rs"]
pub(crate) mod quote;
use crate::{EntryAction, ProgramInfo};
use quote::quote as cli_quote;
#[path = "cli_os.rs"]
pub(crate) mod os_input;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum BaseKind {
    Str,
    Bool,
    StrSlice,
}

/// One root flag (`main.go:42-117`).
pub(crate) struct BaseFlag {
    pub name: &'static str,
    pub kind: BaseKind,
    pub env: Option<&'static str>,
    pub usage: &'static str,
    pub hidden: bool,
}

const fn flag(
    name: &'static str,
    kind: BaseKind,
    env: Option<&'static str>,
    usage: &'static str,
) -> BaseFlag {
    BaseFlag {
        name,
        kind,
        env,
        usage,
        hidden: false,
    }
}

/// Base flags in upstream declaration order, usage texts verbatim.
pub(crate) const BASE_FLAGS: &[BaseFlag] = &[
    flag(
        "bind",
        BaseKind::StrSlice,
        None,
        "IP address to listen on, use flag multiple times to specify multiple addresses",
    ),
    flag("config", BaseKind::Str, None, "path to LiveKit config file"),
    flag(
        "config-body",
        BaseKind::Str,
        Some("LIVEKIT_CONFIG"),
        "LiveKit config in YAML, typically passed in as an environment var in a container",
    ),
    flag(
        "key-file",
        BaseKind::Str,
        None,
        "path to file that contains API keys/secrets",
    ),
    flag(
        "keys",
        BaseKind::Str,
        Some("LIVEKIT_KEYS"),
        "api keys (key: secret\\n)",
    ),
    flag(
        "region",
        BaseKind::Str,
        Some("LIVEKIT_REGION"),
        "region of the current node. Used by regionaware node selector",
    ),
    flag(
        "node-ip",
        BaseKind::Str,
        Some("NODE_IP"),
        "IP address of the current node, used to advertise to clients. Automatically determined by default",
    ),
    flag(
        "udp-port",
        BaseKind::Str,
        Some("UDP_PORT"),
        "UDP port(s) to use for WebRTC traffic",
    ),
    flag(
        "redis-host",
        BaseKind::Str,
        Some("REDIS_HOST"),
        "host (incl. port) to redis server",
    ),
    flag(
        "redis-password",
        BaseKind::Str,
        Some("REDIS_PASSWORD"),
        "password to redis",
    ),
    flag(
        "turn-cert",
        BaseKind::Str,
        Some("LIVEKIT_TURN_CERT"),
        "tls cert file for TURN server",
    ),
    flag(
        "turn-key",
        BaseKind::Str,
        Some("LIVEKIT_TURN_KEY"),
        "tls key file for TURN server",
    ),
    flag(
        "cpuprofile",
        BaseKind::Str,
        None,
        "write CPU profile to `file`",
    ),
    flag(
        "memprofile",
        BaseKind::Str,
        None,
        "write memory profile to `file`",
    ),
    flag(
        "dev",
        BaseKind::Bool,
        None,
        "sets log-level to debug, console formatter, and /debug/pprof. insecure for production",
    ),
    BaseFlag {
        name: "disable-strict-config",
        kind: BaseKind::Bool,
        env: None,
        usage: "disables strict config parsing",
        hidden: true,
    },
];

/// A subcommand (`main.go:141-186`).
pub(crate) struct SubCommand {
    pub name: &'static str,
    pub usage: &'static str,
    pub hidden: bool,
}

pub(crate) const SUBCOMMANDS: &[SubCommand] = &[
    SubCommand {
        name: "generate-keys",
        usage: "generates an API key and secret pair",
        hidden: false,
    },
    SubCommand {
        name: "ports",
        usage: "print ports that server is configured to use",
        hidden: false,
    },
    SubCommand {
        name: "create-join-token",
        usage: "create a room join token for development use",
        hidden: true,
    },
    SubCommand {
        name: "list-nodes",
        usage: "list all nodes",
        hidden: false,
    },
    SubCommand {
        name: "help-verbose",
        usage: "prints app help, including all generated configuration flags",
        hidden: false,
    },
];

pub(crate) const HELP_COMMAND_USAGE: &str = "Shows a list of commands or help for one command";

/// The resolved command.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum Command {
    Run,
    GenerateKeys,
    Ports,
    ListNodes,
    CreateJoinToken {
        room: String,
        identity: String,
        recorder: bool,
    },
}

/// Base flag values; `Some` = set on the CLI or through its env var.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct BaseValues {
    /// Owned non-Unicode flag values, never replacement-character placeholders.
    pub os_values: std::collections::BTreeMap<&'static str, OsString>,
    pub https_overrides: std::collections::BTreeMap<String, String>,
    pub bind: Option<Vec<String>>,
    pub config: Option<String>,
    pub config_body: Option<String>,
    pub key_file: Option<String>,
    pub keys: Option<String>,
    pub region: Option<String>,
    pub node_ip: Option<String>,
    pub udp_port: Option<String>,
    pub redis_host: Option<String>,
    pub redis_password: Option<String>,
    pub turn_cert: Option<String>,
    pub turn_key: Option<String>,
    pub cpuprofile: Option<String>,
    pub memprofile: Option<String>,
    pub dev: Option<bool>,
    pub disable_strict_config: Option<bool>,
}

/// Parsed invocation, before any configuration is loaded.
pub(crate) struct Invocation {
    pub command: Command,
    pub base: BaseValues,
    /// Generated flags that are set (CLI beats env), already type-checked.
    pub generated: Vec<(&'static LeafSpec, Value)>,
}

#[allow(
    clippy::large_enum_variant,
    reason = "Startup-only parse result; a single allocation-free value does not affect the media runtime."
)]
pub(crate) enum Parsed {
    Early(EntryAction),
    Invocation(Invocation),
}

const ROOT_ID: &str = "livekit-server";
const POSITIONAL: &str = "args";

fn value_arg(id: &'static str, global: bool) -> Arg {
    Arg::new(id)
        .long(id)
        .action(ArgAction::Append)
        .num_args(1)
        .allow_hyphen_values(true)
        .value_parser(ValueParser::os_string())
        .global(global)
}

fn bool_arg(id: &'static str, global: bool) -> Arg {
    Arg::new(id)
        .long(id)
        .action(ArgAction::Append)
        .num_args(0..=1)
        .require_equals(true)
        .default_missing_value("true")
        .value_parser(ValueParser::os_string())
        .global(global)
}

fn build_command() -> clap::Command {
    let mut root = clap::Command::new(ROOT_ID)
        .disable_help_flag(true)
        .disable_version_flag(true)
        .disable_help_subcommand(true)
        .arg(bool_arg("help", true).short('h'))
        .arg(bool_arg("version", false).short('v'))
        .arg(
            Arg::new(POSITIONAL)
                .action(ArgAction::Append)
                .num_args(1..)
                .value_parser(ValueParser::os_string()),
        );
    for f in BASE_FLAGS {
        root = root.arg(match f.kind {
            BaseKind::Bool => bool_arg(f.name, true),
            BaseKind::Str | BaseKind::StrSlice => value_arg(f.name, true),
        });
    }
    for s in specs() {
        let id: &'static str = s.path.as_str();
        root = root.arg(if s.kind.takes_value() {
            value_arg(id, true)
        } else {
            bool_arg(id, true)
        });
    }
    for sc in SUBCOMMANDS {
        let mut c = clap::Command::new(sc.name).arg(
            Arg::new(POSITIONAL)
                .action(ArgAction::Append)
                .num_args(1..)
                .value_parser(ValueParser::os_string()),
        );
        if sc.name == "create-join-token" {
            c = c
                .arg(value_arg("room", false))
                .arg(value_arg("identity", false))
                .arg(bool_arg("recorder", false));
        }
        root = root.subcommand(c);
    }
    root.subcommand(
        clap::Command::new("help").alias("h").arg(
            Arg::new("topic")
                .action(ArgAction::Append)
                .num_args(1..)
                .value_parser(ValueParser::os_string()),
        ),
    )
}

/// Every long flag name clap knows (for single-dash normalisation).
fn is_known_long(name: &str) -> bool {
    matches!(name, "help" | "version" | "room" | "identity" | "recorder")
        || BASE_FLAGS.iter().any(|f| f.name == name)
        || specs().iter().any(|s| s.path == name)
}

/// Whether this token consumes the next argv as a value. Go's flag parser
/// consumes a string value verbatim, including `--` or another flag spelling.
/// Boolean flags only accept an optional inline `=value`.
fn consumes_next_arg(arg: &str) -> bool {
    if !arg.starts_with('-') || arg == "--" || arg.contains('=') {
        return false;
    }
    let name = arg.trim_start_matches('-');
    if matches!(name, "room" | "identity") {
        return true;
    }
    if let Some(field) = name.strip_prefix("livecloud-https-") {
        return HTTPS_FIELDS.contains(&field);
    }
    BASE_FLAGS
        .iter()
        .find(|flag| flag.name == name)
        .map(|flag| flag.kind != BaseKind::Bool)
        .or_else(|| {
            specs()
                .iter()
                .find(|spec| spec.path == name)
                .map(|spec| spec.kind.takes_value())
        })
        .unwrap_or(false)
}

/// Rewrites Go-style `-flag[=v]` to `--flag[=v]` for known long flags,
/// preserving the exact bytes of every value token.
fn normalize_args(args: Vec<OsString>) -> Vec<OsString> {
    let mut out = Vec::with_capacity(args.len());
    let mut passthrough = false;
    let mut value = false;
    for (i, a) in args.into_iter().enumerate() {
        if i == 0 || passthrough || value {
            value = false;
            out.push(a);
            continue;
        }
        let Some(s) = a.to_str() else {
            out.push(a);
            continue;
        };
        value = consumes_next_arg(s);
        if s == "--" {
            passthrough = true;
        } else if s == "-"
            || (s.starts_with('-')
                && !s.starts_with("--")
                && s.len() > 1
                && !s[1..].chars().next().is_some_and(char::is_alphabetic))
        {
            out.push(OsString::from("--"));
            passthrough = true;
        } else if s.len() > 2 && s.starts_with('-') && !s.starts_with("--") {
            let name = s[1..].split('=').next().unwrap_or("");
            if is_known_long(name) {
                out.push(OsString::from(format!("-{s}")));
                continue;
            }
        }
        out.push(a);
    }
    out
}

const HTTPS_FIELDS: &[&str] = &[
    "port",
    "cert-file",
    "key-file",
    "bind-addresses",
    "http-enabled",
    "max-connections",
    "handshake-timeout-ms",
];

fn extract_extension_flags(
    args: Vec<OsString>,
) -> Result<(Vec<OsString>, std::collections::BTreeMap<String, String>), ConfigError> {
    let mut out = Vec::new();
    let mut values = std::collections::BTreeMap::new();
    let mut args = args.into_iter();
    if let Some(first) = args.next() {
        out.push(first);
    }
    while let Some(arg) = args.next() {
        let text = arg.to_string_lossy();
        if text == "--" {
            out.push(arg);
            out.extend(args);
            break;
        }
        if let Some(rest) = text.strip_prefix("--livecloud-") {
            let (name, inline) = rest
                .split_once('=')
                .map_or((rest, None), |(n, v)| (n, Some(v)));
            let field = name
                .strip_prefix("https-")
                .filter(|f| HTTPS_FIELDS.contains(f))
                .ok_or_else(|| {
                    ConfigError::msg(format!("unknown extension flag \"--livecloud-{name}\""))
                })?;
            let value = match inline {
                Some(v) => v.to_string(),
                None => args
                    .next()
                    .and_then(|v| v.into_string().ok())
                    .ok_or_else(|| {
                        ConfigError::msg(format!("--livecloud-{name} requires a value"))
                    })?,
            };
            values.insert(field.replace('-', "_"), value);
        } else {
            let consumes_value = consumes_next_arg(&text);
            out.push(arg);
            if consumes_value && let Some(value) = args.next() {
                // An official flag's value is never an extension invocation.
                out.push(value);
            }
        }
    }
    Ok((out, values))
}

fn extension_env(env: &Env) -> Result<std::collections::BTreeMap<String, String>, ConfigError> {
    extension_env_skipping(env, &std::collections::BTreeMap::new())
}

fn extension_env_skipping(
    env: &Env,
    cli: &std::collections::BTreeMap<String, String>,
) -> Result<std::collections::BTreeMap<String, String>, ConfigError> {
    let mut out = std::collections::BTreeMap::new();
    // Declared names are the consumer registry. Unknown ambient ENV stays ignored,
    // but a declared source must not vanish merely because its value is raw OS bytes.
    for field in HTTPS_FIELDS {
        let field = field.replace('-', "_");
        if cli.contains_key(&field) {
            continue; // Actual explicit CLI value overrides even malformed ENV.
        }
        let name = format!("LIVECLOUD_HTTPS_{}", field.to_ascii_uppercase());
        let Some(raw) = env.get_os(&name) else {
            continue;
        };
        let value = match raw.to_str() {
            Some(value) => value.to_owned(),
            None if matches!(
                field.as_str(),
                "port" | "http_enabled" | "max_connections" | "handshake_timeout_ms"
            ) =>
            {
                // These numeric/bool grammars accept ASCII only. A non-Unicode
                // value is a genuine invalid typed input, never an absent source.
                return Err(ConfigError::msg(format!(
                    "invalid livecloud.https.{field} value {}",
                    quote::quote_bytes(raw.as_encoded_bytes())
                )));
            }
            None => {
                return Err(ConfigError::unsupported(
                    format!("LIVECLOUD_HTTPS_{}", field.to_ascii_uppercase()),
                    "non-Unicode proprietary string/path consumer remains OPEN",
                ));
            }
        };
        out.insert(field, value);
    }
    Ok(out)
}

fn usage_error(info: &ProgramInfo, msg: &str, command: Option<&str>) -> EntryAction {
    let help = match command {
        Some(c) => help::command_help(info, c),
        None => help::root_help(info, false),
    };
    EntryAction::PrintHelp {
        text: format!("Incorrect Usage: {msg}\n\n{help}"),
        to_stderr: true,
        exit_code: 1,
    }
}

// urfave builds the subcommand usage suffix only after flag parsing has
// succeeded. Parse failures show the command without `[options]`.
fn flag_usage_error(info: &ProgramInfo, msg: &str, command: Option<&str>) -> EntryAction {
    // The built-in help command has HideHelp=true and inherits no local HelpFlag.
    if command == Some("help") {
        return EntryAction::PrintHelp {
            text: format!("Incorrect Usage: {msg}\n\n"),
            to_stderr: true,
            exit_code: 1,
        };
    }
    let mut action = usage_error(info, msg, command);
    if let (Some(command), EntryAction::PrintHelp { text, .. }) = (command, &mut action) {
        *text = text.replace(
            &format!("   {} {command} [options]\n", info.binary_name),
            &format!("   {} {command}\n", info.binary_name),
        );
    }
    action
}

fn context_string(err: &clap::Error, kind: ContextKind) -> Option<String> {
    match err.get(kind)? {
        ContextValue::String(s) => Some(s.clone()),
        ContextValue::Strings(v) => v.first().cloned(),
        other => Some(other.to_string()),
    }
}

/// `--config <config>` → `config`
fn flag_name_of(s: &str) -> String {
    s.split_whitespace()
        .next()
        .unwrap_or(s)
        .trim_start_matches('-')
        .split('=')
        .next()
        .unwrap_or("")
        .to_string()
}

/// Translates a clap error into urfave's wording.
fn clap_error_message(err: &clap::Error, original_args: &[OsString]) -> String {
    match err.kind() {
        ErrorKind::UnknownArgument => {
            let arg = context_string(err, ContextKind::InvalidArg).unwrap_or_default();
            format!("flag provided but not defined: -{}", flag_name_of(&arg))
        }
        ErrorKind::InvalidValue | ErrorKind::NoEquals | ErrorKind::WrongNumberOfValues => {
            let arg = context_string(err, ContextKind::InvalidArg).unwrap_or_default();
            let name = flag_name_of(&arg);
            let spelling = original_args
                .iter()
                .rev()
                .filter_map(|arg| arg.to_str())
                .find(|arg| arg.starts_with('-') && flag_name_of(arg) == name)
                .unwrap_or(arg.split_whitespace().next().unwrap_or(&arg));
            format!("flag needs an argument: {spelling}")
        }
        _ => {
            let rendered = err.render().to_string();
            rendered
                .lines()
                .next()
                .unwrap_or("")
                .trim_start_matches("error: ")
                .to_string()
        }
    }
}

/// Last CLI value of `id` in `m`, if given on the command line.
fn cli_last_os(m: &ArgMatches, id: &str) -> Option<OsString> {
    m.try_get_many::<OsString>(id)
        .ok()
        .flatten()?
        .next_back()
        .cloned()
}
fn cli_last(m: &ArgMatches, id: &str) -> Option<String> {
    cli_last_os(m, id)?.into_string().ok()
}

fn cli_all(m: &ArgMatches, id: &str) -> Option<Vec<String>> {
    let v: Vec<String> = m
        .try_get_many::<OsString>(id)
        .ok()
        .flatten()?
        .filter_map(|v| v.to_str().map(str::to_owned))
        .collect();
    Some(v)
}

fn cli_flag(m: &ArgMatches, id: &str) -> bool {
    cli_last(m, id).is_some_and(|value| parse_cli_bool(&value).unwrap_or(false))
}

/// urfave v3 treats an explicitly empty CLI boolean value as true. This is
/// deliberately separate from generated ENV parsing, where empty means false.
fn parse_cli_bool(value: &str) -> Result<bool, String> {
    if value.is_empty() {
        Ok(true)
    } else {
        parse_bool(value)
    }
}

/// Collects base flag values (CLI beats env; set-but-empty env counts).
fn base_values(
    m: &ArgMatches,
    env: &Env,
    cmd: Option<&str>,
    info: &ProgramInfo,
) -> Result<BaseValues, EntryAction> {
    let mut b = BaseValues::default();
    for f in BASE_FLAGS {
        if f.kind != BaseKind::Bool {
            let raw = if f.kind == BaseKind::StrSlice {
                m.try_get_many::<OsString>(f.name)
                    .ok()
                    .flatten()
                    .and_then(|mut values| values.find(|v| v.to_str().is_none()).cloned())
                    .or_else(|| cli_last_os(m, f.name))
            } else {
                cli_last_os(m, f.name)
            }
            .or_else(|| {
                f.env
                    .and_then(|key| env.get_os(key))
                    .map(std::ffi::OsStr::to_os_string)
            });
            if let Some(raw) = raw.filter(|v| v.to_str().is_none()) {
                b.os_values.insert(f.name, raw);
            }
        }
        match f.kind {
            BaseKind::StrSlice => {
                if let Some(vals) = cli_all(m, f.name) {
                    let split: Vec<String> = vals
                        .iter()
                        .flat_map(|v| v.split(','))
                        .map(|s| s.trim().to_string())
                        .collect();
                    b.bind = Some(split);
                }
            }
            BaseKind::Bool => {
                let v = match cli_last(m, f.name) {
                    Some(s) => Some(parse_cli_bool(&s).map_err(|_| {
                        flag_usage_error(
                            info,
                            &format!(
                                "invalid value {} for flag -{}: parse error",
                                cli_quote(&s),
                                f.name
                            ),
                            cmd,
                        )
                    })?),
                    None => None,
                };
                match f.name {
                    "dev" => b.dev = v,
                    "disable-strict-config" => b.disable_strict_config = v,
                    _ => {}
                }
            }
            BaseKind::Str => {
                let v = cli_last(m, f.name)
                    .or_else(|| f.env.and_then(|e| env.get(e)).map(str::to_string));
                let slot = match f.name {
                    "config" => &mut b.config,
                    "config-body" => &mut b.config_body,
                    "key-file" => &mut b.key_file,
                    "keys" => &mut b.keys,
                    "region" => &mut b.region,
                    "node-ip" => &mut b.node_ip,
                    "udp-port" => &mut b.udp_port,
                    "redis-host" => &mut b.redis_host,
                    "redis-password" => &mut b.redis_password,
                    "turn-cert" => &mut b.turn_cert,
                    "turn-key" => &mut b.turn_key,
                    "cpuprofile" => &mut b.cpuprofile,
                    "memprofile" => &mut b.memprofile,
                    _ => continue,
                };
                *slot = v;
            }
        }
    }
    Ok(b)
}

enum GenError {
    Usage(EntryAction),
    Config(ConfigError),
}

/// Collects and type-checks generated flags (CLI first, then `LIVEKIT_*` env).
fn generated_values(
    m: &ArgMatches,
    env: &Env,
    cmd: Option<&str>,
    info: &ProgramInfo,
) -> Result<Vec<(&'static LeafSpec, Value)>, GenError> {
    let mut out = Vec::new();
    for s in specs() {
        if let Some(raw) = cli_last_os(m, &s.path).filter(|v| v.to_str().is_none()) {
            if s.kind == crate::generated::Kind::Str {
                out.push((s, Value::OsStr(raw)));
                continue;
            }
            return Err(GenError::Config(ConfigError::unsupported(
                s.path.clone(),
                format!(
                    "non-Unicode generated string projection remains OPEN: {}",
                    quote::quote_bytes(raw.as_encoded_bytes())
                ),
            )));
        }
        if let Some(raw) = cli_last(m, &s.path) {
            let parsed = if s.kind == crate::generated::Kind::Bool {
                parse_cli_bool(&raw).map(Value::Bool)
            } else {
                parse_value(s.kind, &raw)
            };
            let v = parsed.map_err(|e| {
                let e = if s.kind == crate::generated::Kind::Bool {
                    "parse error".to_string()
                } else {
                    e
                };
                GenError::Usage(flag_usage_error(
                    info,
                    &format!(
                        "invalid value {} for flag -{}: {e}",
                        cli_quote(&raw),
                        s.path
                    ),
                    cmd,
                ))
            })?;
            out.push((s, v));
        } else if let Some(raw_os) = env.get_os(&s.env) {
            if s.kind == crate::generated::Kind::Str && raw_os.to_str().is_none() {
                out.push((s, Value::OsStr(raw_os.to_os_string())));
                continue;
            }
            let raw=raw_os.to_str().ok_or_else(||GenError::Config(ConfigError::msg(format!(
                "could not parse {} as {} value from environment variable \"{}\" for flag {}: {}",
                quote::quote_bytes(raw_os.as_encoded_bytes()),s.kind.env_type_name(),s.env,s.path,
                os_input::invalid_value(s.kind,raw_os.as_encoded_bytes())
            ))))?;
            let v = parse_env_value(s.kind, raw).map_err(|e| {
                // boolValue.Set replaces the strconv error.
                let e = if s.kind == crate::generated::Kind::Bool {
                    "parse error".to_string()
                } else {
                    e
                };
                GenError::Config(ConfigError::msg(format!(
                    "could not parse {} as {} value from environment variable \"{}\" for flag {}: {e}",
                    cli_quote(raw),
                    s.kind.env_type_name(),
                    s.env,
                    s.path
                )))
            })?;
            out.push((s, v));
        }
    }
    Ok(out)
}

/// Recover the active subcommand when clap omits a Usage context (for example
/// a missing value). Skip option values so `--config ports` stays root context.
fn command_from_args(args: &[OsString]) -> Option<&'static str> {
    let mut args = args.iter().skip(1);
    while let Some(arg) = args.next().and_then(|arg| arg.to_str()) {
        if let Some(command) = SUBCOMMANDS.iter().find(|command| command.name == arg) {
            return Some(command.name);
        }
        if !arg.starts_with('-') || arg == "--" {
            return None;
        }
        let name = flag_name_of(arg);
        if !is_known_long(&name) && !matches!(name.as_str(), "h" | "v") {
            return None;
        }
        let takes_value = BASE_FLAGS
            .iter()
            .find(|flag| flag.name == name)
            .map(|flag| flag.kind != BaseKind::Bool)
            .or_else(|| {
                specs()
                    .iter()
                    .find(|spec| spec.path == name)
                    .map(|spec| spec.kind.takes_value())
            })
            .unwrap_or(false);
        if takes_value && !arg.contains('=') {
            args.next();
        }
    }
    None
}

/// Root flags are parsed and PostParse ENV validation runs BEFORE a recognized
/// child receives its remaining argv (urfave command_parse.go115–121/run.go).
fn child_boundary(args: &[OsString]) -> Option<(usize, bool)> {
    let mut at = 1;
    let mut first_positional = true;
    while at < args.len() {
        let value = args[at].to_str()?.trim();
        if value == "--" || value == "-" {
            return None;
        }
        if value.starts_with('-')
            && !value.starts_with("--")
            && !value[1..].chars().next().is_some_and(char::is_alphabetic)
        {
            return None;
        }
        if !value.starts_with('-') || value == "-" {
            // Unknown first positional chooses the root action, not a later
            // command-looking word. Preserve existing root action semantics.
            if SUBCOMMANDS.iter().any(|c| c.name == value) || matches!(value, "help" | "h") {
                return Some((at, first_positional));
            }
            first_positional = false;
        } else if consumes_next_arg(value) {
            at += 1;
        }
        at += 1;
    }
    None
}

/// Check original explicit tokens in source parse order, before clap's eventual
/// structural failure can hide an earlier typed error/help flag. This does not
/// load configuration or ENV. Values are consumed verbatim, including flag-like
/// strings; booleans only consume inline values.
fn preflight_stage(args: &[OsString], info: &ProgramInfo) -> Result<(), EntryAction> {
    os_input::preflight(args, info)
}

fn help_action(
    info: &ProgramInfo,
    command: Option<&str>,
    topic: Option<&str>,
    builtin_command: bool,
) -> EntryAction {
    let text = match topic.filter(|value| !value.is_empty()) {
        None => match command {
            Some(c) => help::command_help(info, c),
            None => help::root_help(info, false),
        },
        Some(topic) => {
            let found = if command.is_none() {
                SUBCOMMANDS.iter().any(|c| c.name == topic) || matches!(topic, "help" | "h")
            } else {
                matches!(topic, "help" | "h")
            };
            if !found {
                return EntryAction::PrintHelp {
                    text: format!("No help topic for '{topic}'\n"),
                    // Built-in help handles ExitCoder itself; --help returns
                    // the error to main.go, which prints it on stdout and exits1.
                    to_stderr: builtin_command,
                    exit_code: if builtin_command { 3 } else { 1 },
                };
            }
            if let Some(parent) = command {
                help::nested_builtin_help(info, parent)
            } else {
                help::command_help(info, if topic == "h" { "help" } else { topic })
            }
        }
    };
    EntryAction::PrintHelp {
        text,
        to_stderr: false,
        exit_code: 0,
    }
}

fn stage_help(info: &ProgramInfo, command: Option<&str>) -> EntryAction {
    let mut text = match command {
        Some("help") | None => help::root_help(info, false),
        Some(c) => help::command_help(info, c),
    };
    if let Some(command) = command {
        text = text.replace(
            &format!("   {} {command} [options]\n", info.binary_name),
            &format!("   {} {command}\n", info.binary_name),
        );
    }
    EntryAction::PrintHelp {
        text,
        to_stderr: false,
        exit_code: 0,
    }
}

#[path = "cli_stage.rs"]
mod stages;

/// Explicit product CLI extensions retain their opt-in parser. Environment
/// variables never choose a parser: official stages run before registered
/// proprietary ENV is projected only onto a final server Run invocation.
pub(crate) fn parse_args(
    args: Vec<OsString>,
    env: &Env,
    info: &ProgramInfo,
) -> Result<Parsed, ConfigError> {
    let mut tokens = args.iter().skip(1);
    let mut extension = false;
    while let Some(token) = tokens.next().and_then(|value| value.to_str()) {
        let token = token.trim();
        if token == "--" {
            break;
        }
        if token.starts_with("--livecloud-") {
            extension = true;
            break;
        }
        if consumes_next_arg(token) {
            tokens.next();
        }
    }
    if extension {
        parse_extension_namespace_args(args, env, info)
    } else {
        let mut parsed = stages::parse(args, env, info)?;
        if let Parsed::Invocation(invocation) = &mut parsed
            && invocation.command == Command::Run
        {
            invocation.base.https_overrides = extension_env(env)?;
        }
        Ok(parsed)
    }
}

/// Parse root and child in source order; root parse/ENV work is pure and never
/// executes a server or a command action.
fn parse_extension_namespace_args(
    args: Vec<OsString>,
    env: &Env,
    info: &ProgramInfo,
) -> Result<Parsed, ConfigError> {
    if let Some((at, dispatch)) = child_boundary(&args) {
        let (root_tokens, _) = extract_extension_flags(args[..at].to_vec())?;
        let root_flags_valid = preflight_stage(&root_tokens, info).is_ok();
        match parse_single_stage(args[..at].to_vec(), env, info, dispatch)? {
            Parsed::Early(action) => {
                if dispatch
                    && root_flags_valid
                    && matches!(
                        &action,
                        EntryAction::PrintHelp {
                            exit_code: 0,
                            to_stderr: false,
                            ..
                        }
                    )
                {
                    return Ok(Parsed::Early(help_action(
                        info,
                        None,
                        args[at].to_str(),
                        false,
                    )));
                }
                return Ok(Parsed::Early(action));
            }
            Parsed::Invocation(invocation) if !dispatch => {
                return Ok(Parsed::Invocation(invocation));
            }
            Parsed::Invocation(_) => {}
        }
    }
    parse_single_stage(args, env, info, false)
}

/// Parses argv. Help, version and usage errors come back as [`Parsed::Early`].
fn parse_single_stage(
    args: Vec<OsString>,
    env: &Env,
    info: &ProgramInfo,
    official_root_preflight: bool,
) -> Result<Parsed, ConfigError> {
    let original_args = args.clone();
    let (preflight_args, _) = extract_extension_flags(args.clone())?;
    if let Err(action) = preflight_stage(&preflight_args, info) {
        return Ok(Parsed::Early(action));
    }
    let args = normalize_args(args);
    let (args, https_cli) = extract_extension_flags(args)?;

    let matches = match build_command().try_get_matches_from(args) {
        Ok(m) => m,
        Err(e) => {
            let usage = context_string(&e, ContextKind::Usage).unwrap_or_default();
            let command = SUBCOMMANDS
                .iter()
                .find(|command| usage.split_whitespace().any(|word| word == command.name))
                .map(|command| command.name)
                .or_else(|| command_from_args(&original_args));
            return Ok(Parsed::Early(flag_usage_error(
                info,
                &clap_error_message(&e, &original_args),
                command,
            )));
        }
    };

    let (sub, m): (Option<&str>, &ArgMatches) = match matches.subcommand() {
        Some((name, sm)) => (Some(name), sm),
        None => (None, &matches),
    };

    if cli_flag(m, "help") {
        let topic = m
            .try_get_many::<OsString>(POSITIONAL)
            .ok()
            .flatten()
            .and_then(|mut values| values.next())
            .and_then(|value| value.to_str());
        return Ok(Parsed::Early(help_action(info, sub, topic, false)));
    }
    if cli_flag(&matches, "version") {
        return Ok(Parsed::Early(EntryAction::PrintVersion(
            help::version_line(info),
        )));
    }

    let command = match sub {
        None => Command::Run,
        Some("generate-keys") => Command::GenerateKeys,
        Some("ports") => Command::Ports,
        Some("list-nodes") => Command::ListNodes,
        Some("help-verbose") => {
            return Ok(Parsed::Early(EntryAction::PrintHelp {
                text: help::root_help(info, true),
                to_stderr: false,
                exit_code: 0,
            }));
        }
        Some("help") => {
            let topic = cli_all(m, "topic").and_then(|values| values.into_iter().next());
            return Ok(Parsed::Early(help_action(
                info,
                None,
                topic.as_deref(),
                true,
            )));
        }
        Some("create-join-token") => {
            let room = cli_last(m, "room");
            let identity = cli_last(m, "identity");
            let missing: Vec<&str> = [("room", room.is_none()), ("identity", identity.is_none())]
                .iter()
                .filter(|(_, miss)| *miss)
                .map(|(n, _)| *n)
                .collect();
            if !missing.is_empty() {
                let msg = if missing.len() == 1 {
                    format!("Required flag \"{}\" not set", missing[0])
                } else {
                    format!("Required flags \"{}\" not set", missing.join(", "))
                };
                return Ok(Parsed::Early(usage_error(info, &msg, sub)));
            }
            let recorder = match cli_last(m, "recorder") {
                Some(s) => match parse_cli_bool(&s) {
                    Ok(b) => b,
                    Err(_) => {
                        return Ok(Parsed::Early(flag_usage_error(
                            info,
                            &format!(
                                "invalid value {} for flag -recorder: parse error",
                                cli_quote(&s)
                            ),
                            sub,
                        )));
                    }
                },
                None => false,
            };
            Command::CreateJoinToken {
                room: room.unwrap_or_default(),
                identity: identity.unwrap_or_default(),
                recorder,
            }
        }
        Some(other) => {
            // unreachable with the command set above; keep urfave wording
            return Ok(Parsed::Early(usage_error(
                info,
                &format!("No help topic for '{other}'"),
                None,
            )));
        }
    };

    let mut base = match base_values(m, env, sub, info) {
        Ok(b) => b,
        Err(early) => return Ok(Parsed::Early(early)),
    };
    let generated = match generated_values(m, env, sub, info) {
        Ok(g) => g,
        Err(GenError::Usage(early)) => return Ok(Parsed::Early(early)),
        Err(GenError::Config(e)) => return Err(e),
    };
    if !official_root_preflight && command == Command::Run {
        base.https_overrides = extension_env_skipping(env, &https_cli)?;
        base.https_overrides.extend(https_cli);
    }
    Ok(Parsed::Invocation(Invocation {
        command,
        base,
        generated,
    }))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn info() -> ProgramInfo {
        ProgramInfo {
            binary_name: "livecloud".into(),
            product_version: "0.1.0".into(),
            compat_version: "1.13.10",
        }
    }

    fn args(a: &[&str]) -> Vec<OsString> {
        std::iter::once("livecloud")
            .chain(a.iter().copied())
            .map(OsString::from)
            .collect()
    }

    fn inv(a: &[&str], env: &Env) -> Invocation {
        match parse_args(args(a), env, &info()).unwrap() {
            Parsed::Invocation(i) => i,
            Parsed::Early(e) => panic!("unexpected early action: {e:?}"),
        }
    }

    fn early(a: &[&str], env: &Env) -> EntryAction {
        match parse_args(args(a), env, &info()).unwrap() {
            Parsed::Early(e) => e,
            Parsed::Invocation(_) => panic!("expected early action"),
        }
    }

    #[test]
    fn cli_beats_env_and_empty_env_counts() {
        let env = Env::from_pairs([("LIVEKIT_KEYS", "a: b"), ("NODE_IP", "")]);
        let i = inv(&["--keys", "c: d"], &env);
        assert_eq!(i.base.keys.as_deref(), Some("c: d"));
        assert_eq!(i.base.node_ip.as_deref(), Some(""));
        let i = inv(&[], &env);
        assert_eq!(i.base.keys.as_deref(), Some("a: b"));
        assert_eq!(i.base.region, None);
    }

    #[test]
    fn generated_env_empty_is_set_to_zero() {
        let env = Env::from_pairs([("LIVEKIT_PORT", "")]);
        let i = inv(&[], &env);
        assert_eq!(i.generated.len(), 1);
        assert_eq!(i.generated[0].0.path, "port");
        assert_eq!(i.generated[0].1, Value::Uint(0));
    }

    #[test]
    fn generated_env_parse_error() {
        let env = Env::from_pairs([("LIVEKIT_PORT", "abc")]);
        let err = match parse_args(args(&[]), &env, &info()) {
            Err(e) => e.to_string(),
            Ok(_) => panic!("expected error"),
        };
        assert!(err.starts_with("could not parse \"abc\" as uint value from environment variable \"LIVEKIT_PORT\" for flag port: "), "{err}");
    }

    #[test]
    fn flags_after_subcommand_and_single_dash() {
        let i = inv(&["ports", "--config", "x.yaml", "-dev"], &Env::default());
        assert_eq!(i.command, Command::Ports);
        assert_eq!(i.base.config.as_deref(), Some("x.yaml"));
        assert_eq!(i.base.dev, Some(true));
        let i = inv(
            &["--dev=false", "--rtc.udp_port.start", "9000"],
            &Env::default(),
        );
        assert_eq!(i.base.dev, Some(false));
        assert_eq!(i.generated[0].0.path, "rtc.udp_port.start");
        assert_eq!(i.generated[0].1, Value::Int(9000));
    }

    #[test]
    fn bind_accumulates_and_splits() {
        let i = inv(
            &["--bind", "127.0.0.1,::1", "--bind", "10.0.0.1"],
            &Env::default(),
        );
        assert_eq!(
            i.base.bind,
            Some(vec!["127.0.0.1".into(), "::1".into(), "10.0.0.1".into()])
        );
    }

    #[test]
    fn usage_errors() {
        match early(&["--unknown-flag"], &Env::default()) {
            EntryAction::PrintHelp {
                text,
                to_stderr,
                exit_code,
            } => {
                assert!(
                    text.starts_with(
                        "Incorrect Usage: flag provided but not defined: -unknown-flag"
                    ),
                    "{text}"
                );
                assert!(to_stderr);
                assert_eq!(exit_code, 1);
            }
            other => panic!("{other:?}"),
        }
        match early(&["--dev=invalid"], &Env::default()) {
            EntryAction::PrintHelp { text, .. } => {
                assert!(text.contains("invalid value"), "{text}")
            }
            other => panic!("{other:?}"),
        }
        match early(&["create-join-token", "--room", "r"], &Env::default()) {
            EntryAction::PrintHelp { text, .. } => {
                assert!(
                    text.starts_with("Incorrect Usage: Required flag \"identity\" not set"),
                    "{text}"
                )
            }
            other => panic!("{other:?}"),
        }
    }

    #[test]
    fn invalid_explicit_values_preempt_help_and_version() {
        for argv in [
            vec!["--dev=invalid", "--help"],
            vec!["--dev=invalid", "--version"],
            vec!["--dev=invalid", "--dev=true", "--help"],
            vec!["--rtc.allow_tcp_fallback=invalid", "--help"],
            vec!["--rtc.tcp_port=invalid", "--help"],
            vec!["create-join-token", "--recorder=invalid", "--help"],
        ] {
            match early(&argv, &Env::default()) {
                EntryAction::PrintHelp {
                    text,
                    to_stderr,
                    exit_code,
                } => {
                    assert!(
                        text.starts_with("Incorrect Usage: invalid value \"invalid\" for flag -"),
                        "{text}"
                    );
                    assert!(to_stderr);
                    assert_eq!(exit_code, 1);
                }
                other => panic!("{other:?}"),
            }
        }
    }

    #[test]
    fn help_and_version() {
        match early(&["--help"], &Env::default()) {
            EntryAction::PrintHelp {
                text,
                to_stderr,
                exit_code,
            } => {
                assert!(text.contains("High performance WebRTC server"));
                assert!(text.contains("livecloud"));
                assert!(!text.contains("rtc.tcp_port"));
                assert!(!to_stderr);
                assert_eq!(exit_code, 0);
            }
            other => panic!("{other:?}"),
        }
        match early(&["help-verbose"], &Env::default()) {
            EntryAction::PrintHelp { text, .. } => {
                assert!(text.contains("--rtc.tcp_port uint"));
                assert!(text.contains("[$LIVEKIT_RTC_TCP_PORT]"));
            }
            other => panic!("{other:?}"),
        }
        match early(&["--version"], &Env::default()) {
            // C31: urfave/cli exact shape `{name} version {product_version}`,
            // no compatibility suffix; production passes product_version 1.13.10.
            EntryAction::PrintVersion(v) => assert_eq!(v, "livecloud version 0.1.0"),
            other => panic!("{other:?}"),
        }
    }

    #[test]
    fn usage_errors_preserve_official_flag_and_subcommand_context() {
        for (argv, header, command) in [
            (vec!["--config"], "flag needs an argument: --config", None),
            (vec!["-config"], "flag needs an argument: -config", None),
            (
                vec!["ports", "--config"],
                "flag needs an argument: --config",
                Some("ports"),
            ),
            (
                vec!["ports", "--unknown-flag"],
                "flag provided but not defined: -unknown-flag",
                Some("ports"),
            ),
            (
                vec!["ports", "--dev=invalid"],
                "invalid value \"invalid\" for flag -dev: parse error",
                Some("ports"),
            ),
        ] {
            let EntryAction::PrintHelp {
                text,
                to_stderr,
                exit_code,
            } = early(&argv, &Env::default())
            else {
                panic!("expected usage error")
            };
            assert!(
                text.starts_with(&format!("Incorrect Usage: {header}\n\n")),
                "{text}"
            );
            assert!(to_stderr);
            assert_eq!(exit_code, 1);
            if let Some(command) = command {
                assert!(text.contains(&format!("USAGE:\n   livecloud {command}\n")));
            }
        }
    }

    #[test]
    fn create_join_token_values() {
        let i = inv(
            &[
                "create-join-token",
                "--room",
                "r",
                "--identity",
                "me",
                "--recorder",
            ],
            &Env::default(),
        );
        assert_eq!(
            i.command,
            Command::CreateJoinToken {
                room: "r".into(),
                identity: "me".into(),
                recorder: true
            }
        );
    }

    #[test]
    fn explicit_extension_flags_are_reserved_but_unknown_env_is_ambient() {
        assert!(parse_args(args(&["--livecloud-foo=1"]), &Env::default(), &info()).is_err());
        let env = Env::from_pairs([("LIVECLOUD_X", "1")]);
        let invocation = inv(&[], &env);
        assert_eq!(invocation.command, Command::Run);
        assert!(invocation.base.https_overrides.is_empty());
    }

    #[test]
    fn unknown_positional_starts_server() {
        assert_eq!(inv(&["whatever"], &Env::default()).command, Command::Run);
    }
    #[test]
    fn empty_cli_boolean_values_mean_true_for_all_flag_families() {
        let parsed = inv(
            &[
                "--dev=",
                "--disable-strict-config=",
                "--rtc.congestion_control.enabled=",
            ],
            &Env::default(),
        );
        assert_eq!(parsed.base.dev, Some(true));
        assert_eq!(parsed.base.disable_strict_config, Some(true));
        let (_, value) = parsed
            .generated
            .iter()
            .find(|(spec, _)| spec.path == "rtc.congestion_control.enabled")
            .unwrap();
        assert_eq!(value, &Value::Bool(true));
        let parsed = inv(
            &[
                "create-join-token",
                "--room=r",
                "--identity=i",
                "--recorder=",
            ],
            &Env::default(),
        );
        assert_eq!(
            parsed.command,
            Command::CreateJoinToken {
                room: "r".into(),
                identity: "i".into(),
                recorder: true
            }
        );
    }

    #[test]
    fn empty_generated_env_bool_stays_false_but_cli_empty_overrides_it() {
        let env = Env::from_pairs([("LIVEKIT_RTC_CONGESTION_CONTROL_ENABLED", "")]);
        for (flags, expected) in [
            (&[][..], false),
            (&["--rtc.congestion_control.enabled="][..], true),
            (&["--rtc.congestion_control.enabled=false"][..], false),
        ] {
            let parsed = inv(flags, &env);
            let (_, value) = parsed
                .generated
                .iter()
                .find(|(spec, _)| spec.path == "rtc.congestion_control.enabled")
                .unwrap();
            assert_eq!(value, &Value::Bool(expected));
        }
    }

    #[test]
    fn string_values_that_look_like_known_flags_are_not_rewritten() {
        for (flag, value) in [("--region", "-dev"), ("-region", "-config")] {
            let parsed = inv(&[flag, value, "-dev=false"], &Env::default());
            assert_eq!(parsed.base.region.as_deref(), Some(value));
            assert_eq!(parsed.base.dev, Some(false));
        }
        let parsed = inv(
            &["--turn.domain", "-dev", "--region=-config"],
            &Env::default(),
        );
        assert_eq!(parsed.base.region.as_deref(), Some("-config"));
        let (_, value) = parsed
            .generated
            .iter()
            .find(|(spec, _)| spec.path == "turn.domain")
            .expect("generated string flag");
        assert_eq!(value, &Value::Str("-dev".into()));
    }

    #[test]
    fn official_string_value_cannot_be_stolen_by_extension_parser() {
        let parsed = inv(
            &["--region", "--livecloud-https-port=9999", "--dev"],
            &Env::default(),
        );
        assert_eq!(
            parsed.base.region.as_deref(),
            Some("--livecloud-https-port=9999")
        );
        assert!(parsed.base.https_overrides.is_empty());
        let parsed = inv(
            &["--region", "--livecloud-unknown", "--dev"],
            &Env::default(),
        );
        assert_eq!(parsed.base.region.as_deref(), Some("--livecloud-unknown"));
    }

    #[test]
    fn subcommand_string_values_are_preserved_and_boolean_arity_unchanged() {
        let parsed = inv(
            &[
                "create-join-token",
                "-room",
                "-dev",
                "--identity",
                "-config",
                "--recorder=false",
            ],
            &Env::default(),
        );
        assert_eq!(
            parsed.command,
            Command::CreateJoinToken {
                room: "-dev".into(),
                identity: "-config".into(),
                recorder: false,
            }
        );
        let parsed = inv(&["-dev", "-region", "literal"], &Env::default());
        assert_eq!(parsed.base.dev, Some(true));
        assert_eq!(parsed.base.region.as_deref(), Some("literal"));
    }

    #[test]
    fn value_delimiter_and_non_utf8_are_kept_verbatim() {
        assert_eq!(
            normalize_args(args(&["-region", "--", "-dev"])),
            args(&["--region", "--", "--dev"])
        );
        assert_eq!(normalize_args(args(&["--", "-dev"])), args(&["--", "-dev"]));
        let (remaining, extensions) =
            extract_extension_flags(args(&["--region", "--", "--livecloud-https-port", "8443"]))
                .unwrap();
        assert_eq!(remaining, args(&["--region", "--"]));
        assert_eq!(extensions["port"], "8443");
        #[cfg(unix)]
        {
            use std::os::unix::ffi::OsStringExt;
            let value = OsString::from_vec(vec![b'-', 0xff]);
            assert_eq!(
                normalize_args(vec!["livecloud".into(), "-region".into(), value.clone()]),
                vec!["livecloud".into(), "--region".into(), value]
            );
        }
    }

    #[test]
    fn native_https_cli_overrides_its_namespace_env_only() {
        let env = Env::from_pairs([
            ("LIVECLOUD_HTTPS_PORT", "9443"),
            ("LIVECLOUD_HTTPS_CERT_FILE", "env.pem"),
        ]);
        let inv = match parse_args(
            args(&[
                "--livecloud-https-port=8443",
                "--livecloud-https-cert-file",
                "cli.pem",
            ]),
            &env,
            &info(),
        )
        .unwrap()
        {
            Parsed::Invocation(inv) => inv,
            _ => panic!("expected invocation"),
        };
        assert_eq!(inv.base.https_overrides["port"], "8443");
        assert_eq!(inv.base.https_overrides["cert_file"], "cli.pem");
        assert!(extract_extension_flags(args(&["--livecloud-https-bogus=1"])).is_err());
        assert!(
            extension_env(&Env::from_pairs([("LIVECLOUD_HTTPS_BOGUS", "1")]))
                .unwrap()
                .is_empty()
        );
    }

    #[test]
    fn ambient_extension_env_never_selects_the_legacy_command_graph() {
        for ambient in [
            ("LIVECLOUD_X", "ambient"),
            ("LIVECLOUD_HTTPS_BOGUS", "ambient"),
            ("LIVECLOUD_HTTPS_PORT", "9443"),
            ("UNRELATED_SETTING", "ambient"),
        ] {
            for argv in [
                vec!["--", "ports", "--help"],
                vec!["ports", "help", "ports"],
                vec!["create-join-token", "help", "--help"],
                vec!["help", "--dev=false", "ports"],
                vec!["ports", "--help=false", "--unknown"],
                vec!["--region", "--livecloud-https-port", "ports", "--help"],
            ] {
                let plain = early(&argv, &Env::default());
                let extended = early(&argv, &Env::from_pairs([ambient]));
                assert_eq!(
                    format!("{plain:?}"),
                    format!("{extended:?}"),
                    "{argv:?} {ambient:?}"
                );
            }
        }
    }

    #[test]
    fn registered_extension_env_projects_after_real_nested_dispatch() {
        let env = Env::from_pairs([
            ("LIVECLOUD_HTTPS_PORT", "9443"),
            ("LIVECLOUD_HTTPS_CERT_FILE", "env.pem"),
            ("LIVECLOUD_HTTPS_FUTURE", "ignored"),
        ]);
        let ports = inv(&["--", "ports"], &env);
        assert_eq!(ports.command, Command::Ports);
        assert!(ports.base.https_overrides.is_empty());
        let invocation = inv(&[], &env);
        assert_eq!(invocation.command, Command::Run);
        assert_eq!(invocation.base.https_overrides.len(), 2);
        assert_eq!(invocation.base.https_overrides["port"], "9443");
        assert_eq!(invocation.base.https_overrides["cert_file"], "env.pem");
        let keys = inv(&["--", "generate-keys"], &env);
        assert_eq!(keys.command, Command::GenerateKeys);
        assert!(keys.base.https_overrides.is_empty());
    }

    #[test]
    fn extension_env_cannot_mask_official_post_parse_env_errors() {
        let plain = Env::from_pairs([("LIVEKIT_PORT", "bad")]);
        let extended = Env::from_pairs([
            ("LIVEKIT_PORT", "bad"),
            ("LIVECLOUD_HTTPS_PORT", "9443"),
            ("LIVECLOUD_X", "ignored"),
        ]);
        let plain = parse_args(args(&["--", "ports", "help"]), &plain, &info())
            .err()
            .expect("actual official ENV parse failure");
        let extended = parse_args(args(&["--", "ports", "help"]), &extended, &info())
            .err()
            .expect("same actual official ENV parse failure");
        assert_eq!(plain.to_string(), extended.to_string());
    }
}

#[cfg(test)]
mod source_command_stages {
    use super::*;
    fn info() -> ProgramInfo {
        ProgramInfo {
            binary_name: "livecloud".into(),
            product_version: "0.1.0".into(),
            compat_version: "1.13.10",
        }
    }
    fn parse(args: &[&str], env: &Env) -> Result<Parsed, ConfigError> {
        super::parse_args(
            std::iter::once(OsString::from("livecloud"))
                .chain(args.iter().map(|s| OsString::from(*s)))
                .collect(),
            env,
            &info(),
        )
    }
    fn help(args: &[&str], env: &Env) -> (String, bool, i32) {
        match parse(args, env).unwrap() {
            Parsed::Early(EntryAction::PrintHelp {
                text,
                to_stderr,
                exit_code,
            }) => (text, to_stderr, exit_code),
            _ => panic!("expected source early help"),
        }
    }
    #[test]
    fn root_help_and_version_do_not_parse_child() {
        let (text, stderr, code) = help(&["--help", "ports", "--dev=invalid"], &Env::default());
        assert_eq!(text, crate::help::command_help(&info(), "ports"));
        assert!(!stderr);
        assert_eq!(code, 0);
        assert!(matches!(
            parse(&["--version", "ports", "--dev=invalid"], &Env::default()).unwrap(),
            Parsed::Early(EntryAction::PrintVersion(_))
        ));
        let (text, stderr, code) = help(&["--dev=invalid", "ports", "--help"], &Env::default());
        assert!(stderr);
        assert_eq!(code, 1);
        assert!(text.ends_with(&crate::help::root_help(&info(), false)));
    }
    #[test]
    fn root_env_postparse_precedes_child_help_but_not_root_help() {
        let env = Env::from_pairs([("LIVEKIT_PORT", "invalid")]);
        for args in [
            &["ports", "--help"][..],
            &["help-verbose"][..],
            &["ports", "--port=7880", "--help"][..],
        ] {
            assert!(parse(args, &env).is_err());
        }
        assert_eq!(help(&["--help"], &env).2, 0);
        assert!(matches!(
            parse(&["--version"], &env).unwrap(),
            Parsed::Early(EntryAction::PrintVersion(_))
        ));
        assert_eq!(
            help(&["--port=7880", "ports", "--help"], &env).0,
            crate::help::command_help(&info(), "ports")
        );
    }
    #[test]
    fn explicit_tokens_stop_at_the_first_parse_error() {
        for args in [
            &["--help", "--unknown"][..],
            &["--help", "--config"][..],
            &["ports", "--help", "--unknown"][..],
        ] {
            let (_, stderr, code) = help(args, &Env::default());
            assert!(!stderr);
            assert_eq!(code, 0);
        }
        let (text, stderr, code) = help(&["--dev=invalid", "--help", "--unknown"], &Env::default());
        assert!(stderr);
        assert_eq!(code, 1);
        assert!(text.starts_with("Incorrect Usage: invalid value \"invalid\" for flag -dev:"));
    }
    #[test]
    fn builtin_bool_values_and_repetition_are_real_flags() {
        assert_eq!(help(&["--help", "--help"], &Env::default()).2, 0);
        assert!(matches!(
            parse(&["--version", "--version"], &Env::default()).unwrap(),
            Parsed::Early(EntryAction::PrintVersion(_))
        ));
        let (_, stderr, code) = help(&["--help", "--help=false", "--unknown"], &Env::default());
        assert!(stderr);
        assert_eq!(code, 1);
        let (text, _, _) = help(&["-h=invalid"], &Env::default());
        assert!(text.starts_with("Incorrect Usage: invalid value \"invalid\" for flag -h:"));
        let (text, _, _) = help(&["--unknown=x", "--help"], &Env::default());
        assert!(text.starts_with("Incorrect Usage: flag provided but not defined: -unknown\n"));
    }
    #[test]
    fn flag_like_values_and_unknown_first_positional_do_not_become_children() {
        let result = parse(&["--config", "ports", "--help"], &Env::default()).unwrap();
        assert!(matches!(
            result,
            Parsed::Early(EntryAction::PrintHelp { exit_code: 0, .. })
        ));
        let Parsed::Invocation(inv) = parse(
            &["unknown-first", "ports", "--dev=invalid"],
            &Env::default(),
        )
        .unwrap() else {
            panic!("source root action")
        };
        assert_eq!(inv.command, Command::Run);
        assert_eq!(inv.base.dev, None);
    }
}

#[cfg(test)]
mod final_help_boundaries {
    use super::*;
    fn action(args: &[&str]) -> EntryAction {
        let info = ProgramInfo {
            binary_name: "livecloud".into(),
            product_version: "0.1.0".into(),
            compat_version: "1.13.10",
        };
        match parse_args(
            std::iter::once(OsString::from("livecloud"))
                .chain(args.iter().map(|s| OsString::from(*s)))
                .collect(),
            &Env::default(),
            &info,
        )
        .unwrap()
        {
            Parsed::Early(action) => action,
            _ => panic!("expected early output"),
        }
    }
    #[test]
    fn builtin_help_has_first_topic_and_no_own_help_flag() {
        let EntryAction::PrintHelp { text, .. } = action(&["help", "ports", "list-nodes"]) else {
            panic!()
        };
        assert!(text.starts_with("NAME:\n   livecloud ports -"));
        for args in [&["help", "--help"][..], &["h", "-h"][..]] {
            let EntryAction::PrintHelp {
                text,
                to_stderr,
                exit_code,
            } = action(args)
            else {
                panic!()
            };
            assert!(to_stderr);
            assert_eq!(exit_code, 1);
            assert!(!text.contains("NAME:"));
        }
        let EntryAction::PrintHelp {
            text, exit_code, ..
        } = action(&["help", "h"])
        else {
            panic!()
        };
        assert_eq!(exit_code, 0);
        assert!(text.contains("livecloud help [command]"));
        assert!(!text.contains("OPTIONS:\n   --help"));
    }
    #[test]
    fn lone_dash_stops_later_flags_in_root_and_child() {
        let info = ProgramInfo {
            binary_name: "livecloud".into(),
            product_version: "0.1.0".into(),
            compat_version: "1.13.10",
        };
        for args in [&["-", "--help"][..], &["ports", "-", "--help"][..]] {
            assert!(matches!(
                parse_args(
                    std::iter::once(OsString::from("livecloud"))
                        .chain(args.iter().map(|s| OsString::from(*s)))
                        .collect(),
                    &Env::default(),
                    &info
                )
                .unwrap(),
                Parsed::Invocation(_)
            ));
        }
    }
}
