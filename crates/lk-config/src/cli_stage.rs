// Source-derived control-flow port from urfave/cli v3.11.0.
// Copyright (c) 2023 urfave/cli maintainers. MIT license:
// repository LICENSES/urfave-cli-MIT.txt. Pinned primary files in SOURCE.json.
//! Source-shaped control flow for the finite fixed command graph. Clap stores
//! canonical already-validated flag values; it does not choose command stages.
use super::*;
use crate::strconv::trim_space_bytes;

#[derive(Clone, Copy)]
enum Context {
    Root,
    Ordinary(&'static str),
    Help(Option<&'static str>),
}
impl Context {
    fn child(self, name: &str) -> Option<Self> {
        match self {
            Self::Root => {
                if matches!(name, "help" | "h") {
                    Some(Self::Help(None))
                } else {
                    SUBCOMMANDS
                        .iter()
                        .find(|c| c.name == name)
                        .map(|c| Self::Ordinary(c.name))
                }
            }
            Self::Ordinary(parent) => {
                matches!(name, "help" | "h").then_some(Self::Help(Some(parent)))
            }
            Self::Help(_) => None,
        }
    }
    fn usage(self) -> Option<&'static str> {
        match self {
            Self::Root => None,
            Self::Ordinary(name) => Some(name),
            Self::Help(_) => Some("help"),
        }
    }
    fn owns_token_flags(self) -> bool {
        matches!(
            self,
            Self::Ordinary("create-join-token") | Self::Help(Some("create-join-token"))
        )
    }
    fn has_help_flag(self) -> bool {
        !matches!(self, Self::Help(_))
    }
    fn successful_help(self, info: &ProgramInfo, topic: Option<&std::ffi::OsStr>) -> EntryAction {
        if let Some(raw) = topic.filter(|topic| topic.to_str().is_none()) {
            let mut text = b"No help topic for '".to_vec();
            text.extend_from_slice(raw.as_encoded_bytes());
            text.extend_from_slice(b"'\n");
            let builtin = matches!(self, Self::Help(_));
            return EntryAction::PrintBytes {
                stdout: if builtin { Vec::new() } else { text.clone() },
                stderr: if builtin { text } else { Vec::new() },
                exit_code: if builtin { 3 } else { 1 },
            };
        }
        let topic = topic.and_then(std::ffi::OsStr::to_str);
        match self {
            Self::Root => help_action(info, None, topic, false),
            Self::Ordinary(name) => help_action(info, Some(name), topic, false),
            Self::Help(parent) => help_action(info, parent, topic, true),
        }
    }
}
/// Go strings.TrimSpace's exact Unicode White_Space set, independent of the
/// compiler's current Unicode version. Empty trimmed positional args retain raw.
fn trim_space(value: &str) -> &str {
    value.trim_matches(|c| matches!(c,'\u{0009}'..='\u{000d}'|'\u{0020}'|'\u{0085}'|'\u{00a0}'|'\u{1680}'|'\u{2000}'..='\u{200a}'|'\u{2028}'|'\u{2029}'|'\u{202f}'|'\u{205f}'|'\u{3000}'))
}
/// command_parse.go tests unicode.IsLetter(rune(firstArg[1])): first BYTE,
/// not first decoded Unicode rune. Port that Latin-1 predicate explicitly.
fn letter_byte(value: u8) -> bool {
    matches!(value,b'A'..=b'Z'|b'a'..=b'z'|0xaa|0xb5|0xba|0xc0..=0xd6|0xd8..=0xf6|0xf8..=0xff)
}
struct Stage {
    args: Vec<OsString>,
    flags: Vec<(String, OsString)>,
    help: bool,
    version: bool,
}
fn kind(ctx: Context, name: &str) -> Option<crate::generated::Kind> {
    if (name == "help" && ctx.has_help_flag())
        || (name == "version" && matches!(ctx, Context::Root))
    {
        return Some(crate::generated::Kind::Bool);
    }
    BASE_FLAGS
        .iter()
        .find(|f| f.name == name)
        .map(|f| {
            if f.kind == BaseKind::Bool {
                crate::generated::Kind::Bool
            } else {
                crate::generated::Kind::Str
            }
        })
        .or_else(|| specs().iter().find(|s| s.path == name).map(|s| s.kind))
        .or_else(|| {
            (ctx.owns_token_flags() && matches!(name, "room" | "identity"))
                .then_some(crate::generated::Kind::Str)
        })
        .or_else(|| {
            (ctx.owns_token_flags() && name == "recorder").then_some(crate::generated::Kind::Bool)
        })
}
fn stage_error(info: &ProgramInfo, ctx: Context, help: bool, message: &str) -> EntryAction {
    if help {
        stage_help(info, ctx.usage())
    } else {
        flag_usage_error(info, message, ctx.usage())
    }
}
fn os_value(raw: &[u8], ctx: Context, info: &ProgramInfo) -> Result<OsString, EntryAction> {
    #[cfg(unix)]
    {
        use std::os::unix::ffi::OsStringExt;
        let _ = (ctx, info);
        Ok(OsString::from_vec(raw.to_vec()))
    }
    #[cfg(not(unix))]
    {
        std::str::from_utf8(raw).map(OsString::from).map_err(|_| {
            stage_error(
                info,
                ctx,
                false,
                "non-Unicode byte argv requires POSIX; Windows wide-string profile remains OPEN",
            )
        })
    }
}
fn error_bytes(info: &ProgramInfo, ctx: Context, help: bool, message: &[u8]) -> EntryAction {
    if let Ok(message) = std::str::from_utf8(message) {
        stage_error(info, ctx, help, message)
    } else if help {
        stage_help(info, ctx.usage())
    } else {
        os_input::usage(info, message, ctx.usage())
    }
}
fn scan(input: &[OsString], ctx: Context, info: &ProgramInfo) -> Result<Stage, EntryAction> {
    let mut output = Stage {
        args: Vec::new(),
        flags: Vec::new(),
        help: false,
        version: false,
    };
    let mut at = 0;
    while at < input.len() {
        let original = input[at].as_encoded_bytes();
        let first = input[at]
            .to_str()
            .map(trim_space)
            .map(str::as_bytes)
            .unwrap_or_else(|| trim_space_bytes(original));
        if first.is_empty() {
            output.args.push(input[at].clone());
            at += 1;
            continue;
        }
        if first == b"--" {
            output.args.extend_from_slice(&input[at + 1..]);
            break;
        }
        if !first.starts_with(b"-") {
            if std::str::from_utf8(first)
                .ok()
                .and_then(|name| ctx.child(name))
                .is_some()
            {
                output.args.extend_from_slice(&input[at..]);
                break;
            }
            output.args.push(os_value(first, ctx, info)?);
            at += 1;
            continue;
        }
        if first == b"-" {
            output.args.push("-".into());
            break;
        }
        if !first.starts_with(b"--") && !letter_byte(first[1]) {
            output.args.extend_from_slice(&input[at..]);
            break;
        }
        let spelling = if first.starts_with(b"--") {
            &first[2..]
        } else {
            &first[1..]
        };
        let (written, inline) = match spelling.iter().position(|b| *b == b'=') {
            Some(n) => (&spelling[..n], Some(&spelling[n + 1..])),
            None => (spelling, None),
        };
        let canonical = std::str::from_utf8(written).ok().map(|name| match name {
            "h" => "help",
            "v" => "version",
            name => name,
        });
        let Some((name, kind)) =
            canonical.and_then(|name| kind(ctx, name).map(|kind| (name, kind)))
        else {
            let mut message = b"flag provided but not defined: -".to_vec();
            message.extend_from_slice(written);
            return Err(error_bytes(info, ctx, output.help, &message));
        };
        let value = if kind == crate::generated::Kind::Bool {
            inline.filter(|v| !v.is_empty()).unwrap_or(b"true")
        } else if let Some(value) = inline {
            value
        } else {
            at += 1;
            let Some(value) = input.get(at) else {
                let mut message = b"flag needs an argument: ".to_vec();
                message.extend_from_slice(first);
                return Err(error_bytes(info, ctx, output.help, &message));
            };
            value.as_encoded_bytes()
        };
        let parsed = if kind == crate::generated::Kind::Str {
            Ok(None)
        } else {
            match std::str::from_utf8(value) {
                Ok(value) if kind == crate::generated::Kind::Bool => parse_cli_bool(value)
                    .map(Value::Bool)
                    .map(Some)
                    .map_err(|_| "parse error".to_owned()),
                Ok(value) => parse_value(kind, value).map(Some),
                Err(_) => Err(os_input::invalid_value(kind, value)),
            }
        };
        match parsed {
            Ok(Some(Value::Bool(value))) if name == "help" => output.help = value,
            Ok(Some(Value::Bool(value))) if name == "version" => output.version = value,
            Ok(_) => {}
            Err(error) => {
                let mut message =
                    format!("invalid value {} for flag -", quote::quote_bytes(value)).into_bytes();
                message.extend_from_slice(written);
                message.extend_from_slice(format!(": {error}").as_bytes());
                return Err(error_bytes(info, ctx, output.help, &message));
            }
        }
        if !matches!(name, "help" | "version") {
            output
                .flags
                .push((name.to_owned(), os_value(value, ctx, info)?));
        }
        at += 1;
    }
    Ok(output)
}
type CollectedStage = (BaseValues, Vec<(&'static LeafSpec, Value)>);

fn collect(
    flags: &[(String, OsString)],
    ctx: Context,
    env: &Env,
    info: &ProgramInfo,
) -> Result<CollectedStage, ConfigError> {
    let mut command = clap::Command::new(ROOT_ID)
        .disable_help_flag(true)
        .disable_version_flag(true)
        .disable_help_subcommand(true);
    for flag in BASE_FLAGS {
        command = command.arg(if flag.kind == BaseKind::Bool {
            bool_arg(flag.name, false)
        } else {
            value_arg(flag.name, false)
        });
    }
    for spec in specs() {
        let id: &'static str = spec.path.as_str();
        command = command.arg(if spec.kind.takes_value() {
            value_arg(id, false)
        } else {
            bool_arg(id, false)
        });
    }
    if ctx.owns_token_flags() {
        command = command
            .arg(value_arg("room", false))
            .arg(value_arg("identity", false))
            .arg(bool_arg("recorder", false));
    }
    let mut argv = vec![OsString::from(ROOT_ID)];
    for (name, value) in flags {
        let mut arg = OsString::from(format!("--{name}="));
        arg.push(value);
        argv.push(arg);
    }
    let matches = command
        .try_get_matches_from(argv)
        .map_err(|e| ConfigError::msg(e.to_string()))?;
    let base = base_values(&matches, env, ctx.usage(), info)
        .map_err(|_| ConfigError::msg("validated base flag failed during collection"))?;
    let generated =
        generated_values(&matches, env, ctx.usage(), info).map_err(|error| match error {
            GenError::Config(error) => error,
            GenError::Usage(_) => {
                ConfigError::msg("validated generated flag failed during collection")
            }
        })?;
    Ok((base, generated))
}
fn first(args: &[OsString]) -> Option<&str> {
    args.first().and_then(|value| value.to_str())
}
fn final_command(
    ctx: Context,
    flags: &[(String, OsString)],
    info: &ProgramInfo,
) -> Result<Command, EntryAction> {
    if ctx.owns_token_flags()
        && flags.iter().any(|(name, value)| {
            matches!(name.as_str(), "room" | "identity") && value.to_str().is_none()
        })
    {
        return Err(usage_error(
            info,
            "non-Unicode token claim projection remains OPEN",
            ctx.usage(),
        ));
    }
    let last = |name: &str| {
        flags
            .iter()
            .rev()
            .find(|(key, _)| key == name)
            .and_then(|(_, value)| value.to_str())
            .map(str::to_owned)
    };
    Ok(match ctx {
        Context::Root => Command::Run,
        Context::Ordinary("generate-keys") => Command::GenerateKeys,
        Context::Ordinary("ports") => Command::Ports,
        Context::Ordinary("list-nodes") => Command::ListNodes,
        Context::Ordinary("create-join-token") => {
            let room = last("room");
            let identity = last("identity");
            let missing: Vec<&str> = [("room", room.is_none()), ("identity", identity.is_none())]
                .into_iter()
                .filter_map(|(name, missing)| missing.then_some(name))
                .collect();
            if !missing.is_empty() {
                let message = if missing.len() == 1 {
                    format!("Required flag \"{}\" not set", missing[0])
                } else {
                    format!("Required flags \"{}\" not set", missing.join(", "))
                };
                return Err(usage_error(info, &message, Some("create-join-token")));
            }
            Command::CreateJoinToken {
                room: room.unwrap_or_default(),
                identity: identity.unwrap_or_default(),
                recorder: last("recorder")
                    .is_some_and(|value| parse_cli_bool(&value).unwrap_or(false)),
            }
        }
        _ => unreachable!("help actions resolved before final invocation"),
    })
}
pub(super) fn parse(
    args: Vec<OsString>,
    env: &Env,
    info: &ProgramInfo,
) -> Result<Parsed, ConfigError> {
    let mut ctx = Context::Root;
    let mut input = args.into_iter().skip(1).collect::<Vec<_>>();
    let mut flags = Vec::new();
    loop {
        let stage = match scan(&input, ctx, info) {
            Ok(stage) => stage,
            Err(action) => return Ok(Parsed::Early(action)),
        };
        if stage.help {
            return Ok(Parsed::Early(ctx.successful_help(
                info,
                stage.args.first().map(OsString::as_os_str),
            )));
        }
        if stage.version {
            return Ok(Parsed::Early(EntryAction::PrintVersion(
                help::version_line(info),
            )));
        }
        flags.extend(stage.flags);
        // PostParse ENV runs at EVERY successful stage before child dispatch.
        let (base, generated) = collect(&flags, ctx, env, info)?;
        if let Some(child) = first(&stage.args).and_then(|name| ctx.child(name)) {
            ctx = child;
            input = stage.args.into_iter().skip(1).collect();
            continue;
        }
        match ctx {
            Context::Help(_) => {
                return Ok(Parsed::Early(ctx.successful_help(
                    info,
                    stage.args.first().map(OsString::as_os_str),
                )));
            }
            Context::Ordinary("help-verbose") => {
                return Ok(Parsed::Early(EntryAction::PrintHelp {
                    text: help::root_help(info, true),
                    to_stderr: false,
                    exit_code: 0,
                }));
            }
            _ => {}
        }
        let command = match final_command(ctx, &flags, info) {
            Ok(command) => command,
            Err(action) => return Ok(Parsed::Early(action)),
        };
        return Ok(Parsed::Invocation(Invocation {
            command,
            base,
            generated,
        }));
    }
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
    fn run(args: &[&str], env: &Env) -> Result<Parsed, ConfigError> {
        parse(
            std::iter::once(OsString::from("livecloud"))
                .chain(args.iter().map(|s| OsString::from(*s)))
                .collect(),
            env,
            &info(),
        )
    }
    fn help(args: &[&str]) -> EntryAction {
        match run(args, &Env::default()).unwrap() {
            Parsed::Early(action) => action,
            _ => panic!("expected real help stage"),
        }
    }
    #[test]
    fn finite_command_graph_dispatches_after_separator_and_nested_help() {
        for command in SUBCOMMANDS {
            for alias in ["help", "h"] {
                let EntryAction::PrintHelp {
                    text,
                    to_stderr,
                    exit_code,
                } = help(&[command.name, alias])
                else {
                    panic!()
                };
                assert_eq!(exit_code, 0);
                assert!(!to_stderr);
                assert_eq!(text, help::command_help(&info(), command.name));
            }
        }
        let EntryAction::PrintHelp { text, .. } = help(&["--", "help", "ports"]) else {
            panic!()
        };
        assert_eq!(text, help::command_help(&info(), "ports"));
        let EntryAction::PrintHelp { text, .. } = help(&["--", "ports", "--help"]) else {
            panic!()
        };
        assert_eq!(text, help::command_help(&info(), "ports"));
        let EntryAction::PrintHelp {
            exit_code, text, ..
        } = help(&["ports", "help", "ports"])
        else {
            panic!()
        };
        assert_eq!(exit_code, 3);
        assert_eq!(text, "No help topic for 'ports'\n");
    }
    #[test]
    fn nested_builtin_hides_local_help_and_skips_token_required_flags() {
        let EntryAction::PrintHelp {
            exit_code,
            text,
            to_stderr,
        } = help(&["ports", "help", "--help"])
        else {
            panic!()
        };
        assert_eq!(exit_code, 1);
        assert!(to_stderr);
        assert!(!text.contains("NAME:"));
        let EntryAction::PrintHelp {
            text, exit_code, ..
        } = help(&["create-join-token", "help"])
        else {
            panic!()
        };
        assert_eq!(exit_code, 0);
        assert!(text.contains("--room"));
        let EntryAction::PrintHelp { text, .. } = help(&["ports", "help", "h"]) else {
            panic!()
        };
        assert_eq!(text, help::nested_builtin_help(&info(), "ports"));
    }
    #[test]
    fn trim_space_scanning_does_not_canonicalize_recognized_raw_dispatch_name() {
        let stage = scan(
            &[
                OsString::from("\u{00a0}ports\u{3000}"),
                OsString::from("--help"),
            ],
            Context::Root,
            &info(),
        )
        .unwrap_or_else(|_| panic!("valid source scan"));
        assert_eq!(stage.args[0], OsString::from("\u{00a0}ports\u{3000}"));
        assert!(!stage.help);
        assert!(
            Context::Root
                .child(stage.args[0].to_str().unwrap())
                .is_none()
        );
        let EntryAction::PrintHelp { text, .. } = help(&["\u{00a0}--help\u{3000}"]) else {
            panic!()
        };
        assert_eq!(text, help::root_help(&info(), false));
        let EntryAction::PrintHelp { text, .. } = help(&["help", "\u{00a0}ports\u{3000}"]) else {
            panic!()
        };
        assert_eq!(text, help::command_help(&info(), "ports"));
    }
    #[test]
    fn flag_values_are_verbatim_and_go_checks_first_utf8_byte_for_short_flag() {
        let value = "\u{00a0}untouched\u{3000}";
        let Parsed::Invocation(inv) = run(&["--region", value], &Env::default()).unwrap() else {
            panic!()
        };
        assert_eq!(inv.base.region.as_deref(), Some(value));
        assert!(letter_byte("😀".as_bytes()[0]));
        assert!(!letter_byte("א".as_bytes()[0]));
        let EntryAction::PrintHelp { exit_code, .. } = help(&["-😀"]) else {
            panic!()
        };
        assert_eq!(exit_code, 1);
    }
    #[test]
    fn explicit_empty_cli_bool_is_true_at_each_stage_but_environment_keeps_its_parser() {
        for (ctx, name) in [
            (Context::Root, "dev"),
            (Context::Root, "help"),
            (Context::Root, "version"),
            (Context::Ordinary("create-join-token"), "recorder"),
        ] {
            let stage = scan(&[OsString::from(format!("--{name}="))], ctx, &info())
                .unwrap_or_else(|_| panic!("empty CLI boolean must be true"));
            match name {
                "help" => assert!(stage.help),
                "version" => assert!(stage.version),
                _ => assert_eq!(stage.flags, vec![(name.to_owned(), OsString::from("true"))]),
            }
        }
        let boolean = specs()
            .iter()
            .find(|s| s.kind == crate::generated::Kind::Bool)
            .expect("actual generated bool");
        let stage = scan(
            &[OsString::from(format!("--{}=", boolean.path))],
            Context::Root,
            &info(),
        )
        .unwrap_or_else(|_| panic!("empty generated CLI boolean must be true"));
        assert_eq!(
            stage.flags,
            vec![(boolean.path.clone(), OsString::from("true"))]
        );
        assert_eq!(
            parse_env_value(crate::generated::Kind::Bool, "").unwrap(),
            Value::Bool(false)
        );
    }
    #[test]
    fn postparse_env_cannot_be_retroactively_overridden_by_child() {
        let env = Env::from_pairs([("LIVEKIT_PORT", "invalid")]);
        assert!(run(&["--", "ports", "--port=7880", "--help"], &env).is_err());
        assert!(matches!(
            run(&["--port=7880", "--", "ports", "help"], &env).unwrap(),
            Parsed::Early(EntryAction::PrintHelp { exit_code: 0, .. })
        ));
    }
    #[cfg(unix)]
    #[test]
    fn direct_alias_and_nested_builtin_help_preserve_first_raw_topic_and_exit_streams() {
        use std::os::unix::ffi::OsStringExt;
        for (prefix, builtin) in [
            (&["help"][..], true),
            (&["h"][..], true),
            (&["ports", "help"][..], true),
            (&["--", "help"][..], true),
            (&["--help"][..], false),
            (&["ports", "--help"][..], false),
        ] {
            let argv = std::iter::once(OsString::from("livekit-server"))
                .chain(prefix.iter().map(|value| OsString::from(*value)))
                .chain([OsString::from_vec(vec![255]), OsString::from("ports")])
                .collect();
            let result = parse(argv, &Env::default(), &info()).unwrap();
            let Parsed::Early(EntryAction::PrintBytes {
                stdout,
                stderr,
                exit_code,
            }) = result
            else {
                panic!("raw first topic must never filter into root help success")
            };
            let expected = b"No help topic for '\xff'\n";
            assert_eq!(exit_code, if builtin { 3 } else { 1 });
            assert_eq!(
                stdout,
                if builtin {
                    Vec::new()
                } else {
                    expected.to_vec()
                }
            );
            assert_eq!(
                stderr,
                if builtin {
                    expected.to_vec()
                } else {
                    Vec::new()
                }
            );
        }
    }
    #[cfg(unix)]
    #[test]
    fn byte_values_keep_nested_env_order_and_unicode_space_policy() {
        use std::os::unix::ffi::OsStringExt;
        let stage = scan(
            &[OsString::from_vec(
                b"\xc2\xa0--region=\xff\xe3\x80\x80".to_vec(),
            )],
            Context::Root,
            &info(),
        )
        .unwrap_or_else(|_| panic!("raw string value exists"));
        assert_eq!(stage.flags[0].1.as_encoded_bytes(), b"\xff");
        let env = Env::from_os_pairs([(
            OsString::from("LIVEKIT_PORT"),
            OsString::from_vec(vec![255]),
        )]);
        assert!(
            parse(
                vec!["livekit-server".into(), "ports".into(), "help".into()],
                &env,
                &info()
            )
            .is_err()
        );
    }
}
