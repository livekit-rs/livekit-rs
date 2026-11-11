//! Help text in urfave/cli v3's layout (`CLI/template.go`, `CLI/docs.go`):
//! `NAME` / `USAGE` / `VERSION` / `DESCRIPTION` / `COMMANDS` / `GLOBAL OPTIONS`,
//! flag lines `--name value<TAB>usage (default: X) [$ENV]` aligned like Go's
//! tabwriter (padding 2). `help-verbose` additionally lists every generated flag
//! (usage `generated`); upstream prints those in random map order, we print them
//! in tree order.

use crate::ProgramInfo;
use crate::cli::{BASE_FLAGS, BaseKind, HELP_COMMAND_USAGE, SUBCOMMANDS};
use crate::generated::{Kind, specs};

const USAGE: &str = "High performance WebRTC server";
const DESCRIPTION: &str = "run without subcommands to start the server";

/// `--version` line, byte-identical to upstream urfave/cli: `{name} version {ver}`.
pub(crate) fn version_line(info: &ProgramInfo) -> String {
    format!("{} version {}", info.binary_name, info.product_version)
}

/// urfave `unquoteUsage`: a back-quoted word is the value placeholder.
fn unquote_usage(usage: &str) -> (Option<String>, String) {
    if let Some(start) = usage.find('`')
        && let Some(len) = usage[start + 1..].find('`')
    {
        let name = &usage[start + 1..start + 1 + len];
        let text = format!("{}{}{}", &usage[..start], name, &usage[start + 2 + len..]);
        return (Some(name.to_string()), text);
    }
    (None, usage.to_string())
}

struct FlagLine {
    left: String,
    right: String,
}

fn flag_line(
    name: &str,
    short: Option<char>,
    takes_value: bool,
    multi: bool,
    usage: &str,
    default_text: &str,
    env: Option<&str>,
) -> FlagLine {
    let (placeholder, usage) = unquote_usage(usage);
    let placeholder = if takes_value {
        Some(placeholder.unwrap_or_else(|| "string".into()))
    } else {
        None
    };
    let one = |n: &str| {
        let dashes = if n.len() == 1 { "-" } else { "--" };
        match &placeholder {
            Some(p) => format!("{dashes}{n} {p}"),
            None => format!("{dashes}{n}"),
        }
    };
    let mut left = one(name);
    if let Some(s) = short {
        left = format!("{left}, {}", one(&s.to_string()));
    }
    if multi {
        left = format!("{left} [ {left} ]");
    }
    let mut right = usage;
    if !default_text.is_empty() {
        right = format!("{right} (default: {default_text})");
    }
    let mut right = right.trim().to_string();
    if let Some(e) = env {
        right = format!("{right} [${e}]");
    }
    FlagLine { left, right }
}

fn render_columns(out: &mut String, rows: &[FlagLine]) {
    let width = rows
        .iter()
        .map(|r| r.left.chars().count())
        .max()
        .unwrap_or(0)
        + 2;
    for r in rows {
        let pad = width - r.left.chars().count();
        out.push_str("\n   ");
        out.push_str(&r.left);
        out.push_str(&" ".repeat(pad));
        out.push_str(&r.right);
    }
}

fn global_flag_rows(verbose: bool, include_version: bool) -> Vec<FlagLine> {
    let mut rows = Vec::new();
    for f in BASE_FLAGS.iter().filter(|f| !f.hidden) {
        let default = "";
        rows.push(flag_line(
            f.name,
            None,
            f.kind != BaseKind::Bool,
            f.kind == BaseKind::StrSlice,
            f.usage,
            default,
            f.env,
        ));
    }
    if verbose {
        for s in specs() {
            // urfave help uses flag display names, not strconv ENV type names.
            // An unset *int DefaultText is empty, so IntFlag displays its zero.
            let display_default = if s.kind == Kind::Int && s.default_text.is_empty() {
                "0"
            } else {
                &s.default_text
            };
            let mut row = flag_line(
                &s.path,
                None,
                s.kind.takes_value(),
                false,
                "generated",
                display_default,
                Some(s.env.as_str()),
            );
            if s.kind.takes_value() {
                let display_kind = match s.kind {
                    Kind::Float => "float",
                    Kind::Uint64 => "uint",
                    _ => s.kind.go_name(),
                };
                row.left = row.left.replace(" string", &format!(" {display_kind}"));
            }
            rows.push(row);
        }
    }
    // commands.go helpVerbose replaces Flags with base+generated only;
    // unlike initialized root help, ShowAppHelp does not inject help/version.
    if verbose {
        return rows;
    }
    rows.push(flag_line(
        "help",
        Some('h'),
        false,
        false,
        "show help",
        "",
        None,
    ));
    if include_version {
        rows.push(flag_line(
            "version",
            Some('v'),
            false,
            false,
            "print the version",
            "",
            None,
        ));
    }
    rows
}

/// Root help (`--help`, `help`, `help-verbose` with `verbose`).
pub(crate) fn root_help(info: &ProgramInfo, verbose: bool) -> String {
    let name = &info.binary_name;
    let mut out = String::new();
    out.push_str(&format!("NAME:\n   {name} - {USAGE}\n\n"));
    out.push_str(&format!(
        "USAGE:\n   {name} [global options] [command [command options]]\n\n"
    ));
    out.push_str(&format!("VERSION:\n   {}\n\n", info.product_version));
    out.push_str(&format!("DESCRIPTION:\n   {DESCRIPTION}\n\n"));
    out.push_str("COMMANDS:");
    let mut cmds: Vec<(String, &str)> = SUBCOMMANDS
        .iter()
        .filter(|c| !c.hidden)
        .map(|c| (c.name.to_string(), c.usage))
        .collect();
    cmds.push(("help, h".to_string(), HELP_COMMAND_USAGE));
    let width = cmds.iter().map(|(n, _)| n.len()).max().unwrap_or(0) + 2;
    for (n, u) in &cmds {
        out.push_str(&format!("\n   {n}{}{u}", " ".repeat(width - n.len())));
    }
    out.push_str("\n\nGLOBAL OPTIONS:");
    render_columns(&mut out, &global_flag_rows(verbose, true));
    out.push('\n');
    out
}

/// Subcommand help (`<cmd> --help`, `help <cmd>`).
pub(crate) fn command_help(info: &ProgramInfo, command: &str) -> String {
    let name = &info.binary_name;
    if command == "help" {
        let mut out = format!(
            "NAME:\n   {name} help - {HELP_COMMAND_USAGE}\n\nUSAGE:\n   {name} help [command]\n\nGLOBAL OPTIONS:"
        );
        let globals: Vec<FlagLine> = global_flag_rows(false, false)
            .into_iter()
            .filter(|r| !r.left.starts_with("--help"))
            .collect();
        render_columns(&mut out, &globals);
        out.push('\n');
        return out;
    }
    let usage = SUBCOMMANDS
        .iter()
        .find(|c| c.name == command)
        .map(|c| c.usage)
        .unwrap_or(HELP_COMMAND_USAGE);
    let mut out = String::new();
    out.push_str(&format!("NAME:\n   {name} {command} - {usage}\n\n"));
    out.push_str(&format!("USAGE:\n   {name} {command} [options]\n\n"));
    out.push_str("OPTIONS:");
    let mut own = Vec::new();
    if command == "create-join-token" {
        own.push(flag_line(
            "room",
            None,
            true,
            false,
            "name of room to join",
            "",
            None,
        ));
        own.push(flag_line(
            "identity",
            None,
            true,
            false,
            "identity of participant that holds the token",
            "",
            None,
        ));
        own.push(flag_line(
            "recorder",
            None,
            false,
            false,
            "creates a hidden participant that can only subscribe",
            "",
            None,
        ));
    }
    own.push(flag_line(
        "help",
        Some('h'),
        false,
        false,
        "show help",
        "",
        None,
    ));
    render_columns(&mut out, &own);
    out.push_str("\n\nGLOBAL OPTIONS:");
    let globals: Vec<FlagLine> = global_flag_rows(false, false)
        .into_iter()
        .filter(|r| !r.left.starts_with("--help"))
        .collect();
    render_columns(&mut out, &globals);
    out.push('\n');
    out
}

/// Built-in help command below an ordinary command (ShowCommandHelp on its
/// locally registered `help`/`h` topic); it has no local flags.
pub(crate) fn nested_builtin_help(info: &ProgramInfo, parent: &str) -> String {
    let name = &info.binary_name;
    let mut out = format!(
        "NAME:\n   {name} {parent} help - {HELP_COMMAND_USAGE}\n\nUSAGE:\n   {name} {parent} help [command]\n\nGLOBAL OPTIONS:"
    );
    let globals: Vec<FlagLine> = global_flag_rows(false, false)
        .into_iter()
        .filter(|r| !r.left.starts_with("--help"))
        .collect();
    render_columns(&mut out, &globals);
    out.push('\n');
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn placeholder_from_backticks() {
        let l = flag_line(
            "cpuprofile",
            None,
            true,
            false,
            "write CPU profile to `file`",
            "",
            None,
        );
        assert_eq!(l.left, "--cpuprofile file");
        assert_eq!(l.right, "write CPU profile to file");
        let l = flag_line("bind", None, true, true, "x", "", None);
        assert_eq!(l.left, "--bind string [ --bind string ]");
        let l = flag_line(
            "keys",
            None,
            true,
            false,
            "api keys",
            "",
            Some("LIVEKIT_KEYS"),
        );
        assert_eq!(l.right, "api keys [$LIVEKIT_KEYS]");
    }

    #[test]
    fn verbose_generated_rows_match_actual_fixed_go_cohort() {
        let info = ProgramInfo {
            binary_name: "livekit-server".into(),
            product_version: "0.1.0".into(),
            compat_version: "1.13.10",
        };
        let rendered = root_help(&info, true);
        let mut actual: Vec<&str> = rendered
            .lines()
            .filter(|line| line.contains("  generated"))
            .collect();
        actual.sort_unstable();
        let expected: Vec<&str> =
            include_str!("../tests/fixtures/cli-verbose-generated-v1.13.10.txt")
                .lines()
                .collect();
        // Only upstream's map-dependent generated-row order is normalized.
        // Full row bytes, default values, multiplicity and column padding match.
        assert_eq!(actual, expected);
        assert_eq!(Kind::Float.go_name(), "float64");
        assert_eq!(Kind::Uint64.go_name(), "uint64");
    }

    #[test]
    fn hidden_flags_not_listed() {
        let info = ProgramInfo {
            binary_name: "livekit-server".into(),
            product_version: "0.1.0".into(),
            compat_version: "1.13.10",
        };
        let h = root_help(&info, false);
        assert!(h.starts_with("NAME:\n   livekit-server - High performance WebRTC server"));
        assert!(!h.contains("disable-strict-config"));
        assert!(!h.contains("create-join-token"));
        assert!(h.contains("--dev  ") || h.contains("--dev "));
        let v = root_help(&info, true);
        assert!(v.contains("--development"));
        assert!(v.contains("generated (default: 7881) [$LIVEKIT_RTC_TCP_PORT]"));
    }
}
