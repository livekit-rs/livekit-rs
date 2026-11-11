//! Config differential observer, Rust side (SPEC-0073 M1 W1-G1/G2/G3).
//!
//! Test-only. Renders `LoadedConfig.official` into the same canonical
//! `{dotted.path: text}` map as the Go observer
//! (`tests/oracles/config-observer/go/config_observer_test.go`):
//! scalars as text (bool true/false, ints decimal, floats Go
//! `FormatFloat('g', -1, bits)`, durations `Duration.String()`, strings raw),
//! slices/maps as compact canonical JSON with sorted keys and every nested
//! scalar a JSON string, `None` as `"null"`. Field names are YAML keys
//! (`yaml_struct!` generates the [`Render`] impl next to `Walk`).
//!
//! Run (ignored by default):
//! `CONFIG_OBSERVER_CASES=cases.json CONFIG_OBSERVER_OUT=rust-results.json \
//!  cargo test -p lk-config --lib observer -- --ignored --nocapture`

use std::collections::BTreeMap;
use std::fmt::Write as _;

use crate::duration::GoDuration;
use crate::official::{KeyMap, Opaque, RoomAgentDispatches};
use crate::ports::{NodeIp, PortRange};

pub(crate) type Out = BTreeMap<String, String>;

/// Canonical JSON value (all scalars are strings).
#[derive(Clone, Debug, PartialEq)]
pub(crate) enum J {
    Null,
    Str(String),
    Arr(Vec<J>),
    Obj(BTreeMap<String, J>),
}

impl J {
    /// Go `encoding/json` with `SetEscapeHTML(false)`.
    pub(crate) fn text(&self) -> String {
        let mut s = String::new();
        self.write(&mut s);
        s
    }
    fn write(&self, s: &mut String) {
        match self {
            J::Null => s.push_str("null"),
            J::Str(v) => write_str(s, v),
            J::Arr(a) => {
                s.push('[');
                for (i, v) in a.iter().enumerate() {
                    if i > 0 {
                        s.push(',');
                    }
                    v.write(s);
                }
                s.push(']');
            }
            J::Obj(m) => {
                s.push('{');
                for (i, (k, v)) in m.iter().enumerate() {
                    if i > 0 {
                        s.push(',');
                    }
                    write_str(s, k);
                    s.push(':');
                    v.write(s);
                }
                s.push('}');
            }
        }
    }
    /// Drops null object members (protojson unset messages / oneof members).
    fn strip_nulls(self) -> J {
        match self {
            J::Obj(m) => J::Obj(
                m.into_iter()
                    .filter(|(_, v)| *v != J::Null)
                    .map(|(k, v)| (k, v.strip_nulls()))
                    .collect(),
            ),
            J::Arr(a) => J::Arr(a.into_iter().map(J::strip_nulls).collect()),
            v => v,
        }
    }
}

fn write_str(s: &mut String, v: &str) {
    s.push('"');
    for c in v.chars() {
        match c {
            '"' => s.push_str("\\\""),
            '\\' => s.push_str("\\\\"),
            '\n' => s.push_str("\\n"),
            '\r' => s.push_str("\\r"),
            '\t' => s.push_str("\\t"),
            '\u{8}' => s.push_str("\\b"),
            '\u{c}' => s.push_str("\\f"),
            '\u{2028}' => s.push_str("\\u2028"),
            '\u{2029}' => s.push_str("\\u2029"),
            c if (c as u32) < 0x20 => {
                let _ = write!(s, "\\u{:04x}", c as u32);
            }
            c => s.push(c),
        }
    }
    s.push('"');
}

/// Go `strconv.FormatFloat(v, 'g', -1, bits)` from Rust's shortest `{:e}` digits.
fn go_format_g(e: String, nan: bool, inf: Option<bool>) -> String {
    if nan {
        return "NaN".into();
    }
    if let Some(pos) = inf {
        return if pos { "+Inf".into() } else { "-Inf".into() };
    }
    let (mant, exp) = e.split_once('e').expect("LowerExp has an exponent");
    let exp: i64 = exp.parse().expect("exponent");
    let (neg, mant) = match mant.strip_prefix('-') {
        Some(m) => (true, m),
        None => (false, mant),
    };
    let digits: String = mant.chars().filter(|c| *c != '.').collect();
    let mut out = String::new();
    if neg {
        out.push('-');
    }
    if digits == "0" {
        out.push('0');
        return out;
    }
    let nd = digits.len() as i64;
    let dp = exp + 1;
    if !(-4..6).contains(&exp) {
        out.push_str(&digits[..1]);
        if nd > 1 {
            out.push('.');
            out.push_str(&digits[1..]);
        }
        out.push('e');
        out.push(if exp < 0 { '-' } else { '+' });
        let _ = write!(out, "{:02}", exp.abs());
        return out;
    }
    if dp <= 0 {
        out.push_str("0.");
        out.push_str(&"0".repeat((-dp) as usize));
        out.push_str(&digits);
    } else if dp >= nd {
        out.push_str(&digits);
        out.push_str(&"0".repeat((dp - nd) as usize));
    } else {
        out.push_str(&digits[..dp as usize]);
        out.push('.');
        out.push_str(&digits[dp as usize..]);
    }
    out
}

pub(crate) trait Render {
    fn json(&self) -> J;
    fn render(&self, path: &str, out: &mut Out) {
        let text = match self.json() {
            J::Str(s) => s,
            other => other.text(),
        };
        out.insert(path.to_string(), text);
    }
}

macro_rules! render_display {
    ($($t:ty),*) => {$(
        impl Render for $t {
            fn json(&self) -> J { J::Str(self.to_string()) }
        }
    )*};
}
render_display!(bool, String, u8, u16, u32, u64, i32, i64, GoDuration);

impl Render for f32 {
    fn json(&self) -> J {
        J::Str(go_format_g(
            format!("{self:e}"),
            self.is_nan(),
            self.is_infinite().then_some(*self > 0.0),
        ))
    }
}

impl Render for f64 {
    fn json(&self) -> J {
        J::Str(go_format_g(
            format!("{self:e}"),
            self.is_nan(),
            self.is_infinite().then_some(*self > 0.0),
        ))
    }
}

/// Go pointer: nil is `"null"`; a pointer to a struct is walked in place.
impl<T: Render> Render for Option<T> {
    fn json(&self) -> J {
        match self {
            None => J::Null,
            Some(v) => v.json(),
        }
    }
    fn render(&self, path: &str, out: &mut Out) {
        match self {
            None => {
                out.insert(path.to_string(), "null".into());
            }
            Some(v) => v.render(path, out),
        }
    }
}

impl<T: Render> Render for Vec<T> {
    fn json(&self) -> J {
        J::Arr(self.iter().map(Render::json).collect())
    }
}

impl<K: std::fmt::Display, V: Render> Render for BTreeMap<K, V> {
    fn json(&self) -> J {
        J::Obj(
            self.iter()
                .map(|(k, v)| (k.to_string(), v.json()))
                .collect(),
        )
    }
}

impl Render for KeyMap {
    fn json(&self) -> J {
        J::Obj(
            self.0
                .iter()
                .map(|(k, v)| (k.clone(), J::Str(v.clone())))
                .collect(),
        )
    }
}

impl Render for Opaque {
    fn json(&self) -> J {
        if self.present {
            J::Str("<opaque>".into())
        } else {
            J::Null
        }
    }
}

/// protojson cannot distinguish a nil repeated field from an empty one.
impl Render for RoomAgentDispatches {
    fn json(&self) -> J {
        match &self.0 {
            None => J::Arr(Vec::new()),
            Some(v) => v.json(),
        }
    }
}

impl Render for PortRange {
    fn json(&self) -> J {
        J::Obj(BTreeMap::from([
            ("start".to_string(), self.start.json()),
            ("end".to_string(), self.end.json()),
        ]))
    }
    fn render(&self, path: &str, out: &mut Out) {
        self.start
            .render(&crate::generated::join(path, "start"), out);
        self.end.render(&crate::generated::join(path, "end"), out);
    }
}

impl Render for NodeIp {
    fn json(&self) -> J {
        J::Obj(BTreeMap::from([
            ("ipv4".to_string(), self.v4.json()),
            ("ipv6".to_string(), self.v6.json()),
        ]))
    }
    fn render(&self, path: &str, out: &mut Out) {
        self.v4.render(&crate::generated::join(path, "ipv4"), out);
        self.v6.render(&crate::generated::join(path, "ipv6"), out);
    }
}

/// The Go type tree of the official config (`reflect.Type.String()` per
/// yaml path, `[]`/`{}`/`{key}` for slice/map elements and keys), compared
/// with the original's `go-types.json` so every yaml.v3 type error names the
/// exact upstream type.
pub(crate) trait TypeTree {
    fn types(_path: &str, _go: &str, _out: &mut Out) {}
}

macro_rules! type_leaf {
    ($($t:ty),*) => {$( impl TypeTree for $t {} )*};
}
type_leaf!(
    bool,
    String,
    u8,
    u16,
    u32,
    u64,
    i32,
    i64,
    f32,
    f64,
    GoDuration,
    Opaque,
    RoomAgentDispatches
);

impl<T: TypeTree> TypeTree for Option<T> {
    fn types(path: &str, go: &str, out: &mut Out) {
        T::types(path, go.strip_prefix('*').unwrap_or(go), out);
    }
}

impl<T: TypeTree> TypeTree for Vec<T> {
    fn types(path: &str, go: &str, out: &mut Out) {
        let elem = go.strip_prefix("[]").unwrap_or(go);
        let p = format!("{path}[]");
        out.insert(p.clone(), elem.to_string());
        T::types(&p, elem, out);
    }
}

impl<K, V: TypeTree> TypeTree for BTreeMap<K, V> {
    fn types(path: &str, go: &str, out: &mut Out) {
        let (k, v) = crate::yamlv3::map_parts(go);
        out.insert(format!("{path}{{key}}"), k.to_string());
        let p = format!("{path}{{}}");
        out.insert(p.clone(), v.to_string());
        V::types(&p, v, out);
    }
}

impl TypeTree for KeyMap {
    fn types(path: &str, _go: &str, out: &mut Out) {
        out.insert(format!("{path}{{key}}"), "string".into());
        out.insert(format!("{path}{{}}"), "string".into());
    }
}

impl TypeTree for PortRange {
    fn types(path: &str, _go: &str, out: &mut Out) {
        out.insert(format!("{path}.start"), "int".into());
        out.insert(format!("{path}.end"), "int".into());
    }
}

impl TypeTree for NodeIp {
    fn types(path: &str, _go: &str, out: &mut Out) {
        out.insert(format!("{path}.ipv4"), "string".into());
        out.insert(format!("{path}.ipv6"), "string".into());
    }
}

/// Canonical map of the official tree (same rules as the Go observer).
pub(crate) fn canonical(cfg: &crate::official::Config) -> Out {
    let mut out = Out::new();
    cfg.render("", &mut out);
    // room.room_configurations values are protobuf messages on the Go side
    // (protojson, null members stripped).
    let rc = &cfg.room.room_configurations;
    out.insert(
        "room.room_configurations".into(),
        J::Obj(
            rc.iter()
                .map(|(k, v)| (k.clone(), v.json().strip_nulls()))
                .collect(),
        )
        .text(),
    );
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::ffi::OsString;
    use std::panic::{AssertUnwindSafe, catch_unwind};

    #[test]
    fn go_types_match_original_reflection() {
        let raw = include_str!("../../../tests/oracles/config-observer/go-types.json");
        let expected: BTreeMap<String, String> = serde_json::from_str(raw).unwrap();
        let mut actual = Out::new();
        actual.insert(String::new(), "config.Config".into());
        <crate::official::Config as TypeTree>::types("", "config.Config", &mut actual);
        let mut diffs = Vec::new();
        for (k, v) in &expected {
            match actual.get(k) {
                Some(a) if a == v => {}
                Some(a) => diffs.push(format!("{k}: go {v} rust {a}")),
                None => diffs.push(format!("{k}: go {v} rust <missing>")),
            }
        }
        for k in actual.keys() {
            if !expected.contains_key(k) {
                diffs.push(format!("{k}: rust-only {}", actual[k]));
            }
        }
        assert!(
            diffs.is_empty(),
            "{} type differences:\n{}",
            diffs.len(),
            diffs.join("\n")
        );
    }

    #[test]
    fn go_g_format() {
        assert_eq!(0.9f32.json(), J::Str("0.9".into()));
        assert_eq!(0.95f64.json(), J::Str("0.95".into()));
        assert_eq!(1.5e7f64.json(), J::Str("1.5e+07".into()));
        assert_eq!(200000f64.json(), J::Str("200000".into()));
        assert_eq!(0.00001f64.json(), J::Str("1e-05".into()));
        assert_eq!((-0.6f64).json(), J::Str("-0.6".into()));
        assert_eq!(0f64.json(), J::Str("0".into()));
        assert_eq!(120f64.json(), J::Str("120".into()));
    }

    fn outcome_error(msg: String, usage: bool) -> serde_json::Value {
        let mut m = serde_json::Map::new();
        m.insert("error".into(), msg.into());
        if usage {
            m.insert("usage".into(), true.into());
        }
        serde_json::Value::Object(m)
    }

    fn run_case(case: &serde_json::Value, idx: usize) -> serde_json::Value {
        use std::os::unix::fs::PermissionsExt;
        let id = case["id"].as_str().unwrap_or_default().to_string();
        let dir = std::env::temp_dir().join(format!("cfgobs-rs-{}-{idx}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let dir_s = dir.to_str().unwrap().to_string();
        let sub = |s: &str| s.replace("{dir}", &dir_s);
        if let Some(files) = case["files"].as_object() {
            for (name, body) in files {
                let mode = case["file_modes"][name]
                    .as_str()
                    .map(|m| u32::from_str_radix(m, 8).unwrap())
                    .unwrap_or(0o600);
                let p = dir.join(name);
                std::fs::write(&p, sub(body.as_str().unwrap())).unwrap();
                std::fs::set_permissions(&p, std::fs::Permissions::from_mode(mode)).unwrap();
            }
        }
        let observed = |k: &str| {
            k.starts_with("LIVEKIT_")
                || k.starts_with("REDIS_")
                || k == "NODE_IP"
                || k == "UDP_PORT"
        };
        let mut pairs: Vec<(OsString, OsString)> = Vec::new();
        if let Some(env) = case["env"].as_object() {
            for (k, v) in env {
                pairs.push((k.into(), sub(v.as_str().unwrap()).into()));
            }
        }
        for (k, v) in std::env::vars_os() {
            if k.to_str().is_some_and(observed) {
                continue;
            }
            pairs.push((k, v));
        }
        let env = crate::Env::from_os_pairs(pairs);
        let mut args: Vec<OsString> = vec!["livekit-server".into()];
        if let Some(argv) = case["argv"].as_array() {
            args.extend(
                argv.iter()
                    .map(|a| OsString::from(sub(a.as_str().unwrap()))),
            );
        }
        let info = crate::ProgramInfo {
            binary_name: "livekit-server".into(),
            product_version: "1.13.10".into(),
            compat_version: "1.13.10",
        };
        let scrub = |s: String| s.replace(&dir_s, "{dir}");
        let result = catch_unwind(AssertUnwindSafe(|| crate::parse(args, &env, &info)));
        let run = match result {
            Err(p) => {
                let msg = p
                    .downcast_ref::<String>()
                    .cloned()
                    .or_else(|| p.downcast_ref::<&str>().map(|s| s.to_string()))
                    .unwrap_or_default();
                outcome_error(format!("panic: {msg}"), false)
            }
            Ok(Err(e)) => outcome_error(scrub(e.to_string()), false),
            Ok(Ok(crate::EntryAction::RunServer(lc))) => {
                let ok: serde_json::Map<String, serde_json::Value> = canonical(&lc.official)
                    .into_iter()
                    .map(|(k, v)| (k, scrub(v).into()))
                    .collect();
                serde_json::json!({ "ok": ok })
            }
            Ok(Ok(crate::EntryAction::PrintHelp {
                text, exit_code, ..
            })) if exit_code != 0 => {
                // Usage error: the bare error is the `Incorrect Usage: <err>` header.
                let header = text.split_once("\n\n").map_or(text.as_str(), |(h, _)| h);
                let err = header.strip_prefix("Incorrect Usage: ").unwrap_or(header);
                outcome_error(scrub(err.to_string()), true)
            }
            Ok(Ok(crate::EntryAction::PrintBytes {
                stdout,
                stderr,
                exit_code,
            })) if exit_code != 0 => {
                let text =
                    String::from_utf8_lossy(if stdout.is_empty() { &stderr } else { &stdout })
                        .trim_end()
                        .lines()
                        .last()
                        .unwrap_or_default()
                        .to_string();
                outcome_error(scrub(text), true)
            }
            Ok(Ok(other)) => {
                let label = match other {
                    crate::EntryAction::GenerateKeys => "subcommand:generate-keys",
                    crate::EntryAction::Ports(_) => "subcommand:ports",
                    crate::EntryAction::ListNodes(_) => "subcommand:list-nodes",
                    crate::EntryAction::CreateJoinToken { .. } => "subcommand:create-join-token",
                    _ => "help-or-version",
                };
                serde_json::json!({ "no_action": label })
            }
        };
        let _ = std::fs::remove_dir_all(&dir);
        serde_json::json!({ "id": id, "run": run })
    }

    #[test]
    #[ignore = "differential harness; needs CONFIG_OBSERVER_CASES / CONFIG_OBSERVER_OUT"]
    fn config_observer() {
        let cases_path = std::env::var("CONFIG_OBSERVER_CASES").expect("CONFIG_OBSERVER_CASES");
        let out_path = std::env::var("CONFIG_OBSERVER_OUT").expect("CONFIG_OBSERVER_OUT");
        let doc: serde_json::Value =
            serde_json::from_slice(&std::fs::read(cases_path).unwrap()).unwrap();
        let results: Vec<serde_json::Value> = doc["cases"]
            .as_array()
            .unwrap()
            .iter()
            .enumerate()
            .map(|(i, c)| run_case(c, i))
            .collect();
        let leaves: Vec<serde_json::Value> = crate::generated::specs()
            .iter()
            .map(|s| {
                serde_json::json!({
                    "name": s.path, "kind": format!("{:?}", s.kind),
                    "default": s.default_text, "env": s.env,
                })
            })
            .collect();
        let doc = serde_json::json!({
            "observer": "rust lk-config parse() -> EntryAction::RunServer -> LoadedConfig.official",
            "leaves": leaves,
            "default_tree": canonical(&crate::official::Config::default()),
            "results": results,
        });
        std::fs::write(out_path, serde_json::to_vec_pretty(&doc).unwrap()).unwrap();
    }
}
