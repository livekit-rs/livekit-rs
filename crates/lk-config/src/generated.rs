//! Generated CLI flags (`config.go:765-1104`, impl-notes §3).
//!
//! Upstream walks `Config` with reflection: every scalar leaf of the YAML tree
//! gets a flag named by its dotted path (`--rtc.udp_port.start`) and an env var
//! `LIVEKIT_<PATH_UPPER_UNDERSCORE>`. Struct-typed custom scalars are recursed
//! into (`rtc.udp_port.{start,end}`, `rtc.node_ip.{ipv4,ipv6}`); slices, maps and
//! pointer-to-struct fields get no flag; a path equal to a base flag name
//! (`region`) is skipped.
//!
//! Here the same walk is the [`Walk`] impl that `yaml_struct!` generates for every
//! struct, so there is exactly one table: [`specs()`] (built once from the
//! defaults) drives clap argument creation, env lookup, `help-verbose` and
//! [`apply`], which writes typed values back into the live tree through the same
//! walk.

use std::collections::BTreeMap;
use std::sync::OnceLock;

use crate::duration::{GoDuration, parse_go_duration};
use crate::official::Config;
use crate::ports::{NodeIp, PortRange};
use crate::strconv::{format_float, parse_bool, parse_float, parse_int, parse_uint};

/// Go kind of a generated flag (impl-notes §3.2).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Kind {
    Bool,
    Str,
    /// Go int / int32 / int64: `ParseInt(s, 0, 64)`, converted (wrapping) to the field.
    Int,
    /// Go uint8/16/32: `ParseUint(s, 0, 64)`, converted (truncating) to the field.
    Uint,
    Uint64,
    Float,
    Duration,
}

impl Kind {
    /// Type name used in urfave's env parse error.
    pub fn go_name(self) -> &'static str {
        match self {
            Kind::Bool => "bool",
            Kind::Str => "string",
            Kind::Int => "int",
            Kind::Uint => "uint",
            Kind::Uint64 => "uint64",
            Kind::Float => "float64",
            Kind::Duration => "duration",
        }
    }

    /// `%T` of the flag value in urfave's env parse error.
    pub fn env_type_name(self) -> &'static str {
        match self {
            Kind::Duration => "time.Duration",
            other => other.go_name(),
        }
    }

    pub fn takes_value(self) -> bool {
        self != Kind::Bool
    }
}

/// A parsed flag value.
#[derive(Clone, Debug, PartialEq)]
pub enum Value {
    Bool(bool),
    Str(String),
    /// A source string flag with raw POSIX bytes. Only byte-capable consumers
    /// may consume it; never convert it through a replacement-character string.
    OsStr(std::ffi::OsString),
    Int(i64),
    Uint(u64),
    Float(f64),
    Duration(GoDuration),
}

/// Parses `s` with the Go parser for `kind`. Error text is the Go error.
pub(crate) fn parse_value(kind: Kind, s: &str) -> Result<Value, String> {
    Ok(match kind {
        Kind::Bool => Value::Bool(parse_bool(s)?),
        Kind::Str => Value::Str(s.to_string()),
        Kind::Int => Value::Int(parse_int(s)?),
        Kind::Uint | Kind::Uint64 => Value::Uint(parse_uint(s)?),
        Kind::Float => Value::Float(parse_float(s)?),
        Kind::Duration => Value::Duration(parse_go_duration(s)?),
    })
}

/// Env value semantics (`CLI/flag_impl.go:142-153`): an empty env var counts as
/// set; strings become `""`, bools `false`, numbers/durations zero.
pub(crate) fn parse_env_value(kind: Kind, s: &str) -> Result<Value, String> {
    if s.is_empty() {
        return Ok(match kind {
            Kind::Bool => Value::Bool(false),
            Kind::Str => Value::Str(String::new()),
            Kind::Int => Value::Int(0),
            Kind::Uint | Kind::Uint64 => Value::Uint(0),
            Kind::Float => Value::Float(0.0),
            Kind::Duration => Value::Duration(GoDuration::ZERO),
        });
    }
    parse_value(kind, s)
}

/// A scalar leaf of the tree.
pub trait Leaf {
    fn kind(&self) -> Kind;
    /// `cliDefaultText` (`config.go:1052-1077`).
    fn default_text(&self) -> String;
    /// Stores `v` with Go `reflect.Convert` semantics (wrapping/truncating casts).
    fn set(&mut self, v: &Value);
}

/// Visits every scalar leaf with its dotted path.
pub trait Walk {
    fn walk(&mut self, path: &str, f: &mut dyn FnMut(&str, &mut dyn Leaf));
}

pub(crate) fn join(prefix: &str, name: &str) -> String {
    if prefix.is_empty() {
        name.to_string()
    } else {
        format!("{prefix}.{name}")
    }
}

macro_rules! leaf_int {
    ($($t:ty => $kind:expr),* $(,)?) => {$(
        impl Leaf for $t {
            fn kind(&self) -> Kind { $kind }
            fn default_text(&self) -> String { self.to_string() }
            fn set(&mut self, v: &Value) {
                match v {
                    Value::Int(i) => *self = *i as $t,
                    Value::Uint(u) => *self = *u as $t,
                    _ => {}
                }
            }
        }
        impl Walk for $t {
            fn walk(&mut self, path: &str, f: &mut dyn FnMut(&str, &mut dyn Leaf)) { f(path, self) }
        }
    )*};
}

leaf_int!(
    i32 => Kind::Int,
    i64 => Kind::Int,
    u8 => Kind::Uint,
    u16 => Kind::Uint,
    u32 => Kind::Uint,
    u64 => Kind::Uint64,
);

impl Leaf for bool {
    fn kind(&self) -> Kind {
        Kind::Bool
    }
    fn default_text(&self) -> String {
        self.to_string()
    }
    fn set(&mut self, v: &Value) {
        if let Value::Bool(b) = v {
            *self = *b;
        }
    }
}

impl Leaf for String {
    fn kind(&self) -> Kind {
        Kind::Str
    }
    fn default_text(&self) -> String {
        self.clone()
    }
    fn set(&mut self, v: &Value) {
        if let Value::Str(s) = v {
            *self = s.clone();
        }
    }
}

impl Leaf for f32 {
    fn kind(&self) -> Kind {
        Kind::Float
    }
    fn default_text(&self) -> String {
        format_float(*self as f64)
    }
    fn set(&mut self, v: &Value) {
        if let Value::Float(x) = v {
            *self = *x as f32;
        }
    }
}

impl Leaf for f64 {
    fn kind(&self) -> Kind {
        Kind::Float
    }
    fn default_text(&self) -> String {
        format_float(*self)
    }
    fn set(&mut self, v: &Value) {
        if let Value::Float(x) = v {
            *self = *x;
        }
    }
}

impl Leaf for GoDuration {
    fn kind(&self) -> Kind {
        Kind::Duration
    }
    fn default_text(&self) -> String {
        self.to_string()
    }
    fn set(&mut self, v: &Value) {
        if let Value::Duration(d) = v {
            *self = *d;
        }
    }
}

/// Go `*bool`: flag type is the pointee; a nil default prints "".
impl Leaf for Option<bool> {
    fn kind(&self) -> Kind {
        Kind::Bool
    }
    fn default_text(&self) -> String {
        self.map(|b| b.to_string()).unwrap_or_default()
    }
    fn set(&mut self, v: &Value) {
        if let Value::Bool(b) = v {
            *self = Some(*b);
        }
    }
}

/// Go `*int`.
impl Leaf for Option<i64> {
    fn kind(&self) -> Kind {
        Kind::Int
    }
    fn default_text(&self) -> String {
        self.map(|i| i.to_string()).unwrap_or_default()
    }
    fn set(&mut self, v: &Value) {
        match v {
            Value::Int(i) => *self = Some(*i),
            Value::Uint(u) => *self = Some(*u as i64),
            _ => {}
        }
    }
}

macro_rules! leaf_walk {
    ($($t:ty),*) => {$(
        impl Walk for $t {
            fn walk(&mut self, path: &str, f: &mut dyn FnMut(&str, &mut dyn Leaf)) { f(path, self) }
        }
    )*};
}

leaf_walk!(
    bool,
    String,
    f32,
    f64,
    GoDuration,
    Option<bool>,
    Option<i64>
);

/// Slices and maps have no generated flag.
impl<T> Walk for Vec<T> {
    fn walk(&mut self, _path: &str, _f: &mut dyn FnMut(&str, &mut dyn Leaf)) {}
}

impl<K, V> Walk for BTreeMap<K, V> {
    fn walk(&mut self, _path: &str, _f: &mut dyn FnMut(&str, &mut dyn Leaf)) {}
}

impl Walk for Option<Vec<String>> {
    fn walk(&mut self, _path: &str, _f: &mut dyn FnMut(&str, &mut dyn Leaf)) {}
}

/// `rtc.udp_port` is a Go struct, so the reflection walk recurses into it.
impl Walk for PortRange {
    fn walk(&mut self, path: &str, f: &mut dyn FnMut(&str, &mut dyn Leaf)) {
        f(&join(path, "start"), &mut self.start);
        f(&join(path, "end"), &mut self.end);
    }
}

impl Walk for NodeIp {
    fn walk(&mut self, path: &str, f: &mut dyn FnMut(&str, &mut dyn Leaf)) {
        f(&join(path, "ipv4"), &mut self.v4);
        f(&join(path, "ipv6"), &mut self.v6);
    }
}

/// One generated flag.
#[derive(Clone, Debug)]
pub struct LeafSpec {
    /// Dotted YAML path, also the flag name.
    pub path: String,
    /// `LIVEKIT_<PATH>` with `.` → `_`, upper-cased.
    pub env: String,
    pub kind: Kind,
    /// Default value text shown by `help-verbose`.
    pub default_text: String,
}

/// Base flag names; a generated path equal to one of them is skipped
/// (`config.go:803-805`).
pub(crate) const BASE_FLAG_NAMES: &[&str] = &[
    "bind",
    "config",
    "config-body",
    "key-file",
    "keys",
    "region",
    "node-ip",
    "udp-port",
    "redis-host",
    "redis-password",
    "turn-cert",
    "turn-key",
    "cpuprofile",
    "memprofile",
    "dev",
    "disable-strict-config",
];

pub(crate) fn env_name(path: &str) -> String {
    format!("LIVEKIT_{}", path.to_ascii_uppercase().replace('.', "_"))
}

/// The generated flag table, computed once from the upstream defaults.
pub fn specs() -> &'static [LeafSpec] {
    static SPECS: OnceLock<Vec<LeafSpec>> = OnceLock::new();
    SPECS.get_or_init(|| {
        let mut defaults = Config::default();
        let mut out = Vec::new();
        defaults.walk("", &mut |path: &str, leaf: &mut dyn Leaf| {
            if BASE_FLAG_NAMES.contains(&path) {
                return;
            }
            out.push(LeafSpec {
                path: path.to_string(),
                env: env_name(path),
                kind: leaf.kind(),
                default_text: leaf.default_text(),
            });
        });
        out
    })
}

#[cfg(test)]
pub(crate) fn spec(path: &str) -> Option<&'static LeafSpec> {
    specs().iter().find(|s| s.path == path)
}

/// Writes the given values into `cfg` (`updateFromCLI`, `config.go:1079-1104`).
pub(crate) fn apply(cfg: &mut Config, values: &[(&'static LeafSpec, Value)]) {
    if values.is_empty() {
        return;
    }
    let by_path: BTreeMap<&str, &Value> =
        values.iter().map(|(s, v)| (s.path.as_str(), v)).collect();
    cfg.walk("", &mut |path: &str, leaf: &mut dyn Leaf| {
        if let Some(v) = by_path.get(path) {
            leaf.set(v);
        }
    });
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn table_has_expected_entries() {
        let s = spec("rtc.udp_port.start").unwrap();
        assert_eq!(s.env, "LIVEKIT_RTC_UDP_PORT_START");
        assert_eq!(s.kind, Kind::Int);
        assert_eq!(spec("rtc.tcp_port").unwrap().default_text, "7881");
        assert_eq!(spec("key_file").unwrap().env, "LIVEKIT_KEY_FILE");
        assert_eq!(spec("development").unwrap().kind, Kind::Bool);
        assert_eq!(
            spec("room.create_room_timeout").unwrap().default_text,
            "10s"
        );
        assert_eq!(spec("api.max_check_interval").unwrap().default_text, "5m0s");
        assert_eq!(
            spec("node_selector.cpu_load_limit").unwrap().default_text,
            "0.8999999761581421"
        );
        assert_eq!(spec("rtc.node_ip.ipv6").unwrap().kind, Kind::Str);
        assert_eq!(spec("rtc.allow_tcp_fallback").unwrap().default_text, "");
        assert!(spec("redis.max_redirects").is_some());
        // excluded: base flag collision, slices, maps, pointer-to-struct
        assert!(spec("region").is_none());
        assert!(spec("bind_addresses").is_none());
        assert!(spec("keys").is_none());
        assert!(spec("rtc.stun_servers").is_none());
        assert!(spec("room.enabled_codecs").is_none());
        assert!(spec("redis.tls").is_none());
        assert!(spec("redis.tls.enabled").is_none());
        assert!(spec("logging.component_levels").is_none());
    }

    #[test]
    fn apply_udp_port_start() {
        let mut cfg = Config::default();
        let s = spec("rtc.udp_port.start").unwrap();
        apply(&mut cfg, &[(s, parse_value(Kind::Int, "9000").unwrap())]);
        assert_eq!(
            cfg.rtc.udp_port,
            PortRange {
                start: 9000,
                end: 0
            }
        );
    }

    #[test]
    fn uint_truncates_like_go() {
        let mut cfg = Config::default();
        let s = spec("turn.relay_range_start").unwrap();
        apply(&mut cfg, &[(s, parse_value(Kind::Uint, "70000").unwrap())]);
        assert_eq!(cfg.turn.relay_range_start, 4464);
    }

    #[test]
    fn env_empty_is_zero_but_set() {
        assert_eq!(parse_env_value(Kind::Uint, "").unwrap(), Value::Uint(0));
        assert_eq!(parse_env_value(Kind::Bool, "").unwrap(), Value::Bool(false));
        assert!(parse_env_value(Kind::Duration, "5").is_err());
    }
}
