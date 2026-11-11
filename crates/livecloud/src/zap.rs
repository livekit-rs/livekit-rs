//! zap-compatible stderr encoder (SPEC-0074 C32).
//!
//! Byte format source: go.uber.org/zap v1.28.0 `zapcore/console_encoder.go`
//! with `NewDevelopmentEncoderConfig` (protocol logger `zaputil.NewDevelopmentEncoder`),
//! and `zapcore/json_encoder.go` with `NewProductionEncoderConfig` when
//! `logging.json` is set. Apache-2.0 upstreams are retained under references/.
//!
//! Console line: `{time}\t{LEVEL}\t{names}\t{caller}\t{msg}[\t{"k": v, ...}]`.
//! - time: `ISO8601TimeEncoder` — local time `YYYY-MM-DDTHH:MM:SS.mmm±HH:MM`.
//! - level: `CapitalLevelEncoder` (DEBUG/INFO/WARN/ERROR, no colour encoder).
//! - names: `FullNameEncoder` — dot-joined logger names; the root logger is
//!   named `livekit` (protocol `logger.SetLogger`), components append `.name`.
//!   The segment is skipped when empty, like zap's sliceElementSeparator join.
//! - caller: `ShortCallerEncoder` shape `file:line`; Rust positions necessarily
//!   differ from Go files (documented normalization in SPEC-0074 C32).
//! - context: jsonEncoder `spaced` rendering — `"key": value` joined by `, `,
//!   wrapped in braces; strings quoted, numbers/bools/raw JSON unquoted.
//!
//! JSON mode: production encoder order `{"level","ts","logger","caller","msg",...}`,
//! where `ts` is zap's `EpochSecondsTimeEncoder` — an unquoted float of seconds
//! since the Unix epoch, shortest round-trip rendered.

use std::sync::{OnceLock, RwLock};

/// Numeric level ranks matching zapcore (DEBUG=-1, INFO=0, WARN=1, ERROR=2).
pub const DEBUG: i8 = -1;
pub const INFO: i8 = 0;
pub const WARN: i8 = 1;
pub const ERROR: i8 = 2;

/// zap console/JSON field value with upstream rendering semantics.
#[derive(Clone, Debug, PartialEq)]
pub enum ZapValue {
    /// JSON-quoted in both modes (zap string fields).
    Str(String),
    /// Rendered verbatim as decimal (zap strong-typed numeric fields).
    Num(i64),
    /// Lossless zap Uint64 fields, including counters above i64::MAX.
    Uint(u64),
    /// zap Float64 field: finite numbers use strconv AppendFloat('f', -1),
    /// while nonfinite values use the exact JSON strings NaN/+Inf/-Inf.
    Float(f64),
    Bool(bool),
    /// Typed zap array (`Strings`/`Uint32s`-style fields): jsonEncoder element
    /// separators, so `, ` between items in console (spaced) rendering.
    Array(Vec<ZapValue>),
    /// Reflected Go struct via `zap.Any` fallback: `encoding/json` compact
    /// bytes (`":"` and `","`, Go field casing), e.g. PortRange dumps.
    Object(Vec<(String, ZapValue)>),
}

impl ZapValue {
    /// zap jsonEncoder addValue rendering. `spaced` selects the console
    /// encoder's element separators (`, ` and `: `); production JSON is
    /// compact (`,`). Reflected `Object` bytes never depend on `spaced`
    /// because `encoding/json` writes them directly.
    fn render(&self, out: &mut String, spaced: bool) {
        match self {
            ZapValue::Str(s) => {
                out.push('"');
                escape_zap(s, out);
                out.push('"');
            }
            ZapValue::Num(n) => out.push_str(&n.to_string()),
            ZapValue::Uint(n) => out.push_str(&n.to_string()),
            ZapValue::Float(n) if n.is_nan() => out.push_str("\"NaN\""),
            ZapValue::Float(n) if *n == f64::INFINITY => out.push_str("\"+Inf\""),
            ZapValue::Float(n) if *n == f64::NEG_INFINITY => out.push_str("\"-Inf\""),
            ZapValue::Float(n) => out.push_str(&n.to_string()),
            ZapValue::Bool(b) => out.push_str(if *b { "true" } else { "false" }),
            ZapValue::Array(items) => {
                out.push('[');
                for (i, item) in items.iter().enumerate() {
                    if i > 0 {
                        out.push_str(if spaced { ", " } else { "," });
                    }
                    item.render(out, spaced);
                }
                out.push(']');
            }
            ZapValue::Object(fields) => {
                out.push('{');
                for (i, (key, value)) in fields.iter().enumerate() {
                    if i > 0 {
                        out.push(',');
                    }
                    out.push('"');
                    escape_zap(key, out);
                    out.push_str("\":");
                    value.render(out, spaced);
                }
                out.push('}');
            }
        }
    }
}

/// Go `encoding/json` string escaping (zap safeAddString, HTML escaping off).
fn escape_zap(s: &str, out: &mut String) {
    for c in s.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            c if (c as u32) < 0x20 => out.push_str(&format!("\\u{:04x}", c as u32)),
            c => out.push(c),
        }
    }
}

/// zap `ISO8601TimeEncoder`: Go layout `2006-01-02T15:04:05.000-0700` —
/// local time, milliseconds, offset without a colon (chrono emits `+08:00`).
fn iso8601_now() -> String {
    use chrono::{Local, SecondsFormat};
    go_offset(Local::now().to_rfc3339_opts(SecondsFormat::Millis, false))
}

/// Strip the colon of the trailing UTC offset (`+08:00` → `+0800`); a `Z`
/// suffix (UTC) is untouched. The offset sign is the first `+`/`-` after `T`.
fn go_offset(stamped: String) -> String {
    let bytes: Vec<char> = stamped.chars().collect();
    let t = bytes.iter().position(|c| *c == 'T').unwrap_or(0);
    let Some(sign) = bytes[t..].iter().position(|c| *c == '+' || *c == '-') else {
        return stamped;
    };
    let sign = t + sign;
    if bytes.get(sign + 3) == Some(&':') {
        stamped
            .chars()
            .enumerate()
            .filter(|(i, _)| *i != sign + 3)
            .map(|(_, c)| c)
            .collect()
    } else {
        stamped
    }
}

/// zap's production encoder (`zap.NewProductionEncoderConfig`) time: float
/// seconds since the Unix epoch. Go computes `float64(unix) + float64(nsec)/1e9`
/// and writes it with the shortest round-trip decimal, unquoted, under the `ts`
/// key — so `2026-11-03T22:25:29.998Z` is `1793744729.998`, not a string.
fn epoch_seconds_now() -> String {
    use chrono::Utc;
    epoch_seconds(&Utc::now())
}

fn epoch_seconds<T: chrono::TimeZone>(t: &chrono::DateTime<T>) -> String {
    let unix = t.timestamp();
    let nsec = t.timestamp_subsec_nanos() as u64;
    (unix as f64 + (nsec as f64) / 1e9).to_string()
}

struct Policy {
    json: bool,
    /// Overridable for deterministic tests.
    time: fn() -> String,
}

static POLICY: OnceLock<RwLock<Policy>> = OnceLock::new();

fn policy() -> &'static RwLock<Policy> {
    POLICY.get_or_init(|| {
        RwLock::new(Policy {
            json: false,
            time: iso8601_now,
        })
    })
}

/// `logging.json` selects the production JSON encoder and its time format.
pub fn set_json(json: bool) {
    let mut p = policy().write().unwrap();
    p.json = json;
    p.time = if json { epoch_seconds_now } else { iso8601_now };
}

/// Test hook: deterministic timestamps in captured output.
#[cfg(test)]
pub(crate) fn set_time_source(f: fn() -> String) {
    policy().write().unwrap().time = f;
}

fn level_name(rank: i8) -> &'static str {
    match rank {
        DEBUG => "DEBUG",
        WARN => "WARN",
        ERROR => "ERROR",
        _ => "INFO",
    }
}

fn json_level_name(rank: i8) -> &'static str {
    match rank {
        DEBUG => "debug",
        WARN => "warn",
        ERROR => "error",
        _ => "info",
    }
}

/// zap `ShortCallerEncoder`: keeps the last two path components
/// (`/pkg/service/server.go:149` → `service/server.go:149`).
fn short_caller(caller: &str) -> &str {
    let trimmed = caller.trim_start_matches('/');
    let mut parts = trimmed.rsplitn(3, '/');
    match (parts.next(), parts.next()) {
        (Some(file), Some(dir)) => {
            // `dir` may still carry a prefix: keep only its last component.
            let dir = dir.rsplit('/').next().unwrap_or(dir);
            // Rebuild: caller ends with `file:line`; find its start.
            let suffix = format!("{dir}/{file}");
            caller.rfind(&suffix).map_or(caller, |pos| &caller[pos..])
        }
        _ => caller,
    }
}

/// Render one complete zap line (without the trailing newline) for a logger
/// named `name` ("" for the root `livekit` logger is produced by the caller
/// including its own name; components pass e.g. `livekit.io`).
pub fn format_line(
    rank: i8,
    name: &str,
    caller: &str,
    msg: &str,
    fields: &[(&str, ZapValue)],
    stack: Option<&str>,
) -> String {
    let (json, time) = {
        let p = policy().read().unwrap();
        (p.json, p.time)
    };
    let time = time();
    let mut line = String::new();
    if json {
        line.push('{');
        let mut parts: Vec<String> = Vec::new();
        parts.push(format!("\"level\":\"{}\"", json_level_name(rank)));
        parts.push(format!("\"ts\":{time}"));
        if !name.is_empty() {
            parts.push(format!("\"logger\":\"{name}\""));
        }
        if !caller.is_empty() {
            let mut esc = String::new();
            escape_zap(short_caller(caller), &mut esc);
            parts.push(format!("\"caller\":\"{esc}\""));
        }
        let mut esc = String::new();
        escape_zap(msg, &mut esc);
        parts.push(format!("\"msg\":\"{esc}\""));
        for (key, value) in fields {
            let mut k = String::new();
            escape_zap(key, &mut k);
            let mut v = String::new();
            value.render(&mut v, false);
            parts.push(format!("\"{k}\":{v}"));
        }
        if let Some(stack) = stack {
            let mut esc = String::new();
            escape_zap(stack, &mut esc);
            parts.push(format!("\"stacktrace\":\"{esc}\""));
        }
        line.push_str(&parts.join(","));
        line.push('}');
        return line;
    }
    // Console encoder: metadata elements joined by '\t', then msg, then context.
    line.push_str(&time);
    line.push('\t');
    line.push_str(level_name(rank));
    if !name.is_empty() {
        line.push('\t');
        line.push_str(name);
    }
    if !caller.is_empty() {
        line.push('\t');
        line.push_str(short_caller(caller));
    }
    line.push('\t');
    line.push_str(msg);
    if !fields.is_empty() {
        let mut ctx = String::new();
        for (key, value) in fields {
            if !ctx.is_empty() {
                ctx.push_str(", ");
            }
            let mut k = String::new();
            escape_zap(key, &mut k);
            ctx.push('"');
            ctx.push_str(&k);
            ctx.push_str("\": ");
            value.render(&mut ctx, true);
        }
        line.push('\t');
        line.push('{');
        line.push_str(&ctx);
        line.push('}');
    }
    if let Some(stack) = stack {
        line.push('\n');
        line.push_str(stack);
    }
    line
}

/// Emit a zap line to stderr when `enabled` (the caller resolves the component
/// threshold; this mirrors zap's core decision before encoding).
pub fn emit(
    rank: i8,
    name: &str,
    caller: &str,
    msg: &str,
    fields: &[(&str, ZapValue)],
    stack: Option<&str>,
    enabled: bool,
) {
    if !enabled {
        return;
    }
    use std::io::Write;
    let line = format_line(rank, name, caller, msg, fields, stack);
    let mut err = std::io::stderr().lock();
    let _ = writeln!(err, "{line}");
    let _ = err.flush();
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;
    /// Policy and time source are process-global; serialize the whole
    /// set/emit/assert sequence (a passing interleaving is not guaranteed).
    static POLICY_TEST_LOCK: Mutex<()> = Mutex::new(());

    fn fixed_time() -> String {
        "2026-10-28T23:46:05.200+0800".to_string()
    }

    #[test]
    fn console_line_matches_reference_zap_development_output() {
        let _guard = POLICY_TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        set_json(false);
        set_time_source(fixed_time);
        let line = format_line(
            INFO,
            "livekit",
            "service/server.go:309",
            "starting LiveKit server",
            &[
                ("portHttp", ZapValue::Num(17881)),
                ("nodeID", ZapValue::Str("ND_fzR7EB8Bmman".into())),
                ("version", ZapValue::Str("1.13.10".into())),
                (
                    "bindAddresses",
                    ZapValue::Array(vec![
                        ZapValue::Str("127.0.0.1".into()),
                        ZapValue::Str("::1".into()),
                    ]),
                ),
                ("rtc.portTCP", ZapValue::Num(7881)),
                (
                    "rtc.portUDP",
                    ZapValue::Object(vec![
                        ("Start".into(), ZapValue::Num(7882)),
                        ("End".into(), ZapValue::Num(0)),
                    ]),
                ),
                ("portPrometheus", ZapValue::Num(17882)),
            ],
            None,
        );
        assert_eq!(
            line,
            "2026-10-28T23:46:05.200+0800\tINFO\tlivekit\tservice/server.go:309\tstarting LiveKit server\t{\"portHttp\": 17881, \"nodeID\": \"ND_fzR7EB8Bmman\", \"version\": \"1.13.10\", \"bindAddresses\": [\"127.0.0.1\", \"::1\"], \"rtc.portTCP\": 7881, \"rtc.portUDP\": {\"Start\":7882,\"End\":0}, \"portPrometheus\": 17882}"
        );
    }

    #[test]
    fn console_line_without_fields_or_name_omits_segments() {
        let _guard = POLICY_TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        set_json(false);
        set_time_source(fixed_time);
        let line = format_line(
            INFO,
            "livekit",
            "routing/interfaces.go:181",
            "using single-node routing",
            &[],
            None,
        );
        assert_eq!(
            line,
            "2026-10-28T23:46:05.200+0800\tINFO\tlivekit\trouting/interfaces.go:181\tusing single-node routing"
        );
        let debug = format_line(
            DEBUG,
            "livekit",
            "service/signal.go:120",
            "starting relay signal server",
            &[("topic", ZapValue::Str("ND_x".into()))],
            None,
        );
        assert!(debug.starts_with("2026-10-28T23:46:05.200+0800\tDEBUG\t"));
    }

    #[test]
    fn console_error_line_carries_stack_after_newline() {
        let _guard = POLICY_TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        set_json(false);
        set_time_source(fixed_time);
        let line = format_line(
            ERROR,
            "livekit",
            "config/config.go:850",
            "secret is too short, should be at least 32 characters for security",
            &[("apiKey", ZapValue::Str("devkey".into()))],
            Some(
                "github.com/livekit/livekit-server/pkg/config.(*Config).ValidateKeys\n\t/go/src/config.go:850",
            ),
        );
        assert!(line.starts_with("2026-10-28T23:46:05.200+0800\tERROR\tlivekit\tconfig/config.go:850\tsecret is too short"));
        assert!(line.ends_with("\t/go/src/config.go:850"));
        assert!(line.contains("\n"));
    }

    #[test]
    fn json_mode_uses_production_field_order() {
        let _guard = POLICY_TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        set_json(true);
        set_time_source(|| "1793744699.470478".to_string());
        let line = format_line(
            WARN,
            "livekit",
            "pkg/service/server.go:149",
            "prometheus_port is deprecated, please switch to prometheus.port instead",
            &[
                ("num", ZapValue::Num(3)),
                ("s", ZapValue::Str("x\ny".into())),
            ],
            None,
        );
        assert_eq!(
            line,
            "{\"level\":\"warn\",\"ts\":1793744699.470478,\"logger\":\"livekit\",\"caller\":\"service/server.go:149\",\"msg\":\"prometheus_port is deprecated, please switch to prometheus.port instead\",\"num\":3,\"s\":\"x\\ny\"}"
        );
        set_json(false);
    }

    /// Every expected value below is a `ts` field the reference binary actually
    /// printed while dialing Redis (`cli-redis-startup-tls-topology` receipts),
    /// so the encoder is pinned to zap's own rendering, not to a guess.
    #[test]
    fn json_time_matches_zap_epoch_seconds_encoder() {
        use chrono::{TimeZone, Utc};
        let cases = [
            (1793744699_u64, 470_478_000_u32, "1793744699.470478"),
            (1793744730, 544_369_000, "1793744730.544369"),
            (1793744765, 368_641_100, "1793744765.3686411"),
            (1793744800, 447_610_900, "1793744800.4476109"),
            (1793744841, 793_178_800, "1793744841.7931788"),
            (1793744877, 699_450_000, "1793744877.69945"),
            (1793744908, 704_658_000, "1793744908.704658"),
            (1793744971, 767_081_000, "1793744971.767081"),
        ];
        for (unix, nsec, expected) in cases {
            let at = Utc.timestamp_opt(unix as i64, nsec).unwrap();
            assert_eq!(epoch_seconds(&at), expected, "ts for {unix}.{nsec}");
        }
        // A whole second has no fractional part, exactly like Go's shortest 'f'
        // rendering of the float.
        let whole = chrono::DateTime::from_timestamp(1793744699, 0).unwrap();
        assert_eq!(epoch_seconds(&whole), "1793744699");
    }

    #[test]
    fn short_caller_keeps_last_two_components_like_zap() {
        assert_eq!(
            short_caller("pkg/service/server.go:309"),
            "service/server.go:309"
        );
        assert_eq!(short_caller("cmd/server/main.go:211"), "server/main.go:211");
        assert_eq!(
            short_caller("/pkg/config/config.go:850"),
            "config/config.go:850"
        );
        assert_eq!(short_caller("config.go:1"), "config.go:1");
    }

    #[test]
    fn offset_drops_colon_like_go_layout() {
        assert_eq!(
            go_offset("2026-10-28T23:46:05.200+08:00".into()),
            "2026-10-28T23:46:05.200+0800"
        );
        assert_eq!(
            go_offset("2026-10-28T04:46:05.200-07:00".into()),
            "2026-10-28T04:46:05.200-0700"
        );
        assert_eq!(
            go_offset("2026-10-28T15:46:05.200Z".into()),
            "2026-10-28T15:46:05.200Z"
        );
    }

    #[test]
    fn escaping_matches_go_json_strings() {
        let _guard = POLICY_TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        set_json(false);
        set_time_source(fixed_time);
        let line = format_line(
            INFO,
            "livekit",
            "a/b.go:1",
            "msg",
            &[("k", ZapValue::Str("quote\" back\\slash tab\t end".into()))],
            None,
        );
        assert!(
            line.contains("\"k\": \"quote\\\" back\\\\slash tab\\t end\""),
            "{line}"
        );
    }
    #[derive(Clone)]
    struct CaptureTypedEvents(std::sync::Arc<Mutex<Vec<String>>>);
    impl<S: tracing::Subscriber> tracing_subscriber::Layer<S> for CaptureTypedEvents {
        fn on_event(
            &self,
            event: &tracing::Event<'_>,
            _: tracing_subscriber::layer::Context<'_, S>,
        ) {
            let event = crate::logging::fields::EventFields::collect(event);
            let fields = event
                .fields
                .iter()
                .map(|(key, value)| (key.as_str(), value.clone()))
                .collect::<Vec<_>>();
            self.0.lock().unwrap().push(format_line(
                INFO,
                "livekit.typed",
                "fixture.go:1",
                &event.message,
                &fields,
                None,
            ));
        }
    }
    #[test]
    fn real_tracing_event_preserves_primitive_types_unsigned_boundary_and_error_text() {
        use tracing_subscriber::prelude::*;
        let _guard = POLICY_TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        set_json(true);
        set_time_source(|| "1".into());
        let output = std::sync::Arc::new(Mutex::new(Vec::new()));
        let registry = tracing_subscriber::registry().with(CaptureTypedEvents(output.clone()));
        let error = std::io::Error::other("source error text");
        let as_error: &(dyn std::error::Error + 'static) = &error;
        tracing::subscriber::with_default(registry, || {
            tracing::info!(signed=-12i64,unsigned=u64::MAX,ratio=0.125f64,ready=true,text="line\nquoted",debug=?vec![1,2],fault=as_error,"typed native event");
        });
        let lines = output.lock().unwrap();
        assert_eq!(lines.len(), 1);
        let value: serde_json::Value = serde_json::from_str(&lines[0]).unwrap();
        assert_eq!(value["msg"], "typed native event");
        assert_eq!(value["signed"].as_i64(), Some(-12));
        assert_eq!(value["unsigned"].as_u64(), Some(u64::MAX));
        assert_eq!(value["ratio"].as_f64(), Some(0.125));
        assert_eq!(value["ready"].as_bool(), Some(true));
        assert_eq!(value["text"], "line\nquoted");
        assert_eq!(value["debug"], "[1, 2]");
        assert_eq!(value["fault"], "source error text");
        assert!(lines[0].contains("\"unsigned\":18446744073709551615"));
    }
    #[test]
    fn real_tracing_float_special_values_remain_valid_zap_json_and_console() {
        use tracing_subscriber::prelude::*;
        let _guard = POLICY_TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        for json in [true, false] {
            set_json(json);
            set_time_source(|| "1".into());
            let output = std::sync::Arc::new(Mutex::new(Vec::new()));
            let registry = tracing_subscriber::registry().with(CaptureTypedEvents(output.clone()));
            tracing::subscriber::with_default(registry, || {
                tracing::info!(
                    nan = f64::NAN,
                    pos = f64::INFINITY,
                    neg = f64::NEG_INFINITY,
                    small = 1e-8f64,
                    large = 1e22f64,
                    negative_zero = -0.0f64,
                    "float native event"
                );
            });
            let lines = output.lock().unwrap();
            assert_eq!(lines.len(), 1);
            if json {
                let value: serde_json::Value = serde_json::from_str(&lines[0]).unwrap();
                assert_eq!(value["nan"], "NaN");
                assert_eq!(value["pos"], "+Inf");
                assert_eq!(value["neg"], "-Inf");
                assert_eq!(value["small"].as_f64(), Some(1e-8));
                assert_eq!(value["large"].as_f64(), Some(1e22));
                assert!(lines[0].contains("\"negative_zero\":-0"));
            } else {
                assert!(lines[0].contains("\"nan\": \"NaN\""));
                assert!(lines[0].contains("\"pos\": \"+Inf\""));
                assert!(lines[0].contains("\"negative_zero\": -0"));
            }
            assert!(lines[0].contains("0.00000001"));
            assert!(lines[0].contains("10000000000000000000000"));
        }
    }
    #[test]
    #[ignore = "requires fresh actual fixed protocol logger JSON stderr; LOGGING_TYPED_GO_RESULTS"]
    fn actual_fixed_go_typed_field_rows() {
        use tracing_subscriber::prelude::*;
        let _guard = POLICY_TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let path = std::env::var("LOGGING_TYPED_GO_RESULTS").expect("fresh original logger output");
        let rows = std::fs::read_to_string(path)
            .unwrap()
            .lines()
            .map(|line| serde_json::from_str::<serde_json::Value>(line).unwrap())
            .collect::<Vec<_>>();
        assert_eq!(rows.len(), 2, "exact original logger event count");
        set_json(true);
        set_time_source(|| "1".into());
        let output = std::sync::Arc::new(Mutex::new(Vec::new()));
        let registry = tracing_subscriber::registry().with(CaptureTypedEvents(output.clone()));
        let error = std::io::Error::other("source error text");
        let as_error: &(dyn std::error::Error + 'static) = &error;
        tracing::subscriber::with_default(registry, || {
            tracing::info!(signed=-12i64,unsigned=u64::MAX,ratio=0.125f64,ready=true,text="line\nquoted",debug=?vec![1,2],fault=as_error,"typed native event");
            tracing::info!(
                nan = f64::NAN,
                pos = f64::INFINITY,
                neg = f64::NEG_INFINITY,
                small = 1e-8f64,
                large = 1e22f64,
                negative_zero = -0.0f64,
                "float native event"
            );
        });
        let lines = output.lock().unwrap();
        assert_eq!(lines.len(), 2);
        for (go, rust) in rows.into_iter().zip(lines.iter()) {
            let mut go = go.as_object().expect("Go JSON log").clone();
            let mut rust = serde_json::from_str::<serde_json::Value>(rust)
                .unwrap()
                .as_object()
                .unwrap()
                .clone();
            // Runtime/caller identity differs between languages, field values do not.
            for key in ["ts", "caller"] {
                go.remove(key);
                rust.remove(key);
            }
            assert_eq!(go, rust);
        }
    }
}
