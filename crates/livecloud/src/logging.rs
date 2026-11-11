//! Stderr logging with literal, hierarchical component thresholds, rendered
//! through the zap-compatible encoder (`crate::zap`, SPEC-0074 C32).
//!
//! Policy source: protocol/logger/config.go ResolveComponentLevel and
//! logger.go ParseZapLevel; Apache-2.0, retained in references/protocol-stable.
//! Native targets below are explicit backend equivalents, not Go log names.
use crate::log_sampling::Sampler;
use crate::zap::{self, ZapValue};
use rtc_core::config::LoggingConfig;
use std::collections::BTreeMap;
use std::sync::{Arc, OnceLock};
use std::time::{Duration, Instant};
use tracing::{Event, Level, Subscriber};
#[path = "logging_fields.rs"]
pub(crate) mod fields;
use tracing_log::NormalizeEvent;
use tracing_subscriber::{
    Layer,
    layer::{Context, Filter},
    prelude::*,
    registry::LookupSpan,
};

static ACTIVE: OnceLock<Arc<ComponentFilter>> = OnceLock::new();
static SAMPLING: OnceLock<Arc<SamplingPolicy>> = OnceLock::new();

struct SamplingPolicy {
    root: Option<Sampler>,
    item_seconds: i64,
    item_first: i64,
    item_interval: i64,
    epoch: Instant,
}
impl SamplingPolicy {
    fn new(cfg: &LoggingConfig) -> Self {
        // protocol/logger/logger.go:205-215 intentionally consumes ITEM values
        // here, despite the Config comments mentioning SampleInitial/Interval.
        Self {
            root: cfg.sample.then(|| {
                Sampler::new(
                    Duration::from_secs(1),
                    if cfg.item_sample_initial == 0 {
                        20
                    } else {
                        cfg.item_sample_initial
                    },
                    if cfg.item_sample_interval == 0 {
                        100
                    } else {
                        cfg.item_sample_interval
                    },
                )
            }),
            item_seconds: cfg.item_sample_seconds,
            item_first: cfg.item_sample_initial,
            item_interval: cfg.item_sample_interval,
            epoch: Instant::now(),
        }
    }
    fn room_sampler(&self) -> Option<Arc<Sampler>> {
        (self.item_seconds != 0).then(|| {
            // Go time.Duration multiplication wraps signed 64 bits. Nonpositive
            // windows reset on every event; do not clamp the count settings.
            let nanos = self.item_seconds.wrapping_mul(1_000_000_000);
            Arc::new(Sampler::new(
                Duration::from_nanos(nanos.max(0) as u64),
                self.item_first,
                self.item_interval,
            ))
        })
    }
    fn allows(&self, rank: i8, message: &str) -> bool {
        self.root
            .as_ref()
            .is_none_or(|s| s.allows(rank, message, self.epoch.elapsed()))
    }
}
struct RoomSampler(Arc<Sampler>);

#[derive(Clone, Debug)]
struct ComponentFilter {
    root: i8,
    components: BTreeMap<String, i8>,
}

// zap.UnmarshalText accepts case-insensitive names, without whitespace trimming.
// DPanic/Panic/Fatal exceed ordinary error severity; they must not enable ERROR.
fn level(name: &str) -> i8 {
    match name.to_ascii_lowercase().as_str() {
        "debug" => -1,
        "warn" | "warning" => 1,
        "error" => 2,
        "dpanic" => 3,
        "panic" => 4,
        "fatal" => 5,
        _ => 0,
    }
}

fn component(target: &str) -> &str {
    if let Some(name) = target.strip_prefix("livekit.component.") {
        return name;
    }
    // Upstream loggers already named for zap emit as the root logger; the
    // literal `livekit` component carries no sub-name segment.
    if target == "livekit" {
        return "";
    }
    if target == "is" || target.starts_with("is::") {
        return "transport.pion.ice";
    }
    if target == "dimpl" || target.starts_with("dimpl::") {
        return "transport.pion.dtls";
    }
    if target.starts_with("str0m::bwe") {
        return "transport.cc";
    }
    if target == "str0m" || target.starts_with("str0m::") {
        return "transport.pion";
    }
    if target.starts_with("rtc_runtime::net::tcp") {
        return "pion.tcp_mux";
    }
    // Other producers retain literal targets. Do not invent component ownership
    // for a whole module containing logs from several upstream components.
    target
}

impl ComponentFilter {
    fn new(cfg: &LoggingConfig) -> Self {
        Self {
            root: level(&cfg.level),
            components: cfg
                .component_levels
                .iter()
                .map(|(key, value)| (key.clone(), level(value)))
                .collect(),
        }
    }
    fn threshold(&self, mut name: &str) -> i8 {
        loop {
            if let Some(value) = self.components.get(name) {
                return *value;
            }
            let Some((parent, _)) = name.rsplit_once('.') else {
                return self.root;
            };
            name = parent;
        }
    }
    fn allows(&self, target: &str, severity: &Level) -> bool {
        let name = component(target);
        let rank = match *severity {
            Level::ERROR => 2,
            Level::WARN => 1,
            Level::INFO => 0,
            Level::DEBUG => -1,
            // Pion's Trace/Tracef adapter deliberately calls Debugw.
            _ if name == "pion"
                || name.starts_with("pion.")
                || name.starts_with("transport.pion") =>
            {
                -1
            }
            _ => -2,
        };
        rank >= self.threshold(name)
    }
}
impl<S: Subscriber> Filter<S> for ComponentFilter {
    fn enabled(&self, metadata: &tracing::Metadata<'_>, _: &Context<'_, S>) -> bool {
        // A room logger's lifetime must exist even when INFO is filtered out.
        if metadata.is_span() && metadata.name() == "livekit.room_logger" {
            return true;
        }
        // log events carry the original target in fields, not static metadata.
        metadata.target() == "log" || self.allows(metadata.target(), metadata.level())
    }
    fn event_enabled(&self, event: &Event<'_>, _: &Context<'_, S>) -> bool {
        if let Some(metadata) = event.normalized_metadata() {
            self.allows(metadata.target(), metadata.level())
        } else {
            self.allows(event.metadata().target(), event.metadata().level())
        }
    }
}

impl ComponentFilter {
    fn rank(severity: &Level) -> i8 {
        match *severity {
            Level::ERROR => 2,
            Level::WARN => 1,
            Level::INFO => 0,
            Level::DEBUG => -1,
            _ => -2,
        }
    }
}

/// zap logger name for a tracing target: root `livekit`, components dotted.
fn zap_name(target: &str) -> String {
    let name = component(target);
    if name.is_empty() {
        "livekit".to_string()
    } else {
        format!("livekit.{name}")
    }
}

/// Decision used by direct emissions (startup lines) and the tracing layer.
pub fn enabled(target: &str, severity: &Level) -> bool {
    ACTIVE
        .get()
        .is_some_and(|filter| filter.allows(target, severity))
}

/// Renders every tracing event through the zap encoder (console or JSON).
struct ZapLayer {
    sampling: Arc<SamplingPolicy>,
}

impl<S> Layer<S> for ZapLayer
where
    S: Subscriber + for<'a> LookupSpan<'a>,
{
    fn on_new_span(
        &self,
        attrs: &tracing::span::Attributes<'_>,
        id: &tracing::span::Id,
        ctx: Context<'_, S>,
    ) {
        if attrs.metadata().name() == "livekit.room_logger" {
            if let (Some(span), Some(sampler)) = (ctx.span(id), self.sampling.room_sampler()) {
                span.extensions_mut().insert(RoomSampler(sampler));
            }
        }
    }
    fn on_event(&self, event: &Event<'_>, ctx: Context<'_, S>) {
        let borrowed;
        let metadata = match event.normalized_metadata() {
            Some(normalized) => {
                borrowed = normalized;
                &borrowed
            }
            None => event.metadata(),
        };
        let target = metadata.target();
        let rank = ComponentFilter::rank(metadata.level());
        let name = zap_name(target);
        // tracing locations are `file:line` of the emitting macro; startup lines
        // bypass this layer via `notify` and carry upstream Go callers instead.
        let caller = format!(
            "{}:{}",
            metadata.file().unwrap_or("<unknown>"),
            metadata.line().unwrap_or(0)
        );
        let collect = fields::EventFields::collect(event);
        {
            let policy = &self.sampling;
            let room = ctx.event_scope(event).and_then(|mut scope| {
                scope.find_map(|span| span.extensions().get::<RoomSampler>().map(|s| s.0.clone()))
            });
            let allowed = room.map_or_else(
                || policy.allows(rank, &collect.message),
                |s| s.allows(rank, &collect.message, policy.epoch.elapsed()),
            );
            if !allowed {
                return;
            }
        }
        zap::emit(
            rank,
            &name,
            &caller,
            &collect.message,
            &collect
                .fields
                .iter()
                .map(|(k, v)| (k.as_str(), v.clone()))
                .collect::<Vec<_>>(),
            None,
            true,
        );
    }
}

pub fn init(cfg: &LoggingConfig) {
    let filter = ComponentFilter::new(cfg);
    let sampling = Arc::new(SamplingPolicy::new(cfg));
    let _ = SAMPLING.set(sampling.clone());
    let _ = ACTIVE.set(Arc::new(filter.clone()));
    zap::set_json(cfg.json);
    // tracing-log feature installs the one global LogTracer. Native log-crate
    // producers must pass the same policy as tracing events.
    let result = tracing_subscriber::registry()
        .with(ZapLayer { sampling }.with_filter(filter))
        .try_init();
    if let Err(error) = result {
        eprintln!("could not initialise logging: {error}");
    }
}

/// `cmd/server/main.go:208-214`: once the logger exists, a development-mode
/// configuration logs its INFO notices. `getConfig` runs for every command, not
/// only the server, so `list-nodes` prints the same pair before it dials Redis.
pub fn replay_config_notices(notices: &[String]) {
    for n in notices {
        let (caller, fields): (&str, Vec<(&str, ZapValue)>) = match n.as_str() {
            "no keys provided, using placeholder keys (API Key: devkey, API Secret: secret)" => (
                "cmd/server/main.go:214",
                vec![
                    ("API Key", ZapValue::Str("devkey".into())),
                    ("API Secret", ZapValue::Str("secret".into())),
                ],
            ),
            _ => ("cmd/server/main.go:211", Vec::new()),
        };
        let msg = if fields.is_empty() {
            n.as_str()
        } else {
            "no keys provided, using placeholder keys"
        };
        notify(crate::zap::INFO, "livekit", caller, msg, &fields, None);
    }
}

/// Emit one startup line directly through the zap encoder, bypassing tracing
/// (SPEC-0074 C32): `target` selects the component threshold, exactly like the
/// tracing layer would, but field order and rendering stay under our control.
pub fn notify(
    rank: i8,
    target: &str,
    caller: &str,
    msg: &str,
    fields: &[(&str, ZapValue)],
    stack: Option<&str>,
) {
    let severity = match rank {
        2 => Level::ERROR,
        1 => Level::WARN,
        -1 => Level::DEBUG,
        _ => Level::INFO,
    };
    if !enabled(target, &severity) {
        return;
    }
    if SAMPLING
        .get()
        .is_some_and(|policy| !policy.allows(rank, msg))
    {
        return;
    }
    zap::emit(rank, &zap_name(target), caller, msg, fields, stack, true);
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn fixed_go_root_uses_item_fields_and_defaults_not_global_named_fields() {
        let cfg = LoggingConfig {
            sample: true,
            sample_initial: 1,
            sample_interval: 1,
            ..LoggingConfig::default()
        };
        let policy = SamplingPolicy::new(&cfg);
        let s = policy.root.as_ref().unwrap();
        let t = Duration::from_secs(1);
        assert!((0..20).all(|_| s.allows(0, "same", t)));
        assert!((0..99).all(|_| !s.allows(0, "same", t)));
        assert!(s.allows(0, "same", t)); // 120th = 20 + 100
        let p = SamplingPolicy::new(&LoggingConfig {
            sample: true,
            item_sample_initial: 1,
            item_sample_interval: 2,
            ..LoggingConfig::default()
        });
        let s = p.root.as_ref().unwrap();
        assert!(s.allows(0, "same", t));
        assert!(!s.allows(0, "same", t));
        assert!(s.allows(0, "same", t));
        let disabled = SamplingPolicy::new(&LoggingConfig::default());
        assert!(disabled.root.is_none());
        assert!(disabled.room_sampler().is_none());
    }
    #[test]
    fn room_sampler_replaces_root_has_raw_zero_defaults_and_generation_ownership() {
        let policy = SamplingPolicy::new(&LoggingConfig {
            sample: false,
            item_sample_seconds: 3,
            item_sample_initial: 1,
            item_sample_interval: 0,
            ..LoggingConfig::default()
        });
        assert!(policy.root.is_none()); // item opt-in is independent of sample
        let a = policy.room_sampler().unwrap();
        let b = policy.room_sampler().unwrap();
        let t = Duration::from_secs(10);
        assert!(a.allows(0, "same", t));
        assert!(!a.allows(0, "same", t));
        assert!(b.allows(0, "same", t));
        let clone = a.clone();
        assert!(!clone.allows(0, "same", Duration::from_millis(12_999)));
        assert!(clone.allows(0, "same", Duration::from_secs(13)));
        let zero = SamplingPolicy::new(&LoggingConfig {
            item_sample_seconds: 1,
            ..LoggingConfig::default()
        });
        assert!(!zero.room_sampler().unwrap().allows(0, "same", t));
        let negative = SamplingPolicy::new(&LoggingConfig {
            item_sample_seconds: -1,
            item_sample_initial: 1,
            ..LoggingConfig::default()
        });
        let s = negative.room_sampler().unwrap();
        assert!(s.allows(0, "same", t));
        assert!(s.allows(0, "same", t));
    }
    #[test]
    fn room_lifetime_is_explicit_and_not_filtered_by_info_threshold() {
        let cfg = LoggingConfig {
            level: "error".into(),
            item_sample_seconds: 1,
            item_sample_initial: 1,
            ..LoggingConfig::default()
        };
        let registry = tracing_subscriber::registry().with(
            ZapLayer {
                sampling: Arc::new(SamplingPolicy::new(&cfg)),
            }
            .with_filter(ComponentFilter::new(&cfg)),
        );
        let registry = Arc::new(registry);
        tracing::subscriber::with_default(registry.clone(), || {
            let a = tracing::info_span!("livekit.room_logger");
            let b = tracing::info_span!("livekit.room_logger");
            let state = |span: &tracing::Span| {
                registry
                    .span(&span.id().unwrap())
                    .unwrap()
                    .extensions()
                    .get::<RoomSampler>()
                    .unwrap()
                    .0
                    .clone()
            };
            let a_state = state(&a);
            let b_state = state(&b);
            let now = Duration::from_secs(1);
            assert!(a_state.allows(0, "same", now));
            assert!(!a_state.allows(0, "same", now));
            assert!(b_state.allows(0, "same", now));
            // Mere event fields named room/participant are never consulted.
            drop(a);
            let recreated = tracing::info_span!("livekit.room_logger");
            assert!(state(&recreated).allows(0, "same", now));
        });
    }
    #[test]
    #[ignore = "requires fresh real fixed-Go logger output; see tests/interop/logging-sampling"]
    fn actual_fixed_go_sampling_rows() {
        let path =
            std::env::var("LOGGING_SAMPLING_GO_RESULTS").expect("LOGGING_SAMPLING_GO_RESULTS");
        let rows = std::fs::read_to_string(path).unwrap();
        let mut count = 0;
        for line in rows.lines().filter(|s| !s.is_empty()) {
            let row: serde_json::Value = serde_json::from_str(line).unwrap();
            let policy = SamplingPolicy::new(&LoggingConfig {
                sample: row["sample"].as_bool().unwrap(),
                sample_initial: row["named_initial"].as_i64().unwrap(),
                sample_interval: row["named_interval"].as_i64().unwrap(),
                item_sample_initial: row["initial"].as_i64().unwrap(),
                item_sample_interval: row["interval"].as_i64().unwrap(),
                item_sample_seconds: row["seconds"].as_i64().unwrap(),
                ..LoggingConfig::default()
            });
            let room = row["room"]
                .as_bool()
                .unwrap()
                .then(|| policy.room_sampler().unwrap());
            let sampler = room.as_deref().or(policy.root.as_ref());
            let accepted: Vec<_> = (1..=row["count"].as_u64().unwrap())
                .filter(|_| sampler.is_none_or(|s| s.allows(0, "same", Duration::from_secs(1))))
                .collect();
            assert_eq!(
                serde_json::json!(accepted),
                row["accepted"],
                "{}",
                row["id"]
            );
            count += 1;
        }
        assert_eq!(count, 8);
    }
    fn policy(root: &str, pairs: &[(&str, &str)]) -> ComponentFilter {
        ComponentFilter::new(&LoggingConfig {
            level: root.into(),
            json: true,
            component_levels: pairs
                .iter()
                .map(|(k, v)| ((*k).into(), (*v).into()))
                .collect(),
            ..LoggingConfig::default()
        })
    }
    #[test]
    fn literal_component_keys_cannot_change_root_policy() {
        let p = policy("error", &[("bad target,[x]=debug", "debug")]);
        assert!(!p.allows("livecloud", &Level::INFO));
        assert!(p.allows("livecloud", &Level::ERROR));
    }
    #[test]
    fn closest_dotted_parent_and_native_log_targets_are_consumed() {
        let p = policy(
            "error",
            &[
                ("transport.pion", "debug"),
                ("transport.pion.dtls", "fatal"),
            ],
        );
        assert!(p.allows("is::agent", &Level::DEBUG));
        assert!(p.allows("is::agent", &Level::TRACE));
        assert!(!p.allows("dimpl::dtls", &Level::ERROR));
        assert!(!p.allows("transport.pionish", &Level::DEBUG));
    }
    #[test]
    fn invalid_levels_fall_back_to_info_without_alias_or_trimming() {
        for value in [" debug", "trace", "off"] {
            assert_eq!(level(value), 0);
        }
        assert_eq!(level("DeBuG"), -1);
        assert_eq!(level("WARNING"), 1);
        for value in ["dpanic", "panic", "fatal"] {
            assert!(!policy(value, &[]).allows("livecloud", &Level::ERROR));
        }
    }
    #[test]
    #[ignore = "requires actual fixed-Go logger resolver output"]
    fn actual_go_component_level_rows() {
        let path = std::env::var("LOGGING_GO_RESULTS").expect("LOGGING_GO_RESULTS");
        let lines = std::fs::read_to_string(path).unwrap();
        let mut count = 0;
        for line in lines.lines().filter(|line| !line.is_empty()) {
            let row: serde_json::Value = serde_json::from_str(line).unwrap();
            let pairs: BTreeMap<String, String> =
                serde_json::from_value(row["components"].clone()).unwrap();
            let p = ComponentFilter::new(&LoggingConfig {
                level: row["level"].as_str().unwrap().into(),
                json: true,
                component_levels: pairs,
                ..LoggingConfig::default()
            });
            assert_eq!(
                i64::from(p.threshold(row["component"].as_str().unwrap())),
                row["threshold"].as_i64().unwrap(),
                "{row}"
            );
            count += 1;
        }
        assert_eq!(count, 42);
    }
}
