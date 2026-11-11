//! Node identity producer: `pkg/routing/node.go` plus the registration half of
//! `pkg/routing/redisrouter.go`. The storage primitives (HSET/HDEL/HVALS) live in
//! [`crate::room_control::registry`]; this module owns the *in-memory* node the
//! server registers, refreshes and drains, and the psrpc `Keepalive.Ping`
//! transport the refresh rides.
//!
//! Process wiring (which tasks hold these, what gets logged, when shutdown runs)
//! belongs to the binary, because that is where the upstream log lines and the
//! signal ordering live. Nothing here spawns a task or logs.

use crate::service_bus::channels::{self, Channel};
use crate::service_bus::redis_transport::{RedisTransport, Subscription, TransportError};
use crate::service_bus::wire;
use crate::workers::preserved::WireValue;
use lk_protocol::livekit as pb;
use lk_protocol::rpc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Mutex, MutexGuard};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

/// `config.NodeStatsConfig` fields the router actually consumes.
/// `stats_update_interval` drives both the ping tick and the ping-staleness gate
/// (`redisrouter.go:207,238`); `stats_max_delay` drives only the delayed-stats
/// diagnostic (`redisrouter.go:211-222`) and is *not* the availability cutoff
/// (`selector/utils.go:30` keeps `AvailableSeconds = 5`).
#[derive(Clone, Copy, Debug)]
pub struct NodeStatsPolicy {
    pub update_interval: Duration,
    pub max_delay: Duration,
}

/// Go `time.Since(time.Unix(updated_at, 0)).Seconds()` (`node.go:155-160`).
/// The sub-second part comes from `now`, because `UpdatedAt` is whole seconds.
pub fn seconds_since(now: SystemTime, updated_at: i64) -> f64 {
    let now_seconds = now
        .duration_since(UNIX_EPOCH)
        .map(|elapsed| elapsed.as_secs_f64())
        .unwrap_or_default();
    now_seconds - updated_at as f64
}

fn lock<T>(value: &Mutex<T>) -> MutexGuard<'_, T> {
    value
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

/// Runtime-owned producer; the compatibility layer does not depend on the
/// executable or use test fixture values as node capacity.
pub trait NodeStatsProvider: Send + Sync + std::fmt::Debug {
    fn poll_system(&self) -> Result<(), String>;
    fn sample(&self, started_at: i64, now_unix: i64) -> Result<pb::NodeStats, String>;
}

/// Go `routing.LocalNodeImpl` (`node.go:42-160`): the one node record this
/// process owns. `Clone()` (the `proto.Clone` the router marshals) is a snapshot,
/// so no Redis I/O ever runs while the node is locked.
#[derive(Debug)]
pub struct LocalNode {
    node: Mutex<pb::Node>,
    routing_blocked: AtomicBool,
    stats_provider: Mutex<Option<std::sync::Arc<dyn NodeStatsProvider>>>,
}

impl LocalNode {
    /// Go `NewLocalNode` (`node.go:51-78`). The id comes from the process's
    /// generated `ND_` identity, `ip` from the resolved `rtc.node_ip`
    /// (`PrimaryIP()` = V4 first, else V6 — `pkg/rtcconfig/config.go:280`),
    /// `num_cpus` from the runtime CPU count, `region` from `conf.Region`, and
    /// the state is `SERVING` with `stats.started_at == stats.updated_at == now`
    /// so a fresh node passes `selector.IsAvailable` immediately.
    pub fn serving(
        id: String,
        ip: String,
        region: String,
        num_cpus: u32,
        now_unix: i64,
    ) -> Result<Self, LocalNodeError> {
        if ip.is_empty() {
            // `node.go:53-55` returns `ErrIPNotSet`; discovery normally fills it.
            return Err(LocalNodeError::IpNotSet);
        }
        Ok(Self {
            routing_blocked: AtomicBool::new(false),
            stats_provider: Mutex::new(None),
            node: Mutex::new(pb::Node {
                id,
                ip,
                num_cpus,
                stats: Some(pb::NodeStats {
                    started_at: now_unix,
                    updated_at: now_unix,
                    ..Default::default()
                }),
                r#type: pb::NodeType::Server as i32,
                state: pb::NodeState::Serving as i32,
                region,
            }),
        })
    }

    pub fn set_stats_provider(&self, provider: std::sync::Arc<dyn NodeStatsProvider>) {
        *lock(&self.stats_provider) = Some(provider);
    }
    pub fn has_stats_provider(&self) -> bool {
        lock(&self.stats_provider).is_some()
    }
    pub fn poll_system_stats(&self) {
        if let Some(provider) = lock(&self.stats_provider).clone() {
            if let Err(error) = provider.poll_system() {
                tracing::error!(%error, "failed retrieving CPU idle");
            }
        }
    }

    pub fn id(&self) -> String {
        lock(&self.node).id.clone()
    }

    /// Go `LivekitServer.Node()` — `s.currentNode.Clone()`, read by the health
    /// handler and by whoever needs the current record.
    pub fn snapshot(&self) -> pb::Node {
        let mut node = lock(&self.node).clone();
        if self.routing_blocked.load(Ordering::SeqCst) {
            node.state = pb::NodeState::ShuttingDown as i32;
        }
        node
    }

    /// Quarantine room allocation independently of the server's drain state.
    /// Clearing a reconciliation failure must never undo an actual drain.
    pub fn set_routing_blocked(&self, blocked: bool) {
        self.routing_blocked.store(blocked, Ordering::SeqCst);
    }

    /// `SetState` (`node.go:120-131`). `Drain()` uses `SHUTTING_DOWN`
    /// (`redisrouter.go:185-190`).
    pub fn set_state(&self, state: i32) {
        lock(&self.node).state = state;
    }

    /// `UpdateNodeStats` (`node.go:142-153`). Upstream replaces the whole
    /// `Stats` from `prometheus.GetNodeStats` (gopsutil CPU/memory plus the
    /// rate-measurement ring); the counter port is
    /// `RUNTIME:Redis.NodeStatsSampling` and stays open. This advances the one
    /// field every documented consumer actually reads — `UpdatedAt`, which
    /// `selector.IsAvailable` (`selector/utils.go:30-41`), the startup sweep and
    /// the 4 s health check (`server.go:451-464`) all compare — and preserves
    /// `StartedAt` and whatever counters the record already carries.
    pub fn update_stats(&self, now_unix: i64) -> bool {
        let mut node = lock(&self.node);
        let started_at = node
            .stats
            .as_ref()
            .map(|stats| stats.started_at)
            .unwrap_or(now_unix);
        if let Some(provider) = lock(&self.stats_provider).clone() {
            match provider.sample(started_at, now_unix) {
                Ok(stats) => {
                    node.stats = Some(stats);
                    return true;
                }
                Err(error) => {
                    tracing::error!(%error, "could not update node stats");
                    return false;
                }
            }
        }
        let previous = node.stats.take();
        let mut stats = previous.unwrap_or(pb::NodeStats {
            started_at,
            ..Default::default()
        });
        stats.started_at = started_at;
        stats.updated_at = now_unix;
        node.stats = Some(stats);
        true
    }

    /// `SecondsSinceNodeStatsUpdate` (`node.go:155-160`). Upstream reads it
    /// without a nil check, so a stats-less record would panic; the seeded
    /// constructor means that never happens in a running server.
    pub fn seconds_since_stats_update(&self, now: SystemTime) -> f64 {
        let updated_at = lock(&self.node).stats.as_ref().map(|s| s.updated_at);
        seconds_since(now, updated_at.unwrap_or(0))
    }

    /// The exact value `proto.Marshal(r.currentNode.Clone())` produces
    /// (`redisrouter.go:70`), ready for the registry's HSET.
    pub fn wire_value(&self) -> WireValue<pb::Node> {
        WireValue::from_message(self.snapshot())
    }
}

/// Upstream compares against the literal `4*time.Second` in `healthCheck`
/// (`server.go:455`); it is *not* derived from `stats_update_interval`, so a
/// deployment whose interval exceeds 4 s can report "Not Ready" while healthy.
pub const HEALTH_MAX_STALE_SECONDS: f64 = 4.0;

/// Go `time.Time{}` — the value `healthCheck` uses when `Stats` is nil, and what
/// its `String()` renders.
pub const GO_ZERO_TIME_TEXT: &str = "0001-01-01 00:00:00 +0000 UTC";

/// The zone part Go's `time.Time.String()` appends: the numeric offset plus the
/// abbreviation of the location the value was created in. `time.Unix(u, 0)` is
/// always `Local`, so both come from the process's local zone *at that instant*
/// (a DST boundary can change them between two timestamps).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GoZone {
    pub offset_seconds: i32,
    pub name: String,
}

/// UTC is a real Go zone (`+0000 UTC`), and the fallback when the host C library
/// cannot classify the instant.
pub fn utc_zone() -> GoZone {
    GoZone {
        offset_seconds: 0,
        name: "UTC".to_string(),
    }
}

/// The local zone Go's `time.Local` would report for `time.Unix(unix, 0)`.
/// Go reads its own tzdata; this reads the C library's, which is the same system
/// database on the Linux/macOS hosts this build targets. A host where the two
/// disagree (embedded tzdata, `TZ` handled differently) is a visible log/body
/// difference, not something this function can hide.
#[cfg(unix)]
pub fn local_zone_at(unix: i64) -> GoZone {
    // SAFETY: `localtime_r` writes into the caller-provided `tm`, and `tm_zone`
    // is a pointer into static/tls tzdata that stays valid long enough to copy.
    unsafe {
        let when = unix as libc::time_t;
        let mut broken: libc::tm = std::mem::zeroed();
        if libc::localtime_r(&when, &mut broken).is_null() {
            return utc_zone();
        }
        // macOS types `tm_zone` as a bare `*mut c_char`, Linux as a pointer too;
        // a null abbreviation means the C library has no name for the zone.
        let name = if broken.tm_zone.is_null() {
            String::new()
        } else {
            std::ffi::CStr::from_ptr(broken.tm_zone)
                .to_string_lossy()
                .into_owned()
        };
        GoZone {
            offset_seconds: broken.tm_gmtoff as i32,
            name: if name.is_empty() {
                "UTC".to_string()
            } else {
                name
            },
        }
    }
}

#[cfg(not(unix))]
pub fn local_zone_at(_unix: i64) -> GoZone {
    utc_zone()
}

/// Howard Hinnant's `days_from_civil` inverse: days since 1970-01-01 to a
/// proleptic Gregorian date. Pure, so the date half of the rendering below has a
/// provable contract independent of the host zone.
fn civil_from_days(days: i64) -> (i64, u32, u32) {
    let shifted = days + 719_468;
    let era = if shifted >= 0 {
        shifted
    } else {
        shifted - 146_096
    } / 146_097;
    let day_of_era = (shifted - era * 146_097) as u64;
    let year_of_era =
        (day_of_era - day_of_era / 1460 + day_of_era / 36_524 - day_of_era / 146_096) / 365;
    let year = year_of_era as i64 + era * 400;
    let day_of_year = day_of_era - (365 * year_of_era + year_of_era / 4 - year_of_era / 100);
    let month_of_year = (5 * day_of_year + 2) / 153;
    let day = (day_of_year - (153 * month_of_year + 2) / 5 + 1) as u32;
    let month = if month_of_year < 10 {
        month_of_year + 3
    } else {
        month_of_year - 9
    } as u32;
    (if month <= 2 { year + 1 } else { year }, month, day)
}

/// Go `time.Unix(unix, 0).In(zone).String()`, i.e. the
/// `2006-01-02 15:04:05.999999999 -0700 MST` layout with the fractional second
/// dropped. `time.Unix` always has zero nanoseconds, and `healthCheck` only ever
/// formats such a value (or the zero `time.Time`), so the sub-second branch of
/// Go's layout is unreachable here.
pub fn go_time_string(unix: i64, zone: &GoZone) -> String {
    let local = unix + i64::from(zone.offset_seconds);
    let days = local.div_euclid(86_400);
    let seconds = local.rem_euclid(86_400);
    let (year, month, day) = civil_from_days(days);
    let (sign, year) = if year < 0 { ("-", -year) } else { ("", year) };
    let (offset_sign, offset) = if zone.offset_seconds < 0 {
        ('-', -i64::from(zone.offset_seconds))
    } else {
        ('+', i64::from(zone.offset_seconds))
    };
    format!(
        "{sign}{year:04}-{month:02}-{day:02} {:02}:{:02}:{:02} {offset_sign}{:02}{:02} {}",
        seconds / 3600,
        (seconds % 3600) / 60,
        seconds % 60,
        offset / 3600,
        (offset / 60) % 60,
        zone.name,
    )
}

/// `LivekitServer.healthCheck` (`server.go:450-464`) for one observed record.
/// `None` (no node record) and `stats == nil` are both Go's zero `time.Time`,
/// which always fails the 4 s comparison. The `time.Since` check keeps Go's
/// sub-second precision (`UpdatedAt` is whole seconds, so a record stamped in the
/// current second is never stale), and the 406 body is
/// `fmt.Fprintf(w, "Not Ready\nNode Updated At %s", updatedAt)`.
pub fn health_body(node: Option<&pb::Node>, now: SystemTime, zone: &GoZone) -> Option<String> {
    let Some(updated_at) = node
        .and_then(|node| node.stats.as_ref())
        .map(|s| s.updated_at)
    else {
        return Some(format!("Not Ready\nNode Updated At {GO_ZERO_TIME_TEXT}"));
    };
    if seconds_since(now, updated_at) > HEALTH_MAX_STALE_SECONDS {
        return Some(format!(
            "Not Ready\nNode Updated At {}",
            go_time_string(updated_at, zone)
        ));
    }
    None
}

#[derive(Debug, thiserror::Error)]
pub enum LocalNodeError {
    /// Go `routing.ErrIPNotSet` = "ip address is required and not set".
    #[error("ip address is required and not set")]
    IpNotSet,
}

/// The psrpc channel triple for a node's own keepalive topic, split out so the
/// naming contract is testable without a connection.
pub fn keepalive_channel(node_id: &str) -> Channel {
    channels::request("Keepalive", "Ping", &[node_id], false)
}

/// psrpc `Keepalive` publisher/subscriber pair (`rpc/keepalive.psrpc.go:78,112`).
/// `sd.RegisterMethod("Ping", false, true, false, false)` means
/// affinity=false, multi=true, requireClaim=false, **queue=false**, so
/// `GetRPCChannel()` is a plain pubsub channel and both halves use
/// `bus.Publish`/`bus.Subscribe` rather than a stream or a queue:
/// legacy `Keepalive|Ping|<nodeID>|REQ`, hierarchical `SRV.Keepalive.<nodeID>`,
/// local `Ping.REQ`. The topic is the publishing node's own id, so a ping only
/// ever refreshes the node that sent it.
pub struct KeepaliveBus {
    bus: std::sync::Arc<RedisTransport>,
    channel: Channel,
}

impl KeepaliveBus {
    pub fn new(bus: std::sync::Arc<RedisTransport>, node_id: &str) -> Self {
        Self {
            channel: keepalive_channel(node_id),
            bus,
        }
    }

    pub fn channel(&self) -> &Channel {
        &self.channel
    }

    /// `PublishPing(ctx, nodeID, &rpc.KeepalivePing{Timestamp: time.Now().Unix()})`.
    /// Upstream discards the returned error at the call site
    /// (`redisrouter.go:207`); the observable consequence is that the record
    /// simply ages past `AvailableSeconds`.
    pub async fn publish_ping(&self, timestamp: i64) -> Result<u64, TransportError> {
        self.bus
            .publish(&self.channel, &rpc::KeepalivePing { timestamp })
            .await
    }

    /// `SubscribePing(ctx, nodeID)` = `client.Join`, i.e. a non-queue
    /// subscription to the same channel.
    pub async fn join(&self) -> Result<Subscription, TransportError> {
        self.bus.subscribe(&self.channel, false).await
    }

    /// `Deserialize` reads `internal.Msg`, resolves `type_url` to the schema name
    /// after the last slash, decompresses under the configured cap, then lets the
    /// generated decoder apply Go's known-number/wrong-kind rule.
    pub fn decode_ping(&self, bytes: &[u8]) -> Result<rpc::KeepalivePing, wire::WireError> {
        let raw = wire::decode_raw::<rpc::KeepalivePing>(bytes, self.bus.limits().wire)?;
        lk_protocol::wire::decode::<rpc::KeepalivePing>(&raw.payload)
            .map_err(|error| wire::WireError::Payload(prost::DecodeError::new(error.to_string())))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use prost::Message;

    /// The channel triple is the wire contract a Go subscriber actually listens
    /// on. A node id is `ND_` plus base57 text, all of which escape to itself.
    #[test]
    fn keepalive_ping_channel_matches_psrpc_register_method() {
        let channel = keepalive_channel("ND_0123456789ab");
        assert_eq!(channel.legacy, "Keepalive|Ping|ND_0123456789ab|REQ");
        assert_eq!(channel.server, "SRV.Keepalive.ND_0123456789ab");
        // `RegisterMethod("Ping", false, true, false, false)` has queue=false, so
        // the hierarchical subject carries no `.Q` suffix.
        assert!(!channel.server.ends_with(".Q"));
        assert_eq!(channel.local, "Ping.REQ");
    }

    /// `pkg/bus/bus.go:79` serializes the business message itself, so the
    /// envelope names the schema type and stores `Channel.Local` — a Go
    /// subscriber resolves the payload by that name, not by an `internal.*` one.
    #[test]
    fn keepalive_ping_envelope_names_the_schema_type() {
        assert_eq!(
            <rpc::KeepalivePing as wire::BusMessage>::FULL_NAME,
            "rpc.KeepalivePing"
        );
        // `proto.Marshal(&rpc.KeepalivePing{Timestamp: 1730000000})`: the field-1
        // varint tag (0x08) plus the value in five continuation bytes.
        let ping = rpc::KeepalivePing {
            timestamp: 1_730_000_000,
        };
        assert_eq!(
            ping.encode_to_vec(),
            vec![0x08, 0x80, 0xe9, 0xf6, 0xb8, 0x06]
        );
        assert_eq!(ping.encoded_len(), 6);
    }

    #[test]
    fn serving_node_seeds_stats_so_it_is_immediately_available() {
        let node = LocalNode::serving(
            "ND_seed".into(),
            "203.0.113.7".into(),
            "sg".into(),
            8,
            1_730_000_000,
        )
        .expect("node");
        let snapshot = node.snapshot();
        assert_eq!(snapshot.state, pb::NodeState::Serving as i32);
        assert_eq!(snapshot.r#type, pb::NodeType::Server as i32);
        assert_eq!(snapshot.ip, "203.0.113.7");
        assert_eq!(snapshot.num_cpus, 8);
        assert_eq!(snapshot.region, "sg");
        let stats = snapshot.stats.as_ref().expect("seeded stats");
        assert_eq!(
            (stats.started_at, stats.updated_at),
            (1_730_000_000, 1_730_000_000)
        );
        // `selector.IsAvailable` accepts a record whose stamp is now.
        assert!(crate::room_control::registry::node_available(
            &snapshot,
            1_730_000_004
        ));
    }

    /// A node with no resolvable IP is the upstream hard error, not a record with
    /// an empty `ip` field.
    #[test]
    fn empty_node_ip_is_the_upstream_construction_error() {
        let error = LocalNode::serving("ND_x".into(), String::new(), String::new(), 1, 0)
            .expect_err("ip required");
        assert_eq!(error.to_string(), "ip address is required and not set");
    }

    /// `UpdateNodeStats` replaces the record's stamp every ping and never touches
    /// `StartedAt`, which is what makes the health window and the availability
    /// window move together.
    #[test]
    fn update_stats_refreshes_the_stamp_and_keeps_started_at() {
        let node = LocalNode::serving("ND_x".into(), "10.0.0.1".into(), String::new(), 4, 100)
            .expect("node");
        node.set_state(pb::NodeState::ShuttingDown as i32);
        node.update_stats(140);
        let snapshot = node.snapshot();
        let stats = snapshot.stats.as_ref().expect("stats");
        assert_eq!((stats.started_at, stats.updated_at), (100, 140));
        // Drain's state survives a refresh: `Drain()` sets it, the ping loop
        // re-HSETs the same record.
        assert_eq!(snapshot.state, pb::NodeState::ShuttingDown as i32);
        let now = UNIX_EPOCH + Duration::from_secs(143) + Duration::from_millis(500);
        assert!((node.seconds_since_stats_update(now) - 3.5).abs() < 1e-9);
    }

    /// Go's layout with the fraction dropped, for a zone the caller supplies, so
    /// these vectors hold whatever `TZ` the test runner has. Cross-checked against
    /// `time.Unix(u,0).String()` by the paired live differential.
    #[test]
    fn go_time_string_renders_the_stdlib_layout() {
        let utc = utc_zone();
        assert_eq!(go_time_string(0, &utc), "1970-01-01 00:00:00 +0000 UTC");
        // Hand-derived: 1_730_000_000 = 20023 days + 12_800 s since the epoch.
        assert_eq!(
            go_time_string(1_730_000_000, &utc),
            "2024-10-27 03:33:20 +0000 UTC"
        );
        let cst = GoZone {
            offset_seconds: 8 * 3600,
            name: "CST".to_string(),
        };
        assert_eq!(
            go_time_string(1_730_000_000, &cst),
            "2024-10-27 11:33:20 +0800 CST"
        );
        // A negative offset can cross the day boundary backwards.
        let est = GoZone {
            offset_seconds: -5 * 3600,
            name: "EST".to_string(),
        };
        assert_eq!(
            go_time_string(1_735_689_600, &est),
            "2024-12-31 19:00:00 -0500 EST"
        );
        // Half-hour and 5-digit-year offsets keep Go's padding.
        let ist = GoZone {
            offset_seconds: 30 * 60,
            name: "IST".to_string(),
        };
        assert_eq!(go_time_string(0, &ist), "1970-01-01 00:30:00 +0030 IST");
        // A five-digit year is not padded down to four.
        assert_eq!(
            go_time_string(253_402_300_800, &utc),
            "10000-01-01 00:00:00 +0000 UTC"
        );
    }

    /// The 4 s window is Go's literal `>`, compared with sub-second precision:
    /// exactly four seconds is still ready, a hair more is not.
    #[test]
    fn health_uses_the_literal_four_second_boundary() {
        let node = LocalNode::serving("ND_h".into(), "10.0.0.1".into(), String::new(), 1, 1000)
            .expect("node");
        let at = |nanos: u64| UNIX_EPOCH + Duration::from_nanos(nanos);
        let utc = utc_zone();
        assert_eq!(
            health_body(Some(&node.snapshot()), at(1_004_000_000_000), &utc),
            None
        );
        let stale = health_body(Some(&node.snapshot()), at(1_004_000_000_001), &utc)
            .expect("just past the literal 4s");
        assert_eq!(
            stale,
            "Not Ready\nNode Updated At 1970-01-01 00:16:40 +0000 UTC"
        );
        // A record with no stats at all is Go's zero `time.Time`, not "now".
        let mut bare = node.snapshot();
        bare.stats = None;
        assert_eq!(
            health_body(Some(&bare), at(2_000_000_000_000), &utc).expect("nil stats"),
            format!("Not Ready\nNode Updated At {GO_ZERO_TIME_TEXT}")
        );
        // No record at all is the same Go zero time.
        assert_eq!(
            health_body(None, at(2_000_000_000_000), &utc).expect("no node"),
            format!("Not Ready\nNode Updated At {GO_ZERO_TIME_TEXT}")
        );
    }

    /// `local_zone_at` is the only host-dependent piece of the 406 body. Its
    /// result is *not* asserted here (that would make the test depend on `TZ`);
    /// the live paired differential compares the rendered text on one host.
    #[cfg(unix)]
    #[test]
    fn local_zone_lookup_is_total_over_the_unix_range() {
        for unix in [0, 1_730_000_000, -1_000_000_000] {
            let zone = local_zone_at(unix);
            assert!(!zone.name.is_empty());
            assert!(zone.offset_seconds.abs() < 14 * 3600);
        }
    }
}
