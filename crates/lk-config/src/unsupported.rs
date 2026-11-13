//! CLI-C08: configuration that would enable a capability this build does not
//! implement fails loudly, naming the YAML path, instead of being accepted and
//! silently ignored. Checks look at effective values (defaults and field
//! combinations), so a disabled field copied from a template is not reported.
//!
//! Error format: `unsupported configuration "<yaml.path>": <reason>`.

use crate::error::ConfigError;
use crate::official::Config;

/// One unsupported capability.
pub(crate) struct Rule {
    pub path: &'static str,
    pub active: fn(&Config) -> bool,
    pub reason: &'static str,
}

/// Fatal: the configuration enables something this build cannot do.
pub(crate) const UNSUPPORTED: &[Rule] = &[
    // SPEC-0074 C11: `redis.sentinel_addresses` is now consumed by the server
    // (real master discovery, authentication and failover), so it is no longer
    // rejected here.
    // SPEC-0074 C12: `redis.cluster_addresses` is now consumed via
    // `ClusterConnection` (slot-aware routing, MOVED/ASK handling).
    // Upstream order Sentinel > Cluster > Simple is preserved in the factory.
    // SPEC-0074 C20: `rtc.enable_warp` is now consumed by the media engine — it
    // opts every non-one-shot peer connection into SNAP (`a=sctp-init`) and
    // authenticated DTLS-in-STUN. New SPED acceptance is tracked separately.
];

/// Node selectors accepted by upstream (`selector/interfaces.go:33-75`).
const KNOWN_SELECTORS: &[&str] = &["", "any", "cpuload", "sysload", "regionaware", "random"];

/// A read-only node listing does not activate media/server features. Keep
/// connection features explicit rather than rejecting unrelated server config.
pub(crate) fn check_node_listing_transport(cfg: &Config) -> Result<(), ConfigError> {
    // list-nodes dials through the same Redis endpoint as the server
    // (`cli_redis_retry::read_nodes`), so TLS and Sentinel topologies are
    // supported transports as of SPEC-0074 C10/C11. `redis.pool_size` /
    // `redis.pool_timeout` remain consumed by the plaintext listing client
    // (SPEC-0074 C13).
    for rule in UNSUPPORTED
        .iter()
        .filter(|rule| rule.path.starts_with("redis."))
    {
        if (rule.active)(cfg) {
            return Err(ConfigError::unsupported(rule.path, rule.reason));
        }
    }
    Ok(())
}

/// SPEC-0074 C24: mirror `selector.CreateNodeSelector`
/// (pkg/routing/selector/interfaces.go:33-75). Upstream builds it in
/// `NewRoomAllocator` (service/roomallocator.go:43) from `InitializeServer`
/// (wire_gen.go:64) — after `getConfig`, `ValidateKeys` and `LoadTURNSecrets`,
/// so a bad kind is not a config-load error. A construction error surfaces as
/// `fmt.Println(err)` + `os.Exit(1)` (cmd/server/main.go:190-192), so the message
/// is bare upstream text, not an `unsupported configuration` wrapper.
pub(crate) fn check_selector(cfg: &Config) -> Result<(), ConfigError> {
    let kind = cfg.node_selector.kind.as_str();
    if !KNOWN_SELECTORS.contains(&kind) {
        // ErrUnsupportedSelector (interfaces.go:26)
        return Err(ConfigError::msg("unsupported node selector"));
    }
    if kind == "regionaware" {
        // Only `regionaware` validates at construction
        // (`NewRegionAwareSelector`, regionaware.go:35-67): a blank current
        // region (the top-level `region`, not `node_selector.*`) is fatal, and a
        // non-empty `regions` list omitting the current region is fatal.
        if cfg.region.is_empty() {
            // ErrCurrentRegionNotSet (errors.go:21)
            return Err(ConfigError::msg("current region cannot be blank"));
        }
        if !cfg.node_selector.regions.is_empty()
            && !cfg
                .node_selector
                .regions
                .iter()
                .any(|r| r.name == cfg.region)
        {
            // ErrCurrentRegionUnknownLatLon (errors.go:22)
            return Err(ConfigError::msg(
                "unknown lat and lon for the current region",
            ));
        }
    }
    Ok(())
}

pub(crate) fn check(cfg: &Config) -> Result<(), ConfigError> {
    for rule in UNSUPPORTED {
        if (rule.active)(cfg) {
            return Err(ConfigError::unsupported(rule.path, rule.reason));
        }
    }

    for (name, preset) in &cfg.room.room_configurations {
        let Some(preset) = preset else { continue };
        if preset
            .egress
            .as_ref()
            .is_some_and(|e| e.unsupported_oneof())
        {
            return Err(ConfigError::unsupported(
                format!("room.room_configurations.{name}.egress"),
                "multiple fields set for protobuf YAML oneof",
            ));
        }
    }

    // prometheus basic auth with only one half (`server.go:153-167`)
    let prom_port = if cfg.prometheus_port > 0 {
        cfg.prometheus_port
    } else {
        cfg.prometheus.port
    };
    if prom_port > 0 && (cfg.prometheus.username.is_empty() != cfg.prometheus.password.is_empty()) {
        return Err(ConfigError::msg(
            "prometheus username or password is set but not both, set both or nothing for unauthenticated access",
        ));
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::official::RegionConfig;

    #[test]
    fn redis_tls_is_consumed_by_the_server_but_not_the_node_listing() {
        let mut c = Config::default();
        c.redis.address = "localhost:6379".into();
        assert!(check(&c).is_ok());
        // SPEC-0074 C10: the server runs a real TLS client, so `check` accepts it.
        c.redis.use_tls = true;
        assert!(check(&c).is_ok());
        c.redis.use_tls = false;
        c.redis.tls = Some(crate::official::TlsConfig {
            enabled: true,
            ..Default::default()
        });
        assert!(check(&c).is_ok());
        // SPEC-0074 C10: the read-only node listing also dials through the real
        // TLS endpoint, so it accepts TLS now instead of rejecting it.
        assert!(check_node_listing_transport(&c).is_ok());
    }

    #[test]
    fn redis_pool_tuning_is_consumed_by_both_modes() {
        // SPEC-0074 C13: the `Pool` consumes pool_size/pool_timeout on the
        // server startup Ping and the node listing, so neither is rejected.
        let mut c = Config::default();
        c.redis.address = "localhost:6379".into();
        c.redis.pool_size = 25;
        c.redis.pool_timeout = crate::GoDuration::from_millis(3000);
        assert!(check(&c).is_ok());
        assert!(check_node_listing_transport(&c).is_ok());
    }

    #[test]
    fn cluster_sentinel_precedence_and_acceptance() {
        // SPEC-0074 C12: cluster is now fully supported. Upstream order
        // Sentinel > Cluster > Simple is handled by the factory.
        let mut c = Config::default();
        c.redis.cluster_addresses = vec!["c:3".into()];
        assert!(check(&c).is_ok(), "cluster alone is now supported");
        assert!(check_node_listing_transport(&c).is_ok());
        c.redis.sentinel_addresses = vec!["a:1".into(), "b:2".into()];
        c.redis.sentinel_master_name = "m".into();
        assert!(check(&c).is_ok(), "sentinel takes precedence; must start");
        assert!(
            check_node_listing_transport(&c).is_ok(),
            "listing dials the same sentinel endpoint"
        );
    }

    #[test]
    fn selector_and_prometheus() {
        let mut c = Config::default();
        c.node_selector.kind = "bogus".into();
        // Upstream rejects an unknown kind in `CreateNodeSelector` during
        // `InitializeServer`, not in `getConfig`, so config load must accept it.
        assert!(check(&c).is_ok(), "config load does not build the selector");
        assert_eq!(
            check_selector(&c).unwrap_err().to_string(),
            "unsupported node selector"
        );
        // SPEC-0074 C24: cpuload/sysload/random/any/"" construct without
        // validation upstream and are never invoked in a single standalone
        // node, so they must start (not be rejected).
        c.node_selector.kind = "cpuload".into();
        assert!(check_selector(&c).is_ok(), "cpuload must start in standalone");
        c.node_selector.kind = "sysload".into();
        assert!(check_selector(&c).is_ok(), "sysload must start in standalone");
        c.node_selector.kind = "random".into();
        assert!(check_selector(&c).is_ok(), "random must start in standalone");
        let mut c = Config::default();
        c.prometheus.port = 9100;
        c.prometheus.username = "u".into();
        assert!(
            check(&c)
                .unwrap_err()
                .to_string()
                .starts_with("prometheus username or password")
        );
        // without a port the half-auth is not checked (no listener)
        c.prometheus.port = 0;
        assert!(check(&c).is_ok());
    }

    #[test]
    fn regionaware_selector_validates_current_region_at_startup() {
        // SPEC-0074 C24: regionaware is the only kind that validates at
        // construction (interfaces.go:54 -> regionaware.go:35-67).
        // blank top-level `region` -> ErrCurrentRegionNotSet.
        let mut c = Config::default();
        c.node_selector.kind = "regionaware".into();
        assert!(check(&c).is_ok(), "config load does not build the selector");
        assert_eq!(
            check_selector(&c).unwrap_err().to_string(),
            "current region cannot be blank"
        );

        // region set with an empty regions list -> constructs fine (the
        // unknown-lat/lon error only fires when `regions` is non-empty).
        let mut c = Config::default();
        c.node_selector.kind = "regionaware".into();
        c.region = "US-east".into();
        assert!(
            check_selector(&c).is_ok(),
            "region with empty regions list must start"
        );

        // region omitted from a non-empty regions list -> ErrCurrentRegionUnknownLatLon.
        let mut c = Config::default();
        c.node_selector.kind = "regionaware".into();
        c.region = "US-west".into();
        c.node_selector.regions = vec![RegionConfig {
            name: "US-east".into(),
            ..Default::default()
        }];
        assert_eq!(
            check_selector(&c).unwrap_err().to_string(),
            "unknown lat and lon for the current region"
        );

        // region matches a configured region name -> starts.
        let mut c = Config::default();
        c.node_selector.kind = "regionaware".into();
        c.region = "US-east".into();
        c.node_selector.regions = vec![RegionConfig {
            name: "US-east".into(),
            ..Default::default()
        }];
        assert!(check_selector(&c).is_ok(), "matching region must start");
    }

    #[test]
    fn c10_rtc_network_keys_are_accepted_and_the_rejection_gate_is_alive() {
        // SPEC-0077 C10: the f62 "reject config" anchors (unsupported.rs:62
        // use_mdns, :67 force_tcp) are stale. The UNSUPPORTED table is empty, so
        // these four RTC ICE/network keys are accepted at config load and consumed
        // downstream (force_tcp: server.rs:333 + interfaces.rs:343; use_mdns:
        // room_task.rs:347; batch_io: server.rs:360 + net/batch_tx.rs;
        // external_ip_only: nat1to1.rs:244), matching the fixed Go contract
        // (webrtc_config.go UseMDNS:85, ForceTCP:123, BatchIO:146, ExternalIPOnly:402)
        // which consumes rather than rejects them.
        let mut c = Config::default();
        c.rtc.force_tcp = true;
        assert!(check(&c).is_ok(), "force_tcp must be accepted, not rejected");
        c.rtc.force_tcp = false;
        c.rtc.use_mdns = true;
        assert!(check(&c).is_ok(), "use_mdns must be accepted, not rejected");
        c.rtc.use_mdns = false;
        c.rtc.external_ip_only = true;
        assert!(
            check(&c).is_ok(),
            "external_ip_only must be accepted, not rejected"
        );
        c.rtc.external_ip_only = false;
        c.rtc.batch_io.batch_size = 64;
        c.rtc.batch_io.max_flush_interval = crate::GoDuration::from_millis(1);
        assert!(
            check(&c).is_ok(),
            "nonzero batch_io must be accepted, not rejected"
        );

        // Failability / positive control: `check` is not a constant-Ok stub. The
        // prometheus half-auth rule still rejects, so the acceptances above are
        // meaningful — a gutted checker would fail this arm. This is the same gate
        // the stale f62 anchors claimed rejected the RTC keys.
        let mut bad = Config::default();
        bad.prometheus.port = 9100;
        bad.prometheus.username = "u".into();
        assert!(
            check(&bad).is_err(),
            "the rejection gate must still fire for a genuinely unsupported config"
        );
    }
}
