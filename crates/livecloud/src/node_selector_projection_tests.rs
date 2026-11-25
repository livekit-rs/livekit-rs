//! C96-nsel config_projection legs for the nine `node_selector` leaves.
//!
//! Every leg drives a YAML / ENV / CLI document through the real loading path
//! (`lk_config::parse_with_probe`) and asserts the values that land in the
//! effective config the allocator actually consumes: the
//! `lk_compat::room_control::selector::Config` produced by
//! `allocation_selector_config` (server.rs:86-107), which is handed to
//! `state.enable_local_allocation_selector` (server.rs:455, standalone mode) and
//! to `ManagedConfig.selector` (server.rs:677, Redis/cluster mode). That function
//! is the only production YAML-to-selector projection in the port;
//! `crates/lk-config/src/normalize.rs` has no `node_selector` projection at all.
//!
//! Go oracle: references/livekit-stable @ e5655ac (v1.13.10).
//! - `pkg/config/config.go:341-348` `NodeSelectorConfig` fields/yaml tags,
//!   :358-362 `RegionConfig`, :95 the `NodeSelector` field, :98 top-level `Region`.
//! - `pkg/config/config.go:643-649` upstream defaults (`any`/`random`/`lowest`/
//!   0.9/0.9, empty `Regions`).
//! - `pkg/routing/selector/interfaces.go:33-75` `CreateNodeSelector`: empty kind ->
//!   `any`, `random` -> deprecated `AnySelector`, unknown kind ->
//!   ErrUnsupportedSelector (:26); `regionaware` takes `conf.Region` plus
//!   `conf.NodeSelector.Regions` and carries `SysloadLimit` (:52-58).
//! - `pkg/routing/selector/regionaware.go:35-55` blank-region and unknown-lat/lon
//!   construction errors, :113-127 haversine `distanceBetween`.
//! - `pkg/routing/selector/errors.go:20-26` the exact error texts.
//! - `pkg/routing/selector/utils.go:26-48` availability, :51-57 `GetNodeSysload`,
//!   :60-75 `LimitsReached` (the `limit:` capacity arms), :81-96 blank sort-by /
//!   algorithm gating, :118-160 the accepted `sort_by` keys.
//! - `pkg/service/roomallocator.go:43` builds the selector from the loaded config.

use super::*;
use crate::COMPAT_VERSION;
use lk_compat::room_control::catalog::{CatalogError, NodeSelector};
use lk_compat::room_control::selector::{Config, ConfiguredSelector, Region};
use lk_config::{
    EntryAction, Env, ProgramInfo, generated::specs, netdetect::NetProbe, parse_with_probe,
};
use lk_protocol::livekit as pb;
use std::{ffi::OsString, net::IpAddr, time::Duration};

struct NoNetwork;
impl NetProbe for NoNetwork {
    fn local_addresses(&self) -> Vec<IpAddr> {
        vec!["127.0.0.1".parse().expect("fixture loopback")]
    }
    fn external_ip(&self, _: &[String], _: bool) -> Result<IpAddr, String> {
        panic!("fixture must not discover external IP")
    }
    fn sleep(&self, _: Duration) {
        panic!("fixture must not retry networking")
    }
}

fn load(args: &[&str], env: &Env) -> LoadedConfig {
    let info = ProgramInfo {
        binary_name: "livecloud".into(),
        product_version: "fixture".into(),
        compat_version: COMPAT_VERSION,
    };
    let mut argv: Vec<OsString> = ["livecloud", "--dev"]
        .into_iter()
        .map(OsString::from)
        .collect();
    argv.extend(args.iter().map(OsString::from));
    match parse_with_probe(argv, env, &info, &NoNetwork) {
        Ok(EntryAction::RunServer(loaded)) => *loaded,
        Ok(other) => panic!("expected RunServer, got: {other:?}"),
        Err(e) => panic!("the fixture config must load, got: {e}"),
    }
}

/// The projection site itself: exactly what the allocator is handed.
fn projected(args: &[&str], env: &Env) -> Config {
    allocation_selector_config(&load(args, env))
}

fn yaml(body: &str) -> Vec<&str> {
    vec!["--config-body", body]
}

fn strs<'a>(v: &[&'a str]) -> Vec<&'a str> {
    v.to_vec()
}

fn construction_error(c: Config) -> CatalogError {
    match ConfiguredSelector::new(c) {
        Ok(_) => panic!("fixture expected a construction error"),
        Err(e) => e,
    }
}

fn serving_node(id: &str, region: &str, cpu_load: f32, load_avg: f32) -> pb::Node {
    node_with_clients(id, region, cpu_load, load_avg, 0)
}

/// `sort_by` needs a client count that differs per node, otherwise every leg's
/// winner would be decided by sort stability instead of by the projected config.
fn node_with_clients(
    id: &str,
    region: &str,
    cpu_load: f32,
    load_avg: f32,
    num_clients: i32,
) -> pb::Node {
    pb::Node {
        id: id.into(),
        state: pb::NodeState::Serving as i32,
        region: region.into(),
        stats: Some(pb::NodeStats {
            updated_at: NOW,
            num_cpus: 4,
            cpu_load,
            load_avg_last1min: load_avg,
            num_clients,
            ..Default::default()
        }),
        ..Default::default()
    }
}

/// Inside the 5s `node_available` window (registry.rs:254-258) for a fixed wall clock.
const NOW: i64 = 1_700_000_000;

fn fresh_node(n: &mut pb::Node, updated_at: i64) -> pb::Node {
    n.stats.as_mut().expect("fixture stats").updated_at = updated_at;
    n.clone()
}

fn select(c: Config, nodes: &[pb::Node]) -> Result<String, String> {
    ConfiguredSelector::new(c)
        .map_err(|e| e.to_string())?
        .select_at(nodes, NOW, &mut |_| 0)
        .map_err(|e| e.to_string())
}

// ---------------------------------------------------------------------------
// Leg 1: full YAML block, every leaf set to a distinct non-default value.
// ---------------------------------------------------------------------------

const FULL_YAML: &str = "\
region: us-west-2
node_selector:
  kind: regionaware
  sort_by: clients
  algorithm: twochoice
  cpu_load_limit: 0.42
  sysload_limit: 0.77
  regions:
    - name: us-west-2
      lat: 45.52
      lon: -122.68
    - name: eu-central-1
      lat: 50.11
      lon: 8.68
limit:
  num_tracks: 77
  bytes_per_sec: 123456
";

#[test]
fn nsel_full_yaml_block_projects_every_leaf() {
    let c = projected(&yaml(FULL_YAML), &Env::default());
    assert_eq!(c.kind, "regionaware");
    assert_eq!(c.sort_by, "clients");
    assert_eq!(c.algorithm, "twochoice");
    // float32 fidelity (config.go:345-346 are float32, not float64).
    assert_eq!(c.cpu_limit, 0.42f32);
    assert_eq!(c.sysload_limit, 0.77f32);
    // `region` comes from the TOP-LEVEL key (interfaces.go:53 passes conf.Region;
    // NodeSelectorConfig has no region field, config.go:341-348).
    assert_eq!(c.region, "us-west-2");
    assert_eq!(c.regions.len(), 2);
    assert_eq!(c.regions[0].name, "us-west-2");
    assert_eq!(c.regions[0].lat, 45.52f64);
    assert_eq!(c.regions[0].lon, -122.68f64);
    assert_eq!(c.regions[1].name, "eu-central-1");
    assert_eq!(c.regions[1].lat, 50.11f64);
    assert_eq!(c.regions[1].lon, 8.68f64);
    // The capacity arms come from `limit:` (utils.go:60-75 LimitsReached reads
    // config.LimitConfig), not from `node_selector:`.
    assert_eq!(c.track_limit, 77i32);
    assert_eq!(c.bytes_limit, 123456f32);
}

// ---------------------------------------------------------------------------
// Leg 2: key absent -> the upstream default tree (config.go:643-649).
// ---------------------------------------------------------------------------

#[test]
fn nsel_absent_projects_upstream_defaults() {
    let c = projected(&strs(&[]), &Env::default());
    assert_eq!(c.kind, "any");
    assert_eq!(c.sort_by, "random");
    assert_eq!(c.algorithm, "lowest");
    assert_eq!(c.cpu_limit, 0.9f32);
    assert_eq!(c.sysload_limit, 0.9f32);
    assert_eq!(c.region, "");
    assert!(c.regions.is_empty());
    assert_eq!(c.track_limit, 0i32);
    assert_eq!(c.bytes_limit, 0f32);

    // An empty `sort_by` reaches the constructor un-normalized, exactly as upstream
    // hands AnySelector{""} to SelectSortedNode (utils.go:81-86): the failure is a
    // per-selection error with ErrSortByNotSet text, never a construction error.
    let mut blank = c.clone();
    blank.sort_by = String::new();
    assert!(
        ConfiguredSelector::new(blank.clone()).is_ok(),
        "blank sort_by must construct, like the Go AnySelector with an empty SortBy"
    );
    assert_eq!(
        select(blank, &[serving_node("ND_a", "", 0.0, 0.0)]).unwrap_err(),
        "sort by option cannot be blank"
    );
    let mut blank_algo = c;
    blank_algo.algorithm = String::new();
    assert!(ConfiguredSelector::new(blank_algo.clone()).is_ok());
    assert_eq!(
        select(blank_algo, &[serving_node("ND_a", "", 0.0, 0.0)]).unwrap_err(),
        "node selector algorithm option cannot be blank"
    );
}

// ---------------------------------------------------------------------------
// Leg 3a: the ENV surface (generated.rs:337-359 names LIVEKIT_<PATH_UPPER>).
// ---------------------------------------------------------------------------

#[test]
fn nsel_env_surface_projects_into_effective_config() {
    let env = Env::from_pairs([
        ("LIVEKIT_NODE_SELECTOR_KIND", "cpuload"),
        ("LIVEKIT_NODE_SELECTOR_SORT_BY", "tracks"),
        ("LIVEKIT_NODE_SELECTOR_ALGORITHM", "twochoice"),
        ("LIVEKIT_NODE_SELECTOR_CPU_LOAD_LIMIT", "0.25"),
        ("LIVEKIT_NODE_SELECTOR_SYSLOAD_LIMIT", "0.375"),
        ("LIVEKIT_REGION", "ap-south-1"),
        ("LIVEKIT_LIMIT_NUM_TRACKS", "8"),
        ("LIVEKIT_LIMIT_BYTES_PER_SEC", "1024"),
    ]);
    let c = projected(&strs(&[]), &env);
    assert_eq!(c.kind, "cpuload");
    assert_eq!(c.sort_by, "tracks");
    assert_eq!(c.algorithm, "twochoice");
    assert_eq!(c.cpu_limit, 0.25f32);
    assert_eq!(c.sysload_limit, 0.375f32);
    assert_eq!(c.region, "ap-south-1");
    assert_eq!(c.track_limit, 8i32);
    assert_eq!(c.bytes_limit, 1024f32);
    // regions[] has NO env or flag surface: a slice leaf gets neither
    // (generated.rs:1-12, specs() skips slices), so YAML is its only route.
    assert!(c.regions.is_empty());
    assert!(
        specs().iter().all(|s| !s.path.contains("regions")),
        "regions must not have a generated flag"
    );
    assert!(
        specs()
            .iter()
            .any(|s| s.path == "node_selector.cpu_load_limit"
                && s.env == "LIVEKIT_NODE_SELECTOR_CPU_LOAD_LIMIT"),
        "the scalar leaves must be registered in the flag table"
    );
}

// ---------------------------------------------------------------------------
// Leg 3b: the CLI-flag surface. Measured: the port registers the generated flag
// under the verbatim dotted YAML path (`--node_selector.cpu_load_limit`), and the
// hyphenated spelling `--node-selector.cpu-load-limit` is a usage error. The
// selector leaves are absent from `help` output because generated flags only show
// under `help-verbose` (cli.rs:296-303, :319-327).
// ---------------------------------------------------------------------------

#[test]
fn nsel_cli_flag_surface_projects_the_registered_dotted_spelling() {
    let args = strs(&[
        "--config-body",
        "port: 7880\n",
        "--node_selector.kind=sysload",
        "--node_selector.sort_by=rooms",
        "--node_selector.algorithm=lowest",
        "--node_selector.cpu_load_limit=0.125",
        "--node_selector.sysload_limit=0.0625",
        "--limit.num_tracks=99",
        "--limit.bytes_per_sec=2048",
    ]);
    let c = projected(&args, &Env::default());
    assert_eq!(c.kind, "sysload");
    assert_eq!(c.sort_by, "rooms");
    assert_eq!(c.algorithm, "lowest");
    assert_eq!(c.cpu_limit, 0.125f32);
    assert_eq!(c.sysload_limit, 0.0625f32);
    assert_eq!(c.track_limit, 99i32);
    assert_eq!(c.bytes_limit, 2048f32);
    // `region` is a base flag (cli.rs BASE_FLAG_NAMES), so it is excluded from the
    // generated table: there is no `--region` entry in specs(), only the base one.
    assert!(specs().iter().all(|s| s.path != "region"));

    // The hyphenated spelling does not exist as a surface: same leaf, same value,
    // and the load never reaches RunServer.
    for (bad, short) in [
        (
            "--node-selector.cpu-load-limit=0.5",
            "node-selector.cpu-load-limit",
        ),
        (
            "--node-selector.cpu_load_limit=0.5",
            "node-selector.cpu_load_limit",
        ),
    ] {
        let info = ProgramInfo {
            binary_name: "livecloud".into(),
            product_version: "fixture".into(),
            compat_version: COMPAT_VERSION,
        };
        let argv: Vec<OsString> = ["livecloud", "--dev", bad]
            .into_iter()
            .map(OsString::from)
            .collect();
        let outcome = match parse_with_probe(argv, &Env::default(), &info, &NoNetwork) {
            Ok(EntryAction::RunServer(_)) => "loaded the config".to_string(),
            Ok(EntryAction::PrintHelp { text, .. }) => {
                text.lines().next().unwrap_or("empty help").to_string()
            }
            Ok(other) => format!("{other:?}"),
            Err(e) => format!("error: {e}"),
        };
        assert_eq!(
            outcome,
            format!("Incorrect Usage: flag provided but not defined: -{short}"),
            "the hyphenated spelling must be an unknown-flag usage error"
        );
    }
}

// ---------------------------------------------------------------------------
// Leg 3c: source precedence YAML < ENV < flag, on the projected config.
// ---------------------------------------------------------------------------

#[test]
fn nsel_source_precedence_yaml_env_flag() {
    let body = "node_selector:\n  sort_by: bytespersec\n  cpu_load_limit: 0.5\n";
    let c = projected(&yaml(body), &Env::default());
    assert_eq!(c.sort_by, "bytespersec");
    assert_eq!(c.cpu_limit, 0.5f32);

    let env = Env::from_pairs([("LIVEKIT_NODE_SELECTOR_SORT_BY", "cpuload")]);
    let c = projected(&yaml(body), &env);
    assert_eq!(c.sort_by, "cpuload", "env must override yaml");
    assert_eq!(c.cpu_limit, 0.5f32);

    let c = projected(
        &strs(&["--config-body", body, "--node_selector.sort_by=tracks"]),
        &env,
    );
    assert_eq!(c.sort_by, "tracks", "flag must override env");
}

// ---------------------------------------------------------------------------
// Leg 4: the projection has consequences at ConfiguredSelector::new/select_at.
// ---------------------------------------------------------------------------

#[test]
fn nsel_projected_random_kind_normalizes_to_any() {
    // interfaces.go:69-71 warns and returns AnySelector; selector.rs:56-63 mirrors
    // it. AnySelector never reads the load fields (any.go:20-32), while cpuload
    // dereferences stats first (cpuload.go:24-31): a nil-stats node therefore
    // selects fine under `random` and errors under `cpuload`. A strict limit cannot
    // be used here because an empty candidate filter falls back to the unfiltered
    // list (selector.rs:118-120), which would make both arms agree.
    let mut c = projected(&yaml("node_selector:\n  kind: random\n"), &Env::default());
    assert_eq!(c.kind, "random");
    c.cpu_limit = 0.0;
    c.sysload_limit = 0.0;
    let mut no_stats = serving_node("ND_hot", "", 0.9, 3.6);
    no_stats.stats = None;
    assert_eq!(
        select(c.clone(), &[no_stats.clone()]).expect("random->any must not read stats"),
        "ND_hot"
    );
    // Same projected numbers, same node set, only the kind differs: cpuload fails.
    let mut strict = c;
    strict.kind = "cpuload".into();
    assert_eq!(
        select(strict, &[no_stats]).unwrap_err(),
        "node selector encountered nil stats"
    );
}

#[test]
fn nsel_projected_unsupported_kind_gives_upstream_text() {
    // ErrUnsupportedSelector (interfaces.go:26, default arm :72).
    let c = projected(
        &yaml("node_selector:\n  kind: leastconnections\n"),
        &Env::default(),
    );
    assert_eq!(c.kind, "leastconnections");
    assert_eq!(
        construction_error(c).to_string(),
        "unsupported node selector"
    );
}

#[test]
fn nsel_projected_regionaware_blank_region_is_rejected() {
    // regionaware needs the top-level `region`; its absence is fatal at
    // construction (regionaware.go:35-37, errors.go:21).
    let c = projected(
        &yaml(
            "node_selector:\n  kind: regionaware\n  regions:\n    - name: us-west-2\n      lat: 1\n      lon: 2\n",
        ),
        &Env::default(),
    );
    assert_eq!(c.kind, "regionaware");
    assert_eq!(c.region, "");
    assert_eq!(c.regions.len(), 1);
    assert_eq!(
        construction_error(c).to_string(),
        "current region cannot be blank"
    );
}

#[test]
fn nsel_projected_regionaware_region_missing_from_regions_is_rejected() {
    // regionaware.go:52-54: a non-empty regions list omitting the current region
    // is fatal (ErrCurrentRegionUnknownLatLon, errors.go:22).
    let c = projected(
        &yaml(
            "region: moon-base-1\nnode_selector:\n  kind: regionaware\n  regions:\n    - name: us-west-2\n      lat: 45.52\n      lon: -122.68\n",
        ),
        &Env::default(),
    );
    assert_eq!(c.region, "moon-base-1");
    assert_eq!(c.regions.len(), 1);
    assert_eq!(
        construction_error(c).to_string(),
        "unknown lat and lon for the current region"
    );
}

#[test]
fn nsel_projected_lat_lon_and_region_name_choose_the_nearest() {
    // Current region us-west-2 (45.52,-122.68). The nearest-region arm runs after the
    // kind gate, so the winner needs all three projected fields at once: the region
    // name map, the lat/lon distances and the cpu limit (regionaware.go:64-96).
    let body = "\
region: us-west-2
node_selector:
  kind: regionaware
  sort_by: clients
  algorithm: lowest
  cpu_load_limit: 0.9
  sysload_limit: 0.9
  regions:
    - name: us-west-2
      lat: 45.52
      lon: -122.68
    - name: eu-central-1
      lat: 50.11
      lon: 8.68
    - name: ap-south-1
      lat: 19.08
      lon: 72.88
";
    let c = projected(&yaml(body), &Env::default());
    // ND_hot is in the current region but crosses the projected cpu limit, so the
    // kind gate (regionaware.go:64-74 IsAvailable arms) removes it first; among the
    // survivors the nearest region wins even though it is the busier one.
    let nodes = [
        node_with_clients("ND_hot", "us-west-2", 0.95, 3.8, 0),
        node_with_clients("ND_west", "us-west-2", 0.5, 2.0, 90),
        node_with_clients("ND_eu", "eu-central-1", 0.1, 0.4, 10),
        node_with_clients("ND_asia", "ap-south-1", 0.2, 0.8, 20),
    ];
    assert_eq!(
        select(c.clone(), &nodes).expect("nearest region wins"),
        "ND_west"
    );
    // Drop the coordinates and the same set is judged by sorting alone, which picks
    // the idlest far region: that contrast is what makes lat/lon load-bearing.
    let mut blind = c.clone();
    blind.regions.clear();
    assert_eq!(
        select(blind, &nodes).expect("no distances means no nearest filter"),
        "ND_eu"
    );
    // Latitude-only check: with the current lon placed midway between us-west-2 and
    // eu-central-1, moving only the lat from 45.52 to 60 makes eu nearer than west,
    // so this winner cannot come from `lon` alone.
    let mut north = c.clone();
    north.region = "lat-test".into();
    north.regions.push(Region {
        name: "lat-test".into(),
        lat: 60.0,
        lon: -57.0,
    });
    assert_eq!(
        select(north, &nodes).expect("moved lat moves the winner"),
        "ND_eu"
    );
    // Longitudinal check: move only the current lon (lat stays 45.52), which flips
    // the eu/ap ordering while leaving every other field alone, so the winner can
    // only come from the projected `lon` value.
    let mut east = c.clone();
    east.region = "lon-test".into();
    east.regions.push(Region {
        name: "lon-test".into(),
        lat: 45.52,
        lon: 75.0,
    });
    assert_eq!(
        select(east, &nodes).expect("moved lon moves the winner"),
        "ND_asia"
    );
    // Move the projected coordinates onto eu-central-1 and the win moves with them,
    // so the choice comes from the projected lat/lon and not from node order.
    let mut flipped = c;
    flipped.region = "eu-central-1".into();
    flipped.regions[1].lat = 50.11;
    flipped.regions[1].lon = 8.68;
    assert_eq!(
        select(flipped, &nodes).expect("moved current lat/lon move the winner"),
        "ND_eu"
    );
}

#[test]
fn nsel_projected_cpu_and_sysload_limits_gate_the_winner_independently() {
    // kind=cpuload consumes cpu_limit only (interfaces.go:41-44 CPULoadSelector,
    // cpuload.go:24-31); kind=sysload consumes sysload_limit against
    // load_avg/num_cpus (interfaces.go:46-50, utils.go:51-57). Both arms reject a
    // candidate only when its own metric crosses the projected limit, so each field
    // is moved while the other is held above every node's metric.
    let cold = node_with_clients("ND_cold", "", 0.1, 0.4, 300);
    let hot = node_with_clients("ND_hot", "", 0.95, 3.8, 1);

    let c = projected(
        &yaml(
            "node_selector:\n  kind: cpuload\n  cpu_load_limit: 0.5\n  sysload_limit: 0.9\n  sort_by: clients\n  algorithm: lowest\n",
        ),
        &Env::default(),
    );
    assert_eq!(c.cpu_limit, 0.5f32);
    assert_eq!(c.sysload_limit, 0.9f32);
    // cpu 0.95 >= 0.5 drops ND_hot, so the busier-but-under-load client winner
    // ND_cold survives; without the gate ND_hot (10 clients) would win.
    assert_eq!(
        select(c.clone(), &[hot.clone(), cold.clone()]).unwrap(),
        "ND_cold"
    );
    let mut loose = c.clone();
    loose.cpu_limit = 0.99;
    assert_eq!(
        select(loose.clone(), &[hot.clone(), cold.clone()]).unwrap(),
        "ND_hot",
        "raising only cpu_limit must bring the busy node back"
    );
    // The same stimulus under kind=sysload gates on sysload_limit only, so the
    // projected cpu_load_limit cannot be what rejects here (interfaces.go:46-50):
    // hot passes cpu 0.95 < 0.99 but fails 3.8/4 = 0.95 >= 0.5.
    let mut as_sysload = loose;
    as_sysload.kind = "sysload".into();
    assert_eq!(
        select(as_sysload, &[hot.clone(), cold.clone()]).unwrap(),
        "ND_cold"
    );

    // sysload 3.8/4 = 0.95 >= 0.5 drops ND_hot even though its cpu arm would pass
    // at 0.99: the second projected field gates independently of the first.
    let sys = projected(
        &yaml(
            "node_selector:\n  kind: sysload\n  cpu_load_limit: 0.99\n  sysload_limit: 0.5\n  sort_by: clients\n  algorithm: lowest\n",
        ),
        &Env::default(),
    );
    // Only the projected sysload_limit can produce this winner: hot is under the
    // projected cpu_limit (0.95 < 0.99), so a hardcoded/default 0.9 sysload arm
    // would keep it and let it win on client count.
    assert_eq!(
        select(sys.clone(), &[hot.clone(), cold.clone()]).unwrap(),
        "ND_cold"
    );
    // And with no candidate inside the sysload window the strict filter comes back
    // empty and the selector falls through to the unfiltered list (selector.rs:
    // 118-120), which is why an over-tight limit must be asserted as a fallback and
    // not as an error.
    let stale = fresh_node(&mut hot.clone(), NOW - 6);
    let mut tight = sys.clone();
    tight.sysload_limit = 0.05;
    assert_eq!(
        select(tight, &[stale, cold.clone()]).unwrap(),
        "ND_cold",
        "empty filter must fall back, never report no nodes"
    );
    // The pair that isolates the sysload projection: identical node set and sort, only
    // the projected sysload_limit differs (0.5 vs 0.96). hot measures 3.8/4 = 0.95
    // sysload, so it sits between the two: gated away at 0.5, kept and client-count
    // winner at 0.96. A hardcoded/default limit cannot reproduce both arms.
    let mut gate = sys.clone();
    gate.sysload_limit = 0.96;
    assert_eq!(
        select(gate.clone(), &[hot.clone(), cold.clone()]).unwrap(),
        "ND_hot",
        "fixture: a limit above hot's sysload keeps it and it wins on client count"
    );
    let gated = Config {
        sysload_limit: sys.sysload_limit,
        ..gate
    };
    assert_eq!(
        select(gated, &[hot, cold.clone()]).unwrap(),
        "ND_cold",
        "the projected sysload_limit must be what removes it"
    );
}

#[test]
fn nsel_projected_limit_fields_drive_at_capacity() {
    // track_limit / bytes_limit arrive from `limit:` (utils.go:60-75) and are the
    // only selector inputs the single-node allocation preflight reads.
    let mut c = projected(
        &yaml("limit:\n  num_tracks: 2\n  bytes_per_sec: 100\n"),
        &Env::default(),
    );
    assert_eq!(c.track_limit, 2i32);
    assert_eq!(c.bytes_limit, 100f32);
    let sel = ConfiguredSelector::new(c.clone()).expect("fixture constructs");
    let mut n = serving_node("ND_cap", "", 0.0, 0.0);
    assert!(!sel.at_capacity(&n));
    n.stats.as_mut().expect("fixture stats").num_tracks_in = 2;
    assert!(sel.at_capacity(&n), "track_limit must reach LimitsReached");
    n.stats.as_mut().expect("fixture stats").num_tracks_in = 0;
    n.stats.as_mut().expect("fixture stats").rates = vec![pb::NodeStatsRate {
        bytes_in: 40.0,
        bytes_out: 60.0,
        ..Default::default()
    }];
    assert!(sel.at_capacity(&n), "bytes_limit must reach LimitsReached");
    // Zero limits disable both arms, matching the upstream `> 0` guards.
    c.track_limit = 0;
    c.bytes_limit = 0.0;
    assert!(
        !ConfiguredSelector::new(c)
            .expect("fixture constructs")
            .at_capacity(&n)
    );
}
