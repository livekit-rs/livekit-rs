//! Fixed v1.13.10 selector policies. Runtime sampling is owned by product assembly.
//! Ported from routing/selector (Apache-2.0); equal-key sort uses fixed Go PDQsort.
use super::{
    catalog::{CatalogError, NodeSelector},
    registry::node_available,
};
use lk_protocol::livekit as pb;
use std::cmp::Ordering;
#[path = "selector_go_sort.rs"]
mod go_sort;
#[derive(Clone, Debug)]
pub struct Region {
    pub name: String,
    pub lat: f64,
    pub lon: f64,
}
#[derive(Clone, Debug)]
pub struct Config {
    pub kind: String,
    pub sort_by: String,
    pub algorithm: String,
    pub cpu_limit: f32,
    pub sysload_limit: f32,
    pub region: String,
    pub regions: Vec<Region>,
    pub track_limit: i32,
    pub bytes_limit: f32,
}
#[derive(Clone)]
pub struct ConfiguredSelector {
    config: Config,
    distances: std::collections::BTreeMap<String, f64>,
}
fn invalid(s: &str) -> CatalogError {
    CatalogError::Invalid(s.into())
}
fn sysload(n: &pb::Node) -> Result<f32, CatalogError> {
    let s = n
        .stats
        .as_ref()
        .ok_or_else(|| invalid("node selector encountered nil stats"))?;
    Ok(s.load_avg_last1min / s.num_cpus.max(1) as f32)
}
fn cmp_float(a: f32, b: f32) -> Ordering {
    let delta = a - b;
    if delta < 0.0 {
        Ordering::Less
    } else if delta > 0.0 {
        Ordering::Greater
    } else {
        Ordering::Equal
    }
}
impl ConfiguredSelector {
    pub fn new(mut config: Config) -> Result<Self, CatalogError> {
        if config.kind.is_empty() || config.kind == "random" {
            if config.kind == "random" {
                tracing::warn!(
                    "random node selector is deprecated, please switch to any or another selector"
                );
            }
            config.kind = "any".into();
        }
        if !["any", "cpuload", "sysload", "regionaware"].contains(&config.kind.as_str()) {
            return Err(invalid("unsupported node selector"));
        }
        let mut distances = std::collections::BTreeMap::new();
        if config.kind == "regionaware" {
            if config.region.is_empty() {
                return Err(invalid("current region cannot be blank"));
            }
            let current = config.regions.iter().find(|r| r.name == config.region);
            if current.is_none() && !config.regions.is_empty() {
                return Err(invalid("unknown lat and lon for the current region"));
            }
            if let Some(c) = current {
                for r in &config.regions {
                    distances.insert(r.name.clone(), distance(c.lat, c.lon, r.lat, r.lon));
                }
            }
        }
        Ok(Self { config, distances })
    }
    pub fn select_at(
        &self,
        nodes: &[pb::Node],
        now: i64,
        draw: &mut dyn FnMut(usize) -> usize,
    ) -> Result<String, CatalogError> {
        let mut nodes: Vec<_> = nodes
            .iter()
            .filter(|n| node_available(n, now) && n.state == pb::NodeState::Serving as i32)
            .collect();
        if nodes.is_empty() {
            return Err(invalid("could not find any available nodes"));
        }
        let kind = &self.config.kind;
        if kind != "any" {
            let mut filtered = Vec::new();
            for &n in &nodes {
                let value = if kind == "cpuload" {
                    n.stats
                        .as_ref()
                        .ok_or_else(|| invalid("node selector encountered nil stats"))?
                        .cpu_load
                } else {
                    sysload(n)?
                };
                let limit = if kind == "cpuload" {
                    self.config.cpu_limit
                } else {
                    self.config.sysload_limit
                };
                if value < limit {
                    filtered.push(n);
                }
            }
            if !filtered.is_empty() {
                nodes = filtered;
            }
        }
        if kind == "regionaware" {
            let mut nearest = Vec::new();
            let mut region = String::new();
            let mut min = f64::MAX;
            for &n in &nodes {
                if n.region == region {
                    nearest.push(n);
                    continue;
                }
                if let Some(&d) = self.distances.get(&n.region)
                    && d < min
                {
                    min = d;
                    region = n.region.clone();
                    nearest.clear();
                    nearest.push(n);
                }
            }
            if !nearest.is_empty() {
                nodes = nearest;
            }
        }
        if self.config.sort_by.is_empty() {
            return Err(invalid("sort by option cannot be blank"));
        }
        if self.config.algorithm.is_empty() {
            return Err(invalid("node selector algorithm option cannot be blank"));
        }
        match self.config.algorithm.as_str() {
            "lowest" => {}
            "twochoice" => {
                if nodes.len() > 2 {
                    let first = draw(nodes.len());
                    if first >= nodes.len() {
                        return Err(invalid("invalid entropy index"));
                    }
                    let second = draw(nodes.len() - 1);
                    if second >= nodes.len() - 1 {
                        return Err(invalid("invalid entropy index"));
                    }
                    let second = if second >= first { second + 1 } else { second };
                    nodes = vec![nodes[first], nodes[second]];
                }
            }
            _ => return Err(invalid("unknown node selector algorithm option")),
        }
        let key = &self.config.sort_by;
        if key == "random" {
            let i = draw(nodes.len());
            return nodes
                .get(i)
                .map(|n| n.id.clone())
                .ok_or_else(|| invalid("invalid entropy index"));
        }
        if ![
            "sysload",
            "cpuload",
            "rooms",
            "clients",
            "tracks",
            "bytespersec",
        ]
        .contains(&key.as_str())
        {
            return Err(invalid("unknown sort by option"));
        }
        for n in &nodes {
            if n.stats.is_none() {
                return Err(invalid("node selector encountered nil stats"));
            }
        }
        go_sort::sort_by(&mut nodes, |a, b| {
            let (a, b) = (
                a.stats.as_ref().expect("checked stats"),
                b.stats.as_ref().expect("checked stats"),
            );
            match key.as_str() {
                "sysload" => cmp_float(
                    a.load_avg_last1min / a.num_cpus.max(1) as f32,
                    b.load_avg_last1min / b.num_cpus.max(1) as f32,
                ),
                "cpuload" => cmp_float(a.cpu_load, b.cpu_load),
                "rooms" => a.num_rooms.wrapping_sub(b.num_rooms).cmp(&0),
                "clients" => a.num_clients.wrapping_sub(b.num_clients).cmp(&0),
                "tracks" => a
                    .num_tracks_in
                    .wrapping_add(a.num_tracks_out)
                    .wrapping_sub(b.num_tracks_in.wrapping_add(b.num_tracks_out))
                    .cmp(&0),
                "bytespersec" => {
                    let rate = |s: &pb::NodeStats| {
                        s.rates.first().map_or(0.0, |r| r.bytes_in + r.bytes_out)
                    };
                    cmp_float(rate(a), rate(b))
                }
                _ => Ordering::Equal,
            }
        });
        Ok(nodes[0].id.clone())
    }
}
impl NodeSelector for ConfiguredSelector {
    fn select(&self, nodes: &[pb::Node]) -> Result<String, CatalogError> {
        self.select_at(nodes, rtc_core::util::unix_s(), &mut random_index)
    }
    fn at_capacity(&self, node: &pb::Node) -> bool {
        let Some(s) = &node.stats else {
            return false;
        };
        (self.config.track_limit > 0
            && self.config.track_limit <= s.num_tracks_in.wrapping_add(s.num_tracks_out))
            || (self.config.bytes_limit > 0.0
                && self.config.bytes_limit
                    <= s.rates.first().map_or(0.0, |r| r.bytes_in + r.bytes_out))
    }
}
fn random_index(n: usize) -> usize {
    assert!(n > 0);
    let max = usize::MAX - usize::MAX % n;
    loop {
        let mut b = [0u8; std::mem::size_of::<usize>()];
        rtc_core::util::fill_random(&mut b);
        let v = usize::from_ne_bytes(b);
        if v < max {
            return v % n;
        }
    }
}
fn distance(a: f64, b: f64, c: f64, d: f64) -> f64 {
    let (a, b, c, d) = (
        a.to_radians(),
        b.to_radians(),
        c.to_radians(),
        d.to_radians(),
    );
    let h = ((c - a) / 2.0).sin().powi(2) + a.cos() * c.cos() * ((d - b) / 2.0).sin().powi(2);
    2.0 * 6378100.0 * h.sqrt().asin()
}
#[cfg(test)]
mod tests {
    use super::*;
    fn cfg() -> Config {
        Config {
            kind: "any".into(),
            sort_by: "clients".into(),
            algorithm: "lowest".into(),
            cpu_limit: 0.9,
            sysload_limit: 0.9,
            region: String::new(),
            regions: vec![],
            track_limit: 0,
            bytes_limit: 0.0,
        }
    }
    fn node(id: &str, clients: i32, cpu: f32) -> pb::Node {
        pb::Node {
            id: id.into(),
            state: pb::NodeState::Serving as i32,
            stats: Some(pb::NodeStats {
                updated_at: 100,
                num_clients: clients,
                cpu_load: cpu,
                num_cpus: 2,
                load_avg_last1min: cpu * 2.0,
                ..Default::default()
            }),
            ..Default::default()
        }
    }
    fn select(c: Config, n: &[pb::Node]) -> String {
        ConfiguredSelector::new(c)
            .unwrap()
            .select_at(n, 100, &mut |_| 0)
            .unwrap()
    }
    #[test]
    fn every_policy_and_strict_threshold_fallback() {
        let a = node("a", 1, 0.9);
        let b = node("b", 2, 0.8);
        for kind in ["cpuload", "sysload"] {
            let mut c = cfg();
            c.kind = kind.into();
            assert_eq!(select(c.clone(), &[a.clone(), b.clone()]), "b");
            c.cpu_limit = 0.1;
            c.sysload_limit = 0.1;
            assert_eq!(select(c, &[a.clone(), b.clone()]), "a");
        }
    }
    #[test]
    fn fresh_serving_filter_and_future_stats() {
        let mut a = node("a", 0, 0.1);
        let mut b = node("b", 1, 0.1);
        a.state = pb::NodeState::ShuttingDown as i32;
        b.stats.as_mut().unwrap().updated_at = 101;
        assert_eq!(select(cfg(), &[a, b]), "b");
        let mut n = node("old", 0, 0.0);
        n.stats.as_mut().unwrap().updated_at = 95;
        assert!(
            ConfiguredSelector::new(cfg())
                .unwrap()
                .select_at(&[n], 100, &mut |_| 0)
                .is_err()
        );
    }
    #[test]
    fn twochoice_distinct_before_comparison() {
        let mut c = cfg();
        c.algorithm = "twochoice".into();
        let s = ConfiguredSelector::new(c).unwrap();
        let mut draws = [2, 0].into_iter();
        assert_eq!(
            s.select_at(
                &[node("a", 3, 0.0), node("b", 0, 0.0), node("c", 2, 0.0)],
                100,
                &mut |_| draws.next().unwrap()
            )
            .unwrap(),
            "c"
        );
    }
    #[test]
    fn region_nearest_then_cost() {
        let mut c = cfg();
        c.kind = "regionaware".into();
        c.region = "home".into();
        c.regions = vec![
            Region {
                name: "home".into(),
                lat: 0.0,
                lon: 0.0,
            },
            Region {
                name: "near".into(),
                lat: 0.0,
                lon: 1.0,
            },
            Region {
                name: "far".into(),
                lat: 0.0,
                lon: 100.0,
            },
        ];
        let mut a = node("a", 0, 0.0);
        a.region = "far".into();
        let mut b = node("b", 10, 0.0);
        b.region = "near".into();
        assert_eq!(select(c, &[a, b]), "b");
    }
    #[test]
    fn capacity_only_source_first_rate_and_equality() {
        let mut c = cfg();
        c.track_limit = 2;
        c.bytes_limit = 100.0;
        let s = ConfiguredSelector::new(c).unwrap();
        let mut n = node("a", 0, 0.0);
        assert!(!s.at_capacity(&n));
        n.stats.as_mut().unwrap().num_tracks_in = 2;
        assert!(s.at_capacity(&n));
        n.stats.as_mut().unwrap().num_tracks_in = 0;
        n.stats.as_mut().unwrap().rates = vec![pb::NodeStatsRate {
            bytes_in: 40.0,
            bytes_out: 60.0,
            ..Default::default()
        }];
        assert!(s.at_capacity(&n));
    }
}
#[cfg(test)]
#[path = "selector_oracle_tests.rs"]
mod oracle_tests;
