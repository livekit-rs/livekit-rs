//! SPEC-0074 C22: faithful port of `getNAT1to1IPsForConf`
//! (`pkg/rtcconfig/webrtc_config.go:296`) for the `rtc.external_ip_only`
//! candidate filter.
//!
//! Upstream runs a STUN binding from every eligible local address (optionally
//! over a handful of ephemeral source ports) and records the first external
//! address that resolves per local IP, de-duplicating by external address. When
//! `rtc.external_ip_only` is set it then narrows the ICE IPv4 candidate filter
//! to only those local IPv4s that obtained an external mapping, while leaving
//! IPv6 candidates to the original filter (`webrtc_config.go:402`).
//!
//! The production consumer probes in parallel under the source shared timeout
//! and preserves local/external pairs. The historical injected discover helper
//! remains deterministic for existing source-selection tests only.

use std::collections::HashSet;
use std::net::IpAddr;

use crate::netdetect::{NetProbe, ProbeContext, SystemProbe};
use std::time::Duration;

/// Result of per-interface external-IP discovery.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Nat1to1 {
    /// Local IPv4/IPv6 addresses that resolved to a distinct external address.
    /// `rtc.external_ip_only` keeps only IPv4 members of this set as host
    /// candidates (IPv6 is never filtered out, matching upstream).
    pub mapped: Vec<IpAddr>,
    /// Local-to-external rewrite, including self mappings for unresolved locals.
    pub rewrites: Vec<(IpAddr, IpAddr)>,
}

/// `getNAT1to1IPsForConf` mapped-IP discovery. `servers` must already have the
/// defaults applied; `local_ips` are the eligible local addresses; `ports` are
/// the source ports to try per address (see [`stun_source_ports`]: upstream 5
/// random from the ICE range, the fixed mux port, or `0`). Returns `None` when
/// nothing resolved, matching upstream's `len(natMapping) == 0` early return.
pub(crate) fn discover(
    probe: &dyn NetProbe,
    servers: &[String],
    local_ips: &[IpAddr],
    ports: &[u16],
) -> Option<Nat1to1> {
    let ports = if ports.is_empty() { &[0u16][..] } else { ports };
    let mut seen_external = HashSet::new();
    let mut mapped = Vec::new();
    let mut rewrites = Vec::new();
    for &local in local_ips {
        for &port in ports {
            match probe.external_ip_from(local, port, servers, true) {
                Ok(external) => {
                    // First-wins de-duplication by external address, as upstream
                    // ignores a duplicate external that already maps to a local.
                    if seen_external.insert(external) {
                        mapped.push(local);
                        rewrites.push((local, external));
                    }
                    break;
                }
                // Upstream per-port loop (webrtc_config.go:330-341): an
                // "address already in use" error tries the next source port;
                // any other failure abandons this local address entirely.
                Err(e) if error_is_addr_in_use(&e) => continue,
                Err(_) => break,
            }
        }
    }
    if seen_external.is_empty() {
        return None;
    }
    Some(finish_mapping(mapped, rewrites, local_ips))
}

/// Production getNAT1to1IPsForConf: parallel per-local STUN/hairpin probes.
/// Every worker is scoped/joined; the shared budget cancels all owned sockets.
pub fn discover_system(
    servers: &[String],
    local_ips: &[IpAddr],
    ports: &[u16],
    skip_validation: bool,
) -> Option<Nat1to1> {
    discover_parallel(
        &SystemProbe,
        servers,
        local_ips,
        ports,
        skip_validation,
        Duration::from_secs(5),
        Duration::from_secs(1),
    )
}
fn discover_parallel(
    probe: &(dyn NetProbe + Sync),
    servers: &[String],
    local_ips: &[IpAddr],
    ports: &[u16],
    skip_validation: bool,
    initial_window: Duration,
    settled_window: Duration,
) -> Option<Nat1to1> {
    if local_ips.is_empty() {
        return None;
    }
    let ports = if ports.is_empty() { &[0][..] } else { ports };
    let context = ProbeContext::new(initial_window);
    let mut mapped = Vec::new();
    let mut rewrites = Vec::new();
    let mut seen_external = HashSet::new();
    std::thread::scope(|scope| {
        let (send, recv) = std::sync::mpsc::channel();
        for &local in local_ips {
            let send = send.clone();
            let context = context.clone();
            scope.spawn(move || {
                for &port in ports {
                    match probe.external_ip_from_context(
                        local,
                        port,
                        servers,
                        !skip_validation,
                        &context,
                    ) {
                        Ok(external) => {
                            let _ = send.send((local, external));
                            return;
                        }
                        Err(error) if error_is_addr_in_use(&error) => continue,
                        Err(_) => return,
                    }
                }
            });
        }
        drop(send);
        let mut first = false;
        loop {
            let Some(remaining) = context.remaining(context.deadline()) else {
                break;
            };
            match recv.recv_timeout(remaining) {
                Ok((local, external)) => {
                    if !first {
                        first = true;
                        context.settle_after(settled_window);
                    }
                    if seen_external.insert(external) {
                        mapped.push(local);
                        rewrites.push((local, external));
                    }
                }
                Err(std::sync::mpsc::RecvTimeoutError::Timeout) => break,
                Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => {
                    // Source channel is not closed by workers. It waits for
                    // the original five-second/settled one-second timer.
                    std::thread::sleep(remaining);
                    break;
                }
            }
        }
        context.cancel();
    });
    if rewrites.is_empty() {
        return None;
    }
    Some(finish_mapping(mapped, rewrites, local_ips))
}

fn finish_mapping(
    mapped: Vec<IpAddr>,
    rewrites: Vec<(IpAddr, IpAddr)>,
    locals: &[IpAddr],
) -> Nat1to1 {
    // Source map is keyed by external IP. A fallback self-map may overwrite
    // an existing key when a discovered external equals another local IP.
    // Preserve that observable edge rather than keeping two divergent rules.
    let mut external_to_local: std::collections::BTreeMap<_, _> = rewrites
        .into_iter()
        .map(|(local, external)| (external, local))
        .collect();
    for &local in locals {
        if !external_to_local.values().any(|value| *value == local) {
            external_to_local.insert(local, local);
        }
    }
    let rewrites = external_to_local
        .into_iter()
        .map(|(external, local)| (local, external))
        .collect();
    Nat1to1 { mapped, rewrites }
}

/// Go `strings.Contains(err.Error(), "address already in use")`. Rust's
/// `UdpSocket::bind` surfaces EADDRINUSE through `io::Error` Display on both
/// macOS and Linux, so the same substring matches; os error 98/48 spellings
/// are accepted as well.
fn error_is_addr_in_use(err: &str) -> bool {
    let lower = err.to_lowercase();
    lower.contains("address already in use")
        || lower.contains("os error 98")
        || lower.contains("os error 48")
}

/// Upstream-equivalent STUN source-port selection
/// (`webrtc_config.go:311-320`): ICE range mode samples 5 random ports in
/// `[start, end)`; mux mode uses the fixed first port (`UDPPort.ToSlice()[0]`,
/// i.e. the range start); neither configured probes from an OS-ephemeral (`0`).
pub fn stun_source_ports(ice_range: Option<(u16, u16)>, mux_first_port: Option<u16>) -> Vec<u16> {
    if let Some((start, end)) = ice_range
        && end > start
    {
        let span = usize::from(end - start);
        let mut out = Vec::with_capacity(5);
        for _ in 0..5 {
            let mut buf = [0u8; 8];
            // Same entropy source as the STUN transaction id (crypto getrandom).
            if getrandom::fill(&mut buf).is_ok() {
                let n = u64::from_ne_bytes(buf) as usize;
                out.push(start + (n % span) as u16);
            } else {
                out.push(start);
            }
        }
        return out;
    }
    if let Some(port) = mux_first_port {
        return vec![port];
    }
    vec![0]
}

/// Public entry used by the media-interface layer to obtain the set of local
/// addresses that resolved to an external IP. `None` when nothing mapped
/// (upstream then leaves the candidate filter unchanged).
pub fn mapped_local_ips(
    probe: &dyn NetProbe,
    servers: &[String],
    local_ips: &[IpAddr],
    ports: &[u16],
) -> Option<Vec<IpAddr>> {
    discover(probe, servers, local_ips, ports).map(|nat| nat.mapped)
}

/// Whether `rtc.external_ip_only` should admit a candidate address: IPv6 is
/// always delegated onward, IPv4 only when it obtained an external mapping.
pub fn external_ip_only_admits(ip: IpAddr, mapped: &[IpAddr]) -> bool {
    match ip {
        IpAddr::V6(_) => true,
        IpAddr::V4(_) => mapped.contains(&ip),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::cell::RefCell;

    /// Probe returning a fixed per-source mapping; unlisted sources fail with
    /// the given error text (default "no mapping").
    struct MapProbe {
        from: Vec<(IpAddr, IpAddr)>,
        fail: Vec<IpAddr>,
        fail_msg: Option<String>,
        calls: RefCell<Vec<(IpAddr, u16)>>,
    }

    impl NetProbe for MapProbe {
        fn local_addresses(&self) -> Vec<IpAddr> {
            self.from.iter().map(|(l, _)| *l).collect()
        }
        fn external_ip(&self, _s: &[String], _v4: bool) -> Result<IpAddr, String> {
            Err("unused".into())
        }
        fn external_ip_from(
            &self,
            local: IpAddr,
            port: u16,
            _servers: &[String],
            _validate: bool,
        ) -> Result<IpAddr, String> {
            self.calls.borrow_mut().push((local, port));
            if self.fail.contains(&local) {
                return Err(self.fail_msg.clone().unwrap_or_else(|| "no mapping".into()));
            }
            self.from
                .iter()
                .find(|(l, _)| *l == local)
                .map(|(_, e)| *e)
                .ok_or_else(|| "unknown source".to_string())
        }
        fn sleep(&self, _d: Duration) {}
    }

    fn ip(s: &str) -> IpAddr {
        s.parse().unwrap()
    }

    fn servers() -> Vec<String> {
        vec!["stun.example:3478".to_string()]
    }

    #[test]
    fn maps_each_resolved_local_once() {
        let probe = MapProbe {
            from: vec![
                (ip("10.0.0.1"), ip("203.0.113.1")),
                (ip("10.0.0.2"), ip("203.0.113.2")),
            ],
            fail: vec![],
            fail_msg: None,
            calls: RefCell::new(vec![]),
        };
        let out = discover(&probe, &servers(), &[ip("10.0.0.1"), ip("10.0.0.2")], &[0]).unwrap();
        assert_eq!(out.mapped, vec![ip("10.0.0.1"), ip("10.0.0.2")]);
    }

    #[test]
    fn duplicate_external_collapses_to_first_local() {
        // Two locals behind a symmetric NAT report the same external address;
        // only the first-resolved local is kept, matching upstream ignore.
        let probe = MapProbe {
            from: vec![
                (ip("10.0.0.1"), ip("203.0.113.9")),
                (ip("10.0.0.2"), ip("203.0.113.9")),
            ],
            fail: vec![],
            fail_msg: None,
            calls: RefCell::new(vec![]),
        };
        let out = discover(&probe, &servers(), &[ip("10.0.0.1"), ip("10.0.0.2")], &[0]).unwrap();
        assert_eq!(out.mapped, vec![ip("10.0.0.1")]);
    }

    #[test]
    fn all_failures_yield_none() {
        let probe = MapProbe {
            from: vec![],
            fail: vec![ip("10.0.0.1")],
            fail_msg: None,
            calls: RefCell::new(vec![]),
        };
        assert!(discover(&probe, &servers(), &[ip("10.0.0.1")], &[0, 1, 2]).is_none());
        // A plain (non-EADDRINUSE) failure abandons the local after one try,
        // matching upstream's `return` on any other error.
        assert_eq!(*probe.calls.borrow(), vec![(ip("10.0.0.1"), 0)]);
    }

    #[test]
    fn addr_in_use_tries_next_port_others_abort_local() {
        let probe = MapProbe {
            from: vec![(ip("10.0.0.1"), ip("203.0.113.1"))],
            fail: vec![ip("10.0.0.9")],
            fail_msg: Some("dial udp: Address already in use (os error 98)".to_string()),
            calls: RefCell::new(vec![]),
        };
        let out = discover(
            &probe,
            &servers(),
            &[ip("10.0.0.9"), ip("10.0.0.1")],
            &[5000, 5001, 5002],
        )
        .unwrap();
        // 10.0.0.9 exhausted all three ports (EADDRINUSE keeps the loop going),
        // then 10.0.0.1 mapped on its first port.
        assert_eq!(out.mapped, vec![ip("10.0.0.1")]);
        assert_eq!(
            *probe.calls.borrow(),
            vec![
                (ip("10.0.0.9"), 5000),
                (ip("10.0.0.9"), 5001),
                (ip("10.0.0.9"), 5002),
                (ip("10.0.0.1"), 5000),
            ]
        );
    }

    #[test]
    fn stun_source_ports_match_upstream_branches() {
        // ICE range: 5 random samples inside [start, end).
        let ports = stun_source_ports(Some((50000, 60000)), None);
        assert_eq!(ports.len(), 5);
        assert!(ports.iter().all(|p| *p >= 50000 && *p < 60000));
        // Degenerate range (end <= start) falls through to mux/0 like upstream
        // would only reach the range branch with a valid window; here mux wins.
        assert_eq!(
            stun_source_ports(Some((60000, 50000)), Some(7882)),
            vec![7882]
        );
        // Mux mode: exactly the fixed first port.
        assert_eq!(stun_source_ports(None, Some(7882)), vec![7882]);
        // Range wins over mux when both provided (upstream if/else-if order).
        let both = stun_source_ports(Some((1, 3)), Some(7882));
        assert_eq!(both.len(), 5);
        assert!(both.iter().all(|p| *p == 1 || *p == 2));
        // Neither configured: OS-ephemeral 0.
        assert_eq!(stun_source_ports(None, None), vec![0]);
    }

    #[test]
    fn plain_failure_abandons_local_and_next_local_still_maps() {
        // Upstream returns on any non-EADDRINUSE error: 10.0.0.9 is abandoned
        // after one try; 10.0.0.1 still maps on its first port.
        let probe = MapProbe {
            from: vec![(ip("10.0.0.1"), ip("203.0.113.1"))],
            fail: vec![ip("10.0.0.9")],
            fail_msg: None,
            calls: RefCell::new(vec![]),
        };
        let out = discover(
            &probe,
            &servers(),
            &[ip("10.0.0.9"), ip("10.0.0.1")],
            &[5000, 5001],
        )
        .unwrap();
        assert_eq!(out.mapped, vec![ip("10.0.0.1")]);
        let calls = probe.calls.borrow();
        assert_eq!(*calls, vec![(ip("10.0.0.9"), 5000), (ip("10.0.0.1"), 5000)]);
    }

    #[test]
    fn empty_ports_defaults_to_zero() {
        let probe = MapProbe {
            from: vec![(ip("10.0.0.1"), ip("203.0.113.1"))],
            fail: vec![],
            fail_msg: None,
            calls: RefCell::new(vec![]),
        };
        let out = discover(&probe, &servers(), &[ip("10.0.0.1")], &[]).unwrap();
        assert_eq!(out.mapped, vec![ip("10.0.0.1")]);
        assert_eq!(*probe.calls.borrow(), vec![(ip("10.0.0.1"), 0)]);
    }

    #[test]
    fn filter_keeps_v6_and_mapped_v4_only() {
        let mapped = [ip("10.0.0.1")];
        assert!(external_ip_only_admits(ip("10.0.0.1"), &mapped));
        assert!(!external_ip_only_admits(ip("10.0.0.2"), &mapped));
        assert!(external_ip_only_admits(ip("2001:db8::1"), &mapped));
    }
    #[test]
    fn parallel_discovery_passes_validation_policy_and_preserves_distinct_mappings_and_self_fallback()
     {
        struct Probe(std::sync::Mutex<Vec<(IpAddr, bool)>>);
        impl NetProbe for Probe {
            fn local_addresses(&self) -> Vec<IpAddr> {
                vec![]
            }
            fn external_ip(&self, _: &[String], _: bool) -> Result<IpAddr, String> {
                Err("unused".into())
            }
            fn external_ip_from(
                &self,
                local: IpAddr,
                _: u16,
                _: &[String],
                validate: bool,
            ) -> Result<IpAddr, String> {
                self.0.lock().unwrap().push((local, validate));
                match local.to_string().as_str() {
                    "10.0.0.1" => Ok(ip("203.0.113.1")),
                    "10.0.0.2" => Ok(ip("203.0.113.2")),
                    _ => Err("no mapping".into()),
                }
            }
            fn sleep(&self, _: Duration) {}
        }
        let collision = finish_mapping(
            vec![ip("10.0.0.1")],
            vec![(ip("10.0.0.1"), ip("10.0.0.2"))],
            &[ip("10.0.0.1"), ip("10.0.0.2")],
        );
        assert_eq!(collision.rewrites, vec![(ip("10.0.0.2"), ip("10.0.0.2"))]);
        let probe = Probe(std::sync::Mutex::new(Vec::new()));
        let locals = [ip("10.0.0.1"), ip("10.0.0.2"), ip("10.0.0.3")];
        for skip in [false, true] {
            probe.0.lock().unwrap().clear();
            let out = discover_parallel(
                &probe,
                &servers(),
                &locals,
                &[0],
                skip,
                Duration::from_millis(80),
                Duration::from_millis(10),
            )
            .unwrap();
            let mut rewrite = out.rewrites.clone();
            rewrite.sort();
            assert_eq!(
                rewrite,
                vec![
                    (locals[0], ip("203.0.113.1")),
                    (locals[1], ip("203.0.113.2")),
                    (locals[2], locals[2])
                ]
            );
            assert_eq!(out.mapped.len(), 2);
            let calls = probe.0.lock().unwrap();
            assert_eq!(calls.len(), 3);
            assert!(calls.iter().all(|(_, validate)| *validate == !skip));
        }
    }
}
