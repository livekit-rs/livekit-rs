//! Named interface enumeration before actual media socket/candidate construction.
//! Source: fixed MTU rtcconfig.InterfaceFilterFromConf + transport.localInterfaces.
use rtc_core::config::{RtcConfig, UdpMode};
use rtc_runtime::net::{LocalInterface, NetConfig};
use std::{
    io,
    net::{IpAddr, Ipv6Addr},
};

#[derive(Clone, Debug)]
pub(crate) struct Device {
    pub name: String,
    /// Administrative UP, not carrier/operational RUNNING.
    pub up: bool,
    pub loopback: bool,
    /// None represents a real per-interface address query failure (Go skips it).
    pub addresses: Option<Vec<IpAddr>>,
}
pub(crate) trait InterfaceSource {
    fn devices(&self) -> io::Result<Vec<Device>>;
}
struct SystemInterfaces;
impl InterfaceSource for SystemInterfaces {
    fn devices(&self) -> io::Result<Vec<Device>> {
        getifs::interfaces().map(|devices| {
            devices
                .into_iter()
                .map(|device| Device {
                    name: device.name().to_string(),
                    up: device.flags().contains(getifs::Flags::UP),
                    loopback: device.flags().contains(getifs::Flags::LOOPBACK),
                    addresses: device
                        .addrs()
                        .ok()
                        .map(|a| a.into_iter().map(|a| a.addr()).collect()),
                })
                .collect()
        })
    }
}
fn named_allowed(rtc: &RtcConfig, name: &str) -> bool {
    if !rtc.interfaces.includes.is_empty() {
        rtc.interfaces.includes.iter().any(|n| n == name)
    } else {
        !rtc.interfaces.excludes.iter().any(|n| n == name)
    }
}
fn in_cidr(ip: IpAddr, (net, prefix): (IpAddr, u8)) -> bool {
    // Go IPNet.Contains normalizes both IPv4-mapped input and the masked
    // network number to IPv4, taking the last four bytes of an IPv6 mask.
    let ip = match ip {
        IpAddr::V6(v6) => v6.to_ipv4_mapped().map(IpAddr::V4).unwrap_or(ip),
        _ => ip,
    };
    match net {
        IpAddr::V4(net) => match ip {
            IpAddr::V4(ip) => {
                let p = u32::from(prefix.min(32));
                let mask = if p == 0 { 0 } else { u32::MAX << (32 - p) };
                u32::from(ip) & mask == u32::from(net) & mask
            }
            _ => false,
        },
        IpAddr::V6(net) => {
            let p = u32::from(prefix.min(128));
            let mask = if p == 0 { 0 } else { u128::MAX << (128 - p) };
            let number = Ipv6Addr::from(u128::from(net) & mask);
            if let Some(v4) = number.to_ipv4_mapped() {
                return in_cidr(ip, (IpAddr::V4(v4), prefix.saturating_sub(96)));
            }
            match ip {
                IpAddr::V6(ip) => u128::from(ip) & mask == u128::from(number),
                _ => false,
            }
        }
    }
}
fn ip_allowed(rtc: &RtcConfig, ip: IpAddr) -> bool {
    if !rtc.ips.includes.is_empty() {
        rtc.ips.includes.iter().any(|c| in_cidr(ip, *c))
    } else {
        !rtc.ips.excludes.iter().any(|c| in_cidr(ip, *c))
    }
}
fn supported_v6(ip: Ipv6Addr) -> bool {
    let b = ip.octets();
    !b[..12].iter().all(|n| *n == 0)
        && !(b[0] == 0xfe && (b[1] & 0xc0) == 0xc0)
        && !ip.is_unicast_link_local()
        && !(b[0] == 0xff && (b[1] & 0x0f) == 2)
}
/// Pure consumer reused by the real OS adapter and deterministic source oracle.
/// Empty selection stays empty: exclusions are never bypassed by a primary or
/// loopback fallback. Enumeration errors propagate before opening media sockets.
pub(crate) fn media_interfaces_from(
    rtc: &RtcConfig,
    _development: bool,
    source: &dyn InterfaceSource,
) -> io::Result<Vec<LocalInterface>> {
    let loopback = rtc.enable_loopback_candidate;
    let mut selected = Vec::new();
    for device in source.devices()? {
        if !device.up || (device.loopback && !loopback) || !named_allowed(rtc, &device.name) {
            continue;
        }
        let Some(addresses) = device.addresses else {
            continue;
        };
        for address in addresses {
            let address = match address {
                IpAddr::V6(v6) => v6
                    .to_ipv4_mapped()
                    .map(IpAddr::V4)
                    .unwrap_or(IpAddr::V6(v6)),
                ip => ip,
            };
            if address.is_loopback() && !loopback {
                continue;
            }
            if let IpAddr::V6(v6) = address
                && !supported_v6(v6)
            {
                continue;
            }
            // Fixed source retains each eligible occurrence; a duplicate
            // address at a fixed UDP port must reach the real bind error.
            if !ip_allowed(rtc, address) {
                continue;
            }
            // The upstream catch-all host rewrite applies to every admitted base.
            let mapped = if rtc.use_node_ip_mapping {
                match address {
                    IpAddr::V4(_) => rtc.node_ip.v4.map(IpAddr::V4),
                    IpAddr::V6(_) => rtc.node_ip.v6.map(IpAddr::V6),
                }
            } else {
                None
            };
            selected.push(LocalInterface {
                bind: address,
                advertise: mapped.unwrap_or(address),
            });
        }
    }
    Ok(selected)
}
pub fn media_interfaces(rtc: &RtcConfig, development: bool) -> io::Result<Vec<LocalInterface>> {
    media_interfaces_with_source(rtc, development, &SystemInterfaces)
}
fn media_interfaces_with_source(
    rtc: &RtcConfig,
    development: bool,
    source: &dyn InterfaceSource,
) -> io::Result<Vec<LocalInterface>> {
    let mut selected = media_interfaces_from(rtc, development, source)?;
    if rtc.use_node_ip_mapping && rtc.use_external_ip {
        // Discovery follows rtcconfig.GetLocalIPAddresses, which intentionally
        // differs from the later physical ICE bind eligibility (UP, supported
        // candidate IPv6). Do not collapse these two source consumers.
        let probe_ips = nat_discovery_addresses(rtc, source)?;
        if let Some(mapping) = external_mappings(rtc, &probe_ips) {
            apply_external_mappings(&mut selected, rtc, &mapping);
        }
        // With no mapping, media_interfaces_from retains the node-IP fallback,
        // exactly as fixed getNAT1to1IPsForConf's empty result branch.
    }
    Ok(selected)
}
/// Fixed GetLocalIPAddresses(includeLoopback,true,ifFilter,ipFilter).
/// No interface UP/carrier filter, no extra Pion candidate IPv6 restrictions,
/// no deduplication. Loopbacks follow non-loopbacks, or are the fallback when
/// no eligible non-loopback address exists even if inclusion was disabled.
fn nat_discovery_addresses(
    rtc: &RtcConfig,
    source: &dyn InterfaceSource,
) -> io::Result<Vec<IpAddr>> {
    let mut addresses = Vec::new();
    let mut loopbacks = Vec::new();
    for device in source.devices()? {
        if !named_allowed(rtc, &device.name) {
            continue;
        }
        let Some(local) = device.addresses else {
            continue;
        };
        for ip in local {
            let ip = match ip {
                IpAddr::V6(v6) => v6.to_ipv4_mapped().map(IpAddr::V4).unwrap_or(ip),
                _ => ip,
            };
            let link_local = match ip {
                IpAddr::V4(v4) => v4.is_link_local() || v4.octets()[..3] == [224, 0, 0],
                IpAddr::V6(v6) => {
                    v6.is_unicast_link_local() || (v6.segments()[0] & 0xff0f) == 0xff02
                }
            };
            if link_local || !ip_allowed(rtc, ip) {
                continue;
            }
            if ip.is_loopback() {
                loopbacks.push(ip);
            } else {
                addresses.push(ip);
            }
        }
    }
    if rtc.enable_loopback_candidate || addresses.is_empty() {
        addresses.extend(loopbacks);
    }
    if addresses.is_empty() {
        return Err(io::Error::new(
            io::ErrorKind::AddrNotAvailable,
            "could not find local IP address",
        ));
    }
    Ok(addresses)
}

fn apply_external_mappings(
    selected: &mut Vec<LocalInterface>,
    rtc: &RtcConfig,
    mapping: &lk_config::nat1to1::Nat1to1,
) {
    for iface in selected.iter_mut() {
        iface.advertise = mapping
            .rewrites
            .iter()
            .find(|(local, _)| *local == iface.bind)
            .map_or(iface.bind, |(_, external)| *external);
    }
    if rtc.external_ip_only {
        selected.retain(|iface| {
            lk_config::nat1to1::external_ip_only_admits(iface.bind, &mapping.mapped)
        });
    }
}
fn external_mappings(rtc: &RtcConfig, local_ips: &[IpAddr]) -> Option<lk_config::nat1to1::Nat1to1> {
    use lk_config::netdetect::DEFAULT_STUN_SERVERS;
    let servers: Vec<_> = if rtc.stun_servers.is_empty() {
        DEFAULT_STUN_SERVERS.iter().map(|s| s.to_string()).collect()
    } else {
        rtc.stun_servers.clone()
    };
    let (range, mux) = match &rtc.udp {
        UdpMode::Range { start, end } => (Some((*start, *end)), None),
        UdpMode::Mux { ports } => (None, ports.first().copied()),
        UdpMode::Disabled => (None, None),
    };
    lk_config::nat1to1::discover_system(
        &servers,
        local_ips,
        &lk_config::nat1to1::stun_source_ports(range, mux),
        rtc.skip_external_ip_validation,
    )
}
/// Source CPU-count primitives port Go1.26.3 runtime (Go Authors BSD-3-Clause;
/// existing LICENSES/Go-BSD-3-Clause.txt), with direct OS calls, no Go runtime.
/// Fixed MTU binds only the first min(configured ports, runtime.NumCPU())
/// ports. CPU count is host/affinity count, not cgroup CPU quota.
fn mux_ports_for_cpu(
    rtc: &RtcConfig,
    configured: lk_config::ports::PortRange,
    cpus: usize,
) -> Vec<i64> {
    // ForceTCP never configures the UDP mux. Go otherwise binds only the
    // first min(configured ports, runtime.NumCPU()) ports.
    if rtc.force_tcp || !matches!(rtc.udp, UdpMode::Mux { .. }) {
        return Vec::new();
    }
    configured.iter().take(cpus.max(1)).collect()
}
/// The fixed Go mux always gathers its standalone STUN port independently
/// of the configured port rotation. A duplicate bind remains a startup error.
pub fn standalone_stun_mux_port(rtc: &RtcConfig) -> Option<u16> {
    (!rtc.force_tcp && rtc.use_stun_port_as_ice && matches!(rtc.udp, UdpMode::Mux { .. }))
        .then_some(3478)
}

pub fn startup_mux_ports(rtc: &RtcConfig, configured: lk_config::ports::PortRange) -> Vec<i64> {
    static CPUS: std::sync::OnceLock<usize> = std::sync::OnceLock::new();
    mux_ports_for_cpu(rtc, configured, *CPUS.get_or_init(source_cpu_count))
}
#[cfg(target_os = "linux")]
fn source_cpu_count() -> usize {
    // Go runtime.getCPUCount: real raw sched_getaffinity, fixed 8192-byte
    // buffer, count only returned bytes, error/zero -> 1. No cgroup quota.
    let mut mask = [0u8; 8192];
    // SAFETY: the kernel writes at most mask.len() bytes into this live buffer.
    let n = unsafe {
        libc::syscall(
            libc::SYS_sched_getaffinity,
            0,
            mask.len(),
            mask.as_mut_ptr(),
        )
    };
    if n < 0 {
        return 1;
    }
    mask[..usize::try_from(n).expect("nonnegative affinity length")]
        .iter()
        .map(|v| v.count_ones() as usize)
        .sum::<usize>()
        .max(1)
}
#[cfg(target_os = "macos")]
fn source_cpu_count() -> usize {
    // Go runtime.getCPUCount uses CTL_HW/HW_NCPU, not available_parallelism.
    let mut mib = [libc::CTL_HW, libc::HW_NCPU];
    let mut value = 0u32;
    let mut size = std::mem::size_of_val(&value);
    // SAFETY: both old-value storage and its length are valid; no new value.
    let result = unsafe {
        libc::sysctl(
            mib.as_mut_ptr(),
            2,
            (&raw mut value).cast(),
            &raw mut size,
            std::ptr::null_mut(),
            0,
        )
    };
    if result >= 0 && (value as i32) > 0 {
        value as usize
    } else {
        1
    }
}
#[cfg(not(any(target_os = "linux", target_os = "macos")))]
fn source_cpu_count() -> usize {
    std::thread::available_parallelism()
        .map(usize::from)
        .unwrap_or(1)
}
pub fn net_config(rtc: &RtcConfig, development: bool) -> io::Result<NetConfig> {
    let interfaces = media_interfaces(rtc, development)?;
    let (udp_port, port_range) = match &rtc.udp {
        UdpMode::Mux { ports } => (ports.first().copied(), (0, 0)),
        UdpMode::Range { start, end } => (None, (*start, *end)),
        UdpMode::Disabled => (None, (0, 0)),
    };
    Ok(NetConfig {
        udp_enabled: !matches!(rtc.udp, UdpMode::Disabled) && !rtc.force_tcp,
        udp_port,
        port_range,
        tcp_port: rtc.tcp_port,
        interfaces,
    })
}
#[cfg(test)]
mod tests {
    use super::*;
    use std::net::Ipv4Addr;
    struct Source(Vec<Device>);
    impl InterfaceSource for Source {
        fn devices(&self) -> io::Result<Vec<Device>> {
            Ok(self.0.clone())
        }
    }
    pub(super) fn rtc() -> RtcConfig {
        rtc_with_inputs(&[], &lk_config::Env::default())
    }

    pub(super) fn rtc_with_inputs(extra: &[&str], env: &lk_config::Env) -> RtcConfig {
        struct NoNetwork;
        impl lk_config::netdetect::NetProbe for NoNetwork {
            fn local_addresses(&self) -> Vec<IpAddr> {
                vec!["127.0.0.1".parse().unwrap()]
            }
            fn external_ip(&self, _: &[String], _: bool) -> Result<IpAddr, String> {
                panic!("fixture must not discover external IP")
            }
            fn sleep(&self, _: std::time::Duration) {
                panic!("fixture must not sleep/retry")
            }
        }
        let info = lk_config::ProgramInfo {
            binary_name: "livecloud".into(),
            product_version: "fixture".into(),
            compat_version: "1.13.10",
        };
        let args = ["livecloud", "--dev", "--node-ip", "127.0.0.1"]
            .into_iter()
            .chain(extra.iter().copied())
            .map(Into::into)
            .collect();
        match lk_config::parse_with_probe(args, env, &info, &NoNetwork).unwrap() {
            lk_config::EntryAction::RunServer(c) => c.server.rtc,
            _ => panic!("expected server config"),
        }
    }
    fn device(name: &str, addresses: &[&str]) -> Device {
        Device {
            name: name.into(),
            up: true,
            loopback: false,
            addresses: Some(addresses.iter().map(|s| s.parse().unwrap()).collect()),
        }
    }
    #[test]
    fn nat_probe_enumeration_keeps_source_down_interfaces_duplicates_and_loopback_fallback() {
        let mut cfg = rtc();
        cfg.enable_loopback_candidate = false;
        let mut down = device(
            "down",
            &[
                "10.0.0.2",
                "::2",
                "fec0::1",
                "169.254.1.1",
                "224.0.0.1",
                "fe80::1",
                "ff12::1",
            ],
        );
        down.up = false;
        let source = Source(vec![
            device("loop", &["127.0.0.1", "::1"]),
            down,
            device("active", &["10.0.0.1", "10.0.0.1"]),
        ]);
        let addresses = nat_discovery_addresses(&cfg, &source).unwrap();
        assert_eq!(
            addresses
                .iter()
                .map(ToString::to_string)
                .collect::<Vec<_>>(),
            ["10.0.0.2", "::2", "fec0::1", "10.0.0.1", "10.0.0.1"]
        );
        assert_eq!(
            media_interfaces_from(&cfg, false, &source)
                .unwrap()
                .iter()
                .map(|i| i.bind.to_string())
                .collect::<Vec<_>>(),
            ["10.0.0.1", "10.0.0.1"],
            "physical bind selection remains distinct"
        );
        cfg.enable_loopback_candidate = true;
        let addresses = nat_discovery_addresses(&cfg, &source).unwrap();
        assert_eq!(
            &addresses[addresses.len() - 2..],
            &[
                "127.0.0.1".parse::<IpAddr>().unwrap(),
                "::1".parse().unwrap()
            ]
        );
        assert!(
            media_interfaces_from(&cfg, false, &source)
                .unwrap()
                .iter()
                .all(|iface| iface.bind != "::1".parse::<IpAddr>().unwrap()),
            "fixed ICE candidate restrictions remain separate from discovery"
        );
        cfg.enable_loopback_candidate = false;
        let loopback = Source(vec![device("loop", &["127.0.0.1", "::1"])]);
        assert_eq!(
            nat_discovery_addresses(&cfg, &loopback).unwrap().len(),
            2,
            "source fallback despite disabled loopback candidates"
        );
        assert!(
            media_interfaces_from(&cfg, false, &loopback)
                .unwrap()
                .is_empty()
        );
    }

    #[test]
    fn nat_probe_filters_preserve_name_include_precedence_ip_filters_and_query_failures() {
        let mut cfg = rtc();
        cfg.interfaces.includes = vec!["selected".into()];
        cfg.interfaces.excludes = vec!["selected".into()];
        let mut down = device("selected", &["10.0.0.1", "192.0.2.1", "127.0.0.1"]);
        down.up = false;
        let mut query_failed = device("selected", &[]);
        query_failed.addresses = None;
        let source = Source(vec![device("excluded", &["10.0.0.99"]), query_failed, down]);
        assert_eq!(
            nat_discovery_addresses(&cfg, &source)
                .unwrap()
                .iter()
                .map(ToString::to_string)
                .collect::<Vec<_>>(),
            ["10.0.0.1", "192.0.2.1"]
        );
        cfg.ips.excludes = vec![("10.0.0.0".parse().unwrap(), 8)];
        assert_eq!(
            nat_discovery_addresses(&cfg, &source).unwrap(),
            vec!["192.0.2.1".parse::<IpAddr>().unwrap()]
        );
        cfg.ips.includes = vec![("10.0.0.0".parse().unwrap(), 8)];
        assert_eq!(
            nat_discovery_addresses(&cfg, &source).unwrap(),
            vec!["10.0.0.1".parse::<IpAddr>().unwrap()],
            "IP includes take precedence over excludes"
        );
        cfg.interfaces.includes = vec!["missing".into()];
        assert_eq!(
            nat_discovery_addresses(&cfg, &source)
                .unwrap_err()
                .to_string(),
            "could not find local IP address"
        );
    }

    #[test]
    fn actual_stun_rewrites_each_bound_interface_even_when_external_ip_only_is_false() {
        use std::net::UdpSocket;
        let stun = UdpSocket::bind("127.0.0.1:0").unwrap();
        let endpoint = stun.local_addr().unwrap().to_string();
        stun.set_read_timeout(Some(std::time::Duration::from_secs(3)))
            .unwrap();
        let fixture = std::thread::spawn(move || {
            let mut request = [0; 1500];
            let (size, peer) = stun.recv_from(&mut request).unwrap();
            assert_eq!(size, 20);
            let mut reply = vec![1, 1, 0, 12];
            reply.extend_from_slice(&[0x21, 0x12, 0xa4, 0x42]);
            reply.extend_from_slice(&request[8..20]);
            reply.extend_from_slice(&[0, 0x20, 0, 8, 0, 1, 0x21, 0x12]);
            reply.extend_from_slice(&[127 ^ 0x21, 0x12, 0xa4, 2 ^ 0x42]);
            stun.send_to(&reply, peer).unwrap();
        });
        let mut cfg = rtc();
        cfg.use_external_ip = true;
        cfg.use_node_ip_mapping = true;
        cfg.skip_external_ip_validation = true;
        cfg.external_ip_only = false;
        cfg.enable_loopback_candidate = true;
        cfg.stun_servers = vec![endpoint];
        cfg.udp = UdpMode::Disabled;
        let interfaces = media_interfaces_with_source(
            &cfg,
            false,
            // ::1 is not an admissible ICE IPv6 candidate in the pinned ICE
            // implementation (net.go isSupportedIPv6Partial rejects ::/96).
            // Use an admissible unresolved IPv6 to verify self-map retention.
            &Source(vec![device("owned-fixture", &["127.0.0.1", "2001:db8::1"])]),
        )
        .unwrap();
        fixture.join().unwrap();
        assert_eq!(interfaces.len(), 2);
        assert_eq!(interfaces[0].bind, "127.0.0.1".parse::<IpAddr>().unwrap());
        assert_eq!(
            interfaces[0].advertise,
            "127.0.0.2".parse::<IpAddr>().unwrap()
        );
        assert_eq!(interfaces[1].bind, "2001:db8::1".parse::<IpAddr>().unwrap());
        assert_eq!(
            interfaces[1].bind, interfaces[1].advertise,
            "unresolved IPv6 retains its self mapping"
        );
    }

    #[test]
    fn distinct_external_mappings_and_unresolved_local_self_mapping_are_real_candidate_rewrites() {
        let mut cfg = rtc();
        cfg.use_external_ip = true;
        cfg.use_node_ip_mapping = true;
        let mut selected = vec![
            LocalInterface {
                bind: "10.0.0.1".parse().unwrap(),
                advertise: "203.0.113.99".parse().unwrap(),
            },
            LocalInterface {
                bind: "10.0.0.2".parse().unwrap(),
                advertise: "203.0.113.99".parse().unwrap(),
            },
            LocalInterface {
                bind: "10.0.0.3".parse().unwrap(),
                advertise: "203.0.113.99".parse().unwrap(),
            },
        ];
        let mapping = lk_config::nat1to1::Nat1to1 {
            mapped: vec!["10.0.0.1".parse().unwrap(), "10.0.0.2".parse().unwrap()],
            rewrites: vec![
                ("10.0.0.1".parse().unwrap(), "203.0.113.1".parse().unwrap()),
                ("10.0.0.2".parse().unwrap(), "203.0.113.2".parse().unwrap()),
                ("10.0.0.3".parse().unwrap(), "10.0.0.3".parse().unwrap()),
            ],
        };
        cfg.external_ip_only = false;
        apply_external_mappings(&mut selected, &cfg, &mapping);
        assert_eq!(
            selected
                .iter()
                .map(|i| i.advertise.to_string())
                .collect::<Vec<_>>(),
            ["203.0.113.1", "203.0.113.2", "10.0.0.3"]
        );
        cfg.external_ip_only = true;
        apply_external_mappings(&mut selected, &cfg, &mapping);
        assert_eq!(selected.len(), 2);
        assert_eq!(
            selected[1].advertise,
            "203.0.113.2".parse::<IpAddr>().unwrap()
        );
    }

    #[test]
    fn exact_names_include_precedence_and_all_addresses_not_primary_only() {
        let mut rtc = rtc();
        rtc.interfaces.includes = vec!["en1".into()];
        rtc.interfaces.excludes = vec!["en1".into()];
        let source = Source(vec![
            device("en0", &["10.0.0.1"]),
            device("en1", &["10.1.0.1", "10.1.0.2"]),
            device("en10", &["10.2.0.1"]),
        ]);
        let ips = media_interfaces_from(&rtc, false, &source).unwrap();
        assert_eq!(
            ips.iter().map(|i| i.bind.to_string()).collect::<Vec<_>>(),
            ["10.1.0.1", "10.1.0.2"]
        );
        rtc.interfaces.includes = vec!["en*".into()];
        assert!(
            media_interfaces_from(&rtc, true, &source)
                .unwrap()
                .is_empty()
        );
        rtc.interfaces.includes.clear();
        rtc.interfaces.excludes = vec!["en1".into()];
        assert_eq!(
            media_interfaces_from(&rtc, false, &source).unwrap().len(),
            2
        );
    }
    #[test]
    fn administrative_up_is_not_running_and_query_error_is_not_fallback() {
        let rtc = rtc();
        let mut down = device("down", &["10.0.0.1"]);
        down.up = false;
        let missing = Device {
            name: "missing".into(),
            up: true,
            loopback: false,
            addresses: None,
        };
        assert!(
            media_interfaces_from(&rtc, true, &Source(vec![down, missing]))
                .unwrap()
                .is_empty()
        );
        struct Failure;
        impl InterfaceSource for Failure {
            fn devices(&self) -> io::Result<Vec<Device>> {
                Err(io::Error::other("fixture enum failure"))
            }
        }
        assert_eq!(
            media_interfaces_from(&rtc, true, &Failure)
                .unwrap_err()
                .to_string(),
            "fixture enum failure"
        );
    }
    #[test]
    fn loopback_and_ip_filters_apply_together_with_no_backdoor() {
        let mut rtc = rtc();
        rtc.enable_loopback_candidate = false;
        let mut lo = device("lo0", &["127.0.0.1", "::1"]);
        lo.loopback = true;
        let source = Source(vec![lo, device("en0", &["10.0.0.1", "10.0.1.1"])]);
        assert_eq!(
            media_interfaces_from(&rtc, false, &source).unwrap().len(),
            2
        );
        rtc.interfaces.includes = vec!["lo0".into()];
        assert!(
            media_interfaces_from(&rtc, false, &source)
                .unwrap()
                .is_empty()
        );
        // Source F126 actual8: development does not enable loopback. The
        // previous authored expectation is retained in the candidate history.
        assert!(
            media_interfaces_from(&rtc, true, &source)
                .unwrap()
                .is_empty()
        );
        rtc.enable_loopback_candidate = true;
        assert_eq!(
            media_interfaces_from(&rtc, true, &source).unwrap()[0]
                .bind
                .to_string(),
            "127.0.0.1"
        );
        rtc.ips.excludes = vec![("127.0.0.0".parse().unwrap(), 8)];
        assert!(
            media_interfaces_from(&rtc, true, &source)
                .unwrap()
                .is_empty()
        );
    }
    #[test]
    fn source_ipv6_rules_address_occurrences_and_nat_are_explicit() {
        let mut rtc = rtc();
        rtc.use_node_ip_mapping = true;
        rtc.node_ip.v4 = Some("203.0.113.1".parse().unwrap());
        let source = Source(vec![
            device(
                "first",
                &[
                    "10.0.0.1",
                    "2001:db8::1",
                    "fe80::1",
                    "fec0::1",
                    "ff02::1",
                    "::2",
                ],
            ),
            device("second", &["10.0.0.1"]),
        ]);
        let ips = media_interfaces_from(&rtc, false, &source).unwrap();
        assert_eq!(ips.len(), 3);
        assert_eq!(
            ips[0].bind, ips[2].bind,
            "fixed source does not deduplicate occurrences across devices"
        );
        assert_eq!(ips[0].advertise.to_string(), "203.0.113.1");
        assert_eq!(ips[1].advertise.to_string(), "2001:db8::1");
    }
    #[tokio::test]
    async fn real_named_loopback_selection_drives_actual_udp_socket_and_payload() {
        use rtc_core::ids::TransportId;
        use rtc_runtime::net::NetProto;
        let devices = SystemInterfaces.devices().unwrap();
        let selected = devices
            .iter()
            .find(|d| {
                d.up && d.loopback
                    && d.addresses
                        .as_ref()
                        .is_some_and(|a| a.iter().any(|ip| ip.is_ipv4() && ip.is_loopback()))
            })
            .expect("actual up IPv4 loopback required");
        let mut rtc = rtc();
        rtc.interfaces.includes = vec![selected.name.clone()];
        rtc.enable_loopback_candidate = true;
        rtc.use_node_ip_mapping = false;
        rtc.tcp_port = None;
        rtc.udp = UdpMode::Mux { ports: vec![0] };
        let cfg = net_config(&rtc, false).unwrap();
        assert!(!cfg.interfaces.is_empty());
        assert!(
            cfg.interfaces
                .iter()
                .all(|i| i.bind.is_loopback() && i.bind.is_ipv4())
        );
        let net = rtc_runtime::net::NetRouter::start(cfg).await.unwrap();
        let (sink, _) = tokio::sync::mpsc::channel(1);
        let candidates = net
            .open_transport(TransportId::next(), "f08-named-loopback", sink)
            .unwrap();
        assert!(!candidates.is_empty());
        assert!(candidates.iter().all(|c| c.addr.ip().is_loopback()));
        let receive = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let actual = candidates[0].addr;
        net.sender().send(
            NetProto::Udp,
            actual,
            receive.local_addr().unwrap(),
            b"f08-real-selected-interface",
        );
        let mut bytes = [0u8; 64];
        let (len, source) = tokio::time::timeout(
            std::time::Duration::from_secs(2),
            receive.recv_from(&mut bytes),
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(&bytes[..len], b"f08-real-selected-interface");
        assert_eq!(source, actual);
        net.shutdown();
        rtc.interfaces.includes = vec!["f08-no-such-interface".into()];
        assert!(
            net_config(&rtc, true).unwrap().interfaces.is_empty(),
            "even dev may not bypass explicit name filter"
        );
        let empty = rtc_runtime::net::NetRouter::start(net_config(&rtc, true).unwrap())
            .await
            .unwrap();
        let (sink, _) = tokio::sync::mpsc::channel(1);
        assert!(
            empty
                .open_transport(TransportId::next(), "f08-filter-empty", sink)
                .unwrap()
                .is_empty()
        );
        empty.shutdown();
    }
    #[test]
    fn cidr_matching() {
        let net = (IpAddr::V4(Ipv4Addr::new(10, 0, 0, 0)), 8);
        assert!(in_cidr(IpAddr::V4(Ipv4Addr::new(10, 1, 2, 3)), net));
        assert!(!in_cidr(IpAddr::V4(Ipv4Addr::new(11, 1, 2, 3)), net));
        assert!(in_cidr(
            IpAddr::V4(Ipv4Addr::new(1, 2, 3, 4)),
            (IpAddr::V4(Ipv4Addr::UNSPECIFIED), 0)
        ));
    }
    /// Independently executed fixed-Go MTU real UDP bindings, same host snapshot.
    /// The caller keeps raw machine addresses private and records only safe hashes.
    #[test]
    #[ignore = "requires F106_GO_SYSTEM_RESULTS from actual fixed-Go same-host oracle"]
    fn actual_go_system_mux_addresses_match_real_os_selection() {
        let input =
            std::fs::read_to_string(std::env::var("F106_GO_SYSTEM_RESULTS").unwrap()).unwrap();
        let rows: Vec<serde_json::Value> = input
            .lines()
            .map(|line| serde_json::from_str(line).unwrap())
            .collect();
        for (id, loopback) in [
            ("actual-all-with-loopback", true),
            ("actual-all-without-loopback", false),
        ] {
            let row = rows
                .iter()
                .find(|v| v["id"] == id)
                .expect("real Go mux row required");
            assert!(
                row.get("error").is_none(),
                "actual upstream mux bind must succeed"
            );
            let mut config = rtc();
            config.interfaces.includes.clear();
            config.interfaces.excludes.clear();
            config.ips.includes.clear();
            config.ips.excludes.clear();
            config.enable_loopback_candidate = loopback;
            config.use_node_ip_mapping = false;
            let mut actual: Vec<_> = media_interfaces(&config, false)
                .unwrap()
                .into_iter()
                .map(|i| {
                    assert_eq!(i.bind, i.advertise);
                    i.bind.to_string()
                })
                .collect();
            actual.sort();
            let expected: Vec<_> = row["actual_bound_hosts"]
                .as_array()
                .unwrap()
                .iter()
                .map(|v| v.as_str().unwrap().to_owned())
                .collect();
            assert_eq!(
                actual, expected,
                "actual same-host enumeration must match actual Go mux family/flag/filter rules"
            );
        }
    }

    #[test]
    fn mapped_ipv6_cidr_matches_ipv4_like_go_contains() {
        for (network, prefix, ip, expected) in [
            ("::ffff:192.0.2.0", 120, "192.0.2.9", true),
            ("::ffff:192.0.2.0", 120, "192.0.3.9", false),
            ("::ffff:0:0", 96, "203.0.113.9", true),
            ("::ffff:192.0.2.9", 128, "192.0.2.9", true),
            ("::ffff:192.0.2.9", 128, "::ffff:192.0.2.10", false),
            ("::ffff:192.0.2.0", 95, "192.0.2.9", false),
        ] {
            assert_eq!(
                in_cidr(ip.parse().unwrap(), (network.parse().unwrap(), prefix)),
                expected
            );
        }
    }
    #[tokio::test]
    async fn duplicate_eligible_occurrences_reach_actual_fixed_port_bind_failure() {
        let mut config = rtc();
        config.use_node_ip_mapping = false;
        config.enable_loopback_candidate = true;
        config.tcp_port = None;
        let reserve = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
        let port = reserve.local_addr().unwrap().port();
        drop(reserve);
        config.udp = UdpMode::Mux { ports: vec![port] };
        let selected = media_interfaces_from(
            &config,
            true,
            &Source(vec![
                device("first", &["127.0.0.1"]),
                device("second", &["127.0.0.1"]),
            ]),
        )
        .unwrap();
        assert_eq!(selected.len(), 2);
        let cfg = NetConfig {
            udp_enabled: true,
            udp_port: Some(port),
            port_range: (0, 0),
            tcp_port: None,
            interfaces: selected,
        };
        let error = match rtc_runtime::net::NetRouter::start(cfg).await {
            Ok(router) => {
                router.shutdown();
                panic!("fixed Go repeats ListenUDP and must not silently deduplicate")
            }
            Err(error) => error,
        };
        assert_eq!(error.kind(), io::ErrorKind::AddrInUse);
        std::net::UdpSocket::bind((std::net::Ipv4Addr::LOCALHOST, port))
            .expect("failed transaction releases first occurrence socket");
    }

    #[test]
    #[ignore = "requires actual Go edges oracle output, not authored expected values"]
    fn actual_go_mapped_cidr_predicate_rows() {
        let input =
            std::fs::read_to_string(std::env::var("F106_GO_EDGES_RESULTS").unwrap()).unwrap();
        let mut predicates = 0;
        let mut binds = 0;
        for line in input.lines() {
            let row: serde_json::Value = serde_json::from_str(line).unwrap();
            match row["family"].as_str().unwrap() {
                "mapped-cidr" => {
                    let (cidr, prefix) = row["cidr"].as_str().unwrap().split_once('/').unwrap();
                    let mut config = rtc();
                    config.ips.includes.clear();
                    config.ips.excludes.clear();
                    let network = (cidr.parse().unwrap(), prefix.parse().unwrap());
                    if row["exclude"].as_bool().unwrap() {
                        config.ips.excludes.push(network)
                    } else {
                        config.ips.includes.push(network)
                    };
                    assert_eq!(
                        ip_allowed(&config, row["ip"].as_str().unwrap().parse().unwrap()),
                        row["allowed"].as_bool().unwrap(),
                        "actual Go predicate mismatch: {}",
                        row
                    );
                    predicates += 1;
                }
                "duplicate-occurrence-fixed-port" => {
                    assert_eq!(row["actual_error"], true);
                    assert_eq!(row["addr_in_use"], true);
                    assert_eq!(row["first_socket_released"], true);
                    assert_eq!(row["returned_muxes"], 0);
                    binds += 1;
                }
                _ => panic!("unexpected actual Go row"),
            }
        }
        assert_eq!(predicates, 40);
        assert_eq!(binds, 1);
    }
}

#[cfg(test)]
mod configured_port_tests {
    use super::*;
    use lk_config::ports::PortRange;

    const SINGLE_PORT: PortRange = PortRange {
        start: 7882,
        end: 0,
    };
    #[test]
    fn configured_mux_ports_keep_source_cpu_prefix_and_non_mux_modes() {
        let mut rtc = super::tests::rtc();
        rtc.udp = UdpMode::Mux {
            ports: vec![40001, 40002, 40003],
        };
        let configured = PortRange {
            start: 40001,
            end: 40003,
        };
        assert_eq!(mux_ports_for_cpu(&rtc, configured, 2), vec![40001, 40002]);
        assert_eq!(mux_ports_for_cpu(&rtc, configured, 1), vec![40001]);
        assert_eq!(
            mux_ports_for_cpu(&rtc, configured, 8),
            vec![40001, 40002, 40003]
        );
        rtc.udp = UdpMode::Range {
            start: 40001,
            end: 40003,
        };
        assert!(mux_ports_for_cpu(&rtc, configured, 8).is_empty());
        rtc.udp = UdpMode::Disabled;
        assert!(mux_ports_for_cpu(&rtc, configured, 8).is_empty());
    }
    #[test]
    fn public_stun_mux_flag_selects_standalone_port_through_yaml_env_and_cli() {
        let yaml = "rtc:\n  udp_port: 7882\n  use_stun_port_as_ice: true\n";
        let disabled = lk_config::Env::from_pairs([("LIVEKIT_RTC_USE_STUN_PORT_AS_ICE", "false")]);
        let yaml_enabled =
            super::tests::rtc_with_inputs(&["--config-body", yaml], &lk_config::Env::default());
        let env_disabled = super::tests::rtc_with_inputs(&["--config-body", yaml], &disabled);
        let cli_enabled = super::tests::rtc_with_inputs(
            &["--config-body", yaml, "--rtc.use_stun_port_as_ice=true"],
            &disabled,
        );
        for (config, standalone) in [
            (yaml_enabled, Some(3478)),
            (env_disabled, None),
            (cli_enabled, Some(3478)),
        ] {
            assert_eq!(standalone_stun_mux_port(&config), standalone);
            assert_eq!(mux_ports_for_cpu(&config, SINGLE_PORT, 8), vec![7882]);
        }
    }

    #[test]
    fn force_tcp_drops_udp_and_stun_port_as_ice_adds_3478_only_in_mux_mode() {
        // ForceTCP: no mux ports and UDP disabled in the net stack.
        let mut rtc = super::tests::rtc();
        rtc.udp = UdpMode::Mux { ports: vec![7882] };
        rtc.force_tcp = true;
        rtc.tcp_port = None;
        assert!(mux_ports_for_cpu(&rtc, SINGLE_PORT, 8).is_empty());
        rtc.tcp_port = Some(7881);
        assert!(!net_config(&rtc, false).unwrap().udp_enabled);
        assert_eq!(standalone_stun_mux_port(&rtc), None);
        // UseStunPortAsICE creates a separate mux in mux mode.
        rtc.force_tcp = false;
        assert_eq!(mux_ports_for_cpu(&rtc, SINGLE_PORT, 8), vec![7882]);
        rtc.use_stun_port_as_ice = true;
        assert_eq!(mux_ports_for_cpu(&rtc, SINGLE_PORT, 8), vec![7882]);
        assert_eq!(standalone_stun_mux_port(&rtc), Some(3478));
        rtc.force_tcp = true;
        assert_eq!(standalone_stun_mux_port(&rtc), None);
        rtc.force_tcp = false;
        // Range mode ignores the STUN-port mux (upstream only builds it under
        // a valid UDPPort mux), and the flag alone keeps UDP enabled.
        rtc.udp = UdpMode::Range {
            start: 50000,
            end: 60000,
        };
        assert!(mux_ports_for_cpu(&rtc, SINGLE_PORT, 8).is_empty());
        assert_eq!(standalone_stun_mux_port(&rtc), None);
        assert!(net_config(&rtc, false).unwrap().udp_enabled);
    }
    #[test]
    #[ignore = "requires NETWORK_MULTIPORT_GO_RESULTS from actual same-host fixed Go observer"]
    fn actual_go_multiport_cpu_and_source_sequence() {
        let path = std::env::var("NETWORK_MULTIPORT_GO_RESULTS").unwrap();
        let input = std::fs::read_to_string(path).unwrap();
        let rows: Vec<serde_json::Value> = input
            .lines()
            .map(|s| serde_json::from_str(s).unwrap())
            .collect();
        assert_eq!(rows.len(), 4);
        for (row, index) in rows.iter().zip([1u64, 0, 1, 0]) {
            assert_eq!(
                row["source_num_cpu"].as_u64().unwrap() as usize,
                source_cpu_count()
            );
            assert_eq!(row["configured_ports_count"], 2);
            assert_eq!(row["bound_muxes_count"], 2);
            assert_eq!(row["registered_count"], 1);
            assert_eq!(row["candidate_count"], 1);
            for field in [
                "selected_configured_port_index",
                "candidate_configured_port_index",
                "actual_reply_configured_port_index",
            ] {
                assert_eq!(row[field].as_u64().unwrap(), index);
            }
        }
    }

    #[tokio::test]
    async fn normalized_development_does_not_override_explicit_loopback_interface_admission() {
        use rtc_core::ids::TransportId;
        struct NoDiscovery;
        impl lk_config::netdetect::NetProbe for NoDiscovery {
            fn local_addresses(&self) -> Vec<IpAddr> {
                panic!("explicit NodeIP must avoid discovery")
            }
            fn external_ip(&self, _: &[String], _: bool) -> Result<IpAddr, String> {
                panic!("explicit NodeIP must avoid STUN")
            }
            fn sleep(&self, _: std::time::Duration) {
                panic!("no retry in explicit NodeIP fixture")
            }
        }
        let devices = SystemInterfaces.devices().unwrap();
        let lo = devices
            .iter()
            .find(|d| {
                d.up && d.loopback
                    && d.addresses
                        .as_ref()
                        .is_some_and(|a| a.iter().any(|ip| ip.is_ipv4() && ip.is_loopback()))
            })
            .expect("existing UP IPv4 loopback required")
            .name
            .clone();
        let info = lk_config::ProgramInfo {
            binary_name: "livecloud".into(),
            product_version: "source-consumer".into(),
            compat_version: "1.13.10",
        };
        for development in [false, true] {
            for explicit in [false, true] {
                let body = format!(
                    "development: {development}\nkeys:\n  source-loopback-fixture: source-loopback-fixture-secret-32-bytes\nrtc:\n  node_ip: 127.0.0.1\n  use_external_ip: false\n  enable_loopback_candidate: {explicit}\n"
                );
                let args = vec!["livecloud".into(), "--config-body".into(), body.into()];
                let loaded = match lk_config::parse_with_probe(
                    args,
                    &lk_config::Env::default(),
                    &info,
                    &NoDiscovery,
                )
                .unwrap()
                {
                    lk_config::EntryAction::RunServer(c) => c,
                    _ => panic!("server config required"),
                };
                assert_eq!(loaded.server.development, development);
                let mut rtc = loaded.server.rtc;
                assert_eq!(
                    rtc.enable_loopback_candidate, explicit,
                    "actual loader must retain the explicit flag"
                );
                rtc.interfaces.includes = vec![lo.clone()];
                // Only resource allocation is ephemeral in this consumer test;
                // development, explicit flag and NodeIP mapping are actual loader outputs.
                rtc.tcp_port = None;
                rtc.udp = UdpMode::Mux { ports: vec![0] };
                let cfg = net_config(&rtc, development).unwrap();
                assert_eq!(
                    cfg.interfaces.is_empty(),
                    !explicit,
                    "fixed Go source gates loopback only by explicit flag, dev={development}"
                );
                let net = rtc_runtime::net::NetRouter::start(cfg).await.unwrap();
                let id = TransportId::next();
                let (tx, _) = tokio::sync::mpsc::channel(8);
                let candidates = net.open_transport(id, "source-loopback", tx).unwrap();
                assert_eq!(
                    candidates.is_empty(),
                    !explicit,
                    "real socket/candidate consumer must not fallback"
                );
                if explicit {
                    assert!(
                        candidates
                            .iter()
                            .all(|c| c.addr.ip().is_loopback() && c.base.ip().is_loopback())
                    );
                    let peer = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
                    let c = candidates[0];
                    let sender = net.sender();
                    // The same nonblocking production sink is retried only while
                    // the actual OS readiness handshake becomes writable.
                    let mut bytes = [0; 64];
                    let (n, from) =
                        tokio::time::timeout(std::time::Duration::from_secs(2), async {
                            loop {
                                sender.send(
                                    rtc_runtime::net::NetProto::Udp,
                                    c.base,
                                    peer.local_addr().unwrap(),
                                    b"source-explicit-loopback",
                                );
                                match peer.try_recv_from(&mut bytes) {
                                    Ok(packet) => break packet,
                                    Err(e) if e.kind() == io::ErrorKind::WouldBlock => {
                                        tokio::time::sleep(std::time::Duration::from_millis(1))
                                            .await
                                    }
                                    Err(e) => panic!("actual peer receive: {e}"),
                                }
                            }
                        })
                        .await
                        .unwrap();
                    assert_eq!(&bytes[..n], b"source-explicit-loopback");
                    assert_eq!(from, c.base);
                }
                net.close_transport(id);
                net.shutdown();
            }
        }
    }
}
