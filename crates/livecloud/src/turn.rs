//! Embedded TURN (G-08): official `turn.*` settings → rtc-turn.

use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};
use std::sync::Arc;
use std::time::Duration;

use lk_config::official::RtcConfig;
use rtc_core::config::ServerConfig;
use rtc_turn::{ListenerSpec, LocalRelayPolicy, TurnHandle, TurnServer, TurnServerConfig};

#[derive(Debug, thiserror::Error)]
pub enum TurnStartError {
    #[error("could not configure TURN/TLS: {0}")]
    Tls(String),
    #[error("could not start TURN server: {0}")]
    Start(String),
}

/// Validate and build the TLS `StreamAcceptor` from `TurnConfig` fields.
/// Returns `None` when TLS is not required (no tls_port or external_tls).
/// Returns `Err` when cert/key files are missing or invalid.
fn build_tls_acceptor(
    t: &rtc_core::config::TurnConfig,
) -> Result<Option<Arc<dyn rtc_turn::StreamAcceptor>>, TurnStartError> {
    if t.tls_port.is_some() && !t.external_tls {
        let cert = t
            .cert_file
            .as_ref()
            .ok_or_else(|| TurnStartError::Tls("turn.cert_file is required".into()))?;
        let key = t
            .key_file
            .as_ref()
            .ok_or_else(|| TurnStartError::Tls("turn.key_file is required".into()))?;
        let config =
            rtc_tls::server_config(cert, key).map_err(|e| TurnStartError::Tls(e.to_string()))?;
        Ok(Some(
            Arc::new(TurnTls(rtc_tls::tokio_rustls::TlsAcceptor::from(Arc::new(
                config,
            )))) as Arc<dyn rtc_turn::StreamAcceptor>,
        ))
    } else {
        Ok(None)
    }
}

// Fixed Go service/turnrelay.go:getTURNMediaPorts chooses from the public
// config, before normalized runtime port modes discard the original ranges.
fn media_ports(rtc: &RtcConfig) -> Vec<(u16, u16)> {
    if rtc.force_tcp {
        return Vec::new();
    }
    if rtc.port_range_start != 0 && rtc.port_range_end != 0 {
        return vec![(rtc.port_range_start as u16, rtc.port_range_end as u16)];
    }
    if !rtc.udp_port.valid() {
        return Vec::new();
    }
    let mut ports = vec![(
        rtc.udp_port.start as u16,
        rtc.udp_port.start.max(rtc.udp_port.end) as u16,
    )];
    if rtc.use_stun_port_as_ice {
        ports.push((3478, 3478));
    }
    ports
}

fn local_relay_policy(rtc: &RtcConfig) -> Result<LocalRelayPolicy, TurnStartError> {
    // Enumerate all interface addresses without applying the ICE bind filters.
    let addresses = getifs::interface_addrs().map_err(|error| {
        TurnStartError::Start(format!(
            "could not get local IPs for TURN relay policy: {error}"
        ))
    })?;
    let mut local_ips: Vec<IpAddr> = addresses.into_iter().map(|addr| addr.addr()).collect();
    for node_ip in [&rtc.node_ip.v4, &rtc.node_ip.v6] {
        if node_ip.is_empty() {
            continue;
        }
        match node_ip.parse::<IpAddr>() {
            Ok(ip) => local_ips.push(ip),
            Err(error) => {
                // Go keeps zones on native IPv6 node keys while removing zones
                // from all queried peers. Unmap discards a mapped IPv4 key's zone.
                if let Some((ip, zone)) = node_ip.split_once('%')
                    && !zone.is_empty()
                    && let Ok(ip) = ip.parse::<Ipv6Addr>()
                {
                    local_ips.extend(ip.to_ipv4_mapped().map(IpAddr::V4));
                } else {
                    return Err(TurnStartError::Start(format!(
                        "could not get local IPs for TURN relay policy: {node_ip}: {error}"
                    )));
                }
            }
        }
    }
    Ok(LocalRelayPolicy::new(local_ips, media_ports(rtc)))
}

pub async fn start(
    cfg: &ServerConfig,
    rtc: &RtcConfig,
    auth: Arc<dyn rtc_turn::AuthHandler>,
) -> Result<Option<TurnHandle>, TurnStartError> {
    let t = &cfg.turn;
    if !t.enabled {
        return Ok(None);
    }
    let tls = build_tls_acceptor(t)?;
    let binds: Vec<IpAddr> = if t.bind_addresses.is_empty() {
        vec![IpAddr::V4(Ipv4Addr::UNSPECIFIED)]
    } else {
        t.bind_addresses.clone()
    };
    let mut listeners = Vec::new();
    for ip in &binds {
        if let Some(port) = t.udp_port {
            listeners.push(ListenerSpec::Udp(SocketAddr::new(*ip, port)));
        }
        // external_tls: plain TCP behind a TLS terminator, still advertised as turns:.
        if let Some(port) = t.tls_port {
            listeners.push(ListenerSpec::Tcp(SocketAddr::new(*ip, port)));
        }
    }
    let config = TurnServerConfig {
        realm: "livekit".to_string(),
        listeners,
        tls,
        relay_ipv4: cfg.rtc.node_ip.v4,
        relay_ipv6: cfg.rtc.node_ip.v6,
        relay_bind_ipv4: IpAddr::V4(Ipv4Addr::UNSPECIFIED),
        relay_bind_ipv6: cfg
            .rtc
            .node_ip
            .v6
            .map(|_| IpAddr::V6(std::net::Ipv6Addr::UNSPECIFIED)),
        relay_port_range: t.relay_range,
        per_user_allocation_limit: t.per_user_allocation_limit,
        allow_restricted_peer_cidrs: t.allow_restricted_peer_cidrs.clone(),
        deny_peer_cidrs: t.deny_peer_cidrs.clone(),
        // PROXY protocol only fronts the TCP (TLS-port) listener, matching
        // upstream `newTURNTCPListener`; trusted CIDRs were validated at config
        // time and are non-empty whenever the gate holds.
        proxy_trusted_cidrs: if t.proxy_protocol && t.tls_port.is_some() {
            Some(t.proxy_trusted_cidrs.clone())
        } else {
            None
        },
        max_allocations: 10_000,
        default_lifetime: Duration::from_secs(600),
        max_lifetime: Duration::from_secs(3600),
    };
    tracing::info!(
        udp_port = ?t.udp_port,
        tls_port = ?t.tls_port,
        proxy_protocol = t.proxy_protocol,
        relay_range_start = t.relay_range.0,
        relay_range_end = t.relay_range.1,
        per_user_relay_allocation_limit = t.per_user_allocation_limit,
        "Starting TURN server"
    );
    TurnServer::start_embedded(config, auth, local_relay_policy(rtc)?)
        .await
        .map(Some)
        .map_err(|e| TurnStartError::Start(e.to_string()))
}

struct TurnTls(rtc_tls::tokio_rustls::TlsAcceptor);
impl rtc_turn::StreamAcceptor for TurnTls {
    fn accept(&self, stream: tokio::net::TcpStream) -> rtc_turn::AcceptFuture {
        let acceptor = self.0.clone();
        Box::pin(async move {
            let stream = acceptor.accept(stream).await?;
            Ok(Box::new(stream) as Box<dyn rtc_turn::AsyncStream>)
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rtc_core::config::TurnConfig;
    use std::path::PathBuf;

    fn base_turn() -> TurnConfig {
        TurnConfig {
            enabled: true,
            domain: String::new(),
            cert_file: None,
            key_file: None,
            tls_port: None,
            udp_port: Some(3478),
            relay_range: (30000, 40000),
            external_tls: false,
            bind_addresses: vec![],
            per_user_allocation_limit: 12,
            ttl: Duration::from_secs(300),
            allow_restricted_peer_cidrs: vec![],
            deny_peer_cidrs: vec![],
            proxy_protocol: false,
            proxy_trusted_cidrs: vec![],
        }
    }

    #[test]
    fn embedded_local_relay_ports_follow_official_rtc_mode_precedence() {
        let mut rtc = RtcConfig::default();
        rtc.node_ip.v4 = "203.0.113.32".into();
        rtc.node_ip.v6 = "::ffff:203.0.113.33%test-zone".into();
        let cases = [
            (false, (0, 0), (0, 0), true, vec![]),
            (
                false,
                (50000, 50002),
                (7882, 7884),
                true,
                vec![(50000, 50002)],
            ),
            (false, (50000, 0), (7882, 7884), false, vec![(7882, 7884)]),
            (
                false,
                (0, 0),
                (7882, 0),
                true,
                vec![(7882, 7882), (3478, 3478)],
            ),
            (true, (0, 0), (7882, 7884), true, vec![]),
        ];
        for (force_tcp, range, udp, stun, expected) in cases {
            rtc.force_tcp = force_tcp;
            (rtc.port_range_start, rtc.port_range_end) = range;
            (rtc.udp_port.start, rtc.udp_port.end) = udp;
            rtc.use_stun_port_as_ice = stun;
            assert_eq!(media_ports(&rtc), expected);
            let policy = local_relay_policy(&rtc).unwrap();
            for port in [3478, 7881, 7882, 7884, 7885, 49999, 50000, 50002, 50003] {
                let eligible = expected
                    .iter()
                    .any(|&(start, end)| (start..=end).contains(&port));
                for local in [
                    "127.0.0.1",
                    "203.0.113.32",
                    "203.0.113.33",
                    "::ffff:203.0.113.32",
                ] {
                    assert_eq!(
                        policy.allows(SocketAddr::new(local.parse().unwrap(), port)),
                        eligible,
                        "{local}:{port} with {expected:?}"
                    );
                }
            }
            assert!(policy.allows("198.51.100.1:8080".parse().unwrap()));
        }
    }

    #[test]
    fn no_tls_port_skips_tls_configuration_entirely() {
        let t = base_turn();
        assert!(build_tls_acceptor(&t).unwrap().is_none());
    }

    #[test]
    fn external_tls_skips_local_certificate_validation() {
        let mut t = base_turn();
        t.tls_port = Some(5349);
        t.external_tls = true;
        // external_tls means a load balancer terminates TLS; no local cert needed.
        assert!(build_tls_acceptor(&t).unwrap().is_none());
    }

    #[test]
    fn tls_port_without_cert_file_rejects_with_explicit_error() {
        let mut t = base_turn();
        t.tls_port = Some(5349);
        let result = build_tls_acceptor(&t);
        assert!(result.is_err());
        let err = result.err().unwrap();
        assert!(
            err.to_string().contains("turn.cert_file is required"),
            "{err}"
        );
    }

    #[test]
    fn tls_port_with_cert_but_no_key_file_rejects() {
        let mut t = base_turn();
        t.tls_port = Some(5349);
        t.cert_file = Some(PathBuf::from("server.pem"));
        let result = build_tls_acceptor(&t);
        assert!(result.is_err());
        let err = result.err().unwrap();
        assert!(
            err.to_string().contains("turn.key_file is required"),
            "{err}"
        );
    }

    #[test]
    fn tls_port_with_nonexistent_cert_file_rejects_with_io_or_parse_error() {
        let mut t = base_turn();
        t.tls_port = Some(5349);
        t.cert_file = Some(PathBuf::from("/nonexistent/cert.pem"));
        t.key_file = Some(PathBuf::from("/nonexistent/key.pem"));
        let result = build_tls_acceptor(&t);
        assert!(result.is_err());
        let err = result.err().unwrap();
        assert!(
            err.to_string().contains("could not configure TURN/TLS"),
            "{err}"
        );
    }

    #[test]
    fn tls_with_valid_test_certificates_succeeds() {
        let fixtures =
            std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../rtc-tls/tests/fixtures");
        let mut t = base_turn();
        t.tls_port = Some(5349);
        t.cert_file = Some(fixtures.join("server.pem"));
        t.key_file = Some(fixtures.join("server-key.pem"));
        let acceptor = build_tls_acceptor(&t);
        assert!(acceptor.is_ok(), "expected Ok, got Err");
        assert!(acceptor.unwrap().is_some());
    }
}
