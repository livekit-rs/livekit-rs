//! Resolve raw official listener hosts at the socket-owning entry points.
use std::net::{IpAddr, Ipv6Addr, SocketAddr, SocketAddrV6};

pub(crate) async fn resolve(host: &str, port: u16) -> Result<Option<SocketAddr>, String> {
    if host.is_empty() {
        return Ok(None);
    }
    if let Ok(ip) = host.parse::<IpAddr>() {
        return Ok(Some(SocketAddr::new(ip.to_canonical(), port)));
    }
    if let Some((ip, zone)) = host.rsplit_once('%')
        && let Ok(ip) = ip.parse::<Ipv6Addr>()
    {
        let index = getifs::interfaces()
            .ok()
            .and_then(|interfaces| {
                interfaces
                    .into_iter()
                    .find(|interface| interface.name() == zone)
                    .map(|interface| interface.index())
            })
            .or_else(|| zone.parse().ok())
            .unwrap_or(0);
        return Ok(Some(SocketAddrV6::new(ip, port, 0, index).into()));
    }

    // SplitHostPort sees the extra brackets on already bracketed input.
    let bracket_error = match host.find(']') {
        Some(end) if host.contains(':') && host[end + 1..].starts_with(':') => {
            Some("too many colons in address")
        }
        Some(_) if host.contains(':') => Some("missing port in address"),
        _ if host.starts_with('[') && !host.contains(':') => Some("missing ']' in address"),
        _ if host.contains('[') => Some("unexpected '[' in address"),
        _ if host.contains(']') => Some("unexpected ']' in address"),
        _ => None,
    };
    if let Some(reason) = bracket_error {
        let address = if host.contains(':') {
            format!("[{host}]:{port}")
        } else {
            format!("{host}:{port}")
        };
        return Err(format!("address {address}: {reason}"));
    }

    let mut addresses = tokio::net::lookup_host((host, port))
        .await
        .map_err(|error| {
            let system = error.to_string();
            let reason = if error.kind() == std::io::ErrorKind::NotFound
                || system.contains("nodename nor servname provided, or not known")
                || system.contains("Name or service not known")
                || system.contains("No address associated with hostname")
            {
                "no such host"
            } else {
                &system
            };
            format!("lookup {host}: {reason}")
        })?;
    // Go Listen chooses first(isIPv4); ResolveUDPAddr uses the same
    // preference for a hostname. Do not retry the next address on bind failure.
    let first = addresses
        .next()
        .ok_or_else(|| format!("lookup {host}: no such host"))?;
    let selected = if first.is_ipv4() {
        first
    } else {
        addresses.find(SocketAddr::is_ipv4).unwrap_or(first)
    };
    Ok(Some(selected))
}
