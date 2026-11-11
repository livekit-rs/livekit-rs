// Copyright 2009 The Go Authors. All rights reserved.
// Rust adaptation2026. BSD-3-Clause source attribution retained below.
//! Fixed Go net.SplitHostPort/parsePort and go-redis Addr selection.
//! Go1.26.3 net/{ipsock,port,lookup,cgo_unix,port_unix}.go, BSD-3-Clause;
//! existing LICENSES/Go-BSD-3-Clause.txt. No URL or credential interpretation.

pub(super) struct Address {
    pub connection: redis::ConnectionAddr,
    pub network: &'static str,
    pub endpoint: String,
}

pub(super) async fn parse(raw: &str) -> Result<Address, String> {
    if raw.starts_with('/') {
        return Ok(Address {
            connection: redis::ConnectionAddr::Unix(raw.into()),
            network: "unix",
            endpoint: raw.into(),
        });
    }
    let (host, service) =
        split_host_port(raw).map_err(|why| format!("dial tcp: address {raw}: {why}"))?;
    let port = match numeric_port(service) {
        Some(Ok(port)) => port,
        Some(Err(())) => return Err(format!("dial tcp: address {service}: invalid port")),
        None => {
            let owned = service.to_owned();
            tokio::task::spawn_blocking(move || service_port(&owned))
                .await
                .map_err(|error| format!("could not resolve Redis service port: {error}"))?
                .map_err(|why| format!("dial tcp: lookup tcp/{service}: {why}"))?
        }
    };
    let (literal, zone) = host.rsplit_once('%').unwrap_or((host, ""));
    let endpoint_host = match literal.parse::<std::net::IpAddr>() {
        Ok(ip) => {
            let mut value = match ip {
                std::net::IpAddr::V6(ip) => ip
                    .to_ipv4_mapped()
                    .map(|ip| ip.to_string())
                    .unwrap_or_else(|| ip.to_string()),
                ip => ip.to_string(),
            };
            if !zone.is_empty() {
                value.push('%');
                value.push_str(zone);
            }
            value
        }
        Err(_) => host.to_owned(),
    };
    let endpoint = if endpoint_host.contains(':') {
        format!("[{endpoint_host}]:{port}")
    } else {
        format!("{endpoint_host}:{port}")
    };
    // A Go TCPAddr with an empty host is an unspecified IP, not a DNS name.
    // Connect the actual OS wildcard endpoint; do not URI-reject empty/wildcard.
    let host = if host.is_empty() { "0.0.0.0" } else { host };
    Ok(Address {
        connection: redis::ConnectionAddr::Tcp(host.into(), port),
        network: "tcp",
        endpoint,
    })
}

// Literal source algorithm: bracket errors and last-colon order matter.
fn split_host_port(raw: &str) -> Result<(&str, &str), &'static str> {
    let Some(i) = raw.rfind(':') else {
        return Err("missing port in address");
    };
    let (host, j, k) = if raw.starts_with('[') {
        let Some(end) = raw.find(']') else {
            return Err("missing ']' in address");
        };
        if end + 1 == raw.len() {
            return Err("missing port in address");
        }
        if end + 1 != i {
            return Err(if raw.as_bytes()[end + 1] == b':' {
                "too many colons in address"
            } else {
                "missing port in address"
            });
        }
        (&raw[1..end], 1, end + 1)
    } else {
        let host = &raw[..i];
        if host.contains(':') {
            return Err("too many colons in address");
        }
        (host, 0, 0)
    };
    if raw[j..].contains('[') {
        return Err("unexpected '[' in address");
    }
    if raw[k..].contains(']') {
        return Err("unexpected ']' in address");
    }
    Ok((host, &raw[i + 1..]))
}

// Go permits an empty port and bare signs as zero; decimal only, with the source
// uint32 wrapping multiplication and addition-overflow check before range validation. A nondigit requests service lookup.
fn numeric_port(service: &str) -> Option<Result<u16, ()>> {
    let (negative, digits) = if let Some(rest) = service.strip_prefix('-') {
        (true, rest)
    } else {
        (false, service.strip_prefix('+').unwrap_or(service))
    };
    let mut number = 0u32;
    for digit in digits.bytes() {
        if !digit.is_ascii_digit() {
            return None;
        }
        if number >= 1 << 30 {
            number = u32::MAX;
            break;
        }
        number = number.wrapping_mul(10);
        let next = number.wrapping_add(u32::from(digit - b'0'));
        if next < number {
            number = u32::MAX;
            break;
        }
        number = next;
    }
    if number > u16::MAX as u32 || (negative && number != 0) {
        Some(Err(()))
    } else {
        Some(Ok(number as u16))
    }
}

fn fallback_port(service: &str) -> Option<u16> {
    let service = service.to_ascii_lowercase();
    let mut port = match service.as_str() {
        "ftp" => Some(21),
        "ftps" => Some(990),
        "gopher" => Some(70),
        "http" => Some(80),
        "https" => Some(443),
        "imap2" => Some(143),
        "imap3" => Some(220),
        "imaps" => Some(993),
        "pop3" => Some(110),
        "pop3s" => Some(995),
        "smtp" => Some(25),
        "submissions" => Some(465),
        "ssh" => Some(22),
        "telnet" => Some(23),
        _ => None,
    };
    // Match fixed Go's native fallback: later /etc/services rows overwrite
    // builtins and earlier aliases. Fields are ASCII SP/TAB/CR/LF separators.
    if let Ok(bytes) = std::fs::read("/etc/services") {
        for line in bytes.split(|b| *b == b'\n') {
            let line = line.split(|b| *b == b'#').next().unwrap_or_default();
            let fields: Vec<_> = line
                .split(|b| matches!(*b, b' ' | b'\t' | b'\r' | b'\n'))
                .filter(|field| !field.is_empty())
                .collect();
            if fields.len() < 2 {
                continue;
            }
            let Some(slash) = fields[1].iter().position(|b| *b == b'/') else {
                continue;
            };
            if &fields[1][slash + 1..] != b"tcp" {
                continue;
            }
            let Ok(text) = std::str::from_utf8(&fields[1][..slash]) else {
                continue;
            };
            let Ok(value) = text.parse::<u16>() else {
                continue;
            };
            if value == 0 {
                continue;
            }
            if fields
                .iter()
                .enumerate()
                .any(|(index, field)| index != 1 && *field == service.as_bytes())
            {
                port = Some(value);
            }
        }
    }
    port
}

fn service_port(service: &str) -> Result<u16, String> {
    // Existing libc is the OS service database boundary used by Go cgo, not a
    // new runtime/library. Go tries its native table only when cgo fails.
    #[cfg(unix)]
    {
        use std::ffi::{CStr, CString};
        let service = CString::new(service.to_ascii_lowercase())
            .map_err(|_| "invalid argument".to_owned())?;
        // SAFETY: zero is a valid addrinfo hints initialization; no owned pointers.
        let mut hints: libc::addrinfo = unsafe { std::mem::zeroed() };
        hints.ai_socktype = libc::SOCK_STREAM;
        hints.ai_protocol = libc::IPPROTO_TCP;
        let mut result = std::ptr::null_mut();
        // SAFETY: live NUL-terminated service and hints, writable result pointer;
        // getaddrinfo owns its result until exactly one freeaddrinfo below.
        let code =
            unsafe { libc::getaddrinfo(std::ptr::null(), service.as_ptr(), &hints, &mut result) };
        let resolver_errno = std::io::Error::last_os_error();
        let mut port = None;
        if code == 0 {
            let mut item = result;
            while !item.is_null() {
                // SAFETY: each node and sockaddr belong to the live result chain.
                let entry = unsafe { &*item };
                port = match entry.ai_family {
                    libc::AF_INET => Some(u16::from_be(unsafe {
                        (*(entry.ai_addr as *const libc::sockaddr_in)).sin_port
                    })),
                    libc::AF_INET6 => Some(u16::from_be(unsafe {
                        (*(entry.ai_addr as *const libc::sockaddr_in6)).sin6_port
                    })),
                    _ => None,
                };
                if port.is_some() {
                    break;
                }
                item = entry.ai_next;
            }
            // SAFETY: successful getaddrinfo result is freed exactly once.
            unsafe { libc::freeaddrinfo(result) };
        }
        if let Some(port) = port {
            return Ok(port);
        }
        let service = service
            .to_str()
            .expect("ASCII-lowercase preserves source UTF8");
        if let Some(port) = fallback_port(service) {
            return Ok(port);
        }
        if code == libc::EAI_SERVICE || code == libc::EAI_NONAME || code == 0 {
            return Err("unknown port".into());
        }
        // Preserve genuine resolver failure; do not substitute an invented dial.
        if code == libc::EAI_SYSTEM {
            return Err(resolver_errno.to_string());
        }
        // SAFETY: gai_strerror returns a process-lifetime NUL-terminated string.
        Err(unsafe { CStr::from_ptr(libc::gai_strerror(code)) }
            .to_string_lossy()
            .into_owned())
    }
    #[cfg(not(unix))]
    fallback_port(service).ok_or_else(|| "unknown port".into())
}

#[cfg(test)]
mod source_port_controls {
    // Byte-for-byte canonical ID/address pairs from immutable oracle/ports/cases.json.
    // This only validates the input domain, never supplies expected results.
    const CASES: &[(&str, &str)] = &[
        ("missing-port", "127.0.0.1"),
        ("missing-close-bracket", "[::1:6379"),
        ("unbracketed-ipv6", "::1:6379"),
        ("unexpected-open-bracket", "x[:6379"),
        ("empty-port-zero", "127.0.0.1:"),
        ("bare-plus-zero", "127.0.0.1:+"),
        ("bare-minus-zero", "127.0.0.1:-"),
        ("signed-leading-zero", "127.0.0.1:+00080"),
        ("invalid-negative", "127.0.0.1:-1"),
        ("invalid-range", "127.0.0.1:65536"),
        ("source-uint32-multiplication", "127.0.0.1:4294967300"),
        ("source-addition-overflow", "127.0.0.1:4294967296"),
        ("empty-host", ":80"),
        ("ipv6-literal", "[0:0:0:0:0:0:0:1]:6379"),
        ("ipv4-mapped", "[::ffff:127.0.0.1]:6379"),
        ("ipv6-zone", "[0:0:0:0:0:0:0:1%lo0]:6379"),
        ("named-service-case", "127.0.0.1:HTTP"),
        ("named-service-unknown", "127.0.0.1:owned_unknown_service"),
    ];
    fn checked_rows(data: &[u8]) -> Vec<serde_json::Value> {
        let rows: Vec<serde_json::Value> = data
            .split(|b| *b == b'\n')
            .filter(|line| !line.is_empty())
            .map(|line| serde_json::from_slice(line).expect("actual Go18 JSONL row"))
            .collect();
        assert_eq!(rows.len(), CASES.len(), "complete actual Go18 inventory");
        for (row, (id, address)) in rows.iter().zip(CASES) {
            assert_eq!(
                row["id"].as_str(),
                Some(*id),
                "ordered unique source case ID"
            );
            assert_eq!(
                row["address"].as_str(),
                Some(*address),
                "original source input address for {id}"
            );
        }
        rows
    }
    #[test]
    fn actual_source_inventory_rejects_duplicate_reordered_truncated_or_changed_addresses() {
        // Input inventory validation only, not fabricated source result rows.
        let rows: Vec<_> = CASES
            .iter()
            .map(|(id, address)| serde_json::json!({"id":id,"address":address}))
            .collect();
        let bytes = |rows: &[serde_json::Value]| -> Vec<u8> {
            rows.iter()
                .map(|row| format!("{row}\n"))
                .collect::<String>()
                .into_bytes()
        };
        assert_eq!(checked_rows(&bytes(&rows)).len(), CASES.len());
        let mut duplicate = rows.clone();
        duplicate[1] = duplicate[0].clone();
        assert!(std::panic::catch_unwind(|| checked_rows(&bytes(&duplicate))).is_err());
        let mut reordered = rows.clone();
        reordered.swap(0, 1);
        assert!(std::panic::catch_unwind(|| checked_rows(&bytes(&reordered))).is_err());
        let mut changed = rows.clone();
        changed[0]["address"] = serde_json::json!("127.0.0.1:6379");
        assert!(std::panic::catch_unwind(|| checked_rows(&bytes(&changed))).is_err());
        assert!(
            std::panic::catch_unwind(|| checked_rows(&bytes(&rows[..rows.len() - 1]))).is_err()
        );
    }
    #[tokio::test]
    #[ignore = "requires independently executed fixed Go18 ResolveTCPAddr rows"]
    async fn actual_go_resolve_tcp_addr_controls() {
        let path = std::env::var_os("LIVEKIT_REDIS_PORT_GO_ACTUAL").expect("actual Go18 output");
        let data = std::fs::read(path).unwrap();
        for row in checked_rows(&data) {
            let actual = super::parse(row["address"].as_str().unwrap()).await;
            assert_eq!(
                actual.is_ok(),
                row["accepted"].as_bool().unwrap(),
                "{}",
                row["id"]
            );
            match actual {
                Ok(address) => assert_eq!(
                    address.endpoint,
                    row["endpoint"].as_str().unwrap(),
                    "{}",
                    row["id"]
                ),
                Err(error) => assert_eq!(error, row["error"].as_str().unwrap(), "{}", row["id"]),
            }
        }
    }
}
