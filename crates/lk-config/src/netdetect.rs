//! Node-IP discovery (`MTU/pkg/rtcconfig/ip.go`): local primary address, or the
//! public address via a STUN binding request when `rtc.use_external_ip` is set.
//!
//! Local discovery enumerates interfaces in system order exactly like upstream
//! `GetLocalIPAddresses(false, true, nil, nil)` and prefers the first IPv4.
//!
//! Unbound STUN uses resolver order unless IPv4 is required. Per-interface
//! requests are restricted to the family of their selected local socket.
//! External-IP validation is skipped when no local address is given, as in
//! upstream `validateExternalIP`.

use std::net::{IpAddr, SocketAddr, ToSocketAddrs, UdpSocket};
use std::time::{Duration, Instant};

/// Default STUN servers (`rtcconfig/config.go:38-42`).
pub const DEFAULT_STUN_SERVERS: [&str; 3] = [
    "global.stun.twilio.com:3478",
    "stun.l.google.com:19302",
    "stun1.l.google.com:19302",
];

const STUN_PING_TIMEOUT: Duration = Duration::from_secs(5);
// Defaults of the pinned Pion STUN client. Each retry waits one more RTO.
const STUN_RTO: Duration = Duration::from_millis(300);
const STUN_MAX_RETRIES: u32 = 7;
const ATTEMPTS: usize = 3;
const RETRY_SLEEP: Duration = Duration::from_millis(500);

/// Network access used by the loader; replaceable in tests.
pub trait NetProbe {
    /// Local addresses in upstream `GetLocalIPAddresses` order (non-loopback
    /// first; loopback only if nothing else).
    fn local_addresses(&self) -> Vec<IpAddr>;
    /// One `GetExternalIP` pass over `servers` (first success wins).
    fn external_ip(&self, servers: &[String], ipv4_only: bool) -> Result<IpAddr, String>;
    /// SPEC-0074 C22: `getExternalIP` bound to one specific source address
    /// (`webrtc_config.go:330` per-interface STUN in `getNAT1to1IPsForConf`).
    /// The default rejects, so probes that never advertise an external IP need
    /// not implement it; only the discovery path that has network access does.
    fn external_ip_from(
        &self,
        _local: IpAddr,
        _port: u16,
        _servers: &[String],
        _validate: bool,
    ) -> Result<IpAddr, String> {
        Err("source-bound STUN is not supported by this probe".to_string())
    }
    /// Real per-interface discovery shares the fixed Go cancellation budget.
    fn external_ip_from_context(
        &self,
        local: IpAddr,
        port: u16,
        servers: &[String],
        validate: bool,
        _context: &ProbeContext,
    ) -> Result<IpAddr, String> {
        self.external_ip_from(local, port, servers, validate)
    }
    fn sleep(&self, d: Duration);
}

/// Shared cancellation/deadline of one NAT mapping discovery pass. The first
/// successful mapping shortens the original five-second window to one second,
/// exactly as getNAT1to1IPsForConf; socket polls also observe cancellation.
#[derive(Clone)]
pub struct ProbeContext {
    deadline: std::sync::Arc<std::sync::Mutex<Instant>>,
    canceled: std::sync::Arc<std::sync::atomic::AtomicBool>,
}
impl ProbeContext {
    pub(crate) fn new(timeout: Duration) -> Self {
        Self {
            deadline: std::sync::Arc::new(std::sync::Mutex::new(Instant::now() + timeout)),
            canceled: std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false)),
        }
    }
    pub(crate) fn settle_after(&self, timeout: Duration) {
        let mut deadline = self.deadline.lock().unwrap_or_else(|e| e.into_inner());
        *deadline = Instant::now() + timeout;
    }
    pub(crate) fn cancel(&self) {
        self.canceled
            .store(true, std::sync::atomic::Ordering::Release);
    }
    pub(crate) fn remaining(&self, individual: Instant) -> Option<Duration> {
        if self.canceled.load(std::sync::atomic::Ordering::Acquire) {
            return None;
        }
        let deadline = (*self.deadline.lock().unwrap_or_else(|e| e.into_inner())).min(individual);
        deadline
            .checked_duration_since(Instant::now())
            .filter(|d| !d.is_zero())
    }
    pub(crate) fn deadline(&self) -> Instant {
        *self.deadline.lock().unwrap_or_else(|e| e.into_inner())
    }
}

/// Real network implementation.
pub struct SystemProbe;

/// Go `IP.IsLinkLocalMulticast`: 224.0.0.0/24 and ff02::/16.
fn is_link_local_multicast(ip: &IpAddr) -> bool {
    match ip {
        IpAddr::V4(v4) => v4.octets()[..3] == [224, 0, 0],
        IpAddr::V6(v6) => v6.segments()[0] & 0xff0f == 0xff02,
    }
}

fn is_link_local(ip: &IpAddr) -> bool {
    match ip {
        IpAddr::V4(v4) => v4.is_link_local(),
        IpAddr::V6(v6) => (v6.segments()[0] & 0xffc0) == 0xfe80,
    }
}

impl NetProbe for SystemProbe {
    /// mediatransportutil `GetLocalIPAddresses(false, true, nil, nil)`
    /// (rtcconfig/ip.go): every interface in system order (administrative
    /// state is not checked), IPv4 and IPv6, link-local unicast/multicast
    /// skipped; non-loopback addresses win, loopback only as the fallback.
    fn local_addresses(&self) -> Vec<IpAddr> {
        let Ok(interfaces) = getifs::interfaces() else {
            return Vec::new();
        };
        let mut addresses = Vec::new();
        let mut loopbacks = Vec::new();
        for interface in interfaces {
            let Ok(addrs) = interface.addrs() else {
                continue;
            };
            for addr in addrs {
                let ip = match addr.addr() {
                    IpAddr::V6(v6) => v6
                        .to_ipv4_mapped()
                        .map(IpAddr::V4)
                        .unwrap_or(IpAddr::V6(v6)),
                    ip => ip,
                };
                if is_link_local(&ip) || is_link_local_multicast(&ip) {
                    continue;
                }
                if ip.is_loopback() {
                    loopbacks.push(ip);
                } else {
                    addresses.push(ip);
                }
            }
        }
        if addresses.is_empty() {
            loopbacks
        } else {
            addresses
        }
    }

    fn external_ip(&self, servers: &[String], ipv4_only: bool) -> Result<IpAddr, String> {
        if servers.is_empty() {
            return Err("STUN servers are required but not defined".to_string());
        }
        let mut last = String::new();
        for s in servers {
            match stun_binding(s, ipv4_only, STUN_PING_TIMEOUT) {
                Ok(ip) => return Ok(ip),
                Err(e) => last = e,
            }
        }
        Err(last)
    }

    fn sleep(&self, d: Duration) {
        std::thread::sleep(d);
    }

    /// Per-interface STUN with optional public-IP hairpin validation.
    fn external_ip_from(
        &self,
        local: IpAddr,
        port: u16,
        servers: &[String],
        validate: bool,
    ) -> Result<IpAddr, String> {
        self.external_ip_from_context(
            local,
            port,
            servers,
            validate,
            &ProbeContext::new(STUN_PING_TIMEOUT.saturating_mul((servers.len().max(1) * 2) as u32)),
        )
    }
    fn external_ip_from_context(
        &self,
        local: IpAddr,
        port: u16,
        servers: &[String],
        validate: bool,
        context: &ProbeContext,
    ) -> Result<IpAddr, String> {
        if servers.is_empty() {
            return Err("STUN servers are required but not defined".into());
        }
        let mut last = String::new();
        for server in servers {
            let result = stun_binding_from_context(
                server,
                SocketAddr::new(local, port),
                STUN_PING_TIMEOUT,
                context,
            )
            .and_then(|external| {
                if validate {
                    validate_external_ip(
                        external,
                        SocketAddr::new(local, port),
                        STUN_PING_TIMEOUT,
                        context,
                    )?;
                }
                Ok(external)
            });
            match result {
                Ok(ip) => return Ok(ip),
                Err(error) => last = error,
            }
        }
        Err(last)
    }
}

/// `determineIP` (`ip.go:35-77`). Returns the address text (`net.IP.String()`).
pub(crate) fn determine_ip(
    probe: &dyn NetProbe,
    use_external_ip: bool,
    require_ipv4: bool,
    stun_servers: &[String],
) -> Result<String, String> {
    if use_external_ip {
        let servers: Vec<String> = if stun_servers.is_empty() {
            DEFAULT_STUN_SERVERS.iter().map(|s| s.to_string()).collect()
        } else {
            stun_servers.to_vec()
        };
        let mut err = String::new();
        for _ in 0..ATTEMPTS {
            match probe.external_ip(&servers, require_ipv4) {
                Ok(ip) => return Ok(ip.to_string()),
                Err(e) => {
                    err = e;
                    probe.sleep(RETRY_SLEEP);
                }
            }
        }
        return Err(format!("could not resolve external IP: {err}"));
    }

    let addresses = probe.local_addresses();
    if let Some(v4) = addresses.iter().find(|a| a.is_ipv4()) {
        return Ok(v4.to_string());
    }
    if require_ipv4 {
        return Err("no IPv4 address available".to_string());
    }
    match addresses.first() {
        Some(a) => Ok(a.to_string()),
        None => Err("could not find local IP address".to_string()),
    }
}

// ---------------------------------------------------------------------------
// Minimal STUN (RFC 5389) binding client
// ---------------------------------------------------------------------------

const MAGIC_COOKIE: u32 = 0x2112_A442;
const BINDING_REQUEST: u16 = 0x0001;
const BINDING_SUCCESS: u16 = 0x0101;
const BINDING_ERROR: u16 = 0x0111;
const ATTR_XOR_MAPPED_ADDRESS: u16 = 0x0020;

fn build_binding_request(tid: &[u8; 12]) -> [u8; 20] {
    let mut m = [0u8; 20];
    m[0..2].copy_from_slice(&BINDING_REQUEST.to_be_bytes());
    // length 0
    m[4..8].copy_from_slice(&MAGIC_COOKIE.to_be_bytes());
    m[8..20].copy_from_slice(tid);
    m
}

/// Outcome of parsing one datagram.
#[derive(Debug, PartialEq, Eq)]
enum StunReply {
    /// Not a response to our transaction; keep waiting.
    Ignore,
    Mapped(IpAddr),
    Error(String),
}

fn parse_binding_response(buf: &[u8], tid: &[u8; 12]) -> StunReply {
    if buf.len() < 20 {
        return StunReply::Ignore;
    }
    let msg_type = u16::from_be_bytes([buf[0], buf[1]]);
    let msg_len = u16::from_be_bytes([buf[2], buf[3]]) as usize;
    let cookie = u32::from_be_bytes([buf[4], buf[5], buf[6], buf[7]]);
    if cookie != MAGIC_COOKIE || &buf[8..20] != tid {
        return StunReply::Ignore;
    }
    let Some(mut attributes) = buf.get(20..20 + msg_len) else {
        return StunReply::Ignore;
    };
    // Pion decodes the entire frame before delivering it to the transaction.
    let mut mapped = None;
    while !attributes.is_empty() {
        let [type_hi, type_lo, len_hi, len_lo, rest @ ..] = attributes else {
            return StunReply::Ignore;
        };
        let attr_type = u16::from_be_bytes([*type_hi, *type_lo]);
        let len = usize::from(u16::from_be_bytes([*len_hi, *len_lo]));
        let Some((padded, remaining)) = rest.split_at_checked(len.div_ceil(4) * 4) else {
            return StunReply::Ignore;
        };
        if attr_type == ATTR_XOR_MAPPED_ADDRESS && len >= 8 && mapped.is_none() {
            mapped = Some(&padded[..len]);
        }
        attributes = remaining;
    }
    if msg_type == BINDING_ERROR {
        return StunReply::Error("STUN binding error response".to_string());
    }
    if msg_type != BINDING_SUCCESS {
        return StunReply::Ignore;
    }
    let Some(v) = mapped else {
        return StunReply::Error("attribute not found".to_string());
    };
    let cookie_b = MAGIC_COOKIE.to_be_bytes();
    match v[1] {
        0x01 => {
            let mut ip = [0u8; 4];
            for i in 0..4 {
                ip[i] = v[4 + i] ^ cookie_b[i];
            }
            StunReply::Mapped(IpAddr::from(ip))
        }
        0x02 if v.len() >= 20 => {
            let mut key = [0u8; 16];
            key[..4].copy_from_slice(&cookie_b);
            key[4..].copy_from_slice(tid);
            let mut ip = [0u8; 16];
            for i in 0..16 {
                ip[i] = v[4 + i] ^ key[i];
            }
            StunReply::Mapped(IpAddr::from(ip))
        }
        _ => StunReply::Error("invalid XOR-MAPPED-ADDRESS family".to_string()),
    }
}

#[derive(Clone, Copy)]
enum StunNetwork {
    Any,
    V4,
    V6,
}

impl StunNetwork {
    fn name(self) -> &'static str {
        match self {
            Self::Any => "udp",
            Self::V4 => "udp4",
            Self::V6 => "udp6",
        }
    }

    fn accepts(self, address: SocketAddr) -> bool {
        match self {
            Self::Any => true,
            Self::V4 => address.is_ipv4(),
            Self::V6 => address.is_ipv6(),
        }
    }
}

fn resolve(server: &str, network: StunNetwork) -> Result<SocketAddr, String> {
    let name = network.name();
    let mut addresses = server
        .to_socket_addrs()
        .map_err(|error| format!("dial {name}: lookup {server}: {error}"))?;
    let selected = addresses
        .by_ref()
        .map(|address| match address {
            SocketAddr::V6(v6) => v6
                .ip()
                .to_ipv4_mapped()
                .map(|ip| SocketAddr::new(ip.into(), v6.port()))
                .unwrap_or(address),
            SocketAddr::V4(_) => address,
        })
        .find(|address| network.accepts(*address));
    selected.ok_or_else(|| {
        if let Ok(literal) = server.parse::<SocketAddr>() {
            format!(
                "dial {name}: address {}: no suitable address found",
                literal.ip()
            )
        } else {
            format!("dial {name}: lookup {server}: no suitable address found")
        }
    })
}

/// `findExternalIP` without validation: one binding transaction, 5 s timeout.
fn stun_binding(server: &str, ipv4_only: bool, timeout: Duration) -> Result<IpAddr, String> {
    let network = if ipv4_only {
        StunNetwork::V4
    } else {
        StunNetwork::Any
    };
    let address = resolve(server, network)?;
    let wildcard = if address.is_ipv4() {
        IpAddr::V4(std::net::Ipv4Addr::UNSPECIFIED)
    } else {
        IpAddr::V6(std::net::Ipv6Addr::UNSPECIFIED)
    };
    send_binding(
        address,
        SocketAddr::new(wildcard, 0),
        timeout,
        &ProbeContext::new(timeout),
    )
}

/// Resolve once using the source socket family; never replace the chosen
/// destination with a second DNS answer during the same STUN transaction.
fn stun_binding_from_context(
    server: &str,
    bind: SocketAddr,
    timeout: Duration,
    context: &ProbeContext,
) -> Result<IpAddr, String> {
    let network = if bind.is_ipv4() {
        StunNetwork::V4
    } else {
        StunNetwork::V6
    };
    send_binding(resolve(server, network)?, bind, timeout, context)
}

fn send_binding(
    addr: SocketAddr,
    bind: SocketAddr,
    timeout: Duration,
    context: &ProbeContext,
) -> Result<IpAddr, String> {
    let sock = UdpSocket::bind(bind).map_err(|e| format!("dial udp: {e}"))?;
    sock.connect(addr)
        .map_err(|e| format!("dial udp {addr}: {e}"))?;
    let mut tid = [0u8; 12];
    getrandom::fill(&mut tid).map_err(|e| e.to_string())?;
    let req = build_binding_request(&tid);
    sock.send(&req)
        .map_err(|e| format!("write udp {addr}: {e}"))?;

    let started = Instant::now();
    let deadline = started + timeout;
    let mut retry_at = started + STUN_RTO;
    let mut retries = 0;
    let mut buf = [0u8; 1500];
    loop {
        let remaining = context
            .remaining(deadline)
            .ok_or_else(|| "context deadline exceeded".to_string())?;
        let now = Instant::now();
        if retries < STUN_MAX_RETRIES && now >= retry_at {
            sock.send(&req)
                .map_err(|e| format!("write udp {addr}: {e}"))?;
            retries += 1;
            retry_at = now + STUN_RTO * (retries + 1);
        }
        let mut wait = remaining.min(Duration::from_millis(100));
        if retries < STUN_MAX_RETRIES {
            wait = wait.min(retry_at.duration_since(now));
        }
        sock.set_read_timeout(Some(wait))
            .map_err(|e| e.to_string())?;
        match sock.recv(&mut buf) {
            Ok(n) => match parse_binding_response(&buf[..n], &tid) {
                StunReply::Ignore => continue,
                StunReply::Mapped(ip) => return Ok(ip),
                StunReply::Error(e) => return Err(e),
            },
            Err(e)
                if e.kind() == std::io::ErrorKind::WouldBlock
                    || e.kind() == std::io::ErrorKind::TimedOut =>
            {
                continue;
            }
            Err(e) => return Err(format!("read udp {addr}: {e}")),
        }
    }
}

/// Fixed rtcconfig/ip.go validateExternalIP: close the STUN socket, rebind the
/// specified local address, send the original magic through a distinct socket
/// to the external IP at that newly-bound port, and require actual receipt.
/// Port zero deliberately rebinds a new ephemeral port, as Go does.
fn validate_external_ip(
    external: IpAddr,
    local: SocketAddr,
    timeout: Duration,
    context: &ProbeContext,
) -> Result<(), String> {
    const MAGIC: &[u8] = b"9#B8D2Nvg2xg5P$ZRwJ+f)*^Nne6*W3WamGY";
    let server = UdpSocket::bind(local).map_err(|e| format!("listen udp {local}: {e}"))?;
    let destination = SocketAddr::new(
        external,
        server.local_addr().map_err(|e| e.to_string())?.port(),
    );
    let wildcard = if external.is_ipv4() {
        "0.0.0.0:0"
    } else {
        "[::]:0"
    };
    let client = UdpSocket::bind(wildcard).map_err(|e| format!("dial udp {destination}: {e}"))?;
    client
        .connect(destination)
        .map_err(|e| format!("dial udp {destination}: {e}"))?;
    client
        .send(MAGIC)
        .map_err(|e| format!("write udp {destination}: {e}"))?;
    let deadline = Instant::now() + timeout;
    let mut bytes = [0; 1024];
    loop {
        let remaining = context
            .remaining(deadline)
            .ok_or_else(|| "context deadline exceeded".to_string())?;
        server
            .set_read_timeout(Some(remaining.min(Duration::from_millis(100))))
            .map_err(|e| e.to_string())?;
        match server.recv(&mut bytes) {
            Ok(size) if &bytes[..size] == MAGIC => return Ok(()),
            Ok(_) => {}
            Err(e)
                if matches!(
                    e.kind(),
                    std::io::ErrorKind::WouldBlock | std::io::ErrorKind::TimedOut
                ) => {}
            Err(e) => return Err(format!("read udp {local}: {e}")),
        }
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use std::cell::RefCell;

    /// Deterministic probe for loader tests.
    pub(crate) struct FakeProbe {
        pub local: Vec<IpAddr>,
        pub external: Result<IpAddr, String>,
        pub calls: RefCell<usize>,
    }

    impl FakeProbe {
        pub(crate) fn local(ip: &str) -> FakeProbe {
            FakeProbe {
                local: vec![ip.parse().unwrap()],
                external: Err("no network in tests".into()),
                calls: RefCell::new(0),
            }
        }
    }

    impl NetProbe for FakeProbe {
        fn local_addresses(&self) -> Vec<IpAddr> {
            self.local.clone()
        }
        fn external_ip(&self, _servers: &[String], _v4: bool) -> Result<IpAddr, String> {
            *self.calls.borrow_mut() += 1;
            self.external.clone()
        }
        fn sleep(&self, _d: Duration) {}
    }

    #[test]
    fn external_retries_three_times() {
        let p = FakeProbe::local("10.0.0.1");
        let err = determine_ip(&p, true, false, &[]).unwrap_err();
        assert_eq!(err, "could not resolve external IP: no network in tests");
        assert_eq!(*p.calls.borrow(), 3);
    }

    #[test]
    fn local_prefers_v4() {
        let p = FakeProbe {
            local: vec!["2001:db8::1".parse().unwrap(), "10.1.2.3".parse().unwrap()],
            external: Err(String::new()),
            calls: RefCell::new(0),
        };
        assert_eq!(determine_ip(&p, false, false, &[]).unwrap(), "10.1.2.3");
        let p6 = FakeProbe::local("2001:db8::1");
        assert_eq!(determine_ip(&p6, false, false, &[]).unwrap(), "2001:db8::1");
        assert_eq!(
            determine_ip(&p6, false, true, &[]).unwrap_err(),
            "no IPv4 address available"
        );
    }

    #[test]
    fn stun_response_parsing() {
        let tid = [7u8; 12];
        let mut resp = vec![0x01, 0x01, 0x00, 0x0c];
        resp.extend_from_slice(&MAGIC_COOKIE.to_be_bytes());
        resp.extend_from_slice(&tid);
        // XOR-MAPPED-ADDRESS, len 8, family v4, port ^ 0x2112, 192.0.2.1 ^ cookie
        resp.extend_from_slice(&[0x00, 0x20, 0x00, 0x08, 0x00, 0x01, 0x21, 0x13]);
        let c = MAGIC_COOKIE.to_be_bytes();
        resp.extend_from_slice(&[192 ^ c[0], c[1], 2 ^ c[2], 1 ^ c[3]]);
        assert_eq!(
            parse_binding_response(&resp, &tid),
            StunReply::Mapped("192.0.2.1".parse().unwrap())
        );
        assert_eq!(parse_binding_response(&resp, &[0u8; 12]), StunReply::Ignore);
        assert_eq!(build_binding_request(&tid)[0..2], [0x00, 0x01]);
    }
}

#[cfg(test)]
mod bound_validation_tests {
    use super::*;
    fn stun_fixture(mapped: std::net::Ipv4Addr) -> (String, std::thread::JoinHandle<()>) {
        let socket = UdpSocket::bind("127.0.0.1:0").unwrap();
        socket
            .set_read_timeout(Some(Duration::from_secs(2)))
            .unwrap();
        let address = socket.local_addr().unwrap().to_string();
        let task = std::thread::spawn(move || {
            let mut request = [0; 1500];
            let (size, peer) = socket.recv_from(&mut request).unwrap();
            assert_eq!(size, 20);
            let mut response = vec![0x01, 0x01, 0x00, 0x0c];
            response.extend_from_slice(&MAGIC_COOKIE.to_be_bytes());
            response.extend_from_slice(&request[8..20]);
            response.extend_from_slice(&[0x00, 0x20, 0x00, 0x08, 0x00, 0x01, 0x21, 0x12]);
            for (octet, mask) in mapped.octets().into_iter().zip(MAGIC_COOKIE.to_be_bytes()) {
                response.push(octet ^ mask);
            }
            socket.send_to(&response, peer).unwrap();
        });
        (address, task)
    }
    fn owned_port() -> u16 {
        UdpSocket::bind("127.0.0.1:0")
            .unwrap()
            .local_addr()
            .unwrap()
            .port()
    }
    #[test]
    fn actual_bound_stun_then_public_ip_hairpin_receipt_succeeds_and_releases_port() {
        let (server, fixture) = stun_fixture(std::net::Ipv4Addr::LOCALHOST);
        let port = owned_port();
        let result = SystemProbe
            .external_ip_from("127.0.0.1".parse().unwrap(), port, &[server], true)
            .unwrap();
        assert_eq!(result, "127.0.0.1".parse::<IpAddr>().unwrap());
        fixture.join().unwrap();
        let _released = UdpSocket::bind((std::net::Ipv4Addr::LOCALHOST, port))
            .expect("both STUN and validation socket released");
    }
    #[test]
    fn unreachable_external_address_rejected_but_explicit_skip_admits_same_stun_result() {
        let mapped = "127.0.0.2".parse().unwrap();
        let local = "127.0.0.1".parse().unwrap();
        let port = owned_port();
        let (server, fixture) = stun_fixture(mapped);
        let start = Instant::now();
        let error = SystemProbe
            .external_ip_from_context(
                local,
                port,
                &[server],
                true,
                &ProbeContext::new(Duration::from_millis(120)),
            )
            .unwrap_err();
        assert_eq!(error, "context deadline exceeded");
        assert!(start.elapsed() >= Duration::from_millis(100));
        fixture.join().unwrap();
        {
            let _released = UdpSocket::bind((std::net::Ipv4Addr::LOCALHOST, port))
                .expect("failed validation released source socket");
        }
        let (server, fixture) = stun_fixture(mapped);
        let result = SystemProbe
            .external_ip_from_context(
                local,
                port,
                &[server],
                false,
                &ProbeContext::new(Duration::from_millis(120)),
            )
            .unwrap();
        assert_eq!(result, IpAddr::V4(mapped));
        fixture.join().unwrap();
        let _released = UdpSocket::bind((std::net::Ipv4Addr::LOCALHOST, port))
            .expect("skip path released STUN source socket");
    }
    #[test]
    fn shared_discovery_cancellation_releases_active_validation_without_waiting_individual_timeout()
    {
        let (server, fixture) = stun_fixture("127.0.0.2".parse().unwrap());
        let local = "127.0.0.1".parse().unwrap();
        let port = owned_port();
        let context = ProbeContext::new(Duration::from_secs(5));
        let cancel = context.clone();
        let task = std::thread::spawn(move || {
            SystemProbe.external_ip_from_context(local, port, &[server], true, &context)
        });
        fixture.join().unwrap();
        cancel.cancel();
        assert_eq!(
            task.join().unwrap().unwrap_err(),
            "context deadline exceeded"
        );
        let _released = UdpSocket::bind((std::net::Ipv4Addr::LOCALHOST, port)).unwrap();
    }
}
