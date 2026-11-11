//! Media network I/O: socket ownership and demultiplexing to room tasks.
//!
//! * `udp_port` set: one UDP socket per interface shared by all transports; packets
//!   are routed by the ICE ufrag in STUN requests, then by remote address.
//! * otherwise: each transport gets dedicated UDP sockets from the port range.
//! * `tcp_port` set: passive ICE-TCP (RFC 6544) with RFC 4571 framing.
//!
//! Sockets bind to an interface address and candidates advertise a possibly
//! different address (1:1 NAT / `use_external_ip`). Native ICE keeps the actual
//! bound base distinct from signaled host address; all I/O uses the real base.

mod batch_tx;
pub(crate) mod mdns;
mod srflx;
mod stun_peek;
mod tcp;
mod udp;

use std::collections::{HashMap, HashSet};
use std::io;
use std::net::{IpAddr, SocketAddr};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::time::{Duration, Instant};

use rtc_core::config::NodeIp;
use rtc_core::ids::TransportId;
use tokio::net::UdpSocket;
use tokio::sync::{mpsc, watch};
use tokio_util::sync::CancellationToken;

pub use stun_peek::{is_stun, local_ufrag};

/// Largest datagram accepted from the network.
pub const MAX_DATAGRAM: usize = 2048;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LocalInterface {
    /// Address sockets bind to.
    pub bind: IpAddr,
    /// Address advertised in ICE candidates.
    pub advertise: IpAddr,
}

#[derive(Clone, Debug)]
pub struct NetConfig {
    /// False disables UDP entirely, including per-transport sockets.
    pub udp_enabled: bool,
    pub udp_port: Option<u16>,
    /// Inclusive range for dedicated per-transport sockets when `udp_port` is unset.
    /// `(0, 0)` means OS-chosen ephemeral ports.
    pub port_range: (u16, u16),
    pub tcp_port: Option<u16>,
    pub interfaces: Vec<LocalInterface>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum NetProto {
    Udp,
    Tcp,
}

/// One routed packet with both its signaled alias and physical local address.
#[derive(Debug)]
pub struct InboundPacket {
    pub transport: TransportId,
    pub proto: NetProto,
    pub source: SocketAddr,
    pub destination: SocketAddr,
    /// Actual bound socket identity, separate from the signaled alias.
    pub bound: SocketAddr,
    pub data: Vec<u8>,
    pub received_at: Instant,
}

/// Local candidate to add to a transport.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct LocalCandidate {
    pub proto: NetProto,
    pub addr: SocketAddr,
    /// Genuine local bind used by native ICE input/output routing.
    pub base: SocketAddr,
}

pub(crate) struct SrflxGather {
    id: TransportId,
    ufrag: String,
    generation: Arc<()>,
    sockets: Vec<(SocketAddr, SocketAddr, UdpSocket)>,
}

pub type PacketSink = mpsc::Sender<InboundPacket>;

#[derive(Clone)]
struct Route {
    transport: TransportId,
    generation: Arc<()>,
    stop: CancellationToken,
    sink: PacketSink,
    check_udp_base: bool,
}

struct TcpConnection {
    id: u64,
    generation: Arc<()>,
    tx: mpsc::Sender<Vec<u8>>,
}
type TcpConnections = HashMap<(SocketAddr, SocketAddr), TcpConnection>;
struct TcpTask {
    owner: Option<(TransportId, Arc<()>)>,
    stop: CancellationToken,
    done: watch::Receiver<bool>,
}
/// Lifetime is accepted before spawning a connection, including a peer still
/// waiting for its first frame. Completion is signalled on every return/drop.
pub(super) struct TcpCompletion {
    router: NetRouter,
    id: u64,
    done: watch::Sender<bool>,
}
impl Drop for TcpCompletion {
    fn drop(&mut self) {
        self.router.finish_tcp(self.id);
        let _ = self.done.send(true);
    }
}

type RemoteKey = (NetProto, SocketAddr, SocketAddr); // (proto, actual bound local, remote)

#[derive(Default)]
struct TransportEntry {
    generation: Arc<()>,
    stop: CancellationToken,
    ufrag: String,
    sink: Option<PacketSink>,
    remotes: Vec<RemoteKey>,
    admitted_bases: HashSet<(NetProto, SocketAddr)>,
    dedicated: Vec<(usize, LocalCandidate, CancellationToken)>,
    srflx_gather: Option<CancellationToken>,
    srflx_sockets: Vec<(SocketAddr, CancellationToken, tokio::task::JoinHandle<()>)>,
}

struct Inner {
    cfg: NetConfig,
    stun_servers: RwLock<Arc<[String]>>,
    append_internal_candidates: bool,
    dedicated_hosts: Vec<LocalInterface>,
    separate_dedicated_appends: bool,
    shared_candidates: Vec<LocalCandidate>,
    // Ordered physical-IP groups, like source MultiPortsUDPMux.
    udp_groups: Vec<Vec<LocalCandidate>>,
    udp_round_robin: Mutex<Vec<i32>>,
    by_ufrag: RwLock<HashMap<String, Route>>,
    by_remote: RwLock<HashMap<RemoteKey, Route>>,
    udp_sockets: RwLock<HashMap<SocketAddr, Arc<UdpSocket>>>,
    /// Per-source batched UDP writer, populated when `rtc.batch_io.batch_size`
    /// is set (SPEC-0074 C21). Empty means direct `try_send_to` passthrough.
    batch: RwLock<HashMap<SocketAddr, Arc<batch_tx::BatchConn>>>,
    tcp_conns: RwLock<TcpConnections>,
    tcp_tasks: Mutex<HashMap<u64, TcpTask>>,
    next_tcp_id: AtomicU64,
    transports: Mutex<HashMap<TransportId, TransportEntry>>,
    stop: CancellationToken,
}

/// Owner of all media sockets. Cheap to clone.
#[derive(Clone)]
pub struct NetRouter {
    inner: Arc<Inner>,
}

/// Outbound path handed to room tasks.
#[derive(Clone)]
pub struct NetSender {
    inner: Arc<Inner>,
}
/// Output capability captured once for one exact live transport generation.
/// A retired capability never resolves a replacement by TransportId or address.
#[derive(Clone)]
pub struct OwnedNetSender {
    sender: NetSender,
    transport: TransportId,
    generation: Arc<()>,
}

// The configured concrete-interface path is unchanged. Wildcard TCP is a
// listener policy, not a physical ICE route; expand only genuine UP addresses.
fn system_tcp_bases() -> io::Result<Vec<IpAddr>> {
    let mut bases = Vec::new();
    for device in getifs::interfaces()? {
        if !device.flags().contains(getifs::Flags::UP) {
            continue;
        }
        // Go localInterfaces skips a failed per-device address query.
        let Ok(addrs) = device.addrs() else {
            continue;
        };
        for address in addrs {
            let ip = address.addr();
            if ip.is_unspecified() || ip.is_multicast() {
                continue;
            }
            if let IpAddr::V4(v4) = ip
                && v4.is_broadcast()
            {
                continue;
            }
            // A scoped IPv6 link-local cannot be represented by IpAddr here.
            // The normal named-interface selector applies the same exclusion.
            if let IpAddr::V6(v6) = ip
                && v6.is_unicast_link_local()
            {
                continue;
            }
            if !bases.contains(&ip) {
                bases.push(ip);
            }
        }
    }
    Ok(bases)
}
/// A Go `*net.OpError` listen failure (`listen tcp :7881: bind: ...`), carried
/// inside the returned `io::Error` so the process prints upstream's text.
#[derive(Debug)]
pub struct ListenError(pub String);
impl std::fmt::Display for ListenError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}
impl std::error::Error for ListenError {}

/// Go's lower-case errno text (`address already in use`).
pub fn go_errno_text(e: &io::Error) -> String {
    let s = e.to_string();
    let s = s.split(" (os error ").next().unwrap_or(&s).to_string();
    let mut c = s.chars();
    match c.next() {
        Some(first) => first.to_lowercase().collect::<String>() + c.as_str(),
        None => s,
    }
}

/// Go `*net.OpError` text for a UDP listen failure:
/// `listen udp {ip}:{port}: bind: {errno}`. Carried inside the returned
/// `io::Error` so `media_text` prints upstream's message verbatim.
pub(super) fn udp_listen_error(addr: impl std::fmt::Display, e: &io::Error) -> io::Error {
    io::Error::new(
        e.kind(),
        ListenError(format!("listen udp {addr}: bind: {}", go_errno_text(e))),
    )
}

/// Go `net.ListenTCP("tcp", &TCPAddr{Port: port})`: the dual-stack wildcard,
/// or IPv4 any where IPv6 is unavailable.
async fn listen_tcp_wildcard(port: i64) -> io::Result<tokio::net::TcpListener> {
    let fail = |e: io::Error| {
        io::Error::new(
            e.kind(),
            ListenError(format!("listen tcp :{port}: bind: {}", go_errno_text(&e))),
        )
    };
    let port = u16::try_from(port).map_err(|_| {
        fail(io::Error::new(
            io::ErrorKind::InvalidInput,
            "invalid argument",
        ))
    })?;
    match tokio::net::TcpListener::bind((std::net::Ipv6Addr::UNSPECIFIED, port)).await {
        Ok(l) => Ok(l),
        Err(e) if e.kind() == io::ErrorKind::AddrInUse => Err(fail(e)),
        Err(_) => tokio::net::TcpListener::bind((std::net::Ipv4Addr::UNSPECIFIED, port))
            .await
            .map_err(fail),
    }
}

fn tcp_interface_bases(bind: IpAddr, actual: &[IpAddr]) -> Vec<IpAddr> {
    if !bind.is_unspecified() {
        return vec![bind];
    }
    actual
        .iter()
        .copied()
        .filter(|ip| ip.is_ipv4() == bind.is_ipv4())
        .collect()
}

impl NetRouter {
    /// Bind shared sockets and start reader tasks. Must run inside a tokio runtime.
    pub async fn start(cfg: NetConfig) -> io::Result<NetRouter> {
        let ports: Vec<u16> = cfg.udp_port.into_iter().collect();
        Self::start_with_udp_ports(cfg, &ports).await
    }

    /// Bind the source-selected configured mux ports. The legacy single-port
    /// constructor stays compatible. Each gather admits one port per physical
    /// IP (source atomic Add(1), then modulo), not every configured port.
    pub async fn start_with_udp_ports(cfg: NetConfig, ports: &[u16]) -> io::Result<NetRouter> {
        Self::start_with_address_rewrites(cfg, ports, false).await
    }

    /// Original ICEAddressRewriteAppend: advertise internal candidates alongside
    /// their rewritten aliases, while binding/owning each physical socket once.
    pub async fn start_with_address_rewrites(
        cfg: NetConfig,
        ports: &[u16],
        append_internal_candidates: bool,
    ) -> io::Result<NetRouter> {
        Self::start_with_standalone_udp_port(cfg, ports, None, append_internal_candidates).await
    }

    /// Bind the regular UDP muxes and a standalone port advertised by every
    /// gather. The standalone socket is excluded from the per-IP rotation.
    pub async fn start_with_standalone_udp_port(
        cfg: NetConfig,
        ports: &[u16],
        standalone_port: Option<u16>,
        append_internal_candidates: bool,
    ) -> io::Result<NetRouter> {
        let tcp_port = cfg.tcp_port.map(i64::from);
        let ports: Vec<_> = ports.iter().copied().map(i64::from).collect();
        Self::start_with_configured_ports(
            cfg,
            &ports,
            standalone_port,
            tcp_port,
            append_internal_candidates,
            None,
        )
        .await
    }

    /// Bind the official raw media ports in listener order. Values that
    /// cannot fit in a socket address fail at the corresponding bind step.
    pub async fn start_with_configured_ports(
        mut cfg: NetConfig,
        ports: &[i64],
        standalone_port: Option<u16>,
        tcp_port: Option<i64>,
        append_internal_candidates: bool,
        explicit_node_ip: Option<&NodeIp>,
    ) -> io::Result<NetRouter> {
        cfg.udp_port = ports
            .first()
            .and_then(|&port| u16::try_from(port).ok())
            .or(standalone_port);
        // Explicit name/IP filters may select zero addresses, matching zero
        // upstream UDP muxes. Never bypass them with a primary-IP fallback.
        // Bind transactionally: no reader task owns sockets until every bind
        // succeeds, so an error drops all previously opened descriptors.
        // Ordinary config already contains selected concrete interface bases.
        // Only the legacy public wildcard TCP form needs actual OS expansion.
        let tcp_bases =
            if tcp_port.is_some() && cfg.interfaces.iter().any(|i| i.bind.is_unspecified()) {
                system_tcp_bases()?
            } else {
                Vec::new()
            };
        let mut shared_udp = Vec::new();
        let mut shared_tcp = Vec::new();
        let mut shared_candidates: Vec<LocalCandidate> = Vec::new();
        if cfg.udp_enabled {
            let standalone_port = standalone_port.map(i64::from);
            for (mux_ports, standalone) in [(ports, false), (standalone_port.as_slice(), true)] {
                for itf in &cfg.interfaces {
                    for &port in mux_ports {
                        let port = u16::try_from(port).map_err(|_| {
                            let addr = match itf.bind {
                                IpAddr::V4(ip) => format!("{ip}:{port}"),
                                IpAddr::V6(ip) => format!("[{ip}]:{port}"),
                            };
                            udp_listen_error(
                                addr,
                                &io::Error::new(io::ErrorKind::InvalidInput, "invalid argument"),
                            )
                        })?;
                        let requested = SocketAddr::new(itf.bind, port);
                        let socket = Arc::new(
                            udp::bind(requested).map_err(|e| udp_listen_error(requested, &e))?,
                        );
                        let bound = socket.local_addr()?;
                        let advertised = SocketAddr::new(itf.advertise, bound.port());
                        if standalone {
                            shared_candidates.push(LocalCandidate {
                                proto: NetProto::Udp,
                                addr: advertised,
                                base: bound,
                            });
                        }
                        shared_udp.push((bound, advertised, socket, standalone));
                    }
                }
            }
        }
        if let Some(port) = tcp_port {
            // webrtc_config.go: ONE `net.ListenTCP("tcp", &TCPAddr{Port})`
            // wildcard listener; candidates are still per selected interface.
            let listener = listen_tcp_wildcard(port).await?;
            let port = listener.local_addr()?.port();
            shared_tcp.push(listener);
            for itf in &cfg.interfaces {
                let bound = SocketAddr::new(itf.bind, port);
                for base_ip in tcp_interface_bases(itf.bind, &tcp_bases) {
                    // An unspecified advertised address means no rewrite. A
                    // wildcard listener still yields concrete host candidates.
                    let advertised_ip = if itf.advertise.is_unspecified() {
                        base_ip
                    } else {
                        itf.advertise
                    };
                    let advertised = SocketAddr::new(advertised_ip, bound.port());
                    // The listener may be wildcard; ICE and accepted streams use
                    // a concrete selected base. Pion candidate.Equal excludes
                    // physical base, so repeated public TCP addresses keep the
                    // first successfully constructed candidate.
                    if !shared_candidates
                        .iter()
                        .any(|c| c.proto == NetProto::Tcp && c.addr == advertised)
                    {
                        shared_candidates.push(LocalCandidate {
                            proto: NetProto::Tcp,
                            addr: advertised,
                            base: SocketAddr::new(base_ip, bound.port()),
                        });
                    }
                }
            }
        }
        let mut udp_groups: Vec<Vec<LocalCandidate>> = Vec::new();
        for (base, addr, _, standalone) in &shared_udp {
            if *standalone {
                continue;
            }
            let c = LocalCandidate {
                proto: NetProto::Udp,
                addr: *addr,
                base: *base,
            };
            if let Some(group) = udp_groups.iter_mut().find(|g| g[0].base.ip() == base.ip()) {
                group.push(c);
            } else {
                udp_groups.push(vec![c]);
            }
        }
        // Tokio's first try_send_to may report WouldBlock solely because its
        // newly registered descriptor has no cached readiness yet. Admit every
        // shared mux before exposing any reader/sender, preserving transactional
        // rollback if an initial readiness check fails. Later sends stay
        // nonblocking and retain their existing backpressure behavior.
        for (_, _, socket, _) in &shared_udp {
            socket.writable().await?;
        }
        // Pion's dedicated gather binds once for each append rule result, even
        // when the explicit rewrite has the same IP as the internal address.
        let explicit_append = explicit_node_ip.filter(|_| append_internal_candidates);
        let mut dedicated_hosts = Vec::new();
        for itf in &cfg.interfaces {
            if explicit_append.is_some_and(|ip| match itf.bind {
                IpAddr::V4(_) => ip.v4.is_some(),
                IpAddr::V6(_) => ip.v6.is_some(),
            }) {
                dedicated_hosts.push(LocalInterface {
                    bind: itf.bind,
                    advertise: itf.bind,
                });
            }
            dedicated_hosts.push(itf.clone());
        }
        let udp_round_robin = Mutex::new(vec![0; udp_groups.len()]);
        let inner = Arc::new(Inner {
            cfg: cfg.clone(),
            stun_servers: RwLock::new(Arc::from([])),
            append_internal_candidates,
            dedicated_hosts,
            separate_dedicated_appends: explicit_append.is_some(),
            shared_candidates,
            udp_groups,
            udp_round_robin,
            by_ufrag: RwLock::new(HashMap::new()),
            by_remote: RwLock::new(HashMap::new()),
            udp_sockets: RwLock::new(HashMap::new()),
            batch: RwLock::new(HashMap::new()),
            tcp_conns: RwLock::new(HashMap::new()),
            tcp_tasks: Mutex::new(HashMap::new()),
            next_tcp_id: AtomicU64::new(1),
            transports: Mutex::new(HashMap::new()),
            stop: CancellationToken::new(),
        });
        let router = NetRouter { inner };

        for (bound, advertised, socket, _) in shared_udp {
            router.register_udp(bound, socket.clone());
            tokio::spawn(udp::mux_reader(router.clone(), socket, advertised, bound));
        }
        for listener in shared_tcp {
            let (_, stop, completion) = router
                .begin_tcp()
                .expect("new router cannot be stopped before listener startup");
            tokio::spawn(tcp::accept_loop(router.clone(), listener, stop, completion));
        }
        Ok(router)
    }

    pub fn sender(&self) -> NetSender {
        NetSender {
            inner: self.inner.clone(),
        }
    }

    pub fn config(&self) -> &NetConfig {
        &self.inner.cfg
    }

    /// SPEC-0074 C21: wrap each bound UDP mux socket in a batched writer when
    /// `rtc.batch_io.batch_size > 0`, mirroring upstream `webrtc_config.go:146`
    /// (`UDPMuxFromPortWithBatchWrite`). On Linux the wrappers accumulate and
    /// flush via `sendmmsg`; elsewhere `write` passthroughs to the socket, just
    /// as pion's non-Linux `batchConn == nil` path does. A zero size disables
    /// batching, leaving the direct `try_send_to` send path untouched.
    pub async fn enable_batch(&self, batch_size: usize, interval: Duration) {
        if batch_size == 0 {
            return;
        }
        let sockets: Vec<(SocketAddr, Arc<UdpSocket>)> = read(&self.inner.udp_sockets)
            .iter()
            .map(|(bound, socket)| (*bound, Arc::clone(socket)))
            .collect();
        let map = batch_tx::install(&sockets, batch_size, interval, &self.inner.stop);
        *write(&self.inner.batch) = map;
    }

    /// Register a transport and return the local candidates it should advertise.
    pub fn open_transport(
        &self,
        id: TransportId,
        ufrag: &str,
        sink: PacketSink,
    ) -> io::Result<Vec<LocalCandidate>> {
        let mut transports = lock(&self.inner.transports);
        if self.inner.stop.is_cancelled() {
            return Err(io::Error::new(
                io::ErrorKind::NotConnected,
                "media router is shut down",
            ));
        }
        if transports.contains_key(&id) || read(&self.inner.by_ufrag).contains_key(ufrag) {
            return Err(io::Error::new(
                io::ErrorKind::AlreadyExists,
                "transport id or ICE ufrag is already registered",
            ));
        }
        let owner_stop = self.inner.stop.child_token();
        let route = Route {
            transport: id,
            generation: Arc::new(()),
            stop: owner_stop.clone(),
            sink,
            check_udp_base: false,
        };
        let mut entry = TransportEntry {
            generation: route.generation.clone(),
            stop: owner_stop,
            ufrag: ufrag.to_string(),
            sink: Some(route.sink.clone()),
            ..Default::default()
        };

        // Collect usable sockets before publishing any candidate or route.
        let mut dedicated = Vec::new();
        let mut dedicated_error = None;
        if self.inner.cfg.udp_enabled && self.inner.cfg.udp_port.is_none() {
            for (index, host) in self.inner.dedicated_hosts.iter().enumerate() {
                match self.bind_dedicated_host(host) {
                    Ok((candidate, socket)) => dedicated.push((index, candidate, socket)),
                    Err(error) => {
                        tracing::warn!(bind = %host.bind, %error, "could not allocate dedicated UDP");
                        dedicated_error.get_or_insert(error);
                    }
                }
            }
        }
        let mut candidates = self.gather_shared_candidates();
        if candidates.is_empty() && dedicated.is_empty() {
            if let Some(error) = dedicated_error {
                return Err(error);
            }
        }
        for (index, candidate, socket) in dedicated {
            if self.install_dedicated_udp(&mut entry, &route, index, candidate, socket) {
                candidates.push(candidate);
            }
        }

        self.append_internal_candidates(&mut candidates);

        entry
            .admitted_bases
            .extend(candidates.iter().map(|c| (c.proto, c.base)));
        write(&self.inner.by_ufrag).insert(ufrag.to_string(), route);
        transports.insert(id, entry);
        Ok(candidates)
    }

    fn gather_shared_candidates(&self) -> Vec<LocalCandidate> {
        let mut candidates = Vec::new();
        let mut counters = lock(&self.inner.udp_round_robin);
        let rotating =
            self.inner
                .udp_groups
                .iter()
                .zip(counters.iter_mut())
                .map(|(group, counter)| {
                    *counter = counter.wrapping_add(1);
                    let index = *counter % (group.len() as i32);
                    // Match the source's signed int32 round robin counter.
                    group[usize::try_from(index).expect("UDP mux round-robin index overflow")]
                });
        for candidate in rotating.chain(self.inner.shared_candidates.iter().copied()) {
            // Pion deduplicates rewritten advertised endpoints before GetConn.
            if !candidates
                .iter()
                .any(|c: &LocalCandidate| c.proto == candidate.proto && c.addr == candidate.addr)
            {
                candidates.push(candidate);
            }
        }
        candidates
    }

    fn append_internal_candidates(&self, candidates: &mut Vec<LocalCandidate>) {
        if self.inner.append_internal_candidates {
            let internal: Vec<_> = candidates
                .iter()
                .filter(|c| c.addr != c.base)
                .filter(|c| {
                    !self.inner.separate_dedicated_appends
                        || self.inner.cfg.udp_port.is_some()
                        || c.proto != NetProto::Udp
                })
                .map(|c| LocalCandidate {
                    proto: c.proto,
                    addr: c.base,
                    base: c.base,
                })
                .collect();
            for candidate in internal {
                if !candidates
                    .iter()
                    .any(|c| c.proto == candidate.proto && c.addr == candidate.addr)
                {
                    candidates.push(candidate);
                }
            }
        }
    }

    fn bind_dedicated_host(
        &self,
        host: &LocalInterface,
    ) -> io::Result<(LocalCandidate, Arc<UdpSocket>)> {
        let (socket, port) = udp::bind_in_range(host.bind, self.inner.cfg.port_range)?;
        let candidate = LocalCandidate {
            proto: NetProto::Udp,
            addr: SocketAddr::new(host.advertise, port),
            base: socket.local_addr()?,
        };
        Ok((candidate, Arc::new(socket)))
    }

    fn install_dedicated_udp(
        &self,
        entry: &mut TransportEntry,
        route: &Route,
        index: usize,
        candidate: LocalCandidate,
        socket: Arc<UdpSocket>,
    ) -> bool {
        // The source ICE agent compares advertised host endpoints, even
        // when the sockets were bound to different local interfaces.
        if entry
            .dedicated
            .iter()
            .any(|(_, current, _)| current.addr == candidate.addr)
        {
            return false;
        }
        let bound = candidate.base;
        let token = entry.stop.child_token();
        let reader_route = Route {
            stop: token.clone(),
            ..route.clone()
        };
        self.register_udp(bound, socket.clone());
        tokio::spawn(udp::dedicated_reader(
            self.clone(),
            socket,
            candidate.addr,
            bound,
            reader_route,
            token.clone(),
        ));
        entry.admitted_bases.insert((NetProto::Udp, bound));
        entry.dedicated.push((index, candidate, token));
        true
    }

    pub(crate) fn rotates_udp_mux(&self) -> bool {
        self.inner.udp_groups.iter().any(|group| group.len() > 1)
    }

    pub(crate) fn uses_dedicated_udp(&self) -> bool {
        self.inner.cfg.udp_enabled && self.inner.cfg.udp_port.is_none()
    }

    /// Install the new ICE credentials and retry dedicated interfaces that
    /// could not bind on an earlier gather. When rotating mux or refreshing
    /// dedicated sockets, return the full replacement UDP host set.
    pub fn update_ufrag(
        &self,
        id: TransportId,
        new_ufrag: &str,
        refresh_dedicated: bool,
    ) -> Vec<LocalCandidate> {
        let mut transports = lock(&self.inner.transports);
        let Some(entry) = transports.get_mut(&id) else {
            return Vec::new();
        };
        if entry.ufrag == new_ufrag {
            return Vec::new();
        }
        let route = {
            let mut by_ufrag = write(&self.inner.by_ufrag);
            let Some(route) = by_ufrag.remove(&entry.ufrag) else {
                return Vec::new();
            };
            by_ufrag.insert(new_ufrag.to_string(), route.clone());
            route
        };
        entry.ufrag = new_ufrag.to_string();

        if self.rotates_udp_mux() {
            let mut candidates = self.gather_shared_candidates();
            candidates.retain(|candidate| candidate.proto == NetProto::Udp);
            self.append_internal_candidates(&mut candidates);
            let new_bases: HashSet<_> = candidates.iter().map(|c| (c.proto, c.base)).collect();
            // Keep non-mux bases (notably the dedicated srflx sockets, whose
            // own generation is retired by start_srflx).
            for candidate in self.inner.udp_groups.iter().flatten() {
                let key = (NetProto::Udp, candidate.base);
                if !new_bases.contains(&key) {
                    entry.admitted_bases.remove(&key);
                }
            }
            entry.admitted_bases.extend(new_bases);
            let mut remotes = write(&self.inner.by_remote);
            entry.remotes.retain(|key| {
                if key.0 != NetProto::Udp || entry.admitted_bases.contains(&(key.0, key.1)) {
                    return true;
                }
                if remotes
                    .get(key)
                    .is_some_and(|r| Arc::ptr_eq(&r.generation, &entry.generation))
                {
                    remotes.remove(key);
                }
                false
            });
            return candidates;
        }

        let mut candidates = Vec::new();
        if !self.uses_dedicated_udp() {
            return candidates;
        }
        let previous_count = entry.dedicated.len();
        for (index, host) in self.inner.dedicated_hosts.iter().enumerate() {
            if entry.dedicated.iter().any(|(slot, _, _)| *slot == index) {
                continue;
            }
            let Ok((candidate, socket)) = self.bind_dedicated_host(host) else {
                continue;
            };
            if !self.install_dedicated_udp(entry, &route, index, candidate, socket) {
                continue;
            }
            candidates.push(candidate);
            if self.inner.append_internal_candidates
                && !self.inner.separate_dedicated_appends
                && candidate.addr != candidate.base
            {
                candidates.push(LocalCandidate {
                    addr: candidate.base,
                    ..candidate
                });
            }
        }
        if refresh_dedicated {
            // Fill missing interface slots first. An existing slot only
            // gives up its port once a replacement has been bound, so a
            // fully occupied range can still restart on its usable port.
            for index in (0..previous_count).rev() {
                let (slot, old, _) = entry.dedicated[index];
                let Ok((candidate, socket)) =
                    self.bind_dedicated_host(&self.inner.dedicated_hosts[slot])
                else {
                    continue;
                };
                if self.install_dedicated_udp(entry, &route, slot, candidate, socket) {
                    let (_, _, token) = entry.dedicated.remove(index);
                    token.cancel();
                    self.retire_udp_base(entry, old.base);
                }
            }
            candidates = entry
                .dedicated
                .iter()
                .map(|(_, candidate, _)| *candidate)
                .collect();
            self.append_internal_candidates(&mut candidates);
        }
        candidates
    }

    fn retire_udp_base(&self, entry: &mut TransportEntry, bound: SocketAddr) {
        write(&self.inner.udp_sockets).remove(&bound);
        entry.admitted_bases.remove(&(NetProto::Udp, bound));
        let mut remotes = write(&self.inner.by_remote);
        entry.remotes.retain(|key| {
            if key.0 == NetProto::Udp && key.1 == bound {
                if remotes
                    .get(key)
                    .is_some_and(|route| Arc::ptr_eq(&route.generation, &entry.generation))
                {
                    remotes.remove(key);
                }
                false
            } else {
                true
            }
        });
    }

    /// Enable per-transport STUN separately from node address discovery and client STUN.
    /// With a host UDP mux, the configured ephemeral range is (0, 0).
    /// The srflx sockets stay dedicated to the transport in both modes.
    pub fn enable_server_stun(&self, servers: Vec<String>) {
        if self.inner.cfg.udp_enabled {
            *write(&self.inner.stun_servers) = servers.into();
        }
    }

    pub(crate) fn server_stun_enabled(&self) -> bool {
        !read(&self.inner.stun_servers).is_empty()
    }

    pub(crate) fn start_srflx(
        &self,
        id: TransportId,
        ufrag: &str,
        initial: bool,
        jobs: &mut tokio::task::JoinSet<SrflxGather>,
    ) {
        let mut transports = lock(&self.inner.transports);
        let Some(entry) = transports.get_mut(&id) else {
            return;
        };
        if entry.ufrag != ufrag || (!initial && entry.srflx_gather.is_none()) {
            return;
        }
        let retired = self.retire_srflx(entry);
        let stop = entry.stop.child_token();
        entry.srflx_gather = Some(stop.clone());
        let generation = entry.generation.clone();
        let servers = read(&self.inner.stun_servers).clone();
        let interfaces: HashSet<_> = self.inner.cfg.interfaces.iter().map(|i| i.bind).collect();
        let range = self.inner.cfg.port_range;
        let ufrag = ufrag.to_owned();
        jobs.spawn(async move {
            let gather = async {
                for reader in retired {
                    let _ = reader.await;
                }
                let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
                let mut probes = tokio::task::JoinSet::new();
                for bind in interfaces {
                    for index in 0..servers.len() {
                        let servers = servers.clone();
                        probes.spawn(async move {
                            let endpoint = tokio::net::lookup_host(&servers[index])
                                .await
                                .ok()?
                                .find(|addr| addr.is_ipv4() == bind.is_ipv4())?;
                            let (socket, port) = udp::bind_in_range(bind, range).ok()?;
                            let mapped = srflx::gather(
                                &socket,
                                endpoint,
                                deadline.saturating_duration_since(tokio::time::Instant::now()),
                            )
                            .await
                            .ok()?;
                            if mapped.is_ipv4() != bind.is_ipv4()
                                || mapped.ip().is_unspecified()
                                || mapped.port() == 0
                            {
                                return None;
                            }
                            Some((mapped, SocketAddr::new(bind, port), socket))
                        });
                    }
                }
                let mut sockets = Vec::new();
                while let Ok(Some(result)) =
                    tokio::time::timeout_at(deadline, probes.join_next()).await
                {
                    if let Ok(Some(socket)) = result {
                        sockets.push(socket);
                    }
                }
                sockets
            };
            let sockets = tokio::select! {
                biased;
                _ = stop.cancelled() => Vec::new(),
                sockets = gather => sockets,
            };
            SrflxGather {
                id,
                ufrag,
                generation,
                sockets,
            }
        });
    }

    pub(crate) fn admit_srflx(
        &self,
        gather: SrflxGather,
    ) -> Option<(TransportId, String, Vec<(SocketAddr, SocketAddr)>)> {
        let SrflxGather {
            id,
            ufrag,
            generation,
            sockets,
        } = gather;
        let mut transports = lock(&self.inner.transports);
        let entry = transports.get_mut(&id)?;
        if entry.ufrag != ufrag || !Arc::ptr_eq(&entry.generation, &generation) {
            return None;
        }
        let route = read(&self.inner.by_ufrag).get(&ufrag)?.clone();
        let mut candidates = Vec::new();
        for (mapped, bound, socket) in sockets {
            let socket = Arc::new(socket);
            let stop = entry.stop.child_token();
            self.register_udp(bound, socket.clone());
            entry.admitted_bases.insert((NetProto::Udp, bound));
            let reader_route = Route {
                stop: stop.clone(),
                ..route.clone()
            };
            let reader = tokio::spawn(udp::dedicated_reader(
                self.clone(),
                socket,
                mapped,
                bound,
                reader_route,
                stop.clone(),
            ));
            entry.srflx_sockets.push((bound, stop, reader));
            candidates.push((mapped, bound));
        }
        Some((id, ufrag, candidates))
    }

    fn retire_srflx(&self, entry: &mut TransportEntry) -> Vec<tokio::task::JoinHandle<()>> {
        if let Some(stop) = entry.srflx_gather.take() {
            stop.cancel();
        }
        if entry.srflx_sockets.is_empty() {
            return Vec::new();
        }
        let mut sockets = write(&self.inner.udp_sockets);
        let mut remotes = write(&self.inner.by_remote);
        entry
            .srflx_sockets
            .drain(..)
            .map(|(bound, stop, reader)| {
                stop.cancel();
                sockets.remove(&bound);
                entry.admitted_bases.remove(&(NetProto::Udp, bound));
                entry.remotes.retain(|key| {
                    if key.0 == NetProto::Udp && key.1 == bound {
                        remotes.remove(key);
                        false
                    } else {
                        true
                    }
                });
                reader
            })
            .collect()
    }

    pub fn close_transport(&self, id: TransportId) {
        self.retire_transport(id);
    }

    /// Retire this registration and await every accepted TCP task attached to
    /// its generation. No owner/map lock is held across the wait.
    pub async fn close_transport_and_wait(&self, id: TransportId) {
        for mut done in self.retire_transport(id) {
            while !*done.borrow() {
                if done.changed().await.is_err() {
                    break;
                }
            }
        }
    }

    fn retire_transport(&self, id: TransportId) -> Vec<watch::Receiver<bool>> {
        // Keep retirement and generation replacement mutually exclusive until
        // all maps have been cleaned. A concurrent waiter may join a retirement
        // still in flight without retaining unbounded generation tombstones.
        let mut owners = lock(&self.inner.transports);
        let Some(mut entry) = owners.remove(&id) else {
            return lock(&self.inner.tcp_tasks)
                .values()
                .filter(|task| task.owner.as_ref().is_some_and(|(owner, _)| *owner == id))
                .map(|task| {
                    task.stop.cancel();
                    task.done.clone()
                })
                .collect();
        };
        entry.stop.cancel();
        self.retire_srflx(&mut entry);
        let completions: Vec<_> = lock(&self.inner.tcp_tasks)
            .values()
            .filter(|t| {
                t.owner.as_ref().is_some_and(|(owner, generation)| {
                    *owner == id && Arc::ptr_eq(generation, &entry.generation)
                })
            })
            .map(|t| {
                t.stop.cancel();
                t.done.clone()
            })
            .collect();
        write(&self.inner.tcp_conns).retain(|_, c| !Arc::ptr_eq(&c.generation, &entry.generation));
        {
            let mut by_ufrag = write(&self.inner.by_ufrag);
            if by_ufrag
                .get(&entry.ufrag)
                .is_some_and(|r| Arc::ptr_eq(&r.generation, &entry.generation))
            {
                by_ufrag.remove(&entry.ufrag);
            }
        }
        {
            let mut by_remote = write(&self.inner.by_remote);
            for key in &entry.remotes {
                if by_remote
                    .get(key)
                    .is_some_and(|r| Arc::ptr_eq(&r.generation, &entry.generation))
                {
                    by_remote.remove(key);
                }
            }
        }
        let mut sockets = write(&self.inner.udp_sockets);
        for (_, candidate, token) in entry.dedicated {
            token.cancel();
            sockets.remove(&candidate.base);
        }
        completions
    }

    pub fn shutdown(&self) {
        let mut owners = lock(&self.inner.transports);
        self.inner.stop.cancel();
        write(&self.inner.udp_sockets).clear();
        write(&self.inner.tcp_conns).clear();
        write(&self.inner.by_ufrag).clear();
        write(&self.inner.by_remote).clear();
        owners.clear();
    }

    fn register_udp(&self, bound: SocketAddr, socket: Arc<UdpSocket>) {
        write(&self.inner.udp_sockets).insert(bound, socket);
    }

    pub(crate) fn stop_token(&self) -> CancellationToken {
        self.inner.stop.clone()
    }

    /// Pion checks the registered remote first. A previously unseen remote
    /// can select an existing ufrag for STUN delivery, but receiving the packet
    /// does not register it. Only the actual owner's outbound WriteTo does.
    fn route(
        &self,
        proto: NetProto,
        local: SocketAddr,
        remote: SocketAddr,
        data: &[u8],
    ) -> Option<Route> {
        let owners = lock(&self.inner.transports);
        if self.inner.stop.is_cancelled() {
            return None;
        }
        let key = (proto, local, remote);
        let mut route = read(&self.inner.by_remote).get(&key).cloned().or_else(|| {
            local_ufrag(data).and_then(|u| read(&self.inner.by_ufrag).get(u).cloned())
        })?;
        let entry = owners.get(&route.transport)?;
        if !Arc::ptr_eq(&entry.generation, &route.generation)
            || route.stop.is_cancelled()
            || !entry.admitted_bases.contains(&(proto, local))
        {
            return None;
        }
        // Recheck this shared UDP base when handing the packet to the room:
        // the transport can rotate to a different port after this lookup.
        route.check_udp_base = proto == NetProto::Udp;
        Some(route)
    }

    fn deliver(
        &self,
        route: &Route,
        proto: NetProto,
        local: SocketAddr,
        bound: SocketAddr,
        remote: SocketAddr,
        data: Vec<u8>,
    ) {
        // Linearize delivery with retirement. A captured TCP/dedicated route
        // cannot continue feeding a shared room sink after its owner is gone.
        let owners = lock(&self.inner.transports);
        if self.inner.stop.is_cancelled()
            || route.stop.is_cancelled()
            || !owners.get(&route.transport).is_some_and(|e| {
                Arc::ptr_eq(&e.generation, &route.generation)
                    && (!route.check_udp_base || e.admitted_bases.contains(&(proto, bound)))
            })
        {
            return;
        }
        tracing::trace!(?proto, %local, %remote, transport = %route.transport, len = data.len(), first_octet = ?data.first().copied(), "routing media packet to room");
        let pkt = InboundPacket {
            transport: route.transport,
            proto,
            source: remote,
            destination: local,
            bound,
            data,
            // Stamp at the real delivery producer, in the same monotonic domain
            // used by RoomTask/native timeout handling (unpaused: ordinary std).
            received_at: tokio::time::Instant::now().into_std(),
        };
        // Media is loss tolerant; a full room queue drops instead of blocking I/O.
        if route.sink.try_send(pkt).is_err() {
            tracing::trace!(transport = %route.transport, "room packet queue full, dropping");
        }
    }

    fn tcp_advertised(&self, base: SocketAddr) -> Option<SocketAddr> {
        self.inner
            .shared_candidates
            .iter()
            .find(|c| c.proto == NetProto::Tcp && c.base == base)
            .map(|c| c.addr)
    }

    fn begin_tcp(&self) -> Option<(u64, CancellationToken, TcpCompletion)> {
        let mut tasks = lock(&self.inner.tcp_tasks);
        if self.inner.stop.is_cancelled() {
            return None;
        }
        let id = self.inner.next_tcp_id.fetch_add(1, Ordering::Relaxed);
        let stop = self.inner.stop.child_token();
        let (done, receiver) = watch::channel(false);
        tasks.insert(
            id,
            TcpTask {
                owner: None,
                stop: stop.clone(),
                done: receiver,
            },
        );
        Some((
            id,
            stop,
            TcpCompletion {
                router: self.clone(),
                id,
                done,
            },
        ))
    }

    fn register_tcp(
        &self,
        id: u64,
        route: &Route,
        local: SocketAddr,
        remote: SocketAddr,
        tx: mpsc::Sender<Vec<u8>>,
    ) -> bool {
        let owners = lock(&self.inner.transports);
        if route.stop.is_cancelled()
            || !owners
                .get(&route.transport)
                .is_some_and(|e| Arc::ptr_eq(&e.generation, &route.generation))
        {
            return false;
        }
        let mut tasks = lock(&self.inner.tcp_tasks);
        let Some(task) = tasks.get_mut(&id) else {
            return false;
        };
        task.owner = Some((route.transport, route.generation.clone()));
        let old = write(&self.inner.tcp_conns).insert(
            (local, remote),
            TcpConnection {
                id,
                generation: route.generation.clone(),
                tx,
            },
        );
        if let Some(old) = old
            && let Some(task) = tasks.get(&old.id)
        {
            task.stop.cancel();
        }
        true
    }

    fn finish_tcp(&self, id: u64) {
        lock(&self.inner.tcp_tasks).remove(&id);
        // A retired old connection cannot remove a newer same-tuple writer.
        write(&self.inner.tcp_conns).retain(|_, c| c.id != id);
    }

    /// Global cancellation plus collection includes idle first-frame peers.
    pub async fn shutdown_and_wait(&self) {
        self.shutdown();
        let completions: Vec<_> = lock(&self.inner.tcp_tasks)
            .values()
            .map(|t| t.done.clone())
            .collect();
        for mut done in completions {
            while !*done.borrow() {
                if done.changed().await.is_err() {
                    break;
                }
            }
        }
    }
}

impl NetSender {
    /// Bind the exact current registration once, before its media Rtc emits.
    pub fn for_transport(&self, transport: TransportId) -> Option<OwnedNetSender> {
        let owners = lock(&self.inner.transports);
        let entry = owners.get(&transport)?;
        if self.inner.stop.is_cancelled() || entry.stop.is_cancelled() {
            return None;
        }
        Some(OwnedNetSender {
            sender: self.clone(),
            transport,
            generation: entry.generation.clone(),
        })
    }

    /// Send one datagram/frame. Never blocks; drops on backpressure.
    pub fn send(&self, proto: NetProto, source: SocketAddr, destination: SocketAddr, data: &[u8]) {
        match proto {
            NetProto::Udp => {
                let socket = read(&self.inner.udp_sockets).get(&source).cloned();
                let Some(socket) = socket else {
                    tracing::debug!(%source, "no UDP socket for source address");
                    return;
                };
                if self.inner.send_batched_udp(source, destination, data) {
                    return;
                }
                if let Err(e) = socket.try_send_to(data, destination)
                    && e.kind() != io::ErrorKind::WouldBlock
                {
                    tracing::debug!(%destination, error = %e, "udp send failed");
                }
            }
            NetProto::Tcp => {
                let tx = read(&self.inner.tcp_conns)
                    .get(&(source, destination))
                    .map(|c| c.tx.clone());
                if let Some(tx) = tx {
                    let _ = tx.try_send(data.to_vec());
                }
            }
        }
    }
}

impl OwnedNetSender {
    /// Register this actual owner's remote before attempting UDP output, as
    /// source WriteTo does (including an OS write failure). Incoming STUN never
    /// creates this mapping. Generation and physical base are checked atomically.
    pub fn send(&self, proto: NetProto, source: SocketAddr, destination: SocketAddr, data: &[u8]) {
        let inner = &self.sender.inner;
        let mut owners = lock(&inner.transports);
        let Some(entry) = owners.get_mut(&self.transport) else {
            return;
        };
        if inner.stop.is_cancelled()
            || entry.stop.is_cancelled()
            || !Arc::ptr_eq(&entry.generation, &self.generation)
            || !entry.admitted_bases.contains(&(proto, source))
        {
            return;
        }
        match proto {
            NetProto::Udp => {
                let Some(socket) = read(&inner.udp_sockets).get(&source).cloned() else {
                    return;
                };
                let key = (proto, source, destination);
                if !entry.remotes.contains(&key) {
                    entry.remotes.push(key);
                }
                let Some(sink) = entry.sink.as_ref() else {
                    return;
                };
                write(&inner.by_remote).insert(
                    key,
                    Route {
                        transport: self.transport,
                        generation: self.generation.clone(),
                        stop: entry.stop.clone(),
                        sink: sink.clone(),
                        check_udp_base: false,
                    },
                );
                if inner.send_batched_udp(source, destination, data) {
                    return;
                }
                if let Err(e) = socket.try_send_to(data, destination)
                    && e.kind() != io::ErrorKind::WouldBlock
                {
                    tracing::debug!(%destination,error=%e,"udp send failed");
                }
            }
            NetProto::Tcp => {
                let tx = read(&inner.tcp_conns)
                    .get(&(source, destination))
                    .filter(|c| Arc::ptr_eq(&c.generation, &self.generation))
                    .map(|c| c.tx.clone());
                if let Some(tx) = tx {
                    let _ = tx.try_send(data.to_vec());
                }
            }
        }
    }
}

impl Inner {
    /// Routes a UDP datagram through the batch writer when one is installed for
    /// `source` (SPEC-0074 C21). Returns true when the batch consumed it; false
    /// when the caller should fall back to the direct socket send.
    fn send_batched_udp(&self, source: SocketAddr, destination: SocketAddr, data: &[u8]) -> bool {
        let conn = read(&self.batch).get(&source).cloned();
        match conn {
            Some(conn) => {
                conn.write(destination, data);
                true
            }
            None => false,
        }
    }
}

fn read<T>(l: &RwLock<T>) -> std::sync::RwLockReadGuard<'_, T> {
    l.read().unwrap_or_else(|p| p.into_inner())
}

fn write<T>(l: &RwLock<T>) -> std::sync::RwLockWriteGuard<'_, T> {
    l.write().unwrap_or_else(|p| p.into_inner())
}

fn lock<T>(l: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    l.lock().unwrap_or_else(|p| p.into_inner())
}

/// Discover the primary outbound IP of a family without sending traffic.
pub fn primary_ip(v6: bool) -> Option<IpAddr> {
    let (bind, probe) = if v6 {
        ("[::]:0", "[2001:4860:4860::8888]:53")
    } else {
        ("0.0.0.0:0", "8.8.8.8:53")
    };
    let s = std::net::UdpSocket::bind(bind).ok()?;
    s.connect(probe).ok()?;
    let ip = s.local_addr().ok()?.ip();
    (!ip.is_unspecified()).then_some(ip)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::{Ipv4Addr, UdpSocket as StdUdpSocket};
    use std::time::Duration;

    fn config() -> NetConfig {
        NetConfig {
            udp_enabled: true,
            udp_port: None,
            port_range: (0, 0),
            tcp_port: None,
            interfaces: vec![LocalInterface {
                bind: Ipv4Addr::LOCALHOST.into(),
                advertise: Ipv4Addr::LOCALHOST.into(),
            }],
        }
    }

    #[tokio::test]
    async fn internal_rewrite_append_uses_one_real_socket_for_external_and_internal_candidates() {
        let mut cfg = config();
        cfg.interfaces[0].advertise = "203.0.113.9".parse().unwrap();
        let net = NetRouter::start_with_address_rewrites(cfg, &[], true)
            .await
            .unwrap();
        let (tx, mut rx) = mpsc::channel(1);
        let candidates = net
            .open_transport(TransportId(991), "internal-rewrite", tx)
            .unwrap();
        assert_eq!(candidates.len(), 2);
        let external = candidates
            .iter()
            .find(|c| c.addr.ip() == "203.0.113.9".parse::<IpAddr>().unwrap())
            .unwrap();
        let internal = candidates.iter().find(|c| c.addr == c.base).unwrap();
        assert_eq!(external.base, internal.base);
        assert_eq!(external.addr.port(), internal.addr.port());
        assert_eq!(
            read(&net.inner.udp_sockets).len(),
            1,
            "aliases never double-bind"
        );
        let peer = StdUdpSocket::bind("127.0.0.1:0").unwrap();
        let username = b"internal-rewrite:peer";
        let padding = (4 - username.len() % 4) % 4;
        let length = 4 + username.len() + padding;
        let mut binding = vec![
            0,
            1,
            (length >> 8) as u8,
            length as u8,
            0x21,
            0x12,
            0xa4,
            0x42,
        ];
        binding.extend_from_slice(&[7; 12]);
        binding.extend_from_slice(&[0, 6, (username.len() >> 8) as u8, username.len() as u8]);
        binding.extend_from_slice(username);
        binding.extend(std::iter::repeat_n(0, padding));
        peer.send_to(&binding, internal.addr).unwrap();
        let packet = tokio::time::timeout(Duration::from_secs(2), rx.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(packet.bound, internal.base);
        assert_eq!(packet.data, binding);
        let released = internal.base;
        net.close_transport_and_wait(TransportId(991)).await;
        net.shutdown_and_wait().await;
        let _released = tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                match StdUdpSocket::bind(released) {
                    Ok(socket) => break socket,
                    Err(error) if error.kind() == io::ErrorKind::AddrInUse => {
                        tokio::time::sleep(Duration::from_millis(5)).await
                    }
                    Err(error) => panic!("owned alias release: {error}"),
                }
            }
        })
        .await
        .expect("internal alias closed with exactly one owned socket");
    }

    #[tokio::test]
    async fn ice_tcp_frames_route_bidirectionally_and_stop_on_shutdown() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let mut cfg = config();
        cfg.udp_enabled = false;
        cfg.tcp_port = Some(0);
        let net = NetRouter::start(cfg).await.unwrap();
        let (tx, mut rx) = mpsc::channel(1);
        let candidates = net.open_transport(TransportId(1), "test", tx).unwrap();
        assert_eq!(candidates.len(), 1);
        assert_eq!(candidates[0].proto, NetProto::Tcp);
        let addr = candidates[0].addr;
        assert_ne!(addr.port(), 0);
        let mut stream = tokio::net::TcpStream::connect(addr).await.unwrap();
        // STUN Binding/USERNAME only: routing inspection is separate from the
        // authenticated ICE state machine driven by the media engine.
        let mut binding = vec![0, 1, 0, 16, 0x21, 0x12, 0xa4, 0x42];
        binding.extend_from_slice(&[7; 12]);
        binding.extend_from_slice(&[0, 6, 0, 9]);
        binding.extend_from_slice(b"test:peer");
        binding.extend_from_slice(&[0; 3]);
        stream
            .write_all(&(binding.len() as u16).to_be_bytes())
            .await
            .unwrap();
        stream.write_all(&binding[..5]).await.unwrap();
        stream.write_all(&binding[5..]).await.unwrap();
        let packet = tokio::time::timeout(Duration::from_secs(2), rx.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(packet.data, binding);
        assert_eq!(packet.proto, NetProto::Tcp);
        net.sender()
            .send(NetProto::Tcp, addr, packet.source, b"pong");
        let mut response = [0; 6];
        tokio::time::timeout(Duration::from_secs(2), stream.read_exact(&mut response))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(&response, b"\0\x04pong");
        // Include a second connection still waiting for its first ICE frame.
        let mut idle = tokio::net::TcpStream::connect(addr).await.unwrap();
        tokio::task::yield_now().await;
        net.shutdown();
        let mut byte = [0];
        let closed = tokio::time::timeout(Duration::from_secs(2), idle.read(&mut byte))
            .await
            .unwrap();
        assert!(matches!(closed, Ok(0) | Err(_)));
    }

    #[tokio::test]
    async fn disabled_udp_opens_no_sockets_or_candidates() {
        let mut cfg = config();
        cfg.udp_enabled = false;
        // A mux port must not override the disabled transport setting.
        cfg.udp_port = Some(0);
        let net = NetRouter::start(cfg).await.unwrap();
        let (tx, _) = mpsc::channel(1);
        assert!(
            net.open_transport(TransportId(1), "test", tx)
                .unwrap()
                .is_empty()
        );
        assert!(read(&net.inner.udp_sockets).is_empty());
        net.shutdown();
    }

    #[tokio::test]
    async fn dedicated_udp_routes_packets_and_releases_port_on_close() {
        let net = NetRouter::start(config()).await.unwrap();
        let (tx, mut rx) = mpsc::channel(1);
        let candidates = net.open_transport(TransportId(1), "test", tx).unwrap();
        assert_eq!(candidates.len(), 1);
        let addr = candidates[0].addr;
        assert_ne!(addr.port(), 0);
        let peer = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        peer.send_to(b"media", addr).await.unwrap();
        let packet = tokio::time::timeout(Duration::from_secs(2), rx.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(packet.transport, TransportId(1));
        assert_eq!(packet.data, b"media");
        assert_eq!(packet.source, peer.local_addr().unwrap());
        assert_eq!(packet.destination, addr);
        net.close_transport(TransportId(1));
        // The reader holds its socket until it observes cancellation.
        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                if let Ok(socket) = StdUdpSocket::bind(addr) {
                    drop(socket);
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        net.shutdown();
    }

    #[tokio::test]
    async fn shared_bind_failure_rolls_back_all_sockets() {
        let probe = StdUdpSocket::bind("127.0.0.1:0").unwrap();
        let addr = probe.local_addr().unwrap();
        drop(probe);
        let mut cfg = config();
        cfg.udp_port = Some(addr.port());
        cfg.interfaces.push(cfg.interfaces[0].clone());
        assert!(NetRouter::start(cfg).await.is_err());
        StdUdpSocket::bind(addr).expect("failed startup must release the first socket");
    }

    #[tokio::test]
    async fn dedicated_bind_failure_registers_no_resources() {
        let probe = StdUdpSocket::bind("127.0.0.1:0").unwrap();
        let addr = probe.local_addr().unwrap();
        let mut cfg = config();
        cfg.port_range = (addr.port(), addr.port());
        let net = NetRouter::start(cfg).await.unwrap();
        let (tx, _) = mpsc::channel(1);
        assert!(net.open_transport(TransportId(1), "test", tx).is_err());
        assert!(read(&net.inner.udp_sockets).is_empty());
        assert!(read(&net.inner.by_ufrag).is_empty());
        assert!(lock(&net.inner.transports).is_empty());
        drop(probe);
        StdUdpSocket::bind(addr).expect("failed transport must release the first socket");
        net.shutdown();
    }

    #[tokio::test]
    async fn exhausted_dedicated_udp_keeps_tcp_candidate() {
        let occupied = StdUdpSocket::bind("127.0.0.1:0").unwrap();
        let port = occupied.local_addr().unwrap().port();
        let mut cfg = config();
        cfg.port_range = (port, port);
        cfg.tcp_port = Some(0);
        let net = NetRouter::start(cfg).await.unwrap();
        let (tx, _) = mpsc::channel(1);
        let id = TransportId::next();
        let candidates = net.open_transport(id, "tcp-fallback", tx).unwrap();
        assert_eq!(candidates.len(), 1);
        assert_eq!(candidates[0].proto, NetProto::Tcp);
        assert!(read(&net.inner.udp_sockets).is_empty());
        net.close_transport(id);
        assert!(!read(&net.inner.by_ufrag).contains_key("tcp-fallback"));
        net.shutdown();
    }

    #[tokio::test]
    async fn restart_registers_released_dedicated_udp_on_the_same_transport() {
        let occupied = StdUdpSocket::bind("127.0.0.1:0").unwrap();
        let addr = occupied.local_addr().unwrap();
        let mut cfg = config();
        cfg.port_range = (addr.port(), addr.port());
        cfg.tcp_port = Some(0);
        let net = NetRouter::start(cfg).await.unwrap();
        let (tx, mut rx) = mpsc::channel(1);
        let id = TransportId::next();
        let initial = net.open_transport(id, "before", tx).unwrap();
        assert_eq!(initial.len(), 1);
        assert_eq!(initial[0].proto, NetProto::Tcp);
        assert!(net.update_ufrag(id, "still-occupied", false).is_empty());

        drop(occupied);
        let recovered = net.update_ufrag(id, "released", false);
        assert_eq!(recovered.len(), 1);
        assert_eq!(recovered[0].addr, addr);
        assert_eq!(recovered[0].proto, NetProto::Udp);
        assert!(net.update_ufrag(id, "next", false).is_empty());
        assert!(!read(&net.inner.by_ufrag).contains_key("before"));
        assert!(read(&net.inner.by_ufrag).contains_key("next"));

        let peer = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        peer.send_to(b"probe", addr).await.unwrap();
        let packet = tokio::time::timeout(Duration::from_secs(2), rx.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(packet.transport, id);
        assert_eq!(packet.bound, addr);
        assert_eq!(packet.data, b"probe");
        net.sender()
            .send(NetProto::Udp, addr, packet.source, b"reply");
        let mut reply = [0; 5];
        let (len, from) = tokio::time::timeout(Duration::from_secs(2), peer.recv_from(&mut reply))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(&reply[..len], b"reply");
        assert_eq!(from, addr);

        net.close_transport(id);
        assert!(!read(&net.inner.by_ufrag).contains_key("next"));
        assert!(read(&net.inner.udp_sockets).is_empty());
        tokio::time::timeout(Duration::from_secs(2), async {
            while StdUdpSocket::bind(addr).is_err() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("the restarted transport must release the recovered socket");
        net.shutdown();
    }

    #[tokio::test]
    async fn dedicated_restart_replaces_host_socket_and_keeps_it_on_exhaustion() {
        let (initial, spare) = (0..64)
            .find_map(|_| {
                let initial = StdUdpSocket::bind("127.0.0.1:0").ok()?;
                let next = initial.local_addr().ok()?.port().checked_add(1)?;
                let spare = StdUdpSocket::bind((Ipv4Addr::LOCALHOST, next)).ok()?;
                Some((initial, spare))
            })
            .expect("two consecutive loopback UDP ports");
        let old = initial.local_addr().unwrap();
        let replacement = spare.local_addr().unwrap();
        let mut cfg = config();
        cfg.port_range = (old.port(), replacement.port());
        let net = NetRouter::start(cfg).await.unwrap();
        let (tx, mut rx) = mpsc::channel(2);
        let id = TransportId::next();
        drop(initial);
        assert_eq!(net.open_transport(id, "before", tx).unwrap()[0].base, old);

        assert_eq!(net.update_ufrag(id, "full", true)[0].base, old);
        drop(spare);
        assert_eq!(net.update_ufrag(id, "spare", true)[0].base, replacement);
        assert!(!read(&net.inner.udp_sockets).contains_key(&old));
        let retired = tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                if let Ok(socket) = StdUdpSocket::bind(old) {
                    break socket;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("retired host port must rebind while the transport lives");
        assert_eq!(
            net.update_ufrag(id, "full-again", true)[0].base,
            replacement
        );

        let peer = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        peer.send_to(b"replacement", replacement).await.unwrap();
        let packet = tokio::time::timeout(Duration::from_secs(2), rx.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!((packet.transport, packet.bound), (id, replacement));
        assert_eq!(packet.data, b"replacement");
        drop(retired);
        net.close_transport(id);
        net.shutdown();
    }

    #[tokio::test]
    async fn explicit_append_retries_only_missing_dedicated_host_binding() {
        let (available, occupied) = (0..64)
            .find_map(|_| {
                let available = StdUdpSocket::bind("127.0.0.1:0").unwrap();
                let next = available.local_addr().unwrap().port().checked_add(1)?;
                let occupied = StdUdpSocket::bind((Ipv4Addr::LOCALHOST, next)).ok()?;
                Some((available, occupied))
            })
            .expect("two consecutive loopback UDP ports");
        let original = available.local_addr().unwrap();
        let appended = occupied.local_addr().unwrap();
        let mut cfg = config();
        cfg.port_range = (original.port(), appended.port());
        let ip = NodeIp {
            v4: Some(Ipv4Addr::LOCALHOST),
            v6: None,
        };
        let net = NetRouter::start_with_configured_ports(cfg, &[], None, None, true, Some(&ip))
            .await
            .unwrap();
        let (tx, mut rx) = mpsc::channel(2);
        let id = TransportId::next();
        drop(available);
        let initial = net.open_transport(id, "before", tx).unwrap();
        assert_eq!(initial.len(), 1);
        assert_eq!(initial[0].addr, original);
        assert!(net.update_ufrag(id, "still-occupied", false).is_empty());

        drop(occupied);
        let recovered = net.update_ufrag(id, "released", false);
        assert_eq!(recovered.len(), 1);
        assert_eq!(recovered[0].addr, appended);
        assert_eq!(recovered[0].base, appended);
        assert!(net.update_ufrag(id, "next", false).is_empty());

        let peer = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        for addr in [original, appended] {
            peer.send_to(b"owned", addr).await.unwrap();
            let packet = tokio::time::timeout(Duration::from_secs(2), rx.recv())
                .await
                .unwrap()
                .unwrap();
            assert_eq!(packet.transport, id);
            assert_eq!(packet.bound, addr);
        }

        net.close_transport(id);
        assert!(read(&net.inner.udp_sockets).is_empty());
        tokio::time::timeout(Duration::from_secs(2), async {
            for addr in [original, appended] {
                while StdUdpSocket::bind(addr).is_err() {
                    tokio::task::yield_now().await;
                }
            }
        })
        .await
        .expect("both dedicated bindings must be released");
        net.shutdown();
    }

    #[tokio::test]
    async fn shared_ephemeral_candidate_uses_actual_bound_port() {
        let mut cfg = config();
        cfg.udp_port = Some(0);
        let net = NetRouter::start(cfg).await.unwrap();
        let (tx, _) = mpsc::channel(1);
        let candidates = net.open_transport(TransportId(1), "test", tx).unwrap();
        assert_ne!(candidates[0].addr.port(), 0);
        assert!(read(&net.inner.udp_sockets).contains_key(&candidates[0].addr));
        net.shutdown();
    }
}

#[cfg(test)]
mod socket_identity_tests;

#[cfg(test)]
mod owner_registration_tests;

#[cfg(test)]
mod owner_baseline_red;

// Test-only observation of existing task ownership, not a production counter.
#[cfg(test)]
impl NetRouter {
    pub(crate) fn tcp_owner_test_counts(&self) -> (usize, usize) {
        (
            lock(&self.inner.tcp_tasks).len(),
            read(&self.inner.tcp_conns).len(),
        )
    }
}

#[cfg(test)]
mod receive_clock_tests;
