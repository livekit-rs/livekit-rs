//! TURN server: configuration, listeners, request handling and lifecycle.

use std::fmt;
use std::io;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use bytes::Bytes;
use socket2::{Domain, Protocol, Socket, Type};
use tokio::net::{TcpListener, UdpSocket};
use tokio::sync::{mpsc, watch};
use tokio::task::JoinHandle;

use crate::allocation::{
    Allocation, AllocationManager, BindResult, ClientAddr, ClientSink, FiveTuple, ReserveError,
    Transport,
};
use crate::auth::{AuthHandler, AuthRequest, UnixAuthRequest};
use crate::nonce::NonceManager;
use crate::policy::{Cidr, LocalRelayPolicy, PeerPolicy, unmap};
use crate::stun::{self, ChannelData, Class, Message, MessageBuilder, Method, attr};
use crate::tcp::{self, StreamAcceptor};

/// SOFTWARE attribute value.
const SOFTWARE_NAME: &str = "livekit-rs rtc-turn";
/// Relay port allocation attempts before answering 508 (LiveKit `allocateRetries`).
const ALLOCATE_RETRIES: usize = 50;
/// Reaper period for expired allocations / permissions / channels.
const REAP_INTERVAL: Duration = Duration::from_secs(5);
/// UDP listener receive buffer (max datagram).
const UDP_RECV_BUF: usize = 65_536;

/// A client-facing listener.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ListenerSpec {
    Udp(SocketAddr),
    /// Plain TCP stream framing (used for turn.external_tls where an LB terminates TLS, and for TLS after a handshake).
    Tcp(SocketAddr),
}

/// TURN server configuration.
#[derive(Clone)]
pub struct TurnServerConfig {
    /// Authentication realm, `"livekit"` by default.
    pub realm: String,
    pub listeners: Vec<ListenerSpec>,
    /// Applied to all Tcp listeners when Some.
    pub tls: Option<Arc<dyn StreamAcceptor>>,
    /// Advertised IPv4 relay address (node IP). Falls back to
    /// `relay_bind_ipv4` when that is a specific address.
    pub relay_ipv4: Option<Ipv4Addr>,
    /// Advertised IPv6 relay address.
    pub relay_ipv6: Option<Ipv6Addr>,
    /// Where IPv4 relay sockets bind (must be an IPv4 address).
    pub relay_bind_ipv4: IpAddr,
    /// Where IPv6 relay sockets bind (must be an IPv6 address). Defaults to
    /// `[::]` when `relay_ipv6` is set.
    pub relay_bind_ipv6: Option<IpAddr>,
    /// Inclusive relay port range; (0, 0) = OS ephemeral ports.
    pub relay_port_range: (u16, u16),
    /// Concurrent allocations per user id; 0 = unlimited.
    pub per_user_allocation_limit: u32,
    /// Restricted peer ranges that are nevertheless allowed.
    pub allow_restricted_peer_cidrs: Vec<(IpAddr, u8)>,
    /// Peer ranges that are always denied.
    pub deny_peer_cidrs: Vec<(IpAddr, u8)>,
    /// PROXY protocol (v1/v2) trusted proxy CIDRs. When `Some`, every TCP
    /// connection's direct peer address is checked before the byte stream is
    /// handed to TLS: a trusted peer MUST send a valid PROXY header (parsed
    /// first, so the TURN client address becomes the forwarded source) and any
    /// other peer is closed, mirroring LiveKit's `proxyProtocolPolicy`.
    /// `Some(vec![])` is treated as no trusted proxy (all connections refused).
    pub proxy_trusted_cidrs: Option<Vec<(IpAddr, u8)>>,
    /// Hard cap on allocations across all users.
    pub max_allocations: usize,
    pub default_lifetime: Duration,
    pub max_lifetime: Duration,
}

impl Default for TurnServerConfig {
    fn default() -> Self {
        TurnServerConfig {
            realm: "livekit".to_string(),
            listeners: Vec::new(),
            tls: None,
            relay_ipv4: None,
            relay_ipv6: None,
            relay_bind_ipv4: IpAddr::V4(Ipv4Addr::UNSPECIFIED),
            relay_bind_ipv6: None,
            relay_port_range: (30_000, 40_000),
            per_user_allocation_limit: 12,
            allow_restricted_peer_cidrs: Vec::new(),
            deny_peer_cidrs: Vec::new(),
            proxy_trusted_cidrs: None,
            max_allocations: 10_000,
            default_lifetime: Duration::from_secs(600),
            max_lifetime: Duration::from_secs(3600),
        }
    }
}

impl fmt::Debug for TurnServerConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TurnServerConfig")
            .field("realm", &self.realm)
            .field("listeners", &self.listeners)
            .field("tls", &self.tls.is_some())
            .field("relay_ipv4", &self.relay_ipv4)
            .field("relay_ipv6", &self.relay_ipv6)
            .field("relay_bind_ipv4", &self.relay_bind_ipv4)
            .field("relay_bind_ipv6", &self.relay_bind_ipv6)
            .field("relay_port_range", &self.relay_port_range)
            .field("per_user_allocation_limit", &self.per_user_allocation_limit)
            .field(
                "allow_restricted_peer_cidrs",
                &self.allow_restricted_peer_cidrs,
            )
            .field("deny_peer_cidrs", &self.deny_peer_cidrs)
            .field(
                "proxy_trusted_cidrs",
                &self.proxy_trusted_cidrs.as_ref().map(Vec::len),
            )
            .field("max_allocations", &self.max_allocations)
            .field("default_lifetime", &self.default_lifetime)
            .field("max_lifetime", &self.max_lifetime)
            .finish()
    }
}

/// Errors starting the server.
#[derive(Debug, thiserror::Error)]
pub enum TurnError {
    #[error("invalid TURN configuration: {0}")]
    InvalidConfig(String),
    #[error("could not bind TURN listener {addr}: {source}")]
    Bind {
        addr: SocketAddr,
        #[source]
        source: io::Error,
    },
    #[error("TURN I/O error: {0}")]
    Io(#[from] io::Error),
}

#[derive(Debug, Clone, Copy)]
struct RelayFamily {
    bind: IpAddr,
    advertised: IpAddr,
}

/// Where a response/relay sink for a new allocation comes from.
#[derive(Clone, Copy)]
pub(crate) enum SinkSource<'a> {
    Udp(&'a Arc<UdpSocket>, SocketAddr),
    Tcp(&'a mpsc::Sender<Bytes>, IpAddr),
}

impl SinkSource<'_> {
    fn listener_ip(self, server: SocketAddr) -> IpAddr {
        // UDP's server address already comes from the listener. TCP's
        // accepted stream instead has a concrete address on wildcard binds.
        match self {
            Self::Udp(..) => server.ip(),
            Self::Tcp(_, listener) => listener,
        }
    }

    fn to_sink(self) -> ClientSink {
        match self {
            SinkSource::Udp(socket, client) => ClientSink::Udp {
                socket: socket.clone(),
                client,
            },
            SinkSource::Tcp(tx, _) => ClientSink::Tcp { tx: tx.clone() },
        }
    }
}

struct Authed {
    user_id: String,
    key: [u8; 16],
}

/// Shared server state.
pub(crate) struct ServerCtx {
    realm: String,
    auth: Arc<dyn AuthHandler>,
    nonces: NonceManager,
    pub(crate) allocs: AllocationManager,
    policy: PeerPolicy,
    local_relay_policy: Option<LocalRelayPolicy>,
    relay_v4: Option<RelayFamily>,
    relay_v6: Option<RelayFamily>,
    relay_bind_from_listener: bool,
    port_range: (u16, u16),
    default_lifetime: Duration,
    max_lifetime: Duration,
    relayed_bytes: Arc<AtomicU64>,
}

fn unmap_sa(sa: SocketAddr) -> SocketAddr {
    SocketAddr::new(unmap(sa.ip()), sa.port())
}

/// Creates a non-blocking UDP socket (IPv6 sockets are v6-only so relay
/// families never mix) and registers it with Tokio.
fn bind_udp(addr: SocketAddr) -> io::Result<UdpSocket> {
    let domain = if addr.is_ipv4() {
        Domain::IPV4
    } else {
        Domain::IPV6
    };
    let sock = Socket::new(domain, Type::DGRAM, Some(Protocol::UDP))?;
    if addr.is_ipv6() {
        sock.set_only_v6(true)?;
    }
    sock.set_nonblocking(true)?;
    sock.bind(&addr.into())?;
    UdpSocket::from_std(sock.into())
}

impl ServerCtx {
    /// Handles one datagram / stream frame from a client. Returns the
    /// response to send back, if any. Never blocks.
    pub(crate) fn handle_packet(
        &self,
        buf: &[u8],
        ft: &FiveTuple,
        sink: SinkSource<'_>,
    ) -> Option<Vec<u8>> {
        if stun::is_channel_data(buf) {
            self.handle_channel_data(buf, ft);
            return None;
        }
        let msg = match Message::decode_client(buf) {
            Ok(m) => m,
            Err(e) => {
                tracing::trace!(error = %e, client = %ft.client, "dropping invalid STUN packet");
                return None;
            }
        };
        match msg.class() {
            Class::Request if msg.method() == Method::Binding => ft
                .client
                .inet()
                .map(|addr| self.binding_response(&msg, addr)),
            Class::Request => self.handle_request(&msg, ft, sink),
            Class::Indication => {
                if msg.method() == Method::Send {
                    let unknown = msg.unknown_comprehension_required();
                    if !unknown.is_empty() {
                        return Some(self.unknown_attributes_response(&msg, &unknown, false));
                    }
                    self.handle_send(&msg, ft);
                }
                None
            }
            Class::Success | Class::Error => None,
        }
    }

    fn error_response(&self, req: &Message, code: u16, key: Option<&[u8]>) -> Vec<u8> {
        let mut b = MessageBuilder::response_to(req, Class::Error);
        b.add_error(code)
            .add(attr::SOFTWARE, SOFTWARE_NAME.as_bytes());
        b.build(key, true)
    }

    /// 401 / 438 with a fresh REALM + NONCE, unauthenticated.
    fn challenge(&self, req: &Message, code: u16) -> Vec<u8> {
        let nonce = self.nonces.generate();
        let mut b = MessageBuilder::response_to(req, Class::Error);
        b.add_error(code)
            .add(attr::REALM, self.realm.as_bytes())
            .add(attr::NONCE, nonce.as_bytes())
            .add(attr::SOFTWARE, SOFTWARE_NAME.as_bytes());
        b.build(None, true)
    }

    fn unix_error_response(&self, req: &Message, code: u16) -> Vec<u8> {
        let mut b = MessageBuilder::response_to(req, Class::Error);
        b.add_error(code);
        if matches!(code, 401 | 438) {
            let nonce = self.nonces.generate();
            b.add(attr::NONCE, nonce.as_bytes())
                .add(attr::REALM, self.realm.as_bytes());
        }
        b.build(None, false)
    }

    fn control_error_response(&self, req: &Message, code: u16, key: Option<&[u8]>) -> Vec<u8> {
        let mut response = MessageBuilder::response_to(req, Class::Error);
        response.add(attr::ERROR_CODE, &attr::encode_error_code(code, ""));
        response.build(key, false)
    }

    fn unknown_attributes_response(&self, req: &Message, unknown: &[u16], legacy: bool) -> Vec<u8> {
        let mut response = MessageBuilder::response_to(req, Class::Error);
        response.add_error(420).add(
            attr::UNKNOWN_ATTRIBUTES,
            &attr::encode_unknown_attributes(unknown),
        );
        if legacy {
            response.add(attr::SOFTWARE, SOFTWARE_NAME.as_bytes());
        }
        response.build(None, legacy)
    }

    fn handle_request(
        &self,
        msg: &Message,
        ft: &FiveTuple,
        sink: SinkSource<'_>,
    ) -> Option<Vec<u8>> {
        if msg.method() == Method::Send {
            return None;
        }
        if msg.method() == Method::Data {
            return Some(self.error_response(msg, 400, None));
        }
        let unknown = msg.unknown_comprehension_required();
        if !unknown.is_empty() {
            let legacy = !matches!(msg.method(), Method::CreatePermission | Method::ChannelBind);
            return Some(self.unknown_attributes_response(msg, &unknown, legacy));
        }
        let authed = match self.authenticate(msg, &ft.client) {
            Ok(a) => a,
            Err(resp) => return Some(resp),
        };
        Some(match msg.method() {
            Method::Allocate => self.handle_allocate(msg, ft, sink, &authed),
            Method::Refresh => return self.handle_refresh(msg, ft, &authed),
            Method::CreatePermission => return self.handle_create_permission(msg, ft, &authed),
            Method::ChannelBind => return self.handle_channel_bind(msg, ft, &authed),
            Method::Binding | Method::Send | Method::Data => {
                self.error_response(msg, 400, Some(&authed.key))
            }
        })
    }

    fn binding_response(&self, msg: &Message, src: SocketAddr) -> Vec<u8> {
        let mut b = MessageBuilder::response_to(msg, Class::Success);
        b.add_xor_address(attr::XOR_MAPPED_ADDRESS, unmap_sa(src))
            .add(attr::SOFTWARE, SOFTWARE_NAME.as_bytes());
        b.build(None, true)
    }

    /// Long-term credential check (RFC 8489 §9.2.4).
    fn authenticate(&self, msg: &Message, src: &ClientAddr) -> Result<Authed, Vec<u8>> {
        let unix = src.inet().is_none();
        let control = matches!(msg.method(), Method::CreatePermission | Method::ChannelBind);
        let failure = |code| {
            if control {
                let mut response = MessageBuilder::response_to(msg, Class::Error);
                response.add(attr::ERROR_CODE, &attr::encode_error_code(code, ""));
                if matches!(code, 401 | 438) {
                    let nonce = self.nonces.generate();
                    response
                        .add(attr::NONCE, nonce.as_bytes())
                        .add(attr::REALM, self.realm.as_bytes());
                }
                response.build(None, false)
            } else if unix {
                self.unix_error_response(msg, code)
            } else if code == 400 {
                self.error_response(msg, code, None)
            } else {
                self.challenge(msg, code)
            }
        };
        if !msg.has_integrity() {
            return Err(failure(401));
        }
        let (Some(Ok(username)), Some(Ok(realm)), Some(nonce)) =
            (msg.username(), msg.realm(), msg.nonce())
        else {
            return Err(failure(400));
        };
        if !self.nonces.validate(nonce) {
            return Err(failure(438));
        }
        if realm != self.realm {
            return Err(failure(if control { 400 } else { 401 }));
        }
        let credentials = match src {
            ClientAddr::Inet(src) => self.auth.authenticate(&AuthRequest {
                username,
                realm,
                method: msg.method(),
                src: *src,
            }),
            ClientAddr::Unix { address, .. } => self.auth.authenticate_unix(&UnixAuthRequest {
                username,
                realm,
                method: msg.method(),
                src: address,
            }),
        };
        let Some((user_id, key)) = credentials else {
            tracing::debug!(%src, method = %msg.method(), username, "TURN authentication rejected");
            return Err(failure(if unix || control { 400 } else { 401 }));
        };
        if !msg.verify_integrity(&key) {
            tracing::debug!(%src, method = %msg.method(), username, "TURN message integrity check failed");
            return Err(failure(if unix || control { 400 } else { 401 }));
        }
        Ok(Authed { user_id, key })
    }

    /// Pion uses an explicit LIFETIME only when it is below the maximum.
    fn allocation_lifetime(&self, msg: &Message) -> Duration {
        msg.lifetime()
            .and_then(Result::ok)
            .map(|seconds| Duration::from_secs(u64::from(seconds)))
            .filter(|duration| *duration < self.max_lifetime)
            .unwrap_or(self.default_lifetime)
    }

    fn relay_for_family(&self, v4: bool) -> Option<RelayFamily> {
        if v4 { self.relay_v4 } else { self.relay_v6 }
    }

    /// Default relay family when REQUESTED-ADDRESS-FAMILY is absent: IPv6 for
    /// a listener bound to an IPv6 address (as LiveKit picks the node IP by
    /// bind address family), otherwise IPv4 (RFC 8656 default).
    fn default_family_v4(&self, server: SocketAddr) -> bool {
        let listener_v6 = unmap(server.ip()).is_ipv6();
        if listener_v6 && self.relay_v6.is_some() {
            return false;
        }
        self.relay_v4.is_some()
    }

    fn bind_relay(&self, ip: IpAddr, even: bool) -> io::Result<UdpSocket> {
        let (lo, hi) = self.port_range;
        let mut last_err = None;
        for _ in 0..ALLOCATE_RETRIES {
            let port = if lo == 0 && hi == 0 {
                0
            } else {
                let mut p: u16 = rand::random_range(lo..=hi);
                if even && p % 2 == 1 {
                    p = if p > lo {
                        p - 1
                    } else {
                        p.saturating_add(1).min(hi)
                    };
                }
                p
            };
            match bind_udp(SocketAddr::new(ip, port)) {
                Ok(s) => {
                    if even && s.local_addr()?.port() % 2 != 0 {
                        continue;
                    }
                    return Ok(s);
                }
                Err(e) => last_err = Some(e),
            }
        }
        Err(last_err
            .unwrap_or_else(|| io::Error::new(io::ErrorKind::AddrInUse, "no relay port available")))
    }

    fn handle_allocate(
        &self,
        msg: &Message,
        ft: &FiveTuple,
        sink: SinkSource<'_>,
        authed: &Authed,
    ) -> Vec<u8> {
        let key = Some(&authed.key[..]);
        let client = ft.client.inet();
        let error = |code| {
            if client.is_some() {
                self.error_response(msg, code, key)
            } else {
                self.unix_error_response(msg, code)
            }
        };
        if let Some(existing) = self.allocs.get(ft) {
            if let Some(resp) = existing.state.cached_response(msg.transaction_id()) {
                return resp;
            }
            return error(437);
        }
        match msg.requested_transport() {
            None | Some(Err(_)) => return error(400),
            Some(Ok(p)) if p != attr::PROTO_UDP => {
                tracing::debug!(client = %ft.client, protocol = p, "rejecting non-UDP relay allocation");
                return error(442);
            }
            Some(Ok(_)) => {}
        }
        if msg.contains(attr::RESERVATION_TOKEN) {
            // Reservations are not supported: the token can never be valid.
            let conflicting = msg.contains(attr::EVEN_PORT)
                || msg.contains(attr::REQUESTED_ADDRESS_FAMILY)
                || msg.contains(attr::ADDITIONAL_ADDRESS_FAMILY);
            return error(if conflicting { 400 } else { 508 });
        }
        let even = match msg.even_port() {
            None => false,
            Some(Err(_)) => return error(400),
            // R bit: a reservation of the next port is requested; unsupported.
            Some(Ok(true)) => return error(508),
            Some(Ok(false)) => true,
        };
        let v4 = match msg.requested_address_family() {
            // A Unix destination has no IP family. The Go handler defaults to IPv4.
            None => client.is_none() || self.default_family_v4(ft.server),
            Some(Ok(attr::FAMILY_IPV4)) => true,
            Some(Ok(attr::FAMILY_IPV6)) => false,
            Some(Ok(_)) => return error(440),
            Some(Err(_)) => return error(400),
        };
        let listener_bind = unmap(sink.listener_ip(ft.server));
        let relay_family_v4 = if self.relay_bind_from_listener {
            listener_bind.is_ipv4()
        } else {
            v4
        };
        let Some(mut relay) = self.relay_for_family(relay_family_v4) else {
            return error(440);
        };
        if self.relay_bind_from_listener {
            relay.bind = listener_bind;
        }
        let lifetime = self.allocation_lifetime(msg);
        if lifetime.is_zero() && client.is_some() {
            return error(508);
        }

        match self.allocs.reserve(&authed.user_id) {
            Ok(()) => {}
            Err(ReserveError::UserQuota) => {
                tracing::info!(user_id = %authed.user_id, client = %ft.client, "TURN per-user allocation quota reached");
                return error(486);
            }
            Err(ReserveError::Capacity) => {
                tracing::warn!(client = %ft.client, "TURN allocation capacity reached");
                return error(508);
            }
        }
        if relay_family_v4 != v4 {
            // Pion tries the requested udp4/udp6 with the listener's
            // configured address after checking quota. The bind fails (508).
            self.allocs.release(&authed.user_id);
            return error(508);
        }
        let socket = match self.bind_relay(relay.bind, even) {
            Ok(s) => s,
            Err(e) => {
                self.allocs.release(&authed.user_id);
                tracing::warn!(error = %e, bind = %relay.bind, "could not allocate TURN relay port");
                return error(508);
            }
        };
        let port = match socket.local_addr() {
            Ok(a) => a.port(),
            Err(e) => {
                self.allocs.release(&authed.user_id);
                tracing::warn!(error = %e, "TURN relay socket has no local address");
                return error(500);
            }
        };
        let relay_addr = SocketAddr::new(relay.advertised, port);

        let alloc = Allocation::new(
            ft.clone(),
            authed.user_id.clone(),
            socket,
            relay_addr,
            sink.to_sink(),
            lifetime,
            self.relayed_bytes.clone(),
        );
        let response = if let Some(client) = client {
            let mut b = MessageBuilder::response_to(msg, Class::Success);
            b.add_xor_address(attr::XOR_RELAYED_ADDRESS, relay_addr)
                .add_xor_address(attr::XOR_MAPPED_ADDRESS, unmap_sa(client))
                .add_u32(attr::LIFETIME, lifetime.as_secs() as u32)
                .add(attr::SOFTWARE, SOFTWARE_NAME.as_bytes());
            let response = b.build(key, true);
            alloc
                .state
                .set_cached_response(*msg.transaction_id(), response.clone());
            response
        } else {
            // Pion creates the relay before it discovers that it cannot encode
            // XOR-MAPPED-ADDRESS for Unix. Retain the relay for this connection
            // only, without a cached Allocate success.
            error(400)
        };
        self.allocs.insert(alloc);
        tracing::info!(
            user_id = %authed.user_id,
            client = %ft.client,
            transport = ?ft.transport,
            relay = %relay_addr,
            lifetime_secs = lifetime.as_secs(),
            "TURN allocation created"
        );
        response
    }

    fn handle_refresh(&self, msg: &Message, ft: &FiveTuple, authed: &Authed) -> Option<Vec<u8>> {
        let key = Some(&authed.key[..]);
        // A Refresh without a live allocation produces no wire response.
        let alloc = self.allocs.get(ft)?;
        // The participant owns the relay across TURN credential rotations.
        if alloc.state.user_id != authed.user_id {
            return None;
        }
        if let Some(fam) = msg.requested_address_family() {
            let code = match fam {
                Ok(attr::FAMILY_IPV4) if alloc.state.relay_family_is_v4() => None,
                Ok(attr::FAMILY_IPV6) if !alloc.state.relay_family_is_v4() => None,
                Ok(_) => Some(443),
                Err(_) => Some(400),
            };
            if let Some(code) = code {
                return Some(self.control_error_response(msg, code, None));
            }
        }
        let lifetime = self.allocation_lifetime(msg);
        if !self.allocs.refresh(&alloc, lifetime) {
            return None;
        }
        if lifetime.is_zero() {
            tracing::info!(user_id = %authed.user_id, client = %ft.client, "TURN allocation deleted by client");
        }

        let mut b = MessageBuilder::response_to(msg, Class::Success);
        b.add_u32(attr::LIFETIME, lifetime.as_secs() as u32);
        Some(b.build(key, false))
    }

    /// Checks family and policy for a peer; Err holds the error code.
    fn check_peer(&self, peer: SocketAddr, relay_v4: bool) -> Result<(), u16> {
        let ip = unmap(peer.ip());
        if ip.is_ipv4() != relay_v4 {
            return Err(443);
        }
        if !self.policy.permits(ip) {
            tracing::debug!(peer = %ip, "TURN peer address denied by policy");
            return Err(403);
        }
        Ok(())
    }

    fn handle_create_permission(
        &self,
        msg: &Message,
        ft: &FiveTuple,
        authed: &Authed,
    ) -> Option<Vec<u8>> {
        let key = Some(&authed.key[..]);
        let alloc = self.allocs.get(ft)?;
        if alloc.state.user_id != authed.user_id {
            return None;
        }
        let error = |code| Some(self.control_error_response(msg, code, key));
        let mut peers = msg.get_all(attr::XOR_PEER_ADDRESS).peekable();
        if peers.peek().is_none() {
            return error(400);
        }
        // The pinned server retains permissions granted before a later peer fails.
        for raw in peers {
            let Ok(peer) = attr::decode_turn_peer_address(raw, msg.transaction_id()) else {
                return error(400);
            };
            if let Err(code) = self.check_peer(peer, alloc.state.relay_family_is_v4()) {
                return error(code);
            }
            if !alloc.state.add_permission(peer.ip()) {
                return error(508);
            }
        }
        Some(MessageBuilder::response_to(msg, Class::Success).build(key, false))
    }

    fn handle_channel_bind(
        &self,
        msg: &Message,
        ft: &FiveTuple,
        authed: &Authed,
    ) -> Option<Vec<u8>> {
        let alloc = self.allocs.get(ft)?;
        if alloc.state.user_id != authed.user_id {
            return None;
        }
        let error = |code| Some(self.control_error_response(msg, code, None));
        let number = match msg.channel_number() {
            Some(Ok(n)) => n,
            _ => return error(400),
        };
        let Some(raw) = msg.get(attr::XOR_PEER_ADDRESS) else {
            return error(400);
        };
        let Ok(peer) = attr::decode_turn_peer_address(raw, msg.transaction_id()) else {
            return error(400);
        };
        if let Err(code) = self.check_peer(peer, alloc.state.relay_family_is_v4()) {
            return error(if code == 403 { 401 } else { code });
        }
        let peer = SocketAddr::new(unmap(peer.ip()), peer.port());
        match alloc.state.bind_channel(number, peer) {
            BindResult::Ok => {}
            BindResult::Conflict => return error(400),
            BindResult::Full => return error(508),
        }
        Some(MessageBuilder::response_to(msg, Class::Success).build(Some(&authed.key), false))
    }

    fn handle_send(&self, msg: &Message, ft: &FiveTuple) {
        let Some(alloc) = self.allocs.get(ft) else {
            return;
        };
        let (Some(raw), Some(data)) = (msg.get(attr::XOR_PEER_ADDRESS), msg.data()) else {
            return;
        };
        let Ok(peer) = attr::decode_turn_peer_address(raw, msg.transaction_id()) else {
            return;
        };
        let peer = SocketAddr::new(unmap(peer.ip()), peer.port());
        if peer.is_ipv4() != alloc.state.relay_family_is_v4() {
            return;
        }
        if !alloc.state.has_permission(peer.ip(), Instant::now()) {
            tracing::trace!(%peer, "dropping Send indication without permission");
            return;
        }
        self.relay_to_peer(&alloc, data, peer);
    }

    fn handle_channel_data(&self, buf: &[u8], ft: &FiveTuple) {
        let Ok(cd) = ChannelData::decode(buf) else {
            return;
        };
        let Some(alloc) = self.allocs.get(ft) else {
            return;
        };
        let Some(peer) = alloc.state.channel_peer(cd.number, Instant::now()) else {
            return;
        };
        self.relay_to_peer(&alloc, cd.data, peer);
    }

    fn relay_to_peer(&self, alloc: &Allocation, data: &[u8], peer: SocketAddr) {
        if self
            .local_relay_policy
            .as_ref()
            .is_some_and(|policy| !policy.allows(peer))
        {
            return;
        }
        alloc.state.send_to_peer(data, peer);
    }

    /// Removes the allocation bound to a closed stream connection.
    pub(crate) fn connection_closed(&self, ft: &FiveTuple) {
        if let Some(a) = self.allocs.remove(ft) {
            tracing::info!(user_id = %a.state.user_id, client = %ft.client, "TURN allocation deleted: connection closed");
        }
    }
}

async fn udp_listener(
    ctx: Arc<ServerCtx>,
    socket: Arc<UdpSocket>,
    mut shutdown: watch::Receiver<bool>,
) {
    let local = match socket.local_addr() {
        Ok(a) => a,
        Err(e) => {
            tracing::error!(error = %e, "TURN UDP listener has no local address");
            return;
        }
    };
    let mut buf = vec![0u8; UDP_RECV_BUF];
    loop {
        tokio::select! {
            _ = shutdown.changed() => break,
            r = socket.recv_from(&mut buf) => {
                let (n, src) = match r {
                    Ok(v) => v,
                    Err(e) => {
                        // e.g. ICMP port unreachable surfaced as ECONNRESET on Windows
                        tracing::trace!(error = %e, "TURN UDP recv error");
                        continue;
                    }
                };
                let ft = FiveTuple { client: ClientAddr::Inet(src), server: local, transport: Transport::Udp };
                if let Some(resp) = ctx.handle_packet(&buf[..n], &ft, SinkSource::Udp(&socket, src))
                    && let Err(e) = socket.send_to(&resp, src).await {
                        tracing::trace!(error = %e, client = %src, "TURN UDP response send failed");
                    }
            }
        }
    }
}

async fn reaper(ctx: Arc<ServerCtx>, mut shutdown: watch::Receiver<bool>) {
    let mut interval = tokio::time::interval(REAP_INTERVAL);
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        let next_deadline = ctx.allocs.next_deadline();
        tokio::select! {
            _ = shutdown.changed() => break,
            _ = ctx.allocs.wait_for_deadline_change() => {},
            _ = interval.tick() => ctx.allocs.prune_permissions_and_channels(Instant::now()),
            () = async {
                if let Some(deadline) = next_deadline {
                    tokio::time::sleep_until(deadline.into()).await;
                }
            }, if next_deadline.is_some() => {
                let removed = ctx.allocs.reap(Instant::now());
                for a in &removed {
                    tracing::info!(
                        user_id = %a.state.user_id,
                        client = %a.state.five_tuple.client,
                        relay = %a.state.relay_addr,
                        "TURN allocation expired"
                    );
                }
            }
        }
    }
}

/// Entry point.
pub struct TurnServer;

impl TurnServer {
    /// Validates the config, binds every listener and spawns the server tasks.
    pub async fn start(
        config: TurnServerConfig,
        auth: Arc<dyn AuthHandler>,
    ) -> Result<TurnHandle, TurnError> {
        Self::start_with_local_relay_policy(config, auth, None).await
    }

    /// Starts the server with an optional outbound policy for an embedded SFU.
    pub async fn start_with_local_relay_policy(
        config: TurnServerConfig,
        auth: Arc<dyn AuthHandler>,
        local_relay_policy: Option<LocalRelayPolicy>,
    ) -> Result<TurnHandle, TurnError> {
        Self::start_inner(config, auth, local_relay_policy, false).await
    }

    /// Starts embedded TURN with one relay bind per configured listener.
    /// A wildcard remains wildcard, even after a TCP connection is accepted.
    pub async fn start_embedded(
        config: TurnServerConfig,
        auth: Arc<dyn AuthHandler>,
        local_relay_policy: LocalRelayPolicy,
    ) -> Result<TurnHandle, TurnError> {
        Self::start_inner(config, auth, Some(local_relay_policy), true).await
    }

    async fn start_inner(
        config: TurnServerConfig,
        auth: Arc<dyn AuthHandler>,
        local_relay_policy: Option<LocalRelayPolicy>,
        relay_bind_from_listener: bool,
    ) -> Result<TurnHandle, TurnError> {
        let invalid =
            |s: String| -> Result<TurnHandle, TurnError> { Err(TurnError::InvalidConfig(s)) };
        if config.realm.is_empty() {
            return invalid("realm must not be empty".into());
        }
        if config.listeners.is_empty() {
            return invalid("at least one listener is required".into());
        }
        let (lo, hi) = config.relay_port_range;
        if !((lo == 0 && hi == 0) || (lo > 0 && lo <= hi)) {
            return invalid(format!("invalid relay port range {lo}-{hi}"));
        }
        if config.max_allocations == 0 {
            return invalid("max_allocations must be > 0".into());
        }
        if config.default_lifetime.is_zero() || config.default_lifetime > config.max_lifetime {
            return invalid("default_lifetime must be > 0 and <= max_lifetime".into());
        }
        if !config.relay_bind_ipv4.is_ipv4() {
            return invalid(format!(
                "relay_bind_ipv4 {} is not an IPv4 address",
                config.relay_bind_ipv4
            ));
        }
        if let Some(b) = config.relay_bind_ipv6
            && !b.is_ipv6()
        {
            return invalid(format!("relay_bind_ipv6 {b} is not an IPv6 address"));
        }
        let parse = |field: &str, cidrs: &[(IpAddr, u8)]| -> Result<Vec<Cidr>, TurnError> {
            cidrs
                .iter()
                .map(|(ip, p)| {
                    Cidr::new(*ip, *p).ok_or_else(|| {
                        TurnError::InvalidConfig(format!("invalid CIDR {ip}/{p} in {field}"))
                    })
                })
                .collect()
        };
        let policy = PeerPolicy::new(
            parse(
                "turn.allow_restricted_peer_cidrs",
                &config.allow_restricted_peer_cidrs,
            )?,
            parse("turn.deny_peer_cidrs", &config.deny_peer_cidrs)?,
        );

        let relay_v4 = {
            let bind = config.relay_bind_ipv4;
            let advertised = config
                .relay_ipv4
                .map(IpAddr::V4)
                .or((!bind.is_unspecified()).then_some(bind));
            advertised.map(|advertised| RelayFamily { bind, advertised })
        };
        let relay_v6 = match (config.relay_ipv6, config.relay_bind_ipv6) {
            (Some(adv), bind) => Some(RelayFamily {
                bind: bind.unwrap_or(IpAddr::V6(Ipv6Addr::UNSPECIFIED)),
                advertised: IpAddr::V6(adv),
            }),
            (None, Some(bind)) if !bind.is_unspecified() => Some(RelayFamily {
                bind,
                advertised: bind,
            }),
            (None, _) => None,
        };
        if relay_v4.is_none() && relay_v6.is_none() {
            return invalid(
                "no relay address: set relay_ipv4/relay_ipv6 or a specific relay bind address"
                    .into(),
            );
        }

        // Bind everything before spawning so a failure leaves nothing running.
        enum Bound {
            Udp(Arc<UdpSocket>),
            Tcp(TcpListener),
        }
        let mut bound = Vec::with_capacity(config.listeners.len());
        let mut local_addrs = Vec::with_capacity(config.listeners.len());
        for spec in &config.listeners {
            match *spec {
                ListenerSpec::Udp(addr) => {
                    let s = UdpSocket::bind(addr)
                        .await
                        .map_err(|source| TurnError::Bind { addr, source })?;
                    local_addrs.push(s.local_addr()?);
                    bound.push(Bound::Udp(Arc::new(s)));
                }
                ListenerSpec::Tcp(addr) => {
                    let l = TcpListener::bind(addr)
                        .await
                        .map_err(|source| TurnError::Bind { addr, source })?;
                    local_addrs.push(l.local_addr()?);
                    bound.push(Bound::Tcp(l));
                }
            }
        }

        let ctx = Arc::new(ServerCtx {
            realm: config.realm.clone(),
            auth,
            nonces: NonceManager::new(),
            allocs: AllocationManager::new(
                config.max_allocations,
                config.per_user_allocation_limit,
            ),
            policy,
            local_relay_policy,
            relay_v4,
            relay_v6,
            relay_bind_from_listener,
            port_range: config.relay_port_range,
            default_lifetime: config.default_lifetime,
            max_lifetime: config.max_lifetime,
            relayed_bytes: Arc::new(AtomicU64::new(0)),
        });

        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let mut tasks = Vec::with_capacity(bound.len() + 1);
        for b in bound {
            match b {
                Bound::Udp(s) => tasks.push(tokio::spawn(udp_listener(
                    ctx.clone(),
                    s,
                    shutdown_rx.clone(),
                ))),
                Bound::Tcp(l) => tasks.push(tokio::spawn(tcp::tcp_listener(
                    ctx.clone(),
                    l,
                    config.tls.clone(),
                    config.proxy_trusted_cidrs.clone(),
                    shutdown_rx.clone(),
                    config.max_allocations,
                ))),
            }
        }
        tasks.push(tokio::spawn(reaper(ctx.clone(), shutdown_rx)));

        tracing::info!(
            realm = %config.realm,
            listeners = ?local_addrs,
            tls = config.tls.is_some(),
            relay_ipv4 = ?relay_v4.map(|r| r.advertised),
            relay_ipv6 = ?relay_v6.map(|r| r.advertised),
            relay_range_start = lo,
            relay_range_end = hi,
            per_user_allocation_limit = config.per_user_allocation_limit,
            "Starting TURN server"
        );
        Ok(TurnHandle {
            ctx,
            shutdown_tx,
            tasks,
            local_addrs,
        })
    }
}

/// Handle to a running server. Dropping it stops the server without waiting.
pub struct TurnHandle {
    ctx: Arc<ServerCtx>,
    shutdown_tx: watch::Sender<bool>,
    tasks: Vec<JoinHandle<()>>,
    local_addrs: Vec<SocketAddr>,
}

impl TurnHandle {
    /// Stops all listeners, connections and relay tasks and waits for them.
    pub async fn shutdown(mut self) {
        let _ = self.shutdown_tx.send(true);
        for t in std::mem::take(&mut self.tasks) {
            let _ = t.await;
        }
        let all = self.ctx.allocs.clear();
        tracing::info!(allocations = all.len(), "TURN server stopped");
        for allocation in all {
            if let Ok(mut allocation) = Arc::try_unwrap(allocation) {
                allocation.shutdown().await;
            }
        }
    }

    /// Current number of allocations.
    pub fn allocation_count(&self) -> usize {
        self.ctx.allocs.len()
    }

    /// Total payload bytes relayed in both directions.
    pub fn relayed_bytes(&self) -> u64 {
        self.ctx.relayed_bytes.load(Ordering::Relaxed)
    }

    /// Actual bound listener addresses, in `TurnServerConfig::listeners` order
    /// (useful with port 0).
    pub fn local_addrs(&self) -> &[SocketAddr] {
        &self.local_addrs
    }
}

impl Drop for TurnHandle {
    fn drop(&mut self) {
        let _ = self.shutdown_tx.send(true);
        for t in &self.tasks {
            t.abort();
        }
        drop(self.ctx.allocs.clear());
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::auth::long_term_key;

    struct StaticAuth;

    impl AuthHandler for StaticAuth {
        fn authenticate(&self, req: &AuthRequest<'_>) -> Option<(String, [u8; 16])> {
            match req.username {
                "alice" => Some((
                    "alice-id".into(),
                    long_term_key("alice", req.realm, "secret"),
                )),
                // "expired" credential: only usable for non-Allocate methods
                "late" if req.method != Method::Allocate => {
                    Some(("late-id".into(), long_term_key("late", req.realm, "secret")))
                }
                _ => None,
            }
        }
    }

    fn config() -> TurnServerConfig {
        TurnServerConfig {
            listeners: vec![ListenerSpec::Udp("127.0.0.1:0".parse().unwrap())],
            relay_ipv4: Some(Ipv4Addr::LOCALHOST),
            relay_bind_ipv4: IpAddr::V4(Ipv4Addr::LOCALHOST),
            relay_port_range: (0, 0),
            per_user_allocation_limit: 1,
            // allow loopback peers so tests can relay locally
            allow_restricted_peer_cidrs: vec![(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 0)), 8)],
            ..TurnServerConfig::default()
        }
    }

    struct Client {
        sock: UdpSocket,
        server: SocketAddr,
        nonce: Vec<u8>,
        key: [u8; 16],
        user: &'static str,
    }

    impl Client {
        async fn new(server: SocketAddr, user: &'static str) -> Client {
            let sock = UdpSocket::bind("127.0.0.1:0").await.unwrap();
            Client {
                sock,
                server,
                nonce: Vec::new(),
                key: long_term_key(user, "livekit", "secret"),
                user,
            }
        }

        async fn roundtrip(&self, req: Vec<u8>) -> Message {
            self.sock.send_to(&req, self.server).await.unwrap();
            let mut buf = vec![0u8; 2048];
            let (n, _) =
                tokio::time::timeout(Duration::from_secs(2), self.sock.recv_from(&mut buf))
                    .await
                    .expect("timeout")
                    .unwrap();
            Message::decode(&buf[..n]).unwrap()
        }

        fn signed(&self, method: Method, f: impl FnOnce(&mut MessageBuilder)) -> Vec<u8> {
            let mut b = MessageBuilder::new(method, Class::Request, rand::random());
            f(&mut b);
            b.add(attr::USERNAME, self.user.as_bytes())
                .add(attr::REALM, b"livekit")
                .add(attr::NONCE, &self.nonce);
            b.build(Some(&self.key), true)
        }

        /// Performs the 401 challenge + authenticated Allocate; returns the
        /// authenticated request bytes and the response.
        async fn allocate(&mut self) -> (Vec<u8>, Message) {
            let mut b = MessageBuilder::new(Method::Allocate, Class::Request, rand::random());
            b.add(
                attr::REQUESTED_TRANSPORT,
                &attr::encode_requested_transport(attr::PROTO_UDP),
            );
            let resp = self.roundtrip(b.build(None, true)).await;
            assert_eq!(resp.error_code().unwrap().unwrap().0, 401);
            self.nonce = resp.nonce().unwrap().to_vec();
            let req = self.signed(Method::Allocate, |b| {
                b.add(
                    attr::REQUESTED_TRANSPORT,
                    &attr::encode_requested_transport(attr::PROTO_UDP),
                );
            });
            let resp = self.roundtrip(req.clone()).await;
            (req, resp)
        }
    }

    #[tokio::test]
    async fn relay_preserves_large_peer_datagrams_and_shutdown_releases_ports() {
        let h = TurnServer::start(config(), Arc::new(StaticAuth))
            .await
            .unwrap();
        let listener = h.local_addrs()[0];
        let mut c = Client::new(listener, "alice").await;
        let (_, response) = c.allocate().await;
        assert_eq!(response.class(), Class::Success);
        let relay = response
            .xor_address(attr::XOR_RELAYED_ADDRESS)
            .unwrap()
            .unwrap();
        let peer = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let response = c
            .roundtrip(c.signed(Method::CreatePermission, |b| {
                b.add_xor_address(attr::XOR_PEER_ADDRESS, peer.local_addr().unwrap());
            }))
            .await;
        assert_eq!(response.class(), Class::Success);
        let payload = vec![0x5a; 8192];
        peer.send_to(&payload, relay).await.unwrap();
        let mut buf = vec![0; 16384];
        let (n, _) = tokio::time::timeout(Duration::from_secs(2), c.sock.recv_from(&mut buf))
            .await
            .unwrap()
            .unwrap();
        let indication = Message::decode(&buf[..n]).unwrap();
        assert_eq!(indication.data().unwrap(), payload);
        h.shutdown().await;
        // shutdown joins the relay reader, so no retry/yield is needed here.
        UdpSocket::bind(relay)
            .await
            .expect("relay socket must be released when shutdown returns");
        UdpSocket::bind(listener)
            .await
            .expect("listener socket must be released when shutdown returns");
    }

    #[tokio::test]
    async fn binding_without_auth() {
        let h = TurnServer::start(config(), Arc::new(StaticAuth))
            .await
            .unwrap();
        let c = Client::new(h.local_addrs()[0], "alice").await;
        let b = MessageBuilder::new(Method::Binding, Class::Request, [5u8; 12]);
        let resp = c.roundtrip(b.build(None, true)).await;
        assert_eq!(resp.class(), Class::Success);
        let mapped = resp.xor_address(attr::XOR_MAPPED_ADDRESS).unwrap().unwrap();
        assert_eq!(mapped, c.sock.local_addr().unwrap());
        h.shutdown().await;
    }

    #[tokio::test]
    async fn allocate_permission_relay_and_refresh() {
        let h = TurnServer::start(config(), Arc::new(StaticAuth))
            .await
            .unwrap();
        let mut c = Client::new(h.local_addrs()[0], "alice").await;
        let (alloc_req, resp) = c.allocate().await;
        assert_eq!(resp.class(), Class::Success, "{:?}", resp.error_code());
        assert!(resp.verify_integrity(&c.key));
        let relay = resp
            .xor_address(attr::XOR_RELAYED_ADDRESS)
            .unwrap()
            .unwrap();
        assert_eq!(h.allocation_count(), 1);

        // Retransmission with the same transaction id returns the cached response.
        let retrans = c.roundtrip(alloc_req).await;
        assert_eq!(retrans.raw(), resp.raw());
        assert_eq!(h.allocation_count(), 1);

        // A different transaction id on the same 5-tuple is a mismatch.
        let again = c
            .roundtrip(c.signed(Method::Allocate, |b| {
                b.add(
                    attr::REQUESTED_TRANSPORT,
                    &attr::encode_requested_transport(attr::PROTO_UDP),
                );
            }))
            .await;
        assert_eq!(again.error_code().unwrap().unwrap().0, 437);

        // Peer socket on loopback.
        let peer = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let peer_addr = peer.local_addr().unwrap();
        let resp = c
            .roundtrip(c.signed(Method::CreatePermission, |b| {
                b.add_xor_address(attr::XOR_PEER_ADDRESS, peer_addr);
            }))
            .await;
        assert_eq!(resp.class(), Class::Success, "{:?}", resp.error_code());

        // Send indication -> peer.
        let mut b = MessageBuilder::new(Method::Send, Class::Indication, rand::random());
        b.add_xor_address(attr::XOR_PEER_ADDRESS, peer_addr)
            .add(attr::DATA, b"ping");
        c.sock
            .send_to(&b.build(None, false), c.server)
            .await
            .unwrap();
        let mut buf = [0u8; 64];
        let (n, from) = tokio::time::timeout(Duration::from_secs(2), peer.recv_from(&mut buf))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(&buf[..n], b"ping");
        assert_eq!(from, relay);

        // Peer -> client arrives as a Data indication.
        peer.send_to(b"pong", relay).await.unwrap();
        let mut cbuf = [0u8; 256];
        let (n, _) = tokio::time::timeout(Duration::from_secs(2), c.sock.recv_from(&mut cbuf))
            .await
            .unwrap()
            .unwrap();
        let data = Message::decode(&cbuf[..n]).unwrap();
        assert_eq!(data.method(), Method::Data);
        assert_eq!(data.data().unwrap(), b"pong");

        // ChannelBind then ChannelData both ways.
        let resp = c
            .roundtrip(c.signed(Method::ChannelBind, |b| {
                b.add(attr::CHANNEL_NUMBER, &attr::encode_channel_number(0x4000));
                b.add_xor_address(attr::XOR_PEER_ADDRESS, peer_addr);
            }))
            .await;
        assert_eq!(resp.class(), Class::Success, "{:?}", resp.error_code());
        c.sock
            .send_to(&ChannelData::encode(0x4000, b"cd-up", false), c.server)
            .await
            .unwrap();
        let (n, _) = tokio::time::timeout(Duration::from_secs(2), peer.recv_from(&mut buf))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(&buf[..n], b"cd-up");
        peer.send_to(b"cd-down", relay).await.unwrap();
        let (n, _) = tokio::time::timeout(Duration::from_secs(2), c.sock.recv_from(&mut cbuf))
            .await
            .unwrap()
            .unwrap();
        let cd = ChannelData::decode(&cbuf[..n]).unwrap();
        assert_eq!(cd.number, 0x4000);
        assert_eq!(cd.data, b"cd-down");
        assert_eq!(h.relayed_bytes(), (4 + 4 + 5 + 7) as u64);

        // Restricted peer is denied (192.168/16 not in allow list).
        let resp = c
            .roundtrip(c.signed(Method::CreatePermission, |b| {
                b.add_xor_address(attr::XOR_PEER_ADDRESS, "192.168.1.1:1000".parse().unwrap());
            }))
            .await;
        assert_eq!(resp.error_code().unwrap().unwrap().0, 403);

        // Refresh with lifetime 0 deletes.
        let resp = c
            .roundtrip(c.signed(Method::Refresh, |b| {
                b.add_u32(attr::LIFETIME, 0);
            }))
            .await;
        assert_eq!(resp.class(), Class::Success);
        assert_eq!(h.allocation_count(), 0);
        h.shutdown().await;
    }

    #[tokio::test]
    async fn embedded_local_relay_policy_filters_send_and_channel_data_after_permission() {
        let allowed = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let blocked = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let allowed_addr = allowed.local_addr().unwrap();
        let blocked_addr = blocked.local_addr().unwrap();
        let mut cfg = config();
        let policy = LocalRelayPolicy::new(
            ["::ffff:127.0.0.1".parse().unwrap()],
            vec![(allowed_addr.port(), allowed_addr.port())],
        );
        cfg.deny_peer_cidrs = vec![("127.0.0.2".parse().unwrap(), 32)];
        let h = TurnServer::start_with_local_relay_policy(cfg, Arc::new(StaticAuth), Some(policy))
            .await
            .unwrap();
        let mut c = Client::new(h.local_addrs()[0], "alice").await;
        let (_, allocation) = c.allocate().await;
        assert_eq!(allocation.class(), Class::Success);
        let relay = allocation
            .xor_address(attr::XOR_RELAYED_ADDRESS)
            .unwrap()
            .unwrap();

        let denied = c
            .roundtrip(c.signed(Method::CreatePermission, |b| {
                b.add_xor_address(attr::XOR_PEER_ADDRESS, "127.0.0.2:1234".parse().unwrap());
            }))
            .await;
        assert_eq!(denied.error_code().unwrap().unwrap().0, 403);

        let permission = c
            .roundtrip(c.signed(Method::CreatePermission, |b| {
                b.add_xor_address(attr::XOR_PEER_ADDRESS, blocked_addr);
            }))
            .await;
        assert_eq!(permission.class(), Class::Success);
        let mapped_blocked =
            SocketAddr::new("::ffff:127.0.0.1".parse().unwrap(), blocked_addr.port());
        for (channel, addr) in [(0x4000, mapped_blocked), (0x4001, allowed_addr)] {
            let bound = c
                .roundtrip(c.signed(Method::ChannelBind, |b| {
                    b.add(attr::CHANNEL_NUMBER, &attr::encode_channel_number(channel));
                    b.add_xor_address(attr::XOR_PEER_ADDRESS, addr);
                }))
                .await;
            assert_eq!(bound.class(), Class::Success, "{:?}", bound.error_code());
        }

        for (peer, payload) in [
            (mapped_blocked, b"blocked-send".as_slice()),
            (allowed_addr, b"allowed-send".as_slice()),
        ] {
            let mut send = MessageBuilder::new(Method::Send, Class::Indication, rand::random());
            send.add_xor_address(attr::XOR_PEER_ADDRESS, peer)
                .add(attr::DATA, payload);
            c.sock
                .send_to(&send.build(None, false), c.server)
                .await
                .unwrap();
        }
        for (channel, payload) in [
            (0x4000, b"blocked-channel".as_slice()),
            (0x4001, b"allowed-channel".as_slice()),
        ] {
            c.sock
                .send_to(&ChannelData::encode(channel, payload, false), c.server)
                .await
                .unwrap();
        }
        let mut buf = [0; 64];
        for expected in [b"allowed-send".as_slice(), b"allowed-channel".as_slice()] {
            let (n, from) =
                tokio::time::timeout(Duration::from_secs(2), allowed.recv_from(&mut buf))
                    .await
                    .expect("allowed packet timed out")
                    .unwrap();
            assert_eq!(&buf[..n], expected);
            assert_eq!(from, relay);
        }
        assert!(
            tokio::time::timeout(Duration::from_millis(150), blocked.recv_from(&mut buf))
                .await
                .is_err()
        );
        assert!(
            tokio::time::timeout(Duration::from_millis(150), c.sock.recv_from(&mut buf))
                .await
                .is_err()
        );
        assert_eq!(h.relayed_bytes(), 27);

        let refresh = c
            .roundtrip(c.signed(Method::Refresh, |b| {
                b.add_u32(attr::LIFETIME, 0);
            }))
            .await;
        assert_eq!(refresh.class(), Class::Success);
        assert_eq!(h.allocation_count(), 0);
        h.shutdown().await;
    }

    #[tokio::test]
    async fn rejects_tcp_transport_and_quota() {
        let h = TurnServer::start(config(), Arc::new(StaticAuth))
            .await
            .unwrap();
        let mut c = Client::new(h.local_addrs()[0], "alice").await;
        assert_eq!(c.allocate().await.1.class(), Class::Success);

        // TCP relay rejected with 442.
        let mut c2 = Client::new(h.local_addrs()[0], "alice").await;
        c2.nonce = c.nonce.clone();
        let resp = c2
            .roundtrip(c2.signed(Method::Allocate, |b| {
                b.add(
                    attr::REQUESTED_TRANSPORT,
                    &attr::encode_requested_transport(6),
                );
            }))
            .await;
        assert_eq!(resp.error_code().unwrap().unwrap().0, 442);

        // Per-user quota of 1 reached -> 486.
        let resp = c2
            .roundtrip(c2.signed(Method::Allocate, |b| {
                b.add(
                    attr::REQUESTED_TRANSPORT,
                    &attr::encode_requested_transport(attr::PROTO_UDP),
                );
            }))
            .await;
        assert_eq!(resp.error_code().unwrap().unwrap().0, 486);
        h.shutdown().await;
    }

    #[tokio::test]
    async fn stale_nonce_and_bad_credentials() {
        let h = TurnServer::start(config(), Arc::new(StaticAuth))
            .await
            .unwrap();
        let mut c = Client::new(h.local_addrs()[0], "alice").await;
        c.nonce = b"0000000000000000ffffffffffffffff".to_vec();
        let resp = c
            .roundtrip(c.signed(Method::Allocate, |b| {
                b.add(
                    attr::REQUESTED_TRANSPORT,
                    &attr::encode_requested_transport(attr::PROTO_UDP),
                );
            }))
            .await;
        assert_eq!(resp.error_code().unwrap().unwrap().0, 438);
        assert!(resp.nonce().is_some());
        assert!(resp.realm().is_some());

        // Expired credential cannot Allocate.
        let mut late = Client::new(h.local_addrs()[0], "late").await;
        late.nonce = resp.nonce().unwrap().to_vec();
        let resp = late
            .roundtrip(late.signed(Method::Allocate, |b| {
                b.add(
                    attr::REQUESTED_TRANSPORT,
                    &attr::encode_requested_transport(attr::PROTO_UDP),
                );
            }))
            .await;
        assert_eq!(resp.error_code().unwrap().unwrap().0, 401);

        // Wrong key -> 401.
        c.nonce = resp.nonce().unwrap().to_vec();
        c.key = [0u8; 16];
        let resp = c
            .roundtrip(c.signed(Method::Allocate, |b| {
                b.add(
                    attr::REQUESTED_TRANSPORT,
                    &attr::encode_requested_transport(attr::PROTO_UDP),
                );
            }))
            .await;
        assert_eq!(resp.error_code().unwrap().unwrap().0, 401);
        h.shutdown().await;
    }

    #[tokio::test]
    async fn invalid_config_rejected() {
        let mut cfg = config();
        cfg.deny_peer_cidrs = vec![(IpAddr::V4(Ipv4Addr::UNSPECIFIED), 40)];
        assert!(matches!(
            TurnServer::start(cfg, Arc::new(StaticAuth)).await,
            Err(TurnError::InvalidConfig(_))
        ));
        let mut cfg = config();
        cfg.relay_port_range = (5000, 4000);
        assert!(matches!(
            TurnServer::start(cfg, Arc::new(StaticAuth)).await,
            Err(TurnError::InvalidConfig(_))
        ));
    }
}
