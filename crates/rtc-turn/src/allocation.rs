//! Allocation state, the per-allocation relay task and the allocation table.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::fmt;
use std::net::{IpAddr, SocketAddr};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::{Duration, Instant};

use bytes::Bytes;
use tokio::net::UdpSocket;
use tokio::sync::{Notify, mpsc};
use tokio::task::JoinHandle;

use crate::auth::UnixProxyAddress;
use crate::policy::unmap;
use crate::stun::{Class, MessageBuilder, Method, attr};

/// Permission lifetime (RFC 8656 §9).
pub(crate) const PERMISSION_LIFETIME: Duration = Duration::from_secs(300);
/// Channel binding lifetime (RFC 8656 §12).
pub(crate) const CHANNEL_LIFETIME: Duration = Duration::from_secs(600);
/// Per-allocation cap on installed permissions (memory bound).
pub(crate) const MAX_PERMISSIONS: usize = 1024;
/// Per-allocation cap on channel bindings (memory bound).
pub(crate) const MAX_CHANNELS: usize = 1024;
/// Largest peer datagram relayed back to the client.
const RELAY_RECV_BUF: usize = 65_536;

/// Client-side transport of the 5-tuple.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) enum Transport {
    Udp,
    Tcp,
}

/// The logical peer. Unix connections own their allocations even when a
/// trusted proxy forwards the same Unix address names on multiple streams.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) enum ClientAddr {
    Inet(SocketAddr),
    Unix {
        connection_id: u64,
        address: Arc<UnixProxyAddress>,
    },
}

impl fmt::Display for ClientAddr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Inet(addr) => addr.fmt(f),
            Self::Unix { connection_id, .. } => write!(f, "unix connection {connection_id}"),
        }
    }
}

impl ClientAddr {
    pub(crate) fn inet(&self) -> Option<SocketAddr> {
        match self {
            Self::Inet(addr) => Some(*addr),
            Self::Unix { .. } => None,
        }
    }
}

/// Allocation key: (logical client, server listener address, transport).
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) struct FiveTuple {
    pub client: ClientAddr,
    pub server: SocketAddr,
    pub transport: Transport,
}

/// How data from peers reaches the client.
#[derive(Debug, Clone)]
pub(crate) enum ClientSink {
    /// Sent from the shared UDP listener socket.
    Udp {
        socket: Arc<UdpSocket>,
        client: SocketAddr,
    },
    /// Queued to the TCP connection task (bounded; full = drop).
    Tcp { tx: mpsc::Sender<Bytes> },
}

impl ClientSink {
    async fn send(&self, buf: &[u8]) {
        match self {
            ClientSink::Udp { socket, client } => {
                if let Err(e) = socket.send_to(buf, *client).await {
                    tracing::trace!(error = %e, %client, "TURN send to client failed");
                }
            }
            ClientSink::Tcp { tx } => {
                // Never block the relay on a slow stream; behave like UDP.
                let _ = tx.try_send(Bytes::copy_from_slice(buf));
            }
        }
    }
}

#[derive(Debug, Clone, Copy)]
struct Channel {
    peer: SocketAddr,
    expires: Instant,
}

#[derive(Debug)]
struct AllocInner {
    expires: Instant,
    permissions: HashMap<IpAddr, Instant>,
    channels: HashMap<u16, Channel>,
    channel_by_peer: HashMap<SocketAddr, u16>,
    /// Transaction id + encoded success response of the Allocate request,
    /// replayed on retransmission.
    alloc_txid: [u8; 12],
    alloc_response: Vec<u8>,
}

/// Outcome of a ChannelBind attempt.
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum BindResult {
    Ok,
    /// Channel bound to another peer, or peer bound to another channel.
    Conflict,
    /// Too many channels.
    Full,
}

/// Shared allocation state, referenced by the relay task.
#[derive(Debug)]
pub(crate) struct AllocState {
    pub five_tuple: FiveTuple,
    pub user_id: String,
    pub relay_addr: SocketAddr,
    relay_socket: Arc<UdpSocket>,
    sink: ClientSink,
    relayed_bytes: Arc<AtomicU64>,
    inner: Mutex<AllocInner>,
}

impl AllocState {
    fn lock(&self) -> MutexGuard<'_, AllocInner> {
        // A poisoned lock only means another thread panicked mid-update; the
        // data is still structurally valid for our purposes.
        self.inner.lock().unwrap_or_else(|e| e.into_inner())
    }

    pub(crate) fn relay_family_is_v4(&self) -> bool {
        self.relay_addr.is_ipv4()
    }

    pub(crate) fn cached_response(&self, txid: &[u8; 12]) -> Option<Vec<u8>> {
        let inner = self.lock();
        (inner.alloc_txid == *txid && !inner.alloc_response.is_empty())
            .then(|| inner.alloc_response.clone())
    }

    pub(crate) fn set_cached_response(&self, txid: [u8; 12], resp: Vec<u8>) {
        let mut inner = self.lock();
        inner.alloc_txid = txid;
        inner.alloc_response = resp;
    }

    fn expires_at(&self) -> Instant {
        self.lock().expires
    }

    /// Installs or refreshes one permission, subject to the allocation cap.
    pub(crate) fn add_permission(&self, peer: IpAddr) -> bool {
        let now = Instant::now();
        let mut inner = self.lock();
        inner.permissions.retain(|_, exp| *exp > now);
        let peer = unmap(peer);
        if inner.permissions.len() >= MAX_PERMISSIONS && !inner.permissions.contains_key(&peer) {
            return false;
        }
        inner.permissions.insert(peer, now + PERMISSION_LIFETIME);
        true
    }

    pub(crate) fn has_permission(&self, peer: IpAddr, now: Instant) -> bool {
        self.lock()
            .permissions
            .get(&unmap(peer))
            .is_some_and(|exp| *exp > now)
    }

    /// Binds (or refreshes) `number` <-> `peer`, also refreshing the peer's
    /// permission (RFC 8656 §12.2).
    pub(crate) fn bind_channel(&self, number: u16, peer: SocketAddr) -> BindResult {
        let now = Instant::now();
        let mut inner = self.lock();
        // Drop expired bindings first so their numbers/peers can be reused.
        let expired: Vec<(u16, SocketAddr)> = inner
            .channels
            .iter()
            .filter(|(_, c)| c.expires <= now)
            .map(|(n, c)| (*n, c.peer))
            .collect();
        for (n, p) in expired {
            inner.channels.remove(&n);
            inner.channel_by_peer.remove(&p);
        }
        if let Some(c) = inner.channels.get(&number) {
            if c.peer != peer {
                return BindResult::Conflict;
            }
        } else {
            if inner.channel_by_peer.contains_key(&peer) {
                return BindResult::Conflict;
            }
            if inner.channels.len() >= MAX_CHANNELS {
                return BindResult::Full;
            }
        }
        let ip = unmap(peer.ip());
        if !inner.permissions.contains_key(&ip) && inner.permissions.len() >= MAX_PERMISSIONS {
            return BindResult::Full;
        }
        inner.channels.insert(
            number,
            Channel {
                peer,
                expires: now + CHANNEL_LIFETIME,
            },
        );
        inner.channel_by_peer.insert(peer, number);
        inner.permissions.insert(ip, now + PERMISSION_LIFETIME);
        BindResult::Ok
    }

    /// Peer for a bound, unexpired channel on a live allocation.
    pub(crate) fn channel_peer(&self, number: u16, now: Instant) -> Option<SocketAddr> {
        let inner = self.lock();
        if inner.expires <= now {
            return None;
        }
        inner
            .channels
            .get(&number)
            .filter(|c| c.expires > now)
            .map(|c| c.peer)
    }

    /// Prunes expired permissions and channels.
    pub(crate) fn prune(&self, now: Instant) {
        let mut inner = self.lock();
        inner.permissions.retain(|_, exp| *exp > now);
        let expired: Vec<(u16, SocketAddr)> = inner
            .channels
            .iter()
            .filter(|(_, c)| c.expires <= now)
            .map(|(n, c)| (*n, c.peer))
            .collect();
        for (n, p) in expired {
            inner.channels.remove(&n);
            inner.channel_by_peer.remove(&p);
        }
    }

    /// Client -> peer. Non-blocking: drops the datagram if the socket buffer
    /// is full, like a congested UDP path would.
    pub(crate) fn send_to_peer(&self, data: &[u8], peer: SocketAddr) {
        match self.relay_socket.try_send_to(data, peer) {
            Ok(n) => {
                self.relayed_bytes.fetch_add(n as u64, Ordering::Relaxed);
            }
            Err(e) => tracing::trace!(error = %e, %peer, "TURN relay send to peer failed"),
        }
    }

    /// What to do with a datagram from `peer`: Some(Some(ch)) = ChannelData,
    /// Some(None) = Data indication, None = drop.
    fn route_from_peer(&self, peer: SocketAddr, now: Instant) -> Option<Option<u16>> {
        let inner = self.lock();
        if inner.expires <= now {
            return None;
        }
        let ch = inner
            .channel_by_peer
            .get(&peer)
            .copied()
            .filter(|n| inner.channels.get(n).is_some_and(|c| c.expires > now));
        if let Some(ch) = ch {
            return Some(Some(ch));
        }
        inner
            .permissions
            .get(&unmap(peer.ip()))
            .is_some_and(|exp| *exp > now)
            .then_some(None)
    }
}

/// Peer -> client relay loop. Runs until aborted by [`Allocation`]'s drop.
async fn relay_task(state: Arc<AllocState>) {
    // Reserve the ChannelData header in front of the payload so the common
    // ChannelData path needs no copy for UDP.
    let mut buf = vec![0u8; 4 + RELAY_RECV_BUF + 3];
    loop {
        let (n, peer) = match state
            .relay_socket
            .recv_from(&mut buf[4..4 + RELAY_RECV_BUF])
            .await
        {
            Ok(v) => v,
            Err(e) => {
                // ICMP-induced errors (e.g. ECONNREFUSED on some platforms)
                // must not kill the relay.
                tracing::trace!(error = %e, "TURN relay recv error");
                tokio::time::sleep(Duration::from_millis(1)).await;
                continue;
            }
        };
        let now = Instant::now();
        match state.route_from_peer(peer, now) {
            None => {
                tracing::trace!(%peer, relay = %state.relay_addr, "dropping peer datagram without permission");
            }
            Some(Some(ch)) => {
                buf[0..2].copy_from_slice(&ch.to_be_bytes());
                buf[2..4].copy_from_slice(&(n as u16).to_be_bytes());
                let total = crate::stun::pad4(4 + n);
                buf[4 + n..total].fill(0);
                state.relayed_bytes.fetch_add(n as u64, Ordering::Relaxed);
                state.sink.send(&buf[..total]).await;
            }
            Some(None) => {
                let txid: [u8; 12] = rand::random();
                let mut b = MessageBuilder::new(Method::Data, Class::Indication, txid);
                b.add_xor_address(attr::XOR_PEER_ADDRESS, peer)
                    .add(attr::DATA, &buf[4..4 + n]);
                let msg = b.build(None, false);
                state.relayed_bytes.fetch_add(n as u64, Ordering::Relaxed);
                state.sink.send(&msg).await;
            }
        }
    }
}

/// An allocation. Dropping it stops the relay task and closes the relay socket.
#[derive(Debug)]
pub(crate) struct Allocation {
    pub state: Arc<AllocState>,
    relay_task: JoinHandle<()>,
}

impl Allocation {
    /// Stop and join the reader so shutdown has released its socket on return.
    pub(crate) async fn shutdown(&mut self) {
        self.relay_task.abort();
        let _ = (&mut self.relay_task).await;
    }

    /// Creates the allocation and spawns its relay task. Must be called
    /// inside a Tokio runtime.
    pub(crate) fn new(
        five_tuple: FiveTuple,
        user_id: String,
        relay_socket: UdpSocket,
        relay_addr: SocketAddr,
        sink: ClientSink,
        lifetime: Duration,
        relayed_bytes: Arc<AtomicU64>,
    ) -> Allocation {
        let state = Arc::new(AllocState {
            five_tuple,
            user_id,
            relay_addr,
            relay_socket: Arc::new(relay_socket),
            sink,
            relayed_bytes,
            inner: Mutex::new(AllocInner {
                expires: Instant::now() + lifetime,
                permissions: HashMap::new(),
                channels: HashMap::new(),
                channel_by_peer: HashMap::new(),
                alloc_txid: [0u8; 12],
                alloc_response: Vec::new(),
            }),
        });
        let relay_task = tokio::spawn(relay_task(state.clone()));
        Allocation { state, relay_task }
    }
}

impl Drop for Allocation {
    fn drop(&mut self) {
        self.relay_task.abort();
    }
}

/// Reasons a new allocation slot cannot be reserved.
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum ReserveError {
    /// Per-user quota hit (486).
    UserQuota,
    /// Global cap hit (508).
    Capacity,
}

#[derive(Default)]
struct ManagerInner {
    allocs: HashMap<FiveTuple, Arc<Allocation>>,
    /// Exactly one scheduled deadline per allocation.
    deadlines: BTreeMap<Instant, HashSet<FiveTuple>>,
    /// Allocations + outstanding reservations per user.
    per_user: HashMap<String, u32>,
    /// Allocations + outstanding reservations overall.
    total: usize,
}

/// The allocation table with quota accounting.
pub(crate) struct AllocationManager {
    inner: Mutex<ManagerInner>,
    deadline_changed: Notify,
    max_allocations: usize,
    per_user_limit: u32,
}

impl AllocationManager {
    pub(crate) fn new(max_allocations: usize, per_user_limit: u32) -> AllocationManager {
        AllocationManager {
            inner: Mutex::new(ManagerInner::default()),
            deadline_changed: Notify::new(),
            max_allocations,
            per_user_limit,
        }
    }

    fn lock(&self) -> MutexGuard<'_, ManagerInner> {
        self.inner.lock().unwrap_or_else(|e| e.into_inner())
    }

    pub(crate) fn get(&self, ft: &FiveTuple) -> Option<Arc<Allocation>> {
        let mut inner = self.lock();
        let alloc = inner.allocs.get(ft)?;
        if alloc.state.expires_at() <= Instant::now() {
            Self::remove_locked(&mut inner, ft);
            return None;
        }
        Some(alloc.clone())
    }

    pub(crate) fn contains(&self, ft: &FiveTuple) -> bool {
        self.get(ft).is_some()
    }

    pub(crate) fn len(&self) -> usize {
        self.lock().allocs.len()
    }

    /// Reserves a slot for `user_id`. Must be followed by exactly one of
    /// [`insert`](Self::insert) or [`release`](Self::release).
    pub(crate) fn reserve(&self, user_id: &str) -> Result<(), ReserveError> {
        let mut inner = self.lock();
        if inner.total >= self.max_allocations {
            return Err(ReserveError::Capacity);
        }
        let count = inner.per_user.get(user_id).copied().unwrap_or(0);
        if self.per_user_limit > 0 && count >= self.per_user_limit {
            return Err(ReserveError::UserQuota);
        }
        inner.per_user.insert(user_id.to_owned(), count + 1);
        inner.total += 1;
        Ok(())
    }

    /// Releases a reservation that did not turn into an allocation.
    pub(crate) fn release(&self, user_id: &str) {
        let mut inner = self.lock();
        Self::release_locked(&mut inner, user_id);
    }

    fn release_locked(inner: &mut ManagerInner, user_id: &str) {
        inner.total = inner.total.saturating_sub(1);
        if let Some(c) = inner.per_user.get_mut(user_id) {
            *c = c.saturating_sub(1);
            if *c == 0 {
                inner.per_user.remove(user_id);
            }
        }
    }

    /// Turns a reservation into a live allocation.
    pub(crate) fn insert(&self, alloc: Allocation) {
        let alloc = Arc::new(alloc);
        let ft = &alloc.state.five_tuple;
        let mut inner = self.lock();
        // Callers check under the same 5-tuple serialisation. Still remove an
        // unexpected predecessor from the deadline and quota tables together.
        Self::remove_locked(&mut inner, ft);
        inner
            .deadlines
            .entry(alloc.state.expires_at())
            .or_default()
            .insert(ft.clone());
        inner.allocs.insert(ft.clone(), alloc);
        self.deadline_changed.notify_one();
    }

    /// Reschedules the current, unexpired allocation, or removes it for zero.
    pub(crate) fn refresh(&self, alloc: &Arc<Allocation>, lifetime: Duration) -> bool {
        let mut inner = self.lock();
        let ft = &alloc.state.five_tuple;
        if !inner
            .allocs
            .get(ft)
            .is_some_and(|current| Arc::ptr_eq(current, alloc))
        {
            return false;
        }
        let now = Instant::now();
        let previous_deadline = alloc.state.expires_at();
        if previous_deadline <= now {
            Self::remove_locked(&mut inner, ft);
            return false;
        }
        if lifetime.is_zero() {
            Self::remove_locked(&mut inner, ft);
        } else {
            Self::remove_deadline(&mut inner, ft, previous_deadline);
            let deadline = now + lifetime;
            alloc.state.lock().expires = deadline;
            inner
                .deadlines
                .entry(deadline)
                .or_default()
                .insert(ft.clone());
            self.deadline_changed.notify_one();
        }
        true
    }

    fn remove_deadline(inner: &mut ManagerInner, ft: &FiveTuple, deadline: Instant) {
        if let Some(entries) = inner.deadlines.get_mut(&deadline) {
            entries.remove(ft);
            if entries.is_empty() {
                inner.deadlines.remove(&deadline);
            }
        }
    }

    fn remove_locked(inner: &mut ManagerInner, ft: &FiveTuple) -> Option<Arc<Allocation>> {
        let removed = inner.allocs.remove(ft)?;
        Self::remove_deadline(inner, ft, removed.state.expires_at());
        Self::release_locked(inner, &removed.state.user_id);
        removed.relay_task.abort();
        Some(removed)
    }

    /// Removes an allocation, releasing its quota and stopping the relay.
    pub(crate) fn remove(&self, ft: &FiveTuple) -> Option<Arc<Allocation>> {
        Self::remove_locked(&mut self.lock(), ft)
    }

    pub(crate) fn next_deadline(&self) -> Option<Instant> {
        self.lock().deadlines.keys().next().copied()
    }

    pub(crate) async fn wait_for_deadline_change(&self) {
        self.deadline_changed.notified().await;
    }

    /// Removes only the leases still due when the scheduler acquires the lock.
    pub(crate) fn reap(&self, now: Instant) -> Vec<Arc<Allocation>> {
        let mut inner = self.lock();
        let mut removed = Vec::new();
        while inner
            .deadlines
            .first_key_value()
            .is_some_and(|(d, _)| *d <= now)
        {
            let (_, expired) = inner.deadlines.pop_first().expect("deadline is due");
            for ft in expired {
                if let Some(alloc) = Self::remove_locked(&mut inner, &ft) {
                    removed.push(alloc);
                }
            }
        }
        removed
    }

    pub(crate) fn prune_permissions_and_channels(&self, now: Instant) {
        for alloc in self.lock().allocs.values() {
            alloc.state.prune(now);
        }
    }

    /// Drops every allocation (shutdown).
    pub(crate) fn clear(&self) -> Vec<Arc<Allocation>> {
        let mut inner = self.lock();
        let all: Vec<Arc<Allocation>> = inner.allocs.drain().map(|(_, a)| a).collect();
        inner.deadlines.clear();
        inner.per_user.clear();
        inner.total = 0;
        all
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ft(port: u16) -> FiveTuple {
        FiveTuple {
            client: ClientAddr::Inet(SocketAddr::from(([192, 0, 2, 1], port))),
            server: SocketAddr::from(([192, 0, 2, 100], 3478)),
            transport: Transport::Udp,
        }
    }

    async fn make_alloc(port: u16, user: &str) -> Allocation {
        let relay = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let relay_addr = relay.local_addr().unwrap();
        let listener = Arc::new(UdpSocket::bind("127.0.0.1:0").await.unwrap());
        Allocation::new(
            ft(port),
            user.to_string(),
            relay,
            relay_addr,
            ClientSink::Udp {
                socket: listener,
                client: ft(port).client.inet().unwrap(),
            },
            Duration::from_secs(600),
            Arc::new(AtomicU64::new(0)),
        )
    }

    #[test]
    fn quota_accounting() {
        let m = AllocationManager::new(3, 2);
        assert_eq!(m.reserve("a"), Ok(()));
        assert_eq!(m.reserve("a"), Ok(()));
        assert_eq!(m.reserve("a"), Err(ReserveError::UserQuota));
        assert_eq!(m.reserve("b"), Ok(()));
        assert_eq!(m.reserve("c"), Err(ReserveError::Capacity));
        m.release("a");
        assert_eq!(m.reserve("c"), Ok(()));
    }

    #[test]
    fn unlimited_user_quota() {
        let m = AllocationManager::new(100, 0);
        for _ in 0..50 {
            assert_eq!(m.reserve("a"), Ok(()));
        }
    }

    #[tokio::test]
    async fn insert_remove_releases_quota() {
        let m = AllocationManager::new(10, 1);
        m.reserve("a").unwrap();
        m.insert(make_alloc(1000, "a").await);
        assert_eq!(m.len(), 1);
        assert_eq!(m.reserve("a"), Err(ReserveError::UserQuota));
        assert!(m.remove(&ft(1000)).is_some());
        assert_eq!(m.len(), 0);
        assert_eq!(m.reserve("a"), Ok(()));
    }

    #[tokio::test]
    async fn refresh_replaces_deadline_and_preserves_allocation_identity() {
        let manager = AllocationManager::new(1, 1);
        let tuple = ft(1000);
        manager.reserve("a").unwrap();
        manager.insert(make_alloc(1000, "a").await);

        let original = manager.get(&tuple).unwrap();
        let original_deadline = manager.next_deadline().unwrap();
        assert!(manager.refresh(&original, Duration::from_secs(1800)));
        let renewed_deadline = manager.next_deadline().unwrap();
        assert!(renewed_deadline > original_deadline);
        assert!(manager.reap(original_deadline).is_empty());
        assert!(manager.refresh(&original, Duration::ZERO));

        manager.reserve("a").unwrap();
        manager.insert(make_alloc(1000, "a").await);
        let replacement = manager.get(&tuple).unwrap();
        assert!(!manager.refresh(&original, Duration::ZERO));
        assert!(!manager.refresh(&original, Duration::from_secs(60)));
        assert!(Arc::ptr_eq(&replacement, &manager.get(&tuple).unwrap()));
        let replacement_deadline = manager.next_deadline().unwrap();
        let expired = manager.reap(replacement_deadline);
        assert_eq!(expired.len(), 1);
        assert!(Arc::ptr_eq(&replacement, &expired[0]));
        assert!(manager.next_deadline().is_none());
        assert!(manager.reserve("a").is_ok());
    }

    #[tokio::test]
    async fn channel_bind_rules() {
        let a = make_alloc(1001, "a").await;
        let p1: SocketAddr = "203.0.113.1:5000".parse().unwrap();
        let p2: SocketAddr = "203.0.113.2:5000".parse().unwrap();
        assert_eq!(a.state.bind_channel(0x4000, p1), BindResult::Ok);
        // refresh same binding
        assert_eq!(a.state.bind_channel(0x4000, p1), BindResult::Ok);
        // same number, different peer
        assert_eq!(a.state.bind_channel(0x4000, p2), BindResult::Conflict);
        // same peer, different number
        assert_eq!(a.state.bind_channel(0x4001, p1), BindResult::Conflict);
        assert_eq!(a.state.bind_channel(0x4001, p2), BindResult::Ok);
        let now = Instant::now();
        assert_eq!(a.state.channel_peer(0x4000, now), Some(p1));
        assert!(a.state.has_permission(p1.ip(), now));
        assert_eq!(a.state.channel_peer(0x4002, now), None);
        // ChannelData in both directions outlives the permission, but a
        // different port still needs a live permission for a Data indication.
        let later = now + PERMISSION_LIFETIME + Duration::from_secs(1);
        let unbound = SocketAddr::new(p1.ip(), p1.port() + 1);
        assert_eq!(a.state.route_from_peer(unbound, now), Some(None));
        assert!(!a.state.has_permission(p1.ip(), later));
        assert_eq!(a.state.channel_peer(0x4000, later), Some(p1));
        assert_eq!(a.state.route_from_peer(p1, later), Some(Some(0x4000)));
        assert_eq!(a.state.route_from_peer(unbound, later), None);

        let after_channel = now + CHANNEL_LIFETIME + Duration::from_secs(1);
        a.state.lock().expires = after_channel + Duration::from_secs(1);
        assert_eq!(a.state.channel_peer(0x4000, after_channel), None);
        assert_eq!(a.state.route_from_peer(p1, after_channel), None);
    }

    #[tokio::test]
    async fn permissions_expire() {
        let a = make_alloc(1002, "a").await;
        let ip: IpAddr = "203.0.113.9".parse().unwrap();
        assert!(a.state.add_permission(ip));
        let now = Instant::now();
        assert!(a.state.has_permission(ip, now));
        assert!(
            !a.state
                .has_permission(ip, now + PERMISSION_LIFETIME + Duration::from_secs(1))
        );
    }
}
