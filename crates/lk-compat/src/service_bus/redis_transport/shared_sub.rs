//! One shared pub/sub connection per transport, multiplexing every subscription.
//!
//! Mirrors the locked reference's `redisbus` shape (`psrpc/pkg/bus/redisbus/redisbus.go:63`
//! one `rc.Subscribe`, `:262` batched `ps.Subscribe`, `:152-178` a single read worker
//! dispatching by channel name) instead of dialing one dedicated connection per
//! subscription. Each subscription still owns a bounded inbox and a fail-closed fault
//! watch: a full inbox faults only that subscription (the reference blocks its read
//! worker head-of-line; the candidate keeps the accepted per-subscription Overflow
//! contract), and a shared disconnect faults every registration so each owner drives
//! its own explicit recovery through `subscribe`. Queue dedup stays on the command
//! connection's SET NX, exactly as before.

use super::{TransportError, config};
use crate::redis_endpoint::RedisEndpoint;
use redis::aio::MultiplexedConnection;
use redis::cluster_async::ClusterConnection;
use redis::{AsyncConnectionConfig, PushInfo, PushKind, Value};
use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};
use tokio::sync::{mpsc, watch};

/// A registration key. Dropping it (or an explicit unsubscribe) removes the route.
pub(super) struct SubHandle {
    pub id: u64,
    pub channel: Vec<u8>,
}

struct Route {
    id: u64,
    sender: mpsc::Sender<Vec<u8>>,
    fault: watch::Sender<Option<TransportError>>,
    cap: usize,
}

enum Conn {
    Simple(MultiplexedConnection),
    Cluster(ClusterConnection),
}

impl Conn {
    async fn subscribe(&self, channels: &[Vec<u8>]) -> redis::RedisResult<()> {
        if channels.is_empty() {
            return Ok(());
        }
        match self {
            Conn::Simple(c) => c.clone().subscribe(channels).await,
            Conn::Cluster(c) => c.clone().subscribe(channels).await,
        }
    }
    async fn unsubscribe(&self, channels: &[Vec<u8>]) -> redis::RedisResult<()> {
        if channels.is_empty() {
            return Ok(());
        }
        match self {
            Conn::Simple(c) => c.clone().unsubscribe(channels).await,
            Conn::Cluster(c) => c.clone().unsubscribe(channels).await,
        }
    }
}

struct State {
    generation: u64,
    connection: Option<Conn>,
    subscribed_epoch: u64,
    routes: HashMap<Vec<u8>, Vec<Route>>,
    subscribed: HashSet<Vec<u8>>,
    pending_unsubscribe: HashSet<Vec<u8>>,
}

pub(super) struct SharedSubscriptions {
    endpoint: RedisEndpoint,
    base_config: AsyncConnectionConfig,
    io_timeout: std::time::Duration,
    state: Arc<Mutex<State>>,
    dial_gate: Arc<tokio::sync::Mutex<()>>,
    next_id: AtomicU64,
}

fn lock(state: &Mutex<State>) -> MutexGuard<'_, State> {
    state
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

/// The single push sender installed on the shared connection. It routes by channel
/// name to every registered inbox (fan-out); queue consumers still dedup via SET NX.
/// `generation` fences pushes from a connection that a disconnect already invalidated.
fn push_sender(state: Arc<Mutex<State>>, generation: u64) -> impl redis::aio::AsyncPushSender {
    move |info: PushInfo| -> Result<(), ()> {
        let mut st = lock(&state);
        if st.generation != generation {
            return Ok(());
        }
        match info.kind {
            PushKind::Disconnection => {
                st.generation += 1;
                st.connection = None;
                st.subscribed.clear();
                for routes in st.routes.values() {
                    for route in routes {
                        let _ = route.fault.send_replace(Some(TransportError::Disconnected));
                    }
                }
            }
            PushKind::Message => {
                if info.data.len() != 2 {
                    return Ok(());
                }
                let payload = info.data[1].clone();
                let channel = info.data[0].clone();
                let Value::BulkString(channel) = channel else {
                    return Ok(());
                };
                let Value::BulkString(bytes) = payload else {
                    if let Some(routes) = st.routes.get_mut(&channel) {
                        for route in routes.iter() {
                            let _ = route.fault.send_replace(Some(TransportError::Disconnected));
                        }
                    }
                    return Err(());
                };
                if let Some(routes) = st.routes.get_mut(&channel) {
                    routes.retain(|route| {
                        if bytes.len() > route.cap {
                            let _ = route.fault.send_replace(Some(TransportError::Oversized));
                            return false;
                        }
                        match route.sender.try_send(bytes.clone()) {
                            Ok(()) => true,
                            Err(mpsc::error::TrySendError::Full(_)) => {
                                let _ = route.fault.send_replace(Some(TransportError::Overflow));
                                false
                            }
                            Err(mpsc::error::TrySendError::Closed(_)) => false,
                        }
                    });
                }
            }
            _ => {}
        }
        Ok(())
    }
}

impl SharedSubscriptions {
    pub fn new(endpoint: RedisEndpoint, limits: super::TransportLimits) -> Self {
        Self {
            endpoint,
            base_config: config(limits),
            io_timeout: limits.io_timeout,
            state: Arc::new(Mutex::new(State {
                generation: 0,
                connection: None,
                subscribed_epoch: 0,
                routes: HashMap::new(),
                subscribed: HashSet::new(),
                pending_unsubscribe: HashSet::new(),
            })),
            dial_gate: Arc::new(tokio::sync::Mutex::new(())),
            next_id: AtomicU64::new(1),
        }
    }

    async fn dial(&self, generation: u64) -> Result<Conn, TransportError> {
        let push = push_sender(self.state.clone(), generation);
        if self.endpoint.is_cluster() {
            let builder = self
                .endpoint
                .cluster_builder()
                .ok_or(TransportError::Configuration)?;
            let client = builder
                .push_sender(push)
                .build()
                .map_err(|_| TransportError::Redis)?;
            let conn = client
                .get_async_connection()
                .await
                .map_err(|_| TransportError::Redis)?;
            Ok(Conn::Cluster(conn))
        } else {
            let conn = self
                .endpoint
                .dial(&self.base_config.clone().set_push_sender(push))
                .await
                .map_err(|_| TransportError::Redis)?;
            Ok(Conn::Simple(conn))
        }
    }

    /// Register a bounded inbox for `channel`, ensuring the shared connection is live
    /// and the channel SUBSCRIBEd (acknowledged) before returning. On a fresh dial the
    /// union of still-registered channels is re-subscribed in one batch, so recovering
    /// owners share a single replacement connection.
    pub async fn subscribe(
        &self,
        channel: &[u8],
        inbox: usize,
        cap: usize,
    ) -> Result<
        (
            SubHandle,
            mpsc::Receiver<Vec<u8>>,
            watch::Receiver<Option<TransportError>>,
            u64,
        ),
        TransportError,
    > {
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        let (sender, receiver) = mpsc::channel(inbox);
        let (fault, failure) = watch::channel(None);
        {
            let mut st = lock(&self.state);
            st.routes.entry(channel.to_vec()).or_default().push(Route {
                id,
                sender,
                fault,
                cap,
            });
        }
        let subscribed_epoch = match self.ensure(channel).await {
            Ok(epoch) => epoch,
            Err(error) => {
                self.remove_route(id, channel);
                return Err(error);
            }
        };
        Ok((
            SubHandle {
                id,
                channel: channel.to_vec(),
            },
            receiver,
            failure,
            subscribed_epoch,
        ))
    }

    async fn ensure(&self, channel: &[u8]) -> Result<u64, TransportError> {
        let _gate = self.dial_gate.lock().await;
        loop {
            let epoch = self.endpoint.current_master_epoch();
            let (need_dial, generation) = {
                let st = lock(&self.state);
                let need = st.connection.is_none() || st.subscribed_epoch != epoch;
                (need, st.generation + 1)
            };
            if need_dial {
                let conn = match tokio::time::timeout(self.io_timeout, self.dial(generation)).await
                {
                    Ok(Ok(conn)) => conn,
                    Ok(Err(error)) => return Err(error),
                    Err(_) => return Err(TransportError::Timeout),
                };
                let desired: Vec<Vec<u8>> = {
                    let mut st = lock(&self.state);
                    // A disconnect during the dial invalidated this generation; retry.
                    if st.generation + 1 != generation {
                        continue;
                    }
                    st.generation = generation;
                    st.connection = Some(conn);
                    st.subscribed_epoch = epoch;
                    st.subscribed.clear();
                    st.routes.keys().cloned().collect()
                };
                let pending: Vec<Vec<u8>> = {
                    let mut st = lock(&self.state);
                    st.pending_unsubscribe.drain().collect()
                };
                let conn = {
                    let st = lock(&self.state);
                    match st.connection.as_ref() {
                        Some(Conn::Simple(c)) => Conn::Simple(c.clone()),
                        Some(Conn::Cluster(c)) => Conn::Cluster(c.clone()),
                        None => continue,
                    }
                };
                if !pending.is_empty() {
                    let _ = tokio::time::timeout(self.io_timeout, conn.unsubscribe(&pending)).await;
                }
                tokio::time::timeout(self.io_timeout, conn.subscribe(&desired))
                    .await
                    .map_err(|_| TransportError::Timeout)?
                    .map_err(|_| TransportError::Redis)?;
                let mut st = lock(&self.state);
                if st.generation != generation {
                    continue;
                }
                st.subscribed.extend(desired);
                return Ok(epoch);
            }
            // Connection live: flush any deferred unsubscribes, then subscribe this
            // channel only if it is not already acknowledged on this generation.
            let pending: Vec<Vec<u8>> = {
                let mut st = lock(&self.state);
                st.pending_unsubscribe.drain().collect()
            };
            let already = {
                let st = lock(&self.state);
                st.subscribed.contains(channel)
            };
            let conn = {
                let st = lock(&self.state);
                match st.connection.as_ref() {
                    Some(Conn::Simple(c)) => Conn::Simple(c.clone()),
                    Some(Conn::Cluster(c)) => Conn::Cluster(c.clone()),
                    None => continue,
                }
            };
            if !pending.is_empty() {
                let _ = tokio::time::timeout(self.io_timeout, conn.unsubscribe(&pending)).await;
            }
            if !already {
                tokio::time::timeout(self.io_timeout, conn.subscribe(&[channel.to_vec()]))
                    .await
                    .map_err(|_| TransportError::Timeout)?
                    .map_err(|_| TransportError::Redis)?;
                let mut st = lock(&self.state);
                if st.connection.is_none() {
                    continue;
                }
                st.subscribed.insert(channel.to_vec());
            }
            return Ok(self.endpoint.current_master_epoch().max(epoch));
        }
    }

    fn remove_route(&self, id: u64, channel: &[u8]) -> bool {
        let mut st = lock(&self.state);
        let mut channel_empty = false;
        if let Some(routes) = st.routes.get_mut(channel) {
            routes.retain(|route| route.id != id);
            if routes.is_empty() {
                st.routes.remove(channel);
                channel_empty = true;
            }
        }
        if channel_empty && st.subscribed.contains(channel) {
            st.subscribed.remove(channel);
            if st.connection.is_some() {
                st.pending_unsubscribe.insert(channel.to_vec());
            }
        }
        if st.routes.is_empty() {
            // Last registration gone: drop the shared connection (closes the socket),
            // matching the dedicated-connection drop contract.
            st.connection = None;
            st.subscribed.clear();
            st.pending_unsubscribe.clear();
            st.generation += 1;
        }
        channel_empty
    }

    /// Synchronous release used by `Drop` and recovery: remove the route, drop the
    /// shared connection when nothing remains. A departed channel that still shares a
    /// live connection is queued for UNSUBSCRIBE on the next reconcile.
    pub fn release(&self, handle: &SubHandle) {
        self.remove_route(handle.id, &handle.channel);
    }

    /// Explicit unsubscribe: remove the route and immediately UNSUBSCRIBE the channel
    /// when it was the last registration, dropping the connection when nothing remains.
    pub async fn unsubscribe(&self, handle: &SubHandle) -> Result<(), TransportError> {
        let channel_empty = self.remove_route(handle.id, &handle.channel);
        if !channel_empty {
            return Ok(());
        }
        let _gate = self.dial_gate.lock().await;
        let conn = {
            let mut st = lock(&self.state);
            st.pending_unsubscribe.remove(&handle.channel);
            match st.connection.as_ref() {
                Some(Conn::Simple(c)) => Some(Conn::Simple(c.clone())),
                Some(Conn::Cluster(c)) => Some(Conn::Cluster(c.clone())),
                None => None,
            }
        };
        if let Some(conn) = conn {
            tokio::time::timeout(
                self.io_timeout,
                conn.unsubscribe(std::slice::from_ref(&handle.channel)),
            )
            .await
            .map_err(|_| TransportError::Timeout)?
            .map_err(|_| TransportError::Redis)?;
        }
        Ok(())
    }

    /// Test barrier: PING the shared subscription connection so every push queued
    /// before this call has been dispatched before the caller drains its inbox.
    #[cfg(test)]
    pub async fn test_barrier(&self) -> Result<(), TransportError> {
        let conn = {
            let st = lock(&self.state);
            match st.connection.as_ref() {
                Some(Conn::Simple(c)) => Conn::Simple(c.clone()),
                Some(Conn::Cluster(c)) => Conn::Cluster(c.clone()),
                None => return Err(TransportError::Disconnected),
            }
        };
        let pong = match conn {
            Conn::Simple(mut c) => tokio::time::timeout(
                self.io_timeout,
                redis::cmd("PING").query_async::<String>(&mut c),
            )
            .await
            .map_err(|_| TransportError::Timeout)?
            .map_err(|_| TransportError::Redis)?,
            Conn::Cluster(mut c) => tokio::time::timeout(
                self.io_timeout,
                redis::cmd("PING").query_async::<String>(&mut c),
            )
            .await
            .map_err(|_| TransportError::Timeout)?
            .map_err(|_| TransportError::Redis)?,
        };
        if pong != "PONG" {
            return Err(TransportError::Redis);
        }
        Ok(())
    }

    #[cfg(test)]
    pub fn live_connection_count(&self) -> usize {
        let st = lock(&self.state);
        if st.connection.is_some() { 1 } else { 0 }
    }
}
