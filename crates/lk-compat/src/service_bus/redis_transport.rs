//! Real Redis >=6 RESP3 Pub/Sub transport. One shared multiplexed pub/sub
//! connection per transport carries every subscription (the locked reference's
//! `redisbus` shape); each subscription still owns one bounded push inbox and a
//! fail-closed fault watch. Long-lived RPC owners can explicitly recover failed
//! subscriptions; raw callers retain failure-closed receive semantics.

use super::channels::Channel;
use super::wire::{self, BusMessage, CompressionPolicy, WireLimits};
use crate::redis_endpoint::{RedisEndpoint, SharedConnection, go_dial_error_text, go_error_text};
use base64::{Engine, engine::general_purpose::STANDARD};
use redis::AsyncConnectionConfig;
use sha2::{Digest, Sha256};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{OwnedSemaphorePermit, Semaphore, mpsc, watch};

mod shared_sub;
use shared_sub::{SharedSubscriptions, SubHandle};

#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
pub enum TransportError {
    #[error("invalid Redis transport limits or connection configuration")]
    Configuration,
    #[error("Redis operation failed")]
    Redis,
    /// A startup command-connection dial that reached the wire and failed. The
    /// payload is the Go `net.OpError` dial text (e.g.
    /// `dial tcp 127.0.0.1:6379: connect: connection refused`), so the
    /// `GetRedisClient` caller can wrap it with `unable to connect to redis: `
    /// exactly as upstream does. Distinct from the payload-less [`Self::Redis`]
    /// a mid-operation command or subscription failure reports.
    #[error("{0}")]
    Connect(String),
    #[error("Redis operation timed out")]
    Timeout,
    #[error("Redis subscription disconnected")]
    Disconnected,
    #[error("Redis subscription queue overflow")]
    Overflow,
    #[error("Redis message exceeds configured envelope limit")]
    Oversized,
    #[error("Redis transport capacity exhausted")]
    Capacity,
    #[error("psrpc envelope encoding or decoding failed")]
    Codec,
}

#[derive(Clone, Copy, Debug)]
pub struct TransportLimits {
    pub wire: WireLimits,
    pub compression: CompressionPolicy,
    pub inbox_messages: usize,
    pub subscriptions: usize,
    pub commands: usize,
    pub io_timeout: Duration,
}

pub struct RedisTransport {
    endpoint: RedisEndpoint,
    command: SharedConnection,
    shared: Arc<SharedSubscriptions>,
    limits: TransportLimits,
    /// Source GZIP-only receive cap (`psrpc.max_decompressed_size`), kept separate
    /// from the independent plain `limits.wire.decompressed_bytes` bound. NONE
    /// payloads and publish are never capped by it (Go redisbus maxSize contract).
    gzip_limit: usize,
    subscriptions: Arc<Semaphore>,
    commands: Arc<Semaphore>,
}

fn config(limits: TransportLimits) -> AsyncConnectionConfig {
    AsyncConnectionConfig::new()
        .set_connection_timeout(Some(limits.io_timeout))
        .set_response_timeout(Some(limits.io_timeout))
        .set_pipeline_buffer_size(limits.commands)
        .set_concurrency_limit(limits.commands)
}

impl RedisTransport {
    /// A generated client's subscription capacity view. Share the exact same
    /// endpoint, command connection and admission semaphores; never redial or
    /// construct a separate transport identity just to change channel size.
    pub(crate) fn with_inbox_size(
        self: &Arc<Self>,
        size: usize,
    ) -> Result<Arc<Self>, TransportError> {
        if size == 0 || size > Semaphore::MAX_PERMITS {
            return Err(TransportError::Configuration);
        }
        Ok(Arc::new(Self {
            endpoint: self.endpoint.clone(),
            command: self.command.clone(),
            shared: self.shared.clone(),
            limits: TransportLimits {
                inbox_messages: size,
                ..self.limits
            },
            gzip_limit: self.gzip_limit,
            subscriptions: self.subscriptions.clone(),
            commands: self.commands.clone(),
        }))
    }
    pub async fn connect(url: &str, limits: TransportLimits) -> Result<Arc<Self>, TransportError> {
        let client = redis::Client::open(url).map_err(|_| TransportError::Configuration)?;
        Self::connect_endpoint(
            RedisEndpoint::simple(client.get_connection_info().clone()),
            limits,
        )
        .await
    }

    /// RESP3 is required for the bounded push queue used by subscriptions.
    /// Native connection settings keep credentials out of URL formatting and
    /// preserve the selected database/ACL identity at service assembly. A
    /// Sentinel endpoint resolves the current master before the command
    /// connection dials (SPEC-0074 C11).
    pub async fn connect_endpoint(
        endpoint: RedisEndpoint,
        limits: TransportLimits,
    ) -> Result<Arc<Self>, TransportError> {
        if limits.inbox_messages == 0
            || limits.subscriptions == 0
            || limits.commands == 0
            || limits.subscriptions > Semaphore::MAX_PERMITS
            || limits.commands > Semaphore::MAX_PERMITS
            || limits.io_timeout.is_zero()
            || limits.wire.encoded_bytes == 0
        {
            return Err(TransportError::Configuration);
        }
        // RESP3 pushes allow a genuinely bounded user queue, unlike redis-rs's
        // RESP2 PubSub helper (which internally uses an unbounded message queue).
        let endpoint = endpoint.with_resp3();
        let command = endpoint.connect(&config(limits)).await.map_err(|error| {
            // Preserve the dial-layer detail so the startup `GetRedisClient`
            // caller can wrap `unable to connect to redis: <net.OpError>` exactly
            // as upstream does, instead of the payload-less "Redis operation
            // failed" a mid-operation failure reports. A Sentinel endpoint has no
            // single static dial address (it resolves the master per dial), so it
            // falls back to the error's own reply text.
            TransportError::Connect(match endpoint.dial_address() {
                Some(address) => go_dial_error_text(&error, &address),
                None => go_error_text(&error),
            })
        })?;
        let shared = Arc::new(SharedSubscriptions::new(endpoint.clone(), limits));
        Ok(Arc::new(Self {
            endpoint,
            command,
            shared,
            limits,
            gzip_limit: limits.wire.decompressed_bytes,
            subscriptions: Arc::new(Semaphore::new(limits.subscriptions)),
            commands: Arc::new(Semaphore::new(limits.commands)),
        }))
    }

    /// The shared command connection identity. Server assembly clones this into
    /// the node registry, IO store and SIP catalog so the whole process rides one
    /// pooled command connection, mirroring upstream's single borrowed
    /// `redis.UniversalClient` rather than one eager dial per consumer.
    pub fn command_connection(&self) -> SharedConnection {
        self.command.clone()
    }

    pub fn endpoint(&self) -> &RedisEndpoint {
        &self.endpoint
    }

    /// Build a transport that reuses an already-dialled shared command connection
    /// (the one the startup Ping and SIP catalog used) and owns a fresh shared
    /// pub/sub multiplexer for this endpoint. The command connection must already
    /// negotiate RESP3; the subscription multiplexer dials its own RESP3 connection
    /// lazily on first subscribe, exactly like `redisbus.New`'s `rc.Subscribe`.
    pub async fn from_command(
        endpoint: RedisEndpoint,
        limits: TransportLimits,
        command: SharedConnection,
    ) -> Result<Arc<Self>, TransportError> {
        if limits.inbox_messages == 0
            || limits.subscriptions == 0
            || limits.commands == 0
            || limits.subscriptions > Semaphore::MAX_PERMITS
            || limits.commands > Semaphore::MAX_PERMITS
            || limits.io_timeout.is_zero()
            || limits.wire.encoded_bytes == 0
        {
            return Err(TransportError::Configuration);
        }
        let endpoint = endpoint.with_resp3();
        let shared = Arc::new(SharedSubscriptions::new(endpoint.clone(), limits));
        Ok(Arc::new(Self {
            endpoint,
            command,
            shared,
            limits,
            gzip_limit: limits.wire.decompressed_bytes,
            subscriptions: Arc::new(Semaphore::new(limits.subscriptions)),
            commands: Arc::new(Semaphore::new(limits.commands)),
        }))
    }

    /// A second transport identity sharing the exact same endpoint, command
    /// connection, pub/sub multiplexer and admission semaphores, but with its own
    /// wire/inbox limits. The keepalive/cluster bus rides the worker transport's
    /// single command + subscription connection instead of redialling a pair.
    pub fn with_limits(&self, limits: TransportLimits) -> Result<Arc<Self>, TransportError> {
        if limits.inbox_messages == 0
            || limits.subscriptions == 0
            || limits.commands == 0
            || limits.subscriptions > Semaphore::MAX_PERMITS
            || limits.commands > Semaphore::MAX_PERMITS
            || limits.io_timeout.is_zero()
            || limits.wire.encoded_bytes == 0
        {
            return Err(TransportError::Configuration);
        }
        Ok(Arc::new(Self {
            endpoint: self.endpoint.clone(),
            command: self.command.clone(),
            shared: self.shared.clone(),
            limits,
            gzip_limit: self.gzip_limit,
            subscriptions: self.subscriptions.clone(),
            commands: self.commands.clone(),
        }))
    }

    pub fn limits(&self) -> TransportLimits {
        self.limits
    }
    pub(crate) fn gzip_limit(&self) -> usize {
        self.gzip_limit
    }
    /// A second transport identity sharing the same endpoint, command connection,
    /// pub/sub multiplexer and admission semaphores, but with the source GZIP-only
    /// receive cap set independently of the plain wire bound. `0` means "no separate
    /// cap" and falls back to `limits.wire.decompressed_bytes`, exactly like the
    /// local bus. NONE payloads and publish are never capped by it.
    pub fn with_gzip_limit(&self, gzip_limit: usize) -> Result<Arc<Self>, TransportError> {
        Ok(Arc::new(Self {
            endpoint: self.endpoint.clone(),
            command: self.command.clone(),
            shared: self.shared.clone(),
            limits: self.limits,
            gzip_limit: if gzip_limit == 0 {
                self.limits.wire.decompressed_bytes
            } else {
                gzip_limit
            },
            subscriptions: self.subscriptions.clone(),
            commands: self.commands.clone(),
        }))
    }
    pub fn active_subscriptions(&self) -> usize {
        self.limits.subscriptions - self.subscriptions.available_permits()
    }

    /// The returned object is ready only after Redis acknowledges SUBSCRIBE.
    pub async fn subscribe(
        self: &Arc<Self>,
        channel: &Channel,
        queue: bool,
    ) -> Result<Subscription, TransportError> {
        loop {
            let subscription = self.subscribe_generation(channel, queue).await?;
            // A Sentinel switch between dial and SUBSCRIBE ACK cannot install
            // a subscription on the former master as the current generation.
            if subscription
                .master_epoch
                .as_ref()
                .is_none_or(|epoch| *epoch.borrow() == subscription.subscribed_master_epoch)
            {
                return Ok(subscription);
            }
            drop(subscription);
        }
    }

    async fn subscribe_generation(
        self: &Arc<Self>,
        channel: &Channel,
        queue: bool,
    ) -> Result<Subscription, TransportError> {
        let master_epoch = self.endpoint.master_epoch();
        let subscribed_master_epoch = master_epoch.as_ref().map_or(0, |epoch| *epoch.borrow());
        let permit = self
            .subscriptions
            .clone()
            .try_acquire_owned()
            .map_err(|_| TransportError::Capacity)?;
        let cap = self.limits.wire.encoded_bytes;
        let (handle, receiver, failure, _epoch) = self
            .shared
            .subscribe(channel.legacy.as_bytes(), self.limits.inbox_messages, cap)
            .await?;
        Ok(Subscription {
            handle: Some(handle),
            channel: channel.clone(),
            master_epoch,
            subscribed_master_epoch,
            recovery_delay: Duration::from_millis(100),
            admission: QueueAdmission::default(),
            #[cfg(test)]
            admission_pause: None,
            receiver,
            failure,
            queue,
            bus: self.clone(),
            _permit: Some(permit),
        })
    }

    /// Waits for the real PUBLISH command reply; subscriber count is not proof
    /// that a handler accepted or completed the business operation.
    pub async fn publish<T: BusMessage>(
        &self,
        channel: &Channel,
        message: &T,
    ) -> Result<u64, TransportError> {
        let bytes = wire::encode(
            message,
            &channel.local,
            self.limits.compression,
            self.limits.wire,
        )
        .map_err(|_| TransportError::Codec)?;
        self.publish_encoded(channel, &bytes).await
    }

    pub async fn publish_encoded(
        &self,
        channel: &Channel,
        bytes: &[u8],
    ) -> Result<u64, TransportError> {
        if bytes.len() > self.limits.wire.encoded_bytes {
            return Err(TransportError::Oversized);
        }
        let _permit = self
            .commands
            .clone()
            .try_acquire_owned()
            .map_err(|_| TransportError::Capacity)?;
        let mut connection = self.command.begin();
        tokio::time::timeout(
            self.limits.io_timeout,
            redis::cmd("PUBLISH")
                .arg(&channel.legacy)
                .arg(bytes)
                .query_async::<u64>(&mut connection),
        )
        .await
        .map_err(|_| TransportError::Timeout)?
        .map_err(|_| TransportError::Redis)
    }

    async fn acquire_queue(&self, bytes: &[u8]) -> Result<bool, TransportError> {
        let _permit = self
            .commands
            .clone()
            .try_acquire_owned()
            .map_err(|_| TransportError::Capacity)?;
        let key = STANDARD.encode(Sha256::digest(bytes));
        let mut connection = self.command.begin();
        let acquired: Option<String> = tokio::time::timeout(
            self.limits.io_timeout,
            redis::cmd("SET")
                .arg(key)
                .arg("1")
                .arg("NX")
                .arg("EX")
                .arg(5)
                .query_async(&mut connection),
        )
        .await
        .map_err(|_| TransportError::Timeout)?
        .map_err(|_| TransportError::Redis)?;
        Ok(acquired.is_some())
    }
}

/// Dropping this object releases its route on the shared pub/sub connection and
/// its permit; the shared socket closes once the last registration is gone.
/// Overflow/disconnect fails closed; no invisible reconnect/replay is attempted.
pub struct Subscription {
    handle: Option<SubHandle>,
    channel: Channel,
    master_epoch: Option<watch::Receiver<u64>>,
    subscribed_master_epoch: u64,
    recovery_delay: Duration,
    admission: QueueAdmission,
    #[cfg(test)]
    admission_pause: Option<(
        tokio::sync::oneshot::Sender<bool>,
        tokio::sync::oneshot::Receiver<()>,
    )>,
    receiver: mpsc::Receiver<Vec<u8>>,
    failure: watch::Receiver<Option<TransportError>>,
    queue: bool,
    bus: Arc<RedisTransport>,
    _permit: Option<OwnedSemaphorePermit>,
}

/// A queue item and its single in-flight SET NX belong to the subscription,
/// rather than to one cancellable recv invocation. Retrying SET NX after a
/// successful-but-not-yet-observed reply would suppress our own accepted item.
#[derive(Default)]
struct QueueAdmission {
    pending:
        Option<futures_util::future::BoxFuture<'static, (Vec<u8>, Result<bool, TransportError>)>>,
}
impl QueueAdmission {
    async fn finish(&mut self) -> (Vec<u8>, Result<bool, TransportError>) {
        let result = self.pending.as_mut().expect("pending admission").await;
        self.pending = None;
        result
    }
}

/// A recovery boundary is observable so business caches never survive missed
/// invalidations. Restored means the replacement SUBSCRIBE was acknowledged.
pub(crate) enum SubscriptionEvent {
    Message(Vec<u8>),
    Disconnected,
    Restored,
}

impl Subscription {
    pub fn failure(&self) -> Option<TransportError> {
        self.failure.borrow().clone()
    }
    pub async fn recv(&mut self) -> Result<Vec<u8>, TransportError> {
        loop {
            if let Some(error) = self.failure.borrow().clone() {
                return Err(error);
            }
            if self
                .master_epoch
                .as_ref()
                .is_some_and(|epoch| *epoch.borrow() != self.subscribed_master_epoch)
            {
                return Err(TransportError::Disconnected);
            }
            if self.admission.pending.is_some() {
                let (bytes, acquired) = self.admission.finish().await;
                // A Sentinel transition or failed socket during SET NX cannot
                // turn old-generation queued work into a restored delivery.
                if let Some(error) = self.failure.borrow().clone() {
                    return Err(error);
                }
                if self
                    .master_epoch
                    .as_ref()
                    .is_some_and(|epoch| *epoch.borrow() != self.subscribed_master_epoch)
                {
                    return Err(TransportError::Disconnected);
                }
                if acquired? {
                    return Ok(bytes);
                }
                continue;
            }
            let bytes = tokio::select! {
                biased;
                changed = async {
                    match self.master_epoch.as_mut() {
                        Some(epoch) => epoch.changed().await,
                        None => std::future::pending().await,
                    }
                } => {
                    if changed.is_err() { self.master_epoch = None; }
                    continue;
                }
                changed = self.failure.changed() => {
                    if changed.is_err() { return Err(TransportError::Disconnected); }
                    continue;
                }
                bytes = self.receiver.recv() => bytes.ok_or(TransportError::Disconnected)?,
            };
            if !self.queue {
                return Ok(bytes);
            }
            let bus = self.bus.clone();
            #[cfg(test)]
            let pause = self.admission_pause.take();
            self.admission.pending = Some(Box::pin(async move {
                let acquired = bus.acquire_queue(&bytes).await;
                #[cfg(test)]
                if let Some((entered, release)) = pause {
                    let _ = entered.send(acquired.as_ref().copied().unwrap_or(false));
                    let _ = release.await;
                }
                (bytes, acquired)
            }));
        }
    }

    /// Recover inside the owner's receive future; dropping it cancels retries.
    /// No message replay or business acknowledgement is synthesized.
    pub(crate) async fn recv_recovering(&mut self) -> Result<SubscriptionEvent, TransportError> {
        if self.handle.is_none() {
            loop {
                tokio::time::sleep(self.recovery_delay).await;
                let bus = self.bus.clone();
                let recovered = tokio::time::timeout(
                    bus.limits.io_timeout,
                    bus.subscribe(&self.channel, self.queue),
                )
                .await;
                match recovered {
                    Ok(Ok(replacement)) => {
                        *self = replacement;
                        return Ok(SubscriptionEvent::Restored);
                    }
                    Ok(Err(
                        TransportError::Configuration
                        | TransportError::Oversized
                        | TransportError::Codec,
                    )) => return Err(TransportError::Configuration),
                    _ => {
                        self.recovery_delay = (self.recovery_delay * 2).min(Duration::from_secs(2));
                    }
                }
            }
        }
        match self.recv().await {
            Ok(bytes) => Ok(SubscriptionEvent::Message(bytes)),
            Err(TransportError::Disconnected | TransportError::Redis | TransportError::Timeout) => {
                // The old route and its admission permit must be gone before
                // attempting replacement, even when subscription capacity is 1.
                if let Some(handle) = self.handle.take() {
                    self.bus.shared.release(&handle);
                }
                self.admission.pending = None;
                self._permit.take();
                self.receiver.close();
                self.recovery_delay = Duration::from_millis(100);
                Ok(SubscriptionEvent::Disconnected)
            }
            Err(error) => Err(error),
        }
    }

    pub async fn unsubscribe(mut self, _channel: &Channel) -> Result<(), TransportError> {
        match self.handle.take() {
            None => Ok(()),
            Some(handle) => self.bus.shared.unsubscribe(&handle).await,
        }
    }
}

impl Drop for Subscription {
    fn drop(&mut self) {
        if let Some(handle) = self.handle.take() {
            self.bus.shared.release(&handle);
        }
    }
}

// Test-only protocol barriers: no synthetic jobs/replies and no queue discard.
#[cfg(test)]
impl RedisTransport {
    pub(crate) async fn test_command_barrier(&self) -> Result<(), TransportError> {
        let mut connection = self.command.begin();
        let pong = tokio::time::timeout(
            self.limits.io_timeout,
            redis::cmd("PING").query_async::<String>(&mut connection),
        )
        .await
        .map_err(|_| TransportError::Timeout)?
        .map_err(|_| TransportError::Redis)?;
        if pong != "PONG" {
            return Err(TransportError::Redis);
        }
        Ok(())
    }
}
#[cfg(test)]
impl Subscription {
    pub(crate) async fn test_receive_barrier(&mut self) -> Result<Vec<Vec<u8>>, TransportError> {
        self.bus.shared.test_barrier().await?;
        if let Some(error) = self.failure() {
            return Err(error);
        }
        let mut queued = Vec::new();
        loop {
            match self.receiver.try_recv() {
                Ok(raw) => queued.push(raw),
                Err(tokio::sync::mpsc::error::TryRecvError::Empty) => break,
                Err(tokio::sync::mpsc::error::TryRecvError::Disconnected) => {
                    return Err(TransportError::Disconnected);
                }
            }
        }
        Ok(queued)
    }
}

#[cfg(test)]
#[path = "redis_transport/cancellation_tests.rs"]
mod cancellation_tests;

#[cfg(test)]
#[path = "redis_transport/shared_sub_tests.rs"]
mod shared_sub_tests;
