//! Real Redis >=6 RESP3 Pub/Sub transport. Each subscription owns one bounded
//! push queue and connection. Long-lived RPC owners can explicitly recover
//! failed subscriptions; raw callers retain failure-closed receive semantics.

use super::channels::Channel;
use super::wire::{self, BusMessage, CompressionPolicy, WireLimits};
use crate::redis_endpoint::{RedisEndpoint, SharedConnection};
use base64::{Engine, engine::general_purpose::STANDARD};
use redis::aio::MultiplexedConnection;
use redis::cluster_async::ClusterConnection;
use redis::{AsyncConnectionConfig, PushInfo, PushKind, Value};
use sha2::{Digest, Sha256};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{OwnedSemaphorePermit, Semaphore, mpsc, watch};

#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
pub enum TransportError {
    #[error("invalid Redis transport limits or connection configuration")]
    Configuration,
    #[error("Redis operation failed")]
    Redis,
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
    limits: TransportLimits,
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
            limits: TransportLimits {
                inbox_messages: size,
                ..self.limits
            },
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
        let command = endpoint
            .connect(&config(limits))
            .await
            .map_err(|_| TransportError::Redis)?;
        Ok(Arc::new(Self {
            endpoint,
            command,
            limits,
            subscriptions: Arc::new(Semaphore::new(limits.subscriptions)),
            commands: Arc::new(Semaphore::new(limits.commands)),
        }))
    }

    pub fn limits(&self) -> TransportLimits {
        self.limits
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
        let (sender, receiver) = mpsc::channel(self.limits.inbox_messages);
        let (fault, failure) = watch::channel(None);
        let expected = channel.legacy.as_bytes().to_vec();
        let cap = self.limits.wire.encoded_bytes;
        let push = move |mut info: PushInfo| -> Result<(), ()> {
            match info.kind {
                PushKind::Disconnection => {
                    fault.send_replace(Some(TransportError::Disconnected));
                }
                PushKind::Message => {
                    if info.data.len() != 2 {
                        fault.send_replace(Some(TransportError::Disconnected));
                        return Err(());
                    }
                    let payload = info.data.pop().unwrap();
                    let actual_channel = info.data.pop().unwrap();
                    if !matches!(actual_channel, Value::BulkString(bytes) if bytes == expected) {
                        return Ok(());
                    }
                    let Value::BulkString(bytes) = payload else {
                        fault.send_replace(Some(TransportError::Disconnected));
                        return Err(());
                    };
                    if bytes.len() > cap {
                        fault.send_replace(Some(TransportError::Oversized));
                        return Err(());
                    }
                    if sender.try_send(bytes).is_err() {
                        fault.send_replace(Some(TransportError::Overflow));
                        return Err(());
                    }
                }
                _ => {}
            }
            Ok(())
        };
        let connection = if self.endpoint.is_cluster() {
            // Cluster: build ClusterClient with push_sender bound to our queue.
            let builder = self
                .endpoint
                .cluster_builder()
                .ok_or(TransportError::Configuration)?;
            let client = builder
                .push_sender(push)
                .build()
                .map_err(|_| TransportError::Redis)?;
            let mut conn = client
                .get_async_connection()
                .await
                .map_err(|_| TransportError::Redis)?;
            tokio::time::timeout(self.limits.io_timeout, conn.subscribe(&channel.legacy))
                .await
                .map_err(|_| TransportError::Timeout)?
                .map_err(|_| TransportError::Redis)?;
            SubscriptionConnection::Cluster(conn)
        } else {
            // Simple/Sentinel: existing MultiplexedConnection path.
            let mut connection = self
                .endpoint
                .dial(&config(self.limits).set_push_sender(push))
                .await
                .map_err(|_| TransportError::Redis)?;
            tokio::time::timeout(
                self.limits.io_timeout,
                connection.subscribe(&channel.legacy),
            )
            .await
            .map_err(|_| TransportError::Timeout)?
            .map_err(|_| TransportError::Redis)?;
            SubscriptionConnection::Simple(connection)
        };
        // For cluster, we still need the fault channel reference — but the push
        // closure already captured `fault`. For simple/sentinel it was consumed by
        // set_push_sender. The watch below only reads.
        Ok(Subscription {
            connection: Some(connection),
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

/// Dropping this object drops its dedicated subscription connection and permit.
/// Overflow/disconnect fails closed; no invisible reconnect/replay is attempted.
pub struct Subscription {
    connection: Option<SubscriptionConnection>,
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

enum SubscriptionConnection {
    Simple(MultiplexedConnection),
    Cluster(ClusterConnection),
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
        if self.connection.is_none() {
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
                // The old socket and its admission permit must be gone before
                // attempting replacement, even when subscription capacity is 1.
                self.connection.take();
                self.admission.pending = None;
                self._permit.take();
                self.receiver.close();
                self.recovery_delay = Duration::from_millis(100);
                Ok(SubscriptionEvent::Disconnected)
            }
            Err(error) => Err(error),
        }
    }

    pub async fn unsubscribe(self, channel: &Channel) -> Result<(), TransportError> {
        let io_timeout = self.bus.limits.io_timeout;
        match self.connection {
            None => Ok(()),
            Some(SubscriptionConnection::Simple(mut conn)) => {
                tokio::time::timeout(io_timeout, conn.unsubscribe(&channel.legacy))
                    .await
                    .map_err(|_| TransportError::Timeout)?
                    .map_err(|_| TransportError::Redis)
            }
            Some(SubscriptionConnection::Cluster(mut conn)) => {
                tokio::time::timeout(io_timeout, conn.unsubscribe(&channel.legacy))
                    .await
                    .map_err(|_| TransportError::Timeout)?
                    .map_err(|_| TransportError::Redis)
            }
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
        let pong = match self
            .connection
            .as_mut()
            .ok_or(TransportError::Disconnected)?
        {
            SubscriptionConnection::Simple(conn) => tokio::time::timeout(
                self.bus.limits.io_timeout,
                redis::cmd("PING").query_async::<String>(conn),
            )
            .await
            .map_err(|_| TransportError::Timeout)?
            .map_err(|_| TransportError::Redis)?,
            SubscriptionConnection::Cluster(conn) => tokio::time::timeout(
                self.bus.limits.io_timeout,
                redis::cmd("PING").query_async::<String>(conn),
            )
            .await
            .map_err(|_| TransportError::Timeout)?
            .map_err(|_| TransportError::Redis)?,
        };
        if pong != "PONG" {
            return Err(TransportError::Redis);
        }
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
