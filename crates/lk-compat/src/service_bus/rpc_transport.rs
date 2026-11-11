//! Same RPC codec/client/server lifecycle over Redis or a real process-local bus.
use super::channels::Channel;
use super::local_transport::{LocalRetirement, LocalTransport};
use super::redis_transport::{RedisTransport, Subscription, TransportError, TransportLimits};
use super::wire::{self, BusMessage};
use std::sync::Arc;
#[derive(Clone)]
pub enum RpcBus {
    Redis(Arc<RedisTransport>),
    Local(Arc<LocalTransport>),
}
impl From<Arc<RedisTransport>> for RpcBus {
    fn from(value: Arc<RedisTransport>) -> Self {
        Self::Redis(value)
    }
}
impl From<Arc<LocalTransport>> for RpcBus {
    fn from(value: Arc<LocalTransport>) -> Self {
        Self::Local(value)
    }
}
impl RpcBus {
    /// Per-generated-client inbox capacity with the existing bus membership,
    /// Redis dial plan, command pool and subscription/command slots preserved.
    pub(crate) fn with_inbox_size(&self, size: usize) -> Result<Self, TransportError> {
        match self {
            Self::Redis(bus) => Ok(Self::Redis(bus.with_inbox_size(size)?)),
            Self::Local(bus) => Ok(Self::Local(bus.with_inbox_size(size)?)),
        }
    }
    pub(crate) fn is_local(&self) -> bool {
        matches!(self, Self::Local(_))
    }
    pub fn limits(&self) -> TransportLimits {
        match self {
            Self::Redis(bus) => bus.limits(),
            Self::Local(bus) => bus.limits(),
        }
    }
    pub(crate) fn decode<T: BusMessage>(
        &self,
        bytes: &[u8],
    ) -> Result<wire::Decoded<T>, wire::WireError> {
        match self {
            Self::Redis(_) => wire::decode(bytes, self.limits().wire),
            Self::Local(_) => wire::decode_psrpc(bytes, self.limits().wire),
        }
    }
    pub async fn subscribe<T: BusMessage + Send + 'static>(
        &self,
        channel: &Channel,
        queue: bool,
    ) -> Result<RpcSubscription, TransportError> {
        match self {
            Self::Redis(bus) => Ok(RpcSubscription::Redis(bus.subscribe(channel, queue).await?)),
            Self::Local(bus) => Ok(RpcSubscription::Local(
                DecodedLocalSubscription::new::<T>(
                    bus,
                    channel,
                    queue,
                    bus.limits().inbox_messages,
                )
                .await,
            )),
        }
    }
    pub(crate) async fn subscribe_server<T: BusMessage + Send + 'static>(
        &self,
        channel: &Channel,
        queue: bool,
    ) -> Result<RpcSubscription, TransportError> {
        match self {
            Self::Redis(bus) => Ok(RpcSubscription::Redis(bus.subscribe(channel, queue).await?)),
            Self::Local(bus) => Ok(RpcSubscription::Local(
                DecodedLocalSubscription::new::<T>(bus, channel, queue, 100).await,
            )),
        }
    }
    pub async fn publish<T: BusMessage>(
        &self,
        channel: &Channel,
        message: &T,
    ) -> Result<u64, TransportError> {
        match self {
            Self::Redis(bus) => bus.publish(channel, message).await,
            Self::Local(_) => {
                let limits = self.limits();
                let bytes = wire::encode(message, &channel.local, limits.compression, limits.wire)
                    .map_err(|_| TransportError::Codec)?;
                self.publish_encoded(channel, &bytes).await
            }
        }
    }
    pub async fn publish_encoded(
        &self,
        channel: &Channel,
        bytes: &[u8],
    ) -> Result<u64, TransportError> {
        match self {
            Self::Redis(bus) => bus.publish_encoded(channel, bytes).await,
            Self::Local(bus) => bus.publish_encoded(channel, bytes).await,
        }
    }
    #[cfg(test)]
    pub(crate) async fn test_command_barrier(&self) -> Result<(), TransportError> {
        match self {
            Self::Redis(bus) => bus.test_command_barrier().await,
            Self::Local(_) => Ok(()),
        }
    }
}
pub enum RpcSubscription {
    Redis(Subscription),
    Local(DecodedLocalSubscription),
}
impl RpcSubscription {
    #[cfg(test)]
    pub(crate) fn local_retirement(&self) -> Option<LocalRetirement> {
        match self {
            Self::Local(s) => Some(s.retirement()),
            Self::Redis(_) => None,
        }
    }

    pub async fn recv(&mut self) -> Result<Vec<u8>, TransportError> {
        loop {
            if let super::redis_transport::SubscriptionEvent::Message(bytes) =
                self.recv_event().await?
            {
                return Ok(bytes);
            }
        }
    }
    /// Receive recovery boundaries when missed messages invalidate owner state.
    pub(crate) async fn recv_event(
        &mut self,
    ) -> Result<super::redis_transport::SubscriptionEvent, TransportError> {
        match self {
            Self::Redis(s) => s.recv_recovering().await,
            Self::Local(s) => s
                .recv()
                .await
                .map(super::redis_transport::SubscriptionEvent::Message),
        }
    }
    pub async fn unsubscribe(self, channel: &Channel) -> Result<(), TransportError> {
        match self {
            Self::Redis(s) => s.unsubscribe(channel).await,
            Self::Local(s) => s.unsubscribe(channel).await,
        }
    }
}

/// Source bus.subscription: independent raw Reader(size), one forwarding
/// goroutine and typed Channel(size). Preserve original envelope bytes after
/// validation so unknown cohorts and response provenance are never re-encoded.
pub struct DecodedLocalSubscription {
    retirement: LocalRetirement,
    receiver: tokio::sync::mpsc::Receiver<Vec<u8>>,
    forward: Option<tokio::task::JoinHandle<()>>,
}
impl DecodedLocalSubscription {
    async fn new<T: BusMessage + Send + 'static>(
        bus: &Arc<LocalTransport>,
        channel: &Channel,
        queue: bool,
        size: usize,
    ) -> Self {
        let mut raw = bus.subscribe_sized(channel, queue, size).await;
        let retirement = raw.retirement();
        let (sender, receiver) = tokio::sync::mpsc::channel(size);
        let stop = retirement.clone();
        let limits = bus.limits().wire;
        let gzip_limit = bus.gzip_limit();
        let channel = channel.clone();
        let forward = tokio::spawn(async move {
            loop {
                let Ok(bytes) = raw.recv().await else { break };
                let Ok(projected) =
                    wire::decode_raw_with_gzip_limit::<T>(&bytes, limits, gzip_limit)
                else {
                    continue;
                };
                let Ok(known) = wire::filter_bus_known(T::FULL_NAME, &projected.payload) else {
                    continue;
                };
                if <T as prost::Message>::decode(known.as_slice()).is_err() {
                    continue;
                }
                tokio::select! { biased; _=stop.cancelled()=>break, sent=sender.send(bytes)=>if sent.is_err(){break} }
            }
            let _ = raw.unsubscribe(&channel).await;
        });
        Self {
            retirement,
            receiver,
            forward: Some(forward),
        }
    }
    #[cfg(test)]
    fn retirement(&self) -> LocalRetirement {
        self.retirement.clone()
    }
    async fn recv(&mut self) -> Result<Vec<u8>, TransportError> {
        self.receiver
            .recv()
            .await
            .ok_or(TransportError::Disconnected)
    }
    async fn unsubscribe(mut self, _: &Channel) -> Result<(), TransportError> {
        self.retirement.cancel();
        if let Some(task) = self.forward.take() {
            let _ = task.await;
        }
        Ok(())
    }
}
impl Drop for DecodedLocalSubscription {
    fn drop(&mut self) {
        self.retirement.cancel();
        let _detached = self.forward.take();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::service_bus::{
        rpc::MethodSpec,
        wire::{CompressionPolicy, WireLimits},
    };
    use lk_protocol::psrpc::internal as pb;
    use std::time::Duration;
    fn local(capacity: usize, cap: usize) -> Arc<LocalTransport> {
        LocalTransport::new_with_gzip_limit(
            TransportLimits {
                wire: WireLimits {
                    encoded_bytes: 1 << 20,
                    decompressed_bytes: 1 << 20,
                },
                compression: CompressionPolicy {
                    quality: 0,
                    threshold: 1,
                },
                inbox_messages: capacity,
                subscriptions: 100,
                commands: 100,
                io_timeout: Duration::from_secs(3),
            },
            cap,
        )
        .unwrap()
    }
    fn channel() -> Channel {
        MethodSpec {
            service: "LocalQueueSourceBoundary".into(),
            method: "Message".into(),
            topics: vec!["owned".into()],
            require_claim: true,
            queue: true,
        }
        .request_channel()
    }
    #[tokio::test]
    async fn server_default_and_client_override_have_independent_real_raw_and_typed_capacities() {
        let local = local(3, 0);
        let bus = RpcBus::from(local);
        let ch = channel();
        let server = bus
            .subscribe_server::<pb::Request>(&ch, true)
            .await
            .unwrap();
        let client = bus.subscribe::<pb::Response>(&ch, false).await.unwrap();
        let RpcSubscription::Local(server_local) = &server else {
            panic!("actual local subscription required")
        };
        let RpcSubscription::Local(client_local) = &client else {
            panic!("actual local subscription required")
        };
        assert_eq!(server_local.receiver.max_capacity(), 100);
        assert_eq!(server_local.retirement.raw_capacity(), 100);
        assert_eq!(client_local.receiver.max_capacity(), 3);
        assert_eq!(client_local.retirement.raw_capacity(), 3);
        server.unsubscribe(&ch).await.unwrap();
        client.unsubscribe(&ch).await.unwrap();
    }
    #[tokio::test]
    async fn plain_message_ignores_gzip_cap_but_compressed_input_is_filtered_before_typed_queue() {
        let local = local(1, 1);
        let bus = RpcBus::from(local.clone());
        let ch = channel();
        let mut subscription = bus.subscribe::<pb::Request>(&ch, true).await.unwrap();
        let request = pb::Request {
            request_id: "plain-larger-than-one-byte".into(),
            ..Default::default()
        };
        bus.publish(&ch, &request).await.unwrap();
        let bytes = subscription.recv().await.unwrap();
        assert_eq!(
            wire::decode::<pb::Request>(&bytes, local.limits().wire)
                .unwrap()
                .message,
            request
        );
        let compressed = wire::encode(
            &pb::Request {
                request_id: "x".repeat(4096),
                ..Default::default()
            },
            &ch.local,
            CompressionPolicy {
                quality: 6,
                threshold: 1,
            },
            local.limits().wire,
        )
        .unwrap();
        assert_eq!(
            wire::decode_raw::<pb::Request>(&compressed, local.limits().wire)
                .unwrap()
                .compression,
            1
        );
        bus.publish_encoded(&ch, &compressed).await.unwrap();
        bus.publish(&ch, &request).await.unwrap();
        let next = subscription.recv().await.unwrap();
        assert_eq!(
            wire::decode::<pb::Request>(&next, local.limits().wire)
                .unwrap()
                .message,
            request,
            "oversized expanded GZIP must be dropped without corrupting the following NONE message"
        );
        subscription.unsubscribe(&ch).await.unwrap();
    }
    #[tokio::test]
    async fn independent_raw_and_typed_queues_block_fourth_and_registration_until_reader_closes() {
        use futures_util::{pin_mut, poll};
        use std::task::Poll;
        let local = local(1, 0);
        let bus = RpcBus::from(local.clone());
        let ch = channel();
        let subscription = bus.subscribe::<pb::Request>(&ch, true).await.unwrap();
        for n in 0..3 {
            bus.publish(
                &ch,
                &pb::Request {
                    request_id: n.to_string(),
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        }
        let request = pb::Request {
            request_id: "fourth".into(),
            ..Default::default()
        };
        let fourth = bus.publish(&ch, &request);
        pin_mut!(fourth);
        assert!(matches!(poll!(fourth.as_mut()), Poll::Pending));
        let registering = bus.subscribe::<pb::Request>(&ch, true);
        pin_mut!(registering);
        assert!(
            matches!(poll!(registering.as_mut()), Poll::Pending),
            "source subList membership insertion waits behind the full publish"
        );
        let (closed, published, registered) =
            tokio::join!(subscription.unsubscribe(&ch), fourth, registering);
        closed.unwrap();
        assert_eq!(published.unwrap(), 0);
        let mut replacement = registered.unwrap();
        let next = pb::Request {
            request_id: "only-after-register".into(),
            ..Default::default()
        };
        bus.publish(&ch, &next).await.unwrap();
        let raw = replacement.recv().await.unwrap();
        assert_eq!(
            wire::decode::<pb::Request>(&raw, local.limits().wire)
                .unwrap()
                .message,
            next
        );
        replacement.unsubscribe(&ch).await.unwrap();
    }
    #[tokio::test]
    async fn real_local_rpc_remote_error_keeps_legal_any_wrong_kind_and_rejects_invalid_known_utf8()
    {
        use crate::service_bus::{
            channels,
            rpc::{CallOptions, RpcClient},
        };
        use prost::Message;
        let local = local(3, 0);
        let bus = RpcBus::from(local.clone());
        let spec = MethodSpec {
            service: "LocalAnyConsumer".into(),
            method: "Error".into(),
            topics: vec!["owned".into()],
            require_claim: false,
            queue: true,
        };
        let mut requests = bus
            .subscribe::<pb::Request>(&spec.request_channel(), false)
            .await
            .unwrap();
        let client = Arc::new(RpcClient::new(local.clone(), 8).unwrap());
        let pending = {
            let client = client.clone();
            let spec = spec.clone();
            tokio::spawn(async move {
                client
                    .unary::<_, lk_protocol::livekit::Room>(
                        &spec,
                        &lk_protocol::livekit::DeleteRoomRequest {
                            room: "owned".into(),
                        },
                        CallOptions::new(Duration::from_secs(3)),
                    )
                    .await
            })
        };
        let actual_request =
            wire::decode::<pb::Request>(&requests.recv().await.unwrap(), local.limits().wire)
                .unwrap()
                .message;
        let response = pb::Response {
            request_id: actual_request.request_id,
            server_id: "actual-wire-observer".into(),
            error: "actual remote error".into(),
            code: "invalid_argument".into(),
            ..Default::default()
        };
        let mut malformed = response.encode_to_vec();
        malformed.extend_from_slice(&[0x42, 3, 0x0a, 1, 0xff]);
        let channel = channels::response(&spec.service, &actual_request.client_id);
        let invalid = wire::encode_raw::<pb::Response>(
            &malformed,
            &channel.local,
            CompressionPolicy::default(),
            local.limits().wire,
        )
        .unwrap();
        bus.publish_encoded(&channel, &invalid).await.unwrap();
        let mut valid = response.encode_to_vec();
        valid.extend_from_slice(&[0x42, 5, 0x08, 7, 0x12, 1, b'a']);
        let envelope = wire::encode_raw::<pb::Response>(
            &valid,
            &channel.local,
            CompressionPolicy::default(),
            local.limits().wire,
        )
        .unwrap();
        bus.publish_encoded(&channel, &envelope).await.unwrap();
        let error = pending.await.unwrap().unwrap_err();
        assert_eq!(error.code, "invalid_argument");
        assert!(error.is_remote());
        assert_eq!(error.details.len(), 1);
        assert_eq!(error.details[0].value.as_ref(), b"a");
        let details = error.error_detail_wires().unwrap();
        assert_eq!(
            details[0],
            vec![0x12, 1, b'a', 0x08, 7],
            "legal unknown cohort remains owned by its original Any after real consumer"
        );
        assert_eq!(client.pending_count(), 0);
        requests.unsubscribe(&spec.request_channel()).await.unwrap();
        client.close();
    }
}
