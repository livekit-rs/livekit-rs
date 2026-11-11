// Copyright 2023 LiveKit, Inc.
// SPDX-License-Identifier: Apache-2.0
// Source: pinned psrpc/pkg/bus/localbus/localbus.go; see candidate NOTICE.
//! Source in-process PSRPC transport (psrpc/pkg/bus/localbus/localbus.go).
//! Opaque existing wire envelopes; real current subscribers, no durable replay.
use super::channels::Channel;
use super::redis_transport::{TransportError, TransportLimits};
use std::collections::HashMap;
use std::sync::{Arc, Mutex, Weak};
use tokio::sync::{RwLock as AsyncRwLock, mpsc};
use tokio_util::sync::CancellationToken;

struct Reader {
    sender: mpsc::Sender<Vec<u8>>,
    stop: CancellationToken,
}
#[derive(Default)]
struct Members {
    slots: Vec<Option<Arc<Reader>>>,
    next: usize,
}
struct Group {
    members: Mutex<Members>,
    /// Source queue dispatch holds its selection lock through the actual write.
    dispatch: Arc<AsyncRwLock<()>>,
}
impl Group {
    fn new() -> Self {
        Self {
            members: Mutex::new(Members::default()),
            dispatch: Arc::new(AsyncRwLock::new(())),
        }
    }
}
type Key = (String, bool);
/// Shared within one server process; unrelated instances cannot communicate.
pub struct LocalTransport {
    groups: Arc<Mutex<HashMap<Key, Arc<Group>>>>,
    limits: TransportLimits,
    gzip_limit: usize,
    registration: Arc<AsyncRwLock<()>>,
}
impl LocalTransport {
    pub fn new(limits: TransportLimits) -> Result<Arc<Self>, TransportError> {
        Self::new_with_gzip_limit(limits, limits.wire.decompressed_bytes)
    }
    pub fn new_with_gzip_limit(
        limits: TransportLimits,
        gzip_limit: usize,
    ) -> Result<Arc<Self>, TransportError> {
        if limits.inbox_messages == 0 || limits.wire.encoded_bytes == 0 {
            return Err(TransportError::Configuration);
        }
        Ok(Arc::new(Self {
            groups: Arc::new(Mutex::new(HashMap::new())),
            registration: Arc::new(AsyncRwLock::new(())),
            limits,
            gzip_limit: if gzip_limit == 0 {
                limits.wire.decompressed_bytes
            } else {
                gzip_limit
            },
        }))
    }
    /// 同一总线的客户端视图：只改变订阅容量，共享成员表与注册锁。
    /// EgressInternal 使用 1000，其他客户端使用配置值，不能因此隔离通信。
    pub(crate) fn with_inbox_size(
        &self,
        inbox_messages: usize,
    ) -> Result<Arc<Self>, TransportError> {
        if inbox_messages == 0 || inbox_messages > tokio::sync::Semaphore::MAX_PERMITS {
            return Err(TransportError::Configuration);
        }
        Ok(Arc::new(Self {
            groups: self.groups.clone(),
            registration: self.registration.clone(),
            limits: TransportLimits {
                inbox_messages,
                ..self.limits
            },
            gzip_limit: self.gzip_limit,
        }))
    }
    pub fn limits(&self) -> TransportLimits {
        self.limits
    }
    pub(crate) fn gzip_limit(&self) -> usize {
        self.gzip_limit
    }
    pub async fn subscribe(self: &Arc<Self>, channel: &Channel, queue: bool) -> LocalSubscription {
        self.subscribe_sized(channel, queue, self.limits.inbox_messages)
            .await
    }
    pub(crate) async fn subscribe_sized(
        self: &Arc<Self>,
        channel: &Channel,
        queue: bool,
        size: usize,
    ) -> LocalSubscription {
        let _transport = self.registration.write().await;
        let key = (channel.legacy.clone(), queue);
        loop {
            let group = {
                let mut groups = self.groups.lock().unwrap_or_else(|p| p.into_inner());
                if let Some(group) = groups.get(&key) {
                    group.clone()
                } else {
                    // Source transport lock publishes the first membership
                    // atomically with its group. No visible empty group gap.
                    let group = Arc::new(Group::new());
                    let subscription = self.create_subscription(key.clone(), group.clone(), size);
                    groups.insert(key.clone(), group);
                    return subscription;
                }
            };
            // Source create takes subList.Lock, including while a publication
            // is blocked writing an existing subscriber. Never block a Tokio
            // executor thread with a synchronous guard across this await.
            let _dispatch = group.dispatch.clone().write_owned().await;
            let groups = self.groups.lock().unwrap_or_else(|p| p.into_inner());
            if groups.get(&key).is_some_and(|g| Arc::ptr_eq(g, &group)) {
                return self.create_subscription(key.clone(), group, size);
            }
            // A close retired the old group while this subscriber waited.
            // Register only in the current map generation, not an orphan Arc.
        }
    }
    fn create_subscription(
        self: &Arc<Self>,
        key: Key,
        group: Arc<Group>,
        size: usize,
    ) -> LocalSubscription {
        let (sender, receiver) = mpsc::channel(size);
        let reader = Arc::new(Reader {
            sender,
            stop: CancellationToken::new(),
        });
        let mut members = group.members.lock().unwrap_or_else(|p| p.into_inner());
        let slot = if let Some(i) = members.slots.iter().position(Option::is_none) {
            members.slots[i] = Some(reader.clone());
            i
        } else {
            members.slots.push(Some(reader.clone()));
            members.slots.len() - 1
        };
        drop(members);
        LocalSubscription {
            bus: Arc::downgrade(self),
            group,
            key,
            slot,
            reader,
            receiver,
            closed: false,
        }
    }
    /// Snapshot of current subscriptions only. Later registration cannot claim
    /// a publication made with no subscribers. Source publisher ctx is ignored:
    /// capacity waits end when the actual selected subscriber closes.
    pub async fn publish_encoded(
        &self,
        channel: &Channel,
        bytes: &[u8],
    ) -> Result<u64, TransportError> {
        if bytes.len() > self.limits.wire.encoded_bytes {
            return Err(TransportError::Oversized);
        }
        let (broadcast, queue) = {
            let _transport = self.registration.read().await;
            let groups = self.groups.lock().unwrap_or_else(|p| p.into_inner());
            (
                groups.get(&(channel.legacy.clone(), false)).cloned(),
                groups.get(&(channel.legacy.clone(), true)).cloned(),
            )
        };
        let mut count = 0;
        if let Some(group) = broadcast {
            let _serial = group.dispatch.read().await;
            let readers: Vec<_> = group
                .members
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .slots
                .iter()
                .flatten()
                .cloned()
                .collect();
            for reader in readers {
                count += u64::from(Self::write(&reader, bytes).await);
            }
        }
        if let Some(group) = queue {
            let _serial = group.dispatch.write().await;
            let reader = {
                let mut members = group.members.lock().unwrap_or_else(|p| p.into_inner());
                let len = members.slots.len();
                let mut selected = None;
                for _ in 0..len {
                    if members.next >= len {
                        members.next = 0;
                    }
                    let i = members.next;
                    members.next += 1;
                    if let Some(reader) = &members.slots[i] {
                        selected = Some(reader.clone());
                        break;
                    }
                }
                selected
            };
            if let Some(reader) = reader {
                count += u64::from(Self::write(&reader, bytes).await);
            }
        }
        Ok(count)
    }
    async fn write(reader: &Reader, bytes: &[u8]) -> bool {
        tokio::select! {
            _ = reader.stop.cancelled() => false,
            sent = reader.sender.send(bytes.to_vec()) => sent.is_ok(),
        }
    }
}
/// Actual subscription lifetime, including a blocked local publication.
pub struct LocalSubscription {
    bus: Weak<LocalTransport>,
    group: Arc<Group>,
    key: Key,
    slot: usize,
    reader: Arc<Reader>,
    receiver: mpsc::Receiver<Vec<u8>>,
    closed: bool,
}
impl LocalSubscription {
    pub async fn recv(&mut self) -> Result<Vec<u8>, TransportError> {
        tokio::select! {
            _ = self.reader.stop.cancelled() => Err(TransportError::Disconnected),
            raw = self.receiver.recv() => raw.ok_or(TransportError::Disconnected),
        }
    }
    pub async fn unsubscribe(mut self, _channel: &Channel) -> Result<(), TransportError> {
        self.retirement().close_and_wait().await;
        self.closed = true;
        Ok(())
    }
}
/// Synchronous acceptance retirement for a process-local subscription. It
/// removes only this exact membership; already accepted RPC tasks are separate.
#[derive(Clone)]
pub(crate) struct LocalRetirement {
    bus: Weak<LocalTransport>,
    group: Arc<Group>,
    key: Key,
    slot: usize,
    reader: Arc<Reader>,
}
impl LocalSubscription {
    pub(crate) fn retirement(&self) -> LocalRetirement {
        LocalRetirement {
            bus: self.bus.clone(),
            group: self.group.clone(),
            key: self.key.clone(),
            slot: self.slot,
            reader: self.reader.clone(),
        }
    }
}
impl LocalRetirement {
    pub(crate) fn cancel(&self) {
        self.reader.stop.cancel();
    }
    pub(crate) async fn close_and_wait(&self) {
        // Source Close cancels the actual reader BEFORE waiting on transport
        // then subList locks. That ordering releases the blocked write which a
        // concurrent membership insertion is holding the transport lock for.
        self.cancel();
        let Some(bus) = self.bus.upgrade() else {
            return;
        };
        let _transport = bus.registration.write().await;
        let _dispatch = self.group.dispatch.write().await;
        self.close();
    }
    #[cfg(test)]
    pub(crate) fn raw_capacity(&self) -> usize {
        self.reader.sender.max_capacity()
    }
    pub(crate) async fn cancelled(&self) {
        self.reader.stop.cancelled().await;
    }
    pub(crate) fn close(&self) {
        self.reader.stop.cancel();
        let Some(bus) = self.bus.upgrade() else {
            return;
        };
        let mut groups = bus.groups.lock().unwrap_or_else(|p| p.into_inner());
        let mut members = self.group.members.lock().unwrap_or_else(|p| p.into_inner());
        if members
            .slots
            .get(self.slot)
            .and_then(Option::as_ref)
            .is_some_and(|r| Arc::ptr_eq(r, &self.reader))
        {
            members.slots[self.slot] = None;
        }
        if members.slots.iter().all(Option::is_none)
            && groups
                .get(&self.key)
                .is_some_and(|g| Arc::ptr_eq(g, &self.group))
        {
            groups.remove(&self.key);
        }
    }
}
impl Drop for LocalSubscription {
    fn drop(&mut self) {
        if self.closed {
            return;
        }
        let retirement = self.retirement();
        retirement.cancel();
        if let Ok(runtime) = tokio::runtime::Handle::try_current() {
            drop(runtime.spawn(async move {
                retirement.close_and_wait().await;
            }));
        } else {
            retirement.close();
        } // no async runtime/active local producer on this thread
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::service_bus::rpc::MethodSpec;
    use crate::service_bus::{
        rpc::{CallOptions, RpcClient, RpcError, RpcServer, ServerLimits},
        wire::{CompressionPolicy, WireLimits},
    };
    use lk_protocol::livekit as pb;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;
    fn bus(capacity: usize) -> Arc<LocalTransport> {
        LocalTransport::new(TransportLimits {
            wire: WireLimits {
                encoded_bytes: 1 << 20,
                decompressed_bytes: 1 << 20,
            },
            compression: CompressionPolicy::default(),
            inbox_messages: capacity,
            subscriptions: 100,
            commands: 100,
            io_timeout: Duration::from_secs(3),
        })
        .unwrap()
    }
    fn method() -> MethodSpec {
        MethodSpec {
            service: "AgentDispatchLocalTransportComponent".into(),
            method: "Create".into(),
            topics: vec!["owned".into()],
            require_claim: true,
            queue: true,
        }
    }
    fn options() -> CallOptions {
        let mut options = CallOptions::new(Duration::from_secs(3));
        options.selection.accept_first = true;
        options.selection.affinity_timeout = Some(Duration::from_secs(1));
        options
    }
    fn limits() -> ServerLimits {
        ServerLimits {
            concurrent: 8,
            max_request_duration: Duration::MAX,
            allow_skip_claim: false,
        }
    }
    #[tokio::test]
    async fn client_views_share_membership_but_not_capacity_and_keep_root_isolation() {
        let root = bus(8);
        let small = root.with_inbox_size(1).unwrap();
        let large = root.with_inbox_size(1000).unwrap();
        assert!(root.with_inbox_size(0).is_err());
        assert_eq!(small.gzip_limit(), root.gzip_limit());
        let channel = method().request_channel();
        let mut a = small.subscribe(&channel, false).await;
        let mut b = large.subscribe(&channel, false).await;
        assert_eq!(a.retirement().raw_capacity(), 1);
        assert_eq!(b.retirement().raw_capacity(), 1000);
        assert_eq!(
            bus(8).publish_encoded(&channel, b"isolated").await.unwrap(),
            0
        );
        drop(root);
        assert_eq!(large.publish_encoded(&channel, b"shared").await.unwrap(), 2);
        assert_eq!(a.recv().await.unwrap(), b"shared");
        assert_eq!(b.recv().await.unwrap(), b"shared");
        a.unsubscribe(&channel).await.unwrap();
        b.unsubscribe(&channel).await.unwrap();
        assert!(large.groups.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn absent_publication_is_not_replayed_to_later_subscription_and_queue_round_robins() {
        let bus = bus(8);
        let channel = method().request_channel();
        assert_eq!(bus.publish_encoded(&channel, b"absent").await.unwrap(), 0);
        let mut a = bus.subscribe(&channel, true).await;
        let mut b = bus.subscribe(&channel, true).await;
        assert!(a.receiver.try_recv().is_err());
        assert!(b.receiver.try_recv().is_err());
        for raw in [b"first".as_slice(), b"second", b"third"] {
            assert_eq!(bus.publish_encoded(&channel, raw).await.unwrap(), 1);
        }
        assert_eq!(a.recv().await.unwrap(), b"first");
        assert_eq!(b.recv().await.unwrap(), b"second");
        assert_eq!(a.recv().await.unwrap(), b"third");
        a.unsubscribe(&channel).await.unwrap();
        assert_eq!(bus.publish_encoded(&channel, b"survives").await.unwrap(), 1);
        assert_eq!(b.recv().await.unwrap(), b"survives");
        b.unsubscribe(&channel).await.unwrap();
        assert!(bus.groups.lock().unwrap().is_empty());
    }
    #[tokio::test]
    async fn closing_selected_reader_releases_real_capacity_blocked_publish() {
        let bus = bus(1);
        let channel = method().request_channel();
        let sub = bus.subscribe(&channel, true).await;
        bus.publish_encoded(&channel, b"fills-reader")
            .await
            .unwrap();
        let sending = {
            let bus = bus.clone();
            let channel = channel.clone();
            tokio::spawn(async move { bus.publish_encoded(&channel, b"blocked").await })
        };
        tokio::task::yield_now().await;
        assert!(!sending.is_finished());
        sub.unsubscribe(&channel).await.unwrap();
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(3), sending)
                .await
                .unwrap()
                .unwrap()
                .unwrap(),
            0
        );
        assert!(bus.groups.lock().unwrap().is_empty());
    }
    #[tokio::test]
    async fn real_shared_rpc_claim_grant_and_remote_application_error_use_local_wire() {
        let bus = bus(16);
        let spec = method();
        let entered = Arc::new(AtomicUsize::new(0));
        let count = entered.clone();
        let server = RpcServer::register::<pb::DeleteRoomRequest, pb::Room, _, _>(
            bus.clone(),
            spec.clone(),
            limits(),
            move |_, request| {
                let count = count.clone();
                async move {
                    count.fetch_add(1, Ordering::SeqCst);
                    if request.room == "missing-record" {
                        Err(RpcError::new("not_found", "owned record missing"))
                    } else {
                        Ok(pb::Room {
                            name: request.room,
                            ..Default::default()
                        })
                    }
                }
            },
            None,
        )
        .await
        .unwrap();
        let client = RpcClient::new(bus.clone(), 8).unwrap();
        let room: pb::Room = client
            .unary(
                &spec,
                &pb::DeleteRoomRequest {
                    room: "actual-claimed".into(),
                },
                options(),
            )
            .await
            .unwrap();
        assert_eq!(room.name, "actual-claimed");
        let error = client
            .unary::<_, pb::Room>(
                &spec,
                &pb::DeleteRoomRequest {
                    room: "missing-record".into(),
                },
                options(),
            )
            .await
            .unwrap_err();
        assert_eq!(error.code, "not_found");
        assert!(error.is_remote());
        assert_eq!(entered.load(Ordering::SeqCst), 2);
        assert_eq!(client.pending_count(), 0);
        server.shutdown().await;
        client.close();
    }
    #[tokio::test]
    async fn topic_registered_after_publish_cannot_accept_that_request_but_next_request_can() {
        let bus = bus(16);
        let spec = method();
        let mut observer = bus.subscribe(&spec.request_channel(), false).await;
        let client = Arc::new(RpcClient::new(bus.clone(), 8).unwrap());
        let pending = {
            let client = client.clone();
            let spec = spec.clone();
            tokio::spawn(async move {
                client
                    .unary::<_, pb::Room>(
                        &spec,
                        &pb::DeleteRoomRequest {
                            room: "owned".into(),
                        },
                        options(),
                    )
                    .await
            })
        };
        let original = observer.recv().await.unwrap();
        assert!(!original.is_empty());
        let entered = Arc::new(AtomicUsize::new(0));
        let count = entered.clone();
        let server = RpcServer::register::<pb::DeleteRoomRequest, pb::Room, _, _>(
            bus.clone(),
            spec.clone(),
            limits(),
            move |_, request| {
                let count = count.clone();
                async move {
                    count.fetch_add(1, Ordering::SeqCst);
                    Ok(pb::Room {
                        name: request.room,
                        ..Default::default()
                    })
                }
            },
            None,
        )
        .await
        .unwrap();
        let error = pending.await.unwrap().unwrap_err();
        assert!(error.is_local_no_response());
        assert_eq!(entered.load(Ordering::SeqCst), 0);
        let response: pb::Room = client
            .unary(
                &spec,
                &pb::DeleteRoomRequest {
                    room: "owned".into(),
                },
                options(),
            )
            .await
            .unwrap();
        assert_eq!(response.name, "owned");
        assert_eq!(entered.load(Ordering::SeqCst), 1);
        assert_eq!(client.pending_count(), 0);
        drop(observer);
        server.shutdown().await;
        client.close();
    }
    #[tokio::test]
    async fn real_claimed_handler_survives_caller_cancel_without_a_fake_cancel_wire() {
        let bus = bus(16);
        let spec = method();
        let entered = Arc::new(tokio::sync::Notify::new());
        let release = Arc::new(tokio::sync::Notify::new());
        let completed = Arc::new(AtomicUsize::new(0));
        let server = RpcServer::register::<pb::DeleteRoomRequest, pb::Room, _, _>(
            bus.clone(),
            spec.clone(),
            limits(),
            {
                let entered = entered.clone();
                let release = release.clone();
                let completed = completed.clone();
                move |_, request| {
                    let entered = entered.clone();
                    let release = release.clone();
                    let completed = completed.clone();
                    async move {
                        entered.notify_one();
                        release.notified().await;
                        completed.fetch_add(1, Ordering::SeqCst);
                        Ok(pb::Room {
                            name: request.room,
                            ..Default::default()
                        })
                    }
                }
            },
            None,
        )
        .await
        .unwrap();
        let client = Arc::new(RpcClient::new(bus, 8).unwrap());
        let call_options = options();
        let cancel = call_options.cancel.clone();
        let pending = {
            let client = client.clone();
            let spec = spec.clone();
            tokio::spawn(async move {
                client
                    .unary::<_, pb::Room>(
                        &spec,
                        &pb::DeleteRoomRequest {
                            room: "held".into(),
                        },
                        call_options,
                    )
                    .await
            })
        };
        tokio::time::timeout(Duration::from_secs(3), entered.notified())
            .await
            .unwrap();
        cancel.cancel();
        let error = pending.await.unwrap().unwrap_err();
        assert_eq!(error.code, "canceled");
        assert_eq!(client.pending_count(), 0);
        assert_eq!(completed.load(Ordering::SeqCst), 0);
        release.notify_one();
        server.shutdown().await;
        assert_eq!(completed.load(Ordering::SeqCst), 1);
        client.close();
    }

    #[tokio::test]
    async fn retirement_stops_acceptance_but_does_not_abort_already_entered_local_handler() {
        let bus = bus(16);
        let spec = method();
        let entered = Arc::new(tokio::sync::Notify::new());
        let release = Arc::new(tokio::sync::Notify::new());
        let server = RpcServer::register::<pb::DeleteRoomRequest, pb::Room, _, _>(
            bus.clone(),
            spec.clone(),
            limits(),
            {
                let entered = entered.clone();
                let release = release.clone();
                move |_, request| {
                    let entered = entered.clone();
                    let release = release.clone();
                    async move {
                        entered.notify_one();
                        release.notified().await;
                        Ok(pb::Room {
                            name: request.room,
                            ..Default::default()
                        })
                    }
                }
            },
            None,
        )
        .await
        .unwrap();
        let client = Arc::new(RpcClient::new(bus.clone(), 8).unwrap());
        let pending = {
            let client = client.clone();
            let spec = spec.clone();
            tokio::spawn(async move {
                client
                    .unary::<_, pb::Room>(
                        &spec,
                        &pb::DeleteRoomRequest {
                            room: "accepted-before-retire".into(),
                        },
                        options(),
                    )
                    .await
            })
        };
        tokio::time::timeout(Duration::from_secs(3), entered.notified())
            .await
            .unwrap();
        let receipt = server.retire_local().await.unwrap();
        assert_eq!(
            bus.publish_encoded(&spec.request_channel(), b"no-acceptance")
                .await
                .unwrap(),
            0
        );
        assert!(!receipt.is_finished());
        release.notify_one();
        let response = pending.await.unwrap().unwrap();
        assert_eq!(response.name, "accepted-before-retire");
        receipt.finish().await;
        client.close();
    }
    #[tokio::test]
    async fn stale_membership_retirement_cannot_remove_a_reused_local_slot() {
        let bus = bus(8);
        let channel = method().request_channel();
        let old = bus.subscribe(&channel, true).await;
        let retirement = old.retirement();
        retirement.close_and_wait().await;
        let mut replacement = bus.subscribe(&channel, true).await;
        // Original receiver still exists: its later Drop must not erase either
        // the new group or a new membership which reused its old slot.
        old.unsubscribe(&channel).await.unwrap();
        retirement.close_and_wait().await;
        assert_eq!(
            bus.publish_encoded(&channel, b"new-generation")
                .await
                .unwrap(),
            1
        );
        assert_eq!(replacement.recv().await.unwrap(), b"new-generation");
        replacement.unsubscribe(&channel).await.unwrap();
        assert!(bus.groups.lock().unwrap().is_empty());
    }

    /// Old v2 public local-transport APIs only: isolate the global lock delta.
    #[tokio::test]
    async fn blocked_membership_on_one_topic_blocks_other_topic_lookup_and_close() {
        use futures_util::{pin_mut, poll};
        use std::task::Poll;
        let bus = bus(1);
        let a = method().request_channel();
        let mut b = a.clone();
        b.legacy.push_str("-other");
        let sa = bus.subscribe(&a, true).await;
        let sb = bus.subscribe(&b, true).await;
        bus.publish_encoded(&a, b"fills-a").await.unwrap();
        let full = bus.publish_encoded(&a, b"blocked-a");
        pin_mut!(full);
        assert!(matches!(poll!(full.as_mut()), Poll::Pending));
        let registering = bus.subscribe(&a, true);
        pin_mut!(registering);
        assert!(matches!(poll!(registering.as_mut()), Poll::Pending));
        let other = bus.publish_encoded(&b, b"cross-topic");
        pin_mut!(other);
        assert!(
            matches!(poll!(other.as_mut()), Poll::Pending),
            "source global transport RLock waits behind pending topic-A registration"
        );
        let closing = sb.unsubscribe(&b);
        pin_mut!(closing);
        assert!(
            matches!(poll!(closing.as_mut()), Poll::Pending),
            "source Close also waits for the global transport lock"
        );
        let (a_closed, full, mut replacement, other, b_closed) =
            tokio::join!(sa.unsubscribe(&a), full, registering, other, closing);
        a_closed.unwrap();
        assert_eq!(full.unwrap(), 0);
        other.unwrap();
        b_closed.unwrap();
        bus.publish_encoded(&a, b"fresh-only").await.unwrap();
        assert_eq!(replacement.recv().await.unwrap(), b"fresh-only");
        replacement.unsubscribe(&a).await.unwrap();
    }
}
