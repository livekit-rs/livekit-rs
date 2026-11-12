// Copyright 2023 LiveKit, Inc.
// SPDX-License-Identifier: Apache-2.0
//! PSRPC bidirectional streams, ported from pinned internal/stream/stream.go and
//! pkg/{client,server}/stream.go. This is an explicitly constructed component;
//! it does not activate any public route. Sequence numbers/retry batches belong
//! to LiveKit's RelaySignal payload layer, not internal.Stream. PSRPC Send sends
//! once and waits for its request-ID ACK; a slow receiver deliberately does not
//! ACK. The caller may then retry according to its service-specific policy.
use super::{
    channels::{self, Channel},
    rpc::{CallOptions, Cancellation, MethodSpec, RpcError, RpcErrorOrigin, SelectionState},
    rpc_transport::{RpcBus, RpcSubscription},
};
use lk_protocol::psrpc::internal as pb;
use pb::stream::Body;
use prost::Message;
use std::{
    collections::{BTreeMap, HashMap, HashSet},
    marker::PhantomData,
    sync::{Arc, Mutex, Weak},
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};
use tokio::{
    sync::{Notify, OwnedSemaphorePermit, Semaphore, mpsc, oneshot, watch},
    task::{JoinHandle, JoinSet},
};

#[derive(Clone, Copy, Debug)]
pub struct StreamConfig {
    pub timeout: Duration,
    pub channel_size: usize,
    pub max_streams: usize,
}
impl StreamConfig {
    fn validate(self) -> Result<Self, RpcError> {
        if self.timeout.is_zero()
            || self.channel_size == 0
            || self.channel_size > Semaphore::MAX_PERMITS
            || self.max_streams == 0
            || self.max_streams > Semaphore::MAX_PERMITS
        {
            return Err(RpcError::new("invalid_argument", "invalid stream limits"));
        }
        Ok(self)
    }
}
/// Incoming peer metadata is attached to the stream, never merged into outgoing metadata.
#[derive(Clone, Debug)]
pub struct StreamHeader {
    pub remote_id: String,
    pub sent_at_ns: i64,
    pub metadata: BTreeMap<String, String>,
}
/// Keep exact source payload alongside its typed projection for Blob provenance.
#[derive(Clone, Debug)]
pub struct StreamPayload<T> {
    pub message: T,
    pub raw: Vec<u8>,
}

type Sessions = Arc<Mutex<HashMap<String, Arc<Session>>>>;
type Claims = Arc<Mutex<HashMap<String, oneshot::Sender<pb::ClaimResponse>>>>;
type Validate = fn(&[u8]) -> Result<(), RpcError>;
struct State {
    terminated: bool,
    owned_close: bool,
    error: Option<RpcError>,
    acks: HashMap<String, oneshot::Sender<()>>,
    inbox: Option<mpsc::Sender<Vec<u8>>>,
}
struct Session {
    bus: RpcBus,
    target: Channel,
    id: String,
    timeout: Duration,
    stop: Cancellation,
    state: Mutex<State>,
    pending_changed: Notify,
    close_result: watch::Sender<Option<Result<(), RpcError>>>,
    validate: Validate,
    sessions: Weak<Mutex<HashMap<String, Arc<Session>>>>,
    _slot: Mutex<Option<OwnedSemaphorePermit>>,
}
fn lock<T>(m: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    m.lock().unwrap_or_else(|e| e.into_inner())
}
fn now() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos()
        .min(i64::MAX as u128) as i64
}
fn id(prefix: &str) -> String {
    rtc_core::ids::new_sid(prefix)
}
fn local_error(code: &str, text: &str, origin: RpcErrorOrigin) -> RpcError {
    let mut e = RpcError::new(code, text);
    e.origin = origin;
    e
}
fn closed() -> RpcError {
    local_error("canceled", "stream closed", RpcErrorOrigin::Closed)
}
fn timeout() -> RpcError {
    local_error(
        "deadline_exceeded",
        "request timed out",
        RpcErrorOrigin::Deadline,
    )
}
fn canceled() -> RpcError {
    local_error("canceled", "request canceled", RpcErrorOrigin::Canceled)
}
fn validate<T: Message + Default + prost::Name>(raw: &[u8]) -> Result<(), RpcError> {
    lk_protocol::wire::decode::<T>(raw)
        .map(|_| ())
        .map_err(|_| RpcError::new("malformed_request", "invalid stream payload"))
}
fn frame(stream: &str, timeout: Duration, body: Body) -> pb::Stream {
    let sent_at = now();
    pb::Stream {
        stream_id: stream.into(),
        request_id: id(""),
        sent_at,
        expiry: sent_at.saturating_add(timeout.as_nanos().min(i64::MAX as u128) as i64),
        body: Some(body),
    }
}
impl Session {
    fn error(&self) -> Option<RpcError> {
        lock(&self.state).error.clone()
    }
    fn finish(&self, error: RpcError) -> bool {
        let mut state = lock(&self.state);
        if state.terminated {
            return false;
        }
        state.terminated = true;
        if state.error.is_none() {
            state.error = Some(error);
        }
        state.acks.clear();
        state.inbox.take();
        drop(state);
        self.stop.cancel();
        self.pending_changed.notify_one();
        lock(&self._slot).take();
        if let Some(sessions) = self.sessions.upgrade() {
            lock(&sessions).remove(&self.id);
        }
        true
    }
    async fn close(self: &Arc<Self>, error: RpcError) -> Result<(), RpcError> {
        let (previous, owned_close) = {
            let mut state = lock(&self.state);
            let previous = state.error.clone();
            if previous.is_none() {
                state.error = Some(error.clone());
                state.owned_close = true;
            }
            (previous, state.owned_close)
        };
        if previous.is_none() {
            // This operation owns retirement even if the Close caller is dropped.
            // Every later close/owner shutdown waits for the same completion.
            let session = self.clone();
            tokio::spawn(async move {
                let value = frame(
                    &session.id,
                    session.timeout,
                    Body::Close(pb::StreamClose {
                        code: error.code.clone(),
                        error: error.message.clone(),
                    }),
                );
                let result = tokio::time::timeout(
                    session.timeout,
                    session.bus.publish(&session.target, &value),
                )
                .await
                .map_err(|_| timeout())
                .and_then(|r| r.map(|_| ()).map_err(Into::into));
                // Source Close publishes the terminal frame, then waits for
                // already pending Send operations, each with its own deadline.
                loop {
                    let changed = session.pending_changed.notified();
                    if lock(&session.state).acks.is_empty() {
                        break;
                    }
                    changed.await;
                }
                session.finish(error);
                session.close_result.send_replace(Some(result));
            });
        }
        if owned_close {
            let mut completion = self.close_result.subscribe();
            loop {
                if let Some(result) = completion.borrow().clone() {
                    return previous.map_or(result, Err);
                }
                if completion.changed().await.is_err() {
                    return Err(closed());
                }
            }
        }
        Err(previous.unwrap_or_else(closed))
    }

    async fn cancel_peer(self: &Arc<Self>, cause: RpcError) {
        self.finish(cause.clone());
        let value = frame(
            &self.id,
            self.timeout,
            Body::Close(pb::StreamClose {
                code: cause.code,
                error: cause.message,
            }),
        );
        let _ = tokio::time::timeout(self.timeout, self.bus.publish(&self.target, &value)).await;
    }
    async fn handle(self: &Arc<Self>, value: pb::Stream) -> Result<(), RpcError> {
        // Expiry is checked once by the client/server adapter, just as in
        // pinned runClientStream/streamHandler.run. Rechecking here would drop
        // an ACK/Close that crossed its deadline after admission.
        if value.stream_id != self.id {
            return Ok(());
        }
        match value.body {
            Some(Body::Ack(_)) => {
                if let Some(waiter) = lock(&self.state).acks.remove(&value.request_id) {
                    let _ = waiter.send(());
                }
                self.pending_changed.notify_one();
            }
            Some(Body::Close(close)) => {
                self.finish(local_error(
                    &close.code,
                    &close.error,
                    RpcErrorOrigin::Remote,
                ));
            }
            Some(Body::Message(message)) => {
                if let Err(error) = (self.validate)(&message.raw_message) {
                    let session = self.clone();
                    let cause = error.clone();
                    tokio::spawn(async move {
                        let _ = session.close(cause).await;
                    });
                    return Err(error);
                }
                {
                    let state = lock(&self.state);
                    if let Some(error) = &state.error {
                        return Err(error.clone());
                    }
                    // Source streamBase.Recv uses a nonblocking channel write. A full
                    // application channel means no ACK, not successful delivery.
                    state
                        .inbox
                        .as_ref()
                        .ok_or_else(closed)?
                        .try_send(message.raw_message)
                        .map_err(|_| {
                            RpcError::new(
                                "unavailable",
                                "stream message discarded by slow consumer",
                            )
                        })?;
                }
                let ack = pb::Stream {
                    stream_id: value.stream_id,
                    request_id: value.request_id,
                    sent_at: value.sent_at,
                    expiry: value.expiry,
                    body: Some(Body::Ack(pb::StreamAck {})),
                };
                let remaining =
                    Duration::from_nanos(value.expiry.saturating_sub(now()).max(0) as u64);
                tokio::time::timeout(remaining, self.bus.publish(&self.target, &ack))
                    .await
                    .map_err(|_| timeout())??;
            }
            _ => {}
        }
        Ok(())
    }
}
/// Unique application owner. Dropping a pending send removes its ACK waiter;
/// dropping this owner closes the peer and retires its subscription/registry slot.
pub struct RpcStream<Send, Recv> {
    session: Arc<Session>,
    incoming: mpsc::Receiver<Vec<u8>>,
    pub header: Option<StreamHeader>,
    reader: Option<JoinHandle<()>>,
    _types: PhantomData<fn(Send) -> Recv>,
}
struct AckWaiter {
    session: Arc<Session>,
    request: String,
}
impl Drop for AckWaiter {
    fn drop(&mut self) {
        lock(&self.session.state).acks.remove(&self.request);
        self.session.pending_changed.notify_one();
    }
}
impl<Send: Message + Default + prost::Name, Recv: Message + Default + prost::Name>
    RpcStream<Send, Recv>
{
    pub fn id(&self) -> &str {
        &self.session.id
    }
    pub fn error(&self) -> Option<RpcError> {
        self.session.error()
    }
    /// Observe terminal closure without consuming an application request.
    pub(crate) fn closed_signal(&self) -> Cancellation {
        self.session.stop.clone()
    }
    pub async fn recv(&mut self) -> Option<StreamPayload<Recv>> {
        let raw = self.incoming.recv().await?;
        let message = lk_protocol::wire::decode::<Recv>(&raw).ok()?;
        Some(StreamPayload { message, raw })
    }
    pub async fn send(&self, message: &Send) -> Result<(), RpcError> {
        self.send_raw(&message.encode_to_vec(), self.session.timeout)
            .await
    }
    /// Preserve original source bytes. Schema validation is explicit and occurs
    /// before publishing, so a raw cohort cannot disagree with its message type.
    pub fn sender(&self) -> StreamSender<Send> {
        StreamSender {
            session: self.session.clone(),
            _type: PhantomData,
        }
    }
    pub async fn send_raw(&self, raw: &[u8], budget: Duration) -> Result<(), RpcError> {
        self.sender().send_raw(raw, budget).await
    }
    pub async fn close(&mut self, cause: Option<RpcError>) -> Result<(), RpcError> {
        let result = self.session.close(cause.unwrap_or_else(closed)).await;
        if let Some(reader) = self.reader.take() {
            let _ = reader.await;
        }
        result
    }
}
/// Independently cloned sending half; dropping it does not retire the unique
/// receive owner. This keeps bidirectional application pumps from blocking each
/// other's receive queues while waiting for ACKs.
pub struct StreamSender<S> {
    session: Arc<Session>,
    _type: PhantomData<fn(S)>,
}
impl<S> Clone for StreamSender<S> {
    fn clone(&self) -> Self {
        Self {
            session: self.session.clone(),
            _type: PhantomData,
        }
    }
}
impl<Send: Message + Default + prost::Name> StreamSender<Send> {
    /// Actual terminal stream cause (fixed signalMessageSink.IsClosed).
    pub fn error(&self) -> Option<RpcError> {
        self.session.error()
    }
    pub async fn send_raw(&self, raw: &[u8], budget: Duration) -> Result<(), RpcError> {
        if let Some(error) = self.error() {
            return Err(error);
        }
        lk_protocol::wire::decode::<Send>(raw)
            .map_err(|_| RpcError::new("malformed_request", "invalid outgoing stream payload"))?;
        let value = frame(
            &self.session.id,
            budget,
            Body::Message(pb::StreamMessage {
                message: None,
                raw_message: raw.to_vec(),
            }),
        );
        let (ack, waiting) = oneshot::channel();
        {
            let mut state = lock(&self.session.state);
            if let Some(error) = &state.error {
                return Err(error.clone());
            }
            state.acks.insert(value.request_id.clone(), ack);
        }
        let _waiter = AckWaiter {
            session: self.session.clone(),
            request: value.request_id.clone(),
        };
        let operation = async {
            self.session
                .bus
                .publish(&self.session.target, &value)
                .await?;
            // Only an actual ACK sends Ok(()). Terminal retirement drops the
            // sender, which must propagate the stream cause instead of success.
            match waiting.await {
                Ok(()) => Ok(()),
                Err(_) => Err(self.session.error().unwrap_or_else(closed)),
            }
        };
        tokio::select! {
            biased;
            _=self.session.stop.cancelled()=>Err(self.session.error().unwrap_or_else(closed)),
            result=tokio::time::timeout(budget,operation)=>result.map_err(|_|timeout())?,
        }
    }
}
impl<Send, Recv> Drop for RpcStream<Send, Recv> {
    fn drop(&mut self) {
        let session = self.session.clone();
        let reader = self.reader.take();
        if let Ok(runtime) = tokio::runtime::Handle::try_current() {
            runtime.spawn(async move {
                let _ = session.close(closed()).await;
                if let Some(reader) = reader {
                    let _ = reader.await;
                }
            });
        } else {
            session.finish(closed());
            if let Some(reader) = reader {
                reader.abort();
            }
        }
    }
}
fn make_stream<S: Message, R: Message + Default + prost::Name>(
    bus: RpcBus,
    target: Channel,
    id: String,
    config: StreamConfig,
    sessions: Weak<Mutex<HashMap<String, Arc<Session>>>>,
    slot: Option<OwnedSemaphorePermit>,
) -> RpcStream<S, R> {
    let (sender, incoming) = mpsc::channel(config.channel_size);
    RpcStream {
        session: Arc::new(Session {
            bus,
            target,
            id,
            timeout: config.timeout,
            stop: Cancellation::default(),
            state: Mutex::new(State {
                terminated: false,
                owned_close: false,
                error: None,
                acks: HashMap::new(),
                inbox: Some(sender),
            }),
            pending_changed: Notify::new(),
            close_result: watch::channel(None).0,
            validate: validate::<R>,
            sessions,
            _slot: Mutex::new(slot),
        }),
        incoming,
        header: None,
        reader: None,
        _types: PhantomData,
    }
}
fn stream_channel(spec: &MethodSpec) -> Channel {
    channels::stream_server(
        &spec.service,
        &spec.method,
        &spec.topics.iter().map(String::as_str).collect::<Vec<_>>(),
    )
}
fn claim_channel(spec: &MethodSpec) -> Channel {
    channels::claim_response(
        &spec.service,
        &spec.method,
        &spec.topics.iter().map(String::as_str).collect::<Vec<_>>(),
    )
}

/// Long-lived PSRPC client identity, ported from the official typed client's
/// `WithClientID(nodeID)` (protocol-stable/rpc/typed_api.go NewTypedSignalClient).
/// One shared `service|client_id|STR` subscription demultiplexes incoming stream
/// frames to per-session channels by stream_id, exactly as the server's
/// register_stream demux does. The CLAIM/RES subscriptions are held for the
/// identity's whole lifetime so the idle channel surface matches the official
/// client even before any stream is opened.
#[derive(Clone)]
pub struct ClientStreams {
    inner: Arc<ClientStreamsInner>,
}
struct ClientStreamsInner {
    bus: RpcBus,
    service: String,
    client_id: String,
    sessions: Mutex<HashMap<String, mpsc::Sender<pb::Stream>>>,
    terminal: Mutex<Option<RpcError>>,
    stop: Cancellation,
    task: Mutex<Option<JoinHandle<()>>>,
}
impl std::fmt::Debug for ClientStreams {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ClientStreams")
            .field("service", &self.inner.service)
            .field("client_id", &self.inner.client_id)
            .finish()
    }
}
impl ClientStreams {
    pub async fn new(
        bus: impl Into<RpcBus>,
        service: &str,
        client_id: &str,
    ) -> Result<Self, RpcError> {
        let bus = bus.into();
        let stream_target = channels::stream_client(service, client_id);
        let claim_target = channels::claim_request(service, client_id);
        let response_target = channels::response(service, client_id);
        let mut streams = bus.subscribe::<pb::Stream>(&stream_target, false).await?;
        let claims = bus.subscribe::<pb::ClaimRequest>(&claim_target, false).await?;
        let responses = bus.subscribe::<pb::Response>(&response_target, false).await?;
        let inner = Arc::new(ClientStreamsInner {
            bus: bus.clone(),
            service: service.to_owned(),
            client_id: client_id.to_owned(),
            sessions: Mutex::new(HashMap::new()),
            terminal: Mutex::new(None),
            stop: Cancellation::default(),
            task: Mutex::new(None),
        });
        let dispatch = inner.clone();
        let stopping = inner.stop.clone();
        let task = tokio::spawn(async move {
            let mut claims = Some(claims);
            let mut responses = Some(responses);
            loop {
                tokio::select! {
                    biased;
                    _=stopping.cancelled()=>break,
                    value=streams.recv()=>{
                        let bytes=match value{Ok(bytes)=>bytes,Err(error)=>{dispatch.retire(error.into());break;}};
                        if let Ok(value)=dispatch.bus.decode::<pb::Stream>(&bytes)
                            && value.message.expiry>now()
                        {
                            let sender=lock(&dispatch.sessions).get(&value.message.stream_id).cloned();
                            if let Some(sender)=sender{
                                // Source streamBase.Recv uses a nonblocking write;
                                // a full session channel means no ACK, not delivery.
                                let _=sender.try_send(value.message);
                            }
                        }
                    }
                    value=async{match claims.as_mut(){Some(sub)=>sub.recv().await,None=>std::future::pending().await}}=>{
                        if value.is_err(){claims=None;}
                    }
                    value=async{match responses.as_mut(){Some(sub)=>sub.recv().await,None=>std::future::pending().await}}=>{
                        if value.is_err(){responses=None;}
                    }
                }
            }
            let _ = streams.unsubscribe(&stream_target).await;
            if let Some(sub) = claims {
                let _ = sub.unsubscribe(&claim_target).await;
            }
            if let Some(sub) = responses {
                let _ = sub.unsubscribe(&response_target).await;
            }
            lock(&dispatch.sessions).clear();
        });
        *lock(&inner.task) = Some(task);
        Ok(Self { inner })
    }
    pub fn client_id(&self) -> &str {
        &self.inner.client_id
    }
    fn register(&self, stream_id: &str, sender: mpsc::Sender<pb::Stream>) {
        lock(&self.inner.sessions).insert(stream_id.to_owned(), sender);
    }
    fn unregister(&self, stream_id: &str) {
        lock(&self.inner.sessions).remove(stream_id);
    }
    fn terminal_error(&self) -> Option<RpcError> {
        lock(&self.inner.terminal).clone()
    }
    pub async fn close(&self) {
        self.inner.stop.cancel();
        let task = lock(&self.inner.task).take();
        if let Some(task) = task {
            let _ = task.await;
        }
        lock(&self.inner.sessions).clear();
    }
}
impl ClientStreamsInner {
    fn retire(&self, error: RpcError) {
        *lock(&self.terminal) = Some(error);
        lock(&self.sessions).clear();
    }
}

/// Incoming frame source for one client stream. Shared reads the nodeID-keyed
/// identity demux (already decoded and expiry-checked by its dispatcher);
/// Dedicated owns a random per-call subscription and decodes here. Both preserve
/// runClientStream's single-reader lifetime and terminal-cause semantics.
enum ClientSource {
    Shared {
        rx: mpsc::Receiver<pb::Stream>,
        identity: Arc<ClientStreams>,
        stream_id: String,
    },
    Dedicated {
        subscription: RpcSubscription,
        incoming: Channel,
        bus: RpcBus,
    },
}
impl ClientSource {
    async fn next(&mut self) -> Result<Option<pb::Stream>, RpcError> {
        match self {
            Self::Shared { rx, identity, .. } => match rx.recv().await {
                Some(frame) => Ok(Some(frame)),
                None => Err(identity.terminal_error().unwrap_or_else(closed)),
            },
            Self::Dedicated {
                subscription, bus, ..
            } => loop {
                let bytes = subscription.recv().await.map_err(RpcError::from)?;
                if let Ok(value) = bus.decode::<pb::Stream>(&bytes)
                    && value.message.expiry > now()
                {
                    return Ok(Some(value.message));
                }
            },
        }
    }
    async fn cleanup(self) {
        match self {
            Self::Shared {
                identity,
                stream_id,
                ..
            } => identity.unregister(&stream_id),
            Self::Dedicated {
                subscription,
                incoming,
                ..
            } => {
                let _ = subscription.unsubscribe(&incoming).await;
            }
        }
    }
}

/// Open/claim/ACK completes before a session is returned. Dropping this future
/// closes its partially opened stream rather than leaking a registered receiver.
pub async fn open_stream<
    S: Message + Default + prost::Name,
    R: Message + Default + prost::Name + Send + 'static,
>(
    bus: impl Into<RpcBus>,
    spec: &MethodSpec,
    config: StreamConfig,
    options: CallOptions,
) -> Result<RpcStream<S, R>, RpcError> {
    let config = config.validate()?;
    let bus = bus.into();
    let identity = options.client.clone();
    let client = identity
        .as_ref()
        .map(|identity| identity.client_id().to_owned())
        .unwrap_or_else(|| id(""));
    let incoming = channels::stream_client(&spec.service, &client);
    // A shared nodeID-keyed identity already owns the STR subscription; only a
    // random per-call client opens its own dedicated one.
    let subscription = if identity.is_none() {
        Some(bus.subscribe::<pb::Stream>(&incoming, false).await?)
    } else {
        None
    };
    let claim_target = channels::claim_request(&spec.service, &client);
    let mut claims = if spec.require_claim {
        Some(
            bus.subscribe::<pb::ClaimRequest>(&claim_target, false)
                .await?,
        )
    } else {
        None
    };
    let mut stream = make_stream::<S, R>(
        bus.clone(),
        stream_channel(spec),
        id(""),
        config,
        Weak::new(),
        None,
    );
    let mut request = frame(
        stream.id(),
        options.timeout,
        Body::Open(pb::StreamOpen {
            node_id: client,
            metadata: options.metadata.clone().into_iter().collect(),
        }),
    );
    if let Some(expiry) = options.expiry_ns {
        request.expiry = request.expiry.min(expiry);
    }
    let requested_deadline = Instant::now()
        .checked_add(options.timeout)
        .ok_or_else(|| RpcError::new("invalid_argument", "stream timeout overflow"))?;
    let deadline = options.deadline.map_or(requested_deadline, |deadline| {
        deadline.min(requested_deadline)
    });
    request.expiry = request.expiry.min(
        now().saturating_add(
            deadline
                .saturating_duration_since(Instant::now())
                .as_nanos()
                .min(i64::MAX as u128) as i64,
        ),
    );
    let (ack, waiting) = oneshot::channel();
    lock(&stream.session.state)
        .acks
        .insert(request.request_id.clone(), ack);
    let opening_waiter = AckWaiter {
        session: stream.session.clone(),
        request: request.request_id.clone(),
    };
    let session = stream.session.clone();
    let lifetime_cancel = options.cancel.clone();
    let lifetime_deadline = options.deadline;
    let mut source = match identity {
        Some(identity) => {
            let (sender, rx) = mpsc::channel(config.channel_size);
            let stream_id = stream.id().to_owned();
            // Register before publishing Open so the server's ACK cannot race
            // the demux registration.
            identity.register(&stream_id, sender);
            ClientSource::Shared {
                rx,
                identity,
                stream_id,
            }
        }
        None => ClientSource::Dedicated {
            subscription: subscription.expect("dedicated client owns its STR subscription"),
            incoming,
            bus: bus.clone(),
        },
    };
    stream.reader = Some(tokio::spawn(async move {
        loop {
            tokio::select! {
                biased;
                _=session.stop.cancelled()=>break,
                _=lifetime_cancel.cancelled()=>{session.cancel_peer(canceled()).await;break;},
                _=async{match lifetime_deadline{Some(until)=>tokio::time::sleep_until(until.into()).await,None=>std::future::pending().await}}=>{session.cancel_peer(timeout()).await;break;},
                value=source.next()=>match value{
                    Ok(Some(value))=>{
                        tokio::select!{biased;_=session.stop.cancelled()=>break,_=lifetime_cancel.cancelled()=>{session.cancel_peer(canceled()).await;break;},_=session.handle(value)=>{}}
                    }
                    Ok(None)=>break,
                    Err(error)=>{session.finish(error);break;}
                }
            }
        }
        source.cleanup().await;
    }));
    let operation = async {
        bus.publish(&stream.session.target, &request).await?;
        if let Some(claims) = claims.as_mut() {
            let mut state = SelectionState::default();
            let mut until = options
                .selection
                .affinity_timeout
                .and_then(|d| Instant::now().checked_add(d))
                .unwrap_or(deadline)
                .min(deadline);
            let mut shorted = false;
            let selected = loop {
                tokio::select! {
                    biased;
                    _=options.cancel.cancelled()=>return Err(canceled()),
                    _=tokio::time::sleep_until(until.into())=>break state.finish()?,
                    bytes=claims.recv()=>{
                        let claim=bus.decode::<pb::ClaimRequest>(&bytes?).map_err(|_|RpcError::new("malformed_response","invalid stream claim"))?.message;
                        if claim.request_id!=request.request_id {continue;}
                        if let Some(selected)=state.claim(claim,options.selection){break selected;}
                        if !shorted && state.has_eligible && let Some(short)=options.selection.short_circuit_timeout{
                            until=until.min(Instant::now().checked_add(short).unwrap_or(until));shorted=true;
                        }
                    }
                }
            };
            bus.publish(
                &claim_channel(spec),
                &pb::ClaimResponse {
                    request_id: request.request_id.clone(),
                    server_id: selected.server,
                },
            )
            .await?;
        }
        tokio::select! {
            biased;
            _=options.cancel.cancelled()=>Err(canceled()),
            _=stream.session.stop.cancelled()=>Err(stream.error().unwrap_or_else(closed)),
            _=waiting=>match stream.error(){Some(e)=>Err(e),None=>Ok(())},
        }
    };
    let result = tokio::time::timeout_at(deadline.into(), operation)
        .await
        .map_err(|_| timeout())
        .and_then(|r| r);
    drop(opening_waiter);
    if let Some(claims) = claims.take() {
        let _ = claims.unsubscribe(&claim_target).await;
    }
    if let Err(error) = result {
        let _ = stream.close(Some(error.clone())).await;
        return Err(error);
    }
    Ok(stream)
}

/// Registers a method's single server stream subscription before returning.
/// Each accepted stream is independently owned by its consumer; listener close
/// drains all streams and joins all outstanding claim/open handlers.
pub struct StreamListener<S, R> {
    incoming: mpsc::Receiver<RpcStream<S, R>>,
    stop: Cancellation,
    task: Option<JoinHandle<()>>,
}
impl<S, R> StreamListener<S, R> {
    pub async fn accept(&mut self) -> Option<RpcStream<S, R>> {
        self.incoming.recv().await
    }
    pub async fn close(&mut self) {
        self.stop.cancel();
        if let Some(task) = self.task.take() {
            let _ = task.await;
        }
    }
}
impl<S, R> Drop for StreamListener<S, R> {
    fn drop(&mut self) {
        self.stop.cancel();
    }
}
struct OpenId {
    pending: Arc<Mutex<HashSet<String>>>,
    id: String,
}
impl Drop for OpenId {
    fn drop(&mut self) {
        lock(&self.pending).remove(&self.id);
    }
}
struct ClaimWaiter {
    claims: Claims,
    id: String,
}
impl Drop for ClaimWaiter {
    fn drop(&mut self) {
        lock(&self.claims).remove(&self.id);
    }
}
/// Affinity reads the incoming header before claiming; negative values decline.
pub type StreamAffinity = Arc<dyn Fn(&StreamHeader) -> f32 + Send + Sync>;
pub async fn register_stream<
    S: Message + Default + prost::Name + Send + 'static,
    R: Message + Default + prost::Name + Send + 'static,
>(
    bus: impl Into<RpcBus>,
    spec: MethodSpec,
    config: StreamConfig,
    affinity: Option<StreamAffinity>,
) -> Result<StreamListener<S, R>, RpcError> {
    let config = config.validate()?;
    let bus = bus.into();
    let target = stream_channel(&spec);
    let mut subscription = bus.subscribe::<pb::Stream>(&target, false).await?;
    let claim_target = claim_channel(&spec);
    let mut claim_subscription = if spec.require_claim {
        Some(
            bus.subscribe::<pb::ClaimResponse>(&claim_target, false)
                .await?,
        )
    } else {
        None
    };
    let sessions: Sessions = Arc::new(Mutex::new(HashMap::new()));
    let claims: Claims = Arc::new(Mutex::new(HashMap::new()));
    let server = id("");
    let pending = Arc::new(Mutex::new(HashSet::new()));
    let stop = Cancellation::default();
    let stopping = stop.clone();
    let slots = Arc::new(Semaphore::new(config.max_streams));
    let (accepted, incoming) = mpsc::channel(config.max_streams);
    let task = tokio::spawn(async move {
        let mut opens = JoinSet::new();
        loop {
            tokio::select! {
                biased;
                _=stopping.cancelled()=>break,
                _=opens.join_next(),if !opens.is_empty()=>{},
                response=async{match claim_subscription.as_mut(){Some(sub)=>sub.recv().await,None=>std::future::pending().await}}=>{
                    match response {
                        Ok(bytes)=>if let Ok(response)=bus.decode::<pb::ClaimResponse>(&bytes){if let Some(waiter)=lock(&claims).remove(&response.message.request_id){let _=waiter.send(response.message);}},
                        Err(_)=>break,
                    }
                }
                value=subscription.recv()=>{
                    let bytes=match value{Ok(bytes)=>bytes,Err(_)=>break};
                    let value=match bus.decode::<pb::Stream>(&bytes){Ok(value)=>value.message,Err(_)=>continue};
                    if value.expiry<=now(){continue;}
                    if let Some(Body::Open(open))=&value.body {
                        if lock(&sessions).contains_key(&value.stream_id){continue;}
                        let Ok(slot)=slots.clone().try_acquire_owned()else{continue;};
                        if !lock(&pending).insert(value.stream_id.clone()){continue;}
                        let opening=OpenId{pending:pending.clone(),id:value.stream_id.clone()};
                        let header=StreamHeader{remote_id:open.node_id.clone(),sent_at_ns:value.sent_at,metadata:open.metadata.clone().into_iter().collect()};
                        let score=affinity.as_ref().map_or(1.0,|f|f(&header));
                        if score<0.0 && spec.require_claim{continue;}
                        let bus=bus.clone();let sessions=sessions.clone();let claims=claims.clone();let accepted=accepted.clone();let server=server.clone();let spec=spec.clone();let stopping=stopping.clone();
                        opens.spawn(async move {
                            let _opening=opening;
                            let budget=Duration::from_nanos(value.expiry.saturating_sub(now()).max(0) as u64);
                            let operation=async {
                                if spec.require_claim {
                                    let (send,recv)=oneshot::channel();
                                    lock(&claims).insert(value.request_id.clone(),send);
                                    let _waiter=ClaimWaiter{claims:claims.clone(),id:value.request_id.clone()};
                                    bus.publish(&channels::claim_request(&spec.service,&header.remote_id),&pb::ClaimRequest{request_id:value.request_id.clone(),server_id:server.clone(),affinity:score,handling:false}).await?;
                                    let grant=recv.await.map_err(|_|closed())?;
                                    if grant.server_id!=server{return Ok(());}
                                }
                                let peer=channels::stream_client(&spec.service,&header.remote_id);
                                let mut stream=make_stream::<S,R>(bus.clone(),peer.clone(),value.stream_id.clone(),config,Arc::downgrade(&sessions),Some(slot));
                                stream.header=Some(header);
                                lock(&sessions).insert(value.stream_id.clone(),stream.session.clone());
                                let ack=pb::Stream{stream_id:value.stream_id,request_id:value.request_id,sent_at:value.sent_at,expiry:value.expiry,body:Some(Body::Ack(pb::StreamAck{}))};
                                bus.publish(&peer,&ack).await?;
                                if let Err(error)=accepted.try_send(stream){
                                    let mut stream=error.into_inner();
                                    let _=stream.close(Some(RpcError::new("unavailable","stream accept queue is full"))).await;
                                }
                                Ok::<(),RpcError>(())
                            };
                            tokio::select!{biased;_=stopping.cancelled()=>{},_=tokio::time::timeout(budget,operation)=>{}}
                        });
                    }else{
                        let session=lock(&sessions).get(&value.stream_id).cloned();
                        if let Some(session)=session{
                            // Drop must interrupt an ACK publish blocked by a
                            // peer transport, rather than waiting for an arbitrary
                            // peer-supplied expiry before retiring subscriptions.
                            tokio::select!{biased;_=stopping.cancelled()=>break,_=session.handle(value)=>{}}
                        }
                    }
                }
            }
        }
        stopping.cancel();
        opens.abort_all();
        while opens.join_next().await.is_some() {}
        lock(&claims).clear();
        let retired = lock(&sessions).values().cloned().collect::<Vec<_>>();
        for stream in retired {
            let _ = stream.close(RpcError::new("unavailable", "EOF")).await;
        }
        let _ = subscription.unsubscribe(&target).await;
        if let Some(sub) = claim_subscription {
            let _ = sub.unsubscribe(&claim_target).await;
        }
    });
    Ok(StreamListener {
        incoming,
        stop,
        task: Some(task),
    })
}

#[cfg(test)]
mod tests;
