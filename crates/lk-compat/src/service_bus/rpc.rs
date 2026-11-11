//! Bounded single-response psrpc client/server lifecycle over real Redis.
//! Multi/streams, retries, reconnect and service-specific IOInfo effects remain
//! separate; no public LiveKit route is enabled by constructing this module.

#[cfg(test)]
use super::redis_transport::RedisTransport;
use super::redis_transport::TransportError;
use super::rpc_transport::{RpcBus, RpcSubscription as Subscription};
use super::{channels, wire};
use futures_util::future::BoxFuture;
use lk_protocol::psrpc::internal as pb;
use prost::Message;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::future::Future;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::sync::{Semaphore, oneshot, watch};
use tokio::task::{JoinHandle, JoinSet};

mod detail_projection;
mod local_client;
mod multi;
pub use multi::MultiResponse;
mod response_provenance;
use local_client::{LocalCall, LocalClient};
use response_provenance::ReceivedResponse;

#[derive(Clone, Debug, PartialEq)]
struct ErrorDetailProvenance {
    wires: Vec<Vec<u8>>,
    known: Vec<pbjson_types::Any>,
}

/// Local provenance of an error, never transmitted as a PSRPC field. Identical
/// peer-supplied code/text cannot impersonate a local transport/lifecycle result.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RpcErrorOrigin {
    Application,
    Remote,
    NoResponse,
    Transport,
    Canceled,
    Deadline,
    Closed,
}

#[derive(Clone, Debug, PartialEq, thiserror::Error)]
#[error("{code}: {message}")]
pub struct RpcError {
    pub origin: RpcErrorOrigin,
    pub code: String,
    pub message: String,
    pub details: Vec<pbjson_types::Any>,
    /// Local application context for adapters; never serialized onto PSRPC.
    pub application_metadata: BTreeMap<String, String>,
    // The optional received-wire cohort is owned out of line so common errors
    // remain small without weakening provenance or changing wire semantics.
    detail_provenance: Option<Box<ErrorDetailProvenance>>,
}

/// Application affinity for one decoded request; the selected handler remains
/// owned by the server until an actual grant or valid handling gate.
pub type RequestAffinity<Request> = Arc<dyn Fn(&Request) -> f32 + Send + Sync>;
impl RpcError {
    pub fn new(code: &str, message: impl Into<String>) -> Self {
        Self {
            origin: RpcErrorOrigin::Application,
            code: code.into(),
            message: message.into(),
            details: vec![],
            application_metadata: BTreeMap::new(),
            detail_provenance: None,
        }
    }
    pub fn with_application_metadata(
        mut self,
        key: impl Into<String>,
        value: impl Into<String>,
    ) -> Self {
        self.application_metadata.insert(key.into(), value.into());
        self
    }
    fn with_origin(mut self, origin: RpcErrorOrigin) -> Self {
        self.origin = origin;
        self
    }
    pub fn is_local_no_response(&self) -> bool {
        self.origin == RpcErrorOrigin::NoResponse
    }
    pub fn is_transport(&self) -> bool {
        self.origin == RpcErrorOrigin::Transport
    }
    pub fn is_remote(&self) -> bool {
        self.origin == RpcErrorOrigin::Remote
    }
    fn no_response() -> Self {
        Self::new("unavailable", "no response from servers").with_origin(RpcErrorOrigin::NoResponse)
    }
    fn canceled() -> Self {
        Self::new("canceled", "request canceled").with_origin(RpcErrorOrigin::Canceled)
    }
    fn timed_out() -> Self {
        Self::new("deadline_exceeded", "request timed out").with_origin(RpcErrorOrigin::Deadline)
    }
    pub(crate) fn closed() -> Self {
        Self::new("canceled", "client is closed").with_origin(RpcErrorOrigin::Closed)
    }
    fn remote(response: pb::Response) -> Self {
        Self {
            origin: RpcErrorOrigin::Remote,
            code: response.code,
            message: response.error,
            details: response.error_details,
            application_metadata: BTreeMap::new(),
            detail_provenance: None,
        }
    }
    fn remote_received(response: ReceivedResponse) -> Self {
        let mut error = Self::remote(response.message);
        error.detail_provenance = Some(Box::new(ErrorDetailProvenance {
            wires: response.error_detail_wire,
            known: error.details.clone(),
        }));
        error
    }
    /// Original Any-wrapper provenance belongs to this exact repeated cohort.
    /// Directly mutating/reordering public typed details must not silently attach
    /// unrelated unknowns. Local typed-only errors have no raw sidecar.
    pub(crate) fn error_detail_wires(&self) -> Result<Vec<Vec<u8>>, Self> {
        match &self.detail_provenance {
            Some(source)
                if source.known != self.details || source.wires.len() != self.details.len() =>
            {
                Err(Self::new(
                    "internal",
                    "error detail provenance cohort mismatch",
                ))
            }
            Some(source) => Ok(source.wires.clone()),
            None => Ok(self.details.iter().map(Message::encode_to_vec).collect()),
        }
    }
    /// Explicitly update one same-source Any known projection while retaining
    /// that wrapper's unknowns. Cohort insertion/removal is never inferred.
    pub fn replace_error_detail(
        &mut self,
        index: usize,
        value: pbjson_types::Any,
    ) -> Result<(), Self> {
        self.error_detail_wires()?;
        if index >= self.details.len() {
            return Err(Self::new(
                "invalid_argument",
                "error detail index out of range",
            ));
        }
        if let Some(source) = &mut self.detail_provenance {
            source.wires[index] = detail_projection::overlay_any(&source.wires[index], &value)
                .map_err(|_| Self::new("internal", "invalid error detail provenance"))?;
            source.known[index] = value.clone();
        }
        self.details[index] = value;
        Ok(())
    }
    fn malformed_response(message: impl Into<String>) -> Self {
        Self::new("malformed_response", message).with_origin(RpcErrorOrigin::Transport)
    }
}
impl From<TransportError> for RpcError {
    fn from(error: TransportError) -> Self {
        Self::new("unavailable", error.to_string()).with_origin(RpcErrorOrigin::Transport)
    }
}

/// Local cancellation has no invented cancellation wire frame. Server-side
/// cancellation follows the transmitted expiry (or owned server shutdown).
#[derive(Clone, Debug)]
pub struct Cancellation(watch::Sender<bool>);
impl Default for Cancellation {
    fn default() -> Self {
        Self(watch::channel(false).0)
    }
}
impl Cancellation {
    pub fn cancel(&self) {
        self.0.send_replace(true);
    }
    pub fn is_cancelled(&self) -> bool {
        *self.0.borrow()
    }
    pub async fn cancelled(&self) {
        let mut receiver = self.0.subscribe();
        while !*receiver.borrow_and_update() {
            if receiver.changed().await.is_err() {
                break;
            }
        }
    }
}

#[derive(Clone, Debug)]
pub struct MethodSpec {
    pub service: String,
    pub method: String,
    pub topics: Vec<String>,
    pub require_claim: bool,
    pub queue: bool,
}
impl MethodSpec {
    fn topics(&self) -> Vec<&str> {
        self.topics.iter().map(String::as_str).collect()
    }
    pub(crate) fn request_channel(&self) -> channels::Channel {
        channels::request(&self.service, &self.method, &self.topics(), self.queue)
    }
    fn claim_channel(&self) -> channels::Channel {
        channels::claim_response(&self.service, &self.method, &self.topics())
    }
}

#[derive(Clone, Copy, Debug, Default)]
pub struct SelectionOptions {
    pub minimum_affinity: f32,
    pub maximum_affinity: f32,
    pub accept_first: bool,
    pub affinity_timeout: Option<Duration>,
    pub short_circuit_timeout: Option<Duration>,
}

#[derive(Clone, Debug)]
pub struct CallOptions {
    pub timeout: Duration,
    pub deadline: Option<Instant>,
    /// Original absolute PSRPC expiry, when inherited from an accepted request.
    /// It is an upper bound, independent of wall/monotonic conversion rounding.
    pub expiry_ns: Option<i64>,
    pub cancel: Cancellation,
    pub metadata: BTreeMap<String, String>,
    pub selection: SelectionOptions,
}
impl CallOptions {
    pub fn new(timeout: Duration) -> Self {
        Self {
            timeout,
            deadline: None,
            expiry_ns: None,
            cancel: Cancellation::default(),
            metadata: BTreeMap::new(),
            selection: SelectionOptions::default(),
        }
    }
}

#[derive(Clone, Copy)]
struct CallTiming {
    deadline: Instant,
    expiry_ns: i64,
    sent_at_ns: i64,
}
#[derive(Default)]
struct CallSubscriptions {
    responses: Option<Subscription>,
    claims: Option<Subscription>,
}
struct CallState<'a> {
    subscriptions: &'a mut CallSubscriptions,
    local_call: Option<&'a mut LocalCall>,
}
pub struct RpcClient {
    bus: RpcBus,
    client_id: String,
    tracing: super::telemetry::RpcTracing,
    slots: Arc<Semaphore>,
    pending: Arc<Mutex<HashSet<String>>>,
    close: Cancellation,
    local_clients: Mutex<HashMap<String, Arc<LocalClient>>>,
}
struct PendingGuard {
    pending: Arc<Mutex<HashSet<String>>>,
    id: String,
}
impl Drop for PendingGuard {
    fn drop(&mut self) {
        self.pending
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .remove(&self.id);
    }
}
impl RpcClient {
    pub fn new(bus: impl Into<RpcBus>, max_pending: usize) -> Result<Self, RpcError> {
        if max_pending == 0 || max_pending > Semaphore::MAX_PERMITS {
            return Err(RpcError::new(
                "invalid_argument",
                "pending limit must be positive",
            ));
        }
        Ok(Self {
            bus: bus.into(),
            client_id: rtc_core::ids::new_sid("client_"),
            tracing: super::telemetry::RpcTracing::capture(),
            slots: Arc::new(Semaphore::new(max_pending)),
            pending: Arc::new(Mutex::new(HashSet::new())),
            close: Cancellation::default(),
            local_clients: Mutex::new(HashMap::new()),
        })
    }
    /// Install explicit fixed-Go constructor OTel options; global provider alone is inert.
    pub fn with_tracing(mut self, tracing: super::telemetry::RpcTracing) -> Self {
        self.tracing = tracing;
        self
    }
    /// Source-shaped local ServiceDefinition constructor. This synchronous host
    /// adapter starts one actual persistent setup task; ready/operations await
    /// its real registration result, never a fabricated readiness receipt.
    pub fn new_for_service(
        bus: impl Into<RpcBus>,
        max_pending: usize,
        service: &str,
    ) -> Result<Self, RpcError> {
        let client = Self::new(bus, max_pending)?;
        if client.bus.is_local() {
            client.local_service(service)?;
        }
        Ok(client)
    }
    fn local_service(&self, service: &str) -> Result<Arc<LocalClient>, RpcError> {
        let mut services = self.local_clients.lock().unwrap_or_else(|e| e.into_inner());
        if self.close.is_cancelled() {
            return Err(RpcError::closed());
        }
        if let Some(client) = services.get(service) {
            return Ok(client.clone());
        }
        let client = LocalClient::start(
            self.bus.clone(),
            service.to_owned(),
            self.client_id.clone(),
            self.close.clone(),
        )?;
        services.insert(service.to_owned(), client.clone());
        Ok(client)
    }
    pub(crate) async fn ready_for_service(&self, service: &str) -> Result<(), RpcError> {
        if self.bus.is_local() {
            self.local_service(service)?.ready().await?;
        }
        Ok(())
    }
    pub(crate) async fn close_and_wait(&self) {
        self.close();
        let services: Vec<_> = self
            .local_clients
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .values()
            .cloned()
            .collect();
        for service in services {
            service.join().await;
        }
    }
    pub(crate) fn message_bus(&self) -> RpcBus {
        self.bus.clone()
    }

    pub fn pending_count(&self) -> usize {
        self.pending.lock().unwrap_or_else(|e| e.into_inner()).len()
    }
    pub fn close(&self) {
        self.close.cancel();
    }

    pub async fn unary<Request, Response>(
        &self,
        spec: &MethodSpec,
        request: &Request,
        options: CallOptions,
    ) -> Result<Response, RpcError>
    where
        Request: Message,
        Response: Message + Default,
    {
        self.unary_decoded(spec, request, None, options, |raw| {
            Response::decode(raw).map_err(|error| RpcError::malformed_response(error.to_string()))
        })
        .await
        .map(|response| response.message)
    }

    /// Keep the actual successful response payload while projecting only pinned
    /// known fields. The peer's original unknown bytes never pass through a
    /// typed encode/decode round trip before reaching a service-owned sidecar.
    pub async fn unary_with_wire<Request, Response>(
        &self,
        spec: &MethodSpec,
        request: &Request,
        options: CallOptions,
    ) -> Result<RawResponse<Response>, RpcError>
    where
        Request: Message,
        Response: Message + prost::Name + Default,
    {
        self.unary_decoded(spec, request, None, options, |raw| {
            lk_protocol::wire::decode(raw)
                .map_err(|error| RpcError::malformed_response(error.to_string()))
        })
        .await
    }

    /// Send the request-owned original payload, after verifying that its pinned
    /// known projection matches the supplied message. Unknown child fields are
    /// therefore preserved without reintroducing stale known values.
    pub async fn unary_raw_request<Request, Response>(
        &self,
        spec: &MethodSpec,
        request: &RawRequest<Request>,
        mut options: CallOptions,
    ) -> Result<RawResponse<Response>, RpcError>
    where
        Request: Message + prost::Name + Default + PartialEq,
        Response: Message + prost::Name + Default,
    {
        if self.close.is_cancelled() {
            return Err(RpcError::closed());
        }
        let deadline = Instant::now()
            .checked_add(options.timeout)
            .map(|d| options.deadline.map_or(d, |explicit| d.min(explicit)));
        let span = self.tracing.client(spec, &mut options.metadata, deadline);
        span.run(async {
            if request.raw.len() > self.bus.limits().wire.decompressed_bytes {
                return Err(RpcError::new(
                    "resource_exhausted",
                    "request exceeds payload limit",
                ));
            }
            validate_raw_request(request)?;
            Box::pin(self.unary_attempt(
                spec,
                &request.message,
                Some(&request.raw),
                options,
                |raw| {
                    lk_protocol::wire::decode(raw)
                        .map_err(|error| RpcError::malformed_response(error.to_string()))
                },
            ))
            .await
        })
        .await
    }

    async fn unary_decoded<Request, Response>(
        &self,
        spec: &MethodSpec,
        request: &Request,
        raw_request: Option<&[u8]>,
        mut options: CallOptions,
        decode: impl FnOnce(&[u8]) -> Result<Response, RpcError>,
    ) -> Result<RawResponse<Response>, RpcError>
    where
        Request: Message,
        Response: Message + Default,
    {
        // Go rejects an already closed client before invoking middleware.
        if self.close.is_cancelled() {
            return Err(RpcError::closed());
        }
        let deadline = Instant::now()
            .checked_add(options.timeout)
            .map(|d| options.deadline.map_or(d, |explicit| d.min(explicit)));
        let span = self.tracing.client(spec, &mut options.metadata, deadline);
        span.run(Box::pin(self.unary_attempt(
            spec,
            request,
            raw_request,
            options,
            decode,
        )))
        .await
    }

    async fn unary_attempt<Request, Response>(
        &self,
        spec: &MethodSpec,
        request: &Request,
        raw_request: Option<&[u8]>,
        options: CallOptions,
        decode: impl FnOnce(&[u8]) -> Result<Response, RpcError>,
    ) -> Result<RawResponse<Response>, RpcError>
    where
        Request: Message,
        Response: Message + Default,
    {
        if self.close.is_cancelled() {
            return Err(RpcError::closed());
        }
        if spec.queue
            && (options.selection.minimum_affinity > 0.0
                || options.selection.maximum_affinity > 0.0)
        {
            return Err(RpcError::new(
                "invalid_argument",
                format!(
                    "{}: affinity selection is not valid on a queue rpc",
                    spec.method
                ),
            ));
        }
        if raw_request.map_or_else(|| request.encoded_len(), <[u8]>::len)
            > self.bus.limits().wire.decompressed_bytes
        {
            return Err(RpcError::new(
                "resource_exhausted",
                "request exceeds payload limit",
            ));
        }
        let _slot =
            self.slots.clone().try_acquire_owned().map_err(|_| {
                RpcError::new("resource_exhausted", "pending request limit reached")
            })?;
        let local = if self.bus.is_local() {
            let service = self.local_service(&spec.service)?;
            service.ready().await?;
            if self.close.is_cancelled() {
                return Err(RpcError::closed());
            }
            Some(service)
        } else {
            None
        };
        let now = Instant::now();
        let timeout_deadline = now
            .checked_add(options.timeout)
            .ok_or_else(|| RpcError::new("invalid_argument", "request timeout overflow"))?;
        let deadline = options
            .deadline
            .map_or(timeout_deadline, |d| d.min(timeout_deadline));
        if deadline <= now && !self.bus.is_local() {
            return Err(RpcError::timed_out());
        }
        // Capture once before subscription/claim awaits. An inherited wire
        // expiry is never reconstructed from a monotonic duration.
        let sent_at_ns = unix_ns();
        let expiry_ns = call_expiry_ns(
            now,
            sent_at_ns,
            timeout_deadline,
            options.deadline,
            options.expiry_ns,
        );
        let id = rtc_core::ids::new_sid("request_");
        self.pending
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .insert(id.clone());
        let _pending = PendingGuard {
            pending: self.pending.clone(),
            id: id.clone(),
        };
        let raw = raw_request.map_or_else(|| request.encode_to_vec(), <[u8]>::to_vec);
        let mut subscriptions = CallSubscriptions::default();
        let mut local_call = local
            .as_ref()
            .map(|client| client.request(&id, spec.require_claim));
        // This state machine contains both local and Redis subscription/claim
        // branches. Heap-own it once, so tracing/typed callers and public HTTP
        // wrappers do not inline its maximum frame at every poll layer.
        let call = Box::pin(self.call(
            spec,
            raw,
            &id,
            CallTiming {
                deadline,
                expiry_ns,
                sent_at_ns,
            },
            &options,
            CallState {
                subscriptions: &mut subscriptions,
                local_call: local_call.as_mut(),
            },
        ));
        let result = if self.bus.is_local() {
            // Original localbus ignores publishing caller ctx. Registration and
            // Publish complete before RequestSingle enters ctx selection. Never
            // withdraw a real bounded send by dropping its future here.
            call.await
        } else {
            tokio::select! {
                biased;
                _=options.cancel.cancelled()=>Err(RpcError::canceled()),
                _=self.close.cancelled()=>Err(RpcError::closed()),
                _=tokio::time::sleep_until(deadline.into())=>Err(RpcError::timed_out()),
                result=call=>result,
            }
        };
        // Fixed newRPC completion deletes only per-request routing entries.
        // Persistent local service subscriptions retire only on client Close.
        drop(local_call);
        // Existing Redis drop boundary remains before decoding the result.
        drop(subscriptions);
        let response = result?;
        if !response.message.error.is_empty() {
            return Err(RpcError::remote_received(response));
        }
        decode_response(response.message.raw_response, decode)
    }

    async fn call(
        &self,
        spec: &MethodSpec,
        raw: Vec<u8>,
        id: &str,
        timing: CallTiming,
        options: &CallOptions,
        state: CallState<'_>,
    ) -> Result<ReceivedResponse, RpcError> {
        let CallState {
            subscriptions,
            mut local_call,
        } = state;
        let CallTiming {
            deadline,
            expiry_ns,
            sent_at_ns,
        } = timing;
        // Redis preserves its original per-call registrations and codec path.
        // Local maps were installed into the already-ready persistent client.
        if local_call.is_none() {
            subscriptions.responses = Some(
                self.bus
                    .subscribe::<pb::Response>(
                        &channels::response(&spec.service, &self.client_id),
                        false,
                    )
                    .await?,
            );
            subscriptions.claims = if spec.require_claim {
                Some(
                    self.bus
                        .subscribe::<pb::ClaimRequest>(
                            &channels::claim_request(&spec.service, &self.client_id),
                            false,
                        )
                        .await?,
                )
            } else {
                None
            };
        }
        let sent_at = if self.bus.is_local() {
            sent_at_ns
        } else {
            unix_ns()
        };
        if !self.bus.is_local() && expiry_ns <= sent_at {
            return Err(RpcError::timed_out());
        }
        let request = pb::Request {
            request_id: id.into(),
            client_id: self.client_id.clone(),
            sent_at,
            expiry: expiry_ns,
            multi: false,
            raw_request: raw,
            metadata: options.metadata.clone().into_iter().collect(),
            skip_claim: spec.queue,
            ..Default::default()
        };
        self.bus.publish(&spec.request_channel(), &request).await?;
        // Source creates its method timeout context after synchronous Publish.
        // An inherited absolute caller deadline still wins. Advertised expiry
        // above remains the original pre-publish value, never reminted.
        let deadline = if self.bus.is_local() {
            let timeout = Instant::now()
                .checked_add(options.timeout)
                .ok_or_else(|| RpcError::new("invalid_argument", "request timeout overflow"))?;
            options
                .deadline
                .map_or(timeout, |parent| parent.min(timeout))
        } else {
            deadline
        };
        let (mut local_responses, mut local_claims) = match local_call.as_mut() {
            Some(call) => (Some(&mut call.responses), call.claims.as_mut()),
            None => (None, None),
        };
        if spec.require_claim {
            let mut state = SelectionState::default();
            let started = Instant::now();
            let mut until = options
                .selection
                .affinity_timeout
                .and_then(|d| started.checked_add(d))
                .unwrap_or(deadline)
                .min(deadline);
            let mut shorted = false;
            let selected = loop {
                tokio::select! {
                    biased;
                    _=self.close.cancelled(),if self.bus.is_local()=>return Err(RpcError::closed()),
                    _=options.cancel.cancelled(),if self.bus.is_local()=>break state.finish()?,
                    _=tokio::time::sleep_until(until.into())=>break state.finish()?,
                    bytes=async{match local_claims.as_mut(){Some(receiver)=>receiver.recv().await.ok_or(TransportError::Disconnected),None=>subscriptions.claims.as_mut().expect("claim registered").recv().await}}=>{
                        let claim=self.bus.decode::<pb::ClaimRequest>(&bytes?).map_err(|_|RpcError::malformed_response("invalid claim envelope"))?.message;
                        if claim.request_id!=id {continue;}
                        if let Some(selection)=state.claim(claim,options.selection) {break selection;}
                        if !shorted && state.has_eligible && let Some(timeout)=options.selection.short_circuit_timeout {
                            until=until.min(Instant::now().checked_add(timeout).unwrap_or(until));shorted=true;
                        }
                    }
                    bytes=async{match local_responses.as_mut(){Some(receiver)=>receiver.recv().await.ok_or(TransportError::Disconnected),None=>subscriptions.responses.as_mut().expect("response registered").recv().await}}=>{
                        let response=received_response_for_bus(&self.bus,&bytes?)?;
                        if response.message.request_id!=id {continue;}
                        if response.message.error.is_empty() || spec.queue {return Ok(response);}
                        state.remote_error=Some(RpcError::remote_received(response));
                    }
                }
            };
            if !selected.handling {
                self.bus
                    .publish(
                        &spec.claim_channel(),
                        &pb::ClaimResponse {
                            request_id: id.into(),
                            server_id: selected.server,
                        },
                    )
                    .await?;
            }
        }
        loop {
            let bytes = if self.bus.is_local() {
                tokio::select! {
                    biased;
                    _=self.close.cancelled()=>return Err(RpcError::closed()),
                    _=options.cancel.cancelled()=>return Err(RpcError::canceled()),
                    _=tokio::time::sleep_until(deadline.into())=>return Err(RpcError::timed_out()),
                    bytes=local_responses.as_mut().expect("local response map").recv()=>bytes.ok_or(TransportError::Disconnected)?,
                }
            } else {
                subscriptions
                    .responses
                    .as_mut()
                    .expect("response registered")
                    .recv()
                    .await?
            };
            let response = received_response_for_bus(&self.bus, &bytes)?;
            if response.message.request_id == id {
                return Ok(response);
            }
        }
    }
}
impl Drop for RpcClient {
    fn drop(&mut self) {
        self.close.cancel();
    }
}

pub(crate) struct Selected {
    pub(crate) server: String,
    handling: bool,
}
#[derive(Default)]
pub(crate) struct SelectionState {
    best: Option<String>,
    affinity: f32,
    claims: usize,
    remote_error: Option<RpcError>,
    pub(crate) has_eligible: bool,
}
impl SelectionState {
    pub(crate) fn claim(
        &mut self,
        claim: pb::ClaimRequest,
        options: SelectionOptions,
    ) -> Option<Selected> {
        if claim.handling {
            return Some(Selected {
                server: claim.server_id,
                handling: true,
            });
        }
        self.claims += 1;
        if options.minimum_affinity <= 0.0 || claim.affinity >= options.minimum_affinity {
            self.has_eligible = true;
            if options.accept_first
                || options.maximum_affinity > 0.0 && claim.affinity >= options.maximum_affinity
            {
                return Some(Selected {
                    server: claim.server_id,
                    handling: false,
                });
            }
            if claim.affinity > self.affinity {
                self.affinity = claim.affinity;
                self.best = Some(claim.server_id);
            }
        }
        None
    }
    pub(crate) fn finish(self) -> Result<Selected, RpcError> {
        if let Some(server) = self.best {
            return Ok(Selected {
                server,
                handling: false,
            });
        }
        if let Some(error) = self.remote_error {
            return Err(error);
        }
        if self.claims > 0 {
            return Err(RpcError::new(
                "unavailable",
                format!("no servers available (received {} responses)", self.claims),
            ));
        }
        Err(RpcError::no_response())
    }
}

#[derive(Clone, Debug)]
pub struct RequestContext {
    pub remote_client: String,
    pub sent_at_ns: i64,
    /// Exact original Request.expiry; child requests must never exceed it.
    pub expiry_ns: i64,
    pub deadline: Instant,
    pub metadata: BTreeMap<String, String>,
}
#[derive(Clone, Copy, Debug)]
pub struct ServerLimits {
    pub concurrent: usize,
    pub max_request_duration: Duration,
    pub allow_skip_claim: bool,
}

/// The exact successful peer response payload and its known-field projection.
#[derive(Clone, Debug)]
pub struct RawResponse<Response> {
    pub message: Response,
    pub raw: Vec<u8>,
}
fn decode_response<Response>(
    raw: Vec<u8>,
    decode: impl FnOnce(&[u8]) -> Result<Response, RpcError>,
) -> Result<RawResponse<Response>, RpcError> {
    let message = decode(&raw)?;
    Ok(RawResponse { message, raw })
}

/// One actual request-owned payload plus its known-field typed projection.
/// The immutable bytes are from Request.raw_request, not a re-encoded message.
/// They remain bounded by the transport envelope and expire with handler owners.
#[derive(Clone, Debug)]
pub struct RawRequest<Request> {
    pub message: Request,
    pub raw: Arc<[u8]>,
}
type RequestDecoder<Request> = Arc<dyn Fn(&[u8]) -> Result<Request, RpcError> + Send + Sync>;
type ResponseEncoder<Response> = Arc<dyn Fn(Response) -> Result<Vec<u8>, RpcError> + Send + Sync>;
fn decode_request<Request>(
    raw: Vec<u8>,
    decode: &RequestDecoder<Request>,
) -> Result<RawRequest<Request>, RpcError> {
    let message = decode(&raw)?;
    Ok(RawRequest {
        message,
        raw: raw.into(),
    })
}
fn validate_raw_request<Request>(request: &RawRequest<Request>) -> Result<(), RpcError>
where
    Request: Message + prost::Name + Default + PartialEq,
{
    let actual: Request = lk_protocol::wire::decode(&request.raw)
        .map_err(|_| RpcError::new("malformed_request", "raw request projection is invalid"))?;
    if actual != request.message {
        return Err(RpcError::new(
            "invalid_argument",
            "raw request and typed projection disagree",
        ));
    }
    Ok(())
}
fn encode_raw_response<Response>(response: RawResponse<Response>) -> Result<Vec<u8>, RpcError>
where
    Response: Message + prost::Name + Default + PartialEq,
{
    let actual: Response = lk_protocol::wire::decode(&response.raw)
        .map_err(|_| RpcError::new("internal", "raw response projection is invalid"))?;
    if actual != response.message {
        return Err(RpcError::new(
            "internal",
            "raw response and typed projection disagree",
        ));
    }
    Ok(response.raw)
}

type Handler = Arc<
    dyn Fn(RequestContext, Vec<u8>) -> BoxFuture<'static, Result<Vec<u8>, RpcError>> + Send + Sync,
>;
type Validator = Arc<dyn Fn(&[u8]) -> Result<(), RpcError> + Send + Sync>;
type Affinity = Arc<dyn Fn(&[u8]) -> f32 + Send + Sync>;
type Claims = Arc<Mutex<HashMap<String, oneshot::Sender<pb::ClaimResponse>>>>;
struct ClaimGuard {
    claims: Claims,
    id: String,
}
impl Drop for ClaimGuard {
    fn drop(&mut self) {
        self.claims
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .remove(&self.id);
    }
}

/// Accepted handlers draining after a real unsubscribe acknowledgement.
/// Dropping this receipt detaches the bounded-by-expiry task; it does not abort it.
pub struct RpcRetirement {
    task: Option<JoinHandle<()>>,
}
impl RpcRetirement {
    pub(crate) fn is_finished(&self) -> bool {
        self.task.as_ref().is_none_or(JoinHandle::is_finished)
    }
    pub async fn finish(mut self) {
        if let Some(task) = self.task.take() {
            let _ = task.await;
        }
    }
}
pub struct RpcServer {
    #[cfg(test)]
    handler_started: Arc<std::sync::atomic::AtomicUsize>,
    #[cfg(test)]
    local_acceptance: Vec<super::local_transport::LocalRetirement>,
    retire: Cancellation,
    acceptance_closed: watch::Receiver<Option<Result<(), TransportError>>>,
    cancel: Cancellation,
    shutdown: Cancellation,
    task: Option<JoinHandle<()>>,
    claims: Claims,
}
impl RpcServer {
    /// Registration returns only after both real Redis subscriptions are ready.
    /// Existing callers retain the original generated-prost decode contract.
    pub async fn register<Request, Response, F, Fut>(
        bus: impl Into<RpcBus>,
        spec: MethodSpec,
        limits: ServerLimits,
        handler: F,
        affinity: Option<RequestAffinity<Request>>,
    ) -> Result<Self, RpcError>
    where
        Request: Message + Default + Send + 'static,
        Response: Message + Send + 'static,
        F: Fn(RequestContext, Request) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<Response, RpcError>> + Send + 'static,
    {
        let bus = bus.into();
        let decode: RequestDecoder<Request> = Arc::new(|raw| {
            Request::decode(raw)
                .map_err(|error| RpcError::new("malformed_request", error.to_string()))
        });
        Self::register_decoded(
            bus,
            spec,
            limits,
            false,
            move |context, request: RawRequest<Request>| handler(context, request.message),
            affinity,
            decode,
            Arc::new(|response: Response| Ok(response.encode_to_vec())),
        )
        .await
    }

    /// Register the generated multi-response method shape. A method is unary
    /// or multi before its subscription becomes visible, never toggled later.
    pub async fn register_multi<Request, Response, F, Fut>(
        bus: impl Into<RpcBus>,
        spec: MethodSpec,
        limits: ServerLimits,
        handler: F,
    ) -> Result<Self, RpcError>
    where
        Request: Message + Default + Send + 'static,
        Response: Message + Send + 'static,
        F: Fn(RequestContext, Request) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<Response, RpcError>> + Send + 'static,
    {
        if spec.require_claim {
            return Err(RpcError::new(
                "invalid_argument",
                "multi-response RPC cannot require a single owner",
            ));
        }
        Self::register_decoded(
            bus.into(),
            spec,
            limits,
            true,
            move |context, request: RawRequest<Request>| handler(context, request.message),
            None,
            Arc::new(|raw| {
                Request::decode(raw).map_err(|e| RpcError::new("malformed_request", e.to_string()))
            }),
            Arc::new(|response: Response| Ok(response.encode_to_vec())),
        )
        .await
    }

    /// Preserve original wire for store/forward compatibility while decoding the
    /// known projection using the pinned Go binary field-kind contract. This does
    /// not synthesize a successful reply, retain a global cache or enable a route.
    pub async fn register_with_raw<Request, Response, F, Fut>(
        bus: impl Into<RpcBus>,
        spec: MethodSpec,
        limits: ServerLimits,
        handler: F,
        affinity: Option<RequestAffinity<Request>>,
    ) -> Result<Self, RpcError>
    where
        Request: Message + prost::Name + Default + Send + 'static,
        Response: Message + Send + 'static,
        F: Fn(RequestContext, RawRequest<Request>) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<Response, RpcError>> + Send + 'static,
    {
        let bus = bus.into();
        let decode: RequestDecoder<Request> = Arc::new(|raw| {
            lk_protocol::wire::decode(raw)
                .map_err(|error| RpcError::new("malformed_request", error.to_string()))
        });
        Self::register_decoded(
            bus,
            spec,
            limits,
            false,
            handler,
            affinity,
            decode,
            Arc::new(|response: Response| Ok(response.encode_to_vec())),
        )
        .await
    }

    /// Preserve both request and reply wire. A handler must supply the typed
    /// projection of the same raw reply; disagreement is an application error,
    /// never silent forwarding of stale known fields.
    pub async fn register_with_wire<Request, Response, F, Fut>(
        bus: impl Into<RpcBus>,
        spec: MethodSpec,
        limits: ServerLimits,
        handler: F,
        affinity: Option<RequestAffinity<Request>>,
    ) -> Result<Self, RpcError>
    where
        Request: Message + prost::Name + Default + Send + 'static,
        Response: Message + prost::Name + Default + PartialEq + Send + 'static,
        F: Fn(RequestContext, RawRequest<Request>) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<RawResponse<Response>, RpcError>> + Send + 'static,
    {
        let bus = bus.into();
        let decode: RequestDecoder<Request> = Arc::new(|raw| {
            lk_protocol::wire::decode(raw)
                .map_err(|error| RpcError::new("malformed_request", error.to_string()))
        });
        Self::register_decoded(
            bus,
            spec,
            limits,
            false,
            handler,
            affinity,
            decode,
            Arc::new(encode_raw_response::<Response>),
        )
        .await
    }

    async fn register_decoded<Request, Response, F, Fut>(
        bus: RpcBus,
        spec: MethodSpec,
        limits: ServerLimits,
        multi: bool,
        handler: F,
        affinity: Option<RequestAffinity<Request>>,
        decode: RequestDecoder<Request>,
        encode: ResponseEncoder<Response>,
    ) -> Result<Self, RpcError>
    where
        Request: Message + Default + Send + 'static,
        Response: Send + 'static,
        F: Fn(RequestContext, RawRequest<Request>) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<Response, RpcError>> + Send + 'static,
    {
        if limits.concurrent == 0
            || limits.concurrent > Semaphore::MAX_PERMITS
            || limits.max_request_duration.is_zero()
        {
            return Err(RpcError::new(
                "invalid_argument",
                "server limits must be positive",
            ));
        }
        if spec.queue && affinity.is_some() {
            return Err(RpcError::new(
                "invalid_argument",
                format!(
                    "{}: affinity function is not valid on a queue rpc",
                    spec.method
                ),
            ));
        }
        let requests = bus
            .subscribe_server::<pb::Request>(&spec.request_channel(), spec.queue)
            .await?;
        let grants = if spec.require_claim {
            Some(
                bus.subscribe_server::<pb::ClaimResponse>(&spec.claim_channel(), false)
                    .await?,
            )
        } else {
            None
        };
        #[cfg(test)]
        let local_acceptance = requests
            .local_retirement()
            .into_iter()
            .chain(grants.as_ref().and_then(Subscription::local_retirement))
            .collect();
        let validate_decode = decode.clone();
        let validate: Validator = Arc::new(move |raw| validate_decode(raw).map(|_| ()));
        let affinity_decode = decode.clone();
        let affinity: Affinity = Arc::new(move |raw| match &affinity {
            Some(affinity) => affinity_decode(raw)
                .map(|request| affinity(&request))
                .unwrap_or(-1.0),
            None => 1.0,
        });
        let handler = Arc::new(handler);
        let tracing = super::telemetry::RpcTracing::capture();
        let tracing_spec = spec.clone();
        let invoke: Handler = Arc::new(move |ctx, raw| {
            let handler = handler.clone();
            let decode = decode.clone();
            let encode = encode.clone();
            let tracing = tracing.clone();
            let spec = tracing_spec.clone();
            Box::pin(async move {
                // Decode and claim precede upstream server middleware.
                let request = decode_request(raw, &decode)?;
                let span = tracing.server(&spec, &ctx.metadata, ctx.deadline);
                let response = span.run(async move { handler(ctx, request).await }).await?;
                // Go ends the handler span before encoding/sending response.
                encode(response)
            })
        });
        let cancel = Cancellation::default();
        let shutdown = Cancellation::default();
        let retire = Cancellation::default();
        let (acceptance_closed, acceptance_rx) = watch::channel(None);
        let claims = Arc::new(Mutex::new(HashMap::new()));
        #[cfg(test)]
        let handler_started = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let running = ServerRun {
            #[cfg(test)]
            handler_started: handler_started.clone(),
            multi,
            bus,
            spec,
            limits,
            handler: invoke,
            validate,
            affinity,
            claims: claims.clone(),
            cancel: cancel.clone(),
            shutdown: shutdown.clone(),
            retire: retire.clone(),
            acceptance_closed,
            server_id: rtc_core::ids::new_sid("server_"),
        };
        let task = tokio::spawn(running.run(requests, grants));
        Ok(Self {
            #[cfg(test)]
            local_acceptance,
            #[cfg(test)]
            handler_started,
            retire,
            acceptance_closed: acceptance_rx,
            cancel,
            shutdown,
            task: Some(task),
            claims,
        })
    }
    #[cfg(test)]
    pub(crate) fn handler_started_for_test(&self) -> usize {
        self.handler_started
            .load(std::sync::atomic::Ordering::SeqCst)
    }
    pub fn pending_claims(&self) -> usize {
        self.claims.lock().unwrap_or_else(|e| e.into_inner()).len()
    }
    /// Stop accepting requests, retaining claim delivery and waiting for every
    /// accepted handler to finish or its original transmitted expiry.
    pub async fn shutdown(mut self) {
        self.shutdown.cancel();
        if let Some(task) = self.task.take() {
            let _ = task.await;
        }
    }
    /// Source Multiton.Kill boundary: acknowledge stopping new requests while
    /// preserving already accepted work and its original request expiry.
    /// This does not change close() or shutdown() behavior for existing callers.
    /// Process-local owner callbacks cannot await external IO. The real local
    /// memberships can be removed synchronously, before a replacement registers;
    /// common server task still drains accepted handlers to their original expiry.
    #[cfg(test)]
    pub(crate) async fn retire_local(self) -> Result<RpcRetirement, RpcError> {
        if self.local_acceptance.is_empty() {
            return Err(RpcError::new("invalid_argument", "not a local RPC server"));
        }
        self.retire().await
    }
    pub async fn retire(mut self) -> Result<RpcRetirement, RpcError> {
        self.retire.cancel();
        let status = loop {
            let observed = self.acceptance_closed.borrow().clone();
            if let Some(status) = observed {
                break status;
            }
            self.acceptance_closed
                .changed()
                .await
                .map_err(|_| RpcError::closed())?;
        };
        status.map_err(RpcError::from)?;
        let task = self.task.take();
        // Drop retains its original force-cancel semantics; this retiring local
        // handle must no longer own the token held by the actual running task.
        self.cancel = Cancellation::default();
        Ok(RpcRetirement { task })
    }
    /// Force cancellation of accepted work (the existing Kill behavior).
    pub async fn close(mut self) {
        self.cancel.cancel();
        if let Some(task) = self.task.take() {
            let _ = task.await;
        }
    }
}
impl Drop for RpcServer {
    fn drop(&mut self) {
        self.cancel.cancel();
        if let Some(task) = self.task.take() {
            task.abort();
        }
    }
}

#[derive(Clone)]
struct ServerRun {
    #[cfg(test)]
    handler_started: Arc<std::sync::atomic::AtomicUsize>,
    multi: bool,
    bus: RpcBus,
    spec: MethodSpec,
    limits: ServerLimits,
    handler: Handler,
    validate: Validator,
    affinity: Affinity,
    claims: Claims,
    cancel: Cancellation,
    shutdown: Cancellation,
    retire: Cancellation,
    acceptance_closed: watch::Sender<Option<Result<(), TransportError>>>,
    server_id: String,
}
impl ServerRun {
    async fn run(self, requests: Subscription, mut grants: Option<Subscription>) {
        let mut requests = Some(requests);
        let mut draining = false;
        let slots = Arc::new(Semaphore::new(self.limits.concurrent));
        let mut tasks = JoinSet::new();
        loop {
            if draining && tasks.is_empty() {
                break;
            }
            tokio::select! {
                biased;
                _=self.cancel.cancelled()=>break,
                _=self.retire.cancelled(),if !draining=>{
                    draining = true;
                    let topics:Vec<_>=self.spec.topics.iter().map(String::as_str).collect();
                    let channel=channels::request(&self.spec.service,&self.spec.method,&topics,self.spec.queue);
                    let mut result=match requests.take(){Some(sub)=>sub.unsubscribe(&channel).await,None=>Ok(())};
                    // Fixed Go force-close also closes claim subscription. A
                    // pre-grant request can expire; an already entered app
                    // handler keeps running until its original expiry.
                    let claim_channel=channels::claim_response(&self.spec.service,&self.spec.method,&topics);
                    if let Some(sub)=grants.take(){let closed=sub.unsubscribe(&claim_channel).await;if result.is_ok(){result=closed;}}
                    let failed=result.is_err();
                    self.acceptance_closed.send_replace(Some(result));
                    if failed { break; }
                },
                _=self.shutdown.cancelled(),if !draining=>{
                    draining = true;
                    // Drop the actual subscription, not merely ignore new data.
                    requests.take();
                },
                Some(_)=tasks.join_next(),if !tasks.is_empty()=>{},
                received=async{match grants.as_mut(){Some(sub)=>sub.recv().await,None=>std::future::pending().await}}=>{
                    let Ok(bytes)=received else{break};
                    let Ok(grant)=self.bus.decode::<pb::ClaimResponse>(&bytes) else{continue};
                    let reply=self.claims.lock().unwrap_or_else(|e|e.into_inner()).remove(&grant.message.request_id);
                    if let Some(reply)=reply{let _=reply.send(grant.message);}
                }
                received=async{match requests.as_mut(){Some(sub)=>sub.recv().await,None=>std::future::pending().await}},if !draining=>{
                    let Ok(bytes)=received else{break};
                    let Ok(request)=self.bus.decode::<pb::Request>(&bytes) else{continue};
                    let request=request.message;
                    if request.expiry<=unix_ns() || request.multi!=self.multi {continue;}
                    let permit=slots.clone().try_acquire_owned();
                    let Ok(permit)=permit else {
                        // Bounded overload response, never a fabricated worker success.
                        let _=self.respond(&request,Err(RpcError::new("resource_exhausted","server concurrency limit reached"))).await;
                        continue;
                    };
                    let running=self.clone();
                    tasks.spawn(async move{let _permit=permit;running.handle(request).await;});
                }
            }
        }
        tasks.abort_all();
        while tasks.join_next().await.is_some() {}
        self.claims
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clear();
    }

    async fn handle(self, request: pb::Request) {
        // Sample the monotonic anchor first so handling overhead cannot extend
        // its local deadline. Preserve the absolute wire bound separately.
        let received_at = Instant::now();
        let remaining =
            Duration::from_nanos(request.expiry.saturating_sub(unix_ns()).max(0) as u64);
        if remaining > self.limits.max_request_duration {
            let _ = self
                .respond(
                    &request,
                    Err(RpcError::new(
                        "resource_exhausted",
                        "request expiry exceeds server limit",
                    )),
                )
                .await;
            return;
        }
        if remaining.is_zero() {
            return;
        }
        let deadline = received_at + remaining;
        let body = async {
            (self.validate)(&request.raw_request)?;
            if self.spec.require_claim {
                let affinity = (self.affinity)(&request.raw_request);
                if affinity < 0.0 {
                    return Ok(None);
                }
                let handling =
                    self.spec.queue && request.skip_claim && self.limits.allow_skip_claim;
                let (tx, rx) = oneshot::channel();
                let _guard = if !handling {
                    let mut claims = self.claims.lock().unwrap_or_else(|e| e.into_inner());
                    if claims.contains_key(&request.request_id) {
                        return Ok(None);
                    }
                    claims.insert(request.request_id.clone(), tx);
                    Some(ClaimGuard {
                        claims: self.claims.clone(),
                        id: request.request_id.clone(),
                    })
                } else {
                    None
                };
                self.bus
                    .publish(
                        &channels::claim_request(&self.spec.service, &request.client_id),
                        &pb::ClaimRequest {
                            request_id: request.request_id.clone(),
                            server_id: self.server_id(),
                            affinity,
                            handling,
                        },
                    )
                    .await?;
                if !handling {
                    let grant = rx.await.map_err(|_| {
                        RpcError::new("canceled", "server is closed")
                            .with_origin(RpcErrorOrigin::Closed)
                    })?;
                    if grant.server_id != self.server_id() {
                        return Ok(None);
                    }
                }
            }
            let ctx = RequestContext {
                remote_client: request.client_id.clone(),
                sent_at_ns: request.sent_at,
                expiry_ns: request.expiry,
                deadline,
                metadata: request.metadata.clone().into_iter().collect(),
            };
            #[cfg(test)]
            self.handler_started
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            (self.handler)(ctx, request.raw_request.clone())
                .await
                .map(Some)
        };
        let result = tokio::select! {
            biased;
            _=self.cancel.cancelled()=>return,
            _=tokio::time::sleep_until(deadline.into())=>return,
            result=body=>result,
        };
        match result {
            Ok(None) => {}
            Ok(Some(bytes)) => {
                let _ = self.respond(&request, Ok(bytes)).await;
            }
            Err(error) => {
                let _ = self.respond(&request, Err(error)).await;
            }
        }
    }

    fn server_id(&self) -> String {
        self.server_id.clone()
    }
    async fn respond(
        &self,
        request: &pb::Request,
        result: Result<Vec<u8>, RpcError>,
    ) -> Result<(), TransportError> {
        let mut response = pb::Response {
            request_id: request.request_id.clone(),
            server_id: self.server_id(),
            sent_at: unix_ns(),
            ..Default::default()
        };
        let details = match result {
            Ok(bytes) => {
                response.raw_response = bytes;
                Vec::new()
            }
            Err(error) => {
                let (error, details) = match error.error_detail_wires() {
                    Ok(details) => (error, details),
                    Err(invalid) => (invalid, Vec::new()),
                };
                response.error = error.message;
                response.code = error.code;
                details
            }
        };
        let body = response_provenance::encode_response_body(&response, &details);
        let channel = channels::response(&self.spec.service, &request.client_id);
        let encoded = wire::encode_raw::<pb::Response>(
            &body,
            &channel.local,
            self.bus.limits().compression,
            self.bus.limits().wire,
        )
        .map_err(|_| TransportError::Codec)?;
        self.bus
            .publish_encoded(&channel, &encoded)
            .await
            .map(|_| ())
    }
}
fn received_response_for_bus(bus: &RpcBus, bytes: &[u8]) -> Result<ReceivedResponse, RpcError> {
    if !bus.is_local() {
        return received_response(bytes, bus.limits().wire);
    }
    let raw = wire::decode_raw::<pb::Response>(bytes, bus.limits().wire)
        .map_err(|_| RpcError::malformed_response("invalid response envelope"))?;
    let root = lk_protocol::wire::filter_known_psrpc_root("internal.Response", &raw.payload)
        .map_err(|_| RpcError::malformed_response("invalid response envelope"))?;
    response_provenance::decode_response_body(&root)
}
fn received_response(bytes: &[u8], limits: wire::WireLimits) -> Result<ReceivedResponse, RpcError> {
    let raw = wire::decode_raw::<pb::Response>(bytes, limits)
        .map_err(|_| RpcError::malformed_response("invalid response envelope"))?;
    response_provenance::decode_response_body(&raw.payload)
}

/// Capture the wall-clock bound for a locally accepted monotonic deadline.
pub(crate) fn capture_expiry_ns(deadline: Instant) -> i64 {
    let now = Instant::now();
    wire_expiry_ns(now, unix_ns(), deadline, None)
}

fn call_expiry_ns(
    now: Instant,
    wall_now: i64,
    timeout_deadline: Instant,
    inherited_deadline: Option<Instant>,
    inherited_expiry: Option<i64>,
) -> i64 {
    // The paired original bound wins when the inherited context deadline is
    // selected. Re-minting even a *smaller* value here lets a later retry extend
    // its first child's wire expiry by clock-conversion jitter.
    if inherited_deadline.is_some_and(|deadline| deadline <= timeout_deadline)
        && let Some(expiry) = inherited_expiry
    {
        return expiry;
    }
    let effective =
        inherited_deadline.map_or(timeout_deadline, |deadline| deadline.min(timeout_deadline));
    wire_expiry_ns(now, wall_now, effective, inherited_expiry)
}

fn wire_expiry_ns(now: Instant, wall_now: i64, deadline: Instant, inherited: Option<i64>) -> i64 {
    let remaining = deadline.saturating_duration_since(now);
    let computed = wall_now.saturating_add(remaining.as_nanos().min(i64::MAX as u128) as i64);
    inherited.map_or(computed, |original| original.min(computed))
}

fn unix_ns() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos()
        .min(i64::MAX as u128) as i64
}

#[cfg(test)]
mod tests;

#[cfg(test)]
mod provenance_tests;

#[cfg(test)]
impl RpcClient {
    /// Ordered Redis round trip on this precise request producer connection.
    pub(crate) async fn test_command_barrier(&self) -> Result<(), RpcError> {
        self.bus.test_command_barrier().await.map_err(Into::into)
    }
}

#[cfg(test)]
#[path = "local_phase_tests.rs"]
mod local_phase_tests;
