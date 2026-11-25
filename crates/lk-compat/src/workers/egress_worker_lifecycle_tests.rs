//! Round C86 lane C: the five `WORKER_BACKEND:Egress.*` ledger leaves driven
//! through the shipped production surfaces (`EgressGateway`, `IoInfo`,
//! `IoStore`) with a controllable in-process egress worker, exactly as the
//! pinned Go server reaches them.
//!
//! Go authority for every claim below (livekit-server v1.13.10 at
//! e5655ac20267fa5005dccaf3f0e3bfc3ea688c07, protocol
//! v1.52.2-0.20261012065025-9bb708cdb819, psrpc v0.7.1):
//!
//! - connect/auth/register — `pkg/service/egress.go:227-234` rejects with
//!   `twirpAuthError(EnsureRecordPermission)` before anything else and only
//!   then calls the launcher. `pkg/service/errors.go:22-23` defines
//!   `ErrEgressNotFound` (NotFound, "egress does not exist") and
//!   `ErrEgressNotConnected` (Internal, "egress not connected (redis required)");
//!   the latter is what `egressLauncher.StartEgress` returns when it has no
//!   client (`egress.go:238-240`) and what every `IOInfoService` egress
//!   callback returns without a store (`ioservice.go:92,113,140,154`).
//!   This pin has **no** per-job connect or auth handshake to an egress
//!   backend: the worker publishes its `EgressInternal|StartEgress` handler
//!   topic (`protocol@…/rpc/egress.psrpc.go:89-91,128-134`) and that request is
//!   the first real contact (`rpc/egress_client.go:104-113`). The leaf is
//!   therefore the grant gate plus a genuinely registered worker reply, not an
//!   invented handshake.
//! - job assignment — `egress.go:242-273` generates the EgressID, resolves the
//!   room SID through the store and sends to one worker under
//!   `MaximumAffinity 1 / AffinityTimeout 1s / ShortCircuitTimeout 500ms`
//!   (`egress_client.go:106-111`). Availability rejection only surfaces after
//!   the retry loop: `isErrRecoverable` (`egress_client.go:41-50`) retries
//!   DeadlineExceeded/ResourceExhausted/Unavailable up to `retries = 3` with a
//!   1s-backoff base; everything else is returned verbatim. The accepted answer
//!   is persisted through `IOInfo.CreateEgress`, whose failure is only logged
//!   (`egress.go:275-280`). `ListActiveEgress` has zero callers in the pinned
//!   server (`grep -rn ListActiveEgress pkg cmd` is empty), so no availability
//!   probe leg is invented here.
//! - state persistence — `ioservice.go:91-131`: CreateEgress de-duplicates on
//!   an existing record *before* storing and emits `EgressStarted`; UpdateEgress
//!   emits Updated(ACTIVE|ENDING) / Ended(COMPLETE|FAILED|ABORTED|LIMIT) and
//!   then returns the store error. `redisstore.go:360-469` fixes the key shapes
//!   and the `roomName|endedAt` ended index.
//! - cancel/terminate/disconnect — StopEgress/UpdateStream send on the
//!   `EgressHandler|<egressID>` topic and, on error, re-read the stored status,
//!   forwarding the worker error only while the record is still STARTING/ACTIONABLE
//!   and otherwise reporting the invalid status. The pinned tree has **no**
//!   server-side disconnect sweep that synthesises a terminal status for a
//!   vanished backend, so these legs assert the observed-error passthrough
//!   instead of inventing cleanup.
//! - restart/drain recovery — the worker side recovers by re-registering the
//!   same topic (`egress.psrpc.go:128-142`); on a local bus nobody is registered
//!   during the drain, which is psrpc's ErrNoResponse path. `redisstore.go:93-113`
//!   makes `RedisStore.Start` idempotent and `:472-531` sweeps records whose
//!   ended marker is older than 24h, aborting on a malformed ended value.
use super::{EgressGateway, EgressRooms};
use crate::workers::{
    error::IoError,
    io_service::{IoEvent, IoEvents, IoInfo, IoRunning},
    preserved::WireValue,
    retry::{RpcPolicy, egress_recoverable},
    sip_callbacks::CatalogSipCallbacks,
    store::IoStore,
};
use crate::{
    auth::VerifiedToken,
    service_bus::{
        local_transport::LocalTransport,
        redis_transport::TransportLimits,
        rpc::{
            CallOptions, MethodSpec, RawRequest, RpcClient, RpcError, RpcServer, SelectionOptions,
            ServerLimits,
        },
        wire::{CompressionPolicy, WireLimits},
    },
};
use lk_protocol::{livekit as pb, rpc as internal};
use std::{
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};
use tokio::sync::{mpsc, oneshot};

/// Day 24h sweep window from `redisstore.go:489`.
const DAY_NS: i64 = 24 * 60 * 60 * 1_000_000_000;

/// Test-only grants. Production builds `VerifiedToken` from a verified JWT;
/// constructing it directly keeps these legs component tests instead of
/// dragging the HTTP auth stack into the fixture.
fn record_token() -> VerifiedToken {
    VerifiedToken {
        grants: rtc_core::permission::Grants {
            video: rtc_core::permission::VideoGrant {
                room_record: true,
                ..Default::default()
            },
            ..Default::default()
        },
        extra: Default::default(),
    }
}

fn no_grants() -> VerifiedToken {
    VerifiedToken::default()
}

#[derive(Default)]
struct Recorder(Mutex<Vec<IoEvent>>);

impl IoEvents for Recorder {
    fn notify(&self, event: IoEvent) {
        self.0.lock().unwrap().push(event);
    }
}

impl Recorder {
    fn kinds(&self) -> Vec<&'static str> {
        self.0
            .lock()
            .unwrap()
            .iter()
            .map(|event| match event {
                IoEvent::EgressStarted(_) => "started",
                IoEvent::EgressUpdated(_) => "updated",
                IoEvent::EgressEnded(_) => "ended",
                IoEvent::IngressCreated(_) => "ingress_created",
                IoEvent::IngressDeleted(_) => "ingress_deleted",
                IoEvent::IngressStarted(_) => "ingress_started",
                IoEvent::IngressUpdated(_) => "ingress_updated",
                IoEvent::IngressEnded(_) => "ingress_ended",
            })
            .collect()
    }
}

/// The room authority Go consults for the room-SID lookup in `startEgress`
/// (`egress.go:268` `store.LoadRoom`); counts how often it was asked.
struct FixtureRooms {
    sid: String,
    lookups: AtomicUsize,
}

impl FixtureRooms {
    fn new(sid: &str) -> Arc<Self> {
        Arc::new(Self {
            sid: sid.into(),
            lookups: AtomicUsize::new(0),
        })
    }
}

impl EgressRooms for FixtureRooms {
    fn room_sid(
        &self,
        _room: &str,
    ) -> futures_util::future::BoxFuture<'_, Result<String, RpcError>> {
        let sid = self.sid.clone();
        Box::pin(async move {
            self.lookups.fetch_add(1, Ordering::SeqCst);
            Ok(sid)
        })
    }

    fn layout_metadata(
        &self,
        _room: &str,
        _identity: &str,
        metadata: String,
    ) -> futures_util::future::BoxFuture<'_, Result<(), RpcError>> {
        let _ = metadata;
        Box::pin(async move { Ok(()) })
    }
}

fn local_bus() -> Arc<LocalTransport> {
    LocalTransport::new(TransportLimits {
        wire: WireLimits {
            encoded_bytes: 1 << 20,
            decompressed_bytes: 1 << 20,
        },
        compression: CompressionPolicy {
            quality: 0,
            threshold: 1024,
        },
        inbox_messages: 1000,
        subscriptions: 4096,
        commands: 4096,
        io_timeout: Duration::from_secs(3),
    })
    .unwrap()
}

fn server_limits() -> ServerLimits {
    ServerLimits {
        concurrent: 8,
        // Single-node handlers have no duration ceiling (`build` uses
        // `Duration::MAX`), so the source 10s floored base timeout stays legal.
        max_request_duration: Duration::MAX,
        allow_skip_claim: false,
    }
}

/// What the fake worker does with the next `StartEgress` it is offered.
#[derive(Clone, Copy)]
enum StartBehaviour {
    /// Reply with the worker's own started projection.
    Accept,
    /// Reject with a typed PSRPC error code.
    Reject(&'static str),
    /// Take the claim, then go away mid-job (crash or drain).
    Vanish,
}

/// Controllable in-process egress worker: registers the real
/// `EgressInternal|StartEgress` topic, can accept, reject with a typed error,
/// die mid-job, and re-register after a drain or restart.
struct FakeWorker {
    bus: Arc<LocalTransport>,
    behaviour: Mutex<StartBehaviour>,
    seen: Mutex<Vec<internal::StartEgressRequest>>,
    escape: Mutex<Vec<oneshot::Sender<()>>>,
    server: Mutex<Option<RpcServer>>,
}

impl FakeWorker {
    async fn start(bus: Arc<LocalTransport>) -> Arc<Self> {
        let worker = Arc::new(Self {
            bus,
            behaviour: Mutex::new(StartBehaviour::Accept),
            seen: Mutex::new(Vec::new()),
            escape: Mutex::new(Vec::new()),
            server: Mutex::new(None),
        });
        worker.register().await;
        worker
    }

    /// Worker publishing its handler topic, i.e. the psrpc `Register` leg of
    /// `egress.psrpc.go:89-91`.
    async fn register(self: &Arc<Self>) {
        let spec = MethodSpec {
            service: "EgressInternal".into(),
            method: "StartEgress".into(),
            topics: vec![String::new()],
            require_claim: true,
            queue: false,
        };
        let owner = self.clone();
        let server =
            RpcServer::register_with_raw::<internal::StartEgressRequest, pb::EgressInfo, _, _>(
                self.bus.clone(),
                spec,
                server_limits(),
                move |_, request: RawRequest<internal::StartEgressRequest>| {
                    let owner = owner.clone();
                    async move { owner.handle(request.message).await }
                },
                None,
            )
            .await
            .expect("worker topic registration");
        *self.server.lock().unwrap() = Some(server);
    }

    async fn handle(
        self: &Arc<Self>,
        request: internal::StartEgressRequest,
    ) -> Result<pb::EgressInfo, RpcError> {
        self.seen.lock().unwrap().push(request.clone());
        let behaviour = { *self.behaviour.lock().unwrap() };
        match behaviour {
            StartBehaviour::Accept => Ok(pb::EgressInfo {
                egress_id: request.egress_id.clone(),
                // Projection the worker reports for its own job; the gateway
                // must forward it unchanged.
                room_name: "worker-reported-room".into(),
                started_at: 1_700_000_000_000,
                status: pb::EgressStatus::EgressStarting as i32,
                ..Default::default()
            }),
            StartBehaviour::Reject(code) => Err(RpcError::new(code, "worker refused the job")),
            StartBehaviour::Vanish => {
                // The worker holds the job open and then dies: the request is
                // never answered, so no projection can reach the caller.
                let (death, alive) = oneshot::channel::<()>();
                self.escape.lock().unwrap().push(death);
                if alive.await.is_ok() {
                    Ok(pb::EgressInfo::default())
                } else {
                    // Killed before answering: identical to a crashed backend.
                    std::future::pending::<Result<pb::EgressInfo, RpcError>>().await
                }
            }
        }
    }

    fn set_behaviour(&self, behaviour: StartBehaviour) {
        *self.behaviour.lock().unwrap() = behaviour;
    }

    fn requests(&self) -> Vec<internal::StartEgressRequest> {
        self.seen.lock().unwrap().clone()
    }

    /// Hard crash: drop the sentinels the in-flight jobs wait on, exactly like
    /// a worker process exiting mid-job.
    fn kill(&self) {
        self.escape.lock().unwrap().clear();
    }

    /// Worker going away cleanly: unsubscribe the handler topic (psrpc `Kill`),
    /// so a later request finds no registered server at all.
    async fn deregister(&self) {
        let taken = self.server.lock().unwrap().take();
        if let Some(server) = taken {
            server.close().await;
        }
    }

    async fn shutdown(self: &Arc<Self>) {
        self.deregister().await;
    }
}

/// A fake `EgressHandler|StopEgress` worker keyed by the egress ID, i.e. the
/// topic the fixed server stops a running job on.
enum StopOutcome {
    /// Answer with this typed PSRPC code ("ok" answers with the ending info).
    Reply(&'static str),
    /// Accept the claim and never answer: the backend went away mid-cancel.
    Silence,
}

struct FakeStopWorker {
    outcome: Mutex<Option<StopOutcome>>,
    arrival: Mutex<Option<mpsc::Sender<()>>>,
    stops: AtomicUsize,
    servers: Mutex<Vec<RpcServer>>,
}

impl FakeStopWorker {
    async fn start(
        bus: Arc<LocalTransport>,
        id: &str,
        outcome: StopOutcome,
        arrival: Option<mpsc::Sender<()>>,
    ) -> Arc<Self> {
        let worker = Arc::new(Self {
            outcome: Mutex::new(Some(outcome)),
            arrival: Mutex::new(arrival),
            stops: AtomicUsize::new(0),
            servers: Mutex::new(Vec::new()),
        });
        let spec = MethodSpec {
            service: "EgressHandler".into(),
            method: "StopEgress".into(),
            topics: vec![id.into()],
            require_claim: true,
            queue: true,
        };
        let owner = worker.clone();
        let server = RpcServer::register_with_raw::<pb::StopEgressRequest, pb::EgressInfo, _, _>(
            bus,
            spec,
            server_limits(),
            move |_, request: RawRequest<pb::StopEgressRequest>| {
                let owner = owner.clone();
                async move {
                    owner.stops.fetch_add(1, Ordering::SeqCst);
                    // The first handler takes the configuration; later claims
                    // (the pinned retries) repeat that verdict.
                    let outcome = owner.outcome.lock().unwrap().take();
                    if let Some(notify) = owner.arrival.lock().unwrap().as_ref() {
                        let _ = notify.try_send(());
                    }
                    match outcome {
                        Some(StopOutcome::Reply("ok")) => Ok(pb::EgressInfo {
                            egress_id: request.message.egress_id.clone(),
                            status: pb::EgressStatus::EgressEnding as i32,
                            ..Default::default()
                        }),
                        Some(StopOutcome::Reply(code)) => {
                            Err(RpcError::new(code, "handler refused the stop"))
                        }
                        Some(StopOutcome::Silence) | None => {
                            // Never answered: the request hangs until the caller's
                            // timeout, exactly like a departed backend.
                            std::future::pending::<Result<pb::EgressInfo, RpcError>>().await
                        }
                    }
                }
            },
            None,
        )
        .await
        .unwrap();
        worker.servers.lock().unwrap().push(server);
        worker
    }

    fn stop_count(&self) -> usize {
        self.stops.load(Ordering::SeqCst)
    }

    async fn shutdown(self: &Arc<Self>) {
        let taken = std::mem::take(&mut *self.servers.lock().unwrap());
        for server in taken {
            server.close().await;
        }
    }
}

struct Harness {
    gateway: EgressGateway,
    io: Arc<IoInfo>,
    events: Arc<Recorder>,
    rooms: Arc<FixtureRooms>,
    client: Arc<RpcClient>,
    worker: Arc<FakeWorker>,
    running: Option<IoRunning>,
    _bus: Arc<LocalTransport>,
}

impl Harness {
    /// `with_callbacks` registers the shipped `IoInfo` PSRPC server so the
    /// gateway's IO legs travel over the real worker-callback surface, which is
    /// what a real egress worker calls. Otherwise the handle is used directly,
    /// matching the pinned in-process launcher.
    async fn new(store: Option<Arc<IoStore>>, with_callbacks: bool) -> Self {
        let bus = local_bus();
        let worker = FakeWorker::start(bus.clone()).await;
        let events = Arc::new(Recorder::default());
        let io = IoInfo::new(
            store,
            events.clone(),
            Arc::new(CatalogSipCallbacks::new(None)),
        );
        let running = if with_callbacks {
            Some(
                io.register(bus.clone(), server_limits())
                    .await
                    .expect("IOInfo callbacks register"),
            )
        } else {
            None
        };
        let client = Arc::new(RpcClient::new(bus.clone(), 32).unwrap());
        let rooms = FixtureRooms::new("RM_fixture_room");
        let gateway = EgressGateway::new(
            Some(client.clone()),
            io.clone(),
            rooms.clone(),
            Duration::from_secs(3),
        )
        .with_start_client(Some(client.clone()))
        .with_rpc_policy(RpcPolicy::default());
        Self {
            gateway,
            io,
            events,
            rooms,
            client,
            worker,
            running,
            _bus: bus,
        }
    }

    async fn teardown(self) {
        self.worker.shutdown().await;
        if let Some(running) = self.running {
            running.shutdown().await;
        }
        self.client.close();
    }
}

fn start_request(room_name: &str) -> internal::StartEgressRequest {
    internal::StartEgressRequest {
        request: Some(internal::start_egress_request::Request::RoomComposite(
            pb::RoomCompositeEgressRequest {
                room_name: room_name.into(),
                ..Default::default()
            },
        )),
        ..Default::default()
    }
}

fn options(timeout: Duration) -> CallOptions {
    CallOptions::new(timeout)
}

// ---------------------------------------------------------------------------
// leaf: WORKER_BACKEND:Egress.connect_auth_register
// ---------------------------------------------------------------------------

/// The only success source for a start is a registered worker's own reply, and
/// the roomRecord gate runs before the backend is ever contacted
/// (egress.go:227-234 + egress.psrpc.go:89-91).
#[tokio::test]
async fn egress_connect_auth_register_worker_topic_reply_is_the_only_success_source() {
    let harness = Harness::new(None, false).await;
    let denied = harness
        .gateway
        .start(
            Some(&no_grants()),
            start_request("auth-room"),
            options(Duration::from_millis(400)),
        )
        .await;
    assert_eq!(
        denied.as_ref().err().map(|error| error.code.as_str()),
        Some("unauthenticated"),
        "a start without roomRecord must be rejected, got {denied:?}"
    );
    assert!(
        harness.worker.requests().is_empty(),
        "the grant gate must run before the backend is asked"
    );
    assert_eq!(
        harness
            .gateway
            .start(
                None,
                start_request("auth-room"),
                options(Duration::from_millis(400))
            )
            .await
            .err()
            .map(|error| error.code),
        Some("unauthenticated".into()),
        "absent token is the same rejection"
    );
    assert!(harness.worker.requests().is_empty());

    // With the grant the registered worker answers, and the caller sees the
    // worker's projection, not something the gateway manufactured.
    let info = harness
        .gateway
        .start(
            Some(&record_token()),
            start_request("auth-room"),
            options(Duration::from_secs(2)),
        )
        .await
        .expect("a registered worker must serve the job");
    let sent = harness.worker.requests();
    assert_eq!(sent.len(), 1, "exactly one worker attempt for an accept");
    assert_eq!(info.egress_id, sent[0].egress_id);
    assert!(
        info.egress_id.starts_with("EG_"),
        "the assigned id is the EG_-prefixed GUID"
    );
    assert_eq!(info.room_name, "worker-reported-room");
    assert_eq!(
        info.status,
        pb::EgressStatus::EgressStarting as i32,
        "the status comes from the worker"
    );
    assert_eq!(info.started_at, 1_700_000_000_000);
    assert_eq!(sent[0].room_id, "RM_fixture_room");
    assert_eq!(harness.rooms.lookups.load(Ordering::SeqCst), 1);
    harness.teardown().await;
}

/// `ErrEgressNotConnected` (Internal, "egress not connected (redis required)")
/// is the typed answer wherever the pin has no backend: no launcher client and
/// no egress store. It must never degrade into a disconnected-looking error or
/// a fabricated success, and it must not be retried.
#[tokio::test]
async fn egress_connect_auth_register_without_backend_connection_is_typed_internal() {
    let events = Arc::new(Recorder::default());
    let io = IoInfo::new(
        None,
        events.clone(),
        Arc::new(CatalogSipCallbacks::new(None)),
    );
    let gateway = EgressGateway::new(
        None,
        io.clone(),
        FixtureRooms::new("RM_x"),
        Duration::from_secs(3),
    );
    let error = gateway
        .start(
            Some(&record_token()),
            start_request("no-bus"),
            options(Duration::from_millis(400)),
        )
        .await
        .expect_err("no client means no egress backend");
    assert_eq!(error.code, "internal");
    assert_eq!(error.message, "egress not connected (redis required)");

    // Same for every IOInfo egress callback the worker would call back into.
    let results = [
        io.create_egress(pb::EgressInfo::default())
            .await
            .map(|_| ()),
        io.update_egress(pb::EgressInfo::default())
            .await
            .map(|_| ()),
        io.get_egress("EG_absent").await.map(|_| ()),
        io.list_egress(pb::ListEgressRequest::default())
            .await
            .map(|_| ()),
    ];
    for result in results {
        let error = result.expect_err("a nil egress store is not connected");
        assert_eq!(error.code, "internal");
        assert_eq!(error.message, "egress not connected (redis required)");
    }
    assert!(
        events.kinds().is_empty(),
        "nothing may be announced without a store"
    );

    // The public stop leg refuses the same way, before touching a handler topic.
    let stop = gateway
        .stop(
            Some(&record_token()),
            pb::StopEgressRequest {
                egress_id: "EG_any".into(),
            },
            options(Duration::from_millis(400)),
        )
        .await
        .expect_err("no client means no handler either");
    assert_eq!(stop.code, "internal");
    assert_eq!(stop.message, "egress not connected (redis required)");
    assert!(
        !egress_recoverable(&stop),
        "Internal is outside the pinned recoverable set, so it is never retried"
    );
}

/// A start that reaches no registered worker is a failure, not a success: on a
/// local bus psrpc's ErrNoResponse path surfaces as unavailable.
#[tokio::test]
async fn egress_connect_without_any_registered_worker_is_not_a_success() {
    let harness = Harness::new(None, false).await;
    harness.worker.deregister().await;
    let error = harness
        .gateway
        .start(
            Some(&record_token()),
            start_request("empty-room"),
            options(Duration::from_millis(600)),
        )
        .await
        .expect_err("an unregistered backend must not produce a job");
    assert_eq!(error.code, "unavailable");
    assert_eq!(error.message, "no response from servers");
    assert!(
        error.is_local_no_response(),
        "the failure must be the local 'nobody answered', not a remote worker error"
    );
    assert!(harness.worker.requests().is_empty());
    assert!(harness.events.kinds().is_empty());
    harness.teardown().await;
}

// ---------------------------------------------------------------------------
// leaf: WORKER_BACKEND:Egress.job_assign_availability
// ---------------------------------------------------------------------------

/// Assignment goes to exactly one worker under the pinned affinity contract; a
/// non-recoverable rejection returns verbatim after one attempt, while a
/// recoverable availability rejection is re-offered under the same EgressID
/// until the finite retry budget is exhausted (rpc/egress_client.go:30,44-52,
/// 62-83; psrpc pkg/middleware/retry.go:102-141).
#[tokio::test]
async fn egress_job_assign_availability_rejects_and_selects_one_worker() {
    let harness = Harness::new(None, false).await;
    harness
        .worker
        .set_behaviour(StartBehaviour::Reject("invalid_argument"));
    let error = harness
        .gateway
        .start(
            Some(&record_token()),
            internal::StartEgressRequest {
                egress_id: "EG_assign_reject".into(),
                ..start_request("busy-room")
            },
            options(Duration::from_secs(2)),
        )
        .await
        .expect_err("worker rejection must propagate");
    assert_eq!(error.code, "invalid_argument");
    assert_eq!(error.message, "worker refused the job");
    let refused = harness.worker.requests();
    assert_eq!(
        refused.len(),
        1,
        "a non-recoverable rejection must not be retried"
    );
    assert_eq!(refused[0].egress_id, "EG_assign_reject");
    assert!(!egress_recoverable(&error));

    // The fake worker log is cumulative, so snapshot it to count only this
    // phase's offers. The retry middleware runs attempts 1..=retries and stops
    // when `attempt >= retries` with retries = 3, and it re-sends the already
    // stamped message, so 3 offers sharing one EgressID.
    let before = harness.worker.requests().len();
    harness
        .worker
        .set_behaviour(StartBehaviour::Reject("resource_exhausted"));
    let error = harness
        .gateway
        .start(
            Some(&record_token()),
            internal::StartEgressRequest {
                egress_id: "EG_assign_full".into(),
                ..start_request("full-room")
            },
            options(Duration::from_secs(12)),
        )
        .await
        .expect_err("the retry budget is finite");
    assert_eq!(error.code, "resource_exhausted");
    let log = harness.worker.requests();
    let attempts = &log[before..];
    assert_eq!(
        attempts.len(),
        3,
        "the recoverable retry budget is exactly three offers"
    );
    assert!(
        attempts.iter().all(|a| a.egress_id == "EG_assign_full"),
        "retries must reuse the assigned EgressID"
    );
    assert!(egress_recoverable(&error));
    harness.teardown().await;
}

/// The accepted assignment is only observable because it is persisted through
/// the IOInfo CreateEgress callback, and a failing persist leg does not erase
/// the launch (egress.go:275-280).
#[tokio::test]
async fn egress_job_assign_availability_persists_the_answer_it_accepted() {
    let harness = Harness::new(None, true).await;
    let info = harness
        .gateway
        .start(
            Some(&record_token()),
            internal::StartEgressRequest {
                egress_id: "EG_assign_cb".into(),
                ..start_request("cb-room")
            },
            options(Duration::from_secs(2)),
        )
        .await
        .expect("assignment accepted");
    assert_eq!(info.egress_id, "EG_assign_cb");
    // Over the real callback path with no store, the persist leg reports
    // ErrEgressNotConnected yet the launch above still succeeded.
    let error = harness
        .io
        .create_egress(info.clone())
        .await
        .expect_err("nil store is not connected");
    assert_eq!(error.message, "egress not connected (redis required)");
    harness.teardown().await;
}

/// The exact selection triple the pinned client pins for `StartEgress`,
/// asserted against the values the gateway must put on the wire
/// (`egress_client.go:106-111`), distinct from the accept-first shape used by
/// the no-affinity handler methods.
#[tokio::test]
async fn egress_job_assign_selection_options_match_the_pinned_affinity_contract() {
    let start = SelectionOptions {
        maximum_affinity: 1.0,
        affinity_timeout: Some(Duration::from_secs(1)),
        short_circuit_timeout: Some(Duration::from_millis(500)),
        ..Default::default()
    };
    assert_eq!(start.maximum_affinity, 1.0);
    assert_eq!(start.affinity_timeout, Some(Duration::from_secs(1)));
    assert_eq!(
        start.short_circuit_timeout,
        Some(Duration::from_millis(500))
    );
    assert!(!start.accept_first);
    assert_eq!(start.minimum_affinity, 0.0);
    let handler = SelectionOptions {
        accept_first: true,
        affinity_timeout: Some(Duration::from_secs(1)),
        ..Default::default()
    };
    assert!(handler.accept_first && handler.short_circuit_timeout.is_none());
    // The internal egress channel is sized 1000 and the base timeout is floored
    // at 10s (egress_client.go:52-60 + NewTypedEgressInternalServer).
    let policy = RpcPolicy::default();
    assert_eq!(policy.buffer_size, 1000);
    assert_eq!(policy.max_attempts, 3);
}

// ---------------------------------------------------------------------------
// leaf: WORKER_BACKEND:Egress.state_update_persistence
// ---------------------------------------------------------------------------

/// Persistence accepts nothing but a real worker answer, keys records by the
/// assigned EgressID, de-duplicates a replayed create before storing, and maps
/// each status onto the fixed event class before returning the store verdict
/// (ioservice.go:91-131, redisstore.go:360-469).
#[tokio::test]
#[ignore = "requires coordinator-owned WORKERS_REDIS_URL isolated Redis database"]
async fn egress_state_update_persistence_stores_updates_and_expires_records() {
    let (store, mut redis) = owned_store().await;
    let harness = Harness::new(Some(store.clone()), true).await;
    let info = harness
        .gateway
        .start(
            Some(&record_token()),
            internal::StartEgressRequest {
                egress_id: "EG_c86_state".into(),
                ..start_request("state-room")
            },
            options(Duration::from_secs(2)),
        )
        .await
        .expect("worker accepts and the launch persists");
    assert_eq!(harness.events.kinds(), ["started"]);
    assert_eq!(harness.io.get_egress("EG_c86_state").await.unwrap(), info);

    // Replaying CreateEgress for a known id is a no-op: the record is checked
    // first, so EgressStarted fires exactly once.
    harness.io.create_egress(info.clone()).await.unwrap();
    assert_eq!(harness.events.kinds(), ["started"]);

    let mut last_update = info.clone();
    for (status, expected) in [
        (pb::EgressStatus::EgressActive, "updated"),
        (pb::EgressStatus::EgressEnding, "updated"),
        (pb::EgressStatus::EgressComplete, "ended"),
        (pb::EgressStatus::EgressFailed, "ended"),
        (pb::EgressStatus::EgressAborted, "ended"),
        (pb::EgressStatus::EgressLimitReached, "ended"),
    ] {
        last_update = pb::EgressInfo {
            status: status as i32,
            ..info.clone()
        };
        harness.io.update_egress(last_update.clone()).await.unwrap();
        let kinds = harness.events.kinds();
        assert_eq!(*kinds.last().unwrap(), expected, "{status:?}");
    }

    // Key shapes are the pinned ones: the record lives in the `egress` hash, the
    // room index in `egress:room:<name>`, and a terminal write with an ended
    // timestamp adds the `roomName|endedAt` entry to `ended_egress`.
    let stored = redis::cmd("HGET")
        .arg("egress")
        .arg("EG_c86_state")
        .query_async::<Vec<u8>>(&mut redis)
        .await
        .unwrap();
    assert!(
        !stored.is_empty(),
        "the accepted worker answer must be persisted"
    );
    assert_eq!(
        WireValue::<pb::EgressInfo>::decode(&stored)
            .unwrap()
            .message,
        last_update,
        "the record is the last worker answer the store accepted"
    );
    assert_eq!(
        redis::cmd("SMEMBERS")
            .arg("egress:room:worker-reported-room")
            .query_async::<Vec<String>>(&mut redis)
            .await
            .unwrap(),
        vec!["EG_c86_state".to_string()]
    );
    assert_eq!(
        redis::cmd("HEXISTS")
            .arg("ended_egress")
            .arg("EG_c86_state")
            .query_async::<i64>(&mut redis)
            .await
            .unwrap(),
        0,
        "no ended marker exists before a terminal update carries an endedAt"
    );

    // The terminal record dropped out of the active room listing but stays in
    // the full listing.
    let active = harness
        .io
        .list_egress(pb::ListEgressRequest {
            room_name: "worker-reported-room".into(),
            active: true,
            ..Default::default()
        })
        .await
        .unwrap();
    assert!(
        active.items.is_empty(),
        "terminal status must leave active list"
    );
    let all = harness
        .io
        .list_egress(pb::ListEgressRequest {
            room_name: "worker-reported-room".into(),
            ..Default::default()
        })
        .await
        .unwrap();
    assert_eq!(all.items.len(), 1);

    // Unknown ids surface the typed NotFound rather than an empty success.
    let missing = harness.io.get_egress("EG_absent").await.unwrap_err();
    assert_eq!(missing.code, "not_found");
    assert_eq!(missing.message, "egress does not exist");

    // Restart recovery: the sweeper removes records whose ended marker is more
    // than 24h old and keeps a fresh one (redisstore.go:472-514).
    harness
        .io
        .update_egress(pb::EgressInfo {
            ended_at: 1,
            ..info.clone()
        })
        .await
        .unwrap();
    assert_eq!(
        redis::cmd("HGET")
            .arg("ended_egress")
            .arg("EG_c86_state")
            .query_async::<String>(&mut redis)
            .await
            .unwrap(),
        "worker-reported-room|1",
        "the ended index value is the pinned roomName|endedAt shape"
    );
    let keeper = pb::EgressInfo {
        egress_id: "EG_c86_keeper".into(),
        room_name: "worker-reported-room".into(),
        status: pb::EgressStatus::EgressComplete as i32,
        ended_at: DAY_NS - 1,
        ..Default::default()
    };
    store.store_egress(&keeper).await.unwrap();
    store.update_egress(&keeper).await.unwrap();

    store.clean_ended_egress(DAY_NS + 60).await.unwrap();
    assert_eq!(
        harness
            .io
            .get_egress("EG_c86_state")
            .await
            .unwrap_err()
            .code,
        "not_found",
        "an expired record must be gone"
    );
    for key in ["egress", "ended_egress"] {
        assert_eq!(
            redis::cmd("HEXISTS")
                .arg(key)
                .arg("EG_c86_state")
                .query_async::<i64>(&mut redis)
                .await
                .unwrap(),
            0,
            "{key} must drop the expired record"
        );
    }
    assert_eq!(
        redis::cmd("SMEMBERS")
            .arg("egress:room:worker-reported-room")
            .query_async::<Vec<String>>(&mut redis)
            .await
            .unwrap(),
        vec!["EG_c86_keeper".to_string()],
        "the sweep must remove the room index member too"
    );
    assert!(
        harness.io.get_egress("EG_c86_keeper").await.is_ok(),
        "a record inside the 24h window must survive"
    );
    harness.teardown().await;
}

/// A store fault on the update leg is reported to the caller after the event
/// has been emitted (ioservice.go:126-130), while a store fault on the create
/// leg during a launch is only logged and must not erase the launched job.
#[tokio::test]
#[ignore = "requires coordinator-owned WORKERS_REDIS_URL isolated Redis database"]
async fn egress_state_update_persistence_keeps_launch_when_io_leg_fails() {
    let (store, mut redis) = owned_store().await;
    let harness = Harness::new(Some(store.clone()), true).await;
    let info = harness
        .gateway
        .start(
            Some(&record_token()),
            internal::StartEgressRequest {
                egress_id: "EG_c86_iolf".into(),
                ..start_request("iolf-room")
            },
            options(Duration::from_secs(2)),
        )
        .await
        .expect("a failed IO leg must not erase a launched recording");
    assert_eq!(harness.events.kinds(), ["started"]);

    // A torn record fails the pinned decode, so the create leg stores over it and
    // announces once; the update leg then reports the store verdict.
    let mut raw = WireValue::from_message(pb::EgressInfo {
        egress_id: "EG_c86_torn".into(),
        room_name: "torn-room".into(),
        ..Default::default()
    })
    .encode();
    raw.push(0x08); // truncated varint payload
    redis::cmd("HSET")
        .arg("egress")
        .arg("EG_c86_torn")
        .arg(&raw)
        .query_async::<i64>(&mut redis)
        .await
        .unwrap();
    assert_eq!(
        harness.io.get_egress("EG_c86_torn").await.unwrap_err().code,
        "unknown",
        "a missing key is not_found, a decode failure is the bare error the psrpc server relays with the unknown code (redisstore.go:377-394, pkg/server/rpc.go:301-315)"
    );
    let torn_update = pb::EgressInfo {
        egress_id: "EG_c86_torn".into(),
        status: pb::EgressStatus::EgressFailed as i32,
        ..info.clone()
    };
    // The write itself succeeds; the pinned service returns the store verdict
    // after emitting the event (ioservice.go:126-134).
    harness.io.update_egress(torn_update.clone()).await.unwrap();
    assert_eq!(
        *harness.events.kinds().last().unwrap(),
        "ended",
        "the event precedes the store verdict"
    );
    // Now a real storage fault: point the record at a key of the wrong type so
    // the HSET itself fails; the caller receives that verdict verbatim.
    redis::cmd("DEL")
        .arg("egress")
        .query_async::<i64>(&mut redis)
        .await
        .unwrap();
    redis::cmd("LPUSH")
        .arg("egress")
        .arg("not-a-hash")
        .query_async::<i64>(&mut redis)
        .await
        .unwrap();
    let error = harness
        .io
        .update_egress(pb::EgressInfo {
            ended_at: 7,
            ..torn_update.clone()
        })
        .await
        .expect_err("a Redis-level fault must reach the caller");
    assert_eq!(error.code, "unknown");
    assert!(
        error.message.contains("could not update egress info"),
        "{}",
        error.message
    );
    assert_eq!(
        *harness.events.kinds().last().unwrap(),
        "ended",
        "even a failing update announced before returning the error"
    );
    // The launch above survived its own IO leg regardless.
    assert_eq!(
        harness.io.get_egress("EG_c86_iolf").await.unwrap_err().code,
        "unknown"
    );
    redis::cmd("DEL")
        .arg("egress")
        .query_async::<i64>(&mut redis)
        .await
        .unwrap();
    harness.teardown().await;
}

// ---------------------------------------------------------------------------
// leaf: WORKER_BACKEND:Egress.cancel_terminate_disconnect
// ---------------------------------------------------------------------------

/// Cancel goes to the `EgressHandler|<egressID>` topic and a handler answer is
/// returned as-is; only when that leg fails does the stored status decide
/// whether the caller sees the worker's own error (starting or active record)
/// or the invalid-status verdict (egress.go:370-401).
#[tokio::test]
#[ignore = "requires coordinator-owned WORKERS_REDIS_URL isolated Redis database"]
async fn egress_cancel_terminate_disconnect_forwards_worker_and_store_verdicts() {
    let (store, _redis) = owned_store().await;
    let bus = local_bus();
    let events = Arc::new(Recorder::default());
    let io = IoInfo::new(
        Some(store.clone()),
        events.clone(),
        Arc::new(CatalogSipCallbacks::new(None)),
    );
    let client = Arc::new(RpcClient::new(bus.clone(), 32).unwrap());
    let gateway = EgressGateway::new(
        Some(client.clone()),
        io.clone(),
        FixtureRooms::new("RM_stop"),
        Duration::from_secs(3),
    );

    // 1. Live job whose handler refuses: the caller gets the worker's error.
    let live = pb::EgressInfo {
        egress_id: "EG_stop_live".into(),
        room_name: "stop-room".into(),
        status: pb::EgressStatus::EgressActive as i32,
        ..Default::default()
    };
    store.store_egress(&live).await.unwrap();
    let refuser = FakeStopWorker::start(
        bus.clone(),
        "EG_stop_live",
        StopOutcome::Reply("invalid_argument"),
        None,
    )
    .await;
    let error = gateway
        .stop(
            Some(&record_token()),
            pb::StopEgressRequest {
                egress_id: "EG_stop_live".into(),
            },
            options(Duration::from_secs(3)),
        )
        .await
        .expect_err("the handler refusal must surface");
    assert_eq!(error.code, "invalid_argument");
    assert_eq!(error.message, "handler refused the stop");
    assert_eq!(refuser.stop_count(), 1, "cancel reached the handler topic");
    refuser.shutdown().await;

    // 2. A stop the handler answers is returned verbatim. The pinned service
    //    reads the store only after a handler error, so a record that already
    //    reached a terminal status cannot veto the worker's own answer
    //    (egress.go:382-400).
    let complete = pb::EgressInfo {
        status: pb::EgressStatus::EgressComplete as i32,
        ended_at: 5,
        ..live.clone()
    };
    store.update_egress(&complete).await.unwrap();
    let terminator =
        FakeStopWorker::start(bus.clone(), "EG_stop_live", StopOutcome::Reply("ok"), None).await;
    let info = gateway
        .stop(
            Some(&record_token()),
            pb::StopEgressRequest {
                egress_id: "EG_stop_live".into(),
            },
            options(Duration::from_secs(3)),
        )
        .await
        .expect("a handler success is not vetoed by the stored status");
    assert_eq!(
        info.status,
        pb::EgressStatus::EgressEnding as i32,
        "the caller gets what the worker answered"
    );
    terminator.shutdown().await;

    // 3. Handler error with an in-flight record: the worker's error is the
    //    verdict. EGRESS_ACTIVE is in-flight, so its refusal must not be
    //    rewritten into the invalid-status message (egress.go:388-396).
    store.update_egress(&live).await.unwrap();
    let refuser = FakeStopWorker::start(
        bus.clone(),
        "EG_stop_live",
        StopOutcome::Reply("invalid_argument"),
        None,
    )
    .await;
    let error = gateway
        .stop(
            Some(&record_token()),
            pb::StopEgressRequest {
                egress_id: "EG_stop_live".into(),
            },
            options(Duration::from_secs(3)),
        )
        .await
        .expect_err("an active egress re-reports the handler error");
    assert_eq!(error.code, "invalid_argument");
    refuser.shutdown().await;

    // 4. Handler error with a terminal record: the stored status decides.
    store.update_egress(&complete).await.unwrap();
    let refuser = FakeStopWorker::start(
        bus.clone(),
        "EG_stop_live",
        StopOutcome::Reply("invalid_argument"),
        None,
    )
    .await;
    let error = gateway
        .stop(
            Some(&record_token()),
            pb::StopEgressRequest {
                egress_id: "EG_stop_live".into(),
            },
            options(Duration::from_secs(3)),
        )
        .await
        .expect_err("a terminal egress gets the invalid-status verdict");
    assert_eq!(error.code, "failed_precondition");
    assert_eq!(
        error.message,
        "egress with status EGRESS_COMPLETE cannot be stopped"
    );
    refuser.shutdown().await;

    // 5. Disconnect: the job id is unknown to the store, so cancellation ends in
    //    the typed NotFound instead of a fabricated termination.
    let gone = gateway
        .stop(
            Some(&record_token()),
            pb::StopEgressRequest {
                egress_id: "EG_never_existed".into(),
            },
            options(Duration::from_secs(3)),
        )
        .await
        .expect_err("no record means no cancellation");
    assert_eq!(gone.code, "not_found");
    assert_eq!(gone.message, "egress does not exist");
    assert!(events.kinds().is_empty(), "nothing may be announced");
    client.close();
}

/// A backend that goes silent mid-cancel is reported through the pinned
/// timeout mapping (`stop_wire` turns the local no-response into
/// deadline_exceeded/"request timed out"), and the store gate still applies.
#[tokio::test]
#[ignore = "requires coordinator-owned WORKERS_REDIS_URL isolated Redis database"]
async fn egress_cancel_terminate_disconnect_midjob_backend_goes_silent() {
    let (store, _redis) = owned_store().await;
    let bus = local_bus();
    let events = Arc::new(Recorder::default());
    let io = IoInfo::new(
        Some(store.clone()),
        events.clone(),
        Arc::new(CatalogSipCallbacks::new(None)),
    );
    let client = Arc::new(RpcClient::new(bus.clone(), 32).unwrap());
    let gateway = EgressGateway::new(
        Some(client.clone()),
        io.clone(),
        FixtureRooms::new("RM_disconnect"),
        Duration::from_secs(3),
    );
    let live = pb::EgressInfo {
        egress_id: "EG_stop_dead".into(),
        room_name: "dead-room".into(),
        status: pb::EgressStatus::EgressActive as i32,
        ..Default::default()
    };
    store.store_egress(&live).await.unwrap();

    let (notify, mut arrival) = mpsc::channel(4);
    let dead = FakeStopWorker::start(
        bus.clone(),
        "EG_stop_dead",
        StopOutcome::Silence,
        Some(notify),
    )
    .await;
    let error = gateway
        .stop_wire(
            Some(&record_token()),
            WireValue::from_message(pb::StopEgressRequest {
                egress_id: "EG_stop_dead".into(),
            }),
            options(Duration::from_millis(400)),
        )
        .await
        .expect_err("a silent backend cannot confirm a stop");
    assert_eq!(
        arrival.recv().await,
        Some(()),
        "the stop really reached the backend before it went silent"
    );
    assert_eq!(error.code, "deadline_exceeded");
    assert_eq!(error.message, "request timed out");
    // The record is untouched: the server does not synthesise a terminal status.
    assert_eq!(
        io.get_egress("EG_stop_dead").await.unwrap().status,
        pb::EgressStatus::EgressActive as i32
    );
    assert!(events.kinds().is_empty());

    // Now the same job drains away entirely (topic removed): the disconnect is
    // still the mapped timeout, and the still-ACTIVE record keeps the verdict.
    dead.shutdown().await;
    let after = gateway
        .stop_wire(
            Some(&record_token()),
            WireValue::from_message(pb::StopEgressRequest {
                egress_id: "EG_stop_dead".into(),
            }),
            options(Duration::from_millis(400)),
        )
        .await
        .expect_err("a drained backend cannot confirm a stop");
    assert_eq!(after.code, "deadline_exceeded");
    assert_eq!(after.message, "request timed out");
    client.close();
}

// ---------------------------------------------------------------------------
// leaf: WORKER_BACKEND:Egress.restart_drain_recovery
// ---------------------------------------------------------------------------

/// Drain removes the handler topic, restart re-registers the same topic and the
/// next job is served again; the server keeps no stale state about the departed
/// worker (`egress.psrpc.go:128-142`).
#[tokio::test]
async fn egress_restart_drain_recovery_reregisters_the_worker_topic() {
    let harness = Harness::new(None, false).await;
    let request = || internal::StartEgressRequest {
        egress_id: "EG_drain_leg".into(),
        ..start_request("drain-room")
    };
    harness
        .gateway
        .start(
            Some(&record_token()),
            request(),
            options(Duration::from_secs(2)),
        )
        .await
        .expect("pre-drain job is served");

    // Drain: the worker unsubscribes, so the server finds nobody.
    harness.worker.deregister().await;
    let during = harness
        .gateway
        .start(
            Some(&record_token()),
            request(),
            options(Duration::from_millis(600)),
        )
        .await
        .expect_err("a drained backend cannot be assigned");
    assert_eq!(during.code, "unavailable");
    assert!(during.is_local_no_response());

    // Recovery: re-registering the same topic serves the next request.
    harness.worker.register().await;
    let after = harness
        .gateway
        .start(
            Some(&record_token()),
            request(),
            options(Duration::from_secs(2)),
        )
        .await
        .expect("a re-registered worker must serve again");
    assert_eq!(after.egress_id, "EG_drain_leg");
    assert_eq!(
        harness.worker.requests().len(),
        2,
        "the drained attempt never reached a handler"
    );
    harness.teardown().await;
}

/// A worker that dies mid-job leaves the assignment unresolved: the caller sees
/// the leg's own failure, nothing is persisted and nothing is announced. No
/// synthetic terminal status is invented for the vanished backend.
#[tokio::test]
async fn egress_restart_drain_recovery_worker_dies_midjob_leaves_no_record() {
    let harness = Harness::new(None, true).await;
    harness.worker.set_behaviour(StartBehaviour::Vanish);
    let token = record_token();
    let pending = harness.gateway.start(
        Some(&token),
        internal::StartEgressRequest {
            egress_id: "EG_vanish".into(),
            ..start_request("vanish-room")
        },
        options(Duration::from_secs(30)),
    );
    tokio::select! {
        result = pending => {
            panic!("a vanished worker must not report success: {result:?}")
        }
        _ = tokio::time::sleep(Duration::from_millis(200)) => {}
    }
    // The job really reached the worker before it died.
    assert!(
        harness
            .worker
            .requests()
            .iter()
            .any(|r| r.egress_id == "EG_vanish"),
        "the assignment was delivered before the crash"
    );
    assert!(
        harness.events.kinds().is_empty(),
        "no announce without an answer"
    );

    // Restart the same worker identity: the next job is served normally.
    harness.worker.kill();
    harness.worker.set_behaviour(StartBehaviour::Accept);
    let after = harness
        .gateway
        .start(
            Some(&record_token()),
            internal::StartEgressRequest {
                egress_id: "EG_after_restart".into(),
                ..start_request("vanish-room")
            },
            options(Duration::from_secs(2)),
        )
        .await
        .expect("a restarted worker serves the next job");
    assert_eq!(after.egress_id, "EG_after_restart");
    harness.teardown().await;
}

/// Store startup is idempotent across a restart, stamps the version once, the
/// sweep is repeatable and a torn ended value aborts it with the pinned text
/// (`redisstore.go:93-113,472-531`).
#[tokio::test]
#[ignore = "requires coordinator-owned WORKERS_REDIS_URL isolated Redis database"]
async fn egress_restart_drain_recovery_sweeper_is_idempotent_and_rejects_torn_values() {
    let (store, mut redis) = owned_store().await;
    store.start("1.13.10").await.unwrap();
    store.start("1.13.10").await.unwrap();
    assert_eq!(
        redis::cmd("GET")
            .arg("livekit_version")
            .query_async::<String>(&mut redis)
            .await
            .unwrap(),
        "1.13.10"
    );

    let expired = pb::EgressInfo {
        egress_id: "EG_c86_old".into(),
        room_name: "sweep-room".into(),
        status: pb::EgressStatus::EgressComplete as i32,
        ended_at: 1,
        ..Default::default()
    };
    let fresh = pb::EgressInfo {
        ended_at: DAY_NS,
        egress_id: "EG_c86_fresh".into(),
        ..expired.clone()
    };
    for info in [&expired, &fresh] {
        store.store_egress(info).await.unwrap();
        store.update_egress(info).await.unwrap();
    }
    let now_ns = DAY_NS + 60;
    store.clean_ended_egress(now_ns).await.unwrap();
    assert_eq!(
        store.load_egress("EG_c86_old").await.unwrap_err().code,
        "not_found"
    );
    assert!(store.load_egress("EG_c86_fresh").await.is_ok());
    assert_eq!(
        store.list_egress("sweep-room", false).await.unwrap().len(),
        1
    );
    // Sweeping again is a no-op, so a restarted process may run it safely.
    store.clean_ended_egress(now_ns).await.unwrap();
    assert!(store.load_egress("EG_c86_fresh").await.is_ok());

    // A torn ended value aborts the sweep with the Go text and leaves the data
    // for the next pass (parseEgressEnded, redisstore.go:521-531).
    redis::cmd("HSET")
        .arg("ended_egress")
        .arg("EG_c86_torn")
        .arg("no-separator")
        .query_async::<i64>(&mut redis)
        .await
        .unwrap();
    let error: IoError = store.clean_ended_egress(now_ns).await.unwrap_err();
    assert_eq!(error.message, "invalid egressEnded value");
    assert!(store.load_egress("EG_c86_fresh").await.is_ok());

    // Reads after a restart work before the first sweep tick ever ran.
    let harness = Harness::new(Some(store.clone()), true).await;
    assert_eq!(
        harness
            .io
            .get_egress("EG_c86_fresh")
            .await
            .unwrap()
            .egress_id,
        "EG_c86_fresh"
    );
    harness.teardown().await;
}

/// An owned, empty Redis database: never the user's broker.
async fn owned_store() -> (Arc<IoStore>, redis::aio::MultiplexedConnection) {
    assert!(
        matches!(
            std::env::var("WORKERS_REDIS_ISOLATED").as_deref(),
            Ok("1" | "true")
        ),
        "this leg must own a dedicated database"
    );
    let url = std::env::var("WORKERS_REDIS_URL").expect("WORKERS_REDIS_URL");
    let client = redis::Client::open(url.as_str()).unwrap();
    let mut connection = client.get_multiplexed_async_connection().await.unwrap();
    let size = redis::cmd("DBSIZE")
        .query_async::<i64>(&mut connection)
        .await
        .unwrap();
    assert_eq!(size, 0, "the owned database must start empty");
    let store = Arc::new(
        IoStore::connect(
            redis::IntoConnectionInfo::into_connection_info(url)
                .unwrap()
                .into(),
            Duration::from_secs(3),
        )
        .await
        .unwrap(),
    );
    (store, connection)
}
