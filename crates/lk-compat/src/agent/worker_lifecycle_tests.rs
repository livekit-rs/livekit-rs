//! C86 lane B: the 13 `AGENT_WIRE` ledger leaves driven by an in-process fake
//! worker that talks the **real `/agent` WebSocket transport** — actual TCP
//! loopback, a real HTTP/1.1 upgrade with a real signed access token, real
//! masked client frames and the production protobuf/protojson codec switching.
//! No raw codec call and no hand-built writer channel appears anywhere below:
//! every message crosses the same `websocket::session` dispatch loop the launched
//! binary serves, and every job effect runs through the production object graph
//! (`CompatServices::new(..).assemble(..)` → `RoomManager` → `AgentService` →
//! `bridge::Bridge`).
//!
//! It lives inside the crate (wired from `agent/mod.rs`) rather than under
//! `tests/` because the seams it drives — [`super::wire_tests`],
//! `Registry::oracle_snapshot`, `Registry::running_job_ids_for_test`,
//! `Registry::get_job_state_for_test`, `Registry::affinity` — are all
//! `#[cfg(test)]` or `pub(super)` items, and an integration target links the
//! *non-test* library build, so it could never reach them.
//!
//! Ledger coverage (one test name per leaf id):
//! * `WorkerMessage.register` / `ServerMessage.register` →
//!   [`worker_message_register_answers_server_message_register_over_real_websocket`]
//! * `WorkerMessage.availability` / `ServerMessage.availability` →
//!   [`worker_message_availability_resolves_the_offered_job_and_declines_an_unknown_one`]
//! * `ServerMessage.assignment` →
//!   [`server_message_assignment_is_written_only_after_the_job_is_in_running_jobs`]
//! * `WorkerMessage.update_worker` →
//!   [`worker_message_update_worker_flows_through_the_production_dispatch_loop`]
//! * `WorkerMessage.update_job` →
//!   [`worker_message_update_job_drives_the_production_room_job_state_machine`]
//! * `WorkerMessage.ping` / `ServerMessage.pong` →
//!   [`worker_message_ping_is_answered_by_server_message_pong_with_a_real_round_trip`]
//! * `ServerMessage.termination` →
//!   [`server_message_termination_precedes_the_job_state_flip_over_real_websocket`]
//! * `ServerMessage.go_away` →
//!   [`server_message_go_away_has_no_pinned_go_producer_so_rust_invents_none`]
//! * `WorkerMessage.simulate_job` →
//!   [`worker_message_simulate_job_assigns_through_the_same_production_worker`]
//! * `WorkerMessage.migrate_job` →
//!   [`worker_message_migrate_job_stays_the_pinned_oss_no_op`]
//!
//! Go ground truth, re-read from the pinned oracle
//! (`livekit-server@v1.13.10`), all line numbers in `pkg/agent/worker.go` unless
//! another file is named:
//! * `pkg/service/agentws.go:49-58` — upgrade gate first (404 for a non-upgrade
//!   request), then the `video.agent` grant (401 `ErrPermissionDenied`), and only
//!   then `Upgrade`; `protocol` comes from the query form value (:70).
//! * :52 `CurrentProtocol = 1`; :55 `RegisterTimeout = 10s`; :56
//!   `AssignJobTimeout = 10s`.
//! * :172-177 `MakeWorkerRegistration` allocates the worker id server-side.
//! * :242-284 `HandleRegister` rejects an unknown job type (:243) and answers
//!   `RegisterWorkerResponse{WorkerID, ServerInfo}` after building it (:271-280).
//! * :390-490 `AssignJob`: availability written at :415, `runningJobs[jobID]`
//!   inserted at :470-474 **before** the assignment write at :475, deleted again
//!   at :476-480 when that write fails; the state stamped before the offer carries
//!   `WorkerId`/`AgentId`/`UpdatedAt`/`StartedAt`/`JS_RUNNING` (:405-413).
//! * :495-520 `TerminateJob`: the termination write (:504) precedes the status
//!   update (:517); `reason != AGENT_LEFT_ROOM` maps to `JS_SUCCESS` (:510-515).
//! * :552-566 `HandleAvailability` warns and returns nil for an unknown jobId.
//! * :568-608 `HandleUpdateJob`/`UpdateJobStatus`: unknown job → psrpc
//!   `NotFound` (:589-592) treated as a debug log (:568-578), `started_at` only on
//!   the PENDING→non-PENDING edge (:595-597), ended statuses delete the running
//!   entry (:600-604).
//! * :638-655 `HandleUpdateWorker`: `seq != 0 && seq <= statusSeq` returns before
//!   *all* field writes (:642-647).
//! * :610-636 `HandleSimulateJob`: job type from the participant presence only
//!   (:611-614), `guid.New(AgentJobPrefix)` (:616), namespace/agentName from the
//!   registration (:622-623), then `AssignJob` in a goroutine (:626-633).
//! * :663-669 `HandleMigrateJob`: OSS no-op returning nil.
//! * :130-143 `WorkerPingHandler.HandlePing` echoes `last_timestamp` and stamps
//!   the server clock.
//!
//! Tier honesty: these legs attest `component_behavior`. They do **not** attest
//! `main_server_consumer` (the launched `livecloud` binary was not started here)
//! nor `official_differential_lifecycle` (the official Python/Node agent SDK was
//! not used; the worker is this repo's own in-process fake speaking the wire
//! protocol).

use super::codec::WireCodec;
use super::registry;
use super::registry::{Registration, WorkerError};
use super::service::AgentService;
use super::websocket::{WorkerEvent, WorkerHttpState};
use super::wire_tests::{self as fixture, Frame, RawWs, WsEvent};
use crate::auth::TokenClaims;
use lk_protocol::livekit as pb;
use prost::Message as _;
use rtc_core::{
    config::ServerConfig,
    ids::{Identity, ProjectId, RoomKey},
    model::ParticipantKind,
    permission::{Grants, VideoGrant},
    session::JoinParams,
};
use rtc_runtime::{
    api::{AgentDispatchOptions, AgentDispatchRecord, CreateRoomOptions},
    net::{LocalInterface, NetRouter},
};
use std::{net::Ipv4Addr, sync::Arc, time::Duration};
use tokio::sync::mpsc;

const KEY_ID: &str = "c86b-agent-key";
const KEY_SECRET: &str = "c86b-agent-fixture-secret-0123456789abcdef";
const AGENT_NAME: &str = "c86b-agent";

/// Wall-clock guard for socket IO. These legs run on a paused virtual clock,
/// where `tokio::time::timeout` fires as soon as the runtime idles and would
/// report a spurious `Elapsed` before the peer's bytes land. Polling and yielding
/// keeps a task ready so the virtual clock stays put while a missing frame still
/// fails the assertion.
const IO_BOUND: Duration = Duration::from_secs(10);

fn unix_ms() -> i64 {
    rtc_core::util::unix_ms()
}

fn base_config() -> ServerConfig {
    let args = ["livecloud", "--dev", "--node-ip", "127.0.0.1"]
        .into_iter()
        .map(Into::into)
        .collect();
    struct LoopbackProbe;
    impl lk_config::netdetect::NetProbe for LoopbackProbe {
        fn local_addresses(&self) -> Vec<std::net::IpAddr> {
            vec![std::net::Ipv4Addr::LOCALHOST.into()]
        }
        fn external_ip(&self, _: &[String], _: bool) -> Result<std::net::IpAddr, String> {
            panic!("local test configuration must not request external discovery");
        }
        fn sleep(&self, _: Duration) {
            panic!("local test configuration must not retry network discovery");
        }
    }
    let info = lk_config::ProgramInfo {
        binary_name: "livecloud".into(),
        product_version: "test".into(),
        compat_version: "1.13.10",
    };
    match lk_config::parse_with_probe(args, &lk_config::Env::default(), &info, &LoopbackProbe)
        .unwrap()
    {
        lk_config::EntryAction::RunServer(loaded) => loaded.server,
        action => panic!("expected normalized server configuration, got {action:?}"),
    }
}

/// The production `/agent` control-plane graph: the assembled `AppState` (real
/// `RoomManager`, real `AgentService` with its bridge consumer task) plus the axum
/// route the binary serves.
struct Graph {
    keys: Arc<crate::auth::KeyStore>,
    tokens: Arc<crate::auth::TokenMinter>,
    manager: rtc_runtime::RoomManager,
    service: Arc<AgentService>,
    _net: NetRouter,
}

impl Graph {
    async fn new() -> Self {
        let mut config = base_config();
        // The fixture key table replaces `--dev`'s generated one, so minted worker
        // and assignment tokens verify against a secret this lane knows.
        config.keys = vec![(KEY_ID.into(), KEY_SECRET.into())];
        let local = Ipv4Addr::LOCALHOST.into();
        let net = NetRouter::start(rtc_runtime::net::NetConfig {
            udp_enabled: false,
            udp_port: None,
            port_range: (0, 0),
            tcp_port: None,
            interfaces: vec![LocalInterface {
                bind: local,
                advertise: local,
            }],
        })
        .await
        .unwrap();
        // Exactly the graph the launched binary runs: CompatServices → assemble →
        // RoomManager → AgentService → bridge::Bridge consumer.
        let state = crate::test_support::state_with_keys(config.keys.clone())
            .await
            .0;
        let keys = state.keys.clone();
        let tokens = Arc::new(crate::auth::TokenMinter::new(keys.clone()));
        let manager = state.manager().clone();
        let service = state.agents().clone();
        Self {
            keys,
            tokens,
            manager,
            service,
            _net: net,
        }
    }

    /// The `/agent` route exactly as `AgentService::routes` builds it, served by
    /// axum over loopback TCP. Port 0 lets the OS pick one so concurrent lanes
    /// never collide.
    async fn spawn_agent_route(&self) -> (std::net::SocketAddr, tokio::task::JoinHandle<()>) {
        let router = self.service.clone().routes();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let task = tokio::spawn(async move {
            axum::serve(listener, router).await.unwrap();
        });
        (addr, task)
    }

    /// The same route with the session's event sender swapped for a test-owned
    /// one, so a leg can read what the dispatch loop actually pushed instead of
    /// only what the consumer did with it. Only [`bridge::Bridge`] handlers that
    /// ignore the event may be needed by such a leg: `Registered` and `Ping` are
    /// consumer no-ops (`bridge.rs:321-322`), while `JobState` and `Disconnected`
    /// drive room state and therefore stay on the production sink above.
    /// The registry, keys, tokens and `ServerInfo` are the production ones.
    async fn spawn_route_with_own_sink(
        &self,
    ) -> (
        std::net::SocketAddr,
        tokio::task::JoinHandle<()>,
        mpsc::Receiver<WorkerEvent>,
    ) {
        let (sink, receiver) = mpsc::channel(64);
        let state = WorkerHttpState {
            events: sink,
            ..self.service.worker_state()
        };
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let task = tokio::spawn(async move {
            axum::serve(listener, super::websocket::routes(state))
                .await
                .unwrap();
        });
        (addr, task, receiver)
    }

    /// A worker token carrying `video.agent`, which is what `agentws.go:54-58`
    /// requires before it will even attempt the upgrade.
    fn worker_token(&self) -> String {
        self.tokens
            .mint(
                KEY_ID,
                &TokenClaims {
                    video: Some(VideoGrant {
                        agent: true,
                        ..Default::default()
                    }),
                    ..Default::default()
                },
                Duration::from_secs(60),
                unix_ms() / 1000,
            )
            .unwrap()
    }

    fn create_room_opts() -> CreateRoomOptions {
        CreateRoomOptions::default()
    }

    /// Join one ordinary publisher. Go's room-side dispatch launches a job per
    /// existing participant, and keeping a real media participant connected stops
    /// the empty-room teardown from racing the job's own terminal state.
    async fn join_publisher(&self, room: &str, identity: &str) {
        let key = RoomKey::new(ProjectId::local(), room);
        self.manager
            .join(JoinParams {
                room: key,
                grants: Grants {
                    api_key: KEY_ID.into(),
                    identity: identity.into(),
                    kind: ParticipantKind::Standard,
                    video: VideoGrant {
                        room_join: true,
                        room: room.into(),
                        ..Default::default()
                    },
                    ..Default::default()
                },
                identity: Identity(identity.into()),
                name: String::new(),
                metadata: String::new(),
                attributes: Default::default(),
                capabilities: Default::default(),
                client: Default::default(),
                auto_subscribe: true,
                auto_subscribe_data_track: true,
                adaptive_stream: false,
                single_pc: false,
                disable_ice_lite: false,
                subscriber_allow_pause: None,
                reconnect: false,
                reconnect_reason: 0,
                participant_sid: None,
                add_tracks: Vec::new(),
                publisher_offer: None,
            })
            .await
            .unwrap();
    }

    /// Create a dispatch and drive its offer to acceptance on `worker`, returning
    /// the offered job id. This is the production producer of
    /// `ServerMessage.availability`/`assignment` (`worker.go:415`, `:475`).
    async fn accept_dispatch(
        &self,
        worker: &mut FakeWorker,
        key: &RoomKey,
        dispatch_id: &'static str,
        agent_identity: &str,
    ) -> String {
        self.manager
            .create_agent_dispatch(
                key,
                AgentDispatchOptions {
                    id: dispatch_id.into(),
                    agent_name: AGENT_NAME.into(),
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        let offer = worker.next_data_frame().await;
        let job_id = match offer.one() {
            pb::server_message::Message::Availability(request) => {
                request.job.as_ref().expect("offered job").id.clone()
            }
            other => panic!(
                "a dispatch begins with ServerMessage.availability (worker.go:415), got {:?}",
                other.clone()
            ),
        };
        worker
            .peer
            .send_proto(&message(pb::worker_message::Message::Availability(
                pb::AvailabilityResponse {
                    job_id: job_id.clone(),
                    available: true,
                    participant_identity: agent_identity.into(),
                    ..Default::default()
                },
            )))
            .await;
        worker.expect_assignment(dispatch_id).await;
        poll_until(Duration::from_secs(5), "job in runningJobs", || {
            self.service
                .registry
                .running_job_ids_for_test(&worker.worker_id)
                .contains(&job_id)
        })
        .await;
        job_id
    }
}

/// Yield until the worker session pushed `count` events matching `keep`, returning
/// everything observed so far. Each pong is acknowledged by the real writer
/// before its event is queued (`send_pong` awaits `send_message`), so a reached
/// count proves the matching frames were written. Matching is needed because a
/// session pushes other events (registration) before the ones a leg cares about.
async fn collect_events<F>(
    events: &mut mpsc::Receiver<WorkerEvent>,
    count: usize,
    keep: F,
) -> Vec<WorkerEvent>
where
    F: Fn(&WorkerEvent) -> bool,
{
    let start = std::time::Instant::now();
    let mut seen = Vec::new();
    while seen.iter().filter(|e| keep(e)).count() < count {
        assert!(
            start.elapsed() < IO_BOUND,
            "only {} of {count} consumer events arrived within {IO_BOUND:?}",
            seen.iter().filter(|e| keep(e)).count()
        );
        match events.try_recv() {
            Ok(event) => seen.push(event),
            Err(mpsc::error::TryRecvError::Disconnected) => {
                panic!("the worker event consumer channel closed early")
            }
            Err(mpsc::error::TryRecvError::Empty) => tokio::task::yield_now().await,
        }
    }
    seen
}

/// The in-process fake worker: a real WebSocket peer plus the register handshake.
struct FakeWorker {
    peer: RawWs,
    worker_id: String,
}

impl FakeWorker {
    async fn register(graph: &Graph, addr: std::net::SocketAddr, json: &[u8]) -> Self {
        let mut peer = RawWs::connect(addr, &graph.worker_token()).await;
        peer.send_json(json).await;
        let reply = peer.receive().await;
        let response = match reply.one() {
            pb::server_message::Message::Register(response) => response.clone(),
            other => panic!(
                "register must be answered by ServerMessage.register, got {:?}",
                other.clone()
            ),
        };
        Self {
            peer,
            worker_id: response.worker_id,
        }
    }
    async fn register_room_agent(graph: &Graph, addr: std::net::SocketAddr) -> Self {
        Self::register(
            graph,
            addr,
            format!(r#"{{"register":{{"type":"JT_ROOM","agentName":"{AGENT_NAME}"}}}}"#).as_bytes(),
        )
        .await
    }
    /// Read the next server frame, skipping the ws-level Ping/Pong keepalive the
    /// production writer emits every 10s (`wsprotocol.go:37,219-235`) so a control
    /// frame is never mistaken for a keepalive. A conformant client answers Ping.
    async fn next_data_frame(&mut self) -> Frame {
        loop {
            match self.peer.receive_data_or_ping().await {
                WsEvent::Data(frame) => return *frame,
                WsEvent::KeepalivePing(_) => continue,
            }
        }
    }
    /// Expect the assignment arm for `dispatch_id` (Go's `JobAssignment` carries
    /// the job plus the minted token, `worker.go:475`).
    async fn expect_assignment(&mut self, dispatch_id: &str) {
        let frame = self.next_data_frame().await;
        let assignment = match frame.one() {
            pb::server_message::Message::Assignment(assignment) => assignment.clone(),
            other => panic!(
                "{dispatch_id}: accepted availability must produce ServerMessage.assignment \
                 (worker.go:475), got {:?}",
                other.clone()
            ),
        };
        assert_eq!(
            assignment.job.as_ref().expect("assigned job").dispatch_id,
            dispatch_id,
            "the assignment names the dispatch that produced it"
        );
    }
    fn availability(job_id: &str, available: bool) -> pb::WorkerMessage {
        message(pb::worker_message::Message::Availability(
            pb::AvailabilityResponse {
                job_id: job_id.into(),
                available,
                ..Default::default()
            },
        ))
    }
}

fn message(m: pb::worker_message::Message) -> pb::WorkerMessage {
    pb::WorkerMessage { message: Some(m) }
}

fn update_worker(seq: u64, status: i32, load: f32, draining: bool) -> pb::WorkerMessage {
    message(pb::worker_message::Message::UpdateWorker(
        pb::UpdateWorkerStatus {
            status: Some(status),
            seq,
            load,
            draining,
            ..Default::default()
        },
    ))
}

fn update_job(job_id: &str, status: i32, error: &str) -> pb::WorkerMessage {
    message(pb::worker_message::Message::UpdateJob(
        pb::UpdateJobStatus {
            job_id: job_id.into(),
            status,
            error: error.into(),
        },
    ))
}

fn ping(timestamp: i64) -> pb::WorkerMessage {
    message(pb::worker_message::Message::Ping(pb::WorkerPing {
        timestamp,
    }))
}

async fn poll_until<F>(budget: Duration, what: &str, mut cond: F)
where
    F: FnMut() -> bool,
{
    let start = std::time::Instant::now();
    while !cond() {
        assert!(start.elapsed() < budget, "{what} within {budget:?}");
        tokio::task::yield_now().await;
    }
}

/// Await the room-side projection of a dispatch without letting the paused virtual
/// clock abort the underlying oneshot round trip: the poll runs on the blocking
/// pool with its own real-time runtime, like the rest of this lane's IO waits.
async fn poll_dispatches(
    manager: rtc_runtime::RoomManager,
    key: RoomKey,
    dispatch_id: &str,
    what: &str,
    budget: Duration,
) -> Vec<AgentDispatchRecord> {
    let dispatch_id = dispatch_id.to_string();
    let what = what.to_string();
    tokio::task::spawn_blocking(move || {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        runtime.block_on(async move {
            let start = std::time::Instant::now();
            loop {
                let records = manager
                    .list_agent_dispatches(&key, Some(&dispatch_id))
                    .await
                    .unwrap_or_else(|error| panic!("{dispatch_id}: list failed: {error:?}"));
                if !records.is_empty() {
                    return records;
                }
                assert!(
                    start.elapsed() < budget,
                    "{what}: dispatch {dispatch_id} never reached the room record within {budget:?}"
                );
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
    })
    .await
    .expect("blocking poll task")
}

fn registration_key(graph: &Graph, worker_id: &str) -> registry::WorkerKey {
    graph
        .service
        .registry
        .registration(worker_id)
        .expect("registered worker")
        .key
        .clone()
}

fn permissions(registration: &Registration) -> (bool, bool, bool, bool) {
    (
        registration.permissions.can_subscribe,
        registration.permissions.can_publish,
        registration.permissions.can_publish_data,
        registration.permissions.can_update_metadata,
    )
}

// ---------------------------------------------------------------------------
// AGENT_WIRE:WorkerMessage.register + AGENT_WIRE:ServerMessage.register
// ---------------------------------------------------------------------------

#[tokio::test(start_paused = true)]
async fn worker_message_register_answers_server_message_register_over_real_websocket() {
    let graph = Graph::new().await;
    let (addr, _server) = graph.spawn_agent_route().await;

    // Upgrade/auth gate (agentws.go:49-58): a plain GET is 404, an upgrade without
    // an agent grant is 401. Both happen before any frame is read.
    let plain = fixture::request_status(
        addr,
        "GET /agent?protocol=1 HTTP/1.1\r\nHost: x\r\nConnection: keep-alive\r\n\r\n",
    )
    .await;
    assert_eq!(plain, 404, "agentws.go:49-52 non-upgrade request");
    let no_grant = fixture::request_status(
        addr,
        "GET /agent?protocol=1 HTTP/1.1\r\nHost: x\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\nSec-WebSocket-Version: 13\r\n\r\n",
    )
    .await;
    assert_eq!(
        no_grant, 401,
        "agentws.go:54-58 upgrade without the video.agent grant"
    );

    // Wire contract: JSON in answers JSON out, the id is server-allocated with
    // guid.AgentWorkerPrefix and the response carries ServerInfo with protocol 1
    // (worker.go:52,172-177,242-289).
    let worker = FakeWorker::register_room_agent(&graph, addr).await;
    assert!(
        worker.worker_id.starts_with("AW_"),
        "guid.AgentWorkerPrefix, allocated server-side (worker.go:172-177)"
    );

    // Ordering claim this lane relies on most: the registry only becomes selectable
    // after the registration reply reached the actual writer (Go writes the response
    // at agenthandler.go:197 and registers at :198). A round trip proves both the
    // reply landed and the bookkeeping completed.
    let registration = graph
        .service
        .registry
        .registration(&worker.worker_id)
        .expect("registration follows the written reply");
    assert_eq!(registration.key.agent_name, AGENT_NAME);
    assert_eq!(registration.key.job_type, 0, "JT_ROOM");
    assert_eq!(
        registration.api_key, KEY_ID,
        "identity comes from the authenticated token, never from the request body"
    );
    assert_eq!(registration.protocol, 1, "`protocol` query form value");
    assert_eq!(
        permissions(&registration),
        (true, true, true, true),
        "AllowedPermissions default (worker.go:251-259)"
    );

    // The typed registration errors: an unknown job type is refused and the
    // connection is torn down with no reply (worker.go:243-245 ErrUnknownJobType).
    for (case, body) in [
        ("unknown job type", br#"{"register":{"type":999}}"#.to_vec()),
        (
            "signal before register",
            br#"{"updateWorker":{"status":"WS_AVAILABLE"}}"#.to_vec(),
        ),
    ] {
        let mut peer = RawWs::connect(addr, &graph.worker_token()).await;
        peer.send_json(&body).await;
        peer.expect_abrupt_end().await;
        assert!(
            graph
                .service
                .registry
                .registration(&format!("AW_{case}"))
                .is_none(),
            "{case}: a rejected handshake registers nothing"
        );
    }

    // Closing the socket deregisters (agenthandler.go:217 removeWorker).
    let id = worker.worker_id.clone();
    drop(worker.peer);
    poll_until(Duration::from_secs(5), "deregistration", || {
        graph.service.registry.registration(&id).is_none()
    })
    .await;
}

// ---------------------------------------------------------------------------
// AGENT_WIRE:WorkerMessage.availability + AGENT_WIRE:ServerMessage.availability
// ---------------------------------------------------------------------------

#[tokio::test(start_paused = true)]
async fn worker_message_availability_resolves_the_offered_job_and_declines_an_unknown_one() {
    let graph = Graph::new().await;
    let (addr, _server) = graph.spawn_agent_route().await;
    // `job.State.AgentId` stays empty: `RegisterWorkerRequest` has no agent_id
    // field at all (protobufs/livekit_agent.proto: type 1, version 3,
    // ping_interval 5, namespace 6, allowed_permissions 7, agent_name 8,
    // deployment 9, endpoints 10, instance_id 11, endpoint_protocol 12, with 2
    // and 4 reserved), so a worker cannot declare one. The server's own
    // `WorkerRegistration.AgentID` (worker.go:151) is assigned nowhere in the
    // pinned tree — `grep -rn AgentID pkg cmd` in v1.13.10 hits only that
    // declaration, the log read at :329 and the copy at :410 — so the stamp
    // :410 performs always writes the empty string on the OSS path.
    let mut worker = FakeWorker::register(
        &graph,
        addr,
        format!(r#"{{"register":{{"type":"JT_ROOM","agentName":"{AGENT_NAME}"}}}}"#).as_bytes(),
    )
    .await;
    let room = "c86b-availability";
    let key = RoomKey::new(ProjectId::local(), room);
    graph
        .manager
        .create_room(key.clone(), Graph::create_room_opts())
        .await
        .unwrap();

    // ServerMessage.availability is produced by AssignJob (worker.go:415), which the
    // room-side dispatch consumer drives. An empty room with a built-in dispatch is
    // the production producer.
    let dispatch = graph
        .manager
        .create_agent_dispatch(
            &key,
            AgentDispatchOptions {
                id: "AD_c86b_avail".into(),
                agent_name: AGENT_NAME.into(),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    assert!(
        dispatch.jobs.is_empty(),
        "no job before the worker answers (CommitAgentJob is the only committer)"
    );

    let offer = worker.next_data_frame().await;
    let request = match offer.one() {
        pb::server_message::Message::Availability(request) => request.clone(),
        other => panic!(
            "a dispatch begins with ServerMessage.availability (worker.go:415), got {:?}",
            other.clone()
        ),
    };
    let job = request.job.clone().expect("offered job");
    assert_eq!(job.dispatch_id, "AD_c86b_avail");
    assert!(job.id.starts_with("AJ_"), "guid.AgentJobPrefix");
    assert_eq!(job.r#type, 0, "JT_ROOM");
    assert_eq!(job.room.as_ref().unwrap().name, room);
    assert_eq!(job.agent_name, AGENT_NAME);
    assert!(!request.resuming, "a fresh dispatch is not a resume");
    // Go stamps the job state on the stored clone before writing the offer
    // (worker.go:405-413), so the worker sees JS_RUNNING already in the request.
    let stamped = job.state.as_ref().expect("stamped state");
    assert_eq!(
        stamped.status, 1,
        "JS_RUNNING at offer time (worker.go:413)"
    );
    assert_eq!(
        stamped.worker_id, worker.worker_id,
        "job.State.WorkerId = w.ID (worker.go:407)"
    );
    assert_eq!(
        stamped.agent_id, "",
        "job.State.AgentId copies a server-side field the pinned OSS server never \
         assigns (worker.go:151,410), so the offer carries no agent id"
    );
    assert!(
        stamped.started_at > 0,
        "job.State.StartedAt stamped before the offer (worker.go:411)"
    );

    // WorkerMessage.availability for a jobId the server never offered is ignored
    // with a warning and no error (worker.go:552-566). The pending offer must stay
    // open, which the subsequent correct answer proves.
    worker
        .peer
        .send_proto(&FakeWorker::availability("AJ_not_offered", true))
        .await;
    worker
        .peer
        .send_proto(&FakeWorker::availability(&job.id, false))
        .await;

    // available=false declines: ErrWorkerNotAvailable (worker.go:436) and no
    // ServerMessage.assignment ever reaches the worker. Assert the negative through
    // the room record instead of waiting out a timeout.
    let declined = poll_dispatches(
        graph.manager.clone(),
        key.clone(),
        "AD_c86b_avail",
        "declined dispatch",
        Duration::from_secs(5),
    )
    .await;
    assert!(
        declined[0].jobs.is_empty(),
        "a declined availability creates no room job record"
    );
    assert!(
        graph
            .service
            .registry
            .running_job_ids_for_test(&worker.worker_id)
            .is_empty(),
        "a decline never enters runningJobs (worker.go:470-474 sits past the :436 return)"
    );
    assert!(matches!(
        graph.service.registry.job_state(&worker.worker_id, &job.id),
        Err(WorkerError::JobNotFound)
    ));

    // Accept on a second dispatch: the same production producer offers again because
    // the worker stayed registered and available.
    graph
        .manager
        .delete_agent_dispatch(&key, "AD_c86b_avail")
        .await
        .unwrap();
    let second = graph
        .manager
        .create_agent_dispatch(
            &key,
            AgentDispatchOptions {
                id: "AD_c86b_accept".into(),
                agent_name: AGENT_NAME.into(),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    let offer2 = worker.next_data_frame().await;
    let request2 = match offer2.one() {
        pb::server_message::Message::Availability(request) => request.clone(),
        other => panic!("second availability expected, got {:?}", other.clone()),
    };
    let job2 = request2.job.expect("second offered job");
    assert_eq!(job2.dispatch_id, second.id);
    worker
        .peer
        .send_proto(&message(pb::worker_message::Message::Availability(
            pb::AvailabilityResponse {
                job_id: job2.id.clone(),
                available: true,
                participant_identity: "c86b-agent-participant".into(),
                ..Default::default()
            },
        )))
        .await;
    worker.expect_assignment("AD_c86b_accept").await;
    // Go's HandleAvailability hands the channel out and deletes the map entry once
    // it resolves (worker.go:558-565), so the answered job must not linger in the
    // availability slot either.
    poll_until(Duration::from_secs(5), "resolved pending slot", || {
        !graph
            .service
            .registry
            .pending_job_ids_for_test(&worker.worker_id)
            .contains(&job2.id)
    })
    .await;
    let accepted = poll_dispatches(
        graph.manager.clone(),
        key.clone(),
        "AD_c86b_accept",
        "accepted dispatch",
        Duration::from_secs(5),
    )
    .await;
    assert_eq!(
        accepted[0]
            .jobs
            .iter()
            .map(|j| j.id.as_str())
            .collect::<Vec<_>>(),
        vec![job2.id.as_str()],
        "only the accepted job is committed to the room"
    );
}

// ---------------------------------------------------------------------------
// AGENT_WIRE:ServerMessage.assignment
// ---------------------------------------------------------------------------

#[tokio::test(start_paused = true)]
async fn server_message_assignment_is_written_only_after_the_job_is_in_running_jobs() {
    let graph = Graph::new().await;
    let (addr, _server) = graph.spawn_agent_route().await;
    let mut worker = FakeWorker::register_room_agent(&graph, addr).await;
    let room = "c86b-assignment";
    let key = RoomKey::new(ProjectId::local(), room);
    graph
        .manager
        .create_room(key.clone(), Graph::create_room_opts())
        .await
        .unwrap();
    graph
        .manager
        .create_agent_dispatch(
            &key,
            AgentDispatchOptions {
                id: "AD_c86b_assign".into(),
                agent_name: AGENT_NAME.into(),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    let offer = worker.next_data_frame().await;
    let job = match offer.one() {
        pb::server_message::Message::Availability(request) => {
            request.job.as_ref().expect("offered job").clone()
        }
        other => panic!("availability offer expected, got {:?}", other.clone()),
    };

    // Go's ordering: the pending slot exists while the worker answers
    // (worker.go:391-397), `runningJobs[jobID]` is inserted at :470-474 *before*
    // the assignment write at :475, and a failed write deletes it again
    // (:476-480). Observe the left half of that window directly: the peer has read
    // nothing yet, so the assignment is queued but unwritten, and the insert can
    // only be visible if it really precedes the send.
    worker
        .peer
        .send_proto(&message(pb::worker_message::Message::Availability(
            pb::AvailabilityResponse {
                job_id: job.id.clone(),
                available: true,
                participant_identity: "c86b-assigned-agent".into(),
                participant_name: "C86B Agent".into(),
                ..Default::default()
            },
        )))
        .await;
    poll_until(
        Duration::from_secs(5),
        "pre-write runningJobs insert",
        || {
            graph
                .service
                .registry
                .running_job_ids_for_test(&worker.worker_id)
                .contains(&job.id)
        },
    )
    .await;
    let assignment = worker.next_data_frame().await;
    let assigned = match assignment.one() {
        pb::server_message::Message::Assignment(assignment) => assignment.clone(),
        other => panic!(
            "accepted availability must produce ServerMessage.assignment, got {:?}",
            other.clone()
        ),
    };
    assert_eq!(assigned.job.as_ref().unwrap().id, job.id);
    assert_eq!(
        assigned
            .job
            .as_ref()
            .and_then(|j| j.state.as_ref())
            .map(|s| s.participant_identity.clone())
            .unwrap_or_default(),
        "c86b-assigned-agent",
        "the accepted identity lands in the stored state (worker.go:438)"
    );

    // The minted token is a real source-equivalent agent room-join token, not a
    // placeholder: it verifies against the configured key and carries the agent
    // grant plus the `lk.agent_name` attribute (protoagent.BuildAgentToken,
    // worker.go:447-459 / protocol/agent/token.go).
    let auth = crate::auth::authenticate_token(&graph.keys, Some(&assigned.token))
        .expect("assignment token verifies")
        .expect("non-empty grants");
    assert!(auth.grants.video.agent && auth.grants.video.room_join);
    assert_eq!(auth.grants.video.room, room);
    assert_eq!(auth.grants.identity, "c86b-assigned-agent");
    assert_eq!(auth.grants.attributes["lk.agent_name"], AGENT_NAME);
    assert_eq!(auth.grants.name, "C86B Agent");

    // And the room side only ever stores an assignment the worker actually received.
    let committed = poll_dispatches(
        graph.manager.clone(),
        key.clone(),
        "AD_c86b_assign",
        "committed room job",
        Duration::from_secs(5),
    )
    .await;
    assert_eq!(
        committed[0]
            .jobs
            .iter()
            .map(|j| (
                j.id.as_str(),
                j.state.status,
                j.state.participant_identity.as_str()
            ))
            .collect::<Vec<_>>(),
        vec![(job.id.as_str(), 1, "c86b-assigned-agent")],
        "the committed record is the RUNNING assignment itself"
    );
}

// ---------------------------------------------------------------------------
// AGENT_WIRE:WorkerMessage.update_worker
// ---------------------------------------------------------------------------

#[tokio::test(start_paused = true)]
async fn worker_message_update_worker_flows_through_the_production_dispatch_loop() {
    let graph = Graph::new().await;
    let (addr, _server) = graph.spawn_agent_route().await;
    let mut worker = FakeWorker::register_room_agent(&graph, addr).await;

    // Seq gate (worker.go:642-647): a newer seq wins, a stale or equal seq returns
    // before *every* field write, so nothing may regress.
    worker
        .peer
        .send_proto(&update_worker(5, 1 /* WS_FULL */, 0.9, true))
        .await;
    worker
        .peer
        .send_proto(&update_worker(4, 0 /* WS_AVAILABLE */, 0.1, false))
        .await;
    // Fence: the pong is only emitted after the reader processed both updates,
    // because the session loop is serial (DispatchWorkerSignal, worker.go:80-100).
    worker.peer.send_proto(&ping(1_001)).await;
    let pong = worker.next_data_frame().await;
    assert!(
        matches!(pong.one(), pb::server_message::Message::Pong(p) if p.last_timestamp == 1_001),
        "the pong fences the two queued updates"
    );
    let snapshot = graph.service.registry.oracle_snapshot(&worker.worker_id);
    assert_eq!(
        (snapshot.0, snapshot.1, snapshot.2),
        (1, 0.9, true),
        "the stale seq must not regress status/load/draining"
    );

    // Equal seq is stale too (`seq <= statusSeq`, worker.go:643).
    worker
        .peer
        .send_proto(&update_worker(5, 0, 0.05, false))
        .await;
    worker.peer.send_proto(&ping(1_002)).await;
    worker.next_data_frame().await;
    let snapshot = graph.service.registry.oracle_snapshot(&worker.worker_id);
    assert_eq!((snapshot.0, snapshot.1, snapshot.2), (1, 0.9, true));

    // seq == 0 bypasses the gate entirely (worker.go:642 `if seq := ...; seq != 0`).
    worker
        .peer
        .send_proto(&update_worker(0, 0, 0.25, false))
        .await;
    worker.peer.send_proto(&ping(1_003)).await;
    worker.next_data_frame().await;
    let snapshot = graph.service.registry.oracle_snapshot(&worker.worker_id);
    assert_eq!((snapshot.0, snapshot.1, snapshot.2), (0, 0.25, false));

    // The affinity predicate mirrors Go's `AgentHandler.JobRequestAffinity`
    // (pkg/service/agenthandler.go:449-465): workers matching the job key on
    // agent name/namespace/type/deployment contribute `max(0, targetLoad - Load())`
    // and only while `Status() == WS_AVAILABLE`. The target is the configured
    // `agents.target_load`, whose pinned default is 0.7
    // (pkg/agent/config.go:5, pkg/config/config.go:658), so an AVAILABLE worker at
    // load 0.25 offers 0.45 — not the `1 - load` used by the separate
    // random-walk selection in selectWorkerWeightedByLoad
    // (pkg/service/agenthandler.go:540-568).
    let key = registration_key(&graph, &worker.worker_id);
    assert_eq!(
        graph.service.registry.affinity(&key, 0.7),
        0.45,
        "an AVAILABLE worker at load 0.25 offers target_load - load (agenthandler.go:460)"
    );
    worker
        .peer
        .send_proto(&update_worker(9, 1 /* WS_FULL */, 1.0, false))
        .await;
    worker.peer.send_proto(&ping(1_004)).await;
    worker.next_data_frame().await;
    assert_eq!(
        graph.service.registry.affinity(&key, 0.7),
        0.0,
        "affinity sums only over status==WS_AVAILABLE workers (agenthandler.go:459)"
    );
}

// ---------------------------------------------------------------------------
// AGENT_WIRE:WorkerMessage.update_job
// ---------------------------------------------------------------------------

#[tokio::test(start_paused = true)]
async fn worker_message_update_job_drives_the_production_room_job_state_machine() {
    let graph = Graph::new().await;
    let (addr, _server) = graph.spawn_agent_route().await;
    let mut worker = FakeWorker::register_room_agent(&graph, addr).await;
    let room = "c86b-update-job";
    let key = RoomKey::new(ProjectId::local(), room);
    graph
        .manager
        .create_room(key.clone(), Graph::create_room_opts())
        .await
        .unwrap();
    // A real media participant makes the room non-empty, so the agent job's own
    // terminal state cannot be raced by the empty-room teardown.
    graph.join_publisher(room, "c86b-viewer").await;
    let job_id = graph
        .accept_dispatch(&mut worker, &key, "AD_c86b_uj", "c86b-uj-agent")
        .await;

    // UpdateJobStatus for an unknown jobId surfaces psrpc NotFound
    // (worker.go:589-592). HandleUpdateJob treats it as a debug log and keeps the
    // connection alive (worker.go:568-578), so a later signal must still round-trip.
    worker.peer.send_proto(&update_job("AJ_ghost", 1, "")).await;
    worker.peer.send_proto(&ping(2_001)).await;
    let pong = worker.next_data_frame().await;
    assert!(
        matches!(pong.one(), pb::server_message::Message::Pong(p) if p.last_timestamp == 2_001),
        "an unknown-job update must not tear the session down (worker.go:568-578)"
    );

    // A non-terminal update keeps the original StartedAt: Go stamps it only on the
    // PENDING→non-PENDING edge (worker.go:595-597), and AssignJob already moved the
    // state to JS_RUNNING (worker.go:413), so this update cannot restamp it.
    let before = graph
        .service
        .registry
        .get_job_state_for_test(&worker.worker_id, &job_id)
        .expect("running job state");
    worker.peer.send_proto(&update_job(&job_id, 1, "")).await;
    worker.peer.send_proto(&ping(2_002)).await;
    worker.next_data_frame().await;
    let after = graph
        .service
        .registry
        .get_job_state_for_test(&worker.worker_id, &job_id)
        .expect("still running");
    assert_eq!(after.started_at, before.started_at, "started_at is sticky");
    assert!(after.updated_at >= before.updated_at);

    // An ended status records EndedAt and removes the running entry
    // (worker.go:600-604).
    worker
        .peer
        .send_proto(&update_job(&job_id, 2 /* JS_SUCCESS */, ""))
        .await;
    worker.peer.send_proto(&ping(2_003)).await;
    worker.next_data_frame().await;
    assert!(
        graph
            .service
            .registry
            .running_job_ids_for_test(&worker.worker_id)
            .is_empty(),
        "ended job leaves runningJobs (worker.go:602-604)"
    );
    // The room's dispatch record is *not* updated by a worker job status. Go
    // clones the job in `AssignJob` (worker.go:387), so `UpdateJob` writes the
    // worker-side copy only (worker.go:599-607); the dispatch record keeps the state
    // object the room itself appended (room.go:1838) until `DeleteAgentDispatch`
    // replaces it with a terminate result (room.go:948-950), and
    // `AgentHandler.deregisterJob` still ends with the literal
    // `// TODO update dispatch state` (agenthandler.go:382-388). Rust reproduces that
    // split, so the observable room state stays the running stamp.
    let retired = poll_dispatches(
        graph.manager.clone(),
        key.clone(),
        "AD_c86b_uj",
        "room job record",
        Duration::from_secs(5),
    )
    .await;
    let states = retired
        .iter()
        .flat_map(|d| {
            d.jobs
                .iter()
                .map(|j| (j.id.clone(), j.state.status, j.state.ended_at_ns))
        })
        .collect::<Vec<_>>();
    assert!(
        states
            .iter()
            .any(|(id, status, ended_at)| id == &job_id && *status == 1 && *ended_at == 0),
        "a worker job status update never reaches the room dispatch record \
         (agenthandler.go:387): {states:?}"
    );

    // FAILED carries the worker's own error string verbatim (worker.go:599). A fresh
    // dispatch gives a job that is still running to fail.
    let second = graph
        .accept_dispatch(&mut worker, &key, "AD_c86b_uj_fail", "c86b-uj-agent-2")
        .await;
    worker
        .peer
        .send_proto(&update_job(&second, 3 /* JS_FAILED */, "boom"))
        .await;
    worker.peer.send_proto(&ping(2_004)).await;
    worker.next_data_frame().await;
    let failed = poll_dispatches(
        graph.manager.clone(),
        key.clone(),
        "AD_c86b_uj_fail",
        "failed room job",
        Duration::from_secs(5),
    )
    .await;
    let recorded = failed
        .iter()
        .flat_map(|d| d.jobs.iter())
        .find(|j| j.id == second)
        .expect("the dispatch keeps its job entry even when the worker fails it");
    assert_eq!(
        (recorded.state.status, recorded.state.error.as_str()),
        (1, ""),
        "a worker JS_FAILED stays out of the room dispatch record, for the same reason \
         as the JS_SUCCESS above (agenthandler.go:387)"
    );
    // The failed job leaves the server-side map with its ended status:
    // `UpdateJobStatus` deletes the running entry once `JobStatusIsEnded` matches
    // (worker.go:602-604), so the readable-state seam (`Worker.GetJobState`,
    // worker.go:375-383) answers NotFound afterwards and the state the update
    // returned (worker.go:609) is discarded by `HandleUpdateJob` (:570-572). The
    // worker's own error string therefore reaches no room-visible surface on this
    // path, which is why the room record above still shows `(1, "")`.
    assert!(
        graph
            .service
            .registry
            .get_job_state_for_test(&worker.worker_id, &second)
            .is_none(),
        "an ended job is unreadable from runningJobs (worker.go:602-604, :375-383)"
    );
    // Go's `agentHandlerWorker.HandleUpdateJob` additionally drops the job→worker
    // routing for an ended status (agenthandler.go:578-588 through deregisterJob
    // :382-388), so a later `JobTerminate` for the same job finds no worker
    // (agenthandler.go:467-474) and `Worker.TerminateJob` answers ErrJobNotFound
    // (worker.go:497-502). The registry gate reproduces that without writing a
    // termination frame, which the pong fence below proves.
    assert!(matches!(
        graph
            .service
            .registry
            .terminate(&worker.worker_id, &second, false, 4_000)
            .await,
        Err(WorkerError::JobNotFound)
    ));
    worker.peer.send_proto(&ping(2_005)).await;
    let fenced = worker.next_data_frame().await;
    assert!(
        matches!(fenced.one(), pb::server_message::Message::Pong(p) if p.last_timestamp == 2_005),
        "a refused terminate writes nothing and keeps the session serving (worker.go:497-502)"
    );
    assert!(
        graph
            .service
            .registry
            .running_job_ids_for_test(&worker.worker_id)
            .is_empty(),
        "a failed job also leaves runningJobs (worker.go:602-604)"
    );
}

// ---------------------------------------------------------------------------
// AGENT_WIRE:WorkerMessage.ping + AGENT_WIRE:ServerMessage.pong
// ---------------------------------------------------------------------------

#[tokio::test(start_paused = true)]
async fn worker_message_ping_is_answered_by_server_message_pong_with_a_real_round_trip() {
    let graph = Graph::new().await;
    // This leg owns the session's event sink: the assertion below is about what
    // the dispatch loop pushed, and both events it receives here are consumer
    // no-ops, so removing them from the bridge changes nothing under test.
    let (addr, _server, mut events) = graph.spawn_route_with_own_sink().await;

    // Ping is served during the handshake too: Go embeds WorkerPingHandler inside
    // WorkerRegisterer (worker.go:130-143,210-218).
    let mut peer = RawWs::connect(addr, &graph.worker_token()).await;
    let sent = unix_ms();
    peer.send_json(br#"{"ping":{"timestamp":"1234567890123"}}"#)
        .await;
    let frame = peer.receive().await;
    let pong = match frame.one() {
        pb::server_message::Message::Pong(pong) => *pong,
        other => panic!("ping must be answered by ServerMessage.pong, got {other:?}"),
    };
    assert_eq!(
        pong.last_timestamp, 1_234_567_890_123,
        "echo of the request"
    );
    assert!(
        pong.timestamp >= sent - 1_000,
        "server clock in ms (worker.go:135-141)"
    );
    assert!(frame.text, "JSON in answers JSON out (wsprotocol.go:205)");

    // After registration the same leg is served by the dispatch loop, and the
    // observation is queued for the event consumer, which measures the round
    // trip rather than mutating job state (bridge.rs `WorkerEvent::Ping`).
    peer.send_json(
        format!(r#"{{"register":{{"type":"JT_ROOM","agentName":"{AGENT_NAME}"}}}}"#).as_bytes(),
    )
    .await;
    let reply = peer.receive().await;
    let response = match reply.one() {
        pb::server_message::Message::Register(response) => response.clone(),
        other => panic!("register reply expected, got {:?}", other.clone()),
    };
    let worker_id = response.worker_id;

    // Binary input flips the reply codec to protobuf for this connection
    // (wsprotocol.go:152-158).
    peer.send_proto(&ping(987_654)).await;
    let pong = peer.receive().await;
    assert!(!pong.text, "binary input switches the reply codec");
    assert!(matches!(
        pong.one(),
        pb::server_message::Message::Pong(p) if p.last_timestamp == 987_654
    ));

    // The session queued both pings, in write order, with the echoed pair. The
    // registration event shares the sink, so the wait counts pings only.
    let events = collect_events(&mut events, 2, |event| {
        matches!(event, WorkerEvent::Ping { .. })
    })
    .await;
    let pings: Vec<(String, i64, i64)> = events
        .iter()
        .filter_map(|event| match event {
            WorkerEvent::Ping {
                worker_id: id,
                last_timestamp,
                timestamp,
            } => Some((id.clone(), *last_timestamp, *timestamp)),
            _ => None,
        })
        .collect();
    assert_eq!(
        pings.iter().map(|p| p.1).collect::<Vec<_>>(),
        vec![1_234_567_890_123, 987_654],
        "both served pings reach the production consumer in write order"
    );
    for (id, last, stamp) in &pings {
        assert_eq!(id, &worker_id, "the observation names its worker");
        assert!(
            *stamp >= sent - 60_000,
            "the server stamps its own clock (worker.go:136), not the echoed one: {last}"
        );
    }

    // The pong write is acknowledged before the observation is emitted, so a
    // consumer never sees a ping whose pong failed (DispatchAgentWorkerSignal
    // returns false on a write error, agenthandler.go:43-60): reaching the event
    // implies the frame landed on the socket, which the reads above consumed.
    // And a ping mutates no job/worker state (worker.go:135-143).
    let snapshot = graph.service.registry.oracle_snapshot(&worker_id);
    assert_eq!(
        (snapshot.0, snapshot.1, snapshot.2, snapshot.3),
        (0, 0.0, false, 0),
        "a ping mutates no worker/job state (worker.go:135-143)"
    );
}

// ---------------------------------------------------------------------------
// AGENT_WIRE:ServerMessage.termination
// ---------------------------------------------------------------------------

#[tokio::test(start_paused = true)]
async fn server_message_termination_precedes_the_job_state_flip_over_real_websocket() {
    let graph = Graph::new().await;
    let (addr, _server) = graph.spawn_agent_route().await;
    let mut worker = FakeWorker::register_room_agent(&graph, addr).await;
    let room = "c86b-termination";
    let key = RoomKey::new(ProjectId::local(), room);
    graph
        .manager
        .create_room(key.clone(), Graph::create_room_opts())
        .await
        .unwrap();
    graph.join_publisher(room, "c86b-term-viewer").await;
    // The worker answers the offer without naming an agent participant, so the
    // consumer has no RTC admission to wait for and terminates directly.
    let job_id = graph
        .accept_dispatch(&mut worker, &key, "AD_c86b_term", "")
        .await;

    // `Room.DeleteAgentDispatch` terminates every recorded job (room.go:927-928) and
    // writes the returned state back only afterwards (room.go:948-950), and
    // `Worker.TerminateJob` puts the frame on the wire (worker.go:504) before the
    // status update (:517). Deleting the dispatch is the production producer.
    let removed = graph
        .manager
        .delete_agent_dispatch(&key, "AD_c86b_term")
        .await
        .unwrap();
    let termination = worker.next_data_frame().await;
    let term = match termination.one() {
        pb::server_message::Message::Termination(term) => term.clone(),
        other => panic!(
            "ServerMessage.termination expected, got {:?}",
            other.clone()
        ),
    };
    assert_eq!(term.job_id, job_id);

    let states = removed
        .jobs
        .iter()
        .map(|j| (j.id.clone(), j.state.status, j.state.error.clone()))
        .collect::<Vec<_>>();
    assert!(
        states
            .iter()
            .any(|(id, status, error)| id == &job_id && *status == 1 && error.is_empty()),
        "the delete response carries the record as the room last wrote it; Go's \
         terminate write-back at room.go:948-950 lands on the already-removed object \
         and never reaches the listing: {states:?}"
    );
    // The record is gone from the room (room.go:913 removes it before terminating),
    // so a second delete is the typed not-found error.
    assert!(
        graph
            .manager
            .list_agent_dispatches(&key, Some("AD_c86b_term"))
            .await
            .unwrap()
            .is_empty(),
        "a deleted dispatch leaves the room record set (room.go:913)"
    );
    let reason = graph
        .manager
        .delete_agent_dispatch(&key, "AD_c86b_term")
        .await
        .unwrap_err()
        .to_string();
    assert!(
        reason.contains("dispatch ID not found"),
        "the second delete reports room.go:910 `dispatch ID not found`: {reason}"
    );

    // TERMINATION_REQUESTED is not AGENT_LEFT_ROOM, so the job ends JS_SUCCESS with
    // no error text (worker.go:510-515) and leaves runningJobs (:602-604).
    poll_until(
        Duration::from_secs(12),
        "terminated runningJobs entry",
        || {
            !graph
                .service
                .registry
                .running_job_ids_for_test(&worker.worker_id)
                .contains(&job_id)
        },
    )
    .await;

    // Terminating a job that already ended reports the typed not-found error
    // (worker.go:495-502 `ErrJobNotFound`) and sends no frame.
    let err = graph
        .service
        .registry
        .terminate(&worker.worker_id, &job_id, false, unix_ms() * 1_000_000)
        .await;
    assert_eq!(err.unwrap_err(), WorkerError::JobNotFound);
    // And the refusal wrote nothing: the next frame is still the fenced pong.
    worker.peer.send_proto(&ping(5_001)).await;
    let pong = worker.next_data_frame().await;
    assert!(
        matches!(pong.one(), pb::server_message::Message::Pong(p) if p.last_timestamp == 5_001),
        "a rejected terminate sends no extra frame (worker.go:495-502)"
    );
}

// ---------------------------------------------------------------------------
// AGENT_WIRE:ServerMessage.go_away
// ---------------------------------------------------------------------------

#[tokio::test(start_paused = true)]
async fn server_message_go_away_has_no_pinned_go_producer_so_rust_invents_none() {
    // `ServerMessage.go_away` is field 6 of the protocol envelope
    // (`protobufs/livekit_agent.proto`, `AgentHttp.GoAway`), but no non-generated
    // Go source in the pinned tree constructs or sends it: `grep -rn GoAway
    // livekit-server@v1.13.10` hits only generated `.pb.go` files. So the honest
    // disposition is a documented refusal to invent a producer, evidenced by the
    // fact that a fully exercised real-transport worker receives exactly the frames
    // the pinned Go does — register, pong, availability, assignment, termination —
    // and never a go_away.
    let graph = Graph::new().await;
    let (addr, _server) = graph.spawn_agent_route().await;
    let mut worker = FakeWorker::register_room_agent(&graph, addr).await;
    let room = "c86b-go-away";
    let key = RoomKey::new(ProjectId::local(), room);

    worker.peer.send_proto(&ping(3_001)).await;
    let pong = worker.next_data_frame().await;
    assert!(
        matches!(pong.one(), pb::server_message::Message::Pong(p) if p.last_timestamp == 3_001),
        "AGENT_WIRE:ServerMessage.pong"
    );
    worker
        .peer
        .send_proto(&update_worker(1, 0, 0.1, false))
        .await;
    graph
        .manager
        .create_room(key.clone(), Graph::create_room_opts())
        .await
        .unwrap();
    graph
        .manager
        .create_agent_dispatch(
            &key,
            AgentDispatchOptions {
                id: "AD_c86b_gone".into(),
                agent_name: AGENT_NAME.into(),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    let offer = worker.next_data_frame().await;
    let job_id = match offer.one() {
        pb::server_message::Message::Availability(request) => {
            request.job.as_ref().expect("offered job").id.clone()
        }
        other => panic!(
            "the producer after registration is availability, not go_away, got {:?}",
            other.clone()
        ),
    };
    worker
        .peer
        .send_proto(&FakeWorker::availability(&job_id, true))
        .await;
    let assignment = worker.next_data_frame().await;
    assert!(
        matches!(assignment.one(), pb::server_message::Message::Assignment(_)),
        "AGENT_WIRE:ServerMessage.assignment"
    );
    graph
        .manager
        .delete_agent_dispatch(&key, "AD_c86b_gone")
        .await
        .unwrap();
    let termination = worker.next_data_frame().await;
    assert!(
        matches!(
            termination.one(),
            pb::server_message::Message::Termination(_)
        ),
        "AGENT_WIRE:ServerMessage.termination"
    );
    for frame in [&pong, &offer, &assignment, &termination] {
        assert!(
            !matches!(frame.one(), pb::server_message::Message::GoAway(_)),
            "no observed frame may be go_away: the pinned Go has no producer"
        );
    }

    // The encoded shape itself must stay field-6 compatible so a future producer
    // cannot silently reframe it — through the bare protobuf envelope, through
    // protojson, and through the very `WireCodec` the session switches on.
    let away = pb::ServerMessage {
        message: Some(pb::server_message::Message::GoAway(
            pb::agent_http::GoAway {
                reason: "node drain".into(),
            },
        )),
    };
    let bytes = away.encode_to_vec();
    assert_eq!(
        pb::ServerMessage::decode(bytes.as_slice()),
        Ok(away.clone())
    );
    let json = String::from_utf8(lk_protocol::json::encode_default(&away).unwrap()).unwrap();
    assert!(json.contains("\"goAway\""), "protojson lowerCamel: {json}");
    assert_eq!(
        lk_protocol::json::decode::<pb::ServerMessage>(json.as_bytes()).unwrap(),
        away
    );
    // `WireCodec::encode` is the outbound half the session actually uses
    // (`codec.rs:31-42`), so the frame it produces must still carry `goAway` under
    // the same envelope reader the pinned Go writer would produce.
    for wire_codec in [WireCodec::Protobuf, WireCodec::Json] {
        let frame = wire_codec.encode(&away).unwrap();
        let decoded = match frame {
            axum::extract::ws::Message::Binary(bytes) => {
                assert!(
                    matches!(wire_codec, WireCodec::Protobuf),
                    "protobuf codec must emit a binary frame"
                );
                pb::ServerMessage::decode(bytes.as_ref()).unwrap()
            }
            axum::extract::ws::Message::Text(text) => {
                assert!(
                    matches!(wire_codec, WireCodec::Json),
                    "protojson codec must emit a text frame"
                );
                lk_protocol::json::decode::<pb::ServerMessage>(text.as_ref()).unwrap()
            }
            other => panic!("unexpected frame shape: {other:?}"),
        };
        assert_eq!(decoded, away, "{wire_codec:?} must carry go_away unchanged");
    }
}

// ---------------------------------------------------------------------------
// AGENT_WIRE:WorkerMessage.simulate_job
// ---------------------------------------------------------------------------

#[tokio::test(start_paused = true)]
async fn worker_message_simulate_job_assigns_through_the_same_production_worker() {
    let graph = Graph::new().await;
    let (addr, _server) = graph.spawn_agent_route().await;
    let mut worker = FakeWorker::register(
        &graph,
        addr,
        format!(
            r#"{{"register":{{"type":"JT_ROOM","agentName":"{AGENT_NAME}","namespace":"c86b-ns"}}}}"#
        )
        .as_bytes(),
    )
    .await;

    // HandleSimulateJob (worker.go:610-636): the request's own `type` is ignored;
    // JT_PUBLISHER only when a participant is supplied, the id gets the AJ_ prefix
    // and namespace/agentName come from the registration.
    worker
        .peer
        .send_proto(&message(pb::worker_message::Message::SimulateJob(
            pb::SimulateJobRequest {
                r#type: 2, // ignored by Go
                room: Some(pb::Room {
                    name: "c86b-sim".into(),
                    ..Default::default()
                }),
                participant: None,
            },
        )))
        .await;
    let offer = worker.next_data_frame().await;
    let request = match offer.one() {
        pb::server_message::Message::Availability(request) => request.clone(),
        other => panic!(
            "simulate must offer ServerMessage.availability, got {:?}",
            other.clone()
        ),
    };
    let job = request.job.unwrap();
    assert!(job.id.starts_with("AJ_"), "guid.AgentJobPrefix");
    assert_eq!(job.r#type, 0, "JT_ROOM without a participant");
    assert_eq!(
        job.namespace, "c86b-ns",
        "from the registration, not the request (worker.go:622)"
    );
    assert_eq!(job.agent_name, AGENT_NAME);
    assert_eq!(job.room.as_ref().unwrap().name, "c86b-sim");
    assert_eq!(job.dispatch_id, "", "a simulated job has no dispatch");

    // Accepting the simulated job walks the identical AssignJob path, so the
    // assignment arrives with a real token.
    worker
        .peer
        .send_proto(&FakeWorker::availability(&job.id, true))
        .await;
    let assignment = worker.next_data_frame().await;
    let assigned = match assignment.one() {
        pb::server_message::Message::Assignment(assignment) => assignment.clone(),
        other => panic!(
            "assignment for the simulated job expected, got {:?}",
            other.clone()
        ),
    };
    assert_eq!(assigned.job.as_ref().unwrap().id, job.id);
    let auth = crate::auth::authenticate_token(&graph.keys, Some(&assigned.token))
        .unwrap()
        .unwrap();
    assert!(auth.grants.video.agent);
    assert_eq!(auth.grants.video.room, "c86b-sim");

    // With a participant the same signal yields JT_PUBLISHER (worker.go:612-614).
    worker
        .peer
        .send_proto(&message(pb::worker_message::Message::SimulateJob(
            pb::SimulateJobRequest {
                r#type: 0,
                room: Some(pb::Room {
                    name: "c86b-sim2".into(),
                    ..Default::default()
                }),
                participant: Some(pb::ParticipantInfo {
                    sid: "PA_sim".into(),
                    identity: "sim-target".into(),
                    ..Default::default()
                }),
            },
        )))
        .await;
    let second = worker.next_data_frame().await;
    let request2 = match second.one() {
        pb::server_message::Message::Availability(request) => request.clone(),
        other => panic!("second simulate offer expected, got {:?}", other.clone()),
    };
    let job2 = request2.job.unwrap();
    assert_eq!(job2.r#type, 1, "JT_PUBLISHER with a participant");
    assert_eq!(job2.participant.unwrap().identity, "sim-target");
    worker
        .peer
        .send_proto(&FakeWorker::availability(&job2.id, false))
        .await;
    // A declined simulation never reaches runningJobs (worker.go:436).
    poll_until(Duration::from_secs(5), "declined simulation", || {
        graph
            .service
            .registry
            .running_job_ids_for_test(&worker.worker_id)
            .iter()
            .all(|id| id != &job2.id)
    })
    .await;
}

// ---------------------------------------------------------------------------
// AGENT_WIRE:WorkerMessage.migrate_job
// ---------------------------------------------------------------------------

#[tokio::test(start_paused = true)]
async fn worker_message_migrate_job_stays_the_pinned_oss_no_op() {
    let graph = Graph::new().await;
    let (addr, _server) = graph.spawn_agent_route().await;
    let mut worker = FakeWorker::register_room_agent(&graph, addr).await;
    let room = "c86b-migrate";
    let key = RoomKey::new(ProjectId::local(), room);
    graph
        .manager
        .create_room(key.clone(), Graph::create_room_opts())
        .await
        .unwrap();
    graph.join_publisher(room, "c86b-mig-viewer").await;
    let job_id = graph
        .accept_dispatch(&mut worker, &key, "AD_c86b_mig", "c86b-mig-agent")
        .await;
    let before_status = graph.service.registry.oracle_snapshot(&worker.worker_id);

    // HandleMigrateJob (worker.go:663-669) is an OSS no-op that returns nil: no
    // reply frame, no teardown, no state change.
    worker
        .peer
        .send_proto(&message(pb::worker_message::Message::MigrateJob(
            pb::MigrateJobRequest {
                job_ids: vec![job_id.clone()],
            },
        )))
        .await;
    worker.peer.send_proto(&ping(4_001)).await;
    let pong = worker.next_data_frame().await;
    assert!(
        matches!(pong.one(), pb::server_message::Message::Pong(p) if p.last_timestamp == 4_001),
        "the connection survives the migrate signal"
    );
    assert_eq!(
        graph.service.registry.oracle_snapshot(&worker.worker_id),
        before_status,
        "migrate mutates no worker state"
    );
    assert_eq!(
        graph
            .service
            .registry
            .running_job_ids_for_test(&worker.worker_id),
        vec![job_id.clone()],
        "and it moves no job"
    );
    assert!(
        graph
            .service
            .registry
            .get_job_state_for_test(&worker.worker_id, &job_id)
            .is_some(),
        "the job stays running on the same worker"
    );
}
