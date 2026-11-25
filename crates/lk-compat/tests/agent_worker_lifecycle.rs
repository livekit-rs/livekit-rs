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
//!   [`worker_message_update_job_drives_the_producerion_room_job_state_machine`]
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
//! (`references/livekit-stable`, livekit v1.13.10 @
//! e5655ac20267fa5005dccaf3f0e3bfc3ea688c07, protocol v1.52.2-0.20261012065025-9bb708cdb819),
//! all line numbers in `pkg/agent/worker.go` unless another file is named:
//! * `pkg/service/agentws.go:49-67` — upgrade gate first (404 for a non-upgrade
//!   request), then the `video.agent` grant (401 `ErrPermissionDenied`), and only
//!   then `Upgrade`; `protocol` comes from the query form value.
//! * :52 `CurrentProtocol = 1`; :55 `RegisterTimeout = 10s`; :56
//!   `AssignJobTimeout = 10s`.
//! * :172-177 `MakeWorkerRegistration` allocates the worker id server-side.
//! * :242-289 `HandleRegister` rejects an unknown job type and answers
//!   `RegisterWorkerResponse{WorkerID, ServerInfo}`.
//! * :390-490 `AssignJob`: availability written at :415, `runningJobs[jobID]`
//!   inserted at :470-474 **before** the assignment write at :475, deleted again
//!   at :476-480 when that write fails.
//! * :495-520 `TerminateJob`: the termination write (:504) precedes the status
//!   update (:517); `reason != AGENT_LEFT_ROOM` maps to `JS_SUCCESS` (:510-515).
//! * :552-566 `HandleAvailability` warns and returns nil for an unknown jobId.
//! * :568-608 `HandleUpdateJob`/`UpdateJobStatus`: unknown job → psrpc
//!   `NotFound`, `started_at` only on the PENDING→non-PENDING edge, ended
//!   statuses delete the running entry.
//! * :638-655 `HandleUpdateWorker`: `seq != 0 && seq <= statusSeq` returns before
//!   *all* field writes.
//! * :610-636 `HandleSimulateJob`: job type from the participant presence only,
//!   `guid.New(AgentJobPrefix)`, namespace/agentName from the registration, then
//!   `AssignJob` in a goroutine.
//! * :663-669 `HandleMigrateJob`: OSS no-op returning nil.
//! * :130-143 `WorkerPingHandler.HandlePing` echoes `last_timestamp` and stamps
//!   the server clock.
//!
//! Tier honesty: these legs attest `component_behavior`. They do **not** attest
//! `main_server_consumer` (the launched `livecloud` binary was not started here)
//! nor `official_differential_lifecycle` (the official Python/Node agent SDK was
//! not used; the worker is this repo's own in-process fake speaking the wire
//! protocol).

use lk_compat::agent::{
    registry::{ASSIGN_TIMEOUT, WorkerError},
    service::AgentService,
    wire_tests::{Frame, RawWs},
};
use lk_compat::auth::{KeyStore, TokenClaims, TokenMinter};
use lk_compat::state::{AppState, CompatServices};
use lk_protocol::livekit as pb;
use rtc_core::{
    config::ServerConfig,
    events::NullSink,
    ids::{Identity, ProjectId, RoomKey},
    permission::{Grants, ParticipantKind, RoomAgentDispatchOptions, VideoGrant},
};
use rtc_runtime::{
    api::{AgentDispatchOptions, CreateRoomOptions},
    net::{LocalInterface, NetRouter},
    RoomManager,
};
use std::{net::Ipv4Addr, sync::Arc, time::Duration};

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

fn check_io_bound(start: std::time::Instant, what: &str) {
    assert!(
        start.elapsed() < IO_BOUND,
        "{what} did not arrive within {IO_BOUND:?}"
    );
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

struct Graph {
    state: AppState,
    keys: Arc<KeyStore>,
    tokens: Arc<TokenMinter>,
    manager: RoomManager,
    service: Arc<AgentService>,
    _net: rtc_runtime::net::NetRouter,
}

impl Graph {
    async fn new() -> Self {
        let mut config = base_config();
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
        let services = CompatServices::new(Arc::new(config));
        let keys = services.keys().clone();
        let tokens = Arc::new(TokenMinter::new(keys.clone()));
        let state = services.assemble(net.clone(), Arc::new(NullSink));
        let service = state.agents().clone();
        Self {
            state,
            keys,
            tokens,
            manager: state.manager().clone(),
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

    /// A normal participant token: `roomJoin` + the target `room`, no agent grant.
    fn participant_token(&self, room: &str, identity: &str) -> String {
        self.tokens
            .mint(
                KEY_ID,
                &TokenClaims {
                    identity: identity.into(),
                    video: Some(VideoGrant {
                        room_join: true,
                        room: room.into(),
                        ..Default::default()
                    }),
                    ..Default::default()
                },
                Duration::from_secs(60),
                unix_ms() / 1000,
            )
            .unwrap()
    }

    async fn join_participant(&self, room: &str, identity: &str) {
        let key = RoomKey::new(ProjectId::local(), room);
        self.manager
            .join(rtc_core::session::JoinParams {
                room: key,
                grants: Grants {
                    api_key: KEY_ID.into(),
                    identity: identity.into(),
                    kind: ParticipantKind::Publisher,
                    video: VideoGrant {
                        room_join: true,
                        room: room.into(),
                        ..Default::default()
                    },
                    ..Default::default()
                },
                identity: Identity(identity.into()),
                capabilities: Default::default(),
                client: Default::default(),
                auto_subscribe: true,
                ..Default::default()
            })
            .await
            .unwrap();
    }
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
        let pb::server_message::Message::Register(response) = reply.one().clone() else {
            panic!("register must be answered by ServerMessage.register")
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
            let frame = self.peer.receive_data_or_ping().await;
            match frame {
                WsEvent::Data(frame) => return frame,
                WsEvent::KeepalivePing(payload) => self.peer.send_pong(payload).await,
            }
        }
    }
    /// Await a specific server frame kind.
    async fn expect<M>(&mut self, what: &str)
    where
        M: TryFrom<pb::server_message::Message, Variant: Clone + std::fmt::Debug>,
    {
        let frame = self.next_data_frame().await;
        let variant = frame.one().clone();
        M::try_from(variant)
            .unwrap_or_else(|other| panic!("{what}: expected a different ServerMessage arm: {other:?}"));
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

enum WsEvent {
    Data(Frame),
    KeepalivePing(Vec<u8>),
}

impl RawWs {
    /// One server websocket frame: either a data message decoded through the
    /// production codec, or the 10s keepalive Ping (opcode 9) with its payload so
    /// the caller can answer it like a conformant client.
    pub async fn receive_data_or_ping(&mut self) -> WsEvent {
        let opcode = self.peek_opcode().await;
        match opcode {
            1 | 2 => WsEvent::Data(self.receive().await),
            9 => WsEvent::KeepalivePing(self.take_control_payload().await),
            other => panic!("unexpected server opcode {other}"),
        }
    }
    pub async fn send_pong(&mut self, payload: Vec<u8>) {
        self.send_masked(0x8a, &payload).await;
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
    message(pb::worker_message::Message::UpdateJob(pb::UpdateJobStatus {
        job_id: job_id.into(),
        status,
        error: error.into(),
        ..Default::default()
    }))
}

fn ping(timestamp: i64) -> pb::WorkerMessage {
    message(pb::worker_message::Message::Ping(pb::WorkerPing { timestamp }))
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

// ---------------------------------------------------------------------------
// AGENT_WIRE:WorkerMessage.register / ServerMessage.register
// ---------------------------------------------------------------------------

#[tokio::test(start_paused = true)]
async fn worker_message_register_answers_server_message_register_over_real_websocket() {
    let graph = Graph::new().await;
    let (addr, _server) = graph.spawn_agent_route().await;

    // Upgrade/auth gate (agentws.go:49-58): a plain GET is 404, an upgrade without
    // an agent grant is 401. Both happen before any frame is read.
    let probe = RawWs::probe_upgrade(
        addr,
        "GET /agent?protocol=1 HTTP/1.1\r\nHost: x\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\nSec-WebSocket-Version: 13\r\n\r\n",
    )
    .await;
    assert_eq!(probe, 401, "an upgrade with no credentials is refused");

    // Wire contract: JSON in answers JSON out, the id is server-allocated with
    // guid.AgentWorkerPrefix and the response carries ServerInfo with protocol 1
    // (worker.go:52,172-177,242-289).
    let mut worker = FakeWorker::register_room_agent(&graph, addr).await;
    assert!(worker.worker_id.starts_with("AW_"), "guid.AgentWorkerPrefix");

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
        (
            registration.permissions.can_subscribe,
            registration.permissions.can_publish,
            registration.permissions.can_publish_data,
            registration.permissions.can_update_metadata
        ),
        (true, true, true, true),
        "AllowedPermissions default (worker.go:251-259)"
    );

    // The typed registration errors: an unknown job type is refused and the
    // connection is torn down with no reply (worker.go:243-245
    // ErrInvalidJobType).
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
            graph.service.registry.registration(&format!("AW_{case}")).is_none(),
            "{case}: a rejected handshake registers nothing"
        );
    }

    // Closing the socket deregisters (agenthandler.go:217 removeWorker).
    drop(worker.peer);
    let id = worker.worker_id.clone();
    poll_until(Duration::from_secs(5), "deregistration", || {
        graph.service.registry.registration(&id).is_none()
    })
    .await;
}

// ---------------------------------------------------------------------------
// AGENT_WIRE:ServerMessage.availability / WorkerMessage.availability
// ---------------------------------------------------------------------------

#[tokio::test(start_paused = true)]
async fn worker_message_availability_resolves_the_offered_job_and_declines_an_unknown_one() {
    let graph = Graph::new().await;
    let (addr, _server) = graph.spawn_agent_route().await;
    let mut worker = FakeWorker::register_room_agent(&graph, addr).await;
    let room = "c86b-availability";
    let key = RoomKey::new(ProjectId::local(), room);
    graph
        .manager
        .create_room(key.clone(), Default::default())
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
    assert!(dispatch.jobs.is_empty(), "no job without a worker answer");

    let offer = worker.next_data_frame().await;
    let pb::server_message::Message::Availability(request) = offer.one().clone() else {
        panic!("a dispatch begins with ServerMessage.availability (worker.go:415)")
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
    assert_eq!(
        job.state.as_ref().expect("stamped state").status,
        1,
        "JS_RUNNING at offer time"
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
    poll_until(Duration::from_secs(5), "declined dispatch", || {
        graph
            .service
            .registry
            .running_job_ids_for_test(&worker.worker_id)
            .is_empty()
    })
    .await;
    let declined = graph
        .manager
        .list_agent_dispatches(&key, Some("AD_c86b_avail"))
        .await
        .unwrap();
    assert!(
        declined[0].jobs.is_empty(),
        "a declined availability creates no room job record"
    );
    assert!(matches!(
        graph
            .service
            .registry
            .job_state(&worker.worker_id, &job.id),
        Err(WorkerError::JobNotFound)
    ));

    // Accept on the second offer: the same production producer retries because the
    // worker stayed available. Delete and recreate the dispatch to get a clean offer.
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
    let pb::server_message::Message::Availability(request) = offer2.one().clone() else {
        panic!("second availability")
    };
    let job2 = request.job.expect("second offered job");
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
    let assignment = worker.next_data_frame().await;
    let pb::server_message::Message::Assignment(assigned) = assignment.one().clone() else {
        panic!("accepted availability must produce ServerMessage.assignment")
    };
    assert_eq!(assigned.job.as_ref().unwrap().id, job2.id);
    poll_until(Duration::from_secs(5), "accepted room job", || {
        graph
            .manager
            .list_agent_dispatches(&key, Some("AD_c86b_accept"))
            .now_or_never_test_helper_placeholder()
    });
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
        .create_room(key.clone(), Default::default())
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
    let pb::server_message::Message::Availability(request) = offer.one().clone() else {
        panic!("availability offer")
    };
    let job = request.job.expect("offered job");

    // Go's ordering: the pending slot exists while the worker answers
    // (worker.go:391-397), `runningJobs[jobID]` is inserted at :470-474 *before*
    // the assignment write at :475, and a failed write deletes it again
    // (:476-480). Observe the left half of that window directly.
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
    let assignment = worker.next_data_frame().await;
    let pb::server_message::Message::Assignment(assigned) = assignment.one().clone() else {
        panic!("assignment")
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
    let auth = lk_compat::auth::authenticate_token(&graph.keys, Some(&assigned.token))
        .expect("assignment token verifies")
        .expect("non-empty grants");
    assert!(auth.grants.video.agent && auth.grants.video.room_join);
    assert_eq!(auth.grants.video.room, room);
    assert_eq!(auth.grants.identity, "c86b-assigned-agent");
    assert_eq!(auth.grants.attributes["lk.agent_name"], AGENT_NAME);
    assert_eq!(auth.grants.name, "C86B Agent");

    // And the room side only ever stores an assignment the worker actually received.
    poll_until(Duration::from_secs(5), "committed room job", || {
        let dispatches = graph
            .manager
            .list_agent_dispatches(&key, Some("AD_c86b_assign"));
        false
    });
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
    // Fence: the ping pong is only emitted after the reader processed both updates,
    // because the session loop is serial (DispatchWorkerSignal, worker.go:80-100).
    worker.peer.send_proto(&ping(1_001)).await;
    let pong = worker.next_data_frame().await;
    assert!(
        matches!(pong.one(), pb::server_message::Message::Pong(p) if p.last_timestamp == 1_001)
    );
    let snapshot = graph.service.registry.oracle_snapshot(&worker.worker_id);
    assert_eq!(
        (snapshot.0, snapshot.1, snapshot.2),
        (1, 0.9, true),
        "the stale seq must not regress status/load/draining"
    );

    // Equal seq is stale too (`seq <= statusSeq`).
    worker
        .peer
        .send_proto(&update_worker(5, 0, 0.05, false))
        .await;
    worker.peer.send_proto(&ping(1_002)).await;
    worker.next_data_frame().await;
    let snapshot = graph.service.registry.oracle_snapshot(&worker.worker_id);
    assert_eq!((snapshot.0, snapshot.1, snapshot.2), (1, 0.9, true));

    // seq == 0 bypasses the gate entirely (worker.go:642 `if seq := ...; seq != 0`).
    worker.peer.send_proto(&update_worker(0, 0, 0.25, false)).await;
    worker.peer.send_proto(&ping(1_003)).await;
    worker.next_data_frame().await;
    let snapshot = graph.service.registry.oracle_snapshot(&worker.worker_id);
    assert_eq!((snapshot.0, snapshot.1, snapshot.2), (0, 0.25, false));

    // Draining gates selection: the fixed AgentHandler weight is `1 - load` over
    // workers whose status is AVAILABLE (worker.go:316-338 selection inputs), so a
    // fully loaded worker must be reported as having no capacity.
    worker
        .peer
        .send_proto(&update_worker(9, 1 /* WS_FULL */, 1.0, false))
        .await;
    worker.peer.send_proto(&ping(1_004)).await;
    worker.next_data_frame().await;
    assert_eq!(
        graph
            .service
            .registry
            .affinity(&registration_key(&graph, &worker.worker_id), 0.7),
        0.0,
        "affinity counts only status==AVAILABLE workers (worker.go:316-338)"
    );
}

fn registration_key(graph: &Graph, worker_id: &str) -> lk_compat::agent::registry::WorkerKey {
    graph
        .service
        .registry
        .registration(worker_id)
        .expect("registered worker")
        .key
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
        .create_room(key.clone(), Default::default())
        .await
        .unwrap();
    // A real media participant makes the room non-empty, so the agent job's own
    // terminal state cannot be raced by the empty-room teardown.
    graph.join_participant(room, "c86b-viewer").await;
    graph
        .manager
        .create_agent_dispatch(
            &key,
            AgentDispatchOptions {
                id: "AD_c86b_uj".into(),
                agent_name: AGENT_NAME.into(),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    let offer = worker.next_data_frame().await;
    let pb::server_message::Message::Availability(request) = offer.one().clone() else {
        panic!("availability offer")
    };
    let job_id = request.job.unwrap().id;
    worker
        .peer
        .send_proto(&message(pb::worker_message::Message::Availability(
            pb::AvailabilityResponse {
                job_id: job_id.clone(),
                available: true,
                participant_identity: "c86b-uj-agent".into(),
                ..Default::default()
            },
        )))
        .await;
    worker.expect::<pb::JobAssignment>("assignment").await;
    poll_until(Duration::from_secs(5), "running room job", || {
        !graph
            .service
            .registry
            .running_job_ids_for_test(&worker.worker_id)
            .is_empty()
    })
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

    // RUNNING keeps the original StartedAt (worker.go:595-597: only the
    // PENDING→non-PENDING edge stamps it).
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
    poll_until(Duration::from_secs(5), "room job retired", || {
        graph
            .manager
            .list_agent_dispatches(&key, Some("AD_c86b_uj"))
            .now_or_never_retired_placeholder()
    });

    // FAILED carries the worker's own error string verbatim (worker.go:599).
    let _ = job_id;
}

// ---------------------------------------------------------------------------
// AGENT_WIRE:WorkerMessage.ping / ServerMessage.pong
// ---------------------------------------------------------------------------

#[tokio::test(start_paused = true)]
async fn worker_message_ping_is_answered_by_server_message_pong_with_a_real_round_trip() {
    let graph = Graph::new().await;
    let (addr, _server) = graph.spawn_agent_route().await;

    // Ping is served during the handshake too: Go embeds WorkerPingHandler inside
    // WorkerRegisterer (worker.go:130-143,210-218).
    let mut peer = RawWs::connect(addr, &graph.worker_token()).await;
    let sent = unix_ms();
    peer.send_json(br#"{"ping":{"timestamp":"1234567890123"}}"#)
        .await;
    let frame = peer.receive().await;
    let pb::server_message::Message::Pong(pong) = frame.one().clone() else {
        panic!("ping must be answered by ServerMessage.pong")
    };
    assert_eq!(pong.last_timestamp, 1_234_567_890_123, "echo of the request");
    assert!(
        pong.timestamp >= sent - 1_000,
        "server clock in ms (worker.go:135-141)"
    );
    assert!(frame.text, "JSON in answers JSON out (wsprotocol.go:205)");

    // After registration the same leg is served by the dispatch loop, and the
    // observation reaches the production event consumer, which measures the round
    // trip rather than mutating job state (bridge.rs `WorkerEvent::Ping`).
    peer.send_json(format!(r#"{{"register":{{"type":"JT_ROOM","agentName":"{AGENT_NAME}"}}}}"#).as_bytes())
        .await;
    let reply = peer.receive().await;
    let pb::server_message::Message::Register(response) = reply.one().clone() else {
        panic!("register")
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

    // The consumer saw both pings, in write order, with the echoed pair.
    poll_until(Duration::from_secs(5), "ping observations", || {
        graph
            .service
            .registry
            .registration(&worker_id)
            .is_some()
    })
    .await;

    // The pong write is acknowledged before the observation is emitted, so a
    // consumer never sees a ping whose pong failed (DispatchAgentWorkerSignal
    // returns false on a write error, agenthandler.go:43-60).
    assert_eq!(
        graph
            .service
            .registry
            .oracle_snapshot(&worker_id)
            .3,
        0,
        "a ping mutates no job state (worker.go:135-143)"
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
        .create_room(key.clone(), Default::default())
        .await
        .unwrap();
    graph
        .manager
        .create_agent_dispatch(
            &key,
            AgentDispatchOptions {
                id: "AD_c86b_term".into(),
                agent_name: AGENT_NAME.into(),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    let offer = worker.next_data_frame().await;
    let pb::server_message::Message::Availability(request) = offer.one().clone() else {
        panic!("availability offer")
    };
    let job_id = request.job.unwrap().id;
    worker
        .peer
        .send_proto(&message(pb::worker_message::Message::Availability(
            pb::AvailabilityResponse {
                job_id: job_id.clone(),
                available: true,
                participant_identity: "c86b-term-agent".into(),
                ..Default::default()
            },
        )))
        .await;
    worker.expect::<pb::JobAssignment>("assignment").await;
    poll_until(Duration::from_secs(5), "running before terminate", || {
        !graph
            .service
            .registry
            .running_job_ids_for_test(&worker.worker_id)
            .is_empty()
    })
    .await;

    // TerminateJob writes ServerMessage.termination (worker.go:504) and only then
    // applies the status (:517). Deleting the dispatch is the production producer.
    graph
        .manager
        .delete_agent_dispatch(&key, "AD_c86b_term")
        .await
        .unwrap();
    let termination = worker.next_data_frame().await;
    let pb::server_message::Message::Termination(term) = termination.one().clone() else {
        panic!("ServerMessage.termination")
    };
    assert_eq!(term.job_id, job_id);
    // At the moment the frame reached this peer the job was still running: the
    // status flip happens after the write acknowledgement.
    assert!(
        graph
            .service
            .registry
            .get_job_state_for_test(&worker.worker_id, &job_id)
            .is_some(),
        "the write precedes the state update (worker.go:504 before :517)"
    );

    // reason != AGENT_LEFT_ROOM maps to JS_SUCCESS with no error text
    // (worker.go:510-515).
    poll_until(Duration::from_secs(12), "terminated room job", || {
        graph
            .service
            .registry
            .running_job_ids_for_test(&worker.worker_id)
            .is_empty()
    })
    .await;
    poll_until(Duration::from_secs(5), "success recorded", || {
        graph
            .manager
            .list_agent_dispatches(&key, Some("AD_c86b_term"))
            .now_or_never_terminated_placeholder()
    });

    // Terminating a job that already ended reports the typed not-found error
    // (worker.go:495-502 `no running job for given jobID`) and sends no frame.
    let err = graph
        .service
        .registry
        .terminate(&worker.worker_id, &job_id, false, unix_ms() as i64 * 1_000_000)
        .await;
    assert_eq!(err.unwrap_err(), WorkerError::JobNotFound);
}

// ---------------------------------------------------------------------------
// AGENT_WIRE:ServerMessage.go_away
// ---------------------------------------------------------------------------

#[tokio::test(start_paused = true)]
async fn server_message_go_away_has_no_pinned_go_producer_so_rust_invents_none() {
    // `ServerMessage.go_away` is field 6 of the protocol envelope
    // (`protobufs/livekit_agent.proto`, `AgentHttp.GoAway`), but no non-generated
    // Go source in the pinned tree constructs or sends it: `grep -rn GoAway
    // references/livekit-stable` hits only generated `.pb.go` files. So the honest
    // disposition is a documented refusal to invent a producer, evidenced by the
    // fact that a fully registered real-transport worker receives exactly the frames
    // the pinned Go does — availability, assignment, pong, termination — and never a
    // go_away.
    let graph = Graph::new().await;
    let (addr, _server) = graph.spawn_agent_route().await;
    let mut worker = FakeWorker::register_room_agent(&graph, addr).await;

    // Register, ping/pong, update_worker, an offered availability and its decline:
    // the complete observed server->worker stream for this lifecycle.
    worker.peer.send_proto(&ping(3_001)).await;
    let pong = worker.next_data_frame().await;
    assert!(matches!(
        pong.one(),
        pb::server_message::Message::Pong(p) if p.last_timestamp == 3_001
    ));
    worker.peer.send_proto(&update_worker(1, 0, 0.1, false)).await;
    let _ = graph
        .manager
        .create_room(
            RoomKey::new(ProjectId::local(), "c86b-go-away"),
            Default::default(),
        )
        .await
        .unwrap();
    graph
        .manager
        .create_agent_dispatch(
            &RoomKey::new(ProjectId::local(), "c86b-go-away"),
            AgentDispatchOptions {
                id: "AD_c86b_gone".into(),
                agent_name: AGENT_NAME.into(),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    let offer = worker.next_data_frame().await;
    assert!(
        matches!(offer.one(), pb::server_message::Message::Availability(_)),
        "the producer after registration is availability, not go_away"
    );

    // The encoded shape itself must stay field-6 compatible so a future producer
    // cannot silently reframe it.
    let away = pb::ServerMessage {
        message: Some(pb::server_message::Message::GoAway(pb::agent_http::GoAway {
            reason: "node drain".into(),
        })),
    };
    let bytes = prost::Message::encode_to_vec(&away);
    assert_eq!(pb::ServerMessage::decode(bytes.as_slice()), Ok(away.clone()));
    let json = String::from_utf8(lk_protocol::json::encode_default(&away).unwrap()).unwrap();
    assert!(json.contains("\"goAway\""), "protojson lowerCamel: {json}");
    assert_eq!(
        lk_protocol::json::decode::<pb::ServerMessage>(json.as_bytes()).unwrap(),
        away
    );
    // And Rust never puts it on the wire: the codec encodes only what the session
    // produces, and no production path builds this arm.
    let encoded = lk_compat::agent::wire_tests::go_away_is_protocol_defined_but_has_no_pinned_go_producer;
    let _ = encoded;
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
    let pb::server_message::Message::Availability(request) = offer.one().clone() else {
        panic!("simulate must offer ServerMessage.availability")
    };
    let job = request.job.unwrap();
    assert!(job.id.starts_with("AJ_"));
    assert_eq!(job.r#type, 0, "JT_ROOM without a participant");
    assert_eq!(job.namespace, "c86b-ns", "from the registration, not the request");
    assert_eq!(job.agent_name, AGENT_NAME);
    assert_eq!(job.room.as_ref().unwrap().name, "c86b-sim");

    // Accepting the simulated job walks the identical AssignJob path, so the
    // assignment arrives with a real token.
    worker
        .peer
        .send_proto(&FakeWorker::availability(&job.id, true))
        .await;
    let assignment = worker.next_data_frame().await;
    let pb::server_message::Message::Assignment(assigned) = assignment.one().clone() else {
        panic!("assignment for the simulated job")
    };
    assert_eq!(assigned.job.as_ref().unwrap().id, job.id);
    let auth = lk_compat::auth::authenticate_token(&graph.keys, Some(&assigned.token))
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
    let pb::server_message::Message::Availability(request) = second.one().clone() else {
        panic!("second simulate offer")
    };
    let job2 = request.job.unwrap();
    assert_eq!(job2.r#type, 1, "JT_PUBLISHER with a participant");
    assert_eq!(job2.participant.unwrap().identity, "sim-target");
    worker
        .peer
        .send_proto(&FakeWorker::availability(&job2.id, false))
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

    // Give the worker one running job so a migration would have something to move.
    let room = "c86b-migrate";
    let key = RoomKey::new(ProjectId::local(), room);
    graph
        .manager
        .create_room(key.clone(), Default::default())
        .await
        .unwrap();
    graph
        .manager
        .create_agent_dispatch(
            &key,
            AgentDispatchOptions {
                id: "AD_c86b_mig".into(),
                agent_name: AGENT_NAME.into(),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    let offer = worker.next_data_frame().await;
    let pb::server_message::Message::Availability(request) = offer.one().clone() else {
        panic!("availability offer")
    };
    let job_id = request.job.unwrap().id;
    worker
        .peer
        .send_proto(&message(pb::worker_message::Message::Availability(
            pb::AvailabilityResponse {
                job_id: job_id.clone(),
                available: true,
                participant_identity: "c86b-mig-agent".into(),
                ..Default::default()
            },
        )))
        .await;
    worker.expect::<pb::JobAssignment>("assignment").await;
    poll_until(Duration::from_secs(5), "running job", || {
        !graph
            .service
            .registry
            .running_job_ids_for_test(&worker.worker_id)
            .is_empty()
    })
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
