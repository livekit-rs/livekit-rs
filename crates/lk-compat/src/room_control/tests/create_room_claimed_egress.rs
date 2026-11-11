//! Component fixture: actual public RoomService -> Redis claimed RoomControl
//! -> actual owner -> Redis claimed held Egress handler. No recording success is
//! manufactured. The before-expiry control returns a deliberate typed rejection.
use super::*;
use crate::{
    service_bus::{
        channels,
        rpc::{Cancellation, MethodSpec},
        wire::{self, WireLimits},
    },
    worker_services::{WorkerConfig, WorkerServices},
    workers::{
        ingress::IngressConfig,
        io_service::{IoEvent, IoEvents},
        retry::RpcPolicy,
    },
};
use lk_protocol::{psrpc::internal as bus, rpc as internal};
use std::sync::{
    Mutex as StdMutex,
    atomic::{AtomicUsize, Ordering},
};
use tokio::sync::Notify;

struct ClaimedEvents;
impl IoEvents for ClaimedEvents {
    fn notify(&self, _: IoEvent) {}
}
struct HeldGuard {
    active: Arc<AtomicUsize>,
    finished: Arc<Notify>,
}
impl Drop for HeldGuard {
    fn drop(&mut self) {
        self.active.fetch_sub(1, Ordering::SeqCst);
        self.finished.notify_one();
    }
}
#[derive(Clone)]
struct HandlerObservation {
    sent_at: i64,
    egress_id: String,
    room_id: String,
    deadline: std::time::Instant,
    expiry_ns: i64,
}

#[tokio::test]
#[ignore = "requires exclusive owned CLUSTER_CONTROL_REDIS_URL; real claimed owner and Egress handler"]
async fn public_room_create_claimed_owner_and_held_egress_keep_original_attempt_expiry() {
    for (short, terminal) in [(false, false), (true, false), (false, true)] {
        claimed_profile(short, terminal).await;
    }
}
async fn claimed_profile(short: bool, terminal: bool) {
    let url = std::env::var("CLUSTER_CONTROL_REDIS_URL").expect("dedicated owned broker");
    assert!(
        url.starts_with("redis://127.0.0.1:") && !url.contains('@'),
        "local credential-free isolated broker only"
    );
    let node = rtc_core::ids::new_sid("ND_");
    let (_, net) = state(&node).await;
    let loaded = super::create_room_policy::loaded(
        if short { 1 } else { 3 },
        if short { "100ms" } else { "10s" },
        &node,
    );
    assert_eq!(loaded.room.create_room_attempts, if short { 1 } else { 3 });
    assert_eq!(
        loaded.room.create_room_timeout.as_nanos_i64(),
        if short { 100_000_000 } else { 10_000_000_000 }
    );
    let mut app = crate::state::CompatServices::new(Arc::new(loaded))
        .assemble(net.clone(), Arc::new(NullSink));
    let services = WorkerServices::connect(
        WorkerConfig {
            redis: redis::IntoConnectionInfo::into_connection_info(url.clone())
                .unwrap()
                .into(),
            rpc: RpcPolicy::from_raw(0, 3_000_000_000, 2_000_000_000, 1000),
            compression: wire::CompressionPolicy::default(),
            max_decompressed_size: 1 << 20,
            ingress: IngressConfig::default(),
            ingress_legacy_psrpc: false,
        },
        app.manager.clone(),
        None,
        Arc::new(ClaimedEvents),
    )
    .await
    .unwrap();
    // Install before RoomControl captures owner state: no local branch bypass.
    app.worker_services = Some(services.clone());
    let control = app
        .enable_room_control_component(actual_bus(&url).await, config(&node, &url))
        .await
        .unwrap();
    let observer = actual_bus(&url).await;
    let limits = WireLimits {
        encoded_bytes: 1 << 20,
        decompressed_bytes: 1 << 20,
    };
    // The authoritative registered spec is shared with the production caller.
    let registered = create_spec(&node);
    let owner_channel = registered.request_channel();
    let worker_spec = MethodSpec {
        service: "EgressInternal".into(),
        method: "StartEgress".into(),
        topics: vec![String::new()],
        require_claim: true,
        queue: false,
    };
    let egress_channel = channels::request(&worker_spec.service, &worker_spec.method, &[""], false);
    let mut owner_rx = observer.subscribe(&owner_channel, false).await.unwrap();
    let mut egress_rx = observer.subscribe(&egress_channel, false).await.unwrap();
    let owner_rows = Arc::new(StdMutex::new(Vec::<bus::Request>::new()));
    let egress_rows = Arc::new(StdMutex::new(Vec::<bus::Request>::new()));
    let observer_stop = Cancellation::default();
    let owner_rows_task = owner_rows.clone();
    let stop = observer_stop.clone();
    let owner_observer = tokio::spawn(async move {
        loop {
            tokio::select! {biased;
                value=owner_rx.recv()=>{let raw=value.expect("actual owner observer transport");let row=wire::decode::<bus::Request>(&raw,limits).unwrap().message;owner_rows_task.lock().unwrap().push(row);},
                _=stop.cancelled()=>{
                    for raw in owner_rx.test_receive_barrier().await.unwrap(){let row=wire::decode::<bus::Request>(&raw,limits).unwrap().message;owner_rows_task.lock().unwrap().push(row);}
                    break;
                },
            }
        }
        owner_rx
    });
    let egress_rows_task = egress_rows.clone();
    let stop = observer_stop.clone();
    let worker_observer = tokio::spawn(async move {
        loop {
            tokio::select! {biased;
                value=egress_rx.recv()=>{let raw=value.expect("actual worker observer transport");let row=wire::decode::<bus::Request>(&raw,limits).unwrap().message;egress_rows_task.lock().unwrap().push(row);},
                _=stop.cancelled()=>{
                    for raw in egress_rx.test_receive_barrier().await.unwrap(){let row=wire::decode::<bus::Request>(&raw,limits).unwrap().message;egress_rows_task.lock().unwrap().push(row);}
                    break;
                },
            }
        }
        egress_rx
    });
    let entered = Arc::new(Notify::new());
    let active = Arc::new(AtomicUsize::new(0));
    let finished = Arc::new(Notify::new());
    let observed = Arc::new(StdMutex::new(Vec::<HandlerObservation>::new()));
    let release = Arc::new(Notify::new());
    let held = RpcServer::register::<internal::StartEgressRequest, pb::EgressInfo, _, _>(
        actual_bus(&url).await,
        worker_spec,
        ServerLimits {
            concurrent: 16,
            max_request_duration: Duration::MAX,
            allow_skip_claim: false,
        },
        {
            let entered = entered.clone();
            let active = active.clone();
            let finished = finished.clone();
            let observed = observed.clone();
            let release = release.clone();
            move |context, request| {
                let entered = entered.clone();
                let active = active.clone();
                let finished = finished.clone();
                let observed = observed.clone();
                let release = release.clone();
                async move {
                    active.fetch_add(1, Ordering::SeqCst);
                    let _owned = HeldGuard { active, finished };
                    observed.lock().unwrap().push(HandlerObservation {
                        sent_at: context.sent_at_ns,
                        egress_id: request.egress_id,
                        room_id: request.room_id,
                        deadline: context.deadline,
                        expiry_ns: context.expiry_ns,
                    });
                    entered.notify_one();
                    if terminal {
                        release.notified().await;
                        Err(RpcError::new(
                            "invalid_argument",
                            "owned claimed worker deliberate rejection",
                        ))
                    } else {
                        std::future::pending::<Result<pb::EgressInfo, RpcError>>().await
                    }
                }
            }
        },
        Some(Arc::new(|_| 1.0)),
    )
    .await
    .unwrap();
    let room = rtc_core::ids::new_sid("claimed-egress-");
    let key = app.room_key(&room);
    let request = pb::CreateRoomRequest {
        name: room.clone(),
        node_id: node,
        egress: Some(pb::RoomEgress {
            room: Some(pb::RoomCompositeEgressRequest {
                audio_only: true,
                file_outputs: vec![pb::EncodedFileOutput {
                    filepath: "claimed-held.ogg".into(),
                    ..Default::default()
                }],
                ..Default::default()
            }),
            ..Default::default()
        }),
        ..Default::default()
    };
    let mut auth = VerifiedToken::default();
    auth.grants.video.room_create = true;
    let app_call = app.clone();
    let task = tokio::spawn(async move {
        crate::twirp::room_service::call(
            &app_call,
            Some(&auth),
            "CreateRoom",
            Codec::Protobuf,
            &request.encode_to_vec(),
        )
        .await
    });
    // A cold 100 ms request can expire during owner/worker subscription setup.
    // Fixed Go passes the original deadline through all these steps; it does
    // not guarantee that expired work ever reaches the claimed handler.
    let entered_handler = tokio::time::timeout(Duration::from_secs(2), entered.notified())
        .await
        .is_ok();
    if !short {
        assert!(
            entered_handler,
            "default budget must complete real claim/grant"
        );
    }
    let sid = app
        .manager
        .list_rooms(&key.project, std::slice::from_ref(&key.name))[0]
        .sid
        .clone();
    assert!(app.manager.room_exists(&key));
    if terminal {
        release.notify_one();
    }
    let result = tokio::time::timeout(Duration::from_secs(15), task)
        .await
        .unwrap()
        .unwrap();
    tokio::time::timeout(Duration::from_secs(2), async {
        while active.load(Ordering::SeqCst) != 0 {
            finished.notified().await;
        }
    })
    .await
    .expect("handler ownership expires without worker shutdown");
    assert_eq!(control.client.pending_count(), 0);
    // Real pending work has finished. These PINGs use the actual original
    // owner-request and StartEgress producer connections, not observer commands.
    control.client.test_command_barrier().await.unwrap();
    assert_eq!(services.test_automatic_start_barrier().await.unwrap(), 0);
    observer_stop.cancel();
    let mut owner_rx = owner_observer.await.unwrap();
    let mut egress_rx = worker_observer.await.unwrap();
    // Each recorder used its own RESP3 subscriber PING, decoding and retaining
    // every returned queued envelope before yielding its subscription here.
    let (owner_quiet, worker_quiet) = tokio::join!(
        tokio::time::timeout(Duration::from_millis(50), owner_rx.recv()),
        tokio::time::timeout(Duration::from_millis(50), egress_rx.recv())
    );
    assert!(
        owner_quiet.is_err(),
        "no owner publication after real producer/subscriber barriers"
    );
    assert!(
        worker_quiet.is_err(),
        "no worker publication after real producer/subscriber barriers"
    );
    control.client.test_command_barrier().await.unwrap();
    assert_eq!(services.test_automatic_start_barrier().await.unwrap(), 0);
    let owner_tail = owner_rx.test_receive_barrier().await.unwrap();
    let owner_tail_count = owner_tail.len();
    for raw in owner_tail {
        owner_rows
            .lock()
            .unwrap()
            .push(wire::decode::<bus::Request>(&raw, limits).unwrap().message);
    }
    let worker_tail = egress_rx.test_receive_barrier().await.unwrap();
    let worker_tail_count = worker_tail.len();
    for raw in worker_tail {
        egress_rows
            .lock()
            .unwrap()
            .push(wire::decode::<bus::Request>(&raw, limits).unwrap().message);
    }
    assert_eq!(
        owner_tail_count, 0,
        "second ordered owner barrier has no unrecorded publication"
    );
    assert_eq!(
        worker_tail_count, 0,
        "second ordered worker barrier has no unrecorded publication"
    );
    owner_rx.unsubscribe(&owner_channel).await.unwrap();
    egress_rx.unsubscribe(&egress_channel).await.unwrap();
    let owners = owner_rows.lock().unwrap().clone();
    let workers = egress_rows.lock().unwrap().clone();
    let handlers = observed.lock().unwrap().clone();
    held.shutdown().await;
    // Complete all owned teardown before assertions over captured outcomes.
    assert!(app.manager.room_exists(&key));
    let retained = app
        .manager
        .list_rooms(&key.project, std::slice::from_ref(&key.name))[0]
        .sid
        .clone();
    // Component factory does not launch the executable heartbeat loop. Drive
    // its real public heartbeat before post-10s routing, exactly as source does.
    let mut stats = control
        .node
        .lock()
        .await
        .message
        .stats
        .clone()
        .expect("real serving-node Stats");
    stats.updated_at = unix_now();
    control.heartbeat(stats).await.unwrap();
    control
        .delete_room(pb::DeleteRoomRequest { room: room.clone() })
        .await
        .unwrap();
    control.shutdown().await.unwrap();
    app.manager.close_all().await;
    app.shutdown_workers().await;
    app.agents().shutdown().await;
    net.shutdown_and_wait().await;
    assert_eq!(
        retained, sid,
        "request expiry/worker rejection is never DeleteRoom"
    );
    assert!(
        !owners.is_empty(),
        "actual owner RPC wire is mandatory, not local shortcut"
    );
    assert!(owners.len() <= if short { 1 } else { 3 });
    if !short {
        assert!(!workers.is_empty());
        assert!(!handlers.is_empty());
    }
    assert_eq!(
        entered_handler,
        !handlers.is_empty(),
        "recorded actual handler entry"
    );
    let first_owner = &owners[0];
    // Bounded safe witness precedes every expiry assertion, so failures retain
    // actual nanosecond differences without identities, raw messages or secrets.
    eprintln!(
        "CreateRoom claimed expiry witness {}",
        serde_json::json!({
            "profile": if short { "short100ms" } else if terminal { "terminal10s" } else { "default10s" },
            "owner_count": owners.len(), "worker_count": workers.len(),
            "first_owner_sent_ns": first_owner.sent_at,
            "first_owner_expiry_ns": first_owner.expiry,
            "worker_rows": workers.iter().take(64).enumerate().map(|(ordinal, row)| serde_json::json!({
                "ordinal": ordinal, "sent_ns": row.sent_at, "expiry_ns": row.expiry,
                "expiry_minus_original_ns": row.expiry.saturating_sub(first_owner.expiry),
                "sent_minus_original_expiry_ns": row.sent_at.saturating_sub(first_owner.expiry)
            })).collect::<Vec<_>>(),
            "witness_truncated": workers.len() > 64
        })
    );
    let owner_budget = first_owner.expiry - first_owner.sent_at;
    assert!(owner_budget > 0 && owner_budget <= if short { 100_000_000 } else { 10_000_000_000 });
    let first_id = handlers.first().map(|h| h.egress_id.clone()).or_else(|| {
        workers.first().map(|row| {
            lk_protocol::wire::decode::<internal::StartEgressRequest>(&row.raw_request)
                .unwrap()
                .egress_id
        })
    });
    if let Some(id) = &first_id {
        assert!(!id.is_empty());
    }
    for row in &workers {
        assert!(
            !row.skip_claim,
            "fixture requires real claim/grant, never skip-claim handling"
        );
        assert!(
            row.sent_at < first_owner.expiry,
            "no actual late StartEgress publication"
        );
        assert!(
            row.expiry > row.sent_at && row.expiry <= first_owner.expiry,
            "nested request retains original owner absolute expiry"
        );
        let start =
            lk_protocol::wire::decode::<internal::StartEgressRequest>(&row.raw_request).unwrap();
        assert_eq!(
            Some(&start.egress_id),
            first_id.as_ref(),
            "same EgressID across real legal retries"
        );
        assert_eq!(start.room_id, sid.0);
    }
    for h in &handlers {
        assert!(h.expiry_ns <= first_owner.expiry);
        assert!(
            workers.iter().any(|row| row.expiry == h.expiry_ns),
            "accepted worker context retains the exact observed wire expiry"
        );
        assert_eq!(Some(&h.egress_id), first_id.as_ref());
        assert_eq!(h.room_id, sid.0);
        assert!(
            workers.iter().any(|r| r.sent_at == h.sent_at),
            "handler belongs to observed actual worker request"
        );
    }
    if terminal {
        let e = result.expect_err("actual claimed deliberate rejection must propagate");
        assert_eq!(e.code.as_str(), "invalid_argument");
        assert_eq!(e.msg, "owned claimed worker deliberate rejection");
        assert_eq!(handlers.len(), 1);
    } else if short {
        assert_eq!(result.unwrap_err().code.as_str(), "deadline_exceeded");
    } else {
        let room: pb::Room = response(result.unwrap()).await;
        assert_eq!(room.sid, sid.0, "retry sees same actually registered owner");
        assert!(owners.len() >= 2);
    }
    if !terminal {
        assert!(
            handlers
                .iter()
                .all(|h| h.deadline <= std::time::Instant::now()),
            "held requests ended on their real expiry before shutdown"
        );
    }
}

#[tokio::test]
#[ignore = "requires exclusive owned CLUSTER_CONTROL_REDIS_URL; real claimed CreateRoom and Egress launch"]
async fn catalog_existing_room_rehydrates_without_relaunching_create_only_composite() {
    let url = std::env::var("CLUSTER_CONTROL_REDIS_URL").expect("dedicated owned broker");
    assert!(url.starts_with("redis://127.0.0.1:") && !url.contains('@'));
    let node = rtc_core::ids::new_sid("ND_");
    let (_, net) = state(&node).await;
    let mut app = crate::state::CompatServices::new(Arc::new(super::create_room_policy::loaded(
        1, "10s", &node,
    )))
    .assemble(net.clone(), Arc::new(NullSink));
    let services = WorkerServices::connect(
        WorkerConfig {
            redis: redis::IntoConnectionInfo::into_connection_info(url.clone())
                .unwrap()
                .into(),
            rpc: RpcPolicy::from_raw(1, 3_000_000_000, 0, 1000),
            compression: wire::CompressionPolicy::default(),
            max_decompressed_size: 1 << 20,
            ingress: IngressConfig::default(),
            ingress_legacy_psrpc: false,
        },
        app.manager.clone(),
        None,
        Arc::new(ClaimedEvents),
    )
    .await
    .unwrap();
    app.worker_services = Some(services);
    let control = app
        .enable_room_control_component(actual_bus(&url).await, config(&node, &url))
        .await
        .unwrap();
    let retained_name = rtc_core::ids::new_sid("retained-composite-");
    let (stored, _) = control
        .allocator
        .create(pb::CreateRoomRequest {
            name: retained_name.clone(),
            ..Default::default()
        })
        .await
        .unwrap();
    let retained_sid = stored.room.message.sid.clone();
    assert!(!app.manager.room_exists(&app.room_key(&retained_name)));
    assert_eq!(
        control
            .allocator
            .catalog
            .load(&retained_name)
            .await
            .unwrap()
            .room
            .message
            .sid,
        retained_sid,
    );

    let launches = Arc::new(StdMutex::new(Vec::new()));
    let observed = launches.clone();
    let worker = RpcServer::register::<internal::StartEgressRequest, pb::EgressInfo, _, _>(
        actual_bus(&url).await,
        MethodSpec {
            service: "EgressInternal".into(),
            method: "StartEgress".into(),
            topics: vec![String::new()],
            require_claim: true,
            queue: false,
        },
        ServerLimits {
            concurrent: 4,
            max_request_duration: Duration::MAX,
            allow_skip_claim: false,
        },
        move |_, request| {
            let observed = observed.clone();
            async move {
                observed.lock().unwrap().push(request);
                // Capture real claimed admission without pretending to record.
                Err(RpcError::new(
                    "invalid_argument",
                    "fixture worker observed actual composite launch",
                ))
            }
        },
        Some(Arc::new(|_| 1.0)),
    )
    .await
    .unwrap();

    async fn public_create(
        app: &AppState,
        node: &str,
        name: &str,
    ) -> Result<Response, crate::twirp::error::TwirpError> {
        let mut auth = VerifiedToken::default();
        auth.grants.video.room_create = true;
        let request = pb::CreateRoomRequest {
            name: name.to_owned(),
            node_id: node.to_owned(),
            egress: Some(pb::RoomEgress {
                room: Some(pb::RoomCompositeEgressRequest {
                    room_name: "must-be-replaced-by-real-room-name".into(),
                    audio_only: true,
                    file_outputs: vec![pb::EncodedFileOutput {
                        filepath: "component-only.ogg".into(),
                        ..Default::default()
                    }],
                    ..Default::default()
                }),
                ..Default::default()
            }),
            ..Default::default()
        };
        crate::twirp::room_service::call(
            app,
            Some(&auth),
            "CreateRoom",
            Codec::Protobuf,
            &request.encode_to_vec(),
        )
        .await
    }

    let rehydrated = public_create(&app, &node, &retained_name).await;
    let fresh_name = rtc_core::ids::new_sid("fresh-composite-");
    let fresh = public_create(&app, &node, &fresh_name).await;
    let fresh_sid = control
        .allocator
        .catalog
        .load(&fresh_name)
        .await
        .unwrap()
        .room
        .message
        .sid;
    assert!(app.manager.room_exists(&app.room_key(&retained_name)));
    assert!(app.manager.room_exists(&app.room_key(&fresh_name)));
    worker.shutdown().await;
    let mut stats = control.node.lock().await.message.stats.clone().unwrap();
    stats.updated_at = unix_now();
    control.heartbeat(stats).await.unwrap();
    for room in [retained_name.clone(), fresh_name.clone()] {
        control
            .delete_room(pb::DeleteRoomRequest { room })
            .await
            .unwrap();
    }
    control.shutdown().await.unwrap();
    app.manager.close_all().await;
    app.shutdown_workers().await;
    app.agents().shutdown().await;
    net.shutdown_and_wait().await;

    let launches = launches.lock().unwrap();
    assert!(
        !launches
            .iter()
            .any(|request| request.room_id == retained_sid),
        "existing catalog RoomSID relaunched create-only RoomComposite on a new owner actor",
    );
    assert_eq!(launches.len(), 1);
    assert_eq!(launches[0].room_id, fresh_sid);
    let Some(internal::start_egress_request::Request::RoomComposite(request)) =
        launches[0].request.as_ref()
    else {
        panic!("fresh allocation did not send actual RoomComposite");
    };
    assert_eq!(request.room_name, fresh_name);
    assert!(request.audio_only);
    assert_eq!(
        fresh.unwrap_err().msg,
        "fixture worker observed actual composite launch"
    );
    let retained: pb::Room = response(rehydrated.unwrap()).await;
    assert_eq!(retained.sid, retained_sid);
}
