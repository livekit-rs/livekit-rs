use super::*;
use crate::{
    agent::registry::{Limits, Outbound, Registration},
    auth::KeyStore,
    service_bus::{
        local_transport::LocalTransport,
        redis_transport::TransportLimits,
        wire::{CompressionPolicy, WireLimits},
    },
};
use tokio::sync::mpsc;
fn bus() -> Arc<LocalTransport> {
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
fn keys() -> Arc<KeyStore> {
    Arc::new(KeyStore::new(vec![(
        "key".into(),
        "public-cluster-worker-test-secret-0123456789".into(),
    )]))
}
async fn node(bus: impl Into<RpcBus>) -> Arc<Cluster> {
    Cluster::start(
        Registry::new(Limits::default()),
        Arc::new(TokenMinter::new(keys())),
        0.7,
        bus.into(),
    )
    .await
    .unwrap()
}
fn request(name: &str, namespace: &str, deployment: &str) -> pb::RegisterWorkerRequest {
    pb::RegisterWorkerRequest {
        agent_name: name.into(),
        namespace: Some(namespace.into()),
        deployment: deployment.into(),
        ..Default::default()
    }
}
async fn worker(
    registry: Registry,
    id: &str,
    req: pb::RegisterWorkerRequest,
    load: f32,
    keys: Arc<KeyStore>,
) -> (
    mpsc::UnboundedReceiver<pb::ServerMessage>,
    tokio::task::JoinHandle<()>,
) {
    let mut registration = Registration::from_request(id.into(), 1, &req).unwrap();
    registration.api_key = keys.first().unwrap().0.to_string();
    let (out, mut outgoing) = mpsc::channel::<Outbound>(16);
    let (events, receiver) = mpsc::unbounded_channel();
    let owner = registry.clone();
    let worker_id = id.to_string();
    let task = tokio::spawn(async move {
        while let Some(out) = outgoing.recv().await {
            let value = out.message;
            let _ = out.written.send(Ok(()));
            if let Some(pb::server_message::Message::Availability(offer)) = &value.message {
                let job = offer.job.as_ref().unwrap();
                owner
                    .availability(
                        &worker_id,
                        pb::AvailabilityResponse {
                            job_id: job.id.clone(),
                            available: true,
                            participant_identity: format!("agent-{worker_id}"),
                            ..Default::default()
                        },
                    )
                    .unwrap();
            }
            if let Some(pb::server_message::Message::Assignment(assignment)) = &value.message {
                let grants = crate::auth::authenticate_token(&keys, Some(&assignment.token))
                    .unwrap()
                    .unwrap();
                assert!(grants.grants.video.agent);
                assert_eq!(
                    grants.grants.video.room,
                    assignment.job.as_ref().unwrap().room.as_ref().unwrap().name
                );
            }
            let _ = events.send(value);
        }
    });
    registry.register_ready(registration, out).await.unwrap();
    registry
        .update_worker(
            id,
            &pb::UpdateWorkerStatus {
                load,
                status: Some(0),
                ..Default::default()
            },
        )
        .unwrap();
    (receiver, task)
}
fn job(id: &str, agent: &str, namespace: &str, deployment: &str) -> pb::Job {
    pb::Job {
        id: id.into(),
        agent_name: agent.into(),
        namespace: namespace.into(),
        deployment: deployment.into(),
        room: Some(pb::Room {
            name: format!("room-{id}"),
            sid: format!("RM_{id}"),
            ..Default::default()
        }),
        ..Default::default()
    }
}
#[test]
fn generated_job_terminate_requires_claim_even_without_affinity_function() {
    // protocol/rpc/agent.psrpc.go RegisterMethod order is
    // affinityEnabled,multi,requireClaim,queue (psrpc/pkg/info/info.go).
    let terminate = spec("JobTerminate", vec!["AJ_real".into()]);
    assert!(terminate.require_claim);
    assert!(terminate.queue);
    assert_eq!(terminate.topics, ["AJ_real"]);
    let request = spec("JobRequest", vec!["agent_ns".into(), "room".into()]);
    assert!(request.require_claim);
    assert!(!request.queue);
    assert!(!spec("CheckEnabled", vec![]).require_claim);
}

#[test]
fn official_topics_and_affinity_use_target_load_without_drain_or_hard_local_cutoff() {
    assert_eq!(topic("", "legacy", ""), "legacy");
    assert_eq!(topic("agent", "", "prod"), "agent_prod");
    assert_eq!(topic("agent", "ns", "prod"), "agent_ns_prod");
    let registry = Registry::new(Limits::default());
    let key = WorkerKey {
        agent_name: "a".into(),
        namespace: "n".into(),
        job_type: 0,
        deployment: "d".into(),
    };
    for (id, load, status, draining) in [
        ("w1", 0.1, 0, false),
        ("w2", 0.6, 0, true),
        ("w3", 0.2, 1, false),
        ("w4", 0.9, 0, false),
    ] {
        let mut r = Registration::from_request(id.into(), 1, &request("a", "n", "d")).unwrap();
        r.api_key = "key".into();
        let (tx, _) = mpsc::channel(1);
        registry.register(r, tx).unwrap();
        registry
            .update_worker(
                id,
                &pb::UpdateWorkerStatus {
                    load,
                    status: Some(status),
                    draining,
                    ..Default::default()
                },
            )
            .unwrap();
    }
    assert!((registry.affinity(&key, 0.7) - 0.7).abs() < 0.00001);
    registry
        .update_worker(
            "w1",
            &pb::UpdateWorkerStatus {
                load: f32::NAN,
                ..Default::default()
            },
        )
        .unwrap();
    assert!(registry.affinity(&key, 0.7).is_nan());
}
#[tokio::test]
async fn actual_multi_discovers_other_node_and_claim_affinity_assigns_then_routes_termination() {
    run_multi_affinity(bus().into(), &rtc_core::ids::new_sid("case")).await;
}
async fn run_multi_affinity(bus: RpcBus, prefix: &str) {
    eprintln!("agent Redis fixture affinity isolation={prefix}");
    let agent = format!("agent_{prefix}");
    let namespace = format!("ns_{prefix}");
    let deployment = format!("dep-{prefix}");
    let high_id = format!("AW_{prefix}_high");
    let low_id = format!("AW_{prefix}_low");
    let job_id = format!("AJ_{prefix}");
    let caller = node(bus.clone()).await;
    let high = node(bus.clone()).await;
    let low = node(bus).await;
    let (mut high_events, high_task) = worker(
        high.registry.clone(),
        &high_id,
        request(&agent, &namespace, &deployment),
        0.6,
        keys(),
    )
    .await;
    let (mut low_events, low_task) = worker(
        low.registry.clone(),
        &low_id,
        request(&agent, &namespace, &deployment),
        0.1,
        keys(),
    )
    .await;
    let mut groups = caller.discover(agent.clone(), 0, deployment.clone());
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let group = groups.recv().await.expect("discovered worker namespace");
            if group.namespace == namespace {
                break;
            }
        }
    })
    .await
    .unwrap();
    let state = caller
        .request(&job(&job_id, &agent, &namespace, &deployment))
        .await
        .unwrap();
    assert_eq!(state.worker_id, low_id);
    assert_eq!(state.status, 1);
    assert!(
        high_events.try_recv().is_err(),
        "a losing claim must not reach worker availability or assignment"
    );
    let offer = low_events.recv().await.unwrap();
    assert!(matches!(
        offer.message,
        Some(pb::server_message::Message::Availability(_))
    ));
    let assignment = low_events.recv().await.unwrap();
    assert!(matches!(
        assignment.message,
        Some(pb::server_message::Message::Assignment(_))
    ));
    let stopped = caller.terminate(&job_id, false).await.unwrap();
    assert_eq!(stopped.status, 2);
    assert!(low.registry.job_state(&low_id, &job_id).is_err());
    assert!(
        matches!(low_events.recv().await.unwrap().message,Some(pb::server_message::Message::Termination(t)) if t.job_id==job_id)
    );
    drop(groups);
    caller.shutdown().await;
    high.registry.close_all();
    low.registry.close_all();
    high.shutdown().await;
    low.shutdown().await;
    high_task.await.unwrap();
    low_task.await.unwrap();
}
#[tokio::test]
async fn public_room_dispatch_uses_remote_worker_and_delete_reaches_its_actual_owner() {
    run_public_dispatch(bus().into(), &rtc_core::ids::new_sid("case")).await;
}
async fn run_public_dispatch(bus: RpcBus, prefix: &str) {
    eprintln!("agent Redis fixture public-dispatch isolation={prefix}");
    let agent = format!("remote_agent_{prefix}");
    let worker_id = format!("AW_{prefix}_remote");
    let dispatch_id = format!("AD_{prefix}");
    let room_name = format!("remote-agent-room-{prefix}");
    let (origin, net_a) = crate::test_support::state().await;
    let (worker_node, net_b) = crate::test_support::state().await;
    origin
        .agents()
        .enable_cluster_rpc(bus.clone())
        .await
        .unwrap();
    worker_node.agents().enable_cluster_rpc(bus).await.unwrap();
    let (mut events, actor) = worker(
        worker_node.agents().registry.clone(),
        &worker_id,
        request(&agent, "", ""),
        0.,
        worker_node.keys.clone(),
    )
    .await;
    let key = rtc_core::ids::RoomKey::new(rtc_core::ids::ProjectId::local(), room_name.as_str());
    origin
        .manager()
        .create_room(key.clone(), Default::default())
        .await
        .unwrap();
    let dispatch = origin
        .manager()
        .create_agent_dispatch(
            &key,
            rtc_runtime::api::AgentDispatchOptions {
                id: dispatch_id.clone(),
                agent_name: agent.clone(),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    assert!(dispatch.jobs.is_empty());
    let availability = tokio::time::timeout(Duration::from_secs(3), events.recv())
        .await
        .unwrap()
        .unwrap();
    assert!(matches!(
        availability.message,
        Some(pb::server_message::Message::Availability(_))
    ));
    let assignment = events.recv().await.unwrap();
    let job = match assignment.message {
        Some(pb::server_message::Message::Assignment(a)) => a.job.unwrap(),
        _ => panic!("real assignment required"),
    };
    tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            let jobs = origin
                .manager()
                .list_agent_dispatches(&key, Some(&dispatch_id))
                .await
                .unwrap();
            if jobs[0].jobs.len() == 1 {
                assert_eq!(jobs[0].jobs[0].id, job.id);
                assert_eq!(jobs[0].jobs[0].state.worker_id, worker_id);
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert!(
        origin.agents().registry.registration(&worker_id).is_none(),
        "must not turn the remote worker into a fake local registration"
    );
    // This profile measures completed-launch remote owner routing, not Delete
    // during an unfinished CheckEnabled fanout. Observe the REAL discovery's
    // owned task completion (after call.recv() closes and its channel drops),
    // rather than sleeping or widening the original termination deadline.
    // The separate late-availability test covers deletion during discovery.
    let discovery = origin.agents().registry.cluster().unwrap();
    tokio::time::timeout(Duration::from_secs(6), async {
        loop {
            // Use the real owned discovery JoinSet rather than cache presence:
            // WorkerRegistered can legitimately invalidate a completed cache.
            // reap() only consumes completed production tasks; it neither
            // cancels nor synthesizes a completion for an in-flight task.
            discovery.reap();
            let completed = discovery.jobs.lock().unwrap().is_empty();
            if completed {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("actual source five-second CheckEnabled discovery must finish before completed-launch owner-routing profile deletes");
    let _session = crate::agent::service::tests::join_agent(
        origin.manager(),
        &key,
        &format!("agent-{worker_id}"),
    )
    .await;
    origin
        .manager()
        .delete_agent_dispatch(&key, &dispatch_id)
        .await
        .unwrap();
    let terminated = tokio::time::timeout(Duration::from_secs(3), events.recv())
        .await
        .unwrap()
        .unwrap();
    assert!(
        matches!(terminated.message,Some(pb::server_message::Message::Termination(t)) if t.job_id==job.id)
    );
    origin.manager().delete_room(&key).await.unwrap();
    origin.agents().shutdown().await;
    worker_node.agents().shutdown().await;
    actor.await.unwrap();
    net_a.shutdown();
    net_b.shutdown();
}
#[tokio::test]
async fn worker_registered_invalidates_finished_discovery_cache_and_retired_group_is_removed() {
    let bus = bus();
    let caller = node(bus.clone()).await;
    let owner = node(bus).await;
    let first_epoch = caller.cache_epoch.load(Ordering::Acquire);
    let (_events, first) = worker(
        owner.registry.clone(),
        "AW_one",
        request("agent", "one", ""),
        0.,
        keys(),
    )
    .await;
    tokio::time::timeout(Duration::from_secs(2), async {
        while caller.cache_epoch.load(Ordering::Acquire) == first_epoch {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("initial actual WorkerRegistered received before filling cache");
    let mut first_discovery = caller.discover("agent".into(), 0, "".into());
    let mut seen = BTreeSet::new();
    while let Some(key) = first_discovery.recv().await {
        seen.insert(key.namespace);
    }
    assert_eq!(seen, BTreeSet::from(["one".to_string()]));
    assert!(caller.cache.lock().unwrap().is_some());
    let epoch = caller.cache_epoch.load(Ordering::Acquire);
    let (_events, second) = worker(
        owner.registry.clone(),
        "AW_two",
        request("agent", "two", ""),
        0.,
        keys(),
    )
    .await;
    tokio::time::timeout(Duration::from_secs(2), async {
        while caller.cache_epoch.load(Ordering::Acquire) == epoch {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    let mut next = caller.discover("agent".into(), 0, "".into());
    let mut seen = BTreeSet::new();
    while seen.len() < 2 {
        seen.insert(
            tokio::time::timeout(Duration::from_secs(2), next.recv())
                .await
                .unwrap()
                .unwrap()
                .namespace,
        );
    }
    assert_eq!(seen, BTreeSet::from(["one".to_string(), "two".to_string()]));
    owner.registry.disconnect("AW_one");
    owner.reconcile().await.unwrap();
    assert!(!owner.servers.lock().await.contains_key(&Key {
        method: "JobRequest",
        topics: vec!["agent_one".into(), "room".into()]
    }));
    drop(next);
    caller.shutdown().await;
    owner.registry.close_all();
    owner.shutdown().await;
    first.await.unwrap();
    second.await.unwrap();
}

async fn owned_redis(variable: &str, cluster: bool) -> RpcBus {
    let configured = std::env::var(variable).unwrap_or_else(|_| {
        panic!("{variable} must point to a coordinator-owned isolated Redis deployment")
    });
    let urls: Vec<_> = configured.split(',').filter(|u| !u.is_empty()).collect();
    assert!(
        !urls.is_empty(),
        "owned Redis configuration cannot be empty"
    );
    if !cluster {
        assert_eq!(urls.len(), 1, "single fixture takes one endpoint");
    }
    let nodes = urls
        .into_iter()
        .map(|url| {
            redis::Client::open(url)
                .unwrap_or_else(|_| panic!("invalid coordinator-owned Redis URL"))
                .get_connection_info()
                .clone()
        })
        .collect::<Vec<_>>();
    let endpoint = if cluster {
        crate::redis_endpoint::RedisEndpoint::cluster(nodes, 16)
    } else {
        crate::redis_endpoint::RedisEndpoint::simple(nodes.into_iter().next().unwrap())
    };
    let limits = TransportLimits {
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
    };
    let transport =
        crate::service_bus::redis_transport::RedisTransport::connect_endpoint(endpoint, limits)
            .await
            .expect("connect actual owned Redis RPC transport");
    RpcBus::Redis(transport)
}

#[tokio::test]
#[ignore = "requires LIVEKIT_AGENT_REDIS_SINGLE_URL; actual Redis AgentInternal/room dispatch"]
async fn actual_single_redis_agent_affinity_assignment_and_public_dispatch() {
    let bus = owned_redis("LIVEKIT_AGENT_REDIS_SINGLE_URL", false).await;
    let isolation = rtc_core::ids::new_sid("single");
    run_multi_affinity(bus.clone(), &isolation).await;
    run_public_dispatch(bus, &isolation).await;
}
#[tokio::test]
#[ignore = "requires LIVEKIT_AGENT_REDIS_CLUSTER_URLS; actual Redis Cluster AgentInternal/room dispatch"]
async fn actual_cluster_redis_agent_affinity_assignment_and_public_dispatch() {
    let bus = owned_redis("LIVEKIT_AGENT_REDIS_CLUSTER_URLS", true).await;
    let isolation = rtc_core::ids::new_sid("cluster");
    run_multi_affinity(bus.clone(), &isolation).await;
    run_public_dispatch(bus, &isolation).await;
}

async fn restart_marker_wait(dir: &std::path::Path, marker: &str) {
    tokio::time::timeout(Duration::from_secs(30), async {
        while !dir.join(marker).exists() {
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .expect("coordinator restart barrier");
}

#[tokio::test]
#[ignore = "requires coordinator-owned Redis restart runner and barrier directory"]
async fn actual_redis_restart_recovers_existing_agent_claims_cache_and_termination() {
    let dir = std::path::PathBuf::from(
        std::env::var("LIVEKIT_AGENT_RESTART_BARRIER_DIR")
            .expect("owned restart runner must set LIVEKIT_AGENT_RESTART_BARRIER_DIR"),
    );
    let bus = owned_redis("LIVEKIT_AGENT_REDIS_SINGLE_URL", false).await;
    let transport = match &bus {
        RpcBus::Redis(redis) => redis.clone(),
        _ => unreachable!(),
    };
    let caller = node(bus.clone()).await;
    let owner = node(bus.clone()).await;
    let prefix = rtc_core::ids::new_sid("restart");
    let agent = format!("agent-{prefix}");
    let worker_id = format!("AW_{prefix}");
    let initial_epoch = caller.cache_epoch.load(Ordering::Acquire);
    let (mut events, actor) = worker(
        owner.registry.clone(),
        &worker_id,
        request(&agent, "one", ""),
        0.,
        keys(),
    )
    .await;
    tokio::time::timeout(Duration::from_secs(3), async {
        while caller.cache_epoch.load(Ordering::Acquire) == initial_epoch {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("initial WorkerRegistered notification before cache fill");
    let mut discovery = caller.discover(agent.clone(), 0, "".into());
    assert_eq!(discovery.recv().await.unwrap().namespace, "one");
    while discovery.recv().await.is_some() {}
    assert!(caller.cache.lock().unwrap().is_some());
    let initial_id = format!("AJ_{prefix}_before");
    assert_eq!(
        caller
            .request(&job(&initial_id, &agent, "one", ""))
            .await
            .unwrap()
            .worker_id,
        worker_id
    );
    for _ in 0..2 {
        events.recv().await.unwrap();
    }
    let epoch = caller.cache_epoch.load(Ordering::Acquire);
    // The termination subscription exists before the broker is stopped, so
    // successful post-restart termination must recover a real existing route.
    std::fs::write(
        dir.join("restart-ready"),
        b"actual pre-restart assignment and cache ready",
    )
    .unwrap();
    restart_marker_wait(&dir, "restarted").await;
    tokio::time::timeout(Duration::from_secs(15), async {
        while caller.cache_epoch.load(Ordering::Acquire) < epoch + 2 {
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .expect("disconnect and acknowledged resubscribe invalidate cache");
    assert!(caller.cache.lock().unwrap().is_none());
    // Existing CheckEnabled server, claim/grant subscriptions and worker are
    // reused. No service restart or new worker registration can mask recovery.
    let mut discovery = caller.discover(agent.clone(), 0, "".into());
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(10), discovery.recv())
            .await
            .unwrap()
            .unwrap()
            .namespace,
        "one"
    );
    drop(discovery);
    let after_id = format!("AJ_{prefix}_after");
    let assigned = caller
        .request(&job(&after_id, &agent, "one", ""))
        .await
        .unwrap();
    assert_eq!(assigned.worker_id, worker_id);
    for _ in 0..2 {
        events.recv().await.unwrap();
    }
    for id in [&initial_id, &after_id] {
        caller.terminate(id, false).await.unwrap();
        let event = tokio::time::timeout(Duration::from_secs(3), events.recv())
            .await
            .unwrap()
            .unwrap();
        assert!(
            matches!(event.message, Some(pb::server_message::Message::Termination(t)) if t.job_id == *id)
        );
    }
    // Fresh actual WorkerRegistered traffic must still invalidate after recovery.
    let epoch = caller.cache_epoch.load(Ordering::Acquire);
    bus.publish(
        &channels::request("AgentInternal", "WorkerRegistered", &[""], false),
        &pbjson_types::Empty {},
    )
    .await
    .unwrap();
    tokio::time::timeout(Duration::from_secs(3), async {
        while caller.cache_epoch.load(Ordering::Acquire) == epoch {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    std::fs::write(
        dir.join("stop-again-ready"),
        b"post-restart lifecycle accepted",
    )
    .unwrap();
    restart_marker_wait(&dir, "stopped-again").await;
    tokio::time::timeout(Duration::from_secs(5), async {
        caller.shutdown().await;
        owner.registry.close_all();
        owner.shutdown().await;
        actor.await.unwrap();
    })
    .await
    .expect("shutdown cancels recovery while broker is down");
    assert_eq!(
        transport.active_subscriptions(),
        0,
        "recovery must not leak subscription permits"
    );
}

#[tokio::test]
async fn local_worker_registered_empty_survives_typed_forwarder_and_business_decode() {
    let bus: RpcBus = bus().into();
    let channel = channels::request("AgentInternal", "WorkerRegistered", &[""], false);
    let mut sub = bus
        .subscribe::<pbjson_types::Empty>(&channel, false)
        .await
        .unwrap();
    bus.publish(&channel, &pbjson_types::Empty {})
        .await
        .unwrap();
    let bytes = tokio::time::timeout(Duration::from_secs(1), sub.recv())
        .await
        .expect("local forwarder must not discard business Empty for missing internal descriptor")
        .unwrap();
    bus.decode::<pbjson_types::Empty>(&bytes)
        .expect("owner must decode same business schema as forwarder");
    sub.unsubscribe(&channel).await.unwrap();
}

#[tokio::test]
async fn overlapping_discovery_shares_actual_multi_before_cache_completion() {
    assert_actual_discovery_overlap(bus().into()).await;
}

#[tokio::test]
#[ignore = "requires LIVEKIT_AGENT_REDIS_SINGLE_URL; actual Redis discovery fanout"]
async fn actual_single_redis_overlapping_discovery_shares_inflight_multi() {
    assert_actual_discovery_overlap(owned_redis("LIVEKIT_AGENT_REDIS_SINGLE_URL", false).await)
        .await;
}

#[tokio::test]
#[ignore = "requires LIVEKIT_AGENT_REDIS_CLUSTER_URLS; actual Redis Cluster discovery fanout"]
async fn actual_cluster_redis_overlapping_discovery_shares_inflight_multi() {
    assert_actual_discovery_overlap(owned_redis("LIVEKIT_AGENT_REDIS_CLUSTER_URLS", true).await)
        .await;
}

async fn assert_actual_discovery_overlap(bus: RpcBus) {
    let owner = node(bus.clone()).await;
    let early_calls = Arc::new(AtomicU64::new(0));
    let tail_calls = Arc::new(AtomicU64::new(0));
    let first_tail_entered = Cancellation::default();
    let release_tail = Cancellation::default();
    let multi_limits = ServerLimits {
        max_request_duration: Duration::from_secs(5),
        ..limits()
    };
    let early = RpcServer::register_multi::<
        rpc::CheckEnabledRequest,
        rpc::CheckEnabledResponse,
        _,
        _,
    >(bus.clone(), spec("CheckEnabled", vec![]), multi_limits, {
        let calls = early_calls.clone();
        move |_, _| {
            calls.fetch_add(1, Ordering::SeqCst);
            async {
                Ok(rpc::CheckEnabledResponse {
                    room_enabled: true,
                    namespaces: vec!["early".into()],
                    agent_names: vec!["same-agent".into()],
                    ..Default::default()
                })
            }
        }
    })
    .await
    .unwrap();
    let tail =
        RpcServer::register_multi::<rpc::CheckEnabledRequest, rpc::CheckEnabledResponse, _, _>(
            bus,
            spec("CheckEnabled", vec![]),
            multi_limits,
            {
                let calls = tail_calls.clone();
                let entered = first_tail_entered.clone();
                let release = release_tail.clone();
                move |_, _| {
                    calls.fetch_add(1, Ordering::SeqCst);
                    entered.cancel();
                    let release = release.clone();
                    async move {
                        release.cancelled().await;
                        Ok(rpc::CheckEnabledResponse {
                            room_enabled: true,
                            namespaces: vec!["tail".into()],
                            agent_names: vec!["same-agent".into()],
                            ..Default::default()
                        })
                    }
                }
            },
        )
        .await
        .unwrap();

    let exercise = tokio::time::timeout(Duration::from_secs(8), async {
        let mut first = owner.discover("same-agent".into(), 0, "first-deploy".into());
        first_tail_entered.cancelled().await;
        let first_key = first.recv().await.unwrap();
        assert_eq!(first_key.namespace, "early");
        assert_eq!(first_key.deployment, "first-deploy");

        let mut second = owner.discover("same-agent".into(), 0, "second-deploy".into());
        let second_key = second.recv().await.unwrap();
        assert_eq!(second_key.namespace, "early");
        assert_eq!(second_key.deployment, "second-deploy");
        assert!(!release_tail.is_cancelled());
        let during = early_calls.load(Ordering::SeqCst);
        // Cancellation of one consumer must not cancel the shared RPC.
        drop(first);
        release_tail.cancel();
        let second_key = second.recv().await.unwrap();
        assert_eq!(second_key.namespace, "tail");
        assert_eq!(second_key.deployment, "second-deploy");
        assert!(second.recv().await.is_none());

        let mut cached = owner.discover("same-agent".into(), 0, "cached-deploy".into());
        assert_eq!(cached.recv().await.unwrap().namespace, "early");
        assert_eq!(cached.recv().await.unwrap().namespace, "tail");
        assert!(cached.recv().await.is_none());
        during
    })
    .await;
    release_tail.cancel();
    owner.shutdown().await;
    early.close().await;
    tail.close().await;
    assert_eq!(
        exercise.expect("overlapping actual PSRPC discovery must complete"),
        1,
        "second consumer started another actual CheckEnabled RPC before the first completed",
    );
    assert_eq!(tail_calls.load(Ordering::SeqCst), 1);
    assert_eq!(early_calls.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn invalidation_replaces_inflight_discovery_without_stranding_old_waiters() {
    let bus: RpcBus = bus().into();
    let owner = node(bus.clone()).await;
    let peer_calls = Arc::new(AtomicU64::new(0));
    let first_entered = Cancellation::default();
    let release_first = Cancellation::default();
    let peer =
        RpcServer::register_multi::<rpc::CheckEnabledRequest, rpc::CheckEnabledResponse, _, _>(
            bus.clone(),
            spec("CheckEnabled", vec![]),
            ServerLimits {
                max_request_duration: Duration::from_secs(5),
                ..limits()
            },
            {
                let calls = peer_calls.clone();
                let entered = first_entered.clone();
                let release = release_first.clone();
                move |_, _| {
                    let first = calls.fetch_add(1, Ordering::SeqCst) == 0;
                    let release = release.clone();
                    if first {
                        entered.cancel();
                    }
                    async move {
                        let namespace = if first {
                            release.cancelled().await;
                            "prior-generation"
                        } else {
                            "replacement-generation"
                        };
                        Ok(rpc::CheckEnabledResponse {
                            room_enabled: true,
                            namespaces: vec![namespace.into()],
                            ..Default::default()
                        })
                    }
                }
            },
        )
        .await
        .unwrap();
    let exercise = tokio::time::timeout(Duration::from_secs(8), async {
        let mut first = owner.discover("".into(), 0, "".into());
        first_entered.cancelled().await;
        let epoch = owner.cache_epoch.load(Ordering::Acquire);
        bus.publish(
            &channels::request("AgentInternal", "WorkerRegistered", &[""], false),
            &pbjson_types::Empty {},
        )
        .await
        .unwrap();
        while owner.cache_epoch.load(Ordering::Acquire) == epoch {
            tokio::task::yield_now().await;
        }
        let mut replacement = owner.discover("".into(), 0, "".into());
        assert_eq!(
            replacement.recv().await.unwrap().namespace,
            "replacement-generation"
        );
        assert!(!release_first.is_cancelled());
        release_first.cancel();
        assert_eq!(first.recv().await.unwrap().namespace, "prior-generation");
        assert!(first.recv().await.is_none());
        assert!(replacement.recv().await.is_none());

        let mut cached = owner.discover("".into(), 0, "".into());
        assert_eq!(
            cached.recv().await.unwrap().namespace,
            "replacement-generation"
        );
        assert!(cached.recv().await.is_none());
    })
    .await;
    release_first.cancel();
    owner.shutdown().await;
    peer.close().await;
    exercise.expect("each actual discovery generation must finish");
    assert_eq!(peer_calls.load(Ordering::SeqCst), 2);
}

#[derive(Clone, Default)]
struct ErrorLog(Arc<std::sync::Mutex<Vec<u8>>>);

impl std::io::Write for ErrorLog {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.0.lock().unwrap().extend_from_slice(buf);
        Ok(buf.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for ErrorLog {
    type Writer = ErrorLog;
    fn make_writer(&'a self) -> Self::Writer {
        self.clone()
    }
}

/// Shutdown cancels before it drains the controller, so a registry change that is already
/// pending leaves both select! branches ready and the pick is arbitrary. The official
/// server logs nothing on a clean SIGINT; the deliberate closing refusal must stay silent.
#[tokio::test]
async fn shutdown_with_pending_registry_change_logs_no_reconciliation_error() {
    let log = ErrorLog::default();
    let _guard = tracing::subscriber::set_default(
        tracing_subscriber::fmt()
            .with_writer(log.clone())
            .with_ansi(false)
            .with_max_level(tracing::Level::ERROR)
            .finish(),
    );
    // select! chooses uniformly among ready branches, so one round proves nothing.
    for _ in 0..32 {
        let registry = Registry::new(Limits::default());
        let cluster = Cluster::start(
            registry.clone(),
            Arc::new(TokenMinter::new(keys())),
            0.7,
            bus().into(),
        )
        .await
        .unwrap();
        registry.disconnect("worker-gone");
        cluster.cancel();
        cluster.shutdown().await;
    }
    let captured = String::from_utf8(log.0.lock().unwrap().clone()).unwrap();
    assert!(
        !captured.contains("agent RPC registration reconciliation failed"),
        "graceful shutdown reported a reconciliation failure: {captured}"
    );
}
