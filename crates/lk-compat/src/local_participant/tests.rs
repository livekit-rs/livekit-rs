use super::*;
use crate::service_bus::{
    redis_transport::TransportLimits,
    wire::{CompressionPolicy, WireLimits},
};
use std::sync::atomic::{AtomicUsize, Ordering};
#[tokio::test]
async fn real_local_message_bus_uses_configured_timeout_backoff_and_independent_attempts() {
    let bus = LocalTransport::new(TransportLimits {
        wire: WireLimits {
            encoded_bytes: 1 << 20,
            decompressed_bytes: 1 << 20,
        },
        compression: CompressionPolicy::default(),
        inbox_messages: 100,
        subscriptions: 100,
        commands: 100,
        io_timeout: Duration::from_secs(2),
    })
    .unwrap();
    let rpc = LocalParticipantRpc::new(
        bus.clone(),
        RpcPolicy::default(),
        Arc::new(RpcRegistry::new()),
    )
    .unwrap();
    let calls = Arc::new(AtomicUsize::new(0));
    let count = calls.clone();
    let expiries = Arc::new(Mutex::new(Vec::new()));
    let durations = expiries.clone();
    let server = RpcServer::register::<pb::PerformRpcRequest, pb::PerformRpcResponse, _, _>(
        bus.clone(),
        spec("r", "p"),
        ServerLimits {
            concurrent: 10,
            max_request_duration: Duration::MAX,
            allow_skip_claim: false,
        },
        move |ctx, _| {
            let count = count.clone();
            let durations = durations.clone();
            async move {
                let attempt = count.fetch_add(1, Ordering::SeqCst) + 1;
                durations
                    .lock()
                    .unwrap()
                    .push(ctx.expiry_ns - ctx.sent_at_ns);
                tokio::time::sleep(Duration::from_millis(3500)).await;
                Ok(pb::PerformRpcResponse {
                    payload: attempt.to_string(),
                })
            }
        },
        None,
    )
    .await
    .unwrap();
    let result = rpc
        .perform(pb::PerformRpcRequest {
            room: "r".into(),
            destination_identity: "p".into(),
            method: "actual-local-policy".into(),
            response_timeout_ms: 6000,
            ..Default::default()
        })
        .await
        .unwrap();
    assert_eq!(result.payload, "2");
    assert_eq!(calls.load(Ordering::SeqCst), 2);
    let values = expiries.lock().unwrap().clone();
    assert_eq!(values.len(), 2);
    assert!(values[0] <= 3_000_000_000 && values[0] > 2_900_000_000);
    assert!(values[1] <= 5_000_000_000 && values[1] > 4_900_000_000);
    server.close().await;
    rpc.shutdown().await;
}

fn params(key: RoomKey) -> rtc_core::session::JoinParams {
    use rtc_core::permission::{Grants, VideoGrant};
    rtc_core::session::JoinParams {
        room: key.clone(),
        identity: Identity("receiver".into()),
        grants: Grants {
            identity: "receiver".into(),
            video: VideoGrant {
                room_join: true,
                room: key.name.0.clone(),
                ..Default::default()
            },
            ..Default::default()
        },
        name: String::new(),
        metadata: String::new(),
        attributes: Default::default(),
        client: Default::default(),
        capabilities: Default::default(),
        auto_subscribe: true,
        auto_subscribe_data_track: true,
        adaptive_stream: false,
        single_pc: false,
        disable_ice_lite: false,
        subscriber_allow_pause: None,
        reconnect: false,
        reconnect_reason: 0,
        participant_sid: None,
        add_tracks: vec![],
        publisher_offer: None,
    }
}
#[tokio::test]
async fn actor_join_waits_for_real_membership_and_replacement_fences_old_sid() {
    let (state, net) = crate::test_support::state().await;
    let key = state.room_key("rpc-local-owner-generation");
    state
        .manager
        .create_room(key.clone(), Default::default())
        .await
        .unwrap();
    let first = state.manager.join(params(key.clone())).await.unwrap();
    let old_sid = first.participant_sid.clone();
    let room = state
        .manager
        .list_rooms(&key.project, &[])
        .into_iter()
        .find(|r| r.name == key.name)
        .unwrap();
    let owner = state.manager.capture_room_owner(&key, &room.sid).unwrap();
    assert_eq!(
        owner
            .rpc_participant(Identity("receiver".into()))
            .await
            .unwrap(),
        (old_sid.clone(), 0)
    );
    {
        let groups = state.local_dispatch.participant.groups.lock().await;
        assert_eq!(
            groups.owners[&key].topics["receiver"].sid, old_sid,
            "Join returned after actual membership ACK"
        );
    }
    state
        .manager
        .remove_participant(&key, &Identity("receiver".into()))
        .await
        .unwrap();
    let second = state.manager.join(params(key.clone())).await.unwrap();
    assert_ne!(second.participant_sid, old_sid);
    assert_eq!(
        state.local_dispatch.participant.groups.lock().await.owners[&key].topics["receiver"].sid,
        second.participant_sid
    );
    let stale = crate::rpc::perform_rpc_owned(
        &state.rpc,
        &owner,
        &Identity("receiver".into()),
        "must-not-send",
        "",
        10000,
        &old_sid,
    )
    .await;
    assert!(matches!(
        stale,
        Err(RpcFailure::Api(rtc_runtime::ApiError::Unavailable(_)))
    ));
    assert_eq!(state.rpc.pending_count(), 0);
    state.manager.delete_room(&key).await.unwrap();
    assert!(
        !state
            .local_dispatch
            .participant
            .groups
            .lock()
            .await
            .owners
            .contains_key(&key)
    );
    state.shutdown_workers().await;
    net.shutdown();
}

#[tokio::test(start_paused = true)]
async fn accepted_operation_outlives_expired_caller_and_is_joined_by_owner() {
    let accepted = Accepted::default();
    let completed = Arc::new(AtomicUsize::new(0));
    let effect = completed.clone();
    let receiver = accepted.start(async move {
        tokio::time::sleep(Duration::from_millis(100)).await;
        effect.fetch_add(1, Ordering::SeqCst);
        Ok("actual accepted result".into())
    });
    tokio::task::yield_now().await;
    drop(receiver);
    assert!(!accepted.is_finished());
    tokio::time::advance(Duration::from_millis(100)).await;
    accepted.finish().await;
    assert_eq!(completed.load(Ordering::SeqCst), 1);
    assert!(accepted.is_finished());
}
#[tokio::test]
async fn participant_close_resolves_only_that_captured_sid() {
    let registry = RpcRegistry::new();
    let old = ParticipantSid("old".into());
    let new = ParticipantSid("new".into());
    let (_ack, reply) = registry.register("old-request", old.clone()).unwrap();
    let (_other_ack, mut other_reply) = registry.register("new-request", new.clone()).unwrap();
    registry.disconnect_participant(&old);
    assert_eq!(
        reply.await.unwrap(),
        crate::rpc::RpcOutcome::Error(crate::rpc::DataChannelRpcError::built_in(
            crate::rpc::codes::RECIPIENT_DISCONNECTED
        ))
    );
    assert!(matches!(
        other_reply.try_recv(),
        Err(oneshot::error::TryRecvError::Empty)
    ));
    assert_eq!(registry.pending_count(), 1);
    registry.disconnect_participant(&new);
    assert_eq!(registry.pending_count(), 0);
}

fn local_bus() -> Arc<LocalTransport> {
    LocalTransport::new(TransportLimits {
        wire: WireLimits {
            encoded_bytes: 1 << 20,
            decompressed_bytes: 1 << 20,
        },
        compression: CompressionPolicy::default(),
        inbox_messages: 100,
        subscriptions: 100,
        commands: 100,
        io_timeout: Duration::from_secs(2),
    })
    .unwrap()
}

/// A destination that answers inside the attempt window must run once per
/// `perform`. A lost in-window answer costs the caller a whole 3 s attempt and a
/// second client-visible RPC dispatch, which is the shape the interoperability
/// harness once mistook for duplicate queue membership.
#[tokio::test]
async fn in_window_answer_runs_the_handler_once_per_publish() {
    for handler_ms in [500u64, 1500, 2500] {
        let bus = local_bus();
        let rpc = LocalParticipantRpc::new(
            bus.clone(),
            RpcPolicy::default(),
            Arc::new(RpcRegistry::new()),
        )
        .unwrap();
        let calls = Arc::new(AtomicUsize::new(0));
        let count = calls.clone();
        let server = RpcServer::register::<pb::PerformRpcRequest, pb::PerformRpcResponse, _, _>(
            bus.clone(),
            spec("probe-r", "probe-p"),
            ServerLimits {
                concurrent: 10,
                max_request_duration: Duration::MAX,
                allow_skip_claim: false,
            },
            move |_, _| {
                let count = count.clone();
                async move {
                    let attempt = count.fetch_add(1, Ordering::SeqCst);
                    tokio::time::sleep(Duration::from_millis(handler_ms)).await;
                    Ok(pb::PerformRpcResponse {
                        payload: format!("ok-{attempt}"),
                    })
                }
            },
            None,
        )
        .await
        .unwrap();
        let result = rpc
            .perform(pb::PerformRpcRequest {
                room: "probe-r".into(),
                destination_identity: "probe-p".into(),
                method: "probe".into(),
                response_timeout_ms: 6000,
                ..Default::default()
            })
            .await
            .unwrap();
        assert_eq!(result.payload, "ok-0", "handler_ms={handler_ms}");
        assert_eq!(
            calls.load(Ordering::SeqCst),
            1,
            "handler_ms={handler_ms} must not be re-dispatched"
        );
        server.close().await;
        rpc.shutdown().await;
    }
}

/// One `{room}_{identity}` Participant topic is a PSRPC queue group, so a request
/// can never run the destination handler twice even while two members are live.
/// Two independent guards hold, and the mutant matrix separates them: selecting
/// every queue member in `publish_encoded` turns
/// `service_bus::local_transport::tests::absent_publication_is_not_replayed_to_later_subscription_and_queue_round_robins`
/// red on its write count (`left: 2, right: 1`) yet leaves this test green, because
/// `require_claim: true` with `allow_skip_claim: false` arbitrates a duplicated
/// delivery before the handler runs.
#[tokio::test]
async fn two_members_on_one_topic_run_the_handler_once_per_publish() {
    let bus = local_bus();
    let rpc = LocalParticipantRpc::new(
        bus.clone(),
        RpcPolicy::default(),
        Arc::new(RpcRegistry::new()),
    )
    .unwrap();
    let seen = Arc::new(Mutex::new(Vec::<String>::new()));
    let member = |tag: &'static str| {
        let seen = seen.clone();
        RpcServer::register::<pb::PerformRpcRequest, pb::PerformRpcResponse, _, _>(
            bus.clone(),
            spec("queue-r", "queue-p"),
            ServerLimits {
                concurrent: 10,
                max_request_duration: Duration::MAX,
                allow_skip_claim: false,
            },
            move |_, request| {
                let seen = seen.clone();
                async move {
                    seen.lock()
                        .unwrap()
                        .push(format!("{tag}:{}", request.method));
                    Ok(pb::PerformRpcResponse {
                        payload: format!("{tag}:{}", request.method),
                    })
                }
            },
            None,
        )
    };
    let first = member("left").await.unwrap();
    let second = member("right").await.unwrap();
    let perform = |method: String| {
        rpc.perform(pb::PerformRpcRequest {
            room: "queue-r".into(),
            destination_identity: "queue-p".into(),
            method,
            response_timeout_ms: 6000,
            ..Default::default()
        })
    };
    for idx in 0..3 {
        perform(format!("probe-{idx}")).await.unwrap();
    }
    tokio::time::sleep(Duration::from_millis(50)).await;
    let deliveries = seen.lock().unwrap().clone();
    // Every publish carries its own method, so an extra handler run shows up either
    // as more deliveries than publishes or one method delivered twice.
    assert_eq!(
        deliveries.len(),
        3,
        "one publish must reach one queue member: {deliveries:?}"
    );
    let mut methods: Vec<_> = deliveries
        .iter()
        .map(|d| d.split_once(':').expect("tagged delivery").1.to_string())
        .collect();
    methods.sort();
    methods.dedup();
    assert_eq!(
        methods.len(),
        3,
        "no publish may be served twice: {deliveries:?}"
    );
    first.close().await;
    let after = perform("probe-after-close".to_string())
        .await
        .unwrap()
        .payload;
    let deliveries = seen.lock().unwrap().clone();
    assert_eq!(
        after, "right:probe-after-close",
        "the surviving member must still take publishes: {deliveries:?}"
    );
    assert_eq!(
        deliveries.len(),
        4,
        "the retired member must not keep serving: {deliveries:?}"
    );
    second.close().await;
    rpc.shutdown().await;
}
