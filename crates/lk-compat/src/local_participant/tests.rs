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
