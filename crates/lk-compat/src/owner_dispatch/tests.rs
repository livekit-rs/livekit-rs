use super::*;
use crate::service_bus::{
    rpc::{CallOptions, RpcClient, RpcErrorOrigin},
    wire,
};
use std::time::Duration;

#[test]
fn exact_generated_contract_is_unary_claimed_queue_and_room_topic() {
    for name in ["CreateDispatch", "DeleteDispatch", "ListDispatch"] {
        let spec = method("room.with_under_score", name);
        assert_eq!(spec.service, "AgentDispatchInternal");
        assert_eq!(spec.method, name);
        assert_eq!(spec.topics, ["room.with_under_score"]);
        assert!(spec.queue);
        assert!(spec.require_claim);
    }
}

async fn bus() -> Arc<RedisTransport> {
    use crate::service_bus::redis_transport::TransportLimits;
    RedisTransport::connect(
        &std::env::var("OWNER_DISPATCH_REDIS_URL").expect("exclusive broker"),
        TransportLimits {
            wire: wire::WireLimits {
                encoded_bytes: 1 << 20,
                decompressed_bytes: 1 << 20,
            },
            compression: wire::CompressionPolicy::default(),
            inbox_messages: 64,
            subscriptions: 64,
            commands: 16,
            io_timeout: Duration::from_secs(2),
        },
    )
    .await
    .unwrap()
}
fn limits() -> ServerLimits {
    ServerLimits {
        concurrent: 4,
        max_request_duration: Duration::from_secs(5),
        allow_skip_claim: false,
    }
}
fn options() -> CallOptions {
    let mut options = CallOptions::new(Duration::from_secs(4));
    options.selection.accept_first = true;
    options
}

#[tokio::test]
#[ignore = "requires coordinator-owned exclusive OWNER_DISPATCH_REDIS_URL; real queue+claim subscriptions"]
async fn actual_three_topics_crud_same_dispatch_id_and_owner_generation() {
    let (state, net) = crate::test_support::state().await;
    let key = state.room_key(&rtc_core::ids::new_sid("owner_dispatch_room_"));
    let room = state
        .manager
        .create_room_lease(key.clone(), Default::default())
        .await
        .unwrap();
    let owner = state
        .manager
        .capture_room_owner(&key, &room.snapshot().sid)
        .unwrap();
    let bus = bus().await;
    let group =
        AgentDispatchTopics::register(&state.manager, &key, owner.sid(), bus.clone(), limits())
            .await
            .unwrap();
    let client = RpcClient::new(bus, 8).unwrap();
    let id = rtc_core::ids::new_sid("AD_");
    let request = pb::AgentDispatch {
        id: id.clone(),
        room: key.name.0.clone(),
        agent_name: "actual-dispatch".into(),
        ..Default::default()
    };
    let created: pb::AgentDispatch = client
        .unary(&method(&key.name.0, "CreateDispatch"), &request, options())
        .await
        .unwrap();
    assert_eq!(created.id, id);
    assert_eq!(created.room, key.name.0);
    assert_eq!(created.agent_name, "actual-dispatch");
    assert!(
        created.state.unwrap().jobs.is_empty(),
        "no connected worker implies no invented job"
    );
    assert_eq!(
        owner.list_agent_dispatches(Some(&id)).await.unwrap()[0].id,
        id
    );
    let listed: pb::ListAgentDispatchResponse = client
        .unary(
            &method(&key.name.0, "ListDispatch"),
            &pb::ListAgentDispatchRequest {
                room: key.name.0.clone(),
                dispatch_id: id.clone(),
            },
            options(),
        )
        .await
        .unwrap();
    assert_eq!(listed.agent_dispatches.len(), 1);
    assert_eq!(listed.agent_dispatches[0].id, id);
    let deleted: pb::AgentDispatch = client
        .unary(
            &method(&key.name.0, "DeleteDispatch"),
            &pb::DeleteAgentDispatchRequest {
                room: key.name.0.clone(),
                dispatch_id: id.clone(),
            },
            options(),
        )
        .await
        .unwrap();
    assert_eq!(deleted.id, id);
    assert!(
        owner
            .list_agent_dispatches(Some(&id))
            .await
            .unwrap()
            .is_empty()
    );
    let error = client
        .unary::<_, pb::AgentDispatch>(
            &method(&key.name.0, "DeleteDispatch"),
            &pb::DeleteAgentDispatchRequest {
                room: key.name.0.clone(),
                dispatch_id: id,
            },
            options(),
        )
        .await
        .unwrap_err();
    assert!(error.is_remote());
    assert_eq!(error.code, "not_found");
    assert_eq!(owner.sid(), &room.snapshot().sid);
    for receipt in group.retire().await.unwrap() {
        receipt.finish().await;
    }
    client.close();
    drop(room);
    state.manager.close_all().await;
    net.shutdown();
}

#[tokio::test]
#[ignore = "requires coordinator-owned exclusive OWNER_DISPATCH_REDIS_URL; real accepted request barrier"]
async fn actual_accepted_old_rpc_cancel_and_replacement_do_not_mutate_new_sid() {
    let (state, net) = crate::test_support::state().await;
    let key = state.room_key(&rtc_core::ids::new_sid("held_owner_room_"));
    let old = state
        .manager
        .create_room_lease(key.clone(), Default::default())
        .await
        .unwrap();
    let captured = state
        .manager
        .capture_room_owner(&key, &old.snapshot().sid)
        .unwrap();
    let bus = bus().await;
    let entered = Arc::new(tokio::sync::Notify::new());
    let release = Arc::new(tokio::sync::Notify::new());
    // A real PSRPC server with exact generated contract, actual full Claim->Grant
    // and an entered handler barrier. Its operation is the same captured-handle
    // create() used by the actual three-topic registration above.
    let server = RpcServer::register_with_raw::<pb::AgentDispatch, pb::AgentDispatch, _, _>(
        bus.clone(),
        method(&key.name.0, "CreateDispatch"),
        limits(),
        {
            let captured = captured.clone();
            let entered = entered.clone();
            let release = release.clone();
            move |_, request| {
                let owner = captured.clone();
                let entered = entered.clone();
                let release = release.clone();
                async move {
                    entered.notify_one();
                    release.notified().await;
                    create(&owner, request.message).await
                }
            }
        },
        None,
    )
    .await
    .unwrap();
    let client = Arc::new(RpcClient::new(bus.clone(), 8).unwrap());
    let id = rtc_core::ids::new_sid("AD_");
    let request = pb::AgentDispatch {
        id: id.clone(),
        room: key.name.0.clone(),
        ..Default::default()
    };
    let call_options = options();
    let cancel = call_options.cancel.clone();
    let pending = tokio::spawn({
        let client = client.clone();
        let spec = method(&key.name.0, "CreateDispatch");
        let request = request.clone();
        async move {
            client
                .unary::<_, pb::AgentDispatch>(&spec, &request, call_options)
                .await
        }
    });
    tokio::time::timeout(Duration::from_secs(2), entered.notified())
        .await
        .unwrap();
    let retirement = tokio::time::timeout(Duration::from_secs(1), server.retire())
        .await
        .unwrap()
        .unwrap();
    cancel.cancel();
    let error = pending.await.unwrap().unwrap_err();
    assert_eq!(error.origin, RpcErrorOrigin::Canceled);
    state.manager.delete_room(&key).await.unwrap();
    let new = state
        .manager
        .create_room_lease(key.clone(), Default::default())
        .await
        .unwrap();
    assert_ne!(old.snapshot().sid, new.snapshot().sid);
    let fresh_owner = state
        .manager
        .capture_room_owner(&key, &new.snapshot().sid)
        .unwrap();
    assert!(
        state
            .manager
            .capture_room_owner(&key, captured.sid())
            .is_err()
    );
    let replacement =
        AgentDispatchTopics::register(&state.manager, &key, fresh_owner.sid(), bus, limits())
            .await
            .unwrap();
    release.notify_one();
    tokio::time::timeout(Duration::from_secs(1), retirement.finish())
        .await
        .unwrap();
    assert!(
        fresh_owner
            .list_agent_dispatches(Some(&id))
            .await
            .unwrap()
            .is_empty()
    );
    let created: pb::AgentDispatch = client
        .unary(&method(&key.name.0, "CreateDispatch"), &request, options())
        .await
        .unwrap();
    assert_eq!(created.id, id);
    assert_eq!(
        fresh_owner
            .list_agent_dispatches(Some(&id))
            .await
            .unwrap()
            .len(),
        1
    );
    for receipt in replacement.retire().await.unwrap() {
        receipt.finish().await;
    }
    client.close();
    drop(old);
    drop(new);
    state.manager.close_all().await;
    net.shutdown();
}
