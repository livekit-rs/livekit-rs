use super::*;
#[tokio::test]
async fn actual_room_membership_routes_public_packet_and_retires_without_name_reuse() {
    let (state, net) = crate::test_support::state().await;
    let key = state.room_key("local-room-senddata");
    let first = state
        .manager
        .create_room(key.clone(), Default::default())
        .await
        .unwrap();
    assert_eq!(
        state.local_dispatch.room.groups.lock().await.owners[&key].sid,
        first.sid
    );
    let response = state
        .local_dispatch
        .send_data(pb::SendDataRequest {
            room: key.name.0.clone(),
            data: vec![7, 8, 9],
            nonce: vec![1; 16],
            ..Default::default()
        })
        .await
        .unwrap();
    assert_eq!(response, pb::SendDataResponse {});
    state.manager.delete_room(&key).await.unwrap();
    assert!(
        !state
            .local_dispatch
            .room
            .groups
            .lock()
            .await
            .owners
            .contains_key(&key)
    );
    let second = state
        .manager
        .create_room(key.clone(), Default::default())
        .await
        .unwrap();
    assert_ne!(first.sid, second.sid);
    assert_eq!(
        state.local_dispatch.room.groups.lock().await.owners[&key].sid,
        second.sid
    );
    state.local_dispatch.room.closed(&key, &first.sid).await;
    assert_eq!(
        state.local_dispatch.room.groups.lock().await.owners[&key].sid,
        second.sid,
        "stale close cannot retire replacement topic"
    );
    state
        .local_dispatch
        .send_data(pb::SendDataRequest {
            room: key.name.0.clone(),
            data: vec![4],
            ..Default::default()
        })
        .await
        .unwrap();
    state.manager.delete_room(&key).await.unwrap();
    state.shutdown_workers().await;
    net.shutdown();
}

#[tokio::test]
async fn actual_typed_room_client_retries_with_original_nonce_and_destinations() {
    use crate::service_bus::{
        redis_transport::TransportLimits,
        wire::{CompressionPolicy, WireLimits},
    };
    use std::sync::atomic::{AtomicUsize, Ordering};
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
    let client = LocalRoom::new(bus.clone(), RpcPolicy::default()).unwrap();
    let request = pb::SendDataRequest {
        room: "actual-local-policy".into(),
        data: vec![0, 255, 7],
        destination_identities: vec!["receiver".into()],
        destination_sids: vec!["PA_actual".into()],
        nonce: vec![5; 16],
        topic: Some("retained".into()),
        ..Default::default()
    };
    let expected = request.clone();
    let calls = Arc::new(AtomicUsize::new(0));
    let accepted = calls.clone();
    let lifetime = Arc::new(Mutex::new(Vec::new()));
    let budgets = lifetime.clone();
    let server = RpcServer::register::<pb::SendDataRequest, pb::SendDataResponse, _, _>(
        bus.clone(),
        method(&request.room),
        ServerLimits {
            concurrent: 10,
            max_request_duration: Duration::MAX,
            allow_skip_claim: false,
        },
        move |ctx, request| {
            assert_eq!(request, expected);
            let calls = accepted.clone();
            let budgets = budgets.clone();
            async move {
                calls.fetch_add(1, Ordering::SeqCst);
                budgets.lock().unwrap().push(ctx.expiry_ns - ctx.sent_at_ns);
                tokio::time::sleep(Duration::from_millis(3500)).await;
                Ok(pb::SendDataResponse {})
            }
        },
        None,
    )
    .await
    .unwrap();
    client.send(request).await.unwrap();
    assert_eq!(calls.load(Ordering::SeqCst), 2);
    let budgets = lifetime.lock().unwrap().clone();
    assert_eq!(budgets.len(), 2);
    assert!(budgets[0] > 2_900_000_000 && budgets[0] <= 3_000_000_000);
    assert!(budgets[1] > 4_900_000_000 && budgets[1] <= 5_000_000_000);
    server.close().await;
    client.shutdown().await;
}

/// The production accepted-operation owner, not a retry helper, owns a blocked
/// operation after its PSRPC caller has stopped waiting. Native SCTP backpressure
/// is covered separately by runtime/SDK acceptance; this controls lifetime only.
#[tokio::test(start_paused = true)]
async fn accepted_operation_survives_caller_drop_and_is_joined_before_shutdown_returns() {
    use std::sync::atomic::{AtomicBool, Ordering};
    let accepted = Arc::new(Accepted::default());
    let (release, blocked) = oneshot::channel();
    let completed = Arc::new(AtomicBool::new(false));
    let observed = completed.clone();
    let reply = accepted.start(async move {
        blocked.await.unwrap();
        observed.store(true, Ordering::SeqCst);
        Ok(pb::SendDataResponse {})
    });
    tokio::task::yield_now().await;
    drop(reply);
    let joining = {
        let accepted = accepted.clone();
        tokio::spawn(async move { accepted.join().await })
    };
    tokio::task::yield_now().await;
    assert!(
        !joining.is_finished(),
        "retirement must await actual blocked operation"
    );
    assert!(!completed.load(Ordering::SeqCst));
    release.send(()).unwrap();
    joining.await.unwrap();
    assert!(completed.load(Ordering::SeqCst));
}
