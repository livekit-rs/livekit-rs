//! Old public RoomService/CreateRoom entry, real Redis RPC and real room owner.
//! Controlled unavailable responses are fixture fault injection, not simulated success.
use super::*;
use std::sync::atomic::{AtomicUsize, Ordering};

pub(super) fn loaded(attempts: i64, timeout: &str, node: &str) -> rtc_core::config::ServerConfig {
    struct Probe;
    impl lk_config::netdetect::NetProbe for Probe {
        fn local_addresses(&self) -> Vec<std::net::IpAddr> {
            vec![std::net::Ipv4Addr::LOCALHOST.into()]
        }
        fn external_ip(&self, _: &[String], _: bool) -> Result<std::net::IpAddr, String> {
            panic!("no discovery")
        }
        fn sleep(&self, _: Duration) {
            panic!("no discovery retry")
        }
    }
    let args: Vec<std::ffi::OsString> = vec![
        "livecloud".into(),
        "--dev".into(),
        "--node-ip=127.0.0.1".into(),
        format!("--room.create_room_attempts={attempts}").into(),
        format!("--room.create_room_timeout={timeout}").into(),
    ];
    let info = lk_config::ProgramInfo {
        binary_name: "livecloud".into(),
        product_version: "test".into(),
        compat_version: "1.13.10",
    };
    let lk_config::EntryAction::RunServer(mut loaded) =
        lk_config::parse_with_probe(args, &lk_config::Env::default(), &info, &Probe).unwrap()
    else {
        panic!("server config")
    };
    loaded.server.node_id = node.into();
    loaded.server
}

async fn request_profile(attempts: i64, failures: usize) {
    let url = std::env::var("CLUSTER_CONTROL_REDIS_URL").expect("exclusive broker");
    let node = rtc_core::ids::new_sid("ND_");
    let (_, net) = state(&node).await;
    let mut app = crate::state::CompatServices::new(Arc::new(loaded(attempts, "2s", &node)))
        .assemble(net.clone(), Arc::new(NullSink));
    let control = app
        .enable_room_control_component(actual_bus(&url).await, config(&node, &url))
        .await
        .unwrap();
    control
        .node_server
        .lock()
        .await
        .take()
        .unwrap()
        .close()
        .await;
    let count = Arc::new(AtomicUsize::new(0));
    let weak = Arc::downgrade(&control);
    let observed = count.clone();
    let server = RpcServer::register::<pb::CreateRoomRequest, pb::Room, _, _>(
        control.bus.clone(),
        create_spec(&node),
        control.limits,
        move |_, request| {
            let weak = weak.clone();
            let observed = observed.clone();
            async move {
                let n = observed.fetch_add(1, Ordering::SeqCst);
                if n < failures {
                    return Err(RpcError::new(
                        "unavailable",
                        "owned test recoverable failure",
                    ));
                }
                weak.upgrade().unwrap().create_owner(request).await
            }
        },
        None,
    )
    .await
    .unwrap();
    *control.node_server.lock().await = Some(server);
    let room = rtc_core::ids::new_sid("create-policy-");
    let key = app.room_key(&room);
    let mut auth = VerifiedToken::default();
    auth.grants.video.room_create = true;
    let request = pb::CreateRoomRequest {
        name: room.clone(),
        node_id: node.clone(),
        ..Default::default()
    };
    let result = crate::twirp::room_service::call(
        &app,
        Some(&auth),
        "CreateRoom",
        Codec::Protobuf,
        &request.encode_to_vec(),
    )
    .await;
    if attempts == 1 {
        let error = result.expect_err("first recoverable error must stop at one attempt");
        assert_eq!(error.code.as_str(), "unavailable");
        assert_eq!(count.load(Ordering::SeqCst), 1);
        assert!(!app.manager.room_exists(&key));
    } else {
        let created: pb::Room = response(result.unwrap()).await;
        assert_eq!(count.load(Ordering::SeqCst), failures + 1);
        assert!(app.manager.room_exists(&key));
        // A repeated actual owner create keeps the same SID; after deletion a
        // new successful call must own a distinct generation, not stale cleanup.
        let again = control.create_room(request.clone()).await.unwrap();
        assert_eq!(again.sid, created.sid);
        control
            .delete_room(pb::DeleteRoomRequest { room: room.clone() })
            .await
            .unwrap();
        let replacement = control.create_room(request).await.unwrap();
        assert_ne!(replacement.sid, created.sid);
    }
    assert_eq!(control.client.pending_count(), 0);
    // Match executable shutdown: close actual owners before detaching routing.
    app.manager.close_all().await;
    control.shutdown().await.unwrap();
    assert!(control.generations.lock().await.is_empty());
    app.shutdown_workers().await;
    net.shutdown_and_wait().await;
}
#[tokio::test]
#[ignore = "requires dedicated CLUSTER_CONTROL_REDIS_URL; real public API/Redis/owner, old-API RED"]
async fn public_create_room_attempt_one_is_not_generic_global_three() {
    request_profile(1, 1).await;
}
#[tokio::test]
#[ignore = "requires dedicated CLUSTER_CONTROL_REDIS_URL; real public API/Redis/owner, old-API RED"]
async fn public_create_room_zero_and_negative_attempts_reach_real_fifth_owner_call() {
    request_profile(0, 4).await;
    request_profile(-1, 4).await;
}

#[tokio::test]
#[ignore = "requires dedicated CLUSTER_CONTROL_REDIS_URL; actual request expiry, no synthetic timeout error"]
async fn public_create_room_timeout_applies_to_each_real_rpc_attempt_and_releases_handlers() {
    struct Active(Arc<AtomicUsize>);
    impl Drop for Active {
        fn drop(&mut self) {
            self.0.fetch_sub(1, Ordering::SeqCst);
        }
    }
    let url = std::env::var("CLUSTER_CONTROL_REDIS_URL").expect("exclusive broker");
    let node = rtc_core::ids::new_sid("ND_");
    let (_, net) = state(&node).await;
    let mut app = crate::state::CompatServices::new(Arc::new(loaded(2, "37ms", &node)))
        .assemble(net.clone(), Arc::new(NullSink));
    let control = app
        .enable_room_control_component(actual_bus(&url).await, config(&node, &url))
        .await
        .unwrap();
    control
        .node_server
        .lock()
        .await
        .take()
        .unwrap()
        .close()
        .await;
    let count = Arc::new(AtomicUsize::new(0));
    let active = Arc::new(AtomicUsize::new(0));
    let n = count.clone();
    let a = active.clone();
    let server = RpcServer::register::<pb::CreateRoomRequest, pb::Room, _, _>(
        control.bus.clone(),
        create_spec(&node),
        control.limits,
        move |_, _| {
            n.fetch_add(1, Ordering::SeqCst);
            a.fetch_add(1, Ordering::SeqCst);
            let lease = Active(a.clone());
            async move {
                let _lease = lease;
                std::future::pending::<Result<pb::Room, RpcError>>().await
            }
        },
        None,
    )
    .await
    .unwrap();
    *control.node_server.lock().await = Some(server);
    let room = rtc_core::ids::new_sid("expiry-room-");
    let error = control
        .create_room(pb::CreateRoomRequest {
            name: room.clone(),
            node_id: node,
            ..Default::default()
        })
        .await
        .unwrap_err();
    assert_eq!(error.code, "deadline_exceeded");
    assert_eq!(count.load(Ordering::SeqCst), 2);
    assert_eq!(control.client.pending_count(), 0);
    assert!(!app.manager.room_exists(&app.room_key(&room)));
    control.shutdown().await.unwrap();
    assert_eq!(active.load(Ordering::SeqCst), 0);
    app.manager.close_all().await;
    app.shutdown_workers().await;
    net.shutdown_and_wait().await;
}
