use super::*;
use std::sync::atomic::{AtomicUsize, Ordering};

#[test]
fn absent_selection_and_explicit_override_are_distinct() {
    let defaults = CreateRoomCallOptions::new(Duration::from_millis(17))
        .resolve(Some(Duration::from_secs(10)));
    assert!(defaults.selection.accept_first);
    assert_eq!(
        defaults.selection.affinity_timeout,
        Some(Duration::from_secs(1))
    );
    let mut explicit = CreateRoomCallOptions::new(Duration::ZERO);
    explicit.selection = Some(crate::service_bus::rpc::SelectionOptions::default());
    let resolved = explicit.resolve(Some(Duration::from_secs(10)));
    assert!(!resolved.selection.accept_first);
    assert_eq!(resolved.selection.affinity_timeout, None);
    assert_eq!(resolved.timeout, Duration::from_secs(10));
}
#[tokio::test]
#[ignore = "requires dedicated CLUSTER_CONTROL_REDIS_URL; actual claims, caller context and original source addon"]
async fn real_claimed_create_options_default_and_context_terminal_order() {
    let url = std::env::var("CLUSTER_CONTROL_REDIS_URL").expect("exclusive broker");
    for attempts in [1, 3] {
        let node = rtc_core::ids::new_sid("ND_");
        let (_, net) = state(&node).await;
        let mut app = crate::state::CompatServices::new(Arc::new(
            super::create_room_policy::loaded(attempts, "1s", &node),
        ))
        .assemble(net.clone(), Arc::new(NullSink));
        let control = app
            .enable_room_control_component(actual_bus(&url).await, config(&node, &url))
            .await
            .unwrap();
        // Actual claimed request with only timeout option must reach real owner.
        let room = rtc_core::ids::new_sid("options-positive-");
        let created = control
            .create_room_with_options(
                pb::CreateRoomRequest {
                    name: room.clone(),
                    node_id: node.clone(),
                    ..Default::default()
                },
                CreateRoomCallOptions::new(Duration::from_secs(2)),
            )
            .await
            .unwrap();
        assert_eq!(created.name, room);
        assert!(app.manager.room_exists(&app.room_key(&room)));
        control
            .delete_room(pb::DeleteRoomRequest { room })
            .await
            .unwrap();
        let explicit_name = rtc_core::ids::new_sid("explicit-selection-");
        let mut explicit = CreateRoomCallOptions::new(Duration::from_secs(2));
        // Exercise a non-default affinity window without requiring Redis and
        // both request tasks to complete a round trip within one scheduler tick.
        explicit.selection = Some(crate::service_bus::rpc::SelectionOptions {
            affinity_timeout: Some(Duration::from_millis(250)),
            ..Default::default()
        });
        let explicit_room = control
            .create_room_with_options(
                pb::CreateRoomRequest {
                    name: explicit_name.clone(),
                    node_id: node.clone(),
                    ..Default::default()
                },
                explicit,
            )
            .await
            .unwrap();
        assert_eq!(explicit_room.name, explicit_name);
        control
            .delete_room(pb::DeleteRoomRequest {
                room: explicit_name,
            })
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
        let n = count.clone();
        let server = RpcServer::register::<pb::CreateRoomRequest, pb::Room, _, _>(
            control.bus.clone(),
            create_spec(&node),
            control.limits,
            move |_, _| {
                n.fetch_add(1, Ordering::SeqCst);
                std::future::pending::<Result<pb::Room, RpcError>>()
            },
            None,
        )
        .await
        .unwrap();
        *control.node_server.lock().await = Some(server);
        let mut options = CreateRoomCallOptions::new(Duration::from_secs(1));
        options.deadline = Some(std::time::Instant::now() + Duration::from_millis(100));
        let error = control
            .create_room_with_options(
                pb::CreateRoomRequest {
                    name: rtc_core::ids::new_sid("context-deadline-"),
                    node_id: node.clone(),
                    ..Default::default()
                },
                options,
            )
            .await
            .unwrap_err();
        // Fixed retry.go checks attempt exhaustion first; only another retry
        // reaches Done and returns ErrRequestCanceled (not ctx.Err()).
        assert_eq!(
            error.code,
            if attempts == 1 {
                "deadline_exceeded"
            } else {
                "canceled"
            }
        );
        assert_eq!(count.load(Ordering::SeqCst), 1);
        assert_eq!(control.client.pending_count(), 0);
        let before = count.load(Ordering::SeqCst);
        let options = CreateRoomCallOptions::new(Duration::from_secs(2));
        let cancel = options.cancel.clone();
        let caller = control.clone();
        let pending = tokio::spawn(async move {
            caller
                .create_room_with_options(
                    pb::CreateRoomRequest {
                        name: rtc_core::ids::new_sid("context-cancel-"),
                        node_id: node,
                        ..Default::default()
                    },
                    options,
                )
                .await
        });
        tokio::time::timeout(Duration::from_secs(2), async {
            while count.load(Ordering::SeqCst) == before {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        cancel.cancel();
        assert_eq!(pending.await.unwrap().unwrap_err().code, "canceled");
        assert_eq!(count.load(Ordering::SeqCst), before + 1);
        assert_eq!(control.client.pending_count(), 0);
        control.shutdown().await.unwrap();
        app.manager.close_all().await;
        app.shutdown_workers().await;
        net.shutdown_and_wait().await;
    }
}

#[test]
fn method_nonpositive_inherits_context_before_source_default_and_keeps_expiry() {
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    let mut caller = CreateRoomCallOptions::new(Duration::ZERO);
    caller.deadline = Some(deadline);
    let inherited = caller.resolve(None);
    assert!(inherited.timeout > Duration::from_secs(3));
    assert!(inherited.timeout <= Duration::from_secs(10));
    assert_eq!(inherited.deadline, Some(deadline));
    assert_eq!(
        caller.resolve(Some(Duration::from_secs(7))).timeout,
        Duration::from_secs(7)
    );
    caller.timeout = Duration::from_secs(9);
    assert_eq!(
        caller.resolve(Some(Duration::from_secs(7))).timeout,
        Duration::from_secs(9)
    );
    caller.timeout = Duration::ZERO;
    caller.deadline = None;
    assert_eq!(caller.resolve(None).timeout, Duration::from_secs(3));
    let expired = std::time::Instant::now() - Duration::from_millis(1);
    caller.deadline = Some(expired);
    assert_eq!(caller.resolve(None).timeout, Duration::ZERO);
    assert_eq!(caller.resolve(None).deadline, Some(expired));
}

#[tokio::test]
#[ignore = "requires dedicated CLUSTER_CONTROL_REDIS_URL; actual serialized expiry with nonpositive method timeout"]
async fn real_nonpositive_method_timeout_retains_caller_deadline_on_wire_without_waiting() {
    use crate::service_bus::wire;
    use lk_protocol::psrpc::internal as bus;
    let url = std::env::var("CLUSTER_CONTROL_REDIS_URL").expect("exclusive broker");
    for method in ["0s", "-1s"] {
        let node = rtc_core::ids::new_sid("ND_");
        let (_, net) = state(&node).await;
        let mut app = crate::state::CompatServices::new(Arc::new(
            super::create_room_policy::loaded(1, method, &node),
        ))
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
        let original = Arc::new(std::sync::Mutex::new(None));
        let seen = original.clone();
        let owner = control.clone();
        let server = RpcServer::register::<pb::CreateRoomRequest, pb::Room, _, _>(
            control.bus.clone(),
            create_spec(&node),
            control.limits,
            move |context, request| {
                *seen.lock().unwrap() = Some(context.deadline);
                let owner = owner.clone();
                async move {
                    owner
                        .create_owner_with_expiry(
                            request,
                            Some(context.deadline),
                            Some(context.expiry_ns),
                        )
                        .await
                }
            },
            None,
        )
        .await
        .unwrap();
        *control.node_server.lock().await = Some(server);
        let channel = create_spec(&node).request_channel();
        let mut observer = control.bus.subscribe(&channel, false).await.unwrap();
        let name = rtc_core::ids::new_sid("nonpositive-deadline-");
        let mut options = CreateRoomCallOptions::new(Duration::ZERO);
        let deadline = std::time::Instant::now() + Duration::from_secs(10);
        options.deadline = Some(deadline);
        // The real owner responds immediately; this gate does not wait10s.
        let room = control
            .create_room_with_options(
                pb::CreateRoomRequest {
                    name: name.clone(),
                    node_id: node,
                    ..Default::default()
                },
                options,
            )
            .await
            .unwrap();
        assert_eq!(room.name, name);
        assert!(app.manager.room_exists(&app.room_key(&name)));
        let raw = tokio::time::timeout(Duration::from_secs(2), observer.recv())
            .await
            .unwrap()
            .unwrap();
        let wire = wire::decode::<bus::Request>(&raw, control.bus.limits().wire)
            .unwrap()
            .message;
        // Source actual ctx10s with no WithTimeout emits original context
        // remaining budget, not a fresh/default3s per-attempt budget.
        assert!(
            wire.expiry - wire.sent_at > 3_000_000_000,
            "actual wire must not silently cap inherited caller context at3s"
        );
        assert!(wire.expiry - wire.sent_at <= 10_000_000_000);
        let accepted = original
            .lock()
            .unwrap()
            .expect("real owner handler accepted original wire expiry");
        assert!(accepted > deadline - Duration::from_secs(1));
        assert!(accepted <= deadline + Duration::from_secs(1));
        assert_eq!(control.client.pending_count(), 0);
        observer.unsubscribe(&channel).await.unwrap();
        control
            .delete_room(pb::DeleteRoomRequest { room: name })
            .await
            .unwrap();
        control.shutdown().await.unwrap();
        app.manager.close_all().await;
        app.shutdown_workers().await;
        net.shutdown_and_wait().await;
    }
}
