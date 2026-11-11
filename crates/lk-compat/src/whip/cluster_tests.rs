//! Actual bus/owner lifecycle tests; positive decoded-media acceptance is the
//! separate multi-process native-whip-cluster campaign, not these fixtures.
use super::*;
use crate::service_bus::{
    local_transport::LocalTransport,
    redis_transport::TransportLimits,
    wire::{CompressionPolicy, WireLimits},
};
use rtc_core::permission::{Grants, VideoGrant};
fn bus() -> RpcBus {
    LocalTransport::new(TransportLimits {
        wire: WireLimits {
            encoded_bytes: 1 << 20,
            decompressed_bytes: 1 << 20,
        },
        compression: CompressionPolicy {
            quality: 0,
            threshold: 1024,
        },
        inbox_messages: 100,
        subscriptions: 100,
        commands: 100,
        io_timeout: Duration::from_secs(1),
    })
    .unwrap()
    .into()
}
fn request(room: &str) -> rpc::WhipCreateRequest {
    let grants = Grants {
        identity: "publisher".into(),
        metadata: "owner-limit".into(),
        video: VideoGrant {
            room: room.into(),
            room_join: true,
            ..Default::default()
        },
        ..Default::default()
    };
    let params = crate::signal::params::parse_join(
        &crate::test_support::config(),
        Some(grants),
        &Default::default(),
        &Default::default(),
        None,
        false,
        false,
    )
    .unwrap()
    .params;
    rpc::WhipCreateRequest {
        start_session: Some(crate::cluster_signal::codec::start_session(&params, vec![]).unwrap()),
        offer_sdp: "v=0\r\n".into(),
        ..Default::default()
    }
}
#[tokio::test]
async fn original_whip_create_bus_runs_selected_owner_validation_without_front_shadow_room() {
    let bus = bus();
    let (mut front, front_net) = crate::test_support::state().await;
    let (mut owner, owner_net) = crate::test_support::state().await;
    Arc::make_mut(&mut front.config).node_id = "ND_front".into();
    Arc::make_mut(&mut owner.config).node_id = "ND_owner".into();
    Arc::make_mut(&mut owner.config).limits.max_metadata_size = Some(1);
    front
        .enable_whip_rpc(bus.clone(), RpcPolicy::default())
        .await
        .unwrap();
    owner
        .enable_whip_rpc(bus.clone(), RpcPolicy::default())
        .await
        .unwrap();
    let client = RpcClient::new(bus, 8).unwrap();
    let mut options = CallOptions::new(Duration::from_secs(1));
    options.selection.accept_first = true;
    let error = client
        .unary::<_, rpc::WhipCreateResponse>(
            &spec("WHIP", "Create", "ND_owner"),
            &request("remote-owner"),
            options,
        )
        .await
        .unwrap_err();
    assert!(error.is_remote());
    assert_eq!(error.message, "metadata size exceeds limits");
    assert!(front.manager.debug_rooms().is_empty());
    assert!(owner.manager.debug_rooms().is_empty());
    assert!(
        owner
            .whip_rpc
            .as_ref()
            .unwrap()
            .groups
            .lock()
            .await
            .is_empty()
    );
    client.close_and_wait().await;
    for state in [&front, &owner] {
        state.manager.close_all().await;
        state.shutdown_whip().await;
        state.agents.shutdown().await;
    }
    front_net.shutdown();
    owner_net.shutdown();
}
#[tokio::test]
async fn participant_generation_retirement_preserves_replacement_and_shutdown_removes_topics() {
    let (mut state, net) = crate::test_support::state().await;
    let bus = bus();
    let rpc = state
        .enable_whip_rpc(bus, RpcPolicy::default())
        .await
        .unwrap();
    let room = state.room_key("generation");
    let identity = Identity("publisher".into());
    let old = ParticipantSid("PA_old".into());
    let new = ParticipantSid("PA_new".into());
    // Exercise the same hook installed in actual WhipSessions.create. This test
    // intentionally does not claim the registrations alone prove media success.
    rpc.register_participant(&room, &identity, &old)
        .await
        .unwrap();
    rpc.register_participant(&room, &identity, &new)
        .await
        .unwrap();
    let hook = Topics(Arc::downgrade(&rpc));
    hook.closed(&room, &identity, &old).await;
    assert_eq!(
        rpc.groups
            .lock()
            .await
            .get(&(room.clone(), identity.clone()))
            .unwrap()
            .0,
        new
    );
    hook.closed(&room, &identity, &new).await;
    assert!(rpc.groups.lock().await.is_empty());
    state.manager.close_all().await;
    state.shutdown_whip().await;
    assert!(rpc.create_server.lock().await.is_none());
    assert!(rpc.retiring.lock().await.is_empty());
    state.agents.shutdown().await;
    net.shutdown();
}

#[test]
fn original_whip_start_session_does_not_export_internal_single_pc_or_sdk_expiry() {
    let wire = request("original-whip").start_session.unwrap();
    let mut params = crate::cluster_signal::codec::join_params("fixture", &wire).unwrap();
    params.single_pc = true;
    params.auto_subscribe_data_track = true;
    params.grants.expires_at = 123456;
    params.auto_subscribe = true;
    params.disable_ice_lite = true;
    let start = start_session(&params).unwrap();
    assert!(!start.use_single_peer_connection);
    assert_eq!(start.auto_subscribe_data_track, None);
    assert_eq!(start.token_expires_at, 0);
    assert!(start.auto_subscribe);
    assert!(start.disable_ice_lite);
    assert_eq!(start.room_name, "original-whip");
    assert_eq!(start.identity, "publisher");
    assert_eq!(start.create_room.unwrap().name, "original-whip");
    assert!(start.add_track_requests.is_empty());
    assert!(start.publisher_offer.is_none());
    // Internal RTC configuration is still one-shot and uses one physical PC.
    assert!(params.single_pc);
    assert!(params.auto_subscribe_data_track);
}

#[tokio::test]
async fn actual_whip_factory_clientparams_two_sent_and_default_server_one_recv() {
    use opentelemetry::trace::{FutureExt as _, TraceContextExt as _};
    use opentelemetry_sdk::trace::{InMemorySpanExporter, SdkTracerProvider};
    let exporter = InMemorySpanExporter::default();
    let provider = SdkTracerProvider::builder()
        .with_simple_exporter(exporter.clone())
        .build();
    let context =
        crate::service_bus::telemetry::RpcTracing::from_provider(&provider).scoped_context();
    let bus = bus();
    let (mut front, front_net) = crate::test_support::state().await;
    let (mut owner, owner_net) = crate::test_support::state().await;
    Arc::make_mut(&mut front.config).node_id = "ND_trace_front".into();
    Arc::make_mut(&mut owner.config).node_id = "ND_trace_owner".into();
    Arc::make_mut(&mut owner.config).limits.max_metadata_size = Some(1);
    front
        .enable_whip_rpc(bus.clone(), RpcPolicy::default())
        .with_context(context.clone())
        .await
        .unwrap();
    owner
        .enable_whip_rpc(bus, RpcPolicy::default())
        .with_context(context)
        .await
        .unwrap();
    let result = front
        .whip_rpc
        .as_ref()
        .unwrap()
        .call::<_, rpc::WhipCreateResponse>(
            &spec("WHIP", "Create", "ND_trace_owner"),
            &request("traced-owner-validation"),
        )
        .await;
    assert_eq!(result.unwrap_err().message, "metadata size exceeds limits");
    assert!(front.manager.debug_rooms().is_empty() && owner.manager.debug_rooms().is_empty());
    let spans = exporter.get_finished_spans().unwrap();
    let sent: Vec<_> = spans
        .iter()
        .filter(|s| s.name == "Sent.WHIP.Create")
        .collect();
    let recv: Vec<_> = spans
        .iter()
        .filter(|s| s.name == "Recv.WHIP.Create")
        .collect();
    assert_eq!(sent.len(), 2);
    assert_eq!(recv.len(), 1);
    let inner = sent
        .iter()
        .find(|s| s.span_context.span_id() == recv[0].parent_span_id)
        .unwrap();
    assert!(
        sent.iter()
            .any(|s| s.span_context.span_id() == inner.parent_span_id)
    );
    assert_eq!(
        inner.span_context.trace_id(),
        recv[0].span_context.trace_id()
    );
    for s in spans {
        assert!(matches!(
            s.status,
            opentelemetry::trace::Status::Error { .. }
        ));
    }
    for state in [&front, &owner] {
        state.manager.close_all().await;
        state.shutdown_whip().await;
        state.agents.shutdown().await;
    }
    front_net.shutdown();
    owner_net.shutdown();
}
