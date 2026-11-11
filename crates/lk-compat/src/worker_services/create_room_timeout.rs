//! Actual public direct CreateRoom -> original WorkerServices -> real Redis wire.
//! No worker reply is manufactured; no successful recording is claimed.
use super::*;
use crate::service_bus::{
    channels,
    wire::{self, WireLimits},
};
use lk_protocol::{livekit as pb, psrpc::internal as bus};
use prost::Message;

struct Events;
impl IoEvents for Events {
    fn notify(&self, _: crate::workers::io_service::IoEvent) {}
}

#[tokio::test]
#[ignore = "requires dedicated AUTO_EGRESS_TEST_REDIS_URL; actual request expiry and held worker wire"]
async fn direct_create_attempt_expiry_bounds_actual_worker_without_deleting_owner() {
    deadline_profile(true).await;
    deadline_profile(false).await;
}

struct Observed {
    request: bus::Request,
    received_ns: i64,
}
fn capture(raw: &[u8], limits: WireLimits) -> Observed {
    Observed {
        request: wire::decode::<bus::Request>(raw, limits)
            .expect("actual StartEgress request wire")
            .message,
        received_ns: std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
            .min(i64::MAX as u128) as i64,
    }
}

async fn deadline_profile(short: bool) {
    let url = std::env::var("AUTO_EGRESS_TEST_REDIS_URL").expect("exclusive broker");
    let (initial, net) = crate::test_support::state().await;
    let mut config = (*initial.config).clone();
    if short {
        config.room.create_room_timeout = rtc_core::config::PolicyDuration::from_millis(100);
        config.room.create_room_attempts = 1;
    } else {
        assert_eq!(
            config.room.create_room_timeout.as_nanos_i64(),
            10_000_000_000
        );
        assert_eq!(config.room.create_room_attempts, 3);
    }
    let limit_ns = if short { 100_000_000 } else { 10_000_000_000 };
    let mut state = crate::state::CompatServices::new(Arc::new(config))
        .assemble(net.clone(), Arc::new(rtc_core::events::NullSink));
    let limits = WireLimits {
        encoded_bytes: 1 << 20,
        decompressed_bytes: 1 << 20,
    };
    let observer = RedisTransport::connect(
        &url,
        TransportLimits {
            wire: limits,
            compression: CompressionPolicy::default(),
            inbox_messages: 32,
            subscriptions: 16,
            commands: 32,
            io_timeout: Duration::from_secs(5),
        },
    )
    .await
    .unwrap();
    let channel = channels::request("EgressInternal", "StartEgress", &[""], false);
    let mut requests = observer.subscribe(&channel, false).await.unwrap();
    let (stop_observer, mut stop) = tokio::sync::oneshot::channel::<()>();
    let (first_send, first_receive) = tokio::sync::oneshot::channel();
    let recorder = tokio::spawn(async move {
        let mut rows = Vec::<Observed>::new();
        let mut first = Some(first_send);
        loop {
            tokio::select! {
                raw=requests.recv()=>{
                    let row=capture(&raw.expect("observer transport must remain live"),limits);
                    if let Some(send)=first.take(){let _=send.send(row.request.clone());}
                    rows.push(row);
                }
                _=&mut stop=>{
                    // Producer PING already completed. This second FIFO PING
                    // on the subscription connection puts all prior real pushes
                    // through the actual callback before returning their bytes.
                    for raw in requests.test_receive_barrier().await.unwrap(){rows.push(capture(&raw,limits));}
                    break;
                }
            }
        }
        (rows, requests)
    });
    let services = WorkerServices::connect(
        WorkerConfig {
            redis: redis::IntoConnectionInfo::into_connection_info(url)
                .unwrap()
                .into(),
            rpc: RpcPolicy::from_raw(0, 3_000_000_000, 2_000_000_000, 1000),
            compression: CompressionPolicy::default(),
            max_decompressed_size: 1 << 20,
            ingress: IngressConfig::default(),
            ingress_legacy_psrpc: false,
        },
        state.manager.clone(),
        None,
        Arc::new(Events),
    )
    .await
    .unwrap();
    state.worker_services = Some(services.clone());
    let name = rtc_core::ids::new_sid("attempt-worker-");
    let key = state.room_key(&name);
    let mut auth = crate::auth::VerifiedToken::default();
    auth.grants.video.room_create = true;
    let body = pb::CreateRoomRequest {
        name: name.clone(),
        egress: Some(pb::RoomEgress {
            room: Some(pb::RoomCompositeEgressRequest {
                audio_only: true,
                file_outputs: vec![pb::EncodedFileOutput {
                    filepath: "held-real-worker.ogg".into(),
                    ..Default::default()
                }],
                ..Default::default()
            }),
            ..Default::default()
        }),
        ..Default::default()
    }
    .encode_to_vec();
    let call_state = state.clone();
    let task = tokio::spawn(async move {
        crate::twirp::room_service::call(
            &call_state,
            Some(&auth),
            "CreateRoom",
            crate::twirp::codec::Codec::Protobuf,
            &body,
        )
        .await
    });
    let request = tokio::time::timeout(Duration::from_secs(2), first_receive)
        .await
        .unwrap()
        .unwrap();
    assert!(request.expiry > request.sent_at);
    assert!(
        request.expiry - request.sent_at <= limit_ns,
        "real nested worker wire must retain CreateRoom attempt expiry"
    );
    assert!(state.manager.room_exists(&key));
    let sid = state
        .manager
        .list_rooms(&key.project, std::slice::from_ref(&key.name))[0]
        .sid
        .clone();
    let result = tokio::time::timeout(Duration::from_secs(15), task)
        .await
        .unwrap()
        .unwrap();
    if short {
        assert_eq!(
            result.unwrap_err().code,
            crate::twirp::error::Code::DeadlineExceeded
        );
    } else {
        // Default source retry sees the actual already-registered same owner;
        // it does not start another Composite or erase first expiry side effect.
        let response = result.unwrap();
        let bytes = axum::body::to_bytes(response.into_body(), 1 << 20)
            .await
            .unwrap();
        let room = lk_protocol::wire::decode::<pb::Room>(&bytes).unwrap();
        assert_eq!(room.sid, sid.0);
    }
    // Do not shutdown workers to manufacture this result. Every accepted direct
    // operation must exit via actual deadline while WorkerServices stays live.
    state.room_creates.close_admission();
    tokio::time::timeout(Duration::from_secs(2), state.room_creates.drain())
        .await
        .expect("expired direct worker must finish independently of global cancellation");
    assert_eq!(services.start_client.pending_count(), 0);
    // Same producer connection FIFO, then same subscription FIFO: no unrelated
    // connection or fixed sleep masquerades as an observation boundary.
    services.start_client.test_command_barrier().await.unwrap();
    stop_observer.send(()).unwrap();
    let (rows, mut requests) = recorder.await.unwrap();
    assert!(!rows.is_empty());
    let first_expiry = request.expiry;
    let first_start =
        lk_protocol::wire::decode::<lk_protocol::rpc::StartEgressRequest>(&request.raw_request)
            .unwrap();
    // Print before the original strict assertions: a failing retry must retain
    // its real observed delta, not lose diagnostics at the panic boundary.
    eprintln!(
        "CreateRoom direct expiry witness {}",
        serde_json::json!({
            "profile": if short { "short100ms-attempt1" } else { "default10s-attempt3" },
            "first_expiry_ns": first_expiry, "request_count": rows.len(),
            "requests": rows.iter().take(64).enumerate().map(|(ordinal, row)| serde_json::json!({
                "ordinal": ordinal, "received_ns": row.received_ns,
                "sent_ns": row.request.sent_at, "expiry_ns": row.request.expiry,
                "expiry_minus_original_ns": row.request.expiry.saturating_sub(first_expiry),
                "sent_minus_original_expiry_ns": row.request.sent_at.saturating_sub(first_expiry)
            })).collect::<Vec<_>>(),
            "witness_truncated": rows.len() > 64
        })
    );
    let mut late_received = 0usize;
    let mut witness = Vec::new();
    for row in &rows {
        let actual = &row.request;
        let start =
            lk_protocol::wire::decode::<lk_protocol::rpc::StartEgressRequest>(&actual.raw_request)
                .unwrap();
        assert_eq!(
            start.egress_id, first_start.egress_id,
            "one original launch identity through valid retries"
        );
        assert_eq!(
            start.room_id, sid.0,
            "every actual retry belongs to original owner SID"
        );
        assert!(
            actual.sent_at <= first_expiry,
            "no newly constructed request after original accepted expiry"
        );
        assert!(
            actual.expiry <= first_expiry,
            "no retry extends original accepted absolute deadline"
        );
        assert!(actual.expiry > actual.sent_at);
        late_received += usize::from(row.received_ns > first_expiry);
        witness.push(serde_json::json!({"received_ns":row.received_ns,"sent_ns":actual.sent_at,"expiry_ns":actual.expiry,
            "same_original_egress":start.egress_id==first_start.egress_id,"same_original_room":start.room_id==sid.0}));
    }
    // Retain every pre-deadline retry, even one received late. These actual
    // rows are never discarded or reclassified as forbidden new work.
    eprintln!(
        "CreateRoom deadline witness {}",
        serde_json::json!({"profile":if short{"short100ms-attempt1"}else{"default10s-attempt3"},
        "first_expiry_ns":first_expiry,"requests":witness,"late_received_preexpiry_requests":late_received})
    );
    assert!(
        tokio::time::timeout(Duration::from_millis(50), requests.recv())
            .await
            .is_err(),
        "no fresh publication after producer/subscription barriers and actual owner drain"
    );
    services.start_client.test_command_barrier().await.unwrap();
    assert!(
        requests.test_receive_barrier().await.unwrap().is_empty(),
        "second ordered barrier observes no unrecorded publish"
    );
    assert_eq!(
        state
            .manager
            .list_rooms(&key.project, std::slice::from_ref(&key.name))[0]
            .sid,
        sid,
        "deadline is not room deletion"
    );
    state.manager.delete_room(&key).await.unwrap();
    state.manager.close_all().await;
    state.shutdown_workers().await;
    state.agents().shutdown().await;
    requests.unsubscribe(&channel).await.unwrap();
    drop(observer);
    net.shutdown_and_wait().await;
}
