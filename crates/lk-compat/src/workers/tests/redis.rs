//! Actual Redis tests; coordinator supplies a dedicated isolated database.
use super::super::{
    io_service::{IoEvent, IoEvents, IoInfo},
    preserved::WireValue,
    store::IoStore,
};
use lk_protocol::{livekit as pb, rpc as internal};
use prost::Message;
use std::{
    sync::{Arc, Mutex},
    time::Duration,
};
#[derive(Default)]
struct Events(Mutex<Vec<IoEvent>>);
impl IoEvents for Events {
    fn notify(&self, event: IoEvent) {
        self.0.lock().unwrap().push(event);
    }
}
async fn connection() -> (Arc<IoStore>, redis::aio::MultiplexedConnection) {
    assert!(
        matches!(
            std::env::var("WORKERS_REDIS_ISOLATED").as_deref(),
            Ok("1" | "true")
        ),
        "test must own a dedicated database"
    );
    let url = std::env::var("WORKERS_REDIS_URL").expect("WORKERS_REDIS_URL");
    let client = redis::Client::open(url.as_str()).unwrap();
    let mut connection = client.get_multiplexed_async_connection().await.unwrap();
    redis::cmd("FLUSHDB")
        .query_async::<()>(&mut connection)
        .await
        .unwrap();
    let store = Arc::new(
        IoStore::connect(
            redis::IntoConnectionInfo::into_connection_info(url)
                .unwrap()
                .into(),
            Duration::from_secs(3),
        )
        .await
        .unwrap(),
    );
    (store, connection)
}
fn marker(raw: &mut Vec<u8>) {
    raw.extend_from_slice(&[0xc0, 0xa3, 0x09, 0x2a]);
} // unknown field19000 varint42
#[tokio::test]
#[ignore = "requires coordinator-owned WORKERS_REDIS_URL isolated Redis"]
async fn real_redis_store_retains_unknowns_generation_order_indexes_and_version_marker() {
    let (store, mut redis) = connection().await;
    store.start("1.13.10").await.unwrap();
    assert_eq!(
        redis::cmd("GET")
            .arg("livekit_version")
            .query_async::<String>(&mut redis)
            .await
            .unwrap(),
        "1.13.10"
    );
    store.start("1.12.0").await.unwrap();
    assert_eq!(
        redis::cmd("GET")
            .arg("livekit_version")
            .query_async::<String>(&mut redis)
            .await
            .unwrap(),
        "1.13.10"
    );
    let info = pb::EgressInfo {
        egress_id: "EG_f06_fixture".into(),
        room_name: "room-fixture".into(),
        status: 1,
        ..Default::default()
    };
    let mut raw = info.encode_to_vec();
    marker(&mut raw);
    let value = WireValue::<pb::EgressInfo>::decode(&raw).unwrap();
    store.store_egress_wire(&value).await.unwrap();
    assert_eq!(
        store
            .load_egress_wire(&info.egress_id)
            .await
            .unwrap()
            .encode(),
        raw
    );
    assert_eq!(
        store.list_egress("room-fixture", true).await.unwrap().len(),
        1
    );
    let ended = value.with_message(pb::EgressInfo {
        status: 3,
        ended_at: 1,
        ..info.clone()
    });
    store.update_egress_wire(&ended).await.unwrap();
    assert!(
        store
            .list_egress("room-fixture", true)
            .await
            .unwrap()
            .is_empty()
    );
    store
        .clean_ended_egress(25 * 60 * 60 * 1_000_000_000)
        .await
        .unwrap();
    assert!(store.load_egress(&info.egress_id).await.is_err());
    let ingress = pb::IngressInfo {
        ingress_id: "IN_f06_fixture".into(),
        stream_key: "public-fixture-key".into(),
        room_name: "old-room".into(),
        ..Default::default()
    };
    store.store_ingress(&ingress).await.unwrap();
    let mut next = ingress.clone();
    next.room_name = "new-room".into();
    store.update_ingress(&next).await.unwrap();
    assert_eq!(
        redis::cmd("SISMEMBER")
            .arg("room_{ingress}:old-room")
            .arg(&ingress.ingress_id)
            .query_async::<i64>(&mut redis)
            .await
            .unwrap(),
        0
    );
    assert_eq!(
        redis::cmd("SISMEMBER")
            .arg("room_{ingress}:new-room")
            .arg(&ingress.ingress_id)
            .query_async::<i64>(&mut redis)
            .await
            .unwrap(),
        1
    );
    let state = pb::IngressState {
        started_at: 20,
        updated_at: 30,
        status: 2,
        ..Default::default()
    };
    let mut raw = state.encode_to_vec();
    marker(&mut raw);
    store
        .update_ingress_state_wire(&ingress.ingress_id, &WireValue::decode(&raw).unwrap())
        .await
        .unwrap();
    let stale = pb::IngressState {
        updated_at: 29,
        status: 1,
        ..state.clone()
    };
    store
        .update_ingress_state(&ingress.ingress_id, &stale)
        .await
        .unwrap();
    let loaded = store
        .load_ingress_from_stream_key_wire(&ingress.stream_key)
        .await
        .unwrap();
    assert_eq!(loaded.message.state.as_ref(), Some(&state));
    assert_eq!(loaded.wire.nested("state").unwrap().encode_to_vec(), raw);
    assert!(
        store
            .update_ingress_state(
                &ingress.ingress_id,
                &pb::IngressState {
                    started_at: 19,
                    updated_at: 999,
                    ..Default::default()
                }
            )
            .await
            .is_err()
    );
    store.delete_ingress(&next).await.unwrap();
    assert!(store.load_ingress(&ingress.ingress_id).await.is_err());
}
#[tokio::test]
#[ignore = "requires coordinator-owned WORKERS_REDIS_URL isolated Redis"]
async fn actual_io_stale_same_session_no_write_still_emits_requested_transition_but_old_session_errors()
 {
    let (store, _redis) = connection().await;
    let events = Arc::new(Events::default());
    let sip = Arc::new(super::super::sip_callbacks::CatalogSipCallbacks::new(None));
    let io = IoInfo::new(Some(store.clone()), events.clone(), sip);
    let info = pb::IngressInfo {
        ingress_id: "IN_io_fixture".into(),
        stream_key: "public-key".into(),
        ..Default::default()
    };
    io.create_ingress(info.clone()).await.unwrap();
    let new = pb::IngressState {
        started_at: 50,
        updated_at: 100,
        status: 2,
        ..Default::default()
    };
    io.update_ingress_state(internal::UpdateIngressStateRequest {
        ingress_id: info.ingress_id.clone(),
        state: Some(new.clone()),
    })
    .await
    .unwrap();
    events.0.lock().unwrap().clear();
    let old_update = pb::IngressState {
        updated_at: 99,
        status: 1,
        ..new.clone()
    };
    io.update_ingress_state(internal::UpdateIngressStateRequest {
        ingress_id: info.ingress_id.clone(),
        state: Some(old_update.clone()),
    })
    .await
    .unwrap();
    assert_eq!(
        store.load_ingress(&info.ingress_id).await.unwrap().state,
        Some(new.clone())
    );
    {
        let events_seen = events.0.lock().unwrap();
        assert_eq!(events_seen.len(), 1);
        assert!(
            matches!(&events_seen[0],IoEvent::IngressUpdated(info)if info.state.as_ref()==Some(&old_update))
        );
    }
    events.0.lock().unwrap().clear();
    assert!(
        io.update_ingress_state(internal::UpdateIngressStateRequest {
            ingress_id: info.ingress_id.clone(),
            state: Some(pb::IngressState {
                started_at: 49,
                ..new
            })
        })
        .await
        .is_err()
    );
    assert!(events.0.lock().unwrap().is_empty());
}

#[tokio::test]
#[ignore = "requires coordinator-owned WORKERS_REDIS_URL isolated Redis"]
async fn actual_gateway_handler_claim_is_granted_before_deadline_without_skip_claim() {
    use crate::{
        auth::{KeyStore, TokenClaims, TokenMinter},
        service_bus::{
            redis_transport::{RedisTransport, TransportLimits},
            rpc::{CallOptions, MethodSpec, RpcClient, RpcError, RpcServer, ServerLimits},
            wire::{CompressionPolicy, WireLimits},
        },
        workers::egress::{EgressGateway, EgressRooms},
    };
    use futures_util::future::BoxFuture;
    use std::sync::atomic::{AtomicUsize, Ordering};
    struct Rooms;
    impl EgressRooms for Rooms {
        fn room_sid(&self, _: &str) -> BoxFuture<'_, Result<String, RpcError>> {
            Box::pin(async { Err(RpcError::new("not_found", "unused room fixture")) })
        }
        fn layout_metadata(
            &self,
            _: &str,
            _: &str,
            _: String,
        ) -> BoxFuture<'_, Result<(), RpcError>> {
            Box::pin(async { Err(RpcError::new("not_found", "unused room fixture")) })
        }
    }
    let (store, _redis) = connection().await;
    let events = Arc::new(Events::default());
    let io = IoInfo::new(
        Some(store.clone()),
        events,
        Arc::new(super::super::sip_callbacks::CatalogSipCallbacks::new(None)),
    );
    let info = pb::EgressInfo {
        egress_id: "EG_claim_fixture".into(),
        status: 1,
        ..Default::default()
    };
    store.store_egress(&info).await.unwrap();
    let url = std::env::var("WORKERS_REDIS_URL").unwrap();
    let bus = RedisTransport::connect(
        &url,
        TransportLimits {
            wire: WireLimits {
                encoded_bytes: 1 << 20,
                decompressed_bytes: 1 << 20,
            },
            compression: CompressionPolicy {
                quality: 0,
                threshold: 0,
            },
            inbox_messages: 100,
            subscriptions: 32,
            commands: 32,
            io_timeout: Duration::from_secs(3),
        },
    )
    .await
    .unwrap();
    let calls = Arc::new(AtomicUsize::new(0));
    let observed = calls.clone();
    let reply = info.clone();
    let server = RpcServer::register::<pb::UpdateStreamRequest, pb::EgressInfo, _, _>(
        bus.clone(),
        MethodSpec {
            service: "EgressHandler".into(),
            method: "UpdateStream".into(),
            topics: vec![info.egress_id.clone()],
            require_claim: true,
            queue: true,
        },
        ServerLimits {
            concurrent: 8,
            max_request_duration: Duration::MAX,
            allow_skip_claim: false,
        },
        move |_, request| {
            let calls = observed.clone();
            let info = reply.clone();
            async move {
                assert_eq!(request.egress_id, info.egress_id);
                calls.fetch_add(1, Ordering::SeqCst);
                Ok(info)
            }
        },
        None,
    )
    .await
    .unwrap();
    let client = Arc::new(RpcClient::new(bus.clone(), 32).unwrap());
    let gateway = EgressGateway::new(
        Some(client.clone()),
        io,
        Arc::new(Rooms),
        Duration::from_secs(3),
    );
    let keys = Arc::new(KeyStore::new(vec![(
        "public-fixture-key".into(),
        "public-fixture-secret".into(),
    )]));
    let token = TokenMinter::new(keys.clone())
        .mint(
            "public-fixture-key",
            &TokenClaims {
                video: Some(rtc_core::permission::VideoGrant {
                    room_record: true,
                    ..Default::default()
                }),
                ..Default::default()
            },
            Duration::from_secs(60),
            rtc_core::util::unix_ms() / 1000,
        )
        .unwrap();
    let auth = crate::auth::authenticate_token(&keys, Some(&token))
        .unwrap()
        .unwrap();
    // This is an actual protocol fixture, not a claim of real media recording.
    // Source's no-affinity caller grants the first eligible claim immediately.
    // The old false/None policy never grants this default-no-skip worker.
    let result = tokio::time::timeout(
        Duration::from_secs(2),
        gateway.update_stream(
            Some(&auth),
            pb::UpdateStreamRequest {
                egress_id: info.egress_id.clone(),
                ..Default::default()
            },
            CallOptions::new(Duration::from_secs(3)),
        ),
    )
    .await
    .expect("source no-affinity policy must grant the real handler before its overall deadline")
    .unwrap();
    assert_eq!(result, info);
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    client.close();
    server.close().await;
    assert_eq!(bus.active_subscriptions(), 0);
}
