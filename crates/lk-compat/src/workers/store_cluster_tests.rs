//! Uses a coordinator-owned three-primary Redis Cluster. The original IOInfo
//! worker callbacks travel over the real broker. No worker process is emulated.
use super::*;
use crate::service_bus::{
    redis_transport::{RedisTransport, TransportLimits},
    rpc::{CallOptions, MethodSpec, RpcClient, ServerLimits},
    wire::{CompressionPolicy, WireLimits},
};
use crate::workers::{
    io_service::{IoEvent, IoEvents, IoInfo},
    sip_callbacks::CatalogSipCallbacks,
};
use lk_protocol::rpc as internal;
use std::sync::{Arc, Mutex as StdMutex};

#[derive(Default)]
struct Events(StdMutex<Vec<IoEvent>>);

impl IoEvents for Events {
    fn notify(&self, event: IoEvent) {
        self.0.lock().unwrap().push(event);
    }
}

fn io_method(method: &str) -> MethodSpec {
    MethodSpec {
        service: "IOInfo".into(),
        method: method.into(),
        topics: vec![],
        require_claim: true,
        queue: true,
    }
}

fn io_options() -> CallOptions {
    let mut options = CallOptions::new(Duration::from_secs(3));
    options.selection.accept_first = true;
    options
}

#[tokio::test]
#[ignore = "requires coordinator-owned three-primary Redis Cluster"]
async fn actual_cluster_ingress_worker_callbacks_store_indexes_and_preserve_watch_conflicts() {
    assert_eq!(
        std::env::var("WORKERS_REDIS_CLUSTER_ISOLATED").as_deref(),
        Ok("1"),
        "the test must own a dedicated cluster"
    );
    let seeds: Vec<_> = std::env::var("WORKERS_REDIS_CLUSTER_URLS")
        .expect("WORKERS_REDIS_CLUSTER_URLS")
        .split(',')
        .map(|url| redis::IntoConnectionInfo::into_connection_info(url.trim()).unwrap())
        .collect();
    assert!(seeds.len() >= 3, "requires three actual fixture seeds");
    // A non-owner first seed must still route to the actual slot master.
    let endpoint = RedisEndpoint::cluster(seeds, 8);
    let store = Arc::new(
        IoStore::connect(endpoint.clone(), Duration::from_secs(3))
            .await
            .unwrap(),
    );
    let bus = RedisTransport::connect_endpoint(
        endpoint.clone(),
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
    let events = Arc::new(Events::default());
    let io = IoInfo::new(
        Some(store.clone()),
        events.clone(),
        Arc::new(CatalogSipCallbacks::new(None)),
    );
    let server = io
        .register(
            bus.clone(),
            ServerLimits {
                concurrent: 8,
                max_request_duration: Duration::MAX,
                allow_skip_claim: false,
            },
        )
        .await
        .unwrap();
    let client = RpcClient::new(bus.clone(), 32).unwrap();

    let mut info = pb::IngressInfo {
        ingress_id: "IN_cluster_watch".into(),
        stream_key: "cluster-watch-fixture-stream".into(),
        room_name: "cluster-watch-room-one".into(),
        ..Default::default()
    };
    let create: internal::CreateIngressResponse = client
        .unary(&io_method("CreateIngress"), &info, io_options())
        .await
        .expect("real IOInfo CreateIngress must store both transactions on Cluster");
    assert_eq!(create.info, Some(info.clone()));
    let original_room = info.room_name.clone();
    assert_eq!(store.list_ingress(&original_room).await.unwrap().len(), 1);
    info.room_name = "cluster-watch-room-two".into();
    store.update_ingress(&info).await.unwrap();
    assert!(store.list_ingress(&original_room).await.unwrap().is_empty());
    assert_eq!(store.list_ingress(&info.room_name).await.unwrap().len(), 1);

    let config = redis::AsyncConnectionConfig::new()
        .set_connection_timeout(Some(Duration::from_secs(3)))
        .set_response_timeout(Some(Duration::from_secs(3)));
    let state_key = format!("{INGRESS_STATE}{}", info.ingress_id);
    let mut watched = endpoint
        .watch_connection(&config, &state_key)
        .await
        .unwrap();
    let live_state = pb::IngressState {
        status: 2,
        started_at: 20,
        updated_at: 30,
        ..Default::default()
    };
    let update = |state| internal::UpdateIngressStateRequest {
        ingress_id: info.ingress_id.clone(),
        state: Some(state),
    };
    client
        .unary::<_, ()>(
            &io_method("UpdateIngressState"),
            &update(live_state.clone()),
            io_options(),
        )
        .await
        .unwrap();
    let conflicted: Option<Vec<redis::Value>> = redis::pipe()
        .atomic()
        .cmd("SET")
        .arg(&state_key)
        .arg(pb::IngressState::default().encode_to_vec())
        .query_async(&mut watched)
        .await
        .unwrap();
    assert!(
        conflicted.is_none(),
        "separate write must abort the held WATCH"
    );
    drop(watched);

    client
        .unary::<_, ()>(
            &io_method("UpdateIngressState"),
            &update(pb::IngressState {
                status: 1,
                updated_at: 29,
                ..live_state.clone()
            }),
            io_options(),
        )
        .await
        .unwrap();
    let error = client
        .unary::<_, ()>(
            &io_method("UpdateIngressState"),
            &update(pb::IngressState {
                started_at: 19,
                updated_at: 999,
                ..live_state.clone()
            }),
            io_options(),
        )
        .await
        .unwrap_err();
    assert_eq!(error.code, "failed_precondition");

    let request = internal::GetIngressInfoRequest {
        stream_key: info.stream_key.clone(),
        ..Default::default()
    };
    let read: internal::GetIngressInfoResponse = client
        .unary(&io_method("GetIngressInfo"), &request, io_options())
        .await
        .unwrap();
    let actual = read.info.unwrap();
    assert_eq!(actual.ingress_id, info.ingress_id);
    assert_eq!(actual.room_name, info.room_name);
    assert_eq!(actual.state, Some(live_state.clone()));
    assert!(events.0.lock().unwrap().iter().any(|event| {
        matches!(event, IoEvent::IngressStarted(entry) if entry.state.as_ref() == Some(&live_state))
    }));

    store.delete_ingress(&info).await.unwrap();
    assert!(
        store
            .list_ingress(&info.room_name)
            .await
            .unwrap()
            .is_empty()
    );
    assert!(
        store
            .load_ingress(&info.ingress_id)
            .await
            .unwrap_err()
            .is_ingress_not_found()
    );
    assert!(
        store
            .load_ingress_from_stream_key(&info.stream_key)
            .await
            .unwrap_err()
            .is_ingress_not_found()
    );
    let mut connection = endpoint.connect(&config).await.unwrap();
    assert!(
        !redis::cmd("EXISTS")
            .arg(state_key)
            .query_async::<bool>(&mut connection)
            .await
            .unwrap()
    );
    drop(client);
    server.shutdown().await;
    assert_eq!(bus.active_subscriptions(), 0);
}

#[tokio::test]
#[ignore = "requires fresh coordinator-owned WORKERS_REDIS_CLUSTER_URLS and WORKERS_REDIS_ISOLATED=1"]
async fn actual_cluster_egress_store_update_and_cleanup_route_original_keys() {
    assert_eq!(std::env::var("WORKERS_REDIS_ISOLATED").as_deref(), Ok("1"));
    let urls: Vec<String> = serde_json::from_str(
        &std::env::var("WORKERS_REDIS_CLUSTER_URLS")
            .expect("actual three-master Cluster URLs JSON"),
    )
    .unwrap();
    assert!(urls.len() >= 3);
    let nodes = urls
        .into_iter()
        .map(|url| redis::IntoConnectionInfo::into_connection_info(url).unwrap())
        .collect();
    let store = IoStore::connect(RedisEndpoint::cluster(nodes, 2), Duration::from_secs(3))
        .await
        .unwrap();
    let id = rtc_core::ids::new_sid("EG_");
    let room = rtc_core::ids::new_sid("cluster-room-");
    let mut info = pb::EgressInfo {
        egress_id: id.clone(),
        room_name: room.clone(),
        status: pb::EgressStatus::EgressStarting as i32,
        ..Default::default()
    };
    store.store_egress(&info).await.unwrap();
    assert_eq!(store.load_egress(&id).await.unwrap(), info);
    assert_eq!(
        store.list_egress(&room, true).await.unwrap(),
        vec![info.clone()]
    );
    info.status = pb::EgressStatus::EgressComplete as i32;
    info.ended_at = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos() as i64;
    store.update_egress(&info).await.unwrap();
    assert_eq!(store.load_egress(&id).await.unwrap(), info);
    assert!(store.list_egress(&room, true).await.unwrap().is_empty());
    store
        .clean_ended_egress(info.ended_at + 24 * 60 * 60 * 1_000_000_000 + 1)
        .await
        .unwrap();
    let absent = store.load_egress(&id).await.unwrap_err();
    assert_eq!(absent.code, "not_found");
    assert_eq!(absent.message, "egress does not exist");
    assert!(store.list_egress(&room, false).await.unwrap().is_empty());
    // Ordinary pipeline semantics: a command error does not suppress an
    // independent command. Only a fresh dedicated Cluster may exercise this
    // global source key with an intentionally wrong type.
    let bad_id = rtc_core::ids::new_sid("EG_");
    let bad_room = rtc_core::ids::new_sid("cluster-error-");
    let mut c = store.connection.lock().await;
    let exists: bool = redis::cmd("EXISTS")
        .arg(EGRESS)
        .query_async(&mut *c)
        .await
        .unwrap();
    assert!(!exists, "dedicated fixture must contain no other egress");
    redis::cmd("SET")
        .arg(EGRESS)
        .arg("fixture-wrong-type")
        .query_async::<()>(&mut *c)
        .await
        .unwrap();
    drop(c);
    assert!(
        store
            .store_egress(&pb::EgressInfo {
                egress_id: bad_id.clone(),
                room_name: bad_room.clone(),
                ..Default::default()
            })
            .await
            .is_err()
    );
    let mut c = store.connection.lock().await;
    let indexed: bool = redis::cmd("SISMEMBER")
        .arg(format!("{ROOM_EGRESS}{bad_room}"))
        .arg(&bad_id)
        .query_async(&mut *c)
        .await
        .unwrap();
    assert!(
        indexed,
        "source ordinary pipeline still executes second command after first error"
    );
    for key in [EGRESS.to_owned(), format!("{ROOM_EGRESS}{bad_room}")] {
        redis::cmd("DEL")
            .arg(key)
            .query_async::<()>(&mut *c)
            .await
            .unwrap();
    }
}
