//! Real localbus constructor and owned-close consumers. No network, fake handler
//! completion, substituted business registry or injected successful API effects.
use super::*;
use crate::service_bus::{
    local_transport::{LocalSubscription, LocalTransport},
    redis_transport::{TransportError, TransportLimits},
    wire::{CompressionPolicy, WireLimits},
};
use futures_util::{FutureExt, poll};
use lk_protocol::livekit::AgentDispatch;
use std::{
    future::Future,
    pin::Pin,
    sync::atomic::{AtomicUsize, Ordering},
    task::Poll,
};

const CONSTRUCTOR_SOURCE_IDS: [&str; 5] = [
    "default1000",
    "explicit-zero",
    "explicit-three",
    "default1000-concurrent",
    "close-during-blocked-publication",
];
// Shape/provenance only: business outcomes remain compared to actual Rust calls.
fn validate_constructor_source_shape(row: &serde_json::Value, id: &str) {
    fn keys(value: &serde_json::Value, expected: &[&str]) {
        let object = value.as_object().expect("source row/object shape");
        let actual: std::collections::BTreeSet<_> = object.keys().map(String::as_str).collect();
        let expected: std::collections::BTreeSet<_> = expected.iter().copied().collect();
        assert_eq!(actual, expected, "exact original source field inventory");
    }
    assert_eq!(row["id"].as_str(), Some(id));
    if id == "close-during-blocked-publication" {
        keys(
            row,
            &[
                "id",
                "completed_before_reader_release",
                "delivered_id",
                "error",
                "scope",
            ],
        );
        assert!(row["completed_before_reader_release"].is_boolean());
        assert!(row["delivered_id"].is_string());
        assert!(row["error"].is_string());
        assert_eq!(
            row["scope"].as_str(),
            Some(
                "actual synchronous source Publish must settle before closed selection; original request decoded from Reader"
            )
        );
    } else {
        keys(
            row,
            &[
                "id",
                "buffer",
                "client_subscription_sizes",
                "actual_calls",
                "actual_closed_readers",
                "scope",
            ],
        );
        assert!(
            row["buffer"].as_i64().is_some(),
            "source buffer must be an integer"
        );
        let sizes = row["client_subscription_sizes"]
            .as_array()
            .expect("source subscription sizes array");
        assert_eq!(sizes.len(), 2, "two actual constructor subscriptions");
        assert!(
            sizes.iter().all(|n| n.as_u64().is_some()),
            "subscription sizes must be nonnegative integers"
        );
        assert!(row["actual_closed_readers"].as_u64().is_some());
        let call_ids: &[&str] = if id == "default1000-concurrent" {
            &["first", "second"]
        } else {
            &["first"]
        };
        keys(&row["actual_calls"], call_ids);
        for call_id in call_ids {
            let outcome = &row["actual_calls"][*call_id];
            keys(outcome, &["equal", "error"]);
            assert!(outcome["equal"].is_boolean());
            assert!(outcome["error"].is_string());
        }
        assert_eq!(
            row["scope"].as_str(),
            Some(
                "fixed generated typed constructor and actual localbus Request/Claim/Response; echo is not worker assignment"
            )
        );
    }
}

fn constructor_rows(path: std::ffi::OsString) -> Vec<serde_json::Value> {
    let text = std::fs::read_to_string(path).expect("read actual constructor source rows");
    let rows: Vec<serde_json::Value> = text
        .lines()
        .map(|line| serde_json::from_str(line).expect("source JSONL"))
        .collect();
    assert_eq!(rows.len(), CONSTRUCTOR_SOURCE_IDS.len());
    let mut seen = std::collections::BTreeSet::new();
    for (row, id) in rows.iter().zip(CONSTRUCTOR_SOURCE_IDS) {
        assert_eq!(row["id"].as_str(), Some(id));
        assert!(seen.insert(id));
        validate_constructor_source_shape(row, id);
    }
    rows
}
fn constructor_source(id: &str) -> Option<serde_json::Value> {
    let path = std::env::var_os("LOCAL_RPC_CONSTRUCTOR_GO_RESULTS")?;
    Some(
        constructor_rows(path)
            .into_iter()
            .find(|row| row["id"] == id)
            .expect("case inventory"),
    )
}
#[test]
#[ignore = "requires actual original Go constructor observer output; no prefilled values"]
fn actual_constructor_source_five_unique_rows() {
    constructor_rows(
        std::env::var_os("LOCAL_RPC_CONSTRUCTOR_GO_RESULTS").expect("actual source rows required"),
    );
}

fn transport(size: usize) -> Arc<LocalTransport> {
    LocalTransport::new_with_gzip_limit(
        TransportLimits {
            wire: WireLimits {
                encoded_bytes: 1 << 20,
                decompressed_bytes: 1 << 20,
            },
            compression: CompressionPolicy {
                quality: 0,
                threshold: 1,
            },
            inbox_messages: size,
            subscriptions: 100,
            commands: 100,
            io_timeout: Duration::from_secs(3),
        },
        0,
    )
    .unwrap()
}
async fn bounded<T>(future: impl Future<Output = T>) -> T {
    tokio::time::timeout(Duration::from_secs(10), future)
        .await
        .expect("owned constructor/close consumer must settle")
}
fn method(service: &str) -> MethodSpec {
    MethodSpec {
        service: service.into(),
        method: "Echo".into(),
        topics: vec!["real-owner".into()],
        require_claim: true,
        queue: true,
    }
}
fn options() -> CallOptions {
    let mut options = CallOptions::new(Duration::from_secs(3));
    options.selection.accept_first = true;
    options
}
async fn echo(local: Arc<LocalTransport>, spec: MethodSpec, hits: Arc<AtomicUsize>) -> RpcServer {
    RpcServer::register(
        local,
        spec,
        ServerLimits {
            concurrent: 8,
            max_request_duration: Duration::from_secs(3),
            allow_skip_claim: false,
        },
        move |_, request: AgentDispatch| {
            hits.fetch_add(1, Ordering::SeqCst);
            async move { Ok(request) }
        },
        None,
    )
    .await
    .unwrap()
}
// A real full queue holds its group lock. An actual pending Subscribe holds the
// global membership lock. No test-only lock or synthetic readiness is injected.
struct HeldGate {
    channel: channels::Channel,
    reader: LocalSubscription,
    publish: Pin<Box<dyn Future<Output = Result<u64, TransportError>> + Send>>,
    subscribe: Pin<Box<dyn Future<Output = LocalSubscription> + Send>>,
}
impl HeldGate {
    async fn new(local: &Arc<LocalTransport>, name: &str) -> Self {
        let channel = channels::Channel {
            legacy: name.into(),
            ..Default::default()
        };
        let reader = local.subscribe_sized(&channel, true, 1).await;
        local
            .publish_encoded(&channel, b"real-first")
            .await
            .unwrap();
        let bus = local.clone();
        let ch = channel.clone();
        let mut publish = async move { bus.publish_encoded(&ch, b"real-second").await }.boxed();
        assert!(matches!(poll!(publish.as_mut()), Poll::Pending));
        let bus = local.clone();
        let ch = channel.clone();
        let mut subscribe = async move { bus.subscribe_sized(&ch, true, 1).await }.boxed();
        assert!(matches!(poll!(subscribe.as_mut()), Poll::Pending));
        Self {
            channel,
            reader,
            publish,
            subscribe,
        }
    }
    async fn release(mut self) {
        let (closed, published, replacement) = tokio::join!(
            self.reader.unsubscribe(&self.channel),
            self.publish.as_mut(),
            self.subscribe.as_mut(),
        );
        closed.unwrap();
        published.unwrap();
        replacement.unsubscribe(&self.channel).await.unwrap();
    }
}
async fn subscriptions(bus: &RpcBus, client: &RpcClient, service: &str) -> (u64, u64) {
    let response = bus
        .publish(
            &channels::response(service, &client.client_id),
            &pb::Response {
                request_id: "not-a-pending-call".into(),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    let claim = bus
        .publish(
            &channels::claim_request(service, &client.client_id),
            &pb::ClaimRequest {
                request_id: "not-a-pending-call".into(),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    (response, claim)
}

#[tokio::test]
async fn concurrent_first_calls_share_one_real_constructor_and_deliver_each_once() {
    let local = transport(1000);
    let bus = RpcBus::from(local.clone());
    let spec = method("ConstructorConcurrent");
    let hits = Arc::new(AtomicUsize::new(0));
    let server = echo(local.clone(), spec.clone(), hits.clone()).await;
    let gate = HeldGate::new(&local, "constructor-concurrent-global").await;
    let client = RpcClient::new(local, 4).unwrap();
    let first = source_constructor_request("first");
    let second = source_constructor_request("second");
    let mut a = Box::pin(client.unary::<_, AgentDispatch>(&spec, &first, options()));
    let mut b = Box::pin(client.unary::<_, AgentDispatch>(&spec, &second, options()));
    assert!(matches!(poll!(a.as_mut()), Poll::Pending));
    assert!(matches!(poll!(b.as_mut()), Poll::Pending));
    assert_eq!(hits.load(Ordering::SeqCst), 0);
    bounded(gate.release()).await;
    let (a, b) = bounded(async { tokio::join!(a, b) }).await;
    let a = a.unwrap();
    let b = b.unwrap();
    assert_eq!(
        a, first,
        "all protobuf request fields must survive the real call"
    );
    assert_eq!(
        b, second,
        "all protobuf request fields must survive the real call"
    );
    let actual_calls = serde_json::json!({"first":{"equal":a == first,"error":""},"second":{"equal":b == second,"error":""}});
    let actual = (a.id, b.id);
    let instance = client
        .local_clients
        .lock()
        .unwrap()
        .get(&spec.service)
        .unwrap()
        .clone();
    let route = instance.request("concurrent-capacity-probe", true);
    let claim_capacity = route.claims.as_ref().unwrap().capacity();
    let response_capacity = route.responses.capacity();
    drop(route);
    let memberships = bounded(subscriptions(&bus, &client, &spec.service)).await;
    let calls = hits.load(Ordering::SeqCst);
    client.close_and_wait().await;
    server.shutdown().await;
    assert_eq!(actual, ("first".to_owned(), "second".to_owned()));
    assert_eq!(calls, 2);
    assert_eq!(memberships, (1, 1));
    assert_eq!(client.pending_count(), 0);
    if let Some(row) = constructor_source("default1000-concurrent") {
        assert_eq!(row["actual_calls"].as_object().unwrap().len(), calls);
        for id in ["first", "second"] {
            assert_eq!(row["actual_calls"][id]["equal"], true);
            assert_eq!(row["actual_calls"][id]["error"], "");
        }
        assert_eq!(
            row["client_subscription_sizes"].as_array().unwrap().len(),
            2
        );
        assert_eq!(row["buffer"], 1000);
        assert_eq!(bus.limits().inbox_messages, 1000);
        assert_eq!(claim_capacity, 1000);
        assert_eq!(response_capacity, 1);
        assert_eq!(
            row["client_subscription_sizes"],
            serde_json::json!([1000, 1000])
        );
        assert_eq!(
            row["actual_calls"], actual_calls,
            "exact actual source keys, count and complete outcomes"
        );
        assert_eq!(row["actual_closed_readers"], 2);
    }
}

#[tokio::test]
async fn close_during_real_constructor_wait_retires_memberships_without_business_publication() {
    let local = transport(3);
    let bus = RpcBus::from(local.clone());
    let spec = method("ConstructorClose");
    let hits = Arc::new(AtomicUsize::new(0));
    let server = echo(local.clone(), spec.clone(), hits.clone()).await;
    let mut observed_requests = local
        .subscribe_sized(&spec.request_channel(), false, 1)
        .await;
    let gate = HeldGate::new(&local, "constructor-close-global").await;
    let client = RpcClient::new_for_service(local, 4, &spec.service).unwrap();
    let body = AgentDispatch {
        id: "must-not-publish".into(),
        ..Default::default()
    };
    let mut call = Box::pin(client.unary::<_, AgentDispatch>(&spec, &body, options()));
    assert!(matches!(poll!(call.as_mut()), Poll::Pending));
    client.close();
    let mut joined = Box::pin(client.close_and_wait());
    assert!(matches!(poll!(joined.as_mut()), Poll::Pending));
    bounded(gate.release()).await;
    let (error, ()) =
        bounded(async { tokio::join!(async { call.await.unwrap_err() }, joined) }).await;
    let memberships = bounded(subscriptions(&bus, &client, &spec.service)).await;
    let mut observed = Box::pin(observed_requests.recv());
    let no_publication = matches!(poll!(observed.as_mut()), Poll::Pending);
    drop(observed);
    observed_requests
        .unsubscribe(&spec.request_channel())
        .await
        .unwrap();
    server.shutdown().await;
    assert!(
        no_publication,
        "closed host constructor must not publish a business request"
    );
    assert_eq!(error.origin, RpcErrorOrigin::Closed);
    assert_eq!(hits.load(Ordering::SeqCst), 0);
    assert_eq!(memberships, (0, 0));
    assert_eq!(client.pending_count(), 0);
}

#[tokio::test]
async fn close_during_capacity_blocked_publish_preserves_the_original_wire_request() {
    let local = transport(1);
    let bus = RpcBus::from(local.clone());
    let spec = method("CloseBlockedPublication");
    let mut requests = local
        .subscribe_sized(&spec.request_channel(), true, 1)
        .await;
    let client = RpcClient::new_for_service(local.clone(), 4, &spec.service).unwrap();
    client.ready_for_service(&spec.service).await.unwrap();
    bus.publish(
        &spec.request_channel(),
        &pb::Request {
            request_id: "first-real-filler".into(),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    let body = AgentDispatch {
        id: "original-after-close".into(),
        room: "owned".into(),
        ..Default::default()
    };
    let mut call = Box::pin(client.unary::<_, AgentDispatch>(&spec, &body, options()));
    assert!(matches!(poll!(call.as_mut()), Poll::Pending));
    client.close();
    assert!(
        matches!(poll!(call.as_mut()), Poll::Pending),
        "Close must not withdraw source synchronous Publish"
    );
    let first = bounded(requests.recv()).await.unwrap();
    let error = bounded(call).await.unwrap_err();
    let second = bounded(requests.recv()).await.unwrap();
    let delivered = wire::decode::<pb::Request>(&second, local.limits().wire)
        .unwrap()
        .message;
    requests.unsubscribe(&spec.request_channel()).await.unwrap();
    client.close_and_wait().await;
    assert_eq!(
        wire::decode::<pb::Request>(&first, local.limits().wire)
            .unwrap()
            .message
            .request_id,
        "first-real-filler"
    );
    assert_eq!(
        AgentDispatch::decode(delivered.raw_request.as_slice()).unwrap(),
        body
    );
    assert_eq!(error.origin, RpcErrorOrigin::Closed);
    assert_eq!(client.pending_count(), 0);
    if let Some(row) = constructor_source("close-during-blocked-publication") {
        assert_eq!(row["completed_before_reader_release"], false);
        assert_eq!(row["delivered_id"], body.id);
        assert_eq!(row["error"], error.message);
    }
    assert_eq!(
        bounded(subscriptions(&bus, &client, &spec.service)).await,
        (0, 0)
    );
}

#[tokio::test]
async fn canceled_join_keeps_real_owned_completion_for_all_later_joiners() {
    let local = transport(3);
    let bus = RpcBus::from(local.clone());
    let service = "JoinCompletion";
    let client = RpcClient::new_for_service(local.clone(), 4, service).unwrap();
    client.ready_for_service(service).await.unwrap();
    let gate = HeldGate::new(&local, "close-join-global").await;
    client.close();
    let service_client = client
        .local_clients
        .lock()
        .unwrap()
        .get(service)
        .unwrap()
        .clone();
    let mut first = Box::pin(service_client.join());
    assert!(matches!(poll!(first.as_mut()), Poll::Pending));
    drop(first); // cancel only the waiter; actual subscription owner still exists
    let mut second = Box::pin(service_client.join());
    let mut third = Box::pin(service_client.join());
    let pending_second = matches!(poll!(second.as_mut()), Poll::Pending);
    let pending_third = matches!(poll!(third.as_mut()), Poll::Pending);
    bounded(gate.release()).await;
    bounded(async {
        tokio::join!(second, third);
    })
    .await;
    let memberships = bounded(subscriptions(&bus, &client, service)).await;
    assert!(
        pending_second && pending_third,
        "join must not report completion while real Close is blocked"
    );
    assert_eq!(memberships, (0, 0));
}

#[tokio::test]
async fn closing_real_pending_call_releases_a_dispatcher_blocked_on_late_duplicates() {
    let local = transport(1);
    let bus = RpcBus::from(local.clone());
    let mut spec = method("ClosePendingDispatcher");
    spec.require_claim = false;
    let mut requests = local
        .subscribe_sized(&spec.request_channel(), true, 1)
        .await;
    let client = RpcClient::new_for_service(local.clone(), 4, &spec.service).unwrap();
    client.ready_for_service(&spec.service).await.unwrap();
    let body = AgentDispatch {
        id: "actual-late-response".into(),
        ..Default::default()
    };
    let mut call = Box::pin(client.unary::<_, AgentDispatch>(&spec, &body, options()));
    assert!(matches!(poll!(call.as_mut()), Poll::Pending));
    let request = wire::decode::<pb::Request>(
        &bounded(requests.recv()).await.unwrap(),
        local.limits().wire,
    )
    .unwrap()
    .message;
    let ch = channels::response(&spec.service, &request.client_id);
    let response = pb::Response {
        request_id: request.request_id,
        raw_response: request.raw_request,
        ..Default::default()
    };
    // First two source stages: one per-request slot, next dispatcher send. The
    // public caller is deliberately not polled while actual duplicates arrive.
    bounded(bus.publish(&ch, &response)).await.unwrap();
    bounded(bus.publish(&ch, &response)).await.unwrap();
    bounded(bus.publish(&ch, &response)).await.unwrap();
    bounded(bus.publish(&ch, &response)).await.unwrap();
    bounded(bus.publish(&ch, &response)).await.unwrap();
    let mut sixth = Box::pin(bus.publish(&ch, &response));
    assert!(
        matches!(poll!(sixth.as_mut()), Poll::Pending),
        "actual response pipeline must be capacity-blocked"
    );
    client.close();
    // Source select can choose ready response or closure; neither winner is
    // fabricated. The consumer validates every successful original payload.
    let (result, published, ()) =
        bounded(async { tokio::join!(call, sixth, client.close_and_wait()) }).await;
    published.unwrap();
    if let Ok(actual) = &result {
        assert_eq!(actual, &body);
    } else {
        assert_eq!(result.as_ref().unwrap_err().origin, RpcErrorOrigin::Closed);
    }
    client.close_and_wait().await;
    requests.unsubscribe(&spec.request_channel()).await.unwrap();
    assert_eq!(client.pending_count(), 0);
    assert_eq!(
        bounded(subscriptions(&bus, &client, &spec.service)).await,
        (0, 0)
    );
}

#[tokio::test]
async fn configured_source_buffer_profiles_consume_real_claim_grant_and_response() {
    for (configured, effective) in [(1000, 1000), (0, 100), (3, 3)] {
        let policy = crate::workers::retry::RpcPolicy::from_raw(1, 3_000_000_000, 0, configured);
        let size = policy.handler_channel_size().unwrap();
        let local = transport(size);
        let spec = method(&format!("ConfiguredConstructor{configured}"));
        let hits = Arc::new(AtomicUsize::new(0));
        let server = echo(local.clone(), spec.clone(), hits.clone()).await;
        let client = RpcClient::new_for_service(local.clone(), 4, &spec.service).unwrap();
        client.ready_for_service(&spec.service).await.unwrap();
        let instance = client
            .local_clients
            .lock()
            .unwrap()
            .get(&spec.service)
            .unwrap()
            .clone();
        let actual = bounded(client.unary::<_, AgentDispatch>(
            &spec,
            &AgentDispatch {
                id: format!("actual-configured-{configured}"),
                ..Default::default()
            },
            options(),
        ))
        .await
        .unwrap();
        let route = instance.request("actual-capacity-probe", true);
        let claim_size = route.claims.as_ref().unwrap().capacity();
        let response_size = route.responses.capacity();
        drop(route);
        client.close_and_wait().await;
        server.shutdown().await;
        assert_eq!(size, effective);
        assert_eq!(claim_size, effective);
        assert_eq!(response_size, 1);
        assert_eq!(actual.id, format!("actual-configured-{configured}"));
        assert_eq!(hits.load(Ordering::SeqCst), 1);
        assert_eq!(client.pending_count(), 0);
        let id = match configured {
            1000 => "default1000",
            0 => "explicit-zero",
            _ => "explicit-three",
        };
        if let Some(row) = constructor_source(id) {
            assert_eq!(row["buffer"], configured);
            assert_eq!(
                row["client_subscription_sizes"],
                serde_json::json!([effective, effective])
            );
            assert_eq!(row["actual_calls"]["first"]["equal"], true);
            assert_eq!(row["actual_calls"]["first"]["error"], "");
            assert_eq!(row["actual_closed_readers"], 2);
        }
    }
}

// Exact constructor source requests; no success body is prefilled as an outcome.
fn source_constructor_request(id: &str) -> AgentDispatch {
    AgentDispatch {
        id: id.into(),
        room: "owned".into(),
        metadata: "actual-generated-claim-grant-response".into(),
        ..Default::default()
    }
}
#[tokio::test]
async fn source_matched_constructor_capacity_profiles_compare_full_responses_and_exact_call_inventory()
 {
    for (configured, effective, id) in [
        (1000, 1000, "default1000"),
        (0, 100, "explicit-zero"),
        (3, 3, "explicit-three"),
    ] {
        let policy = crate::workers::retry::RpcPolicy::from_raw(1, 3_000_000_000, 0, configured);
        let size = policy.handler_channel_size().unwrap();
        let local = transport(size);
        let spec = method(&format!("SourceMatchedConstructor{configured}"));
        let hits = Arc::new(AtomicUsize::new(0));
        let server = echo(local.clone(), spec.clone(), hits.clone()).await;
        let client = RpcClient::new_for_service(local.clone(), 4, &spec.service).unwrap();
        client.ready_for_service(&spec.service).await.unwrap();
        let request = source_constructor_request("first");
        let actual = bounded(client.unary::<_, AgentDispatch>(&spec, &request, options()))
            .await
            .unwrap();
        let instance = client
            .local_clients
            .lock()
            .unwrap()
            .get(&spec.service)
            .unwrap()
            .clone();
        let route = instance.request("source-capacity-profile-probe", true);
        let actual_claim_capacity = route.claims.as_ref().unwrap().capacity();
        let actual_response_capacity = route.responses.capacity();
        drop(route);
        let actual_calls = serde_json::json!({"first":{"equal":actual==request,"error":""}});
        client.close_and_wait().await;
        server.shutdown().await;
        assert_eq!(
            actual, request,
            "real response must equal every original protobuf field"
        );
        assert_eq!(size, effective);
        assert_eq!(local.limits().inbox_messages, effective);
        assert_eq!(actual_claim_capacity, effective);
        assert_eq!(actual_response_capacity, 1);
        assert_eq!(hits.load(Ordering::SeqCst), 1);
        assert_eq!(client.pending_count(), 0);
        if let Some(row) = constructor_source(id) {
            assert_eq!(row["buffer"], configured);
            assert_eq!(
                row["actual_calls"], actual_calls,
                "exact call keys, multiplicities and source equality/error outcomes"
            );
            assert_eq!(
                row["actual_calls"].as_object().unwrap().len(),
                hits.load(Ordering::SeqCst)
            );
            assert_eq!(
                row["client_subscription_sizes"],
                serde_json::json!([effective, effective])
            );
            assert_eq!(row["actual_closed_readers"], 2);
        }
    }
}
