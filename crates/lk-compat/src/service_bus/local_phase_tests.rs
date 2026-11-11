//! Same existing v3 public RPC APIs; no injected successful business effects.
use super::*;
use crate::service_bus::{
    local_transport::LocalTransport,
    redis_transport::TransportLimits,
    wire::{CompressionPolicy, WireLimits},
};
use futures_util::{pin_mut, poll};
use std::task::Poll;
const SOURCE_IDS: [&str; 12] = [
    "plain-cap-one-and-independent-channel-defaults",
    "raw-and-decoded-size-one-lock-boundary",
    "transport-global-blocked-membership",
    "typed-response-Any-legal-unknown",
    "consumer-internal.Request",
    "consumer-internal.ClaimRequest",
    "consumer-internal.ClaimResponse",
    "consumer-internal.Response",
    "blocked-caller-cancel",
    "blocked-caller-deadline",
    "blocked-method-timeout",
    "persistent-client-completion-global-gate",
];
fn checked_source_rows(path: std::ffi::OsString) -> Vec<serde_json::Value> {
    let text = std::fs::read_to_string(path).expect("read actual fixed-Go JSONL");
    let rows: Vec<serde_json::Value> = text
        .lines()
        .map(|line| serde_json::from_str(line).expect("actual source JSONL row"))
        .collect();
    assert_eq!(
        rows.len(),
        SOURCE_IDS.len(),
        "actual source inventory must be complete"
    );
    let mut seen = std::collections::BTreeSet::new();
    for (row, want) in rows.iter().zip(SOURCE_IDS) {
        let id = row["id"].as_str().expect("actual source string case ID");
        assert!(
            seen.insert(id.to_owned()),
            "duplicate actual source case ID {id}"
        );
        assert_eq!(
            id, want,
            "unexpected, missing or reordered actual source case ID"
        );
    }
    rows
}
fn source_row(id: &str) -> Option<serde_json::Value> {
    let path = std::env::var_os("LOCAL_RPC_V4_GO_RESULTS")?;
    Some(
        checked_source_rows(path)
            .into_iter()
            .find(|row| row["id"] == id)
            .unwrap_or_else(|| panic!("missing actual source row {id}")),
    )
}
fn compare_source_legal(id: &str) {
    if let Some(row) = source_row(id) {
        assert_eq!(row["success"], true);
        assert_eq!(row["equal"], true);
        assert!(row["injected_count"].as_u64().unwrap() > 0);
        assert_eq!(row["error"], "");
    }
}
fn compare_source_blocked(mode: &str, delivered: &str, error: &RpcError, expired: bool) {
    if let Some(row) = source_row(&format!("blocked-{mode}")) {
        assert_eq!(row["completed_before_reader_release"], false);
        assert_eq!(row["delivered_id"], delivered);
        assert_eq!(row["error"], error.message);
        assert_eq!(row["delivered_after_original_expiry"], expired);
    }
}
#[test]
#[ignore = "requires actual fixed-Go v4 observer JSONL plus execution of this whole module with same env"]
fn actual_go_v4_row_inventory() {
    let path =
        std::env::var_os("LOCAL_RPC_V4_GO_RESULTS").expect("actual fixed-Go results required");
    let rows = checked_source_rows(path);
    assert_eq!(rows.len(), 12);
}

fn local() -> Arc<LocalTransport> {
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
            inbox_messages: 1,
            subscriptions: 100,
            commands: 100,
            io_timeout: Duration::from_secs(3),
        },
        0,
    )
    .unwrap()
}
fn spec() -> MethodSpec {
    MethodSpec {
        service: "LocalPhaseV4".into(),
        method: "Call".into(),
        topics: vec!["owned".into()],
        require_claim: true,
        queue: true,
    }
}
// Use only pre-v5 public APIs. A real warm-up request/timeout finishes client
// initialization before any phase-specific blocked-publication assertion.
async fn warmed_client(
    local: &Arc<LocalTransport>,
    spec: &MethodSpec,
    requests: &mut crate::service_bus::local_transport::LocalSubscription,
) -> RpcClient {
    let client = RpcClient::new(local.clone(), 4).unwrap();
    let request = lk_protocol::livekit::AgentDispatch {
        id: "actual-warmup".into(),
        ..Default::default()
    };
    let error = client
        .unary::<_, lk_protocol::livekit::AgentDispatch>(
            spec,
            &request,
            CallOptions::new(Duration::from_millis(2)),
        )
        .await
        .unwrap_err();
    assert!(error.is_local_no_response());
    let actual = wire::decode::<pb::Request>(&requests.recv().await.unwrap(), local.limits().wire)
        .unwrap()
        .message;
    assert_eq!(
        lk_protocol::livekit::AgentDispatch::decode(actual.raw_request.as_slice()).unwrap(),
        request
    );
    client
}
async fn envelope<T: wire::BusMessage>(
    bus: &RpcBus,
    ch: &channels::Channel,
    message: &T,
    unknown: bool,
) {
    let mut bytes = message.encode_to_vec();
    if unknown {
        bytes.extend_from_slice(&[0x08, 7]);
    }
    let bytes = wire::encode_raw::<T>(
        &bytes,
        &ch.local,
        bus.limits().compression,
        bus.limits().wire,
    )
    .unwrap();
    bus.publish_encoded(ch, &bytes).await.unwrap();
}
// Both requests and grants reach the real common server and real handler. The
// raw body remains attached to the request; generated-only envelope second
// decode would instead silently time out before the handler.
#[tokio::test]
async fn legal_request_and_grant_unknown_reach_real_registered_handler() {
    for corrupt_request in [true, false] {
        let local = local();
        let bus = RpcBus::from(local.clone());
        let spec = spec();
        let observed = Arc::new(Mutex::new(Vec::new()));
        let out = observed.clone();
        let server = RpcServer::register_with_raw(
            local.clone(),
            spec.clone(),
            ServerLimits {
                concurrent: 4,
                max_request_duration: Duration::from_secs(3),
                allow_skip_claim: false,
            },
            move |_, request: RawRequest<lk_protocol::livekit::AgentDispatch>| {
                let out = out.clone();
                async move {
                    out.lock().unwrap().push(request.raw.to_vec());
                    Ok(request.message)
                }
            },
            None,
        )
        .await
        .unwrap();
        let claim_ch = channels::claim_request(&spec.service, "wire-client");
        let response_ch = channels::response(&spec.service, "wire-client");
        let mut claims = bus
            .subscribe::<pb::ClaimRequest>(&claim_ch, false)
            .await
            .unwrap();
        let mut responses = bus
            .subscribe::<pb::Response>(&response_ch, false)
            .await
            .unwrap();
        let body = lk_protocol::livekit::AgentDispatch {
            id: "AD_real_handler".into(),
            room: "owned".into(),
            metadata: "original body".into(),
            ..Default::default()
        };
        let raw = body.encode_to_vec();
        let request = pb::Request {
            request_id: "actual-request".into(),
            client_id: "wire-client".into(),
            sent_at: unix_ns(),
            expiry: unix_ns() + 3_000_000_000,
            raw_request: raw.clone(),
            ..Default::default()
        };
        envelope(&bus, &spec.request_channel(), &request, corrupt_request).await;
        let bytes = tokio::time::timeout(Duration::from_secs(3), claims.recv())
            .await
            .unwrap()
            .unwrap();
        // Claims emitted by the actual server are ordinary generated envelopes.
        let claim = wire::decode::<pb::ClaimRequest>(&bytes, bus.limits().wire)
            .unwrap()
            .message;
        envelope(
            &bus,
            &spec.claim_channel(),
            &pb::ClaimResponse {
                request_id: claim.request_id,
                server_id: claim.server_id,
            },
            !corrupt_request,
        )
        .await;
        let bytes = tokio::time::timeout(Duration::from_secs(3), responses.recv())
            .await
            .unwrap()
            .unwrap();
        let response = received_response(&bytes, bus.limits().wire).unwrap();
        assert!(response.message.error.is_empty());
        assert_eq!(
            lk_protocol::livekit::AgentDispatch::decode(response.message.raw_response.as_slice())
                .unwrap(),
            body
        );
        assert_eq!(*observed.lock().unwrap(), vec![raw]);
        compare_source_legal(if corrupt_request {
            "consumer-internal.Request"
        } else {
            "consumer-internal.ClaimResponse"
        });
        claims.unsubscribe(&claim_ch).await.unwrap();
        responses.unsubscribe(&response_ch).await.unwrap();
        server.shutdown().await;
    }
}
#[tokio::test]
async fn legal_claim_unknown_selects_actual_client_and_emits_real_grant() {
    let local = local();
    let bus = RpcBus::from(local.clone());
    let spec = spec();
    let mut requests = local
        .subscribe_sized(&spec.request_channel(), true, 1)
        .await;
    let mut grants = bus
        .subscribe::<pb::ClaimResponse>(&spec.claim_channel(), false)
        .await
        .unwrap();
    let client = Arc::new(RpcClient::new(local.clone(), 4).unwrap());
    let task = {
        let client = client.clone();
        let spec = spec.clone();
        tokio::spawn(async move {
            let mut options = CallOptions::new(Duration::from_secs(3));
            options.selection.accept_first = true;
            client
                .unary::<_, lk_protocol::livekit::AgentDispatch>(
                    &spec,
                    &lk_protocol::livekit::AgentDispatch {
                        id: "AD_claim".into(),
                        ..Default::default()
                    },
                    options,
                )
                .await
        })
    };
    let request = wire::decode::<pb::Request>(&requests.recv().await.unwrap(), bus.limits().wire)
        .unwrap()
        .message;
    envelope(
        &bus,
        &channels::claim_request(&spec.service, &request.client_id),
        &pb::ClaimRequest {
            request_id: request.request_id.clone(),
            server_id: "wire-observer".into(),
            affinity: 1.0,
            handling: false,
        },
        true,
    )
    .await;
    let grant = tokio::time::timeout(Duration::from_secs(3), grants.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        wire::decode::<pb::ClaimResponse>(&grant, bus.limits().wire)
            .unwrap()
            .message
            .server_id,
        "wire-observer"
    );
    bus.publish(
        &channels::response(&spec.service, &request.client_id),
        &pb::Response {
            request_id: request.request_id,
            server_id: "wire-observer".into(),
            raw_response: request.raw_request,
            ..Default::default()
        },
    )
    .await
    .unwrap();
    assert_eq!(task.await.unwrap().unwrap().id, "AD_claim");
    compare_source_legal("consumer-internal.ClaimRequest");
    assert_eq!(client.pending_count(), 0);
    requests.unsubscribe(&spec.request_channel()).await.unwrap();
    grants.unsubscribe(&spec.claim_channel()).await.unwrap();
    client.close();
}
#[tokio::test]
async fn cancelled_capacity_blocked_publish_still_delivers_original_request_before_context_selection()
 {
    let local = local();
    let bus = RpcBus::from(local.clone());
    let spec = spec();
    let ch = spec.request_channel();
    let mut requests = local.subscribe_sized(&ch, true, 1).await;
    let client = warmed_client(&local, &spec, &mut requests).await;
    bus.publish(
        &ch,
        &pb::Request {
            request_id: "filler".into(),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    let options = CallOptions::new(Duration::from_secs(3));
    let cancel = options.cancel.clone();
    let request = lk_protocol::livekit::AgentDispatch {
        id: "AD_after_cancel".into(),
        ..Default::default()
    };
    let call = client.unary::<_, lk_protocol::livekit::AgentDispatch>(&spec, &request, options);
    pin_mut!(call);
    assert!(matches!(poll!(call.as_mut()), Poll::Pending));
    cancel.cancel();
    assert!(
        matches!(poll!(call.as_mut()), Poll::Pending),
        "caller cancellation must not withdraw synchronous source local Publish"
    );
    let filler = requests.recv().await.unwrap();
    assert_eq!(
        wire::decode::<pb::Request>(&filler, bus.limits().wire)
            .unwrap()
            .message
            .request_id,
        "filler"
    );
    // Poll publication through completion and source selection, then prove actual
    // original delivery, not a task-only cancel receipt or fabricated handler.
    let error = call.await.unwrap_err();
    assert!(error.is_local_no_response());
    let delivered = wire::decode::<pb::Request>(&requests.recv().await.unwrap(), bus.limits().wire)
        .unwrap()
        .message;
    assert_eq!(
        lk_protocol::livekit::AgentDispatch::decode(delivered.raw_request.as_slice()).unwrap(),
        request
    );
    assert_eq!(client.pending_count(), 0);
    compare_source_blocked(
        "caller-cancel",
        &request.id,
        &error,
        delivered.expiry < unix_ns(),
    );
    requests.unsubscribe(&ch).await.unwrap();
    client.close();
}
#[tokio::test]
async fn deadline_during_capacity_blocked_publish_does_not_withdraw_original_envelope() {
    let local = local();
    let bus = RpcBus::from(local.clone());
    let spec = spec();
    let ch = spec.request_channel();
    let mut requests = local.subscribe_sized(&ch, true, 1).await;
    let client = warmed_client(&local, &spec, &mut requests).await;
    bus.publish(
        &ch,
        &pb::Request {
            request_id: "filler".into(),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    let mut options = CallOptions::new(Duration::from_secs(3));
    options.deadline = Some(Instant::now() + Duration::from_millis(30));
    let request = lk_protocol::livekit::AgentDispatch {
        id: "AD_after_deadline".into(),
        ..Default::default()
    };
    let call = client.unary::<_, lk_protocol::livekit::AgentDispatch>(&spec, &request, options);
    pin_mut!(call);
    assert!(matches!(poll!(call.as_mut()), Poll::Pending));
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert!(matches!(poll!(call.as_mut()), Poll::Pending));
    requests.recv().await.unwrap();
    let error = call.await.unwrap_err();
    assert!(error.is_local_no_response());
    let delivered = wire::decode::<pb::Request>(&requests.recv().await.unwrap(), bus.limits().wire)
        .unwrap()
        .message;
    assert!(delivered.expiry < unix_ns());
    compare_source_blocked("caller-deadline", &request.id, &error, true);
    assert_eq!(
        lk_protocol::livekit::AgentDispatch::decode(delivered.raw_request.as_slice()).unwrap(),
        request
    );
    requests.unsubscribe(&ch).await.unwrap();
    client.close();
}

#[tokio::test]
async fn cancelled_registration_wait_does_not_withdraw_subscribe_or_later_request() {
    let local = local();
    let bus = RpcBus::from(local.clone());
    let spec = spec();
    let request_ch = spec.request_channel();
    let mut requests = local.subscribe_sized(&request_ch, true, 1).await;
    let client = warmed_client(&local, &spec, &mut requests).await;
    let held = channels::Channel {
        legacy: "v4-global-held".into(),
        ..Default::default()
    };
    let reader = local.subscribe_sized(&held, true, 1).await;
    local.publish_encoded(&held, b"filler").await.unwrap();
    let blocked = local.publish_encoded(&held, b"second");
    pin_mut!(blocked);
    assert!(matches!(poll!(blocked.as_mut()), Poll::Pending));
    let registering = local.subscribe_sized(&held, true, 1);
    pin_mut!(registering);
    assert!(matches!(poll!(registering.as_mut()), Poll::Pending));
    let options = CallOptions::new(Duration::from_secs(3));
    let cancel = options.cancel.clone();
    let request = lk_protocol::livekit::AgentDispatch {
        id: "AD_after_registration".into(),
        ..Default::default()
    };
    let call = client.unary::<_, lk_protocol::livekit::AgentDispatch>(&spec, &request, options);
    pin_mut!(call);
    assert!(matches!(poll!(call.as_mut()), Poll::Pending));
    cancel.cancel();
    assert!(
        matches!(poll!(call.as_mut()), Poll::Pending),
        "source synchronous membership must finish before caller ctx selection"
    );
    // The source call runs in its own goroutine. Keep this already-polled
    // future driven while releasing the real FIFO RwLock waiters; otherwise
    // its granted read reservation would prevent unsubscribe from completing.
    let (closed, published, replacement, error) =
        tokio::join!(reader.unsubscribe(&held), blocked, registering, async {
            call.await.unwrap_err()
        },);
    closed.unwrap();
    published.unwrap();
    replacement.unsubscribe(&held).await.unwrap();
    assert!(error.is_local_no_response());
    let actual = wire::decode::<pb::Request>(&requests.recv().await.unwrap(), bus.limits().wire)
        .unwrap()
        .message;
    assert_eq!(
        lk_protocol::livekit::AgentDispatch::decode(actual.raw_request.as_slice()).unwrap(),
        request
    );
    requests.unsubscribe(&request_ch).await.unwrap();
    client.close();
}

#[tokio::test]
async fn relative_method_timeout_starts_after_actual_local_request_publish() {
    let local = local();
    let bus = RpcBus::from(local.clone());
    let spec = spec();
    let ch = spec.request_channel();
    let mut requests = local.subscribe_sized(&ch, true, 1).await;
    let client = warmed_client(&local, &spec, &mut requests).await;
    bus.publish(
        &ch,
        &pb::Request {
            request_id: "filler".into(),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    let options = CallOptions::new(Duration::from_millis(30));
    let request = lk_protocol::livekit::AgentDispatch {
        id: "AD_method_timeout".into(),
        ..Default::default()
    };
    let call = client.unary::<_, lk_protocol::livekit::AgentDispatch>(&spec, &request, options);
    pin_mut!(call);
    assert!(matches!(poll!(call.as_mut()), Poll::Pending));
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert!(matches!(poll!(call.as_mut()), Poll::Pending));
    requests.recv().await.unwrap();
    assert!(
        matches!(poll!(call.as_mut()), Poll::Pending),
        "relative timeout starts only after source synchronous Publish"
    );
    let delivered = wire::decode::<pb::Request>(&requests.recv().await.unwrap(), bus.limits().wire)
        .unwrap()
        .message;
    let error = call.await.unwrap_err();
    assert!(error.is_local_no_response());
    assert!(delivered.expiry < unix_ns());
    compare_source_blocked("method-timeout", &request.id, &error, true);
    assert_eq!(
        lk_protocol::livekit::AgentDispatch::decode(delivered.raw_request.as_slice()).unwrap(),
        request
    );
    requests.unsubscribe(&ch).await.unwrap();
    client.close();
}

// Original v4 APIs only. Three real response publications prove the first is
// in the client's existing bounded decoded/pending stages before the unrelated
// global membership lock is blocked. Assertions occur after owned cleanup.
#[tokio::test]
async fn queued_response_completion_does_not_close_service_subscription_behind_unrelated_global_gate()
 {
    let local = local();
    let bus = RpcBus::from(local.clone());
    let spec = spec();
    let request_ch = spec.request_channel();
    let mut requests = local.subscribe_sized(&request_ch, true, 1).await;
    let client = warmed_client(&local, &spec, &mut requests).await;
    let mut no_claim = spec.clone();
    no_claim.require_claim = false;
    let request = lk_protocol::livekit::AgentDispatch {
        id: "AD_queued_completion".into(),
        ..Default::default()
    };
    let call = client.unary::<_, lk_protocol::livekit::AgentDispatch>(
        &no_claim,
        &request,
        CallOptions::new(Duration::from_secs(3)),
    );
    pin_mut!(call);
    assert!(matches!(poll!(call.as_mut()), Poll::Pending));
    let actual = wire::decode::<pb::Request>(&requests.recv().await.unwrap(), bus.limits().wire)
        .unwrap()
        .message;
    let response_ch = channels::response(&spec.service, &actual.client_id);
    let response = pb::Response {
        request_id: actual.request_id,
        server_id: "actual-wire-observer".into(),
        raw_response: actual.raw_request,
        ..Default::default()
    };
    for _ in 0..3 {
        bus.publish(&response_ch, &response).await.unwrap();
    }
    tokio::task::yield_now().await;
    let held = channels::Channel {
        legacy: "v5-completion-global-held".into(),
        ..Default::default()
    };
    let reader = local.subscribe_sized(&held, true, 1).await;
    local.publish_encoded(&held, b"filler").await.unwrap();
    let blocked = local.publish_encoded(&held, b"second");
    pin_mut!(blocked);
    assert!(matches!(poll!(blocked.as_mut()), Poll::Pending));
    let registering = local.subscribe_sized(&held, true, 1);
    pin_mut!(registering);
    assert!(matches!(poll!(registering.as_mut()), Poll::Pending));
    let completion = match poll!(call.as_mut()) {
        Poll::Ready(result) => Some(result),
        Poll::Pending => None,
    };
    let completed_before_release = completion.is_some();
    let (closed, published, replacement) =
        tokio::join!(reader.unsubscribe(&held), blocked, registering);
    closed.unwrap();
    published.unwrap();
    replacement.unsubscribe(&held).await.unwrap();
    let result = match completion {
        Some(result) => result,
        None => call.await,
    };
    let result = result.unwrap();
    assert_eq!(result, request);
    assert_eq!(client.pending_count(), 0);
    requests.unsubscribe(&request_ch).await.unwrap();
    client.close();
    assert!(
        completed_before_release,
        "source completed request only removes pending map, it must not close persistent service subscriptions"
    );
    if let Some(row) = source_row("persistent-client-completion-global-gate") {
        assert_eq!(
            row["completed_before_global_release"],
            completed_before_release
        );
        assert_eq!(row["equal"], true);
        assert_eq!(row["error"], "");
        assert_eq!(row["additional_subscriptions_second_call"], 0);
    }
}

#[tokio::test]
async fn persistent_local_response_root_wrongkind_preserves_original_any_children() {
    let local = local();
    let bus = RpcBus::from(local.clone());
    let spec = spec();
    let mut requests = local
        .subscribe_sized(&spec.request_channel(), true, 1)
        .await;
    let client = Arc::new(RpcClient::new(local.clone(), 4).unwrap());
    let mut no_claim = spec.clone();
    no_claim.require_claim = false;
    let task = {
        let client = client.clone();
        tokio::spawn(async move {
            client
                .unary::<_, lk_protocol::livekit::AgentDispatch>(
                    &no_claim,
                    &lk_protocol::livekit::AgentDispatch::default(),
                    CallOptions::new(Duration::from_secs(3)),
                )
                .await
        })
    };
    let request = wire::decode::<pb::Request>(&requests.recv().await.unwrap(), bus.limits().wire)
        .unwrap()
        .message;
    let response = pb::Response {
        request_id: request.request_id,
        error: "actual remote error".into(),
        code: "invalid_argument".into(),
        ..Default::default()
    };
    let mut bytes = response.encode_to_vec();
    bytes.extend_from_slice(&[0x08, 7, 0x42, 5, 0x08, 7, 0x12, 1, b'a']);
    let ch = channels::response(&spec.service, &request.client_id);
    let wire = wire::encode_raw::<pb::Response>(
        &bytes,
        &ch.local,
        bus.limits().compression,
        bus.limits().wire,
    )
    .unwrap();
    bus.publish_encoded(&ch, &wire).await.unwrap();
    let error = task.await.unwrap().unwrap_err();
    assert!(error.is_remote());
    assert_eq!(error.code, "invalid_argument");
    assert_eq!(
        error.error_detail_wires().unwrap(),
        &[vec![0x12, 1, b'a', 0x08, 7]]
    );
    compare_source_legal("consumer-internal.Response");
    requests.unsubscribe(&spec.request_channel()).await.unwrap();
    client.close_and_wait().await;
}
