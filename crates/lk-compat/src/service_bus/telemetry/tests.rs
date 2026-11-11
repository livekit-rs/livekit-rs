use super::*;
use crate::service_bus::{
    local_transport::LocalTransport,
    redis_transport::TransportLimits,
    rpc::{CallOptions, RpcClient, RpcServer, ServerLimits},
};
use lk_protocol::livekit::AgentDispatch;
use opentelemetry::trace::{SpanContext, SpanId, TraceFlags, TraceId, TraceState};
use opentelemetry_sdk::trace::{InMemorySpanExporter, SdkTracerProvider, SpanData};
use std::{
    sync::atomic::{AtomicUsize, Ordering},
    time::Duration,
};

fn tracing() -> (SdkTracerProvider, InMemorySpanExporter, Context) {
    let exporter = InMemorySpanExporter::default();
    let provider = SdkTracerProvider::builder()
        .with_simple_exporter(exporter.clone())
        .build();
    let context = RpcTracing::from_provider(&provider).scoped_context();
    (provider, exporter, context)
}
fn method(name: &str) -> MethodSpec {
    MethodSpec {
        service: "TraceProduction".into(),
        method: name.into(),
        topics: vec![],
        require_claim: true,
        queue: true,
    }
}
fn limits() -> ServerLimits {
    ServerLimits {
        concurrent: 8,
        max_request_duration: Duration::from_secs(3),
        allow_skip_claim: false,
    }
}
fn options() -> CallOptions {
    // Fixed psrpc/pkg/client/options.go54-65 defaults no-affinity methods to
    // AcceptFirstAvailable and the original one-second selection timeout.
    // This fixture declares queued handlers with no affinity callback; passing
    // the low-level all-zero selection instead waits the entire2s method budget
    // before granting a handler, so it never exercises the tracing middleware.
    let mut options = CallOptions::new(Duration::from_secs(2));
    options.selection.accept_first = true;
    options.selection.affinity_timeout = Some(Duration::from_secs(1));
    options
}
fn parent(context: Context, trace_byte: u8) -> Context {
    context.with_remote_span_context(SpanContext::new(
        TraceId::from_bytes([trace_byte; 16]),
        SpanId::from_bytes([9; 8]),
        TraceFlags::SAMPLED,
        true,
        TraceState::from_key_value([("vendor", "state")]).unwrap(),
    ))
}
fn spans(exporter: &InMemorySpanExporter) -> Vec<SpanData> {
    exporter.get_finished_spans().unwrap()
}
fn attributes(span: &SpanData) -> BTreeMap<String, String> {
    span.attributes
        .iter()
        .map(|kv| (kv.key.to_string(), kv.value.to_string()))
        .collect()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn real_nested_calls_preserve_parent_and_application_metadata_without_leaking_incoming_fields()
 {
    let (_provider, exporter, context) = tracing();
    let local = LocalTransport::new(TransportLimits {
        wire: crate::service_bus::wire::WireLimits {
            encoded_bytes: 1 << 20,
            decompressed_bytes: 1 << 20,
        },
        compression: crate::service_bus::wire::CompressionPolicy {
            quality: 0,
            threshold: 1,
        },
        inbox_messages: 32,
        subscriptions: 100,
        commands: 100,
        io_timeout: Duration::from_secs(3),
    })
    .unwrap();
    let child_spec = method("Child");
    let outer_spec = method("Outer");
    let child = RpcServer::register(
        local.clone(),
        child_spec.clone(),
        limits(),
        |ctx, request: AgentDispatch| async move {
            assert_eq!(
                ctx.metadata.get("outgoing-only").map(String::as_str),
                Some("kept")
            );
            assert!(!ctx.metadata.contains_key("incoming-only"));
            tokio::task::yield_now().await;
            Ok(request)
        },
        None,
    )
    .with_context(context.clone())
    .await
    .unwrap();
    let nested_client = Arc::new({
        let _scope = context.clone().attach();
        RpcClient::new(local.clone(), 8).unwrap()
    });
    let nested_owner = nested_client.clone();
    let outer = RpcServer::register(
        local.clone(),
        outer_spec.clone(),
        limits(),
        move |ctx, request: AgentDispatch| {
            let client = nested_client.clone();
            let spec = child_spec.clone();
            async move {
                assert_eq!(
                    ctx.metadata.get("incoming-only").map(String::as_str),
                    Some("not-forwarded")
                );
                let mut opts = options();
                opts.metadata.insert("outgoing-only".into(), "kept".into());
                tokio::task::yield_now().await;
                client
                    .unary::<_, AgentDispatch>(&spec, &request, opts)
                    .await
            }
        },
        None,
    )
    .with_context(context.clone())
    .await
    .unwrap();
    let client = {
        let _scope = context.clone().attach();
        RpcClient::new(local, 8).unwrap()
    };
    let mut opts = options();
    opts.metadata
        .insert("incoming-only".into(), "not-forwarded".into());
    opts.metadata
        .insert("traceparent".into(), "stale-overridden".into());
    let request = AgentDispatch {
        id: "real-roundtrip".into(),
        ..Default::default()
    };
    let result: AgentDispatch = client
        .unary(&outer_spec, &request, opts)
        .with_context(parent(context, 3))
        .await
        .unwrap();
    assert_eq!(result.id, request.id);
    let output = spans(&exporter);
    assert_eq!(output.len(), 4);
    let outer_sent = output
        .iter()
        .find(|s| s.name == "Sent.TraceProduction.Outer")
        .unwrap();
    let outer_recv = output
        .iter()
        .find(|s| s.name == "Recv.TraceProduction.Outer")
        .unwrap();
    let child_sent = output
        .iter()
        .find(|s| s.name == "Sent.TraceProduction.Child")
        .unwrap();
    let child_recv = output
        .iter()
        .find(|s| s.name == "Recv.TraceProduction.Child")
        .unwrap();
    assert_eq!(outer_sent.parent_span_id, SpanId::from_bytes([9; 8]));
    assert_eq!(outer_recv.parent_span_id, outer_sent.span_context.span_id());
    assert_eq!(child_sent.parent_span_id, outer_recv.span_context.span_id());
    assert_eq!(child_recv.parent_span_id, child_sent.span_context.span_id());
    for s in &output {
        assert_eq!(s.span_context.trace_id(), TraceId::from_bytes([3; 16]));
        assert_eq!(s.instrumentation_scope.name(), "github.com/livekit/psrpc");
        assert_eq!(s.instrumentation_scope.version(), Some("v0.8.0"));
        assert_eq!(s.span_context.trace_state().header(), "vendor=state");
        assert_eq!(s.status, Status::Ok);
        assert_eq!(
            s.events
                .events
                .iter()
                .map(|e| e.name.as_ref())
                .collect::<Vec<_>>(),
            ["Outbound message", "Inbound message"]
        );
    }
    assert_eq!(outer_sent.span_kind, SpanKind::Client);
    assert_eq!(outer_recv.span_kind, SpanKind::Server);
    client.close_and_wait().await;
    outer.shutdown().await;
    nested_owner.close_and_wait().await;
    child.shutdown().await;
    assert!(!Context::current().span().span_context().is_valid());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn retry_attempts_are_siblings_with_exact_error_code_and_unprefixed_message() {
    let (_provider, exporter, context) = tracing();
    let local = LocalTransport::new(TransportLimits {
        wire: crate::service_bus::wire::WireLimits {
            encoded_bytes: 1 << 20,
            decompressed_bytes: 1 << 20,
        },
        compression: crate::service_bus::wire::CompressionPolicy {
            quality: 0,
            threshold: 1,
        },
        inbox_messages: 32,
        subscriptions: 100,
        commands: 100,
        io_timeout: Duration::from_secs(3),
    })
    .unwrap();
    let spec = method("Retry");
    let calls = Arc::new(AtomicUsize::new(0));
    let hits = calls.clone();
    let server = RpcServer::register(
        local.clone(),
        spec.clone(),
        limits(),
        move |_, request: AgentDispatch| {
            let hits = hits.clone();
            async move {
                if hits.fetch_add(1, Ordering::SeqCst) == 0 {
                    Err(RpcError::new("unavailable", "retry this actual handler"))
                } else {
                    Ok(request)
                }
            }
        },
        None,
    )
    .with_context(context.clone())
    .await
    .unwrap();
    let client = {
        let _scope = context.clone().attach();
        RpcClient::new(local, 8).unwrap()
    };
    let request = AgentDispatch::default();
    async {
        let mut retry = crate::workers::retry::GenericRetry::new(
            crate::workers::retry::RpcPolicy::from_raw(2, 1_000_000_000, 0, 1000),
        );
        loop {
            match client
                .unary::<_, AgentDispatch>(&spec, &request, options())
                .await
            {
                Ok(_) => break,
                Err(error) => {
                    assert_eq!(error.code, "unavailable");
                    assert!(retry.next(&error));
                }
            }
        }
    }
    .with_context(parent(context, 4))
    .await;
    let output = spans(&exporter);
    assert_eq!(output.len(), 4);
    assert_eq!(calls.load(Ordering::SeqCst), 2);
    let sent: Vec<_> = output
        .iter()
        .filter(|s| s.span_kind == SpanKind::Client)
        .collect();
    assert_eq!(sent.len(), 2);
    assert_ne!(
        sent[0].span_context.span_id(),
        sent[1].span_context.span_id()
    );
    assert!(
        sent.iter()
            .all(|s| s.parent_span_id == SpanId::from_bytes([9; 8]))
    );
    let errors: Vec<_> = output
        .iter()
        .filter(|s| matches!(s.status, Status::Error { .. }))
        .collect();
    assert_eq!(errors.len(), 2);
    for s in errors {
        assert_eq!(s.status, Status::error("retry this actual handler"));
        let a = attributes(s);
        assert_eq!(a["rpc.psrpc.status_code"], "unavailable");
        assert_eq!(a["rpc.grpc.status_code"], "14");
        assert_eq!(s.events.events.last().unwrap().name, "exception");
        let exception = &s.events.events.last().unwrap().attributes;
        assert!(
            exception
                .iter()
                .any(|kv| kv.key.as_str() == "exception.message"
                    && kv.value.as_str() == "retry this actual handler")
        );
    }
    client.close_and_wait().await;
    server.shutdown().await;
}

#[tokio::test]
async fn multi_client_has_no_sent_but_real_multi_server_handlers_have_recv() {
    let (_provider, exporter, context) = tracing();
    let local = LocalTransport::new(TransportLimits {
        wire: crate::service_bus::wire::WireLimits {
            encoded_bytes: 1 << 20,
            decompressed_bytes: 1 << 20,
        },
        compression: crate::service_bus::wire::CompressionPolicy {
            quality: 0,
            threshold: 1,
        },
        inbox_messages: 32,
        subscriptions: 100,
        commands: 100,
        io_timeout: Duration::from_secs(3),
    })
    .unwrap();
    let mut spec = method("Discover");
    spec.queue = false;
    spec.require_claim = false;
    let server = RpcServer::register_multi(
        local.clone(),
        spec.clone(),
        limits(),
        |_, r: AgentDispatch| async move { Ok(r) },
    )
    .with_context(context.clone())
    .await
    .unwrap();
    let client = {
        let _scope = context.clone().attach();
        RpcClient::new(local, 8).unwrap()
    };
    let mut response = client
        .multi::<_, AgentDispatch>(&spec, &AgentDispatch::default(), options())
        .with_context(parent(context, 5))
        .await
        .unwrap();
    assert!(response.recv().await.unwrap().is_ok());
    response.close().await;
    let output = spans(&exporter);
    assert_eq!(output.len(), 1);
    assert_eq!(output[0].name, "Recv.TraceProduction.Discover");
    client.close_and_wait().await;
    server.shutdown().await;
}

#[tokio::test]
async fn client_close_creates_no_span_and_dropping_running_call_ends_non_success_once() {
    let (_provider, exporter, context) = tracing();
    let local = LocalTransport::new(TransportLimits {
        wire: crate::service_bus::wire::WireLimits {
            encoded_bytes: 1 << 20,
            decompressed_bytes: 1 << 20,
        },
        compression: crate::service_bus::wire::CompressionPolicy {
            quality: 0,
            threshold: 1,
        },
        inbox_messages: 32,
        subscriptions: 100,
        commands: 100,
        io_timeout: Duration::from_secs(3),
    })
    .unwrap();
    let spec = method("Cancel");
    let entered = Arc::new(tokio::sync::Notify::new());
    let notify = entered.clone();
    let server = RpcServer::register(
        local.clone(),
        spec.clone(),
        limits(),
        move |_, _: AgentDispatch| {
            let notify = notify.clone();
            async move {
                notify.notify_one();
                std::future::pending::<Result<AgentDispatch, RpcError>>().await
            }
        },
        None,
    )
    .with_context(context.clone())
    .await
    .unwrap();
    let client = Arc::new({
        let _scope = context.clone().attach();
        RpcClient::new(local, 8).unwrap()
    });
    let c = client.clone();
    let s = spec.clone();
    let task = tokio::spawn(
        async move {
            c.unary::<_, AgentDispatch>(&s, &AgentDispatch::default(), options())
                .await
        }
        .with_context(context),
    );
    tokio::time::timeout(Duration::from_secs(2), entered.notified())
        .await
        .unwrap();
    task.abort();
    let _ = task.await;
    client.close();
    let error = client
        .unary::<_, AgentDispatch>(&spec, &AgentDispatch::default(), options())
        .await
        .unwrap_err();
    assert_eq!(error.code, "canceled");
    server.close().await;
    client.close_and_wait().await;
    let output = spans(&exporter);
    assert_eq!(output.len(), 2);
    assert!(
        output
            .iter()
            .all(|s| matches!(s.status, Status::Error { .. }))
    );
    assert!(
        output
            .iter()
            .all(|s| s.events.events.iter().all(|e| e.name != "Inbound message"))
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn concurrent_actual_calls_never_share_context_across_awaits() {
    let (_provider, exporter, context) = tracing();
    let local = LocalTransport::new(TransportLimits {
        wire: crate::service_bus::wire::WireLimits {
            encoded_bytes: 1 << 20,
            decompressed_bytes: 1 << 20,
        },
        compression: crate::service_bus::wire::CompressionPolicy {
            quality: 0,
            threshold: 1,
        },
        inbox_messages: 32,
        subscriptions: 100,
        commands: 100,
        io_timeout: Duration::from_secs(3),
    })
    .unwrap();
    let spec = method("Concurrent");
    let server = RpcServer::register(
        local.clone(),
        spec.clone(),
        limits(),
        |_, r: AgentDispatch| async move {
            tokio::task::yield_now().await;
            Ok(r)
        },
        None,
    )
    .with_context(context.clone())
    .await
    .unwrap();
    let client = Arc::new({
        let _scope = context.clone().attach();
        RpcClient::new(local, 8).unwrap()
    });
    let mut tasks = tokio::task::JoinSet::new();
    for byte in 10..18 {
        let c = client.clone();
        let s = spec.clone();
        tasks.spawn(
            async move {
                c.unary::<_, AgentDispatch>(&s, &AgentDispatch::default(), options())
                    .await
                    .unwrap();
            }
            .with_context(parent(context.clone(), byte)),
        );
    }
    while let Some(result) = tasks.join_next().await {
        result.unwrap();
    }
    let output = spans(&exporter);
    assert_eq!(output.len(), 16);
    for byte in 10..18 {
        let pair: Vec<_> = output
            .iter()
            .filter(|s| s.span_context.trace_id() == TraceId::from_bytes([byte; 16]))
            .collect();
        assert_eq!(pair.len(), 2);
        let sent = pair
            .iter()
            .find(|s| s.span_kind == SpanKind::Client)
            .unwrap();
        let recv = pair
            .iter()
            .find(|s| s.span_kind == SpanKind::Server)
            .unwrap();
        assert_eq!(recv.parent_span_id, sent.span_context.span_id());
    }
    client.close_and_wait().await;
    server.shutdown().await;
    assert!(!Context::current().span().span_context().is_valid());
}

#[test]
fn noop_provider_preserves_valid_context_and_invalid_carriers_do_not_create_remote_parent() {
    let tracing = RpcTracing::from_provider(&opentelemetry::trace::noop::NoopTracerProvider::new());
    let mut metadata = BTreeMap::from([("application".into(), "kept".into())]);
    let context = parent(Context::new(), 8);
    let _scope = context.attach();
    let span = tracing.client(&method("Noop"), &mut metadata, None);
    assert_eq!(metadata["application"], "kept");
    assert!(metadata["traceparent"].contains(&"08".repeat(16)));
    drop(span);
    let malformed = BTreeMap::from([("traceparent".into(), "not-w3c".into())]);
    let extracted =
        TraceContextPropagator::new().extract_with_context(&Context::new(), &Carrier(&malformed));
    assert!(!extracted.span().span_context().is_valid());
}

fn constructor_bus() -> Arc<LocalTransport> {
    LocalTransport::new(TransportLimits {
        wire: crate::service_bus::wire::WireLimits {
            encoded_bytes: 1 << 20,
            decompressed_bytes: 1 << 20,
        },
        compression: crate::service_bus::wire::CompressionPolicy {
            quality: 0,
            threshold: 1024,
        },
        inbox_messages: 100,
        subscriptions: 100,
        commands: 100,
        io_timeout: Duration::from_secs(3),
    })
    .unwrap()
}

#[tokio::test]
async fn plain_factory_default_zero_does_not_instrument_an_ambient_recorded_parent() {
    let (provider, exporter, _explicit_option) = tracing();
    let parent_span = provider.tracer("ambient-test-only").start("ambient-parent");
    let ambient = Context::new().with_span(parent_span);
    let local = constructor_bus();
    let mut spec = method("JobRequest");
    spec.service = "AgentInternal".into();
    let server = RpcServer::register(
        local.clone(),
        spec.clone(),
        limits(),
        |ctx, request: AgentDispatch| async move {
            assert_eq!(
                ctx.metadata.get("traceparent").map(String::as_str),
                Some("kept-unmodified")
            );
            assert!(
                !Context::current().span().span_context().is_valid(),
                "plain handler did not extract header"
            );
            Ok(request)
        },
        None,
    )
    .with_context(Context::new())
    .await
    .unwrap();
    let client = {
        let _scope = Context::new().attach();
        RpcClient::new(local, 8).unwrap()
    };
    let mut opts = options();
    opts.metadata
        .insert("traceparent".into(), "kept-unmodified".into());
    let reply: AgentDispatch = client
        .unary(&spec, &AgentDispatch::default(), opts)
        .with_context(ambient.clone())
        .await
        .unwrap();
    assert_eq!(reply, AgentDispatch::default());
    ambient.span().end();
    client.close_and_wait().await;
    server.shutdown().await;
    let exported = spans(&exporter);
    assert_eq!(exported.len(), 1);
    assert_eq!(exported[0].name, "ambient-parent");
}

#[tokio::test]
async fn fixed_client_params_two_layers_and_roommanager_zero_are_real_rpc_constructor_policies() {
    let (_provider, exporter, context) = tracing();
    let local = constructor_bus();
    let mut spec = method("DeleteRoom");
    spec.service = "Room".into();
    let server = RpcServer::register(
        local.clone(),
        spec.clone(),
        limits(),
        |_, request: AgentDispatch| async move { Ok(request) },
        None,
    )
    .with_context(Context::new())
    .await
    .unwrap();
    let policy = {
        let _scope = context.clone().attach();
        RpcTracing::production_client_params().excluding_service("RoomManager")
    };
    let client = RpcClient::new(local.clone(), 8)
        .unwrap()
        .with_tracing(policy);
    let _: AgentDispatch = client
        .unary(&spec, &AgentDispatch::default(), options())
        .await
        .unwrap();
    let exported = spans(&exporter);
    assert_eq!(
        exported.len(),
        2,
        "fixed Params.Options plus wire explicit OTel gives two Sent interceptors"
    );
    let outer = exported
        .iter()
        .find(|s| s.parent_span_id == SpanId::INVALID)
        .unwrap();
    let inner = exported
        .iter()
        .find(|s| s.parent_span_id == outer.span_context.span_id())
        .unwrap();
    assert_eq!(outer.name, "Sent.Room.DeleteRoom");
    assert_eq!(inner.name, "Sent.Room.DeleteRoom");
    assert_eq!(outer.span_context.trace_id(), inner.span_context.trace_id());
    assert!(outer.start_time <= inner.start_time && inner.end_time <= outer.end_time);
    for s in &exported {
        assert!(matches!(s.status, Status::Ok));
        assert_eq!(
            s.events
                .events
                .iter()
                .map(|e| e.name.as_ref())
                .collect::<Vec<_>>(),
            ["Outbound message", "Inbound message"]
        );
    }
    let mut roommanager = method("CreateRoom");
    roommanager.service = "RoomManager".into();
    let second = RpcServer::register(
        local,
        roommanager.clone(),
        limits(),
        |_, request: AgentDispatch| async move { Ok(request) },
        None,
    )
    .with_context(Context::new())
    .await
    .unwrap();
    let _: AgentDispatch = client
        .unary(&roommanager, &AgentDispatch::default(), options())
        .await
        .unwrap();
    assert_eq!(
        spans(&exporter).len(),
        2,
        "RoomManager constructor discards Params.Options in fixed Go"
    );
    client.close_and_wait().await;
    server.shutdown().await;
    second.shutdown().await;
}

#[tokio::test]
async fn actual_ioinfo_factory_adds_one_recv_and_keeps_real_not_connected_error() {
    use crate::workers::{io_service::IoInfo, sip_callbacks::CatalogSipCallbacks};
    let (_provider, exporter, context) = tracing();
    let local = constructor_bus();
    let io = IoInfo::new(
        None,
        Arc::new(crate::worker_services::NoRecordingNotifications),
        Arc::new(CatalogSipCallbacks::new(None)),
    );
    let running = io
        .register(local.clone(), limits())
        .with_context(context)
        .await
        .unwrap();
    let client = RpcClient::new(local, 8).unwrap();
    let mut spec = method("ListEgress");
    spec.service = "IOInfo".into();
    let result = client
        .unary::<_, lk_protocol::livekit::ListEgressResponse>(
            &spec,
            &lk_protocol::livekit::ListEgressRequest::default(),
            options(),
        )
        .await;
    let error = result.unwrap_err();
    assert_eq!(error.code, "internal");
    assert!(error.message.contains("egress not connected"));
    client.close_and_wait().await;
    running.shutdown().await;
    let exported = spans(&exporter);
    assert_eq!(
        exported.len(),
        1,
        "real IOInfo register macro opts in; plain caller opts out"
    );
    assert_eq!(exported[0].name, "Recv.IOInfo.ListEgress");
    assert_eq!(exported[0].span_kind, SpanKind::Server);
    assert!(
        matches!(exported[0].status, Status::Error { .. }),
        "source error remains error, never positive media evidence"
    );
}
