//! Development-only real Redis/psrpc probe; never linked into the product server.
use base64::{Engine, engine::general_purpose::STANDARD};
use lk_compat::service_bus::redis_transport::{RedisTransport, TransportError, TransportLimits};
use lk_compat::service_bus::rpc::{
    CallOptions, MethodSpec, RpcClient, RpcError, RpcServer, ServerLimits,
};
use lk_compat::service_bus::{channels, wire};
use lk_protocol::psrpc::internal as pb;
use serde_json::json;
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};
use std::time::{Duration, Instant};

fn limits() -> TransportLimits {
    TransportLimits {
        wire: wire::WireLimits {
            encoded_bytes: 1024 * 1024,
            decompressed_bytes: 1024 * 1024,
        },
        compression: wire::CompressionPolicy {
            quality: 6,
            threshold: 128,
        },
        inbox_messages: 64,
        subscriptions: 64,
        commands: 32,
        io_timeout: Duration::from_secs(2),
    }
}
fn spec(topic: &str, claim: bool, queue: bool) -> MethodSpec {
    MethodSpec {
        service: "CompatEcho".into(),
        method: "Echo".into(),
        topics: vec![topic.into()],
        require_claim: claim,
        queue,
    }
}
fn options(timeout: Duration, queue: bool) -> CallOptions {
    let mut options = CallOptions::new(timeout);
    options.metadata.insert("synthetic".into(), "yes".into());
    options.selection.affinity_timeout = Some(timeout / 2);
    options.selection.short_circuit_timeout = Some(Duration::from_millis(50));
    if !queue {
        options.selection.maximum_affinity = 1.0;
    }
    options
}
fn output(
    path: Option<&String>,
    value: serde_json::Value,
) -> Result<(), Box<dyn std::error::Error>> {
    let bytes = serde_json::to_vec_pretty(&value)?;
    if let Some(path) = path {
        std::fs::write(path, bytes)?;
    } else {
        println!("{}", String::from_utf8(bytes)?);
    }
    Ok(())
}

async fn server(
    bus: Arc<RedisTransport>,
    method: MethodSpec,
    label: String,
    affinity: f32,
    skip: bool,
    calls: Arc<AtomicUsize>,
) -> Result<RpcServer, RpcError> {
    let bid = if method.queue {
        None
    } else {
        Some(Arc::new(move |_: &String| affinity) as Arc<dyn Fn(&String) -> f32 + Send + Sync>)
    };
    RpcServer::register::<String, String, _, _>(
        bus,
        method,
        ServerLimits {
            concurrent: 16,
            max_request_duration: Duration::from_secs(10),
            allow_skip_claim: skip,
        },
        move |context, value: String| {
            let label = label.clone();
            let calls = calls.clone();
            async move {
                calls.fetch_add(1, Ordering::SeqCst);
                if context.metadata.get("synthetic").map(String::as_str) != Some("yes") {
                    return Err(RpcError::new("invalid_argument", "metadata missing"));
                }
                if value == "error" {
                    return Err(RpcError::new("invalid_argument", "synthetic remote error"));
                }
                if let Some(delay) = value.strip_prefix("sleep:") {
                    tokio::time::sleep(Duration::from_millis(delay.parse().unwrap_or(0))).await;
                }
                Ok(format!("{label}:{value}"))
            }
        },
        bid,
    )
    .await
}

async fn selftest(url: &str) -> Result<serde_json::Value, Box<dyn std::error::Error>> {
    let bus = RedisTransport::connect(url, limits()).await?;
    let unique = rtc_core::ids::new_sid("probe_");
    let channel = channels::request("TransportProbe", "Echo", &[&unique], false);
    let mut first = bus.subscribe(&channel, false).await?;
    let mut second = bus.subscribe(&channel, false).await?;
    let packet = pb::Request {
        request_id: "synthetic-wire".into(),
        ..Default::default()
    };
    assert_eq!(bus.publish(&channel, &packet).await?, 2);
    for sub in [&mut first, &mut second] {
        let bytes = tokio::time::timeout(Duration::from_secs(1), sub.recv()).await??;
        assert_eq!(
            wire::decode::<pb::Request>(&bytes, limits().wire)?.message,
            packet
        );
    }
    first.unsubscribe(&channel).await?;
    second.unsubscribe(&channel).await?;
    assert_eq!(bus.active_subscriptions(), 0);
    let channel = channels::request("TransportProbe", "Queue", &[&unique], true);
    let mut first = bus.subscribe(&channel, true).await?;
    let mut second = bus.subscribe(&channel, true).await?;
    let encoded = wire::encode(
        &packet,
        &channel.local,
        wire::CompressionPolicy::default(),
        limits().wire,
    )?;
    let lease_started = Instant::now();
    bus.publish_encoded(&channel, &encoded).await?;
    let (a, b) = tokio::join!(
        tokio::time::timeout(Duration::from_millis(150), first.recv()),
        tokio::time::timeout(Duration::from_millis(150), second.recv())
    );
    assert_eq!(
        usize::from(matches!(a, Ok(Ok(_)))) + usize::from(matches!(b, Ok(Ok(_)))),
        1,
        "one actual SETNX owner"
    );
    let lease_acquired_by = Instant::now();
    let redis = redis::Client::open(url)?;
    let mut connection = redis.get_multiplexed_async_connection().await?;
    let ttl_query_started = Instant::now();
    let ttl: i64 = redis::cmd("PTTL")
        .arg(STANDARD.encode(Sha256::digest(&encoded)))
        .query_async(&mut connection)
        .await?;
    let ttl_query_ended = Instant::now();
    let lower = 5000 - ttl_query_ended.duration_since(lease_started).as_millis() as i64 - 1;
    let upper = 5000
        - ttl_query_started
            .duration_since(lease_acquired_by)
            .as_millis() as i64
        + 1;
    assert!(
        ttl > 0 && ttl >= lower && ttl <= upper,
        "actual Redis lease {ttl}ms outside measured five-second bracket {lower}..{upper}"
    );

    bus.publish_encoded(&channel, &encoded).await?;
    let (a, b) = tokio::join!(
        tokio::time::timeout(Duration::from_millis(100), first.recv()),
        tokio::time::timeout(Duration::from_millis(100), second.recv())
    );
    assert!(
        a.is_err() && b.is_err(),
        "same packet cannot acquire twice within five-second lease"
    );
    first.unsubscribe(&channel).await?;
    second.unsubscribe(&channel).await?;
    let calls = Arc::new(AtomicUsize::new(0));
    for claim in [false, true] {
        let method = spec(&format!("{unique}-{claim}"), claim, false);
        let running = server(
            bus.clone(),
            method.clone(),
            "rust".into(),
            1.0,
            false,
            calls.clone(),
        )
        .await?;
        let client = RpcClient::new(bus.clone(), 4)?;
        let baseline = bus.active_subscriptions();
        let response: String = client
            .unary(
                &method,
                &"hello".to_string(),
                options(Duration::from_secs(1), false),
            )
            .await?;
        assert_eq!(response, "rust:hello");
        let error = client
            .unary::<String, String>(
                &method,
                &"error".to_string(),
                options(Duration::from_secs(1), false),
            )
            .await
            .unwrap_err();
        assert_eq!(error.code, "invalid_argument");
        let error = client
            .unary::<String, String>(
                &method,
                &"sleep:500".to_string(),
                options(Duration::from_millis(60), false),
            )
            .await
            .unwrap_err();
        assert_eq!(error.code, "deadline_exceeded");
        assert_eq!(client.pending_count(), 0);
        assert_eq!(bus.active_subscriptions(), baseline);
        let canceled = options(Duration::from_secs(1), false);
        canceled.cancel.cancel();
        assert_eq!(
            client
                .unary::<String, String>(&method, &"hello".to_string(), canceled)
                .await
                .unwrap_err()
                .code,
            "canceled"
        );
        assert_eq!(client.pending_count(), 0);
        assert_eq!(bus.active_subscriptions(), baseline);
        // Abandon an actually-polled call. Its map entry and subscription work
        // are owned by the future, not an immortal background request task.
        let slow = "sleep:500".to_string();
        let mut abandoned = Box::pin(client.unary::<String, String>(
            &method,
            &slow,
            options(Duration::from_secs(1), false),
        ));
        tokio::select! {result=&mut abandoned=>panic!("unexpected early result {result:?}"),_=tokio::time::sleep(Duration::from_millis(10))=>{}}
        assert_eq!(client.pending_count(), 1);
        drop(abandoned);
        assert_eq!(client.pending_count(), 0);
        assert_eq!(bus.active_subscriptions(), baseline);
        running.close().await;
    }
    let client = RpcClient::new(bus.clone(), 2)?;
    let method = spec(&format!("{unique}-missing"), true, false);
    let error = client
        .unary::<String, String>(
            &method,
            &"hello".to_string(),
            options(Duration::from_millis(200), false),
        )
        .await
        .unwrap_err();
    assert_eq!(error.code, "unavailable");
    assert_eq!(error.message, "no response from servers");
    assert_eq!(client.pending_count(), 0);
    assert_eq!(bus.active_subscriptions(), 0);
    let calls = Arc::new(AtomicUsize::new(0));
    let method = spec(&format!("{unique}-affinity"), true, false);
    let low = server(
        bus.clone(),
        method.clone(),
        "low".into(),
        0.25,
        false,
        calls.clone(),
    )
    .await?;
    let high = server(
        bus.clone(),
        method.clone(),
        "high".into(),
        1.0,
        false,
        calls.clone(),
    )
    .await?;
    let response: String = client
        .unary(
            &method,
            &"select".to_string(),
            options(Duration::from_secs(1), false),
        )
        .await?;
    assert_eq!(response, "high:select");
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    low.close().await;
    high.close().await;
    // Force a bounded slow consumer without guessing a delay for readiness.
    let mut small = limits();
    small.inbox_messages = 1;
    let small = RedisTransport::connect(url, small).await?;
    let channel = channels::request("TransportProbe", "Overflow", &[&unique], false);
    let subscription = small.subscribe(&channel, false).await?;
    for index in 0..4 {
        small
            .publish(
                &channel,
                &pb::Request {
                    request_id: format!("safe-{index}"),
                    ..Default::default()
                },
            )
            .await?;
    }
    let until = Instant::now() + Duration::from_secs(1);
    while subscription.failure().is_none() && Instant::now() < until {
        tokio::task::yield_now().await;
    }
    assert_eq!(subscription.failure(), Some(TransportError::Overflow));
    drop(subscription);
    assert_eq!(small.active_subscriptions(), 0);
    Ok(
        json!({"status":"PASS","scope":"real Redis transport and synthetic RPC lifecycle, not worker/API execution","ack_ready_fanout":true,"queue_setnx_single_acquisition":true,"queue_duplicate_lease_blocked":true,"typed_echo_error_timeout_cancel_drop_cleanup":true,"no_responder":true,"affinity_single_execution":true,"bounded_push_overflow":true,"pending":client.pending_count()}),
    )
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut args = std::env::args().skip(1);
    let mode = args.next().ok_or("mode required")?;
    let mut flags = BTreeMap::new();
    while let Some(key) = args.next() {
        let value = args.next().ok_or("flag value required")?;
        flags.insert(key, value);
    }
    let url = flags
        .get("--redis-url")
        .map(String::as_str)
        .unwrap_or("redis://127.0.0.1:6379/");
    let result = flags.get("--result");
    if mode == "selftest" {
        output(result, selftest(url).await?)?;
        return Ok(());
    }
    let bus = RedisTransport::connect(url, limits()).await?;
    let queue = flags.get("--queue").is_some_and(|v| v == "true");
    let claim = flags.get("--claim").is_none_or(|v| v == "true");
    let topic = flags
        .get("--topic")
        .map(String::as_str)
        .unwrap_or("room.東京😀");
    let mut method = spec(topic, claim, queue);
    if let Some(value) = flags.get("--service") {
        method.service = value.clone();
    }
    if let Some(value) = flags.get("--method") {
        method.method = value.clone();
    }
    let timeout = Duration::from_millis(
        flags
            .get("--timeout-ms")
            .map(|v| v.parse())
            .transpose()?
            .unwrap_or(3000),
    );
    match mode.as_str() {
        "serve" => {
            let calls = Arc::new(AtomicUsize::new(0));
            let running = server(
                bus,
                method,
                flags.get("--label").cloned().unwrap_or("rust".into()),
                flags
                    .get("--affinity")
                    .map(|v| v.parse())
                    .transpose()?
                    .unwrap_or(1.0),
                flags.get("--skip-claim").is_some_and(|v| v == "true"),
                calls.clone(),
            )
            .await?;
            output(
                flags.get("--ready"),
                json!({"ready":true,"subscriptions_acknowledged":true,"scope":"synthetic psrpc server only"}),
            )?;
            let lifetime = Duration::from_millis(
                flags
                    .get("--lifetime-ms")
                    .map(|v| v.parse())
                    .transpose()?
                    .unwrap_or(30000),
            );
            tokio::select! {_=tokio::time::sleep(lifetime)=>{},signal=tokio::signal::ctrl_c()=>{signal?;}}
            running.close().await;
            output(
                result,
                json!({"status":"STOPPED","actual_handler_calls":calls.load(Ordering::SeqCst)}),
            )?;
        }
        "call" => {
            let client = RpcClient::new(bus, 8)?;
            let value = if let Some(bytes) = flags.get("--payload-bytes") {
                "x".repeat(bytes.parse()?)
            } else {
                flags.get("--value").cloned().unwrap_or("hello".into())
            };
            let response: Result<String, RpcError> =
                client.unary(&method, &value, options(timeout, queue)).await;
            match response {
                Ok(response) => {
                    if flags.contains_key("--expect-code") {
                        return Err("expected error, received success".into());
                    }
                    let label = flags
                        .get("--expect-label")
                        .map(String::as_str)
                        .unwrap_or("go");
                    assert_eq!(response, format!("{label}:{value}"));
                    output(
                        result,
                        json!({"status":"PASS","actual_response_bytes":response.len(),"actual_response_sha256":format!("{:x}",Sha256::digest(response.as_bytes())),"pending":client.pending_count()}),
                    )?;
                }
                Err(error) => {
                    if flags.get("--expect-code").map(String::as_str) != Some(error.code.as_str()) {
                        return Err(error.into());
                    }
                    output(
                        result,
                        json!({"status":"PASS","expected_error_code":error.code,"pending":client.pending_count()}),
                    )?;
                }
            }
        }
        _ => return Err("unknown mode".into()),
    }
    Ok(())
}
