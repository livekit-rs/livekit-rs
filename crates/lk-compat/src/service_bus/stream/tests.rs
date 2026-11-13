use super::*;
use crate::service_bus::{
    local_transport::LocalTransport,
    redis_transport::{RedisTransport, TransportLimits},
    wire::{CompressionPolicy, WireLimits},
};
use lk_protocol::livekit as lk;
fn limits() -> TransportLimits {
    TransportLimits {
        wire: WireLimits {
            encoded_bytes: 1 << 20,
            decompressed_bytes: 1 << 20,
        },
        compression: CompressionPolicy::default(),
        inbox_messages: 100,
        subscriptions: 100,
        commands: 100,
        io_timeout: Duration::from_secs(2),
    }
}
fn config() -> StreamConfig {
    StreamConfig {
        timeout: Duration::from_millis(300),
        channel_size: 1,
        max_streams: 8,
    }
}
fn spec(claim: bool) -> MethodSpec {
    MethodSpec {
        service: "StreamSourceTest".into(),
        method: "Exchange".into(),
        topics: vec![id("topic_")],
        require_claim: claim,
        queue: false,
    }
}
fn options() -> CallOptions {
    let mut options = CallOptions::new(Duration::from_secs(2));
    options.selection.accept_first = true;
    options
}
fn request(value: &str) -> lk::Room {
    lk::Room {
        name: value.into(),
        ..Default::default()
    }
}
async fn exchange(bus: RpcBus, claimed: bool) {
    let method = spec(claimed);
    let mut listener =
        register_stream::<lk::Room, lk::Room>(bus.clone(), method.clone(), config(), None)
            .await
            .unwrap();
    let mut opening = tokio::spawn(async move {
        open_stream::<lk::Room, lk::Room>(bus, &method, config(), options())
            .await
            .unwrap()
    });
    let mut server = tokio::time::timeout(Duration::from_secs(2), listener.accept())
        .await
        .unwrap()
        .unwrap();
    let mut client = (&mut opening).await.unwrap();
    client.send(&request("from-client")).await.unwrap();
    let received = server.recv().await.unwrap();
    assert_eq!(received.message.name, "from-client");
    assert_eq!(received.raw, request("from-client").encode_to_vec());
    server.send(&request("from-server")).await.unwrap();
    assert_eq!(client.recv().await.unwrap().message.name, "from-server");
    client.close(None).await.unwrap();
    assert!(
        tokio::time::timeout(Duration::from_secs(1), server.recv())
            .await
            .unwrap()
            .is_none()
    );
    assert_eq!(server.error().unwrap().origin, RpcErrorOrigin::Remote);
    listener.close().await;
}
#[tokio::test]
async fn local_open_ack_bidirectional_message_close() {
    exchange(LocalTransport::new(limits()).unwrap().into(), false).await;
}
#[tokio::test]
async fn local_claim_before_open_ack() {
    exchange(LocalTransport::new(limits()).unwrap().into(), true).await;
}
#[tokio::test]
async fn slow_application_consumer_does_not_ack_or_drop_existing_message() {
    let bus: RpcBus = LocalTransport::new(limits()).unwrap().into();
    let method = spec(false);
    let mut listener =
        register_stream::<lk::Room, lk::Room>(bus.clone(), method.clone(), config(), None)
            .await
            .unwrap();
    let mut client = open_stream::<lk::Room, lk::Room>(bus, &method, config(), options())
        .await
        .unwrap();
    let mut server = listener.accept().await.unwrap();
    client.send(&request("first")).await.unwrap();
    let error = client.send(&request("second")).await.unwrap_err();
    assert_eq!(error.origin, RpcErrorOrigin::Deadline);
    assert_eq!(server.recv().await.unwrap().message.name, "first");
    client.send(&request("retry-second")).await.unwrap();
    assert_eq!(server.recv().await.unwrap().message.name, "retry-second");
    client.close(None).await.unwrap();
    listener.close().await;
}
#[tokio::test]
async fn cancellation_after_open_closes_peer_and_releases_server_slot() {
    let bus: RpcBus = LocalTransport::new(limits()).unwrap().into();
    let method = spec(false);
    let mut cfg = config();
    cfg.max_streams = 1;
    let mut listener =
        register_stream::<lk::Room, lk::Room>(bus.clone(), method.clone(), cfg, None)
            .await
            .unwrap();
    let options = options();
    let cancel = options.cancel.clone();
    let mut client = open_stream::<lk::Room, lk::Room>(bus.clone(), &method, cfg, options)
        .await
        .unwrap();
    let mut server = listener.accept().await.unwrap();
    cancel.cancel();
    assert!(
        tokio::time::timeout(Duration::from_secs(1), server.recv())
            .await
            .unwrap()
            .is_none()
    );
    assert!(
        tokio::time::timeout(Duration::from_secs(1), client.recv())
            .await
            .unwrap()
            .is_none()
    );
    // The closed server handle is intentionally still alive; its permit must not
    // prevent admission of the next generation.
    let mut next = open_stream::<lk::Room, lk::Room>(bus, &method, cfg, super::tests::options())
        .await
        .unwrap();
    let _next_server = listener.accept().await.unwrap();
    next.close(None).await.unwrap();
    listener.close().await;
}
#[tokio::test]
async fn no_listener_open_times_out_without_success() {
    let bus: RpcBus = LocalTransport::new(limits()).unwrap().into();
    let mut opts = options();
    opts.timeout = Duration::from_millis(30);
    let result = open_stream::<lk::Room, lk::Room>(bus, &spec(false), config(), opts).await;
    assert!(matches!(result,Err(error) if error.origin==RpcErrorOrigin::Deadline));
}
#[tokio::test]
async fn malformed_payload_closes_instead_of_acknowledging() {
    let bus: RpcBus = LocalTransport::new(limits()).unwrap().into();
    let method = spec(false);
    let mut listener =
        register_stream::<lk::Room, lk::Room>(bus.clone(), method.clone(), config(), None)
            .await
            .unwrap();
    let mut client = open_stream::<lk::Room, lk::Room>(bus.clone(), &method, config(), options())
        .await
        .unwrap();
    let mut server = listener.accept().await.unwrap();
    bus.publish(
        &stream_channel(&method),
        &frame(
            client.id(),
            Duration::from_secs(1),
            Body::Message(pb::StreamMessage {
                message: None,
                raw_message: vec![0xff],
            }),
        ),
    )
    .await
    .unwrap();
    assert!(
        tokio::time::timeout(Duration::from_secs(1), client.recv())
            .await
            .unwrap()
            .is_none()
    );
    assert_eq!(client.error().unwrap().code, "malformed_request");
    assert!(server.recv().await.is_none());
    listener.close().await;
}
/// Real Redis fixture is externally owned; this test never starts or stops it.
/// Coordinator: LIVEKIT_TEST_REDIS_URL=redis://127.0.0.1:PORT/ cargo test -p lk-compat service_bus::stream::tests::real_redis -- --ignored --nocapture
#[tokio::test]
#[ignore = "requires explicitly supplied, externally owned Redis"]
async fn real_redis_open_claim_message_close() {
    let url = std::env::var("LIVEKIT_TEST_REDIS_URL").expect("supply isolated Redis fixture URL");
    let bus: RpcBus = RedisTransport::connect(&url, limits())
        .await
        .unwrap()
        .into();
    exchange(bus.clone(), false).await;
    exchange(bus, true).await;
}

/// Real-Redis fixture helpers. The coordinator owns the fixture; these tests
/// never start or stop it. Each test uses a unique node/topic id so concurrent
/// tests on one shared Redis never collide on a channel's subscriber count.
async fn redis_bus() -> RpcBus {
    redis_bus_with(limits()).await
}
async fn redis_bus_with(limits: TransportLimits) -> RpcBus {
    let url = std::env::var("LIVEKIT_TEST_REDIS_URL").expect("supply isolated Redis fixture URL");
    RedisTransport::connect(&url, limits).await.unwrap().into()
}
fn signal_spec_topic(topic: &str) -> MethodSpec {
    MethodSpec {
        service: "Signal".into(),
        method: "RelaySignal".into(),
        topics: vec![topic.into()],
        require_claim: false,
        queue: false,
    }
}
/// Poll the real Redis PUBLISH receiver count for the identity's STR/CLAIM/RES
/// surface until all three reach `want`. Redis subscription setup/teardown is
/// asynchronous, so a single immediate read would be a flaky assertion.
async fn wait_surface(bus: &RpcBus, node: &str, want: u64) {
    let str_channel = channels::stream_client("Signal", node);
    let claim_channel = channels::claim_request("Signal", node);
    let res_channel = channels::response("Signal", node);
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let s = bus.publish(&str_channel, &ack_frame("none")).await.unwrap();
            let c = bus
                .publish(
                    &claim_channel,
                    &pb::ClaimRequest { request_id: "none".into(), ..Default::default() },
                )
                .await
                .unwrap();
            let r = bus
                .publish(
                    &res_channel,
                    &pb::Response { request_id: "none".into(), ..Default::default() },
                )
                .await
                .unwrap();
            if s == want && c == want && r == want {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("real Redis STR/CLAIM/RES surface did not reach {want}"));
}

/// G04 lifecycle on real Redis: dropping the last handle without an explicit
/// close must unsubscribe STR/CLAIM/RES. Positive control: the pre-fix Arc cycle
/// (dispatcher strongly holding inner) kept every count at 1, so this poll would
/// time out. The Weak dispatcher + owner Drop is what makes the surface release.
#[tokio::test]
#[ignore = "requires explicitly supplied, externally owned Redis"]
async fn real_redis_shared_identity_drop_releases_claim_res_str_surface() {
    let bus = redis_bus().await;
    let node = id("node_");
    wait_surface(&bus, &node, 0).await;
    let identity = ClientStreams::new(bus.clone(), "Signal", &node).await.unwrap();
    wait_surface(&bus, &node, 1).await;
    drop(identity);
    wait_surface(&bus, &node, 0).await;
}

/// G04 lifecycle on real Redis: a stream opened on a retired identity must fail
/// with the identity's real terminal cause (Closed), not hang to the deadline.
#[tokio::test]
#[ignore = "requires explicitly supplied, externally owned Redis"]
async fn real_redis_shared_identity_register_after_retire_returns_terminal_cause() {
    let bus = redis_bus().await;
    let node = id("node_");
    let identity = Arc::new(ClientStreams::new(bus.clone(), "Signal", &node).await.unwrap());
    wait_surface(&bus, &node, 1).await;
    identity.close().await;
    wait_surface(&bus, &node, 0).await;
    let mut opts = options();
    opts.timeout = Duration::from_secs(1);
    opts.client = Some(identity.clone());
    let result = open_stream::<lk::Room, lk::Room>(bus, &signal_spec(), config(), opts).await;
    assert!(
        matches!(result, Err(error) if error.origin == RpcErrorOrigin::Closed),
        "a retired identity must reject registration with Closed, not a Deadline timeout"
    );
}

/// G04 lifecycle/reconnect on real Redis: after the shared subscription is lost,
/// a fresh nodeID identity on the same Redis re-establishes the surface and
/// relays bidirectionally again. Retirement is per-identity, not a bus poison.
#[tokio::test]
#[ignore = "requires explicitly supplied, externally owned Redis"]
async fn real_redis_shared_identity_recovers_with_fresh_identity_after_loss() {
    let bus = redis_bus().await;
    let node = id("node_");
    let old = ClientStreams::new(bus.clone(), "Signal", &node).await.unwrap();
    wait_surface(&bus, &node, 1).await;
    old.close().await;
    wait_surface(&bus, &node, 0).await;
    let identity = Arc::new(ClientStreams::new(bus.clone(), "Signal", &node).await.unwrap());
    wait_surface(&bus, &node, 1).await;
    let method = signal_spec_topic(&id("owner_"));
    let mut listener =
        register_stream::<lk::Room, lk::Room>(bus.clone(), method.clone(), config(), None)
            .await
            .unwrap();
    let mut opts = options();
    opts.client = Some(identity.clone());
    let mut client =
        open_stream::<lk::Room, lk::Room>(bus.clone(), &method, config(), opts).await.unwrap();
    let mut server = listener.accept().await.unwrap();
    assert_eq!(server.header.as_ref().unwrap().remote_id, node);
    server.send(&request("recovered")).await.unwrap();
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(3), client.recv())
            .await
            .unwrap()
            .unwrap()
            .message
            .name,
        "recovered"
    );
    client.send(&request("client-after-recovery")).await.unwrap();
    assert_eq!(server.recv().await.unwrap().message.name, "client-after-recovery");
    client.close(None).await.unwrap();
    listener.close().await;
    identity.close().await;
    wait_surface(&bus, &node, 0).await;
}

/// G04 concurrent isolation on real Redis: two streams share one nodeID identity
/// and both servers reply on the same Signal|node|STR channel; the client must
/// demultiplex by stream_id. A misroute hangs the wrong recv and times out.
#[tokio::test]
#[ignore = "requires explicitly supplied, externally owned Redis"]
async fn real_redis_shared_identity_demultiplexes_concurrent_streams() {
    let bus = redis_bus().await;
    let node = id("node_");
    let identity = Arc::new(ClientStreams::new(bus.clone(), "Signal", &node).await.unwrap());
    wait_surface(&bus, &node, 1).await;
    let method = signal_spec_topic(&id("owner_"));
    let mut listener =
        register_stream::<lk::Room, lk::Room>(bus.clone(), method.clone(), config(), None)
            .await
            .unwrap();
    let mut opts = options();
    opts.client = Some(identity.clone());
    let mut c1 =
        open_stream::<lk::Room, lk::Room>(bus.clone(), &method, config(), opts.clone())
            .await
            .unwrap();
    let mut s1 = listener.accept().await.unwrap();
    let mut c2 = open_stream::<lk::Room, lk::Room>(bus.clone(), &method, config(), opts)
        .await
        .unwrap();
    let mut s2 = listener.accept().await.unwrap();
    assert_eq!(s1.header.as_ref().unwrap().remote_id, node);
    assert_eq!(s2.header.as_ref().unwrap().remote_id, node);
    assert_ne!(c1.id(), c2.id());
    s1.send(&request("reply-one")).await.unwrap();
    s2.send(&request("reply-two")).await.unwrap();
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(3), c1.recv()).await.unwrap().unwrap().message.name,
        "reply-one"
    );
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(3), c2.recv()).await.unwrap().unwrap().message.name,
        "reply-two"
    );
    c1.send(&request("one")).await.unwrap();
    c2.send(&request("two")).await.unwrap();
    assert_eq!(s1.recv().await.unwrap().message.name, "one");
    assert_eq!(s2.recv().await.unwrap().message.name, "two");
    c1.close(None).await.unwrap();
    c2.close(None).await.unwrap();
    listener.close().await;
    identity.close().await;
    wait_surface(&bus, &node, 0).await;
}

/// G04 relay fidelity on real Redis: unknown protobuf trailing fields survive the
/// shared-identity relay in BOTH directions (client->owner and owner->client),
/// byte-for-byte, while the decoded message stays intact.
#[tokio::test]
#[ignore = "requires explicitly supplied, externally owned Redis"]
async fn real_redis_shared_identity_relay_preserves_bidirectional_unknown_fields() {
    let bus = redis_bus().await;
    let node = id("node_");
    let identity = Arc::new(ClientStreams::new(bus.clone(), "Signal", &node).await.unwrap());
    wait_surface(&bus, &node, 1).await;
    let method = signal_spec_topic(&id("owner_"));
    let mut listener =
        register_stream::<lk::Room, lk::Room>(bus.clone(), method.clone(), config(), None)
            .await
            .unwrap();
    let mut opts = options();
    opts.client = Some(identity.clone());
    let mut client =
        open_stream::<lk::Room, lk::Room>(bus.clone(), &method, config(), opts).await.unwrap();
    let mut server = listener.accept().await.unwrap();
    let mut up = request("unknown-up").encode_to_vec();
    up.extend_from_slice(&[0xa0, 0x06, 0x07]);
    client.send_raw(&up, config().timeout).await.unwrap();
    let received = server.recv().await.unwrap();
    assert_eq!(received.raw, up);
    assert_eq!(received.message.name, "unknown-up");
    let mut down = request("unknown-down").encode_to_vec();
    down.extend_from_slice(&[0xa0, 0x06, 0x09]);
    server.send_raw(&down, config().timeout).await.unwrap();
    let received = client.recv().await.unwrap();
    assert_eq!(received.raw, down);
    assert_eq!(received.message.name, "unknown-down");
    client.close(None).await.unwrap();
    listener.close().await;
    identity.close().await;
    wait_surface(&bus, &node, 0).await;
}

/// G04 partial-init release on real Redis: with only two subscription permits the
/// third (RES) acquire fails, and the fix must unsubscribe the already-acquired
/// STR/CLAIM. Positive control: a leak keeps the Redis counts at 1 and exhausts
/// the budget, so the two fresh subscribes below could not both succeed.
#[tokio::test]
#[ignore = "requires explicitly supplied, externally owned Redis"]
async fn real_redis_shared_identity_partial_init_failure_releases_surface() {
    let mut tight = limits();
    tight.subscriptions = 2;
    let bus = redis_bus_with(tight).await;
    let node = id("node_");
    assert!(
        ClientStreams::new(bus.clone(), "Signal", &node).await.is_err(),
        "the third subscribe must fail under a two-permit budget"
    );
    wait_surface(&bus, &node, 0).await;
    let str_channel = channels::stream_client("Signal", &node);
    let claim_channel = channels::claim_request("Signal", &node);
    let s = bus
        .subscribe::<pb::Stream>(&str_channel, false)
        .await
        .expect("STR permit must be released after partial-init failure");
    let c = bus
        .subscribe::<pb::ClaimRequest>(&claim_channel, false)
        .await
        .expect("CLAIM permit must be released after partial-init failure");
    s.unsubscribe(&str_channel).await.unwrap();
    c.unsubscribe(&claim_channel).await.unwrap();
    wait_surface(&bus, &node, 0).await;
}

/// G04 backpressure/ACK on real Redis: a size-one peer inbox cannot admit a second
/// message, so the sender gets no ACK and times out; the first message is never
/// dropped, and after the app consumes it the retry is ACKed.
#[tokio::test]
#[ignore = "requires explicitly supplied, externally owned Redis"]
async fn real_redis_shared_identity_backpressure_withholds_ack_until_consumed() {
    let bus = redis_bus().await;
    let node = id("node_");
    let identity = Arc::new(ClientStreams::new(bus.clone(), "Signal", &node).await.unwrap());
    wait_surface(&bus, &node, 1).await;
    let method = signal_spec_topic(&id("owner_"));
    let mut cfg = config();
    cfg.timeout = Duration::from_millis(500);
    let mut listener =
        register_stream::<lk::Room, lk::Room>(bus.clone(), method.clone(), cfg, None)
            .await
            .unwrap();
    let mut opts = options();
    opts.client = Some(identity.clone());
    let mut client =
        open_stream::<lk::Room, lk::Room>(bus.clone(), &method, cfg, opts).await.unwrap();
    let mut server = listener.accept().await.unwrap();
    client.send(&request("first")).await.unwrap();
    let error = client.send(&request("second")).await.unwrap_err();
    assert_eq!(error.origin, RpcErrorOrigin::Deadline);
    assert_eq!(server.recv().await.unwrap().message.name, "first");
    client.send(&request("retry-second")).await.unwrap();
    assert_eq!(server.recv().await.unwrap().message.name, "retry-second");
    client.close(None).await.unwrap();
    listener.close().await;
    identity.close().await;
    wait_surface(&bus, &node, 0).await;
}

/// G04 cancel on real Redis: cancelling the call options closes the peer stream
/// with a terminal cause and the identity surface is still released on close.
#[tokio::test]
#[ignore = "requires explicitly supplied, externally owned Redis"]
async fn real_redis_shared_identity_cancel_closes_peer_and_releases_surface() {
    let bus = redis_bus().await;
    let node = id("node_");
    let identity = Arc::new(ClientStreams::new(bus.clone(), "Signal", &node).await.unwrap());
    wait_surface(&bus, &node, 1).await;
    let method = signal_spec_topic(&id("owner_"));
    let mut listener =
        register_stream::<lk::Room, lk::Room>(bus.clone(), method.clone(), config(), None)
            .await
            .unwrap();
    let mut opts = options();
    opts.client = Some(identity.clone());
    let cancel = opts.cancel.clone();
    let mut client =
        open_stream::<lk::Room, lk::Room>(bus.clone(), &method, config(), opts).await.unwrap();
    let mut server = listener.accept().await.unwrap();
    cancel.cancel();
    assert!(
        tokio::time::timeout(Duration::from_secs(2), server.recv()).await.unwrap().is_none(),
        "cancel must close the peer stream"
    );
    assert!(server.error().is_some());
    listener.close().await;
    identity.close().await;
    wait_surface(&bus, &node, 0).await;
}

#[tokio::test]
async fn raw_payload_preserves_unknown_fields_and_incoming_metadata() {
    let bus: RpcBus = LocalTransport::new(limits()).unwrap().into();
    let method = spec(false);
    let mut listener =
        register_stream::<lk::Room, lk::Room>(bus.clone(), method.clone(), config(), None)
            .await
            .unwrap();
    let mut opts = options();
    opts.metadata.insert("source-region".into(), "hk".into());
    let mut client = open_stream::<lk::Room, lk::Room>(bus, &method, config(), opts)
        .await
        .unwrap();
    let mut server = listener.accept().await.unwrap();
    assert_eq!(
        server
            .header
            .as_ref()
            .unwrap()
            .metadata
            .get("source-region")
            .map(String::as_str),
        Some("hk")
    );
    let mut raw = request("unknown-source").encode_to_vec();
    raw.extend_from_slice(&[0xa0, 0x06, 0x07]);
    client.send_raw(&raw, config().timeout).await.unwrap();
    let received = server.recv().await.unwrap();
    assert_eq!(received.raw, raw);
    assert_eq!(received.message.name, "unknown-source");
    client.close(None).await.unwrap();
    listener.close().await;
}

#[tokio::test]
async fn dropping_client_closes_peer_without_explicit_close() {
    let bus: RpcBus = LocalTransport::new(limits()).unwrap().into();
    let method = spec(false);
    let mut listener =
        register_stream::<lk::Room, lk::Room>(bus.clone(), method.clone(), config(), None)
            .await
            .unwrap();
    let client = open_stream::<lk::Room, lk::Room>(bus, &method, config(), options())
        .await
        .unwrap();
    let mut server = listener.accept().await.unwrap();
    drop(client);
    assert!(
        tokio::time::timeout(Duration::from_secs(1), server.recv())
            .await
            .unwrap()
            .is_none()
    );
    listener.close().await;
}

#[tokio::test]
async fn canceled_close_caller_cannot_abandon_pending_retirement() {
    let bus: RpcBus = LocalTransport::new(limits()).unwrap().into();
    let stream = make_stream::<lk::Room, lk::Room>(
        bus,
        channels::stream_client("CloseOwned", "nobody"),
        id(""),
        config(),
        Weak::new(),
        None,
    );
    let session = stream.session.clone();
    let (sender, _receiver) = oneshot::channel();
    lock(&session.state)
        .acks
        .insert("pending-send".into(), sender);
    let waiter = AckWaiter {
        session: session.clone(),
        request: "pending-send".into(),
    };
    let closing = {
        let session = session.clone();
        tokio::spawn(async move { session.close(closed()).await })
    };
    tokio::time::timeout(Duration::from_secs(1), async {
        while session.error().is_none() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    closing.abort();
    assert!(closing.await.unwrap_err().is_cancelled());
    drop(waiter);
    tokio::time::timeout(Duration::from_secs(1), session.stop.cancelled())
        .await
        .unwrap();
    let mut completion = session.close_result.subscribe();
    tokio::time::timeout(Duration::from_secs(1), async {
        while completion.borrow().is_none() {
            completion.changed().await.unwrap();
        }
    })
    .await
    .unwrap();
    assert!(lock(&session.state).acks.is_empty());
}

#[tokio::test]
async fn claim_affinity_selects_one_server_and_releases_losing_open() {
    let bus: RpcBus = LocalTransport::new(limits()).unwrap().into();
    let method = spec(true);
    let mut low = register_stream::<lk::Room, lk::Room>(
        bus.clone(),
        method.clone(),
        config(),
        Some(Arc::new(|_| 1.0)),
    )
    .await
    .unwrap();
    let mut high = register_stream::<lk::Room, lk::Room>(
        bus.clone(),
        method.clone(),
        config(),
        Some(Arc::new(|_| 2.0)),
    )
    .await
    .unwrap();
    let mut opts = options();
    opts.selection.accept_first = false;
    opts.selection.maximum_affinity = 2.0;
    let mut client = open_stream::<lk::Room, lk::Room>(bus, &method, config(), opts)
        .await
        .unwrap();
    let _winner = tokio::time::timeout(Duration::from_secs(1), high.accept())
        .await
        .unwrap()
        .unwrap();
    assert!(
        tokio::time::timeout(Duration::from_millis(40), low.accept())
            .await
            .is_err()
    );
    client.close(None).await.unwrap();
    low.close().await;
    high.close().await;
}

#[tokio::test]
async fn admitted_ack_and_close_are_not_expired_again_inside_session() {
    let bus: RpcBus = LocalTransport::new(limits()).unwrap().into();
    let stream = make_stream::<lk::Room, lk::Room>(
        bus,
        channels::stream_client("ExpiryBoundary", "none"),
        id(""),
        config(),
        Weak::new(),
        None,
    );
    let (sender, mut receiver) = oneshot::channel();
    lock(&stream.session.state)
        .acks
        .insert("already-admitted".into(), sender);
    let mut value = frame(
        stream.id(),
        Duration::from_secs(1),
        Body::Ack(pb::StreamAck {}),
    );
    value.request_id = "already-admitted".into();
    value.expiry = now() - 1;
    stream.session.handle(value.clone()).await.unwrap();
    assert_eq!(receiver.try_recv(), Ok(()));
    value.body = Some(Body::Close(pb::StreamClose {
        code: "unavailable".into(),
        error: "EOF".into(),
    }));
    stream.session.handle(value).await.unwrap();
    assert_eq!(stream.error().unwrap().origin, RpcErrorOrigin::Remote);
}

#[tokio::test]
async fn both_adapters_ignore_expired_close_and_server_ignores_expired_ack() {
    let bus: RpcBus = LocalTransport::new(limits()).unwrap().into();
    let method = spec(false);
    let mut listener =
        register_stream::<lk::Room, lk::Room>(bus.clone(), method.clone(), config(), None)
            .await
            .unwrap();
    let mut client = open_stream::<lk::Room, lk::Room>(bus.clone(), &method, config(), options())
        .await
        .unwrap();
    let mut server = listener.accept().await.unwrap();
    let mut close = frame(
        client.id(),
        Duration::from_secs(1),
        Body::Close(pb::StreamClose {
            code: "unavailable".into(),
            error: "stale".into(),
        }),
    );
    close.expiry = now() - 1;
    bus.publish(&stream_channel(&method), &close).await.unwrap();
    bus.publish(&server.session.target, &close).await.unwrap();
    // A successful subsequent round trip proves each expired close was ignored.
    client.send(&request("still-open")).await.unwrap();
    assert_eq!(server.recv().await.unwrap().message.name, "still-open");
    server.send(&request("also-open")).await.unwrap();
    assert_eq!(client.recv().await.unwrap().message.name, "also-open");
    assert!(server.error().is_none());
    assert!(client.error().is_none());
    let (sender, mut waiting) = oneshot::channel();
    lock(&server.session.state)
        .acks
        .insert("expiry-ack".into(), sender);
    let mut ack = frame(
        client.id(),
        Duration::from_secs(1),
        Body::Ack(pb::StreamAck {}),
    );
    ack.request_id = "expiry-ack".into();
    ack.expiry = now() - 1;
    bus.publish(&stream_channel(&method), &ack).await.unwrap();
    // This fresh round trip follows the expired ACK on the same producer, so its
    // completion establishes that the adapter processed the earlier frame.
    client.send(&request("ack-barrier")).await.unwrap();
    server.recv().await.unwrap();
    assert_eq!(waiting.try_recv(), Err(oneshot::error::TryRecvError::Empty));
    ack.expiry = now() + 1_000_000_000;
    bus.publish(&stream_channel(&method), &ack).await.unwrap();
    tokio::time::timeout(Duration::from_secs(1), waiting)
        .await
        .unwrap()
        .unwrap();
    client.close(None).await.unwrap();
    listener.close().await;
}

#[tokio::test]
async fn listener_drop_retires_task_subscriptions_and_slot_with_accepted_owner_alive() {
    let bus: RpcBus = LocalTransport::new(limits()).unwrap().into();
    let method = spec(true);
    let mut listener =
        register_stream::<lk::Room, lk::Room>(bus.clone(), method.clone(), config(), None)
            .await
            .unwrap();
    let mut client = open_stream::<lk::Room, lk::Room>(bus.clone(), &method, config(), options())
        .await
        .unwrap();
    let mut server = listener.accept().await.unwrap();
    let session = server.session.clone();
    let task = listener.task.take().unwrap();
    drop(listener);
    tokio::time::timeout(Duration::from_secs(1), task)
        .await
        .unwrap()
        .unwrap();
    assert!(lock(&session._slot).is_none());
    assert!(server.recv().await.is_none());
    assert!(client.recv().await.is_none());
    assert_eq!(
        bus.publish(
            &stream_channel(&method),
            &frame(
                client.id(),
                Duration::from_secs(1),
                Body::Ack(pb::StreamAck {})
            )
        )
        .await
        .unwrap(),
        0
    );
    assert_eq!(
        bus.publish(
            &claim_channel(&method),
            &pb::ClaimResponse {
                request_id: "none".into(),
                server_id: "none".into()
            }
        )
        .await
        .unwrap(),
        0
    );
}

#[tokio::test]
async fn listener_drop_interrupts_blocked_ack_even_with_long_peer_expiry() {
    let local = LocalTransport::new(limits()).unwrap();
    let bus: RpcBus = local.clone().into();
    let method = spec(false);
    let mut listener =
        register_stream::<lk::Room, lk::Room>(bus.clone(), method.clone(), config(), None)
            .await
            .unwrap();
    let mut client = open_stream::<lk::Room, lk::Room>(bus, &method, config(), options())
        .await
        .unwrap();
    let mut server = listener.accept().await.unwrap();
    let peer = server.session.target.clone();
    // This extra subscriber intentionally never reads its one-message queue.
    let blocked = local.subscribe_sized(&peer, false, 1).await;
    client.send(&request("fill-ack-queue")).await.unwrap();
    server.recv().await.unwrap();
    client
        .send_raw(
            &request("block-second-ack").encode_to_vec(),
            Duration::from_secs(20),
        )
        .await
        .unwrap();
    let task = listener.task.take().unwrap();
    drop(listener);
    tokio::time::timeout(Duration::from_secs(2), task)
        .await
        .expect("Drop must not wait for peer's twenty-second expiry")
        .unwrap();
    assert!(lock(&server.session._slot).is_none());
    blocked.unsubscribe(&peer).await.unwrap();
}

#[tokio::test]
async fn business_payload_accepts_fixed_go_wrong_kind_unknown_without_losing_raw() {
    let bus: RpcBus = LocalTransport::new(limits()).unwrap().into();
    let method = spec(false);
    let mut listener =
        register_stream::<lk::Room, lk::Room>(bus.clone(), method.clone(), config(), None)
            .await
            .unwrap();
    let mut client = open_stream::<lk::Room, lk::Room>(bus, &method, config(), options())
        .await
        .unwrap();
    let mut server = listener.accept().await.unwrap();
    let mut raw = request("known-name").encode_to_vec();
    raw.extend_from_slice(&[0x08, 7]); // Room.sid #1 expects string.
    assert!(lk::Room::decode(raw.as_slice()).is_err());
    client.send_raw(&raw, config().timeout).await.unwrap();
    let received = server.recv().await.unwrap();
    assert_eq!(received.message.name, "known-name");
    assert_eq!(received.raw, raw);
    client.close(None).await.unwrap();
    listener.close().await;
}

fn signal_spec() -> MethodSpec {
    MethodSpec {
        service: "Signal".into(),
        method: "RelaySignal".into(),
        topics: vec!["owner".into()],
        require_claim: false,
        queue: false,
    }
}
fn ack_frame(stream_id: &str) -> pb::Stream {
    frame(stream_id, Duration::from_secs(1), Body::Ack(pb::StreamAck {}))
}

/// G04: the official typed Signal client subscribes CLAIM/RES/STR for its whole
/// lifetime under WithClientID(nodeID). Prove the identity really owns all three
/// client channels (positive control: absent before, present after, retired on
/// close) rather than only opening a per-call STR channel.
#[tokio::test]
async fn shared_client_identity_holds_claim_res_str_surface_until_close() {
    let bus: RpcBus = LocalTransport::new(limits()).unwrap().into();
    let str_channel = channels::stream_client("Signal", "nodeX");
    let claim_channel = channels::claim_request("Signal", "nodeX");
    let res_channel = channels::response("Signal", "nodeX");
    assert_eq!(bus.publish(&str_channel, &ack_frame("none")).await.unwrap(), 0);
    let identity = ClientStreams::new(bus.clone(), "Signal", "nodeX").await.unwrap();
    assert_eq!(identity.client_id(), "nodeX");
    assert_eq!(bus.publish(&str_channel, &ack_frame("none")).await.unwrap(), 1);
    assert_eq!(
        bus.publish(
            &claim_channel,
            &pb::ClaimRequest {
                request_id: "none".into(),
                ..Default::default()
            }
        )
        .await
        .unwrap(),
        1
    );
    assert_eq!(
        bus.publish(
            &res_channel,
            &pb::Response {
                request_id: "none".into(),
                ..Default::default()
            }
        )
        .await
        .unwrap(),
        1
    );
    identity.close().await;
    assert_eq!(bus.publish(&str_channel, &ack_frame("none")).await.unwrap(), 0);
    assert_eq!(
        bus.publish(
            &claim_channel,
            &pb::ClaimRequest {
                request_id: "none".into(),
                ..Default::default()
            }
        )
        .await
        .unwrap(),
        0
    );
    assert_eq!(
        bus.publish(
            &res_channel,
            &pb::Response {
                request_id: "none".into(),
                ..Default::default()
            }
        )
        .await
        .unwrap(),
        0
    );
}

/// G04: two concurrent streams share one nodeID identity, so both servers reply
/// on the same Signal|nodeX|STR channel. The client must demultiplex by stream_id
/// exactly as the server's register_stream does; a misroute hangs the wrong recv.
#[tokio::test]
async fn shared_client_identity_demultiplexes_concurrent_streams() {
    let bus: RpcBus = LocalTransport::new(limits()).unwrap().into();
    let identity = Arc::new(ClientStreams::new(bus.clone(), "Signal", "nodeX").await.unwrap());
    let method = signal_spec();
    let mut listener =
        register_stream::<lk::Room, lk::Room>(bus.clone(), method.clone(), config(), None)
            .await
            .unwrap();
    let mut opts = options();
    opts.client = Some(identity.clone());
    let mut c1 =
        open_stream::<lk::Room, lk::Room>(bus.clone(), &method, config(), opts.clone())
            .await
            .unwrap();
    let mut s1 = listener.accept().await.unwrap();
    let mut c2 = open_stream::<lk::Room, lk::Room>(bus.clone(), &method, config(), opts)
        .await
        .unwrap();
    let mut s2 = listener.accept().await.unwrap();
    assert_eq!(s1.header.as_ref().unwrap().remote_id, "nodeX");
    assert_eq!(s2.header.as_ref().unwrap().remote_id, "nodeX");
    assert_ne!(c1.id(), c2.id());
    s1.send(&request("reply-one")).await.unwrap();
    s2.send(&request("reply-two")).await.unwrap();
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(2), c1.recv())
            .await
            .unwrap()
            .unwrap()
            .message
            .name,
        "reply-one"
    );
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(2), c2.recv())
            .await
            .unwrap()
            .unwrap()
            .message
            .name,
        "reply-two"
    );
    c1.send(&request("one")).await.unwrap();
    assert_eq!(s1.recv().await.unwrap().message.name, "one");
    c2.send(&request("two")).await.unwrap();
    assert_eq!(s2.recv().await.unwrap().message.name, "two");
    c1.close(None).await.unwrap();
    assert!(
        tokio::time::timeout(Duration::from_secs(1), s1.recv())
            .await
            .unwrap()
            .is_none()
    );
    assert_eq!(s1.error().unwrap().origin, RpcErrorOrigin::Remote);
    // Retiring c1's demux registration must not disturb the still-open c2.
    s2.send(&request("still-two")).await.unwrap();
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(2), c2.recv())
            .await
            .unwrap()
            .unwrap()
            .message
            .name,
        "still-two"
    );
    c2.close(None).await.unwrap();
    listener.close().await;
    identity.close().await;
}

/// G04 negative control: the shared identity must not fabricate an Open ACK when
/// no server owns the target topic; it times out exactly like the dedicated path.
#[tokio::test]
async fn shared_client_identity_without_listener_times_out() {
    let bus: RpcBus = LocalTransport::new(limits()).unwrap().into();
    let identity = Arc::new(ClientStreams::new(bus.clone(), "Signal", "nodeX").await.unwrap());
    let mut opts = options();
    opts.timeout = Duration::from_millis(30);
    opts.client = Some(identity.clone());
    let result = open_stream::<lk::Room, lk::Room>(bus, &signal_spec(), config(), opts).await;
    assert!(matches!(result, Err(error) if error.origin == RpcErrorOrigin::Deadline));
    identity.close().await;
}

/// G04 lifecycle: dropping the last handle without an explicit close must still
/// release the shared STR/CLAIM/RES surface. The dispatcher holds only a Weak, so
/// the owner's Drop cancels it and it unsubscribes all three. Positive control:
/// the pre-fix Arc cycle kept every count at 1, so this poll would time out.
#[tokio::test]
async fn shared_client_identity_drop_releases_claim_res_str_surface() {
    let bus: RpcBus = LocalTransport::new(limits()).unwrap().into();
    let str_channel = channels::stream_client("Signal", "nodeX");
    let claim_channel = channels::claim_request("Signal", "nodeX");
    let res_channel = channels::response("Signal", "nodeX");
    let identity = ClientStreams::new(bus.clone(), "Signal", "nodeX")
        .await
        .unwrap();
    assert_eq!(bus.publish(&str_channel, &ack_frame("none")).await.unwrap(), 1);
    assert_eq!(
        bus.publish(
            &claim_channel,
            &pb::ClaimRequest {
                request_id: "none".into(),
                ..Default::default()
            }
        )
        .await
        .unwrap(),
        1
    );
    assert_eq!(
        bus.publish(
            &res_channel,
            &pb::Response {
                request_id: "none".into(),
                ..Default::default()
            }
        )
        .await
        .unwrap(),
        1
    );
    drop(identity);
    let released = tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            let str_count = bus.publish(&str_channel, &ack_frame("none")).await.unwrap();
            let claim_count = bus
                .publish(
                    &claim_channel,
                    &pb::ClaimRequest {
                        request_id: "none".into(),
                        ..Default::default()
                    },
                )
                .await
                .unwrap();
            let res_count = bus
                .publish(
                    &res_channel,
                    &pb::Response {
                        request_id: "none".into(),
                        ..Default::default()
                    },
                )
                .await
                .unwrap();
            if str_count == 0 && claim_count == 0 && res_count == 0 {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await;
    assert!(
        released.is_ok(),
        "dropping the identity must unsubscribe STR/CLAIM/RES, not leak them via an Arc cycle"
    );
}

/// G04 lifecycle: opening a stream on an already-retired identity must fail with
/// the identity's real terminal cause (Closed), not hang until the call deadline.
/// register() rejects under the shared state lock, so a retire-vs-open race can
/// never leave a session that is registered but never receives.
#[tokio::test]
async fn shared_client_identity_register_after_retire_returns_terminal_cause() {
    let bus: RpcBus = LocalTransport::new(limits()).unwrap().into();
    let identity = Arc::new(
        ClientStreams::new(bus.clone(), "Signal", "nodeX")
            .await
            .unwrap(),
    );
    identity.close().await;
    let mut opts = options();
    opts.timeout = Duration::from_secs(1);
    opts.client = Some(identity.clone());
    let result = open_stream::<lk::Room, lk::Room>(bus, &signal_spec(), config(), opts).await;
    assert!(
        matches!(result, Err(error) if error.origin == RpcErrorOrigin::Closed),
        "a retired identity must reject registration with its Closed terminal cause, not a Deadline timeout"
    );
}

/// G04 lifecycle: after the shared subscription is lost (identity closed), a
/// fresh nodeID identity on the same bus re-establishes the surface and relays
/// again. Retirement is per-identity, not a permanent bus-level poison.
#[tokio::test]
async fn shared_client_identity_recovers_with_fresh_identity_after_loss() {
    let bus: RpcBus = LocalTransport::new(limits()).unwrap().into();
    let str_channel = channels::stream_client("Signal", "nodeX");
    let old = ClientStreams::new(bus.clone(), "Signal", "nodeX")
        .await
        .unwrap();
    assert_eq!(bus.publish(&str_channel, &ack_frame("none")).await.unwrap(), 1);
    old.close().await;
    assert_eq!(bus.publish(&str_channel, &ack_frame("none")).await.unwrap(), 0);
    let identity = Arc::new(
        ClientStreams::new(bus.clone(), "Signal", "nodeX")
            .await
            .unwrap(),
    );
    assert_eq!(bus.publish(&str_channel, &ack_frame("none")).await.unwrap(), 1);
    let method = signal_spec();
    let mut listener =
        register_stream::<lk::Room, lk::Room>(bus.clone(), method.clone(), config(), None)
            .await
            .unwrap();
    let mut opts = options();
    opts.client = Some(identity.clone());
    let mut client =
        open_stream::<lk::Room, lk::Room>(bus.clone(), &method, config(), opts)
            .await
            .unwrap();
    let mut server = listener.accept().await.unwrap();
    assert_eq!(server.header.as_ref().unwrap().remote_id, "nodeX");
    server.send(&request("recovered")).await.unwrap();
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(2), client.recv())
            .await
            .unwrap()
            .unwrap()
            .message
            .name,
        "recovered"
    );
    client.send(&request("client-after-recovery")).await.unwrap();
    assert_eq!(server.recv().await.unwrap().message.name, "client-after-recovery");
    client.close(None).await.unwrap();
    listener.close().await;
    identity.close().await;
}
