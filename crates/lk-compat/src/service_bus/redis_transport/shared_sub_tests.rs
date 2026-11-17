//! Real-Redis proof that one transport multiplexes every subscription onto a
//! single pub/sub connection (the locked `redisbus` shape) while preserving the
//! accepted per-subscription semantics: fan-out, queue SET NX single delivery,
//! fail-closed Overflow on a full inbox only, explicit disconnect recovery, and
//! connection teardown when the last registration leaves.
//!
//! Requires a dedicated coordinator-owned broker (`IO_RPC_TEST_REDIS_URL`); the
//! recovery vector issues `CLIENT KILL TYPE pubsub`, so it must not share a
//! broker with other live pub/sub owners. Run with `--test-threads=1`.

use super::*;
use crate::service_bus::channels;
use std::time::Duration;

fn limits(inbox: usize) -> TransportLimits {
    TransportLimits {
        wire: WireLimits {
            encoded_bytes: 1 << 20,
            decompressed_bytes: 1 << 20,
        },
        compression: CompressionPolicy::default(),
        inbox_messages: inbox,
        subscriptions: 64,
        commands: 16,
        io_timeout: Duration::from_secs(3),
    }
}

async fn broker() -> Arc<RedisTransport> {
    let url = std::env::var("IO_RPC_TEST_REDIS_URL").expect("dedicated Redis broker");
    RedisTransport::connect(&url, limits(16)).await.unwrap()
}

fn fanout_channel(topic: &str) -> Channel {
    channels::request("SharedSubFanout", "Message", &[topic], false)
}

fn queue_channel(topic: &str) -> Channel {
    channels::request("SharedSubQueue", "Message", &[topic], true)
}

#[tokio::test]
#[ignore = "requires dedicated coordinator-owned IO_RPC_TEST_REDIS_URL broker"]
async fn many_channels_share_one_pubsub_connection_and_fan_out() {
    let bus = broker().await;
    let mut subs = Vec::new();
    let mut chans = Vec::new();
    for _ in 0..8 {
        let topic = rtc_core::ids::new_sid("shared_many_");
        let ch = fanout_channel(&topic);
        subs.push(bus.subscribe(&ch, false).await.unwrap());
        chans.push(ch);
        assert_eq!(
            bus.shared.live_connection_count(),
            1,
            "every subscription must reuse the one shared pub/sub connection"
        );
    }
    // A second registration on an existing channel also stays on one connection.
    let extra = bus.subscribe(&chans[0], false).await.unwrap();
    assert_eq!(bus.shared.live_connection_count(), 1);

    let payload = b"fan-out-to-every-registration".to_vec();
    // Redis counts subscriber *connections*, not local registrations: the one
    // shared pub/sub socket subscribes the channel once (the locked redisbus
    // shape), so PUBLISH reports 1 while the dispatcher fans out to both routes.
    assert_eq!(
        bus.publish_encoded(&chans[0], &payload).await.unwrap(),
        1,
        "one shared subscriber connection backs both local registrations"
    );
    assert_eq!(subs[0].recv().await.unwrap(), payload);
    let mut extra = extra;
    assert_eq!(extra.recv().await.unwrap(), payload);
    for n in 1..8 {
        let p = format!("channel-{n}").into_bytes();
        bus.publish_encoded(&chans[n], &p).await.unwrap();
        assert_eq!(subs[n].recv().await.unwrap(), p);
    }
    extra.unsubscribe(&chans[0]).await.unwrap();
    for (sub, ch) in subs.into_iter().zip(chans.iter()) {
        sub.unsubscribe(ch).await.unwrap();
    }
    assert_eq!(
        bus.shared.live_connection_count(),
        0,
        "the shared socket closes once the last registration leaves"
    );
}

#[tokio::test]
#[ignore = "requires dedicated coordinator-owned IO_RPC_TEST_REDIS_URL broker"]
async fn queue_regions_dedup_to_single_delivery_on_shared_connection() {
    let bus = broker().await;
    let topic = rtc_core::ids::new_sid("shared_queue_");
    let ch = queue_channel(&topic);
    let mut a = bus.subscribe(&ch, true).await.unwrap();
    let mut b = bus.subscribe(&ch, true).await.unwrap();
    assert_eq!(bus.shared.live_connection_count(), 1);
    let payload = format!("queued-once {topic}").into_bytes();
    let lock = STANDARD.encode(Sha256::digest(&payload));
    bus.publish_encoded(&ch, &payload).await.unwrap();
    let ra = tokio::time::timeout(Duration::from_secs(2), a.recv()).await;
    let rb = tokio::time::timeout(Duration::from_millis(200), b.recv()).await;
    let delivered = [ra.is_ok(), rb.is_ok()].iter().filter(|x| **x).count();
    assert_eq!(
        delivered, 1,
        "SET NX dedup must yield exactly one consumer even though both inboxes were fed"
    );
    if let Ok(Ok(bytes)) = ra {
        assert_eq!(bytes, payload);
    }
    if let Ok(Ok(bytes)) = rb {
        assert_eq!(bytes, payload);
    }
    a.unsubscribe(&ch).await.unwrap();
    b.unsubscribe(&ch).await.unwrap();
    let mut command = bus.command.begin();
    redis::cmd("DEL")
        .arg(&lock)
        .query_async::<()>(&mut command)
        .await
        .unwrap();
}

#[tokio::test]
#[ignore = "requires dedicated coordinator-owned IO_RPC_TEST_REDIS_URL broker"]
async fn full_inbox_faults_only_that_registration_and_keeps_shared_connection() {
    let bus = broker().await;
    let small_bus = bus.with_inbox_size(1).unwrap();
    let topic = rtc_core::ids::new_sid("shared_overflow_");
    let ch = fanout_channel(&topic);
    let mut big = bus.subscribe(&ch, false).await.unwrap();
    let small = small_bus.subscribe(&ch, false).await.unwrap();
    assert_eq!(bus.shared.live_connection_count(), 1);
    for n in 0..3 {
        let p = format!("burst-{n}").into_bytes();
        bus.publish_encoded(&ch, &p).await.unwrap();
    }
    bus.shared.test_barrier().await.unwrap();
    assert_eq!(
        small.failure(),
        Some(TransportError::Overflow),
        "the capacity-1 inbox fails closed without touching the shared socket"
    );
    assert_eq!(bus.shared.live_connection_count(), 1);
    for n in 0..3 {
        assert_eq!(big.recv().await.unwrap(), format!("burst-{n}").into_bytes());
    }
    drop(small);
    big.unsubscribe(&ch).await.unwrap();
    assert_eq!(bus.shared.live_connection_count(), 0);
}

#[tokio::test]
#[ignore = "requires dedicated coordinator-owned IO_RPC_TEST_REDIS_URL broker; issues CLIENT KILL TYPE pubsub"]
async fn shared_disconnect_recovers_through_one_replacement_connection() {
    let bus = broker().await;
    let topic = rtc_core::ids::new_sid("shared_recover_");
    let ch = fanout_channel(&topic);
    let mut sub = bus.subscribe(&ch, false).await.unwrap();
    assert_eq!(bus.shared.live_connection_count(), 1);
    let killed: i64 = redis::cmd("CLIENT")
        .arg("KILL")
        .arg("TYPE")
        .arg("pubsub")
        .query_async(&mut bus.command.begin())
        .await
        .unwrap();
    assert!(killed >= 1, "the shared pub/sub socket must be killed");
    let event = tokio::time::timeout(Duration::from_secs(3), sub.recv_recovering())
        .await
        .expect("disconnect observed")
        .unwrap();
    assert!(matches!(event, SubscriptionEvent::Disconnected));
    assert_eq!(
        bus.shared.live_connection_count(),
        0,
        "the faulted shared socket is released before replacement"
    );
    let event = tokio::time::timeout(Duration::from_secs(3), sub.recv_recovering())
        .await
        .expect("re-subscribe acknowledged")
        .unwrap();
    assert!(matches!(event, SubscriptionEvent::Restored));
    assert_eq!(bus.shared.live_connection_count(), 1);
    let payload = b"after-recovery".to_vec();
    bus.publish_encoded(&ch, &payload).await.unwrap();
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(2), sub.recv())
            .await
            .unwrap()
            .unwrap(),
        payload
    );
    sub.unsubscribe(&ch).await.unwrap();
    assert_eq!(bus.shared.live_connection_count(), 0);
}