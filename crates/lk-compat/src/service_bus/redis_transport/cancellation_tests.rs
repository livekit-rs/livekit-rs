use super::*;
use std::sync::atomic::{AtomicUsize, Ordering};
use tokio::sync::oneshot;

#[tokio::test]
async fn canceled_receive_retains_the_same_admission_future_and_payload() {
    for acquired in [true, false] {
        let calls = Arc::new(AtomicUsize::new(0));
        let started = calls.clone();
        let (entered, ready) = oneshot::channel();
        let (release, held) = oneshot::channel();
        let mut admission = QueueAdmission {
            pending: Some(Box::pin(async move {
                started.fetch_add(1, Ordering::SeqCst);
                entered.send(()).unwrap();
                held.await.unwrap();
                (b"already-dequeued-request".to_vec(), Ok(acquired))
            })),
        };
        // Same cancellation shape as ServerRun's competing grants/join_next.
        tokio::select! {
            biased;
            _ = admission.finish() => panic!("admission must still be held"),
            _ = ready => {},
        }
        assert!(admission.pending.is_some());
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        release.send(()).unwrap();
        let (payload, result) = admission.finish().await;
        assert_eq!(payload, b"already-dequeued-request");
        assert_eq!(result, Ok(acquired));
        assert_eq!(calls.load(Ordering::SeqCst), 1, "SET NX must not restart");
        assert!(admission.pending.is_none());
    }
}

#[tokio::test]
async fn admission_failure_is_retained_across_receive_cancellation() {
    let (entered, ready) = oneshot::channel();
    let (release, held) = oneshot::channel();
    let mut admission = QueueAdmission {
        pending: Some(Box::pin(async move {
            entered.send(()).unwrap();
            held.await.unwrap();
            (vec![1], Err(TransportError::Redis))
        })),
    };
    tokio::select! {
        biased;
        _ = admission.finish() => panic!("must wait"),
        _ = ready => {},
    }
    release.send(()).unwrap();
    assert_eq!(
        admission.finish().await,
        (vec![1], Err(TransportError::Redis))
    );
    assert!(admission.pending.is_none());
}

#[tokio::test]
#[ignore = "requires dedicated coordinator-owned IO_RPC_TEST_REDIS_URL broker"]
async fn actual_redis_queued_request_survives_select_cancellation_after_setnx() {
    let url = std::env::var("IO_RPC_TEST_REDIS_URL").expect("dedicated Redis broker");
    let bus = RedisTransport::connect(
        &url,
        TransportLimits {
            wire: WireLimits {
                encoded_bytes: 1 << 20,
                decompressed_bytes: 1 << 20,
            },
            compression: CompressionPolicy::default(),
            inbox_messages: 16,
            subscriptions: 4,
            commands: 8,
            io_timeout: Duration::from_secs(2),
        },
    )
    .await
    .unwrap();
    let topic = rtc_core::ids::new_sid("queue_cancel_");
    let channel = super::super::channels::request("CancelSafety", "ActualQueue", &[&topic], true);
    let mut subscription = bus.subscribe(&channel, true).await.unwrap();
    let payload = format!("actual Redis queue {topic}").into_bytes();
    let lock = STANDARD.encode(Sha256::digest(&payload));
    let (entered, ready) = oneshot::channel();
    let (release, held) = oneshot::channel();
    subscription.admission_pause = Some((entered, held));
    assert_eq!(bus.publish_encoded(&channel, &payload).await.unwrap(), 1);
    tokio::time::timeout(Duration::from_secs(3), async {
        tokio::select! {
            biased;
            result = subscription.recv() => panic!("must pause after actual SET NX: {result:?}"),
            acquired = ready => assert!(acquired.unwrap(), "actual lock must be acquired"),
        }
    })
    .await
    .expect("real Redis admission reached");
    let mut command = bus.command.begin();
    let actual: Option<String> = redis::cmd("GET")
        .arg(&lock)
        .query_async(&mut command)
        .await
        .unwrap();
    assert_eq!(
        actual.as_deref(),
        Some("1"),
        "real SET NX was completed before cancellation"
    );
    release.send(()).unwrap();
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(2), subscription.recv())
            .await
            .unwrap()
            .unwrap(),
        payload
    );
    assert!(subscription.admission.pending.is_none());
    // Replaying the same payload still obeys official Redis queue dedup. The
    // cancellation fix must not remove the lock or bypass another consumer.
    assert_eq!(bus.publish_encoded(&channel, &payload).await.unwrap(), 1);
    assert!(
        tokio::time::timeout(Duration::from_millis(50), subscription.recv())
            .await
            .is_err()
    );
    let next = format!("next actual queue {topic}").into_bytes();
    assert_eq!(bus.publish_encoded(&channel, &next).await.unwrap(), 1);
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(2), subscription.recv())
            .await
            .unwrap()
            .unwrap(),
        next
    );
    subscription.unsubscribe(&channel).await.unwrap();
    // Only this test's two payload-hash keys are task-owned.
    redis::cmd("DEL")
        .arg(&lock)
        .arg(STANDARD.encode(Sha256::digest(&next)))
        .query_async::<()>(&mut command)
        .await
        .unwrap();
}
