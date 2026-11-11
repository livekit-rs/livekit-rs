//! Actual production send/outcome helper under a paused clock: no transport or
//! fabricated media is needed to prove a blocked send cannot defer RPC timers.
use super::*;
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};
struct Sending(Arc<AtomicBool>);
impl Drop for Sending {
    fn drop(&mut self) {
        self.0.store(true, Ordering::SeqCst);
    }
}

#[tokio::test(start_paused = true)]
async fn missing_ack_expires_even_while_actual_send_future_is_blocked() {
    let registry = Arc::new(RpcRegistry::new());
    let dropped = Arc::new(AtomicBool::new(false));
    let sender_dropped = dropped.clone();
    let pending = registry.clone();
    let task = tokio::spawn(async move {
        let (ack, response) = pending
            .register("blocked-ack", ParticipantSid("recipient".into()))
            .unwrap();
        let _guard = PendingGuard {
            registry: &pending,
            id: "blocked-ack".into(),
        };
        send_with_outcome(
            tokio::time::Instant::now(),
            async move {
                let _sending = Sending(sender_dropped);
                std::future::pending::<Result<(), ApiError>>().await
            },
            ack,
            response,
            Duration::from_secs(2),
            Duration::from_secs(10),
        )
        .await
    });
    tokio::task::yield_now().await;
    assert_eq!(registry.pending_count(), 1);
    tokio::time::advance(Duration::from_secs(2)).await;
    assert_eq!(
        task.await.unwrap(),
        Err(RpcFailure::Rpc(DataChannelRpcError::built_in(
            codes::CONNECTION_TIMEOUT
        )))
    );
    assert!(dropped.load(Ordering::SeqCst));
    assert_eq!(registry.pending_count(), 0);
}

#[tokio::test(start_paused = true)]
async fn ack_does_not_stop_response_deadline_while_send_is_blocked() {
    let registry = Arc::new(RpcRegistry::new());
    let pending = registry.clone();
    let task = tokio::spawn(async move {
        let (ack, response) = pending
            .register("blocked-response", ParticipantSid("recipient".into()))
            .unwrap();
        let _guard = PendingGuard {
            registry: &pending,
            id: "blocked-response".into(),
        };
        send_with_outcome(
            tokio::time::Instant::now(),
            std::future::pending(),
            ack,
            response,
            Duration::from_secs(2),
            Duration::from_secs(5),
        )
        .await
    });
    tokio::task::yield_now().await;
    assert!(registry.handle_ack(&ParticipantSid("recipient".into()), "blocked-response"));
    tokio::task::yield_now().await;
    tokio::time::advance(Duration::from_secs(5)).await;
    assert_eq!(
        task.await.unwrap(),
        Err(RpcFailure::Rpc(DataChannelRpcError::built_in(
            codes::RESPONSE_TIMEOUT
        )))
    );
    assert_eq!(registry.pending_count(), 0);
}

#[tokio::test(start_paused = true)]
async fn completing_slow_send_does_not_restart_either_original_clock() {
    let (ack_tx, ack) = oneshot::channel();
    let (_response_tx, response) = oneshot::channel();
    let task = tokio::spawn(send_with_outcome(
        tokio::time::Instant::now(),
        async {
            tokio::time::sleep(Duration::from_secs(4)).await;
            Ok(())
        },
        ack,
        response,
        Duration::from_secs(2),
        Duration::from_secs(5),
    ));
    assert!(ack_tx.send(()).is_ok());
    tokio::task::yield_now().await;
    tokio::time::advance(Duration::from_secs(4)).await;
    tokio::task::yield_now().await;
    assert!(!task.is_finished());
    tokio::time::advance(Duration::from_secs(1)).await;
    assert_eq!(
        task.await.unwrap(),
        Err(RpcFailure::Rpc(DataChannelRpcError::built_in(
            codes::RESPONSE_TIMEOUT
        )))
    );
}

#[tokio::test(start_paused = true)]
async fn genuine_response_can_complete_before_pending_send_returns() {
    let (ack_tx, ack) = oneshot::channel();
    let (response_tx, response) = oneshot::channel();
    let task = tokio::spawn(send_with_outcome(
        tokio::time::Instant::now(),
        std::future::pending(),
        ack,
        response,
        Duration::from_secs(2),
        Duration::from_secs(5),
    ));
    tokio::task::yield_now().await;
    assert!(ack_tx.send(()).is_ok());
    assert!(
        response_tx
            .send(RpcOutcome::Payload("actual-response".into()))
            .is_ok()
    );
    assert_eq!(task.await.unwrap(), Ok("actual-response".into()));
}

#[tokio::test(start_paused = true)]
async fn actual_send_failure_preserves_the_existing_internal_error_mapping() {
    let (_ack_tx, ack) = oneshot::channel();
    let (_response_tx, response) = oneshot::channel();
    let failure = send_with_outcome(
        tokio::time::Instant::now(),
        async { Err(ApiError::Unavailable("owned transport failed".into())) },
        ack,
        response,
        Duration::from_secs(2),
        Duration::from_secs(5),
    )
    .await;
    assert_eq!(
        failure,
        Err(RpcFailure::Api(ApiError::Internal(
            "owned transport failed".into()
        )))
    );
}
