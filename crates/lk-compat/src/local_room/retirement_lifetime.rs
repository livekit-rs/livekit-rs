//! Admitted but not-yet-polled handler + new generation collection pass.
use super::*;
use std::sync::atomic::{AtomicBool, Ordering};
#[tokio::test(start_paused = true)]
async fn retirement_prune_cannot_abort_late_entering_accepted_work_after_caller_expiry() {
    let accepted = Arc::new(Accepted::default());
    // This Arc is held by a retired real server's admitted handler before that
    // future's first poll, just as ServerRun.handle holds its handler closure.
    let admitted = accepted.clone();
    let mut groups = Groups::default();
    groups.accepted.push(accepted);
    groups.retain_accepted_work();
    assert_eq!(
        groups.accepted.len(),
        1,
        "unentered admitted handler still needs an operation owner"
    );
    let (release, blocked) = oneshot::channel();
    let entered = Arc::new(AtomicBool::new(false));
    let completed = Arc::new(AtomicBool::new(false));
    let seen_entered = entered.clone();
    let seen_completed = completed.clone();
    let handler = tokio::spawn(async move {
        admitted
            .start(async move {
                seen_entered.store(true, Ordering::SeqCst);
                blocked.await.unwrap();
                seen_completed.store(true, Ordering::SeqCst);
                Ok(pb::SendDataResponse {})
            })
            .await
    });
    while !entered.load(Ordering::SeqCst) {
        tokio::task::yield_now().await;
    }
    handler.abort();
    assert!(handler.await.unwrap_err().is_cancelled());
    groups.retain_accepted_work();
    assert_eq!(
        groups.accepted.len(),
        1,
        "expired caller cannot retire unfinished accepted operation"
    );
    assert!(!completed.load(Ordering::SeqCst));
    release.send(()).unwrap();
    groups.accepted[0].join().await;
    assert!(completed.load(Ordering::SeqCst));
    groups.retain_accepted_work();
    assert!(
        groups.accepted.is_empty(),
        "completed sole-owner group must still release"
    );
}
