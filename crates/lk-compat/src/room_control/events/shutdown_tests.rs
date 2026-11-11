use super::*;

#[tokio::test]
async fn concurrent_authority_detach_waits_actual_worker_and_shares_success_or_failure() {
    for fail in [false, true] {
        let authority = Arc::new(RoutingEvents::new(Arc::new(rtc_core::events::NullSink)));
        authority.enabled.store(true, Ordering::Release);
        *authority.shared.drain_budget.lock().unwrap() = Duration::from_secs(3);
        let release = Arc::new(tokio::sync::Semaphore::new(0));
        let held = release.clone();
        *authority.worker.lock().unwrap() = Some(tokio::spawn(async move {
            let permit = held.acquire().await.unwrap();
            permit.forget();
            if fail {
                Err(RpcError::new(
                    "unavailable",
                    "controlled dirty authority remains",
                ))
            } else {
                Ok(())
            }
        }));
        let first = tokio::spawn({
            let authority = authority.clone();
            async move { authority.detach().await }
        });
        tokio::time::timeout(Duration::from_secs(1), async {
            while authority.shared.stop.lock().unwrap().is_none() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        let second = tokio::spawn({
            let authority = authority.clone();
            async move { authority.detach().await }
        });
        tokio::time::timeout(Duration::from_secs(1), async {
            while authority.shared.completion.receiver_count() < 2 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert!(!first.is_finished());
        assert!(
            !second.is_finished(),
            "second detach must not pretend the held worker completed"
        );
        release.add_permits(1);
        let left = first.await.unwrap();
        let right = second.await.unwrap();
        assert_eq!(left, right);
        assert_eq!(left.is_err(), fail);
        assert_eq!(
            authority.detach().await,
            left,
            "late caller observes exact same completed worker result"
        );
    }
}

#[tokio::test]
async fn detached_before_attachment_has_no_phantom_work() {
    let authority = RoutingEvents::new(Arc::new(rtc_core::events::NullSink));
    authority.detach().await.unwrap();
    assert!(authority.worker.lock().unwrap().is_none());
    assert_eq!(authority.unresolved(), 0);
}

#[tokio::test]
async fn canceled_first_detach_does_not_lose_actual_worker_completion() {
    let authority = Arc::new(RoutingEvents::new(Arc::new(rtc_core::events::NullSink)));
    authority.enabled.store(true, Ordering::Release);
    *authority.shared.drain_budget.lock().unwrap() = Duration::from_secs(3);
    let release = Arc::new(tokio::sync::Semaphore::new(0));
    let held = release.clone();
    *authority.worker.lock().unwrap() = Some(tokio::spawn(async move {
        let permit = held.acquire().await.unwrap();
        permit.forget();
        Err(RpcError::new("unavailable", "retained dirty projection"))
    }));
    let first = tokio::spawn({
        let authority = authority.clone();
        async move { authority.detach().await }
    });
    tokio::time::timeout(Duration::from_secs(1), async {
        while authority.shared.drainer.lock().unwrap().is_none() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    first.abort();
    assert!(first.await.unwrap_err().is_cancelled());
    release.add_permits(1);
    let result = tokio::time::timeout(Duration::from_secs(1), authority.detach())
        .await
        .unwrap()
        .unwrap_err();
    assert_eq!(result.code, "unavailable");
    assert_eq!(result.message, "retained dirty projection");
    assert!(
        authority
            .shared
            .completion
            .borrow()
            .as_ref()
            .unwrap()
            .is_err()
    );
}
