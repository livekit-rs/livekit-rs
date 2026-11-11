use super::*;
use tokio::sync::mpsc;

fn context(receipt: Arc<()>, room_sid: &str) -> Launch {
    Launch {
        key: RoomKey::new(rtc_core::ids::ProjectId::local(), "same-name"),
        room_sid: room_sid.into(),
        job: pb::Job {
            id: "AJ_receipt".into(),
            dispatch_id: "AD_same".into(),
            ..Default::default()
        },
        group: WorkerKey {
            agent_name: "receipt-agent".into(),
            namespace: String::new(),
            job_type: 0,
            deployment: String::new(),
        },
        receipt,
    }
}

#[tokio::test]
async fn real_task_error_panic_abort_and_discovery_drop_release_accepted_receipt() {
    for outcome in ["error", "panic", "abort", "discovery"] {
        let (state, net) = crate::test_support::state().await;
        let (_runtime, rx) = mpsc::channel(1);
        let (_workers, workers) = mpsc::channel(1);
        let (stop, stopped) = watch::channel(false);
        let mut bridge = Bridge::new(
            state.manager.clone(),
            Registry::new(super::super::registry::Limits::default()),
            state.tokens.clone(),
            rx,
            workers,
            stopped,
            super::super::service::ServiceConfig::default(),
        );
        let receipt = Arc::new(());
        let weak = Arc::downgrade(&receipt);
        bridge
            .launch_receipts
            .insert(("RM_old".into(), "AD_same".into()), receipt.clone());
        let work = context(receipt, "RM_old");
        if outcome == "discovery" {
            bridge.discoveries.spawn(async move {
                let _accepted_discovery = work;
            });
        } else {
            bridge.pending.insert(work.job.id.clone(), work.clone());
            let name = outcome.to_owned();
            let task = bridge.tasks.spawn(async move {
                let _accepted_launch = work;
                if name == "panic" {
                    panic!("controlled accepted assignment future panic");
                }
                if name == "abort" {
                    std::future::pending::<()>().await;
                }
                ("AJ_receipt".into(), Err(WorkerError::Closed))
            });
            bridge.task_jobs.insert(task.id(), "AJ_receipt".into());
            if outcome == "abort" {
                task.abort();
            }
        }
        let running = tokio::spawn(bridge.run());
        tokio::time::timeout(std::time::Duration::from_secs(2), async {
            while weak.strong_count() != 0 { tokio::task::yield_now().await; }
        }).await.expect("actual JoinSet completion/drop must release both task and pending ownership exactly once");
        stop.send(true).unwrap();
        running.await.unwrap();
        assert_eq!(weak.strong_count(), 0);
        state.agents.shutdown().await;
        state.manager.close_all().await;
        net.shutdown();
    }
}

#[tokio::test]
async fn room_sid_scoped_receipts_and_queued_fanout_do_not_cross_same_name_replacement() {
    let (state, net) = crate::test_support::state().await;
    let (_runtime, rx) = mpsc::channel(1);
    let (_workers, workers) = mpsc::channel(1);
    let (_stop, stopped) = watch::channel(false);
    let mut bridge = Bridge::new(
        state.manager.clone(),
        Registry::new(super::super::registry::Limits::default()),
        state.tokens.clone(),
        rx,
        workers,
        stopped,
        super::super::service::ServiceConfig::default(),
    );
    let old = Arc::new(());
    let new = Arc::new(());
    bridge
        .launch_receipts
        .insert(("RM_old".into(), "AD_same".into()), old.clone());
    bridge
        .launch_receipts
        .insert(("RM_new".into(), "AD_same".into()), new.clone());
    bridge.queue.push_back(context(old, "RM_old"));
    bridge.deleted.insert(("RM_old".into(), "AD_same".into()));
    bridge.reap_tombstones();
    assert!(
        bridge
            .deleted
            .contains(&("RM_old".into(), "AD_same".into())),
        "accepted queued/discovered fanout is still part of original dispatch launch"
    );
    assert!(
        !bridge
            .deleted
            .contains(&("RM_new".into(), "AD_same".into()))
    );
    bridge.queue.clear();
    bridge.reap_tombstones();
    assert!(
        !bridge
            .launch_receipts
            .contains_key(&("RM_old".into(), "AD_same".into()))
    );
    assert!(bridge.dispatch_launch_pending(&"RM_new".into(), "AD_same"));
    drop(new);
    bridge.reap_tombstones();
    assert!(bridge.launch_receipts.is_empty());
    // Shutdown explicitly cancels and joins a still-waiting discovery instead
    // of waiting for an unavailable peer or inheriting it into a new room.
    let receipt = Arc::new(());
    let weak = Arc::downgrade(&receipt);
    bridge
        .launch_receipts
        .insert(("RM_waiting".into(), "AD_waiting".into()), receipt.clone());
    bridge.discoveries.spawn(async move {
        let _owned = receipt;
        std::future::pending::<()>().await;
    });
    _stop.send(true).unwrap();
    tokio::time::timeout(std::time::Duration::from_secs(1), bridge.run())
        .await
        .expect("service shutdown must cancel/join accepted discovery rather than hold forever");
    assert_eq!(weak.strong_count(), 0);
    state.agents.shutdown().await;
    state.manager.close_all().await;
    net.shutdown();
}
