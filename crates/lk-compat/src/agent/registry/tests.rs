use super::*;
fn registration() -> Registration {
    Registration::from_request("AW_test".into(), 1, &pb::RegisterWorkerRequest::default()).unwrap()
}
fn job() -> pb::Job {
    pb::Job {
        id: "AJ_test".into(),
        room: Some(pb::Room {
            name: "room".into(),
            ..Default::default()
        }),
        ..Default::default()
    }
}
#[test]
fn registration_permissions_and_unknown_type_follow_source() {
    let r = registration();
    assert!(r.permissions.can_publish);
    assert!(r.permissions.can_subscribe);
    assert!(r.permissions.can_publish_data);
    assert!(r.permissions.can_update_metadata);
    let req = pb::RegisterWorkerRequest {
        r#type: 999,
        ..Default::default()
    };
    assert_eq!(
        Registration::from_request("w".into(), 1, &req).unwrap_err(),
        WorkerError::UnknownJobType
    );
    let req = pb::RegisterWorkerRequest {
        allowed_permissions: Some(pb::ParticipantPermission::default()),
        ..Default::default()
    };
    assert!(
        !Registration::from_request("w".into(), 1, &req)
            .unwrap()
            .permissions
            .can_publish
    );
}
#[test]
fn stale_worker_status_does_not_regress_load_or_draining_but_zero_seq_applies() {
    let r = Registry::new(Limits::default());
    let (tx, _) = mpsc::channel(2);
    r.register(registration(), tx).unwrap();
    r.update_worker(
        "AW_test",
        &pb::UpdateWorkerStatus {
            status: Some(1),
            seq: 5,
            load: 0.9,
            draining: true,
            ..Default::default()
        },
    )
    .unwrap();
    r.update_worker(
        "AW_test",
        &pb::UpdateWorkerStatus {
            status: Some(0),
            seq: 4,
            load: 0.1,
            draining: false,
            ..Default::default()
        },
    )
    .unwrap();
    {
        let s = r.inner.lock().unwrap();
        let w = &s.workers["AW_test"];
        assert_eq!(w.status, 1);
        assert_eq!(w.load, 0.9);
        assert!(w.draining);
    }
    r.update_worker(
        "AW_test",
        &pb::UpdateWorkerStatus {
            status: Some(0),
            seq: 0,
            load: 0.1,
            ..Default::default()
        },
    )
    .unwrap();
    let s = r.inner.lock().unwrap();
    let w = &s.workers["AW_test"];
    assert_eq!(w.status, 0);
    assert_eq!(w.status_seq, 5);
    assert_eq!(w.load, 0.1);
    assert!(!w.draining);
}
#[tokio::test]
async fn availability_accept_then_assignment_registers_running_before_worker_updates_and_ends_only_on_real_update()
 {
    let r = Registry::new(Limits::default());
    let (tx, mut rx) = mpsc::channel(2);
    r.register(registration(), tx).unwrap();
    let copy = r.clone();
    let task = tokio::spawn(async move {
        copy.assign("AW_test", job(), 100, |_, _, _| {
            Ok("unit-test-token-factory".into())
        })
        .await
    });
    let offer = rx.recv().await.unwrap();
    assert!(matches!(
        offer.message.message,
        Some(pb::server_message::Message::Availability(_))
    ));
    let _ = offer.written.send(Ok(()));
    assert_eq!(
        r.job_state("AW_test", "AJ_test").unwrap_err(),
        WorkerError::JobNotFound
    );
    r.availability(
        "AW_test",
        pb::AvailabilityResponse {
            job_id: "AJ_test".into(),
            available: true,
            participant_identity: "agent".into(),
            ..Default::default()
        },
    )
    .unwrap();
    let assignment = rx.recv().await.unwrap();
    assert!(matches!(
        assignment.message.message,
        Some(pb::server_message::Message::Assignment(_))
    ));
    let _ = assignment.written.send(Ok(()));
    let state = task.await.unwrap().unwrap();
    assert_eq!(state.status, 1);
    assert_eq!(state.participant_identity, "agent");
    assert_eq!(r.job_state("AW_test", "AJ_test").unwrap().status, 1);
    let end = r
        .update_job(
            "AW_test",
            &pb::UpdateJobStatus {
                job_id: "AJ_test".into(),
                status: 2,
                error: String::new(),
            },
            500,
        )
        .unwrap();
    assert_eq!(end.ended_at, 500);
    assert_eq!(
        r.job_state("AW_test", "AJ_test").unwrap_err(),
        WorkerError::JobNotFound
    );
}
#[tokio::test]
async fn disconnect_cancels_pending_availability_without_creating_running_job() {
    let r = Registry::new(Limits::default());
    let (tx, mut rx) = mpsc::channel(1);
    r.register(registration(), tx).unwrap();
    let copy = r.clone();
    let task = tokio::spawn(async move {
        copy.assign("AW_test", job(), 100, |_, _, _| {
            panic!("no acceptance must not mint token")
        })
        .await
    });
    let offer = rx.recv().await.unwrap();
    let _ = offer.written.send(Ok(()));
    assert!(r.disconnect("AW_test").is_empty());
    assert_eq!(task.await.unwrap().unwrap_err(), WorkerError::Closed);
    assert!(r.registration("AW_test").is_none());
}
#[tokio::test]
async fn abort_while_assignment_queue_backpressured_rolls_back_unsent_running_state() {
    let r = Registry::new(Limits::default());
    let (tx, mut rx) = mpsc::channel(1);
    let fill = tx.clone();
    r.register(registration(), tx).unwrap();
    let copy = r.clone();
    let task = tokio::spawn(async move {
        copy.assign("AW_test", job(), 100, |_, _, _| {
            Ok("unit-test-token-factory".into())
        })
        .await
    });
    let offer = rx.recv().await.unwrap();
    let _ = offer.written.send(Ok(()));
    let (written, _receipt) = oneshot::channel();
    fill.send(Outbound {
        message: pb::ServerMessage::default(),
        written,
    })
    .await
    .unwrap();
    r.availability(
        "AW_test",
        pb::AvailabilityResponse {
            job_id: "AJ_test".into(),
            available: true,
            ..Default::default()
        },
    )
    .unwrap();
    for _ in 0..100 {
        if r.job_state("AW_test", "AJ_test").is_ok() {
            break;
        }
        tokio::task::yield_now().await;
    }
    assert!(r.job_state("AW_test", "AJ_test").is_ok());
    task.abort();
    let _ = task.await;
    assert_eq!(
        r.job_state("AW_test", "AJ_test").unwrap_err(),
        WorkerError::JobNotFound
    );
}
#[test]
fn source_weighted_selection_uses_available_status_and_does_not_mistake_draining_for_full() {
    let r = Registry::new(Limits::default());
    let (tx, _) = mpsc::channel(2);
    r.register(registration(), tx).unwrap();
    let mut second = registration();
    second.worker_id = "AW_second".into();
    let (tx, _) = mpsc::channel(2);
    r.register(second, tx).unwrap();
    r.update_worker(
        "AW_test",
        &pb::UpdateWorkerStatus {
            load: 0.2,
            draining: true,
            ..Default::default()
        },
    )
    .unwrap();
    r.update_worker(
        "AW_second",
        &pb::UpdateWorkerStatus {
            load: 0.8,
            ..Default::default()
        },
    )
    .unwrap();
    let key = registration().key;
    assert_eq!(r.select(&key, &Default::default(), 0.1).unwrap(), "AW_test");
    assert_eq!(
        r.select(&key, &Default::default(), 0.9).unwrap(),
        "AW_second"
    );
    r.update_worker(
        "AW_test",
        &pb::UpdateWorkerStatus {
            status: Some(1),
            load: 0.2,
            ..Default::default()
        },
    )
    .unwrap();
    assert_eq!(
        r.select(&key, &Default::default(), 0.1).unwrap(),
        "AW_second"
    );
    r.update_worker(
        "AW_second",
        &pb::UpdateWorkerStatus {
            load: 1.,
            ..Default::default()
        },
    )
    .unwrap();
    assert_eq!(
        r.select(&key, &Default::default(), 0.1).unwrap_err(),
        WorkerError::NoCapacity
    );
}
