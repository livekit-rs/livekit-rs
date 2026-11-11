use super::*;
use rtc_core::events::EventSink;
fn projection(sid: &str) -> RoomProjection {
    RoomProjection {
        room: crate::convert::room_info_from_proto(&lk_protocol::livekit::Room {
            name: "initial-pump-room".into(),
            sid: "RM_initial".into(),
            ..Default::default()
        }),
        participants: vec![crate::convert::participant_info_from_proto(
            &lk_protocol::livekit::ParticipantInfo {
                identity: "alice".into(),
                sid: sid.into(),
                ..Default::default()
            },
        )],
        terminal: false,
    }
}
fn authority() -> (Arc<RoutingEvents>, RoomProjection) {
    let authority = Arc::new(RoutingEvents::new(Arc::new(rtc_core::events::NullSink)));
    let value = projection("PA_old");
    let mut ledger = ledger::Ledger::new(2).unwrap();
    ledger.reserve(&value.room).unwrap();
    ledger.start(&value.room);
    ledger.project(value.clone());
    *authority.shared.ledger.lock().unwrap() = Some(ledger);
    authority.enabled.store(true, Ordering::Release);
    (authority, value)
}
async fn subscribed(authority: &RoutingEvents) {
    tokio::time::timeout(Duration::from_secs(1), async {
        while authority.shared.publication.receiver_count() != 1 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
}
#[tokio::test]
async fn actual_publication_watch_waits_attempt_not_successful_reconcile_ack() {
    let (authority, value) = authority();
    let waiter = tokio::spawn({
        let a = authority.clone();
        async move {
            a.wait_participant_admitted("initial-pump-room", "alice", "PA_old")
                .await
        }
    });
    subscribed(&authority).await;
    assert!(
        !waiter.is_finished(),
        "Join output alone is not store admission"
    );
    authority.participant_store_attempted(&value);
    waiter.await.unwrap().unwrap();
    assert_eq!(
        authority
            .shared
            .ledger
            .lock()
            .unwrap()
            .as_ref()
            .unwrap()
            .unresolved(),
        1,
        "request worker starts after attempt without claiming successful catalog ACK"
    );
    assert_eq!(authority.shared.publication.receiver_count(), 0);
}
#[tokio::test]
async fn actual_publication_watch_releases_on_replacement_registration_error_and_cancel() {
    for mode in 0..4 {
        let (authority, value) = authority();
        let waiter = tokio::spawn({
            let a = authority.clone();
            async move {
                a.wait_participant_admitted("initial-pump-room", "alice", "PA_old")
                    .await
            }
        });
        subscribed(&authority).await;
        match mode {
            0 => authority.project_room(projection("PA_replacement")),
            1 => authority.participant_topics_failed(
                &value,
                &RpcError::new("unavailable", "actual topic registration failed"),
            ),
            2 => {
                let mut retired = value.clone();
                retired.terminal = true;
                retired.participants.clear();
                authority.project_room(retired);
            }
            _ => {
                waiter.abort();
                assert!(waiter.await.unwrap_err().is_cancelled());
                assert_eq!(authority.shared.publication.receiver_count(), 0);
                continue;
            }
        }
        let error = tokio::time::timeout(Duration::from_secs(1), waiter)
            .await
            .unwrap()
            .unwrap()
            .unwrap_err();
        assert_eq!(
            error.code,
            if mode == 1 { "unavailable" } else { "aborted" }
        );
        assert_eq!(authority.shared.publication.receiver_count(), 0);
    }
}
