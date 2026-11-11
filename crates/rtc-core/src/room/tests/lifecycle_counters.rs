use super::*;

#[test]
fn accepted_join_active_resume_and_pending_publish_have_distinct_outcomes() {
    let (mut room, now) = room();
    let participant = room.join(params("alice", 16, true), now).unwrap();
    let transport = transport_of(&room.take_effects(), &participant.sid, PcRole::Single);
    assert_eq!(room.lifecycle_counters().participant_rtc_connected, 1);
    assert_eq!(room.lifecycle_counters().participant_rtc_active, 0);
    for _ in 0..2 {
        room.handle_media_event(
            MediaEvent::TransportConnected {
                participant: participant.sid.clone(),
                transport,
            },
            now,
        );
    }
    assert_eq!(room.lifecycle_counters().participant_rtc_active, 1);
    let mut resume = params("alice", 16, true);
    resume.reconnect = true;
    room.join(resume, now).unwrap();
    assert_eq!(room.lifecycle_counters().participant_rtc_connected, 1);
    assert_eq!(room.lifecycle_counters().participant_rtc_active, 2);
    let generation = room.participant_generation(&participant.sid).unwrap();
    for _ in 0..2 {
        room.handle_request(
            &participant.sid,
            generation,
            SessionRequest::AddTrack(add_track(
                "pending-cid",
                TrackKind::Audio,
                TrackSource::Microphone,
            )),
            now,
        );
    }
    assert_eq!(
        room.lifecycle_counters().track_publish_attempts,
        2,
        "accepted pending and queued requests each have one attempt"
    );
    assert_eq!(room.lifecycle_counters().track_publish_success, 0);
    assert_eq!(
        room.published_track_count(),
        0,
        "signaling TrackPublished acknowledgement is not published media"
    );
    room.remove_participant(
        &participant.sid,
        DisconnectReason::ClientInitiated,
        false,
        now,
    );
    assert_eq!(room.lifecycle_counters().track_publish_cancels, 2);
    assert_eq!(
        room.lifecycle_counters().participant_rtc_canceled,
        0,
        "ICE-connected removal is not canceled RTC admission"
    );
}

#[test]
fn cancellation_uses_source_close_reason_and_short_signal_lifetime() {
    for (reason, elapsed, canceled) in [
        (DisconnectReason::ClientInitiated, 10, 1),
        (DisconnectReason::DuplicateIdentity, 10, 1),
        (DisconnectReason::ParticipantRemoved, 1, 0),
        (DisconnectReason::ConnectionTimeout, 1, 0),
        (DisconnectReason::MediaFailure, 1, 0),
        (DisconnectReason::SignalClose, 2, 1),
        (DisconnectReason::SignalClose, 3, 0),
        (DisconnectReason::JoinFailure, 60, 0),
    ] {
        let (mut room, now) = room();
        let joined = room.join(params("alice", 16, true), now).unwrap();
        room.remove_participant(
            &joined.sid,
            reason,
            false,
            now + Duration::from_secs(elapsed),
        );
        room.remove_participant(
            &joined.sid,
            reason,
            false,
            now + Duration::from_secs(elapsed),
        );
        assert_eq!(
            room.lifecycle_counters().participant_rtc_canceled,
            canceled,
            "{reason:?} elapsed={elapsed}"
        );
    }
}

#[test]
fn pending_subscription_retry_is_one_attempt_and_unsubscribe_is_one_cancel() {
    let (mut room, now) = room();
    let joined = room.join(params("alice", 16, true), now).unwrap();
    let missing: TrackSid = "TR_missing".into();
    room.subscribe(&joined.sid, &missing, now);
    room.subscribe(&joined.sid, &missing, now + Duration::from_secs(1));
    room.resolve_subscription(&joined.sid, &missing, now + Duration::from_secs(2));
    assert_eq!(room.lifecycle_counters().track_subscribe_attempts, 1);
    assert_eq!(room.lifecycle_counters().track_subscribe_success, 0);
    room.unsubscribe(&joined.sid, &missing, now);
    room.unsubscribe(&joined.sid, &missing, now);
    assert_eq!(room.lifecycle_counters().track_subscribe_cancels, 1);
    room.subscribe(&joined.sid, &missing, now);
    assert_eq!(room.lifecycle_counters().track_subscribe_attempts, 2);
    room.subscription_failed(&joined.sid, &missing, SubscriptionError::CodecUnsupported);
    room.unsubscribe(&joined.sid, &missing, now);
    assert_eq!(
        room.lifecycle_counters().track_subscribe_cancels,
        1,
        "reported failure is not relabeled cancellation"
    );
}

#[test]
fn delta_accumulation_preserves_retired_generations_and_transport_totals() {
    let mut process = LifecycleCounters {
        participant_signal_connected: 9,
        participant_rtc_init: 4,
        ..Default::default()
    };
    let first = LifecycleCounters {
        track_publish_attempts: 3,
        track_publish_success: 2,
        ..Default::default()
    };
    let mut previous = LifecycleCounters::default();
    process.accumulate_delta(first, &mut previous);
    process.accumulate_delta(first, &mut previous);
    let mut replacement = LifecycleCounters::default();
    process.accumulate_delta(
        LifecycleCounters {
            track_publish_attempts: 1,
            ..Default::default()
        },
        &mut replacement,
    );
    assert_eq!(process.track_publish_attempts, 4);
    assert_eq!(process.track_publish_success, 2);
    assert_eq!(process.participant_signal_connected, 9);
    assert_eq!(process.participant_rtc_init, 4);
}

#[test]
fn actual_publication_and_binding_increment_success_once_without_pending_gauge() {
    let (mut room, now) = room();
    let publisher = room.join(params("publisher", 16, true), now).unwrap();
    let transport = transport_of(&room.take_effects(), &publisher.sid, PcRole::Single);
    let subscriber = room.join(params("subscriber", 16, true), now).unwrap();
    room.take_effects();
    room.handle_request(
        &publisher.sid,
        publisher.generation,
        SessionRequest::AddTrack(add_track(
            "real-audio",
            TrackKind::Audio,
            TrackSource::Microphone,
        )),
        now,
    );
    let track = signals_to(&room.take_effects(), &publisher.sid)
        .into_iter()
        .find_map(|event| match event {
            SignalEvent::TrackPublished { track, .. } => Some(track.sid),
            _ => None,
        })
        .unwrap();
    assert_eq!(room.published_track_count(), 0);
    assert_eq!(room.lifecycle_counters().track_publish_success, 0);
    room.media_arrives(
        RemoteMedia {
            registration: crate::media_api::PublicationRegistration::Pending,
            participant: publisher.sid.clone(),
            transport,
            mid: "0".into(),
            kind: TrackKind::Audio,
            msid_track_id: "real-audio".into(),
            rids: vec![],
            mime_type: "audio/opus".into(),
        },
        now,
    );
    assert_eq!(room.published_track_count(), 1);
    assert_eq!(room.lifecycle_counters().track_publish_attempts, 1);
    assert_eq!(room.lifecycle_counters().track_publish_success, 1);
    room.subscribe(&subscriber.sid, &track, now);
    assert_eq!(room.lifecycle_counters().track_subscribe_attempts, 1);
    assert_eq!(room.lifecycle_counters().track_subscribe_success, 0);
    room.subscription_bound(&subscriber.sid, &track);
    room.subscription_bound(&subscriber.sid, &track);
    assert_eq!(room.lifecycle_counters().track_subscribe_success, 1);
    room.unsubscribe(&subscriber.sid, &track, now);
    assert_eq!(
        room.lifecycle_counters().track_subscribe_cancels,
        0,
        "completed subscription is not pending cancellation"
    );
}

#[test]
fn missing_source_expiry_is_reported_failure_not_user_cancel() {
    let (mut room, now) = room();
    let joined = room.join(params("subscriber", 16, true), now).unwrap();
    let missing: TrackSid = "TR_missing-expiry".into();
    room.subscribe(&joined.sid, &missing, now);
    room.resolve_subscription(&joined.sid, &missing, now + Duration::from_secs(61));
    assert_eq!(room.lifecycle_counters().track_subscribe_attempts, 1);
    assert_eq!(room.lifecycle_counters().track_subscribe_success, 0);
    assert_eq!(room.lifecycle_counters().track_subscribe_cancels, 0);
}

#[test]
fn slow_owner_preparation_is_included_in_short_signal_cancel_boundary() {
    let (mut room, actor_now) = room();
    let started = actor_now.checked_sub(Duration::from_secs(4)).unwrap();
    let joined = room
        .join_with_session_start(params("slow-owner", 16, true), actor_now, started)
        .unwrap();
    room.signal_closed(
        &joined.sid,
        joined.generation,
        actor_now + Duration::from_millis(10),
    );
    assert_eq!(
        room.lifecycle_counters().participant_rtc_canceled,
        0,
        "four seconds before actor execution is an actual failed connection, not quick user cancel"
    );
}
