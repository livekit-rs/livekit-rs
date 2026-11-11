use super::*;
#[test]
fn actual_room_filters_report_transport_subscription_and_participant_generation() {
    let (mut room, now) = room();
    let joined = room.join(params("receiver", 16, false), now).unwrap();
    let effects = room.take_effects();
    let subscriber = transport_of(&effects, &joined.sid, PcRole::Subscriber);
    let publisher = transport_of(&effects, &joined.sid, PcRole::Publisher);
    let track = TrackSid("real-bound-test-track".into());
    room.participants
        .get_mut(&joined.sid)
        .unwrap()
        .subscriptions
        .insert(
            track.clone(),
            super::super::participant::Subscription {
                bound: true,
                desired: true,
                ..Default::default()
            },
        );
    let at = now + Duration::from_secs(6);
    for transport in [publisher, TransportId::next()] {
        room.handle_media_event(
            MediaEvent::SubscriberRtt {
                transport,
                track: track.clone(),
                rtt_ms: 21,
                received_at: at,
            },
            at,
        );
    }
    assert_eq!(
        room.rpc_participant(&Identity("receiver".into())).unwrap(),
        (joined.sid.clone(), 0)
    );
    room.handle_media_event(
        MediaEvent::SubscriberRtt {
            transport: subscriber,
            track: track.clone(),
            rtt_ms: 21,
            received_at: at,
        },
        at,
    );
    assert_eq!(
        room.rpc_participant(&Identity("receiver".into())).unwrap(),
        (joined.sid.clone(), 21)
    );
    let mut resume = params("receiver", 16, false);
    resume.reconnect = true;
    let resumed = room.join(resume, at).unwrap();
    assert_eq!(resumed.sid, joined.sid);
    assert_eq!(
        room.rpc_participant(&Identity("receiver".into()))
            .unwrap()
            .1,
        21
    );
    room.remove_participant(&joined.sid, DisconnectReason::ClientInitiated, false, at);
    let replacement = room.join(params("receiver", 16, false), at).unwrap();
    assert_ne!(replacement.sid, joined.sid);
    room.handle_media_event(
        MediaEvent::SubscriberRtt {
            transport: subscriber,
            track,
            rtt_ms: 99,
            received_at: at + Duration::from_secs(6),
        },
        at + Duration::from_secs(6),
    );
    assert_eq!(
        room.rpc_participant(&Identity("receiver".into())).unwrap(),
        (replacement.sid, 0)
    );
}
