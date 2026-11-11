//! Public Room state machine inputs, not physical native/actor proof.
use super::*;
fn denial() -> SubscriptionPermission {
    SubscriptionPermission {
        all_participants: false,
        track_permissions: vec![],
    }
}
#[test]
fn actual_permission_regrant_does_not_reuse_old_bound_subscription_deadline() {
    for enabled in [false, true] {
        let (mut r, at) = room();
        Arc::make_mut(&mut r.policy).reconnect_on_subscription_error = enabled;
        let a = r.join(params("publisher", 17, true), at).unwrap();
        let effects = r.take_effects();
        let transport = transport_of(&effects, &a.sid, PcRole::Single);
        r.handle_media_event(
            MediaEvent::TransportConnected {
                participant: a.sid.clone(),
                transport,
            },
            at,
        );
        r.handle_request(
            &a.sid,
            a.generation,
            SessionRequest::AddTrack(add_track(
                "audio",
                TrackKind::Audio,
                TrackSource::Microphone,
            )),
            at,
        );
        r.media_arrives(
            RemoteMedia {
                registration: crate::media_api::PublicationRegistration::Pending,
                participant: a.sid.clone(),
                transport,
                mid: "1".into(),
                kind: TrackKind::Audio,
                msid_track_id: "audio".into(),
                rids: vec![],
                mime_type: "audio/opus".into(),
            },
            at,
        );
        let track = r.participants[&a.sid].info.tracks[0].sid.clone();
        r.take_effects();
        let b = r.join(params("subscriber", 17, true), at).unwrap();
        let effects = r.take_effects();
        let bt = transport_of(&effects, &b.sid, PcRole::Single);
        r.handle_media_event(
            MediaEvent::TransportConnected {
                participant: b.sid.clone(),
                transport: bt,
            },
            at,
        );
        r.handle_media_event(
            MediaEvent::SubscriptionPrepared {
                subscriber: b.sid.clone(),
                track_sid: track.clone(),
            },
            at,
        );
        r.handle_media_event(
            MediaEvent::SubscriptionBound {
                subscriber: b.sid.clone(),
                track_sid: track.clone(),
                mid: "down".into(),
            },
            at,
        );
        assert!(r.participants[&b.sid].subscriptions[&track].bound);
        r.take_effects();
        r.tick(at + Duration::from_secs(20));
        r.take_effects();
        r.handle_request(
            &a.sid,
            a.generation,
            SessionRequest::SubscriptionPermission(denial()),
            at + Duration::from_secs(21),
        );
        let revoked = r.take_effects();
        assert!(revoked.iter().any(|e|matches!(e,Effect::Media(MediaCommand::Unsubscribe{subscriber,track_sid})if subscriber==&b.sid&&track_sid==&track)));
        r.tick(at + Duration::from_secs(40));
        r.take_effects();
        r.handle_request(
            &a.sid,
            a.generation,
            SessionRequest::SubscriptionPermission(SubscriptionPermission {
                all_participants: true,
                track_permissions: vec![],
            }),
            at + Duration::from_secs(41),
        );
        let granted = r.take_effects();
        assert!(granted.iter().any(|e|matches!(e,Effect::Media(MediaCommand::Subscribe(spec))if spec.subscriber==b.sid&&spec.track_sid==track)));
        r.handle_media_event(
            MediaEvent::SubscriptionPrepared {
                subscriber: b.sid.clone(),
                track_sid: track.clone(),
            },
            at + Duration::from_secs(41),
        );
        r.tick(at + Duration::from_secs(45));
        let fresh = r.take_effects();
        assert!(!fresh.iter().any(|e|matches!(e,Effect::Signal{event:SignalEvent::SubscriptionResponse{track_sid,..},..}if track_sid==&track)),"new prepared downtrack must not inherit its old bound epoch");
        assert!(
            r.participants.contains_key(&b.sid),
            "reconnect flag cannot make a fresh grant immediately fatal"
        );
        r.handle_media_event(
            MediaEvent::SubscriptionBound {
                subscriber: b.sid.clone(),
                track_sid: track.clone(),
                mid: "down".into(),
            },
            at + Duration::from_secs(46),
        );
        r.tick(at + Duration::from_secs(70));
        assert!(!r.take_effects().iter().any(|e|matches!(e,Effect::Signal{event:SignalEvent::SubscriptionResponse{track_sid,..},..}if track_sid==&track)));
    }
}

#[test]
fn already_allowed_permission_update_does_not_renew_an_unbound_attempt() {
    let (mut r, at) = room();
    let a = r.join(params("publisher", 17, true), at).unwrap();
    let effects = r.take_effects();
    let transport = transport_of(&effects, &a.sid, PcRole::Single);
    r.handle_media_event(
        MediaEvent::TransportConnected {
            participant: a.sid.clone(),
            transport,
        },
        at,
    );
    r.handle_request(
        &a.sid,
        a.generation,
        SessionRequest::AddTrack(add_track(
            "audio",
            TrackKind::Audio,
            TrackSource::Microphone,
        )),
        at,
    );
    r.media_arrives(
        RemoteMedia {
            registration: crate::media_api::PublicationRegistration::Pending,
            participant: a.sid.clone(),
            transport,
            mid: "1".into(),
            kind: TrackKind::Audio,
            msid_track_id: "audio".into(),
            rids: vec![],
            mime_type: "audio/opus".into(),
        },
        at,
    );
    let track = r.participants[&a.sid].info.tracks[0].sid.clone();
    r.take_effects();
    let b = r.join(params("subscriber", 17, true), at).unwrap();
    let effects = r.take_effects();
    let bt = transport_of(&effects, &b.sid, PcRole::Single);
    r.handle_media_event(
        MediaEvent::TransportConnected {
            participant: b.sid.clone(),
            transport: bt,
        },
        at,
    );
    r.handle_media_event(
        MediaEvent::SubscriptionPrepared {
            subscriber: b.sid.clone(),
            track_sid: track.clone(),
        },
        at,
    );

    r.handle_request(
        &a.sid,
        a.generation,
        SessionRequest::SubscriptionPermission(SubscriptionPermission {
            all_participants: true,
            track_permissions: vec![],
        }),
        at + Duration::from_secs(10),
    );
    r.take_effects();
    r.tick(at + Duration::from_secs(16));
    assert!(r.take_effects().iter().any(|e|matches!(e,Effect::Signal{to,event:SignalEvent::SubscriptionResponse{track_sid,..}}if to==&b.sid&&track_sid==&track)),"already-allowed is not a fresh publisher grant");
}

#[test]
fn publisher_denial_only_retires_the_denied_recipient_monitor() {
    let (mut r, at) = room();
    let a = r.join(params("publisher", 17, true), at).unwrap();
    let effects = r.take_effects();
    let transport = transport_of(&effects, &a.sid, PcRole::Single);
    r.handle_media_event(
        MediaEvent::TransportConnected {
            participant: a.sid.clone(),
            transport,
        },
        at,
    );
    r.handle_request(
        &a.sid,
        a.generation,
        SessionRequest::AddTrack(add_track(
            "audio",
            TrackKind::Audio,
            TrackSource::Microphone,
        )),
        at,
    );
    r.media_arrives(
        RemoteMedia {
            registration: crate::media_api::PublicationRegistration::Pending,
            participant: a.sid.clone(),
            transport,
            mid: "1".into(),
            kind: TrackKind::Audio,
            msid_track_id: "audio".into(),
            rids: vec![],
            mime_type: "audio/opus".into(),
        },
        at,
    );
    let track = r.participants[&a.sid].info.tracks[0].sid.clone();
    r.take_effects();
    let b = r.join(params("subscriber", 17, true), at).unwrap();
    let effects = r.take_effects();
    let bt = transport_of(&effects, &b.sid, PcRole::Single);
    r.handle_media_event(
        MediaEvent::TransportConnected {
            participant: b.sid.clone(),
            transport: bt,
        },
        at,
    );
    r.handle_media_event(
        MediaEvent::SubscriptionPrepared {
            subscriber: b.sid.clone(),
            track_sid: track.clone(),
        },
        at,
    );

    let c = r.join(params("unaffected", 17, true), at).unwrap();
    let effects = r.take_effects();
    let ct = transport_of(&effects, &c.sid, PcRole::Single);
    r.handle_media_event(
        MediaEvent::TransportConnected {
            participant: c.sid.clone(),
            transport: ct,
        },
        at,
    );
    r.handle_media_event(
        MediaEvent::SubscriptionPrepared {
            subscriber: c.sid.clone(),
            track_sid: track.clone(),
        },
        at,
    );
    r.take_effects();
    r.handle_request(
        &a.sid,
        a.generation,
        SessionRequest::SubscriptionPermission(SubscriptionPermission {
            all_participants: false,
            track_permissions: vec![TrackPermission {
                participant_sid: None,
                participant_identity: Some("unaffected".into()),
                all_tracks: true,
                track_sids: vec![],
            }],
        }),
        at + Duration::from_secs(10),
    );
    r.take_effects();
    r.tick(at + Duration::from_secs(16));
    let effects = r.take_effects();
    assert!(effects.iter().any(|e|matches!(e,Effect::Signal{to,event:SignalEvent::SubscriptionResponse{track_sid,..}}if to==&c.sid&&track_sid==&track)),"unrelated actual attempt deadline must survive publisher policy change");
    assert!(!effects.iter().any(|e|matches!(e,Effect::Signal{to,event:SignalEvent::SubscriptionResponse{..}}if to==&b.sid)),"denied recipient is ineligible for a binding timeout");
    r.handle_request(
        &a.sid,
        a.generation,
        SessionRequest::SubscriptionPermission(SubscriptionPermission {
            all_participants: true,
            track_permissions: vec![],
        }),
        at + Duration::from_secs(21),
    );
    r.take_effects();
    r.handle_media_event(
        MediaEvent::SubscriptionPrepared {
            subscriber: b.sid.clone(),
            track_sid: track.clone(),
        },
        at + Duration::from_secs(21),
    );
    r.tick(at + Duration::from_secs(36));
    assert!(!r.take_effects().iter().any(|e|matches!(e,Effect::Signal{to,event:SignalEvent::SubscriptionResponse{..}}if to==&b.sid)),"strict >15s boundary remains");
    r.tick(at + Duration::from_secs(37));
    assert!(r.take_effects().iter().any(|e|matches!(e,Effect::Signal{to,event:SignalEvent::SubscriptionResponse{track_sid,..}}if to==&b.sid&&track_sid==&track)),"fresh prepared attempt must still fail after its own deadline");
}
