use super::*;
use crate::transport_fallback::{IceConnection, IcePreference};
#[test]
fn actual_room_failure_sets_both_pc_preferences_and_resume_uses_owner_cache_before_signaling() {
    let (mut room, now) = room();
    let joined = room.join(params("fallback", 16, false), now).unwrap();
    let initial = room.take_effects();
    let publisher = transport_of(&initial, &joined.sid, PcRole::Publisher);
    let subscriber = transport_of(&initial, &joined.sid, PcRole::Subscriber);
    for transport in [publisher, subscriber] {
        room.handle_media_event(
            MediaEvent::IcePath {
                transport,
                connection: IceConnection::Udp,
                first_connected: Some(now),
            },
            now,
        );
    }
    room.handle_media_event(
        MediaEvent::TransportFailed {
            recovery_epoch: 0,
            participant: joined.sid.clone(),
            transport: publisher,
        },
        now,
    );
    let effects = room.take_effects();
    for transport in [publisher, subscriber] {
        assert!(effects.iter().any(|effect|matches!(effect,Effect::Media(MediaCommand::SetIcePreference{transport:actual,preference:IcePreference::Tcp}) if *actual==transport)),"both native PCs must receive TCP preference");
    }
    assert!(
        signals_to(&effects, &joined.sid)
            .iter()
            .any(|signal| matches!(
                signal,
                SignalEvent::Leave {
                    action: LeaveAction::Resume,
                    ..
                }
            ))
    );
    let mut resume = params("fallback", 16, false);
    resume.reconnect = true;
    resume.participant_sid = Some(joined.sid.clone());
    let resumed = room.join(resume, now + Duration::from_millis(1)).unwrap();
    assert!(resumed.resumed);
    assert!(signals_to(&room.take_effects(),&joined.sid).iter().any(|signal|matches!(signal,SignalEvent::Reconnect(info) if info.ice_preference==IcePreference::Tcp)));
    let policy = room.policy.clone();
    let key = room.key.clone();
    drop(room);
    let mut replacement = Room::new(
        key,
        policy,
        Arc::new(EchoData),
        &Default::default(),
        now + Duration::from_secs(1),
    );
    let fresh = replacement
        .join(params("fallback", 16, false), now + Duration::from_secs(1))
        .unwrap();
    let effects = replacement.take_effects();
    assert!(signals_to(&effects, &fresh.sid).iter().any(
        |signal| matches!(signal,SignalEvent::Join(info) if info.ice_preference==IcePreference::Tcp)
    ));
    assert!(
        effects
            .iter()
            .filter_map(|effect| match effect {
                Effect::Media(MediaCommand::CreateTransport(spec)) => Some(spec),
                _ => None,
            })
            .all(|spec| spec.ice_preference == IcePreference::Tcp),
        "cached preference is applied before the first native offer"
    );
}
#[test]
fn actual_room_disabled_fallback_or_stale_signal_does_not_change_native_candidates() {
    for (enabled, age) in [(false, 0u64), (true, 15)] {
        let (mut room, now) = room();
        Arc::make_mut(&mut room.policy).transport_fallback.allow_tcp = enabled;
        let joined = room.join(params("no-fallback", 16, true), now).unwrap();
        let transport = transport_of(&room.take_effects(), &joined.sid, PcRole::Single);
        room.handle_media_event(
            MediaEvent::IcePath {
                transport,
                connection: IceConnection::Udp,
                first_connected: Some(now),
            },
            now,
        );
        room.handle_media_event(
            MediaEvent::TransportFailed {
                recovery_epoch: 0,
                participant: joined.sid.clone(),
                transport,
            },
            now + Duration::from_secs(age),
        );
        assert!(!room.take_effects().iter().any(|effect| matches!(
            effect,
            Effect::Media(MediaCommand::SetIcePreference {
                preference: IcePreference::Tcp | IcePreference::Tls,
                ..
            })
        )));
        assert_eq!(
            room.participants[&joined.sid].fallback.preference,
            IcePreference::Any
        );
    }
}

#[test]
fn client_short_failure_preserves_original_resume_snapshot_then_restores_cached_preference() {
    for reason in [2, 3] {
        let (mut room, now) = room();
        let joined = room.join(params("client-short", 16, false), now).unwrap();
        let initial = room.take_effects();
        for role in [PcRole::Publisher, PcRole::Subscriber] {
            let transport = transport_of(&initial, &joined.sid, role);
            room.handle_media_event(
                MediaEvent::IcePath {
                    transport,
                    connection: IceConnection::Udp,
                    first_connected: Some(now),
                },
                now,
            );
        }
        room.take_effects();
        let mut request = params("client-short", 16, false);
        request.reconnect = true;
        request.participant_sid = Some(joined.sid.clone());
        request.reconnect_reason = reason;
        assert!(
            room.join(request, now + Duration::from_secs(1))
                .unwrap()
                .resumed
        );
        let effects = room.take_effects();
        assert!(signals_to(&effects,&joined.sid).iter().any(|event|matches!(event,SignalEvent::Reconnect(info) if info.ice_preference==IcePreference::Any && info.ice_servers_preference==IcePreference::Any)));
        let preferences = effects
            .iter()
            .filter_map(|effect| match effect {
                Effect::Media(MediaCommand::SetIcePreference { preference, .. }) => {
                    Some(*preference)
                }
                _ => None,
            })
            .collect::<Vec<_>>();
        assert!(
            preferences.contains(&IcePreference::Tcp),
            "actual client failure callback must run"
        );
        assert_eq!(
            preferences.last(),
            Some(&IcePreference::Any),
            "fixed caller restores earlier cached snapshot before ICE restart"
        );
    }
}

#[test]
fn decoded_activity_has_no_rtt_side_effect_and_requires_current_owner_generation() {
    let (mut room, now) = room();
    let joined = room.join(params("activity-only", 16, false), now).unwrap();
    room.take_effects();
    room.handle_request(
        &joined.sid,
        joined.generation,
        SessionRequest::SignalRtt(73),
        now,
    );
    room.take_effects();
    let seen = now + Duration::from_secs(3);
    room.handle_request(
        &joined.sid,
        joined.generation,
        SessionRequest::SignalActivity,
        seen,
    );
    assert_eq!(room.participants[&joined.sid].fallback.last_signal, seen);
    assert_eq!(room.participants[&joined.sid].signal_rtt_ms, 73);
    assert!(
        room.take_effects().is_empty(),
        "activity alone cannot manufacture RTT/native transport work"
    );
    room.handle_request(
        &joined.sid,
        SessionGeneration(joined.generation.0.wrapping_add(1)),
        SessionRequest::SignalActivity,
        seen + Duration::from_secs(1),
    );
    assert_eq!(
        room.participants[&joined.sid].fallback.last_signal, seen,
        "mismatched generation cannot refresh a current owner"
    );
    assert_eq!(room.participants[&joined.sid].signal_rtt_ms, 73);
}

#[test]
fn dual_pc_resume_rejects_queued_old_epoch_failures_but_keeps_new_epoch_failure_real() {
    let (mut room, now) = room();
    let joined = room.join(params("dual-epoch", 16, false), now).unwrap();
    let effects = room.take_effects();
    let publisher = transport_of(&effects, &joined.sid, PcRole::Publisher);
    let subscriber = transport_of(&effects, &joined.sid, PcRole::Subscriber);
    for transport in [publisher, subscriber] {
        room.handle_media_event(
            MediaEvent::IcePath {
                transport,
                connection: IceConnection::Udp,
                first_connected: Some(now),
            },
            now,
        );
    }
    room.handle_media_event(
        MediaEvent::TransportFailed {
            participant: joined.sid.clone(),
            transport: subscriber,
            recovery_epoch: 0,
        },
        now,
    );
    assert!(
        signals_to(&room.take_effects(), &joined.sid)
            .iter()
            .any(|e| matches!(
                e,
                SignalEvent::Leave {
                    action: LeaveAction::Resume,
                    ..
                }
            ))
    );
    let mut resume = params("dual-epoch", 16, false);
    resume.reconnect = true;
    resume.participant_sid = Some(joined.sid.clone());
    assert!(
        room.join(resume, now + Duration::from_millis(1))
            .unwrap()
            .resumed
    );
    let effects = room.take_effects();
    assert!(effects.iter().any(|e| matches!(e, Effect::Media(MediaCommand::BeginTransportRecovery { participant, recovery_epoch: 1 }) if participant == &joined.sid)));
    for transport in [publisher, subscriber] {
        room.handle_media_event(
            MediaEvent::TransportFailed {
                participant: joined.sid.clone(),
                transport,
                recovery_epoch: 0,
            },
            now + Duration::from_millis(8),
        );
        room.handle_media_event(
            MediaEvent::IceTcpConnectTimedOut {
                participant: joined.sid.clone(),
                transport,
                recovery_epoch: 0,
            },
            now + Duration::from_millis(8),
        );
    }
    room.handle_media_event(
        MediaEvent::NegotiationFailed {
            participant: joined.sid.clone(),
            recovery_epoch: 0,
        },
        now + Duration::from_millis(8),
    );
    assert!(
        room.take_effects().is_empty(),
        "queued old epoch cannot trigger second Resume, reset TCP policy or full reconnect"
    );
    assert_eq!(
        room.participants[&joined.sid].fallback.preference,
        IcePreference::Tcp
    );
    room.handle_media_event(
        MediaEvent::TransportFailed {
            participant: joined.sid.clone(),
            transport: publisher,
            recovery_epoch: 1,
        },
        now + Duration::from_secs(1),
    );
    assert!(
        signals_to(&room.take_effects(), &joined.sid)
            .iter()
            .any(|e| matches!(
                e,
                SignalEvent::Leave {
                    action: LeaveAction::Resume,
                    ..
                }
            )),
        "new epoch failure is never time-window deduplicated"
    );
}
