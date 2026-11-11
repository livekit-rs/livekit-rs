//! Dual-PC `SwitchCandidateProtocol(TLS)` without TURN/TLS: the rejoined session never
//! connects. Fixed Go has two outcomes depending on timing, and Rust must produce the
//! same observable state for each order of events:
//!
//! - Both PCs reach ICE failed (pion: >15 s checking) while the signal is still valid:
//!   `handleConnectionFailed` counts two non-short failures within a minute
//!   (`failureCountThreshold = 2`), `getNext` for a non-UDP-preferred lowest type
//!   returns `ICT_NONE`, the config is cached per (room, identity), and the next join
//!   uses any transport (Go recovered over UDP).
//! - The first failure's `onAnyTransportFailed` (leave + `CloseSignalConnection`)
//!   is processed first: `HandleSignalSourceClose` closes the never-connected
//!   participant and the second failure has no participant; TLS stays cached.
use super::*;
use crate::transport_fallback::IcePreference;

fn tls_session(room: &mut Room, now: Instant) -> (Joined, TransportId, TransportId) {
    let key = room.key.clone();
    room.policy
        .transport_fallback
        .remember(&key, &"tls".into(), IcePreference::Tls, now);
    let joined = room.join(params("tls", 16, false), now).unwrap();
    let effects = room.take_effects();
    let publisher = transport_of(&effects, &joined.sid, PcRole::Publisher);
    let subscriber = transport_of(&effects, &joined.sid, PcRole::Subscriber);
    assert!(
        effects.iter().any(|e| matches!(e, Effect::Media(MediaCommand::CreateTransport(spec)) if spec.ice_preference == IcePreference::Tls)),
        "the rejoin starts with the cached TLS preference"
    );
    (joined, publisher, subscriber)
}

fn failed(room: &mut Room, sid: &ParticipantSid, transport: TransportId, now: Instant) {
    room.handle_media_event(
        MediaEvent::TransportFailed {
            recovery_epoch: 0,
            participant: sid.clone(),
            transport,
        },
        now,
    );
}

#[test]
fn both_pcs_fail_before_signal_close_falls_back_to_any_like_go_ict_none() {
    let (mut room, now) = room();
    let (joined, publisher, subscriber) = tls_session(&mut room, now);
    // The SDK pings every few seconds, keeping the signal recent (hasRecentSignalLocked).
    room.handle_request(
        &joined.sid,
        joined.generation,
        SessionRequest::SignalActivity,
        now + Duration::from_secs(10),
    );
    room.take_effects();
    let at = now + Duration::from_millis(15_100);
    failed(&mut room, &joined.sid, subscriber, at);
    let first = room.take_effects();
    assert!(
        signals_to(&first, &joined.sid).iter().any(|s| matches!(
            s,
            SignalEvent::Leave {
                action: LeaveAction::Resume,
                ..
            }
        )),
        "onAnyTransportFailed: leave with resume"
    );
    assert!(
        first
            .iter()
            .any(|e| matches!(e, Effect::CloseSignal { to } if *to == joined.sid))
    );
    assert_eq!(
        room.policy
            .transport_fallback
            .cached(&room.key, &"tls".into(), at),
        IcePreference::Tls,
        "one non-short failure is below failureCountThreshold"
    );
    // The second PC fails 24 ms later, before the signal close is processed.
    let second = at + Duration::from_millis(24);
    failed(&mut room, &joined.sid, publisher, second);
    assert_eq!(
        room.policy
            .transport_fallback
            .cached(&room.key, &"tls".into(), second),
        IcePreference::Any,
        "two failures with a valid signal: Go caches ICT_NONE"
    );
    // HandleSignalSourceClose then closes the never-connected participant.
    room.signal_closed(
        &joined.sid,
        joined.generation,
        second + Duration::from_millis(1),
    );
    assert!(!room.participants.contains_key(&joined.sid));
    // The next join (SDK full reconnect) uses any transport.
    let next = room
        .join(params("tls", 16, false), second + Duration::from_secs(5))
        .unwrap();
    let effects = room.take_effects();
    assert!(effects.iter().any(|e| matches!(e, Effect::Media(MediaCommand::CreateTransport(spec)) if spec.ice_preference == IcePreference::Any)));
    assert_ne!(next.sid, joined.sid);
}

#[test]
fn signal_close_before_second_failure_keeps_tls_like_go_stuck_reconnecting() {
    let (mut room, now) = room();
    let (joined, publisher, subscriber) = tls_session(&mut room, now);
    // The SDK pings every few seconds, keeping the signal recent (hasRecentSignalLocked).
    room.handle_request(
        &joined.sid,
        joined.generation,
        SessionRequest::SignalActivity,
        now + Duration::from_secs(10),
    );
    room.take_effects();
    let at = now + Duration::from_millis(15_100);
    failed(&mut room, &joined.sid, subscriber, at);
    room.take_effects();
    // The close of the signal (leave sent, socket closed) is processed first.
    room.signal_closed(
        &joined.sid,
        joined.generation,
        at + Duration::from_millis(3),
    );
    assert!(
        !room.participants.contains_key(&joined.sid),
        "HandleSignalSourceClose closes a participant that never connected"
    );
    failed(
        &mut room,
        &joined.sid,
        publisher,
        at + Duration::from_millis(24),
    );
    assert_eq!(
        room.policy.transport_fallback.cached(
            &room.key,
            &"tls".into(),
            at + Duration::from_millis(24)
        ),
        IcePreference::Tls,
        "a failure without a participant is not counted; the next join stays TLS-only"
    );
    room.join(params("tls", 16, false), at + Duration::from_secs(5))
        .unwrap();
    let effects = room.take_effects();
    assert!(effects.iter().any(|e| matches!(e, Effect::Media(MediaCommand::CreateTransport(spec)) if spec.ice_preference == IcePreference::Tls)));
}

#[test]
fn signal_close_after_either_ice_path_connects_preserves_session_before_dtls() {
    for role in [PcRole::Publisher, PcRole::Subscriber] {
        let (mut room, now) = room();
        let (joined, publisher, subscriber) = tls_session(&mut room, now);
        let transport = if role == PcRole::Publisher {
            publisher
        } else {
            subscriber
        };
        room.handle_media_event(
            MediaEvent::IcePath {
                transport,
                connection: crate::transport_fallback::IceConnection::Udp,
                first_connected: Some(now),
            },
            now,
        );
        assert!(!room.participants[&joined.sid].ever_active);
        room.signal_closed(
            &joined.sid,
            joined.generation,
            now + Duration::from_millis(1),
        );
        assert!(!room.participants[&joined.sid].signal_connected);
        let mut request = params("tls", 16, false);
        request.reconnect = true;
        request.participant_sid = Some(joined.sid.clone());
        let resumed = room.join(request, now + Duration::from_millis(2)).unwrap();
        assert!(resumed.resumed);
        assert_eq!(resumed.sid, joined.sid);
    }
}
