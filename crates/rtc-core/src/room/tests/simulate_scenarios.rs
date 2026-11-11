//! Fixed Go room.onSimulateScenario parity (SPEC-0077 M06, official SDK simulateScenario differential).
use super::*;
use crate::session::Simulate;
use crate::transport_fallback::IcePreference;

fn simulate(room: &mut Room, joined: &Joined, sim: Simulate, now: Instant) {
    room.handle_request(
        &joined.sid,
        joined.generation,
        SessionRequest::Simulate(sim),
        now,
    );
}

#[test]
fn speaker_update_sends_active_now_and_inactive_after_the_requested_seconds() {
    let (mut room, now) = room();
    let alice = room.join(params("alice", 16, true), now).unwrap();
    let bob = room.join(params("bob", 16, true), now).unwrap();
    room.take_effects();
    simulate(
        &mut room,
        &alice,
        Simulate::SpeakerUpdate { seconds: 3 },
        now,
    );
    let effects = room.take_effects();
    assert!(signals_to(&effects, &alice.sid).iter().any(
        |s| matches!(s, SignalEvent::SpeakersChanged(v) if v.len() == 1 && v[0].sid == alice.sid && v[0].active && v[0].level == 0.9)
    ));
    // Bob is not subscribed to Alice: Go still writes the (empty) scoped update.
    assert!(
        signals_to(&effects, &bob.sid)
            .iter()
            .any(|s| matches!(s, SignalEvent::SpeakersChanged(v) if v.is_empty()))
    );
    // The inactive update waits for the deadline, and tick schedules it.
    assert!(room.tick(now + Duration::from_secs(1)) <= now + Duration::from_secs(3));
    assert!(
        !signals_to(&room.take_effects(), &alice.sid)
            .iter()
            .any(|s| matches!(s, SignalEvent::SpeakersChanged(_)))
    );
    room.tick(now + Duration::from_secs(3));
    assert!(signals_to(&room.take_effects(), &alice.sid).iter().any(
        |s| matches!(s, SignalEvent::SpeakersChanged(v) if v.len() == 1 && v[0].sid == alice.sid && !v[0].active && v[0].level == 0.0)
    ));
    room.tick(now + Duration::from_secs(4));
    assert!(
        !signals_to(&room.take_effects(), &alice.sid)
            .iter()
            .any(|s| matches!(s, SignalEvent::SpeakersChanged(_))),
        "sent once"
    );
}

#[test]
fn node_failure_and_migration_close_the_participant_without_a_leave() {
    for (sim, reason) in [
        (Simulate::NodeFailure, DisconnectReason::ServerShutdown),
        (Simulate::Migration, DisconnectReason::Migration),
    ] {
        let (mut room, now) = room();
        let alice = room.join(params("alice", 16, true), now).unwrap();
        let bob = room.join(params("bob", 16, true), now).unwrap();
        room.take_effects();
        simulate(&mut room, &alice, sim, now);
        let effects = room.take_effects();
        assert!(
            !signals_to(&effects, &alice.sid)
                .iter()
                .any(|s| matches!(s, SignalEvent::Leave { .. })),
            "no leave to the closed participant"
        );
        assert!(
            effects
                .iter()
                .any(|e| matches!(e, Effect::CloseSignal { to } if *to == alice.sid))
        );
        assert!(signals_to(&effects, &bob.sid).iter().any(|s| matches!(s, SignalEvent::ParticipantUpdate(infos)
            if infos.iter().any(|i| i.sid == alice.sid && i.state == ParticipantState::Disconnected))));
        assert!(
            !room.participants.contains_key(&alice.sid)
                || room.participants[&alice.sid].is_disconnected(),
            "participant is closed"
        );
        assert!(
            signals_to(&effects, &bob.sid)
                .iter()
                .any(|s| matches!(s, SignalEvent::ParticipantUpdate(infos)
            if infos.iter().any(|i| i.sid == alice.sid && i.disconnect_reason == reason))),
            "Go ToDisconnectReason"
        );
        // The client's resume finds no session: full reconnect, as fixed Go.
        let mut resume = params("alice", 16, true);
        resume.reconnect = true;
        resume.participant_sid = Some(alice.sid.clone());
        assert!(
            room.join(resume, now + Duration::from_millis(5))
                .map(|j| !j.resumed)
                .unwrap_or(true)
        );
    }
}

#[test]
fn switch_candidate_protocol_sets_both_pc_preferences_restarts_subscriber_ice_and_is_remembered() {
    for (kind, preference) in [(1, IcePreference::Tcp), (2, IcePreference::Tls)] {
        let (mut room, now) = room();
        let alice = room.join(params("alice", 16, false), now).unwrap();
        let initial = room.take_effects();
        let publisher = transport_of(&initial, &alice.sid, PcRole::Publisher);
        let subscriber = transport_of(&initial, &alice.sid, PcRole::Subscriber);
        simulate(
            &mut room,
            &alice,
            Simulate::SwitchCandidateProtocol(kind),
            now,
        );
        let effects = room.take_effects();
        for transport in [publisher, subscriber] {
            assert!(effects.iter().any(|e| matches!(e, Effect::Media(MediaCommand::SetIcePreference { transport: t, preference: p }) if *t == transport && *p == preference)));
        }
        assert!(effects.iter().any(|e| matches!(e, Effect::Media(MediaCommand::IceRestart { transport }) if *transport == subscriber)));
        // The SDK follows with a reconnect: the resumed session carries the preference.
        let mut resume = params("alice", 16, false);
        resume.reconnect = true;
        resume.participant_sid = Some(alice.sid.clone());
        assert!(
            room.join(resume, now + Duration::from_millis(1))
                .unwrap()
                .resumed
        );
        assert!(signals_to(&room.take_effects(), &alice.sid).iter().any(
            |s| matches!(s, SignalEvent::Reconnect(info) if info.ice_preference == preference)
        ));
    }
}
