//! Fixed pion keeps a PeerConnection connected when the remote sends a DTLS
//! close_notify: only the DTLS transport becomes closed. LiveKit runs
//! `handleConnectionFailed` when ICE fails, i.e. once the selected pair has been
//! silent for 10s + 5s (`transport.go:81-82`, `transport.go:895`). An immediate
//! failure would drive the ICE fallback while the signal is still valid and
//! cache a TCP/TLS preference that Go never selects.
use super::*;

fn failed(engine: &mut MediaEngine) -> usize {
    let mut events = Vec::new();
    engine.drain_events(&mut events);
    events
        .iter()
        .filter(|e| matches!(e, MediaEvent::TransportFailed { .. }))
        .count()
}

#[test]
fn remote_close_on_connected_pc_fails_only_at_source_ice_timeout() {
    let start = Instant::now();
    let mut engine = engine(start);
    let tid = transport(&mut engine, PcRole::Publisher, start);
    {
        let t = engine.transports.get_mut(&tid).unwrap();
        t.ice_first_connected_at = Some(start);
        t.selected_ice_last_receive = Some(start);
    }
    failed(&mut engine);
    let closed = start + Duration::from_secs(1);
    engine.handle_event(tid, str0m::Event::Closed, closed);
    engine.run(closed);
    assert_eq!(
        failed(&mut engine),
        0,
        "remote close alone is not a failure"
    );
    engine.handle_timeout(start + Duration::from_secs(14));
    assert_eq!(failed(&mut engine), 0, "selected pair still within 15s");
    engine.handle_timeout(start + Duration::from_millis(15_001));
    assert_eq!(
        failed(&mut engine),
        1,
        "ICE failure after 10s + 5s of silence"
    );
}

#[test]
fn remote_close_before_any_selected_pair_still_fails_immediately() {
    let start = Instant::now();
    let mut engine = engine(start);
    let tid = transport(&mut engine, PcRole::Publisher, start);
    failed(&mut engine);
    engine.handle_event(tid, str0m::Event::Closed, start);
    engine.run(start);
    assert_eq!(failed(&mut engine), 1);
}

#[test]
fn remote_close_during_pending_ice_restart_fails_fifteen_seconds_after_the_close() {
    let start = Instant::now();
    let mut engine = engine(start);
    let tid = transport(&mut engine, PcRole::Subscriber, start);
    {
        let t = engine.transports.get_mut(&tid).unwrap();
        t.ice_first_connected_at = Some(start);
        // simulate SwitchCandidateProtocol: the server ICE restart is in flight.
        t.ice_restart_pending = true;
    }
    failed(&mut engine);
    let closed = start + Duration::from_secs(2);
    engine.handle_event(tid, str0m::Event::Closed, closed);
    engine.run(closed);
    assert_eq!(
        failed(&mut engine),
        0,
        "pending restart does not turn a remote close into a failure"
    );
    engine.handle_timeout(closed + Duration::from_millis(14_999));
    assert_eq!(failed(&mut engine), 0);
    engine.handle_timeout(closed + Duration::from_secs(15));
    assert_eq!(
        failed(&mut engine),
        1,
        "source ICE failure 10s + 5s after the last consent"
    );
}

#[test]
fn never_connected_pc_fails_after_source_ice_checking_timeout() {
    let start = Instant::now();
    let mut engine = engine(start);
    let tid = transport(&mut engine, PcRole::Publisher, start);
    let mut peer = client(start);
    offer(&mut engine, &mut peer, tid, start, |api| {
        api.add_media(MediaKind::Audio, Direction::SendOnly, None, None, None);
    });
    assert_eq!(
        engine.transports[&tid].ice_checking_since,
        Some(start),
        "checking starts with the applied remote description"
    );
    failed(&mut engine);
    // Native exhaustion of candidate pairs must not start the shorter
    // disconnected deadline before an ICE transport was ever connected.
    let disconnected = start + Duration::from_millis(200);
    engine.handle_event(
        tid,
        str0m::Event::IceConnectionStateChange(str0m::IceConnectionState::Disconnected),
        disconnected,
    );
    engine.handle_timeout(start + Duration::from_secs(6));
    assert_eq!(failed(&mut engine), 0);
    // The peer never sends a connectivity check (e.g. TLS-only preference without TURN/TLS).
    engine.handle_timeout(start + Duration::from_secs(15));
    assert_eq!(failed(&mut engine), 0, "pion fails strictly after 10s + 5s");
    engine.handle_timeout(start + Duration::from_millis(15_199));
    assert_eq!(failed(&mut engine), 0, "no connectivity check is due yet");
    engine.handle_timeout(start + Duration::from_millis(15_200));
    assert_eq!(
        failed(&mut engine),
        1,
        "never-connected PC reaches ICE failed"
    );
}
