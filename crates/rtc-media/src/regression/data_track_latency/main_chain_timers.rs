//! Main-chain attribution for the SCTP retransmission timer.
//!
//! The fractional RTT/RTO history is compared against the fixed original-Pion observer at the
//! library tier. What this file pins on the assembled engine is that the association deadline
//! reaches `Transport::next_timeout` and that recovery of a lost encrypted record is driven by
//! feeding the engine clock, not by wall-clock progress alone.
use super::wire_alignment_consumers::{reliable_buffered, reliable_pair};
use super::*;

fn pump_client_outputs(pair: &mut Pair, now: Instant, feed_engine: bool) {
    loop {
        match pair.client.poll_output().unwrap() {
            Output::Transmit(p) => {
                if feed_engine {
                    pair.engine.handle_datagram(Datagram {
                        transport: pair.tid,
                        proto: Proto::Udp,
                        source: pair.client_addr,
                        destination: pair.server_addr,
                        data: &p.contents,
                        received_at: now,
                    })
                }
            }
            Output::Event(Event::ChannelOpen(id, _)) if id == pair.channel => pair.opened = true,
            Output::Event(Event::ChannelClose(id)) if id == pair.channel => pair.closed = true,
            Output::Event(Event::ChannelData(d)) => pair.received.push(d.data),
            Output::Timeout(_) => break,
            _ => {}
        }
    }
}

/// Drive the client peer while the engine receives neither a datagram timestamp nor a timeout.
/// str0m advances its timers on `Input::Receive` as well as on `Input::Timeout`, so skipping only
/// `MediaEngine::handle_timeout` would not stop the association timer: the engine has to be left
/// un-driven for the timer input to be the isolated variable.
///
/// Records the engine queued while it *was* still driven are already on the wire, so they are
/// delivered before the client is advanced and the engine gets nothing back: their arrival cannot
/// be counted as output of an un-driven engine.
fn pump_without_engine_clock(pair: &mut Pair, tick: u64) {
    let now = pair.start + Duration::from_millis(tick);
    let in_flight: Vec<_> = pair.packets.0.lock().unwrap().drain(..).collect();
    for bytes in in_flight {
        pair.client
            .handle_input(Input::Receive(
                now,
                Receive::new(Protocol::Udp, pair.server_addr, pair.client_addr, &bytes).unwrap(),
            ))
            .unwrap();
        pump_client_outputs(pair, now, false);
    }
    pair.client.handle_input(Input::Timeout(now)).unwrap();
    pump_client_outputs(pair, now, false);
    let outgoing: Vec<_> = pair.packets.0.lock().unwrap().drain(..).collect();
    assert!(
        outgoing.is_empty(),
        "an un-driven engine must emit nothing: {} queued record(s)",
        outgoing.len()
    );
}

fn reliable_generation(pair: &mut Pair) -> Option<u64> {
    let (cid, _) = pair.engine.transports[&pair.tid]
        .channel_for(DataKind::Reliable)
        .expect("real reliable DCEP channel");
    pair.engine
        .transports
        .get_mut(&pair.tid)
        .unwrap()
        .rtc
        .channel(cid)
        .unwrap()
        .sctp_association_generation()
}

#[test]
fn reliable_sctp_recovery_is_attributed_to_the_engine_clock_not_wall_clock() {
    let mut pair = reliable_pair();
    let mut tick = (0..3000)
        .find(|&tick| {
            pair.step(tick);
            pair.opened
                && pair.client.is_connected()
                && pair.engine.transports[&pair.tid]
                    .channel_for(DataKind::Reliable)
                    .is_some()
        })
        .expect("genuine reliable ICE/DTLS/SCTP channel must open");
    while tick < 3000 && reliable_buffered(&mut pair) != 0 {
        tick += 1;
        pair.step(tick);
    }
    assert_eq!(reliable_buffered(&mut pair), 0);
    // The restart-generation accessor must read the live association of the assembled engine;
    // without it no main-chain restart observation could be attributed at all.
    assert_eq!(
        reliable_generation(&mut pair),
        Some(0),
        "connected association has observed no peer restart"
    );
    for _ in 0..100 {
        tick += 1;
        pair.step(tick);
    }
    assert!(pair.received.is_empty());

    let payload: Vec<u8> = (0..50 * 1024).map(|i| (i * 31 + 7) as u8).collect();
    pair.drop_application_records = 1;
    let loss = pair.start + Duration::from_millis(tick + 1);
    pair.engine.handle_timeout(loss);
    pair.engine.handle_command(
        rtc_core::media_api::MediaCommand::SendData {
            to: "PA_test".into(),
            kind: DataKind::Reliable,
            payload: payload.clone(),
        },
        loss,
    );
    assert_eq!(
        reliable_buffered(&mut pair),
        payload.len(),
        "real accepted reliable fragmented message"
    );
    tick += 1;
    pair.step(tick);
    assert_eq!(
        pair.dropped_application_records, 1,
        "controlled actual encrypted wire loss on the original send"
    );
    assert_eq!(pair.drop_application_records, 0);
    assert_ne!(
        reliable_buffered(&mut pair),
        0,
        "the lost record stays owed, so a later recovery cannot be a silent local drop"
    );
    let deadline = pair.engine.transports[&pair.tid].next_timeout;
    assert!(
        deadline >= loss && deadline - loss <= Duration::from_secs(4),
        "engine must advertise the association retransmit deadline, not a far-future idle slot: {deadline:?}"
    );

    // Phase A: 500 ms of real client activity while the engine is never pumped. Any recovery in
    // this phase could only come from a clock the engine advanced without being driven.
    let phase_a_end = tick + 500;
    while tick < phase_a_end {
        tick += 1;
        pump_without_engine_clock(&mut pair, tick);
    }
    assert!(
        pair.received.is_empty(),
        "withholding the engine clock must withhold retransmission"
    );
    assert!(!pair.closed);
    assert_ne!(
        reliable_buffered(&mut pair),
        0,
        "phase A is attributable to the missing engine clock, not to an abandoned send"
    );

    // Phase B: resume the engine pump on the same association and wire state.
    let bound = tick + 12_000;
    while tick < bound && pair.received.is_empty() {
        tick += 1;
        pair.step(tick);
    }
    assert_eq!(
        pair.received,
        vec![payload],
        "whole original reliable message recovered by the engine-driven retransmission"
    );
    assert!(!pair.closed);
    assert_eq!(
        reliable_generation(&mut pair),
        Some(0),
        "a retransmission must not be an association restart"
    );
}
