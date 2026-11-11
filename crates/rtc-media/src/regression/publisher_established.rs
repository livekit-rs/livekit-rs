//! Real authenticated native STUN exchange without delivering DTLS or RTP.
use super::*;
use crate::net::Datagram;
use str0m::net::{Protocol, Receive};
use str0m::{Candidate, Input, Output};
fn stun(data: &[u8]) -> bool {
    data.len() >= 20 && data[0] & 0xc0 == 0 && data[4..8] == [0x21, 0x12, 0xa4, 0x42]
}
#[test]
fn original_pc_established_producer_precedes_dtls_and_rejects_retired_transport_packets() {
    let start = Instant::now();
    let mut engine = engine(start);
    let packets = PacketQueue::default();
    engine.out = Box::new(packets.clone());
    let tid = transport(&mut engine, PcRole::Publisher, start);
    let server = "127.0.0.1:19001".parse().unwrap();
    let peer = "127.0.0.1:19002".parse().unwrap();
    // Native ICE bookkeeping can already say Checking before any remote SDP.
    // That must not establish the public PC identity for SetPermission.
    engine.handle_timeout(start);
    let mut before_sdp = vec![];
    engine.drain_events(&mut before_sdp);
    assert!(
        !before_sdp
            .iter()
            .any(|e| matches!(e, MediaEvent::TransportEstablished { .. }))
    );
    assert!(!engine.transports[&tid].established_reported);
    let candidate = engine
        .transports
        .get_mut(&tid)
        .unwrap()
        .rtc
        .add_local_candidate(Candidate::host(server, "udp").unwrap())
        .unwrap()
        .clone();
    engine
        .transports
        .get_mut(&tid)
        .unwrap()
        .candidates
        .push(candidate);
    let mut client = client(start);
    client.add_local_candidate(Candidate::host(peer, "udp").unwrap());
    let initial = offer(&mut engine, &mut client, tid, start, |change| {
        change.add_channel("native-checking".into());
    });
    let mut observed = initial
        .into_iter()
        .filter(|e| matches!(e,MediaEvent::TransportEstablished{transport,..}if *transport==tid))
        .count();
    let mut authentic_stun = None;
    let mut discarded_dtls = 0;
    for step in 0..300 {
        let now = start + Duration::from_millis(step * 10);
        client.handle_input(Input::Timeout(now)).unwrap();
        loop {
            match client.poll_output().unwrap() {
                Output::Transmit(packet) => {
                    if stun(&packet.contents) {
                        authentic_stun.get_or_insert_with(|| packet.contents.to_vec());
                        engine.handle_datagram(Datagram {
                            transport: tid,
                            proto: Proto::Udp,
                            source: peer,
                            destination: server,
                            data: &packet.contents,
                            received_at: now,
                        });
                    } else {
                        discarded_dtls += 1;
                    }
                }
                Output::Event(str0m::Event::Connected) => {
                    panic!("DTLS packets are deliberately never delivered")
                }
                Output::Event(_) => {}
                Output::Timeout(_) => break,
            }
        }
        engine.handle_timeout(now);
        for bytes in packets.0.lock().unwrap().drain(..).collect::<Vec<_>>() {
            if stun(&bytes) {
                client
                    .handle_input(Input::Receive(
                        now,
                        Receive::new(Protocol::Udp, server, peer, &bytes).unwrap(),
                    ))
                    .unwrap();
            } else {
                discarded_dtls += 1;
            }
        }
        let mut events = vec![];
        engine.drain_events(&mut events);
        for event in events {
            match event {
                MediaEvent::TransportEstablished {
                    participant,
                    transport,
                } => {
                    assert_eq!(transport, tid);
                    assert_eq!(participant.as_str(), "PA_test");
                    observed += 1;
                }
                MediaEvent::TransportConnected { .. } => {
                    panic!("PC!=New must not depend on completed DTLS")
                }
                _ => {}
            }
        }
    }
    assert!(
        authentic_stun.is_some(),
        "must drive actual native ICE packets"
    );
    assert_eq!(
        observed, 1,
        "native state changes only report established once per logical PC"
    );
    assert!(
        discarded_dtls > 0,
        "real handshake must have been held before DTLS completion"
    );
    assert!(!engine.transports[&tid].connected);
    engine.handle_command(
        rtc_core::media_api::MediaCommand::CloseTransport(tid),
        start + Duration::from_secs(4),
    );
    let mut events = vec![];
    engine.drain_events(&mut events);
    engine.handle_datagram(Datagram {
        transport: tid,
        proto: Proto::Udp,
        source: peer,
        destination: server,
        data: &authentic_stun.unwrap(),
        received_at: start + Duration::from_secs(5),
    });
    engine.drain_events(&mut events);
    assert!(
        !events
            .iter()
            .any(|e| matches!(e, MediaEvent::TransportEstablished { .. })),
        "retired native inputs cannot resurrect a transport"
    );
}
