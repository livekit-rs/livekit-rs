//! Actual encrypted ICE/DTLS/SCTP writer regression. No synthetic ChannelOpen.
use super::*;
use crate::net::Datagram;
use str0m::net::{Protocol, Receive};
use str0m::{Candidate, Event, Input, Output};

struct Pair {
    engine: MediaEngine,
    client: Rtc,
    packets: PacketQueue,
    tid: TransportId,
    channel: str0m::channel::ChannelId,
    start: Instant,
    server_addr: std::net::SocketAddr,
    client_addr: std::net::SocketAddr,
    opened: bool,
    closed: bool,
    seen: Vec<MediaEvent>,
    received: Vec<Vec<u8>>,
    drop_application_records: usize,
    dropped_application_records: usize,
}
impl Pair {
    fn new() -> Self {
        Self::new_with_fast(0)
    }
    fn new_with_fast(fast: i64) -> Self {
        let start = Instant::now();
        let mut engine = engine(start);
        engine.cfg.rtc.sctp.fast_rtx_wnd = fast;
        let packets = PacketQueue::default();
        engine.out = Box::new(packets.clone());
        let tid = transport(&mut engine, PcRole::Single, start);
        let server_addr = "127.0.0.1:9201".parse().unwrap();
        let client_addr = "127.0.0.1:9202".parse().unwrap();
        let t = engine.transports.get_mut(&tid).unwrap();
        let candidate = t
            .rtc
            .add_local_candidate(Candidate::host(server_addr, "udp").unwrap())
            .unwrap()
            .clone();
        t.candidates.push(candidate);
        let mut client = client(start);
        client
            .add_local_candidate(Candidate::host(client_addr, "udp").unwrap())
            .unwrap();
        let mut channel = None;
        offer(&mut engine, &mut client, tid, start, |change| {
            channel = Some(
                change.add_channel_with_config(str0m::channel::ChannelConfig {
                    label: "_data_track".into(),
                    ordered: false,
                    reliability: str0m::channel::Reliability::MaxRetransmits { retransmits: 0 },
                    ..Default::default()
                }),
            );
        });
        Self {
            engine,
            client,
            packets,
            tid,
            channel: channel.unwrap(),
            start,
            server_addr,
            client_addr,
            opened: false,
            closed: false,
            seen: vec![],
            received: vec![],
            drop_application_records: 0,
            dropped_application_records: 0,
        }
    }
    fn step(&mut self, tick: u64) {
        let now = self.start + Duration::from_millis(tick);
        self.client.handle_input(Input::Timeout(now)).unwrap();
        loop {
            match self.client.poll_output().unwrap() {
                Output::Transmit(p) => self.engine.handle_datagram(Datagram {
                    transport: self.tid,
                    proto: Proto::Udp,
                    source: self.client_addr,
                    destination: self.server_addr,
                    data: &p.contents,
                    received_at: now,
                }),
                Output::Event(Event::ChannelOpen(id, _)) if id == self.channel => {
                    self.opened = true
                }
                Output::Event(Event::ChannelClose(id)) if id == self.channel => self.closed = true,
                Output::Event(Event::ChannelData(d)) => self.received.push(d.data),
                Output::Timeout(_) => break,
                _ => {}
            }
        }
        self.engine.handle_timeout(now);
        let mut events = vec![];
        self.engine.drain_events(&mut events);
        self.seen.extend(events);
        let outgoing: Vec<_> = self.packets.0.lock().unwrap().drain(..).collect();
        for bytes in outgoing {
            // Actual encrypted DTLS application records only; never inspect or
            // synthesize plaintext SCTP DATA/ACKs. Armed after real channel idle.
            if self.drop_application_records > 0 && bytes.first() == Some(&23) {
                self.drop_application_records -= 1;
                self.dropped_application_records += 1;
                continue;
            }
            self.client
                .handle_input(Input::Receive(
                    now,
                    Receive::new(Protocol::Udp, self.server_addr, self.client_addr, &bytes)
                        .unwrap(),
                ))
                .unwrap();
            // Every mutation is drained by the next iteration before timeout.
            loop {
                match self.client.poll_output().unwrap() {
                    Output::Transmit(p) => self.engine.handle_datagram(Datagram {
                        transport: self.tid,
                        proto: Proto::Udp,
                        source: self.client_addr,
                        destination: self.server_addr,
                        data: &p.contents,
                        received_at: now,
                    }),
                    Output::Event(Event::ChannelOpen(id, _)) if id == self.channel => {
                        self.opened = true
                    }
                    Output::Event(Event::ChannelClose(id)) if id == self.channel => {
                        self.closed = true
                    }
                    Output::Event(Event::ChannelData(d)) => self.received.push(d.data),
                    Output::Timeout(_) => break,
                    _ => {}
                }
            }
        }
    }
    fn connect(&mut self) -> u64 {
        for tick in 0..3000 {
            self.step(tick);
            if self.opened
                && self.client.is_connected()
                && self.engine.data_track_channel(&"PA_test".into()).is_some()
            {
                return tick;
            }
        }
        panic!("genuine ICE/DTLS/SCTP channel must open");
    }
    fn buffered(&mut self) -> usize {
        let (tid, cid) = self.engine.data_track_channel(&"PA_test".into()).unwrap();
        self.engine
            .transports
            .get_mut(&tid)
            .unwrap()
            .rtc
            .channel(cid)
            .unwrap()
            .buffered_amount()
    }
}
#[test]
fn cold_actual_data_track_writer_does_not_drop_at_old_static_64k() {
    let mut pair = Pair::new();
    let tick = pair.connect();
    let before = pair.buffered();
    for i in 0..5u8 {
        pair.engine
            .send_data_track(&"PA_test".into(), &vec![i; 20_000]);
    }
    assert_eq!(
        pair.buffered() - before,
        100_000,
        "source has no valid busy-rate sample: fixed 64KiB is not the target-latency contract"
    );
    for t in tick + 1..tick + 3000 {
        pair.step(t);
        if pair.received.len() == 5 {
            break;
        }
    }
    assert_eq!(
        pair.received.len(),
        5,
        "all five actual encrypted messages must arrive"
    );
    for i in 0..5u8 {
        assert!(pair.received.iter().any(|v| v == &vec![i; 20_000]));
    }
}
#[test]
fn configured_zero_actual_data_track_writer_bypasses_latency_and_native_static_budget() {
    let mut pair = Pair::new();
    pair.engine.cfg.rtc.data_track_target_latency = rtc_core::config::PolicyDuration::ZERO;
    let tick = pair.connect();
    let before = pair.buffered();
    for i in 0..10u8 {
        pair.engine.send_data_track_at(
            &"PA_test".into(),
            &vec![i; 20_000],
            pair.start + Duration::from_millis(tick),
        );
    }
    assert_eq!(pair.buffered() - before, 200_000);
    for t in tick + 1..tick + 6000 {
        pair.step(t);
        if pair.received.len() == 10 {
            break;
        }
    }
    assert_eq!(pair.received.len(), 10);
    for i in 0..10u8 {
        assert!(pair.received.iter().any(|v| v == &vec![i; 20_000]));
    }
}

#[test]
fn actual_undrained_sctp_busy_samples_apply_latency_but_zero_really_bypasses() {
    for (target_ns, expected) in [(0, 20_000), (100_000_000, 9_000)] {
        let mut pair = Pair::new();
        pair.engine.cfg.rtc.data_track_target_latency =
            rtc_core::config::PolicyDuration::from_nanos(target_ns);
        let mut tick = pair.connect();
        // ChannelOpen precedes the genuine encrypted SACK of its DCEP ACK.
        // Complete that same handshake inside the original connection deadline;
        // do not inject/reset a buffer or relax the zero-control-byte assertion.
        while tick < 3000 && pair.buffered() != 0 {
            tick += 1;
            pair.step(tick);
        }
        assert_eq!(
            pair.buffered(),
            0,
            "genuine channel establishment must settle control bytes before data cohort"
        );
        let start = pair.start + Duration::from_millis(tick + 1);
        pair.engine
            .send_data_track_at(&"PA_test".into(), &vec![1; 1000], start);
        pair.engine.run(start);
        // Deliberately retain genuine encrypted output: no remote delivery/ACK.
        // The source busy sample is 100ms with zero actual drained bytes.
        for i in 1..20u8 {
            let at = start + Duration::from_millis(100);
            pair.engine
                .send_data_track_at(&"PA_test".into(), &vec![i; 1000], at);
            pair.engine.run(at);
        }
        assert_eq!(
            pair.buffered(),
            expected,
            "actual SCTP buffer, not injected calculator sample: target {target_ns}"
        );
    }
}

fn source_receive_boundary_pair() -> Pair {
    let start = Instant::now();
    let mut engine = engine(start);
    let packets = PacketQueue::default();
    engine.out = Box::new(packets.clone());
    let tid = transport(&mut engine, PcRole::Single, start);
    let server_addr = "127.0.0.1:9301".parse().unwrap();
    let client_addr = "127.0.0.1:9302".parse().unwrap();
    let t = engine.transports.get_mut(&tid).unwrap();
    let candidate = t
        .rtc
        .add_local_candidate(Candidate::host(server_addr, "udp").unwrap())
        .unwrap()
        .clone();
    t.candidates.push(candidate);
    let mut client = client(start);
    client
        .add_local_candidate(Candidate::host(client_addr, "udp").unwrap())
        .unwrap();
    let mut change = client.sdp_api();
    let channel = change.add_channel_with_config(str0m::channel::ChannelConfig {
        label: "_data_track".into(),
        ordered: false,
        reliability: str0m::channel::Reliability::MaxRetransmits { retransmits: 0 },
        ..Default::default()
    });
    change.add_channel("_reliable".into());
    let (offer, pending) = change.apply().unwrap();
    engine.on_remote_description(tid, SdpType::Offer, offer.to_sdp_string(), 1, start);
    let mut events = vec![];
    engine.drain_events(&mut events);
    let answer = events
        .iter()
        .find_map(|e| {
            if let MediaEvent::LocalDescription {
                kind: SdpType::Answer,
                sdp,
                ..
            } = e
            {
                Some(sdp)
            } else {
                None
            }
        })
        .unwrap();
    assert!(
        answer.contains("a=max-message-size:65535"),
        "actual server SDP must advertise source receive capacity"
    );
    // Deliberate protocol-negative peer ignores the advertised receive limit.
    // This changes only its local assumption, never server parsing or crypto;
    // it is not an official SDK correctness claim.
    let ignored = answer.replace("a=max-message-size:65535", "a=max-message-size:262144");
    client
        .sdp_api()
        .accept_answer(pending, SdpAnswer::from_sdp_string(&ignored).unwrap())
        .unwrap();
    Pair {
        engine,
        client,
        packets,
        tid,
        channel,
        start,
        server_addr,
        client_addr,
        opened: false,
        closed: false,
        seen: vec![],
        received: vec![],
        drop_application_records: 0,
        dropped_application_records: 0,
    }
}
#[test]
fn actual_inbound_over_65535_closes_only_data_track_reader_not_association() {
    let mut pair = source_receive_boundary_pair();
    let tick = pair.connect();
    assert!(
        pair.client
            .channel(pair.channel)
            .unwrap()
            .write(true, &vec![9; 65536])
            .unwrap()
    );
    for t in tick + 1..tick + 6000 {
        pair.step(t);
        if pair.closed {
            break;
        }
    }
    assert!(
        pair.closed,
        "actual authenticated over-limit SCTP message must close its reader/channel"
    );
    assert!(
        !pair
            .seen
            .iter()
            .any(|e| matches!(e,MediaEvent::DataReceived{payload,..} if payload.len()==65536))
    );
    assert!(
        pair.client.is_connected(),
        "one reader error must not terminate ICE/DTLS association"
    );
    let at = pair.start + Duration::from_millis(tick + 6001);
    pair.engine.send_data(
        &"PA_test".into(),
        DataKind::Reliable,
        b"independent-reliable-survives",
    );
    pair.engine.run(at);
    for t in tick + 6001..tick + 9000 {
        pair.step(t);
        if pair
            .received
            .iter()
            .any(|v| v == b"independent-reliable-survives")
        {
            break;
        }
    }
    assert!(
        pair.received
            .iter()
            .any(|v| v == b"independent-reliable-survives")
    );
}
#[test]
fn actual_outbound_respects_remote_large_capability_independent_of_local_65535() {
    let mut pair = Pair::new();
    pair.engine.cfg.rtc.data_track_target_latency = rtc_core::config::PolicyDuration::ZERO;
    let tick = pair.connect();
    let payload = vec![0x56; 200_000];
    pair.engine.send_data_track_at(
        &"PA_test".into(),
        &payload,
        pair.start + Duration::from_millis(tick),
    );
    for t in tick + 1..tick + 9000 {
        pair.step(t);
        if pair.received.iter().any(|v| v == &payload) {
            break;
        }
    }
    assert!(
        pair.received.iter().any(|v| v == &payload),
        "remote advertises256KiB; local65535 must not become outbound truncation"
    );
}

#[test]
fn actual_enabled_fast_pr_zero_retransmits_keeps_whole_fragmented_first_send_and_ack_debt() {
    for fast in [0, 3000] {
        let mut pair = Pair::new_with_fast(fast);
        pair.engine.cfg.rtc.data_track_target_latency = rtc_core::config::PolicyDuration::ZERO;
        let tick = pair.connect();
        let before = pair.buffered();
        let payload = vec![0x39; 50_000];
        let now = pair.start + Duration::from_millis(tick);
        pair.engine
            .send_data_track_at(&"PA_test".into(), &payload, now);
        assert_eq!(pair.buffered() - before, payload.len());
        // Run genuine SCTP original-send selection without delivering any peer
        // ACK. PR0 must neither erase pending tail nor invent buffer release.
        pair.engine.run(now);
        assert_eq!(pair.buffered() - before, payload.len());
        for at in tick + 1..tick + 6000 {
            pair.step(at);
            if pair.received.iter().any(|v| v == &payload) && pair.buffered() == 0 {
                break;
            }
        }
        assert!(
            pair.received.iter().any(|v| v == &payload),
            "fast={fast}: actual authenticated50KiB single DataTrack message must remain complete"
        );
        assert_eq!(
            pair.buffered(),
            0,
            "fast={fast}: actual peer ACK must release source buffer debt"
        );
    }
}

#[test]
fn default_fast_zero_actual_encrypted_fragmented_max_retransmits_zero_recovers_loss() {
    let mut pair = Pair::new();
    pair.engine.cfg.rtc.data_track_target_latency = rtc_core::config::PolicyDuration::ZERO;
    let mut tick = pair.connect();
    while tick < 3000 && pair.buffered() != 0 {
        tick += 1;
        pair.step(tick);
    }
    assert_eq!(
        pair.buffered(),
        0,
        "actual DCEP/SACK complete before data cohort"
    );
    for _ in 0..100 {
        tick += 1;
        pair.step(tick);
    }
    assert_eq!(pair.buffered(), 0);
    assert!(pair.received.is_empty());
    let payload: Vec<u8> = (0..50 * 1024).map(|i| (i * 31 + 7) as u8).collect();
    pair.drop_application_records = 1;
    pair.engine.send_data_track_at(
        &"PA_test".into(),
        &payload,
        pair.start + Duration::from_millis(tick + 1),
    );
    assert_eq!(
        pair.buffered(),
        payload.len(),
        "real accepted fragmented message"
    );
    for t in tick + 1..tick + 12001 {
        pair.step(t);
        if !pair.received.is_empty() {
            break;
        }
    }
    assert_eq!(
        pair.dropped_application_records, 1,
        "controlled actual encrypted wire loss"
    );
    assert_eq!(pair.drop_application_records, 0);
    assert!(!pair.closed);
    assert_eq!(
        pair.received,
        vec![payload],
        "whole original message, exactly once"
    );
}

#[path = "data_track_latency/wire_alignment_consumers.rs"]
mod wire_alignment_consumers;
