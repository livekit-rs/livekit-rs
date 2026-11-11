//! Actual old public Engine SendData control: one encrypted record lost.
//! Same real Pair driver, 50 KiB bytes, and 12-second bound as the original MaxRTX0 gate.
use super::*;

fn reliable_pair() -> Pair {
    let start = Instant::now();
    let mut engine = engine(start);
    engine.cfg.rtc.sctp.fast_rtx_wnd = 0;
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
                label: "_reliable".into(),
                ordered: true,
                reliability: str0m::channel::Reliability::Reliable,
                ..Default::default()
            }),
        );
    });
    Pair {
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

fn reliable_buffered(pair: &mut Pair) -> usize {
    let (cid, kind) = pair.engine.transports[&pair.tid]
        .channel_for(DataKind::Reliable)
        .expect("real reliable DCEP channel");
    assert_eq!(kind, DataKind::Reliable);
    pair.engine
        .transports
        .get_mut(&pair.tid)
        .unwrap()
        .rtc
        .channel(cid)
        .unwrap()
        .buffered_amount()
}

#[test]
fn default_fast_zero_actual_encrypted_reliable_50k_recovers_first_record_loss() {
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
    assert_eq!(
        reliable_buffered(&mut pair),
        0,
        "actual DCEP/SACK complete before data cohort"
    );
    for _ in 0..100 {
        tick += 1;
        pair.step(tick);
    }
    assert_eq!(reliable_buffered(&mut pair), 0);
    assert!(pair.received.is_empty());
    let payload: Vec<u8> = (0..50 * 1024).map(|i| (i * 31 + 7) as u8).collect();
    pair.drop_application_records = 1;
    let now = pair.start + Duration::from_millis(tick + 1);
    pair.engine.handle_timeout(now);
    pair.engine.handle_command(
        rtc_core::media_api::MediaCommand::SendData {
            to: "PA_test".into(),
            kind: DataKind::Reliable,
            payload: payload.clone(),
        },
        now,
    );
    assert_eq!(
        reliable_buffered(&mut pair),
        payload.len(),
        "real accepted reliable fragmented message"
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
        "whole original reliable message, exactly once"
    );
}
