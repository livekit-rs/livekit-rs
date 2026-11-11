//! Wire-level check of fixed Go RTPMunger sequence semantics on the real
//! ICE/DTLS/SRTP path: upstream sequence numbers that never reach the SFU stay a
//! gap downstream, and a subscriber NACK for them is not answered (Go's
//! sequencer has nothing to retransmit).
use super::*;
use crate::net::Datagram;
use rtc_core::media_api::{DownTrackSpec, MediaCommand};
use rtc_core::model::TrackSettings;
use str0m::media::{Frequency, MediaTime};
use str0m::net::{Protocol, Receive};
use str0m::{Candidate, Event, Input, Output};

#[derive(Clone, Copy, PartialEq)]
enum Fault {
    /// Ten consecutive upstream sequence numbers never reach the SFU.
    UpstreamLoss,
    /// The subscriber disables the track for a while (Go `Forwarder.Mute`:
    /// `shouldDrop` without munging, then a resync through `UpdateSnTsOffsets`).
    SubscriberMute,
}

struct Observed {
    injected: bool,
    downstream: Vec<u16>,
    nacks: u64,
}

fn run(fault: Fault) -> Observed {
    let start = Instant::now();
    let mut engine = engine(start);
    let packets = PacketQueue::default();
    engine.out = Box::new(packets.clone());
    let tid = transport(&mut engine, PcRole::Single, start);
    let server_addr: std::net::SocketAddr = "127.0.0.1:9101".parse().unwrap();
    let client_addr: std::net::SocketAddr = "127.0.0.1:9102".parse().unwrap();
    let t = engine.transports.get_mut(&tid).unwrap();
    let candidate = t
        .rtc
        .add_local_candidate(Candidate::host(server_addr, "udp").unwrap())
        .unwrap()
        .clone();
    t.candidates.push(candidate);
    // The client offers Opus NACK, so both directions repair losses when they can.
    let mut config = Rtc::builder()
        .set_crypto_provider(rtc_crypto::provider().into())
        .set_dtls_cert(rtc_crypto::dtls_cert().unwrap())
        .enable_raw_packets(true);
    for param in config.codec_config().iter_mut() {
        if param.spec().codec == str0m::format::Codec::Opus {
            param.set_fb_nack(true);
        }
    }
    let mut client = config.build(start);
    client.add_local_candidate(Candidate::host(client_addr, "udp").unwrap());
    let tx_mid = published_mid(&offer(&mut engine, &mut client, tid, start, |change| {
        change.add_media(MediaKind::Audio, Direction::SendOnly, None, None, None);
    }));
    let track = TrackSid::from("TR_gap");
    engine.bind_track(tid, &tx_mid.to_string(), track.clone(), start);
    engine.subscribe(
        DownTrackSpec {
            subscriber: ParticipantSid::from("PA_test"),
            publisher: ParticipantSid::from("PA_test"),
            track_sid: track,
            stream_id: "PA_test|TR_gap".into(),
            settings: TrackSettings::default(),
            adaptive_stream: false,
        },
        start,
    );
    let mut rx_mid = None;
    offer(&mut engine, &mut client, tid, start, |change| {
        rx_mid = Some(change.add_media(MediaKind::Audio, Direction::RecvOnly, None, None, None));
    });
    let rx_mid = rx_mid.unwrap();
    let opus = vec![0xf8, 0xff, 0xfe];
    // Upstream sequence numbers that never reach the SFU, including retransmissions.
    let mut lost_upstream: Option<(u16, u16)> = None;
    let mut first_upstream: Option<u16> = None;
    let mut downstream: Vec<u16> = Vec::new();
    let mut downstream_nacks = 0u64;
    let (mut muted_once, mut muted_at) = (false, None::<u64>);
    for tick in 0..1500u64 {
        let now = start + Duration::from_millis(tick * 10);
        if let Some(rx) = client.direct_api().stream_rx_by_mid(rx_mid, None) {
            rx.suppress_nack(false);
        }
        client.handle_input(Input::Timeout(now)).unwrap();
        if client.is_connected() && tick % 2 == 0 {
            let writer = client.writer(tx_mid).unwrap();
            let pt = writer
                .payload_params()
                .find(|p| p.spec().codec == str0m::format::Codec::Opus)
                .unwrap()
                .pt();
            writer
                .write(
                    pt,
                    now,
                    MediaTime::new(tick * 480, Frequency::FORTY_EIGHT_KHZ),
                    opus.clone(),
                )
                .unwrap();
        }
        loop {
            match client.poll_output().unwrap() {
                Output::Transmit(p) => {
                    let rtp = p.contents.len() >= 12
                        && p.contents[0] >> 6 == 2
                        && p.contents[1] & 0x7f == 111;
                    if rtp {
                        let seq = u16::from_be_bytes([p.contents[2], p.contents[3]]);
                        let first = *first_upstream.get_or_insert(seq);
                        // After 100 packets, ten consecutive sequence numbers are lost for good.
                        if fault == Fault::UpstreamLoss
                            && lost_upstream.is_none()
                            && seq.wrapping_sub(first) == 100
                        {
                            lost_upstream = Some((seq, seq.wrapping_add(9)));
                        }
                        if let Some((a, b)) = lost_upstream
                            && seq.wrapping_sub(a) <= b.wrapping_sub(a)
                        {
                            continue;
                        }
                    }
                    engine.handle_datagram(Datagram {
                        transport: tid,
                        proto: Proto::Udp,
                        source: client_addr,
                        destination: server_addr,
                        data: &p.contents,
                        received_at: now,
                    });
                }
                Output::Event(Event::RawPacket(packet)) => match *packet {
                    str0m::rtp::RawPacket::RtpRx(header, _)
                        if header.payload_type == str0m::media::Pt::from(111) =>
                    {
                        downstream.push(header.sequence_number);
                    }
                    str0m::rtp::RawPacket::RtcpTx(str0m::rtp::rtcp::Rtcp::Nack(report)) => {
                        downstream_nacks += report
                            .reports
                            .iter()
                            .map(|e| 1 + e.blp.count_ones() as u64)
                            .sum::<u64>();
                    }
                    _ => {}
                },
                Output::Timeout(_) => break,
                _ => {}
            }
        }
        engine.handle_timeout(now);
        engine.drain_events(&mut Vec::new());
        for bytes in packets.0.lock().unwrap().drain(..).collect::<Vec<_>>() {
            client
                .handle_input(Input::Receive(
                    now,
                    Receive::new(Protocol::Udp, server_addr, client_addr, &bytes).unwrap(),
                ))
                .unwrap();
        }
        if fault == Fault::SubscriberMute {
            let mute = |disabled: bool| MediaCommand::UpdateTrackSettings {
                subscriber: ParticipantSid::from("PA_test"),
                track_sid: TrackSid::from("TR_gap"),
                settings: TrackSettings {
                    disabled,
                    ..TrackSettings::default()
                },
            };
            if downstream.len() >= 100 && !muted_once {
                muted_once = true;
                muted_at = Some(tick);
                engine.handle_command(mute(true), now);
            }
            if muted_at.is_some_and(|at| tick == at + 60) {
                engine.handle_command(mute(false), now);
            }
        }
        if downstream.len() > 250 {
            break;
        }
    }
    Observed {
        injected: match fault {
            Fault::UpstreamLoss => lost_upstream.is_some(),
            Fault::SubscriberMute => muted_once,
        },
        downstream,
        nacks: downstream_nacks,
    }
}

#[test]
fn upstream_opus_gap_stays_a_gap_downstream_and_is_never_retransmitted() {
    let Observed {
        injected,
        downstream,
        nacks: downstream_nacks,
    } = run(Fault::UpstreamLoss);
    assert!(injected, "the upstream burst was injected");
    assert!(
        downstream.len() > 250,
        "downstream audio kept flowing: {}",
        downstream.len()
    );
    // Exactly one forward jump of 11 (ten never-received upstream packets); every
    // other step is +1, and no number inside the gap ever arrives (no retransmission).
    let mut jumps = Vec::new();
    for w in downstream.windows(2) {
        let d = w[1].wrapping_sub(w[0]);
        if d != 1 {
            jumps.push((w[0], d));
        }
    }
    assert_eq!(jumps.len(), 1, "one gap downstream: {jumps:?}");
    let (before, step) = jumps[0];
    assert_eq!(
        step, 11,
        "the ten upstream losses stay a gap of ten downstream"
    );
    for missing in 1..=10u16 {
        assert!(
            !downstream.contains(&before.wrapping_add(missing)),
            "a gap number was retransmitted"
        );
    }
    assert!(
        downstream_nacks >= 10,
        "the subscriber NACKed the gap: {downstream_nacks}"
    );
}

/// Packets the SFU drops itself (subscriber mute) are not a gap: Go resumes the
/// munger with `UpdateSnTsOffsets`, so the subscriber sees contiguous numbers and
/// has nothing to NACK.
#[test]
fn subscriber_mute_drops_are_not_a_downstream_gap() {
    let Observed {
        injected,
        downstream,
        nacks,
    } = run(Fault::SubscriberMute);
    assert!(injected, "the subscriber muted the track");
    assert!(
        downstream.len() > 250,
        "downstream audio resumed: {}",
        downstream.len()
    );
    let jumps: Vec<_> = downstream
        .windows(2)
        .filter(|w| w[1].wrapping_sub(w[0]) != 1)
        .map(|w| (w[0], w[1]))
        .collect();
    assert!(jumps.is_empty(), "contiguous across the mute: {jumps:?}");
    assert_eq!(nacks, 0, "nothing to NACK");
}

/// A transport that is driven only because another transport's event wrote
/// media to it (forwarding) must send with the current time: str0m stamps
/// abs-send-time and paces with its last input time, and fixed Go stamps at the
/// actual write. Without advancing the clock the stamp lags by the time since
/// the subscriber's own last input, which Chrome's delay-based REMB reads as
/// queueing (evidence spec0077-m06-layers-bwe).
#[test]
fn driving_a_transport_advances_its_str0m_clock_to_the_drive_time() {
    let start = Instant::now();
    let mut engine = engine(start);
    let tid = transport(&mut engine, PcRole::Subscriber, start);
    let later = start + Duration::from_millis(40);
    engine.mark_dirty(tid);
    engine.run(later);
    assert_eq!(engine.transports[&tid].rtc_clock, later);
    // Never moves backwards.
    engine.mark_dirty(tid);
    engine.run(start + Duration::from_millis(10));
    assert_eq!(engine.transports[&tid].rtc_clock, later);
}
