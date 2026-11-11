use super::*;
use rtc_core::{
    media_api::{MediaCommand, PublishedTrackSpec},
    model::{BackupCodecPolicy, TrackCodec, TrackKind, TrackSource},
};
use str0m::{Candidate, Input, Output};
use str0m::{
    format::Codec,
    media::{Frequency, MediaTime},
    net::{Protocol, Receive},
};
struct Sender {
    engine: MediaEngine,
    peer: Rtc,
    tid: TransportId,
    mids: [Mid; 3],
    sid: TrackSid,
    packets: RoutedPacketQueue,
    server: std::net::SocketAddr,
    address: std::net::SocketAddr,
    start: Instant,
}
fn sender() -> Sender {
    let start = Instant::now();
    let mut engine = engine(start);
    engine.cfg.rtc.congestion_control.enabled = false;
    engine.cfg.video.codec_regression_threshold = 0;
    for mime in ["video/vp9", "video/vp8", "video/h264", "video/rtx"] {
        engine.cfg.enabled_codecs.push(CodecSpec {
            mime: mime.into(),
            fmtp_line: String::new(),
        });
    }
    let packets = RoutedPacketQueue::default();
    engine.out = Box::new(packets.clone());
    let server = "127.0.0.1:9701".parse().unwrap();
    let address = "127.0.0.1:9702".parse().unwrap();
    let tid = transport(&mut engine, PcRole::Publisher, start);
    {
        let t = engine.transports.get_mut(&tid).unwrap();
        let candidate = t
            .rtc
            .add_local_candidate(Candidate::host(server, "udp").unwrap())
            .unwrap()
            .clone();
        t.candidates.push(candidate);
    }
    let sid = TrackSid::from("TR_source_windows");
    let declarations = [
        ("video/vp9", "primary"),
        ("video/vp8", "target"),
        ("video/h264", "third"),
    ];
    engine.handle_command(
        MediaCommand::ExpectTrack {
            transport: tid,
            track: PublishedTrackSpec {
                backup_codec_policy: BackupCodecPolicy::PreferRegression,
                sid: sid.clone(),
                kind: TrackKind::Video,
                source: TrackSource::Camera,
                cid: "primary".into(),
                layers: vec![],
                codecs: declarations
                    .iter()
                    .map(|(mime, cid)| TrackCodec {
                        mime_type: (*mime).into(),
                        cid: (*cid).into(),
                        ..Default::default()
                    })
                    .collect(),
                stereo: false,
                disable_dtx: false,
                disable_red: false,
                encrypted: false,
                muted: false,
            },
        },
        start,
    );
    let mut peer = client(start);
    peer.add_local_candidate(Candidate::host(address, "udp").unwrap());
    let mut change = peer.sdp_api();
    let mids = declarations.map(|(_, cid)| {
        change.add_media(
            MediaKind::Video,
            Direction::SendOnly,
            None,
            Some(cid.into()),
            None,
        )
    });
    let (offered, pending) = change.apply().unwrap();
    let offered = crate::sdp_munge::map_sections(&offered.to_sdp_string(), |mid, section| {
        let index = mids
            .iter()
            .position(|candidate| Some(candidate.to_string().as_str()) == mid)
            .unwrap_or(0);
        Some(crate::sdp_munge::prioritize_publisher_codec(
            section,
            declarations[index].0,
        ))
    });
    engine.on_remote_description(tid, SdpType::Offer, offered, 1, start);
    let mut events = vec![];
    engine.drain_events(&mut events);
    let answer = events
        .iter()
        .find_map(|event| match event {
            MediaEvent::LocalDescription {
                kind: SdpType::Answer,
                sdp,
                ..
            } => Some(sdp),
            _ => None,
        })
        .unwrap();
    peer.sdp_api()
        .accept_answer(pending, SdpAnswer::from_sdp_string(answer).unwrap())
        .unwrap();
    for mid in mids {
        engine.bind_track(tid, &mid.to_string(), sid.clone(), start);
    }
    assert_eq!(
        mids.iter()
            .map(|mid| engine.up_by_mid[&(tid, *mid)])
            .collect::<std::collections::HashSet<_>>()
            .len(),
        3
    );
    Sender {
        engine,
        peer,
        tid,
        mids,
        sid,
        packets,
        server,
        address,
        start,
    }
}
fn frame(codec: Codec) -> Vec<u8> {
    let mut bytes = vec![0; 64];
    match codec {
        Codec::Vp9 => {
            bytes[0] = 0x82;
            bytes[63] = 9;
        }
        Codec::Vp8 => {
            bytes[..10].copy_from_slice(&[0x10, 0, 0, 0x9d, 1, 0x2a, 0x80, 2, 0x68, 1]);
            bytes[63] = 8;
        }
        Codec::H264 => {
            bytes[..5].copy_from_slice(&[0, 0, 0, 1, 0x65]);
            bytes[63] = 3;
        }
        _ => unreachable!(),
    };
    bytes
}
fn server_outputs(s: &mut Sender, now: Instant) {
    for (destination, bytes) in s.packets.0.lock().unwrap().drain(..) {
        assert_eq!(destination, s.address);
        s.peer
            .handle_input(Input::Receive(
                now,
                Receive::new(Protocol::Udp, s.server, s.address, &bytes).unwrap(),
            ))
            .unwrap();
    }
}
#[test]
fn actual_three_codec_windows_discard_old_backup_delta_and_flush_current_window_on_close() {
    let mut s = sender();
    let mut pts = std::collections::HashMap::<u8, usize>::new();
    let mut wire = Vec::<(usize, Instant, u64)>::new();
    let mut duplicate = false;
    for tick in 0..13000u64 {
        let now = s.start + Duration::from_millis(tick);
        s.peer.handle_input(Input::Timeout(now)).unwrap();
        if s.peer.is_connected() && tick.is_multiple_of(30) {
            for (index, codec) in [Codec::Vp9, Codec::Vp8, Codec::H264]
                .into_iter()
                .enumerate()
            {
                if (index == 1 && tick < 7000) || (index == 2 && tick < 500) {
                    continue;
                }
                let writer = s.peer.writer(s.mids[index]).unwrap();
                let pt = writer
                    .payload_params()
                    .find(|param| param.spec().codec == codec)
                    .unwrap()
                    .pt();
                pts.insert(*pt, index);
                writer
                    .write(
                        pt,
                        now,
                        MediaTime::new(tick * 90, Frequency::NINETY_KHZ),
                        frame(codec),
                    )
                    .unwrap();
            }
        }
        loop {
            match s.peer.poll_output().unwrap() {
                Output::Transmit(packet) => {
                    let measured = if packet.contents.len() >= 12 && packet.contents[0] >> 6 == 2 {
                        pts.get(&(packet.contents[1] & 127)).copied().map(|index| {
                            (
                                index,
                                (packet.contents.len()
                                    - s.engine.transports[&s.tid]
                                        .rtc
                                        .receive_srtp_auth_tag_len()
                                        .unwrap()) as u64,
                            )
                        })
                    } else {
                        None
                    };
                    if let Some((index, bytes)) = measured {
                        wire.push((index, now, bytes));
                    }
                    s.engine.handle_datagram(crate::net::Datagram {
                        transport: s.tid,
                        proto: Proto::Udp,
                        source: s.address,
                        destination: s.server,
                        data: &packet.contents,
                        received_at: now,
                    });
                    if let Some((0, bytes)) = measured {
                        if tick > 3000 && !duplicate {
                            duplicate = true;
                            wire.push((0, now, bytes));
                            s.engine.handle_datagram(crate::net::Datagram {
                                transport: s.tid,
                                proto: Proto::Udp,
                                source: s.address,
                                destination: s.server,
                                data: &packet.contents,
                                received_at: now,
                            });
                        }
                    }
                }
                Output::Timeout(_) => break,
                _ => {}
            }
        }
        s.engine.handle_timeout(now);
        s.engine.drain_events(&mut vec![]);
        server_outputs(&mut s, now);
        if tick == 5900 || tick == 11000 {
            let first_primary = wire.iter().find(|(codec, _, _)| *codec == 0).unwrap().1;
            let first_third = wire.iter().find(|(codec, _, _)| *codec == 2).unwrap().1;
            let primary_end =
                first_primary + Duration::from_secs(if tick == 5900 { 5 } else { 10 });
            let eligible = wire
                .iter()
                .filter(|(codec, at, _)| {
                    (*codec == 0 && *at <= primary_end)
                        || (tick == 11000
                            && *codec == 2
                            && *at > first_third + Duration::from_secs(5)
                            && *at <= first_third + Duration::from_secs(10))
                })
                .collect::<Vec<_>>();
            let stats = s.engine.stats();
            assert_eq!(
                stats.bytes_in,
                eligible.iter().map(|event| event.2).sum::<u64>(),
                "actual wire ledger at window barrier {tick}"
            );
            assert_eq!(stats.packets_in, eligible.len() as u64);
            if tick == 11000 {
                assert!(
                    eligible
                        .iter()
                        .any(|event| event.0 == 2 && event.1 < s.start + Duration::from_secs(7)),
                    "current backup window includes authenticated packets before target opened the gate"
                );
            }
        }
    }
    assert!(duplicate);
    let first_third = wire.iter().find(|(codec, _, _)| *codec == 2).unwrap().1;
    let eligible = wire
        .iter()
        .filter(|(codec, at, _)| *codec != 2 || *at > first_third + Duration::from_secs(5))
        .collect::<Vec<_>>();
    assert!(
        wire.iter()
            .any(|(codec, at, _)| *codec == 2 && *at <= first_third + Duration::from_secs(5)),
        "a real discarded backup window must exist"
    );
    let end = s.start + Duration::from_secs(14);
    s.engine.handle_command(
        MediaCommand::UnpublishTrack {
            track_sid: s.sid.clone(),
        },
        end,
    );
    let final_stats = s.engine.stats();
    assert_eq!(
        final_stats.bytes_in,
        eligible.iter().map(|event| event.2).sum::<u64>()
    );
    assert_eq!(final_stats.packets_in, eligible.len() as u64);
    assert_eq!(
        final_stats.bytes_out, 0,
        "upstream codec gate cannot manufacture downstream traffic"
    );
    let original_ssrc = s
        .peer
        .direct_api()
        .stream_tx_by_mid(s.mids[0], None)
        .unwrap()
        .ssrc();
    // Reuse the same authenticated PC, m-line and SSRC for another logical
    // publication. Native telemetry must rotate independently of RTP/SCTP.
    for (offset, direction) in [(10, Direction::Inactive), (20, Direction::SendOnly)] {
        let now = end + Duration::from_millis(offset);
        let mut change = s.peer.sdp_api();
        change.set_direction(s.mids[0], direction);
        let (offer, pending) = change.apply().unwrap();
        s.engine
            .on_remote_description(s.tid, SdpType::Offer, offer.to_sdp_string(), 2, now);
        let mut events = vec![];
        s.engine.drain_events(&mut events);
        let answer = events
            .iter()
            .find_map(|event| match event {
                MediaEvent::LocalDescription {
                    kind: SdpType::Answer,
                    sdp,
                    ..
                } => Some(sdp),
                _ => None,
            })
            .unwrap();
        s.peer
            .sdp_api()
            .accept_answer(pending, SdpAnswer::from_sdp_string(answer).unwrap())
            .unwrap();
    }
    let next = TrackSid::from("TR_reused_window");
    s.engine.handle_command(
        MediaCommand::ExpectTrack {
            transport: s.tid,
            track: PublishedTrackSpec {
                backup_codec_policy: BackupCodecPolicy::PreferRegression,
                sid: next.clone(),
                kind: TrackKind::Video,
                source: TrackSource::Camera,
                cid: "primary".into(),
                layers: vec![],
                codecs: vec![TrackCodec {
                    mime_type: "video/vp9".into(),
                    cid: "primary".into(),
                    ..Default::default()
                }],
                stereo: false,
                disable_dtx: false,
                disable_red: false,
                encrypted: false,
                muted: false,
            },
        },
        end + Duration::from_millis(20),
    );
    s.engine.bind_track(
        s.tid,
        &s.mids[0].to_string(),
        next,
        end + Duration::from_millis(20),
    );
    assert_eq!(
        s.peer
            .direct_api()
            .stream_tx_by_mid(s.mids[0], None)
            .unwrap()
            .ssrc(),
        original_ssrc
    );
    let mut extra = (0u64, 0u64);
    for offset in 30..200u64 {
        let now = end + Duration::from_millis(offset);
        s.peer.handle_input(Input::Timeout(now)).unwrap();
        if offset.is_multiple_of(30) {
            let writer = s.peer.writer(s.mids[0]).unwrap();
            let pt = writer
                .payload_params()
                .find(|param| param.spec().codec == Codec::Vp9)
                .unwrap()
                .pt();
            pts.insert(*pt, 0);
            writer
                .write(
                    pt,
                    now,
                    MediaTime::new((14000 + offset) * 90, Frequency::NINETY_KHZ),
                    frame(Codec::Vp9),
                )
                .unwrap();
        }
        loop {
            match s.peer.poll_output().unwrap() {
                Output::Transmit(packet) => {
                    if packet.contents.len() >= 12
                        && packet.contents[0] >> 6 == 2
                        && pts.get(&(packet.contents[1] & 127)) == Some(&0)
                    {
                        extra.0 += (packet.contents.len()
                            - s.engine.transports[&s.tid]
                                .rtc
                                .receive_srtp_auth_tag_len()
                                .unwrap()) as u64;
                        extra.1 += 1;
                    }
                    s.engine.handle_datagram(crate::net::Datagram {
                        transport: s.tid,
                        proto: Proto::Udp,
                        source: s.address,
                        destination: s.server,
                        data: &packet.contents,
                        received_at: now,
                    });
                }
                Output::Timeout(_) => break,
                _ => {}
            }
        }
        s.engine.handle_timeout(now);
        s.engine.drain_events(&mut vec![]);
        server_outputs(&mut s, now);
    }
    assert!(extra.1 > 0, "new same-PC receiver must carry real SRTP");
    assert_eq!(
        s.engine.stats().bytes_in,
        final_stats.bytes_in,
        "new receiver has not consumed its first 5s window"
    );
    s.engine.handle_command(
        MediaCommand::CloseTransport(s.tid),
        end + Duration::from_millis(200),
    );
    assert_eq!(
        s.engine.stats().bytes_in,
        final_stats.bytes_in + extra.0,
        "new epoch terminal delta cannot resurrect old discarded history"
    );
    assert_eq!(
        s.engine.stats().packets_in,
        final_stats.packets_in + extra.1
    );
    assert!(
        s.engine.receiver_traffic_is_empty(),
        "all receive-source/window resources must retire"
    );
}

#[test]
fn actual_generated_nack_is_counted_before_any_rtcp_emission_and_survives_disconnect() {
    let mut s = sender();
    let mut dropped = false;
    let mut primary_pt = None;
    let mut sent_primary = 0u64;
    for tick in 0..3000u64 {
        let now = s.start + Duration::from_millis(tick);
        s.peer.handle_input(Input::Timeout(now)).unwrap();
        if s.peer.is_connected() && tick.is_multiple_of(30) {
            let writer = s.peer.writer(s.mids[0]).unwrap();
            let pt = writer
                .payload_params()
                .find(|p| p.spec().codec == Codec::Vp9)
                .unwrap()
                .pt();
            primary_pt = Some(*pt);
            writer
                .write(
                    pt,
                    now,
                    MediaTime::new(tick * 90, Frequency::NINETY_KHZ),
                    frame(Codec::Vp9),
                )
                .unwrap();
        }
        loop {
            match s.peer.poll_output().unwrap() {
                Output::Transmit(packet) => {
                    let primary = packet.contents.len() >= 12
                        && packet.contents[0] >> 6 == 2
                        && Some(packet.contents[1] & 127) == primary_pt;
                    if primary {
                        sent_primary += 1;
                    }
                    if primary && tick > 1000 && !dropped {
                        dropped = true;
                        continue;
                    }
                    if primary && dropped {
                        let before = s.packets.0.lock().unwrap().len();
                        // Real authenticated next sequence creates a gap in the
                        // production receiver, without polling its queued output.
                        let rtc = &mut s.engine.transports.get_mut(&s.tid).unwrap().rtc;
                        rtc.handle_input(Input::Receive(
                            now,
                            Receive::new(Protocol::Udp, s.address, s.server, &packet.contents)
                                .unwrap(),
                        ))
                        .unwrap();
                        rtc.handle_input(Input::Timeout(now + Duration::from_millis(100)))
                            .unwrap();
                        let generated = rtc.media_traffic_stats().nacks_sent;
                        assert!(
                            generated > 0,
                            "real missing authenticated sequence must generate NACK requests"
                        );
                        assert_eq!(
                            s.packets.0.lock().unwrap().len(),
                            before,
                            "no native output was polled or emitted"
                        );
                        assert_eq!(s.peer.media_traffic_stats().nacks_received, 0);
                        assert_eq!(
                            rtc.media_traffic_stats().packets_in,
                            sent_primary - 1,
                            "one encrypted primary was lost, not delivered"
                        );
                        rtc.disconnect();
                        s.engine.handle_command(
                            MediaCommand::CloseTransport(s.tid),
                            now + Duration::from_millis(100),
                        );
                        assert_eq!(
                            s.engine.stats().nacks,
                            generated,
                            "terminal source sample retains generated but untransmitted NACK requests"
                        );
                        assert_eq!(
                            s.engine.stats().packets_in,
                            sent_primary,
                            "fixed Go source delta exports expected SN span, including the missing packet"
                        );
                        assert!(s.engine.receiver_traffic_is_empty());
                        return;
                    }
                    s.engine.handle_datagram(crate::net::Datagram {
                        transport: s.tid,
                        proto: Proto::Udp,
                        source: s.address,
                        destination: s.server,
                        data: &packet.contents,
                        received_at: now,
                    });
                }
                Output::Timeout(_) => break,
                _ => {}
            }
        }
        s.engine.handle_timeout(now);
        server_outputs(&mut s, now);
    }
    panic!("real primary gap did not converge");
}

#[test]
fn actual_duplicate_only_srtp_window_is_consumed_without_resurrecting_bytes_on_close() {
    let mut s = sender();
    let mut primary_pt = None;
    let mut sent = (0u64, 0u64);
    let mut replay = None;
    let mut first_window = None;
    for tick in 0..11000u64 {
        let now = s.start + Duration::from_millis(tick);
        s.peer.handle_input(Input::Timeout(now)).unwrap();
        if tick < 2000 && s.peer.is_connected() && tick.is_multiple_of(30) {
            let writer = s.peer.writer(s.mids[0]).unwrap();
            let pt = writer
                .payload_params()
                .find(|p| p.spec().codec == Codec::Vp9)
                .unwrap()
                .pt();
            primary_pt = Some(*pt);
            writer
                .write(
                    pt,
                    now,
                    MediaTime::new(tick * 90, Frequency::NINETY_KHZ),
                    frame(Codec::Vp9),
                )
                .unwrap();
        }
        loop {
            match s.peer.poll_output().unwrap() {
                Output::Transmit(packet) => {
                    if packet.contents.len() >= 12
                        && packet.contents[0] >> 6 == 2
                        && Some(packet.contents[1] & 127) == primary_pt
                    {
                        let auth = s.engine.transports[&s.tid]
                            .rtc
                            .receive_srtp_auth_tag_len()
                            .unwrap();
                        sent.0 += (packet.contents.len() - auth) as u64;
                        sent.1 += 1;
                        replay = Some(packet.contents.to_vec());
                    }
                    s.engine.handle_datagram(crate::net::Datagram {
                        transport: s.tid,
                        proto: Proto::Udp,
                        source: s.address,
                        destination: s.server,
                        data: &packet.contents,
                        received_at: now,
                    });
                }
                Output::Timeout(_) => break,
                _ => {}
            }
        }
        if tick == 5900 {
            let stats = s.engine.stats();
            assert!(sent.1 > 1);
            assert_eq!((stats.bytes_in, stats.packets_in), sent);
            first_window = Some((stats.bytes_in, stats.packets_in));
        }
        if tick == 6500 {
            let replay = replay.as_ref().unwrap();
            for _ in 0..2 {
                s.engine.handle_datagram(crate::net::Datagram {
                    transport: s.tid,
                    proto: Proto::Udp,
                    source: s.address,
                    destination: s.server,
                    data: replay,
                    received_at: now,
                });
            }
            let raw = s.engine.transports[&s.tid].rtc.media_traffic_stats();
            let auth = s.engine.transports[&s.tid]
                .rtc
                .receive_srtp_auth_tag_len()
                .unwrap();
            assert_eq!(
                raw.packets_in,
                sent.1 + 2,
                "both replays authenticated at the native boundary"
            );
            assert_eq!(raw.bytes_in, sent.0 + 2 * (replay.len() - auth) as u64);
        }
        s.engine.handle_timeout(now);
        s.engine.drain_events(&mut vec![]);
        server_outputs(&mut s, now);
    }
    let expected = first_window.unwrap();
    assert_eq!(
        (s.engine.stats().bytes_in, s.engine.stats().packets_in),
        expected,
        "zero sequence-progress window discards duplicate bytes and packets"
    );
    s.engine.handle_command(
        MediaCommand::CloseTransport(s.tid),
        s.start + Duration::from_secs(11),
    );
    assert_eq!(
        (s.engine.stats().bytes_in, s.engine.stats().packets_in),
        expected,
        "consumed duplicate-only delta must not be revived by terminal flush"
    );
    assert!(s.engine.receiver_traffic_is_empty());
}
