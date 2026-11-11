//! Actual SRTP spatial packets and successful subscriber admissions.
use super::*;
use rtc_core::media_api::{DownTrackSpec, MediaCommand, PublishedTrackSpec};
use rtc_core::model::{TrackKind, TrackSettings, TrackSource, VideoLayer, VideoQuality};
use str0m::format::Codec;
use str0m::net::{Protocol, Receive};
use str0m::rtp::RtpWrite;
use str0m::{Candidate, Event, Input, Output};

// Several real transports can emit offers in the same debounce tick. Only
// remove this target's offer; another target still owns its pending exchange.
fn answer_owned_offer(engine: &mut MediaEngine, client: &mut Rtc, tid: TransportId, now: Instant) {
    let mut events = Vec::new();
    engine.drain_events(&mut events);
    let position=events.iter().position(|event|matches!(event,MediaEvent::LocalDescription{transport,kind:SdpType::Offer,..}if *transport==tid))
        .expect("subscriber transport emits a serialized server offer");
    let MediaEvent::LocalDescription { sdp, id, .. } = events.remove(position) else {
        unreachable!()
    };
    engine.events.extend(events);
    let offer = str0m::change::SdpOffer::from_sdp_string(&sdp).unwrap();
    let answer = client.sdp_api().accept_offer(offer).unwrap();
    engine.on_remote_description(tid, SdpType::Answer, answer.to_sdp_string(), id, now);
    engine.run(now);
}

#[test]
fn encrypted_original_dd_fragments_arrive_before_assembly_preserve_frame_ids_and_retire() {
    for enabled in [false, true] {
        original_packets_case(enabled, false);
        original_packets_case(enabled, true);
    }
}
// Native frame payload receipts, not a pixel-decoder test. Wire serializer is
// test-owned; the receiving engine uses the unchanged production DD parser.
#[derive(Debug)]
struct SourceDd;
impl str0m::rtp::ExtensionSerializer for SourceDd {
    fn write_to(&self, out: &mut [u8], values: &str0m::rtp::ExtensionValues) -> usize {
        let Some(raw) = values.user_values.get::<crate::av1::RawDescriptor>() else {
            return 0;
        };
        if out.len() < raw.0.len() {
            return 0;
        }
        out[..raw.0.len()].copy_from_slice(&raw.0);
        raw.0.len()
    }
    fn parse_value(&self, input: &[u8], values: &mut str0m::rtp::ExtensionValues) -> bool {
        str0m::rtp::ExtensionSerializer::parse_value(&crate::av1::Serializer, input, values)
    }
    fn is_video(&self) -> bool {
        true
    }
    fn is_audio(&self) -> bool {
        false
    }
    fn requires_two_byte_form(&self, _: &str0m::rtp::ExtensionValues) -> bool {
        true
    }
}
fn raw_chain_descriptor(
    frame: u16,
    template: u8,
    attached: bool,
    first: bool,
    last: bool,
) -> Vec<u8> {
    let mut bits = Vec::new();
    let mut write = |value: u32, count: usize| {
        for shift in (0..count).rev() {
            bits.push(((value >> shift) & 1) as u8);
        }
    };
    write(u32::from(first) * 2 + u32::from(last), 2);
    write(u32::from(template), 6);
    write(u32::from(frame), 16);
    if attached {
        write(1, 1);
        write(0, 4); // attached default flags
        write(0, 6);
        write(0, 5); // id0, one target
        write(0, 2);
        write(0, 2);
        write(0, 2);
        write(3, 2); // four S0T0 templates
        for dti in [2, 3, 0, 3] {
            write(dti, 2);
        }
        for _ in 0..3 {
            write(0, 1);
        } // first three templates have no explicit frame dependency
        write(1, 1);
        write(0, 4);
        write(0, 1); // fourth depends on previous original frame
        write(1, 1); // ns(2) chain count1
        // ns(1) chain0 target map consumes zero bits
        for chain in [0, 1, 1, 1] {
            write(chain, 4);
        }
        write(0, 1); // no resolutions
    }
    let mut bytes = vec![0; (bits.len() + 7) / 8];
    for (i, bit) in bits.into_iter().enumerate() {
        bytes[i / 8] |= bit << (7 - i % 8);
    }
    bytes
}
fn original_packets_case(timing_enabled: bool, dropped_slot: bool) {
    let mixed = false;
    let independent_high_only = false;
    let start = Instant::now();
    let mut engine = engine(start);
    engine.cfg.playout_delay = Some((100, 200));
    engine.cfg.rtc.congestion_control.enabled = false;
    engine.cfg.enabled_codecs = vec![CodecSpec {
        mime: "video/AV1".into(),
        fmtp_line: String::new(),
    }];
    let stats =
        rtc_core::forward_stats::ForwardStats::new(100_000_000, 100_000_000, 1_000_000_000, start)
            .unwrap();
    engine.cfg.rtc.forward_stats = timing_enabled.then(|| stats.clone());
    let packets = RoutedPacketQueue::default();
    engine.out = Box::new(packets.clone());
    let server_addr = "127.0.0.1:9691".parse().unwrap();
    let pub_addr = "127.0.0.1:9692".parse().unwrap();
    let pub_tid = transport(&mut engine, PcRole::Publisher, start);
    let t = engine.transports.get_mut(&pub_tid).unwrap();
    let candidate = t
        .rtc
        .add_local_candidate(Candidate::host(server_addr, "udp").unwrap())
        .unwrap()
        .clone();
    t.candidates.push(candidate);
    t.spec.primary = false;
    let sid = TrackSid::from("TR_av1_packet_union");
    engine.handle_command(
        MediaCommand::ExpectTrack {
            transport: pub_tid,
            track: PublishedTrackSpec {
                sid: sid.clone(),
                kind: TrackKind::Video,
                source: TrackSource::Camera,
                cid: "svc-source".into(),
                layers: [VideoQuality::Low, VideoQuality::Medium, VideoQuality::High]
                    .into_iter()
                    .enumerate()
                    .map(|(i, quality)| VideoLayer {
                        quality,
                        width: 320 * (i as u32 + 1),
                        height: 180 * (i as u32 + 1),
                        bitrate: 10000,
                        ssrc: 0,
                        spatial_layer: i as i32,
                        rid: String::new(),
                        repair_ssrc: 0,
                    })
                    .collect(),
                codecs: vec![],
                stereo: false,
                disable_dtx: false,
                disable_red: false,
                encrypted: false,
                muted: false,
                backup_codec_policy: Default::default(),
            },
        },
        start,
    );
    let peer = |now| {
        Rtc::builder()
            .set_rtp_mode(true)
            .set_crypto_provider(rtc_crypto::provider().into())
            .set_dtls_cert(rtc_crypto::dtls_cert().unwrap())
            .clear_codecs()
            .enable_av1(true)
            .set_extension(6, str0m::rtp::Extension::PlayoutDelay)
            .set_extension(
                13,
                str0m::rtp::Extension::with_serializer(crate::av1::URI, SourceDd),
            )
            .build(now)
    };
    let mut publisher = peer(start);
    publisher.add_local_candidate(Candidate::host(pub_addr, "udp").unwrap());
    let mid = published_mid(&offer(&mut engine, &mut publisher, pub_tid, start, |api| {
        api.add_media(
            MediaKind::Video,
            Direction::SendOnly,
            None,
            Some("svc-source".into()),
            None,
        );
    }));
    engine.bind_track(pub_tid, &mid.to_string(), sid.clone(), start);
    let mut subscribers = Vec::new();
    for index in 0..if mixed { 2 } else { 1 } {
        let tid = TransportId::next();
        let participant = ParticipantSid::from(format!("PA_sink_{index}"));
        let addr = std::net::SocketAddr::from(([127, 0, 0, 1], 9693 + index as u16));
        engine.create_transport(
            TransportSpec {
                one_shot: false,
                id: tid,
                participant: participant.clone(),
                role: PcRole::Subscriber,
                ice_lite: true,
                server_data_channels: true,
                enable_data_tracks: true,
                primary: true,
                client_supports_red: false,
                codec_switch_requires_renegotiation: false,
                disable_playout_delay: index == 1,
                allow_pause: false,
                ice_preference: Default::default(),
            },
            IceCredentials::random(),
            vec![crate::net::LocalCandidate {
                proto: Proto::Udp,
                addr: server_addr,
                base: server_addr,
            }],
            start,
        );
        let mut subscriber = peer(start);
        subscriber.add_local_candidate(Candidate::host(addr, "udp").unwrap());
        answer_owned_offer(&mut engine, &mut subscriber, tid, start);
        engine.handle_command(
            MediaCommand::Subscribe(DownTrackSpec {
                subscriber: participant,
                publisher: "PA_test".into(),
                track_sid: sid.clone(),
                stream_id: format!("source|{sid}"),
                settings: TrackSettings {
                    quality: if independent_high_only {
                        VideoQuality::High
                    } else if index == 0 {
                        VideoQuality::Low
                    } else {
                        VideoQuality::High
                    },
                    ..Default::default()
                },
                adaptive_stream: false,
            }),
            start,
        );
        subscribers.push((tid, addr, subscriber, Vec::<Vec<u8>>::new()));
    }
    let negotiated = start + Duration::from_millis(160);
    engine.handle_timeout(negotiated);
    for (tid, _, subscriber, _) in &mut subscribers {
        answer_owned_offer(&mut engine, subscriber, *tid, negotiated);
    }
    let pt = publisher
        .codec_config()
        .params()
        .iter()
        .find(|p| p.spec().codec == Codec::Av1)
        .unwrap()
        .pt();
    let mut sent = 0usize;
    let mut last_send = 0u64;
    let mut checkpoints = Vec::new();
    let source_frames = [10u16, 11, 12, 13, 14, 16, 15, 17, 18];
    let selected = [true, false, false, true, true, true, true, true, false];
    let mut wire_receipts = Vec::new();
    let mut wire_padding = Vec::new();
    for tick in 0..4800u64 {
        let now = negotiated + Duration::from_millis(tick);
        publisher.handle_input(Input::Timeout(now)).unwrap();
        for (tid, _, peer, _) in &mut subscribers {
            if engine.transports.contains_key(tid) {
                peer.handle_input(Input::Timeout(now)).unwrap();
            }
        }
        let ready = publisher.is_connected()
            && subscribers.iter().all(|(tid, _, peer, _)| {
                !engine.transports.contains_key(tid) || peer.is_connected()
            });
        if ready && sent < 18 && (sent == 0 || tick >= last_send + 200) {
            if sent > 0 {
                stats.tick(now);
                let expected = selected.iter().take(sent / 2).filter(|v| **v).count() * 2
                    + usize::from(sent % 2 == 1 && selected[sent / 2]);
                assert_eq!(
                    subscribers[0].3.len(),
                    expected,
                    "physical admission before source stage {sent}"
                );
                let late_seen = sent.saturating_sub(12).min(2);
                assert_eq!(
                    stats.snapshot().histogram_count,
                    if timing_enabled {
                        (expected - late_seen) as u64
                    } else {
                        0
                    },
                    "per-original successful chain admission before source stage {sent}"
                );
                checkpoints.push((
                    sent,
                    subscribers[0].3.len(),
                    stats.snapshot().histogram_count,
                ));
            }
            if sent == 16 {
                engine.handle_command(
                    MediaCommand::UnpublishTrack {
                        track_sid: sid.clone(),
                    },
                    now,
                );
            }
            let template = match sent / 2 {
                1 => 2,
                2 | 4 => 1,
                5 | 6 | 7 => 3,
                _ => 0,
            };
            let mut values = str0m::rtp::ExtensionValues::default();
            values.user_values.set(crate::av1::RawDescriptor(
                raw_chain_descriptor(
                    source_frames[sent / 2],
                    template,
                    sent == 0,
                    sent % 2 == 0,
                    sent % 2 == 1,
                )
                .into(),
            ));
            if !(dropped_slot && sent == 3) {
                publisher
                    .direct_api()
                    .stream_tx_by_mid(mid, None)
                    .unwrap()
                    .write_rtp(
                        RtpWrite::new(
                            pt,
                            (1000
                                + u64::from(source_frames[sent / 2] - 10) * 2
                                + (sent % 2) as u64)
                                .into(),
                            u32::from(source_frames[sent / 2]) * 3000,
                            now,
                            if sent % 2 == 0 {
                                vec![if sent == 0 || sent == 6 { 0x58 } else { 0x50 }, 0x34, 0]
                            } else {
                                vec![0x90, source_frames[sent / 2] as u8]
                            },
                        )
                        .marker(sent % 2 == 1)
                        .ext_vals(values),
                    );
            }
            sent += 1;
            last_send = tick;
        }
        // Drain each sans-I/O input immediately. In RTP mode a single
        // pending event slot is not a queue; frame mode must also be driven.
        loop {
            match publisher.poll_output().unwrap() {
                Output::Transmit(p) => {
                    engine.handle_datagram(crate::net::Datagram {
                        transport: pub_tid,
                        proto: Proto::Udp,
                        source: pub_addr,
                        destination: server_addr,
                        data: &p.contents,
                        received_at: now,
                    });
                }
                Output::Timeout(_) => break,
                _ => {}
            }
        }
        for (tid, addr, subscriber, _) in &mut subscribers {
            if !engine.transports.contains_key(tid) {
                continue;
            }
            loop {
                match subscriber.poll_output().unwrap() {
                    Output::Transmit(p) => {
                        if engine.transports.contains_key(tid) {
                            engine.handle_datagram(crate::net::Datagram {
                                transport: *tid,
                                proto: Proto::Udp,
                                source: *addr,
                                destination: server_addr,
                                data: &p.contents,
                                received_at: now,
                            });
                        }
                    }
                    Output::Timeout(_) => break,
                    _ => {}
                }
            }
        }
        engine.handle_timeout(now);
        // The mutex guard must end before callbacks can synchronously
        // enqueue DTLS/RTCP into this same PacketOut queue.
        let outbound = { packets.0.lock().unwrap().drain(..).collect::<Vec<_>>() };
        for (destination, bytes) in outbound {
            if destination == pub_addr {
                publisher
                    .handle_input(Input::Receive(
                        now,
                        Receive::new(Protocol::Udp, server_addr, pub_addr, &bytes).unwrap(),
                    ))
                    .unwrap();
                continue;
            }
            let (tid, addr, subscriber, frames) = subscribers
                .iter_mut()
                .find(|(_, addr, _, _)| *addr == destination)
                .unwrap();
            if !engine.transports.contains_key(tid) {
                continue;
            }
            subscriber
                .handle_input(Input::Receive(
                    now,
                    Receive::new(Protocol::Udp, server_addr, *addr, &bytes).unwrap(),
                ))
                .unwrap();
            loop {
                match subscriber.poll_output().unwrap() {
                    Output::Event(Event::RtpPacket(packet)) => {
                        if packet.header.has_padding {
                            assert!(
                                packet.payload.is_empty(),
                                "real source-dropped slot is RTP padding, never fabricated AV1 media"
                            );
                            wire_padding.push(*packet.seq_no);
                            continue;
                        }
                        let expected_delay = !engine.transports[tid].spec.disable_playout_delay;
                        assert_eq!(
                            packet.header.ext_vals.play_delay_min,
                            expected_delay.then(|| str0m::media::MediaTime::from_millis(100)),
                            "authenticated raw RTP follows per-PC playout min policy"
                        );
                        assert_eq!(
                            packet.header.ext_vals.play_delay_max,
                            expected_delay.then(|| str0m::media::MediaTime::from_millis(200)),
                            "authenticated raw RTP follows per-PC playout max policy"
                        );
                        let raw = packet
                            .header
                            .ext_vals
                            .user_values
                            .get::<crate::av1::RawDescriptor>()
                            .expect("every original media packet retains DD");
                        let frame = u16::from_be_bytes([raw.0[1], raw.0[2]]);
                        assert!(
                            [10, 13, 14, 15, 16, 17].contains(&frame),
                            "known broken chain must not cross actual wire"
                        );
                        assert_eq!(
                            raw.0[0] & 0xc0,
                            if packet.header.marker { 0x40 } else { 0x80 },
                            "actual first and final flags remain per fragment"
                        );
                        wire_receipts.push((frame, *packet.seq_no, packet.header.marker));
                        frames.push(packet.payload.to_vec());
                    }
                    Output::Transmit(p) => engine.handle_datagram(crate::net::Datagram {
                        transport: *tid,
                        proto: Proto::Udp,
                        source: *addr,
                        destination: server_addr,
                        data: &p.contents,
                        received_at: now,
                    }),
                    Output::Timeout(_) => break,
                    _ => {}
                }
            }
        }
    }
    assert_eq!(
        sent, 18,
        "real connected source must emit full chain/drop/recovery/retirement sequence"
    );
    stats.tick(negotiated + Duration::from_secs(6));
    assert_eq!(
        subscribers[0].3,
        [10u8, 13, 14, 16, 15, 17]
            .into_iter()
            .flat_map(|frame| [
                vec![
                    if frame == 10 || frame == 13 {
                        0x58
                    } else {
                        0x50
                    },
                    0x34,
                    0
                ],
                vec![0x90, frame]
            ])
            .collect::<Vec<_>>(),
        "actual authenticated subscriber may only receive intact first and reset recovery frames"
    );
    assert_eq!(
        stats.snapshot().histogram_count,
        if timing_enabled { 10 } else { 0 },
        "known drops, two genuinely out-of-order repaired packets, padding and retired source cannot gain C21 samples"
    );
    assert_eq!(checkpoints.len(), 17);
    let base = wire_receipts[0].1;
    let expected_offsets = if dropped_slot {
        vec![0, 1, 4, 5, 6, 7, 10, 11, 8, 9, 12, 13]
    } else {
        vec![0, 1, 2, 3, 4, 5, 8, 9, 6, 7, 10, 11]
    };
    let frames = [10u16, 10, 13, 13, 14, 14, 16, 16, 15, 15, 17, 17];
    assert_eq!(
        wire_receipts
            .iter()
            .map(|(frame, seq, marker)| (*frame, *seq - base, *marker))
            .collect::<Vec<_>>(),
        frames
            .into_iter()
            .zip(expected_offsets)
            .enumerate()
            .map(|(index, (frame, offset))| (frame, offset, index % 2 == 1))
            .collect::<Vec<_>>(),
        "wire DD source IDs, genuine reserved RTP holes and late repaired fragments cannot be collapsed into one picture counter"
    );
    assert_eq!(
        wire_padding
            .iter()
            .map(|seq| *seq - base)
            .collect::<Vec<_>>(),
        if dropped_slot { vec![3] } else { vec![] },
        "known dropped packet after an unknown source gap writes one genuine authenticated padding slot"
    );
    assert!(engine.up_by_sid.is_empty());
    for (tid, _, _, _) in subscribers {
        engine.handle_command(
            MediaCommand::CloseTransport(tid),
            negotiated + Duration::from_secs(6),
        );
    }
    engine.handle_command(
        MediaCommand::CloseTransport(pub_tid),
        negotiated + Duration::from_secs(6),
    );
    assert!(engine.transports.is_empty());
}
