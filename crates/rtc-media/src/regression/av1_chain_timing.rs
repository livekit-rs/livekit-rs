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
fn encrypted_dd_protected_chain_rejects_known_drop_recovers_and_releases() {
    for enabled in [false, true] {
        chain_case(enabled);
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
fn raw_chain_descriptor(frame: u16, template: u8, attached: bool) -> Vec<u8> {
    let mut bits = Vec::new();
    let mut write = |value: u32, count: usize| {
        for shift in (0..count).rev() {
            bits.push(((value >> shift) & 1) as u8);
        }
    };
    write(3, 2);
    write(u32::from(template), 6);
    write(u32::from(frame), 16);
    if attached {
        write(1, 1);
        write(0, 4); // attached default flags
        write(0, 6);
        write(0, 5); // id0, one target
        write(0, 2);
        write(0, 2);
        write(3, 2); // three S0T0 templates
        for dti in [2, 3, 0] {
            write(dti, 2);
        }
        for _ in 0..3 {
            write(0, 1);
        } // no frame dependencies
        write(1, 1); // ns(2) chain count1
        // ns(1) chain0 target map consumes zero bits
        for chain in [0, 1, 1] {
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
fn chain_case(timing_enabled: bool) {
    let mixed = false;
    let independent_high_only = false;
    let start = Instant::now();
    let mut engine = engine(start);
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
            .set_crypto_provider(rtc_crypto::provider().into())
            .set_dtls_cert(rtc_crypto::dtls_cert().unwrap())
            .clear_codecs()
            .enable_av1(true)
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
                ice_preference: Default::default(),
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
                disable_playout_delay: false,
                allow_pause: false,
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
        .writer(mid)
        .unwrap()
        .payload_params()
        .find(|p| p.spec().codec == Codec::Av1)
        .unwrap()
        .pt();
    let mut sent = 0usize;
    let mut last_send = 0u64;
    let mut checkpoints = Vec::new();
    for tick in 0..2800u64 {
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
        if ready && sent < 5 && (sent == 0 || tick >= last_send + 400) {
            if sent > 0 {
                stats.tick(now);
                let expected = if sent <= 3 { 1 } else { 2 };
                assert_eq!(
                    subscribers[0].3.len(),
                    expected,
                    "physical admission before source stage {sent}"
                );
                assert_eq!(
                    stats.snapshot().histogram_count,
                    if timing_enabled { expected as u64 } else { 0 },
                    "per-original successful chain admission before source stage {sent}"
                );
                checkpoints.push((
                    sent,
                    subscribers[0].3.len(),
                    stats.snapshot().histogram_count,
                ));
            }
            if sent == 4 {
                engine.handle_command(
                    MediaCommand::UnpublishTrack {
                        track_sid: sid.clone(),
                    },
                    now,
                );
            }
            let template = match sent {
                1 => 2,
                2 => 1,
                _ => 0,
            };
            let mut values = str0m::rtp::ExtensionValues::default();
            values.user_values.set(crate::av1::RawDescriptor(
                raw_chain_descriptor(10 + sent as u16, template, sent == 0).into(),
            ));
            publisher
                .direct_api()
                .stream_tx_by_mid(mid, None)
                .unwrap()
                .write_rtp(
                    RtpWrite::new(
                        pt,
                        (1000 + sent as u64).into(),
                        3000 + sent as u32 * 3000,
                        now,
                        vec![
                            if sent == 0 || sent == 3 { 0x18 } else { 0x10 },
                            0x34,
                            0,
                            sent as u8,
                        ],
                    )
                    .marker(true)
                    .ext_vals(values),
                );
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
                    Output::Event(Event::MediaData(data)) => frames.push(data.data.to_vec()),
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
        sent, 5,
        "real connected source must emit full chain/drop/recovery/retirement sequence"
    );
    stats.tick(negotiated + Duration::from_secs(4));
    assert_eq!(
        subscribers[0].3,
        vec![vec![0x36, 0, 1, 0], vec![0x36, 0, 1, 3]],
        "actual authenticated subscriber may only receive intact first and reset recovery frames"
    );
    assert_eq!(
        stats.snapshot().histogram_count,
        if timing_enabled { 2 } else { 0 },
        "drop and retired source must not gain samples"
    );
    assert_eq!(checkpoints.len(), 4);
    assert!(engine.up_by_sid.is_empty());
    for (tid, _, _, _) in subscribers {
        engine.handle_command(
            MediaCommand::CloseTransport(tid),
            negotiated + Duration::from_secs(4),
        );
    }
    engine.handle_command(
        MediaCommand::CloseTransport(pub_tid),
        negotiated + Duration::from_secs(4),
    );
    assert!(engine.transports.is_empty());
}
