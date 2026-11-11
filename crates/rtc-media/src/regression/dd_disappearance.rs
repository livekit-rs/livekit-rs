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

#[test]
fn encrypted_dd_disappearance_drops_trigger_recovers_payload_and_keeps_selector_latched() {
    for codec in [Codec::Vp9, Codec::Av1] {
        for timing in [false, true] {
            for startup in [false, true] {
                case(codec, timing, startup, false);
            }
        }
    }
}
#[test]
#[ignore = "requires actual original public Buffer.Write -> DownTrack physical lifecycle receipt"]
fn actual_original_dd_disappearance_physical_matches_payload_recovery() {
    for codec in [Codec::Vp9, Codec::Av1] {
        for timing in [false, true] {
            for startup in [false, true] {
                case(codec, timing, startup, true);
            }
        }
    }
}
fn payload(codec: Codec, frame: u8, key: bool) -> Vec<u8> {
    if codec == Codec::Av1 {
        return if key {
            vec![0x28, 1, 8, 0x18, 0]
        } else {
            vec![0x10, 0x30, 0x20, frame]
        };
    }
    let mut value = vec![if key { 0xbc } else { 0xfc }, 0x81, frame, 0];
    if !key {
        value.push(2);
    }
    value.extend_from_slice(&[if key { 0x80 } else { 0x34 }, frame]);
    value
}
fn case(codec: Codec, timing_enabled: bool, startup_missing: bool, require_original: bool) {
    let mixed = false;
    let independent_high_only = false;
    let start = Instant::now();
    let mut engine = engine(start);
    engine.cfg.playout_delay = Some((100, 200));
    engine.cfg.rtc.congestion_control.enabled = false;
    engine.cfg.enabled_codecs = vec![CodecSpec {
        mime: crate::codecs::mime_of(codec).into(),
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
    let sid = TrackSid::from("TR_dd_lifecycle");
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
            .enable_av1(codec == Codec::Av1)
            .enable_vp9(codec == Codec::Vp9)
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
        .find(|p| p.spec().codec == codec)
        .unwrap()
        .pt();

    let mut sent = 0usize;
    let mut malformed_sent = false;
    let mut last_send = 0u64;
    let mut receipts = Vec::new();
    for tick in 0..4800u64 {
        let now = negotiated + Duration::from_millis(tick);
        publisher.handle_input(Input::Timeout(now)).unwrap();
        for (_, _, peer, _) in &mut subscribers {
            peer.handle_input(Input::Timeout(now)).unwrap();
        }
        let ready = publisher.is_connected()
            && subscribers
                .iter()
                .all(|(_, _, peer, _)| peer.is_connected());
        if ready && !malformed_sent {
            let mut malformed = str0m::rtp::ExtensionValues::default();
            malformed
                .user_values
                .set(crate::av1::RawDescriptor(vec![0x80, 0].into()));
            publisher
                .direct_api()
                .stream_tx_by_mid(mid, None)
                .unwrap()
                .write_rtp(
                    RtpWrite::new(pt, 999.into(), 27000, now, payload(codec, 9, true))
                        .marker(true)
                        .ext_vals(malformed),
                );
            malformed_sent = true;
            last_send = tick;
        } else if ready && sent < 7 && tick >= last_send + 200 {
            let trigger = if startup_missing { 0 } else { 2 };
            if sent == 0 {
                assert!(
                    receipts.is_empty(),
                    "actual authenticated malformed-present DD cannot reach a subscriber or disable its valid-DD selector"
                );
            }
            if sent > 0 {
                let expected = (0..sent.min(6)).filter(|stage| *stage != trigger).count();
                assert_eq!(
                    receipts.len(),
                    expected,
                    "every accepted original packet must reach authenticated receiver before next source phase; triggering missingDD alone drops"
                );
            }
            if sent == 6 {
                engine.handle_command(
                    MediaCommand::UnpublishTrack {
                        track_sid: sid.clone(),
                    },
                    now,
                );
            }
            let has_dd = if startup_missing {
                sent == 4
            } else {
                matches!(sent, 0 | 1 | 4 | 6)
            };
            let mut values = str0m::rtp::ExtensionValues::default();
            if has_dd {
                values.user_values.set(crate::av1::RawDescriptor(
                    raw_chain_descriptor(
                        10 + sent as u16,
                        0,
                        sent == 0 || sent == 4 || sent == 6,
                        true,
                        true,
                    )
                    .into(),
                ));
            }
            let key = sent == 0 || (startup_missing && sent == 1);
            publisher
                .direct_api()
                .stream_tx_by_mid(mid, None)
                .unwrap()
                .write_rtp(
                    RtpWrite::new(
                        pt,
                        (1000 + sent as u64).into(),
                        3000 * (10 + sent as u32),
                        now,
                        payload(codec, 10 + sent as u8, key),
                    )
                    .marker(true)
                    .ext_vals(values),
                );
            sent += 1;
            last_send = tick;
        }
        loop {
            match publisher.poll_output().unwrap() {
                Output::Transmit(p) => engine.handle_datagram(crate::net::Datagram {
                    transport: pub_tid,
                    proto: Proto::Udp,
                    source: pub_addr,
                    destination: server_addr,
                    data: &p.contents,
                    received_at: now,
                }),
                Output::Timeout(_) => break,
                _ => {}
            }
        }
        for (tid, addr, subscriber, _) in &mut subscribers {
            loop {
                match subscriber.poll_output().unwrap() {
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
        engine.handle_timeout(now);
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
            let (tid, addr, subscriber, _) = subscribers
                .iter_mut()
                .find(|(_, addr, _, _)| *addr == destination)
                .unwrap();
            subscriber
                .handle_input(Input::Receive(
                    now,
                    Receive::new(Protocol::Udp, server_addr, *addr, &bytes).unwrap(),
                ))
                .unwrap();
            loop {
                match subscriber.poll_output().unwrap() {
                    Output::Event(Event::RtpPacket(p)) => {
                        assert!(
                            !p.header.has_padding,
                            "contiguous trigger is a real dropped slot, not invented padding"
                        );
                        let expected_delay = !engine.transports[tid].spec.disable_playout_delay;
                        assert_eq!(
                            p.header.ext_vals.play_delay_min,
                            expected_delay.then(|| str0m::media::MediaTime::from_millis(100)),
                            "authenticated raw RTP follows per-PC playout min policy"
                        );
                        assert_eq!(
                            p.header.ext_vals.play_delay_max,
                            expected_delay.then(|| str0m::media::MediaTime::from_millis(200)),
                            "authenticated raw RTP follows per-PC playout max policy"
                        );
                        receipts.push((
                            *p.seq_no,
                            p.header.timestamp,
                            p.header.marker,
                            p.payload.to_vec(),
                            p.header
                                .ext_vals
                                .user_values
                                .get::<crate::av1::RawDescriptor>()
                                .is_some(),
                        ));
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
        if sent == 7 && tick >= last_send + 200 {
            break;
        }
    }
    assert_eq!(
        sent, 7,
        "real source must finish all positive/retired phases"
    );
    assert_eq!(receipts.len(), 5);
    let trigger = if startup_missing { 0 } else { 2 };
    let stages = (0..6).filter(|stage| *stage != trigger).collect::<Vec<_>>();
    let base = receipts[0].0;
    let first_ts = receipts[0].1;
    for (i, stage) in stages.iter().copied().enumerate() {
        let (seq, ts, marker, body, output_dd) = &receipts[i];
        assert_eq!(
            *seq - base,
            i as u64,
            "source trigger drop compacts exactly one sequence slot"
        );
        assert_eq!(
            *ts - first_ts,
            3000 * (stage - stages[0]) as u32,
            "source timestamps keep the dropped-frame duration"
        );
        assert!(*marker);
        assert_eq!(
            *body,
            payload(
                codec,
                10 + stage as u8,
                stage == 0 || (startup_missing && stage == 1)
            )
        );
        assert_eq!(
            *output_dd,
            !startup_missing && stage < 2,
            "valid DD reappearance cannot resurrect the old selector"
        );
    }
    stats.tick(negotiated + Duration::from_secs(6));
    assert_eq!(
        stats.snapshot().histogram_count,
        if timing_enabled { 5 } else { 0 },
        "rejected trigger and retired publication never gain samples"
    );
    if require_original {
        let log = std::fs::read_to_string(
            std::env::var("GO_DD_LIFECYCLE_RESULTS")
                .expect("actual original Buffer and DownTrack receipt"),
        )
        .unwrap();
        let rows = log
            .lines()
            .filter_map(|line| line.strip_prefix("SOURCE_DD_LIFECYCLE_PHYSICAL "))
            .map(|line| serde_json::from_str::<serde_json::Value>(line).unwrap())
            .filter(|row| {
                row["codec"].as_str() == Some(crate::codecs::mime_of(codec))
                    && row["startup_missing"].as_bool() == Some(startup_missing)
            })
            .collect::<Vec<_>>();
        assert_eq!(rows.len(), 6);
        let accepted = rows
            .iter()
            .filter(|row| row["writes"].as_i64() == Some(1))
            .collect::<Vec<_>>();
        assert_eq!(accepted.len(), 5);
        let go_base = accepted[0]["sequence"].as_u64().unwrap();
        let go_ts = accepted[0]["timestamp"].as_u64().unwrap();
        for (i, row) in accepted.iter().enumerate() {
            let (seq, ts, marker, body, dd) = &receipts[i];
            assert_eq!(row["sequence"].as_u64().unwrap() - go_base, *seq - base);
            assert_eq!(
                row["timestamp"].as_u64().unwrap() - go_ts,
                u64::from(*ts - first_ts)
            );
            assert_eq!(row["output_dd"].as_bool(), Some(*dd));
            let hex = body.iter().map(|b| format!("{b:02x}")).collect::<String>();
            assert_eq!(row["payload_hex"].as_str(), Some(hex.as_str()));
            assert!(*marker);
        }
    }
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
