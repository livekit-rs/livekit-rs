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

#[test]
fn encrypted_dd_padding_order_keeps_source_offsets_and_selector_lifetimes_with_timing_on_or_off() {
    for codec in [Codec::Vp9, Codec::Av1] {
        for timing in [false, true] {
            for alternate in [false, true] {
                case_padding(codec, timing, true, false, alternate, false);
                for gap in [false, true] {
                    case_padding(codec, timing, false, gap, alternate, false);
                }
            }
        }
    }
}
#[test]
#[ignore = "requires actual original Go Buffer/DownTrack in-order/OOO and alternate video PT padding receipts"]
fn original_go_dd_padding_physical_offsets_and_latches_match_encrypted_packets() {
    for codec in [Codec::Vp9, Codec::Av1] {
        for timing in [false, true] {
            for alternate in [false, true] {
                case_padding(codec, timing, true, false, alternate, true);
                for gap in [false, true] {
                    case_padding(codec, timing, false, gap, alternate, true);
                }
            }
        }
    }
}
fn case_padding(
    codec: Codec,
    timing_enabled: bool,
    out_of_order: bool,
    gap: bool,
    alternate_pt: bool,
    require_original: bool,
) {
    let mixed = false;
    let independent_high_only = false;
    let start = Instant::now();
    let mut engine = engine(start);
    engine.cfg.rtc.congestion_control.enabled = false;
    engine.cfg.enabled_codecs = vec![CodecSpec {
        mime: crate::codecs::mime_of(codec).into(),
        fmtp_line: String::new(),
    }];
    let other_codec = if codec == Codec::Av1 {
        Codec::Vp9
    } else {
        Codec::Av1
    };
    if alternate_pt {
        engine.cfg.enabled_codecs.push(CodecSpec {
            mime: crate::codecs::mime_of(other_codec).into(),
            fmtp_line: String::new(),
        });
    }
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
            .enable_av1(alternate_pt || codec == Codec::Av1)
            .enable_vp9(alternate_pt || codec == Codec::Vp9)
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
    let ui = engine.up_by_mid[&(pub_tid, mid)];
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
                disable_playout_delay: false,
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

    let padding_pt = if alternate_pt {
        publisher
            .codec_config()
            .params()
            .iter()
            .find(|p| p.spec().codec == other_codec)
            .unwrap()
            .pt()
    } else {
        pt
    };
    if alternate_pt {
        assert_ne!(padding_pt, pt);
    }
    let mut source_rate_identity = None;
    let plan: Vec<(u16, bool, bool)> = if out_of_order {
        vec![
            (1000, false, true),
            (1002, false, true),
            (1001, true, false),
            (1003, false, true),
            (1004, false, false),
            (1005, false, true),
        ]
    } else {
        let p = if gap { 1002 } else { 1001 };
        vec![(1000, false, true), (p, true, false), (p + 1, false, true)]
    };
    let accepted: Vec<usize> = if out_of_order {
        if codec == Codec::Vp9 {
            vec![0, 1, 4, 5]
        } else {
            vec![0, 1, 3, 4, 5]
        }
    } else {
        vec![0, 2]
    };
    let mut sent = 0usize;
    let mut last_send = 0u64;
    let mut receipts = Vec::new();
    let mut retired = false;
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
        if ready && sent < plan.len() && (sent == 0 || tick >= last_send + 200) {
            let expected = accepted.iter().filter(|stage| **stage < sent).count()
                + usize::from(out_of_order && sent > 2);
            assert_eq!(
                receipts.len(),
                expected,
                "every real media/padding output must arrive before next input phase; no frame aggregation or timing-enabled-only control"
            );
            if sent > 0 {
                let up = engine.ups.get(ui).unwrap();
                assert!(
                    up.mime.eq_ignore_ascii_case(crate::codecs::mime_of(codec)),
                    "empty alternate-PT padding cannot change the bound source codec"
                );
                assert!(
                    !engine.codec_receiver_unavailable(ui),
                    "padding cannot perform a genuine nonempty codec-change invalidation"
                );
                let current = up.source_rates.test_source_ids();
                if let Some(expected) = source_rate_identity {
                    assert_eq!(
                        current, expected,
                        "padding cannot reset independent receiver FPS lifetimes"
                    );
                } else {
                    source_rate_identity = Some(current);
                }
            }
            let (sequence, padding, dd) = plan[sent];
            let mut values = str0m::rtp::ExtensionValues::default();
            if dd {
                values.user_values.set(crate::av1::RawDescriptor(
                    raw_chain_descriptor(sequence - 990, 0, true, true, true).into(),
                ));
            }
            let mut direct = publisher.direct_api();
            let stream = direct.stream_tx_by_mid(mid, None).unwrap();
            if padding {
                stream.write_rtp(RtpWrite::padding_only(
                    padding_pt,
                    u64::from(sequence).into(),
                    3000 * (u32::from(sequence) - 990),
                    now,
                ));
            } else {
                stream.write_rtp(
                    RtpWrite::new(
                        pt,
                        u64::from(sequence).into(),
                        3000 * (u32::from(sequence) - 990),
                        now,
                        payload(codec, (sequence - 990) as u8, sent == 0),
                    )
                    .marker(true)
                    .ext_vals(values),
                );
            }
            sent += 1;
            last_send = tick;
        }
        if sent == plan.len() && !retired && tick >= last_send + 300 {
            engine.handle_command(
                MediaCommand::UnpublishTrack {
                    track_sid: sid.clone(),
                },
                now,
            );
            publisher
                .direct_api()
                .stream_tx_by_mid(mid, None)
                .unwrap()
                .write_rtp(
                    RtpWrite::new(pt, 1100.into(), 330000, now, payload(codec, 100, false))
                        .marker(true),
                );
            retired = true;
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
        for (tid, addr, peer, _) in &mut subscribers {
            loop {
                match peer.poll_output().unwrap() {
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
            let (tid, addr, peer, _) = subscribers
                .iter_mut()
                .find(|(_, addr, _, _)| *addr == destination)
                .unwrap();
            peer.handle_input(Input::Receive(
                now,
                Receive::new(Protocol::Udp, server_addr, *addr, &bytes).unwrap(),
            ))
            .unwrap();
            loop {
                match peer.poll_output().unwrap() {
                    Output::Event(Event::RtpPacket(p)) => receipts.push((
                        *p.seq_no,
                        p.header.timestamp,
                        p.header.has_padding,
                        p.payload.to_vec(),
                        p.header
                            .ext_vals
                            .user_values
                            .get::<crate::av1::RawDescriptor>()
                            .is_some(),
                    )),
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
        if retired && tick >= last_send + 500 {
            break;
        }
    }
    assert_eq!(sent, plan.len());
    assert!(retired);
    let padding = receipts.iter().filter(|p| p.2).collect::<Vec<_>>();
    let media = receipts.iter().filter(|p| !p.2).collect::<Vec<_>>();
    assert_eq!(media.len(), accepted.len());
    assert_eq!(padding.len(), usize::from(out_of_order));
    let base = media[0].0;
    if out_of_order {
        assert_eq!(
            padding[0].0 - base,
            1,
            "a genuine OOO missing slot is filled with actual RTP padding"
        );
        assert!(padding[0].3.is_empty());
    }
    let expected_offsets = if out_of_order {
        if codec == Codec::Vp9 {
            vec![0, 2, 4, 5]
        } else {
            vec![0, 2, 3, 4, 5]
        }
    } else {
        vec![0, if gap { 2 } else { 1 }]
    };
    for (i, stage) in accepted.iter().copied().enumerate() {
        let (sequence, _, _) = plan[stage];
        let (seq, ts, _, body, has_dd) = media[i];
        assert_eq!(*seq - base, expected_offsets[i]);
        assert_eq!(*ts - media[0].1, 3000 * (u32::from(sequence) - 1000));
        assert_eq!(*body, payload(codec, (sequence - 990) as u8, stage == 0));
        assert_eq!(
            *has_dd,
            !out_of_order || stage < 2,
            "in-order padding never retires DD, OOO nilDD retires selector but not VP9 Buffer parser"
        );
    }
    stats.tick(negotiated + Duration::from_secs(6));
    assert_eq!(
        stats.snapshot().histogram_count,
        if timing_enabled {
            accepted.len() as u64
        } else {
            0
        },
        "padding/rejected DD packets/retired source cannot add timing samples"
    );
    if require_original {
        let log = std::fs::read_to_string(
            std::env::var("GO_DD_PADDING_RESULTS").expect("actual original Go padding controls"),
        )
        .unwrap();
        if out_of_order {
            let rows = log
                .lines()
                .filter_map(|line| {
                    line.strip_prefix(if alternate_pt {
                        "SOURCE_DD_OOO_PADDING_ALT_PT "
                    } else {
                        "SOURCE_DD_OOO_PADDING "
                    })
                })
                .map(|line| serde_json::from_str::<serde_json::Value>(line).unwrap())
                .filter(|row| row["codec"].as_str() == Some(crate::codecs::mime_of(codec)))
                .collect::<Vec<_>>();
            assert_eq!(rows.len(), 6);
            for (stage, row) in rows.iter().enumerate() {
                assert_eq!(
                    row["writes"].as_u64(),
                    Some(u64::from(accepted.contains(&stage))),
                    "actual Go per-original-packet accepted count"
                );
            }
        } else {
            let rows = log
                .lines()
                .filter_map(|line| {
                    line.strip_prefix(if alternate_pt {
                        "SOURCE_DD_INORDER_PADDING_ALT_PT "
                    } else {
                        "SOURCE_DD_INORDER_PADDING "
                    })
                })
                .map(|line| serde_json::from_str::<serde_json::Value>(line).unwrap())
                .filter(|row| {
                    row["codec"].as_str() == Some(crate::codecs::mime_of(codec))
                        && row["gap"].as_bool() == Some(gap)
                })
                .collect::<Vec<_>>();
            assert_eq!(rows.len(), 2);
            let go_base = rows[0]["output_sequence"].as_u64().unwrap();
            for (i, row) in rows.iter().enumerate() {
                assert_eq!(
                    row["output_sequence"].as_u64().unwrap() - go_base,
                    media[i].0 - base
                );
            }
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
