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
fn encrypted_spatial_packet_admission_counts_only_successful_layer_union_and_retires() {
    for (mixed, out_of_order) in [(false, false), (true, false), (true, true)] {
        spatial_case(mixed, out_of_order, false, true, false);
    }
}
#[test]
fn encrypted_high_only_independent_spatial_frame_reaches_real_high_subscriber() {
    spatial_case(false, false, true, true, false);
}
#[test]
fn encrypted_high_only_spatial_forwarding_does_not_require_timing_enabled() {
    spatial_case(false, false, true, false, false);
}
#[test]
fn encrypted_lower_spatial_higher_temporal_packet_follows_original_vp9_order() {
    spatial_case(true, false, false, true, true);
}
fn spatial_case(
    mixed: bool,
    out_of_order: bool,
    independent_high_only: bool,
    timing_enabled: bool,
    temporal_probe: bool,
) {
    let start = Instant::now();
    let mut engine = engine(start);
    engine.cfg.rtc.congestion_control.enabled = false;
    engine.cfg.enabled_codecs = vec![CodecSpec {
        mime: "video/VP9".into(),
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
    let sid = TrackSid::from("TR_svc_packet_union");
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
            .enable_vp9(true)
            .enable_raw_packets(true)
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
                    fps: if temporal_probe { 1 } else { 0 },
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
        .find(|p| p.spec().codec == Codec::Vp9)
        .unwrap()
        .pt();
    let mut received_metadata = Vec::new();
    let mut received_packets = Vec::new();
    let mut primed = 0usize;
    let mut last_prime = 0u64;
    let mut baseline_count = 0u64;
    let mut sent = 0usize;
    let mut first_send = None;
    let mut after_first = None;
    let mut after_second = None;
    let mut first_received_counts = None;
    for tick in 0..2400u64 {
        let now = negotiated + Duration::from_millis(tick);
        publisher.handle_input(Input::Timeout(now)).unwrap();
        for (tid, _, subscriber, _) in &mut subscribers {
            if engine.transports.contains_key(tid) {
                subscriber.handle_input(Input::Timeout(now)).unwrap();
            }
        }
        let ready = publisher.is_connected()
            && subscribers.iter().all(|(tid, _, peer, _)| {
                !engine.transports.contains_key(tid) || peer.is_connected()
            });
        if ready && temporal_probe && primed < 9 && (primed == 0 || tick >= last_prime + 50) {
            for spatial in 0..3u16 {
                let payload = if primed == 0 {
                    vec![
                        0xbc,
                        primed as u8,
                        (spatial as u8) << 1 | u8::from(spatial > 0),
                        0x80 + spatial as u8,
                        0xf0 + primed as u8,
                    ]
                } else {
                    vec![
                        0xfc,
                        primed as u8,
                        (spatial as u8) << 1 | u8::from(spatial > 0),
                        2,
                        0x84 + spatial as u8,
                        0xf0 + primed as u8,
                    ]
                };
                publisher
                    .direct_api()
                    .stream_tx_by_mid(mid, None)
                    .unwrap()
                    .write_rtp(
                        RtpWrite::new(
                            pt,
                            (1000 + primed as u64 * 3 + u64::from(spatial)).into(),
                            3000 + primed as u32 * 6000,
                            now,
                            payload,
                        )
                        .marker(spatial == 2),
                    );
            }
            primed += 1;
            last_prime = tick;
        }
        if ready && (!temporal_probe || (primed == 9 && tick >= last_prime + 80)) {
            if temporal_probe && first_send.is_none() {
                assert_eq!(
                    subscribers.iter().map(|s| s.3.len()).collect::<Vec<_>>(),
                    vec![9, 9],
                    "actual encrypted T0 source priming must reach both targets"
                );
                assert!(
                    engine.ups.iter().all(|(_, up)| up.svc_temporal_cap(1) == 0),
                    "real source RTP cadence must resolve FPS1 to T0"
                );
                assert!(
                    engine
                        .downs
                        .iter()
                        .all(|(_, d)| d.forwarded_temporal_cap() == 0),
                    "actual FPS1 target must resolve to T0 before comparing fixed Go T0 observer"
                );
                stats.tick(now);
                baseline_count = stats.snapshot().histogram_count;
                assert_eq!(
                    baseline_count, 27,
                    "nine actual three-component source pictures primed the real successful union"
                );
                for (_, _, _, frames) in &mut subscribers {
                    frames.clear();
                }
                received_metadata.clear();
                received_packets.clear();
            }
            let begin = *first_send.get_or_insert(tick);
            if sent == 0
                || (sent == 1 && tick >= begin + 500)
                || (sent == 2 && tick >= begin + 1000)
            {
                if sent == 1 {
                    stats.tick(now);
                    after_first = Some(stats.snapshot().histogram_count - baseline_count);
                    first_received_counts = Some(
                        subscribers
                            .iter()
                            .map(|(_, _, _, frames)| frames.len())
                            .collect::<Vec<_>>(),
                    );
                    if mixed {
                        engine.handle_command(MediaCommand::CloseTransport(subscribers[1].0), now);
                    }
                }
                if sent == 2 {
                    stats.tick(now);
                    after_second = Some(stats.snapshot().histogram_count - baseline_count);
                    engine.handle_command(
                        MediaCommand::UnpublishTrack {
                            track_sid: sid.clone(),
                        },
                        now,
                    );
                }
                let order = if independent_high_only {
                    vec![2u16]
                } else if out_of_order {
                    vec![0, 2, 1]
                } else {
                    vec![0, 1, 2]
                };
                for spatial in order {
                    let seq = if independent_high_only {
                        1000 + sent as u64
                    } else {
                        if temporal_probe {
                            1027 + sent as u64 * 4 + u64::from(spatial)
                        } else {
                            1000 + sent as u64 * 3 + u64::from(spatial)
                        }
                    };
                    let layer =
                        (spatial as u8) << 1 | u8::from(spatial > 0 && !independent_high_only);
                    publisher
                        .direct_api()
                        .stream_tx_by_mid(mid, None)
                        .unwrap()
                        .write_rtp(
                            RtpWrite::new(
                                pt,
                                seq.into(),
                                if temporal_probe {
                                    57000 + sent as u32 * 6000
                                } else {
                                    3000 + sent as u32 * 3000
                                },
                                now,
                                vec![0x3c, layer, 0x80 + spatial as u8, sent as u8],
                            )
                            .marker(spatial == 2),
                        );
                }
                if temporal_probe {
                    // Fixed original Select with current(S2,T0) admits (S0,T2)
                    // while current(S0,T0) rejects it. Actual encrypted source,
                    // distinct timestamp/SN, followed by real subscriber bytes.
                    publisher
                        .direct_api()
                        .stream_tx_by_mid(mid, None)
                        .unwrap()
                        .write_rtp(
                            RtpWrite::new(
                                pt,
                                (1027 + sent as u64 * 4 + 3).into(),
                                60000 + sent as u32 * 6000,
                                now,
                                vec![0x7c, 2 << 5, 2, 0x80, 0xee + sent as u8],
                            )
                            .marker(true),
                        );
                }
                sent += 1;
            }
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
                    Output::Event(Event::MediaData(data)) => {
                        received_metadata.push((*tid, data.codec_extra));
                        frames.push(data.data.to_vec());
                    }
                    Output::Event(Event::RawPacket(packet)) => {
                        if let str0m::rtp::RawPacket::RtpRx(header, body) = *packet {
                            received_packets.push((*tid, header, body));
                        }
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
        sent, 3,
        "actual connected source must emit initial, post-close, and retired publications"
    );
    let expected_first = if !timing_enabled {
        0
    } else if independent_high_only {
        1
    } else if mixed {
        if out_of_order {
            2
        } else if temporal_probe {
            4
        } else {
            3
        }
    } else {
        1
    };
    assert_eq!(after_first, Some(expected_first));
    assert_eq!(
        after_second,
        Some(expected_first + u64::from(timing_enabled)),
        "closed high target cannot admit enhancement packets"
    );
    stats.tick(negotiated + Duration::from_secs(3));
    assert_eq!(
        stats.snapshot().histogram_count - baseline_count,
        expected_first + u64::from(timing_enabled),
        "unpublished source must not retain or admit queued timing cohorts"
    );
    let initial = first_received_counts.unwrap();
    assert!(
        initial.iter().all(|n| *n > 0),
        "each admitted target must actually receive authenticated media"
    );
    let low = &subscribers[0].3;
    assert_eq!(
        low[0],
        if independent_high_only {
            vec![0x82, 0]
        } else {
            vec![0x80, 0]
        },
        "sink receives exactly its selected spatial bytes"
    );
    if mixed {
        assert_eq!(
            subscribers[1].3[0],
            vec![0x80, 0, 0x81, 0, 0x82, 0],
            "high sink receives every real separately described spatial component"
        );
    }
    if mixed {
        assert!(received_metadata.iter().any(|(tid,extra)|*tid==subscribers[1].0&&matches!(extra,str0m::format::CodecExtra::Vp9(v)if v.layers_scheme==[Some(2),Some(4),Some(6)]&&v.tid==Some(0))),
            "real received VP9 metadata must retain all three component boundaries/T0, not one flattened SID0");
    }
    if temporal_probe {
        assert_eq!(
            initial,
            vec![1, 1],
            "lower-only T2 is forwarded as a non-final component, not an extra displayed picture"
        );
        let matches_probe = |body: &[u8]| {
            body.len() >= 7
                && (body[0] & 0xa0) == 0xa0
                && (body[0] & 0x10) == 0
                && body[3] >> 5 == 2
                && (body[3] >> 1) & 7 == 0
                && body[body.len() - 2..] == [0x80, 0xee]
        };
        assert!(
            !received_packets
                .iter()
                .any(|(tid, _, b)| *tid == subscribers[0].0 && matches_probe(b)),
            "low T0 sink must not receive S0T2"
        );
        assert!(
            received_packets
                .iter()
                .any(|(tid, h, b)| *tid == subscribers[1].0 && matches_probe(b) && !h.marker),
            "high sink must physically receive authenticated S0T2 bytes with original non-final marker semantics"
        );
    }
    assert!(engine.up_by_sid.is_empty());
    for (tid, _, _, _) in subscribers {
        engine.handle_command(
            MediaCommand::CloseTransport(tid),
            negotiated + Duration::from_secs(3),
        );
    }
    engine.handle_command(
        MediaCommand::CloseTransport(pub_tid),
        negotiated + Duration::from_secs(3),
    );
    assert!(engine.transports.is_empty());
}

#[test]
#[ignore = "requires original Go physical DownTrack observer GO_SVC_FORWARD_RESULTS"]
fn original_go_per_packet_successful_writes_match_native_admission_union() {
    use str0m::media::ForwardingPacket;
    let log = std::fs::read_to_string(
        std::env::var("GO_SVC_FORWARD_RESULTS").expect("actual original Go log"),
    )
    .unwrap();
    let now = Instant::now();
    let mut engine = engine(now);
    let stats =
        rtc_core::forward_stats::ForwardStats::new(100_000_000, 100_000_000, 1_000_000_000, now)
            .unwrap();
    engine.cfg.rtc.forward_stats = Some(stats.clone());
    let mut rows = 0;
    let mut expected_samples = 0;
    for line in log.lines() {
        let Some((_, raw)) = line.split_once("SOURCE_SVC_ADMISSION ") else {
            continue;
        };
        let row: serde_json::Value = serde_json::from_str(raw).unwrap();
        let packet = ForwardingPacket {
            sequence: row["sequence"].as_u64().unwrap(),
            spatial: Some(row["spatial"].as_u64().unwrap() as u8),
            temporal: Some(row["temporal"].as_u64().unwrap() as u8),
            extension_index: 0,
            received: now,
            out_of_order: row["out_of_order"].as_bool().unwrap(),
            buffered: false,
        };
        let mut admissions = crate::forward_timing::ForwardingAdmissions::new(&[packet]);
        admissions.accepted_vp9(0, 0, 0);
        if row["case"].as_str().unwrap() != "low_only" && row["phase"].as_u64().unwrap() == 0 {
            admissions.accepted_vp9(2, 0, 0)
        }
        assert_eq!(
            u64::from(admissions.write_counts()[0]),
            row["writes"].as_u64().unwrap(),
            "actual Go DownTrack/pacer returned: {row}"
        );
        engine.record_forwarding_cohort(
            &admissions,
            now + Duration::from_millis(5),
            now + Duration::from_millis(1),
        );
        expected_samples += u64::from(row["eligible"].as_bool().unwrap());
        rows += 1;
    }
    assert_eq!(rows, 24, "all physical original writer rows must execute");
    stats.tick(now + Duration::from_millis(100));
    assert_eq!(stats.snapshot().histogram_count, expected_samples);
}
