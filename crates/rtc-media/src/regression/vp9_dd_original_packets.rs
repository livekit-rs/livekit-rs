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

// Genuine flexible-mode VP9 header: 15-bit PID, actual TID2 and P_DIFF1.
// DD says T0, so a payload-based temporal selector must not replace this DD
// decision. The original Go Null munger retains these exact source bytes.
fn source_vp9_payload(frame: u8, first: bool) -> Vec<u8> {
    let predicted = frame != 10 && frame != 13;
    let mut bytes = vec![
        0xb0 | if predicted { 0x40 } else { 0 } | if first { 0x08 } else { 0x04 },
        0x81,
        frame,
        0x40,
    ];
    if predicted {
        bytes.push(2);
    }
    bytes.extend_from_slice(&[0x34, frame, u8::from(first)]);
    bytes
}
#[test]
fn encrypted_vp9_dd_original_fragments_keep_payload_pid_references_and_retire() {
    for enabled in [false, true] {
        vp9_dd_original_packets_case(enabled, false, false, false);
        vp9_dd_original_packets_case(enabled, true, false, false);
    }
}
#[test]
fn forwarded_vp9_dd_keyframe_ends_subscriber_nack_throttle() {
    vp9_dd_original_packets_case(false, false, false, true);
}

#[test]
fn encrypted_vp9_dd_mask_change_reallocates_before_the_next_bitrate_report() {
    let start = Instant::now();
    let mut engine = engine(start);
    engine.cfg.rtc.congestion_control.use_send_side_bwe = false;
    let report_interval = Duration::from_millis(125);
    engine.cfg.video.bitrate_report.video = [Some(report_interval); 3];
    engine.cfg.enabled_codecs = vec![CodecSpec {
        mime: "video/VP9".into(),
        fmtp_line: String::new(),
    }];
    let packets = PacketQueue::default();
    engine.out = Box::new(packets.clone());
    let server_addr = "127.0.0.1:9691".parse().unwrap();
    let peer_addr = "127.0.0.1:9692".parse().unwrap();
    let tid = transport(&mut engine, PcRole::Single, start);
    let transport = engine.transports.get_mut(&tid).unwrap();
    let candidate = transport
        .rtc
        .add_local_candidate(Candidate::host(server_addr, "udp").unwrap())
        .unwrap()
        .clone();
    transport.candidates.push(candidate);
    let mut peer = Rtc::builder()
        .set_rtp_mode(true)
        .set_crypto_provider(rtc_crypto::provider().into())
        .set_dtls_cert(rtc_crypto::dtls_cert().unwrap())
        .clear_codecs()
        .enable_vp9(true)
        .set_extension(
            13,
            str0m::rtp::Extension::with_serializer(crate::av1::URI, SourceDd),
        )
        .build(start);
    peer.add_local_candidate(Candidate::host(peer_addr, "udp").unwrap());
    let mid = published_mid(&offer(&mut engine, &mut peer, tid, start, |change| {
        change.add_media(MediaKind::Video, Direction::SendOnly, None, None, None);
    }));
    let sid = TrackSid::from("TR_vp9_dd_mask_allocation");
    engine.bind_track(tid, &mid.to_string(), sid.clone(), start);
    let ui = engine.up_by_sid[&sid];
    let up = engine.ups.get_mut(ui).unwrap();
    up.layers = [VideoQuality::Low, VideoQuality::Medium, VideoQuality::High]
        .into_iter()
        .enumerate()
        .map(|(spatial, quality)| VideoLayer {
            quality,
            width: 320 * (spatial as u32 + 1),
            height: 180 * (spatial as u32 + 1),
            bitrate: 10_000,
            ssrc: 0,
            spatial_layer: spatial as i32,
            rid: String::new(),
            repair_ssrc: 0,
        })
        .collect();
    up.spec.codecs = vec![rtc_core::model::TrackCodec {
        mime_type: "video/VP9".into(),
        layer_mode: rtc_core::model::VideoLayerMode::MultipleSpatialLayersPerStream,
        ..Default::default()
    }];
    let participant = ParticipantSid::from("PA_test");
    engine.subscribe(
        DownTrackSpec {
            subscriber: participant.clone(),
            publisher: participant.clone(),
            track_sid: sid.clone(),
            stream_id: format!("source|{sid}"),
            settings: TrackSettings::default(),
            adaptive_stream: false,
        },
        start,
    );
    offer(&mut engine, &mut peer, tid, start, |change| {
        change.add_media(MediaKind::Video, Direction::RecvOnly, None, None, None);
    });
    engine.handle_command(
        MediaCommand::SetBandwidthOverride {
            participant: participant.clone(),
            bps: 250_000,
        },
        start,
    );
    let di = engine.down_by_key[&(participant, sid)];
    assert!(engine.downs.get(di).unwrap().negotiated);
    let pt = peer
        .codec_config()
        .params()
        .iter()
        .find(|p| p.spec().codec == Codec::Vp9)
        .unwrap()
        .pt();
    let source: [(u64, &[u8]); 5] = [
        (0, &[0xc0, 0, 1, 0x80, 0x01, 0xba, 0x20]),
        (33, &[0xc1, 0, 2]),
        (145, &[0xc0, 0, 3, 0x42]),
        (178, &[0xc0, 0, 4, 0x46]),
        (211, &[0xc1, 0, 5]),
    ];
    let mut origin = None;
    let mut sent = 0;
    let mut verified = 0;
    let mut warm_costs = None;
    let mut received = Vec::new();
    for tick in 0..1200 {
        let now = start + Duration::from_millis(tick);
        peer.handle_input(Input::Timeout(now)).unwrap();
        if peer.is_connected()
            && let Some(&(offset, descriptor)) = source.get(sent)
        {
            let origin = *origin.get_or_insert(tick);
            if tick >= origin + offset {
                let frame = (sent + 1) as u8;
                let mut values = str0m::rtp::ExtensionValues::default();
                values
                    .user_values
                    .set(crate::av1::RawDescriptor(descriptor.into()));
                peer.direct_api()
                    .stream_tx_by_mid(mid, None)
                    .unwrap()
                    .write_rtp(
                        RtpWrite::new(
                            pt,
                            (1000 + u64::from(frame)).into(),
                            u32::from(frame) * 3000,
                            now,
                            source_vp9_payload(frame, true),
                        )
                        .marker(true)
                        .ext_vals(values),
                    );
                sent += 1;
            }
        }
        loop {
            match peer.poll_output().unwrap() {
                Output::Transmit(packet) => {
                    let data = packet.contents.as_ref();
                    let marker = (data.len() >= 12 && data[1] == (0x80 | *pt))
                        .then(|| u16::from_be_bytes([data[2], data[3]]));
                    if marker == Some(1003) {
                        let up = engine.ups.get(ui).unwrap();
                        let [base, upper, _] = up.bitrate_observation().bitrates;
                        assert!(base > 0 && upper > base, "warmed authenticated rates");
                        assert!(up.next_bitrate_report().unwrap() > now);
                        assert_eq!(engine.transports[&tid].allocated_bps, upper);
                        warm_costs = Some((base, upper));
                    }
                    engine.handle_datagram(crate::net::Datagram {
                        transport: tid,
                        proto: Proto::Udp,
                        source: peer_addr,
                        destination: server_addr,
                        data,
                        received_at: now,
                    });
                    if matches!(marker, Some(1003 | 1004)) {
                        let up = engine.ups.get(ui).unwrap();
                        assert!(up.next_bitrate_report().unwrap() > now);
                        let (base, upper) = warm_costs.unwrap();
                        let (cap, bps) = if marker == Some(1003) {
                            (0, base)
                        } else {
                            (1, upper)
                        };
                        assert_eq!(engine.downs.get(di).unwrap().alloc_cap, cap);
                        assert_eq!(engine.transports[&tid].allocated_bps, bps);
                        verified += 1;
                    }
                }
                Output::Event(Event::RtpPacket(packet)) if !packet.payload.is_empty() => {
                    received.push(packet.payload[2]);
                }
                Output::Timeout(_) => break,
                _ => {}
            }
        }
        engine.handle_timeout(now);
        let outbound = packets.0.lock().unwrap().drain(..).collect::<Vec<_>>();
        for packet in outbound {
            peer.handle_input(Input::Receive(
                now,
                Receive::new(Protocol::Udp, server_addr, peer_addr, &packet).unwrap(),
            ))
            .unwrap();
        }
        if verified == 2 && received.contains(&5) {
            assert!(received.contains(&3), "base packet forwarded after shrink");
            assert!(received.contains(&4), "base packet forwarded on restore");
            return;
        }
    }
    panic!(
        "DD mask transition did not converge: {sent} sent, {verified} allocations, {received:?} received"
    );
}

#[test]
#[ignore = "requires original public Go DownTrack/Null codec munger physical SRTP receipt"]
fn actual_original_vp9_dd_null_munger_matches_original_fragments() {
    for timing in [false, true] {
        for dropped in [false, true] {
            vp9_dd_original_packets_case(timing, dropped, true, false);
        }
    }
}
// Native frame payload receipts, not a pixel-decoder test. Wire serializer is
// test-owned; the receiving engine uses the unchanged production DD parser.
#[derive(Debug)]
pub(super) struct SourceDd;
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
fn vp9_dd_original_packets_case(
    timing_enabled: bool,
    dropped_slot: bool,
    require_original: bool,
    check_throttle: bool,
) {
    let mixed = false;
    let independent_high_only = false;
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
            .enable_vp9(true)
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
        .find(|p| p.spec().codec == Codec::Vp9)
        .unwrap()
        .pt();
    let mut sent = 0usize;
    let mut last_send = 0u64;
    let mut checkpoints = Vec::new();
    let source_frames = [10u16, 11, 12, 13, 14, 16, 15, 17, 18];
    let source_payload = |frame, first| {
        let mut payload = source_vp9_payload(frame, first);
        if check_throttle && frame == 10 && first {
            // Profile 0 keyframe in the first original RTP fragment.
            payload[4] = 0x80;
        }
        payload
    };
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
            if check_throttle && sent < 2 {
                let subscriber = subscribers[0].0;
                let &(_, down_mid) = engine
                    .down_by_mid
                    .keys()
                    .find(|(transport, _)| *transport == subscriber)
                    .expect("subscriber downtrack must be bound");
                let mut direct = engine
                    .transports
                    .get_mut(&subscriber)
                    .unwrap()
                    .rtc
                    .direct_api();
                let stream = direct.stream_tx_by_mid(down_mid, None).unwrap();
                if sent == 0 {
                    let keyframe = source_payload(10, true);
                    assert!(crate::dd_fallback::vp9_key(&keyframe));
                    assert!(!crate::dd_fallback::av1_key(&keyframe));
                    stream.set_nack_throttled(true);
                } else {
                    assert_eq!(subscribers[0].3.len(), 1);
                    assert!(
                        !stream.nack_throttled(),
                        "forwarded VP9 keyframe must clear the throttle"
                    );
                }
            }
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
                            source_payload(source_frames[sent / 2] as u8, sent % 2 == 0),
                        )
                        .marker(sent % 2 == 1 && source_frames[sent / 2] != 14)
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
                                "real source-dropped slot is RTP padding, never fabricated VP9 media"
                            );
                            wire_padding.push(*packet.seq_no);
                            continue;
                        }
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
            .flat_map(|frame| [source_payload(frame, true), source_payload(frame, false)])
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
    if require_original {
        let log = std::fs::read_to_string(
            std::env::var("GO_VP9_DD_RESULTS").expect("actual original VP9 DD SRTP receipt"),
        )
        .unwrap();
        let rows = log
            .lines()
            .filter_map(|line| line.strip_prefix("SOURCE_VP9_ORIGINAL_PACKET "))
            .map(|line| serde_json::from_str::<serde_json::Value>(line).unwrap())
            .filter(|row| row["dropped_slot"].as_bool() == Some(dropped_slot))
            .collect::<Vec<_>>();
        assert_eq!(
            rows.len(),
            12,
            "exact original physical fragments, no no-tests/partial receipt"
        );
        let go_base = rows[0]["sequence"].as_u64().unwrap();
        for (i, row) in rows.iter().enumerate() {
            let (frame, sequence, marker) = wire_receipts[i];
            assert_eq!(row["frame"].as_u64(), Some(u64::from(frame)));
            assert_eq!(row["sequence"].as_u64().unwrap() - go_base, sequence - base);
            assert_eq!(row["part"].as_u64(), Some((i % 2) as u64));
            assert_eq!(row["marker"].as_bool(), Some(marker));
            let payload_hex = subscribers[0].3[i]
                .iter()
                .map(|byte| format!("{byte:02x}"))
                .collect::<String>();
            assert_eq!(
                row["payload_hex"].as_str(),
                Some(payload_hex.as_str()),
                "original VP9 PID/reference bytes must not be compacted by a codec munger"
            );
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

#[test]
fn encrypted_vp9_dd_zero_fps_retains_subscriber_temporal_cap() {
    let start = Instant::now();
    let mut engine = engine(start);
    engine.cfg.rtc.congestion_control.enabled = false;
    let bitrate_window = Duration::from_millis(132);
    engine.cfg.video.bitrate_report.video = [Some(bitrate_window), None, None];
    engine.cfg.enabled_codecs = vec![CodecSpec {
        mime: "video/VP9".into(),
        fmtp_line: String::new(),
    }];
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
                codecs: vec![rtc_core::model::TrackCodec {
                    mime_type: "video/VP9".into(),
                    layer_mode: rtc_core::model::VideoLayerMode::MultipleSpatialLayersPerStream,
                    ..Default::default()
                }],
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
            .enable_vp9(true)
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
    let tid = TransportId::next();
    let participant = ParticipantSid::from("PA_fps_sink");
    let addr = std::net::SocketAddr::from(([127, 0, 0, 1], 9693));
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
            subscriber: participant.clone(),
            publisher: "PA_test".into(),
            track_sid: sid.clone(),
            stream_id: format!("source|{sid}"),
            settings: TrackSettings {
                quality: VideoQuality::Low,
                ..Default::default()
            },
            adaptive_stream: false,
        }),
        start,
    );
    let negotiated = start + Duration::from_millis(160);
    engine.handle_timeout(negotiated);
    answer_owned_offer(&mut engine, &mut subscriber, tid, negotiated);
    let pt = publisher
        .codec_config()
        .params()
        .iter()
        .find(|p| p.spec().codec == Codec::Vp9)
        .unwrap()
        .pt();
    let mut sent = 0u16;
    let mut next_at = 0;
    let mut stage = 0;
    let mut received = Vec::new();
    let mut wire_sizes = [0usize; 48];
    let mut allocated = Vec::new();
    for tick in 0..5000 {
        let now = negotiated + Duration::from_millis(tick);
        for peer in [&mut publisher, &mut subscriber] {
            peer.handle_input(Input::Timeout(now)).unwrap();
        }
        if publisher.is_connected() && subscriber.is_connected() && tick >= next_at {
            if (stage == 0 && sent == 40) || (stage == 1 && sent == 44) {
                engine.allocate_transport(tid, now);
                allocated.push(engine.transports[&tid].allocated_bps);
                let fps = if stage == 0 { 1 } else { 0 };
                if fps > 0 {
                    let up = engine.ups.get(engine.up_by_mid[&(pub_tid, mid)]).unwrap();
                    assert_eq!(
                        up.svc_temporal_cap_for_spatial(0, fps),
                        0,
                        "authenticated rate training"
                    );
                }
                engine.handle_command(
                    MediaCommand::UpdateTrackSettings {
                        subscriber: participant.clone(),
                        track_sid: sid.clone(),
                        settings: TrackSettings {
                            quality: VideoQuality::Low,
                            fps,
                            ..Default::default()
                        },
                    },
                    now,
                );
                stage += 1;
                next_at = tick + 150;
            } else if sent < 48 {
                let frame = sent + 1;
                let (descriptor, mut payload) =
                    super::dd_one_rid_counterexample::two_temporal_vp9_frame(frame);
                let marker = payload.pop().unwrap();
                payload.resize(if frame % 2 == 1 { 89 } else { 139 }, 0);
                payload.push(marker);
                let mut values = str0m::rtp::ExtensionValues::default();
                values
                    .user_values
                    .set(crate::av1::RawDescriptor(descriptor.into()));
                publisher
                    .direct_api()
                    .stream_tx_by_mid(mid, None)
                    .unwrap()
                    .write_rtp(
                        RtpWrite::new(
                            pt,
                            (1000 + u64::from(frame)).into(),
                            u32::from(frame) * 3000,
                            now,
                            payload,
                        )
                        .marker(true)
                        .ext_vals(values),
                    );
                sent += 1;
                next_at = tick + 33;
            }
        }
        for (peer, peer_tid, source) in [
            (&mut publisher, pub_tid, pub_addr),
            (&mut subscriber, tid, addr),
        ] {
            loop {
                match peer.poll_output().unwrap() {
                    Output::Transmit(p) => {
                        let contents = &p.contents;
                        if peer_tid == pub_tid
                            && contents.len() >= 12
                            && contents[0] >> 6 == 2
                            && contents[1] & 0x7f == *pt
                        {
                            let seq = u16::from_be_bytes([contents[2], contents[3]]);
                            assert_eq!(contents[0] & 0x20, 0);
                            wire_sizes[usize::from(seq - 1001)] = contents.len();
                        }
                        engine.handle_datagram(crate::net::Datagram {
                            transport: peer_tid,
                            proto: Proto::Udp,
                            source,
                            destination: server_addr,
                            data: contents,
                            received_at: now,
                        });
                    }
                    Output::Event(Event::RtpPacket(packet))
                        if peer_tid == tid && !packet.payload.is_empty() =>
                    {
                        received.push(*packet.payload.last().unwrap());
                    }
                    Output::Timeout(_) => break,
                    _ => {}
                }
            }
        }
        engine.handle_timeout(now);
        let outbound = { packets.0.lock().unwrap().drain(..).collect::<Vec<_>>() };
        for (destination, bytes) in outbound {
            let peer = if destination == pub_addr {
                &mut publisher
            } else {
                assert_eq!(destination, addr);
                &mut subscriber
            };
            peer.handle_input(Input::Receive(
                now,
                Receive::new(Protocol::Udp, server_addr, destination, &bytes).unwrap(),
            ))
            .unwrap();
            loop {
                match peer.poll_output().unwrap() {
                    Output::Transmit(p) => engine.handle_datagram(crate::net::Datagram {
                        transport: if destination == addr { tid } else { pub_tid },
                        proto: Proto::Udp,
                        source: destination,
                        destination: server_addr,
                        data: &p.contents,
                        received_at: now,
                    }),
                    Output::Event(Event::RtpPacket(packet))
                        if destination == addr && !packet.payload.is_empty() =>
                    {
                        received.push(*packet.payload.last().unwrap());
                    }
                    Output::Timeout(_) => break,
                    _ => {}
                }
            }
        }
        if sent == 48 && tick > next_at + 200 {
            engine.allocate_transport(tid, now);
            allocated.push(engine.transports[&tid].allocated_bps);
            break;
        }
    }
    assert_eq!((sent, stage), (48, 2));
    let before: Vec<_> = received
        .iter()
        .copied()
        .filter(|n| (37..=40).contains(n))
        .collect();
    let capped: Vec<_> = received
        .iter()
        .copied()
        .filter(|n| (41..=44).contains(n))
        .collect();
    let zero: Vec<_> = received
        .iter()
        .copied()
        .filter(|n| (45..=48).contains(n))
        .collect();
    eprintln!("authenticated subscriber packets before={before:?} capped={capped:?} zero={zero:?}");
    assert_eq!(before, [37, 38, 39, 40], "uncapped DD admits T0 and T1");
    assert_eq!(capped, [41, 43], "applied positive FPS caps DD at T0");
    assert_eq!(
        zero,
        [45, 47],
        "zero FPS retains the applied DD temporal maximum"
    );

    let auth_tag_len = engine.transports[&pub_tid]
        .rtc
        .receive_srtp_auth_tag_len()
        .expect("negotiated publisher SRTP tag length");
    let logical_base_bytes: usize = [41, 43]
        .into_iter()
        .map(|frame| {
            wire_sizes[frame - 1]
                .checked_sub(auth_tag_len)
                .expect("base cohort packet observed on the publisher wire")
        })
        .sum();
    let expected_base = (logical_base_bytes as f64 * 8.0 / bitrate_window.as_secs_f64()) as u64;
    eprintln!(
        "authenticated T0 cohort {logical_base_bytes} RTP bytes, expected {expected_base} bps, allocations={allocated:?}"
    );
    assert_eq!(allocated.len(), 3);
    assert!(
        allocated[0] > expected_base,
        "default temporal budget includes enhancement packets"
    );
    assert_eq!(
        allocated[1..],
        [expected_base, expected_base],
        "the allocated temporal cost follows applied FPS and remains after zero"
    );
    let retire_at = negotiated + Duration::from_secs(6);
    engine.handle_command(
        MediaCommand::UnpublishTrack {
            track_sid: sid.clone(),
        },
        retire_at,
    );
    engine.handle_command(MediaCommand::CloseTransport(pub_tid), retire_at);
    engine.handle_command(MediaCommand::CloseTransport(tid), retire_at);
    assert!(engine.transports.is_empty() && !engine.up_by_sid.contains_key(&sid));
}
