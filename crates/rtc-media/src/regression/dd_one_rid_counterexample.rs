//! Actual SRTP spatial packets and successful subscriber admissions.
use super::*;
#[path = "dd_one_rid_literal.rs"]
mod literal;
use rtc_core::media_api::{DownTrackSpec, MediaCommand, PublishedTrackSpec};
use rtc_core::model::{
    TrackCodec, TrackKind, TrackSettings, TrackSource, VideoLayer, VideoLayerMode, VideoQuality,
};
use str0m::format::Codec;
use str0m::media::{Rid, Simulcast, SimulcastLayer};
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

#[test]
fn encrypted_one_mode_original_fragment_must_precede_frame_assembly() {
    for codec in [Codec::Vp9, Codec::Av1] {
        for mode in [
            VideoLayerMode::OneSpatialLayerPerStream,
            VideoLayerMode::OneSpatialLayerPerStreamIncompleteRtcpSr,
        ] {
            case(codec, mode, Scenario::Fragments { rid: false });
        }
    }
}
#[test]
fn encrypted_rid_low_original_fragment_must_precede_frame_assembly() {
    for codec in [Codec::Vp9, Codec::Av1] {
        for mode in [
            VideoLayerMode::OneSpatialLayerPerStream,
            VideoLayerMode::OneSpatialLayerPerStreamIncompleteRtcpSr,
        ] {
            case(codec, mode, Scenario::Fragments { rid: true });
        }
    }
}
#[derive(Clone, Copy)]
enum Scenario<'a> {
    Fragments { rid: bool },
    LateTemporal,
    SwitchRid,
    Literal(&'a literal::Campaign),
    PerPcPlayout { retained_negotiation: bool },
}

#[test]
fn encrypted_one_filters_temporal_fragments_and_repairs_their_late_reserved_slot() {
    for codec in [Codec::Vp9, Codec::Av1] {
        for mode in [
            VideoLayerMode::OneSpatialLayerPerStream,
            VideoLayerMode::OneSpatialLayerPerStreamIncompleteRtcpSr,
        ] {
            case(codec, mode, Scenario::LateTemporal);
        }
    }
}

#[test]
fn encrypted_rid_uses_independent_descriptor_domains_and_sender_report_clocks() {
    for codec in [Codec::Vp9, Codec::Av1] {
        for mode in [
            VideoLayerMode::OneSpatialLayerPerStream,
            VideoLayerMode::OneSpatialLayerPerStreamIncompleteRtcpSr,
        ] {
            case(codec, mode, Scenario::SwitchRid);
        }
    }
}

#[test]
fn encrypted_one_original_packets_consume_independent_playout_policy() {
    for codec in [Codec::Vp9, Codec::Av1] {
        for retained_negotiation in [false, true] {
            case(
                codec,
                VideoLayerMode::OneSpatialLayerPerStream,
                Scenario::PerPcPlayout {
                    retained_negotiation,
                },
            );
        }
    }
}

#[derive(Clone, Copy)]
struct Phase {
    rid: Option<&'static str>,
    sn: u16,
    frame: u16,
    timestamp: u32,
    template: u8,
    first: bool,
    last: bool,
    attached: bool,
}
impl Phase {
    fn whole(rid: Option<&'static str>, sn: u16, frame: u16, timestamp: u32, template: u8) -> Self {
        Self {
            rid,
            sn,
            frame,
            timestamp,
            template,
            first: true,
            last: true,
            attached: template == 0,
        }
    }
    fn structure(self) -> u8 {
        if self.rid == Some("f") { 8 } else { 0 }
    }
    fn payload(self, codec: Codec) -> Vec<u8> {
        let label = self.rid.map_or(b'o', |rid| rid.as_bytes()[0]);
        let frame = self.frame as u8;
        if codec == Codec::Av1 {
            if !self.first {
                return vec![0x90, label, frame];
            }
            let fragment = if self.last { 0 } else { 0x40 };
            return if self.template == 0 {
                vec![0x28 | fragment, 1, 8, 0x18, 0, label, frame]
            } else {
                vec![0x10 | fragment, 0x30, 0x20, label, frame]
            };
        }
        let mut flags = 0xb0;
        if self.first {
            flags |= 8;
        }
        if self.last {
            flags |= 4;
        }
        if self.template != 0 {
            flags |= 0x40;
        }
        let mut payload = vec![flags, 0x81, frame, 0];
        if self.template == 0 {
            payload.push(0x80);
        } else {
            let diff = [0, 2, 1, 4, 1][usize::from(self.template)];
            payload.extend([diff << 1, 0x34]);
        }
        payload.extend([label, frame]);
        payload
    }
}

fn source_descriptor(phase: Phase) -> Vec<u8> {
    // Same two targets and five templates as the original Go ONE/RID oracle.
    let mut bits = Vec::new();
    let mut w = |value: u32, n: usize| {
        for i in (0..n).rev() {
            bits.push(((value >> i) & 1) as u8);
        }
    };
    w(u32::from(phase.first) * 2 + u32::from(phase.last), 2);
    w(u32::from(phase.structure() + phase.template), 6);
    w(u32::from(phase.frame), 16);
    if phase.attached {
        w(1, 1);
        w(0, 4);
        w(u32::from(phase.structure()), 6);
        w(1, 5);
        for _ in 0..3 {
            w(0, 2);
        }
        w(1, 2);
        w(3, 2);
        for [a, b] in [[2, 2], [3, 3], [3, 3], [3, 3], [0, 2]] {
            w(a, 2);
            w(b, 2);
        }
        w(0, 1);
        for diff in [2, 1, 4, 1] {
            w(1, 1);
            w(diff - 1, 4);
            w(0, 1);
        }
        w(0, 1);
        w(0, 1);
    }
    let mut raw = vec![0; (bits.len() + 7) / 8];
    for (i, bit) in bits.into_iter().enumerate() {
        raw[i / 8] |= bit << (7 - i % 8);
    }
    raw
}
fn case(codec: Codec, mode: VideoLayerMode, scenario: Scenario<'_>) {
    let campaign = match scenario {
        Scenario::Literal(campaign) => Some(campaign),
        _ => None,
    };
    let with_rids = matches!(
        scenario,
        Scenario::Fragments { rid: true } | Scenario::SwitchRid
    ) || campaign.is_some_and(|campaign| campaign.rid);
    let per_pc = match scenario {
        Scenario::PerPcPlayout {
            retained_negotiation,
        } => Some(retained_negotiation),
        _ => None,
    };
    let start = Instant::now();
    let mut engine = engine(start);
    // These scenarios send a handful of frames per layer, far below fixed Go's
    // default packet tracker (5 packets per 500 ms for enhancement layers),
    // which would declare the sparse layers stopped. They verify descriptor
    // domains and sender-report clocks, so configure the official
    // `video.stream_tracker_manager` packet tracker for that traffic instead.
    let sparse = Some(rtc_core::config::StreamTrackerSpec::Packet(
        rtc_core::config::PacketTrackerSpec {
            samples_required: 1,
            cycles_required: 1,
            cycle_duration: Duration::from_secs(2),
        },
    ));
    engine.cfg.video.stream_trackers.video = [sparse; 3];
    engine.cfg.rtc.congestion_control.enabled = false;
    if per_pc.is_some() {
        engine.cfg.playout_delay = Some((100, 200));
    }
    engine.cfg.enabled_codecs = vec![CodecSpec {
        mime: crate::codecs::mime_of(codec).into(),
        fmtp_line: String::new(),
    }];
    let packets = RoutedPacketQueue::default();
    engine.out = Box::new(packets.clone());
    let server_addr = "127.0.0.1:9691".parse().unwrap();
    let pub_addr = "127.0.0.1:9692".parse().unwrap();
    let pub_tid = transport(&mut engine, PcRole::Publisher, start);
    let t = engine.transports.get_mut(&pub_tid).unwrap();
    if campaign.is_some() {
        // Literal clock fixture feeds the fixed report observations directly,
        // just like the Go oracle. The sibling test retains genuine SRTCP.
        t.rtc.direct_api().set_rtcp_admission(Some(|_| false));
    }
    let candidate = t
        .rtc
        .add_local_candidate(Candidate::host(server_addr, "udp").unwrap())
        .unwrap()
        .clone();
    t.candidates.push(candidate);
    t.spec.primary = false;
    let sid = TrackSid::from("TR_dd_one_rid_wire");
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
                        rid: if with_rids {
                            ["q", "h", "f"][i].to_string()
                        } else {
                            String::new()
                        },
                        repair_ssrc: 0,
                    })
                    .collect(),
                codecs: vec![TrackCodec {
                    mime_type: crate::codecs::mime_of(codec).into(),
                    cid: "svc-source".into(),
                    sdp_cid: "svc-source".into(),
                    layer_mode: mode,
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
        let config = Rtc::builder()
            .set_rtp_mode(true)
            .set_crypto_provider(rtc_crypto::provider().into())
            .set_dtls_cert(rtc_crypto::dtls_cert().unwrap())
            .clear_codecs()
            .enable_av1(codec == Codec::Av1)
            .enable_vp9(codec == Codec::Vp9)
            .set_extension(
                13,
                str0m::rtp::Extension::with_serializer(crate::av1::URI, SourceDd),
            );
        let config = if per_pc.is_some() {
            config.set_extension(6, str0m::rtp::Extension::PlayoutDelay)
        } else {
            config
        };
        config.build(now)
    };
    let mut publisher = peer(start);
    publisher.add_local_candidate(Candidate::host(pub_addr, "udp").unwrap());
    let mid = published_mid(&offer(&mut engine, &mut publisher, pub_tid, start, |api| {
        let simulcast = with_rids.then(|| {
            let mut s = Simulcast::new();
            for rid in ["q", "h", "f"] {
                s.add_send_layer(SimulcastLayer::new(rid));
            }
            s
        });
        api.add_media(
            MediaKind::Video,
            Direction::SendOnly,
            None,
            Some("svc-source".into()),
            simulcast,
        );
    }));
    engine.bind_track(pub_tid, &mid.to_string(), sid.clone(), start);
    let mut subscribers = Vec::new();
    for index in 0..if per_pc.is_some() { 2 } else { 1 } {
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
                allow_pause: false,
                ice_preference: Default::default(),
                disable_playout_delay: index == 1 && per_pc == Some(false),
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
                    disabled: campaign.is_some(),
                    quality: VideoQuality::Low,
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
    if let Some(retained) = per_pc {
        for (index, (tid, _, _, _)) in subscribers.iter().enumerate() {
            let participant = ParticipantSid::from(format!("PA_sink_{index}"));
            let down = engine
                .downs
                .get(engine.down_by_key[&(participant, sid.clone())])
                .unwrap();
            let transport = engine.transports.get_mut(tid).unwrap();
            let media = transport
                .rtc
                .media(down.mid.expect("negotiated subscriber MID"))
                .unwrap();
            assert_eq!(
                media
                    .remote_extmap()
                    .iter()
                    .any(|(_, extension)| *extension == str0m::rtp::Extension::PlayoutDelay),
                index == 0 || retained,
                "actual subscriber SDP extension map"
            );
            // Control the producer in isolation: leave the previously
            // negotiated extmap in place, then apply the disabled policy
            // before the first packet. Runtime Firefox already omits SDP.
            if index == 1 && retained {
                transport.spec.disable_playout_delay = true;
            }
        }
    }
    let pt = publisher
        .codec_config()
        .params()
        .iter()
        .find(|p| p.spec().codec == codec)
        .unwrap()
        .pt();

    let rid = with_rids.then_some("q");
    let mut phases = Vec::new();
    if matches!(scenario, Scenario::LateTemporal) {
        // Real authenticated inputs train the existing receiver rate consumer.
        // The application then requests 1 fps using normal TrackSettings.
        for frame in 1..=40 {
            let template = if frame == 1 {
                0
            } else if frame % 2 == 0 {
                4
            } else {
                1
            };
            phases.push(Phase::whole(
                None,
                1000 + frame,
                frame,
                u32::from(frame) * 3000,
                template,
            ));
        }
    }
    if campaign.is_some() {
        // Establish the same T0 target through the production frame-rate
        // calculator while the sink is disabled. Start the measured source
        // frames after a DD wrap so their literal 16-bit IDs remain intact.
        let rids: &[Option<&str>] = if with_rids {
            &[Some("q"), Some("f")]
        } else {
            &[None]
        };
        for n in 0..40u16 {
            for &rid in rids {
                let template = if n == 0 {
                    0
                } else if n % 2 == 1 {
                    4
                } else {
                    1
                };
                phases.push(Phase::whole(
                    rid,
                    900 + n,
                    65480 + n,
                    1000 + u32::from(n) * 500,
                    template,
                ));
            }
        }
    }
    let test_start = phases.len();
    if let Some(campaign) = campaign {
        phases.extend(campaign.phases());
    } else if matches!(scenario, Scenario::SwitchRid) {
        phases.extend([
            Phase::whole(rid, 1000, 10, 30000, 0),
            Phase::whole(Some("f"), 1000, 10, 90000, 0),
            Phase::whole(rid, 1001, 12, 36000, 1),
            Phase::whole(Some("f"), 1001, 12, 96000, 1),
            Phase::whole(rid, 1002, 16, 39000, 3),
            // Source timestamps deliberately differ by much more than +1.
            Phase::whole(Some("f"), 1002, 13, 120000, 0),
            Phase::whole(rid, 1003, 17, 42000, 2),
            Phase::whole(Some("f"), 1003, 15, 126000, 1),
        ]);
    } else {
        let n = if matches!(scenario, Scenario::LateTemporal) {
            1041
        } else {
            1000
        };
        let frame = if matches!(scenario, Scenario::LateTemporal) {
            50
        } else {
            10
        };
        let ts = u32::from(frame) * 3000;
        phases.extend([
            Phase {
                last: false,
                ..Phase::whole(rid, n, frame, ts, 0)
            },
            Phase {
                first: false,
                attached: false,
                ..Phase::whole(rid, n + 1, frame, ts, 0)
            },
        ]);
        if matches!(scenario, Scenario::LateTemporal) {
            phases.extend([
                Phase {
                    last: false,
                    ..Phase::whole(rid, n + 2, frame + 1, ts + 3000, 4)
                },
                Phase {
                    last: false,
                    ..Phase::whole(rid, n + 4, frame + 2, ts + 6000, 1)
                },
                Phase {
                    first: false,
                    ..Phase::whole(rid, n + 5, frame + 2, ts + 6000, 1)
                },
                Phase {
                    first: false,
                    ..Phase::whole(rid, n + 3, frame + 1, ts + 3000, 4)
                },
            ]);
        }
    }
    let retired_phase = if campaign.is_some()
        || matches!(
            scenario,
            Scenario::Fragments { .. } | Scenario::PerPcPlayout { .. }
        ) {
        None
    } else {
        let previous = *phases.iter().max_by_key(|phase| phase.frame).unwrap();
        let source = if matches!(scenario, Scenario::SwitchRid) {
            Some("f")
        } else {
            None
        };
        let last = phases
            .iter()
            .filter(|phase| phase.rid == source)
            .max_by_key(|phase| phase.sn)
            .unwrap();
        let phase = phases.len();
        phases.push(Phase::whole(
            source,
            last.sn + 1,
            previous.frame + 1,
            last.timestamp + 3000,
            2,
        ));
        Some(phase)
    };
    let mut retired = false;
    let mut sent = 0usize;
    let mut next_at = 0;
    let mut settings_changed = false;
    let mut literal_enabled = false;
    let mut received_at_phase = Vec::new();
    let mut received = Vec::<str0m::rtp::RtpPacket>::new();
    let mut disabled_received = Vec::<str0m::rtp::RtpPacket>::new();
    let mut sr_clock = None;
    for tick in 0..9500u64 {
        let now = negotiated + Duration::from_millis(tick);
        publisher.handle_input(Input::Timeout(now)).unwrap();
        for (_, _, peer, _) in &mut subscribers {
            peer.handle_input(Input::Timeout(now)).unwrap();
        }
        let ready = publisher.is_connected()
            && subscribers
                .iter()
                .all(|(_, _, peer, _)| peer.is_connected());
        let switch = matches!(scenario, Scenario::SwitchRid);
        let late = matches!(scenario, Scenario::LateTemporal);
        if ready && campaign.is_some() && !literal_enabled && sent == test_start {
            let up = engine.ups.get(engine.up_by_mid[&(pub_tid, mid)]).unwrap();
            assert_eq!(up.svc_temporal_cap_for_spatial(0, 1), 0);
            if with_rids {
                assert_eq!(up.svc_temporal_cap_for_spatial(2, 1), 0);
                if mode == VideoLayerMode::OneSpatialLayerPerStream {
                    literal::inject_reports(&mut engine, pub_tid, mid, now);
                }
            }
            engine.handle_command(
                MediaCommand::UpdateTrackSettings {
                    subscriber: ParticipantSid::from("PA_sink_0"),
                    track_sid: sid.clone(),
                    settings: TrackSettings {
                        quality: VideoQuality::Low,
                        fps: 1,
                        ..Default::default()
                    },
                },
                now,
            );
            literal_enabled = true;
            next_at = tick + 150;
        }
        let literal_switch =
            campaign.is_some_and(|campaign| campaign.rid) && sent == test_start + 5;
        if ready
            && !settings_changed
            && ((switch && sent == 3) || (late && sent == test_start) || literal_switch)
        {
            engine.handle_command(
                MediaCommand::UpdateTrackSettings {
                    subscriber: ParticipantSid::from("PA_sink_0"),
                    track_sid: sid.clone(),
                    settings: TrackSettings {
                        quality: if switch || literal_switch {
                            VideoQuality::High
                        } else {
                            VideoQuality::Low
                        },
                        fps: u32::from(late || literal_switch),
                        ..Default::default()
                    },
                },
                now,
            );
            settings_changed = true;
            next_at = tick + 150;
        }
        if ready && !retired && retired_phase == Some(sent) && tick >= next_at {
            engine.handle_command(
                MediaCommand::Unsubscribe {
                    subscriber: ParticipantSid::from("PA_sink_0"),
                    track_sid: sid.clone(),
                },
                now,
            );
            assert!(engine.down_by_key.is_empty());
            assert!(
                engine.transports.contains_key(&pub_tid),
                "publisher remains live after unsubscribe"
            );
            retired = true;
            next_at = tick + 150;
        }
        let complete_switch =
            switch && sent == 5 && mode == VideoLayerMode::OneSpatialLayerPerStream;
        let mut clock_ready = !complete_switch;
        if complete_switch {
            let up = engine.ups.get(engine.up_by_mid[&(pub_tid, mid)]).unwrap();
            if let (Some(q), Some(f)) = (up.one_dd.get_report(0), up.one_dd.get_report(2)) {
                assert_ne!(q.ssrc, f.ssrc, "real per-RID authenticated Sender Reports");
                // Predict the exact switch from the native reports actually
                // received immediately before this packet, independently of
                // the engine's output state.
                let signed = q.ntp_time.duration_since(f.ntp_time).map_or_else(
                    |err| -(err.duration().as_nanos() as i128),
                    |span| span.as_nanos() as i128,
                );
                let offset = (q.rtp_time.numer() as u32).wrapping_sub(
                    (f.rtp_time.numer() as u32)
                        .wrapping_add((signed * 90000 / 1_000_000_000) as u32),
                );
                sr_clock = Some(phases[sent].timestamp.wrapping_add(offset));
                clock_ready = true;
            }
        }
        if ready && clock_ready && tick >= next_at && sent < phases.len() {
            let phase = phases[sent];
            if late && sent == test_start {
                let up = engine.ups.get(engine.up_by_mid[&(pub_tid, mid)]).unwrap();
                assert_eq!(
                    up.svc_temporal_cap_for_spatial(0, 1),
                    0,
                    "real authenticated DD input must establish T0 through the production FPS calculator"
                );
            }
            received_at_phase.push(received.len());
            let mut values = str0m::rtp::ExtensionValues::default();
            values
                .user_values
                .set(crate::av1::RawDescriptor(source_descriptor(phase).into()));
            publisher
                .direct_api()
                .stream_tx_by_mid(mid, phase.rid.map(Rid::from))
                .expect("actual negotiated source encoding exists")
                .write_rtp(
                    RtpWrite::new(
                        pt,
                        u64::from(phase.sn).into(),
                        phase.timestamp,
                        now,
                        phase.payload(codec),
                    )
                    .marker(phase.last)
                    .ext_vals(values),
                );
            let training = (late || campaign.is_some()) && sent < test_start;
            next_at = tick
                + if training {
                    6
                } else if switch || sent != test_start {
                    110
                } else {
                    300
                };
            sent += 1;
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
                    Output::Event(Event::RtpPacket(p)) => {
                        if per_pc.is_some() && addr.port() == 9694 {
                            disabled_received.push(p);
                        } else {
                            received.push(p);
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
        if sent == phases.len() && tick >= next_at + 100 {
            break;
        }
    }
    assert_eq!(
        sent,
        phases.len(),
        "actual ICE/DTLS, real source encodings and Sender Reports"
    );
    received_at_phase.push(received.len());
    let rows = |phase: usize| &received[received_at_phase[phase]..received_at_phase[phase + 1]];
    if let Some(index) = retired_phase {
        assert!(
            retired && rows(index).is_empty(),
            "the closed subscriber must get no output from the still-live publisher"
        );
    }
    let assert_packet = |index: usize, frame: u16| {
        let input = phases[index];
        let [packet] = rows(index) else {
            panic!(
                "expected one actual SRTP packet for phase {index}, got {}",
                rows(index).len()
            )
        };
        assert!(!packet.header.has_padding);
        assert_eq!(*packet.payload, input.payload(codec));
        assert_eq!(packet.header.marker, input.last);
        let dd = packet
            .header
            .ext_vals
            .user_values
            .get::<crate::av1::RawDescriptor>()
            .expect("per-original-packet DD must survive");
        assert_eq!(u16::from_be_bytes([dd.0[1], dd.0[2]]), frame);
        assert_eq!(
            dd.0[0] >> 6,
            u8::from(input.first) * 2 + u8::from(input.last)
        );
        packet
    };
    match scenario {
        Scenario::Literal(campaign) => {
            campaign.assert_replay(
                &phases[test_start..],
                &received,
                &received_at_phase[test_start..],
            );
        }
        Scenario::PerPcPlayout { .. } => {
            assert_packet(0, 10);
            assert_packet(1, 10);
            assert_eq!(received.len(), 2);
            assert_eq!(
                disabled_received.len(),
                2,
                "disabled PC still receives the complete media"
            );
            for (positive, negative) in received.iter().zip(&disabled_received) {
                assert_eq!(positive.payload, negative.payload);
                assert_eq!(positive.header.marker, negative.header.marker);
                let enabled_exts = &positive.header.ext_vals;
                assert_eq!(
                    enabled_exts.play_delay_min,
                    Some(str0m::media::MediaTime::from_millis(100))
                );
                assert_eq!(
                    enabled_exts.play_delay_max,
                    Some(str0m::media::MediaTime::from_millis(200))
                );
                let disabled_exts = &negative.header.ext_vals;
                assert_eq!(disabled_exts.play_delay_min, None);
                assert_eq!(disabled_exts.play_delay_max, None);
            }
        }
        Scenario::Fragments { .. } => {
            assert!(
                !received.is_empty(),
                "the complete source frame must reach the encrypted peer"
            );
            assert_eq!(
                received_at_phase[1], 1,
                "fixed Go forwards the authenticated first original fragment before any final fragment"
            );
            assert_eq!(received.len(), 2);
            assert_packet(0, 10);
            assert_packet(1, 10);
        }
        Scenario::LateTemporal => {
            let n = test_start;
            // The keyframe's final marker commits T0 for subsequent frames.
            let first = assert_packet(n, 50);
            let base = first.header.sequence_number;
            for (phase, frame, offset) in [(n + 1, 50, 1), (n + 3, 52, 3), (n + 4, 52, 4)] {
                assert_eq!(
                    assert_packet(phase, frame)
                        .header
                        .sequence_number
                        .wrapping_sub(base),
                    offset
                );
            }
            assert!(
                rows(n + 2).is_empty(),
                "contiguous T1 fragment is compacted"
            );
            let [padding] = rows(n + 5) else {
                panic!("late filtered packet needs exactly its reserved padding slot");
            };
            assert!(padding.header.has_padding && padding.payload.is_empty());
            assert_eq!(padding.header.sequence_number.wrapping_sub(base), 2);
            let mut parser = crate::av1::PacketReceiver::default();
            for (index, mask) in [(n, 3), (n + 1, 3), (n + 3, 1), (n + 4, 1)] {
                let packet = &rows(index)[0];
                let dd = parser
                    .parse(
                        &packet.header.ext_vals,
                        *packet.seq_no,
                        packet.timestamp,
                        false,
                    )
                    .expect("actual output DD remains parseable across the temporal filter");
                assert_eq!(dd.active, mask);
            }
        }
        Scenario::SwitchRid => {
            let first = assert_packet(0, 10);
            for phase in [1, 3, 6] {
                assert!(
                    rows(phase).is_empty(),
                    "wrong RID, target delta, or retired source cannot forward at phase {phase}"
                );
            }
            let last_low = assert_packet(4, 16);
            let switched = assert_packet(5, 17);
            let next = assert_packet(7, 19);
            let expect_ts = if let Some(mapped) = sr_clock {
                assert!(
                    mapped.wrapping_sub(last_low.header.timestamp) as i32 > 1,
                    "fixture must distinguish SR mapping from nominal +1"
                );
                mapped
            } else {
                last_low.header.timestamp.wrapping_add(1)
            };
            assert_eq!(switched.header.timestamp, expect_ts);
            assert_eq!(
                next.header
                    .timestamp
                    .wrapping_sub(switched.header.timestamp),
                6000
            );
            for (index, packet) in [first, assert_packet(2, 12), last_low, switched, next]
                .into_iter()
                .enumerate()
            {
                assert_eq!(
                    packet
                        .header
                        .sequence_number
                        .wrapping_sub(first.header.sequence_number),
                    index as u16
                );
                assert_eq!(packet.header.ssrc, first.header.ssrc);
            }
        }
    }
    engine.handle_command(
        MediaCommand::UnpublishTrack {
            track_sid: sid.clone(),
        },
        negotiated + Duration::from_secs(12),
    );
    for (tid, _, _, _) in subscribers {
        engine.handle_command(
            MediaCommand::CloseTransport(tid),
            negotiated + Duration::from_secs(12),
        );
    }
    engine.handle_command(
        MediaCommand::CloseTransport(pub_tid),
        negotiated + Duration::from_secs(12),
    );
    assert!(engine.transports.is_empty());
    assert!(engine.up_by_mid.is_empty());
    assert!(engine.down_by_key.is_empty());
}

pub(super) fn two_temporal_vp9_frame(frame: u16) -> (Vec<u8>, Vec<u8>) {
    let template = if frame == 1 {
        0
    } else if frame % 2 == 0 {
        4
    } else {
        1
    };
    let phase = Phase::whole(None, 1000 + frame, frame, u32::from(frame) * 3000, template);
    (source_descriptor(phase), phase.payload(Codec::Vp9))
}
