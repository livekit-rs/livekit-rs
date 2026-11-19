use super::f93_closed_write::{F93Drop, f93_params, f93_policy};
use super::*;
use crate::net::Datagram;
use rtc_core::events::EngineEvent;
use rtc_core::ids::{ProjectId, RoomKey};
use rtc_core::media_api::{MediaCommand, PublicationRegistration};
use rtc_core::model::{TrackInfo, TrackKind, TrackSource, VideoLayer, VideoQuality};
use rtc_core::room::{Effect, Room};
use rtc_core::session::{AddTrack, SessionRequest, SignalEvent, SimulcastCodecRequest};
use str0m::format::Codec;
use str0m::media::{Simulcast, SimulcastLayer};
use str0m::net::{Protocol, Receive};
use str0m::rtp::RtpWrite;
use str0m::{Candidate, Input, Output};

fn layer_json(layer: &VideoLayer) -> serde_json::Value {
    let VideoLayer {
        quality,
        width,
        height,
        bitrate,
        ssrc,
        spatial_layer,
        rid,
        repair_ssrc,
    } = layer;
    serde_json::json!({"quality":quality.as_i32(),"width":width,"height":height,"bitrate":bitrate,
        "ssrc":ssrc,"spatial_layer":spatial_layer,"rid":rid,"repair_ssrc":repair_ssrc})
}

fn track_json(track: &TrackInfo) -> serde_json::Value {
    let TrackInfo {
        sid,
        kind,
        name,
        muted,
        width,
        height,
        simulcast,
        source,
        layers,
        mime_type,
        mid,
        codecs,
        disable_red,
        disable_dtx,
        stereo,
        encryption,
        stream,
        version,
        audio_features,
        packet_trailer_features,
        backup_codec_policy,
    } = track;
    serde_json::json!({
        "sid":sid.as_str(),"type":kind.as_i32(),"name":name,"muted":muted,"width":width,
        "height":height,"simulcast":simulcast,"source":source.as_i32(),
        "layers":layers.iter().map(layer_json).collect::<Vec<_>>(),"mime_type":mime_type,"mid":mid,
        "codecs":codecs.iter().map(|codec| {
            let rtc_core::model::TrackCodec { mime_type, mid, cid, sdp_cid, layers, layer_mode } = codec;
            serde_json::json!({"mime_type":mime_type,"mid":mid,"cid":cid,"sdp_cid":sdp_cid,
                "layers":layers.iter().map(layer_json).collect::<Vec<_>>(),"video_layer_mode":layer_mode.as_i32()})
        }).collect::<Vec<_>>(),
        "disable_red":disable_red,"disable_dtx":disable_dtx,"stereo":stereo,"encryption":encryption.as_i32(),
        "stream":stream,"version":{"unix_micro":version.unix_micro,"ticks":version.ticks},
        "audio_features":audio_features.iter().map(|f| f.as_i32()).collect::<Vec<_>>(),
        "packet_trailer_features":packet_trailer_features,"backup_codec_policy":backup_codec_policy.as_i32(),
    })
}

#[derive(Default)]
struct Observed {
    ack: Vec<TrackInfo>,
    published: Vec<TrackInfo>,
    registrations: usize,
    starts: usize,
    pending_media: usize,
    hold_bind: bool,
    held_bind: Vec<MediaCommand>,
}

fn effects(room: &mut Room, engine: &mut MediaEngine, observed: &mut Observed, now: Instant) {
    for effect in room.take_effects() {
        match effect {
            Effect::Media(command @ MediaCommand::BindTrack { .. }) if observed.hold_bind => {
                observed.held_bind.push(command);
            }
            Effect::Media(command) => engine.handle_command(command, now),
            Effect::Event(EngineEvent::TrackPublished { track, .. }) => {
                observed.published.push(track);
            }
            Effect::Signal {
                event: SignalEvent::TrackPublished { track, .. },
                ..
            } => {
                observed.ack.push(track);
            }
            _ => {}
        }
    }
}

fn events(
    room: &mut Room,
    engine: &mut MediaEngine,
    observed: &mut Observed,
    pending: Vec<MediaEvent>,
    now: Instant,
) {
    for event in pending {
        match &event {
            MediaEvent::RemoteMediaAdded(media) => {
                assert_eq!(media.registration, PublicationRegistration::Pending);
                assert_eq!(media.rids, ["q", "f"]);
                assert_eq!(media.msid_track_id, "c65-video");
                observed.pending_media += 1;
            }
            MediaEvent::PublicationRtpRegistered {
                mime_type,
                payload_type,
                ..
            } => {
                assert_eq!(mime_type, "video/VP8");
                assert_eq!(*payload_type, 96);
                observed.registrations += 1;
            }
            MediaEvent::TrackMediaStarted { .. } => observed.starts += 1,
            _ => {}
        }
        room.handle_media_event(event, now);
    }
    effects(room, engine, observed, now);
}

fn pump(room: &mut Room, engine: &mut MediaEngine, observed: &mut Observed, now: Instant) {
    for _ in 0..32 {
        let mut pending = vec![];
        engine.drain_events(&mut pending);
        if pending.is_empty() {
            return;
        }
        events(room, engine, observed, pending, now);
    }
    panic!("Room/native event loop must become quiescent");
}

fn check_first_rid_packet(duplicate: bool, marker: bool, late_bind: bool) {
    let start = Instant::now();
    let mut policy = f93_policy();
    policy.use_ice_lite = true;
    let mut room = Room::new(
        RoomKey::new(ProjectId::local(), "r1"),
        std::sync::Arc::new(policy),
        std::sync::Arc::new(F93Drop),
        &Default::default(),
        start,
    );
    room.take_effects();
    let mut params = f93_params("c65", 6, false);
    params.grants.video.can_publish = Some(true);
    params.grants.video.can_subscribe = Some(false);
    params.grants.video.can_publish_data = Some(false);
    params.auto_subscribe = false;
    let joined = room.join(params, start).unwrap();
    let mut engine = engine(start);
    engine.cfg.enabled_codecs.push(CodecSpec {
        mime: "video/VP8".into(),
        fmtp_line: String::new(),
    });
    let packets = RoutedPacketQueue::default();
    engine.out = Box::new(packets.clone());
    let mut publisher = None;
    for effect in room.take_effects() {
        match effect {
            Effect::Media(MediaCommand::CreateTransport(spec)) => {
                if spec.role == PcRole::Publisher {
                    publisher = Some(spec.id);
                }
                engine.create_transport(spec, IceCredentials::random(), vec![], start);
            }
            Effect::Media(command) => engine.handle_command(command, start),
            _ => {}
        }
    }
    let tid = publisher.unwrap();
    let rows = if duplicate {
        vec![
            (0, 320, 180, 130_000, 7, "c64-low-first"),
            (0, 480, 270, 230_000, 8, "c64-low-second"),
            (2, 1280, 720, 930_000, 9, "c64-high"),
        ]
    } else {
        vec![
            (0, 320, 180, 120_000, 7, "c64-low"),
            (2, 1280, 720, 910_000, 9, "c64-high"),
        ]
    };
    let layers: Vec<_> = rows
        .into_iter()
        .map(
            |(q, width, height, bitrate, spatial_layer, rid)| VideoLayer {
                quality: VideoQuality::from_i32(q),
                width,
                height,
                bitrate,
                spatial_layer,
                rid: rid.into(),
                ssrc: 0,
                repair_ssrc: 0,
            },
        )
        .collect();
    let request = AddTrack {
        token: 1,
        cid: "c65-video".into(),
        name: "C65 video".into(),
        kind: TrackKind::Video,
        width: 1280,
        height: 720,
        muted: false,
        disable_dtx: false,
        source: TrackSource::Camera,
        layers: layers.clone(),
        simulcast_codecs: vec![SimulcastCodecRequest {
            codec: "vp8".into(),
            cid: "c65-video".into(),
            layers: layers.clone(),
            layer_mode: Default::default(),
        }],
        sid: None,
        stereo: false,
        disable_red: false,
        encryption: Default::default(),
        stream: "c65-stream".into(),
        backup_codec_policy: Default::default(),
        audio_features: vec![],
        packet_trailer_features: vec![],
    };
    let mut observed = Observed {
        hold_bind: late_bind,
        ..Default::default()
    };
    room.handle_request(
        &joined.sid,
        joined.generation,
        SessionRequest::AddTrack(request),
        start,
    );
    effects(&mut room, &mut engine, &mut observed, start);
    assert_eq!(observed.ack.len(), 1);
    assert_eq!(room.published_track_count(), 0);
    assert!(observed.published.is_empty());
    let server_addr = "127.0.0.1:9491".parse().unwrap();
    let peer_addr = "127.0.0.1:9492".parse().unwrap();
    let transport = engine.transports.get_mut(&tid).unwrap();
    let candidate = transport
        .rtc
        .add_local_candidate(Candidate::host(server_addr, "udp").unwrap())
        .unwrap()
        .clone();
    transport.candidates.push(candidate);
    let mut peer = client(start);
    peer.add_local_candidate(Candidate::host(peer_addr, "udp").unwrap());
    let mut api = peer.sdp_api();
    let mid = api.add_media(
        MediaKind::Video,
        Direction::SendOnly,
        Some("c65-stream".into()),
        Some("c65-video".into()),
        Some(Simulcast {
            send: vec![SimulcastLayer::new("q"), SimulcastLayer::new("f")],
            recv: vec![],
        }),
    );
    let (offer, pending) = api.apply().unwrap();
    let offer_sdp = offer.to_sdp_string();
    engine.on_remote_description(tid, SdpType::Offer, offer_sdp.clone(), 1, start);
    let mut initial = vec![];
    engine.drain_events(&mut initial);
    let answer_sdp = initial
        .iter()
        .find_map(|event| match event {
            MediaEvent::LocalDescription {
                kind: SdpType::Answer,
                sdp,
                ..
            } => Some(sdp.clone()),
            _ => None,
        })
        .expect("native server answers real offer");
    peer.sdp_api()
        .accept_answer(pending, SdpAnswer::from_sdp_string(&answer_sdp).unwrap())
        .unwrap();
    assert_eq!(published_mid(&initial), mid);
    events(&mut room, &mut engine, &mut observed, initial, start);
    peer.direct_api()
        .reset_stream_tx(mid, Some("q".into()), 424242.into(), None)
        .expect("sender SSRC must actually change before RTP");
    let mut connected_at = None;
    let mut sent = false;
    let mut rejected_bad_auth = false;
    for tick in 0..2000u64 {
        let now = start + Duration::from_millis(tick);
        peer.handle_input(Input::Timeout(now)).unwrap();
        if peer.is_connected() {
            let connected = *connected_at.get_or_insert(tick);
            if !sent && tick >= connected + 100 {
                assert_eq!(
                    room.published_track_count(),
                    0,
                    "RID publication must wait for actual RTP"
                );
                assert!(observed.published.is_empty());
                let pt = peer
                    .writer(mid)
                    .unwrap()
                    .payload_params()
                    .find(|p| p.spec().codec == Codec::Vp8)
                    .unwrap()
                    .pt();
                assert_eq!(*pt, 96);
                peer.direct_api()
                    .stream_tx_by_mid(mid, Some("q".into()))
                    .unwrap()
                    .write_rtp(
                        RtpWrite::new(pt, 1000.into(), 90_000, now, vec![0x10, 0x01, 0, 0, 0])
                            .marker(marker),
                    );
                sent = true;
            }
        }
        loop {
            match peer.poll_output().unwrap() {
                Output::Transmit(packet) => {
                    if sent
                        && !rejected_bad_auth
                        && packet.contents.len() > 12
                        && packet.contents[0] >> 6 == 2
                        && packet.contents[1] & 0x7f == 96
                    {
                        let mut invalid = packet.contents.to_vec();
                        *invalid.last_mut().unwrap() ^= 1;
                        engine.handle_datagram(Datagram {
                            transport: tid,
                            proto: Proto::Udp,
                            source: peer_addr,
                            destination: server_addr,
                            data: &invalid,
                            received_at: now,
                        });
                        pump(&mut room, &mut engine, &mut observed, now);
                        assert!(engine.transports[&tid].publication_rtp.is_empty());
                        assert!(observed.published.is_empty());
                        assert_eq!(room.published_track_count(), 0);
                        rejected_bad_auth = true;
                    }
                    engine.handle_datagram(Datagram {
                        transport: tid,
                        proto: Proto::Udp,
                        source: peer_addr,
                        destination: server_addr,
                        data: &packet.contents,
                        received_at: now,
                    });
                }
                Output::Timeout(_) => break,
                _ => {}
            }
        }
        engine.handle_timeout(now);
        engine.run(now);
        pump(&mut room, &mut engine, &mut observed, now);
        if observed.hold_bind && !engine.transports[&tid].publication_rtp.is_empty() {
            assert_eq!(observed.held_bind.len(), 1);
            assert_eq!(observed.registrations, 0);
            assert!(observed.published.is_empty());
            observed.hold_bind = false;
            for command in std::mem::take(&mut observed.held_bind) {
                engine.handle_command(command, now);
            }
            pump(&mut room, &mut engine, &mut observed, now);
        }
        for (destination, bytes) in packets.0.lock().unwrap().drain(..).collect::<Vec<_>>() {
            assert_eq!(destination, peer_addr);
            peer.handle_input(Input::Receive(
                now,
                Receive::new(Protocol::Udp, server_addr, peer_addr, &bytes).unwrap(),
            ))
            .unwrap();
        }
    }
    let authenticated = engine.transports[&tid].publication_rtp.len();
    let receiver = engine
        .transports
        .get_mut(&tid)
        .unwrap()
        .rtc
        .direct_api()
        .stream_rx_by_mid(mid, Some("q".into()))
        .map(|stream| {
            (
                *stream.ssrc(),
                stream.rtx().map(|ssrc| *ssrc).unwrap_or(0),
                stream.media_packets_received(),
            )
        });
    let visible = room.published_track_count();
    let media_started = engine
        .up_by_mid
        .get(&(tid, mid))
        .and_then(|ui| engine.ups.get(*ui))
        .is_some_and(|up| up.started);
    let settled = room.get_participant(&"c65".into()).unwrap().tracks;
    eprintln!(
        "C65_RID_NATIVE {}",
        serde_json::json!({
            "duplicate": duplicate, "marker": marker, "late_bind": late_bind, "mid": mid.to_string(), "sent": sent,
            "offer_sdp": offer_sdp, "answer_sdp": answer_sdp, "receiver": receiver,
            "rejected_bad_auth": rejected_bad_auth,
            "rtp": {"pt":96,"ssrc":424242,"sequence_number":1000,"timestamp":90000,"payload_hex":"1001000000","marker":marker,"rid":"q"},
            "authenticated": authenticated, "registrations": observed.registrations,
            "media_started_events": observed.starts, "media_started": media_started, "visible": visible,
            "ack": observed.ack.iter().map(track_json).collect::<Vec<_>>(),
            "published": observed.published.iter().map(track_json).collect::<Vec<_>>(),
            "settled": settled.iter().map(track_json).collect::<Vec<_>>(),
        })
    );
    assert!(
        sent && connected_at.is_some(),
        "real peers must connect and send SRTP"
    );
    assert!(
        rejected_bad_auth,
        "tampered SRTP must not release publication"
    );
    assert_eq!(observed.pending_media, 1);
    assert_eq!(
        receiver,
        Some((424242, 0, 1)),
        "exactly one native primary packet on RID q"
    );
    assert_eq!(observed.ack[0].layers, layers);
    assert_eq!(observed.ack[0].codecs[0].layers, layers);
    assert_eq!(
        authenticated, 1,
        "actual primary SRTP must authenticate and resolve MID"
    );
    assert_eq!(observed.registrations, 1);
    assert_eq!(
        media_started,
        marker && !late_bind,
        "publication must not activate frame forwarding"
    );
    assert_eq!(
        visible, 1,
        "RID publication must not require a complete VP8 frame"
    );
    assert_eq!(
        observed.published.len(),
        1,
        "exactly one actual Room publication"
    );
    let track = &observed.published[0];
    let mut expected = layers;
    for layer in &mut expected {
        layer.spatial_layer = if layer.quality == VideoQuality::Low {
            0
        } else {
            1
        };
        layer.rid = if layer.quality == VideoQuality::Low {
            "q"
        } else {
            "f"
        }
        .into();
    }
    expected[0].ssrc = 424242;
    assert_eq!(track.layers, expected);
    assert_eq!(track.codecs.len(), 1);
    assert_eq!(track.codecs[0].layers, expected);
    assert_eq!(track.codecs[0].mid, mid.to_string());
    assert_eq!(track.mime_type, "video/VP8");
    assert!(track.simulcast);
    assert_eq!(settled.len(), 1);
    assert_eq!(settled[0].layers, expected);
    assert_eq!(settled[0].codecs[0].layers, expected);
    peer.direct_api()
        .reset_stream_tx(mid, Some("f".into()), 434343.into(), None)
        .expect("second RID uses its own actual SSRC");
    peer.direct_api()
        .stream_tx_by_mid(mid, Some("f".into()))
        .unwrap()
        .write_rtp(
            RtpWrite::new(
                96.into(),
                1001.into(),
                180_000,
                start + Duration::from_secs(2),
                vec![0x10, 0x01, 0, 0, 0],
            )
            .marker(true),
        );
    for tick in 2000..2300 {
        let now = start + Duration::from_millis(tick);
        peer.handle_input(Input::Timeout(now)).unwrap();
        loop {
            match peer.poll_output().unwrap() {
                Output::Transmit(packet) => engine.handle_datagram(Datagram {
                    transport: tid,
                    proto: Proto::Udp,
                    source: peer_addr,
                    destination: server_addr,
                    data: &packet.contents,
                    received_at: now,
                }),
                Output::Timeout(_) => break,
                _ => {}
            }
        }
        engine.handle_timeout(now);
        engine.run(now);
        pump(&mut room, &mut engine, &mut observed, now);
        for (destination, bytes) in packets.0.lock().unwrap().drain(..).collect::<Vec<_>>() {
            assert_eq!(destination, peer_addr);
            peer.handle_input(Input::Receive(
                now,
                Receive::new(Protocol::Udp, server_addr, peer_addr, &bytes).unwrap(),
            ))
            .unwrap();
        }
    }
    let before_rebind = room.get_participant(&"c65".into()).unwrap().tracks;
    expected.last_mut().unwrap().ssrc = 434343;
    assert_eq!(before_rebind[0].layers, expected);
    assert_eq!(before_rebind[0].codecs[0].layers, expected);
    let rebind_at = start + Duration::from_millis(2300);
    engine.bind_track(
        tid,
        &mid.to_string(),
        observed.ack[0].sid.clone(),
        rebind_at,
    );
    pump(&mut room, &mut engine, &mut observed, rebind_at);
    let after_rebind = room.get_participant(&"c65".into()).unwrap().tracks;
    eprintln!(
        "C65_RID_REBIND {}",
        serde_json::json!({
            "duplicate":duplicate,"marker":marker,"late_bind":late_bind,
            "before":before_rebind.iter().map(track_json).collect::<Vec<_>>(),
            "after":after_rebind.iter().map(track_json).collect::<Vec<_>>(),
        })
    );
    let transports: Vec<_> = engine.transports.keys().copied().collect();
    for transport in transports {
        engine.handle_command(MediaCommand::CloseTransport(transport), rebind_at);
    }
    assert_eq!(
        after_rebind, before_rebind,
        "same receiver rebind must not replay stale metadata"
    );
    assert_eq!(observed.published.len(), 1);
}

#[test]
fn c65_rid_sparse_first_fragment_publishes() {
    check_first_rid_packet(false, false, false);
}

#[test]
fn c65_rid_sparse_complete_frame_publishes() {
    check_first_rid_packet(false, true, false);
}

#[test]
fn c65_rid_duplicate_first_fragment_publishes() {
    check_first_rid_packet(true, false, false);
}

#[test]
fn c65_rid_duplicate_complete_frame_publishes() {
    check_first_rid_packet(true, true, false);
}

#[test]
fn c65_rid_sparse_first_fragment_before_bind_publishes() {
    check_first_rid_packet(false, false, true);
}

#[test]
fn c65_rid_sparse_complete_frame_before_bind_publishes() {
    check_first_rid_packet(false, true, true);
}

#[test]
fn c65_rid_duplicate_first_fragment_before_bind_publishes() {
    check_first_rid_packet(true, false, true);
}

#[test]
fn c65_rid_duplicate_complete_frame_before_bind_publishes() {
    check_first_rid_packet(true, true, true);
}
