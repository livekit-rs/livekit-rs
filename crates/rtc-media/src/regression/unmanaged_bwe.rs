//! Config → real engine allocation/stream → actual ICE/DTLS/SRTP RTP consumer.
//! Declared layer costs isolate the exact unmanaged accounting decision, not a
//! claimed measurement of end-to-end network capacity or decoded video pixels.
use super::*;
use crate::net::Datagram;
use rtc_core::media_api::DownTrackSpec;
use rtc_core::model::{TrackSettings, VideoLayer, VideoQuality};
use str0m::net::{Protocol, Receive};
use str0m::{Event, Input, Output};

fn fixture(
    start: Instant,
    native: bool,
    disable: bool,
) -> (MediaEngine, TransportId, usize, Rtc, PacketQueue) {
    use rtc_core::media_api::DownTrackSpec;
    use rtc_core::model::{TrackSettings, VideoLayer, VideoQuality};
    let mut engine = engine(start);
    engine.cfg.rtc.congestion_control.use_send_side_bwe = native;
    engine
        .cfg
        .rtc
        .congestion_control
        .use_send_side_bwe_interceptor = false;
    engine
        .cfg
        .rtc
        .congestion_control
        .stream_allocator
        .disable_estimation_unmanaged_tracks = disable;
    engine.cfg.enabled_codecs.push(CodecSpec {
        mime: "video/vp8".into(),
        fmtp_line: String::new(),
    });
    let tid = transport(&mut engine, PcRole::Single, start);
    let packets = PacketQueue::default();
    engine.out = Box::new(packets.clone());
    let candidate = engine
        .transports
        .get_mut(&tid)
        .unwrap()
        .rtc
        .add_local_candidate(
            str0m::Candidate::host("127.0.0.1:9401".parse().unwrap(), "udp").unwrap(),
        )
        .unwrap()
        .clone();
    engine
        .transports
        .get_mut(&tid)
        .unwrap()
        .candidates
        .push(candidate);
    let mut peer = Rtc::builder()
        .set_crypto_provider(rtc_crypto::provider().into())
        .set_dtls_cert(rtc_crypto::dtls_cert().unwrap())
        .set_rtp_mode(true)
        .build(start);
    peer.add_local_candidate(
        str0m::Candidate::host("127.0.0.1:9402".parse().unwrap(), "udp").unwrap(),
    );
    let mid = published_mid(&offer(&mut engine, &mut peer, tid, start, |change| {
        change.add_media(MediaKind::Video, Direction::SendOnly, None, None, None);
    }));
    let sid = TrackSid::from("TR_peer_stats_allocation");
    engine.bind_track(tid, &mid.to_string(), sid.clone(), start);
    let ui = engine.up_by_sid[&sid];
    engine.ups.get_mut(ui).unwrap().spec.source = rtc_core::model::TrackSource::ScreenShare;
    engine.ups.get_mut(ui).unwrap().layers = vec![VideoLayer {
        quality: VideoQuality::High,
        width: 960,
        height: 540,
        bitrate: 500_000,
        ssrc: 1,
        spatial_layer: 0,
        rid: String::new(),
        repair_ssrc: 0,
    }];
    engine.subscribe(
        DownTrackSpec {
            subscriber: "PA_test".into(),
            publisher: "PA_test".into(),
            track_sid: sid.clone(),
            stream_id: "PA_test|TR_peer_stats_allocation".into(),
            settings: TrackSettings::default(),
            adaptive_stream: false,
        },
        start,
    );
    offer(&mut engine, &mut peer, tid, start, |change| {
        change.add_media(MediaKind::Video, Direction::RecvOnly, None, None, None);
    });
    let di = engine.down_by_key[&("PA_test".into(), sid)];
    assert!(
        engine.downs.get(di).unwrap().negotiated,
        "real SDP negotiated downtrack"
    );
    engine.transports.get_mut(&tid).unwrap().connected = true;
    engine.allocate_transport(tid, start);
    assert_eq!(engine.transports[&tid].allocated_bps, 500_000);
    assert!(!engine.downs.get(di).unwrap().bw_paused);
    (engine, tid, di, peer, packets)
}

fn add_managed_camera(
    engine: &mut MediaEngine,
    peer: &mut Rtc,
    tid: TransportId,
    now: Instant,
) -> usize {
    let mid = published_mid(&offer(engine, peer, tid, now, |api| {
        api.add_media(MediaKind::Video, Direction::SendOnly, None, None, None);
    }));
    let sid = TrackSid::from("TR_managed_bwe_camera");
    engine.bind_track(tid, &mid.to_string(), sid.clone(), now);
    let ui = engine.up_by_sid[&sid];
    let up = engine.ups.get_mut(ui).unwrap();
    up.spec.source = rtc_core::model::TrackSource::Camera;
    up.layers = vec![VideoLayer {
        quality: VideoQuality::High,
        width: 960,
        height: 540,
        bitrate: 200_000,
        ssrc: 1,
        spatial_layer: 0,
        rid: String::new(),
        repair_ssrc: 0,
    }];
    engine.subscribe(
        DownTrackSpec {
            subscriber: "PA_test".into(),
            publisher: "PA_test".into(),
            track_sid: sid.clone(),
            stream_id: "PA_test|TR_managed_bwe_camera".into(),
            settings: TrackSettings::default(),
            adaptive_stream: false,
        },
        now,
    );
    offer(engine, peer, tid, now, |api| {
        api.add_media(MediaKind::Video, Direction::RecvOnly, None, None, None);
    });
    let index = engine.down_by_key[&("PA_test".into(), sid)];
    assert!(engine.downs.get(index).unwrap().negotiated);
    index
}

fn controlled_consumer(native: bool) {
    for disable in [false, true] {
        let start = Instant::now();
        let (mut engine, tid, share, mut peer, packets) = fixture(start, native, disable);
        let camera = add_managed_camera(&mut engine, &mut peer, tid, start);
        engine.transports.get_mut(&tid).unwrap().bw_override_bps = 500_000;
        engine.allocate_transport(tid, start);
        assert!(
            !engine.downs.get(share).unwrap().bw_paused,
            "unmanaged optimal share is never withheld"
        );
        assert_eq!(
            engine.downs.get(camera).unwrap().bw_paused,
            !disable,
            "actual managed downtrack must receive restored capacity only when configured"
        );
        assert_eq!(
            engine.transports[&tid].allocated_bps,
            if disable { 700_000 } else { 500_000 },
            "statistics retain all actual configured output allocation"
        );
        let share_mid = engine.downs.get(share).unwrap().mid.unwrap();
        let camera_mid = engine.downs.get(camera).unwrap().mid.unwrap();
        let t = engine.transports.get_mut(&tid).unwrap();
        let share_ssrc = {
            let mut api = t.rtc.direct_api();
            let stream = api.stream_tx_by_mid(share_mid, None).unwrap();
            assert_eq!(stream.bwe_participating(), !disable);
            stream.ssrc()
        };
        let camera_ssrc = {
            let mut api = t.rtc.direct_api();
            let stream = api.stream_tx_by_mid(camera_mid, None).unwrap();
            assert!(
                stream.bwe_participating(),
                "managed stream keeps estimation on same transport"
            );
            stream.ssrc()
        };
        assert_eq!(
            t.remote_feedback.is_none(),
            native,
            "both actual native and Feedback owners covered"
        );
        let server_addr = "127.0.0.1:9401".parse().unwrap();
        let peer_addr = "127.0.0.1:9402".parse().unwrap();
        let mut sent = false;
        let mut received = Vec::new();
        for tick in 0..1500u64 {
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
                    Output::Event(Event::RtpPacket(packet)) => received.push(packet),
                    Output::Timeout(_) => break,
                    _ => {}
                }
            }
            if peer.is_connected() && !sent {
                let t = engine.transports.get_mut(&tid).unwrap();
                for mid in std::iter::once(share_mid).chain(disable.then_some(camera_mid)) {
                    let pt = t
                        .rtc
                        .writer(mid)
                        .unwrap()
                        .payload_params()
                        .find(|p| p.spec().codec == str0m::format::Codec::Vp8)
                        .unwrap()
                        .pt();
                    t.rtc
                        .direct_api()
                        .stream_tx_by_mid(mid, None)
                        .unwrap()
                        .write_rtp(
                            str0m::rtp::RtpWrite::new(
                                pt,
                                1.into(),
                                90_000,
                                now,
                                vec![0x10, 0, 0, 0x9d, 1, 0x2a],
                            )
                            .marker(false),
                        );
                }
                engine.dirty.push(tid);
                sent = true;
            }
            engine.handle_timeout(now);
            let outbound: Vec<_> = packets.0.lock().unwrap().drain(..).collect();
            for bytes in outbound {
                peer.handle_input(Input::Receive(
                    now,
                    Receive::new(Protocol::Udp, server_addr, peer_addr, &bytes).unwrap(),
                ))
                .unwrap();
                // Sans-I/O contract: drain after EACH input. RTP mode owns one
                // pending packet; a same-tick unpaced burst must not overwrite
                // its earlier authenticated share packet with the camera packet.
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
                        Output::Event(Event::RtpPacket(packet)) => received.push(packet),
                        Output::Timeout(_) => break,
                        _ => {}
                    }
                }
            }
            if received.iter().any(|p| p.header.ssrc == share_ssrc)
                && (!disable || received.iter().any(|p| p.header.ssrc == camera_ssrc))
            {
                break;
            }
        }
        assert!(
            peer.is_connected() && sent,
            "actual ICE/DTLS handshake and RTP write required"
        );
        let share = received
            .iter()
            .find(|p| p.header.ssrc == share_ssrc)
            .expect("actual authenticated SRTP share received");
        assert_eq!(share.payload.as_ref(), &[0x10, 0, 0, 0x9d, 1, 0x2a]);
        if disable {
            assert!(share.header.ext_vals.abs_send_time.is_none());
            assert!(share.header.ext_vals.transport_cc.is_none());
        } else if native {
            assert!(share.header.ext_vals.transport_cc.is_some());
        } else {
            assert!(share.header.ext_vals.abs_send_time.is_some());
        }
        if disable {
            let camera = received
                .iter()
                .find(|p| p.header.ssrc == camera_ssrc)
                .expect("actual managed RTP after share exemption");
            if native {
                assert!(camera.header.ext_vals.transport_cc.is_some());
            } else {
                assert!(camera.header.ext_vals.abs_send_time.is_some());
            }
        }
        engine.handle_command(
            rtc_core::media_api::MediaCommand::CloseTransport(tid),
            start + Duration::from_secs(2),
        );
        assert_eq!(
            engine.stats().transports,
            0,
            "actual public close releases the transport"
        );
    }
}
#[test]
fn configured_unmanaged_exemption_drives_native_bwe_real_downtrack_and_srtp_packets() {
    controlled_consumer(true);
}
#[test]
fn configured_unmanaged_exemption_drives_feedback_owner_real_downtrack_and_srtp_packets() {
    controlled_consumer(false);
}
