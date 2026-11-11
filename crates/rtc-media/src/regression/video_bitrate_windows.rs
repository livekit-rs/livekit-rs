//! Authenticated VP8 publication through the production engine and rate observer.
use super::*;
use rtc_core::media_api::{MediaCommand, PublishedTrackSpec};
use rtc_core::model::{TrackKind, TrackSource};
use str0m::format::Codec;
use str0m::media::{Frequency, MediaTime};
use str0m::net::{Protocol, Receive};
use str0m::{Candidate, Input, Output};

fn report_after(mut engine: MediaEngine, source: TrackSource, start: Instant) -> Option<Duration> {
    engine.cfg.enabled_codecs = vec![CodecSpec {
        mime: "video/VP8".into(),
        fmtp_line: String::new(),
    }];
    let queue = RoutedPacketQueue::default();
    engine.out = Box::new(queue.clone());
    let server = "127.0.0.1:9721".parse().unwrap();
    let remote = "127.0.0.1:9722".parse().unwrap();
    let tid = transport(&mut engine, PcRole::Publisher, start);
    let transport = engine.transports.get_mut(&tid).unwrap();
    let candidate = transport
        .rtc
        .add_local_candidate(Candidate::host(server, "udp").unwrap())
        .unwrap()
        .clone();
    transport.candidates.push(candidate);
    let sid = TrackSid::from("TR_configured_bitrate");
    engine.handle_command(
        MediaCommand::ExpectTrack {
            transport: tid,
            track: PublishedTrackSpec {
                sid: sid.clone(),
                kind: TrackKind::Video,
                source,
                cid: "bitrate-window".into(),
                layers: vec![],
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
    let mut peer = Rtc::builder()
        .set_crypto_provider(rtc_crypto::provider().into())
        .set_dtls_cert(rtc_crypto::dtls_cert().unwrap())
        .clear_codecs()
        .enable_vp8(true)
        .build(start);
    peer.add_local_candidate(Candidate::host(remote, "udp").unwrap());
    let mid = published_mid(&offer(&mut engine, &mut peer, tid, start, |change| {
        change.add_media(
            MediaKind::Video,
            Direction::SendOnly,
            None,
            Some("bitrate-window".into()),
            None,
        );
    }));
    engine.bind_track(tid, &mid.to_string(), sid.clone(), start);
    let ui = engine.up_by_sid[&sid];
    assert_eq!(engine.ups.get(ui).unwrap().spec.source, source);
    let mut first_frame = None;
    let mut first_bitrate = None;
    let mut release_at = start;
    for tick in 0..1800u64 {
        let now = start + Duration::from_millis(tick);
        release_at = now;
        peer.handle_input(Input::Timeout(now)).unwrap();
        if peer.is_connected() && tick.is_multiple_of(15) {
            let writer = peer.writer(mid).unwrap();
            let pt = writer
                .payload_params()
                .find(|p| p.spec().codec == Codec::Vp8)
                .unwrap()
                .pt();
            let mut frame = vec![0; 80];
            frame[..10].copy_from_slice(&[0x10, 0, 0, 0x9d, 1, 0x2a, 0x80, 2, 0x68, 1]);
            writer
                .write(
                    pt,
                    now,
                    MediaTime::new(tick * 90, Frequency::NINETY_KHZ),
                    frame,
                )
                .unwrap();
        }
        loop {
            match peer.poll_output().unwrap() {
                Output::Transmit(packet) => engine.handle_datagram(crate::net::Datagram {
                    transport: tid,
                    proto: Proto::Udp,
                    source: remote,
                    destination: server,
                    data: &packet.contents,
                    received_at: now,
                }),
                Output::Timeout(_) => break,
                _ => {}
            }
        }
        engine.handle_timeout(now);
        let outbound = { queue.0.lock().unwrap().drain(..).collect::<Vec<_>>() };
        for (destination, data) in outbound {
            assert_eq!(destination, remote);
            peer.handle_input(Input::Receive(
                now,
                Receive::new(Protocol::Udp, server, remote, &data).unwrap(),
            ))
            .unwrap();
            loop {
                match peer.poll_output().unwrap() {
                    Output::Transmit(packet) => engine.handle_datagram(crate::net::Datagram {
                        transport: tid,
                        proto: Proto::Udp,
                        source: remote,
                        destination: server,
                        data: &packet.contents,
                        received_at: now,
                    }),
                    Output::Timeout(_) => break,
                    _ => {}
                }
            }
        }
        let up = engine.ups.get(ui).unwrap();
        if let Some(received) = up.stats[0].last_seen {
            let first = *first_frame.get_or_insert(received);
            let bps = up.bitrates()[0];
            if bps > 0 {
                first_bitrate = Some(now.duration_since(first));
                eprintln!("actual source {source:?} first_report={first_bitrate:?} bps={bps}");
                break;
            }
        }
    }
    assert!(
        peer.is_connected() && first_frame.is_some(),
        "real ICE/DTLS/SRTP did not produce a VP8 frame"
    );
    engine.handle_command(
        MediaCommand::UnpublishTrack {
            track_sid: sid.clone(),
        },
        release_at,
    );
    engine.handle_command(MediaCommand::CloseTransport(tid), release_at);
    assert!(!engine.transports.contains_key(&tid));
    assert!(!engine.up_by_sid.contains_key(&sid));
    first_bitrate
}

#[test]
fn source_specific_windows_report_real_authenticated_video() {
    for (source, interval_ms) in [(TrackSource::Camera, 125), (TrackSource::ScreenShare, 450)] {
        let now = Instant::now();
        let mut engine = engine(now);
        engine.cfg.video.bitrate_report.video[0] = Some(Duration::from_millis(125));
        engine.cfg.video.bitrate_report.screenshare[0] = Some(Duration::from_millis(450));
        let actual = report_after(engine, source, now).expect("real VP8 report");
        assert!(
            (Duration::from_millis(interval_ms)..=Duration::from_millis(interval_ms + 8))
                .contains(&actual),
            "{source:?}: {actual:?}"
        );
    }
}

#[test]
fn absent_report_window_still_accepts_real_screen_video() {
    let now = Instant::now();
    let mut engine = engine(now);
    engine.cfg.video.bitrate_report.screenshare = [None; 3];
    assert!(report_after(engine, TrackSource::ScreenShare, now).is_none());
}

#[test]
fn frame_tracker_uses_authenticated_vp8_markers_before_depacketization() {
    use rtc_core::config::StreamTrackerSpec;
    use str0m::rtp::RtpWrite;

    let start = Instant::now();
    let mut engine = engine(start);
    engine.cfg.enabled_codecs = vec![CodecSpec {
        mime: "video/VP8".into(),
        fmtp_line: String::new(),
    }];
    engine.cfg.video.stream_trackers.video =
        [Some(StreamTrackerSpec::Frame { min_fps: 5.0 }), None, None];
    let queue = RoutedPacketQueue::default();
    engine.out = Box::new(queue.clone());
    let server = "127.0.0.1:9731".parse().unwrap();
    let remote = "127.0.0.1:9732".parse().unwrap();
    let tid = transport(&mut engine, PcRole::Publisher, start);
    let transport = engine.transports.get_mut(&tid).unwrap();
    let candidate = transport
        .rtc
        .add_local_candidate(Candidate::host(server, "udp").unwrap())
        .unwrap()
        .clone();
    transport.candidates.push(candidate);
    let mut peer = Rtc::builder()
        .set_crypto_provider(rtc_crypto::provider().into())
        .set_dtls_cert(rtc_crypto::dtls_cert().unwrap())
        .clear_codecs()
        .enable_vp8(true)
        .set_rtp_mode(true)
        .build(start);
    peer.add_local_candidate(Candidate::host(remote, "udp").unwrap());
    let mid = published_mid(&offer(&mut engine, &mut peer, tid, start, |change| {
        change.add_media(MediaKind::Video, Direction::SendOnly, None, None, None);
    }));
    let sid = TrackSid::from("TR_frame_tracker");
    engine.bind_track(tid, &mid.to_string(), sid.clone(), start);
    let ui = engine.up_by_sid[&sid];
    let mut first = None;
    let mut seq = 0u64;
    for tick in 0..2000u32 {
        let now = start + Duration::from_millis(u64::from(tick));
        peer.handle_input(Input::Timeout(now)).unwrap();
        if peer.is_connected() && tick.is_multiple_of(25) {
            let origin = *first.get_or_insert(tick);
            let elapsed = tick - origin;
            seq += 1;
            // Every RTP payload is a continuation fragment. No complete VP8
            // frame can be assembled, even when the marker bit is set.
            peer.direct_api()
                .stream_tx_by_mid(mid, None)
                .unwrap()
                .write_rtp(
                    RtpWrite::new(96.into(), seq.into(), elapsed * 90, now, vec![0, 0x55])
                        .marker(matches!(elapsed, 700 | 800)),
                );
        }
        loop {
            match peer.poll_output().unwrap() {
                Output::Transmit(packet) => engine.handle_datagram(crate::net::Datagram {
                    transport: tid,
                    proto: Proto::Udp,
                    source: remote,
                    destination: server,
                    data: &packet.contents,
                    received_at: now,
                }),
                Output::Timeout(_) => break,
                _ => {}
            }
        }
        engine.handle_timeout(now);
        let outbound = { queue.0.lock().unwrap().drain(..).collect::<Vec<_>>() };
        for (destination, data) in outbound {
            assert_eq!(destination, remote);
            peer.handle_input(Input::Receive(
                now,
                Receive::new(Protocol::Udp, server, remote, &data).unwrap(),
            ))
            .unwrap();
        }
        if let Some(origin) = first {
            let up = engine.ups.get(ui).unwrap();
            match tick - origin {
                100 | 1050 => assert!(up.layer_available(0, now)),
                600 | 1550 => assert!(!up.layer_available(0, now)),
                _ => {}
            }
            assert!(
                up.stats[0].last_seen.is_none(),
                "unexpected assembled VP8 frame"
            );
            assert!(
                !up.layer_available(1, now),
                "unconfigured layer was exposed"
            );
        }
    }
    assert!(
        first.is_some_and(|origin| origin < 450),
        "test did not exercise every timing window"
    );
}
