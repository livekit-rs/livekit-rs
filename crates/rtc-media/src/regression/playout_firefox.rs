//! Actual same-engine independent subscribers: native SDP, DTLS/SRTP, forwarded
//! VP8 frames and their decrypted RTP header extensions, not a config getter.
use super::*;
use crate::net::{Datagram, LocalCandidate};
use rtc_core::media_api::{DownTrackSpec, MediaCommand};
use rtc_core::model::TrackSettings;
use std::net::SocketAddr;
use str0m::media::{Frequency, MediaTime};
use str0m::net::{Protocol, Receive};
use str0m::{Event, Input, Output};

const URI: &str = "http://www.webrtc.org/experiments/rtp-hdrext/playout-delay";

struct Subscriber {
    name: &'static str,
    tid: TransportId,
    addr: SocketAddr,
    peer: Rtc,
    expected_delay: Option<(MediaTime, MediaTime)>,
    frames: usize,
}

fn drain_peer(
    engine: &mut MediaEngine,
    peer: &mut Rtc,
    tid: TransportId,
    addr: SocketAddr,
    server: SocketAddr,
    now: Instant,
    mut media: impl FnMut(str0m::media::MediaData),
) {
    loop {
        match peer.poll_output().unwrap() {
            Output::Transmit(packet) => engine.handle_datagram(Datagram {
                transport: tid,
                proto: Proto::Udp,
                source: addr,
                destination: server,
                data: &packet.contents,
                received_at: now,
            }),
            Output::Event(Event::MediaData(data)) => media(data),
            Output::Event(_) => {}
            Output::Timeout(_) => break,
        }
    }
}

#[test]
fn same_room_chrome_firefox_safari_consume_independent_playout_sdp_and_encrypted_video() {
    for sync_streams in [false, true] {
        let start = Instant::now();
        let mut engine = engine(start);
        engine.cfg.playout_delay = Some((100, 200));
        engine.cfg.enabled_codecs.push(CodecSpec {
            mime: "video/vp8".into(),
            fmtp_line: String::new(),
        });
        let packets = RoutedPacketQueue::default();
        engine.out = Box::new(packets.clone());
        let server = "127.0.0.1:20201".parse().unwrap();
        let source = "127.0.0.1:20202".parse().unwrap();
        let pub_id = transport(&mut engine, PcRole::Publisher, start);
        let t = engine.transports.get_mut(&pub_id).unwrap();
        let candidate = t
            .rtc
            .add_local_candidate(str0m::Candidate::host(server, "udp").unwrap())
            .unwrap()
            .clone();
        t.candidates.push(candidate);
        t.spec.primary = false;
        let mut publisher = client(start);
        publisher.add_local_candidate(str0m::Candidate::host(source, "udp").unwrap());
        let mid = published_mid(&offer(&mut engine, &mut publisher, pub_id, start, |api| {
            api.add_media(MediaKind::Video, Direction::SendOnly, None, None, None);
        }));
        let track = TrackSid::from("TR_playout_browser_profiles");
        engine.bind_track(pub_id, &mid.to_string(), track.clone(), start);
        let mut subscribers = Vec::new();
        let mut now = start;
        for (i, (name, disable)) in [
            ("Chrome", false),
            ("Firefox", sync_streams),
            ("Safari", false),
        ]
        .into_iter()
        .enumerate()
        {
            // Actual Room→TransportSpec and browser→capability are tested
            // separately. Here these are the independent resulting PC policies.
            let tid = TransportId::next();
            let addr = SocketAddr::from(([127, 0, 0, 1], 20203 + i as u16));
            let participant = ParticipantSid::from(format!("PA_{name}"));
            engine.create_transport(
                TransportSpec {
                    ice_preference: Default::default(),
                    one_shot: false,
                    id: tid,
                    participant: participant.clone(),
                    role: PcRole::Subscriber,
                    ice_lite: true,
                    server_data_channels: true,
                    enable_data_tracks: false,
                    primary: true,
                    client_supports_red: false,
                    codec_switch_requires_renegotiation: false,
                    disable_playout_delay: disable,
                    allow_pause: true,
                },
                IceCredentials::random(),
                vec![LocalCandidate {
                    proto: Proto::Udp,
                    addr: server,
                    base: server,
                }],
                now,
            );
            let mut peer = Rtc::builder()
                .set_crypto_provider(rtc_crypto::provider().into())
                .set_dtls_cert(rtc_crypto::dtls_cert().unwrap())
                .set_extension(6, str0m::rtp::Extension::PlayoutDelay)
                .build(now);
            peer.add_local_candidate(str0m::Candidate::host(addr, "udp").unwrap());
            answer_server_offer(&mut engine, &mut peer, tid, now);
            engine.subscribe(
                DownTrackSpec {
                    subscriber: participant,
                    publisher: "PA_test".into(),
                    track_sid: track.clone(),
                    stream_id: format!("PA_test|{track}"),
                    settings: TrackSettings::default(),
                    adaptive_stream: false,
                },
                now,
            );
            now += Duration::from_millis(160);
            engine.handle_timeout(now);
            let mut offers = vec![];
            engine.drain_events(&mut offers);
            let (offer_id, video_sdp) = offers
                .into_iter()
                .find_map(|e| match e {
                    MediaEvent::LocalDescription {
                        transport,
                        kind: SdpType::Offer,
                        sdp,
                        id,
                        ..
                    } if transport == tid => Some((id, sdp)),
                    _ => None,
                })
                .expect("actual video subscriber offer");
            assert!(video_sdp.contains("m=video "));
            assert_eq!(
                video_sdp.contains(URI),
                !disable,
                "{name} actual SDP, sync_streams={sync_streams}"
            );
            let answer = peer
                .sdp_api()
                .accept_offer(str0m::change::SdpOffer::from_sdp_string(&video_sdp).unwrap())
                .unwrap();
            engine.on_remote_description(
                tid,
                SdpType::Answer,
                answer.to_sdp_string(),
                offer_id,
                now,
            );
            subscribers.push(Subscriber {
                name,
                tid,
                addr,
                peer,
                expected_delay: (!disable)
                    .then(|| (MediaTime::from_millis(100), MediaTime::from_millis(200))),
                frames: 0,
            });
        }
        let mut keyframe = vec![0u8; 2200];
        keyframe[..10].copy_from_slice(&[0x10, 0, 0, 0x9d, 1, 0x2a, 0x80, 2, 0x68, 1]);
        for tick in 0..6000u64 {
            let at = now + Duration::from_millis(tick);
            publisher.handle_input(Input::Timeout(at)).unwrap();
            drain_peer(
                &mut engine,
                &mut publisher,
                pub_id,
                source,
                server,
                at,
                |_| {},
            );
            if publisher.is_connected() && tick.is_multiple_of(30) {
                let writer = publisher.writer(mid).unwrap();
                let pt = writer
                    .payload_params()
                    .find(|p| p.spec().codec == str0m::format::Codec::Vp8)
                    .unwrap()
                    .pt();
                writer
                    .write(
                        pt,
                        at,
                        MediaTime::new(tick * 90, Frequency::NINETY_KHZ),
                        keyframe.clone(),
                    )
                    .unwrap();
                drain_peer(
                    &mut engine,
                    &mut publisher,
                    pub_id,
                    source,
                    server,
                    at,
                    |_| {},
                );
            }
            for sub in &mut subscribers {
                sub.peer.handle_input(Input::Timeout(at)).unwrap();
                drain_peer(
                    &mut engine,
                    &mut sub.peer,
                    sub.tid,
                    sub.addr,
                    server,
                    at,
                    |_| {},
                );
            }
            engine.handle_timeout(at);
            let outgoing = packets.0.lock().unwrap().drain(..).collect::<Vec<_>>();
            for (destination, data) in outgoing {
                if destination == source {
                    publisher
                        .handle_input(Input::Receive(
                            at,
                            Receive::new(Protocol::Udp, server, source, &data).unwrap(),
                        ))
                        .unwrap();
                    drain_peer(
                        &mut engine,
                        &mut publisher,
                        pub_id,
                        source,
                        server,
                        at,
                        |_| {},
                    );
                    continue;
                }
                let sub = subscribers
                    .iter_mut()
                    .find(|s| s.addr == destination)
                    .expect("owned routed recipient");
                sub.peer
                    .handle_input(Input::Receive(
                        at,
                        Receive::new(Protocol::Udp, server, destination, &data).unwrap(),
                    ))
                    .unwrap();
                let Subscriber {
                    name,
                    tid,
                    addr,
                    peer,
                    expected_delay,
                    frames,
                } = sub;
                drain_peer(&mut engine, peer, *tid, *addr, server, at, |data| {
                    assert!(!data.data.is_empty(), "actual decrypted VP8 media");
                    assert_eq!(
                        data.ext_vals.play_delay_min,
                        expected_delay.map(|d| d.0),
                        "{name} SRTP min, sync={sync_streams}"
                    );
                    assert_eq!(
                        data.ext_vals.play_delay_max,
                        expected_delay.map(|d| d.1),
                        "{name} SRTP max, sync={sync_streams}"
                    );
                    *frames += 1;
                });
            }
            if subscribers.iter().all(|s| s.frames >= 5) {
                now = at;
                break;
            }
        }
        for sub in &subscribers {
            assert!(
                sub.frames >= 5 && sub.peer.is_connected(),
                "{} received {} authenticated media frames, sync={sync_streams}",
                sub.name,
                sub.frames
            );
            engine.handle_command(MediaCommand::CloseTransport(sub.tid), now);
        }
        engine.handle_command(MediaCommand::CloseTransport(pub_id), now);
        assert!(engine.transports.is_empty());
    }
}
