//! Executable state-machine regressions using real str0m offer/answer objects.
use std::time::{Duration, Instant};

use rtc_core::config::{
    AudioConfig, CongestionControl, Filter, NodeIp, PliThrottle, RtcConfig, UdpMode, VideoConfig,
};
use rtc_core::ids::{ParticipantSid, TrackSid, TransportId};
use rtc_core::media_api::{DataKind, MediaEvent, PcRole, TransportSpec};
use rtc_core::model::CodecSpec;
use rtc_core::session::SdpType;
use str0m::Rtc;
use str0m::change::SdpAnswer;
use str0m::media::{Direction, MediaKind, Mid};

use crate::engine::{MediaEngine, MediaEngineConfig};
use crate::net::{IceCredentials, PacketOut, Proto};
use crate::transport::ChannelEntry;

struct Discard;
impl PacketOut for Discard {
    fn send(&self, _: Proto, _: std::net::SocketAddr, _: std::net::SocketAddr, _: &[u8]) {}
}

fn engine(now: Instant) -> MediaEngine {
    engine_with_audio_interval(now, Duration::from_millis(500))
}

fn engine_with_audio_interval(now: Instant, interval: Duration) -> MediaEngine {
    MediaEngine::new(
        MediaEngineConfig {
            rtc: RtcConfig {
                ice_acceptance_min_waits: [
                    Duration::ZERO,
                    Duration::from_millis(500),
                    Duration::from_secs(1),
                    Duration::from_secs(2),
                ],
                forward_stats: None,
                udp: UdpMode::Disabled,
                tcp_port: None,
                node_ip: NodeIp::default(),
                use_node_ip_mapping: false,
                advertise_internal_ip: false,
                stun_servers: vec![],
                turn_servers: vec![],
                use_ice_lite: true,
                force_tcp: false,
                use_stun_port_as_ice: false,
                use_external_ip: false,
                skip_external_ip_validation: false,
                external_ip_only: false,
                batch_write_size: 0,
                batch_write_interval: Duration::ZERO,
                media_batch_io_enabled: false,
                interfaces: Filter::default(),
                ips: Filter::default(),
                enable_loopback_candidate: false,
                use_mdns: false,
                enable_warp: false,
                enable_rtp_stream_restart_detection: false,
                data_channel_slow_threshold: 0,
                data_channel_max_buffered_amount: 0,
                data_channel_lossy_target_latency: rtc_core::config::PolicyDuration::from_nanos(0),
                data_track_target_latency: rtc_core::config::PolicyDuration::from_millis(100),
                sctp: Default::default(),
                packet_buffer_size_video: 500,
                packet_buffer_size_audio: 200,
                pli_throttle: PliThrottle {
                    low: Duration::from_millis(500),
                    mid: Duration::from_millis(500),
                    high: Duration::from_millis(500),
                },
                congestion_control: CongestionControl {
                    enabled: true,
                    // Upstream production default (`pkg/config/config.go:584 AllowPause: false`).
                    // With the Go allocation authority a cold transport has committed nothing,
                    // so `allow_pause: true` would pause every managed track until a probe
                    // cluster commits capacity; the pause branch has its own vector.
                    allow_pause: false,
                    use_send_side_bwe: true,
                    ..CongestionControl::default()
                },
                allow_tcp_fallback: true,
                tcp_fallback_rtt_threshold: 0,
                allow_udp_unstable_fallback: false,
                reconnect_on_publication_error: false,
                reconnect_on_subscription_error: false,
                reconnect_on_data_channel_error: false,
            },
            audio: AudioConfig {
                active_level: 35,
                min_percentile: 40,
                update_interval: interval,
                smooth_intervals: 2,
                active_red_encoding: false,
            },
            video: VideoConfig {
                codec_regression_threshold: 5,
                dynacast_pause_delay: Duration::from_secs(5),
                bitrate_report: Default::default(),
                stream_trackers: Default::default(),
            },
            enabled_codecs: vec![CodecSpec {
                mime: "audio/opus".into(),
                fmtp_line: String::new(),
            }],
            playout_delay: None,
            sif_trailer: vec![],
        },
        Box::new(Discard),
        now,
    )
    .unwrap()
}

fn transport_spec(tid: TransportId, role: PcRole) -> TransportSpec {
    TransportSpec {
        ice_preference: Default::default(),
        one_shot: false,
        id: tid,
        participant: ParticipantSid::from("PA_test"),
        role,
        ice_lite: true,
        server_data_channels: false,
        enable_data_tracks: true,
        primary: true,
        client_supports_red: false,
        codec_switch_requires_renegotiation: false,
        disable_playout_delay: false,
        // `pkg/config/config.go: AllowPause` defaults to false, and the
        // production path builds this spec from that config
        // (`rtc_core::config::RoomPolicy::from`); pause has its own vectors.
        allow_pause: false,
    }
}

fn transport(engine: &mut MediaEngine, role: PcRole, now: Instant) -> TransportId {
    let tid = TransportId::next();
    engine.create_transport(
        transport_spec(tid, role),
        IceCredentials::random(),
        vec![],
        now,
    );
    tid
}

fn client(now: Instant) -> Rtc {
    Rtc::builder()
        .set_crypto_provider(rtc_crypto::provider().into())
        .set_dtls_cert(rtc_crypto::dtls_cert().unwrap())
        .build(now)
}

fn offer(
    engine: &mut MediaEngine,
    client: &mut Rtc,
    tid: TransportId,
    now: Instant,
    edit: impl FnOnce(&mut str0m::change::SdpApi<'_>),
) -> Vec<MediaEvent> {
    let mut change = client.sdp_api();
    edit(&mut change);
    let (offer, pending) = change.apply().unwrap();
    engine.on_remote_description(tid, SdpType::Offer, offer.to_sdp_string(), 1, now);
    let mut events = vec![];
    engine.drain_events(&mut events);
    let answer = events
        .iter()
        .find_map(|e| match e {
            MediaEvent::LocalDescription {
                kind: SdpType::Answer,
                sdp,
                ..
            } => Some(sdp),
            _ => None,
        })
        .expect("server answers a real client offer");
    client
        .sdp_api()
        .accept_answer(pending, SdpAnswer::from_sdp_string(answer).unwrap())
        .unwrap();
    events
}

#[test]
fn server_reflexive_trickle_finishes_only_the_current_generation() {
    fn candidates(events: &[MediaEvent]) -> Vec<(&str, bool)> {
        events
            .iter()
            .filter_map(|event| match event {
                MediaEvent::LocalCandidate {
                    candidate, last, ..
                } => Some((candidate.candidate.as_str(), *last)),
                _ => None,
            })
            .collect()
    }

    let now = Instant::now();
    let mut engine = engine(now);
    let tid = TransportId::next();
    let mut spec = transport_spec(tid, PcRole::Single);
    spec.ice_lite = false;
    let creds = IceCredentials::random();
    let old_ufrag = creds.ufrag.clone();
    let hosts = ["127.0.0.1:9001", "127.0.0.1:9002"].map(|address| {
        let address = address.parse().unwrap();
        crate::LocalCandidate {
            proto: Proto::Udp,
            addr: address,
            base: address,
        }
    });
    engine.create_transport_with_srflx(spec, creds, Vec::from(hosts), true, now);
    let mut peer = client(now);
    let initial = offer(&mut engine, &mut peer, tid, now, |change| {
        change.add_media(MediaKind::Audio, Direction::SendOnly, None, None, None);
    });
    let hosts = candidates(&initial);
    assert_eq!(hosts.len(), 1);
    assert!(hosts[0].0.contains("9001 typ host") && !hosts[0].1);

    let mapped = "203.0.113.2:9004".parse().unwrap();
    let base = "127.0.0.1:9003".parse().unwrap();
    engine.complete_srflx(tid, &old_ufrag, [(mapped, base)], now);
    let mut completion = vec![];
    engine.drain_events(&mut completion);
    let completed = candidates(&completion);
    assert_eq!(completed.len(), 2);
    assert!(completed[0].0.contains("9002 typ host") && !completed[0].1);
    assert!(
        completed[1]
            .0
            .contains("1694498815 203.0.113.2 9004 typ srflx raddr 127.0.0.1 rport 9003")
    );
    assert!(completed[1].1);

    let restart = offer(&mut engine, &mut peer, tid, now, |change| {
        change.ice_restart(true);
    });
    let new_ufrag = engine.transports[&tid].ufrag.clone();
    assert_ne!(old_ufrag, new_ufrag);
    let restarting = candidates(&restart);
    assert_eq!(restarting.len(), 1);
    assert!(restarting[0].0.contains("9001 typ host") && !restarting[0].1);
    assert!(restarting[0].0.ends_with(&new_ufrag));

    engine.complete_srflx(tid, &old_ufrag, [(mapped, base)], now);
    completion.clear();
    engine.drain_events(&mut completion);
    assert!(candidates(&completion).is_empty());
    engine.complete_srflx(tid, &new_ufrag, [], now);
    engine.drain_events(&mut completion);
    let failed_gather = candidates(&completion);
    assert_eq!(failed_gather.len(), 1);
    assert!(failed_gather[0].0.contains("9002 typ host") && failed_gather[0].1);
    assert!(failed_gather[0].0.ends_with(&new_ufrag));
}

fn published_mid(events: &[MediaEvent]) -> Mid {
    events
        .iter()
        .find_map(|e| match e {
            MediaEvent::RemoteMediaAdded(m) => Some(Mid::from(m.mid.as_str())),
            _ => None,
        })
        .expect("remote publication discovered")
}

#[test]
fn unpublish_then_republish_on_same_mid_is_discovered_and_bound() {
    let now = Instant::now();
    let mut engine = engine(now);
    let tid = transport(&mut engine, PcRole::Publisher, now);
    let mut client = client(now);
    let mid = published_mid(&offer(&mut engine, &mut client, tid, now, |change| {
        change.add_media(MediaKind::Audio, Direction::SendOnly, None, None, None);
    }));
    engine.bind_track(tid, &mid.to_string(), TrackSid::from("TR_first"), now);
    assert_eq!(engine.stats().uptracks, 1);
    engine.unpublish(&TrackSid::from("TR_first"), now);
    assert_eq!(engine.stats().uptracks, 0);
    // A new logical publication can retain the browser transceiver and MID.
    offer(&mut engine, &mut client, tid, now, |change| {
        change.set_direction(mid, Direction::Inactive);
    });
    assert_eq!(
        published_mid(&offer(&mut engine, &mut client, tid, now, |change| {
            change.set_direction(mid, Direction::SendOnly);
        })),
        mid
    );
    engine.bind_track(tid, &mid.to_string(), TrackSid::from("TR_second"), now);
    assert!(engine.up_by_sid.contains_key(&TrackSid::from("TR_second")));
    assert!(!engine.up_by_sid.contains_key(&TrackSid::from("TR_first")));
}

#[test]
fn inactive_unbound_section_is_rediscovered_when_resumed() {
    let now = Instant::now();
    let mut engine = engine(now);
    let tid = transport(&mut engine, PcRole::Single, now);
    let mut client = client(now);
    let mid = published_mid(&offer(&mut engine, &mut client, tid, now, |change| {
        change.add_media(MediaKind::Audio, Direction::SendOnly, None, None, None);
    }));
    offer(&mut engine, &mut client, tid, now, |change| {
        change.set_direction(mid, Direction::Inactive);
    });
    assert_eq!(
        published_mid(&offer(&mut engine, &mut client, tid, now, |change| {
            change.set_direction(mid, Direction::SendOnly);
        })),
        mid
    );
}

#[test]
fn reliable_and_lossy_data_channels_do_not_substitute_for_each_other() {
    let now = Instant::now();
    let mut engine = engine(now);
    let tid = transport(&mut engine, PcRole::Single, now);
    let t = engine.transports.get_mut(&tid).unwrap();
    let lossy = t.rtc.sdp_api().add_channel("_lossy".into());
    t.channels.push(ChannelEntry {
        id: lossy,
        label: "_lossy".into(),
        kind: Some(DataKind::Lossy),
    });
    assert_eq!(t.channel_for(DataKind::Reliable), None);
    assert_eq!(
        t.channel_for(DataKind::Lossy),
        Some((lossy, DataKind::Lossy))
    );
    let reliable = t.rtc.sdp_api().add_channel("_reliable".into());
    t.channels.push(ChannelEntry {
        id: reliable,
        label: "_reliable".into(),
        kind: Some(DataKind::Reliable),
    });
    assert_eq!(
        t.channel_for(DataKind::Reliable),
        Some((reliable, DataKind::Reliable))
    );
    t.channels.retain(|c| c.kind != Some(DataKind::Lossy));
    assert_eq!(t.channel_for(DataKind::Lossy), None);
}

#[test]
fn disabled_remote_data_track_channel_is_not_registered_or_delivered() {
    let now = Instant::now();
    let mut engine = engine(now);
    let tid = transport(&mut engine, PcRole::Single, now);
    let t = engine.transports.get_mut(&tid).unwrap();
    t.spec.enable_data_tracks = false;
    let id = t.rtc.sdp_api().add_channel("_data_track".into());
    engine.on_channel_open(tid, id, "_data_track".into());
    engine.on_channel_data(
        tid,
        str0m::channel::ChannelData {
            id,
            binary: true,
            data: vec![0; 12],
        },
    );
    assert!(engine.transports[&tid].channels.is_empty());
    assert!(
        !engine
            .events
            .iter()
            .any(|e| matches!(e, MediaEvent::DataReceived { .. }))
    );
}

#[test]
fn data_track_never_falls_back_to_secondary_or_packet_channels() {
    let now = Instant::now();
    let mut engine = engine(now);
    let primary = transport(&mut engine, PcRole::Subscriber, now);
    let secondary = transport(&mut engine, PcRole::Publisher, now);
    engine.transports.get_mut(&secondary).unwrap().spec.primary = false;
    let id = engine
        .transports
        .get_mut(&secondary)
        .unwrap()
        .rtc
        .sdp_api()
        .add_channel("_data_track".into());
    engine.on_channel_open(secondary, id, "_data_track".into());
    let packet = engine
        .transports
        .get_mut(&primary)
        .unwrap()
        .rtc
        .sdp_api()
        .add_channel("_reliable".into());
    engine.on_channel_open(primary, packet, "_reliable".into());
    assert_eq!(engine.data_track_channel(&"PA_test".into()), None);
    let id = engine
        .transports
        .get_mut(&primary)
        .unwrap()
        .rtc
        .sdp_api()
        .add_channel("_data_track".into());
    engine.on_channel_open(primary, id, "_data_track".into());
    assert_eq!(
        engine.data_track_channel(&"PA_test".into()),
        Some((primary, id))
    );
    engine
        .transports
        .get_mut(&primary)
        .unwrap()
        .spec
        .enable_data_tracks = false;
    assert_eq!(engine.data_track_channel(&"PA_test".into()), None);
}

#[test]
fn reopened_data_track_channel_replaces_old_writer_and_ignores_stale_data() {
    let now = Instant::now();
    let mut engine = engine(now);
    let tid = transport(&mut engine, PcRole::Single, now);
    let old = engine
        .transports
        .get_mut(&tid)
        .unwrap()
        .rtc
        .sdp_api()
        .add_channel("_data_track".into());
    engine.on_channel_open(tid, old, "_data_track".into());
    let new = engine
        .transports
        .get_mut(&tid)
        .unwrap()
        .rtc
        .sdp_api()
        .add_channel("_data_track".into());
    engine.on_channel_open(tid, new, "_data_track".into());
    assert_eq!(engine.transports[&tid].channels.len(), 1);
    assert_eq!(
        engine.data_track_channel(&"PA_test".into()),
        Some((tid, new))
    );
    engine.on_channel_data(
        tid,
        str0m::channel::ChannelData {
            id: old,
            binary: true,
            data: vec![0; 12],
        },
    );
    assert!(
        !engine
            .events
            .iter()
            .any(|e| matches!(e, MediaEvent::DataReceived { .. }))
    );
}

#[derive(Clone, Default)]
struct PacketQueue(std::sync::Arc<std::sync::Mutex<std::collections::VecDeque<Vec<u8>>>>);
impl PacketOut for PacketQueue {
    fn send(&self, _: Proto, _: std::net::SocketAddr, _: std::net::SocketAddr, data: &[u8]) {
        self.0.lock().unwrap().push_back(data.to_vec());
    }
}

/// Exercises the real ICE/DTLS/SRTP/SCTP path, without opening network sockets.
/// It deliberately uses two m-lines on one peer: client publishes, then adds a
/// receiving m-line after the engine requests space for a subscription.
#[test]
fn single_pc_dynamic_subscription_forwards_opus_and_reliable_data() {
    single_pc_opus_and_data_channel(false, false);
}

#[test]
fn single_pc_data_track_uses_independent_unreliable_sctp_channel() {
    single_pc_opus_and_data_channel(true, false);
}

#[test]
fn single_pc_plain_opus_recovers_dropped_srtp_with_real_audio_nack() {
    single_pc_opus_and_data_channel(false, true);
}

fn single_pc_opus_and_data_channel(data_track: bool, nack_loss: bool) {
    use crate::net::Datagram;
    use rtc_core::media_api::DownTrackSpec;
    use rtc_core::model::TrackSettings;
    use str0m::media::{Frequency, MediaTime};
    use str0m::net::{Protocol, Receive};
    use str0m::{Candidate, Event, Input, Output};

    let start = Instant::now();
    let mut engine = engine(start);
    let packets = PacketQueue::default();
    engine.out = Box::new(packets.clone());
    let tid = transport(&mut engine, PcRole::Single, start);
    let server_addr: std::net::SocketAddr = "127.0.0.1:9001".parse().unwrap();
    let client_addr: std::net::SocketAddr = "127.0.0.1:9002".parse().unwrap();
    let t = engine.transports.get_mut(&tid).unwrap();
    let candidate = t
        .rtc
        .add_local_candidate(Candidate::host(server_addr, "udp").unwrap())
        .unwrap()
        .clone();
    t.candidates.push(candidate);
    let mut client = if nack_loss {
        let mut config = Rtc::builder()
            .set_crypto_provider(rtc_crypto::provider().into())
            .set_dtls_cert(rtc_crypto::dtls_cert().unwrap())
            .enable_raw_packets(true);
        for param in config.codec_config().iter_mut() {
            if param.spec().codec == str0m::format::Codec::Opus {
                param.set_fb_nack(true);
            }
        }
        config.build(start)
    } else {
        client(start)
    };
    client.add_local_candidate(Candidate::host(client_addr, "udp").unwrap());
    let mut channel_id = None;
    let tx_mid = published_mid(&offer(&mut engine, &mut client, tid, start, |change| {
        change.add_media(MediaKind::Audio, Direction::SendOnly, None, None, None);
        channel_id = Some(
            change.add_channel_with_config(str0m::channel::ChannelConfig {
                label: if data_track {
                    "_data_track"
                } else {
                    "_reliable"
                }
                .into(),
                ordered: !data_track,
                reliability: if data_track {
                    str0m::channel::Reliability::MaxRetransmits { retransmits: 0 }
                } else {
                    str0m::channel::Reliability::Reliable
                },
                ..Default::default()
            }),
        );
    }));
    let track = TrackSid::from("TR_loopback");
    engine.bind_track(tid, &tx_mid.to_string(), track.clone(), start);
    engine.subscribe(
        DownTrackSpec {
            subscriber: ParticipantSid::from("PA_test"),
            publisher: ParticipantSid::from("PA_test"),
            track_sid: track.clone(),
            stream_id: "PA_test|TR_loopback".into(),
            settings: TrackSettings::default(),
            adaptive_stream: false,
        },
        start,
    );
    let mut rx_mid = None;
    let answer_events = offer(&mut engine, &mut client, tid, start, |change| {
        rx_mid = Some(change.add_media(MediaKind::Audio, Direction::RecvOnly, None, None, None));
    });
    assert!(answer_events.iter().any(|e| matches!(e,
        MediaEvent::LocalDescription { sdp, .. } if sdp.contains("a=msid:PA_test|TR_loopback TR_loopback")
    )), "single-PC answer advertises the LiveKit subscription identity");
    let channel_id = channel_id.unwrap();
    let rx_mid = rx_mid.unwrap();
    let opus = vec![0xf8, 0xff, 0xfe];
    let mut received_opus = false;
    let mut received_echo = false;
    let mut sent_data = false;
    let mut got_server_data = false;
    let mut dropped_audio = None;
    let mut recovered_audio = false;
    let mut nack_sent = false;
    let mut authenticated_duplicate = None;
    for tick in 0..3000u64 {
        let now = start + Duration::from_millis(tick * 10);
        if nack_loss && let Some(rx) = client.direct_api().stream_rx_by_mid(rx_mid, None) {
            rx.suppress_nack(false);
        }
        client.handle_input(Input::Timeout(now)).unwrap();
        if client.is_connected() {
            if !sent_data && let Some(mut channel) = client.channel(channel_id) {
                sent_data = channel.write(true, b"reliable-upstream").unwrap();
            }
            if tick % 2 == 0 {
                let writer = client.writer(tx_mid).unwrap();
                let pt = writer
                    .payload_params()
                    .find(|p| p.spec().codec == str0m::format::Codec::Opus)
                    .unwrap()
                    .pt();
                writer
                    .write(
                        pt,
                        now,
                        MediaTime::new(tick * 480, Frequency::FORTY_EIGHT_KHZ),
                        opus.clone(),
                    )
                    .unwrap();
            }
        }
        // Every str0m mutation is followed by a complete output drain before
        // the next mutation, matching the production engine's invariant.
        loop {
            match client.poll_output().unwrap() {
                Output::Transmit(p) => {
                    let is_rtp = p.contents.len() >= 22
                        && p.contents[0] >> 6 == 2
                        && p.contents[1] & 0x7f == 111;
                    if is_rtp && authenticated_duplicate.is_none() {
                        let before = engine.transports[&tid].rtc.media_traffic_stats();
                        let mut corrupt = p.contents.to_vec();
                        *corrupt.last_mut().unwrap() ^= 0x80;
                        engine.handle_datagram(Datagram {
                            transport: tid,
                            proto: Proto::Udp,
                            source: client_addr,
                            destination: server_addr,
                            data: &corrupt,
                            received_at: now,
                        });
                        assert_eq!(
                            engine.transports[&tid].rtc.media_traffic_stats().bytes_in,
                            before.bytes_in,
                            "bad SRTP tag must never increment media NodeStats"
                        );
                        engine.handle_datagram(Datagram {
                            transport: tid,
                            proto: Proto::Udp,
                            source: client_addr,
                            destination: server_addr,
                            data: &p.contents,
                            received_at: now,
                        });
                        let first = engine.transports[&tid].rtc.media_traffic_stats();
                        assert!(first.bytes_in > before.bytes_in);
                        let tag = engine.transports[&tid]
                            .rtc
                            .receive_srtp_auth_tag_len()
                            .unwrap();
                        assert_eq!(
                            first.bytes_in - before.bytes_in,
                            (p.contents.len() - tag) as u64,
                            "real RTP header/payload only, no SRTP authentication tag"
                        );
                        engine.handle_datagram(Datagram {
                            transport: tid,
                            proto: Proto::Udp,
                            source: client_addr,
                            destination: server_addr,
                            data: &p.contents,
                            received_at: now,
                        });
                        let repeated = engine.transports[&tid].rtc.media_traffic_stats();
                        assert_eq!(
                            repeated.bytes_in - first.bytes_in,
                            first.bytes_in - before.bytes_in,
                            "authenticated duplicate bytes follow fixed Go accounting, without delivering the frame twice"
                        );
                        authenticated_duplicate = Some(first.bytes_in - before.bytes_in);
                    } else {
                        engine.handle_datagram(Datagram {
                            transport: tid,
                            proto: Proto::Udp,
                            source: client_addr,
                            destination: server_addr,
                            data: &p.contents,
                            received_at: now,
                        });
                    }
                }
                Output::Event(Event::MediaData(d)) if d.mid == rx_mid => {
                    assert_eq!(d.data.as_ref(), opus.as_slice());
                    received_opus = true;
                }
                Output::Event(Event::RawPacket(packet)) if nack_loss => match *packet {
                    str0m::rtp::RawPacket::RtcpTx(str0m::rtp::rtcp::Rtcp::Nack(_)) => {
                        nack_sent = true
                    }
                    str0m::rtp::RawPacket::RtpRx(header, _)
                        if dropped_audio == Some((*header.ssrc, header.sequence_number)) =>
                    {
                        assert_eq!(header.payload_type, str0m::media::Pt::from(111));
                        recovered_audio = true;
                    }
                    _ => {}
                },
                Output::Event(Event::ChannelData(d)) => {
                    assert_eq!(d.data, b"reliable-downstream");
                    received_echo = true;
                }
                Output::Timeout(_) => break,
                _ => {}
            }
        }
        engine.handle_timeout(now);
        let mut events = vec![];
        engine.drain_events(&mut events);
        for ev in events {
            if let MediaEvent::DataReceived {
                kind,
                label,
                payload,
                ..
            } = ev
            {
                assert_eq!(kind, DataKind::Reliable);
                assert_eq!(
                    label,
                    if data_track {
                        "_data_track"
                    } else {
                        "_reliable"
                    }
                );
                assert_eq!(payload.as_slice(), b"reliable-upstream");
                got_server_data = true;
            }
        }
        if got_server_data && !received_echo {
            if data_track {
                engine.send_data_track(&ParticipantSid::from("PA_test"), b"reliable-downstream");
            } else {
                engine.send_data(
                    &ParticipantSid::from("PA_test"),
                    DataKind::Reliable,
                    b"reliable-downstream",
                );
            }
            engine.run(now);
        }
        let outbound: Vec<_> = packets.0.lock().unwrap().drain(..).collect();
        for bytes in outbound {
            if nack_loss
                && dropped_audio.is_none()
                && tick >= 50
                && bytes.len() >= 12
                && bytes[0] >> 6 == 2
                && bytes[1] & 0x7f == 111
            {
                dropped_audio = Some((
                    u32::from_be_bytes(bytes[8..12].try_into().unwrap()),
                    u16::from_be_bytes(bytes[2..4].try_into().unwrap()),
                ));
                continue; // Drop exactly one actual encrypted primary RTP packet.
            }
            client
                .handle_input(Input::Receive(
                    now,
                    Receive::new(Protocol::Udp, server_addr, client_addr, &bytes).unwrap(),
                ))
                .unwrap();
        }
        if received_opus
            && received_echo
            && got_server_data
            && (!nack_loss || (dropped_audio.is_some() && nack_sent && recovered_audio))
        {
            let stats = engine.stats();
            let client_media = client.media_traffic_stats();
            let server_media = engine.transports[&tid].rtc.media_traffic_stats();
            assert_eq!(
                server_media.bytes_in,
                client_media.bytes_out + authenticated_duplicate.unwrap(),
                "RTP logical header+payload totals exclude ICE/DTLS/SRTCP/data and include the one authenticated duplicate"
            );
            assert_eq!(server_media.packets_in, client_media.packets_out + 1);
            assert!(
                stats.raw_bytes_in > server_media.bytes_in
                    && stats.raw_packets_in > server_media.packets_in
            );
            assert!(stats.data_packets_out > 0);
            assert_eq!(
                stats.data_bytes_out,
                stats.data_packets_out * b"reliable-downstream".len() as u64,
                "one accepted application message is counted once regardless of SCTP fragmentation/retry"
            );
            if nack_loss {
                assert!(
                    stats.retransmit_packets_out > 0 && stats.retransmit_bytes_out > 0,
                    "actual same-SSRC Opus NACK retry enters the subset, without a fake RTX prefix"
                );
                assert!(server_media.nacks_received > 0);
                assert_eq!(server_media.nacks_received, client_media.nacks_sent);
            }
            engine.handle_command(rtc_core::media_api::MediaCommand::CloseTransport(tid), now);
            let terminal = engine.stats();
            assert_eq!(terminal.transports, 0);
            assert_eq!(
                terminal.bytes_in, server_media.bytes_in,
                "source's terminal 5s-delta flush must include all authenticated primary traffic exactly once"
            );
            assert_eq!(terminal.packets_in, server_media.packets_in);
            assert_eq!(
                terminal.nacks,
                server_media.nacks_received + server_media.nacks_sent
            );
            assert!(terminal.bytes_out >= stats.bytes_out);
            assert_eq!(
                engine.stats().bytes_in,
                terminal.bytes_in,
                "terminal sampling must not double count retained transport traffic"
            );
            return;
        }
    }
    panic!(
        "media path did not converge: connected={}, opus={received_opus}, upstream_data={got_server_data}, downstream_data={received_echo}, dropped={dropped_audio:?}, nack={nack_sent}, recovered={recovered_audio}",
        client.is_connected()
    );
}

type RoutedPackets = std::collections::VecDeque<(std::net::SocketAddr, Vec<u8>)>;

#[derive(Clone, Default)]
struct RoutedPacketQueue(std::sync::Arc<std::sync::Mutex<RoutedPackets>>);
impl PacketOut for RoutedPacketQueue {
    fn send(
        &self,
        _: Proto,
        _: std::net::SocketAddr,
        destination: std::net::SocketAddr,
        data: &[u8],
    ) {
        self.0
            .lock()
            .unwrap()
            .push_back((destination, data.to_vec()));
    }
}

fn answer_server_offer(engine: &mut MediaEngine, client: &mut Rtc, tid: TransportId, now: Instant) {
    let _ = answer_server_offer_events(engine, client, tid, now);
}

// Preserve setup events for harnesses that assert the complete dynacast sequence.
fn answer_server_offer_events(
    engine: &mut MediaEngine,
    client: &mut Rtc,
    tid: TransportId,
    now: Instant,
) -> Vec<MediaEvent> {
    let mut events = vec![];
    engine.drain_events(&mut events);
    let (sdp, id) = events
        .iter()
        .find_map(|e| match e {
            MediaEvent::LocalDescription {
                transport,
                kind: SdpType::Offer,
                sdp,
                id,
                ..
            } if *transport == tid => Some((sdp, *id)),
            _ => None,
        })
        .expect("subscriber transport emits a serialized server offer");
    let answer = client
        .sdp_api()
        .accept_offer(str0m::change::SdpOffer::from_sdp_string(sdp).unwrap())
        .unwrap();
    engine.on_remote_description(tid, SdpType::Answer, answer.to_sdp_string(), id, now);
    engine.run(now);
    events
}

#[test]
fn dual_pc_publisher_and_subscriber_exchange_opus_and_reliable_data() {
    use crate::net::Datagram;
    use rtc_core::media_api::DownTrackSpec;
    use rtc_core::model::TrackSettings;
    use str0m::media::{Frequency, MediaTime};
    use str0m::net::{Protocol, Receive};
    use str0m::{Candidate, Event, Input, Output};

    let start = Instant::now();
    let mut engine = engine(start);
    let packets = RoutedPacketQueue::default();
    engine.out = Box::new(packets.clone());
    let pub_tid = transport(&mut engine, PcRole::Publisher, start);
    engine.transports.get_mut(&pub_tid).unwrap().spec.primary = false;
    let server_addr: std::net::SocketAddr = "127.0.0.1:9101".parse().unwrap();
    let pub_addr: std::net::SocketAddr = "127.0.0.1:9102".parse().unwrap();
    let sub_addr: std::net::SocketAddr = "127.0.0.1:9103".parse().unwrap();
    let t = engine.transports.get_mut(&pub_tid).unwrap();
    let c = t
        .rtc
        .add_local_candidate(Candidate::host(server_addr, "udp").unwrap())
        .unwrap()
        .clone();
    t.candidates.push(c);
    let mut publisher = client(start);
    publisher.add_local_candidate(Candidate::host(pub_addr, "udp").unwrap());
    let mut upstream_channel = None;
    let tx_mid = published_mid(&offer(
        &mut engine,
        &mut publisher,
        pub_tid,
        start,
        |change| {
            change.add_media(MediaKind::Audio, Direction::SendOnly, None, None, None);
            upstream_channel = Some(change.add_channel("_reliable".into()));
        },
    ));
    let track = TrackSid::from("TR_dual");
    engine.bind_track(pub_tid, &tx_mid.to_string(), track.clone(), start);

    let sub_tid = TransportId::next();
    engine.create_transport(
        TransportSpec {
            ice_preference: Default::default(),
            one_shot: false,
            id: sub_tid,
            participant: ParticipantSid::from("PA_test"),
            role: PcRole::Subscriber,
            ice_lite: true,
            server_data_channels: true,
            enable_data_tracks: true,
            primary: true,
            client_supports_red: false,
            codec_switch_requires_renegotiation: false,
            disable_playout_delay: false,
            allow_pause: true,
        },
        IceCredentials::random(),
        vec![crate::net::LocalCandidate {
            proto: Proto::Udp,
            addr: server_addr,
            base: server_addr,
        }],
        start,
    );
    let mut subscriber = client(start);
    subscriber.add_local_candidate(Candidate::host(sub_addr, "udp").unwrap());
    answer_server_offer(&mut engine, &mut subscriber, sub_tid, start);
    engine.subscribe(
        DownTrackSpec {
            subscriber: ParticipantSid::from("PA_test"),
            publisher: ParticipantSid::from("PA_test"),
            track_sid: track.clone(),
            stream_id: "PA_test|TR_dual".into(),
            settings: TrackSettings::default(),
            adaptive_stream: false,
        },
        start,
    );
    let negotiation_at = start + Duration::from_millis(160);
    engine.handle_timeout(negotiation_at);
    answer_server_offer(&mut engine, &mut subscriber, sub_tid, negotiation_at);
    let expected_mid = engine.downs.iter().next().unwrap().1.mid.unwrap();
    let opus = vec![0xf8, 0xff, 0xfe];
    let mut received_audio = false;
    let mut sent_data = false;
    let mut received_upstream = false;
    let mut received_downstream = false;
    let upstream_channel = upstream_channel.unwrap();
    for tick in 0..3000u64 {
        let now = negotiation_at + Duration::from_millis(tick * 10);
        publisher.handle_input(Input::Timeout(now)).unwrap();
        subscriber.handle_input(Input::Timeout(now)).unwrap();
        if publisher.is_connected() {
            if !sent_data && let Some(mut c) = publisher.channel(upstream_channel) {
                sent_data = c.write(true, b"dual-upstream").unwrap();
            }
            if tick % 2 == 0 {
                let writer = publisher.writer(tx_mid).unwrap();
                let pt = writer
                    .payload_params()
                    .find(|p| p.spec().codec == str0m::format::Codec::Opus)
                    .unwrap()
                    .pt();
                writer
                    .write(
                        pt,
                        now,
                        MediaTime::new(tick * 480, Frequency::FORTY_EIGHT_KHZ),
                        opus.clone(),
                    )
                    .unwrap();
            }
        }
        for (peer, tid, addr, receiving) in [
            (&mut publisher, pub_tid, pub_addr, false),
            (&mut subscriber, sub_tid, sub_addr, true),
        ] {
            loop {
                match peer.poll_output().unwrap() {
                    Output::Transmit(p) => engine.handle_datagram(Datagram {
                        transport: tid,
                        proto: Proto::Udp,
                        source: addr,
                        destination: server_addr,
                        data: &p.contents,
                        received_at: now,
                    }),
                    Output::Event(Event::MediaData(d)) if receiving && d.mid == expected_mid => {
                        assert_eq!(d.data.as_ref(), opus.as_slice());
                        received_audio = true;
                    }
                    Output::Event(Event::ChannelData(d)) if receiving => {
                        assert_eq!(d.data, b"dual-downstream");
                        received_downstream = true;
                    }
                    Output::Timeout(_) => break,
                    _ => {}
                }
            }
        }
        engine.handle_timeout(now);
        let mut events = vec![];
        engine.drain_events(&mut events);
        for event in events {
            if let MediaEvent::DataReceived { kind, payload, .. } = event {
                assert_eq!(kind, DataKind::Reliable);
                assert_eq!(payload, b"dual-upstream");
                received_upstream = true;
            }
        }
        if received_upstream && !received_downstream {
            engine.send_data(
                &ParticipantSid::from("PA_test"),
                DataKind::Reliable,
                b"dual-downstream",
            );
            engine.run(now);
        }
        let outbound: Vec<_> = packets.0.lock().unwrap().drain(..).collect();
        for (destination, bytes) in outbound {
            let peer = if destination == pub_addr {
                &mut publisher
            } else {
                assert_eq!(destination, sub_addr);
                &mut subscriber
            };
            peer.handle_input(Input::Receive(
                now,
                Receive::new(Protocol::Udp, server_addr, destination, &bytes).unwrap(),
            ))
            .unwrap();
        }
        if received_audio && received_upstream && received_downstream {
            return;
        }
    }
    panic!(
        "dual-PC path stalled: publisher={}, subscriber={}, opus={received_audio}, upstream={received_upstream}, downstream={received_downstream}",
        publisher.is_connected(),
        subscriber.is_connected()
    );
}

#[test]
fn single_pc_video_answer_preserves_publish_bitrate_hint_and_idle_send_sections() {
    let now = Instant::now();
    let mut engine = engine(now);
    engine.cfg.enabled_codecs.push(CodecSpec {
        mime: "video/vp8".into(),
        fmtp_line: String::new(),
    });
    let tid = transport(&mut engine, PcRole::Single, now);
    let mut client = client(now);
    let mut change = client.sdp_api();
    let placeholder = change.add_media(MediaKind::Video, Direction::RecvOnly, None, None, None);
    let published = change.add_media(MediaKind::Video, Direction::SendOnly, None, None, None);
    let (offer, _) = change.apply().unwrap();
    let offered = crate::sdp_munge::map_sections(&offer.to_sdp_string(), |mid, section| {
        (mid == Some(published.to_string().as_str())).then(|| {
            section.replace(
                "a=rtpmap:96 VP8/90000\r\n",
                "a=rtpmap:96 VP8/90000\r\na=fmtp:96 x-google-start-bitrate=549\r\n",
            )
        })
    });
    engine.on_remote_description(tid, SdpType::Offer, offered.clone(), 1, now);
    let mut events = vec![];
    engine.drain_events(&mut events);
    let answer = events
        .iter()
        .find_map(|e| match e {
            MediaEvent::LocalDescription {
                kind: SdpType::Answer,
                sdp,
                ..
            } => Some(sdp),
            _ => None,
        })
        .unwrap();
    for section in crate::sdp_munge::split_sections(answer).1 {
        if crate::sdp_munge::section_mid(section) == Some(published.to_string().as_str()) {
            assert!(section.contains("a=fmtp:96 x-google-start-bitrate=549\r\n"));
            assert!(section.contains("a=recvonly\r\n"));
        } else if crate::sdp_munge::section_mid(section) == Some(placeholder.to_string().as_str()) {
            assert!(section.contains("a=sendonly\r\n"));
            assert!(!section.contains("a=fmtp:96 "));
            assert!(!section.contains("a=msid:"));
            assert!(!section.contains("a=ssrc:"));
        }
    }
    // Repeated offers must keep the same per-m-line hint, not leak it to the
    // recvonly placeholder (which has the same PT but the opposite direction).
    engine.on_remote_description(tid, SdpType::Offer, offered, 2, now);
    events.clear();
    engine.drain_events(&mut events);
    let repeated = events
        .iter()
        .find_map(|e| match e {
            MediaEvent::LocalDescription {
                kind: SdpType::Answer,
                sdp,
                ..
            } => Some(sdp),
            _ => None,
        })
        .unwrap();
    assert_eq!(repeated.matches("x-google-start-bitrate=549").count(), 1);
}

#[test]
fn actual_av1_answer_retains_hint_on_retired_mid_without_leaking_to_new_mid() {
    use crate::sdp_munge::{map_sections, section_direction, section_mid, split_sections};

    let mut missing_inactive_hints = Vec::new();
    for role in [PcRole::Single, PcRole::Publisher] {
        let now = Instant::now();
        let mut engine = engine(now);
        engine.cfg.enabled_codecs.push(CodecSpec {
            mime: "video/av1".into(),
            fmtp_line: String::new(),
        });
        let tid = transport(&mut engine, role, now);
        let mut peer = client(now);
        let mut change = peer.sdp_api();
        let subscriber = (role == PcRole::Single)
            .then(|| change.add_media(MediaKind::Video, Direction::RecvOnly, None, None, None));
        let original = change.add_media(MediaKind::Video, Direction::SendOnly, None, None, None);
        let (initial_offer, pending) = change.apply().expect("actual AV1 offer");
        let bitrate = if role == PcRole::Single { 1000 } else { 987 };
        let hint = format!("x-google-start-bitrate={bitrate}");
        let chrome_fmtp = |sdp: &str, initial: bool| {
            map_sections(sdp, |mid, section| {
                let rtpmap = section
                    .lines()
                    .find(|line| line.eq_ignore_ascii_case("a=rtpmap:45 AV1/90000"))?;
                let mut fmtp = "a=fmtp:45 level-idx=5;profile=0;tier=0".to_owned();
                // The recorded Chromium single-PC offer contains the same hint
                // on its receive-only placeholder and on its actual publisher.
                if initial
                    && (mid == Some(original.to_string().as_str())
                        || subscriber.is_some_and(|id| mid == Some(id.to_string().as_str())))
                {
                    fmtp.push(';');
                    fmtp.push_str(&hint);
                }
                Some(
                    if let Some(previous) =
                        section.lines().find(|line| line.starts_with("a=fmtp:45 "))
                    {
                        section.replace(previous, &fmtp)
                    } else {
                        section.replace(rtpmap, &format!("{rtpmap}\r\n{fmtp}"))
                    },
                )
            })
        };
        let initial_offer = chrome_fmtp(&initial_offer.to_sdp_string(), true);

        let answer = |engine: &mut MediaEngine, sdp: String, sequence| {
            engine.on_remote_description(tid, SdpType::Offer, sdp, sequence, now);
            let mut events = vec![];
            engine.drain_events(&mut events);
            events
                .into_iter()
                .find_map(|event| match event {
                    MediaEvent::LocalDescription {
                        kind: SdpType::Answer,
                        sdp,
                        ..
                    } => Some(sdp),
                    _ => None,
                })
                .expect("server emitted its real local SDP answer")
        };
        let fmtp = |phase: &str, sdp: &str, mid: Mid, direction: &str| {
            let section = split_sections(sdp)
                .1
                .into_iter()
                .find(|section| section_mid(section) == Some(mid.to_string().as_str()))
                .unwrap_or_else(|| panic!("{role:?} {phase}: actual answer is missing MID {mid}"));
            assert_eq!(
                section_direction(section),
                Some(direction),
                "{role:?} {phase} MID {mid} actual direction",
            );
            assert!(
                section
                    .lines()
                    .any(|line| line.eq_ignore_ascii_case("a=rtpmap:45 AV1/90000")),
                "{role:?} {phase} MID {mid} must advertise actual AV1 PT45",
            );
            let mut parameters = section
                .lines()
                .filter_map(|line| line.strip_prefix("a=fmtp:45 "));
            let fmtp = parameters.next().map(str::to_owned);
            assert!(
                parameters.next().is_none(),
                "{role:?} {phase} MID {mid} duplicate fmtp"
            );
            println!(
                "AV1_SDP role={role:?} phase={phase} mid={mid} direction={direction} pt=45 fmtp={fmtp:?}"
            );
            fmtp
        };
        let has_expected_hint = |params: Option<&str>| {
            params.is_some_and(|params| params.split(';').any(|value| value.trim() == hint))
        };
        let assert_unhinted = |phase, params: Option<String>| {
            assert!(
                params.is_none_or(|params| !params.contains("x-google-start-bitrate")),
                "{role:?} {phase} must not inherit the original publishing hint",
            );
        };

        let initial = answer(&mut engine, initial_offer, 1);
        let initial_fmtp = fmtp("initial_publisher", &initial, original, "recvonly");
        assert!(
            has_expected_hint(initial_fmtp.as_deref()),
            "{role:?} initial published MID must carry the exact supplied hint",
        );
        if let Some(subscriber) = subscriber {
            assert_unhinted(
                "initial_subscriber",
                fmtp("initial_subscriber", &initial, subscriber, "sendonly"),
            );
        }
        peer.sdp_api()
            .accept_answer(pending, SdpAnswer::from_sdp_string(&initial).unwrap())
            .unwrap();
        let original_track = TrackSid::from("TR_av1_original_bitrate");
        engine.bind_track(tid, &original.to_string(), original_track.clone(), now);
        engine.unpublish(&original_track, now);
        let mut change = peer.sdp_api();
        change.set_direction(original, Direction::Inactive);
        let replacement = change.add_media(MediaKind::Video, Direction::SendOnly, None, None, None);
        assert_ne!(original, replacement);
        let (replacement_offer, pending) = change.apply().expect("actual replacement offer");
        let replacement_offer = chrome_fmtp(&replacement_offer.to_sdp_string(), false);
        assert!(!replacement_offer.contains("x-google-start-bitrate"));
        let replacement_answer = answer(&mut engine, replacement_offer, 2);
        assert_unhinted(
            "replacement_publisher",
            fmtp(
                "replacement_publisher",
                &replacement_answer,
                replacement,
                "recvonly",
            ),
        );
        if let Some(subscriber) = subscriber {
            assert_unhinted(
                "replacement_subscriber",
                fmtp(
                    "replacement_subscriber",
                    &replacement_answer,
                    subscriber,
                    "sendonly",
                ),
            );
        }
        let inactive_fmtp = fmtp(
            "retired_publisher",
            &replacement_answer,
            original,
            "inactive",
        );
        if !has_expected_hint(inactive_fmtp.as_deref()) {
            missing_inactive_hints.push((role, original, inactive_fmtp));
        }
        peer.sdp_api()
            .accept_answer(
                pending,
                SdpAnswer::from_sdp_string(&replacement_answer).unwrap(),
            )
            .unwrap();
    }
    assert!(
        missing_inactive_hints.is_empty(),
        "fixed Go retains the exact original negotiated hint on both inactive MIDs: {missing_inactive_hints:?}",
    );
}

/// Real ICE/DTLS/SRTP/TWCC endpoints over a deterministic bounded packet link.
/// No fake capacity override is used: the subscriber's feedback drives BWE.
#[test]
fn bandwidth_limited_webrtc_link_pauses_and_recovers_video() {
    use crate::net::Datagram;
    use rtc_core::media_api::DownTrackSpec;
    use rtc_core::model::TrackSettings;
    use std::collections::VecDeque;
    use str0m::media::{Frequency, MediaTime};
    use str0m::net::{Protocol, Receive};
    use str0m::{Candidate, Event, Input, Output};

    let start = Instant::now();
    let mut engine = engine(start);
    engine.cfg.playout_delay = Some((30, 90));
    engine.cfg.enabled_codecs.push(CodecSpec {
        mime: "video/vp8".into(),
        fmtp_line: String::new(),
    });
    let packets = RoutedPacketQueue::default();
    engine.out = Box::new(packets.clone());
    let pub_tid = transport(&mut engine, PcRole::Publisher, start);
    let server_addr = "127.0.0.1:9301".parse().unwrap();
    let pub_addr = "127.0.0.1:9302".parse().unwrap();
    let sub_addr = "127.0.0.1:9303".parse().unwrap();
    let t = engine.transports.get_mut(&pub_tid).unwrap();
    let candidate = t
        .rtc
        .add_local_candidate(Candidate::host(server_addr, "udp").unwrap())
        .unwrap()
        .clone();
    t.candidates.push(candidate);
    t.spec.primary = false;
    let mut publisher = client(start);
    publisher.add_local_candidate(Candidate::host(pub_addr, "udp").unwrap());
    let tx_mid = published_mid(&offer(
        &mut engine,
        &mut publisher,
        pub_tid,
        start,
        |change| {
            change.add_media(MediaKind::Video, Direction::SendOnly, None, None, None);
        },
    ));
    let track = TrackSid::from("TR_congestion");
    engine.bind_track(pub_tid, &tx_mid.to_string(), track.clone(), start);
    let sub_tid = TransportId::next();
    engine.create_transport(
        TransportSpec {
            ice_preference: Default::default(),
            one_shot: false,
            id: sub_tid,
            participant: "PA_test".into(),
            role: PcRole::Subscriber,
            ice_lite: true,
            server_data_channels: true,
            enable_data_tracks: true,
            primary: true,
            client_supports_red: false,
            codec_switch_requires_renegotiation: false,
            disable_playout_delay: false,
            allow_pause: true,
        },
        IceCredentials::random(),
        vec![crate::net::LocalCandidate {
            proto: Proto::Udp,
            addr: server_addr,
            base: server_addr,
        }],
        start,
    );
    let mut subscriber = Rtc::builder()
        .set_crypto_provider(rtc_crypto::provider().into())
        .set_dtls_cert(rtc_crypto::dtls_cert().unwrap())
        .set_extension(6, str0m::rtp::Extension::PlayoutDelay)
        .build(start);
    subscriber.add_local_candidate(Candidate::host(sub_addr, "udp").unwrap());
    answer_server_offer(&mut engine, &mut subscriber, sub_tid, start);
    engine.subscribe(
        DownTrackSpec {
            subscriber: "PA_test".into(),
            publisher: "PA_test".into(),
            track_sid: track,
            stream_id: "PA_test|TR_congestion".into(),
            settings: TrackSettings::default(),
            adaptive_stream: true,
        },
        start,
    );
    let negotiated = start + Duration::from_millis(160);
    engine.handle_timeout(negotiated);
    answer_server_offer(&mut engine, &mut subscriber, sub_tid, negotiated);

    let mut queue = VecDeque::<Vec<u8>>::new();
    let mut queued = 0usize;
    let mut tokens = 0u64;
    let mut frames_before = 0;
    let mut frames_recovered = 0;
    let mut paused = false;
    let mut dropped = 0;
    let mut throttled_ticks = 0;
    let mut lowest_budget = u64::MAX;
    // Synthetic VP8 keyframe body. Endpoints validate RTP framing/feedback, not
    // video pixels; decoded browser video has its separate official-SDK gate.
    let mut keyframe = vec![0u8; 2200];
    keyframe[..10].copy_from_slice(&[0x10, 0, 0, 0x9d, 1, 0x2a, 0x80, 2, 0x68, 1]);
    for tick in 0..70000u64 {
        let now = negotiated + Duration::from_millis(tick);
        publisher.handle_input(Input::Timeout(now)).unwrap();
        subscriber.handle_input(Input::Timeout(now)).unwrap();
        if publisher.is_connected() && tick.is_multiple_of(30) {
            let writer = publisher.writer(tx_mid).unwrap();
            let pt = writer
                .payload_params()
                .find(|p| p.spec().codec == str0m::format::Codec::Vp8)
                .unwrap()
                .pt();
            writer
                .write(
                    pt,
                    now,
                    MediaTime::new(tick * 90, Frequency::NINETY_KHZ),
                    keyframe.clone(),
                )
                .unwrap();
        }
        for (peer, tid, addr, receiving) in [
            (&mut publisher, pub_tid, pub_addr, false),
            (&mut subscriber, sub_tid, sub_addr, true),
        ] {
            loop {
                match peer.poll_output().unwrap() {
                    Output::Transmit(p) => engine.handle_datagram(Datagram {
                        transport: tid,
                        proto: Proto::Udp,
                        source: addr,
                        destination: server_addr,
                        data: &p.contents,
                        received_at: now,
                    }),
                    Output::Event(Event::MediaData(data)) if receiving => {
                        // Assert actual decrypted/parsed forwarded RTP metadata,
                        // rather than treating the engine config getter as wire proof.
                        assert_eq!(
                            data.ext_vals.play_delay_min,
                            Some(MediaTime::from_millis(30))
                        );
                        assert_eq!(
                            data.ext_vals.play_delay_max,
                            Some(MediaTime::from_millis(90))
                        );
                        if tick < 5000 {
                            frames_before += 1;
                        }
                        if tick >= 45000 {
                            frames_recovered += 1;
                        }
                    }
                    Output::Timeout(_) => break,
                    _ => {}
                }
            }
        }
        engine.handle_timeout(now);
        engine.drain_events(&mut vec![]);
        let capacity: u64 = if (5000..25000).contains(&tick) {
            100_000
        } else {
            3_000_000
        };
        tokens = tokens
            .saturating_add(capacity / 8000)
            .min((capacity / 32).max(1500));
        for (destination, bytes) in packets.0.lock().unwrap().drain(..) {
            if destination == pub_addr {
                publisher
                    .handle_input(Input::Receive(
                        now,
                        Receive::new(Protocol::Udp, server_addr, destination, &bytes).unwrap(),
                    ))
                    .unwrap();
            } else {
                assert_eq!(destination, sub_addr);
                if queued + bytes.len() > (capacity / 32).max(1500) as usize {
                    dropped += 1;
                } else {
                    queued += bytes.len();
                    queue.push_back(bytes);
                }
            }
        }
        if queue
            .front()
            .is_some_and(|packet| packet.len() as u64 > tokens)
        {
            throttled_ticks += 1;
        }
        while queue
            .front()
            .is_some_and(|packet| packet.len() as u64 <= tokens)
        {
            let packet = queue.pop_front().unwrap();
            queued -= packet.len();
            tokens -= packet.len() as u64;
            subscriber
                .handle_input(Input::Receive(
                    now,
                    Receive::new(Protocol::Udp, server_addr, sub_addr, &packet).unwrap(),
                ))
                .unwrap();
        }
        if (10000..25000).contains(&tick) {
            let t = &engine.transports[&sub_tid];
            paused |= t.paused_video_tracks > 0;
            lowest_budget = lowest_budget.min(t.capacity().unwrap_or(u64::MAX));
        }
    }
    let stats = engine.stats();
    eprintln!(
        "bounded-link frames before={frames_before}, recovered={frames_recovered}, throttled={throttled_ticks}, drops={dropped}, min_capacity={lowest_budget}, final_capacity={:?}, probes={}/{}/{}",
        engine.transports[&sub_tid].capacity(),
        stats.probe_attempts,
        stats.probe_successes,
        stats.probe_failures
    );
    let send_side = engine.transports[&sub_tid]
        .send_side
        .as_ref()
        .expect("native mode owns the send-side estimator")
        .stats();
    eprintln!("probe padding stats: {send_side:?}");
    assert!(
        frames_before > 30,
        "initial media did not flow: {frames_before}"
    );
    assert!(
        throttled_ticks > 0 && lowest_budget < 500_000,
        "link impairment not observed: throttled={throttled_ticks}, dropped={dropped}, budget={lowest_budget}, probes={}",
        stats.probe_attempts
    );
    assert!(
        paused,
        "video never paused under 100kbps capacity: budget={lowest_budget}"
    );
    assert!(
        stats.probe_attempts > 0,
        "no real TWCC probe demand was issued"
    );
    assert!(
        stats.probe_successes > 0,
        "no probe confirmed capacity through TWCC feedback"
    );
    assert!(
        frames_recovered > 30,
        "video failed to recover: frames={frames_recovered}, budget={:?}, probes={}/{}/{}",
        engine.transports[&sub_tid].capacity(),
        stats.probe_attempts,
        stats.probe_successes,
        stats.probe_failures
    );
    // Upstream `handleSignalSendProbe` charges the prober with what
    // `WriteProbePackets` actually returned, so a bucket that advances is
    // evidence of bytes on the wire, not of a request. Here the producer of
    // those bytes is the vendor probe window opened by the cluster switch, and
    // the meter is the authenticated send-side boundary, so the same claim is
    // checkable only against emitted padding datagrams.
    assert!(
        send_side.padding_packets > 0 && send_side.padding_bytes > 0,
        "the probe window opened but never emitted a padding datagram: {send_side:?}"
    );
    assert!(
        send_side.probe_bucket_bytes > 0,
        "padding was emitted but nothing reached the prober bucket, so the cluster \
         goal was never charged and cannot have been met by traffic: {send_side:?}"
    );
    assert_eq!(
        send_side.probe_bucket_bytes + send_side.uncharged_padding_bytes,
        send_side.padding_bytes,
        "every emitted padding octet must be accounted exactly once, either charged \
         to the open cluster or recorded as uncharged: {send_side:?}"
    );
    // The charge is gated on the cluster stamp that the switch installs, which
    // is Go's per-track `SetProbeClusterId` in `handleSignalProbeClusterSwitch`.
    // Without an accepted cluster there is nothing to charge, so a bucket that
    // advanced while no cluster started would mean padding was billed to a
    // cluster that never existed.
    assert!(
        send_side.cluster_starts > 0,
        "a charged bucket requires an accepted cluster start: {send_side:?}"
    );
}

#[test]
fn add_track_vp9_primary_selects_vp9_even_when_browser_offers_vp8_first() {
    use rtc_core::media_api::{MediaCommand, PublishedTrackSpec};
    use rtc_core::model::{TrackCodec, TrackKind, TrackSource};
    let now = Instant::now();
    let mut engine = engine(now);
    for mime in ["video/vp8", "video/vp9", "video/rtx"] {
        engine.cfg.enabled_codecs.push(CodecSpec {
            mime: mime.into(),
            fmtp_line: String::new(),
        });
    }
    let tid = transport(&mut engine, PcRole::Single, now);
    engine.handle_command(
        MediaCommand::ExpectTrack {
            transport: tid,
            track: PublishedTrackSpec {
                backup_codec_policy: Default::default(),
                sid: TrackSid::from("TR_vp9"),
                kind: TrackKind::Video,
                source: TrackSource::Camera,
                cid: "vp9-camera".into(),
                layers: vec![],
                codecs: vec![TrackCodec {
                    mime_type: "video/vp9".into(),
                    cid: "vp9-camera".into(),
                    ..Default::default()
                }],
                stereo: false,
                disable_dtx: false,
                disable_red: false,
                encrypted: false,
                muted: false,
            },
        },
        now,
    );
    let mut client = client(now);
    let mut change = client.sdp_api();
    let mid = change.add_media(
        MediaKind::Video,
        Direction::SendOnly,
        None,
        Some("vp9-camera".into()),
        None,
    );
    let (offer, pending) = change.apply().unwrap();
    let offered = offer.to_sdp_string();
    let offered_section = crate::sdp_munge::split_sections(&offered).1[0];
    let offered_first = offered_section
        .lines()
        .next()
        .unwrap()
        .split_whitespace()
        .nth(3)
        .unwrap();
    assert!(
        offered_section.contains(&format!("a=rtpmap:{offered_first} VP8/90000")),
        "fixture must reproduce the browser's VP8-first offer"
    );
    engine.on_remote_description(tid, SdpType::Offer, offered, 1, now);
    let mut events = vec![];
    engine.drain_events(&mut events);
    let answer = events
        .iter()
        .find_map(|event| match event {
            MediaEvent::LocalDescription {
                kind: SdpType::Answer,
                sdp,
                ..
            } => Some(sdp),
            _ => None,
        })
        .unwrap();
    let section = crate::sdp_munge::split_sections(answer)
        .1
        .into_iter()
        .find(|section| crate::sdp_munge::section_mid(section) == Some(mid.to_string().as_str()))
        .unwrap();
    let pt = section
        .lines()
        .next()
        .unwrap()
        .split_whitespace()
        .nth(3)
        .unwrap();
    assert!(
        section.contains(&format!("a=rtpmap:{pt} VP9/90000")),
        "AddTrack codec must lead the answer: {section}"
    );
    client
        .sdp_api()
        .accept_answer(pending, SdpAnswer::from_sdp_string(answer).unwrap())
        .unwrap();
}

/// Exercises vendor metadata preservation and output fragment flags through
/// real public str0m APIs, ICE/DTLS/SRTP, and the engine's AV1 forwarding path.
/// The AV1 bytes are syntactically valid OBUs, not a pixel-decoding fixture;
/// real Chrome decoder acceptance is a separate interoperability gate.
#[test]
fn av1_fragmented_dd_metadata_survives_public_webrtc_frame_pipeline() {
    use crate::net::Datagram;
    use rtc_core::media_api::DownTrackSpec;
    use rtc_core::model::TrackSettings;
    use std::sync::{Arc, Mutex};
    use str0m::media::{Frequency, MediaTime};
    use str0m::net::{Protocol, Receive};
    use str0m::rtp::{Extension, ExtensionValues, FrameUserExtensions, UserExtensionValues};
    use str0m::{Candidate, Event, Input, Output};
    #[derive(Debug)]
    struct RecordedDd(Arc<Mutex<Vec<Vec<u8>>>>);
    impl str0m::rtp::ExtensionSerializer for RecordedDd {
        fn write_to(&self, out: &mut [u8], values: &ExtensionValues) -> usize {
            let n = str0m::rtp::ExtensionSerializer::write_to(&crate::av1::Serializer, out, values);
            if n > 0 {
                self.0.lock().unwrap().push(out[..n].to_vec());
            }
            n
        }
        fn parse_value(&self, input: &[u8], values: &mut ExtensionValues) -> bool {
            str0m::rtp::ExtensionSerializer::parse_value(&crate::av1::Serializer, input, values)
        }
        fn is_video(&self) -> bool {
            true
        }
        fn is_audio(&self) -> bool {
            false
        }
        fn requires_two_byte_form(&self, _: &ExtensionValues) -> bool {
            true
        }
    }
    let source_dd = Arc::new(Mutex::new(Vec::new()));
    let start = Instant::now();
    let mut engine = engine(start);
    engine.cfg.enabled_codecs.push(CodecSpec {
        mime: "video/av1".into(),
        fmtp_line: String::new(),
    });
    engine.cfg.rtc.congestion_control.enabled = false;
    let packets = PacketQueue::default();
    engine.out = Box::new(packets.clone());
    let tid = transport(&mut engine, PcRole::Single, start);
    let server_addr = "127.0.0.1:9401".parse().unwrap();
    let client_addr = "127.0.0.1:9402".parse().unwrap();
    let candidate = engine
        .transports
        .get_mut(&tid)
        .unwrap()
        .rtc
        .add_local_candidate(Candidate::host(server_addr, "udp").unwrap())
        .unwrap()
        .clone();
    engine
        .transports
        .get_mut(&tid)
        .unwrap()
        .candidates
        .push(candidate);
    let mut client = Rtc::builder()
        .set_crypto_provider(rtc_crypto::provider().into())
        .set_dtls_cert(rtc_crypto::dtls_cert().unwrap())
        .set_extension(
            13,
            Extension::with_serializer(crate::av1::URI, RecordedDd(source_dd.clone())),
        )
        .build(start);
    client.add_local_candidate(Candidate::host(client_addr, "udp").unwrap());
    let tx_mid = published_mid(&offer(&mut engine, &mut client, tid, start, |change| {
        change.add_media(MediaKind::Video, Direction::SendOnly, None, None, None);
        change.add_channel("_reliable".into());
    }));
    let track = TrackSid::from("TR_dd_pipeline");
    engine.bind_track(tid, &tx_mid.to_string(), track.clone(), start);
    engine.subscribe(
        DownTrackSpec {
            subscriber: "PA_test".into(),
            publisher: "PA_test".into(),
            track_sid: track,
            stream_id: "PA_test|TR_dd_pipeline".into(),
            settings: TrackSettings::default(),
            adaptive_stream: false,
        },
        start,
    );
    let mut rx_mid = None;
    offer(&mut engine, &mut client, tid, start, |change| {
        rx_mid = Some(change.add_media(MediaKind::Video, Direction::RecvOnly, None, None, None));
    });
    let rx_mid = rx_mid.unwrap();
    // One complete independent frame, with an attached S0T0 structure.
    let raw: Arc<[u8]> = Arc::from([192, 0, 0, 128, 0, 59, 64, 128, 119, 128, 67, 128]);
    let mut raw_values = UserExtensionValues::default();
    raw_values.set(crate::av1::RawDescriptor(raw));
    let mut values = ExtensionValues::default();
    values
        .user_values
        .set(FrameUserExtensions(vec![raw_values]));
    let mut data = vec![0x0a, 1, 0, 0x36, 0, 0x80, 0x20]; // sequence OBU + frame OBU, payload length 4096
    data.extend(vec![0; 4096]);
    let picture = crate::av1::Receiver::default()
        .picture(&data, &values, true)
        .unwrap();
    let prepared = crate::av1::Forwarder::default()
        .prepare(&picture, &data, 0)
        .unwrap();
    let mut stage = 0;
    let mut deadline = 0;
    let mut drop_next_media_packet = false;
    let mut dropped_packets = 0;
    let mut blocked_packet = None;
    let mut loss_timestamp = None;
    let mut media_pt = None;
    let mut repair_pt = None;
    let mut saw_initial_dd = false;
    let mut recovery_source_dd_start = None;
    for tick in 0..3000u64 {
        let now = start + Duration::from_millis(tick * 10);
        client.handle_input(Input::Timeout(now)).unwrap();
        if (stage == 2 || stage == 4) && tick >= deadline {
            stage += 1;
        }
        if client.is_connected() && tick.is_multiple_of(20) && matches!(stage, 0 | 1 | 3 | 5) {
            let writer = client.writer(tx_mid).unwrap();
            let pt = writer
                .payload_params()
                .find(|params| params.spec().codec == str0m::format::Codec::Av1)
                .unwrap()
                .pt();
            media_pt = Some(*pt);
            repair_pt = writer
                .payload_params()
                .find(|params| params.pt() == pt)
                .and_then(|params| params.resend())
                .map(|pt| *pt);
            let mut body = data.clone();
            let writer = match stage {
                1 => {
                    *body.last_mut().unwrap() = 1;
                    stage = 2;
                    deadline = tick + 40;
                    writer
                }
                3 => {
                    *body.last_mut().unwrap() = 2;
                    stage = 4;
                    deadline = tick + 60;
                    drop_next_media_packet = true;
                    loss_timestamp = Some((tick * 900) as u32);
                    writer.user_extension_value(prepared.descriptor.clone())
                }
                5 => {
                    recovery_source_dd_start = Some(source_dd.lock().unwrap().len());
                    *body.last_mut().unwrap() = 3;
                    stage = 6;
                    writer.user_extension_value(prepared.descriptor.clone())
                }
                _ => writer.user_extension_value(prepared.descriptor.clone()),
            };
            writer
                .write(
                    pt,
                    now,
                    MediaTime::new(tick * 900, Frequency::NINETY_KHZ),
                    body,
                )
                .unwrap();
        }
        loop {
            match client.poll_output().unwrap() {
                Output::Transmit(packet) => {
                    let bytes = &packet.contents;
                    let media_header = if bytes.len() >= 12
                        && bytes[0] & 0xc0 == 0x80
                        && Some(bytes[1] & 0x7f) == media_pt
                    {
                        Some((
                            u32::from_be_bytes(bytes[4..8].try_into().unwrap()),
                            u16::from_be_bytes(bytes[2..4].try_into().unwrap()),
                        ))
                    } else {
                        None
                    };
                    let blocked_repair = bytes.len() >= 12
                        && bytes[0] & 0xc0 == 0x80
                        && Some(bytes[1] & 0x7f) == repair_pt
                        && Some(u32::from_be_bytes(bytes[4..8].try_into().unwrap()))
                            == loss_timestamp;
                    if blocked_repair
                        || (media_header.is_some()
                            && ((drop_next_media_packet
                                && media_header.map(|header| header.0) == loss_timestamp)
                                || blocked_packet == media_header))
                    {
                        drop_next_media_packet = false;
                        if media_header.is_some() {
                            blocked_packet = media_header;
                        }
                        dropped_packets += 1;
                    } else {
                        engine.handle_datagram(Datagram {
                            transport: tid,
                            proto: Proto::Udp,
                            source: client_addr,
                            destination: server_addr,
                            data: bytes,
                            received_at: now,
                        });
                    }
                }
                Output::Event(Event::MediaData(received)) if received.mid == rx_mid => {
                    let marker = *received.data.last().unwrap();
                    assert!(
                        !matches!(marker, 1 | 2),
                        "frame with missing DD or missing RTP fragment was forwarded: marker={marker}, dropped={dropped_packets}, len={}",
                        received.data.len()
                    );
                    let mut expected = data.clone();
                    *expected.last_mut().unwrap() = marker;
                    assert_eq!(received.data.as_ref(), expected.as_slice());
                    if marker == 0 {
                        let packets = &received
                            .ext_vals
                            .user_values
                            .get::<FrameUserExtensions>()
                            .unwrap()
                            .0;
                        let descriptors: Vec<_> = packets
                            .iter()
                            .filter_map(|values| values.get::<crate::av1::RawDescriptor>())
                            .collect();
                        assert!(descriptors.len() >= 3, "actual fragmented frame required");
                        assert_ne!(descriptors[0].0[0] & 0x80, 0);
                        assert_eq!(descriptors[0].0[0] & 0x40, 0);
                        assert!(
                            descriptors[0].0.len() > 3,
                            "attached template must survive the first packet"
                        );
                        for descriptor in &descriptors[1..descriptors.len() - 1] {
                            assert_eq!(descriptor.0[0] & 0xc0, 0);
                        }
                        assert_eq!(descriptors.last().unwrap().0[0] & 0xc0, 0x40);
                        let source_wire = source_dd.lock().unwrap().clone();
                        assert!(
                            source_wire.iter().any(|raw| raw[0] & 0x80 != 0
                                && raw.len() > 3
                                && raw[3] & 0x80 != 0),
                            "source FrameWriter actually wrote an attached first descriptor"
                        );
                        let following_source: Vec<_> = source_wire
                            .iter()
                            .filter(|raw| raw[0] & 0x80 == 0)
                            .collect();
                        assert!(
                            following_source.len() >= 2
                                && following_source.iter().all(|raw| raw.len() == 3),
                            "actual source continuations are bare three-byte DDs, not repeated attached structures"
                        );
                        // Original DD selector publishes the selected active mask on
                        // every non-attached packet. One target adds one extension
                        // byte, while the attached structure must not be repeated.
                        assert!(
                            descriptors[1..]
                                .iter()
                                .all(|descriptor| descriptor.0.len() == 4
                                    && descriptor.0[3] & 0x80 == 0
                                    && descriptor.0[3] & 0x40 != 0)
                        );
                        saw_initial_dd = true;
                    } else {
                        assert_eq!(
                            marker, 3,
                            "only the complete recovery frame may follow the initial DD frame"
                        );
                        assert!(
                            saw_initial_dd,
                            "initial marker0 must first pass all original fragmented DD metadata assertions"
                        );
                        let records = source_dd.lock().unwrap();
                        let start=recovery_source_dd_start.expect("recovery source frame was actually serialized with its valid descriptor");
                        assert!(
                            records[start..].iter().any(|raw| raw.len() > 3
                                && raw[0] & 0x80 != 0
                                && raw[3] & 0x80 != 0),
                            "recovery source really sent an attached first DD, not an absent source extension"
                        );
                        assert!(
                            received
                                .ext_vals
                                .user_values
                                .get::<crate::av1::RawDescriptor>()
                                .is_none()
                        );
                        assert!(
                            received
                                .ext_vals
                                .user_values
                                .get::<FrameUserExtensions>()
                                .is_none_or(|packets| packets.0.iter().all(|values| values
                                    .get::<crate::av1::RawDescriptor>()
                                    .is_none())),
                            "fixed Go selector remains unfiltered after missingDD: source DD reappearance must not produce receiver DD"
                        );
                    }
                    if marker == 3 {
                        assert!(dropped_packets >= 1);
                        // Same-owner diagnostics must come from this actual
                        // encrypted media exchange, including fragmented RTP.
                        let snapshot = engine.debug_snapshot(now);
                        let published = &snapshot.published[&TrackSid::from("TR_dd_pipeline")];
                        assert_eq!(published.publisher, ParticipantSid::from("PA_test"));
                        assert!(
                            published
                                .receivers
                                .iter()
                                .any(|receiver| receiver.mime == "video/AV1"
                                    && !receiver.up_tracks.is_empty())
                        );
                        let down = &published.down_tracks[0];
                        assert_eq!(down.bind_state, rtc_core::media_debug::BindDebug::Bound);
                        assert!(down.ssrc.is_some());
                        let last = down.last_packet.expect("emitted primary packet");
                        let previous = down.second_last_packet.expect("fragmented primary history");
                        assert!(last.sequence > previous.sequence);
                        assert!(last.marker);
                        if let Some(report) = down.sender_report {
                            assert!(report.packet_count > 0);
                        }
                        return;
                    }
                    if stage == 0 {
                        stage = 1;
                    }
                }
                Output::Timeout(_) => break,
                _ => {}
            }
        }
        engine.handle_timeout(now);
        let outbound: Vec<_> = packets.0.lock().unwrap().drain(..).collect();
        for bytes in outbound {
            client
                .handle_input(Input::Receive(
                    now,
                    Receive::new(Protocol::Udp, server_addr, client_addr, &bytes).unwrap(),
                ))
                .unwrap();
        }
    }
    panic!("AV1/DD public API metadata path did not converge");
}

fn peer_stats_allocation_fixture(start: Instant) -> (MediaEngine, TransportId, usize) {
    let (engine, tid, di, _, _) = peer_stats_connected_fixture(start);
    (engine, tid, di)
}

fn peer_stats_connected_fixture(
    start: Instant,
) -> (MediaEngine, TransportId, usize, Rtc, PacketQueue) {
    use rtc_core::media_api::DownTrackSpec;
    use rtc_core::model::{TrackSettings, VideoLayer, VideoQuality};
    let mut engine = engine(start);
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
    let mut peer = client(start);
    peer.add_local_candidate(
        str0m::Candidate::host("127.0.0.1:9402".parse().unwrap(), "udp").unwrap(),
    );
    let mid = published_mid(&offer(&mut engine, &mut peer, tid, start, |change| {
        change.add_media(MediaKind::Video, Direction::SendOnly, None, None, None);
    }));
    let sid = TrackSid::from("TR_peer_stats_allocation");
    engine.bind_track(tid, &mid.to_string(), sid.clone(), start);
    let ui = engine.up_by_sid[&sid];
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

fn peer_stats_observation(at: Instant, raw: u64) -> str0m::bwe::BweSnapshot {
    use str0m::bwe::{Bitrate, BweEstimateCause, BweFeedbackObservation, BweSnapshot};
    BweSnapshot {
        raw_estimate: Some(Bitrate::bps(raw)),
        acknowledged_bitrate: Some(Bitrate::bps(raw)),
        cause: BweEstimateCause::Normal,
        last_twcc_feedback_at: Some(at),
        latest_feedback: Some(BweFeedbackObservation {
            at,
            received_packets: 10,
            lost_packets: 0,
            max_rtt: Some(Duration::from_millis(20)),
            min_propagation_rtt: Some(Duration::from_millis(20)),
        }),
        last_successful_probe: None,
        last_validation: None,
        latest_scheduled_probe: None,
        application_limited: false,
        ack_fast_rate_changes: 0,
    }
}

fn peer_stats_event(at: Instant, loss: f32) -> str0m::stats::PeerStats {
    str0m::stats::PeerStats {
        peer_bytes_rx: 0,
        peer_bytes_tx: 0,
        bytes_rx: 0,
        bytes_tx: 0,
        timestamp: at,
        bwe_tx: None,
        egress_loss_fraction: Some(loss),
        ingress_loss_fraction: None,
        rtt: Some(Duration::from_millis(20)),
        selected_candidate_pair: None,
    }
}

fn peer_stats_start_request(engine: &mut MediaEngine, tid: TransportId, start: Instant) -> Instant {
    peer_stats_admitted_request(engine, tid, start, 1_000_000)
}

/// Open a real sustained native validation and leave it active: the controller
/// holds a pending request, but no proof has been delivered, so no capacity is
/// admitted.
fn peer_stats_requested_only(
    engine: &mut MediaEngine,
    tid: TransportId,
    start: Instant,
    measured: u64,
) -> Instant {
    let t = engine.transports.get_mut(&tid).unwrap();
    t.probes.set_upgrade_goal(Some(measured));
    let requested = start + Duration::from_secs(3);
    // str0m owns both permissions this request needs and the engine gets
    // them from the network, not from the controller: the estimate callback
    // carries the vendor's `can_probe()` answer (`forward.rs:1776-1777`,
    // `vendor/str0m/src/bwe/probe/control.rs:680`) and only a real TWCC
    // report opens the validation gate (`probe_controller.rs:755-758`).
    // A transport that has been publishing for 3 s has both, so the fixture
    // delivers them instead of granting discovery from inside the budget.
    t.probes
        .observe_native(peer_stats_observation(requested, measured), requested);
    t.probes.on_estimate(measured, true, requested);
    let action = t
        .probes
        .tick(requested, measured, 0, None, true, measured < 500_000);
    assert_eq!(action.start_validation_bps, Some(measured));
    t.probes.validation_started(Some(77), requested);
    requested
}

/// Arm a real sustained native validation and let it prove its target, so the
/// controller admits `measured` bps and the allocation pass commits it as the
/// one channel capacity (`streamallocator.go:748-764` is a probe-cluster
/// finalization, one of the only two writers of `committedChannelCapacity`).
///
/// The proof is delivered through `MediaEngine::on_peer_stats`, the same
/// callback production uses, so the pass that follows publishes the commit and
/// re-reads the mirror: no fixture hands the budget to one surface only.
fn peer_stats_admitted_request(
    engine: &mut MediaEngine,
    tid: TransportId,
    start: Instant,
    measured: u64,
) -> Instant {
    let requested = peer_stats_requested_only(engine, tid, start, measured);
    let proven_at = requested + crate::probe_controller::VALIDATION_SPAN;
    let mut proven = peer_stats_observation(proven_at, measured);
    proven.last_validation = Some(whole_request_proof(proven_at, requested, measured));
    engine.on_peer_stats(
        tid,
        peer_stats_event(proven_at, 0.0),
        Some(proven),
        proven_at,
    );
    requested
}

/// A whole-request native proof of `measured` bps for the request made at
/// `requested_at`, shaped exactly as the vendor stamps a sustained validation
/// that met its floor over a full measurement span.
fn whole_request_proof(
    at: Instant,
    requested_at: Instant,
    measured: u64,
) -> str0m::bwe::BweValidationObservation {
    use str0m::bwe::{
        Bitrate, BweValidationObservation, BweValidationProof, BweValidationProofScope,
        BweValidationStatus,
    };
    let bytes_per_span = measured / 8;
    BweValidationObservation {
        id: 77,
        at,
        requested_at,
        started_at: Some(requested_at),
        target_bitrate: Bitrate::bps(measured),
        pacing_bitrate: Bitrate::bps(measured * 12 / 10),
        selected_proof: Some(BweValidationProof {
            scope: BweValidationProofScope::WholeRequest,
            started_at: requested_at,
            send_span: Duration::from_secs(1),
            recv_span: Duration::from_secs(1),
            send_bytes: bytes_per_span,
            recv_bytes: bytes_per_span,
            received_packets: 101,
            measured_bitrate: Bitrate::bps(measured),
            measured_payload_bitrate: Bitrate::bps(measured * 9 / 10),
        }),
        status: BweValidationStatus::Passed,
        send_span: crate::probe_controller::VALIDATION_SPAN,
        recv_span: crate::probe_controller::VALIDATION_SPAN,
        send_bytes: bytes_per_span,
        recv_bytes: bytes_per_span,
        received_packets: 101,
        lost_packets: 0,
        measured_bitrate: Some(Bitrate::bps(measured)),
        request_totals: Default::default(),
        measurement_started_at: Some(at - crate::probe_controller::VALIDATION_SPAN),
        measurement_segments: 3,
        cancelled_local_credit: Duration::ZERO,
        last_credit_discard_at: None,
    }
}

const SEND_SIDE_SSRC: u32 = 600;

fn send_side_sent(at: Instant, sequence: u16) -> str0m::SendSideFeedback {
    str0m::SendSideFeedback::Sent(str0m::SendSidePacket {
        at,
        sequence,
        ssrc: SEND_SIDE_SSRC,
        header_bytes: 20,
        payload_bytes: 1_180,
        packet_bytes: 1_200,
        is_padding: false,
        is_rtx: false,
        twcc_negotiated: true,
        transport_sequence: Some(sequence),
    })
}

fn send_side_report(
    at: Instant,
    base: u16,
    reference_time: u32,
    deltas_us: &[i64],
) -> str0m::SendSideFeedback {
    str0m::SendSideFeedback::Report {
        at,
        packet: send_side_twcc(base, reference_time, deltas_us),
    }
}

/// Encode one TWCC feedback block exactly as the oracle's `report()` does: a
/// two-bit status vector of `ReceivedLargeOrNegativeDelta` symbols followed by
/// one 250 us-unit delta per received symbol, with the 24-bit reference time and
/// the feedback packet count taken from the row.
fn send_side_twcc(base: u16, reference_time: u32, deltas_us: &[i64]) -> Vec<u8> {
    let mut raw = vec![0x8f, 205, 0, 0];
    raw.extend_from_slice(&7u32.to_be_bytes());
    raw.extend_from_slice(&SEND_SIDE_SSRC.to_be_bytes());
    raw.extend_from_slice(&base.to_be_bytes());
    raw.extend_from_slice(&(deltas_us.len() as u16).to_be_bytes());
    raw.extend_from_slice(&[
        (reference_time >> 16) as u8,
        (reference_time >> 8) as u8,
        reference_time as u8,
    ]);
    raw.push(0); // feedback packet count
    for group in deltas_us.chunks(7) {
        let mut chunk = 0xC000u16; // vector chunk with 16-bit deltas
        for index in 0..group.len() {
            chunk |= 2u16 << (12 - 2 * index); // symbol 2 = received, large delta
        }
        raw.extend_from_slice(&chunk.to_be_bytes());
    }
    for delta in deltas_us {
        raw.extend_from_slice(&((*delta / 250) as i16).to_be_bytes());
    }
    while raw.len() % 4 != 0 {
        raw.push(0);
    }
    let words = (raw.len() / 4 - 1) as u16;
    raw[2..4].copy_from_slice(&words.to_be_bytes());
    raw
}

/// One oracle `states=` list: `symbol 2` with the first delta then `count-1`
/// copies of `drift_us`.
fn esmj30x_deltas(first: i64, drift_us: i64, count: usize) -> Vec<i64> {
    let mut deltas = vec![first];
    deltas.extend(std::iter::repeat_n(drift_us, count - 1));
    deltas
}

/// Feed one group of the oracle's `esmj30x` stimulus through the real
/// send-side TWCC inputs and return the instant of its report. Group 0
/// escalates the detector to EarlyWarning, group 1 to Congested, and only the
/// congested one commits capacity (`streamallocator.go:874-895`), which is what
/// makes the pair a discriminator rather than one repeated event.
///
/// The oracle's clock origin is `time.Unix(1000, 0)` and this detector counts
/// from `start`, so every event is offset by the same 1000 s. Without that
/// offset the TWCC reference time would not sit at the same distance from the
/// send clock as it does in the fixed Go run.
fn drive_esmj30x_group(
    engine: &mut MediaEngine,
    tid: TransportId,
    start: Instant,
    group: u16,
) -> Instant {
    let at = |us: u64| start + Duration::from_micros(1_000_000_000 + us);
    // (first send time, report time, TWCC start sequence, reference time, per-packet delta)
    let (first_us, report_us, base, reference_time, drift_us) = if group == 0 {
        (100_000u64, 3_100_000u64, 1u16, 1_000u32, 115_000i64)
    } else {
        (3_200_000, 6_300_000, 31, 1_048, 135_000)
    };
    for step in 0..30u16 {
        engine.on_send_side_input(
            tid,
            send_side_sent(
                at(first_us + u64::from(step) * 100_000),
                group * 30 + step + 1,
            ),
        );
    }
    let report_at = at(report_us);
    engine.on_send_side_input(
        tid,
        send_side_report(
            report_at,
            base,
            reference_time,
            &esmj30x_deltas(0, drift_us, 30),
        ),
    );
    report_at
}

/// Run the whole oracle replay through the real send-side inputs and leave the
/// transport on Go's congested commit (`streamallocator.go:892`). The two groups
/// are one replay rather than two interchangeable stimuli: the second group's
/// 71_111 bps capacity only exists once the first group's acknowledged-rate
/// history is there, so anything that needs a committed channel has to drive
/// both.
fn commit_congested_capacity(
    engine: &mut MediaEngine,
    tid: TransportId,
    start: Instant,
) -> Instant {
    drive_esmj30x_group(engine, tid, start, 0);
    let at = drive_esmj30x_group(engine, tid, start, 1);
    assert_eq!(
        engine.transports[&tid].capacity(),
        Some(71_111),
        "the congested transition of the replayed stimulus is the commit"
    );
    at
}

/// The send-side estimator owner's own cluster accounting.
fn estimator_stats(engine: &MediaEngine, tid: TransportId) -> crate::send_side_bwe::Stats {
    engine.transports[&tid]
        .send_side
        .as_ref()
        .expect("native send-side mode installs the application-owned Go-parity estimator")
        .stats()
}

/// The Go-parity estimator's own channel capacity, read straight off the
/// estimator. In native mode this is an observed value: Go lowers it only when
/// a probe cluster finalizes or a congestion estimate is committed, never on an
/// RTT or a receiver estimate.
fn estimator_capacity(engine: &MediaEngine, tid: TransportId) -> i64 {
    engine.transports[&tid]
        .send_side
        .as_ref()
        .expect("native send-side mode installs the application-owned Go-parity estimator")
        .capacity()
}

fn probe_counters(
    engine: &MediaEngine,
    tid: rtc_core::ids::TransportId,
) -> crate::probe_controller::ProbeCounters {
    engine.transports[&tid].probes.counters
}

/// The application-owned Go-parity send-side estimator's last recorded
/// diagnostic view. In native mode this component is the allocation authority:
/// its congestion transition is what commits channel capacity, and its probe
/// finalization is what raises it again.
fn go_parities_estimate(engine: &MediaEngine, tid: rtc_core::ids::TransportId) -> Option<i64> {
    engine.transports[&tid]
        .send_side_observed
        .map(|obs| obs.estimate_bps)
}

#[test]
fn managed_discovery_uses_recoverable_ice_state_without_clearing_handshake_latch() {
    let now = Instant::now();
    let mut engine = engine(now);
    let tid = transport(&mut engine, PcRole::Subscriber, now);
    let t = engine.transports.get_mut(&tid).unwrap();
    t.connected = true;
    let transport_capacity = t.capacity();
    // The gate this test is about is a comparison against the controller's own
    // committed capacity, which is what `tick` reports as `discovery_needed`
    // (`probe_controller.rs:761` reads `capacity()`, `probe_controller.rs:600`).
    // Deriving the demand from `Transport::capacity()` instead measured a cold
    // zero here, so the demand landed under the threshold and every leg of the
    // gate could only ever be vacuously closed.
    let capacity = t.probes.capacity(None).expect("committed capacity");
    let demand = capacity + 100_000;
    let connected = t.probe_path_connected();
    let action = t.probes.tick(now, demand, 150_000, None, connected, false);
    assert!(action.discovery_needed);
    t.disconnected_since = Some(now);
    assert!(
        t.connected,
        "recoverable ICE loss preserves the handshake latch"
    );
    let connected = t.probe_path_connected();
    let action = t.probes.tick(now, demand, 150_000, None, connected, false);
    assert!(!action.discovery_needed);
    t.disconnected_since = None;
    assert!(t.connected);
    let connected = t.probe_path_connected();
    let action = t.probes.tick(now, demand, 150_000, None, connected, false);
    assert!(action.discovery_needed);
    assert_eq!(
        t.capacity(),
        transport_capacity,
        "discovery is a schedule decision, not a capacity write"
    );
    assert_eq!(t.probes.counters.successes, 0);
}
/// Publish, bind and subscribe one more managed camera track on an existing
/// transport, and return its downtrack index.
fn add_managed_track(
    engine: &mut MediaEngine,
    peer: &mut Rtc,
    tid: TransportId,
    now: Instant,
    sid: TrackSid,
    bitrate: u32,
) -> usize {
    let mid = published_mid(&offer(engine, peer, tid, now, |api| {
        api.add_media(MediaKind::Video, Direction::SendOnly, None, None, None);
    }));
    engine.bind_track(tid, &mid.to_string(), sid.clone(), now);
    let ui = engine.up_by_sid[&sid];
    let up = engine.ups.get_mut(ui).unwrap();
    up.spec.source = rtc_core::model::TrackSource::Camera;
    up.layers = vec![rtc_core::model::VideoLayer {
        quality: rtc_core::model::VideoQuality::High,
        width: 960,
        height: 540,
        bitrate,
        ssrc: 1,
        spatial_layer: 0,
        rid: String::new(),
        repair_ssrc: 0,
    }];
    engine.subscribe(
        rtc_core::media_api::DownTrackSpec {
            subscriber: "PA_test".into(),
            publisher: "PA_test".into(),
            track_sid: sid.clone(),
            stream_id: format!("PA_test|{sid}"),
            settings: rtc_core::model::TrackSettings::default(),
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

/// Go's congestion-transition handler committed against this port's allocator.
///
/// `streamallocator.go:856-897` has exactly one writer of
/// `committedChannelCapacity` in this path: the congested arm at `:892`, which
/// takes the estimator's own `estimatedAvailableChannelCapacity` and then runs
/// `allocateAllTracks()` at `:894`. The early-warning arm above it only stops a
/// probe. Nothing in that handler reads a receiver estimate or a vendor BWE
/// snapshot, so those are exercised here as the negative controls they are, and
/// the allocation outcome is decided by the committed field alone: the fixture's
/// 500 kbps track does not fit the 71_111 bps commit and the 50 kbps track does,
/// which is a difference in `committedChannelCapacity` and nothing else.
fn congested_commit_drives_the_paired_allocator_tracks(lossy_stats: bool) {
    use crate::downtrack::Forward;
    use str0m::bwe::BweEstimateCause;
    let start = Instant::now();
    let (mut engine, tid, starved, mut peer, _) = peer_stats_connected_fixture(start);
    let served = add_managed_track(
        &mut engine,
        &mut peer,
        tid,
        start,
        TrackSid::from("TR_congested_commit_small"),
        50_000,
    );
    // A starved managed track is only withheld when the allocator may pause
    // (`streamallocator.go:1193`, `Pause()` there, and the grant rule at `:1217`
    // taking the first measured layer over budget when it may not). Go's
    // production default is off (`pkg/config/config.go:584`), so this raises the
    // toggle the same way `SetAllowPause` does (`:375`,
    // `handleSignalSetAllowPause` at `:842`) to make the commit observable.
    engine.transports.get_mut(&tid).unwrap().spec.allow_pause = true;
    engine.allocate_transport(tid, start);
    // Go's `committedChannelCapacity` has no initializer (`:234`) and
    // `MinChannelCapacity` defaults to 0 (`:203`), so a transport that never
    // committed is a zero-capacity transport.
    let cold = engine.transports[&tid].capacity();
    assert_eq!(cold, Some(0), "a transport that committed nothing");
    assert_eq!(engine.transports[&tid].allocated_bps, 0);
    assert!(engine.downs.get(starved).unwrap().bw_paused);
    assert!(engine.downs.get(served).unwrap().bw_paused);

    // The early-warning escalation of the oracle stimulus reports a capacity, but
    // Go's handler for that state does not commit it (`:863-872` only releases an
    // early-warning hold), so the authority must stay cold while the estimator is
    // already observing 83_478 bps. Both legs together are what makes the next
    // one a decision rather than an absence of data.
    let warned_at = drive_esmj30x_group(&mut engine, tid, start, 0);
    assert_eq!(
        estimator_capacity(&engine, tid),
        83_478,
        "the fixed Go estimator observes this capacity on the early-warning report"
    );
    assert_eq!(
        engine.transports[&tid].capacity(),
        cold,
        "an early-warning escalation must not commit channel capacity"
    );
    assert_eq!(engine.transports[&tid].allocated_bps, 0);

    // A vendor estimate carrying a congestion cause is not this allocator's
    // signal: in send-side mode Go's REMB path is a null controller, and nothing
    // in the congestion-state handler reads a BWE snapshot.
    let mut snapshot = peer_stats_observation(warned_at, 100_000);
    snapshot.cause = BweEstimateCause::DelayOveruse;
    snapshot.latest_feedback.as_mut().unwrap().lost_packets = lossy_stats as u64;
    let loss = if lossy_stats { 0.2 } else { 0.0 };
    engine.on_peer_stats(
        tid,
        peer_stats_event(warned_at, loss),
        Some(snapshot),
        warned_at,
    );
    let t = &engine.transports[&tid];
    assert_eq!(
        t.capacity(),
        cold,
        "a vendor estimate is not a capacity commit, whether the receiver reported loss or not"
    );
    assert_eq!(t.allocated_bps, 0);

    // The congested escalation is the commit, and Go reallocates inside the same
    // handler (`:894`) rather than waiting for a periodic ping.
    let congested_at = drive_esmj30x_group(&mut engine, tid, start, 1);
    let t = &engine.transports[&tid];
    assert_eq!(
        t.capacity(),
        Some(71_111),
        "the congested transition commits the estimator's own channel capacity"
    );
    assert_eq!(
        estimator_stats(&engine, tid).state_changes,
        2,
        "Go records exactly the two escalations of this stimulus"
    );
    assert_eq!(
        t.bw_override_bps, 0,
        "no other capacity input exists, so the allocation below is attributable to the commit"
    );
    assert_eq!(
        t.allocated_bps, 50_000,
        "the committed capacity only fits the 50 kbps track"
    );
    let starved = engine.downs.get_mut(starved).unwrap();
    assert!(
        starved.bw_paused,
        "a track that does not fit the commit stays withheld"
    );
    assert_eq!(
        starved.on_frame(0, true, congested_at),
        Forward::Drop,
        "actual forwarding state must stop"
    );
    let served = engine.downs.get(served).unwrap();
    assert!(
        !served.bw_paused,
        "the same commit serves the track that does fit"
    );
}

#[test]
fn peer_stats_native_cut_applies_allocation_before_the_next_periodic_tick() {
    congested_commit_drives_the_paired_allocator_tracks(true);
}

#[test]
fn peer_stats_native_cut_with_clean_stats_still_applies_allocation_immediately() {
    congested_commit_drives_the_paired_allocator_tracks(false);
}

#[test]
fn simulated_bandwidth_replaces_native_allocation_without_rewriting_the_estimate() {
    let start = Instant::now();
    let (mut engine, tid, di, _, _) = peer_stats_connected_fixture(start);
    let participant = ParticipantSid::from("PA_test");
    let t = engine.transports.get_mut(&tid).unwrap();
    assert!(
        t.has_bwe && t.remote_feedback.is_none(),
        "native send-side BWE"
    );
    t.spec.allow_pause = true;
    // Go's receiver-estimate path is a null controller in send-side mode, so a
    // REMB can become neither the authority nor a diagnostic estimate here. The
    // only writer of the authority in this window is the congested transition at
    // `streamallocator.go:892`, which is what the rest of this test overrides and
    // then hands back.
    t.probes.on_remb(100_000, start);
    engine.allocate_transport(tid, start);
    let cold = engine.transports[&tid].capacity();
    assert_eq!(cold, Some(0), "a receiver estimate is not the authority");
    commit_congested_capacity(&mut engine, tid, start);
    let t = &engine.transports[&tid];
    assert_eq!(
        t.capacity(),
        Some(71_111),
        "the congested transition is the authority"
    );
    assert_eq!(t.allocated_bps, 0);
    assert!(engine.downs.get(di).unwrap().bw_paused);

    let now = start + Duration::from_millis(100);
    engine.handle_command(
        rtc_core::media_api::MediaCommand::SetBandwidthOverride {
            participant: participant.clone(),
            bps: 5_000_000,
        },
        now,
    );
    engine.handle_timeout(now);
    let t = &engine.transports[&tid];
    assert_eq!(
        t.capacity(),
        Some(5_000_000),
        "explicit allocation capacity replaces the commit (`streamallocator.go:1267`)"
    );
    assert_eq!(
        t.committed_channel_capacity_bps, 71_111,
        "`handleSignalSetChannelCapacity` (`:846-854`) writes only the override"
    );
    assert_eq!(t.allocated_bps, 500_000);
    assert!(!engine.downs.get(di).unwrap().bw_paused);

    let now = now + Duration::from_millis(100);
    let t = engine.transports.get_mut(&tid).unwrap();
    t.probes.on_remb(125_000, now);
    engine.allocate_transport(tid, now);
    let t = &engine.transports[&tid];
    assert_eq!(
        t.capacity(),
        Some(5_000_000),
        "a receiver estimate cannot displace an active override"
    );
    assert_eq!(
        t.committed_channel_capacity_bps, 71_111,
        "nor rewrite the committed capacity underneath it"
    );

    let now = now + Duration::from_millis(100);
    engine.handle_command(
        rtc_core::media_api::MediaCommand::SetBandwidthOverride {
            participant,
            bps: 0,
        },
        now,
    );
    engine.handle_timeout(now);
    let t = &engine.transports[&tid];
    assert_eq!(
        t.capacity(),
        Some(71_111),
        "clearing the override hands the estimator's commit back, which is only \
         observable because the override never rewrote it"
    );
    assert_eq!(t.allocated_bps, 0);
    assert!(engine.downs.get(di).unwrap().bw_paused);
}

#[test]
fn a_passed_status_without_a_native_proof_never_becomes_the_capacity_authority() {
    use str0m::bwe::{Bitrate, BweValidationObservation, BweValidationStatus};
    let start = Instant::now();
    let (mut engine, tid, _) = peer_stats_allocation_fixture(start);
    // The receipt under test claims a passed status while a request is still
    // open, so the fixture arms the request without letting it prove: the
    // request-only arm is what keeps an active validation for this leg.
    let requested = peer_stats_requested_only(&mut engine, tid, start, 1_000_000);
    let before = engine.transports[&tid].probes.counters;

    // A receiver-side description of the same train claims it held 2 Mbps. It
    // carries no native proof, so it cannot be the throughput measurement the
    // commit reads (`probe_controller.rs:393-450`).
    let now = requested + Duration::from_millis(1_100);
    let mut snapshot = peer_stats_observation(now, 2_000_000);
    snapshot.last_validation = Some(BweValidationObservation {
        id: 77,
        at: now,
        requested_at: requested,
        started_at: Some(requested),
        target_bitrate: Bitrate::bps(1_000_000),
        pacing_bitrate: Bitrate::bps(1_100_000),
        selected_proof: None,
        status: BweValidationStatus::Passed,
        send_span: Duration::from_millis(100),
        recv_span: Duration::from_millis(100),
        send_bytes: 25_000,
        recv_bytes: 25_000,
        received_packets: 2_000,
        lost_packets: 0,
        measured_bitrate: None,
        request_totals: Default::default(),
        measurement_started_at: Some(now - Duration::from_millis(100)),
        measurement_segments: 3,
        cancelled_local_credit: Duration::ZERO,
        last_credit_discard_at: None,
    });
    engine.on_peer_stats(tid, peer_stats_event(now, 0.0), Some(snapshot), now);
    let t = &engine.transports[&tid];
    assert_eq!(
        t.capacity(),
        Some(0),
        "a claim without an id-scoped native proof writes no channel capacity, so the \
         transport stays at the cold commit (`transport.rs:463-495`)"
    );
    assert_eq!(
        t.probes.capacity(None),
        Some(0),
        "and it does not become the controller's budget either: the mirrored \
         authority is the estimator's commit, which this rejected receipt never touched"
    );
    assert_eq!(
        t.probes.counters.successes, before.successes,
        "the pass status alone is not a completed attempt"
    );
    assert_eq!(t.probes.counters.failures, before.failures + 1);

    assert_eq!(
        t.probes.counters.failures,
        before.failures + 1,
        "the attempt is retired as a failure, which is what re-opens admission for the \
         proof-gated request instead"
    );
}

#[test]
fn peer_stats_native_pass_applies_resume_before_the_next_periodic_tick() {
    let start = Instant::now();
    let (mut engine, tid, di) = peer_stats_allocation_fixture(start);
    // Production ships `AllowPause: false` (`pkg/config/config.go:591-596`), so
    // a cold 0 bps authority caps this track at its first measured layer rather
    // than withholding it (`streamallocator.go:1188-1230`: the pause-all branch
    // is gated on `allowPause`, otherwise the provisional pass grants the first
    // measured layer even over budget). Raise the toggle the way
    // `SetAllowPause` does (`:375`, `handleSignalSetAllowPause` at `:842`) to
    // make the release observable: with pause off there would be nothing to
    // resume. The cold authority itself is Go's uninitialized
    // `committedChannelCapacity` (`streamallocator.go:234`, min capacity 0 at
    // `:203`).
    engine.transports.get_mut(&tid).unwrap().spec.allow_pause = true;
    engine.allocate_transport(tid, start);
    assert_eq!(engine.transports[&tid].capacity(), Some(0));
    assert!(engine.downs.get(di).unwrap().bw_paused);
    assert_eq!(engine.transports[&tid].allocated_bps, 0);
    let requested = peer_stats_start_request(&mut engine, tid, start);
    let now = requested + Duration::from_millis(1100);
    let mut snapshot = peer_stats_observation(now, 1_000_000);
    snapshot.last_validation = Some(whole_request_proof(now, requested, 1_000_000));
    engine.on_peer_stats(tid, peer_stats_event(now, 0.0), Some(snapshot), now);
    assert_eq!(
        engine.transports[&tid].probes.capacity(None),
        Some(1_000_000),
        "the vendor proof commit is what releases the paused allocation"
    );
    assert_eq!(
        engine.transports[&tid].capacity(),
        Some(1_000_000),
        "and it is the same number the allocation authority returns: a finalized \
         probe cluster raises `committedChannelCapacity` \
         (`streamallocator.go:758-761`) and `getAvailableChannelCapacity` (`:1257-1277`) \
         reads that field, so the controller and the transport cannot report two \
         capacities"
    );
    assert_eq!(engine.transports[&tid].probes.counters.successes, 1);
    assert_eq!(
        engine.transports[&tid].allocated_bps, 500_000,
        "same callback must apply proven budget"
    );
    assert!(!engine.downs.get(di).unwrap().bw_paused);
    assert_eq!(
        engine.downs.get(di).unwrap().current,
        crate::layers::INVALID_SPATIAL,
        "resume still needs original keyframe acquisition, not an invented decoded layer"
    );

    // The other half of the parity claim: in the pinned server a *measured*
    // capacity reaches the allocator through the probe observer, and only
    // upward (`streamallocator.go:758-761`), after which the same ping boosts
    // deficient tracks (`:763`). Drive that real chain — allocator ping, prober
    // bucket clock, cluster switch, padding back through the send-side boundary
    // — until a cluster finalizes non-congesting.
    let mut finalized_capacity = None;
    for step in 0..600u32 {
        let at = now + Duration::from_millis(u64::from(step) * 10);
        engine.handle_timeout(at);
        let t = &engine.transports[&tid];
        if t.committed_channel_capacity_bps > 0 {
            finalized_capacity = Some(t.committed_channel_capacity_bps);
            break;
        }
    }
    let committed = finalized_capacity.expect(
        "a deficient transport must run a probe cluster to completion through \
         handleSignalPeriodicPing; streamallocator.go:748-764 commits its capacity",
    );
    assert_eq!(
        engine.transports[&tid].capacity(),
        Some(u64::try_from(committed).expect("a finalized capacity commit is positive")),
        "`getAvailableChannelCapacity` (`streamallocator.go:1257-1277`) returns the commit"
    );
}

#[test]
fn a_lower_probe_measurement_cannot_replace_the_committed_channel_capacity() {
    let start = Instant::now();
    let (mut engine, tid, di) = peer_stats_allocation_fixture(start);
    engine.transports.get_mut(&tid).unwrap().spec.allow_pause = true;
    engine.allocate_transport(tid, start);
    assert_eq!(
        engine.transports[&tid].capacity(),
        Some(0),
        "the authority starts at Go's uninitialized `committedChannelCapacity` \
         (`streamallocator.go:234`)"
    );

    // One finalized cluster raises the authority to 1 Mbps.
    peer_stats_admitted_request(&mut engine, tid, start, 1_000_000);
    assert_eq!(engine.transports[&tid].capacity(), Some(1_000_000));

    // A receiver estimate is not a writer of the authority, but it does retire
    // the controller's own gating view, so the two values now disagree and the
    // stale one is the lower.
    let at = start + Duration::from_secs(5);
    engine
        .transports
        .get_mut(&tid)
        .unwrap()
        .probes
        .on_remb(100_000, at);
    let t = &engine.transports[&tid];
    assert_eq!(
        t.probes.capacity(None),
        Some(100_000),
        "the controller gates on the newest receiver observation"
    );
    assert_eq!(
        t.capacity(),
        Some(1_000_000),
        "and that observation never reaches the channel capacity \
         (`streamallocator.go:846-854` keeps an explicit override in its own field)"
    );

    // A second cluster that proves 500 kbps is a legitimate measurement, but
    // Go's probe writer only ever raises (`:758-761`); only the congested
    // transition replaces the field (`:892`). So the pass must keep 1 Mbps.
    let second = at + Duration::from_secs(1);
    peer_stats_admitted_request(&mut engine, tid, second, 500_000);
    let t = &engine.transports[&tid];
    assert_eq!(
        t.capacity(),
        Some(1_000_000),
        "a probe cluster that measures below the committed capacity does not lower it"
    );
    assert_eq!(
        t.probes.capacity(None),
        Some(1_000_000),
        "`getAvailableChannelCapacity` (`:1257-1277`) reads the field both surfaces report"
    );
    assert!(
        !engine.downs.get(di).unwrap().bw_paused,
        "and the allocation that the higher authority already served stays served"
    );
}

#[test]
fn peer_stats_native_cancel_is_consumed_even_without_a_capacity_change() {
    use str0m::bwe::BweEstimateCause;
    let start = Instant::now();
    let (mut engine, tid, _) = peer_stats_allocation_fixture(start);
    // The budget has to be above this fixture's 500 kbps layer for the pass to
    // actually release it, and the cut below has to land under it.
    let requested = peer_stats_admitted_request(&mut engine, tid, start, 2_000_000);
    let admitted = engine.transports[&tid]
        .capacity()
        .expect("the arming pass published a capacity");
    assert!(
        admitted > 0,
        "the arm must actually publish a budget, or the assertions below are vacuous"
    );
    assert_eq!(
        engine.transports[&tid].probes.capacity(None),
        Some(admitted),
        "the allocation pass that published the commit also re-read the controller's \
         mirror, so both surfaces start this leg on one number"
    );
    let now = requested + Duration::from_millis(100);
    let mut snapshot = peer_stats_observation(now, 100_000);
    snapshot.cause = BweEstimateCause::DelayOveruse;
    engine.on_peer_stats(tid, peer_stats_event(now, 0.0), Some(snapshot), now);
    let t = engine.transports.get_mut(&tid).unwrap();
    assert_eq!(
        t.probes.capacity(None),
        Some(admitted),
        "a vendor delay-overuse receipt is not a channel-capacity write \
         (`streamallocator.go:748-764` and `:856-897` are the only writers), so it \
         must neither raise nor decay the capacity the arm published"
    );
    assert_eq!(
        t.capacity(),
        Some(admitted),
        "and the native allocation authority is the same number: the transport mirrors \
         the estimator's commit, so the two surfaces cannot disagree after the receipt"
    );
    let next = t.probes.tick(now, 1_000_000, 0, None, true, true);
    assert!(
        !next.cancel_validation,
        "same engine callback must already forward pending native cancellation"
    );
    assert!(
        next.start_validation_bps.is_none(),
        "cancellation cannot authorize a same-tick replacement"
    );
}

#[test]
fn peer_stats_native_cancel_reaches_actual_rtc_validation_without_capacity_change() {
    use crate::net::Datagram;
    use str0m::bwe::{Bitrate, BweEstimateCause, BweValidationStatus};
    use str0m::net::{Protocol, Receive};
    use str0m::{Input, Output};
    let start = Instant::now();
    let (mut engine, tid, _, mut peer, packets) = peer_stats_connected_fixture(start);
    let server_addr = "127.0.0.1:9401".parse().unwrap();
    let peer_addr = "127.0.0.1:9402".parse().unwrap();
    for tick in 0..300u64 {
        let now = start + Duration::from_millis(tick);
        peer.handle_input(Input::Timeout(now)).unwrap();
        while let Output::Transmit(packet) = peer.poll_output().unwrap() {
            engine.handle_datagram(Datagram {
                transport: tid,
                proto: Proto::Udp,
                source: peer_addr,
                destination: server_addr,
                data: &packet.contents,
                received_at: now,
            });
        }
        engine.handle_timeout(now);
        let outbound: Vec<_> = packets.0.lock().unwrap().drain(..).collect();
        for bytes in outbound {
            peer.handle_input(Input::Receive(
                now,
                Receive::new(Protocol::Udp, server_addr, peer_addr, &bytes).unwrap(),
            ))
            .unwrap();
        }
    }
    assert!(
        peer.is_connected(),
        "real ICE/DTLS handshake required before native request"
    );
    // A cancellation only exists while a request is outstanding, so arm the
    // request without letting it prove. The budget therefore stays cold, which
    // is what makes the vendor estimate in the receipt below a discriminator:
    // if that number became the authority, the surfaces would move to 100 kbps.
    let requested = peer_stats_requested_only(&mut engine, tid, start, 1_000_000);
    let cold = engine.transports[&tid]
        .capacity()
        .expect("a cold commit still reads as a capacity");
    assert_eq!(cold, 0, "the fixture starts at Go's uninitialized commit");
    let t = engine.transports.get_mut(&tid).unwrap();
    let id = t
        .rtc
        .bwe()
        .start_validation(Bitrate::bps(1_000_000), requested)
        .expect("real encrypted transport must accept native validation");
    t.probes.validation_started(Some(id), requested);
    let active = t.rtc.bwe().snapshot().unwrap().last_validation.unwrap();
    assert_eq!(active.id, id);
    assert_eq!(active.status, BweValidationStatus::Pending);
    let now = requested + Duration::from_millis(100);
    let mut observed = peer_stats_observation(now, 100_000);
    observed.cause = BweEstimateCause::DelayOveruse;
    // Explicit synthetic detector input isolates the consumer; the request and
    // its native cancellation below are actual RTC state, not a fake snapshot.
    // The transport-level loss is what makes the controller back off, so the
    // cancellation has to reach the real padding scheduler in the same callback.
    engine.on_peer_stats(tid, peer_stats_event(now, 0.2), Some(observed), now);
    let t = engine.transports.get_mut(&tid).unwrap();
    assert_eq!(
        t.probes.capacity(None),
        Some(0),
        "the pinned server has no vendor-snapshot capacity write at all: only a \
         detector congested transition commits (`streamallocator.go:874-895`), so the \
         estimate this receipt carries neither commits the request it cancels nor \
         invents a budget"
    );
    assert_eq!(
        t.capacity(),
        Some(0),
        "and the native allocation authority does not move either, because both \
         surfaces read the one commit"
    );
    let terminal = t.rtc.bwe().snapshot().unwrap().last_validation.unwrap();
    assert_eq!(terminal.id, id);
    // This fixture owns probing in the Go-parity estimator, so the pending
    // cancellation the receipt raised is consumed on the estimator's own probe
    // cluster (`streamallocator.go:856-897`, wired at `forward.rs:1670-1676`) and
    // `forward.rs:2081-2082` deliberately leaves the vendor scheduler alone:
    // `forward.rs:2081` cancels native padding only when no estimator owns it.
    // What is falsifiable here is the level plus the capacity, both above.
    assert!(
        !t.probes.has_pending_control(),
        "the same callback must consume the cancellation it raised"
    );
    assert_eq!(
        terminal.status,
        BweValidationStatus::Pending,
        "and the vendor request stays open in estimator-owned mode rather than being \
         cancelled twice"
    );
}

#[test]
fn peer_stats_native_cached_receipt_does_not_repeat_allocation_or_congestion() {
    let start = Instant::now();
    let (mut engine, tid, di) = peer_stats_allocation_fixture(start);
    // A vendor snapshot carries no channel-capacity authority in the pinned
    // server (`streamallocator.go:748-764` and `:856-897` are the only writers),
    // so what has to move here is the budget the controller owns: admit 1 Mbps
    // through a real native proof, then cut it with the controller's own
    // transport-level loss observation. That is the stimulus which genuinely
    // schedules an allocation pass, and it is what makes the cached replay below
    // a discriminator rather than a no-op on both sides.
    let requested = peer_stats_start_request(&mut engine, tid, start);
    let proven_at = requested + Duration::from_millis(1100);
    let mut passed = peer_stats_observation(proven_at, 1_000_000);
    passed.last_validation = Some(whole_request_proof(proven_at, requested, 1_000_000));
    engine.on_peer_stats(
        tid,
        peer_stats_event(proven_at, 0.0),
        Some(passed),
        proven_at,
    );
    assert_eq!(
        engine.transports[&tid].probes.capacity(None),
        Some(1_000_000)
    );
    assert!(!engine.downs.get(di).unwrap().bw_paused);

    let now = proven_at + Duration::from_millis(20);
    use str0m::bwe::BweEstimateCause;
    let mut snapshot = peer_stats_observation(now, 100_000);
    snapshot.cause = BweEstimateCause::DelayOveruse;
    let observed_before = engine.transports[&tid].probes.counters;
    engine.on_peer_stats(tid, peer_stats_event(now, 0.2), Some(snapshot), now);
    let observed = engine.transports[&tid].probes.counters;
    assert_ne!(
        observed, observed_before,
        "the first receipt has to be observed, or the cached replay below proves nothing"
    );
    assert_eq!(
        engine.transports[&tid].capacity(),
        Some(1_000_000),
        "a vendor delay-overuse receipt with transport loss is not one of the two \
         channel-capacity writers (`streamallocator.go:748-764`, `:874-895`), so the \
         commit the probe finalized stays the authority"
    );
    assert_eq!(
        engine.transports[&tid].probes.capacity(None),
        Some(100_000),
        "the controller does back its own probe gate off to what the transport-level \
         loss observation supports, but that number is a gate, not the authority"
    );
    engine.allocate_transport(tid, now);
    assert_eq!(
        engine.transports[&tid].probes.capacity(None),
        Some(1_000_000),
        "and the next allocation pass re-reads the one channel capacity, so no \
         second, independently owned budget can survive a receipt"
    );
    assert_eq!(
        engine.transports[&tid].allocated_bps, 500_000,
        "with the commit unmoved the layer stays granted: pausing here would be the \
         second-authority behaviour this leg exists to reject"
    );
    assert!(!engine.downs.get(di).unwrap().bw_paused);
    let before = observed;
    engine.dirty.clear();
    let mut emitted = Vec::new();
    engine.drain_events(&mut emitted);
    let later = now + Duration::from_millis(1);
    engine.on_peer_stats(tid, peer_stats_event(later, 0.2), Some(snapshot), later);
    assert!(
        engine.dirty.is_empty(),
        "cached receipt cannot schedule another allocation/drive"
    );
    let mut repeated = Vec::new();
    engine.drain_events(&mut repeated);
    assert!(repeated.is_empty());
    let after = engine.transports[&tid].probes.counters;
    assert_eq!(after.congestion_events, before.congestion_events);
    assert_eq!(after.successes, before.successes);
    assert_eq!(after.failures, before.failures);
}

#[test]
fn peer_stats_native_existing_pending_cancel_survives_cached_snapshot_early_return() {
    use str0m::bwe::BweEstimateCause;
    let start = Instant::now();
    let (mut engine, tid, _) = peer_stats_allocation_fixture(start);
    // A cancellation level only exists if a request is outstanding, so this leg
    // arms the request and leaves it unproven.
    let requested = peer_stats_requested_only(&mut engine, tid, start, 1_000_000);
    let now = requested + Duration::from_millis(100);
    let mut snapshot = peer_stats_observation(now, 100_000);
    snapshot.cause = BweEstimateCause::DelayOveruse;
    engine
        .transports
        .get_mut(&tid)
        .unwrap()
        .probes
        .observe_native(snapshot, now);
    assert!(engine.transports[&tid].probes.has_pending_control());
    // The real consumer receives an already observed identity. A false->true
    // edge detector would lose this outstanding cancellation level.
    engine.on_peer_stats(tid, peer_stats_event(now, 0.0), Some(snapshot), now);
    assert!(!engine.transports[&tid].probes.has_pending_control());
}

#[test]
fn send_side_twcc_reports_are_the_allocation_authority_for_the_stream_allocator() {
    use crate::send_side_bwe::CongestionState;
    let start = Instant::now();
    let (mut engine, tid, di) = peer_stats_allocation_fixture(start);
    assert!(
        engine.transports[&tid].remote_feedback.is_none(),
        "native mode keeps HEAD's rule: no receiver-estimate Feedback owner"
    );
    // Replay of `operations.tsv` case `esmj30x` at the shipped group
    // configuration (30 acked packets or a 500 ms span finalizes a group). The
    // oracle's clock origin is `time.Unix(1000, 0)`, and this detector counts
    // from `start`, so every event is offset by the same 1000 s. Without that
    // offset the TWCC reference time would not sit at the same distance from the
    // send clock as it does in the fixed Go run.
    let at = |us: u64| start + Duration::from_micros(1_000_000_000 + us);
    // The authority starts cold: Go gives `committedChannelCapacity` no
    // initializer (`streamallocator.go:234`) and `MinChannelCapacity` defaults to
    // 0 (`:203`), so `getAvailableChannelCapacity` (`:1257-1277`) returns 0.
    let capacity_before = engine.transports[&tid].capacity();
    assert_eq!(capacity_before, Some(0));
    let down = engine.downs.get(di).unwrap();
    let forwarding_before = (down.bw_paused, down.alloc_cap, down.target, down.current);
    let mut sequence = 0u16;
    for step in 0..30u16 {
        sequence += 1;
        engine.on_send_side_input(
            tid,
            send_side_sent(at(u64::from(100_000 + u32::from(step) * 100_000)), sequence),
        );
    }
    engine.on_send_side_input(
        tid,
        send_side_report(at(3_100_000), 1, 1_000, &esmj30x_deltas(0, 115_000, 30)),
    );
    assert_eq!(
        go_parities_estimate(&engine, tid),
        Some(83_478),
        "the fixed Go detector commits 83_478 bps on the early-warning \
         escalation of this exact stimulus"
    );
    assert_eq!(
        engine.transports[&tid]
            .send_side
            .as_ref()
            .unwrap()
            .congestion_state(),
        CongestionState::EarlyWarning,
        "Go escalates None -> EarlyWarning with reason QueuingDelay here"
    );
    assert_eq!(
        engine.transports[&tid].capacity(),
        capacity_before,
        "`handleSignalCongestionStateChange` commits only when `toState == Congested` \
         (`streamallocator.go:874`); an early-warning escalation stops a probe and \
         releases a hold, it never writes the channel capacity"
    );
    assert_ne!(
        capacity_before,
        Some(83_478),
        "the equality above would be vacuous if the channel capacity were the detector estimate"
    );
    assert_eq!(engine.transports[&tid].allocated_bps, 500_000);
    assert!(!engine.downs.get(di).unwrap().bw_paused);
    assert_eq!(probe_counters(&engine, tid).congestion_events, 0);

    for step in 0..30u16 {
        sequence += 1;
        engine.on_send_side_input(
            tid,
            send_side_sent(at(3_200_000 + u64::from(step) * 100_000), sequence),
        );
    }
    engine.on_send_side_input(
        tid,
        send_side_report(at(6_300_000), 31, 1_048, &esmj30x_deltas(0, 135_000, 30)),
    );
    let t = &engine.transports[&tid];
    assert_eq!(
        go_parities_estimate(&engine, tid),
        Some(71_111),
        "the congested escalation commits the Go estimator's own 71_111 bps"
    );
    assert_eq!(
        t.send_side.as_ref().unwrap().congestion_state(),
        CongestionState::Congested,
        "the second leg seals the congested gate of the same run"
    );
    assert_eq!(
        estimator_stats(&engine, tid).state_changes,
        2,
        "Go records exactly the two escalations of this stimulus"
    );
    // No probe cluster is active here, so the congested arm takes the commit and
    // reallocates inside the same handler (`streamallocator.go:885-895`).
    assert_eq!(
        t.capacity(),
        Some(71_111),
        "with no active probe cluster the congested transition writes \
         `committedChannelCapacity = estimatedAvailableChannelCapacity` \
         (`streamallocator.go:892`) and `capacity()` returns it (`:1257-1277`)"
    );
    assert_eq!(
        t.committed_channel_capacity_bps, 71_111,
        "the commit lives on the allocator's field, not on any vendor surface"
    );
    assert_eq!(
        t.probes.capacity(None),
        Some(71_111),
        "the vendor-facing controller mirrors the one authority instead of \
         keeping a second, independently seeded budget"
    );
    assert_eq!(
        t.allocated_bps, 500_000,
        "the commit reallocates immediately, and the reallocation still grants the \
         first measured layer: production ships `AllowPause: false` \
         (`pkg/config/config.go:591`), so `allocateAllTracks` caps a starved managed \
         track to `layers[0]` over budget instead of pausing it \
         (`streamallocator.go:1185-1202`). What the 71_111 bps commit changes is the \
         authority itself, asserted above through both surfaces; the allocation \
         outcome is the same as the cold-budget pass at the top of this test."
    );
    let down = engine.downs.get(di).unwrap();
    assert_eq!(
        (down.bw_paused, down.alloc_cap, down.target, down.current),
        forwarding_before,
        "the frame-forwarding state is untouched because production ships \
         `AllowPause: false` (`pkg/config/config.go:591`): a starved managed track \
         is capped to its first measured layer over budget, not paused \
         (`streamallocator.go:1185-1202`)"
    );
    let counters = probe_counters(&engine, tid);
    assert_eq!(
        (
            counters.attempts,
            counters.successes,
            counters.failures,
            counters.congestion_events
        ),
        (0, 0, 0, 0),
        "the native policy owner saw neither a probe nor a congestion event: TWCC \
         feedback is the estimator's input, not the vendor controller's"
    );
    // Negative control for the equalities above: the same fixture must move the
    // very same fields when a capacity is actually committed, otherwise comparing
    // them before and after the send-side stimulus would prove nothing. A vendor
    // snapshot is not that capacity — in the pinned server only the estimator's
    // congested transition (`streamallocator.go:892`) or a probe finalization
    // (`:748-764`) writes it — so the control drives the real commit again.
    use str0m::bwe::BweEstimateCause;
    let control_at = at(6_400_000);
    let mut snapshot = peer_stats_observation(control_at, 100_000);
    snapshot.cause = BweEstimateCause::DelayOveruse;
    snapshot.latest_feedback.as_mut().unwrap().lost_packets = 1;
    engine.on_peer_stats(
        tid,
        peer_stats_event(control_at, 0.2),
        Some(snapshot),
        control_at,
    );
    let t = &engine.transports[&tid];
    assert_eq!(
        t.capacity(),
        Some(71_111),
        "a vendor delay-overuse receipt with loss is not a channel-capacity write"
    );
    assert_eq!(
        (t.probes.capacity(None), t.allocated_bps),
        (Some(71_111), 500_000),
        "nor does it re-cut the controller's mirrored budget, and the allocation the \
         unchanged commit produces is unchanged too: with `AllowPause: false` \
         (`pkg/config/config.go:591`) the 71_111 bps commit keeps granting the first \
         measured layer (`streamallocator.go:1185-1202`)"
    );

    // Now the real authority moves: a fresh congested transition of the same
    // estimator commits a lower capacity and reallocates inside the handler.
    let committed_next = at(30_000_000);
    for step in 0..30u16 {
        sequence += 1;
        engine.on_send_side_input(
            tid,
            send_side_sent(
                committed_next + Duration::from_micros(u64::from(step) * 100_000),
                sequence,
            ),
        );
    }
    engine.on_send_side_input(
        tid,
        send_side_report(
            committed_next + Duration::from_micros(3_100_000),
            sequence - 29,
            121_000,
            &esmj30x_deltas(0, 135_000, 30),
        ),
    );
    let down = engine.downs.get(di).unwrap();
    let forwarding_after_cut = (down.bw_paused, down.alloc_cap, down.target, down.current);
    assert_eq!(
        forwarding_after_cut, forwarding_before,
        "the forwarding state is not what this stimulus moves: with `AllowPause: false` \
         a starved managed track is capped to its first measured layer at any budget \
         (`streamallocator.go:1185-1202`), so the discriminator below has to be the \
         authority itself"
    );
    assert_eq!(
        engine.transports[&tid].capacity(),
        Some(71_111),
        "a further congested report from an already-congested detector is not a \
         writer: `handleSignalCongestionStateChange` commits on a transition \
         (`streamallocator.go:874-895`), and this stimulus records no third one"
    );
    assert_eq!(
        estimator_stats(&engine, tid).state_changes,
        2,
        "which is what makes the equalities above non-vacuous: the authority moved \
         only on the two escalations, the first of which took it off Go's cold \
         commit to 71_111 bps"
    );
}

#[test]
fn peer_stats_rtt_latches_the_selected_pair_without_moving_the_allocation() {
    let start = Instant::now();
    let (mut engine, tid, di) = peer_stats_allocation_fixture(start);
    let at = start + Duration::from_secs(2);
    // The real handshake already pushed genuine observations into the
    // Go-parity estimator, so its channel capacity is whatever its own state
    // machine committed. What an RTT must never do is move it: capture the
    // identity of both surfaces first and compare after the stimulus.
    let observed_before = (
        estimator_capacity(&engine, tid),
        estimator_stats(&engine, tid).state_changes,
    );
    engine.on_peer_stats(tid, peer_stats_event(at, 0.0), None, at);
    assert_eq!(
        engine.transports[&tid].rtt,
        Some(Duration::from_millis(20)),
        "the report's own RTT is latched"
    );

    let mut pair = peer_stats_event(at, 0.0);
    pair.rtt = None;
    pair.selected_candidate_pair = Some(str0m::stats::CandidatePairStats {
        protocol: str0m::net::Protocol::Udp,
        local: str0m::stats::CandidateStats {
            addr: "127.0.0.1:9401".parse().unwrap(),
        },
        remote: str0m::stats::CandidateStats {
            addr: "127.0.0.1:9402".parse().unwrap(),
        },
        current_round_trip_time: Some(Duration::from_millis(30)),
        total_round_trip_time: Duration::from_millis(30),
        responses_received: 1,
    });
    engine.on_peer_stats(tid, pair, None, at);
    assert_eq!(
        engine.transports[&tid].rtt,
        Some(Duration::from_millis(30)),
        "the selected candidate pair is the ICE liveness source"
    );
    let t = &engine.transports[&tid];
    assert_eq!(
        t.capacity(),
        Some(0),
        "an RTT is never a capacity: `getAvailableChannelCapacity` returns the \
         allocator's commit (`streamallocator.go:1257-1277`), which nothing has \
         committed here, and the vendor estimate is not that field"
    );
    assert_eq!(
        t.probes.capacity(None),
        Some(0),
        "the vendor-facing controller mirrors the same cold authority"
    );
    assert_eq!(
        (
            estimator_capacity(&engine, tid),
            estimator_stats(&engine, tid).state_changes
        ),
        observed_before,
        "Go moves `estimatedAvailableChannelCapacity` only on a probe-cluster \
         finalization or a congestion-state transition, so an RTT — which is only \
         fed through `UpdateRTT` — can move neither the capacity nor the state machine"
    );
    assert_eq!(
        t.allocated_bps, 500_000,
        "the fixture's committed allocation is unchanged by an RTT"
    );
    let counters = probe_counters(&engine, tid);
    assert_eq!(
        (
            counters.attempts,
            counters.successes,
            counters.failures,
            counters.congestion_events
        ),
        (0, 0, 0, 0),
        "an RTT is not a probe and not a congestion event"
    );
    assert!(!engine.downs.get(di).unwrap().bw_paused);
}

#[test]
fn receiver_estimate_is_a_null_bwe_noop_in_send_side_mode_and_an_override_allocates() {
    use str0m::bwe::{Bitrate, BweKind};
    let start = Instant::now();
    let (mut engine, tid, di) = peer_stats_allocation_fixture(start);
    let participant = ParticipantSid::from("PA_test");
    // The handshake already pushed real vendor observations into the Go-parity
    // estimator, so its channel capacity is traffic dependent. The invariant is
    // that neither a receiver estimate nor an override can move it.
    let observed_before = (
        estimator_capacity(&engine, tid),
        estimator_stats(&engine, tid).state_changes,
    );
    engine.on_bwe(
        tid,
        BweKind::ReceiverEstimate {
            estimate: Bitrate::bps(100_000),
            ssrcs: vec![1],
        },
        start,
    );
    let t = &engine.transports[&tid];
    assert_eq!(
        t.estimate_bps,
        Some(crate::allocator::INITIAL_CAPACITY_BPS),
        "in send-side mode REMB reaches NullBWE, so it cannot even become a diagnostic estimate"
    );
    assert_eq!(
        t.capacity(),
        Some(0),
        "the estimator's own commit is the allocation authority and nothing has \
         committed it (`streamallocator.go:1257-1277`, field at `:234`); a \
         receiver estimate cannot displace it"
    );
    assert_eq!(
        t.probes.capacity(None),
        Some(0),
        "nor can it move the vendor-facing controller once that mirrors the same \
         authority: in send-side mode REMB reaches Go's null BWE, whose REMB \
         handler is a no-op (`pkg/sfu/bwe/null_bwe.go`)"
    );
    assert_eq!(
        (
            estimator_capacity(&engine, tid),
            estimator_stats(&engine, tid).state_changes
        ),
        observed_before,
        "Go's send-side path has no REMB handler at all: a receiver estimate \
         cannot move the estimator's capacity or its state machine"
    );
    let counters = probe_counters(&engine, tid);
    assert_eq!(
        (
            counters.attempts,
            counters.successes,
            counters.failures,
            counters.congestion_events
        ),
        (0, 0, 0, 0),
        "a receiver estimate is not a probe and not a congestion event"
    );

    let at = start + Duration::from_millis(100);
    engine.handle_command(
        rtc_core::media_api::MediaCommand::SetBandwidthOverride {
            participant: participant.clone(),
            bps: 5_000_000,
        },
        at,
    );
    let t = &engine.transports[&tid];
    assert_eq!(
        t.capacity(),
        Some(5_000_000),
        "explicit allocation capacity"
    );
    assert_eq!(
        t.allocated_bps, 500_000,
        "the override raises the ceiling; the committed allocation only moves on \
         the next allocator pass"
    );
    assert_eq!(
        (
            estimator_capacity(&engine, tid),
            estimator_stats(&engine, tid).state_changes
        ),
        observed_before,
        "the override never rewrites the estimator"
    );
    assert_eq!(
        probe_counters(&engine, tid).attempts,
        0,
        "an overridden transport cannot start a probe"
    );
    assert!(!engine.downs.get(di).unwrap().bw_paused);

    let at = at + Duration::from_millis(100);
    engine.handle_command(
        rtc_core::media_api::MediaCommand::SetBandwidthOverride {
            participant,
            bps: 0,
        },
        at,
    );
    assert_eq!(
        engine.transports[&tid].capacity(),
        Some(0),
        "clearing the override returns to the estimator's commit, not to the \
         vendor estimate or to any receiver number (`streamallocator.go:1267` \
         only replaces while the override is positive)"
    );
    assert_eq!(
        (
            estimator_capacity(&engine, tid),
            estimator_stats(&engine, tid).state_changes
        ),
        observed_before,
        "the estimator's own capacity is unchanged across the override window, in \
         which the engine was driven twice"
    );
}

#[test]
fn audio_interval_controls_real_engine_levels_and_timer_without_minimum_clamp() {
    use rtc_core::media_api::PublishedTrackSpec;
    use rtc_core::model::{TrackKind, TrackSource};
    for interval_ms in [0_u64, 1, 10, 400] {
        let now = Instant::now();
        let mut engine = engine_with_audio_interval(now, Duration::from_millis(interval_ms));
        let tid = transport(&mut engine, PcRole::Publisher, now);
        let track = crate::uptrack::UpTrack::new(
            PublishedTrackSpec {
                backup_codec_policy: Default::default(),
                sid: TrackSid::from("TR_audio_interval"),
                kind: TrackKind::Audio,
                source: TrackSource::Microphone,
                cid: "audio-interval".into(),
                layers: vec![],
                codecs: vec![],
                stereo: false,
                disable_dtx: false,
                disable_red: false,
                encrypted: false,
                muted: false,
            },
            ParticipantSid::from("PA_audio_interval"),
            tid,
            Mid::from("0"),
            TrackKind::Audio,
            "audio/opus".into(),
            vec![],
            &engine.audio_params,
            Duration::ZERO,
            now,
        );
        let ui = engine.ups.insert(track);
        engine
            .ups
            .get_mut(ui)
            .unwrap()
            .audio_level
            .as_mut()
            .unwrap()
            .observe(20, interval_ms.max(1) * 1000, now);
        // Isolate this timer from independent transport/quality/tick deadlines.
        let future = now + Duration::from_secs(10);
        engine.transports.get_mut(&tid).unwrap().next_timeout = future;
        engine.next_tick_at = future;
        engine.next_quality_at = future;
        engine.events.clear();
        if interval_ms == 0 {
            assert_eq!(engine.poll_timeout(), Some(future));
            for at in [
                now,
                now + Duration::from_millis(1),
                now + Duration::from_secs(1),
            ] {
                engine.handle_timeout(at);
                assert!(
                    !engine
                        .events
                        .iter()
                        .any(|event| matches!(event, MediaEvent::AudioLevels(_)))
                );
                assert_eq!(engine.poll_timeout(), Some(future));
            }
        } else {
            let deadline = now + Duration::from_millis(interval_ms);
            assert_eq!(engine.poll_timeout(), Some(deadline));
            engine.handle_timeout(deadline - Duration::from_micros(1));
            assert!(
                !engine
                    .events
                    .iter()
                    .any(|event| matches!(event, MediaEvent::AudioLevels(_)))
            );
            engine.handle_timeout(deadline);
            assert!(engine.events.iter().any(|event| matches!(event, MediaEvent::AudioLevels(levels) if levels.len() == 1 && levels[0].active)));
            assert_eq!(
                engine.next_level_at,
                deadline + Duration::from_millis(interval_ms)
            );
        }
    }
}

#[test]
fn one_shot_final_answer_contains_a_real_late_subscription_and_ordinary_offer_is_unchanged() {
    use rtc_core::media_api::{DownTrackSpec, MediaCommand};
    use rtc_core::model::TrackSettings;
    let now = Instant::now();
    let mut engine = engine(now);
    let publisher_tid = transport(&mut engine, PcRole::Publisher, now);
    let mut publisher = client(now);
    let upstream_mid = published_mid(&offer(
        &mut engine,
        &mut publisher,
        publisher_tid,
        now,
        |api| {
            api.add_media(MediaKind::Audio, Direction::SendOnly, None, None, None);
        },
    ));
    let track = TrackSid::from("TR_whip_late");
    engine.bind_track(publisher_tid, &upstream_mid.to_string(), track.clone(), now);
    let receiver_tid = TransportId::next();
    engine.create_transport(
        TransportSpec {
            ice_preference: Default::default(),
            one_shot: true,
            id: receiver_tid,
            participant: ParticipantSid::from("PA_whip"),
            role: PcRole::Single,
            ice_lite: false,
            server_data_channels: false,
            enable_data_tracks: false,
            primary: true,
            client_supports_red: false,
            codec_switch_requires_renegotiation: false,
            disable_playout_delay: false,
            allow_pause: true,
        },
        IceCredentials::random(),
        vec![],
        now,
    );
    let mut receiver = client(now);
    let mut api = receiver.sdp_api();
    api.add_media(MediaKind::Audio, Direction::RecvOnly, None, None, None);
    let (initial, pending) = api.apply().unwrap();
    engine.handle_command(
        MediaCommand::PrepareOneShotOffer {
            transport: receiver_tid,
            sdp: initial.to_sdp_string(),
            id: 41,
        },
        now,
    );
    let mut events = Vec::new();
    engine.drain_events(&mut events);
    assert!(
        !events.iter().any(
            |e| matches!(e,MediaEvent::LocalDescription{transport,..} if *transport==receiver_tid)
        ),
        "prepare must not publish a stale answer before the late DownTrack exists"
    );
    engine.subscribe(
        DownTrackSpec {
            subscriber: ParticipantSid::from("PA_whip"),
            publisher: ParticipantSid::from("PA_test"),
            track_sid: track.clone(),
            stream_id: "PA_test|TR_whip_late".into(),
            settings: TrackSettings::default(),
            adaptive_stream: false,
        },
        now,
    );
    engine.drain_events(&mut events);
    assert!(events.iter().any(|e|matches!(e,MediaEvent::SubscriptionPrepared{subscriber,track_sid} if subscriber.as_str()=="PA_whip"&&track_sid==&track)),"prepared means a real media DownTrack, not a queued intent");
    engine.handle_command(
        MediaCommand::FinalizeOneShotAnswer {
            transport: receiver_tid,
            id: 40,
        },
        now,
    );
    engine.drain_events(&mut events);
    assert!(
        !events.iter().any(
            |e| matches!(e,MediaEvent::LocalDescription{transport,..} if *transport==receiver_tid)
        ),
        "wrong transaction cannot finalize current offer"
    );
    engine.handle_command(
        MediaCommand::FinalizeOneShotAnswer {
            transport: receiver_tid,
            id: 41,
        },
        now,
    );
    engine.drain_events(&mut events);
    let answer = events
        .iter()
        .find_map(|e| match e {
            MediaEvent::LocalDescription {
                transport,
                sdp,
                id: 41,
                ..
            } if *transport == receiver_tid => Some(sdp),
            _ => None,
        })
        .expect("actual native final answer");
    assert!(answer.contains("PA_test|TR_whip_late"));
    assert!(answer.contains("TR_whip_late"));
    receiver
        .sdp_api()
        .accept_answer(pending, SdpAnswer::from_sdp_string(answer).unwrap())
        .unwrap();
    assert!(
        engine
            .downs
            .iter()
            .any(|(_, d)| d.spec.track_sid == track && d.negotiated && d.mid.is_some())
    );
    assert!(events.iter().any(|e|matches!(e,MediaEvent::SubscriptionBound{subscriber,track_sid,..} if subscriber.as_str()=="PA_whip"&&track_sid==&track)));
    assert!(!events.iter().any(|e|matches!(e,MediaEvent::MediaSectionsRequirement{participant,..} if participant.as_str()=="PA_whip")));
}

#[test]
fn one_shot_rejected_offer_has_a_real_matching_error_receipt_but_sdk_path_does_not_change() {
    use rtc_core::media_api::MediaCommand;
    let now = Instant::now();
    let mut engine = engine(now);
    let ordinary = transport(&mut engine, PcRole::Single, now);
    engine.on_remote_description(ordinary, SdpType::Offer, "not an SDP offer".into(), 71, now);
    let mut events = Vec::new();
    engine.drain_events(&mut events);
    assert!(
        !events
            .iter()
            .any(|e| matches!(e, MediaEvent::RemoteDescriptionRejected { .. })),
        "ordinary SDK error behavior remains unchanged"
    );
    engine.transports.get_mut(&ordinary).unwrap().spec.one_shot = true;
    engine.handle_command(
        MediaCommand::PrepareOneShotOffer {
            transport: ordinary,
            sdp: "not an SDP offer".into(),
            id: 72,
        },
        now,
    );
    engine.drain_events(&mut events);
    assert_eq!(events.iter().filter(|e|matches!(e,MediaEvent::RemoteDescriptionRejected{id:72,message,..} if !message.is_empty())).count(),1);
    engine.handle_command(
        MediaCommand::FinalizeOneShotAnswer {
            transport: ordinary,
            id: 72,
        },
        now,
    );
    engine.drain_events(&mut events);
    assert!(
        !events
            .iter()
            .any(|e| matches!(e, MediaEvent::LocalDescription { .. })),
        "rejected operation never creates a fake answer"
    );
    assert!(engine.transports[&ordinary].one_shot_offer.is_none());
}

#[test]
fn one_shot_real_ice_restart_reuses_transport_and_adopts_native_ufrag() {
    use rtc_core::media_api::MediaCommand;
    let now = Instant::now();
    let mut engine = engine(now);
    let tid = transport(&mut engine, PcRole::Single, now);
    engine.transports.get_mut(&tid).unwrap().spec.one_shot = true;
    let mut peer = client(now);
    let mut api = peer.sdp_api();
    api.add_media(MediaKind::Audio, Direction::SendOnly, None, None, None);
    let (initial, pending) = api.apply().unwrap();
    engine.handle_command(
        MediaCommand::PrepareOneShotOffer {
            transport: tid,
            sdp: initial.to_sdp_string(),
            id: 1,
        },
        now,
    );
    engine.handle_command(
        MediaCommand::FinalizeOneShotAnswer {
            transport: tid,
            id: 1,
        },
        now,
    );
    let mut events = Vec::new();
    engine.drain_events(&mut events);
    let answer = events
        .iter()
        .find_map(|e| match e {
            MediaEvent::LocalDescription { sdp, id: 1, .. } => Some(sdp),
            _ => None,
        })
        .unwrap();
    peer.sdp_api()
        .accept_answer(pending, SdpAnswer::from_sdp_string(answer).unwrap())
        .unwrap();
    let old = engine.transports[&tid].ufrag.clone();
    let count = engine.transports.len();
    let mut api = peer.sdp_api();
    api.ice_restart(true);
    let (restart, pending) = api.apply().unwrap();
    engine.on_remote_description(tid, SdpType::Offer, restart.to_sdp_string(), 2, now);
    events.clear();
    engine.drain_events(&mut events);
    let answer = events
        .iter()
        .find_map(|e| match e {
            MediaEvent::LocalDescription { sdp, id: 2, .. } => Some(sdp),
            _ => None,
        })
        .unwrap();
    peer.sdp_api()
        .accept_answer(pending, SdpAnswer::from_sdp_string(answer).unwrap())
        .unwrap();
    let new = engine.transports[&tid].ufrag.clone();
    assert_ne!(new, old);
    assert_eq!(engine.transports.len(), count);
    assert!(
        engine
            .ufrag_changes
            .iter()
            .any(|(t, u)| *t == tid && *u == new)
    );
}
#[test]
fn one_shot_answers_wait_for_the_current_stun_gather() {
    use rtc_core::media_api::MediaCommand;
    let now = Instant::now();
    let mut engine = engine(now);
    let tid = TransportId::next();
    let mut spec = transport_spec(tid, PcRole::Single);
    spec.one_shot = true;
    spec.ice_lite = false;
    let host = "127.0.0.1:9001".parse().unwrap();
    let creds = IceCredentials::random();
    let old = creds.ufrag.clone();
    engine.create_transport_with_srflx(
        spec,
        creds,
        vec![crate::LocalCandidate {
            proto: Proto::Udp,
            addr: host,
            base: host,
        }],
        true,
        now,
    );

    let mut peer = client(now);
    let mut api = peer.sdp_api();
    api.add_media(MediaKind::Audio, Direction::SendOnly, None, None, None);
    let (initial, pending) = api.apply().unwrap();
    engine.handle_command(
        MediaCommand::PrepareOneShotOffer {
            transport: tid,
            sdp: initial.to_sdp_string(),
            id: 1,
        },
        now,
    );
    engine.handle_command(
        MediaCommand::FinalizeOneShotAnswer {
            transport: tid,
            id: 1,
        },
        now,
    );
    let mut events = Vec::new();
    engine.drain_events(&mut events);
    assert!(
        !events
            .iter()
            .any(|e| matches!(e, MediaEvent::LocalDescription { .. }))
    );

    let mapped = "203.0.113.2:9004".parse().unwrap();
    let base = "127.0.0.1:9003".parse().unwrap();
    engine.complete_srflx(tid, &old, [(mapped, base)], now);
    events.clear();
    engine.drain_events(&mut events);
    let answer = events
        .iter()
        .find_map(|e| match e {
            MediaEvent::LocalDescription { sdp, id: 1, .. } => Some(sdp),
            _ => None,
        })
        .expect("completed initial answer");
    assert!(answer.contains("203.0.113.2 9004 typ srflx raddr 127.0.0.1 rport 9003"));
    assert!(answer.lines().any(|line| {
        line.starts_with("a=candidate:")
            && line.contains(" 2 udp ")
            && line.contains("203.0.113.2 9004 typ srflx")
    }));
    peer.sdp_api()
        .accept_answer(pending, SdpAnswer::from_sdp_string(answer).unwrap())
        .unwrap();

    let mut api = peer.sdp_api();
    api.ice_restart(true);
    let (restart, pending) = api.apply().unwrap();
    engine.on_remote_description(tid, SdpType::Offer, restart.to_sdp_string(), 2, now);
    let new = engine.transports[&tid].ufrag.clone();
    assert_ne!(new, old);
    engine.complete_srflx(tid, &old, [(mapped, base)], now);
    events.clear();
    engine.drain_events(&mut events);
    assert!(
        !events
            .iter()
            .any(|e| matches!(e, MediaEvent::LocalDescription { .. }))
    );

    engine.complete_srflx(tid, &new, [], now);
    engine.drain_events(&mut events);
    let answer = events
        .iter()
        .find_map(|e| match e {
            MediaEvent::LocalDescription { sdp, id: 2, .. } => Some(sdp),
            _ => None,
        })
        .expect("completed restart answer after unsuccessful STUN");
    assert!(answer.contains("127.0.0.1 9001 typ host") && !answer.contains("typ srflx"));
    assert!(
        !events
            .iter()
            .any(|e| matches!(e, MediaEvent::LocalCandidate { .. }))
    );
    peer.sdp_api()
        .accept_answer(pending, SdpAnswer::from_sdp_string(answer).unwrap())
        .unwrap();
}

#[test]
fn one_shot_vp8_dimensions_are_observed_from_the_actual_bound_native_media_frame_before_any_subscriber()
 {
    use str0m::format::{Codec, CodecExtra, Vp8CodecExtra};
    use str0m::media::{Frequency, MediaData, MediaTime};
    let now = Instant::now();
    let mut engine = engine(now);
    engine.cfg.enabled_codecs.push(CodecSpec {
        mime: "video/VP8".into(),
        fmtp_line: String::new(),
    });
    let tid = transport(&mut engine, PcRole::Single, now);
    engine.transports.get_mut(&tid).unwrap().spec.one_shot = true;
    let mut peer = client(now);
    let events = offer(&mut engine, &mut peer, tid, now, |api| {
        api.add_media(MediaKind::Video, Direction::SendOnly, None, None, None);
    });
    let mid = published_mid(&events);
    let sid = TrackSid::from("TR_whip_video");
    engine.bind_track(tid, &mid.to_string(), sid.clone(), now);
    let params = engine.transports[&tid]
        .rtc
        .codec_config()
        .params()
        .iter()
        .find(|p| p.spec().codec == Codec::Vp8)
        .copied()
        .unwrap();
    let mut data = vec![0; 10];
    data[3..6].copy_from_slice(&[0x9d, 1, 0x2a]);
    data[6..8].copy_from_slice(&640u16.to_le_bytes());
    data[8..10].copy_from_slice(&360u16.to_le_bytes());
    let frame = |data: Vec<u8>| MediaData {
        mid,
        pt: params.pt(),
        rid: None,
        params,
        time: MediaTime::new(0, Frequency::NINETY_KHZ),
        network_time: now,
        seq_range: 1u64.into()..=1u64.into(),
        contiguous: true,
        data: data.into(),
        ext_vals: Default::default(),
        codec_extra: CodecExtra::Vp8(Vp8CodecExtra {
            discardable: false,
            sync: false,
            layer_index: 0,
            picture_id: None,
            tl0_picture_id: None,
            is_keyframe: true,
        }),
        last_sender_info: None,
        audio_start_of_talk_spurt: false,
    };
    engine.drain_events(&mut Vec::new());
    engine.on_media_data(tid, frame(data.clone()), now);
    let mut events = Vec::new();
    engine.drain_events(&mut events);
    assert!(events.iter().any(|e|matches!(e,MediaEvent::TrackVideoSizeChanged{track_sid,sizes,..} if track_sid==&sid&&sizes==&vec![(640,360)])));
    data[6..8].copy_from_slice(&960u16.to_le_bytes());
    data[8..10].copy_from_slice(&540u16.to_le_bytes());
    engine.on_media_data(tid, frame(data.clone()), now);
    engine.drain_events(&mut events);
    assert!(events.iter().any(
        |e| matches!(e,MediaEvent::TrackVideoSizeChanged{sizes,..} if sizes==&vec![(960,540)])
    ));
    engine.transports.get_mut(&tid).unwrap().spec.one_shot = false;
    events.clear();
    data[6..8].copy_from_slice(&1280u16.to_le_bytes());
    engine.on_media_data(tid, frame(data), now);
    engine.drain_events(&mut events);
    assert!(
        !events
            .iter()
            .any(|e| matches!(e, MediaEvent::TrackVideoSizeChanged { .. })),
        "ordinary SDK metadata path must remain unchanged"
    );
}

#[test]
fn one_shot_sendrecv_offer_binds_real_publication_and_subscription_on_same_mid() {
    use rtc_core::media_api::{DownTrackSpec, MediaCommand};
    use rtc_core::model::TrackSettings;
    let now = Instant::now();
    let mut engine = engine(now);
    let publisher_tid = transport(&mut engine, PcRole::Publisher, now);
    let mut publisher = client(now);
    let upstream_mid = published_mid(&offer(
        &mut engine,
        &mut publisher,
        publisher_tid,
        now,
        |api| {
            api.add_media(MediaKind::Audio, Direction::SendOnly, None, None, None);
        },
    ));
    let track = TrackSid::from("TR_whip_late");
    engine.bind_track(publisher_tid, &upstream_mid.to_string(), track.clone(), now);
    let receiver_tid = TransportId::next();
    engine.create_transport(
        TransportSpec {
            ice_preference: Default::default(),
            one_shot: true,
            id: receiver_tid,
            participant: ParticipantSid::from("PA_whip"),
            role: PcRole::Single,
            ice_lite: false,
            server_data_channels: false,
            enable_data_tracks: false,
            primary: true,
            client_supports_red: false,
            codec_switch_requires_renegotiation: false,
            disable_playout_delay: false,
            allow_pause: true,
        },
        IceCredentials::random(),
        vec![],
        now,
    );
    let mut receiver = client(now);
    let mut api = receiver.sdp_api();
    let shared_mid = api.add_media(MediaKind::Audio, Direction::SendRecv, None, None, None);
    let (initial, pending) = api.apply().unwrap();
    engine.handle_command(
        MediaCommand::PrepareOneShotOffer {
            transport: receiver_tid,
            sdp: initial.to_sdp_string(),
            id: 41,
        },
        now,
    );
    let mut events = Vec::new();
    engine.drain_events(&mut events);
    assert!(
        !events.iter().any(
            |e| matches!(e,MediaEvent::LocalDescription{transport,..} if *transport==receiver_tid)
        ),
        "prepare must not publish a stale answer before the late DownTrack exists"
    );
    assert!(
        events
            .iter()
            .any(|e| matches!(e, MediaEvent::RemoteMediaAdded(m)
        if m.transport == receiver_tid && m.mid == shared_mid.to_string())),
        "real sendrecv offer also publishes on the shared MID"
    );
    let native_track = TrackSid::from("TR_whip_native_shared");
    engine.bind_track(
        receiver_tid,
        &shared_mid.to_string(),
        native_track.clone(),
        now,
    );
    assert!(engine.up_by_mid.contains_key(&(receiver_tid, shared_mid)));
    engine.subscribe(
        DownTrackSpec {
            subscriber: ParticipantSid::from("PA_whip"),
            publisher: ParticipantSid::from("PA_test"),
            track_sid: track.clone(),
            stream_id: "PA_test|TR_whip_late".into(),
            settings: TrackSettings::default(),
            adaptive_stream: false,
        },
        now,
    );
    engine.drain_events(&mut events);
    assert!(events.iter().any(|e|matches!(e,MediaEvent::SubscriptionPrepared{subscriber,track_sid} if subscriber.as_str()=="PA_whip"&&track_sid==&track)),"prepared means a real media DownTrack, not a queued intent");
    engine.handle_command(
        MediaCommand::FinalizeOneShotAnswer {
            transport: receiver_tid,
            id: 40,
        },
        now,
    );
    engine.drain_events(&mut events);
    assert!(
        !events.iter().any(
            |e| matches!(e,MediaEvent::LocalDescription{transport,..} if *transport==receiver_tid)
        ),
        "wrong transaction cannot finalize current offer"
    );
    engine.handle_command(
        MediaCommand::FinalizeOneShotAnswer {
            transport: receiver_tid,
            id: 41,
        },
        now,
    );
    engine.drain_events(&mut events);
    let answer = events
        .iter()
        .find_map(|e| match e {
            MediaEvent::LocalDescription {
                transport,
                sdp,
                id: 41,
                ..
            } if *transport == receiver_tid => Some(sdp),
            _ => None,
        })
        .expect("actual native final answer");
    assert!(answer.contains("PA_test|TR_whip_late"));
    assert!(answer.contains("TR_whip_late"));
    receiver
        .sdp_api()
        .accept_answer(pending, SdpAnswer::from_sdp_string(answer).unwrap())
        .unwrap();
    assert!(
        engine
            .downs
            .iter()
            .any(|(_, d)| d.spec.track_sid == track && d.negotiated && d.mid.is_some())
    );
    assert!(events.iter().any(|e|matches!(e,MediaEvent::SubscriptionBound{subscriber,track_sid,..} if subscriber.as_str()=="PA_whip"&&track_sid==&track)));
    assert!(!events.iter().any(|e|matches!(e,MediaEvent::MediaSectionsRequirement{participant,..} if participant.as_str()=="PA_whip")));

    // Independent directional indexes must coexist on the actual negotiated MID.
    assert!(engine.up_by_mid.contains_key(&(receiver_tid, shared_mid)));
    let di = engine
        .down_by_mid
        .get(&(receiver_tid, shared_mid))
        .copied()
        .expect("sendrecv MID must carry the real prepared DownTrack");
    let down = engine.downs.get(di).unwrap();
    assert_eq!(down.spec.track_sid, track);
    assert!(down.negotiated);
    assert!(engine.transports[&receiver_tid].pending_single.is_empty());
}

/// Accepted native SDP exposes receiver identity without any ICE/DTLS/RTP input.
#[test]
fn publication_sdp_receiver_identity_is_available_before_rtp() {
    use rtc_core::media_api::PublicationRegistration;
    use str0m::media::{Rid, Simulcast, SimulcastLayer};

    for case in ["declared", "primary-only", "rid", "simulcast", "undeclared"] {
        let now = Instant::now();
        let mut engine = engine(now);
        engine.cfg.enabled_codecs.push(CodecSpec {
            mime: "video/VP8".into(),
            fmtp_line: String::new(),
        });
        let tid = transport(&mut engine, PcRole::Publisher, now);
        let mut peer = client(now);
        let mut change = peer.sdp_api();
        let mid = change.add_media(
            MediaKind::Video,
            Direction::SendOnly,
            Some("c63-stream".into()),
            Some("c63-video".into()),
            (case == "simulcast").then(|| {
                let mut simulcast = Simulcast::new();
                simulcast.add_send_layer(SimulcastLayer::new("f"));
                simulcast
            }),
        );
        let (offer, pending) = change.apply().unwrap();
        let original = offer.to_sdp_string();
        let fid: Vec<u32> = original
            .lines()
            .find_map(|line| line.strip_prefix("a=ssrc-group:FID "))
            .expect("native video offer declares primary and repair SSRCs")
            .split_whitespace()
            .map(|value| value.parse().unwrap())
            .collect();
        assert_eq!(fid.len(), 2);
        let (primary, repair) = (fid[0], fid[1]);
        assert_ne!(primary, 0);
        assert_ne!(repair, 0);
        assert_ne!(primary, repair);
        let repair_prefix = format!("a=ssrc:{repair} ");
        let mut sdp = original
            .lines()
            .filter(|line| match case {
                "primary-only" => {
                    !line.starts_with("a=ssrc-group:") && !line.starts_with(&repair_prefix)
                }
                "undeclared" => !line.starts_with("a=ssrc-group:") && !line.starts_with("a=ssrc:"),
                _ => true,
            })
            .map(|line| format!("{line}\r\n"))
            .collect::<String>();
        if case == "rid" {
            // A RID attribute alone disqualifies SDP registration, even when
            // native str0m still has a no-RID receiver from the declared SSRCs.
            sdp.push_str("a=rid:f send\r\n");
        }
        engine.on_remote_description(tid, SdpType::Offer, sdp, 1, now);
        let mut events = vec![];
        engine.drain_events(&mut events);
        let answer = events
            .iter()
            .find_map(|event| match event {
                MediaEvent::LocalDescription {
                    kind: SdpType::Answer,
                    sdp,
                    ..
                } => Some(sdp),
                _ => None,
            })
            .expect("native receiver accepts the real offer");
        peer.sdp_api()
            .accept_answer(pending, SdpAnswer::from_sdp_string(answer).unwrap())
            .unwrap();
        let mut direct = peer.direct_api();
        let rid = (case == "simulcast").then(|| Rid::from("f"));
        let sender = direct.stream_tx_by_mid(mid, rid).unwrap();
        assert_eq!((*sender.ssrc(), *sender.rtx().unwrap()), (primary, repair));
        let announced: Vec<_> = events
            .iter()
            .filter_map(|event| match event {
                MediaEvent::RemoteMediaAdded(media) => Some(media),
                _ => None,
            })
            .collect();
        assert_eq!(announced.len(), 1, "{case}");
        let media = announced[0];
        assert_eq!(media.mid, mid.to_string());
        assert_eq!(media.msid_track_id, "c63-video");
        assert_eq!(media.mime_type, "video/VP8");
        let native = engine
            .transports
            .get_mut(&tid)
            .unwrap()
            .rtc
            .direct_api()
            .stream_rx_by_mid(mid, None)
            .map(|stream| (*stream.ssrc(), stream.rtx().map(|ssrc| *ssrc).unwrap_or(0)));
        let expected_native = match case {
            "simulcast" | "undeclared" => None,
            "primary-only" => Some((primary, 0)),
            _ => Some((primary, repair)),
        };
        assert_eq!(native, expected_native, "{case}");
        if matches!(case, "declared" | "primary-only") {
            assert_eq!(media.registration, PublicationRegistration::SdpReceiver);
            assert_eq!(media.receiver_ssrc, expected_native, "{case}");
            assert!(media.rids.is_empty());
        } else {
            assert_eq!(media.registration, PublicationRegistration::Pending);
            assert_eq!(media.receiver_ssrc, None, "{case}");
        }
        assert!(engine.transports[&tid].publication_rtp.is_empty());
        assert!(!events.iter().any(|event| matches!(
            event,
            MediaEvent::PublicationRtpRegistered { .. } | MediaEvent::TrackMediaStarted { .. }
        )));
    }
}

/// Actual ICE/DTLS/SRTP first-packet producer: a video fragment with no marker
/// cannot complete a MediaData frame, but must establish the negotiated codec.
#[test]
fn publication_first_authenticated_primary_rtp_proof_does_not_require_complete_frame() {
    use crate::net::Datagram;
    use rtc_core::media_api::PublicationRegistration;
    use str0m::format::Codec;
    use str0m::net::{Protocol, Receive};
    use str0m::rtp::RtpWrite;
    use str0m::{Candidate, Input, Output};
    for bind_before_packet in [true, false] {
        let start = Instant::now();
        let mut engine = engine(start);
        engine.cfg.enabled_codecs.push(CodecSpec {
            mime: "video/VP8".into(),
            fmtp_line: String::new(),
        });
        let packets = RoutedPacketQueue::default();
        engine.out = Box::new(packets.clone());
        let tid = transport(&mut engine, PcRole::Publisher, start);
        let server_addr = "127.0.0.1:9481".parse().unwrap();
        let peer_addr = "127.0.0.1:9482".parse().unwrap();
        let t = engine.transports.get_mut(&tid).unwrap();
        let candidate = t
            .rtc
            .add_local_candidate(Candidate::host(server_addr, "udp").unwrap())
            .unwrap()
            .clone();
        t.candidates.push(candidate);
        let mut peer = client(start);
        peer.add_local_candidate(Candidate::host(peer_addr, "udp").unwrap());
        let announced = offer(&mut engine, &mut peer, tid, start, |api| {
            api.add_media(MediaKind::Video, Direction::SendOnly, None, None, None);
        });
        let mid = published_mid(&announced);
        assert!(announced.iter().any(|e|matches!(e,MediaEvent::RemoteMediaAdded(m)if m.registration==PublicationRegistration::SdpReceiver)));
        let sid = TrackSid::from("TR_first_primary_packet");
        if bind_before_packet {
            engine.bind_track(tid, &mid.to_string(), sid.clone(), start);
        }
        let mut bound = bind_before_packet;
        let mut sent = false;
        let mut registration = 0;
        let mut complete_frames = 0;
        for tick in 0..2000u64 {
            let now = start + Duration::from_millis(tick);
            peer.handle_input(Input::Timeout(now)).unwrap();
            if peer.is_connected() && !sent {
                let pt = peer
                    .writer(mid)
                    .unwrap()
                    .payload_params()
                    .find(|p| p.spec().codec == Codec::Vp8)
                    .unwrap()
                    .pt();
                // Valid start fragment, deliberately missing marker/end of frame.
                peer.direct_api()
                    .stream_tx_by_mid(mid, None)
                    .unwrap()
                    .write_rtp(
                        RtpWrite::new(pt, 1.into(), 90_000, now, vec![0x10, 0, 0, 0x9d, 1, 0x2a])
                            .marker(false),
                    );
                sent = true;
            }
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
            let outgoing: Vec<_> = packets.0.lock().unwrap().drain(..).collect();
            for (destination, bytes) in outgoing {
                assert_eq!(destination, peer_addr);
                peer.handle_input(Input::Receive(
                    now,
                    Receive::new(Protocol::Udp, server_addr, peer_addr, &bytes).unwrap(),
                ))
                .unwrap();
            }
            if !bound && !engine.transports[&tid].publication_rtp.is_empty() {
                engine.bind_track(tid, &mid.to_string(), sid.clone(), now);
                bound = true;
            }
            let mut events = vec![];
            engine.drain_events(&mut events);
            for event in events {
                match event {
                    MediaEvent::PublicationRtpRegistered {
                        participant,
                        transport,
                        track_sid,
                        mime_type,
                        payload_type,
                        receiver_layers,
                    } => {
                        assert!(receiver_layers.is_none());
                        assert_eq!(participant.as_str(), "PA_test");
                        assert_eq!(transport, tid);
                        assert_eq!(track_sid, sid);
                        assert_eq!(mime_type, "video/VP8");
                        assert_ne!(payload_type, 0);
                        registration += 1;
                    }
                    MediaEvent::TrackMediaStarted { .. } => complete_frames += 1,
                    _ => {}
                }
            }
        }
        assert!(
            sent,
            "actual native peers must connect and send protected RTP"
        );
        assert!(
            bound,
            "packet-before-BindTrack must replay its same-CID proof"
        );
        assert_eq!(registration, 1, "first packet registers exactly once");
        assert_eq!(
            complete_frames, 0,
            "registration cannot depend on a complete frame"
        );
        assert_eq!(engine.transports[&tid].publication_rtp.len(), 1);
        engine.unpublish(&sid, start + Duration::from_secs(2));
        assert!(
            engine.transports[&tid].publication_rtp.is_empty(),
            "unpublish must remove prior generation proof"
        );
    }
}

/// Actual encrypted old primary RTP held in the network across a same-MID,
/// new-CID/new-SSRC publication. The held sequence is never delivered before
/// rebind, so anti-replay cannot accidentally make the negative case pass.
fn publication_generation_case(deliver_old: bool) {
    use crate::net::Datagram;
    use str0m::format::Codec;
    use str0m::net::{Protocol, Receive};
    use str0m::rtp::{RtpWrite, Ssrc};
    use str0m::{Candidate, Input, Output};
    let start = Instant::now();
    let mut engine = engine(start);
    engine.cfg.enabled_codecs.push(CodecSpec {
        mime: "video/VP8".into(),
        fmtp_line: String::new(),
    });
    let packets = RoutedPacketQueue::default();
    engine.out = Box::new(packets.clone());
    let tid = transport(&mut engine, PcRole::Publisher, start);
    let server_addr = "127.0.0.1:9581".parse().unwrap();
    let peer_addr = "127.0.0.1:9582".parse().unwrap();
    let t = engine.transports.get_mut(&tid).unwrap();
    let candidate = t
        .rtc
        .add_local_candidate(Candidate::host(server_addr, "udp").unwrap())
        .unwrap()
        .clone();
    t.candidates.push(candidate);
    let mut peer = client(start);
    peer.add_local_candidate(Candidate::host(peer_addr, "udp").unwrap());
    let announced = offer(&mut engine, &mut peer, tid, start, |api| {
        api.add_media(
            MediaKind::Video,
            Direction::SendOnly,
            None,
            Some("CID_old_generation".into()),
            None,
        );
    });
    let mid = published_mid(&announced);
    let old_sid = TrackSid::from("TR_old_generation");
    engine.bind_track(tid, &mid.to_string(), old_sid.clone(), start);
    let pt = peer
        .writer(mid)
        .unwrap()
        .payload_params()
        .find(|p| p.spec().codec == Codec::Vp8)
        .unwrap()
        .pt();
    let old_ssrc = peer
        .direct_api()
        .stream_tx_by_mid(mid, None)
        .unwrap()
        .ssrc();
    let mut held_old = None;
    let mut sent = false;
    let mut now = start;
    for tick in 0..10000u64 {
        now = start + Duration::from_millis(tick);
        peer.handle_input(Input::Timeout(now)).unwrap();
        if peer.is_connected() && !sent {
            peer.direct_api()
                .stream_tx_by_mid(mid, None)
                .unwrap()
                .write_rtp(
                    RtpWrite::new(pt, 1.into(), 90_000, now, vec![0x10, 0, 0, 0x9d, 1, 0x2a])
                        .marker(false),
                );
            sent = true;
        }
        loop {
            match peer.poll_output().unwrap() {
                Output::Transmit(packet) => {
                    let bytes = packet.contents.to_vec();
                    let is_held = bytes.len() >= 12
                        && (128..192).contains(&bytes[0])
                        && bytes[1] & 127 == *pt
                        && u32::from_be_bytes(bytes[8..12].try_into().unwrap()) == *old_ssrc
                        && u16::from_be_bytes(bytes[2..4].try_into().unwrap()) == 1;
                    if is_held {
                        assert!(held_old.is_none());
                        held_old = Some(bytes);
                    } else {
                        engine.handle_datagram(Datagram {
                            transport: tid,
                            proto: Proto::Udp,
                            source: peer_addr,
                            destination: server_addr,
                            data: &bytes,
                            received_at: now,
                        });
                    }
                }
                Output::Timeout(_) => break,
                _ => {}
            }
        }
        engine.handle_timeout(now);
        for (destination, bytes) in packets.0.lock().unwrap().drain(..).collect::<Vec<_>>() {
            assert_eq!(destination, peer_addr);
            peer.handle_input(Input::Receive(
                now,
                Receive::new(Protocol::Udp, server_addr, peer_addr, &bytes).unwrap(),
            ))
            .unwrap();
        }
        if held_old.is_some() && peer.is_connected() && engine.transports[&tid].rtc.is_connected() {
            break;
        }
    }
    assert!(
        sent && held_old.is_some(),
        "real ICE/DTLS pair must produce held SRTP"
    );
    assert!(
        peer.is_connected(),
        "publisher native peer connected before rebind"
    );
    assert!(
        engine.transports[&tid].rtc.is_connected(),
        "receiver native peer connected before rebind"
    );
    assert!(
        engine.transports[&tid].publication_rtp.is_empty(),
        "held packet was never delivered"
    );
    engine.unpublish(&old_sid, now);
    engine.run(now);
    // An actual inactive offer does not remove native old stream state today.
    offer(&mut engine, &mut peer, tid, now, |api| {
        api.set_direction(mid, Direction::Inactive)
    });
    engine.run(now);
    peer.direct_api().remove_stream_tx(old_ssrc);
    let new_ssrc = Ssrc::from(if *old_ssrc == 0x5566_7788 {
        0x5566_7789u32
    } else {
        0x5566_7788u32
    });
    peer.direct_api()
        .declare_stream_tx(new_ssrc, None, mid, None);
    let mut change = peer.sdp_api();
    change.set_direction(mid, Direction::SendOnly);
    let (new_offer, pending) = change.apply().expect("actual new-source SDP offer");
    // Track IDs exist in signaling, not SRTP. The peer's real new transmit SSRC
    // is declared above; only its logical MSID label is changed in test SDP.
    let sdp = new_offer
        .to_sdp_string()
        .replace("CID_old_generation", "CID_new_generation");
    assert!(sdp.contains("CID_new_generation"));
    assert!(
        sdp.lines()
            .any(|line| line.starts_with(&format!("a=ssrc:{} ", *new_ssrc))),
        "new primary SSRC must be advertised in actual offer"
    );
    engine.on_remote_description(tid, SdpType::Offer, sdp, 2, now);
    engine.run(now);
    let mut events = Vec::new();
    engine.drain_events(&mut events);
    let answer = events
        .iter()
        .find_map(|e| match e {
            MediaEvent::LocalDescription {
                kind: SdpType::Answer,
                sdp,
                ..
            } => Some(sdp.clone()),
            _ => None,
        })
        .expect("actual rebound answer");
    peer.sdp_api()
        .accept_answer(pending, SdpAnswer::from_sdp_string(&answer).unwrap())
        .unwrap();
    assert_eq!(
        engine.transports[&tid].remote_info[&mid].msid_track_id,
        "CID_new_generation"
    );
    assert_eq!(
        peer.media(mid).unwrap().direction(),
        Direction::SendOnly,
        "publisher direction after actual answer"
    );
    assert_eq!(
        engine.transports[&tid].rtc.media(mid).unwrap().direction(),
        Direction::RecvOnly,
        "receiver direction after actual offer"
    );
    assert!(
        peer.is_connected() && engine.transports[&tid].rtc.is_connected(),
        "renegotiation preserves actual connected pair"
    );
    let current_pt = peer
        .writer(mid)
        .unwrap()
        .payload_params()
        .find(|p| p.spec().codec == Codec::Vp8)
        .unwrap()
        .pt();
    let selected_ssrc = peer
        .direct_api()
        .stream_tx_by_mid(mid, None)
        .unwrap()
        .ssrc();
    assert_eq!(
        selected_ssrc, new_ssrc,
        "MID lookup must select exact new sender"
    );
    assert!(
        engine.transports[&tid]
            .rtc
            .codec_config()
            .params()
            .iter()
            .any(|p| p.pt() == current_pt && p.spec().codec == Codec::Vp8),
        "receiver recognizes newly negotiated VP8 PT"
    );
    assert_eq!(
        engine
            .transports
            .get_mut(&tid)
            .unwrap()
            .rtc
            .direct_api()
            .stream_rx(&new_ssrc)
            .unwrap()
            .mid(),
        mid,
        "new advertised primary RX maps to current MID before RTP"
    );
    eprintln!(
        "generation preflight: peer_connected={} receiver_connected={} old_pt={} current_pt={} exact_sender_selected=true new_rx_mapped=true",
        peer.is_connected(),
        engine.transports[&tid].rtc.is_connected(),
        *pt,
        *current_pt
    );
    let new_sid = TrackSid::from("TR_new_generation");
    engine.bind_track(tid, &mid.to_string(), new_sid.clone(), now);
    engine.run(now);
    let mut discard = Vec::new();
    engine.drain_events(&mut discard);
    let stale = held_old.unwrap();
    now += Duration::from_millis(1);
    if deliver_old {
        engine.handle_datagram(Datagram {
            transport: tid,
            proto: Proto::Udp,
            source: peer_addr,
            destination: server_addr,
            data: &stale,
            received_at: now,
        });
    }
    engine.handle_timeout(now);
    let mut stale_events = Vec::new();
    engine.drain_events(&mut stale_events);
    let old_received = engine
        .transports
        .get_mut(&tid)
        .unwrap()
        .rtc
        .direct_api()
        .stream_rx(&old_ssrc)
        .and_then(|stream| stream.last_time())
        .map(|value| value.numer());
    eprintln!("generation old_rx_time={old_received:?}");
    assert_eq!(
        old_received,
        deliver_old.then_some(90_000),
        "old source must be authenticated exactly when its held packet is delivered"
    );
    assert!(
        !stale_events.iter().any(
            |e| matches!(e,MediaEvent::PublicationRtpRegistered{track_sid,..}if track_sid==&new_sid)
        ),
        "old generation's first-delivery authenticated SSRC must not prove new CID/TrackSID"
    );
    assert!(
        engine.transports[&tid].publication_rtp.is_empty(),
        "old authenticated packet must not be cached under new CID"
    );
    // A fix cannot suppress all same-MID publications: actual new-source SRTP
    // must still produce the one current-generation proof without full frame.
    let mut registered = 0;
    let mut new_sent = false;
    let mut new_protected_output = 0;
    let mut other_output = 0;
    for tick in 1..2001u64 {
        let at = now + Duration::from_millis(tick);
        peer.handle_input(Input::Timeout(at)).unwrap();
        if !new_sent {
            peer.direct_api().stream_tx(&new_ssrc).unwrap().write_rtp(
                RtpWrite::new(
                    current_pt,
                    1.into(),
                    180_000,
                    at,
                    vec![0x10, 0, 0, 0x9d, 1, 0x2a],
                )
                .marker(false),
            );
            new_sent = true;
        }
        loop {
            match peer.poll_output().unwrap() {
                Output::Transmit(packet) => {
                    let bytes = &packet.contents;
                    let is_new = bytes.len() >= 12
                        && (128..192).contains(&bytes[0])
                        && bytes[1] & 127 == *current_pt
                        && u32::from_be_bytes(bytes[8..12].try_into().unwrap()) == *new_ssrc
                        && u16::from_be_bytes(bytes[2..4].try_into().unwrap()) == 1;
                    if is_new {
                        new_protected_output += 1;
                    } else {
                        other_output += 1;
                    }
                    engine.handle_datagram(Datagram {
                        transport: tid,
                        proto: Proto::Udp,
                        source: peer_addr,
                        destination: server_addr,
                        data: bytes,
                        received_at: at,
                    });
                }
                Output::Timeout(_) => break,
                _ => {}
            }
        }
        engine.handle_timeout(at);
        for (destination, bytes) in packets.0.lock().unwrap().drain(..).collect::<Vec<_>>() {
            assert_eq!(destination, peer_addr);
            peer.handle_input(Input::Receive(
                at,
                Receive::new(Protocol::Udp, server_addr, peer_addr, &bytes).unwrap(),
            ))
            .unwrap();
        }
        let mut events = Vec::new();
        engine.drain_events(&mut events);
        registered+=events.iter().filter(|e|matches!(e,MediaEvent::PublicationRtpRegistered{track_sid,..}if track_sid==&new_sid)).count();
        if registered > 0 {
            break;
        }
    }
    eprintln!(
        "generation completion: write_queued={} new_protected_output={} other_output={} peer_connected={} receiver_connected={} current_proof_cached={}",
        new_sent,
        new_protected_output,
        other_output,
        peer.is_connected(),
        engine.transports[&tid].rtc.is_connected(),
        !engine.transports[&tid].publication_rtp.is_empty()
    );
    assert!(new_sent, "positive test queued actual new-source packet");
    assert!(
        new_protected_output > 0,
        "positive control must actually emit new-source SRTP, not merely enqueue write_rtp"
    );
    assert_eq!(
        registered, 1,
        "new native primary source must still register exactly once"
    );
}

#[test]
fn publication_generation_delayed_source_does_not_register_rebound_track() {
    publication_generation_case(true);
}
#[test]
fn publication_generation_new_source_registers_without_delayed_packet() {
    publication_generation_case(false);
}

// Physical protocol tests plus the real Room consumer. No fabricated fatal events.
mod f93_closed_write {
    use super::*;
    use rtc_core::config::RoomPolicy;
    use rtc_core::data::{DataHandler, DataSender, DataVerdict};
    use rtc_core::ids::*;
    use rtc_core::media_api::MediaCommand;
    use rtc_core::model::*;
    use rtc_core::permission::{Grants, VideoGrant};
    use rtc_core::room::{Effect as RoomEffect, Room};
    use rtc_core::session::*;
    use std::collections::BTreeMap;
    pub(super) struct F93Drop;
    impl DataHandler for F93Drop {
        fn inbound(&self, _: &DataSender<'_>, _: DataKind, _: &str, _: &[u8]) -> DataVerdict {
            DataVerdict::Drop
        }
    }
    pub(super) fn f93_policy() -> RoomPolicy {
        RoomPolicy {
            transport_fallback: Default::default(),
            enabled_codecs: vec![
                CodecSpec {
                    mime: "audio/opus".into(),
                    fmtp_line: String::new(),
                },
                CodecSpec {
                    mime: "video/VP8".into(),
                    fmtp_line: String::new(),
                },
                CodecSpec {
                    mime: "video/rtx".into(),
                    fmtp_line: String::new(),
                },
            ],
            max_participants: 0,
            empty_timeout: Duration::from_secs(300),
            departure_timeout: Duration::from_secs(20),
            enable_remote_unmute: false,
            playout_delay: None,
            enable_data_tracks: true,
            enable_participant_data_blob: false,
            max_data_track_custom_encoding_length: 32,
            max_data_blob_key_length: 256,
            max_data_blob_size: 64000,
            sync_streams: false,
            enable_full_room_in_webhooks: false,
            update_batch_target_size: 128 << 10,
            subscription_limit_video: None,
            subscription_limit_audio: None,
            max_participant_name_length: 256,
            max_metadata_size: Some(512 << 10),
            max_attributes_size: Some(64 << 10),
            use_ice_lite: false,
            allow_pause: false,
            reconnect_on_publication_error: false,
            reconnect_on_subscription_error: false,
            reconnect_on_data_channel_error: false,
            region: String::new(),
        }
    }

    pub(super) fn f93_params(identity: &str, protocol: i32, single_pc: bool) -> JoinParams {
        JoinParams {
            room: RoomKey::new(ProjectId::local(), "r1"),
            grants: Grants {
                identity: identity.into(),
                video: VideoGrant {
                    room_join: true,
                    room: "r1".into(),
                    ..Default::default()
                },
                ..Default::default()
            },
            identity: identity.into(),
            name: String::new(),
            metadata: String::new(),
            attributes: BTreeMap::new(),
            client: ClientMeta {
                sdk: SdkKind::Js,
                protocol,
                ..Default::default()
            },
            capabilities: ClientCapabilities {
                video_publication_requires_rtp: false,
                codec_switch_requires_renegotiation: false,
                incomplete_rtcp_sender_reports: false,
                ice_tcp: true,
                one_shot: false,
                subscriber_primary: true,
                speaker_changed: true,
                connection_quality: true,
                ice_lite: true,
                unpublish: true,
                fast_start: true,
                disconnected_update: true,
                sync_stream_id: true,
                disable_playout_delay_for_sync: false,
                connection_quality_lost: true,
                identity_based_reconnection: true,
                regions_in_leave: true,
                non_error_signal_response: true,
                request_response: true,
                audio_red: true,
            },
            auto_subscribe: true,
            auto_subscribe_data_track: false,
            adaptive_stream: false,
            single_pc,
            disable_ice_lite: false,
            subscriber_allow_pause: None,
            reconnect: false,
            reconnect_reason: 0,
            participant_sid: None,
            add_tracks: vec![],
            publisher_offer: None,
        }
    }

    #[test]
    fn public_joining_reliable_messages_replay_only_after_real_ice_dtls_both_channels() {
        use crate::net::Datagram;
        use str0m::net::{Protocol, Receive};
        use str0m::{Candidate, Event, Input, Output};
        let start = Instant::now();
        let mut room = Room::new(
            RoomKey::new(ProjectId::local(), "r1"),
            std::sync::Arc::new(f93_policy()),
            std::sync::Arc::new(F93Drop),
            &Default::default(),
            start,
        );
        room.take_effects();
        let joined = room
            .join(f93_params("joining-wire", 17, true), start)
            .unwrap();
        let spec = room
            .take_effects()
            .into_iter()
            .find_map(|e| match e {
                RoomEffect::Media(MediaCommand::CreateTransport(s)) => Some(s),
                _ => None,
            })
            .unwrap();
        let tid = spec.id;
        let mut engine = engine(start);
        let packets = PacketQueue::default();
        engine.out = Box::new(packets.clone());
        engine.create_transport(spec, IceCredentials::random(), vec![], start);
        // Original public API, invoked before any transport/channel exists. Two
        // encoded-byte-equivalent messages exactly fill source100000 budget.
        room.api_send_data(DataKind::Reliable, vec![1; 50_000], &[], &[]);
        room.api_send_data(DataKind::Reliable, vec![2; 50_000], &[], &[]);
        room.api_send_data(DataKind::Reliable, vec![3], &[], &[]);
        room.api_send_data(DataKind::Lossy, vec![9], &[], &[]);
        assert!(
            room.take_effects().is_empty(),
            "joining data is not prematurely written or signalled"
        );
        let server_addr = "127.0.0.1:9171".parse().unwrap();
        let client_addr = "127.0.0.1:9172".parse().unwrap();
        let t = engine.transports.get_mut(&tid).unwrap();
        let candidate = t
            .rtc
            .add_local_candidate(Candidate::host(server_addr, "udp").unwrap())
            .unwrap()
            .clone();
        t.candidates.push(candidate);
        let mut peer = client(start);
        peer.add_local_candidate(Candidate::host(client_addr, "udp").unwrap());
        let mut reliable = None;
        offer(&mut engine, &mut peer, tid, start, |change| {
            reliable = Some(change.add_channel("_reliable".into()));
        });
        let reliable = reliable.unwrap();
        let mut delivered = Vec::new();
        let mut lossy = None;
        for tick in 0..10000_u64 {
            let now = start + Duration::from_millis(tick);
            peer.handle_input(Input::Timeout(now)).unwrap();
            loop {
                match peer.poll_output().unwrap() {
                    Output::Transmit(p) => engine.handle_datagram(Datagram {
                        transport: tid,
                        proto: Proto::Udp,
                        source: client_addr,
                        destination: server_addr,
                        data: &p.contents,
                        received_at: now,
                    }),
                    Output::Event(Event::ChannelData(d)) => {
                        assert_eq!(d.id, reliable);
                        delivered.push(d.data);
                    }
                    Output::Timeout(_) => break,
                    _ => {}
                }
            }
            engine.handle_timeout(now);
            engine.run(now);
            if lossy.is_none()
                && tick >= 1000
                && engine.transports[&tid].connected
                && engine.transports[&tid]
                    .channel_for(DataKind::Reliable)
                    .is_some()
            {
                assert!(
                    delivered.is_empty(),
                    "ACTIVE plus one actual reliable writer is not source fully-established"
                );
                lossy = Some(peer.direct_api().create_data_channel(
                    str0m::channel::ChannelConfig {
                        label: "_lossy".into(),
                        ordered: false,
                        reliability: str0m::channel::Reliability::MaxRetransmits { retransmits: 0 },
                        ..Default::default()
                    },
                ));
            }
            let mut events = Vec::new();
            engine.drain_events(&mut events);
            // Only actual engine events cross the owner seam. No fabricated
            // TransportConnected/DataChannelReady event makes this test pass.
            for event in events {
                room.handle_media_event(event, now);
            }
            for effect in room.take_effects() {
                if let RoomEffect::Media(command) = effect {
                    engine.handle_command(command, now);
                }
            }
            // Own the datagrams before returning traffic to engine/PacketQueue.
            // A100k source burst must not fill the peer DTLS receive queue while
            // the fixture refuses to poll its real decrypted application/ACK output.
            let server_packets = {
                let mut q = packets.0.lock().unwrap();
                q.drain(..).collect::<Vec<_>>()
            };
            for bytes in server_packets {
                peer.handle_input(Input::Receive(
                    now,
                    Receive::new(Protocol::Udp, server_addr, client_addr, &bytes).unwrap(),
                ))
                .unwrap();
                loop {
                    match peer.poll_output().unwrap() {
                        Output::Transmit(p) => engine.handle_datagram(Datagram {
                            transport: tid,
                            proto: Proto::Udp,
                            source: client_addr,
                            destination: server_addr,
                            data: &p.contents,
                            received_at: now,
                        }),
                        Output::Event(Event::ChannelData(d)) => {
                            assert_eq!(d.id, reliable);
                            delivered.push(d.data);
                        }
                        Output::Timeout(_) => break,
                        _ => {}
                    }
                }
            }
            if delivered.len() >= 2 {
                assert_eq!(delivered, vec![vec![1; 50_000], vec![2; 50_000]]);
                // Another actual second of pumping must not produce overflow,
                // joining lossy, or a second replay. Continue to the original bound.
                if tick > 3000 {
                    assert!(lossy.is_some());
                    assert_eq!(joined.sid, engine.transports[&tid].spec.participant);
                    return;
                }
            }
        }
        panic!(
            "public joining replay missing: received={} (no timeout relaxation)",
            delivered.len()
        );
    }

    fn half_ready_failed_resume_case(reason: i32) {
        use crate::net::Datagram;
        use str0m::net::{Protocol, Receive};
        use str0m::{Candidate, Event, Input, Output};
        let start = Instant::now();
        let mut room = Room::new(
            RoomKey::new(ProjectId::local(), "r1"),
            std::sync::Arc::new(f93_policy()),
            std::sync::Arc::new(F93Drop),
            &Default::default(),
            start,
        );
        room.take_effects();
        let joined = room
            .join(f93_params("joining-wire", 17, true), start)
            .unwrap();
        let spec = room
            .take_effects()
            .into_iter()
            .find_map(|e| match e {
                RoomEffect::Media(MediaCommand::CreateTransport(s)) => Some(s),
                _ => None,
            })
            .unwrap();
        let tid = spec.id;
        let mut engine = engine(start);
        let packets = PacketQueue::default();
        engine.out = Box::new(packets.clone());
        engine.create_transport(spec, IceCredentials::random(), vec![], start);
        // Original public API, invoked before any transport/channel exists. Two
        // encoded-byte-equivalent messages exactly fill source100000 budget.
        room.api_send_data(DataKind::Reliable, vec![1; 50_000], &[], &[]);
        room.api_send_data(DataKind::Reliable, vec![2; 50_000], &[], &[]);
        room.api_send_data(DataKind::Reliable, vec![3], &[], &[]);
        room.api_send_data(DataKind::Lossy, vec![9], &[], &[]);
        assert!(
            room.take_effects().is_empty(),
            "joining data is not prematurely written or signalled"
        );
        let server_addr = "127.0.0.1:9171".parse().unwrap();
        let client_addr = "127.0.0.1:9172".parse().unwrap();
        let t = engine.transports.get_mut(&tid).unwrap();
        let candidate = t
            .rtc
            .add_local_candidate(Candidate::host(server_addr, "udp").unwrap())
            .unwrap()
            .clone();
        t.candidates.push(candidate);
        let mut peer = client(start);
        peer.add_local_candidate(Candidate::host(client_addr, "udp").unwrap());
        let mut reliable = None;
        offer(&mut engine, &mut peer, tid, start, |change| {
            reliable = Some(change.add_channel("_reliable".into()));
        });
        let reliable = reliable.unwrap();
        let mut delivered = Vec::new();
        let mut lossy = None;
        let mut restarted = false;
        let mut late_writer_proved = false;
        for tick in 0..10000_u64 {
            let now = start + Duration::from_millis(tick);
            peer.handle_input(Input::Timeout(now)).unwrap();
            loop {
                match peer.poll_output().unwrap() {
                    Output::Transmit(p) => {
                        let stun = p.contents.len() >= 20
                            && p.contents[0] < 4
                            && p.contents[4..8] == [0x21, 0x12, 0xa4, 0x42];
                        if !(restarted && tick < 2500 && stun) {
                            engine.handle_datagram(Datagram {
                                transport: tid,
                                proto: Proto::Udp,
                                source: client_addr,
                                destination: server_addr,
                                data: &p.contents,
                                received_at: now,
                            });
                        }
                    }
                    Output::Event(Event::ChannelData(d)) => {
                        assert_eq!(d.id, reliable);
                        delivered.push(d.data);
                    }
                    Output::Timeout(_) => break,
                    _ => {}
                }
            }
            engine.handle_timeout(now);
            engine.run(now);
            if !restarted
                && tick >= 1000
                && engine.transports[&tid].connected
                && engine.transports[&tid]
                    .channel_for(DataKind::Reliable)
                    .is_some()
            {
                assert!(delivered.is_empty(), "half-ready queue has not replayed");
                let mut resumed = f93_params("joining-wire", 17, true);
                resumed.reconnect = true;
                resumed.reconnect_reason = reason;
                resumed.participant_sid = Some(joined.sid.clone());
                assert_eq!(room.join(resumed, now).unwrap().sid, joined.sid);
                for effect in room.take_effects() {
                    if let RoomEffect::Media(command) = effect {
                        engine.handle_command(command, now);
                    }
                }
                offer(&mut engine, &mut peer, tid, now, |change| {
                    change.ice_restart(true);
                });
                restarted = true;
                lossy = Some(peer.direct_api().create_data_channel(
                    str0m::channel::ChannelConfig {
                        label: "_lossy".into(),
                        ordered: false,
                        reliability: str0m::channel::Reliability::MaxRetransmits { retransmits: 0 },
                        ..Default::default()
                    },
                ));
            }
            if restarted && tick == 2400 {
                assert!(
                    engine.transports[&tid]
                        .channel_for(DataKind::Lossy)
                        .is_some(),
                    "real late DCEP must actually open before releasing restart STUN; otherwise fixture premise failed"
                );
                assert!(
                    !engine.transports[&tid].rtc.is_connected(),
                    "genuine restart must still lack a nominated connected pair while its STUN is withheld"
                );
                late_writer_proved = true;
                if reason == 2 {
                    assert!(
                        delivered.is_empty(),
                        "failed-reconnect reset must prevent historical Connected from authorizing late writer replay"
                    );
                }
            }
            if reason == 2 && tick == 3200 {
                assert!(
                    engine.transports[&tid].rtc.is_connected(),
                    "actual resumed ICE/DTLS connection must recover"
                );
                assert!(
                    delivered.is_empty(),
                    "source subsequent PC Connected alone does not invoke fully-established callback"
                );
                // Source invokes maybeNotifyFullyEstablished after every handled
                // actual channel OnOpen, including a permitted unlabeled channel.
                peer.direct_api()
                    .create_data_channel(str0m::channel::ChannelConfig {
                        label: "_source_readiness_observer".into(),
                        ..Default::default()
                    });
            }
            let mut events = Vec::new();
            engine.drain_events(&mut events);
            // Only actual engine events cross the owner seam. No fabricated
            // TransportConnected/DataChannelReady event makes this test pass.
            for event in events {
                room.handle_media_event(event, now);
            }
            for effect in room.take_effects() {
                if let RoomEffect::Media(command) = effect {
                    engine.handle_command(command, now);
                }
            }
            let server_packets = {
                let mut q = packets.0.lock().unwrap();
                q.drain(..).collect::<Vec<_>>()
            };
            for bytes in server_packets {
                let stun =
                    bytes.len() >= 20 && bytes[0] < 4 && bytes[4..8] == [0x21, 0x12, 0xa4, 0x42];
                if !(restarted && tick < 2500 && stun) {
                    peer.handle_input(Input::Receive(
                        now,
                        Receive::new(Protocol::Udp, server_addr, client_addr, &bytes).unwrap(),
                    ))
                    .unwrap();
                    loop {
                        match peer.poll_output().unwrap() {
                            Output::Transmit(p) => {
                                let stun = p.contents.len() >= 20
                                    && p.contents[0] < 4
                                    && p.contents[4..8] == [0x21, 0x12, 0xa4, 0x42];
                                if !(restarted && tick < 2500 && stun) {
                                    engine.handle_datagram(Datagram {
                                        transport: tid,
                                        proto: Proto::Udp,
                                        source: client_addr,
                                        destination: server_addr,
                                        data: &p.contents,
                                        received_at: now,
                                    });
                                }
                            }
                            Output::Event(Event::ChannelData(d)) => {
                                assert_eq!(d.id, reliable);
                                delivered.push(d.data);
                            }
                            Output::Timeout(_) => break,
                            _ => {}
                        }
                    }
                }
            }
            if delivered.len() >= 2 {
                assert_eq!(delivered, vec![vec![1; 50_000], vec![2; 50_000]]);
                // Another actual second of pumping must not produce overflow,
                // joining lossy, or a second replay. Continue to the original bound.
                if tick > 3000 {
                    assert!(late_writer_proved);
                    assert!(lossy.is_some());
                    assert_eq!(joined.sid, engine.transports[&tid].spec.participant);
                    return;
                }
            }
        }
        panic!(
            "public joining replay missing: received={} (no timeout relaxation)",
            delivered.len()
        );
    }

    #[test]
    fn public_failed_resume_real_restart_late_writer_requires_new_connection() {
        half_ready_failed_resume_case(2);
    }
    #[test]
    fn public_nonfailure_resume_real_restart_does_not_reset_source_connection_history() {
        half_ready_failed_resume_case(0);
    }

    // Append to existing rtc-media/src/regression.rs in an isolated test tree only.
    // Real ICE/DTLS/SCTP engine/client exchange; no injected fatal actor event.
    fn f93_physical_remote_close(
        kind: DataKind,
        attempt_closed_write: bool,
        reopen: bool,
        reconnect: Option<bool>,
        replace_open: bool,
    ) -> (usize, usize, Option<bool>) {
        use crate::net::Datagram;
        use str0m::net::{Protocol, Receive};
        use str0m::{Candidate, Event, Input, Output};
        let start = Instant::now();
        let mut engine = engine(start);
        let packets = PacketQueue::default();
        engine.out = Box::new(packets.clone());
        let (mut actual_room, tid, participant) = if let Some(enabled) = reconnect {
            let mut policy = f93_policy();
            policy.reconnect_on_data_channel_error = enabled;
            let mut room = Room::new(
                RoomKey::new(ProjectId::local(), "r1"),
                std::sync::Arc::new(policy),
                std::sync::Arc::new(F93Drop),
                &Default::default(),
                start,
            );
            room.take_effects();
            let joined = room
                .join(f93_params("physical-owner", 17, true), start)
                .unwrap();
            let effects = room.take_effects();
            let spec = effects
                .into_iter()
                .find_map(|e| match e {
                    RoomEffect::Media(MediaCommand::CreateTransport(spec)) => Some(spec),
                    _ => None,
                })
                .expect("actual owner created a transport");
            let tid = spec.id;
            assert_eq!(spec.participant, joined.sid);
            engine.create_transport(spec, IceCredentials::random(), vec![], start);
            (Some(room), tid, joined.sid)
        } else {
            (
                None,
                transport(&mut engine, PcRole::Single, start),
                ParticipantSid::from("PA_test"),
            )
        };
        let label = match kind {
            DataKind::Reliable => "_reliable",
            DataKind::Lossy | DataKind::Unrecognized(_) => "_lossy",
        };
        let channel_config = str0m::channel::ChannelConfig {
            label: label.into(),
            ordered: kind == DataKind::Reliable,
            reliability: if kind == DataKind::Reliable {
                str0m::channel::Reliability::Reliable
            } else {
                str0m::channel::Reliability::MaxRetransmits { retransmits: 0 }
            },
            ..Default::default()
        };
        let mut room_reconnected = false;
        let server_addr = "127.0.0.1:9101".parse().unwrap();
        let client_addr = "127.0.0.1:9102".parse().unwrap();
        let t = engine.transports.get_mut(&tid).unwrap();
        let candidate = t
            .rtc
            .add_local_candidate(Candidate::host(server_addr, "udp").unwrap())
            .unwrap()
            .clone();
        t.candidates.push(candidate);
        let mut client = client(start);
        client.add_local_candidate(Candidate::host(client_addr, "udp").unwrap());
        let mut cid = None;
        offer(&mut engine, &mut client, tid, start, |change| {
            cid = Some(change.add_channel_with_config(channel_config.clone()));
            // Source ordinary primary readiness uses both labeled channels;
            // keep original closed-writer and reconnect assertions unchanged.
            change.add_channel_with_config(str0m::channel::ChannelConfig {
                label: if kind == DataKind::Reliable {
                    "_lossy"
                } else {
                    "_reliable"
                }
                .into(),
                ordered: kind != DataKind::Reliable,
                reliability: if kind == DataKind::Reliable {
                    str0m::channel::Reliability::MaxRetransmits { retransmits: 0 }
                } else {
                    str0m::channel::Reliability::Reliable
                },
                ..Default::default()
            });
        });
        let cid = cid.unwrap();
        engine.send_data(&participant, kind, b"before-established");
        let mut pre = Vec::new();
        engine.drain_events(&mut pre);
        assert!(
            !pre.iter()
                .any(|e| matches!(e, MediaEvent::DataSendError { .. })),
            "never-open is not closed-write"
        );
        let mut delivered = false;
        let mut close_requested = false;
        let mut peer_closed = false;
        let mut server_had_channel = false;
        let mut closed_at = None;
        let mut sent_after_close = false;
        let mut fatal = 0usize;
        let mut sent_preclose = false;
        let mut actual_server_channel = None;
        let mut new_channel = None;
        let mut sent_new = false;
        let mut received_new = false;
        // Upstream Pion has no aggregate SCTP queue cap for ordinary writes, so
        // the oversized (> str0m default buffered amount, < negotiated
        // max-message-size) API message is really transmitted and delivered.
        let mut oversized_delivered = false;
        let oversized_len = Rtc::builder().sctp_max_buffered_amount() + 1;
        // Fixture fairness: the peer's decrypted output/ACKs are drained after
        // each datagram so a large burst cannot overflow its DTLS receive queue.
        macro_rules! pump_client {
            ($now:expr) => {
                loop {
                    match client.poll_output().unwrap() {
                        Output::Transmit(p) => engine.handle_datagram(Datagram {
                            transport: tid,
                            proto: Proto::Udp,
                            source: client_addr,
                            destination: server_addr,
                            data: &p.contents,
                            received_at: $now,
                        }),
                        Output::Event(Event::ChannelData(d)) => {
                            if d.id == cid && d.data.len() == oversized_len {
                                assert!(d.data.iter().all(|b| *b == 0));
                                if kind == DataKind::Reliable {
                                    assert!(
                                        !delivered,
                                        "ordered: oversized precedes the open write"
                                    );
                                }
                                oversized_delivered = true;
                            } else if d.id == cid {
                                assert_eq!(d.data, b"actual-open-write");
                                delivered = true;
                            } else {
                                assert_eq!(Some(d.id), new_channel);
                                assert_eq!(d.data, b"actual-new-writer");
                                received_new = true;
                            }
                        }
                        Output::Event(Event::ChannelClose(id)) if id == cid => peer_closed = true,
                        Output::Timeout(_) => break,
                        _ => {}
                    }
                }
            };
        }
        for tick in 0..10000u64 {
            let now = start + Duration::from_millis(tick);
            client.handle_input(Input::Timeout(now)).unwrap();
            pump_client!(now);
            engine.handle_timeout(now);
            engine.run(now);
            let mut events = Vec::new();
            engine.drain_events(&mut events);
            let count = events.iter().filter(|e| matches!(e, MediaEvent::DataSendError { participant, transport } if *participant == engine.transports[&tid].spec.participant && *transport == tid)).count();
            assert!(
                sent_after_close || count == 0,
                "neither close alone nor pressure/never-open may issue fatal callback"
            );
            fatal += count;
            if let Some(room) = actual_room.as_mut() {
                // Only actual engine-produced events cross the real consumer seam.
                // Never synthesize a DataSendError to make the flag test pass.
                for event in events {
                    room.handle_media_event(event, now);
                }
                let reconnect_effect = room.take_effects().into_iter().any(|e| matches!(e,
                RoomEffect::Signal { to, event: SignalEvent::Leave { action: LeaveAction::Reconnect, .. } }
                    if to == participant));
                assert!(
                    sent_after_close || !reconnect_effect,
                    "close alone is not a reconnect"
                );
                room_reconnected |= reconnect_effect;
            }
            let open = engine.transports[&tid].channel_for(kind).is_some();
            server_had_channel |= open;
            if open && !sent_preclose {
                if let Some(room) = actual_room.as_mut() {
                    assert_eq!(
                        room.get_participant(&"physical-owner".into())
                            .unwrap()
                            .state,
                        ParticipantState::Active,
                        "actual engine connection activated the real owner"
                    );
                }
                // Exact public native buffer-admission negative control. Engine uses
                // the unchanged default SCTP capacity, no per-test lowered budget.
                let oversized = vec![0u8; Rtc::builder().sctp_max_buffered_amount() + 1];
                let server_cid = engine.transports[&tid].channel_for(kind).unwrap().0;
                actual_server_channel = Some(server_cid);
                assert!(
                    !engine
                        .transports
                        .get_mut(&tid)
                        .unwrap()
                        .rtc
                        .channel(server_cid)
                        .unwrap()
                        .write(true, &oversized)
                        .unwrap(),
                    "actual native buffer pressure must be Ok(false)"
                );
                engine.send_data(&participant, kind, &oversized);
                engine.run(now);
                let mut pressure = Vec::new();
                engine.drain_events(&mut pressure);
                assert!(
                    !pressure
                        .iter()
                        .any(|e| matches!(e, MediaEvent::DataSendError { .. })),
                    "oversized ordinary write is nonfatal"
                );
                engine.send_data(&participant, kind, b"actual-open-write");
                engine.run(now);
                sent_preclose = true;
            }
            if delivered && replace_open && new_channel.is_none() {
                assert!(!peer_closed, "old peer channel must still be open");
                let new = client
                    .direct_api()
                    .create_data_channel(channel_config.clone());
                assert_ne!(new, cid, "replacement label has a fresh ChannelId");
                new_channel = Some(new);
                close_requested = true;
            }
            if delivered && !close_requested {
                client.direct_api().close_data_channel(cid);
                close_requested = true;
            }
            let old_open = engine.transports[&tid]
                .channel_for(kind)
                .is_some_and(|(id, _)| Some(id) == actual_server_channel);
            if close_requested && server_had_channel && !old_open && closed_at.is_none() {
                closed_at = Some(tick);
            }
            if let Some(at) = closed_at {
                // Keep a real no-send interval after observed reset; an immediate
                // callback-on-Close bug cannot hide behind the later write result.
                if tick > at + 20 && attempt_closed_write && !sent_after_close {
                    assert!(
                        engine
                            .transports
                            .get_mut(&tid)
                            .unwrap()
                            .rtc
                            .channel(actual_server_channel.expect("actual open writer ID"))
                            .is_none(),
                        "native lookup no longer yields a write handle after actual reset"
                    );
                    engine.send_data(&participant, kind, b"actual-post-reset-write");
                    engine.run(now);
                    sent_after_close = true;
                }
                if reopen
                    && !replace_open
                    && peer_closed
                    && tick > at + 50
                    && (!attempt_closed_write || sent_after_close)
                    && new_channel.is_none()
                {
                    let new = client
                        .direct_api()
                        .create_data_channel(channel_config.clone());
                    assert_ne!(new, cid, "reopened label has a fresh ChannelId");
                    new_channel = Some(new);
                }
            }
            if new_channel.is_some()
                && !sent_new
                && engine.transports[&tid]
                    .channel_for(kind)
                    .is_some_and(|(id, _)| Some(id) != actual_server_channel)
            {
                engine.send_data(&participant, kind, b"actual-new-writer");
                engine.run(now);
                sent_new = true;
            }
            let outbound: Vec<_> = packets.0.lock().unwrap().drain(..).collect();
            for bytes in outbound {
                client
                    .handle_input(Input::Receive(
                        now,
                        Receive::new(Protocol::Udp, server_addr, client_addr, &bytes).unwrap(),
                    ))
                    .unwrap();
                pump_client!(now);
            }
            if peer_closed
                && closed_at.is_some()
                && (!attempt_closed_write || sent_after_close)
                && (!reopen || received_new)
                && tick > 1500
            {
                assert!(delivered && server_had_channel);
                assert!(
                    oversized_delivered,
                    "upstream-uncapped ordinary write is delivered"
                );
                return (fatal, tick as usize, reconnect.map(|_| room_reconnected));
            }
        }
        panic!(
            "physical close did not converge: delivered={delivered}, peer_closed={peer_closed}, closed_at={closed_at:?}, new_delivered={received_new}"
        );
    }

    #[test]
    fn physical_sctp_closed_write_mechanism_observation() {
        for kind in [DataKind::Reliable, DataKind::Lossy] {
            let (fatal, elapsed_ms, _) = f93_physical_remote_close(kind, true, false, None, false);
            eprintln!(
                "F93_PHYSICAL kind={kind:?} closed_write_fatal_events={fatal} elapsed_ms={elapsed_ms}"
            );
            assert!(fatal <= 1, "one attempted write cannot duplicate callbacks");
        }
    }
    #[test]
    fn physical_sctp_close_without_send_is_nonfatal_and_same_label_reopen_delivers() {
        for kind in [DataKind::Reliable, DataKind::Lossy] {
            let (fatal, _, _) = f93_physical_remote_close(kind, false, true, None, false);
            assert_eq!(fatal, 0, "Close alone/reopened writer is not a failed send");
        }
    }
    #[test]
    fn physical_sctp_same_label_replaces_open_writer_and_delivers() {
        for kind in [DataKind::Reliable, DataKind::Lossy] {
            let (fatal, _, _) = f93_physical_remote_close(kind, false, true, None, true);
            assert_eq!(fatal, 0, "retiring an old writer is not a failed send");
        }
    }
    fn actual_go_results() -> Vec<(DataKind, bool)> {
        let text =
            std::fs::read_to_string(std::env::var("F93_GO_ORACLE_RESULTS").expect("actual Go TSV"))
                .unwrap();
        let rows: Vec<_> = text.lines().collect();
        assert_eq!(rows.len(), 2);
        [DataKind::Reliable, DataKind::Lossy]
            .into_iter()
            .zip(rows)
            .map(|(kind, row)| {
                let fields: Vec<_> = row.split('\t').collect();
                assert_eq!(fields.len(), 5);
                assert_eq!(fields[0], "real-peer-stream-reset");
                assert_eq!(
                    fields[1],
                    if kind == DataKind::Reliable {
                        "reliable"
                    } else {
                        "lossy"
                    }
                );
                assert_eq!(fields[2], "true");
                let fatal = match fields[4] {
                    "true" => true,
                    "false" => false,
                    _ => panic!("actual Go predicate"),
                };
                assert_eq!(
                    fatal,
                    matches!(fields[3], "sctp.ErrStreamClosed" | "io.ErrClosedPipe")
                );
                (kind, fatal)
            })
            .collect()
    }
    #[test]
    #[ignore = "actual pinned Pion + fixed LiveKit writer TSV F93_GO_ORACLE_RESULTS"]
    fn physical_sctp_closed_write_matches_actual_go_oracle() {
        for (kind, fatal) in actual_go_results() {
            for reopen in [false, true] {
                let (actual, _, _) = f93_physical_remote_close(kind, true, reopen, None, false);
                assert_eq!(
                    actual,
                    usize::from(fatal),
                    "actual Go result is the authority"
                );
            }
        }
    }
    #[test]
    #[ignore = "actual engine events -> real Room; actual Go F93_GO_ORACLE_RESULTS required"]
    fn physical_closed_write_drives_real_owner_flag_consumer() {
        for (kind, fatal) in actual_go_results() {
            for enabled in [false, true] {
                let (actual, _, reconnected) =
                    f93_physical_remote_close(kind, true, false, Some(enabled), false);
                assert_eq!(
                    actual,
                    usize::from(fatal),
                    "physical compatibility must precede flag assertion"
                );
                assert_eq!(reconnected, Some(fatal && enabled));
            }
        }
    }
}

// Candidate-only bounded adapter-state controls. These deliberately seed the
// terminal slot to test selection/lifetime, not to claim physical error proof.
#[test]
fn terminal_data_slot_is_two_bits_and_send_selects_terminal_primary_before_secondary() {
    let now = Instant::now();
    let mut engine = engine(now);
    let primary = transport(&mut engine, PcRole::Single, now);
    let secondary = transport(&mut engine, PcRole::Publisher, now);
    engine.transports.get_mut(&secondary).unwrap().spec.primary = false;
    let t = engine.transports.get_mut(&secondary).unwrap();
    let cid = t.rtc.sdp_api().add_channel("_reliable".into());
    t.channels.push(ChannelEntry {
        id: cid,
        label: "_reliable".into(),
        kind: Some(DataKind::Reliable),
    });
    engine
        .transports
        .get_mut(&primary)
        .unwrap()
        .closed_data_kinds = [true, false];
    assert_eq!(
        std::mem::size_of_val(&engine.transports[&primary].closed_data_kinds),
        2,
        "constant two bools, no per-channel tombstone collection"
    );
    let mut events = Vec::new();
    engine.drain_events(&mut events);
    assert!(
        !events
            .iter()
            .any(|e| matches!(e, MediaEvent::DataSendError { .. })),
        "terminal state alone is not a write callback"
    );
    engine.send_data(
        &"PA_test".into(),
        DataKind::Lossy,
        b"unrelated-missing-kind",
    );
    engine.drain_events(&mut events);
    assert!(
        !events
            .iter()
            .any(|e| matches!(e, MediaEvent::DataSendError { .. })),
        "reliable terminal slot cannot authorize lossy fatal"
    );
    events.clear();
    engine.send_data(
        &"PA_test".into(),
        DataKind::Reliable,
        b"actual-selection-command",
    );
    engine.drain_events(&mut events);
    let matching: Vec<_> = events
        .iter()
        .filter_map(|e| match e {
            MediaEvent::DataSendError { transport, .. } => Some(*transport),
            _ => None,
        })
        .collect();
    assert_eq!(
        matching,
        vec![primary],
        "closed primary retained writer must not fall back to a secondary label"
    );
}

#[test]
fn terminal_data_slot_new_open_and_transport_removal_do_not_replay_old_rights() {
    let now = Instant::now();
    let mut engine = engine(now);
    let tid = transport(&mut engine, PcRole::Single, now);
    engine.transports.get_mut(&tid).unwrap().closed_data_kinds = [true, true];
    let cid = engine
        .transports
        .get_mut(&tid)
        .unwrap()
        .rtc
        .sdp_api()
        .add_channel("_reliable".into());
    engine.on_channel_open(tid, cid, "_reliable".into());
    assert_eq!(
        engine.transports[&tid].closed_data_kinds,
        [false, true],
        "new reliable writer clears only its own kind"
    );
    engine.send_data(
        &"PA_test".into(),
        DataKind::Reliable,
        b"not-yet-established-new-writer",
    );
    let mut events = Vec::new();
    engine.drain_events(&mut events);
    assert!(
        !events
            .iter()
            .any(|e| matches!(e, MediaEvent::DataSendError { .. })),
        "public native channel None before establishment is nonfatal"
    );
    engine.handle_command(rtc_core::media_api::MediaCommand::CloseTransport(tid), now);
    assert!(!engine.transports.contains_key(&tid));
    let fresh = transport(&mut engine, PcRole::Single, now);
    assert_ne!(fresh, tid);
    assert_eq!(
        engine.transports[&fresh].closed_data_kinds,
        [false, false],
        "new transport generation has no old terminal state"
    );
}

#[test]
fn terminal_data_slot_unusable_transport_is_not_closed_writer_delivery_authority() {
    let now = Instant::now();
    let mut engine = engine(now);
    let tid = transport(&mut engine, PcRole::Single, now);
    let t = engine.transports.get_mut(&tid).unwrap();
    t.closed_data_kinds = [true, true];
    t.failed = true;
    engine.send_data(&"PA_test".into(), DataKind::Reliable, b"failed-transport");
    let mut events = Vec::new();
    engine.drain_events(&mut events);
    assert!(
        !events
            .iter()
            .any(|e| matches!(e, MediaEvent::DataSendError { .. })),
        "failed PC is a different source class, not retained live writer failure"
    );
}

#[test]
fn real_feedback_mode_factory_selects_source_direction_without_native_fallback() {
    use rtc_core::config::FeedbackMode;
    let now = Instant::now();
    for (send_side, interceptor, mode) in [
        (false, false, FeedbackMode::ReceiverEstimate),
        (false, true, FeedbackMode::InterceptorEstimate),
        (true, false, FeedbackMode::NativeSendSide),
        (true, true, FeedbackMode::NativeSendSide),
    ] {
        let mut e = engine(now);
        e.cfg.rtc.congestion_control.use_send_side_bwe = send_side;
        e.cfg.rtc.congestion_control.use_send_side_bwe_interceptor = interceptor;
        e.cfg.enabled_codecs.extend([
            CodecSpec {
                mime: "video/VP8".into(),
                fmtp_line: String::new(),
            },
            CodecSpec {
                mime: "video/rtx".into(),
                fmtp_line: String::new(),
            },
        ]);
        for role in [PcRole::Publisher, PcRole::Subscriber, PcRole::Single] {
            let tid = transport(&mut e, role, now);
            let t = e.transports.get_mut(&tid).unwrap();
            assert_eq!(
                t.remote_feedback.is_some(),
                role != PcRole::Publisher && mode != FeedbackMode::NativeSendSide,
                "the application-owned feedback driver exists only in the modes \
                 where it owns the allocation"
            );
            assert_eq!(
                t.rtc.bwe().snapshot().is_some(),
                role != PcRole::Publisher && mode == FeedbackMode::NativeSendSide,
                "native send-side mode keeps str0m's vendor estimator as the \
                 allocation authority; the application-owned modes do not enable it"
            );
            if role != PcRole::Publisher && mode == FeedbackMode::InterceptorEstimate {
                assert_eq!(
                    t.remote_feedback.as_ref().unwrap().interceptor_target(),
                    Some(1_000_000)
                );
                assert_eq!(
                    t.rtc.bwe().configured_pacing_rate(),
                    None,
                    "interceptor owns no normal GCC pacer"
                );
            }
            let mut change = t.rtc.sdp_api();
            change.add_media(
                MediaKind::Video,
                if role == PcRole::Publisher {
                    Direction::RecvOnly
                } else {
                    Direction::SendOnly
                },
                None,
                None,
                None,
            );
            change.add_media(MediaKind::Audio, Direction::SendRecv, None, None, None);
            let (offer, _) = change.apply().unwrap();
            let sdp = offer.to_sdp_string();
            let audio = sdp
                .split("m=audio")
                .nth(1)
                .unwrap()
                .split("m=")
                .next()
                .unwrap();
            let video = sdp
                .split("m=video")
                .nth(1)
                .unwrap()
                .split("m=")
                .next()
                .unwrap();
            if mode != FeedbackMode::NativeSendSide {
                assert!(
                    !audio.contains("transport-wide-cc-extensions"),
                    "source audio has no TWCC for {role:?}"
                );
                assert!(
                    !audio.contains("abs-send-time"),
                    "source audio has no ABS for {role:?}"
                );
                assert!(!audio.contains("transport-cc"));
                assert!(!audio.contains("goog-remb"));
                assert!(
                    video.contains("transport-wide-cc-extensions")
                        || role == PcRole::Subscriber && mode == FeedbackMode::ReceiverEstimate
                );
            } else {
                assert!(
                    audio.contains("transport-wide-cc-extensions"),
                    "explicit native policy remains unchanged"
                );
            }
            if role == PcRole::Subscriber && mode == FeedbackMode::ReceiverEstimate {
                assert!(sdp.contains("goog-remb"));
                assert!(!sdp.contains("transport-cc"));
                assert!(sdp.contains("abs-send-time"));
                assert!(!sdp.contains("transport-wide-cc-extensions"));
            } else {
                assert!(sdp.contains("transport-cc"));
                assert!(sdp.contains("transport-wide-cc-extensions"));
            }
            if role == PcRole::Single && mode != FeedbackMode::NativeSendSide {
                assert!(
                    sdp.contains("goog-remb") && sdp.contains("abs-send-time"),
                    "singlePC keeps publisher union"
                );
            }
        }
    }
}

#[test]
fn disabled_allocator_does_not_gain_a_limit_when_remote_receiver_reports_arrive() {
    let now = Instant::now();
    let mut e = engine(now);
    e.cfg.rtc.congestion_control = CongestionControl {
        enabled: false,
        ..CongestionControl::default()
    };
    let tid = transport(&mut e, PcRole::Single, now);
    let t = e.transports.get_mut(&tid).unwrap();
    assert!(t.remote_feedback.is_some());
    assert!(!t.has_bwe);
    t.estimate_bps = Some(100_000); // diagnostic value is not an enabled allocation limit.
    assert_eq!(t.capacity(), None);
    assert!(t.rtc.bwe().snapshot().is_none());
}

#[test]
fn pion_feedback_owner_generation_is_retired_on_transport_failure() {
    let now = Instant::now();
    let mut engine = engine(now);
    engine.cfg.rtc.congestion_control.use_send_side_bwe = false;
    engine
        .cfg
        .rtc
        .congestion_control
        .use_send_side_bwe_interceptor = true;
    let tid = transport(&mut engine, PcRole::Single, now);
    let sent = str0m::SendSidePacket {
        at: now,
        sequence: 1,
        ssrc: 44,
        header_bytes: 20,
        payload_bytes: 100,
        // The interceptor path sizes an ack from header plus payload; the
        // padding fields only matter to the native send-side owner.
        packet_bytes: 120,
        is_padding: false,
        is_rtx: false,
        twcc_negotiated: true,
        transport_sequence: Some(1),
    };
    engine.on_interceptor_input(tid, str0m::SendSideFeedback::Sent(sent));
    assert_eq!(
        engine.transports[&tid]
            .remote_feedback
            .as_ref()
            .unwrap()
            .interceptor_stats()
            .unwrap()
            .sent_packets,
        1
    );
    engine.fail_transport(tid, "test");
    engine.on_interceptor_input(
        tid,
        str0m::SendSideFeedback::Sent(str0m::SendSidePacket {
            sequence: 2,
            transport_sequence: Some(2),
            ..sent
        }),
    );
    assert_eq!(
        engine.transports[&tid]
            .remote_feedback
            .as_ref()
            .unwrap()
            .interceptor_stats()
            .unwrap()
            .sent_packets,
        1,
        "late old transport observation cannot mutate retired owner"
    );
    let fresh = transport(&mut engine, PcRole::Single, now);
    assert_ne!(fresh, tid);
    assert_eq!(
        engine.transports[&fresh]
            .remote_feedback
            .as_ref()
            .unwrap()
            .interceptor_stats()
            .unwrap()
            .sent_packets,
        0
    );
    assert!(
        engine
            .transports
            .get_mut(&fresh)
            .unwrap()
            .rtc
            .bwe()
            .snapshot()
            .is_none(),
        "fresh Pion owner has no native estimator fallback"
    );
}

#[test]
#[ignore = "requires genuine fixed Go stateful raw-wire/history callback output"]
fn actual_source_owner_dispatches_prefix_before_later_admitted_error() {
    let input = std::fs::read_to_string(std::env::var("PION_PREFIX_INPUT").unwrap()).unwrap();
    let output = std::fs::read_to_string(std::env::var("PION_PREFIX_OUTPUT").unwrap()).unwrap();
    let rows: Vec<serde_json::Value> = output
        .lines()
        .map(|s| serde_json::from_str(s).unwrap())
        .collect();
    let now = Instant::now();
    let mut cursor = 0;
    let mut cases = 0;
    let mut challenged = 0;
    for line in input.lines() {
        let case: serde_json::Value = serde_json::from_str(line).unwrap();
        let mut e = engine(now);
        e.cfg.rtc.congestion_control.use_send_side_bwe = false;
        e.cfg.rtc.congestion_control.use_send_side_bwe_interceptor = true;
        let tid = transport(&mut e, PcRole::Single, now);
        // Existing allocation bootstrap is independent of source callback initialization.
        let mut last_callback = e.transports[&tid].estimate_bps;
        let challenge = case["challenge_index"].as_u64().unwrap() as usize;
        for (index, step) in case["steps"].as_array().unwrap().iter().enumerate() {
            let row = &rows[cursor];
            cursor += 1;
            assert_eq!(row["case"], case["name"]);
            assert_eq!(row["index"].as_u64().unwrap(), index as u64);
            let at = now + Duration::from_nanos(step["at_ns"].as_u64().unwrap());
            let event = if step["kind"] == "sent" {
                let header_bytes = step["header_bytes"].as_u64().unwrap() as usize;
                let payload_bytes = step["payload_bytes"].as_u64().unwrap() as usize;
                str0m::SendSideFeedback::Sent(str0m::SendSidePacket {
                    at,
                    sequence: step["sequence"].as_u64().unwrap() as u16,
                    ssrc: step["ssrc"].as_u64().unwrap() as u32,
                    header_bytes,
                    payload_bytes,
                    packet_bytes: header_bytes + payload_bytes,
                    is_padding: false,
                    is_rtx: false,
                    twcc_negotiated: true,
                    transport_sequence: Some(step["transport_sequence"].as_u64().unwrap() as u16),
                })
            } else {
                let packet = step["wire_hex"]
                    .as_str()
                    .unwrap()
                    .as_bytes()
                    .as_chunks::<2>()
                    .0
                    .iter()
                    .map(|p| u8::from_str_radix(std::str::from_utf8(p).unwrap(), 16).unwrap())
                    .collect();
                str0m::SendSideFeedback::Report { at, packet }
            };
            e.on_interceptor_input(tid, event);
            let changes = row["changes"].as_array().unwrap();
            for value in changes {
                last_callback = Some(value.as_u64().unwrap());
            }
            assert_eq!(
                e.transports[&tid].estimate_bps, last_callback,
                "real owner callback delivery: case {} step {index}",
                case["name"]
            );
            assert_eq!(
                e.transports[&tid]
                    .remote_feedback
                    .as_ref()
                    .unwrap()
                    .interceptor_target(),
                Some(row["target"].as_u64().unwrap())
            );
            if index == challenge && case["name"] == "good-bad-good" {
                assert_eq!(row["process_error"], "invalid feedback");
                assert!(
                    !changes.is_empty(),
                    "actual source prefix must change target"
                );
                challenged += 1;
            }
        }
        cases += 1;
    }
    assert_eq!(cases, 4);
    assert_eq!(cursor, rows.len());
    assert_eq!(challenged, 1);
}

mod data_track_latency;

#[test]
fn open_data_kind_actual_encrypted_channels_preserve_payload_and_nonzero_uses_lossy() {
    use crate::net::Datagram;
    use str0m::net::{Protocol, Receive};
    use str0m::{Candidate, Event, Input, Output};
    let start = Instant::now();
    let mut engine = engine(start);
    let packets = PacketQueue::default();
    engine.out = Box::new(packets.clone());
    let tid = transport(&mut engine, PcRole::Single, start);
    let server_addr = "127.0.0.1:9161".parse().unwrap();
    let client_addr = "127.0.0.1:9162".parse().unwrap();
    let t = engine.transports.get_mut(&tid).unwrap();
    let candidate = t
        .rtc
        .add_local_candidate(Candidate::host(server_addr, "udp").unwrap())
        .unwrap()
        .clone();
    t.candidates.push(candidate);
    let mut client = client(start);
    client.add_local_candidate(Candidate::host(client_addr, "udp").unwrap());
    let mut ids = Vec::new();
    offer(&mut engine, &mut client, tid, start, |change| {
        for (label, ordered, reliability) in [
            ("_reliable", true, str0m::channel::Reliability::Reliable),
            (
                "_lossy",
                false,
                str0m::channel::Reliability::MaxRetransmits { retransmits: 0 },
            ),
        ] {
            ids.push(
                change.add_channel_with_config(str0m::channel::ChannelConfig {
                    label: label.into(),
                    ordered,
                    reliability,
                    ..Default::default()
                }),
            );
        }
    });
    let values = [0_i32, 1, 99, -1, i32::MIN, i32::MAX];
    let mut sent = false;
    let mut received = std::collections::BTreeMap::new();
    for tick in 0..10000_u64 {
        let now = start + Duration::from_millis(tick);
        client.handle_input(Input::Timeout(now)).unwrap();
        loop {
            match client.poll_output().unwrap() {
                Output::Transmit(p) => engine.handle_datagram(Datagram {
                    transport: tid,
                    proto: Proto::Udp,
                    source: client_addr,
                    destination: server_addr,
                    data: &p.contents,
                    received_at: now,
                }),
                Output::Event(Event::ChannelData(d)) => {
                    let raw = i32::from_le_bytes(
                        d.data
                            .as_slice()
                            .try_into()
                            .expect("exact unchanged signed-i32 marker"),
                    );
                    assert!(values.contains(&raw));
                    assert!(received.insert(raw, d.id).is_none(), "duplicate delivery");
                    assert_eq!(
                        d.id,
                        ids[usize::from(raw != 0)],
                        "actual receiver channel, not packet metadata"
                    );
                }
                Output::Timeout(_) => break,
                _ => {}
            }
        }
        engine.handle_timeout(now);
        engine.run(now);
        if !sent
            && engine.transports[&tid]
                .channel_for(DataKind::Reliable)
                .is_some()
            && engine.transports[&tid]
                .channel_for(DataKind::Lossy)
                .is_some()
        {
            for raw in values {
                engine.send_data(
                    &ParticipantSid::from("PA_test"),
                    DataKind::from_i32(raw),
                    &raw.to_le_bytes(),
                );
            }
            engine.run(now);
            sent = true;
        }
        for bytes in packets.0.lock().unwrap().drain(..).collect::<Vec<_>>() {
            client
                .handle_input(Input::Receive(
                    now,
                    Receive::new(Protocol::Udp, server_addr, client_addr, &bytes).unwrap(),
                ))
                .unwrap();
        }
        if received.len() == values.len() {
            assert!(sent);
            return;
        }
    }
    panic!("real authenticated SCTP delivery incomplete: sent={sent}, received={received:?}");
}

#[test]
fn actual_transport_offer_keeps_enabled_red_for_false_client_red_capability() {
    for role in [PcRole::Publisher, PcRole::Subscriber, PcRole::Single] {
        let now = Instant::now();
        let mut e = engine(now);
        e.cfg.enabled_codecs.push(CodecSpec {
            mime: "audio/red".into(),
            fmtp_line: String::new(),
        });
        // Existing transport helper explicitly supplies client_supports_red=false.
        let tid = transport(&mut e, role, now);
        let t = e.transports.get_mut(&tid).unwrap();
        assert!(!t.spec.client_supports_red);
        let mut change = t.rtc.sdp_api();
        change.add_media(
            MediaKind::Audio,
            if role == PcRole::Publisher {
                Direction::RecvOnly
            } else {
                Direction::SendOnly
            },
            None,
            None,
            None,
        );
        let (offer, _) = change.apply().unwrap();
        let sdp = offer.to_sdp_string();
        assert!(
            sdp.lines().any(|line| line.starts_with("a=rtpmap:")
                && line.to_ascii_lowercase().contains(" red/48000/2")),
            "{role:?}: actual configured native SDP lacks enabled RED"
        );
        assert!(sdp.lines().any(|line| line.starts_with("a=rtpmap:")
            && line.to_ascii_lowercase().contains(" opus/48000/2")));
    }
}

/// One forwarded audio frame: a plausible Opus packet (the upstream silence TOC) plus a per-frame
/// marker so the redundancy chain can be attributed to the exact frames it must carry.
fn opus_frame(marker: u8) -> Vec<u8> {
    vec![0xf8, 0xff, 0xfe, marker]
}

/// The marker byte of a payload that must be exactly one forwarded Opus frame.
fn opus_frame_marker(payload: &[u8]) -> u8 {
    assert_eq!(
        payload,
        opus_frame(*payload.last().unwrap_or(&0xFF)).as_slice(),
        "every forwarded block must be one whole marker-carrying Opus frame, got {payload:02x?}"
    );
    payload[3]
}

/// The RFC 2198 chain of a real egress payload as `(block PT, timestamp offset, block marker)`,
/// oldest block first with the primary last.
fn red_chain(payload: &[u8]) -> Vec<(u8, u32, u8)> {
    let blocks = str0m::rtp::RedDecoder::decode(payload)
        .expect("the SFU must emit a decodable RFC 2198 payload on the RED PT");
    blocks
        .iter()
        .map(|b| (b.pt, b.timestamp_offset, opus_frame_marker(b.payload)))
        .collect()
}

/// One depayloaded RED frame: `(payload type, primary marker, RED chain)`, where
/// a chain entry is `(block payload type, timestamp offset, block marker)`.
type RedChain = Vec<(u8, u32, u8)>;
type RedFrame = (u8, u8, RedChain);

struct RedWire {
    /// Payload type of every authenticated audio packet the subscriber received.
    all_pts: Vec<u8>,
    /// The last `RED_WINDOW` frames the subscriber depayloaded.
    window: Vec<RedFrame>,
}

const RED_WINDOW: usize = 8;

/// Real ICE/DTLS/SRTP one-PC audio forwarding with `room.enabled_codecs = [audio/opus, audio/red]`
/// and a subscriber that negotiates RED PT 63, driven only by `audio.active_red_encoding`.
///
/// Go computes `DisableRed = !IsRedEnabled(ti) || !ActiveREDEncoding`
/// (`pkg/rtc/mediatrackreceiver.go:578`); that one flag both adds `audio/red` to the codec set
/// offered to the subscriber (`pkg/rtc/wrappedreceiver.go:70-74`) and selects the encoding
/// `RedReceiver` (`pkg/sfu/receiver_base.go:1056`) which prepends Go's `maxRedCount = 2` previous
/// frames (`pkg/sfu/redreceiver.go:176`). The SDP therefore looks identical either way, and the
/// only observable difference may be the egress payload type and its redundancy chain.
fn single_pc_active_red_encoding(active_red: bool) -> RedWire {
    use crate::net::Datagram;
    use rtc_core::media_api::DownTrackSpec;
    use rtc_core::model::TrackSettings;
    use str0m::media::{Frequency, MediaTime};
    use str0m::net::{Protocol, Receive};
    use str0m::{Candidate, Event, Input, Output};

    let start = Instant::now();
    let mut engine = engine(start);
    engine.cfg.audio.active_red_encoding = active_red;
    engine.cfg.enabled_codecs.push(CodecSpec {
        mime: "audio/red".into(),
        fmtp_line: String::new(),
    });
    let packets = PacketQueue::default();
    engine.out = Box::new(packets.clone());
    let tid = transport(&mut engine, PcRole::Single, start);
    let server_addr: std::net::SocketAddr = "127.0.0.1:9011".parse().unwrap();
    let client_addr: std::net::SocketAddr = "127.0.0.1:9012".parse().unwrap();
    let t = engine.transports.get_mut(&tid).unwrap();
    let candidate = t
        .rtc
        .add_local_candidate(Candidate::host(server_addr, "udp").unwrap())
        .unwrap()
        .clone();
    t.candidates.push(candidate);
    let mut client = Rtc::builder()
        .set_crypto_provider(rtc_crypto::provider().into())
        .set_dtls_cert(rtc_crypto::dtls_cert().unwrap())
        .enable_opus(true, true)
        .enable_raw_packets(true)
        .build(start);
    client.add_local_candidate(Candidate::host(client_addr, "udp").unwrap());
    let tx_mid = published_mid(&offer(&mut engine, &mut client, tid, start, |change| {
        change.add_media(MediaKind::Audio, Direction::SendOnly, None, None, None);
    }));
    let track = TrackSid::from("TR_red");
    engine.bind_track(tid, &tx_mid.to_string(), track.clone(), start);
    engine.subscribe(
        DownTrackSpec {
            subscriber: ParticipantSid::from("PA_test"),
            publisher: ParticipantSid::from("PA_test"),
            track_sid: track.clone(),
            stream_id: "PA_test|TR_red".into(),
            settings: TrackSettings::default(),
            adaptive_stream: false,
        },
        start,
    );
    let mut rx_mid = None;
    let events = offer(&mut engine, &mut client, tid, start, |change| {
        rx_mid = Some(change.add_media(MediaKind::Audio, Direction::RecvOnly, None, None, None));
    });
    let rx_mid = rx_mid.unwrap();
    let answer = events
        .iter()
        .find_map(|e| match e {
            MediaEvent::LocalDescription {
                kind: SdpType::Answer,
                sdp,
                ..
            } => Some(sdp.clone()),
            _ => None,
        })
        .expect("server answers the subscription");
    assert!(
        answer
            .lines()
            .any(|line| line.starts_with("a=rtpmap:63 red")),
        "RED must stay negotiated on the subscription m-line for active_red={active_red}: {answer}"
    );

    let mut all_pts = vec![];
    let mut observed: Vec<RedFrame> = vec![];
    let mut delivered = vec![];
    let mut marker: u8 = 0;
    for tick in 0..3000u64 {
        let now = start + Duration::from_millis(tick * 10);
        client.handle_input(Input::Timeout(now)).unwrap();
        if client.is_connected() && observed.len() < RED_WINDOW + 2 {
            let writer = client.writer(tx_mid).unwrap();
            let pt = writer
                .payload_params()
                .find(|p| p.spec().codec == str0m::format::Codec::Opus)
                .unwrap()
                .pt();
            writer
                .write(
                    pt,
                    now,
                    MediaTime::new(tick * 480, Frequency::FORTY_EIGHT_KHZ),
                    opus_frame(marker),
                )
                .unwrap();
            marker = marker.wrapping_add(1);
        }
        loop {
            match client.poll_output().unwrap() {
                Output::Transmit(p) => {
                    engine.handle_datagram(Datagram {
                        transport: tid,
                        proto: Proto::Udp,
                        source: client_addr,
                        destination: server_addr,
                        data: &p.contents,
                        received_at: now,
                    });
                }
                Output::Event(Event::MediaData(d)) if d.mid == rx_mid => {
                    let primary: &[u8] = d.data.as_ref();
                    delivered.push(opus_frame_marker(primary));
                }
                Output::Event(Event::RawPacket(packet)) => {
                    if let str0m::rtp::RawPacket::RtpRx(header, payload) = &*packet {
                        let pt = *header.payload_type;
                        if pt == 63 || pt == 111 {
                            all_pts.push(pt);
                            let chain = if pt == 63 {
                                red_chain(payload)
                            } else {
                                vec![(111, 0, opus_frame_marker(payload))]
                            };
                            let primary = chain.last().expect("a RED payload has a primary block");
                            assert_eq!(
                                (primary.0, primary.1),
                                (111, 0),
                                "the primary block is last, unoffset and Opus: {chain:?}"
                            );
                            observed.push((pt, primary.2, chain));
                        }
                    }
                }
                Output::Timeout(_) => break,
                _ => {}
            }
        }
        engine.handle_timeout(now);
        let mut engine_events = vec![];
        engine.drain_events(&mut engine_events);
        for bytes in packets.0.lock().unwrap().drain(..).collect::<Vec<_>>() {
            client
                .handle_input(Input::Receive(
                    now,
                    Receive::new(Protocol::Udp, server_addr, client_addr, &bytes).unwrap(),
                ))
                .unwrap();
        }
        if observed.len() >= RED_WINDOW + 2 {
            break;
        }
    }
    assert!(
        observed.len() >= RED_WINDOW,
        "audio forwarding did not converge: pts={all_pts:?}, delivered={delivered:?}"
    );
    let window: Vec<_> = observed.drain(observed.len() - RED_WINDOW..).collect();
    for pair in window.windows(2) {
        assert_eq!(
            pair[1].1,
            pair[0].1.wrapping_add(1),
            "the window must be consecutive forwarded frames: {:?}",
            window.iter().map(|w| w.1).collect::<Vec<_>>()
        );
    }
    let markers: Vec<u8> = window.iter().map(|w| w.1).collect();
    assert!(
        delivered.len() >= RED_WINDOW,
        "the subscriber must depayload {RED_WINDOW} Opus frames, got {delivered:?}"
    );
    assert_eq!(
        &delivered[delivered.len() - RED_WINDOW..],
        markers.as_slice(),
        "the subscriber's RED decode yields the primary frames in order"
    );
    RedWire { all_pts, window }
}

#[test]
fn active_red_encoding_forwards_two_redundant_opus_levels_on_the_red_payload_type() {
    let wire = single_pc_active_red_encoding(true);
    assert!(
        wire.all_pts.iter().all(|pt| *pt == 63),
        "active encoding must wrap every forwarded audio packet in RED: {:?}",
        wire.all_pts
    );
    for (pt, marker, chain) in &wire.window {
        assert_eq!(
            (pt, chain),
            (
                &63,
                &vec![
                    (111, 960, marker.wrapping_sub(2)),
                    (111, 480, marker.wrapping_sub(1)),
                    (111, 0, *marker),
                ],
            ),
            "Go's maxRedCount=2 window must carry the two previous frames, oldest first at \
             20 ms and 10 ms of timestamp offset, for frame {marker}"
        );
    }
}

#[test]
fn inactive_red_encoding_forwards_plain_opus_and_never_wraps() {
    let wire = single_pc_active_red_encoding(false);
    assert!(
        wire.all_pts.iter().all(|pt| *pt == 111),
        "the disabled gate must forward the original Opus payload type: {:?}",
        wire.all_pts
    );
    for (pt, marker, chain) in &wire.window {
        assert_eq!(
            (pt, chain),
            (&111, &vec![(111, 0, *marker)]),
            "no redundancy may be added while active_red_encoding is off, frame {marker}"
        );
    }
}

#[test]
#[ignore = "requires original fixed Go callback sequence via JOINING_CALLBACK_GO_RESULTS"]
fn actual_go_joining_connection_callback_sequence_all_eighteen_stages() {
    use serde_json::Value;
    let rows: Vec<Value> = std::fs::read_to_string(
        std::env::var("JOINING_CALLBACK_GO_RESULTS").expect("actual original source output"),
    )
    .unwrap()
    .lines()
    .map(|l| serde_json::from_str(l).unwrap())
    .collect();
    let sequences = [
        (
            "initial-ready",
            vec!["constructor", "first-connected", "repeat-connected"],
        ),
        (
            "reset-late-before-connected",
            vec![
                "constructor",
                "first-connected",
                "reset",
                "late-channel-open",
                "again-connected",
                "next-channel-open",
            ],
        ),
        (
            "normal-late-before-connected",
            vec![
                "constructor",
                "first-connected",
                "late-channel-open",
                "again-connected",
            ],
        ),
        (
            "reset-connected-before-late",
            vec![
                "constructor",
                "first-connected",
                "reset",
                "again-connected",
                "late-channel-open",
            ],
        ),
    ];
    assert_eq!(
        rows.len(),
        18,
        "all ordered original observer rows required"
    );
    let mut index = 0;
    for (id, stages) in sequences {
        let now = Instant::now();
        let mut e = engine(now);
        let tid = transport(&mut e, PcRole::Subscriber, now);
        let mut initial_callbacks = 0usize;
        let mut fully_callbacks = 0usize;
        for stage in stages {
            assert_eq!(rows[index]["id"], id);
            assert_eq!(rows[index]["stage"], stage);
            match stage {
                "constructor" => {}
                "first-connected" => {
                    let t = e.transports.get_mut(&tid).unwrap();
                    t.opened_data_kinds = [true, id == "initial-ready"];
                    if t.record_data_connection_proof() {
                        initial_callbacks += 1;
                        e.maybe_report_fully_established(tid);
                    }
                }
                "reset" => {
                    let t = e.transports.get_mut(&tid).unwrap();
                    t.reset_established_on_ice_restart = true;
                    assert!(t.consume_short_connection_reset());
                }
                "late-channel-open" => {
                    e.transports.get_mut(&tid).unwrap().opened_data_kinds[1] = true;
                    e.maybe_report_fully_established(tid);
                }
                "next-channel-open" => e.maybe_report_fully_established(tid),
                "again-connected" | "repeat-connected" => {
                    // This is the production connection proof helper. Native callers
                    // supply real DTLS/ICE readiness; the source observer also controls
                    // the original PC callback directly, without a wire claim.
                    if e.transports
                        .get_mut(&tid)
                        .unwrap()
                        .record_data_connection_proof()
                    {
                        initial_callbacks += 1;
                        e.maybe_report_fully_established(tid);
                    }
                }
                _ => unreachable!(),
            }
            let mut events = vec![];
            e.drain_events(&mut events);
            fully_callbacks += events
                .iter()
                .filter(|event| matches!(event, MediaEvent::PrimaryTransportEstablished { .. }))
                .count();
            let t = &e.transports[&tid];
            let row = &rows[index];
            assert_eq!(row["first"].as_bool(), Some(t.data_connection_seen));
            assert_eq!(row["last"].as_bool(), Some(t.fully_established_connected));
            assert_eq!(
                row["reliable_opened"].as_bool(),
                Some(t.opened_data_kinds[0])
            );
            assert_eq!(row["lossy_opened"].as_bool(), Some(t.opened_data_kinds[1]));
            assert_eq!(
                row["initial_callbacks"].as_u64(),
                Some(initial_callbacks as u64)
            );
            assert_eq!(
                row["fully_callbacks"].as_u64(),
                Some(fully_callbacks as u64)
            );
            assert_eq!(
                row["eligible"].as_bool(),
                Some(t.fully_established_connected && t.opened_data_kinds == [true, true])
            );
            index += 1;
        }
    }
    assert_eq!(index, 18);
}

// Old-public command/control regression: real native SDP offer, no answer and no
// injected connection callback. This proves resend/pending recovery, not SRTP.
#[test]
fn local_restart_before_first_answer_resends_same_offer_then_restarts_credentials() {
    use rtc_core::media_api::MediaCommand;
    let now = Instant::now();
    let mut e = engine(now);
    let tid = transport(&mut e, PcRole::Subscriber, now);
    e.transports
        .get_mut(&tid)
        .unwrap()
        .spec
        .server_data_channels = true;
    e.negotiate(tid, true, now);
    let mut events = Vec::new();
    e.drain_events(&mut events);
    let (original, id) = events
        .iter()
        .find_map(|event| match event {
            MediaEvent::LocalDescription {
                kind: SdpType::Offer,
                sdp,
                id,
                ..
            } => Some((sdp.clone(), *id)),
            _ => None,
        })
        .expect("real first local offer");
    assert!(!e.transports[&tid].remote_description_applied);
    e.handle_command(
        MediaCommand::ArmShortConnectionReset {
            participant: ParticipantSid::from("PA_test"),
        },
        now,
    );
    e.handle_command(MediaCommand::IceRestart { transport: tid }, now);
    events.clear();
    e.drain_events(&mut events);
    let (resent, next_id) = events
        .iter()
        .find_map(|event| match event {
            MediaEvent::LocalDescription {
                kind: SdpType::Offer,
                sdp,
                id,
                ..
            } => Some((sdp.clone(), *id)),
            _ => None,
        })
        .expect("restart without an answer resends the pending local offer");
    assert_eq!(resent, original);
    assert_eq!(next_id, id + 1);
    e.negotiate(tid, true, now);
    events.clear();
    e.drain_events(&mut events);
    assert!(
        !events
            .iter()
            .any(|event| matches!(event, MediaEvent::LocalDescription { .. })),
        "ordinary negotiation must not repeat an already handled explicit restart"
    );
    e.handle_command(MediaCommand::IceRestart { transport: tid }, now);
    events.clear();
    e.drain_events(&mut events);
    let (repeated, repeated_id) = events
        .iter()
        .find_map(|event| match event {
            MediaEvent::LocalDescription {
                kind: SdpType::Offer,
                sdp,
                id,
                ..
            } => Some((sdp.clone(), *id)),
            _ => None,
        })
        .expect("a second explicit restart really resends");
    assert_eq!(repeated, resent);
    assert_eq!(repeated_id, next_id + 1);

    assert!(!e.transports[&tid].reset_established_on_ice_restart);
    let mut peer = client(now);
    let answer = peer
        .sdp_api()
        .accept_offer(str0m::change::SdpOffer::from_sdp_string(&resent).unwrap())
        .unwrap();
    e.handle_command(
        MediaCommand::RemoteDescription {
            transport: tid,
            kind: SdpType::Answer,
            sdp: answer.to_sdp_string(),
            id: repeated_id,
        },
        now,
    );
    events.clear();
    e.drain_events(&mut events);
    let restart = events
        .iter()
        .find_map(|event| match event {
            MediaEvent::LocalDescription {
                kind: SdpType::Offer,
                sdp,
                ..
            } => Some(sdp),
            _ => None,
        })
        .expect("real pending answer causes deferred ICE restart offer");
    let ufrag = |s: &str| {
        s.lines()
            .find_map(|line| line.strip_prefix("a=ice-ufrag:"))
            .unwrap()
            .to_owned()
    };
    assert_ne!(ufrag(&original), ufrag(restart));
}

#[test]
fn local_restart_before_any_local_offer_keeps_reset_armed_without_offer() {
    use rtc_core::media_api::MediaCommand;
    let now = Instant::now();
    let mut e = engine(now);
    let tid = transport(&mut e, PcRole::Subscriber, now);
    let mut discarded = Vec::new();
    e.drain_events(&mut discarded);
    e.handle_command(
        MediaCommand::ArmShortConnectionReset {
            participant: ParticipantSid::from("PA_test"),
        },
        now,
    );
    e.handle_command(MediaCommand::IceRestart { transport: tid }, now);
    let mut events = Vec::new();
    e.drain_events(&mut events);
    assert!(
        !events
            .iter()
            .any(|event| matches!(event, MediaEvent::LocalDescription { .. }))
    );
    assert!(e.transports[&tid].reset_established_on_ice_restart);
    assert!(!e.transports[&tid].ice_restart_pending);
}

#[test]
#[ignore = "actual original Go fanout3 source observations required; admission scheduling component, not wire proof"]
fn actual_go_fanout_worker_count_and_synchronous_completion() {
    use rtc_core::media_api::{DataWrite, MediaCommand};
    use serde_json::Value;
    let text = std::fs::read_to_string(std::env::var("ORDINARY_FANOUT_RESULTS").unwrap()).unwrap();
    let rows: Vec<Value> = text
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect();
    assert_eq!(rows.len(), 3);
    for row in rows {
        let n = row["participants"].as_u64().unwrap() as usize;
        assert_eq!(
            row["runtime_cpus"].as_u64().unwrap() as usize,
            crate::data_fanout_cpu::count()
        );
        assert_eq!(row["returned_while_blocked"], false);
        let calls = row["calls"].as_object().unwrap();
        assert_eq!(calls.len(), n);
        let mut writes = Vec::new();
        for (sid, observed) in calls {
            assert_eq!(observed["kind"], 0);
            assert_eq!(observed["sender"], "");
            assert_eq!(observed["sequence"], 0);
            let hex = observed["wire_hex"].as_str().unwrap();
            assert_eq!(hex.len() % 2, 0);
            let raw = (0..hex.len())
                .step_by(2)
                .map(|at| u8::from_str_radix(&hex[at..at + 2], 16).unwrap())
                .collect::<Vec<_>>();
            writes.push(DataWrite {
                to: ParticipantSid::from(sid.as_str()),
                kind: DataKind::Reliable,
                payload: raw,
                expected_transport: None,
            });
        }
        let now = Instant::now();
        let mut e = engine(now);
        e.handle_command(MediaCommand::SendDataFanout { id: 42, writes }, now);
        let mut events = Vec::new();
        e.drain_events(&mut events);
        assert!(
            !events
                .iter()
                .any(|event| matches!(event, MediaEvent::DataFanoutCompleted { .. }))
        );
        let mut pending = Vec::new();
        let mut seen = std::collections::BTreeSet::new();
        let mut collect = |events: &[MediaEvent], pending: &mut Vec<u64>| {
            for event in events {
                if let MediaEvent::DataWriteAdmission {
                    request,
                    to,
                    kind,
                    payload,
                    ..
                } = event
                {
                    let observed = &calls[to.as_str()];
                    assert_eq!(*kind, DataKind::Reliable);
                    let hex = payload
                        .iter()
                        .map(|v| format!("{v:02x}"))
                        .collect::<String>();
                    assert_eq!(observed["wire_hex"], hex);
                    assert!(seen.insert(to.to_string()));
                    pending.push(*request);
                }
            }
        };
        collect(&events, &mut pending);
        assert_eq!(
            pending.len(),
            row["maximum_active"].as_u64().unwrap() as usize
        );
        let mut completed = 0;
        while let Some(request) = pending.pop() {
            // Explicit admission-component outcome. No fake native write is
            // claimed: public encrypted runtime tests exercise that separately.
            e.handle_command(
                MediaCommand::DataWriteAdmitted {
                    request,
                    write: false,
                },
                now,
            );
            events.clear();
            e.drain_events(&mut events);
            collect(&events, &mut pending);
            for event in &events {
                if let MediaEvent::DataFanoutCompleted { id } = event {
                    assert_eq!(*id, 42);
                    completed += 1;
                    assert!(pending.is_empty())
                }
            }
        }
        drop(collect);
        assert_eq!(seen.len(), n);
        assert_eq!(completed, 1);
    }
}

/// Real attach_down -> native codec feedback -> SDP production, with the
/// fixed Go configureSenderAudio sticky session-wide behavior.
#[test]
fn red_sender_disables_opus_nack_stickily_for_single_and_dual_pc_only() {
    use rtc_core::media_api::DownTrackSpec;
    use rtc_core::model::TrackSettings;
    for role in [PcRole::Single, PcRole::Subscriber] {
        for supports_red in [false, true] {
            let now = Instant::now();
            let mut engine = engine(now);
            engine.cfg.audio.active_red_encoding = true;
            engine.cfg.enabled_codecs.push(CodecSpec {
                mime: "audio/red".into(),
                fmtp_line: String::new(),
            });
            engine.cfg.enabled_codecs.push(CodecSpec {
                mime: "video/VP8".into(),
                fmtp_line: String::new(),
            });
            let pub_tid = transport(&mut engine, PcRole::Publisher, now);
            let mut publisher = client(now);
            let mid = published_mid(&offer(&mut engine, &mut publisher, pub_tid, now, |api| {
                api.add_media(MediaKind::Audio, Direction::SendOnly, None, None, None);
            }));
            let track = TrackSid::from("TR_audio_nack");
            engine.bind_track(pub_tid, &mid.to_string(), track.clone(), now);
            let sub_tid = transport(&mut engine, role, now);
            engine
                .transports
                .get_mut(&sub_tid)
                .unwrap()
                .spec
                .client_supports_red = supports_red;
            let mut subscriber_config = Rtc::builder()
                .set_crypto_provider(rtc_crypto::provider().into())
                .set_dtls_cert(rtc_crypto::dtls_cert().unwrap())
                .enable_opus(true, true);
            for payload in subscriber_config.codec_config().iter_mut() {
                if payload.spec().codec == str0m::format::Codec::Opus {
                    payload.set_fb_nack(true);
                }
            }
            let mut subscriber = subscriber_config.build(now);
            let independent = transport(&mut engine, PcRole::Subscriber, now);
            engine
                .transports
                .get_mut(&independent)
                .unwrap()
                .spec
                .participant = ParticipantSid::from("PA_isolated");
            // The participant index chooses the newest sending transport: make
            // its ownership match the isolated spec before subscribing PA_test.
            engine
                .participants
                .get_mut(&ParticipantSid::from("PA_test"))
                .unwrap()
                .retain(|id| *id != independent);
            engine
                .participants
                .insert(ParticipantSid::from("PA_isolated"), vec![independent]);
            let nack = |engine: &MediaEngine, tid| {
                engine.transports[&tid]
                    .rtc
                    .codec_config()
                    .iter()
                    .find(|p| p.spec().codec == str0m::format::Codec::Opus)
                    .unwrap()
                    .fb_nack()
            };
            assert!(nack(&engine, sub_tid));
            engine.subscribe(
                DownTrackSpec {
                    subscriber: ParticipantSid::from("PA_test"),
                    publisher: ParticipantSid::from("PA_test"),
                    track_sid: track.clone(),
                    stream_id: "PA_test|TR_audio_nack".into(),
                    settings: TrackSettings::default(),
                    adaptive_stream: false,
                },
                now,
            );
            assert_eq!(nack(&engine, sub_tid), !supports_red);
            assert!(
                nack(&engine, independent),
                "NACK policy must not leak across peer connections"
            );
            assert!(
                engine.transports[&sub_tid]
                    .rtc
                    .codec_config()
                    .iter()
                    .any(|p| p.spec().codec.is_video() && p.fb_nack()),
                "video feedback is independent"
            );
            if role == PcRole::Subscriber {
                engine.handle_timeout(now + Duration::from_millis(160));
            }
            let descriptions = if role == PcRole::Single {
                offer(&mut engine, &mut subscriber, sub_tid, now, |api| {
                    api.add_media(MediaKind::Audio, Direction::RecvOnly, None, None, None);
                })
            } else {
                let mut events = vec![];
                engine.drain_events(&mut events);
                events
            };
            let sdp = descriptions
                .iter()
                .find_map(|event| match event {
                    MediaEvent::LocalDescription { transport, sdp, .. }
                        if *transport == sub_tid =>
                    {
                        Some(sdp)
                    }
                    _ => None,
                })
                .expect("actual sender description");
            assert_eq!(
                sdp.lines().any(|line| line.trim() == "a=rtcp-fb:111 nack"),
                !supports_red
            );
            // Detach and reattach to the same transport with a plain-Opus-only
            // source. A subsequent sender must never restore previously removed
            // feedback, including across a new/retired m-line negotiation.
            let di = engine.down_by_key[&(ParticipantSid::from("PA_test"), track.clone())];
            engine.detach_down(di, now);
            let ui = engine.up_by_sid[&track];
            engine.ups.get_mut(ui).unwrap().spec.disable_red = true;
            engine.attach_down(di, now);
            assert_eq!(nack(&engine, sub_tid), !supports_red);
            assert!(nack(&engine, independent));
        }
    }
}

/// Real ICE/DTLS/SRTP three-RID publication. Unsupported UpdateTrackSettings
/// quality must pause forwarding instead of silently selecting LOW; resuming
/// known HIGH must still wait for that RID's real VP8 keyframe.
#[test]
fn open_quality_settings_reach_three_layer_srtp_forwarding_and_keyframe_resume() {
    three_layer_srtp_quality_and_padding(false, false, false);
}

#[test]
fn node_padding_telemetry_matches_real_remote_feedback_rtx_srtp_output() {
    three_layer_srtp_quality_and_padding(true, false, false);
}

#[test]
fn reference_layer_dynacast_waits_for_real_sender_report_over_three_layer_srtp() {
    three_layer_srtp_quality_and_padding(false, true, false);
}

#[test]
fn authenticated_source_restart_clears_reference_reports_before_new_layer_forwarding() {
    three_layer_srtp_quality_and_padding(false, true, true);
}

fn three_layer_srtp_quality_and_padding(
    independent_padding: bool,
    reference_sr: bool,
    source_restart: bool,
) {
    use crate::net::Datagram;
    use rtc_core::media_api::{DownTrackSpec, MediaCommand, PublishedTrackSpec};
    use rtc_core::model::{TrackKind, TrackSettings, TrackSource, VideoLayer, VideoQuality};
    use str0m::media::{Frequency, MediaTime, Simulcast, SimulcastLayer};
    use str0m::net::{Protocol, Receive};
    use str0m::{Candidate, Event, Input, Output};
    let start = Instant::now();
    let mut engine = engine(start);
    engine.cfg.rtc.congestion_control.enabled = independent_padding;
    engine.cfg.rtc.enable_rtp_stream_restart_detection = source_restart;
    if reference_sr {
        engine.cfg.video.dynacast_pause_delay = Duration::ZERO;
    }
    if independent_padding {
        // Real remote feedback / Pion interceptor mode owns explicit padding;
        // native GCC is intentionally a different producer, not switched off at runtime.
        engine.cfg.rtc.congestion_control.use_send_side_bwe = false;
        engine
            .cfg
            .rtc
            .congestion_control
            .use_send_side_bwe_interceptor = true;
    }
    if independent_padding {
        // Fixed mediaengine only registers VP8 repair when video/rtx is enabled.
        engine.cfg.enabled_codecs.push(CodecSpec {
            mime: "video/rtx".into(),
            fmtp_line: String::new(),
        });
    }
    engine.cfg.enabled_codecs.push(CodecSpec {
        mime: "video/vp8".into(),
        fmtp_line: String::new(),
    });
    let packets = RoutedPacketQueue::default();
    engine.out = Box::new(packets.clone());
    let server_addr = "127.0.0.1:9501".parse().unwrap();
    let pub_addr = "127.0.0.1:9502".parse().unwrap();
    let sub_addr = "127.0.0.1:9503".parse().unwrap();
    let pub_tid = transport(&mut engine, PcRole::Publisher, start);
    let t = engine.transports.get_mut(&pub_tid).unwrap();
    let candidate = t
        .rtc
        .add_local_candidate(Candidate::host(server_addr, "udp").unwrap())
        .unwrap()
        .clone();
    t.candidates.push(candidate);
    t.spec.primary = false;
    let sid = TrackSid::from("TR_open_quality_wire");
    let layers = [
        (VideoQuality::Low, 180),
        (VideoQuality::Medium, 360),
        (VideoQuality::High, 720),
    ]
    .map(|(quality, height)| VideoLayer {
        quality,
        width: height * 16 / 9,
        height,
        bitrate: 10_000,
        ssrc: 0,
        spatial_layer: 0,
        rid: String::new(),
        repair_ssrc: 0,
    })
    .to_vec();
    engine.handle_command(
        MediaCommand::ExpectTrack {
            transport: pub_tid,
            track: PublishedTrackSpec {
                backup_codec_policy: Default::default(),
                sid: sid.clone(),
                kind: TrackKind::Video,
                source: TrackSource::Camera,
                cid: "open-quality".into(),
                layers,
                codecs: vec![],
                stereo: false,
                disable_dtx: false,
                disable_red: false,
                encrypted: false,
                muted: false,
            },
        },
        start,
    );
    let mut publisher = Rtc::builder()
        .set_crypto_provider(rtc_crypto::provider().into())
        .set_dtls_cert(rtc_crypto::dtls_cert().unwrap())
        .enable_raw_packets(true)
        .build(start);
    publisher.add_local_candidate(Candidate::host(pub_addr, "udp").unwrap());
    let publish_events = offer(&mut engine, &mut publisher, pub_tid, start, |change| {
        let mut simulcast = Simulcast::new();
        for rid in ["q", "h", "f"] {
            simulcast.add_send_layer(SimulcastLayer::new(rid));
        }
        change.add_media(
            MediaKind::Video,
            Direction::SendOnly,
            None,
            Some("open-quality".into()),
            Some(simulcast),
        );
    });
    let tx_mid = published_mid(&publish_events);
    let mut reference_events = if reference_sr {
        publish_events
    } else {
        Vec::new()
    };
    engine.bind_track(pub_tid, &tx_mid.to_string(), sid.clone(), start);
    let sub_tid = TransportId::next();
    engine.create_transport(
        TransportSpec {
            ice_preference: Default::default(),
            one_shot: false,
            id: sub_tid,
            participant: "PA_test".into(),
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
    let mut subscriber = Rtc::builder()
        .set_crypto_provider(rtc_crypto::provider().into())
        .set_dtls_cert(rtc_crypto::dtls_cert().unwrap())
        .enable_raw_packets(true)
        .build(start);
    subscriber.add_local_candidate(Candidate::host(sub_addr, "udp").unwrap());
    if reference_sr {
        reference_events.extend(answer_server_offer_events(
            &mut engine,
            &mut subscriber,
            sub_tid,
            start,
        ));
    } else {
        answer_server_offer(&mut engine, &mut subscriber, sub_tid, start);
    }
    engine.handle_command(
        MediaCommand::Subscribe(DownTrackSpec {
            subscriber: "PA_test".into(),
            publisher: "PA_test".into(),
            track_sid: sid.clone(),
            stream_id: "PA_test|TR_open_quality_wire".into(),
            settings: TrackSettings::default(),
            adaptive_stream: false,
        }),
        start,
    );
    let negotiated = start + Duration::from_millis(160);
    engine.handle_timeout(negotiated);
    if reference_sr {
        reference_events.extend(answer_server_offer_events(
            &mut engine,
            &mut subscriber,
            sub_tid,
            negotiated,
        ));
    } else {
        answer_server_offer(&mut engine, &mut subscriber, sub_tid, negotiated);
    }
    let di = engine.down_by_key[&("PA_test".into(), sid.clone())];
    let mut quality_events = Vec::new();
    let mut dropped_srtcp = 0usize;
    let mut delivered_srtcp = 0usize;
    let mut first_reference_report = None;
    let mut counts = [0usize; 4];
    let mut received_before_high_keyframe = 0usize;
    let mut wire_padding = (0u64, 0u64);
    let mut lost_up_ssrc = None;
    let mut lost_down_ssrc = None;
    let mut lost_up = 0;
    let mut lost_down = 0;
    let mut peer_nack_sent = 0u64;
    let mut peer_nack_received = 0u64;
    let mut multi_sn_nack = false;
    for tick in 0..6500u64 {
        let now = negotiated + Duration::from_millis(tick);
        publisher.handle_input(Input::Timeout(now)).unwrap();
        subscriber.handle_input(Input::Timeout(now)).unwrap();
        if let Some(quality) = match tick {
            2000 => Some(VideoQuality::from_i32(77)),
            3000 => Some(VideoQuality::Low),
            4000 if !reference_sr => Some(VideoQuality::High),
            4700 if reference_sr => Some(VideoQuality::from_i32(77)),
            _ => None,
        } {
            engine.handle_command(
                MediaCommand::UpdateTrackSettings {
                    subscriber: "PA_test".into(),
                    track_sid: sid.clone(),
                    settings: TrackSettings {
                        quality,
                        ..Default::default()
                    },
                },
                now,
            );
        }
        if independent_padding && tick == 1700 {
            let mid = engine.downs.get(di).unwrap().mid.unwrap();
            assert!(
                engine
                    .transports
                    .get_mut(&sub_tid)
                    .unwrap()
                    .rtc
                    .writer(mid)
                    .unwrap()
                    .payload_params()
                    .any(|payload| payload.spec().codec == str0m::format::Codec::Vp8
                        && payload.resend().is_some()),
                "real accepted subscriber SDP must contain VP8 RTX before requesting padding"
            );
            engine
                .transports
                .get_mut(&sub_tid)
                .unwrap()
                .rtc
                .direct_api()
                .request_padding_burst(2048, Duration::from_millis(100), now)
                .expect("actual negotiated RTX padding source");
        }
        if publisher.is_connected() && tick.is_multiple_of(30) {
            for (index, rid) in ["q", "h", "f"].iter().enumerate() {
                // Establish HIGH from real media before introducing lower encodings.
                if reference_sr && tick < 900 && index != 2 {
                    continue;
                }
                let writer = publisher.writer(tx_mid).unwrap().rid((*rid).into());
                let pt = writer
                    .payload_params()
                    .find(|p| p.spec().codec == str0m::format::Codec::Vp8)
                    .unwrap()
                    .pt();
                let mut frame = vec![0u8; 64];
                frame[..10].copy_from_slice(&[0x10, 0, 0, 0x9d, 1, 0x2a, 0x80, 2, 0x68, 1]);
                if index == 2 && (4000..4350).contains(&tick) {
                    frame[0] |= 1;
                }
                frame[63] = index as u8;
                writer
                    .write(
                        pt,
                        now,
                        MediaTime::new(tick * 90, Frequency::NINETY_KHZ),
                        frame,
                    )
                    .unwrap();
            }
        }
        for (peer, tid, addr, receiving) in [
            (&mut publisher, pub_tid, pub_addr, false),
            (&mut subscriber, sub_tid, sub_addr, true),
        ] {
            loop {
                match peer.poll_output().unwrap() {
                    Output::Transmit(p) => {
                        // SRTCP leaves its version/type header unencrypted.
                        if reference_sr
                            && !receiving
                            && p.contents.len() >= 2
                            && p.contents[0] >> 6 == 2
                            && (192..=223).contains(&p.contents[1])
                        {
                            if tick < 3500 {
                                dropped_srtcp += 1;
                                continue;
                            }
                            delivered_srtcp += 1;
                        }
                        if independent_padding
                            && !receiving
                            && tick >= 750
                            && lost_up < 3
                            && p.contents.len() >= 12
                            && p.contents[0] >> 6 == 2
                            && p.contents[1] & 0x7f == 96
                        {
                            let ssrc = u32::from_be_bytes(p.contents[8..12].try_into().unwrap());
                            if lost_up_ssrc.is_none() {
                                lost_up_ssrc = Some(ssrc);
                            }
                            if lost_up_ssrc == Some(ssrc) {
                                lost_up += 1;
                                continue;
                            }
                        }
                        engine.handle_datagram(Datagram {
                            transport: tid,
                            proto: Proto::Udp,
                            source: addr,
                            destination: server_addr,
                            data: &p.contents,
                            received_at: now,
                        });
                    }
                    Output::Event(Event::RawPacket(packet)) => {
                        use str0m::rtp::{RawPacket, rtcp::Rtcp};
                        match *packet {
                            RawPacket::RtcpTx(Rtcp::Nack(ref report)) if receiving => {
                                let count = report
                                    .reports
                                    .iter()
                                    .map(|e| 1 + e.blp.count_ones() as u64)
                                    .sum::<u64>();
                                peer_nack_sent += count;
                                multi_sn_nack |= count > 1;
                            }
                            RawPacket::RtcpRx(Rtcp::Nack(ref report)) if !receiving => {
                                let count = report
                                    .reports
                                    .iter()
                                    .map(|e| 1 + e.blp.count_ones() as u64)
                                    .sum::<u64>();
                                peer_nack_received += count;
                                multi_sn_nack |= count > 1;
                            }
                            _ => {}
                        }
                    }
                    Output::Event(Event::MediaData(data)) if receiving => {
                        let layer = *data.data.last().unwrap();
                        if reference_sr {
                            match tick {
                                1000..1900 => {
                                    assert_eq!(layer, 2);
                                    counts[0] += 1;
                                }
                                2250..3000 => counts[1] += 1,
                                3300..4700 => {
                                    assert_eq!(layer, 0);
                                    counts[2] += 1;
                                }
                                5000..6500 => counts[3] += 1,
                                _ => {}
                            }
                            continue;
                        }
                        if (1000..1900).contains(&tick) {
                            assert_eq!(layer, 2);
                            counts[0] += 1;
                        }
                        if (2250..2950).contains(&tick) {
                            counts[1] += 1;
                        }
                        if (3300..3900).contains(&tick) {
                            assert_eq!(layer, 0);
                            counts[2] += 1;
                        }
                        if (4200..4320).contains(&tick) && layer == 2 {
                            received_before_high_keyframe += 1;
                        }
                        if tick > 4600 {
                            assert_eq!(layer, 2);
                            counts[3] += 1;
                        }
                    }
                    Output::Timeout(_) => break,
                    _ => {}
                }
            }
        }
        engine.handle_timeout(now);
        if reference_sr {
            engine.drain_events(&mut reference_events);
            for event in reference_events.drain(..) {
                if let MediaEvent::SubscribedQuality {
                    publisher,
                    track_sid,
                    codecs,
                } = event
                {
                    assert_eq!(publisher, ParticipantSid::from("PA_test"));
                    assert_eq!(track_sid, sid);
                    println!("reference_sr tick={tick} SubscribedQuality={codecs:?}");
                    quality_events.push((tick, codecs));
                }
            }
        } else {
            engine.drain_events(&mut Vec::new());
        }
        for (destination, bytes) in packets.0.lock().unwrap().drain(..) {
            if independent_padding
                && destination == sub_addr
                && tick >= 1250
                && lost_down < 3
                && bytes.len() >= 12
                && bytes[0] >> 6 == 2
                && bytes[1] & 0x7f == 96
            {
                let ssrc = u32::from_be_bytes(bytes[8..12].try_into().unwrap());
                if lost_down_ssrc.is_none() {
                    lost_down_ssrc = Some(ssrc);
                }
                if lost_down_ssrc == Some(ssrc) {
                    lost_down += 1;
                    continue;
                }
            }
            if destination == sub_addr
                && bytes.len() >= 12
                && bytes[0] >> 6 == 2
                && bytes[0] & 0x20 != 0
                && !(192..=223).contains(&bytes[1])
            {
                let auth_tag = subscriber.receive_srtp_auth_tag_len().unwrap();
                wire_padding.0 += (bytes.len() - auth_tag) as u64;
                wire_padding.1 += 1;
            }
            let peer = if destination == pub_addr {
                &mut publisher
            } else {
                assert_eq!(destination, sub_addr);
                &mut subscriber
            };
            peer.handle_input(Input::Receive(
                now,
                Receive::new(Protocol::Udp, server_addr, destination, &bytes).unwrap(),
            ))
            .unwrap();
        }
        if tick == 2250 {
            assert_eq!(engine.downs.get(di).unwrap().max_spatial, -1);
            assert_eq!(engine.downs.get(di).unwrap().settings.quality.as_i32(), 77);
        }
        if reference_sr {
            let down = engine.downs.get(di).unwrap();
            let (reference, reports) = down.reference_state();
            if tick < 3500 {
                assert_eq!(
                    reports, [false; 3],
                    "tick={tick}: elapsed time and real RTP must not fabricate sender reports"
                );
            }
            if tick >= 1800 {
                assert_eq!(reference, Some(2), "reference survives OFF/LOW at {tick}");
            }
            if reports[2] && first_reference_report.is_none() {
                println!(
                    "reference_sr first real reference SR tick={tick} reference={reference:?} reports={reports:?} droppedSRTCP={dropped_srtcp} deliveredSRTCP={delivered_srtcp}"
                );
                assert!(tick >= 3500 && delivered_srtcp > 0);
                first_reference_report = Some(tick);
            }
            if [1800, 2250, 3300, 4600, 5000].contains(&tick) {
                let committed = engine
                    .ups
                    .get(down.up)
                    .unwrap()
                    .dynacast
                    .as_ref()
                    .unwrap()
                    .committed_message()
                    .unwrap();
                println!(
                    "reference_sr tick={tick} reference={reference:?} reports={reports:?} current={} max={} demand={} committed={committed:?} droppedSRTCP={dropped_srtcp} deliveredSRTCP={delivered_srtcp} media={counts:?}",
                    down.current,
                    down.max_spatial,
                    down.demand_spatial()
                );
                let expected = match tick {
                    1800 => (2, 2, 2),
                    2250 => (-1, -1, 2),
                    3300 => (0, 0, 2),
                    4600 => (0, 0, 0),
                    5000 => (-1, -1, -1),
                    _ => unreachable!(),
                };
                if tick >= 4600 {
                    assert!(
                        reports[2],
                        "real reference SR must arrive by 4600ms after SRTCP resumes at 3500ms"
                    );
                } else {
                    assert_eq!(reports, [false; 3]);
                }
                assert_eq!(
                    (down.current, down.max_spatial, down.demand_spatial()),
                    expected
                );
                assert_eq!(
                    committed,
                    crate::dynacast::message(
                        "vp8",
                        if tick == 5000 {
                            VideoQuality::Off
                        } else {
                            VideoQuality::High
                        }
                    ),
                    "SR alone must not recompute committed dynacast demand"
                );
                let mut expected_events = vec![
                    crate::dynacast::message("vp8", VideoQuality::Off),
                    crate::dynacast::message("vp8", VideoQuality::High),
                ];
                if tick == 5000 {
                    expected_events.push(crate::dynacast::message("vp8", VideoQuality::Off));
                }
                assert_eq!(
                    quality_events
                        .iter()
                        .map(|(_, codecs)| codecs.clone())
                        .collect::<Vec<_>>(),
                    expected_events,
                    "complete quality sequence includes pre-subscription OFF, without an SR-driven LOW"
                );
            }
        }
    }
    assert!(
        publisher.is_connected() && subscriber.is_connected(),
        "same ICE/DTLS connections survive unsupported settings"
    );
    if reference_sr {
        println!(
            "reference_sr final droppedSRTCP={dropped_srtcp} deliveredSRTCP={delivered_srtcp} firstReferenceSR={first_reference_report:?} quality_events={quality_events:?} media[HIGH,OFF,LOW,OFF]={counts:?} connected=({}, {}) reference={:?}",
            publisher.is_connected(),
            subscriber.is_connected(),
            engine.downs.get(di).unwrap().reference_state()
        );
        assert!(engine.transports[&pub_tid].rtc.is_connected());
        assert!(engine.transports[&sub_tid].rtc.is_connected());
        assert!(dropped_srtcp > 0 && delivered_srtcp > 0);
        assert!(first_reference_report.is_some_and(|tick| (3500..=4600).contains(&tick)));
        assert!(
            counts[0] > 10 && counts[2] > 10,
            "real HIGH/LOW MediaData: {counts:?}"
        );
        assert_eq!(
            (counts[1], counts[3]),
            (0, 0),
            "both unsupported phases must be silent"
        );
        assert_eq!(
            quality_events
                .iter()
                .map(|(_, codecs)| codecs.clone())
                .collect::<Vec<_>>(),
            vec![
                crate::dynacast::message("vp8", VideoQuality::Off),
                crate::dynacast::message("vp8", VideoQuality::High),
                crate::dynacast::message("vp8", VideoQuality::Off),
            ],
            "preserve every quality event through the final tick"
        );
        assert!((4700..=5000).contains(&quality_events.last().unwrap().0));
        if source_restart {
            let original_up = engine.downs.get(di).unwrap().up;
            let original_mid = engine.downs.get(di).unwrap().mid;
            let mut restarted = false;
            let mut rebuilt = false;
            let mut received_after_restart = 0;
            engine.handle_command(
                MediaCommand::UpdateTrackSettings {
                    subscriber: "PA_test".into(),
                    track_sid: sid.clone(),
                    settings: TrackSettings {
                        quality: VideoQuality::Low,
                        ..Default::default()
                    },
                },
                negotiated + Duration::from_millis(6500),
            );
            for tick in 6500..8500u64 {
                let now = negotiated + Duration::from_millis(tick);
                publisher.handle_input(Input::Timeout(now)).unwrap();
                subscriber.handle_input(Input::Timeout(now)).unwrap();
                if tick == 7000 {
                    let down = engine.downs.get_mut(di).unwrap();
                    let (reference, reports) = down.reference_state();
                    assert_eq!(reference, Some(2), "real HIGH reference survives LOW");
                    assert!(reports[2], "the old receiver has a real reference SR");
                    assert!(reports[0], "the future LOW reference also has an old SR");
                    assert_eq!((down.current, down.target), (0, 0));
                    assert!(down.last_pt.is_some(), "LOW was actually forwarded");
                    // Pause admission to observe the real restart before its keyframe rebuilds state.
                    down.negotiated = false;
                    let pt = publisher
                        .writer(tx_mid)
                        .unwrap()
                        .payload_params()
                        .find(|p| p.spec().codec == str0m::format::Codec::Vp8)
                        .unwrap()
                        .pt();
                    let mut direct = publisher.direct_api();
                    let stream = direct.stream_tx_by_mid(tx_mid, Some("q".into())).unwrap();
                    // Five backward timestamps must trigger native authenticated restart detection.
                    for index in 1..=5u32 {
                        let sequence = stream.next_seq_no();
                        stream.write_rtp(
                            str0m::rtp::RtpWrite::new(
                                pt,
                                sequence,
                                index * 1000,
                                now,
                                vec![0x10, 0, 0, 0x9d, 1, 0x2a],
                            )
                            .marker(true),
                        );
                    }
                }
                if tick == 7100 {
                    let down = engine.downs.get_mut(di).unwrap();
                    assert!(restarted);
                    assert_eq!(down.reference_state(), (None, [false; 3]));
                    assert!(down.last_pt.is_none());
                    down.negotiated = true;
                }
                if tick.is_multiple_of(30) && !(7000..7100).contains(&tick) {
                    for (index, rid) in ["q", "h", "f"].iter().enumerate() {
                        let writer = publisher.writer(tx_mid).unwrap().rid((*rid).into());
                        let pt = writer
                            .payload_params()
                            .find(|p| p.spec().codec == str0m::format::Codec::Vp8)
                            .unwrap()
                            .pt();
                        let mut frame = vec![0u8; 64];
                        frame[..10].copy_from_slice(&[0x10, 0, 0, 0x9d, 1, 0x2a, 0x80, 2, 0x68, 1]);
                        frame[63] = index as u8;
                        writer
                            .write(
                                pt,
                                now,
                                MediaTime::new(tick * 90, Frequency::NINETY_KHZ),
                                frame,
                            )
                            .unwrap();
                    }
                }
                for (peer, tid, addr, receiving) in [
                    (&mut publisher, pub_tid, pub_addr, false),
                    (&mut subscriber, sub_tid, sub_addr, true),
                ] {
                    loop {
                        match peer.poll_output().unwrap() {
                            Output::Transmit(packet) => {
                                // No fresh publisher SR may hide inherited report
                                // flags or release the new reference after restart.
                                if !receiving
                                    && packet.contents.len() >= 2
                                    && packet.contents[0] >> 6 == 2
                                    && (192..=223).contains(&packet.contents[1])
                                {
                                    continue;
                                }
                                engine.handle_datagram(Datagram {
                                    transport: tid,
                                    proto: Proto::Udp,
                                    source: addr,
                                    destination: server_addr,
                                    data: &packet.contents,
                                    received_at: now,
                                });
                                if !receiving && (7000..7100).contains(&tick) {
                                    let down = engine.downs.get(di).unwrap();
                                    if !restarted && down.reference_state().0.is_none() {
                                        assert_eq!(down.reference_state(), (None, [false; 3]));
                                        assert_eq!((down.current, down.target), (-1, -1));
                                        assert!(down.last_pt.is_none());
                                        restarted = true;
                                    }
                                }
                                if !receiving && tick >= 7100 {
                                    let down = engine.downs.get(di).unwrap();
                                    if down.reference_state().0.is_some() {
                                        assert_eq!(down.reference_state(), (Some(0), [false; 3]));
                                        assert_eq!(down.current, 0);
                                        assert!(down.last_pt.is_some());
                                        rebuilt = true;
                                    }
                                }
                            }
                            Output::Event(Event::MediaData(data)) if receiving && tick >= 7200 => {
                                assert_eq!(data.data.last(), Some(&0));
                                received_after_restart += 1;
                            }
                            Output::Timeout(_) => break,
                            _ => {}
                        }
                    }
                }
                if tick == 7099 {
                    assert!(
                        restarted,
                        "paced authenticated RTP must trigger restart before admission resumes"
                    );
                }
                engine.handle_timeout(now);
                engine.drain_events(&mut Vec::new());
                for (destination, bytes) in packets.0.lock().unwrap().drain(..) {
                    let peer = if destination == pub_addr {
                        &mut publisher
                    } else {
                        assert_eq!(destination, sub_addr);
                        &mut subscriber
                    };
                    peer.handle_input(Input::Receive(
                        now,
                        Receive::new(Protocol::Udp, server_addr, destination, &bytes).unwrap(),
                    ))
                    .unwrap();
                }
            }
            let down = engine.downs.get(di).unwrap();
            assert!(restarted && rebuilt && received_after_restart > 10);
            assert_eq!(down.reference_state(), (Some(0), [false; 3]));
            assert_eq!((down.current, down.target), (0, 0));
            assert_eq!(down.up, original_up);
            assert_eq!(down.mid, original_mid);
            assert_eq!(engine.up_by_mid[&(pub_tid, tx_mid)], original_up);
            assert!(publisher.is_connected() && subscriber.is_connected());
            assert!(engine.transports[&pub_tid].rtc.is_connected());
            assert!(engine.transports[&sub_tid].rtc.is_connected());
        }
        return;
    }
    assert!(
        counts[0] > 10 && counts[2] > 10 && counts[3] > 10,
        "actual authenticated selected-layer packets must arrive: {counts:?}"
    );
    assert_eq!(counts[1], 0, "unknown quality must never alias LOW");
    if independent_padding {
        assert_eq!(
            (lost_up, lost_down),
            (3, 3),
            "real authenticated RTP gaps in both directions"
        );
        assert!(
            peer_nack_sent > 0 && peer_nack_received > 0 && multi_sn_nack,
            "real multi-sequence-number NACK feedback must traverse both encrypted directions"
        );
        let up = engine.transports[&pub_tid].rtc.media_traffic_stats();
        let down = engine.transports[&sub_tid].rtc.media_traffic_stats();
        assert_eq!(up.nacks_sent, peer_nack_received);
        assert_eq!(down.nacks_received, peer_nack_sent);
        assert_eq!(
            engine.stats().nacks,
            up.nacks_sent + up.nacks_received + down.nacks_sent + down.nacks_received
        );
    }
    let actual_padding = engine.transports[&sub_tid].rtc.media_traffic_stats();
    if independent_padding {
        assert!(
            actual_padding.padding_packets_out > 0 && actual_padding.padding_bytes_out > 0,
            "bounded real SRTP padding must enter its own media accounting subset"
        );
    }
    assert_eq!(
        actual_padding.padding_bytes_out, wire_padding.0,
        "actual emitted padding RTP bytes exclude the negotiated SRTP tag"
    );
    assert_eq!(
        actual_padding.padding_packets_out, wire_padding.1,
        "one native produced padding packet is counted exactly once"
    );
    assert_eq!(
        actual_padding.bytes_out,
        actual_padding.primary_bytes_out
            + actual_padding.padding_bytes_out
            + actual_padding.retransmit_bytes_out
    );
    assert_eq!(
        received_before_high_keyframe, 0,
        "high delta frames cannot bypass keyframe gate"
    );
}

#[test]
fn node_subscribed_track_gauge_uses_actual_sdp_bound_lifecycle_not_pending_graph_size() {
    use rtc_core::media_api::{DownTrackSpec, MediaCommand};
    use rtc_core::model::TrackSettings;
    let start = Instant::now();
    let mut engine = engine(start);
    let publisher_tid = transport(&mut engine, PcRole::Publisher, start);
    engine
        .transports
        .get_mut(&publisher_tid)
        .unwrap()
        .spec
        .primary = false;
    let mut publisher = client(start);
    let events = offer(
        &mut engine,
        &mut publisher,
        publisher_tid,
        start,
        |change| {
            change.add_media(MediaKind::Audio, Direction::SendOnly, None, None, None);
        },
    );
    let mid = published_mid(&events);
    let sid = TrackSid::from("TR_actual_subscription_gauge");
    engine.bind_track(publisher_tid, &mid.to_string(), sid.clone(), start);
    let spec = DownTrackSpec {
        subscriber: "PA_test".into(),
        publisher: "PA_test".into(),
        track_sid: sid.clone(),
        stream_id: "PA_test|TR_actual_subscription_gauge".into(),
        settings: TrackSettings::default(),
        adaptive_stream: false,
    };
    engine.handle_command(MediaCommand::Subscribe(spec.clone()), start);
    assert_eq!(engine.stats().downtracks, 1);
    assert_eq!(
        engine.stats().subscribed_tracks,
        0,
        "pending without transport is not a completed subscribe"
    );
    let subscriber_tid = transport(&mut engine, PcRole::Subscriber, start);
    engine.handle_timeout(start + Duration::from_millis(200));
    assert_eq!(
        engine.stats().subscribed_tracks,
        0,
        "creating/offering a real m-line still has no accepted answer"
    );
    let mut subscriber = client(start);
    answer_server_offer(
        &mut engine,
        &mut subscriber,
        subscriber_tid,
        start + Duration::from_millis(200),
    );
    assert_eq!(
        engine.stats().subscribed_tracks,
        1,
        "real accepted SDP emits SubscriptionBound"
    );
    engine.handle_command(
        MediaCommand::UpdateTrackSettings {
            subscriber: "PA_test".into(),
            track_sid: sid.clone(),
            settings: TrackSettings {
                disabled: true,
                ..Default::default()
            },
        },
        start + Duration::from_millis(210),
    );
    engine.handle_timeout(start + Duration::from_millis(400));
    assert_eq!(
        engine.stats().subscribed_tracks,
        1,
        "pause must not cancel an existing subscription"
    );
    engine.handle_command(
        MediaCommand::SetPublisherMuted {
            track_sid: sid.clone(),
            muted: true,
        },
        start + Duration::from_millis(400),
    );
    assert_eq!(
        engine.stats().subscribed_tracks,
        1,
        "publisher mute is not unsubscribe"
    );
    engine.handle_command(
        MediaCommand::Unsubscribe {
            subscriber: "PA_test".into(),
            track_sid: sid.clone(),
        },
        start + Duration::from_millis(410),
    );
    assert_eq!(engine.stats().subscribed_tracks, 0);
    assert_eq!(engine.stats().downtracks, 0);
    engine.handle_command(
        MediaCommand::Subscribe(spec),
        start + Duration::from_millis(420),
    );
    engine.handle_timeout(start + Duration::from_millis(650));
    assert_eq!(
        engine.stats().subscribed_tracks,
        0,
        "new subscription requires its own real answer"
    );
    answer_server_offer(
        &mut engine,
        &mut subscriber,
        subscriber_tid,
        start + Duration::from_millis(650),
    );
    assert_eq!(engine.stats().subscribed_tracks, 1);
    engine.handle_command(
        MediaCommand::CloseTransport(subscriber_tid),
        start + Duration::from_millis(700),
    );
    assert_eq!(
        engine.stats().subscribed_tracks,
        0,
        "retired transport cannot retain the active bound gauge"
    );
    assert_eq!(
        engine.stats().downtracks,
        1,
        "pending graph is retained for reconnect but not counted active"
    );
}

#[path = "regression/audio_dynacast_notify.rs"]
mod audio_dynacast_notify;
#[path = "regression/audio_dynacast_sdp.rs"]
mod audio_dynacast_sdp;
#[path = "codec_regression_tests.rs"]
mod codec_regression_tests;
#[path = "regression/dynacast_notify.rs"]
mod dynacast_notify;

#[path = "receiver_traffic_tests.rs"]
mod receiver_traffic_tests;

#[test]
fn authenticated_source_restart_config_discards_only_old_partial_window_and_keeps_transport() {
    use crate::net::Datagram;
    use str0m::format::Codec;
    use str0m::net::{Protocol, Receive};
    use str0m::rtp::RtpWrite;
    use str0m::{Candidate, Input, Output};
    for enabled in [false, true] {
        let start = Instant::now();
        let mut engine = engine(start);
        let forward_stats = rtc_core::forward_stats::ForwardStats::new(
            100_000_000,
            100_000_000,
            1_000_000_000,
            start,
        )
        .unwrap();
        engine.cfg.rtc.forward_stats = Some(forward_stats.clone());
        engine.cfg.rtc.enable_rtp_stream_restart_detection = enabled;
        engine.cfg.enabled_codecs.push(CodecSpec {
            mime: "video/VP8".into(),
            fmtp_line: String::new(),
        });
        let packets = RoutedPacketQueue::default();
        engine.out = Box::new(packets.clone());
        let tid = transport(&mut engine, PcRole::Publisher, start);
        let server_addr = "127.0.0.1:9671".parse().unwrap();
        let peer_addr = "127.0.0.1:9672".parse().unwrap();
        let t = engine.transports.get_mut(&tid).unwrap();
        let candidate = t
            .rtc
            .add_local_candidate(Candidate::host(server_addr, "udp").unwrap())
            .unwrap()
            .clone();
        t.candidates.push(candidate);
        t.spec.primary = false;
        let mut peer = client(start);
        peer.add_local_candidate(Candidate::host(peer_addr, "udp").unwrap());
        let announced = offer(&mut engine, &mut peer, tid, start, |api| {
            api.add_media(
                MediaKind::Video,
                Direction::SendOnly,
                None,
                Some("CID_restart".into()),
                None,
            );
        });
        let mid = published_mid(&announced);
        let sid = TrackSid::from("TR_restart");
        engine.bind_track(tid, &mid.to_string(), sid.clone(), start);
        let original_up = engine.up_by_sid[&sid];
        let pt = peer
            .writer(mid)
            .unwrap()
            .payload_params()
            .find(|p| p.spec().codec == Codec::Vp8)
            .unwrap()
            .pt();
        let mut sent = 0u16;
        let mut first = None;
        let mut last_source = None;
        for tick in 0..7000u64 {
            let now = start + Duration::from_millis(tick);
            peer.handle_input(Input::Timeout(now)).unwrap();
            if peer.is_connected() {
                let at = *first.get_or_insert(tick);
                if sent < 7 && tick >= at + u64::from(sent) * 30 {
                    let ts = if sent == 0 {
                        90000
                    } else if sent == 6 {
                        if enabled { 8000 } else { 93000 }
                    } else {
                        u32::from(sent) * 1000
                    };
                    peer.direct_api()
                        .stream_tx_by_mid(mid, None)
                        .unwrap()
                        .write_rtp(
                            RtpWrite::new(
                                pt,
                                (1000 + u64::from(sent)).into(),
                                ts,
                                now,
                                vec![0x10, 0, 0, 0x9d, 1, 0x2a],
                            )
                            .marker(true),
                        );
                    sent += 1;
                }
            }
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
            for (destination, bytes) in packets.0.lock().unwrap().drain(..).collect::<Vec<_>>() {
                assert_eq!(destination, peer_addr);
                peer.handle_input(Input::Receive(
                    now,
                    Receive::new(Protocol::Udp, server_addr, peer_addr, &bytes).unwrap(),
                ))
                .unwrap();
            }
            if tick == 1000 {
                assert_eq!(sent, 7, "real authenticated sender must emit all packets");
                assert!(peer.is_connected() && engine.transports[&tid].rtc.is_connected());
                let (sources, source) = engine.receiver_traffic.test_bound_sources();
                assert_eq!(
                    sources, 1,
                    "restart replaces source, never logical track/transport"
                );
                last_source = source;
            }
        }
        forward_stats.tick(start + Duration::from_secs(7));
        assert_eq!(
            forward_stats.snapshot().histogram_count,
            0,
            "actual incoming SRTP without any subscriber admission must not invent samples"
        );
        assert_eq!(engine.transports.len(), 1);
        assert_eq!(engine.up_by_sid.len(), 1);
        assert_eq!(engine.ups.len(), 1);
        assert_eq!(
            engine.up_by_sid[&sid], original_up,
            "restart must preserve exact logical track allocation"
        );
        assert_eq!(
            engine.up_by_mid[&(tid, mid)],
            original_up,
            "source must remain attached to its original transport/MID"
        );
        assert!(last_source.is_some());
        let traffic = engine.receiver_traffic.accepted;
        assert_eq!(
            traffic.packets_in,
            if enabled { 2 } else { 7 },
            "old unreported source must be discarded only on enabled restart; disabled sees original plus final legal packet"
        );
        engine.unpublish(&sid, start + Duration::from_secs(8));
        assert!(engine.up_by_mid.is_empty());
        assert!(engine.up_by_sid.is_empty());
        assert_eq!(engine.ups.len(), 0);
        assert!(
            engine.receiver_traffic.test_bound_sources().0 == 0,
            "unpublish releases all bound source consumers"
        );
        engine.handle_command(
            rtc_core::media_api::MediaCommand::CloseTransport(tid),
            start + Duration::from_secs(8),
        );
        assert!(engine.transports.is_empty());
    }
}
#[path = "regression/unmanaged_bwe.rs"]
mod unmanaged_bwe;

#[test]
fn forwarding_packet_cohort_filters_buffered_and_out_of_order_without_replacing_arrivals() {
    use str0m::media::ForwardingPacket;
    let now = Instant::now();
    let mut engine = engine(now);
    let stats =
        rtc_core::forward_stats::ForwardStats::new(100_000_000, 100_000_000, 1_000_000_000, now)
            .unwrap();
    engine.cfg.rtc.forward_stats = Some(stats.clone());
    let packets = [
        ForwardingPacket {
            sequence: 0,
            spatial: None,
            temporal: None,
            extension_index: 0,
            received: now,
            out_of_order: false,
            buffered: false,
        },
        ForwardingPacket {
            sequence: 0,
            spatial: None,
            temporal: None,
            extension_index: 0,
            received: now + Duration::from_millis(4),
            out_of_order: false,
            buffered: false,
        },
        ForwardingPacket {
            sequence: 0,
            spatial: None,
            temporal: None,
            extension_index: 0,
            received: now + Duration::from_millis(3),
            out_of_order: true,
            buffered: false,
        },
        ForwardingPacket {
            sequence: 0,
            spatial: None,
            temporal: None,
            extension_index: 0,
            received: now + Duration::from_millis(2),
            out_of_order: false,
            buffered: true,
        },
    ];
    let mut admissions = crate::forward_timing::ForwardingAdmissions::new(&packets);
    admissions.accepted(0, 2, 0);
    admissions.accepted(0, 2, 0);
    engine.record_forwarding_cohort(
        &admissions,
        now + Duration::from_millis(5),
        now + Duration::from_millis(1),
    );
    stats.tick(now + Duration::from_millis(100));
    assert_eq!(stats.snapshot().histogram_count, 2);
    assert_eq!(stats.snapshot().histogram_sum_ns, 6_000_000.0);
}

#[path = "regression/svc_forward_timing.rs"]
mod svc_forward_timing;

mod av1_target_timing;

mod av1_chain_timing;

#[path = "regression/audio_level_source.rs"]
mod audio_level_source;

mod ice_fallback_source;

#[path = "regression/publisher_established.rs"]
mod publisher_established;

mod selected_ice_liveness;

mod av1_original_packets;

mod rate_receiver_domains;

mod vp9_dd_original_packets;

mod dd_disappearance;

mod dd_padding;

mod playout_firefox;

#[path = "regression/dd_one_rid_counterexample.rs"]
mod dd_one_rid_counterexample;

mod video_bitrate_windows;

mod video_dd_bitrate_windows;

#[path = "regression/gap_pli.rs"]
mod gap_pli;

#[path = "regression/remote_close.rs"]
mod remote_close;

#[path = "regression/seq_gap_wire.rs"]
mod seq_gap_wire;

mod rid_publication;

mod media_counting_boundary;

#[path = "regression/batch_io_lb_threshold.rs"]
mod batch_io_lb_threshold;
