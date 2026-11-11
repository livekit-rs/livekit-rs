//! SPEC-0074 C20: `rtc.enable_warp` — SNAP and DTLS-in-STUN consumers.
//!
//! Upstream turns WARP into two setting-engine switches,
//! `EnableSped` (DTLS-in-STUN) and `EnableSctpSnap` (SCTP INIT in SDP), and only
//! for non-one-shot peer connections (`roommanager.go:487` computes
//! `enableWarp := !useOneShotSignallingMode && rtc.enable_warp`, then
//! `transport.go:389-393` applies both). This module covers the SNAP half end to
//! end: the config reaches the real `str0m` peer connection, offers and answers
//! carry `a=sctp-init:<base64 INIT>`, and a SNAP association carries a real data
//! channel message over real ICE/DTLS/SCTP.
//!
//! SPED now reaches native authenticated STUN/DTLS flight and ACK consumers.
//! The real packet/loss/fallback test below is pending coordinator execution;
//! historical SNAP evidence alone never certifies this new SPED candidate.

use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use rtc_core::config::{
    AudioConfig, CongestionControl, Filter, NodeIp, PliThrottle, RtcConfig, UdpMode, VideoConfig,
};
use rtc_core::ids::{ParticipantSid, TransportId};
use rtc_core::media_api::{MediaEvent, PcRole, TransportSpec};
use rtc_core::model::CodecSpec;
use rtc_core::session::SdpType;
use str0m::change::SdpAnswer;

use crate::engine::{MediaEngine, MediaEngineConfig};
use crate::net::{IceCredentials, LocalCandidate, PacketOut, Proto};

type Wire = Vec<(SocketAddr, Vec<u8>)>;

/// Packets the engine wants on the wire, drained by the test loop.
#[derive(Clone, Default)]
struct RoutedPackets(Arc<Mutex<Wire>>);

impl PacketOut for RoutedPackets {
    fn send(&self, _: Proto, _: SocketAddr, destination: SocketAddr, data: &[u8]) {
        self.0.lock().unwrap().push((destination, data.to_vec()));
    }
}

fn engine(now: Instant, enable_warp: bool) -> MediaEngine {
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
                interfaces: Filter::default(),
                ips: Filter::default(),
                enable_loopback_candidate: false,
                use_mdns: false,
                enable_warp,
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
                    allow_pause: true,
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
                update_interval: Duration::from_millis(500),
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

struct Discard;
impl PacketOut for Discard {
    fn send(&self, _: Proto, _: SocketAddr, _: SocketAddr, _: &[u8]) {}
}

fn spec(
    tid: TransportId,
    role: PcRole,
    one_shot: bool,
    server_data_channels: bool,
) -> TransportSpec {
    TransportSpec {
        ice_preference: Default::default(),
        one_shot,
        id: tid,
        participant: ParticipantSid::from("PA_warp"),
        role,
        ice_lite: true,
        server_data_channels,
        enable_data_tracks: true,
        primary: true,
        client_supports_red: false,
        codec_switch_requires_renegotiation: false,
        disable_playout_delay: false,
        allow_pause: true,
    }
}

/// A real str0m client that opted into SNAP, i.e. what a WARP-aware peer sends.
fn snap_client(now: Instant) -> str0m::Rtc {
    str0m::Rtc::builder()
        .set_crypto_provider(rtc_crypto::provider().into())
        .set_dtls_cert(rtc_crypto::dtls_cert().unwrap())
        .set_snap_enabled(true)
        .build(now)
}

fn take(events: &[MediaEvent], kind: SdpType) -> Option<String> {
    events.iter().find_map(|e| match e {
        MediaEvent::LocalDescription { kind: k, sdp, .. } if *k == kind => Some(sdp.clone()),
        _ => None,
    })
}

/// `a=sctp-init` must sit on the `m=application` section, not on audio/video.
fn application_section_has_sctp_init(sdp: &str) -> bool {
    let Some(at) = sdp.find("m=application") else {
        return false;
    };
    sdp[at..].contains("a=sctp-init:")
}

#[test]
fn warp_opts_the_server_offer_into_snap() {
    let now = Instant::now();
    let mut engine = engine(now, true);
    let tid = TransportId::next();
    engine.create_transport(
        spec(tid, PcRole::Subscriber, false, true),
        IceCredentials::random(),
        vec![],
        now,
    );
    let mut events = vec![];
    engine.drain_events(&mut events);
    let sdp = take(&events, SdpType::Offer).expect("subscriber transport offers a data channel");
    assert!(
        application_section_has_sctp_init(&sdp),
        "enable_warp must put a=sctp-init on the application m-line:\n{sdp}"
    );
}

#[test]
fn default_config_leaves_the_server_offer_plain() {
    let now = Instant::now();
    let mut engine = engine(now, false);
    let tid = TransportId::next();
    engine.create_transport(
        spec(tid, PcRole::Subscriber, false, true),
        IceCredentials::random(),
        vec![],
        now,
    );
    let mut events = vec![];
    engine.drain_events(&mut events);
    let sdp = take(&events, SdpType::Offer).expect("subscriber transport offers a data channel");
    assert!(
        !sdp.contains("a=sctp-init:"),
        "the default must not opt into SNAP:\n{sdp}"
    );
}

#[test]
fn one_shot_signalling_never_opts_in_to_snap() {
    let now = Instant::now();
    let mut engine = engine(now, true);
    let tid = TransportId::next();
    // roommanager.go:487: `enableWarp := !useOneShotSignallingMode && rtc.enable_warp`.
    engine.create_transport(
        spec(tid, PcRole::Subscriber, true, true),
        IceCredentials::random(),
        vec![],
        now,
    );
    let mut events = vec![];
    engine.drain_events(&mut events);
    let Some(sdp) = take(&events, SdpType::Offer) else {
        // One-shot signalling normally lets the client offer; nothing to inspect.
        return;
    };
    assert!(
        !sdp.contains("a=sctp-init:"),
        "one-shot transports must never opt into SNAP:\n{sdp}"
    );
}

/// Answer side: the engine reciprocates a peer's SNAP offer only when WARP is on
/// (pion gates the answer on its own `EnableSctpSnap`, peerconnection.go:3014).
#[test]
fn answer_reciprocation_follows_the_warp_flag() {
    for (warp, expect) in [(true, true), (false, false)] {
        let now = Instant::now();
        let mut engine = engine(now, warp);
        let tid = TransportId::next();
        engine.create_transport(
            spec(tid, PcRole::Single, false, false),
            IceCredentials::random(),
            vec![],
            now,
        );
        let mut client = snap_client(now);
        let mut change = client.sdp_api();
        change.add_channel("_reliable".into());
        let (offer, pending) = change.apply().unwrap();
        let offer_sdp = offer.to_sdp_string();
        assert!(
            offer_sdp.contains("a=sctp-init:"),
            "the SNAP client must offer a=sctp-init:\n{offer_sdp}"
        );
        engine.on_remote_description(tid, SdpType::Offer, offer_sdp, 1, now);
        let mut events = vec![];
        engine.drain_events(&mut events);
        let answer = take(&events, SdpType::Answer).expect("server answers the client offer");
        assert_eq!(
            application_section_has_sctp_init(&answer),
            expect,
            "enable_warp={warp} answer mismatch:\n{answer}"
        );
        // Both variants must be accepted by a real SNAP-capable peer.
        client
            .sdp_api()
            .accept_answer(pending, SdpAnswer::from_sdp_string(&answer).unwrap())
            .unwrap();
    }
}

/// Full path: SNAP offer → answer with `a=sctp-init` → real ICE/DTLS/SCTP →
/// data-channel payload delivered to the room.
fn snap_session(enable_warp: bool) -> (String, bool) {
    use crate::net::Datagram;
    use str0m::net::{Protocol, Receive};
    use str0m::{Candidate, Input, Output};

    let start = Instant::now();
    let mut engine = engine(start, enable_warp);
    let packets = RoutedPackets::default();
    engine.out = Box::new(packets.clone());
    let server_addr: SocketAddr = "127.0.0.1:9301".parse().unwrap();
    let client_addr: SocketAddr = "127.0.0.1:9302".parse().unwrap();
    let tid = TransportId::next();
    engine.create_transport(
        spec(tid, PcRole::Single, false, false),
        IceCredentials::random(),
        vec![LocalCandidate {
            proto: Proto::Udp,
            addr: server_addr,
            base: server_addr,
        }],
        start,
    );
    let mut client = snap_client(start);
    client.add_local_candidate(Candidate::host(client_addr, "udp").unwrap());
    let mut change = client.sdp_api();
    let cid = change.add_channel("_reliable".into());
    let (offer, pending) = change.apply().unwrap();
    engine.on_remote_description(tid, SdpType::Offer, offer.to_sdp_string(), 1, start);
    let mut events = vec![];
    engine.drain_events(&mut events);
    let answer = take(&events, SdpType::Answer).expect("server answers the client offer");
    client
        .sdp_api()
        .accept_answer(pending, SdpAnswer::from_sdp_string(&answer).unwrap())
        .unwrap();

    let mut sent = false;
    for tick in 0..3000u64 {
        let now = start + Duration::from_millis(tick * 10);
        client.handle_input(Input::Timeout(now)).unwrap();
        if !sent
            && client.is_connected()
            && let Some(mut ch) = client.channel(cid)
        {
            sent = ch.write(true, b"snap-upstream").unwrap();
        }
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
                Output::Event(_) => {}
                Output::Timeout(_) => break,
            }
        }
        engine.handle_timeout(now);
        let mut events = vec![];
        engine.drain_events(&mut events);
        for event in events {
            if let MediaEvent::DataReceived { payload, .. } = event {
                assert_eq!(payload, b"snap-upstream");
                return (answer, true);
            }
        }
        let outbound: Vec<_> = packets.0.lock().unwrap().drain(..).collect();
        for (destination, bytes) in outbound {
            assert_eq!(destination, client_addr);
            client
                .handle_input(Input::Receive(
                    now,
                    Receive::new(Protocol::Udp, server_addr, destination, &bytes).unwrap(),
                ))
                .unwrap();
        }
    }
    (answer, false)
}

#[test]
fn warp_snap_association_carries_real_data_over_ice_dtls_sctp() {
    let (answer, received) = snap_session(true);
    assert!(
        application_section_has_sctp_init(&answer),
        "warp must reciprocate the peer's SNAP offer:\n{answer}"
    );
    assert!(
        received,
        "a SNAP association must carry the data-channel payload through the real engine"
    );
}

#[test]
fn warp_off_declines_snap_and_the_regular_handshake_still_works() {
    let (answer, received) = snap_session(false);
    assert!(
        !application_section_has_sctp_init(&answer),
        "without warp the answer must not opt into SNAP:\n{answer}"
    );
    assert!(
        received,
        "the fallback (non-SNAP) SCTP handshake must still deliver the payload"
    );
}

/// Inspect the actual serialized STUN message, not an ICE config getter.
fn sped_attribute(data: &[u8], kind: u16) -> Option<(usize, &[u8])> {
    if data.len() < 20 || data[0] & 0xc0 != 0 || data[4..8] != [0x21, 0x12, 0xa4, 0x42] {
        return None;
    }
    let mut at = 20;
    while at + 4 <= data.len() {
        let typ = u16::from_be_bytes([data[at], data[at + 1]]);
        let len = u16::from_be_bytes([data[at + 2], data[at + 3]]) as usize;
        if at + 4 + len > data.len() {
            return None;
        }
        if typ == kind {
            return Some((at + 4, &data[at + 4..at + 4 + len]));
        }
        at += 4 + ((len + 3) & !3);
    }
    None
}

/// DTLS-in-STUN must carry native cryptographic records all the way through
/// authenticated ICE to an established SCTP channel, including loss/fallback.
#[test]
fn warp_sped_real_authenticated_flights_ack_loss_and_plain_peer_fallback() {
    use crate::net::Datagram;
    use str0m::net::{Protocol, Receive};
    use str0m::{Candidate, Input, Output};
    for (server_warp, client_sped, drop_first) in [
        (true, true, false),
        (true, true, true),
        (true, false, false),
        (false, true, false),
    ] {
        let start = Instant::now();
        let mut engine = engine(start, server_warp);
        let packets = RoutedPackets::default();
        engine.out = Box::new(packets.clone());
        let server_addr: SocketAddr = "127.0.0.1:9601".parse().unwrap();
        let client_addr: SocketAddr = "127.0.0.1:9602".parse().unwrap();
        let tid = TransportId::next();
        engine.create_transport(
            spec(tid, PcRole::Single, false, false),
            IceCredentials::random(),
            vec![LocalCandidate {
                proto: Proto::Udp,
                addr: server_addr,
                base: server_addr,
            }],
            start,
        );
        let mut client = str0m::Rtc::builder()
            .set_crypto_provider(rtc_crypto::provider().into())
            .set_dtls_cert(rtc_crypto::dtls_cert().unwrap())
            .set_snap_enabled(true)
            .set_sped_enabled(client_sped)
            .build(start);
        client.add_local_candidate(Candidate::host(client_addr, "udp").unwrap());
        let mut change = client.sdp_api();
        let cid = change.add_channel("_reliable".into());
        let (offer, pending) = change.apply().unwrap();
        engine.on_remote_description(tid, SdpType::Offer, offer.to_sdp_string(), 1, start);
        let mut events = Vec::new();
        engine.drain_events(&mut events);
        let answer = take(&events, SdpType::Answer).unwrap();
        client
            .sdp_api()
            .accept_answer(pending, SdpAnswer::from_sdp_string(&answer).unwrap())
            .unwrap();
        let (
            mut embedded_client,
            mut embedded_server,
            mut acks,
            mut dropped,
            mut tampered,
            mut sent,
            mut received,
        ) = (0, 0, 0, false, false, false, false);
        for tick in 0..1500u64 {
            let now = start + Duration::from_millis(tick * 10);
            client.handle_input(Input::Timeout(now)).unwrap();
            if !sent && client.is_connected() {
                if let Some(mut channel) = client.channel(cid) {
                    sent = channel.write(true, b"actual-sped-dtls-sctp").unwrap();
                }
            }
            loop {
                match client.poll_output().unwrap() {
                    Output::Transmit(p) => {
                        if let Some((offset, record)) = sped_attribute(&p.contents, 0xc070) {
                            assert!(!record.is_empty());
                            embedded_client += 1;
                            if !tampered && server_warp && client_sped {
                                let mut corrupt = p.contents.to_vec();
                                corrupt[offset] ^= 0x80;
                                // Same transaction/credentials, changed covered bytes:
                                // if injected before integrity it destroys the handshake.
                                engine.handle_datagram(Datagram {
                                    transport: tid,
                                    proto: Proto::Udp,
                                    source: client_addr,
                                    destination: server_addr,
                                    data: &corrupt,
                                    received_at: now,
                                });
                                tampered = true;
                            }
                            if drop_first && !dropped {
                                dropped = true;
                                continue;
                            }
                        }
                        if let Some((_, ack)) = sped_attribute(&p.contents, 0xc071) {
                            assert!(ack.len() <= 16 && ack.len() % 4 == 0);
                            if !ack.is_empty() {
                                acks += 1;
                            }
                        }
                        engine.handle_datagram(Datagram {
                            transport: tid,
                            proto: Proto::Udp,
                            source: client_addr,
                            destination: server_addr,
                            data: &p.contents,
                            received_at: now,
                        });
                    }
                    Output::Timeout(_) => break,
                    _ => {}
                }
            }
            engine.handle_timeout(now);
            engine.drain_events(&mut events);
            received|=events.iter().any(|e|matches!(e,MediaEvent::DataReceived {payload,..} if payload==b"actual-sped-dtls-sctp"));
            events.clear();
            for (destination, bytes) in packets.0.lock().unwrap().drain(..) {
                assert_eq!(destination, client_addr);
                if let Some((_, record)) = sped_attribute(&bytes, 0xc070) {
                    assert!(!record.is_empty());
                    embedded_server += 1;
                }
                if let Some((_, ack)) = sped_attribute(&bytes, 0xc071) {
                    assert!(ack.len() <= 16 && ack.len() % 4 == 0);
                    if !ack.is_empty() {
                        acks += 1;
                    }
                }
                client
                    .handle_input(Input::Receive(
                        now,
                        Receive::new(Protocol::Udp, server_addr, client_addr, &bytes).unwrap(),
                    ))
                    .unwrap();
            }
            if received {
                break;
            }
        }
        assert!(
            sent && received && client.is_connected(),
            "native DTLS/SCTP completion server_warp={server_warp} client_sped={client_sped} loss={drop_first}; embedded={embedded_client}/{embedded_server},acks={acks}"
        );
        if server_warp && client_sped {
            assert!(embedded_client > 0 && embedded_server > 0 && acks > 0 && tampered);
            assert_eq!(dropped, drop_first);
        }
        if !server_warp {
            assert_eq!(embedded_server, 0);
        }
        if !client_sped {
            assert_eq!(embedded_client, 0);
        }
        engine.handle_command(
            rtc_core::media_api::MediaCommand::CloseTransport(tid),
            start + Duration::from_secs(16),
        );
        assert!(
            !engine.transports.contains_key(&tid),
            "teardown releases controller and pending flight owner"
        );
    }
}

/// Requires a coordinator-built immutable helper from
/// tests/oracles/warp-sped/go-peer. This exercises the fixed Go implementation,
/// not a second Rust peer or a fixture that invents the expected protocol.
#[test]
#[ignore = "requires fixed Go/Pion helper GO_LIVEKIT_SPED_PEER"]
fn warp_sped_fixed_go_pion_peer_over_real_udp() {
    use crate::net::Datagram;
    use std::io::{BufRead, BufReader, Write};
    use std::process::{Child, Command, Stdio};
    use std::sync::mpsc;
    struct OwnedPeer(Child);
    impl Drop for OwnedPeer {
        fn drop(&mut self) {
            if self.0.try_wait().ok().flatten().is_none() {
                let _ = self.0.kill();
            }
            let _ = self.0.wait();
        }
    }
    let binary =
        std::env::var_os("GO_LIVEKIT_SPED_PEER").expect("immutable fixed-source Go helper path");
    assert!(std::path::Path::new(&binary).is_absolute());
    for (server_warp, go_sped, loss) in [
        (true, true, false),
        (true, true, true),
        (true, false, false),
        (false, true, false),
    ] {
        let mut peer = OwnedPeer(
            Command::new(&binary)
                .args([format!("--sped={go_sped}"), format!("--drop-first={loss}")])
                .stdin(Stdio::piped())
                .stdout(Stdio::piped())
                .stderr(Stdio::inherit())
                .spawn()
                .unwrap(),
        );
        let mut input = peer.0.stdin.take().unwrap();
        let output = peer.0.stdout.take().unwrap();
        let (send, recv) = mpsc::channel();
        let reader = std::thread::spawn(move || {
            for line in BufReader::new(output).lines() {
                let line = line.unwrap();
                assert!(line.len() < 1024 * 1024);
                if send.send(line).is_err() {
                    break;
                }
            }
        });
        let offer: serde_json::Value = serde_json::from_str(
            &recv
                .recv_timeout(Duration::from_secs(8))
                .expect("Go actual gathered offer"),
        )
        .unwrap();
        assert_eq!(offer["type"], "offer");
        let socket = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
        socket.set_nonblocking(true).unwrap();
        let local = socket.local_addr().unwrap();
        let start = Instant::now();
        let mut engine = engine(start, server_warp);
        let packets = RoutedPackets::default();
        engine.out = Box::new(packets.clone());
        let tid = TransportId::next();
        engine.create_transport(
            spec(tid, PcRole::Single, false, false),
            IceCredentials::random(),
            vec![LocalCandidate {
                proto: Proto::Udp,
                addr: local,
                base: local,
            }],
            start,
        );
        engine.on_remote_description(
            tid,
            SdpType::Offer,
            offer["sdp"].as_str().unwrap().to_owned(),
            1,
            start,
        );
        let mut events = Vec::new();
        engine.drain_events(&mut events);
        let answer = take(&events, SdpType::Answer).unwrap();
        events.clear();
        writeln!(
            input,
            "{}",
            serde_json::json!({"type":"answer","sdp":answer})
        )
        .unwrap();
        input.flush().unwrap();
        let deadline = start + Duration::from_secs(15);
        let mut received = false;
        let mut buffer = [0u8; 65536];
        let mut rust_embedded = 0;
        while Instant::now() < deadline && !received {
            let now = Instant::now();
            loop {
                match socket.recv_from(&mut buffer) {
                    Ok((n, source)) => engine.handle_datagram(Datagram {
                        transport: tid,
                        proto: Proto::Udp,
                        source,
                        destination: local,
                        data: &buffer[..n],
                        received_at: now,
                    }),
                    Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => break,
                    Err(e) => panic!("actual Go UDP receive failed: {e}"),
                }
            }
            engine.handle_timeout(now);
            engine.drain_events(&mut events);
            received|=events.iter().any(|event|matches!(event,MediaEvent::DataReceived {payload,..} if payload==b"pion-sped-upstream"));
            events.clear();
            for (destination, bytes) in packets.0.lock().unwrap().drain(..) {
                if sped_attribute(&bytes, 0xc070).is_some() {
                    rust_embedded += 1;
                }
                assert_eq!(socket.send_to(&bytes, destination).unwrap(), bytes.len());
            }
            std::thread::sleep(Duration::from_millis(1));
        }
        assert!(
            received,
            "fixed Go data did not reach production media consumer: server={server_warp},Go={go_sped},loss={loss}"
        );
        writeln!(input, "{}", serde_json::json!({"type":"received"})).unwrap();
        input.flush().unwrap();
        drop(input);
        let result: serde_json::Value = serde_json::from_str(
            &recv
                .recv_timeout(Duration::from_secs(8))
                .expect("Go close and packet observation report"),
        )
        .unwrap();
        assert_eq!(result["type"], "result");
        assert_eq!(result["actual_sctp_send"], true);
        assert_eq!(result["pc_closed"], true);
        if server_warp && go_sped {
            assert!(result["sent_embedded_dtls"].as_u64().unwrap() > 0);
            assert!(result["received_embedded_dtls"].as_u64().unwrap() > 0);
            assert!(
                result["sent_ack"].as_u64().unwrap() + result["received_ack"].as_u64().unwrap() > 0
            );
            assert!(rust_embedded > 0);
            assert_eq!(result["dropped"], loss);
        }
        if !server_warp {
            assert_eq!(rust_embedded, 0);
            assert_eq!(result["received_embedded_dtls"], 0);
        }
        if !go_sped {
            assert_eq!(result["sent_embedded_dtls"], 0);
        }
        let end = Instant::now() + Duration::from_secs(5);
        let status = loop {
            if let Some(status) = peer.0.try_wait().unwrap() {
                break status;
            }
            assert!(Instant::now() < end, "Go helper must terminate itself");
            std::thread::sleep(Duration::from_millis(10));
        };
        assert!(status.success());
        reader.join().unwrap();
        engine.handle_command(
            rtc_core::media_api::MediaCommand::CloseTransport(tid),
            Instant::now(),
        );
        assert!(!engine.transports.contains_key(&tid));
        eprintln!("fixed-go-sped server={server_warp} go={go_sped} loss={loss}: {result}");
    }
}
