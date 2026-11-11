//! End-to-end acceptance for `rtc.use_mdns`.
//!
//! The peer offers no address at all: every `a=candidate:` line is stripped
//! from the offer and the only candidate it trickles is a `.local` name. The
//! server therefore has exactly one way to reach it — query mDNS, rewrite the
//! candidate and let str0m run ICE/DTLS against the resolved address.
//!
//! The query target is a unicast responder instead of the mDNS group so the
//! case is deterministic (see `mdns::GROUPS` for the production target and
//! `production_targets_are_the_standard_mdns_groups` for its assertion).
//!
//! The client runs ICE-lite: it never sends checks, so connectivity proves the
//! server really used the resolved remote candidate (a full-ICE peer would
//! connect from a peer-reflexive candidate created by its own checks, hiding a
//! missing resolution).
use super::*;
use crate::net::{LocalInterface, NetConfig, mdns};
use rtc_core::data::{DataHandler, DataSender, DataVerdict};
use rtc_core::events::{NullSink, RoomEndReason};
use rtc_core::permission::{Grants, RoomConfig, VideoGrant};
use rtc_core::session::{ClientCapabilities, ClientMeta, SdkKind};
use std::net::{IpAddr, Ipv4Addr};
use std::sync::atomic::Ordering;
use str0m::Candidate;
use str0m::change::SdpAnswer;
use str0m::net::{Protocol, Receive};
use str0m::{Event, Input, Output, Rtc};

/// The name the peer trickles and the responder owns.
const MDNS_NAME: &str = "lk-mdns-e2e.local";

struct DropData;
impl DataHandler for DropData {
    fn inbound(&self, _: &DataSender<'_>, _: DataKind, _: &str, _: &[u8]) -> DataVerdict {
        DataVerdict::Drop
    }
}

/// Full ICE on the server side so it must initiate checks towards the remote
/// candidate instead of learning the peer from inbound checks.
fn full_ice_params(name: &str) -> JoinParams {
    JoinParams {
        room: RoomKey::new(ProjectId::local(), name),
        grants: Grants {
            identity: name.into(),
            video: VideoGrant {
                room_join: true,
                room: name.into(),
                ..Default::default()
            },
            ..Default::default()
        },
        identity: name.into(),
        name: String::new(),
        metadata: String::new(),
        attributes: BTreeMap::new(),
        client: ClientMeta {
            sdk: SdkKind::Js,
            protocol: 17,
            ..Default::default()
        },
        capabilities: ClientCapabilities {
            subscriber_primary: true,
            ice_lite: false,
            unpublish: true,
            fast_start: true,
            request_response: true,
            ..Default::default()
        },
        auto_subscribe: true,
        auto_subscribe_data_track: false,
        adaptive_stream: false,
        single_pc: true,
        disable_ice_lite: true,
        subscriber_allow_pause: None,
        reconnect: false,
        reconnect_reason: 0,
        participant_sid: None,
        add_tracks: vec![],
        publisher_offer: None,
    }
}

/// A real `RoomTask` with real media sockets, exactly like the manager builds
/// it, plus the inbox so the test can join and drive it.
async fn running_task(
    use_mdns: bool,
) -> (
    mpsc::Sender<RoomInput>,
    NetRouter,
    Arc<std::sync::atomic::AtomicUsize>,
) {
    let mut config = super::tests::test_config();
    config.rtc.use_mdns = use_mdns;
    config.rtc.use_ice_lite = false;
    let cfg = Arc::new(config);
    let net = NetRouter::start(NetConfig {
        udp_enabled: true,
        udp_port: Some(0),
        port_range: (0, 0),
        tcp_port: None,
        interfaces: vec![LocalInterface {
            bind: Ipv4Addr::LOCALHOST.into(),
            advertise: Ipv4Addr::LOCALHOST.into(),
        }],
    })
    .await
    .unwrap();
    let now = Instant::now();
    let room = Room::new(
        RoomKey::new(ProjectId::local(), "mdns"),
        Arc::new(rtc_core::config::RoomPolicy::from(cfg.as_ref())),
        Arc::new(DropData),
        &RoomConfig::default(),
        now,
    );
    let snapshot = Arc::new(Mutex::new(RoomSnapshot {
        info: room.info(),
        participants: 0,
        tracks_in: 0,
        tracks_out: 0,
        bytes_in: 0,
        bytes_out: 0,
        packets_in: 0,
        packets_out: 0,
        nacks: 0,
        retransmit_bytes_out: 0,
        retransmit_packets_out: 0,
        estimated_capacity_bps: 0,
        allocated_bps: 0,
        allocation_overshoot_bps: 0,
        paused_video_tracks: 0,
        probe_attempts: 0,
        probe_successes: 0,
        probe_failures: 0,
        congestion_events: 0,
    }));
    let (tx, rx) = mpsc::channel(ROOM_INBOX);
    let mut task = RoomTask::new(
        room,
        cfg.as_ref(),
        net.clone(),
        Arc::new(NullSink),
        snapshot,
        tx.clone(),
        rx,
    )
    .unwrap();
    let (responder, queries) =
        mdns::test_support::responder_with_counter(MDNS_NAME, IpAddr::V4(Ipv4Addr::LOCALHOST))
            .await;
    task.mdns_targets = vec![responder];
    let inbox = tx.clone();
    tokio::spawn(task.run());
    (inbox, net, queries)
}

/// Drive a real ICE-lite str0m peer whose only candidate is the mDNS name and
/// report whether the session connected.
async fn probe(use_mdns: bool) -> (usize, bool) {
    let (inbox, net, queries) = running_task(use_mdns).await;
    let (reply, joined) = oneshot::channel();
    inbox
        .send(RoomInput::Join {
            session_started_at: tokio::time::Instant::now().into_std(),
            params: Box::new(full_ice_params(if use_mdns {
                "mdns-e2e-on"
            } else {
                "mdns-e2e-off"
            })),
            reply,
        })
        .await
        .unwrap();
    let mut session = joined.await.unwrap().unwrap();
    let socket = tokio::net::UdpSocket::bind((Ipv4Addr::LOCALHOST, 0))
        .await
        .unwrap();
    let local = socket.local_addr().unwrap();
    let mut rtc = Rtc::builder()
        .set_crypto_provider(rtc_crypto::provider().into())
        .set_dtls_cert(rtc_crypto::dtls_cert().unwrap())
        .set_ice_lite(true)
        .build(std::time::Instant::now());
    rtc.add_local_candidate(Candidate::host(local, "udp").unwrap())
        .unwrap();
    let mut sdp = rtc.sdp_api();
    sdp.add_channel("_reliable".into());
    let (offer, pending) = sdp.apply().unwrap();
    // The offer must not leak the peer's real address: the server may only
    // reach it through the mDNS candidate trickled below.
    let stripped: String = offer
        .to_sdp_string()
        .lines()
        .filter(|line| !line.starts_with("a=candidate:"))
        .map(|line| format!("{line}\r\n"))
        .collect();
    assert!(
        session
            .apply(SessionRequest::Offer {
                sdp: stripped,
                id: 1
            })
            .await
    );
    let answer = tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            match session.output.recv().await.expect("signal stays live") {
                SessionOutput::Signal(SignalEvent::Answer { sdp, .. }) => break sdp,
                SessionOutput::Close => panic!("room closed before the answer"),
                _ => {}
            }
        }
    })
    .await
    .expect("room answers the offer");
    rtc.sdp_api()
        .accept_answer(pending, SdpAnswer::from_sdp_string(&answer).unwrap())
        .unwrap();
    assert!(
        session
            .apply(SessionRequest::Trickle {
                token: 1,
                candidate: CandidateInit {
                    candidate: format!(
                        "candidate:1 1 udp 2130706431 {MDNS_NAME} {} typ host",
                        local.port()
                    ),
                    sdp_mid: Some("0".into()),
                    sdp_mline_index: Some(0),
                    username_fragment: None,
                },
                target: SignalTarget::Publisher,
                last: false,
            })
            .await
    );
    let deadline = std::time::Instant::now() + Duration::from_secs(8);
    let mut connected = false;
    while std::time::Instant::now() < deadline {
        let now = std::time::Instant::now();
        rtc.handle_input(Input::Timeout(now)).unwrap();
        loop {
            match rtc.poll_output().unwrap() {
                Output::Transmit(p) => {
                    let _ = socket.send_to(&p.contents, p.destination).await;
                }
                Output::Event(Event::ChannelOpen(..)) | Output::Event(Event::ChannelData(..)) => {}
                Output::Timeout(_) => break,
                _ => {}
            }
        }
        while let Ok(output) = session.output.try_recv() {
            if let SessionOutput::Signal(SignalEvent::Trickle {
                candidate,
                last: false,
                ..
            }) = output
                && let Ok(candidate) = Candidate::from_sdp_string(&candidate.candidate)
            {
                rtc.add_remote_candidate(candidate);
            }
        }
        let mut buf = [0u8; 4096];
        loop {
            match socket.try_recv_from(&mut buf) {
                Ok((len, source)) => {
                    rtc.handle_input(Input::Receive(
                        now,
                        Receive::new(Protocol::Udp, source, local, &buf[..len]).unwrap(),
                    ))
                    .unwrap();
                }
                Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => break,
                Err(e) => panic!("peer socket receive: {e}"),
            }
        }
        connected = rtc.is_connected();
        if connected {
            break;
        }
        tokio::time::sleep(Duration::from_millis(2)).await;
    }
    let _ = inbox
        .send(RoomInput::Close(RoomEndReason::ServerShutdown))
        .await;
    drop(session);
    net.shutdown_and_wait().await;
    (queries.load(Ordering::SeqCst), connected)
}

#[tokio::test]
async fn resolved_mdns_candidate_carries_real_ice_and_dtls() {
    let (queries, connected) = probe(true).await;
    assert!(queries > 0, "the server must query the remote mDNS name");
    assert!(
        connected,
        "with use_mdns=true the resolved `.local` candidate must carry ICE/DTLS"
    );
}

#[tokio::test]
async fn disabled_mdns_leaves_the_peer_unreachable() {
    let (queries, connected) = probe(false).await;
    assert_eq!(queries, 0, "use_mdns=false must not query at all");
    assert!(
        !connected,
        "with use_mdns=false a `.local`-only peer can never be reached"
    );
}

/// The differential in one run: same peer, same signalling, only the setting
/// differs.
#[tokio::test]
async fn mdns_setting_alone_decides_reachability() {
    let (off_queries, off) = probe(false).await;
    let (on_queries, on) = probe(true).await;
    assert_eq!(off_queries, 0);
    assert!(on_queries > 0);
    assert!(!off, "use_mdns=false must leave the peer unreachable");
    assert!(on, "use_mdns=true must reach the peer");
}

/// Query counting for the disabled path: the responder sees exactly the
/// queries the room issues, so zero queries with the flag off is observable
/// rather than inferred.
#[tokio::test]
async fn disabled_policy_issues_no_query_through_the_real_task() {
    let (responder_address, queries) = mdns::test_support::silent_spy().await;
    let mut config = super::tests::test_config();
    config.rtc.use_mdns = false;
    let cfg = Arc::new(config);
    let net = NetRouter::start(NetConfig {
        udp_enabled: false,
        udp_port: None,
        port_range: (0, 0),
        tcp_port: None,
        interfaces: vec![LocalInterface {
            bind: Ipv4Addr::LOCALHOST.into(),
            advertise: Ipv4Addr::LOCALHOST.into(),
        }],
    })
    .await
    .unwrap();
    let now = Instant::now();
    let room = Room::new(
        RoomKey::new(ProjectId::local(), "mdns-spy"),
        Arc::new(rtc_core::config::RoomPolicy::from(cfg.as_ref())),
        Arc::new(DropData),
        &RoomConfig::default(),
        now,
    );
    let snapshot = Arc::new(Mutex::new(RoomSnapshot {
        info: room.info(),
        participants: 0,
        tracks_in: 0,
        tracks_out: 0,
        bytes_in: 0,
        bytes_out: 0,
        packets_in: 0,
        packets_out: 0,
        nacks: 0,
        retransmit_bytes_out: 0,
        retransmit_packets_out: 0,
        estimated_capacity_bps: 0,
        allocated_bps: 0,
        allocation_overshoot_bps: 0,
        paused_video_tracks: 0,
        probe_attempts: 0,
        probe_successes: 0,
        probe_failures: 0,
        congestion_events: 0,
    }));
    let (tx, rx) = mpsc::channel(ROOM_INBOX);
    let mut task = RoomTask::new(
        room,
        cfg.as_ref(),
        net.clone(),
        Arc::new(NullSink),
        snapshot,
        tx,
        rx,
    )
    .unwrap();
    task.mdns_targets = vec![responder_address];
    let handle = {
        let (reply, joined) = oneshot::channel();
        task.on_input(RoomInput::Join {
            session_started_at: tokio::time::Instant::now().into_std(),
            params: Box::new(full_ice_params("mdns-spy")),
            reply,
        });
        joined.await.unwrap().unwrap()
    };
    task.on_input(RoomInput::Request {
        sid: handle.participant_sid.clone(),
        generation: handle.generation,
        request: SessionRequest::Trickle {
            token: 2,
            candidate: CandidateInit {
                candidate: format!("candidate:2 1 udp 2130706431 {MDNS_NAME} 9 typ host"),
                sdp_mid: Some("0".into()),
                sdp_mline_index: Some(0),
                username_fragment: None,
            },
            target: SignalTarget::Publisher,
            last: false,
        },
    });
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert_eq!(
        queries.load(Ordering::SeqCst),
        0,
        "use_mdns=false must not query, even with a live task"
    );
    net.shutdown();
}
