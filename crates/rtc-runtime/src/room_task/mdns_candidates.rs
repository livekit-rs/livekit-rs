//! `rtc.use_mdns` at the room boundary: remote `.local`/`.invalid` candidates
//! are filtered on the way in (`pkg/rtc/transport.go:2494`/`:2566`) and, with
//! the setting on, resolved and re-added as ordinary trickle candidates
//! (`livekit-ice/agent.go:1048-1068`).
use super::*;
use crate::net::{LocalInterface, NetConfig, mdns};
use rtc_core::data::{DataHandler, DataSender, DataVerdict};
use rtc_core::events::NullSink;
use rtc_core::permission::{Grants, RoomConfig, VideoGrant};
use rtc_core::session::{ClientCapabilities, ClientMeta, SdkKind};
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::atomic::Ordering;

struct DropData;
impl DataHandler for DropData {
    fn inbound(&self, _: &DataSender<'_>, _: DataKind, _: &str, _: &[u8]) -> DataVerdict {
        DataVerdict::Drop
    }
}

fn params(name: &str) -> JoinParams {
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
            ice_lite: true,
            unpublish: true,
            fast_start: true,
            request_response: true,
            ..Default::default()
        },
        auto_subscribe: true,
        auto_subscribe_data_track: false,
        adaptive_stream: false,
        single_pc: true,
        disable_ice_lite: false,
        subscriber_allow_pause: None,
        reconnect: false,
        reconnect_reason: 0,
        participant_sid: None,
        add_tracks: vec![],
        publisher_offer: None,
    }
}

/// A real `RoomTask` whose `use_mdns` comes from the same normalized config the
/// binary uses, plus the inbox the mDNS resolver injects into.
async fn task(use_mdns: bool) -> (RoomTask, NetRouter) {
    let mut config = super::tests::test_config();
    config.rtc.use_mdns = use_mdns;
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
    let task = RoomTask::new(
        room,
        cfg.as_ref(),
        net.clone(),
        Arc::new(NullSink),
        snapshot,
        tx,
        rx,
    )
    .unwrap();
    (task, net)
}

async fn join(task: &mut RoomTask, name: &str) -> SessionHandle {
    let (reply, result) = oneshot::channel();
    task.on_input(RoomInput::Join {
        session_started_at: tokio::time::Instant::now().into_std(),
        params: Box::new(params(name)),
        reply,
    });
    result.await.unwrap().unwrap()
}

fn candidate(line: &str, mid: Option<&str>, mline: Option<u32>) -> CandidateInit {
    CandidateInit {
        candidate: line.to_string(),
        sdp_mid: mid.map(str::to_string),
        sdp_mline_index: mline,
        username_fragment: None,
    }
}

fn offer(sdp: &str) -> SessionRequest {
    SessionRequest::Offer {
        sdp: sdp.to_string(),
        id: 1,
    }
}

/// Next request the resolver injected into the room inbox.
async fn injected(task: &mut RoomTask) -> RoomInput {
    tokio::time::timeout(Duration::from_secs(3), task.inbox.recv())
        .await
        .expect("mDNS resolution must re-inject the candidate")
        .expect("room inbox stays open")
}

#[tokio::test]
async fn disabled_policy_drops_mdns_candidates_and_sends_no_query() {
    let (mut task, net) = task(false).await;
    let (spy, queries) = mdns::test_support::silent_spy().await;
    task.mdns_targets = vec![spy];
    let handle = join(&mut task, "mdns-off").await;
    for address in ["abcd-1234.local", "abcd-1234.invalid"] {
        task.on_input(RoomInput::Request {
            sid: handle.participant_sid.clone(),
            generation: handle.generation,
            request: SessionRequest::Trickle {
                token: 0,
                candidate: candidate(
                    &format!("candidate:2 1 udp 2130706431 {address} 5001 typ host"),
                    Some("0"),
                    Some(0),
                ),
                target: SignalTarget::Publisher,
                last: false,
            },
        });
    }
    tokio::time::sleep(Duration::from_millis(150)).await;
    assert_eq!(
        queries.load(Ordering::SeqCst),
        0,
        "use_mdns=false must not query mDNS"
    );
    assert!(
        task.inbox.try_recv().is_err(),
        "use_mdns=false must not re-add the candidate"
    );
    net.shutdown();
}

#[tokio::test]
async fn accept_policy_resolves_and_reinjects_trickled_mdns_candidate() {
    let (mut task, net) = task(true).await;
    let ip = IpAddr::V4(Ipv4Addr::new(192, 0, 2, 42));
    let responder = mdns::test_support::responder("abcd-1234.local", ip).await;
    task.mdns_targets = vec![responder];
    let handle = join(&mut task, "mdns-on").await;
    task.on_input(RoomInput::Request {
        sid: handle.participant_sid.clone(),
        generation: handle.generation,
        request: SessionRequest::Trickle {
            token: 0,
            candidate: candidate(
                "candidate:2 1 udp 2130706431 abcd-1234.local 5001 typ host",
                Some("0"),
                Some(0),
            ),
            target: SignalTarget::Publisher,
            last: false,
        },
    });
    let RoomInput::Request {
        sid,
        generation,
        request,
    } = injected(&mut task).await
    else {
        panic!("resolved candidate must come back as a room request")
    };
    assert_eq!(sid, handle.participant_sid);
    assert_eq!(generation, handle.generation);
    let SessionRequest::Trickle {
        candidate,
        target,
        last,
        ..
    } = request
    else {
        panic!("resolved candidate must be trickled like pion does")
    };
    assert_eq!(
        candidate.candidate,
        "candidate:2 1 udp 2130706431 192.0.2.42 5001 typ host"
    );
    assert_eq!(candidate.sdp_mid.as_deref(), Some("0"));
    assert_eq!(candidate.sdp_mline_index, Some(0));
    assert_eq!(target, SignalTarget::Publisher);
    assert!(!last);
    net.shutdown();
}

#[tokio::test]
async fn accept_policy_never_queries_dot_invalid_candidates() {
    let (mut task, net) = task(true).await;
    let (spy, queries) = mdns::test_support::silent_spy().await;
    task.mdns_targets = vec![spy];
    let handle = join(&mut task, "mdns-invalid").await;
    task.on_input(RoomInput::Request {
        sid: handle.participant_sid.clone(),
        generation: handle.generation,
        request: SessionRequest::Trickle {
            token: 0,
            candidate: candidate(
                "candidate:3 1 udp 2130706431 abcd-1234.invalid 5002 typ host",
                Some("0"),
                Some(0),
            ),
            target: SignalTarget::Publisher,
            last: false,
        },
    });
    tokio::time::sleep(Duration::from_millis(150)).await;
    assert_eq!(
        queries.load(Ordering::SeqCst),
        0,
        "`.invalid` names are never resolvable, so they must not be queried"
    );
    assert!(task.inbox.try_recv().is_err());
    net.shutdown();
}

#[tokio::test]
async fn sdp_mdns_candidates_are_stripped_and_resolved_per_media_section() {
    let (mut task, net) = task(true).await;
    let ip = IpAddr::V4(Ipv4Addr::new(192, 0, 2, 43));
    let responder = mdns::test_support::responder("abcd-1234.local", ip).await;
    task.mdns_targets = vec![responder];
    let handle = join(&mut task, "mdns-sdp").await;
    let sdp = "v=0\r\n\
               m=audio 9 UDP/TLS/RTP/SAVPF 111\r\n\
               a=mid:0\r\n\
               a=candidate:2 1 udp 2130706431 abcd-1234.local 5001 typ host\r\n\
               a=candidate:4 1 udp 2130706431 192.0.2.10 5003 typ host\r\n\
               m=video 9 UDP/TLS/RTP/SAVPF 96\r\n\
               a=mid:1\r\n\
               a=candidate:5 1 udp 2130706431 abcd-1234.local 5002 typ host\r\n";
    let filtered = task
        .mdns_filter(&handle.participant_sid, handle.generation, offer(sdp))
        .expect("a description always reaches the room");
    let SessionRequest::Offer { sdp: kept, id } = filtered else {
        panic!("offers stay offers")
    };
    assert_eq!(id, 1);
    assert!(!kept.contains(".local"), "mDNS lines are handled here");
    assert!(kept.contains("a=candidate:4 1 udp 2130706431 192.0.2.10 5003 typ host"));
    assert!(kept.contains("a=mid:0") && kept.contains("a=mid:1"));
    let mut resolved = Vec::new();
    for _ in 0..2 {
        let RoomInput::Request {
            request: SessionRequest::Trickle {
                candidate, target, ..
            },
            ..
        } = injected(&mut task).await
        else {
            panic!("each mDNS candidate is re-trickled")
        };
        assert_eq!(target, SignalTarget::Publisher);
        assert!(candidate.candidate.contains("192.0.2.43"));
        resolved.push((candidate.sdp_mid, candidate.sdp_mline_index));
    }
    assert_eq!(
        resolved,
        vec![
            (Some("0".to_string()), Some(0)),
            (Some("1".to_string()), Some(1))
        ],
        "each candidate keeps the media section it came from"
    );
    net.shutdown();
}

#[tokio::test]
async fn answer_mdns_candidates_belong_to_the_subscriber_transport() {
    let (mut task, net) = task(true).await;
    let ip = IpAddr::V4(Ipv4Addr::new(192, 0, 2, 44));
    let responder = mdns::test_support::responder("efgh-5678.local", ip).await;
    task.mdns_targets = vec![responder];
    let handle = join(&mut task, "mdns-answer").await;
    let sdp = "v=0\r\n\
               m=audio 9 UDP/TLS/RTP/SAVPF 111\r\n\
               a=mid:0\r\n\
               a=candidate:6 1 udp 2130706431 efgh-5678.local 5004 typ host\r\n";
    let filtered = task.mdns_filter(
        &handle.participant_sid,
        handle.generation,
        SessionRequest::Answer {
            sdp: sdp.to_string(),
            id: 2,
        },
    );
    let Some(SessionRequest::Answer { sdp: kept, id }) = filtered else {
        panic!("answers stay answers")
    };
    assert_eq!(id, 2);
    assert!(!kept.contains(".local"));
    let RoomInput::Request {
        request: SessionRequest::Trickle {
            candidate, target, ..
        },
        ..
    } = injected(&mut task).await
    else {
        panic!("the answer candidate is re-trickled")
    };
    assert_eq!(target, SignalTarget::Subscriber);
    assert_eq!(
        candidate.candidate,
        "candidate:6 1 udp 2130706431 192.0.2.44 5004 typ host"
    );
    net.shutdown();
}

#[tokio::test]
async fn ordinary_candidates_and_uppercase_suffixes_stay_on_the_direct_path() {
    let (mut task, net) = task(true).await;
    let (spy, queries) = mdns::test_support::silent_spy().await;
    task.mdns_targets = vec![spy];
    let handle = join(&mut task, "mdns-plain").await;
    for line in [
        "candidate:7 1 udp 2130706431 192.0.2.10 5005 typ host",
        // Go's strings.HasSuffix is case sensitive: `.LOCAL` is not an mDNS
        // candidate and is forwarded untouched.
        "candidate:8 1 udp 2130706431 abcd.local.LOCAL 5006 typ host",
    ] {
        let filtered = task.mdns_filter(
            &handle.participant_sid,
            handle.generation,
            SessionRequest::Trickle {
                token: 9,
                candidate: candidate(line, Some("0"), Some(0)),
                target: SignalTarget::Publisher,
                last: false,
            },
        );
        let Some(SessionRequest::Trickle {
            token,
            candidate,
            target,
            ..
        }) = filtered
        else {
            panic!("non-mDNS candidates reach the room directly")
        };
        assert_eq!(token, 9);
        assert_eq!(candidate.candidate, line);
        assert_eq!(target, SignalTarget::Publisher);
    }
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert_eq!(queries.load(Ordering::SeqCst), 0);
    assert!(task.inbox.try_recv().is_err());
    net.shutdown();
}

#[tokio::test]
async fn description_without_mdns_candidates_is_forwarded_byte_identical() {
    let (mut task, net) = task(true).await;
    let handle = join(&mut task, "mdns-plain-sdp").await;
    let sdp = "v=0\r\nm=audio 9 UDP/TLS/RTP/SAVPF 111\r\na=mid:0\r\n\
               a=candidate:9 1 udp 2130706431 192.0.2.10 5007 typ host\r\n";
    let filtered = task.mdns_filter(&handle.participant_sid, handle.generation, offer(sdp));
    let Some(SessionRequest::Offer { sdp: kept, .. }) = filtered else {
        panic!("offers stay offers")
    };
    assert_eq!(kept, sdp);
    net.shutdown();
}

/// Production wiring: the mDNS groups are what a real task queries, so a
/// regression that replaces them with test targets or drops the setting is a
/// compile-time or assertion-time failure.
#[test]
fn production_targets_are_the_standard_mdns_groups() {
    let groups: Vec<SocketAddr> = mdns::GROUPS.to_vec();
    assert_eq!(
        groups,
        vec![
            SocketAddr::new(IpAddr::V4(Ipv4Addr::new(224, 0, 0, 251)), 5353),
            "[ff02::fb]:5353".parse().unwrap(),
        ]
    );
    assert_eq!(MdnsPolicy::from_config(false), MdnsPolicy::Disabled);
    assert_eq!(MdnsPolicy::from_config(true), MdnsPolicy::Accept);
}
