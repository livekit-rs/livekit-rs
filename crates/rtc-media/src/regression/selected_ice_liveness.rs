//! Actual parsed SDP, authenticated STUN and DTLS exchange followed by real
//! selected-path loss. No hand-promoted ICE state or injected timestamp/stat.
use super::*;
use crate::net::Datagram;
use str0m::net::{Protocol, Receive};
use str0m::{Input, Output};
fn connect_selected(
    start: Instant,
) -> (
    MediaEngine,
    Rtc,
    TransportId,
    PacketQueue,
    std::net::SocketAddr,
    std::net::SocketAddr,
    Instant,
) {
    let mut engine = engine(start);
    let queue = PacketQueue::default();
    engine.out = Box::new(queue.clone());
    let tid = transport(&mut engine, PcRole::Publisher, start);
    let server = "127.0.0.1:20001".parse().unwrap();
    let peer_addr = "127.0.0.1:20002".parse().unwrap();
    let candidate = engine
        .transports
        .get_mut(&tid)
        .unwrap()
        .rtc
        .add_local_candidate(str0m::Candidate::host(server, "udp").unwrap())
        .unwrap()
        .clone();
    engine
        .transports
        .get_mut(&tid)
        .unwrap()
        .candidates
        .push(candidate);
    let mut peer = client(start);
    peer.add_local_candidate(str0m::Candidate::host(peer_addr, "udp").unwrap());
    offer(&mut engine, &mut peer, tid, start, |change| {
        change.add_channel("_reliable".into());
    });
    for step in 0..1000 {
        let now = start + Duration::from_millis(step * 10);
        peer.handle_input(Input::Timeout(now)).unwrap();
        exchange(&mut engine, &mut peer, tid, &queue, server, peer_addr, now);
        engine.handle_timeout(now);
        exchange(&mut engine, &mut peer, tid, &queue, server, peer_addr, now);
        if peer.is_connected()
            && engine.transports[&tid].rtc.is_connected()
            && engine.transports[&tid].ice_first_connected_at.is_some()
        {
            assert!(
                engine.transports[&tid]
                    .rtc
                    .ice_last_selected_receive()
                    .is_some()
            );
            return (engine, peer, tid, queue, server, peer_addr, now);
        }
    }
    panic!("actual authenticated ICE+DTLS did not connect");
}
fn exchange(
    engine: &mut MediaEngine,
    peer: &mut Rtc,
    tid: TransportId,
    queue: &PacketQueue,
    server: std::net::SocketAddr,
    peer_addr: std::net::SocketAddr,
    now: Instant,
) {
    for _ in 0..1000 {
        loop {
            match peer.poll_output().unwrap() {
                Output::Transmit(packet) => engine.handle_datagram(Datagram {
                    transport: tid,
                    proto: Proto::Udp,
                    source: peer_addr,
                    destination: server,
                    data: &packet.contents,
                    received_at: now,
                }),
                Output::Event(_) => {}
                Output::Timeout(_) => break,
            }
        }
        // Drain after EACH native input, preserving sans-I/O pending output ownership.
        let packet = queue.0.lock().unwrap().pop_front();
        if let Some(bytes) = packet {
            peer.handle_input(Input::Receive(
                now,
                Receive::new(Protocol::Udp, server, peer_addr, &bytes).unwrap(),
            ))
            .unwrap();
        } else {
            return;
        }
    }
    panic!("bounded actual handshake output did not quiesce");
}
#[test]
fn actual_selected_pair_disconnects_strictly_after_ten_and_fails_strictly_after_fifteen_seconds() {
    let start = Instant::now();
    let (mut engine, _peer, tid, _queue, _server, _peer_addr, _now) = connect_selected(start);
    let last = engine.transports[&tid]
        .rtc
        .ice_last_selected_receive()
        .unwrap();
    let mut discard = vec![];
    engine.drain_events(&mut discard);
    engine.handle_timeout(last + Duration::from_secs(10));
    assert!(!engine.transports[&tid].failed);
    assert_eq!(engine.transports[&tid].disconnected_since, None);
    engine.handle_timeout(last + Duration::from_secs(10) + Duration::from_nanos(1));
    assert!(!engine.transports[&tid].failed);
    assert_eq!(
        engine.transports[&tid].disconnected_since,
        Some(last + Duration::from_secs(10))
    );
    engine.handle_timeout(last + Duration::from_secs(15));
    assert!(
        !engine.transports[&tid].failed,
        "fixed Go strict greater-than threshold"
    );
    engine.handle_timeout(last + Duration::from_secs(15) + Duration::from_nanos(1));
    assert!(engine.transports[&tid].failed);
    let mut events = vec![];
    engine.drain_events(&mut events);
    assert_eq!(
        events
            .iter()
            .filter(|e| matches!(e,MediaEvent::TransportFailed{transport,..}if *transport==tid))
            .count(),
        1
    );
    engine.handle_timeout(last + Duration::from_secs(16));
    let mut again = vec![];
    engine.drain_events(&mut again);
    assert!(
        !again
            .iter()
            .any(|e| matches!(e, MediaEvent::TransportFailed { .. })),
        "one failure per physical generation"
    );
    engine.handle_command(
        rtc_core::media_api::MediaCommand::CloseTransport(tid),
        last + Duration::from_secs(16),
    );
    assert!(engine.transports.is_empty());
}
#[test]
fn real_selected_stun_refresh_cancels_pending_failure_without_fake_rtp_or_rtt() {
    let start = Instant::now();
    let (mut engine, mut peer, tid, queue, server, peer_addr, _now) = connect_selected(start);
    let last = engine.transports[&tid]
        .rtc
        .ice_last_selected_receive()
        .unwrap();
    let at = last + Duration::from_secs(10) + Duration::from_nanos(1);
    engine.handle_timeout(at);
    assert!(engine.transports[&tid].disconnected_since.is_some());
    peer.handle_input(Input::Timeout(at)).unwrap();
    exchange(&mut engine, &mut peer, tid, &queue, server, peer_addr, at);
    let refreshed = engine.transports[&tid]
        .rtc
        .ice_last_selected_receive()
        .unwrap();
    assert!(
        refreshed > last,
        "actual authenticated keepalive must advance selected receipt"
    );
    engine.run(at);
    assert!(!engine.transports[&tid].failed);
    assert_eq!(engine.transports[&tid].disconnected_since, None);
    engine.handle_timeout(last + Duration::from_secs(15) + Duration::from_nanos(1));
    assert!(
        !engine.transports[&tid].failed,
        "old receipt's deadline cannot fail refreshed path"
    );
    engine.handle_command(
        rtc_core::media_api::MediaCommand::CloseTransport(tid),
        at + Duration::from_secs(6),
    );
    assert!(engine.transports.is_empty());
}

#[test]
fn actual_new_ice_credentials_clear_old_received_deadline_without_replacing_transport() {
    let start = Instant::now();
    let (mut engine, mut peer, tid, _queue, _server, _peer_addr, _now) = connect_selected(start);
    let last = engine.transports[&tid]
        .selected_ice_last_receive
        .expect("real initial selected receipt");
    let at = last + Duration::from_secs(11);
    engine.handle_timeout(at);
    assert!(engine.transports[&tid].disconnected_since.is_some());
    let (offer, _pending) = {
        let mut api = peer.sdp_api();
        api.ice_restart(true);
        api.apply().expect("real credential restart offer")
    };
    engine.on_remote_description(tid, SdpType::Offer, offer.to_sdp_string(), 77, at);
    assert_eq!(
        engine.transports[&tid].selected_ice_last_receive, None,
        "accepted new credentials retire old arrival epoch"
    );
    assert_eq!(engine.transports[&tid].disconnected_since, None);
    assert!(!engine.transports[&tid].failed);
    engine.handle_timeout(last + Duration::from_secs(16));
    assert!(
        !engine.transports[&tid].failed,
        "old selected pair deadline cannot fail new unselected ICE generation"
    );
    engine.handle_command(
        rtc_core::media_api::MediaCommand::CloseTransport(tid),
        last + Duration::from_secs(16),
    );
    assert!(engine.transports.is_empty());
}

fn connect_selected_subscriber(
    start: Instant,
) -> (
    MediaEngine,
    Rtc,
    TransportId,
    PacketQueue,
    std::net::SocketAddr,
    std::net::SocketAddr,
    Instant,
) {
    let mut engine = engine(start);
    let queue = PacketQueue::default();
    engine.out = Box::new(queue.clone());
    let tid = TransportId::next();
    let server = "127.0.0.1:20101".parse().unwrap();
    let peer_addr = "127.0.0.1:20102".parse().unwrap();
    engine.create_transport(
        TransportSpec {
            one_shot: false,
            id: tid,
            participant: "PA_pending_subscriber".into(),
            role: PcRole::Subscriber,
            ice_lite: true,
            server_data_channels: true,
            enable_data_tracks: false,
            primary: true,
            client_supports_red: false,
            codec_switch_requires_renegotiation: false,
            disable_playout_delay: false,
            allow_pause: true,
            ice_preference: rtc_core::transport_fallback::IcePreference::Any,
        },
        IceCredentials::random(),
        vec![crate::net::LocalCandidate {
            proto: Proto::Udp,
            addr: server,
            base: server,
        }],
        start,
    );
    let mut peer = client(start);
    peer.add_local_candidate(str0m::Candidate::host(peer_addr, "udp").unwrap());
    answer_server_offer(&mut engine, &mut peer, tid, start);
    for step in 0..1000 {
        let now = start + Duration::from_millis(step * 10);
        peer.handle_input(Input::Timeout(now)).unwrap();
        exchange(&mut engine, &mut peer, tid, &queue, server, peer_addr, now);
        engine.handle_timeout(now);
        exchange(&mut engine, &mut peer, tid, &queue, server, peer_addr, now);
        if peer.is_connected()
            && engine.transports[&tid].rtc.is_connected()
            && engine.transports[&tid]
                .channel_for(DataKind::Reliable)
                .is_some()
        {
            assert!(engine.transports[&tid].selected_ice_last_receive.is_some());
            return (engine, peer, tid, queue, server, peer_addr, now);
        }
    }
    panic!("actual dual-PC subscriber ICE+DTLS+SCTP not connected");
}
fn begin_pending_restart(engine: &mut MediaEngine, tid: TransportId, at: Instant) -> (u32, String) {
    let mut old = vec![];
    engine.drain_events(&mut old);
    engine.handle_command(
        rtc_core::media_api::MediaCommand::IceRestart { transport: tid },
        at,
    );
    assert!(
        engine.transports[&tid].pending_restart_ufrag.is_some(),
        "real native pending local credential offer"
    );
    assert_eq!(engine.transports[&tid].selected_ice_last_receive, None);
    assert_eq!(engine.transports[&tid].disconnected_since, None);
    let mut events = vec![];
    engine.drain_events(&mut events);
    events
        .into_iter()
        .find_map(|event| match event {
            MediaEvent::LocalDescription {
                kind: SdpType::Offer,
                id,
                sdp,
                ..
            } => Some((id, sdp)),
            _ => None,
        })
        .expect("actual restart offer on signal output")
}
#[test]
fn pending_local_subscriber_offer_fences_old_pair_keepalive_until_real_answer_and_new_stun() {
    let start = Instant::now();
    let (mut engine, mut peer, tid, queue, server, peer_addr, _now) =
        connect_selected_subscriber(start);
    let last = engine.transports[&tid].selected_ice_last_receive.unwrap();
    let at = last + Duration::from_secs(11);
    engine.handle_timeout(at);
    let (id, sdp) = begin_pending_restart(&mut engine, tid, at);
    // Native still owns the OLD pair until accept_answer applies new credentials.
    assert!(
        engine.transports[&tid]
            .rtc
            .ice_last_selected_receive()
            .is_some()
    );
    let stale_at = at + Duration::from_millis(1);
    peer.handle_input(Input::Timeout(stale_at)).unwrap();
    exchange(
        &mut engine,
        &mut peer,
        tid,
        &queue,
        server,
        peer_addr,
        stale_at,
    );
    assert!(
        engine.transports[&tid]
            .rtc
            .ice_last_selected_receive()
            .is_some_and(|received| received > last),
        "actual old-credential authenticated keepalive reached native"
    );
    assert_eq!(
        engine.transports[&tid].selected_ice_last_receive, None,
        "old pair cannot initialize the pending credential epoch"
    );
    engine.handle_timeout(last + Duration::from_secs(15) + Duration::from_nanos(1));
    let mut premature = vec![];
    engine.drain_events(&mut premature);
    assert!(
        !premature
            .iter()
            .any(|event| matches!(event, MediaEvent::TransportFailed { .. })),
        "no second Resume from old pair deadline during pending answer"
    );
    let answer_at = last + Duration::from_secs(16);
    let answer = peer
        .sdp_api()
        .accept_offer(str0m::change::SdpOffer::from_sdp_string(&sdp).unwrap())
        .unwrap();
    engine.on_remote_description(tid, SdpType::Answer, answer.to_sdp_string(), id, answer_at);
    assert!(engine.transports[&tid].pending_restart_ufrag.is_none());
    assert_eq!(
        engine.transports[&tid].selected_ice_last_receive, None,
        "answer alone is not a fabricated new receipt"
    );
    let mut restored = None;
    for step in 1..500 {
        let now = answer_at + Duration::from_millis(step * 10);
        peer.handle_input(Input::Timeout(now)).unwrap();
        exchange(&mut engine, &mut peer, tid, &queue, server, peer_addr, now);
        engine.handle_timeout(now);
        exchange(&mut engine, &mut peer, tid, &queue, server, peer_addr, now);
        if engine.transports[&tid]
            .selected_ice_last_receive
            .is_some_and(|received| received >= answer_at)
        {
            restored = Some(now);
            break;
        }
    }
    let restored = restored.expect("actual new-credential STUN must restore selected receipt");
    assert!(!engine.transports[&tid].failed);
    assert_eq!(engine.transports[&tid].disconnected_since, None);
    engine.handle_command(
        rtc_core::media_api::MediaCommand::CloseTransport(tid),
        restored,
    );
    assert!(engine.transports.is_empty());
}
#[test]
fn pending_restart_retains_real_answer_deadline_and_cancellation_releases_transport() {
    for cancel in [false, true] {
        let start = Instant::now();
        let (mut engine, _peer, tid, _queue, _server, _peer_addr, now) =
            connect_selected_subscriber(start);
        let _ = begin_pending_restart(&mut engine, tid, now + Duration::from_millis(1));
        let deadline = engine.transports[&tid]
            .answer_deadline
            .expect("existing bounded answer timeout retained");
        if cancel {
            engine.handle_command(
                rtc_core::media_api::MediaCommand::CloseTransport(tid),
                now + Duration::from_millis(2),
            );
            assert!(engine.transports.is_empty());
            engine.handle_timeout(deadline);
            assert!(engine.transports.is_empty());
        } else {
            engine.handle_timeout(deadline);
            let mut events = vec![];
            engine.drain_events(&mut events);
            assert!(
                events
                    .iter()
                    .any(|event| matches!(event, MediaEvent::NegotiationFailed { .. })),
                "epoch fence cannot indefinitely exempt pending answer from source negotiation timeout"
            );
            engine.handle_command(
                rtc_core::media_api::MediaCommand::CloseTransport(tid),
                deadline,
            );
            assert!(engine.transports.is_empty());
        }
    }
}

#[test]
fn dual_pc_recovery_fences_publisher_old_pair_and_requires_both_real_restart_exchanges() {
    use rtc_core::media_api::MediaCommand;
    let start = Instant::now();
    let (mut engine, mut publisher, pub_id, pub_queue, pub_addr, pub_peer, pub_at) =
        connect_selected(start);
    engine.transports.get_mut(&pub_id).unwrap().owned_out = Some(Box::new(pub_queue.clone()));
    let sub_queue = PacketQueue::default();
    engine.out = Box::new(sub_queue.clone());
    let sub_id = TransportId::next();
    let sub_addr = "127.0.0.1:20101".parse().unwrap();
    let sub_peer = "127.0.0.1:20102".parse().unwrap();
    let participant = engine.transports[&pub_id].spec.participant.clone();
    engine.create_transport(
        TransportSpec {
            id: sub_id,
            participant: participant.clone(),
            role: PcRole::Subscriber,
            one_shot: false,
            ice_lite: true,
            server_data_channels: true,
            enable_data_tracks: false,
            primary: true,
            client_supports_red: false,
            codec_switch_requires_renegotiation: false,
            disable_playout_delay: false,
            allow_pause: true,
            ice_preference: Default::default(),
        },
        IceCredentials::random(),
        vec![crate::net::LocalCandidate {
            proto: Proto::Udp,
            addr: sub_addr,
            base: sub_addr,
        }],
        pub_at,
    );
    let mut subscriber = client(pub_at);
    subscriber.add_local_candidate(str0m::Candidate::host(sub_peer, "udp").unwrap());
    answer_server_offer(&mut engine, &mut subscriber, sub_id, pub_at);
    let mut connected = None;
    for step in 1..1000 {
        let now = pub_at + Duration::from_millis(step * 10);
        for (peer, tid, queue, addr, remote) in [
            (&mut publisher, pub_id, &pub_queue, pub_addr, pub_peer),
            (&mut subscriber, sub_id, &sub_queue, sub_addr, sub_peer),
        ] {
            peer.handle_input(Input::Timeout(now)).unwrap();
            exchange(&mut engine, peer, tid, queue, addr, remote, now);
        }
        engine.handle_timeout(now);
        if publisher.is_connected()
            && subscriber.is_connected()
            && [pub_id, sub_id].iter().all(|id| {
                engine.transports[id].rtc.is_connected()
                    && engine.transports[id].selected_ice_last_receive.is_some()
            })
        {
            connected = Some(now);
            break;
        }
    }
    let connected = connected.expect("both real SDP/STUN/DTLS PCs connected");
    let first_connected = [pub_id, sub_id].map(|id| engine.transports[&id].ice_first_connected_at);
    let mut discarded = vec![];
    engine.drain_events(&mut discarded);
    // Both old PCs physically lose authenticated consent. These events may be
    // queued across the actor's real signal resume; their epoch remains zero.
    let failed_at = connected + Duration::from_secs(16);
    engine.handle_timeout(failed_at);
    let mut old_failed = vec![];
    engine.drain_events(&mut old_failed);
    for tid in [pub_id, sub_id] {
        assert!(old_failed.iter().any(|e| matches!(e, MediaEvent::TransportFailed { transport, recovery_epoch: 0, .. } if *transport == tid)));
    }
    let resumed_at = failed_at + Duration::from_millis(1);
    engine.handle_command(
        MediaCommand::BeginTransportRecovery {
            participant,
            recovery_epoch: 1,
        },
        resumed_at,
    );
    let (answer_id, server_offer) = begin_pending_restart(&mut engine, sub_id, resumed_at);
    // Old publisher traffic is truly authenticated by native. It cannot end
    // recovery before a new client offer has actually changed credentials.
    let stale_at = resumed_at + Duration::from_millis(2);
    publisher.handle_input(Input::Timeout(stale_at)).unwrap();
    exchange(
        &mut engine,
        &mut publisher,
        pub_id,
        &pub_queue,
        pub_addr,
        pub_peer,
        stale_at,
    );
    engine.handle_timeout(stale_at);
    assert!(engine.transports[&pub_id].recovery_pending.is_some());
    assert_eq!(engine.transports[&pub_id].selected_ice_last_receive, None);
    let mut stale_events = vec![];
    engine.drain_events(&mut stale_events);
    assert!(
        !stale_events
            .iter()
            .any(|e| matches!(e, MediaEvent::TransportFailed { .. })),
        "old publisher PC cannot generate a second current-epoch Resume while waiting for real client offer"
    );
    let restart_at = resumed_at + Duration::from_millis(10);
    offer(&mut engine, &mut publisher, pub_id, restart_at, |change| {
        change.ice_restart(true);
    });
    assert!(
        engine.transports[&pub_id]
            .recovery_pending
            .unwrap()
            .sdp_applied
    );
    assert_eq!(
        engine.transports[&pub_id].selected_ice_last_receive, None,
        "new offer alone is not new STUN"
    );
    let answer = subscriber
        .sdp_api()
        .accept_offer(str0m::change::SdpOffer::from_sdp_string(&server_offer).unwrap())
        .unwrap();
    engine.on_remote_description(
        sub_id,
        SdpType::Answer,
        answer.to_sdp_string(),
        answer_id,
        restart_at,
    );
    assert!(
        engine.transports[&sub_id]
            .recovery_pending
            .unwrap()
            .sdp_applied
    );
    let mut restored = None;
    for step in 1..1000 {
        let now = restart_at + Duration::from_millis(step * 10);
        for (peer, tid, queue, addr, remote) in [
            (&mut publisher, pub_id, &pub_queue, pub_addr, pub_peer),
            (&mut subscriber, sub_id, &sub_queue, sub_addr, sub_peer),
        ] {
            peer.handle_input(Input::Timeout(now)).unwrap();
            exchange(&mut engine, peer, tid, queue, addr, remote, now);
        }
        engine.handle_timeout(now);
        if [pub_id, sub_id].iter().all(|id| {
            engine.transports[id].recovery_pending.is_none()
                && !engine.transports[id].failed
                && engine.transports[id]
                    .selected_ice_last_receive
                    .is_some_and(|last| last >= restart_at)
        }) {
            restored = Some(now);
            break;
        }
    }
    let restored = restored.expect("both new authenticated STUN generations restore real PCs");
    assert_eq!(
        [pub_id, sub_id].map(|id| engine.transports[&id].ice_first_connected_at),
        first_connected,
        "90s first connection clock retained"
    );
    let mut events = vec![];
    engine.drain_events(&mut events);
    assert!(
        !events
            .iter()
            .any(|e| matches!(e, MediaEvent::TransportFailed { .. })),
        "no spurious second failure during actual paired recovery"
    );
    for tid in [pub_id, sub_id] {
        engine.handle_command(MediaCommand::CloseTransport(tid), restored);
    }
    assert!(engine.transports.is_empty());
}

#[test]
fn publisher_recovery_without_new_offer_is_bounded_and_cancellation_releases_it() {
    use rtc_core::media_api::MediaCommand;
    for cancel in [false, true] {
        let start = Instant::now();
        let (mut engine, _peer, tid, _queue, _addr, _remote, now) = connect_selected(start);
        let failed_at = now + Duration::from_secs(16);
        engine.handle_timeout(failed_at);
        assert!(
            engine.transports[&tid].failed,
            "missing-offer timeout applies to an actually failed publisher, not signal-only reconnect"
        );
        let now = failed_at + Duration::from_millis(1);
        let participant = engine.transports[&tid].spec.participant.clone();
        engine.handle_command(
            MediaCommand::BeginTransportRecovery {
                participant,
                recovery_epoch: 1,
            },
            now,
        );
        let deadline = engine.transports[&tid].recovery_pending.unwrap().deadline;
        let mut before = vec![];
        engine.drain_events(&mut before);
        if cancel {
            engine.handle_command(
                MediaCommand::CloseTransport(tid),
                now + Duration::from_millis(1),
            );
            engine.handle_timeout(deadline);
            assert!(engine.transports.is_empty());
        } else {
            engine.handle_timeout(deadline - Duration::from_nanos(1));
            let mut events = vec![];
            engine.drain_events(&mut events);
            assert!(
                !events
                    .iter()
                    .any(|e| matches!(e, MediaEvent::TransportFailed { .. }))
            );
            engine.handle_timeout(deadline);
            engine.drain_events(&mut events);
            assert!(events.iter().any(|e| matches!(e, MediaEvent::TransportFailed { transport, recovery_epoch: 1, .. } if *transport == tid)), "no indefinite publisher-wait exemption");
            engine.handle_command(MediaCommand::CloseTransport(tid), deadline);
            assert!(engine.transports.is_empty());
        }
    }
}

#[test]
fn healthy_publisher_signal_only_resume_retains_real_pc_without_new_offer_beyond_fifteen_seconds() {
    use rtc_core::media_api::MediaCommand;
    let start = Instant::now();
    let (mut engine, mut publisher, tid, queue, server, peer_addr, now) = connect_selected(start);
    let first_connected = engine.transports[&tid].ice_first_connected_at;
    let original_remote = engine.transports[&tid].remote_offer_ice.clone();
    let participant = engine.transports[&tid].spec.participant.clone();
    let mut initial = vec![];
    engine.drain_events(&mut initial);
    engine.handle_command(
        MediaCommand::BeginTransportRecovery {
            participant,
            recovery_epoch: 1,
        },
        now,
    );
    assert!(
        engine.transports[&tid].recovery_pending.is_none(),
        "healthy publisher has no new-offer obligation on a signal-only resume"
    );
    let mut newer = None;
    for step in 1..=1700 {
        let at = now + Duration::from_millis(step * 10);
        publisher.handle_input(Input::Timeout(at)).unwrap();
        exchange(
            &mut engine,
            &mut publisher,
            tid,
            &queue,
            server,
            peer_addr,
            at,
        );
        engine.handle_timeout(at);
        exchange(
            &mut engine,
            &mut publisher,
            tid,
            &queue,
            server,
            peer_addr,
            at,
        );
        newer = engine.transports[&tid].selected_ice_last_receive;
    }
    assert!(
        newer.is_some_and(|received| received > now + Duration::from_secs(15)),
        "real authenticated keepalives continue after the former false deadline"
    );
    assert!(!engine.transports[&tid].failed);
    assert_eq!(
        engine.transports[&tid].remote_offer_ice, original_remote,
        "no fabricated or forced publisher restart SDP"
    );
    assert_eq!(
        engine.transports[&tid].ice_first_connected_at,
        first_connected
    );
    let mut events = vec![];
    engine.drain_events(&mut events);
    assert!(
        !events.iter().any(|e| matches!(
            e,
            MediaEvent::TransportFailed { .. } | MediaEvent::NegotiationFailed { .. }
        )),
        "healthy signal-only resume must not emit second Resume/full reconnect"
    );
    engine.handle_command(
        MediaCommand::CloseTransport(tid),
        now + Duration::from_secs(18),
    );
    assert!(engine.transports.is_empty());
}

#[test]
fn subscriber_only_resume_leaves_unused_publisher_idle_beyond_fifteen_seconds() {
    use rtc_core::media_api::MediaCommand;
    let start = Instant::now();
    let (mut engine, mut subscriber, sub_id, queue, server, peer_addr, now) =
        connect_selected_subscriber(start);
    let participant = engine.transports[&sub_id].spec.participant.clone();
    let unused = TransportId::next();
    engine.create_transport(
        TransportSpec {
            id: unused,
            participant: participant.clone(),
            role: PcRole::Publisher,
            one_shot: false,
            ice_lite: true,
            server_data_channels: false,
            enable_data_tracks: false,
            primary: false,
            client_supports_red: false,
            codec_switch_requires_renegotiation: false,
            disable_playout_delay: false,
            allow_pause: true,
            ice_preference: Default::default(),
        },
        IceCredentials::random(),
        vec![],
        now,
    );
    assert!(!engine.transports[&unused].remote_description_applied);
    assert!(engine.transports[&unused].ice_first_connected_at.is_none());
    let mut initial = vec![];
    engine.drain_events(&mut initial);
    engine.handle_command(
        MediaCommand::BeginTransportRecovery {
            participant,
            recovery_epoch: 1,
        },
        now,
    );
    assert!(engine.transports[&unused].recovery_pending.is_none());
    assert_eq!(
        engine.transports[&unused].recovery_epoch, 1,
        "unused transport still fences old queued events"
    );
    let (id, sdp) = begin_pending_restart(&mut engine, sub_id, now);
    let answer = subscriber
        .sdp_api()
        .accept_offer(str0m::change::SdpOffer::from_sdp_string(&sdp).unwrap())
        .unwrap();
    engine.on_remote_description(
        sub_id,
        SdpType::Answer,
        answer.to_sdp_string(),
        id,
        now + Duration::from_millis(1),
    );
    for step in 1..=1700 {
        let at = now + Duration::from_millis(step * 10);
        subscriber.handle_input(Input::Timeout(at)).unwrap();
        exchange(
            &mut engine,
            &mut subscriber,
            sub_id,
            &queue,
            server,
            peer_addr,
            at,
        );
        engine.handle_timeout(at);
        exchange(
            &mut engine,
            &mut subscriber,
            sub_id,
            &queue,
            server,
            peer_addr,
            at,
        );
    }
    assert!(engine.transports[&sub_id].rtc.is_connected());
    assert!(!engine.transports[&sub_id].failed);
    assert!(!engine.transports[&unused].remote_description_applied);
    assert!(engine.transports[&unused].ice_first_connected_at.is_none());
    assert!(!engine.transports[&unused].failed);
    let mut events = vec![];
    engine.drain_events(&mut events);
    assert!(
        !events.iter().any(|e| matches!(
            e,
            MediaEvent::TransportFailed { .. } | MediaEvent::NegotiationFailed { .. }
        )),
        "unused publisher must not impose a 15s deadline on subscriber-only resume"
    );
    for tid in [sub_id, unused] {
        engine.handle_command(
            MediaCommand::CloseTransport(tid),
            now + Duration::from_secs(18),
        );
    }
    assert!(engine.transports.is_empty());
}
