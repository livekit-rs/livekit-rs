#[test]
fn selected_liveness_uses_actual_authenticated_stun_and_tuple_lifetime_not_preference() {
    let start = Instant::now();
    let mut agent = acceptance_checked_pair(
        CandidateKind::Host,
        CandidateKind::Host,
        [Duration::ZERO; 4],
        start,
        ipv4_1(),
    );
    assert_eq!(
        agent.nominated_last_received(),
        Some(start + Duration::from_millis(1))
    );
    let original = agent.nominated_last_received();
    assert!(!agent.notify_selected_data_received(
        Protocol::Tcp,
        ipv4_3(),
        start + Duration::from_secs(2)
    ));
    assert!(!agent.notify_selected_data_received(
        Protocol::Udp,
        ipv4_4(),
        start + Duration::from_secs(2)
    ));
    assert_eq!(
        agent.nominated_last_received(),
        original,
        "unknown/protocol-mismatched data cannot refresh selected pair"
    );
    assert!(agent.notify_selected_data_received(
        Protocol::Udp,
        ipv4_3(),
        start + Duration::from_secs(3)
    ));
    assert_eq!(
        agent.nominated_last_received(),
        Some(start + Duration::from_secs(3))
    );
    assert!(agent.notify_selected_data_received(
        Protocol::Udp,
        ipv4_3(),
        start + Duration::from_secs(2)
    ));
    assert_eq!(
        agent.nominated_last_received(),
        Some(start + Duration::from_secs(3)),
        "old observations cannot move source clock backwards"
    );
    agent.ice_restart(IceCreds::new(), true);
    assert_eq!(agent.nominated_last_received(), None);
    assert!(
        !agent.notify_selected_data_received(
            Protocol::Udp,
            ipv4_3(),
            start + Duration::from_secs(4)
        ),
        "retired tuple cannot refresh unproved new generation"
    );
}

#[test]
fn authenticated_success_last_received_survives_real_outbound_attempt_eviction_and_rejects_bad_auth()
 {
    let start = Instant::now();
    let mut agent = acceptance_checked_pair(
        CandidateKind::Host,
        CandidateKind::Host,
        [Duration::ZERO; 4],
        start,
        ipv4_1(),
    );
    let proved = start + Duration::from_millis(1);
    assert_eq!(agent.nominated_last_received(), Some(proved));
    while agent.poll_transmit().is_some() {}
    let mut latest = None;
    let mut transmitted = 0;
    for step in 0..32 {
        agent.stun_client_binding_request(start + Duration::from_millis(2 + step), 0);
        while let Some(packet) = agent.poll_transmit() {
            let request = StunMessage::parse(&packet.contents).unwrap();
            assert!(request.is_binding_request());
            latest = Some(request.trans_id());
            transmitted += 1;
        }
    }
    assert_eq!(
        transmitted, 32,
        "actual native request serializer and bounded new_attempt history"
    );
    assert!(
        agent.candidate_pairs[0].last_successful_rtt().is_none(),
        "the unique successful transaction was actually evicted"
    );
    assert_eq!(
        agent.nominated_last_received(),
        Some(proved),
        "eviction cannot erase Pion Remote.LastReceived"
    );
    let tx = latest.unwrap();
    let remote = agent.remote_credentials().unwrap().clone();
    let invalid = make_authenticated_stun_reply(tx, ipv4_1(), "wrong-password");
    assert!(!agent.handle_packet(
        start + Duration::from_millis(100),
        StunPacket {
            message: StunMessage::parse(&invalid).unwrap(),
            source: ipv4_3(),
            destination: ipv4_1(),
            proto: Protocol::Udp
        }
    ));
    assert_eq!(agent.nominated_last_received(), Some(proved));
    let valid = make_authenticated_stun_reply(tx, ipv4_1(), &remote.pass);
    assert!(agent.handle_packet(
        start + Duration::from_millis(101),
        StunPacket {
            message: StunMessage::parse(&valid).unwrap(),
            source: ipv4_3(),
            destination: ipv4_1(),
            proto: Protocol::Udp
        }
    ));
    assert_eq!(
        agent.nominated_last_received(),
        Some(start + Duration::from_millis(101))
    );
}

fn liveness_authenticated_lite(start: Instant) -> IceAgent {
    let mut agent = new_test_agent();
    agent.set_ice_lite(true);
    agent.set_controlling(false);
    let remote = IceCreds::new();
    agent.set_remote_credentials(remote.clone());
    agent
        .add_local_candidate(Candidate::host(ipv4_1(), "udp").unwrap())
        .unwrap();
    let mut candidate = Candidate::host(ipv4_3(), "udp").unwrap();
    candidate.set_ufrag(&remote.ufrag);
    agent.add_remote_candidate(candidate);
    agent.handle_timeout(start);
    let local = agent.local_credentials().clone();
    let username = format!("{}:{}", local.ufrag, remote.ufrag);
    let request =
        StunMessage::binding_request(&username, TransId::new(), true, 42, 1_800_000_000, true);
    let bytes = serialize_stun_msg(request, &local.pass);
    assert!(agent.handle_packet(
        start + Duration::from_millis(1),
        StunPacket {
            message: StunMessage::parse(&bytes).unwrap(),
            source: ipv4_3(),
            destination: ipv4_1(),
            proto: Protocol::Udp
        }
    ));
    agent.handle_timeout(start + Duration::from_millis(2));
    assert_eq!(
        agent.nominated_transport(),
        Some((Protocol::Udp, false)),
        "actual authenticated USE-CANDIDATE nomination"
    );
    agent
}
#[test]
fn selected_receive_retention_is_opt_in_and_only_current_tuple_data_keeps_lite_pair_alive() {
    for enabled in [false, true] {
        let start = Instant::now();
        let mut agent = liveness_authenticated_lite(start);
        if enabled {
            agent.set_selected_receive_timeout(Some(Duration::from_secs(15)));
        }
        assert!(agent.notify_selected_data_received(
            Protocol::Udp,
            ipv4_3(),
            start + Duration::from_secs(14)
        ));
        agent.handle_timeout(start + Duration::from_secs(16));
        assert_eq!(
            agent.nominated_transport().is_some(),
            enabled,
            "generic original STUN-only pruning remains unchanged unless explicitly opted in"
        );
        if enabled {
            assert!(!agent.notify_selected_data_received(
                Protocol::Udp,
                ipv4_4(),
                start + Duration::from_secs(28)
            ));
            agent.handle_timeout(start + Duration::from_secs(29) + Duration::from_nanos(1));
            assert_eq!(
                agent.nominated_last_received(),
                None,
                "no wrong-peer refresh and expired real receive lifetime retires the pair"
            );
        }
    }
}
