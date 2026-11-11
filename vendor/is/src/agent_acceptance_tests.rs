// Source oracle: fixed LiveKit ICE selection.go controllingSelector (both
// candidate kinds), and agent_config.go's four defaults. These tests drive
// authenticated STUN, not manually promoted pair state or hidden events.
fn acceptance_candidate(kind: CandidateKind, address: SocketAddr) -> Candidate {
    match kind {
        CandidateKind::Host => Candidate::host(address, "udp").unwrap(),
        CandidateKind::ServerReflexive => {
            Candidate::server_reflexive(address, address, "udp").unwrap()
        }
        CandidateKind::PeerReflexive => Candidate::peer_reflexive(
            Protocol::Udp,
            address,
            address,
            1_800_000_000,
            None,
            "peer".into(),
        ),
        CandidateKind::Relayed => Candidate::relayed(address, address, "udp").unwrap(),
    }
}

fn acceptance_checked_pair(
    local: CandidateKind,
    remote: CandidateKind,
    waits: [Duration; 4],
    start: Instant,
    mapped: SocketAddr,
) -> IceAgent {
    let mut agent = new_test_agent();
    agent.set_controlling(true);
    agent.set_candidate_acceptance_min_waits(waits);
    let remote_creds = IceCreds::new();
    agent.set_remote_credentials(remote_creds.clone());
    agent
        .add_local_candidate(acceptance_candidate(local, ipv4_1()))
        .unwrap();
    let mut candidate = acceptance_candidate(remote, ipv4_3());
    candidate.set_ufrag(&remote_creds.ufrag);
    agent.add_remote_candidate(candidate);
    agent.handle_timeout(start);
    let request = agent.poll_transmit().expect("actual connectivity check");
    let request = StunMessage::parse(&request.contents).unwrap();
    assert!(request.is_binding_request());
    assert!(
        !request.use_candidate(),
        "unvalidated pairs cannot be nominated"
    );
    let reply = make_authenticated_stun_reply(request.trans_id(), mapped, &remote_creds.pass);
    assert!(agent.handle_packet(
        start + Duration::from_millis(1),
        StunPacket {
            message: StunMessage::parse(&reply).unwrap(),
            source: ipv4_3(),
            destination: ipv4_1(),
            proto: Protocol::Udp,
        }
    ));
    assert_eq!(agent.candidate_pairs[0].state(), CheckState::Succeeded);
    agent
}

fn assert_no_nomination(agent: &mut IceAgent) {
    assert!(agent.nominated_send.is_none());
    assert!(!agent.candidate_pairs.iter().any(|p| p.is_nominated()));
    while let Some(datagram) = agent.poll_transmit() {
        assert!(
            !StunMessage::parse(&datagram.contents)
                .unwrap()
                .use_candidate()
        );
    }
    assert!(
        !agent
            .events
            .iter()
            .any(|event| matches!(event, IceAgentEvent::NominatedSend { .. }))
    );
}

#[test]
fn acceptance_all_sixteen_kind_pairs_gate_real_use_candidate_at_maximum_age() {
    use CandidateKind::*;
    let kinds = [Host, ServerReflexive, PeerReflexive, Relayed];
    let waits = [80, 120, 160, 200].map(Duration::from_millis);
    for (li, local) in kinds.into_iter().enumerate() {
        for (ri, remote) in kinds.into_iter().enumerate() {
            let start = Instant::now();
            let mut agent = acceptance_checked_pair(local, remote, waits, start, ipv4_1());
            assert_no_nomination(&mut agent);
            let deadline = start + waits[li].max(waits[ri]);
            let wake = agent
                .poll_timeout()
                .expect("pending nomination must be scheduled");
            assert!(wake <= deadline, "do not wait for the next STUN RTO");
            agent.handle_timeout(deadline - Duration::from_nanos(1));
            assert_no_nomination(&mut agent);
            agent.handle_timeout(deadline);
            assert!(agent.candidate_pairs[0].is_nominated());
            let nomination = agent
                .poll_transmit()
                .expect("actual nomination datagram at boundary");
            assert!(
                StunMessage::parse(&nomination.contents)
                    .unwrap()
                    .use_candidate()
            );
            assert_eq!(agent.stats.nomination_send_count, 1);
        }
    }
}

#[test]
fn acceptance_mapped_local_peer_reflexive_kind_is_not_original_host_kind() {
    let start = Instant::now();
    let waits = [
        Duration::ZERO,
        Duration::from_millis(500),
        Duration::from_secs(1),
        Duration::from_secs(2),
    ];
    let mut agent = acceptance_checked_pair(
        CandidateKind::Host,
        CandidateKind::Host,
        waits,
        start,
        ipv4_2(),
    );
    assert_eq!(
        agent.candidate_pairs[0]
            .valid_local_candidate(&agent.local_candidates)
            .kind(),
        CandidateKind::PeerReflexive
    );
    assert_no_nomination(&mut agent);
    agent.handle_timeout(start + Duration::from_millis(999));
    assert_no_nomination(&mut agent);
    agent.handle_timeout(start + Duration::from_secs(1));
    assert!(
        agent
            .poll_transmit()
            .is_some_and(|tx| StunMessage::parse(&tx.contents).unwrap().use_candidate())
    );
}

#[test]
fn acceptance_controlled_use_candidate_is_not_delayed_by_any_wait() {
    let start = Instant::now();
    let mut agent = new_test_agent();
    agent.set_ice_lite(true);
    agent.set_candidate_acceptance_min_waits([Duration::from_secs(60); 4]);
    agent
        .add_local_candidate(Candidate::host(ipv4_1(), "udp").unwrap())
        .unwrap();
    let remote = IceCreds::new();
    agent.set_remote_credentials(remote.clone());
    let candidate = Candidate::host(ipv4_3(), "udp").unwrap();
    let username = format!("{}:{}", agent.local_credentials.ufrag, remote.ufrag);
    let request = StunMessage::binding_request(
        &username,
        TransId::new(),
        true,
        u64::MAX,
        candidate.prio(),
        true,
    );
    let bytes = serialize_stun_msg(request, &agent.local_credentials.pass);
    assert!(agent.handle_packet(
        start,
        StunPacket {
            message: StunMessage::parse(&bytes).unwrap(),
            source: ipv4_3(),
            destination: ipv4_1(),
            proto: Protocol::Udp
        }
    ));
    agent.handle_timeout(start);
    assert!(agent.nominated_send.is_some());
    assert!(agent.poll_transmit().is_some_and(|tx| {
        StunMessage::parse(&tx.contents)
            .unwrap()
            .is_successful_binding_response()
    }));
}

#[test]
fn acceptance_zero_generic_defaults_preserve_immediate_nomination() {
    let start = Instant::now();
    let mut agent = acceptance_checked_pair(
        CandidateKind::Host,
        CandidateKind::Host,
        [Duration::ZERO; 4],
        start,
        ipv4_1(),
    );
    assert!(agent.nominated_send.is_some());
    agent.handle_timeout(start + Duration::from_millis(50));
    assert!(
        agent
            .poll_transmit()
            .is_some_and(|tx| StunMessage::parse(&tx.contents).unwrap().use_candidate())
    );
}

#[test]
fn acceptance_restart_and_role_change_reset_selection_age_not_stun_pacing() {
    let start = Instant::now();
    let mut agent = acceptance_checked_pair(
        CandidateKind::Host,
        CandidateKind::Host,
        [Duration::from_millis(200); 4],
        start,
        ipv4_1(),
    );
    let timeout_before = agent.last_now;
    let role_time = start + Duration::from_millis(150);
    agent.switch_role(false, role_time);
    agent.switch_role(true, role_time);
    assert_eq!(
        agent.last_now, timeout_before,
        "packet-driven role change cannot reset Ta"
    );
    agent.handle_timeout(start + Duration::from_millis(200));
    assert_no_nomination(&mut agent);
    agent.handle_timeout(role_time + Duration::from_millis(200));
    assert!(agent.nominated_send.is_some());
    agent.ice_restart(IceCreds::new(), true);
    assert!(agent.nomination_started_at.is_none());
    assert!(agent.nominated_send.is_none());
    assert!(agent.candidate_pairs.is_empty());
    agent.set_remote_credentials(IceCreds::new());
    let restarted = start + Duration::from_secs(10);
    agent.handle_timeout(restarted);
    assert_eq!(agent.nomination_started_at, Some(restarted));
}

#[test]
fn acceptance_does_not_bypass_ineligible_best_valid_pair_with_weaker_ready_pair() {
    let start = Instant::now();
    let mut agent = acceptance_checked_pair(
        CandidateKind::Relayed,
        CandidateKind::Host,
        [
            Duration::from_millis(200),
            Duration::ZERO,
            Duration::ZERO,
            Duration::ZERO,
        ],
        start,
        ipv4_1(),
    );
    let remote_credentials = agent.remote_credentials().unwrap().clone();
    let mut lower = acceptance_candidate(CandidateKind::Relayed, ipv4_4());
    lower.set_ufrag(&remote_credentials.ufrag);
    agent.add_remote_candidate(lower);
    agent.handle_timeout(start + Duration::from_millis(50));
    let tx = agent
        .poll_transmit()
        .expect("check the additional weaker pair");
    assert_eq!(tx.destination, ipv4_4());
    let parsed = StunMessage::parse(&tx.contents).unwrap();
    assert!(!parsed.use_candidate());
    let response =
        make_authenticated_stun_reply(parsed.trans_id(), ipv4_1(), &remote_credentials.pass);
    assert!(agent.handle_packet(
        start + Duration::from_millis(51),
        StunPacket {
            message: StunMessage::parse(&response).unwrap(),
            source: ipv4_4(),
            destination: ipv4_1(),
            proto: Protocol::Udp,
        }
    ));
    assert!(
        agent
            .candidate_pairs
            .iter()
            .all(|pair| pair.state() == CheckState::Succeeded)
    );
    assert_no_nomination(&mut agent);
    agent.handle_timeout(start + Duration::from_millis(200));
    let nomination = agent.poll_transmit().expect("nominate the best pair");
    assert_eq!(nomination.destination, ipv4_3());
    assert!(
        StunMessage::parse(&nomination.contents)
            .unwrap()
            .use_candidate()
    );
}

#[test]
fn acceptance_queued_authenticated_request_role_switch_starts_at_processing_not_old_arrival() {
    let start = Instant::now();
    let mut agent = new_test_agent();
    agent.control_tie_breaker = 1;
    agent.set_candidate_acceptance_min_waits([Duration::from_millis(200); 4]);
    agent
        .add_local_candidate(Candidate::host(ipv4_1(), "udp").unwrap())
        .unwrap();
    let remote = IceCreds::new();
    let username = format!("{}:{}", agent.local_credentials.ufrag, remote.ufrag);
    let request = StunMessage::binding_request(
        &username,
        TransId::new(),
        false,
        u64::MAX,
        Candidate::host(ipv4_3(), "udp").unwrap().prio(),
        false,
    );
    let bytes = serialize_stun_msg(request, &agent.local_credentials.pass);
    assert!(agent.handle_packet(
        start,
        StunPacket {
            message: StunMessage::parse(&bytes).unwrap(),
            source: ipv4_3(),
            destination: ipv4_1(),
            proto: Protocol::Udp,
        }
    ));
    assert_eq!(agent.stun_server_queue.len(), 1);
    assert!(
        agent.nomination_started_at.is_none(),
        "pre-start traffic cannot spend the wait"
    );
    agent.set_remote_credentials(remote);
    let processed = start + Duration::from_secs(1);
    agent.handle_timeout(processed);
    assert!(
        agent.controlling(),
        "actual authenticated role conflict selects controlling"
    );
    assert_eq!(agent.nomination_started_at, Some(processed));
    assert!(agent.stun_server_queue.is_empty());
}
