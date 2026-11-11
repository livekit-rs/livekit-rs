#[test]
fn fallback_selected_route_uses_authenticated_pair_and_prflx_relay_ip_not_port() {
    let start = Instant::now();
    let mut agent = acceptance_checked_pair(
        CandidateKind::Host,
        CandidateKind::Host,
        [Duration::ZERO; 4],
        start,
        ipv4_1(),
    );
    assert_eq!(agent.nominated_transport(), Some((Protocol::Udp, false)));
    let mut relay = Candidate::relayed("3.4.5.6:6000".parse().unwrap(), ipv4_4(), "udp").unwrap();
    relay.set_ufrag(&agent.remote_credentials().unwrap().ufrag);
    agent.add_remote_candidate(relay);
    assert_eq!(
        agent.nominated_transport(),
        Some((Protocol::Udp, false)),
        "a host at a relay IP is still a host"
    );
    let mut agent = acceptance_checked_pair(
        CandidateKind::Host,
        CandidateKind::PeerReflexive,
        [Duration::ZERO; 4],
        start,
        ipv4_1(),
    );
    assert_eq!(agent.nominated_transport(), Some((Protocol::Udp, false)));
    let mut relay = Candidate::relayed("3.4.5.6:6000".parse().unwrap(), ipv4_4(), "udp").unwrap();
    relay.set_ufrag(&agent.remote_credentials().unwrap().ufrag);
    agent.add_remote_candidate(relay);
    assert_eq!(
        agent.nominated_transport(),
        Some((Protocol::Udp, true)),
        "fixed source intentionally ignores TURN port for prflx classification"
    );
    agent.ice_restart(IceCreds::new(), true);
    assert_eq!(
        agent.nominated_transport(),
        None,
        "a retired selected pair cannot classify a replacement connection"
    );
}
