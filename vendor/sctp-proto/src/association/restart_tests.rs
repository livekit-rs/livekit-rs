use super::*;

fn live_tcb(now: Instant) -> Association {
    let config = Arc::new(TransportConfig::default().with_source_partial_reliability(true));
    let mut a = Association::new_common(
        config,
        1400,
        "127.0.0.1:9000".parse().unwrap(),
        None,
        Side::Server,
        111,
        100,
    );
    a.state = AssociationState::Established;
    a.handshake_completed = true;
    a.peer_verification_tag = 222;
    a.peer_last_tsn = 199;
    a.rwnd = 1 << 20;
    a.open_stream(1, PayloadProtocolIdentifier::Binary).unwrap();
    a.events.clear();
    let _ = now;
    a
}
fn receive_wire(a: &mut Association, packet: Packet, now: Instant) {
    let encoded = packet.marshal().unwrap();
    let parsed = Packet::unmarshal(&encoded).unwrap();
    a.handle_inbound(parsed, now).unwrap();
}
fn request(a: &Association, peer_tag: u32) -> Packet {
    let mut init = ChunkInit {
        initiate_tag: peer_tag,
        initial_tsn: 500,
        advertised_receiver_window_credit: 1 << 20,
        num_inbound_streams: 100,
        num_outbound_streams: 100,
        ..Default::default()
    };
    init.set_supported_extensions();
    Packet {
        common_header: CommonHeader {
            source_port: a.destination_port,
            destination_port: a.source_port,
            verification_tag: 0,
        },
        chunks: vec![Box::new(init)],
    }
}
fn challenge(a: &mut Association, peer_tag: u32, now: Instant) -> (u32, Bytes) {
    let packet = request(a, peer_tag);
    receive_wire(a, packet, now);
    let ack = a.control_queue.pop_front().expect("wire INIT-ACK");
    let ack = Packet::unmarshal(&ack.marshal().unwrap()).unwrap();
    let init = ack.chunks[0].as_any().downcast_ref::<ChunkInit>().unwrap();
    assert!(init.is_ack);
    let cookie = init
        .params
        .iter()
        .find_map(|param| param.as_any().downcast_ref::<ParamStateCookie>())
        .unwrap();
    (init.initiate_tag, cookie.cookie.clone())
}
fn echo(a: &Association, local_tag: u32, cookie: Bytes) -> Packet {
    Packet {
        common_header: CommonHeader {
            source_port: a.destination_port,
            destination_port: a.source_port,
            verification_tag: local_tag,
        },
        chunks: vec![Box::new(ChunkCookieEcho { cookie })],
    }
}

#[test]
fn wire_restart_commits_same_tcb_only_after_authenticated_cookie_and_preserves_open_stream() {
    let now = Instant::now();
    let mut a = live_tcb(now);
    let identity = &a as *const Association;
    let stream_generation = a.streams[&1].stats.generation;
    a.stream(1).unwrap().write(b"old-inflight").unwrap();
    let old = a.send_next_data_chunk(now).unwrap();
    a.stream(1).unwrap().write(b"old-pending").unwrap();
    a.timers.start(Timer::T3RTX, now, 1000);
    let (tag, cookie) = challenge(&mut a, 333, now);
    let (_, competing) = challenge(&mut a, 444, now);
    assert_eq!(a.my_verification_tag, 111);
    assert!(
        a.stream(1).unwrap().buffered_amount().unwrap() > 0,
        "INIT cannot discard live data"
    );
    let mut forged = cookie.to_vec();
    *forged.last_mut().unwrap() ^= 1;
    let packet = echo(&a, tag, forged.into());
    receive_wire(&mut a, packet, now);
    assert_eq!(a.restart_generation, 0);
    let packet = echo(&a, tag, cookie.clone());
    receive_wire(&mut a, packet, now + Duration::from_micros(250));
    assert_eq!(
        &a as *const Association, identity,
        "restart must not construct a new association"
    );
    assert_eq!(a.restart_generation, 1);
    assert_eq!(a.my_verification_tag, tag);
    assert_eq!(a.peer_verification_tag, 333);
    assert_eq!(a.state, AssociationState::Established);
    assert_eq!(
        a.streams[&1].stats.generation, stream_generation,
        "application stream remains open"
    );
    assert_eq!(a.stream(1).unwrap().buffered_amount().unwrap(), 0);
    assert!(a.stream(1).unwrap().read().unwrap().is_none());
    assert!(a.timers.get(Timer::T3RTX).is_none());
    assert_eq!(
        a.outbound_queue.pending_len() + a.outbound_queue.inflight_len(),
        0
    );
    assert!(!a.ordinary_write_pending);
    assert!(
        a.events
            .iter()
            .any(|event| matches!(event, Event::Stream(StreamEvent::Writable { id: 1 })))
    );
    assert!(a.events.iter().all(|event| !matches!(
        event,
        Event::AssociationLost { .. } | Event::Stream(StreamEvent::Finished { id: 1 })
    )));
    let ack = a.control_queue.pop_back().unwrap();
    assert!(ack.chunks[0].as_any().is::<ChunkCookieAck>());
    a.stream(1).unwrap().write(b"new-generation").unwrap();
    let sent = a
        .send_next_data_chunk(now + Duration::from_millis(1))
        .unwrap();
    assert_eq!(sent.stream_sequence_number, 0);
    assert_eq!(sent.tsn, old.tsn.wrapping_add(1));
    let mut peer = live_tcb(now);
    peer.my_verification_tag = 333;
    peer.peer_verification_tag = tag;
    peer.peer_last_tsn = sent.tsn.wrapping_sub(1);
    let wire = a.create_packet(vec![Box::new(sent.clone())]);
    receive_wire(&mut peer, wire, now + Duration::from_millis(2));
    let message = peer
        .stream(1)
        .unwrap()
        .read()
        .unwrap()
        .expect("post-restart DATA delivered");
    let mut payload = [0u8; 14];
    assert_eq!(message.read(&mut payload).unwrap(), payload.len());
    assert_eq!(&payload, b"new-generation");
    // A stale old-tag SACK cannot acknowledge the fresh flight queue.
    let old_ack = Packet {
        common_header: CommonHeader {
            source_port: a.destination_port,
            destination_port: a.source_port,
            verification_tag: 111,
        },
        chunks: vec![Box::new(ChunkSelectiveAck {
            cumulative_tsn_ack: sent.tsn,
            advertised_receiver_window_credit: 1 << 20,
            ..Default::default()
        })],
    };
    receive_wire(&mut a, old_ack, now + Duration::from_millis(3));
    assert!(a.stream(1).unwrap().buffered_amount().unwrap() > 0);
    let packet = echo(&a, tag, cookie);
    receive_wire(&mut a, packet, now + Duration::from_millis(4));
    assert_eq!(
        a.restart_generation, 1,
        "duplicate accepted COOKIE-ECHO only repeats ACK"
    );
    let competing_tag = u32::from_be_bytes(competing[16..20].try_into().unwrap());
    let packet = echo(&a, competing_tag, competing);
    receive_wire(&mut a, packet, now + Duration::from_millis(5));
    assert_eq!(
        a.restart_generation, 1,
        "competing old tie-tags cannot roll the TCB back"
    );
}

#[test]
fn wire_restart_rejects_stale_cookie_ports_and_new_addresses_without_discarding_live_data() {
    use crate::chunk::{RESTART_OF_AN_ASSOCIATION_WITH_NEW_ADDRESSES, STALE_COOKIE_ERROR};
    use crate::param::param_header::ParamHeader;
    use crate::param::param_type::ParamType;
    let now = Instant::now();
    let mut a = live_tcb(now);
    a.stream(1).unwrap().write(b"retained").unwrap();
    let (tag, cookie) = challenge(&mut a, 333, now);
    let mut wrong_port = echo(&a, tag, cookie.clone());
    wrong_port.common_header.source_port += 1;
    receive_wire(&mut a, wrong_port, now);
    assert_eq!(a.restart_generation, 0);
    let packet = echo(&a, tag, cookie);
    receive_wire(&mut a, packet, now + Duration::from_secs(61));
    let response = a.control_queue.pop_back().unwrap();
    let error = response.chunks[0]
        .as_any()
        .downcast_ref::<ChunkError>()
        .unwrap();
    assert_eq!(error.error_causes[0].code, STALE_COOKIE_ERROR);
    assert_eq!(&error.error_causes[0].raw[..], &1_000_000u32.to_be_bytes());
    assert_eq!(a.stream(1).unwrap().buffered_amount().unwrap(), 8);
    let mut init = request(&a, 444);
    let value = init.chunks[0].as_any().downcast_ref::<ChunkInit>().unwrap();
    let mut value = value.clone();
    value.params.push(Box::new(ParamHeader {
        typ: ParamType::HostNameAddr,
        value_length: 0,
    }));
    // Exercise the production restart decision before unsupported address parsing
    // (the native generic parser already rejects such an address on the wire).
    let packets = a.handle_init_at(&init, &value, now).unwrap();
    let abort = packets[0].chunks[0]
        .as_any()
        .downcast_ref::<ChunkAbort>()
        .unwrap();
    assert_eq!(
        abort.error_causes[0].code,
        RESTART_OF_AN_ASSOCIATION_WITH_NEW_ADDRESSES
    );
    init.common_header.source_port += 1;
    receive_wire(&mut a, init, now);
    assert_eq!(
        a.state,
        AssociationState::Established,
        "invalid restart must not close the live TCB"
    );
}

fn emitted_data(a: &mut Association, now: Instant) -> ChunkPayloadData {
    let packets = a.gather_outbound(now).0;
    packets
        .iter()
        .find_map(|wire| {
            let packet = Packet::unmarshal(wire).unwrap();
            packet
                .chunks
                .iter()
                .find_map(|chunk| chunk.as_any().downcast_ref::<ChunkPayloadData>().cloned())
        })
        .expect("actual encoded DATA packet")
}

#[test]
fn wire_sack_fractional_history_drives_precise_t3_and_karn_exclusion() {
    let start = Instant::now();
    let mut a = live_tcb(start);
    let mut expected_srtt = 0.0_f64;
    let mut expected_variance = 0.0_f64;
    for (index, sample_us) in [1_250_125u64, 1_750_875, 1_125_375, 2_250_625]
        .into_iter()
        .enumerate()
    {
        let sent_at = start + Duration::from_secs(index as u64 * 20);
        a.stream(1).unwrap().write(b"sample").unwrap();
        let sent = emitted_data(&mut a, sent_at);
        let elapsed = Duration::from_micros(sample_us);
        let received_at = sent_at + elapsed;
        let ack = Packet {
            common_header: CommonHeader {
                source_port: a.destination_port,
                destination_port: a.source_port,
                verification_tag: a.my_verification_tag,
            },
            chunks: vec![Box::new(ChunkSelectiveAck {
                cumulative_tsn_ack: sent.tsn,
                advertised_receiver_window_credit: 1 << 20,
                ..Default::default()
            })],
        };
        receive_wire(&mut a, ack, received_at);
        let sample_ms = sample_us as f64 / 1000.0;
        if expected_srtt == 0.0 {
            expected_srtt = sample_ms;
            expected_variance = sample_ms / 2.0;
        } else {
            expected_variance = 0.75 * expected_variance + 0.25 * (expected_srtt - sample_ms).abs();
            expected_srtt = 0.875 * expected_srtt + 0.125 * sample_ms;
        }
        let expected_rto = (expected_srtt + 4.0 * expected_variance).clamp(1000.0, 60000.0);
        assert!((a.rto_mgr.get_rto_precise() - expected_rto).abs() < 1e-8);
        a.stream(1).unwrap().write(b"timer-owner").unwrap();
        let flight = emitted_data(&mut a, received_at);
        // The actual outgoing DATA production must arm the physical T3 timer;
        // no helper call or synthetic deadline is allowed to arm it here.
        let deadline = received_at + Duration::from_nanos((expected_rto * 1_000_000.0) as u64);
        assert_eq!(a.timers.get(Timer::T3RTX), Some(deadline));
        assert!(
            !a.timers
                .is_expired(Timer::T3RTX, deadline - Duration::from_nanos(1))
                .0
        );
        let previous = a.rto_mgr.get_rto_precise();
        a.handle_timeout(deadline);
        assert!(a.stats.get_num_t3timeouts() > 0);
        let retry = emitted_data(&mut a, deadline);
        assert_eq!(
            retry.tsn, flight.tsn,
            "physical timeout produces the original DATA again"
        );
        assert!(a.outbound_queue.get(flight.tsn).unwrap().nsent > 1);
        let mut ack = a.create_packet(vec![Box::new(ChunkSelectiveAck {
            cumulative_tsn_ack: flight.tsn,
            advertised_receiver_window_credit: 1 << 20,
            ..Default::default()
        })]);
        ack.common_header.verification_tag = a.my_verification_tag;
        receive_wire(&mut a, ack, deadline + Duration::from_micros(125));
        assert_eq!(
            a.rto_mgr.get_rto_precise(),
            previous,
            "Karn: retransmitted DATA must not update RTT"
        );
        a.timers.stop(Timer::T3RTX);
    }
}

#[test]
fn endpoint_routes_restart_cookie_and_new_tag_data_to_the_original_handle() {
    let now = Instant::now();
    let config = Arc::new(TransportConfig::default().with_source_partial_reliability(true));
    let local = crate::generate_snap_token(&config).unwrap();
    let remote_init = crate::generate_snap_token(&config).unwrap();
    let mut endpoint = crate::Endpoint::new(Arc::new(crate::EndpointConfig::new()), None);
    let mut client_config = crate::ClientConfig::new().with_snap(local, remote_init);
    client_config.transport = config;
    let remote: SocketAddr = "127.0.0.1:9000".parse().unwrap();
    let (handle, mut a) = endpoint.connect(client_config, remote).unwrap();
    a.open_stream(1, PayloadProtocolIdentifier::Binary).unwrap();
    while a.poll().is_some() {}
    let packet = request(&a, 333);
    let (routed, event) = endpoint
        .handle(now, remote, None, None, packet.marshal().unwrap())
        .unwrap();
    assert_eq!(routed, handle);
    match event {
        crate::DatagramEvent::AssociationEvent(event) => a.handle_event(event),
        _ => panic!("restart INIT must not allocate another association"),
    }
    let ack = a.control_queue.pop_front().unwrap();
    let init = ack.chunks[0].as_any().downcast_ref::<ChunkInit>().unwrap();
    let tag = init.initiate_tag;
    let cookie = init
        .params
        .iter()
        .find_map(|param| param.as_any().downcast_ref::<ParamStateCookie>())
        .unwrap()
        .cookie
        .clone();
    let packet = echo(&a, tag, cookie);
    let (routed, event) = endpoint
        .handle(now, remote, None, None, packet.marshal().unwrap())
        .unwrap();
    assert_eq!(routed, handle);
    match event {
        crate::DatagramEvent::AssociationEvent(event) => a.handle_event(event),
        _ => panic!("restart COOKIE-ECHO must retain association ownership"),
    }
    while let Some(event) = a.poll_endpoint_event() {
        endpoint.handle_event(handle, event);
    }
    let packet = Packet {
        common_header: CommonHeader {
            source_port: a.destination_port,
            destination_port: a.source_port,
            verification_tag: tag,
        },
        chunks: vec![Box::new(ChunkPayloadData {
            tsn: 500,
            stream_identifier: 1,
            stream_sequence_number: 0,
            beginning_fragment: true,
            ending_fragment: true,
            payload_type: PayloadProtocolIdentifier::Binary,
            user_data: Bytes::from_static(b"same-handle"),
            ..Default::default()
        })],
    };
    let (routed, event) = endpoint
        .handle(now, remote, None, None, packet.marshal().unwrap())
        .unwrap();
    assert_eq!(
        routed, handle,
        "new tag DATA must route after the endpoint event"
    );
    match event {
        crate::DatagramEvent::AssociationEvent(event) => a.handle_event(event),
        _ => panic!("DATA cannot allocate a replacement association"),
    }
    let message = a.stream(1).unwrap().read().unwrap().unwrap();
    let mut payload = [0u8; 11];
    assert_eq!(message.read(&mut payload).unwrap(), payload.len());
    assert_eq!(&payload, b"same-handle");
}

#[test]
fn current_init_retransmission_and_shutdown_ack_state_do_not_start_restart() {
    let now = Instant::now();
    let mut a = live_tcb(now);
    a.stream(1).unwrap().write(b"preserve-live-data").unwrap();
    let buffered = a.stream(1).unwrap().buffered_amount().unwrap();
    let tag = a.my_verification_tag;
    let packet = request(&a, a.peer_verification_tag);
    receive_wire(&mut a, packet, now);
    assert!(a.control_queue.is_empty());
    assert!(a.restart_cookie_key.is_none());
    assert_eq!(a.my_verification_tag, tag);
    assert_eq!(a.restart_generation, 0);
    assert_eq!(a.stream(1).unwrap().buffered_amount().unwrap(), buffered);
    a.state = AssociationState::ShutdownAckSent;
    a.will_send_shutdown = true;
    a.timers.start(Timer::T2Shutdown, now, 1000);
    let mut wrong = request(&a, 333);
    wrong.common_header.source_port += 1;
    receive_wire(&mut a, wrong, now);
    assert!(a.control_queue.is_empty());
    assert!(!a.will_send_shutdown_ack);
    assert!(a.will_send_shutdown);
    assert!(a.timers.get(Timer::T2Shutdown).is_some());
    let packet = request(&a, 333);
    receive_wire(&mut a, packet, now);
    assert!(a.will_send_shutdown_ack);
    assert!(!a.will_send_shutdown);
    assert!(a.timers.get(Timer::T2Shutdown).is_none());
    assert!(a.control_queue.is_empty());
    assert!(a.restart_cookie_key.is_none());
    assert_eq!(a.my_verification_tag, tag);
    assert_eq!(a.restart_generation, 0);
    assert_eq!(a.state, AssociationState::ShutdownAckSent);
    assert_eq!(a.stream(1).unwrap().buffered_amount().unwrap(), buffered);
}

/// Read observations produced by the unmodified pinned Pion algorithm and
/// replay the same valid CRC32C INIT/COOKIE frames through native production
/// parsing. Random cookies/timestamps are opaque and deliberately not compared
/// byte-for-byte. The separately-recorded real UDP peer lifecycle is not
/// mislabeled as a native UDP execution by this reader.
#[test]
#[ignore = "requires GO_SCTP_RESTART_HISTORY from exact Pion source observer"]
fn fixed_pion_restart_wire_history_matches_native() {
    use serde_json::{Value, json};
    let input = std::env::var("GO_SCTP_RESTART_HISTORY").expect("fixed Pion JSONL observations");
    let rows: Vec<Value> = std::fs::read_to_string(input)
        .unwrap()
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect();
    assert_eq!(
        rows.len(),
        9,
        "six wire records plus three actual UDP generations required"
    );
    let now = Instant::now();
    let mut a = live_tcb(now);
    let identity = &a as *const Association;
    let stream_generation = a.streams[&1].stats.generation;
    a.stream(1).unwrap().write(b"old-inflight").unwrap();
    a.send_next_data_chunk(now).unwrap();
    a.stream(1).unwrap().write(b"old-pending").unwrap();
    let buffered = a.stream(1).unwrap().buffered_amount().unwrap();
    let mut actual = Vec::new();
    let packet = request(&a, a.peer_verification_tag);
    receive_wire(&mut a, packet, now);
    actual.push(json!({"phase":"duplicate-init","generation":a.restart_generation,"no_challenge":a.control_queue.is_empty(),"buffer_preserved":a.stream(1).unwrap().buffered_amount().unwrap()==buffered}));
    let (tag, cookie) = challenge(&mut a, 333, now);
    let (other_tag, other_cookie) = challenge(&mut a, 444, now);
    let mut forged = cookie.to_vec();
    *forged.last_mut().unwrap() ^= 1;
    let packet = echo(&a, tag, forged.into());
    receive_wire(&mut a, packet, now);
    actual.push(json!({"phase":"invalid-cookie","generation":a.restart_generation,"buffer_preserved":a.stream(1).unwrap().buffered_amount().unwrap()==buffered,"tags_preserved":a.my_verification_tag==111&&a.peer_verification_tag==222}));
    let mut packet = echo(&a, tag, cookie.clone());
    packet.common_header.source_port += 1;
    receive_wire(&mut a, packet, now);
    actual.push(json!({"phase":"wrong-port-cookie","generation":a.restart_generation,"buffer_preserved":a.stream(1).unwrap().buffered_amount().unwrap()==buffered}));
    let packet = echo(&a, tag, cookie.clone());
    receive_wire(&mut a, packet, now);
    actual.push(json!({"phase":"commit","generation":a.restart_generation,"same_tcb":std::ptr::eq(&a,identity),"same_stream":a.streams[&1].stats.generation==stream_generation,"stream_open":a.stream(1).unwrap().buffered_amount().is_ok(),"buffer_released":a.stream(1).unwrap().buffered_amount().unwrap()==0,"pending_empty":a.outbound_queue.pending_len()==0,"inflight_empty":a.outbound_queue.inflight_len()==0,"write_pending":a.ordinary_write_pending}));
    let packet = echo(&a, tag, cookie);
    receive_wire(&mut a, packet, now);
    let packet = echo(&a, other_tag, other_cookie);
    receive_wire(&mut a, packet, now);
    actual.push(json!({"phase":"duplicate-competing-cookie","generation":a.restart_generation,"same_tcb":std::ptr::eq(&a,identity),"same_stream":a.streams[&1].stats.generation==stream_generation,"tags_preserved":a.my_verification_tag==tag&&a.peer_verification_tag==333}));
    let mut shutdown = live_tcb(now);
    shutdown.state = AssociationState::ShutdownAckSent;
    let packet = request(&shutdown, 333);
    receive_wire(&mut shutdown, packet, now);
    actual.push(json!({"phase":"shutdown-ack-init","generation":shutdown.restart_generation,"no_challenge":shutdown.control_queue.is_empty(),"shutdown_ack_pending":shutdown.will_send_shutdown_ack}));
    for row in &actual {
        let matches: Vec<_> = rows
            .iter()
            .filter(|source| source["case"] == "wire" && source["phase"] == row["phase"])
            .collect();
        assert_eq!(
            matches.len(),
            1,
            "one exact source observation for {}",
            row["phase"]
        );
        for (key, value) in row.as_object().unwrap() {
            assert_eq!(
                &matches[0][key], value,
                "actual native vs fixed Go phase={} field={key}",
                row["phase"]
            );
        }
    }
    // Require the physically-executed Go scope to exist. Native physical peer
    // and async writer-owner consumers have their own execution gates; this
    // reader does not turn a recorded Go callback into native execution proof.
    for generation in 0..3 {
        let phase = format!("generation-{generation}");
        let row = rows
            .iter()
            .find(|row| row["case"] == "physical" && row["phase"] == phase)
            .unwrap();
        assert_eq!(row["generation"], generation);
        assert_eq!(row["same_tcb"], true);
        assert_eq!(row["same_stream"], true);
        assert_eq!(row["stream_open"], true);
        assert_eq!(row["actual_bidirectional_data"], true);
        assert_eq!(row["old_attempt_retired"], generation > 0);
        assert_eq!(row["buffer_callback_released"], generation > 0);
    }
}
