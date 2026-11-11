// Uses existing public Endpoint/Association APIs; no new candidate types.
use super::*;
use crate::{ClientConfig, DatagramEvent, Endpoint, EndpointConfig, generate_snap_token};
struct Peer {
    endpoint: Endpoint,
    association: Association,
    handle: crate::AssociationHandle,
    addr: SocketAddr,
}
fn pair() -> Result<(Peer, Peer)> {
    let transport = Arc::new(TransportConfig::default().with_source_partial_reliability(true));
    let one = generate_snap_token(&transport)?;
    let two = generate_snap_token(&transport)?;
    let left_addr = SocketAddr::from_str("127.0.0.1:15000").unwrap();
    let right_addr = SocketAddr::from_str("127.0.0.1:15001").unwrap();
    let mut left = Endpoint::new(Arc::new(EndpointConfig::default()), None);
    let mut right = Endpoint::new(Arc::new(EndpointConfig::default()), None);
    let mut config = ClientConfig::new().with_snap(one.clone(), two.clone());
    config.transport = transport.clone();
    let (lh, la) = left.connect(config, right_addr).unwrap();
    let mut config = ClientConfig::new().with_snap(two, one);
    config.transport = transport;
    let (rh, ra) = right.connect(config, left_addr).unwrap();
    Ok((
        Peer {
            endpoint: left,
            association: la,
            handle: lh,
            addr: left_addr,
        },
        Peer {
            endpoint: right,
            association: ra,
            handle: rh,
            addr: right_addr,
        },
    ))
}
fn outputs(p: &mut Peer, now: Instant) -> Vec<Bytes> {
    let mut all = vec![];
    while let Some(t) = p.association.poll_transmit(now) {
        let Payload::RawEncode(raw) = t.payload else {
            panic!("actual wire output required")
        };
        all.extend(raw);
    }
    all
}
fn route(p: &mut Peer, from: SocketAddr, raw: Bytes, now: Instant) {
    let (h, event) = p
        .endpoint
        .handle(now, from, None, None, raw)
        .expect("existing endpoint route must survive pending SHUTDOWN");
    assert_eq!(h, p.handle);
    let DatagramEvent::AssociationEvent(event) = event else {
        panic!("must use existing association")
    };
    p.association.handle_event(event);
}
fn contains<T: 'static>(raw: &Bytes) -> bool {
    crate::packet::PartialDecode::unmarshal(raw)
        .unwrap()
        .finish()
        .unwrap()
        .chunks
        .iter()
        .any(|c| c.as_any().is::<T>())
}
#[test]
fn source_shutdown_public_endpoint_retains_route_through_actual_complete() -> Result<()> {
    let now = Instant::now();
    let (mut a, mut b) = pair()?;
    a.association.shutdown()?;
    assert!(
        a.association.poll_endpoint_event().is_none(),
        "graceful request must not release route before peer ACK"
    );
    for raw in outputs(&mut a, now) {
        route(&mut b, a.addr, raw, now);
    }
    assert!(b.association.poll_endpoint_event().is_none());
    let ack = outputs(&mut b, now);
    assert!(ack.iter().any(contains::<ChunkShutdownAck>));
    for raw in ack {
        route(&mut a, b.addr, raw, now);
    }
    assert!(
        a.association.poll_endpoint_event().is_none(),
        "route retained until real terminal wire handoff"
    );
    let complete = outputs(&mut a, now);
    assert!(complete.iter().any(contains::<ChunkShutdownComplete>));
    assert!(a.association.is_closed());
    let event = a
        .association
        .poll_endpoint_event()
        .expect("actual close releases route exactly once");
    a.endpoint.handle_event(a.handle, event);
    assert!(a.association.poll_endpoint_event().is_none());
    for raw in complete {
        route(&mut b, a.addr, raw, now);
    }
    assert!(b.association.is_closed());
    let event = b
        .association
        .poll_endpoint_event()
        .expect("peer complete releases route");
    b.endpoint.handle_event(b.handle, event);
    assert!(b.association.poll_endpoint_event().is_none());
    Ok(())
}
#[test]
fn source_shutdown_real_carried_ack_releases_data_without_sack_rwnd() -> Result<()> {
    let base = Instant::now();
    let (mut a, mut b) = pair()?;
    a.association
        .open_stream(1, PayloadProtocolIdentifier::Binary)?
        .write_sctp(
            &Bytes::from_static(b"carried-ack"),
            PayloadProtocolIdentifier::Binary,
        )?;
    let raw = outputs(&mut a, base);
    assert!(raw.iter().any(contains::<ChunkPayloadData>));
    assert!(a.association.outbound_queue.inflight_len() > 0);
    for raw in raw {
        route(&mut b, a.addr, raw, base + Duration::from_millis(1));
    }
    b.association.shutdown()?;
    let raw = outputs(&mut b, base + Duration::from_millis(2));
    let shutdown: Vec<_> = raw.into_iter().filter(contains::<ChunkShutdown>).collect();
    assert_eq!(
        shutdown.len(),
        1,
        "real peer emits carried cumulative ACK; deliberately do not deliver separate SACK"
    );
    let rwnd = a.association.rwnd;
    let sacks = a.association.stats.get_num_sacks();
    for raw in shutdown {
        route(&mut a, b.addr, raw, base + Duration::from_millis(3));
    }
    assert_eq!(
        a.association.outbound_queue.inflight_len(),
        0,
        "carried ACK must own real DATA release"
    );
    assert_eq!(a.association.rwnd, rwnd, "SHUTDOWN has no a_rwnd field");
    assert_eq!(
        a.association.stats.get_num_sacks(),
        sacks,
        "not a fabricated received SACK"
    );
    assert!(
        outputs(&mut a, base + Duration::from_millis(3))
            .iter()
            .any(contains::<ChunkShutdownAck>)
    );
    Ok(())
}
#[test]
#[ignore = "requires actual fixed-Pion GO_SCTP_SHUTDOWN_OUTPUT; no prefilled expected"]
fn actual_go_shutdown_carried_ack_fields_match() -> Result<()> {
    use alloc::{borrow::ToOwned, collections::BTreeSet};
    let path = std::env::var("GO_SCTP_SHUTDOWN_OUTPUT").expect("actual Go output required");
    let text = std::fs::read_to_string(path).unwrap();
    let mut seen = BTreeSet::new();
    for line in text.lines() {
        let cols: Vec<_> = line.split('\t').collect();
        assert_eq!(cols.len(), 10);
        let id = cols[0];
        assert!(seen.insert(id.to_owned()));
        let now = Instant::now();
        let mut a = Association {
            state: AssociationState::Established,
            source_partial_reliability: true,
            use_forward_tsn: true,
            my_next_tsn: 1,
            cumulative_tsn_ack_point: 0,
            advanced_peer_tsn_ack_point: 0,
            cwnd: 10000,
            rwnd: 9999,
            mtu: 1228,
            max_payload_size: 1200,
            ..Default::default()
        };
        a.create_stream(0, false, PayloadProtocolIdentifier::Binary)
            .unwrap();
        for i in 0..2 {
            a.outbound_queue.push(vec![ChunkPayloadData {
                beginning_fragment: true,
                ending_fragment: true,
                stream_identifier: 0,
                payload_type: PayloadProtocolIdentifier::Binary,
                user_data: Bytes::from(vec![i, 2]),
                ..Default::default()
            }]);
            let chunk = a.send_next_data_chunk(now).unwrap();
            assert!(a.create_packet(vec![Box::new(chunk)]).marshal()?.len() >= 28);
        }
        let mut through = 2;
        match id {
            "all-acknowledged" => {}
            "partial-acknowledged" => through = 1,
            "invalid-future" => through = 3,
            "stale-ack" => through = u32::MAX,
            "pending-data" => a.outbound_queue.push(vec![ChunkPayloadData {
                beginning_fragment: true,
                ending_fragment: true,
                stream_identifier: 0,
                payload_type: PayloadProtocolIdentifier::Binary,
                user_data: Bytes::from_static(&[3, 2]),
                ..Default::default()
            }]),
            "crossed-shutdown" | "shutdown-ack" => {
                a.set_state(AssociationState::ShutdownSent);
                a.will_send_shutdown = true;
            }
            "repeat-shutdown" => a.set_state(AssociationState::ShutdownAckSent),
            _ => panic!("unrecognized actual source case {id}"),
        }
        let result = if id == "shutdown-ack" {
            a.source_handle_shutdown_ack(&ChunkShutdownAck {})
        } else {
            a.source_handle_shutdown(
                &ChunkShutdown {
                    cumulative_tsn_ack: through,
                },
                now + Duration::from_millis(1),
            )
        };
        let actual = alloc::format!(
            "{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}",
            id,
            a.state(),
            a.outbound_queue.inflight_len(),
            a.outbound_queue.pending_len(),
            a.cumulative_tsn_ack_point,
            a.rwnd,
            a.will_send_shutdown_ack,
            a.will_send_shutdown,
            a.will_send_shutdown_complete,
            result.is_err()
        );
        assert_eq!(actual, line, "strict actual source fields {id}");
    }
    assert_eq!(seen.len(), 8);
    Ok(())
}

#[test]
fn source_shutdown_pending_before_first_poll_drains_original_and_rejects_new_write() -> Result<()> {
    let (mut a, mut b) = pair()?;
    let now = Instant::now();
    let payload = Bytes::from_static(b"accepted-before-shutdown");
    a.association
        .open_stream(1, PayloadProtocolIdentifier::Binary)?
        .write_sctp(&payload, PayloadProtocolIdentifier::Binary)?;
    assert_eq!(a.association.outbound_queue.inflight_len(), 0);
    assert!(a.association.outbound_queue.pending_len() > 0);
    a.association.shutdown()?;
    assert_eq!(a.association.state(), AssociationState::ShutdownPending);
    assert!(matches!(
        a.association.stream(1)?.write_sctp(
            &Bytes::from_static(b"must-not-be-accepted"),
            PayloadProtocolIdentifier::Binary
        ),
        Err(Error::ErrStreamClosed)
    ));
    assert!(a.association.poll_endpoint_event().is_none());
    let mut received = Vec::new();
    let mut releases = [0, 0];
    for tick in 0..2000 {
        let time = now + Duration::from_millis(tick);
        a.association.handle_timeout(time);
        b.association.handle_timeout(time);
        for raw in outputs(&mut a, time) {
            route(&mut b, a.addr, raw, time);
            if let Ok(mut stream) = b.association.stream(1) {
                while let Ok(Some(message)) = stream.read_sctp() {
                    let mut bytes = vec![0; message.len()];
                    message.read(&mut bytes)?;
                    received.extend(bytes);
                }
            }
        }
        for raw in outputs(&mut b, time) {
            route(&mut a, b.addr, raw, time);
        }
        for (index, peer) in [&mut a, &mut b].into_iter().enumerate() {
            while let Some(event) = peer.association.poll_endpoint_event() {
                releases[index] += 1;
                peer.endpoint.handle_event(peer.handle, event);
            }
        }
        if a.association.is_closed() && b.association.is_closed() {
            break;
        }
    }
    assert_eq!(
        received,
        payload.as_ref(),
        "all pre-shutdown accepted bytes exactly once"
    );
    assert!(a.association.is_closed() && b.association.is_closed());
    assert_eq!(releases, [1, 1], "each real endpoint route is retired once");
    Ok(())
}
