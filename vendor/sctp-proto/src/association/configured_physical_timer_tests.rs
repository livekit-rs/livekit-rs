// Test-only real UDP/T3 consumer. No timer shortening or recovery selector calls.
struct PhysicalTimerPeer {
    ep: crate::Endpoint,
    assoc: Association,
    handle: crate::AssociationHandle,
    socket: std::net::UdpSocket,
}
impl PhysicalTimerPeer {
    fn receive_available(&mut self, now: Instant) {
        let mut bytes = [0u8; 8192];
        loop {
            let (n, remote) = match self.socket.recv_from(&mut bytes) {
                Ok(value) => value,
                Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => break,
                Err(e) => panic!("actual UDP receive: {e}"),
            };
            let (_, event) = self
                .ep
                .handle(now, remote, None, None, Bytes::copy_from_slice(&bytes[..n]))
                .expect("routable actual SCTP packet");
            match event {
                crate::DatagramEvent::AssociationEvent(e) => self.assoc.handle_event(e),
                _ => panic!("unexpected real association event"),
            }
        }
    }
    fn packets(&mut self, now: Instant) -> Vec<Bytes> {
        let mut output = vec![];
        loop {
            while let Some(e) = self.assoc.poll_endpoint_event() {
                if let Some(e) = self.ep.handle_event(self.handle, e) {
                    self.assoc.handle_event(e);
                }
            }
            while self.assoc.poll().is_some() {}
            let Some(t) = self
                .assoc
                .poll_transmit(now)
                .or_else(|| self.ep.poll_transmit())
            else {
                break;
            };
            match t.payload {
                crate::Payload::RawEncode(packets) => output.extend(packets),
                _ => panic!("outgoing SCTP packet is not encoded"),
            }
        }
        output
    }
}
fn physical_timer_pair(source: bool, reliable: bool) -> (PhysicalTimerPeer, PhysicalTimerPeer) {
    let left = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
    let right = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
    left.set_nonblocking(true).unwrap();
    right.set_nonblocking(true).unwrap();
    let tc = Arc::new(TransportConfig::default().with_source_partial_reliability(source));
    let ta = crate::generate_snap_token(&tc).unwrap();
    let tb = crate::generate_snap_token(&tc).unwrap();
    let mut ea = crate::Endpoint::new(Arc::new(crate::EndpointConfig::new()), None);
    let mut eb = crate::Endpoint::new(Arc::new(crate::EndpointConfig::new()), None);
    let mut ca = crate::ClientConfig::new().with_snap(ta.clone(), tb.clone());
    ca.transport = tc.clone();
    let mut cb = crate::ClientConfig::new().with_snap(tb, ta);
    cb.transport = tc;
    let (ha, mut a) = ea.connect(ca, right.local_addr().unwrap()).unwrap();
    let (hb, mut b) = eb.connect(cb, left.local_addr().unwrap()).unwrap();
    let kind = if reliable {
        ReliabilityType::Reliable
    } else {
        ReliabilityType::Rexmit
    };
    for association in [&mut a, &mut b] {
        association
            .open_stream(0, PayloadProtocolIdentifier::Binary)
            .unwrap()
            .set_reliability_params(true, kind, 0)
            .unwrap();
    }
    assert_eq!(a.fast_rtx_wnd, 0);
    (
        PhysicalTimerPeer {
            ep: ea,
            assoc: a,
            handle: ha,
            socket: left,
        },
        PhysicalTimerPeer {
            ep: eb,
            assoc: b,
            handle: hb,
            socket: right,
        },
    )
}
fn physical_data_tsns(packet: &[u8]) -> Vec<u32> {
    assert!(packet.len() >= 12);
    let mut tsns = vec![];
    let mut offset = 12;
    while offset < packet.len() {
        assert!(offset + 4 <= packet.len());
        let size = usize::from(u16::from_be_bytes(
            packet[offset + 2..offset + 4].try_into().unwrap(),
        ));
        assert!(size >= 4 && offset + size <= packet.len());
        if packet[offset] == 0 {
            assert!(size >= 16);
            tsns.push(u32::from_be_bytes(
                packet[offset + 4..offset + 8].try_into().unwrap(),
            ));
        }
        offset += (size + 3) & !3;
    }
    tsns
}
#[test]
#[ignore = "real UDP/wall-clock T3; coordinator runs only after original Go physical oracle"]
fn real_udp_default_fast_zero_fragmented_message_matches_actual_go_timer_result() {
    // This TSV is derived strictly from the actual original Go JSON observations.
    // No predicted source delivery value is hard-coded into this test.
    let actual = std::fs::read_to_string(std::env::var("GO_SCTP_PHYSICAL_TSV").unwrap()).unwrap();
    let mut rows = 0;
    for line in actual.lines() {
        let columns: Vec<_> = line.split('\t').collect();
        assert_eq!(columns.len(), 5);
        let id = columns[0];
        let size: usize = columns[1].parse().unwrap();
        let reliable = columns[2] == "true";
        let loss = columns[3] == "true";
        let delivered = columns[4] == "true";
        for source in [false, true] {
            let (mut a, mut b) = physical_timer_pair(source, reliable);
            let expected: Vec<u8> = (0..size).map(|i| (i * 31 + 7) as u8).collect();
            assert_eq!(a.assoc.stream(0).unwrap().write(&expected).unwrap(), size);
            let start = Instant::now();
            let mut received = vec![];
            let mut dropped = 0u64;
            let mut repeated = 0u64;
            let mut seen = std::collections::BTreeSet::new();
            // Keep the original positive deadline; negatives observe the same
            // full15s window as the actual original Go outer context.
            let observation = Duration::from_secs(if delivered { 12 } else { 15 });
            while start.elapsed() < observation {
                let now = Instant::now();
                a.assoc.handle_timeout(now);
                b.assoc.handle_timeout(now);
                let before_t3 = a.assoc.stats.get_num_t3timeouts() == 0;
                for packet in a.packets(now) {
                    let tsns = physical_data_tsns(&packet);
                    for tsn in &tsns {
                        if !seen.insert(*tsn) {
                            repeated += 1;
                        }
                    }
                    if loss && before_t3 && !tsns.is_empty() {
                        dropped += 1;
                        continue;
                    }
                    assert_eq!(
                        a.socket
                            .send_to(&packet, b.socket.local_addr().unwrap())
                            .unwrap(),
                        packet.len()
                    );
                }
                b.receive_available(now);
                for packet in b.packets(now) {
                    assert_eq!(
                        b.socket
                            .send_to(&packet, a.socket.local_addr().unwrap())
                            .unwrap(),
                        packet.len()
                    );
                }
                a.receive_available(now);
                while let Some(chunks) = b.assoc.stream(0).unwrap().read().unwrap() {
                    let mut body = vec![0; chunks.len()];
                    chunks.read(&mut body).unwrap();
                    received.push(body);
                }
                if !received.is_empty() {
                    break;
                }
                std::thread::sleep(Duration::from_millis(1));
            }
            let t3 = a.assoc.stats.get_num_t3timeouts();
            if loss {
                assert!(
                    dropped > 0 && t3 > 0,
                    "{id}: actual loss and real timer required"
                );
            }
            assert!(
                received.len() <= 1,
                "{id}: whole messages only, no duplication"
            );
            if let Some(body) = received.first() {
                assert_eq!(body, &expected, "{id}: actual payload");
            }
            std::eprintln!("physical SCTP {id} source={source} delivered={} t3={t3} dropped={dropped} repeated={repeated}", !received.is_empty());
            if source {
                assert_eq!(
                    !received.is_empty(),
                    delivered,
                    "{id}: actual fixed Go result, default fast0"
                );
            } else if !loss || reliable {
                assert_eq!(
                    !received.is_empty(),
                    delivered,
                    "{id}: unchanged native reliable/unlost controls"
                );
            }
            // Legacy native's fragmented MaxRTX0 outcome is recorded, not asserted
            // to be equivalent to source. Source-only mutation false must fail the
            // same actual-Go assertion when the source loss observation is positive.
        }
        rows += 1;
    }
    assert_eq!(rows, 4);
}
