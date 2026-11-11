use super::*;
use alloc::borrow::ToOwned;
include!("adaptive_rack_cases.rs");
fn prepare(input: &Input, now: Instant) -> Association {
    let mut a = Association {
        state: AssociationState::Established,
        use_forward_tsn: true,
        source_partial_reliability: true,
        my_next_tsn: input.initial,
        cumulative_tsn_ack_point: input.initial.wrapping_sub(1),
        advanced_peer_tsn_ack_point: input.initial.wrapping_sub(1),
        cwnd: 10000,
        rwnd: 10000,
        mtu: 1228,
        ..Default::default()
    };
    a.create_stream(0, false, PayloadProtocolIdentifier::Binary)
        .unwrap()
        .set_reliability_params(false, ReliabilityType::Reliable, 0)
        .unwrap();
    a.outbound_queue.push(vec![ChunkPayloadData {
        beginning_fragment: true,
        ending_fragment: true,
        stream_identifier: 0,
        payload_type: PayloadProtocolIdentifier::Binary,
        user_data: Bytes::from(vec![1, 2]),
        ..Default::default()
    }]);
    a.send_next_data_chunk(now - Duration::from_nanos(input.head_age))
        .unwrap();
    let c = a.outbound_queue.get_mut(input.initial).unwrap();
    if input.retried {
        c.nsent = 2;
    }
    if input.abandoned {
        c.abandoned = true;
    }
    a.source_rack_ack.highest_delivered_tsn = input.highest;
    a.source_rack_ack.reordering_seen = input.reordered;
    a.source_rack_ack.keep_inflated = input.keep;
    a.source_rack_ack.min_rtt = Duration::from_nanos(input.prior_min);
    a.source_rack_ack.reo = Duration::from_nanos(input.prior_reo);
    a.in_fast_recovery = input.recovery;
    for &(age, value) in input.samples {
        a.source_rack_ack
            .rtt(now - Duration::from_nanos(age), Duration::from_nanos(value));
    }
    if input.t3 {
        a.timers.start(Timer::T3RTX, now, 1000);
    }
    a
}
fn observe(input: &Input, a: &mut Association, now: Instant) {
    let delivery = SourceRackDelivery {
        newest: input.delivered.then(|| {
            (
                now - Duration::from_nanos(input.delivered_age),
                input.delivered_tsn,
            )
        }),
        next_measure: a
            .source_rack_ack
            .min_tsn_measure
            .unwrap_or(a.min_tsn2measure_rtt),
        ..SourceRackDelivery::default()
    };
    let sack = ChunkSelectiveAck {
        duplicate_tsn: if input.duplicate {
            vec![input.initial.wrapping_add(1)]
        } else {
            vec![]
        },
        ..Default::default()
    };
    a.source_rack_after_ack(delivery, &sack, now);
}
#[test]
#[ignore = "requires actual fixed-Pion GO_SCTP_ADAPTIVE_RACK_OUTPUT TSV20"]
fn actual_go_adaptive_rack_state_and_deadline_match_native() {
    let text =
        std::fs::read_to_string(std::env::var_os("GO_SCTP_ADAPTIVE_RACK_OUTPUT").unwrap()).unwrap();
    let mut seen = std::collections::BTreeSet::new();
    for line in text.lines() {
        let id = line.split('\t').next().unwrap();
        assert!(seen.insert(id.to_owned()), "duplicate actual source id");
        let input = INPUTS.iter().find(|row| row.id == id).unwrap();
        let now = Instant::now();
        let mut a = prepare(input, now);
        observe(input, &mut a, now);
        let rack = &a.source_rack_ack;
        let head = a.outbound_queue.get(input.initial).unwrap();
        let deadline = rack
            .deadline
            .map_or(-1i128, |at| at.duration_since(now).as_nanos() as i128);
        let actual = format!(
            "{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{:016x}\t{}",
            id,
            rack.min_rtt.as_nanos(),
            rack.reo.as_nanos(),
            rack.keep_inflated,
            rack.highest_delivered_tsn,
            rack.reordering_seen,
            head.retransmit,
            rack.order.contains(&input.initial),
            deadline,
            rack.srtt_ms.to_bits(),
            rack.window_len()
        );
        assert_eq!(actual, line, "actual Pion adaptive RACK {id}");
    }
    assert_eq!(seen.len(), INPUTS.len());
    assert_eq!(INPUTS.len(), 20);
}
#[test]
fn high_initial_tsn_uses_observed_submillisecond_window_without_resetting_watermark() {
    let input = INPUTS
        .iter()
        .find(|r| r.id == "submillisecond-window")
        .unwrap();
    let now = Instant::now();
    let mut a = prepare(input, now);
    observe(input, &mut a, now);
    assert!(a.source_rack_ack.reordering_seen);
    assert_eq!(a.source_rack_ack.highest_delivered_tsn, 0);
    assert_eq!(a.source_rack_ack.reo, Duration::from_micros(100));
    assert!(a.outbound_queue.get(input.initial).unwrap().retransmit);
    assert!(!a.get_data_packets_to_retransmit(now).is_empty());
}
#[test]
fn actual_rack_deadline_is_polled_and_not_fired_early_or_rearmed_by_polling() {
    let input = INPUTS
        .iter()
        .find(|r| r.id == "high-initial-insufficient-gap")
        .unwrap();
    let now = Instant::now();
    let mut a = prepare(input, now);
    observe(input, &mut a, now);
    assert!(!a.outbound_queue.get(input.initial).unwrap().retransmit);
    let deadline = a.source_rack_ack.deadline.unwrap();
    assert_eq!(a.poll_timeout(), Some(deadline));
    a.handle_timeout(deadline - Duration::from_nanos(1));
    assert_eq!(a.source_rack_ack.deadline, Some(deadline));
    a.handle_timeout(deadline);
    assert!(a.source_rack_ack.deadline.is_none());
    assert!(
        !a.outbound_queue.get(input.initial).unwrap().retransmit,
        "source RACK timeout reuses delivered-time threshold, not fabricated elapsed-age loss"
    );
    a.handle_timeout(deadline + Duration::from_nanos(1));
    assert!(a.source_rack_ack.deadline.is_none());
}
#[test]
fn adaptive_state_and_deadline_retire_with_actual_association_close() {
    let input = INPUTS
        .iter()
        .find(|r| r.id == "high-initial-insufficient-gap")
        .unwrap();
    let now = Instant::now();
    let mut a = prepare(input, now);
    observe(input, &mut a, now);
    a.close().unwrap();
    assert!(a.source_rack_ack.deadline.is_none());
    assert!(a.source_rack_ack.order.is_empty());
    assert_eq!(a.source_rack_ack.window_len(), 0);
}

#[test]
#[ignore = "requires actual original-Pion GO_SCTP_ACK_RTT_OUTPUT TSV12"]
fn actual_go_real_ack_karn_and_next_measure_history_matches_native() {
    use crate::chunk::chunk_selective_ack::GapAckBlock;
    let text =
        std::fs::read_to_string(std::env::var_os("GO_SCTP_ACK_RTT_OUTPUT").unwrap()).unwrap();
    assert_eq!(
        text.lines().count(),
        12,
        "all actual rows including duplicate detection"
    );
    let expected: std::collections::BTreeMap<_, _> = text
        .lines()
        .map(|line| {
            let f: Vec<_> = line.split('\t').collect();
            assert_eq!(f.len(), 7);
            (format!("{}\t{}", f[0], f[1]), line.to_owned())
        })
        .collect();
    assert_eq!(expected.len(), 12);
    let mut count = 0;
    for (id, initial) in [("low", 1u32), ("high", 0x90000000), ("wrap", 0xffffffff)] {
        let now = Instant::now();
        let mut a = Association {
            state: AssociationState::Established,
            use_forward_tsn: true,
            source_partial_reliability: true,
            my_next_tsn: initial,
            min_tsn2measure_rtt: initial,
            cumulative_tsn_ack_point: initial.wrapping_sub(1),
            advanced_peer_tsn_ack_point: initial.wrapping_sub(1),
            cwnd: 10000,
            rwnd: 10000,
            mtu: 1228,
            ..Default::default()
        };
        a.create_stream(0, false, PayloadProtocolIdentifier::Binary)
            .unwrap()
            .set_reliability_params(false, ReliabilityType::Reliable, 0)
            .unwrap();
        a.outbound_queue.push(
            (0..5)
                .map(|i| ChunkPayloadData {
                    beginning_fragment: true,
                    ending_fragment: true,
                    stream_identifier: 0,
                    payload_type: PayloadProtocolIdentifier::Binary,
                    user_data: Bytes::from(vec![i, 2]),
                    ..Default::default()
                })
                .collect(),
        );
        a.send_next_data_chunk(now).unwrap();
        a.send_next_data_chunk(now + Duration::from_millis(1))
            .unwrap();
        a.timers.start(Timer::T3RTX, now, 1000);
        let mut sack = ChunkSelectiveAck {
            cumulative_tsn_ack: initial.wrapping_sub(1),
            advertised_receiver_window_credit: 10000,
            gap_ack_blocks: vec![GapAckBlock { start: 2, end: 2 }],
            duplicate_tsn: vec![],
        };
        let mut check = |stage: &str, a: &Association| {
            let r = &a.source_rack_ack;
            let actual = format!(
                "{id}\t{stage}\t{:016x}\t{}\t{}\t{}\t{}",
                r.srtt_ms.to_bits(),
                r.window_len(),
                r.min_tsn_measure.unwrap(),
                r.min_rtt.as_nanos(),
                r.reordering_seen
            );
            assert_eq!(expected.get(&format!("{id}\t{stage}")).unwrap(), &actual);
            count += 1;
        };
        a.handle_sack(&sack, now + Duration::from_millis(2))
            .unwrap();
        check("first-original", &a);
        a.handle_sack(&sack, now + Duration::from_micros(2500))
            .unwrap();
        check("duplicate-ack", &a);
        let packets = a.get_data_packets_to_retransmit(now + Duration::from_millis(3));
        assert!(!packets.is_empty());
        assert_eq!(a.outbound_queue.get(initial).unwrap().nsent, 2);
        sack = ChunkSelectiveAck {
            cumulative_tsn_ack: initial,
            advertised_receiver_window_credit: 10000,
            ..Default::default()
        };
        a.handle_sack(&sack, now + Duration::from_millis(4))
            .unwrap();
        check("retransmitted-ack-karn", &a);
        a.send_next_data_chunk(now + Duration::from_millis(4))
            .unwrap();
        sack.gap_ack_blocks = vec![GapAckBlock { start: 1, end: 2 }];
        a.handle_sack(&sack, now + Duration::from_millis(7))
            .unwrap();
        check("next-original", &a);
    }
    assert_eq!(count, 12);
}

// Additive test inside adaptive_rack_tests.rs; no expected outputs copied from Go.
#[test]
#[ignore = "requires actual original-Pion GO_SCTP_FRACTIONAL_ACK_OUTPUT TSV24"]
fn actual_go_fractional_ack_history_matches_every_srtt_bit() {
    let text = std::fs::read_to_string(std::env::var_os("GO_SCTP_FRACTIONAL_ACK_OUTPUT").unwrap())
        .unwrap();
    let expected: std::collections::BTreeMap<_, _> = text
        .lines()
        .map(|l| {
            let f: Vec<_> = l.split('\t').collect();
            assert_eq!(f.len(), 8);
            (format!("{}\t{}", f[0], f[1]), l.to_owned())
        })
        .collect();
    assert_eq!(text.lines().count(), 24);
    assert_eq!(expected.len(), 24);
    let samples = [
        1234567u64, 177777, 999999, 2345678, 333333, 1000001, 7654321, 123457,
    ];
    let mut count = 0;
    for (id, initial) in [("low", 1u32), ("high", 0x90000000), ("wrap", 0xfffffffc)] {
        let now = Instant::now();
        let mut a = Association {
            state: AssociationState::Established,
            use_forward_tsn: true,
            source_partial_reliability: true,
            my_next_tsn: initial,
            min_tsn2measure_rtt: initial,
            cumulative_tsn_ack_point: initial.wrapping_sub(1),
            advanced_peer_tsn_ack_point: initial.wrapping_sub(1),
            cwnd: 10000,
            rwnd: 10000,
            mtu: 1228,
            ..Default::default()
        };
        a.create_stream(0, false, PayloadProtocolIdentifier::Binary)
            .unwrap()
            .set_reliability_params(false, ReliabilityType::Reliable, 0)
            .unwrap();
        for (i, ns) in samples.into_iter().enumerate() {
            let sent = now + Duration::from_millis(i as u64 * 20);
            a.outbound_queue.push(vec![ChunkPayloadData {
                beginning_fragment: true,
                ending_fragment: true,
                stream_identifier: 0,
                payload_type: PayloadProtocolIdentifier::Binary,
                user_data: Bytes::from(vec![i as u8, 2]),
                ..Default::default()
            }]);
            let chunk = a.send_next_data_chunk(sent).unwrap();
            assert_eq!(chunk.tsn, initial.wrapping_add(i as u32));
            assert_eq!(chunk.nsent, 1);
            a.handle_sack(
                &ChunkSelectiveAck {
                    cumulative_tsn_ack: initial.wrapping_add(i as u32),
                    advertised_receiver_window_credit: 10000,
                    ..Default::default()
                },
                sent + Duration::from_nanos(ns),
            )
            .unwrap();
            let rack = &a.source_rack_ack;
            let actual = format!(
                "{id}\t{i}\t{ns}\t{:016x}\t{}\t{}\t{}\t{}",
                rack.srtt_ms.to_bits(),
                rack.window_len(),
                rack.min_tsn_measure.unwrap(),
                rack.min_rtt.as_nanos(),
                rack.reordering_seen
            );
            assert_eq!(expected.get(&format!("{id}\t{i}")).unwrap(), &actual);
            count += 1;
        }
    }
    assert_eq!(count, 24);
}
