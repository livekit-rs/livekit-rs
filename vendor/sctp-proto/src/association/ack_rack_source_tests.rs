use super::*;
use crate::chunk::chunk_selective_ack::GapAckBlock;
use alloc::{borrow::ToOwned, string::ToString};
fn fixture(source: bool, kind: ReliabilityType, payload: PayloadProtocolIdentifier) -> Association {
    let mut a = Association {
        state: AssociationState::Established,
        use_forward_tsn: true,
        source_partial_reliability: source,
        my_next_tsn: 1,
        max_payload_size: 2,
        max_send_message_size: 100,
        cwnd: 10000,
        rwnd: 10000,
        mtu: 1228,
        ..Default::default()
    };
    a.create_stream(0, false, payload)
        .unwrap()
        .set_reliability_params(false, kind, 0)
        .unwrap();
    a.outbound_queue.push(
        (0..5)
            .map(|i| ChunkPayloadData {
                beginning_fragment: i == 0,
                ending_fragment: i == 4,
                stream_identifier: 0,
                payload_type: payload,
                user_data: Bytes::from(vec![i, 2]),
                ..Default::default()
            })
            .collect(),
    );
    a
}
fn ack(a: &mut Association, through: u16, now: Instant, duplicate: bool) {
    let sack = ChunkSelectiveAck {
        cumulative_tsn_ack: 0,
        advertised_receiver_window_credit: 10000,
        gap_ack_blocks: vec![GapAckBlock {
            start: 2,
            end: through,
        }],
        duplicate_tsn: if duplicate { vec![2] } else { vec![] },
    };
    a.handle_sack(&sack, now).unwrap();
}
fn history(a: &mut Association, base: Instant, later: Instant) {
    a.send_next_data_chunk(base).unwrap();
    a.send_next_data_chunk(base).unwrap();
    a.timers.start(Timer::T3RTX, base, 1000);
    ack(a, 2, base + Duration::from_millis(100), false);
    assert!(
        !a.outbound_queue.get(1).unwrap().retransmit,
        "equal original send-time cohort is not RACK loss"
    );
    a.send_next_data_chunk(later).unwrap();
    ack(a, 3, base + Duration::from_millis(101), false);
}
#[test]
fn ack_rack_later_cohort_marks_and_repeats_missing_fragment_before_tail() {
    let now = Instant::now();
    let mut a = fixture(
        true,
        ReliabilityType::Rexmit,
        PayloadProtocolIdentifier::Binary,
    );
    history(&mut a, now, now + Duration::from_millis(1));
    assert!(a.outbound_queue.pending_len() > 0);
    assert!(!a.outbound_queue.get(1).unwrap().abandoned);
    assert!(
        a.outbound_queue.get(1).unwrap().retransmit,
        "real later send-cohort ACK must select missing head without three misses"
    );
    assert_eq!(a.outbound_queue.get(1).unwrap().miss_indicator, 2);
    let packets = a.get_data_packets_to_retransmit(now + Duration::from_millis(101));
    assert!(
        !packets.is_empty(),
        "ordinary retransmission actually emits selected DATA"
    );
    assert_eq!(a.outbound_queue.get(1).unwrap().nsent, 2);
    assert_eq!(a.stats.get_num_fast_retrans(), 0);
}
#[test]
fn ack_rack_equal_cohort_and_source_false_do_not_gain_new_recovery() {
    let now = Instant::now();
    for source in [false, true] {
        let mut a = fixture(
            source,
            ReliabilityType::Reliable,
            PayloadProtocolIdentifier::Binary,
        );
        history(
            &mut a,
            now,
            if source {
                now
            } else {
                now + Duration::from_millis(1)
            },
        );
        assert!(!a.outbound_queue.get(1).unwrap().retransmit);
        assert!(
            a.get_data_packets_to_retransmit(now + Duration::from_millis(101))
                .is_empty()
        );
    }
}

#[test]
#[ignore = "requires actual fixed-Pion GO_SCTP_ACK_RACK_OUTPUT TSV"]
fn actual_go_ack_rack_zero_window_history_matches_native() {
    let text =
        std::fs::read_to_string(std::env::var_os("GO_SCTP_ACK_RACK_OUTPUT").unwrap()).unwrap();
    let mut seen = std::collections::BTreeSet::new();
    for line in text.lines() {
        let fields: Vec<_> = line.split('\t').collect();
        assert_eq!(fields.len(), 7);
        let id = fields[0];
        assert!(seen.insert(id.to_owned()), "duplicate actual source id");
        let third_ns = match id {
            "equal-send-cohort" => 0,
            "earlier-send-cohort" => -1_000_000i64,
            "later-send-cohort"
            | "later-reliable"
            | "later-retransmitted-head"
            | "later-fully-inflight-abandoned"
            | "later-dcep-reliable"
            | "later-duplicate-tsn"
            | "later-reordering-observed" => 1_000_000,
            _ => panic!("unexpected actual source id {id}"),
        };
        let kind = if id == "later-reliable" {
            ReliabilityType::Reliable
        } else {
            ReliabilityType::Rexmit
        };
        let payload = if id == "later-dcep-reliable" {
            PayloadProtocolIdentifier::Dcep
        } else {
            PayloadProtocolIdentifier::Binary
        };
        let mut a = fixture(true, kind, payload);
        let base = Instant::now();
        a.send_next_data_chunk(base).unwrap();
        a.send_next_data_chunk(base).unwrap();
        if id == "later-retransmitted-head" {
            a.outbound_queue.get_mut(1).unwrap().nsent = 2;
        }
        if id == "later-reordering-observed" {
            a.source_rack_ack.highest_delivered_tsn = 100;
        }
        a.timers.start(Timer::T3RTX, base, 1000);
        ack(&mut a, 2, base + Duration::from_millis(100), false);
        let first = a.outbound_queue.get(1).unwrap().retransmit;
        let third = if third_ns < 0 {
            base - Duration::from_nanos(third_ns.unsigned_abs())
        } else {
            base + Duration::from_nanos(third_ns as u64)
        };
        a.send_next_data_chunk(third).unwrap();
        if id == "later-fully-inflight-abandoned" {
            while a.outbound_queue.pending_len() > 0 {
                a.send_next_data_chunk(third).unwrap();
            }
        }
        ack(
            &mut a,
            3,
            base + Duration::from_millis(101),
            id == "later-duplicate-tsn",
        );
        let second = a.outbound_queue.get(1).unwrap().retransmit;
        let packets = a.get_data_packets_to_retransmit(base + Duration::from_millis(101));
        let mut tsns = vec![];
        for p in packets {
            let b = p.marshal().unwrap();
            let mut offset = 12;
            while offset + 4 <= b.len() {
                let length = u16::from_be_bytes([b[offset + 2], b[offset + 3]]) as usize;
                assert!(length >= 4 && offset + length <= b.len());
                if b[offset] == 0 {
                    tsns.push(
                        u32::from_be_bytes(b[offset + 4..offset + 8].try_into().unwrap())
                            .to_string(),
                    );
                }
                offset += (length + 3) & !3;
            }
        }
        let head = a.outbound_queue.get(1).unwrap();
        let actual = format!(
            "{id}\t{first}\t{second}\t{}\t{}\t{}\t{}",
            head.nsent,
            head.abandoned,
            a.source_rack_ack.reordering_seen,
            tsns.join(",")
        );
        assert_eq!(actual, line, "actual Pion ACK history {id}");
    }
    assert_eq!(seen.len(), 9, "all frozen source inputs must be observed");
}
