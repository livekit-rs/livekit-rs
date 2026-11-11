use super::*;
use crate::chunk::chunk_selective_ack::GapAckBlock;
#[test]
fn adaptive_rack_high_initial_tsn_real_ack_history_recovers_pending_head() {
    let now = Instant::now();
    let initial = 0x9000_0000u32;
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
        .set_reliability_params(false, ReliabilityType::Rexmit, 0)
        .unwrap();
    a.outbound_queue.push(
        (0..5)
            .map(|i| ChunkPayloadData {
                beginning_fragment: i == 0,
                ending_fragment: i == 4,
                stream_identifier: 0,
                payload_type: PayloadProtocolIdentifier::Binary,
                user_data: Bytes::from(vec![i, 2]),
                ..Default::default()
            })
            .collect(),
    );
    a.send_next_data_chunk(now).unwrap();
    a.send_next_data_chunk(now).unwrap();
    a.timers.start(Timer::T3RTX, now, 1000);
    let mut sack = ChunkSelectiveAck {
        cumulative_tsn_ack: initial.wrapping_sub(1),
        advertised_receiver_window_credit: 10000,
        gap_ack_blocks: vec![GapAckBlock { start: 2, end: 2 }],
        duplicate_tsn: vec![],
    };
    a.handle_sack(&sack, now + Duration::from_millis(1))
        .unwrap();
    assert!(
        a.source_rack_ack.reordering_seen,
        "preserve actual source high-watermark comparison"
    );
    assert!(
        !a.outbound_queue.get(initial).unwrap().retransmit,
        "equal cohort is not lost"
    );
    a.send_next_data_chunk(now + Duration::from_millis(1))
        .unwrap();
    sack.gap_ack_blocks[0].end = 3;
    a.handle_sack(&sack, now + Duration::from_millis(2))
        .unwrap();
    assert!(a.outbound_queue.pending_len() > 0);
    assert!(!a.outbound_queue.get(initial).unwrap().abandoned);
    assert!(
        a.outbound_queue.get(initial).unwrap().retransmit,
        "real ACK of later cohort must not be permanently declined after source-observed reordering"
    );
    assert!(
        !a.get_data_packets_to_retransmit(now + Duration::from_millis(2))
            .is_empty()
    );
    assert_eq!(a.outbound_queue.get(initial).unwrap().nsent, 2);
    assert_eq!(a.stats.get_num_fast_retrans(), 0);
}
