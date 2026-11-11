use super::*;
use alloc::borrow::ToOwned;
fn association(source: bool, now: Instant) -> Association {
    let mut a = Association {
        state: AssociationState::Established,
        source_partial_reliability: source,
        use_forward_tsn: true,
        my_next_tsn: 1,
        min_tsn2measure_rtt: 1,
        cumulative_tsn_ack_point: 0,
        advanced_peer_tsn_ack_point: 0,
        cwnd: 100000,
        rwnd: 100000,
        mtu: 1228,
        ..Default::default()
    };
    a.create_stream(0, false, PayloadProtocolIdentifier::Binary)
        .unwrap()
        .set_reliability_params(false, ReliabilityType::Reliable, 0)
        .unwrap();
    queue(&mut a, 1);
    a.send_next_data_chunk(now).unwrap();
    a.handle_sack(
        &ChunkSelectiveAck {
            cumulative_tsn_ack: 1,
            advertised_receiver_window_credit: 100000,
            ..Default::default()
        },
        now + Duration::from_millis(2),
    )
    .unwrap();
    a
}
fn queue(a: &mut Association, n: usize) {
    a.outbound_queue.push(
        (0..n)
            .map(|i| ChunkPayloadData {
                beginning_fragment: true,
                ending_fragment: true,
                stream_identifier: 0,
                payload_type: PayloadProtocolIdentifier::Binary,
                user_data: Bytes::from(vec![i as u8; 1200]),
                ..Default::default()
            })
            .collect(),
    );
    a.awake_write_loop();
}
fn tsns(payload: &Payload) -> Vec<u32> {
    let Payload::RawEncode(packets) = payload else {
        panic!("actual serialized packets required")
    };
    let mut out = vec![];
    for b in packets {
        let mut off = 12;
        while off + 4 <= b.len() {
            let n = u16::from_be_bytes([b[off + 2], b[off + 3]]) as usize;
            assert!(n >= 4 && off + n <= b.len());
            if b[off] == 0 {
                out.push(u32::from_be_bytes(b[off + 4..off + 8].try_into().unwrap()));
            }
            off += (n + 3) & !3;
        }
    }
    out
}
#[test]
fn pto_real_ack_then_pending_data_consumes_one_coalesced_source_writer_burst() {
    let now = Instant::now();
    let mut a = association(true, now);
    a.cwnd = 1200;
    queue(&mut a, 20);
    let sent = now + Duration::from_millis(3);
    let first = a.poll_transmit(sent).unwrap();
    assert_eq!(tsns(&first.payload), vec![2]);
    let deadline = a.poll_timeout().unwrap();
    assert_eq!(deadline, sent + Duration::from_millis(204));
    assert!(
        a.poll_transmit(deadline - Duration::from_nanos(1))
            .is_none()
    );
    a.handle_timeout(deadline - Duration::from_nanos(1));
    assert!(!a.source_tlr.active);
    a.handle_timeout(deadline);
    assert!(a.source_tlr.active);
    assert!(a.source_tlr.first_rtt);
    assert!(
        !a.outbound_queue.get(2).unwrap().retransmit,
        "pending DATA takes precedence over a PTO retransmission probe"
    );
    a.cwnd = 100000;
    let burst = a.poll_transmit(deadline).unwrap();
    assert_eq!(tsns(&burst.payload), vec![3, 4, 5, 6]);
    assert!(
        a.poll_transmit(deadline).is_none(),
        "spurious polling must not replenish a TLR budget"
    );
    assert!(
        a.poll_transmit(deadline + Duration::from_nanos(1))
            .is_none()
    );
    // Real producer wakes coalesce, rather than queuing three extra budgets.
    a.awake_write_loop();
    a.awake_write_loop();
    a.awake_write_loop();
    assert_eq!(
        tsns(&a.poll_transmit(deadline).unwrap().payload),
        vec![7, 8, 9, 10]
    );
    assert!(a.poll_transmit(deadline).is_none());
    assert_eq!(a.outbound_queue.pending_len(), 11);
}
#[test]
fn pto_without_pending_marks_only_latest_live_data_and_cancels_on_real_ack() {
    let now = Instant::now();
    let mut a = association(true, now);
    queue(&mut a, 2);
    let sent = now + Duration::from_millis(3);
    assert_eq!(tsns(&a.poll_transmit(sent).unwrap().payload), vec![2, 3]);
    let deadline = a.poll_timeout().unwrap();
    assert_eq!(deadline, sent + Duration::from_millis(6));
    a.handle_timeout(deadline);
    assert!(!a.outbound_queue.get(2).unwrap().retransmit);
    assert!(a.outbound_queue.get(3).unwrap().retransmit);
    assert_eq!(tsns(&a.poll_transmit(deadline).unwrap().payload), vec![3]);
    assert_eq!(a.outbound_queue.get(3).unwrap().nsent, 2);
    a.handle_sack(
        &ChunkSelectiveAck {
            cumulative_tsn_ack: 3,
            advertised_receiver_window_credit: 100000,
            ..Default::default()
        },
        deadline + Duration::from_millis(1),
    )
    .unwrap();
    assert!(!a.source_tlr.active);
    assert!(a.source_tlr.pto_deadline.is_none());
}
#[test]
fn source_writer_does_not_gate_native_default_or_legacy_fast_window_policy() {
    let now = Instant::now();
    let mut a = association(false, now);
    queue(&mut a, 2);
    a.source_writer_pending.store(false, Ordering::Relaxed);
    assert_eq!(tsns(&a.poll_transmit(now).unwrap().payload), vec![2, 3]);
}
#[test]
fn tlr_budget_is_shared_by_ordinary_new_and_fast_selection_in_source_order() {
    let now = Instant::now();
    let mut a = association(true, now);
    queue(&mut a, 4);
    for _ in 0..4 {
        a.send_next_data_chunk(now).unwrap();
    }
    a.outbound_queue.get_mut(2).unwrap().retransmit = true;
    a.outbound_queue.get_mut(3).unwrap().retransmit = true;
    a.outbound_queue.get_mut(4).unwrap().miss_indicator = 3;
    a.outbound_queue.get_mut(5).unwrap().miss_indicator = 3;
    a.will_retransmit_fast = true;
    a.fast_rtx_wnd = 10000;
    queue(&mut a, 8);
    a.source_tlr.begin(now, 5);
    let burst = a.poll_transmit(now).unwrap();
    assert_eq!(tsns(&burst.payload), vec![2, 3, 6, 7]);
    assert_eq!(
        a.outbound_queue.get(4).unwrap().nsent,
        1,
        "fast RTX cannot obtain an independent budget"
    );
    assert_eq!(a.outbound_queue.pending_len(), 6);
    assert!(a.poll_transmit(now).is_none());
}
#[test]
fn tlr_state_and_deadline_retire_on_actual_close() {
    let now = Instant::now();
    let mut a = association(true, now);
    queue(&mut a, 1);
    a.poll_transmit(now).unwrap();
    a.source_tlr.begin(now, 2);
    a.close().unwrap();
    assert!(!a.source_tlr.active);
    assert!(a.source_tlr.pto_deadline.is_none());
    assert!(a.source_burst.is_none());
}

#[test]
#[ignore = "requires actual fixed-Pion GO_SCTP_PTO_TLR_OUTPUT TSV18"]
fn actual_go_pto_tlr_and_shared_writer_budget_match_native() {
    let text =
        std::fs::read_to_string(std::env::var_os("GO_SCTP_PTO_TLR_OUTPUT").unwrap()).unwrap();
    let rows = [
        ("pto-no-rtt-single", 0u64, 1, 0, "pto"),
        ("pto-no-rtt-multi", 0, 2, 0, "pto"),
        ("pto-actual-rtt-single", 1000001, 1, 0, "pto"),
        ("pto-actual-rtt-multi", 1000001, 2, 0, "pto"),
        ("pto-actual-subms-single", 177777, 1, 0, "pto"),
        ("pto-pending-burst", 2000000, 1, 10, "pto"),
        ("phase-before", 2000000, 3, 0, "before"),
        ("phase-equal", 2000000, 3, 0, "equal"),
        ("first-loss-once", 2000000, 3, 0, "first1"),
        ("first-loss-floor", 2000000, 3, 0, "first5"),
        ("later-loss-once", 2000000, 3, 0, "later1"),
        ("later-loss-floor", 2000000, 3, 0, "later5"),
        ("ack-progress-phase", 2000000, 3, 0, "progress"),
        ("finish-clean", 2000000, 3, 0, "finish"),
        ("finish-with-loss", 2000000, 3, 0, "finishloss"),
        ("clean-reset-sixteen", 2000000, 3, 0, "reset16"),
        ("shared-ordinary-new-fast", 2000000, 4, 8, "shared"),
        ("first-exceeds-budget", 2000000, 1, 0, "oversize"),
    ];
    assert_eq!(text.lines().count(), 18);
    let expected: std::collections::BTreeMap<_, _> = text
        .lines()
        .map(|l| {
            assert_eq!(l.split('\t').count(), 15);
            (l.split('\t').next().unwrap().to_owned(), l.to_owned())
        })
        .collect();
    assert_eq!(expected.len(), 18);
    for (id, rtt, inflight, pending, op) in rows {
        let base = Instant::now();
        let mut now = base;
        let mut a = Association {
            state: AssociationState::Established,
            source_partial_reliability: true,
            use_forward_tsn: true,
            my_next_tsn: 1,
            min_tsn2measure_rtt: 1,
            cumulative_tsn_ack_point: 0,
            advanced_peer_tsn_ack_point: 0,
            cwnd: 100000,
            rwnd: 100000,
            mtu: 1228,
            fast_rtx_wnd: 0,
            ..Default::default()
        };
        a.create_stream(0, false, PayloadProtocolIdentifier::Binary)
            .unwrap()
            .set_reliability_params(false, ReliabilityType::Reliable, 0)
            .unwrap();
        if rtt > 0 {
            queue(&mut a, 1);
            a.send_next_data_chunk(now).unwrap();
            now += Duration::from_nanos(rtt);
            a.handle_sack(
                &ChunkSelectiveAck {
                    cumulative_tsn_ack: 1,
                    advertised_receiver_window_credit: 100000,
                    ..Default::default()
                },
                now,
            )
            .unwrap();
        }
        now = base + Duration::from_millis(10);
        queue(&mut a, inflight);
        for _ in 0..inflight {
            a.send_next_data_chunk(now).unwrap();
        }
        queue(&mut a, pending);
        a.source_writer_pending.store(false, Ordering::Relaxed);
        a.source_schedule_pto(now);
        let delta = a
            .source_tlr
            .pto_deadline
            .unwrap()
            .duration_since(now)
            .as_nanos();
        let mut allow = String::new();
        match op {
            "pto" => {
                now = a.source_tlr.pto_deadline.take().unwrap();
                a.source_on_pto(now);
            }
            "shared" => {
                a.fast_rtx_wnd = 10000;
                a.source_begin_tlr(now);
                let start = a.cumulative_tsn_ack_point.wrapping_add(1);
                for i in 0..2 {
                    a.outbound_queue.get_mut(start + i).unwrap().retransmit = true;
                }
                for i in 2..4 {
                    a.outbound_queue.get_mut(start + i).unwrap().miss_indicator = 3;
                }
                a.will_retransmit_fast = true;
                a.awake_write_loop();
            }
            _ => {
                a.source_begin_tlr(now);
                match op {
                    "before" => now += Duration::from_nanos(rtt - 1),
                    "equal" => now += Duration::from_nanos(rtt),
                    "first1" | "first5" | "later1" | "later5" | "finishloss" => {
                        if op.starts_with("later") {
                            now += Duration::from_nanos(rtt);
                        }
                        for _ in 0..if op.ends_with('5') { 5 } else { 1 } {
                            a.source_tlr
                                .additional_loss(now, a.source_rack_ack.srtt_duration());
                        }
                    }
                    "progress" => a.source_tlr.finish(true, a.cumulative_tsn_ack_point),
                    "finish" => {
                        a.cumulative_tsn_ack_point = a.source_tlr.end_tsn;
                        a.source_tlr.finish(true, a.cumulative_tsn_ack_point);
                    }
                    "reset16" => {
                        a.source_tlr.first_units = 8;
                        a.source_tlr.later_units = 5;
                        for _ in 0..16 {
                            a.source_begin_tlr(now);
                            a.cumulative_tsn_ack_point = a.source_tlr.end_tsn;
                            a.source_tlr.finish(true, a.cumulative_tsn_ack_point);
                        }
                    }
                    "oversize" => {
                        let mut b = SourceBurst {
                            remaining: 1,
                            consumed: false,
                        };
                        let first = b.allow(1228);
                        let second = b.allow(1);
                        allow = format!("{first},{second},{},{}", b.remaining, b.consumed);
                    }
                    _ => panic!("unknown source case"),
                }
                if op == "finishloss" {
                    a.cumulative_tsn_ack_point = a.source_tlr.end_tsn;
                    a.source_tlr.finish(true, a.cumulative_tsn_ack_point);
                }
            }
        }
        let budget = a
            .source_tlr
            .budget(now, a.source_rack_ack.srtt_duration(), a.mtu)
            .map_or(0, |b| b.remaining);
        let t = &a.source_tlr;
        let prefix = format!(
            "{id}\t{delta}\t{:016x}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}",
            a.source_rack_ack.srtt_ms.to_bits(),
            t.active,
            t.first_rtt,
            t.had_additional_loss,
            t.end_tsn,
            t.first_units,
            t.later_units,
            t.good_ops,
            budget,
            u8::from(a.source_writer_pending.load(Ordering::Relaxed))
        );
        let mut wire = String::new();
        let mut bytes = 0;
        if op == "pto" || op == "shared" {
            // A direct gather models one already-admitted source writer wake;
            // normal tests above separately verify actual public poll gating.
            a.source_writer_pending.store(false, Ordering::Relaxed);
            let (packets, _) = a.gather_outbound(now);
            bytes = packets.iter().map(|b| b.len()).sum();
            let observed = tsns(&Payload::RawEncode(packets));
            for (i, tsn) in observed.iter().enumerate() {
                if i > 0 {
                    wire.push(',');
                }
                wire.push_str(&format!("{tsn}"));
            }
        }
        let actual = format!("{prefix}\t{wire}\t{bytes}\t{allow}");
        assert_eq!(expected.get(id).unwrap(), &actual, "source PTO/TLR {id}");
    }
}
