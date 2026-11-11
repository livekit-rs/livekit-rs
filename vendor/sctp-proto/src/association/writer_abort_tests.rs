// Regression for the real receive-capacity consumer, using only pre-PTO APIs.
// Adapted from existing association_test.rs capacity helpers; no synthetic event.
use super::*;
fn capacity_fragment(tsn: u32) -> ChunkPayloadData {
    ChunkPayloadData {
        tsn,
        stream_identifier: 1,
        beginning_fragment: tsn == 1,
        ending_fragment: false,
        user_data: Bytes::from(vec![0; 1024]),
        ..Default::default()
    }
}
fn receive_fragment(a: &mut Association, chunk: ChunkPayloadData, now: Instant) -> Result<()> {
    let raw = a.create_packet(vec![Box::new(chunk)]).marshal()?;
    a.handle_event(AssociationEvent(AssociationEventInner::Datagram(
        Transmit {
            now,
            remote: a.remote_addr(),
            ecn: None,
            local_ip: None,
            payload: Payload::PartialDecode(crate::packet::PartialDecode::unmarshal(&raw)?),
        },
    )));
    Ok(())
}
#[test]
fn source_writer_wake_preserves_real_receive_capacity_abort() -> Result<()> {
    for source in [false, true] {
        let now = Instant::now();
        let mut a = Association::new(
            None,
            Arc::new(
                TransportConfig::default()
                    .with_max_receive_buffer_size(32 * 1024)
                    .with_source_partial_reliability(source),
            ),
            1400,
            0,
            SocketAddr::from_str("0.0.0.0:0").unwrap(),
            None,
            now,
        );
        a.set_state(AssociationState::Established);
        a.peer_last_tsn = 0;
        for tsn in 1..=32 {
            receive_fragment(&mut a, capacity_fragment(tsn), now)?;
        }
        assert!(a.is_closed());
        assert!(core::iter::from_fn(|| a.poll()).any(|e| matches!(
            e,
            Event::AssociationLost {
                reason: AssociationError::ReceiveBufferExhausted
            }
        )));
        let transmit = a
            .poll_transmit(now)
            .expect("actual terminal OUT_OF_RESOURCE ABORT must survive close/wake");
        let Payload::RawEncode(packets) = transmit.payload else {
            panic!("real serialized SCTP required")
        };
        assert_eq!(packets.len(), 1);
        let packet = crate::packet::PartialDecode::unmarshal(&packets[0])?.finish()?;
        let abort = packet.chunks[0]
            .as_any()
            .downcast_ref::<ChunkAbort>()
            .unwrap();
        assert_eq!(abort.error_causes.len(), 1);
        assert_eq!(abort.error_causes[0].code, crate::chunk::OUT_OF_RESOURCE);
        assert!(
            a.poll_transmit(now).is_none(),
            "terminal output must not repeat or open DATA budgets"
        );
        assert_eq!(a.get_my_receiver_window_credit(), 32 * 1024);
    }
    Ok(())
}
