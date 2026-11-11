// SPDX-FileCopyrightText: 2026 The Pion community <https://pion.ly>
// SPDX-License-Identifier: MIT
// Pion SCTP v1.12.0 processShutdownAcknowledgement/handleShutdown and drain.
use super::*;
impl Association {
    pub(super) fn source_has_pending_or_inflight(&self) -> bool {
        self.outbound_queue.pending_len() != 0 || self.outbound_queue.inflight_len() != 0
    }
    pub(super) fn source_validate_acknowledgement(&self, d: &ChunkSelectiveAck) -> Result<()> {
        // Original source validates the owned cumulative endpoints and every
        // gap endpoint before any pop/callback/RTT mutation. Queue TSNs are contiguous.
        if sna32lt(self.cumulative_tsn_ack_point, d.cumulative_tsn_ack)
            && (self
                .outbound_queue
                .get(self.cumulative_tsn_ack_point.wrapping_add(1))
                .is_none()
                || self.outbound_queue.get(d.cumulative_tsn_ack).is_none())
        {
            return Err(Error::ErrInflightQueueTsnPop);
        }
        for gap in &d.gap_ack_blocks {
            if gap.start == 0
                || gap.start > gap.end
                || self
                    .outbound_queue
                    .get(d.cumulative_tsn_ack.wrapping_add(u32::from(gap.start)))
                    .is_none()
                || self
                    .outbound_queue
                    .get(d.cumulative_tsn_ack.wrapping_add(u32::from(gap.end)))
                    .is_none()
            {
                return Err(Error::ErrTsnRequestNotExist);
            }
        }
        Ok(())
    }
    pub(super) fn source_handle_shutdown(
        &mut self,
        c: &ChunkShutdown,
        now: Instant,
    ) -> Result<Vec<Packet>> {
        if self.source_shutdown_complete_pending {
            return Ok(vec![]);
        }
        let state = self.state();
        if state == AssociationState::ShutdownAckSent {
            self.source_retransmit_shutdown_ack();
            return Ok(vec![]);
        }
        if state == AssociationState::ShutdownSent {
            self.timers.stop(Timer::T2Shutdown);
            self.will_send_shutdown = false;
            self.will_send_shutdown_ack = true;
            self.set_state(AssociationState::ShutdownAckSent);
            self.awake_write_loop();
            return Ok(vec![]);
        }
        if !matches!(
            state,
            AssociationState::Established
                | AssociationState::ShutdownPending
                | AssociationState::ShutdownReceived
        ) {
            return Ok(vec![]);
        }
        let enters = matches!(
            state,
            AssociationState::Established | AssociationState::ShutdownPending
        );
        if enters {
            self.set_state(AssociationState::ShutdownReceived);
        }
        let previous = self.cumulative_tsn_ack_point;
        let result = (|| {
            // This is normalization of the real SHUTDOWN carried cumulative
            // ACK, exactly like source; no fabricated wire SACK or a_rwnd.
            if !sna32gt(self.cumulative_tsn_ack_point, c.cumulative_tsn_ack) {
                let d = ChunkSelectiveAck {
                    cumulative_tsn_ack: c.cumulative_tsn_ack,
                    ..Default::default()
                };
                let (delivery, htna, advanced) = self.process_acknowledged_data(&d, now)?;
                self.finish_acknowledged_data(&d, htna, advanced, now)?;
                let progress = advanced || delivery.newest.is_some();
                self.source_rack_after_ack(delivery, &d, now);
                self.source_tlr
                    .finish(progress, self.cumulative_tsn_ack_point);
            }
            Ok(())
        })();
        if result.is_err() {
            if self.cumulative_tsn_ack_point == previous {
                self.set_state(state);
            }
            return result.map(|()| vec![]);
        }
        if self.source_has_pending_or_inflight() {
            self.set_state(AssociationState::ShutdownReceived);
            if self.outbound_queue.pending_len() != 0 {
                self.awake_write_loop();
            }
        } else {
            self.will_send_shutdown_ack = true;
            self.set_state(AssociationState::ShutdownAckSent);
            self.awake_write_loop();
        }
        Ok(vec![])
    }
    fn source_retransmit_shutdown_ack(&mut self) {
        if self.source_shutdown_complete_pending {
            return;
        }
        self.timers.stop(Timer::T2Shutdown);
        self.will_send_shutdown = false;
        self.will_send_shutdown_ack = true;
        self.awake_write_loop();
    }
    pub(super) fn source_handle_shutdown_ack(
        &mut self,
        _: &ChunkShutdownAck,
    ) -> Result<Vec<Packet>> {
        if matches!(
            self.state(),
            AssociationState::ShutdownSent | AssociationState::ShutdownAckSent
        ) {
            self.timers.stop(Timer::T2Shutdown);
            self.will_send_shutdown = false;
            self.will_send_shutdown_ack = false;
            self.source_shutdown_complete_pending = true;
            self.will_send_shutdown_complete = true;
            self.awake_write_loop();
        }
        Ok(vec![])
    }
}
