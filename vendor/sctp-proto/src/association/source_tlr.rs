// SPDX-FileCopyrightText: 2026 The Pion community <https://pion.ly>
// SPDX-License-Identifier: MIT
// Port of fixed Pion SCTP v1.12.0 association.go DATA PTO and adaptive TLR.
// Keep native/default policy and its integer-ms RTO unchanged.
use super::*;

#[derive(Debug)]
pub(super) struct SourceTlr {
    pub pto_deadline: Option<Instant>,
    pub active: bool,
    pub first_rtt: bool,
    pub had_additional_loss: bool,
    pub started: Option<Instant>,
    pub end_tsn: u32,
    pub first_units: i64,
    pub later_units: i64,
    pub good_ops: u32,
}
impl Default for SourceTlr {
    fn default() -> Self {
        Self {
            pto_deadline: None,
            active: false,
            first_rtt: false,
            had_additional_loss: false,
            started: None,
            end_tsn: 0,
            first_units: 16,
            later_units: 8,
            good_ops: 0,
        }
    }
}
#[derive(Debug)]
pub(super) struct SourceBurst {
    pub remaining: i64,
    pub consumed: bool,
}
impl SourceBurst {
    pub(super) fn allow(&mut self, bytes: u32) -> bool {
        let need = i64::from(bytes) * 4;
        if self.consumed && self.remaining < need {
            return false;
        }
        self.remaining = (self.remaining - need).max(0);
        self.consumed = true;
        true
    }
}
impl SourceTlr {
    pub fn schedule(&mut self, now: Instant, inflight: usize, srtt: Option<Duration>) {
        self.pto_deadline = if inflight == 0 {
            None
        } else {
            let pto = srtt.map_or(Duration::from_secs(1), |srtt| {
                let extra = if inflight == 1 {
                    Duration::from_millis(200)
                } else {
                    Duration::from_millis(2)
                };
                srtt.checked_mul(2)
                    .and_then(|d| d.checked_add(extra))
                    .expect("source PTO duration")
            });
            now.checked_add(pto)
        };
    }
    pub fn begin(&mut self, now: Instant, end_tsn: u32) {
        self.active = true;
        self.first_rtt = true;
        self.had_additional_loss = false;
        self.started = Some(now);
        self.end_tsn = end_tsn;
    }
    fn phase(&mut self, now: Instant, srtt: Option<Duration>) {
        if self.active
            && self.first_rtt
            && self.started.is_some_and(|start| {
                now.saturating_duration_since(start) >= srtt.unwrap_or(Duration::from_secs(1))
            })
        {
            self.first_rtt = false;
        }
    }
    pub fn additional_loss(&mut self, now: Instant, srtt: Option<Duration>) {
        if !self.active {
            return;
        }
        self.phase(now, srtt);
        self.had_additional_loss = true;
        self.good_ops = 0;
        if self.first_rtt {
            self.first_units = (self.first_units - 4).max(8);
        } else {
            self.later_units = (self.later_units - 1).max(5);
        }
    }
    pub fn finish(&mut self, progress: bool, cumulative: u32) {
        if !self.active {
            return;
        }
        if self.first_rtt && progress {
            self.first_rtt = false;
        }
        if sna32gte(cumulative, self.end_tsn) {
            if !self.had_additional_loss {
                self.good_ops += 1;
                if self.good_ops >= 16 {
                    self.first_units = 16;
                    self.later_units = 8;
                    self.good_ops = 0;
                }
            } else {
                self.good_ops = 0;
            }
            self.active = false;
            self.first_rtt = false;
            self.had_additional_loss = false;
            self.end_tsn = 0;
        }
    }
    pub fn budget(
        &mut self,
        now: Instant,
        srtt: Option<Duration>,
        mtu: u32,
    ) -> Option<SourceBurst> {
        if !self.active {
            return None;
        }
        self.phase(now, srtt);
        let units = if self.first_rtt {
            self.first_units
        } else {
            self.later_units
        };
        Some(SourceBurst {
            remaining: units * i64::from(mtu),
            consumed: false,
        })
    }
}
impl Association {
    pub(super) fn source_schedule_pto(&mut self, now: Instant) {
        self.source_tlr.schedule(
            now,
            self.outbound_queue.inflight_len(),
            self.source_rack_ack.srtt_duration(),
        );
    }
    pub(super) fn source_on_pto(&mut self, now: Instant) {
        let count = self.outbound_queue.inflight_len();
        if count == 0 {
            self.source_tlr.pto_deadline = None;
            // Active HEARTBEAT/ACK RTT seeding is the next, separately gated increment.
            // No DATA/TLR is fabricated on this otherwise unreachable idle-data branch.
            return;
        }
        if self.source_tlr.active {
            self.source_tlr
                .additional_loss(now, self.source_rack_ack.srtt_duration());
        } else {
            self.source_begin_tlr(now);
        }
        if self.outbound_queue.pending_len() != 0 {
            self.awake_write_loop();
            return;
        }
        let mut latest = None;
        for i in 0..count {
            let tsn = self
                .cumulative_tsn_ack_point
                .wrapping_add(i as u32)
                .wrapping_add(1);
            let Some(c) = self.outbound_queue.get(tsn) else {
                break;
            };
            if !c.acked && !c.abandoned {
                latest = Some(tsn);
            }
        }
        if let Some(tsn) = latest {
            let c = self.outbound_queue.get_mut(tsn).unwrap();
            if !c.retransmit {
                c.retransmit = true;
                self.awake_write_loop();
            }
        }
    }
    pub(super) fn source_begin_tlr(&mut self, now: Instant) {
        let mut end = self.cumulative_tsn_ack_point;
        for i in 0..self.outbound_queue.inflight_len() {
            let tsn = self
                .cumulative_tsn_ack_point
                .wrapping_add(i as u32)
                .wrapping_add(1);
            if self.outbound_queue.get(tsn).is_none() {
                break;
            }
            end = tsn;
        }
        self.source_tlr.begin(now, end);
    }
    /// One tracker per selection stage, one budget shared by the whole source gather.
    pub(super) fn source_reserve_chunk(&mut self, chunk_bytes: u32, in_packet: &mut u32) -> bool {
        if *in_packet != 0 && *in_packet + chunk_bytes > self.mtu {
            *in_packet = 0;
        }
        let add = chunk_bytes
            + if *in_packet == 0 {
                COMMON_HEADER_SIZE
            } else {
                0
            };
        if *in_packet == 0 && add > self.mtu {
            return false;
        }
        if self
            .source_burst
            .as_mut()
            .is_some_and(|budget| !budget.allow(add))
        {
            return false;
        }
        if *in_packet == 0 {
            *in_packet = COMMON_HEADER_SIZE;
        }
        *in_packet += chunk_bytes;
        true
    }
    pub(super) fn source_bundle_packets(&self, selected: Vec<ChunkPayloadData>) -> Vec<Packet> {
        let mut packets = vec![];
        let mut chunks: Vec<Box<dyn Chunk + Send + Sync>> = vec![];
        let mut used = COMMON_HEADER_SIZE;
        for chunk in selected {
            let size = (DATA_CHUNK_HEADER_SIZE + chunk.user_data.len() as u32 + 3) & !3;
            if used + size > self.mtu {
                if !chunks.is_empty() {
                    packets.push(self.create_packet(core::mem::take(&mut chunks)));
                }
                used = COMMON_HEADER_SIZE;
            }
            used += size;
            chunks.push(Box::new(chunk));
        }
        if !chunks.is_empty() {
            packets.push(self.create_packet(chunks));
        }
        packets
    }
}
