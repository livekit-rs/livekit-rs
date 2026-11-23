// Copyright 2023 LiveKit, Inc.
// SPDX-License-Identifier: Apache-2.0
//! Port of `pkg/sfu/bwe/sendsidebwe/packet_info.go` and `packet_tracker.go`.
//!
//! Upstream differences that are deliberate and documented:
//!
//! * Upstream owns the transport-wide sequence number itself and starts it at a
//!   random value in the third quartile of the sequence space
//!   (`rand.Intn(1<<14) + 1<<15`). In this architecture str0m assigns the
//!   wire sequence, so the tracker reconstructs a monotone 64-bit sequence by
//!   forward-unwrapping the observed 16-bit values. Group membership, bitmap
//!   indexing and pruning thresholds only ever compare differences, so the
//!   chosen origin does not change behaviour.
//! * Upstream timestamps are `mono.UnixMicro()` minus a base taken at the first
//!   recorded send. Here the base is an `Instant`, and every recorded timestamp
//!   is the truncated microsecond difference, so the same `0 == absent`
//!   sentinel semantics hold, including upstream's quirk that the very first
//!   sent packet has `send_time == 0` and is therefore skipped by the feedback
//!   walk.

use super::ccutils::ProbeClusterId;
use std::time::Instant;

use super::time::elapsed_micros;

/// `packetInfo`. `None`-like state is represented by the upstream sentinel:
/// `send_time == 0` means never recorded and `recv_time == 0` means not received.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct PacketInfo {
    pub(crate) sequence_number: u64,
    pub(crate) send_time: i64,
    pub(crate) recv_time: i64,
    pub(crate) probe_cluster_id: ProbeClusterId,
    /// Upstream truncates the datagram size to `uint16`, which is replayed.
    pub(crate) size: u16,
    pub(crate) is_rtx: bool,
    pub(crate) is_probe: bool,
}

impl PacketInfo {
    /// `int(pi.size)` as used by the group statistics.
    #[cfg_attr(not(test), allow(dead_code))] // no shipped caller yet: the `#[cfg(test)]` oracle replay executes it; C74-e wires the pacer/prober/allocator consumer
    pub(crate) fn size_i64(&self) -> i64 {
        i64::from(self.size)
    }
}

/// Ring length of `packetTracker.packetInfos [2048]packetInfo`.
const TRACKER_SLOTS: usize = 2_048;

/// The oracle's "no base recorded yet" column value. Go prints
/// `baseSendTime - origin.UnixMicro()` and a zero time is unix 0, while the
/// campaign clock origin is `time.Unix(1000, 0)` = 1e9 microseconds, so the
/// sentinel is exactly minus one billion microseconds.
#[cfg(test)]
pub(crate) const NO_BASE_US: i64 = -1_000_000_000;

/// `packetTracker`.
pub(crate) struct PacketTracker {
    sequence_number: u64,
    last_wire_sequence: Option<u16>,
    base_send_at: Option<Instant>,
    packet_infos: Vec<PacketInfo>,
    base_recv_time: Option<i64>,
    /// Index into `packet_infos`, replacing upstream's `*packetInfo` alias.
    last_recv_index: Option<usize>,
    probe_cluster_id: ProbeClusterId,
    probe_max_sequence_number: u64,
}

impl Default for PacketTracker {
    fn default() -> Self {
        Self::new()
    }
}

impl PacketTracker {
    pub(crate) fn new() -> Self {
        Self {
            // Upstream randomises the origin; see the module header for why this
            // port starts at zero instead.
            sequence_number: 0,
            last_wire_sequence: None,
            base_send_at: None,
            packet_infos: vec![PacketInfo::default(); TRACKER_SLOTS],
            base_recv_time: None,
            last_recv_index: None,
            probe_cluster_id: super::ccutils::PROBE_CLUSTER_ID_INVALID,
            probe_max_sequence_number: 0,
        }
    }

    /// `RecordPacketSendAndGetSequenceNumber`. Returns the reconstructed 64-bit
    /// sequence for the observed wire sequence.
    pub(crate) fn record_packet_send(
        &mut self,
        at: Instant,
        size: usize,
        is_rtx: bool,
        probe_cluster_id: ProbeClusterId,
        is_probe: bool,
        wire_sequence: u16,
    ) -> u64 {
        let base = *self.base_send_at.get_or_insert(at);
        self.sequence_number = self.extend_sequence(wire_sequence);
        let index = Self::slot(wire_sequence);
        self.packet_infos[index] = PacketInfo {
            sequence_number: self.sequence_number,
            send_time: elapsed_micros(at, base),
            recv_time: 0,
            probe_cluster_id,
            size: go_u16(size),
            is_rtx,
            is_probe,
        };
        if Some(index) == self.last_recv_index {
            self.last_recv_index = None;
        }
        let recorded = self.packet_infos[index];
        if self.probe_cluster_id != super::ccutils::PROBE_CLUSTER_ID_INVALID
            && self.probe_cluster_id == recorded.probe_cluster_id
            && recorded.sequence_number > self.probe_max_sequence_number
        {
            self.probe_max_sequence_number = recorded.sequence_number;
        }
        recorded.sequence_number
    }

    /// `BaseSendTimeThreshold`: microseconds since the first send minus the
    /// threshold, or `None` while nothing has been sent.
    pub(crate) fn base_send_time_threshold(&self, now: Instant, threshold_us: i64) -> Option<i64> {
        let base = self.base_send_at?;
        Some(elapsed_micros(now, base).wrapping_sub(threshold_us))
    }

    /// `RecordPacketIndicationFromRemote`. `None` means the tracker has no
    /// record for that sequence number. The `Some` payload is
    /// `(piRecv, sendDelta, recvDelta)`.
    pub(crate) fn record_packet_indication_from_remote(
        &mut self,
        wire_sequence: u16,
        recv_time: i64,
    ) -> Option<(PacketInfo, i64, i64)> {
        let index = Self::slot(wire_sequence);
        if self.packet_infos[index].sequence_number as u16 != wire_sequence {
            return None;
        }
        if recv_time == 0 {
            // Maybe lost, or already received but reported lost in a later report.
            return Some((self.packet_infos[index], 0, 0));
        }
        if self.base_recv_time.is_none() {
            self.base_recv_time = Some(recv_time);
            self.last_recv_index = Some(index);
        }
        let base = self.base_recv_time.unwrap_or(0);
        self.packet_infos[index].recv_time = recv_time.wrapping_sub(base);
        let deltas = match self.last_recv_index {
            Some(last) => (
                self.packet_infos[index]
                    .send_time
                    .wrapping_sub(self.packet_infos[last].send_time),
                self.packet_infos[index]
                    .recv_time
                    .wrapping_sub(self.packet_infos[last].recv_time),
            ),
            None => (0, 0),
        };
        self.last_recv_index = Some(index);
        Some((self.packet_infos[index], deltas.0, deltas.1))
    }

    /// `ProbeClusterStarting`.
    #[cfg_attr(not(test), allow(dead_code))] // no shipped caller yet: the `#[cfg(test)]` oracle replay executes it; C74-e wires the pacer/prober/allocator consumer
    pub(crate) fn probe_cluster_starting(&mut self, probe_cluster_id: ProbeClusterId) {
        self.probe_cluster_id = probe_cluster_id;
    }

    /// `ProbeClusterDone`.
    #[cfg_attr(not(test), allow(dead_code))] // no shipped caller yet: the `#[cfg(test)]` oracle replay executes it; C74-e wires the pacer/prober/allocator consumer
    pub(crate) fn probe_cluster_done(&mut self, probe_cluster_id: ProbeClusterId) {
        if self.probe_cluster_id == probe_cluster_id {
            self.probe_cluster_id = super::ccutils::PROBE_CLUSTER_ID_INVALID;
        }
    }

    /// `ProbeMaxSequenceNumber`.
    #[cfg_attr(not(test), allow(dead_code))] // no shipped caller yet: the `#[cfg(test)]` oracle replay executes it; C74-e wires the pacer/prober/allocator consumer
    pub(crate) fn probe_max_sequence_number(&self) -> u64 {
        self.probe_max_sequence_number
    }

    /// The two base stamps the oracle echoes, in campaign microseconds relative to
    /// `origin`. An unset base reports `NO_BASE_US`, which is what Go prints for
    /// a zero `mono.UnixTime` minus `time.Unix(1000, 0)`: `1e9` microseconds
    /// below zero.
    ///
    /// The two bases are in different frames by construction, so each is put
    /// back into the oracle's before it is echoed. `baseSendAt` is an `Instant`
    /// measured from the campaign origin, so the echo is the elapsed offset.
    /// `baseRecvTime` is the absolute receive clock the report carried, exactly
    /// as upstream holds it, and upstream prints it relative to its own absolute
    /// `time.Unix(1000, 0)` origin; the campaign origin here is zero, so the same
    /// one billion microseconds come off. Every consumer of either base only
    /// ever subtracts it, so the frame cannot reach the policy.
    #[cfg(test)]
    pub(crate) fn base_times_us(&self, origin: Instant) -> (i64, i64) {
        (
            self.base_send_at
                .map_or(NO_BASE_US, |at| elapsed_micros(at, origin)),
            self.base_recv_time.unwrap_or(0) + NO_BASE_US,
        )
    }

    /// `getPacketInfoExisting`: a slot only counts as this sequence when the
    /// sequence recorded there still matches, so a stale slot is not a packet.
    #[cfg(test)]
    pub(crate) fn packet_info_existing(&self, wire_sequence: u16) -> Option<&PacketInfo> {
        let info = &self.packet_infos[Self::slot(wire_sequence)];
        (info.sequence_number as u16 == wire_sequence).then_some(info)
    }

    /// `piLastRecv.sequenceNumber`: whatever the aliased slot holds now, which is
    /// upstream's behaviour when a later send reused the slot.
    #[cfg(test)]
    pub(crate) fn last_recv_sequence(&self) -> Option<u64> {
        self.last_recv_index
            .map(|index| self.packet_infos[index].sequence_number)
    }

    #[cfg(test)]
    pub(crate) fn sequence_number(&self) -> u64 {
        self.sequence_number
    }

    /// Forward unwrap of the 16-bit wire sequence into the tracker counter.
    fn extend_sequence(&mut self, wire_sequence: u16) -> u64 {
        let extended = match self.last_wire_sequence {
            None => u64::from(wire_sequence),
            Some(last) => {
                let delta = wire_sequence.wrapping_sub(last);
                self.sequence_number.wrapping_add(u64::from(delta))
            }
        };
        self.last_wire_sequence = Some(wire_sequence);
        extended
    }

    /// `getPacketInfo`/`getPacketInfoExisting` slot selection.
    fn slot(wire_sequence: u16) -> usize {
        usize::from(wire_sequence) % TRACKER_SLOTS
    }
}

/// Go's `uint16(size)` truncation of a datagram size.
fn go_u16(size: usize) -> u16 {
    size as u16
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    fn tracker_with_sends(count: u16) -> (PacketTracker, Instant) {
        let start = Instant::now();
        let mut tracker = PacketTracker::new();
        for index in 0..count {
            tracker.record_packet_send(
                start + Duration::from_micros(u64::from(index) * 1_000),
                1_200,
                false,
                0,
                false,
                index,
            );
        }
        (tracker, start)
    }

    #[test]
    fn first_send_keeps_the_upstream_zero_send_time_sentinel() {
        let (tracker, _) = tracker_with_sends(3);
        assert_eq!(tracker.packet_infos[PacketTracker::slot(0)].send_time, 0);
        assert_eq!(
            tracker.packet_infos[PacketTracker::slot(1)].send_time,
            1_000
        );
        assert_eq!(
            tracker.packet_infos[PacketTracker::slot(2)].send_time,
            2_000
        );
        assert_eq!(tracker.sequence_number, 2);
    }

    #[test]
    fn unknown_sequence_numbers_are_not_tracked() {
        let (mut tracker, _) = tracker_with_sends(2);
        // Slot 9 has never been filled with sequence 9.
        assert!(
            tracker
                .record_packet_indication_from_remote(9, 5_000)
                .is_none()
        );
        let (pi, send_delta, recv_delta) = tracker
            .record_packet_indication_from_remote(1, 5_000)
            .unwrap();
        assert_eq!(pi.send_time, 1_000);
        assert_eq!(pi.recv_time, 0, "first indication sets the base");
        assert_eq!((send_delta, recv_delta), (0, 0));
        let (pi, send_delta, recv_delta) = tracker
            .record_packet_indication_from_remote(1, 7_000)
            .unwrap();
        assert_eq!(pi.recv_time, 2_000);
        assert_eq!(
            (send_delta, recv_delta),
            (0, 0),
            "upstream's piLastRecv aliases this very packet, so the deltas are against itself"
        );
    }

    #[test]
    fn ring_replacement_clears_the_last_received_alias() {
        let (mut tracker, start) = tracker_with_sends(1);
        let wire = TRACKER_SLOTS as u16;
        tracker.record_packet_indication_from_remote(0, 1_000);
        assert_eq!(tracker.last_recv_index, Some(PacketTracker::slot(0)));
        tracker.record_packet_send(start + Duration::from_secs(1), 100, false, 0, false, wire);
        assert_eq!(tracker.last_recv_index, None);
    }

    #[test]
    fn probe_cluster_window_tracks_only_the_active_cluster() {
        let start = Instant::now();
        let mut tracker = PacketTracker::new();
        tracker.probe_cluster_starting(7);
        for index in 0..4u16 {
            tracker.record_packet_send(
                start + Duration::from_micros(u64::from(index)),
                100,
                false,
                if index == 3 { 7 } else { 0 },
                index == 3,
                index,
            );
        }
        assert_eq!(tracker.probe_max_sequence_number(), 3);
        tracker.probe_cluster_done(7);
        tracker.record_packet_send(start + Duration::from_secs(1), 100, false, 7, true, 9);
        assert_eq!(
            tracker.probe_max_sequence_number(),
            3,
            "a done cluster must not extend the probe window"
        );
    }

    #[test]
    fn size_is_truncated_to_u16_like_the_upstream_struct_field() {
        let start = Instant::now();
        let mut tracker = PacketTracker::new();
        tracker.record_packet_send(start, 65_536 + 5, false, 0, false, 1);
        assert_eq!(tracker.packet_infos[PacketTracker::slot(1)].size, 5);
        assert_eq!(tracker.packet_infos[PacketTracker::slot(1)].size_i64(), 5);
    }

    #[test]
    fn base_send_time_threshold_is_relative_and_absent_before_any_send() {
        let tracker = PacketTracker::new();
        assert_eq!(
            tracker.base_send_time_threshold(Instant::now(), 10_000_000),
            None
        );
        let (tracker, start) = tracker_with_sends(1);
        assert_eq!(
            tracker.base_send_time_threshold(start + Duration::from_micros(11_000), 10_000_000),
            Some(-9_989_000),
            "the threshold is measured from the base send time, so it is negative well before the age expires"
        );
        assert_eq!(
            tracker.base_send_time_threshold(start + Duration::from_micros(11_000), 1_000),
            Some(10_000)
        );
    }
}
