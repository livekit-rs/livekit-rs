//! Publisher-side RFC 2198 blocks retained for verbatim audio forwarding.
//!
//! Frame mode depacketization narrows a RED payload to its primary codec before
//! [`str0m::media::MediaData`] is emitted, so the chain a publisher actually sent is only
//! visible on the raw packet event. str0m pushes that event ahead of the frame it produces,
//! so the block is matched to a frame by the sequence numbers that frame was built from:
//! an RTP timestamp is shared by retransmissions and by packets the depacketizer refused,
//! and cannot name which of them produced the frame.

use std::collections::HashMap;
use std::collections::VecDeque;
use std::sync::Arc;

/// Retained timestamps, i.e. a few seconds of 20 ms frames. Every subscriber of one
/// publication reads the same timestamp, so retention is keyed by timestamp, not by consumer.
const WINDOW: usize = 150;

/// Distinct blocks retained for one timestamp. A publisher retransmits a frame a couple of
/// times at most; a timestamp crowded with more payloads than this cannot be attributed.
const PER_TIMESTAMP: usize = 4;

/// Whether `seq` lies in the inclusive wire-sequence range `start..=end`, across the wrap.
fn within_range(seq: u16, start: u16, end: u16) -> bool {
    seq.wrapping_sub(start) <= end.wrapping_sub(start)
}

#[derive(Clone)]
enum Slot {
    /// Blocks seen for this timestamp, each with the sequence number that carried it.
    Blocks(Vec<(u16, Arc<[u8]>)>),
    /// This timestamp carries too many distinct blocks to name the one a frame came from.
    Crowded,
}

/// Timestamp-indexed registry of the chains seen on one publication's audio stream.
#[derive(Default)]
pub(crate) struct RedChains {
    slots: HashMap<u32, Slot>,
    order: VecDeque<u32>,
}

impl RedChains {
    /// Record the authenticated block of a RED-wrapped inbound packet, together with the
    /// sequence number that carried it.
    pub(crate) fn note(&mut self, timestamp: u32, seq: u16, chain: &[u8]) {
        let fresh = !self.slots.contains_key(&timestamp);
        let mut crowded = false;
        if let Some(Slot::Blocks(blocks)) = self.slots.get_mut(&timestamp) {
            match blocks.iter().find(|(seen, _)| *seen == seq) {
                // The same packet re-noted is only a conflict if its bytes differ: two
                // authenticated payloads cannot both occupy one sequence number.
                Some((_, seen)) if &**seen != chain => crowded = true,
                Some(_) => {}
                None if blocks.len() >= PER_TIMESTAMP => crowded = true,
                None => blocks.push((seq, Arc::from(chain))),
            }
        }
        if crowded {
            self.slots.insert(timestamp, Slot::Crowded);
        } else if fresh {
            self.slots
                .insert(timestamp, Slot::Blocks(vec![(seq, Arc::from(chain))]));
            self.order.push_back(timestamp);
        }
        while self.order.len() > WINDOW {
            if let Some(old) = self.order.pop_front() {
                self.slots.remove(&old);
            }
        }
    }

    /// The block carried by a packet inside the sequence range of one delivered frame, when
    /// every in-range packet agrees on it. Peeking keeps the slot readable by the
    /// publication's other subscribers, and a retransmission of the same bytes stays
    /// attributable for the frame built from either copy.
    pub(crate) fn get(&self, timestamp: u32, start: u16, end: u16) -> Option<&Arc<[u8]>> {
        let Slot::Blocks(blocks) = self.slots.get(&timestamp)? else {
            return None;
        };
        let mut found: Option<&Arc<[u8]>> = None;
        for (seq, chain) in blocks {
            if !within_range(*seq, start, end) {
                continue;
            }
            match found {
                None => found = Some(chain),
                Some(first) if **first == **chain => {}
                Some(_) => return None,
            }
        }
        found
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn one_chain_per_timestamp_survives_until_the_window_passes() {
        let mut chains = RedChains::default();
        chains.note(960, 10, &[0x68, 0x01, 0x02]);
        assert_eq!(&**chains.get(960, 10, 10).unwrap(), &[0x68, 0x01, 0x02]);
        // The slot stays readable: several subscribers forward the same publication.
        assert!(chains.get(960, 10, 10).is_some());
        assert!(chains.get(1000, 10, 10).is_none());
        for index in 1..=WINDOW as u32 {
            chains.note(960 + index * 960, 10 + index as u16, &[index as u8]);
        }
        // The evicted timestamp is the oldest one, not a recent arrival.
        assert!(chains.get(960, 10, 10).is_none());
        assert!(
            chains
                .get(
                    960 + WINDOW as u32 * 960,
                    10 + WINDOW as u16,
                    10 + WINDOW as u16
                )
                .is_some()
        );
    }

    #[test]
    fn a_block_outside_the_frame_sequence_range_names_no_chain() {
        let mut chains = RedChains::default();
        // A RED packet the depacketizer refused, then the packet that formed the frame.
        chains.note(4800, 20, &[0xfe, 0x00]);
        assert!(
            chains.get(4800, 21, 21).is_none(),
            "a frame built from sequence 21 cannot be charged with the block from 20"
        );
        // The refused block is still readable for the frame it did build, so the
        // discrimination comes from the range and not from the slot being gone.
        assert_eq!(&**chains.get(4800, 20, 20).unwrap(), &[0xfe, 0x00]);
    }

    #[test]
    fn an_identical_retransmit_stays_attributable() {
        let mut chains = RedChains::default();
        chains.note(4800, 30, &[0x68, 0x11]);
        chains.note(4800, 31, &[0x68, 0x11]);
        // Each copy forms a frame, and both frames carry the published chain.
        assert_eq!(&**chains.get(4800, 30, 30).unwrap(), &[0x68, 0x11]);
        assert_eq!(&**chains.get(4800, 31, 31).unwrap(), &[0x68, 0x11]);
        assert_eq!(&**chains.get(4800, 30, 31).unwrap(), &[0x68, 0x11]);
        // Re-noting the same packet does not crowd the timestamp.
        chains.note(4800, 30, &[0x68, 0x11]);
        assert_eq!(&**chains.get(4800, 30, 31).unwrap(), &[0x68, 0x11]);
    }

    #[test]
    fn two_payloads_on_one_sequence_number_name_no_chain() {
        let mut chains = RedChains::default();
        chains.note(4800, 30, &[0x68, 0x11]);
        chains.note(4800, 30, &[0x68, 0x99]);
        assert!(
            chains.get(4800, 30, 30).is_none(),
            "two authenticated payloads cannot both be the frame's source"
        );
    }

    #[test]
    fn two_different_blocks_inside_one_frame_range_name_no_chain() {
        let mut chains = RedChains::default();
        chains.note(4800, 30, &[0x68, 0x11]);
        chains.note(4800, 31, &[0x68, 0x99]);
        assert!(
            chains.get(4800, 30, 31).is_none(),
            "an in-range disagreement must not pick a block"
        );
        // Each copy is still attributable to the frame it alone formed.
        assert_eq!(&**chains.get(4800, 30, 30).unwrap(), &[0x68, 0x11]);
        assert_eq!(&**chains.get(4800, 31, 31).unwrap(), &[0x68, 0x99]);
    }

    #[test]
    fn a_crowded_timestamp_stops_naming_chains() {
        let mut chains = RedChains::default();
        for (offset, seq) in (40u16..).take(PER_TIMESTAMP).enumerate() {
            chains.note(4800, seq, &[offset as u8]);
        }
        assert_eq!(&**chains.get(4800, 40, 40).unwrap(), &[0]);
        chains.note(4800, 44, &[PER_TIMESTAMP as u8]);
        assert!(
            chains.get(4800, 40, 40).is_none(),
            "past the bound the registry stops naming blocks for that timestamp entirely"
        );
        // A fresh timestamp is unaffected.
        chains.note(5760, 45, &[0xe8]);
        assert_eq!(&**chains.get(5760, 45, 45).unwrap(), &[0xe8]);
    }

    #[test]
    fn sequence_ranges_are_compared_across_the_wrap() {
        let mut chains = RedChains::default();
        chains.note(4800, u16::MAX, &[0x68, 0x07]);
        assert_eq!(&**chains.get(4800, u16::MAX, 1).unwrap(), &[0x68, 0x07]);
        assert!(chains.get(4800, 0, 1).is_none());
        chains.note(4800, 0, &[0x68, 0x08]);
        assert!(
            chains.get(4800, u16::MAX, 1).is_none(),
            "a wrapped range spanning both blocks cannot pick one"
        );
    }
}
