//! Publisher-side RFC 2198 blocks retained for verbatim audio forwarding.
//!
//! Frame mode depacketization narrows a RED payload to its primary codec before
//! [`str0m::media::MediaData`] is emitted, so the chain a publisher actually sent is only
//! visible on the raw packet event. str0m pushes that event ahead of the frame it produces,
//! which lets the forwarder name the block by RTP timestamp instead of carrying it through
//! the depacketizer.

use std::collections::hash_map::Entry;
use std::collections::{HashMap, VecDeque};
use std::sync::Arc;

/// Retained timestamps, i.e. a few seconds of 20 ms frames. Every subscriber of one
/// publication reads the same slot, so retention is keyed by timestamp, not by consumer.
const WINDOW: usize = 150;

#[derive(Clone)]
enum Slot {
    Chain(Arc<[u8]>),
    /// More than one packet carries this timestamp, so no single block describes the frame.
    Ambiguous,
}

/// Timestamp-indexed registry of the chains seen on one publication's audio stream.
#[derive(Default)]
pub(crate) struct RedChains {
    slots: HashMap<u32, Slot>,
    order: VecDeque<u32>,
}

impl RedChains {
    /// Record the authenticated block of a RED-wrapped inbound packet.
    pub(crate) fn note(&mut self, timestamp: u32, chain: &[u8]) {
        match self.slots.entry(timestamp) {
            // Second packet for the timestamp: the frame has no single chain to name.
            Entry::Occupied(mut slot) => {
                slot.insert(Slot::Ambiguous);
            }
            Entry::Vacant(slot) => {
                slot.insert(Slot::Chain(Arc::from(chain)));
                self.order.push_back(timestamp);
            }
        }
        while self.order.len() > WINDOW {
            if let Some(old) = self.order.pop_front() {
                self.slots.remove(&old);
            }
        }
    }

    /// The block the frame with this RTP timestamp arrived in, when exactly one packet
    /// carried it. Peeking keeps the slot readable by the publication's other subscribers.
    pub(crate) fn get(&self, timestamp: u32) -> Option<&Arc<[u8]>> {
        match self.slots.get(&timestamp) {
            Some(Slot::Chain(chain)) => Some(chain),
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn one_chain_per_timestamp_survives_until_the_window_passes() {
        let mut chains = RedChains::default();
        chains.note(960, &[0x68, 0x01, 0x02]);
        assert_eq!(&**chains.get(960).unwrap(), &[0x68, 0x01, 0x02]);
        // The slot stays readable: several subscribers forward the same publication.
        assert!(chains.get(960).is_some());
        assert!(chains.get(1000).is_none());
        for index in 1..=WINDOW as u32 {
            chains.note(960 + index * 960, &[index as u8]);
        }
        // The evicted timestamp is the oldest one, not a recent arrival.
        assert!(chains.get(960).is_none());
        assert!(chains.get(960 + WINDOW as u32 * 960).is_some());
    }

    #[test]
    fn a_second_packet_on_the_same_timestamp_names_no_chain() {
        let mut chains = RedChains::default();
        chains.note(960, &[0x68]);
        chains.note(960, &[0x68]);
        assert!(chains.get(960).is_none());
        // A later frame at a fresh timestamp is still attributable.
        chains.note(1920, &[0xe8, 0x7f]);
        assert_eq!(&**chains.get(1920).unwrap(), &[0xe8, 0x7f]);
    }
}
