//! DTLS-in-STUN flight/ack controller from the fixed LiveKit ICE
//! v4.4.0-warp.3 `piggyback.go`. STUN authentication is the caller's boundary:
//! unauthenticated attributes must never reach `receive`.
use std::collections::VecDeque;

const CRC: crc::Crc<u32> = crc::Crc::<u32>::new(&crc::CRC_32_ISO_HDLC);

#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
enum State {
    #[default]
    Off,
    Tentative,
    Confirmed,
    Pending,
    Complete,
}
#[derive(Debug, Default)]
pub(crate) struct Sped {
    state: State,
    packets: VecDeque<(Vec<u8>, u32)>,
    index: usize,
    acks: Vec<u32>,
    new_flight: bool,
    callback: bool,
    connected: bool,
    pub(crate) dirty: bool,
}
impl Sped {
    pub(crate) fn enable(&mut self, enabled: bool) {
        *self = Self::default();
        if enabled {
            self.state = State::Tentative;
            self.callback = true;
        }
    }
    pub(crate) fn enabled(&self) -> bool {
        self.state != State::Off
    }
    pub(crate) fn active(&self) -> bool {
        !matches!(self.state, State::Off | State::Complete)
    }
    /// Returns false only when the caller must send this packet as plain DTLS.
    pub(crate) fn queue(&mut self, packet: Vec<u8>, end: bool) -> bool {
        let idle = !self.active();
        if idle && self.connected {
            return false;
        }
        if self.new_flight {
            self.packets.clear();
            self.index = 0;
        }
        self.new_flight = end;
        let checksum = CRC.checksum(&packet);
        self.packets.push_back((packet, checksum));
        self.dirty = true;
        true
    }
    pub(crate) fn complete(&mut self) {
        if self.active() {
            self.state = State::Pending;
            // The fixed DTLS transport removes its ICE callback on completion.
            self.callback = false;
            self.dirty = true;
        }
    }
    /// Own copies let the caller serialize STUN without borrowing mutable state.
    /// Empty ACK is distinct from absent ACK: it advertises SPED support.
    pub(crate) fn attributes(&mut self) -> Option<(Option<Vec<u8>>, Vec<u32>)> {
        if !self.active() {
            return None;
        }
        let packet = if self.packets.is_empty() {
            None
        } else {
            self.index %= self.packets.len();
            let packet = self.packets[self.index].0.clone();
            self.index = (self.index + 1) % self.packets.len();
            Some(packet)
        };
        Some((packet, self.acks.clone()))
    }
    /// Called only after short-term MESSAGE-INTEGRITY/transaction checks.
    /// Return payload even when duplicate: DTLS owns replay/flight handling.
    pub(crate) fn receive(
        &mut self,
        packet: Option<&[u8]>,
        ack_bytes: Option<&[u8]>,
    ) -> Option<Vec<u8>> {
        if !self.active() {
            return None;
        }
        // Fixed Go ignores malformed ACK attributes, treating GetFrom error as
        // absence; a valid independently embedded DTLS packet is still delivered.
        let acks = ack_bytes.filter(|v| v.len() <= 16 && v.len() % 4 == 0);
        if packet.is_none() && acks.is_none() {
            self.state = if self.state == State::Tentative {
                State::Off
            } else {
                State::Complete
            };
            self.callback = false;
            self.acks.clear();
            return None;
        }
        if self.state == State::Tentative {
            self.state = State::Confirmed;
        }
        if let Some(acks) = acks {
            self.packets.retain(|(_, checksum)| {
                !acks
                    .chunks_exact(4)
                    .any(|ack| u32::from_be_bytes(ack.try_into().unwrap()) == *checksum)
            });
            if self.packets.is_empty() {
                self.index = 0;
            } else {
                self.index %= self.packets.len();
            }
        }
        if packet.is_none_or(|v| v.is_empty()) {
            self.acks.clear();
        }
        let packet = packet.filter(|p| !p.is_empty())?;
        if !self.callback {
            return None;
        }
        self.report_plain(packet);
        Some(packet.to_vec())
    }
    /// Both embedded and plain DTLS handshake records contribute CRC32 ACKs.
    pub(crate) fn report_plain(&mut self, packet: &[u8]) -> bool {
        if !self.active() {
            return false;
        }
        let checksum = CRC.checksum(packet);
        if self.acks.contains(&checksum) {
            return false;
        }
        self.acks.push(checksum);
        self.dirty = true;
        if self.acks.len() > 4 {
            self.acks.remove(0);
        }
        true
    }
    pub(crate) fn connected(&mut self) {
        self.connected = true;
    }
    /// Fallback/peer completion cannot strand a flight intercepted before ICE.
    pub(crate) fn poll_plain(&mut self) -> Option<Vec<u8>> {
        if !self.connected || self.active() {
            return None;
        }
        self.packets.pop_front().map(|p| p.0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn support_ack_crc_flight_and_authenticated_fallback_contract() {
        let mut a = Sped::default();
        a.enable(true);
        assert_eq!(a.attributes(), Some((None, vec![])));
        assert!(a.queue(b"123456789".to_vec(), true));
        assert_eq!(a.packets[0].1, 0xcbf43926, "IEEE CRC32, not Castagnoli");
        let ack = 0xcbf43926u32.to_be_bytes();
        assert_eq!(a.receive(Some(b"peer"), Some(&ack)), Some(b"peer".to_vec()));
        assert!(a.packets.is_empty());
        assert_eq!(a.acks, vec![CRC.checksum(b"peer")]);
        assert!(a.queue(b"next flight".to_vec(), true));
        a.receive(None, None);
        a.connected();
        assert!(!a.active());
        assert_eq!(a.poll_plain(), Some(b"next flight".to_vec()));
        assert!(!a.queue(b"late retry".to_vec(), true));
        assert_eq!(a.attributes(), None);
    }
    #[test]
    fn no_support_flushes_before_retry_and_malformed_ack_does_not_discard_dtls() {
        let mut a = Sped::default();
        a.enable(true);
        assert!(a.queue(vec![22, 1], true));
        a.receive(None, None);
        assert_eq!(a.poll_plain(), None);
        a.connected();
        assert_eq!(a.poll_plain(), Some(vec![22, 1]));
        a.enable(true);
        assert_eq!(
            a.receive(Some(b"record"), Some(&[1, 2, 3])),
            Some(b"record".to_vec())
        );
        for i in 0..8 {
            a.report_plain(&[i]);
        }
        assert_eq!(a.acks.len(), 4);
        a.complete();
        assert_eq!(a.receive(Some(b"after connected"), Some(&[])), None);
        a.enable(false);
        assert!(!a.active());
        assert_eq!(a.poll_plain(), None);
    }
}
