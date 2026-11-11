//! Fixed original frameintegrity.go: 180 frame entities / 1024 packet bits.
//! All queues are bounded and owned by the actual source receiver generation.
use super::descriptor::Descriptor;
#[derive(Default, Clone)]
struct Frame {
    first: Option<u64>,
    last: Option<u64>,
    intact: bool,
}
pub(super) struct Integrity {
    base: Option<u64>,
    last: u64,
    frames: Vec<Frame>,
    packet_base: Option<u64>,
    packet_last: u64,
    packets: [bool; 1024],
}
impl Default for Integrity {
    fn default() -> Self {
        Self {
            base: None,
            last: 0,
            frames: vec![Frame::default(); 180],
            packet_base: None,
            packet_last: 0,
            packets: [false; 1024],
        }
    }
}
impl Integrity {
    fn packet_slot(&self, sequence: u64) -> usize {
        (sequence.wrapping_sub(self.packet_base.unwrap()) % 1024) as usize
    }
    fn packet(&mut self, sequence: u64) {
        if self.packet_base.is_none() {
            self.packet_base = Some(sequence.saturating_sub(100));
            self.packet_last = sequence;
            let index = self.packet_slot(sequence);
            self.packets[index] = true;
            return;
        }
        if sequence <= self.packet_base.unwrap() {
            return;
        }
        if sequence <= self.packet_last {
            if self.packet_last - sequence < 1024 {
                let index = self.packet_slot(sequence);
                self.packets[index] = true;
            }
            return;
        }
        for seq in (self.packet_last + 1).max(sequence.saturating_sub(1024))..sequence {
            let index = self.packet_slot(seq);
            self.packets[index] = false;
        }
        let index = self.packet_slot(sequence);
        self.packets[index] = true;
        self.packet_last = sequence;
    }
    fn consecutive(&self, first: u64, last: u64) -> bool {
        first <= last
            && last - first < 1024
            && (first..=last).all(|seq| self.packets[self.packet_slot(seq)])
    }
    pub fn add(&mut self, sequence: u64, frame: u64, descriptor: &Descriptor) -> bool {
        self.packet(sequence);
        if self.base.is_none() {
            self.base = Some(frame);
            self.last = frame;
        }
        let base = self.base.unwrap();
        if frame < base {
            return false;
        }
        if frame <= self.last && self.last - frame >= 180 {
            return false;
        }
        if frame > self.last {
            for f in (self.last + 1).max(frame.saturating_sub(179))..=frame {
                self.frames[((f - base) % 180) as usize] = Frame::default();
            }
            self.last = frame;
        }
        let index = ((frame - base) % 180) as usize;
        let f = &mut self.frames[index];
        if !f.intact {
            if f.first.is_none() && descriptor.first {
                f.first = Some(sequence);
            }
            if f.last.is_none() && descriptor.last {
                f.last = Some(sequence);
            }
            let span = f.first.zip(f.last);
            if let Some((first, last)) = span {
                if self.consecutive(first, last) {
                    self.frames[index].intact = true;
                }
            }
        }
        self.frames[index].intact
    }
}
