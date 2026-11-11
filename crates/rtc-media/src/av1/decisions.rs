//! Fixed Go SelectorDecisionCache (256 entries / 80 NACK entries). Frame IDs
//! arrive here as raw DD u16; the native bridge unwraps nearest the last source
//! frame. Explicit source restart generations remain a separate parser contract.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(super) enum Decision {
    #[default]
    Missing,
    Dropped,
    Forwarded,
    Unknown,
}
pub(super) struct Decisions {
    base: Option<u64>,
    last: u64,
    entries: [Decision; 256],
}
impl Default for Decisions {
    fn default() -> Self {
        Self {
            base: None,
            last: 0,
            entries: [Decision::Missing; 256],
        }
    }
}
impl Decisions {
    fn extended(&self, raw: u16) -> u64 {
        if self.base.is_none() {
            return u64::from(raw);
        }
        let mut value = (self.last & !65535) | u64::from(raw);
        if value + 32768 < self.last {
            value += 65536;
        } else if value > self.last + 32768 && value >= 65536 {
            value -= 65536;
        }
        value
    }
    fn slot(&self, frame: u64) -> usize {
        ((frame - self.base.unwrap()) % 256) as usize
    }
    fn set(&mut self, frame: u64, decision: Decision) {
        let index = self.slot(frame);
        self.entries[index] = decision;
    }
    pub fn get(&self, raw: u16) -> Decision {
        let Some(base) = self.base else {
            return Decision::Missing;
        };
        let frame = self.extended(raw);
        if frame < base {
            return Decision::Missing;
        }
        if frame > self.last {
            return Decision::Unknown;
        }
        if self.last - frame >= 256 {
            return Decision::Missing;
        }
        self.entries[self.slot(frame)]
    }
    pub fn expect(&self, raw: u16) -> bool {
        let Some(base) = self.base else {
            return false;
        };
        let frame = self.extended(raw);
        frame >= base && (frame >= self.last || self.last - frame < 256)
    }
    pub fn add(&mut self, raw: u16, decision: Decision) {
        let frame = self.extended(raw);
        let Some(base) = self.base else {
            self.base = Some(frame);
            self.last = frame;
            self.set(frame, decision);
            return;
        };
        if frame <= base {
            return;
        }
        if frame <= self.last {
            self.set(frame, decision);
            return;
        }
        let floor = frame.saturating_sub(256);
        for value in (self.last + 1).max(floor)..frame {
            self.set(value, Decision::Unknown);
        }
        let mut start = if self.last > 80 + base {
            self.last - 80
        } else {
            base
        };
        let end = if frame > 80 + base { frame - 80 } else { base };
        if end > start + 256 {
            start = end - 256;
        }
        for value in start..end {
            if self.entries[self.slot(value)] == Decision::Unknown {
                self.set(value, Decision::Missing);
            }
        }
        self.set(frame, decision);
        self.last = frame;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn actual_nack_horizon_expires_unknown_and_keeps_late_forwarded_decisions() {
        let mut cache = Decisions::default();
        cache.add(10, Decision::Forwarded);
        cache.add(12, Decision::Forwarded);
        assert_eq!(cache.get(11), Decision::Unknown);
        assert!(cache.expect(11));
        cache.add(11, Decision::Forwarded);
        assert_eq!(cache.get(11), Decision::Forwarded);
        cache.add(14, Decision::Forwarded);
        assert_eq!(cache.get(13), Decision::Unknown);
        cache.add(94, Decision::Forwarded);
        assert_eq!(cache.get(13), Decision::Missing);
        cache.add(350, Decision::Forwarded);
        assert_eq!(cache.get(14), Decision::Missing);
        assert!(!cache.expect(14));
    }
    #[test]
    fn dd_raw_wrap_keeps_chain_reference_identity_inside_real_source_window() {
        let mut cache = Decisions::default();
        cache.add(65534, Decision::Forwarded);
        cache.add(0, Decision::Forwarded);
        assert_eq!(cache.get(65535), Decision::Unknown);
        cache.add(65535, Decision::Dropped);
        assert_eq!(cache.get(65535), Decision::Dropped);
        assert_eq!(cache.get(0), Decision::Forwarded);
    }
}
