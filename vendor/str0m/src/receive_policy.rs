//! Fixed LiveKit RTPStatsReceiver sequence/timestamp admission, after SRTP auth.
//! Ported from Apache-2.0 LiveKit v1.13.10 and mediatransportutil WrapAround.
use std::{collections::VecDeque, time::Instant};
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Rejected {
    Unconfigured,
    PaddingOnly,
    PreStartTimestamp,
    OldTimestamp,
    PreStartSequence,
    OldSequence,
    Restart,
}
#[derive(Clone, Copy, Debug)]
pub(crate) struct Flow {
    pub seq: u64,
    pub timestamp: u64,
    pub in_order: bool,
}
#[derive(Clone, Copy, Debug)]
struct Update {
    unhandled: bool,
    previous: u64,
    value: u64,
}
#[derive(Clone, Copy, Debug)]
struct Wrap {
    bits: u32,
    initialized: bool,
    start: u64,
    highest: u64,
    cycles: u64,
}
impl Wrap {
    fn new(bits: u32) -> Self {
        Self {
            bits,
            initialized: false,
            start: 0,
            highest: 0,
            cycles: 0,
        }
    }
    fn range(self) -> u64 {
        1u64 << self.bits
    }
    fn mask(self) -> u64 {
        self.range() - 1
    }
    fn ext(self) -> u64 {
        self.cycles.wrapping_add(self.highest)
    }
    fn gap(self, a: u64, b: u64) -> u64 {
        a.wrapping_sub(b) & self.mask()
    }
    fn update(&mut self, value: u64, rollovers: Option<u64>) -> Update {
        let value = value & self.mask();
        if !self.initialized {
            self.initialized = true;
            self.start = value;
            self.highest = value;
            return Update {
                unhandled: false,
                previous: value.wrapping_sub(1),
                value,
            };
        }
        let previous = self.ext();
        if let Some(rollovers) = rollovers {
            self.cycles = self.cycles.wrapping_add(rollovers * self.range());
            self.highest = value;
            return Update {
                unhandled: false,
                previous,
                value: self.ext(),
            };
        }
        if self.gap(value, self.highest) > self.range() / 2 {
            let mut cycles = self.cycles;
            let total = self.ext().wrapping_sub(self.start).wrapping_add(1);
            let wrapback = self.highest < self.range() / 2 && value >= self.range() / 2;
            if total > self.range() / 2 {
                if wrapback {
                    cycles = cycles.wrapping_sub(self.range());
                }
            } else if self.gap(value, self.start) > self.range() / 2 {
                return Update {
                    unhandled: true,
                    previous,
                    value: cycles.wrapping_add(value),
                };
            } else if wrapback {
                cycles = cycles.wrapping_sub(self.range());
            }
            return Update {
                unhandled: false,
                previous,
                value: cycles.wrapping_add(value),
            };
        }
        if value < self.highest {
            self.cycles = self.cycles.wrapping_add(self.range());
        }
        self.highest = value;
        Update {
            unhandled: false,
            previous,
            value: self.ext(),
        }
    }
}
#[derive(Debug)]
pub(crate) struct Policy {
    sn: Wrap,
    ts: Wrap,
    highest_at: Option<Instant>,
    restart: VecDeque<(u16, u32)>,
}
impl Default for Policy {
    fn default() -> Self {
        Self {
            sn: Wrap::new(16),
            ts: Wrap::new(32),
            highest_at: None,
            restart: VecDeque::new(),
        }
    }
}
impl Policy {
    pub fn admit(
        &mut self,
        sn: u16,
        ts: u32,
        media: bool,
        clock_rate: u32,
        now: Instant,
    ) -> Result<Flow, Rejected> {
        if clock_rate == 0 {
            return Err(Rejected::Unconfigured);
        }
        let previous_sn = self.sn;
        let previous_ts = self.ts;
        if !self.sn.initialized {
            if !media {
                return Err(Rejected::PaddingOnly);
            }
            let s = self.sn.update(u64::from(sn), None);
            let t = self.ts.update(u64::from(ts), None);
            self.highest_at = Some(now);
            self.restart.clear();
            return Ok(Flow {
                seq: s.value,
                timestamp: t.value,
                in_order: true,
            });
        }
        let mut s = self.sn.update(u64::from(sn), None);
        let mut gap_sn = s.value.wrapping_sub(s.previous) as i64;
        let highest_at = self.highest_at.unwrap();
        let elapsed = if now >= highest_at {
            now.duration_since(highest_at).as_nanos() as i128
        } else {
            -(highest_at.duration_since(now).as_nanos() as i128)
        };
        let expected = elapsed * i128::from(clock_rate) / 1_000_000_000;
        let threshold = (1i128 << 31) * 1_000_000_000 / i128::from(clock_rate);
        let rollovers = if elapsed < threshold {
            None
        } else {
            let excess = (elapsed - threshold * 2) * i128::from(clock_rate) / 1_000_000_000;
            let mut cycles = (excess / (1i128 << 32)).max(0) as u64;
            if self.ts.highest > u64::from(ts) {
                cycles += 1;
            }
            Some(cycles)
        };
        let t = self.ts.update(u64::from(ts), rollovers);
        let gap_ts = t.value.wrapping_sub(t.previous) as i64;
        let rejection = if t.unhandled {
            Some(Rejected::PreStartTimestamp)
        } else if !s.unhandled
            && gap_sn < 0
            && gap_ts > 0
            && (gap_ts as f64) > (expected as f64) * 1.5
        {
            Some(Rejected::OldTimestamp)
        } else if !s.unhandled && gap_ts < 0 && gap_sn > 0 {
            Some(Rejected::OldSequence)
        } else {
            None
        };
        if let Some(reason) = rejection {
            self.sn = previous_sn;
            self.ts = previous_ts;
            return Err(self.rejected(sn, ts, media, reason));
        }
        if (gap_sn < 0 || gap_sn > (1 << 15)) && gap_ts > 0 && media {
            let cycles = u64::from(u64::from(sn) < self.sn.highest);
            s = self.sn.update(u64::from(sn), Some(cycles));
        }
        if s.unhandled {
            self.sn = previous_sn;
            self.ts = previous_ts;
            return Err(self.rejected(sn, ts, media, Rejected::PreStartSequence));
        }
        gap_sn = s.value.wrapping_sub(s.previous) as i64;
        if gap_sn > 0 && ts != t.previous as u32 {
            self.highest_at = Some(now);
        }
        self.restart.clear();
        Ok(Flow {
            seq: s.value,
            timestamp: t.value,
            in_order: gap_sn > 0,
        })
    }
    fn rejected(&mut self, sn: u16, ts: u32, media: bool, reason: Rejected) -> Rejected {
        if media {
            if self.restart.len() == 5 {
                self.restart.pop_front();
            }
            self.restart.push_back((sn, ts));
        }
        if self.restart.len() == 5
            && self
                .restart
                .iter()
                .zip(self.restart.iter().skip(1))
                .all(|(a, b)| b.0 == a.0.wrapping_add(1) && b.1.wrapping_sub(a.1) <= 1 << 31)
        {
            self.restart.clear();
            Rejected::Restart
        } else {
            reason
        }
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn rejects_timestamp_and_sequence_without_poisoning_the_next_real_packet() {
        let now = Instant::now();
        let mut p = Policy::default();
        assert_eq!(p.admit(100, 3000, true, 90000, now).unwrap().seq, 100);
        assert_eq!(
            p.admit(
                100,
                1,
                true,
                90000,
                now + std::time::Duration::from_millis(10)
            )
            .unwrap_err(),
            Rejected::PreStartTimestamp
        );
        assert_eq!(
            p.admit(
                101,
                9000,
                true,
                90000,
                now + std::time::Duration::from_millis(33)
            )
            .unwrap()
            .seq,
            101
        );
        assert_eq!(
            p.admit(
                102,
                6000,
                true,
                90000,
                now + std::time::Duration::from_millis(40)
            )
            .unwrap_err(),
            Rejected::OldSequence
        );
        assert_eq!(
            p.admit(
                100,
                12000,
                true,
                90000,
                now + std::time::Duration::from_millis(50)
            )
            .unwrap_err(),
            Rejected::OldTimestamp
        );
        assert_eq!(
            p.admit(
                102,
                9000,
                true,
                90000,
                now + std::time::Duration::from_millis(70)
            )
            .unwrap()
            .seq,
            102
        );
    }
    #[test]
    #[ignore = "requires actual original Go SOURCE_RTP_ADMISSION rows"]
    fn original_go_admission_packets_match_native_source_policy() {
        let path = std::env::var("GO_RTP_ADMISSION_RESULTS").expect("original observer log");
        let log = std::fs::read_to_string(path).unwrap();
        let mut cases = std::collections::HashMap::<String, Policy>::new();
        let start = Instant::now();
        let mut rows = 0;
        for line in log.lines() {
            let Some((_, row)) = line.split_once("SOURCE_RTP_ADMISSION ") else {
                continue;
            };
            let v: serde_json::Value = serde_json::from_str(row).unwrap();
            let p = cases
                .entry(v["case"].as_str().unwrap().to_owned())
                .or_default();
            let out = p.admit(
                v["sn"].as_u64().unwrap() as u16,
                v["ts"].as_u64().unwrap() as u32,
                v["payload"].as_u64().unwrap() > 0,
                90000,
                start + std::time::Duration::from_millis(v["ms"].as_u64().unwrap()),
            );
            let reason = match &out {
                Ok(_) => "NONE",
                Err(Rejected::Unconfigured) => "UNCONFIGURED",
                Err(Rejected::PaddingOnly) => "PADDING_ONLY",
                Err(Rejected::PreStartTimestamp) => "PRE_START_TIMESTAMP",
                Err(Rejected::OldTimestamp) => "OLD_TIMESTAMP",
                Err(Rejected::PreStartSequence) => "PRE_START_SEQUENCE_NUMBER",
                Err(Rejected::OldSequence) => "OLD_SEQUENCE_NUMBER",
                Err(Rejected::Restart) => "RESTART",
            };
            assert_eq!(reason, v["reason"].as_str().unwrap(), "{v}");
            if let Ok(f) = out {
                assert_eq!(f.seq, v["seq"].as_u64().unwrap(), "{v}");
                assert_eq!(f.timestamp, v["timestamp"].as_u64().unwrap(), "{v}");
                assert_eq!(!f.in_order, v["out_of_order"].as_bool().unwrap(), "{v}");
            }
            rows += 1;
        }
        assert!(
            rows >= 26,
            "must execute actual original packet rows, got {rows}"
        );
        assert_eq!(cases.len(), 6);
    }
}
