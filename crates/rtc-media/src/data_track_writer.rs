//! LiveKit datachannel_writer.go unreliable admission plus bitrate.go BusyOnly.
//! Source is fixed by the adjacent candidate manifest. No throughput estimate is
//! synthesized: samples use accepted write bytes and actual post-write SCTP buffer.
use std::collections::VecDeque;
use std::time::Instant;

const WINDOW_NS: i64 = 100_000_000;
const DURATION_NS: i64 = 2_000_000_000;
const MINIMUM_BUFFERED: u64 = 8 * 1024;
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Admission {
    Write,
    HighBuffered,
    /// Upstream dereferences a nil calculator for a negative target. Preserve
    /// this invalid source domain as an explicit failed write, never a bypass.
    InvalidNegativeTarget,
}
#[derive(Clone, Copy, Debug)]
struct Window {
    start: Instant,
    bytes: i64,
    busy: i64,
}
fn elapsed(now: Instant, then: Instant) -> i64 {
    if now >= then {
        now.duration_since(then).as_nanos() as i64
    } else {
        -(then.duration_since(now).as_nanos() as i64)
    }
}
#[derive(Debug)]
pub(crate) struct Rate {
    windows: VecDeque<Window>,
    active: Window,
    bytes: i64,
    busy: i64,
    last_buffered: i64,
    last: Option<Instant>,
}
impl Rate {
    pub(crate) fn new(now: Instant) -> Self {
        Self {
            windows: VecDeque::new(),
            active: Window {
                start: now,
                bytes: 0,
                busy: 0,
            },
            bytes: 0,
            busy: 0,
            last_buffered: 0,
            last: None,
        }
    }
    pub(crate) fn add(&mut self, written: usize, buffered: usize, now: Instant) {
        let written = written as i64;
        let buffered = buffered as i64;
        let backlogged = buffered.wrapping_sub(written) > 0;
        let busy = if backlogged {
            self.last.map_or(0, |last| elapsed(now, last).max(0))
        } else {
            0
        };
        self.last = Some(now);
        let drained = written
            .wrapping_sub(buffered.wrapping_sub(self.last_buffered))
            .max(0);
        self.last_buffered = buffered;
        if elapsed(now, self.active.start) >= WINDOW_NS {
            self.windows.push_back(self.active);
            self.active = Window {
                start: now,
                bytes: 0,
                busy: 0,
            };
            while let Some(front) = self.windows.front() {
                if elapsed(now, front.start) > DURATION_NS + WINDOW_NS {
                    self.bytes = self.bytes.wrapping_sub(front.bytes);
                    self.busy = self.busy.wrapping_sub(front.busy);
                    self.windows.pop_front();
                } else {
                    break;
                }
            }
            if self.windows.is_empty() {
                self.bytes = 0;
                self.busy = 0;
            }
        }
        self.bytes = self.bytes.wrapping_add(drained);
        self.active.bytes = self.active.bytes.wrapping_add(drained);
        self.busy = self.busy.wrapping_add(busy);
        self.active.busy = self.active.busy.wrapping_add(busy);
    }
    pub(crate) fn bitrate(&self) -> Option<i64> {
        if self.busy < WINDOW_NS {
            return None;
        }
        Some(self.bytes.wrapping_mul(8).wrapping_mul(1000) / (self.busy / 1_000_000))
    }
}
#[derive(Debug)]
pub(crate) struct DataTrackWriter {
    target_ns: i64,
    rate: Option<Rate>,
}
impl DataTrackWriter {
    #[cfg(test)]
    pub(crate) fn source_bitrate_for_test(&self) -> Option<i64> {
        self.rate.as_ref().and_then(|r| r.bitrate())
    }

    pub(crate) fn new(target_ns: i64, now: Instant) -> Self {
        Self {
            target_ns,
            rate: (target_ns > 0).then(|| Rate::new(now)),
        }
    }
    pub(crate) fn admit(&self, buffered: usize) -> Admission {
        self.admit_with_minimum(buffered, MINIMUM_BUFFERED)
    }
    fn admit_with_minimum(&self, buffered: usize, minimum: u64) -> Admission {
        if self.target_ns == 0 {
            return Admission::Write;
        }
        let Some(rate) = &self.rate else {
            return Admission::InvalidNegativeTarget;
        };
        if let Some(bitrate) = rate.bitrate() {
            let limit = (bitrate.wrapping_mul(self.target_ns) / 8 / 1_000_000_000) as u64;
            if buffered as u64 > limit && buffered as u64 > minimum {
                return Admission::HighBuffered;
            }
        }
        Admission::Write
    }
    pub(crate) fn record(&mut self, written: usize, buffered: usize, now: Instant) {
        if let Some(rate) = &mut self.rate {
            rate.add(written, buffered, now);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;
    #[test]
    fn unmeasured_and_zero_latency_do_not_use_the_old_fixed_64k_drop() {
        let now = Instant::now();
        for target in [0, 100_000_000] {
            assert_eq!(
                DataTrackWriter::new(target, now).admit(1_000_000),
                Admission::Write
            );
        }
    }
    #[test]
    fn strict_measured_limit_and_strict_minimum_are_both_required() {
        let now = Instant::now();
        let mut w = DataTrackWriter::new(100_000_000, now);
        w.record(1000, 6000, now);
        w.record(1000, 6000, now + Duration::from_millis(100));
        assert_eq!(w.rate.as_ref().unwrap().bitrate(), Some(80_000));
        assert_eq!(w.admit(8192), Admission::Write);
        assert_eq!(w.admit(8193), Admission::HighBuffered);
        let mut high = DataTrackWriter::new(1_000_000_000, now);
        high.record(100_000, 300_000, now);
        high.record(100_000, 300_000, now + Duration::from_millis(100));
        assert_eq!(high.admit(70_000), Admission::Write);
    }
    #[test]
    fn source_negative_is_not_silently_clamped_to_zero() {
        assert_eq!(
            DataTrackWriter::new(-1, Instant::now()).admit(0),
            Admission::InvalidNegativeTarget
        );
    }
}

#[cfg(test)]
mod source_oracle {
    use super::*;
    use serde_json::{Value, json};
    use std::time::Duration;
    #[test]
    #[ignore = "requires actual fixed Go writer rows via GO_DATA_TRACK_WRITER_RESULTS"]
    fn actual_go_writer_rows_match_all_supplied_operations() {
        let actual =
            std::fs::read_to_string(std::env::var("GO_DATA_TRACK_WRITER_RESULTS").unwrap())
                .unwrap();
        let expected: Vec<Value> = actual
            .lines()
            .filter(|l| !l.is_empty())
            .map(|line| serde_json::from_str(line).unwrap())
            .collect();
        let mut rows = vec![];
        for line in include_str!("../tests/oracles/datachannel-writer/oracle/cases.jsonl").lines() {
            let case: Value = serde_json::from_str(line).unwrap();
            let start = Instant::now();
            let mut writer = DataTrackWriter::new(case["target_ns"].as_i64().unwrap(), start);
            let mut writes = 0;
            let mut deadlines = 0;
            let mut deadline_delta = 0;
            for (index, op) in case["operations"].as_array().unwrap().iter().enumerate() {
                let at = start + Duration::from_nanos(op["at_ns"].as_u64().unwrap());
                let before = op["before"].as_u64().unwrap() as usize;
                let mut buffered = before;
                let mut n = 0;
                let mut error = "";
                let mut source_panics = false;
                match writer.admit_with_minimum(before, case["minimum"].as_u64().unwrap()) {
                    Admission::HighBuffered => error = "high_buffered",
                    Admission::InvalidNegativeTarget => source_panics = true,
                    Admission::Write => {
                        deadlines += 1;
                        deadline_delta = 50_000_000;
                        if op["deadline_error"].as_bool().unwrap_or(false) {
                            error = "closed_pipe";
                        } else {
                            writes += 1;
                            buffered = op["after"].as_u64().unwrap() as usize;
                            n = op["write_n"]
                                .as_u64()
                                .unwrap_or_else(|| op["bytes"].as_u64().unwrap())
                                as usize;
                            error = match op["error"].as_str().unwrap_or("") {
                                "timeout" => "deadline",
                                "closed" => "closed_pipe",
                                _ => "",
                            };
                            writer.record(n, buffered, at);
                        }
                    }
                }
                let bitrate = writer.rate.as_ref().and_then(|r| r.bitrate());
                rows.push(json!({"id":case["id"],"index":index,"n":n,"error":error,"panic":source_panics,"raw_writes":writes,"deadline_calls":deadlines,"deadline_delta_ns":deadline_delta,"bitrate":bitrate.unwrap_or(0),"rate_ready":bitrate.is_some(),"buffered":buffered}));
            }
        }
        assert_eq!(rows.len(), 131);
        assert_eq!(expected.len(), rows.len());
        for (index, (actual, expected)) in rows.iter().zip(expected.iter()).enumerate() {
            assert_eq!(actual, expected, "actual source writer row {index}");
        }
    }
}
