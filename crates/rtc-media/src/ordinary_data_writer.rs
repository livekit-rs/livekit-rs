//! Ordinary (protobuf packet) data writer policy. Source DataChannelWriter, not
//! the independent DataTrack channel. All samples come from actual SCTP writes.
use crate::data_track_writer::{Admission, DataTrackWriter, Rate};
use std::time::{Duration, Instant};

pub(crate) const WRITE_TIMEOUT: Duration = Duration::from_millis(50);
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Dropped {
    MaxBuffered,
    HighBuffered,
    Deadline,
    SlowReader,
    SourceInvalidPolicy,
}
#[derive(Debug)]
pub(crate) struct Writer {
    reliable: bool,
    slow_threshold: i64,
    max_buffered: u64,
    rate: Option<Rate>,
    lossy: DataTrackWriter,
    deadline: Option<Instant>,
    context_epoch: u64,
    raw_owner: Option<(u64, u64)>,
    raw_generation: Option<u64>,
}
impl Writer {
    pub(crate) fn new(reliable: bool, slow: i64, max: u64, lossy_ns: i64, now: Instant) -> Self {
        Self {
            reliable,
            slow_threshold: slow,
            max_buffered: max,
            rate: (reliable && slow > 0).then(|| Rate::new(now)),
            lossy: DataTrackWriter::new(lossy_ns, now),
            deadline: None,
            context_epoch: 0,
            raw_owner: None,
            raw_generation: None,
        }
    }
    /// Raw SCTP stream deadlines are shared across concurrent callers. Extending
    /// an unexpired deadline retains the same live context; resetting an already
    /// expired deadline creates a new context and cannot revive an older waiter.
    pub(crate) fn set_deadline(&mut self, now: Instant) {
        if self.deadline.is_some_and(|at| now >= at) {
            self.context_epoch = self
                .context_epoch
                .checked_add(1)
                .expect("write deadline context exhausted");
        }
        self.deadline = Some(now + WRITE_TIMEOUT);
    }
    pub(crate) fn lock_raw(&mut self, id: u64) -> Option<u64> {
        match self.raw_owner {
            Some((owner, epoch)) if owner == id => Some(epoch),
            Some(_) => None,
            None => {
                self.raw_owner = Some((id, self.context_epoch));
                Some(self.context_epoch)
            }
        }
    }
    pub(crate) fn release_raw(&mut self, id: u64) {
        if self.raw_owner.is_some_and(|(owner, _)| owner == id) {
            self.raw_owner = None;
            self.raw_generation = None;
        }
    }
    /// Pion BlockWrite captures associationGeneration after obtaining the raw
    /// writer. A peer restart interrupts this call; it cannot silently replay
    /// a previously blocked write on the replacement protocol generation.
    pub(crate) fn retain_generation(&mut self, generation: u64) -> bool {
        match self.raw_generation {
            Some(captured) => captured == generation,
            None => {
                self.raw_generation = Some(generation);
                true
            }
        }
    }

    pub(crate) fn expired(&self, epoch: u64, now: Instant) -> bool {
        epoch < self.context_epoch || self.deadline.is_some_and(|at| now >= at)
    }
    pub(crate) fn wake_deadline(&self, now: Instant) -> Option<Instant> {
        let (_, epoch) = self.raw_owner?;
        if epoch < self.context_epoch {
            Some(now)
        } else {
            self.deadline
        }
    }

    /// PCTransport's check is outside the wrapper retry loop and is shared by
    /// reliable/lossy. A configured slow threshold disables this deprecated cap.
    pub(crate) fn begin(&self, buffered: usize) -> Result<(), Dropped> {
        if self.slow_threshold == 0 && self.max_buffered > 0 && buffered as u64 > self.max_buffered
        {
            return Err(Dropped::MaxBuffered);
        }
        if self.reliable {
            // Original negative reliable threshold dereferences its nil rate;
            // keep that invalid runtime domain explicit, never clamp/bypass it.
            if self.slow_threshold < 0 {
                return Err(Dropped::SourceInvalidPolicy);
            }
            return Ok(());
        }
        match self.lossy.admit(buffered) {
            Admission::Write => Ok(()),
            Admission::HighBuffered => Err(Dropped::HighBuffered),
            Admission::InvalidNegativeTarget => Err(Dropped::SourceInvalidPolicy),
        }
    }
    /// Result of one actual raw write, including an elapsed blocked-write
    /// deadline with zero written bytes. true means original wrapper retries.
    pub(crate) fn result(
        &mut self,
        written: usize,
        buffered: usize,
        timed_out: bool,
        now: Instant,
    ) -> Result<bool, Dropped> {
        if self.reliable {
            if self.slow_threshold == 0 {
                return if timed_out {
                    Err(Dropped::Deadline)
                } else {
                    Ok(false)
                };
            }
            let Some(rate) = &mut self.rate else {
                return Err(Dropped::SourceInvalidPolicy);
            };
            rate.add(written, buffered, now);
            if timed_out {
                if rate.bitrate().is_none_or(|bps| bps >= self.slow_threshold) {
                    return Ok(true);
                }
                return Err(Dropped::SlowReader);
            }
            Ok(false)
        } else {
            self.lossy.record(written, buffered, now);
            if timed_out {
                Err(Dropped::Deadline)
            } else {
                Ok(false)
            }
        }
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn peer_restart_terminates_old_raw_attempt_but_allows_fresh_call() {
        let now = Instant::now();
        let mut writer = Writer::new(true, 1, 0, 0, now);
        writer.set_deadline(now);
        assert!(writer.lock_raw(1).is_some());
        assert!(writer.retain_generation(0));
        assert!(writer.lock_raw(2).is_none());
        assert!(
            !writer.retain_generation(1),
            "old blocked call must terminate, not retry its data"
        );
        writer.release_raw(1);
        assert!(writer.lock_raw(2).is_some());
        assert!(
            writer.retain_generation(1),
            "new call is allowed on the still-open channel"
        );
        writer.release_raw(2);
    }

    #[test]
    fn shared_raw_deadline_cannot_revive_an_expired_waiter() {
        let now = Instant::now();
        let mut w = Writer::new(true, 1, 0, 0, now);
        w.set_deadline(now);
        let epoch = w.lock_raw(1).unwrap();
        assert_eq!(w.lock_raw(2), None);
        w.set_deadline(now + Duration::from_millis(20));
        assert!(!w.expired(epoch, now + Duration::from_millis(60)));
        w.set_deadline(now + Duration::from_millis(71));
        assert!(w.expired(epoch, now + Duration::from_millis(71)));
        w.release_raw(1);
        assert!(w.lock_raw(2).unwrap() > epoch);
    }
    #[test]
    fn deprecated_max_buffer_limit_is_strict_and_disabled_by_slow_policy() {
        let now = Instant::now();
        for reliable in [false, true] {
            let w = Writer::new(reliable, 0, 100, 0, now);
            assert_eq!(w.begin(100), Ok(()));
            assert_eq!(w.begin(101), Err(Dropped::MaxBuffered));
            assert_eq!(Writer::new(reliable, 1, 100, 0, now).begin(101), Ok(()));
        }
    }
    #[test]
    #[ignore = "actual fixed Go ordinary-writer20 JSONL and immutable source input required"]
    fn actual_go_ordinary_writer_all_twenty_results() {
        use serde_json::{Value, json};
        let cases: Value = serde_json::from_str(include_str!(
            "../../../tests/oracles/ordinary-data-writer/cases.json"
        ))
        .unwrap();
        let text =
            std::fs::read_to_string(std::env::var("ORDINARY_WRITER_RESULTS").unwrap()).unwrap();
        let rows: Vec<Value> = text
            .lines()
            .map(|line| serde_json::from_str(line).unwrap())
            .collect();
        assert_eq!(rows.len(), 20);
        let mut cursor = 0;
        let mut unique = std::collections::BTreeSet::new();
        for c in cases.as_array().unwrap() {
            let id = c["id"].as_str().unwrap();
            assert!(unique.insert(id));
            let reliable = c["reliable"].as_bool().unwrap();
            let now = Instant::now();
            let mut at = now;
            let mut buffered = 0;
            let mut w = Writer::new(
                reliable,
                c["slow"].as_i64().unwrap(),
                0,
                c["target_ns"].as_i64().unwrap(),
                now,
            );
            for (index, history) in c["calls"].as_array().unwrap().iter().enumerate() {
                let mut used = Vec::<Value>::new();
                let mut deadlines = Vec::new();
                let mut n = 0;
                let mut family = "ok";
                let mut i = 0;
                match w.begin(buffered) {
                    Err(Dropped::HighBuffered) => family = "high",
                    Err(other) => panic!("unexpected policy admission {other:?}"),
                    Ok(()) => loop {
                        w.set_deadline(at);
                        deadlines.push(WRITE_TIMEOUT.as_nanos() as u64);
                        let v = history
                            .as_array()
                            .unwrap()
                            .get(i)
                            .expect("actual source action count");
                        i += 1;
                        used.push(v.clone());
                        at = now + Duration::from_nanos(v["at_ns"].as_u64().unwrap());
                        n = v["n"].as_u64().unwrap();
                        buffered = v["buffered"].as_u64().unwrap() as usize;
                        let timeout = v["timeout"].as_bool().unwrap();
                        match w.result(n as usize, buffered, timeout, at) {
                            Ok(true) => continue,
                            Ok(false) => {
                                if v["closed"].as_bool().unwrap() {
                                    family = "closed"
                                };
                                break;
                            }
                            Err(Dropped::Deadline) => {
                                family = "deadline";
                                break;
                            }
                            Err(Dropped::SlowReader) => {
                                family = "slow";
                                break;
                            }
                            Err(other) => panic!("unexpected writer outcome {other:?}"),
                        }
                    },
                }
                let rate = if reliable {
                    w.rate.as_ref().and_then(|r| r.bitrate())
                } else {
                    w.lossy.source_bitrate_for_test()
                };
                let expected = json!({"id":id,"call":index,"n":n,"family":family,"raw_calls":i,
                    "deadlines_ns":if deadlines.is_empty(){Value::Null}else{json!(deadlines)},
                    "used":if used.is_empty(){Value::Null}else{json!(used)},"buffered":buffered,"bitrate":rate.unwrap_or(0),"bitrate_valid":rate.is_some()});
                assert_eq!(rows[cursor], expected, "source full result {id}/{index}");
                cursor += 1;
            }
        }
        assert_eq!(unique.len(), 12);
        assert_eq!(cursor, rows.len());
    }
}
