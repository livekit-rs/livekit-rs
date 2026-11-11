// Copyright2023 LiveKit, Inc. SPDX-License-Identifier: Apache-2.0
//! Fixed ccutils/prober.go uniform Cluster initProbes/Process, explicit clock.
use std::time::{Duration, Instant};
const MIN_BYTES: i64 = 1100;
#[derive(Debug)]
pub(super) struct Uniform {
    pub desired_bytes: i64,
    pub count: u64,
    pub base: Duration,
    pub bytes_per_bucket: i64,
    pub index: u64,
    started: Option<Instant>,
}
impl Uniform {
    pub fn new(desired: i64, expected: i64, duration: Duration) -> Self {
        let seconds = duration.as_secs_f64();
        let desired_bytes = (desired as f64 * seconds / 8.0 + 0.5).round() as i64;
        let count = (seconds / 0.020 + 0.5).round().max(1.0) as u64;
        let base = Duration::from_nanos(
            (duration.as_nanos() / u128::from(count)).min(u64::MAX as u128) as u64,
        )
        .max(Duration::from_millis(10));
        let intervals = (seconds / base.as_secs_f64() + 0.5).round();
        let bytes_per_bucket =
            ((seconds * (desired - expected) as f64 / 8.0 + intervals - 1.0) / intervals + 0.5)
                .round() as i64;
        Self {
            desired_bytes,
            count,
            base,
            bytes_per_bucket,
            index: 0,
            started: None,
        }
    }
    pub fn poll(&mut self, now: Instant, actual_probe_bytes: i64) -> i64 {
        let Some(started) = self.started else {
            self.started = Some(now);
            return MIN_BYTES;
        };
        let deadline = self
            .base
            .as_nanos()
            .saturating_mul(u128::from(self.index + 1));
        if now.duration_since(started).as_nanos() > deadline {
            self.index += 1;
            let overflow = self.index >= self.count;
            self.index = self.index.min(self.count - 1);
            let target = (self.index + 1) as i64 * self.bytes_per_bucket;
            if target > actual_probe_bytes || overflow {
                (target - actual_probe_bytes).max(MIN_BYTES)
            } else {
                0
            }
        } else {
            0
        }
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn only_actual_probes_advance_bytes_and_large_stalls_advance_one_source_bucket() {
        let now = Instant::now();
        let mut u = Uniform::new(1_000_000, 800_000, Duration::from_millis(200));
        assert_eq!(u.poll(now, 0), 1100);
        assert_eq!(u.poll(now, 0), 0);
        assert_eq!(u.index, 0);
        let _ = u.poll(now + Duration::from_secs(1), 0);
        assert_eq!(u.index, 1);
        let _ = u.poll(now + Duration::from_secs(1), 0);
        assert_eq!(u.index, 2);
        // Requesting did not claim a single byte was sent. The caller supplies actual output.
        assert_eq!(u.poll(now + Duration::from_secs(1), 1_000_000), 0);
    }
}

#[cfg(test)]
mod official_oracle {
    use super::*;
    #[test]
    #[ignore = "requires actual fixed Go source result REMOTE_UNIFORM_ORACLE"]
    fn actual_go_uniform_bucket_operations() {
        let results = std::fs::read_to_string(
            std::env::var("REMOTE_UNIFORM_ORACLE").expect("actual Go output required"),
        )
        .unwrap();
        let expected: Vec<serde_json::Value> = results
            .lines()
            .map(|line| serde_json::from_str(line).unwrap())
            .collect();
        let cases: serde_json::Value = serde_json::from_str(include_str!(
            "../../../../tests/oracles/feedback-runtime/uniform-cases.json"
        ))
        .unwrap();
        let mut offset = 0;
        for case in cases.as_array().unwrap() {
            let mut schedule = Uniform::new(
                case["Desired"].as_i64().unwrap(),
                case["Expected"].as_i64().unwrap(),
                Duration::from_nanos(case["DurationNS"].as_u64().unwrap()),
            );
            let origin = Instant::now();
            let mut actual = 0;
            for tick in case["Ticks"].as_array().unwrap() {
                let row = &expected[offset];
                offset += 1;
                assert_eq!(row["case"], case["Name"]);
                assert_eq!(row["at_ns"], tick["AtNS"]);
                let requested = schedule.poll(
                    origin + Duration::from_nanos(tick["AtNS"].as_u64().unwrap()),
                    actual,
                );
                assert_eq!(row["requested"].as_i64().unwrap(), requested);
                assert_eq!(row["actual_before"].as_i64().unwrap(), actual);
                assert_eq!(
                    row["desired_bytes"].as_i64().unwrap(),
                    schedule.desired_bytes
                );
                assert_eq!(row["count"].as_u64().unwrap(), schedule.count);
                assert_eq!(row["index"].as_u64().unwrap(), schedule.index);
                assert_eq!(
                    row["base_ns"].as_u64().unwrap() as u128,
                    schedule.base.as_nanos()
                );
                assert_eq!(
                    row["bytes_per_bucket"].as_i64().unwrap(),
                    schedule.bytes_per_bucket
                );
                actual += tick["ActualBytes"].as_i64().unwrap();
            }
        }
        assert_eq!(offset, expected.len());
        assert_eq!(offset, 2250);
    }
}
