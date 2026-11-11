//! Fixed protocol 9bb708cdb819 logger/zaputil/sampler.go policy.
//! Counts are shared across components/callers/fields, keyed only by level and
//! XXH3(message) modulo 4096. A window starts at its first admitted event, not
//! at a wall-clock second boundary. Keep the source's signed-to-u64 conversion.
use std::sync::Mutex;
use std::time::Duration;

const BUCKETS: usize = 4096;
#[derive(Clone, Copy, Default)]
struct Counter {
    reset_at: u128,
    count: u64,
}

pub(super) struct Sampler {
    counters: Mutex<Vec<Counter>>,
    tick: Duration,
    first: u64,
    thereafter: u64,
}
impl Sampler {
    pub(super) fn new(tick: Duration, first: i64, thereafter: i64) -> Self {
        Self {
            counters: Mutex::new(vec![Counter::default(); 7 * BUCKETS]),
            tick,
            first: first as u64,
            thereafter: thereafter as u64,
        }
    }
    pub(super) fn allows(&self, rank: i8, message: &str, now: Duration) -> bool {
        if !(-1..=5).contains(&rank) {
            return true;
        }
        let bucket = xxhash_rust::xxh3::xxh3_64(message.as_bytes()) as usize % BUCKETS;
        let mut counters = self.counters.lock().unwrap_or_else(|e| e.into_inner());
        let c = &mut counters[(rank + 1) as usize * BUCKETS + bucket];
        if c.reset_at > now.as_nanos() {
            c.count = c.count.wrapping_add(1);
        } else {
            c.count = 1;
            c.reset_at = now.as_nanos().saturating_add(self.tick.as_nanos());
        }
        c.count <= self.first
            || (self.thereafter != 0 && (c.count - self.first) % self.thereafter == 0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn first_then_every_m_and_exact_window_boundary() {
        let s = Sampler::new(Duration::from_secs(1), 2, 3);
        let t = Duration::from_millis(150);
        let decisions: Vec<_> = (0..9).map(|_| s.allows(0, "same", t)).collect();
        assert_eq!(
            decisions,
            [true, true, false, false, true, false, false, true, false]
        );
        assert!(!s.allows(0, "same", Duration::from_millis(1149)));
        assert!(s.allows(0, "same", Duration::from_millis(1150)));
    }
    #[test]
    fn levels_separate_but_hash_collisions_share_bounded_counter() {
        let s = Sampler::new(Duration::from_secs(1), 1, 0);
        let t = Duration::from_secs(1);
        assert!(s.allows(0, "same", t));
        assert!(!s.allows(0, "same", t));
        assert!(s.allows(1, "same", t));
        let bucket = xxhash_rust::xxh3::xxh3_64(b"same") % BUCKETS as u64;
        let collision = (0..100_000)
            .map(|n| format!("collision-{n}"))
            .find(|m| xxhash_rust::xxh3::xxh3_64(m.as_bytes()) % BUCKETS as u64 == bucket)
            .expect("bounded collision fixture");
        assert!(!s.allows(0, &collision, t));
        assert!(s.allows(-2, "same", t));
    }
    #[test]
    fn zero_and_signed_go_conversion_are_not_clamped() {
        let t = Duration::from_secs(1);
        let never = Sampler::new(Duration::from_secs(1), 0, 0);
        assert!(!never.allows(0, "m", t));
        let every = Sampler::new(Duration::from_secs(1), 0, 2);
        assert!(!every.allows(0, "m", t));
        assert!(every.allows(0, "m", t));
        let negative_first = Sampler::new(Duration::from_secs(1), -1, 0);
        assert!((0..100).all(|_| negative_first.allows(0, "m", t)));
        let negative_interval = Sampler::new(Duration::from_secs(1), 1, -1);
        assert!(negative_interval.allows(0, "m", t));
        assert!(!negative_interval.allows(0, "m", t));
    }
}
