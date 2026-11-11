//! TWCC delta quantization relative to the previously encoded timestamp.
//!
//! Preserve the remainder instead of truncating each actual interarrival gap.
//! Reference: Google WebRTC transport_feedback.cc::AddReceivedPacket, commit
//! 202acee882062f7ca632472e4810f25938828f6a (BSD-3-Clause).
use std::time::{Duration, Instant};

pub(super) fn quantize_delta(previous: Instant, received: Instant) -> Option<(i16, Instant)> {
    let difference = if received >= previous {
        received.duration_since(previous).as_micros() as i128
    } else {
        -(previous.duration_since(received).as_micros() as i128)
    };
    // Rust integer division truncates toward zero; offset both signs to round
    // half ticks away from zero, as in the reference encoder.
    let ticks = if difference >= 0 {
        (difference + 125) / 250
    } else {
        (difference - 125) / 250
    };
    let ticks = i16::try_from(ticks).ok()?;
    let offset = Duration::from_micros(u64::from(ticks.unsigned_abs()) * 250);
    let encoded = if ticks >= 0 {
        previous.checked_add(offset)?
    } else {
        previous.checked_sub(offset)?
    };
    Some((ticks, encoded))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn distance(a: Instant, b: Instant) -> Duration {
        a.max(b).duration_since(a.min(b))
    }

    #[test]
    fn sub_tick_gaps_preserve_elapsed_time_instead_of_accumulating_truncation() {
        let start = Instant::now();
        let mut encoded = start;
        let mut total_ticks = 0i64;
        for n in 0..=10_000 {
            let actual = start + Duration::from_micros(n * 100);
            let (ticks, next) = quantize_delta(encoded, actual).unwrap();
            total_ticks += i64::from(ticks);
            encoded = next;
            assert!(distance(actual, encoded) <= Duration::from_micros(125));
        }
        assert_eq!(total_ticks, 4_000);
        assert_eq!(encoded, start + Duration::from_secs(1));
    }

    #[test]
    fn rounding_boundaries_and_negative_reordered_arrivals() {
        let base = Instant::now() + Duration::from_secs(10);
        for (micros, expected) in [
            (0i64, 0),
            (124, 0),
            (125, 1),
            (249, 1),
            (-124, 0),
            (-125, -1),
            (-249, -1),
        ] {
            let actual = if micros < 0 {
                base - Duration::from_micros(micros.unsigned_abs())
            } else {
                base + Duration::from_micros(micros as u64)
            };
            let (ticks, encoded) = quantize_delta(base, actual).unwrap();
            assert_eq!(ticks, expected);
            assert!(distance(actual, encoded) <= Duration::from_micros(125));
        }
        let mut encoded = base;
        for micros in [2_001u64, 1_873, 2_999, 17, 64_001, 63_876, 128_004] {
            let actual = base + Duration::from_micros(micros);
            encoded = quantize_delta(encoded, actual).unwrap().1;
            assert!(distance(actual, encoded) <= Duration::from_micros(125));
        }
    }

    #[test]
    fn encoded_tick_limits_trigger_new_report_without_overflow() {
        let base = Instant::now() + Duration::from_secs(10);
        assert_eq!(
            quantize_delta(base, base + Duration::from_micros(8_191_874))
                .unwrap()
                .0,
            i16::MAX
        );
        assert!(quantize_delta(base, base + Duration::from_micros(8_191_875)).is_none());
        assert_eq!(
            quantize_delta(base, base - Duration::from_micros(8_192_124))
                .unwrap()
                .0,
            i16::MIN
        );
        assert!(quantize_delta(base, base - Duration::from_micros(8_192_125)).is_none());
    }
}
