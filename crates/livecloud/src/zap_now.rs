//! Unix wall-clock helpers for the zap console/JSON encoders.
//!
//! Exists so `crates/livecloud/src/zap.rs` can format timestamps without a
//! calendar crate (RUSTSEC-2026-0543: chrono is unmaintained with no patched
//! version). Both halves mirror what Go's `time.Local` does for the layout
//! `2006-01-02T15:04:05.000-0700`: read the process TZ through the platform C
//! library and let it resolve the offset for the given instant, so DST and
//! historical rules come from the system database rather than a Rust table.
//!
//! The example `crates/livecloud/examples/logging_policy_probe.rs` re-includes
//! `src/zap.rs` under its own crate root, so this module is included there too
//! and must not use `crate::` paths of its own.

/// Seconds since the Unix epoch (negative before 1970) and the non-negative
/// sub-second remainder, floored so `unix * 1_000_000_000 + nanos` always equals the
/// instant. A wall clock set before 1970 therefore logs 1969 rather than aborting the
/// process from inside a log call, which is what Go's `time.Now()` does.
fn epoch_from_elapsed(elapsed: Result<std::time::Duration, std::time::Duration>) -> (i64, u32) {
    match elapsed {
        Ok(since) => (since.as_secs() as i64, since.subsec_nanos()),
        Err(before) => {
            let (secs, nanos) = (before.as_secs() as i64, before.subsec_nanos());
            if nanos == 0 {
                (-secs, 0)
            } else {
                (-secs - 1, 1_000_000_000 - nanos)
            }
        }
    }
}

/// Seconds since the Unix epoch plus the millisecond remainder.
pub fn unix_now_millis() -> (i64, u32) {
    let (secs, nanos) = now_since_epoch();
    (secs, nanos / 1_000_000)
}

/// Seconds since the Unix epoch plus the full sub-second nanosecond remainder,
/// which zap's `EpochSeconds` encoder needs to reproduce Go's float rendering.
pub fn unix_now_nanos() -> (i64, u32) {
    now_since_epoch()
}

fn now_since_epoch() -> (i64, u32) {
    let now = std::time::SystemTime::now();
    // A clock set before 1970 reports the distance to the epoch as the error, so the
    // error arm is the pre-epoch case rather than a failure.
    let elapsed = now
        .duration_since(std::time::UNIX_EPOCH)
        .map_err(|before_epoch| before_epoch.duration());
    epoch_from_elapsed(elapsed)
}

/// Local civil date/time for a UTC epoch second, plus the offset in seconds that
/// was in effect at that instant (east positive), as the platform reports it.
///
/// Returns `None` when the platform cannot represent the instant or when built
/// for a non-Unix target; callers fall back to UTC so logging never fails
/// because of a clock outlier.
pub fn local_parts_with_offset(unix: i64) -> Option<(i64, u32, u32, u32, u32, u32, i64)> {
    #[cfg(unix)]
    {
        // SAFETY: `tm` is a POD struct that is legal zero-initialised, and
        // `localtime_r` writes only through the pointer we hand it on a
        // thread-owned stack value. The return either aliases `tm` or is null,
        // and `tm` is read only after a non-null return.
        // `libc::time_t` is i64 on Apple and glibc but i32 on some targets, so the
        // epoch goes through `TryFrom`. `tm_gmtoff` is i64 on Apple and i32 on glibc;
        // coercing it in a `let` binding widens either way without tripping clippy's
        // useless-conversion lint on the platform that already matches the field type.
        let stamp = libc::time_t::try_from(unix).ok()?;
        let mut tm: libc::tm = unsafe { std::mem::zeroed() };
        if unsafe { libc::localtime_r(&stamp, &mut tm) }.is_null() {
            return None;
        }
        // tm_year counts years since 1900 and tm_mon is zero-based.
        let year = i64::from(tm.tm_year) + 1900;
        let month = u32::try_from(tm.tm_mon + 1).ok()?;
        let day = u32::try_from(tm.tm_mday).ok()?;
        let hour = u32::try_from(tm.tm_hour).ok()?;
        let min = u32::try_from(tm.tm_min).ok()?;
        let sec = u32::try_from(tm.tm_sec).ok()?;
        let gmtoff: i64 = tm.tm_gmtoff as _;
        Some((year, month, day, hour, min, sec, gmtoff))
    }
    #[cfg(not(unix))]
    {
        let _ = unix;
        None
    }
}

/// zap-compatible local parts: `(year, month, day, hour, min, sec, offset_secs)`.
/// Falls back to UTC (offset 0) when the platform rejects the instant.
pub fn local_parts(unix: i64) -> (i64, u32, u32, u32, u32, u32, i64) {
    match local_parts_with_offset(unix) {
        Some(parts) => parts,
        None => {
            let days = unix.div_euclid(86_400);
            let secs = unix.rem_euclid(86_400);
            let (year, month, day) = civil_from_days(days);
            (
                year,
                month,
                day,
                (secs / 3600) as u32,
                ((secs % 3600) / 60) as u32,
                (secs % 60) as u32,
                0_i64,
            )
        }
    }
}

/// Civil date from days since 1970-01-01 (may be negative), proleptic Gregorian.
/// Howard Hinnant's algorithm; integer-only, no table, no crate.
pub fn civil_from_days(z: i64) -> (i64, u32, u32) {
    let z = z + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = z - era * 146_097; // [0, 146096]
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146_096) / 365; // [0, 399]
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100); // [0, 365]
    let mp = (5 * doy + 2) / 153; // [0, 11]
    let d = (doy - (153 * mp + 2) / 5 + 1) as u32; // [1, 31]
    let m = if mp < 10 { mp + 3 } else { mp - 9 } as u32;
    (if m <= 2 { y + 1 } else { y }, m, d)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The pure UTC half must be exact across the boundaries a calendar library
    /// usually hides: epoch start, pre-epoch, both leap-day transitions, and a
    /// century year that is not a leap year.
    /// Day numbers below are from the Python `datetime` ordinal difference, so a
    /// wrong constant cannot be hidden by a matching wrong expectation.
    #[test]
    fn utc_fallback_matches_known_dates() {
        assert_eq!(civil_from_days(0), (1970, 1, 1));
        assert_eq!(civil_from_days(-1), (1969, 12, 31));
        assert_eq!(civil_from_days(20_089), (2025, 1, 1));
        assert_eq!(civil_from_days(20_454), (2026, 1, 1));
        assert_eq!(civil_from_days(20_819), (2027, 1, 1));
        // 2024-02-29 exists, 2100-02-29 does not (not divisible by 400).
        assert_eq!(civil_from_days(19_782), (2024, 2, 29));
        assert_eq!(civil_from_days(19_783), (2024, 3, 1));
        assert_eq!(civil_from_days(47_540), (2100, 2, 28));
        assert_eq!(civil_from_days(47_541), (2100, 3, 1));
        // The pinned zap epoch used by the encoder tests is 2026-11-03 UTC.
        assert_eq!(civil_from_days(1_793_742_365 / 86_400), (2026, 11, 3));
    }

    /// The platform `localtime_r` breakdown must equal the pure UTC arithmetic
    /// shifted by the offset the platform itself reports. That identity holds for
    /// any host timezone, so the test needs no process-global TZ manipulation and
    /// stays valid under the suite's default multi-threaded runner.
    #[cfg(unix)]
    #[test]
    fn localtime_r_agrees_with_the_pure_utc_arithmetic() {
        let unix = 1_793_742_365_i64; // 2026-11-03T21:46:05Z
        let (uy, um, ud, uh, umin, us, _) = {
            let days = unix.div_euclid(86_400);
            let secs = unix.rem_euclid(86_400);
            let (y, m, d) = civil_from_days(days);
            (
                y,
                m,
                d,
                (secs / 3600) as u32,
                ((secs % 3600) / 60) as u32,
                (secs % 60) as u32,
                0_i64,
            )
        };
        assert_eq!(
            (uy, um, ud, uh, umin, us),
            (2026, 11, 3, 21, 46, 5),
            "the pinned instant must split as documented"
        );
        let (y, mo, d, h, mi, s, offset) =
            local_parts_with_offset(unix).expect("platform must represent a 2026 instant");
        // Offsets are whole minutes and bounded by ±14h in the IANA database.
        assert!(offset.abs() <= 14 * 3600, "implausible offset {offset}");
        assert_eq!(offset % 60, 0, "offset must be a whole number of minutes");
        // local == utc + offset, recomputed through the epoch to avoid hand-coding
        // month/day carry rules in the test.
        let shifted = unix + offset;
        let days = shifted.div_euclid(86_400);
        let secs = shifted.rem_euclid(86_400);
        let (ey, em, ed) = civil_from_days(days);
        assert_eq!(
            (y, mo, d, h, mi, s),
            (
                ey,
                em,
                ed,
                (secs / 3600) as u32,
                ((secs % 3600) / 60) as u32,
                (secs % 60) as u32
            ),
            "TZ offset {offset} must reconcile local parts with the UTC instant"
        );
    }

    #[test]
    fn unix_now_millis_is_sane() {
        let (unix, millis) = unix_now_millis();
        assert!(unix > 1_700_000_000, "clock returned {unix}");
        assert!(millis < 1000, "millis out of range: {millis}");
    }

    /// A clock behind 1970 must produce a floored second with a non-negative fraction,
    /// not a panic and not a truncated-toward-zero second. The nanosecond reconstruction
    /// is the invariant, so each case states the instant it represents.
    #[test]
    fn elapsed_before_the_epoch_floors_instead_of_panicking() {
        use std::time::Duration;
        let cases = [
            // Exactly the epoch, and instants after it.
            (Ok(Duration::ZERO), (0_i64, 0_u32)),
            (Ok(Duration::new(1, 500_000_000)), (1, 500_000_000)),
            // 0.5 s before the epoch is still inside 1969-12-31T23:59:59.5.
            (Err(Duration::new(0, 500_000_000)), (-1, 500_000_000)),
            // Whole-second magnitudes keep a zero fraction.
            (Err(Duration::new(1, 0)), (-1, 0)),
            (Err(Duration::new(2, 1)), (-3, 999_999_999)),
        ];
        for (elapsed, expected) in cases {
            assert_eq!(epoch_from_elapsed(elapsed), expected, "for {elapsed:?}");
            let (secs, nanos) = expected;
            assert_eq!(
                secs * 1_000_000_000 + i64::from(nanos),
                match elapsed {
                    Ok(d) => d.as_nanos() as i64,
                    Err(d) => -(d.as_nanos() as i64),
                },
                "{secs}.{nanos} must name the same instant as {elapsed:?}"
            );
        }
    }

    /// The floored pre-epoch second must name the day before the epoch in UTC, which is
    /// what the `local_parts` fallback renders and what Go prints for the same instant.
    #[test]
    fn pre_epoch_second_is_the_day_before_in_utc() {
        let (secs, nanos) = epoch_from_elapsed(Err(std::time::Duration::new(0, 500_000_000)));
        assert_eq!((secs, nanos), (-1, 500_000_000));
        assert_eq!(
            civil_from_days(secs.div_euclid(86_400)),
            (1969, 12, 31),
            "second {secs}"
        );
    }
}
