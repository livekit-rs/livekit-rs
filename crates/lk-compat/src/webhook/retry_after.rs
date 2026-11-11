use std::sync::LazyLock;
use std::time::{Duration, SystemTime};

use regex::Regex;
use time::{Date, Month, OffsetDateTime, Time};

const NANOS_PER_SECOND: i64 = 1_000_000_000;
const SHORT_MONTHS: [&str; 12] = [
    "Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec",
];

pub(super) fn parse(value: &str, now: SystemTime) -> Option<Duration> {
    if let Ok(seconds) = value.parse::<i64>() {
        if seconds < 0 {
            return None;
        }
        // Go multiplies two signed time.Duration values. A wrapped negative
        // delay fires its timer immediately, like a delay of zero.
        let nanos = seconds.wrapping_mul(NANOS_PER_SECOND).max(0);
        return Some(Duration::from_nanos(nanos as u64));
    }

    let date = parse_rfc1123_gmt(value)?;
    let nanos = (date - OffsetDateTime::from(now))
        .whole_nanoseconds()
        .clamp(0, i128::from(i64::MAX));
    Some(Duration::from_nanos(nanos as u64))
}

fn parse_rfc1123_gmt(value: &str) -> Option<OffsetDateTime> {
    // Go uses time.RFC1123, so legacy RFC850 and asctime forms are not
    // accepted. Its parser recognizes the weekday without validating it.
    static DATE: LazyLock<Regex> = LazyLock::new(|| {
        Regex::new(concat!(
            r"^(?i:Mon|Tue|Wed|Thu|Fri|Sat|Sun), +([0-9]{2}) +([A-Za-z]{3}) +([0-9]{4}) +",
            r"([0-9]{1,2}):([0-9]{2}):([0-9]{2})(?:[.,]([0-9]+))? +(GMT(?:[+-][0-9]+)?|UTC)$",
        ))
        .expect("static RFC1123 date pattern")
    });
    let parts = DATE.captures(value)?;
    let month = SHORT_MONTHS
        .iter()
        .position(|m| m.eq_ignore_ascii_case(&parts[2]))?;
    let date = Date::from_calendar_date(
        parts[3].parse().ok()?,
        Month::try_from(month as u8 + 1).ok()?,
        parts[1].parse().ok()?,
    )
    .ok()?;
    let nanos = parts.get(7).map_or(0, |fraction| {
        fraction
            .as_str()
            .bytes()
            .take(9)
            .zip([
                100_000_000,
                10_000_000,
                1_000_000,
                100_000,
                10_000,
                1_000,
                100,
                10,
                1,
            ])
            .map(|(digit, factor)| u32::from(digit - b'0') * factor)
            .sum()
    });
    let clock = Time::from_hms_nano(
        parts[4].parse().ok()?,
        parts[5].parse().ok()?,
        parts[6].parse().ok()?,
        nanos,
    )
    .ok()?;
    let offset: i8 = match &parts[8] {
        "GMT" | "UTC" => 0,
        gmt => gmt[3..].parse().ok()?,
    };
    if offset.unsigned_abs() > 23 {
        return None;
    }
    Some(date.with_time(clock).assume_utc() - time::Duration::hours(i64::from(offset)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::UNIX_EPOCH;

    #[test]
    fn signed_seconds_match_go_duration_and_overflow() {
        let now = UNIX_EPOCH;
        assert_eq!(parse("-1", now), None);
        assert_eq!(parse("-0", now), Some(Duration::ZERO));
        assert_eq!(parse("+0", now), Some(Duration::ZERO));
        assert_eq!(parse("9223372036854775808", now), None);
        assert_eq!(parse("9223372037", now), Some(Duration::ZERO));
        assert_eq!(
            parse("18446744074", now),
            Some(Duration::from_nanos(290_448_384))
        );
        assert_eq!(parse("9223372036854775807", now), Some(Duration::ZERO));
    }

    #[test]
    fn rfc1123_past_future_and_legacy_forms() {
        let now = UNIX_EPOCH + Duration::from_secs(784_111_776);
        assert_eq!(
            parse("Sun, 06 Nov 1994 08:49:37 GMT", now),
            Some(Duration::from_secs(1)),
        );
        assert_eq!(
            parse("Mon, 06 nov 1994 8:49:37.125 UTC", now),
            Some(Duration::from_millis(1125)),
        );
        assert_eq!(
            parse("Sun, 06 Nov 1994 10:49:37 GMT+2", now),
            Some(Duration::from_secs(1)),
        );
        assert_eq!(
            parse("Sun, 06 Nov 1960 08:49:37 GMT", now),
            Some(Duration::ZERO)
        );
        assert_eq!(parse("Sunday, 06-Nov-94 08:49:37 GMT", now), None);
        assert_eq!(parse("Sun Nov  6 08:49:37 1994", now), None);
        assert_eq!(parse("Sun, 31 Feb 1994 08:49:37 GMT", now), None);
    }
}
