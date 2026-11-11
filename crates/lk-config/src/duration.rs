//! Go `time.Duration`: signed nanoseconds, parsed with `time.ParseDuration`
//! and printed with `Duration.String()`.
//!
//! YAML, generated flags and their env vars all go through [`parse_go_duration`].
//! A bare YAML integer is rejected (yaml.v3 refuses `!!int` for a Duration,
//! impl-notes §2.0), so `500` must be written `500ms`.

use std::fmt;
use std::time::Duration;

use serde::de::{self, Deserialize, Deserializer, Visitor};

use crate::strconv::go_quote;

/// Go `time.Duration` (nanoseconds, may be negative).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct GoDuration(pub i64);

const NS: u64 = 1;
const US: u64 = 1_000;
const MS: u64 = 1_000_000;
const SEC: u64 = 1_000_000_000;
const MIN: u64 = 60 * SEC;
const HOUR: u64 = 60 * MIN;

impl GoDuration {
    pub const ZERO: GoDuration = GoDuration(0);

    pub const fn from_millis(ms: i64) -> Self {
        GoDuration(ms * MS as i64)
    }

    pub const fn from_secs(s: i64) -> Self {
        GoDuration(s * SEC as i64)
    }

    pub const fn nanos(self) -> i64 {
        self.0
    }

    /// Converts to `std::time::Duration`; negative values become zero.
    pub fn to_std(self) -> Duration {
        if self.0 <= 0 {
            Duration::ZERO
        } else {
            Duration::from_nanos(self.0 as u64)
        }
    }
}

impl fmt::Display for GoDuration {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&format_go_duration(self.0))
    }
}

fn unit_value(u: &str) -> Option<u64> {
    match u {
        "ns" => Some(NS),
        "us" | "\u{00b5}s" | "\u{03bc}s" => Some(US),
        "ms" => Some(MS),
        "s" => Some(SEC),
        "m" => Some(MIN),
        "h" => Some(HOUR),
        _ => None,
    }
}

/// Go `time.ParseDuration`, including its error texts:
/// `time: invalid duration "x"`, `time: missing unit in duration "5"`,
/// `time: unknown unit "d" in duration "5d"`.
pub fn parse_go_duration(orig: &str) -> Result<GoDuration, String> {
    let invalid = || format!("time: invalid duration {}", go_quote(orig));
    let mut s = orig;
    let mut neg = false;
    if let Some(c) = s.as_bytes().first()
        && (*c == b'-' || *c == b'+')
    {
        neg = *c == b'-';
        s = &s[1..];
    }
    if s == "0" {
        return Ok(GoDuration(0));
    }
    if s.is_empty() {
        return Err(invalid());
    }
    const LIMIT: u64 = 1u64 << 63;
    let mut d: u64 = 0;
    while !s.is_empty() {
        let b = s.as_bytes();
        if !(b[0] == b'.' || b[0].is_ascii_digit()) {
            return Err(invalid());
        }
        // leading int
        let mut i = 0;
        let mut v: u64 = 0;
        while i < b.len() && b[i].is_ascii_digit() {
            if v > LIMIT / 10 {
                return Err(invalid());
            }
            v = v * 10 + (b[i] - b'0') as u64;
            if v > LIMIT {
                return Err(invalid());
            }
            i += 1;
        }
        let pre = i > 0;
        s = &s[i..];
        // fraction
        let mut f: u64 = 0;
        let mut scale: f64 = 1.0;
        let mut post = false;
        if s.as_bytes().first() == Some(&b'.') {
            s = &s[1..];
            let b = s.as_bytes();
            let mut j = 0;
            let mut overflow = false;
            while j < b.len() && b[j].is_ascii_digit() {
                if !overflow {
                    if f > (LIMIT - 1) / 10 {
                        overflow = true;
                    } else {
                        let y = f * 10 + (b[j] - b'0') as u64;
                        if y > LIMIT {
                            overflow = true;
                        } else {
                            f = y;
                            scale *= 10.0;
                        }
                    }
                }
                j += 1;
            }
            post = j > 0;
            s = &s[j..];
        }
        if !pre && !post {
            return Err(invalid());
        }
        // unit
        let b = s.as_bytes();
        let mut k = 0;
        while k < b.len() && !(b[k] == b'.' || b[k].is_ascii_digit()) {
            k += 1;
        }
        if k == 0 {
            return Err(format!("time: missing unit in duration {}", go_quote(orig)));
        }
        let u = &s[..k];
        s = &s[k..];
        let unit = match unit_value(u) {
            Some(u) => u,
            None => {
                return Err(format!(
                    "time: unknown unit {} in duration {}",
                    go_quote(u),
                    go_quote(orig)
                ));
            }
        };
        if v > LIMIT / unit {
            return Err(invalid());
        }
        v *= unit;
        if f > 0 {
            v += (f as f64 * (unit as f64 / scale)) as u64;
            if v > LIMIT {
                return Err(invalid());
            }
        }
        d += v;
        if d > LIMIT {
            return Err(invalid());
        }
    }
    if neg {
        return Ok(GoDuration((d as i64).wrapping_neg()));
    }
    if d > LIMIT - 1 {
        return Err(invalid());
    }
    Ok(GoDuration(d as i64))
}

/// Go `time.Duration.String()`: `0s`, `1.5µs`, `500ms`, `7.5s`, `1m0s`, `2h0m0s`.
pub fn format_go_duration(d: i64) -> String {
    // Build right-to-left like Go, then reverse.
    let mut buf: Vec<char> = Vec::with_capacity(32);
    let neg = d < 0;
    let mut u: u64 = if neg {
        (d as u64).wrapping_neg()
    } else {
        d as u64
    };

    fn fmt_frac(buf: &mut Vec<char>, mut v: u64, prec: usize) -> u64 {
        let mut print = false;
        for _ in 0..prec {
            let digit = v % 10;
            print = print || digit != 0;
            if print {
                buf.push((b'0' + digit as u8) as char);
            }
            v /= 10;
        }
        if print {
            buf.push('.');
        }
        v
    }
    fn fmt_int(buf: &mut Vec<char>, mut v: u64) {
        if v == 0 {
            buf.push('0');
            return;
        }
        while v > 0 {
            buf.push((b'0' + (v % 10) as u8) as char);
            v /= 10;
        }
    }

    if u < SEC {
        buf.push('s');
        let prec;
        if u == 0 {
            return "0s".to_string();
        } else if u < US {
            prec = 0;
            buf.push('n');
        } else if u < MS {
            prec = 3;
            buf.push('\u{00b5}');
        } else {
            prec = 6;
            buf.push('m');
        }
        u = fmt_frac(&mut buf, u, prec);
        fmt_int(&mut buf, u);
    } else {
        buf.push('s');
        u = fmt_frac(&mut buf, u, 9);
        fmt_int(&mut buf, u % 60);
        u /= 60;
        if u > 0 {
            buf.push('m');
            fmt_int(&mut buf, u % 60);
            u /= 60;
            if u > 0 {
                buf.push('h');
                fmt_int(&mut buf, u);
            }
        }
    }
    if neg {
        buf.push('-');
    }
    buf.iter().rev().collect()
}

impl<'de> Deserialize<'de> for GoDuration {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        struct V;
        impl<'de> Visitor<'de> for V {
            type Value = GoDuration;
            fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str("a Go duration string such as \"500ms\" or \"1m30s\"")
            }
            fn visit_str<E: de::Error>(self, v: &str) -> Result<GoDuration, E> {
                parse_go_duration(v).map_err(E::custom)
            }
            fn visit_i64<E: de::Error>(self, v: i64) -> Result<GoDuration, E> {
                Err(E::custom(format!(
                    "cannot unmarshal !!int `{v}` into time.Duration"
                )))
            }
            fn visit_u64<E: de::Error>(self, v: u64) -> Result<GoDuration, E> {
                Err(E::custom(format!(
                    "cannot unmarshal !!int `{v}` into time.Duration"
                )))
            }
            fn visit_f64<E: de::Error>(self, v: f64) -> Result<GoDuration, E> {
                Err(E::custom(format!(
                    "cannot unmarshal !!float `{v}` into time.Duration"
                )))
            }
            fn visit_bool<E: de::Error>(self, v: bool) -> Result<GoDuration, E> {
                Err(E::custom(format!(
                    "cannot unmarshal !!bool `{v}` into time.Duration"
                )))
            }
        }
        d.deserialize_any(V)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_ok() {
        assert_eq!(
            parse_go_duration("500ms").unwrap(),
            GoDuration::from_millis(500)
        );
        assert_eq!(
            parse_go_duration("1m30s").unwrap(),
            GoDuration::from_secs(90)
        );
        assert_eq!(
            parse_go_duration("7.5s").unwrap(),
            GoDuration::from_millis(7500)
        );
        assert_eq!(parse_go_duration("0").unwrap(), GoDuration(0));
        assert_eq!(
            parse_go_duration("-1.5h").unwrap(),
            GoDuration(-5_400 * SEC as i64)
        );
        assert_eq!(parse_go_duration("1\u{00b5}s").unwrap(), GoDuration(1000));
    }

    #[test]
    fn parse_errors() {
        assert_eq!(
            parse_go_duration("5").unwrap_err(),
            "time: missing unit in duration \"5\""
        );
        assert_eq!(
            parse_go_duration("5d").unwrap_err(),
            "time: unknown unit \"d\" in duration \"5d\""
        );
        assert_eq!(
            parse_go_duration("").unwrap_err(),
            "time: invalid duration \"\""
        );
        assert_eq!(
            parse_go_duration(".s").unwrap_err(),
            "time: invalid duration \".s\""
        );
    }

    #[test]
    fn format() {
        assert_eq!(format_go_duration(0), "0s");
        assert_eq!(format_go_duration(5 * SEC as i64), "5s");
        assert_eq!(format_go_duration(60 * SEC as i64), "1m0s");
        assert_eq!(format_go_duration(120 * SEC as i64), "2m0s");
        assert_eq!(format_go_duration(300 * SEC as i64), "5m0s");
        assert_eq!(format_go_duration(7_500 * MS as i64), "7.5s");
        assert_eq!(format_go_duration(100 * MS as i64), "100ms");
        assert_eq!(format_go_duration(1_500), "1.5\u{00b5}s");
        assert_eq!(format_go_duration(3_600 * SEC as i64), "1h0m0s");
        assert_eq!(format_go_duration(-(500 * MS as i64)), "-500ms");
    }
}
