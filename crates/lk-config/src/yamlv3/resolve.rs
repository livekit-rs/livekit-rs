//! yaml.v3 `resolve.go`: plain-scalar tag resolution and the timestamp check.
//!
//! Ported from gopkg.in/yaml.v3 v3.0.1 (MIT / Apache-2.0, Copyright (c)
//! 2011-2019 Canonical Ltd). Number lexing uses Go `strconv` ports.

use super::Fail;

pub(crate) const NULL_TAG: &str = "!!null";
pub(crate) const BOOL_TAG: &str = "!!bool";
pub(crate) const STR_TAG: &str = "!!str";
pub(crate) const INT_TAG: &str = "!!int";
pub(crate) const FLOAT_TAG: &str = "!!float";
pub(crate) const TIMESTAMP_TAG: &str = "!!timestamp";
pub(crate) const SEQ_TAG: &str = "!!seq";
pub(crate) const MAP_TAG: &str = "!!map";
pub(crate) const BINARY_TAG: &str = "!!binary";
pub(crate) const MERGE_TAG: &str = "!!merge";

const LONG_TAG_PREFIX: &str = "tag:yaml.org,2002:";

/// A resolved scalar (`resolve`'s `interface{}` result). On 64-bit Go every
/// in-range `int64` resolves to `int`, so one signed variant suffices.
#[derive(Clone, Debug, PartialEq)]
pub(crate) enum Resolved {
    Null,
    Bool(bool),
    Int(i64),
    Uint(u64),
    Float(f64),
    Str(String),
    /// `time.Time`; only its type matters to the decoder.
    Timestamp,
}

/// `shortTag`.
pub(crate) fn short_tag(tag: &str) -> String {
    match tag.strip_prefix(LONG_TAG_PREFIX) {
        Some(rest) => format!("!!{rest}"),
        None => tag.to_string(),
    }
}

fn resolvable_tag(tag: &str) -> bool {
    matches!(
        tag,
        "" | STR_TAG | BOOL_TAG | INT_TAG | FLOAT_TAG | NULL_TAG | TIMESTAMP_TAG
    )
}

fn resolve_map(s: &str) -> Option<(&'static str, Resolved)> {
    Some(match s {
        "true" | "True" | "TRUE" => (BOOL_TAG, Resolved::Bool(true)),
        "false" | "False" | "FALSE" => (BOOL_TAG, Resolved::Bool(false)),
        "" | "~" | "null" | "Null" | "NULL" => (NULL_TAG, Resolved::Null),
        ".nan" | ".NaN" | ".NAN" => (FLOAT_TAG, Resolved::Float(go_nan())),
        ".inf" | ".Inf" | ".INF" | "+.inf" | "+.Inf" | "+.INF" => {
            (FLOAT_TAG, Resolved::Float(f64::INFINITY))
        }
        "-.inf" | "-.Inf" | "-.INF" => (FLOAT_TAG, Resolved::Float(f64::NEG_INFINITY)),
        "<<" => (MERGE_TAG, Resolved::Str("<<".into())),
        _ => return None,
    })
}

/// Go `math.NaN()`.
fn go_nan() -> f64 {
    f64::from_bits(0x7ff8_0000_0000_0001)
}

fn hint(s: &str) -> u8 {
    match s.as_bytes().first() {
        None => b'N',
        Some(b'+' | b'-') => b'S',
        Some(b'0'..=b'9') => b'D',
        Some(b'y' | b'Y' | b'n' | b'N' | b't' | b'T' | b'f' | b'F' | b'o' | b'O' | b'~') => b'M',
        Some(b'.') => b'.',
        Some(_) => 0,
    }
}

fn parse_float(s: &str) -> Option<f64> {
    crate::protoyaml::float::parse(s, false).ok()
}

/// `^[-+]?(\.[0-9]+|[0-9]+(\.[0-9]*)?)([eE][-+]?[0-9]+)?$`
fn yaml_style_float(s: &str) -> bool {
    let b = s.as_bytes();
    let mut i = 0;
    if matches!(b.first(), Some(b'+' | b'-')) {
        i += 1;
    }
    let digits = |i: &mut usize| {
        let start = *i;
        while *i < b.len() && b[*i].is_ascii_digit() {
            *i += 1;
        }
        *i - start
    };
    if b.get(i) == Some(&b'.') {
        i += 1;
        if digits(&mut i) == 0 {
            return false;
        }
    } else {
        if digits(&mut i) == 0 {
            return false;
        }
        if b.get(i) == Some(&b'.') {
            i += 1;
            digits(&mut i);
        }
    }
    if matches!(b.get(i), Some(b'e' | b'E')) {
        i += 1;
        if matches!(b.get(i), Some(b'+' | b'-')) {
            i += 1;
        }
        if digits(&mut i) == 0 {
            return false;
        }
    }
    i == b.len()
}

fn int_result(v: Result<i64, String>) -> Option<Resolved> {
    v.ok().map(Resolved::Int)
}

fn parse_radix_signed(digits: &str, radix: u32, negative: bool) -> Option<i64> {
    // strconv.ParseInt(s, radix, 64) on an already prefix-stripped literal.
    if digits.is_empty() || digits.starts_with(['+', '-']) {
        return None;
    }
    let magnitude = u64::from_str_radix(digits, radix).ok()?;
    if negative {
        if magnitude > 1u64 << 63 {
            None
        } else {
            Some((magnitude as i64).wrapping_neg())
        }
    } else if magnitude >= 1u64 << 63 {
        None
    } else {
        Some(magnitude as i64)
    }
}

fn parse_radix_unsigned(digits: &str, radix: u32) -> Option<u64> {
    if digits.is_empty() || digits.starts_with(['+', '-']) {
        return None;
    }
    u64::from_str_radix(digits, radix).ok()
}

/// `resolve(tag, in)`: the resolved short tag and value. An explicit
/// resolvable tag that disagrees with the lexeme fails like upstream.
pub(crate) fn resolve(tag: &str, input: &str) -> Result<(String, Resolved), Fail> {
    let tag = short_tag(tag);
    if !resolvable_tag(&tag) {
        return Ok((tag, Resolved::Str(input.to_string())));
    }
    let (rtag, out) = resolve_plain(&tag, input);
    // The deferred check of resolve.go.
    if tag.is_empty() || tag == rtag || tag == STR_TAG || tag == BINARY_TAG {
        return Ok((rtag.to_string(), out));
    }
    if tag == FLOAT_TAG
        && rtag == INT_TAG
        && let Resolved::Int(v) = out
    {
        return Ok((FLOAT_TAG.to_string(), Resolved::Float(v as f64)));
    }
    Err(Fail(format!(
        "yaml: cannot decode {} `{input}` as a {}",
        short_tag(rtag),
        short_tag(&tag)
    )))
}

fn resolve_plain(tag: &str, input: &str) -> (&'static str, Resolved) {
    let hint = hint(input);
    if hint != 0 && tag != STR_TAG && tag != BINARY_TAG {
        if let Some(item) = resolve_map(input) {
            return item;
        }
        match hint {
            b'M' => {}
            b'.' => {
                if let Some(f) = parse_float(input) {
                    return (FLOAT_TAG, Resolved::Float(f));
                }
            }
            b'D' | b'S' => {
                if (tag.is_empty() || tag == TIMESTAMP_TAG) && is_timestamp(input) {
                    return (TIMESTAMP_TAG, Resolved::Timestamp);
                }
                let plain = input.replace('_', "");
                if let Some(v) = int_result(crate::strconv::parse_int(&plain)) {
                    return (INT_TAG, v);
                }
                if let Ok(v) = crate::strconv::parse_uint(&plain) {
                    return (INT_TAG, Resolved::Uint(v));
                }
                if yaml_style_float(&plain)
                    && let Some(f) = parse_float(&plain)
                {
                    return (FLOAT_TAG, Resolved::Float(f));
                }
                if let Some(rest) = plain.strip_prefix("0b") {
                    if let Some(v) = parse_radix_signed(rest, 2, false) {
                        return (INT_TAG, Resolved::Int(v));
                    }
                    if let Some(v) = parse_radix_unsigned(rest, 2) {
                        return (INT_TAG, Resolved::Uint(v));
                    }
                } else if let Some(rest) = plain.strip_prefix("-0b")
                    && let Some(v) = parse_radix_signed(rest, 2, true)
                {
                    return (INT_TAG, Resolved::Int(v));
                }
                if let Some(rest) = plain.strip_prefix("0o") {
                    if let Some(v) = parse_radix_signed(rest, 8, false) {
                        return (INT_TAG, Resolved::Int(v));
                    }
                    if let Some(v) = parse_radix_unsigned(rest, 8) {
                        return (INT_TAG, Resolved::Uint(v));
                    }
                } else if let Some(rest) = plain.strip_prefix("-0o")
                    && let Some(v) = parse_radix_signed(rest, 8, true)
                {
                    return (INT_TAG, Resolved::Int(v));
                }
            }
            _ => unreachable!("resolve table hint"),
        }
    }
    (STR_TAG, Resolved::Str(input.to_string()))
}

// ---------------------------------------------------------------------------
// parseTimestamp: Go time.Parse over the four allowed layouts.
// ---------------------------------------------------------------------------

/// `parseTimestamp(s)` succeeds.
pub(crate) fn is_timestamp(s: &str) -> bool {
    let b = s.as_bytes();
    let i = b.iter().take_while(|c| c.is_ascii_digit()).count();
    if i != 4 || i == b.len() || b[i] != b'-' {
        return false;
    }
    [
        Layout::DateTimeZone(b'T'),
        Layout::DateTimeZone(b't'),
        Layout::DateTimeSpace,
        Layout::Date,
    ]
    .into_iter()
    .any(|layout| parse_layout(b, layout))
}

#[derive(Clone, Copy)]
enum Layout {
    /// `2006-1-2T15:4:5.999999999Z07:00` (and the lower-case `t` form).
    DateTimeZone(u8),
    /// `2006-1-2 15:4:5.999999999`.
    DateTimeSpace,
    /// `2006-1-2`.
    Date,
}

/// time `getnum(s, false)`: one or two digits.
fn getnum(v: &[u8]) -> Option<(u32, &[u8])> {
    let d0 = *v.first().filter(|c| c.is_ascii_digit())?;
    match v.get(1).filter(|c| c.is_ascii_digit()) {
        Some(d1) => Some((u32::from(d0 - b'0') * 10 + u32::from(d1 - b'0'), &v[2..])),
        None => Some((u32::from(d0 - b'0'), &v[1..])),
    }
}

/// time `getnum(s, true)`: exactly two digits.
fn getnum_fixed(v: &[u8]) -> Option<u32> {
    if v.len() == 2 && v.iter().all(u8::is_ascii_digit) {
        Some(u32::from(v[0] - b'0') * 10 + u32::from(v[1] - b'0'))
    } else {
        None
    }
}

fn literal(v: &[u8], c: u8) -> Option<&[u8]> {
    (v.first() == Some(&c)).then(|| &v[1..])
}

/// time `skip` for a single layout space: one or more value spaces.
fn space(v: &[u8]) -> Option<&[u8]> {
    if v.first().is_some_and(|c| *c != b' ') {
        return None;
    }
    let n = v.iter().take_while(|c| **c == b' ').count();
    Some(&v[n..])
}

fn days_in(month: u32, year: i64) -> u32 {
    match month {
        2 if (year % 4 == 0 && year % 100 != 0) || year % 400 == 0 => 29,
        2 => 28,
        4 | 6 | 9 | 11 => 30,
        _ => 31,
    }
}

fn parse_layout(value: &[u8], layout: Layout) -> bool {
    parse_layout_inner(value, layout).is_some()
}

fn parse_layout_inner(value: &[u8], layout: Layout) -> Option<()> {
    // 2006
    if value.len() < 4 || !value[0].is_ascii_digit() {
        return None;
    }
    let year_text = std::str::from_utf8(&value[..4]).ok()?;
    let year: i64 = crate::strconv::atoi(year_text).ok()?;
    let v = &value[4..];
    let v = literal(v, b'-')?;
    let (month, v) = getnum(v)?;
    if !(1..=12).contains(&month) {
        return None;
    }
    let v = literal(v, b'-')?;
    let (day, v) = getnum(v)?;
    if day > 31 {
        return None;
    }
    let v = match layout {
        Layout::Date => v,
        Layout::DateTimeZone(t) => time_part(literal(v, t)?, true)?,
        Layout::DateTimeSpace => time_part(space(v)?, false)?,
    };
    if !v.is_empty() {
        return None;
    }
    if day < 1 || day > days_in(month, year) {
        return None;
    }
    Some(())
}

/// `15:4:5.999999999` then, when `zone`, `Z07:00`. Returns the rest.
fn time_part(v: &[u8], zone: bool) -> Option<&[u8]> {
    let (hour, v) = getnum(v)?;
    if hour >= 24 {
        return None;
    }
    let v = literal(v, b':')?;
    let (minute, v) = getnum(v)?;
    if minute >= 60 {
        return None;
    }
    let v = literal(v, b':')?;
    let (second, mut v) = getnum(v)?;
    if second >= 60 {
        return None;
    }
    // stdFracSecond9: optional `.`/`,` followed by digits.
    if v.len() >= 2 && matches!(v[0], b'.' | b',') && v[1].is_ascii_digit() {
        let n = 1 + v[1..].iter().take_while(|c| c.is_ascii_digit()).count();
        v = &v[n..];
    }
    if !zone {
        return Some(v);
    }
    // stdISO8601ColonTZ
    if let Some(rest) = literal(v, b'Z') {
        return Some(rest);
    }
    if v.len() < 6 || v[3] != b':' || !matches!(v[0], b'+' | b'-') {
        return None;
    }
    let hh = getnum_fixed(&v[1..3])?;
    let mm = getnum_fixed(&v[4..6])?;
    if hh > 24 || mm > 60 {
        return None;
    }
    Some(&v[6..])
}

#[cfg(test)]
mod tests {
    use super::*;

    fn r(s: &str) -> (String, Resolved) {
        resolve("", s).unwrap()
    }

    #[test]
    fn plain_scalars_resolve_like_yaml_v3() {
        assert_eq!(r(""), ("!!null".into(), Resolved::Null));
        assert_eq!(r("~"), ("!!null".into(), Resolved::Null));
        assert_eq!(r("True"), ("!!bool".into(), Resolved::Bool(true)));
        assert_eq!(r("yes").0, "!!str");
        assert_eq!(r("0x1F"), ("!!int".into(), Resolved::Int(31)));
        assert_eq!(r("1_000"), ("!!int".into(), Resolved::Int(1000)));
        assert_eq!(r("0o17"), ("!!int".into(), Resolved::Int(15)));
        assert_eq!(r("017"), ("!!int".into(), Resolved::Int(15)));
        assert_eq!(r("-0b101"), ("!!int".into(), Resolved::Int(-5)));
        assert_eq!(
            r("18446744073709551615"),
            ("!!int".into(), Resolved::Uint(u64::MAX))
        );
        assert_eq!(r("99999999999999999999").0, "!!float");
        assert_eq!(r("1.5"), ("!!float".into(), Resolved::Float(1.5)));
        assert_eq!(r(".5"), ("!!float".into(), Resolved::Float(0.5)));
        assert_eq!(r("1e3"), ("!!float".into(), Resolved::Float(1000.0)));
        assert_eq!(
            r("-.inf"),
            ("!!float".into(), Resolved::Float(f64::NEG_INFINITY))
        );
        assert_eq!(r("2001-12-14").0, "!!timestamp");
        assert_eq!(r("2001-12-14t21:59:43.10-05:00").0, "!!timestamp");
        assert_eq!(r("2001-12-14 21:59:43.10").0, "!!timestamp");
        assert_eq!(r("2001-02-30").0, "!!str");
        assert_eq!(r("2001-1-2").0, "!!timestamp");
        assert_eq!(r("5x").0, "!!str");
        // `<` has no resolve hint: the merge tag comes from the parser.
        assert_eq!(r("<<").0, "!!str");
    }

    #[test]
    fn explicit_tags_check_like_yaml_v3() {
        assert_eq!(
            resolve("!!float", "1").unwrap(),
            ("!!float".into(), Resolved::Float(1.0))
        );
        assert_eq!(
            resolve("!!int", "abc").unwrap_err().0,
            "yaml: cannot decode !!str `abc` as a !!int"
        );
        assert_eq!(
            resolve("!!str", "1").unwrap(),
            ("!!str".into(), Resolved::Str("1".into()))
        );
        assert_eq!(
            resolve("!foo", "1").unwrap(),
            ("!foo".into(), Resolved::Str("1".into()))
        );
    }
}
