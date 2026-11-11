//! Go `strconv` look-alikes. Flag and env values are parsed with Go's rules
//! (urfave/cli uses `strconv.ParseBool`, `ParseInt(s, 0, 64)`, `ParseUint(s, 0, 64)`,
//! `ParseFloat(s, 64)`; PortRange uses `Atoi`) and the error texts mirror Go's
//! `*strconv.NumError` rendering: `strconv.ParseInt: parsing "x": invalid syntax`.

/// Exact Go `strconv.Quote` for valid UTF-8 diagnostic values, sharing the
/// source-bound print tables used by CLI `%q` wrappers.
pub(crate) fn go_quote(s: &str) -> String {
    crate::cli::quote::quote(s)
}

// Trim valid Go White_Space byte sequences without replacing invalid bytes.
pub(crate) fn trim_space_bytes(mut value: &[u8]) -> &[u8] {
    const MULTI: &[&[u8]] = &[
        b"\xc2\x85",
        b"\xc2\xa0",
        b"\xe1\x9a\x80",
        b"\xe2\x80\x80",
        b"\xe2\x80\x81",
        b"\xe2\x80\x82",
        b"\xe2\x80\x83",
        b"\xe2\x80\x84",
        b"\xe2\x80\x85",
        b"\xe2\x80\x86",
        b"\xe2\x80\x87",
        b"\xe2\x80\x88",
        b"\xe2\x80\x89",
        b"\xe2\x80\x8a",
        b"\xe2\x80\xa8",
        b"\xe2\x80\xa9",
        b"\xe2\x80\xaf",
        b"\xe2\x81\x9f",
        b"\xe3\x80\x80",
    ];
    loop {
        if value
            .first()
            .is_some_and(|b| matches!(*b, b'\t'..=b'\r' | b' '))
        {
            value = &value[1..];
            continue;
        }
        if let Some(prefix) = MULTI.iter().find(|w| value.starts_with(w)) {
            value = &value[prefix.len()..];
            continue;
        }
        break;
    }
    loop {
        if value
            .last()
            .is_some_and(|b| matches!(*b, b'\t'..=b'\r' | b' '))
        {
            value = &value[..value.len() - 1];
            continue;
        }
        if let Some(suffix) = MULTI.iter().find(|w| value.ends_with(w)) {
            value = &value[..value.len() - suffix.len()];
            continue;
        }
        break;
    }
    value
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum NumErr {
    Syntax,
    Range,
}

fn num_error(func: &str, input: &str, e: NumErr) -> String {
    let what = match e {
        NumErr::Syntax => "invalid syntax",
        NumErr::Range => "value out of range",
    };
    format!("strconv.{func}: parsing {}: {what}", go_quote(input))
}

/// `strconv.ParseBool`.
pub(crate) fn parse_bool(s: &str) -> Result<bool, String> {
    match s {
        "1" | "t" | "T" | "TRUE" | "true" | "True" => Ok(true),
        "0" | "f" | "F" | "FALSE" | "false" | "False" => Ok(false),
        _ => Err(num_error("ParseBool", s, NumErr::Syntax)),
    }
}

/// Go's `underscoreOK`, simplified: underscores only between digits (or right
/// after a base prefix).
fn underscore_ok(s: &str) -> bool {
    let t = s.trim_start_matches(['+', '-']);
    !t.starts_with('_') && !t.ends_with('_') && !t.contains("__")
}

/// Core of `strconv.ParseUint(s, 0, 64)` without the function name.
fn parse_uint_base0(s: &str) -> Result<u64, NumErr> {
    if s.is_empty() {
        return Err(NumErr::Syntax);
    }
    let bytes = s.as_bytes();
    let (base, digits): (u64, &[u8]) = if bytes[0] == b'0' {
        if bytes.len() >= 3 && (bytes[1] | 0x20) == b'b' {
            (2, &bytes[2..])
        } else if bytes.len() >= 3 && (bytes[1] | 0x20) == b'o' {
            (8, &bytes[2..])
        } else if bytes.len() >= 3 && (bytes[1] | 0x20) == b'x' {
            (16, &bytes[2..])
        } else {
            (8, &bytes[1..])
        }
    } else {
        (10, bytes)
    };
    let mut underscores = false;
    let mut n: u64 = 0;
    let mut overflow = false;
    for &c in digits {
        let d = match c {
            b'_' => {
                underscores = true;
                continue;
            }
            b'0'..=b'9' => (c - b'0') as u64,
            b'a'..=b'z' => (c - b'a' + 10) as u64,
            b'A'..=b'Z' => (c - b'A' + 10) as u64,
            _ => return Err(NumErr::Syntax),
        };
        if d >= base {
            return Err(NumErr::Syntax);
        }
        if overflow {
            continue;
        }
        match n.checked_mul(base).and_then(|v| v.checked_add(d)) {
            Some(v) => n = v,
            None => overflow = true,
        }
    }
    if underscores && !underscore_ok(s) {
        return Err(NumErr::Syntax);
    }
    if overflow {
        return Err(NumErr::Range);
    }
    Ok(n)
}

/// `strconv.ParseUint(s, 0, 64)`.
pub(crate) fn parse_uint(s: &str) -> Result<u64, String> {
    parse_uint_base0(s).map_err(|e| num_error("ParseUint", s, e))
}

/// `strconv.ParseInt(s, 0, 64)`.
pub(crate) fn parse_int(s: &str) -> Result<i64, String> {
    if s.is_empty() {
        return Err(num_error("ParseInt", s, NumErr::Syntax));
    }
    let (neg, rest) = match s.as_bytes()[0] {
        b'+' => (false, &s[1..]),
        b'-' => (true, &s[1..]),
        _ => (false, s),
    };
    let un = parse_uint_base0(rest).map_err(|e| num_error("ParseInt", s, e))?;
    const CUTOFF: u64 = 1u64 << 63;
    if !neg && un >= CUTOFF {
        return Err(num_error("ParseInt", s, NumErr::Range));
    }
    if neg && un > CUTOFF {
        return Err(num_error("ParseInt", s, NumErr::Range));
    }
    Ok(if neg {
        (un as i64).wrapping_neg()
    } else {
        un as i64
    })
}

/// `strconv.ParseFloat(s, 64)` (Rust's grammar is close enough: it accepts
/// decimal, exponent, `inf`, `infinity`, `nan`; Go additionally accepts hex floats
/// and underscores, which are not supported here).
pub(crate) fn parse_float(s: &str) -> Result<f64, String> {
    s.parse::<f64>()
        .map_err(|_| num_error("ParseFloat", s, NumErr::Syntax))
}

/// `strconv.Atoi` (base 10, optional sign, no underscores).
pub(crate) fn atoi(s: &str) -> Result<i64, String> {
    let digits = s.strip_prefix(['+', '-']).unwrap_or(s);
    if digits.is_empty() || !digits.bytes().all(|b| b.is_ascii_digit()) {
        return Err(num_error("Atoi", s, NumErr::Syntax));
    }
    s.parse::<i64>()
        .map_err(|_| num_error("Atoi", s, NumErr::Range))
}

/// Go `strconv.FormatFloat(v, 'f', -1, 64)`: shortest round-trip, never exponent.
/// Rust's `Display` for `f64` has exactly these semantics.
pub(crate) fn format_float(v: f64) -> String {
    if v.is_infinite() {
        return if v > 0.0 {
            "+Inf".into()
        } else {
            "-Inf".into()
        };
    }
    if v.is_nan() {
        return "NaN".into();
    }
    format!("{v}")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bool_forms() {
        assert_eq!(parse_bool("T"), Ok(true));
        assert_eq!(parse_bool("False"), Ok(false));
        assert_eq!(
            parse_bool("yes").unwrap_err(),
            "strconv.ParseBool: parsing \"yes\": invalid syntax"
        );
    }

    #[test]
    fn int_bases() {
        assert_eq!(parse_int("0x10"), Ok(16));
        assert_eq!(parse_int("0o10"), Ok(8));
        assert_eq!(parse_int("010"), Ok(8));
        assert_eq!(parse_int("-1_000"), Ok(-1000));
        assert_eq!(parse_int("0"), Ok(0));
        assert!(
            parse_int("9223372036854775808")
                .unwrap_err()
                .contains("value out of range")
        );
        assert_eq!(parse_int("-9223372036854775808"), Ok(i64::MIN));
        assert!(parse_uint("-1").unwrap_err().contains("invalid syntax"));
        assert!(
            parse_int("abc")
                .unwrap_err()
                .starts_with("strconv.ParseInt: parsing \"abc\"")
        );
    }

    #[test]
    fn atoi_errors() {
        assert_eq!(
            atoi(" 1").unwrap_err(),
            "strconv.Atoi: parsing \" 1\": invalid syntax"
        );
        assert_eq!(atoi("+7"), Ok(7));
    }

    #[test]
    fn float_text() {
        assert_eq!(format_float(0.9f32 as f64), "0.8999999761581421");
        assert_eq!(format_float(1.5), "1.5");
        assert_eq!(format_float(-0.6), "-0.6");
        assert_eq!(format_float(0.0), "0");
    }
}
