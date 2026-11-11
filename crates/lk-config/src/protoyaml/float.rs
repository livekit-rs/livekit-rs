//! Float lexical/hex conversion boundary from fixed Go1.26.3 internal/strconv.
//! Copyright The Go Authors. BSD-3-Clause; retained upstream source/LICENSE.
//! Decimal conversion uses Rust's correctly rounded parser only AFTER the Go
//! lexer has validated the entire token; this does not equate the two grammars.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Error {
    Syntax,
    Range,
}
impl Error {
    pub fn message(self) -> &'static str {
        match self {
            Self::Syntax => "invalid syntax",
            Self::Range => "value out of range",
        }
    }
}

pub(crate) fn parse(value: &str, single: bool) -> Result<f64, Error> {
    if value.eq_ignore_ascii_case("nan") {
        return Ok(if single {
            f64::from(f32::NAN)
        } else {
            f64::from_bits(0x7ff8_0000_0000_0001)
        });
    }
    let (negative, unsigned) = value.strip_prefix('-').map_or_else(
        || (false, value.strip_prefix('+').unwrap_or(value)),
        |s| (true, s),
    );
    if unsigned.eq_ignore_ascii_case("inf") || unsigned.eq_ignore_ascii_case("infinity") {
        return Ok(if negative {
            f64::NEG_INFINITY
        } else {
            f64::INFINITY
        });
    }
    let parsed = lex(value)?;
    if parsed.hex {
        return hexadecimal(parsed, single);
    }
    let clean = value.replace('_', "");
    let result = if single {
        clean.parse::<f32>().map(f64::from)
    } else {
        clean.parse::<f64>()
    }
    .map_err(|_| Error::Syntax)?;
    if result.is_infinite() {
        Err(Error::Range)
    } else {
        Ok(result)
    }
}
struct Parsed {
    mantissa: u64,
    exponent: i32,
    negative: bool,
    truncated: bool,
    hex: bool,
}
fn lex(value: &str) -> Result<Parsed, Error> {
    let s = value.as_bytes();
    let mut i = 0;
    let mut negative = false;
    if s.first() == Some(&b'+') {
        i += 1;
    } else if s.first() == Some(&b'-') {
        negative = true;
        i += 1;
    }
    let hex = i + 2 < s.len() && s[i] == b'0' && s[i + 1].eq_ignore_ascii_case(&b'x');
    let (base, max_digits, exponent_char) = if hex {
        i += 2;
        (16u64, 16, b'p')
    } else {
        (10, 19, b'e')
    };
    let mut dot = false;
    let mut digits = false;
    let mut count = 0i32;
    let mut kept = 0i32;
    let mut point = 0i32;
    let mut mantissa = 0u64;
    let mut truncated = false;
    while i < s.len() {
        let c = s[i];
        if c == b'_' {
            i += 1;
            continue;
        }
        if c == b'.' {
            if dot {
                break;
            }
            dot = true;
            point = count;
            i += 1;
            continue;
        }
        let digit = if c.is_ascii_digit() {
            Some(c - b'0')
        } else if hex && (b'a'..=b'f').contains(&c.to_ascii_lowercase()) {
            Some(c.to_ascii_lowercase() - b'a' + 10)
        } else {
            None
        };
        let Some(digit) = digit else {
            break;
        };
        digits = true;
        if digit == 0 && count == 0 {
            point -= 1;
            i += 1;
            continue;
        }
        count += 1;
        if kept < max_digits {
            mantissa = mantissa.wrapping_mul(base).wrapping_add(u64::from(digit));
            kept += 1;
        } else if digit != 0 {
            truncated = true;
        }
        i += 1;
    }
    if !digits {
        return Err(Error::Syntax);
    }
    if !dot {
        point = count;
    }
    if hex {
        point *= 4;
        kept *= 4;
    }
    if i < s.len() && s[i].eq_ignore_ascii_case(&exponent_char) {
        i += 1;
        let mut sign = 1;
        if s.get(i) == Some(&b'+') {
            i += 1;
        } else if s.get(i) == Some(&b'-') {
            sign = -1;
            i += 1;
        }
        if !s.get(i).is_some_and(u8::is_ascii_digit) {
            return Err(Error::Syntax);
        }
        let mut exponent = 0;
        while i < s.len() && (s[i].is_ascii_digit() || s[i] == b'_') {
            if s[i] != b'_' && exponent < 10000 {
                exponent = exponent * 10 + i32::from(s[i] - b'0');
            }
            i += 1;
        }
        point += sign * exponent;
    } else if hex {
        return Err(Error::Syntax);
    }
    if i != s.len() || !underscore_ok(value) {
        return Err(Error::Syntax);
    }
    Ok(Parsed {
        mantissa,
        exponent: if mantissa != 0 { point - kept } else { 0 },
        negative,
        truncated,
        hex,
    })
}
fn underscore_ok(value: &str) -> bool {
    let value = value.strip_prefix(['+', '-']).unwrap_or(value);
    let s = value.as_bytes();
    let mut i = 0;
    let mut saw = b'^';
    let mut hex = false;
    if s.len() >= 2 && s[0] == b'0' && matches!(s[1].to_ascii_lowercase(), b'b' | b'o' | b'x') {
        i = 2;
        saw = b'0';
        hex = s[1].eq_ignore_ascii_case(&b'x');
    }
    for &c in &s[i..] {
        if c.is_ascii_digit() || (hex && (b'a'..=b'f').contains(&c.to_ascii_lowercase())) {
            saw = b'0';
            continue;
        }
        if c == b'_' {
            if saw != b'0' {
                return false;
            }
            saw = b'_';
            continue;
        }
        if saw == b'_' {
            return false;
        }
        saw = b'!';
    }
    saw != b'_'
}
fn hexadecimal(value: Parsed, single: bool) -> Result<f64, Error> {
    let (mantissa_bits, exponent_bits, bias) = if single {
        (23u32, 8u32, -127i32)
    } else {
        (52, 11, -1023)
    };
    let max_exponent = (1i32 << exponent_bits) + bias - 2;
    let min_exponent = bias + 1;
    let mut exponent = value.exponent + mantissa_bits as i32;
    let mut mantissa = value.mantissa;
    while mantissa != 0 && mantissa >> (mantissa_bits + 2) == 0 {
        mantissa <<= 1;
        exponent -= 1;
    }
    if value.truncated {
        mantissa |= 1;
    }
    while mantissa >> (1 + mantissa_bits + 2) != 0 {
        mantissa = (mantissa >> 1) | (mantissa & 1);
        exponent += 1;
    }
    while mantissa > 1 && exponent < min_exponent - 2 {
        mantissa = (mantissa >> 1) | (mantissa & 1);
        exponent += 1;
    }
    let mut round = mantissa & 3;
    mantissa >>= 2;
    round |= mantissa & 1;
    exponent += 2;
    if round == 3 {
        mantissa += 1;
        if mantissa == 1 << (1 + mantissa_bits) {
            mantissa >>= 1;
            exponent += 1;
        }
    }
    if mantissa >> mantissa_bits == 0 {
        exponent = bias;
    }
    if exponent > max_exponent {
        return Err(Error::Range);
    }
    let mut bits = mantissa & ((1u64 << mantissa_bits) - 1);
    bits |= u64::from(((exponent - bias) as u32) & ((1u32 << exponent_bits) - 1)) << mantissa_bits;
    if value.negative {
        bits |= 1u64 << (mantissa_bits + exponent_bits);
    }
    Ok(if single {
        f64::from(f32::from_bits(bits as u32))
    } else {
        f64::from_bits(bits)
    })
}
