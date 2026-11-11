// Copyright 2009 The Go Authors. BSD-3-Clause. Go1.26 netip/strconv port.
//! Byte-oriented because percent-unescaped Go host strings need not be UTF-8.
pub(super) struct UrlSupport;
pub(super) enum LiteralError {
    Ipv4,
    Parse(String),
}
impl UrlSupport {
    pub(super) fn quote(&self, bytes: &[u8]) -> String {
        let mut out = String::from("\"");
        let mut rest = bytes;
        while !rest.is_empty() {
            let (valid, skip) = match std::str::from_utf8(rest) {
                Ok(s) => (s, 0),
                Err(e) => (std::str::from_utf8(&rest[..e.valid_up_to()]).unwrap(), 1),
            };
            for c in valid.chars() {
                match c {
                    '"' => out.push_str("\\\""),
                    '\\' => out.push_str("\\\\"),
                    '\n' => out.push_str("\\n"),
                    '\r' => out.push_str("\\r"),
                    '\t' => out.push_str("\\t"),
                    '\u{7}' => out.push_str("\\a"),
                    '\u{8}' => out.push_str("\\b"),
                    '\u{b}' => out.push_str("\\v"),
                    '\u{c}' => out.push_str("\\f"),
                    c if (c as u32) < 32 || c == '\u{7f}' => {
                        out.push_str(&format!("\\x{:02x}", c as u32))
                    }
                    c if crate::sip::domain::is_go_print(c) => out.push(c),
                    c if (c as u32) < 0x10000 => out.push_str(&format!("\\u{:04x}", c as u32)),
                    c => out.push_str(&format!("\\U{:08x}", c as u32)),
                }
            }
            rest = &rest[valid.len()..];
            if skip == 1 {
                out.push_str(&format!("\\x{:02x}", rest[0]));
                rest = &rest[1..];
            }
        }
        out.push('"');
        out
    }
    pub(super) fn validate_ipv6_literal(&self, value: &[u8]) -> Result<(), LiteralError> {
        let ipv4 = parse(value).map_err(LiteralError::Parse)?;
        if ipv4 {
            Err(LiteralError::Ipv4)
        } else {
            Ok(())
        }
    }
}
fn error(input: &[u8], message: &str, at: &[u8]) -> String {
    let q = UrlSupport;
    let suffix = if at.is_empty() {
        String::new()
    } else {
        format!(" (at {})", q.quote(at))
    };
    format!("ParseAddr({}): {message}{suffix}", q.quote(input))
}
fn ipv4(input: &[u8], s: &[u8]) -> Result<(), String> {
    let (mut value, mut position, mut digits) = (0u32, 0, 0);
    for (i, &c) in s.iter().enumerate() {
        if c.is_ascii_digit() {
            if digits == 1 && value == 0 {
                return Err(error(input, "IPv4 field has octet with leading zero", b""));
            }
            value = value * 10 + u32::from(c - b'0');
            digits += 1;
            if value > 255 {
                return Err(error(input, "IPv4 field has value >255", b""));
            }
        } else if c == b'.' {
            if i == 0 || i == s.len() - 1 || s[i - 1] == b'.' {
                return Err(error(
                    input,
                    "IPv4 field must have at least one digit",
                    &s[i..],
                ));
            }
            if position == 3 {
                return Err(error(input, "IPv4 address too long", b""));
            }
            position += 1;
            value = 0;
            digits = 0;
        } else {
            return Err(error(input, "unexpected character", &s[i..]));
        }
    }
    if position < 3 {
        return Err(error(input, "IPv4 address too short", b""));
    }
    Ok(())
}
fn ipv6(input: &[u8]) -> Result<(), String> {
    let mut s = input;
    if let Some(z) = s.iter().position(|b| *b == b'%') {
        if z + 1 == s.len() {
            return Err(error(input, "zone must be a non-empty string", b""));
        }
        s = &s[..z];
    }
    let mut ellipsis = false;
    if s.starts_with(b"::") {
        ellipsis = true;
        s = &s[2..];
        if s.is_empty() {
            return Ok(());
        }
    }
    let mut i = 0;
    while i < 16 {
        let (mut off, mut value) = (0usize, 0u32);
        for &c in s {
            let digit = if c.is_ascii_digit() {
                c - b'0'
            } else if (b'a'..=b'f').contains(&c) {
                c - b'a' + 10
            } else if (b'A'..=b'F').contains(&c) {
                c - b'A' + 10
            } else {
                break;
            };
            value = (value << 4) + u32::from(digit);
            if off > 3 {
                return Err(error(input, "each group must have 4 or less digits", s));
            }
            if value > u32::from(u16::MAX) {
                return Err(error(input, "IPv6 field has value >=2^16", s));
            }
            off += 1;
        }
        if off == 0 {
            return Err(error(
                input,
                "each colon-separated field must have at least one digit",
                s,
            ));
        }
        if s.get(off) == Some(&b'.') {
            if !ellipsis && i != 12 {
                return Err(error(
                    input,
                    "embedded IPv4 address must replace the final 2 fields of the address",
                    s,
                ));
            }
            if i + 4 > 16 {
                return Err(error(
                    input,
                    "too many hex fields to fit an embedded IPv4 at the end of the address",
                    s,
                ));
            }
            ipv4(input, s)?;
            s = b"";
            i += 4;
            break;
        }
        i += 2;
        s = &s[off..];
        if s.is_empty() {
            break;
        }
        if s[0] != b':' {
            return Err(error(input, "unexpected character, want colon", s));
        }
        if s.len() == 1 {
            return Err(error(input, "colon must be followed by more characters", s));
        }
        s = &s[1..];
        if s.starts_with(b":") {
            if ellipsis {
                return Err(error(input, "multiple :: in address", s));
            }
            ellipsis = true;
            s = &s[1..];
            if s.is_empty() {
                break;
            }
        }
    }
    if !s.is_empty() {
        return Err(error(input, "trailing garbage after address", s));
    }
    if i < 16 && !ellipsis {
        return Err(error(input, "address string too short", b""));
    }
    if i == 16 && ellipsis {
        return Err(error(
            input,
            "the :: must expand to at least one field of zeros",
            b"",
        ));
    }
    Ok(())
}
fn parse(input: &[u8]) -> Result<bool, String> {
    for &c in input {
        match c {
            b'.' => {
                ipv4(input, input)?;
                return Ok(true);
            }
            b':' => {
                ipv6(input)?;
                return Ok(false);
            }
            b'%' => return Err(error(input, "missing IPv6 address", b"")),
            _ => {}
        }
    }
    Err(error(input, "unable to parse IP", b""))
}
