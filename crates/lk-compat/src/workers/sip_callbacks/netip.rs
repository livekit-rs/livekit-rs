// Copyright 2009 The Go Authors. SPDX-License-Identifier: BSD-3-Clause
// Error-validation port of Go 1.26 net/netip ParseAddr/parseIPv4Fields/parseIPv6.
// No address object or policy is manufactured. See LICENSE-GO and SOURCE.json.
use crate::service_bus::rpc::RpcError;
use crate::sip::domain::is_go_print;
pub(super) fn go_quote(value: &str) -> String {
    let mut out = String::from("\"");
    for c in value.chars() {
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
            c if (c as u32) < 32 || c == '\u{7f}' => out.push_str(&format!("\\x{:02x}", c as u32)),
            c if is_go_print(c) => out.push(c),
            c if (c as u32) < 0x10000 => out.push_str(&format!("\\u{:04x}", c as u32)),
            c => out.push_str(&format!("\\U{:08x}", c as u32)),
        }
    }
    out.push('"');
    out
}
fn error(input: &str, message: &str, at: &str) -> String {
    let suffix = if at.is_empty() {
        String::new()
    } else {
        format!(" (at {})", go_quote(at))
    };
    format!("ParseAddr({}): {message}{suffix}", go_quote(input))
}
fn ipv4(input: &str, s: &str) -> Result<(), String> {
    let (mut value, mut position, mut digits) = (0u32, 0, 0);
    let b = s.as_bytes();
    for (i, &c) in b.iter().enumerate() {
        if c.is_ascii_digit() {
            if digits == 1 && value == 0 {
                return Err(error(input, "IPv4 field has octet with leading zero", ""));
            }
            value = value * 10 + u32::from(c - b'0');
            digits += 1;
            if value > 255 {
                return Err(error(input, "IPv4 field has value >255", ""));
            }
        } else if c == b'.' {
            if i == 0 || i == b.len() - 1 || b[i - 1] == b'.' {
                return Err(error(
                    input,
                    "IPv4 field must have at least one digit",
                    &s[i..],
                ));
            }
            if position == 3 {
                return Err(error(input, "IPv4 address too long", ""));
            }
            position += 1;
            value = 0;
            digits = 0;
        } else {
            return Err(error(input, "unexpected character", &s[i..]));
        }
    }
    if position < 3 {
        return Err(error(input, "IPv4 address too short", ""));
    }
    Ok(())
}
fn ipv6(input: &str) -> Result<(), String> {
    let mut s = input;
    if let Some((host, zone)) = s.split_once('%') {
        if zone.is_empty() {
            return Err(error(input, "zone must be a non-empty string", ""));
        }
        s = host;
    }
    let mut ellipsis = false;
    if let Some(rest) = s.strip_prefix("::") {
        ellipsis = true;
        s = rest;
        if s.is_empty() {
            return Ok(());
        }
    }
    let mut i = 0;
    while i < 16 {
        let mut off = 0;
        let mut value = 0u32;
        for c in s.bytes() {
            let Some(digit) = char::from(c).to_digit(16) else {
                break;
            };
            value = (value << 4) + digit;
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
        if s.as_bytes().get(off) == Some(&b'.') {
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
            s = "";
            i += 4;
            break;
        }
        i += 2;
        s = &s[off..];
        if s.is_empty() {
            break;
        }
        if !s.starts_with(':') {
            return Err(error(input, "unexpected character, want colon", s));
        }
        if s.len() == 1 {
            return Err(error(input, "colon must be followed by more characters", s));
        }
        s = &s[1..];
        if let Some(rest) = s.strip_prefix(':') {
            if ellipsis {
                return Err(error(input, "multiple :: in address", s));
            }
            ellipsis = true;
            s = rest;
            if s.is_empty() {
                break;
            }
        }
    }
    if !s.is_empty() {
        return Err(error(input, "trailing garbage after address", s));
    }
    if i < 16 && !ellipsis {
        return Err(error(input, "address string too short", ""));
    }
    if i == 16 && ellipsis {
        return Err(error(
            input,
            "the :: must expand to at least one field of zeros",
            "",
        ));
    }
    Ok(())
}
pub(super) fn parse(input: &str) -> Result<(), String> {
    for c in input.bytes() {
        match c {
            b'.' => return ipv4(input, input),
            b':' => return ipv6(input),
            b'%' => return Err(error(input, "missing IPv6 address", "")),
            _ => {}
        }
    }
    Err(error(input, "unable to parse IP", ""))
}
pub(super) fn validate_source(input: &str) -> Result<(), RpcError> {
    if input.is_empty() {
        return Ok(());
    }
    parse(input).map_err(|e| super::twirp_error("invalid_argument", e))
}
