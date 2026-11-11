// Copyright 2023 LiveKit, Inc. Licensed under Apache-2.0.
// Source: protocol/livekit/sip_validation.go (fixed SOURCE.json).
use super::ValidationResult;
fn token(c: char) -> bool {
    c.is_ascii_alphanumeric() || "-.!%*_+`'~".contains(c)
}
fn invalid_char(c: char) -> String {
    if (c as u32) >= 128 {
        format!(
            "char {} out of range, consider explicilty adding utf8 characters",
            c as u32
        )
    } else {
        format!("char {} not allowed", c as u32)
    }
}
pub fn validate_header_name(name: &str, restrict: bool) -> Result<(), String> {
    if name.is_empty() {
        return Err("header name cannot be empty".into());
    }
    if name.len() > 255 {
        return Err("header name too long (max 255 characters)".into());
    }
    if let Some(c) = name.chars().find(|c| !token(*c)) {
        return Err(format!(
            "header name {name} contains invalid characters: {}",
            invalid_char(c)
        ));
    }
    if restrict
        && matches!(
            name.to_lowercase().as_str(),
            "accept"
                | "accept-encoding"
                | "accept-language"
                | "allow"
                | "allow-events"
                | "call-id"
                | "contact"
                | "content-encoding"
                | "content-length"
                | "content-type"
                | "cseq"
                | "event"
                | "expires"
                | "from"
                | "max-forwards"
                | "record-route"
                | "refer-to"
                | "reply-to"
                | "k"
                | "l"
                | "m"
                | "o"
                | "r"
                | "t"
                | "u"
                | "v"
        )
    {
        return Err(format!("header name {name} not supported"));
    }
    Ok(())
}
pub fn validate_header_value(name: &str, value: &str) -> ValidationResult {
    if value.is_empty() {
        return ValidationResult::default();
    }
    if value.len() > 1024 {
        return ValidationResult::fail(format!(
            "header {name}: value too long (max 1024 characters)"
        ));
    }
    if let Some(c) = value
        .chars()
        .find(|c| c.is_ascii() && !matches!(*c, '\t' | ' '..='~'))
    {
        return ValidationResult::fail(format!("header {name}: value: {}", invalid_char(c)));
    }
    let mut out = ValidationResult::default();
    if matches!(
        name.to_lowercase().as_str(),
        "from"
            | "to"
            | "contact"
            | "route"
            | "record-route"
            | "reply-to"
            | "p-asserted-identity"
            | "referred-by"
    ) && let Err(e) = name_addr(value)
    {
        out.soft.push(format!("header {name}: value: {e}"));
    }
    out
}
fn name_addr(value: &str) -> Result<(), String> {
    let (mut start, mut end) = (None, None);
    for (i, c) in value.char_indices() {
        match c {
            '<' => {
                if start.is_some() {
                    return Err("multiple opening brackets".into());
                }
                start = Some(i)
            }
            '>' => {
                if end.is_some() {
                    return Err("multiple closing brackets".into());
                }
                end = Some(i)
            }
            _ => {}
        }
    }
    if start.is_some() != end.is_some() {
        return Err("mismatched angle brackets".into());
    }
    let uri = match (start, end) {
        (Some(s), Some(e)) => {
            if s > e {
                return Err("malformed angle brackets".into());
            }
            display_name(value[..s].trim())?;
            &value[s + 1..e]
        }
        _ => {
            if value.contains([',', '?', ' ']) {
                return Err("bare URI with special characters".into());
            }
            value
        }
    };
    let scheme = uri.split(':').next().unwrap_or("");
    if !matches!(scheme, "sip" | "sips" | "tel") {
        return Err("uri: scheme not one of sip, sips, or tel".into());
    }
    if uri.contains(' ') {
        return Err("uri: contains spaces".into());
    }
    Ok(())
}
fn display_name(name: &str) -> Result<(), String> {
    if name.is_empty() {
        return Ok(());
    }
    if name.starts_with('"') && name.ends_with('"') {
        return go_quoted_string(name).map_err(|e| format!("display name: {e}"));
    }
    if let Some(c) = name
        .chars()
        .find(|c| !token(*c) && !matches!(*c, ' ' | '\t'))
    {
        return Err(format!("display name: {}", invalid_char(c)));
    }
    Ok(())
}
// strconv.Unquote double-quoted string validity, including Go-only escapes.
fn go_quoted_string(name: &str) -> Result<(), String> {
    let b = name.as_bytes();
    if b.len() < 2 {
        return Err("invalid syntax".into());
    }
    let inner = &b[1..b.len() - 1];
    let mut i = 0;
    while i < inner.len() {
        let c = inner[i];
        i += 1;
        if c == b'"' || c == b'\n' {
            return Err("invalid syntax".into());
        }
        if c != b'\\' {
            continue;
        }
        if i == inner.len() {
            return Err("invalid syntax".into());
        }
        let c = inner[i];
        i += 1;
        match c {
            b'a' | b'b' | b'f' | b'n' | b'r' | b't' | b'v' | b'\\' | b'"' => {}
            b'x' | b'u' | b'U' => {
                let n = match c {
                    b'x' => 2,
                    b'u' => 4,
                    _ => 8,
                };
                if inner.len() - i < n {
                    return Err("invalid syntax".into());
                }
                if !inner[i..i + n].iter().all(u8::is_ascii_hexdigit) {
                    return Err("invalid syntax".into());
                }
                let digits = std::str::from_utf8(&inner[i..i + n]).map_err(|_| "invalid syntax")?;
                let v = u32::from_str_radix(digits, 16).map_err(|_| "invalid syntax")?;
                if c != b'x' && char::from_u32(v).is_none() {
                    return Err("invalid syntax".into());
                }
                i += n;
            }
            b'0'..=b'7' => {
                if inner.len() - i < 2 {
                    return Err("invalid syntax".into());
                }
                let mut v = u32::from(c - b'0');
                for &d in &inner[i..i + 2] {
                    if !(b'0'..=b'7').contains(&d) {
                        return Err("invalid syntax".into());
                    }
                    v = v * 8 + u32::from(d - b'0');
                }
                if v > 255 {
                    return Err("invalid syntax".into());
                }
                i += 2;
            }
            _ => return Err("invalid syntax".into()),
        }
    }
    Ok(())
}
