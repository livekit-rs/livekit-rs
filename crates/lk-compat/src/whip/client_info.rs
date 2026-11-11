// Go1.26 encoding/json Decoder.Decode + fixed WHIP anonymous ClientInfo struct.
// Source-backed iterative syntax scanner; only source-selected fields are decoded.
use std::collections::BTreeMap;
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ClientInfo {
    pub client_ip: String,
    pub subscribed: BTreeMap<String, Vec<String>>,
}
#[derive(Clone, Copy)]
enum Frame {
    Object(u8),
    Array(u8),
}
fn white(b: u8) -> bool {
    matches!(b, b' ' | b'\n' | b'\r' | b'\t')
}
fn ws(b: &[u8], mut i: usize) -> usize {
    while i < b.len() && white(b[i]) {
        i += 1;
    }
    i
}
fn quote(b: u8) -> String {
    let s = match b {
        b'\'' => "\\'".into(),
        b'"' => "\"".into(),
        b'\\' => "\\\\".into(),
        7 => "\\a".into(),
        8 => "\\b".into(),
        12 => "\\f".into(),
        10 => "\\n".into(),
        13 => "\\r".into(),
        9 => "\\t".into(),
        11 => "\\v".into(),
        0..=31 | 127 => format!("\\x{b:02x}"),
        128..=160 | 173 => format!("\\u{b:04x}"),
        _ => char::from(b).to_string(),
    };
    format!("'{s}'")
}
fn bad(b: u8, context: &str) -> String {
    format!("invalid character {} {context}", quote(b))
}
fn string_end(b: &[u8], mut i: usize) -> Result<usize, String> {
    i += 1;
    while i < b.len() {
        match b[i] {
            b'"' => return Ok(i + 1),
            b'\\' => {
                i += 1;
                if i == b.len() {
                    return Err("unexpected EOF".into());
                }
                match b[i] {
                    b'b' | b'f' | b'n' | b'r' | b't' | b'\\' | b'/' | b'"' => {}
                    b'u' => {
                        for _ in 0..4 {
                            i += 1;
                            if i == b.len() {
                                return Err("unexpected EOF".into());
                            }
                            if !b[i].is_ascii_hexdigit() {
                                return Err(bad(b[i], "in \\u hexadecimal character escape"));
                            }
                        }
                    }
                    c => return Err(bad(c, "in string escape code")),
                }
            }
            c if c < 32 => return Err(bad(c, "in string literal")),
            _ => {}
        }
        i += 1;
    }
    Err("unexpected EOF".into())
}
fn scalar_end(b: &[u8], mut i: usize) -> Result<usize, String> {
    if b[i] == b'"' {
        return string_end(b, i);
    }
    if matches!(b[i], b't' | b'f' | b'n') {
        let literal: &[u8] = match b[i] {
            b't' => b"true",
            b'f' => b"false",
            _ => b"null",
        };
        for (offset, want) in literal.iter().copied().enumerate().skip(1) {
            let at = i + offset;
            let got = *b.get(at).ok_or("unexpected EOF")?;
            if got != want {
                return Err(bad(
                    got,
                    &format!(
                        "in literal {} (expecting '{}')",
                        std::str::from_utf8(literal).unwrap(),
                        char::from(want)
                    ),
                ));
            }
        }
        return Ok(i + literal.len());
    }
    if b[i] == b'-' {
        i += 1;
        let got = *b.get(i).ok_or("unexpected EOF")?;
        if !got.is_ascii_digit() {
            return Err(bad(got, "in numeric literal"));
        }
    }
    match b.get(i).copied() {
        Some(b'0') => i += 1,
        Some(b'1'..=b'9') => {
            i += 1;
            while b.get(i).is_some_and(u8::is_ascii_digit) {
                i += 1;
            }
        }
        Some(c) => return Err(bad(c, "looking for beginning of value")),
        None => return Err("unexpected EOF".into()),
    }
    if b.get(i) == Some(&b'.') {
        i += 1;
        let got = *b.get(i).ok_or("unexpected EOF")?;
        if !got.is_ascii_digit() {
            return Err(bad(got, "after decimal point in numeric literal"));
        }
        while b.get(i).is_some_and(u8::is_ascii_digit) {
            i += 1;
        }
    }
    if b.get(i).is_some_and(|c| matches!(c, b'e' | b'E')) {
        i += 1;
        if b.get(i).is_some_and(|c| matches!(c, b'+' | b'-')) {
            i += 1;
        }
        let got = *b.get(i).ok_or("unexpected EOF")?;
        if !got.is_ascii_digit() {
            return Err(bad(got, "in exponent of numeric literal"));
        }
        while b.get(i).is_some_and(u8::is_ascii_digit) {
            i += 1;
        }
    }
    Ok(i)
}
/// End offset of exactly the FIRST complete JSON value, matching Decoder.Decode.
fn value_end(b: &[u8], start: usize) -> Result<usize, String> {
    let mut i = ws(b, start);
    if i == b.len() {
        return Err(if start == 0 { "EOF" } else { "unexpected EOF" }.into());
    }
    let mut stack: Vec<Frame> = Vec::new();
    let mut need_value = true;
    loop {
        i = ws(b, i);
        let c = *b.get(i).ok_or("unexpected EOF")?;
        if need_value {
            if matches!(c, b'{' | b'[') {
                stack.push(if c == b'{' {
                    Frame::Object(0)
                } else {
                    Frame::Array(0)
                });
                if stack.len() > 10000 {
                    return Err(bad(c, "exceeded max depth"));
                }
                i += 1;
                need_value = false;
                continue;
            }
            i = scalar_end(b, i)?;
            need_value = false;
            if stack.is_empty() {
                return Ok(i);
            }
            continue;
        }
        let Some(frame) = stack.last_mut() else {
            return Ok(i);
        };
        match frame {
            Frame::Object(state) => match *state {
                0 | 1 => {
                    if *state == 0 && c == b'}' {
                        stack.pop();
                        i += 1;
                        if stack.is_empty() {
                            return Ok(i);
                        }
                    } else {
                        if c != b'"' {
                            return Err(bad(c, "looking for beginning of object key string"));
                        }
                        i = string_end(b, i)?;
                        *state = 2;
                    }
                }
                2 => {
                    if c != b':' {
                        return Err(bad(c, "after object key"));
                    }
                    i += 1;
                    *state = 3;
                    need_value = true;
                }
                _ => match c {
                    b',' => {
                        i += 1;
                        *state = 1;
                    }
                    b'}' => {
                        stack.pop();
                        i += 1;
                        if stack.is_empty() {
                            return Ok(i);
                        }
                    }
                    _ => return Err(bad(c, "after object key:value pair")),
                },
            },
            Frame::Array(state) => match *state {
                0 | 1 => {
                    if *state == 0 && c == b']' {
                        stack.pop();
                        i += 1;
                        if stack.is_empty() {
                            return Ok(i);
                        }
                    } else {
                        *state = 2;
                        need_value = true;
                    }
                }
                _ => match c {
                    b',' => {
                        i += 1;
                        *state = 1;
                    }
                    b']' => {
                        stack.pop();
                        i += 1;
                        if stack.is_empty() {
                            return Ok(i);
                        }
                    }
                    _ => return Err(bad(c, "after array element")),
                },
            },
        }
    }
}
fn unquote(b: &[u8]) -> String {
    let mut out = String::new();
    let mut i = 1;
    while i + 1 < b.len() {
        if b[i] != b'\\' {
            let start = i;
            while i + 1 < b.len() && b[i] != b'\\' {
                i += 1;
            }
            out.push_str(&String::from_utf8_lossy(&b[start..i]));
            continue;
        }
        i += 1;
        match b[i] {
            b'"' => out.push('"'),
            b'\\' => out.push('\\'),
            b'/' => out.push('/'),
            b'b' => out.push('\x08'),
            b'f' => out.push('\x0c'),
            b'n' => out.push('\n'),
            b'r' => out.push('\r'),
            b't' => out.push('\t'),
            b'u' => {
                let hex = |part: &[u8]| {
                    u16::from_str_radix(std::str::from_utf8(part).unwrap(), 16).unwrap()
                };
                let first = hex(&b[i + 1..i + 5]);
                i += 4;
                let rune = if (0xd800..=0xdbff).contains(&first)
                    && b.get(i + 1..i + 3) == Some(b"\\u")
                    && i + 7 < b.len()
                {
                    let second = hex(&b[i + 3..i + 7]);
                    if (0xdc00..=0xdfff).contains(&second) {
                        i += 6;
                        0x10000 + ((u32::from(first) - 0xd800) << 10) + (u32::from(second) - 0xdc00)
                    } else {
                        0xfffd
                    }
                } else if (0xd800..=0xdfff).contains(&first) {
                    0xfffd
                } else {
                    u32::from(first)
                };
                out.push(char::from_u32(rune).unwrap_or('\u{fffd}'));
            }
            _ => unreachable!("scanner validated escapes"),
        };
        i += 1;
    }
    out
}
fn pairs(b: &[u8]) -> Vec<(String, &[u8])> {
    let mut out = Vec::new();
    let mut i = ws(b, 1);
    while b[i] != b'}' {
        let end = string_end(b, i).unwrap();
        let key = unquote(&b[i..end]);
        i = ws(b, end) + 1;
        i = ws(b, i);
        let end = value_end(b, i).unwrap();
        out.push((key, &b[i..end]));
        i = ws(b, end);
        if b[i] == b',' {
            i = ws(b, i + 1);
        } else {
            break;
        }
    }
    out
}
fn elements(b: &[u8]) -> Vec<&[u8]> {
    let mut out = Vec::new();
    let mut i = ws(b, 1);
    while b[i] != b']' {
        let end = value_end(b, i).unwrap();
        out.push(&b[i..end]);
        i = ws(b, end);
        if b[i] == b',' {
            i = ws(b, i + 1);
        } else {
            break;
        }
    }
    out
}
fn folded(key: &str) -> String {
    key.chars()
        .map(|c| match c {
            'ſ' => 'S',
            'K' => 'K',
            c => c.to_ascii_uppercase(),
        })
        .collect()
}
fn kind(b: &[u8]) -> &'static str {
    match b[0] {
        b'"' => "string",
        b'[' => "array",
        b'{' => "object",
        b't' | b'f' => "bool",
        b'n' => "null",
        _ => "number",
    }
}
fn mismatch(b: &[u8], field: &str, ty: &str) -> String {
    format!(
        "json: cannot unmarshal {} into Go struct field .{field} of type {ty}",
        kind(b)
    )
}
pub fn decode(bytes: &[u8]) -> Result<ClientInfo, String> {
    let end = value_end(bytes, 0)?;
    let start = ws(bytes, 0);
    let root = &bytes[start..end];
    let mut out = ClientInfo::default();
    if root == b"null" {
        return Ok(out);
    }
    const STRUCT_TYPE: &str = r#"struct { ClientIP string "json:\"clientIp\""; SubscribedParticipantTrackNames map[string][]string "json:\"subscribedParticipantTrackNames\"" }"#;
    if root[0] != b'{' {
        return Err(format!(
            "json: cannot unmarshal {} into Go value of type {STRUCT_TYPE}",
            kind(root)
        ));
    }
    let mut error = None;
    for (key, value) in pairs(root) {
        match folded(&key).as_str() {
            "CLIENTIP" => {
                if value == b"null" {
                    continue;
                }
                if value[0] == b'"' {
                    out.client_ip = unquote(value);
                } else {
                    error.get_or_insert_with(|| mismatch(value, "clientIp", "string"));
                }
            }
            "SUBSCRIBEDPARTICIPANTTRACKNAMES" => {
                if value == b"null" {
                    out.subscribed.clear();
                    continue;
                }
                if value[0] != b'{' {
                    error.get_or_insert_with(|| {
                        mismatch(
                            value,
                            "subscribedParticipantTrackNames",
                            "map[string][]string",
                        )
                    });
                    continue;
                }
                for (name, list) in pairs(value) {
                    if list == b"null" {
                        out.subscribed.insert(name, Vec::new());
                        continue;
                    }
                    if list[0] != b'[' {
                        error.get_or_insert_with(|| {
                            mismatch(list, "subscribedParticipantTrackNames", "[]string")
                        });
                        continue;
                    }
                    let mut tracks = Vec::new();
                    for item in elements(list) {
                        if item == b"null" {
                            tracks.push(String::new());
                        } else if item[0] == b'"' {
                            tracks.push(unquote(item));
                        } else {
                            error.get_or_insert_with(|| {
                                mismatch(item, "subscribedParticipantTrackNames", "string")
                            });
                            tracks.push(String::new());
                        }
                    }
                    out.subscribed.insert(name, tracks);
                }
            }
            _ => {}
        }
    }
    if let Some(error) = error {
        Err(error)
    } else {
        Ok(out)
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn preserves_ordered_duplicate_map_null_and_first_document() {
        assert_eq!(
            decode(br#"{"clientIp":"one","clientIp":null} {"clientIp":"ignored"}"#)
                .unwrap()
                .client_ip,
            "one"
        );
        assert_eq!(decode(br#"{"subscribedParticipantTrackNames":{"p":["a"]},"subscribedParticipantTrackNames":{"q":["b"]}}"#).unwrap().subscribed.len(),2);
        assert!(decode(br#"{"subscribedParticipantTrackNames":{"p":["a"]},"subscribedParticipantTrackNames":null}"#).unwrap().subscribed.is_empty());
    }
    #[test]
    #[ignore = "requires actual fixed Go1.26 ClientInfo decoder oracle"]
    fn actual_go_client_header_decoder_rows() {
        let cases: Vec<serde_json::Value> = serde_json::from_slice(
            &std::fs::read(std::env::var("WHIP_CLIENT_GO_CASES").expect("cases")).unwrap(),
        )
        .unwrap();
        let results = std::fs::read_to_string(
            std::env::var("WHIP_CLIENT_GO_RESULTS").expect("actual Go JSONL"),
        )
        .unwrap();
        let rows: Vec<serde_json::Value> = results
            .lines()
            .map(|l| serde_json::from_str(l).unwrap())
            .collect();
        assert_eq!(cases.len(), 33);
        assert_eq!(rows.len(), 33);
        for (case, row) in cases.iter().zip(rows.iter()) {
            assert_eq!(case["id"], row["id"]);
            match decode(case["input"].as_str().unwrap().as_bytes()) {
                Err(error) => assert_eq!(error, row["error"].as_str().unwrap(), "{}", case["id"]),
                Ok(value) => {
                    assert_eq!(row["error"], "");
                    assert_eq!(value.client_ip, row["client_ip"]);
                    assert_eq!(
                        serde_json::to_value(value.subscribed).unwrap(),
                        row["subscribed"]
                    );
                }
            }
        }
    }
}
