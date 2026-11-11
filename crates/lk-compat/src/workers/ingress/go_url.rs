//! Narrow net/url Parse + String port for gateway input, Go1.26.0.
//! Go BSD-3-Clause; see archived source/Go LICENSE. Not compiled or executed.
//! Byte-oriented netip and strconv primitives are in url_support.rs; no
//! WHATWG URL or Rust Debug string approximation.
use super::gateway::{GoUrlParser, ParsedUrl};
use crate::service_bus::rpc::RpcError;
const PATH: u8 = 1;
const HOST: u8 = 4;
const ZONE: u8 = 8;
const USER: u8 = 16;
const FRAGMENT: u8 = 64;
const TABLE: [u8; 256] = [
    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
    0, 76, 12, 0, 95, 0, 95, 12, 76, 76, 76, 95, 93, 127, 127, 65, 255, 255, 255, 255, 255, 255,
    255, 255, 255, 255, 79, 93, 12, 95, 12, 64, 67, 255, 255, 255, 255, 255, 255, 127, 127, 127,
    127, 127, 127, 127, 127, 127, 127, 127, 127, 127, 127, 127, 127, 127, 127, 127, 127, 12, 0, 12,
    0, 127, 0, 255, 255, 255, 255, 255, 255, 127, 127, 127, 127, 127, 127, 127, 127, 127, 127, 127,
    127, 127, 127, 127, 127, 127, 127, 127, 127, 0, 0, 0, 127, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
];
use super::url_support::UrlSupport;
#[derive(Default)]
pub struct FixedGoUrl;
impl GoUrlParser for FixedGoUrl {
    fn parse(&self, raw: &str) -> Result<ParsedUrl, RpcError> {
        let support = UrlSupport;
        parse(raw, &support).map_err(|(scope, error)| {
            RpcError::new(
                "invalid_argument",
                format!("parse {}: {}", support.quote(scope.as_bytes()), error),
            )
        })
    }
}
fn parse<'a>(raw: &'a str, s: &UrlSupport) -> Result<ParsedUrl, (&'a str, String)> {
    let (base, frag) = raw.split_once('#').unwrap_or((raw, ""));
    let (scheme, normalized) = parse_base(base, s).map_err(|e| (base, e))?;
    if frag.is_empty() {
        return Ok(ParsedUrl { scheme, normalized });
    }
    let fragment = escaped_component(frag, FRAGMENT, s).map_err(|e| (raw, e))?;
    Ok(ParsedUrl {
        scheme,
        normalized: format!("{normalized}#{fragment}"),
    })
}
fn parse_base(raw: &str, s: &UrlSupport) -> Result<(String, String), String> {
    if raw.bytes().any(|b| b < 32 || b == 127) {
        return Err("net/url: invalid control character in URL".into());
    }
    if raw == "*" {
        return Ok((String::new(), "*".into()));
    }
    let (scheme, rest) = scheme(raw)?;
    let scheme = scheme.to_ascii_lowercase();
    let (rest, query, force_query) =
        if rest.ends_with('?') && rest.bytes().filter(|b| *b == b'?').count() == 1 {
            (&rest[..rest.len() - 1], "", true)
        } else {
            let (a, b) = rest.split_once('?').unwrap_or((rest, ""));
            (a, b, false)
        };
    let mut output = String::new();
    if !scheme.is_empty() {
        output.push_str(&scheme);
        output.push(':');
    }
    if !rest.starts_with('/') && !scheme.is_empty() {
        output.push_str(rest);
        append_query(&mut output, query, force_query);
        return Ok((scheme, output));
    }
    if !rest.starts_with('/') && rest.split('/').next().unwrap_or("").contains(':') {
        return Err("first path segment in URL cannot contain colon".into());
    }
    let mut path = rest;
    let authority = if rest.starts_with("//") && (!scheme.is_empty() || !rest.starts_with("///")) {
        let tail = &rest[2..];
        let (a, p) = tail
            .find('/')
            .map_or((tail, ""), |i| (&tail[..i], &tail[i..]));
        path = p;
        Some(a)
    } else {
        None
    };
    let mut host = Vec::new();
    let mut user = None;
    if let Some(authority) = authority {
        let (userinfo, h) = authority
            .rsplit_once('@')
            .map_or((None, authority), |(u, h)| (Some(u), h));
        host = parse_host(&scheme, h, s)?;
        if let Some(u) = userinfo {
            if !u
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b"-._:~!$&'()*+,;=%@".contains(&b))
            {
                return Err("net/url: invalid userinfo".into());
            }
            user = Some(if let Some((a, b)) = u.split_once(':') {
                format!(
                    "{}:{}",
                    escape(&unescape(a, USER, s)?, USER),
                    escape(&unescape(b, USER, s)?, USER)
                )
            } else {
                escape(&unescape(u, USER, s)?, USER)
            });
        }
    }
    let escaped_path = escaped_component(path, PATH, s)?;
    let omit_host = authority.is_none() && !scheme.is_empty() && rest.starts_with('/');
    if (!scheme.is_empty() || !host.is_empty() || user.is_some())
        && !(omit_host && host.is_empty() && user.is_none())
    {
        if !host.is_empty() || !escaped_path.is_empty() || user.is_some() {
            output.push_str("//");
        }
        if let Some(user) = user {
            output.push_str(&user);
            output.push('@');
        }
        output.push_str(&escape(&host, HOST));
    }
    if !escaped_path.is_empty() && !escaped_path.starts_with('/') && !host.is_empty() {
        output.push('/');
    }
    if output.is_empty() && escaped_path.split('/').next().unwrap_or("").contains(':') {
        output.push_str("./");
    }
    output.push_str(&escaped_path);
    append_query(&mut output, query, force_query);
    Ok((scheme, output))
}
fn append_query(out: &mut String, q: &str, force: bool) {
    if force || !q.is_empty() {
        out.push('?');
        out.push_str(q);
    }
}
fn scheme(raw: &str) -> Result<(&str, &str), String> {
    for (i, b) in raw.bytes().enumerate() {
        match b {
            b'a'..=b'z' | b'A'..=b'Z' => {}
            b'0'..=b'9' | b'+' | b'-' | b'.' => {
                if i == 0 {
                    return Ok(("", raw));
                }
            }
            b':' => {
                return if i == 0 {
                    Err("missing protocol scheme".into())
                } else {
                    Ok((&raw[..i], &raw[i + 1..]))
                };
            }
            _ => return Ok(("", raw)),
        }
    }
    Ok(("", raw))
}
fn port(value: &str) -> bool {
    value.is_empty()
        || value
            .strip_prefix(':')
            .is_some_and(|x| x.bytes().all(|b| b.is_ascii_digit()))
}
fn parse_host(scheme: &str, host: &str, s: &UrlSupport) -> Result<Vec<u8>, String> {
    if let Some(open) = host.rfind('[') {
        let close = host.rfind(']').ok_or("missing ']' in host")?;
        let p = &host[close + 1..];
        if !port(p) {
            return Err(format!("invalid port {} after host", s.quote(p.as_bytes())));
        }
        let name = &host[open + 1..close];
        let hostname = if let Some(z) = name.find("%25") {
            let mut a = unescape(&name[..z], HOST, s)?;
            a.extend(unescape(&name[z..], ZONE, s)?);
            a
        } else {
            unescape(name, HOST, s)?
        };
        s.validate_ipv6_literal(&hostname)
            .map_err(|error| match error {
                super::url_support::LiteralError::Ipv4 => "invalid IP-literal".into(),
                super::url_support::LiteralError::Parse(error) => format!("invalid host: {error}"),
            })?;
        let mut value = vec![b'['];
        value.extend(hostname);
        value.push(b']');
        value.extend(unescape(p, HOST, s)?);
        return Ok(value);
    }
    if let Some(first) = host.find(':') {
        let i = if matches!(scheme, "postgresql" | "postgres") {
            host.rfind(':').unwrap()
        } else {
            first
        };
        if !port(&host[i..]) {
            return Err(format!(
                "invalid port {} after host",
                s.quote(&host.as_bytes()[i..])
            ));
        }
    }
    unescape(host, HOST, s)
}
fn unescape(raw: &str, mode: u8, s: &UrlSupport) -> Result<Vec<u8>, String> {
    let b = raw.as_bytes();
    let mut out = Vec::with_capacity(b.len());
    let mut i = 0;
    while i < b.len() {
        if b[i] == b'%' {
            let invalid = || {
                format!(
                    "invalid URL escape {}",
                    s.quote(&b[i..(i + 3).min(b.len())])
                )
            };
            if i + 2 >= b.len() || !b[i + 1].is_ascii_hexdigit() || !b[i + 2].is_ascii_hexdigit() {
                return Err(invalid());
            }
            let hex = |b: u8| {
                if b.is_ascii_digit() {
                    b - b'0'
                } else {
                    b.to_ascii_lowercase() - b'a' + 10
                }
            };
            let v = hex(b[i + 1]) * 16 + hex(b[i + 2]);
            if mode == HOST && v < 128 && &b[i..i + 3] != b"%25" {
                return Err(invalid());
            }
            if mode == ZONE && &b[i..i + 3] != b"%25" && v != b' ' && TABLE[v as usize] & HOST == 0
            {
                return Err(invalid());
            }
            out.push(v);
            i += 3;
        } else {
            if (mode == HOST || mode == ZONE) && b[i] < 128 && TABLE[b[i] as usize] & mode == 0 {
                return Err(format!(
                    "invalid character {} in host name",
                    s.quote(&b[i..i + 1])
                ));
            }
            out.push(b[i]);
            i += 1;
        }
    }
    Ok(out)
}
fn escape(raw: &[u8], mode: u8) -> String {
    let mut out = String::new();
    for b in raw {
        if TABLE[*b as usize] & mode != 0 {
            out.push(*b as char);
        } else {
            out.push_str(&format!("%{b:02X}"));
        }
    }
    out
}
fn escaped_component(raw: &str, mode: u8, s: &UrlSupport) -> Result<String, String> {
    let decoded = unescape(raw, mode, s)?;
    if mode == PATH && raw == "*" {
        return Ok("*".into());
    }
    let valid = raw
        .bytes()
        .all(|b| b"!$&'()*+,;=:@[]%".contains(&b) || TABLE[b as usize] & mode != 0);
    if valid {
        return Ok(raw.into());
    }
    Ok(escape(&decoded, mode))
}

impl FixedGoUrl {
    /// Same net/url Parse validation for Pion SDP's u= field. No HTTP URL policy
    /// or WHATWG normalization is imposed on the SDP grammar.
    pub(crate) fn validate_sdp_uri(raw: &str) -> Result<(), String> {
        let support = UrlSupport;
        parse(raw, &support).map(|_| ()).map_err(|(scope, error)| {
            format!("parse {}: {}", support.quote(scope.as_bytes()), error)
        })
    }
}
