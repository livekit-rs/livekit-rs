//! Channel builders ported from pinned psrpc/pkg/info/channels.go.
//! Client/server/local builders intentionally do not share legacy escaping rules.

/// One bus destination represented for its legacy and hierarchical transports.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Channel {
    pub legacy: String,
    pub server: String,
    pub local: String,
}

enum Part<'a> {
    Text(&'a str),
    Topics(&'a [&'a str]),
}

fn escaped(out: &mut String, text: &str) {
    use std::fmt::Write;
    for c in text.chars() {
        if c.is_ascii_alphanumeric() || c == '_' {
            out.push(c);
        } else if (c as u32) < 0x10000 {
            write!(out, "u+{:04x}", c as u32).unwrap();
        } else {
            write!(out, "U+{:08x}", c as u32).unwrap();
        }
    }
}

fn append_parts(out: &mut String, delimiter: char, parts: &[Part<'_>]) {
    let mut prefix = false;
    for part in parts {
        if prefix {
            out.push(delimiter);
        }
        let previous_length = out.len();
        match part {
            Part::Text(text) => escaped(out, text),
            Part::Topics(topics) => {
                let nested: Vec<_> = topics.iter().map(|text| Part::Text(text)).collect();
                append_parts(out, delimiter, &nested);
            }
        }
        prefix = out.len() > previous_length;
    }
}

fn legacy(service: &str, method: &str, topics: &[&str], suffix: &str) -> String {
    let mut out = String::new();
    append_parts(
        &mut out,
        '|',
        &[
            Part::Text(service),
            Part::Text(method),
            Part::Topics(topics),
            Part::Text(suffix),
        ],
    );
    out
}

fn server(service: &str, topics: &[&str], queue: bool) -> String {
    let mut out = format!("SRV.{service}");
    for topic in topics.iter().filter(|topic| !topic.is_empty()) {
        out.push('.');
        escaped(&mut out, topic);
    }
    if queue {
        out.push_str(".Q");
    }
    out
}

/// Request channel; queue affects only the hierarchical server subject.
pub fn request(service: &str, method: &str, topics: &[&str], queue: bool) -> Channel {
    Channel {
        legacy: legacy(service, method, topics, "REQ"),
        server: server(service, topics, queue),
        local: format!("{method}.REQ"),
    }
}

/// Claim response channel has no queue suffix even for a queued request.
pub fn claim_response(service: &str, method: &str, topics: &[&str]) -> Channel {
    Channel {
        legacy: legacy(service, method, topics, "RCLAIM"),
        server: server(service, topics, false),
        local: format!("{method}.RCLAIM"),
    }
}

/// Per-method server stream channel.
pub fn stream_server(service: &str, method: &str, topics: &[&str]) -> Channel {
    Channel {
        legacy: legacy(service, method, topics, "STR"),
        server: server(service, topics, false),
        local: format!("{method}.STR"),
    }
}

fn client(service: &str, client: &str, suffix: &str) -> Channel {
    let mut flat = String::new();
    append_parts(
        &mut flat,
        '|',
        &[Part::Text(service), Part::Text(client), Part::Text(suffix)],
    );
    Channel {
        legacy: flat,
        server: format!("CLI.{service}.{client}.{suffix}"),
        local: String::new(),
    }
}

pub fn response(service: &str, client_id: &str) -> Channel {
    client(service, client_id, "RES")
}
pub fn claim_request(service: &str, client_id: &str) -> Channel {
    client(service, client_id, "CLAIM")
}
pub fn stream_client(service: &str, node_id: &str) -> Channel {
    client(service, node_id, "STR")
}

/// Handler keys escape each field using '.', unlike Local's raw method name.
pub fn handler_key(method: &str, topics: &[&str]) -> String {
    let mut out = String::new();
    append_parts(&mut out, '.', &[Part::Text(method), Part::Topics(topics)]);
    out
}
