// Apache-2.0; adapted from fixed pkg/agent/endpoint/{manifest,router/template}.go.
use super::super::registry::WorkerError;
use lk_protocol::livekit as pb;
use regex::Regex;
use std::collections::HashSet;

#[derive(Clone, Debug)]
pub struct Endpoint {
    pub raw: String,
    pub canonical: String,
    pub methods: Vec<String>,
    pub public: bool,
    pattern: Regex,
}
impl Endpoint {
    pub fn matches(&self, path: &str) -> bool {
        self.pattern.is_match(path)
    }
}
fn invalid(s: impl Into<String>) -> WorkerError {
    WorkerError::InvalidRegistration(s.into())
}
pub fn negotiate(req: &pb::RegisterWorkerRequest) -> Result<Vec<Endpoint>, WorkerError> {
    if req.agent_name.is_empty() {
        return Err(invalid("agent HTTP endpoints require an agent name"));
    }
    if matches!(req.agent_name.as_str(), "_" | "." | "..") {
        return Err(invalid(format!(
            "agent name {:?} is reserved and cannot serve HTTP endpoints",
            req.agent_name
        )));
    }
    if req.endpoints.len() > 256 {
        return Err(invalid("manifest exceeds 256 routes"));
    }
    let mut endpoints = Vec::new();
    for ep in &req.endpoints {
        let (pattern, canonical) = template(&ep.path)?;
        if ep.kind != 0 {
            return Err(invalid(format!(
                "endpoint {:?} has unsupported kind {}",
                ep.path, ep.kind
            )));
        }
        if ep.methods.is_empty() {
            return Err(invalid(format!(
                "endpoint {:?} declares no methods",
                ep.path
            )));
        }
        for method in &ep.methods {
            if method.to_uppercase() != *method {
                return Err(invalid(format!(
                    "endpoint {:?} method {:?} must be uppercase",
                    ep.path, method
                )));
            }
            if !matches!(
                method.as_str(),
                "GET" | "HEAD" | "POST" | "PUT" | "PATCH" | "DELETE" | "OPTIONS" | "TRACE"
            ) {
                return Err(invalid(format!(
                    "endpoint {:?} declares unsupported method {:?}",
                    ep.path, method
                )));
            }
        }
        endpoints.push(Endpoint {
            raw: ep.path.clone(),
            canonical,
            methods: ep.methods.clone(),
            public: ep.public,
            pattern,
        });
    }
    if req.instance_id.is_empty() {
        return Err(invalid(
            "registrations with endpoints require an instance_id",
        ));
    }
    if req.endpoint_protocol == 0 {
        return Err(invalid(
            "worker declared endpoints but no endpoint protocol; upgrade the agent SDK to one that speaks the endpoint data plane",
        ));
    }
    Ok(endpoints)
}
fn ident(b: u8) -> bool {
    b.is_ascii_alphabetic() || b == b'_'
}
fn scan(s: &[u8], start: usize) -> usize {
    if start >= s.len() || !ident(s[start]) {
        return start;
    }
    let mut i = start + 1;
    while i < s.len() && (ident(s[i]) || s[i].is_ascii_digit()) {
        i += 1;
    }
    i
}
fn template(path: &str) -> Result<(Regex, String), WorkerError> {
    if !path.starts_with('/') {
        return Err(invalid(format!(
            "path template must start with '/': {path:?}"
        )));
    }
    let b = path.as_bytes();
    let mut i = 0;
    let mut literal = 0;
    let mut pattern = String::from("\\A");
    let mut canonical = String::new();
    let mut seen = HashSet::new();
    while i < b.len() {
        if b[i] != b'{' {
            i += 1;
            continue;
        }
        let end_name = scan(b, i + 1);
        if end_name == i + 1 {
            i += 1;
            continue;
        }
        let name = &path[i + 1..end_name];
        let mut j = end_name;
        let mut converter = "str";
        if j < b.len() && b[j] == b':' {
            let end = scan(b, j + 1);
            if end == j + 1 {
                i += 1;
                continue;
            }
            converter = &path[j + 1..end];
            j = end;
        }
        if j >= b.len() || b[j] != b'}' {
            i += 1;
            continue;
        }
        let (kind, regex) = match converter {
            "str" => (1, "[^/]+"),
            "path" => (2, ".*"),
            "int" => (3, "[0-9]+"),
            "float" => (4, "[0-9]+(?:\\.[0-9]+)?"),
            "uuid" => (
                5,
                "[0-9a-fA-F]{8}-?[0-9a-fA-F]{4}-?[0-9a-fA-F]{4}-?[0-9a-fA-F]{4}-?[0-9a-fA-F]{12}",
            ),
            _ => {
                return Err(invalid(format!(
                    "unknown path convertor {converter:?} in template {path:?}"
                )));
            }
        };
        if !seen.insert(name) {
            return Err(invalid(format!(
                "duplicated param name {name:?} in template {path:?}"
            )));
        }
        if i > literal {
            let lit = &path[literal..i];
            pattern.push_str(&regex::escape(lit));
            canonical.push_str(&format!("L{}:{lit}", lit.len()));
        }
        pattern.push_str(regex);
        canonical.push_str(&format!("K{kind}"));
        i = j + 1;
        literal = i;
    }
    if literal < b.len() {
        let lit = &path[literal..];
        pattern.push_str(&regex::escape(lit));
        canonical.push_str(&format!("L{}:{lit}", lit.len()));
    }
    pattern.push_str("\\z");
    Ok((
        Regex::new(&pattern).map_err(|e| invalid(e.to_string()))?,
        canonical,
    ))
}
