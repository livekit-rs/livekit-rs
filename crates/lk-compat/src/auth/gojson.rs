//! Minimal emulation of Go `encoding/json` struct decoding over a
//! `serde_json::Value`, as used by the upstream token verifier:
//!
//! * object keys match struct field tags **case-insensitively** (an exact match
//!   is preferred); `room_join` does not match `roomJoin`;
//! * JSON `null` leaves a plain field at its zero value and a pointer field nil;
//! * a value of the wrong JSON type is a decode error.
//!
//! Deviation: when an object holds several keys that differ only in case, Go
//! keeps the one that appears last in the document; `serde_json::Map` does not
//! preserve document order, so the exact-case key wins, else the first
//! case-insensitive match in key order. Case folding uses Unicode lowercase
//! comparison rather than Go's simple-fold table (differs only for exotic
//! characters such as `ſ`).

use serde_json::{Map, Value};

/// Decode error with a Go-like message:
/// `json: cannot unmarshal <json type> into Go struct field <path> of type <go type>`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DecodeError(pub String);

impl std::fmt::Display for DecodeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

fn type_name(v: &Value) -> &'static str {
    match v {
        Value::Null => "null",
        Value::Bool(_) => "bool",
        Value::Number(_) => "number",
        Value::String(_) => "string",
        Value::Array(_) => "array",
        Value::Object(_) => "object",
    }
}

fn type_error(v: &Value, path: &str, go_type: &str) -> DecodeError {
    DecodeError(format!(
        "json: cannot unmarshal {} into Go struct field {} of type {}",
        type_name(v),
        path,
        go_type
    ))
}

fn fold_eq(a: &str, b: &str) -> bool {
    if a.len() == b.len() && a.eq_ignore_ascii_case(b) {
        return true;
    }
    let mut ai = a.chars().flat_map(char::to_lowercase);
    let mut bi = b.chars().flat_map(char::to_lowercase);
    loop {
        match (ai.next(), bi.next()) {
            (None, None) => return true,
            (Some(x), Some(y)) if x == y => continue,
            _ => return false,
        }
    }
}

/// A JSON object viewed as a Go struct with path information for errors.
#[derive(Clone, Copy)]
pub struct GoObject<'a> {
    map: &'a Map<String, Value>,
    path: &'a str,
}

impl<'a> GoObject<'a> {
    pub fn new(map: &'a Map<String, Value>, path: &'a str) -> Self {
        Self { map, path }
    }

    /// Field lookup with Go's matching rules. `null` is reported as absent.
    pub fn get(&self, tag: &str) -> Option<&'a Value> {
        let v = match self.map.get(tag) {
            Some(v) => Some(v),
            None => self
                .map
                .iter()
                .find(|(k, _)| fold_eq(k, tag))
                .map(|(_, v)| v),
        };
        v.filter(|v| !v.is_null())
    }

    fn field_path(&self, tag: &str) -> String {
        format!("{}.{}", self.path, tag)
    }

    pub fn string(&self, tag: &str) -> Result<String, DecodeError> {
        match self.get(tag) {
            None => Ok(String::new()),
            Some(Value::String(s)) => Ok(s.clone()),
            Some(v) => Err(type_error(v, &self.field_path(tag), "string")),
        }
    }

    pub fn bool(&self, tag: &str) -> Result<bool, DecodeError> {
        Ok(self.opt_bool(tag)?.unwrap_or(false))
    }

    /// `*bool` field: absent or `null` → `None`.
    pub fn opt_bool(&self, tag: &str) -> Result<Option<bool>, DecodeError> {
        match self.get(tag) {
            None => Ok(None),
            Some(Value::Bool(b)) => Ok(Some(*b)),
            Some(v) => Err(type_error(v, &self.field_path(tag), "bool")),
        }
    }

    /// `[]string`; `null` elements decode to `""` like Go.
    pub fn string_list(&self, tag: &str) -> Result<Vec<String>, DecodeError> {
        match self.get(tag) {
            None => Ok(Vec::new()),
            Some(Value::Array(items)) => items
                .iter()
                .map(|item| match item {
                    Value::Null => Ok(String::new()),
                    Value::String(s) => Ok(s.clone()),
                    v => Err(type_error(v, &self.field_path(tag), "string")),
                })
                .collect(),
            Some(v) => Err(type_error(v, &self.field_path(tag), "[]string")),
        }
    }

    /// `map[string]string`; `null` values decode to `""`.
    pub fn string_map(&self, tag: &str) -> Result<Vec<(String, String)>, DecodeError> {
        match self.get(tag) {
            None => Ok(Vec::new()),
            Some(Value::Object(m)) => m
                .iter()
                .map(|(k, v)| match v {
                    Value::Null => Ok((k.clone(), String::new())),
                    Value::String(s) => Ok((k.clone(), s.clone())),
                    v => Err(type_error(v, &self.field_path(tag), "string")),
                })
                .collect(),
            Some(v) => Err(type_error(v, &self.field_path(tag), "map[string]string")),
        }
    }

    /// Nested struct pointer; `None` when absent or `null`.
    pub fn object(
        &self,
        tag: &str,
        go_type: &str,
    ) -> Result<Option<&'a Map<String, Value>>, DecodeError> {
        match self.get(tag) {
            None => Ok(None),
            Some(Value::Object(m)) => Ok(Some(m)),
            Some(v) => Err(type_error(v, &self.field_path(tag), go_type)),
        }
    }

    /// `jwt.NumericDate`: a JSON number of (possibly fractional) unix seconds.
    pub fn numeric_date(&self, tag: &str) -> Result<Option<f64>, DecodeError> {
        match self.get(tag) {
            None => Ok(None),
            Some(Value::Number(n)) => n.as_f64().map(Some).ok_or_else(|| {
                DecodeError(format!("json: invalid number for {}", self.field_path(tag)))
            }),
            Some(v) => Err(type_error(v, &self.field_path(tag), "*jwt.NumericDate")),
        }
    }

    /// `jwt.ClaimStrings`: a string or an array of strings.
    pub fn claim_strings(&self, tag: &str) -> Result<Vec<String>, DecodeError> {
        match self.get(tag) {
            None => Ok(Vec::new()),
            Some(Value::String(s)) => Ok(vec![s.clone()]),
            Some(Value::Array(items)) => items
                .iter()
                .map(|item| match item {
                    Value::String(s) => Ok(s.clone()),
                    _ => Err(DecodeError(
                        "token has invalid claims: invalid type for claim".into(),
                    )),
                })
                .collect(),
            Some(_) => Err(DecodeError(
                "token has invalid claims: invalid type for claim".into(),
            )),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn case_insensitive_lookup() {
        let v = json!({"RoomJoin": true, "room_admin": true, "canpublish": false});
        let m = v.as_object().unwrap();
        let o = GoObject::new(m, "video");
        assert!(o.bool("roomJoin").unwrap());
        assert!(!o.bool("roomAdmin").unwrap(), "snake_case must not match");
        assert_eq!(o.opt_bool("canPublish").unwrap(), Some(false));
        assert_eq!(o.opt_bool("canSubscribe").unwrap(), None);
    }

    #[test]
    fn exact_key_preferred() {
        let v = json!({"ROOM": "a", "room": "b"});
        let o = GoObject::new(v.as_object().unwrap(), "video");
        assert_eq!(o.string("room").unwrap(), "b");
    }

    #[test]
    fn null_and_type_errors() {
        let v = json!({"canPublish": null, "roomJoin": "yes", "attributes": {"a": null}});
        let o = GoObject::new(v.as_object().unwrap(), "x");
        assert_eq!(o.opt_bool("canPublish").unwrap(), None);
        let err = o.bool("roomJoin").unwrap_err();
        assert!(err.0.contains("cannot unmarshal string"), "{err}");
        assert_eq!(
            o.string_map("attributes").unwrap(),
            vec![("a".to_string(), String::new())]
        );
    }

    #[test]
    fn claim_strings_accepts_string_or_list() {
        let v = json!({"aud": "x", "AUD2": ["a", "b"]});
        let o = GoObject::new(v.as_object().unwrap(), "c");
        assert_eq!(o.claim_strings("aud").unwrap(), vec!["x"]);
        assert_eq!(o.claim_strings("aud2").unwrap(), vec!["a", "b"]);
    }
}
