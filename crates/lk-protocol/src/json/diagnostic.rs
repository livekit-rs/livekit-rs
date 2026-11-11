// Source-informed behavior: Google protobuf Go decoder, Copyright 2018 The Go Authors.
// SPDX-License-Identifier: BSD-3-Clause
// License: LICENSES/google-protobuf-BSD-3-Clause.txt. Original source identity is in SOURCE.json.
//! Source-spanned diagnostics for descriptor ProtoJSON. The ordinary decoder
//! remains authoritative for values; this pass checks fields in encounter order.
//! No freeform error text is parsed or classified.
use std::collections::BTreeSet;
use std::fmt;
use std::ops::Range;

use prost_reflect::{FieldDescriptor, Kind, MessageDescriptor};

/// Source-supported protobuf prefix variants. Go chooses once per executable.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Prefix {
    Space,
    NonBreakingSpace,
}

/// Stable structured reason, independent of its public rendering.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Cause {
    DuplicateField,
    DuplicateMapKey,
    InvalidScalar { kind: &'static str, field: String },
    InvalidMapKey { kind: &'static str },
    UnexpectedToken,
    OneofAlreadySet { name: String },
}

/// Original UTF-8 token span and Google-compatible character position.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Diagnostic {
    pub cause: Cause,
    pub span: Range<usize>,
    pub line: usize,
    pub column: usize,
    pub raw_token: String,
}
impl Diagnostic {
    pub fn render(&self, prefix: Prefix) -> String {
        let separator = match prefix {
            Prefix::Space => ' ',
            Prefix::NonBreakingSpace => '\u{a0}',
        };
        let detail = match &self.cause {
            Cause::DuplicateField => format!("duplicate field {}", self.raw_token),
            Cause::DuplicateMapKey => format!("duplicate map key {}", self.raw_token),
            Cause::InvalidScalar { kind, field } => {
                format!("invalid value for {kind} field {field}: {}", self.raw_token)
            }
            Cause::InvalidMapKey { kind } => {
                format!("invalid value for {kind} key: {}", self.raw_token)
            }
            Cause::UnexpectedToken => format!("unexpected token {}", self.raw_token),
            Cause::OneofAlreadySet { name } => format!(
                "error parsing {}, oneof {name} is already set",
                self.raw_token
            ),
        };
        let syntax = if self.cause == Cause::UnexpectedToken {
            "syntax error "
        } else {
            ""
        };
        format!(
            "proto:{separator}{syntax}(line {}:{}): {detail}",
            self.line, self.column
        )
    }
}
impl fmt::Display for Diagnostic {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.render(Prefix::Space))
    }
}

/// Unsupported diagnostic families retain their actual decoder error. They are
/// not guessed from strings and are not advertised as source-exact rendering.
#[derive(Debug)]
pub enum Error {
    Source(Diagnostic),
    Decoder(serde_json::Error),
}
impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Source(e) => e.fmt(f),
            Self::Decoder(e) => e.fmt(f),
        }
    }
}
impl std::error::Error for Error {}
impl From<serde_json::Error> for Error {
    fn from(error: serde_json::Error) -> Self {
        Self::Decoder(error)
    }
}

type Result<T> = std::result::Result<T, Error>;
#[derive(Debug)]
struct Node {
    span: Range<usize>,
}
#[derive(Debug)]
struct Member {
    name: String,
    key: Range<usize>,
    value: Node,
}

// JSON syntax/string validation happens before this bounded source cursor. The
// cursor only records spans; serde still decodes key strings and scalar values.
struct Cursor<'a> {
    source: &'a str,
    at: usize,
}
impl Cursor<'_> {
    fn whitespace(&mut self) {
        while self
            .source
            .as_bytes()
            .get(self.at)
            .is_some_and(u8::is_ascii_whitespace)
        {
            self.at += 1;
        }
    }
    fn string(&mut self) -> Range<usize> {
        let start = self.at;
        self.at += 1;
        while self.at < self.source.len() {
            match self.source.as_bytes()[self.at] {
                b'\\' => self.at += 2,
                b'"' => {
                    self.at += 1;
                    break;
                }
                _ => self.at += 1,
            }
        }
        start..self.at
    }
    fn node(&mut self) -> Node {
        self.whitespace();
        let start = self.at;
        match self.source.as_bytes().get(self.at) {
            Some(b'"') => {
                self.string();
            }
            Some(b'{' | b'[') => {
                let mut nesting = 0usize;
                while self.at < self.source.len() {
                    match self.source.as_bytes()[self.at] {
                        b'"' => {
                            self.string();
                            continue;
                        }
                        b'{' | b'[' => nesting += 1,
                        b'}' | b']' => {
                            nesting -= 1;
                            if nesting == 0 {
                                self.at += 1;
                                break;
                            }
                        }
                        _ => {}
                    }
                    self.at += 1;
                }
            }
            _ => {
                while self
                    .source
                    .as_bytes()
                    .get(self.at)
                    .is_some_and(|b| !b.is_ascii_whitespace() && !b",]}".contains(b))
                {
                    self.at += 1;
                }
            }
        }
        Node {
            span: start..self.at,
        }
    }
}

struct Context<'a> {
    source: &'a str,
}
impl Context<'_> {
    fn raw(&self, node: &Node) -> &str {
        &self.source[node.span.clone()]
    }
    fn fail<T>(&self, cause: Cause, span: Range<usize>) -> Result<T> {
        let before = &self.source[..span.start];
        let line = before.bytes().filter(|b| *b == b'\n').count() + 1;
        let column = before.rsplit('\n').next().unwrap_or("").chars().count() + 1;
        // A container's opening delimiter is the token read by Google's lexer.
        let span = if self.source[span.clone()].starts_with(['{', '[']) {
            span.start..span.start + 1
        } else {
            span
        };
        let raw_token = self.source[span.clone()].to_owned();
        Err(Error::Source(Diagnostic {
            cause,
            span,
            line,
            column,
            raw_token,
        }))
    }
    fn object(&self, node: &Node) -> Result<Vec<Member>> {
        if !self.raw(node).starts_with('{') {
            return self.fail(Cause::UnexpectedToken, node.span.clone());
        }
        let mut cursor = Cursor {
            source: self.source,
            at: node.span.start + 1,
        };
        cursor.whitespace();
        let mut members = Vec::new();
        while self.source.as_bytes().get(cursor.at) != Some(&b'}') {
            let key = cursor.string();
            let name = serde_json::from_str(&self.source[key.clone()])?;
            cursor.whitespace();
            cursor.at += 1;
            let value = cursor.node();
            members.push(Member { name, key, value });
            cursor.whitespace();
            if self.source.as_bytes().get(cursor.at) != Some(&b',') {
                break;
            }
            cursor.at += 1;
            cursor.whitespace();
        }
        Ok(members)
    }
    fn array(&self, node: &Node) -> Result<Vec<Node>> {
        if !self.raw(node).starts_with('[') {
            return self.fail(Cause::UnexpectedToken, node.span.clone());
        }
        let mut cursor = Cursor {
            source: self.source,
            at: node.span.start + 1,
        };
        cursor.whitespace();
        let mut nodes = Vec::new();
        while self.source.as_bytes().get(cursor.at) != Some(&b']') {
            nodes.push(cursor.node());
            cursor.whitespace();
            if self.source.as_bytes().get(cursor.at) != Some(&b',') {
                break;
            }
            cursor.at += 1;
            cursor.whitespace();
        }
        Ok(nodes)
    }
    fn message(&self, desc: &MessageDescriptor, node: &Node, depth: usize) -> Result<()> {
        if depth > 100 {
            return Err(super::error("message recursion limit").into());
        }
        // WKT error families are intentionally delegated, never mislabeled as
        // ordinary-object errors. Their existing admission behavior is retained.
        if desc.full_name().starts_with("google.protobuf.") {
            let normalized = super::normalize_message(desc, self.raw(node), depth)?;
            super::builder::message(desc.clone(), &normalized, depth)?;
            return Ok(());
        }
        let mut fields_seen = BTreeSet::new();
        let mut oneofs_seen = BTreeSet::new();
        for member in self.object(node)? {
            let Some(field) = desc
                .get_field_by_json_name(&member.name)
                .or_else(|| desc.get_field_by_name(&member.name))
            else {
                continue;
            };
            if !fields_seen.insert(field.number()) {
                return self.fail(Cause::DuplicateField, member.key.clone());
            }
            let raw = self.raw(&member.value);
            if raw == "null" && !accepts_null(&field.kind()) {
                continue;
            }
            if !field.is_map() && !field.is_list() {
                if let Some(oneof) = field.containing_oneof() {
                    if !oneofs_seen.insert(oneof.full_name().to_owned()) {
                        return self.fail(
                            Cause::OneofAlreadySet {
                                name: oneof.full_name().to_owned(),
                            },
                            member.key.clone(),
                        );
                    }
                }
            }
            if field.is_map() {
                self.map(&field, &member.value, depth + 1)?;
            } else if field.is_list() {
                for value in self.array(&member.value)? {
                    self.value(&field, &value, depth + 1, true)?;
                }
            } else {
                self.value(&field, &member.value, depth + 1, false)?;
            }
        }
        Ok(())
    }
    fn value(
        &self,
        field: &FieldDescriptor,
        node: &Node,
        depth: usize,
        list_item: bool,
    ) -> Result<()> {
        if let Kind::Message(desc) = field.kind() {
            return self.message(&desc, node, depth);
        }
        let raw = self.raw(node);
        if raw == "null" && !accepts_null(&field.kind()) {
            return self.fail(
                Cause::InvalidScalar {
                    kind: kind_name(&field.kind()),
                    field: field.json_name().to_owned(),
                },
                node.span.clone(),
            );
        }
        // All actual scalar/range/precision checks reuse the same implementation
        // as normal decode. No float-to-integer conversion or error-text match.
        let attempt = (|| -> super::Result<()> {
            let normalized = super::normalize_kind(&field.kind(), raw, depth)?;
            let normalized = if list_item {
                format!("[{normalized}]")
            } else {
                normalized
            };
            let object = super::object([(field.json_name().to_owned(), normalized)])?;
            super::builder::message(field.parent_message().clone(), &object, depth)?;
            Ok(())
        })();
        if attempt.is_err() {
            return self.fail(
                Cause::InvalidScalar {
                    kind: kind_name(&field.kind()),
                    field: field.json_name().to_owned(),
                },
                node.span.clone(),
            );
        }
        Ok(())
    }
    fn map(&self, field: &FieldDescriptor, node: &Node, depth: usize) -> Result<()> {
        let Kind::Message(entry) = field.kind() else {
            return Err(super::error("invalid map descriptor").into());
        };
        let key_field = entry
            .get_field(1)
            .ok_or_else(|| super::error("missing map key"))?;
        let value_field = entry
            .get_field(2)
            .ok_or_else(|| super::error("missing map value"))?;
        let mut seen = std::collections::HashSet::new();
        for member in self.object(node)? {
            let key = match super::builder::map_key(&key_field.kind(), &member.name) {
                Ok(key) => key,
                Err(_) => {
                    return self.fail(
                        Cause::InvalidMapKey {
                            kind: kind_name(&key_field.kind()),
                        },
                        member.key.clone(),
                    );
                }
            };
            if seen.contains(&key) {
                return self.fail(Cause::DuplicateMapKey, member.key.clone());
            }
            self.value(&value_field, &member.value, depth, false)?;
            // Discarded future enum names do not populate the map, matching Go
            // mmap.Has rather than a blanket lexical-key set.
            if !super::builder::unknown_enum(&value_field.kind(), self.raw(&member.value))? {
                seen.insert(key);
            }
        }
        Ok(())
    }
}
fn accepts_null(kind: &Kind) -> bool {
    match kind {
        Kind::Message(desc) => desc.full_name() == "google.protobuf.Value",
        Kind::Enum(desc) => desc.full_name() == "google.protobuf.NullValue",
        _ => false,
    }
}
fn kind_name(kind: &Kind) -> &'static str {
    match kind {
        Kind::Double => "double",
        Kind::Float => "float",
        Kind::Int32 => "int32",
        Kind::Int64 => "int64",
        Kind::Uint32 => "uint32",
        Kind::Uint64 => "uint64",
        Kind::Sint32 => "sint32",
        Kind::Sint64 => "sint64",
        Kind::Fixed32 => "fixed32",
        Kind::Fixed64 => "fixed64",
        Kind::Sfixed32 => "sfixed32",
        Kind::Sfixed64 => "sfixed64",
        Kind::Bool => "bool",
        Kind::String => "string",
        Kind::Bytes => "bytes",
        Kind::Enum(_) => "enum",
        Kind::Message(_) => "message",
    }
}

/// Decode the same known message with typed source diagnostics for ordinary
/// fields. Other error families are retained as Decoder, not string-guessed.
pub fn decode<T: prost::Message + prost::Name + Default>(body: &[u8]) -> Result<T> {
    // Diagnostics cannot change admission or the returned typed value. This is
    // especially important for discarded unknown/WKT/future enum profiles.
    let original_error = match super::decode(body) {
        Ok(value) => return Ok(value),
        Err(error) => error,
    };
    let Ok(source) = std::str::from_utf8(body) else {
        return Err(Error::Decoder(original_error));
    };
    let mut input = serde_json::Deserializer::from_str(source);
    let Ok(raw) = <Box<serde_json::value::RawValue> as serde::Deserialize>::deserialize(&mut input)
    else {
        return Err(Error::Decoder(original_error));
    };
    if super::validate_string_tokens(raw.get()).is_err() {
        return Err(Error::Decoder(original_error));
    }
    let mut cursor = Cursor { source, at: 0 };
    let node = cursor.node();
    let Some(descriptor) = super::pool().get_message_by_name(&T::full_name()) else {
        return Err(Error::Decoder(original_error));
    };
    if let Err(Error::Source(diagnostic)) = (Context { source }).message(&descriptor, &node, 0) {
        return Err(Error::Source(diagnostic));
    }
    // A not-yet-structured family retains the exact original decoder error.
    Err(Error::Decoder(original_error))
}

#[cfg(test)]
mod tests;
