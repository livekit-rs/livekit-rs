//! A faithful yaml.v3 (gopkg.in/yaml.v3 v3.0.1) decoder for the upstream
//! configuration surfaces: `yaml.NewDecoder(r).Decode` (config files and
//! bodies, key files) and `yaml.Unmarshal` (`--keys`).
//!
//! Layers, each a port of the matching upstream file:
//! - [`parser`]: libyaml reader/scanner/parser (`readerc.go`, `scannerc.go`,
//!   `parserc.go`) producing events with marks and exact syntax errors;
//! - [`node`]: the `Node` tree builder (`decode.go` `parser`);
//! - [`resolve`]: plain-scalar resolution (`resolve.go`);
//! - [`decode`]: the reflective decoder (`decode.go` `decoder`), expressed as
//!   the [`Decode`] trait;
//! - [`serde_bridge`]: a serde view of a node for non-upstream extensions.

mod decode;
mod node;
pub(crate) mod parser;
#[cfg(test)]
mod parser_api_tests;
#[cfg(test)]
mod parser_differential_tests;
mod resolve;
pub(crate) mod serde_bridge;

pub(crate) use decode::PANIC_PREFIX;
#[cfg(test)]
pub(crate) use decode::map_parts;
pub(crate) use decode::{
    Any, Dec, Decode, GoType, MapEntries, NilSlice, StructFields, decode_map, decode_pointer,
    decode_struct, go_pointer,
};
pub(crate) use node::{Doc, Id, Kind, Node, NodeParser};

/// `yaml.Style` bits of [`Node::style`].
pub(crate) mod node_style {
    pub(crate) use super::node::{DOUBLE_QUOTED, FLOW, FOLDED, LITERAL, SINGLE_QUOTED, TAGGED};
}

/// The `fail`/`failf` path: the complete `err.Error()` text.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct Fail(pub String);

impl From<Fail> for String {
    fn from(f: Fail) -> String {
        f.0
    }
}

fn type_error(terrors: &[String]) -> String {
    format!("yaml: unmarshal errors:\n  {}", terrors.join("\n  "))
}

/// `yaml.NewDecoder(bytes.NewReader(input)).KnownFields(known).Decode(out)`:
/// `Err("EOF")` when the stream holds no document.
pub(crate) fn decoder_decode<T: Decode + ?Sized>(
    input: &[u8],
    known_fields: bool,
    out: &mut T,
    go: &str,
) -> Result<(), String> {
    let mut parser = NodeParser::new_reader(input);
    let Some((doc, root)) = parser.parse_document()? else {
        return Err("EOF".to_string());
    };
    decode_root(&doc, root, known_fields, out, go)
}

/// `yaml.Unmarshal(input, out)`: an empty stream is not an error.
pub(crate) fn unmarshal<T: Decode + ?Sized>(
    input: &[u8],
    out: &mut T,
    go: &str,
) -> Result<(), String> {
    let mut parser = NodeParser::new_bytes(input);
    let Some((doc, root)) = parser.parse_document()? else {
        return Ok(());
    };
    decode_root(&doc, root, false, out, go)
}

/// `(*Node).Decode`-style decode of one node of `doc` (fresh decoder).
pub(crate) fn decode_root<T: Decode + ?Sized>(
    doc: &Doc,
    root: Id,
    known_fields: bool,
    out: &mut T,
    go: &str,
) -> Result<(), String> {
    let mut d = Dec::new(doc, known_fields);
    d.unmarshal(root, out, go)?;
    if d.terrors.is_empty() {
        Ok(())
    } else {
        Err(type_error(&d.terrors))
    }
}
