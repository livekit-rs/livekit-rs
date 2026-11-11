//! yaml.v3 `decoder` (`decode.go`): unmarshals a node tree onto typed values.
//!
//! Ported from gopkg.in/yaml.v3 v3.0.1 (MIT / Apache-2.0, Copyright (c)
//! 2011-2019 Canonical Ltd). Go reflection is replaced by the [`Decode`]
//! trait: every target type implements the `reflect.Kind` branch upstream
//! takes for it, and passes the Go type name (`out.Type()`) used in type
//! errors. Type errors are collected (`d.terrors`) and decoding continues;
//! [`Fail`] is the panic path (`fail`/`failf`) that aborts the decode.

use std::collections::{BTreeMap, HashSet};

use super::Fail;
use super::node::{Doc, Id, Kind};
use super::resolve::{BINARY_TAG, MAP_TAG, NULL_TAG, Resolved, SEQ_TAG, STR_TAG, resolve};
use crate::duration::{GoDuration, parse_go_duration};

/// A key recorded by merge handling (`mergedFields map[interface{}]bool`):
/// the dynamic Go type and the value, so `int32(1)` and `int(1)` differ.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(crate) struct MergeKey(pub &'static str, pub String);

pub(crate) struct Dec<'a> {
    pub doc: &'a Doc,
    pub known_fields: bool,
    pub terrors: Vec<String>,
    aliases: HashSet<Id>,
    decode_count: usize,
    alias_count: usize,
    alias_depth: usize,
    pub merged_fields: Option<HashSet<MergeKey>>,
}

fn allowed_alias_ratio(decode_count: usize) -> f64 {
    const LOW: usize = 400_000;
    const HIGH: usize = 4_000_000;
    if decode_count <= LOW {
        0.99
    } else if decode_count >= HIGH {
        0.10
    } else {
        0.99 - 0.89 * ((decode_count - LOW) as f64 / (HIGH - LOW) as f64)
    }
}

/// A Go type's static name as printed by `reflect.Type.String()`.
pub(crate) trait GoType {
    fn go_type() -> String;
}

/// One decodable Go value (`d.unmarshal(n, out)` after alias dispatch).
pub(crate) trait Decode {
    /// `go` is `out.Type().String()` for this value; it names the type in
    /// `cannot unmarshal ... into <go>` and lets containers name elements.
    fn decode(&mut self, d: &mut Dec<'_>, n: Id, go: &str) -> Result<bool, Fail>;
}

impl<'a> Dec<'a> {
    pub fn new(doc: &'a Doc, known_fields: bool) -> Dec<'a> {
        Dec {
            doc,
            known_fields,
            terrors: Vec::new(),
            aliases: HashSet::new(),
            decode_count: 0,
            alias_count: 0,
            alias_depth: 0,
            merged_fields: None,
        }
    }

    /// `d.unmarshal`.
    pub fn unmarshal<T: Decode + ?Sized>(
        &mut self,
        n: Id,
        out: &mut T,
        go: &str,
    ) -> Result<bool, Fail> {
        self.decode_count += 1;
        if self.alias_depth > 0 {
            self.alias_count += 1;
        }
        if self.alias_count > 100
            && self.decode_count > 1000
            && self.alias_count as f64 / self.decode_count as f64
                > allowed_alias_ratio(self.decode_count)
        {
            return Err(Fail("yaml: document contains excessive aliasing".into()));
        }
        let node = self.doc.node(n);
        match node.kind {
            Kind::Document => {
                if node.content.len() == 1 {
                    let child = node.content[0];
                    self.unmarshal(child, out, go)?;
                    Ok(true)
                } else {
                    Ok(false)
                }
            }
            Kind::Alias => {
                if self.aliases.contains(&n) {
                    return Err(Fail(format!(
                        "yaml: anchor '{}' value contains itself",
                        node.value
                    )));
                }
                let target = node.alias.expect("parser resolves every alias");
                self.aliases.insert(n);
                self.alias_depth += 1;
                let good = self.unmarshal(target, out, go);
                self.alias_depth -= 1;
                self.aliases.remove(&n);
                good
            }
            _ => out.decode(self, n, go),
        }
    }

    pub fn kind(&self, n: Id) -> Kind {
        self.doc.node(n).kind
    }

    pub fn value(&self, n: Id) -> &'a str {
        &self.doc.node(n).value
    }

    pub fn line(&self, n: Id) -> usize {
        self.doc.node(n).line
    }

    pub fn content(&self, n: Id) -> &'a [Id] {
        &self.doc.node(n).content
    }

    pub fn is_null(&self, n: Id) -> bool {
        self.doc.short_tag(n) == NULL_TAG
    }

    /// `d.terror`.
    pub fn terror(&mut self, n: Id, tag: &str, go: &str) {
        let node = self.doc.node(n);
        let tag = if node.tag.is_empty() {
            tag.to_string()
        } else {
            node.tag.clone()
        };
        let mut value = String::new();
        if tag != SEQ_TAG && tag != MAP_TAG {
            let v = &node.value;
            // Go slices bytes: value[:7] of a multi-byte string may split a
            // rune; the bytes are printed as-is by %s.
            if v.len() > 10 {
                value = format!(" `{}...`", lossy_prefix(v, 7));
            } else {
                value = format!(" `{v}`");
            }
        }
        self.terrors.push(format!(
            "line {}: cannot unmarshal {}{value} into {go}",
            node.line,
            super::resolve::short_tag(&tag)
        ));
    }

    /// The resolved value of a scalar (`d.scalar`'s first half).
    pub fn scalar(&self, n: Id) -> Result<(String, Resolved), Fail> {
        let node = self.doc.node(n);
        if let Some(bits) = node.float_bits {
            return Ok((
                super::resolve::FLOAT_TAG.into(),
                Resolved::Float(f64::from_bits(bits)),
            ));
        }
        if self.doc.indicated_string(n) {
            return Ok((STR_TAG.into(), Resolved::Str(node.value.clone())));
        }
        let (tag, resolved) = resolve(&node.tag, &node.value)?;
        if tag == BINARY_TAG {
            let text = match resolved {
                Resolved::Str(s) => s,
                _ => node.value.clone(),
            };
            return Ok((tag, Resolved::Str(decode_base64(&text)?)));
        }
        Ok((tag, resolved))
    }

    /// The unique-key check that opens `d.mapping`; `false` when it failed.
    pub fn check_unique(&mut self, n: Id) -> bool {
        let content = self.content(n);
        let before = self.terrors.len();
        let mut i = 0;
        while i < content.len() {
            let ni = self.doc.node(content[i]);
            let mut j = i + 2;
            while j < content.len() {
                let nj = self.doc.node(content[j]);
                if ni.kind == nj.kind && ni.value == nj.value {
                    self.terrors.push(format!(
                        "line {}: mapping key {} already defined at line {}",
                        nj.line,
                        crate::strconv::go_quote(&nj.value),
                        ni.line
                    ));
                }
                j += 2;
            }
            i += 2;
        }
        self.terrors.len() == before
    }

    /// The non-scalar branches for a target whose Go kind accepts only
    /// scalars: a mapping or sequence is a type error.
    pub fn not_a_collection(&mut self, n: Id, go: &str) -> bool {
        match self.kind(n) {
            Kind::Mapping => {
                if self.check_unique(n) {
                    self.terror(n, MAP_TAG, go);
                }
                false
            }
            Kind::Sequence => {
                self.terror(n, SEQ_TAG, go);
                false
            }
            _ => unreachable!("scalar handled by the caller"),
        }
    }

    /// The `interface{}` value of a key, as `d.merge` records parent keys.
    fn iface_key(&mut self, n: Id) -> Result<Option<MergeKey>, Fail> {
        let mut any = Any::Null;
        if !self.unmarshal(n, &mut any, "interface {}")? {
            return Ok(None);
        }
        Ok(Some(any.merge_key()))
    }

    /// `d.merge(parent, merge, out)`.
    pub fn merge<T: Decode + ?Sized>(
        &mut self,
        parent: Id,
        merge: Id,
        out: &mut T,
        go: &str,
    ) -> Result<(), Fail> {
        let saved = self.merged_fields.clone();
        if saved.is_none() {
            let mut keys = HashSet::new();
            let content = self.content(parent);
            let mut i = 0;
            while i < content.len() {
                if let Some(k) = self.iface_key(content[i])? {
                    keys.insert(k);
                }
                i += 2;
            }
            self.merged_fields = Some(keys);
        }
        let want_map =
            || Fail("yaml: map merge requires map or sequence of maps as the value".into());
        match self.kind(merge) {
            Kind::Mapping => {
                self.unmarshal(merge, out, go)?;
            }
            Kind::Alias => {
                let target = self.doc.node(merge).alias;
                if target.is_some_and(|t| self.kind(t) != Kind::Mapping) {
                    return Err(want_map());
                }
                self.unmarshal(merge, out, go)?;
            }
            Kind::Sequence => {
                for &ni in self.content(merge) {
                    if self.kind(ni) == Kind::Alias {
                        let target = self.doc.node(ni).alias;
                        if target.is_some_and(|t| self.kind(t) != Kind::Mapping) {
                            return Err(want_map());
                        }
                    } else if self.kind(ni) != Kind::Mapping {
                        return Err(want_map());
                    }
                    self.unmarshal(ni, out, go)?;
                }
            }
            _ => return Err(want_map()),
        }
        self.merged_fields = saved;
        Ok(())
    }

    /// `mergedFields` bookkeeping for one key: `false` = already merged.
    pub fn first_merge(merged: &mut Option<HashSet<MergeKey>>, key: MergeKey) -> bool {
        match merged {
            Some(set) => set.insert(key),
            None => true,
        }
    }
}

fn lossy_prefix(s: &str, n: usize) -> String {
    String::from_utf8_lossy(&s.as_bytes()[..n]).into_owned()
}

/// Go `base64.StdEncoding.DecodeString` (which skips `\r` and `\n`).
fn decode_base64(text: &str) -> Result<String, Fail> {
    use base64::Engine as _;
    let clean: String = text.chars().filter(|c| *c != '\r' && *c != '\n').collect();
    base64::engine::general_purpose::STANDARD
        .decode(clean.as_bytes())
        .map(|b| String::from_utf8_lossy(&b).into_owned())
        .map_err(|_| Fail("yaml: !!binary value contains invalid base64 data".into()))
}

// ---------------------------------------------------------------------------
// Struct decoding (`d.mappingStruct`)
// ---------------------------------------------------------------------------

/// Implemented by every config struct (`yaml_struct!`).
/// A [`Fail`] text starting with this is a Go runtime panic (exit status 2),
/// not a returned error.
pub(crate) const PANIC_PREFIX: &str = "panic: ";

pub(crate) trait StructFields {
    /// Index of the field named `key` (yaml.v3 `sinfo.FieldsMap`).
    fn field_index(key: &str) -> Option<usize>;
    /// The field belongs to an embedded `yaml:",inline"` struct (`info.Inline`).
    fn is_inline(_index: usize) -> bool {
        false
    }
    /// Decodes field `index` from node `value` (`d.unmarshal(value, field)`).
    fn decode_field(&mut self, d: &mut Dec<'_>, index: usize, value: Id) -> Result<(), Fail>;
}

/// A struct value (`reflect.Struct`).
pub(crate) fn decode_struct<T: StructFields + Decode>(
    out: &mut T,
    d: &mut Dec<'_>,
    n: Id,
    go: &str,
) -> Result<bool, Fail> {
    match d.kind(n) {
        Kind::Scalar => {
            let (tag, resolved) = d.scalar(n)?;
            if resolved == Resolved::Null {
                return Ok(false);
            }
            d.terror(n, &tag, go);
            Ok(false)
        }
        Kind::Sequence => {
            d.terror(n, SEQ_TAG, go);
            Ok(false)
        }
        _ => {
            if !d.check_unique(n) {
                return Ok(false);
            }
            mapping_struct(out, d, n, go)
        }
    }
}

fn mapping_struct<T: StructFields + Decode>(
    out: &mut T,
    d: &mut Dec<'_>,
    n: Id,
    go: &str,
) -> Result<bool, Fail> {
    let mut merged = d.merged_fields.take();
    let mut merge_node = None;
    let mut done = HashSet::new();
    let content = d.content(n);
    let mut i = 0;
    while i < content.len() {
        let ni = content[i];
        let value = content[i + 1];
        i += 2;
        if d.doc.is_merge(ni) {
            merge_node = Some(value);
            continue;
        }
        let mut name = String::new();
        if !d.unmarshal(ni, &mut name, "string")? {
            continue;
        }
        if !Dec::first_merge(&mut merged, MergeKey("string", name.clone())) {
            continue;
        }
        if let Some(index) = T::field_index(&name) {
            if !done.insert(index) {
                d.terrors.push(format!(
                    "line {}: field {name} already set in type {go}",
                    d.line(ni)
                ));
                continue;
            }
            if T::is_inline(index) && d.is_null(n) {
                // fieldByIndex returns the zero Value for a null-tagged
                // mapping; unmarshal then panics in reflect (process crash).
                return Err(Fail(format!(
                    "{PANIC_PREFIX}reflect: call of reflect.Value.Type on zero Value"
                )));
            }
            out.decode_field(d, index, value)?;
        } else if d.known_fields {
            d.terrors.push(format!(
                "line {}: field {name} not found in type {go}",
                d.line(ni)
            ));
        }
    }
    d.merged_fields = merged;
    if let Some(m) = merge_node {
        d.merge(n, m, out, go)?;
    }
    Ok(true)
}

// ---------------------------------------------------------------------------
// Scalars
// ---------------------------------------------------------------------------

fn overflow_int(v: i64, bits: u32) -> bool {
    if bits >= 64 {
        return false;
    }
    let min = -(1i64 << (bits - 1));
    let max = (1i64 << (bits - 1)) - 1;
    v < min || v > max
}

fn overflow_uint(v: u64, bits: u32) -> bool {
    bits < 64 && v > (1u64 << bits) - 1
}

/// The `reflect.Int*` branch of `d.scalar`.
fn scalar_int(
    d: &mut Dec<'_>,
    n: Id,
    go: &str,
    bits: u32,
    duration: bool,
) -> Result<Option<i64>, Fail> {
    if d.kind(n) != Kind::Scalar {
        d.not_a_collection(n, go);
        return Ok(None);
    }
    let (tag, resolved) = d.scalar(n)?;
    let set = match &resolved {
        Resolved::Null => return Ok(None),
        Resolved::Int(v) if !duration && !overflow_int(*v, bits) => Some(*v),
        Resolved::Uint(v)
            if !duration && *v <= i64::MAX as u64 && !overflow_int(*v as i64, bits) =>
        {
            Some(*v as i64)
        }
        // int64(f) saturates like the arm64/Rust conversion.
        Resolved::Float(f)
            if !duration && *f <= i64::MAX as f64 && !overflow_int(*f as i64, bits) =>
        {
            Some(*f as i64)
        }
        Resolved::Str(s) if duration => parse_go_duration(s).ok().map(|g| g.0),
        _ => None,
    };
    if set.is_none() {
        d.terror(n, &tag, go);
    }
    Ok(set)
}

fn scalar_uint(d: &mut Dec<'_>, n: Id, go: &str, bits: u32) -> Result<Option<u64>, Fail> {
    if d.kind(n) != Kind::Scalar {
        d.not_a_collection(n, go);
        return Ok(None);
    }
    let (tag, resolved) = d.scalar(n)?;
    let set = match &resolved {
        Resolved::Null => return Ok(None),
        Resolved::Int(v) if *v >= 0 && !overflow_uint(*v as u64, bits) => Some(*v as u64),
        Resolved::Uint(v) if !overflow_uint(*v, bits) => Some(*v),
        Resolved::Float(f) if *f <= u64::MAX as f64 && !overflow_uint(*f as u64, bits) => {
            Some(*f as u64)
        }
        _ => None,
    };
    if set.is_none() {
        d.terror(n, &tag, go);
    }
    Ok(set)
}

fn scalar_float(d: &mut Dec<'_>, n: Id, go: &str) -> Result<Option<f64>, Fail> {
    if d.kind(n) != Kind::Scalar {
        d.not_a_collection(n, go);
        return Ok(None);
    }
    let (tag, resolved) = d.scalar(n)?;
    let set = match resolved {
        Resolved::Null => return Ok(None),
        Resolved::Int(v) => Some(v as f64),
        Resolved::Uint(v) => Some(v as f64),
        Resolved::Float(f) => Some(f),
        _ => None,
    };
    if set.is_none() {
        d.terror(n, &tag, go);
    }
    Ok(set)
}

macro_rules! int_decode {
    ($($t:ty => $go:literal, $bits:expr;)*) => {$(
        impl GoType for $t {
            fn go_type() -> String {
                $go.into()
            }
        }
        impl Decode for $t {
            fn decode(&mut self, d: &mut Dec<'_>, n: Id, go: &str) -> Result<bool, Fail> {
                match scalar_int(d, n, go, $bits, false)? {
                    Some(v) => {
                        *self = v as $t;
                        Ok(true)
                    }
                    None => Ok(false),
                }
            }
        }
    )*};
}

int_decode! {
    i8 => "int8", 8;
    i16 => "int16", 16;
    i32 => "int32", 32;
    // Go `int` (64-bit); `int64` fields name their type explicitly.
    i64 => "int", 64;
}

macro_rules! uint_decode {
    ($($t:ty => $go:literal, $bits:expr;)*) => {$(
        impl GoType for $t {
            fn go_type() -> String {
                $go.into()
            }
        }
        impl Decode for $t {
            fn decode(&mut self, d: &mut Dec<'_>, n: Id, go: &str) -> Result<bool, Fail> {
                match scalar_uint(d, n, go, $bits)? {
                    Some(v) => {
                        *self = v as $t;
                        Ok(true)
                    }
                    None => Ok(false),
                }
            }
        }
    )*};
}

uint_decode! {
    u8 => "uint8", 8;
    u16 => "uint16", 16;
    u32 => "uint32", 32;
    u64 => "uint64", 64;
    usize => "uint", 64;
}

impl GoType for f64 {
    fn go_type() -> String {
        "float64".into()
    }
}

impl Decode for f64 {
    fn decode(&mut self, d: &mut Dec<'_>, n: Id, go: &str) -> Result<bool, Fail> {
        match scalar_float(d, n, go)? {
            Some(v) => {
                *self = v;
                Ok(true)
            }
            None => Ok(false),
        }
    }
}

impl GoType for f32 {
    fn go_type() -> String {
        "float32".into()
    }
}

impl Decode for f32 {
    fn decode(&mut self, d: &mut Dec<'_>, n: Id, go: &str) -> Result<bool, Fail> {
        match scalar_float(d, n, go)? {
            Some(v) => {
                // reflect SetFloat on a float32: float32(x), round to nearest.
                *self = v as f32;
                Ok(true)
            }
            None => Ok(false),
        }
    }
}

impl GoType for GoDuration {
    fn go_type() -> String {
        "time.Duration".into()
    }
}

impl Decode for GoDuration {
    fn decode(&mut self, d: &mut Dec<'_>, n: Id, go: &str) -> Result<bool, Fail> {
        match scalar_int(d, n, go, 64, true)? {
            Some(v) => {
                self.0 = v;
                Ok(true)
            }
            None => Ok(false),
        }
    }
}

impl GoType for bool {
    fn go_type() -> String {
        "bool".into()
    }
}

impl Decode for bool {
    fn decode(&mut self, d: &mut Dec<'_>, n: Id, go: &str) -> Result<bool, Fail> {
        if d.kind(n) != Kind::Scalar {
            d.not_a_collection(n, go);
            return Ok(false);
        }
        let (tag, resolved) = d.scalar(n)?;
        let set = match &resolved {
            Resolved::Null => return Ok(false),
            Resolved::Bool(b) => Some(*b),
            // YAML 1.1 compatibility, only for typed bool targets.
            Resolved::Str(s) => match s.as_str() {
                "y" | "Y" | "yes" | "Yes" | "YES" | "on" | "On" | "ON" => Some(true),
                "n" | "N" | "no" | "No" | "NO" | "off" | "Off" | "OFF" => Some(false),
                _ => None,
            },
            _ => None,
        };
        match set {
            Some(b) => {
                *self = b;
                Ok(true)
            }
            None => {
                d.terror(n, &tag, go);
                Ok(false)
            }
        }
    }
}

impl GoType for String {
    fn go_type() -> String {
        "string".into()
    }
}

impl Decode for String {
    fn decode(&mut self, d: &mut Dec<'_>, n: Id, go: &str) -> Result<bool, Fail> {
        if d.kind(n) != Kind::Scalar {
            d.not_a_collection(n, go);
            return Ok(false);
        }
        let (tag, resolved) = d.scalar(n)?;
        match resolved {
            Resolved::Null => Ok(false),
            Resolved::Str(s) if tag == BINARY_TAG => {
                *self = s;
                Ok(true)
            }
            _ => {
                *self = d.value(n).to_string();
                Ok(true)
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Pointers, slices, maps
// ---------------------------------------------------------------------------

fn strip_pointer(go: &str) -> &str {
    go.strip_prefix('*').unwrap_or(go)
}

fn slice_elem(go: &str) -> &str {
    go.strip_prefix("[]").unwrap_or(go)
}

/// `map[K]V` -> (`K`, `V`).
pub(crate) fn map_parts(go: &str) -> (&str, &str) {
    let Some(rest) = go.strip_prefix("map[") else {
        return ("string", go);
    };
    let mut depth = 1usize;
    for (i, c) in rest.char_indices() {
        match c {
            '[' => depth += 1,
            ']' => {
                depth -= 1;
                if depth == 0 {
                    return (&rest[..i], &rest[i + 1..]);
                }
            }
            _ => {}
        }
    }
    ("string", go)
}

/// A Go pointer (`*T`): `null` resets to nil, anything else allocates (or
/// reuses) the value and decodes into it ([`decode_pointer`]).
impl<T: GoType> GoType for Option<T> {
    fn go_type() -> String {
        format!("*{}", T::go_type())
    }
}

/// `d.prepare` + `d.null` for a Go pointer `*T`.
pub(crate) fn decode_pointer<T: Decode + Default>(
    this: &mut Option<T>,
    d: &mut Dec<'_>,
    n: Id,
    go: &str,
) -> Result<bool, Fail> {
    let this_ = this;
    {
        if d.is_null(n) {
            // d.prepare returns early; null() then resets the pointer. A
            // null-tagged collection reaches mapping()/sequence() on the
            // pointer itself, which is a type error.
            return match d.kind(n) {
                Kind::Scalar => {
                    *this_ = None;
                    Ok(true)
                }
                _ => Ok(d.not_a_collection(n, go)),
            };
        }
        let mut value = this_.take().unwrap_or_default();
        // d.prepare dereferences in the same unmarshal call.
        let good = value.decode(d, n, strip_pointer(go))?;
        *this_ = Some(value);
        Ok(good)
    }
}

/// Plain Go pointers (`*bool`, `*int`, `*xtls.Config`, ...).
macro_rules! go_pointer {
    ($($t:ty),* $(,)?) => {$(
        impl $crate::yamlv3::Decode for Option<$t> {
            fn decode(
                &mut self,
                d: &mut $crate::yamlv3::Dec<'_>,
                n: $crate::yamlv3::Id,
                go: &str,
            ) -> Result<bool, $crate::yamlv3::Fail> {
                $crate::yamlv3::decode_pointer(self, d, n, go)
            }
        }
    )*};
}

pub(crate) use go_pointer;

go_pointer!(bool, i64);

impl<T: GoType> GoType for Vec<T> {
    fn go_type() -> String {
        format!("[]{}", T::go_type())
    }
}

/// `d.sequence` into a slice; `null` resets it (nil and empty slices are
/// indistinguishable here; see [`NilSlice`] where they are not).
impl<T: Decode + Default> Decode for Vec<T> {
    fn decode(&mut self, d: &mut Dec<'_>, n: Id, go: &str) -> Result<bool, Fail> {
        let mut nil = NilSlice(Some(std::mem::take(self)));
        let good = nil.decode(d, n, go)?;
        *self = nil.0.unwrap_or_default();
        Ok(good)
    }
}

/// A Go slice whose nil-ness is observable (`None` = nil).
#[derive(Clone, Debug, Default, PartialEq)]
pub struct NilSlice<T>(pub Option<Vec<T>>);

impl<T: Decode + Default> Decode for NilSlice<T> {
    fn decode(&mut self, d: &mut Dec<'_>, n: Id, go: &str) -> Result<bool, Fail> {
        match d.kind(n) {
            Kind::Scalar => {
                let (tag, resolved) = d.scalar(n)?;
                if resolved == Resolved::Null {
                    self.0 = None;
                    return Ok(true);
                }
                d.terror(n, &tag, go);
                Ok(false)
            }
            Kind::Mapping => {
                if d.check_unique(n) {
                    d.terror(n, MAP_TAG, go);
                }
                Ok(false)
            }
            _ => {
                let elem = slice_elem(go).to_string();
                let mut out = Vec::with_capacity(d.content(n).len());
                for &child in d.content(n) {
                    let mut e = T::default();
                    if d.unmarshal(child, &mut e, &elem)? {
                        out.push(e);
                    }
                }
                self.0 = Some(out);
                Ok(true)
            }
        }
    }
}

/// Values that can be map keys and merge-tracked.
pub(crate) trait MapKey: Decode + Default + Ord + Clone {
    fn merge_key(&self, go: &str) -> MergeKey;
}

impl MapKey for String {
    fn merge_key(&self, _go: &str) -> MergeKey {
        MergeKey("string", self.clone())
    }
}

impl MapKey for i32 {
    fn merge_key(&self, _go: &str) -> MergeKey {
        MergeKey("int32", self.to_string())
    }
}

impl<K: GoType, V: GoType> GoType for BTreeMap<K, V> {
    fn go_type() -> String {
        format!("map[{}]{}", K::go_type(), V::go_type())
    }
}

/// `d.mapping` into a typed map: entries merge into the existing map, every
/// value is decoded fresh, `null` resets the whole map.
impl<K: MapKey, V: Decode + Default> Decode for BTreeMap<K, V> {
    fn decode(&mut self, d: &mut Dec<'_>, n: Id, go: &str) -> Result<bool, Fail> {
        let mut entries = MapEntries::Sorted(self);
        decode_map(&mut entries, d, n, go)
    }
}

/// Map storage used by [`decode_map`].
pub(crate) enum MapEntries<'m, K, V> {
    Sorted(&'m mut BTreeMap<K, V>),
    /// Insertion-ordered (`keys:`; the first key signs refreshed tokens).
    Ordered(&'m mut Vec<(K, V)>),
}

impl<K: Ord, V> MapEntries<'_, K, V> {
    fn contains(&self, k: &K) -> bool {
        match self {
            MapEntries::Sorted(m) => m.contains_key(k),
            MapEntries::Ordered(v) => v.iter().any(|(ek, _)| ek == k),
        }
    }
    fn insert(&mut self, k: K, value: V) {
        match self {
            MapEntries::Sorted(m) => {
                m.insert(k, value);
            }
            MapEntries::Ordered(v) => match v.iter_mut().find(|(ek, _)| *ek == k) {
                Some(e) => e.1 = value,
                None => v.push((k, value)),
            },
        }
    }
    fn clear(&mut self) {
        match self {
            MapEntries::Sorted(m) => m.clear(),
            MapEntries::Ordered(v) => v.clear(),
        }
    }
}

pub(crate) fn decode_map<K: MapKey, V: Decode + Default>(
    out: &mut MapEntries<'_, K, V>,
    d: &mut Dec<'_>,
    n: Id,
    go: &str,
) -> Result<bool, Fail> {
    match d.kind(n) {
        Kind::Scalar => {
            let (tag, resolved) = d.scalar(n)?;
            if resolved == Resolved::Null {
                out.clear();
                return Ok(true);
            }
            d.terror(n, &tag, go);
            Ok(false)
        }
        Kind::Sequence => {
            d.terror(n, SEQ_TAG, go);
            Ok(false)
        }
        _ => {
            if !d.check_unique(n) {
                return Ok(false);
            }
            let (kgo, vgo) = map_parts(go);
            let (kgo, vgo) = (kgo.to_string(), vgo.to_string());
            let mut merged = d.merged_fields.take();
            let mut merge_node = None;
            let content = d.content(n);
            let mut i = 0;
            while i < content.len() {
                let (kn, vn) = (content[i], content[i + 1]);
                i += 2;
                if d.doc.is_merge(kn) {
                    merge_node = Some(vn);
                    continue;
                }
                let mut k = K::default();
                if !d.unmarshal(kn, &mut k, &kgo)? {
                    continue;
                }
                if !Dec::first_merge(&mut merged, k.merge_key(&kgo)) {
                    continue;
                }
                let mut e = V::default();
                let good = d.unmarshal(vn, &mut e, &vgo)?;
                if good || d.is_null(vn) && !out.contains(&k) {
                    out.insert(k, e);
                }
            }
            d.merged_fields = merged;
            if let Some(m) = merge_node {
                let mut proxy = MapProxy { out };
                d.merge(n, m, &mut proxy, go)?;
            }
            Ok(true)
        }
    }
}

/// Re-entry point so `d.merge` can decode further mappings into the same map.
struct MapProxy<'p, 'm, K, V> {
    out: &'p mut MapEntries<'m, K, V>,
}

impl<K: MapKey, V: Decode + Default> Decode for MapProxy<'_, '_, K, V> {
    fn decode(&mut self, d: &mut Dec<'_>, n: Id, go: &str) -> Result<bool, Fail> {
        decode_map(self.out, d, n, go)
    }
}

// ---------------------------------------------------------------------------
// interface{}
// ---------------------------------------------------------------------------

/// A decoded `interface{}` value.
#[derive(Clone, Debug, Default, PartialEq)]
pub(crate) enum Any {
    #[default]
    Null,
    Bool(bool),
    Int(i64),
    Uint(u64),
    Float(f64),
    Str(String),
    Timestamp(String),
    Seq(Vec<Any>),
    Map(Vec<(Any, Any)>),
}

impl Any {
    fn merge_key(&self) -> MergeKey {
        match self {
            Any::Null => MergeKey("nil", String::new()),
            Any::Bool(b) => MergeKey("bool", b.to_string()),
            Any::Int(v) => MergeKey("int", v.to_string()),
            Any::Uint(v) => MergeKey("uint64", v.to_string()),
            Any::Float(f) => MergeKey("float64", f.to_bits().to_string()),
            Any::Str(s) => MergeKey("string", s.clone()),
            Any::Timestamp(s) => MergeKey("time.Time", s.clone()),
            Any::Seq(_) | Any::Map(_) => MergeKey("unhashable", format!("{self:?}")),
        }
    }
}

impl Decode for Any {
    fn decode(&mut self, d: &mut Dec<'_>, n: Id, _go: &str) -> Result<bool, Fail> {
        match d.kind(n) {
            Kind::Scalar => {
                let (_, resolved) = d.scalar(n)?;
                *self = match resolved {
                    Resolved::Null => Any::Null,
                    Resolved::Bool(b) => Any::Bool(b),
                    Resolved::Int(v) => Any::Int(v),
                    Resolved::Uint(v) => Any::Uint(v),
                    Resolved::Float(f) => Any::Float(f),
                    Resolved::Str(s) => Any::Str(s),
                    Resolved::Timestamp => Any::Timestamp(d.value(n).to_string()),
                };
                Ok(true)
            }
            Kind::Sequence => {
                let mut out = Vec::new();
                for &child in d.content(n) {
                    let mut e = Any::Null;
                    if d.unmarshal(child, &mut e, "interface {}")? {
                        out.push(e);
                    }
                }
                *self = Any::Seq(out);
                Ok(true)
            }
            _ => {
                if !d.check_unique(n) {
                    return Ok(false);
                }
                let mut merged = d.merged_fields.take();
                let mut merge_node = None;
                let mut entries: Vec<(Any, Any)> = match std::mem::take(self) {
                    Any::Map(m) => m,
                    _ => Vec::new(),
                };
                let content = d.content(n);
                let mut i = 0;
                while i < content.len() {
                    let (kn, vn) = (content[i], content[i + 1]);
                    i += 2;
                    if d.doc.is_merge(kn) {
                        merge_node = Some(vn);
                        continue;
                    }
                    let mut k = Any::Null;
                    if !d.unmarshal(kn, &mut k, "interface {}")? {
                        continue;
                    }
                    if !Dec::first_merge(&mut merged, k.merge_key()) {
                        continue;
                    }
                    if matches!(k, Any::Seq(_) | Any::Map(_)) {
                        return Err(Fail(format!("yaml: invalid map key: {}", go_sprint(&k))));
                    }
                    let mut e = Any::Null;
                    d.unmarshal(vn, &mut e, "interface {}")?;
                    match entries.iter_mut().find(|(ek, _)| *ek == k) {
                        Some(slot) => slot.1 = e,
                        None => entries.push((k, e)),
                    }
                }
                d.merged_fields = merged;
                *self = Any::Map(entries);
                if let Some(m) = merge_node {
                    d.merge(n, m, self, "interface {}")?;
                }
                Ok(true)
            }
        }
    }
}

/// `%#v` of a composite key (only reached on invalid input).
fn go_sprint(v: &Any) -> String {
    match v {
        Any::Seq(_) => "[]interface {}{...}".into(),
        _ => "map[string]interface {}{...}".into(),
    }
}
