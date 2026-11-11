//! A serde `Deserializer` over one yaml.v3 node, for the non-upstream
//! `livecloud:` extension section (its error texts are our own). Scalars are
//! typed by yaml.v3 resolution; aliases are followed.

use serde::de::{self, DeserializeSeed, IntoDeserializer, MapAccess, SeqAccess, Visitor};

use super::node::{Doc, Id, Kind};
use super::resolve::{Resolved, resolve};

#[derive(Clone, Copy)]
pub(crate) struct NodeDeserializer<'a> {
    pub doc: &'a Doc,
    pub id: Id,
}

impl<'a> NodeDeserializer<'a> {
    fn target(self) -> Self {
        let mut id = self.id;
        let mut hops = 0;
        while self.doc.node(id).kind == Kind::Alias && hops < 64 {
            id = self.doc.node(id).alias.expect("parser resolves aliases");
            hops += 1;
        }
        NodeDeserializer { doc: self.doc, id }
    }

    fn at(&self) -> String {
        let n = self.doc.node(self.id);
        format!(" at line {}, column {}", n.line, n.column)
    }
}

impl<'de> IntoDeserializer<'de, de::value::Error> for NodeDeserializer<'_> {
    type Deserializer = Self;
    fn into_deserializer(self) -> Self {
        self
    }
}

impl<'de> de::Deserializer<'de> for NodeDeserializer<'_> {
    type Error = de::value::Error;

    fn deserialize_any<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, Self::Error> {
        let this = self.target();
        let n = this.doc.node(this.id);
        match n.kind {
            Kind::Mapping => visitor.visit_map(Entries {
                doc: this.doc,
                items: &n.content,
                at: 0,
            }),
            Kind::Sequence => visitor.visit_seq(Items {
                doc: this.doc,
                items: &n.content,
                at: 0,
            }),
            _ => {
                if this.doc.indicated_string(this.id) {
                    return visitor.visit_string(n.value.clone());
                }
                let (_, resolved) =
                    resolve(&n.tag, &n.value).map_err(|e| de::Error::custom(e.0))?;
                match resolved {
                    Resolved::Null => visitor.visit_unit(),
                    Resolved::Bool(b) => visitor.visit_bool(b),
                    Resolved::Int(v) => visitor.visit_i64(v),
                    Resolved::Uint(v) => visitor.visit_u64(v),
                    Resolved::Float(f) => visitor.visit_f64(f),
                    Resolved::Str(_) | Resolved::Timestamp => visitor.visit_string(n.value.clone()),
                }
                .map_err(|e: Self::Error| de::Error::custom(format!("{e}{}", this.at())))
            }
        }
    }

    fn deserialize_option<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, Self::Error> {
        let this = self.target();
        if this.doc.node(this.id).kind == Kind::Scalar && this.doc.short_tag(this.id) == "!!null" {
            visitor.visit_none()
        } else {
            visitor.visit_some(this)
        }
    }

    fn deserialize_newtype_struct<V: Visitor<'de>>(
        self,
        _: &'static str,
        visitor: V,
    ) -> Result<V::Value, Self::Error> {
        visitor.visit_newtype_struct(self)
    }

    serde::forward_to_deserialize_any! {
        bool i8 i16 i32 i64 i128 u8 u16 u32 u64 u128 f32 f64 char str string
        bytes byte_buf unit unit_struct seq tuple tuple_struct map struct enum
        identifier ignored_any
    }
}

struct Entries<'a> {
    doc: &'a Doc,
    items: &'a [Id],
    at: usize,
}

impl<'de> MapAccess<'de> for Entries<'_> {
    type Error = de::value::Error;
    fn next_key_seed<K: DeserializeSeed<'de>>(
        &mut self,
        seed: K,
    ) -> Result<Option<K::Value>, Self::Error> {
        if self.at >= self.items.len() {
            return Ok(None);
        }
        let id = self.items[self.at];
        self.at += 1;
        seed.deserialize(NodeDeserializer { doc: self.doc, id })
            .map(Some)
    }
    fn next_value_seed<V: DeserializeSeed<'de>>(
        &mut self,
        seed: V,
    ) -> Result<V::Value, Self::Error> {
        let id = self.items[self.at];
        self.at += 1;
        seed.deserialize(NodeDeserializer { doc: self.doc, id })
    }
}

struct Items<'a> {
    doc: &'a Doc,
    items: &'a [Id],
    at: usize,
}

impl<'de> SeqAccess<'de> for Items<'_> {
    type Error = de::value::Error;
    fn next_element_seed<T: DeserializeSeed<'de>>(
        &mut self,
        seed: T,
    ) -> Result<Option<T::Value>, Self::Error> {
        if self.at >= self.items.len() {
            return Ok(None);
        }
        let id = self.items[self.at];
        self.at += 1;
        seed.deserialize(NodeDeserializer { doc: self.doc, id })
            .map(Some)
    }
}
