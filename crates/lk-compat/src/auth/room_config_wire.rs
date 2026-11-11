//! Preserve only the actual raw nested RoomConfig validation boundary. The
//! upstream wrapper DISCARDs unknown fields; known lexical duplicates still fail.
use serde::de::{MapAccess, Visitor};
use serde::{Deserialize, Deserializer};
use serde_json::value::RawValue;
use std::fmt;

struct RawClaims(Vec<(String, Box<RawValue>)>);
impl<'de> Deserialize<'de> for RawClaims {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct Object;
        impl<'de> Visitor<'de> for Object {
            type Value = RawClaims;
            fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str("JWT object or null")
            }
            fn visit_unit<E: serde::de::Error>(self) -> Result<Self::Value, E> {
                Ok(RawClaims(Vec::new()))
            }
            fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> Result<Self::Value, A::Error> {
                let mut fields = Vec::new();
                while let Some(pair) = map.next_entry()? {
                    fields.push(pair);
                }
                Ok(RawClaims(fields))
            }
        }
        deserializer.deserialize_any(Object)
    }
}
/// Check each actual encoding/json RoomConfig field occurrence BEFORE the
/// ordinary value map collapses equal keys. Null remains a nil pointer. This
/// does not alter top-level ordinary-claim last-wins semantics or unknown fields.
pub(super) fn validate(raw_claims: &[u8]) -> Result<(), String> {
    let RawClaims(fields) = serde_json::from_slice(raw_claims).map_err(|e| e.to_string())?;
    for (name, value) in fields {
        // The ASCII tag has no K/S Unicode SimpleFold special group; decoded
        // ASCII case-insensitive spellings match encoding/json's field tag.
        if name.eq_ignore_ascii_case("roomConfig") && value.get() != "null" {
            let _: lk_protocol::livekit::RoomConfiguration =
                lk_protocol::json::diagnostic::decode(value.get().as_bytes())
                    .map_err(|e| e.to_string())?;
        }
    }
    Ok(())
}
