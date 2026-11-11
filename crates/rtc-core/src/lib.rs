//! Protocol-agnostic RTC engine core: identifiers, domain model, permissions,
//! normalized configuration, the room state machine and the contracts between
//! sessions, rooms and the media engine. Must not depend on any `lk-*` crate
//! (ARCH-01).

pub mod auto_egress;
pub mod config;
pub mod data;
pub mod events;
pub mod forward_stats;
pub mod ids;
pub mod media_api;
pub mod media_debug;
pub mod model;
pub mod permission;
pub mod room;
pub mod session;
pub mod transport_fallback;
pub mod util;
