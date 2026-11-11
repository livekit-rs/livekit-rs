//! LiveKit v1.13.10 compatibility adapter.
//!
//! Translates the LiveKit wire protocol (JWT access tokens, Twirp server API,
//! webhooks, data-channel framing, TURN credentials and WebSocket signaling) to
//! the protocol-agnostic engine in `rtc-core` / `rtc-runtime`. Behavior follows
//! `livekit-rs-docs/impl-notes/api-auth-webhook.md` and `signaling.md`.

// prost marks fields with `[deprecated = true]` in the .proto as `#[deprecated]`;
// upstream still reads and writes them, so do we.
#![allow(deprecated)]

pub mod agent;
pub mod auth;
pub mod cluster_signal;
pub mod convert;
pub mod data;
pub mod http;
mod local_allocation;
mod local_dispatch;
mod local_mutations;
mod local_participant;
mod local_room;
/// Generation-bound trusted room control RPC. Product activation is separate.
pub mod owner_dispatch;
mod participant_updates;
pub mod profile;
pub mod redis_endpoint;
/// Explicit component room-routing collaborator; product activation is separate.
pub mod room_control;
mod room_create;
/// Node identity producer: the record this server registers, refreshes and drains.
pub mod routing;
pub mod rpc;
pub mod service_bus;
pub mod signal;
pub mod sip;
pub mod state;
pub mod turn_auth;
pub mod twirp;
pub mod webhook;
pub mod worker_notifications;
pub mod worker_services;
pub mod workers;

pub use state::{AppState, CompatServices};

#[cfg(test)]
pub(crate) mod test_support;

/// Native one-shot WebRTC HTTP transport.
pub mod whip;
