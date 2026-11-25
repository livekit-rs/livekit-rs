//! Official Agent Worker control-plane compatibility.
//! Room/job dispatch integration is separate; a registered worker is not a
//! successful agent job. See SPEC0017 and source-bound registry evidence.
pub(crate) mod cluster;
mod codec;
pub mod endpoint;
pub mod registry;
mod token;
pub mod websocket;
pub mod wt_control;

pub(crate) mod bridge;
pub mod service;

#[cfg(test)]
mod oracle_tests;
/// AGENT_WIRE leaf suite. Compiled into the library only for tests so both the
/// in-crate legs (the WebTransport control plane) and the integration-tier fake
/// worker (`tests/agent_worker_lifecycle.rs`, the real `/agent` WebSocket plus
/// the real room/job graph) share one codec-framing implementation instead of
/// two diverging copies.
#[cfg(test)]
pub mod wire_tests;
