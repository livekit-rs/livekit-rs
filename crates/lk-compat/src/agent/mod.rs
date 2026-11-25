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
/// AGENT_WIRE leaf suite. Compiled into the library only for tests so both this
/// module's legs (the WebTransport control plane and the raw `/agent` WebSocket
/// fake worker) share one codec-framing implementation instead of two diverging
/// copies. It is a `pub` in-crate module because
/// [`worker_lifecycle_tests`] drives it through the same production registry,
/// event sink and room graph; an integration target under `tests/` could not see
/// any of it, since `tests/*.rs` link the non-test library build.
#[cfg(test)]
pub mod wire_tests;
/// C86 lane B: the 13 `AGENT_WIRE` leaves driven over the real `/agent`
/// WebSocket with the production service graph. In-crate for the same reason as
/// [`wire_tests`].
#[cfg(test)]
#[path = "worker_lifecycle_tests.rs"]
mod worker_lifecycle_tests;
