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
