//! Isolated psrpc wire foundation. No public service routes are enabled here.
//!
//! Redis subscriptions, request lifecycle, worker claims and real service side
//! effects belong to a subsequent cut. Callers must select explicit byte limits.

pub mod channels;
pub mod wire;

pub mod redis_transport;
pub mod rpc;

pub mod local_transport;
pub mod rpc_transport;

pub mod stream;

/// Fixed PSRPC OpenTelemetry span and W3C carrier integration.
pub mod telemetry;
