//! Worker-backed IOInfo callbacks, state and Egress gateway.
//! External workers provide real media operations; no fabricated result fallback.
pub mod auto_egress_policy;
pub mod egress;
pub mod error;
pub mod ingress;
pub mod io_service;
pub mod methods;
pub mod preserved;
pub mod retry;
pub mod sip_callbacks;
pub mod store;
#[cfg(test)]
mod tests;
mod version;

pub mod sip_gateway;
