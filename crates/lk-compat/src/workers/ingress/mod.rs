//! Source-bound external Ingress gateway. Native WHIP HTTP RTC stays separate.
mod events;
mod failure;
mod gateway;
mod go_url;
mod guid;
mod url_support;
mod validation;
pub use events::IngressDeletionEvents;
pub use failure::IngressFailure;
pub use gateway::{IngressAdapters, IngressConfig, IngressDeleted, IngressGateway};
pub use go_url::FixedGoUrl;
pub use guid::FixedGuid;
#[cfg(test)]
mod url_oracle_tests;
