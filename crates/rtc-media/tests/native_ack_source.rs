//! Execute the exact vendored source's private tests using its production units.
//! No copied estimator and no str0m upstream dev-crypto feature graph.
#![allow(dead_code)]
use str0m_proto::Bitrate;
mod rtp_ {
    pub use str0m_proto::DataSize;
}
#[path = "../../../vendor/str0m/src/bwe/acked_bitrate_estimator.rs"]
mod native_ack_source;
