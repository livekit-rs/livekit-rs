//! Exact production robust-throughput source vectors with production unit types.
#![allow(dead_code)]
use str0m_proto::Bitrate;
mod rtp_ {
    pub use str0m_proto::DataSize;
}
#[path = "../../../vendor/str0m/src/bwe/robust_throughput_estimator.rs"]
mod native_robust_source;
