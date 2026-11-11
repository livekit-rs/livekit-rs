//! Engine errors. Almost everything inside the engine is handled locally (a broken
//! transport fails that transport only), so only construction can fail publicly.

use rtc_core::ids::TransportId;

#[derive(Debug, thiserror::Error)]
pub enum MediaError {
    /// The shared DTLS certificate could not be generated.
    #[error("dtls certificate generation failed: {0}")]
    Certificate(String),
    #[error("unknown transport {0}")]
    UnknownTransport(TransportId),
    #[error("invalid sdp: {0}")]
    Sdp(String),
    #[error("invalid candidate: {0}")]
    Candidate(String),
    #[error("rtc: {0}")]
    Rtc(#[from] str0m::RtcError),
}
