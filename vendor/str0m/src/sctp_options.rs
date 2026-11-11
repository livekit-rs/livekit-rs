//! Explicit SCTP transport parameters; defaults preserve existing native behavior.
use sctp_proto::TransportConfig;
/// Application-selected association parameters. Zero keeps each native default.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct SctpTransportOptions {
    /// Minimum congestion window in bytes.
    pub min_cwnd: u32,
    /// Fast-retransmission burst budget in bytes; individual packets remain MTU bound.
    pub fast_rtx_wnd: u32,
    /// Use source-compatible partial reliability independently of burst size.
    /// Defaults to false for library callers; RTC Engine explicitly selects true.
    pub source_partial_reliability: bool,
    /// Congestion avoidance increment in bytes, with a floor of one MTU.
    pub cwnd_ca_step: u32,
}
impl SctpTransportOptions {
    pub(crate) fn transport(self) -> TransportConfig {
        TransportConfig::default()
            .with_min_cwnd(self.min_cwnd)
            .with_fast_rtx_wnd(self.fast_rtx_wnd)
            .with_source_partial_reliability(self.source_partial_reliability)
            .with_cwnd_ca_step(self.cwnd_ca_step)
    }
}
