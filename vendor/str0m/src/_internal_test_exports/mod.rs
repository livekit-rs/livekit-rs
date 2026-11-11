//! Exported things with feature `_internal_test_exports`.

use crate::Rtc;
use crate::format::PayloadParams;
use crate::media::Media;
use crate::media::Mid;
use crate::rtp::{ExtensionMap, RtpHeader};
use is::IceCreds;

pub mod fuzz;
mod rng;
use rng::Rng;

mod setup;

impl Rtc {
    /// UNSTABLE: test-only authenticated RTCP fixture. Uses this real peer's
    /// established DTLS-SRTP send context; returns None before key installation.
    /// No key bytes are exported and no production feature enables this API.
    pub fn _protect_rtcp(&mut self, plain: &[u8]) -> Option<Vec<u8>> {
        self.session.protect_rtcp_test(plain)
    }

    /// UNSTABLE: not public API!
    pub fn _mids(&self) -> Vec<Mid> {
        self.session.medias.iter().map(Media::mid).collect()
    }

    /// UNSTABLE: not public API!
    pub fn _exts(&self) -> &ExtensionMap {
        &self.session.exts
    }

    /// UNSTABLE: not public API!
    pub fn _local_ice_creds(&self) -> IceCreds {
        self.ice.local_credentials().clone()
    }
}

impl RtpHeader {
    /// UNSTABLE: not public API!
    pub fn _parse(buf: &[u8], exts: &ExtensionMap) -> Option<RtpHeader> {
        Self::parse(buf, exts)
    }
}

impl PayloadParams {
    /// UNSTABLE: not public API!
    pub fn _is_locked(&self) -> bool {
        self.locked
    }
}

/// UNSTABLE: not public API!
pub use crate::bwe_::ProbeClusterConfig;
