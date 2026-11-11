//! Exact vendored SRTP context vectors with the production RustCrypto provider.
//! No optional native/default str0m crypto feature is enabled.
#![allow(dead_code)]
#[macro_use]
extern crate tracing;
mod crypto {
    pub use str0m::crypto::*;
    pub fn from_feature_flags() -> CryptoProvider {
        rtc_crypto::provider()
    }
    pub(crate) fn test_default_provider() -> &'static CryptoProvider {
        static PROVIDER: std::sync::OnceLock<CryptoProvider> = std::sync::OnceLock::new();
        PROVIDER.get_or_init(from_feature_flags)
    }
}
mod io {
    // Native src/io/mod.rs uses this solely as initial decryption scratch capacity;
    // not a crypto, packet-size acceptance or authentication policy substitute.
    pub const DATAGRAM_MAX_PACKET_SIZE: usize = 2000;
}
// Use the exact private header/extension/id/time implementations as well;
// public str0m re-exports intentionally do not expose their parse method.
// Retain the original dependency source and its range idioms byte-for-byte.
#[allow(unexpected_cfgs, clippy::manual_range_contains)]
#[path = "../../../vendor/str0m/src/rtp/ext.rs"]
mod ext;
// The borrowed header implementation targets the upstream MSRV.
#[allow(clippy::manual_is_multiple_of)]
#[path = "../../../vendor/str0m/src/rtp/header.rs"]
mod header;
#[allow(unexpected_cfgs)]
#[path = "../../../vendor/str0m/src/rtp/mtime.rs"]
mod mtime;
#[allow(unexpected_cfgs)]
#[path = "../../../vendor/str0m/src/rtp/id.rs"]
mod native_ids;
// The exact native utility module exports helpers used by other native modules;
// this source-exact SRTP subset does not consume those unrelated re-exports.
#[allow(unused_imports)]
#[path = "../../../vendor/str0m/src/util/mod.rs"]
mod util;
use native_ids::{Mid, Pt, Rid, SeqNo, Ssrc};
// Exact native src/rtp/mod.rs blank-padding payload resource constant.
const MAX_BLANK_PADDING_PAYLOAD_SIZE: usize = 240;
mod rtp {
    pub use crate::ext::ExtensionMap;
}
mod rtp_ {
    pub use crate::ext::{Extension, ExtensionMap};
    pub use crate::mtime::Frequency;
}
fn init_crypto_default() {
    crypto::from_feature_flags().install_process_default();
}
#[path = "../../../vendor/str0m/src/rtp/srtp.rs"]
mod native_srtp_source;
