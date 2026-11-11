//! Execute the exact vendored probe timing module with real identifier/unit types.
//! No copied pacing guard and no vendor dev-crypto feature graph.
#![allow(dead_code)]
#[macro_use]
extern crate tracing;
// The included upstream identifier source supports its own optional `drv`
// feature; this integration target intentionally does not enable that feature.
#[allow(unexpected_cfgs)]
#[path = "../../../vendor/str0m/src/rtp/id.rs"]
mod native_ids;
#[path = "../../../vendor/str0m/src/util/time_tricks.rs"]
mod native_time;
mod rtp_ {
    pub use crate::native_ids::TwccClusterId;
    pub use str0m_proto::{Bitrate, DataSize};
}
mod util {
    pub(crate) use crate::native_time::{already_happened, not_happening};
}
#[path = "../../../vendor/str0m/src/bwe/probe/cluster.rs"]
mod native_probe_source;

use native_probe_source::{ProbeClusterConfig, ProbeKind};
#[path = "../../../vendor/str0m/src/bwe/probe/control.rs"]
// Keep unrelated upstream nested-if style intact in this exact-source harness.
#[allow(clippy::collapsible_if)]
mod native_probe_control;
