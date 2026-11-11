//! Same production loader and logger, deterministic producer markers. This is a
//! process-level filtering check, not evidence of an actual ICE negotiation.
#[path = "../src/log_sampling.rs"]
#[allow(dead_code)]
mod log_sampling;
#[path = "../src/logging.rs"]
#[allow(dead_code)]
mod logging;
#[path = "../src/zap.rs"]
#[allow(dead_code)]
mod zap;
fn main() {
    let info = lk_config::ProgramInfo {
        binary_name: "livecloud".into(),
        product_version: "f137-probe".into(),
        compat_version: "1.13.10",
    };
    let action = lk_config::parse(
        std::env::args_os().collect(),
        &lk_config::Env::from_process(),
        &info,
    )
    .expect("actual config pipeline");
    let lk_config::EntryAction::RunServer(loaded) = action else {
        panic!("expected RunServer")
    };
    logging::init(&loaded.server.logging);
    tracing::info!(target: "livecloud", "F137:root-info");
    tracing::error!(target: "livecloud", "F137:root-error");
    tracing::debug!(target: "is::agent", "F137:ice-debug");
    tracing::trace!(target: "is::agent", "F137:ice-trace");
    log::debug!(target: "dimpl::dtls", "F137:dtls-debug-log");
    log::error!(target: "dimpl::dtls", "F137:dtls-error-log");
}
