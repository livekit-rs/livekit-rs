//! Actual SIP call/transfer gateway; no invented worker replies or retries.
use super::super::{codec::Codec, error::TwirpError};
use super::super::{
    worker_codec::{input, output},
    worker_error::failure,
};
use crate::{
    auth::VerifiedToken, service_bus::rpc::CallOptions, state::AppState,
    workers::preserved::WireValue,
};
use axum::response::Response;
use lk_protocol::livekit as pb;
use std::time::{Duration, Instant};
pub async fn call(
    state: &AppState,
    auth: Option<&VerifiedToken>,
    method: &str,
    codec: Codec,
    body: &[u8],
    deadline: Option<Instant>,
) -> Result<Response, TwirpError> {
    // Even without Redis, source validation/real room lookup are performed in
    // their original order. There is no successful disconnected fallback.
    let disconnected = crate::worker_services::local_sip_gateway(
        state.manager.clone(),
        state.sip_catalog.as_deref().cloned(),
    );
    let gateway = state
        .worker_services
        .as_deref()
        .map(|s| &s.sip)
        .unwrap_or(&disconnected);
    let mut options = CallOptions::new(Duration::from_secs(80));
    options.deadline = deadline;
    match method {
        "CreateSIPParticipant" => {
            let value: WireValue<pb::CreateSipParticipantRequest> = input(codec, body)?;
            Ok(output(
                codec,
                gateway
                    .create_wire(auth, value, options)
                    .await
                    .map_err(failure)?,
            ))
        }
        "TransferSIPParticipant" => {
            let value: WireValue<pb::TransferSipParticipantRequest> = input(codec, body)?;
            Ok(output(
                codec,
                gateway
                    .transfer_wire(auth, value, options)
                    .await
                    .map_err(failure)?,
            ))
        }
        _ => Err(TwirpError::new(
            super::super::error::Code::BadRoute,
            "no handler for method",
        )),
    }
}
