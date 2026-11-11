//! Real Egress gateway. Public request decoding stays ahead of auth; worker
//! results retain their own protobuf provenance all the way to the HTTP response.
use super::super::{
    codec::{Codec, decode},
    error::{Code, TwirpError},
};
use super::super::{
    worker_codec::{input, output},
    worker_error::failure,
};
use crate::{
    auth::VerifiedToken,
    state::AppState,
    workers::{egress as gateway, preserved::WireValue},
};
use axum::response::Response;
use lk_protocol::{livekit as pb, rpc};
pub async fn call(
    state: &AppState,
    auth: Option<&VerifiedToken>,
    method: &str,
    codec: Codec,
    bytes: &[u8],
    deadline: Option<std::time::Instant>,
) -> Result<Response, TwirpError> {
    let services = state
        .worker_services
        .as_deref()
        .ok_or_else(|| TwirpError::internal("egress not connected (redis required)"))?;
    let mut options = services.call_options();
    options.deadline = deadline;
    options.expiry_ns = deadline.map(crate::service_bus::rpc::capture_expiry_ns);
    let id = || rtc_core::ids::new_sid("EG_");
    macro_rules! start {
        ($ty:ty, $wrap:path) => {{
            let request: WireValue<$ty> = input(codec, bytes)?;
            let internal: WireValue<rpc::StartEgressRequest> = $wrap(request, id());
            let result = services
                .egress
                .start_wire(auth, internal, options)
                .await
                .map_err(failure)?;
            Ok(output(codec, result))
        }};
    }
    match method {
        "StartEgress" => start!(pb::StartEgressRequest, gateway::wrap_start_wire),
        "StartRoomCompositeEgress" => {
            start!(pb::RoomCompositeEgressRequest, gateway::wrap_room_wire)
        }
        "StartWebEgress" => start!(pb::WebEgressRequest, gateway::wrap_web_wire),
        "StartParticipantEgress" => {
            start!(pb::ParticipantEgressRequest, gateway::wrap_participant_wire)
        }
        "StartTrackCompositeEgress" => {
            start!(pb::TrackCompositeEgressRequest, gateway::wrap_tracks_wire)
        }
        "StartTrackEgress" => start!(pb::TrackEgressRequest, gateway::wrap_track_wire),
        "ListEgress" => {
            let request: pb::ListEgressRequest = decode(codec, bytes)?;
            Ok(output(
                codec,
                services
                    .egress
                    .list_wire(auth, request)
                    .await
                    .map_err(failure)?,
            ))
        }
        "UpdateLayout" => {
            let request: pb::UpdateLayoutRequest = decode(codec, bytes)?;
            Ok(output(
                codec,
                services
                    .egress
                    .update_layout_wire(auth, request)
                    .await
                    .map_err(failure)?,
            ))
        }
        "UpdateStream" => {
            let request: WireValue<pb::UpdateStreamRequest> = input(codec, bytes)?;
            Ok(output(
                codec,
                services
                    .egress
                    .update_stream_wire(auth, request, options)
                    .await
                    .map_err(failure)?,
            ))
        }
        "StopEgress" => {
            let request: WireValue<pb::StopEgressRequest> = input(codec, bytes)?;
            Ok(output(
                codec,
                services
                    .egress
                    .stop_wire(auth, request, options)
                    .await
                    .map_err(failure)?,
            ))
        }
        _ => Err(TwirpError::new(Code::BadRoute, "no handler for method")),
    }
}
