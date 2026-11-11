//! Actual IngressHandler callback lifecycle, roommanager_service.go:117–210.
//! A real callback response is required; unavailable broker/worker is never success.
use crate::service_bus::rpc::{CallOptions, Cancellation, MethodSpec, RpcClient, RpcError};
use lk_protocol::{livekit as pb, rpc};
use rtc_core::{
    ids::{Identity, ParticipantSid, RoomKey},
    model::TrackKind,
};
use rtc_runtime::RoomManager;
use std::{sync::Arc, time::Duration};
use tokio::sync::watch;
async fn send(
    client: Option<&RpcClient>,
    request: rpc::WhiprtcConnectionNotifyRequest,
    cancel: Cancellation,
) -> Result<(), RpcError> {
    let Some(client) = client else {
        return Err(RpcError::new(
            "unavailable",
            "ingress handler transport is not connected",
        ));
    };
    let spec = MethodSpec {
        service: "IngressHandler".into(),
        method: "WHIPRTCConnectionNotify".into(),
        topics: vec![request.participant_id.clone()],
        require_claim: false,
        queue: true,
    };
    let mut options = CallOptions::new(Duration::from_secs(3));
    options.cancel = cancel;
    options.selection.accept_first = true;
    options.selection.affinity_timeout = Some(Duration::from_secs(1));
    let _: () = client.unary(&spec, &request, options).await?;
    Ok(())
}
async fn state(
    manager: &RoomManager,
    room: &RoomKey,
    identity: &Identity,
    sid: &ParticipantSid,
) -> Option<rpc::WhiprtcConnectionNotifyRequest> {
    let tracks = manager
        .published_media(room, identity.clone(), sid.clone())
        .await
        .ok()?;
    let mut request = rpc::WhiprtcConnectionNotifyRequest {
        participant_id: sid.0.clone(),
        ..Default::default()
    };
    for media in tracks {
        let track = media.track;
        match track.kind {
            TrackKind::Audio if request.audio.is_none() => {
                request.audio = Some(pb::InputAudioState {
                    mime_type: track.mime_type,
                    channels: if track.stereo { 2 } else { 1 },
                    average_bitrate: media.average_bitrate,
                    ..Default::default()
                })
            }
            TrackKind::Video if request.video.is_none() => {
                request.video = Some(pb::InputVideoState {
                    mime_type: track.mime_type,
                    width: track.width,
                    height: track.height,
                    average_bitrate: media.average_bitrate,
                    ..Default::default()
                })
            }
            _ => {}
        }
    }
    Some(request)
}
/// Cancellation is owned by the real participant resource. A failed initial
/// notification does not suppress future ticks or the independent Closed callback.
pub async fn run(
    manager: RoomManager,
    room: RoomKey,
    identity: Identity,
    sid: ParticipantSid,
    client: Option<Arc<RpcClient>>,
    mut done: watch::Receiver<bool>,
) {
    let mut ticker = tokio::time::interval(Duration::from_secs(10));
    loop {
        if *done.borrow() {
            break;
        }
        tokio::select! {
         changed=done.changed()=>{if changed.is_err()||*done.borrow(){break;}},
         _=ticker.tick()=>{
          let Some(request)=state(&manager,&room,&identity,&sid).await else {break;};
          let cancellation=Cancellation::default();let call=send(client.as_deref(),request,cancellation.clone());tokio::pin!(call);
          tokio::select!{
           result=&mut call=>{if let Err(error)=result{tracing::warn!(error=%error,"WHIP ingress connection notification failed");}},
           _=done.changed()=>{cancellation.cancel();break;}
          }
         }
        }
    }
    // OnClose runs after UpTrackManager.Close; published tracks have been cleared.
    // Preserve final callback even if earlier alive calls failed.
    let closed = rpc::WhiprtcConnectionNotifyRequest {
        participant_id: sid.0,
        closed: true,
        ..Default::default()
    };
    if let Err(error) = send(client.as_deref(), closed, Cancellation::default()).await {
        tracing::warn!(error=%error,"WHIP ingress closed notification failed");
    }
}
