//! One SDK websocket whose participant lives on another RTC node. Only the
//! fixed SignalRequest/Response wire crosses this edge; engine events stay on
//! their owner. Binary Blob provenance is never projected on the controller.
use crate::cluster_signal::RemoteSession;
use axum::extract::ws::{CloseFrame, Message, Utf8Bytes, WebSocket};
use futures_util::{SinkExt, StreamExt};
use lk_protocol::livekit as pb;
use prost::Message as _;
use std::time::Duration;

pub(super) async fn run(
    socket: WebSocket,
    mut session: RemoteSession,
    first: Vec<u8>,
    manager: rtc_runtime::RoomManager,
) {
    let (mut tx, mut rx) = socket.split();
    let mut json = false;
    let mut closing = false;
    let mut invalid = false;
    let mut oversized = false;
    let mut protocol_error_close = None;
    let mut ping = tokio::time::interval(Duration::from_secs(10));
    ping.tick().await;
    if !matches!(
        super::session::send(&manager, &mut tx, Message::Binary(first.into())).await,
        Ok(())
    ) {
        return;
    }
    manager.record_signal_connected();
    loop {
        tokio::select! {
            output=session.incoming.recv()=>{
                let Some(raw)=output else{closing=true;break;};
                let frame=if json{match lk_protocol::wire::decode::<pb::SignalResponse>(&raw).ok().and_then(|response|serde_json::to_string(&response).ok()){Some(text)=>Message::Text(text.into()),None=>break}}else{Message::Binary(raw.into())};
                if !matches!(super::session::send(&manager, &mut tx, frame).await,Ok(())){break;}
            },input=rx.next()=>{
                let Some(input)=input else{break;};
                let frame = match input {
                    Ok(frame) => frame,
                    Err(error) => {
                        oversized = super::session::capacity_message_too_long(&error);
                        invalid = super::session::invalid_compressed_frame(&error);
                        protocol_error_close = super::session::protocol_close(&error);
                        break;
                    }
                };
                let received_bytes=super::session::application_frame_len(&frame);
                let raw=match frame{
                    Message::Binary(raw)=>{json=false;if lk_protocol::wire::decode::<pb::SignalRequest>(&raw).is_err(){invalid=true;break;}raw.to_vec()},
                    Message::Text(text)=>{json=true;match lk_protocol::json::decode_frame::<pb::SignalRequest>(text.as_bytes()){Ok(request)=>request.encode_to_vec(),Err(_)=>{invalid=true;break;}}},
                    Message::Close(_)=>break,Message::Ping(_)|Message::Pong(_)=>continue,
                };
                let request=match lk_protocol::wire::decode::<pb::SignalRequest>(&raw){Ok(request)=>request,Err(_)=>{invalid=true;break;}};
                manager.record_signaling(received_bytes.unwrap(),false);
                let response_frame=if matches!(&request.message,Some(pb::signal_request::Message::Ping(_)|pb::signal_request::Message::PingReq(_))){
                    let mut cache=super::encode::RequestCache::default();
                    let response=match super::encode::decode_request(request,&mut cache){super::encode::Inbound::Reply(response)|super::encode::Inbound::ForwardAndReply(_,response)=>Some(response),_=>None};
                    if let Some(response)=response{
                        Some(if json{match serde_json::to_string(&response){Ok(text)=>Message::Text(text.into()),Err(_)=>break}}else{Message::Binary(response.encode_to_vec().into())})
                    }else{None}
                }else{None};
                // Even a failed controller pong write must not discard the
                // successfully decoded request before its owner sink attempt.
                if !super::session::reply_then_forward(async {
                    match response_frame {Some(frame)=>super::session::send(&manager,&mut tx,frame).await.is_ok(),None=>true}
                },session.send(raw)).await{break;}
            },_=ping.tick()=>{
                if !matches!(super::session::send(&manager, &mut tx, Message::Ping(Vec::new().into())).await,Ok(())){break;}
            }
        }
    }
    // The controller's request sink drains as soon as its read loop ends.
    // A slow WebSocket close must not delay the owner's final requests or EOF.
    session.begin_request_close();
    if closing || invalid || oversized || protocol_error_close.is_some() {
        let _ = tokio::time::timeout(
            Duration::from_secs(5),
            tx.send(Message::Close(Some(protocol_error_close.unwrap_or_else(
                || CloseFrame {
                    code: if oversized { 1009 } else { 1000 },
                    reason: Utf8Bytes::from_static(""),
                },
            )))),
        )
        .await;
    }
    let _ = tx.close().await;
    session.close().await;
}
