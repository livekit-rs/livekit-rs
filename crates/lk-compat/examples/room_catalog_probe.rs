//! Development-only actual Redis RoomCatalog reader. Wire output is PRIVATE:
//! real Room protobuf includes TURN credentials; persist safe hashes only.
use base64::{Engine, engine::general_purpose::STANDARD};
use lk_compat::{room_control::catalog::RoomCatalog, service_bus::rpc::RpcError};
use lk_protocol::livekit as pb;
use redis::IntoConnectionInfo;
use serde::Deserialize;
use std::{
    io::{Read, Write},
    time::Duration,
};
#[derive(Deserialize)]
struct Input {
    case: String,
    op: String,
    #[serde(default)]
    room: String,
    #[serde(default)]
    identity: String,
    #[serde(default)]
    names: Vec<String>,
}
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let info = std::env::var("CLUSTER_CONTROL_REDIS_URL")?.into_connection_info()?;
    let catalog = RoomCatalog::connect(info, Duration::from_secs(2), 1 << 20).await?;
    let mut input = Vec::new();
    std::io::stdin()
        .take((1 << 20) + 1)
        .read_to_end(&mut input)?;
    if input.len() > 1 << 20 {
        return Err("fixture input exceeds one MiB".into());
    }
    let mut output = std::io::stdout().lock();
    let mut count = 0;
    for line in input.split(|b| *b == b'\n').filter(|line| !line.is_empty()) {
        count += 1;
        if count > 128 {
            return Err("fixture command count exceeds128".into());
        }
        let request: Input = serde_json::from_slice(line)?;
        let result:Result<serde_json::Value,RpcError>=async{match request.op.as_str(){
   "LoadRoom"=>{let row=catalog.load(&request.room).await.map_err(RpcError::from)?;Ok(serde_json::json!({"status":"OK","proto_name":"livekit.Room","wire_base64":STANDARD.encode(row.room.encode())}))},
   "ListRooms"=>{let names=(!request.names.is_empty()).then_some(request.names.as_slice());let rooms=catalog.list(names).await.map_err(RpcError::from)?;let mut list=lk_compat::sip::wire::PreservedMessage::from_message(&pb::ListRoomsResponse::default());list.replace_repeated("rooms",rooms.into_iter().map(|r|r.wire));Ok(serde_json::json!({"status":"OK","proto_name":"livekit.ListRoomsResponse","wire_base64":STANDARD.encode(list.encode_to_vec())}))},
   "LoadParticipant"=>{let row=catalog.load_participant(&request.room,&request.identity).await.map_err(RpcError::from)?;Ok(serde_json::json!({"status":"OK","proto_name":"livekit.ParticipantInfo","wire_base64":STANDARD.encode(row.encode())}))},
   "ListParticipants"=>{let participants=catalog.list_participants(&request.room).await.map_err(RpcError::from)?;let mut list=lk_compat::sip::wire::PreservedMessage::from_message(&pb::ListParticipantsResponse::default());list.replace_repeated("participants",participants.into_iter().map(|p|p.wire));Ok(serde_json::json!({"status":"OK","proto_name":"livekit.ListParticipantsResponse","wire_base64":STANDARD.encode(list.encode_to_vec())}))},
   _=>Err(RpcError::new("invalid_argument","unknown fixture operation")),
  }}.await;
        let mut row = match result {
            Ok(row) => row,
            Err(error) => {
                serde_json::json!({"status":"ERROR","code":error.code,"message":error.message})
            }
        };
        row["case"] = request.case.into();
        row["op"] = request.op.into();
        serde_json::to_writer(&mut output, &row)?;
        output.write_all(b"\n")?;
    }
    output.flush()?;
    Ok(())
}
