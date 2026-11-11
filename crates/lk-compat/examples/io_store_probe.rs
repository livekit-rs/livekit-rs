//! Real production IOInfo/IoStore/RpcClient probe, never a mock worker or product entry.
use base64::{Engine, engine::general_purpose::STANDARD};
use lk_compat::{
    service_bus::{
        redis_transport::{RedisTransport, TransportLimits},
        rpc::{
            CallOptions, MethodSpec, RawRequest, RawResponse, RpcClient, RpcError, ServerLimits,
        },
        wire::{CompressionPolicy, WireLimits},
    },
    sip::catalog::SipCatalog,
    workers::{
        io_service::{IoEvent, IoEvents, IoInfo},
        preserved::WireValue,
        sip_callbacks::CatalogSipCallbacks,
        store::IoStore,
    },
};
use lk_protocol::{livekit as pb, rpc};
use prost::{Message, Name};
use redis::IntoConnectionInfo;
use serde::Deserialize;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::{
    collections::BTreeMap,
    io::{BufRead, Write},
    sync::{Arc, Mutex},
    time::Duration,
};
#[derive(Deserialize)]
struct Input {
    #[serde(default)]
    case: String,
    op: String,
    #[serde(default)]
    message: Value,
    #[serde(default)]
    wire_base64: String,
    #[serde(default)]
    id: String,
    #[serde(default)]
    room: String,
    #[serde(default)]
    active: bool,
    #[serde(default)]
    timeout_ms: u64,
}
#[derive(Default)]
struct Recorder(Mutex<(BTreeMap<String, usize>, Vec<Value>)>);
impl IoEvents for Recorder {
    fn notify(&self, event: IoEvent) {
        let (name, id, status, started, updated) = match event {
            IoEvent::EgressStarted(v) => (
                "egress_started",
                v.egress_id,
                v.status,
                v.started_at,
                v.updated_at,
            ),
            IoEvent::EgressUpdated(v) => (
                "egress_updated",
                v.egress_id,
                v.status,
                v.started_at,
                v.updated_at,
            ),
            IoEvent::EgressEnded(v) => (
                "egress_ended",
                v.egress_id,
                v.status,
                v.started_at,
                v.updated_at,
            ),
            IoEvent::IngressCreated(v) => ingress("ingress_created", *v),
            IoEvent::IngressDeleted(v) => ingress("ingress_deleted", *v),
            IoEvent::IngressStarted(v) => ingress("ingress_started", *v),
            IoEvent::IngressUpdated(v) => ingress("ingress_updated", *v),
            IoEvent::IngressEnded(v) => ingress("ingress_ended", *v),
        };
        let mut record = self.0.lock().unwrap_or_else(|e| e.into_inner());
        let next = record.0.len();
        let ordinal = *record.0.entry(id).or_insert(next);
        record.1.push(json!({"event":name,"entity_ordinal":ordinal,"status":status,"started_at":started,"updated_at":updated}));
    }
}
fn ingress(name: &'static str, v: pb::IngressInfo) -> (&'static str, String, i32, i64, i64) {
    let s = v.state.unwrap_or_default();
    (name, v.ingress_id, s.status, s.started_at, s.updated_at)
}
fn limits() -> TransportLimits {
    TransportLimits {
        wire: WireLimits {
            encoded_bytes: 1 << 20,
            decompressed_bytes: 1 << 20,
        },
        compression: CompressionPolicy {
            quality: 0,
            threshold: 1024,
        },
        inbox_messages: 128,
        subscriptions: 64,
        commands: 32,
        io_timeout: Duration::from_secs(3),
    }
}
fn spec(method: &str) -> MethodSpec {
    MethodSpec {
        service: "IOInfo".into(),
        method: method.into(),
        topics: vec![],
        require_claim: true,
        queue: true,
    }
}
fn decode<T: Message + Name + Default>(input: &Input) -> Result<WireValue<T>, RpcError> {
    if !input.wire_base64.is_empty() {
        let raw = STANDARD
            .decode(&input.wire_base64)
            .map_err(|_| RpcError::new("invalid_argument", "invalid fixture base64"))?;
        return WireValue::decode(&raw).map_err(Into::into);
    }
    let typed = lk_protocol::json::decode(
        &serde_json::to_vec(&input.message)
            .map_err(|_| RpcError::new("invalid_argument", "invalid fixture JSON"))?,
    )
    .map_err(|_| RpcError::new("invalid_argument", "invalid fixture protobuf JSON"))?;
    Ok(WireValue::from_message(typed))
}
fn emit<T: Message + Name + Default>(value: WireValue<T>) -> (String, Vec<u8>) {
    (T::full_name(), value.encode())
}
fn emit_raw<T: Message + Name>(value: RawResponse<T>) -> (String, Vec<u8>) {
    (T::full_name(), value.raw)
}
fn empty() -> (String, Vec<u8>) {
    ("google.protobuf.Empty".into(), Vec::new())
}
async fn call(client: &RpcClient, input: &Input) -> Result<(String, Vec<u8>), RpcError> {
    let millis = if input.timeout_ms == 0 {
        3000
    } else {
        input.timeout_ms
    };
    if !(1..=30000).contains(&millis) {
        return Err(RpcError::new(
            "invalid_argument",
            "request timeout outside fixture bound",
        ));
    }
    let mut options = CallOptions::new(Duration::from_millis(millis));
    // Fixed psrpc getRequestOpts: non-affinity IOInfo accepts first available
    // bid, with the one-second default selection deadline, then sends a grant.
    options.selection.accept_first = true;
    options.selection.affinity_timeout = Some(Duration::from_secs(1));
    macro_rules! request {
        ($request:ty,$response:ty,$value:expr) => {{
            let value: WireValue<$request> = $value;
            let raw = value.encode();
            let request = RawRequest {
                message: value.message,
                raw: raw.into(),
            };
            emit_raw(
                client
                    .unary_raw_request::<$request, $response>(&spec(&input.op), &request, options)
                    .await?,
            )
        }};
    }
    Ok(match input.op.as_str() {
        "CreateEgress" | "UpdateEgress" => request!(pb::EgressInfo, (), decode(input)?),
        "GetEgress" => request!(
            rpc::GetEgressRequest,
            pb::EgressInfo,
            WireValue::from_message(rpc::GetEgressRequest {
                egress_id: input.id.clone()
            })
        ),
        "ListEgress" => request!(
            pb::ListEgressRequest,
            pb::ListEgressResponse,
            WireValue::from_message(pb::ListEgressRequest {
                egress_id: input.id.clone(),
                room_name: input.room.clone(),
                active: input.active,
                ..Default::default()
            })
        ),
        "CreateIngress" => request!(pb::IngressInfo, rpc::CreateIngressResponse, decode(input)?),
        "GetIngressInfo" => request!(
            rpc::GetIngressInfoRequest,
            rpc::GetIngressInfoResponse,
            decode(input)?
        ),
        "UpdateIngressState" => request!(rpc::UpdateIngressStateRequest, (), decode(input)?),
        "GetSIPTrunkAuthentication" => request!(
            rpc::GetSipTrunkAuthenticationRequest,
            rpc::GetSipTrunkAuthenticationResponse,
            decode(input)?
        ),
        "EvaluateSIPDispatchRules" => request!(
            rpc::EvaluateSipDispatchRulesRequest,
            rpc::EvaluateSipDispatchRulesResponse,
            decode(input)?
        ),
        "UpdateSIPCallState" => request!(rpc::UpdateSipCallStateRequest, (), decode(input)?),
        "RecordCallContext" => request!(rpc::RecordCallContextRequest, (), decode(input)?),
        _ => return Err(RpcError::new("unknown", "unsupported fixed operation")),
    })
}
async fn execute_store(store: &IoStore, input: &Input) -> Result<(String, Vec<u8>), RpcError> {
    match input.op.as_str() {
        "StoreEgress" => {
            store.store_egress_wire(&decode(input)?).await?;
            Ok(empty())
        }
        "UpdateEgress" => {
            store.update_egress_wire(&decode(input)?).await?;
            Ok(empty())
        }
        "GetEgress" => Ok(emit(store.load_egress_wire(&input.id).await?)),
        "ListEgress" => {
            let items = store.list_egress_wire(&input.room, input.active).await?;
            let mut result = WireValue::from_message(pb::ListEgressResponse {
                items: items.iter().map(|v| v.message.clone()).collect(),
                ..Default::default()
            });
            result
                .wire
                .replace_repeated("items", items.into_iter().map(|v| v.wire));
            Ok(emit(result))
        }
        "CleanEndedEgress" => {
            store
                .clean_ended_egress(rtc_core::util::unix_ms().saturating_mul(1_000_000))
                .await?;
            Ok(empty())
        }
        "StoreIngress" => {
            store.store_ingress_wire(&decode(input)?).await?;
            Ok(empty())
        }
        "UpdateIngress" => {
            store.update_ingress_wire(&decode(input)?).await?;
            Ok(empty())
        }
        "GetIngress" => Ok(emit(store.load_ingress_wire(&input.id).await?)),
        "ListIngress" => {
            let items = store.list_ingress_wire(&input.room).await?;
            let mut result = WireValue::from_message(pb::ListIngressResponse {
                items: items.iter().map(|v| v.message.clone()).collect(),
                ..Default::default()
            });
            result
                .wire
                .replace_repeated("items", items.into_iter().map(|v| v.wire));
            Ok(emit(result))
        }
        "UpdateIngressState" => {
            let value: WireValue<rpc::UpdateIngressStateRequest> = decode(input)?;
            let state = value
                .wire
                .nested("state")
                .ok_or_else(|| RpcError::new("invalid_argument", "fixture state missing"))?;
            store
                .update_ingress_state_wire(
                    &value.message.ingress_id,
                    &WireValue::decode(&state.encode_to_vec())?,
                )
                .await?;
            Ok(empty())
        }
        "DeleteIngress" => {
            let info = store.load_ingress(&input.id).await?;
            store.delete_ingress(&info).await?;
            Ok(empty())
        }
        _ => Err(RpcError::new(
            "unknown",
            "unsupported fixed store operation",
        )),
    }
}
fn arg(name: &str) -> Option<String> {
    std::env::args().skip_while(|v| v != name).nth(1)
}
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mode = arg("--mode").unwrap_or_else(|| "call".into());
    let url = std::env::var("IO_REDIS_URL")
        .or_else(|_| arg("--redis").ok_or(std::env::VarError::NotPresent))?;
    let settings = url.clone().into_connection_info()?;
    let store = Arc::new(IoStore::connect(settings.into(), Duration::from_secs(3)).await?);
    let bus = RedisTransport::connect(&url, limits()).await?;
    if mode == "serve" {
        let recorder = Arc::new(Recorder::default());
        let catalog = SipCatalog::connect(&url, Duration::from_secs(3), 32).await?;
        let service = IoInfo::new(
            Some(store.clone()),
            recorder.clone(),
            Arc::new(CatalogSipCallbacks::new(Some(catalog))),
        );
        store.start("1.13.10").await?;
        let running = service
            .register(
                bus,
                ServerLimits {
                    concurrent: 32,
                    max_request_duration: Duration::from_secs(30),
                    allow_skip_claim: false,
                },
            )
            .await?;
        // register has returned only after every real Redis subscription ACK.
        std::fs::write(arg("--ready").ok_or("ready path required")?, b"ACK_READY\n")?;
        #[cfg(unix)]
        {
            let mut terminate =
                tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
            tokio::select! {result=tokio::signal::ctrl_c()=>result?,_=terminate.recv()=>{}}
        }
        #[cfg(not(unix))]
        tokio::signal::ctrl_c().await?;
        running.shutdown().await;
        let events = recorder
            .0
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .1
            .clone();
        std::fs::write(
            arg("--events").ok_or("events path required")?,
            serde_json::to_vec(&events)?,
        )?;
        return Ok(());
    }
    let client = RpcClient::new(bus, 32)?;
    let stdin = std::io::stdin();
    let mut stdout = std::io::stdout().lock();
    let mut total = 0;
    for line in stdin.lock().lines() {
        let line = line?;
        total += line.len();
        if line.len() > 1 << 20 || total > 64 << 20 {
            return Err("fixture input bound exceeded".into());
        }
        let input: Input = serde_json::from_str(&line)?;
        let result = if mode == "store" {
            execute_store(&store, &input).await
        } else {
            call(&client, &input).await
        };
        let row = match result {
            Ok((name, raw)) => {
                json!({"case":input.case,"status":"OK","proto_name":name,"wire_base64":STANDARD.encode(&raw),"wire_sha256":format!("{:x}",Sha256::digest(&raw))})
            }
            Err(error) => {
                json!({"case":input.case,"status":"ERROR","code":error.code,"message":error.message})
            }
        };
        serde_json::to_writer(&mut stdout, &row)?;
        writeln!(stdout)?;
        stdout.flush()?;
    }
    client.close();
    Ok(())
}
