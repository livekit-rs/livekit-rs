//! Development-only real Redis SIP catalog oracle; not a public service endpoint.
use base64::{Engine, engine::general_purpose::STANDARD};
use lk_compat::sip::catalog::{CatalogError, SipCatalog};
use lk_compat::sip::wire::{PreservedMessage, Stored};
use lk_protocol::livekit::*;
use prost::Message;
use serde::{Deserialize, Serialize};
use std::{
    io::{Read, Write},
    time::Duration,
};

#[derive(Deserialize)]
struct Command {
    run_id: String,
    op: String,
    #[serde(default)]
    id: String,
    #[serde(default)]
    protobuf: String,
}
#[derive(Serialize)]
struct ResultRow {
    run_id: String,
    op: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    protobuf: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    json: Option<serde_json::Value>,
    #[serde(skip_serializing_if = "std::ops::Not::not")]
    nil: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<String>,
}
impl ResultRow {
    fn preserved(&mut self, wire: &PreservedMessage) -> Result<(), String> {
        self.protobuf = Some(STANDARD.encode(wire.encode_to_vec()));
        self.json = Some(
            serde_json::from_slice(
                &lk_protocol::json::encode_twirp_dynamic(wire.as_dynamic())
                    .map_err(|_| "JSON encode failed")?,
            )
            .map_err(|_| "JSON encode failed")?,
        );
        self.nil = false;
        Ok(())
    }
    fn message<T, U>(&mut self, value: &T) -> Result<(), String>
    where
        T: Message + prost::Name,
        U: Message + Default + Serialize,
    {
        self.protobuf = Some(STANDARD.encode(value.encode_to_vec()));
        self.json = Some(
            serde_json::from_slice(
                &lk_protocol::to_twirp_json::<T, U>(value).map_err(|_| "JSON encode failed")?,
            )
            .map_err(|_| "JSON encode failed")?,
        );
        self.nil = false;
        Ok(())
    }
}
async fn execute(store: &SipCatalog, c: Command) -> Result<ResultRow, String> {
    let mut r = ResultRow {
        run_id: c.run_id,
        op: c.op.clone(),
        protobuf: None,
        json: None,
        nil: true,
        error: None,
    };
    let raw = STANDARD
        .decode(c.protobuf)
        .map_err(|_| "invalid input protobuf base64")?;
    macro_rules! read {
        ($t:ty) => {
            <$t>::decode(raw.as_slice()).map_err(|_| "invalid input protobuf")?
        };
    }
    macro_rules! emit {
        ($value:expr,$t:ident) => {
            r.message::<$t, lk_protocol::twirp_json::livekit::$t>(&$value)?
        };
    }
    let action: Result<(), Box<dyn std::error::Error>> = async {
        match c.op.as_str() {
            "store_legacy" => {
                store
                    .store_legacy_preserved(&Stored::with_wire(
                        read!(SipTrunkInfo),
                        PreservedMessage::decode(
                            &<SipTrunkInfo as prost::Name>::full_name(),
                            &raw,
                        )?,
                    ))
                    .await?
            }
            "store_inbound" => {
                store
                    .store_inbound_preserved(&Stored::with_wire(
                        read!(SipInboundTrunkInfo),
                        PreservedMessage::decode(
                            &<SipInboundTrunkInfo as prost::Name>::full_name(),
                            &raw,
                        )?,
                    ))
                    .await?
            }
            "store_outbound" => {
                store
                    .store_outbound_preserved(&Stored::with_wire(
                        read!(SipOutboundTrunkInfo),
                        PreservedMessage::decode(
                            &<SipOutboundTrunkInfo as prost::Name>::full_name(),
                            &raw,
                        )?,
                    ))
                    .await?
            }
            "store_rule" => {
                store
                    .store_rule_preserved(&Stored::with_wire(
                        read!(SipDispatchRuleInfo),
                        PreservedMessage::decode(
                            &<SipDispatchRuleInfo as prost::Name>::full_name(),
                            &raw,
                        )?,
                    ))
                    .await?
            }
            "load_legacy" => {
                let v = store.load_legacy_preserved(&c.id).await?;
                r.preserved(&v.wire)?;
            }
            "load_inbound" => {
                if let Some(v) = store.load_inbound_preserved(&c.id).await? {
                    r.preserved(&v.wire)?;
                }
            }
            "load_outbound" => {
                if let Some(v) = store.load_outbound_preserved(&c.id).await? {
                    r.preserved(&v.wire)?;
                }
            }
            "load_rule" => {
                let v = store.load_rule_preserved(&c.id).await?;
                r.preserved(&v.wire)?;
            }
            "list_legacy" => {
                let values = store
                    .list_legacy_preserved(&read!(ListSipTrunkRequest))
                    .await?;
                let mut wire = PreservedMessage::from_message(&ListSipTrunkResponse::default());
                wire.replace_repeated("items", values.into_iter().map(|v| v.wire));
                r.preserved(&wire)?;
            }
            "list_inbound" => {
                let values = store
                    .list_inbound_preserved(&read!(ListSipInboundTrunkRequest))
                    .await?;
                let mut wire =
                    PreservedMessage::from_message(&ListSipInboundTrunkResponse::default());
                wire.replace_repeated(
                    "items",
                    values.into_iter().map(|v| {
                        v.map(|v| v.wire).unwrap_or_else(|| {
                            PreservedMessage::from_message(&SipInboundTrunkInfo::default())
                        })
                    }),
                );
                r.preserved(&wire)?;
            }
            "list_outbound" => {
                let values = store
                    .list_outbound_preserved(&read!(ListSipOutboundTrunkRequest))
                    .await?;
                let mut wire =
                    PreservedMessage::from_message(&ListSipOutboundTrunkResponse::default());
                wire.replace_repeated(
                    "items",
                    values.into_iter().map(|v| {
                        v.map(|v| v.wire).unwrap_or_else(|| {
                            PreservedMessage::from_message(&SipOutboundTrunkInfo::default())
                        })
                    }),
                );
                r.preserved(&wire)?;
            }
            "list_rules" => {
                let values = store
                    .list_rules_preserved(&read!(ListSipDispatchRuleRequest))
                    .await?;
                let mut wire =
                    PreservedMessage::from_message(&ListSipDispatchRuleResponse::default());
                wire.replace_repeated("items", values.into_iter().map(|v| v.wire));
                r.preserved(&wire)?;
            }
            "delete_trunk" => store.delete_trunk(&c.id).await?,
            "delete_rule" => store.delete_rule(&c.id).await?,
            "encoding_nil_inbound" => {
                emit!(
                    ListSipInboundTrunkResponse {
                        items: vec![
                            SipInboundTrunkInfo::default(),
                            SipInboundTrunkInfo {
                                sip_trunk_id: "ST_fixture".into(),
                                ..Default::default()
                            }
                        ]
                    },
                    ListSipInboundTrunkResponse
                );
            }
            "encoding_nil_get" => {
                emit!(
                    GetSipInboundTrunkResponse { trunk: None },
                    GetSipInboundTrunkResponse
                );
            }
            _ => return Err(CatalogError::Configuration.into()),
        }
        Ok(())
    }
    .await;
    if let Err(e) = action {
        r.error = Some(
            e.downcast_ref::<CatalogError>()
                .map_or_else(|| e.to_string(), CatalogError::compatible_public_message),
        );
    }
    Ok(r)
}
fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<_> = std::env::args().skip(1).collect();
    let arg = |name: &str| args.windows(2).find(|p| p[0] == name).map(|p| p[1].clone());
    let url = arg("--redis").ok_or("--redis required")?;
    let run = arg("--run-id")
        .filter(|v| !v.is_empty())
        .ok_or("--run-id required")?;
    // Finite batch tool: caller supplies EOF and a process deadline. Whole input
    // is bounded before JSON allocation; no per-line unbounded read_line.
    const LIMIT: usize = 16 << 20;
    let mut input = Vec::new();
    std::io::stdin()
        .take((LIMIT + 1) as u64)
        .read_to_end(&mut input)?;
    if input.len() > LIMIT {
        return Err("input exceeds 16MiB".into());
    }
    let mut commands = Vec::new();
    for line in input.split(|b| *b == b'\n').filter(|l| !l.is_empty()) {
        if line.len() > 4 << 20 || commands.len() >= 4096 {
            return Err("input batch bound exceeded".into());
        }
        let c: Command = serde_json::from_slice(line)?;
        if c.run_id != run {
            return Err("stale run identity".into());
        }
        commands.push(c);
    }
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    runtime.block_on(async {
        tokio::time::timeout(Duration::from_secs(60), async {
            let store = SipCatalog::connect(&url, Duration::from_secs(3), 8).await?;
            let mut output = std::io::BufWriter::new(std::io::stdout());
            for command in commands {
                let row = execute(&store, command)
                    .await
                    .map_err(std::io::Error::other)?;
                serde_json::to_writer(&mut output, &row)?;
                output.write_all(b"\n")?;
            }
            output.flush()?;
            Ok::<_, Box<dyn std::error::Error>>(())
        })
        .await
    })??;
    Ok(())
}
