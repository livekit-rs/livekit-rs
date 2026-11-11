//! Development parser consumer oracle; no server, worker or media is started.
use lk_config::{EntryAction, Env, ProgramInfo, netdetect::NetProbe};
use prost::Message;
use std::io::{self, BufRead, Write};
use std::net::{IpAddr, Ipv4Addr};
use std::time::Duration;
struct LocalProbe;
impl NetProbe for LocalProbe {
    fn local_addresses(&self) -> Vec<IpAddr> {
        vec![Ipv4Addr::LOCALHOST.into()]
    }
    fn external_ip(&self, _: &[String], _: bool) -> Result<IpAddr, String> {
        Err("external network not part of parser oracle".into())
    }
    fn sleep(&self, _: Duration) {}
}
fn one(value: serde_json::Value) -> serde_json::Value {
    let name = value["name"].as_str().unwrap_or("");
    let Some(yaml) = value["yaml"].as_str() else {
        return serde_json::json!({"name":name,"error":"oracle YAML input required"});
    };
    let mut args = vec![
        "auto-probe".into(),
        "--dev".into(),
        "--config-body".into(),
        yaml.into(),
    ];
    if value["strict"] == false {
        args.push("--disable-strict-config".into());
    }
    let info = ProgramInfo {
        binary_name: "auto-probe".into(),
        product_version: "f96-oracle".into(),
        compat_version: "1.13.10",
    };
    match lk_config::parse_with_probe(args, &Env::default(), &info, &LocalProbe) {
        Ok(EntryAction::RunServer(loaded)) => {
            let Some(recording) = loaded
                .server
                .room
                .presets
                .get("record")
                .and_then(|p| p.as_ref())
                .and_then(|p| p.automatic_recording.as_ref())
            else {
                return serde_json::json!({"name":name,"presence":false});
            };
            let message = lk_compat::workers::auto_egress_policy::room_egress_out(recording);
            let dynamic = lk_protocol::json::decode_wire_dynamic(
                "livekit.RoomEgress",
                &message.encode_to_vec(),
            )
            .expect("actual generated RoomEgress known projection");
            let bytes = lk_protocol::json::encode_twirp_dynamic(&dynamic)
                .expect("actual production response encoder");
            let canonical: serde_json::Value =
                serde_json::from_slice(&bytes).expect("actual JSON encoder output");
            serde_json::json!({"name":name,"presence":true,"egress":canonical})
        }
        Err(error) => serde_json::json!({"name":name,"presence":false,"error":error.to_string()}),
        Ok(_) => {
            serde_json::json!({"name":name,"presence":false,"error":"unexpected non-loader action"})
        }
    }
}
fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut out = io::BufWriter::new(io::stdout());
    for line in io::stdin().lock().lines() {
        let line = line?;
        if line.len() > 1024 * 1024 {
            return Err("oracle input exceeded bound".into());
        }
        let value = serde_json::from_str(&line)?;
        serde_json::to_writer(&mut out, &one(value))?;
        out.write_all(b"\n")?;
    }
    out.flush()?;
    Ok(())
}
