//! Observe actual production RedisStore-compatible Egress/Ingress methods.
use lk_compat::{redis_endpoint::RedisEndpoint, workers::store::IoStore};
use lk_protocol::livekit::{EgressInfo, IngressInfo};
use redis::IntoConnectionInfo;

#[tokio::main]
async fn main() {
    let mut args = std::env::args().skip(1);
    let address = args.next().expect("owned Redis proxy URL");
    let operation = args.next().expect("operation");
    let endpoint = RedisEndpoint::simple(address.into_connection_info().unwrap());
    let store = IoStore::connect(endpoint, std::time::Duration::from_secs(5))
        .await
        .unwrap();
    let result = match operation.as_str() {
        "egress" => {
            store
                .store_egress(&EgressInfo {
                    egress_id: "EG_fixture".into(),
                    room_name: "fixture".into(),
                    ..Default::default()
                })
                .await
        }
        "ingress" => {
            store
                .delete_ingress(&IngressInfo {
                    ingress_id: "IN_fixture".into(),
                    room_name: "fixture".into(),
                    stream_key: "stream".into(),
                    ..Default::default()
                })
                .await
        }
        _ => panic!("unrecognized operation"),
    };
    let recovery = if operation == "ingress" {
        Some(
            store
                .store_egress(&EgressInfo {
                    egress_id: "EG_recovery".into(),
                    room_name: "recovery".into(),
                    ..Default::default()
                })
                .await
                .is_ok(),
        )
    } else {
        None
    };
    println!(
        "{}",
        serde_json::json!({
            "success": result.is_ok(),
            "recovery": recovery,
            "error": result.err().map(|error| error.message),
        })
    );
}
