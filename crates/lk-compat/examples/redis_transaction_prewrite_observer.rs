//! Actual owned RedisStore-compatible deletion with configured write deadline.
use lk_compat::{
    redis_endpoint::{RedisEndpoint, SentinelTarget, TlsSniOverride},
    workers::store::IoStore,
};
use lk_protocol::livekit::{EgressInfo, IngressInfo};
use redis::IntoConnectionInfo;
use std::{sync::Arc, time::Duration};

#[tokio::main]
async fn main() {
    let _ = rtc_tls::crypto_provider().install_default();
    let mut args = std::env::args().skip(1);
    let address = args.next().expect("owned Redis proxy URL");
    let room_bytes = args.next().expect("room byte count").parse().unwrap();
    let topology = args.next();
    let tls = args.next().map(|root| TlsSniOverride {
        server_name: "localhost".into(),
        root_pem: Some(std::fs::read(root).unwrap()),
        client_cert_pem: None,
        client_key_pem: None,
    });
    let scheme = if tls.is_some() { "rediss" } else { "redis" };
    let endpoint = match topology.as_deref() {
        None => RedisEndpoint::simple_from_sentinel_branch(
            address.into_connection_info().unwrap(),
            tls,
            1000,
            2000,
            180,
        ),
        Some("cluster") => RedisEndpoint::cluster_from_sentinel_branch(
            address
                .split(',')
                .map(|addr| addr.into_connection_info().unwrap())
                .collect(),
            tls,
            1000,
            2000,
            180,
        ),
        Some("sentinel") => {
            let info = move |address: &str| {
                format!("{scheme}://{address}")
                    .into_connection_info()
                    .map_err(|error| error.to_string())
            };
            let target = SentinelTarget {
                master_name: "transaction-fixture".into(),
                addresses: vec![address],
                sentinel_username: String::new(),
                sentinel_password: String::new(),
                sentinel_info: Arc::new(info),
                master_info: Arc::new(move |address| info(&address)),
                discovery_dial_timeout: Duration::from_secs(1),
                discovery_read_timeout: Duration::from_secs(2),
                write_timeout: Some(Duration::from_millis(180)),
            };
            match tls {
                Some(tls) => RedisEndpoint::sentinel_with_tls_sni(target, tls),
                None => RedisEndpoint::sentinel(target),
            }
        }
        Some(_) => panic!("unknown Redis topology"),
    };
    let store = IoStore::connect(endpoint, Duration::from_secs(5))
        .await
        .unwrap();
    let result = store
        .delete_ingress(&IngressInfo {
            ingress_id: "IN_fixture".into(),
            room_name: "p".repeat(room_bytes),
            stream_key: "stream".into(),
            ..Default::default()
        })
        .await;
    if let Some(gate) = std::env::var_os("LK_REDIS_RECOVERY_GATE") {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(25);
        while !tokio::fs::try_exists(&gate).await.unwrap() {
            assert!(
                tokio::time::Instant::now() < deadline,
                "fixture did not finish Redis transition"
            );
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }
    let recovery = store
        .store_egress(&EgressInfo {
            egress_id: "EG_recovery".into(),
            room_name: "recovery".into(),
            ..Default::default()
        })
        .await;
    println!(
        "{}",
        serde_json::json!({
            "success": result.is_ok(),
            "recovery": recovery.is_ok(),
            "error": result.err().map(|error| error.message),
        })
    );
}
