//! Wire observer for one production pooled Redis command followed by recovery.
use lk_compat::redis_endpoint::RedisEndpoint;
use redis::{AsyncConnectionConfig, IntoConnectionInfo};

#[tokio::main]
async fn main() {
    let address = std::env::args().nth(1).expect("owned Redis proxy URL");
    let endpoint = RedisEndpoint::simple(address.into_connection_info().unwrap());
    let client = endpoint
        .connect(&AsyncConnectionConfig::new())
        .await
        .unwrap();
    for key in ["retry_target", "recovery"] {
        let result: redis::RedisResult<String> = redis::cmd("GET")
            .arg(key)
            .query_async(&mut client.begin())
            .await;
        match result {
            Ok(value) => println!("{}", serde_json::json!({"key": key, "value": value})),
            Err(error) => println!("{}", serde_json::json!({"key": key, "error": error.code()})),
        }
    }
}
