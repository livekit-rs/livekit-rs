//! Compare the standalone server's clustered command connection with fixed Go.
use lk_compat::redis_endpoint::RedisEndpoint;
use redis::{AsyncConnectionConfig, IntoConnectionInfo};
use serde_json::json;
use std::io::{self, BufRead};
use std::time::Instant;

#[tokio::main]
async fn main() {
    let args: Vec<_> = std::env::args().skip(1).collect();
    let [addresses, operation, key, field] = args.as_slice() else {
        panic!("expected addresses, command, key and field");
    };
    let nodes = addresses
        .split(',')
        .map(|address| format!("redis://{address}").into_connection_info().unwrap())
        .collect();
    let client = RedisEndpoint::cluster(nodes, 3)
        .connect(&AsyncConnectionConfig::new())
        .await
        .expect("owned Cluster");
    let warm: String = redis::cmd("HGET")
        .arg(key)
        .arg(field)
        .query_async(&mut client.begin())
        .await
        .expect("warm HGET");
    assert_eq!(warm, "fixture");
    println!("{}", json!({"phase": "warm", "value": warm}));
    let gate = io::stdin().lock().lines().next().expect("fault barrier");
    gate.expect("read fault barrier");
    let started = Instant::now();
    let mut command = redis::cmd(operation);
    command.arg(key).arg(field);
    let result = match operation.as_str() {
        "HGET" => command
            .query_async::<String>(&mut client.begin())
            .await
            .map(|value| json!(value)),
        "HDEL" => command
            .query_async::<i64>(&mut client.begin())
            .await
            .map(|value| json!(value)),
        _ => panic!("unknown command"),
    };
    let elapsed_ms = started.elapsed().as_millis();
    match result {
        Ok(value) => println!(
            "{}",
            json!({"phase": "request", "elapsed_ms": elapsed_ms, "value": value})
        ),
        Err(error) => println!(
            "{}",
            json!({"phase": "request", "elapsed_ms": elapsed_ms, "error": error.to_string()})
        ),
    }
}
