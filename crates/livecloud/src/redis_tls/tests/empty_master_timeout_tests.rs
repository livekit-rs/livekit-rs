use super::*;
use std::net::IpAddr;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpListener;
use tokio::task::JoinSet;

struct NoNetwork;

impl lk_config::netdetect::NetProbe for NoNetwork {
    fn local_addresses(&self) -> Vec<IpAddr> {
        vec!["127.0.0.1".parse().unwrap()]
    }

    fn external_ip(&self, _: &[String], _: bool) -> Result<IpAddr, String> {
        panic!("fixture supplies the node address")
    }

    fn sleep(&self, _: Duration) {
        panic!("fixture must not retry discovery")
    }
}

fn configured_endpoint(address: &str, alias: bool, read_ms: i64) -> RedisEndpoint {
    let selection = if alias {
        format!("  sentinel_addresses: ['{address}']\n  sentinel_master_name: ''\n")
    } else {
        format!("  address: '{address}'\n")
    };
    let yaml = format!(
        "rtc:\n  node_ip: 127.0.0.1\nredis:\n{selection}  read_timeout: {read_ms}\n  dial_timeout: 500\n  write_timeout: 500\n"
    );
    let args = vec![
        "livecloud".into(),
        "--dev".into(),
        "--config-body".into(),
        yaml.into(),
    ];
    let info = lk_config::ProgramInfo {
        binary_name: "livecloud".into(),
        product_version: "fixture".into(),
        compat_version: "1.13.10",
    };
    let action =
        lk_config::parse_with_probe(args, &lk_config::Env::default(), &info, &NoNetwork).unwrap();
    let lk_config::EntryAction::RunServer(loaded) = action else {
        panic!("expected ordinary server configuration");
    };
    redis_endpoint(&loaded.official.redis).unwrap()
}

async fn read_command(reader: &mut (impl AsyncBufReadExt + Unpin)) -> Option<Vec<String>> {
    let mut line = String::new();
    if reader.read_line(&mut line).await.unwrap() == 0 {
        return None;
    }
    let count: usize = line.strip_prefix('*').unwrap().trim_end().parse().unwrap();
    let mut args = Vec::with_capacity(count);
    for _ in 0..count {
        line.clear();
        reader.read_line(&mut line).await.unwrap();
        let length: usize = line.strip_prefix('$').unwrap().trim_end().parse().unwrap();
        let mut bytes = vec![0; length + 2];
        reader.read_exact(&mut bytes).await.unwrap();
        assert_eq!(&bytes[length..], b"\r\n");
        bytes.truncate(length);
        args.push(String::from_utf8(bytes).unwrap());
    }
    Some(args)
}

#[tokio::test]
async fn empty_master_single_address_regular_pool_consumes_only_sentinel_branch_timeout() {
    for (alias, read_ms, caller_ms, resp3, timed_out) in [
        (true, 80, 800, false, true),
        (true, 80, 800, true, true),
        (true, 1000, 800, false, false),
        (true, -1, 80, false, false),
        (false, 80, 800, false, false),
    ] {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap().to_string();
        let gets = Arc::new(AtomicUsize::new(0));
        let get_count = gets.clone();
        let peer = tokio::spawn(async move {
            let mut clients = JoinSet::new();
            loop {
                tokio::select! {
                    Some(result) = clients.join_next() => result.unwrap(),
                    Ok((socket, _)) = listener.accept() => {
                        let gets = get_count.clone();
                        clients.spawn(async move {
                            let (read, mut write) = socket.into_split();
                            let mut reader = BufReader::new(read);
                            while let Some(command) = read_command(&mut reader).await {
                                let response: &[u8] = match command[0].to_ascii_uppercase().as_str() {
                                    "CLIENT" => b"+OK\r\n",
                                    "HELLO" => b"%2\r\n+server\r\n+redis\r\n+proto\r\n:3\r\n",
                                    "GET" => {
                                        assert_eq!(command[1], "fixture-key");
                                        gets.fetch_add(1, Ordering::Relaxed);
                                        tokio::time::sleep(Duration::from_millis(250)).await;
                                        b"$7\r\nfixture\r\n"
                                    }
                                    method => panic!("regular endpoint sent unexpected {method}"),
                                };
                                if write.write_all(response).await.is_err() {
                                    return;
                                }
                            }
                        });
                    }
                }
            }
        });

        let endpoint = configured_endpoint(&address, alias, read_ms);
        assert!(!endpoint.is_cluster());
        assert!(endpoint.master_epoch().is_none());
        let endpoint = if resp3 {
            endpoint.with_resp3()
        } else {
            endpoint
        };
        let config = redis::AsyncConnectionConfig::new()
            .set_response_timeout(Some(Duration::from_millis(caller_ms)));
        let pool = endpoint.connect(&config).await.unwrap();
        let result = tokio::time::timeout(
            Duration::from_secs(2),
            redis::cmd("GET")
                .arg("fixture-key")
                .query_async::<String>(&mut pool.begin()),
        )
        .await
        .expect("test watchdog");
        if timed_out {
            let error = result.expect_err("selected timeout must affect real GET");
            assert!(error.is_timeout(), "{error}");
        } else {
            assert_eq!(result.unwrap(), "fixture");
        }
        assert!(
            gets.load(Ordering::Relaxed) > 0,
            "actual GET reached ordinary Redis"
        );
        drop(pool);
        peer.abort();
        assert!(peer.await.unwrap_err().is_cancelled());
    }
}

#[tokio::test]
#[ignore = "requires three test-owned Redis primaries from redis-scoped/run-cluster.py"]
async fn empty_master_multiple_addresses_apply_cluster_node_read_timeout_only_from_sentinel_branch()
{
    let urls: Vec<String> = serde_json::from_str(
        &std::env::var("LIVEKIT_EMPTY_MASTER_CLUSTER_URLS").expect("owned fixture URLs"),
    )
    .unwrap();
    assert_eq!(urls.len(), 3);
    let addresses: Vec<_> = urls
        .iter()
        .map(|url| url.strip_prefix("redis://").unwrap())
        .collect();
    let mut node_ids = std::collections::BTreeSet::new();
    let mut controls = Vec::new();
    for url in &urls {
        let client = redis::Client::open(url.as_str()).unwrap();
        let mut connection = client.get_multiplexed_async_connection().await.unwrap();
        let id: String = redis::cmd("CLUSTER")
            .arg("MYID")
            .query_async(&mut connection)
            .await
            .unwrap();
        let info: String = redis::cmd("CLUSTER")
            .arg("INFO")
            .query_async(&mut connection)
            .await
            .unwrap();
        assert!(
            info.contains("cluster_state:ok")
                && info.contains("cluster_slots_assigned:16384")
                && info.contains("cluster_size:3")
        );
        assert!(
            node_ids.insert(id),
            "all three real Redis primary identities are distinct"
        );
        controls.push(connection);
    }

    let rows = [
        (true, 80, false, true),
        (true, 80, true, true),
        (true, 2500, false, false),
        (true, -1, false, false),
        (false, 80, false, false),
    ];
    for (index, (alias, read_ms, resp3, should_timeout)) in rows.into_iter().enumerate() {
        let selector = if alias {
            "sentinel_addresses"
        } else {
            "cluster_addresses"
        };
        let yaml = format!(
            "rtc:\n  node_ip: 127.0.0.1\nredis:\n  {selector}: ['{}']\n  sentinel_master_name: ''\n  read_timeout: {read_ms}\n  dial_timeout: 600\n  write_timeout: 750\n",
            addresses.join("', '")
        );
        let args = vec![
            "livecloud".into(),
            "--dev".into(),
            "--config-body".into(),
            yaml.into(),
        ];
        let info = lk_config::ProgramInfo {
            binary_name: "livecloud".into(),
            product_version: "fixture".into(),
            compat_version: "1.13.10",
        };
        let lk_config::EntryAction::RunServer(loaded) =
            lk_config::parse_with_probe(args, &lk_config::Env::default(), &info, &NoNetwork)
                .unwrap()
        else {
            panic!("expected Cluster config");
        };
        let endpoint = redis_endpoint(&loaded.official.redis).unwrap();
        assert!(endpoint.is_cluster());
        assert!(
            endpoint.master_epoch().is_none(),
            "alias must not use Sentinel discovery"
        );
        let endpoint = if resp3 {
            endpoint.with_resp3()
        } else {
            endpoint
        };
        let pool = endpoint
            .connect(&redis::AsyncConnectionConfig::new())
            .await
            .unwrap();

        let key = format!("timeout-observer-rust:{{owned-{index}}}");
        let slot: u16 = redis::cmd("CLUSTER")
            .arg("KEYSLOT")
            .arg(&key)
            .query_async(&mut controls[0])
            .await
            .unwrap();
        let slots: Vec<(u16, u16, Vec<redis::Value>)> = redis::cmd("CLUSTER")
            .arg("SLOTS")
            .query_async(&mut controls[0])
            .await
            .unwrap();
        let (_, _, owner) = slots
            .iter()
            .find(|(start, end, _)| (*start..=*end).contains(&slot))
            .unwrap();
        let port: u16 = redis::from_redis_value(owner[1].clone()).unwrap();
        let master = addresses
            .iter()
            .position(|address| address.ends_with(&format!(":{port}")))
            .unwrap();
        let paused: String = redis::cmd("CLIENT")
            .arg("PAUSE")
            .arg(1400)
            .arg("WRITE")
            .query_async(&mut controls[master])
            .await
            .unwrap();
        assert_eq!(paused, "OK");
        let start = std::time::Instant::now();
        let result = tokio::time::timeout(
            Duration::from_secs(5),
            redis::cmd("SET")
                .arg(&key)
                .arg("fixture")
                .query_async::<String>(&mut pool.begin()),
        )
        .await;
        let elapsed = start.elapsed();
        let unpaused: String = redis::cmd("CLIENT")
            .arg("UNPAUSE")
            .query_async(&mut controls[master])
            .await
            .unwrap();
        assert_eq!(unpaused, "OK");
        let result = result.expect("test watchdog is not the expected node read timeout");
        if should_timeout {
            let error = result.expect_err("the selected node's shorter read budget must fire");
            assert!(error.is_timeout(), "alias={alias} RESP3={resp3}: {error}");
            assert!(
                elapsed < Duration::from_millis(1400),
                "command exhausted pause: {elapsed:?}"
            );
        } else {
            assert_eq!(result.unwrap(), "OK");
            assert!(
                elapsed >= Duration::from_millis(1200),
                "master did not pause writes: {elapsed:?}"
            );
            let received: String = redis::cmd("GET")
                .arg(&key)
                .query_async(&mut pool.begin())
                .await
                .unwrap();
            assert_eq!(received, "fixture");
        }
        eprintln!(
            "alias={alias} read_ms={read_ms} RESP3={resp3} elapsed={elapsed:?} expected_timeout={should_timeout}"
        );
    }
}

#[tokio::test]
#[ignore = "requires the owned three-peer Redis Cluster wire-fault fixture"]
async fn empty_master_cluster_write_and_verified_discovered_tls_dial_use_wire_deadlines() {
    let _ = rtc_tls::crypto_provider().install_default();
    let yaml = std::env::var("LIVEKIT_CLUSTER_WIRE_FAULT_YAML").expect("owned fixture YAML");
    let case = std::env::var("LIVEKIT_CLUSTER_WIRE_FAULT_CASE").expect("owned fixture case");
    let key = std::env::var("LIVEKIT_CLUSTER_WIRE_FAULT_KEY").expect("owned fixture key");
    let args = vec![
        "livecloud".into(),
        "--dev".into(),
        "--config-body".into(),
        yaml.into(),
    ];
    let info = lk_config::ProgramInfo {
        binary_name: "livecloud".into(),
        product_version: "fixture".into(),
        compat_version: "1.13.10",
    };
    let lk_config::EntryAction::RunServer(loaded) =
        lk_config::parse_with_probe(args, &lk_config::Env::default(), &info, &NoNetwork).unwrap()
    else {
        panic!("expected Cluster config");
    };
    let cfg = &loaded.official.redis;
    assert_eq!(
        cfg.sentinel_addresses.len(),
        2,
        "third slot owner must be discovered"
    );
    assert!(cfg.sentinel_master_name.is_empty());
    let endpoint = redis_endpoint(cfg).unwrap();
    assert!(endpoint.is_cluster());
    assert!(endpoint.master_epoch().is_none());

    let result = tokio::time::timeout(Duration::from_secs(8), async {
        let pool = endpoint
            .connect(&redis::AsyncConnectionConfig::new())
            .await?;
        let payload = if case.starts_with("write_") {
            vec![b'x'; 16 << 20]
        } else {
            b"verified-peer".to_vec()
        };
        let response = redis::cmd("SET")
            .arg(&key)
            .arg(payload)
            .query_async::<String>(&mut pool.begin())
            .await;
        if case == "write_short" {
            let error = response.expect_err("partial writer must not succeed");
            assert!(
                error.is_timeout() && error.to_string().contains("Redis socket write timed out"),
                "{error}"
            );
            let recovery: String = redis::cmd("SET")
                .arg(&key)
                .arg("recover-after-timeout")
                .query_async(&mut pool.begin())
                .await?;
            assert_eq!(recovery, "OK");
            let exists: u64 = redis::cmd("EXISTS")
                .arg(&key)
                .query_async(&mut pool.begin())
                .await?;
            return Ok((recovery, exists));
        }
        let response = response?;
        let exists: u64 = redis::cmd("EXISTS")
            .arg(&key)
            .query_async(&mut pool.begin())
            .await?;
        Ok::<_, redis::RedisError>((response, exists))
    })
    .await
    .expect("owned test watchdog is not a production outcome");

    match case.as_str() {
        "tls_short" => {
            let error = result.expect_err("a stalled discovered TLS owner must not succeed");
            eprintln!("discovered TLS dial failed: {error}");
        }
        "write_short" | "write_long" | "tls_long" => {
            assert_eq!(result.unwrap(), ("OK".to_string(), 1));
        }
        _ => panic!("unknown fixture case {case}"),
    }
}
