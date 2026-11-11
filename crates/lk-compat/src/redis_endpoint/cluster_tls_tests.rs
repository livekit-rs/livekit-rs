//! Real coordinator-owned three-master TLS Cluster campaign. No test server is
//! started here; the paired runner owns private keys, processes and cleanup.
use super::*;
use redis::IntoConnectionInfo;

fn fixture() -> (Vec<ConnectionInfo>, TlsSniOverride) {
    // Matches livecloud::run_cli before any redis-rs TLS dial. No ring/aws-lc.
    let _ = rtc_tls::crypto_provider().install_default();
    let addresses = std::env::var("LIVEKIT_TLS_CLUSTER_NODES").expect("owned TLS Cluster nodes");
    let nodes = addresses
        .split(',')
        .map(|address| address.into_connection_info().unwrap())
        .collect::<Vec<_>>();
    assert_eq!(nodes.len(), 3, "dedicated three-master campaign");
    let read = |name| std::fs::read(std::env::var(name).expect(name)).expect("fixture TLS bytes");
    let tls = TlsSniOverride {
        server_name: std::env::var("LIVEKIT_TLS_CLUSTER_NAME").expect("DNS-only server identity"),
        root_pem: Some(read("LIVEKIT_TLS_CLUSTER_CA")),
        client_cert_pem: Some(read("LIVEKIT_TLS_CLUSTER_CLIENT_CERT")),
        client_key_pem: Some(read("LIVEKIT_TLS_CLUSTER_CLIENT_KEY")),
    };
    (nodes, tls)
}
fn config() -> AsyncConnectionConfig {
    AsyncConnectionConfig::new()
        .set_connection_timeout(Some(Duration::from_secs(3)))
        .set_response_timeout(Some(Duration::from_secs(3)))
}
fn string(value: &Value) -> String {
    match value {
        Value::BulkString(value) => String::from_utf8(value.clone()).unwrap(),
        Value::SimpleString(value) => value.clone(),
        _ => panic!("expected string: {value:?}"),
    }
}
#[tokio::test]
#[ignore = "requires owned three-master DNS-only TLS/mTLS Cluster from run-owned.py"]
async fn real_cluster_tls_identity_slots_ask_moved_and_rejections() {
    let (nodes, tls) = fixture();
    // All dials use numeric hosts; the public fixture certificate has only DNS
    // SAN. These successes require SNI override on discovered node sockets too.
    for node in &nodes {
        assert!(
            matches!(node.addr(), ConnectionAddr::TcpTls { host, insecure:false, .. } if host.parse::<std::net::IpAddr>().is_ok())
        );
    }
    let endpoint = RedisEndpoint::cluster_with_tls_sni(vec![nodes[0].clone()], 8, tls.clone());
    let mut connection = endpoint
        .connect(&config())
        .await
        .expect("verified mTLS seed/discovery");
    for index in 0..64 {
        let key = format!("c12:slot:{index}");
        let _: () = redis::cmd("SET")
            .arg(&key)
            .arg(index)
            .query_async(&mut connection)
            .await
            .unwrap();
        let got: u64 = redis::cmd("GET")
            .arg(&key)
            .query_async(&mut connection)
            .await
            .unwrap();
        assert_eq!(got, index);
        let _: u64 = redis::cmd("DEL")
            .arg(key)
            .query_async(&mut connection)
            .await
            .unwrap();
    }
    let mut direct = Vec::new();
    for info in &nodes {
        direct.push(
            RedisEndpoint::simple_with_tls_sni(info.clone(), tls.clone())
                .dial(&config())
                .await
                .unwrap(),
        );
    }
    let source: String = redis::cmd("CLUSTER")
        .arg("MYID")
        .query_async(&mut direct[0])
        .await
        .unwrap();
    let destination: String = redis::cmd("CLUSTER")
        .arg("MYID")
        .query_async(&mut direct[1])
        .await
        .unwrap();
    let slots: Vec<Value> = redis::cmd("CLUSTER")
        .arg("SLOTS")
        .query_async(&mut direct[0])
        .await
        .unwrap();
    let (first, last) = slots
        .iter()
        .find_map(|value| {
            let Value::Array(row) = value else {
                return None;
            };
            let (Value::Int(first), Value::Int(last), Value::Array(owner)) =
                (&row[0], &row[1], &row[2])
            else {
                return None;
            };
            (string(&owner[2]) == source).then_some((*first as u16, *last as u16))
        })
        .expect("seed owns a slot range");
    let mut selected = None;
    for index in 0..1024 {
        let key = format!("c12:redirect:{{owned-{index}}}");
        let slot: u16 = redis::cmd("CLUSTER")
            .arg("KEYSLOT")
            .arg(&key)
            .query_async(&mut direct[0])
            .await
            .unwrap();
        if (first..=last).contains(&slot) {
            selected = Some((key, slot));
            break;
        }
    }
    let (key, slot) = selected.expect("find source-owned key");
    let missing: Option<String> = redis::cmd("GET")
        .arg(&key)
        .query_async(&mut connection)
        .await
        .unwrap();
    assert!(missing.is_none()); // prime the connection's original slot authority
    let _: () = redis::cmd("CLUSTER")
        .arg("SETSLOT")
        .arg(slot)
        .arg("IMPORTING")
        .arg(&source)
        .query_async(&mut direct[1])
        .await
        .unwrap();
    let _: () = redis::cmd("CLUSTER")
        .arg("SETSLOT")
        .arg(slot)
        .arg("MIGRATING")
        .arg(&destination)
        .query_async(&mut direct[0])
        .await
        .unwrap();
    let _: () = redis::cmd("ASKING")
        .query_async(&mut direct[1])
        .await
        .unwrap();
    let _: () = redis::cmd("SET")
        .arg(&key)
        .arg("ask-value")
        .query_async(&mut direct[1])
        .await
        .unwrap();
    let ask = redis::cmd("GET")
        .arg(&key)
        .query_async::<String>(&mut direct[0])
        .await
        .unwrap_err();
    assert_eq!(
        ask.code(),
        Some("ASK"),
        "actual source must return an ASK redirect"
    );
    let got: String = redis::cmd("GET")
        .arg(&key)
        .query_async(&mut connection)
        .await
        .expect("actual ASKING+TLS redirect to numeric target");
    assert_eq!(got, "ask-value");
    for node in &mut direct {
        let _: () = redis::cmd("CLUSTER")
            .arg("SETSLOT")
            .arg(slot)
            .arg("NODE")
            .arg(&destination)
            .query_async(node)
            .await
            .unwrap();
    }
    let moved = redis::cmd("GET")
        .arg(&key)
        .query_async::<String>(&mut direct[0])
        .await
        .unwrap_err();
    assert_eq!(
        moved.code(),
        Some("MOVED"),
        "actual source must return MOVED"
    );
    let got: String = redis::cmd("GET")
        .arg(&key)
        .query_async(&mut connection)
        .await
        .expect("actual MOVED+verified TLS target");
    assert_eq!(got, "ask-value");
    let _: u64 = redis::cmd("DEL")
        .arg(&key)
        .query_async(&mut connection)
        .await
        .unwrap();
    // This fixture is owned. Restore topology so the same runner can perform
    // negative checks without leaving a half-migrated slot behind.
    for node in &mut direct {
        let _: () = redis::cmd("CLUSTER")
            .arg("SETSLOT")
            .arg(slot)
            .arg("NODE")
            .arg(&source)
            .query_async(node)
            .await
            .unwrap();
    }
    let mut wrong_name = tls.clone();
    wrong_name.server_name = "wrong.fixture.invalid".into();
    assert!(
        RedisEndpoint::cluster_with_tls_sni(vec![nodes[0].clone()], 2, wrong_name)
            .connect(&config())
            .await
            .is_err(),
        "wrong peer name must fail"
    );
    let mut missing_client = tls.clone();
    missing_client.client_cert_pem = None;
    missing_client.client_key_pem = None;
    assert!(
        RedisEndpoint::cluster_with_tls_sni(vec![nodes[0].clone()], 2, missing_client)
            .connect(&config())
            .await
            .is_err(),
        "mTLS server must reject missing client identity"
    );
    let mut wrong_ca = tls;
    wrong_ca.root_pem = Some(
        std::fs::read(std::env::var("LIVEKIT_TLS_CLUSTER_WRONG_CA").expect("untrusted CA fixture"))
            .unwrap(),
    );
    assert!(
        RedisEndpoint::cluster_with_tls_sni(vec![nodes[0].clone()], 2, wrong_ca)
            .connect(&config())
            .await
            .is_err(),
        "untrusted server CA must fail"
    );
}

#[tokio::test]
#[ignore = "requires run-bad-peer.py: node1 has trusted CA but wrong DNS identity"]
async fn real_cluster_tls_rejects_bad_identity_on_discovery_ask_and_moved() {
    let (nodes, tls) = fixture();
    let mut admins = Vec::new();
    for (index, node) in nodes.iter().enumerate() {
        let mut identity = tls.clone();
        if index == 1 {
            identity.server_name = "wrong-peer.fixture.test".into();
        }
        admins.push(
            RedisEndpoint::simple_with_tls_sni(node.clone(), identity)
                .dial(&config())
                .await
                .expect("fixture admin verifies each actual certificate name"),
        );
    }
    let source: String = redis::cmd("CLUSTER")
        .arg("MYID")
        .query_async(&mut admins[0])
        .await
        .unwrap();
    let destination: String = redis::cmd("CLUSTER")
        .arg("MYID")
        .query_async(&mut admins[1])
        .await
        .unwrap();
    let slots: Vec<Value> = redis::cmd("CLUSTER")
        .arg("SLOTS")
        .query_async(&mut admins[0])
        .await
        .unwrap();
    let ranges = slots
        .iter()
        .filter_map(|value| {
            let Value::Array(row) = value else {
                return None;
            };
            let (Value::Int(first), Value::Int(last), Value::Array(owner)) =
                (&row[0], &row[1], &row[2])
            else {
                return None;
            };
            Some((string(&owner[2]), *first as u16, *last as u16))
        })
        .collect::<Vec<_>>();
    let find = |id: &str| {
        ranges
            .iter()
            .find(|(node, _, _)| node == id)
            .map(|(_, first, last)| (*first, *last))
            .unwrap()
    };
    let source_range = find(&source);
    let destination_range = find(&destination);
    let mut source_key = None;
    let mut destination_key = None;
    for index in 0..4096 {
        let key = format!("c12:bad-peer:{{owned-{index}}}");
        let slot: u16 = redis::cmd("CLUSTER")
            .arg("KEYSLOT")
            .arg(&key)
            .query_async(&mut admins[0])
            .await
            .unwrap();
        if (source_range.0..=source_range.1).contains(&slot) && source_key.is_none() {
            source_key = Some((key.clone(), slot));
        }
        if (destination_range.0..=destination_range.1).contains(&slot) && destination_key.is_none()
        {
            destination_key = Some(key);
        }
        if source_key.is_some() && destination_key.is_some() {
            break;
        }
    }
    let (key, slot) = source_key.unwrap();
    let destination_key = destination_key.unwrap();
    let endpoint = RedisEndpoint::cluster_with_tls_sni(vec![nodes[0].clone()], 2, tls.clone());
    let mut connection = endpoint
        .connect(&config())
        .await
        .expect("valid seed and valid source slots remain usable");
    let missing: Option<String> = redis::cmd("GET")
        .arg(&key)
        .query_async(&mut connection)
        .await
        .unwrap();
    assert!(missing.is_none());
    // A valid seed is not enough: the CLUSTER SLOTS peer carries another DNS
    // identity under the same trusted CA and must not be accepted as fallback.
    assert!(
        RedisEndpoint::simple_with_tls_sni(nodes[1].clone(), tls)
            .dial(&config())
            .await
            .is_err()
    );
    let discovered = redis::cmd("GET")
        .arg(&destination_key)
        .query_async::<Option<String>>(&mut connection)
        .await;
    assert!(
        discovered.is_err(),
        "a discovered node with wrong certificate identity must be rejected"
    );
    let _: () = redis::cmd("CLUSTER")
        .arg("SETSLOT")
        .arg(slot)
        .arg("IMPORTING")
        .arg(&source)
        .query_async(&mut admins[1])
        .await
        .unwrap();
    let _: () = redis::cmd("CLUSTER")
        .arg("SETSLOT")
        .arg(slot)
        .arg("MIGRATING")
        .arg(&destination)
        .query_async(&mut admins[0])
        .await
        .unwrap();
    let _: () = redis::cmd("ASKING")
        .query_async(&mut admins[1])
        .await
        .unwrap();
    let _: () = redis::cmd("SET")
        .arg(&key)
        .arg("must-not-read")
        .query_async(&mut admins[1])
        .await
        .unwrap();
    let ask = redis::cmd("GET")
        .arg(&key)
        .query_async::<String>(&mut admins[0])
        .await
        .unwrap_err();
    assert_eq!(ask.code(), Some("ASK"));
    assert!(
        redis::cmd("GET")
            .arg(&key)
            .query_async::<String>(&mut connection)
            .await
            .is_err(),
        "ASK target must retain configured peer-name verification"
    );
    for node in &mut admins {
        let _: () = redis::cmd("CLUSTER")
            .arg("SETSLOT")
            .arg(slot)
            .arg("NODE")
            .arg(&destination)
            .query_async(node)
            .await
            .unwrap();
    }
    let moved = redis::cmd("GET")
        .arg(&key)
        .query_async::<String>(&mut admins[0])
        .await
        .unwrap_err();
    assert_eq!(moved.code(), Some("MOVED"));
    assert!(
        redis::cmd("GET")
            .arg(&key)
            .query_async::<String>(&mut connection)
            .await
            .is_err(),
        "MOVED target must retain configured peer-name verification"
    );
    let _: u64 = redis::cmd("DEL")
        .arg(&key)
        .query_async(&mut admins[1])
        .await
        .unwrap();
    for node in &mut admins {
        let _: () = redis::cmd("CLUSTER")
            .arg("SETSLOT")
            .arg(slot)
            .arg("NODE")
            .arg(&source)
            .query_async(node)
            .await
            .unwrap();
    }
}
