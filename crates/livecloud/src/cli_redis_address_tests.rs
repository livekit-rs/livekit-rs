//! Old API-only consumer: compile/run unchanged before adopting address helper.
//! Coordinator supplies real original-Go observer + CLI observations and retains
//! the same exclusively owned Redis until this test finishes.
use std::path::Path;

#[tokio::test]
#[ignore = "requires actual source/CLI12 receipt and same coordinator-owned Redis"]
async fn actual_original_redis_address_public_consumer() {
    let path = std::env::var_os("LIVEKIT_REDIS_ADDRESS_ACTUAL").expect("actual fixture receipt");
    let bytes = std::fs::read(path).unwrap();
    let report: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
    let rows = report["cases"].as_array().unwrap();
    assert_eq!(rows.len(), 12, "original10 plus2 additive boundaries");
    assert_eq!(report["observer_rows"].as_array().unwrap().len(), 12);
    let expected_ids = [
        "unix-literal-punctuation-db1",
        "unix-missing-real-dial",
        "tcp-ipv6-explicit-db1",
        "tcp-host-missing-port",
        "tcp-missing-bracket",
        "tcp-refused-real-dial",
        "tcp-password-wrong",
        "tcp-acl-user-password",
        "tcp-db0-distinct-nodes",
        "cli-masks-bad-env",
        "additive-negative-db-source-keeps-zero",
        "additive-username-only-no-auth-control",
    ];
    for (row, expected_id) in rows.iter().zip(expected_ids) {
        assert_eq!(row["id"], expected_id);
        assert_eq!(row["source_setup_valid"], true);
        assert_eq!(row["official"]["timed_out"], false);
    }
    let mut failures = Vec::new();
    for row in rows {
        let input = Path::new(row["config_path"].as_str().unwrap());
        let info = lk_config::ProgramInfo {
            binary_name: "livekit-server".into(),
            product_version: "1.13.10".into(),
            compat_version: "1.13.10",
        };
        let mut env = lk_config::Env::default();
        if row["id"] == "cli-masks-bad-env" {
            env = lk_config::Env::from_pairs([("REDIS_HOST", "127.0.0.1")]);
        }
        let mut args = vec![
            "livekit-server".into(),
            "--config".into(),
            input.as_os_str().to_owned(),
        ];
        if let Some(address) = row["cli_redis_host"].as_str() {
            args.extend(["--redis-host".into(), address.into()]);
        }
        args.push("list-nodes".into());
        let lk_config::EntryAction::ListNodes(config) =
            lk_config::parse(args, &env, &info).unwrap()
        else {
            panic!("actual old public loader must select node listing")
        };
        let actual = match crate::cli_nodes::list_nodes(&config).await {
            Ok(output) => (0, output),
            Err(error) => (1, format!("{error}\n")),
        };
        let expected = &row["official"];
        let expected_stdout = expected["stdout"].as_str().unwrap();
        if actual.0 != expected["exit"].as_i64().unwrap() || actual.1 != expected_stdout {
            failures.push(format!(
                "{}: exit {} vs {}, actual {:?}, source {:?}",
                row["id"], actual.0, expected["exit"], actual.1, expected_stdout
            ));
        }
    }
    assert!(
        failures.is_empty(),
        "actual original source public-consumer mismatches: {failures:#?}"
    );
}

/// CONFIG_BEHAVIOR:Redis.LiteralDialAddress real transport consumer (batch 21i,
/// evidence slot `c14-redis-literal-dial-address`). The item's address
/// parse/oracle differential alone is explicitly "not Rust I/O acceptance", so
/// this drives the actual literal-address dial (`dial_once` ->
/// `redis_address::parse` -> real `TcpStream::connect`). It proves the CLI treats
/// the configured value as a literal host:port (not a Redis URL), applies no
/// missing-port `:6379` default, and reaches the real socket layer.
#[tokio::test]
#[ignore = "drives the real literal-address Redis dial (LIVEKIT_REDIS_LITERAL_ADDR, default 127.0.0.1:6379)"]
async fn actual_literal_address_real_transport_consumer() {
    use lk_config::official::RedisConfig;
    async fn read_nodes(address: &str) -> Result<(), crate::cli_nodes::NodeListError> {
        let cfg = RedisConfig {
            address: address.into(),
            ..Default::default()
        };
        crate::cli_nodes::cli_redis_retry::read_nodes(&cfg)
            .await
            .map(|_| ())
    }

    // (a) A bare literal host carries no port and must be rejected before any
    // dial: there is no implicit `:6379` default, matching Go net.SplitHostPort.
    let err = read_nodes("127.0.0.1")
        .await
        .expect_err("bare literal host has no port and must not default to :6379");
    assert!(
        err.to_string().contains("missing port in address"),
        "expected literal missing-port rejection, got {err}"
    );

    // (b) A refused literal host:port must reach the real TCP connect layer
    // (no Redis-URL reinterpretation of the address host).
    let err = read_nodes("127.0.0.1:1")
        .await
        .expect_err("closed literal port must attempt a real TCP dial");
    assert!(
        err.to_string().contains("connection refused"),
        "expected a real TCP connection-refused on a literal host:port, got {err}"
    );

    // (c) Positive real transport: the literal host:port dials a live Redis and
    // completes the Ping + HVALS the node listing performs over the real socket.
    let addr = std::env::var("LIVEKIT_REDIS_LITERAL_ADDR")
        .unwrap_or_else(|_| "127.0.0.1:6379".to_string());
    read_nodes(&addr).await.unwrap_or_else(|err| {
        panic!("live Redis at {addr} must accept a literal host:port dial: {err}")
    });
}
