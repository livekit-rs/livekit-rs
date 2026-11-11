//! Actual production handler, real socket-free RuntimeDeps, actual fixed Go21.
//! No router replacement, mock response, listener, worker, or room actor.
use super::*;
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

struct LocalProbe;
impl lk_config::netdetect::NetProbe for LocalProbe {
    fn local_addresses(&self) -> Vec<std::net::IpAddr> {
        vec![std::net::Ipv4Addr::LOCALHOST.into()]
    }
    fn external_ip(&self, _: &[String], _: bool) -> Result<std::net::IpAddr, String> {
        panic!("handler-only config must not discover external IP")
    }
    fn sleep(&self, _: std::time::Duration) {
        panic!("handler-only config must not retry discovery")
    }
}

#[tokio::test]
async fn actual_handler_matches_fixed_go21_for_all_17_wire_headers() {
    let cases: Vec<serde_json::Value> =
        serde_json::from_str(include_str!("metrics_auth_fixtures/cases.json")).unwrap();
    let rows: Vec<serde_json::Value> =
        include_str!("metrics_auth_fixtures/fixed-go21-actual.jsonl")
            .lines()
            .map(|line| serde_json::from_str(line).unwrap())
            .collect();
    let case_ids: BTreeSet<_> = cases
        .iter()
        .map(|case| case["id"].as_str().unwrap())
        .collect();
    let source: BTreeMap<_, _> = rows
        .iter()
        .map(|row| (row["id"].as_str().unwrap(), row))
        .collect();
    assert_eq!(cases.len(), 21);
    assert_eq!(rows.len(), 21);
    assert_eq!(case_ids.len(), 21);
    assert_eq!(source.len(), 21);
    assert_eq!(case_ids, source.keys().copied().collect());
    let info = lk_config::ProgramInfo {
        binary_name: "livecloud".into(),
        product_version: "test".into(),
        compat_version: "1.13.10",
    };
    let args = ["livecloud", "--dev", "--node-ip", "127.0.0.1"]
        .into_iter()
        .map(Into::into)
        .collect();
    let config =
        match lk_config::parse_with_probe(args, &lk_config::Env::default(), &info, &LocalProbe)
            .unwrap()
        {
            lk_config::EntryAction::RunServer(loaded) => loaded.server,
            other => panic!("expected real normalized configuration: {other:?}"),
        };
    let net = rtc_runtime::net::NetRouter::start(rtc_runtime::net::NetConfig {
        udp_enabled: false,
        udp_port: None,
        port_range: (0, 0),
        tcp_port: None,
        interfaces: vec![],
    })
    .await
    .unwrap();
    let manager = RoomManager::new(rtc_runtime::RuntimeDeps {
        config: Arc::new(config),
        net: net.clone(),
        events: Arc::new(rtc_core::events::NullSink),
        data: Arc::new(lk_compat::data::LiveKitDataHandler::new(Arc::new(
            lk_compat::rpc::RpcRegistry::new(),
        ))),
    });
    assert_eq!(manager.node_stats().rooms, 0);
    let mut observed = BTreeSet::new();
    for case in &cases {
        if case["wire_http"] != true {
            continue;
        }
        let id = case["id"].as_str().unwrap();
        assert!(observed.insert(id), "duplicate wire case {id}");
        let expected = source[id];
        let mut headers = HeaderMap::new();
        if let Some(values) = case
            .get("header_values")
            .and_then(serde_json::Value::as_array)
        {
            for value in values {
                headers.append(
                    header::AUTHORIZATION,
                    axum::http::HeaderValue::from_bytes(value.as_str().unwrap().as_bytes())
                        .unwrap(),
                );
            }
        } else if let Some(value) = case["authorization"].as_str() {
            headers.append(
                header::AUTHORIZATION,
                axum::http::HeaderValue::from_bytes(value.as_bytes()).unwrap(),
            );
        }
        // Call the real endpoint, including its actual authorization, registry
        // construction, node_stats consumer, text encoder and response builder.
        let response = metrics(
            State(MetricsState {
                manager: manager.clone(),
                node_id: "owned-handler-node".into(),
                basic_auth: Some((
                    case["username"].as_str().unwrap().into(),
                    case["password"].as_str().unwrap().into(),
                )),
            }),
            headers,
        )
        .await;
        assert_eq!(
            u64::from(response.status().as_u16()),
            expected["status"].as_u64().unwrap(),
            "status {id}"
        );
        assert_eq!(
            response
                .headers()
                .get(header::WWW_AUTHENTICATE)
                .map(|h| h.to_str().unwrap())
                .unwrap_or(""),
            expected["www_authenticate"].as_str().unwrap(),
            "challenge {id}"
        );
        let content_type = response
            .headers()
            .get(header::CONTENT_TYPE)
            .map(|h| h.to_str().unwrap())
            .unwrap_or("")
            .to_owned();
        let body = axum::body::to_bytes(response.into_body(), 1 << 20)
            .await
            .unwrap();
        if expected["downstream_called"].as_bool().unwrap() {
            assert_eq!(
                content_type, "application/openmetrics-text; version=1.0.0; charset=utf-8",
                "real metrics type {id}"
            );
            let text = std::str::from_utf8(&body).unwrap();
            assert!(
                text.contains("# HELP livekit_room_total Rooms on this node"),
                "actual registry HELP {id}"
            );
            assert!(
                text.contains("livekit_room_total{node_id=\"owned-handler-node\"} 0"),
                "actual zero-room node_stats {id}"
            );
            assert!(
                text.ends_with("# EOF\n"),
                "complete actual OpenMetrics {id}"
            );
        } else {
            assert_eq!(
                content_type,
                expected["content_type"].as_str().unwrap(),
                "401 content type {id}"
            );
            assert_eq!(
                body.len() as u64,
                expected["body_length"].as_u64().unwrap(),
                "401 body length {id}"
            );
            assert!(body.is_empty(), "401 body remains empty {id}");
        }
    }
    assert_eq!(observed.len(), 17);
    assert_eq!(
        observed,
        cases
            .iter()
            .filter(|case| case["wire_http"] == true)
            .map(|case| case["id"].as_str().unwrap())
            .collect()
    );
    assert_eq!(
        manager.node_stats().rooms,
        0,
        "handler must not manufacture rooms"
    );
    manager.close_all().await;
    net.shutdown_and_wait().await;
}

#[tokio::test]
async fn forward_stats_shared_collector_reaches_actual_metrics_handler_and_node_protobuf() {
    let info = lk_config::ProgramInfo {
        binary_name: "livecloud".into(),
        product_version: "test".into(),
        compat_version: "1.13.10",
    };
    let args = [
        "livecloud",
        "--dev",
        "--node-ip",
        "127.0.0.1",
        "--config-body",
        "rtc: {forward_stats: {summary_interval: 10s, report_interval: 10s, report_window: 30s}}",
    ]
    .into_iter()
    .map(Into::into)
    .collect();
    let lk_config::EntryAction::RunServer(loaded) =
        lk_config::parse_with_probe(args, &lk_config::Env::default(), &info, &LocalProbe).unwrap()
    else {
        panic!("actual server config")
    };
    let collector = loaded.server.rtc.forward_stats.as_ref().unwrap().clone();
    let net = rtc_runtime::net::NetRouter::start(rtc_runtime::net::NetConfig {
        udp_enabled: false,
        udp_port: None,
        port_range: (0, 0),
        tcp_port: None,
        interfaces: vec![],
    })
    .await
    .unwrap();
    let manager = RoomManager::new(rtc_runtime::RuntimeDeps {
        config: Arc::new(loaded.server),
        net: net.clone(),
        events: Arc::new(rtc_core::events::NullSink),
        data: Arc::new(lk_compat::data::LiveKitDataHandler::new(Arc::new(
            lk_compat::rpc::RpcRegistry::new(),
        ))),
    });
    let now = std::time::Instant::now();
    collector.record_forward_samples(
        now + std::time::Duration::from_millis(5),
        [now, now + std::time::Duration::from_millis(4)],
    );
    collector.tick(now + std::time::Duration::from_secs(10));
    assert_eq!(manager.node_stats().forward_stats.latency_ns, 5_000_000);
    let sampler =
        crate::node_stats::Sampler::new(manager.clone(), std::time::Duration::from_secs(1), vec![])
            .unwrap();
    let proto = lk_compat::routing::NodeStatsProvider::sample(&sampler, 0, 1).unwrap();
    assert_eq!(
        proto.forward_latency, 5_000_000,
        "actual runtime→node sampler→official protobuf consumer"
    );
    let response = metrics(
        State(MetricsState {
            manager: manager.clone(),
            node_id: "ND_forward_owned".into(),
            basic_auth: None,
        }),
        HeaderMap::new(),
    )
    .await;
    assert_eq!(response.status(), StatusCode::OK);
    let bytes = axum::body::to_bytes(response.into_body(), 1_000_000)
        .await
        .unwrap();
    let body = std::str::from_utf8(&bytes).unwrap();
    assert!(body.contains(
        "livekit_forward_latency{node_id=\"ND_forward_owned\",node_type=\"SERVER\"} 5000000\n"
    ));
    assert!(body.contains(
        "livekit_forward_latency_ns_count{node_id=\"ND_forward_owned\",node_type=\"SERVER\"} 2\n"
    ));
    assert!(body.contains("livekit_forward_latency_ns_sum{node_id=\"ND_forward_owned\",node_type=\"SERVER\"} 6000000\n"));
    assert_eq!(
        manager.node_stats().rooms,
        0,
        "collector/handler does not invent a media room"
    );
    manager.close_all().await;
    net.shutdown_and_wait().await;
    let stopped = collector.snapshot().histogram_count;
    collector.record_forward_samples(now, [now]);
    std::thread::sleep(std::time::Duration::from_millis(2));
    assert_eq!(
        collector.snapshot().histogram_count,
        stopped,
        "runtime shutdown joined its collector worker"
    );
}
