//! Optional actual fixed Go IOInfoService/RedisStore cross-language oracle.
use super::*;
use base64::{Engine, engine::general_purpose::STANDARD};
use prost::Message;
#[tokio::test]
#[ignore = "requires dedicated empty SIP_CALLBACK_REDIS_URL and actual SIP_CALLBACK_ORACLE_RESULTS"]
async fn actual_go_callbacks_over_real_separate_redis_catalog() {
    let url = std::env::var("SIP_CALLBACK_REDIS_URL").expect("dedicated isolated Redis URL");
    let results =
        std::env::var("SIP_CALLBACK_ORACLE_RESULTS").expect("actual fixed Go output JSON file");
    let cases: Vec<serde_json::Value> = serde_json::from_str(include_str!(
        "../../../../../tests/oracles/sip-callbacks/cases.json"
    ))
    .unwrap();
    let expected: Vec<serde_json::Value> =
        serde_json::from_slice(&std::fs::read(results).unwrap()).unwrap();
    assert_eq!(expected.len(), cases.len());
    let catalog = SipCatalog::connect(&url, std::time::Duration::from_secs(5), 8)
        .await
        .unwrap();
    let client = redis::Client::open(url.as_str()).unwrap();
    let mut connection = client.get_multiplexed_async_connection().await.unwrap();
    let keys = [
        "sip_trunk",
        "sip_inbound_trunk",
        "sip_outbound_trunk",
        "sip_dispatch_rule",
    ];
    let count: i64 = redis::cmd("EXISTS")
        .arg(keys.as_slice())
        .query_async(&mut connection)
        .await
        .unwrap();
    assert_eq!(count, 0, "dedicated broker catalog must be empty");
    for (case, expected) in cases.iter().zip(&expected) {
        assert_eq!(case["id"], expected["id"]);
        let id = case["id"].as_str().unwrap();
        for trunk in case["trunks"].as_array().unwrap() {
            let value: pb::SipInboundTrunkInfo =
                lk_protocol::json::decode(trunk.to_string().as_bytes()).unwrap();
            catalog.store_inbound(&value).await.unwrap();
        }
        for rule in case["rules"].as_array().unwrap() {
            let value: pb::SipDispatchRuleInfo =
                lk_protocol::json::decode(rule.to_string().as_bytes()).unwrap();
            catalog.store_rule(&value).await.unwrap();
        }
        let callback =
            CatalogSipCallbacks::new(if case["disconnected"].as_bool().unwrap_or(false) {
                None
            } else {
                Some(catalog.clone())
            });
        let actual = match case["operation"].as_str().unwrap() {
            "authenticate" => {
                let req =
                    lk_protocol::json::decode(case["request"].to_string().as_bytes()).unwrap();
                callback.authenticate(req).await.map(|v| v.encode())
            }
            "evaluate" => {
                let req =
                    lk_protocol::json::decode(case["request"].to_string().as_bytes()).unwrap();
                callback.evaluate(req).await.map(|v| v.encode())
            }
            _ => panic!("unknown actual oracle operation"),
        };
        // Cleanup task-owned keys before asserting, including failure paths.
        let _: i64 = redis::cmd("DEL")
            .arg(keys.as_slice())
            .query_async(&mut connection)
            .await
            .unwrap();
        if let Some(error) = expected["error"].as_str() {
            let actual = actual.unwrap_err();
            assert_eq!(actual.code, expected["code"].as_str().unwrap(), "{id}");
            assert_eq!(actual.message, error, "{id}");
        } else {
            let actual = actual.unwrap_or_else(|e| panic!("{id}: {e}"));
            let wire = STANDARD
                .decode(expected["wire_base64"].as_str().unwrap_or(""))
                .unwrap();
            // Compare true Go protobuf semantic fields/presence, not object-key order or float JSON render.
            match case["operation"].as_str().unwrap() {
                "authenticate" => assert_eq!(
                    rpc::GetSipTrunkAuthenticationResponse::decode(actual.as_slice()).unwrap(),
                    rpc::GetSipTrunkAuthenticationResponse::decode(wire.as_slice()).unwrap(),
                    "{id}"
                ),
                _ => assert_eq!(
                    rpc::EvaluateSipDispatchRulesResponse::decode(actual.as_slice()).unwrap(),
                    rpc::EvaluateSipDispatchRulesResponse::decode(wire.as_slice()).unwrap(),
                    "{id}"
                ),
            }
        }
    }
}
