//! Differential component oracle: expected rows are the actual pinned Go worker
//! output, not a second Rust implementation or hand-maintained expected values.
//! WebSocket registration framing remains covered by wire-control.mjs.
use super::{
    registry::{Limits, Registration, Registry, send_message},
    token,
};
use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use lk_protocol::livekit as pb;
use serde_json::{Value, json};
use std::sync::Arc;
use tokio::sync::mpsc;

const VECTORS: &str = include_str!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../../tests/fixtures/upstream/validation/agent-worker-oracle/vectors.json"
));
const GO: &str = include_str!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../../tests/fixtures/upstream/validation/agent-worker-oracle/go-results.json"
));
fn decode<T: prost::Message + prost::Name + Default>(value: &Value) -> T {
    lk_protocol::json::decode(&serde_json::to_vec(value).unwrap()).unwrap()
}
fn safe_claims(jwt: &str) -> Value {
    let bytes = URL_SAFE_NO_PAD
        .decode(jwt.split('.').nth(1).expect("real signed JWT payload"))
        .unwrap();
    let mut claims: Value = serde_json::from_slice(&bytes).unwrap();
    for time in ["exp", "nbf", "iat"] {
        claims.as_object_mut().unwrap().remove(time);
    }
    claims
}

#[tokio::test]
async fn actual_registry_registration_assignment_and_token_match_ten_executed_go_rows() {
    let vectors: Vec<Value> = serde_json::from_str(VECTORS).unwrap();
    let expected: Vec<Value> = serde_json::from_str(GO).unwrap();
    assert_eq!(vectors.len(), 10);
    assert_eq!(expected.len(), vectors.len());
    let keys = Arc::new(crate::auth::KeyStore::new(vec![(
        "unit-oracle-key".into(),
        "unit-oracle-public-fixture-secret".into(),
    )]));
    let minter = Arc::new(crate::auth::TokenMinter::new(keys.clone()));
    let mut rows = Vec::new();
    for vector in vectors {
        let mut row = json!({"name": vector["name"]});
        let request: pb::RegisterWorkerRequest = decode(&vector["register"]);
        let registration = match Registration::from_request("AW_fixture".into(), 1, &request) {
            Ok(value) => value,
            Err(error) => {
                row["error"] = json!(error.to_string());
                rows.push(row);
                continue;
            }
        };
        let registry = Registry::new(Limits::default());
        let (tx, mut rx) = mpsc::channel(4);
        // The actual production send waits for a writer receipt. Verify it has
        // not completed merely because the bounded queue accepted the message.
        let registration_tx = tx.clone();
        let register_write = tokio::spawn(async move {
            send_message(
                &registration_tx,
                pb::ServerMessage {
                    message: Some(pb::server_message::Message::Register(
                        pb::RegisterWorkerResponse {
                            worker_id: "AW_fixture".into(),
                            server_info: Some(pb::ServerInfo::default()),
                            endpoint_settings: None,
                        },
                    )),
                },
            )
            .await
        });
        let written = rx.recv().await.unwrap();
        assert!(!register_write.is_finished());
        let Some(pb::server_message::Message::Register(response)) = written.message.message else {
            panic!("actual registration response")
        };
        row["worker_id"] = json!(response.worker_id);
        row["endpoint_settings_absent"] = json!(response.endpoint_settings.is_none());
        written.written.send(Ok(())).unwrap();
        register_write.await.unwrap().unwrap();
        registry.register(registration, tx).unwrap();
        let registration = registry.registration("AW_fixture").unwrap();
        row["registered"] = json!(true);
        row["permissions"] = json!({"subscribe":registration.permissions.can_subscribe,"publish":registration.permissions.can_publish,"data":registration.permissions.can_publish_data,"metadata":registration.permissions.can_update_metadata});
        let mut states = Vec::new();
        for update in vector["updates"].as_array().into_iter().flatten() {
            registry
                .update_worker("AW_fixture", &decode::<pb::UpdateWorkerStatus>(update))
                .unwrap();
            let (status, load, draining, _) = registry.oracle_snapshot("AW_fixture");
            // Go's JSON encoder prints float32 with float32 precision, rather
            // than exposing the intermediate f64 conversion's extra digits.
            let load: Value = serde_json::from_str(&serde_json::to_string(&load).unwrap()).unwrap();
            states.push(json!({"status":status,"load":load,"draining":draining}));
        }
        row["states"] = json!(states);
        if !vector["availability"].is_null() {
            let mut availability: pb::AvailabilityResponse = decode(&vector["availability"]);
            availability.job_id = "AJ_fixture".into();
            let accept = availability.available && !availability.terminate;
            let task_registry = registry.clone();
            let tokens = minter.clone();
            let now = rtc_core::util::unix_ms() / 1000;
            let task = tokio::spawn(async move {
                task_registry
                    .assign(
                        "AW_fixture",
                        pb::Job {
                            id: "AJ_fixture".into(),
                            room: Some(pb::Room {
                                name: "oracle-room".into(),
                                ..Default::default()
                            }),
                            ..Default::default()
                        },
                        now * 1_000_000_000,
                        |r, j, a| token::mint_assignment(&tokens, "unit-oracle-key", r, j, a, now),
                    )
                    .await
            });
            let offer = rx.recv().await.unwrap();
            let Some(pb::server_message::Message::Availability(offer_message)) =
                offer.message.message
            else {
                panic!("real availability")
            };
            row["offered"] = json!(offer_message.job.unwrap().id);
            offer.written.send(Ok(())).unwrap();
            registry.availability("AW_fixture", availability).unwrap();
            if accept {
                let assignment = rx.recv().await.unwrap();
                assert!(
                    !task.is_finished(),
                    "assignment success requires actual writer ACK"
                );
                let Some(pb::server_message::Message::Assignment(message)) =
                    assignment.message.message
                else {
                    panic!("real assignment")
                };
                row["assigned"] = json!(message.job.unwrap().id);
                crate::auth::authenticate_token(&keys, Some(&message.token))
                    .unwrap()
                    .unwrap();
                row["token_claims_without_time"] = safe_claims(&message.token);
                assignment.written.send(Ok(())).unwrap();
            }
            match task.await.unwrap() {
                Ok(state) => {
                    row["assignment_status"] = json!(state.status);
                    row["assignment_identity"] = json!(state.participant_identity);
                }
                Err(error) => row["assignment_error"] = json!(error.to_string()),
            }
            row["running_jobs"] = json!(registry.oracle_snapshot("AW_fixture").3);
        }
        registry.disconnect("AW_fixture");
        rows.push(row);
    }
    assert_eq!(
        rows, expected,
        "all actual Go component observations, with only time claims removed"
    );
}
