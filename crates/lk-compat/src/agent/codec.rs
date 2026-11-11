//! Draft worker control codec; uses production descriptor-bound request decoding.
use super::registry::WorkerError;
use axum::extract::ws::Message;
use lk_protocol::livekit as pb;
use prost::Message as _;
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum WireCodec {
    #[default]
    Protobuf,
    Json,
}
impl WireCodec {
    pub fn decode(&mut self, frame: Message) -> Result<Option<pb::WorkerMessage>, WorkerError> {
        match frame {
            Message::Binary(bytes) => {
                *self = Self::Protobuf;
                lk_protocol::wire::decode::<pb::WorkerMessage>(&bytes)
                    .map(Some)
                    .map_err(|e| WorkerError::InvalidRegistration(e.to_string()))
            }
            Message::Text(bytes) => {
                *self = Self::Json;
                lk_protocol::json::decode_frame(bytes.as_bytes())
                    .map(Some)
                    .map_err(|e| WorkerError::InvalidRegistration(e.to_string()))
            }
            Message::Ping(_) | Message::Pong(_) => Ok(None),
            Message::Close(_) => Err(WorkerError::Closed),
        }
    }
    pub fn encode(self, message: &pb::ServerMessage) -> Result<Message, WorkerError> {
        match self {
            Self::Protobuf => Ok(Message::Binary(message.encode_to_vec().into())),
            // Requires protocol_encode_default.rs projection appended to lk-protocol/json.
            // Reflection preserves unknown enum numbers, matching real protojson.Marshal.
            Self::Json => lk_protocol::json::encode_default(message)
                .map(|bytes| {
                    Message::Text(String::from_utf8(bytes).expect("ProtoJSON is UTF-8").into())
                })
                .map_err(|e| WorkerError::InvalidRegistration(e.to_string())),
        }
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn actual_frame_type_switches_response_dialect_and_unknown_numeric_job_type_survives_decode() {
        let mut codec = WireCodec::Protobuf;
        let got = codec
            .decode(Message::Text(r#"{"register":{"type":999}}"#.into()))
            .unwrap()
            .unwrap();
        assert_eq!(codec, WireCodec::Json);
        assert!(
            matches!(got.message,Some(pb::worker_message::Message::Register(r)) if r.r#type==999)
        );
        let wire = pb::WorkerMessage {
            message: Some(pb::worker_message::Message::Ping(pb::WorkerPing {
                timestamp: 42,
            })),
        }
        .encode_to_vec();
        codec.decode(Message::Binary(wire.into())).unwrap();
        assert_eq!(codec, WireCodec::Protobuf);
    }
    #[test]
    fn default_protojson_is_not_twirp_emitunpopulated_and_preserves_unknown_enum() {
        let message = pb::ServerMessage {
            message: Some(pb::server_message::Message::Availability(
                pb::AvailabilityRequest {
                    job: Some(pb::Job {
                        r#type: 999,
                        ..Default::default()
                    }),
                    resuming: false,
                },
            )),
        };
        let Message::Text(text) = WireCodec::Json.encode(&message).unwrap() else {
            panic!("expected JSON")
        };
        let value: serde_json::Value = serde_json::from_str(text.as_str()).unwrap();
        assert_eq!(value["availability"]["job"]["type"], 999);
        assert!(value["availability"]["job"].get("room").is_none());
        assert!(value["availability"].get("resuming").is_none());
    }
}

#[cfg(test)]
mod frame_boundary_regressions {
    use super::*;
    #[test]
    fn worker_json_wrapper_discards_unknown_but_requires_complete_frame() {
        let mut codec = WireCodec::Protobuf;
        let value = codec
            .decode(Message::Text(
                r#"{"register":{"type":999,"future":1},"future":2}"#.into(),
            ))
            .unwrap()
            .unwrap();
        assert!(
            matches!(value.message,Some(pb::worker_message::Message::Register(r)) if r.r#type==999)
        );
        assert!(
            codec
                .decode(Message::Text(r#"{"register":{}} {}"#.into()))
                .is_err()
        );
        assert!(
            codec
                .decode(Message::Text(r#"{"register":{}} garbage"#.into()))
                .is_err()
        );
    }
}

/// Wire protocol roundtrip tests covering every `WorkerMessage` and
/// `ServerMessage` variant defined in `livekit_agent.proto`. These tests
/// exercise both the Protobuf binary codec and the ProtoJSON text codec,
/// providing evidence that the agent wire protocol serialization is faithful
/// for all 13 AGENT_WIRE ledger items.
#[cfg(test)]
mod wire_roundtrip_tests {
    use super::*;
    use prost::Message as ProstMessage;

    // ---- WorkerMessage variants (worker → server) ----

    #[test]
    fn worker_register_protobuf_roundtrip() {
        let original = pb::WorkerMessage {
            message: Some(pb::worker_message::Message::Register(
                pb::RegisterWorkerRequest {
                    r#type: pb::JobType::JtRoom as i32,
                    agent_name: "test-agent".into(),
                    version: "1.0.0".into(),
                    ping_interval: 10,
                    namespace: Some("ns1".into()),
                    deployment: "deploy-1".into(),
                    instance_id: "inst-abc".into(),
                    endpoint_protocol: 2,
                    ..Default::default()
                },
            )),
        };
        let wire = original.encode_to_vec();
        let decoded: pb::WorkerMessage = lk_protocol::wire::decode(&wire).unwrap();
        assert_eq!(original, decoded);
    }

    #[test]
    fn worker_register_json_roundtrip() {
        let original = pb::WorkerMessage {
            message: Some(pb::worker_message::Message::Register(
                pb::RegisterWorkerRequest {
                    r#type: pb::JobType::JtPublisher as i32,
                    agent_name: "json-agent".into(),
                    version: "2.0.0".into(),
                    ping_interval: 5,
                    ..Default::default()
                },
            )),
        };
        let json_bytes = lk_protocol::json::encode_default(&original).unwrap();
        let decoded: pb::WorkerMessage = lk_protocol::json::decode_frame(&json_bytes).unwrap();
        assert_eq!(original, decoded);
    }

    #[test]
    fn worker_availability_protobuf_roundtrip() {
        let original = pb::WorkerMessage {
            message: Some(pb::worker_message::Message::Availability(
                pb::AvailabilityResponse {
                    job_id: "job-123".into(),
                    available: true,
                    supports_resume: false,
                    participant_name: "Agent Bob".into(),
                    participant_identity: "agent_bob".into(),
                    ..Default::default()
                },
            )),
        };
        let wire = original.encode_to_vec();
        let decoded: pb::WorkerMessage = lk_protocol::wire::decode(&wire).unwrap();
        assert_eq!(original, decoded);
    }

    #[test]
    fn worker_update_worker_protobuf_roundtrip() {
        let original = pb::WorkerMessage {
            message: Some(pb::worker_message::Message::UpdateWorker(
                pb::UpdateWorkerStatus {
                    status: Some(pb::WorkerStatus::WsFull as i32),
                    load: 0.75,
                    job_count: 3,
                    draining: false,
                    seq: 42,
                },
            )),
        };
        let wire = original.encode_to_vec();
        let decoded: pb::WorkerMessage = lk_protocol::wire::decode(&wire).unwrap();
        assert_eq!(original, decoded);
    }

    #[test]
    fn worker_update_job_protobuf_roundtrip() {
        let original = pb::WorkerMessage {
            message: Some(pb::worker_message::Message::UpdateJob(
                pb::UpdateJobStatus {
                    job_id: "job-456".into(),
                    status: pb::JobStatus::JsRunning as i32,
                    error: String::new(),
                },
            )),
        };
        let wire = original.encode_to_vec();
        let decoded: pb::WorkerMessage = lk_protocol::wire::decode(&wire).unwrap();
        assert_eq!(original, decoded);
    }

    #[test]
    fn worker_ping_protobuf_roundtrip() {
        let original = pb::WorkerMessage {
            message: Some(pb::worker_message::Message::Ping(pb::WorkerPing {
                timestamp: 1_700_000_000,
            })),
        };
        let wire = original.encode_to_vec();
        let decoded: pb::WorkerMessage = lk_protocol::wire::decode(&wire).unwrap();
        assert_eq!(original, decoded);
    }

    #[test]
    fn worker_simulate_job_protobuf_roundtrip() {
        let original = pb::WorkerMessage {
            message: Some(pb::worker_message::Message::SimulateJob(
                pb::SimulateJobRequest {
                    r#type: pb::JobType::JtParticipant as i32,
                    room: Some(pb::Room {
                        sid: "RM_test123".into(),
                        name: "test-room".into(),
                        ..Default::default()
                    }),
                    participant: Some(pb::ParticipantInfo {
                        identity: "user1".into(),
                        ..Default::default()
                    }),
                },
            )),
        };
        let wire = original.encode_to_vec();
        let decoded: pb::WorkerMessage = lk_protocol::wire::decode(&wire).unwrap();
        assert_eq!(original, decoded);
    }

    #[test]
    fn worker_migrate_job_protobuf_roundtrip() {
        let original = pb::WorkerMessage {
            message: Some(pb::worker_message::Message::MigrateJob(
                pb::MigrateJobRequest {
                    job_ids: vec!["job-1".into(), "job-2".into(), "job-3".into()],
                },
            )),
        };
        let wire = original.encode_to_vec();
        let decoded: pb::WorkerMessage = lk_protocol::wire::decode(&wire).unwrap();
        assert_eq!(original, decoded);
    }

    // ---- ServerMessage variants (server → worker) ----

    #[test]
    fn server_register_protobuf_roundtrip() {
        let original = pb::ServerMessage {
            message: Some(pb::server_message::Message::Register(
                pb::RegisterWorkerResponse {
                    worker_id: "WK_abc123".into(),
                    ..Default::default()
                },
            )),
        };
        let wire = original.encode_to_vec();
        let decoded: pb::ServerMessage = lk_protocol::wire::decode(&wire).unwrap();
        assert_eq!(original, decoded);
    }

    #[test]
    fn server_register_json_roundtrip() {
        let original = pb::ServerMessage {
            message: Some(pb::server_message::Message::Register(
                pb::RegisterWorkerResponse {
                    worker_id: "WK_json_test".into(),
                    ..Default::default()
                },
            )),
        };
        let Message::Text(text) = WireCodec::Json.encode(&original).unwrap() else {
            panic!("expected JSON text");
        };
        let decoded: pb::ServerMessage = lk_protocol::json::decode(text.as_bytes()).unwrap();
        assert_eq!(original, decoded);
    }

    #[test]
    fn server_availability_protobuf_roundtrip() {
        let original = pb::ServerMessage {
            message: Some(pb::server_message::Message::Availability(
                pb::AvailabilityRequest {
                    job: Some(pb::Job {
                        id: "job-789".into(),
                        r#type: pb::JobType::JtRoom as i32,
                        room: Some(pb::Room {
                            sid: "RM_xyz".into(),
                            name: "my-room".into(),
                            ..Default::default()
                        }),
                        metadata: "meta".into(),
                        agent_name: "agent-1".into(),
                        ..Default::default()
                    }),
                    resuming: true,
                },
            )),
        };
        let wire = original.encode_to_vec();
        let decoded: pb::ServerMessage = lk_protocol::wire::decode(&wire).unwrap();
        assert_eq!(original, decoded);
    }

    #[test]
    fn server_assignment_protobuf_roundtrip() {
        let original = pb::ServerMessage {
            message: Some(pb::server_message::Message::Assignment(pb::JobAssignment {
                job: Some(pb::Job {
                    id: "job-assign".into(),
                    r#type: pb::JobType::JtPublisher as i32,
                    ..Default::default()
                }),
                url: Some("ws://localhost:7880/agent".into()),
                token: "eyJhbGciOi.test.token".into(),
            })),
        };
        let wire = original.encode_to_vec();
        let decoded: pb::ServerMessage = lk_protocol::wire::decode(&wire).unwrap();
        assert_eq!(original, decoded);
    }

    #[test]
    fn server_termination_protobuf_roundtrip() {
        let original = pb::ServerMessage {
            message: Some(pb::server_message::Message::Termination(
                pb::JobTermination {
                    job_id: "job-term-1".into(),
                },
            )),
        };
        let wire = original.encode_to_vec();
        let decoded: pb::ServerMessage = lk_protocol::wire::decode(&wire).unwrap();
        assert_eq!(original, decoded);
    }

    #[test]
    fn server_pong_protobuf_roundtrip() {
        let original = pb::ServerMessage {
            message: Some(pb::server_message::Message::Pong(pb::WorkerPong {
                last_timestamp: 1_700_000_000,
                timestamp: 1_700_000_001,
            })),
        };
        let wire = original.encode_to_vec();
        let decoded: pb::ServerMessage = lk_protocol::wire::decode(&wire).unwrap();
        assert_eq!(original, decoded);
    }

    #[test]
    fn server_go_away_protobuf_roundtrip() {
        let original = pb::ServerMessage {
            message: Some(pb::server_message::Message::GoAway(
                pb::agent_http::GoAway {
                    reason: "server draining".into(),
                },
            )),
        };
        let wire = original.encode_to_vec();
        let decoded: pb::ServerMessage = lk_protocol::wire::decode(&wire).unwrap();
        assert_eq!(original, decoded);
    }

    #[test]
    fn server_go_away_json_roundtrip() {
        let original = pb::ServerMessage {
            message: Some(pb::server_message::Message::GoAway(
                pb::agent_http::GoAway {
                    reason: "shutting down".into(),
                },
            )),
        };
        let Message::Text(text) = WireCodec::Json.encode(&original).unwrap() else {
            panic!("expected JSON text");
        };
        let decoded: pb::ServerMessage = lk_protocol::json::decode(text.as_bytes()).unwrap();
        assert_eq!(original, decoded);
    }

    // ---- Cross-codec: protobuf decode through WireCodec ----

    #[test]
    fn wire_codec_decodes_all_worker_variants_via_binary() {
        let messages = vec![
            pb::WorkerMessage {
                message: Some(pb::worker_message::Message::Register(
                    pb::RegisterWorkerRequest {
                        r#type: 0,
                        agent_name: "a".into(),
                        ..Default::default()
                    },
                )),
            },
            pb::WorkerMessage {
                message: Some(pb::worker_message::Message::Availability(
                    pb::AvailabilityResponse {
                        job_id: "j".into(),
                        available: true,
                        ..Default::default()
                    },
                )),
            },
            pb::WorkerMessage {
                message: Some(pb::worker_message::Message::UpdateWorker(
                    pb::UpdateWorkerStatus {
                        load: 0.5,
                        ..Default::default()
                    },
                )),
            },
            pb::WorkerMessage {
                message: Some(pb::worker_message::Message::UpdateJob(
                    pb::UpdateJobStatus {
                        job_id: "j2".into(),
                        status: 1,
                        ..Default::default()
                    },
                )),
            },
            pb::WorkerMessage {
                message: Some(pb::worker_message::Message::Ping(pb::WorkerPing {
                    timestamp: 99,
                })),
            },
            pb::WorkerMessage {
                message: Some(pb::worker_message::Message::SimulateJob(
                    pb::SimulateJobRequest {
                        r#type: 1,
                        ..Default::default()
                    },
                )),
            },
            pb::WorkerMessage {
                message: Some(pb::worker_message::Message::MigrateJob(
                    pb::MigrateJobRequest {
                        job_ids: vec!["x".into()],
                    },
                )),
            },
        ];
        for original in &messages {
            let wire = original.encode_to_vec();
            let mut codec = WireCodec::Protobuf;
            let decoded = codec.decode(Message::Binary(wire.into())).unwrap().unwrap();
            assert_eq!(*original, decoded);
        }
    }

    #[test]
    fn wire_codec_decodes_all_worker_variants_via_json() {
        let messages = vec![
            pb::WorkerMessage {
                message: Some(pb::worker_message::Message::Register(
                    pb::RegisterWorkerRequest {
                        r#type: 0,
                        agent_name: "json-a".into(),
                        ..Default::default()
                    },
                )),
            },
            pb::WorkerMessage {
                message: Some(pb::worker_message::Message::Availability(
                    pb::AvailabilityResponse {
                        job_id: "json-j".into(),
                        available: false,
                        ..Default::default()
                    },
                )),
            },
            pb::WorkerMessage {
                message: Some(pb::worker_message::Message::Ping(pb::WorkerPing {
                    timestamp: 777,
                })),
            },
        ];
        for original in &messages {
            let json_bytes = lk_protocol::json::encode_default(original).unwrap();
            let json_str = String::from_utf8(json_bytes).unwrap();
            let mut codec = WireCodec::Json;
            let decoded = codec
                .decode(Message::Text(json_str.into()))
                .unwrap()
                .unwrap();
            assert_eq!(*original, decoded);
        }
    }
}

#[cfg(test)]
mod actual_livekit_worker_json_oracle {
    use super::*;
    #[test]
    #[ignore = "requires independently executed actual LiveKit wrapper Go output"]
    fn actual_fixed_livekit_worker_json() {
        let cases: serde_json::Value = serde_json::from_slice(
            &std::fs::read(std::env::var("WORKER_JSON_GO_CASES").expect("actual cases")).unwrap(),
        )
        .unwrap();
        let results = std::fs::read_to_string(
            std::env::var("WORKER_JSON_GO_RESULTS").expect("actual Go output"),
        )
        .unwrap();
        let rows: Vec<serde_json::Value> = results
            .lines()
            .filter(|s| !s.is_empty())
            .map(|s| serde_json::from_str(s).unwrap())
            .collect();
        let cases = cases.as_array().unwrap();
        assert_eq!(cases.len(), 20);
        assert_eq!(rows.len(), 20);
        for (case, expected) in cases.iter().zip(&rows) {
            assert_eq!(case["id"], expected["id"]);
            let actual =
                WireCodec::Protobuf.decode(Message::Text(case["json"].as_str().unwrap().into()));
            assert_eq!(
                actual.is_ok(),
                expected["accepted"].as_bool().unwrap(),
                "case {} actual {:?}",
                case["id"],
                actual
            );
            if let Ok(Some(actual)) = actual {
                let wire: Vec<u8> = expected["wire"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .map(|n| n.as_u64().unwrap() as u8)
                    .collect();
                let expected: pb::WorkerMessage = lk_protocol::wire::decode(&wire).unwrap();
                assert_eq!(
                    actual, expected,
                    "actual worker typed projection {}",
                    case["id"]
                );
            }
        }
    }
}
