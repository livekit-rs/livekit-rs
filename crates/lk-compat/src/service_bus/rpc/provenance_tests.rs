use super::*;
use serde::Deserialize;
#[derive(Deserialize)]
struct AnyRow {
    type_url: String,
    value_hex: String,
    canonical_wrapper_hex: String,
}
#[derive(Deserialize)]
struct Row {
    id: String,
    response_wire_hex: String,
    decode_ok: bool,
    request_id: Option<String>,
    server_id: Option<String>,
    details: Vec<AnyRow>,
    legacy_response: Option<AnyRow>,
    meta: BTreeMap<String, String>,
}
// Official internal.Response response#4 projection, decoded from the real
// generated typed result; avoids touching its deprecated generated Rust field.
#[derive(Clone, PartialEq, prost::Message)]
struct LegacyResponseProjection {
    #[prost(message, optional, tag = "4")]
    response: Option<pbjson_types::Any>,
}
fn unhex(value: &str) -> Vec<u8> {
    assert_eq!(value.len() % 2, 0);
    value
        .as_bytes()
        .as_chunks::<2>()
        .0
        .iter()
        .map(|b| u8::from_str_radix(std::str::from_utf8(b).unwrap(), 16).unwrap())
        .collect()
}
#[test]
fn actual_go_wrappers_survive_response_error_and_full_status() {
    let mut typed_loss = false;
    let mut count = 0;
    let mut legacy_count = 0;
    for line in include_str!("../../../../../tests/fixtures/upstream/evidence/2026-10-25/f07-rpc-error-provenance-go-actual.jsonl").lines().filter(|s|!s.trim().is_empty()){
        let row:Row=serde_json::from_str(line).unwrap();count+=1;
        let raw=unhex(&row.response_wire_hex);
        let decoded=response_provenance::decode_response_body(&raw);
        if !row.decode_ok{assert!(decoded.is_err(),"{}",row.id);continue;}
        let received=decoded.unwrap();assert_eq!(Some(received.message.request_id.clone()),row.request_id,"{}",row.id);assert_eq!(Some(received.message.server_id.clone()),row.server_id,"{}",row.id);
        assert_eq!(received.error_detail_wire.len(),row.details.len());
        let legacy=LegacyResponseProjection::decode(received.message.encode_to_vec().as_slice()).unwrap();
        match (legacy.response.as_ref(),row.legacy_response.as_ref()) {
            (Some(actual),Some(expected))=>{
                legacy_count+=1;
                assert_eq!(actual.type_url,expected.type_url,"{} actualGo singularAny type_url",row.id);
                assert_eq!(actual.value.to_vec(),unhex(&expected.value_hex),"{} actualGo singularAny value",row.id);
            }
            (None,None)=>{},
            _=>panic!("{} actualGo singularAny presence",row.id),
        }
        for ((wire,typed),expected) in received.error_detail_wire.iter().zip(&received.message.error_details).zip(&row.details){
            assert_eq!(*wire,unhex(&expected.canonical_wrapper_hex),"{} actualGo wrapper",row.id);
            assert_eq!(typed.type_url,expected.type_url);assert_eq!(typed.value.to_vec(),unhex(&expected.value_hex));
            typed_loss|=typed.encode_to_vec()!=*wire;
        }
        let error=RpcError::remote_received(received);assert!(error.is_remote());assert!(!error.is_local_no_response());
        let actual=crate::twirp::worker_error::failure(error);
        assert_eq!(actual.meta,row.meta,"{} actualGo fullStatus",row.id);
    }
    assert_eq!(count, 18);
    assert_eq!(legacy_count, 4);
    assert!(
        typed_loss,
        "actual old typed-only path loses outer Any unknowns"
    );
}
fn remote_with_unknown(unknown: u8) -> RpcError {
    let any = pbjson_types::Any {
        type_url: "future.Detail".into(),
        value: vec![8, 7].into(),
    };
    let mut wire = any.encode_to_vec();
    wire.extend([0xa0, 6, unknown]);
    let response = pb::Response {
        request_id: "request-fixture".into(),
        server_id: "server-fixture".into(),
        code: "unavailable".into(),
        error: "no response from servers".into(),
        ..Default::default()
    };
    let raw = response_provenance::encode_response_body(&response, &[wire]);
    RpcError::remote_received(response_provenance::decode_response_body(&raw).unwrap())
}
#[test]
fn exact_source_cohort_is_owned_and_explicit_known_update_cannot_resurrect_stale() {
    let mut first = remote_with_unknown(7);
    let other = remote_with_unknown(8);
    let original = first.error_detail_wires().unwrap();
    assert_ne!(original, other.error_detail_wires().unwrap());
    let current = pbjson_types::Any {
        type_url: "future.New".into(),
        value: vec![2, 3].into(),
    };
    first.replace_error_detail(0, current.clone()).unwrap();
    let wire = first.error_detail_wires().unwrap().remove(0);
    let known = lk_protocol::wire::filter_known("google.protobuf.Any", &wire).unwrap();
    assert_eq!(
        pbjson_types::Any::decode(known.as_slice()).unwrap(),
        current
    );
    assert!(wire.ends_with(&[0xa0, 6, 7]));
    assert!(other.error_detail_wires().unwrap()[0].ends_with(&[0xa0, 6, 8]));
    assert_eq!(
        original[0],
        remote_with_unknown(7).error_detail_wires().unwrap()[0]
    );
}
#[test]
fn direct_cohort_mutation_fails_closed_and_remote_cannot_impersonate_local_origin() {
    let mut error = remote_with_unknown(7);
    assert!(error.is_remote());
    assert!(!error.is_local_no_response());
    assert!(RpcError::no_response().is_local_no_response());
    assert_eq!(error.message, RpcError::no_response().message);
    error.details.push(pbjson_types::Any::default());
    assert!(error.error_detail_wires().is_err());
    let result = crate::twirp::worker_error::failure(error);
    assert_eq!(result.code.as_str(), "internal");
    assert!(!result.meta.contains_key("error_details"));
    let mut error = remote_with_unknown(7);
    error.details[0].type_url = "uncoordinated changed type".into();
    assert!(error.error_detail_wires().is_err());
    assert!(RpcError::canceled().detail_provenance.is_none());
    assert!(RpcError::timed_out().detail_provenance.is_none());
}
#[test]
fn raw_envelope_plain_and_gzip_keep_bounds_and_exact_payload() {
    let mut error = remote_with_unknown(7);
    error.message = "actual compressible peer failure ".repeat(2000);
    let response = pb::Response {
        error: error.message.clone(),
        code: error.code.clone(),
        ..Default::default()
    };
    let body =
        response_provenance::encode_response_body(&response, &error.error_detail_wires().unwrap());
    let limits = wire::WireLimits {
        encoded_bytes: 1 << 20,
        decompressed_bytes: 1 << 20,
    };
    for quality in [0, 6] {
        let encoded = wire::encode_raw::<pb::Response>(
            &body,
            "fixture-channel",
            wire::CompressionPolicy {
                quality,
                threshold: 0,
            },
            limits,
        )
        .unwrap();
        let decoded = wire::decode_raw::<pb::Response>(&encoded, limits).unwrap();
        assert_eq!(decoded.payload, body);
        assert_eq!(decoded.local_channel, "fixture-channel");
        assert_eq!(decoded.compression, if quality == 0 { 0 } else { 1 });
        let actual = received_response(&encoded, limits).unwrap();
        assert_eq!(
            actual.error_detail_wire,
            error.error_detail_wires().unwrap()
        );
        assert!(
            wire::decode_raw::<pb::Response>(
                &encoded,
                wire::WireLimits {
                    encoded_bytes: encoded.len() - 1,
                    decompressed_bytes: 1 << 20
                }
            )
            .is_err()
        );
        assert!(
            wire::decode_raw::<pb::Response>(
                &encoded,
                wire::WireLimits {
                    encoded_bytes: 1 << 20,
                    decompressed_bytes: body.len() - 1
                }
            )
            .is_err()
        );
    }
}

#[tokio::test]
#[ignore = "requires coordinator-owned IO_RPC_TEST_REDIS_URL broker; real Redis raw-error path"]
async fn actual_redis_plain_and_gzip_error_wrappers_survive_server_client_and_forwarding() {
    use super::super::redis_transport::TransportLimits;
    let url = std::env::var("IO_RPC_TEST_REDIS_URL").expect("dedicated broker URL");
    for quality in [0, 6] {
        let bus = RedisTransport::connect(
            &url,
            TransportLimits {
                wire: wire::WireLimits {
                    encoded_bytes: 1 << 20,
                    decompressed_bytes: 1 << 20,
                },
                compression: wire::CompressionPolicy {
                    quality,
                    threshold: 0,
                },
                inbox_messages: 64,
                subscriptions: 32,
                commands: 16,
                io_timeout: Duration::from_secs(2),
            },
        )
        .await
        .unwrap();
        let spec = MethodSpec {
            service: "ActualRawErrorFixture".into(),
            method: "Call".into(),
            topics: vec![rtc_core::ids::new_sid("run_")],
            require_claim: true,
            queue: false,
        };
        let mut original = remote_with_unknown(7);
        original.message = "actual repeated peer failure ".repeat(2000);
        let expected = original.error_detail_wires().unwrap();
        // Actual server emits a previously received owned error. This locks the
        // server-forwarding leg, not only the client metadata extractor.
        let server = RpcServer::register::<String, String, _, _>(
            bus.clone(),
            spec.clone(),
            ServerLimits {
                concurrent: 2,
                max_request_duration: Duration::from_secs(5),
                allow_skip_claim: false,
            },
            move |_, _| {
                let error = original.clone();
                async move { Err(error) }
            },
            None,
        )
        .await
        .unwrap();
        let client = RpcClient::new(bus.clone(), 2).unwrap();
        let mut options = CallOptions::new(Duration::from_secs(3));
        options.selection.accept_first = true;
        let error = client
            .unary::<String, String>(&spec, &"request".into(), options)
            .await
            .unwrap_err();
        assert!(error.is_remote());
        assert_eq!(error.error_detail_wires().unwrap(), expected);
        let meta = crate::twirp::worker_error::failure(error);
        assert!(meta.meta.contains_key("error_details"));
        server.shutdown().await;
    }
}
