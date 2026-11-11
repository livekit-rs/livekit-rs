#[test]
fn old_public_grants_kind_details_whole_null_matches_slice_admission() {
    for (value, accepted) in [
        (serde_json::json!({}), true),
        (serde_json::json!({"kindDetails":null}), true),
        (serde_json::json!({"kindDetails":[]}), true),
        (serde_json::json!({"kindDetails":[null]}), true),
        (serde_json::json!({"kindDetails":["x",null]}), true),
        (serde_json::json!({"kindDetails":[9]}), false),
        (serde_json::json!({"kindDetails":[{}]}), false),
        (serde_json::json!({"kindDetails":"x"}), false),
    ] {
        assert_eq!(
            crate::auth::claims::decode_grants(value.as_object().unwrap()).is_ok(),
            accepted,
            "whole nullable slice does not permit invalid elements"
        );
    }
}
#[test]
#[ignore = "requires actual original signed-token Go8 result; no source expected authored"]
fn actual_go_kind_details_signed_jwt8() {
    use base64::Engine as _;
    use hmac::{Hmac, Mac};
    use sha2::Sha256;
    let cases: Vec<serde_json::Value> = serde_json::from_str(
        &std::fs::read_to_string(std::env::var_os("KIND_DETAILS_GO_CASES").unwrap()).unwrap(),
    )
    .unwrap();
    let raw =
        std::fs::read_to_string(std::env::var_os("KIND_DETAILS_GO_RESULTS").unwrap()).unwrap();
    let rows: Vec<serde_json::Value> = raw
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect();
    assert_eq!(cases.len(), 8);
    assert_eq!(rows.len(), 8);
    let mut seen = std::collections::BTreeSet::new();
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs_f64();
    for (input, row) in cases.iter().zip(rows) {
        assert_eq!(input["id"], row["id"]);
        assert!(seen.insert(input["id"].as_str().unwrap()));
        let mut claims = input["claims"].as_object().unwrap().clone();
        claims.insert("iss".into(), serde_json::json!("owned-key"));
        claims.insert("sub".into(), serde_json::json!("owned-identity"));
        claims.insert("exp".into(), serde_json::json!(now + 3600.0));
        claims.insert(
            "video".into(),
            serde_json::json!({"roomJoin":true,"room":"owned"}),
        );
        let b64 = base64::engine::general_purpose::URL_SAFE_NO_PAD;
        let signing = format!(
            "{}.{}",
            b64.encode(br#"{"alg":"HS256","typ":"JWT"}"#),
            b64.encode(serde_json::to_vec(&claims).unwrap())
        );
        let mut mac = <Hmac<Sha256> as Mac>::new_from_slice(b"owned-secret").unwrap();
        mac.update(signing.as_bytes());
        let token = format!("{}.{}", signing, b64.encode(mac.finalize().into_bytes()));
        let keys = crate::auth::KeyStore::new(vec![("owned-key".into(), "owned-secret".into())]);
        assert_eq!(
            serde_json::json!(crate::auth::verify_token(&keys, &token, now).is_ok()),
            row["accepted"]
        );
    }
    assert_eq!(seen.len(), 8);
}
