//! Test adapter to wire once the concrete GoUrlSupport has been integrated.
//! No self-authored expected strings, no execute/pass claim without real Go file.
pub fn verify_actual_go_url_rows(parser: &dyn super::gateway::GoUrlParser, file: &std::path::Path) {
    let rows: Vec<serde_json::Value> =
        serde_json::from_slice(&std::fs::read(file).unwrap()).unwrap();
    let mut count = 0;
    for row in rows.iter().filter(|r| r["kind"] == "url") {
        let actual = parser.parse(row["input"].as_str().unwrap());
        if let Some(expected) = row["error"].as_str() {
            let error = actual
                .err()
                .expect("actual Go rejected URL must not succeed");
            assert_eq!(error.code, "invalid_argument");
            assert_eq!(error.message, expected, "{}", row["name"]);
        } else {
            let actual = actual.expect("actual Go accepted URL must parse");
            assert_eq!(
                actual.scheme,
                row["scheme"].as_str().unwrap(),
                "{}",
                row["name"]
            );
            assert_eq!(
                actual.normalized,
                row["normalized"].as_str().unwrap(),
                "{}",
                row["name"]
            );
        }
        count += 1;
    }
    assert_eq!(count, 70);
}

#[test]
#[ignore = "requires actual Go1.26 net/url output"]
fn fixed_parser_matches_actual_go_rows() {
    let path = std::env::var("INGRESS_URL_GUID_GO_RESULTS").unwrap();
    verify_actual_go_url_rows(&super::go_url::FixedGoUrl, std::path::Path::new(&path));
}
#[test]
fn typed_out_of_date_is_not_reconstructed_from_code_or_text() {
    use crate::workers::error::IoError;
    assert!(IoError::out_of_date().is_ingress_out_of_date());
    assert!(
        !IoError::typed("failed_precondition", IoError::out_of_date().message)
            .is_ingress_out_of_date()
    );
    assert!(!IoError::ordinary(IoError::out_of_date().message).is_ingress_out_of_date());
}
#[tokio::test]
#[ignore = "requires actual fixed Go HTTP recovery boundary output"]
async fn nil_state_matches_actual_go_http_failure_category() {
    use axum::body::to_bytes;
    let file = std::env::var("INGRESS_BOUNDARY_GO_RESULTS").unwrap();
    let rows: Vec<serde_json::Value> =
        serde_json::from_slice(&std::fs::read(file).unwrap()).unwrap();
    let mut count = 0;
    for row in rows
        .iter()
        .filter(|r| r["kind"] == "nil_state" || r["kind"] == "nil_store")
    {
        let response = super::failure::IngressFailure::PanicRecovery
            .panic_response()
            .unwrap();
        assert_eq!(
            u64::from(response.status().as_u16()),
            row["status"].as_u64().unwrap()
        );
        assert_eq!(
            response.headers()[axum::http::header::CONTENT_TYPE]
                .to_str()
                .unwrap(),
            row["content_type"].as_str().unwrap()
        );
        let bytes = to_bytes(response.into_body(), 4096).await.unwrap();
        let text = std::str::from_utf8(&bytes).unwrap();
        assert_eq!(
            text.lines().next().unwrap(),
            row["first_line"].as_str().unwrap()
        );
        assert!(row["dynamic_stack_omitted"].as_bool().unwrap());
        count += 1;
    }
    assert_eq!(count, 4);
}

#[test]
#[ignore = "requires actual fixed Go sentinel identity output"]
fn local_out_of_date_identity_matches_actual_go_switch() {
    use crate::workers::error::IoError;
    let file = std::env::var("INGRESS_BOUNDARY_GO_RESULTS").unwrap();
    let rows: Vec<serde_json::Value> =
        serde_json::from_slice(&std::fs::read(file).unwrap()).unwrap();
    let mut count = 0;
    for row in rows.iter().filter(|r| r["kind"] == "typed_identity") {
        let source = IoError::out_of_date();
        let error = match row["input"].as_str().unwrap() {
            "actual_sentinel" => source,
            "remote_reconstruction" => IoError::typed(source.code, source.message),
            "ordinary_same_text" => IoError::ordinary(source.message),
            _ => panic!("unknown actual oracle input"),
        };
        let tolerated = error.is_ingress_out_of_date();
        assert_eq!(tolerated, row["info_present"].as_bool().unwrap());
        assert_eq!(!tolerated, row["error_present"].as_bool().unwrap());
        if !tolerated {
            assert_eq!(error.message, row["error"].as_str().unwrap());
        }
        count += 1;
    }
    assert_eq!(count, 3);
}
