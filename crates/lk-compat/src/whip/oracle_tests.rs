//! Opt-in actual fixed-Go comparison. No generated expected Rust outputs.
use super::fragment::{Document, Error, Fragment};
use serde_json::Value;
#[test]
#[ignore = "requires coordinator-run fixed Go fragment oracle outputs"]
fn all_29_actual_go_fragment_rows_match() {
    let cases: Vec<Value> = serde_json::from_slice(
        &std::fs::read(std::env::var("WHIP_FRAGMENT_GO_CASES").expect("actual cases path"))
            .unwrap(),
    )
    .unwrap();
    let results = std::fs::read_to_string(
        std::env::var("WHIP_FRAGMENT_GO_RESULTS").expect("actual Go results"),
    )
    .unwrap();
    let rows: Vec<Value> = results
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect();
    assert_eq!(cases.len(), 29);
    assert_eq!(rows.len(), cases.len());
    let field = |v: &Value, key: &str| {
        v.get(key)
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_owned()
    };
    for (case, row) in cases.iter().zip(rows.iter()) {
        assert_eq!(case["id"], row["id"]);
        let id = field(case, "id");
        let parsed = Fragment::parse(&field(case, "fragment"));
        match parsed {
            Err(error) => assert_eq!(error_text(&error), field(row, "error"), "{id}"),
            Ok(fragment) => {
                assert_eq!(field(row, "error"), "", "{id}");
                assert_eq!(fragment.media.mid, field(row, "mid"), "{id}");
                let candidates: Vec<String> = row["candidates"]
                    .as_array()
                    .map(|a| a.iter().map(|v| v.as_str().unwrap().to_owned()).collect())
                    .unwrap_or_default();
                assert_eq!(fragment.media.candidates, candidates, "{id}");
                assert_eq!(fragment.marshal(), field(row, "marshaled"), "{id}");
                match fragment.credentials() {
                    Ok((ufrag, pwd)) => {
                        assert_eq!(field(row, "credential_error"), "", "{id}");
                        assert_eq!(ufrag, field(row, "ufrag"));
                        assert_eq!(pwd, field(row, "pwd"));
                    }
                    Err(error) => {
                        assert!(!field(row, "credential_error").is_empty(), "{id}");
                        assert_eq!(
                            credential_kind(&error),
                            field(row, "credential_kind"),
                            "{id}"
                        );
                    }
                }
                let mut document = Document::from_validated(&field(case, "sdp"));
                match fragment.patch(&mut document) {
                    Err(error) => assert_eq!(error_text(&error), field(row, "patch_error"), "{id}"),
                    Ok(()) => {
                        assert_eq!(field(row, "patch_error"), "", "{id}");
                        assert_eq!(document.marshal(), field(row, "patched"), "{id}");
                        match document.extract_fragment() {
                            Ok(extracted) => {
                                assert_eq!(extracted.marshal(), field(row, "extracted"), "{id}")
                            }
                            Err(error) => {
                                assert_eq!(error_text(&error), field(row, "extract_error"), "{id}")
                            }
                        }
                    }
                }
            }
        }
    }
}
fn credential_kind(error: &Error) -> &'static str {
    match error {
        Error::MissingUfrag => "missing_ufrag",
        Error::MissingPwd => "missing_pwd",
        Error::ConflictingUfrag => "conflicting_ufrag",
        Error::ConflictingPwd => "conflicting_pwd",
        _ => "unexpected",
    }
}
fn error_text(error: &Error) -> String {
    match error {
        Error::InvalidMedia => "invalid media section".into(),
        Error::TooManyMedia => "too many media sections".into(),
        Error::InvalidAttribute => "invalid attribute".into(),
        Error::MissingMedia => "missing media section".into(),
        Error::BundleMismatch { expected, got } => {
            format!("bundle media mismatch, expected: {expected}, got: {got}")
        }
        Error::IceLiteMismatch => "ice lite mismatch".into(),
        Error::IceOptionsMismatch => "ice options mismatch".into(),
        Error::MissingMid => "could not find media mid".into(),
        Error::MissingBundle => "could not get bundle mid".into(),
        Error::MissingBundleMedia(mid) => format!("could not find bundle media: {mid}"),
        _ => format!("{error:?}"),
    }
}
