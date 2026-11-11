use super::*;
use crate::livekit::RoomConfiguration;
use serde::Deserialize;
use serde_json::value::RawValue;

fn source(raw: &str) -> Diagnostic {
    match decode::<RoomConfiguration>(raw.as_bytes()).unwrap_err() {
        Error::Source(error) => error,
        Error::Decoder(error) => panic!("not a structured error: {error}"),
    }
}
#[test]
fn original_raw_key_unicode_column_and_multiline_are_retained() {
    let escaped = source(r#"{"emptyTimeout":1,"empty\u0054imeout":2}"#);
    assert_eq!(escaped.raw_token, r#""empty\u0054imeout""#);
    assert_eq!((escaped.line, escaped.column), (1, 19));
    let unicode = source(r#"{"未知":1,"emptyTimeout":1,"emptyTimeout":2}"#);
    assert_eq!((unicode.line, unicode.column), (1, 26));
    let multiline = source("{\n  \"emptyTimeout\":1,\n  \"emptyTimeout\":2\n}");
    assert_eq!((multiline.line, multiline.column), (3, 3));
}
#[test]
fn first_scalar_error_wins_over_later_duplicate_without_text_classification() {
    let error = source(r#"{"emptyTimeout":"invalid","emptyTimeout":2}"#);
    assert_eq!(
        error.cause,
        Cause::InvalidScalar {
            kind: "uint32",
            field: "emptyTimeout".into()
        }
    );
    assert_eq!(error.raw_token, r#""invalid""#);
    assert_eq!((error.line, error.column), (1, 17));
}
#[test]
fn unknown_discard_and_integer_exponent_precision_do_not_change() {
    for raw in [
        r#"{"unknown":1,"unknown":2}"#,
        r#"{"emptyTimeout":4.2e1}"#,
        r#"{"emptyTimeout":"4294967295"}"#,
        r#"{"egress":{"participant":{"preset":"F104_FUTURE"}}}"#,
        r#"{"egress":{"participant":{"preset":-7}}}"#,
    ] {
        let original = crate::json::decode::<RoomConfiguration>(raw.as_bytes()).unwrap();
        assert_eq!(
            decode::<RoomConfiguration>(raw.as_bytes()).unwrap(),
            original
        );
    }
}
#[test]
fn null_presence_duplicate_and_map_value_failures_are_structured() {
    assert_eq!(
        source(r#"{"emptyTimeout":null,"emptyTimeout":2}"#).cause,
        Cause::DuplicateField
    );
    let error = source(r#"{"tags":{"x":null}}"#);
    assert_eq!(
        error.cause,
        Cause::InvalidScalar {
            kind: "string",
            field: "value".into()
        }
    );
    assert_eq!(
        source(r#"{"tags":{"x":"a","x":"b"}}"#).cause,
        Cause::DuplicateMapKey
    );
    assert_eq!(source(r#"{"agents":{}}"#).cause, Cause::UnexpectedToken);
}

#[derive(Deserialize)]
struct Case {
    id: String,
    #[serde(default)]
    config: Option<Box<RawValue>>,
    #[serde(default)]
    config_raw: Option<String>,
}
#[derive(Deserialize)]
struct Observation {
    id: String,
    direct_error: String,
    verified: bool,
}

#[test]
#[ignore = "requires captured actual pinned Go25 results, not generated Rust expectations"]
fn actual_go_twenty_five_structured_room_config_diagnostics() {
    let cases = std::fs::read_to_string(std::env::var("F103_JWT_GO_CASES").unwrap()).unwrap();
    let rows = std::fs::read_to_string(std::env::var("F103_JWT_GO_RESULTS").unwrap()).unwrap();
    let cases: Vec<Case> = cases
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect();
    let rows: Vec<Observation> = rows
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect();
    assert_eq!((cases.len(), rows.len()), (25, 25));
    let mut accepted = 0;
    let mut rejected = 0;
    for (case, go) in cases.iter().zip(rows) {
        assert_eq!(case.id, go.id);
        let raw = case
            .config_raw
            .as_deref()
            .or_else(|| case.config.as_deref().map(RawValue::get))
            .unwrap();
        match decode::<RoomConfiguration>(raw.as_bytes()) {
            Ok(value) => {
                accepted += 1;
                assert!(go.direct_error.is_empty(), "{}: Go rejected", case.id);
                assert!(go.verified, "{}", case.id);
                assert_eq!(
                    value,
                    crate::json::decode::<RoomConfiguration>(raw.as_bytes()).unwrap()
                );
            }
            Err(Error::Source(error)) => {
                rejected += 1;
                assert!(!go.direct_error.is_empty(), "{}: Go accepted", case.id);
                // Go's two source-defined prefix values are tested explicitly.
                // No token/suffix/whitespace normalization is permitted.
                let prefix = if go.direct_error.starts_with("proto:\u{a0}") {
                    Prefix::NonBreakingSpace
                } else {
                    Prefix::Space
                };
                assert_eq!(error.render(prefix), go.direct_error, "{}", case.id);
            }
            Err(Error::Decoder(error)) => panic!(
                "{}: unimplemented structured diagnostic family: {error}",
                case.id
            ),
        }
    }
    assert_eq!((accepted, rejected), (7, 18));
}
