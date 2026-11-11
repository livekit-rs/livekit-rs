//! Focused API tests for the yaml.v3 event parser port (expected values taken
//! from gopkg.in/yaml.v3 v3.0.1, see tests/oracles/yamlv3-parser/go_dump.txt).

use super::parser::{ErrorKind, EventKind, Mark, ParseError, Parser, ScalarStyle};

fn collect(input: &str) -> (Vec<EventKind>, Option<ParseError>) {
    let mut p = Parser::new(input.as_bytes());
    let mut out = Vec::new();
    loop {
        match p.parse() {
            Ok(ev) => {
                let end = ev.kind == EventKind::StreamEnd;
                out.push(ev.kind);
                if end {
                    return (out, None);
                }
            }
            Err(e) => return (out, Some(e)),
        }
    }
}

#[test]
fn resolves_tags_and_reports_scalar_properties() {
    let (events, err) =
        collect("%TAG !e! tag:example.com,2000:\n---\na: !e!x &v 'q'\nb: !!str 1\n");
    assert!(err.is_none());
    assert!(events.contains(&EventKind::Scalar {
        anchor: Some("v".into()),
        tag: Some("tag:example.com,2000:x".into()),
        value: "q".into(),
        style: ScalarStyle::SingleQuoted,
        plain_implicit: false,
        quoted_implicit: false,
    }));
    assert!(events.contains(&EventKind::Scalar {
        anchor: None,
        tag: Some("tag:yaml.org,2002:str".into()),
        value: "1".into(),
        style: ScalarStyle::Plain,
        plain_implicit: false,
        quoted_implicit: false,
    }));
}

#[test]
fn error_message_matches_yaml_v3_fail() {
    let (_, err) = collect("rtc: [\n");
    let err = err.unwrap();
    assert_eq!(err.kind, ErrorKind::Parser);
    // Parser errors use the 0-based context line as is (no +1).
    assert_eq!(err.message(), "line 1: did not find expected node content");

    // Scanner errors on the first line carry no "line N: " prefix (yaml.v3 quirk).
    let (_, err) = collect("a: b: c\n");
    assert_eq!(
        err.unwrap().message(),
        "mapping values are not allowed in this context"
    );

    // Scanner errors add one to the 0-based line.
    let (_, err) = collect("a:\n  b: 1\n  c: \"x\\q\"\n");
    assert_eq!(
        err.unwrap().message(),
        "line 3: found unknown escape character"
    );

    // Reader errors render only the problem.
    let (_, err) = collect("a: \u{1}\n");
    let err = err.unwrap();
    assert_eq!(err.kind, ErrorKind::Reader);
    assert_eq!(err.message(), "control characters are not allowed");
    assert_eq!((err.problem_offset, err.problem_value), (3, 1));

    let empty = ParseError {
        kind: ErrorKind::Parser,
        problem: String::new(),
        problem_mark: Mark::default(),
        problem_offset: 0,
        problem_value: 0,
        context: String::new(),
        context_mark: Mark::default(),
    };
    assert_eq!(empty.message(), "unknown problem parsing YAML content");
}

#[test]
fn parsing_is_lazy_and_errors_are_sticky() {
    let mut p = Parser::new(b"a: 1\n---\nb: [\n");
    let mut seen_doc_end = false;
    let err = loop {
        match p.parse() {
            Ok(ev) => {
                if matches!(ev.kind, EventKind::DocumentEnd { .. }) {
                    seen_doc_end = true;
                }
            }
            Err(e) => break e,
        }
    };
    // The first document is delivered completely before the second fails.
    assert!(seen_doc_end);
    assert_eq!(err.message(), "line 3: did not find expected node content");
    assert_eq!(p.parse().unwrap_err(), err);
    assert_eq!(p.parse().unwrap_err(), err);
}

#[test]
fn marks_and_comments() {
    let mut p = Parser::new("# head\nkey: é # line\n".as_bytes());
    let mut scalars = Vec::new();
    while let Ok(ev) = p.parse() {
        if let EventKind::Scalar { value, .. } = &ev.kind {
            scalars.push((
                value.clone(),
                ev.start,
                ev.end,
                ev.head_comment.clone(),
                ev.line_comment.clone(),
            ));
        }
        if ev.kind == EventKind::StreamEnd {
            break;
        }
    }
    assert_eq!(scalars.len(), 2);
    assert_eq!(scalars[0].0, "key");
    assert_eq!(
        scalars[0].1,
        Mark {
            index: 7,
            line: 1,
            column: 0
        }
    );
    assert_eq!(scalars[0].3, "# head");
    // Columns and indexes count characters, not bytes.
    assert_eq!(scalars[1].0, "é");
    assert_eq!(
        scalars[1].1,
        Mark {
            index: 12,
            line: 1,
            column: 5
        }
    );
    assert_eq!(
        scalars[1].2,
        Mark {
            index: 13,
            line: 1,
            column: 6
        }
    );
    assert_eq!(scalars[1].4, "# line");
}

#[test]
fn stream_end_repeats() {
    let mut p = Parser::new(b"");
    let mut last = None;
    for _ in 0..3 {
        let ev = p.parse().unwrap();
        last = Some(ev.kind);
    }
    assert_eq!(last, Some(EventKind::StreamEnd));
    assert_eq!(p.parse().unwrap().kind, EventKind::StreamEnd);
}
