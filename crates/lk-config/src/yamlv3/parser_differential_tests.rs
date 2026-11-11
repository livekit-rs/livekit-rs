//! Differential test: the Rust port of the yaml.v3 event parser must reproduce
//! the event stream (types, properties, marks, comments) and the error fields
//! plus `(*parser).fail()` text of the real gopkg.in/yaml.v3 v3.0.1, as dumped by
//! tests/oracles/yamlv3-parser/go/dump_test.go into go_dump.txt there.

use std::collections::HashMap;
use std::fmt::Write as _;

use super::parser::{ErrorKind, Event, EventKind, Mark, ParseError, Parser, ScalarStyle};

const DUMP_VERBATIM_EVENTS: usize = 200;

fn unescape(s: &str) -> Vec<u8> {
    let b = s.as_bytes();
    let mut out = Vec::new();
    let mut i = 0;
    while i < b.len() {
        if b[i] != b'\\' {
            out.push(b[i]);
            i += 1;
            continue;
        }
        i += 1;
        match b[i] {
            b'n' => out.push(b'\n'),
            b'r' => out.push(b'\r'),
            b't' => out.push(b'\t'),
            b'\\' => out.push(b'\\'),
            b'x' => {
                let hex = std::str::from_utf8(&b[i + 1..i + 3]).unwrap();
                out.push(u8::from_str_radix(hex, 16).unwrap());
                i += 2;
            }
            c => panic!("bad corpus escape {c}"),
        }
        i += 1;
    }
    out
}

fn esc(b: &[u8]) -> String {
    let mut s = String::from("\"");
    for &c in b {
        if (0x20..0x7f).contains(&c) && c != b'\\' && c != b'"' {
            s.push(c as char);
        } else {
            let _ = write!(s, "\\x{c:02x}");
        }
    }
    s.push('"');
    s
}

fn opt(o: &Option<String>) -> String {
    match o {
        Some(v) if !v.is_empty() => esc(v.as_bytes()),
        _ => "-".to_string(),
    }
}

fn mark(m: Mark) -> String {
    format!("{}.{}.{}", m.index, m.line, m.column)
}

fn style_num(s: ScalarStyle) -> u8 {
    // yaml_scalar_style_t values
    match s {
        ScalarStyle::Any => 0,
        ScalarStyle::Plain => 2,
        ScalarStyle::SingleQuoted => 4,
        ScalarStyle::DoubleQuoted => 8,
        ScalarStyle::Literal => 16,
        ScalarStyle::Folded => 32,
    }
}

fn dump_event(ev: &Event) -> String {
    let b = |v: bool| u8::from(v);
    let mut s = String::from("EV ");
    match &ev.kind {
        EventKind::StreamStart => s.push_str("STREAM-START"),
        EventKind::StreamEnd => s.push_str("STREAM-END"),
        EventKind::DocumentStart { implicit } => {
            let _ = write!(s, "DOC-START implicit={}", b(*implicit));
        }
        EventKind::DocumentEnd { implicit } => {
            let _ = write!(s, "DOC-END implicit={}", b(*implicit));
        }
        EventKind::Alias { anchor } => {
            let _ = write!(s, "ALIAS anchor={}", esc(anchor.as_bytes()));
        }
        EventKind::Scalar {
            anchor,
            tag,
            value,
            style,
            plain_implicit,
            quoted_implicit,
        } => {
            let _ = write!(
                s,
                "SCALAR anchor={} tag={} value={} style={} pi={} qi={}",
                opt(anchor),
                opt(tag),
                esc(value.as_bytes()),
                style_num(*style),
                b(*plain_implicit),
                b(*quoted_implicit)
            );
        }
        EventKind::SequenceStart {
            anchor,
            tag,
            implicit,
            flow,
        } => {
            let _ = write!(
                s,
                "SEQ-START anchor={} tag={} implicit={} flow={}",
                opt(anchor),
                opt(tag),
                b(*implicit),
                b(*flow)
            );
        }
        EventKind::SequenceEnd => s.push_str("SEQ-END"),
        EventKind::MappingStart {
            anchor,
            tag,
            implicit,
            flow,
        } => {
            let _ = write!(
                s,
                "MAP-START anchor={} tag={} implicit={} flow={}",
                opt(anchor),
                opt(tag),
                b(*implicit),
                b(*flow)
            );
        }
        EventKind::MappingEnd => s.push_str("MAP-END"),
        EventKind::TailComment => s.push_str("TAIL"),
    }
    let _ = write!(
        s,
        " {}-{} h={} l={} f={}",
        mark(ev.start),
        mark(ev.end),
        esc(ev.head_comment.as_bytes()),
        esc(ev.line_comment.as_bytes()),
        esc(ev.foot_comment.as_bytes())
    );
    s
}

fn dump_error(e: &ParseError) -> Vec<String> {
    let kind = match e.kind {
        ErrorKind::Reader => "reader",
        ErrorKind::Scanner => "scanner",
        ErrorKind::Parser => "parser",
    };
    vec![
        format!(
            "ERR kind={} problem={} pmark={} poff={} pval={} ctx={} cmark={}",
            kind,
            esc(e.problem.as_bytes()),
            mark(e.problem_mark),
            e.problem_offset,
            e.problem_value,
            esc(e.context.as_bytes()),
            mark(e.context_mark)
        ),
        format!("MSG {}", esc(format!("yaml: {}", e.message()).as_bytes())),
    ]
}

struct Folder {
    lines: Vec<String>,
    emitted: usize,
    rest: u64,
    rest_n: usize,
}

impl Folder {
    fn emit(&mut self, line: String) {
        if self.rest_n == 0 && self.emitted < DUMP_VERBATIM_EVENTS {
            self.emitted += 1;
            self.lines.push(line);
            return;
        }
        for c in line.bytes().chain(std::iter::once(b'\n')) {
            self.rest ^= u64::from(c);
            self.rest = self.rest.wrapping_mul(1_099_511_628_211);
        }
        self.rest_n += 1;
    }

    fn flush(&mut self) {
        if self.rest_n > 0 {
            self.lines
                .push(format!("EVREST n={} fnv64={:016x}", self.rest_n, self.rest));
            self.rest_n = 0;
        }
    }
}

/// Returns the dump lines (without the DEC line) and the error, if any.
fn rust_dump(input: &[u8]) -> (Vec<String>, Option<ParseError>) {
    let mut p = Parser::new(input);
    let mut f = Folder {
        lines: Vec::new(),
        emitted: 0,
        rest: 14_695_981_039_346_656_037,
        rest_n: 0,
    };
    let mut err = None;
    for _ in 0..1_000_000 {
        match p.parse() {
            Ok(ev) => {
                let end = ev.kind == EventKind::StreamEnd;
                f.emit(dump_event(&ev));
                if end {
                    break;
                }
            }
            Err(e) => {
                f.flush();
                f.lines.extend(dump_error(&e));
                // After an error the same error must keep being returned.
                assert_eq!(p.parse().unwrap_err(), e, "error is not sticky");
                err = Some(e);
                break;
            }
        }
    }
    f.flush();
    (f.lines, err)
}

#[test]
fn differential_against_go_yaml_v3() {
    let root = env!("CARGO_MANIFEST_DIR");
    // YAMLV3_CORPUS / YAMLV3_DUMP allow running ad-hoc (e.g. randomized) corpora.
    let corpus_path = std::env::var("YAMLV3_CORPUS")
        .unwrap_or_else(|_| format!("{root}/../../tests/oracles/yamlv3-parser/corpus.txt"));
    let dump_path = std::env::var("YAMLV3_DUMP")
        .unwrap_or_else(|_| format!("{root}/../../tests/oracles/yamlv3-parser/go_dump.txt"));
    let corpus = std::fs::read_to_string(corpus_path).unwrap();
    let dump = std::fs::read_to_string(dump_path).unwrap();

    // Parse the Go dump into name -> lines.
    let mut go: HashMap<String, Vec<String>> = HashMap::new();
    let mut order = Vec::new();
    let mut cur: Option<String> = None;
    for line in dump.lines() {
        if let Some(name) = line.strip_prefix("=== ") {
            cur = Some(name.to_string());
            order.push(name.to_string());
            go.insert(name.to_string(), Vec::new());
        } else {
            go.get_mut(cur.as_ref().unwrap())
                .unwrap()
                .push(line.to_string());
        }
    }

    let mut total = 0;
    let mut passed = 0;
    let mut error_cases = 0;
    let mut decode_checked = 0;
    let mut failures = Vec::new();
    for line in corpus.lines().filter(|l| !l.is_empty()) {
        let (name, enc) = line.split_once('\t').unwrap();
        let input = unescape(enc);
        total += 1;
        let Some(expected) = go.get(name) else {
            failures.push(format!("{name}: missing from Go dump"));
            continue;
        };
        let (dec, expected_events): (Vec<&String>, Vec<&String>) =
            expected.iter().partition(|l| l.starts_with("DEC "));
        let expected_events: Vec<String> = expected_events.into_iter().cloned().collect();
        let (got, err) = rust_dump(&input);
        if err.is_some() {
            error_cases += 1;
        }
        // Cross-check message() against yaml.Decoder.Decode where the decoder
        // reported exactly the low-level parser failure.
        if let (Some(e), Some(d)) = (&err, dec.first()) {
            let rendered = format!("DEC {}", esc(format!("yaml: {}", e.message()).as_bytes()));
            let go_msg = expected_events
                .iter()
                .find(|l| l.starts_with("MSG "))
                .map(|m| &m[4..]);
            if go_msg == Some(&d[4..]) {
                decode_checked += 1;
                if &rendered != *d {
                    failures.push(format!(
                        "{name}: decode error mismatch\n  go:   {d}\n  rust: {rendered}"
                    ));
                    continue;
                }
            }
        }
        if got == expected_events {
            passed += 1;
        } else {
            let mut msg = format!("{name}: mismatch\n");
            let n = got.len().max(expected_events.len());
            for i in 0..n {
                let g = got.get(i).map_or("<none>", String::as_str);
                let e = expected_events.get(i).map_or("<none>", String::as_str);
                if g != e {
                    let _ = write!(msg, "  line {i}\n  go:   {e}\n  rust: {g}");
                    break;
                }
            }
            failures.push(msg);
        }
    }
    eprintln!(
        "differential: {passed}/{total} cases identical ({error_cases} error cases, \
         {decode_checked} decode-error cross-checks)"
    );
    for f in failures.iter().take(15) {
        eprintln!("{f}");
    }
    assert!(
        failures.is_empty(),
        "{} of {total} cases differ",
        failures.len()
    );
    assert!(total >= 300);
}
