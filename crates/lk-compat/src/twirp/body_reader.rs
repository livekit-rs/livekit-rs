//! Exact first-value framing for the fixed Go json.Decoder readValue contract.
//! This is only framing: existing descriptor-aware JSON decoding remains the
//! syntax/schema authority, retaining duplicates, numeric lexemes and unknowns.
use axum::body::Body;
use futures_util::StreamExt;
use serde::Deserialize;

#[derive(Debug)]
pub(super) enum ReadError {
    Invalid,
    Limit,
    Transport(axum::Error),
}
impl std::fmt::Display for ReadError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Invalid => f.write_str("invalid JSON value framing"),
            Self::Limit => f.write_str("http: request body too large"),
            Self::Transport(error) => std::fmt::Display::fmt(error, f),
        }
    }
}
impl std::error::Error for ReadError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Transport(error) => Some(error),
            _ => None,
        }
    }
}
#[derive(Clone, Copy)]
enum Number {
    Minus,
    Zero,
    Digits,
    Dot,
    Fraction,
    E,
    Sign,
    Exponent,
}
enum Kind {
    Start,
    Container(Vec<u8>),
    String,
    Literal(&'static [u8], usize),
    Number(Number),
    Complete,
}
struct Frame {
    kind: Kind,
    quoted: bool,
    escaped: bool,
}
impl Frame {
    fn new() -> Self {
        Self {
            kind: Kind::Start,
            quoted: false,
            escaped: false,
        }
    }
    /// True means the current byte is lookahead, not part of the first value.
    fn byte(&mut self, byte: u8) -> Result<bool, ReadError> {
        match &mut self.kind {
            Kind::Start => {
                self.kind = match byte {
                    b' ' | b'\n' | b'\r' | b'\t' => return Ok(false),
                    b'{' => Kind::Container(vec![b'}']),
                    b'[' => Kind::Container(vec![b']']),
                    b'"' => Kind::String,
                    b't' => Kind::Literal(b"true", 1),
                    b'f' => Kind::Literal(b"false", 1),
                    b'n' => Kind::Literal(b"null", 1),
                    b'-' => Kind::Number(Number::Minus),
                    b'0' => Kind::Number(Number::Zero),
                    b'1'..=b'9' => Kind::Number(Number::Digits),
                    _ => return Err(ReadError::Invalid),
                };
            }
            Kind::Container(stack) => {
                if self.quoted {
                    if self.escaped {
                        self.escaped = false;
                    } else if byte == b'\\' {
                        self.escaped = true;
                    } else if byte == b'"' {
                        self.quoted = false;
                    }
                } else {
                    match byte {
                        b'"' => self.quoted = true,
                        b'{' | b'[' => {
                            if stack.len() >= 10_000 {
                                return Err(ReadError::Invalid);
                            }
                            stack.push(if byte == b'{' { b'}' } else { b']' });
                        }
                        b'}' | b']' => {
                            if stack.pop() != Some(byte) {
                                return Err(ReadError::Invalid);
                            }
                            if stack.is_empty() {
                                self.kind = Kind::Complete;
                            }
                        }
                        _ => {}
                    }
                }
            }
            Kind::String => {
                if self.escaped {
                    self.escaped = false;
                } else if byte == b'\\' {
                    self.escaped = true;
                } else if byte == b'"' {
                    self.kind = Kind::Complete;
                }
            }
            Kind::Literal(word, at) => {
                if word.get(*at) != Some(&byte) {
                    return Err(ReadError::Invalid);
                }
                *at += 1;
                if *at == word.len() {
                    self.kind = Kind::Complete;
                }
            }
            Kind::Number(state) => {
                use Number::*;
                *state = match (*state, byte) {
                    (Minus, b'0') => Zero,
                    (Minus, b'1'..=b'9') => Digits,
                    (Digits, b'0'..=b'9') => Digits,
                    (Zero | Digits, b'.') => Dot,
                    (Zero | Digits | Fraction, b'e' | b'E') => E,
                    (Dot | Fraction, b'0'..=b'9') => Fraction,
                    (E, b'+' | b'-') => Sign,
                    (E | Sign | Exponent, b'0'..=b'9') => Exponent,
                    (Zero | Digits | Fraction | Exponent, _) => return Ok(true),
                    _ => return Err(ReadError::Invalid),
                };
            }
            Kind::Complete => return Ok(true),
        }
        Ok(false)
    }
    fn container_done(&self) -> bool {
        matches!(self.kind, Kind::Complete)
    }
}

/// A live-body prefix has no EOF. The mature parser must distinguish a real
/// syntax error from needing bytes beyond the currently available prefix.
struct AvailablePrefix<'a>(&'a [u8]);
impl std::io::Read for AvailablePrefix<'_> {
    fn read(&mut self, out: &mut [u8]) -> std::io::Result<usize> {
        if out.is_empty() {
            return Ok(0);
        }
        if self.0.is_empty() {
            return Err(std::io::ErrorKind::WouldBlock.into());
        }
        let count = out.len().min(self.0.len());
        out[..count].copy_from_slice(&self.0[..count]);
        self.0 = &self.0[count..];
        Ok(count)
    }
}

/// Read only the first raw JSON value. A completed object/array does not need a
/// subsequent read; strings/literals/numbers require real lookahead or EOF, as
/// Go readValue does. No buffer-boundary or parseable-number-prefix EOF is made.
pub(super) async fn json(body: Body, limit: usize) -> Result<Vec<u8>, ReadError> {
    let mut source = body.into_data_stream();
    let mut frame = Frame::new();
    let mut bytes = Vec::new();
    let mut compound = None;
    let mut consumed = 0;
    while let Some(chunk) = source.next().await {
        let chunk = chunk.map_err(ReadError::Transport)?;
        for &byte in chunk.as_ref() {
            // A delimiter beyond the configured MaxBytesReader budget is not
            // available lookahead. Exact-cap EOF is handled only by real EOF.
            if consumed == limit {
                return Err(ReadError::Limit);
            }
            consumed += 1;
            let leading_space =
                matches!(&frame.kind, Kind::Start) && matches!(byte, b' ' | b'\n' | b'\r' | b'\t');
            if frame.byte(byte)? {
                return Ok(bytes);
            }
            if !leading_space {
                bytes.push(byte);
            }
            if compound.is_none() && !byte.is_ascii_whitespace() {
                compound = Some(matches!(byte, b'{' | b'['));
            }
            if compound == Some(true) && frame.container_done() {
                return Ok(bytes);
            }
        }
        // Go scanner rejects an invalid prefix without waiting for another
        // read. IgnoredAny checks syntax only, with digit scanning rather than
        // f64 conversion; it never reconstructs/normalizes the raw bytes.
        let mut syntax = serde_json::Deserializer::from_reader(AvailablePrefix(&bytes));
        if let Err(error) = serde::de::IgnoredAny::deserialize(&mut syntax)
            && error.io_error_kind() != Some(std::io::ErrorKind::WouldBlock)
        {
            return Err(ReadError::Invalid);
        }
    }
    // Preserve raw input and let the existing typed decoder reject incomplete
    // values, empty input, invalid escapes/UTF8 and malformed nested JSON.
    Ok(bytes)
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::Bytes;
    use futures_util::stream;
    fn chunks(parts: &[&'static [u8]], error: bool) -> Body {
        let mut values: Vec<Result<Bytes, std::io::Error>> =
            parts.iter().map(|p| Ok(Bytes::from_static(p))).collect();
        if error {
            values.push(Err(std::io::Error::other("owned read failure")));
        }
        Body::from_stream(stream::iter(values))
    }
    #[tokio::test]
    async fn complete_compound_stops_before_transport_error_or_overflow() {
        for parts in [
            vec![b"{}".as_slice(), b"huge trailing"],
            vec![b"{".as_slice(), b"}", b"huge trailing"],
        ] {
            assert_eq!(json(chunks(&parts, true), 2).await.unwrap(), b"{}");
        }
        assert_eq!(
            json(Body::from("{}garbage beyond cap"), 2).await.unwrap(),
            b"{}"
        );
    }
    #[tokio::test]
    async fn preserves_exact_duplicate_unknown_and_numeric_bytes() {
        let raw = b" {\"name\":\"a\",\"name\":\"b\",\"unknown\":18446744073709551615}";
        let mut body = raw.to_vec();
        body.extend_from_slice(b" INVALID trailing");
        assert_eq!(json(Body::from(body), usize::MAX).await.unwrap(), &raw[1..]);
    }
    #[tokio::test]
    async fn numbers_wait_for_real_delimiter_or_eof_not_chunk_boundary() {
        assert_eq!(
            json(chunks(&[b"1", b"e", b"+2", b"X"], false), 20)
                .await
                .unwrap(),
            b"1e+2"
        );
        assert_eq!(
            json(chunks(&[b"1", b"2", b"3"], false), 20).await.unwrap(),
            b"123"
        );
        assert!(matches!(
            json(chunks(&[b"1"], true), 20).await,
            Err(ReadError::Transport(_))
        ));
        assert!(matches!(
            json(chunks(&[b"1e", b"X"], false), 20).await,
            Err(ReadError::Invalid)
        ));
        assert_eq!(json(Body::from("01"), 20).await.unwrap(), b"0");
    }
    #[tokio::test]
    async fn primitive_and_string_need_lookahead_while_compounds_do_not() {
        for raw in [b"true".as_slice(), b"null", b"\"x\""] {
            assert!(matches!(
                json(chunks(&[raw], true), 100).await,
                Err(ReadError::Transport(_))
            ));
            assert_eq!(json(chunks(&[raw, b"X"], true), 100).await.unwrap(), raw);
            assert_eq!(json(chunks(&[raw], false), 100).await.unwrap(), raw);
        }
    }
    #[tokio::test]
    async fn exact_limit_eof_and_overflow_have_distinct_read_outcomes() {
        assert_eq!(json(Body::from("123"), 3).await.unwrap(), b"123");
        assert!(matches!(
            json(Body::from("123 "), 3).await,
            Err(ReadError::Limit)
        ));
        assert!(matches!(
            json(Body::from("{\"a\":123}"), 5).await,
            Err(ReadError::Limit)
        ));
        let error = json(chunks(&[b"{"], true), 100).await.unwrap_err();
        match error {
            ReadError::Transport(error) => {
                assert!(error.to_string().contains("owned read failure"))
            }
            _ => panic!("transport failure must remain distinct"),
        }
    }
    #[tokio::test]
    async fn escaped_delimiters_and_split_utf8_remain_exact() {
        let raw = "[\"\\\"}]é\",{}]".as_bytes();
        let values: Vec<Result<Bytes, std::io::Error>> = raw
            .iter()
            .map(|b| Ok(Bytes::copy_from_slice(&[*b])))
            .collect();
        assert_eq!(
            json(Body::from_stream(stream::iter(values)), 100)
                .await
                .unwrap(),
            raw
        );
    }
    /// Coordinator supplies independently executed Go output; never authored
    /// expected GREEN values. This is reader framing, not public schema proof.
    #[tokio::test]
    #[ignore = "requires actual Go first-value oracle results"]
    async fn actual_go_first_value_reader_oracle() {
        use serde::Deserialize;
        let cases = std::fs::read_to_string(std::env::var("F101_JSON_CASES").unwrap()).unwrap();
        let results =
            std::fs::read_to_string(std::env::var("F101_JSON_GO_RESULTS").unwrap()).unwrap();
        let rows: Vec<serde_json::Value> = results
            .lines()
            .map(|line| serde_json::from_str(line).unwrap())
            .collect();
        let inputs: Vec<serde_json::Value> = cases
            .lines()
            .map(|line| serde_json::from_str(line).unwrap())
            .collect();
        assert_eq!(inputs.len(), rows.len());
        for (input, expected) in inputs.iter().zip(&rows) {
            assert_eq!(input["name"], expected["name"]);
            let mut parts: Vec<Result<Bytes, std::io::Error>> = input["parts"]
                .as_array()
                .unwrap()
                .iter()
                .map(|part| Ok(Bytes::copy_from_slice(part.as_str().unwrap().as_bytes())))
                .collect();
            if input["final_error"].as_bool().unwrap() {
                parts.push(Err(std::io::Error::other("owned read failure")));
            }
            let max = input["limit"].as_u64().unwrap();
            let max = if max == 0 {
                usize::MAX
            } else {
                usize::try_from(max).unwrap()
            };
            let read = json(Body::from_stream(stream::iter(parts)), max).await;
            let actual = read.ok().and_then(|raw| {
                let mut decoder = serde_json::Deserializer::from_slice(&raw);
                Box::<serde_json::value::RawValue>::deserialize(&mut decoder)
                    .ok()
                    .map(|value| value.get().as_bytes().to_vec())
            });
            assert_eq!(
                actual.is_some(),
                expected["status"] == "ok",
                "{}",
                input["name"]
            );
            if let Some(raw) = actual {
                let hex: String = raw.iter().map(|b| format!("{b:02x}")).collect();
                assert_eq!(
                    hex,
                    expected["raw_hex"].as_str().unwrap(),
                    "{}",
                    input["name"]
                );
            }
        }
    }
    #[tokio::test]
    async fn invalid_open_value_fails_before_polling_a_non_eof_body() {
        use std::task::Poll;
        for prefix in [
            b"{\"name\":x".as_slice(),
            b"{\"name\":\"bare\x01",
            b"{\"name\":\"bad\\q",
            b"{\"x\":1e+x",
            b"{\"x\":01",
        ] {
            let source =
                stream::once(async move { Ok::<_, std::io::Error>(Bytes::from_static(prefix)) })
                    .chain(stream::pending());
            let mut read = Box::pin(json(Body::from_stream(source), usize::MAX));
            assert!(matches!(
                std::future::Future::poll(
                    read.as_mut(),
                    &mut std::task::Context::from_waker(std::task::Waker::noop())
                ),
                Poll::Ready(Err(ReadError::Invalid))
            ));
        }
        for prefix in [
            b"{\"name\":\"valid".as_slice(),
            b"{\"n\":1844674407370955161599999999999999999999999999",
            b"1e+",
            b"1.",
            b"-",
            b"{\"x\":1e+",
            b"{\"x\":1.",
            b"{\"x\":-",
        ] {
            let source =
                stream::once(async move { Ok::<_, std::io::Error>(Bytes::from_static(prefix)) })
                    .chain(stream::pending());
            let mut read = Box::pin(json(Body::from_stream(source), usize::MAX));
            assert!(matches!(
                std::future::Future::poll(
                    read.as_mut(),
                    &mut std::task::Context::from_waker(std::task::Waker::noop())
                ),
                Poll::Pending
            ));
        }
    }
    #[tokio::test]
    async fn framing_depth_matches_go_not_a_serde_128_limit() {
        use std::task::Poll;
        for depth in [129, 10_000] {
            let raw = format!("{}0{}", "[".repeat(depth), "]".repeat(depth));
            assert_eq!(
                json(Body::from(raw.clone()), usize::MAX).await.unwrap(),
                raw.as_bytes()
            );
            // Force the mature syntax check before a genuine pending next read.
            let prefix = Bytes::from("[".repeat(depth));
            let source = stream::once(async move { Ok::<_, std::io::Error>(prefix) })
                .chain(stream::pending());
            let mut read = Box::pin(json(Body::from_stream(source), usize::MAX));
            assert!(matches!(
                std::future::Future::poll(
                    read.as_mut(),
                    &mut std::task::Context::from_waker(std::task::Waker::noop())
                ),
                Poll::Pending
            ));
        }
        let raw = format!("{}0{}", "[".repeat(10_001), "]".repeat(10_001));
        assert!(matches!(
            json(Body::from(raw), usize::MAX).await,
            Err(ReadError::Invalid)
        ));
    }
    #[tokio::test]
    #[ignore = "requires actual Go no-EOF read-boundary oracle results"]
    async fn actual_go_no_eof_read_boundary_oracle() {
        use std::task::Poll;
        let cases = std::fs::read_to_string(std::env::var("F101_NOEOF_CASES").unwrap()).unwrap();
        let results =
            std::fs::read_to_string(std::env::var("F101_NOEOF_GO_RESULTS").unwrap()).unwrap();
        let cases: Vec<serde_json::Value> = cases
            .lines()
            .map(|s| serde_json::from_str(s).unwrap())
            .collect();
        let results: Vec<serde_json::Value> = results
            .lines()
            .map(|s| serde_json::from_str(s).unwrap())
            .collect();
        assert_eq!(cases.len(), results.len());
        for (input, expected) in cases.iter().zip(&results) {
            assert_eq!(input["name"], expected["name"]);
            let prefix = Bytes::copy_from_slice(input["prefix"].as_str().unwrap().as_bytes());
            let source = stream::once(async move { Ok::<_, std::io::Error>(prefix) })
                .chain(stream::pending());
            let mut read = Box::pin(json(Body::from_stream(source), usize::MAX));
            let observed = std::future::Future::poll(
                read.as_mut(),
                &mut std::task::Context::from_waker(std::task::Waker::noop()),
            );
            let demanded = observed.is_pending();
            if !demanded {
                assert_eq!(
                    matches!(observed, Poll::Ready(Err(_))),
                    !expected["final_error"].as_str().unwrap().is_empty(),
                    "{}",
                    input["name"]
                );
            }
            assert_eq!(
                demanded,
                expected["demanded_second_read"].as_bool().unwrap(),
                "{}",
                input["name"]
            );
        }
    }
}
