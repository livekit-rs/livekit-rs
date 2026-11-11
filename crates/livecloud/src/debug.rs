//! Native diagnostics behind the standalone debug listener. CPU/heap payloads
//! use the real Rust sampler and standard pprof protobuf, readable by pprof.
//! Rust thread dumps and sampling traces identify their native runtime/format;
//! they never invent Go goroutines, GC fields or Go execution-trace records.

#[path = "debug_media.rs"]
mod media;

use crate::profiling::{Capture, HeapSampling};
use axum::Router;
use axum::body::{Body, HttpBody, to_bytes};
use axum::extract::rejection::PathRejection;
use axum::extract::{FromRequest, Path, RawQuery, Request, State};
use axum::http::{Method, StatusCode, header};
use axum::middleware::{self, Next};
use axum::response::{IntoResponse, Response};
use axum::routing::any;
use rtc_runtime::RoomManager;
use std::{
    collections::HashMap,
    convert::Infallible,
    future::poll_fn,
    pin::Pin,
    sync::Arc,
    time::{Duration, UNIX_EPOCH},
};

#[derive(Clone)]
struct DebugState {
    manager: RoomManager,
    _heap: Arc<HeapSampling>,
}

/// Builds the debug mux mirroring upstream's route registrations.
pub fn router(manager: RoomManager) -> Router {
    let routes = Router::new()
        // The dedicated Go debug mux registers handlers without method filters.
        .route("/debug/pprof/", any(index))
        .route("/debug/pprof/cmdline", any(cmdline))
        .route("/debug/pprof/profile", any(profile))
        .route("/debug/pprof/symbol", any(symbol).post(symbol_post))
        .route("/debug/pprof/trace", any(trace))
        .route("/debug/pprof/{*name}", any(named_profile))
        .route("/debug/goroutine", any(goroutines))
        .route("/debug/rooms", any(rooms))
        .fallback(unknown_debug_path)
        .with_state(DebugState {
            manager,
            _heap: Arc::new(HeapSampling::start()),
        });
    // Route rewriting must run before matching the diagnostic handlers.
    Router::new()
        .fallback_service(routes)
        .layer(middleware::from_fn(canonical_debug_path))
}

async fn canonical_debug_path(mut request: Request, next: Next) -> Response {
    // ServeMux cleans the escaped path before dispatching, even on unknown routes.
    let path = request.uri().path();
    if !path
        .as_bytes()
        .split(|&byte| byte == b'%')
        .skip(1)
        .all(|part| {
            part.get(..2)
                .is_some_and(|hex| hex.iter().all(u8::is_ascii_hexdigit))
        })
    {
        return (
            StatusCode::BAD_REQUEST,
            [(header::CONNECTION, "close")],
            "400 Bad Request",
        )
            .into_response();
    }
    let clean = if request.method() != Method::CONNECT
        && (path.contains("//") || path.split('/').any(|segment| matches!(segment, "." | "..")))
    {
        Some(
            String::from_utf8(clean_debug_path(path.as_bytes()))
                .expect("cleaning the URI preserves UTF-8"),
        )
    } else {
        None
    };
    let route_path = clean.as_deref().unwrap_or(path);
    let decoded = decode_route_literals(route_path);
    let matched_path = decoded.as_deref().unwrap_or(route_path);
    if matched_path == "/debug/pprof" {
        // ServeMux matches the cleaned escaped path, but builds the slash
        // redirect from the decoded original URL.Path.
        let decoded_path = decode_path_bytes(path, |_| true);
        let mut target = clean_debug_path(decoded_path.as_deref().unwrap_or(path.as_bytes()));
        if !target.ends_with(b"/") {
            target.push(b'/');
        }
        return debug_redirect(
            request.method(),
            escape_debug_path(&target),
            request.uri().query(),
        );
    }
    if let Some(clean) = clean {
        return debug_redirect(
            request.method(),
            escape_debug_path(clean.as_bytes()),
            request.uri().query(),
        );
    }
    if let Some(mut decoded) = decoded {
        if let Some(query) = request.uri().query() {
            decoded.push('?');
            decoded.push_str(query);
        }
        let mut parts = request.uri().clone().into_parts();
        // Decoding ASCII letters and digits keeps the path-and-query valid.
        parts.path_and_query = Some(decoded.parse().expect("valid decoded debug path"));
        *request.uri_mut() = axum::http::Uri::from_parts(parts).expect("valid debug URI");
    }
    next.run(request).await
}

fn clean_debug_path(path: &[u8]) -> Vec<u8> {
    let mut segments = Vec::new();
    for segment in path.split(|&byte| byte == b'/') {
        match segment {
            b"" | b"." => {}
            b".." => {
                segments.pop();
            }
            _ => segments.push(segment),
        }
    }
    let mut clean = vec![b'/'];
    for segment in segments {
        if clean.len() > 1 {
            clean.push(b'/');
        }
        clean.extend_from_slice(segment);
    }
    if clean.len() > 1 && path.ends_with(b"/") {
        clean.push(b'/');
    }
    clean
}

fn decode_route_literals(path: &str) -> Option<String> {
    decode_path_bytes(path, |byte| byte.is_ascii_alphanumeric())
        .map(|decoded| String::from_utf8(decoded).expect("decoding ASCII preserves UTF-8"))
}

fn decode_path_bytes(path: &str, accepts: impl Fn(u8) -> bool) -> Option<Vec<u8>> {
    let first = path.find('%')?;
    let path = path.as_bytes();
    let mut decoded = Vec::with_capacity(path.len());
    decoded.extend_from_slice(&path[..first]);
    let mut rest = &path[first..];
    while let Some(offset) = rest.iter().position(|&byte| byte == b'%') {
        decoded.extend_from_slice(&rest[..offset]);
        rest = &rest[offset..];
        let byte = rest.get(1..3).and_then(|hex| {
            let high = char::from(hex[0]).to_digit(16)?;
            let low = char::from(hex[1]).to_digit(16)?;
            Some(((high << 4) | low) as u8)
        });
        if let Some(byte) = byte.filter(|&byte| accepts(byte)) {
            decoded.push(byte);
            rest = &rest[3..];
        } else {
            decoded.push(b'%');
            rest = &rest[1..];
        }
    }
    decoded.extend_from_slice(rest);
    (decoded != path).then_some(decoded)
}

fn escape_debug_path(clean: &[u8]) -> String {
    // Go serializes the cleaned escaped path as URL.Path, escaping '%' again.
    use std::fmt::Write;
    let mut location = String::with_capacity(clean.len());
    for &byte in clean {
        if byte.is_ascii_alphanumeric() || b"-_.~$&+,/:;=@".contains(&byte) {
            location.push(char::from(byte));
        } else {
            let _ = write!(location, "%{byte:02X}");
        }
    }
    location
}

async fn unknown_debug_path() -> Response {
    (
        StatusCode::NOT_FOUND,
        [
            (header::CONTENT_TYPE, "text/plain; charset=utf-8"),
            (header::X_CONTENT_TYPE_OPTIONS, "nosniff"),
        ],
        "404 page not found\n",
    )
        .into_response()
}

fn debug_redirect(method: &Method, mut location: String, query: Option<&str>) -> Response {
    if let Some(query) = query.filter(|query| !query.is_empty()) {
        location.push('?');
        location.push_str(query);
    }
    let body = if *method == Method::GET {
        let escaped = location
            .replace('&', "&amp;")
            .replace('<', "&lt;")
            .replace('>', "&gt;")
            .replace('"', "&#34;")
            .replace('\'', "&#39;");
        Body::from(format!("<a href=\"{escaped}\">Temporary Redirect</a>.\n\n"))
    } else if *method == Method::HEAD {
        // Preserve an unknown length until Axum suppresses the HEAD body.
        Body::from_stream(Body::empty().into_data_stream())
    } else {
        Body::empty()
    };
    let mut response = (
        StatusCode::TEMPORARY_REDIRECT,
        [(header::LOCATION, location)],
        body,
    )
        .into_response();
    if matches!(*method, Method::GET | Method::HEAD) {
        response.headers_mut().insert(
            header::CONTENT_TYPE,
            header::HeaderValue::from_static("text/html; charset=utf-8"),
        );
    }
    response
}

async fn index() -> Response {
    (StatusCode::OK,[
        (header::CONTENT_TYPE,"text/html; charset=utf-8"),
        (header::X_CONTENT_TYPE_OPTIONS,"nosniff"),
    ],concat!(
        "<html><head><title>livekit debug</title></head><body><h2>Rust runtime diagnostics</h2>\n",
        "<a href=\"/debug/pprof/profile\">profile</a> CPU samples (standard pprof)<br>\n",
        "<a href=\"/debug/pprof/heap\">heap</a> sampled live allocations (standard pprof)<br>\n",
        "<a href=\"/debug/pprof/allocs\">allocs</a> sampled allocations (standard pprof)<br>\n",
        "<a href=\"/debug/goroutine\">thread stacks</a> native sampled Rust stacks<br>\n",
        "<a href=\"/debug/pprof/trace\">sampling trace</a> Chrome JSON, native Rust threads<br>\n",
        "<a href=\"/debug/rooms\">rooms</a> live owner JSON<br>\n",
        "<a href=\"/debug/pprof/cmdline\">cmdline</a><br><a href=\"/debug/pprof/symbol\">symbol</a> sampled native symbols\n",
        "</body></html>\n")).into_response()
}

async fn cmdline() -> Response {
    // Go pprof writes NUL-separated os.Args; same external contract here.
    let body = cmdline_body(&std::env::args().collect::<Vec<_>>());
    (
        StatusCode::OK,
        [
            (header::CONTENT_TYPE, "text/plain; charset=utf-8"),
            (header::X_CONTENT_TYPE_OPTIONS, "nosniff"),
        ],
        body,
    )
        .into_response()
}

fn cmdline_body(args: &[String]) -> String {
    args.join("\0")
}

struct PprofForm(HashMap<String, String>);

impl<S: Send + Sync> FromRequest<S> for PprofForm {
    type Rejection = Infallible;

    async fn from_request(request: Request, _state: &S) -> Result<Self, Self::Rejection> {
        let (parts, body) = request.into_parts();
        let mut fields = HashMap::new();
        // Go Request.FormValue uses the first value from POST, PUT or PATCH
        // form data ahead of the raw query. An unreadable or oversized form
        // leaves the query usable.
        let urlencoded = parts
            .headers
            .get(header::CONTENT_TYPE)
            .and_then(|value| value.to_str().ok())
            .and_then(|value| value.split(';').next())
            .is_some_and(|value| {
                value
                    .trim()
                    .eq_ignore_ascii_case("application/x-www-form-urlencoded")
            });
        if urlencoded
            && matches!(parts.method, Method::POST | Method::PUT | Method::PATCH)
            && let Ok(form) = to_bytes(body, 10 << 20).await
        {
            extend_pprof_form(&mut fields, &form);
        }
        if let Some(query) = parts.uri.query() {
            extend_pprof_form(&mut fields, query.as_bytes());
        }
        Ok(Self(fields))
    }
}

fn extend_pprof_form(fields: &mut HashMap<String, String>, form: &[u8]) {
    for pair in form.split(|&byte| byte == b'&') {
        if pair.is_empty() || pair.contains(&b';') {
            continue;
        }
        let mut components = pair.splitn(2, |&byte| byte == b'=');
        let key = components.next().unwrap_or_default();
        let value = components.next().unwrap_or_default();
        if let (Some(key), Some(value)) = (decode_form_value(key), decode_form_value(value)) {
            fields.entry(key).or_insert(value);
        }
    }
}

fn decode_form_value(bytes: &[u8]) -> Option<String> {
    let mut decoded = Vec::with_capacity(bytes.len());
    let mut bytes = bytes.iter().copied();
    while let Some(byte) = bytes.next() {
        decoded.push(match byte {
            b'+' => b' ',
            b'%' => {
                let high = char::from(bytes.next()?).to_digit(16)?;
                let low = char::from(bytes.next()?).to_digit(16)?;
                ((high << 4) | low) as u8
            }
            _ => byte,
        });
    }
    Some(String::from_utf8_lossy(&decoded).into_owned())
}

fn profile_seconds(query: &HashMap<String, String>) -> Duration {
    let seconds = query
        .get("seconds")
        .and_then(|value| value.parse::<i64>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(30);
    Duration::from_secs(seconds as u64)
}
fn trace_seconds(query: &HashMap<String, String>) -> Duration {
    let seconds = query
        .get("seconds")
        .and_then(|value| value.parse::<f64>().ok())
        .filter(|value| value.is_finite() && *value > 0.0)
        .unwrap_or(1.0);
    Duration::try_from_secs_f64(seconds).unwrap_or(Duration::from_secs(1))
}
fn collector_error(error: impl std::fmt::Display) -> Response {
    pprof_error(StatusCode::INTERNAL_SERVER_ERROR, error)
}

fn pprof_error(status: StatusCode, message: impl std::fmt::Display) -> Response {
    (
        status,
        [
            (header::CONTENT_TYPE, "text/plain; charset=utf-8"),
            (header::X_CONTENT_TYPE_OPTIONS, "nosniff"),
            (header::HeaderName::from_static("x-go-pprof"), "1"),
        ],
        format!("{message}\n"),
    )
        .into_response()
}
fn pprof_response(profile: rpprof::protos::Profile, filename: &str) -> Response {
    match crate::profiling::gzip_profile(profile) {
        Ok(bytes) => {
            let mut response = (
                [
                    (header::CONTENT_TYPE, "application/octet-stream"),
                    (header::X_CONTENT_TYPE_OPTIONS, "nosniff"),
                    (
                        header::HeaderName::from_static("x-livekit-diagnostic-runtime"),
                        "rust",
                    ),
                ],
                bytes,
            )
                .into_response();
            if let Ok(value) = format!("attachment; filename=\"{filename}\"").parse() {
                response
                    .headers_mut()
                    .insert(header::CONTENT_DISPOSITION, value);
            }
            response
        }
        Err(error) => collector_error(error),
    }
}
async fn sampled(
    clock: rpprof::ClockType,
    duration: Duration,
) -> Result<rpprof::Report, crate::profiling::ProfileError> {
    let capture = Capture::start(clock)?;
    tokio::time::sleep(duration).await;
    capture.report()
}
async fn profile(PprofForm(query): PprofForm) -> Response {
    match sampled(rpprof::ClockType::Cpu, profile_seconds(&query)).await {
        Ok(report) => match rpprof::alloc::without_tracking(|| report.pprof()) {
            Ok(profile) => pprof_response(profile, "profile"),
            Err(error) => collector_error(error),
        },
        Err(error) => collector_error(format!("Could not enable CPU profiling: {error}")),
    }
}
async fn symbol(RawQuery(query): RawQuery) -> Response {
    let mut words = SymbolWords::default();
    words.push(query.as_deref().unwrap_or_default().as_bytes());
    words.response()
}

async fn symbol_post(mut body: Body) -> Response {
    let mut words = SymbolWords::default();
    while words.read_error.is_none() {
        match poll_fn(|cx| Pin::new(&mut body).poll_frame(cx)).await {
            Some(Ok(frame)) => {
                if let Ok(bytes) = frame.into_data() {
                    words.push(&bytes);
                    words.flush_symbols();
                }
            }
            Some(Err(_)) => words.read_error = Some("unexpected EOF"),
            None => break,
        }
    }
    words.response()
}

const SYMBOL_WORD_CAPACITY: usize = 4096;

#[derive(Default)]
struct SymbolWords {
    word: Vec<u8>,
    addresses: Vec<u64>,
    symbols: String,
    read_error: Option<&'static str>,
}

impl SymbolWords {
    // Match Go bufio.ReadSlice('+'): a delimiter in the last byte is
    // accepted, but a full buffer without one ends the request scan.
    fn push(&mut self, bytes: &[u8]) {
        for part in bytes.split_inclusive(|&byte| byte == b'+') {
            let delimited = part.last() == Some(&b'+');
            let available = SYMBOL_WORD_CAPACITY - self.word.len();
            if part.len() > available || (!delimited && part.len() == available) {
                self.word.extend_from_slice(&part[..available]);
                self.read_error = Some("bufio: buffer full");
                break;
            }
            self.word.extend_from_slice(part);
            if delimited {
                self.word.pop();
                self.finish_word();
            }
        }
    }

    fn finish_word(&mut self) {
        if let Some(address) = symbol_address(&self.word).filter(|&address| address != 0) {
            self.addresses.push(address);
        }
        self.word.clear();
    }

    fn response(mut self) -> Response {
        self.finish_word();
        self.flush_symbols();
        symbol_response(&self.symbols, self.read_error)
    }

    fn flush_symbols(&mut self) {
        use std::fmt::Write;
        if self.addresses.is_empty() {
            return;
        }
        for (address, name) in crate::profiling::symbol_names(&self.addresses) {
            let _ = writeln!(self.symbols, "{address:#x} {name}");
        }
        self.addresses.clear();
    }
}

fn symbol_address(word: &[u8]) -> Option<u64> {
    let (radix, digits, explicit_prefix) = match word {
        [b'0', b'x' | b'X', digits @ ..] => (16, digits, true),
        [b'0', b'o' | b'O', digits @ ..] => (8, digits, true),
        [b'0', b'b' | b'B', digits @ ..] => (2, digits, true),
        [b'0', ..] => (8, word, false),
        _ => (10, word, false),
    };
    let mut previous_digit = explicit_prefix;
    let mut value = 0u64;
    for &byte in digits {
        if byte == b'_' {
            if !previous_digit {
                return None;
            }
            previous_digit = false;
        } else {
            let digit = char::from(byte).to_digit(radix)?;
            value = value
                .saturating_mul(u64::from(radix))
                .saturating_add(u64::from(digit));
            previous_digit = true;
        }
    }
    (previous_digit && !digits.is_empty()).then_some(value)
}

fn symbol_response(symbols: &str, read_error: Option<&str>) -> Response {
    use std::fmt::Write;
    let mut body = symbol_body().to_owned();
    body.push_str(symbols);
    if let Some(error) = read_error {
        let _ = writeln!(body, "reading request: {error}");
    }
    (
        [
            (header::CONTENT_TYPE, "text/plain; charset=utf-8"),
            (header::X_CONTENT_TYPE_OPTIONS, "nosniff"),
            (
                header::HeaderName::from_static("x-livekit-diagnostic-runtime"),
                "rust",
            ),
        ],
        body,
    )
        .into_response()
}

fn symbol_body() -> &'static str {
    // pprof uses the count as a capability bit even before collecting a sample.
    "num_symbols: 1\n"
}
async fn trace(PprofForm(query): PprofForm) -> Response {
    match sampled(rpprof::ClockType::Wall, trace_seconds(&query)).await {
        Ok(report) => {
            let events=report.data.iter().map(|(frames,count)|{
            let timestamp=frames.sample_timestamp.duration_since(UNIX_EPOCH).unwrap_or_default().as_micros();
            let stack=frames.frames.iter().flat_map(|frame|frame.iter().map(|symbol|symbol.name())).collect::<Vec<_>>();
            serde_json::json!({"name":"rust stack sample","cat":"native-sampling","ph":"i","s":"t","pid":std::process::id(),"tid":frames.thread_id,"ts":timestamp,"args":{"thread":frames.thread_name,"stack":stack,"sample_count":count}})
        }).collect::<Vec<_>>();
            let body = serde_json::json!({"traceEvents":events,"displayTimeUnit":"ms","runtime":"rust","format":"chrome-trace-json","collection":"wall samples aggregated by native stack; one actual sample timestamp per cohort","missed_samples":report.missed_samples});
            (
                [
                    (header::CONTENT_TYPE, "application/json"),
                    (
                        header::HeaderName::from_static("x-livekit-diagnostic-runtime"),
                        "rust",
                    ),
                    (
                        header::HeaderName::from_static("x-livekit-diagnostic-format"),
                        "chrome-sampling-json",
                    ),
                ],
                body.to_string(),
            )
                .into_response()
        }
        Err(error) => collector_error(error),
    }
}
async fn goroutines() -> Response {
    match sampled(rpprof::ClockType::Wall, Duration::from_millis(100)).await {
        Ok(report) => {
            use std::fmt::Write;
            let mut body = format!(
                "Rust native thread stack samples (not Go goroutine state)\nsamples={} missed_samples={}\n",
                report.total_samples(),
                report.missed_samples
            );
            for (frames, count) in &report.data {
                let _ = writeln!(
                    body,
                    "\nrust thread {} {:?} samples={count}",
                    frames.thread_id, frames.thread_name
                );
                for frame in &frames.frames {
                    for symbol in frame {
                        let _ = writeln!(
                            body,
                            "  {} {}:{}",
                            symbol.name(),
                            symbol.filename(),
                            symbol.lineno()
                        );
                    }
                }
            }
            (
                [
                    (header::CONTENT_TYPE, "text/plain; charset=utf-8"),
                    (
                        header::HeaderName::from_static("x-livekit-diagnostic-runtime"),
                        "rust",
                    ),
                ],
                body,
            )
                .into_response()
        }
        Err(error) => collector_error(error),
    }
}
async fn named_profile(path: Result<Path<String>, PathRejection>, request: Request) -> Response {
    // Go treats a valid percent escape of an invalid UTF-8 byte as part of
    // the requested profile name. It is unknown, not a malformed URL.
    let Ok(Path(name)) = path else {
        return pprof_error(StatusCode::NOT_FOUND, "Unknown profile");
    };
    match name.as_str() {
        "heap" | "allocs" => {
            let Ok(PprofForm(query)) = PprofForm::from_request(request, &()).await;
            let duration = match query.get("seconds").filter(|value| !value.is_empty()) {
                Some(value) => match value.parse::<i64>().ok().filter(|value| *value > 0) {
                    Some(seconds) => Some(Duration::from_secs(seconds as u64)),
                    None => {
                        return pprof_error(
                            StatusCode::BAD_REQUEST,
                            "invalid value for \"seconds\" - must be a positive integer",
                        );
                    }
                },
                None => None,
            };
            let debug = query
                .get("debug")
                .and_then(|value| value.parse::<i64>().ok())
                .unwrap_or(0);
            if duration.is_some() && debug != 0 {
                return pprof_error(
                    StatusCode::BAD_REQUEST,
                    "seconds and debug params are incompatible",
                );
            }
            let before = if duration.is_some() {
                match HeapSampling::snapshot_async().await {
                    Ok(profile) => Some(profile),
                    Err(error) => return collector_error(error),
                }
            } else {
                None
            };
            if let Some(duration) = duration {
                tokio::time::sleep(duration).await;
            }
            match HeapSampling::snapshot_async().await {
                Ok(mut profile) => {
                    let render = tokio::task::spawn_blocking(move || {
                        rpprof::alloc::without_tracking(|| {
                            let filename = if let Some(before) = before {
                                profile = profile_delta(&before, profile);
                                format!("{name}-delta")
                            } else {
                                let kind = if name == "heap" {
                                    "inuse_space"
                                } else {
                                    "alloc_space"
                                };
                                profile.default_sample_type = profile
                                    .string_table
                                    .iter()
                                    .position(|value| value == kind)
                                    .unwrap_or(0)
                                    as i64;
                                name
                            };
                            if debug != 0 {
                                native_profile_text(&profile)
                            } else {
                                pprof_response(profile, &filename)
                            }
                        })
                    })
                    .await;
                    match render {
                        Ok(response) => response,
                        Err(_) => (
                            StatusCode::INTERNAL_SERVER_ERROR,
                            "heap renderer task failed",
                        )
                            .into_response(),
                    }
                }
                Err(error) => collector_error(error),
            }
        }
        "goroutine" => match sampled(rpprof::ClockType::Wall, Duration::from_millis(100)).await {
            Ok(report) => match rpprof::alloc::without_tracking(|| report.pprof()) {
                Ok(profile) => pprof_response(profile, "rust-thread-stacks"),
                Err(error) => collector_error(error),
            },
            Err(error) => collector_error(error),
        },
        _ => pprof_error(StatusCode::NOT_FOUND, "Unknown profile"),
    }
}
fn native_profile_text(profile: &rpprof::protos::Profile) -> Response {
    use std::fmt::Write;
    let mut body = String::from("Rust native allocation samples\n");
    let kinds = profile
        .sample_type
        .iter()
        .map(|kind| {
            format!(
                "{}({})",
                profile.string_table[kind.ty as usize], profile.string_table[kind.unit as usize]
            )
        })
        .collect::<Vec<_>>();
    let _ = writeln!(body, "{}", kinds.join(" "));
    for (sample, stack) in profile.sample.iter().zip(profile_stacks(profile)) {
        let _ = writeln!(body, "{:?} @ {:?}", sample.value, stack);
    }
    (
        [
            (header::CONTENT_TYPE, "text/plain; charset=utf-8"),
            (
                header::HeaderName::from_static("x-livekit-diagnostic-runtime"),
                "rust",
            ),
        ],
        body,
    )
        .into_response()
}
fn profile_stacks(profile: &rpprof::protos::Profile) -> Vec<Vec<String>> {
    // Location/function IDs are snapshot-local. Resolve actual symbols once
    // per location rather than scanning both tables for every sample/frame.
    let functions = profile
        .function
        .iter()
        .map(|function| (function.id, function))
        .collect::<std::collections::HashMap<_, _>>();
    let locations = profile
        .location
        .iter()
        .map(|location| {
            let names = location
                .line
                .iter()
                .filter_map(|line| {
                    functions.get(&line.function_id).map(|function| {
                        format!(
                            "{}:{}:{}",
                            profile.string_table[function.name as usize],
                            profile.string_table[function.filename as usize],
                            line.line
                        )
                    })
                })
                .collect::<Vec<_>>();
            (location.id, format!("{}:{names:?}", location.address))
        })
        .collect::<std::collections::HashMap<_, _>>();
    profile
        .sample
        .iter()
        .map(|sample| {
            sample
                .location_id
                .iter()
                .filter_map(|id| locations.get(id).cloned())
                .collect()
        })
        .collect()
}
#[cfg(test)]
fn profile_stack(
    profile: &rpprof::protos::Profile,
    sample: &rpprof::protos::Sample,
) -> Vec<String> {
    let mut view = profile.clone();
    view.sample = vec![sample.clone()];
    profile_stacks(&view).pop().unwrap_or_default()
}
fn profile_delta(
    before: &rpprof::protos::Profile,
    mut after: rpprof::protos::Profile,
) -> rpprof::protos::Profile {
    fn totals(
        profile: &rpprof::protos::Profile,
        stacks: &[Vec<String>],
    ) -> std::collections::BTreeMap<Vec<String>, Vec<i64>> {
        let mut totals = std::collections::BTreeMap::new();
        for (sample, stack) in profile.sample.iter().zip(stacks) {
            let row = totals
                .entry(stack.clone())
                .or_insert_with(|| vec![0i64; sample.value.len()]);
            for (sum, value) in row.iter_mut().zip(&sample.value) {
                *sum = sum.saturating_add(*value);
            }
        }
        totals
    }
    let previous_keys = profile_stacks(before);
    let keys = profile_stacks(&after);
    let previous = totals(before, &previous_keys);
    let mut differences = totals(&after, &keys);
    for (key, values) in &mut differences {
        if let Some(previous) = previous.get(key) {
            for (value, previous) in values.iter_mut().zip(previous) {
                *value = value.saturating_sub(*previous);
            }
        }
    }
    for (sample, key) in after.sample.iter_mut().zip(keys) {
        sample.value = differences
            .remove(&key)
            .unwrap_or_else(|| vec![0; sample.value.len()]);
    }
    after
        .sample
        .retain(|sample| sample.value.iter().any(|value| *value != 0));
    after.duration_nanos = after.time_nanos.saturating_sub(before.time_nanos).max(0);
    // The Go internal profile writer omits tag 14 for merged deltas.
    after.default_sample_type = 0;
    after
}

async fn rooms(State(state): State<DebugState>) -> Response {
    match rooms_json(&state.manager).await {
        // Go writes the marshaled bytes without setting Content-Type.
        Ok(body) => (StatusCode::OK, body).into_response(),
        // Go returns 400 with the marshal error text on failure.
        Err(error) => (StatusCode::BAD_REQUEST, error.to_string()).into_response(),
    }
}

/// Go room.DebugInfo's identity map, populated from the room owner's live state.
/// Identity/state, pending publication queues and published tracks have actual
/// generation-bound producers. Migration origin, DownTracks and codec receiver
/// diagnostics remain partial until their media consumers supply real state.
pub(crate) async fn rooms_json(manager: &RoomManager) -> serde_json::Result<String> {
    let rooms: Vec<_> = manager
        .debug_room_details()
        .await
        .into_iter()
        .map(room_debug_json)
        .collect();
    serde_json::to_string(&rooms)
}

fn room_debug_json(snapshot: rtc_core::room::DebugRoomSnapshot) -> serde_json::Value {
    let info = snapshot.room;
    let participants = snapshot.participants;
    let media = snapshot.media;
    use rtc_core::model::{ParticipantState, TrackKind};
    let participants: serde_json::Map<String, serde_json::Value> = participants.into_iter().map(|snapshot| {
        let pending= snapshot.pending_tracks.into_iter().map(|(cid,pending)|{
            let track_infos=pending.track_infos.into_iter().map(|track|{
                let proto=lk_compat::convert::track_info_to_proto(&track);
                let wire=lk_compat::sip::wire::PreservedMessage::from_message(&proto);
                format!("{}",wire.as_dynamic())
            }).collect::<Vec<_>>();
            let mut row=serde_json::json!({"TrackInfos":track_infos});
            if let Some(migrated)=pending.migrated{row["Migrated"]=serde_json::json!(migrated);}
            (cid,row)
        }).collect::<serde_json::Map<String,serde_json::Value>>();
        let p=snapshot.info;
        let state = match p.state {
            ParticipantState::Joining => "JOINING",
            ParticipantState::Joined => "JOINED",
            ParticipantState::Active => "ACTIVE",
            ParticipantState::Disconnected => "DISCONNECTED",
        };
        let mut tracks: serde_json::Map<String, serde_json::Value> = p.tracks.into_iter().map(|t| {
            let kind = match t.kind {
                TrackKind::Audio => "AUDIO".to_string(),
                TrackKind::Video => "VIDEO".to_string(),
                TrackKind::Data => "DATA".to_string(),
                unknown => unknown.as_i32().to_string(),
            };
            (t.sid.to_string(), serde_json::json!({"ID": t.sid.to_string(), "Kind": kind, "PubMuted": t.muted}))
        }).collect();
        if let Some(media) = &media { media::enrich_tracks(media, &p.sid, &mut tracks); }
        (p.identity.to_string(), serde_json::json!({
            "ID": p.sid.to_string(), "State": state,
            "PendingTracks":pending,
            "UpTrackManager": {"PublishedTracks": tracks}
        }))
    }).collect();
    serde_json::json!({
        "Name": info.name.to_string(), "Sid": info.sid.to_string(),
        "CreatedAt": info.creation_time_ms / 1000, "Participants": participants
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use rtc_core::events::NullSink;
    use rtc_core::ids::{ProjectId, RoomKey};
    use rtc_runtime::net::{LocalInterface, NetConfig, NetRouter};
    use rtc_runtime::{CreateRoomOptions, RuntimeDeps};
    use std::net::Ipv4Addr;
    use std::sync::Arc;

    struct RejectData;
    impl rtc_core::data::DataHandler for RejectData {
        fn inbound(
            &self,
            _sender: &rtc_core::data::DataSender<'_>,
            _kind: rtc_core::media_api::DataKind,
            _label: &str,
            _raw: &[u8],
        ) -> rtc_core::data::DataVerdict {
            rtc_core::data::DataVerdict::Drop
        }
    }
    fn test_config() -> rtc_core::config::ServerConfig {
        // Real normalization path (same as lk-compat test_support): argv only.
        let info = lk_config::ProgramInfo {
            binary_name: "livecloud".into(),
            product_version: "test".into(),
            compat_version: "1.13.10",
        };
        let args = ["livecloud", "--dev", "--node-ip", "127.0.0.1"]
            .into_iter()
            .map(Into::into)
            .collect();
        match lk_config::parse(args, &lk_config::Env::default(), &info).unwrap() {
            lk_config::EntryAction::RunServer(loaded) => loaded.server,
            action => panic!("expected server config, got {action:?}"),
        }
    }

    async fn manager() -> (RoomManager, rtc_runtime::net::NetRouter) {
        let local = Ipv4Addr::LOCALHOST.into();
        let net = NetRouter::start(NetConfig {
            udp_enabled: false,
            udp_port: None,
            port_range: (0, 0),
            tcp_port: None,
            interfaces: vec![LocalInterface {
                bind: local,
                advertise: local,
            }],
        })
        .await
        .unwrap();
        let manager = RoomManager::new(RuntimeDeps {
            config: Arc::new(test_config()),
            net: net.clone(),
            events: Arc::new(NullSink),
            data: Arc::new(RejectData),
        });
        (manager, net)
    }

    fn debug_join(key: RoomKey, identity: &str) -> rtc_core::session::JoinParams {
        use rtc_core::permission::{Grants, VideoGrant};
        use rtc_core::session::{ClientCapabilities, ClientMeta, JoinParams};
        JoinParams {
            grants: Grants {
                identity: identity.into(),
                video: VideoGrant {
                    room_join: true,
                    room: key.name.to_string(),
                    ..Default::default()
                },
                ..Default::default()
            },
            room: key,
            identity: identity.into(),
            name: String::new(),
            metadata: String::new(),
            attributes: Default::default(),
            client: ClientMeta::default(),
            capabilities: ClientCapabilities::default(),
            auto_subscribe: true,
            auto_subscribe_data_track: false,
            adaptive_stream: false,
            single_pc: false,
            disable_ice_lite: false,
            subscriber_allow_pause: None,
            reconnect: false,
            reconnect_reason: 0,
            participant_sid: None,
            add_tracks: vec![],
            publisher_offer: None,
        }
    }

    #[tokio::test]
    async fn rooms_debug_uses_live_identity_state_and_drops_old_generation() {
        let (manager, net) = manager().await;
        let key = RoomKey::new(ProjectId::local(), "debug-live");
        let alice = manager
            .join(debug_join(key.clone(), "alice"))
            .await
            .unwrap();
        let bob = manager.join(debug_join(key.clone(), "bob")).await.unwrap();
        let body: serde_json::Value =
            serde_json::from_str(&rooms_json(&manager).await.unwrap()).unwrap();
        let room = &body[0];
        let old_sid = room["Sid"].as_str().unwrap().to_string();
        assert_eq!(room["Participants"].as_object().unwrap().len(), 2);
        assert_eq!(
            room["Participants"]["alice"]["ID"],
            alice.participant_sid.to_string()
        );
        assert_eq!(
            room["Participants"]["bob"]["ID"],
            bob.participant_sid.to_string()
        );
        let actual = manager.list_participants(&key).await.unwrap();
        let alice_state = actual
            .iter()
            .find(|p| p.identity.as_str() == "alice")
            .unwrap()
            .state;
        let state = match alice_state {
            rtc_core::model::ParticipantState::Joining => "JOINING",
            rtc_core::model::ParticipantState::Joined => "JOINED",
            rtc_core::model::ParticipantState::Active => "ACTIVE",
            rtc_core::model::ParticipantState::Disconnected => "DISCONNECTED",
        };
        assert_eq!(room["Participants"]["alice"]["State"], state);
        assert_eq!(
            room["Participants"]["alice"]["UpTrackManager"]["PublishedTracks"],
            serde_json::json!({})
        );
        assert_eq!(
            room["Participants"]["alice"]["PendingTracks"],
            serde_json::json!({}),
            "an actual owner snapshot proves there are no pending tracks"
        );
        assert!(room["Participants"].get("NumParticipants").is_none());
        manager
            .remove_participant(&key, &"alice".into())
            .await
            .unwrap();
        let body: serde_json::Value =
            serde_json::from_str(&rooms_json(&manager).await.unwrap()).unwrap();
        assert!(body[0]["Participants"].get("alice").is_none());
        assert_eq!(body[0]["Participants"].as_object().unwrap().len(), 1);
        manager.delete_room(&key).await.unwrap();
        assert_eq!(rooms_json(&manager).await.unwrap(), "[]");
        manager
            .create_room(key.clone(), CreateRoomOptions::default())
            .await
            .unwrap();
        let body: serde_json::Value =
            serde_json::from_str(&rooms_json(&manager).await.unwrap()).unwrap();
        assert_ne!(body[0]["Sid"], old_sid);
        assert_eq!(body[0]["Participants"], serde_json::json!({}));
        drop((alice, bob));
        manager.close_all().await;
        net.shutdown();
    }

    #[tokio::test]
    async fn rooms_json_lists_actual_created_rooms() {
        let (manager, net) = manager().await;
        assert_eq!(rooms_json(&manager).await.unwrap(), "[]");

        let key = RoomKey::new(ProjectId::local(), "debug-room");
        manager
            .create_room(key.clone(), CreateRoomOptions::default())
            .await
            .unwrap();
        manager
            .create_room(
                RoomKey::new(ProjectId::local(), "second"),
                CreateRoomOptions::default(),
            )
            .await
            .unwrap();

        let value: serde_json::Value =
            serde_json::from_str(&rooms_json(&manager).await.unwrap()).unwrap();
        let rooms = value.as_array().unwrap();
        assert_eq!(rooms.len(), 2, "{rooms:?}");
        // HashMap iteration order is not contractual; match rooms by name.
        let find = |name: &str| {
            rooms
                .iter()
                .find(|r| r["Name"] == name)
                .unwrap_or_else(|| panic!("room {name} missing in {rooms:?}"))
                .clone()
        };
        let first = find("debug-room");
        assert!(first["Sid"].as_str().unwrap().starts_with("RM_"), "{first}");
        assert!(first["CreatedAt"].as_i64().unwrap() > 0);
        assert_eq!(first["Participants"], serde_json::json!({}));
        assert!(find("second")["Sid"].as_str().is_some());

        // Deleting a room removes it from the next render (live data, not a
        // static body).
        manager.delete_room(&key).await.unwrap();
        let value: serde_json::Value =
            serde_json::from_str(&rooms_json(&manager).await.unwrap()).unwrap();
        let rooms = value.as_array().unwrap();
        assert_eq!(rooms.len(), 1, "{rooms:?}");
        assert_eq!(rooms[0]["Name"], "second");

        net.shutdown_and_wait().await;
    }

    #[test]
    fn sync_handlers_produce_real_bodies() {
        // cmdline mirrors the actual process arguments (Go NUL-joined argv).
        let args: Vec<String> = std::env::args().collect();
        let body = cmdline_body(&args);
        assert_eq!(body, args.join("\0"));
        assert!(!body.is_empty());
        assert!(symbol_body().starts_with("num_symbols: "));
        assert_eq!(profile_seconds(&HashMap::new()), Duration::from_secs(30));
        assert_eq!(trace_seconds(&HashMap::new()), Duration::from_secs(1));
    }

    #[test]
    fn symbol_words_preserve_base_zero_and_the_full_buffer_prefix() {
        let mut words = SymbolWords::default();
        words.push(b"0x_2a+0b101010+0");
        words.push(b"o52+052+42_0+ 42+0x2a_+\xff+");
        assert_eq!(words.addresses, [42, 42, 42, 42, 420]);
        words.push(&[b'0'; SYMBOL_WORD_CAPACITY - 2]);
        words.push(b"52+77");
        assert_eq!(words.read_error, Some("bufio: buffer full"));
        words.finish_word();
        assert_eq!(words.addresses, [42, 42, 42, 42, 420, 42]);
    }
}

#[cfg(test)]
mod collector_tests {
    use super::*;
    use axum::body::to_bytes;
    use rpprof::protos::Message as _;
    use std::io::Read;
    fn decode(bytes: &[u8]) -> rpprof::protos::Profile {
        let mut body = Vec::new();
        flate2::read::GzDecoder::new(bytes)
            .read_to_end(&mut body)
            .unwrap();
        rpprof::protos::Profile::decode(body.as_slice()).unwrap()
    }
    #[inline(never)]
    fn real_cpu_work(stop: std::sync::mpsc::Receiver<()>) {
        let mut value = 1u64;
        while stop.try_recv() == Err(std::sync::mpsc::TryRecvError::Empty) {
            for n in 0..10000 {
                value =
                    std::hint::black_box(value.wrapping_mul(6364136223846793005).wrapping_add(n));
            }
        }
        std::hint::black_box(value);
    }
    #[tokio::test]
    async fn cpu_http_returns_real_sampled_gzip_pprof_and_native_symbol_lookup() {
        let _serial = crate::profiling::TEST_SAMPLING
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        // Keep the workload alive throughout sampler initialization and capture,
        // including when other tests are running in the same process.
        let (stop, until_stopped) = std::sync::mpsc::channel();
        let worker = std::thread::Builder::new()
            .name("http-pprof-workload".into())
            .spawn(|| real_cpu_work(until_stopped))
            .unwrap();
        let response = profile(PprofForm(
            [("seconds".into(), "1".into())].into_iter().collect(),
        ))
        .await;
        drop(stop);
        worker.join().unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            response.headers()["content-type"],
            "application/octet-stream"
        );
        let profile = decode(&to_bytes(response.into_body(), 16 << 20).await.unwrap());
        assert!(
            profile
                .sample
                .iter()
                .any(|sample| !sample.location_id.is_empty()
                    && sample.value.iter().any(|value| *value > 0)),
            "HTTP body must contain actual CPU stack samples: {} samples, {} without stack, {} with a positive value, {} locations",
            profile.sample.len(),
            profile
                .sample
                .iter()
                .filter(|s| s.location_id.is_empty())
                .count(),
            profile
                .sample
                .iter()
                .filter(|s| s.value.iter().any(|v| *v > 0))
                .count(),
            profile.location.len(),
        );
        assert!(
            profile
                .sample_type
                .iter()
                .any(|kind| profile.string_table[kind.ty as usize] == "cpu")
        );
        assert!(
            profile
                .comment
                .iter()
                .any(|index| profile.string_table[*index as usize].contains("runtime=rust"))
        );
        let address = profile
            .location
            .iter()
            .find(|location| location.address != 0)
            .expect("sampled native address")
            .address;
        let response = symbol(RawQuery(Some(format!("{address:#x}")))).await;
        let body = String::from_utf8(
            to_bytes(response.into_body(), 1 << 20)
                .await
                .unwrap()
                .to_vec(),
        )
        .unwrap();
        assert!(
            body.contains(&format!("{address:#x} ")),
            "query must resolve a real sampled function"
        );
    }
    #[tokio::test]
    async fn aborting_http_capture_releases_sampler_for_next_request() {
        let _serial = crate::profiling::TEST_SAMPLING
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        let task =
            tokio::spawn(async { sampled(rpprof::ClockType::Cpu, Duration::from_secs(30)).await });
        tokio::time::timeout(Duration::from_secs(1), async {
            while !crate::profiling::capture_active() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert!(Capture::start(rpprof::ClockType::Cpu).is_err());
        task.abort();
        assert!(task.await.unwrap_err().is_cancelled());
        let capture = Capture::start(rpprof::ClockType::Cpu)
            .expect("abandoned request must not retain global CPU collector");
        drop(capture);
    }
    #[tokio::test]
    async fn heap_endpoint_preserves_shared_live_sampling_and_exports_real_allocations() {
        let _serial = crate::profiling::TEST_SAMPLING
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        let first = HeapSampling::start();
        let second = HeapSampling::start();
        let allocations = (0..128)
            .map(|n| vec![n as u8; 128 * 1024])
            .collect::<Vec<_>>();
        let response = named_profile(Ok(Path("heap".into())), Request::new(Body::empty())).await;
        assert_eq!(response.status(), StatusCode::OK);
        let profile = decode(&to_bytes(response.into_body(), 16 << 20).await.unwrap());
        let inuse = profile
            .sample_type
            .iter()
            .position(|kind| profile.string_table[kind.ty as usize] == "inuse_space")
            .unwrap();
        assert!(
            profile.sample.iter().any(|sample| sample.value[inuse] > 0),
            "retained real allocations must populate heap samples"
        );
        drop(first);
        assert!(
            rpprof::alloc::is_active(),
            "another heap owner still samples"
        );
        drop(allocations);
        drop(second);
        assert!(!rpprof::alloc::is_active());
    }
    #[tokio::test]
    async fn native_trace_is_explicit_json_with_actual_thread_sample_timestamps() {
        let _serial = crate::profiling::TEST_SAMPLING
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        let response = trace(PprofForm(
            [("seconds".into(), "0.1".into())].into_iter().collect(),
        ))
        .await;
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            response.headers()["x-livekit-diagnostic-format"],
            "chrome-sampling-json"
        );
        let value: serde_json::Value =
            serde_json::from_slice(&to_bytes(response.into_body(), 16 << 20).await.unwrap())
                .unwrap();
        assert_eq!(value["runtime"], "rust");
        assert_eq!(value["format"], "chrome-trace-json");
        let events = value["traceEvents"].as_array().unwrap();
        assert!(
            !events.is_empty(),
            "wall profiler must collect actual stacks"
        );
        assert!(events.iter().all(|event| {
            event["tid"].as_u64().is_some_and(|id| id > 0)
                && event["ts"].as_u64().is_some_and(|time| time > 0)
                && event["args"]["sample_count"]
                    .as_i64()
                    .is_some_and(|count| count > 0)
        }));
    }
    #[test]
    fn profile_delta_matches_actual_native_stack_not_unstable_profile_ids() {
        let _serial = crate::profiling::TEST_SAMPLING
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        let _sampling = HeapSampling::start();
        let before = HeapSampling::snapshot().unwrap();
        let allocations = (0..32)
            .map(|n| vec![n as u8; 256 * 1024])
            .collect::<Vec<_>>();
        let after = HeapSampling::snapshot().unwrap();
        let captured_at = after.time_nanos;
        let delta = profile_delta(&before, after);
        assert_eq!(delta.time_nanos, captured_at);
        assert_eq!(delta.duration_nanos, captured_at - before.time_nanos);
        let alloc = delta
            .sample_type
            .iter()
            .position(|kind| delta.string_table[kind.ty as usize] == "alloc_space")
            .unwrap();
        assert!(delta.sample.iter().any(|sample| sample.value[alloc] > 0));
        std::hint::black_box(&allocations);
    }
}

#[cfg(test)]
mod profile_query_tests {
    use super::*;
    use axum::body::to_bytes;
    #[tokio::test]
    async fn allocation_delta_rejects_invalid_seconds_and_text_mix_before_collecting() {
        for seconds in ["0", "-1", "garbage", "0.5"] {
            let response = named_profile(
                Ok(Path("heap".into())),
                Request::builder()
                    .uri(format!("/debug/pprof/heap?seconds={seconds}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await;
            assert_eq!(response.status(), StatusCode::BAD_REQUEST);
            let body = to_bytes(response.into_body(), 4096).await.unwrap();
            assert_eq!(
                body.as_ref(),
                b"invalid value for \"seconds\" - must be a positive integer\n"
            );
        }
        let response = named_profile(
            Ok(Path("allocs".into())),
            Request::builder()
                .uri("/debug/pprof/allocs?seconds=1&debug=1")
                .body(Body::empty())
                .unwrap(),
        )
        .await;
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        assert_eq!(
            to_bytes(response.into_body(), 4096).await.unwrap().as_ref(),
            b"seconds and debug params are incompatible\n"
        );
    }
}
