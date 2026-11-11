//! SPEC-0074 C16: `trace.jaeger_url` OTLP/Jaeger trace export.
//!
//! Port of `github.com/livekit/protocol/tracer/jaeger` `Configure` (called from
//! upstream `cmd/server/main.go:253` when `conf.Trace.JaegerURL != ""`). It
//! normalizes the endpoint, installs a global batched OTLP/HTTP
//! [`opentelemetry::global::TracerProvider`] whose resource carries
//! `service.name`, and produces **no startup output** — the server then runs
//! with the identical default startup line as the Go binary (verified by the
//! real-binary config differential).
//!
//! Faithfulness notes vs the Go source:
//! - URL normalization mirrors `strings.HasPrefix`/`url.Parse`/
//!   `net.SplitHostPort`/`net.JoinHostPort` exactly (default port 4318, path
//!   `/v1/traces`); see [`normalize_endpoint`] and its unit tests.
//! - `sdkresource.Merge(Default(), New(WithFromEnv, WithHost, WithContainer,
//!   attrs))` maps onto `Resource::builder()`, which already runs
//!   `SdkProvidedResourceDetector` + `TelemetryResourceDetector` +
//!   `EnvResourceDetector` (the `WithFromEnv` equivalent); `service.name` is
//!   appended last so it wins, as in Go. `host.name` is filled from
//!   `gethostname`; the Go `WithContainer` detector (cgroup parsing) has no
//!   vendored equivalent and is omitted. This remains a collector-observable
//!   resource-attribute gap, not a reason to waive that surface.
//! - The default batch span processor owns a std thread; its exporter uses
//!   a blocking HTTP client with the same pure-Rust TLS provider as the
//!   other outbound transports. HTTPS verifies the collector's identity.
//! - Export failures are reported by the owned exporter wrapper rather than
//!   silently relying on a feature-disabled SDK internal logger.

use opentelemetry::KeyValue;
use opentelemetry_otlp::{SpanExporter, WithExportConfig, WithHttpConfig};
use opentelemetry_sdk::{
    error::OTelSdkResult,
    resource::Resource,
    trace::{SdkTracerProvider, SpanData, SpanExporter as SdkSpanExporter},
};
use std::{env, fs, time::Duration};

/// Error while installing the tracer provider (endpoint build failure).
#[derive(Debug, thiserror::Error)]
#[error("could not configure Jaeger tracing: {0}")]
pub(crate) struct JaegerError(String);

/// Reproduce `jaeger.Configure`'s endpoint normalization:
/// prepend `http://` when the address has no scheme, default the port to 4318
/// when the authority carries none, and default the path to `/v1/traces` when
/// it is empty. Query/fragment suffixes and an explicit path are preserved.
pub(crate) fn normalize_endpoint(addr: &str) -> String {
    // Go: if !strings.HasPrefix(addr, "http") { addr = "http://" + addr }
    let with_scheme = if addr.starts_with("http") {
        addr.to_string()
    } else {
        format!("http://{addr}")
    };
    let sep = match with_scheme.find("://") {
        Some(i) => i,
        // Only reachable when the raw value already starts with "http" but
        // carries no scheme separator; keep it deterministic (never a
        // malformed slice) rather than panicking.
        None => return format!("http://{with_scheme}:4318/v1/traces"),
    };
    let scheme = &with_scheme[..sep];
    let rest = &with_scheme[sep + "://".len()..];
    // Authority ends at the first path/query/fragment delimiter.
    let auth_end = rest.find(['/', '?', '#']).unwrap_or(rest.len());
    let authority = &rest[..auth_end];
    let tail = &rest[auth_end..];
    // net.SplitHostPort/JoinHostPort: append :4318 only when no port present.
    let authority = if authority_has_port(authority) {
        authority.to_string()
    } else {
        format!("{authority}:4318")
    };
    // u.Path is the segment before any `?`/`#`; empty means default it.
    let (path, suffix) = match tail.find(['?', '#']) {
        Some(i) => (&tail[..i], &tail[i..]),
        None => (tail, ""),
    };
    let path = if path.is_empty() { "/v1/traces" } else { path };
    format!("{scheme}://{authority}{path}{suffix}")
}

/// True when the URL authority already carries a port. Mirrors the bracket
/// handling of `net.SplitHostPort` for IPv6 literals.
fn authority_has_port(authority: &str) -> bool {
    if let Some(inner) = authority.strip_prefix('[') {
        // `[::1]` or `[::1]:4318`: a port only exists after the closing bracket.
        inner
            .rfind(']')
            .is_some_and(|b| inner[b + 1..].starts_with(':'))
    } else {
        authority.contains(':')
    }
}

fn host_name() -> Option<String> {
    let mut buf = [0 as libc::c_char; 256];
    // SAFETY: gethostname writes at most buf.len() bytes into a live buffer.
    let rc = unsafe { libc::gethostname(buf.as_mut_ptr(), buf.len()) };
    if rc != 0 {
        return None;
    }
    let bytes = buf
        .as_slice()
        .iter()
        .take_while(|&&c| c != 0)
        .map(|&c| c as u8)
        .collect::<Vec<u8>>();
    String::from_utf8(bytes).ok().filter(|s| !s.is_empty())
}

/// Install the global OTLP/Jaeger tracer provider for `addr`, tagging the
/// resource `service.name = name`. Mirrors `jaeger.Configure(ctx, addr, name)`.
pub(crate) fn configure(addr: &str, name: &str) -> Result<JaegerGuard, JaegerError> {
    let provider = build_provider(addr, name)?;
    opentelemetry::global::set_tracer_provider(provider.clone());
    Ok(JaegerGuard(Some(provider)))
}

fn tls_config() -> Result<rtc_tls::rustls::ClientConfig, JaegerError> {
    let read = |key| {
        let path = env::var_os(key).filter(|value| !value.is_empty())?;
        match fs::read(path) {
            Ok(bytes) => Some(bytes),
            Err(error) => {
                tracing::warn!(%error, env = key, "could not read OTLP TLS material");
                None
            }
        }
    };
    // Fixed Go processes generic, then trace-specific values, leaving a
    // previous valid setting intact if a replacement file is unusable.
    let mut roots = None;
    let mut config = None;
    for key in [
        "OTEL_EXPORTER_OTLP_CERTIFICATE",
        "OTEL_EXPORTER_OTLP_TRACES_CERTIFICATE",
    ] {
        if let Some(bytes) = read(key) {
            match rtc_tls::verified_client_config(Some(&bytes), None, None) {
                Ok(valid) => {
                    roots = Some(bytes);
                    config = Some(valid);
                }
                Err(error) => tracing::warn!(%error, env = key, "invalid OTLP CA bundle"),
            }
        }
    }
    for (cert, key) in [
        (
            "OTEL_EXPORTER_OTLP_CLIENT_CERTIFICATE",
            "OTEL_EXPORTER_OTLP_CLIENT_KEY",
        ),
        (
            "OTEL_EXPORTER_OTLP_TRACES_CLIENT_CERTIFICATE",
            "OTEL_EXPORTER_OTLP_TRACES_CLIENT_KEY",
        ),
    ] {
        if env::var_os(cert).is_some_and(|value| !value.is_empty())
            && env::var_os(key).is_some_and(|value| !value.is_empty())
            && let (Some(cert), Some(key)) = (read(cert), read(key))
        {
            match rtc_tls::verified_client_config(roots.as_deref(), Some(&cert), Some(&key)) {
                Ok(valid) => config = Some(valid),
                Err(error) => tracing::warn!(%error, "invalid OTLP client certificate pair"),
            }
        }
    }
    if let Some(config) = config {
        return Ok(config);
    }
    match rtc_tls::verified_client_config(None, None, None) {
        Ok(config) => Ok(config),
        // Loading the trust store is not an OTLP startup gate in Go. An
        // empty trust store still verifies and rejects any untrusted peer.
        Err(error) => {
            tracing::warn!(%error, "no OTLP system trust roots");
            rtc_tls::client_config_with_roots(rtc_tls::rustls::RootCertStore::empty())
                .map_err(|error| JaegerError(error.to_string()))
        }
    }
}

fn otlp_http_client() -> Result<reqwest::blocking::Client, JaegerError> {
    let mut tls = tls_config()?;
    tls.alpn_protocols = vec![b"h2".to_vec(), b"http/1.1".to_vec()];
    let timeout = [
        "OTEL_EXPORTER_OTLP_TRACES_TIMEOUT",
        "OTEL_EXPORTER_OTLP_TIMEOUT",
    ]
    .into_iter()
    .find_map(|key| env::var(key).ok()?.parse().ok())
    .map(Duration::from_millis)
    .unwrap_or(Duration::from_secs(10));
    // The blocking client starts and owns its own runtime. Building it on
    // a std thread also permits this provider to be configured from Tokio.
    std::thread::spawn(move || {
        reqwest::blocking::Client::builder()
            .timeout(timeout)
            .tls_backend_preconfigured(tls)
            .build()
    })
    .join()
    .map_err(|_| JaegerError("OTLP HTTP client initialization panicked".into()))?
    .map_err(|error| JaegerError(error.to_string()))
}

fn build_provider(addr: &str, name: &str) -> Result<SdkTracerProvider, JaegerError> {
    let endpoint = normalize_endpoint(addr);
    let exporter = SpanExporter::builder()
        .with_http()
        .with_endpoint(endpoint)
        .with_http_client(otlp_http_client()?)
        .build()
        .map_err(|e| JaegerError(e.to_string()))?;

    let mut resource =
        Resource::builder().with_attribute(KeyValue::new("service.name", name.to_string()));
    if let Some(host) = host_name() {
        resource = resource.with_attribute(KeyValue::new("host.name", host));
    }

    let provider = SdkTracerProvider::builder()
        .with_batch_exporter(ReportingExporter(exporter))
        .with_resource(resource.build())
        .build();

    Ok(provider)
}

/// Mirror the fixed Go error handler for actual export failures. The SDK's
/// internal-logs feature is not a replacement for this application consumer.
#[derive(Debug)]
struct ReportingExporter(SpanExporter);
impl SdkSpanExporter for ReportingExporter {
    async fn export(&self, batch: Vec<SpanData>) -> OTelSdkResult {
        let result = self.0.export(batch).await;
        if let Err(error) = &result {
            tracing::warn!(%error, "jaeger error");
        }
        result
    }
    fn shutdown_with_timeout(&self, timeout: std::time::Duration) -> OTelSdkResult {
        self.0.shutdown_with_timeout(timeout)
    }
    fn force_flush(&self) -> OTelSdkResult {
        self.0.force_flush()
    }
    fn set_resource(&mut self, resource: &Resource) {
        self.0.set_resource(resource);
    }
}

/// Own the processor through all server startup/shutdown paths. Global provider
/// installation retains a clone, so dropping a plain local variable is not a
/// shutdown/flush operation.
pub(crate) struct JaegerGuard(Option<SdkTracerProvider>);
impl JaegerGuard {
    pub(crate) async fn shutdown(&mut self) -> Result<(), JaegerError> {
        let Some(provider) = self.0.take() else {
            return Ok(());
        };
        tokio::task::spawn_blocking(move || {
            provider.shutdown_with_timeout(std::time::Duration::from_secs(5))
        })
        .await
        .map_err(|error| JaegerError(error.to_string()))?
        .map_err(|error| JaegerError(error.to_string()))
    }
}
impl Drop for JaegerGuard {
    fn drop(&mut self) {
        // Early startup errors still release the owned exporter thread. The
        // blocking exporter does not depend on polling this Tokio worker.
        if let Some(provider) = self.0.take() {
            if let Err(error) = provider.shutdown_with_timeout(std::time::Duration::from_secs(5)) {
                tracing::warn!(%error, "jaeger shutdown failed");
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bare_host_gets_scheme_port_and_trace_path() {
        // Upstream feeds an arbitrary token in the config differential.
        assert_eq!(
            normalize_endpoint("x-trace.jaeger_url"),
            "http://x-trace.jaeger_url:4318/v1/traces"
        );
    }

    #[test]
    fn default_port_when_authority_has_none() {
        assert_eq!(
            normalize_endpoint("http://collector:4318"),
            "http://collector:4318/v1/traces"
        );
        assert_eq!(
            normalize_endpoint("collector.internal"),
            "http://collector.internal:4318/v1/traces"
        );
    }

    #[test]
    fn explicit_port_and_path_are_preserved() {
        assert_eq!(
            normalize_endpoint("http://my-collector:4317/traces?k=v"),
            "http://my-collector:4317/traces?k=v"
        );
    }

    #[test]
    fn https_authority_keeps_scheme() {
        assert_eq!(
            normalize_endpoint("https://otel.example.com"),
            "https://otel.example.com:4318/v1/traces"
        );
    }

    #[test]
    fn ipv6_literal_authority_takes_default_port() {
        assert_eq!(
            normalize_endpoint("http://[::1]"),
            "http://[::1]:4318/v1/traces"
        );
        assert_eq!(
            normalize_endpoint("http://[::1]:4318/custom"),
            "http://[::1]:4318/custom"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn actual_otlp_batch_shutdown_exports_last_span_before_default_batch_interval() {
        use opentelemetry::trace::{Span, Tracer, TracerProvider};
        use std::io::{Read, Write};
        use std::net::TcpListener;
        use std::time::{Duration, Instant};
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        listener.set_nonblocking(true).unwrap();
        let collector = std::thread::spawn(move || {
            let deadline = Instant::now() + Duration::from_secs(8);
            let (mut socket, _) = loop {
                match listener.accept() {
                    Ok(socket) => break socket,
                    Err(error)
                        if error.kind() == std::io::ErrorKind::WouldBlock
                            && Instant::now() < deadline =>
                    {
                        std::thread::sleep(Duration::from_millis(5));
                    }
                    Err(error) => panic!("owned OTLP collector did not receive flush: {error}"),
                }
            };
            // On macOS an accepted socket can inherit O_NONBLOCK from the
            // listener. The collector reads with the actual bounded timeout.
            socket.set_nonblocking(false).unwrap();
            socket
                .set_read_timeout(Some(Duration::from_secs(5)))
                .unwrap();
            let mut request = Vec::new();
            let header_end = loop {
                let mut byte = [0];
                socket.read_exact(&mut byte).unwrap();
                request.push(byte[0]);
                assert!(request.len() < 65536);
                if request.ends_with(b"\r\n\r\n") {
                    break request.len();
                }
            };
            let headers = String::from_utf8_lossy(&request).to_ascii_lowercase();
            assert!(headers.starts_with("post /v1/traces http/1.1\r\n"));
            let length: usize = headers
                .lines()
                .find_map(|line| line.strip_prefix("content-length:").map(str::trim))
                .unwrap()
                .parse()
                .unwrap();
            assert!(length > 0 && length < 1 << 20);
            request.resize(header_end + length, 0);
            socket.read_exact(&mut request[header_end..]).unwrap();
            socket
                .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\nConnection: close\r\n\r\n")
                .unwrap();
            request[header_end..].to_vec()
        });
        // No global provider mutation: exercise the same real exporter builder.
        let provider = build_provider(&format!("http://{addr}"), "actual-flush-test").unwrap();
        let tracer = provider.tracer("actual-owned-shutdown");
        let mut span = tracer.start("actual-span-before-immediate-shutdown");
        span.end();
        let mut owned = JaegerGuard(Some(provider));
        owned.shutdown().await.unwrap();
        // Idempotent owner shutdown cannot poll a finished handle or re-export.
        owned.shutdown().await.unwrap();
        let payload = collector.join().unwrap();
        assert!(
            payload
                .windows(b"actual-span-before-immediate-shutdown".len())
                .any(|part| part == b"actual-span-before-immediate-shutdown")
        );
    }
}
