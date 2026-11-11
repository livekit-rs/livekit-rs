//! One webhook POST with go-retryablehttp v0.7.8 default semantics
//! (impl-notes/api-auth-webhook.md §4.3-4.4): `RetryMax = 4` (5 attempts),
//! exponential backoff `1s·2^n` capped at 30 s, explicit `Retry-After` delays
//! honored on 429/503, retry on
//! transport errors, 429 and 5xx except 501, never on other statuses. The same
//! body and JWT are reused for every attempt.

use std::time::{Duration, SystemTime};

use bytes::Bytes;
use reqwest::header::{
    ACCEPT_ENCODING, AUTHORIZATION, CONTENT_TYPE, HeaderMap, RETRY_AFTER, USER_AGENT,
};

pub const RETRY_MAX: u32 = 4;
pub const BACKOFF_MIN: Duration = Duration::from_secs(1);
pub const BACKOFF_MAX: Duration = Duration::from_secs(30);
pub const CONTENT_TYPE_WEBHOOK: &str = "application/webhook+json";
pub const USER_AGENT_LIVEKIT: &str = "LiveKit";

/// Result of a delivery attempt sequence.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum DeliveryOutcome {
    /// A response was received and not retried (2xx, or a non-retryable status;
    /// upstream logs 4xx as "sent webhook" too).
    Delivered(u16),
    /// Retries exhausted or a non-retryable transport error.
    Failed(String),
}

/// `DefaultRetryPolicy` for a received status.
pub fn should_retry_status(status: u16) -> bool {
    status == 429 || (status >= 500 && status != 501)
}

/// `DefaultBackoff(min=1s, max=30s, attemptNum, resp)`.
pub fn backoff(attempt: u32, status: Option<u16>, headers: Option<&HeaderMap>) -> Duration {
    if matches!(status, Some(429) | Some(503))
        && let Some(d) = headers.and_then(retry_after)
    {
        return d;
    }
    let mult = 1u64.checked_shl(attempt).unwrap_or(u64::MAX);
    let secs = BACKOFF_MIN.as_secs().saturating_mul(mult);
    Duration::from_secs(secs).min(BACKOFF_MAX)
}

/// `Retry-After` as signed decimal seconds or an RFC1123 GMT/UTC date.
pub fn retry_after(headers: &HeaderMap) -> Option<Duration> {
    let value = headers.get(RETRY_AFTER)?.to_str().ok()?;
    super::retry_after::parse(value, SystemTime::now())
}

/// POST `body` to `url` with the webhook headers, retrying per policy.
pub async fn deliver(
    client: &reqwest::Client,
    url: &str,
    jwt: &str,
    body: Bytes,
) -> DeliveryOutcome {
    deliver_impl(client, None, url, jwt, body).await
}

pub async fn deliver_with_https(
    client: &reqwest::Client,
    https: &rtc_tls::http::HttpsClient,
    url: &str,
    jwt: &str,
    body: Bytes,
) -> DeliveryOutcome {
    deliver_impl(client, Some(https), url, jwt, body).await
}

async fn deliver_impl(
    client: &reqwest::Client,
    https: Option<&rtc_tls::http::HttpsClient>,
    url: &str,
    jwt: &str,
    body: Bytes,
) -> DeliveryOutcome {
    let mut last_error = String::new();
    for attempt in 0..=RETRY_MAX {
        let result: Result<(u16, HeaderMap), (String, bool)> = if let Some(https) = https {
            // One parser/connector handles both schemes and every redirect hop.
            // Raw prefix checks misroute legal case-insensitive HTTPS schemes,
            // and a plaintext-only client cannot follow HTTP -> HTTPS redirects.
            let mut headers = HeaderMap::new();
            let Ok(auth) = jwt.parse() else {
                return DeliveryOutcome::Failed("invalid Authorization header".into());
            };
            headers.insert(AUTHORIZATION, auth);
            headers.insert(
                CONTENT_TYPE,
                CONTENT_TYPE_WEBHOOK.parse().expect("static header"),
            );
            headers.insert(
                USER_AGENT,
                USER_AGENT_LIVEKIT.parse().expect("static header"),
            );
            // Go's http.Transport advertises gzip on every request it sends.
            headers.insert(ACCEPT_ENCODING, "gzip".parse().expect("static header"));
            https.post(url, headers, body.clone()).await.map_err(|e| {
                let terminal = matches!(e, rtc_tls::http::HttpsError::Request(_));
                (e.to_string(), terminal)
            })
        } else {
            client
                .post(url)
                .header(AUTHORIZATION, jwt)
                .header(CONTENT_TYPE, CONTENT_TYPE_WEBHOOK)
                .header(USER_AGENT, USER_AGENT_LIVEKIT)
                // Go's http.Transport advertises gzip on every request it sends.
                .header(ACCEPT_ENCODING, "gzip")
                .body(body.clone())
                .send()
                .await
                .map(|response| (response.status().as_u16(), response.headers().clone()))
                .map_err(|e| {
                    let terminal = e.is_builder() || e.is_redirect();
                    (e.to_string(), terminal)
                })
        };
        let wait = match result {
            Ok((status, headers)) => {
                if !should_retry_status(status) {
                    return DeliveryOutcome::Delivered(status);
                }
                last_error = format!("unexpected status {status}");
                backoff(attempt, Some(status), Some(&headers))
                // the response body is dropped unread
            }
            Err((error, terminal)) => {
                if terminal {
                    return DeliveryOutcome::Failed(error);
                }
                last_error = error;
                backoff(attempt, None, None)
            }
        };
        if attempt == RETRY_MAX {
            break;
        }
        tokio::time::sleep(wait).await;
    }
    DeliveryOutcome::Failed(format!(
        "POST {url} giving up after {} attempt(s): {last_error}",
        RETRY_MAX + 1
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use reqwest::header::HeaderValue;

    #[test]
    fn retry_policy() {
        assert!(should_retry_status(429));
        assert!(should_retry_status(500));
        assert!(should_retry_status(503));
        assert!(!should_retry_status(501));
        assert!(!should_retry_status(404));
        assert!(!should_retry_status(200));
    }

    #[test]
    fn backoff_schedule() {
        let secs: Vec<u64> = (0..7).map(|n| backoff(n, None, None).as_secs()).collect();
        assert_eq!(secs, vec![1, 2, 4, 8, 16, 30, 30]);
        assert_eq!(backoff(200, None, None), BACKOFF_MAX);
    }

    #[test]
    fn retry_after_on_429_and_503_only() {
        let mut h = HeaderMap::new();
        h.insert(RETRY_AFTER, HeaderValue::from_static("7"));
        assert_eq!(backoff(0, Some(429), Some(&h)), Duration::from_secs(7));
        assert_eq!(backoff(0, Some(503), Some(&h)), Duration::from_secs(7));
        assert_eq!(backoff(0, Some(500), Some(&h)), Duration::from_secs(1));
        h.insert(
            RETRY_AFTER,
            HeaderValue::from_static("Wed, 21 Oct 2015 07:28:00 GMT"),
        );
        assert_eq!(backoff(2, Some(429), Some(&h)), Duration::ZERO);
        assert_eq!(backoff(2, Some(500), Some(&h)), Duration::from_secs(4));
        h.insert(RETRY_AFTER, HeaderValue::from_static("9223372036854775808"));
        assert_eq!(backoff(0, Some(503), Some(&h)), Duration::from_secs(1));
    }

    #[test]
    fn retry_after_honors_long_explicit_delays_and_keeps_exponential_cap() {
        for value in [300, 301, 99_999] {
            let mut headers = HeaderMap::new();
            headers.insert(
                RETRY_AFTER,
                HeaderValue::from_str(&value.to_string()).unwrap(),
            );
            headers.append(RETRY_AFTER, HeaderValue::from_static("0"));
            let delay = Duration::from_secs(value);
            assert_eq!(backoff(20, Some(429), Some(&headers)), delay);
            assert_eq!(backoff(20, Some(503), Some(&headers)), delay);
            assert_eq!(backoff(20, Some(500), Some(&headers)), BACKOFF_MAX);
        }

        let mut headers = HeaderMap::new();
        headers.insert(
            RETRY_AFTER,
            HeaderValue::from_static("Tue, 01 Jan 9999 00:00:00 GMT"),
        );
        assert!(backoff(0, Some(503), Some(&headers)) > Duration::from_secs(300));
    }

    #[tokio::test]
    async fn https_delivery_preserves_signed_headers_and_retries_503() {
        use rtc_tls::rustls::pki_types::{CertificateDer, pem::PemObject};
        use std::sync::Arc;
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let cert = include_bytes!("../../../rtc-tls/tests/fixtures/server.pem");
        let key = include_bytes!("../../../rtc-tls/tests/fixtures/server-key.pem");
        let root = include_bytes!("../../../rtc-tls/tests/fixtures/root.pem");
        let server_config = Arc::new(rtc_tls::server_config_from_pem(cert, key).unwrap());
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!(
            "HTTPS://localhost:{}/hook",
            listener.local_addr().unwrap().port()
        );
        let body = Bytes::from_static(br#"{"event":"room_started"}"#);
        let keys = Arc::new(crate::auth::KeyStore::new(vec![(
            "key".into(),
            "secret".into(),
        )]));
        let jwt = crate::auth::TokenMinter::new(keys.clone())
            .webhook_token("key", &body, rtc_core::util::unix_s())
            .unwrap();
        let expected_body = body.clone();
        let expected_jwt = jwt.clone();
        let server = tokio::spawn(async move {
            for attempt in 0..2 {
                let (tcp, _) = listener.accept().await.unwrap();
                let mut tls = rtc_tls::tokio_rustls::TlsAcceptor::from(server_config.clone())
                    .accept(tcp)
                    .await
                    .unwrap();
                let mut request = Vec::new();
                loop {
                    let mut bytes = [0; 1024];
                    let count = tls.read(&mut bytes).await.unwrap();
                    assert!(count > 0);
                    request.extend_from_slice(&bytes[..count]);
                    assert!(request.len() < 65536);
                    if let Some(end) = request.windows(4).position(|part| part == b"\r\n\r\n")
                        && request.len() >= end + 4 + expected_body.len()
                    {
                        break;
                    }
                }
                let request = String::from_utf8(request).unwrap();
                assert!(request.starts_with("POST /hook HTTP/1.1"));
                assert!(request.contains(&format!("authorization: {expected_jwt}")));
                assert!(request.contains("content-type: application/webhook+json"));
                assert!(request.ends_with(std::str::from_utf8(&expected_body).unwrap()));
                let token = crate::auth::verify_token(
                    &keys,
                    &expected_jwt,
                    rtc_core::util::unix_s() as f64,
                )
                .unwrap();
                assert_eq!(
                    token.grants.sha256,
                    crate::auth::minter::body_sha256(&expected_body)
                );
                let status = if attempt == 0 {
                    "503 Service Unavailable"
                } else {
                    "200 OK"
                };
                let response = format!(
                    "HTTP/1.1 {status}\r\nRetry-After: 0\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
                );
                tls.write_all(response.as_bytes()).await.unwrap();
                let _ = tls.shutdown().await;
            }
        });
        let mut roots = rtc_tls::rustls::RootCertStore::empty();
        roots
            .add(CertificateDer::from_pem_slice(root).unwrap())
            .unwrap();
        let https = rtc_tls::http::HttpsClient::with_config(
            rtc_tls::client_config_with_roots(roots).unwrap(),
            Duration::from_secs(2),
            Duration::from_secs(5),
        );
        let plain = reqwest::Client::new();
        assert_eq!(
            deliver_with_https(&plain, &https, &url, &jwt, body).await,
            DeliveryOutcome::Delivered(200)
        );
        server.await.unwrap();
    }
}
