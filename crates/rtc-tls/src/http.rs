//! HTTPS transport with an explicit verified rustls configuration. No platform
//! verifier, native crypto backend or global default provider is involved.
use std::{
    sync::{Arc, OnceLock},
    time::Duration,
};

use bytes::Bytes;
use http_body_util::Full;
use hyper::http::{HeaderMap, Method, Request, header};
use hyper_rustls::{HttpsConnector, HttpsConnectorBuilder};
use hyper_util::client::legacy::{Client, connect::HttpConnector};
use hyper_util::rt::TokioExecutor;

type VerifiedClient = Client<HttpsConnector<HttpConnector>, Full<Bytes>>;
#[derive(Clone)]
pub struct HttpsClient {
    plain: Client<HttpConnector, Full<Bytes>>,
    tls: Arc<OnceLock<Result<VerifiedClient, String>>>,
    connect_timeout: Duration,
    timeout: Duration,
}

#[derive(Debug, thiserror::Error)]
pub enum HttpsError {
    #[error("invalid HTTPS request: {0}")]
    Request(String),
    #[error("HTTPS request timed out")]
    Timeout,
    #[error("HTTPS request failed: {0}")]
    Transport(String),
}

impl HttpsClient {
    pub fn new(connect_timeout: Duration, timeout: Duration) -> Result<Self, super::TlsError> {
        // Match Go's lazy trust initialization: plain HTTP works on hosts
        // without a CA store; only an actual HTTPS hop needs trust anchors.
        let mut tcp = HttpConnector::new();
        tcp.set_connect_timeout(Some(connect_timeout));
        Ok(Self {
            plain: Client::builder(TokioExecutor::new()).build(tcp),
            tls: Arc::new(OnceLock::new()),
            connect_timeout,
            timeout,
        })
    }

    pub fn with_config(
        config: rustls::ClientConfig,
        connect_timeout: Duration,
        timeout: Duration,
    ) -> Self {
        let client =
            Self::new(connect_timeout, timeout).expect("plain client construction is infallible");
        let _ = client
            .tls
            .set(Ok(Self::tls_client(config, connect_timeout)));
        client
    }

    fn tls_client(config: rustls::ClientConfig, connect_timeout: Duration) -> VerifiedClient {
        let mut tcp = HttpConnector::new();
        tcp.enforce_http(false);
        tcp.set_connect_timeout(Some(connect_timeout));
        let connector = HttpsConnectorBuilder::new()
            .with_tls_config(config)
            .https_or_http()
            .enable_http1()
            .wrap_connector(tcp);
        Client::builder(TokioExecutor::new()).build(connector)
    }

    fn verified_client(&self) -> Result<&VerifiedClient, HttpsError> {
        self.tls
            .get_or_init(|| {
                super::client_config()
                    .map(|config| Self::tls_client(config, self.connect_timeout))
                    .map_err(|error| error.to_string())
            })
            .as_ref()
            .map_err(|error| HttpsError::Request(format!("TLS trust configuration: {error}")))
    }

    /// Follow at most ten requests using net/http's 301/302/303 GET conversion
    /// and 307/308 replay semantics. Across origins, authorization/cookies are
    /// removed permanently. Every HTTPS hop verifies the new host/chain.
    pub async fn post(
        &self,
        url: &str,
        headers: HeaderMap,
        body: Bytes,
    ) -> Result<(u16, HeaderMap), HttpsError> {
        let mut url = url::Url::parse(url).map_err(|e| HttpsError::Request(e.to_string()))?;
        if !matches!(url.scheme(), "http" | "https") {
            return Err(HttpsError::Request(
                "initial endpoint must use http or https".into(),
            ));
        }
        tokio::time::timeout(self.timeout, async {
            let mut method = Method::POST;
            let mut body = body;
            let mut headers = headers;
            for hop in 0..10 {
                let mut request = Request::builder()
                    .method(method.clone())
                    .uri(url.as_str())
                    .body(Full::new(body.clone()))
                    .map_err(|e| HttpsError::Request(e.to_string()))?;
                *request.headers_mut() = headers.clone();
                let response = if url.scheme() == "https" {
                    self.verified_client()?.request(request).await
                } else {
                    self.plain.request(request).await
                }
                .map_err(|e| {
                    HttpsError::Transport(format!(
                        "{e}: {}",
                        std::error::Error::source(&e)
                            .map(ToString::to_string)
                            .unwrap_or_default()
                    ))
                })?;
                let status = response.status().as_u16();
                if !matches!(status, 301 | 302 | 303 | 307 | 308) {
                    return Ok((status, response.headers().clone()));
                }
                let Some(location) = response.headers().get(header::LOCATION) else {
                    return Ok((status, response.headers().clone()));
                };
                if hop == 9 {
                    return Err(HttpsError::Request("stopped after 10 redirects".into()));
                }
                let location = location
                    .to_str()
                    .map_err(|e| HttpsError::Request(e.to_string()))?;
                let next = url
                    .join(location)
                    .map_err(|e| HttpsError::Request(e.to_string()))?;
                if !matches!(next.scheme(), "http" | "https") {
                    return Err(HttpsError::Request(
                        "redirect must use http or https".into(),
                    ));
                }
                if url.origin() != next.origin() {
                    headers.remove(header::AUTHORIZATION);
                    headers.remove(header::COOKIE);
                    headers.remove(header::PROXY_AUTHORIZATION);
                }
                if matches!(status, 301..=303) && method != Method::GET && method != Method::HEAD {
                    method = Method::GET;
                    body = Bytes::new();
                    headers.remove(header::CONTENT_LENGTH);
                    headers.remove(header::TRANSFER_ENCODING);
                }
                url = next;
            }
            unreachable!("loop returns on its last hop")
        })
        .await
        .map_err(|_| HttpsError::Timeout)?
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    #[tokio::test]
    async fn empty_trust_store_does_not_disable_plain_http_or_fail_until_https_hop() {
        let client = HttpsClient::new(Duration::from_secs(1), Duration::from_secs(2)).unwrap();
        assert!(client.tls.get().is_none());
        // Model a host whose first TLS trust-source load found no valid roots.
        assert!(
            client
                .tls
                .set(Err("no valid system trust anchors".into()))
                .is_ok()
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            for redirect in [false, true] {
                let (mut stream, _) = listener.accept().await.unwrap();
                let mut bytes = [0; 2048];
                let n = stream.read(&mut bytes).await.unwrap();
                assert!(n > 0);
                let status = if redirect {
                    "307 Temporary Redirect\r\nLocation: https://localhost:9/"
                } else {
                    "200 OK"
                };
                stream
                    .write_all(
                        format!(
                            "HTTP/1.1 {status}\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
                        )
                        .as_bytes(),
                    )
                    .await
                    .unwrap();
            }
        });
        let url = format!("http://{addr}/");
        assert_eq!(
            client
                .post(&url, HeaderMap::new(), Bytes::new())
                .await
                .unwrap()
                .0,
            200
        );
        assert!(matches!(
            client.post(&url, HeaderMap::new(), Bytes::new()).await,
            Err(HttpsError::Request(_))
        ));
        server.await.unwrap();
    }
}
