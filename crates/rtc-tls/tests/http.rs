use bytes::Bytes;
use hyper::http::{HeaderMap, header};
use rtc_tls::tokio_rustls::TlsAcceptor;
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

async fn endpoint(responses: Vec<String>) -> (String, tokio::task::JoinHandle<Vec<String>>) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!(
        "https://localhost:{}",
        listener.local_addr().unwrap().port()
    );
    let config = rtc_tls::server_config_from_pem(
        include_bytes!("fixtures/server.pem"),
        include_bytes!("fixtures/server-key.pem"),
    )
    .unwrap();
    let task = tokio::spawn(async move {
        let mut requests = Vec::new();
        for response in responses {
            let (tcp, _) = listener.accept().await.unwrap();
            let mut tls = TlsAcceptor::from(Arc::new(config.clone()))
                .accept(tcp)
                .await
                .unwrap();
            let mut request = Vec::new();
            loop {
                let mut buffer = [0; 1024];
                let count = tls.read(&mut buffer).await.unwrap();
                assert!(count > 0);
                request.extend_from_slice(&buffer[..count]);
                assert!(request.len() <= 65536);
                if let Some(split) = request.windows(4).position(|chunk| chunk == b"\r\n\r\n") {
                    let head = String::from_utf8_lossy(&request[..split]);
                    let len: usize = head
                        .lines()
                        .find_map(|line| {
                            line.to_ascii_lowercase()
                                .strip_prefix("content-length:")
                                .map(|value| value.trim().parse().unwrap())
                        })
                        .unwrap_or(0);
                    if request.len() >= split + 4 + len {
                        break;
                    }
                }
            }
            requests.push(String::from_utf8(request).unwrap());
            tls.write_all(response.as_bytes()).await.unwrap();
            let _ = tls.shutdown().await;
        }
        requests
    });
    (url, task)
}
fn client() -> rtc_tls::http::HttpsClient {
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/root.pem");
    rtc_tls::http::HttpsClient::with_config(
        rtc_tls::client_config_with_ca_file(&path).unwrap(),
        Duration::from_secs(2),
        Duration::from_secs(5),
    )
}
fn headers() -> HeaderMap {
    let mut headers = HeaderMap::new();
    headers.insert(
        header::AUTHORIZATION,
        "signed-webhook-token".parse().unwrap(),
    );
    headers.insert(
        header::CONTENT_TYPE,
        "application/webhook+json".parse().unwrap(),
    );
    headers
}
#[tokio::test]
async fn https_post_and_307_keep_method_body_and_same_origin_authorization() {
    let (url, server) = endpoint(vec!["HTTP/1.1 307 Temporary Redirect\r\nLocation: /next\r\nContent-Length: 0\r\nConnection: close\r\n\r\n".into(), "HTTP/1.1 200 OK\r\nContent-Length: 0\r\nConnection: close\r\n\r\n".into()]).await;
    let (status, _) = client()
        .post(&url, headers(), Bytes::from_static(b"event-body"))
        .await
        .unwrap();
    assert_eq!(status, 200);
    let requests = server.await.unwrap();
    assert!(requests[0].starts_with("POST / HTTP/1.1"));
    assert!(requests[1].starts_with("POST /next HTTP/1.1"));
    for request in requests {
        assert!(request.contains("authorization: signed-webhook-token"));
        assert!(request.ends_with("event-body"));
    }
}
#[tokio::test]
async fn redirect_to_another_origin_removes_authorization_and_302_changes_to_get() {
    let (destination, dest_server) = endpoint(vec![
        "HTTP/1.1 200 OK\r\nContent-Length: 0\r\nConnection: close\r\n\r\n".into(),
    ])
    .await;
    let (url, server) = endpoint(vec![format!("HTTP/1.1 302 Found\r\nLocation: {destination}/next\r\nContent-Length: 0\r\nConnection: close\r\n\r\n")]).await;
    assert_eq!(
        client()
            .post(&url, headers(), Bytes::from_static(b"event-body"))
            .await
            .unwrap()
            .0,
        200
    );
    assert_eq!(server.await.unwrap().len(), 1);
    let requests = dest_server.await.unwrap();
    assert!(requests[0].starts_with("GET /next HTTP/1.1"));
    assert!(!requests[0].contains("authorization:"));
    assert!(!requests[0].contains("event-body"));
}
#[tokio::test]
async fn redirects_have_a_finite_hop_limit() {
    let redirect = "HTTP/1.1 307 Temporary Redirect\r\nLocation: /again\r\nContent-Length: 0\r\nConnection: close\r\n\r\n";
    let (url, server) = endpoint(vec![redirect.into(); 10]).await;
    let error = client()
        .post(&url, headers(), Bytes::new())
        .await
        .unwrap_err();
    assert!(matches!(error, rtc_tls::http::HttpsError::Request(_)));
    assert_eq!(server.await.unwrap().len(), 10);
}

#[tokio::test]
async fn http_upgrade_redirect_uses_verified_https_and_preserves_307_body() {
    let (destination, server) = endpoint(vec![
        "HTTP/1.1 200 OK\r\nContent-Length: 0\r\nConnection: close\r\n\r\n".into(),
    ])
    .await;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let origin = format!("HTTP://127.0.0.1:{}", listener.local_addr().unwrap().port());
    let redirect = tokio::spawn(async move {
        let (mut tcp, _) = listener.accept().await.unwrap();
        let mut request = Vec::new();
        loop {
            let mut buffer = [0; 1024];
            let count = tcp.read(&mut buffer).await.unwrap();
            assert!(count > 0);
            request.extend_from_slice(&buffer[..count]);
            if request.windows(4).any(|part| part == b"\r\n\r\n") {
                break;
            }
        }
        tcp.write_all(format!("HTTP/1.1 307 Temporary Redirect\r\nLocation: {destination}/upgraded\r\nContent-Length: 0\r\nConnection: close\r\n\r\n").as_bytes()).await.unwrap();
    });
    assert_eq!(
        client()
            .post(&origin, headers(), Bytes::from_static(b"event-body"))
            .await
            .unwrap()
            .0,
        200
    );
    redirect.await.unwrap();
    let requests = server.await.unwrap();
    assert!(requests[0].starts_with("POST /upgraded HTTP/1.1"));
    assert!(requests[0].ends_with("event-body"));
    assert!(!requests[0].contains("authorization:"));
}
