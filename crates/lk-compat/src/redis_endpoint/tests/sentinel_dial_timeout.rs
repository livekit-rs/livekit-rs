use super::{sentinel_read_timeout::serve, *};
use rtc_tls::tokio_rustls::TlsAcceptor;
use std::path::Path;
use tokio::io::AsyncReadExt;
use tokio::net::TcpListener;
use tokio::task::{JoinHandle, JoinSet};

const DIAL: Duration = Duration::from_millis(150);
const READ: Duration = Duration::from_millis(800);

fn tls_fixture(
    listener: TcpListener,
    tls: TlsAcceptor,
    auth_delay: Duration,
    sentinel_master: Option<u16>,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        let mut clients = JoinSet::new();
        loop {
            tokio::select! {
                Some(result) = clients.join_next() => result.unwrap(),
                Ok((socket, _)) = listener.accept() => {
                    let tls = tls.clone();
                    clients.spawn(async move {
                        if let Ok(stream) = tls.accept(socket).await {
                            serve(stream, Duration::ZERO, auth_delay, sentinel_master).await;
                        }
                    });
                }
            }
        }
    })
}

#[tokio::test]
async fn sentinel_master_dial_bounds_tls_but_not_redis_auth_for_native_and_sni_override() {
    // The livecloud entry point installs this before using redis-rs TLS.
    let _ = rtc_tls::crypto_provider().install_default();
    let files = Path::new(env!("CARGO_MANIFEST_DIR")).join("../rtc-tls/tests/fixtures");
    let root = std::fs::read(files.join("root.pem")).unwrap();
    let tls = TlsAcceptor::from(Arc::new(
        rtc_tls::server_config(&files.join("server.pem"), &files.join("server-key.pem")).unwrap(),
    ));
    for use_sni in [false, true] {
        let stalled_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let stalled_addr = stalled_listener.local_addr().unwrap();
        let stalled = tokio::spawn(async move {
            let (mut peer, _) = stalled_listener.accept().await.unwrap();
            let mut hello = Vec::new();
            peer.read_to_end(&mut hello).await.unwrap();
            assert_eq!(
                hello.first(),
                Some(&22),
                "peer must receive TLS ClientHello"
            );
        });
        let healthy = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let healthy_addr = healthy.local_addr().unwrap();
        let master = tls_fixture(healthy, tls.clone(), Duration::from_millis(350), None);
        let endpoint_for = |sentinel_addr: std::net::SocketAddr| {
            let material = root.clone();
            let make = move |addr: &str| -> Result<ConnectionInfo, String> {
                let client = redis::Client::build_with_tls(
                    format!("rediss://{addr}/"),
                    redis::TlsCertificates {
                        client_tls: None,
                        root_cert: Some(material.clone()),
                    },
                )
                .map_err(|e| e.to_string())?;
                Ok(client.get_connection_info().clone())
            };
            let make_master = make.clone();
            let mut target = plaintext_target(vec![sentinel_addr.to_string()]);
            target.discovery_dial_timeout = DIAL;
            target.discovery_read_timeout = READ;
            target.sentinel_info = Arc::new(make);
            target.master_info = Arc::new(move |addr| {
                let info = make_master(&addr)?;
                let settings = info.redis_settings().clone().set_password("fixture");
                Ok(info.set_redis_settings(settings))
            });
            if use_sni {
                RedisEndpoint::sentinel_with_tls_sni(
                    target,
                    TlsSniOverride {
                        server_name: "localhost".into(),
                        root_pem: Some(root.clone()),
                        client_cert_pem: None,
                        client_key_pem: None,
                    },
                )
            } else {
                RedisEndpoint::sentinel(target)
            }
        };
        let caller =
            AsyncConnectionConfig::new().set_connection_timeout(Some(Duration::from_secs(5)));
        let mut sentinels = Vec::new();
        for (master_addr, timeout) in [(stalled_addr, true), (healthy_addr, false)] {
            let sentinel = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let endpoint = endpoint_for(sentinel.local_addr().unwrap());
            sentinels.push(tls_fixture(
                sentinel,
                tls.clone(),
                Duration::ZERO,
                Some(master_addr.port()),
            ));
            let result = tokio::time::timeout(Duration::from_secs(2), endpoint.connect(&caller))
                .await
                .expect("outer test deadline; the real TLS dial budget must fire");
            if timeout {
                let Err(error) = result else {
                    panic!("stalled TLS dial unexpectedly succeeded")
                };
                assert!(error.is_timeout(), "actual TLS dial error {error}");
            } else {
                let pool = result.expect("350ms AUTH must use read budget rather than 150ms dial");
                let pong: String = redis::cmd("PING")
                    .query_async(&mut pool.begin())
                    .await
                    .unwrap();
                assert_eq!(pong, "PONG");
            }
        }
        tokio::time::timeout(Duration::from_secs(1), stalled)
            .await
            .unwrap()
            .unwrap();
        for task in std::iter::once(master).chain(sentinels) {
            task.abort();
            assert!(task.await.unwrap_err().is_cancelled());
        }
    }
}
