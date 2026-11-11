use super::*;
use futures_util::StreamExt;
use tokio::io::{AsyncRead, AsyncWrite, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::task::{JoinHandle, JoinSet};
use tokio_util::codec::FramedRead;

pub(super) fn mock_redis(
    listener: TcpListener,
    ping_delay: Duration,
    sentinel_master: Option<u16>,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        let mut clients = JoinSet::new();
        loop {
            tokio::select! {
                Some(result) = clients.join_next() => result.unwrap(),
                Ok((socket, _)) = listener.accept() => {
                    clients.spawn(async move {
                        serve(socket, ping_delay, Duration::ZERO, sentinel_master).await;
                    });
                }
            }
        }
    })
}

pub(super) async fn serve(
    stream: impl AsyncRead + AsyncWrite + Unpin,
    ping_delay: Duration,
    auth_delay: Duration,
    sentinel_master: Option<u16>,
) {
    let (read, mut write) = tokio::io::split(stream);
    let mut reader = FramedRead::new(read, redis::ValueCodec::default());
    while let Some(Ok(value)) = reader.next().await {
        let command: Vec<String> = redis::from_redis_value(value).unwrap();
        let response = match command[0].to_ascii_uppercase().as_str() {
            "PING" => {
                tokio::time::sleep(ping_delay).await;
                "+PONG\r\n".into()
            }
            "AUTH" => {
                assert_eq!(command[1], "fixture");
                tokio::time::sleep(auth_delay).await;
                "+OK\r\n".into()
            }
            "SENTINEL" if command[1].eq_ignore_ascii_case("GET-MASTER-ADDR-BY-NAME") => {
                assert_eq!(command[2], "mymaster");
                let port = sentinel_master.unwrap().to_string();
                format!("*2\r\n$9\r\n127.0.0.1\r\n${}\r\n{port}\r\n", port.len())
            }
            "CLIENT" => "+OK\r\n".into(),
            // Watch subscriptions are outside this command fixture.
            _ => "-ERR unexpected fixture command\r\n".into(),
        };
        if write.write_all(response.as_bytes()).await.is_err() || write.flush().await.is_err() {
            return;
        }
    }
}

#[tokio::test]
async fn configured_sentinel_read_budget_reaches_master_commands() {
    let master_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let master_address = master_listener.local_addr().unwrap();
    let sentinel_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let sentinel_address = sentinel_listener.local_addr().unwrap();
    let master = mock_redis(master_listener, Duration::from_millis(250), None);
    let sentinel = mock_redis(
        sentinel_listener,
        Duration::ZERO,
        Some(master_address.port()),
    );
    let caller_config = AsyncConnectionConfig::new()
        .set_connection_timeout(Some(Duration::from_secs(2)))
        .set_response_timeout(Some(Duration::from_secs(2)));

    for (read_timeout, timed_out) in [(0, true), (35, true), (1000, false), (-1, false)] {
        let mut target = plaintext_target(vec![sentinel_address.to_string()]);
        (target.discovery_dial_timeout, target.discovery_read_timeout) =
            sentinel_timeouts(0, read_timeout);
        let endpoint = RedisEndpoint::sentinel(target);
        let commands = endpoint.connect(&caller_config).await.unwrap();
        let observed = tokio::time::timeout(
            Duration::from_secs(2),
            redis::cmd("PING").query_async::<String>(&mut commands.begin()),
        )
        .await
        .expect("outer test deadline");
        if timed_out {
            let err = observed.expect_err("master command must use the Sentinel read budget");
            assert!(err.is_timeout(), "unexpected master error {err}");
        } else {
            assert_eq!(observed.unwrap(), "PONG");
        }
    }

    // A non-Sentinel endpoint still uses its caller's budget.
    let simple = RedisEndpoint::simple(
        format!("redis://{master_address}/")
            .into_connection_info()
            .unwrap(),
    );
    let commands = simple.connect(&caller_config).await.unwrap();
    let pong: String = redis::cmd("PING")
        .query_async(&mut commands.begin())
        .await
        .unwrap();
    assert_eq!(pong, "PONG");

    for task in [master, sentinel] {
        task.abort();
        assert!(task.await.unwrap_err().is_cancelled());
    }
}
