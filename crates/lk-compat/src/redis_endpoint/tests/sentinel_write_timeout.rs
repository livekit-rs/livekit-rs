use super::{sentinel_read_timeout::mock_redis, *};
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpSocket};
use tokio::sync::oneshot;

const PAYLOAD: usize = 16 << 20;

async fn line(reader: &mut (impl AsyncBufReadExt + Unpin)) -> String {
    let mut line = String::new();
    assert!(reader.read_line(&mut line).await.unwrap() > 0);
    line
}

async fn read_set_prefix(
    reader: &mut (impl AsyncBufReadExt + Unpin),
    write: &mut (impl AsyncWriteExt + Unpin),
) {
    loop {
        let array = line(reader).await;
        let _length = line(reader).await;
        let method = line(reader).await;
        if method.eq_ignore_ascii_case("SET\r\n") {
            assert_eq!(array, "*3\r\n");
            assert_eq!(line(reader).await, "$7\r\n");
            assert_eq!(line(reader).await, "payload\r\n");
            assert_eq!(line(reader).await, format!("${PAYLOAD}\r\n"));
            break;
        }
        assert!(method.eq_ignore_ascii_case("CLIENT\r\n"), "{method}");
        assert_eq!(array, "*4\r\n");
        for _ in 0..6 {
            let _ = line(reader).await;
        }
        write.write_all(b"+OK\r\n").await.unwrap();
    }
}

#[tokio::test]
async fn sentinel_write_timeout_aborts_partial_tcp_command_and_preserves_complete_writes() {
    for (timeout_ms, reject) in [(35, true), (0, true), (1000, false), (-1, false)] {
        let socket = TcpSocket::new_v4().unwrap();
        socket.set_recv_buffer_size(4096).unwrap();
        socket.bind("127.0.0.1:0".parse().unwrap()).unwrap();
        let master = socket.listen(8).unwrap();
        let master_port = master.local_addr().unwrap().port();
        let sentinel = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let mut target = plaintext_target(vec![sentinel.local_addr().unwrap().to_string()]);
        target.discovery_read_timeout = Duration::ZERO; // Separate write from response timeout.
        target.write_timeout = sentinel_write_timeout(timeout_ms);
        let discovery = mock_redis(sentinel, Duration::ZERO, Some(master_port));
        let (resume, hold) = oneshot::channel();
        let peer = tokio::spawn(async move {
            let (stream, _) = master.accept().await.unwrap();
            let (read, mut write) = stream.into_split();
            let mut reader = BufReader::new(read);
            read_set_prefix(&mut reader, &mut write).await;
            // A completed prefix establishes that the command reached the
            // wire. The 4 KiB TCP window cannot hold the 16 MiB request.
            if reject {
                hold.await.unwrap();
                let bytes = tokio::io::copy(&mut reader, &mut tokio::io::sink())
                    .await
                    .unwrap();
                assert!(
                    bytes > 0 && bytes < (PAYLOAD + 2) as u64,
                    "partial bytes {bytes}"
                );
            } else {
                tokio::time::sleep(Duration::from_millis(80)).await;
                let mut payload = vec![0; PAYLOAD + 2];
                reader.read_exact(&mut payload).await.unwrap();
                assert!(payload[..PAYLOAD].iter().all(|byte| *byte == b'x'));
                assert_eq!(&payload[PAYLOAD..], b"\r\n");
                write.write_all(b"+OK\r\n").await.unwrap();
                assert_eq!(line(&mut reader).await, "*1\r\n");
                assert_eq!(line(&mut reader).await, "$4\r\n");
                assert!(line(&mut reader).await.eq_ignore_ascii_case("PING\r\n"));
                write.write_all(b"+PONG\r\n").await.unwrap();
            }
        });

        let endpoint = RedisEndpoint::sentinel(target);
        let mut connection = endpoint.dial(&AsyncConnectionConfig::new()).await.unwrap();
        let cmd = &mut redis::cmd("SET");
        cmd.arg("payload").arg(vec![b'x'; PAYLOAD]);
        let observed = tokio::time::timeout(
            Duration::from_secs(4),
            cmd.query_async::<()>(&mut connection),
        )
        .await
        .expect("outer watchdog must not be the observed write deadline");
        if reject {
            let err = observed.expect_err("partial frame must not complete");
            assert!(err.is_timeout(), "configured {timeout_ms}ms: {err}");
            assert!(
                err.to_string().contains("Redis socket write timed out"),
                "{err}"
            );
            resume.send(()).unwrap();
        } else {
            observed.expect("positive control should finish writing");
            let pong: String = redis::cmd("PING")
                .query_async(&mut connection)
                .await
                .unwrap();
            assert_eq!(pong, "PONG");
        }
        tokio::time::timeout(Duration::from_secs(2), peer)
            .await
            .unwrap()
            .unwrap();
        discovery.abort();
        assert!(discovery.await.unwrap_err().is_cancelled());
    }
}

#[tokio::test]
async fn sentinel_read_clock_follows_the_real_flush_and_failed_writer_releases_pending_commands() {
    for (write_ms, read_ms, expected_op) in
        [(2000, 350, ""), (2000, 350, "read"), (35, 1000, "write")]
    {
        let socket = TcpSocket::new_v4().unwrap();
        socket.set_recv_buffer_size(4096).unwrap();
        socket.bind("127.0.0.1:0".parse().unwrap()).unwrap();
        let master = socket.listen(8).unwrap();
        let master_port = master.local_addr().unwrap().port();
        let sentinel = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let mut target = plaintext_target(vec![sentinel.local_addr().unwrap().to_string()]);
        target.discovery_read_timeout = Duration::from_millis(read_ms);
        target.write_timeout = sentinel_write_timeout(write_ms);
        let discovery = mock_redis(sentinel, Duration::ZERO, Some(master_port));
        let (resume, hold) = oneshot::channel();
        let (prefix_seen, observed_prefix) = oneshot::channel();
        let peer = tokio::spawn(async move {
            let (stream, _) = master.accept().await.unwrap();
            let (read, mut write) = stream.into_split();
            let mut reader = BufReader::new(read);
            read_set_prefix(&mut reader, &mut write).await;
            prefix_seen.send(()).unwrap();
            if expected_op == "write" {
                hold.await.unwrap();
                let bytes = tokio::io::copy(&mut reader, &mut tokio::io::sink())
                    .await
                    .unwrap();
                assert!(
                    bytes > 0 && bytes < (PAYLOAD + 2) as u64,
                    "partial bytes {bytes}"
                );
                return;
            }
            // A timer armed before flush must expire during this stall. Leave
            // room in the separate read budget for the last TCP window to drain.
            tokio::time::sleep(Duration::from_millis(500)).await;
            let mut payload = vec![0; PAYLOAD + 2];
            reader.read_exact(&mut payload).await.unwrap();
            if expected_op != "read" {
                // Reply as soon as the complete request arrives. The test's
                // debug-mode 16MiB validation scan is not Redis read latency.
                write.write_all(b"+OK\r\n").await.unwrap();
            }
            assert!(payload[..PAYLOAD].iter().all(|byte| *byte == b'x'));
            assert_eq!(&payload[PAYLOAD..], b"\r\n");
            if expected_op == "read" {
                // The timeout must follow a complete write.
                tokio::time::sleep(Duration::from_millis(600)).await;
                assert_eq!(reader.read(&mut [0]).await.unwrap(), 0);
            }
        });
        let endpoint = RedisEndpoint::sentinel(target);
        let mut connection = endpoint.dial(&AsyncConnectionConfig::new()).await.unwrap();
        let mut concurrent = connection.clone();
        let mut set = redis::cmd("SET");
        set.arg("payload").arg(vec![b'x'; PAYLOAD]);
        let commands = async {
            tokio::join!(set.query_async::<()>(&mut connection), async {
                observed_prefix.await.unwrap();
                if expected_op == "write" {
                    let result = redis::cmd("PING")
                        .query_async::<String>(&mut concurrent)
                        .await;
                    let err = result.expect_err("a request behind a partial writer must terminate");
                    assert!(
                        err.is_connection_dropped() || err.is_timeout(),
                        "unexpected pending request failure: {err}"
                    );
                }
            })
        };
        let (result, ()) = tokio::time::timeout(Duration::from_secs(4), commands)
            .await
            .expect("both admitted and pending requests must terminate");
        if expected_op.is_empty() {
            result.expect("slow socket write followed by quick response must succeed");
        } else {
            let err = result.expect_err("expected actual socket/response timeout");
            assert!(err.is_timeout(), "{expected_op}: {err}");
            assert_eq!(
                err.to_string().contains("Redis socket write timed out"),
                expected_op == "write",
                "{err}"
            );
        }
        let peer_exit = tokio::time::timeout(Duration::from_secs(2), peer);
        if expected_op == "write" {
            resume.send(()).unwrap();
            // Observe EOF while both handles still exist. Dropping the
            // caller's last handle must not explain the socket closure.
            peer_exit.await.unwrap().unwrap();
        } else {
            drop(connection);
            drop(concurrent);
            peer_exit.await.unwrap().unwrap();
        }
        discovery.abort();
        assert!(discovery.await.unwrap_err().is_cancelled());
    }
}
