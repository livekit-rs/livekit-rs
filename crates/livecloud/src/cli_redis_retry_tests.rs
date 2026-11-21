//! Same tests compile against the pre-candidate public loader/list_nodes.
#[cfg(unix)]
mod unix {
    use std::{
        io::{BufRead, BufReader, Read, Write},
        os::unix::net::{UnixListener, UnixStream},
        sync::{
            Arc, Mutex,
            atomic::{AtomicBool, AtomicUsize, Ordering},
        },
        thread,
    };
    struct Peer {
        path: std::path::PathBuf,
        stop: Arc<AtomicBool>,
        thread: Option<thread::JoinHandle<()>>,
        accepts: Arc<AtomicUsize>,
        hits: Arc<AtomicUsize>,
        active: Arc<Mutex<Vec<UnixStream>>>,
        /// Per-connection command-name trace, indexed by accept order. The
        /// message-bus read connection (C41) is the one that never sends a
        /// command: it carries only the go-redis init handshake.
        traces: Arc<Mutex<Vec<Vec<String>>>>,
    }
    impl Peer {
        fn new(mode: &'static str) -> Self {
            static NEXT: AtomicUsize = AtomicUsize::new(0);
            let path = std::env::temp_dir().join(format!(
                "lk-retry-{}-{}.sock",
                std::process::id(),
                NEXT.fetch_add(1, Ordering::SeqCst)
            ));
            let listener = UnixListener::bind(&path).unwrap();
            let stop = Arc::new(AtomicBool::new(false));
            let accepts = Arc::new(AtomicUsize::new(0));
            let hits = Arc::new(AtomicUsize::new(0));
            let active = Arc::new(Mutex::new(Vec::new()));
            let current = active.clone();
            let traces = Arc::new(Mutex::new(Vec::new()));
            let recorded = traces.clone();
            let (s, a, h) = (stop.clone(), accepts.clone(), hits.clone());
            let task = thread::spawn(move || {
                let mut workers = Vec::new();
                for item in listener.incoming() {
                    if s.load(Ordering::SeqCst) {
                        break;
                    }
                    let Ok(mut stream) = item else { break };
                    current.lock().unwrap().push(stream.try_clone().unwrap());
                    let n = a.fetch_add(1, Ordering::SeqCst);
                    let h = h.clone();
                    let rec = recorded.clone();
                    workers.push(thread::spawn(move || {
                        {
                            let mut t = rec.lock().unwrap();
                            if t.len() <= n {
                                t.resize(n + 1, Vec::new());
                            }
                        }
                        stream
                            .set_read_timeout(Some(std::time::Duration::from_secs(10)))
                            .unwrap();
                        let mut reader = BufReader::new(stream.try_clone().unwrap());
                        if mode == "setup-eof" && n < 3 {
                            assert!(read_command(&mut reader).is_some());
                            if let Err(error) = stream.shutdown(std::net::Shutdown::Write) {
                                assert!(
                                    matches!(
                                        error.kind(),
                                        std::io::ErrorKind::NotConnected
                                            | std::io::ErrorKind::BrokenPipe
                                    ),
                                    "unexpected half-close failure: {error}"
                                );
                            }
                            let mut drain = Vec::new();
                            let _ = reader.read_to_end(&mut drain);
                            return;
                        }
                        while let Some(args) = read_command(&mut reader) {
                            let cmd = String::from_utf8_lossy(&args[0]).to_ascii_uppercase();
                            rec.lock().unwrap()[n].push(cmd.clone());
                            let response: &[u8] = if cmd == "HELLO" {
                                b"-ERR unknown command 'hello'\r\n".as_slice()
                            } else if cmd == "PING" {
                                let n = if mode == "ping-loading" {
                                    h.fetch_add(1, Ordering::SeqCst)
                                } else {
                                    9
                                };
                                if n < 3 {
                                    b"-LOADING loading actual scripted dependency\r\n"
                                } else {
                                    b"+PONG\r\n"
                                }
                            } else if cmd == "HVALS" {
                                let n = if mode == "hvals-tryagain" {
                                    h.fetch_add(1, Ordering::SeqCst)
                                } else {
                                    9
                                };
                                if n < 3 {
                                    b"-TRYAGAIN actual scripted dependency\r\n"
                                } else {
                                    b"*0\r\n"
                                }
                            } else {
                                b"+OK\r\n"
                            };
                            if let Err(error) = stream.write_all(response) {
                                assert!(
                                    matches!(
                                        error.kind(),
                                        std::io::ErrorKind::NotConnected
                                            | std::io::ErrorKind::BrokenPipe
                                    ),
                                    "unexpected response-write failure: {error}"
                                );
                                break;
                            }
                        }
                    }));
                }
                for worker in workers {
                    worker.join().unwrap()
                }
            });

            Self {
                path,
                stop,
                thread: Some(task),
                accepts,
                hits,
                active,
                traces,
            }
        }
    }
    impl Drop for Peer {
        fn drop(&mut self) {
            self.stop.store(true, Ordering::SeqCst);
            for stream in self.active.lock().unwrap().drain(..) {
                let _ = stream.shutdown(std::net::Shutdown::Both);
            }
            let _ = UnixStream::connect(&self.path);
            if let Some(t) = self.thread.take() {
                t.join().unwrap()
            }
            let _ = std::fs::remove_file(&self.path);
        }
    }
    fn read_command(reader: &mut BufReader<UnixStream>) -> Option<Vec<Vec<u8>>> {
        let mut line = String::new();
        reader.read_line(&mut line).ok()?;
        let count: usize = line.strip_prefix('*')?.trim_end().parse().ok()?;
        let mut args = Vec::new();
        for _ in 0..count {
            line.clear();
            reader.read_line(&mut line).ok()?;
            let len: usize = line.strip_prefix('$')?.trim_end().parse().ok()?;
            let mut data = vec![0; len];
            reader.read_exact(&mut data).ok()?;
            let mut crlf = [0; 2];
            reader.read_exact(&mut crlf).ok()?;
            if crlf != *b"\r\n" {
                return None;
            }
            args.push(data)
        }
        Some(args)
    }
    async fn list_nodes_against(peer: &Peer) -> Result<String, crate::cli_nodes::NodeListError> {
        let config = peer.path.with_extension("yaml");
        std::fs::write(
            &config,
            format!(
                "redis:\n  address: '{}'\nlogging:\n  level: error\n",
                peer.path.display()
            ),
        )
        .unwrap();
        let info = lk_config::ProgramInfo {
            binary_name: "livekit-server".into(),
            product_version: "1.13.10".into(),
            compat_version: "1.13.10",
        };
        let args = vec![
            "livekit-server".into(),
            "--config".into(),
            config.as_os_str().to_owned(),
            "list-nodes".into(),
        ];
        let lk_config::EntryAction::ListNodes(cfg) =
            lk_config::parse(args, &lk_config::Env::default(), &info).unwrap()
        else {
            panic!("public list-nodes action")
        };
        let result = crate::cli_nodes::list_nodes(&cfg).await;
        let _ = std::fs::remove_file(&config);
        result
    }
    async fn run(mode: &'static str) -> Peer {
        let peer = Peer::new(mode);
        let result = list_nodes_against(&peer).await;
        assert!(
            result.is_ok(),
            "genuine transient dependency must recover: {result:?}"
        );
        // C41: `list-nodes` builds the message bus (redisbus.New), whose read
        // worker dials a SECOND, dedicated pubsub connection. `spawn_message_bus`
        // is awaited before HVALS, so that connection is already established when
        // list_nodes returns — the accept count is deterministic, not a race.
        if mode == "setup-eof" {
            // 4 command-path dials (three half-closed setup EOFs then the working
            // connection) + 1 message-bus read connection.
            assert_eq!(peer.accepts.load(Ordering::SeqCst), 5)
        } else {
            // The command path reuses one pooled connection across its retries;
            // the message bus adds exactly one more. The bus connection sends no
            // PING/HVALS, so the retry hit count is unchanged at 4.
            assert_eq!(peer.hits.load(Ordering::SeqCst), 4);
            assert_eq!(peer.accepts.load(Ordering::SeqCst), 2)
        }
        peer
    }
    #[tokio::test]
    async fn old_public_recovers_three_real_setup_eofs() {
        run("setup-eof").await;
    }
    #[tokio::test]
    async fn old_public_recovers_three_real_ping_loading_replies() {
        run("ping-loading").await;
    }
    #[tokio::test]
    async fn old_public_recovers_three_real_hvals_tryagain_replies() {
        run("hvals-tryagain").await;
    }

    /// C41 `list-nodes.PSRPCMessageBusSecondRedisConnection`: the second connection
    /// the message-bus read worker dials is a pubsub READ connection — it carries
    /// only the go-redis init handshake (hello + the two `client setinfo` identity
    /// commands in this RESP2 mock) and never a PING, HVALS or SUBSCRIBE, because
    /// `list-nodes` never starts the router. The command connection is the one that
    /// runs PING + HVALS. Removing the bus dial drops accepts to 1 and leaves no
    /// init-only connection, so this assertion is falsifiable.
    #[tokio::test]
    async fn message_bus_read_connection_is_init_only() {
        // "bus-normal": PING and HVALS each succeed on the first reply, so the
        // command connection's trace is the clean init + Ping + HVals sequence.
        let peer = Peer::new("bus-normal");
        let result = list_nodes_against(&peer).await;
        assert!(result.is_ok(), "list-nodes must succeed: {result:?}");
        assert_eq!(peer.accepts.load(Ordering::SeqCst), 2);
        let traces = peer.traces.lock().unwrap().clone();
        assert_eq!(
            traces.len(),
            2,
            "command connection + message-bus connection"
        );
        let mut init_only = Vec::new();
        let mut command = Vec::new();
        for trace in &traces {
            if trace.iter().any(|c| c == "PING" || c == "HVALS") {
                command.push(trace.clone());
            } else {
                init_only.push(trace.clone());
            }
        }
        assert_eq!(command.len(), 1, "exactly one connection runs the commands");
        assert_eq!(
            command[0],
            vec!["HELLO", "CLIENT", "CLIENT", "PING", "HVALS"],
            "command connection: init handshake then Ping + HVals"
        );
        assert_eq!(init_only.len(), 1, "exactly one init-only read connection");
        assert_eq!(
            init_only[0],
            vec!["HELLO", "CLIENT", "CLIENT"],
            "message-bus connection carries only the init handshake, no subscribe/command"
        );
    }
}
