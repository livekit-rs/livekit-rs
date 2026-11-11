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
                    workers.push(thread::spawn(move || {
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
    async fn run(mode: &'static str) {
        let peer = Peer::new(mode);
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
        assert!(
            result.is_ok(),
            "genuine transient dependency must recover: {result:?}"
        );
        if mode == "setup-eof" {
            assert_eq!(peer.accepts.load(Ordering::SeqCst), 4)
        } else {
            assert_eq!(peer.hits.load(Ordering::SeqCst), 4)
        }
    }
    #[tokio::test]
    async fn old_public_recovers_three_real_setup_eofs() {
        run("setup-eof").await
    }
    #[tokio::test]
    async fn old_public_recovers_three_real_ping_loading_replies() {
        run("ping-loading").await
    }
    #[tokio::test]
    async fn old_public_recovers_three_real_hvals_tryagain_replies() {
        run("hvals-tryagain").await
    }
}
