//! Original simple-client initialization and command pool lifecycle.
use super::{
    NodeListError,
    cli_redis_wire::{self as wire, Fault, Socket, Wire},
    redis_address,
};
use std::{
    io,
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::task::JoinHandle;
#[derive(Clone)]
pub(super) struct Options {
    pub username: String,
    pub password: String,
    pub db: i64,
    pub pool_size: i64,
    /// `redis.pool_timeout`: how long a command waits for a free pooled
    /// connection once the pool has saturated (`Pool::dial` below). A zero value
    /// keeps the acquisition immediate, matching go-redis's default no-wait for
    /// a fully failed dial group.
    pub pool_timeout: Duration,
    pub retries: u32,
    pub dial: Option<Duration>,
    pub read: Option<Duration>,
    pub write: Option<Duration>,
    /// `xtls.Config` material for a TLS dial (`redis.use_tls` / `redis.tls.*`).
    /// `None` dials plaintext. The handshake is part of the same go-redis dial,
    /// so it shares `dial`'s deadline and the 5-attempt group below.
    pub tls: Option<crate::redis_tls::SessionTls>,
}
#[derive(Default)]
struct PoolState {
    failures: u32,
    last_error: Option<Fault>,
}
struct Pool {
    address: String,
    options: Options,
    limit: u32,
    state: Arc<Mutex<PoolState>>,
    probe: Option<JoinHandle<()>>,
}
impl Drop for Pool {
    fn drop(&mut self) {
        if let Some(task) = self.probe.take() {
            task.abort()
        }
    }
}
impl Pool {
    fn new(address: String, options: Options) -> Result<Self, NodeListError> {
        let size = options.pool_size;
        if size > i32::MAX as i64 {
            return Err(NodeListError::FatalRedisPool(format!(
                "panic: redis: failed to create connection pool: redis: PoolSize value {size} exceeds maximum allowed value {}",
                i32::MAX
            )));
        }
        if size < i32::MIN as i64 {
            return Err(NodeListError::FatalRedisPool(format!(
                "panic: redis: failed to create connection pool: redis: PoolSize value {size} is below minimum allowed value {}",
                i32::MIN
            )));
        }
        if size < 0 {
            return Err(NodeListError::FatalRedisPool(
                "panic: makechan: size out of range".into(),
            ));
        }
        Ok(Self {
            address,
            options,
            limit: if size == 0 { 10 } else { size as u32 },
            state: Arc::default(),
            probe: None,
        })
    }
    async fn dial(&mut self) -> Result<Wire, Fault> {
        // Acquire a free pooled connection. A saturated pool (every dial in a
        // group failed up to `PoolSize`) waits at most `PoolTimeout` for the
        // background probe to recover before surfacing the cached dial error;
        // with `PoolTimeout` unset the failure is reported immediately.
        let deadline = {
            let state = self.state.lock().unwrap();
            if state.failures >= self.limit {
                if self.options.pool_timeout.is_zero() {
                    return Err(state
                        .last_error
                        .clone()
                        .expect("saturated after real dial group"));
                }
                Some(tokio::time::Instant::now() + self.options.pool_timeout)
            } else {
                None
            }
        };
        if let Some(deadline) = deadline {
            loop {
                let recovered = {
                    let state = self.state.lock().unwrap();
                    if state.failures < self.limit {
                        true
                    } else if tokio::time::Instant::now() >= deadline {
                        return Err(state
                            .last_error
                            .clone()
                            .expect("pool timeout keeps a cached dial error"));
                    } else {
                        false
                    }
                };
                if recovered {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        }
        let mut last = None;
        for attempt in 0..5 {
            let result = dial_once(&self.address, &self.options).await;
            match result {
                Ok(v) => return Ok(v),
                Err(e) => last = Some(e),
            }
            if attempt < 4 {
                tokio::time::sleep(Duration::from_millis(100)).await
            }
        }
        let last = last.expect("five attempts");
        super::cli_redis_retry::pool_diagnostic(&last.bytes());
        let saturated = {
            let mut state = self.state.lock().unwrap();
            state.last_error = Some(last.clone());
            state.failures = state.failures.wrapping_add(1);
            state.failures == self.limit
        };
        if saturated {
            let state = self.state.clone();
            let address = self.address.clone();
            let options = self.options.clone();
            self.probe = Some(tokio::spawn(async move {
                loop {
                    let result = dial_once(&address, &options).await;
                    match result {
                        Ok(wire) => {
                            drop(wire);
                            state.lock().unwrap().failures = 0;
                            return;
                        }
                        Err(e) => {
                            state.lock().unwrap().last_error = Some(e);
                            tokio::time::sleep(Duration::from_secs(1)).await
                        }
                    }
                }
            }));
        }
        Err(last)
    }
}
async fn dial_once(raw: &str, options: &Options) -> Result<Wire, Fault> {
    let address = redis_address::parse(raw).await.map_err(Fault::Protocol)?;
    // go-redis dials with one `ctx`: the TCP connect and the TLS handshake share
    // the `DialTimeout` budget, and a deadline hit during the handshake is the
    // bare `context deadline exceeded` rather than a `dial tcp` timeout.
    let dial_deadline = options
        .dial
        .filter(|d| !d.is_zero())
        .map(|d| tokio::time::Instant::now() + d);
    let connecting = async {
        if options.dial == Some(Duration::ZERO) {
            return Err(io::Error::from(io::ErrorKind::TimedOut));
        }
        let (socket, local, remote): (Socket, String, String) = match &address.connection {
            redis::ConnectionAddr::Tcp(host, port) => {
                let stream = tokio::net::TcpStream::connect((host.as_str(), *port)).await?;
                stream.set_nodelay(true)?;
                let local = stream.local_addr()?.to_string();
                let remote = stream.peer_addr()?.to_string();
                match &options.tls {
                    None => (Box::new(stream), local, remote),
                    Some(tls) => (
                        tls_upgrade(stream, tls, host, dial_deadline).await?,
                        local,
                        remote,
                    ),
                }
            }
            #[cfg(unix)]
            redis::ConnectionAddr::Unix(path) => {
                let stream = tokio::net::UnixStream::connect(path).await?;
                let local=stream.local_addr()?.as_pathname().map(|p|p.to_string_lossy().into_owned()).unwrap_or_default() /* Go UnixAddr of an unnamed client socket is "" */;
                (Box::new(stream), local, address.endpoint.clone())
            }
            _ => {
                return Err(io::Error::new(
                    io::ErrorKind::Unsupported,
                    "unsupported Redis socket",
                ));
            }
        };
        Ok::<_, io::Error>((socket, local, remote))
    };
    let result = if let Some(d) = options.dial {
        match tokio::time::timeout(d, connecting).await {
            Ok(v) => v,
            Err(_) => Err(io::ErrorKind::TimedOut.into()),
        }
    } else {
        connecting.await
    };
    let (socket, local, remote) = result.map_err(|e| {
        // The TLS phase renders its own Go text (see [`GoDialText`]); only a
        // plain TCP failure falls through to the dial renderer below.
        if let Some(text) = e
            .get_ref()
            .and_then(|inner| inner.downcast_ref::<GoDialText>())
        {
            return Fault::Handshake(text.0.clone());
        }
        let text = if e.kind() == io::ErrorKind::TimedOut {
            format!("dial {} {}: i/o timeout", address.network, address.endpoint)
        } else {
            super::address_connection_message(&redis::RedisError::from(e), &address)
        };
        Fault::Protocol(text)
    })?;
    Ok(Wire::new(
        socket,
        address.network,
        local,
        remote,
        options.read,
        options.write,
    ))
}
/// A dial failure whose Go text is already decided — the TLS handshake renders
/// `crypto/tls`/`x509` wording, which the TCP renderer below must not relabel as
/// a `dial tcp …: connect: …` message.
#[derive(Debug)]
struct GoDialText(String);

impl std::fmt::Display for GoDialText {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for GoDialText {}

/// `tls.Client(conn, tlsConfig).HandshakeContext(ctx)`: the handshake runs on the
/// dial's own deadline, so an expired budget is Go's bare
/// `context deadline exceeded` (the TCP connect already succeeded) and a rejected
/// peer certificate is Go's `tls: … x509: …` text.
async fn tls_upgrade(
    stream: tokio::net::TcpStream,
    tls: &crate::redis_tls::SessionTls,
    host: &str,
    deadline: Option<tokio::time::Instant>,
) -> Result<Socket, io::Error> {
    let handshake = tls_connect(stream, tls, host);
    match deadline {
        Some(deadline) => match tokio::time::timeout_at(deadline, handshake).await {
            Ok(result) => result,
            Err(_elapsed) => Err(io::Error::new(
                io::ErrorKind::TimedOut,
                GoDialText("context deadline exceeded".into()),
            )),
        },
        None => handshake.await,
    }
}

async fn tls_connect(
    stream: tokio::net::TcpStream,
    tls: &crate::redis_tls::SessionTls,
    host: &str,
) -> Result<Socket, io::Error> {
    use rtc_tls::rustls::pki_types::ServerName;
    let config = crate::redis_tls::session_client_config(tls)
        .map_err(|text| io::Error::new(io::ErrorKind::InvalidInput, GoDialText(text)))?;
    let name = ServerName::try_from(crate::redis_tls::session_server_name(tls, host))
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, GoDialText(e.to_string())))?;
    let connector = rtc_tls::tokio_rustls::TlsConnector::from(std::sync::Arc::new(config));
    connector
        .connect(name, stream)
        .await
        .map(|tls| Box::new(tls) as Socket)
        .map_err(|e| {
            io::Error::new(
                e.kind(),
                GoDialText(crate::redis_tls::go_handshake_text(&e)),
            )
        })
}

pub(super) struct Session {
    pool: Pool,
    current: Option<Wire>,
    maint: bool,
}
/// Reference-build identity metadata, not a product capability.
///
/// `LIB-NAME`/`LIB-VER` are the two strings a Redis server sees when go-redis
/// introduces itself (`commands.go:330-341`, sent unless identity is disabled —
/// `redis.go:991-1001` builds the pair). They describe the *library and toolchain
/// the reference binary was built with*, so they are recorded separately from the
/// product version being matched:
///
/// * product source version: LiveKit Server v1.13.10 (the locked reference source
///   tree, `references/livekit-stable`, `references/protocol-stable`);
/// * library: go-redis v9.22.0, pinned by that source (`LIB_VERSION`);
/// * Go toolchain of the locked reference **executable**: `go1.26.0`, observed on
///   the wire and recorded in
///   `livekit-rs-docs/evidence/2026-11-04/c18-redis-command-casing-paired/tap-connections.json`
///   (`LIB_NAME`; `commands.go:338` renders `go-redis(<suffix>,<runtime.Version()>)`
///   and upstream sets no suffix).
///
/// Scope: this pair is only an identity advertisement on the CLI Redis dial. The
/// functional contract of the same handshake is the RESP command *shape* — lowercase
/// command names, uppercase `LIB-*` tokens — which is what a server can branch on;
/// the tap compares the packed bytes, so a different reference build forces a
/// re-pin instead of silent drift. Nothing else in the product depends on these two
/// strings, and accepting them is not evidence for any other Redis behaviour.
const LIB_VERSION: &str = "9.22.0";
const LIB_NAME: &str = "go-redis(,go1.26.0)";

/// The connection-initialisation commands, built exactly as go-redis v9.22.0
/// builds them: every command name is the lowercase literal from the library's
/// command builder, and only the tokens that library writes in upper case
/// (`LIB-NAME`, `LIB-VER`) are uppercase. A RESP tap is the only surface that can
/// see this, so the builders are one function each and unit-tested against the
/// packed bytes rather than inferred from the flow.
///
/// `commands.go:360 Hello` → `hello 3 [auth <user|default> <password>] [setname <name>]`.
fn hello_command(options: &Options) -> redis::Cmd {
    let mut hello = wire::command("hello", &["3"]);
    if !options.password.is_empty() {
        hello
            .arg("auth")
            .arg(if options.username.is_empty() {
                "default"
            } else {
                &options.username
            })
            .arg(&options.password);
    }
    hello
}

/// `commands.go:282/291 Auth` — the fallback when the server rejects RESP3.
fn auth_command(options: &Options) -> redis::Cmd {
    if options.username.is_empty() {
        wire::command("auth", &[options.password.as_str()])
    } else {
        wire::command(
            "auth",
            &[options.username.as_str(), options.password.as_str()],
        )
    }
}

/// `commands.go:311 Select`.
fn select_command(db: i64) -> redis::Cmd {
    wire::command("select", &[&db.to_string()])
}

/// `commands.go:556 ClientMaintNotifications` →
/// `client maint_notifications on moving-endpoint-type <type>`.
fn maint_notifications_command(endpoint: &str) -> redis::Cmd {
    wire::command(
        "client",
        &[
            "maint_notifications",
            "on",
            "moving-endpoint-type",
            endpoint,
        ],
    )
}

/// `commands.go:330 ClientSetInfo`, sent as the two-command pipeline
/// `redis.go:991-1002` builds when identity is not disabled.
fn identity_commands() -> Vec<redis::Cmd> {
    vec![
        wire::command("client", &["setinfo", "LIB-NAME", LIB_NAME]),
        wire::command("client", &["setinfo", "LIB-VER", LIB_VERSION]),
    ]
}

impl Session {
    pub fn new(address: String, options: Options) -> Result<Self, NodeListError> {
        Ok(Self {
            pool: Pool::new(address, options)?,
            current: None,
            maint: true,
        })
    }
    async fn open(&mut self) -> Result<Wire, (Fault, bool)> {
        let mut wire = self.pool.dial().await.map_err(|e| (e, true))?;
        let options = &self.pool.options;
        let hello = hello_command(options);
        let mut sticky = None;
        let resp3 =
            match handshake_command(&mut wire, hello, self.pool.options.retries, &mut sticky).await
            {
                Ok(redis::Value::Map(_)) => true,
                Ok(redis::Value::Array(values)) if values.len() % 2 == 0 => true,
                Ok(_) => {
                    return Err((
                        Fault::Protocol("redis: can't parse map reply".into()),
                        false,
                    ));
                }
                Err(error) if error.server() => {
                    self.maint = false;
                    if !options.password.is_empty() {
                        handshake_command(
                            &mut wire,
                            auth_command(options),
                            options.retries,
                            &mut sticky,
                        )
                        .await
                        .map_err(|e| (e.prefix("failed to authenticate: "), false))?;
                    }
                    false
                }
                Err(error) => return Err((error, false)),
            };
        wire.allow_push = resp3;
        if options.db > 0 {
            handshake_pipeline(
                &mut wire,
                vec![select_command(options.db)],
                options.retries,
                &mut sticky,
            )
            .await
            .map_err(|e| (e.prefix("failed to initialize connection options: "), false))?;
        }
        if self.maint && resp3 {
            let endpoint = endpoint_type(&self.pool.address, false).await;
            match handshake_command(
                &mut wire,
                maint_notifications_command(&endpoint),
                options.retries,
                &mut sticky,
            )
            .await
            {
                Ok(_) => {}
                Err(e) if e.server() => self.maint = false,
                Err(e) => return Err((e, false)),
            }
        }
        match handshake_pipeline(&mut wire, identity_commands(), options.retries, &mut sticky).await
        {
            Ok(_) => {}
            Err(e) if e.server() => {}
            Err(e) => return Err((e, false)),
        }
        Ok(wire)
    }
    pub async fn query(&mut self, cmd: redis::Cmd) -> Result<redis::Value, Fault> {
        let mut last = None;
        for attempt in 0..=self.pool.options.retries {
            if attempt > 0 {
                tokio::time::sleep(super::cli_redis_retry::command_backoff(attempt)).await;
            }
            if self.current.as_ref().is_some_and(|wire| !wire.healthy()) {
                self.current = None;
            }
            if self.current.is_none() {
                match self.open().await {
                    Ok(wire) => self.current = Some(wire),
                    Err((error, dial)) => {
                        let error = if dial { error } else { error.unwrap_once() };
                        // A TCP-connect failure is go-redis' `*net.OpError{Op: "dial"}`,
                        // which `shouldRetry` retries whatever the command timeout says.
                        // A handshake failure is not: it arrives as bare `context`/x509
                        // text, so the command is abandoned after that one dial group.
                        let retry = if dial {
                            !error.handshake()
                        } else {
                            error.retry(false)
                        };
                        last = Some(error);
                        if retry { continue } else { break }
                    }
                }
            }
            // Take ownership across await: cancellation drops the partially read socket
            // rather than returning its unread response tail to a later command.
            let mut wire = self.current.take().unwrap();
            match wire.query(cmd.clone()).await {
                Ok(v) => {
                    self.current = Some(wire);
                    return Ok(v);
                }
                Err(error) => {
                    let retry = error.retry(true);
                    if !error.bad_connection() {
                        self.current = Some(wire);
                    }
                    last = Some(error);
                    if !retry {
                        break;
                    }
                }
            }
        }
        Err(last.expect("failed actual command attempt"))
    }
}
async fn endpoint_type(address: &str, tls: bool) -> String {
    let host = match redis_address::parse(address).await {
        Ok(a) => match a.connection {
            redis::ConnectionAddr::Tcp(host, _) => host,
            _ => address.to_owned(),
        },
        Err(_) => address.to_owned(),
    };
    let internal = if host.is_empty() {
        true
    } else if let Ok(ip) = host.parse::<std::net::IpAddr>() {
        private_ip(ip)
    } else {
        match tokio::time::timeout(
            Duration::from_secs(2),
            tokio::net::lookup_host((host.as_str(), 0)),
        )
        .await
        {
            Ok(Ok(addresses)) => {
                let mut found = false;
                let mut all = true;
                for a in addresses {
                    found = true;
                    all &= private_ip(a.ip())
                }
                found && all
            }
            _ => false,
        }
    };
    format!(
        "{}-{}",
        if internal { "internal" } else { "external" },
        if tls { "fqdn" } else { "ip" }
    )
}
fn private_ip(ip: std::net::IpAddr) -> bool {
    match ip {
        std::net::IpAddr::V4(v) => {
            v.is_private()
                || v.is_loopback()
                || v.is_link_local()
                || (u32::from(v) & 0xffc0_0000 == 0x6440_0000)
        }
        std::net::IpAddr::V6(v) => v.to_ipv4_mapped().map_or(
            v.is_loopback() || v.is_unique_local() || v.is_unicast_link_local(),
            |v| private_ip(v.into()),
        ),
    }
}

async fn handshake_command(
    wire: &mut Wire,
    cmd: redis::Cmd,
    retries: u32,
    sticky: &mut Option<Fault>,
) -> Result<redis::Value, Fault> {
    let mut last = None;
    for attempt in 0..=retries {
        if attempt > 0 {
            tokio::time::sleep(super::cli_redis_retry::command_backoff(attempt)).await;
        }
        let result = if let Some(error) = sticky.as_ref() {
            Err(error.clone())
        } else {
            wire.query(cmd.clone()).await
        };
        match result {
            Ok(value) => return Ok(value),
            Err(error) => {
                let retry = error.retry(true);
                if error.bad_connection() {
                    *sticky = Some(error.clone())
                }
                last = Some(error);
                if !retry {
                    break;
                }
            }
        }
    }
    Err(last.expect("actual or sticky handshake command error"))
}
async fn handshake_pipeline(
    wire: &mut Wire,
    commands: Vec<redis::Cmd>,
    retries: u32,
    sticky: &mut Option<Fault>,
) -> Result<Vec<redis::Value>, Fault> {
    let mut last = None;
    for attempt in 0..=retries {
        if attempt > 0 {
            tokio::time::sleep(super::cli_redis_retry::command_backoff(attempt)).await;
        }
        let result = if let Some(error) = sticky.as_ref() {
            Err(error.clone())
        } else {
            wire.pipeline(commands.clone()).await
        };
        match result {
            Ok(values) => return Ok(values),
            Err(error) => {
                let retry = (!error.server() || wire.pipeline_retry_first) && error.retry(true);
                if error.bad_connection() {
                    *sticky = Some(error.clone())
                }
                last = Some(error);
                if !retry {
                    break;
                }
            }
        }
    }
    Err(last.expect("actual or sticky handshake pipeline error"))
}

#[cfg(test)]
#[path = "cli_redis_session_tests.rs"]
mod tests;
