//! Source-phase RESP I/O: no background driver, so idle EOF is read lazily like
//! go-redis and a write failure cannot be relabeled as an earlier synthetic EOF.
use redis::Value;
use std::{io, time::Duration};
use tokio::io::{AsyncRead, AsyncWrite, AsyncWriteExt};
pub(super) trait Stream: AsyncRead + AsyncWrite + Unpin + Send {
    #[cfg(unix)]
    fn raw_fd(&self) -> Option<std::os::fd::RawFd> {
        None
    }
}
impl Stream for tokio::net::TcpStream {
    #[cfg(unix)]
    fn raw_fd(&self) -> Option<std::os::fd::RawFd> {
        use std::os::fd::AsRawFd;
        Some(self.as_raw_fd())
    }
}
#[cfg(unix)]
impl Stream for tokio::net::UnixStream {
    fn raw_fd(&self) -> Option<std::os::fd::RawFd> {
        use std::os::fd::AsRawFd;
        Some(self.as_raw_fd())
    }
}
// Match connCheck's outer-connection policy: never unwrap TLS encrypted records.
// A `redis.tls` session therefore reports no raw fd, exactly as Go's connCheck on
// a `*tls.Conn` inspects the outer connection instead of the plaintext one.
impl Stream for rtc_tls::tokio_rustls::client::TlsStream<tokio::net::TcpStream> {}

pub(super) type Socket = Box<dyn Stream>;
#[derive(Clone, Debug)]
pub(super) enum Fault {
    Server(Vec<u8>),
    Io {
        kind: io::ErrorKind,
        message: String,
        inner: Option<String>,
    },
    Protocol(String),
    /// A dial failure reported by the TLS handshake instead of the TCP connect.
    /// Go renders it as `crypto/tls`/`x509`/`context` text that is not a
    /// `*net.OpError{Op: "dial"}`, which is the only dial shape go-redis'
    /// `shouldRetry` retries a command for.
    Handshake(String),
    Wrapped {
        prefix: String,
        source: Box<Fault>,
    },
}
impl Fault {
    pub fn text(&self) -> String {
        match self {
            Self::Server(s) => String::from_utf8_lossy(s).into_owned(),
            Self::Wrapped { prefix, source } => format!("{prefix}{}", source.text()),
            Self::Protocol(s) => s.clone(),
            Self::Handshake(s) => s.clone(),
            Self::Io { message, .. } => message.clone(),
        }
    }
    pub fn bytes(&self) -> Vec<u8> {
        match self {
            Self::Server(s) => s.clone(),
            Self::Wrapped { prefix, source } => {
                let mut bytes = prefix.as_bytes().to_vec();
                bytes.extend(source.bytes());
                bytes
            }
            _ => self.text().into_bytes(),
        }
    }
    pub fn server(&self) -> bool {
        match self {
            Self::Server(_) => true,
            Self::Wrapped { source, .. } => source.server(),
            _ => false,
        }
    }
    /// Whether the dial reached the TLS handshake, so go-redis' command-level
    /// retry rule is `shouldRetry(err, false)` on the bare error rather than the
    /// unconditional retry a `*net.OpError{Op: "dial"}` gets.
    pub fn handshake(&self) -> bool {
        match self {
            Self::Handshake(_) => true,
            Self::Wrapped { source, .. } => source.handshake(),
            _ => false,
        }
    }
    pub fn retry(&self, retry_timeout: bool) -> bool {
        match self {
            Self::Wrapped { source, .. } => source.retry(retry_timeout),
            Self::Io {
                kind: io::ErrorKind::UnexpectedEof,
                ..
            } => true,
            Self::Io {
                kind: io::ErrorKind::TimedOut | io::ErrorKind::WouldBlock,
                ..
            } => retry_timeout,
            // `Wire::io` only reports socket read/write failures, and Go's
            // `shouldRetry` treats any non-timeout `net.Error` (ECONNRESET, EPIPE,
            // …) as retryable, independently of the command timeout flag.
            Self::Io { .. } => true,
            Self::Server(raw) => {
                let s = String::from_utf8_lossy(raw);
                [
                    "ERR max number of clients reached",
                    "LOADING ",
                    "READONLY ",
                    "CLUSTERDOWN ",
                    "TRYAGAIN ",
                    "MASTERDOWN ",
                    "NOREPLICAS ",
                ]
                .iter()
                .any(|prefix| s.starts_with(prefix))
                    || s.contains("-READONLY You can't write against a read only replica")
            }
            _ => false,
        }
    }
    pub fn bad_connection(&self) -> bool {
        match self {
            Self::Wrapped { source, .. } => source.bad_connection(),
            _ => {
                !self.server()
                    || self.text().starts_with("READONLY ")
                    || self
                        .text()
                        .contains("-READONLY You can't write against a read only replica")
            }
        }
    }
    // initPooledConn unwraps exactly one layer; PubSub initConn does not.
    pub fn unwrap_once(self) -> Self {
        match self {
            Self::Wrapped { source, .. } => *source,
            Self::Io {
                kind,
                inner: Some(message),
                ..
            } => Self::Io {
                kind,
                message,
                inner: None,
            },
            other => other,
        }
    }
    pub fn prefix(self, prefix: &str) -> Self {
        Self::Wrapped {
            prefix: prefix.into(),
            source: Box::new(self),
        }
    }
}
pub(super) struct Wire {
    stream: Socket,
    pub network: &'static str,
    pub local: String,
    pub remote: String,
    pub read: Option<Duration>,
    pub write: Option<Duration>,
    pub allow_push: bool,
    pub pipeline_retry_first: bool,
}
impl Wire {
    pub fn new(
        stream: Socket,
        network: &'static str,
        local: String,
        remote: String,
        read: Option<Duration>,
        write: Option<Duration>,
    ) -> Self {
        Self {
            stream,
            network,
            local,
            remote,
            read,
            write,
            allow_push: false,
            pipeline_retry_first: true,
        }
    }
    pub fn healthy(&self) -> bool {
        #[cfg(unix)]
        if let Some(fd) = self.stream.raw_fd() {
            let mut byte = 0u8;
            let count = unsafe {
                libc::recv(
                    fd,
                    (&mut byte as *mut u8).cast(),
                    1,
                    libc::MSG_PEEK | libc::MSG_DONTWAIT,
                )
            };
            if count == 0 {
                return false;
            }
            if count > 0 {
                return self.allow_push && byte == b'>';
            }
            return io::Error::last_os_error().kind() == io::ErrorKind::WouldBlock;
        }
        true
    }
    fn io(&self, error: io::Error, stage: &str) -> Fault {
        let kind = error.kind();
        if kind == io::ErrorKind::UnexpectedEof {
            return Fault::Io {
                kind,
                message: "EOF".into(),
                inner: None,
            };
        }
        let cause = match kind {
            io::ErrorKind::TimedOut | io::ErrorKind::WouldBlock => "i/o timeout".into(),
            io::ErrorKind::BrokenPipe => "broken pipe".into(),
            io::ErrorKind::ConnectionReset => "connection reset by peer".into(),
            _ => error.to_string(),
        };
        // net.OpError wraps the syscall error for EPIPE/ECONNRESET, while
        // deadline errors have no syscall prefix. Preserve the one-level boundary.
        let inner = if error.raw_os_error().is_some() {
            format!("{stage}: {cause}")
        } else {
            cause
        };
        Fault::Io {
            kind,
            message: format!(
                "{stage} {} {}->{}: {inner}",
                self.network, self.local, self.remote
            ),
            inner: Some(inner),
        }
    }
    pub async fn send_bytes(&mut self, commands: &[redis::Cmd]) -> Result<(), Fault> {
        let mut packed = Vec::new();
        for cmd in commands {
            packed.extend(cmd.get_packed_command());
        }
        let limit = self.write;
        let sending = async {
            self.stream.write_all(&packed).await?;
            self.stream.flush().await
        };
        let result = if let Some(d) = limit {
            match tokio::time::timeout(d, sending).await {
                Ok(v) => v,
                Err(_) => Err(io::ErrorKind::TimedOut.into()),
            }
        } else {
            sending.await
        };
        result.map_err(|e| self.io(e, "write"))
    }
    fn read_error(&self, error: super::cli_redis_resp::Error) -> Fault {
        use super::cli_redis_resp::Error;
        match error {
            Error::Io(error) => self.io(error, "read"),
            Error::Server(bytes) => Fault::Server(bytes),
            Error::Nil => Fault::Server(b"redis: nil".to_vec()),
            Error::Eof => Fault::Io {
                kind: io::ErrorKind::UnexpectedEof,
                message: "EOF".into(),
                inner: None,
            },
            Error::UnexpectedEof => Fault::Io {
                kind: io::ErrorKind::UnexpectedEof,
                message: "unexpected EOF".into(),
                inner: None,
            },
            Error::Protocol(text) => Fault::Protocol(text),
        }
    }
    async fn cohort(&mut self, commands: &[redis::Cmd]) -> Result<Vec<Value>, Fault> {
        let mut reader = super::cli_redis_resp::Reader::new(&mut self.stream);
        let mut first = None;
        let mut first_index = None;
        let mut result = Vec::new();
        for (index, cmd) in commands.iter().enumerate() {
            match reader.command(cmd).await {
                Ok(value) => result.push(value),
                Err(
                    e @ (super::cli_redis_resp::Error::Server(_)
                    | super::cli_redis_resp::Error::Nil),
                ) => {
                    if first.is_none() {
                        first = Some(e);
                        first_index = Some(index)
                    }
                    result.push(Value::Nil)
                }
                Err(error) => {
                    drop(reader);
                    return Err(self.read_error(error));
                }
            }
        }
        drop(reader);
        self.pipeline_retry_first = first_index == Some(0);
        if let Some(error) = first {
            Err(self.read_error(error))
        } else {
            Ok(result)
        }
    }
    async fn read_cohort(&mut self, commands: &[redis::Cmd]) -> Result<Vec<Value>, Fault> {
        let limit = self.read;
        if let Some(d) = limit {
            match tokio::time::timeout(d, self.cohort(commands)).await {
                Ok(v) => v,
                Err(_) => Err(self.io(io::ErrorKind::TimedOut.into(), "read")),
            }
        } else {
            self.cohort(commands).await
        }
    }
    pub async fn query(&mut self, cmd: redis::Cmd) -> Result<Value, Fault> {
        self.send_bytes(std::slice::from_ref(&cmd)).await?;
        Ok(self.read_cohort(&[cmd]).await?.remove(0))
    }
    pub async fn pipeline(&mut self, commands: Vec<redis::Cmd>) -> Result<Vec<Value>, Fault> {
        self.pipeline_retry_first = true;
        if commands.is_empty() {
            return Ok(Vec::new());
        }
        self.send_bytes(&commands).await?;
        // One source WithReader deadline covers the entire pipeline cohort, including
        // preceding push frames. Read all server-error replies before returning first.
        self.read_cohort(&commands).await
    }
}
/// A RESP command as go-redis writes it: the command name is always the lowercase
/// literal from the library's command builders (`hello`, `client`, `select`),
/// whatever case the call site spells it in. Sub-command *arguments* keep the case
/// their builder uses (`setinfo` is lowercase, `LIB-NAME` is not) — see
/// `references/go-modules/go-redis-v9.22.0/commands.go:339,364`.
pub(super) fn command(name: &str, args: &[&str]) -> redis::Cmd {
    let mut cmd = redis::cmd(&name.to_lowercase());
    for arg in args {
        cmd.arg(arg);
    }
    cmd
}

#[cfg(test)]
#[path = "cli_redis_wire_tests.rs"]
mod tests;
