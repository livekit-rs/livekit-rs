//! Typed Go RESP-reader boundaries for the actual CLI command set. A wrong
//! status/map tag fails at its header; it never waits for an irrelevant body.
use super::cli_redis_wire::Socket;
use redis::Value;
use std::{collections::VecDeque, future::Future, io, pin::Pin};
use tokio::io::AsyncReadExt;
#[derive(Debug)]
pub(super) enum Error {
    Io(io::Error),
    Server(Vec<u8>),
    Protocol(String),
    Nil,
    Eof,
    UnexpectedEof,
}
impl From<io::Error> for Error {
    fn from(e: io::Error) -> Self {
        Self::Io(e)
    }
}
type Result<T> = std::result::Result<T, Error>;
pub(super) struct Reader<'a> {
    stream: &'a mut Socket,
    pending: Option<Vec<u8>>,
    replay: VecDeque<u8>,
}
impl<'a> Reader<'a> {
    pub fn new(stream: &'a mut Socket) -> Self {
        Self {
            stream,
            pending: None,
            replay: VecDeque::new(),
        }
    }
    async fn byte(&mut self) -> Result<u8> {
        if let Some(byte) = self.replay.pop_front() {
            return Ok(byte);
        }
        let mut byte = [0];
        if self.stream.read(&mut byte).await? == 0 {
            return Err(Error::Eof);
        }
        Ok(byte[0])
    }
    async fn full(&mut self, bytes: &mut [u8]) -> Result<()> {
        let mut read = bytes.len().min(self.replay.len());
        for (dst, src) in bytes.iter_mut().zip(self.replay.drain(..read)) {
            *dst = src;
        }
        while read < bytes.len() {
            let count = self.stream.read(&mut bytes[read..]).await?;
            if count == 0 {
                return Err(if read == 0 {
                    Error::Eof
                } else {
                    Error::UnexpectedEof
                });
            }
            read += count;
        }
        Ok(())
    }
    async fn raw_line(&mut self) -> Result<Vec<u8>> {
        let first = self.byte().await?;
        self.line_from(first).await
    }
    async fn line_from(&mut self, first: u8) -> Result<Vec<u8>> {
        let mut line = Vec::new();
        let mut byte = first;
        loop {
            line.push(byte);
            if byte == b'\n' {
                if line.len() <= 2 || !line.ends_with(b"\r\n") {
                    return Err(Error::Protocol(format!(
                        "redis: invalid reply: {}",
                        super::cli_redis_quote::quote_bytes(&line)
                    )));
                }
                line.truncate(line.len() - 2);
                return Ok(line);
            }
            byte = self.byte().await?;
        }
    }
    fn line<'b>(&'b mut self) -> Pin<Box<dyn Future<Output = Result<Vec<u8>>> + Send + 'b>> {
        Box::pin(async move {
            loop {
                let line = if let Some(line) = self.pending.take() {
                    line
                } else {
                    self.raw_line().await?
                };
                match line[0] {
                    b'-' => return Err(Error::Server(line[1..].to_vec())),
                    b'_' => return Err(Error::Nil),
                    b'!' => return Err(Error::Server(self.bulk(&line).await?)),
                    b'|' => {
                        let count = length(&line)?;
                        for _ in 0..count {
                            self.discard().await?;
                            self.discard().await?;
                        }
                    }
                    _ if line == b"$-1" || line == b"*-1" => return Err(Error::Nil),
                    _ => return Ok(line),
                }
            }
        })
    }
    fn discard<'b>(&'b mut self) -> Pin<Box<dyn Future<Output = Result<()>> + Send + 'b>> {
        Box::pin(async move {
            let line = self.raw_line().await?;
            match line[0] {
                b'$' | b'=' | b'!' => match self.bulk(&line).await {
                    Ok(_) | Err(Error::Nil) => {}
                    Err(error) => return Err(error),
                },
                b'*' | b'~' | b'>' => {
                    let count = match length(&line) {
                        Ok(n) => n,
                        Err(Error::Nil) => 0,
                        Err(error) => return Err(error),
                    };
                    for _ in 0..count {
                        self.discard().await?
                    }
                }
                b'%' | b'|' => {
                    let count = match length(&line) {
                        Ok(n) => n,
                        Err(Error::Nil) => 0,
                        Err(error) => return Err(error),
                    };
                    for _ in 0..count {
                        self.discard().await?;
                        self.discard().await?
                    }
                }
                _ => {}
            }
            Ok(())
        })
    }
    async fn bulk(&mut self, line: &[u8]) -> Result<Vec<u8>> {
        let size = length(line)?;
        let mut bytes = vec![
            0;
            size.checked_add(2).ok_or_else(|| Error::Protocol(
                "redis: invalid reply length".into()
            ))?
        ];
        self.full(&mut bytes).await?;
        bytes.truncate(size);
        Ok(bytes)
    }
    pub async fn string(&mut self) -> Result<Vec<u8>> {
        let line = self.line().await?;
        match line[0] {
            b'+' | b':' | b',' => Ok(line[1..].to_vec()),
            b'$' => self.bulk(&line).await,
            b'#' => match &line[1..] {
                b"t" => Ok(b"true".to_vec()),
                b"f" => Ok(b"false".to_vec()),
                _ => Err(Error::Protocol(format!(
                    "redis: can't parse bool reply: {}",
                    quote(&line)
                ))),
            },
            b'=' => {
                let body = self.bulk(&line).await?;
                if body.len() < 4 || body[3] != b':' {
                    return Err(Error::Protocol(format!(
                        "redis: can't parse verbatim string reply: {}",
                        super::cli_redis_quote::quote_bytes(&line)
                    )));
                }
                Ok(body[4..].to_vec())
            }
            b'(' => canonical_integer(&line),
            _ => Err(Error::Protocol(format!(
                "redis: can't parse reply={} reading string",
                quote(&line)
            ))),
        }
    }
    pub async fn array_len(&mut self) -> Result<usize> {
        let line = self.line().await?;
        if matches!(line[0], b'*' | b'~' | b'>') {
            length(&line)
        } else {
            Err(Error::Protocol(format!(
                "redis: can't parse array/set/push reply: {}",
                quote(&line)
            )))
        }
    }
    pub async fn map_len(&mut self) -> Result<usize> {
        let line = self.line().await?;
        match line[0] {
            b'%' => length(&line),
            b'*' | b'~' | b'>' => {
                let count = length(&line)?;
                if count % 2 != 0 {
                    return Err(Error::Protocol(format!(
                        "redis: the length of the array must be a multiple of 2, got: {count}"
                    )));
                }
                Ok(count / 2)
            }
            _ => Err(Error::Protocol(format!(
                "redis: can't parse map reply: {}",
                quote(&line)
            ))),
        }
    }
    pub fn any<'b>(&'b mut self) -> Pin<Box<dyn Future<Output = Result<Value>> + Send + 'b>> {
        Box::pin(async move {
            let line = self.line().await?;
            match line[0] {
                b'+' => Ok(Value::BulkString(line[1..].to_vec())),
                b'$' => Ok(Value::BulkString(self.bulk(&line).await?)),
                b':' => Ok(Value::Int(decimal(&line[1..])?)),
                b',' => {
                    let text = std::str::from_utf8(&line[1..])
                        .map_err(|_| Error::Protocol("redis: invalid float".into()))?;
                    let value = match text {
                        "inf" => f64::INFINITY,
                        "-inf" => f64::NEG_INFINITY,
                        "nan" | "-nan" => f64::NAN,
                        _ => text
                            .parse::<f64>()
                            .map_err(|_| Error::Protocol("redis: invalid float".into()))?,
                    };
                    Ok(Value::Double(value))
                }
                b'#' => match &line[1..] {
                    b"t" => Ok(Value::Boolean(true)),
                    b"f" => Ok(Value::Boolean(false)),
                    _ => Err(Error::Protocol(format!(
                        "redis: can't parse bool reply: {}",
                        super::cli_redis_quote::quote_bytes(&line)
                    ))),
                },
                b'(' => Ok(Value::BulkString(canonical_integer(&line)?)),
                b'=' => {
                    let body = self.bulk(&line).await?;
                    if body.len() < 4 || body[3] != b':' {
                        return Err(Error::Protocol(format!(
                            "redis: can't parse verbatim string reply: {}",
                            super::cli_redis_quote::quote_bytes(&line)
                        )));
                    }
                    Ok(Value::BulkString(body[4..].to_vec()))
                }
                b'*' | b'~' | b'>' => {
                    let count = length(&line)?;
                    let mut values = Vec::with_capacity(count);
                    for _ in 0..count {
                        match self.any().await {
                            Ok(v) => values.push(v),
                            Err(Error::Nil) => values.push(Value::Nil),
                            Err(Error::Server(v)) if !typed_error(&v) => {
                                values.push(server_value(&v)?)
                            }
                            Err(e) => return Err(e),
                        }
                    }
                    Ok(Value::Array(values))
                }
                b'%' => {
                    let count = length(&line)?;
                    let mut values = Vec::with_capacity(count);
                    for _ in 0..count {
                        let key = self.any().await?;
                        match key{Value::Array(_)=>return Err(Error::Protocol("redis: RESP3 map key must be a scalar type, got []interface {}".into())),Value::Map(_)=>return Err(Error::Protocol("redis: RESP3 map key must be a scalar type, got map[interface {}]interface {}".into())),_=>{}}
                        let value = match self.any().await {
                            Ok(v) => v,
                            Err(Error::Nil) => Value::Nil,
                            Err(Error::Server(v)) if !typed_error(&v) => server_value(&v)?,
                            Err(e) => return Err(e),
                        };
                        values.push((key, value))
                    }
                    Ok(Value::Map(values))
                }
                _ => Err(Error::Protocol(format!(
                    "redis: can't parse {}",
                    quote(&line)
                ))),
            }
        })
    }
    async fn strings(&mut self) -> Result<Value> {
        let count = self.array_len().await?;
        let mut values = Vec::with_capacity(count);
        for _ in 0..count {
            match self.string().await {
                Ok(value) => values.push(Value::BulkString(value)),
                Err(Error::Nil) => values.push(Value::BulkString(Vec::new())),
                Err(error) => return Err(error),
            }
        }
        Ok(Value::Array(values))
    }
    async fn hello(&mut self) -> Result<Value> {
        let count = self.map_len().await?;
        let mut values = Vec::with_capacity(count);
        for _ in 0..count {
            let key = Value::BulkString(self.string().await?);
            let value = match self.any().await {
                Ok(v) => v,
                Err(Error::Nil) => Value::Nil,
                Err(Error::Server(v)) if !typed_error(&v) => server_value(&v)?,
                Err(error) => return Err(error),
            };
            values.push((key, value));
        }
        Ok(Value::Map(values))
    }
    async fn command_owns_push(&mut self, header: &[u8]) -> bool {
        let digits = &header[1..];
        if digits.is_empty() || !digits.iter().all(u8::is_ascii_digit) {
            return true;
        }

        // go-redis peeks at most 4096 bytes and leaves Pub/Sub pushes for
        // the command reader. Replay the inspected bytes for typed decoding.
        let limit = 4096usize.saturating_sub(header.len() + 2);
        let mut prefix = Vec::new();
        let owned = self.push_name(&mut prefix, limit).await.unwrap_or(true);
        prefix.extend(self.replay.drain(..));
        self.replay = prefix.into();
        owned
    }
    async fn push_name(&mut self, prefix: &mut Vec<u8>, limit: usize) -> Option<bool> {
        let name = loop {
            if prefix.len() == limit {
                return Some(false);
            }
            let byte = self.byte().await.ok()?;
            prefix.push(byte);
            if !matches!(prefix[0], b'+' | b'$') {
                return None;
            }
            if let Some(line) = prefix.strip_suffix(b"\r\n") {
                break &line[1..];
            }
        };
        let name = if prefix[0] == b'$' {
            if name.is_empty() || !name.iter().all(u8::is_ascii_digit) {
                return None;
            }
            let length = std::str::from_utf8(name).ok()?.parse::<usize>().ok()?;
            let start = prefix.len();
            if length > limit - start {
                return Some(false);
            }
            for _ in 0..length {
                prefix.push(self.byte().await.ok()?);
            }
            &prefix[start..]
        } else {
            name
        };
        Some(matches!(
            name,
            b"message"
                | b"pmessage"
                | b"subscribe"
                | b"unsubscribe"
                | b"psubscribe"
                | b"punsubscribe"
                | b"smessage"
                | b"ssubscribe"
                | b"sunsubscribe"
        ))
    }
    pub async fn command(&mut self, cmd: &redis::Cmd) -> Result<Value> {
        loop {
            let first = match self.byte().await {
                // The go-redis RESP3 push probe consumes a socket read error
                // from bufio.Peek before the command reads the reply. A reset
                // here can leave EOF for that read. Preserve errors mid-frame.
                Err(Error::Io(error)) if error.kind() == io::ErrorKind::ConnectionReset => {
                    self.byte().await?
                }
                result => result?,
            };
            let line = self.line_from(first).await?;
            let discard_push = line[0] == b'>' && !self.command_owns_push(&line).await;
            self.pending = Some(line);
            if discard_push {
                self.any().await?;
            } else {
                break;
            }
        }
        let args = cmd
            .args_iter()
            .filter_map(|arg| match arg {
                redis::Arg::Simple(bytes) => Some(bytes.to_ascii_uppercase()),
                _ => None,
            })
            .collect::<Vec<_>>();
        let name = args.first().map(Vec::as_slice).unwrap_or_default();
        match name {
            b"HELLO" => self.hello().await,
            b"PING" | b"AUTH" | b"SELECT" | b"CLIENT" | b"ASKING" => {
                Ok(Value::BulkString(self.string().await?))
            }
            b"HVALS" => self.strings().await,
            _ => self.any().await,
        }
    }
}
fn length(line: &[u8]) -> Result<usize> {
    let value = decimal(&line[1..]).map_err(|error| match error {
        Error::Protocol(message) => {
            Error::Protocol(message.replacen("strconv.ParseInt:", "strconv.Atoi:", 1))
        }
        other => other,
    })?;
    if value == -1 {
        return Err(Error::Nil);
    }
    usize::try_from(value).map_err(|_| {
        Error::Protocol(format!(
            "redis: invalid reply: {}",
            super::cli_redis_quote::quote_bytes(line)
        ))
    })
}
fn decimal(bytes: &[u8]) -> Result<i64> {
    let text = std::str::from_utf8(bytes).unwrap_or("");
    text.parse::<i64>().map_err(|e| {
        let reason = if matches!(
            e.kind(),
            std::num::IntErrorKind::PosOverflow | std::num::IntErrorKind::NegOverflow
        ) {
            "value out of range"
        } else {
            "invalid syntax"
        };
        Error::Protocol(format!(
            "strconv.ParseInt: parsing {}: {reason}",
            super::cli_redis_quote::quote_bytes(bytes)
        ))
    })
}
fn canonical_integer(line: &[u8]) -> Result<Vec<u8>> {
    let bytes = &line[1..];
    let (mut negative, mut value) = (false, bytes);
    if let Some(first) = value.first()
        && (*first == b'+' || *first == b'-')
    {
        negative = *first == b'-';
        value = &value[1..];
    }
    if value.is_empty() || !value.iter().all(u8::is_ascii_digit) {
        return Err(Error::Protocol(format!(
            "redis: can't parse bigInt reply: {}",
            super::cli_redis_quote::quote_bytes(line)
        )));
    }
    while value.len() > 1 && value[0] == b'0' {
        value = &value[1..];
    }
    let mut result = Vec::new();
    if negative && value != b"0" {
        result.push(b'-')
    }
    result.extend(value);
    Ok(result)
}
fn quote(bytes: &[u8]) -> String {
    let mut end = 0;
    let mut runes = 0;
    while end < bytes.len() && runes < 100 {
        let tail = &bytes[end..];
        let size = match std::str::from_utf8(tail) {
            Ok(s) => s.chars().next().unwrap().len_utf8(),
            Err(e) if e.valid_up_to() > 0 => std::str::from_utf8(&tail[..e.valid_up_to()])
                .unwrap()
                .chars()
                .next()
                .unwrap()
                .len_utf8(),
            Err(_) => 1,
        };
        end += size;
        runes += 1;
    }
    super::cli_redis_quote::quote_bytes(&bytes[..end])
}

fn server_value(raw: &[u8]) -> Result<Value> {
    let mut frame = vec![b'-'];
    frame.extend(raw);
    frame.extend(b"\r\n");
    redis::parse_redis_value(&frame).map_err(|e| Error::Protocol(e.to_string()))
}

fn typed_error(bytes: &[u8]) -> bool {
    [
        b"LOADING ".as_slice(),
        b"READONLY ",
        b"MOVED ",
        b"ASK ",
        b"CLUSTERDOWN ",
        b"TRYAGAIN ",
        b"MASTERDOWN ",
        b"NOREPLICAS ",
        b"NOAUTH ",
        b"WRONGPASS ",
        b"NOPERM ",
        b"EXECABORT ",
        b"OOM ",
    ]
    .iter()
    .any(|prefix| bytes.starts_with(prefix))
        || bytes == b"ERR max number of clients reached"
        || bytes
            .windows(b"unauthenticated".len())
            .any(|w| w == b"unauthenticated")
}
