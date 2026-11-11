use super::Runtime;
use std::future::{Future, pending};
use std::io;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::{Duration, Instant};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};

/// Marks a failed flush whose connection must be replaced before a retry.
#[derive(Clone, Copy, Debug)]
enum SocketWriteTimeout {
    IncompleteWrite,
    Flush,
}

impl std::fmt::Display for SocketWriteTimeout {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("Redis socket write timed out")
    }
}

impl std::error::Error for SocketWriteTimeout {}

#[cfg(feature = "cluster-async")]
pub(crate) fn is_socket_write_timeout(error: &crate::RedisError) -> bool {
    error
        .as_io_error()
        .and_then(io::Error::get_ref)
        .is_some_and(|source| source.is::<SocketWriteTimeout>())
}

/// Whether the deadline expired with bytes still pending in poll_write.
pub(super) fn is_incomplete_write_timeout(error: &crate::RedisError) -> bool {
    error
        .as_io_error()
        .and_then(io::Error::get_ref)
        .and_then(|source| source.downcast_ref::<SocketWriteTimeout>())
        .is_some_and(|stage| matches!(stage, SocketWriteTimeout::IncompleteWrite))
}

/// Times a transport write across partial progress and its flush. No timer is
/// allocated unless the socket actually returns Pending.
pub(super) struct WriteDeadline<S> {
    stream: S,
    timeout: Option<Duration>,
    started: Option<Instant>,
    wake: Option<Pin<Box<dyn Future<Output = ()> + Send>>>,
}

impl<S> WriteDeadline<S> {
    pub(super) fn new(stream: S, timeout: Option<Duration>) -> Self {
        Self {
            stream,
            timeout,
            started: None,
            wake: None,
        }
    }

    fn remaining(&self) -> Option<Duration> {
        Some(self.timeout?.saturating_sub(self.started?.elapsed()))
    }

    fn on_pending<T>(
        &mut self,
        cx: &mut Context<'_>,
        stage: SocketWriteTimeout,
    ) -> Poll<io::Result<T>> {
        if let Some(remaining) = self.remaining() {
            if remaining.is_zero() {
                return Poll::Ready(Err(Self::expired(stage)));
            }
            let timer = self.wake.get_or_insert_with(|| {
                Box::pin(async move {
                    let _ = Runtime::locate().timeout(remaining, pending::<()>()).await;
                })
            });
            if timer.as_mut().poll(cx).is_ready() {
                return Poll::Ready(Err(Self::expired(stage)));
            }
        }
        Poll::Pending
    }

    fn expired(stage: SocketWriteTimeout) -> io::Error {
        io::Error::new(io::ErrorKind::TimedOut, stage)
    }
}

impl<S: AsyncRead + Unpin> AsyncRead for WriteDeadline<S> {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().stream).poll_read(cx, buf)
    }
}

impl<S: AsyncWrite + Unpin> AsyncWrite for WriteDeadline<S> {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        let this = self.get_mut();
        let stage = if buf.is_empty() {
            SocketWriteTimeout::Flush
        } else {
            SocketWriteTimeout::IncompleteWrite
        };
        if this.timeout.is_some() {
            this.started.get_or_insert_with(Instant::now);
            if this
                .remaining()
                .is_some_and(|remaining| remaining.is_zero())
            {
                return Poll::Ready(Err(Self::expired(stage)));
            }
        }
        match Pin::new(&mut this.stream).poll_write(cx, buf) {
            Poll::Pending => this.on_pending(cx, stage),
            ready => ready,
        }
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        let stage = SocketWriteTimeout::Flush;
        if this
            .remaining()
            .is_some_and(|remaining| remaining.is_zero())
        {
            return Poll::Ready(Err(Self::expired(stage)));
        }
        match Pin::new(&mut this.stream).poll_flush(cx) {
            Poll::Pending => this.on_pending(cx, stage),
            ready => {
                this.started = None;
                this.wake = None;
                ready
            }
        }
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().stream).poll_shutdown(cx)
    }
}
