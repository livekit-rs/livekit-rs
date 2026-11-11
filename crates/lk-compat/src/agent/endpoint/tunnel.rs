// Apache-2.0; adapted from fixed endpoint/tunnel.go and tunnels.go.
use super::{Registration, Registry};
use std::{io, sync::Arc, sync::atomic::Ordering, time::Duration};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::sync::Mutex;

#[derive(Default)]
struct Frames {
    header: [u8; 14],
    used: usize,
    remaining: u64,
}
impl Frames {
    fn boundary(&self) -> bool {
        self.used == 0 && self.remaining == 0
    }
    fn advance(&mut self, mut bytes: &[u8]) {
        while !bytes.is_empty() {
            if self.remaining > 0 {
                let take = self.remaining.min(bytes.len() as u64) as usize;
                self.remaining -= take as u64;
                bytes = &bytes[take..];
                continue;
            }
            self.header[self.used] = bytes[0];
            self.used += 1;
            bytes = &bytes[1..];
            let mut needed = 2;
            if self.used >= 2 {
                needed += match self.header[1] & 127 {
                    126 => 2,
                    127 => 8,
                    _ => 0,
                };
                if self.header[1] & 128 != 0 {
                    needed += 4;
                }
            }
            if self.used < needed {
                continue;
            }
            self.remaining = match self.header[1] & 127 {
                126 => u16::from_be_bytes(self.header[2..4].try_into().unwrap()) as u64,
                127 => u64::from_be_bytes(self.header[2..10].try_into().unwrap()),
                n => n as u64,
            };
            self.used = 0;
        }
    }
}
struct ClientWriter<W> {
    io: W,
    frames: Frames,
}
async fn down<R: AsyncRead + Unpin, W: AsyncWrite + Unpin>(
    mut read: R,
    write: Arc<Mutex<ClientWriter<W>>>,
) -> io::Result<()> {
    let mut buf = [0u8; 8192];
    loop {
        let n = read.read(&mut buf).await?;
        let mut sink = write.lock().await;
        if n == 0 {
            return sink.io.shutdown().await;
        }
        let mut at = 0;
        while at < n {
            let sent = sink.io.write(&buf[at..n]).await?;
            if sent == 0 {
                return Err(io::ErrorKind::WriteZero.into());
            }
            sink.frames.advance(&buf[at..at + sent]);
            at += sent;
        }
        sink.io.flush().await?;
    }
}
async fn closing<W: AsyncWrite + Unpin>(write: Arc<Mutex<ClientWriter<W>>>) {
    // Never inject a close in the middle of a frame or wait indefinitely behind
    // a non-reading client. Returning drops both pipe futures and their streams.
    let _ = tokio::time::timeout(Duration::from_secs(1), async {
        let mut sink = write.lock().await;
        if sink.frames.boundary() {
            sink.io.write_all(&[0x88, 0x02, 0x03, 0xe9]).await?;
            sink.io.flush().await?;
        }
        Ok::<(), io::Error>(())
    })
    .await;
}
pub(super) async fn splice<C, W>(
    client: C,
    worker: W,
    registration: Arc<Registration>,
    registry: Registry,
) where
    C: AsyncRead + AsyncWrite + Unpin + Send + 'static,
    W: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    let (mut cr, cw) = tokio::io::split(client);
    let (wr, mut ww) = tokio::io::split(worker);
    let client_write = Arc::new(Mutex::new(ClientWriter {
        io: cw,
        frames: Frames::default(),
    }));
    let copy_down = down(wr, client_write.clone());
    let copy_up = async {
        tokio::io::copy(&mut cr, &mut ww).await?;
        ww.shutdown().await
    };
    tokio::pin!(copy_up);
    tokio::pin!(copy_down);
    let pipe = async {
        tokio::select! {
            result=&mut copy_up => if result.is_ok(){let _=copy_down.await;},
            result=&mut copy_down => if result.is_ok(){let _=tokio::time::timeout(Duration::from_secs(5),copy_up).await;},
        }
    };
    let mut closed = registration.closed();
    let mut force = registry.tunnel_force.subscribe();
    let mut front = registry.tunnel_draining.subscribe();
    let initially_draining = *front.borrow();
    let drain = async {
        let mut poll = tokio::time::interval(Duration::from_secs(1));
        loop {
            if *front.borrow() || registration.draining.load(Ordering::Acquire) {
                break;
            }
            tokio::select! {_=poll.tick()=>{},_=front.changed()=>{}}
        }
        tokio::time::sleep(registry.tunnel_timeout()).await;
    };
    if initially_draining || *force.borrow() {
        closing(client_write).await;
        return;
    }
    if *closed.borrow() {
        return;
    }
    tokio::select! {
        _=pipe=>{},
        _=closed.changed()=>{},
        _=drain=>{closing(client_write).await;},
        _=force.changed()=>{closing(client_write).await;},
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn frame_boundaries_survive_split_extended_and_masked_headers() {
        for payload in [0usize, 5, 126, 65536] {
            let mut frame = vec![0x82];
            if payload < 126 {
                frame.push(payload as u8 | 128);
            } else if payload <= 65535 {
                frame.push(126 | 128);
                frame.extend_from_slice(&(payload as u16).to_be_bytes());
            } else {
                frame.push(127 | 128);
                frame.extend_from_slice(&(payload as u64).to_be_bytes());
            }
            frame.extend_from_slice(&[1, 2, 3, 4]);
            frame.resize(frame.len() + payload, 0);
            let mut state = Frames::default();
            for (idx, byte) in frame.iter().enumerate() {
                state.advance(&[*byte]);
                assert_eq!(state.boundary(), idx == frame.len() - 1);
            }
        }
    }
}
