//! Fixed v1.13.10 agent HTTP data plane. A registration owns one live WT
//! session. HTTP streams are opened on demand; max_streams is not a semaphore.
mod front;
mod manifest;
mod tunnel;
use super::registry::WorkerError;
pub use manifest::Endpoint;
use std::{
    collections::{HashMap, HashSet},
    future::Future,
    pin::Pin,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicI64, AtomicU32, AtomicUsize, Ordering},
    },
};
use tokio::{
    io::{AsyncRead, AsyncWrite},
    sync::watch,
};

/// Inserted by the actual TLS listener, never derived from caller headers or
/// an absolute-form request URI.
#[derive(Clone, Copy)]
pub struct TlsTransport;

/// Application code from an authenticated peer's response-side WT reset.
#[derive(Clone, Copy, Debug, thiserror::Error)]
#[error("worker response reset with WebTransport application code {0}")]
pub struct RemoteResponseReset(pub u32);

pub trait EndpointIo: AsyncRead + AsyncWrite + Unpin + Send {}
impl<T: AsyncRead + AsyncWrite + Unpin + Send> EndpointIo for T {}
pub type StreamIo = Box<dyn EndpointIo>;
pub type OpenFuture = Pin<Box<dyn Future<Output = std::io::Result<StreamIo>> + Send>>;
pub type StreamOpener = Arc<dyn Fn() -> OpenFuture + Send + Sync>;

pub struct Registration {
    pub worker_id: String,
    pub api_key: String,
    pub agent_name: String,
    pub deployment: String,
    pub manifest: Vec<Endpoint>,
    pub opener: StreamOpener,
    open: AtomicUsize,
    draining: AtomicBool,
    closed: watch::Sender<bool>,
    max_streams: usize,
    tunnels: AtomicUsize,
    tunnel_limit: usize,
}
impl Registration {
    pub fn inflight(&self) -> usize {
        self.open.load(Ordering::Acquire)
    }
    pub fn spare_streams(&self) -> usize {
        if *self.closed.borrow() {
            0
        } else {
            self.max_streams.saturating_sub(self.inflight())
        }
    }
    pub fn has_session(&self) -> bool {
        !*self.closed.borrow()
    }
    pub fn close(&self) {
        self.closed.send_replace(true);
    }
    pub fn closed(&self) -> watch::Receiver<bool> {
        self.closed.subscribe()
    }
    pub async fn open(self: &Arc<Self>) -> std::io::Result<(StreamIo, StreamLease)> {
        let mut closed = self.closed();
        if *closed.borrow() {
            return Err(std::io::Error::other(
                "registration has no data-plane session",
            ));
        }
        let io = tokio::select! {
            result=(self.opener)()=>result?,
            _=closed.changed()=>return Err(std::io::Error::other("registration closed")),
        };
        if *closed.borrow() {
            return Err(std::io::Error::other("registration closed"));
        }
        self.open.fetch_add(1, Ordering::AcqRel);
        Ok((io, StreamLease(self.clone())))
    }
}
pub struct StreamLease(Arc<Registration>);
impl Drop for StreamLease {
    fn drop(&mut self) {
        self.0.open.fetch_sub(1, Ordering::AcqRel);
    }
}
#[derive(Clone)]
pub struct Registry {
    inner: Arc<Mutex<HashMap<String, Arc<Registration>>>>,
    disabled: Arc<AtomicBool>,
    max_streams: Arc<AtomicU32>,
    max_tunnels: Arc<AtomicU32>,
    drain_timeout_ns: Arc<AtomicI64>,
    tunnel_count: Arc<AtomicUsize>,
    tunnel_changed: Arc<tokio::sync::Notify>,
    tunnel_draining: Arc<watch::Sender<bool>>,
    tunnel_force: Arc<watch::Sender<bool>>,
}
impl Default for Registry {
    fn default() -> Self {
        Self {
            inner: Default::default(),
            disabled: Default::default(),
            max_streams: Arc::new(AtomicU32::new(256)),
            max_tunnels: Default::default(),
            drain_timeout_ns: Default::default(),
            tunnel_count: Default::default(),
            tunnel_changed: Default::default(),
            tunnel_draining: Arc::new(watch::channel(false).0),
            tunnel_force: Arc::new(watch::channel(false).0),
        }
    }
}
impl Registry {
    pub fn configure(&self, disabled: bool, max_streams: u32) {
        self.disabled.store(disabled, Ordering::Release);
        self.max_streams.store(
            if max_streams == 0 { 256 } else { max_streams },
            Ordering::Release,
        );
    }
    pub fn configure_tunnels(&self, max_tunnels: u32, drain_timeout_ns: i64) {
        self.max_tunnels.store(max_tunnels, Ordering::Release);
        self.drain_timeout_ns
            .store(drain_timeout_ns, Ordering::Release);
    }
    fn tunnel_timeout(&self) -> std::time::Duration {
        let ns = self.drain_timeout_ns.load(Ordering::Acquire);
        if ns <= 0 {
            std::time::Duration::from_secs(30)
        } else {
            std::time::Duration::from_nanos(ns as u64)
        }
    }
    fn acquire_tunnel(&self, reg: &Arc<Registration>) -> Option<TunnelLease> {
        if *self.tunnel_draining.borrow() {
            return None;
        }
        let mut n = reg.tunnels.load(Ordering::Acquire);
        loop {
            if n >= reg.tunnel_limit {
                return None;
            }
            match reg
                .tunnels
                .compare_exchange_weak(n, n + 1, Ordering::AcqRel, Ordering::Acquire)
            {
                Ok(_) => break,
                Err(actual) => n = actual,
            }
        }
        Some(TunnelLease {
            registry: self.clone(),
            registration: reg.clone(),
            active: false,
        })
    }
    pub fn begin_tunnel_drain(&self) {
        self.tunnel_draining.send_replace(true);
    }
    pub fn force_tunnel_drain(&self) {
        self.begin_tunnel_drain();
        self.tunnel_force.send_replace(true);
    }
    pub async fn drain_tunnels(&self) {
        self.begin_tunnel_drain();
        let idle = async {
            loop {
                let changed = self.tunnel_changed.notified();
                tokio::pin!(changed);
                changed.as_mut().enable();
                if self.tunnel_count.load(Ordering::Acquire) == 0 {
                    break;
                }
                changed.await;
            }
        };
        if tokio::time::timeout(self.tunnel_timeout(), idle)
            .await
            .is_err()
        {
            self.tunnel_force.send_replace(true);
            loop {
                let changed = self.tunnel_changed.notified();
                tokio::pin!(changed);
                changed.as_mut().enable();
                if self.tunnel_count.load(Ordering::Acquire) == 0 {
                    break;
                }
                changed.await;
            }
        }
    }
    pub fn open_tunnels(&self) -> usize {
        self.tunnel_count.load(Ordering::Acquire)
    }
    pub fn disabled(&self) -> bool {
        self.disabled.load(Ordering::Acquire)
    }
    pub fn negotiate(
        &self,
        req: &lk_protocol::livekit::RegisterWorkerRequest,
    ) -> Result<Vec<Endpoint>, WorkerError> {
        if self.disabled() {
            return Err(WorkerError::InvalidRegistration(
                "agent HTTP endpoints are disabled on this server".into(),
            ));
        }
        manifest::negotiate(req)
    }
    pub fn register(
        &self,
        worker: &super::registry::Registration,
        manifest: Vec<Endpoint>,
        opener: StreamOpener,
    ) {
        let (closed, _) = watch::channel(false);
        let reg = Arc::new(Registration {
            worker_id: worker.worker_id.clone(),
            api_key: worker.api_key.clone(),
            agent_name: worker.key.agent_name.clone(),
            deployment: worker.key.deployment.clone(),
            manifest,
            opener,
            open: AtomicUsize::new(0),
            draining: AtomicBool::new(false),
            closed,
            max_streams: self.max_streams.load(Ordering::Acquire) as usize,
            tunnels: AtomicUsize::new(0),
            tunnel_limit: match self.max_tunnels.load(Ordering::Acquire) {
                0 => (self.max_streams.load(Ordering::Acquire) as usize / 2).max(1),
                n => n as usize,
            },
        });
        if let Some(old) = self
            .inner
            .lock()
            .unwrap()
            .insert(reg.worker_id.clone(), reg)
        {
            old.close();
        }
    }
    pub fn disconnect(&self, id: &str) {
        if let Some(old) = self.inner.lock().unwrap().remove(id) {
            old.close();
        }
    }
    pub fn drain(&self, id: &str, value: bool) {
        if let Some(r) = self.inner.lock().unwrap().get(id) {
            r.draining.store(value, Ordering::Release);
        }
    }
    pub fn close_all(&self) {
        for (_, r) in self.inner.lock().unwrap().drain() {
            r.close();
        }
    }
    /// Same headroom consumed by the fixed multi-node endpoint-affinity layer.
    /// Exposed real state; distributed affinity activation remains a separate gap.
    pub fn spare_streams(&self, key: &str, agent: &str, deployment: &str) -> usize {
        self.scope(key, agent, deployment)
            .iter()
            .filter(|r| !r.draining.load(Ordering::Acquire))
            .map(|r| r.spare_streams())
            .sum()
    }
    fn scope(&self, key: &str, agent: &str, deployment: &str) -> Vec<Arc<Registration>> {
        self.inner
            .lock()
            .unwrap()
            .values()
            .filter(|r| r.api_key == key && r.agent_name == agent && r.deployment == deployment)
            .cloned()
            .collect()
    }
    fn single_key(&self) -> Option<String> {
        let keys: HashSet<_> = self
            .inner
            .lock()
            .unwrap()
            .values()
            .map(|r| r.api_key.clone())
            .collect();
        (keys.len() == 1).then(|| keys.into_iter().next().unwrap())
    }
}

pub fn routes(state: super::websocket::WorkerHttpState) -> axum::Router {
    front::routes(state)
}

struct TunnelLease {
    registry: Registry,
    registration: Arc<Registration>,
    active: bool,
}
impl TunnelLease {
    fn activate(&mut self) {
        if !self.active {
            self.active = true;
            self.registry.tunnel_count.fetch_add(1, Ordering::AcqRel);
        }
    }
}
impl Drop for TunnelLease {
    fn drop(&mut self) {
        self.registration.tunnels.fetch_sub(1, Ordering::AcqRel);
        if self.active {
            self.registry.tunnel_count.fetch_sub(1, Ordering::AcqRel);
            self.registry.tunnel_changed.notify_waiters();
        }
    }
}

#[cfg(test)]
mod tests;
