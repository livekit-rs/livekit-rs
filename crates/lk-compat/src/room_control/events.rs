//! Authoritative bounded generation projections. Notify is only a hint: failures
//! retain dirty revisions, unlike lossy lifecycle/webhook event queues.
use super::RoomControl;
use crate::service_bus::rpc::{RpcError, RpcErrorOrigin};
use rtc_core::events::{EngineEvent, EventSink, RoomProjection};
use rtc_core::model::RoomInfo;
use std::sync::{
    Arc, Mutex, Weak,
    atomic::{AtomicBool, Ordering},
};
use std::time::{Duration, Instant};
use tokio::sync::Notify;
fn closed() -> RpcError {
    let mut e = RpcError::new("canceled", "server is closed");
    e.origin = RpcErrorOrigin::Closed;
    e
}
fn timed_out() -> RpcError {
    let mut e = RpcError::new("deadline_exceeded", "request timed out");
    e.origin = RpcErrorOrigin::Deadline;
    e
}
#[derive(Clone, Copy)]
pub(super) struct ReconcileConfig {
    pub min_retry: Duration,
    pub max_retry: Duration,
    pub retry_timeout: Duration,
}

#[path = "events/ledger.rs"]
mod ledger;
struct Shared {
    quarantine: Mutex<Option<&'static str>>,
    ledger: Mutex<Option<ledger::Ledger>>,
    changed: Notify,
    publication: tokio::sync::watch::Sender<u64>,
    stop: Mutex<Option<Instant>>,
    drain_budget: Mutex<Duration>,
    completion: tokio::sync::watch::Sender<Option<Result<(), RpcError>>>,
    drainer: Mutex<Option<tokio::task::JoinHandle<()>>>,
}
pub struct RoutingEvents {
    downstream: Arc<dyn EventSink>,
    shared: Arc<Shared>,
    enabled: Arc<AtomicBool>,
    worker: Mutex<Option<tokio::task::JoinHandle<Result<(), RpcError>>>>,
}
impl RoutingEvents {
    pub fn new(downstream: Arc<dyn EventSink>) -> Self {
        let completion = tokio::sync::watch::channel(None).0;
        Self {
            downstream,
            shared: Arc::new(Shared {
                quarantine: Mutex::new(None),
                ledger: Mutex::new(None),
                changed: Notify::new(),
                publication: tokio::sync::watch::channel(0).0,
                stop: Mutex::new(None),
                drain_budget: Mutex::new(Duration::ZERO),
                completion,
                drainer: Mutex::new(None),
            }),
            enabled: Arc::new(AtomicBool::new(false)),
            worker: Mutex::new(None),
        }
    }
    pub(super) fn participant_store_attempted(&self, projection: &RoomProjection) {
        if let Some(ledger) = self
            .shared
            .ledger
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .as_mut()
        {
            ledger.participant_store_attempted(projection);
        }
        self.shared
            .publication
            .send_modify(|epoch| *epoch = epoch.wrapping_add(1));
    }
    pub(super) fn participant_topics_failed(&self, projection: &RoomProjection, error: &RpcError) {
        if let Some(ledger) = self
            .shared
            .ledger
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .as_mut()
        {
            ledger.participant_topics_failed(projection, error);
        }
        self.shared
            .publication
            .send_modify(|epoch| *epoch = epoch.wrapping_add(1));
    }
    /// Register before checking so publication/retirement cannot be missed.
    /// This runs in the relay pump, never while the media actor awaits lifecycle.
    pub(crate) async fn wait_participant_admitted(
        &self,
        room: &str,
        identity: &str,
        sid: &str,
    ) -> Result<(), RpcError> {
        let mut changed = self.shared.publication.subscribe();
        loop {
            if self
                .shared
                .stop
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .is_some()
            {
                return Err(closed());
            }
            let admitted = self
                .shared
                .ledger
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .as_ref()
                .and_then(|ledger| ledger.participant_admitted(room, identity, sid));
            match admitted {
                Some(Ok(true)) => return Ok(()),
                Some(Ok(false)) => {}
                Some(Err(error)) => return Err(error),
                None => {
                    return Err(RpcError::new(
                        "aborted",
                        "signal participant generation retired or replaced before initial store",
                    ));
                }
            }
            changed.changed().await.map_err(|_| closed())?;
        }
    }
    pub(super) fn ensure_attachable(&self) -> Result<(), RpcError> {
        if let Some(reason) = *self
            .shared
            .quarantine
            .lock()
            .unwrap_or_else(|p| p.into_inner())
        {
            return Err(RpcError::new("unavailable", reason));
        }
        if self
            .shared
            .ledger
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .is_some()
        {
            return Err(RpcError::new(
                "already_exists",
                "room authority already attached",
            ));
        }
        Ok(())
    }
    pub fn attach(
        &self,
        router: Weak<RoomControl>,
        _legacy_hint_queue_limit: usize,
    ) -> Result<(), RpcError> {
        self.ensure_attachable()?;
        let actual = router.upgrade().ok_or_else(closed)?;
        let capacity = actual.reconcile_capacity();
        let cfg = actual.reconcile_config();
        let mut slot = self.shared.ledger.lock().unwrap_or_else(|p| p.into_inner());
        if slot.is_some() {
            return Err(RpcError::new(
                "already_exists",
                "room authority already attached",
            ));
        }
        *slot =
            Some(ledger::Ledger::new(capacity).map_err(|e| RpcError::new("invalid_argument", e))?);
        *self
            .shared
            .drain_budget
            .lock()
            .unwrap_or_else(|p| p.into_inner()) = cfg.retry_timeout;
        drop(slot);
        let shared = self.shared.clone();
        let worker = tokio::spawn(async move {
            let mut backoff = cfg.min_retry;
            loop {
                let changed = shared.changed.notified();
                let (pending, fatal, retry_health, retained) = {
                    let lock = shared.ledger.lock().unwrap_or_else(|p| p.into_inner());
                    let l = lock.as_ref().expect("attached ledger");
                    (l.pending(), l.fatal, l.failed, l.len())
                };
                let stopping = *shared.stop.lock().unwrap_or_else(|p| p.into_inner());
                if let Some(deadline) = stopping {
                    if retained == 0 && !fatal {
                        return Ok(());
                    }
                    if Instant::now() >= deadline {
                        return Err(RpcError::new(
                            "unavailable",
                            format!(
                                "routing shutdown retains {retained} active or unacknowledged generations"
                            ),
                        ));
                    }
                }
                if fatal {
                    if let Some(actual) = router.upgrade() {
                        let _ = actual.reconciliation_blocked(true).await;
                    }
                    return Err(RpcError::new(
                        "internal",
                        "routing authority invariant failed; no revisions acknowledged",
                    ));
                }
                if pending.is_empty() && !retry_health {
                    tokio::select! {_=changed=>{},_=async{if let Some(deadline)=stopping{tokio::time::sleep_until(deadline.into()).await;}else{std::future::pending::<()>().await;}}=>{}}
                    continue;
                }
                let Some(actual) = router.upgrade() else {
                    return Err(RpcError::new(
                        "unavailable",
                        "routing owner dropped before authority drained",
                    ));
                };
                let mut failed = false;
                for snapshot in pending {
                    if !shared
                        .ledger
                        .lock()
                        .unwrap_or_else(|p| p.into_inner())
                        .as_ref()
                        .expect("attached ledger")
                        .is_current(&snapshot)
                    {
                        continue;
                    }
                    let mut deadline =
                        Instant::now()
                            .checked_add(cfg.retry_timeout)
                            .ok_or_else(|| {
                                RpcError::new(
                                    "invalid_argument",
                                    "reconciliation deadline overflow",
                                )
                            })?;
                    if let Some(stop) = *shared.stop.lock().unwrap_or_else(|p| p.into_inner()) {
                        deadline = deadline.min(stop);
                    }
                    let result = tokio::time::timeout_at(
                        deadline.into(),
                        actual.reconcile_projection(
                            &snapshot.projection,
                            || {
                                shared
                                    .ledger
                                    .lock()
                                    .unwrap_or_else(|p| p.into_inner())
                                    .as_ref()
                                    .expect("attached ledger")
                                    .is_current(&snapshot)
                            },
                            |observed| {
                                shared
                                    .ledger
                                    .lock()
                                    .unwrap_or_else(|p| p.into_inner())
                                    .as_mut()
                                    .expect("attached ledger")
                                    .prepare_participant_cache(&snapshot, observed)
                            },
                        ),
                    )
                    .await
                    .unwrap_or_else(|_| Err(timed_out()));
                    let mut slot = shared.ledger.lock().unwrap_or_else(|p| p.into_inner());
                    let l = slot.as_mut().expect("attached ledger");
                    match result {
                        Ok(()) => l.acknowledge(&snapshot),
                        Err(error) => {
                            failed = true;
                            if error.code != "aborted" {
                                l.failed = true;
                                // Includes outer reconciliation timeout: no
                                // request-pump waiter may outlive failed initial
                                // topic admission. Completed store attempts are
                                // already admitted and retain Go's log/continue.
                                l.participant_topics_failed(&snapshot.projection, &error);
                                shared
                                    .publication
                                    .send_modify(|epoch| *epoch = epoch.wrapping_add(1));
                            }
                            tracing::warn!(code=%error.code,"routing generation remains dirty; retrying");
                        }
                    }
                }
                let healthy = {
                    let mut slot = shared.ledger.lock().unwrap_or_else(|p| p.into_inner());
                    let l = slot.as_mut().expect("attached ledger");
                    if l.quiescent() {
                        l.failed = false;
                    }
                    !l.failed && !l.fatal
                };
                if let Err(error) = actual.reconciliation_blocked(!healthy).await {
                    tracing::warn!(code=%error.code,"routing node admission state update failed; retrying");
                    failed = true;
                    let mut slot = shared.ledger.lock().unwrap_or_else(|p| p.into_inner());
                    slot.as_mut().expect("attached ledger").failed = true;
                }
                drop(actual);
                if failed {
                    let delay = backoff;
                    backoff = backoff.saturating_mul(2).min(cfg.max_retry);
                    tokio::time::sleep(delay).await;
                } else {
                    backoff = cfg.min_retry;
                }
            }
        });
        *self.worker.lock().unwrap_or_else(|p| p.into_inner()) = Some(worker);
        self.enabled.store(true, Ordering::Release);
        Ok(())
    }
    pub async fn detach(&self) -> Result<(), RpcError> {
        // Subscribe before ownership transfer. The independent, tracked finisher owns
        // the actual worker even if the first caller is canceled while awaiting it.
        let mut completion = self.shared.completion.subscribe();
        let task = self.worker.lock().unwrap_or_else(|p| p.into_inner()).take();
        if let Some(mut task) = task {
            let budget = *self
                .shared
                .drain_budget
                .lock()
                .unwrap_or_else(|p| p.into_inner());
            let deadline = Instant::now() + budget;
            *self.shared.stop.lock().unwrap_or_else(|p| p.into_inner()) = Some(deadline);
            self.shared.changed.notify_one();
            self.shared
                .publication
                .send_modify(|epoch| *epoch = epoch.wrapping_add(1));
            let shared = self.shared.clone();
            let enabled = self.enabled.clone();
            let finisher = tokio::spawn(async move {
                let result = match tokio::time::timeout_at(deadline.into(), &mut task).await {
                    Ok(result) => result.unwrap_or_else(|_| {
                        Err(RpcError::new(
                            "internal",
                            "routing reconciliation task failed",
                        ))
                    }),
                    Err(_) => {
                        task.abort();
                        let _ = task.await;
                        Err(RpcError::new(
                            "unavailable",
                            "routing reconciliation shutdown deadline exceeded",
                        ))
                    }
                };
                if result.is_ok() {
                    enabled.store(false, Ordering::Release);
                }
                shared.completion.send_replace(Some(result));
            });
            *self
                .shared
                .drainer
                .lock()
                .unwrap_or_else(|p| p.into_inner()) = Some(finisher);
        }
        loop {
            if let Some(result) = completion.borrow().clone() {
                return result;
            }
            // A pre-attachment no-op has no task and no terminal/dirty authority.
            if !self.enabled.load(Ordering::Acquire)
                && self
                    .shared
                    .stop
                    .lock()
                    .unwrap_or_else(|p| p.into_inner())
                    .is_none()
            {
                return Ok(());
            }
            completion.changed().await.map_err(|_| closed())?;
        }
    }
    // A timed-out HDEL has unknown execution outcome. Do not advertise a
    // reusable attachment then risk a late delete erasing its fresh same-ID node.
    // Source Stop cancels despite errors; it does not promise same-ID retry.
    pub(super) async fn quarantine_empty_attachment(&self) -> Result<(), RpcError> {
        *self
            .shared
            .quarantine
            .lock()
            .unwrap_or_else(|p| p.into_inner()) =
            Some("room authority quarantined after uncertain node unregister");
        let result = self.detach().await;
        let ledger = self.shared.ledger.lock().unwrap_or_else(|p| p.into_inner());
        if ledger.as_ref().is_some_and(|l| l.len() != 0) {
            return Err(RpcError::new(
                "failed_precondition",
                "quarantined authority retains owned generations",
            ));
        }
        self.enabled.store(false, Ordering::Release);
        // Keep ledger/stop/quarantine identity. No autonomous reset, cleanup retry or
        // new node registration may race the uncertain old registry command.
        result
    }
    // Startup has not published a usable collaborator. Roll back only an empty
    // attachment; never discard a real owner projection to make retry appear safe.
    pub(super) async fn rollback_empty_attachment(&self) -> Result<(), RpcError> {
        self.detach().await?;
        let mut ledger = self.shared.ledger.lock().unwrap_or_else(|p| p.into_inner());
        if ledger.as_ref().is_some_and(|l| l.len() != 0) {
            return Err(RpcError::new(
                "failed_precondition",
                "cannot roll back authority with owned generations",
            ));
        }
        self.enabled.store(false, Ordering::Release);
        *ledger = None;
        *self.shared.stop.lock().unwrap_or_else(|p| p.into_inner()) = None;
        self.shared.completion.send_replace(None);
        self.shared
            .drainer
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .take();
        Ok(())
    }
    #[cfg(test)]
    pub(super) fn drain_budget(&self) -> Duration {
        *self
            .shared
            .drain_budget
            .lock()
            .unwrap_or_else(|p| p.into_inner())
    }
    #[cfg(test)]
    pub(super) fn failed(&self) -> bool {
        self.shared
            .ledger
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .as_ref()
            .is_some_and(|l| l.failed)
    }
    pub fn unresolved(&self) -> usize {
        self.shared
            .ledger
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .as_ref()
            .map_or(0, ledger::Ledger::unresolved)
    }
}
impl EventSink for RoutingEvents {
    fn emit(&self, event: EngineEvent) {
        self.downstream.emit(event.clone());
        if !self.enabled.load(Ordering::Acquire) {
            return;
        }
        {
            let mut slot = self.shared.ledger.lock().unwrap_or_else(|p| p.into_inner());
            let l = slot.as_mut().expect("enabled ledger");
            match event {
                EngineEvent::RoomStarted { room } => l.start(&room),
                EngineEvent::RoomFinished { room, .. } => l.finish(&room),
                _ => {}
            }
        }
        self.shared.changed.notify_one();
        self.shared
            .publication
            .send_modify(|epoch| *epoch = epoch.wrapping_add(1));
    }
    fn reserve_room(&self, room: &RoomInfo) -> Result<(), String> {
        if let Some(reason) = *self
            .shared
            .quarantine
            .lock()
            .unwrap_or_else(|p| p.into_inner())
        {
            return Err(reason.into());
        }
        if self
            .shared
            .stop
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .is_some()
        {
            return Err("routing authority is shutting down".into());
        }
        self.downstream.reserve_room(room)?;
        if self.enabled.load(Ordering::Acquire)
            && let Err(error) = self
                .shared
                .ledger
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .as_mut()
                .expect("enabled ledger")
                .reserve(room)
        {
            self.downstream.cancel_room_reservation(room);
            return Err(error);
        }
        Ok(())
    }
    fn cancel_room_reservation(&self, room: &RoomInfo) {
        self.downstream.cancel_room_reservation(room);
        if self.enabled.load(Ordering::Acquire) {
            self.shared
                .ledger
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .as_mut()
                .expect("enabled ledger")
                .cancel(room);
            self.shared.changed.notify_one();
            self.shared
                .publication
                .send_modify(|epoch| *epoch = epoch.wrapping_add(1));
        }
    }
    fn wants_room_projection(&self) -> bool {
        self.enabled.load(Ordering::Acquire) || self.downstream.wants_room_projection()
    }
    fn project_room(&self, projection: RoomProjection) {
        self.downstream.project_room(projection.clone());
        if self.enabled.load(Ordering::Acquire) {
            self.shared
                .ledger
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .as_mut()
                .expect("enabled ledger")
                .project(projection);
            self.shared.changed.notify_one();
            self.shared
                .publication
                .send_modify(|epoch| *epoch = epoch.wrapping_add(1));
        }
    }
}
impl Drop for RoutingEvents {
    fn drop(&mut self) {
        *self.shared.stop.lock().unwrap_or_else(|p| p.into_inner()) = Some(Instant::now());
        self.shared.changed.notify_one();
        self.shared
            .publication
            .send_modify(|epoch| *epoch = epoch.wrapping_add(1));
    }
}

#[cfg(test)]
#[path = "events/shutdown_tests.rs"]
mod shutdown_tests;

#[cfg(test)]
#[path = "events/admission_tests.rs"]
mod admission_tests;
