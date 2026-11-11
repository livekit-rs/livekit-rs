// Copyright 2024 LiveKit, Inc. Apache-2.0; adapted from fixed pkg/agent/worker.go.
// See CONTRACT.md and SOURCE.json for implementation and verification boundaries.
use lk_protocol::livekit as pb;
use std::{
    collections::{BTreeSet, HashMap, HashSet},
    sync::{Arc, Mutex, Weak},
    time::Duration,
};
use tokio::sync::{mpsc, oneshot};

pub const REGISTER_TIMEOUT: Duration = Duration::from_secs(10);
pub const ASSIGN_TIMEOUT: Duration = Duration::from_secs(10);

pub struct Outbound {
    pub message: pb::ServerMessage,
    /// Complete only after the actual WebSocket writer succeeds/fails. Queueing
    /// alone is not equivalent to upstream WriteServerMessage.
    pub written: oneshot::Sender<Result<(), WorkerError>>,
}
pub(super) async fn send_message(
    tx: &mpsc::Sender<Outbound>,
    message: pb::ServerMessage,
) -> Result<(), WorkerError> {
    let (written, receipt) = oneshot::channel();
    tx.send(Outbound { message, written })
        .await
        .map_err(|_| WorkerError::Closed)?;
    receipt.await.map_err(|_| WorkerError::Closed)?
}

#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
pub enum WorkerError {
    #[error("{0}")]
    Cluster(String),
    #[error("unknown job type")]
    UnknownJobType,
    #[error("{0}")]
    InvalidRegistration(String),
    #[error("worker closed")]
    Closed,
    #[error("worker not available")]
    Unavailable,
    #[error("no workers available")]
    NoWorkers,
    #[error("no workers with sufficient capacity")]
    NoCapacity,
    #[error("agent worker availability timeout")]
    AvailabilityTimeout,
    #[error("duplicate job assignment")]
    DuplicateJob,
    #[error("no running job for given jobID")]
    JobNotFound,
    #[error("received job update for unknown job")]
    UpdateNotFound,
    #[error("worker registry capacity exceeded")]
    Capacity,
    #[error("{0}")]
    Token(String),
}
#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct WorkerKey {
    pub agent_name: String,
    pub namespace: String,
    pub job_type: i32,
    pub deployment: String,
}
#[derive(Clone, Debug)]
pub struct Registration {
    pub worker_id: String,
    pub api_key: String,
    /// Server-controlled agent identity; never copied from worker input.
    pub agent_id: String,
    pub protocol: i64,
    pub version: String,
    pub key: WorkerKey,
    pub permissions: pb::ParticipantPermission,
}
impl Registration {
    // WS control registration does not negotiate endpoint_settings, even if the
    // request contains endpoints. The real source only negotiates with WT session.
    pub fn from_request(
        id: String,
        protocol: i64,
        request: &pb::RegisterWorkerRequest,
    ) -> Result<Self, WorkerError> {
        if !(0..=2).contains(&request.r#type) {
            return Err(WorkerError::UnknownJobType);
        }
        if request.deployment.len() > 64 {
            return Err(WorkerError::InvalidRegistration(
                "deployment exceeds 64 bytes".into(),
            ));
        }
        for (i, b) in request.deployment.bytes().enumerate() {
            if b == b'_' {
                return Err(WorkerError::InvalidRegistration(
                    "deployment contains reserved character '_'".into(),
                ));
            }
            if b <= b' ' || b == 127 {
                return Err(WorkerError::InvalidRegistration(format!(
                    "deployment contains whitespace or control byte at position {i}"
                )));
            }
        }
        Ok(Self {
            worker_id: id,
            api_key: String::new(),
            agent_id: String::new(),
            protocol,
            version: request.version.clone(),
            key: WorkerKey {
                agent_name: request.agent_name.clone(),
                namespace: request.namespace.clone().unwrap_or_default(),
                job_type: request.r#type,
                deployment: request.deployment.clone(),
            },
            permissions: request
                .allowed_permissions
                .clone()
                .unwrap_or(pb::ParticipantPermission {
                    can_subscribe: true,
                    can_publish: true,
                    can_publish_data: true,
                    can_update_metadata: true,
                    ..Default::default()
                }),
        })
    }
}
#[derive(Clone, Copy, Debug)]
pub struct Limits {
    pub workers: usize,
    pub pending_per_worker: usize,
    pub jobs_per_worker: usize,
}
impl Default for Limits {
    fn default() -> Self {
        Self {
            workers: 4096,
            pending_per_worker: 256,
            jobs_per_worker: 4096,
        }
    }
}
struct Pending {
    generation: u64,
    response: oneshot::Sender<pb::AvailabilityResponse>,
}
struct Worker {
    registration: Registration,
    status: i32,
    status_seq: u64,
    load: f32,
    draining: bool,
    outbound: mpsc::Sender<Outbound>,
    pending: HashMap<String, Pending>,
    jobs: HashMap<String, pb::Job>,
    job_generations: HashMap<String, u64>,
    ready_jobs: HashSet<String>,
}
#[derive(Default)]
struct State {
    workers: HashMap<String, Worker>,
    registration_order: Vec<String>,
    next_pending: u64,
}
#[derive(Clone)]
pub struct Registry {
    pub endpoints: super::endpoint::Registry,
    changes: Arc<tokio::sync::watch::Sender<u64>>,
    cluster: Arc<Mutex<Option<Weak<super::cluster::Cluster>>>>,
    inner: Arc<Mutex<State>>,
    limits: Limits,
}
struct PendingGuard {
    registry: Registry,
    worker: String,
    job: String,
    generation: u64,
}
impl Drop for PendingGuard {
    fn drop(&mut self) {
        let mut state = self.registry.inner.lock().unwrap();
        if let Some(worker) = state.workers.get_mut(&self.worker)
            && worker
                .pending
                .get(&self.job)
                .is_some_and(|p| p.generation == self.generation)
        {
            worker.pending.remove(&self.job);
        }
    }
}
struct RunningGuard {
    registry: Registry,
    worker: String,
    job: String,
    generation: u64,
    committed: bool,
}
impl Drop for RunningGuard {
    fn drop(&mut self) {
        if self.committed {
            return;
        }
        let mut state = self.registry.inner.lock().unwrap();
        if let Some(w) = state.workers.get_mut(&self.worker)
            && w.job_generations.get(&self.job) == Some(&self.generation)
        {
            w.jobs.remove(&self.job);
            w.job_generations.remove(&self.job);
            w.ready_jobs.remove(&self.job);
        }
    }
}
impl Registry {
    pub fn new(limits: Limits) -> Self {
        Self {
            inner: Arc::new(Mutex::new(State::default())),
            endpoints: super::endpoint::Registry::default(),
            changes: Arc::new(tokio::sync::watch::channel(0).0),
            cluster: Arc::new(Mutex::new(None)),
            limits,
        }
    }
    fn changed(&self) {
        self.changes.send_modify(|v| *v = v.wrapping_add(1));
    }
    pub(super) fn changes(&self) -> tokio::sync::watch::Receiver<u64> {
        self.changes.subscribe()
    }
    pub(super) fn cluster(&self) -> Option<Arc<super::cluster::Cluster>> {
        self.cluster
            .lock()
            .unwrap()
            .as_ref()
            .and_then(Weak::upgrade)
    }
    pub(super) fn set_cluster(&self, cluster: &Arc<super::cluster::Cluster>) {
        *self.cluster.lock().unwrap() = Some(Arc::downgrade(cluster));
    }
    pub(super) fn all_groups(&self) -> Vec<WorkerKey> {
        self.inner
            .lock()
            .unwrap()
            .workers
            .values()
            .map(|w| w.registration.key.clone())
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect()
    }
    pub(super) fn running_jobs(&self) -> Vec<String> {
        self.inner
            .lock()
            .unwrap()
            .workers
            .values()
            .flat_map(|w| {
                w.jobs
                    .keys()
                    .filter(|id| w.ready_jobs.contains(*id))
                    .cloned()
            })
            .collect()
    }
    pub(super) fn worker_for_job(&self, id: &str) -> Option<String> {
        self.inner
            .lock()
            .unwrap()
            .workers
            .iter()
            .find(|(_, w)| w.jobs.contains_key(id))
            .map(|(id, _)| id.clone())
    }
    pub(super) fn affinity(&self, key: &WorkerKey, target_load: f32) -> f32 {
        self.inner
            .lock()
            .unwrap()
            .workers
            .values()
            .filter(|w| w.registration.key == *key && w.status == 0)
            .map(|w| {
                let delta = target_load - w.load;
                if delta.is_nan() {
                    f32::NAN
                } else {
                    delta.max(0.)
                }
            })
            .sum()
    }
    pub async fn register_ready(
        &self,
        registration: Registration,
        outbound: mpsc::Sender<Outbound>,
    ) -> Result<(), WorkerError> {
        let id = registration.worker_id.clone();
        self.register(registration, outbound)?;
        if let Some(cluster) = self.cluster() {
            if let Err(error) = cluster.reconcile().await {
                self.disconnect(&id);
                return Err(WorkerError::Cluster(error.to_string()));
            }
        }
        Ok(())
    }
    pub fn register(
        &self,
        registration: Registration,
        outbound: mpsc::Sender<Outbound>,
    ) -> Result<(), WorkerError> {
        let mut state = self.inner.lock().unwrap();
        if state.workers.len() >= self.limits.workers {
            return Err(WorkerError::Capacity);
        }
        if state.workers.contains_key(&registration.worker_id) {
            return Err(WorkerError::InvalidRegistration(
                "duplicate worker registration".into(),
            ));
        }
        state
            .registration_order
            .push(registration.worker_id.clone());
        state.workers.insert(
            registration.worker_id.clone(),
            Worker {
                registration,
                status: 0,
                status_seq: 0,
                load: 0.,
                draining: false,
                outbound,
                pending: HashMap::new(),
                jobs: HashMap::new(),
                job_generations: HashMap::new(),
                ready_jobs: HashSet::new(),
            },
        );
        self.changed();
        Ok(())
    }
    // Drops availability senders immediately; disconnect does not invent success
    // or mutate a room's job state (fixed upstream deregisterJob has TODO there).
    pub fn disconnect(&self, id: &str) -> Vec<String> {
        self.endpoints.disconnect(id);
        let mut state = self.inner.lock().unwrap();
        state.registration_order.retain(|entry| entry != id);
        let jobs = state
            .workers
            .remove(id)
            .map(|w| w.jobs.into_keys().collect())
            .unwrap_or_default();
        self.changed();
        jobs
    }
    /// CheckEnabled/LaunchJob eventually attempts every matching namespace once.
    pub(super) fn groups(
        &self,
        agent_name: &str,
        job_type: i32,
        deployment: &str,
    ) -> Vec<WorkerKey> {
        self.inner
            .lock()
            .unwrap()
            .workers
            .values()
            .filter(|w| {
                w.registration.key.agent_name == agent_name
                    && w.registration.key.job_type == job_type
                    && w.registration.key.deployment == deployment
            })
            .map(|w| w.registration.key.clone())
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect()
    }
    /// Fixed AgentHandler selection. sample is a production RNG's [0,1) float.
    pub(super) fn select(
        &self,
        key: &WorkerKey,
        attempted: &HashSet<String>,
        sample: f32,
    ) -> Result<String, WorkerError> {
        let state = self.inner.lock().unwrap();
        let workers = state
            .registration_order
            .iter()
            .filter_map(|id| state.workers.get(id))
            .filter(|w| &w.registration.key == key)
            .collect::<Vec<_>>();
        if workers.is_empty() {
            return Err(WorkerError::NoWorkers);
        }
        let weights = workers
            .iter()
            .filter(|w| !attempted.contains(&w.registration.worker_id) && w.status == 0)
            .map(|w| {
                let value = 1. - w.load;
                (*w, if value.is_nan() { value } else { value.max(0.) })
            })
            .collect::<Vec<_>>();
        let sum = weights.iter().map(|(_, weight)| weight).sum::<f32>();
        if sum == 0. {
            return Err(WorkerError::NoCapacity);
        }
        let mut threshold = sample * sum;
        for (worker, weight) in weights {
            threshold -= weight;
            if threshold <= 0. {
                return Ok(worker.registration.worker_id.clone());
            }
        }
        // Match the source's fallback, including exceptional non-finite loads.
        Ok(workers[0].registration.worker_id.clone())
    }
    pub(super) fn close_all(&self) {
        self.endpoints.close_all();
        let mut state = self.inner.lock().unwrap();
        state.registration_order.clear();
        state.workers.clear();
        self.changed();
    }
    #[cfg(test)]
    pub(super) fn oracle_snapshot(&self, id: &str) -> (i32, f32, bool, usize) {
        let state = self.inner.lock().unwrap();
        let worker = state
            .workers
            .get(id)
            .expect("actual registered oracle worker");
        (
            worker.status,
            worker.load,
            worker.draining,
            worker.jobs.len(),
        )
    }
    pub fn registration(&self, id: &str) -> Option<Registration> {
        self.inner
            .lock()
            .unwrap()
            .workers
            .get(id)
            .map(|w| w.registration.clone())
    }
    pub fn update_worker(
        &self,
        id: &str,
        update: &pb::UpdateWorkerStatus,
    ) -> Result<(), WorkerError> {
        let mut state = self.inner.lock().unwrap();
        let w = state.workers.get_mut(id).ok_or(WorkerError::Closed)?;
        if update.seq != 0 {
            if update.seq <= w.status_seq {
                return Ok(());
            }
            w.status_seq = update.seq;
        }
        if let Some(status) = update.status {
            w.status = status;
        }
        w.load = update.load;
        w.draining = update.draining;
        self.endpoints.drain(id, update.draining);
        self.changed();
        Ok(())
    }
    pub fn availability(
        &self,
        id: &str,
        response: pb::AvailabilityResponse,
    ) -> Result<(), WorkerError> {
        let mut state = self.inner.lock().unwrap();
        let w = state.workers.get_mut(id).ok_or(WorkerError::Closed)?;
        if let Some(pending) = w.pending.remove(&response.job_id) {
            let _ = pending.response.send(response);
        }
        Ok(())
    }
    pub fn job_state(&self, id: &str, job: &str) -> Result<pb::JobState, WorkerError> {
        self.inner
            .lock()
            .unwrap()
            .workers
            .get(id)
            .and_then(|w| w.jobs.get(job))
            .and_then(|j| j.state.clone())
            .ok_or(WorkerError::JobNotFound)
    }
    pub fn update_job(
        &self,
        id: &str,
        update: &pb::UpdateJobStatus,
        now_ns: i64,
    ) -> Result<pb::JobState, WorkerError> {
        let mut state = self.inner.lock().unwrap();
        let w = state.workers.get_mut(id).ok_or(WorkerError::Closed)?;
        let job = w
            .jobs
            .get_mut(&update.job_id)
            .ok_or(WorkerError::UpdateNotFound)?;
        let s = job.state.get_or_insert_with(Default::default);
        s.updated_at = now_ns;
        if s.status == 0 && update.status != 0 {
            s.started_at = now_ns;
        }
        s.status = update.status;
        s.error = update.error.clone();
        if matches!(update.status, 2 | 3) {
            s.ended_at = now_ns;
        }
        let result = s.clone();
        if matches!(update.status, 2 | 3) {
            w.jobs.remove(&update.job_id);
            w.job_generations.remove(&update.job_id);
            w.ready_jobs.remove(&update.job_id);
        }
        self.changed();
        Ok(result)
    }
    // Called only after an actual dispatch/room generation is selected. The token
    // factory must mint the official room-join agent token; no placeholder token.
    pub async fn assign<F>(
        &self,
        id: &str,
        mut job: pb::Job,
        now_ns: i64,
        token: F,
    ) -> Result<pb::JobState, WorkerError>
    where
        F: FnOnce(
            &Registration,
            &pb::Job,
            &pb::AvailabilityResponse,
        ) -> Result<String, WorkerError>,
    {
        let (outbound, registration, receiver, generation) = {
            let mut state = self.inner.lock().unwrap();
            state.next_pending = state.next_pending.wrapping_add(1);
            let generation = state.next_pending;
            let w = state.workers.get_mut(id).ok_or(WorkerError::Closed)?;
            if w.pending.contains_key(&job.id) {
                return Err(WorkerError::DuplicateJob);
            }
            if w.pending.len() >= self.limits.pending_per_worker
                || w.jobs.len() >= self.limits.jobs_per_worker
            {
                return Err(WorkerError::Capacity);
            }
            let (send, recv) = oneshot::channel();
            w.pending.insert(
                job.id.clone(),
                Pending {
                    generation,
                    response: send,
                },
            );
            (w.outbound.clone(), w.registration.clone(), recv, generation)
        };
        let _guard = PendingGuard {
            registry: self.clone(),
            worker: id.into(),
            job: job.id.clone(),
            generation,
        };
        let s = job.state.get_or_insert_with(Default::default);
        s.worker_id = id.into();
        s.agent_id = registration.agent_id.clone();
        s.updated_at = now_ns;
        s.started_at = now_ns;
        s.status = 1;
        send_message(
            &outbound,
            pb::ServerMessage {
                message: Some(pb::server_message::Message::Availability(
                    pb::AvailabilityRequest {
                        job: Some(job.clone()),
                        resuming: false,
                    },
                )),
            },
        )
        .await?;
        let response = tokio::time::timeout(ASSIGN_TIMEOUT, receiver)
            .await
            .map_err(|_| WorkerError::AvailabilityTimeout)?
            .map_err(|_| WorkerError::Closed)?;
        if response.terminate {
            let s = job.state.as_mut().unwrap();
            s.ended_at = now_ns;
            s.status = 2;
            return Ok(s.clone());
        }
        if !response.available {
            return Err(WorkerError::Unavailable);
        }
        job.state.as_mut().unwrap().participant_identity = response.participant_identity.clone();
        let token = token(&registration, &job, &response)?;
        let result = job.state.clone().unwrap();
        {
            let mut state = self.inner.lock().unwrap();
            let w = state.workers.get_mut(id).ok_or(WorkerError::Closed)?;
            w.jobs.insert(job.id.clone(), job.clone());
            w.job_generations.insert(job.id.clone(), generation);
        }
        let job_id = job.id.clone();
        let mut running_guard = RunningGuard {
            registry: self.clone(),
            worker: id.into(),
            job: job_id.clone(),
            generation,
            committed: false,
        };
        if send_message(
            &outbound,
            pb::ServerMessage {
                message: Some(pb::server_message::Message::Assignment(pb::JobAssignment {
                    job: Some(job),
                    url: None,
                    token,
                })),
            },
        )
        .await
        .is_err()
        {
            return Err(WorkerError::Closed);
        }
        running_guard.committed = true;
        if let Some(worker) = self.inner.lock().unwrap().workers.get_mut(id) {
            if worker.job_generations.get(&job_id) == Some(&generation) {
                worker.ready_jobs.insert(job_id);
            }
        }
        self.changed();
        Ok(result)
    }
    pub async fn terminate(
        &self,
        id: &str,
        job: &str,
        agent_left: bool,
        now_ns: i64,
    ) -> Result<pb::JobState, WorkerError> {
        let outbound = {
            let state = self.inner.lock().unwrap();
            let w = state.workers.get(id).ok_or(WorkerError::Closed)?;
            if !w.jobs.contains_key(job) {
                return Err(WorkerError::JobNotFound);
            }
            w.outbound.clone()
        };
        let write = send_message(
            &outbound,
            pb::ServerMessage {
                message: Some(pb::server_message::Message::Termination(
                    pb::JobTermination { job_id: job.into() },
                )),
            },
        )
        .await;
        let state = self.update_job(
            id,
            &pb::UpdateJobStatus {
                job_id: job.into(),
                status: if agent_left { 3 } else { 2 },
                error: if agent_left {
                    "agent worker left the room".into()
                } else {
                    String::new()
                },
            },
            now_ns,
        )?;
        write?;
        Ok(state)
    }
}

#[cfg(test)]
mod tests;
