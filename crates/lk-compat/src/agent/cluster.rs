//! Fixed AgentInternal server/client and production group/owner lifecycle.
//! Topics and payloads are the public pinned rpc/agent.proto contract.
use super::{
    registry::{Registry, WorkerError, WorkerKey},
    token,
};
use crate::{
    auth::TokenMinter,
    service_bus::{
        channels,
        redis_transport::SubscriptionEvent,
        rpc::{
            CallOptions, Cancellation, MethodSpec, RequestAffinity, RpcClient, RpcError, RpcServer,
            ServerLimits,
        },
        rpc_transport::RpcBus,
    },
};
use lk_protocol::{livekit as pb, rpc};
use std::{
    collections::{BTreeSet, HashMap},
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};
use tokio::{
    sync::{Mutex as AsyncMutex, mpsc, watch},
    task::{JoinHandle, JoinSet},
};

#[derive(Clone, Hash, PartialEq, Eq)]
struct Key {
    method: &'static str,
    topics: Vec<String>,
}
fn spec(method: &str, topics: Vec<String>) -> MethodSpec {
    MethodSpec {
        service: "AgentInternal".into(),
        method: method.into(),
        topics,
        // Generated RegisterMethod arguments are affinity,multi,claim,queue.
        // Both job methods require a claim; Terminate is queued with default
        // affinity rather than using the JobRequest worker-load function.
        require_claim: matches!(method, "JobRequest" | "JobTerminate"),
        queue: method == "JobTerminate",
    }
}
pub(super) fn topic(name: &str, namespace: &str, deployment: &str) -> String {
    let mut value = if name.is_empty() {
        namespace.to_string()
    } else if namespace.is_empty() {
        name.to_string()
    } else {
        format!("{name}_{namespace}")
    };
    if !deployment.is_empty() {
        value.push('_');
        value.push_str(deployment);
    }
    value
}
fn job_topics(key: &WorkerKey) -> Vec<String> {
    vec![
        topic(&key.agent_name, &key.namespace, &key.deployment),
        match key.job_type {
            0 => "room",
            1 => "publisher",
            _ => "participant",
        }
        .into(),
    ]
}
fn job_key(job: &pb::Job) -> WorkerKey {
    WorkerKey {
        agent_name: job.agent_name.clone(),
        namespace: job.namespace.clone(),
        job_type: job.r#type,
        deployment: job.deployment.clone(),
    }
}
fn limits() -> ServerLimits {
    ServerLimits {
        concurrent: 1024,
        max_request_duration: Duration::from_secs(3),
        allow_skip_claim: false,
    }
}
fn now_ns() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos()
        .min(i64::MAX as u128) as i64
}
fn worker_error(error: WorkerError) -> RpcError {
    let code = match error {
        WorkerError::NoWorkers | WorkerError::NoCapacity => "resource_exhausted",
        WorkerError::Unavailable | WorkerError::Closed => "unavailable",
        WorkerError::AvailabilityTimeout => "deadline_exceeded",
        WorkerError::DuplicateJob => "already_exists",
        WorkerError::JobNotFound | WorkerError::UpdateNotFound => "not_found",
        WorkerError::InvalidRegistration(_) | WorkerError::UnknownJobType => "invalid_argument",
        _ => "internal",
    };
    RpcError::new(code, error.to_string())
}
struct Cache {
    at: Instant,
    epoch: u64,
    responses: watch::Receiver<Vec<rpc::CheckEnabledResponse>>,
}
pub(super) struct Discovery {
    incoming: mpsc::Receiver<WorkerKey>,
    stop: Cancellation,
}
impl Discovery {
    pub async fn recv(&mut self) -> Option<WorkerKey> {
        self.incoming.recv().await
    }
}
impl Drop for Discovery {
    fn drop(&mut self) {
        self.stop.cancel();
    }
}

pub(crate) struct Cluster {
    registry: Registry,
    tokens: Arc<TokenMinter>,
    target_load: f32,
    bus: RpcBus,
    client: Arc<RpcClient>,
    servers: AsyncMutex<HashMap<Key, RpcServer>>,
    stop: Cancellation,
    cache: Mutex<Option<Cache>>,
    cache_epoch: AtomicU64,
    tasks: Mutex<Vec<JoinHandle<()>>>,
    jobs: Mutex<JoinSet<()>>,
    retiring: Mutex<JoinSet<()>>,
}
impl Cluster {
    pub async fn start(
        registry: Registry,
        tokens: Arc<TokenMinter>,
        target_load: f32,
        bus: RpcBus,
    ) -> Result<Arc<Self>, RpcError> {
        let bus = bus.with_inbox_size(100)?;
        let client = Arc::new(RpcClient::new(bus.clone(), 4096)?);
        let cluster = Arc::new(Self {
            registry,
            tokens,
            target_load,
            bus: bus.clone(),
            client,
            servers: AsyncMutex::new(HashMap::new()),
            stop: Cancellation::default(),
            cache: Mutex::new(None),
            cache_epoch: AtomicU64::new(0),
            tasks: Mutex::new(vec![]),
            jobs: Mutex::new(JoinSet::new()),
            retiring: Mutex::new(JoinSet::new()),
        });
        let registry = cluster.registry.clone();
        let check =
            RpcServer::register_multi::<rpc::CheckEnabledRequest, rpc::CheckEnabledResponse, _, _>(
                bus.clone(),
                spec("CheckEnabled", vec![]),
                // CheckEnabled explicitly uses a five-second client deadline in
                // the fixed Go consumer, beyond the ordinary three-second call.
                ServerLimits {
                    max_request_duration: Duration::from_secs(5),
                    ..limits()
                },
                move |_, _| {
                    let keys = registry.all_groups();
                    async move {
                        Ok(rpc::CheckEnabledResponse {
                            room_enabled: keys.iter().any(|k| k.job_type == 0),
                            publisher_enabled: keys.iter().any(|k| k.job_type == 1),
                            participant_enabled: keys.iter().any(|k| k.job_type == 2),
                            namespaces: keys
                                .iter()
                                .map(|k| k.namespace.clone())
                                .collect::<BTreeSet<_>>()
                                .into_iter()
                                .collect(),
                            agent_names: keys
                                .iter()
                                .map(|k| k.agent_name.clone())
                                .collect::<BTreeSet<_>>()
                                .into_iter()
                                .collect(),
                        })
                    }
                },
            )
            .await?;
        cluster.servers.lock().await.insert(
            Key {
                method: "CheckEnabled",
                topics: vec![],
            },
            check,
        );
        if let Err(error) = cluster.reconcile().await {
            cluster.shutdown().await;
            return Err(error);
        }
        let registered = channels::request("AgentInternal", "WorkerRegistered", &[""], false);
        let mut sub = match bus
            .subscribe::<pbjson_types::Empty>(&registered, false)
            .await
        {
            Ok(sub) => sub,
            Err(error) => {
                cluster.shutdown().await;
                return Err(error.into());
            }
        };
        let stop = cluster.stop.clone();
        let weak = Arc::downgrade(&cluster);
        let subscribing_bus = bus.clone();
        let invalidate = tokio::spawn(async move {
            loop {
                let bytes =
                    tokio::select! {_=stop.cancelled()=>break,bytes=sub.recv_event()=>bytes};
                match bytes {
                    Ok(SubscriptionEvent::Message(bytes)) => {
                        if subscribing_bus
                            .decode::<pbjson_types::Empty>(&bytes)
                            .is_ok()
                        {
                            if let Some(c) = weak.upgrade() {
                                c.invalidate_discovery();
                            } else {
                                break;
                            }
                        }
                    }
                    Ok(SubscriptionEvent::Disconnected | SubscriptionEvent::Restored) => {
                        // Redis Pub/Sub has no replay. Invalidate both when the
                        // gap begins and after acknowledged replacement, because
                        // WorkerRegistered publications may be lost in between.
                        if let Some(c) = weak.upgrade() {
                            c.invalidate_discovery();
                        } else {
                            break;
                        }
                    }
                    Err(error) => {
                        tracing::error!(%error,"agent WorkerRegistered subscription failed");
                        break;
                    }
                }
            }
            if let Err(error) = sub.unsubscribe(&registered).await {
                tracing::error!(%error,"agent WorkerRegistered unsubscribe failed");
            }
        });
        let stop = cluster.stop.clone();
        let weak = Arc::downgrade(&cluster);
        let mut changed = cluster.registry.changes();
        let controller = tokio::spawn(async move {
            loop {
                tokio::select! {_=stop.cancelled()=>break,change=changed.changed()=>if change.is_err(){break;}}
                // A registry change and cancellation can both be ready; select! picks
                // arbitrarily, so shutdown must win here rather than report the
                // deliberate closing refusal as a reconciliation failure.
                if stop.is_cancelled() {
                    break;
                }
                let Some(c) = weak.upgrade() else {
                    break;
                };
                if let Err(error) = c.reconcile().await {
                    tracing::error!(%error,"agent RPC registration reconciliation failed");
                }
                c.reap();
            }
        });
        cluster
            .tasks
            .lock()
            .unwrap()
            .extend([invalidate, controller]);
        cluster.registry.set_cluster(&cluster);
        Ok(cluster)
    }
    fn reap(&self) {
        while self.jobs.lock().unwrap().try_join_next().is_some() {}
        while self.retiring.lock().unwrap().try_join_next().is_some() {}
    }
    pub fn reconcile(
        self: &Arc<Self>,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<(), RpcError>> + Send + '_>>
    {
        Box::pin(async move {
            if self.stop.is_cancelled() {
                return Err(RpcError::new("unavailable", "agent RPC service is closing"));
            }
            let mut wanted = HashMap::new();
            for key in self.registry.all_groups() {
                let route = Key {
                    method: "JobRequest",
                    topics: job_topics(&key),
                };
                if let Some(old) = wanted.insert(route, key.clone()) {
                    if old != key {
                        return Err(RpcError::new(
                            "already_exists",
                            "agent topic collides with another worker group",
                        ));
                    }
                }
            }
            let jobs = self.registry.running_jobs();
            let mut servers = self.servers.lock().await;
            let removed: Vec<_> = servers
                .keys()
                .filter(|k| {
                    k.method == "JobRequest" && !wanted.contains_key(*k)
                        || k.method == "JobTerminate"
                            && !jobs
                                .iter()
                                .any(|id| k.topics.len() == 1 && k.topics.first() == Some(id))
                })
                .cloned()
                .collect();
            for key in removed {
                if let Some(server) = servers.remove(&key) {
                    let retirement = server.retire().await?;
                    self.retiring.lock().unwrap().spawn(async move {
                        retirement.finish().await;
                    });
                }
            }
            for key in wanted.keys() {
                if servers.contains_key(key) {
                    continue;
                }
                let weak = Arc::downgrade(self);
                let registry = self.registry.clone();
                let target = self.target_load;
                let affinity: RequestAffinity<pb::Job> =
                    Arc::new(move |job| registry.affinity(&job_key(job), target));
                let server=RpcServer::register::<pb::Job,rpc::JobRequestResponse,_,_>(self.bus.clone(),spec(key.method,key.topics.clone()),limits(),move |_,job|{
                let weak=weak.clone();async move {let c=weak.upgrade().ok_or_else(||RpcError::new("unavailable","agent service closed"))?;let state=c.assign_local(job).await?;
                    // Go keeps a genuinely running job even if registering its
                    // terminate topic fails; do not replace its positive state.
                    if let Err(error)=c.reconcile().await {tracing::error!(%error,"failed to register agent job terminate handler");}
                    Ok(rpc::JobRequestResponse{state:Some(state)})}
            },Some(affinity)).await?;
                servers.insert(key.clone(), server);
                if let Err(error) = self
                    .bus
                    .publish(
                        &channels::request("AgentInternal", "WorkerRegistered", &[""], false),
                        &pbjson_types::Empty {},
                    )
                    .await
                {
                    tracing::error!(%error,"failed to publish agent WorkerRegistered");
                }
            }
            for id in jobs {
                let key = Key {
                    method: "JobTerminate",
                    topics: vec![id],
                };
                if servers.contains_key(&key) {
                    continue;
                }
                let registry = self.registry.clone();
                let server = RpcServer::register::<
                    rpc::JobTerminateRequest,
                    rpc::JobTerminateResponse,
                    _,
                    _,
                >(
                    self.bus.clone(),
                    spec(key.method, key.topics.clone()),
                    limits(),
                    move |_, request| {
                        let registry = registry.clone();
                        async move {
                            let worker = registry
                                .worker_for_job(&request.job_id)
                                .ok_or_else(|| RpcError::new("not_found", "no worker for jobID"))?;
                            let state = registry
                                .terminate(&worker, &request.job_id, request.reason == 1, now_ns())
                                .await
                                .map_err(worker_error)?;
                            Ok(rpc::JobTerminateResponse { state: Some(state) })
                        }
                    },
                    None,
                )
                .await?;
                servers.insert(key, server);
            }
            Ok(())
        })
    }
    async fn assign_local(&self, job: pb::Job) -> Result<pb::JobState, RpcError> {
        let key = job_key(&job);
        let mut attempted = std::collections::HashSet::new();
        loop {
            let mut random = [0u8; 4];
            getrandom::fill(&mut random).map_err(|e| RpcError::new("internal", e.to_string()))?;
            let sample = (u32::from_le_bytes(random) >> 8) as f32 / (1u32 << 24) as f32;
            let id = self
                .registry
                .select(&key, &attempted, sample)
                .map_err(worker_error)?;
            attempted.insert(id.clone());
            match self
                .registry
                .assign(&id, job.clone(), now_ns(), |registration, job, response| {
                    token::mint_assignment(
                        &self.tokens,
                        &registration.api_key,
                        registration,
                        job,
                        response,
                        rtc_core::util::unix_ms() / 1000,
                    )
                })
                .await
            {
                Ok(state) => return Ok(state),
                Err(WorkerError::Closed | WorkerError::Unavailable) => continue,
                Err(error) => return Err(worker_error(error)),
            }
        }
    }
    fn invalidate_discovery(&self) {
        let mut cache = self.cache.lock().unwrap();
        self.cache_epoch.fetch_add(1, Ordering::AcqRel);
        // Current waiters keep their own generation, as the fixed Go
        // dispatchers do. Only new discoveries see the replacement cache.
        *cache = None;
    }

    pub fn discover(self: &Arc<Self>, agent: String, kind: i32, deployment: String) -> Discovery {
        self.reap();
        let stop = Cancellation::default();
        let (out, incoming) = mpsc::channel(128);
        let mut jobs = self.jobs.lock().unwrap();
        if !self.stop.is_cancelled() {
            let mut cache = self.cache.lock().unwrap();
            let epoch = self.cache_epoch.load(Ordering::Acquire);
            if !cache.as_ref().is_some_and(|current| {
                current.epoch == epoch && current.at.elapsed() < Duration::from_secs(60)
            }) {
                let (updates, responses) = watch::channel(Vec::new());
                jobs.spawn(self.clone().collect_discovery(updates));
                *cache = Some(Cache {
                    at: Instant::now(),
                    epoch,
                    responses,
                });
            }
            let cache = cache.as_ref().unwrap();
            jobs.spawn(self.clone().discover_into(
                agent,
                kind,
                deployment,
                out,
                stop.clone(),
                cache.responses.clone(),
            ));
        }
        Discovery { incoming, stop }
    }

    async fn collect_discovery(
        self: Arc<Self>,
        responses: watch::Sender<Vec<rpc::CheckEnabledResponse>>,
    ) {
        let mut options = CallOptions::new(Duration::from_secs(5));
        options.cancel = self.stop.clone();
        let mut call = match self
            .client
            .multi::<rpc::CheckEnabledRequest, rpc::CheckEnabledResponse>(
                &spec("CheckEnabled", vec![]),
                &rpc::CheckEnabledRequest {},
                options,
            )
            .await
        {
            Ok(call) => call,
            Err(error) => {
                tracing::error!(%error, "failed to check enabled agent workers");
                return;
            }
        };
        loop {
            let row = tokio::select! {
                _ = self.stop.cancelled() => break,
                row = call.recv() => row,
            };
            match row {
                Some(Ok(row)) => {
                    responses.send_modify(|rows| rows.push(row.message));
                }
                Some(Err(error)) => {
                    tracing::error!(%error, "agent CheckEnabled peer response failed")
                }
                None => break,
            }
        }
        call.close().await;
        // Dropping the sender finishes every subscriber, even on setup error
        // or task cancellation. It does not mutate a newer cache generation.
    }

    async fn discover_into(
        self: Arc<Self>,
        agent: String,
        kind: i32,
        deployment: String,
        out: mpsc::Sender<WorkerKey>,
        child_stop: Cancellation,
        mut responses: watch::Receiver<Vec<rpc::CheckEnabledResponse>>,
    ) {
        let mut emitted = BTreeSet::new();
        loop {
            let snapshot = responses.borrow_and_update().clone();
            emit(
                &snapshot,
                &agent,
                kind,
                &deployment,
                &mut emitted,
                &out,
                &child_stop,
                &self.stop,
            )
            .await;
            let update = tokio::select! {
                _ = child_stop.cancelled() => break,
                _ = self.stop.cancelled() => break,
                update = responses.changed() => update,
            };
            if update.is_err() {
                break;
            }
        }
    }
    pub async fn request(&self, job: &pb::Job) -> Result<pb::JobState, WorkerError> {
        // Generated AgentInternal is constructed with no ClientParams retry
        // interceptor in the fixed Go graph; use the actual psrpc defaults.
        let mut options = CallOptions::new(Duration::from_secs(3));
        options.cancel = self.stop.clone();
        options.selection.affinity_timeout = Some(Duration::from_secs(1));
        options.selection.short_circuit_timeout = Some(Duration::from_millis(200));
        self.client
            .unary::<pb::Job, rpc::JobRequestResponse>(
                &spec("JobRequest", job_topics(&job_key(job))),
                job,
                options,
            )
            .await
            .map_err(|e| WorkerError::Cluster(e.to_string()))?
            .state
            .ok_or_else(|| WorkerError::Cluster("agent job response has no state".into()))
    }
    pub async fn terminate(&self, id: &str, agent_left: bool) -> Result<pb::JobState, WorkerError> {
        let mut options = CallOptions::new(Duration::from_secs(3));
        options.cancel = self.stop.clone();
        // Fixed psrpc/client/options.go chooses first available for methods
        // without an affinity function, even though queue methods still claim.
        // Waiting the entire request deadline here grants an already-expired
        // request and prevents the genuine worker termination from executing.
        options.selection.accept_first = true;
        options.selection.affinity_timeout = Some(Duration::from_secs(1));
        self.client
            .unary::<rpc::JobTerminateRequest, rpc::JobTerminateResponse>(
                &spec("JobTerminate", vec![id.into()]),
                &rpc::JobTerminateRequest {
                    job_id: id.into(),
                    reason: if agent_left { 1 } else { 0 },
                },
                options,
            )
            .await
            .map_err(|e| WorkerError::Cluster(e.to_string()))?
            .state
            .ok_or_else(|| WorkerError::Cluster("agent termination response has no state".into()))
    }
    pub fn cancel(&self) {
        self.stop.cancel();
        self.client.close();
    }
    pub async fn shutdown(&self) {
        self.cancel();
        let tasks = std::mem::take(&mut *self.tasks.lock().unwrap());
        for task in tasks {
            let _ = task.await;
        }
        let servers = std::mem::take(&mut *self.servers.lock().await);
        for (_, server) in servers {
            server.close().await;
        }
        let mut jobs = std::mem::take(&mut *self.jobs.lock().unwrap());
        while jobs.join_next().await.is_some() {}
        let mut retiring = std::mem::take(&mut *self.retiring.lock().unwrap());
        while retiring.join_next().await.is_some() {}
    }
}
async fn emit(
    responses: &[rpc::CheckEnabledResponse],
    agent: &str,
    kind: i32,
    deployment: &str,
    emitted: &mut BTreeSet<String>,
    out: &mpsc::Sender<WorkerKey>,
    stop: &Cancellation,
    closed: &Cancellation,
) {
    let relevant: Vec<_> = responses
        .iter()
        .filter(|r| match kind {
            0 => r.room_enabled,
            1 => r.publisher_enabled,
            2 => r.participant_enabled,
            _ => false,
        })
        .collect();
    if !agent.is_empty()
        && !relevant
            .iter()
            .any(|r| r.agent_names.iter().any(|a| a == agent))
    {
        return;
    }
    for namespace in relevant.iter().flat_map(|r| &r.namespaces) {
        if !emitted.insert(namespace.clone()) {
            continue;
        }
        let key = WorkerKey {
            agent_name: agent.into(),
            namespace: namespace.clone(),
            job_type: kind,
            deployment: deployment.into(),
        };
        tokio::select! {_=stop.cancelled()=>return,_=closed.cancelled()=>return,sent=out.send(key)=>if sent.is_err(){return;}}
    }
}
impl Drop for Cluster {
    fn drop(&mut self) {
        self.cancel();
        for task in self.tasks.get_mut().unwrap().drain(..) {
            task.abort();
        }
        self.jobs.get_mut().unwrap().abort_all();
        self.retiring.get_mut().unwrap().abort_all();
    }
}

#[cfg(test)]
mod tests;
