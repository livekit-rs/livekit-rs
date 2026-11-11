//! One owner translates runtime effects and genuine worker results. No speculative
//! successful job is stored, and registration never relaunches past dispatches.
use super::{
    registry::{Registry, WorkerError, WorkerKey},
    websocket::WorkerEvent,
};
use crate::auth::TokenMinter;
use lk_protocol::livekit as pb;
use rtc_core::ids::{Identity, RoomKey, RoomSid};
use rtc_runtime::{
    RoomManager,
    api::{AgentJobRecord, AgentJobState, AgentWorkEvent},
};
use std::{
    collections::{HashMap, HashSet, VecDeque},
    sync::Arc,
};
use tokio::{
    sync::{mpsc, watch},
    task::JoinSet,
};
#[derive(Clone)]
struct Launch {
    key: RoomKey,
    room_sid: RoomSid,
    job: pb::Job,
    group: WorkerKey,
    // One real accepted launch/discovery lifetime. All queue/task/commit clones
    // retain this receipt; panic/cancel/error Drop releases it without a counter.
    receipt: Arc<()>,
}
#[derive(Clone)]
struct Assigned {
    context: Launch,
    worker: String,
    via_rpc: bool,
    job: pb::Job,
}
#[derive(Clone)]
struct Owned {
    key: RoomKey,
    room_sid: RoomSid,
    dispatch: String,
    worker: String,
    via_rpc: bool,
    identity: String,
}
pub(super) struct Bridge {
    manager: RoomManager,
    registry: Registry,
    tokens: Arc<TokenMinter>,
    runtime: mpsc::Receiver<AgentWorkEvent>,
    workers: mpsc::Receiver<WorkerEvent>,
    shutdown: watch::Receiver<bool>,
    enable_recording: bool,
    enable_redaction: bool,
    queue: VecDeque<Launch>,
    pub(super) launch_queue_limit: usize,
    pending: HashMap<String, Launch>,
    // The bridge retains exactly one base Arc per room-generation/dispatch.
    // Other strong owners are actual accepted discovery/launch/commit work.
    launch_receipts: HashMap<(RoomSid, String), Arc<()>>,
    task_jobs: HashMap<tokio::task::Id, String>,
    commit_jobs: HashMap<tokio::task::Id, String>,
    committing: HashMap<String, Assigned>,
    terminate_after_launch: HashSet<String>,
    tasks: JoinSet<(String, Result<Assigned, WorkerError>)>,
    discoveries: JoinSet<()>,
    discovered_tx: mpsc::Sender<Launch>,
    discovered: mpsc::Receiver<Launch>,
    commits: JoinSet<(String, Assigned, bool)>,
    cleanup: JoinSet<()>,
    cleanup_queue: VecDeque<(String, Owned, bool)>,
    owned: HashMap<String, Owned>,
    deleted: HashSet<(RoomSid, String)>,
    closed: HashSet<RoomSid>,
    ended_before_assignment: HashSet<String>,
}
impl Bridge {
    pub(super) fn new(
        manager: RoomManager,
        registry: Registry,
        tokens: Arc<TokenMinter>,
        runtime: mpsc::Receiver<AgentWorkEvent>,
        workers: mpsc::Receiver<WorkerEvent>,
        shutdown: watch::Receiver<bool>,
        config: super::service::ServiceConfig,
    ) -> Self {
        let (discovered_tx, discovered) = mpsc::channel(128);
        Self {
            manager,
            registry,
            tokens,
            runtime,
            workers,
            shutdown,
            enable_recording: config.enable_recording,
            enable_redaction: config.enable_redaction,
            queue: VecDeque::new(),
            launch_queue_limit: 4096,
            pending: HashMap::new(),
            launch_receipts: HashMap::new(),
            task_jobs: HashMap::new(),
            commit_jobs: HashMap::new(),
            committing: HashMap::new(),
            terminate_after_launch: HashSet::new(),
            tasks: JoinSet::new(),
            discoveries: JoinSet::new(),
            discovered_tx,
            discovered,
            commits: JoinSet::new(),
            cleanup: JoinSet::new(),
            cleanup_queue: VecDeque::new(),
            owned: HashMap::new(),
            deleted: HashSet::new(),
            closed: HashSet::new(),
            ended_before_assignment: HashSet::new(),
        }
    }
    pub(super) async fn run(mut self) {
        let mut runtime_open = true;
        let mut workers_open = true;
        loop {
            while self.tasks.len() + self.commits.len() < 50 {
                let Some(context) = self.queue.pop_front() else {
                    break;
                };
                if self.closed.contains(&context.room_sid) {
                    continue;
                }
                let registry = self.registry.clone();
                let tokens = self.tokens.clone();
                let id = context.job.id.clone();
                self.pending.insert(id.clone(), context.clone());
                let task_id = id.clone();
                let task = self
                    .tasks
                    .spawn(async move { (id, assign(registry, tokens, context).await) });
                self.task_jobs.insert(task.id(), task_id);
            }
            self.release_completed_dispatches();
            while self.cleanup.len() < 50 {
                let Some((id, owned, agent_left)) = self.cleanup_queue.pop_front() else {
                    break;
                };
                let manager = self.manager.clone();
                let registry = self.registry.clone();
                self.cleanup
                    .spawn(cleanup_job(manager, registry, id, owned, agent_left));
            }
            tokio::select! {
             _=self.shutdown.changed()=>{break;},
             event=self.runtime.recv(),if runtime_open&&self.queue.len()<self.launch_queue_limit&&self.discoveries.len()<50=>match event {Some(event)=>self.runtime_event(event),None=>runtime_open=false},
             event=self.workers.recv(),if workers_open=>match event {Some(event)=>self.worker_event(event),None=>workers_open=false},
             discovered=self.discovered.recv(),if self.queue.len()<self.launch_queue_limit&&(!self.discoveries.is_empty()||!self.discovered.is_empty())=>if let Some(context)=discovered {if !self.closed.contains(&context.room_sid){self.queue.push_back(context);}},
             _=self.discoveries.join_next(),if !self.discoveries.is_empty()=>{},
             done=self.tasks.join_next_with_id(),if !self.tasks.is_empty()=>{match done {
                Some(Ok((task,(id,result))))=>{self.task_jobs.remove(&task);match result {
                    Ok(assigned)=>self.begin_commit(assigned),
                    Err(error)=>{tracing::warn!(job_id=%id,%error,"agent job launch failed before state commit");self.pending.remove(&id);self.ended_before_assignment.remove(&id);}
                }},
                Some(Err(error))=>{if let Some(id)=self.task_jobs.remove(&error.id()){self.pending.remove(&id);self.ended_before_assignment.remove(&id);}tracing::error!(%error,"agent job launch task failed");},
                None=>{},
             }},
             done=self.commits.join_next_with_id(),if !self.commits.is_empty()=>{match done {
                Some(Ok((task,(id,assigned,stored))))=>{self.commit_jobs.remove(&task);self.committing.remove(&id);self.pending.remove(&id);self.committed(assigned,stored);self.ended_before_assignment.remove(&id);},
                Some(Err(error))=>{if let Some(id)=self.commit_jobs.remove(&error.id()){self.pending.remove(&id);if let Some(assigned)=self.committing.remove(&id){self.committed(assigned,false);}self.ended_before_assignment.remove(&id);}tracing::error!(%error,"agent job state commit task failed");},
                None=>{},
             }},
             _=self.cleanup.join_next(),if !self.cleanup.is_empty()=>{},
             else=>break,
            }
            self.reap_tombstones();
        }
        self.discoveries.abort_all();
        self.tasks.abort_all();
        self.commits.abort_all();
        self.cleanup.abort_all();
        // Complete actual abort/drop receipts before returning service shutdown;
        // discovery and accepted launch ownership cannot leak into a new room.
        while self.discoveries.join_next().await.is_some() {}
        while self.tasks.join_next().await.is_some() {}
        while self.commits.join_next().await.is_some() {}
        while self.cleanup.join_next().await.is_some() {}
        self.queue.clear();
        self.pending.clear();
        self.committing.clear();
        self.launch_receipts.clear();
    }
    fn cancelled(&self, context: &Launch) -> bool {
        self.closed.contains(&context.room_sid)
            || self
                .deleted
                .contains(&(context.room_sid.clone(), context.job.dispatch_id.clone()))
    }
    fn runtime_event(&mut self, event: AgentWorkEvent) {
        match event {
            AgentWorkEvent::Launch(work) => {
                let rtc_runtime::api::AgentLaunch {
                    key,
                    room,
                    dispatch,
                    participant,
                    job_type,
                } = *work;
                if self.closed.contains(&room.sid)
                    || self
                        .deleted
                        .contains(&(room.sid.clone(), dispatch.id.clone()))
                {
                    return;
                }
                let receipt = self
                    .launch_receipts
                    .entry((room.sid.clone(), dispatch.id.clone()))
                    .or_insert_with(|| Arc::new(()))
                    .clone();
                let template = pb::Job {
                    dispatch_id: dispatch.id.clone(),
                    r#type: job_type,
                    room: Some(crate::convert::room_info_to_proto(&room)),
                    participant: participant
                        .as_ref()
                        .map(crate::convert::participant_info_to_proto),
                    metadata: dispatch.metadata.clone(),
                    agent_name: dispatch.agent_name.clone(),
                    deployment: dispatch.deployment.clone(),
                    attributes: dispatch.attributes.clone().into_iter().collect(),
                    enable_recording: self.enable_recording,
                    enable_redaction: self.enable_redaction,
                    ..Default::default()
                };
                if let Some(cluster) = self.registry.cluster() {
                    let tx = self.discovered_tx.clone();
                    self.discoveries.spawn(async move {
                        let mut discovery =
                            cluster.discover(dispatch.agent_name, job_type, dispatch.deployment);
                        while let Some(group) = discovery.recv().await {
                            let mut job = template.clone();
                            job.id = rtc_core::ids::new_sid("AJ_");
                            job.namespace = group.namespace.clone();
                            if tx
                                .send(Launch {
                                    key: key.clone(),
                                    room_sid: room.sid.clone(),
                                    job,
                                    group,
                                    receipt: receipt.clone(),
                                })
                                .await
                                .is_err()
                            {
                                break;
                            }
                        }
                    });
                } else {
                    for group in
                        self.registry
                            .groups(&dispatch.agent_name, job_type, &dispatch.deployment)
                    {
                        let mut job = template.clone();
                        job.id = rtc_core::ids::new_sid("AJ_");
                        job.namespace = group.namespace.clone();
                        self.queue.push_back(Launch {
                            key: key.clone(),
                            room_sid: room.sid.clone(),
                            job,
                            group,
                            receipt: receipt.clone(),
                        });
                    }
                }
            }
            AgentWorkEvent::DeleteDispatch {
                room_sid,
                dispatch_id,
                ..
            } => {
                self.deleted.insert((room_sid.clone(), dispatch_id.clone()));
                let ids = self
                    .owned
                    .iter()
                    .filter(|(_, o)| o.room_sid == room_sid && o.dispatch == dispatch_id)
                    .map(|(id, _)| id.clone())
                    .collect::<Vec<_>>();
                self.terminate_after_launch.extend(ids);
            }
            AgentWorkEvent::RoomClosed { room_sid, .. } => {
                self.closed.insert(room_sid.clone());
                self.queue.retain(|c| c.room_sid != room_sid);
                let ids = self
                    .owned
                    .iter()
                    .filter(|(_, o)| o.room_sid == room_sid)
                    .map(|(id, _)| id.clone())
                    .collect::<Vec<_>>();
                // Room teardown remains independent. The original job's
                // asynchronous cleanup still owns its accepted launch.
                self.terminate_after_launch.extend(ids);
            }
            AgentWorkEvent::ParticipantLeft {
                room_sid, identity, ..
            } => {
                let ids = self
                    .owned
                    .iter()
                    .filter(|(_, o)| o.room_sid == room_sid && o.identity == identity.0)
                    .map(|(id, _)| id.clone())
                    .collect::<Vec<_>>();
                for id in ids {
                    self.terminate_owned(&id, true);
                }
            }
        }
    }
    fn worker_event(&mut self, event: WorkerEvent) {
        match event {
            WorkerEvent::Registered { .. } => {} // future LaunchJob discovers current groups; no speculation retry
            WorkerEvent::JobState { job_id, state, .. } => {
                if matches!(state.status, 2 | 3) {
                    self.owned.remove(&job_id);
                    self.terminate_after_launch.remove(&job_id);
                    if self.pending.contains_key(&job_id) {
                        self.ended_before_assignment.insert(job_id);
                    }
                }
            }
            WorkerEvent::Disconnected { worker_id, .. } => {
                self.owned.retain(|_, o| o.worker != worker_id)
            }
        }
    }
    fn begin_commit(&mut self, assigned: Assigned) {
        let manager = self.manager.clone();
        let cancelled = self.cancelled(&assigned.context);
        let id = assigned.job.id.clone();
        self.committing.insert(id.clone(), assigned.clone());
        let task = self.commits.spawn(async move {
            let stored = if cancelled { false } else {
                match manager.commit_agent_job(
                    &assigned.context.key,
                    assigned.context.room_sid.clone(),
                    to_record(&assigned.job),
                ).await {
                    Ok(()) => true,
                    Err(error) => {
                        tracing::warn!(job_id=%assigned.job.id,%error,"agent job running state commit failed");
                        false
                    }
                }
            };
            (assigned.job.id.clone(), assigned, stored)
        });
        self.commit_jobs.insert(task.id(), id);
    }
    fn committed(&mut self, assigned: Assigned, stored: bool) {
        let state = assigned.job.state.as_ref().expect("genuine assign state");
        let running = state.status == 1
            && !self.ended_before_assignment.contains(&assigned.job.id)
            && (assigned.via_rpc
                || self
                    .registry
                    .job_state(&assigned.worker, &assigned.job.id)
                    .is_ok());
        if !running {
            return;
        }
        let owned = Owned {
            key: assigned.context.key.clone(),
            room_sid: assigned.context.room_sid.clone(),
            dispatch: assigned.job.dispatch_id.clone(),
            worker: assigned.worker,
            via_rpc: assigned.via_rpc,
            identity: state.participant_identity.clone(),
        };
        if !stored || self.cancelled(&assigned.context) {
            // Go DeleteAgentDispatch waits the accepted LaunchJob, including
            // namespace discovery, before terminating its genuine late jobs.
            self.terminate_after_launch.insert(assigned.job.id.clone());
        }
        self.owned.insert(assigned.job.id, owned);
    }
    fn terminate_owned(&mut self, id: &str, agent_left: bool) {
        self.terminate_after_launch.remove(id);
        if let Some(owned) = self.owned.remove(id) {
            self.cleanup_queue
                .push_back((id.to_owned(), owned, agent_left));
        }
    }
    fn dispatch_launch_pending(&self, sid: &RoomSid, dispatch: &str) -> bool {
        self.launch_receipts
            .get(&(sid.clone(), dispatch.to_owned()))
            .is_some_and(|receipt| Arc::strong_count(receipt) > 1)
    }
    fn release_completed_dispatches(&mut self) {
        let ready = self
            .terminate_after_launch
            .iter()
            .filter(|id| {
                self.owned.get(*id).is_none_or(|owned| {
                    !self.dispatch_launch_pending(&owned.room_sid, &owned.dispatch)
                })
            })
            .cloned()
            .collect::<Vec<_>>();
        for id in ready {
            self.terminate_owned(&id, false);
        }
    }
    fn reap_tombstones(&mut self) {
        self.release_completed_dispatches();
        let active = self
            .launch_receipts
            .iter()
            .filter(|(_, receipt)| Arc::strong_count(receipt) > 1)
            .map(|(key, _)| key.clone())
            .collect::<HashSet<_>>();
        self.deleted.retain(|scope| active.contains(scope));
        self.closed
            .retain(|sid| active.iter().any(|(room, _)| room == sid));
        self.launch_receipts
            .retain(|_, receipt| Arc::strong_count(receipt) > 1);
    }
}
async fn cleanup_job(
    manager: RoomManager,
    registry: Registry,
    id: String,
    owned: Owned,
    agent_left: bool,
) {
    let identity = Identity(owned.identity);
    // A worker can acknowledge assignment before its cold process can receive
    // termination. Wait for RTC admission in the original room, bounded by
    // the pinned SDK's default process initialization timeout. A vanished room
    // releases the wait, and a job without an RTC participant still receives
    // its exact JobID termination at the deadline.
    let participant = if !agent_left && !identity.0.is_empty() {
        tokio::time::timeout(
            std::time::Duration::from_secs(10),
            manager.wait_for_agent_participant(&owned.key, owned.room_sid.clone(), &identity),
        )
        .await
        .ok()
        .and_then(Result::ok)
    } else {
        None
    };
    let terminated = if owned.via_rpc {
        match registry.cluster() {
            Some(cluster) => cluster.terminate(&id, agent_left).await,
            None => Err(WorkerError::Closed),
        }
    } else {
        registry
            .terminate(&owned.worker, &id, agent_left, now_ns())
            .await
    };
    if let Err(error) = terminated {
        tracing::warn!(job_id=%id,%error,"agent job termination failed");
        return;
    }
    if let Some(mut participant) = participant {
        let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(3);
        let retired = participant.signal_retired
            || if let Some(signal) = participant.session_retirement.as_mut() {
                // The room actor completes this receipt after observing actual
                // signal closure, participant leave, or session replacement.
                // The removal check still requires the original generation.
                tokio::time::timeout_at(deadline, signal.changed())
                    .await
                    .is_ok()
            } else {
                false
            };
        if retired {
            match manager
                .remove_agent_participant(
                    &owned.key,
                    owned.room_sid.clone(),
                    identity.clone(),
                    participant.participant_sid.clone(),
                    Some(participant.signal_generation),
                )
                .await
            {
                Ok(false) => {} // The captured participant resumed signaling.
                _ => return,    // Removed, already left, or original room retired.
            }
        }
        tokio::time::sleep_until(deadline).await;
        let _ = manager
            .remove_agent_participant(
                &owned.key,
                owned.room_sid,
                identity,
                participant.participant_sid,
                None,
            )
            .await;
    }
}
fn now_ns() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos()
        .min(i64::MAX as u128) as i64
}
async fn assign(
    registry: Registry,
    tokens: Arc<TokenMinter>,
    context: Launch,
) -> Result<Assigned, WorkerError> {
    if let Some(cluster) = registry.cluster() {
        let state = cluster.request(&context.job).await?;
        let worker = state.worker_id.clone();
        let mut job = context.job.clone();
        job.state = Some(state);
        return Ok(Assigned {
            context,
            worker,
            job,
            via_rpc: true,
        });
    }
    let mut attempted = HashSet::new();
    loop {
        let mut random = [0u8; 4];
        getrandom::fill(&mut random)
            .map_err(|e| WorkerError::InvalidRegistration(e.to_string()))?;
        let sample = (u32::from_le_bytes(random) >> 8) as f32 / (1u32 << 24) as f32;
        let worker = registry.select(&context.group, &attempted, sample)?;
        attempted.insert(worker.clone());
        match registry
            .assign(&worker, context.job.clone(), now_ns(), |r, j, a| {
                super::token::mint_assignment(
                    &tokens,
                    &r.api_key,
                    r,
                    j,
                    a,
                    rtc_core::util::unix_ms() / 1000,
                )
            })
            .await
        {
            Ok(state) => {
                let mut job = context.job.clone();
                job.state = Some(state);
                return Ok(Assigned {
                    context,
                    worker,
                    job,
                    via_rpc: false,
                });
            }
            Err(WorkerError::Unavailable | WorkerError::Closed) => continue,
            Err(error) => return Err(error),
        }
    }
}
pub(crate) fn to_proto(job: &AgentJobRecord) -> pb::Job {
    pb::Job {
        id: job.id.clone(),
        dispatch_id: job.dispatch_id.clone(),
        r#type: job.job_type,
        room: Some(crate::convert::room_info_to_proto(&job.room)),
        participant: job
            .participant
            .as_ref()
            .map(crate::convert::participant_info_to_proto),
        namespace: job.namespace.clone(),
        metadata: job.metadata.clone(),
        agent_name: job.agent_name.clone(),
        deployment: job.deployment.clone(),
        attributes: job.attributes.clone().into_iter().collect(),
        enable_recording: job.enable_recording,
        enable_redaction: job.enable_redaction,
        state: Some(pb::JobState {
            status: job.state.status,
            error: job.state.error.clone(),
            started_at: job.state.started_at_ns,
            ended_at: job.state.ended_at_ns,
            updated_at: job.state.updated_at_ns,
            participant_identity: job.state.participant_identity.clone(),
            worker_id: job.state.worker_id.clone(),
            agent_id: job.state.agent_id.clone(),
        }),
    }
}
fn to_record(job: &pb::Job) -> AgentJobRecord {
    let state = job.state.as_ref().expect("assignment state");
    AgentJobRecord {
        id: job.id.clone(),
        dispatch_id: job.dispatch_id.clone(),
        job_type: job.r#type,
        room: crate::convert::room_info_from_proto(job.room.as_ref().expect("launch room")),
        participant: job
            .participant
            .as_ref()
            .map(crate::convert::participant_info_from_proto),
        namespace: job.namespace.clone(),
        metadata: job.metadata.clone(),
        agent_name: job.agent_name.clone(),
        deployment: job.deployment.clone(),
        attributes: job.attributes.clone().into_iter().collect(),
        enable_recording: job.enable_recording,
        enable_redaction: job.enable_redaction,
        state: AgentJobState {
            status: state.status,
            error: state.error.clone(),
            started_at_ns: state.started_at,
            ended_at_ns: state.ended_at,
            updated_at_ns: state.updated_at,
            participant_identity: state.participant_identity.clone(),
            worker_id: state.worker_id.clone(),
            agent_id: state.agent_id.clone(),
        },
    }
}

#[cfg(test)]
#[path = "bridge/lifecycle_tests.rs"]
mod lifecycle_tests;
