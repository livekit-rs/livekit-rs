//! Room registry and the API surface adapters call.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use rtc_core::config::{RoomPolicy, ServerConfig};
use rtc_core::data::DataHandler;
use rtc_core::events::{EventSink, RoomEndReason};
use rtc_core::ids::*;
use rtc_core::media_api::DataKind;
use rtc_core::model::*;
use rtc_core::room::{OpError, Room};
use rtc_core::session::{JoinError, JoinParams};
use tokio::sync::{Notify, mpsc, oneshot, watch};

use crate::api::{
    AgentDispatchOptions, AgentDispatchRecord, AgentParticipantLookup, AgentParticipantScope,
    ApiError, CreateRoomOptions, NodeStats, ParticipantUpdate, SessionHandle,
};
use crate::net::NetRouter;
use crate::room_task::{ApiCall, ROOM_INBOX, RoomInput, RoomSnapshot, RoomTask, SharedSnapshot};

/// Upstream answer when a room/participant topic has no owner.
const NO_RESPONSE: &str = "no response from servers";
const ROOM_NOT_FOUND: &str = "requested room does not exist";

/// Collaborators injected by the assembly.
#[derive(Clone)]
pub struct RuntimeDeps {
    pub config: Arc<ServerConfig>,
    pub net: NetRouter,
    pub events: Arc<dyn EventSink>,
    pub data: Arc<dyn DataHandler>,
}

pub(crate) type OwnerReadyReceiver = watch::Receiver<Option<Result<(), String>>>;
pub(crate) type OwnerReadySender = watch::Sender<Option<Result<(), String>>>;
pub type RoomOwnerOperation<'a, T> =
    std::pin::Pin<Box<dyn std::future::Future<Output = T> + Send + 'a>>;

struct RoomEntry {
    inbox: mpsc::Sender<RoomInput>,
    snapshot: SharedSnapshot,
    idle_hold: Arc<crate::idle_hold::IdleHoldState>,
    owner_ready: OwnerReadyReceiver,
}

/// A held actual room generation; success/error/cancellation all release the
/// same generation through RAII. Never resolves a later room by name.
pub struct RoomCreationLease {
    inbox: mpsc::Sender<RoomInput>,
    snapshot: SharedSnapshot,
    created: bool,
    composite: Option<rtc_core::auto_egress::RoomCompositeRecordingPolicy>,
    _hold: crate::idle_hold::IdleHold,
    owner_ready: OwnerReadyReceiver,
}
impl RoomCreationLease {
    async fn wait_owner_ready(&mut self) -> Result<(), ApiError> {
        loop {
            let observed = self.owner_ready.borrow_and_update().clone();
            if let Some(result) = observed {
                return result.map_err(ApiError::Unavailable);
            }
            self.owner_ready
                .changed()
                .await
                .map_err(|_| ApiError::Unavailable(NO_RESPONSE.into()))?;
        }
    }

    pub fn created(&self) -> bool {
        self.created
    }
    /// Effective create-only policy, resolved under the same generation lock.
    pub fn composite_policy(&self) -> Option<&rtc_core::auto_egress::RoomCompositeRecordingPolicy> {
        self.composite.as_ref()
    }
    pub fn snapshot(&self) -> RoomInfo {
        self.snapshot
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .info
            .clone()
    }

    /// Same generation's published RoomInfo, equivalent to Go Room.ToProto's
    /// protoProxy.Get. Actor mutations publish this view before their replies.
    /// Reading it must not enqueue another operation behind a full actor inbox.
    /// Keep the async API for existing callers; this read itself never waits on
    /// media, membership subscriptions, Redis or an unrelated replacement room.
    pub async fn current_snapshot(&self) -> RoomInfo {
        self.snapshot()
    }
}

/// A trusted accepted request's exact actor, independent of later name reuse.
/// This is not an idle hold and cannot keep an empty room alive. Operations never
/// resolve another actor through RoomManager after this handle is captured.
#[derive(Clone)]
pub struct RoomOwnerHandle {
    key: RoomKey,
    sid: RoomSid,
    inbox: mpsc::Sender<RoomInput>,
}
impl RoomOwnerHandle {
    pub async fn rpc_participant(
        &self,
        identity: Identity,
    ) -> Result<(ParticipantSid, u32), ApiError> {
        let (reply, response) = oneshot::channel();
        call(
            &self.inbox,
            RoomInput::Api(ApiCall::RpcParticipant(identity, reply)),
            response,
        )
        .await?
        .map_err(op_error)
    }
    pub fn key(&self) -> &RoomKey {
        &self.key
    }
    pub async fn room_info(&self) -> Result<RoomInfo, ApiError> {
        let (reply, response) = oneshot::channel();
        call(
            &self.inbox,
            RoomInput::Api(ApiCall::GetRoomInfo(reply)),
            response,
        )
        .await
    }
    pub async fn update_metadata(&self, metadata: String) -> Result<RoomInfo, ApiError> {
        let (reply, response) = oneshot::channel();
        call(
            &self.inbox,
            RoomInput::Api(ApiCall::UpdateMetadata(metadata, reply)),
            response,
        )
        .await
    }
    pub async fn update_participant(
        &self,
        identity: Identity,
        update: ParticipantUpdate,
    ) -> Result<ParticipantInfo, ApiError> {
        let (reply, response) = oneshot::channel();
        call(
            &self.inbox,
            RoomInput::Api(ApiCall::UpdateParticipant {
                identity,
                update,
                reply,
            }),
            response,
        )
        .await?
        .map_err(op_error)
    }
    pub async fn remove_participant(&self, identity: Identity) -> Result<(), ApiError> {
        let (reply, response) = oneshot::channel();
        call(
            &self.inbox,
            RoomInput::Api(ApiCall::RemoveParticipant(identity, reply)),
            response,
        )
        .await?
        .map_err(op_error)
    }
    pub async fn mute_published_track(
        &self,
        identity: Identity,
        track: TrackSid,
        muted: bool,
    ) -> Result<Option<TrackInfo>, ApiError> {
        let (reply, response) = oneshot::channel();
        call(
            &self.inbox,
            RoomInput::Api(ApiCall::MuteTrack {
                identity,
                track,
                muted,
                reply,
            }),
            response,
        )
        .await?
        .map_err(op_error)
    }
    pub async fn update_subscriptions(
        &self,
        identity: Identity,
        track_sids: Vec<TrackSid>,
        subscribe: bool,
    ) -> Result<(), ApiError> {
        let (reply, response) = oneshot::channel();
        call(
            &self.inbox,
            RoomInput::Api(ApiCall::UpdateSubscriptions {
                identity,
                track_sids,
                subscribe,
                reply,
            }),
            response,
        )
        .await?
        .map_err(op_error)
    }
    pub async fn send_data_with_nonce(
        &self,
        kind: DataKind,
        payload: Vec<u8>,
        destination_identities: Vec<Identity>,
        destination_sids: Vec<ParticipantSid>,
        nonce: Vec<u8>,
    ) -> Result<(), ApiError> {
        let (reply, response) = oneshot::channel();
        call(
            &self.inbox,
            RoomInput::Api(ApiCall::SendData {
                kind,
                payload,
                destination_identities,
                destination_sids,
                nonce,
                reply,
            }),
            response,
        )
        .await
    }
    pub async fn list_participants(&self) -> Result<Vec<ParticipantInfo>, ApiError> {
        let (reply, response) = oneshot::channel();
        call(
            &self.inbox,
            RoomInput::Api(ApiCall::ListParticipants(reply)),
            response,
        )
        .await
    }
    pub async fn get_participant(&self, identity: Identity) -> Result<ParticipantInfo, ApiError> {
        let (reply, response) = oneshot::channel();
        call(
            &self.inbox,
            RoomInput::Api(ApiCall::GetParticipant(identity, reply)),
            response,
        )
        .await?
        .map_err(op_error)
    }
    pub async fn close_room(&self) -> Result<(), ApiError> {
        self.inbox
            .send(RoomInput::Close(RoomEndReason::ApiDelete))
            .await
            .map_err(|_| ApiError::Unavailable(NO_RESPONSE.into()))?;
        self.inbox.closed().await;
        Ok(())
    }
    /// Read-only control-mailbox capacity for owner diagnostics/acceptance.
    /// This cannot reserve, enqueue, pause or mutate an actor operation.
    #[doc(hidden)]
    pub fn control_capacity(&self) -> usize {
        self.inbox.capacity()
    }
    pub fn sid(&self) -> &RoomSid {
        &self.sid
    }
    pub async fn create_agent_dispatch(
        &self,
        options: AgentDispatchOptions,
    ) -> Result<AgentDispatchRecord, ApiError> {
        let (reply, response) = oneshot::channel();
        call(
            &self.inbox,
            RoomInput::Api(ApiCall::CreateAgentDispatch(options, reply)),
            response,
        )
        .await
    }
    pub async fn list_agent_dispatches(
        &self,
        dispatch_id: Option<&str>,
    ) -> Result<Vec<AgentDispatchRecord>, ApiError> {
        let (reply, response) = oneshot::channel();
        call(
            &self.inbox,
            RoomInput::Api(ApiCall::ListAgentDispatches(
                dispatch_id.filter(|v| !v.is_empty()).map(str::to_owned),
                reply,
            )),
            response,
        )
        .await
    }
    pub async fn delete_agent_dispatch(
        &self,
        dispatch_id: &str,
    ) -> Result<AgentDispatchRecord, ApiError> {
        let (reply, response) = oneshot::channel();
        call(
            &self.inbox,
            RoomInput::Api(ApiCall::DeleteAgentDispatch(dispatch_id.into(), reply)),
            response,
        )
        .await?
    }
}

/// Protocol-neutral asynchronous owner-topic lifecycle. Subscription readiness
/// and close acknowledgement may wait for real transport backpressure. Neither
/// operation runs while the synchronous manager generation map is locked.
pub trait RoomOwnerLifecycle: Send + Sync + 'static {
    fn opened(&self, owner: RoomOwnerHandle) -> RoomOwnerOperation<'_, Result<(), String>>;
    fn closed(&self, key: RoomKey, sid: RoomSid) -> RoomOwnerOperation<'_, ()>;
    /// Owner actor waits for real participant topic admission before join returns.
    fn participants(
        &self,
        _owner: RoomOwnerHandle,
        _participants: Vec<ParticipantInfo>,
    ) -> RoomOwnerOperation<'_, Result<(), String>> {
        Box::pin(async { Ok(()) })
    }
}
struct Inner {
    forward_stats_worker: crate::forward_stats::ForwardStatsWorker,
    owner_lifecycle: Mutex<Option<Arc<dyn RoomOwnerLifecycle>>>,
    deps: RuntimeDeps,
    recording: Arc<crate::automatic_recording::RecordingExecutor>,
    agent_events: crate::api::AgentEventSender,
    room_policy: Arc<RoomPolicy>,
    participant_update_sizer: Mutex<Option<Arc<dyn rtc_core::room::ParticipantUpdateSizer>>>,
    rooms: Mutex<HashMap<RoomKey, RoomEntry>>,
    process_stats: crate::process_stats::ProcessStats,
    rooms_changed: Notify,
    draining: AtomicBool,
    started_at_ms: i64,
}

#[derive(Clone)]
pub struct RoomManager {
    inner: Arc<Inner>,
}

impl RoomManager {
    pub fn new(deps: RuntimeDeps) -> RoomManager {
        let room_policy = Arc::new(RoomPolicy::from(deps.config.as_ref()));
        RoomManager {
            inner: Arc::new(Inner {
                forward_stats_worker: crate::forward_stats::ForwardStatsWorker::new(
                    deps.config.rtc.forward_stats.as_ref(),
                ),
                owner_lifecycle: Mutex::new(None),
                deps,
                recording: Arc::new(crate::automatic_recording::RecordingExecutor::default()),
                agent_events: Arc::new(Mutex::new(None)),
                room_policy,
                participant_update_sizer: Mutex::new(None),
                rooms: Mutex::new(HashMap::new()),
                process_stats: Default::default(),
                rooms_changed: Notify::new(),
                draining: AtomicBool::new(false),
                started_at_ms: rtc_core::util::unix_ms(),
            }),
        }
    }

    /// Bind the protocol adapter's real encoded-size provider before rooms exist.
    /// The core owns batching but remains independent of any wire schema.
    pub fn attach_participant_update_sizer(
        &self,
        sizer: Arc<dyn rtc_core::room::ParticipantUpdateSizer>,
    ) -> Result<(), ApiError> {
        let rooms = self.inner.rooms.lock().unwrap();
        if !rooms.is_empty() {
            return Err(ApiError::FailedPrecondition(
                "participant size provider must be attached before room creation".into(),
            ));
        }
        *self.inner.participant_update_sizer.lock().unwrap() = Some(sizer);
        Ok(())
    }

    /// Attach before any actual owner, with the same registration gate as creation.
    pub fn attach_owner_lifecycle(
        &self,
        lifecycle: Arc<dyn RoomOwnerLifecycle>,
    ) -> Result<(), ApiError> {
        let rooms = self.rooms();
        if !rooms.is_empty() {
            return Err(ApiError::FailedPrecondition(
                "owner lifecycle must attach before rooms".into(),
            ));
        }
        let mut slot = self
            .inner
            .owner_lifecycle
            .lock()
            .unwrap_or_else(|p| p.into_inner());
        if slot.is_some() {
            return Err(ApiError::FailedPrecondition(
                "owner lifecycle already attached".into(),
            ));
        }
        *slot = Some(lifecycle);
        Ok(())
    }

    /// Install the neutral recording bridge before any owner is visible.
    pub fn attach_recording_launcher(
        &self,
        launcher: Arc<dyn crate::automatic_recording::RecordingLauncher>,
    ) -> Result<(), ApiError> {
        // Hold the registry lock through installation, fencing concurrent creation.
        let rooms = self.rooms();
        if !rooms.is_empty() {
            return Err(ApiError::FailedPrecondition(
                "recording bridge must attach before rooms are created".into(),
            ));
        }
        self.inner
            .recording
            .install(launcher)
            .map_err(ApiError::FailedPrecondition)
    }

    /// Install the one adapter-owned job bridge before room owners become visible.
    pub fn attach_agent_events(
        &self,
        sender: mpsc::Sender<crate::api::AgentWorkEvent>,
    ) -> Result<(), ApiError> {
        if !self.rooms().is_empty() {
            return Err(ApiError::FailedPrecondition(
                "agent bridge must attach before rooms are created".into(),
            ));
        }
        let mut slot = self
            .inner
            .agent_events
            .lock()
            .unwrap_or_else(|p| p.into_inner());
        if slot.is_some() {
            return Err(ApiError::FailedPrecondition(
                "agent bridge already attached".into(),
            ));
        }
        *slot = Some(sender);
        Ok(())
    }
    /// Commit a real completed assignment only into its original room generation.
    pub async fn commit_agent_job(
        &self,
        key: &RoomKey,
        room_sid: RoomSid,
        job: crate::api::AgentJobRecord,
    ) -> Result<(), ApiError> {
        let inbox = self
            .inbox(key)
            .ok_or_else(|| ApiError::NotFound(ROOM_NOT_FOUND.into()))?;
        let (tx, rx) = oneshot::channel();
        inbox
            .send(RoomInput::Api(ApiCall::CommitAgentJob {
                room_sid,
                job: Box::new(job),
                reply: tx,
            }))
            .await
            .map_err(|_| ApiError::NotFound(ROOM_NOT_FOUND.into()))?;
        rx.await
            .map_err(|_| ApiError::NotFound(ROOM_NOT_FOUND.into()))?
    }
    pub async fn agent_participant_scope(
        &self,
        key: &RoomKey,
        room_sid: RoomSid,
        identity: &Identity,
    ) -> Result<Option<AgentParticipantScope>, ApiError> {
        let inbox = self
            .inbox(key)
            .ok_or_else(|| ApiError::NotFound(ROOM_NOT_FOUND.into()))?;
        Ok(
            match Self::lookup_agent_participant(&inbox, room_sid, identity).await? {
                AgentParticipantLookup::Present(scope) => Some(scope),
                AgentParticipantLookup::AwaitAdmission(_) => None,
            },
        )
    }

    pub async fn wait_for_agent_participant(
        &self,
        key: &RoomKey,
        room_sid: RoomSid,
        identity: &Identity,
    ) -> Result<AgentParticipantScope, ApiError> {
        let inbox = self
            .inbox(key)
            .ok_or_else(|| ApiError::NotFound(ROOM_NOT_FOUND.into()))?;
        loop {
            match Self::lookup_agent_participant(&inbox, room_sid.clone(), identity).await? {
                AgentParticipantLookup::Present(scope) => return Ok(scope),
                AgentParticipantLookup::AwaitAdmission(mut admission) => {
                    admission
                        .changed()
                        .await
                        .map_err(|_| ApiError::NotFound(ROOM_NOT_FOUND.into()))?;
                }
            }
        }
    }

    async fn lookup_agent_participant(
        inbox: &mpsc::Sender<RoomInput>,
        room_sid: RoomSid,
        identity: &Identity,
    ) -> Result<AgentParticipantLookup, ApiError> {
        let (tx, rx) = oneshot::channel();
        inbox
            .send(RoomInput::Api(ApiCall::AgentParticipantScope {
                room_sid,
                identity: identity.clone(),
                reply: tx,
            }))
            .await
            .map_err(|_| ApiError::NotFound(ROOM_NOT_FOUND.into()))?;
        rx.await
            .map_err(|_| ApiError::NotFound(ROOM_NOT_FOUND.into()))?
    }
    pub async fn remove_agent_participant(
        &self,
        key: &RoomKey,
        room_sid: RoomSid,
        identity: Identity,
        participant_sid: ParticipantSid,
        retired_signal_generation: Option<SessionGeneration>,
    ) -> Result<bool, ApiError> {
        let inbox = self
            .inbox(key)
            .ok_or_else(|| ApiError::NotFound(ROOM_NOT_FOUND.into()))?;
        let (tx, rx) = oneshot::channel();
        inbox
            .send(RoomInput::Api(ApiCall::RemoveAgentParticipant {
                room_sid,
                identity,
                participant_sid,
                retired_signal_generation,
                reply: tx,
            }))
            .await
            .map_err(|_| ApiError::NotFound(ROOM_NOT_FOUND.into()))?;
        rx.await
            .map_err(|_| ApiError::NotFound(ROOM_NOT_FOUND.into()))?
    }
    pub async fn published_media(
        &self,
        key: &RoomKey,
        identity: Identity,
        sid: ParticipantSid,
    ) -> Result<Vec<rtc_core::media_api::PublishedMediaState>, ApiError> {
        let inbox = self
            .inbox(key)
            .ok_or_else(|| ApiError::NotFound(ROOM_NOT_FOUND.into()))?;
        let (reply, receipt) = oneshot::channel();
        inbox
            .send(RoomInput::Api(ApiCall::PublishedMedia {
                identity,
                sid,
                reply,
            }))
            .await
            .map_err(|_| ApiError::NotFound(ROOM_NOT_FOUND.into()))?;
        receipt
            .await
            .map_err(|_| ApiError::NotFound(ROOM_NOT_FOUND.into()))?
    }
    pub fn config(&self) -> &Arc<ServerConfig> {
        &self.inner.deps.config
    }

    fn rooms(&self) -> std::sync::MutexGuard<'_, HashMap<RoomKey, RoomEntry>> {
        self.inner.rooms.lock().unwrap_or_else(|p| p.into_inner())
    }

    fn inbox(&self, key: &RoomKey) -> Option<mpsc::Sender<RoomInput>> {
        self.rooms().get(key).map(|e| e.inbox.clone())
    }

    pub fn room_exists(&self, key: &RoomKey) -> bool {
        self.rooms().contains_key(key)
    }

    /// Ordinary callers retain the inbox/snapshot interface.
    async fn get_or_create(
        &self,
        key: &RoomKey,
        overrides: &rtc_core::permission::RoomConfig,
    ) -> Result<(mpsc::Sender<RoomInput>, SharedSnapshot), ApiError> {
        let mut lease = self.get_or_create_generation(key, overrides)?;
        lease.wait_owner_ready().await?;
        Ok((lease.inbox.clone(), lease.snapshot.clone()))
    }

    /// The created bit is decided under the same room-map lock as registration.
    /// No adapter-level room_exists/read-then-create inference authorizes a launch.
    fn get_or_create_generation(
        &self,
        key: &RoomKey,
        overrides: &rtc_core::permission::RoomConfig,
    ) -> Result<RoomCreationLease, ApiError> {
        self.get_or_create_generation_with_preset(key, overrides, "")
    }

    fn get_or_create_generation_with_preset(
        &self,
        key: &RoomKey,
        overrides: &rtc_core::permission::RoomConfig,
        preset: &str,
    ) -> Result<RoomCreationLease, ApiError> {
        self.get_or_create_bootstrapped_generation(key, overrides, preset, None)
    }

    fn get_or_create_bootstrapped_generation(
        &self,
        key: &RoomKey,
        overrides: &rtc_core::permission::RoomConfig,
        preset: &str,
        bootstrap: Option<&RoomInfo>,
    ) -> Result<RoomCreationLease, ApiError> {
        if bootstrap.is_some_and(|info| info.sid.0.is_empty() || info.name != key.name) {
            return Err(ApiError::FailedPrecondition(
                "invalid shared room identity".into(),
            ));
        }
        let mut rooms = self.rooms();
        if let Some(e) = rooms.get(key) {
            if !e.inbox.is_closed()
                && let Some(hold) = e.idle_hold.acquire()
            {
                if let Some(expected) = bootstrap {
                    let current = e.snapshot.lock().unwrap_or_else(|p| p.into_inner());
                    if current.info.sid != expected.sid {
                        return Err(ApiError::FailedPrecondition(
                            "shared room generation does not match local owner".into(),
                        ));
                    }
                }
                return Ok(RoomCreationLease {
                    inbox: e.inbox.clone(),
                    snapshot: e.snapshot.clone(),
                    created: false,
                    composite: None,
                    _hold: hold,
                    owner_ready: e.owner_ready.clone(),
                });
            }
            rooms.remove(key);
        }
        if self.inner.draining.load(Ordering::Relaxed) {
            return Err(ApiError::Unavailable("server is shutting down".into()));
        }
        let mut resolved = overrides.clone();
        if !preset.is_empty() {
            let preset = self.config().room.presets.get(preset).ok_or_else(|| {
                ApiError::InvalidArgument(
                    "unknown room configuration in create room request".into(),
                )
            })?;
            // applyNamedRoomConfiguration dereferences a nil preset and the Go
            // handler panics. Its psrpc CreateRoom chain installs no recovery
            // middleware, so Go crashes (observed SIGSEGV, exit 2). Rust instead
            // returns a clean structured internal error and stays alive (R03).
            let preset = preset
                .as_ref()
                .ok_or_else(|| ApiError::Internal("Internal service panic".into()))?;
            resolved.inherit(preset);
        }
        let overrides = &resolved;
        let deps = &self.inner.deps;
        let mut room = if let Some(info) = bootstrap {
            Room::from_bootstrap(
                key.clone(),
                self.inner.room_policy.clone(),
                deps.data.clone(),
                overrides,
                std::time::Instant::now(),
                info.clone(),
            )
            .map_err(|error| ApiError::FailedPrecondition(error.into()))?
        } else {
            Room::new(
                key.clone(),
                self.inner.room_policy.clone(),
                deps.data.clone(),
                overrides,
                std::time::Instant::now(),
            )
        };
        if let Some(sizer) = self.inner.participant_update_sizer.lock().unwrap().clone() {
            room.set_participant_update_sizer(sizer);
        }
        let admitted = room.info();
        deps.events
            .reserve_room(&admitted)
            .map_err(ApiError::Unavailable)?;
        let snapshot: SharedSnapshot = Arc::new(Mutex::new(RoomSnapshot {
            info: room.info(),
            participants: 0,
            tracks_in: 0,
            tracks_out: 0,
            bytes_in: 0,
            bytes_out: 0,
            packets_in: 0,
            packets_out: 0,
            nacks: 0,
            retransmit_bytes_out: 0,
            retransmit_packets_out: 0,
            estimated_capacity_bps: 0,
            allocated_bps: 0,
            allocation_overshoot_bps: 0,
            paused_video_tracks: 0,
            probe_attempts: 0,
            probe_successes: 0,
            probe_failures: 0,
            congestion_events: 0,
        }));
        let idle_hold = Arc::new(crate::idle_hold::IdleHoldState::default());
        let hold = idle_hold.acquire().expect("fresh room generation");
        let (tx, rx) = mpsc::channel(ROOM_INBOX);
        let mut task = RoomTask::new(
            room,
            &deps.config,
            deps.net.clone(),
            deps.events.clone(),
            snapshot.clone(),
            tx.clone(),
            rx,
        )
        .map_err(|error| {
            deps.events.cancel_room_reservation(&admitted);
            ApiError::Internal(error)
        })?;
        task.set_process_stats(self.inner.process_stats.reporter());
        task.set_idle_hold(idle_hold.clone());
        task.initialize_recording(
            overrides.automatic_recording.clone(),
            self.inner.recording.clone(),
        );
        task.set_agent_event_sender(self.inner.agent_events.clone());
        task.initialize_agent_dispatches(overrides.agent_dispatches.as_deref());
        let owner_lifecycle = self
            .inner
            .owner_lifecycle
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .clone();
        let (owner_ready_tx, owner_ready) = watch::channel(if owner_lifecycle.is_none() {
            Some(Ok(()))
        } else {
            None
        });
        let owner = RoomOwnerHandle {
            key: key.clone(),
            sid: admitted.sid.clone(),
            inbox: tx.clone(),
        };
        task.set_owner_lifecycle(owner_lifecycle, owner, owner_ready_tx);
        rooms.insert(
            key.clone(),
            RoomEntry {
                inbox: tx.clone(),
                snapshot: snapshot.clone(),
                idle_hold,
                owner_ready: owner_ready.clone(),
            },
        );
        drop(rooms);
        self.inner.rooms_changed.notify_waiters();

        let manager = self.clone();
        let key = key.clone();
        let inbox = tx.clone();
        tokio::spawn(async move {
            task.run().await;
            // Unregister only if the entry still belongs to this task.
            let mut rooms = manager.rooms();
            if rooms
                .get(&key)
                .is_some_and(|e| e.inbox.same_channel(&inbox))
            {
                rooms.remove(&key);
                manager.inner.rooms_changed.notify_waiters();
            }
        });
        Ok(RoomCreationLease {
            inbox: tx,
            snapshot,
            created: true,
            composite: overrides
                .automatic_recording
                .as_ref()
                .and_then(|e| e.room.clone()),
            _hold: hold,
            owner_ready,
        })
    }

    // ----- signaling -----

    /// Join (or resume) a room. Applies the auto-create rule: without
    /// `room.auto_create` and a `roomCreate` grant the room must exist.
    pub async fn join(&self, params: JoinParams) -> Result<SessionHandle, JoinError> {
        self.join_started(params, tokio::time::Instant::now().into_std())
            .await
    }
    /// Exact RTC owner StartSession clock, retained through room allocation and
    /// actor queueing for source short-signal-close cancellation classification.
    pub async fn join_started(
        &self,
        params: JoinParams,
        session_started_at: std::time::Instant,
    ) -> Result<SessionHandle, JoinError> {
        let key = params.room.clone();
        // Fixed native WHIP StartSession calls getOrCreateRoom directly, without
        // signaling ValidateCreateRoom. Preserve the regular SDK auto-create policy.
        let may_create = params.capabilities.one_shot
            || self.config().room.auto_create
            || params.grants.video.room_create;
        if !may_create && !self.room_exists(&key) {
            return Err(JoinError::RoomNotFound);
        }
        self.inner.process_stats.record_rtc_init();
        let result = self.join_accepted(params, session_started_at).await;
        if let Err(error) = &result {
            // Fixed room.Join failure closes the just-created participant with
            // JoinFailed (one cancel) and records StartSession cancel as well.
            let count = if matches!(
                error,
                JoinError::MaxParticipants | JoinError::RoomClosed | JoinError::AlreadyJoined
            ) {
                2
            } else {
                1
            };
            self.inner.process_stats.record_rtc_canceled(count);
        }
        result
    }
    async fn join_accepted(
        &self,
        params: JoinParams,
        session_started_at: std::time::Instant,
    ) -> Result<SessionHandle, JoinError> {
        let key = params.room.clone();
        let overrides = params.grants.room_config.clone().unwrap_or_default();
        let preset = params.grants.room_preset.clone();
        let params = Box::new(params);
        // A room closing concurrently rejects with RoomClosed: retry once on a new room.
        let mut params = Some(params);
        for _ in 0..2 {
            let mut lease = self
                .get_or_create_generation_with_preset(&key, &overrides, &preset)
                .map_err(|e| JoinError::Internal(e.to_string()))?;
            lease
                .wait_owner_ready()
                .await
                .map_err(|e| JoinError::Internal(e.to_string()))?;
            if lease.created()
                && let Some(policy) = lease.composite_policy().cloned()
            {
                // getOrCreateRoom holds the exact generation while worker starts;
                // failure releases the hold, never deletes a replacement.
                self.inner
                    .recording
                    .launch_room(lease.snapshot(), policy)
                    .await
                    .map_err(JoinError::Internal)?;
            }
            let inbox = lease.inbox.clone();
            let (reply_tx, reply_rx) = oneshot::channel();
            let p = params.take().expect("params present");
            let retained = p.clone();
            if inbox
                .send(RoomInput::Join {
                    params: p,
                    session_started_at,
                    reply: reply_tx,
                })
                .await
                .is_err()
            {
                params = Some(retained);
                continue;
            }
            match reply_rx.await {
                Ok(Err(JoinError::RoomClosed)) => {
                    params = Some(retained);
                    continue;
                }
                Ok(res) => return res,
                Err(_) => {
                    params = Some(retained);
                    continue;
                }
            }
        }
        Err(JoinError::RoomClosed)
    }

    // ----- server API -----

    pub async fn create_room(
        &self,
        key: RoomKey,
        opts: CreateRoomOptions,
    ) -> Result<RoomInfo, ApiError> {
        let (_, snapshot) = self.get_or_create(&key, &opts.config).await?;
        let info = snapshot
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .info
            .clone();
        Ok(info)
    }

    /// Resolve a named configuration only if this call creates the actual owner.
    pub async fn create_room_lease_with_preset(
        &self,
        key: RoomKey,
        opts: CreateRoomOptions,
        preset: &str,
    ) -> Result<RoomCreationLease, ApiError> {
        let mut lease = self.get_or_create_generation_with_preset(&key, &opts.config, preset)?;
        lease.wait_owner_ready().await?;
        Ok(lease)
    }

    /// Hold the actual generation across adapter initialization (including real
    /// worker RPC). Empty/departure idle timeout cannot retire it while held.
    /// Instantiate a preallocated shared catalog generation without replacing a
    /// live different owner. The catalog adapter must resolve presets first and
    /// pass their effective neutral policy in `opts`; no protocol enters runtime.
    /// The returned idle hold spans actual external initialization awaits.
    pub async fn create_room_lease_from_catalog(
        &self,
        key: RoomKey,
        opts: CreateRoomOptions,
        info: RoomInfo,
    ) -> Result<RoomCreationLease, ApiError> {
        let mut lease =
            self.get_or_create_bootstrapped_generation(&key, &opts.config, "", Some(&info))?;
        lease.wait_owner_ready().await?;
        Ok(lease)
    }

    pub async fn create_room_lease(
        &self,
        key: RoomKey,
        opts: CreateRoomOptions,
    ) -> Result<RoomCreationLease, ApiError> {
        let mut lease = self.get_or_create_generation(&key, &opts.config)?;
        lease.wait_owner_ready().await?;
        Ok(lease)
    }

    pub fn list_rooms(&self, project: &ProjectId, names: &[RoomName]) -> Vec<RoomInfo> {
        self.rooms()
            .iter()
            .filter(|(k, e)| {
                &k.project == project
                    && !e.inbox.is_closed()
                    && (names.is_empty() || names.contains(&k.name))
            })
            .map(|(_, e)| {
                e.snapshot
                    .lock()
                    .unwrap_or_else(|p| p.into_inner())
                    .info
                    .clone()
            })
            .collect()
    }

    /// SPEC-0074 C14: every live room generation across all projects, exactly
    /// what Go's `/debug/rooms` iterates (`s.roomManager.rooms`, no project
    /// filter, server.go:427-432). Closed inboxes are excluded like
    /// `list_rooms`; the snapshot carries the room's actual name, sid,
    /// creation time and live participant/publisher counts.
    pub fn debug_rooms(&self) -> Vec<RoomInfo> {
        self.rooms()
            .iter()
            .filter(|(_, e)| !e.inbox.is_closed())
            .map(|(_, e)| {
                e.snapshot
                    .lock()
                    .unwrap_or_else(|p| p.into_inner())
                    .info
                    .clone()
            })
            .collect()
    }

    /// Current owner-state debug views across projects. Clone generation-bound
    /// inboxes before awaiting, so deletion/recreation cannot mix a room SID
    /// with a different generation's participants. Closed generations are omitted.
    pub async fn debug_room_details(&self) -> Vec<rtc_core::room::DebugRoomSnapshot> {
        let inboxes: Vec<_> = self
            .rooms()
            .values()
            .filter(|e| !e.inbox.is_closed())
            .map(|e| e.inbox.clone())
            .collect();
        let mut details = Vec::with_capacity(inboxes.len());
        for inbox in inboxes {
            let (reply, receipt) = oneshot::channel();
            if inbox
                .send(RoomInput::Api(ApiCall::GetDebugRoom(reply)))
                .await
                .is_ok()
            {
                if let Ok(detail) = receipt.await {
                    details.push(detail);
                }
            }
        }
        details
    }

    pub async fn delete_room(&self, key: &RoomKey) -> Result<(), ApiError> {
        let inbox = self
            .inbox(key)
            .ok_or_else(|| ApiError::NotFound(ROOM_NOT_FOUND.into()))?;
        inbox
            .send(RoomInput::Close(RoomEndReason::ApiDelete))
            .await
            .map_err(|_| ApiError::NotFound(ROOM_NOT_FOUND.into()))?;
        // Enqueueing Close is not completion: the room can still be listed and
        // accept work until its owner processes the message. Receiver closure is
        // published after close effects and signal-session shutdown are settled.
        inbox.closed().await;
        // The owner may already have unregistered, or another task may own a
        // newly recreated room of this name. Only remove the deleted generation.
        let mut rooms = self.rooms();
        if rooms
            .get(key)
            .is_some_and(|entry| entry.inbox.same_channel(&inbox))
        {
            rooms.remove(key);
            self.inner.rooms_changed.notify_waiters();
        }
        Ok(())
    }

    // A genuine room topic can register while the original request is waiting.
    // Register Notify before the predicate check, avoiding a lost registration
    // wakeup. One absolute deadline bounds lookup, backpressure and response.
    async fn participant_owner_call<T: Send>(
        &self,
        key: &RoomKey,
        request: impl FnOnce(oneshot::Sender<T>) -> ApiCall + Send,
        timeout: Duration,
    ) -> Result<T, ApiError> {
        let response = async {
            let inbox = loop {
                let changed = self.inner.rooms_changed.notified();
                tokio::pin!(changed);
                changed.as_mut().enable();
                if let Some(inbox) = self.inbox(key).filter(|inbox| !inbox.is_closed()) {
                    break inbox;
                }
                changed.await;
            };
            let (tx, rx) = oneshot::channel();
            call(&inbox, RoomInput::Api(request(tx)), rx).await
        };
        tokio::time::timeout(timeout, response)
            .await
            .unwrap_or_else(|_| Err(ApiError::Unavailable(NO_RESPONSE.into())))
    }

    /// Public RoomService read policy; internal owner queries remain live.
    pub async fn list_participants_for_api(
        &self,
        key: &RoomKey,
        timeout: Duration,
    ) -> Result<Vec<ParticipantInfo>, ApiError> {
        if self.config().enable_psrpc_for_get_list_participants {
            return self
                .participant_owner_call(key, ApiCall::ListParticipants, timeout)
                .await;
        }
        let Some(inbox) = self.inbox(key) else {
            return Ok(Vec::new());
        };
        let (tx, rx) = oneshot::channel();
        if inbox
            .send(RoomInput::Api(ApiCall::ListStoredParticipants(tx)))
            .await
            .is_err()
        {
            return Ok(Vec::new());
        }
        Ok(rx.await.unwrap_or_default())
    }

    /// Read the last stored participant change unless live PSRPC reads are enabled.
    pub async fn get_participant_for_api(
        &self,
        key: &RoomKey,
        identity: &Identity,
        timeout: Duration,
    ) -> Result<ParticipantInfo, ApiError> {
        if self.config().enable_psrpc_for_get_list_participants {
            return self
                .participant_owner_call(
                    key,
                    |reply| ApiCall::GetParticipant(identity.clone(), reply),
                    timeout,
                )
                .await?
                .map_err(op_error);
        }
        let inbox = self
            .inbox(key)
            .ok_or_else(|| ApiError::NotFound("participant does not exist".into()))?;
        let (tx, rx) = oneshot::channel();
        call(
            &inbox,
            RoomInput::Api(ApiCall::GetStoredParticipant(identity.clone(), tx)),
            rx,
        )
        .await?
        .map_err(op_error)
    }

    pub async fn list_participants(&self, key: &RoomKey) -> Result<Vec<ParticipantInfo>, ApiError> {
        let Some(inbox) = self.inbox(key) else {
            return Ok(Vec::new());
        };
        let (tx, rx) = oneshot::channel();
        if inbox
            .send(RoomInput::Api(ApiCall::ListParticipants(tx)))
            .await
            .is_err()
        {
            return Ok(Vec::new());
        }
        Ok(rx.await.unwrap_or_default())
    }

    pub async fn get_participant(
        &self,
        key: &RoomKey,
        identity: &Identity,
    ) -> Result<ParticipantInfo, ApiError> {
        let inbox = self
            .inbox(key)
            .ok_or_else(|| ApiError::NotFound("participant does not exist".into()))?;
        let (tx, rx) = oneshot::channel();
        call(
            &inbox,
            RoomInput::Api(ApiCall::GetParticipant(identity.clone(), tx)),
            rx,
        )
        .await?
        .map_err(op_error)
    }

    pub async fn remove_participant(
        &self,
        key: &RoomKey,
        identity: &Identity,
    ) -> Result<(), ApiError> {
        let inbox = self
            .inbox(key)
            .ok_or_else(|| ApiError::NotFound("participant does not exist".into()))?;
        let (tx, rx) = oneshot::channel();
        call(
            &inbox,
            RoomInput::Api(ApiCall::RemoveParticipant(identity.clone(), tx)),
            rx,
        )
        .await?
        .map_err(op_error)
    }

    pub async fn mute_published_track(
        &self,
        key: &RoomKey,
        identity: &Identity,
        track_sid: &TrackSid,
        muted: bool,
    ) -> Result<Option<TrackInfo>, ApiError> {
        let inbox = self
            .inbox(key)
            .ok_or_else(|| ApiError::Unavailable(NO_RESPONSE.into()))?;
        let (tx, rx) = oneshot::channel();
        let input = RoomInput::Api(ApiCall::MuteTrack {
            identity: identity.clone(),
            track: track_sid.clone(),
            muted,
            reply: tx,
        });
        // Upstream has no store pre-check here: a missing participant is "no response".
        call(&inbox, input, rx).await?.map_err(|e| match e {
            OpError::NotFound(_) => ApiError::Unavailable(NO_RESPONSE.into()),
            other => op_error(other),
        })
    }

    pub async fn update_participant(
        &self,
        key: &RoomKey,
        identity: &Identity,
        update: ParticipantUpdate,
    ) -> Result<ParticipantInfo, ApiError> {
        let inbox = self
            .inbox(key)
            .ok_or_else(|| ApiError::NotFound("participant does not exist".into()))?;
        let (tx, rx) = oneshot::channel();
        let input = RoomInput::Api(ApiCall::UpdateParticipant {
            identity: identity.clone(),
            update,
            reply: tx,
        });
        call(&inbox, input, rx).await?.map_err(op_error)
    }

    pub async fn update_subscriptions(
        &self,
        key: &RoomKey,
        identity: &Identity,
        track_sids: Vec<TrackSid>,
        subscribe: bool,
    ) -> Result<(), ApiError> {
        let inbox = self
            .inbox(key)
            .ok_or_else(|| ApiError::Unavailable(NO_RESPONSE.into()))?;
        let (tx, rx) = oneshot::channel();
        let input = RoomInput::Api(ApiCall::UpdateSubscriptions {
            identity: identity.clone(),
            track_sids,
            subscribe,
            reply: tx,
        });
        call(&inbox, input, rx).await?.map_err(|e| match e {
            OpError::NotFound(_) => ApiError::Unavailable(NO_RESPONSE.into()),
            other => op_error(other),
        })
    }

    pub async fn send_data(
        &self,
        key: &RoomKey,
        kind: DataKind,
        payload: Vec<u8>,
        destination_identities: Vec<Identity>,
        destination_sids: Vec<ParticipantSid>,
    ) -> Result<(), ApiError> {
        self.send_data_with_nonce(
            key,
            kind,
            payload,
            destination_identities,
            destination_sids,
            Vec::new(),
        )
        .await
    }

    /// RoomService `SendData`: a non-empty `UserPacket.nonce` already seen in
    /// the room's 2 s window is dropped like upstream room.go2128–2131, and the
    /// call still succeeds.
    pub async fn send_data_with_nonce(
        &self,
        key: &RoomKey,
        kind: DataKind,
        payload: Vec<u8>,
        destination_identities: Vec<Identity>,
        destination_sids: Vec<ParticipantSid>,
        nonce: Vec<u8>,
    ) -> Result<(), ApiError> {
        let inbox = self
            .inbox(key)
            .ok_or_else(|| ApiError::Unavailable(NO_RESPONSE.into()))?;
        let (tx, rx) = oneshot::channel();
        let input = RoomInput::Api(ApiCall::SendData {
            kind,
            payload,
            destination_identities,
            destination_sids,
            nonce,
            reply: tx,
        });
        call(&inbox, input, rx).await
    }

    pub async fn update_room_metadata(
        &self,
        key: &RoomKey,
        metadata: String,
    ) -> Result<RoomInfo, ApiError> {
        let inbox = self
            .inbox(key)
            .ok_or_else(|| ApiError::NotFound(ROOM_NOT_FOUND.into()))?;
        let (tx, rx) = oneshot::channel();
        call(
            &inbox,
            RoomInput::Api(ApiCall::UpdateMetadata(metadata, tx)),
            rx,
        )
        .await
    }

    /// Capture a trusted owner's actual inbox under the room-generation gate.
    /// SID validation and inbox selection are atomic with owner registration.
    pub fn capture_room_owner(
        &self,
        key: &RoomKey,
        expected: &RoomSid,
    ) -> Result<RoomOwnerHandle, ApiError> {
        let rooms = self.rooms();
        let entry = rooms
            .get(key)
            .filter(|entry| !entry.inbox.is_closed())
            .ok_or_else(|| ApiError::NotFound(ROOM_NOT_FOUND.into()))?;
        let snapshot = entry.snapshot.lock().unwrap_or_else(|p| p.into_inner());
        if snapshot.info.sid != *expected {
            return Err(ApiError::FailedPrecondition(
                "shared room generation does not match local owner".into(),
            ));
        }
        Ok(RoomOwnerHandle {
            key: key.clone(),
            sid: expected.clone(),
            inbox: entry.inbox.clone(),
        })
    }

    /// Wait only the actually captured old generation, never a replacement.
    pub async fn wait_room_generation_closed(&self, key: &RoomKey, sid: &RoomSid) {
        let inbox = {
            let rooms = self.rooms();
            rooms
                .get(key)
                .filter(|entry| {
                    entry
                        .snapshot
                        .lock()
                        .unwrap_or_else(|p| p.into_inner())
                        .info
                        .sid
                        == *sid
                })
                .map(|entry| entry.inbox.clone())
        };
        if let Some(inbox) = inbox {
            inbox.closed().await;
        }
    }

    /// Insert into the existing room's actor-owned dispatch registry.
    /// Auto-creation, authentication and deployment/size checks belong to adapters.
    pub async fn create_agent_dispatch(
        &self,
        key: &RoomKey,
        options: AgentDispatchOptions,
    ) -> Result<AgentDispatchRecord, ApiError> {
        let inbox = self.dispatch_inbox(key)?;
        let (tx, rx) = oneshot::channel();
        call(
            &inbox,
            RoomInput::Api(ApiCall::CreateAgentDispatch(options, tx)),
            rx,
        )
        .await
    }

    /// A filter that does not match returns an empty list, as upstream does.
    pub async fn list_agent_dispatches(
        &self,
        key: &RoomKey,
        dispatch_id: Option<&str>,
    ) -> Result<Vec<AgentDispatchRecord>, ApiError> {
        let inbox = self.dispatch_inbox(key)?;
        let (tx, rx) = oneshot::channel();
        call(
            &inbox,
            RoomInput::Api(ApiCall::ListAgentDispatches(
                dispatch_id.filter(|id| !id.is_empty()).map(str::to_owned),
                tx,
            )),
            rx,
        )
        .await
    }

    /// Remove and return the existing record; absent IDs are not_found.
    pub async fn delete_agent_dispatch(
        &self,
        key: &RoomKey,
        dispatch_id: &str,
    ) -> Result<AgentDispatchRecord, ApiError> {
        let inbox = self.dispatch_inbox(key)?;
        let (tx, rx) = oneshot::channel();
        call(
            &inbox,
            RoomInput::Api(ApiCall::DeleteAgentDispatch(dispatch_id.into(), tx)),
            rx,
        )
        .await?
    }

    fn dispatch_inbox(&self, key: &RoomKey) -> Result<mpsc::Sender<RoomInput>, ApiError> {
        self.inbox(key)
            .filter(|inbox| !inbox.is_closed())
            .ok_or_else(|| ApiError::NotFound(ROOM_NOT_FOUND.into()))
    }

    /// Record one successfully decoded or written external signaling message.
    /// Callers supply application payload size, excluding websocket framing,
    /// compression, control frames and any internal cluster relay.
    /// Actual service/transport lifecycle boundaries, separate from bytes and
    /// actor projections. Called only by the owner or successful WS first write.
    pub fn record_rtc_init(&self) {
        self.inner.process_stats.record_rtc_init();
    }
    pub fn record_rtc_canceled(&self, count: u64) {
        self.inner.process_stats.record_rtc_canceled(count);
    }
    pub fn record_signal_connected(&self) {
        self.inner.process_stats.record_signal_connected();
    }
    pub fn record_signaling(&self, bytes: usize, outgoing: bool) {
        self.inner.process_stats.record_signaling(bytes, outgoing);
    }

    pub fn node_stats(&self) -> NodeStats {
        let mut stats = NodeStats {
            forward_stats: self
                .inner
                .deps
                .config
                .rtc
                .forward_stats
                .as_ref()
                .map_or_else(Default::default, |collector| collector.snapshot()),
            started_at_ms: self.inner.started_at_ms,
            ..self.inner.process_stats.snapshot()
        };
        for entry in self.rooms().values() {
            let s = entry.snapshot.lock().unwrap_or_else(|p| p.into_inner());
            stats.rooms += 1;
            stats.participants += s.participants;
            stats.tracks_in += s.tracks_in;
            stats.tracks_out += s.tracks_out;
            stats.estimated_capacity_bps += s.estimated_capacity_bps;
            stats.allocated_bps += s.allocated_bps;
            stats.allocation_overshoot_bps += s.allocation_overshoot_bps;
            stats.paused_video_tracks += s.paused_video_tracks;
        }
        stats
    }

    // ----- lifecycle -----

    /// Stop creating rooms and wait until every participant left on its own
    /// (first SIGTERM upstream). Logs progress every 5 s.
    pub async fn drain(&self) {
        self.inner.draining.store(true, Ordering::Relaxed);
        let mut waited = Duration::ZERO;
        loop {
            let stats = self.node_stats();
            if stats.participants == 0 {
                return;
            }
            if waited.as_secs().is_multiple_of(5) {
                tracing::info!(
                    participants = stats.participants,
                    "waiting for participants to exit"
                );
            }
            tokio::time::sleep(Duration::from_secs(1)).await;
            waited += Duration::from_secs(1);
        }
    }

    /// Join accepted recording tasks only after adapter global RPC cancellation.
    /// Normal room closure must never await potentially-unlimited worker retries.
    pub async fn drain_recording_work(&self) {
        self.inner.recording.drain().await;
    }

    /// Close every room with SERVER_SHUTDOWN and wait (bounded) for the tasks.
    pub async fn close_all(&self) {
        self.inner.draining.store(true, Ordering::Relaxed);
        let inboxes: Vec<mpsc::Sender<RoomInput>> =
            self.rooms().values().map(|e| e.inbox.clone()).collect();
        for inbox in &inboxes {
            let _ = inbox
                .send(RoomInput::Close(RoomEndReason::ServerShutdown))
                .await;
        }
        for _ in 0..50 {
            if self.rooms().is_empty() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        self.inner.recording.close_admission();
        self.inner.deps.net.shutdown_and_wait().await;
        self.inner.forward_stats_worker.stop();
    }
}

async fn call<T>(
    inbox: &mpsc::Sender<RoomInput>,
    input: RoomInput,
    rx: oneshot::Receiver<T>,
) -> Result<T, ApiError> {
    inbox
        .send(input)
        .await
        .map_err(|_| ApiError::Unavailable(NO_RESPONSE.into()))?;
    rx.await
        .map_err(|_| ApiError::Unavailable(NO_RESPONSE.into()))
}

fn op_error(e: OpError) -> ApiError {
    match e {
        OpError::MetadataLimit(m) => ApiError::MetadataLimit(m.into()),
        OpError::NotFound(m) => ApiError::NotFound(m.into()),
        OpError::FailedPrecondition(m) => ApiError::FailedPrecondition(m.into()),
    }
}

#[cfg(test)]
mod participant_reply_failure_tests {
    use super::*;
    #[tokio::test]
    async fn participant_owner_reply_loss_is_an_error_not_an_empty_vector() {
        // Exact production channel helper, explicit channel-failure boundary;
        // this is not a substitute for the real late-room-owner test.
        let (inbox, mut input) = mpsc::channel(1);
        let (reply, result) = oneshot::channel::<Vec<ParticipantInfo>>();
        let caller = tokio::spawn(async move {
            call(
                &inbox,
                RoomInput::Api(ApiCall::ListParticipants(reply)),
                result,
            )
            .await
        });
        let Some(RoomInput::Api(ApiCall::ListParticipants(reply))) = input.recv().await else {
            panic!("actual list request");
        };
        drop(reply);
        assert!(
            matches!(caller.await.unwrap(), Err(ApiError::Unavailable(message)) if message == "no response from servers")
        );
    }
}
