//! The room task: owns one [`Room`] and its [`MediaEngine`], executes effects and
//! drives media I/O and timers. Everything that touches the forwarding graph runs
//! here, sequentially (§4.4: single owner per room, no global locks).

use std::cell::RefCell;
use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use rtc_core::config::ServerConfig;
use rtc_core::events::{EngineEvent, EventSink, RoomEndReason};
use rtc_core::ids::*;
use rtc_core::media_api::{DataKind, MediaCommand, MediaEvent};
use rtc_core::model::*;
use rtc_core::room::{Effect, OpError, Room};
use rtc_core::session::{
    CandidateInit, JoinError, JoinParams, SessionOutput, SessionRequest, SignalEvent, SignalTarget,
};
use rtc_media::{Datagram, IceCredentials, MediaEngine, MediaEngineConfig, PacketOut, Proto};
use tokio::sync::{mpsc, oneshot, watch};
use tracing::Instrument;

use crate::api::{
    AgentDispatchOptions, AgentDispatchRecord, ApiError, ParticipantUpdate, SessionHandle,
};
use crate::net::{
    InboundPacket, LocalCandidate as NetCandidate, NetProto, NetRouter, NetSender, SrflxGather,
    mdns,
};

/// Control inbox depth per room.
pub(crate) const ROOM_INBOX: usize = 1024;
/// Media packets queued towards one room before the socket readers drop.
pub(crate) const ROOM_PACKET_QUEUE: usize = 8192;
/// Signal messages queued towards one session; a slower client is disconnected.
const SESSION_QUEUE: usize = 1024;
/// Packets processed per wakeup before checking other inputs.
const PACKET_BATCH: usize = 256;
/// Diagnostic threshold for `slow media loop iteration` (debug level).
const SLOW_ITERATION: std::time::Duration = std::time::Duration::from_millis(15);
const IDLE_CHECK: Duration = Duration::from_secs(1);
/// Upper bound for one sleep so stats snapshots stay fresh.
const MAX_SLEEP: Duration = Duration::from_millis(500);

pub(crate) enum ApiCall {
    GetRoomInfo(oneshot::Sender<RoomInfo>),
    /// One owner-turn view; never the API participant-store cache.
    GetDebugRoom(oneshot::Sender<rtc_core::room::DebugRoomSnapshot>),
    PublishedMedia {
        identity: Identity,
        sid: ParticipantSid,
        reply: oneshot::Sender<Result<Vec<rtc_core::media_api::PublishedMediaState>, ApiError>>,
    },
    ListParticipants(oneshot::Sender<Vec<ParticipantInfo>>),
    ListStoredParticipants(oneshot::Sender<Vec<ParticipantInfo>>),
    GetStoredParticipant(Identity, oneshot::Sender<Result<ParticipantInfo, OpError>>),
    GetParticipant(Identity, oneshot::Sender<Result<ParticipantInfo, OpError>>),
    RpcParticipant(
        Identity,
        oneshot::Sender<Result<(ParticipantSid, u32), OpError>>,
    ),
    RemoveParticipant(Identity, oneshot::Sender<Result<(), OpError>>),
    MuteTrack {
        identity: Identity,
        track: TrackSid,
        muted: bool,
        reply: oneshot::Sender<Result<Option<TrackInfo>, OpError>>,
    },
    UpdateParticipant {
        identity: Identity,
        update: ParticipantUpdate,
        reply: oneshot::Sender<Result<ParticipantInfo, OpError>>,
    },
    UpdateSubscriptions {
        identity: Identity,
        track_sids: Vec<TrackSid>,
        subscribe: bool,
        reply: oneshot::Sender<Result<(), OpError>>,
    },
    SendData {
        kind: DataKind,
        payload: Vec<u8>,
        destination_identities: Vec<Identity>,
        destination_sids: Vec<ParticipantSid>,
        nonce: Vec<u8>,
        reply: oneshot::Sender<()>,
    },
    UpdateMetadata(String, oneshot::Sender<RoomInfo>),
    CreateAgentDispatch(AgentDispatchOptions, oneshot::Sender<AgentDispatchRecord>),
    ListAgentDispatches(Option<String>, oneshot::Sender<Vec<AgentDispatchRecord>>),
    AgentParticipantScope {
        room_sid: RoomSid,
        identity: Identity,
        reply: oneshot::Sender<Result<crate::api::AgentParticipantLookup, ApiError>>,
    },
    RemoveAgentParticipant {
        room_sid: RoomSid,
        identity: Identity,
        participant_sid: ParticipantSid,
        retired_signal_generation: Option<SessionGeneration>,
        reply: oneshot::Sender<Result<bool, ApiError>>,
    },
    CommitAgentJob {
        room_sid: RoomSid,
        job: Box<crate::api::AgentJobRecord>,
        reply: oneshot::Sender<Result<(), ApiError>>,
    },
    DeleteAgentDispatch(
        String,
        oneshot::Sender<Result<AgentDispatchRecord, ApiError>>,
    ),
}

pub(crate) enum RoomInput {
    NamedSubscriptionsPrepared {
        sid: ParticipantSid,
        generation: SessionGeneration,
        targets: Vec<rtc_core::session::NamedSubscription>,
        reply: oneshot::Sender<bool>,
    },
    ApplySessionRequest {
        sid: ParticipantSid,
        generation: SessionGeneration,
        request: SessionRequest,
        reply: oneshot::Sender<bool>,
    },

    Join {
        params: Box<JoinParams>,
        session_started_at: Instant,
        reply: oneshot::Sender<Result<SessionHandle, JoinError>>,
    },
    Request {
        sid: ParticipantSid,
        generation: SessionGeneration,
        request: SessionRequest,
    },
    SignalClosed {
        sid: ParticipantSid,
        generation: SessionGeneration,
    },
    Api(ApiCall),
    Close(RoomEndReason),
}

/// Live snapshot shared with the manager for listing and stats.
#[derive(Clone, Debug)]
pub(crate) struct RoomSnapshot {
    pub info: RoomInfo,
    pub participants: u32,
    pub tracks_in: u32,
    pub tracks_out: u32,
    pub bytes_in: u64,
    pub bytes_out: u64,
    pub packets_in: u64,
    pub packets_out: u64,
    pub nacks: u64,
    pub retransmit_bytes_out: u64,
    pub retransmit_packets_out: u64,
    pub estimated_capacity_bps: u64,
    pub allocated_bps: u64,
    pub allocation_overshoot_bps: u64,
    pub paused_video_tracks: u64,
    pub probe_attempts: u64,
    pub probe_successes: u64,
    pub probe_failures: u64,
    pub congestion_events: u64,
}

pub(crate) type SharedSnapshot = Arc<Mutex<RoomSnapshot>>;

struct NetOut(NetSender);

impl PacketOut for NetOut {
    fn for_transport(&self, transport: TransportId) -> Option<Box<dyn PacketOut>> {
        self.0
            .for_transport(transport)
            .map(|sender| Box::new(OwnedNetOut(sender)) as Box<dyn PacketOut>)
    }

    fn send(
        &self,
        proto: Proto,
        source: std::net::SocketAddr,
        destination: std::net::SocketAddr,
        data: &[u8],
    ) {
        let proto = match proto {
            Proto::Udp => NetProto::Udp,
            Proto::Tcp => NetProto::Tcp,
        };
        self.0.send(proto, source, destination, data);
    }
}

struct OwnedNetOut(crate::net::OwnedNetSender);
impl PacketOut for OwnedNetOut {
    fn send(
        &self,
        proto: Proto,
        source: std::net::SocketAddr,
        destination: std::net::SocketAddr,
        data: &[u8],
    ) {
        let proto = match proto {
            Proto::Udp => NetProto::Udp,
            Proto::Tcp => NetProto::Tcp,
        };
        self.0.send(proto, source, destination, data);
    }
}

struct Session {
    one_shot: bool,
    generation: SessionGeneration,
    tx: mpsc::Sender<SessionOutput>,
}

/// Remote mDNS candidate handling (`rtc.use_mdns`).
///
/// Upstream turns pion's mDNS off when the flag is unset and drops remote
/// `.local`/`.invalid` candidates at the peer-connection boundary
/// (`pkg/rtc/transport.go:2494`/`:2566`); when set, pion runs in `QueryOnly`
/// mode and resolves remote `.local` candidates before using them
/// (`livekit-ice/agent.go:1048`).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum MdnsPolicy {
    /// Discard remote mDNS candidates.
    Disabled,
    /// Resolve `.local` candidates and accept them.
    Accept,
}

impl MdnsPolicy {
    fn from_config(use_mdns: bool) -> Self {
        if use_mdns {
            MdnsPolicy::Accept
        } else {
            MdnsPolicy::Disabled
        }
    }
}

struct OwnerLifecycle {
    service: Arc<dyn crate::RoomOwnerLifecycle>,
    owner: crate::RoomOwnerHandle,
    ready: crate::manager::OwnerReadySender,
}

pub(crate) struct RoomTask {
    owner_lifecycle: Option<OwnerLifecycle>,
    deferred_joins: Vec<(
        oneshot::Sender<Result<SessionHandle, JoinError>>,
        SessionHandle,
    )>,
    room: Room,
    recording: Option<rtc_core::auto_egress::RecordingLifecycle>,
    recording_executor: Option<Arc<crate::automatic_recording::RecordingExecutor>>,
    engine: MediaEngine,
    net: NetRouter,
    events: Arc<dyn EventSink>,
    idle_hold: Arc<crate::idle_hold::IdleHoldState>,
    sessions: HashMap<ParticipantSid, Session>,
    // Agent cleanup observes actual room signal state, even after the output
    // slot closes but before the matching transport handle reports EOF.
    agent_signal_retirements: HashMap<ParticipantSid, (SessionGeneration, watch::Sender<()>)>,
    agent_admissions: watch::Sender<()>,
    // The task is the sole owner; disposal never survives into a same-name room.
    agent_dispatches: BTreeMap<String, AgentDispatchRecord>,
    agent_events: crate::api::AgentEventSender,
    agent_publishers: HashSet<ParticipantSid>,
    agent_outbox: RefCell<VecDeque<crate::api::AgentWorkEvent>>,
    inbox_tx: mpsc::Sender<RoomInput>,
    inbox: mpsc::Receiver<RoomInput>,
    packet_tx: mpsc::Sender<InboundPacket>,
    packets: mpsc::Receiver<InboundPacket>,
    snapshot: SharedSnapshot,
    process_stats: Option<crate::process_stats::Reporter>,
    room_deadline: Instant,
    next_idle_check: Instant,
    media_events: Vec<MediaEvent>,
    next_data_completion: u64,
    data_completions: HashMap<u64, oneshot::Sender<()>>,
    ufrag_changes: Vec<(TransportId, String)>,
    srflx_jobs: tokio::task::JoinSet<SrflxGather>,
    mdns: MdnsPolicy,
    /// Addresses remote mDNS queries are sent to: the mDNS groups in
    /// production. Tests point this at a unicast responder so resolution is
    /// deterministic.
    mdns_targets: Vec<std::net::SocketAddr>,
}

impl RoomTask {
    pub(crate) fn new(
        room: Room,
        cfg: &ServerConfig,
        net: NetRouter,
        events: Arc<dyn EventSink>,
        snapshot: SharedSnapshot,
        inbox_tx: mpsc::Sender<RoomInput>,
        inbox: mpsc::Receiver<RoomInput>,
    ) -> Result<RoomTask, String> {
        // Match sleep_until's monotonic clock; unpaused Tokio wraps std time.
        // Tests can pause both owner and native peer without splitting epochs.
        let now = tokio::time::Instant::now().into_std();
        let engine_cfg = MediaEngineConfig {
            rtc: cfg.rtc.clone(),
            audio: cfg.audio.clone(),
            video: cfg.video.clone(),
            enabled_codecs: cfg.room.enabled_codecs.clone(),
            playout_delay: room.playout_delay(),
            sif_trailer: room.sif_trailer().to_vec(),
        };
        let engine = MediaEngine::new(engine_cfg, Box::new(NetOut(net.sender())), now)
            .map_err(|e| e.to_string())?;
        let (packet_tx, packets) = mpsc::channel(ROOM_PACKET_QUEUE);
        Ok(RoomTask {
            owner_lifecycle: None,
            deferred_joins: Vec::new(),
            room,
            recording: None,
            recording_executor: None,
            engine,
            net,
            events,
            idle_hold: Arc::new(crate::idle_hold::IdleHoldState::default()),
            sessions: HashMap::new(),
            agent_signal_retirements: HashMap::new(),
            agent_admissions: watch::channel(()).0,
            agent_dispatches: BTreeMap::new(),
            agent_events: Arc::new(Mutex::new(None)),
            agent_publishers: HashSet::new(),
            agent_outbox: RefCell::new(VecDeque::new()),
            inbox_tx,
            inbox,
            packet_tx,
            packets,
            snapshot,
            process_stats: None,
            room_deadline: now,
            next_idle_check: now + IDLE_CHECK,
            media_events: Vec::new(),
            next_data_completion: 0,
            data_completions: HashMap::new(),
            ufrag_changes: Vec::new(),
            srflx_jobs: tokio::task::JoinSet::new(),
            mdns: MdnsPolicy::from_config(cfg.rtc.use_mdns),
            mdns_targets: mdns::GROUPS.to_vec(),
        })
    }

    pub(crate) fn set_process_stats(&mut self, reporter: crate::process_stats::Reporter) {
        self.process_stats = Some(reporter);
    }

    pub(crate) fn set_owner_lifecycle(
        &mut self,
        lifecycle: Option<Arc<dyn crate::RoomOwnerLifecycle>>,
        owner: crate::RoomOwnerHandle,
        ready: crate::manager::OwnerReadySender,
    ) {
        self.owner_lifecycle = lifecycle.map(|service| OwnerLifecycle {
            service,
            owner,
            ready,
        });
    }

    /// Apply the remote mDNS policy and hand the request to the room.
    ///
    /// Upstream filters and resolves candidate addresses at the peer-connection
    /// boundary (`transport.go:2494`, `:2566`), so the room and media engine only
    /// ever see usable candidates.
    fn dispatch_session_request(
        &mut self,
        sid: ParticipantSid,
        generation: SessionGeneration,
        request: SessionRequest,
        now: Instant,
    ) {
        let Some(request) = self.mdns_filter(&sid, generation, request) else {
            return;
        };
        self.room.handle_request(&sid, generation, request, now);
    }

    fn mdns_filter(
        &self,
        sid: &ParticipantSid,
        generation: SessionGeneration,
        request: SessionRequest,
    ) -> Option<SessionRequest> {
        match request {
            SessionRequest::Offer { sdp, id } => {
                let (sdp, extracted) = mdns::split_sdp_mdns_candidates(&sdp);
                self.forward_mdns_candidates(sid, generation, extracted, SignalTarget::Publisher);
                Some(SessionRequest::Offer { sdp, id })
            }
            SessionRequest::Answer { sdp, id } => {
                let (sdp, extracted) = mdns::split_sdp_mdns_candidates(&sdp);
                self.forward_mdns_candidates(sid, generation, extracted, SignalTarget::Subscriber);
                Some(SessionRequest::Answer { sdp, id })
            }
            SessionRequest::PrepareOneShotOffer { sdp, id } => {
                let (sdp, extracted) = mdns::split_sdp_mdns_candidates(&sdp);
                self.forward_mdns_candidates(sid, generation, extracted, SignalTarget::Publisher);
                Some(SessionRequest::PrepareOneShotOffer { sdp, id })
            }
            SessionRequest::Trickle {
                token,
                candidate,
                target,
                last,
            } => {
                if mdns::candidate_address(&candidate.candidate)
                    .is_some_and(|a| mdns::classify(a).is_some())
                {
                    let extracted = mdns::ExtractedCandidate {
                        line: candidate.candidate,
                        mid: candidate.sdp_mid,
                        mline: candidate.sdp_mline_index,
                    };
                    self.forward_mdns_candidates(sid, generation, vec![extracted], target);
                    None
                } else {
                    Some(SessionRequest::Trickle {
                        token,
                        candidate,
                        target,
                        last,
                    })
                }
            }
            other => Some(other),
        }
    }

    /// Accept-and-resolve with mDNS on, drop with it off, mirroring
    /// `IsCandidateMDNS` filtering at the peer-connection boundary.
    fn forward_mdns_candidates(
        &self,
        sid: &ParticipantSid,
        generation: SessionGeneration,
        extracted: Vec<mdns::ExtractedCandidate>,
        target: SignalTarget,
    ) {
        for candidate in extracted {
            let Some(address) = mdns::candidate_address(&candidate.line) else {
                continue;
            };
            match mdns::classify(address) {
                Some(mdns::MdnsKind::Local) if self.mdns == MdnsPolicy::Accept => {
                    self.spawn_mdns_resolution(sid.clone(), generation, candidate, target);
                }
                Some(kind) => {
                    // Go `Debugw("ignoring mDNS candidate", ...)`; `.invalid`
                    // names never resolve, so they are dropped for the same
                    // reason they can never produce a valid pair upstream.
                    tracing::debug!(candidate = %candidate.line, kind = ?kind, "ignoring mDNS candidate");
                }
                None => {}
            }
        }
    }

    /// Resolve one `.local` candidate off the room loop, then re-inject it as a
    /// trickle candidate carrying the resolved IP (pion rewrites the candidate
    /// address in `resolveAndAddMulticastCandidate`).
    fn spawn_mdns_resolution(
        &self,
        sid: ParticipantSid,
        generation: SessionGeneration,
        candidate: mdns::ExtractedCandidate,
        target: SignalTarget,
    ) {
        let Some(name) = mdns::candidate_address(&candidate.line) else {
            return;
        };
        let name = name.to_string();
        let tx = self.inbox_tx.clone();
        let targets = self.mdns_targets.clone();
        tokio::spawn(async move {
            let Some(ip) = mdns::resolve_via(&name, mdns::QUERY_TIMEOUT, &targets).await else {
                tracing::warn!(name = %name, "failed to discover mDNS candidate");
                return;
            };
            let Some(rewritten) = mdns::rewrite_candidate_address(&candidate.line, &ip.to_string())
            else {
                return;
            };
            let request = SessionRequest::Trickle {
                token: 0,
                candidate: CandidateInit {
                    candidate: rewritten,
                    sdp_mid: candidate.mid,
                    sdp_mline_index: candidate.mline,
                    username_fragment: None,
                },
                target,
                last: false,
            };
            let _ = tx
                .send(RoomInput::Request {
                    sid,
                    generation,
                    request,
                })
                .await;
        });
    }

    pub(crate) async fn run(self) {
        // Equivalent ownership to LoggerWithRoom(...).WithItemSampler(): one
        // independent sampler per room generation, retained across async polls.
        // No event-field inference: only this explicit logger scope opts in.
        let logger = tracing::info_span!("livekit.room_logger");
        self.run_with_logger().instrument(logger).await;
    }

    async fn run_with_logger(mut self) {
        if let Some(lifecycle) = &self.owner_lifecycle {
            let result = lifecycle.service.opened(lifecycle.owner.clone()).await;
            lifecycle.ready.send_replace(Some(result.clone()));
            if result.is_err() {
                self.events.cancel_room_reservation(&self.room.info());
                lifecycle
                    .service
                    .closed(lifecycle.owner.key().clone(), lifecycle.owner.sid().clone())
                    .await;
                self.inbox.close();
                while let Ok(input) = self.inbox.try_recv() {
                    if let RoomInput::Join { reply, .. } = input {
                        let _ = reply.send(Err(JoinError::RoomClosed));
                    }
                }
                return;
            }
        }

        // Effects of Room::new (room_started).
        self.settle(tokio::time::Instant::now().into_std());
        loop {
            // Guarantee forward progress for control messages even while the
            // packet queue stays ready. Media work remains bounded per wakeup.
            if let Ok(input) = self.inbox.try_recv() {
                self.on_input(input);
            }
            self.refresh_owner_participants().await;
            if self.room.is_closed() {
                break;
            }
            let now = tokio::time::Instant::now().into_std();
            let mut deadline = self
                .room_deadline
                .min(self.next_idle_check)
                .min(now + MAX_SLEEP);
            if let Some(t) = self.engine.poll_timeout() {
                deadline = deadline.min(t);
            }
            let sleep = tokio::time::sleep_until(tokio::time::Instant::from_std(deadline));

            let agent_sender = self
                .agent_events
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .clone();
            let has_agent_work = !self.agent_outbox.borrow().is_empty();
            let woke = std::cell::Cell::new(
                None::<(std::time::Instant, Option<std::time::Instant>, usize)>,
            );
            tokio::select! {
                permit = async { agent_sender.expect("attached sender for queued effects").reserve_owned().await }, if has_agent_work => {
                    match permit {
                        Ok(permit) => { if let Some(event) = self.agent_outbox.get_mut().pop_front() { permit.send(event); } }
                        Err(_) => { tracing::error!("agent bridge closed with unconsumed room work"); self.agent_outbox.get_mut().clear(); }
                    }
                }
                pkt = self.packets.recv() => {
                    let Some(pkt) = pkt else { break };
                    let started = std::time::Instant::now();
                    let oldest = pkt.received_at;
                    self.on_packet(pkt);
                    let mut batch = 1;
                    for _ in 0..PACKET_BATCH {
                        match self.packets.try_recv() {
                            Ok(pkt) => { self.on_packet(pkt); batch += 1; }
                            Err(_) => break,
                        }
                    }
                    woke.set(Some((started, Some(oldest), batch)));
                }
                result = self.srflx_jobs.join_next(), if !self.srflx_jobs.is_empty() => {
                    if let Some(Ok(result)) = result
                        && let Some((id, ufrag, candidates)) = self.net.admit_srflx(result)
                    {
                        self.engine.complete_srflx(
                            id, &ufrag, candidates, tokio::time::Instant::now().into_std(),
                        );
                    }
                }
                input = self.inbox.recv() => {
                    let Some(input) = input else { break };
                    self.on_input(input);
                }
                _ = sleep => {}
            }

            let now = tokio::time::Instant::now().into_std();
            self.engine.handle_timeout(now);
            if now >= self.room_deadline {
                self.room_deadline = self.room.tick(now);
            }
            if now >= self.next_idle_check {
                self.next_idle_check = now + IDLE_CHECK;
                if self.room.should_close() {
                    let held = self.idle_hold.clone();
                    held.close_if_unheld(|| self.room.close(RoomEndReason::IdleTimeout, now));
                }
            }
            self.settle(now);
            self.refresh_owner_participants().await;
            self.publish_snapshot();
            if let Some((started, oldest, batch)) = woke.get() {
                // Diagnostic only: a media loop iteration (packet batch plus the
                // following timers) delaying forwarded media past a frame interval.
                let busy = started.elapsed();
                let queued = oldest.map(|at| started.saturating_duration_since(at));
                if busy >= SLOW_ITERATION || queued.is_some_and(|q| q >= SLOW_ITERATION) {
                    tracing::debug!(
                        busy_ms = busy.as_secs_f64() * 1e3,
                        queued_ms = queued.map(|q| q.as_secs_f64() * 1e3),
                        batch,
                        timers_ms = now.elapsed().as_secs_f64() * 1e3,
                        "slow media loop iteration"
                    );
                }
            }

            if self.room.is_closed() {
                break;
            }
        }
        self.shutdown().await;
        self.flush_agent_outbox().await;
    }

    async fn refresh_owner_participants(&mut self) {
        if let Some(lifecycle) = &self.owner_lifecycle {
            let result = lifecycle
                .service
                .participants(
                    lifecycle.owner.clone(),
                    self.room.list_participants_stored(),
                )
                .await;
            if let Err(error) = result {
                tracing::error!(%error, "participant RPC topic admission failed");
                self.room.close(
                    RoomEndReason::Unknown,
                    tokio::time::Instant::now().into_std(),
                );
            }
        }
        for (reply, handle) in self.deferred_joins.drain(..) {
            let _ = reply.send(if self.room.is_closed() {
                Err(JoinError::RoomClosed)
            } else {
                Ok(handle)
            });
        }
    }

    fn on_packet(&mut self, pkt: InboundPacket) {
        let proto = match pkt.proto {
            NetProto::Udp => Proto::Udp,
            NetProto::Tcp => Proto::Tcp,
        };
        self.engine.handle_datagram(Datagram {
            transport: pkt.transport,
            proto,
            source: pkt.source,
            destination: pkt.bound,
            data: &pkt.data,
            received_at: pkt.received_at,
        });
    }

    fn on_input(&mut self, input: RoomInput) {
        let now = tokio::time::Instant::now().into_std();
        match input {
            RoomInput::Join {
                params,
                session_started_at,
                reply,
            } => {
                let identity = params.identity.clone();
                let one_shot = params.capabilities.one_shot;
                match self
                    .room
                    .join_with_session_start(*params, now, session_started_at)
                {
                    Ok(joined) => {
                        let resumed = joined.resumed;
                        let (tx, rx) = mpsc::channel(SESSION_QUEUE);
                        // Replacing the entry drops the old sender: the previous
                        // signal connection (resume) closes after draining.
                        self.agent_signal_retirements.remove(&joined.sid);
                        self.sessions.insert(
                            joined.sid.clone(),
                            Session {
                                one_shot,
                                generation: joined.generation,
                                tx,
                            },
                        );
                        let handle = SessionHandle {
                            participant_sid: joined.sid,
                            identity,
                            generation: joined.generation,
                            room: self.inbox_tx.clone(),
                            output: rx,
                        };
                        if let Some(recording) = self.recording.as_mut() {
                            recording.registered(
                                &self.room.info().sid,
                                handle.identity.clone(),
                                handle.participant_sid.clone(),
                            );
                        }
                        // Deliver join effects before the caller starts reading.
                        self.settle(now);
                        if !resumed
                            && let Ok(participant) = self.room.get_participant(&handle.identity)
                        {
                            if participant.kind == ParticipantKind::Agent {
                                self.agent_admissions.send_replace(());
                            }
                            self.launch_agent_targets(None, Some(participant), 2);
                        }
                        if self.owner_lifecycle.is_some() {
                            self.deferred_joins.push((reply, handle));
                        } else {
                            let _ = reply.send(Ok(handle));
                        }
                    }
                    Err(e) => {
                        self.settle(now);
                        let _ = reply.send(Err(e));
                    }
                }
            }
            RoomInput::Request {
                sid,
                generation,
                request,
            } => {
                self.dispatch_session_request(sid, generation, request, now);
            }
            RoomInput::ApplySessionRequest {
                sid,
                generation,
                request,
                reply,
            } => {
                let current = self.room.participant_generation(&sid) == Some(generation);
                if current {
                    self.dispatch_session_request(sid, generation, request, now);
                }
                self.settle(now);
                let _ = reply.send(current);
            }
            RoomInput::NamedSubscriptionsPrepared {
                sid,
                generation,
                targets,
                reply,
            } => {
                let _ = reply.send(
                    self.room
                        .named_subscriptions_prepared(&sid, generation, &targets),
                );
            }
            RoomInput::SignalClosed { sid, generation } => {
                if self
                    .sessions
                    .get(&sid)
                    .is_some_and(|s| s.generation == generation)
                {
                    self.sessions.remove(&sid);
                }
                self.room.signal_closed(&sid, generation, now);
                if self
                    .agent_signal_retirements
                    .get(&sid)
                    .is_some_and(|(captured, _)| *captured == generation)
                {
                    self.agent_signal_retirements.remove(&sid);
                }
            }
            RoomInput::Api(call) => self.on_api(call, now),
            RoomInput::Close(reason) => {
                self.idle_hold.mark_closed();
                self.room.close(reason, now);
            }
        }
    }

    fn on_api(&mut self, call: ApiCall, now: Instant) {
        match call {
            ApiCall::GetDebugRoom(reply) => {
                let mut snapshot = self.room.debug_snapshot();
                snapshot.media = Some(self.engine.debug_snapshot(now));
                let _ = reply.send(snapshot);
            }
            ApiCall::GetRoomInfo(reply) => {
                self.project_directory();
                let _ = reply.send(self.room.info());
            }
            ApiCall::PublishedMedia {
                identity,
                sid,
                reply,
            } => {
                let result = self
                    .room
                    .get_participant(&identity)
                    .map_err(|_| ApiError::NotFound("participant does not exist".into()))
                    .and_then(|p| {
                        if p.sid != sid {
                            return Err(ApiError::NotFound("participant does not exist".into()));
                        }
                        Ok(p.tracks
                            .into_iter()
                            .map(|track| {
                                let average_bitrate =
                                    self.engine.published_average_bitrate(&track.sid, now);
                                rtc_core::media_api::PublishedMediaState {
                                    track,
                                    average_bitrate,
                                }
                            })
                            .collect())
                    });
                self.project_directory();
                let _ = reply.send(result);
            }
            ApiCall::ListStoredParticipants(reply) => {
                self.project_directory();
                let _ = reply.send(self.room.list_participants_stored());
            }
            ApiCall::GetStoredParticipant(identity, reply) => {
                self.project_directory();
                let _ = reply.send(self.room.get_participant_stored(&identity));
            }
            ApiCall::ListParticipants(reply) => {
                self.project_directory();
                let _ = reply.send(self.room.list_participants());
            }
            ApiCall::RpcParticipant(identity, reply) => {
                let _ = reply.send(self.room.rpc_participant(&identity));
            }
            ApiCall::GetParticipant(identity, reply) => {
                self.project_directory();
                let _ = reply.send(self.room.get_participant(&identity));
            }
            ApiCall::RemoveParticipant(identity, reply) => {
                let result = self.room.api_remove_participant(&identity, now);
                self.project_directory();
                let _ = reply.send(result);
            }
            ApiCall::MuteTrack {
                identity,
                track,
                muted,
                reply,
            } => {
                let result = self.room.api_mute_track(&identity, &track, muted, now);
                self.project_directory();
                let _ = reply.send(result);
            }
            ApiCall::UpdateParticipant {
                identity,
                update,
                reply,
            } => {
                let res = self.room.api_update_participant(
                    &identity,
                    &update.name,
                    &update.metadata,
                    &update.attributes,
                    update.permission,
                    now,
                );
                self.project_directory();
                let _ = reply.send(res);
            }
            ApiCall::UpdateSubscriptions {
                identity,
                track_sids,
                subscribe,
                reply,
            } => {
                let result =
                    self.room
                        .api_update_subscriptions(&identity, &track_sids, subscribe, now);
                self.project_directory();
                let _ = reply.send(result);
            }
            ApiCall::SendData {
                kind,
                payload,
                destination_identities,
                destination_sids,
                nonce,
                reply,
            } => {
                let id = self.next_data_completion;
                self.next_data_completion = id
                    .checked_add(1)
                    .expect("data completion identity exhausted");
                self.data_completions.insert(id, reply);
                self.room.api_send_data_tracked(
                    id,
                    kind,
                    payload,
                    &destination_identities,
                    &destination_sids,
                    &nonce,
                    now,
                );
                self.project_directory();
            }
            ApiCall::UpdateMetadata(metadata, reply) => {
                let result = self.room.api_update_metadata(metadata);
                self.project_directory();
                let _ = reply.send(result);
            }
            ApiCall::CreateAgentDispatch(options, reply) => {
                let dispatch = self.insert_agent_dispatch(options);
                for participant in self.room.list_participants() {
                    if participant.is_publisher {
                        self.launch_agent_targets(Some(&dispatch), Some(participant.clone()), 1);
                    }
                    self.launch_agent_targets(Some(&dispatch), Some(participant), 2);
                }
                self.project_directory();
                let _ = reply.send(dispatch);
            }
            ApiCall::AgentParticipantScope {
                room_sid,
                identity,
                reply,
            } => {
                let result = if self.room.is_closed() || self.room.info().sid != room_sid {
                    Err(ApiError::NotFound("room generation changed".into()))
                } else {
                    let scope = self.room.get_participant(&identity).ok().and_then(|p| {
                        if p.kind != ParticipantKind::Agent {
                            return None;
                        }
                        let (generation, connected) = self.room.participant_signal_state(&p.sid)?;
                        let session_retirement = connected.then(|| {
                            self.agent_signal_retirements
                                .entry(p.sid.clone())
                                .or_insert_with(|| (generation, watch::channel(()).0))
                                .1
                                .subscribe()
                        });
                        Some(crate::api::AgentParticipantScope {
                            participant_sid: p.sid,
                            signal_generation: generation,
                            signal_retired: !connected,
                            session_retirement,
                        })
                    });
                    Ok(match scope {
                        Some(scope) => crate::api::AgentParticipantLookup::Present(scope),
                        None => crate::api::AgentParticipantLookup::AwaitAdmission(
                            self.agent_admissions.subscribe(),
                        ),
                    })
                };
                self.project_directory();
                let _ = reply.send(result);
            }
            ApiCall::RemoveAgentParticipant {
                room_sid,
                identity,
                participant_sid,
                retired_signal_generation,
                reply,
            } => {
                let result = if self.room.info().sid != room_sid {
                    Err(ApiError::NotFound("room generation changed".into()))
                } else if self
                    .room
                    .get_participant(&identity)
                    .ok()
                    .is_some_and(|p| p.sid == participant_sid)
                {
                    if retired_signal_generation.is_some_and(|generation| {
                        self.room.participant_signal_state(&participant_sid)
                            != Some((generation, false))
                    }) {
                        Ok(false)
                    } else {
                        self.room
                            .api_remove_participant(&identity, now)
                            .map(|_| true)
                            .map_err(|e| ApiError::Internal(format!("{e:?}")))
                    }
                } else {
                    Err(ApiError::NotFound("participant generation changed".into()))
                };
                self.project_directory();
                let _ = reply.send(result);
            }
            ApiCall::CommitAgentJob {
                room_sid,
                job,
                reply,
            } => {
                let result = if self.room.is_closed() || self.room.info().sid != room_sid {
                    Err(ApiError::NotFound("room generation changed".into()))
                } else if let Some(dispatch) = self.agent_dispatches.get_mut(&job.dispatch_id) {
                    if !dispatch.jobs.iter().any(|old| old.id == job.id) {
                        dispatch.jobs.push(*job);
                    }
                    Ok(())
                } else {
                    Err(ApiError::NotFound("dispatch ID not found".into()))
                };
                self.project_directory();
                let _ = reply.send(result);
            }
            ApiCall::ListAgentDispatches(filter, reply) => {
                let records = self
                    .agent_dispatches
                    .values()
                    .filter(|record| filter.as_ref().is_none_or(|id| &record.id == id))
                    .cloned()
                    .collect();
                self.project_directory();
                let _ = reply.send(records);
            }
            ApiCall::DeleteAgentDispatch(id, reply) => {
                let result = self
                    .agent_dispatches
                    .remove(&id)
                    .ok_or_else(|| ApiError::NotFound("dispatch ID not found".into()));
                if result.is_ok() {
                    self.emit_agent_work(crate::api::AgentWorkEvent::DeleteDispatch {
                        key: self.room.key().clone(),
                        room_sid: self.room.info().sid,
                        dispatch_id: id,
                    });
                }
                self.project_directory();
                let _ = reply.send(result);
            }
        }
    }

    fn insert_agent_dispatch(&mut self, options: AgentDispatchOptions) -> AgentDispatchRecord {
        let created_at_ns = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos()
            .min(i64::MAX as u128) as i64;
        let dispatch = AgentDispatchRecord {
            jobs: Vec::new(),
            id: options.id,
            agent_name: options.agent_name,
            room: self.room.info().name.0,
            metadata: options.metadata,
            restart_policy: options.restart_policy,
            deployment: options.deployment,
            attributes: options.attributes,
            created_at_ns,
        };
        self.agent_dispatches
            .insert(dispatch.id.clone(), dispatch.clone());
        self.launch_agent_targets(Some(&dispatch), None, 0);
        dispatch
    }

    /// Initializes the new room's internal dispatch configuration exactly once,
    /// before its inbox is made visible. Empty internal lists mean automatic
    /// unnamed dispatch, not disabling agent discovery.
    pub(crate) fn initialize_agent_dispatches(
        &mut self,
        agents: Option<&[rtc_core::permission::RoomAgentDispatchOptions]>,
    ) {
        debug_assert!(self.agent_dispatches.is_empty());
        let default = [rtc_core::permission::RoomAgentDispatchOptions::default()];
        let agents = agents
            .filter(|agents| !agents.is_empty())
            .unwrap_or(&default);
        for (index, agent) in agents.iter().enumerate() {
            let valid = agent.deployment.len() <= 64
                && agent
                    .deployment
                    .bytes()
                    .all(|byte| byte != b'_' && byte > b' ' && byte != 0x7f);
            if !valid {
                tracing::warn!(
                    agent_index = index,
                    "skipping room agent with invalid deployment"
                );
                continue;
            }
            self.insert_agent_dispatch(AgentDispatchOptions {
                id: new_sid("AD_"),
                agent_name: agent.agent_name.clone(),
                metadata: agent.metadata.clone(),
                restart_policy: agent.restart_policy,
                deployment: agent.deployment.clone(),
                attributes: agent.attributes.clone(),
            });
        }
    }

    /// Run effects and media events until neither side produces more work.
    fn settle(&mut self, now: Instant) {
        // Bounded to avoid a livelock between room and media on a logic bug.
        for _ in 0..64 {
            let effects = self.room.take_effects();
            let had_effects = !effects.is_empty();
            self.project_directory();
            for effect in effects {
                self.execute(effect, now);
            }
            self.engine.drain_ufrag_changes(&mut self.ufrag_changes);
            for (t, ufrag) in self.ufrag_changes.drain(..) {
                let refresh_dedicated = self.net.uses_dedicated_udp();
                let candidates = self.net.update_ufrag(t, &ufrag, refresh_dedicated);
                self.net.start_srflx(t, &ufrag, false, &mut self.srflx_jobs);
                if refresh_dedicated || self.net.rotates_udp_mux() {
                    self.engine.replace_restarted_udp_host_candidates(
                        t,
                        candidates.into_iter().map(convert_candidate).collect(),
                    );
                    self.engine.complete_one_shot_answer(t, &ufrag, now);
                } else if !candidates.is_empty() {
                    self.engine.add_restarted_udp_candidates(
                        t,
                        candidates.into_iter().map(convert_candidate),
                    );
                }
            }
            self.engine.drain_events(&mut self.media_events);
            let had_events = !self.media_events.is_empty();
            let events = std::mem::take(&mut self.media_events);
            for ev in events {
                if let MediaEvent::DataFanoutCompleted { id } = ev {
                    if let Some(reply) = self.data_completions.remove(&id) {
                        let _ = reply.send(());
                    }
                    continue;
                }
                self.room.handle_media_event(ev, now);
            }
            if !had_effects && !had_events {
                return;
            }
        }
        tracing::warn!(room = %self.room.key().name, "room/media settle did not converge");
    }

    fn execute(&mut self, effect: Effect, now: Instant) {
        match effect {
            Effect::Signal { to, event } => {
                let Some(session) = self.sessions.get(&to) else {
                    return;
                };
                // Fixed one-shot source uses a null sink, not an interactive WS queue.
                // Only actual operation answers/errors belong to its owned HTTP adapter.
                if session.one_shot
                    && !matches!(
                        event,
                        SignalEvent::Answer { .. } | SignalEvent::RequestResponse { .. }
                    )
                {
                    return;
                }
                if session.tx.try_send(SessionOutput::Signal(event)).is_err() {
                    // Slow or gone consumer: drop the signal connection (§4.4).
                    tracing::warn!(participant = %to, "signal queue full or closed, closing session");
                    self.sessions.remove(&to);
                }
            }
            Effect::CloseSignal { to } => {
                if let Some(session) = self.sessions.remove(&to) {
                    let _ = session.tx.try_send(SessionOutput::Close);
                }
            }
            Effect::Event(ev) => {
                match &ev {
                    EngineEvent::TrackPublished {
                        room,
                        participant,
                        track,
                    } => {
                        if let (Some(recording), Some(executor)) =
                            (self.recording.as_mut(), self.recording_executor.as_ref())
                        {
                            for action in recording.published(
                                &room.sid,
                                &participant.identity,
                                &participant.sid,
                                participant.kind,
                                track.sid.clone(),
                            ) {
                                executor.submit(room.clone(), action);
                            }
                        }
                        if self.agent_publishers.insert(participant.sid.clone()) {
                            self.launch_agent_targets(None, Some(participant.clone()), 1);
                        }
                    }
                    EngineEvent::ParticipantLeft { participant, .. }
                    | EngineEvent::ParticipantConnectionAborted { participant, .. } => {
                        if let Some(recording) = self.recording.as_mut() {
                            recording.left(
                                &self.room.info().sid,
                                &participant.identity,
                                &participant.sid,
                            );
                        }
                        self.agent_signal_retirements.remove(&participant.sid);
                        self.agent_publishers.remove(&participant.sid);
                        self.emit_agent_work(crate::api::AgentWorkEvent::ParticipantLeft {
                            key: self.room.key().clone(),
                            room_sid: self.room.info().sid,
                            identity: participant.identity.clone(),
                        });
                    }
                    EngineEvent::RoomFinished { room, .. } => {
                        if let Some(recording) = self.recording.as_mut() {
                            recording.closed(&room.sid);
                        }
                        self.emit_agent_work(crate::api::AgentWorkEvent::RoomClosed {
                            key: self.room.key().clone(),
                            room_sid: room.sid.clone(),
                        })
                    }
                    _ => {}
                }
                self.events.emit(ev);
            }
            Effect::Media(cmd) => self.media_command(cmd, now),
        }
    }

    pub(crate) fn set_idle_hold(&mut self, state: Arc<crate::idle_hold::IdleHoldState>) {
        self.idle_hold = state;
    }

    pub(crate) fn initialize_recording(
        &mut self,
        policy: Option<rtc_core::auto_egress::AutomaticRecording>,
        executor: Arc<crate::automatic_recording::RecordingExecutor>,
    ) {
        self.recording = policy.map(|policy| {
            rtc_core::auto_egress::RecordingLifecycle::new(self.room.info().sid, policy)
        });
        self.recording_executor = Some(executor);
    }

    pub(crate) fn set_agent_event_sender(&mut self, sender: crate::api::AgentEventSender) {
        self.agent_events = sender;
    }
    fn emit_agent_work(&self, event: crate::api::AgentWorkEvent) {
        if self
            .agent_events
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .is_some()
        {
            self.agent_outbox.borrow_mut().push_back(event);
        }
    }
    /// A completed owner turn drains before consuming more control input. The
    /// shared bridge channel is bounded; fullness backpressures rather than
    /// silently dropping accepted dispatch effects. No mutex is held across wait.
    async fn flush_agent_outbox(&mut self) {
        let sender = self
            .agent_events
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .clone();
        let Some(sender) = sender else {
            return;
        };
        while !self.agent_outbox.get_mut().is_empty() {
            // Even final teardown must not wait on the bridge while a bridge
            // commit waits on this owner. Keep servicing the owner inbox.
            tokio::select! {
                permit = sender.clone().reserve_owned() => match permit {
                    Ok(permit) => { if let Some(event) = self.agent_outbox.get_mut().pop_front() { permit.send(event); } }
                    Err(_) => { tracing::error!("agent bridge closed with unconsumed room work"); break; }
                },
                input = self.inbox.recv() => { if let Some(input) = input { self.on_input(input); } }
            }
        }
    }
    fn launch_agent_targets(
        &self,
        only: Option<&AgentDispatchRecord>,
        participant: Option<ParticipantInfo>,
        job_type: i32,
    ) {
        if participant.as_ref().is_some_and(|p| {
            matches!(p.kind, ParticipantKind::Agent | ParticipantKind::Egress)
                || p.permission.agent
                || p.permission.recorder
        }) {
            return;
        }
        let targets = only
            .map(|d| vec![d])
            .unwrap_or_else(|| self.agent_dispatches.values().collect());
        for dispatch in targets {
            self.emit_agent_work(crate::api::AgentWorkEvent::Launch(Box::new(
                crate::api::AgentLaunch {
                    key: self.room.key().clone(),
                    room: self.room.info(),
                    dispatch: dispatch.clone(),
                    participant: participant.clone(),
                    job_type,
                },
            )));
        }
    }

    fn media_command(&mut self, cmd: MediaCommand, now: Instant) {
        self.engine
            .set_room_participant_count(self.room.participant_count());
        match cmd {
            MediaCommand::CreateTransport(spec) => {
                let creds = IceCredentials::random();
                match self
                    .net
                    .open_transport(spec.id, &creds.ufrag, self.packet_tx.clone())
                {
                    Ok(cands) => {
                        let cands = cands.into_iter().map(convert_candidate).collect();
                        let gather = self.net.server_stun_enabled() && !spec.ice_lite;
                        if gather {
                            self.net
                                .start_srflx(spec.id, &creds.ufrag, true, &mut self.srflx_jobs);
                        }
                        let id = spec.id;
                        let refresh_one_shot = spec.one_shot
                            && (self.net.uses_dedicated_udp() || self.net.rotates_udp_mux());
                        self.engine
                            .create_transport_with_srflx(spec, creds, cands, gather, now);
                        if refresh_one_shot {
                            self.engine.enable_one_shot_host_refresh(id);
                        }
                    }
                    Err(e) => {
                        // Without sockets the participant cannot connect; the join
                        // verification timeout removes it.
                        tracing::error!(error = %e, transport = %spec.id, "could not allocate media sockets");
                    }
                }
            }
            MediaCommand::CloseTransport(id) => {
                self.engine
                    .handle_command(MediaCommand::CloseTransport(id), now);
                self.net.close_transport(id);
            }
            MediaCommand::RemoveParticipant(sid) => {
                let transports = self.engine.transports_of(&sid);
                self.engine
                    .handle_command(MediaCommand::RemoveParticipant(sid), now);
                for t in transports {
                    self.net.close_transport(t);
                }
            }
            other => self.engine.handle_command(other, now),
        }
    }

    /// Publish completed local cached state; this only replaces an in-memory
    /// generation revision, never waits for Redis or consumes a lossy event queue.
    fn project_directory(&self) {
        // Go Room.ToProto is a cached proxy read, not an actor request. Publish
        // this exact generation's RoomInfo before completed API replies and
        // before an asynchronous lifecycle ACK can hold the owner again. Only
        // Info is updated here; traffic/statistics retain their sampling hook.
        self.snapshot.lock().unwrap_or_else(|p| p.into_inner()).info = self.room.info();
        if self.events.wants_room_projection() {
            self.events.project_room(rtc_core::events::RoomProjection {
                room: self.room.info(),
                participants: self.room.list_participants_stored(),
                terminal: self.room.is_closed(),
            });
        }
    }

    fn publish_snapshot(&mut self) {
        let stats = self.engine.stats();
        let (data_bytes_in, data_packets_in) = self.room.data_traffic_in();
        let info = self.room.info();
        let snapshot = RoomSnapshot {
            participants: self.room.participant_count() as u32,
            info,
            tracks_in: self.room.published_track_count() as u32,
            tracks_out: stats.subscribed_tracks as u32,
            bytes_in: stats.bytes_in.wrapping_add(data_bytes_in),
            bytes_out: stats.bytes_out,
            packets_in: stats.packets_in.wrapping_add(data_packets_in),
            packets_out: stats.packets_out,
            nacks: stats.nacks,
            retransmit_bytes_out: stats.retransmit_bytes_out,
            retransmit_packets_out: stats.retransmit_packets_out,
            estimated_capacity_bps: stats.estimated_capacity_bps,
            allocated_bps: stats.allocated_bps,
            allocation_overshoot_bps: stats.allocation_overshoot_bps,
            paused_video_tracks: stats.paused_video_tracks as u64,
            probe_attempts: stats.probe_attempts,
            probe_successes: stats.probe_successes,
            probe_failures: stats.probe_failures,
            congestion_events: stats.congestion_events,
        };
        if let Some(reporter) = &mut self.process_stats {
            reporter.sample(&snapshot, self.room.lifecycle_counters());
        }
        self.project_directory();
        *self.snapshot.lock().unwrap_or_else(|p| p.into_inner()) = snapshot;
    }

    async fn shutdown(&mut self) {
        self.idle_hold.mark_closed();
        let now = tokio::time::Instant::now().into_std();
        if !self.room.is_closed() {
            self.room.close(RoomEndReason::Unknown, now);
        }
        self.settle(now);
        // Upstream SendData returns success once room.SendDataPacket returns
        // (roommanager.go1001); a closing room does not turn a pending
        // synchronous fanout into an API error.
        for (_, reply) in self.data_completions.drain() {
            let _ = reply.send(());
        }
        for (_, s) in self.sessions.drain() {
            let _ = s.tx.try_send(SessionOutput::Close);
        }
        // Remaining queued inputs: answer joins so callers do not hang.
        self.publish_snapshot();
        // Fixed Room.Close closes participant sessions before OnClose kills
        // its RPC topics. Await acceptance Close ACK before publishing receiver
        // closure (the public manager deletion completion), not before Leave.
        if let Some(lifecycle) = self.owner_lifecycle.take() {
            lifecycle
                .service
                .closed(lifecycle.owner.key().clone(), lifecycle.owner.sid().clone())
                .await;
        }
        for (reply, _) in self.deferred_joins.drain(..) {
            let _ = reply.send(Err(JoinError::RoomClosed));
        }
        self.inbox.close();
        while let Ok(input) = self.inbox.try_recv() {
            if let RoomInput::Join { reply, .. } = input {
                let _ = reply.send(Err(JoinError::RoomClosed));
            }
        }
    }
}

fn convert_candidate(c: NetCandidate) -> rtc_media::LocalCandidate {
    rtc_media::LocalCandidate {
        proto: match c.proto {
            NetProto::Udp => Proto::Udp,
            NetProto::Tcp => Proto::Tcp,
        },
        addr: c.addr,
        base: c.base,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::net::{LocalInterface, NetConfig};
    use rtc_core::config::*;
    use rtc_core::data::{DataHandler, DataSender, DataVerdict};
    use rtc_core::events::NullSink;
    use rtc_core::permission::RoomConfig;
    use std::collections::BTreeMap;
    use std::net::Ipv4Addr;
    use std::sync::atomic::{AtomicBool, Ordering};

    pub(super) fn test_config() -> ServerConfig {
        ServerConfig {
            http: HttpConfig {
                port: 7880,
                bind_addresses: None,
                max_request_body: 10 << 20,
            },
            rtc: RtcConfig {
                ice_acceptance_min_waits: [
                    Duration::ZERO,
                    Duration::from_millis(500),
                    Duration::from_secs(1),
                    Duration::from_secs(2),
                ],
                forward_stats: None,
                udp: UdpMode::Mux { ports: vec![7882] },
                tcp_port: Some(7881),
                node_ip: NodeIp::default(),
                use_node_ip_mapping: false,
                advertise_internal_ip: false,
                stun_servers: vec![],
                turn_servers: vec![],
                use_ice_lite: false,
                force_tcp: false,
                use_stun_port_as_ice: false,
                use_external_ip: false,
                skip_external_ip_validation: false,
                external_ip_only: false,
                batch_write_size: 0,
                batch_write_interval: std::time::Duration::ZERO,
                interfaces: Filter::default(),
                ips: Filter::default(),
                enable_loopback_candidate: false,
                use_mdns: false,
                enable_warp: false,
                enable_rtp_stream_restart_detection: false,
                data_channel_slow_threshold: 0,
                data_channel_max_buffered_amount: 0,
                data_channel_lossy_target_latency: rtc_core::config::PolicyDuration::from_nanos(0),
                data_track_target_latency: rtc_core::config::PolicyDuration::from_millis(100),
                sctp: Default::default(),
                packet_buffer_size_video: 500,
                packet_buffer_size_audio: 200,
                pli_throttle: PliThrottle {
                    low: Duration::from_millis(500),
                    mid: Duration::from_secs(1),
                    high: Duration::from_secs(1),
                },
                congestion_control: CongestionControl {
                    enabled: true,
                    allow_pause: false,
                    use_send_side_bwe: false,
                    ..CongestionControl::default()
                },
                allow_tcp_fallback: true,
                tcp_fallback_rtt_threshold: 0,
                allow_udp_unstable_fallback: false,
                reconnect_on_publication_error: false,
                reconnect_on_subscription_error: false,
                reconnect_on_data_channel_error: false,
            },
            room: RoomDefaults {
                create_room_timeout: rtc_core::config::PolicyDuration::from_secs(10),
                create_room_attempts: 3,
                auto_create: true,
                enabled_codecs: vec![
                    CodecSpec {
                        mime: "audio/opus".into(),
                        fmtp_line: String::new(),
                    },
                    CodecSpec {
                        mime: "video/VP8".into(),
                        fmtp_line: String::new(),
                    },
                    CodecSpec {
                        mime: "video/rtx".into(),
                        fmtp_line: String::new(),
                    },
                ],
                max_participants: 0,
                empty_timeout: Duration::from_secs(300),
                departure_timeout: Duration::from_secs(20),
                enable_remote_unmute: false,
                playout_delay: None,
                sync_streams: false,
                enable_full_room_in_webhooks: false,
                update_batch_target_size: 128 << 10,
                presets: BTreeMap::new(),
            },
            audio: AudioConfig {
                active_level: 35,
                min_percentile: 40,
                update_interval: Duration::from_millis(400),
                smooth_intervals: 2,
                active_red_encoding: false,
            },
            video: VideoConfig {
                codec_regression_threshold: 5,
                dynacast_pause_delay: Duration::from_secs(5),
                bitrate_report: Default::default(),
                stream_trackers: Default::default(),
            },
            turn: TurnConfig {
                enabled: false,
                domain: String::new(),
                cert_file: None,
                key_file: None,
                tls_port: None,
                udp_port: None,
                relay_range: (30000, 40000),
                external_tls: false,
                bind_addresses: vec![],
                per_user_allocation_limit: 12,
                ttl: Duration::from_secs(300),
                allow_restricted_peer_cidrs: vec![],
                deny_peer_cidrs: vec![],
                proxy_protocol: false,
                proxy_trusted_cidrs: vec![],
            },
            webhook: WebhookConfig::default(),
            limits: Limits {
                subscription_limit_video: None,
                subscription_limit_audio: None,
                max_metadata_size: Some(512 << 10),
                max_attributes_size: Some(64 << 10),
                max_room_name_length: 256,
                max_participant_identity_length: 256,
                max_participant_name_length: 256,
                signal_message_size_limit: Some(2 << 20),
                max_data_track_custom_encoding_length: 32,
                max_data_blob_key_length: 256,
                max_data_blob_size: 64000,
            },
            logging: LoggingConfig {
                level: "info".into(),
                json: false,
                component_levels: BTreeMap::new(),
                ..LoggingConfig::default()
            },
            prometheus: PrometheusConfig::default(),
            keys: vec![("devkey".into(), "secret".into())],
            region: String::new(),
            node_id: "ND_test".into(),
            development: true,
            enable_data_tracks: false,
            enable_participant_data_blob: false,
            enable_psrpc_for_get_list_participants: false,
            extension: None,
        }
    }

    struct DropData;
    impl DataHandler for DropData {
        fn inbound(&self, _: &DataSender<'_>, _: DataKind, _: &str, _: &[u8]) -> DataVerdict {
            DataVerdict::Drop
        }
    }

    /// The current-thread executor makes the old enqueue-only implementation
    /// fail deterministically: create/send do not yield to the room owner, so an
    /// immediate list would still contain it. Completion must await the owner.
    #[tokio::test]
    async fn delete_room_completes_before_list_and_same_name_recreation() {
        let cfg = Arc::new(test_config());
        let net = NetRouter::start(NetConfig {
            udp_enabled: false,
            udp_port: None,
            port_range: (0, 0),
            tcp_port: None,
            interfaces: vec![LocalInterface {
                bind: Ipv4Addr::LOCALHOST.into(),
                advertise: Ipv4Addr::LOCALHOST.into(),
            }],
        })
        .await
        .unwrap();
        let manager = crate::RoomManager::new(crate::RuntimeDeps {
            config: cfg,
            net: net.clone(),
            events: Arc::new(NullSink),
            data: Arc::new(DropData),
        });
        let key = RoomKey::new(ProjectId::local(), "delete-and-recreate");
        let mut previous_sid = None;
        for _ in 0..16 {
            let created = manager
                .create_room(key.clone(), crate::CreateRoomOptions::default())
                .await
                .unwrap();
            assert!(manager.room_exists(&key));
            assert_eq!(
                manager
                    .list_rooms(&key.project, std::slice::from_ref(&key.name))
                    .len(),
                1
            );
            if let Some(previous) = previous_sid {
                assert_ne!(created.sid, previous);
            }
            previous_sid = Some(created.sid);
            manager.delete_room(&key).await.unwrap();
            assert!(
                !manager.room_exists(&key),
                "DeleteRoom acknowledged before registry removal"
            );
            assert!(
                manager
                    .list_rooms(&key.project, std::slice::from_ref(&key.name))
                    .is_empty()
            );
        }
        assert!(matches!(
            manager.delete_room(&key).await,
            Err(crate::ApiError::NotFound(_))
        ));
        net.shutdown();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn shutdown_is_processed_under_continuous_media_load() {
        let cfg = Arc::new(test_config());
        let net = NetRouter::start(NetConfig {
            udp_enabled: false,
            udp_port: None,
            port_range: (0, 0),
            tcp_port: None,
            interfaces: vec![LocalInterface {
                bind: Ipv4Addr::LOCALHOST.into(),
                advertise: Ipv4Addr::LOCALHOST.into(),
            }],
        })
        .await
        .unwrap();
        let room = Room::new(
            RoomKey::new(ProjectId::local(), "busy"),
            Arc::new(rtc_core::config::RoomPolicy::from(cfg.as_ref())),
            Arc::new(DropData),
            &RoomConfig::default(),
            Instant::now(),
        );
        let snapshot = Arc::new(Mutex::new(RoomSnapshot {
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
        let (tx, rx) = mpsc::channel(ROOM_INBOX);
        let task = RoomTask::new(
            room,
            &cfg,
            net.clone(),
            Arc::new(NullSink),
            snapshot,
            tx.clone(),
            rx,
        )
        .unwrap();
        let packets = task.packet_tx.clone();
        let packet = || InboundPacket {
            transport: TransportId(999),
            proto: NetProto::Udp,
            source: "127.0.0.1:10000".parse().unwrap(),
            destination: "127.0.0.1:20000".parse().unwrap(),
            bound: "127.0.0.1:20000".parse().unwrap(),
            data: vec![0; 32],
            received_at: Instant::now(),
        };
        for _ in 0..ROOM_PACKET_QUEUE {
            packets.try_send(packet()).unwrap();
        }
        let stop = Arc::new(AtomicBool::new(false));
        let producer_stop = stop.clone();
        let producer = std::thread::spawn(move || {
            while !producer_stop.load(Ordering::Relaxed) {
                let _ = packets.try_send(packet());
                std::thread::yield_now();
            }
        });
        tx.send(RoomInput::Close(RoomEndReason::ServerShutdown))
            .await
            .unwrap();
        let mut running = tokio::spawn(task.run());
        let result = tokio::time::timeout(Duration::from_secs(2), &mut running).await;
        stop.store(true, Ordering::Relaxed);
        producer.join().unwrap();
        if result.is_err() {
            running.abort();
        }
        result
            .expect("media traffic must not starve room shutdown")
            .unwrap();
        net.shutdown();
    }

    #[tokio::test]
    async fn process_traffic_survives_terminal_room_and_replacement_generation() {
        // Keep the original32/48-byte lifetime, no-double-count and generation
        // reconstruction assertions, now driven by real ICE/DTLS/SCTP messages.
        super::network_owner_consumers::process_traffic_lifetime_with_authenticated_messages()
            .await;
    }

    pub(super) async fn dispatch_manager() -> (crate::RoomManager, NetRouter) {
        let net = NetRouter::start(NetConfig {
            udp_enabled: false,
            udp_port: None,
            port_range: (0, 0),
            tcp_port: None,
            interfaces: vec![LocalInterface {
                bind: Ipv4Addr::LOCALHOST.into(),
                advertise: Ipv4Addr::LOCALHOST.into(),
            }],
        })
        .await
        .unwrap();
        let manager = crate::RoomManager::new(crate::RuntimeDeps {
            config: Arc::new(test_config()),
            net: net.clone(),
            events: Arc::new(NullSink),
            data: Arc::new(DropData),
        });
        (manager, net)
    }

    #[tokio::test]
    async fn dispatch_registry_is_room_owned_shared_and_project_isolated() {
        let (manager, net) = dispatch_manager().await;
        let first_key = RoomKey::new(ProjectId::local(), "same-name");
        let other_key = RoomKey::new(ProjectId("other-project".into()), "same-name");
        for key in [&first_key, &other_key] {
            assert!(matches!(
                manager.list_agent_dispatches(key, None).await,
                Err(ApiError::NotFound(_))
            ));
            manager
                .create_room(key.clone(), Default::default())
                .await
                .unwrap();
        }
        let options = AgentDispatchOptions {
            id: "AD_same-id".into(),
            agent_name: "worker-name".into(),
            metadata: "first".into(),
            restart_policy: 1,
            deployment: "prod".into(),
            attributes: [("key".into(), "value".into())].into_iter().collect(),
        };
        let before_ns = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos() as i64;
        let created = manager
            .create_agent_dispatch(&first_key, options.clone())
            .await
            .unwrap();
        let after_ns = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos() as i64;
        assert!((before_ns..=after_ns).contains(&created.created_at_ns));
        assert_eq!(created.room, "same-name");
        assert_eq!(created.restart_policy, 1);
        assert_eq!(created.attributes, options.attributes);
        let other_defaults = manager
            .list_agent_dispatches(&other_key, None)
            .await
            .unwrap();
        assert_eq!(other_defaults.len(), 1);
        assert!(other_defaults[0].agent_name.is_empty());
        let copy = manager.clone();
        let mut listed = copy
            .list_agent_dispatches(&first_key, Some("AD_same-id"))
            .await
            .unwrap();
        assert_eq!(listed, vec![created.clone()]);
        listed[0].metadata.clear();
        let all = manager
            .list_agent_dispatches(&first_key, Some(""))
            .await
            .unwrap();
        assert_eq!(all.len(), 2);
        assert!(all.contains(&created));
        assert!(
            manager
                .list_agent_dispatches(&first_key, Some("AD_missing"))
                .await
                .unwrap()
                .is_empty()
        );
        assert!(
            matches!(manager.delete_agent_dispatch(&other_key, "AD_same-id").await,
                         Err(ApiError::NotFound(message)) if message == "dispatch ID not found")
        );
        assert_eq!(
            copy.delete_agent_dispatch(&first_key, "AD_same-id")
                .await
                .unwrap(),
            created
        );
        let remaining = manager
            .list_agent_dispatches(&first_key, None)
            .await
            .unwrap();
        assert_eq!(remaining.len(), 1);
        assert!(remaining[0].agent_name.is_empty());
        manager.close_all().await;
        net.shutdown();
    }

    #[tokio::test]
    async fn dispatch_requests_stay_with_their_room_generation_and_concurrent_creates_survive() {
        let (manager, net) = dispatch_manager().await;
        let key = RoomKey::new(ProjectId::local(), "recreated-dispatches");
        for generation in 0..8 {
            manager
                .create_room(key.clone(), Default::default())
                .await
                .unwrap();
            let defaults = manager.list_agent_dispatches(&key, None).await.unwrap();
            assert_eq!(defaults.len(), 1);
            assert!(defaults[0].agent_name.is_empty());
            let options = |id: &str| AgentDispatchOptions {
                id: id.into(),
                metadata: generation.to_string(),
                ..Default::default()
            };
            let copy = manager.clone();
            let (left, right) = tokio::join!(
                manager.create_agent_dispatch(&key, options("AD_left")),
                copy.create_agent_dispatch(&key, options("AD_right")),
            );
            left.unwrap();
            right.unwrap();
            let records = manager.list_agent_dispatches(&key, None).await.unwrap();
            assert_eq!(records.len(), 3);
            assert!(
                records
                    .iter()
                    .filter(|record| record.id == "AD_left" || record.id == "AD_right")
                    .all(|record| record.metadata == generation.to_string())
            );
            manager.delete_room(&key).await.unwrap();
            assert!(matches!(
                manager
                    .create_agent_dispatch(&key, options("AD_left"))
                    .await,
                Err(ApiError::NotFound(_))
            ));
        }
        net.shutdown();
    }

    #[tokio::test]
    async fn room_internal_default_dispatch_can_be_deleted_and_is_fresh_on_recreation() {
        let (manager, net) = dispatch_manager().await;
        let key = RoomKey::new(ProjectId::local(), "defaults");
        let mut prior = None;
        for agents in [None, Some(Vec::new()), None] {
            manager
                .create_room(
                    key.clone(),
                    crate::CreateRoomOptions {
                        config: RoomConfig {
                            agent_dispatches: agents,
                            ..Default::default()
                        },
                    },
                )
                .await
                .unwrap();
            let defaults = manager.list_agent_dispatches(&key, None).await.unwrap();
            assert_eq!(defaults.len(), 1);
            let default = defaults[0].clone();
            assert!(default.agent_name.is_empty());
            assert_eq!(default.room, "defaults");
            assert!(default.id.starts_with("AD_"));
            assert_ne!(prior.as_ref(), Some(&default.id));
            prior = Some(default.id.clone());
            assert_eq!(
                manager
                    .delete_agent_dispatch(&key, &default.id)
                    .await
                    .unwrap(),
                default
            );
            assert!(
                manager
                    .list_agent_dispatches(&key, None)
                    .await
                    .unwrap()
                    .is_empty()
            );
            assert!(
                matches!(manager.delete_agent_dispatch(&key, &default.id).await,
                            Err(ApiError::NotFound(message)) if message == "dispatch ID not found")
            );
            manager.delete_room(&key).await.unwrap();
        }
        net.shutdown();
    }

    #[tokio::test]
    async fn explicit_room_agents_skip_invalid_deployments_without_synthesizing_default() {
        use rtc_core::permission::RoomAgentDispatchOptions;
        let (manager, net) = dispatch_manager().await;
        let key = RoomKey::new(ProjectId::local(), "explicit");
        let good = RoomAgentDispatchOptions {
            agent_name: "configured".into(),
            metadata: "context".into(),
            restart_policy: 1,
            deployment: "東京-prod".into(),
            attributes: [("k".into(), "v".into())].into_iter().collect(),
        };
        let invalid = RoomAgentDispatchOptions {
            deployment: "bad_name".into(),
            ..Default::default()
        };
        manager
            .create_room(
                key.clone(),
                crate::CreateRoomOptions {
                    config: RoomConfig {
                        agent_dispatches: Some(vec![invalid.clone(), good.clone()]),
                        ..Default::default()
                    },
                },
            )
            .await
            .unwrap();
        let records = manager.list_agent_dispatches(&key, None).await.unwrap();
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].agent_name, good.agent_name);
        assert_eq!(records[0].attributes, good.attributes);
        assert_eq!(records[0].restart_policy, 1);
        manager.delete_room(&key).await.unwrap();
        manager
            .create_room(
                key.clone(),
                crate::CreateRoomOptions {
                    config: RoomConfig {
                        agent_dispatches: Some(vec![invalid]),
                        ..Default::default()
                    },
                },
            )
            .await
            .unwrap();
        assert!(
            manager
                .list_agent_dispatches(&key, None)
                .await
                .unwrap()
                .is_empty(),
            "an invalid nonempty list is skipped, not converted to an unnamed default"
        );
        manager.close_all().await;
        net.shutdown();
    }
}

#[cfg(test)]
mod room_playout_consumer_tests {
    use super::*;
    use crate::net::{LocalInterface, NetConfig};
    use rtc_core::config::RoomPolicy;
    use rtc_core::data::{DataHandler, DataSender, DataVerdict};
    use rtc_core::events::NullSink;
    use rtc_core::permission::RoomConfig;

    struct DropData;
    impl DataHandler for DropData {
        fn inbound(&self, _: &DataSender<'_>, _: DataKind, _: &str, _: &[u8]) -> DataVerdict {
            DataVerdict::Drop
        }
    }

    #[tokio::test]
    async fn resolved_room_pair_reaches_actual_media_engine_without_global_crosstalk() {
        let mut cfg = super::tests::test_config();
        cfg.room.playout_delay = Some((100, 200));
        let net = NetRouter::start(NetConfig {
            udp_enabled: false,
            udp_port: None,
            port_range: (0, 0),
            tcp_port: None,
            interfaces: vec![LocalInterface {
                bind: std::net::Ipv4Addr::LOCALHOST.into(),
                advertise: std::net::Ipv4Addr::LOCALHOST.into(),
            }],
        })
        .await
        .unwrap();
        let mut prior_sid = None;
        for (min, max, expected) in [
            (None, None, Some((100, 200))),
            (Some(30), None, Some((30, 0))),
            (None, Some(90), Some((0, 90))),
            (Some(30), Some(90), Some((30, 90))),
            (Some(0), Some(0), Some((100, 200))),
        ] {
            let room = Room::new(
                RoomKey::new(ProjectId::local(), "per-generation-playout"),
                Arc::new(RoomPolicy::from(&cfg)),
                Arc::new(DropData),
                &RoomConfig {
                    min_playout_delay: min,
                    max_playout_delay: max,
                    ..Default::default()
                },
                Instant::now(),
            );
            let sid = room.sid().clone();
            if let Some(previous) = prior_sid {
                assert_ne!(sid, previous);
            }
            prior_sid = Some(sid);
            let snapshot = Arc::new(Mutex::new(RoomSnapshot {
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
            let (tx, rx) = mpsc::channel(ROOM_INBOX);
            let task = RoomTask::new(
                room,
                &cfg,
                net.clone(),
                Arc::new(NullSink),
                snapshot,
                tx,
                rx,
            )
            .unwrap();
            assert_eq!(task.engine.playout_delay(), expected);
            assert_eq!(cfg.room.playout_delay, Some((100, 200)));
        }
        net.shutdown();
    }
    #[tokio::test]
    async fn agent_effect_backpressure_preserves_launch_and_real_result_is_generation_guarded() {
        let (manager, net) = super::tests::dispatch_manager().await;
        let (events, mut receiver) = mpsc::channel(1);
        manager.attach_agent_events(events).unwrap();
        let key = RoomKey::new(ProjectId::local(), "agent-generation");
        let original = manager
            .create_room(key.clone(), Default::default())
            .await
            .unwrap();
        let dispatch = manager
            .create_agent_dispatch(
                &key,
                AgentDispatchOptions {
                    id: "AD_named".into(),
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        // A full channel must not discard the next accepted create effect.
        let first = tokio::time::timeout(Duration::from_secs(2), receiver.recv())
            .await
            .unwrap()
            .unwrap();
        assert!(matches!(
            first,
            crate::api::AgentWorkEvent::Launch(ref work) if work.job_type == 0
        ));
        let second = tokio::time::timeout(Duration::from_secs(2), receiver.recv())
            .await
            .unwrap()
            .unwrap();
        assert!(
            matches!(second,crate::api::AgentWorkEvent::Launch(ref work) if work.dispatch.id==dispatch.id && work.job_type==0)
        );
        let job = crate::api::AgentJobRecord {
            id: "AJ_adapter_result".into(),
            dispatch_id: dispatch.id.clone(),
            job_type: 0,
            room: original.clone(),
            participant: None,
            namespace: String::new(),
            metadata: String::new(),
            agent_name: String::new(),
            deployment: String::new(),
            attributes: BTreeMap::new(),
            enable_recording: false,
            enable_redaction: false,
            state: crate::api::AgentJobState {
                status: 1,
                error: String::new(),
                started_at_ns: 1,
                ended_at_ns: 0,
                updated_at_ns: 1,
                participant_identity: "agent".into(),
                worker_id: "AW_fixture".into(),
                agent_id: String::new(),
            },
        };
        manager
            .commit_agent_job(&key, original.sid.clone(), job.clone())
            .await
            .unwrap();
        manager
            .commit_agent_job(&key, original.sid.clone(), job.clone())
            .await
            .unwrap();
        assert_eq!(
            manager
                .list_agent_dispatches(&key, Some("AD_named"))
                .await
                .unwrap()[0]
                .jobs
                .len(),
            1
        );
        manager.delete_room(&key).await.unwrap();
        assert!(matches!(
            receiver.recv().await.unwrap(),
            crate::api::AgentWorkEvent::RoomClosed { .. }
        ));
        let replacement = manager
            .create_room(key.clone(), Default::default())
            .await
            .unwrap();
        assert_ne!(replacement.sid, original.sid);
        receiver.recv().await.unwrap();
        assert!(matches!(
            manager.commit_agent_job(&key, original.sid, job).await,
            Err(ApiError::NotFound(_))
        ));
        assert!(
            manager
                .list_agent_dispatches(&key, None)
                .await
                .unwrap()
                .iter()
                .all(|d| d.jobs.is_empty())
        );
        manager.delete_room(&key).await.unwrap();
        receiver.recv().await.unwrap();
        net.shutdown();
    }
}

#[cfg(test)]
mod participant_api_owner_tests {
    use super::*;
    use crate::net::{LocalInterface, NetConfig};
    use rtc_core::data::{DataHandler, DataSender, DataVerdict};
    use rtc_core::events::NullSink;
    use std::net::Ipv4Addr;

    struct DropData;
    impl DataHandler for DropData {
        fn inbound(&self, _: &DataSender<'_>, _: DataKind, _: &str, _: &[u8]) -> DataVerdict {
            DataVerdict::Drop
        }
    }
    async fn manager(live: bool) -> (crate::RoomManager, NetRouter) {
        let mut cfg = super::tests::test_config();
        cfg.enable_psrpc_for_get_list_participants = live;
        let net = NetRouter::start(NetConfig {
            udp_enabled: false,
            udp_port: None,
            port_range: (0, 0),
            tcp_port: None,
            interfaces: vec![LocalInterface {
                bind: Ipv4Addr::LOCALHOST.into(),
                advertise: Ipv4Addr::LOCALHOST.into(),
            }],
        })
        .await
        .unwrap();
        let manager = crate::RoomManager::new(crate::RuntimeDeps {
            config: Arc::new(cfg),
            net: net.clone(),
            events: Arc::new(NullSink),
            data: Arc::new(DropData),
        });
        (manager, net)
    }

    #[tokio::test]
    async fn live_participant_reads_wait_for_a_real_late_room_owner_inside_original_deadline() {
        let (manager, net) = manager(true).await;
        let key = RoomKey::new(ProjectId::local(), "late-live-owner");
        let a = manager.clone();
        let ka = key.clone();
        let list = tokio::spawn(async move {
            a.list_participants_for_api(&ka, Duration::from_secs(3))
                .await
        });
        let a = manager.clone();
        let ka = key.clone();
        let get = tokio::spawn(async move {
            a.get_participant_for_api(&ka, &Identity("absent".into()), Duration::from_secs(3))
                .await
        });
        tokio::task::yield_now().await;
        assert!(
            !list.is_finished(),
            "no owner is not immediate empty success"
        );
        assert!(!get.is_finished(), "no owner is not cached NotFound");
        manager
            .create_room(key.clone(), crate::CreateRoomOptions::default())
            .await
            .unwrap();
        assert!(
            list.await.unwrap().unwrap().is_empty(),
            "real newly registered owner answers its actual empty room"
        );
        assert_eq!(
            get.await.unwrap(),
            Err(ApiError::NotFound("participant does not exist".into())),
            "active owner provides the actual participant lookup error"
        );
        manager.delete_room(&key).await.unwrap();
        net.shutdown();
    }

    #[tokio::test]
    async fn live_no_owner_deadline_and_cached_store_absence_are_distinct() {
        let (live, live_net) = manager(true).await;
        let key = RoomKey::new(ProjectId::local(), "never-created");
        let duration = Duration::from_millis(20);
        let at = Instant::now();
        let absent = Identity("absent".into());
        let (list, get) = tokio::join!(
            live.list_participants_for_api(&key, duration),
            live.get_participant_for_api(&key, &absent, duration)
        );
        assert_eq!(
            list,
            Err(ApiError::Unavailable("no response from servers".into()))
        );
        assert_eq!(
            get,
            Err(ApiError::Unavailable("no response from servers".into()))
        );
        assert!(
            at.elapsed() >= duration,
            "configured owner deadline must actually elapse"
        );
        let (cached, cached_net) = manager(false).await;
        assert!(
            tokio::time::timeout(
                Duration::from_secs(1),
                cached.list_participants_for_api(&key, duration)
            )
            .await
            .unwrap()
            .unwrap()
            .is_empty()
        );
        assert_eq!(
            cached
                .get_participant_for_api(&key, &Identity("absent".into()), duration)
                .await,
            Err(ApiError::NotFound("participant does not exist".into()))
        );
        live_net.shutdown();
        cached_net.shutdown();
    }

    #[tokio::test]
    async fn cancelled_waiter_does_not_block_a_later_actual_owner_or_other_reader() {
        let (manager, net) = manager(true).await;
        let key = RoomKey::new(ProjectId::local(), "cancelled-owner-wait");
        let a = manager.clone();
        let ka = key.clone();
        let cancelled = tokio::spawn(async move {
            a.list_participants_for_api(&ka, Duration::from_secs(3))
                .await
        });
        tokio::task::yield_now().await;
        assert!(!cancelled.is_finished());
        cancelled.abort();
        assert!(cancelled.await.unwrap_err().is_cancelled());
        let a = manager.clone();
        let ka = key.clone();
        let surviving = tokio::spawn(async move {
            a.list_participants_for_api(&ka, Duration::from_secs(3))
                .await
        });
        tokio::task::yield_now().await;
        assert!(!surviving.is_finished());
        manager
            .create_room(key.clone(), crate::CreateRoomOptions::default())
            .await
            .unwrap();
        assert!(surviving.await.unwrap().unwrap().is_empty());
        manager.delete_room(&key).await.unwrap();
        assert_eq!(
            manager
                .list_participants_for_api(&key, Duration::from_millis(20))
                .await,
            Err(ApiError::Unavailable("no response from servers".into())),
            "deleted owner is not empty cached success"
        );
        net.shutdown();
    }
}

#[cfg(test)]
mod owner_metadata_api_tests {
    use super::*;
    use crate::net::{LocalInterface, NetConfig};
    use rtc_core::data::{DataHandler, DataSender, DataVerdict};
    use rtc_core::events::NullSink;
    use rtc_core::permission::{Grants, VideoGrant};
    use rtc_core::session::*;
    use std::net::Ipv4Addr;
    struct DropData;
    impl DataHandler for DropData {
        fn inbound(&self, _: &DataSender<'_>, _: DataKind, _: &str, _: &[u8]) -> DataVerdict {
            DataVerdict::Drop
        }
    }
    fn owner_params(identity: &str, protocol: i32, single_pc: bool) -> JoinParams {
        JoinParams {
            room: RoomKey::new(ProjectId::local(), "r1"),
            grants: Grants {
                identity: identity.into(),
                video: VideoGrant {
                    room_join: true,
                    room: "r1".into(),
                    ..Default::default()
                },
                ..Default::default()
            },
            identity: identity.into(),
            name: String::new(),
            metadata: String::new(),
            attributes: BTreeMap::new(),
            client: ClientMeta {
                sdk: SdkKind::Js,
                protocol,
                ..Default::default()
            },
            capabilities: ClientCapabilities {
                video_publication_requires_rtp: false,
                codec_switch_requires_renegotiation: false,
                ice_tcp: true,
                one_shot: false,
                subscriber_primary: true,
                speaker_changed: true,
                connection_quality: true,
                ice_lite: true,
                unpublish: true,
                fast_start: true,
                disconnected_update: true,
                sync_stream_id: true,
                disable_playout_delay_for_sync: false,
                connection_quality_lost: true,
                identity_based_reconnection: true,
                regions_in_leave: true,
                non_error_signal_response: true,
                request_response: true,
                audio_red: true,
            },
            auto_subscribe: true,
            auto_subscribe_data_track: false,
            adaptive_stream: false,
            single_pc,
            disable_ice_lite: false,
            subscriber_allow_pause: None,
            reconnect: false,
            reconnect_reason: 0,
            participant_sid: None,
            add_tracks: vec![],
            publisher_offer: None,
        }
    }
    async fn setup() -> (crate::RoomManager, NetRouter, RoomKey, SessionHandle) {
        let mut config = super::tests::test_config();
        config.limits.max_participant_name_length = -1;
        let net = NetRouter::start(NetConfig {
            udp_enabled: false,
            udp_port: None,
            port_range: (0, 0),
            tcp_port: None,
            interfaces: vec![LocalInterface {
                bind: Ipv4Addr::LOCALHOST.into(),
                advertise: Ipv4Addr::LOCALHOST.into(),
            }],
        })
        .await
        .unwrap();
        let manager = crate::RoomManager::new(crate::RuntimeDeps {
            config: Arc::new(config),
            net: net.clone(),
            events: Arc::new(NullSink),
            data: Arc::new(DropData),
        });
        let mut params = owner_params("runtime-owner", 17, true);
        params.name = "existing".into();
        params.grants.video.can_update_own_metadata = Some(false);
        let key = params.room.clone();
        let session = manager.join(params).await.unwrap();
        (manager, net, key, session)
    }

    #[tokio::test]
    async fn owner_runtime_api_rejection_is_real_owner_call_without_identity_permission_mutation() {
        let (manager, net, key, session) = setup().await;
        assert!(matches!(
            manager
                .update_participant(&key, &"missing".into(), ParticipantUpdate::default())
                .await,
            Err(ApiError::NotFound(_))
        ));
        let before = manager
            .get_participant(&key, &"runtime-owner".into())
            .await
            .unwrap();
        let mut permission = before.permission.clone();
        permission.can_publish = !permission.can_publish;
        let outcome = manager
            .update_participant(
                &key,
                &"runtime-owner".into(),
                ParticipantUpdate {
                    name: String::new(),
                    metadata: "attempted-runtime".into(),
                    attributes: BTreeMap::from([("x".into(), "é".into())]),
                    permission: Some(permission),
                },
            )
            .await;
        let after = manager
            .get_participant(&key, &"runtime-owner".into())
            .await
            .unwrap();
        // Close genuine task/network resources before expected baseline RED.
        drop(session);
        manager.close_all().await;
        net.shutdown();
        assert_eq!(
            outcome.unwrap_err().to_string(),
            "name length exceeds limits"
        );
        assert_eq!(after.name, before.name);
        assert_eq!(after.metadata, before.metadata);
        assert_eq!(after.attributes, before.attributes);
        assert_eq!(after.permission, before.permission);
    }

    #[tokio::test]
    async fn owner_runtime_rejection_delivers_actual_attempt_echo_to_existing_session() {
        let (manager, net, key, mut session) = setup().await;
        let before = manager
            .get_participant(&key, &"runtime-owner".into())
            .await
            .unwrap();
        assert!(
            !before.permission.can_update_metadata,
            "admin owner update is distinct from own-metadata permission"
        );
        let attributes =
            BTreeMap::from([("x".into(), "é".into()), ("remove".into(), String::new())]);
        assert_eq!(
            manager
                .update_participant(
                    &key,
                    &"runtime-owner".into(),
                    ParticipantUpdate {
                        name: String::new(),
                        metadata: "attempted-runtime".into(),
                        attributes: attributes.clone(),
                        permission: None
                    }
                )
                .await
                .unwrap_err()
                .to_string(),
            "name length exceeds limits"
        );
        let request = tokio::time::timeout(Duration::from_secs(2), async {
            while let Some(output) = session.output.recv().await {
                if let SessionOutput::Signal(SignalEvent::OwnedMetadataRequestResponse {
                    request,
                    outcome,
                    message,
                }) = output
                {
                    assert_eq!(outcome, rtc_core::session::RequestOutcome::LimitExceeded);
                    assert_eq!(message, "exceeds name length limit");
                    return request;
                }
            }
            panic!("actual session closed without owner rejection echo");
        })
        .await
        .expect("actual RoomTask output, no injected SignalEvent");
        assert_eq!(request.request_id, 0);
        assert!(request.name.is_empty());
        assert_eq!(request.metadata, "attempted-runtime");
        assert_eq!(request.attributes, attributes);
        drop(session);
        manager.close_all().await;
        net.shutdown();
    }
}

#[cfg(test)]
mod auto_egress_creation_tests {
    use super::tests::dispatch_manager;
    use super::*;
    use rtc_core::permission::RoomConfig;
    #[tokio::test]
    async fn create_lease_holds_idle_generation_and_returns_latest_same_sid_snapshot() {
        let (manager, net) = dispatch_manager().await;
        let key = RoomKey::new(ProjectId::local(), "auto-egress-held");
        let opts = crate::CreateRoomOptions {
            config: RoomConfig {
                empty_timeout_s: Some(1),
                ..Default::default()
            },
        };
        let a = manager.clone();
        let b = manager.clone();
        let (left, right) = tokio::join!(
            a.create_room_lease(key.clone(), opts.clone()),
            b.create_room_lease(key.clone(), opts),
        );
        let left = left.unwrap();
        let right = right.unwrap();
        assert_eq!(left.snapshot().sid, right.snapshot().sid);
        assert_ne!(left.created(), right.created());
        let old_sid = left.snapshot().sid;
        drop(right);
        // Represents a real external initialization await, not simulated worker
        // success. Real clock exceeds the configured empty timeout.
        tokio::time::sleep(Duration::from_millis(2200)).await;
        assert!(manager.room_exists(&key), "hold must defeat idle expiry");
        manager
            .update_room_metadata(&key, "changed-during-launch".into())
            .await
            .unwrap();
        assert_eq!(left.snapshot().sid, old_sid);
        assert_eq!(
            left.current_snapshot().await.metadata,
            "changed-during-launch"
        );
        drop(left); // same behavior on worker error/caller cancellation
        tokio::time::timeout(Duration::from_secs(3), async {
            while manager.room_exists(&key) {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("released empty room must close normally");
        let fresh = manager
            .create_room_lease(key.clone(), Default::default())
            .await
            .unwrap();
        assert!(fresh.created());
        assert_ne!(fresh.snapshot().sid, old_sid);
        drop(fresh);
        manager.close_all().await;
        net.shutdown();
    }

    #[tokio::test]
    async fn explicit_delete_and_recreate_are_not_blocked_or_released_by_old_lease() {
        let (manager, net) = dispatch_manager().await;
        let key = RoomKey::new(ProjectId::local(), "auto-egress-explicit-close");
        let old = manager
            .create_room_lease(key.clone(), Default::default())
            .await
            .unwrap();
        let old_sid = old.snapshot().sid;
        manager.delete_room(&key).await.unwrap();
        let new = manager
            .create_room_lease(
                key.clone(),
                crate::CreateRoomOptions {
                    config: RoomConfig {
                        empty_timeout_s: Some(1),
                        metadata: Some("replacement-generation".into()),
                        ..Default::default()
                    },
                },
            )
            .await
            .unwrap();
        assert_ne!(new.snapshot().sid, old_sid);
        let old_final = old.current_snapshot().await;
        assert_eq!(
            old_final.sid, old_sid,
            "closed original owner's final snapshot, not name lookup"
        );
        assert_ne!(old_final.metadata, "replacement-generation");
        assert_eq!(
            new.current_snapshot().await.metadata,
            "replacement-generation"
        );
        drop(old);
        tokio::time::sleep(Duration::from_millis(2200)).await;
        assert!(manager.room_exists(&key));
        assert_eq!(
            manager.list_rooms(&key.project, std::slice::from_ref(&key.name))[0].sid,
            new.snapshot().sid
        );
        drop(new);
        manager.close_all().await;
        net.shutdown();
    }

    #[tokio::test]
    async fn aborting_actual_lease_holder_releases_only_its_generation() {
        let (manager, net) = dispatch_manager().await;
        let key = RoomKey::new(ProjectId::local(), "auto-egress-cancel-owner");
        let owned = manager.clone();
        let owned_key = key.clone();
        let (acquired, ready) = tokio::sync::oneshot::channel();
        let task = tokio::spawn(async move {
            let lease = owned
                .create_room_lease(
                    owned_key,
                    crate::CreateRoomOptions {
                        config: RoomConfig {
                            empty_timeout_s: Some(1),
                            ..Default::default()
                        },
                    },
                )
                .await
                .unwrap();
            let _ = acquired.send(lease.snapshot().sid);
            // This pending initialization represents cancellation lifetime only;
            // no worker reply or worker error is fabricated by this test.
            std::future::pending::<()>().await;
            drop(lease);
        });
        let sid = tokio::time::timeout(Duration::from_secs(2), ready)
            .await
            .unwrap()
            .unwrap();
        tokio::time::sleep(Duration::from_millis(2200)).await;
        assert_eq!(
            manager.list_rooms(&key.project, std::slice::from_ref(&key.name))[0].sid,
            sid
        );
        task.abort();
        assert!(task.await.unwrap_err().is_cancelled());
        tokio::time::timeout(Duration::from_secs(3), async {
            while manager.room_exists(&key) {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("aborted future must release the real owner's idle hold");
        let replacement = manager
            .create_room_lease(key, Default::default())
            .await
            .unwrap();
        assert_ne!(replacement.snapshot().sid, sid);
        drop(replacement);
        manager.close_all().await;
        net.shutdown();
    }
}

#[cfg(test)]
mod automatic_recording_owner_tests {
    use super::*;
    use rtc_core::data::{DataHandler, DataSender, DataVerdict};
    use rtc_core::{
        auto_egress::*,
        permission::{Grants, RoomConfig, VideoGrant},
        session::*,
    };
    struct DropData;
    impl DataHandler for DropData {
        fn inbound(
            &self,
            _: &DataSender<'_>,
            _: rtc_core::media_api::DataKind,
            _: &str,
            _: &[u8],
        ) -> DataVerdict {
            DataVerdict::Drop
        }
    }
    use crate::automatic_recording::{RecordingExecutor, RecordingLauncher};
    use rtc_core::events::NullSink;
    use std::{future::Future, pin::Pin};
    struct Recorder(mpsc::UnboundedSender<RecordingLaunch>);
    impl RecordingLauncher for Recorder {
        fn launch(
            &self,
            _room: RoomInfo,
            action: RecordingLaunch,
        ) -> Pin<Box<dyn Future<Output = Result<(), String>> + Send + 'static>> {
            let sender = self.0.clone();
            Box::pin(async move {
                sender.send(action).map_err(|e| e.to_string())?;
                Ok(())
            })
        }
    }
    fn params(identity: &str, protocol: i32, single_pc: bool) -> JoinParams {
        JoinParams {
            room: RoomKey::new(ProjectId::local(), "r1"),
            grants: Grants {
                identity: identity.into(),
                video: VideoGrant {
                    room_join: true,
                    room: "r1".into(),
                    ..Default::default()
                },
                ..Default::default()
            },
            identity: identity.into(),
            name: String::new(),
            metadata: String::new(),
            attributes: BTreeMap::new(),
            client: ClientMeta {
                sdk: SdkKind::Js,
                protocol,
                ..Default::default()
            },
            capabilities: ClientCapabilities {
                video_publication_requires_rtp: false,
                codec_switch_requires_renegotiation: false,
                ice_tcp: true,
                one_shot: false,
                subscriber_primary: true,
                speaker_changed: true,
                connection_quality: true,
                ice_lite: true,
                unpublish: true,
                fast_start: true,
                disconnected_update: true,
                sync_stream_id: true,
                disable_playout_delay_for_sync: false,
                connection_quality_lost: true,
                identity_based_reconnection: true,
                regions_in_leave: true,
                non_error_signal_response: true,
                request_response: true,
                audio_red: true,
            },
            auto_subscribe: true,
            auto_subscribe_data_track: false,
            adaptive_stream: false,
            single_pc,
            disable_ice_lite: false,
            subscriber_allow_pause: None,
            reconnect: false,
            reconnect_reason: 0,
            participant_sid: None,
            add_tracks: vec![],
            publisher_offer: None,
        }
    }

    #[tokio::test]
    async fn add_track_ack_does_not_launch_until_owner_media_registration_and_duplicate_does_not_relaunch()
     {
        let (manager, net) = super::tests::dispatch_manager().await;
        let cfg = manager.config();
        let now = Instant::now();
        let room = Room::new(
            RoomKey::new(ProjectId::local(), "r1"),
            Arc::new(rtc_core::config::RoomPolicy::from(cfg.as_ref())),
            Arc::new(DropData),
            &RoomConfig::default(),
            now,
        );
        let snapshot = Arc::new(Mutex::new(RoomSnapshot {
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
        let (tx, rx) = mpsc::channel(ROOM_INBOX);
        let mut task =
            RoomTask::new(room, cfg, net.clone(), Arc::new(NullSink), snapshot, tx, rx).unwrap();
        let executor = Arc::new(RecordingExecutor::default());
        let (events, mut received) = mpsc::unbounded_channel();
        executor.install(Arc::new(Recorder(events))).unwrap();
        task.initialize_recording(
            Some(AutomaticRecording {
                participant: Some(Default::default()),
                tracks: Some(Default::default()),
                ..Default::default()
            }),
            executor.clone(),
        );
        let (reply, joined) = oneshot::channel();
        task.on_input(RoomInput::Join {
            params: Box::new(params("human", 17, false)),
            session_started_at: tokio::time::Instant::now().into_std(),
            reply,
        });
        let joined = joined.await.unwrap().unwrap();
        assert!(received.try_recv().is_err());
        task.room.handle_request(
            &joined.participant_sid,
            joined.generation,
            SessionRequest::AddTrack(AddTrack {
                token: 1,
                cid: "actual-audio".into(),
                name: String::new(),
                kind: TrackKind::Audio,
                width: 0,
                height: 0,
                muted: false,
                disable_dtx: false,
                source: TrackSource::Microphone,
                layers: vec![],
                simulcast_codecs: vec![],
                sid: None,
                stereo: false,
                disable_red: false,
                encryption: EncryptionType::None,
                stream: String::new(),
                backup_codec_policy: BackupCodecPolicy::PreferRegression,
                audio_features: vec![],
                packet_trailer_features: vec![],
            }),
            now,
        );

        let effects = task.room.take_effects();
        let transport = effects
            .iter()
            .find_map(|e| match e {
                Effect::Media(MediaCommand::ExpectTrack { transport, .. }) => Some(*transport),
                _ => None,
            })
            .expect("actual AddTrack effect carries publisher transport");
        for effect in effects {
            task.execute(effect, now);
        }
        assert!(
            received.try_recv().is_err(),
            "AddTrack ACK is not media publication"
        );
        let media = MediaEvent::RemoteMediaAdded(rtc_core::media_api::RemoteMedia {
            registration: rtc_core::media_api::PublicationRegistration::SdpReceiver,
            participant: joined.participant_sid.clone(),
            transport,
            mid: "0".into(),
            kind: TrackKind::Audio,
            msid_track_id: "actual-audio".into(),
            rids: vec![],
            mime_type: "audio/opus".into(),
        });
        // Media only arrives over a connected (DTLS) publisher transport.
        task.room.handle_media_event(
            MediaEvent::TransportConnected {
                participant: joined.participant_sid.clone(),
                transport,
            },
            now,
        );
        task.room.handle_media_event(media.clone(), now);
        task.settle(now);
        let first = tokio::time::timeout(Duration::from_secs(2), received.recv())
            .await
            .unwrap()
            .unwrap();
        let second = tokio::time::timeout(Duration::from_secs(2), received.recv())
            .await
            .unwrap()
            .unwrap();
        let actions = [first, second];
        assert_eq!(actions.iter().filter(|a|matches!(a,RecordingLaunch::Participant{participant_sid,..}if participant_sid==&joined.participant_sid)).count(),1);
        assert_eq!(actions.iter().filter(|a|matches!(a,RecordingLaunch::Track{participant_sid,..}if participant_sid==&joined.participant_sid)).count(),1);
        task.room.handle_media_event(media, now);
        task.settle(now);
        executor.drain().await;
        assert!(
            received.try_recv().is_err(),
            "duplicate actual owner publication cannot issue another worker attempt"
        );
        net.shutdown();
    } // This recorder captures authorized work only. It returns an error so no
    // test can mistake an observed launch intention for successful worker IO.
    struct CreationRecorder(mpsc::UnboundedSender<(RoomInfo, RoomCompositeRecordingPolicy)>);
    impl RecordingLauncher for CreationRecorder {
        fn launch_room(
            &self,
            room: RoomInfo,
            policy: RoomCompositeRecordingPolicy,
        ) -> Pin<Box<dyn Future<Output = Result<(), String>> + Send + 'static>> {
            let sender = self.0.clone();
            Box::pin(async move {
                sender.send((room, policy)).map_err(|e| e.to_string())?;
                Ok(())
            })
        }
        fn launch(
            &self,
            _: RoomInfo,
            _: RecordingLaunch,
        ) -> Pin<Box<dyn Future<Output = Result<(), String>> + Send + 'static>> {
            Box::pin(async { Err("test only expects create-composite consumer".into()) })
        }
    }
    #[tokio::test]
    async fn owner_join_inherits_whole_preset_and_existing_owner_does_not_relaunch() {
        let (_, net) = super::tests::dispatch_manager().await;
        let mut config = super::tests::test_config();
        config.room.presets.insert(
            "record".into(),
            Some(RoomConfig {
                automatic_recording: Some(AutomaticRecording {
                    room: Some(RoomCompositeRecordingPolicy {
                        layout: "preset-layout".into(),
                        audio_only: true,
                        ..Default::default()
                    }),
                    tracks: Some(Default::default()),
                    ..Default::default()
                }),
                ..Default::default()
            }),
        );
        let manager = crate::RoomManager::new(crate::RuntimeDeps {
            config: Arc::new(config),
            net: net.clone(),
            events: Arc::new(NullSink),
            data: Arc::new(DropData),
        });
        let (tx, mut rx) = mpsc::unbounded_channel();
        manager
            .attach_recording_launcher(Arc::new(CreationRecorder(tx)))
            .unwrap();
        let mut first = params("first", 17, false);
        first.grants.room_preset = "record".into();
        let one = manager.join(first.clone()).await.unwrap();
        let (room, policy) = rx.try_recv().unwrap();
        assert_eq!(policy.layout, "preset-layout");
        assert!(policy.audio_only);
        let mut second = params("second", 17, false);
        second.grants.room_preset = "missing-but-existing-owner".into();
        let two = manager.join(second).await.unwrap();
        assert!(rx.try_recv().is_err());
        assert_eq!(room.name.0, "r1");
        let mut empty = params("third", 17, false);
        empty.room = RoomKey::new(ProjectId::local(), "explicit-empty");
        empty.grants.video.room = "explicit-empty".into();
        empty.grants.room_preset = "record".into();
        empty.grants.room_config = Some(RoomConfig {
            automatic_recording: Some(Default::default()),
            ..Default::default()
        });
        let three = manager.join(empty).await.unwrap();
        assert!(
            rx.try_recv().is_err(),
            "empty Egress suppresses entire preset including composite"
        );
        drop((one, two, three));
        manager.close_all().await;
        net.shutdown();
    }

    struct IntentOnly(mpsc::UnboundedSender<RecordingLaunch>);
    impl RecordingLauncher for IntentOnly {
        fn launch(
            &self,
            _: RoomInfo,
            action: RecordingLaunch,
        ) -> Pin<Box<dyn Future<Output = Result<(), String>> + Send + 'static>> {
            let sender = self.0.clone();
            Box::pin(async move {
                sender.send(action).map_err(|e| e.to_string())?;
                Err("owner-boundary capture; no worker was called".into())
            })
        }
    }
    async fn video_owner() -> (
        RoomTask,
        Arc<RecordingExecutor>,
        mpsc::UnboundedReceiver<RecordingLaunch>,
        NetRouter,
    ) {
        let (manager, net) = super::tests::dispatch_manager().await;
        let mut cfg = manager.config().as_ref().clone();
        if !cfg
            .room
            .enabled_codecs
            .iter()
            .any(|c| c.mime.eq_ignore_ascii_case("video/vp8"))
        {
            cfg.room.enabled_codecs.push(CodecSpec {
                mime: "video/VP8".into(),
                fmtp_line: String::new(),
            });
        }
        let now = Instant::now();
        let room = Room::new(
            RoomKey::new(ProjectId::local(), "r1"),
            Arc::new(rtc_core::config::RoomPolicy::from(&cfg)),
            Arc::new(DropData),
            &RoomConfig::default(),
            now,
        );
        let snapshot = Arc::new(Mutex::new(RoomSnapshot {
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
        let (tx, rx) = mpsc::channel(ROOM_INBOX);
        let mut task = RoomTask::new(
            room,
            &cfg,
            net.clone(),
            Arc::new(NullSink),
            snapshot,
            tx,
            rx,
        )
        .unwrap();
        let executor = Arc::new(RecordingExecutor::default());
        let (send, receive) = mpsc::unbounded_channel();
        executor.install(Arc::new(IntentOnly(send))).unwrap();
        task.initialize_recording(
            Some(AutomaticRecording {
                room: None,
                participant: Some(Default::default()),
                tracks: Some(Default::default()),
            }),
            executor.clone(),
        );
        (task, executor, receive, net)
    }
    async fn owner_join(task: &mut RoomTask, request: JoinParams) -> SessionHandle {
        let (reply, result) = oneshot::channel();
        task.on_input(RoomInput::Join {
            params: Box::new(request),
            session_started_at: tokio::time::Instant::now().into_std(),
            reply,
        });
        result.await.unwrap().unwrap()
    }
    async fn no_intent(receive: &mut mpsc::UnboundedReceiver<RecordingLaunch>) {
        tokio::task::yield_now().await;
        assert!(
            receive.try_recv().is_err(),
            "this owner phase must not authorize automatic work"
        );
    }
    async fn two_intents(
        receive: &mut mpsc::UnboundedReceiver<RecordingLaunch>,
        sid: &ParticipantSid,
    ) {
        let first = tokio::time::timeout(Duration::from_secs(2), receive.recv())
            .await
            .unwrap()
            .unwrap();
        let second = tokio::time::timeout(Duration::from_secs(2), receive.recv())
            .await
            .unwrap()
            .unwrap();
        let values = [first, second];
        assert_eq!(values.iter().filter(|a|matches!(a,RecordingLaunch::Participant {participant_sid,..} if participant_sid==sid)).count(),1);
        assert_eq!(values.iter().filter(|a|matches!(a,RecordingLaunch::Track {participant_sid,..} if participant_sid==sid)).count(),1);
        no_intent(receive).await;
    }
    fn video_ack(task: &mut RoomTask, joined: &SessionHandle, cid: &str, mid: &str) -> MediaEvent {
        let now = Instant::now();
        task.room.handle_request(
            &joined.participant_sid,
            joined.generation,
            SessionRequest::AddTrack(AddTrack {
                token: 1,
                cid: cid.into(),
                name: String::new(),
                kind: TrackKind::Video,
                width: 640,
                height: 360,
                muted: false,
                disable_dtx: false,
                source: TrackSource::Camera,
                layers: vec![],
                simulcast_codecs: vec![],
                sid: None,
                stereo: false,
                disable_red: false,
                encryption: EncryptionType::None,
                stream: String::new(),
                backup_codec_policy: BackupCodecPolicy::PreferRegression,
                audio_features: vec![],
                packet_trailer_features: vec![],
            }),
            now,
        );
        let effects = task.room.take_effects();
        assert!(
            effects.iter().any(|e| matches!(
                e,
                Effect::Signal {
                    event: SignalEvent::TrackPublished { .. },
                    ..
                }
            )),
            "real AddTrack produces its ACK"
        );
        let transport = effects
            .iter()
            .find_map(|e| match e {
                Effect::Media(MediaCommand::ExpectTrack { transport, .. }) => Some(*transport),
                _ => None,
            })
            .expect("actual AddTrack authorizes publisher transport");
        for effect in effects {
            task.execute(effect, now);
        }
        MediaEvent::RemoteMediaAdded(rtc_core::media_api::RemoteMedia {
            registration: rtc_core::media_api::PublicationRegistration::SdpReceiver,
            participant: joined.participant_sid.clone(),
            transport,
            mid: mid.into(),
            kind: TrackKind::Video,
            msid_track_id: cid.into(),
            rids: vec![],
            mime_type: "video/VP8".into(),
        })
    }
    fn accepted_video(task: &mut RoomTask, event: MediaEvent, identity: &str) {
        let now = Instant::now();
        // Media only arrives over a connected (DTLS) publisher transport; fixed Go publishes a
        // declared-SSRC section from pion OnTrack once that transport is established.
        if let MediaEvent::RemoteMediaAdded(media) = &event {
            task.room.handle_media_event(
                MediaEvent::TransportConnected {
                    participant: media.participant.clone(),
                    transport: media.transport,
                },
                now,
            );
        }
        task.room.handle_media_event(event, now);
        task.settle(now);
        let info = task.room.get_participant(&identity.into()).unwrap();
        assert!(
            info.tracks.iter().any(|t| t.kind == TrackKind::Video),
            "real core video publication committed"
        );
    }
    #[tokio::test]
    async fn active_only_and_add_track_ack_do_not_launch_but_video_publication_does() {
        let (mut task, executor, mut received, net) = video_owner().await;
        let joined = owner_join(&mut task, params("video-only", 17, true)).await;
        no_intent(&mut received).await;
        let ids = task.engine.transports_of(&joined.participant_sid);
        assert_eq!(ids.len(), 1);
        let now = Instant::now();
        task.room.handle_media_event(
            MediaEvent::TransportConnected {
                participant: joined.participant_sid.clone(),
                transport: ids[0],
            },
            now,
        );
        task.settle(now);
        assert_eq!(
            task.room
                .get_participant(&"video-only".into())
                .unwrap()
                .state,
            ParticipantState::Active
        );
        no_intent(&mut received).await;
        let media = video_ack(&mut task, &joined, "video-first", "0");
        no_intent(&mut received).await;
        accepted_video(&mut task, media, "video-only");
        two_intents(&mut received, &joined.participant_sid).await;
        task.shutdown().await;
        executor.drain().await;
        net.shutdown();
    }
    #[tokio::test]
    async fn same_sid_resume_retains_participant_attempt_but_new_video_has_its_own_track_attempt() {
        let (mut task, executor, mut received, net) = video_owner().await;
        let joined = owner_join(&mut task, params("resume-owner", 17, true)).await;
        let media = video_ack(&mut task, &joined, "video-before-resume", "0");
        accepted_video(&mut task, media.clone(), "resume-owner");
        two_intents(&mut received, &joined.participant_sid).await;
        let mut request = params("resume-owner", 17, true);
        request.reconnect = true;
        request.participant_sid = Some(joined.participant_sid.clone());
        let resumed = owner_join(&mut task, request).await;
        assert_eq!(resumed.participant_sid, joined.participant_sid);
        assert_ne!(resumed.generation, joined.generation);
        task.room.handle_media_event(media, Instant::now());
        task.settle(Instant::now());
        no_intent(&mut received).await;
        let next = video_ack(&mut task, &resumed, "video-after-resume", "1");
        accepted_video(&mut task, next, "resume-owner");
        let action = tokio::time::timeout(Duration::from_secs(2), received.recv())
            .await
            .unwrap()
            .unwrap();
        assert!(
            matches!(action,RecordingLaunch::Track {participant_sid,..} if participant_sid==resumed.participant_sid)
        );
        no_intent(&mut received).await;
        task.shutdown().await;
        executor.drain().await;
        net.shutdown();
    }
    #[tokio::test]
    async fn replacement_sid_gets_new_first_publication_and_stale_old_media_does_not_clear_it() {
        let (mut task, executor, mut received, net) = video_owner().await;
        let old = owner_join(&mut task, params("replacement-owner", 17, true)).await;
        let old_media = video_ack(&mut task, &old, "video-old-owner", "0");
        accepted_video(&mut task, old_media.clone(), "replacement-owner");
        two_intents(&mut received, &old.participant_sid).await;
        let new = owner_join(&mut task, params("replacement-owner", 17, true)).await;
        assert_ne!(new.participant_sid, old.participant_sid);
        no_intent(&mut received).await;
        task.room.handle_media_event(old_media, Instant::now());
        task.settle(Instant::now());
        no_intent(&mut received).await;
        let media = video_ack(&mut task, &new, "video-new-owner", "0");
        accepted_video(&mut task, media.clone(), "replacement-owner");
        two_intents(&mut received, &new.participant_sid).await;
        task.room.handle_media_event(media, Instant::now());
        task.settle(Instant::now());
        no_intent(&mut received).await;
        task.shutdown().await;
        executor.drain().await;
        net.shutdown();
    }
}

#[cfg(test)]
mod shared_catalog_generation_tests {
    use super::tests::dispatch_manager;
    use super::*;
    use rtc_core::permission::{RoomAgentDispatchOptions, RoomConfig};

    #[tokio::test]
    async fn catalog_bootstrap_uses_one_real_actor_and_current_dispatch_initialization() {
        let (manager, net) = dispatch_manager().await;
        let key = RoomKey::new(ProjectId::local(), "catalog-actor");
        let seed = manager
            .create_room_lease(key.clone(), Default::default())
            .await
            .unwrap();
        let mut catalog = seed.snapshot();
        manager.delete_room(&key).await.unwrap();
        drop(seed);
        catalog.num_participants = 37;
        catalog.num_publishers = 19;
        catalog.active_recording = true;
        catalog.metadata = "catalog-metadata".into();
        let options = crate::CreateRoomOptions {
            config: RoomConfig {
                agent_dispatches: Some(vec![RoomAgentDispatchOptions {
                    agent_name: "catalog-agent".into(),
                    ..Default::default()
                }]),
                ..Default::default()
            },
        };
        let left_manager = manager.clone();
        let right_manager = manager.clone();
        let (left, right) = tokio::join!(
            left_manager.create_room_lease_from_catalog(
                key.clone(),
                options.clone(),
                catalog.clone()
            ),
            right_manager.create_room_lease_from_catalog(key.clone(), options, catalog.clone()),
        );
        let left = left.unwrap();
        let right = right.unwrap();
        assert_ne!(left.created(), right.created());
        assert_eq!(left.snapshot().sid, catalog.sid);
        assert_eq!(right.snapshot().sid, catalog.sid);
        let actual = left.current_snapshot().await;
        assert_eq!(actual.metadata, "catalog-metadata");
        assert_eq!(actual.num_participants, 0);
        assert_eq!(actual.num_publishers, 0);
        assert!(!actual.active_recording);
        let dispatches = manager.list_agent_dispatches(&key, None).await.unwrap();
        assert_eq!(dispatches.len(), 1);
        assert_eq!(dispatches[0].agent_name, "catalog-agent");
        let mut wrong_generation = catalog.clone();
        wrong_generation.sid = RoomSid::generate();
        assert!(matches!(
            manager
                .create_room_lease_from_catalog(key.clone(), Default::default(), wrong_generation)
                .await,
            Err(crate::api::ApiError::FailedPrecondition(_))
        ));
        assert_eq!(manager.list_rooms(&key.project, &[])[0].sid, catalog.sid);
        drop(left);
        drop(right);
        manager.close_all().await;
        net.shutdown();
    }

    #[tokio::test]
    async fn catalog_lease_holds_exact_generation_but_explicit_delete_fences_replacement() {
        let (manager, net) = dispatch_manager().await;
        let key = RoomKey::new(ProjectId::local(), "catalog-held");
        let seed = manager
            .create_room_lease(key.clone(), Default::default())
            .await
            .unwrap();
        let mut catalog = seed.snapshot();
        manager.delete_room(&key).await.unwrap();
        drop(seed);
        catalog.empty_timeout_s = 1;
        let held = manager
            .create_room_lease_from_catalog(key.clone(), Default::default(), catalog.clone())
            .await
            .unwrap();
        tokio::time::sleep(Duration::from_millis(2200)).await;
        assert!(
            manager.room_exists(&key),
            "actual idle hold must span external awaits"
        );
        manager
            .update_room_metadata(&key, "changed-during-worker-await".into())
            .await
            .unwrap();
        assert_eq!(
            held.current_snapshot().await.metadata,
            "changed-during-worker-await"
        );
        manager.delete_room(&key).await.unwrap();
        let replacement = manager
            .create_room_lease(key.clone(), Default::default())
            .await
            .unwrap();
        assert_ne!(replacement.snapshot().sid, catalog.sid);
        assert_eq!(held.current_snapshot().await.sid, catalog.sid);
        drop(held);
        assert_eq!(
            replacement.current_snapshot().await.sid,
            replacement.snapshot().sid
        );
        assert!(manager.room_exists(&key));
        drop(replacement);
        manager.close_all().await;
        net.shutdown();
    }

    #[tokio::test]
    async fn invalid_catalog_identity_does_not_create_or_mutate_an_owner() {
        let (manager, net) = dispatch_manager().await;
        let key = RoomKey::new(ProjectId::local(), "catalog-invalid");
        let held = manager
            .create_room_lease(key.clone(), Default::default())
            .await
            .unwrap();
        let original = held.snapshot();
        let mut invalid = original.clone();
        invalid.sid = RoomSid(String::new());
        assert!(matches!(
            manager
                .create_room_lease_from_catalog(key.clone(), Default::default(), invalid)
                .await,
            Err(crate::api::ApiError::FailedPrecondition(_))
        ));
        let mut invalid = original.clone();
        invalid.name = RoomName("another-room".into());
        assert!(matches!(
            manager
                .create_room_lease_from_catalog(key.clone(), Default::default(), invalid)
                .await,
            Err(crate::api::ApiError::FailedPrecondition(_))
        ));
        assert_eq!(held.current_snapshot().await.sid, original.sid);
        assert_eq!(manager.list_rooms(&key.project, &[]).len(), 1);
        drop(held);
        manager.close_all().await;
        net.shutdown();
    }
}

#[cfg(test)]
mod captured_owner_generation_tests {
    use super::tests::dispatch_manager;
    use super::*;
    #[tokio::test]
    async fn captured_dispatch_owner_never_resolves_replacement_by_name() {
        let (manager, net) = dispatch_manager().await;
        let key = RoomKey::new(ProjectId::local(), "captured-dispatch-owner");
        let old = manager
            .create_room_lease(key.clone(), Default::default())
            .await
            .unwrap();
        let captured = manager
            .capture_room_owner(&key, &old.snapshot().sid)
            .unwrap();
        manager.delete_room(&key).await.unwrap();
        let fresh = manager
            .create_room_lease(key.clone(), Default::default())
            .await
            .unwrap();
        let id = "AD_same-id";
        assert!(
            captured
                .create_agent_dispatch(crate::api::AgentDispatchOptions {
                    id: id.into(),
                    ..Default::default()
                })
                .await
                .is_err()
        );
        assert!(captured.list_agent_dispatches(None).await.is_err());
        assert!(captured.delete_agent_dispatch(id).await.is_err());
        assert!(
            manager
                .list_agent_dispatches(&key, Some(id))
                .await
                .unwrap()
                .is_empty()
        );
        assert!(manager.capture_room_owner(&key, captured.sid()).is_err());
        let new_owner = manager
            .capture_room_owner(&key, &fresh.snapshot().sid)
            .unwrap();
        let created = new_owner
            .create_agent_dispatch(crate::api::AgentDispatchOptions {
                id: id.into(),
                ..Default::default()
            })
            .await
            .unwrap();
        assert_eq!(created.id, id);
        assert_eq!(new_owner.sid(), &fresh.snapshot().sid);
        assert_ne!(new_owner.sid(), captured.sid());
        drop(old);
        drop(fresh);
        manager.close_all().await;
        net.shutdown();
    }
    #[tokio::test]
    async fn captured_owner_capability_is_not_an_idle_hold() {
        let (manager, net) = dispatch_manager().await;
        let key = RoomKey::new(ProjectId::local(), "captured-owner-not-hold");
        let lease = manager
            .create_room_lease(
                key.clone(),
                crate::CreateRoomOptions {
                    config: rtc_core::permission::RoomConfig {
                        empty_timeout_s: Some(1),
                        ..Default::default()
                    },
                },
            )
            .await
            .unwrap();
        let captured = manager
            .capture_room_owner(&key, &lease.snapshot().sid)
            .unwrap();
        drop(lease);
        tokio::time::timeout(Duration::from_secs(3), async {
            while manager.room_exists(&key) {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("RPC registration must not hold an empty generation alive");
        assert!(captured.list_agent_dispatches(None).await.is_err());
        manager.close_all().await;
        net.shutdown();
    }
}

#[cfg(test)]
mod network_owner_consumers;

#[cfg(test)]
mod mdns_candidates;

#[cfg(test)]
mod mdns_e2e;

#[cfg(test)]
mod missing_subscription_owner;

#[cfg(test)]
#[path = "room_task/permission_restore_epoch.rs"]
mod permission_restore_epoch;

#[cfg(test)]
#[path = "room_task/subscription_settings_lifetime.rs"]
mod subscription_settings_lifetime;
