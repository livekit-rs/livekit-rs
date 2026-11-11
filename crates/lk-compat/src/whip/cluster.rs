//! Pinned WHIP / WHIPParticipant PSRPC ownership. HTTP front nodes never join
//! shadow rooms; only the selected node owns the real one-shot media session.
use super::{
    fragment::Document,
    offer,
    runtime::{Failure, ParticipantTopics},
};
use crate::{
    service_bus::{
        rpc::{CallOptions, MethodSpec, RpcClient, RpcError, RpcServer, ServerLimits},
        rpc_transport::RpcBus,
    },
    state::AppState,
    workers::retry::{GenericRetry, RpcPolicy},
};
use lk_protocol::rpc;
use opentelemetry::trace::FutureExt as _;
use rtc_core::{
    ids::{Identity, ParticipantSid, RoomKey},
    session::{JoinParams, NamedSubscription},
};
use std::{
    collections::HashMap,
    sync::{Arc, Weak},
    time::Duration,
};
use tokio::{sync::Mutex, task::JoinSet};

pub struct WhipRpc {
    state: AppState,
    bus: RpcBus,
    client: RpcClient,
    policy: RpcPolicy,
    create_server: Mutex<Option<RpcServer>>,
    groups: Mutex<HashMap<(RoomKey, Identity), (ParticipantSid, Vec<RpcServer>)>>,
    retiring: Mutex<JoinSet<()>>,
}
fn spec(service: &str, method: &str, topic: &str) -> MethodSpec {
    // Generated RegisterMethod affinity=false,multi=false,claim=true,queue=true.
    MethodSpec {
        service: service.into(),
        method: method.into(),
        topics: vec![topic.into()],
        require_claim: true,
        queue: true,
    }
}
/// WHIP ParticipantInit is distinct from Rust's internal one-shot PC mode.
/// Fixed whipservice.go initializes neither optional data-track subscription,
/// token expiry nor UseSinglePeerConnection; StartSession(..., true) separately
/// enables one-shot on the selected owner.
fn start_session(params: &JoinParams) -> Result<lk_protocol::livekit::StartSession, RpcError> {
    let mut start = crate::cluster_signal::codec::start_session(params, vec![])?;
    start.use_single_peer_connection = false;
    start.auto_subscribe_data_track = None;
    start.token_expires_at = 0;
    Ok(start)
}
fn participant_topic(room: &str, identity: &str) -> String {
    format!("{room}_{identity}")
}
fn limits() -> ServerLimits {
    ServerLimits {
        concurrent: 1024,
        max_request_duration: Duration::MAX,
        allow_skip_claim: false,
    }
}
fn rpc_error(value: Failure) -> RpcError {
    let code = match &value {
        Failure::NotFound | Failure::RoomNotFound => "not_found",
        Failure::InvalidFragment | Failure::MidMismatch | Failure::CredentialMismatch => {
            "invalid_argument"
        }
        Failure::IceSessionMismatch { .. } => "failed_precondition",
        Failure::Deadline => "deadline_exceeded",
        Failure::NoResponse => "unavailable",
        Failure::Remote(error) => return error.clone(),
        _ => "internal",
    };
    RpcError::new(code, super::failure_message(value))
}
fn failure(value: RpcError) -> Failure {
    Failure::Remote(value)
}
impl WhipRpc {
    pub async fn start(
        state: AppState,
        bus: RpcBus,
        policy: RpcPolicy,
    ) -> Result<Arc<Self>, RpcError> {
        let inbox = policy
            .handler_channel_size()
            .map_err(|e| RpcError::new("invalid_argument", e))?;
        let client = RpcClient::new(bus.with_inbox_size(inbox)?, 1024)?
            .with_tracing(crate::service_bus::telemetry::RpcTracing::production_client_params());
        let this = Arc::new(Self {
            state,
            bus,
            client,
            policy,
            create_server: Mutex::new(None),
            groups: Mutex::new(HashMap::new()),
            retiring: Mutex::new(JoinSet::new()),
        });
        let topic = this
            .state
            .node
            .as_ref()
            .map(|n| n.id())
            .unwrap_or_else(|| this.state.config.node_id.clone());
        let weak = Arc::downgrade(&this);
        let server = RpcServer::register::<rpc::WhipCreateRequest, rpc::WhipCreateResponse, _, _>(
            this.bus.clone(),
            spec("WHIP", "Create", &topic),
            limits(),
            move |ctx, request| {
                let weak = weak.clone();
                async move {
                    let this = weak.upgrade().ok_or_else(RpcError::closed)?;
                    this.create_owned(request, ctx.deadline).await
                }
            },
            None,
        )
        .with_context(crate::service_bus::telemetry::RpcTracing::factory_options().scoped_context())
        .await?;
        *this.create_server.lock().await = Some(server);
        this.state
            .whip
            .sessions
            .set_topics(Arc::new(Topics(Arc::downgrade(&this))));
        Ok(this)
    }
    async fn call<Q: prost::Message, R: prost::Message + Default>(
        &self,
        spec: &MethodSpec,
        request: &Q,
    ) -> Result<R, RpcError> {
        let mut retry = GenericRetry::new(self.policy);
        loop {
            let mut options = CallOptions::new(
                retry
                    .timeout()
                    .unwrap_or_else(|| self.policy.request_timeout()),
            );
            options.selection.accept_first = true;
            options.selection.affinity_timeout = Some(Duration::from_secs(1));
            match self.client.unary(spec, request, options).await {
                Err(error) if retry.next(&error) => {}
                result => return result,
            }
        }
    }
    pub(super) async fn create(
        &self,
        params: &JoinParams,
        offer: String,
        targets: Vec<NamedSubscription>,
        from_ingress: bool,
    ) -> Result<rpc::WhipCreateResponse, RpcError> {
        let node = if let Some(control) = &self.state.room_control {
            control
                .select_whip_owner(&params.room.name.0)
                .await
                .map_err(|e| e.with_application_metadata("whip_allocation", "true"))?
        } else {
            self.state
                .check_local_room_allocation()
                .map_err(|e| e.with_application_metadata("whip_allocation", "true"))?;
            self.state
                .node
                .as_ref()
                .map(|n| n.id())
                .unwrap_or_else(|| self.state.config.node_id.clone())
        };
        let start = start_session(params)?;
        let mut subscribed_participant_tracks: HashMap<
            String,
            rpc::whip_create_request::TrackList,
        > = HashMap::new();
        for target in targets {
            subscribed_participant_tracks
                .entry(target.publisher.0)
                .or_default()
                .track_names
                .push(target.track_name);
        }
        self.call(
            &spec("WHIP", "Create", &node),
            &rpc::WhipCreateRequest {
                start_session: Some(start),
                offer_sdp: offer,
                subscribed_participant_tracks,
                from_ingress,
            },
        )
        .await
    }
    async fn create_owned(
        &self,
        request: rpc::WhipCreateRequest,
        deadline: std::time::Instant,
    ) -> Result<rpc::WhipCreateResponse, RpcError> {
        let start = request
            .start_session
            .ok_or_else(|| RpcError::new("invalid_argument", "missing start session"))?;
        if let Some(control) = &self.state.room_control {
            control.verify_signal_owner(&start.room_name).await?;
        }
        let key = self
            .state
            .keys
            .first()
            .map(|(key, _)| key)
            .unwrap_or_default();
        let mut params = crate::cluster_signal::codec::join_params(key, &start)?;
        super::prepare_owner(&self.state, &mut params.grants).map_err(rpc_error)?;
        params.add_tracks = offer::synthesize(&request.offer_sdp);
        let room = params.room.clone();
        let identity = params.identity.clone();
        let targets = request
            .subscribed_participant_tracks
            .into_iter()
            .flat_map(|(name, list)| {
                list.track_names
                    .into_iter()
                    .map(move |track_name| NamedSubscription {
                        publisher: Identity(name.clone()),
                        track_name,
                    })
            })
            .collect();
        let answer = self
            .state
            .whip
            .sessions
            .create(params, request.offer_sdp, targets, deadline)
            .await
            .map_err(rpc_error)?;
        if request.from_ingress {
            self.state
                .whip
                .notify(&self.state, room, identity, answer.sid.clone())
                .await;
        }
        let ice_servers = self
            .state
            .keys
            .first()
            .map(|(key, _)| crate::signal::ice::ice_servers(&self.state, key, &answer.sid))
            .unwrap_or_default();
        Ok(rpc::WhipCreateResponse {
            answer_sdp: answer.sdp,
            participant_id: answer.sid.0,
            ice_servers,
            ice_session_id: answer.ice_session_id,
        })
    }
    pub(super) async fn trickle(
        &self,
        room: &RoomKey,
        identity: &Identity,
        sid: &ParticipantSid,
        tag: String,
        fragment: String,
    ) -> Result<(), Failure> {
        self.call::<_, pbjson_types::Empty>(
            &spec(
                "WHIPParticipant",
                "ICETrickle",
                &participant_topic(&room.name.0, &identity.0),
            ),
            &rpc::WhipParticipantIceTrickleRequest {
                room: room.name.0.clone(),
                participant_identity: identity.0.clone(),
                participant_id: sid.0.clone(),
                ice_session_id: tag,
                sdp_fragment: fragment,
            },
        )
        .await
        .map(|_| ())
        .map_err(failure)
    }
    pub(super) async fn restart(
        &self,
        room: &RoomKey,
        identity: &Identity,
        sid: &ParticipantSid,
        fragment: String,
    ) -> Result<rpc::WhipParticipantIceRestartResponse, Failure> {
        self.call(
            &spec(
                "WHIPParticipant",
                "ICERestart",
                &participant_topic(&room.name.0, &identity.0),
            ),
            &rpc::WhipParticipantIceRestartRequest {
                room: room.name.0.clone(),
                participant_identity: identity.0.clone(),
                participant_id: sid.0.clone(),
                sdp_fragment: fragment,
            },
        )
        .await
        .map_err(failure)
    }
    pub(super) async fn delete(
        &self,
        room: &RoomKey,
        identity: &Identity,
        sid: &ParticipantSid,
    ) -> Result<(), Failure> {
        self.call::<_, pbjson_types::Empty>(
            &spec(
                "WHIPParticipant",
                "DeleteSession",
                &participant_topic(&room.name.0, &identity.0),
            ),
            &rpc::WhipParticipantDeleteSessionRequest {
                room: room.name.0.clone(),
                participant_identity: identity.0.clone(),
                participant_id: sid.0.clone(),
                from_sweeper: false,
            },
        )
        .await
        .map(|_| ())
        .map_err(failure)
    }
    async fn retire(&self, servers: Vec<RpcServer>) {
        let mut tasks = self.retiring.lock().await;
        while tasks.try_join_next().is_some() {}
        for server in servers {
            if let Ok(retirement) = server.retire().await {
                tasks.spawn(retirement.finish());
            }
        }
    }
    async fn register_participant(
        self: &Arc<Self>,
        room: &RoomKey,
        identity: &Identity,
        sid: &ParticipantSid,
    ) -> Result<(), Failure> {
        let mut groups = self.groups.lock().await;
        if let Some((_, old)) = groups.remove(&(room.clone(), identity.clone())) {
            self.retire(old).await;
        }
        let topic = participant_topic(&room.name.0, &identity.0);
        let weak = Arc::downgrade(self);
        let mut servers = Vec::new();
        let result = async {
            let w = weak.clone();
            servers.push(
                RpcServer::register::<
                    rpc::WhipParticipantIceTrickleRequest,
                    pbjson_types::Empty,
                    _,
                    _,
                >(
                    self.bus.clone(),
                    spec("WHIPParticipant", "ICETrickle", &topic),
                    limits(),
                    move |ctx, r| {
                        let w = w.clone();
                        async move {
                            let this = w.upgrade().ok_or_else(RpcError::closed)?;
                            this.state
                                .whip
                                .sessions
                                .trickle(
                                    &this.state.room_key(&r.room),
                                    &Identity(r.participant_identity),
                                    &ParticipantSid(r.participant_id),
                                    r.ice_session_id,
                                    r.sdp_fragment,
                                    ctx.deadline,
                                )
                                .await
                                .map_err(rpc_error)?;
                            Ok(pbjson_types::Empty {})
                        }
                    },
                    None,
                )
                .await?,
            );
            let w = weak.clone();
            servers.push(
                RpcServer::register::<
                    rpc::WhipParticipantIceRestartRequest,
                    rpc::WhipParticipantIceRestartResponse,
                    _,
                    _,
                >(
                    self.bus.clone(),
                    spec("WHIPParticipant", "ICERestart", &topic),
                    limits(),
                    move |ctx, r| {
                        let w = w.clone();
                        async move {
                            let this = w.upgrade().ok_or_else(RpcError::closed)?;
                            let answer = this
                                .state
                                .whip
                                .sessions
                                .restart(
                                    &this.state.room_key(&r.room),
                                    &Identity(r.participant_identity),
                                    &ParticipantSid(r.participant_id),
                                    r.sdp_fragment,
                                    ctx.deadline,
                                )
                                .await
                                .map_err(rpc_error)?;
                            let fragment = Document::from_validated(&answer.sdp)
                                .extract_fragment()
                                .map_err(|e| {
                                    RpcError::new("internal", format!("local SDP fragment: {e:?}"))
                                })?;
                            Ok(rpc::WhipParticipantIceRestartResponse {
                                ice_session_id: answer.ice_session_id,
                                sdp_fragment: fragment.marshal(),
                            })
                        }
                    },
                    None,
                )
                .await?,
            );
            let w = weak;
            servers.push(
                RpcServer::register::<
                    rpc::WhipParticipantDeleteSessionRequest,
                    pbjson_types::Empty,
                    _,
                    _,
                >(
                    self.bus.clone(),
                    spec("WHIPParticipant", "DeleteSession", &topic),
                    limits(),
                    move |ctx, r| {
                        let w = w.clone();
                        async move {
                            let this = w.upgrade().ok_or_else(RpcError::closed)?;
                            this.state
                                .whip
                                .sessions
                                .delete(
                                    &this.state.room_key(&r.room),
                                    &Identity(r.participant_identity),
                                    &ParticipantSid(r.participant_id),
                                    ctx.deadline,
                                )
                                .await
                                .map_err(rpc_error)?;
                            Ok(pbjson_types::Empty {})
                        }
                    },
                    None,
                )
                .await?,
            );
            Ok::<(), RpcError>(())
        }
        .await;
        if let Err(error) = result {
            for server in servers {
                server.close().await;
            }
            return Err(failure(error));
        }
        groups.insert((room.clone(), identity.clone()), (sid.clone(), servers));
        Ok(())
    }
    pub(crate) async fn stop_admission(&self) {
        if let Some(server) = self.create_server.lock().await.take() {
            server.close().await;
        }
        self.client.close_and_wait().await;
    }
    pub(crate) async fn shutdown(&self) {
        let groups = std::mem::take(&mut *self.groups.lock().await);
        for (_, (_, servers)) in groups {
            for server in servers {
                server.close().await;
            }
        }
        let mut tasks = self.retiring.lock().await;
        while tasks.join_next().await.is_some() {}
    }
}
struct Topics(Weak<WhipRpc>);
impl ParticipantTopics for Topics {
    fn opened<'a>(
        &'a self,
        room: &'a RoomKey,
        identity: &'a Identity,
        sid: &'a ParticipantSid,
    ) -> futures_util::future::BoxFuture<'a, Result<(), Failure>> {
        Box::pin(async move {
            self.0
                .upgrade()
                .ok_or(Failure::RoomGone)?
                .register_participant(room, identity, sid)
                .await
        })
    }
    fn closed<'a>(
        &'a self,
        room: &'a RoomKey,
        identity: &'a Identity,
        sid: &'a ParticipantSid,
    ) -> futures_util::future::BoxFuture<'a, ()> {
        Box::pin(async move {
            if let Some(this) = self.0.upgrade() {
                let mut groups = this.groups.lock().await;
                let key = (room.clone(), identity.clone());
                if groups.get(&key).is_some_and(|(current, _)| current == sid) {
                    if let Some((_, servers)) = groups.remove(&key) {
                        drop(groups);
                        this.retire(servers).await;
                    }
                }
            }
        })
    }
}

#[cfg(test)]
#[path = "cluster_tests.rs"]
mod tests;
