//! Production Signal/RelaySignal over the fixed PSRPC stream protocol. Room and
//! node ownership remain in RoomControl and NodeRouter respectively.
pub(crate) mod codec;
use crate::{
    service_bus::{
        redis_transport::RedisTransport,
        rpc::{CallOptions, Cancellation, MethodSpec, RpcError},
        stream::{self, ClientStreams, RpcStream, StreamConfig, StreamSender},
    },
    sip::wire::PreservedMessage,
    state::AppState,
};
use lk_protocol::{livekit as pb, rpc};
use prost::Message;
use rtc_core::session::{JoinError, JoinParams, SessionOutput, SignalClaimsSnapshot, SignalEvent};
use std::{
    sync::{Arc, Mutex as StdMutex},
    time::{Duration, Instant},
};
use tokio::{
    sync::{Mutex, RwLock, mpsc, watch},
    task::{JoinHandle, JoinSet},
};

#[derive(Clone, Copy, Debug)]
pub struct RelayConfig {
    pub retry_timeout: Duration,
    pub min_retry_interval: Duration,
    pub max_retry_interval: Duration,
    pub stream_buffer_size: usize,
    pub connect_attempts: usize,
}
impl RelayConfig {
    fn validate(self) -> Result<Self, RpcError> {
        if self.retry_timeout.is_zero()
            || self.min_retry_interval.is_zero()
            || self.max_retry_interval < self.min_retry_interval
            || self.stream_buffer_size == 0
        {
            return Err(RpcError::new(
                "invalid_argument",
                "invalid signal relay configuration",
            ));
        }
        Ok(self)
    }
    fn stream(self, bus: &RedisTransport) -> StreamConfig {
        StreamConfig {
            timeout: Duration::from_secs(3),
            channel_size: self.stream_buffer_size,
            max_streams: bus.limits().commands,
        }
    }
}
fn failure(error: impl std::fmt::Display) -> RpcError {
    RpcError::new("unknown", error.to_string())
}
fn method(node: &str) -> MethodSpec {
    MethodSpec {
        service: "Signal".into(),
        method: "RelaySignal".into(),
        topics: vec![node.into()],
        require_claim: false,
        queue: false,
    }
}

pub enum SignalConnection {
    Local(rtc_runtime::SessionHandle),
    Remote(RemoteSession),
}
pub struct ClusterSignal {
    state: AppState,
    bus: Arc<RedisTransport>,
    config: RelayConfig,
    stop: Cancellation,
    task: Mutex<Option<JoinHandle<()>>>,
    clients: StdMutex<Vec<watch::Receiver<bool>>>,
    admission: RwLock<()>,
    client: Arc<ClientStreams>,
}
impl ClusterSignal {
    pub async fn start(
        state: AppState,
        bus: Arc<RedisTransport>,
        config: RelayConfig,
    ) -> Result<Arc<Self>, RpcError> {
        let config = config.validate()?;
        if state.room_control.is_none() {
            return Err(RpcError::new(
                "failed_precondition",
                "room authority must be installed before relay",
            ));
        }
        let node = state
            .node
            .as_ref()
            .map(|node| node.id())
            .unwrap_or_else(|| state.config.node_id.clone());
        let mut listener = stream::register_stream::<
            rpc::RelaySignalResponse,
            rpc::RelaySignalRequest,
        >(bus.clone(), method(&node), config.stream(&bus), None)
        .await?;
        let client = Arc::new(ClientStreams::new(bus.clone(), "Signal", &node).await?);
        let stop = Cancellation::default();
        let stopping = stop.clone();
        let owner_state = state.clone();
        let task = tokio::spawn(async move {
            let mut sessions = JoinSet::new();
            loop {
                tokio::select! {biased;_=stopping.cancelled()=>break,_=sessions.join_next(),if !sessions.is_empty()=>{},session=listener.accept()=>{
                    let Some(session)=session else{break};let state=owner_state.clone();let stopping=stopping.clone();sessions.spawn(async move{if let Err(error)=run_owner(state,session,config,stopping).await{tracing::warn!(code=%error.code,error=%error.message,"relay signal session ended");}});
                }}
            }
            listener.close().await;
            while sessions.join_next().await.is_some() {}
        });
        Ok(Arc::new(Self {
            state,
            bus,
            config,
            stop,
            task: Mutex::new(Some(task)),
            clients: StdMutex::new(Vec::new()),
            admission: RwLock::new(()),
            client,
        }))
    }
    pub async fn shutdown(&self) {
        self.stop.cancel();
        let _admission = self.admission.write().await;
        if let Some(task) = self.task.lock().await.take() {
            let _ = task.await;
        }
        let clients = std::mem::take(&mut *self.clients.lock().unwrap_or_else(|e| e.into_inner()));
        for mut client in clients {
            while !*client.borrow() {
                if client.changed().await.is_err() {
                    break;
                }
            }
        }
        self.client.close().await;
    }
    pub async fn connect(
        &self,
        params: JoinParams,
        tracks: Vec<pb::AddTrackRequest>,
    ) -> Result<SignalConnection, JoinError> {
        self.connect_attempts(params, tracks, self.config.connect_attempts, false)
            .await
    }
    /// HTTP RTCService owns complete start+first-message attempts; do not nest
    /// the relay's historical stream-setup retry loop under that policy.
    pub(crate) async fn connect_once(
        &self,
        params: JoinParams,
        tracks: Vec<pb::AddTrackRequest>,
    ) -> Result<SignalConnection, JoinError> {
        self.connect_attempts(params, tracks, 1, true).await
    }
    async fn connect_attempts(
        &self,
        params: JoinParams,
        tracks: Vec<pb::AddTrackRequest>,
        attempts: usize,
        admission_validated: bool,
    ) -> Result<SignalConnection, JoinError> {
        let _admission = self.admission.read().await;
        if self.stop.is_cancelled() {
            return Err(JoinError::Internal("signal server stopped".into()));
        }
        let control = self
            .state
            .room_control
            .as_ref()
            .expect("assembled room authority");
        let owner = if admission_validated {
            // HTTP already checked room existence once. Fixed Go retries
            // SelectRoomNode and relay setup, not ValidateCreateRoom.
            control.select_signal_owner(&params.room.name.0).await
        } else {
            control.signal_owner(&params).await
        };
        let owner = owner.map_err(|error| {
            if error.code == "not_found" {
                JoinError::RoomNotFound
            } else {
                JoinError::Internal(error.message)
            }
        })?;
        let node = self
            .state
            .node
            .as_ref()
            .map(|node| node.id())
            .unwrap_or_else(|| self.state.config.node_id.clone());
        if owner == node {
            crate::signal::check_owner_profile_sizes(&self.state, &params)
                .map_err(JoinError::ProfileLimit)?;
            let session_started_at = tokio::time::Instant::now().into_std();
            let prepared = control
                .prepare_signal_room(crate::room_control::signal_create_request(&params))
                .await
                .map_err(|error| JoinError::Internal(error.message));
            if let Err(error) = prepared {
                self.state.manager.record_rtc_init();
                self.state.manager.record_rtc_canceled(1);
                return Err(error);
            }
            return self
                .state
                .manager
                .join_started(params, session_started_at)
                .await
                .map(SignalConnection::Local);
        }
        let start =
            codec::start_session(&params, tracks).map_err(|e| JoinError::Internal(e.message))?;
        let mut error = None;
        for _ in 0..attempts {
            if self.stop.is_cancelled() {
                break;
            }
            let mut options = CallOptions::new(Duration::from_secs(3));
            options.cancel = self.stop.clone();
            options.client = Some(self.client.clone());
            match stream::open_stream::<rpc::RelaySignalRequest, rpc::RelaySignalResponse>(
                self.bus.clone(),
                &method(&owner),
                self.config.stream(&self.bus),
                options,
            )
            .await
            {
                Ok(mut relay) => {
                    let first = rpc::RelaySignalRequest {
                        start_session: Some(start.clone()),
                        ..Default::default()
                    };
                    if let Err(failure) = relay.send(&first).await {
                        let _ = relay.close(Some(failure.clone())).await;
                        error = Some(failure);
                        continue;
                    }
                    let remote = remote_session(relay, self.config, self.stop.clone());
                    let mut clients = self.clients.lock().unwrap_or_else(|e| e.into_inner());
                    clients.retain(|client| !*client.borrow());
                    clients.push(remote.complete.clone());
                    return Ok(SignalConnection::Remote(remote));
                }
                Err(failure) => error = Some(failure),
            }
        }
        Err(JoinError::Internal(
            error
                .map(|e| e.message)
                .unwrap_or_else(|| "could not connect to signal server".into()),
        ))
    }
}
impl Drop for ClusterSignal {
    fn drop(&mut self) {
        self.stop.cancel();
        self.client.cancel();
    }
}

pub struct RemoteSession {
    outgoing: Option<mpsc::Sender<Vec<u8>>>,
    pub incoming: mpsc::Receiver<Vec<u8>>,
    stop: Cancellation,
    task: Option<JoinHandle<()>>,
    complete: watch::Receiver<bool>,
}
impl RemoteSession {
    pub async fn send(&self, raw: Vec<u8>) -> bool {
        match &self.outgoing {
            Some(sender) => sender.send(raw).await.is_ok(),
            None => false,
        }
    }
    /// Start draining the actual relay request queue. Dropping only the sender
    /// lets its writer forward queued requests, then its terminal Close batch.
    pub(crate) fn begin_request_close(&mut self) {
        self.outgoing.take();
    }

    pub async fn close(&mut self) {
        self.incoming.close();
        self.begin_request_close();
        if let Some(task) = self.task.take() {
            let _ = task.await;
        }
    }
}
impl Drop for RemoteSession {
    fn drop(&mut self) {
        self.stop.cancel();
    }
}

async fn write_batches<S: Message + Default + prost::Name>(
    sender: StreamSender<S>,
    mut input: mpsc::Receiver<Vec<u8>>,
    config: RelayConfig,
    stop: Cancellation,
    response: bool,
) -> Result<(), RpcError> {
    let mut seq = 0u64;
    'batches: loop {
        if let Some(error) = sender.error() {
            return Err(error);
        }
        let first =
            tokio::select! {biased;_=stop.cancelled()=>return Ok(()),first=input.recv()=>first};
        let mut batch = first.into_iter().collect::<Vec<_>>();
        while let Ok(raw) = input.try_recv() {
            batch.push(raw);
        }
        let closing = input.is_closed() && input.is_empty();
        let raw = if response {
            codec::response_batch(seq, closing, &batch)?
        } else {
            codec::request_batch(seq, closing, &batch)?
        };
        let deadline = Instant::now() + config.retry_timeout;
        let mut interval = config.min_retry_interval;
        loop {
            // Fixed signal.go317 checks Stream.Err on every retry. Without
            // this gate an already-closed stream returns immediately forever
            // until RetryTimeout, monopolizing a Tokio worker and starving IO.
            if let Some(error) = sender.error() {
                return Err(error);
            }
            let result = tokio::select! {biased;_=stop.cancelled()=>return Ok(()),result=sender.send_raw(&raw,interval)=>result};
            match result {
                Ok(()) => break,
                Err(error) => {
                    if Instant::now() > deadline {
                        if !response {
                            return Err(error);
                        }
                        seq = seq.saturating_add(batch.len() as u64);
                        while input.try_recv().is_ok() {
                            seq = seq.saturating_add(1);
                        }
                        if input.is_closed() {
                            return Ok(());
                        }
                        tracing::warn!(error=%error,"could not send signal message");
                        continue 'batches;
                    }
                    interval = interval.saturating_mul(2).min(config.max_retry_interval);
                    // Go goroutines are preemptible even when a live transport
                    // fails synchronously. Preserve its timeout/deadline policy
                    // without inventing retry sleeps or blocking other tasks.
                    tokio::task::yield_now().await;
                }
            }
        }
        seq = seq.saturating_add(batch.len() as u64);
        if closing {
            return Ok(());
        }
    }
}
fn remote_session(
    mut relay: RpcStream<rpc::RelaySignalRequest, rpc::RelaySignalResponse>,
    config: RelayConfig,
    parent: Cancellation,
) -> RemoteSession {
    let (outgoing, requests) = mpsc::channel(config.stream_buffer_size);
    let (responses, incoming) = mpsc::channel(config.stream_buffer_size);
    let stop = Cancellation::default();
    let stopping = stop.clone();
    let (complete, completion) = watch::channel(false);
    let task = tokio::spawn(async move {
        let _completion = Completion(complete);
        let sending = relay.sender();
        let cancel = stopping.clone();
        let mut writer = tokio::spawn(write_batches(sending, requests, config, cancel, false));
        let mut expected = 0u64;
        let mut writer_joined = false;
        let mut result = loop {
            tokio::select! {biased;_=parent.cancelled()=>break Ok(()),_=stopping.cancelled()=>break Ok(()),result=&mut writer=>{writer_joined=true;break result.map_err(failure).and_then(|r|r)},value=relay.recv()=>{
                let Some(value)=value else{break Ok(())};
                let raw=match codec::read_batch("rpc.RelaySignalResponse","responses",&value.raw,value.message.seq,&mut expected){Ok(raw)=>raw,Err(error)=>break Err(error)};
                for response in raw{tokio::select!{biased;_=stopping.cancelled()=>break, _=parent.cancelled()=>break,_=responses.send(response)=>{}}}
                if value.message.close{break Ok(());}
            }}
        };
        stopping.cancel();
        if !writer_joined {
            let completed = (&mut writer)
                .await
                .map_err(failure)
                .and_then(|result| result);
            if result.is_ok() {
                result = completed;
            }
        }
        if let Err(error) = &result {
            tracing::warn!(error=%error,"remote signal pump ended");
        }
        let _ = relay.close(result.err()).await;
    });
    RemoteSession {
        outgoing: Some(outgoing),
        incoming,
        stop,
        task: Some(task),
        complete: completion,
    }
}
struct Completion(watch::Sender<bool>);
impl Drop for Completion {
    fn drop(&mut self) {
        self.0.send_replace(true);
    }
}

async fn run_owner(
    state: AppState,
    mut relay: RpcStream<rpc::RelaySignalResponse, rpc::RelaySignalRequest>,
    config: RelayConfig,
    stop: Cancellation,
) -> Result<(), RpcError> {
    let first = tokio::select! {biased;_=stop.cancelled()=>return Ok(()),value=tokio::time::timeout(config.retry_timeout,relay.recv())=>value.map_err(|_|RpcError::new("deadline_exceeded","timeout waiting for start session"))?.ok_or_else(||RpcError::new("unavailable","stream closed before start session"))?};
    let start = first
        .message
        .start_session
        .ok_or_else(|| RpcError::new("invalid_argument", "expected start session message"))?;
    let params = codec::join_params(
        state.keys.first().map(|(key, _)| key).unwrap_or_default(),
        &start,
    )?;
    if let Err(message) = crate::signal::check_owner_profile_sizes(&state, &params) {
        // Fixed RelaySignal closes its application sink on a StartSession
        // rejection. Retire the relay after the empty final response, so a
        // fast rejection cannot race the ingress's StartSession ACK as a
        // transport-level setup failure.
        let (responses, pending) = mpsc::channel(1);
        drop(responses);
        let _ = write_batches(relay.sender(), pending, config, stop, true).await;
        let _ = relay.close(None).await;
        return Err(RpcError::new("invalid_argument", message));
    }
    let session_started_at = tokio::time::Instant::now().into_std();
    let prepared =
        state
            .room_control
            .as_ref()
            .ok_or_else(|| RpcError::new("failed_precondition", "room authority missing"))?
            .prepare_signal_room(start.create_room.clone().unwrap_or_else(|| {
                pb::CreateRoomRequest {
                    name: start.room_name.clone(),
                    ..Default::default()
                }
            }))
            .await;
    if let Err(error) = prepared {
        state.manager.record_rtc_init();
        state.manager.record_rtc_canceled(1);
        return Err(error);
    }
    let mut cache = crate::signal::encode::RequestCache::default();
    for (index, request) in start.add_track_requests.iter().enumerate() {
        cache.store_with(
            index as u32 + 1,
            pb::request_response::Request::AddTrack(request.clone()),
        );
    }
    let mut context = crate::signal::session::SessionCtx {
        state: state.clone(),
        grants: params.grants.clone(),
        client: params.client.clone(),
        cache,
    };
    let mut handle = match state.manager.join_started(params, session_started_at).await {
        Ok(handle) => handle,
        Err(JoinError::ReconnectStateMismatch) => {
            let event = SignalEvent::Leave {
                reason: rtc_core::model::DisconnectReason::StateMismatch,
                action: rtc_core::session::LeaveAction::Reconnect,
            };
            if let Some(raw) =
                context.compose_wire(event, &rtc_core::ids::ParticipantSid::default())
            {
                let raw = codec::response_batch(0, true, &[raw])?;
                let _ = relay.send_raw(&raw, config.retry_timeout).await;
            }
            let _ = relay.close(None).await;
            return Ok(());
        }
        Err(error) => return Err(failure(error)),
    };
    let sid = handle.participant_sid.clone();
    let identity = handle.identity.clone();
    let admission =
        state
            .routing_events
            .wait_participant_admitted(&start.room_name, &identity.0, &sid.0);
    tokio::pin!(admission);
    let mut input_admitted = false;
    let mut pending_input = None;
    // Hold only the initial Join until its own real topic/store attempt.
    // Stream closure remains observable even with a queued early request.
    let mut pending_initial_join = None::<SignalEvent>;
    let relay_closed = relay.closed_signal();
    let (responses, input) = mpsc::channel(config.stream_buffer_size);
    let writer_stop = Cancellation::default();
    let mut writer = tokio::spawn(write_batches(
        relay.sender(),
        input,
        config,
        writer_stop.clone(),
        true,
    ));
    let mut expected = 0u64;
    let mut last_claims: Option<SignalClaimsSnapshot> = None;
    let mut refresh = tokio::time::interval(Duration::from_secs(5 * 60));
    refresh.tick().await;
    let mut writer_joined = false;
    let mut result:Result<(),RpcError> = async { 'reactor: loop {
        tokio::select! {biased;_=stop.cancelled()=>break Ok(()),result=&mut writer=>{writer_joined=true;break result.map_err(failure).and_then(|r|r)},_=relay_closed.cancelled(),if pending_initial_join.is_some()=>break Ok(()),output=handle.output.recv(),if pending_initial_join.is_none()=>{
            match output{Some(SessionOutput::Signal(event))=>{
                if !input_admitted && matches!(&event, SignalEvent::Join(_)) {
                    pending_initial_join = Some(event);
                    continue;
                }
                if let SignalEvent::ClaimsChanged(info)=&event{last_claims=Some((**info).clone());}
                if let Some(raw)=context.compose_wire(event,&sid){tokio::select!{biased;_=stop.cancelled()=>break Ok(()),result=responses.send(raw)=>if result.is_err(){break Ok(())}}}
            },Some(SessionOutput::Close)|None=>break Ok(())}
        },admitted=&mut admission,if !input_admitted=>{
            admitted?;
            input_admitted=true;
            if let Some(event) = pending_initial_join.take()
                && let Some(raw) = context.compose_wire(event, &sid)
            {
                tokio::select! {biased;
                    _=stop.cancelled()=>break Ok(()),
                    result=responses.send(raw)=>if result.is_err(){break Ok(())}
                }
            }
        },value=async { if input_admitted && pending_input.is_some() { pending_input.take() } else { relay.recv().await } },if input_admitted || pending_input.is_none()=>{
            let Some(value)=value else{break Ok(())};
            if !input_admitted { pending_input=Some(value);continue; }
            let requests=match codec::read_batch("rpc.RelaySignalRequest","requests",&value.raw,value.message.seq,&mut expected){Ok(requests)=>requests,Err(error)=>break Err(error)};
            for raw in requests{
                let request:pb::SignalRequest=lk_protocol::wire::decode(&raw).map_err(failure)?;
                let inbound=if matches!(&request.message,Some(pb::signal_request::Message::StoreDataBlobRequest(_)|pb::signal_request::Message::GetDataBlobRequest(_))){crate::signal::blob::decode_request(&raw).map_err(failure)?}else{match request.message.as_ref(){
                    Some(pb::signal_request::Message::Ping(_))=>crate::signal::encode::Inbound::Forward(rtc_core::session::SessionRequest::SignalActivity),
                    Some(pb::signal_request::Message::PingReq(ping))=>if ping.rtt>0{crate::signal::encode::Inbound::Forward(rtc_core::session::SessionRequest::SignalRtt(ping.rtt as u32))}else{crate::signal::encode::Inbound::Forward(rtc_core::session::SessionRequest::SignalActivity)},
                    _=>crate::signal::encode::decode_request(request,&mut context.cache),
                }};
                use crate::signal::encode::Inbound;
                match inbound{
                    Inbound::Forward(request)=>{if !tokio::select!{biased;_=stop.cancelled()=>false,result=handle.send(request)=>result}{break 'reactor Ok(());}},
                    Inbound::Reply(response)=>{tokio::select!{biased;_=stop.cancelled()=>break 'reactor Ok(()),_=responses.send(response.encode_to_vec())=>{}}},
                    Inbound::ForwardAndReply(request,response)=>{tokio::select!{biased;_=stop.cancelled()=>break 'reactor Ok(()),_=responses.send(response.encode_to_vec())=>{}};if !tokio::select!{biased;_=stop.cancelled()=>false,result=handle.send(*request)=>result}{break 'reactor Ok(());}},Inbound::Ignore=>{}
                }
            }
            if value.message.close{break Ok(());}
        },_=refresh.tick()=>{
            if let Some(info)=last_claims.clone()&&let Some(token)=context.refresh_token(&info){let response=pb::SignalResponse{message:Some(pb::signal_response::Message::RefreshToken(token))};tokio::select!{biased;_=stop.cancelled()=>break Ok(()),_=responses.send(response.encode_to_vec())=>{}};}
        }}
    }}.await;
    drop(handle);
    drop(responses);
    if stop.is_cancelled() || result.is_err() {
        writer_stop.cancel();
    }
    if !writer_joined {
        let completed = (&mut writer)
            .await
            .map_err(failure)
            .and_then(|result| result);
        if result.is_ok() {
            result = completed;
        }
    }
    let _ = relay.close(result.clone().err()).await;
    result
}

#[cfg(test)]
mod tests;

#[cfg(test)]
#[path = "writer_lifecycle_tests.rs"]
mod writer_lifecycle_tests;

#[cfg(test)]
mod signal_activity_tests;
