//! Isolated concrete real-room WHIP session owner; no fabricated SDP or ICE reply.
//! Requires the explicit one-shot core/media integration in integration.json before activation.
use super::fragment::{Document, Fragment};
use rtc_core::{
    ids::{Identity, ParticipantSid, RoomKey},
    model::DisconnectReason,
    session::{
        CandidateInit, JoinParams, SessionOutput, SessionRequest, SignalEvent, SignalTarget,
    },
};
use rtc_runtime::{RoomManager, SessionHandle};
use std::{
    collections::{BTreeMap, HashMap},
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Instant,
};
use tokio::sync::{Mutex, Notify, mpsc, oneshot, watch};
#[derive(Clone, Debug)]
pub struct Answer {
    pub sid: ParticipantSid,
    pub sdp: String,
    pub ice_session_id: String,
}
#[derive(Clone, Debug)]
pub enum Failure {
    NotFound,
    RoomNotFound,
    RoomGone,
    InvalidFragment,
    MidMismatch,
    CredentialMismatch,
    IceSessionMismatch { expected: String, got: String },
    NoResponse,
    Deadline,
    Internal(String),
    Remote(crate::service_bus::rpc::RpcError),
}
#[derive(Clone)]
struct Resource {
    room: RoomKey,
    identity: Identity,
    sid: ParticipantSid,
    control: mpsc::Sender<Control>,
    done: watch::Receiver<bool>,
}
enum Control {
    Trickle {
        tag: String,
        fragment: String,
        deadline: Instant,
        reply: oneshot::Sender<Result<(), Failure>>,
    },
    Restart {
        fragment: String,
        deadline: Instant,
        reply: oneshot::Sender<Result<Answer, Failure>>,
    },
    Delete {
        deadline: Instant,
        reply: oneshot::Sender<Result<(), Failure>>,
    },
}
pub(super) trait ParticipantTopics: Send + Sync {
    fn opened<'a>(
        &'a self,
        room: &'a RoomKey,
        identity: &'a Identity,
        sid: &'a ParticipantSid,
    ) -> futures_util::future::BoxFuture<'a, Result<(), Failure>>;
    fn closed<'a>(
        &'a self,
        room: &'a RoomKey,
        identity: &'a Identity,
        sid: &'a ParticipantSid,
    ) -> futures_util::future::BoxFuture<'a, ()>;
}
#[derive(Clone)]
pub struct WhipSessions {
    manager: RoomManager,
    resources: Arc<Mutex<HashMap<ParticipantSid, Resource>>>,
    topics_changed: Arc<Notify>,
    lifecycle: Arc<std::sync::RwLock<Option<Arc<dyn ParticipantTopics>>>>,
    admission: Arc<Mutex<()>>,
    closed: Arc<AtomicBool>,
    tasks: Arc<Mutex<tokio::task::JoinSet<()>>>,
}
impl WhipSessions {
    pub fn new(manager: RoomManager) -> Self {
        Self {
            manager,
            resources: Arc::new(Mutex::new(HashMap::new())),
            topics_changed: Arc::new(Notify::new()),
            lifecycle: Arc::new(std::sync::RwLock::new(None)),
            admission: Arc::new(Mutex::new(())),
            closed: Arc::new(AtomicBool::new(false)),
            tasks: Arc::new(Mutex::new(tokio::task::JoinSet::new())),
        }
    }
    pub(super) fn set_topics(&self, topics: Arc<dyn ParticipantTopics>) {
        *self.lifecycle.write().expect("WHIP topics lock") = Some(topics);
    }
    /// Caller has authenticated, applied room preset/config and parsed offer + synthesized tracks.
    /// Deadline comes from the actual source-mapped RPC request policy, not an invented lease.
    pub async fn create(
        &self,
        params: JoinParams,
        offer: String,
        targets: Vec<rtc_core::session::NamedSubscription>,
        deadline: Instant,
    ) -> Result<Answer, Failure> {
        // RPC/HTTP cancellation cannot drop a half-created real participant.
        // Setup and the resulting media actor stay in the service-owned task set.
        let admission = self.admission.lock().await;
        if self.closed.load(Ordering::Acquire) {
            return Err(Failure::RoomGone);
        }
        let (reply, result) = oneshot::channel();
        let owned = self.clone();
        self.tasks.lock().await.spawn(async move {
            let value = owned.create_inner(params, offer, targets, deadline).await;
            let _ = reply.send(value);
        });
        drop(admission);
        result.await.map_err(|_| Failure::RoomGone)?
    }
    async fn create_inner(
        &self,
        mut params: JoinParams,
        offer: String,
        targets: Vec<rtc_core::session::NamedSubscription>,
        deadline: Instant,
    ) -> Result<Answer, Failure> {
        let _admission = self.admission.lock().await;
        if self.closed.load(Ordering::Acquire) {
            return Err(Failure::RoomGone);
        }
        params.capabilities.one_shot = true;
        params.single_pc = true;
        params.disable_ice_lite = true;
        params.auto_subscribe = true;
        params.adaptive_stream = false;
        params.publisher_offer = None;
        let room = params.room.clone();
        let identity = params.identity.clone();
        let session = tokio::time::timeout_at(deadline.into(), self.manager.join(params))
            .await
            .map_err(|_| Failure::Deadline)?
            .map_err(|e| Failure::Internal(e.to_string()))?;
        let sid = session.participant_sid.clone();
        let (control, mut rx) = mpsc::channel(32);
        let (done_tx, done) = watch::channel(false);
        let resource = Resource {
            room: room.clone(),
            identity: identity.clone(),
            sid: sid.clone(),
            control,
            done,
        };
        let lifecycle = self.lifecycle.read().expect("WHIP topics lock").clone();
        self.resources.lock().await.insert(sid.clone(), resource);
        if let Some(topics) = &lifecycle {
            if let Err(error) = topics.opened(&room, &identity, &sid).await {
                self.resources.lock().await.remove(&sid);
                // Registration failure must release the actual participant too.
                session
                    .apply(SessionRequest::Leave {
                        reason: DisconnectReason::ServerShutdown,
                    })
                    .await;
                return Err(error);
            }
        }
        self.topics_changed.notify_waiters();
        let weak = Arc::downgrade(&self.resources);
        let owned_sid = sid.clone();
        let topics_changed = self.topics_changed.clone();
        let (reply, initial) = oneshot::channel();
        let mut tasks = self.tasks.lock().await;
        while tasks.try_join_next().is_some() {}
        tasks.spawn(async move {
            let mut owner = Owner {
                session,
                remote: Document::from_validated(&offer),
                local: None,
                next_id: 1,
                pending_remote: BTreeMap::new(),
            };
            let prepared = owner
                .session
                .apply(SessionRequest::PrepareOneShotOffer { sdp: offer, id: 0 });
            let result = match tokio::time::timeout_at(deadline.into(), prepared).await {
                Ok(true) => owner.wait_and_finalize(targets, 0, deadline, &mut rx).await,
                Ok(false) => Err(Failure::RoomGone),
                Err(_) => Err(Failure::Deadline),
            };
            match result {
                Ok(answer) => {
                    let _ = reply.send(Ok(answer));
                    owner.run(rx).await;
                }
                Err(error) => {
                    let _ = reply.send(Err(error));
                    let cleanup = Instant::now() + std::time::Duration::from_secs(3);
                    let _ = owner.delete_applied(cleanup).await;
                }
            }
            if let Some(topics) = lifecycle {
                topics.closed(&room, &identity, &owned_sid).await;
            }
            done_tx.send_replace(true);
            // A unique real participant SID is the generation key; never identity-only cleanup.
            if let Some(resources) = weak.upgrade() {
                resources.lock().await.remove(&owned_sid);
                topics_changed.notify_waiters();
            }
        });
        drop(tasks);
        drop(_admission);
        initial.await.map_err(|_| Failure::RoomGone)?
    }
    /// Owner closure is driven by actual room/transport state, not HTTP call completion.
    pub async fn done(&self, sid: &ParticipantSid) -> watch::Receiver<bool> {
        self.resources
            .lock()
            .await
            .get(sid)
            .map(|r| r.done.clone())
            .unwrap_or_else(|| watch::channel(true).1)
    }
    pub async fn shutdown(&self) {
        let _admission = self.admission.lock().await;
        self.closed.store(true, Ordering::Release);
        self.resources.lock().await.clear();
        self.topics_changed.notify_waiters();
        drop(_admission);
        let mut tasks = self.tasks.lock().await;
        while tasks.join_next().await.is_some() {}
    }
    /// Local equivalent of the registered WHIP participant RPC topic. The
    /// source registers it only for one-shot sessions and keys it by room and
    /// identity. A missing SID is not proof that the topic itself has no owner.
    async fn topic(
        &self,
        room: &RoomKey,
        identity: &Identity,
        deadline: Instant,
    ) -> Result<Resource, Failure> {
        loop {
            let changed = self.topics_changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            let candidates: Vec<_> = self
                .resources
                .lock()
                .await
                .values()
                .filter(|r| {
                    r.room == *room
                        && r.identity == *identity
                        && !r.control.is_closed()
                        && !*r.done.borrow()
                })
                .cloned()
                .collect();
            if !candidates.is_empty() {
                if !self.manager.room_exists(room) {
                    return Err(Failure::RoomNotFound);
                }
                let current = tokio::time::timeout_at(
                    deadline.into(),
                    self.manager.get_participant(room, identity),
                )
                .await
                .map_err(|_| Failure::Deadline)?
                .map_err(|_| Failure::NotFound)?;
                return candidates
                    .into_iter()
                    .find(|r| r.sid == current.sid)
                    .ok_or(Failure::NotFound);
            }
            tokio::select! {
                _ = changed => {},
                _ = tokio::time::sleep_until(deadline.into()) => return Err(Failure::NoResponse),
            }
        }
    }
    async fn resource(
        &self,
        room: &RoomKey,
        identity: &Identity,
        sid: &ParticipantSid,
        deadline: Instant,
    ) -> Result<Resource, Failure> {
        let resource = self.topic(room, identity, deadline).await?;
        if resource.sid != *sid {
            return Err(Failure::NotFound);
        }
        Ok(resource)
    }
    pub async fn trickle(
        &self,
        room: &RoomKey,
        identity: &Identity,
        sid: &ParticipantSid,
        tag: String,
        fragment: String,
        deadline: Instant,
    ) -> Result<(), Failure> {
        let r = self.resource(room, identity, sid, deadline).await?;
        let (tx, rx) = oneshot::channel();
        tokio::time::timeout_at(
            deadline.into(),
            r.control.send(Control::Trickle {
                tag,
                fragment,
                deadline,
                reply: tx,
            }),
        )
        .await
        .map_err(|_| Failure::Deadline)?
        .map_err(|_| Failure::RoomGone)?;
        tokio::time::timeout_at(deadline.into(), rx)
            .await
            .map_err(|_| Failure::Deadline)?
            .map_err(|_| Failure::RoomGone)?
    }
    pub async fn restart(
        &self,
        room: &RoomKey,
        identity: &Identity,
        sid: &ParticipantSid,
        fragment: String,
        deadline: Instant,
    ) -> Result<Answer, Failure> {
        let r = self.resource(room, identity, sid, deadline).await?;
        let (tx, rx) = oneshot::channel();
        tokio::time::timeout_at(
            deadline.into(),
            r.control.send(Control::Restart {
                fragment,
                deadline,
                reply: tx,
            }),
        )
        .await
        .map_err(|_| Failure::Deadline)?
        .map_err(|_| Failure::RoomGone)?;
        tokio::time::timeout_at(deadline.into(), rx)
            .await
            .map_err(|_| Failure::Deadline)?
            .map_err(|_| Failure::RoomGone)?
    }
    pub async fn delete(
        &self,
        room: &RoomKey,
        identity: &Identity,
        sid: &ParticipantSid,
        deadline: Instant,
    ) -> Result<(), Failure> {
        // Only an actual registered identity topic can run DeleteSession.
        // Once reached, source accepts a stale/mismatched SID as a no-op.
        let r = self.topic(room, identity, deadline).await?;
        if r.sid != *sid {
            return Ok(());
        }
        let (tx, rx) = oneshot::channel();
        tokio::time::timeout_at(
            deadline.into(),
            r.control.send(Control::Delete {
                deadline,
                reply: tx,
            }),
        )
        .await
        .map_err(|_| Failure::Deadline)?
        .map_err(|_| Failure::RoomGone)?;
        tokio::time::timeout_at(deadline.into(), rx)
            .await
            .map_err(|_| Failure::Deadline)?
            .map_err(|_| Failure::RoomGone)?
    }
}
struct Owner {
    session: SessionHandle,
    remote: Document,
    local: Option<Answer>,
    next_id: u32,
    pending_remote: BTreeMap<u32, Document>,
}
impl Owner {
    async fn wait_and_finalize(
        &mut self,
        targets: Vec<rtc_core::session::NamedSubscription>,
        id: u32,
        deadline: Instant,
        rx: &mut mpsc::Receiver<Control>,
    ) -> Result<Answer, Failure> {
        let mut poll = tokio::time::interval(std::time::Duration::from_millis(50));
        loop {
            tokio::select! {
             _=tokio::time::sleep_until(deadline.into())=>return Err(Failure::Deadline),
             command=rx.recv()=>match command {
              Some(Control::Delete{deadline,reply})=>{
               let result=self.delete_applied(deadline).await;let _=reply.send(result);return Err(Failure::RoomGone);
              }
              Some(Control::Trickle{reply,..})=>{let _=reply.send(Err(Failure::Internal("no local description".into())));}
              Some(Control::Restart{reply,..})=>{let _=reply.send(Err(Failure::Internal("no local description".into())));}
              None=>return Err(Failure::RoomGone),
             },
             event=self.session.output.recv()=>match event {
              Some(SessionOutput::Signal(SignalEvent::RequestResponse{request_id,outcome:rtc_core::session::RequestOutcome::UnclassifiedError,message,..})) if request_id==id=>return Err(Failure::Internal(message)),
              None|Some(SessionOutput::Close)=>return Err(Failure::RoomGone),
              _=>{}
             },
             _=poll.tick()=>{
              let ready=tokio::time::timeout_at(deadline.into(),self.session.named_subscriptions_prepared(targets.clone())).await.map_err(|_|Failure::Deadline)?;
              if ready { break; }
             }
            }
        }
        if !tokio::time::timeout_at(
            deadline.into(),
            self.session
                .apply(SessionRequest::FinalizeOneShotAnswer { id }),
        )
        .await
        .map_err(|_| Failure::Deadline)?
        {
            return Err(Failure::RoomGone);
        }
        self.answer(id, deadline).await
    }
    async fn answer(&mut self, id: u32, deadline: Instant) -> Result<Answer, Failure> {
        loop {
            let event = tokio::time::timeout_at(deadline.into(), self.session.output.recv())
                .await
                .map_err(|_| Failure::Deadline)?
                .ok_or(Failure::RoomGone)?;
            match event {
                SessionOutput::Signal(SignalEvent::RequestResponse {
                    request_id,
                    outcome: rtc_core::session::RequestOutcome::UnclassifiedError,
                    message,
                    ..
                }) if request_id == id => {
                    self.pending_remote.remove(&id);
                    return Err(Failure::Internal(message));
                }
                SessionOutput::Signal(SignalEvent::Answer { sdp, id: got, .. }) if id == got => {
                    if let Some(accepted) = self.pending_remote.remove(&got) {
                        self.remote = accepted;
                    }
                    let fragment = Document::from_validated(&sdp)
                        .extract_fragment()
                        .map_err(|e| Failure::Internal(format!("local SDP fragment: {e:?}")))?;
                    let (ufrag, _) = fragment
                        .credentials()
                        .map_err(|e| Failure::Internal(format!("local ICE credentials: {e:?}")))?;
                    let answer = Answer {
                        sid: self.session.participant_sid.clone(),
                        sdp,
                        ice_session_id: ufrag.to_owned(),
                    };
                    self.local = Some(answer.clone());
                    return Ok(answer);
                }
                SessionOutput::Close => return Err(Failure::RoomGone),
                _ => {} // Real room signals are continuously drained, never queued as fake HTTP answers.
            }
        }
    }
    async fn run(&mut self, mut rx: mpsc::Receiver<Control>) {
        loop {
            tokio::select! {
             event=self.session.output.recv()=>{match event {
              None|Some(SessionOutput::Close)=>break,
              Some(SessionOutput::Signal(SignalEvent::RequestResponse{request_id,outcome:rtc_core::session::RequestOutcome::UnclassifiedError,..}))=>{self.pending_remote.remove(&request_id);},
              Some(SessionOutput::Signal(SignalEvent::Answer{sdp,id,..}))=>{
               if let Some(accepted)=self.pending_remote.remove(&id){
                self.remote=accepted;
                if let Ok(fragment) = Document::from_validated(&sdp).extract_fragment()
                    && let Ok((ufrag, _)) = fragment.credentials()
                {
                    self.local = Some(Answer {
                        sid: self.session.participant_sid.clone(),
                        sdp,
                        ice_session_id: ufrag.to_owned(),
                    });
                }
               }
              },_=>{}
             }},
             command=rx.recv()=>{let Some(command)=command else{break;};match command{
              Control::Trickle{tag,fragment,deadline,reply}=>{let result=self.trickle(&tag,&fragment,deadline).await;let _=reply.send(result);},
              Control::Restart{fragment,deadline,reply}=>{let result=self.restart(&fragment,deadline).await;let _=reply.send(result);},
              Control::Delete{deadline,reply}=>{
               // SessionHandle sends sid+generation through RoomInput, so concurrent identity replacement is safe.
               let result=self.delete_applied(deadline).await;
               let _=reply.send(result);break;
              }
             }}
            }
        }
        // Real room owns actual media/UDP/DTLS teardown, never an HTTP fake resource.
    }
    async fn delete_applied(&self, deadline: Instant) -> Result<(), Failure> {
        // Missing/stale generation within an existing room is source-successful no-op.
        let _current = tokio::time::timeout_at(
            deadline.into(),
            self.session.apply(SessionRequest::Leave {
                reason: DisconnectReason::ClientInitiated,
            }),
        )
        .await
        .map_err(|_| Failure::Deadline)?;
        Ok(())
    }
    async fn trickle(&mut self, tag: &str, text: &str, deadline: Instant) -> Result<(), Failure> {
        let current = self.local.as_ref().ok_or(Failure::RoomGone)?;
        if current.ice_session_id != tag {
            return Err(Failure::IceSessionMismatch {
                expected: current.ice_session_id.clone(),
                got: tag.to_owned(),
            });
        }
        let fragment = Fragment::parse(text).map_err(|_| Failure::InvalidFragment)?;
        let remote = self
            .remote
            .extract_fragment()
            .map_err(|e| Failure::Internal(format!("remote SDP: {e:?}")))?;
        if fragment.media.mid != remote.media.mid {
            return Err(Failure::MidMismatch);
        }
        let (u, p) = fragment
            .credentials()
            .map_err(|_| Failure::InvalidFragment)?;
        let (ru, rp) = remote
            .credentials()
            .map_err(|e| Failure::Internal(format!("remote ICE: {e:?}")))?;
        if (!u.is_empty() && u != ru) || (!p.is_empty() && p != rp) {
            return Err(Failure::CredentialMismatch);
        }
        for candidate in fragment.media.candidates {
            let request = SessionRequest::Trickle {
                token: 0,
                candidate: CandidateInit {
                    candidate: format!("candidate:{candidate}"),
                    sdp_mid: Some(fragment.media.mid.clone()),
                    ..Default::default()
                },
                target: SignalTarget::Publisher,
                last: false,
            };
            if !tokio::time::timeout_at(deadline.into(), self.session.apply(request))
                .await
                .map_err(|_| Failure::Deadline)?
            {
                return Err(Failure::RoomGone);
            }
        }
        Ok(())
    }
    async fn restart(&mut self, text: &str, deadline: Instant) -> Result<Answer, Failure> {
        let fragment = Fragment::parse(text).map_err(|_| Failure::InvalidFragment)?;
        let mut updated = self.remote.clone();
        fragment
            .patch(&mut updated)
            .map_err(|e| Failure::Internal(e.to_string()))?;
        let id = self.next_id;
        self.next_id = self
            .next_id
            .checked_add(1)
            .ok_or_else(|| Failure::Internal("SDP transaction id exhausted".into()))?;
        self.pending_remote.insert(id, updated.clone());
        if !tokio::time::timeout_at(
            deadline.into(),
            self.session.apply(SessionRequest::Offer {
                sdp: updated.marshal(),
                id,
            }),
        )
        .await
        .map_err(|_| Failure::Deadline)?
        {
            return Err(Failure::RoomGone);
        }
        let answer = self.answer(id, deadline).await?;
        Ok(answer)
    }
}

#[cfg(test)]
mod topic_tests {
    use super::*;
    #[tokio::test]
    async fn absent_topic_waits_for_original_deadline_instead_of_inventing_room_error() {
        let (state, net) = crate::test_support::state().await;
        let sessions = WhipSessions::new(state.manager.clone());
        for create_room in [false, true] {
            let room = state.room_key("topic-owner");
            if create_room {
                state
                    .manager
                    .create_room(room.clone(), Default::default())
                    .await
                    .unwrap();
            }
            let start = Instant::now();
            let deadline = start + std::time::Duration::from_millis(20);
            assert!(matches!(
                sessions
                    .delete(
                        &room,
                        &Identity("absent".into()),
                        &ParticipantSid("PA_missing".into()),
                        deadline
                    )
                    .await,
                Err(Failure::NoResponse)
            ));
            assert!(
                Instant::now() >= deadline,
                "no-response preserves the configured caller deadline"
            );
        }
        state.manager.close_all().await;
        state.agents.shutdown().await;
        state.shutdown_whip().await;
        net.shutdown();
    }
}
