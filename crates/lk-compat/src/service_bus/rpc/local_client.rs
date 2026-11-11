//! Source client/client.go: one response/claim subscription per service/client,
//! and request-ID routing maps. No bus Close on individual RPC completion.
use super::*;
use tokio::sync::mpsc;
struct Routes {
    responses: mpsc::Sender<Vec<u8>>,
    claims: Option<mpsc::Sender<Vec<u8>>>,
}
#[derive(Default)]
struct Pending {
    routes: Mutex<HashMap<String, Routes>>,
}
pub(super) struct LocalClient {
    pending: Arc<Pending>,
    ready: watch::Receiver<Option<Result<(), RpcError>>>,
    task: tokio::sync::Mutex<Option<JoinHandle<()>>>,
    claim_capacity: usize,
}
pub(super) struct LocalCall {
    pending: Arc<Pending>,
    id: String,
    pub(super) responses: mpsc::Receiver<Vec<u8>>,
    pub(super) claims: Option<mpsc::Receiver<Vec<u8>>>,
}
impl Drop for LocalCall {
    fn drop(&mut self) {
        self.pending
            .routes
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .remove(&self.id);
    }
}
impl LocalClient {
    pub(super) fn start(
        bus: RpcBus,
        service: String,
        client_id: String,
        close: Cancellation,
    ) -> Result<Arc<Self>, RpcError> {
        let runtime = tokio::runtime::Handle::try_current().map_err(|_| {
            RpcError::new(
                "internal",
                "local RPC constructor requires an async runtime",
            )
        })?;
        let pending = Arc::new(Pending::default());
        let routes = pending.clone();
        let (ready_tx, ready) = watch::channel(None);
        // Generic client default ChannelSize100; production TypedClient overrides
        // with PSRPC BufferSize (default1000, zero100) through the local bus config.
        let claim_capacity = bus.limits().inbox_messages;
        let task=runtime.spawn(async move {
   let response_ch=channels::response(&service,&client_id);let claim_ch=channels::claim_request(&service,&client_id);
   let mut responses=match bus.subscribe::<pb::Response>(&response_ch,false).await{Ok(sub)=>sub,Err(e)=>{ready_tx.send_replace(Some(Err(e.into())));return}};
   let mut claims=match bus.subscribe::<pb::ClaimRequest>(&claim_ch,false).await{Ok(sub)=>sub,Err(e)=>{let _=responses.unsubscribe(&response_ch).await;ready_tx.send_replace(Some(Err(e.into())));return}};
   ready_tx.send_replace(Some(Ok(())));
   loop {
    tokio::select! {
     _=close.cancelled()=>break,
     bytes=responses.recv()=>{
      let Ok(bytes)=bytes else{close.cancel();break};
      let Ok(decoded)=received_response_for_bus(&bus,&bytes) else{continue};
      let sender=routes.routes.lock().unwrap_or_else(|e|e.into_inner()).get(&decoded.message.request_id).map(|route|route.responses.clone());
      // Source forwarding has its own bounded request channel. It does not
      // hold the map mutex or any transport membership lock through the send.
      if let Some(sender)=sender{let _=sender.send(bytes).await;}
     }
     bytes=claims.recv()=>{
      let Ok(bytes)=bytes else{close.cancel();break};
      let Ok(decoded)=bus.decode::<pb::ClaimRequest>(&bytes) else{continue};
      let sender=routes.routes.lock().unwrap_or_else(|e|e.into_inner()).get(&decoded.message.request_id).and_then(|route|route.claims.clone());
      if let Some(sender)=sender{let _=sender.send(bytes).await;}
     }
    }
   }
   let _=claims.unsubscribe(&claim_ch).await;let _=responses.unsubscribe(&response_ch).await;
  });
        Ok(Arc::new(Self {
            pending,
            ready,
            task: tokio::sync::Mutex::new(Some(task)),
            claim_capacity,
        }))
    }
    pub(super) async fn ready(&self) -> Result<(), RpcError> {
        let mut ready = self.ready.clone();
        loop {
            if let Some(result) = ready.borrow_and_update().clone() {
                return result;
            }
            if ready.changed().await.is_err() {
                return Err(RpcError::closed());
            }
        }
    }
    pub(super) fn request(&self, id: &str, claim: bool) -> LocalCall {
        let (response_tx, responses) = mpsc::channel(1);
        let (claims_tx, claims) = if claim {
            let (tx, rx) = mpsc::channel(self.claim_capacity);
            (Some(tx), Some(rx))
        } else {
            (None, None)
        };
        self.pending
            .routes
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .insert(
                id.to_owned(),
                Routes {
                    responses: response_tx,
                    claims: claims_tx,
                },
            );
        LocalCall {
            pending: self.pending.clone(),
            id: id.to_owned(),
            responses,
            claims,
        }
    }
    // Await the actual task by mutable borrow while retaining ownership under an
    // async mutex. Canceling one waiter never detaches or falsely completes it.
    pub(super) async fn join(&self) {
        let mut owned = self.task.lock().await;
        if let Some(task) = owned.as_mut() {
            let _ = task.await;
        }
        owned.take();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::service_bus::{
        local_transport::LocalTransport,
        redis_transport::TransportLimits,
        wire::{CompressionPolicy, WireLimits},
    };
    #[tokio::test]
    async fn persistent_subscriptions_close_once_at_client_close_and_join() {
        let local = LocalTransport::new_with_gzip_limit(
            TransportLimits {
                wire: WireLimits {
                    encoded_bytes: 1 << 20,
                    decompressed_bytes: 1 << 20,
                },
                compression: CompressionPolicy {
                    quality: 0,
                    threshold: 1,
                },
                inbox_messages: 3,
                subscriptions: 100,
                commands: 100,
                io_timeout: Duration::from_secs(3),
            },
            0,
        )
        .unwrap();
        let bus = RpcBus::from(local.clone());
        let client = RpcClient::new_for_service(local, 4, "ActualLifecycle").unwrap();
        client.ready_for_service("ActualLifecycle").await.unwrap();
        let responses = channels::response("ActualLifecycle", &client.client_id);
        let claims = channels::claim_request("ActualLifecycle", &client.client_id);
        assert_eq!(
            bus.publish(
                &responses,
                &pb::Response {
                    request_id: "not-pending".into(),
                    ..Default::default()
                }
            )
            .await
            .unwrap(),
            1
        );
        assert_eq!(
            bus.publish(
                &claims,
                &pb::ClaimRequest {
                    request_id: "not-pending".into(),
                    ..Default::default()
                }
            )
            .await
            .unwrap(),
            1
        );
        client.close_and_wait().await;
        assert_eq!(
            bus.publish(&responses, &pb::Response::default())
                .await
                .unwrap(),
            0
        );
        assert_eq!(
            bus.publish(&claims, &pb::ClaimRequest::default())
                .await
                .unwrap(),
            0
        );
        assert_eq!(client.pending_count(), 0);
        let request = lk_protocol::livekit::AgentDispatch::default();
        let spec = MethodSpec {
            service: "ActualLifecycle".into(),
            method: "NeverPublished".into(),
            topics: vec![],
            require_claim: true,
            queue: true,
        };
        let error = client
            .unary::<_, lk_protocol::livekit::AgentDispatch>(
                &spec,
                &request,
                CallOptions::new(Duration::from_secs(3)),
            )
            .await
            .unwrap_err();
        assert_eq!(error.origin, RpcErrorOrigin::Closed);
    }
}

#[cfg(test)]
#[path = "constructor_tests.rs"]
mod constructor_tests;
