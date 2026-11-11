//! Fixed PSRPC RequestMulti: publish once, retain each independently successful
//! or failed reply until the collection window closes. There is no success
//! fabricated from the number of subscribers and no first-reply short circuit.
use super::*;
use tokio::sync::mpsc;

pub struct MultiResponse<T> {
    incoming: mpsc::Receiver<Result<RawResponse<T>, RpcError>>,
    stop: Cancellation,
    task: Option<JoinHandle<()>>,
}
impl<T> MultiResponse<T> {
    pub async fn recv(&mut self) -> Option<Result<RawResponse<T>, RpcError>> {
        self.incoming.recv().await
    }
    pub async fn close(&mut self) {
        self.stop.cancel();
        if let Some(task) = self.task.take() {
            let _ = task.await;
        }
    }
}
impl<T> Drop for MultiResponse<T> {
    fn drop(&mut self) {
        self.stop.cancel();
    }
}
impl RpcClient {
    pub async fn multi<Request, Response>(
        &self,
        spec: &MethodSpec,
        request: &Request,
        options: CallOptions,
    ) -> Result<MultiResponse<Response>, RpcError>
    where
        Request: Message,
        Response: Message + prost::Name + Default + Send + 'static,
    {
        if self.close.is_cancelled() {
            return Err(RpcError::closed());
        }
        if spec.require_claim {
            return Err(RpcError::new(
                "invalid_argument",
                "multi-response RPC does not select a single claim owner",
            ));
        }
        let start = Instant::now();
        let timeout = start
            .checked_add(options.timeout)
            .ok_or_else(|| RpcError::new("invalid_argument", "request timeout overflow"))?;
        let deadline = options.deadline.map_or(timeout, |d| d.min(timeout));
        let permit = tokio::select! {
            biased;
            _=self.close.cancelled()=>return Err(RpcError::closed()),
            _=options.cancel.cancelled()=>return Err(RpcError::canceled()),
            _=tokio::time::sleep_until(deadline.into())=>return Err(RpcError::timed_out()),
            permit=self.slots.clone().acquire_owned()=>permit.map_err(|_|RpcError::closed())?,
        };
        let id = rtc_core::ids::new_sid("request_");
        let channel = channels::response(&spec.service, &self.client_id);
        let mut subscription = tokio::select! {
            biased;
            _=self.close.cancelled()=>return Err(RpcError::closed()),
            _=options.cancel.cancelled()=>return Err(RpcError::canceled()),
            _=tokio::time::sleep_until(deadline.into())=>return Err(RpcError::timed_out()),
            sub=self.bus.subscribe::<pb::Response>(&channel,false)=>sub?,
        };
        let sent_at = unix_ns();
        let expiry = call_expiry_ns(start, sent_at, timeout, options.deadline, options.expiry_ns);
        let message = pb::Request {
            request_id: id.clone(),
            client_id: self.client_id.clone(),
            sent_at,
            expiry,
            multi: true,
            raw_request: request.encode_to_vec(),
            metadata: options.metadata.clone().into_iter().collect(),
            ..Default::default()
        };
        self.pending
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .insert(id.clone());
        let pending = PendingGuard {
            pending: self.pending.clone(),
            id: id.clone(),
        };
        let request_channel = spec.request_channel();
        tokio::select! {
            biased;
            _=self.close.cancelled()=>return Err(RpcError::closed()),
            _=options.cancel.cancelled()=>return Err(RpcError::canceled()),
            _=tokio::time::sleep_until(deadline.into())=>return Err(RpcError::timed_out()),
            result=self.bus.publish(&request_channel,&message)=>{result?;}
        }
        let (out, incoming) = mpsc::channel(self.bus.limits().inbox_messages);
        let stop = Cancellation::default();
        let stopping = stop.clone();
        let closed = self.close.clone();
        let bus = self.bus.clone();
        let task = tokio::spawn(async move {
            let _permit = permit;
            let _pending = pending;
            loop {
                let received = tokio::select! {
                    biased;
                    _=stopping.cancelled()=>break,
                    _=closed.cancelled()=>break,
                    _=options.cancel.cancelled()=>break,
                    _=tokio::time::sleep_until(deadline.into())=>break,
                    bytes=subscription.recv()=>bytes,
                };
                let bytes = match received {
                    Ok(bytes) => bytes,
                    Err(error) => {
                        let _ = out.try_send(Err(error.into()));
                        break;
                    }
                };
                let response = match received_response_for_bus(&bus, &bytes) {
                    Ok(response) => response,
                    Err(_) => continue,
                };
                if response.message.request_id != id {
                    continue;
                }
                let response = if response.message.error.is_empty() {
                    decode_response(response.message.raw_response, |raw| {
                        lk_protocol::wire::decode(raw)
                            .map_err(|e| RpcError::malformed_response(e.to_string()))
                    })
                } else {
                    Err(RpcError::remote_received(response))
                };
                tokio::select! {
                    biased;
                    _=stopping.cancelled()=>break,
                    _=closed.cancelled()=>break,
                    _=options.cancel.cancelled()=>break,
                    _=tokio::time::sleep_until(deadline.into())=>break,
                    sent=out.send(response)=>if sent.is_err(){break},
                }
            }
            let _ = subscription.unsubscribe(&channel).await;
        });
        Ok(MultiResponse {
            incoming,
            stop,
            task: Some(task),
        })
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
    use lk_protocol::livekit as lk;
    fn bus() -> Arc<LocalTransport> {
        LocalTransport::new(TransportLimits {
            wire: WireLimits {
                encoded_bytes: 1 << 20,
                decompressed_bytes: 1 << 20,
            },
            compression: CompressionPolicy::default(),
            inbox_messages: 16,
            subscriptions: 100,
            commands: 100,
            io_timeout: Duration::from_secs(1),
        })
        .unwrap()
    }
    fn spec() -> MethodSpec {
        MethodSpec {
            service: "MultiConsumerTest".into(),
            method: "Check".into(),
            topics: vec![rtc_core::ids::new_sid("topic_")],
            require_claim: false,
            queue: false,
        }
    }
    fn limits() -> ServerLimits {
        ServerLimits {
            concurrent: 8,
            max_request_duration: Duration::from_secs(1),
            allow_skip_claim: false,
        }
    }
    #[tokio::test]
    async fn actual_broadcast_keeps_every_node_reply_and_remote_error_until_window_close() {
        let bus = bus();
        let spec = spec();
        let a = RpcServer::register_multi::<lk::Room, lk::Room, _, _>(
            bus.clone(),
            spec.clone(),
            limits(),
            |_, _| async {
                Ok(lk::Room {
                    name: "a".into(),
                    ..Default::default()
                })
            },
        )
        .await
        .unwrap();
        let b = RpcServer::register_multi::<lk::Room, lk::Room, _, _>(
            bus.clone(),
            spec.clone(),
            limits(),
            |_, _| async {
                Ok(lk::Room {
                    name: "b".into(),
                    ..Default::default()
                })
            },
        )
        .await
        .unwrap();
        let c = RpcServer::register_multi::<lk::Room, lk::Room, _, _>(
            bus.clone(),
            spec.clone(),
            limits(),
            |_, _| async { Err(RpcError::new("unavailable", "real failed node")) },
        )
        .await
        .unwrap();
        let client = RpcClient::new(bus, 8).unwrap();
        let mut responses = client
            .multi::<_, lk::Room>(
                &spec,
                &lk::Room::default(),
                CallOptions::new(Duration::from_millis(100)),
            )
            .await
            .unwrap();
        let mut names = Vec::new();
        let mut errors = Vec::new();
        while let Some(response) = responses.recv().await {
            match response {
                Ok(response) => {
                    assert_eq!(response.raw, response.message.encode_to_vec());
                    names.push(response.message.name)
                }
                Err(error) => errors.push(error),
            }
        }
        names.sort();
        assert_eq!(names, ["a", "b"]);
        assert_eq!(errors.len(), 1);
        assert_eq!(errors[0].origin, RpcErrorOrigin::Remote);
        assert_eq!(errors[0].message, "real failed node");
        responses.close().await;
        assert_eq!(client.pending_count(), 0);
        a.close().await;
        b.close().await;
        c.close().await;
    }
    #[tokio::test]
    async fn missing_nodes_expire_without_invented_reply_and_drop_releases_pending_slot() {
        let bus = bus();
        let spec = spec();
        let client = RpcClient::new(bus, 1).unwrap();
        let mut replies = client
            .multi::<_, lk::Room>(
                &spec,
                &lk::Room::default(),
                CallOptions::new(Duration::from_millis(20)),
            )
            .await
            .unwrap();
        assert!(replies.recv().await.is_none());
        replies.close().await;
        assert_eq!(client.pending_count(), 0);
        let replies = client
            .multi::<_, lk::Room>(
                &spec,
                &lk::Room::default(),
                CallOptions::new(Duration::from_secs(60)),
            )
            .await
            .unwrap();
        assert_eq!(client.pending_count(), 1);
        drop(replies);
        tokio::time::timeout(Duration::from_secs(1), async {
            while client.pending_count() != 0 {
                tokio::task::yield_now().await
            }
        })
        .await
        .unwrap();
        let mut again = client
            .multi::<_, lk::Room>(
                &spec,
                &lk::Room::default(),
                CallOptions::new(Duration::from_millis(20)),
            )
            .await
            .unwrap();
        again.close().await;
        assert_eq!(client.pending_count(), 0);
    }
}
