//! Single-node equivalent of the original local PSRPC CreateRoom request boundary.
//! Client timeouts do not drop accepted owner/Background automatic-egress work.
use crate::room_control::create_policy::CreateRoomRetry;
use crate::service_bus::rpc::{Cancellation, RpcError};
use lk_protocol::livekit as pb;
use rtc_core::config::RoomDefaults;
use std::future::Future;
use std::sync::Mutex;
use tokio::sync::oneshot;
use tokio::task::JoinSet;

#[derive(Default)]
struct Tasks {
    closed: bool,
    accepted: JoinSet<()>,
}
#[derive(Default)]
pub(crate) struct LocalRoomCreate {
    tasks: Mutex<Tasks>,
    stopped: Cancellation,
    drain_gate: tokio::sync::Mutex<()>,
}
impl LocalRoomCreate {
    fn submit<F>(&self, work: F) -> Result<oneshot::Receiver<Result<pb::Room, RpcError>>, RpcError>
    where
        F: Future<Output = Result<pb::Room, RpcError>> + Send + 'static,
    {
        let mut tasks = self.tasks.lock().unwrap_or_else(|p| p.into_inner());
        if tasks.closed {
            return Err(RpcError::closed());
        }
        while tasks.accepted.try_join_next().is_some() {}
        let (send, receive) = oneshot::channel();
        tasks.accepted.spawn(async move {
            let result = work.await;
            let _ = send.send(result);
        });
        Ok(receive)
    }
    pub(crate) async fn request<F, Make>(
        &self,
        room: &RoomDefaults,
        mut make: Make,
    ) -> Result<pb::Room, RpcError>
    where
        Make: FnMut(std::time::Instant, i64) -> F,
        F: Future<Output = Result<pb::Room, RpcError>> + Send + 'static,
    {
        let mut retry = CreateRoomRetry::new(room);
        loop {
            let deadline = std::time::Instant::now() + retry.timeout();
            let expiry_ns = crate::service_bus::rpc::capture_expiry_ns(deadline);
            let response = self.submit(make(deadline, expiry_ns))?;
            let result = tokio::select! {
                biased;
                _=self.stopped.cancelled()=>return Err(RpcError::closed()),
                result=tokio::time::timeout_at(deadline.into(),response)=>match result {
                    Ok(Ok(value))=>value,
                    Ok(Err(_))=>Err(RpcError::new("internal","room owner task terminated without response")),
                    Err(_)=>Err(RpcError::new("deadline_exceeded","request timed out")),
                },
            };
            match result {
                Ok(room) => return Ok(room),
                Err(error) if retry.next(&error) => {}
                Err(error) => return Err(error),
            }
        }
    }
    pub(crate) fn close_admission(&self) {
        self.tasks.lock().unwrap_or_else(|p| p.into_inner()).closed = true;
        self.stopped.cancel();
    }
    pub(crate) async fn drain(&self) {
        // Every concurrent shutdown caller waits for this same actual join,
        // rather than returning early after another caller took the JoinSet.
        let _gate = self.drain_gate.lock().await;
        let mut accepted = {
            let mut tasks = self.tasks.lock().unwrap_or_else(|p| p.into_inner());
            assert!(tasks.closed);
            std::mem::take(&mut tasks.accepted)
        };
        while let Some(result) = accepted.join_next().await {
            if let Err(error) = result {
                tracing::error!(%error,"accepted room owner task failed");
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tokio::sync::Notify;

    #[tokio::test]
    async fn actual_request_timeout_does_not_abort_accepted_room_generation() {
        let (app, net) = crate::test_support::state().await;
        let owner = Arc::new(LocalRoomCreate::default());
        let release = Arc::new(Notify::new());
        let entered = Arc::new(Notify::new());
        let key = app.room_key("timeout-retained-owner");
        let mut policy = app.config.room.clone();
        policy.create_room_attempts = 1;
        policy.create_room_timeout = rtc_core::config::PolicyDuration::from_millis(5);
        let manager = app.manager.clone();
        let work_key = key.clone();
        let release_work = release.clone();
        let entered_work = entered.clone();
        let result = owner
            .request(&policy, move |_deadline, _expiry_ns| {
                let manager = manager.clone();
                let key = work_key.clone();
                let release = release_work.clone();
                let entered = entered_work.clone();
                async move {
                    // The real owner exists before held Background work, like
                    // getOrCreateRoom registering newRoom before StartEgress.
                    let room = manager
                        .create_room(key, rtc_runtime::CreateRoomOptions::default())
                        .await
                        .map_err(|e| RpcError::new("internal", e.to_string()))?;
                    entered.notify_one();
                    release.notified().await;
                    Ok(crate::convert::room_info_to_proto(&room))
                }
            })
            .await;
        assert_eq!(result.unwrap_err().code, "deadline_exceeded");
        entered.notified().await;
        assert!(app.manager.room_exists(&key));
        let timed_out_sid = app
            .manager
            .list_rooms(&key.project, std::slice::from_ref(&key.name))[0]
            .sid
            .clone();
        owner.close_admission();
        release.notify_one();
        owner.drain().await;
        let rooms = app
            .manager
            .list_rooms(&key.project, std::slice::from_ref(&key.name));
        assert_eq!(rooms.len(), 1);
        let original = rooms[0].sid.clone();
        assert_eq!(original, timed_out_sid);
        app.manager.delete_room(&key).await.unwrap();
        let replacement = app
            .manager
            .create_room(key.clone(), rtc_runtime::CreateRoomOptions::default())
            .await
            .unwrap();
        assert_ne!(replacement.sid, original);
        owner.drain().await;
        assert_eq!(
            app.manager
                .list_rooms(&key.project, std::slice::from_ref(&key.name))[0]
                .sid,
            replacement.sid
        );
        app.manager.close_all().await;
        app.shutdown_workers().await;
        net.shutdown_and_wait().await;
    }
    #[tokio::test]
    async fn concurrent_shutdown_waits_for_every_accepted_task_and_rejects_new_work() {
        let owner = Arc::new(LocalRoomCreate::default());
        let release = Arc::new(Notify::new());
        let entered = Arc::new(Notify::new());
        let completed = Arc::new(AtomicUsize::new(0));
        let w_release = release.clone();
        let w_entered = entered.clone();
        let w_completed = completed.clone();
        let response = owner
            .submit(async move {
                w_entered.notify_one();
                w_release.notified().await;
                w_completed.fetch_add(1, Ordering::SeqCst);
                Ok(pb::Room::default())
            })
            .unwrap();
        entered.notified().await;
        drop(response); // client abandonment is not owner cancellation
        owner.close_admission();
        assert!(owner.submit(async { Ok(pb::Room::default()) }).is_err());
        let a = owner.clone();
        let b = owner.clone();
        let first = tokio::spawn(async move {
            a.drain().await;
        });
        let second = tokio::spawn(async move {
            b.drain().await;
        });
        tokio::task::yield_now().await;
        assert!(!first.is_finished());
        assert!(!second.is_finished());
        release.notify_one();
        first.await.unwrap();
        second.await.unwrap();
        assert_eq!(completed.load(Ordering::SeqCst), 1);
    }
}
