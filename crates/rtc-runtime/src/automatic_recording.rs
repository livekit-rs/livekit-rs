//! Tracked, protocol-neutral execution of actual owner-authorized recording work.
//! Go launches with Background: closing a room must not cancel an already
//! accepted worker RPC. Server shutdown drains work before closing worker buses.
use rtc_core::{auto_egress::RecordingLaunch, model::RoomInfo};
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use tokio::task::JoinSet;

/// Adapter implementation owns actual worker transport and failure notification.
pub trait RecordingLauncher: Send + Sync {
    /// Create-only composite operation. The caller retains its generation lease
    /// across this await; default is an honest unavailable launcher.
    fn launch_room(
        &self,
        _room: RoomInfo,
        _policy: rtc_core::auto_egress::RoomCompositeRecordingPolicy,
    ) -> Pin<Box<dyn Future<Output = Result<(), String>> + Send + 'static>> {
        Box::pin(async { Err("egress not connected (redis required)".into()) })
    }
    /// A launch attempt is not proof of media output. The owned room and action
    /// retain their original room/participant/track SIDs across all awaits.
    fn launch(
        &self,
        room: RoomInfo,
        action: RecordingLaunch,
    ) -> Pin<Box<dyn Future<Output = Result<(), String>> + Send + 'static>>;
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct RecordingWorkStats {
    pub completed: u64,
    pub failed: u64,
    pub panicked_or_cancelled: u64,
}
struct Work {
    launcher: Option<Arc<dyn RecordingLauncher>>,
    tasks: JoinSet<Result<(), String>>,
    stats: RecordingWorkStats,
    closing: bool,
}
impl Default for Work {
    fn default() -> Self {
        Self {
            launcher: None,
            tasks: JoinSet::new(),
            stats: Default::default(),
            closing: false,
        }
    }
}
/// No detached tasks and no name-only completion map. Completed handles are
/// reaped on submission; remaining handles are awaited on server shutdown.
#[derive(Default)]
pub(crate) struct RecordingExecutor {
    work: Mutex<Work>,
    drain_gate: tokio::sync::Mutex<()>,
}
impl RecordingExecutor {
    pub(crate) fn install(&self, launcher: Arc<dyn RecordingLauncher>) -> Result<(), String> {
        let mut work = self.work.lock().unwrap_or_else(|p| p.into_inner());

        if work.launcher.is_some() || work.closing {
            return Err("recording bridge already attached or shutting down".into());
        }
        work.launcher = Some(launcher);
        Ok(())
    }
    fn completed(work: &mut Work, result: Result<Result<(), String>, tokio::task::JoinError>) {
        match result {
            Ok(Ok(())) => work.stats.completed += 1,
            Ok(Err(error)) => {
                work.stats.failed += 1;
                tracing::error!(%error,"automatic recording launch failed");
            }
            Err(error) => {
                work.stats.panicked_or_cancelled += 1;
                tracing::error!(%error,"automatic recording task did not complete");
            }
        }
    }
    pub(crate) fn submit(&self, room: RoomInfo, action: RecordingLaunch) {
        let mut work = self.work.lock().unwrap_or_else(|p| p.into_inner());
        while let Some(result) = work.tasks.try_join_next() {
            Self::completed(&mut work, result);
        }
        // No arbitrary fixed in-flight cap: source permits all actual accepted
        // publications. Memory is proportional to real pending worker calls,
        // never to historical completed tracks or a detached unbounded queue.
        if work.closing {
            work.stats.failed += 1;
            tracing::warn!("automatic recording rejected during server shutdown");
            return;
        }
        let Some(launcher) = work.launcher.clone() else {
            work.stats.failed += 1;
            tracing::error!("automatic recording launcher unavailable");
            return;
        };
        work.tasks.spawn(launcher.launch(room, action));
    }
    pub(crate) async fn launch_room(
        &self,
        room: RoomInfo,
        policy: rtc_core::auto_egress::RoomCompositeRecordingPolicy,
    ) -> Result<(), String> {
        let launcher = {
            let work = self.work.lock().unwrap_or_else(|p| p.into_inner());
            if work.closing {
                return Err("server is shutting down".into());
            }
            work.launcher
                .clone()
                .ok_or_else(|| "egress not connected (redis required)".to_string())?
        };
        launcher.launch_room(room, policy).await
    }
    pub(crate) fn close_admission(&self) {
        self.work.lock().unwrap_or_else(|p| p.into_inner()).closing = true;
    }
    pub(crate) async fn drain(&self) {
        // Every concurrent caller waits for the same completed drain, rather
        // than returning early after another caller takes the JoinSet.
        let _drain = self.drain_gate.lock().await;
        let mut tasks = {
            let mut work = self.work.lock().unwrap_or_else(|p| p.into_inner());
            work.closing = true;

            std::mem::take(&mut work.tasks)
        };
        while let Some(result) = tasks.join_next().await {
            Self::completed(
                &mut self.work.lock().unwrap_or_else(|p| p.into_inner()),
                result,
            );
        }
    }
    #[cfg(test)]
    fn stats(&self) -> RecordingWorkStats {
        self.work.lock().unwrap_or_else(|p| p.into_inner()).stats
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rtc_core::auto_egress::ParticipantRecordingPolicy;
    use tokio::sync::{Notify, oneshot};
    struct Held {
        entered: Arc<Notify>,
        release: Mutex<Option<oneshot::Receiver<()>>>,
    }
    impl RecordingLauncher for Held {
        fn launch(
            &self,
            _room: RoomInfo,
            _action: RecordingLaunch,
        ) -> Pin<Box<dyn Future<Output = Result<(), String>> + Send + 'static>> {
            let entered = self.entered.clone();
            let release = self.release.lock().unwrap().take().unwrap();
            Box::pin(async move {
                entered.notify_one();
                let _ = release.await;
                Err("actual-test-launch-failure".into())
            })
        }
    }
    // No sleep/worker simulation: this proves tracking/drain, not media support.
    #[tokio::test]
    async fn pending_launch_is_tracked_until_real_completion_and_error_collected() {
        let executor = Arc::new(RecordingExecutor::default());
        let entered = Arc::new(Notify::new());
        let (tx, rx) = oneshot::channel();
        executor
            .install(Arc::new(Held {
                entered: entered.clone(),
                release: Mutex::new(Some(rx)),
            }))
            .unwrap();
        let room = RoomInfo {
            turn_password: String::new(),
            sid: "RM_owned".into(),
            name: "room".into(),
            empty_timeout_s: 0,
            departure_timeout_s: 0,
            max_participants: 0,
            creation_time_ms: 0,
            metadata: String::new(),
            num_participants: 0,
            num_publishers: 0,
            active_recording: false,
            enabled_codecs: Vec::new(),
            version: rtc_core::model::TimedVersion {
                unix_micro: 0,
                ticks: 0,
            },
        };
        executor.submit(
            room.clone(),
            RecordingLaunch::Participant {
                room_sid: room.sid,
                participant_sid: "PA_owned".into(),
                identity: "human".into(),
                policy: ParticipantRecordingPolicy::default(),
            },
        );
        entered.notified().await;
        let owned = executor.clone();
        let draining = tokio::spawn(async move {
            owned.drain().await;
        });
        let also_owned = executor.clone();
        let second = tokio::spawn(async move {
            also_owned.drain().await;
        });
        tokio::task::yield_now().await;
        assert!(!draining.is_finished());
        assert!(!second.is_finished());
        tx.send(()).unwrap();
        draining.await.unwrap();
        second.await.unwrap();
        assert_eq!(executor.stats().failed, 1);
        assert_eq!(executor.stats().completed, 0);
    }
}
