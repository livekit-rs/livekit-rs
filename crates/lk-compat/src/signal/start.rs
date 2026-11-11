//! Fixed RTCService startConnection + readInitialResponse retry boundary.
use crate::{
    cluster_signal::{RemoteSession, SignalConnection},
    state::AppState,
};
use lk_protocol::livekit as pb;
use rtc_core::session::{JoinError, JoinParams, SessionOutput};
use std::time::Duration;

pub(super) enum Started {
    Local(rtc_runtime::SessionHandle, SessionOutput),
    Remote(RemoteSession, Vec<u8>),
    StateMismatch,
}
#[derive(Debug)]
pub(super) enum StartError {
    Join(JoinError),
    Closed,
    Timeout,
}

pub(super) async fn attempt(
    state: &AppState,
    params: JoinParams,
    tracks: Vec<pb::AddTrackRequest>,
    budget: Duration,
) -> Result<Started, StartError> {
    let admission_room = state
        .room_control
        .as_ref()
        .map(|_| params.room.name.0.clone());
    let connected = match &state.cluster_signal {
        Some(cluster) => cluster.connect_once(params, tracks).await,
        None => match super::check_owner_profile_sizes(state, &params) {
            Ok(()) => state
                .manager
                .join(params)
                .await
                .map(SignalConnection::Local),
            Err(message) => Err(JoinError::ProfileLimit(message)),
        },
    };
    match connected {
        Err(JoinError::ReconnectStateMismatch) => Ok(Started::StateMismatch),
        Err(JoinError::ProfileLimit(message)) => {
            tracing::warn!(error = message, "error starting signal session");
            Err(StartError::Closed)
        }
        Err(error) => Err(StartError::Join(error)),
        Ok(SignalConnection::Local(mut handle)) => {
            let deadline = tokio::time::Instant::now()
                .checked_add(budget)
                .ok_or(StartError::Timeout)?;
            let first = read_initial(&mut handle.output, budget).await?;
            // A native SDK can finish its Connect callback from the initial Join
            // before sending any RTC request. The input-pump gate alone does not
            // make a healthy Redis-backed participant visible at that point.
            // Use the same initial-response budget and actual room admission
            // receipt: a genuine store I/O failure still follows log/continue.
            if let Some(room) = admission_room
                && matches!(
                    &first,
                    SessionOutput::Signal(rtc_core::session::SignalEvent::Join(_))
                )
            {
                let admitted = state.routing_events.wait_participant_admitted(
                    &room,
                    &handle.identity.0,
                    &handle.participant_sid.0,
                );
                tokio::time::timeout_at(deadline, admitted)
                    .await
                    .map_err(|_| StartError::Timeout)?
                    .map_err(|error| StartError::Join(JoinError::Internal(error.message)))?;
            }
            // Closing a source without a response is not a successful handshake.
            if matches!(first, SessionOutput::Close) {
                return Err(StartError::Closed);
            }
            Ok(Started::Local(handle, first))
        }
        Ok(SignalConnection::Remote(mut remote)) => {
            match read_initial(&mut remote.incoming, budget).await {
                Ok(first) => Ok(Started::Remote(remote, first)),
                Err(error) => {
                    // Close both request/response directions and join the actual
                    // stream pump before selecting/starting another owner attempt.
                    remote.close().await;
                    Err(error)
                }
            }
        }
    }
}
async fn read_initial<T>(
    source: &mut tokio::sync::mpsc::Receiver<T>,
    budget: Duration,
) -> Result<T, StartError> {
    match tokio::time::timeout(budget, source.recv()).await {
        Ok(Some(first)) => Ok(first),
        Ok(None) => Err(StartError::Closed),
        Err(_) => Err(StartError::Timeout),
    }
}
/// Setup failure and first-response failure share one attempt counter. There is
/// no sleep: source RTCService tries immediately again with3+attempt seconds.
pub(super) async fn retry_start<T, E, F, Fut>(attempts: usize, mut start: F) -> Result<Option<T>, E>
where
    F: FnMut(usize, Duration) -> Fut,
    Fut: std::future::Future<Output = Result<T, E>>,
{
    // Signed source loop runs zero iterations for both0 and negatives. The
    // service projects negatives to0 and distinguishes this from any real
    // attempt failure. Never fabricate an owner/session or panic here.
    if attempts == 0 {
        return Ok(None);
    }
    let mut last = None;
    for attempt in 0..attempts {
        let seconds = 3u64.saturating_add(attempt as u64);
        match start(attempt, Duration::from_secs(seconds)).await {
            Ok(started) => return Ok(Some(started)),
            Err(error) => last = Some(error),
        }
    }
    match last {
        Some(error) => Err(error),
        None => Ok(None),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rtc_core::{
        ids::{Identity, ProjectId, RoomKey},
        permission::{Grants, VideoGrant},
    };
    use std::sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    };
    fn params(key: &RoomKey, grants: Grants) -> JoinParams {
        JoinParams {
            room: key.clone(),
            identity: Identity(grants.identity.clone()),
            name: grants.name.clone(),
            metadata: grants.metadata.clone(),
            attributes: grants.attributes.clone(),
            grants,
            client: Default::default(),
            capabilities: Default::default(),
            auto_subscribe: true,
            auto_subscribe_data_track: true,
            adaptive_stream: false,
            single_pc: false,
            disable_ice_lite: false,
            subscriber_allow_pause: None,
            reconnect: false,
            reconnect_reason: 0,
            participant_sid: None,
            add_tracks: vec![],
            publisher_offer: None,
        }
    }
    struct Retained(Arc<AtomicUsize>);
    impl Drop for Retained {
        fn drop(&mut self) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }
    #[tokio::test]
    async fn nonpositive_count_never_calls_start_factory_or_allocates_a_session() {
        for signed in [0_i64, -1] {
            let called = Arc::new(AtomicUsize::new(0));
            let result = retry_start(signed.max(0) as usize, |_, _| {
                called.fetch_add(1, Ordering::SeqCst);
                async { Ok::<_, StartError>(()) }
            })
            .await;
            assert!(matches!(result, Ok(None)));
            assert_eq!(called.load(Ordering::SeqCst), 0);
        }
    }
    #[tokio::test(start_paused = true)]
    async fn actual_initial_channels_use_three_four_five_second_deadlines_and_drop_before_retry() {
        let history = Arc::new(Mutex::new(Vec::new()));
        let retired = Arc::new(AtomicUsize::new(0));
        let start = tokio::time::Instant::now();
        let result = retry_start(3, |index, budget| {
            let history = history.clone();
            let retired = retired.clone();
            async move {
                assert_eq!(
                    retired.load(Ordering::SeqCst),
                    index,
                    "old attempt must release before next begins"
                );
                let (tx, mut rx) = tokio::sync::mpsc::channel::<Retained>(1);
                tx.send(Retained(retired.clone()))
                    .await
                    .unwrap_or_else(|_| panic!("live receiver"));
                let _owned = rx.recv().await.unwrap();
                history
                    .lock()
                    .unwrap()
                    .push((index, budget, tokio::time::Instant::now() - start));
                let result = read_initial(&mut rx, budget).await;
                drop(tx);
                result.map(|_| ())
            }
        })
        .await;
        assert!(matches!(result, Err(StartError::Timeout)));
        assert_eq!(retired.load(Ordering::SeqCst), 3);
        assert_eq!(
            *history.lock().unwrap(),
            vec![
                (0, Duration::from_secs(3), Duration::ZERO),
                (1, Duration::from_secs(4), Duration::from_secs(3)),
                (2, Duration::from_secs(5), Duration::from_secs(7))
            ]
        );
        assert_eq!(tokio::time::Instant::now() - start, Duration::from_secs(12));
    }
    #[tokio::test(start_paused = true)]
    async fn source_closure_retries_entire_factory_but_first_response_stops_attempts() {
        let called = Arc::new(AtomicUsize::new(0));
        let first = retry_start(5, |index, budget| {
            let called = called.clone();
            async move {
                called.fetch_add(1, Ordering::SeqCst);
                let (tx, mut rx) = tokio::sync::mpsc::channel(1);
                if index == 0 {
                    drop(tx);
                } else {
                    tx.send("real first response").await.unwrap();
                }
                read_initial(&mut rx, budget).await
            }
        })
        .await
        .unwrap();
        assert_eq!(first, Some("real first response"));
        assert_eq!(called.load(Ordering::SeqCst), 2);
    }
    #[tokio::test]
    async fn actual_standalone_join_returns_real_first_response_before_upgrade() {
        let (state, net) = crate::test_support::state().await;
        assert_eq!(state.signal_connect_attempts, 3);
        let params = params(
            &RoomKey::new(ProjectId::local(), "actual-start-policy"),
            Grants {
                identity: "participant".into(),
                video: VideoGrant {
                    room_join: true,
                    room: "actual-start-policy".into(),
                    ..Default::default()
                },
                ..Default::default()
            },
        );
        let started = retry_start(state.signal_connect_attempts, |_, budget| {
            attempt(&state, params.clone(), vec![], budget)
        })
        .await
        .unwrap();
        match started {
            Some(Started::Local(
                handle,
                SessionOutput::Signal(rtc_core::session::SignalEvent::Join(_)),
            )) => drop(handle),
            _ => panic!("actual owner must supply its real join before upgrade"),
        }
        state.manager.close_all().await;
        state.shutdown_workers().await;
        net.shutdown_and_wait().await;
    }
    #[tokio::test(start_paused = true)]
    async fn cancelling_the_wait_drops_the_actual_source_without_starting_another_attempt() {
        let calls = Arc::new(AtomicUsize::new(0));
        let (tx, rx) = tokio::sync::mpsc::channel::<String>(1);
        let task = {
            let calls = calls.clone();
            tokio::spawn(async move {
                let mut receiver = Some(rx);
                retry_start(3, |_, budget| {
                    calls.fetch_add(1, Ordering::SeqCst);
                    let mut receiver = receiver
                        .take()
                        .expect("cancelled first attempt cannot restart");
                    async move { read_initial(&mut receiver, budget).await }
                })
                .await
            })
        };
        tokio::task::yield_now().await;
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert!(!tx.is_closed());
        task.abort();
        assert!(task.await.unwrap_err().is_cancelled());
        tx.closed().await;
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }
}
