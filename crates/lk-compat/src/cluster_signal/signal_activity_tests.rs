//! Genuine room actor observations; no activity counter on the controller.
use super::*;
use crate::signal::encode::{Inbound, RequestCache, decode_request};
use rtc_core::room::DebugParticipantSnapshot;
use rtc_core::session::SessionRequest;

pub(super) async fn owner(
    state: &crate::state::AppState,
    room: &str,
    identity: &str,
) -> DebugParticipantSnapshot {
    state
        .manager
        .debug_room_details()
        .await
        .into_iter()
        .find(|r| r.room.name.0 == room)
        .expect("real owner room")
        .participants
        .into_iter()
        .find(|p| p.info.identity.0 == identity)
        .expect("real owner participant")
}

fn requests() -> Vec<(pb::SignalRequest, Option<u32>)> {
    use pb::signal_request::Message as R;
    vec![
        (
            pb::SignalRequest {
                message: Some(R::PingReq(pb::Ping {
                    timestamp: 1,
                    rtt: 95,
                })),
            },
            Some(95),
        ),
        (
            pb::SignalRequest {
                message: Some(R::Ping(1)),
            },
            None,
        ),
        (
            pb::SignalRequest {
                message: Some(R::PingReq(pb::Ping {
                    timestamp: 2,
                    rtt: 0,
                })),
            },
            None,
        ),
        (
            pb::SignalRequest {
                message: Some(R::PingReq(pb::Ping {
                    timestamp: 3,
                    rtt: -1,
                })),
            },
            None,
        ),
        (pb::SignalRequest { message: None }, None),
        (
            pb::SignalRequest {
                message: Some(R::UpdateLayers(Default::default())),
            },
            None,
        ),
        (
            pb::SignalRequest {
                message: Some(R::Simulate(Default::default())),
            },
            None,
        ),
        // Decodable SignalRequest with semantic-invalid ICE JSON still reaches
        // UpdateLastSeenSignal in Go before its semantic handler is invoked.
        (
            pb::SignalRequest {
                message: Some(R::Trickle(pb::TrickleRequest {
                    candidate_init: "{".into(),
                    ..Default::default()
                })),
            },
            None,
        ),
    ]
}

#[tokio::test]
async fn decoded_local_signals_refresh_actual_owner_without_fabricating_rtt_and_reject_stale_sid() {
    let (state, net) = crate::test_support::state().await;
    let room = "local-signal-activity";
    let identity = "actual-owner";
    let params = super::tests::join(&state.config, room, identity);
    let handle = state.manager.join(params.clone()).await.unwrap();
    let mut cache = RequestCache::default();
    let mut rtt = 0;
    for (request, measurement) in requests() {
        let before = owner(&state, room, identity).await;
        tokio::time::sleep(Duration::from_millis(2)).await;
        let inbound = decode_request(request, &mut cache);
        let actual = match inbound {
            Inbound::Forward(value) => value,
            Inbound::ForwardAndReply(value, response) => {
                assert!(
                    response.message.is_some(),
                    "existing local response remains intact"
                );
                *value
            }
            _ => panic!("successfully decoded Go SignalRequest must reach its owner"),
        };
        assert!(handle.send(actual).await);
        let after = owner(&state, room, identity).await;
        assert!(after.signaling_last_seen > before.signaling_last_seen);
        if let Some(value) = measurement {
            rtt = value;
        }
        assert_eq!(
            after.signaling_rtt_ms, rtt,
            "activity is independent of positive RTT sampling"
        );
    }
    let before = owner(&state, room, identity).await;
    for raw in [&[0x80][..], &[0xff, 0xff][..]] {
        assert!(crate::signal::session::decode_binary_request(raw).is_err());
    }
    assert!(crate::signal::session::decode_text_request("{").is_err());
    let after = owner(&state, room, identity).await;
    assert_eq!(
        after.signaling_last_seen, before.signaling_last_seen,
        "malformed wire cannot manufacture owner activity"
    );
    assert_eq!(after.signaling_rtt_ms, 95);

    // Replacing the same identity gives a different SID. The old bounded actor
    // sender may still enqueue, but neither freshness nor RTT may touch it.
    let replacement = state.manager.join(params).await.unwrap();
    assert_ne!(replacement.participant_sid, handle.participant_sid);
    let before = owner(&state, room, identity).await;
    let _ = handle.send(SessionRequest::SignalActivity).await;
    let _ = handle.send(SessionRequest::SignalRtt(777)).await;
    let after = owner(&state, room, identity).await;
    assert_eq!(after.signaling_last_seen, before.signaling_last_seen);
    assert_eq!(after.signaling_rtt_ms, 0);
    state
        .manager
        .delete_room(&rtc_core::ids::RoomKey::new(
            rtc_core::ids::ProjectId::local(),
            room,
        ))
        .await
        .unwrap();
    assert!(
        !replacement.send(SessionRequest::SignalActivity).await,
        "closed room cannot accept activity"
    );
    drop(handle);
    drop(replacement);
    net.shutdown();
}

pub(super) async fn remote_owner_activity(
    entry: &crate::state::AppState,
    state: &crate::state::AppState,
    remote: &mut RemoteSession,
    room: &str,
    identity: &str,
) {
    let mut rtt = 0;
    for (request, measurement) in requests() {
        let before = owner(state, room, identity).await;
        tokio::time::sleep(Duration::from_millis(2)).await;
        assert!(remote.send(request.encode_to_vec()).await);
        let after = tokio::time::timeout(Duration::from_secs(3), async {
            loop {
                let value = owner(state, room, identity).await;
                if value.signaling_last_seen > before.signaling_last_seen {
                    break value;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("real PSRPC relay must deliver decoded signal activity to owner");
        if let Some(value) = measurement {
            rtt = value;
        }
        assert_eq!(
            after.signaling_rtt_ms, rtt,
            "relay activity must not become a zero/negative RTT sample"
        );
        assert!(
            !entry.manager.room_exists(&rtc_core::ids::RoomKey::new(
                rtc_core::ids::ProjectId::local(),
                room
            )),
            "controller cannot create a shadow owner merely to record activity"
        );
    }
}

#[tokio::test]
async fn real_closed_reply_writer_still_attempts_bounded_owner_before_session_closes() {
    use std::sync::atomic::{AtomicBool, Ordering};
    use tokio::io::AsyncWriteExt;
    for close_owner in [false, true] {
        let (state, net) = crate::test_support::state().await;
        let room = "closed-reply-owner-activity";
        let identity = "real-closed-writer";
        let handle = state
            .manager
            .join(super::tests::join(&state.config, room, identity))
            .await
            .unwrap();
        let before = owner(&state, room, identity).await;
        if close_owner {
            state
                .manager
                .delete_room(&rtc_core::ids::RoomKey::new(
                    rtc_core::ids::ProjectId::local(),
                    room,
                ))
                .await
                .unwrap();
        }
        tokio::time::sleep(Duration::from_millis(2)).await;
        let (mut actual_writer, reader) = tokio::io::duplex(16);
        drop(reader); // Real closed byte-stream peer, not a fabricated Ok/Err.
        let write_attempted = AtomicBool::new(false);
        let forwarded = AtomicBool::new(false);
        let keep_session = tokio::time::timeout(
            Duration::from_secs(1),
            crate::signal::session::reply_then_forward(
                async {
                    let result = actual_writer.write_all(b"actual-pong").await;
                    assert!(result.is_err(), "real closed writer must reject the pong");
                    write_attempted.store(true, Ordering::SeqCst);
                    false
                },
                async {
                    assert!(
                        write_attempted.load(Ordering::SeqCst),
                        "source response attempt precedes owner forwarding"
                    );
                    let result = handle.send(SessionRequest::SignalActivity).await;
                    forwarded.store(result, Ordering::SeqCst);
                    result
                },
            ),
        )
        .await
        .expect("an actually closed owner sink must not wait forever");
        assert!(!keep_session, "failed reply retains session-close behavior");
        assert_eq!(
            forwarded.load(Ordering::SeqCst),
            !close_owner,
            "owner admission is observed, never fabricated after close"
        );
        if !close_owner {
            let after = owner(&state, room, identity).await;
            assert!(
                after.signaling_last_seen > before.signaling_last_seen,
                "failed reply must not suppress actual owner activity"
            );
            assert_eq!(
                after.signaling_rtt_ms, 0,
                "local pong failure cannot invent RTT"
            );
            state
                .manager
                .delete_room(&rtc_core::ids::RoomKey::new(
                    rtc_core::ids::ProjectId::local(),
                    room,
                ))
                .await
                .unwrap();
        }
        drop(handle);
        net.shutdown();
    }
}
