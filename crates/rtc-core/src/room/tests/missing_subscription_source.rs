//! Exact source resolver observation reader, not a RoomTask scheduling proof.
use super::*;
#[test]
#[ignore = "requires actual fixed Go5 source output normalized losslessly to TSV"]
fn actual_go_missing_resolver_observations_match_core_resolver() {
    let text = std::fs::read_to_string(
        std::env::var("GO_MISSING_SUBSCRIPTION_TSV").expect("actual source rows"),
    )
    .unwrap();
    let mut state: Option<(String, Room, Joined, Instant)> = None;
    let mut cases = std::collections::BTreeSet::new();
    let mut row_count = 0;
    let mut tally = (0usize, false);
    for line in text.lines() {
        let row: Vec<_> = line.split('\t').collect();
        assert_eq!(row.len(), 10);
        let id = row[0];
        let at_ns: u64 = row[2].parse().unwrap();
        let op = row[3];
        if state.as_ref().is_none_or(|(name, _, _, _)| name != id) {
            assert!(
                cases.insert(id.to_owned()),
                "case rows must remain contiguous"
            );
            let (mut room, at) = room();
            Arc::make_mut(&mut room.policy).reconnect_on_subscription_error = true;
            let sub = room.join(params("subscriber", 17, true), at).unwrap();
            let transport = transport_of(&room.take_effects(), &sub.sid, PcRole::Single);
            room.handle_media_event(
                MediaEvent::TransportConnected {
                    participant: sub.sid.clone(),
                    transport,
                },
                at,
            );
            room.take_effects();
            state = Some((id.into(), room, sub, at));
        }
        let (_, room, sub, origin) = state.as_mut().unwrap();
        let now = *origin + Duration::from_nanos(at_ns);
        let track = TrackSid::from("TR_missing");
        match op {
            "subscribe" => {
                room.api_update_subscriptions(
                    &Identity::from("subscriber"),
                    std::slice::from_ref(&track),
                    true,
                    now,
                )
                .unwrap();
            }
            "unsubscribe" => {
                room.api_update_subscriptions(
                    &Identity::from("subscriber"),
                    std::slice::from_ref(&track),
                    false,
                    now,
                )
                .unwrap();
            }
            "deny" => {
                room.participants
                    .get_mut(&sub.sid)
                    .unwrap()
                    .info
                    .permission
                    .can_subscribe = false
            }
            "allow" => {
                room.participants
                    .get_mut(&sub.sid)
                    .unwrap()
                    .info
                    .permission
                    .can_subscribe = true
            }
            "disconnect" => {
                room.participants.get_mut(&sub.sid).unwrap().info.state =
                    ParticipantState::Disconnected
            }
            "connect" => {
                room.participants.get_mut(&sub.sid).unwrap().info.state = ParticipantState::Active
            }
            "tick" => {
                if room.participants.get(&sub.sid).unwrap().info.state
                    != ParticipantState::Disconnected
                    && room
                        .participants
                        .get(&sub.sid)
                        .unwrap()
                        .subscriptions
                        .contains_key(&track)
                {
                    // Source observer manually drives original reconcileSubscription, likewise
                    // compare this private resolver without pretending an arbitrary tick is3s.
                    room.resolve_subscription(&sub.sid, &track, now);
                }
            }
            _ => panic!("unknown original source operation"),
        }
        let expected_desired: bool = row[4].parse().unwrap();
        let callback_count: usize = row[6].parse().unwrap();
        let expected_fatal: bool = row[7].parse().unwrap();
        assert_eq!(row[8], "3000000000");
        assert_eq!(row[9], "60000000000");
        let desired = room
            .participants
            .get(&sub.sid)
            .unwrap()
            .subscriptions
            .contains_key(&track);
        assert_eq!(desired, expected_desired, "{id} row{} desired", row[1]);
        // Effects are cumulative for the source callback list, not just this action.
        let current = room.take_effects();
        if row[1] == "0" {
            tally = (0, false)
        };
        for e in signals_to(&current, &sub.sid) {
            match e {
                SignalEvent::SubscriptionResponse {
                    track_sid,
                    error: SubscriptionError::TrackNotFound,
                } if track_sid == track => tally.0 += 1,
                SignalEvent::Leave { .. } => tally.1 = true,
                _ => {}
            }
        }
        assert_eq!(tally.0, callback_count, "{id} source callback count");
        assert_eq!(tally.1, expected_fatal, "{id} source fatal/reconnect bit");
        row_count += 1;
    }
    assert_eq!(cases.len(), 5);
    assert_eq!(row_count, 21, "all original five cases/rows required");
}
