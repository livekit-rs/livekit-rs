//! Fixed LiveKit participant-message byte sizes at the actual adapter boundary.
use prost::Message;
use rtc_core::{model::ParticipantInfo, room::ParticipantUpdateSizer};

pub(crate) struct LiveKitParticipantSizer;
impl ParticipantUpdateSizer for LiveKitParticipantSizer {
    fn encoded_len(&self, participant: &ParticipantInfo) -> usize {
        crate::convert::participant_info_to_proto(participant).encoded_len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rtc_core::{
        config::RoomPolicy,
        data::{DataHandler, DataSender, DataVerdict},
        ids::{Identity, ParticipantSid, ProjectId, RoomKey},
        media_api::DataKind,
        permission::{Grants, VideoGrant},
        room::{Effect, Room},
        session::{ClientCapabilities, ClientMeta, JoinParams, SessionOutput, SignalEvent},
    };
    use std::{
        collections::BTreeMap,
        sync::Arc,
        time::{Duration, Instant},
    };
    struct NoData;
    impl DataHandler for NoData {
        fn inbound(&self, _: &DataSender<'_>, _: DataKind, _: &str, _: &[u8]) -> DataVerdict {
            DataVerdict::Consumed
        }
    }
    fn params(identity: &str) -> JoinParams {
        JoinParams {
            room: RoomKey::new(ProjectId::local(), "batch-real"),
            grants: Grants {
                identity: identity.into(),
                video: VideoGrant {
                    room_join: true,
                    room: "batch-real".into(),
                    ..Default::default()
                },
                ..Default::default()
            },
            identity: Identity(identity.into()),
            name: format!("name-{identity}"),
            metadata: "unicode 東京 and varint-boundary metadata ".repeat(7),
            attributes: BTreeMap::from([("tenant".into(), "actual-protobuf".into())]),
            client: ClientMeta {
                protocol: 17,
                ..Default::default()
            },
            capabilities: ClientCapabilities::default(),
            auto_subscribe: false,
            auto_subscribe_data_track: false,
            adaptive_stream: false,
            single_pc: true,
            disable_ice_lite: false,
            subscriber_allow_pause: None,
            reconnect: false,
            reconnect_reason: 0,
            participant_sid: None,
            add_tracks: vec![],
            publisher_offer: None,
        }
    }
    fn joined_room(target: i64) -> (Room, Instant, ParticipantSid, Vec<usize>) {
        let mut cfg = crate::test_support::config();
        cfg.room.update_batch_target_size = target;
        let now = Instant::now();
        let mut room = Room::new(
            RoomKey::new(ProjectId::local(), "batch-real"),
            Arc::new(RoomPolicy::from(&cfg)),
            Arc::new(NoData),
            &Default::default(),
            now,
        );
        room.set_participant_update_sizer(Arc::new(LiveKitParticipantSizer));
        let mut ids = Vec::new();
        for identity in ["observer", "alpha", "beta"] {
            ids.push(room.join(params(identity), now).unwrap().sid);
        }
        let sizes = ["observer", "alpha", "beta"]
            .into_iter()
            .map(|id| {
                LiveKitParticipantSizer
                    .encoded_len(&room.get_participant(&Identity(id.into())).unwrap())
            })
            .collect();
        room.take_effects();
        (room, now, ids.remove(0), sizes)
    }
    fn actual_chunks(
        room: &mut Room,
        now: Instant,
        observer: &ParticipantSid,
    ) -> Vec<Vec<lk_protocol::livekit::ParticipantInfo>> {
        room.tick(now + Duration::from_secs(3));
        room.take_effects()
            .into_iter()
            .filter_map(|effect| match effect {
                Effect::Signal {
                    to,
                    event: SignalEvent::ParticipantUpdate(infos),
                } if &to == observer => Some(
                    infos
                        .iter()
                        .map(crate::convert::participant_info_to_proto)
                        .collect(),
                ),
                _ => None,
            })
            .collect()
    }
    #[test]
    fn actual_join_broadcast_obeys_signed_zero_oversize_and_exact_protobuf_boundaries() {
        let (_, _, _, sizes) = joined_room(i64::MAX);
        let exact_first_pair = (sizes[0] + sizes[1]) as i64;
        for (target, counts) in [
            (-1, vec![1, 1, 1]),
            (0, vec![1, 1, 1]),
            (1, vec![1, 1, 1]),
            (exact_first_pair, vec![2, 1]),
            (i64::MAX, vec![3]),
        ] {
            let (mut room, now, observer, actual_sizes) = joined_room(target);
            assert_eq!(
                actual_sizes, sizes,
                "actual fixed-width SID/time projection sizes must match the fixture boundary"
            );
            let chunks = actual_chunks(&mut room, now, &observer);
            assert_eq!(
                chunks.iter().map(Vec::len).collect::<Vec<_>>(),
                counts,
                "target={target}"
            );
            let identities = chunks
                .iter()
                .flatten()
                .map(|p| p.identity.as_str())
                .collect::<Vec<_>>();
            assert_eq!(
                identities,
                ["observer", "alpha", "beta"],
                "no participant can be dropped or reordered"
            );
            for chunk in &chunks {
                let bytes: usize = chunk.iter().map(Message::encoded_len).sum();
                assert!(chunk.len() == 1 || bytes as i128 <= target as i128);
                // The wire wrapper overhead is deliberately outside Go's target.
                let encoded = lk_protocol::livekit::SignalResponse {
                    message: Some(lk_protocol::livekit::signal_response::Message::Update(
                        lk_protocol::livekit::ParticipantUpdate {
                            participants: chunk.clone(),
                        },
                    )),
                }
                .encode_to_vec();
                let parsed =
                    lk_protocol::livekit::SignalResponse::decode(encoded.as_slice()).unwrap();
                assert!(
                    matches!(parsed.message, Some(lk_protocol::livekit::signal_response::Message::Update(update)) if update.participants == *chunk)
                );
            }
        }
    }
    #[test]
    fn actual_batch_keeps_hidden_filter_and_next_tick_does_not_repeat_old_entries() {
        let (mut room, now, observer, _) = joined_room(i64::MAX);
        let mut hidden = params("hidden");
        hidden.grants.video.hidden = true;
        let hidden = room.join(hidden, now).unwrap();
        room.take_effects();
        let chunks = actual_chunks(&mut room, now, &observer);
        assert_eq!(chunks.len(), 1);
        assert!(chunks[0].iter().all(|p| p.sid != hidden.sid.0));
        assert_eq!(chunks[0].len(), 3);
        assert!(actual_chunks(&mut room, now + Duration::from_secs(3), &observer).is_empty());
    }
    #[test]
    fn immediate_real_metadata_update_dequeues_only_its_pending_member() {
        let (mut room, now, observer, _) = joined_room(i64::MAX);
        room.api_update_participant(
            &Identity("alpha".into()),
            "",
            "updated immediately",
            &BTreeMap::new(),
            None,
            now,
        )
        .unwrap();
        let immediate = room.take_effects();
        assert!(immediate.iter().any(|effect| matches!(effect,
            Effect::Signal { to, event: SignalEvent::ParticipantUpdate(infos) }
            if to == &observer && infos.iter().any(|p| p.identity.0 == "alpha" && p.metadata == "updated immediately"))));
        let chunks = actual_chunks(&mut room, now, &observer);
        assert_eq!(
            chunks
                .iter()
                .flatten()
                .map(|p| p.identity.as_str())
                .collect::<Vec<_>>(),
            ["observer", "beta"],
            "source immediate delivery removes alpha's queued callback; unrelated pending members survive"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn actual_application_assembly_injects_sizer_into_room_actor() {
        use rtc_core::events::NullSink;
        use rtc_runtime::net::{LocalInterface, NetConfig, NetRouter};
        let net = NetRouter::start(NetConfig {
            udp_enabled: false,
            udp_port: None,
            port_range: (0, 0),
            tcp_port: None,
            interfaces: vec![LocalInterface {
                bind: std::net::Ipv4Addr::LOCALHOST.into(),
                advertise: std::net::Ipv4Addr::LOCALHOST.into(),
            }],
        })
        .await
        .unwrap();
        let mut cfg = crate::test_support::config();
        cfg.room.update_batch_target_size = 0;
        let state = crate::state::CompatServices::new(Arc::new(cfg))
            .assemble(net.clone(), Arc::new(NullSink));
        let mut observer = state.manager().join(params("observer")).await.unwrap();
        let alpha = state.manager().join(params("alpha")).await.unwrap();
        let beta = state.manager().join(params("beta")).await.unwrap();
        while observer.output.try_recv().is_ok() {}
        // Room creation currently samples std::Instant; advance past the
        // three-second batch edge so fixture setup time cannot straddle it.
        tokio::time::advance(Duration::from_secs(4)).await;
        let mut updates = Vec::new();
        for _ in 0..20 {
            tokio::task::yield_now().await;
            while let Ok(event) = observer.output.try_recv() {
                if let SessionOutput::Signal(SignalEvent::ParticipantUpdate(infos)) = event {
                    updates.push(infos);
                }
            }
            if updates.len() >= 3 {
                break;
            }
        }
        assert_eq!(
            updates.iter().map(Vec::len).collect::<Vec<_>>(),
            [1, 1, 1],
            "real owner consumes target=0; a projected-only setting would send one 3-participant batch"
        );
        assert_eq!(
            updates
                .iter()
                .flatten()
                .map(|p| p.identity.0.as_str())
                .collect::<Vec<_>>(),
            ["observer", "alpha", "beta"]
        );
        state
            .manager()
            .delete_room(&params("observer").room)
            .await
            .unwrap();
        drop((observer, alpha, beta));
        state.agents().shutdown().await;
        net.shutdown();
    }
}
