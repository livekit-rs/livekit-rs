//! Fixed Go publication visibility (pion OnTrack with FireOnTrackBySdp): a declared-SSRC SDP section
//! becomes visible once the publisher transport is connected (DTLS), RID / undeclared sections on
//! their first RTP; a publisher whose transport never connects never exposes a track ("ghost track").
use super::*;

fn audio(
    sid: &ParticipantSid,
    transport: TransportId,
    registration: crate::media_api::PublicationRegistration,
    rids: Vec<String>,
) -> RemoteMedia {
    RemoteMedia {
        registration,
        receiver_ssrc: None,
        participant: sid.clone(),
        transport,
        mid: "0".into(),
        kind: TrackKind::Audio,
        msid_track_id: "cid".into(),
        rids,
        mime_type: "audio/opus".into(),
    }
}

fn setup() -> (Room, Instant, Joined, TransportId) {
    let (mut room, now) = room();
    let alice = room.join(params("alice", 16, true), now).unwrap();
    let transport = transport_of(&room.take_effects(), &alice.sid, PcRole::Single);
    room.handle_request(
        &alice.sid,
        alice.generation,
        SessionRequest::AddTrack(add_track("cid", TrackKind::Audio, TrackSource::Microphone)),
        now,
    );
    room.take_effects();
    (room, now, alice, transport)
}

fn connected(room: &mut Room, sid: &ParticipantSid, transport: TransportId, now: Instant) {
    room.handle_media_event(
        MediaEvent::TransportConnected {
            participant: sid.clone(),
            transport,
        },
        now,
    );
}

#[test]
fn sdp_receiver_is_visible_only_after_the_publisher_transport_connects() {
    use crate::media_api::PublicationRegistration::SdpReceiver;
    let (mut room, now, alice, transport) = setup();
    room.handle_media_event(
        MediaEvent::RemoteMediaAdded(audio(&alice.sid, transport, SdpReceiver, vec![])),
        now,
    );
    let effects = room.take_effects();
    assert!(
        effects
            .iter()
            .any(|e| matches!(e, Effect::Media(MediaCommand::BindTrack { .. }))),
        "receiver is bound so media can arrive"
    );
    assert_eq!(room.published_track_count(), 0, "not visible before DTLS");
    assert!(!effects.iter().any(|e| matches!(
        e,
        Effect::Event(crate::events::EngineEvent::TrackPublished { .. })
    )));
    connected(&mut room, &alice.sid, transport, now);
    assert_eq!(room.published_track_count(), 1);
    assert!(room.take_effects().iter().any(|e| matches!(
        e,
        Effect::Event(crate::events::EngineEvent::TrackPublished { .. })
    )));
}

#[test]
fn renegotiated_sdp_receiver_on_a_connected_transport_is_visible_immediately() {
    use crate::media_api::PublicationRegistration::SdpReceiver;
    let (mut room, now, alice, transport) = setup();
    connected(&mut room, &alice.sid, transport, now);
    room.take_effects();
    room.handle_media_event(
        MediaEvent::RemoteMediaAdded(audio(&alice.sid, transport, SdpReceiver, vec![])),
        now,
    );
    assert_eq!(room.published_track_count(), 1);
}

#[test]
fn rid_or_undeclared_section_waits_for_its_first_rtp_even_when_connected() {
    use crate::media_api::PublicationRegistration::Pending;
    let (mut room, now, alice, transport) = setup();
    connected(&mut room, &alice.sid, transport, now);
    room.handle_media_event(
        MediaEvent::RemoteMediaAdded(audio(&alice.sid, transport, Pending, vec![])),
        now,
    );
    assert_eq!(
        room.published_track_count(),
        0,
        "pion OnTrack for undeclared/RID sections needs RTP"
    );
    let track = room.participants[&alice.sid].deferred_publications[0]
        .info
        .sid
        .clone();
    room.handle_media_event(
        MediaEvent::TrackMediaStarted {
            track_sid: track,
            mime_type: "audio/opus".into(),
            layers: vec![],
        },
        now,
    );
    assert_eq!(room.published_track_count(), 1);
}

#[test]
fn publisher_whose_transport_never_connects_never_exposes_the_track() {
    use crate::media_api::PublicationRegistration::SdpReceiver;
    let (mut room, now, alice, transport) = setup();
    let bob = room.join(params("bob", 16, true), now).unwrap();
    room.take_effects();
    room.handle_media_event(
        MediaEvent::RemoteMediaAdded(audio(&alice.sid, transport, SdpReceiver, vec![])),
        now,
    );
    room.tick(now + Duration::from_secs(5));
    let effects = room.take_effects();
    assert_eq!(room.published_track_count(), 0);
    assert!(
        !signals_to(&effects, &bob.sid)
            .iter()
            .any(|s| matches!(s, SignalEvent::ParticipantUpdate(infos)
        if infos.iter().any(|i| i.sid == alice.sid && !i.tracks.is_empty()))),
        "bob never sees alice's track"
    );
}

#[test]
fn section_stopped_before_visibility_is_released_without_a_publication() {
    use crate::media_api::PublicationRegistration::SdpReceiver;
    let (mut room, now, alice, transport) = setup();
    room.handle_media_event(
        MediaEvent::RemoteMediaAdded(audio(&alice.sid, transport, SdpReceiver, vec![])),
        now,
    );
    let track = room.participants[&alice.sid].deferred_publications[0]
        .info
        .sid
        .clone();
    room.take_effects();
    room.handle_media_event(
        MediaEvent::RemoteMediaStopped {
            participant: alice.sid.clone(),
            track_sid: track.clone(),
        },
        now,
    );
    let effects = room.take_effects();
    assert!(effects.iter().any(|e| matches!(e, Effect::Media(MediaCommand::UnpublishTrack { track_sid }) if *track_sid == track)));
    assert!(
        !effects.iter().any(|e| matches!(
            e,
            Effect::Event(crate::events::EngineEvent::TrackUnpublished { .. })
        )),
        "never published, no unpublish event"
    );
    connected(&mut room, &alice.sid, transport, now);
    assert_eq!(
        room.published_track_count(),
        0,
        "a cancelled section is not published later"
    );
}

fn video_metadata_setup(
    rid: &str,
    ssrc: u32,
    repair_ssrc: u32,
) -> (Room, Instant, Joined, TransportId, TrackInfo) {
    let (mut room, now) = room();
    let alice = room.join(params("alice", 16, true), now).unwrap();
    let transport = transport_of(&room.take_effects(), &alice.sid, PcRole::Single);
    let mut request = add_track("cid", TrackKind::Video, TrackSource::Camera);
    request.layers = vec![VideoLayer {
        quality: VideoQuality::High,
        width: 1280,
        height: 720,
        bitrate: 800_000,
        ssrc,
        spatial_layer: 2,
        rid: rid.into(),
        repair_ssrc,
    }];
    request.simulcast_codecs = vec![SimulcastCodecRequest {
        codec: "VP8".into(),
        cid: "cid".into(),
        layers: request.layers.clone(),
        layer_mode: VideoLayerMode::OneSpatialLayerPerStream,
    }];
    room.handle_request(
        &alice.sid,
        alice.generation,
        SessionRequest::AddTrack(request),
        now,
    );
    let tracks = signals_to(&room.take_effects(), &alice.sid)
        .into_iter()
        .filter_map(|event| match event {
            SignalEvent::TrackPublished { track, .. } => Some(track),
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(tracks.len(), 1);
    assert_eq!(room.published_track_count(), 0);
    (
        room,
        now,
        alice,
        transport,
        tracks.into_iter().next().unwrap(),
    )
}

fn video_media(
    alice: &Joined,
    transport: TransportId,
    registration: crate::media_api::PublicationRegistration,
) -> RemoteMedia {
    RemoteMedia {
        registration,
        receiver_ssrc: None,
        participant: alice.sid.clone(),
        transport,
        mid: "0".into(),
        kind: TrackKind::Video,
        msid_track_id: "cid".into(),
        rids: vec![],
        mime_type: "video/VP8".into(),
    }
}

fn publication_tracks(effects: &[Effect]) -> Vec<TrackInfo> {
    effects
        .iter()
        .filter_map(|effect| match effect {
            Effect::Event(crate::events::EngineEvent::TrackPublished { track, .. }) => {
                Some(track.clone())
            }
            _ => None,
        })
        .collect()
}

#[test]
fn sdp_publication_without_rids_clears_signalled_layer_rids() {
    use crate::media_api::PublicationRegistration::SdpReceiver;
    for rid in ["", "f"] {
        let (mut room, now, alice, transport, ack) = video_metadata_setup(rid, 17, 19);
        connected(&mut room, &alice.sid, transport, now);
        room.take_effects();
        room.handle_media_event(
            MediaEvent::RemoteMediaAdded(video_media(&alice, transport, SdpReceiver)),
            now,
        );
        let tracks = publication_tracks(&room.take_effects());
        assert_eq!(tracks.len(), 1);
        let mut expected = ack.layers;
        expected[0].rid.clear();
        expected[0].spatial_layer = 0;
        assert_eq!(
            tracks[0].layers, expected,
            "top-level signalled RID={rid:?}"
        );
        assert_eq!(
            tracks[0].codecs[0].layers, expected,
            "codec signalled RID={rid:?}"
        );
    }
}

#[test]
fn sdp_receiver_identity_precedes_publication_and_preserves_signalled_ssrc() {
    use crate::media_api::PublicationRegistration::SdpReceiver;
    for already_connected in [false, true] {
        for (ssrc, repair, received_repair, expected_ssrc, expected_repair) in [
            (0, 0, 29, 23, 29),
            (0, 19, 0, 23, 19),
            (17, 19, 29, 17, 19),
            (17, 0, 29, 17, 0),
        ] {
            let (mut room, now, alice, transport, ack) = video_metadata_setup("f", ssrc, repair);
            if already_connected {
                connected(&mut room, &alice.sid, transport, now);
            }
            room.take_effects();
            let mut media = video_media(&alice, transport, SdpReceiver);
            media.receiver_ssrc = Some((23, received_repair));
            room.handle_media_event(MediaEvent::RemoteMediaAdded(media), now);
            let effects = room.take_effects();
            let updated = effects
                .iter()
                .find_map(|effect| match effect {
                    Effect::Media(MediaCommand::UpdatePublishedTrack { track }) => Some(track),
                    _ => None,
                })
                .unwrap();
            let mut expected = ack.layers.clone();
            expected[0].rid.clear();
            expected[0].spatial_layer = 0;
            expected[0].ssrc = expected_ssrc;
            expected[0].repair_ssrc = expected_repair;
            assert_eq!(updated.layers, expected);
            assert_eq!(updated.codecs[0].layers, expected);
            let tracks = if already_connected {
                publication_tracks(&effects)
            } else {
                assert!(publication_tracks(&effects).is_empty());
                connected(&mut room, &alice.sid, transport, now);
                publication_tracks(&room.take_effects())
            };
            assert_eq!(tracks.len(), 1);
            assert_eq!(tracks[0].sid, ack.sid);
            assert_eq!(tracks[0].mime_type, "video/VP8");
            assert_eq!(tracks[0].codecs[0].mime_type, "video/VP8");
            assert_eq!(tracks[0].layers, expected);
            assert_eq!(tracks[0].codecs[0].layers, expected);
            assert_eq!(room.participants[&alice.sid].info.tracks[0], tracks[0]);
        }
    }
}

#[test]
fn first_rtp_publication_contains_receiver_metadata_before_emitting_snapshot() {
    use crate::media_api::PublicationRegistration::Pending;
    let (mut room, now, alice, transport, ack) = video_metadata_setup("f", 17, 19);
    connected(&mut room, &alice.sid, transport, now);
    let bob = room.join(params("bob", 16, true), now).unwrap();
    room.take_effects();
    room.handle_media_event(
        MediaEvent::RemoteMediaAdded(video_media(&alice, transport, Pending)),
        now,
    );
    assert_eq!(room.published_track_count(), 0);
    assert!(publication_tracks(&room.take_effects()).is_empty());
    let mut expected = ack.layers.clone();
    expected[0].rid.clear();
    expected[0].spatial_layer = 0;
    expected[0].ssrc = 23;
    expected[0].repair_ssrc = 29;
    room.handle_media_event(
        MediaEvent::TrackMediaStarted {
            track_sid: ack.sid.clone(),
            mime_type: "video/vp8".into(),
            layers: expected.clone(),
        },
        now,
    );
    let effects = room.take_effects();
    let tracks = publication_tracks(&effects);
    assert_eq!(tracks.len(), 1);
    let observer_tracks = signals_to(&effects, &bob.sid)
        .into_iter()
        .filter_map(|event| match event {
            SignalEvent::ParticipantUpdate(infos) => infos
                .into_iter()
                .find(|info| info.sid == alice.sid)
                .map(|info| info.tracks),
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(observer_tracks, vec![tracks.clone()]);
    assert_eq!(tracks[0].sid, ack.sid);
    assert_eq!(tracks[0].layers, expected);
    assert_eq!(tracks[0].mime_type, "video/vp8");
    assert_eq!(tracks[0].codecs[0].layers, expected);
    assert_eq!(tracks[0].codecs[0].mime_type, "video/vp8");
    assert_eq!(tracks[0].codecs[0].mid, "0");
    assert_eq!(tracks[0].codecs[0].cid, "cid");
    assert_eq!(room.participants[&alice.sid].info.tracks[0], tracks[0]);
    room.handle_media_event(
        MediaEvent::TrackMediaStarted {
            track_sid: ack.sid,
            mime_type: "video/vp8".into(),
            layers: expected,
        },
        now,
    );
    let duplicate = room.take_effects();
    assert!(publication_tracks(&duplicate).is_empty());
    assert!(signals_to(&duplicate, &bob.sid).is_empty());
    assert_eq!(room.participants[&alice.sid].info.tracks[0], tracks[0]);
    assert_eq!(room.published_track_count(), 1);
}

#[test]
fn cancelled_deferred_video_ignores_late_receiver_metadata() {
    use crate::media_api::PublicationRegistration::Pending;
    let (mut room, now, alice, transport, ack) = video_metadata_setup("f", 17, 19);
    connected(&mut room, &alice.sid, transport, now);
    room.handle_media_event(
        MediaEvent::RemoteMediaAdded(video_media(&alice, transport, Pending)),
        now,
    );
    assert_eq!(room.participants[&alice.sid].deferred_publications.len(), 1);
    room.handle_media_event(
        MediaEvent::RemoteMediaStopped {
            participant: alice.sid.clone(),
            track_sid: ack.sid.clone(),
        },
        now,
    );
    assert!(
        room.participants[&alice.sid]
            .deferred_publications
            .is_empty()
    );
    room.take_effects();
    room.handle_media_event(
        MediaEvent::TrackMediaStarted {
            track_sid: ack.sid,
            mime_type: "video/vp8".into(),
            layers: ack.layers,
        },
        now,
    );
    assert!(room.take_effects().is_empty());
    assert_eq!(room.published_track_count(), 0);
    assert!(room.participants[&alice.sid].info.tracks.is_empty());
}

#[test]
fn raw_rid_registration_publishes_complete_metadata_and_completes_supervision() {
    use crate::media_api::PublicationRegistration::Pending;
    for backup in [false, true] {
        let (mut room, now, alice, transport, ack) = video_metadata_setup("f", 0, 0);
        Arc::make_mut(&mut room.policy).reconnect_on_publication_error = true;
        connected(&mut room, &alice.sid, transport, now);
        let bob = room.join(params("bob", 16, true), now).unwrap();
        let mut media = video_media(&alice, transport, Pending);
        media.rids = vec!["f".into()];
        room.handle_media_event(MediaEvent::RemoteMediaAdded(media), now);
        let deferred = &mut room
            .participants
            .get_mut(&alice.sid)
            .unwrap()
            .deferred_publications[0];
        if backup {
            let mut codec = deferred.info.codecs[0].clone();
            codec.mime_type = "video/VP9".into();
            codec.cid = "backup-cid".into();
            codec.mid = "1".into();
            deferred.info.codecs.push(codec);
        }
        deferred.info.version = crate::model::TimedVersion {
            unix_micro: i64::MAX,
            ticks: 7,
        };
        let before = deferred.info.clone();
        let mut expected = before.layers.clone();
        expected[0].rid = "f".into();
        expected[0].spatial_layer = 0;
        expected[0].ssrc = 23;
        expected[0].repair_ssrc = 29;
        let mime = if backup { "video/VP9" } else { "video/VP8" };
        room.take_effects();
        let registered = || MediaEvent::PublicationRtpRegistered {
            participant: alice.sid.clone(),
            transport,
            track_sid: ack.sid.clone(),
            mime_type: mime.into(),
            payload_type: 96,
            receiver_layers: Some(expected.clone()),
        };
        room.handle_media_event(registered(), now);
        let effects = room.take_effects();
        let tracks = publication_tracks(&effects);
        assert_eq!(tracks.len(), 1);
        let mut complete = before.clone();
        complete.codecs[usize::from(backup)].layers = expected.clone();
        if !backup {
            complete.layers = expected.clone();
        }
        complete.version.advance();
        assert_eq!(tracks[0], complete);
        assert_eq!(room.participants[&alice.sid].info.tracks, tracks);
        let observer_tracks = signals_to(&effects, &bob.sid)
            .into_iter()
            .filter_map(|event| match event {
                SignalEvent::ParticipantUpdate(infos) => infos
                    .into_iter()
                    .find(|info| info.sid == alice.sid)
                    .map(|info| info.tracks),
                _ => None,
            })
            .collect::<Vec<_>>();
        assert_eq!(observer_tracks, vec![tracks.clone()]);
        assert!(
            !room.publication_monitors[&(alice.sid.clone(), ack.sid.clone())]
                .timed_out(now + Duration::from_secs(31))
        );
        room.handle_media_event(registered(), now);
        assert!(
            room.take_effects().is_empty(),
            "duplicate registration is idempotent"
        );
        room.tick(now + Duration::from_secs(31));
        assert!(room.participants.contains_key(&alice.sid));
        assert!(
            !signals_to(&room.take_effects(), &alice.sid)
                .iter()
                .any(|event| matches!(event, SignalEvent::Leave { .. }))
        );
    }
}

#[test]
fn raw_rid_registration_rejects_stale_invalid_and_canceled_publications() {
    use crate::media_api::PublicationRegistration::Pending;
    let (mut room, now, alice, transport, ack) = video_metadata_setup("f", 0, 0);
    room.participants
        .get_mut(&alice.sid)
        .unwrap()
        .capabilities
        .video_publication_requires_rtp = true;
    connected(&mut room, &alice.sid, transport, now);
    let mut media = video_media(&alice, transport, Pending);
    media.rids = vec!["f".into()];
    room.handle_media_event(MediaEvent::RemoteMediaAdded(media), now);
    let before = room.participants[&alice.sid].deferred_publications[0]
        .info
        .clone();
    let mut layers = before.layers.clone();
    layers[0].ssrc = 23;
    layers[0].repair_ssrc = 29;
    room.take_effects();
    for invalid in 0..9 {
        if invalid == 8 {
            room.participants.get_mut(&alice.sid).unwrap().info.state =
                ParticipantState::Disconnected;
        }
        room.handle_media_event(
            MediaEvent::PublicationRtpRegistered {
                participant: if invalid == 0 {
                    ParticipantSid::from("PA_old")
                } else {
                    alice.sid.clone()
                },
                transport: if invalid == 1 {
                    TransportId::next()
                } else {
                    transport
                },
                track_sid: if invalid == 2 {
                    TrackSid::from("TR_old")
                } else {
                    ack.sid.clone()
                },
                mime_type: match invalid {
                    4 => "audio/opus",
                    5 => "video/VP9",
                    _ => "video/VP8",
                }
                .into(),
                payload_type: if invalid == 3 { 0 } else { 96 },
                receiver_layers: match invalid {
                    6 => Some(vec![]),
                    7 => None,
                    _ => Some(layers.clone()),
                },
            },
            now,
        );
        assert_eq!(
            room.participants[&alice.sid].deferred_publications[0].info,
            before
        );
        assert_eq!(room.published_track_count(), 0);
        assert!(
            room.publication_monitors[&(alice.sid.clone(), ack.sid.clone())]
                .timed_out(now + Duration::from_secs(31))
        );
        assert!(room.take_effects().is_empty(), "invalid case {invalid}");
    }
    room.participants.get_mut(&alice.sid).unwrap().info.state = ParticipantState::Active;
    room.handle_media_event(
        MediaEvent::RemoteMediaStopped {
            participant: alice.sid.clone(),
            track_sid: ack.sid.clone(),
        },
        now,
    );
    room.take_effects();
    room.handle_media_event(
        MediaEvent::PublicationRtpRegistered {
            participant: alice.sid.clone(),
            transport,
            track_sid: ack.sid.clone(),
            mime_type: "video/VP8".into(),
            payload_type: 96,
            receiver_layers: Some(layers),
        },
        now,
    );
    assert!(
        room.participants[&alice.sid]
            .deferred_publications
            .is_empty()
    );
    assert_eq!(room.published_track_count(), 0);
    assert!(
        !room
            .publication_monitors
            .contains_key(&(alice.sid, ack.sid))
    );
    assert!(room.take_effects().is_empty());
}

#[test]
fn sdp_preference_does_not_replace_declared_codec_identity() {
    use crate::media_api::PublicationRegistration::SdpReceiver;
    let (mut room, now, alice, transport, ack) = video_metadata_setup("f", 0, 0);
    connected(&mut room, &alice.sid, transport, now);
    room.take_effects();
    let mut media = video_media(&alice, transport, SdpReceiver);
    media.mime_type = "video/VP9".into();
    room.handle_media_event(MediaEvent::RemoteMediaAdded(media), now);
    let effects = room.take_effects();
    let published = publication_tracks(&effects);
    assert_eq!(published.len(), 1);
    assert_eq!(published[0].codecs[0].mime_type, ack.codecs[0].mime_type);
    let updated = effects
        .iter()
        .find_map(|effect| match effect {
            Effect::Media(MediaCommand::UpdatePublishedTrack { track }) => Some(track),
            _ => None,
        })
        .unwrap();
    assert_eq!(updated.codecs[0].mime_type, ack.codecs[0].mime_type);
}

fn check_no_rid_layer_presence(qualities: &[VideoQuality], spatial: &[i32]) {
    use crate::media_api::PublicationRegistration::SdpReceiver;
    let (mut room, now) = room();
    let alice = room.join(params("alice", 16, true), now).unwrap();
    let transport = transport_of(&room.take_effects(), &alice.sid, PcRole::Single);
    let mut request = add_track("cid", TrackKind::Video, TrackSource::Camera);
    request.layers = qualities
        .iter()
        .enumerate()
        .map(|(i, quality)| VideoLayer {
            quality: *quality,
            width: 320 * (i as u32 + 1),
            height: 180 * (i as u32 + 1),
            bitrate: 100_000 * (i as u32 + 1),
            ssrc: 0,
            spatial_layer: 17,
            rid: format!("signalled-{i}"),
            repair_ssrc: 0,
        })
        .collect();
    request.simulcast_codecs = vec![SimulcastCodecRequest {
        codec: "VP8".into(),
        cid: "cid".into(),
        layers: request.layers.clone(),
        layer_mode: VideoLayerMode::OneSpatialLayerPerStream,
    }];
    let mut expected = request.layers.clone();
    assert_eq!(expected.len(), spatial.len());
    for (layer, spatial) in expected.iter_mut().zip(spatial) {
        layer.spatial_layer = *spatial;
        layer.rid.clear();
    }
    if let Some(layer) = expected.iter_mut().find(|layer| layer.spatial_layer == 0) {
        layer.ssrc = 424242;
        layer.repair_ssrc = 424243;
    }
    room.handle_request(
        &alice.sid,
        alice.generation,
        SessionRequest::AddTrack(request),
        now,
    );
    connected(&mut room, &alice.sid, transport, now);
    room.take_effects();
    let mut media = video_media(&alice, transport, SdpReceiver);
    media.receiver_ssrc = Some((424242, 424243));
    room.handle_media_event(MediaEvent::RemoteMediaAdded(media), now);
    let effects = room.take_effects();
    let tracks = publication_tracks(&effects);
    assert_eq!(tracks.len(), 1);
    assert_eq!(tracks[0].layers, expected);
    assert_eq!(tracks[0].codecs[0].layers, expected);
    let updated = effects
        .iter()
        .find_map(|effect| match effect {
            Effect::Media(MediaCommand::UpdatePublishedTrack { track }) => Some(track),
            _ => None,
        })
        .unwrap();
    assert_eq!(updated.layers, expected);
    assert_eq!(updated.codecs[0].layers, expected);
}

#[test]
fn no_rid_sparse_qualities_use_present_spatial_indices() {
    use VideoQuality::{High, Low, Medium};
    check_no_rid_layer_presence(&[Low, High], &[0, 1]);
    check_no_rid_layer_presence(&[Medium, High], &[0, 1]);
}

#[test]
fn no_rid_duplicate_qualities_share_spatial_without_dropping_metadata() {
    use VideoQuality::{High, Low};
    check_no_rid_layer_presence(&[Low, Low, High], &[0, 0, 1]);
}

#[test]
fn no_rid_unknown_quality_does_not_acquire_a_spatial_index() {
    use VideoQuality::{High, Low};
    check_no_rid_layer_presence(&[Low, High, VideoQuality::from_i32(99)], &[0, 1, -1]);
}
