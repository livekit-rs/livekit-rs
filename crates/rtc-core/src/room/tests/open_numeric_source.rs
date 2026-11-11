use super::*;
#[test]
fn actual_core_later_bind_keeps_numeric_source_identity_and_raw_authorization() {
    let (mut room, now) = room();
    let mut join = params("open-source-core", 16, true);
    join.grants.video.can_publish_sources = vec!["99".into()];
    let owner = room.join(join, now).unwrap();
    let effects = room.take_effects();
    let transport = transport_of(&effects, &owner.sid, PcRole::Single);
    let source = TrackSource::from_i32(99);
    room.handle_request(
        &owner.sid,
        owner.generation,
        SessionRequest::AddTrack(add_track("open99", TrackKind::Video, source)),
        now,
    );
    let effects = room.take_effects();
    let sid = signals_to(&effects, &owner.sid)
        .into_iter()
        .find_map(|event| match event {
            SignalEvent::TrackPublished { track, .. } => {
                assert_eq!(track.source.as_i32(), 99);
                assert!(track.sid.0.starts_with("TR_V"));
                Some(track.sid)
            }
            _ => None,
        })
        .unwrap();
    room.media_arrives(
        RemoteMedia {
            registration: crate::media_api::PublicationRegistration::Pending,
            participant: owner.sid.clone(),
            transport,
            mid: "0".into(),
            kind: TrackKind::Video,
            msid_track_id: "open99".into(),
            rids: vec![],
            mime_type: "video/VP8".into(),
        },
        now,
    );
    let effects = room.take_effects();
    assert!(effects.iter().any(|effect|matches!(effect,Effect::Media(MediaCommand::UpdatePublishedTrack{track})if track.sid==sid&&track.source.as_i32()==99)));
    let info = room
        .get_participant(&Identity("open-source-core".into()))
        .unwrap();
    assert_eq!(info.tracks.len(), 1);
    assert_eq!(info.tracks[0].source.as_i32(), 99);
    assert_eq!(
        info.permission.can_publish_sources,
        vec![TrackSource::Unknown],
        "permission string99 still projects UNKNOWN while raw99 authorizes binding"
    );
}
