//! Source state-machine controls; native transport production and public SDK
//! consumers have separate tests, so these events alone are not network proof.
use super::*;
use crate::permission::ParticipantPermission;
fn permission(room: &Room, sid: &ParticipantSid, can_publish: bool) -> ParticipantPermission {
    let mut value = room.participants[sid].info.permission.clone();
    value.can_publish = can_publish;
    value
}
fn update(
    room: &mut Room,
    identity: &str,
    permission: ParticipantPermission,
    now: Instant,
) -> ParticipantInfo {
    room.api_update_participant(
        &identity.into(),
        "",
        "",
        &BTreeMap::new(),
        Some(permission),
        now,
    )
    .unwrap()
}
#[test]
fn publisher_identity_is_sticky_after_real_media_publication_and_last_unpublish() {
    let (mut room, now) = room();
    let owner = room.join(params("publisher", 17, true), now).unwrap();
    let tid = transport_of(&room.take_effects(), &owner.sid, PcRole::Single);
    room.handle_request(
        &owner.sid,
        owner.generation,
        SessionRequest::AddTrack(add_track(
            "audio",
            TrackKind::Audio,
            TrackSource::Microphone,
        )),
        now,
    );
    room.media_arrives(
        RemoteMedia {
            registration: crate::media_api::PublicationRegistration::SdpReceiver,
            participant: owner.sid.clone(),
            transport: tid,
            mid: "0".into(),
            kind: TrackKind::Audio,
            msid_track_id: "audio".into(),
            rids: vec![],
            mime_type: "audio/opus".into(),
        },
        now,
    );
    let track = room.get_participant(&"publisher".into()).unwrap().tracks[0]
        .sid
        .clone();
    assert!(
        room.get_participant(&"publisher".into())
            .unwrap()
            .is_publisher
    );
    room.handle_media_event(
        MediaEvent::RemoteMediaStopped {
            participant: owner.sid.clone(),
            track_sid: track,
        },
        now,
    );
    let info = room.get_participant(&"publisher".into()).unwrap();
    assert!(info.tracks.is_empty());
    assert!(
        info.is_publisher,
        "normal unpublish does not reset original publisher identity"
    );
    assert_eq!(room.info().num_publishers, 1);
    let revoked = permission(&room, &owner.sid, false);
    assert!(!update(&mut room, "publisher", revoked, now).is_publisher);
    assert_eq!(room.info().num_publishers, 0);
}
#[test]
fn data_only_publisher_is_real_but_permission_recomputes_using_current_publisher_pc() {
    let (mut room, now) = room();
    let owner = room.join(params("data-publisher", 17, false), now).unwrap();
    let effects = room.take_effects();
    let publisher = transport_of(&effects, &owner.sid, PcRole::Publisher);
    let subscriber = transport_of(&effects, &owner.sid, PcRole::Subscriber);
    room.handle_request(
        &owner.sid,
        owner.generation,
        SessionRequest::PublishDataTrack(data_publish(1, "source-data")),
        now,
    );
    assert!(
        room.get_participant(&"data-publisher".into())
            .unwrap()
            .is_publisher,
        "fixed participant_data_track.go137 sets publisher on actual data publication"
    );
    let revoked = permission(&room, &owner.sid, false);
    let info = update(&mut room, "data-publisher", revoked, now);
    assert!(!info.is_publisher);
    assert_eq!(
        info.data_tracks.len(),
        1,
        "canPublishData remains independently allowed"
    );
    let granted = permission(&room, &owner.sid, true);
    assert!(
        !update(&mut room, "data-publisher", granted, now).is_publisher,
        "new publisher PC is not established"
    );
    room.handle_media_event(
        MediaEvent::TransportEstablished {
            participant: owner.sid.clone(),
            transport: subscriber,
        },
        now,
    );
    let mut p = permission(&room, &owner.sid, true);
    p.can_subscribe = false;
    assert!(
        !update(&mut room, "data-publisher", p, now).is_publisher,
        "subscriber PC must not stand in for publisher"
    );
    room.handle_media_event(
        MediaEvent::TransportEstablished {
            participant: owner.sid.clone(),
            transport: publisher,
        },
        now,
    );
    let mut p = permission(&room, &owner.sid, true);
    p.can_subscribe = true;
    assert!(
        update(&mut room, "data-publisher", p, now).is_publisher,
        "Checking alone establishes PC before DTLS/ACTIVE"
    );
    assert!(
        !room.participants[&owner.sid].ever_active,
        "the test must not use TransportConnected as proof"
    );
    room.handle_request(
        &owner.sid,
        owner.generation,
        SessionRequest::UnpublishDataTrack {
            token: 0,
            pub_handle: 1,
        },
        now,
    );
    let info = room.get_participant(&"data-publisher".into()).unwrap();
    assert!(info.data_tracks.is_empty());
    assert!(info.is_publisher);
    room.remove_participant(&owner.sid, DisconnectReason::ParticipantRemoved, true, now);
    let replacement = room.join(params("data-publisher", 17, false), now).unwrap();
    room.take_effects();
    assert_ne!(replacement.sid, owner.sid);
    room.handle_media_event(
        MediaEvent::TransportEstablished {
            participant: owner.sid.clone(),
            transport: publisher,
        },
        now,
    );
    room.handle_media_event(
        MediaEvent::TransportEstablished {
            participant: replacement.sid.clone(),
            transport: publisher,
        },
        now,
    );
    let mut p = permission(&room, &replacement.sid, true);
    p.can_subscribe = false;
    let fresh = update(&mut room, "data-publisher", p, now);
    assert!(
        !fresh.is_publisher,
        "old owner SID and old transport cannot establish replacement PC"
    );
    assert!(
        room.participants[&replacement.sid]
            .established_transports
            .is_empty()
    );
}

#[test]
#[ignore = "requires actual fixed Go ORIGINAL_PUBLISHER_IDENTITY_TSV observer log"]
fn original_go_data_and_permission_publisher_fields_match_actual_core_operations() {
    let log = std::fs::read_to_string(
        std::env::var("GO_PUBLISHER_IDENTITY_RESULTS").expect("actual fixed Go observer log path"),
    )
    .unwrap();
    let (mut room, now) = room();
    let joined = room.join(params("publisher", 17, true), now).unwrap();
    let tid = transport_of(&room.take_effects(), &joined.sid, PcRole::Single);
    let mut actual = Vec::new();
    fn push(out: &mut Vec<(String, bool, usize, usize)>, room: &mut Room, name: &str) {
        let p = room.get_participant(&"publisher".into()).unwrap();
        out.push((
            name.to_owned(),
            p.is_publisher,
            p.tracks.len(),
            p.data_tracks.len(),
        ));
    }
    push(&mut actual, &mut room, "new");
    room.handle_request(
        &joined.sid,
        joined.generation,
        SessionRequest::PublishDataTrack(data_publish(1, "source-data")),
        now,
    );
    push(&mut actual, &mut room, "data-publish");
    let revoked = permission(&room, &joined.sid, false);
    update(&mut room, "publisher", revoked, now);
    push(&mut actual, &mut room, "can-publish-false-data-remains");
    let granted = permission(&room, &joined.sid, true);
    update(&mut room, "publisher", granted, now);
    push(&mut actual, &mut room, "can-publish-true-pc-new");
    room.handle_media_event(
        MediaEvent::TransportEstablished {
            participant: joined.sid.clone(),
            transport: tid,
        },
        now,
    );
    let mut p = permission(&room, &joined.sid, true);
    p.can_subscribe = !p.can_subscribe;
    update(&mut room, "publisher", p, now);
    push(
        &mut actual,
        &mut room,
        "permission-recomputed-with-real-native-checking",
    );
    room.handle_request(
        &joined.sid,
        joined.generation,
        SessionRequest::UnpublishDataTrack {
            token: 0,
            pub_handle: 1,
        },
        now,
    );
    push(&mut actual, &mut room, "last-data-unpublish-sticky");
    let expected: Vec<_> = log
        .lines()
        .filter_map(|line| {
            line.split_once("ORIGINAL_PUBLISHER_IDENTITY_TSV=")
                .map(|(_, rest)| rest)
        })
        .map(|rest| {
            let mut fields = rest.split('\t');
            (
                fields.next().unwrap().to_owned(),
                fields.next().unwrap().parse::<bool>().unwrap(),
                fields.next().unwrap().parse::<usize>().unwrap(),
                fields.next().unwrap().trim().parse::<usize>().unwrap(),
            )
        })
        .collect();
    assert_eq!(
        expected.len(),
        6,
        "actual original observer must include every source branch"
    );
    assert_eq!(
        actual, expected,
        "native Checking production is accepted separately, not by this injected state-machine event"
    );
}
