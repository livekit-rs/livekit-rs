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
