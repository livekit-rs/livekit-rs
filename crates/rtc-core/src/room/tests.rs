//! Room state machine tests (not executed yet: compile/run after review).

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use super::*;
use crate::config::*;
use crate::data::{DataHandler, DataSender, DataVerdict};
use crate::media_api::{DataKind, MediaCommand, MediaEvent, PcRole, RemoteMedia};
use crate::permission::{Grants, VideoGrant};
use crate::session::*;

struct EchoData;

impl DataHandler for EchoData {
    fn inbound(&self, _s: &DataSender<'_>, _k: DataKind, _l: &str, raw: &[u8]) -> DataVerdict {
        DataVerdict::Forward {
            payload: raw.to_vec(),
            destination_identities: Vec::new(),
            destination_sids: Vec::new(),
            sequence: 0,
            nonce: None,
        }
    }
}

fn test_policy() -> RoomPolicy {
    RoomPolicy {
        transport_fallback: Default::default(),
        enabled_codecs: vec![
            CodecSpec {
                mime: "audio/opus".into(),
                fmtp_line: String::new(),
            },
            CodecSpec {
                mime: "video/VP8".into(),
                fmtp_line: String::new(),
            },
            CodecSpec {
                mime: "video/rtx".into(),
                fmtp_line: String::new(),
            },
        ],
        max_participants: 0,
        empty_timeout: Duration::from_secs(300),
        departure_timeout: Duration::from_secs(20),
        enable_remote_unmute: false,
        playout_delay: None,
        enable_data_tracks: true,
        enable_participant_data_blob: false,
        max_data_track_custom_encoding_length: 32,
        max_data_blob_key_length: 256,
        max_data_blob_size: 64000,
        sync_streams: false,
        enable_full_room_in_webhooks: false,
        update_batch_target_size: 128 << 10,
        subscription_limit_video: None,
        subscription_limit_audio: None,
        max_participant_name_length: 256,
        max_metadata_size: Some(512 << 10),
        max_attributes_size: Some(64 << 10),
        use_ice_lite: false,
        allow_pause: false,
        reconnect_on_publication_error: false,
        reconnect_on_subscription_error: false,
        reconnect_on_data_channel_error: false,
        region: String::new(),
    }
}

fn room() -> (Room, Instant) {
    let now = Instant::now();
    let key = RoomKey::new(ProjectId::local(), "r1");
    let mut room = Room::new(
        key,
        Arc::new(test_policy()),
        Arc::new(EchoData),
        &Default::default(),
        now,
    );
    let started = room.take_effects();
    assert!(matches!(
        started.as_slice(),
        [Effect::Event(
            crate::events::EngineEvent::RoomStarted { .. }
        )]
    ));
    (room, now)
}

fn params(identity: &str, protocol: i32, single_pc: bool) -> JoinParams {
    JoinParams {
        room: RoomKey::new(ProjectId::local(), "r1"),
        grants: Grants {
            identity: identity.into(),
            video: VideoGrant {
                room_join: true,
                room: "r1".into(),
                ..Default::default()
            },
            ..Default::default()
        },
        identity: identity.into(),
        name: String::new(),
        metadata: String::new(),
        attributes: BTreeMap::new(),
        client: ClientMeta {
            sdk: SdkKind::Js,
            protocol,
            ..Default::default()
        },
        capabilities: ClientCapabilities {
            video_publication_requires_rtp: false,
            codec_switch_requires_renegotiation: false,
            ice_tcp: true,
            one_shot: false,
            subscriber_primary: true,
            speaker_changed: true,
            connection_quality: true,
            ice_lite: true,
            unpublish: true,
            fast_start: true,
            disconnected_update: true,
            sync_stream_id: true,
            disable_playout_delay_for_sync: false,
            connection_quality_lost: true,
            identity_based_reconnection: true,
            regions_in_leave: true,
            non_error_signal_response: true,
            request_response: true,
            audio_red: true,
        },
        auto_subscribe: true,
        auto_subscribe_data_track: false,
        adaptive_stream: false,
        single_pc,
        disable_ice_lite: false,
        subscriber_allow_pause: None,
        reconnect: false,
        reconnect_reason: 0,
        participant_sid: None,
        add_tracks: vec![],
        publisher_offer: None,
    }
}

fn signals_to(effects: &[Effect], sid: &ParticipantSid) -> Vec<SignalEvent> {
    effects
        .iter()
        .filter_map(|e| match e {
            Effect::Signal { to, event } if to == sid => Some(event.clone()),
            _ => None,
        })
        .collect()
}

fn add_track(cid: &str, kind: TrackKind, source: TrackSource) -> AddTrack {
    AddTrack {
        token: 1,
        cid: cid.into(),
        name: String::new(),
        kind,
        width: 0,
        height: 0,
        muted: false,
        disable_dtx: false,
        source,
        layers: vec![],
        simulcast_codecs: vec![],
        sid: None,
        stereo: false,
        disable_red: false,
        encryption: EncryptionType::None,
        stream: String::new(),
        backup_codec_policy: BackupCodecPolicy::PreferRegression,
        audio_features: vec![],
        packet_trailer_features: vec![],
    }
}

fn transport_of(effects: &[Effect], sid: &ParticipantSid, role: PcRole) -> TransportId {
    effects
        .iter()
        .find_map(|e| match e {
            Effect::Media(MediaCommand::CreateTransport(spec))
                if &spec.participant == sid && spec.role == role =>
            {
                Some(spec.id)
            }
            _ => None,
        })
        .expect("transport created")
}

#[test]
fn join_response_is_first_and_shows_joining() {
    let (mut room, now) = room();
    let joined = room.join(params("alice", 16, false), now).unwrap();
    let effects = room.take_effects();
    let signals = signals_to(&effects, &joined.sid);
    match &signals[0] {
        SignalEvent::Join(info) => {
            assert_eq!(info.participant.state, ParticipantState::Joining);
            assert!(info.subscriber_primary);
            assert_eq!(info.ping_interval_s, 5);
            assert_eq!(info.ping_timeout_s, 15);
            assert!(
                info.enabled_publish_codecs
                    .iter()
                    .all(|c| c.mime != "video/rtx")
            );
        }
        other => panic!("first signal must be join, got {other:?}"),
    }
    assert!(
        signals
            .iter()
            .any(|s| matches!(s, SignalEvent::ClaimsChanged(_)))
    );
    // Dual PC: publisher + subscriber transports, server channels on subscriber.
    let subs = effects
        .iter()
        .filter(|e| matches!(e, Effect::Media(MediaCommand::CreateTransport(_))))
        .count();
    assert_eq!(subs, 2);
}

#[test]
fn single_pc_is_never_subscriber_primary() {
    let (mut room, now) = room();
    let joined = room.join(params("alice", 16, true), now).unwrap();
    let effects = room.take_effects();
    match &signals_to(&effects, &joined.sid)[0] {
        SignalEvent::Join(info) => assert!(!info.subscriber_primary),
        _ => panic!(),
    }
    transport_of(&effects, &joined.sid, PcRole::Single);
}

#[test]
fn old_protocol_is_publisher_primary() {
    let (mut room, now) = room();
    let mut legacy = params("alice", 2, false);
    legacy.capabilities.subscriber_primary = false;
    let joined = room.join(legacy, now).unwrap();
    let effects = room.take_effects();
    match &signals_to(&effects, &joined.sid)[0] {
        SignalEvent::Join(info) => assert!(!info.subscriber_primary),
        _ => panic!(),
    }
}

#[test]
fn add_track_publishes_immediately_and_binds_on_media() {
    let (mut room, now) = room();
    let alice = room.join(params("alice", 16, true), now).unwrap();
    let effects = room.take_effects();
    let t = transport_of(&effects, &alice.sid, PcRole::Single);

    room.handle_request(
        &alice.sid,
        alice.generation,
        SessionRequest::AddTrack(add_track("cid1", TrackKind::Audio, TrackSource::Microphone)),
        now,
    );
    let effects = room.take_effects();
    let track_sid = signals_to(&effects, &alice.sid)
        .into_iter()
        .find_map(|s| match s {
            SignalEvent::TrackPublished { cid, track } if cid == "cid1" => Some(track.sid),
            _ => None,
        })
        .expect("track_published sent before media");
    assert!(track_sid.0.starts_with("TR_AM"));
    assert_eq!(track_sid.0.len(), "TR_AM".len() + 12);

    room.media_arrives(
        RemoteMedia {
            registration: crate::media_api::PublicationRegistration::Pending,
            participant: alice.sid.clone(),
            transport: t,
            mid: "0".into(),
            kind: TrackKind::Audio,
            msid_track_id: "cid1".into(),
            rids: vec![],
            mime_type: "audio/opus".into(),
        },
        now,
    );
    let effects = room.take_effects();
    assert!(
        effects
            .iter()
            .any(|e| matches!(e, Effect::Media(MediaCommand::BindTrack { mid, .. }) if mid == "0"))
    );
    assert!(effects.iter().any(|e| matches!(
        e,
        Effect::Event(crate::events::EngineEvent::TrackPublished { .. })
    )));
    assert_eq!(room.published_track_count(), 1);
}

#[test]
fn duplicate_cid_is_queued() {
    let (mut room, now) = room();
    let alice = room.join(params("alice", 16, true), now).unwrap();
    room.take_effects();
    room.handle_request(
        &alice.sid,
        alice.generation,
        SessionRequest::AddTrack(add_track("c", TrackKind::Video, TrackSource::Camera)),
        now,
    );
    room.take_effects();
    room.handle_request(
        &alice.sid,
        alice.generation,
        SessionRequest::AddTrack(add_track("c", TrackKind::Video, TrackSource::Camera)),
        now,
    );
    let effects = room.take_effects();
    assert!(signals_to(&effects, &alice.sid).iter().any(|s| matches!(
        s,
        SignalEvent::RequestResponse {
            outcome: RequestOutcome::Queued,
            ..
        }
    )));
}

#[test]
fn first_subscription_reports_allowed_and_track_subscribed_once() {
    let (mut room, now) = room();
    let alice = room.join(params("alice", 16, true), now).unwrap();
    let effects = room.take_effects();
    let ta = transport_of(&effects, &alice.sid, PcRole::Single);
    room.handle_media_event(
        MediaEvent::TransportConnected {
            participant: alice.sid.clone(),
            transport: ta,
        },
        now,
    );
    room.handle_request(
        &alice.sid,
        alice.generation,
        SessionRequest::AddTrack(add_track("v", TrackKind::Video, TrackSource::Camera)),
        now,
    );
    room.media_arrives(
        RemoteMedia {
            registration: crate::media_api::PublicationRegistration::Pending,
            participant: alice.sid.clone(),
            transport: ta,
            mid: "1".into(),
            kind: TrackKind::Video,
            msid_track_id: "v".into(),
            rids: vec![],
            mime_type: "video/VP8".into(),
        },
        now,
    );
    room.take_effects();

    let bob = room.join(params("bob", 16, true), now).unwrap();
    let effects = room.take_effects();
    let tb = transport_of(&effects, &bob.sid, PcRole::Single);
    // Single PC subscribes to existing tracks at join.
    let track_sid = effects
        .iter()
        .find_map(|e| match e {
            Effect::Media(MediaCommand::Subscribe(spec)) if spec.subscriber == bob.sid => {
                Some(spec.track_sid.clone())
            }
            _ => None,
        })
        .expect("bob subscribed");
    assert!(signals_to(&effects, &bob.sid).iter().any(|s| matches!(
        s,
        SignalEvent::SubscriptionPermissionUpdate { allowed: true, .. }
    )));
    let spec_stream = effects.iter().find_map(|e| match e {
        Effect::Media(MediaCommand::Subscribe(spec)) => Some(spec.stream_id.clone()),
        _ => None,
    });
    assert_eq!(spec_stream.unwrap(), format!("{}|{}", alice.sid, track_sid));

    room.handle_media_event(
        MediaEvent::TransportConnected {
            participant: bob.sid.clone(),
            transport: tb,
        },
        now,
    );
    room.handle_media_event(
        MediaEvent::SubscriptionBound {
            subscriber: bob.sid.clone(),
            track_sid: track_sid.clone(),
            mid: "0".into(),
        },
        now,
    );
    room.handle_media_event(
        MediaEvent::SubscriptionBound {
            subscriber: bob.sid.clone(),
            track_sid: track_sid.clone(),
            mid: "0".into(),
        },
        now,
    );
    let effects = room.take_effects();
    let n = signals_to(&effects, &alice.sid)
        .iter()
        .filter(|s| matches!(s, SignalEvent::TrackSubscribed { .. }))
        .count();
    assert_eq!(n, 1);
}

#[test]
fn client_leave_is_echoed_and_others_see_disconnected() {
    let (mut room, now) = room();
    let alice = room.join(params("alice", 16, true), now).unwrap();
    let bob = room.join(params("bob", 16, true), now).unwrap();
    room.take_effects();
    room.handle_request(
        &alice.sid,
        alice.generation,
        SessionRequest::Leave {
            reason: DisconnectReason::ClientInitiated,
        },
        now,
    );
    let effects = room.take_effects();
    assert!(signals_to(&effects, &alice.sid).iter().any(|s| matches!(
        s,
        SignalEvent::Leave {
            action: LeaveAction::Disconnect,
            ..
        }
    )));
    assert!(signals_to(&effects, &bob.sid).iter().any(|s| {
        match s {
            SignalEvent::ParticipantUpdate(infos) => infos
                .iter()
                .any(|i| i.sid == alice.sid && i.state == ParticipantState::Disconnected),
            _ => false,
        }
    }));
    assert!(effects.iter().any(|e| matches!(
        e,
        Effect::Event(crate::events::EngineEvent::ParticipantConnectionAborted { .. })
    )));
}

#[test]
fn duplicate_identity_replaces_old_session() {
    let (mut room, now) = room();
    let first = room.join(params("alice", 16, true), now).unwrap();
    room.take_effects();
    let second = room.join(params("alice", 16, true), now).unwrap();
    assert_ne!(first.sid, second.sid);
    let effects = room.take_effects();
    assert!(signals_to(&effects, &first.sid).iter().any(|s| matches!(
        s,
        SignalEvent::Leave {
            reason: DisconnectReason::DuplicateIdentity,
            ..
        }
    )));
}

#[test]
fn resume_sends_reconnect_first_and_keeps_sid() {
    let (mut room, now) = room();
    let alice = room.join(params("alice", 16, false), now).unwrap();
    room.take_effects();
    room.signal_closed(&alice.sid, alice.generation, now);
    // ICE never connected → closed immediately; reconnect then mismatches.
    let mut p = params("alice", 16, false);
    p.reconnect = true;
    assert!(matches!(
        room.join(p, now),
        Err(JoinError::ReconnectStateMismatch)
    ));

    let bob = room.join(params("bob", 16, false), now).unwrap();
    let effects = room.take_effects();
    let tp = transport_of(&effects, &bob.sid, PcRole::Publisher);
    room.handle_media_event(
        MediaEvent::TransportConnected {
            participant: bob.sid.clone(),
            transport: tp,
        },
        now,
    );
    room.signal_closed(&bob.sid, bob.generation, now);
    room.take_effects();
    let mut p = params("bob", 16, false);
    p.reconnect = true;
    let resumed = room.join(p, now).unwrap();
    assert!(resumed.resumed);
    assert_eq!(resumed.sid, bob.sid);
    let effects = room.take_effects();
    assert!(matches!(
        signals_to(&effects, &bob.sid)[0],
        SignalEvent::Reconnect(_)
    ));
    assert!(
        effects
            .iter()
            .any(|e| matches!(e, Effect::Media(MediaCommand::IceRestart { .. })))
    );
}

#[test]
fn reconnect_without_participant_is_state_mismatch() {
    let (mut room, now) = room();
    let mut p = params("ghost", 16, false);
    p.reconnect = true;
    assert_eq!(
        room.join(p, now).unwrap_err(),
        JoinError::ReconnectStateMismatch
    );
}

#[test]
fn max_participants_excludes_agents() {
    let now = Instant::now();
    let mut cfg = test_policy();
    cfg.max_participants = 1;
    let mut room = Room::new(
        RoomKey::new(ProjectId::local(), "r1"),
        Arc::new(cfg),
        Arc::new(EchoData),
        &Default::default(),
        now,
    );
    room.join(params("a", 16, true), now).unwrap();
    assert_eq!(
        room.join(params("b", 16, true), now).unwrap_err(),
        JoinError::MaxParticipants
    );
    let mut agent = params("agent", 16, true);
    agent.grants.kind = ParticipantKind::Agent;
    assert!(room.join(agent, now).is_ok());
}

#[test]
fn data_is_routed_to_active_others_only() {
    let (mut room, now) = room();
    let alice = room.join(params("alice", 16, true), now).unwrap();
    let bob = room.join(params("bob", 16, true), now).unwrap();
    let effects = room.take_effects();
    let tb = transport_of(&effects, &bob.sid, PcRole::Single);
    room.handle_media_event(
        MediaEvent::TransportConnected {
            participant: bob.sid.clone(),
            transport: tb,
        },
        now,
    );
    room.take_effects();
    room.handle_media_event(
        MediaEvent::DataReceived {
            participant: alice.sid.clone(),
            kind: DataKind::Lossy,
            label: "_lossy".into(),
            payload: vec![1, 2, 3],
        },
        now,
    );
    let effects = room.take_effects();
    // Forwarded data is one batch for all recipients (per-source FIFO).
    let sends: Vec<&ParticipantSid> = effects
        .iter()
        .flat_map(|e| match e {
            Effect::Media(MediaCommand::SendData { to, .. }) => vec![to],
            Effect::Media(MediaCommand::ForwardData { writes, .. }) => {
                writes.iter().map(|w| &w.to).collect()
            }
            _ => Vec::new(),
        })
        .collect();
    assert_eq!(sends, vec![&bob.sid]);
    assert_eq!(
        effects
            .iter()
            .filter(|e| matches!(e, Effect::Media(MediaCommand::ForwardData { source, .. }) if *source == alice.sid))
            .count(),
        1
    );
}

#[test]
fn idle_room_closes_after_empty_timeout_rules() {
    let (room, _) = room();
    // Just created, nobody joined: closes only after empty_timeout (300 s).
    assert!(!room.should_close());
}

#[test]
fn speakers_send_only_changes() {
    let (mut room, now) = room();
    let alice = room.join(params("alice", 16, true), now).unwrap();
    room.take_effects();
    let level = |l: f32, active: bool| {
        MediaEvent::AudioLevels(vec![crate::media_api::AudioLevel {
            participant: alice.sid.clone(),
            level: l,
            active,
        }])
    };
    room.handle_media_event(level(0.3, true), now);
    let first = signals_to(&room.take_effects(), &alice.sid);
    assert!(
        first
            .iter()
            .any(|s| matches!(s, SignalEvent::SpeakersChanged(v) if v[0].level == 0.375))
    );
    room.handle_media_event(level(0.31, true), now);
    assert!(signals_to(&room.take_effects(), &alice.sid).is_empty());
    room.handle_media_event(MediaEvent::AudioLevels(vec![]), now);
    let stopped = signals_to(&room.take_effects(), &alice.sid);
    assert!(stopped.iter().any(
        |s| matches!(s, SignalEvent::SpeakersChanged(v) if !v[0].active && v[0].level == 0.0)
    ));
}

#[test]
fn revoked_pending_publications_cannot_bind_late_media() {
    for source_only in [false, true] {
        let (mut room, now) = room();
        let alice = room.join(params("alice", 16, true), now).unwrap();
        let transport = transport_of(&room.take_effects(), &alice.sid, PcRole::Single);
        room.add_track(
            &alice.sid,
            add_track("revoked", TrackKind::Video, TrackSource::Camera),
            now,
        );
        let track = room.participants[&alice.sid].pending[0].info.sid.clone();
        room.add_track(
            &alice.sid,
            add_track("revoked", TrackKind::Video, TrackSource::Camera),
            now,
        );
        room.take_effects();
        let mut permission = room.participants[&alice.sid].info.permission.clone();
        if source_only {
            permission.can_publish_sources = vec![TrackSource::Microphone];
        } else {
            permission.can_publish = false;
        }
        room.api_update_participant(
            &Identity::from("alice"),
            "",
            "",
            &BTreeMap::new(),
            Some(permission),
            now,
        )
        .unwrap();
        let effects = room.take_effects();
        assert!(effects.iter().any(|effect| matches!(effect,
            Effect::Media(MediaCommand::UnpublishTrack { track_sid }) if track_sid == &track)));
        assert!(
            signals_to(&effects, &alice.sid)
                .iter()
                .any(|event| matches!(event,
            SignalEvent::TrackUnpublished { track_sid } if track_sid == &track))
        );
        assert!(room.participants[&alice.sid].pending.is_empty());
        assert!(room.participants[&alice.sid].queued.is_empty());
        room.api_update_participant(
            &Identity::from("alice"),
            "",
            "",
            &BTreeMap::new(),
            Some(Default::default()),
            now,
        )
        .unwrap();
        room.add_track(
            &alice.sid,
            add_track("fresh", TrackKind::Video, TrackSource::Camera),
            now,
        );
        room.take_effects();
        let media = |cid: &str| RemoteMedia {
            registration: crate::media_api::PublicationRegistration::Pending,
            participant: alice.sid.clone(),
            transport,
            mid: "0".into(),
            kind: TrackKind::Video,
            msid_track_id: cid.into(),
            rids: vec![],
            mime_type: "video/VP8".into(),
        };
        room.media_arrives(media("revoked"), now);
        assert_eq!(room.published_track_count(), 0);
        assert_eq!(room.participants[&alice.sid].pending.len(), 1);
        assert!(
            !room
                .take_effects()
                .iter()
                .any(|effect| matches!(effect, Effect::Media(MediaCommand::BindTrack { .. })))
        );
        room.media_arrives(media("fresh"), now);
        assert_eq!(room.published_track_count(), 1);
    }
}

#[test]
fn actual_publication_rechecks_current_source_permission() {
    let (mut room, now) = room();
    let alice = room.join(params("alice", 16, true), now).unwrap();
    let transport = transport_of(&room.take_effects(), &alice.sid, PcRole::Single);
    room.add_track(
        &alice.sid,
        add_track("cid", TrackKind::Audio, TrackSource::Microphone),
        now,
    );
    room.take_effects();
    // Real permission updates keep the authoritative grant and its public
    // projection together. This fixture isolates the late-media recheck.
    let participant = room.participants.get_mut(&alice.sid).unwrap();
    participant.grants.video.can_publish = Some(false);
    participant.info.permission.can_publish = false;
    room.media_arrives(
        RemoteMedia {
            registration: crate::media_api::PublicationRegistration::Pending,
            participant: alice.sid.clone(),
            transport,
            mid: "0".into(),
            kind: TrackKind::Audio,
            msid_track_id: "cid".into(),
            rids: vec![],
            mime_type: "audio/opus".into(),
        },
        now,
    );
    assert_eq!(room.published_track_count(), 0);
    assert!(room.participants[&alice.sid].pending.is_empty());
    assert!(
        !room
            .take_effects()
            .iter()
            .any(|effect| matches!(effect, Effect::Media(MediaCommand::BindTrack { .. })))
    );
}

#[test]
fn late_publication_checks_raw_source_instead_of_public_enum_projection() {
    let (mut room, now) = room();
    let mut join = params("alice", 16, true);
    join.grants.video.can_publish_sources = vec!["microphone".into()];
    let alice = room.join(join, now).unwrap();
    let transport = transport_of(&room.take_effects(), &alice.sid, PcRole::Single);
    room.add_track(
        &alice.sid,
        add_track("cid", TrackKind::Audio, TrackSource::Microphone),
        now,
    );
    room.take_effects();
    // Internal policy-consumer fixture, not an admin API rewrite: uppercase
    // retains the same public permission projection but denies exact admission.
    let participant = room.participants.get_mut(&alice.sid).unwrap();
    participant.grants.video.can_publish_sources = vec!["MICROPHONE".into()];
    assert_eq!(
        participant.info.permission,
        participant.grants.video.to_permission()
    );
    assert!(
        participant
            .info
            .permission
            .can_publish_source(TrackSource::Microphone)
    );
    room.media_arrives(
        RemoteMedia {
            registration: crate::media_api::PublicationRegistration::Pending,
            participant: alice.sid.clone(),
            transport,
            mid: "0".into(),
            kind: TrackKind::Audio,
            msid_track_id: "cid".into(),
            rids: vec![],
            mime_type: "audio/opus".into(),
        },
        now,
    );
    assert_eq!(room.published_track_count(), 0);
    assert!(room.participants[&alice.sid].pending.is_empty());
    assert!(
        !room
            .take_effects()
            .iter()
            .any(|effect| matches!(effect, Effect::Media(MediaCommand::BindTrack { .. })))
    );
}

#[test]
fn newly_authorized_request_can_reuse_a_cancelled_cid() {
    let (mut room, now) = room();
    let alice = room.join(params("alice", 16, true), now).unwrap();
    let transport = transport_of(&room.take_effects(), &alice.sid, PcRole::Single);
    room.add_track(
        &alice.sid,
        add_track("cid", TrackKind::Audio, TrackSource::Microphone),
        now,
    );
    room.api_update_participant(
        &Identity::from("alice"),
        "",
        "",
        &BTreeMap::new(),
        Some(crate::permission::ParticipantPermission {
            can_publish: false,
            ..Default::default()
        }),
        now,
    )
    .unwrap();
    room.api_update_participant(
        &Identity::from("alice"),
        "",
        "",
        &BTreeMap::new(),
        Some(Default::default()),
        now,
    )
    .unwrap();
    room.add_track(
        &alice.sid,
        add_track("cid", TrackKind::Audio, TrackSource::Microphone),
        now,
    );
    room.media_arrives(
        RemoteMedia {
            registration: crate::media_api::PublicationRegistration::Pending,
            participant: alice.sid.clone(),
            transport,
            mid: "0".into(),
            kind: TrackKind::Audio,
            msid_track_id: "cid".into(),
            rids: vec![],
            mime_type: "audio/opus".into(),
        },
        now,
    );
    assert_eq!(room.published_track_count(), 1);
}

#[test]
fn explicit_subscription_survives_permission_revocation() {
    let (mut room, now) = room();
    let alice = room.join(params("alice", 16, true), now).unwrap();
    let transport = transport_of(&room.take_effects(), &alice.sid, PcRole::Single);
    room.add_track(
        &alice.sid,
        add_track("cid", TrackKind::Audio, TrackSource::Microphone),
        now,
    );
    room.media_arrives(
        RemoteMedia {
            registration: crate::media_api::PublicationRegistration::Pending,
            participant: alice.sid.clone(),
            transport,
            mid: "0".into(),
            kind: TrackKind::Audio,
            msid_track_id: "cid".into(),
            rids: vec![],
            mime_type: "audio/opus".into(),
        },
        now,
    );
    let track = room.participants[&alice.sid].info.tracks[0].sid.clone();
    let mut bob_params = params("bob", 16, true);
    bob_params.auto_subscribe = false;
    let bob = room.join(bob_params, now).unwrap();
    room.subscribe(&bob.sid, &track, now);
    room.take_effects();
    room.api_update_participant(
        &Identity::from("bob"),
        "",
        "",
        &BTreeMap::new(),
        Some(crate::permission::ParticipantPermission {
            can_subscribe: false,
            ..Default::default()
        }),
        now,
    )
    .unwrap();
    assert!(!room.participants[&bob.sid].subscriptions[&track].requested);
    room.subscription_bound(&bob.sid, &track);
    assert!(!room.participants[&bob.sid].subscriptions[&track].bound);
    room.take_effects();
    room.api_update_participant(
        &Identity::from("bob"),
        "",
        "",
        &BTreeMap::new(),
        Some(Default::default()),
        now,
    )
    .unwrap();
    assert!(room.participants[&bob.sid].subscriptions[&track].requested);
    assert!(room.take_effects().iter().any(|effect| matches!(effect,
        Effect::Media(MediaCommand::Subscribe(spec)) if spec.subscriber == bob.sid && spec.track_sid == track)));
}

#[test]
fn old_session_cannot_mutate_resumed_participant() {
    let (mut room, now) = room();
    let alice = room.join(params("alice", 16, true), now).unwrap();
    let transport = transport_of(&room.take_effects(), &alice.sid, PcRole::Single);
    room.handle_media_event(
        MediaEvent::TransportConnected {
            participant: alice.sid.clone(),
            transport,
        },
        now,
    );
    let mut reconnect = params("alice", 16, true);
    reconnect.reconnect = true;
    let resumed = room.join(reconnect, now).unwrap();
    room.take_effects();
    room.handle_request(
        &alice.sid,
        alice.generation,
        SessionRequest::Leave {
            reason: DisconnectReason::ClientInitiated,
        },
        now,
    );
    room.signal_closed(&alice.sid, alice.generation, now);
    assert_eq!(
        room.participant_generation(&alice.sid),
        Some(resumed.generation)
    );
    assert!(room.participants[&alice.sid].signal_connected);
    assert!(room.take_effects().is_empty());
}

#[test]
fn removing_participant_does_not_restart_queued_publication() {
    let (mut room, now) = room();
    let alice = room.join(params("alice", 16, true), now).unwrap();
    let transport = transport_of(&room.take_effects(), &alice.sid, PcRole::Single);
    room.add_track(
        &alice.sid,
        add_track("cid", TrackKind::Audio, TrackSource::Microphone),
        now,
    );
    room.media_arrives(
        RemoteMedia {
            registration: crate::media_api::PublicationRegistration::Pending,
            participant: alice.sid.clone(),
            transport,
            mid: "0".into(),
            kind: TrackKind::Audio,
            msid_track_id: "cid".into(),
            rids: vec![],
            mime_type: "audio/opus".into(),
        },
        now,
    );
    room.add_track(
        &alice.sid,
        add_track("cid", TrackKind::Audio, TrackSource::Microphone),
        now,
    );
    room.take_effects();
    room.api_remove_participant(&Identity::from("alice"), now)
        .unwrap();
    let effects = room.take_effects();
    assert!(
        !effects
            .iter()
            .any(|effect| matches!(effect, Effect::Media(MediaCommand::ExpectTrack { .. })))
    );
    assert!(
        !signals_to(&effects, &alice.sid)
            .iter()
            .any(|event| matches!(event, SignalEvent::TrackPublished { .. }))
    );
    assert_eq!(room.published_track_count(), 0);
}

#[test]
fn cancelled_cid_aliases_are_cleared_together_on_republication() {
    let (mut room, now) = room();
    let alice = room.join(params("alice", 16, true), now).unwrap();
    room.take_effects();
    let participant = room.participants.get_mut(&alice.sid).unwrap();
    participant.remember_released_cids(vec!["cid".into(), "sdp-alias".into()]);
    room.add_track(
        &alice.sid,
        add_track("cid", TrackKind::Audio, TrackSource::Microphone),
        now,
    );
    assert!(!room.participants[&alice.sid].is_released_cid("sdp-alias"));
}

#[test]
fn consumed_reliable_messages_advance_sequence_before_adapter_side_effects() {
    use std::sync::atomic::{AtomicUsize, Ordering};
    struct ConsumedData(AtomicUsize);
    impl DataHandler for ConsumedData {
        fn reliable_sequence(&self, _: &str, raw: &[u8]) -> u32 {
            raw[0] as u32
        }
        fn inbound(&self, _: &DataSender<'_>, _: DataKind, _: &str, _: &[u8]) -> DataVerdict {
            self.0.fetch_add(1, Ordering::SeqCst);
            DataVerdict::Consumed
        }
    }
    let (mut room, now) = room();
    let data = Arc::new(ConsumedData(AtomicUsize::new(0)));
    room.data = data.clone();
    let alice = room.join(params("alice", 16, true), now).unwrap();
    room.take_effects();
    for seq in [1, 1, 2, 1] {
        room.data_received(&alice.sid, DataKind::Reliable, "_reliable", &[seq], now);
    }
    assert_eq!(data.0.load(Ordering::SeqCst), 2);
    assert_eq!(room.participants[&alice.sid].last_reliable_seq, 2);
    let mut reconnect = params("alice", 16, true);
    reconnect.reconnect = true;
    room.join(reconnect, now).unwrap();
    assert!(
        signals_to(&room.take_effects(), &alice.sid)
            .iter()
            .any(|event| matches!(event,
        SignalEvent::Reconnect(info) if info.last_message_seq == 2))
    );
}

#[test]
fn explicit_capabilities_not_metadata_select_join_transport_behavior() {
    for enabled in [false, true] {
        let (mut room, now) = room();
        Arc::make_mut(&mut room.policy).use_ice_lite = true;
        let mut p = params("profile", if enabled { 0 } else { 99 }, false);
        p.client.browser = if enabled { "Firefox Safari" } else { "Chrome" }.into();
        p.capabilities.subscriber_primary = enabled;
        p.capabilities.ice_lite = enabled;
        p.capabilities.audio_red = enabled;
        let joined = room.join(p, now).unwrap();
        let effects = room.take_effects();
        assert!(matches!(&signals_to(&effects, &joined.sid)[0],
            SignalEvent::Join(info) if info.subscriber_primary == enabled));
        let transports: Vec<_> = effects
            .iter()
            .filter_map(|e| match e {
                Effect::Media(MediaCommand::CreateTransport(spec)) => Some(spec),
                _ => None,
            })
            .collect();
        assert!(!transports.is_empty());
        assert!(
            transports
                .iter()
                .all(|spec| spec.ice_lite == enabled && spec.client_supports_red == enabled)
        );
    }
}

#[test]
fn resume_replaces_capabilities_before_disconnected_updates_and_keeps_reconnect_first() {
    for supports_after in [false, true] {
        let (mut room, now) = room();
        let mut original = params("returning", 99, false);
        original.capabilities.disconnected_update = !supports_after;
        let old = room.join(original, now).unwrap();
        let effects = room.take_effects();
        let transport = transport_of(&effects, &old.sid, PcRole::Publisher);
        room.handle_media_event(
            MediaEvent::TransportConnected {
                participant: old.sid.clone(),
                transport,
            },
            now,
        );
        room.signal_closed(&old.sid, old.generation, now);
        let gone = room.join(params("gone", 16, true), now).unwrap();
        room.handle_request(
            &gone.sid,
            gone.generation,
            SessionRequest::Leave {
                reason: DisconnectReason::ClientInitiated,
            },
            now,
        );
        room.take_effects();
        // Ordinary updates queue while signaling is down regardless of the
        // disconnected replay capability. The leave overwrites its joining
        // version, so exactly one queued disconnected entry must survive.
        let queued = &room.participants[&old.sid].queued_updates;
        assert_eq!(
            queued.get(&gone.sid).unwrap().state,
            ParticipantState::Disconnected
        );
        assert_eq!(
            queued.values().filter(|info| info.sid == gone.sid).count(),
            1
        );
        let mut incoming = params("returning", 0, false);
        incoming.reconnect = true;
        incoming.capabilities.disconnected_update = supports_after;
        let expected = incoming.capabilities;
        let resumed = room.join(incoming, now).unwrap();
        assert_eq!(resumed.sid, old.sid);
        assert_ne!(resumed.generation, old.generation);
        assert_eq!(room.participants[&old.sid].capabilities, expected);
        assert_eq!(room.participants[&old.sid].client.protocol, 0);
        let effects = room.take_effects();
        let signals = signals_to(&effects, &old.sid);
        assert!(matches!(signals[0], SignalEvent::Reconnect(_)));
        let gone_reports = signals
            .iter()
            .filter_map(|signal| match signal {
                SignalEvent::ParticipantUpdate(infos) => Some(
                    infos
                        .iter()
                        .filter(|info| {
                            info.sid == gone.sid && info.state == ParticipantState::Disconnected
                        })
                        .count(),
                ),
                _ => None,
            })
            .sum::<usize>();
        assert!(
            matches!(&signals[1], SignalEvent::ParticipantUpdate(infos)
            if infos.iter().any(|info| info.sid == gone.sid)),
            "ordinary queued update flush follows Reconnect for both capability values"
        );
        assert_eq!(
            gone_reports,
            1 + usize::from(supports_after),
            "one ordinary queued update plus exactly one capability-gated recent-disconnect replay"
        );
        assert!(room.participants[&old.sid].queued_updates.is_empty());
    }
}

#[test]
fn policy_room_overrides_keep_zero_fallback_and_explicit_false() {
    let now = Instant::now();
    let mut policy = test_policy();
    policy.max_participants = 7;
    policy.empty_timeout = Duration::from_millis(15_999);
    policy.departure_timeout = Duration::from_millis(8_999);
    policy.sync_streams = true;
    let expected_codecs = policy.enabled_codecs.clone();
    let room = Room::new(
        RoomKey::new(ProjectId::local(), "overrides"),
        Arc::new(policy.clone()),
        Arc::new(EchoData),
        &crate::permission::RoomConfig {
            max_participants: Some(0),
            empty_timeout_s: Some(0),
            departure_timeout_s: Some(0),
            sync_streams: Some(false),
            metadata: Some("room metadata".into()),
            ..Default::default()
        },
        now,
    );
    assert_eq!(room.info().max_participants, 7);
    assert_eq!(room.info().empty_timeout_s, 15);
    assert_eq!(room.info().departure_timeout_s, 8);
    assert_eq!(room.info().enabled_codecs, expected_codecs);
    assert_eq!(room.info().metadata, "room metadata");
    assert!(!room.sync_streams);
    let overridden = Room::new(
        RoomKey::new(ProjectId::local(), "positive"),
        Arc::new(policy),
        Arc::new(EchoData),
        &crate::permission::RoomConfig {
            max_participants: Some(2),
            empty_timeout_s: Some(3),
            departure_timeout_s: Some(4),
            ..Default::default()
        },
        now,
    );
    assert_eq!(overridden.info().max_participants, 2);
    assert_eq!(overridden.info().empty_timeout_s, 3);
    assert_eq!(overridden.info().departure_timeout_s, 4);
    assert!(overridden.sync_streams);
}

#[test]
fn policy_identity_limits_count_utf8_bytes_and_keep_empty_update_rules() {
    let (mut room, now) = room();
    let policy = Arc::make_mut(&mut room.policy);
    policy.max_participant_name_length = 3;
    policy.max_metadata_size = Some(3);
    policy.max_attributes_size = Some(3);
    let mut join = params("alice", 17, true);
    join.grants.video.can_update_own_metadata = Some(true);
    let alice = room.join(join, now).unwrap();
    room.take_effects();
    for (name, metadata, attributes) in [
        ("éé", "", BTreeMap::new()),
        ("", "éé", BTreeMap::new()),
        ("", "", BTreeMap::from([("é".into(), "é".into())])),
    ] {
        room.handle_request(
            &alice.sid,
            alice.generation,
            SessionRequest::UpdateMetadata {
                token: 1,
                request_id: 2,
                name: name.into(),
                metadata: metadata.into(),
                attributes,
            },
            now,
        );
        let effects = room.take_effects();
        assert!(signals_to(&effects, &alice.sid).iter().any(|s| matches!(
            s,
            SignalEvent::RequestResponse {
                outcome: RequestOutcome::LimitExceeded,
                ..
            }
        )));
        let unchanged = room.get_participant(&"alice".into()).unwrap();
        assert!(
            unchanged.name.is_empty()
                && unchanged.metadata.is_empty()
                && unchanged.attributes.is_empty()
        );
    }
    room.handle_request(
        &alice.sid,
        alice.generation,
        SessionRequest::UpdateMetadata {
            token: 3,
            request_id: 4,
            name: "éa".into(),
            metadata: "界".into(),
            attributes: BTreeMap::from([("é".into(), "x".into())]),
        },
        now,
    );
    let effects = room.take_effects();
    assert!(signals_to(&effects, &alice.sid).iter().any(|s| matches!(
        s,
        SignalEvent::RequestResponse {
            outcome: RequestOutcome::Ok,
            ..
        }
    )));
    let updated = room.get_participant(&"alice".into()).unwrap();
    assert_eq!(updated.name, "éa");
    assert_eq!(updated.metadata, "界");
    assert_eq!(updated.attributes.get("é").map(String::as_str), Some("x"));
    room.handle_request(
        &alice.sid,
        alice.generation,
        SessionRequest::UpdateMetadata {
            token: 5,
            request_id: 6,
            name: String::new(),
            metadata: String::new(),
            attributes: BTreeMap::from([("é".into(), String::new())]),
        },
        now,
    );
    let updated = room.get_participant(&"alice".into()).unwrap();
    assert_eq!(updated.name, "éa");
    assert_eq!(updated.metadata, "界");
    assert!(updated.attributes.is_empty());
}

#[test]
fn policy_remote_unmute_controls_actual_server_api_operation() {
    for allowed in [false, true] {
        let (mut room, now) = room();
        Arc::make_mut(&mut room.policy).enable_remote_unmute = allowed;
        let alice = room.join(params("alice", 17, true), now).unwrap();
        room.take_effects();
        let mut request = add_track("mic", TrackKind::Audio, TrackSource::Microphone);
        request.muted = true;
        room.handle_request(
            &alice.sid,
            alice.generation,
            SessionRequest::AddTrack(request),
            now,
        );
        let effects = room.take_effects();
        let track = signals_to(&effects, &alice.sid)
            .into_iter()
            .find_map(|signal| match signal {
                SignalEvent::TrackPublished { track, .. } => Some(track.sid),
                _ => None,
            })
            .expect("pending publication announced");
        let result = room.api_mute_track(&"alice".into(), &track, false, now);
        if allowed {
            assert!(!result.unwrap().expect("pending publication exists").muted);
            assert!(
                signals_to(&room.take_effects(), &alice.sid)
                    .iter()
                    .any(|s| matches!(s,
                        SignalEvent::Mute { track_sid, muted: false } if track_sid == &track
                    ))
            );
        } else {
            assert_eq!(
                result.unwrap_err(),
                OpError::FailedPrecondition("remote unmute not enabled")
            );
            assert!(room.take_effects().is_empty());
        }
    }
}

#[test]
fn policy_subscription_limits_apply_to_real_audio_and_video_publications() {
    for (kind, source, mime) in [
        (TrackKind::Audio, TrackSource::Microphone, "audio/opus"),
        (TrackKind::Video, TrackSource::Camera, "video/VP8"),
    ] {
        for limit in [0, 1] {
            let (mut room, now) = room();
            let policy = Arc::make_mut(&mut room.policy);
            policy.subscription_limit_audio = Some(limit);
            policy.subscription_limit_video = Some(limit);
            let alice = room.join(params("alice", 17, true), now).unwrap();
            let transport = transport_of(&room.take_effects(), &alice.sid, PcRole::Single);
            room.handle_media_event(
                MediaEvent::TransportConnected {
                    participant: alice.sid.clone(),
                    transport,
                },
                now,
            );
            for cid in ["first", "second"] {
                room.handle_request(
                    &alice.sid,
                    alice.generation,
                    SessionRequest::AddTrack(add_track(cid, kind, source)),
                    now,
                );
                room.media_arrives(
                    RemoteMedia {
                        registration: crate::media_api::PublicationRegistration::Pending,
                        participant: alice.sid.clone(),
                        transport,
                        mid: cid.into(),
                        kind,
                        msid_track_id: cid.into(),
                        rids: vec![],
                        mime_type: mime.into(),
                    },
                    now,
                );
            }
            room.take_effects();
            let bob = room.join(params("bob", 17, true), now).unwrap();
            let effects = room.take_effects();
            assert_eq!(
                effects
                    .iter()
                    .filter(|e| matches!(e,
                        Effect::Media(MediaCommand::Subscribe(spec)) if spec.subscriber == bob.sid
                    ))
                    .count(),
                limit as usize
            );
            assert_eq!(
                signals_to(&effects, &bob.sid)
                    .iter()
                    .filter(|s| matches!(
                        s,
                        SignalEvent::SubscriptionResponse {
                            error: SubscriptionError::Unknown,
                            ..
                        }
                    ))
                    .count(),
                2 - limit as usize
            );
        }
    }
}

fn data_publish(handle: u32, name: &str) -> PublishDataTrack {
    PublishDataTrack {
        token: 91,
        pub_handle: handle,
        name: name.into(),
        encryption: EncryptionType::None,
        frame_encoding: Some(DataTrackEncoding::WellKnown(7)),
        schema: None,
    }
}

fn activate_data_participant(room: &mut Room, sid: &ParticipantSid, now: Instant) {
    let transport = room.participants[sid].primary;
    room.handle_media_event(
        MediaEvent::TransportConnected {
            participant: sid.clone(),
            transport,
        },
        now,
    );
}

fn data_packet(handle: u16) -> Vec<u8> {
    let mut packet = vec![0x18, 0, 0, 0, 0, 9, 0, 4, 0, 0, 0, 1];
    packet[2..4].copy_from_slice(&handle.to_be_bytes());
    packet.extend_from_slice(b"frame payload");
    packet
}

#[test]
fn data_track_publication_validation_preserves_upstream_outcomes_and_byte_limits() {
    let (mut room, now) = room();
    let publisher = room.join(params("data-publisher", 17, true), now).unwrap();
    room.take_effects();
    for (request, expected) in [
        (data_publish(0, "x"), RequestOutcome::InvalidHandle),
        (data_publish(65536, "x"), RequestOutcome::InvalidHandle),
        (data_publish(1, ""), RequestOutcome::InvalidName),
        (
            data_publish(1, &"界".repeat(86)),
            RequestOutcome::InvalidName,
        ),
        (
            PublishDataTrack {
                frame_encoding: Some(DataTrackEncoding::Custom("".into())),
                ..data_publish(1, "x")
            },
            RequestOutcome::InvalidRequest,
        ),
        (
            PublishDataTrack {
                schema: Some(DataTrackSchema {
                    name: "schema".into(),
                    encoding: Some(DataTrackEncoding::Custom("z".repeat(33))),
                }),
                ..data_publish(1, "x")
            },
            RequestOutcome::InvalidRequest,
        ),
    ] {
        room.handle_request(
            &publisher.sid,
            SessionGeneration::default(),
            SessionRequest::PublishDataTrack(request),
            now,
        );
        assert!(
            signals_to(&room.take_effects(), &publisher.sid).iter().any(
                |s| matches!(s,SignalEvent::RequestResponse{outcome,..} if *outcome==expected)
            )
        );
        assert!(
            room.participants[&publisher.sid]
                .info
                .data_tracks
                .is_empty()
        );
    }
    room.handle_request(
        &publisher.sid,
        SessionGeneration::default(),
        SessionRequest::PublishDataTrack(data_publish(65535, "position")),
        now,
    );
    let effects = room.take_effects();
    let info = signals_to(&effects, &publisher.sid)
        .into_iter()
        .find_map(|s| {
            if let SignalEvent::DataTrackPublished(t) = s {
                Some(t)
            } else {
                None
            }
        })
        .unwrap();
    assert!(info.sid.as_str().starts_with("DTR_"));
    assert_eq!(info.pub_handle, 65535);
    assert!(room.participants[&publisher.sid].info.is_publisher);
    for (handle, name, expected) in [
        (65535, "different", RequestOutcome::DuplicateHandle),
        (2, "position", RequestOutcome::DuplicateName),
    ] {
        room.handle_request(
            &publisher.sid,
            SessionGeneration::default(),
            SessionRequest::PublishDataTrack(data_publish(handle, name)),
            now,
        );
        assert!(
            signals_to(&room.take_effects(), &publisher.sid).iter().any(
                |s| matches!(s,SignalEvent::RequestResponse{outcome,..} if *outcome==expected)
            )
        );
    }
    Arc::make_mut(&mut room.policy).max_data_track_custom_encoding_length = -1;
    room.handle_request(
        &publisher.sid,
        SessionGeneration::default(),
        SessionRequest::PublishDataTrack(PublishDataTrack {
            frame_encoding: Some(DataTrackEncoding::Custom("x".into())),
            ..data_publish(2, "negative-limit")
        }),
        now,
    );
    assert!(
        signals_to(&room.take_effects(), &publisher.sid)
            .iter()
            .any(|s| matches!(
                s,
                SignalEvent::RequestResponse {
                    outcome: RequestOutcome::InvalidRequest,
                    ..
                }
            ))
    );
    Arc::make_mut(&mut room.policy).max_data_track_custom_encoding_length = 0;
    room.handle_request(
        &publisher.sid,
        SessionGeneration::default(),
        SessionRequest::PublishDataTrack(PublishDataTrack {
            frame_encoding: Some(DataTrackEncoding::Custom("x".repeat(100))),
            ..data_publish(2, "unlimited")
        }),
        now,
    );
    assert!(
        signals_to(&room.take_effects(), &publisher.sid)
            .iter()
            .any(|s| matches!(s, SignalEvent::DataTrackPublished(_)))
    );
    room.handle_request(
        &publisher.sid,
        SessionGeneration::default(),
        SessionRequest::UnpublishDataTrack {
            token: 0,
            pub_handle: 2,
        },
        now,
    );
    room.take_effects();
    // The upstream unpublish lookup narrows uint32 to uint16 before searching.
    room.handle_request(
        &publisher.sid,
        SessionGeneration::default(),
        SessionRequest::UnpublishDataTrack {
            token: 92,
            pub_handle: 131071,
        },
        now,
    );
    assert!(
        signals_to(&room.take_effects(), &publisher.sid)
            .iter()
            .any(|s| matches!(s,SignalEvent::DataTrackUnpublished(t) if t.sid==info.sid))
    );
    assert!(
        room.participants[&publisher.sid]
            .info
            .data_tracks
            .is_empty()
    );
}

#[test]
fn data_track_raw_routing_rewrites_only_subscriber_handle_and_honors_unsubscribe() {
    let (mut room, now) = room();
    let publisher = room.join(params("publisher", 17, true), now).unwrap();
    let subscriber = room.join(params("subscriber", 17, true), now).unwrap();
    activate_data_participant(&mut room, &publisher.sid, now);
    activate_data_participant(&mut room, &subscriber.sid, now);
    room.handle_request(
        &publisher.sid,
        SessionGeneration::default(),
        SessionRequest::PublishDataTrack(data_publish(500, "position")),
        now,
    );
    let info = room.participants[&publisher.sid].info.data_tracks[0].clone();
    room.handle_request(
        &subscriber.sid,
        SessionGeneration::default(),
        SessionRequest::DataSubscription(vec![DataTrackSubscriptionUpdate {
            track_sid: info.sid.clone(),
            subscribe: true,
            target_fps: Some(0),
        }]),
        now,
    );
    let handle = room.participants[&subscriber.sid].data_subscriptions[&info.sid]
        .handle
        .unwrap();
    assert_ne!(handle, 500);
    assert_eq!(
        room.participants[&subscriber.sid].data_subscriptions[&info.sid].target_fps,
        Some(0)
    );
    let mappings = signals_to(&room.take_effects(), &subscriber.sid);
    assert!(mappings.iter().any(|s|matches!(s,SignalEvent::DataTrackSubscriberHandles(h) if h.get(&(handle as u32)).is_some_and(|t|t.track_sid==info.sid && t.publisher_sid==publisher.sid))));
    let packet = data_packet(500);
    room.data_received(&publisher.sid, DataKind::Lossy, "_data_track", &packet, now);
    let effects = room.take_effects();
    let forwarded = effects
        .iter()
        .find_map(|e| {
            if let Effect::Media(MediaCommand::SendDataTrack { to, payload }) = e {
                assert_eq!(to, &subscriber.sid);
                Some(payload)
            } else {
                None
            }
        })
        .unwrap();
    let mut expected = packet.clone();
    expected[2..4].copy_from_slice(&handle.to_be_bytes());
    assert_eq!(forwarded, &expected);
    assert!(!effects.iter().any(|e| matches!(
        e,
        Effect::Media(MediaCommand::SendData { .. } | MediaCommand::ForwardData { .. })
    )));
    // Fragment flags, header extensions and opaque E2EE bytes remain intact.
    let mut extended = packet[..12].to_vec();
    extended[0] = 0x14;
    extended.extend_from_slice(&[0, 1, 7, 2, 0xaa, 0xbb, 0, 0]);
    extended.extend_from_slice(&[0, 255, 128, 1]);
    room.data_received(
        &publisher.sid,
        DataKind::Lossy,
        "_data_track",
        &extended,
        now,
    );
    let effects = room.take_effects();
    let mut expected = extended;
    expected[2..4].copy_from_slice(&handle.to_be_bytes());
    assert!(effects.iter().any(|e|matches!(e,Effect::Media(MediaCommand::SendDataTrack{to,payload}) if to==&subscriber.sid && payload==&expected)));
    for malformed in [vec![0; 11], vec![4; 12], {
        let mut p = packet.clone();
        p[0] |= 4;
        p[12] = 255;
        p[13] = 255;
        p
    }] {
        room.data_received(
            &publisher.sid,
            DataKind::Lossy,
            "_data_track",
            &malformed,
            now,
        );
        assert!(
            !room
                .take_effects()
                .iter()
                .any(|e| matches!(e, Effect::Media(MediaCommand::SendDataTrack { .. })))
        );
    }
    room.handle_request(
        &subscriber.sid,
        SessionGeneration::default(),
        SessionRequest::DataSubscription(vec![DataTrackSubscriptionUpdate {
            track_sid: info.sid.clone(),
            subscribe: false,
            target_fps: Some(99),
        }]),
        now,
    );
    room.take_effects();
    room.data_received(&publisher.sid, DataKind::Lossy, "_data_track", &packet, now);
    assert!(room.take_effects().is_empty());
}

#[test]
fn data_track_auto_subscription_waits_for_active_and_permission_revocation_stops_data() {
    let (mut room, now) = room();
    let publisher = room.join(params("publisher", 17, true), now).unwrap();
    let mut sp = params("subscriber", 17, true);
    sp.auto_subscribe_data_track = true;
    let subscriber = room.join(sp, now).unwrap();
    room.handle_request(
        &publisher.sid,
        SessionGeneration::default(),
        SessionRequest::PublishDataTrack(data_publish(3, "x")),
        now,
    );
    let track = room.participants[&publisher.sid].info.data_tracks[0]
        .sid
        .clone();
    assert!(
        room.participants[&subscriber.sid]
            .data_subscriptions
            .is_empty()
    );
    activate_data_participant(&mut room, &subscriber.sid, now);
    let first = room.participants[&subscriber.sid].data_subscriptions[&track]
        .handle
        .unwrap();
    room.handle_request(
        &publisher.sid,
        SessionGeneration::default(),
        SessionRequest::SubscriptionPermission(SubscriptionPermission {
            all_participants: false,
            track_permissions: vec![],
        }),
        now,
    );
    assert!(
        room.participants[&subscriber.sid].data_subscriptions[&track]
            .handle
            .is_none()
    );
    room.take_effects();
    room.data_received(
        &publisher.sid,
        DataKind::Lossy,
        "_data_track",
        &data_packet(3),
        now,
    );
    assert!(room.take_effects().is_empty());
    room.handle_request(
        &publisher.sid,
        SessionGeneration::default(),
        SessionRequest::SubscriptionPermission(SubscriptionPermission {
            all_participants: true,
            track_permissions: vec![],
        }),
        now,
    );
    assert!(
        room.participants[&subscriber.sid].data_subscriptions[&track]
            .handle
            .unwrap()
            > first
    );
    room.remove_participant(&publisher.sid, DisconnectReason::ClientInitiated, true, now);
    assert!(
        !room.participants[&subscriber.sid]
            .data_subscriptions
            .contains_key(&track)
    );
}

#[test]
fn data_track_disabled_or_publish_permission_missing_is_not_success_and_stale_generation_is_ignored()
 {
    let (mut room, now) = room();
    let publisher = room.join(params("publisher", 17, true), now).unwrap();
    room.take_effects();
    Arc::make_mut(&mut room.policy).enable_data_tracks = false;
    room.handle_request(
        &publisher.sid,
        SessionGeneration::default(),
        SessionRequest::PublishDataTrack(data_publish(1, "x")),
        now,
    );
    assert!(
        signals_to(&room.take_effects(), &publisher.sid)
            .iter()
            .any(|s| matches!(
                s,
                SignalEvent::RequestResponse {
                    outcome: RequestOutcome::NotAllowed,
                    ..
                }
            ))
    );
    Arc::make_mut(&mut room.policy).enable_data_tracks = true;
    room.participants
        .get_mut(&publisher.sid)
        .unwrap()
        .info
        .permission
        .can_publish_data = false;
    room.handle_request(
        &publisher.sid,
        SessionGeneration::default(),
        SessionRequest::PublishDataTrack(data_publish(1, "x")),
        now,
    );
    assert!(
        signals_to(&room.take_effects(), &publisher.sid)
            .iter()
            .any(|s| matches!(
                s,
                SignalEvent::RequestResponse {
                    outcome: RequestOutcome::NotAllowed,
                    ..
                }
            ))
    );
    room.participants
        .get_mut(&publisher.sid)
        .unwrap()
        .info
        .permission
        .can_publish_data = true;
    let mut reconnect = params("publisher", 17, true);
    reconnect.reconnect = true;
    reconnect.participant_sid = Some(publisher.sid.clone());
    let resumed = room.join(reconnect, now + Duration::from_secs(1)).unwrap();
    room.take_effects();
    room.handle_request(
        &publisher.sid,
        SessionGeneration::default(),
        SessionRequest::PublishDataTrack(data_publish(1, "old")),
        now,
    );
    assert!(room.take_effects().is_empty());
    room.handle_request(
        &publisher.sid,
        resumed.generation,
        SessionRequest::PublishDataTrack(data_publish(1, "new")),
        now,
    );
    assert_eq!(
        room.participants[&publisher.sid].info.data_tracks[0].name,
        "new"
    );
}

#[test]
fn data_track_resume_retains_publications_and_validates_reported_inventory() {
    let (mut room, now) = room();
    let publisher = room.join(params("publisher", 17, true), now).unwrap();
    room.handle_request(
        &publisher.sid,
        SessionGeneration::default(),
        SessionRequest::PublishDataTrack(data_publish(9, "persistent")),
        now,
    );
    let track = room.participants[&publisher.sid].info.data_tracks[0].clone();
    let mut reconnect = params("publisher", 17, true);
    reconnect.reconnect = true;
    reconnect.participant_sid = Some(publisher.sid.clone());
    let resumed = room.join(reconnect, now + Duration::from_secs(1)).unwrap();
    assert_eq!(
        room.participants[&publisher.sid].info.data_tracks,
        vec![track.clone()]
    );
    room.take_effects();
    let sync = |tracks| {
        SessionRequest::SyncState(SyncState {
            publish_tracks: vec![],
            publish_data_tracks: tracks,
            track_sids_disabled: vec![],
            subscribe: true,
            subscription_track_sids: vec![],
        })
    };
    room.handle_request(
        &publisher.sid,
        resumed.generation,
        sync(vec![track.sid]),
        now,
    );
    assert!(room.participants.contains_key(&publisher.sid));
    assert!(room.take_effects().is_empty());
    room.handle_request(
        &publisher.sid,
        resumed.generation,
        sync(vec![TrackSid::from("")]),
        now,
    );
    assert!(!room.participants.contains_key(&publisher.sid));
    assert!(
        signals_to(&room.take_effects(), &publisher.sid)
            .iter()
            .any(|s| matches!(
                s,
                SignalEvent::Leave {
                    reason: DisconnectReason::StateMismatch,
                    action: LeaveAction::Reconnect
                }
            ))
    );
}

#[test]
fn room_generation_resolves_complete_playout_pair_once() {
    let mut policy = test_policy();
    policy.playout_delay = Some((100, 200));
    let policy = Arc::new(policy);
    let now = Instant::now();
    let mut ids = std::collections::BTreeSet::new();
    for (min, max, expected) in [
        (None, None, Some((100, 200))),
        (Some(0), Some(0), Some((100, 200))),
        (Some(30), None, Some((30, 0))),
        (None, Some(90), Some((0, 90))),
        (Some(30), Some(90), Some((30, 90))),
    ] {
        let mut overrides = RoomConfig {
            min_playout_delay: min,
            max_playout_delay: max,
            ..Default::default()
        };
        let room = Room::new(
            RoomKey::new(ProjectId::local(), "playout"),
            policy.clone(),
            Arc::new(EchoData),
            &overrides,
            now,
        );
        assert_eq!(room.playout_delay(), expected);
        assert!(ids.insert(room.sid().clone()));
        overrides.min_playout_delay = Some(999);
        assert_eq!(room.playout_delay(), expected);
        assert_eq!(policy.playout_delay, Some((100, 200)));
    }
    let disabled = Room::new(
        RoomKey::new(ProjectId::local(), "disabled"),
        Arc::new(test_policy()),
        Arc::new(EchoData),
        &RoomConfig::default(),
        now,
    );
    assert_eq!(disabled.playout_delay(), None);
}

#[test]
fn one_shot_named_subscription_waits_for_actual_prepared_media_not_just_request_or_name() {
    let (mut room, now) = room();
    let alice = room.join(params("alice", 17, true), now).unwrap();
    let effects = room.take_effects();
    let ta = transport_of(&effects, &alice.sid, PcRole::Single);
    room.handle_request(
        &alice.sid,
        alice.generation,
        SessionRequest::AddTrack(add_track("v", TrackKind::Video, TrackSource::Camera)),
        now,
    );
    room.media_arrives(
        RemoteMedia {
            registration: crate::media_api::PublicationRegistration::Pending,
            participant: alice.sid.clone(),
            transport: ta,
            mid: "1".into(),
            kind: TrackKind::Video,
            msid_track_id: "v".into(),
            rids: vec![],
            mime_type: "video/VP8".into(),
        },
        now,
    );
    room.take_effects();
    let mut join = params("whip", 17, true);
    join.capabilities.one_shot = true;
    let bob = room.join(join, now).unwrap();
    let effects = room.take_effects();
    let track = effects
        .iter()
        .find_map(|effect| match effect {
            Effect::Media(MediaCommand::Subscribe(spec)) if spec.subscriber == bob.sid => {
                Some(spec.track_sid.clone())
            }
            _ => None,
        })
        .unwrap();
    let name = room.participants[&alice.sid]
        .info
        .tracks
        .iter()
        .find(|t| t.sid == track)
        .unwrap()
        .name
        .clone();
    let targets = vec![NamedSubscription {
        publisher: Identity::from("alice"),
        track_name: name,
    }];
    assert!(!room.named_subscriptions_prepared(&bob.sid, bob.generation, &targets));
    room.handle_media_event(
        MediaEvent::SubscriptionPrepared {
            subscriber: bob.sid.clone(),
            track_sid: track.clone(),
        },
        now,
    );
    assert!(room.named_subscriptions_prepared(&bob.sid, bob.generation, &targets));
    assert!(
        !room.participants[&bob.sid].subscriptions[&track].bound,
        "final answer binding must not be a prerequisite for constructing the answer"
    );
    assert!(!room.named_subscriptions_prepared(
        &bob.sid,
        SessionGeneration(bob.generation.0 + 1),
        &targets
    ));
    assert!(!room.named_subscriptions_prepared(
        &bob.sid,
        bob.generation,
        &[NamedSubscription {
            publisher: Identity::from("other"),
            track_name: targets[0].track_name.clone()
        }]
    ));
    room.handle_media_event(
        MediaEvent::SubscriptionFailed {
            subscriber: bob.sid.clone(),
            track_sid: track,
            error: SubscriptionError::CodecUnsupported,
        },
        now,
    );
    assert!(!room.named_subscriptions_prepared(&bob.sid, bob.generation, &targets));
}

#[test]
fn one_shot_dependent_accounting_does_not_change_sdk_participant_kind_or_capacity() {
    let (mut room, now) = room();
    room.info.max_participants = 1;
    room.join(params("regular", 17, true), now).unwrap();
    let mut one_shot = params("whip", 17, true);
    one_shot.capabilities.one_shot = true;
    let joined = room
        .join(one_shot, now)
        .expect("fixed source one-shot is dependent");
    assert_eq!(
        room.participants[&joined.sid].info.kind,
        ParticipantKind::Standard
    );
    assert!(room.participants[&joined.sid].is_dependent());
    assert!(matches!(
        room.join(params("other-regular", 17, true), now),
        Err(JoinError::MaxParticipants)
    ));
}
#[test]
fn one_shot_actual_video_sizes_update_real_track_codec_and_media_effect_without_touching_sdk_metadata()
 {
    let (mut room, now) = room();
    let mut join = params("whip", 17, true);
    join.capabilities.one_shot = true;
    let publisher = room.join(join, now).unwrap();
    let effects = room.take_effects();
    let transport = transport_of(&effects, &publisher.sid, PcRole::Single);
    let mut request = add_track("video", TrackKind::Video, TrackSource::Camera);
    request.layers = vec![VideoLayer {
        quality: VideoQuality::Low,
        width: 0,
        height: 0,
        bitrate: 0,
        ssrc: 0,
        spatial_layer: 0,
        rid: String::new(),
        repair_ssrc: 0,
    }];
    room.handle_request(
        &publisher.sid,
        publisher.generation,
        SessionRequest::AddTrack(request),
        now,
    );
    room.media_arrives(
        RemoteMedia {
            registration: crate::media_api::PublicationRegistration::Pending,
            participant: publisher.sid.clone(),
            transport,
            mid: "0".into(),
            kind: TrackKind::Video,
            msid_track_id: "video".into(),
            rids: vec![],
            mime_type: "video/VP8".into(),
        },
        now,
    );
    let track = room.participants[&publisher.sid].info.tracks[0].clone();
    room.take_effects();
    room.handle_media_event(
        MediaEvent::TrackVideoSizeChanged {
            track_sid: track.sid.clone(),
            mime_type: "video/VP8".into(),
            sizes: vec![(640, 360)],
        },
        now,
    );
    let updated = &room.participants[&publisher.sid].info.tracks[0];
    assert_eq!((updated.width, updated.height), (640, 360));
    assert_eq!(
        (
            updated.codecs[0].layers[0].width,
            updated.codecs[0].layers[0].height
        ),
        (640, 360)
    );
    assert_eq!(
        updated.layers, track.layers,
        "upstream updates matching codec layers, not deprecated flat layers"
    );
    assert_eq!(updated.version, track.version);
    let effects = room.take_effects();
    assert!(effects.iter().any(|e|matches!(e,Effect::Media(MediaCommand::UpdatePublishedTrack{track}) if track.codecs[0].layers[0].width==640)));
    assert_eq!(
        effects.len(),
        1,
        "source size update is receiver-only: no participant signaling, webhook, or store-change effect"
    );
    room.handle_media_event(
        MediaEvent::TrackVideoSizeChanged {
            track_sid: track.sid.clone(),
            mime_type: "video/VP8".into(),
            sizes: vec![(640, 360)],
        },
        now,
    );
    assert!(
        room.take_effects().is_empty(),
        "cached dimensions are idempotent"
    );
    room.participants
        .get_mut(&publisher.sid)
        .unwrap()
        .capabilities
        .one_shot = false;
    room.handle_media_event(
        MediaEvent::TrackVideoSizeChanged {
            track_sid: track.sid.clone(),
            mime_type: "video/VP8".into(),
            sizes: vec![(320, 180)],
        },
        now,
    );
    assert_eq!(room.participants[&publisher.sid].info.tracks[0].width, 640);
    assert!(room.take_effects().is_empty());
    room.participants
        .get_mut(&publisher.sid)
        .unwrap()
        .capabilities
        .one_shot = true;
    room.handle_request(
        &publisher.sid,
        publisher.generation,
        SessionRequest::Leave {
            reason: DisconnectReason::ClientInitiated,
        },
        now,
    );
    room.take_effects();
    room.handle_media_event(
        MediaEvent::TrackVideoSizeChanged {
            track_sid: track.sid,
            mime_type: "video/VP8".into(),
            sizes: vec![(1920, 1080)],
        },
        now,
    );
    assert!(
        room.take_effects().is_empty(),
        "old removed publication cannot change a new generation"
    );
}

#[test]
fn stored_api_view_keeps_private_native_dimensions_until_real_participant_change() {
    let (mut room, now) = room();
    let mut request = params("stored-native", 17, true);
    request.capabilities.one_shot = true;
    let publisher = room.join(request, now).unwrap();
    let transport = transport_of(&room.take_effects(), &publisher.sid, PcRole::Single);
    room.handle_request(
        &publisher.sid,
        publisher.generation,
        SessionRequest::AddTrack(add_track("video", TrackKind::Video, TrackSource::Camera)),
        now,
    );
    room.media_arrives(
        RemoteMedia {
            registration: crate::media_api::PublicationRegistration::Pending,
            participant: publisher.sid.clone(),
            transport,
            mid: "0".into(),
            kind: TrackKind::Video,
            msid_track_id: "video".into(),
            rids: vec![],
            mime_type: "video/VP8".into(),
        },
        now,
    );
    let identity = Identity("stored-native".into());
    let stored = room.get_participant_stored(&identity).unwrap();
    assert_eq!(
        stored.tracks.len(),
        1,
        "actual publication is stored before native size arrives"
    );
    let track = stored.tracks[0].sid.clone();
    room.take_effects();
    room.handle_media_event(
        MediaEvent::TrackVideoSizeChanged {
            track_sid: track.clone(),
            mime_type: "video/VP8".into(),
            sizes: vec![(640, 360)],
        },
        now,
    );
    assert_eq!(
        room.get_participant(&identity).unwrap().tracks[0].width,
        640
    );
    assert_eq!(
        room.get_participant_stored(&identity).unwrap().tracks[0].width,
        0
    );
    assert_eq!(room.list_participants_stored()[0].tracks[0].width, 0);
    assert!(
        room.take_effects()
            .iter()
            .all(|e| matches!(e, Effect::Media(MediaCommand::UpdatePublishedTrack { .. })))
    );
    // A no-op update and a normal batch flush cannot turn private receiver changes into store writes.
    room.api_update_participant(&identity, "", "", &BTreeMap::new(), None, now)
        .unwrap();
    room.flush_batched_updates();
    assert_eq!(
        room.get_participant_stored(&identity).unwrap().tracks[0].width,
        0
    );
    room.api_update_participant(
        &identity,
        "",
        "actual-metadata",
        &BTreeMap::new(),
        None,
        now,
    )
    .unwrap();
    let refreshed = room.get_participant_stored(&identity).unwrap();
    assert_eq!(refreshed.metadata, "actual-metadata");
    assert_eq!(
        (refreshed.tracks[0].width, refreshed.tracks[0].height),
        (640, 360)
    );
    assert_eq!(refreshed.sid, stored.sid);
    room.handle_media_event(
        MediaEvent::TrackVideoSizeChanged {
            track_sid: track,
            mime_type: "video/VP8".into(),
            sizes: vec![(960, 540)],
        },
        now,
    );
    assert_eq!(
        room.get_participant(&identity).unwrap().tracks[0].width,
        960
    );
    assert_eq!(
        room.get_participant_stored(&identity).unwrap().tracks[0].width,
        640
    );
    room.api_remove_participant(&identity, now).unwrap();
    assert!(room.get_participant_stored(&identity).is_err());
    assert!(room.list_participants_stored().is_empty());
    let replacement = room.join(params("stored-native", 17, true), now).unwrap();
    assert_ne!(replacement.sid, stored.sid);
    assert!(
        room.get_participant_stored(&identity)
            .unwrap()
            .tracks
            .is_empty()
    );
}

#[test]
fn hidden_publication_and_real_mute_are_stored_without_read_refresh() {
    let (mut room, now) = room();
    let mut request = params("hidden-stored", 17, true);
    request.grants.video.hidden = true;
    let publisher = room.join(request, now).unwrap();
    let transport = transport_of(&room.take_effects(), &publisher.sid, PcRole::Single);
    room.handle_request(
        &publisher.sid,
        publisher.generation,
        SessionRequest::AddTrack(add_track(
            "audio",
            TrackKind::Audio,
            TrackSource::Microphone,
        )),
        now,
    );
    room.media_arrives(
        RemoteMedia {
            registration: crate::media_api::PublicationRegistration::Pending,
            participant: publisher.sid.clone(),
            transport,
            mid: "0".into(),
            kind: TrackKind::Audio,
            msid_track_id: "audio".into(),
            rids: vec![],
            mime_type: "audio/opus".into(),
        },
        now,
    );
    let identity = Identity("hidden-stored".into());
    let stored = room.get_participant_stored(&identity).unwrap();
    assert_eq!(
        stored.tracks.len(),
        1,
        "hidden does not suppress store callbacks"
    );
    assert!(stored.permission.hidden);
    room.api_mute_track(&identity, &stored.tracks[0].sid, true, now)
        .unwrap();
    assert!(room.get_participant_stored(&identity).unwrap().tracks[0].muted);
}

#[test]
fn participant_blob_store_get_is_private_state_without_publish_or_subscribe_grants() {
    let (mut r, now) = room();
    Arc::make_mut(&mut r.policy).enable_participant_data_blob = true;
    let mut a = params("blob-a", 18, false);
    a.grants.video.can_publish_data = Some(false);
    a.grants.video.can_subscribe = Some(false);
    a.grants.video.can_publish = Some(false);
    let a = r.join(a, now).unwrap();
    let b = r.join(params("blob-b", 18, false), now).unwrap();
    r.take_effects();
    let key = DataBlobKey::generic("schema-定义".into());
    let blob = DataBlob {
        wire: None,
        key: Some(key.clone()),
        contents: vec![0, 255, 1, 127],
    };
    let before = (
        r.participants[&a.sid].dirty,
        r.participants[&a.sid].info.version,
    );
    r.handle_request(
        &a.sid,
        a.generation,
        SessionRequest::StoreDataBlob {
            request_id: 5,
            blob: Some(blob.clone()),
        },
        now,
    );
    let effects = r.take_effects();
    assert!(
        matches!(effects.as_slice(), [Effect::Signal { to, event: SignalEvent::DataBlobStored { request_id: 5, key: actual } }] if to == &a.sid && actual == &key)
    );
    assert_eq!(
        before,
        (
            r.participants[&a.sid].dirty,
            r.participants[&a.sid].info.version
        )
    );
    r.handle_request(
        &b.sid,
        b.generation,
        SessionRequest::GetDataBlob {
            request_id: 6,
            participant_identity: Identity("blob-a".into()),
            key: Some(key),
        },
        now,
    );
    assert!(
        matches!(r.take_effects().as_slice(), [Effect::Signal { to, event: SignalEvent::DataBlobRetrieved { request_id: 6, blob: actual } }] if to == &b.sid && actual == &blob)
    );
}

#[test]
fn participant_blob_disabled_store_does_not_disable_get_validation_or_lookup() {
    let (mut r, now) = room();
    let a = r.join(params("blob-a", 18, false), now).unwrap();
    r.take_effects();
    let cases = [
        (
            SessionRequest::StoreDataBlob {
                request_id: 1,
                blob: None,
            },
            RequestOutcome::NotAllowed,
            "data blob not enabled",
        ),
        (
            SessionRequest::GetDataBlob {
                request_id: 2,
                participant_identity: Identity("missing".into()),
                key: None,
            },
            RequestOutcome::InvalidRequest,
            "data blob key is required",
        ),
        (
            SessionRequest::GetDataBlob {
                request_id: 3,
                participant_identity: Identity("missing".into()),
                key: Some(DataBlobKey::generic("x".into())),
            },
            RequestOutcome::NotFound,
            "participant not found",
        ),
        (
            SessionRequest::GetDataBlob {
                request_id: 4,
                participant_identity: Identity("blob-a".into()),
                key: Some(DataBlobKey::unset()),
            },
            RequestOutcome::NotFound,
            "data blob not found",
        ),
    ];
    for (request, expected, text) in cases {
        r.handle_request(&a.sid, a.generation, request, now);
        assert!(
            matches!(r.take_effects().as_slice(), [Effect::Signal { event: SignalEvent::RequestResponse { outcome, message, .. }, .. }] if *outcome == expected && message == text)
        );
    }
}

#[test]
fn participant_blob_overwrite_quota_includes_old_value_and_rejected_write_keeps_content() {
    let (mut r, now) = room();
    let policy = Arc::make_mut(&mut r.policy);
    policy.enable_participant_data_blob = true;
    policy.max_data_blob_size = 5;
    let a = r.join(params("blob-a", 18, false), now).unwrap();
    r.take_effects();
    let key = DataBlobKey::generic("x".into());
    let first = DataBlob {
        wire: None,
        key: Some(key.clone()),
        contents: vec![1, 2],
    };
    r.handle_request(
        &a.sid,
        a.generation,
        SessionRequest::StoreDataBlob {
            request_id: 1,
            blob: Some(first.clone()),
        },
        now,
    );
    assert!(matches!(
        r.take_effects().as_slice(),
        [Effect::Signal {
            event: SignalEvent::DataBlobStored { .. },
            ..
        }]
    ));
    r.handle_request(
        &a.sid,
        a.generation,
        SessionRequest::StoreDataBlob {
            request_id: 2,
            blob: Some(DataBlob {
                wire: None,
                key: Some(key.clone()),
                contents: vec![3, 4],
            }),
        },
        now,
    );
    assert!(
        matches!(r.take_effects().as_slice(), [Effect::Signal { event: SignalEvent::RequestResponse { outcome: RequestOutcome::LimitExceeded, message, .. }, .. }] if message == "async attribute definition exceeds limit")
    );
    r.handle_request(
        &a.sid,
        a.generation,
        SessionRequest::GetDataBlob {
            request_id: 3,
            participant_identity: Identity("blob-a".into()),
            key: Some(key),
        },
        now,
    );
    assert!(
        matches!(r.take_effects().as_slice(), [Effect::Signal { event: SignalEvent::DataBlobRetrieved { blob, .. }, .. }] if blob == &first)
    );
}

#[test]
fn participant_blob_utf8_signed_key_limit_validation_order_and_new_generation() {
    let (mut r, now) = room();
    let policy = Arc::make_mut(&mut r.policy);
    policy.enable_participant_data_blob = true;
    policy.max_data_blob_key_length = 2;
    let a = r.join(params("blob-a", 18, false), now).unwrap();
    r.take_effects();
    let request = || SessionRequest::StoreDataBlob {
        request_id: 1,
        blob: Some(DataBlob {
            wire: None,
            key: Some(DataBlobKey::generic("汉".into())),
            contents: vec![1],
        }),
    };
    r.handle_request(&a.sid, a.generation, request(), now);
    assert!(
        matches!(r.take_effects().as_slice(), [Effect::Signal { event: SignalEvent::RequestResponse { outcome: RequestOutcome::InvalidRequest, message, .. }, .. }] if message == "data blob is invalid")
    );
    Arc::make_mut(&mut r.policy).max_data_blob_key_length = -1;
    r.handle_request(&a.sid, a.generation, request(), now);
    assert!(matches!(
        r.take_effects().as_slice(),
        [Effect::Signal {
            event: SignalEvent::RequestResponse {
                outcome: RequestOutcome::InvalidRequest,
                ..
            },
            ..
        }]
    ));
    Arc::make_mut(&mut r.policy).max_data_blob_key_length = 0;
    r.handle_request(&a.sid, a.generation, request(), now);
    assert!(matches!(
        r.take_effects().as_slice(),
        [Effect::Signal {
            event: SignalEvent::DataBlobStored { .. },
            ..
        }]
    ));
    r.remove_participant(&a.sid, DisconnectReason::ClientInitiated, false, now);
    r.take_effects();
    let b = r.join(params("blob-a", 18, false), now).unwrap();
    r.take_effects();
    r.handle_request(&a.sid, a.generation, request(), now);
    assert!(r.take_effects().is_empty());
    r.handle_request(
        &b.sid,
        b.generation,
        SessionRequest::GetDataBlob {
            request_id: 2,
            participant_identity: Identity("blob-a".into()),
            key: Some(DataBlobKey::generic("汉".into())),
        },
        now,
    );
    assert!(matches!(
        r.take_effects().as_slice(),
        [Effect::Signal {
            event: SignalEvent::RequestResponse {
                outcome: RequestOutcome::NotFound,
                ..
            },
            ..
        }]
    ));
}

#[test]
fn negative_name_limit_checks_even_omitted_name_but_keeps_permission_and_update_order() {
    for limit in [-1, 0, 3] {
        for permitted in [false, true] {
            let (mut room, now) = room();
            Arc::make_mut(&mut room.policy).max_participant_name_length = limit;
            let mut join = params("name-limits", 17, true);
            join.name = "existing".into();
            join.grants.video.can_update_own_metadata = Some(permitted);
            let actor = room.join(join, now).unwrap();
            room.take_effects();
            for (id, name, expected_name) in [(1, "", "existing"), (2, "éa", "éa"), (3, "éé", "éa")]
            {
                room.handle_request(
                    &actor.sid,
                    actor.generation,
                    SessionRequest::UpdateMetadata {
                        token: id,
                        request_id: id,
                        name: name.into(),
                        metadata: format!("attempt-{id}"),
                        attributes: BTreeMap::new(),
                    },
                    now,
                );
                let effects = room.take_effects();
                let signals = signals_to(&effects, &actor.sid);
                let rejected =
                    !permitted || limit < 0 || (limit > 0 && name.len() > limit as usize);
                let expected = if !permitted {
                    RequestOutcome::NotAllowed
                } else if rejected {
                    RequestOutcome::LimitExceeded
                } else {
                    RequestOutcome::Ok
                };
                assert!(signals.iter().any(|e|matches!(e,SignalEvent::RequestResponse{request_id,outcome,..}if *request_id==id&&*outcome==expected)));
                let info = room.get_participant(&"name-limits".into()).unwrap();
                if !permitted || limit < 0 {
                    assert_eq!(info.name, "existing");
                    assert!(info.metadata.is_empty());
                } else if !rejected {
                    assert_eq!(info.name, if name.is_empty() { "existing" } else { name });
                    assert_eq!(info.metadata, format!("attempt-{id}"));
                } else {
                    assert_eq!(info.name, expected_name);
                    assert_eq!(info.metadata, "attempt-2");
                }
            }
        }
    }
}

#[test]
fn reconnect_publication_flag_consumes_real_addtrack_connected_timeout_and_replacement() {
    for enabled in [false, true] {
        let (mut room, now) = room();
        Arc::make_mut(&mut room.policy).reconnect_on_publication_error = enabled;
        let p = room.join(params("publication", 17, true), now).unwrap();
        let tid = transport_of(&room.take_effects(), &p.sid, PcRole::Single);
        room.handle_request(
            &p.sid,
            p.generation,
            SessionRequest::AddTrack(add_track(
                "never-sends-rtp",
                TrackKind::Audio,
                TrackSource::Microphone,
            )),
            now,
        );
        room.take_effects();
        room.tick(now + Duration::from_secs(31));
        assert!(
            room.participants.contains_key(&p.sid),
            "not connected does not start the publish timeout"
        );
        room.take_effects();
        room.handle_media_event(
            MediaEvent::TransportConnected {
                participant: p.sid.clone(),
                transport: tid,
            },
            now + Duration::from_secs(32),
        );
        room.take_effects();
        room.tick(now + Duration::from_secs(62));
        assert!(
            room.participants.contains_key(&p.sid),
            "30s exact is not timeout"
        );
        room.take_effects();
        // Repeated connection receipt cannot keep extending the original window.
        room.handle_media_event(
            MediaEvent::TransportConnected {
                participant: p.sid.clone(),
                transport: tid,
            },
            now + Duration::from_secs(62),
        );
        room.take_effects();
        room.tick(now + Duration::from_secs(63));
        let effects = room.take_effects();
        assert_eq!(room.participants.contains_key(&p.sid), !enabled);
        assert_eq!(
            signals_to(&effects, &p.sid).iter().any(|e| matches!(
                e,
                SignalEvent::Leave {
                    reason: DisconnectReason::StateMismatch,
                    action: LeaveAction::Reconnect
                }
            )),
            enabled
        );
        if enabled {
            assert!(
                effects
                    .iter()
                    .any(|e| matches!(e,Effect::CloseSignal{to}if to==&p.sid))
            );
            let next = room
                .join(
                    params("publication", 17, true),
                    now + Duration::from_secs(64),
                )
                .unwrap();
            assert_ne!(p.sid, next.sid);
            room.take_effects();
            room.tick(now + Duration::from_secs(65));
            assert!(
                signals_to(&room.take_effects(), &next.sid)
                    .iter()
                    .all(|e| !matches!(e, SignalEvent::Leave { .. }))
            );
        }
    }
}

#[test]
fn reconnect_data_error_uses_owned_transport_and_flag_not_cached_or_foreign_identity() {
    for enabled in [false, true] {
        let (mut room, now) = room();
        Arc::make_mut(&mut room.policy).reconnect_on_data_channel_error = enabled;
        let p = room.join(params("channel", 17, true), now).unwrap();
        let tid = transport_of(&room.take_effects(), &p.sid, PcRole::Single);
        room.handle_media_event(
            MediaEvent::DataSendError {
                participant: p.sid.clone(),
                transport: TransportId::next(),
            },
            now,
        );
        assert!(room.take_effects().is_empty());
        assert!(room.participants.contains_key(&p.sid));
        room.handle_media_event(
            MediaEvent::DataSendError {
                participant: p.sid.clone(),
                transport: tid,
            },
            now,
        );
        let effects = room.take_effects();
        assert_eq!(
            signals_to(&effects, &p.sid).iter().any(|e| matches!(
                e,
                SignalEvent::Leave {
                    action: LeaveAction::Reconnect,
                    ..
                }
            )),
            enabled
        );
        if enabled {
            let next = room.join(params("channel", 17, true), now).unwrap();
            room.take_effects();
            room.handle_media_event(
                MediaEvent::DataSendError {
                    participant: next.sid.clone(),
                    transport: tid,
                },
                now,
            );
            assert!(room.participants.contains_key(&next.sid));
            assert!(room.take_effects().is_empty());
        }
    }
}

#[test]
fn reconnect_subscription_flag_requires_actual_subscription_preparation_and_elapsed_active_wait() {
    for enabled in [false, true] {
        let (mut room, now) = room();
        Arc::make_mut(&mut room.policy).reconnect_on_subscription_error = enabled;
        let publisher = room.join(params("publisher", 17, true), now).unwrap();
        let pubtid = transport_of(&room.take_effects(), &publisher.sid, PcRole::Single);
        room.handle_request(
            &publisher.sid,
            publisher.generation,
            SessionRequest::AddTrack(add_track(
                "real-source",
                TrackKind::Audio,
                TrackSource::Microphone,
            )),
            now,
        );
        room.take_effects();
        room.media_arrives(
            RemoteMedia {
                registration: crate::media_api::PublicationRegistration::Pending,
                participant: publisher.sid.clone(),
                transport: pubtid,
                mid: "0".into(),
                kind: TrackKind::Audio,
                msid_track_id: "real-source".into(),
                rids: vec![],
                mime_type: "audio/opus".into(),
            },
            now,
        );
        let track = room.participants[&publisher.sid].info.tracks[0].sid.clone();
        room.take_effects();
        let subscriber = room.join(params("subscriber", 17, true), now).unwrap();
        let subtid = transport_of(&room.take_effects(), &subscriber.sid, PcRole::Single);
        room.handle_media_event(
            MediaEvent::TransportConnected {
                participant: subscriber.sid.clone(),
                transport: subtid,
            },
            now,
        );
        room.take_effects();
        assert!(room.participants[&subscriber.sid].subscriptions[&track].requested);
        room.handle_media_event(
            MediaEvent::SubscriptionPrepared {
                subscriber: subscriber.sid.clone(),
                track_sid: track.clone(),
            },
            now,
        );
        room.take_effects();
        room.tick(now + Duration::from_secs(15));
        assert!(room.participants.contains_key(&subscriber.sid));
        room.take_effects();
        room.tick(now + Duration::from_secs(16));
        let effects = room.take_effects();
        let signals = signals_to(&effects, &subscriber.sid);
        let response = signals
            .iter()
            .position(
                |e| matches!(e,SignalEvent::SubscriptionResponse{track_sid,..}if track_sid==&track),
            )
            .expect("real unbound timeout response");
        let leave = signals.iter().position(|e| {
            matches!(
                e,
                SignalEvent::Leave {
                    action: LeaveAction::Reconnect,
                    ..
                }
            )
        });
        assert_eq!(leave.is_some(), enabled);
        if let Some(leave) = leave {
            assert!(response < leave);
        }
    }
}

#[test]
fn reconnect_registered_sdp_publication_silence_does_not_require_first_complete_frame() {
    for kind in [TrackKind::Audio, TrackKind::Video] {
        let (mut room, now) = room();
        Arc::make_mut(&mut room.policy).reconnect_on_publication_error = true;
        let p = room.join(params("silent-sdp", 17, true), now).unwrap();
        let tid = transport_of(&room.take_effects(), &p.sid, PcRole::Single);
        let (source, mime) = if kind == TrackKind::Audio {
            (TrackSource::Microphone, "audio/opus")
        } else {
            (TrackSource::Camera, "video/VP8")
        };
        room.handle_request(
            &p.sid,
            p.generation,
            SessionRequest::AddTrack(add_track("registered", kind, source)),
            now,
        );
        room.take_effects();
        // Actual accepted SDP receiver event, not a synthetic TrackMediaStarted.
        room.media_arrives(
            RemoteMedia {
                registration: crate::media_api::PublicationRegistration::SdpReceiver,
                participant: p.sid.clone(),
                transport: tid,
                mid: "0".into(),
                kind,
                msid_track_id: "registered".into(),
                rids: vec![],
                mime_type: mime.into(),
            },
            now,
        );
        assert_eq!(room.participants[&p.sid].info.tracks.len(), 1);
        room.handle_media_event(
            MediaEvent::TransportConnected {
                participant: p.sid.clone(),
                transport: tid,
            },
            now,
        );
        room.take_effects();
        room.tick(now + Duration::from_secs(31));
        assert!(room.participants.contains_key(&p.sid));
        assert!(
            signals_to(&room.take_effects(), &p.sid)
                .iter()
                .all(|e| !matches!(e, SignalEvent::Leave { .. }))
        );
    }
}

#[test]
fn reconnect_historical_failure_does_not_survive_actual_unpublish_or_permission_revocation() {
    for invalidate in [0, 1, 2] {
        for error in [
            SubscriptionError::CodecUnsupported,
            SubscriptionError::Unknown,
        ] {
            let (mut room, now) = room();
            Arc::make_mut(&mut room.policy).reconnect_on_subscription_error = true;
            let publisher = room.join(params("publisher", 17, true), now).unwrap();
            let tid = transport_of(&room.take_effects(), &publisher.sid, PcRole::Single);
            room.handle_request(
                &publisher.sid,
                publisher.generation,
                SessionRequest::AddTrack(add_track(
                    "source",
                    TrackKind::Audio,
                    TrackSource::Microphone,
                )),
                now,
            );
            room.take_effects();
            room.media_arrives(
                RemoteMedia {
                    registration: crate::media_api::PublicationRegistration::Pending,
                    participant: publisher.sid.clone(),
                    transport: tid,
                    mid: "0".into(),
                    kind: TrackKind::Audio,
                    msid_track_id: "source".into(),
                    rids: vec![],
                    mime_type: "audio/opus".into(),
                },
                now,
            );
            let track = room.participants[&publisher.sid].info.tracks[0].sid.clone();
            room.take_effects();
            let subscriber = room.join(params("subscriber", 17, true), now).unwrap();
            let stid = transport_of(&room.take_effects(), &subscriber.sid, PcRole::Single);
            room.handle_media_event(
                MediaEvent::TransportConnected {
                    participant: subscriber.sid.clone(),
                    transport: stid,
                },
                now,
            );
            room.handle_media_event(
                MediaEvent::SubscriptionPrepared {
                    subscriber: subscriber.sid.clone(),
                    track_sid: track.clone(),
                },
                now,
            );
            room.handle_media_event(
                MediaEvent::SubscriptionFailed {
                    subscriber: subscriber.sid.clone(),
                    track_sid: track.clone(),
                    error,
                },
                now,
            );
            assert_eq!(
                room.subscription_monitors[&(subscriber.sid.clone(), track.clone())].failed,
                Some(error)
            );
            if invalidate == 1 {
                room.handle_request(
                    &publisher.sid,
                    publisher.generation,
                    SessionRequest::SubscriptionPermission(SubscriptionPermission {
                        all_participants: false,
                        track_permissions: vec![],
                    }),
                    now + Duration::from_secs(1),
                );
            } else if invalidate == 2 {
                room.handle_media_event(
                    MediaEvent::RemoteMediaStopped {
                        participant: publisher.sid.clone(),
                        track_sid: track.clone(),
                    },
                    now + Duration::from_secs(1),
                );
            }
            room.take_effects();
            room.tick(now + Duration::from_secs(16));
            assert_eq!(
                room.participants.contains_key(&subscriber.sid),
                invalidate != 0
            );
            let effects = room.take_effects();
            assert_eq!(
                signals_to(&effects, &subscriber.sid)
                    .iter()
                    .any(|e| matches!(e, SignalEvent::Leave { .. })),
                invalidate == 0
            );
            if invalidate != 0 {
                assert!(
                    room.subscription_monitors
                        .get(&(subscriber.sid.clone(), track.clone()))
                        .is_none_or(|m| m.failed.is_none())
                );
            }
        }
    }
}

#[test]
fn reconnect_rtp_fired_profiles_wait_for_real_registration_event_and_reject_old_generation() {
    use crate::media_api::PublicationRegistration;
    for profile in ["go-video", "js-rid", "js-zero-ssrc"] {
        for supply_packet in [false, true] {
            let (mut room, now) = room();
            Arc::make_mut(&mut room.policy).reconnect_on_publication_error = true;
            let mut join = params("rtp-publication", 17, true);
            join.capabilities.video_publication_requires_rtp = profile == "go-video";
            let p = room.join(join, now).unwrap();
            let tid = transport_of(&room.take_effects(), &p.sid, PcRole::Single);
            room.handle_request(
                &p.sid,
                p.generation,
                SessionRequest::AddTrack(add_track(
                    "first-packet",
                    TrackKind::Video,
                    TrackSource::Camera,
                )),
                now,
            );
            room.take_effects();
            room.media_arrives(
                RemoteMedia {
                    // Go must still reject SDP-only completion, even if the SDP
                    // receiver itself was eligible. RID/SSRC0 never has that proof.
                    registration: if profile == "go-video" {
                        PublicationRegistration::SdpReceiver
                    } else {
                        PublicationRegistration::Pending
                    },
                    participant: p.sid.clone(),
                    transport: tid,
                    mid: "0".into(),
                    kind: TrackKind::Video,
                    msid_track_id: "first-packet".into(),
                    rids: if profile == "js-rid" {
                        vec!["f".into()]
                    } else {
                        vec![]
                    },
                    mime_type: "video/VP8".into(),
                },
                now,
            );
            let sid = room.participants[&p.sid].info.tracks[0].sid.clone();
            room.handle_media_event(
                MediaEvent::TransportConnected {
                    participant: p.sid.clone(),
                    transport: tid,
                },
                now,
            );
            // Foreign transport and track generation can never complete it.
            for (transport, track_sid) in [
                (TransportId::next(), sid.clone()),
                (tid, TrackSid::from("TR_replaced")),
            ] {
                room.handle_media_event(
                    MediaEvent::PublicationRtpRegistered {
                        participant: p.sid.clone(),
                        transport,
                        track_sid,
                        mime_type: "video/VP8".into(),
                        payload_type: 96,
                    },
                    now,
                );
            }
            room.handle_media_event(
                MediaEvent::PublicationRtpRegistered {
                    participant: p.sid.clone(),
                    transport: tid,
                    track_sid: sid.clone(),
                    mime_type: "audio/opus".into(),
                    payload_type: 111,
                },
                now,
            );
            if profile == "go-video" {
                room.handle_media_event(
                    MediaEvent::PublicationRtpRegistered {
                        participant: p.sid.clone(),
                        transport: tid,
                        track_sid: sid.clone(),
                        mime_type: "video/VP8".into(),
                        payload_type: 0,
                    },
                    now,
                );
            }
            if supply_packet {
                room.handle_media_event(
                    MediaEvent::PublicationRtpRegistered {
                        participant: p.sid.clone(),
                        transport: tid,
                        track_sid: sid,
                        mime_type: "video/VP8".into(),
                        payload_type: 96,
                    },
                    now + Duration::from_secs(1),
                );
            }
            room.take_effects();
            room.tick(now + Duration::from_secs(31));
            assert_eq!(
                room.participants.contains_key(&p.sid),
                supply_packet,
                "{profile}"
            );
            assert_eq!(
                signals_to(&room.take_effects(), &p.sid)
                    .iter()
                    .any(|e| matches!(e, SignalEvent::Leave { .. })),
                !supply_packet
            );
        }
    }
}

#[test]
fn owner_local_update_checks_existing_participant_before_limits_and_does_not_mutate_permission_on_error()
 {
    let (mut room, now) = room();
    let p = room.join(params("owner-limit", 17, true), now).unwrap();
    room.take_effects();
    Arc::make_mut(&mut room.policy).max_participant_name_length = -1;
    assert_eq!(
        room.api_update_participant(&"missing".into(), "", "", &BTreeMap::new(), None, now),
        Err(OpError::NotFound("participant does not exist"))
    );
    let before = room.get_participant(&"owner-limit".into()).unwrap();
    let mut changed = before.permission.clone();
    changed.can_publish = !changed.can_publish;
    assert_eq!(
        room.api_update_participant(
            &"owner-limit".into(),
            "",
            "new-metadata",
            &BTreeMap::new(),
            Some(changed),
            now
        )
        .unwrap_err()
        .to_string(),
        "name length exceeds limits"
    );
    let after = room.get_participant(&"owner-limit".into()).unwrap();
    assert_eq!(after.name, before.name);
    assert_eq!(after.metadata, before.metadata);
    assert_eq!(after.permission, before.permission);
    assert!(signals_to(&room.take_effects(),&p.sid).iter().any(|e|matches!(e,SignalEvent::OwnedMetadataRequestResponse{request,outcome:RequestOutcome::LimitExceeded,message}if request.request_id==0 && request.name.is_empty() && request.metadata=="new-metadata" && request.attributes.is_empty() && message=="exceeds name length limit")));
    Arc::make_mut(&mut room.policy).max_participant_name_length = 0;
    let accepted = room
        .api_update_participant(
            &"owner-limit".into(),
            "é".repeat(300).as_str(),
            "new-metadata",
            &BTreeMap::new(),
            None,
            now,
        )
        .unwrap();
    assert_eq!(accepted.name, "é".repeat(300));
    assert_eq!(accepted.metadata, "new-metadata");
}

#[test]
fn owner_local_metadata_limits_keep_order_and_no_write_for_empty_optional_name() {
    let (mut room, now) = room();
    let p = room.join(params("owner-metadata", 17, true), now).unwrap();
    room.take_effects();
    let policy = Arc::make_mut(&mut room.policy);
    policy.max_participant_name_length = 0;
    policy.max_metadata_size = Some(1);
    policy.max_attributes_size = Some(1);
    for (metadata, attrs, cause, message) in [
        (
            "xx",
            BTreeMap::from([("x".into(), "x".into())]),
            "metadata size exceeds limits",
            "exceeds metadata size limit",
        ),
        (
            "",
            BTreeMap::from([("x".into(), "x".into())]),
            "attributes size exceeds limits",
            "exceeds attributes size limit",
        ),
    ] {
        assert_eq!(
            room.api_update_participant(&"owner-metadata".into(), "", metadata, &attrs, None, now)
                .unwrap_err()
                .to_string(),
            cause
        );
        let info = room.get_participant(&"owner-metadata".into()).unwrap();
        assert!(info.metadata.is_empty());
        assert!(info.attributes.is_empty());
        assert!(signals_to(&room.take_effects(),&p.sid).iter().any(|e|matches!(e,SignalEvent::OwnedMetadataRequestResponse{request,outcome:RequestOutcome::LimitExceeded,message:actual}if actual==message && request.request_id==0 && request.name.is_empty() && request.metadata==metadata && request.attributes==attrs)));
    }
}

#[test]
fn owner_local_api_rejection_preserves_state_and_permission() {
    let (mut room, now) = room();
    let p = room.join(params("owner-red", 17, true), now).unwrap();
    room.take_effects();
    for (limit, metadata_limit, attributes_limit, name, metadata, attributes, cause) in [
        (
            -1,
            None,
            None,
            "",
            "new",
            BTreeMap::new(),
            "name length exceeds limits",
        ),
        (
            0,
            Some(1),
            Some(1),
            "",
            "xx",
            BTreeMap::from([("x".into(), "x".into())]),
            "metadata size exceeds limits",
        ),
        (
            0,
            None,
            Some(1),
            "",
            "",
            BTreeMap::from([("x".into(), "x".into())]),
            "attributes size exceeds limits",
        ),
    ] {
        let policy = Arc::make_mut(&mut room.policy);
        policy.max_participant_name_length = limit;
        policy.max_metadata_size = metadata_limit;
        policy.max_attributes_size = attributes_limit;
        assert!(
            matches!(
                room.api_update_participant(
                    &"missing".into(),
                    name,
                    metadata,
                    &attributes,
                    None,
                    now
                ),
                Err(OpError::NotFound(_))
            ),
            "actual missing-participant lookup precedes metadata validation"
        );
        let before = room.get_participant(&"owner-red".into()).unwrap();
        let mut permission = before.permission.clone();
        permission.can_publish = !permission.can_publish;
        let error = room
            .api_update_participant(
                &"owner-red".into(),
                name,
                metadata,
                &attributes,
                Some(permission),
                now,
            )
            .unwrap_err();
        assert_eq!(error.to_string(), cause);
        let after = room.get_participant(&"owner-red".into()).unwrap();
        assert_eq!(after.name, before.name);
        assert_eq!(after.metadata, before.metadata);
        assert_eq!(after.attributes, before.attributes);
        assert_eq!(after.permission, before.permission);
        room.take_effects();
    }
    let policy = Arc::make_mut(&mut room.policy);
    policy.max_participant_name_length = 0;
    policy.max_metadata_size = None;
    policy.max_attributes_size = None;
    let accepted = room
        .api_update_participant(
            &"owner-red".into(),
            "éa",
            "accepted",
            &BTreeMap::from([("unicode".into(), "中文".into())]),
            None,
            now,
        )
        .unwrap();
    assert_eq!(accepted.name, "éa");
    assert_eq!(accepted.metadata, "accepted");
    assert_eq!(accepted.attributes["unicode"], "中文");
    assert!(room.participants.contains_key(&p.sid));
}

#[test]
fn owner_admin_rejection_echo_respects_capability_and_success_has_no_admin_ack() {
    for responses in [false, true] {
        let (mut room, now) = room();
        let mut join = params("owner-caps", 17, true);
        join.capabilities.request_response = responses;
        join.grants.video.can_update_own_metadata = Some(false);
        let actor = room.join(join, now).unwrap();
        room.take_effects();
        Arc::make_mut(&mut room.policy).max_participant_name_length = -1;
        assert_eq!(
            room.api_update_participant(
                &"owner-caps".into(),
                "",
                "attempt",
                &BTreeMap::new(),
                None,
                now
            )
            .unwrap_err()
            .to_string(),
            "name length exceeds limits"
        );
        let effects = room.take_effects();
        assert_eq!(
            signals_to(&effects, &actor.sid)
                .iter()
                .filter(|e| matches!(e, SignalEvent::OwnedMetadataRequestResponse { .. }))
                .count(),
            usize::from(responses)
        );
        Arc::make_mut(&mut room.policy).max_participant_name_length = 0;
        let info = room
            .api_update_participant(
                &"owner-caps".into(),
                "changed",
                "accepted",
                &BTreeMap::new(),
                None,
                now,
            )
            .unwrap();
        assert_eq!(info.name, "changed");
        assert_eq!(info.metadata, "accepted");
        assert!(
            !signals_to(&room.take_effects(), &actor.sid)
                .iter()
                .any(|e| matches!(
                    e,
                    SignalEvent::OwnedMetadataRequestResponse { .. }
                        | SignalEvent::RequestResponse { .. }
                )),
            "admin request0 success does not fabricate response, regardless of own-metadata permission"
        );
    }
}

mod missing_subscription_source;

mod permission_restore_epoch;

mod subscription_settings_lifetime;

#[test]
fn public_admin_nonempty_attribute_noops_still_advance_version_and_claims() {
    let (mut room, now) = room();
    let joined = room.join(params("attribute-owner", 17, true), now).unwrap();
    let identity = Identity::from("attribute-owner");
    room.take_effects();
    let first = room
        .api_update_participant(
            &identity,
            "",
            "",
            &BTreeMap::from([("k".into(), "v".into())]),
            None,
            now,
        )
        .unwrap();
    room.take_effects();
    let mut previous = first;
    for patch in [
        BTreeMap::from([("k".into(), "v".into())]),
        BTreeMap::from([("absent".into(), String::new())]),
    ] {
        let updated = room
            .api_update_participant(&identity, "", "", &patch, None, now)
            .unwrap();
        assert_eq!(updated.sid, previous.sid);
        assert_eq!(
            updated.attributes, previous.attributes,
            "same final data does not suppress accepted nonempty map callbacks"
        );
        assert_eq!(updated.version, previous.version.wrapping_add(1));
        assert_eq!(
            room.get_participant_stored(&identity).unwrap().version,
            updated.version
        );
        let effects = room.take_effects();
        assert_eq!(signals_to(&effects, &joined.sid).iter().filter(|event| matches!(event,
            SignalEvent::ClaimsChanged(info) if info.version == updated.version && info.attributes == updated.attributes
        )).count(), 1, "actual public update must emit new claims snapshot exactly once");
        previous = updated;
    }
    let empty = room
        .api_update_participant(&identity, "", "", &BTreeMap::new(), None, now)
        .unwrap();
    assert_eq!(
        empty.version, previous.version,
        "source len(attrs)==0 remains a true no-op"
    );
    assert!(
        !signals_to(&room.take_effects(), &joined.sid)
            .iter()
            .any(|event| matches!(event, SignalEvent::ClaimsChanged(_)))
    );
}

#[test]
fn public_self_metadata_nonempty_noop_obeys_permission_version_and_claims() {
    for allowed in [true, false] {
        let (mut room, now) = room();
        let mut join = params("self-attribute-owner", 17, true);
        join.grants.video.can_update_own_metadata = Some(allowed);
        let joined = room.join(join, now).unwrap();
        let identity = Identity::from("self-attribute-owner");
        room.api_update_participant(
            &identity,
            "",
            "",
            &BTreeMap::from([("k".into(), "v".into())]),
            None,
            now,
        )
        .unwrap();
        room.take_effects();
        for patch in [
            BTreeMap::from([("k".into(), "v".into())]),
            BTreeMap::from([("absent".into(), String::new())]),
        ] {
            let before = room.get_participant(&identity).unwrap();
            room.handle_request(
                &joined.sid,
                joined.generation,
                SessionRequest::UpdateMetadata {
                    token: 73,
                    request_id: 81,
                    name: String::new(),
                    metadata: String::new(),
                    attributes: patch,
                },
                now,
            );
            let after = room.get_participant(&identity).unwrap();
            assert_eq!(after.sid, before.sid);
            assert_eq!(after.attributes, before.attributes);
            assert_eq!(after.name, before.name);
            assert_eq!(after.metadata, before.metadata);
            let effects = room.take_effects();
            let signals = signals_to(&effects, &joined.sid);
            let expected = if allowed {
                RequestOutcome::Ok
            } else {
                RequestOutcome::NotAllowed
            };
            assert_eq!(signals.iter().filter(|signal| matches!(signal,
                SignalEvent::RequestResponse { token: 73, request_id: 81, outcome, .. } if *outcome == expected
            )).count(), 1, "real SessionRequest completion must be observed");
            let claims = signals.iter().filter(|signal| matches!(signal,
                SignalEvent::ClaimsChanged(info) if info.version == after.version && info.attributes == after.attributes
            )).count();
            if allowed {
                assert_eq!(after.version, before.version.wrapping_add(1));
                assert_eq!(claims, 1);
                assert_eq!(
                    room.get_participant_stored(&identity).unwrap().version,
                    after.version
                );
            } else {
                assert_eq!(after.version, before.version);
                assert_eq!(claims, 0);
                assert!(signals.iter().any(|signal| matches!(signal,
                    SignalEvent::RequestResponse { message, .. } if message == "does not have permission to update own metadata"
                )));
            }
        }
        // Empty map has the same permission gate, but is never an update.
        let before = room.get_participant(&identity).unwrap();
        room.handle_request(
            &joined.sid,
            joined.generation,
            SessionRequest::UpdateMetadata {
                token: 74,
                request_id: 82,
                name: String::new(),
                metadata: String::new(),
                attributes: BTreeMap::new(),
            },
            now,
        );
        let after = room.get_participant(&identity).unwrap();
        assert_eq!(after.version, before.version);
        assert_eq!(after.attributes, before.attributes);
        let signals = signals_to(&room.take_effects(), &joined.sid);
        assert!(
            !signals
                .iter()
                .any(|signal| matches!(signal, SignalEvent::ClaimsChanged(_)))
        );
        let expected = if allowed {
            RequestOutcome::Ok
        } else {
            RequestOutcome::NotAllowed
        };
        assert!(signals.iter().any(|signal| matches!(signal,
            SignalEvent::RequestResponse { token: 74, request_id: 82, outcome, .. } if *outcome == expected
        )));
    }
}

#[path = "tests/open_numeric_source.rs"]
mod open_numeric_source;

// Source owner state tests: public request/media-event APIs, not native RTP proof.
fn update_audio_owner_track(
    room: &mut Room,
    now: Instant,
    owner: &crate::room::Joined,
    transport: TransportId,
    kind: TrackKind,
    published: bool,
) -> TrackSid {
    let mut request = add_track("audio-owner-update", kind, TrackSource::Microphone);
    request.stereo = true;
    request.disable_dtx = true;
    room.handle_request(
        &owner.sid,
        owner.generation,
        SessionRequest::AddTrack(request),
        now,
    );
    let sid = signals_to(&room.take_effects(), &owner.sid)
        .into_iter()
        .find_map(|s| {
            if let SignalEvent::TrackPublished { track, .. } = s {
                Some(track.sid)
            } else {
                None
            }
        })
        .expect("real owner AddTrack response");
    if published {
        update_audio_owner_bind(room, now, owner, transport, kind);
        room.take_effects();
    }
    sid
}

fn update_audio_owner_bind(
    room: &mut Room,
    now: Instant,
    owner: &crate::room::Joined,
    transport: TransportId,
    kind: TrackKind,
) {
    room.media_arrives(
        RemoteMedia {
            registration: crate::media_api::PublicationRegistration::Pending,
            participant: owner.sid.clone(),
            transport,
            mid: "audio-owner-mid".into(),
            kind,
            msid_track_id: "audio-owner-update".into(),
            rids: vec![],
            mime_type: if kind == TrackKind::Audio {
                "audio/opus"
            } else {
                "video/VP8"
            }
            .into(),
        },
        now,
    );
}

fn update_audio_owner_request(
    room: &mut Room,
    owner: &crate::room::Joined,
    sid: &TrackSid,
    raw: &[i32],
    now: Instant,
) {
    room.handle_request(
        &owner.sid,
        owner.generation,
        SessionRequest::UpdateAudioTrack {
            token: 91,
            sid: sid.clone(),
            features: raw.iter().copied().map(AudioFeature::from_i32).collect(),
        },
        now,
    );
}

#[test]
fn old_public_update_audio_pending_sorts_flags_any_kind_without_broadcast() {
    for kind in [TrackKind::Audio, TrackKind::Video] {
        let (mut room, now) = room();
        let owner = room.join(params("alice", 16, true), now).unwrap();
        let transport = transport_of(&room.take_effects(), &owner.sid, PcRole::Single);
        let sid = update_audio_owner_track(&mut room, now, &owner, transport, kind, false);
        update_audio_owner_request(&mut room, &owner, &sid, &[99, -1, 4, 99], now);
        assert!(
            room.take_effects().is_empty(),
            "pending update has no published callback or response"
        );
        update_audio_owner_bind(&mut room, now, &owner, transport, kind);
        let effects = room.take_effects();
        let info = room.get_participant(&"alice".into()).unwrap();
        let track = info.tracks.iter().find(|t| t.sid == sid).unwrap();
        assert_eq!(
            track
                .audio_features
                .iter()
                .map(|f| f.as_i32())
                .collect::<Vec<_>>(),
            vec![-1, 4, 99]
        );
        assert!(
            track.stereo && !track.disable_dtx,
            "original Go Compact clears duplicate tail to TF_STEREO before the flags loop"
        );
        assert!(effects.iter().any(|e| matches!(e, Effect::Media(MediaCommand::UpdatePublishedTrack { track }) if track.stereo && !track.disable_dtx)), "actual promotion command consumes original Go compact-tail stereo side effect");
    }
}

#[test]
fn old_public_update_audio_published_sort_noop_preserves_track_version_but_broadcasts() {
    let (mut room, now) = room();
    let owner = room.join(params("alice", 16, true), now).unwrap();
    let transport = transport_of(&room.take_effects(), &owner.sid, PcRole::Single);
    let sid = update_audio_owner_track(&mut room, now, &owner, transport, TrackKind::Audio, true);
    let initial = room.get_participant(&"alice".into()).unwrap();
    let track_version = initial.tracks[0].version;
    for raw in [vec![99, 1, -1, 0, 99], vec![99, -1, 0, 1]] {
        let before = room.get_participant(&"alice".into()).unwrap();
        update_audio_owner_request(&mut room, &owner, &sid, &raw, now);
        let effects = room.take_effects();
        let after = room.get_participant(&"alice".into()).unwrap();
        assert_eq!(
            after.tracks[0]
                .audio_features
                .iter()
                .map(|f| f.as_i32())
                .collect::<Vec<_>>(),
            vec![-1, 0, 1, 99]
        );
        assert!(after.tracks[0].stereo && after.tracks[0].disable_dtx);
        assert_eq!(
            after.tracks[0].version, track_version,
            "source receiver does not advance TrackInfo.Version"
        );
        assert!(
            after.version > before.version,
            "UpTrackManager always invokes participant dirty callback, including proto-equal receiver update"
        );
        assert!(signals_to(&effects, &owner.sid).iter().any(|s| matches!(s, SignalEvent::ParticipantUpdate(infos) if infos.iter().any(|p| p.sid == owner.sid))), "source onTrackUpdated broadcasts even for normalized noop");
        assert_eq!(
            room.get_participant_stored(&"alice".into())
                .unwrap()
                .version,
            after.version,
            "callback updates actual stored participant view"
        );
    }
}

#[test]
fn old_public_update_audio_published_video_ignored_but_callback_still_broadcasts() {
    let (mut room, now) = room();
    let owner = room.join(params("alice", 16, true), now).unwrap();
    let transport = transport_of(&room.take_effects(), &owner.sid, PcRole::Single);
    let sid = update_audio_owner_track(&mut room, now, &owner, transport, TrackKind::Video, true);
    let before = room.get_participant(&"alice".into()).unwrap();
    update_audio_owner_request(&mut room, &owner, &sid, &[99, -1], now);
    let effects = room.take_effects();
    let after = room.get_participant(&"alice".into()).unwrap();
    assert_eq!(
        after.tracks, before.tracks,
        "published VIDEO returns before audio mutation"
    );
    assert!(
        after.version > before.version,
        "outer published-track callback remains unconditional"
    );
    assert!(
        signals_to(&effects, &owner.sid)
            .iter()
            .any(|s| matches!(s, SignalEvent::ParticipantUpdate(_)))
    );
}

#[test]
fn old_public_update_audio_missing_has_source_empty_diagnostic() {
    let (mut room, now) = room();
    let owner = room.join(params("alice", 16, true), now).unwrap();
    room.take_effects();
    update_audio_owner_request(
        &mut room,
        &owner,
        &TrackSid::from("TR_missing"),
        &[99, 99],
        now,
    );
    let effects = room.take_effects();
    let signals = signals_to(&effects, &owner.sid);
    assert!(
        matches!(signals.as_slice(), [SignalEvent::RequestResponse { token: 91, request_id: 0, outcome: RequestOutcome::NotFound, message }] if message.is_empty()),
        "actual source diagnostic clones request, leaves message empty"
    );
}
// Independent actual-result producer. No source results embedded, no JSON crate.
// The external strict reader compares this actual owner output to original Go11.
#[test]
#[ignore = "requires coordinator-owned create_new UPDATE_AUDIO_RUST_RESULT and strict source comparison"]
fn old_public_update_audio_actual_go11_owner_output() {
    use std::io::Write as _;
    let path = std::env::var_os("UPDATE_AUDIO_RUST_RESULT").expect("actual result path required");
    let mut file = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(path)
        .unwrap();
    let mut emit = |id: &str,
                    track: Option<&TrackInfo>,
                    callbacks: usize,
                    version_delta: u32,
                    changed: Option<bool>,
                    unchanged_version: Option<bool>,
                    missing: bool,
                    message_empty: bool| {
        let features: Vec<i32> = track
            .map(|t| t.audio_features.iter().map(|f| f.as_i32()).collect())
            .unwrap_or_default();
        let kind = track
            .map(|t| t.kind.as_i32().to_string())
            .unwrap_or_else(|| "null".into());
        let stereo = track
            .map(|t| t.stereo.to_string())
            .unwrap_or_else(|| "null".into());
        let dtx = track
            .map(|t| t.disable_dtx.to_string())
            .unwrap_or_else(|| "null".into());
        let changed = changed
            .map(|v| v.to_string())
            .unwrap_or_else(|| "null".into());
        let stable = unchanged_version
            .map(|v| v.to_string())
            .unwrap_or_else(|| "null".into());
        writeln!(file, "{{\"id\":\"{id}\",\"kind\":{kind},\"features\":{features:?},\"stereo\":{stereo},\"disable_dtx\":{dtx},\"callbacks\":{callbacks},\"participant_version_delta\":{version_delta},\"fields_changed\":{changed},\"track_version_unchanged\":{stable},\"missing_not_found\":{missing},\"missing_message_empty\":{message_empty}}}").unwrap();
    };
    for (kind, label) in [(TrackKind::Audio, "AUDIO"), (TrackKind::Video, "VIDEO")] {
        for empty in [false, true] {
            let (mut room, now) = room();
            let owner = room.join(params("alice", 16, true), now).unwrap();
            let transport = transport_of(&room.take_effects(), &owner.sid, PcRole::Single);
            let sid = update_audio_owner_track(&mut room, now, &owner, transport, kind, false);
            let before = room.get_participant(&"alice".into()).unwrap();
            update_audio_owner_request(&mut room, &owner, &sid, &[99, -1, 4, 99], now);
            let mut effects = room.take_effects();
            if empty {
                assert!(effects.is_empty(), "first pending operation is silent");
                update_audio_owner_request(&mut room, &owner, &sid, &[], now);
                effects = room.take_effects();
            }
            let after = room.get_participant(&"alice".into()).unwrap();
            let callbacks = signals_to(&effects, &owner.sid)
                .iter()
                .filter(|s| matches!(s, SignalEvent::ParticipantUpdate(_)))
                .count();
            assert!(
                effects.is_empty(),
                "pending operation has no published callbacks"
            );
            let delta = after.version.checked_sub(before.version).unwrap();
            update_audio_owner_bind(&mut room, now, &owner, transport, kind);
            room.take_effects();
            let promoted = room.get_participant(&"alice".into()).unwrap();
            let track = promoted.tracks.iter().find(|t| t.sid == sid).unwrap();
            emit(
                &format!("pending-{}{label}", if empty { "empty-" } else { "" }),
                Some(track),
                callbacks,
                delta,
                None,
                None,
                false,
                false,
            );
        }
    }
    for (kind, label) in [(TrackKind::Audio, "AUDIO"), (TrackKind::Video, "VIDEO")] {
        let (mut room, now) = room();
        let owner = room.join(params("alice", 16, true), now).unwrap();
        let transport = transport_of(&room.take_effects(), &owner.sid, PcRole::Single);
        let sid = update_audio_owner_track(&mut room, now, &owner, transport, kind, true);
        for (index, raw) in [vec![99, 1, -1, 0, 99], vec![99, -1, 0, 1], vec![]]
            .iter()
            .enumerate()
        {
            let before = room.get_participant(&"alice".into()).unwrap();
            let old = before.tracks.iter().find(|t| t.sid == sid).unwrap();
            update_audio_owner_request(&mut room, &owner, &sid, raw, now);
            let effects = room.take_effects();
            let after = room.get_participant(&"alice".into()).unwrap();
            let track = after.tracks.iter().find(|t| t.sid == sid).unwrap();
            let changed = old.audio_features != track.audio_features
                || old.stereo != track.stereo
                || old.disable_dtx != track.disable_dtx;
            let callbacks = signals_to(&effects, &owner.sid).iter().filter(|s| matches!(s, SignalEvent::ParticipantUpdate(infos) if infos.iter().any(|p| p.sid == owner.sid))).count();
            assert_eq!(
                room.get_participant_stored(&"alice".into())
                    .unwrap()
                    .version,
                after.version
            );
            emit(
                &format!("published-{label}-{index}"),
                Some(track),
                callbacks,
                after.version.checked_sub(before.version).unwrap(),
                Some(changed),
                Some(old.version == track.version),
                false,
                false,
            );
        }
    }
    let (mut room, now) = room();
    let owner = room.join(params("alice", 16, true), now).unwrap();
    room.take_effects();
    let before = room.get_participant(&"alice".into()).unwrap();
    update_audio_owner_request(
        &mut room,
        &owner,
        &TrackSid::from("TR_missing"),
        &[99, 99],
        now,
    );
    let effects = room.take_effects();
    let after = room.get_participant(&"alice".into()).unwrap();
    let signals = signals_to(&effects, &owner.sid);
    let missing = matches!(
        signals.as_slice(),
        [SignalEvent::RequestResponse {
            token: 91,
            request_id: 0,
            outcome: RequestOutcome::NotFound,
            ..
        }]
    );
    let message_empty = matches!(signals.as_slice(), [SignalEvent::RequestResponse { message, .. }] if message.is_empty());
    let callbacks = signals
        .iter()
        .filter(|s| matches!(s, SignalEvent::ParticipantUpdate(_)))
        .count();
    emit(
        "missing",
        None,
        callbacks,
        after.version.checked_sub(before.version).unwrap(),
        None,
        None,
        missing,
        message_empty,
    );
    file.sync_all().unwrap();
}

#[test]
fn old_public_update_audio_compact_tail_duplicate_unique_empty_controls() {
    for (kind, published) in [
        (TrackKind::Audio, false),
        (TrackKind::Video, false),
        (TrackKind::Audio, true),
    ] {
        let (mut room, now) = room();
        let owner = room.join(params("alice", 16, true), now).unwrap();
        let transport = transport_of(&room.take_effects(), &owner.sid, PcRole::Single);
        let sid = update_audio_owner_track(&mut room, now, &owner, transport, kind, published);
        let vectors = [vec![99, -1, 4, 99], vec![99, -1, 4], vec![]];
        if published {
            update_audio_owner_request(&mut room, &owner, &sid, &[1], now);
            room.take_effects();
            assert!(
                !room.get_participant(&"alice".into()).unwrap().tracks[0].stereo,
                "duplicate control starts with stereo false"
            );
            let version = room.get_participant(&"alice".into()).unwrap().tracks[0].version;
            for (index, raw) in vectors.iter().enumerate() {
                let before = room.get_participant(&"alice".into()).unwrap();
                update_audio_owner_request(&mut room, &owner, &sid, raw, now);
                let effects = room.take_effects();
                let after = room.get_participant(&"alice".into()).unwrap();
                let track = &after.tracks[0];
                assert_eq!(
                    track
                        .audio_features
                        .iter()
                        .map(|f| f.as_i32())
                        .collect::<Vec<_>>(),
                    if index == 2 { vec![] } else { vec![-1, 4, 99] }
                );
                assert_eq!(
                    track.stereo,
                    index == 0,
                    "only duplicate compaction introduces zero into original-length flags input"
                );
                assert!(!track.disable_dtx, "tail zero cannot introduce TF_NO_DTX1");
                assert_eq!(track.version, version);
                assert_eq!(after.version, before.version + 1);
                assert!(
                    signals_to(&effects, &owner.sid)
                        .iter()
                        .any(|s| matches!(s, SignalEvent::ParticipantUpdate(_)))
                );
            }
        } else {
            for wanted in 0..3 {
                let (mut pending_room, pending_now) = self::room();
                let pending_owner = pending_room
                    .join(params("alice", 16, true), pending_now)
                    .unwrap();
                let pending_transport = transport_of(
                    &pending_room.take_effects(),
                    &pending_owner.sid,
                    PcRole::Single,
                );
                let pending_sid = update_audio_owner_track(
                    &mut pending_room,
                    pending_now,
                    &pending_owner,
                    pending_transport,
                    kind,
                    false,
                );
                update_audio_owner_request(
                    &mut pending_room,
                    &pending_owner,
                    &pending_sid,
                    &[1],
                    pending_now,
                );
                assert!(pending_room.take_effects().is_empty());
                for raw in vectors.iter().take(wanted + 1) {
                    update_audio_owner_request(
                        &mut pending_room,
                        &pending_owner,
                        &pending_sid,
                        raw,
                        pending_now,
                    );
                    assert!(pending_room.take_effects().is_empty());
                }
                update_audio_owner_bind(
                    &mut pending_room,
                    pending_now,
                    &pending_owner,
                    pending_transport,
                    kind,
                );
                let effects = pending_room.take_effects();
                let after = pending_room.get_participant(&"alice".into()).unwrap();
                let track = &after.tracks[0];
                assert_eq!(
                    track
                        .audio_features
                        .iter()
                        .map(|f| f.as_i32())
                        .collect::<Vec<_>>(),
                    if wanted == 2 { vec![] } else { vec![-1, 4, 99] }
                );
                assert_eq!(track.stereo, wanted == 0);
                assert!(!track.disable_dtx);
                assert!(effects.iter().any(|effect| matches!(effect, Effect::Media(MediaCommand::UpdatePublishedTrack { track }) if track.stereo == (wanted == 0) && !track.disable_dtx)));
            }
        }
    }
}

#[test]
#[ignore = "requires original additive compact9 actual output and coordinator-owned Rust output"]
fn old_public_update_audio_actual_compact9_owner_output() {
    use std::io::Write as _;
    let path = std::env::var_os("UPDATE_AUDIO_RUST_COMPACT_RESULT").expect("actual compact9 path");
    let mut file = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(path)
        .unwrap();
    let mut emit = |id: &str,
                    track: &TrackInfo,
                    callbacks: usize,
                    delta: u32,
                    changed: Option<bool>,
                    stable: Option<bool>| {
        let features: Vec<i32> = track.audio_features.iter().map(|f| f.as_i32()).collect();
        let kind = track.kind.as_i32();
        let stereo = track.stereo;
        let dtx = track.disable_dtx;
        let changed = changed
            .map(|b| b.to_string())
            .unwrap_or_else(|| "null".into());
        let stable = stable
            .map(|b| b.to_string())
            .unwrap_or_else(|| "null".into());
        writeln!(file, "{{\"id\":\"{id}\",\"kind\":{kind},\"features\":{features:?},\"stereo\":{stereo},\"disable_dtx\":{dtx},\"callbacks\":{callbacks},\"participant_version_delta\":{delta},\"fields_changed\":{changed},\"track_version_unchanged\":{stable},\"missing_not_found\":false,\"missing_message_empty\":false}}").unwrap();
    };
    let vectors = [vec![99, -1, 4, 99], vec![99, -1, 4], vec![]];
    let labels = ["duplicate-no-zero", "unique-no-zero", "empty-clear"];
    for (kind, label) in [(TrackKind::Audio, "AUDIO"), (TrackKind::Video, "VIDEO")] {
        for (index, case_label) in labels.iter().enumerate() {
            let (mut room, now) = room();
            let owner = room.join(params("alice", 16, true), now).unwrap();
            let transport = transport_of(&room.take_effects(), &owner.sid, PcRole::Single);
            let sid = update_audio_owner_track(&mut room, now, &owner, transport, kind, false);
            update_audio_owner_request(&mut room, &owner, &sid, &[1], now);
            assert!(room.take_effects().is_empty());
            let before = room.get_participant(&"alice".into()).unwrap();
            for raw in vectors.iter().take(index + 1) {
                update_audio_owner_request(&mut room, &owner, &sid, raw, now);
                assert!(room.take_effects().is_empty());
            }
            let after = room.get_participant(&"alice".into()).unwrap();
            update_audio_owner_bind(&mut room, now, &owner, transport, kind);
            room.take_effects();
            let promoted = room.get_participant(&"alice".into()).unwrap();
            let track = promoted.tracks.iter().find(|t| t.sid == sid).unwrap();
            emit(
                &format!("compact-pending-{label}-{case_label}"),
                track,
                0,
                after.version.checked_sub(before.version).unwrap(),
                None,
                None,
            );
        }
    }
    let (mut room, now) = room();
    let owner = room.join(params("alice", 16, true), now).unwrap();
    let transport = transport_of(&room.take_effects(), &owner.sid, PcRole::Single);
    let sid = update_audio_owner_track(&mut room, now, &owner, transport, TrackKind::Audio, true);
    update_audio_owner_request(&mut room, &owner, &sid, &[1], now);
    room.take_effects();
    assert!(!room.get_participant(&"alice".into()).unwrap().tracks[0].stereo);
    for (index, raw) in vectors.iter().enumerate() {
        let before = room.get_participant(&"alice".into()).unwrap();
        let old = before.tracks.iter().find(|t| t.sid == sid).unwrap();
        update_audio_owner_request(&mut room, &owner, &sid, raw, now);
        let effects = room.take_effects();
        let after = room.get_participant(&"alice".into()).unwrap();
        let track = after.tracks.iter().find(|t| t.sid == sid).unwrap();
        let changed = old.audio_features != track.audio_features
            || old.stereo != track.stereo
            || old.disable_dtx != track.disable_dtx;
        let callbacks = signals_to(&effects, &owner.sid).iter().filter(|s| matches!(s, SignalEvent::ParticipantUpdate(infos) if infos.iter().any(|p| p.sid == owner.sid))).count();
        emit(
            &format!("compact-published-AUDIO-{}", labels[index]),
            track,
            callbacks,
            after.version.checked_sub(before.version).unwrap(),
            Some(changed),
            Some(track.version == old.version),
        );
    }
    file.sync_all().unwrap();
}

// Existing public Room request/event APIs only; owner state, not native RTP proof.
fn video_owner_track(
    room: &mut Room,
    now: Instant,
    owner: &crate::room::Joined,
    transport: TransportId,
    kind: TrackKind,
    published: bool,
) -> TrackSid {
    video_owner_track_info(room, now, owner, transport, kind, published).sid
}
fn video_owner_track_info(
    room: &mut Room,
    now: Instant,
    owner: &crate::room::Joined,
    transport: TransportId,
    kind: TrackKind,
    published: bool,
) -> TrackInfo {
    let mut request = add_track("video-owner-update", kind, TrackSource::Camera);
    request.width = 640;
    request.height = 360;
    room.handle_request(
        &owner.sid,
        owner.generation,
        SessionRequest::AddTrack(request),
        now,
    );
    let track = signals_to(&room.take_effects(), &owner.sid)
        .into_iter()
        .find_map(|s| {
            if let SignalEvent::TrackPublished { track, .. } = s {
                Some(track)
            } else {
                None
            }
        })
        .expect("actual AddTrack ACK");
    if published {
        video_owner_bind(room, now, owner, transport, kind);
        room.take_effects();
    }
    track
}
fn video_owner_bind(
    room: &mut Room,
    now: Instant,
    owner: &crate::room::Joined,
    transport: TransportId,
    kind: TrackKind,
) {
    room.media_arrives(
        RemoteMedia {
            registration: crate::media_api::PublicationRegistration::Pending,
            participant: owner.sid.clone(),
            transport,
            mid: "video-owner-mid".into(),
            kind,
            msid_track_id: "video-owner-update".into(),
            rids: vec![],
            mime_type: if kind == TrackKind::Audio {
                "audio/opus"
            } else {
                "video/VP8"
            }
            .into(),
        },
        now,
    );
}
fn video_owner_request(
    room: &mut Room,
    owner: &crate::room::Joined,
    sid: &TrackSid,
    width: u32,
    height: u32,
    now: Instant,
) {
    room.handle_request(
        &owner.sid,
        owner.generation,
        SessionRequest::UpdateVideoTrack {
            token: 92,
            sid: sid.clone(),
            width,
            height,
        },
        now,
    );
}
#[test]
fn old_public_update_video_pending_any_kind_dimensions_no_callback() {
    for kind in [TrackKind::Audio, TrackKind::Video] {
        let (mut room, now) = room();
        let owner = room.join(params("alice", 16, true), now).unwrap();
        let transport = transport_of(&room.take_effects(), &owner.sid, PcRole::Single);
        let sid = video_owner_track(&mut room, now, &owner, transport, kind, false);
        for _ in 0..2 {
            video_owner_request(&mut room, &owner, &sid, 1280, 720, now);
            assert!(room.take_effects().is_empty());
        }
        video_owner_bind(&mut room, now, &owner, transport, kind);
        room.take_effects();
        let p = room.get_participant(&"alice".into()).unwrap();
        let t = p.tracks.iter().find(|t| t.sid == sid).unwrap();
        assert_eq!((t.width, t.height), (1280, 720));
    }
}
#[test]
fn old_public_update_video_published_video_change_noop_clear_version_stable() {
    let (mut room, now) = room();
    let owner = room.join(params("alice", 16, true), now).unwrap();
    let transport = transport_of(&room.take_effects(), &owner.sid, PcRole::Single);
    let sid = video_owner_track(&mut room, now, &owner, transport, TrackKind::Video, true);
    let initial = room.get_participant(&"alice".into()).unwrap();
    let version = initial.tracks[0].version;
    for (width, height) in [(1280, 720), (1280, 720), (0, 0)] {
        let before = room.get_participant(&"alice".into()).unwrap();
        video_owner_request(&mut room, &owner, &sid, width, height, now);
        let effects = room.take_effects();
        let after = room.get_participant(&"alice".into()).unwrap();
        assert_eq!(
            (after.tracks[0].width, after.tracks[0].height),
            (width, height)
        );
        assert_eq!(
            after.tracks[0].version, version,
            "source receiver retains TrackInfo.Version on changed and no-op updates"
        );
        assert!(
            after.version > before.version,
            "outer callback is unconditional"
        );
        assert!(
            signals_to(&effects, &owner.sid)
                .iter()
                .any(|s| matches!(s, SignalEvent::ParticipantUpdate(_)))
        );
        assert_eq!(
            room.get_participant_stored(&"alice".into())
                .unwrap()
                .version,
            after.version
        );
    }
}
#[test]
fn old_public_update_video_published_audio_ignored_but_callback() {
    let (mut room, now) = room();
    let owner = room.join(params("alice", 16, true), now).unwrap();
    let transport = transport_of(&room.take_effects(), &owner.sid, PcRole::Single);
    let sid = video_owner_track(&mut room, now, &owner, transport, TrackKind::Audio, true);
    for _ in 0..2 {
        let before = room.get_participant(&"alice".into()).unwrap();
        video_owner_request(&mut room, &owner, &sid, 1280, 720, now);
        let effects = room.take_effects();
        let after = room.get_participant(&"alice".into()).unwrap();
        assert_eq!(
            after.tracks, before.tracks,
            "source published non-VIDEO receiver returns before dimensions/version mutation"
        );
        assert!(after.version > before.version);
        assert!(
            signals_to(&effects, &owner.sid)
                .iter()
                .any(|s| matches!(s, SignalEvent::ParticipantUpdate(_)))
        );
    }
}
#[test]
fn old_public_update_video_missing_source_empty_diagnostic() {
    let (mut room, now) = room();
    let owner = room.join(params("alice", 16, true), now).unwrap();
    room.take_effects();
    video_owner_request(
        &mut room,
        &owner,
        &TrackSid::from("TR_missing"),
        1280,
        720,
        now,
    );
    let effects = room.take_effects();
    let signals = signals_to(&effects, &owner.sid);
    assert!(
        matches!(signals.as_slice(),[SignalEvent::RequestResponse {token:92,request_id:0,outcome:RequestOutcome::NotFound,message}] if message.is_empty())
    );
}
#[test]
#[ignore = "requires original Go10 actual rows and coordinator-owned UPDATE_VIDEO_RUST_RESULT"]
fn old_public_update_video_actual_owner10_output() {
    use std::io::Write as _;
    let mut file = std::fs::OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(std::env::var_os("UPDATE_VIDEO_RUST_RESULT").expect("actual result path"))
        .unwrap();
    let mut emit = |id: &str,
                    track: Option<&TrackInfo>,
                    callbacks: usize,
                    delta: u32,
                    changed: Option<bool>,
                    stable: Option<bool>,
                    missing: bool,
                    empty: bool| {
        let raw = |v: Option<bool>| v.map(|v| v.to_string()).unwrap_or_else(|| "null".into());
        let kind = track
            .map(|t| t.kind.as_i32().to_string())
            .unwrap_or_else(|| "null".into());
        let width = track
            .map(|t| t.width.to_string())
            .unwrap_or_else(|| "null".into());
        let height = track
            .map(|t| t.height.to_string())
            .unwrap_or_else(|| "null".into());
        writeln!(file,"{{\"id\":\"{id}\",\"kind\":{kind},\"width\":{width},\"height\":{height},\"callbacks\":{callbacks},\"participant_version_delta\":{delta},\"fields_changed\":{},\"track_version_unchanged\":{},\"missing_not_found\":{missing},\"missing_message_empty\":{empty}}}",raw(changed),raw(stable)).unwrap();
    };
    for (kind, label) in [(TrackKind::Audio, "AUDIO"), (TrackKind::Video, "VIDEO")] {
        for index in 0..2 {
            let (mut room, now) = room();
            let owner = room.join(params("alice", 16, true), now).unwrap();
            let transport = transport_of(&room.take_effects(), &owner.sid, PcRole::Single);
            let ack = video_owner_track_info(&mut room, now, &owner, transport, kind, false);
            let sid = ack.sid.clone();
            for _ in 0..index {
                video_owner_request(&mut room, &owner, &sid, 1280, 720, now);
                room.take_effects();
            }
            // Read the real pending object in this child test module. This is
            // observation only: no injected state/event, getter or timer.
            let pending_before = room
                .participants
                .get(&owner.sid)
                .unwrap()
                .pending
                .iter()
                .find(|p| p.info.sid == sid)
                .unwrap()
                .info
                .version;
            let before = room.get_participant(&"alice".into()).unwrap();
            video_owner_request(&mut room, &owner, &sid, 1280, 720, now);
            let effects = room.take_effects();
            let after = room.get_participant(&"alice".into()).unwrap();
            assert!(effects.is_empty());
            let pending_after = room
                .participants
                .get(&owner.sid)
                .unwrap()
                .pending
                .iter()
                .find(|p| p.info.sid == sid)
                .unwrap()
                .info
                .version;
            video_owner_bind(&mut room, now, &owner, transport, kind);
            room.take_effects();
            let promoted = room.get_participant(&"alice".into()).unwrap();
            let track = promoted.tracks.iter().find(|t| t.sid == sid).unwrap();
            emit(
                &format!("pending-{label}-{index}"),
                Some(track),
                0,
                after.version.checked_sub(before.version).unwrap(),
                None,
                Some(pending_before == pending_after),
                false,
                false,
            );
        }
    }
    for (kind, label) in [(TrackKind::Audio, "AUDIO"), (TrackKind::Video, "VIDEO")] {
        let (mut room, now) = room();
        let owner = room.join(params("alice", 16, true), now).unwrap();
        let transport = transport_of(&room.take_effects(), &owner.sid, PcRole::Single);
        let sid = video_owner_track(&mut room, now, &owner, transport, kind, true);
        let mut dimensions = vec![(1280, 720), (1280, 720)];
        if kind == TrackKind::Video {
            dimensions.push((0, 0));
        }
        for (index, (width, height)) in dimensions.into_iter().enumerate() {
            let before = room.get_participant(&"alice".into()).unwrap();
            let old = before.tracks.iter().find(|t| t.sid == sid).unwrap();
            video_owner_request(&mut room, &owner, &sid, width, height, now);
            let effects = room.take_effects();
            let after = room.get_participant(&"alice".into()).unwrap();
            let track = after.tracks.iter().find(|t| t.sid == sid).unwrap();
            let callbacks = signals_to(&effects, &owner.sid)
                .iter()
                .filter(|s| matches!(s, SignalEvent::ParticipantUpdate(_)))
                .count();
            emit(
                &format!("published-{label}-{index}"),
                Some(track),
                callbacks,
                after.version.checked_sub(before.version).unwrap(),
                Some((old.width, old.height) != (track.width, track.height)),
                Some(old.version == track.version),
                false,
                false,
            );
        }
    }
    let (mut room, now) = room();
    let owner = room.join(params("alice", 16, true), now).unwrap();
    room.take_effects();
    let before = room.get_participant(&"alice".into()).unwrap();
    video_owner_request(
        &mut room,
        &owner,
        &TrackSid::from("TR_missing"),
        1280,
        720,
        now,
    );
    let effects = room.take_effects();
    let after = room.get_participant(&"alice".into()).unwrap();
    let signals = signals_to(&effects, &owner.sid);
    let missing = matches!(
        signals.as_slice(),
        [SignalEvent::RequestResponse {
            token: 92,
            request_id: 0,
            outcome: RequestOutcome::NotFound,
            ..
        }]
    );
    let empty = matches!(signals.as_slice(),[SignalEvent::RequestResponse {message,..}] if message.is_empty());
    emit(
        "missing",
        None,
        0,
        after.version.checked_sub(before.version).unwrap(),
        None,
        None,
        missing,
        empty,
    );
    file.sync_all().unwrap();
}

// Full source schema, finite public-owner observable subscope; not native.
mod subscription_source_observable;

// Model-only compatibility collector for the original queue assertions. Each
// original body/kind/recipient/count remains checked; this is NOT native proof.
impl Room {
    fn source_replay_model(&mut self, now: Instant) -> Vec<Effect> {
        let effects = self.take_effects();
        let mut collected = Vec::new();
        for effect in effects {
            match effect {
                Effect::Media(MediaCommand::ReplayJoiningReliable {
                    participant,
                    transport,
                    writes,
                }) => {
                    assert!(!self.participants[&participant].can_write_reliable);
                    assert!(self.participants[&participant].joining_replay_in_progress);
                    for write in writes {
                        collected.push(Effect::Media(MediaCommand::SendData {
                            to: write.to,
                            kind: write.kind,
                            payload: write.payload,
                        }));
                    }
                    self.handle_media_event(
                        MediaEvent::JoiningReliableReplayed {
                            participant,
                            transport,
                        },
                        now,
                    );
                }
                other => collected.push(other),
            }
        }
        collected
    }
}

#[test]
fn joining_reliable_queue_inclusive_cap_primary_fence_and_ready_fifo() {
    let (mut room, now) = room();
    let joined = room.join(params("queue", 17, true), now).unwrap();
    let effects = room.source_replay_model(now);
    let primary = transport_of(&effects, &joined.sid, PcRole::Single);
    room.api_send_data(DataKind::Reliable, vec![1; 50_000], &[], &[]);
    room.api_send_data(DataKind::Reliable, vec![2; 50_000], &[], &[]);
    room.api_send_data(DataKind::Reliable, vec![3], &[], &[]);
    assert!(room.source_replay_model(now).is_empty());
    let p = &room.participants[&joined.sid];
    assert_eq!(p.joining_reliable_bytes, 100_000);
    assert_eq!(p.joining_reliable.len(), 2);
    assert!(!p.can_write_reliable);
    // ACTIVE alone does not replay. This is model-only state coverage, not a native readiness proof.
    room.handle_media_event(
        MediaEvent::TransportConnected {
            participant: joined.sid.clone(),
            transport: primary,
        },
        now,
    );
    assert!(
        !room
            .source_replay_model(now)
            .iter()
            .any(|e| matches!(e, Effect::Media(MediaCommand::SendData { .. })))
    );
    room.handle_media_event(
        MediaEvent::PrimaryTransportEstablished {
            participant: joined.sid.clone(),
            transport: TransportId::next(),
        },
        now,
    );
    assert!(room.source_replay_model(now).is_empty());
    room.handle_media_event(
        MediaEvent::PrimaryTransportEstablished {
            participant: joined.sid.clone(),
            transport: primary,
        },
        now,
    );
    let sent: Vec<_> = room
        .source_replay_model(now)
        .into_iter()
        .filter_map(|e| match e {
            Effect::Media(MediaCommand::SendData { to, kind, payload }) => {
                Some((to, kind, payload))
            }
            _ => None,
        })
        .collect();
    assert_eq!(sent.len(), 2);
    for (i, (to, kind, body)) in sent.iter().enumerate() {
        assert_eq!(to, &joined.sid);
        assert_eq!(*kind, DataKind::Reliable);
        assert_eq!(body, &vec![(i + 1) as u8; 50_000]);
    }
    assert_eq!(room.participants[&joined.sid].joining_reliable_bytes, 0);
    assert!(room.participants[&joined.sid].can_write_reliable);
    room.handle_media_event(
        MediaEvent::PrimaryTransportEstablished {
            participant: joined.sid.clone(),
            transport: primary,
        },
        now,
    );
    assert!(room.source_replay_model(now).is_empty());
    room.api_send_data(DataKind::Reliable, vec![4], &[], &[]);
    assert!(
        matches!(room.source_replay_model(now).as_slice(),[Effect::Media(MediaCommand::SendData { payload,.. })] if payload == &[4])
    );
}

#[test]
fn joining_reliable_queue_destroyed_with_participant_not_replayed_into_replacement() {
    let (mut room, now) = room();
    let old = room.join(params("replace-queue", 17, true), now).unwrap();
    let primary = transport_of(&room.source_replay_model(now), &old.sid, PcRole::Single);
    room.api_send_data(DataKind::Reliable, vec![7], &[], &[]);
    room.source_replay_model(now);
    room.api_remove_participant(&Identity::from("replace-queue"), now)
        .unwrap();
    room.source_replay_model(now);
    let new = room.join(params("replace-queue", 17, true), now).unwrap();
    room.source_replay_model(now);
    assert_ne!(old.sid, new.sid);
    room.handle_media_event(
        MediaEvent::PrimaryTransportEstablished {
            participant: old.sid,
            transport: primary,
        },
        now,
    );
    assert!(room.source_replay_model(now).is_empty());
    assert_eq!(room.participants[&new.sid].joining_reliable_bytes, 0);
    assert!(room.participants[&new.sid].joining_reliable.is_empty());
    assert!(!room.participants[&new.sid].can_write_reliable);
}

#[test]
#[ignore = "requires actual fixed Go JOINING_RELIABLE_GO_RESULTS TSV"]
fn actual_go_joining_reliable_queue_five_component_observations() {
    let text = std::fs::read_to_string(
        std::env::var("JOINING_RELIABLE_GO_RESULTS").expect("actual source TSV"),
    )
    .unwrap();
    let mut seen = std::collections::BTreeSet::new();
    for line in text.lines() {
        let f: Vec<_> = line.split('\t').collect();
        assert_eq!(f.len(), 11);
        let id = f[0];
        assert!(seen.insert(id.to_owned()));
        assert!(
            [
                "empty",
                "inclusive-cap",
                "over-cap",
                "cloned-input",
                "post-established"
            ]
            .contains(&id)
        );
        let (mut room, now) = room();
        let joined = room.join(params("queue-source", 17, true), now).unwrap();
        let primary = transport_of(&room.source_replay_model(now), &joined.sid, PcRole::Single);
        let mut errors = Vec::new();
        let mut written = Vec::new();
        if id == "post-established" {
            room.handle_media_event(
                MediaEvent::TransportConnected {
                    participant: joined.sid.clone(),
                    transport: primary,
                },
                now,
            );
            room.source_replay_model(now);
            room.handle_media_event(
                MediaEvent::PrimaryTransportEstablished {
                    participant: joined.sid.clone(),
                    transport: primary,
                },
                now,
            );
            room.source_replay_model(now);
        }
        let mut send = |room: &mut Room, body: Vec<u8>| {
            let before = room.participants[&joined.sid].joining_reliable.len();
            let queueing = !room.participants[&joined.sid].can_write_reliable;
            room.api_send_data(DataKind::Reliable, body, &[], &[]);
            errors
                .push(queueing && room.participants[&joined.sid].joining_reliable.len() == before);
            for effect in room.source_replay_model(now) {
                if let Effect::Media(MediaCommand::SendData { kind, payload, .. }) = effect {
                    assert_eq!(kind, DataKind::Reliable);
                    written.extend(payload);
                }
            }
        };
        match id {
            "empty" => {}
            "inclusive-cap" => {
                send(&mut room, vec![1; 50_000]);
                send(&mut room, vec![2; 50_000]);
            }
            "over-cap" => {
                send(&mut room, vec![1; 100_000]);
                send(&mut room, vec![2]);
            }
            "cloned-input" => {
                let mut body = vec![1, 2, 3];
                send(&mut room, body.clone());
                body[0] = 99;
                assert_eq!(body[0], 99);
            }
            "post-established" => send(&mut room, vec![9]),
            _ => unreachable!(),
        }
        let p = &room.participants[&joined.sid];
        assert_eq!(
            errors
                .iter()
                .map(bool::to_string)
                .collect::<Vec<_>>()
                .join(","),
            f[1]
        );
        assert_eq!(p.joining_reliable.len(), f[2].parse::<usize>().unwrap());
        assert_eq!(p.joining_reliable_bytes, f[3].parse::<usize>().unwrap());
        assert_eq!(written.len(), f[4].parse::<usize>().unwrap());
        room.handle_media_event(
            MediaEvent::PrimaryTransportEstablished {
                participant: joined.sid.clone(),
                transport: primary,
            },
            now,
        );
        for effect in room.source_replay_model(now) {
            if let Effect::Media(MediaCommand::SendData { kind, payload, .. }) = effect {
                assert_eq!(kind, DataKind::Reliable);
                written.extend(payload);
            }
        }
        assert_eq!(written.len(), f[5].parse::<usize>().unwrap());
        let mut runs = Vec::new();
        let mut i = 0;
        while i < written.len() {
            let mut j = i + 1;
            while j < written.len() && written[j] == written[i] {
                j += 1;
            }
            runs.push(format!("{}:{}", written[i], j - i));
            i = j;
        }
        assert_eq!(runs.join(","), f[6]);
        assert_eq!(f[7], "0");
        let p = &room.participants[&joined.sid];
        assert_eq!(p.joining_reliable_bytes, f[8].parse::<usize>().unwrap());
        assert_eq!(p.joining_reliable.len(), f[9].parse::<usize>().unwrap());
        assert_eq!(p.can_write_reliable.to_string(), f[10]);
    }
    assert_eq!(seen.len(), 5);
}

#[test]
fn api_send_data_duplicate_nonce_completes_without_writes() {
    // room.go2128–2131: a repeated non-empty nonce is dropped room-wide; the
    // RoomService call still completes (empty fanout batch).
    let (mut room, now) = room();
    room.join(params("nonce-target", 17, true), now).unwrap();
    room.take_effects();
    let nonce = [9u8; 16];
    let fanouts = |effects: Vec<Effect>| -> Vec<(u64, usize)> {
        effects
            .into_iter()
            .filter_map(|e| match e {
                Effect::Media(MediaCommand::SendDataFanout { id, writes }) => {
                    Some((id, writes.len()))
                }
                _ => None,
            })
            .collect()
    };
    room.api_send_data_tracked(1, DataKind::Reliable, vec![1], &[], &[], &nonce, now);
    assert_eq!(fanouts(room.take_effects()), vec![(1, 1)]);
    room.api_send_data_tracked(2, DataKind::Reliable, vec![2], &[], &[], &nonce, now);
    assert_eq!(
        fanouts(room.take_effects()),
        vec![(2, 0)],
        "duplicate nonce writes nothing but completes"
    );
    // Empty nonce is never deduplicated; the window expires after 2 s.
    room.api_send_data_tracked(3, DataKind::Reliable, vec![3], &[], &[], &[], now);
    assert_eq!(fanouts(room.take_effects()), vec![(3, 1)]);
    let later = now + Duration::from_secs(3);
    room.api_send_data_tracked(4, DataKind::Reliable, vec![4], &[], &[], &nonce, later);
    assert_eq!(fanouts(room.take_effects()), vec![(4, 1)]);
}

#[test]
fn debug_snapshot_tracks_actual_pending_publications_and_generation_removal() {
    let (mut room, now) = room();
    let alice = room.join(params("debug-alice", 16, false), now).unwrap();
    room.take_effects();
    room.handle_request(
        &alice.sid,
        alice.generation,
        SessionRequest::AddTrack(add_track(
            "debug-cid",
            TrackKind::Video,
            TrackSource::Camera,
        )),
        now,
    );
    let snapshot = room.debug_snapshot();
    assert_eq!(snapshot.room.sid, room.info().sid);
    let participant = snapshot
        .participants
        .iter()
        .find(|p| p.info.sid == alice.sid)
        .unwrap();
    let pending = participant
        .pending_tracks
        .get("debug-cid")
        .expect("actual pending publication");
    assert_eq!(pending.track_infos.len(), 1);
    assert_eq!(pending.track_infos[0].kind, TrackKind::Video);
    assert!(pending.track_infos[0].sid.0.starts_with("TR_"));
    assert_eq!(pending.migrated, None);
    room.api_remove_participant(&"debug-alice".into(), now)
        .unwrap();
    assert!(
        room.debug_snapshot()
            .participants
            .iter()
            .all(|p| p.info.sid != alice.sid)
    );
    // The retained immutable diagnostic belongs to the original actor turn.
    assert_eq!(snapshot.participants.len(), 1);
}

#[test]
fn data_traffic_counts_permitted_messages_before_decoder_drop_and_survives_close() {
    use std::sync::atomic::{AtomicUsize, Ordering};
    struct DecoderDrop(Arc<AtomicUsize>);
    impl DataHandler for DecoderDrop {
        fn inbound(&self, _: &DataSender<'_>, _: DataKind, _: &str, _: &[u8]) -> DataVerdict {
            self.0.fetch_add(1, Ordering::SeqCst);
            DataVerdict::Drop
        }
    }
    let (mut room, now) = room();
    let decoded = Arc::new(AtomicUsize::new(0));
    room.data = Arc::new(DecoderDrop(decoded.clone()));
    let joined = room.join(params("data-accounting", 17, true), now).unwrap();
    let malformed = [0xff, 0xff, 0xff];
    room.data_received(
        &ParticipantSid::from("PA_missing"),
        DataKind::Reliable,
        "_reliable",
        &malformed,
        now,
    );
    room.participants
        .get_mut(&joined.sid)
        .unwrap()
        .info
        .permission
        .can_publish_data = false;
    room.data_received(
        &joined.sid,
        DataKind::Reliable,
        "_reliable",
        &malformed,
        now,
    );
    assert_eq!(room.data_traffic_in(), (0, 0));
    assert_eq!(
        decoded.load(Ordering::SeqCst),
        0,
        "denied senders must not reach decoder"
    );
    room.participants
        .get_mut(&joined.sid)
        .unwrap()
        .info
        .permission
        .can_publish_data = true;
    room.data_received(
        &joined.sid,
        DataKind::Reliable,
        "_reliable",
        &malformed,
        now,
    );
    room.data_received(&joined.sid, DataKind::Lossy, "_lossy", &[], now);
    assert_eq!(
        room.data_traffic_in(),
        (3, 2),
        "empty/malformed accepted messages are actual traffic"
    );
    assert_eq!(decoded.load(Ordering::SeqCst), 2);
    room.close(RoomEndReason::ServerShutdown, now);
    room.data_received(
        &joined.sid,
        DataKind::Reliable,
        "_reliable",
        &malformed,
        now,
    );
    assert_eq!(
        room.data_traffic_in(),
        (3, 2),
        "terminal cleanup must retain totals but reject late input"
    );
}

#[test]
fn filtered_empty_participant_update_does_not_create_an_empty_signal_frame() {
    // Fixed rtc/participant_signal.go filters first; signalling/signalling.go
    // SignalParticipantUpdate returns nil for the resulting empty slice.
    let (mut room, now) = room();
    let observer = room.join(params("filter-observer", 17, true), now).unwrap();
    let source = room.join(params("filter-source", 17, true), now).unwrap();
    let old = room
        .get_participant(&Identity("filter-source".into()))
        .unwrap();
    room.api_update_participant(
        &Identity("filter-source".into()),
        "",
        "newer version",
        &BTreeMap::new(),
        None,
        now,
    )
    .unwrap();
    room.take_effects();
    room.send_participant_updates(&observer.sid, vec![old]);
    assert!(
        signals_to(&room.take_effects(), &observer.sid).is_empty(),
        "an outdated chunk becoming empty is suppressed by the actual signaling consumer"
    );
    let mut hidden = room
        .get_participant(&Identity("filter-source".into()))
        .unwrap();
    hidden.permission.hidden = true;
    room.send_participant_updates(&observer.sid, vec![hidden]);
    assert!(signals_to(&room.take_effects(), &observer.sid).is_empty());
    assert_ne!(source.sid, observer.sid);
}

#[test]
fn actual_owner_binds_backup_codec_cid_without_consuming_another_pending_publication() {
    let now = Instant::now();
    let mut policy = test_policy();
    policy.enabled_codecs.push(CodecSpec {
        mime: "video/vp9".into(),
        fmtp_line: String::new(),
    });
    let mut room = Room::new(
        RoomKey::new(ProjectId::local(), "codec-owner"),
        Arc::new(policy),
        Arc::new(EchoData),
        &Default::default(),
        now,
    );
    room.take_effects();
    let owner = room
        .join(params("multi-codec-owner", 17, true), now)
        .unwrap();
    let transport = transport_of(&room.take_effects(), &owner.sid, PcRole::Single);
    let mut request = add_track("primary-cid", TrackKind::Video, TrackSource::Camera);
    request.simulcast_codecs = [("vp9", "primary-cid"), ("vp8", "")]
        .into_iter()
        .map(|(codec, cid)| SimulcastCodecRequest {
            codec: codec.into(),
            cid: cid.into(),
            layers: vec![],
            layer_mode: Default::default(),
        })
        .collect();
    room.handle_request(
        &owner.sid,
        owner.generation,
        SessionRequest::AddTrack(request),
        now,
    );
    room.take_effects();
    let event = |mid: &str, cid: &str, mime: &str, transport| {
        (RemoteMedia {
            registration: crate::media_api::PublicationRegistration::SdpReceiver,
            participant: owner.sid.clone(),
            transport,
            mid: mid.into(),
            kind: TrackKind::Video,
            msid_track_id: cid.into(),
            rids: vec![],
            mime_type: mime.into(),
        })
    };
    room.media_arrives(event("0", "primary-cid", "video/vp9", transport), now);
    room.take_effects();
    let track = room.participants[&owner.sid].info.tracks[0].sid.clone();
    assert_eq!(
        room.participants[&owner.sid].info.tracks[0].codecs.len(),
        2,
        "both declared codecs must be truly enabled, not collapsed by fallback registration"
    );
    let mut backup = add_track("backup-cid", TrackKind::Video, TrackSource::Camera);
    backup.sid = Some(track.clone());
    backup.simulcast_codecs = vec![SimulcastCodecRequest {
        codec: "VP8".into(),
        cid: "backup-cid".into(),
        layers: vec![],
        layer_mode: Default::default(),
    }];
    room.handle_request(
        &owner.sid,
        owner.generation,
        SessionRequest::AddTrack(backup),
        now,
    );
    assert!(room.take_effects().iter().any(|effect| matches!(
        effect, Effect::Media(MediaCommand::ExpectTrack { track, .. })
            if track.codecs[1].cid == "backup-cid"
    )));
    room.handle_request(
        &owner.sid,
        owner.generation,
        SessionRequest::AddTrack(add_track(
            "unrelated",
            TrackKind::Video,
            TrackSource::Camera,
        )),
        now,
    );
    room.take_effects();
    room.handle_media_event(
        MediaEvent::RemoteMediaAdded(event("1", "backup-cid", "video/vp8", TransportId::next())),
        now,
    );
    assert!(
        room.take_effects()
            .iter()
            .all(|effect| !matches!(effect, Effect::Media(MediaCommand::BindTrack { .. })))
    );
    room.media_arrives(event("1", "backup-cid", "video/vp8", transport), now);
    let effects = room.take_effects();
    assert!(effects.iter().any(|effect|matches!(effect,Effect::Media(MediaCommand::BindTrack{transport:tid,mid,track_sid}) if *tid==transport && mid=="1" && track_sid==&track)));
    assert_eq!(room.participants[&owner.sid].pending.len(), 1);
    assert_eq!(room.participants[&owner.sid].info.tracks.len(), 1);
    let info = &room.participants[&owner.sid].info.tracks[0];
    assert_eq!(info.codecs[0].mid, "0");
    assert_eq!(info.codecs[1].mid, "1");
    room.handle_media_event(
        MediaEvent::TrackMediaStarted {
            track_sid: track.clone(),
            mime_type: "video/vp8".into(),
            layers: vec![],
        },
        now,
    );
    let info = &room.participants[&owner.sid].info.tracks[0];
    assert_eq!(info.mime_type, "video/vp9");
    assert_eq!(info.codecs[0].mime_type, "video/vp9");
    assert_eq!(info.codecs[1].mime_type, "video/vp8");
}

mod lifecycle_counters;

mod media_rtt;

mod transport_fallback;

mod simulate_scenarios;

mod publication_visibility;

mod force_tls_failure_branches;

mod publisher_identity;

#[test]
fn resolved_room_stream_sync_and_firefox_capability_reach_actual_pc_creation() {
    for sync in [false, true] {
        let (seed, now) = room();
        let mut policy = (*seed.policy).clone();
        policy.sync_streams = sync;
        policy.playout_delay = Some((100, 200));
        let mut actual = Room::new(
            seed.key.clone(),
            Arc::new(policy),
            Arc::new(EchoData),
            &Default::default(),
            now,
        );
        for (identity, firefox) in [("chrome", false), ("firefox", true), ("safari", false)] {
            let mut input = params(identity, 17, false);
            input.capabilities.disable_playout_delay_for_sync = firefox;
            let joined = actual.join(input, now).unwrap();
            let transports = actual
                .take_effects()
                .into_iter()
                .filter_map(|effect| match effect {
                    Effect::Media(MediaCommand::CreateTransport(spec))
                        if spec.participant == joined.sid =>
                    {
                        Some(spec)
                    }
                    _ => None,
                })
                .collect::<Vec<_>>();
            assert_eq!(transports.len(), 2, "actual dual-PC transport construction");
            for spec in transports {
                assert_eq!(
                    spec.effective_playout_delay(actual.playout_delay()),
                    if sync && firefox {
                        None
                    } else {
                        Some((100, 200))
                    },
                    "{identity}, sync={sync}"
                );
            }
        }
    }
}

impl Room {
    /// Test helper: an SDP media section followed by its first RTP. Fixed Go makes a publication
    /// visible from pion OnTrack — after DTLS for a declared-SSRC section, after the first RTP for
    /// RID or undeclared sections — so "media arrived" includes the first RTP here.
    fn media_arrives(&mut self, media: RemoteMedia, now: Instant) {
        let (participant, mid, mime) = (
            media.participant.clone(),
            media.mid.clone(),
            media.mime_type.clone(),
        );
        self.handle_media_event(MediaEvent::RemoteMediaAdded(media), now);
        let deferred = self.participants.get(&participant).and_then(|p| {
            p.deferred_publications
                .iter()
                .find(|d| d.info.mid == mid)
                .map(|d| d.info.sid.clone())
        });
        if let Some(track_sid) = deferred {
            self.handle_media_event(
                MediaEvent::TrackMediaStarted {
                    track_sid,
                    mime_type: mime,
                    layers: vec![],
                },
                now,
            );
        }
    }
}
