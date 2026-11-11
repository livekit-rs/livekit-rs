//! Actual public Room model subscription retry consumers, using a paused Tokio clock.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use rtc_core::config::*;
use rtc_core::data::{DataHandler, DataSender, DataVerdict};
use rtc_core::ids::*;
use rtc_core::media_api::{DataKind, MediaCommand, MediaEvent, PcRole, RemoteMedia};
use rtc_core::model::*;
use rtc_core::permission::{Grants, VideoGrant};
use rtc_core::room::{Effect, Joined, Room};
use rtc_core::session::*;

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

fn room(enabled: bool) -> (Room, Instant) {
    let now = tokio::time::Instant::now().into_std();
    let key = RoomKey::new(ProjectId::local(), "r1");
    let mut policy = test_policy();
    policy.reconnect_on_subscription_error = enabled;
    let mut room = Room::new(
        key,
        Arc::new(policy),
        Arc::new(EchoData),
        &Default::default(),
        now,
    );
    let started = room.take_effects();
    assert!(matches!(
        started.as_slice(),
        [Effect::Event(
            rtc_core::events::EngineEvent::RoomStarted { .. }
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

fn now() -> Instant {
    tokio::time::Instant::now().into_std()
}
fn active_with_transport(
    room: &mut Room,
    identity: &str,
    can_subscribe: bool,
) -> (Joined, TransportId) {
    let mut input = params(identity, 17, true);
    input.auto_subscribe = false;
    input.grants.video.can_subscribe = Some(can_subscribe);
    let joined = room.join(input, now()).unwrap();
    let transport = transport_of(&room.take_effects(), &joined.sid, PcRole::Single);
    room.handle_media_event(
        MediaEvent::TransportConnected {
            participant: joined.sid.clone(),
            transport,
        },
        now(),
    );
    room.take_effects();
    (joined, transport)
}
fn active(room: &mut Room, identity: &str, can_subscribe: bool) -> Joined {
    active_with_transport(room, identity, can_subscribe).0
}
fn desire(room: &mut Room, sub: &Joined, track: &TrackSid, value: bool) {
    room.api_update_subscriptions(
        &Identity::from("subscriber"),
        std::slice::from_ref(track),
        value,
        now(),
    )
    .unwrap();
    assert!(room.participant_generation(&sub.sid).is_some());
}
fn missing_count(effects: &[Effect], sub: &Joined, track: &TrackSid) -> usize {
    signals_to(effects, &sub.sid).iter().filter(|e| matches!(e,
        SignalEvent::SubscriptionResponse {track_sid,error:SubscriptionError::TrackNotFound} if track_sid==track)).count()
}
fn no_reconnect(room: &mut Room, sub: &Joined, effects: &[Effect]) {
    assert!(
        !signals_to(effects, &sub.sid)
            .iter()
            .any(|e| matches!(e, SignalEvent::Leave { .. }))
    );
    assert!(room.participant_exists(&Identity::from("subscriber")));
}
async fn advance(room: &mut Room, duration: Duration) -> Vec<Effect> {
    tokio::time::advance(duration).await;
    room.tick(now());
    room.take_effects()
}
#[tokio::test(start_paused = true)]
async fn missing_public_api_intent_expires_only_on_reconcile_strictly_after_sixty_nonfatal() {
    for reconnect in [false, true] {
        let (mut room, _) = room(reconnect);
        let sub = active(&mut room, "subscriber", true);
        let track = TrackSid::from("TR_never_published");
        desire(&mut room, &sub, &track, true);
        room.take_effects();
        for _ in 0..20 {
            let effects = advance(&mut room, Duration::from_secs(3)).await;
            assert_eq!(
                missing_count(&effects, &sub, &track),
                0,
                "strict60s boundary"
            );
            no_reconnect(&mut room, &sub, &effects);
        }
        let effects = advance(&mut room, Duration::from_nanos(1)).await;
        assert_eq!(
            missing_count(&effects, &sub, &track),
            0,
            "a tick before the3s reconcile must not expire"
        );
        let effects = advance(&mut room, Duration::from_nanos(2_999_999_999)).await;
        assert_eq!(
            missing_count(&effects, &sub, &track),
            1,
            "real Room.tick must emit TRACK_NOTFOUND"
        );
        no_reconnect(&mut room, &sub, &effects);
        let effects = advance(&mut room, Duration::from_secs(63)).await;
        assert_eq!(
            missing_count(&effects, &sub, &track),
            0,
            "desired=false prevents repeated failure"
        );
        no_reconnect(&mut room, &sub, &effects);
    }
}
#[tokio::test(start_paused = true)]
async fn explicit_resubscribe_resets_missing_clock_and_cancel_removes_intent() {
    for cancel in [false, true] {
        let (mut room, _) = room(true);
        let sub = active(&mut room, "subscriber", true);
        let track = TrackSid::from("TR_reset");
        desire(&mut room, &sub, &track, true);
        room.take_effects();
        let effects = advance(&mut room, Duration::from_secs(60)).await;
        assert_eq!(missing_count(&effects, &sub, &track), 0);
        desire(&mut room, &sub, &track, !cancel);
        room.take_effects();
        let effects = advance(&mut room, Duration::from_secs(60)).await;
        assert_eq!(missing_count(&effects, &sub, &track), 0);
        let effects = advance(&mut room, Duration::from_secs(3)).await;
        assert_eq!(missing_count(&effects, &sub, &track), usize::from(!cancel));
        no_reconnect(&mut room, &sub, &effects);
    }
}
#[tokio::test(start_paused = true)]
async fn missing_track_permission_priority_and_restored_actual_api_reconcile() {
    let (mut room, _) = room(true);
    let sub = active(&mut room, "subscriber", false);
    let track = TrackSid::from("TR_no_permission");
    desire(&mut room, &sub, &track, true);
    room.take_effects();
    let effects = advance(&mut room, Duration::from_secs(63)).await;
    assert_eq!(
        missing_count(&effects, &sub, &track),
        0,
        "CanSubscribe checked before missing track"
    );
    let permission = room
        .get_participant(&Identity::from("subscriber"))
        .unwrap()
        .permission;
    room.api_update_participant(
        &Identity::from("subscriber"),
        "",
        "",
        &BTreeMap::new(),
        Some(rtc_core::permission::ParticipantPermission {
            can_subscribe: true,
            ..permission
        }),
        now(),
    )
    .unwrap();
    let effects = room.take_effects();
    assert_eq!(
        missing_count(&effects, &sub, &track),
        1,
        "permission API queues genuine resolver cause without renewing original intent"
    );
    no_reconnect(&mut room, &sub, &effects);
}
#[tokio::test(start_paused = true)]
async fn publication_before_deadline_resolves_real_subscribe_and_does_not_later_fail() {
    let (mut room, _) = room(true);
    let (publisher, transport) = active_with_transport(&mut room, "publisher", true);
    let sub = active(&mut room, "subscriber", true);
    // AddTrack reserves the actual server SID; it is still pending without media.
    room.handle_request(
        &publisher.sid,
        publisher.generation,
        SessionRequest::AddTrack(add_track("pending", TrackKind::Video, TrackSource::Camera)),
        now(),
    );
    let effects = room.take_effects();
    let track = signals_to(&effects, &publisher.sid)
        .iter()
        .find_map(|e| match e {
            SignalEvent::TrackPublished { track, .. } => Some(track.sid.clone()),
            _ => None,
        })
        .expect("actual AddTrack response SID");
    desire(&mut room, &sub, &track, true);
    room.take_effects();
    let effects = advance(&mut room, Duration::from_secs(57)).await;
    assert_eq!(missing_count(&effects, &sub, &track), 0);
    // Accepted media registration is the real owner publication producer.
    room.handle_media_event(
        MediaEvent::RemoteMediaAdded(RemoteMedia {
            registration: rtc_core::media_api::PublicationRegistration::Pending,
            participant: publisher.sid.clone(),
            transport,
            mid: "0".into(),
            kind: TrackKind::Video,
            msid_track_id: "pending".into(),
            rids: vec![],
            mime_type: "video/VP8".into(),
        }),
        now(),
    );
    // Fixed Go publishes an undeclared-SSRC section on its first RTP (pion OnTrack).
    room.handle_media_event(
        MediaEvent::TrackMediaStarted {
            track_sid: track.clone(),
            mime_type: "video/VP8".into(),
            layers: vec![],
        },
        now(),
    );
    let effects = room.take_effects();
    assert!(effects.iter().any(|e|matches!(e,Effect::Media(MediaCommand::Subscribe(spec))if spec.subscriber==sub.sid&&spec.track_sid==track)));
    room.handle_media_event(
        MediaEvent::SubscriptionBound {
            subscriber: sub.sid.clone(),
            track_sid: track.clone(),
            mid: "0".into(),
        },
        now(),
    );
    room.take_effects();
    let effects = advance(&mut room, Duration::from_secs(63)).await;
    assert_eq!(missing_count(&effects, &sub, &track), 0);
    no_reconnect(&mut room, &sub, &effects);
}
