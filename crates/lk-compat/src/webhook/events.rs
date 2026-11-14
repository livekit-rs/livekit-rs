//! Engine event → `WebhookEvent` (impl-notes/api-auth-webhook.md §4.2,
//! `pkg/telemetry/events.go`).

use lk_protocol::livekit as pb;
use rtc_core::events::EngineEvent;
use rtc_core::model::ParticipantInfo;

use crate::convert::{
    minimal_room_proto, participant_info_to_proto, room_end_reason_to_proto, room_info_to_proto,
    track_info_to_proto,
};

pub const ROOM_STARTED: &str = "room_started";
pub const ROOM_FINISHED: &str = "room_finished";
pub const PARTICIPANT_JOINED: &str = "participant_joined";
pub const PARTICIPANT_LEFT: &str = "participant_left";
pub const PARTICIPANT_CONNECTION_ABORTED: &str = "participant_connection_aborted";
pub const TRACK_PUBLISHED: &str = "track_published";
pub const TRACK_UNPUBLISHED: &str = "track_unpublished";

/// Participant as included in track events: `{sid, identity}` only.
fn participant_ref(p: &ParticipantInfo) -> pb::ParticipantInfo {
    pb::ParticipantInfo {
        sid: p.sid.0.clone(),
        identity: p.identity.0.clone(),
        ..Default::default()
    }
}

/// Build the event payload without `id`/`created_at` (set at enqueue).
pub fn to_webhook_event(event: &EngineEvent, full_room_in_track_events: bool) -> pb::WebhookEvent {
    let mut out = pb::WebhookEvent::default();
    match event {
        EngineEvent::RoomStarted { room } => {
            out.event = ROOM_STARTED.into();
            out.room = Some(room_info_to_proto(room));
        }
        EngineEvent::RoomFinished { room, reason } => {
            out.event = ROOM_FINISHED.into();
            out.room = Some(room_info_to_proto(room));
            out.room_end_reason = room_end_reason_to_proto(*reason);
        }
        EngineEvent::ParticipantJoined { room, participant } => {
            out.event = PARTICIPANT_JOINED.into();
            out.room = Some(room_info_to_proto(room));
            out.participant = Some(participant_info_to_proto(participant));
        }
        EngineEvent::ParticipantLeft { room, participant } => {
            out.event = PARTICIPANT_LEFT.into();
            out.room = Some(room_info_to_proto(room));
            out.participant = Some(participant_info_to_proto(participant));
        }
        EngineEvent::ParticipantConnectionAborted { room, participant } => {
            out.event = PARTICIPANT_CONNECTION_ABORTED.into();
            out.room = Some(room_info_to_proto(room));
            out.participant = Some(participant_info_to_proto(participant));
        }
        EngineEvent::TrackPublished {
            room,
            participant,
            track,
        }
        | EngineEvent::TrackUnpublished {
            room,
            participant,
            track,
        } => {
            out.event = if matches!(event, EngineEvent::TrackPublished { .. }) {
                TRACK_PUBLISHED.into()
            } else {
                TRACK_UNPUBLISHED.into()
            };
            out.room = Some(if full_room_in_track_events {
                room_info_to_proto(room)
            } else {
                minimal_room_proto(room)
            });
            out.participant = Some(participant_ref(participant));
            out.track = Some(track_info_to_proto(track));
        }
    }
    out
}

/// Queue key (`EventKey`, notifier.go:207-225): first of egress id, ingress id,
/// room name, participant identity, track sid, else `"default"`.
pub fn event_key(e: &pb::WebhookEvent) -> String {
    if let Some(eg) = &e.egress_info {
        return eg.egress_id.clone();
    }
    if let Some(ing) = &e.ingress_info {
        return ing.ingress_id.clone();
    }
    if let Some(r) = &e.room {
        return r.name.clone();
    }
    if let Some(p) = &e.participant {
        return p.identity.clone();
    }
    if let Some(t) = &e.track {
        return t.sid.clone();
    }
    "default".into()
}

/// Include list wins over exclude list (`filter.go`).
pub fn is_allowed(event: &str, include: &[String], exclude: &[String]) -> bool {
    if !include.is_empty() {
        return include.iter().any(|e| e == event);
    }
    if !exclude.is_empty() {
        return !exclude.iter().any(|e| e == event);
    }
    true
}

#[cfg(test)]
mod tests {
    use super::*;
    use rtc_core::events::RoomEndReason;
    use rtc_core::ids::*;
    use rtc_core::model::*;
    use rtc_core::permission::ParticipantPermission;

    fn room() -> RoomInfo {
        RoomInfo {
            turn_password: String::new(),
            sid: RoomSid("RM_a".into()),
            name: RoomName("r".into()),
            empty_timeout_s: 300,
            departure_timeout_s: 20,
            max_participants: 0,
            creation_time_ms: 1_700_000_000_000,
            metadata: String::new(),
            num_participants: 0,
            num_publishers: 0,
            active_recording: false,
            enabled_codecs: vec![],
            version: TimedVersion::default(),
        }
    }

    fn participant() -> ParticipantInfo {
        ParticipantInfo {
            sid: ParticipantSid("PA_a".into()),
            identity: Identity("alice".into()),
            name: "Alice".into(),
            state: ParticipantState::Active,
            tracks: vec![],
            data_tracks: vec![],
            metadata: "m".into(),
            attributes: Default::default(),
            joined_at_ms: 0,
            version: 1,
            permission: ParticipantPermission::default(),
            region: String::new(),
            is_publisher: false,
            kind: ParticipantKind::Standard,
            disconnect_reason: DisconnectReason::Unknown,
            client_protocol: 0,
            capabilities: vec![],
        }
    }

    fn track() -> TrackInfo {
        TrackInfo {
            sid: TrackSid("TR_a".into()),
            kind: TrackKind::Audio,
            name: String::new(),
            muted: false,
            width: 0,
            height: 0,
            simulcast: false,
            source: TrackSource::Microphone,
            layers: vec![],
            mime_type: "audio/opus".into(),
            mid: String::new(),
            codecs: vec![],
            disable_red: false,
            disable_dtx: false,
            stereo: false,
            encryption: EncryptionType::None,
            stream: String::new(),
            version: TimedVersion::default(),
            audio_features: vec![],
            backup_codec_policy: BackupCodecPolicy::PreferRegression,
            packet_trailer_features: vec![],
        }
    }

    #[test]
    fn track_events_use_minimal_room_and_participant() {
        let e = EngineEvent::TrackPublished {
            room: room(),
            participant: participant(),
            track: track(),
        };
        let w = to_webhook_event(&e, false);
        assert_eq!(w.event, "track_published");
        let r = w.room.unwrap();
        assert_eq!(
            (r.sid.as_str(), r.name.as_str(), r.empty_timeout),
            ("RM_a", "r", 0)
        );
        let p = w.participant.unwrap();
        assert_eq!(
            (p.identity.as_str(), p.name.as_str(), p.metadata.as_str()),
            ("alice", "", "")
        );
        assert_eq!(w.track.unwrap().sid, "TR_a");

        let w = to_webhook_event(&e, true);
        assert_eq!(w.room.unwrap().empty_timeout, 300);
    }

    #[test]
    fn room_finished_reason_and_key() {
        let w = to_webhook_event(
            &EngineEvent::RoomFinished {
                room: room(),
                reason: RoomEndReason::IdleTimeout,
            },
            false,
        );
        assert_eq!(
            w.room_end_reason,
            pb::RoomEndReason::RoomEndIdleTimeout as i32
        );
        assert_eq!(event_key(&w), "r");
        assert_eq!(event_key(&pb::WebhookEvent::default()), "default");
    }

    #[test]
    fn camel_case_body_omits_defaults() {
        let mut w = to_webhook_event(&EngineEvent::RoomStarted { room: room() }, false);
        w.id = "EV_x".into();
        w.created_at = 1_700_000_000;
        let v: serde_json::Value = serde_json::to_value(&w).unwrap();
        assert_eq!(v["event"], "room_started");
        assert_eq!(v["createdAt"], "1700000000");
        assert_eq!(v["room"]["emptyTimeout"], 300);
        assert!(v["room"].get("maxParticipants").is_none());
        assert!(v.get("roomEndReason").is_none());
    }

    #[test]
    fn filters() {
        let inc = vec!["room_started".to_string()];
        let exc = vec!["room_started".to_string()];
        assert!(is_allowed("room_started", &inc, &exc));
        assert!(!is_allowed("room_finished", &inc, &[]));
        assert!(!is_allowed("room_started", &[], &exc));
        assert!(is_allowed("x", &[], &[]));
    }
}
