//! `ClaimGrants` decoding (`protocol/auth/grants.go:161-292`) into engine grants.

use std::collections::BTreeMap;

use lk_protocol::livekit as pb;
use rtc_core::permission::{
    ParticipantPermission, RoomAgentDispatchOptions, RoomConfig, VideoGrant,
};
use serde_json::{Map, Value};

use super::gojson::{DecodeError, GoObject};
use crate::convert::participant_kind_from_claim;

/// `SIPGrant` (`grants.go:553-559`).
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct SipGrant {
    pub admin: bool,
    pub call: bool,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct AgentEndpointGrant {
    pub call: bool,
    pub agent_name: String,
    pub deployment: String,
}
impl AgentEndpointGrant {
    pub fn allows(&self, agent: &str, deployment: &str) -> bool {
        self.call
            && (self.agent_name.is_empty() || self.agent_name == agent)
            && (self.deployment.is_empty() || self.deployment == deployment)
    }
}

/// Grant fields that the engine `Grants` does not model.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ExtraGrants {
    pub sip: SipGrant,
    pub agent_endpoint: AgentEndpointGrant,
    /// `video.canManageAgentSession` (tri-state, default false).
    pub can_manage_agent_session: Option<bool>,
    /// `video` was present (non-null). Upstream permission predicates fail when
    /// the video grant is nil; with the engine's default grant they fail anyway,
    /// so this is informational.
    pub has_video: bool,
}

/// Decoded `ClaimGrants` (identity is filled by the verifier from `sub`/`jti`).
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct DecodedGrants {
    pub name: String,
    pub kind: rtc_core::model::ParticipantKind,
    pub metadata: String,
    pub attributes: BTreeMap<String, String>,
    pub video: VideoGrant,
    pub sha256: String,
    pub room_preset: String,
    pub room_config: Option<RoomConfig>,
    pub extra: ExtraGrants,
}

fn video_grant(o: GoObject<'_>) -> Result<(VideoGrant, Option<bool>), DecodeError> {
    let can_publish_sources = if matches!(o.get("canPublishSources"), Some(Value::Null)) {
        Vec::new()
    } else {
        o.string_list("canPublishSources")?
    };
    let v = VideoGrant {
        room_create: o.bool("roomCreate")?,
        room_list: o.bool("roomList")?,
        room_record: o.bool("roomRecord")?,
        room_admin: o.bool("roomAdmin")?,
        room_join: o.bool("roomJoin")?,
        room: o.string("room")?,
        destination_room: o.string("destinationRoom")?,
        can_publish: o.opt_bool("canPublish")?,
        can_subscribe: o.opt_bool("canSubscribe")?,
        can_publish_data: o.opt_bool("canPublishData")?,
        can_publish_sources,
        can_update_own_metadata: o.opt_bool("canUpdateOwnMetadata")?,
        ingress_admin: o.bool("ingressAdmin")?,
        hidden: o.bool("hidden")?,
        recorder: o.bool("recorder")?,
        agent: o.bool("agent")?,
        can_subscribe_metrics: o.opt_bool("canSubscribeMetrics")?,
        can_manage_agent_session: o.opt_bool("canManageAgentSession")?,
    };
    let manage = v.can_manage_agent_session;
    Ok((v, manage))
}

/// Actual JWT RoomConfiguration wrapper uses permissive descriptor ProtoJSON.
/// Preserve the pinned floating negative zero across the intermediate wire
/// projection; implicit-default comparison in prost otherwise drops its sign.
pub(super) fn decode_room_config_proto(
    value: &serde_json::Value,
) -> Result<pb::RoomConfiguration, DecodeError> {
    let bytes = serde_json::to_vec(value).map_err(|e| DecodeError(e.to_string()))?;
    let mut config: pb::RoomConfiguration = lk_protocol::json::decode(&bytes)
        .map_err(|e| DecodeError(format!("proto: (line 1): {e}")))?;
    for role in ["participant", "room"] {
        let raw = value.pointer(&format!("/egress/{role}/advanced"));
        let number = raw.and_then(|v| {
            v.get("keyFrameInterval")
                .or_else(|| v.get("key_frame_interval"))
        });
        let negative_zero = number
            .and_then(|v| match v {
                serde_json::Value::Number(n) => n.as_f64(),
                serde_json::Value::String(n) => n.parse::<f64>().ok(),
                _ => None,
            })
            .is_some_and(|v| v == 0.0 && v.is_sign_negative());
        if negative_zero && let Some(egress) = &mut config.egress {
            let options = if role == "participant" {
                egress
                    .participant
                    .as_mut()
                    .and_then(|p| match &mut p.options {
                        Some(pb::auto_participant_egress::Options::Advanced(v)) => Some(v),
                        _ => None,
                    })
            } else {
                egress.room.as_mut().and_then(|p| match &mut p.options {
                    Some(pb::room_composite_egress_request::Options::Advanced(v)) => Some(v),
                    _ => None,
                })
            };
            if let Some(options) = options {
                options.key_frame_interval = -0.0;
            }
        }
    }
    Ok(config)
}

/// `RoomConfiguration` (protojson, both name styles, unknown fields discarded)
/// → engine room options. Only values upstream treats as "set" (> 0 / non-empty /
/// true) become `Some`.
pub fn room_config_from_proto(c: &pb::RoomConfiguration) -> RoomConfig {
    RoomConfig {
        tags: c.tags.iter().map(|(k, v)| (k.clone(), v.clone())).collect(),
        automatic_recording: c
            .egress
            .as_ref()
            .map(crate::workers::auto_egress_policy::room_egress_in),
        empty_timeout_s: (c.empty_timeout > 0).then_some(c.empty_timeout),
        departure_timeout_s: (c.departure_timeout > 0).then_some(c.departure_timeout),
        max_participants: (c.max_participants > 0).then_some(c.max_participants),
        metadata: (!c.metadata.is_empty()).then(|| c.metadata.clone()),
        min_playout_delay: (c.min_playout_delay > 0).then_some(c.min_playout_delay),
        max_playout_delay: (c.max_playout_delay > 0).then_some(c.max_playout_delay),
        sync_streams: c.sync_streams.then_some(true),
        agent_dispatches: (!c.agents.is_empty())
            .then(|| c.agents.iter().map(room_agent_from_proto).collect()),
    }
}

/// Engine room options → `RoomConfiguration`; agents survive refreshed tokens.
/// Known recording options and search tags remain intact; this does not add a tag index.
pub fn room_config_to_proto(c: &RoomConfig) -> pb::RoomConfiguration {
    pb::RoomConfiguration {
        tags: c.tags.iter().map(|(k, v)| (k.clone(), v.clone())).collect(),
        egress: c
            .automatic_recording
            .as_ref()
            .map(crate::workers::auto_egress_policy::room_egress_out),
        empty_timeout: c.empty_timeout_s.unwrap_or(0),
        departure_timeout: c.departure_timeout_s.unwrap_or(0),
        max_participants: c.max_participants.unwrap_or(0),
        metadata: c.metadata.clone().unwrap_or_default(),
        min_playout_delay: c.min_playout_delay.unwrap_or(0),
        max_playout_delay: c.max_playout_delay.unwrap_or(0),
        sync_streams: c.sync_streams.unwrap_or(false),
        agents: c
            .agent_dispatches
            .as_deref()
            .unwrap_or_default()
            .iter()
            .map(room_agent_to_proto)
            .collect(),
        ..Default::default()
    }
}

/// Project a wire agent specification without retaining protobuf engine types.
pub fn room_agent_from_proto(agent: &pb::RoomAgentDispatch) -> RoomAgentDispatchOptions {
    RoomAgentDispatchOptions {
        agent_name: agent.agent_name.clone(),
        metadata: agent.metadata.clone(),
        restart_policy: agent.restart_policy,
        deployment: agent.deployment.clone(),
        attributes: agent
            .attributes
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect(),
    }
}

fn room_agent_to_proto(agent: &RoomAgentDispatchOptions) -> pb::RoomAgentDispatch {
    pb::RoomAgentDispatch {
        agent_name: agent.agent_name.clone(),
        metadata: agent.metadata.clone(),
        restart_policy: agent.restart_policy,
        deployment: agent.deployment.clone(),
        attributes: agent
            .attributes
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect(),
    }
}

/// Decode the grant part of a claim set.
pub fn decode_grants(claims: &Map<String, Value>) -> Result<DecodedGrants, String> {
    decode_inner(claims).map_err(|e| e.0)
}

fn decode_inner(claims: &Map<String, Value>) -> Result<DecodedGrants, DecodeError> {
    let o = GoObject::new(claims, "tokenClaims");
    // Decoded for type checking only (the verifier overwrites identity; kind
    // details and cloud-only grants are not used by the OSS server).
    o.string("identity")?;
    o.string_list("kindDetails")?;
    for (tag, ty) in [
        ("agent", "auth.AgentGrant"),
        ("inference", "auth.InferenceGrant"),
        ("observability", "auth.ObservabilityGrant"),
        ("agentEndpoint", "auth.AgentEndpointGrant"),
        ("agentThread", "auth.AgentThreadGrant"),
    ] {
        o.object(tag, ty)?;
    }

    let mut out = DecodedGrants {
        name: o.string("name")?,
        kind: participant_kind_from_claim(&o.string("kind")?),
        metadata: o.string("metadata")?,
        attributes: o.string_map("attributes")?.into_iter().collect(),
        sha256: o.string("sha256")?,
        room_preset: o.string("roomPreset")?,
        ..Default::default()
    };

    if let Some(video) = o.object("video", "auth.VideoGrant")? {
        let (v, manage) = video_grant(GoObject::new(video, "tokenClaims.video"))?;
        out.video = v;
        out.extra.can_manage_agent_session = manage;
        out.extra.has_video = true;
    }
    if let Some(endpoint) = o.object("agentEndpoint", "auth.AgentEndpointGrant")? {
        let endpoint = GoObject::new(endpoint, "tokenClaims.agentEndpoint");
        out.extra.agent_endpoint = AgentEndpointGrant {
            call: endpoint.bool("call")?,
            agent_name: endpoint.string("agentName")?,
            deployment: endpoint.string("deployment")?,
        };
    }
    if let Some(sip) = o.object("sip", "auth.SIPGrant")? {
        let s = GoObject::new(sip, "tokenClaims.sip");
        out.extra.sip = SipGrant {
            admin: s.bool("admin")?,
            call: s.bool("call")?,
        };
    }
    if let Some(rc) = o.get("roomConfig") {
        let cfg = decode_room_config_proto(rc)?;
        out.room_config = Some(room_config_from_proto(&cfg));
    }
    Ok(out)
}

/// `VideoGrant.ToPermission()` with the separately decoded
/// `canManageAgentSession` claim applied to the effective permission.
pub fn permission_from_grants(video: &VideoGrant, extra: &ExtraGrants) -> ParticipantPermission {
    let mut p = video.to_permission();
    p.can_publish_data = video.can_publish_data.unwrap_or(p.can_publish);
    p.can_manage_agent_session = extra.can_manage_agent_session.unwrap_or(false);
    p
}

/// `VideoGrant.UpdateFromPermission`: every permission becomes an explicit value.
pub fn video_grant_with_permission(video: &VideoGrant, p: &ParticipantPermission) -> VideoGrant {
    let mut v = video.clone();
    v.can_publish = Some(p.can_publish);
    v.can_publish_data = Some(p.can_publish_data);
    v.can_publish_sources = p
        .can_publish_sources
        .iter()
        .map(|s| rtc_core::permission::canonical_source(*s).into_owned())
        .collect();
    v.can_subscribe = Some(p.can_subscribe);
    v.can_update_own_metadata = Some(p.can_update_metadata);
    v.hidden = p.hidden;
    v.recorder = p.recorder;
    v.agent = p.agent;
    v.can_subscribe_metrics = Some(p.can_subscribe_metrics);
    v
}

#[cfg(test)]
mod tests {
    use super::*;
    use rtc_core::model::{ParticipantKind, TrackSource};
    use serde_json::json;

    #[test]
    fn endpoint_grant_is_exact_case_sensitive_and_cannot_be_replaced_by_video_agent() {
        let grant =
            decode(json!({"agentEndpoint":{"call":true,"agentName":"Agent","deployment":"prod"}}))
                .unwrap()
                .extra
                .agent_endpoint;
        assert!(grant.allows("Agent", "prod"));
        assert!(!grant.allows("agent", "prod"));
        assert!(!grant.allows("Agent", "Prod"));
        assert!(
            !decode(json!({"video":{"agent":true}}))
                .unwrap()
                .extra
                .agent_endpoint
                .allows("Agent", "prod")
        );
        assert!(decode(json!({"agentEndpoint":{"call":"true"}})).is_err());
        assert!(decode(json!({"agentEndpoint":{"agentName":7}})).is_err());
        assert!(
            !decode(json!({"agentEndpoint":null}))
                .unwrap()
                .extra
                .agent_endpoint
                .allows("Agent", "prod")
        );
        assert!(
            decode(json!({"agentEndpoint":{"call":true}}))
                .unwrap()
                .extra
                .agent_endpoint
                .allows("any", "deployment")
        );
    }

    fn decode(v: Value) -> Result<DecodedGrants, String> {
        decode_grants(v.as_object().unwrap())
    }

    #[test]
    fn video_defaults_and_tristate() {
        let g =
            decode(json!({"video": {"roomJoin": true, "room": "r", "canPublish": false}})).unwrap();
        assert!(g.video.room_join);
        assert_eq!(g.video.room, "r");
        assert_eq!(g.video.can_publish, Some(false));
        assert_eq!(g.video.can_subscribe, None);
        let p = permission_from_grants(&g.video, &g.extra);
        assert!(!p.can_publish);
        // canPublishData defaults to GetCanPublish()
        assert!(!p.can_publish_data);
        assert!(p.can_subscribe);
        assert!(!p.can_update_metadata);
    }

    #[test]
    fn sources_kind_and_attributes() {
        let g = decode(json!({
            "kind": "Agent",
            "attributes": {"k": "v"},
            "metadata": "m",
            "video": {"canPublishSources": ["camera", "MICROPHONE"]},
            "sip": {"admin": true}
        }))
        .unwrap();
        assert_eq!(g.kind, ParticipantKind::Agent);
        assert_eq!(g.attributes.get("k").map(String::as_str), Some("v"));
        assert_eq!(
            g.video.can_publish_sources,
            vec!["camera".to_owned(), "MICROPHONE".to_owned()]
        );
        assert_eq!(
            g.video.to_permission().can_publish_sources,
            vec![TrackSource::Camera, TrackSource::Microphone]
        );
        assert!(g.video.can_publish_source(TrackSource::Camera));
        assert!(
            !g.video.can_publish_source(TrackSource::Microphone),
            "uppercase raw entry never grants canonical microphone"
        );
        assert!(g.extra.sip.admin);
        assert!(!g.extra.sip.call);
    }

    #[test]
    fn room_config_both_name_styles() {
        let g = decode(
            json!({"roomConfig": {"emptyTimeout": 10, "max_participants": 3, "unknownField": 1}}),
        )
        .unwrap();
        let rc = g.room_config.unwrap();
        assert_eq!(rc.empty_timeout_s, Some(10));
        assert_eq!(rc.max_participants, Some(3));
        assert_eq!(rc.departure_timeout_s, None);
    }

    #[test]
    fn type_errors() {
        assert!(decode(json!({"video": {"roomJoin": "true"}})).is_err());
        assert!(decode(json!({"video": "x"})).is_err());
        assert!(decode(json!({"attributes": {"a": 1}})).is_err());
        assert!(decode(json!({"video": null})).is_ok());
    }

    #[test]
    fn update_from_permission_is_explicit() {
        let v = VideoGrant::default();
        let p = ParticipantPermission::default();
        let u = video_grant_with_permission(&v, &p);
        assert_eq!(u.can_publish, Some(true));
        assert_eq!(u.can_update_own_metadata, Some(false));
        assert_eq!(u.can_subscribe_metrics, Some(false));
    }

    #[test]
    fn room_agent_configuration_survives_protobuf_and_signed_jwt_roundtrip() {
        use crate::auth::{KeyStore, TokenClaims, TokenMinter, verify_token};
        use prost::Message;
        use std::sync::Arc;
        use std::time::Duration;
        let wire = pb::RoomConfiguration {
            agents: vec![pb::RoomAgentDispatch {
                agent_name: "assistant".into(),
                metadata: "context".into(),
                restart_policy: 1,
                deployment: "東京-prod".into(),
                attributes: [("language".into(), "日本語".into())].into_iter().collect(),
            }],
            ..Default::default()
        };
        let decoded = pb::RoomConfiguration::decode(wire.encode_to_vec().as_slice()).unwrap();
        let core = room_config_from_proto(&decoded);
        assert_eq!(room_config_to_proto(&core), wire);
        let keys = Arc::new(KeyStore::new(vec![("api".into(), "secret".into())]));
        let minter = TokenMinter::new(keys.clone());
        let jwt = minter
            .mint(
                "api",
                &TokenClaims {
                    identity: "caller".into(),
                    room_config: Some(core.clone()),
                    ..Default::default()
                },
                Duration::from_secs(600),
                1_000,
            )
            .unwrap();
        let verified = verify_token(&keys, &jwt, 1_001.0).unwrap();
        assert_eq!(verified.grants.room_config, Some(core.clone()));
        let refreshed = minter
            .refresh_token_at(&verified.grants, None, 1_002)
            .unwrap();
        assert_eq!(
            verify_token(&keys, &refreshed, 1_003.0)
                .unwrap()
                .grants
                .room_config,
            Some(core)
        );
    }
}

#[cfg(test)]
mod automatic_recording_token_tests {
    use super::*;
    use crate::auth::{KeyStore, TokenClaims, TokenMinter, verify_token};
    use std::{sync::Arc, time::Duration};
    #[test]
    fn complete_known_recording_oneofs_survive_signed_join_and_refresh() {
        let wire = pb::RoomConfiguration {
            egress: Some(pb::RoomEgress {
                room: Some(pb::RoomCompositeEgressRequest {
                    room_name: "overridden-owner".into(),
                    audio_only: true,
                    audio_mixing: 2,
                    output: Some(pb::room_composite_egress_request::Output::Stream(
                        pb::StreamOutput {
                            protocol: 3,
                            urls: vec!["wss://synthetic.invalid".into()],
                        },
                    )),
                    options: Some(pb::room_composite_egress_request::Options::Preset(0)),
                    file_outputs: vec![pb::EncodedFileOutput {
                        filepath: "room.ogg".into(),
                        disable_manifest: true,
                        ..Default::default()
                    }],
                    webhooks: vec![pb::WebhookConfig {
                        url: "https://synthetic.invalid".into(),
                        signing_key: "api".into(),
                        filter_params: Some(pb::FilterParams {
                            include_events: vec!["egress_ended".into()],
                            exclude_events: vec![],
                        }),
                    }],
                    ..Default::default()
                }),
                participant: Some(pb::AutoParticipantEgress {
                    options: Some(pb::auto_participant_egress::Options::Advanced(
                        pb::EncodingOptions {
                            key_frame_interval: -0.0,
                            audio_bitrate: 96,
                            ..Default::default()
                        },
                    )),
                    ..Default::default()
                }),
                tracks: Some(pb::AutoTrackEgress {
                    filepath: "track.ogg".into(),
                    disable_manifest: true,
                    output: Some(pb::auto_track_egress::Output::S3(pb::S3Upload {
                        access_key: "synthetic-key".into(),
                        secret: "synthetic-secret".into(),
                        ..Default::default()
                    })),
                }),
            }),
            ..Default::default()
        };
        let projected = room_config_from_proto(&wire);
        assert_eq!(room_config_to_proto(&projected), wire);
        assert!(!format!("{projected:?}").contains("synthetic-secret"));
        let keys = Arc::new(KeyStore::new(vec![("api".into(), "secret".into())]));
        let minter = TokenMinter::new(keys.clone());
        let claims = TokenClaims {
            identity: "publisher".into(),
            room_preset: "record".into(),
            room_config: Some(projected.clone()),
            ..Default::default()
        };
        assert_eq!(
            minter
                .mint("api", &claims, Duration::from_secs(600), 1000)
                .unwrap_err()
                .to_string(),
            "room configuration should not contain sensitive credentials"
        );
        let token = minter
            .mint_with_sensitive_credentials("api", &claims, Duration::from_secs(600), 1000, true)
            .unwrap();
        let verified = verify_token(&keys, &token, 1001.0).unwrap();
        assert_eq!(verified.grants.room_config, Some(projected.clone()));
        assert_eq!(verified.grants.room_preset, "record");
        assert_eq!(
            minter
                .refresh_token_at(&verified.grants, None, 1002)
                .unwrap_err()
                .to_string(),
            "room configuration should not contain sensitive credentials"
        );
        // A separate source-safe control retains the complete original input
        // above, including its -0 and sensitive oneofs, without erasing them.
        let mut safe_wire = wire.clone();
        let egress = safe_wire.egress.as_mut().unwrap();
        if let Some(pb::auto_track_egress::Output::S3(s3)) =
            &mut egress.tracks.as_mut().unwrap().output
        {
            s3.secret.clear();
        }
        egress.room.as_mut().unwrap().stream_outputs.clear();
        let safe = room_config_from_proto(&safe_wire);
        let token = minter
            .mint(
                "api",
                &TokenClaims {
                    room_config: Some(safe.clone()),
                    ..claims
                },
                Duration::from_secs(600),
                1000,
            )
            .unwrap();
        let verified = verify_token(&keys, &token, 1001.0).unwrap();
        assert_eq!(verified.grants.room_config, Some(safe.clone()));
        let refreshed = minter
            .refresh_token_at(&verified.grants, None, 1002)
            .unwrap();
        assert_eq!(
            verify_token(&keys, &refreshed, 1003.0)
                .unwrap()
                .grants
                .room_config,
            Some(safe)
        );
    }
}

#[cfg(test)]
mod room_tag_projection_tests {
    use super::*;
    #[test]
    fn actual_claims_projection_does_not_discard_known_tags_on_refresh() {
        let proto = pb::RoomConfiguration {
            tags: [
                ("category".to_owned(), "voice".to_owned()),
                ("utf8".to_owned(), "hé".to_owned()),
            ]
            .into_iter()
            .collect(),
            ..Default::default()
        };
        let neutral = room_config_from_proto(&proto);
        assert_eq!(neutral.tags.len(), 2);
        assert_eq!(room_config_to_proto(&neutral).tags, proto.tags);
        let raw = serde_json::json!({"roomConfig":{"tags":{"category":"voice","utf8":"hé"}}});
        let decoded = decode_grants(raw.as_object().unwrap()).unwrap();
        assert_eq!(
            room_config_to_proto(&decoded.room_config.unwrap()).tags,
            proto.tags
        );
    }
}

#[cfg(test)]
#[path = "kind_details_null_tests.rs"]
mod kind_details_null_tests;
