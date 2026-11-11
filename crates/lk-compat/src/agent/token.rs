//! Real agent assignment token; fixed protocol/agent/token.go.
use super::registry::{Registration, WorkerError};
use crate::auth::{TokenClaims, TokenMinter};
use lk_protocol::livekit as pb;
use rtc_core::{model::ParticipantKind, permission::VideoGrant};
use std::{collections::BTreeMap, time::Duration};
pub fn mint_assignment(
    minter: &TokenMinter,
    api_key: &str,
    registration: &Registration,
    job: &pb::Job,
    response: &pb::AvailabilityResponse,
    now_s: i64,
) -> Result<String, WorkerError> {
    let room = job
        .room
        .as_ref()
        .ok_or_else(|| WorkerError::Token("missing job room".into()))?;
    let permissions = &registration.permissions;
    let mut attributes: BTreeMap<String, String> = response
        .participant_attributes
        .clone()
        .into_iter()
        .collect();
    attributes.insert("lk.agent_name".into(), registration.key.agent_name.clone());
    let claims = TokenClaims {
        identity: response.participant_identity.clone(),
        name: response.participant_name.clone(),
        metadata: response.participant_metadata.clone(),
        attributes,
        kind: Some(ParticipantKind::Agent),
        video: Some(VideoGrant {
            room_join: true,
            agent: true,
            room: room.name.clone(),
            can_subscribe: Some(permissions.can_subscribe),
            can_publish: Some(permissions.can_publish),
            can_publish_data: Some(permissions.can_publish_data),
            hidden: permissions.hidden,
            can_update_own_metadata: Some(permissions.can_update_metadata),
            can_subscribe_metrics: Some(permissions.can_subscribe_metrics),
            ..Default::default()
        }),
        ..Default::default()
    };
    minter
        .mint(api_key, &claims, Duration::from_secs(3600), now_s)
        .map_err(|e| WorkerError::Token(e.to_string()))
}
#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    #[test]
    fn real_assignment_token_verifies_as_agent_with_source_permissions_and_name_attribute() {
        let keys = Arc::new(crate::auth::KeyStore::new(vec![(
            "unit-agent-key".into(),
            "public-unit-fixture-secret".into(),
        )]));
        let minter = TokenMinter::new(keys.clone());
        let registration = Registration::from_request(
            "AW_unit".into(),
            1,
            &pb::RegisterWorkerRequest {
                agent_name: "source-agent".into(),
                allowed_permissions: Some(pb::ParticipantPermission {
                    can_subscribe: true,
                    can_publish: false,
                    can_publish_data: true,
                    hidden: true,
                    can_update_metadata: true,
                    can_subscribe_metrics: true,
                    ..Default::default()
                }),
                ..Default::default()
            },
        )
        .unwrap();
        let job = pb::Job {
            room: Some(pb::Room {
                name: "unit-room".into(),
                ..Default::default()
            }),
            ..Default::default()
        };
        let response = pb::AvailabilityResponse {
            participant_identity: "agent-identity".into(),
            participant_name: "Agent Name".into(),
            participant_metadata: "unit metadata".into(),
            participant_attributes: [("lk.agent_name".into(), "must-be-overwritten".into())]
                .into_iter()
                .collect(),
            ..Default::default()
        };
        let now = rtc_core::util::unix_ms() / 1000;
        let token = mint_assignment(
            &minter,
            "unit-agent-key",
            &registration,
            &job,
            &response,
            now,
        )
        .unwrap();
        let auth = crate::auth::authenticate_token(&keys, Some(&token))
            .unwrap()
            .unwrap();
        assert_eq!(auth.grants.identity, "agent-identity");
        assert_eq!(auth.grants.name, "Agent Name");
        assert_eq!(auth.grants.kind, ParticipantKind::Agent);
        assert_eq!(auth.grants.video.room, "unit-room");
        assert!(auth.grants.video.room_join);
        assert!(auth.grants.video.agent);
        assert_eq!(auth.grants.video.can_publish, Some(false));
        assert_eq!(auth.grants.video.can_subscribe, Some(true));
        assert!(auth.grants.video.hidden);
        assert_eq!(auth.grants.video.can_subscribe_metrics, Some(true));
        assert_eq!(auth.grants.attributes["lk.agent_name"], "source-agent");
        assert_eq!(auth.grants.expires_at, now + 3600);
    }
}
