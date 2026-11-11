//! Token minting: refreshed participant tokens and webhook signatures.
//! Claim layout and `omitempty` rules follow `protocol/auth/accesstoken.go` and
//! `grants.go`; signing is HS256 via `jsonwebtoken::encode`.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use base64::Engine;
use base64::engine::general_purpose::STANDARD;
use jsonwebtoken::{Algorithm, EncodingKey, Header};
use rtc_core::model::ParticipantKind;
use rtc_core::permission::{Grants, ParticipantPermission, RoomConfig, VideoGrant};
use serde_json::{Map, Value, json};
use sha2::{Digest, Sha256};

use super::KeyStore;
use super::claims::{room_config_to_proto, video_grant_with_permission};
use crate::convert::participant_kind_to_claim;

/// Minimum lifetime of a refreshed token (`tokenDefaultTTL`, roommanager.go:63).
pub const REFRESH_MIN_TTL: Duration = Duration::from_secs(10 * 60);
/// Lifetime of a webhook signature token (`resource_url_notifier.go:373-376`).
pub const WEBHOOK_TOKEN_TTL: Duration = Duration::from_secs(5 * 60);

#[derive(Debug, thiserror::Error)]
pub enum MintError {
    #[error("no API key configured")]
    NoKeys,
    #[error("no singing key or secret was provided")]
    UnknownKey,
    #[error("could not sign token: {0}")]
    Sign(#[from] jsonwebtoken::errors::Error),
    #[error("room configuration should not contain sensitive credentials")]
    SensitiveCredentials,
    #[error("could not serialize token claims: {0}")]
    ClaimsJson(#[from] serde_json::Error),
    #[error("pinned room configuration JSON layout mismatch")]
    ClaimLayout,
}

/// Contents of an access token besides the registered claims.
#[derive(Clone, Debug, Default)]
pub struct TokenClaims {
    pub identity: String,
    pub name: String,
    pub kind: Option<ParticipantKind>,
    pub metadata: String,
    pub attributes: BTreeMap<String, String>,
    pub video: Option<VideoGrant>,
    /// `video.canManageAgentSession`; only emitted together with `video`.
    pub can_manage_agent_session: Option<bool>,
    pub room_config: Option<RoomConfig>,
    pub room_preset: String,
    pub sha256: String,
}

fn put_str(m: &mut Map<String, Value>, k: &str, v: &str) {
    if !v.is_empty() {
        m.insert(k.to_string(), Value::String(v.to_string()));
    }
}

fn put_true(m: &mut Map<String, Value>, k: &str, v: bool) {
    if v {
        m.insert(k.to_string(), Value::Bool(true));
    }
}

fn put_opt(m: &mut Map<String, Value>, k: &str, v: Option<bool>) {
    if let Some(b) = v {
        m.insert(k.to_string(), Value::Bool(b));
    }
}

/// `VideoGrant` JSON with Go `omitempty` semantics.
pub fn video_grant_json(v: &VideoGrant, can_manage_agent_session: Option<bool>) -> Value {
    let mut m = Map::new();
    put_true(&mut m, "roomCreate", v.room_create);
    put_true(&mut m, "roomList", v.room_list);
    put_true(&mut m, "roomRecord", v.room_record);
    put_true(&mut m, "roomAdmin", v.room_admin);
    put_true(&mut m, "roomJoin", v.room_join);
    put_str(&mut m, "room", &v.room);
    put_opt(&mut m, "canPublish", v.can_publish);
    put_opt(&mut m, "canSubscribe", v.can_subscribe);
    put_opt(&mut m, "canPublishData", v.can_publish_data);
    if !v.can_publish_sources.is_empty() {
        m.insert(
            "canPublishSources".into(),
            Value::Array(
                v.can_publish_sources
                    .iter()
                    .map(|s| Value::String(s.clone()))
                    .collect(),
            ),
        );
    }
    put_opt(&mut m, "canUpdateOwnMetadata", v.can_update_own_metadata);
    put_true(&mut m, "ingressAdmin", v.ingress_admin);
    put_true(&mut m, "hidden", v.hidden);
    put_true(&mut m, "recorder", v.recorder);
    put_true(&mut m, "agent", v.agent);
    put_opt(&mut m, "canSubscribeMetrics", v.can_subscribe_metrics);
    put_opt(&mut m, "canManageAgentSession", can_manage_agent_session);
    put_str(&mut m, "destinationRoom", &v.destination_room);
    Value::Object(m)
}

/// Full JWT payload: registered claims (`iat = nbf = now`, `exp = now + valid_for`)
/// merged with the grants.
pub fn claims_json(
    api_key: &str,
    c: &TokenClaims,
    now_s: i64,
    valid_for: Duration,
) -> Result<Value, MintError> {
    let mut m = Map::new();
    m.insert("iss".into(), json!(api_key));
    put_str(&mut m, "sub", &c.identity);
    m.insert(
        "exp".into(),
        json!(now_s.saturating_add(valid_for.as_secs() as i64)),
    );
    m.insert("nbf".into(), json!(now_s));
    m.insert("iat".into(), json!(now_s));
    put_str(&mut m, "identity", &c.identity);
    put_str(&mut m, "name", &c.name);
    if let Some(kind) = c.kind {
        m.insert("kind".into(), json!(participant_kind_to_claim(kind)));
    }
    if let Some(v) = &c.video {
        m.insert(
            "video".into(),
            video_grant_json(v, c.can_manage_agent_session),
        );
    }
    if let Some(rc) = &c.room_config {
        // protojson (camelCase, defaults omitted), as `grants.go:43-49` marshals it.
        m.insert(
            "roomConfig".into(),
            room_config_json(&room_config_to_proto(rc))?,
        );
    }
    put_str(&mut m, "roomPreset", &c.room_preset);
    put_str(&mut m, "sha256", &c.sha256);
    put_str(&mut m, "metadata", &c.metadata);
    if !c.attributes.is_empty() {
        m.insert("attributes".into(), json!(c.attributes));
    }
    Ok(Value::Object(m))
}

/// Pinned RoomConfiguration default ProtoJSON, including a present negative
/// floating zero. Prost/pbjson both otherwise treat -0 as the implicit +0.
fn room_config_json(config: &lk_protocol::livekit::RoomConfiguration) -> Result<Value, MintError> {
    use lk_protocol::livekit as pb;
    let mut value: Value = serde_json::from_slice(&lk_protocol::json::encode_default(config)?)?;
    if let Some(egress) = &config.egress {
        let participant = egress.participant.as_ref().and_then(|p| match &p.options {
            Some(pb::auto_participant_egress::Options::Advanced(options)) => Some(options),
            _ => None,
        });
        let room = egress.room.as_ref().and_then(|p| match &p.options {
            Some(pb::room_composite_egress_request::Options::Advanced(options)) => Some(options),
            _ => None,
        });
        // EncodingOptions.key_frame_interval is the pinned RoomConfiguration
        // graph's only float field. The descriptor regression guards this set.
        for (role, options) in [("participant", participant), ("room", room)] {
            if let Some(options) = options {
                let n = options.key_frame_interval;
                if n == 0.0 && n.is_sign_negative() {
                    let path = format!("/egress/{role}/advanced");
                    let Some(Value::Object(fields)) = value.pointer_mut(&path) else {
                        return Err(MintError::ClaimLayout);
                    };
                    fields.insert("keyFrameInterval".into(), json!(n));
                }
            }
        }
    }
    Ok(value)
}

// Source: protocol/auth/grants.go CheckCredentials/checkOutputForCredentials.
// This preserves its exact selected types, not a broader credential policy.
fn room_config_has_sensitive_credentials(config: &lk_protocol::livekit::RoomConfiguration) -> bool {
    use lk_protocol::livekit as pb;
    fn file(output: &Option<pb::encoded_file_output::Output>) -> bool {
        use pb::encoded_file_output::Output as O;
        match output {
            Some(O::S3(v)) => !v.secret.is_empty(),
            Some(O::Gcp(v)) => !v.credentials.is_empty(),
            Some(O::Azure(v)) => !v.account_key.is_empty(),
            Some(O::AliOss(v)) => !v.secret.is_empty(),
            None => false,
        }
    }
    fn segment(output: &Option<pb::segmented_file_output::Output>) -> bool {
        use pb::segmented_file_output::Output as O;
        match output {
            Some(O::S3(v)) => !v.secret.is_empty(),
            Some(O::Gcp(v)) => !v.credentials.is_empty(),
            Some(O::Azure(v)) => !v.account_key.is_empty(),
            Some(O::AliOss(v)) => !v.secret.is_empty(),
            None => false,
        }
    }
    let Some(egress) = &config.egress else {
        return false;
    };
    if egress.participant.as_ref().is_some_and(|v| {
        v.file_outputs.iter().any(|v| file(&v.output))
            || v.segment_outputs.iter().any(|v| segment(&v.output))
    }) {
        return true;
    }
    if egress.room.as_ref().is_some_and(|v| {
        v.file_outputs.iter().any(|v| file(&v.output))
            || v.segment_outputs.iter().any(|v| segment(&v.output))
            || !v.stream_outputs.is_empty()
        // Source calls checkOutputForCredentials on ImageOutput, but none of
        // its distinct oneof wrapper types match the source type switch.
        // Legacy room.output, image outputs and webhooks are not rejected.
    }) {
        return true;
    }
    use pb::auto_track_egress::Output as O;
    match egress.tracks.as_ref().and_then(|v| v.output.as_ref()) {
        Some(O::S3(v)) => !v.secret.is_empty(),
        Some(O::Gcp(v)) => !v.credentials.is_empty(),
        Some(O::Azure(v)) => !v.account_key.is_empty(),
        Some(O::AliOss(v)) => !v.secret.is_empty(),
        None => false,
    }
}
fn check_room_config_credentials(
    config: Option<&RoomConfig>,
    allow_sensitive: bool,
) -> Result<(), MintError> {
    if !allow_sensitive
        && config.is_some_and(|v| room_config_has_sensitive_credentials(&room_config_to_proto(v)))
    {
        return Err(MintError::SensitiveCredentials);
    }
    Ok(())
}

/// base64 (std, padded) SHA-256 of `body`, as carried in the `sha256` claim.
pub fn body_sha256(body: &[u8]) -> String {
    let digest = Sha256::digest(body);
    STANDARD.encode(&digest[..])
}

/// Signs tokens with configured API keys.
pub struct TokenMinter {
    keys: Arc<KeyStore>,
}

impl TokenMinter {
    pub fn new(keys: Arc<KeyStore>) -> Self {
        Self { keys }
    }

    /// Sign arbitrary claims with `api_key`'s secret.
    pub fn mint(
        &self,
        api_key: &str,
        claims: &TokenClaims,
        valid_for: Duration,
        now_s: i64,
    ) -> Result<String, MintError> {
        self.mint_with_sensitive_credentials(api_key, claims, valid_for, now_s, false)
    }

    /// Source AccessToken.SetAllowSensitiveCredentials. Default mint and all
    /// server refreshes keep this false; inbound verified claims stay accepted.
    pub fn mint_with_sensitive_credentials(
        &self,
        api_key: &str,
        claims: &TokenClaims,
        valid_for: Duration,
        now_s: i64,
        allow_sensitive: bool,
    ) -> Result<String, MintError> {
        let secret = self.keys.secret(api_key).ok_or(MintError::UnknownKey)?;
        check_room_config_credentials(claims.room_config.as_ref(), allow_sensitive)?;
        sign(secret, &claims_json(api_key, claims, now_s, valid_for)?)
    }

    /// Refreshed participant token (roommanager.go:1156-1191) signed with the
    /// first configured key: identity, name, kind, metadata, attributes, video
    /// grant, room config and preset from `grants`;
    /// `validFor = max(10 min, time left on the original token)`.
    ///
    /// Preserve the owner's current video grant, including omitted optional
    /// fields. Metadata-only updates do not materialize permission defaults.
    /// Actual permission changes are applied by the owner before this snapshot;
    /// [`TokenMinter::refresh_token_with_permission`] is the explicit conversion
    /// variant. `canManageAgentSession` follows its stored optional value.
    pub fn refresh_token(&self, grants: &Grants) -> Result<String, MintError> {
        self.refresh_token_at(
            grants,
            grants.video.can_manage_agent_session,
            rtc_core::util::unix_s(),
        )
    }

    /// Like [`TokenMinter::refresh_token`], first rewriting the video grant from
    /// `permission` (including `canManageAgentSession`).
    pub fn refresh_token_with_permission(
        &self,
        grants: &Grants,
        permission: &ParticipantPermission,
        now_s: i64,
    ) -> Result<String, MintError> {
        let mut g = grants.clone();
        g.video = video_grant_with_permission(&grants.video, permission);
        self.refresh_token_at(&g, Some(permission.can_manage_agent_session), now_s)
    }

    pub fn refresh_token_at(
        &self,
        grants: &Grants,
        can_manage_agent_session: Option<bool>,
        now_s: i64,
    ) -> Result<String, MintError> {
        let (api_key, secret) = self.keys.first().ok_or(MintError::NoKeys)?;
        let remaining = grants.expires_at.saturating_sub(now_s);
        let valid_for = if remaining > REFRESH_MIN_TTL.as_secs() as i64 {
            Duration::from_secs(remaining as u64)
        } else {
            REFRESH_MIN_TTL
        };
        let claims = TokenClaims {
            identity: grants.identity.clone(),
            name: grants.name.clone(),
            kind: Some(grants.kind),
            metadata: grants.metadata.clone(),
            attributes: grants.attributes.clone(),
            video: Some(grants.video.clone()),
            can_manage_agent_session,
            room_config: grants.room_config.clone(),
            room_preset: grants.room_preset.clone(),
            sha256: String::new(),
        };
        check_room_config_credentials(claims.room_config.as_ref(), false)?;
        sign(secret, &claims_json(api_key, &claims, now_s, valid_for)?)
    }

    /// Webhook `Authorization` token: `iss`, `nbf`, `iat`, `exp = now + 5 min` and
    /// the body digest; no `sub`, no grants.
    pub fn webhook_token(
        &self,
        api_key: &str,
        body: &[u8],
        now_s: i64,
    ) -> Result<String, MintError> {
        let secret = self
            .keys
            .secret(api_key)
            .filter(|_| !api_key.is_empty())
            .ok_or(MintError::UnknownKey)?;
        let claims = TokenClaims {
            sha256: body_sha256(body),
            ..Default::default()
        };
        sign(
            secret,
            &claims_json(api_key, &claims, now_s, WEBHOOK_TOKEN_TTL)?,
        )
    }
}

fn sign(secret: &str, payload: &Value) -> Result<String, MintError> {
    super::hs256_provider::ensure_installed()?;
    let header = Header::new(Algorithm::HS256);
    Ok(jsonwebtoken::encode(
        &header,
        payload,
        &EncodingKey::from_secret(secret.as_bytes()),
    )?)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::auth::{KeyStore, verify_token};

    fn minter() -> TokenMinter {
        TokenMinter::new(Arc::new(KeyStore::new(vec![
            ("k1".into(), "secret1".into()),
            ("k2".into(), "secret2".into()),
        ])))
    }

    #[test]
    fn webhook_token_claims() {
        let m = minter();
        let body = br#"{"event":"room_started"}"#;
        let jwt = m.webhook_token("k2", body, 1_000).unwrap();
        let keys = KeyStore::new(vec![("k2".into(), "secret2".into())]);
        let t = verify_token(&keys, &jwt, 1_000.0).unwrap();
        assert_eq!(t.grants.api_key, "k2");
        assert_eq!(t.grants.identity, "");
        assert_eq!(t.grants.sha256, body_sha256(body));
        assert_eq!(t.grants.expires_at, 1_300);
        assert!(!t.extra.has_video);
        assert!(m.webhook_token("nope", body, 0).is_err());
    }

    #[test]
    fn refresh_uses_first_key_and_min_ttl() {
        let m = minter();
        let mut g = Grants {
            identity: "alice".into(),
            name: "Alice".into(),
            expires_at: 1_000 + 60,
            ..Default::default()
        };
        g.video.room_join = true;
        g.video.room = "r".into();
        let perm = ParticipantPermission {
            can_publish: false,
            ..Default::default()
        };
        let jwt = m.refresh_token_with_permission(&g, &perm, 1_000).unwrap();
        let keys = KeyStore::new(vec![("k1".into(), "secret1".into())]);
        let t = verify_token(&keys, &jwt, 1_000.0).unwrap();
        assert_eq!(t.grants.api_key, "k1");
        assert_eq!(t.grants.identity, "alice");
        assert_eq!(t.grants.expires_at, 1_000 + 600);
        assert_eq!(t.grants.video.can_publish, Some(false));
        assert_eq!(t.grants.video.can_update_own_metadata, Some(false));
        assert_eq!(t.extra.can_manage_agent_session, Some(false));

        g.expires_at = 1_000 + 7_200;
        let jwt = m.refresh_token_with_permission(&g, &perm, 1_000).unwrap();
        let t = verify_token(&keys, &jwt, 1_000.0).unwrap();
        assert_eq!(t.grants.expires_at, 1_000 + 7_200);
    }

    #[test]
    fn omitempty_video() {
        let v = VideoGrant {
            room_join: true,
            can_publish: Some(false),
            ..Default::default()
        };
        let j = video_grant_json(&v, None);
        assert_eq!(j, json!({"roomJoin": true, "canPublish": false}));
    }
}

#[cfg(test)]
mod room_config_jwt_source_tests {
    use super::*;
    use crate::auth::{
        claims::{decode_room_config_proto, room_config_from_proto},
        verify_token,
    };
    use lk_protocol::livekit as pb;
    #[test]
    fn pinned_room_config_float_leaf_set_is_explicit() {
        use std::collections::BTreeSet;
        let mut stack = vec![
            lk_protocol::json::descriptor_pool()
                .get_message_by_name("livekit.RoomConfiguration")
                .unwrap(),
        ];
        let mut seen = BTreeSet::new();
        let mut floats = BTreeSet::new();
        while let Some(message) = stack.pop() {
            if !seen.insert(message.full_name().to_owned()) {
                continue;
            }
            for field in message.fields() {
                match field.kind() {
                    prost_reflect::Kind::Float | prost_reflect::Kind::Double => {
                        floats.insert(field.full_name().to_owned());
                    }
                    prost_reflect::Kind::Message(message) => stack.push(message),
                    _ => {}
                }
            }
        }
        assert_eq!(
            floats,
            BTreeSet::from(["livekit.EncodingOptions.key_frame_interval".to_owned()])
        );
    }
    #[test]
    fn signed_zero_quoted_and_alias_values_survive_real_mint_verify_refresh() {
        let keys = Arc::new(KeyStore::new(vec![(
            "f99-owned-api".into(),
            "f99-owned-secret".into(),
        )]));
        let minter = TokenMinter::new(keys.clone());
        for role in ["participant", "room"] {
            for field in ["keyFrameInterval", "key_frame_interval"] {
                for value in [json!(-0.0), json!("-0")] {
                    let input = json!({"egress":{(role):{"advanced":{(field):value}}}});
                    let pb = decode_room_config_proto(&input).unwrap();
                    let core = room_config_from_proto(&pb);
                    let claims = TokenClaims {
                        identity: "f99-actor".into(),
                        room_config: Some(core.clone()),
                        ..Default::default()
                    };
                    let signed = minter
                        .mint("f99-owned-api", &claims, Duration::from_secs(600), 1000)
                        .unwrap();
                    let verified = verify_token(&keys, &signed, 1001.0).unwrap();
                    assert_eq!(verified.grants.room_config, Some(core.clone()));
                    let refreshed = minter
                        .refresh_token_at(&verified.grants, None, 1002)
                        .unwrap();
                    assert_eq!(
                        verify_token(&keys, &refreshed, 1003.0)
                            .unwrap()
                            .grants
                            .room_config,
                        Some(core)
                    );
                    let projected =
                        room_config_to_proto(verified.grants.room_config.as_ref().unwrap());
                    let encoded = room_config_json(&projected).unwrap();
                    let number = encoded
                        .pointer(&format!("/egress/{role}/advanced/keyFrameInterval"))
                        .unwrap()
                        .as_f64()
                        .unwrap();
                    assert_eq!(number.to_bits(), (-0.0_f64).to_bits());
                }
            }
        }
    }
    #[test]
    #[ignore = "actual fixed Go37 comparison; F99_JWT_GO_RESULTS/F99_JWT_GO_CASES"]
    fn actual_go_room_config_mint_verify_refresh_thirty_seven() {
        let cases =
            std::fs::read_to_string(std::env::var("F99_JWT_GO_CASES").expect("actual input JSONL"))
                .unwrap();
        let rows = std::fs::read_to_string(
            std::env::var("F99_JWT_GO_RESULTS").expect("actual Go output JSONL"),
        )
        .unwrap();
        let cases: Vec<Value> = cases
            .lines()
            .map(|line| serde_json::from_str(line).unwrap())
            .collect();
        let rows: Vec<Value> = rows
            .lines()
            .map(|line| serde_json::from_str(line).unwrap())
            .collect();
        assert_eq!(cases.len(), 37);
        assert_eq!(rows.len(), 37);
        let keys = Arc::new(KeyStore::new(vec![(
            "f99-owned-api".into(),
            "f99-owned-secret".into(),
        )]));
        let minter = TokenMinter::new(keys.clone());
        for (input, row) in cases.into_iter().zip(rows) {
            let id = input["id"].as_str().unwrap();
            assert_eq!(row["id"], id);
            assert_eq!(row["decode_error"], "");
            let pb = decode_room_config_proto(&input["config"]).unwrap();
            let core = room_config_from_proto(&pb);
            assert_eq!(
                room_config_json(&pb).unwrap(),
                row["proto"],
                "{id}: actual default ProtoJSON"
            );
            let mut bits = serde_json::Map::new();
            if let Some(eg) = &pb.egress {
                let participant = eg.participant.as_ref().and_then(|v| match &v.options {
                    Some(pb::auto_participant_egress::Options::Advanced(v)) => Some(v),
                    _ => None,
                });
                let room = eg.room.as_ref().and_then(|v| match &v.options {
                    Some(pb::room_composite_egress_request::Options::Advanced(v)) => Some(v),
                    _ => None,
                });
                for (role, v) in [("participant", participant), ("room", room)] {
                    if let Some(v) = v {
                        bits.insert(
                            role.to_owned(),
                            json!(format!("{:016x}", v.key_frame_interval.to_bits())),
                        );
                    }
                }
            }
            assert_eq!(Value::Object(bits), row["bits"], "{id}: signed-zero bits");
            let check = if room_config_has_sensitive_credentials(&pb) {
                "room configuration should not contain sensitive credentials"
            } else {
                ""
            };
            assert_eq!(
                check,
                row["check_error"].as_str().unwrap(),
                "{id}: exact source selected credential types"
            );
            let claims = TokenClaims {
                identity: "f99-actor".into(),
                video: Some(VideoGrant {
                    room_join: true,
                    room: "f99-room".into(),
                    ..Default::default()
                }),
                room_config: Some(core.clone()),
                ..Default::default()
            };
            let minted = minter.mint("f99-owned-api", &claims, Duration::from_secs(600), 1000);
            assert_eq!(
                minted
                    .as_ref()
                    .err()
                    .map(ToString::to_string)
                    .unwrap_or_default(),
                row["mint_error"].as_str().unwrap(),
                "{id}: default mint"
            );
            let allowed = minter.mint_with_sensitive_credentials(
                "f99-owned-api",
                &claims,
                Duration::from_secs(600),
                1000,
                true,
            );
            assert_eq!(
                allowed
                    .as_ref()
                    .err()
                    .map(ToString::to_string)
                    .unwrap_or_default(),
                row["allow_error"].as_str().unwrap(),
                "{id}: explicit allow"
            );
            let verified = verify_token(&keys, &allowed.unwrap(), 1001.0);
            assert_eq!(
                verified
                    .as_ref()
                    .err()
                    .map(ToString::to_string)
                    .unwrap_or_default(),
                row["verify_error"].as_str().unwrap(),
                "{id}: inbound does not reject sensitive credentials"
            );
            let verified = verified.unwrap();
            assert_eq!(
                verified.grants.room_config,
                Some(core),
                "{id}: real token known config projection"
            );
            let refreshed = minter.refresh_token_at(&verified.grants, None, 1002);
            assert_eq!(
                refreshed
                    .as_ref()
                    .err()
                    .map(ToString::to_string)
                    .unwrap_or_default(),
                row["refresh_error"].as_str().unwrap(),
                "{id}: default refresh"
            );
            if let Ok(refreshed) = refreshed {
                let grants = verify_token(&keys, &refreshed, 1003.0).unwrap().grants;
                let proto = room_config_to_proto(grants.room_config.as_ref().unwrap());
                assert_eq!(
                    room_config_json(&proto).unwrap(),
                    row["refreshed"],
                    "{id}: actual refreshed known fields"
                );
            }
        }
    }
}

#[cfg(test)]
mod f99_existing_api_red_tests {
    use super::*;
    use crate::auth::{
        claims::{room_config_from_proto, room_config_to_proto},
        verify_token,
    };
    use lk_protocol::livekit as pb;
    fn minter_and_keys() -> (TokenMinter, Arc<KeyStore>) {
        let keys = Arc::new(KeyStore::new(vec![("f99-api".into(), "f99-secret".into())]));
        (TokenMinter::new(keys.clone()), keys)
    }
    #[test]
    fn f99_existing_api_signed_zero_must_survive_real_token_roundtrip() {
        let wire = pb::RoomConfiguration {
            egress: Some(pb::RoomEgress {
                participant: Some(pb::AutoParticipantEgress {
                    options: Some(pb::auto_participant_egress::Options::Advanced(
                        pb::EncodingOptions {
                            key_frame_interval: -0.0,
                            ..Default::default()
                        },
                    )),
                    ..Default::default()
                }),
                ..Default::default()
            }),
            ..Default::default()
        };
        let config = room_config_from_proto(&wire);
        assert!(
            room_config_to_proto(&config).egress.is_some(),
            "prerequisite: F96 complete recording DTO must be installed before this JWT baseline gate"
        );
        let (minter, keys) = minter_and_keys();
        let signed = minter
            .mint(
                "f99-api",
                &TokenClaims {
                    identity: "f99-actor".into(),
                    room_config: Some(config.clone()),
                    ..Default::default()
                },
                Duration::from_secs(600),
                1000,
            )
            .unwrap();
        assert_eq!(
            verify_token(&keys, &signed, 1001.0)
                .unwrap()
                .grants
                .room_config,
            Some(config)
        );
    }
    #[test]
    fn f99_existing_api_default_mint_rejects_source_sensitive_track_secret() {
        let wire = pb::RoomConfiguration {
            egress: Some(pb::RoomEgress {
                tracks: Some(pb::AutoTrackEgress {
                    output: Some(pb::auto_track_egress::Output::S3(pb::S3Upload {
                        secret: "f99-synthetic-sensitive".into(),
                        ..Default::default()
                    })),
                    ..Default::default()
                }),
                ..Default::default()
            }),
            ..Default::default()
        };
        let config = room_config_from_proto(&wire);
        assert!(
            room_config_to_proto(&config).egress.is_some(),
            "F96 DTO prerequisite"
        );
        let (minter, _) = minter_and_keys();
        let outcome = minter.mint(
            "f99-api",
            &TokenClaims {
                room_config: Some(config),
                ..Default::default()
            },
            Duration::from_secs(600),
            1000,
        );
        assert_eq!(
            outcome.unwrap_err().to_string(),
            "room configuration should not contain sensitive credentials"
        );
    }
    #[test]
    fn f99_existing_api_refresh_rejects_source_sensitive_track_secret() {
        let wire = pb::RoomConfiguration {
            egress: Some(pb::RoomEgress {
                tracks: Some(pb::AutoTrackEgress {
                    output: Some(pb::auto_track_egress::Output::S3(pb::S3Upload {
                        secret: "f99-synthetic-sensitive".into(),
                        ..Default::default()
                    })),
                    ..Default::default()
                }),
                ..Default::default()
            }),
            ..Default::default()
        };
        let config = room_config_from_proto(&wire);
        assert!(
            room_config_to_proto(&config).egress.is_some(),
            "F96 DTO prerequisite"
        );
        let (minter, _) = minter_and_keys();
        let grants = rtc_core::permission::Grants {
            identity: "f99-actor".into(),
            room_config: Some(config),
            expires_at: 1600,
            ..Default::default()
        };
        assert_eq!(
            minter
                .refresh_token_at(&grants, None, 1002)
                .unwrap_err()
                .to_string(),
            "room configuration should not contain sensitive credentials"
        );
    }
}
