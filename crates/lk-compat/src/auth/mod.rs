//! API keys, JWT access tokens and the HTTP auth middleware semantics
//! (impl-notes/api-auth-webhook.md §1).

pub mod claims;
pub mod gojson;
mod hs256_provider;
pub mod jwt;
pub mod minter;
pub mod permissions;
mod room_config_wire;

use axum::http::{HeaderMap, StatusCode, header};
use axum::response::{IntoResponse, Response};
use rtc_core::config::ServerConfig;
use rtc_core::permission::{Grants, ParticipantPermission};

pub use claims::{ExtraGrants, SipGrant};
pub use minter::{MintError, TokenClaims, TokenMinter};

/// API key → secret, in configured order (the first pair signs refreshed tokens).
#[derive(Clone, Debug, Default)]
pub struct KeyStore {
    keys: Vec<(String, String)>,
}

impl KeyStore {
    pub fn new(keys: Vec<(String, String)>) -> Self {
        Self { keys }
    }

    pub fn from_config(config: &ServerConfig) -> Self {
        Self::new(config.keys.clone())
    }

    /// Secret for `api_key`; an empty secret means the key is unknown
    /// (`KeyProvider.GetSecret` returns "").
    pub fn secret(&self, api_key: &str) -> Option<&str> {
        self.keys
            .iter()
            .find(|(k, _)| k == api_key)
            .map(|(_, s)| s.as_str())
            .filter(|s| !s.is_empty())
    }

    /// First configured key pair with a non-empty secret.
    pub fn first(&self) -> Option<(&str, &str)> {
        self.keys
            .iter()
            .find(|(_, s)| !s.is_empty())
            .map(|(k, s)| (k.as_str(), s.as_str()))
    }

    /// Fixed endpoint front's unauthenticated tenant resolution: only one
    /// configured API key is unambiguous; first-key selection is not equivalent.
    pub fn single_api_key(&self) -> Option<&str> {
        (self.keys.len() == 1).then(|| self.keys[0].0.as_str())
    }

    pub fn is_empty(&self) -> bool {
        self.keys.is_empty()
    }
}

/// A verified token: engine grants plus the grant fields the engine does not model.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct VerifiedToken {
    pub grants: Grants,
    pub extra: ExtraGrants,
}

impl VerifiedToken {
    /// Participant permission from the video grant (`VideoGrant.ToPermission`).
    pub fn permission(&self) -> ParticipantPermission {
        claims::permission_from_grants(&self.grants.video, &self.extra)
    }
}

/// Request extension set by the HTTP auth middleware for every request on the
/// main port. `None` = no token presented (handlers reject as unauthenticated).
#[derive(Clone, Debug, Default)]
pub struct RequestAuth(pub Option<std::sync::Arc<VerifiedToken>>);

/// Middleware-level authentication failures: always 401 with a plain-text body
/// (`auth.go:63-118`, `HandleError`).
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
pub enum AuthHttpError {
    #[error("invalid authorization header. Must start with Bearer ")]
    InvalidHeader,
    #[error("invalid authorization token")]
    InvalidToken,
    #[error("invalid API key")]
    InvalidApiKey,
    #[error("invalid authorization token: {0}")]
    Verify(String),
}

impl AuthHttpError {
    pub fn status(&self) -> StatusCode {
        StatusCode::UNAUTHORIZED
    }
}

impl IntoResponse for AuthHttpError {
    fn into_response(self) -> Response {
        (
            self.status(),
            [(header::CONTENT_TYPE, "text/plain; charset=utf-8")],
            self.to_string(),
        )
            .into_response()
    }
}

const BEARER_PREFIX: &[u8] = b"Bearer ";

/// Source of the `access_token` query parameter: a raw query string
/// (`Option<&str>` / `str`, parsed with Go `url.ParseQuery` rules) or already
/// parsed parameters (`HashMap<String, String>`, e.g. from `axum::extract::Query`).
pub trait QueryParams {
    fn param(&self, name: &str) -> Option<String>;
}

impl QueryParams for str {
    fn param(&self, name: &str) -> Option<String> {
        query_param(self, name)
    }
}

impl QueryParams for Option<&str> {
    fn param(&self, name: &str) -> Option<String> {
        self.and_then(|q| query_param(q, name))
    }
}

impl QueryParams for std::collections::HashMap<String, String> {
    fn param(&self, name: &str) -> Option<String> {
        self.get(name).cloned()
    }
}

/// Token from `Authorization: Bearer <t>` (case-sensitive prefix, single space),
/// else from the `access_token` query parameter. An empty token means
/// unauthenticated (`Ok(None)`).
pub fn extract_token<Q: QueryParams + ?Sized>(
    headers: &HeaderMap,
    query: &Q,
) -> Result<Option<String>, AuthHttpError> {
    // `Header.Get`: first value; an empty value counts as absent.
    if let Some(value) = headers.get(header::AUTHORIZATION) {
        let bytes = value.as_bytes();
        if !bytes.is_empty() {
            let Some(rest) = bytes.strip_prefix(BEARER_PREFIX) else {
                return Err(AuthHttpError::InvalidHeader);
            };
            let token = String::from_utf8_lossy(rest).into_owned();
            return Ok((!token.is_empty()).then_some(token));
        }
    }
    let token = query.param("access_token").unwrap_or_default();
    Ok((!token.is_empty()).then_some(token))
}

/// Parse and verify `token` against `keys` at `now` (unix seconds).
pub fn verify_token(
    keys: &KeyStore,
    token: &str,
    now: f64,
) -> Result<VerifiedToken, AuthHttpError> {
    let parsed = jwt::parse_unverified(token).map_err(|_| AuthHttpError::InvalidToken)?;
    let secret = keys
        .secret(&parsed.api_key)
        .ok_or(AuthHttpError::InvalidApiKey)?;
    let (registered, decoded) = jwt::verify(&parsed, secret, now, |claims| {
        let raw = parsed.raw_claims_json()?;
        room_config_wire::validate(&raw)?;
        claims::decode_grants(claims)
    })
    .map_err(|e| AuthHttpError::Verify(e.0))?;
    let grants = Grants {
        api_key: parsed.api_key.clone(),
        // The `identity` claim is ignored: identity = sub || jti (verifier.go:90).
        identity: parsed.identity.clone(),
        name: decoded.name,
        metadata: decoded.metadata,
        attributes: decoded.attributes,
        video: decoded.video,
        kind: decoded.kind,
        sha256: decoded.sha256,
        room_preset: decoded.room_preset,
        room_config: decoded.room_config,
        expires_at: registered.exp.map(|e| e.floor() as i64).unwrap_or(0),
    };
    Ok(VerifiedToken {
        grants,
        extra: decoded.extra,
    })
}

fn now_unix_f64() -> f64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs_f64())
        .unwrap_or(0.0)
}

/// Full middleware semantics on an extracted token: `None` passes through
/// unauthenticated; otherwise the token must verify.
pub fn authenticate_token(
    keys: &KeyStore,
    token: Option<&str>,
) -> Result<Option<VerifiedToken>, AuthHttpError> {
    match token {
        None | Some("") => Ok(None),
        Some(t) => verify_token(keys, t, now_unix_f64()).map(Some),
    }
}

/// As [`authenticate_token`], returning only the engine grants.
pub fn authenticate(keys: &KeyStore, token: Option<&str>) -> Result<Option<Grants>, AuthHttpError> {
    Ok(authenticate_token(keys, token)?.map(|t| t.grants))
}

/// First value of `name` in a raw query string with Go `url.ParseQuery` rules:
/// pairs split on `&`, segments containing `;` are skipped, `+` decodes to a
/// space, pairs with invalid percent escapes are skipped.
pub fn query_param(raw: &str, name: &str) -> Option<String> {
    raw.split('&')
        .filter_map(query_pair)
        .find_map(|(key, value)| (key == name).then_some(value))
}

/// The first decoded value for each key, as returned by Go FormValue on GET.
pub(crate) fn first_query_values(raw: &str) -> std::collections::HashMap<String, String> {
    let mut values = std::collections::HashMap::new();
    for (key, value) in raw.split('&').filter_map(query_pair) {
        values.entry(key).or_insert(value);
    }
    values
}

fn query_pair(segment: &str) -> Option<(String, String)> {
    if segment.is_empty() || segment.contains(';') {
        return None;
    }
    let (key, value) = segment.split_once('=').unwrap_or((segment, ""));
    Some((query_unescape(key)?, query_unescape(value)?))
}

/// Go `url.QueryUnescape`.
pub fn query_unescape(s: &str) -> Option<String> {
    fn hex(b: u8) -> Option<u8> {
        match b {
            b'0'..=b'9' => Some(b - b'0'),
            b'a'..=b'f' => Some(b - b'a' + 10),
            b'A'..=b'F' => Some(b - b'A' + 10),
            _ => None,
        }
    }
    let bytes = s.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        match bytes[i] {
            b'%' => {
                let hi = hex(*bytes.get(i + 1)?)?;
                let lo = hex(*bytes.get(i + 2)?)?;
                out.push(hi << 4 | lo);
                i += 3;
            }
            b'+' => {
                out.push(b' ');
                i += 1;
            }
            b => {
                out.push(b);
                i += 1;
            }
        }
    }
    Some(String::from_utf8_lossy(&out).into_owned())
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::http::HeaderValue;
    use jwt::tests::make_token;

    const HS256: &str = r#"{"alg":"HS256","typ":"JWT"}"#;

    fn keys() -> KeyStore {
        KeyStore::new(vec![
            ("key".into(), "secret".into()),
            ("empty".into(), String::new()),
        ])
    }

    #[test]
    fn header_and_query_extraction() {
        let mut h = HeaderMap::new();
        assert_eq!(
            extract_token(&h, &Some("access_token=abc")).unwrap(),
            Some("abc".into())
        );
        assert_eq!(
            extract_token(&h, "x=1&access_token=a%2Bb+c").unwrap(),
            Some("a+b c".into())
        );
        assert_eq!(extract_token(&h, &None::<&str>).unwrap(), None);
        h.insert(
            header::AUTHORIZATION,
            HeaderValue::from_static("Bearer tok"),
        );
        assert_eq!(
            extract_token(&h, &Some("access_token=abc")).unwrap(),
            Some("tok".into())
        );
        h.insert(
            header::AUTHORIZATION,
            HeaderValue::from_static("bearer tok"),
        );
        assert_eq!(
            extract_token(&h, &None::<&str>),
            Err(AuthHttpError::InvalidHeader)
        );
        h.insert(header::AUTHORIZATION, HeaderValue::from_static("Bearer "));
        assert_eq!(extract_token(&h, &Some("access_token=abc")).unwrap(), None);
        let h = HeaderMap::new();
        let mut parsed = std::collections::HashMap::new();
        parsed.insert("access_token".to_string(), "q".to_string());
        assert_eq!(extract_token(&h, &parsed).unwrap(), Some("q".into()));
    }

    #[test]
    fn error_bodies() {
        assert_eq!(
            AuthHttpError::InvalidHeader.to_string(),
            "invalid authorization header. Must start with Bearer "
        );
        assert_eq!(AuthHttpError::InvalidApiKey.to_string(), "invalid API key");
        assert_eq!(
            AuthHttpError::Verify("token has invalid claims: token is expired".into()).to_string(),
            "invalid authorization token: token has invalid claims: token is expired"
        );
    }

    #[test]
    fn verify_paths() {
        let k = keys();
        assert_eq!(
            verify_token(&k, "garbage", 0.0),
            Err(AuthHttpError::InvalidToken)
        );
        let t = make_token(HS256, r#"{"iss":"nokey","exp":99999999999}"#, "secret");
        assert_eq!(verify_token(&k, &t, 0.0), Err(AuthHttpError::InvalidApiKey));
        let t = make_token(HS256, r#"{"iss":"empty","exp":99999999999}"#, "");
        assert_eq!(verify_token(&k, &t, 0.0), Err(AuthHttpError::InvalidApiKey));
        let t = make_token(
            HS256,
            r#"{"iss":"key","sub":"alice","identity":"mallory","exp":99999999999,"video":{"roomJoin":true,"room":"r"}}"#,
            "secret",
        );
        let v = verify_token(&k, &t, 0.0).unwrap();
        assert_eq!(v.grants.identity, "alice");
        assert!(v.grants.video.room_join);
        assert_eq!(v.grants.expires_at, 99999999999);
        let t = make_token(HS256, r#"{"iss":"key","exp":10}"#, "secret");
        assert_eq!(
            verify_token(&k, &t, 100.0),
            Err(AuthHttpError::Verify(
                "token has invalid claims: token is expired".into()
            ))
        );
    }

    #[test]
    fn first_query_values_keeps_empty_and_ignores_malformed_pairs() {
        let raw = concat!(
            "access_token=%zz&access%5Ftoken=first&access_token=second&",
            "join_request=&join_request=ignored&unused=1;bad=2&attributes=a%2Bb+c"
        );
        let values = first_query_values(raw);
        assert_eq!(
            values.get("access_token").map(String::as_str),
            Some("first")
        );
        assert_eq!(values.get("join_request").map(String::as_str), Some(""));
        assert_eq!(values.get("attributes").map(String::as_str), Some("a+b c"));
        assert!(!values.contains_key("unused"));
        assert_eq!(
            query_param(raw, "access_token"),
            values.get("access_token").cloned()
        );
    }

    #[test]
    fn query_rules() {
        assert_eq!(
            query_param("a=1;b=2&access_token=x", "access_token"),
            Some("x".into())
        );
        assert_eq!(
            query_param("access_token=%zz&access_token=y", "access_token"),
            Some("y".into())
        );
        assert_eq!(
            query_param("access_token", "access_token"),
            Some(String::new())
        );
    }
}

#[cfg(test)]
mod room_config_wire_tests {
    use super::jwt::tests::make_token;
    use super::{KeyStore, verify_token};
    use serde::Deserialize;
    use serde_json::value::RawValue;

    fn check(config: &str) -> bool {
        let payload = format!(
            r#"{{"iss":"f102-owned-api","sub":"f102-actor","exp":2000,"unknownTopLevel":{{"nested":1}},"roomConfig":{config}}}"#
        );
        let token = make_token(
            r#"{"alg":"HS256","typ":"JWT"}"#,
            &payload,
            "f102-owned-secret",
        );
        let keys = KeyStore::new(vec![("f102-owned-api".into(), "f102-owned-secret".into())]);
        verify_token(&keys, &token, 1000.0).is_ok()
    }

    #[test]
    fn signed_jwt_preserves_known_nested_duplicate_rejection() {
        for config in [
            r#"{"emptyTimeout":1,"emptyTimeout":2}"#,
            r#"{"emptyTimeout":1,"empty_timeout":2}"#,
            r#"{"agents":[],"agents":[]}"#,
            r#"{"egress":{"participant":{"advanced":{"keyFrameInterval":1,"keyFrameInterval":2}}}}"#,
        ] {
            assert!(
                !check(config),
                "signed nested known duplicate accepted: {config}"
            );
        }
    }

    #[test]
    fn signed_jwt_unknown_fields_remain_discarded() {
        for config in [
            r#"{"unknownField":1,"unknownField":2}"#,
            r#"{"egress":{"unknownField":1,"unknownField":2}}"#,
            r#"{"egress":{"participant":{"preset":"F102_FUTURE_ENUM"}}}"#,
            r#"{"egress":{"participant":{"preset":-7}}}"#,
        ] {
            assert!(
                check(config),
                "source forward-compatible JSON rejected: {config}"
            );
        }
    }

    #[derive(Deserialize)]
    struct Case {
        id: String,
        config: Box<RawValue>,
    }
    #[derive(Deserialize)]
    struct Observation {
        id: String,
        verified: bool,
        direct_error: String,
        verify_error: String,
    }

    #[test]
    #[ignore = "requires actual pinned Go13 source oracle results"]
    fn actual_go_thirteen_room_config_lexical_boundaries() {
        let cases =
            std::fs::read_to_string(std::env::var("F102_JWT_GO_CASES").expect("F102_JWT_GO_CASES"))
                .expect("read actual cases");
        let observations = std::fs::read_to_string(
            std::env::var("F102_JWT_GO_RESULTS").expect("F102_JWT_GO_RESULTS"),
        )
        .expect("read actual Go observations");
        let cases: Vec<Case> = cases
            .lines()
            .map(|line| serde_json::from_str(line).unwrap())
            .collect();
        let observations: Vec<Observation> = observations
            .lines()
            .map(|line| serde_json::from_str(line).unwrap())
            .collect();
        assert_eq!(cases.len(), 13);
        assert_eq!(observations.len(), 13);
        let mut accepted = 0;
        for (case, go) in cases.iter().zip(&observations) {
            assert_eq!(case.id, go.id);
            assert_eq!(
                go.verified,
                go.direct_error.is_empty(),
                "source direct/verify discrepancy: {}",
                case.id
            );
            assert_eq!(
                go.verified,
                go.verify_error.is_empty(),
                "source verify/error discrepancy: {}",
                case.id
            );
            assert_eq!(
                check(case.config.get()),
                go.verified,
                "actual signed JWT: {}",
                case.id
            );
            accepted += usize::from(go.verified);
        }
        assert_eq!(accepted, 7);
    }
}
