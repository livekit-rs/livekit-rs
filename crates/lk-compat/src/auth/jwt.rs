//! JWT parsing and verification with golang-jwt v5.3.1 semantics as configured by
//! `protocol/auth/verifier.go` (impl-notes/api-auth-webhook.md §1.2):
//!
//! 1. [`parse_unverified`] (`ParseAPIToken`): three segments, unpadded base64url,
//!    JSON header with a registered `alg`, registered claims decodable. Yields
//!    `apiKey = iss` and `identity = sub || jti`. Any failure is reported by the
//!    middleware as `invalid authorization token`.
//! 2. [`verify`] (`Verify(secret)`): full claims decode, `alg` must be HS256,
//!    HMAC-SHA256 signature over the raw signing input, then claim validation
//!    (`exp` required, 60 s leeway on `exp`/`nbf`, `iat` ignored). Error strings
//!    reproduce golang-jwt's wording.
//!
//! The signature is checked with `hmac` + `sha2` directly (constant-time
//! `verify_slice`) rather than `jsonwebtoken::decode`, because jsonwebtoken's
//! validation order and error texts differ from golang-jwt and the exact texts
//! are part of the HTTP contract.

use base64::Engine;
use base64::alphabet::URL_SAFE;
use base64::engine::{DecodePaddingMode, GeneralPurpose, GeneralPurposeConfig};
use hmac::{Hmac, Mac};
use serde_json::{Map, Value};
use sha2::Sha256;

use super::gojson::GoObject;

/// Go `base64.RawURLEncoding` (non-strict): no padding, trailing bits ignored.
const RAW_URL: GeneralPurpose = GeneralPurpose::new(
    &URL_SAFE,
    GeneralPurposeConfig::new()
        .with_encode_padding(false)
        .with_decode_allow_trailing_bits(true)
        .with_decode_padding_mode(DecodePaddingMode::RequireNone),
);

/// Signing methods registered in golang-jwt v5 (`GetSigningMethod`). Others make
/// `ParseUnverified` fail with "signing method (alg) is unavailable".
const REGISTERED_ALGS: &[&str] = &[
    "HS256", "HS384", "HS512", "RS256", "RS384", "RS512", "PS256", "PS384", "PS512", "ES256",
    "ES384", "ES512", "EdDSA", "none",
];

/// Token leeway (`tokenLeeway`, verifier.go:24).
pub const LEEWAY_SECS: f64 = 60.0;

/// Output of the unverified parse.
#[derive(Clone, Debug)]
pub struct ParsedToken {
    signing_input: String,
    signature: Vec<u8>,
    alg: String,
    claims: Map<String, Value>,
    pub api_key: String,
    pub identity: String,
}

impl ParsedToken {
    pub fn claims(&self) -> &Map<String, Value> {
        &self.claims
    }

    /// Original signed claim JSON, retained by signing_input. Decode only for
    /// validation of custom nested ProtoJSON, before a map can erase duplicates.
    pub(super) fn raw_claims_json(&self) -> Result<Vec<u8>, String> {
        let (_, encoded) = self
            .signing_input
            .split_once('.')
            .ok_or_else(|| "missing signed claims segment".to_owned())?;
        decode_segment(encoded)
    }
}

/// Registered claims after decode.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct RegisteredClaims {
    pub iss: String,
    pub sub: String,
    pub jti: String,
    pub aud: Vec<String>,
    pub exp: Option<f64>,
    pub nbf: Option<f64>,
    pub iat: Option<f64>,
}

/// The parse failed; upstream discards the detail.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ParseError(pub String);

fn decode_segment(seg: &str) -> Result<Vec<u8>, String> {
    RAW_URL.decode(seg).map_err(|e| e.to_string())
}

/// JSON body of a header/claims segment: an object, or `null` (Go decodes `null`
/// into a nil map / zero struct without error).
fn json_object(bytes: &[u8]) -> Result<Map<String, Value>, String> {
    match serde_json::from_slice::<Value>(bytes).map_err(|e| e.to_string())? {
        Value::Object(m) => Ok(m),
        Value::Null => Ok(Map::new()),
        other => Err(format!(
            "json: cannot unmarshal {} into Go value",
            kind(&other)
        )),
    }
}

fn kind(v: &Value) -> &'static str {
    match v {
        Value::Null => "null",
        Value::Bool(_) => "bool",
        Value::Number(_) => "number",
        Value::String(_) => "string",
        Value::Array(_) => "array",
        Value::Object(_) => "object",
    }
}

pub fn registered_claims(claims: &Map<String, Value>) -> Result<RegisteredClaims, String> {
    let o = GoObject::new(claims, "RegisteredClaims");
    Ok(RegisteredClaims {
        iss: o.string("iss").map_err(|e| e.0)?,
        sub: o.string("sub").map_err(|e| e.0)?,
        jti: o.string("jti").map_err(|e| e.0)?,
        aud: o.claim_strings("aud").map_err(|e| e.0)?,
        exp: o.numeric_date("exp").map_err(|e| e.0)?,
        nbf: o.numeric_date("nbf").map_err(|e| e.0)?,
        iat: o.numeric_date("iat").map_err(|e| e.0)?,
    })
}

/// `ParseAPIToken` (verifier.go:39-54).
pub fn parse_unverified(raw: &str) -> Result<ParsedToken, ParseError> {
    let mut it = raw.splitn(3, '.');
    let (Some(h), Some(c), Some(s)) = (it.next(), it.next(), it.next()) else {
        return Err(ParseError(
            "token is malformed: token contains an invalid number of segments".into(),
        ));
    };
    if s.contains('.') {
        return Err(ParseError(
            "token is malformed: token contains an invalid number of segments".into(),
        ));
    }
    let header = decode_segment(h)
        .and_then(|b| json_object(&b))
        .map_err(|e| ParseError(format!("token is malformed: could not decode header: {e}")))?;
    let claims = decode_segment(c)
        .and_then(|b| json_object(&b))
        .map_err(|e| ParseError(format!("token is malformed: could not decode claim: {e}")))?;
    let registered = registered_claims(&claims).map_err(|e| {
        ParseError(format!(
            "token is malformed: could not JSON decode claim: {e}"
        ))
    })?;
    // `token.Header["alg"].(string)`: exact key, Go map lookup.
    let alg = match header.get("alg") {
        Some(Value::String(a)) => a.clone(),
        _ => {
            return Err(ParseError(
                "token is unverifiable: signing method (alg) is unspecified".into(),
            ));
        }
    };
    if !REGISTERED_ALGS.contains(&alg.as_str()) {
        return Err(ParseError(
            "token is unverifiable: signing method (alg) is unavailable".into(),
        ));
    }
    let signature = decode_segment(s).map_err(|e| {
        ParseError(format!(
            "token is malformed: could not base64 decode signature: {e}"
        ))
    })?;

    let identity = if registered.sub.is_empty() {
        registered.jti.clone()
    } else {
        registered.sub.clone()
    };
    Ok(ParsedToken {
        signing_input: format!("{h}.{c}"),
        signature,
        alg,
        api_key: registered.iss,
        identity,
        claims,
    })
}

/// Verification failure; `Display` is the golang-jwt error text.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct VerifyError(pub String);

impl std::fmt::Display for VerifyError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

/// Signature and claim validation (`jwt.ParseWithClaims` with HS256 only,
/// `WithIssuer(iss)`, `WithLeeway(1m)`, `WithExpirationRequired()`).
///
/// `decode_grants` runs at the position where golang-jwt decodes the full claim
/// set (before the algorithm and signature checks) and returns the decoded value
/// or the decode error detail.
pub fn verify<T>(
    token: &ParsedToken,
    secret: &str,
    now_unix: f64,
    decode_grants: impl FnOnce(&Map<String, Value>) -> Result<T, String>,
) -> Result<(RegisteredClaims, T), VerifyError> {
    let malformed = |e: String| {
        VerifyError(format!(
            "token is malformed: could not JSON decode claim: {e}"
        ))
    };
    let registered = registered_claims(&token.claims).map_err(malformed)?;
    let grants = decode_grants(&token.claims).map_err(malformed)?;

    if token.alg != "HS256" {
        return Err(VerifyError(format!(
            "token signature is invalid: signing method {} is invalid",
            token.alg
        )));
    }
    if secret.is_empty() {
        return Err(VerifyError(
            "token is unverifiable: error while executing keyfunc: keys missing".into(),
        ));
    }
    let mut mac = <Hmac<Sha256> as Mac>::new_from_slice(secret.as_bytes())
        .map_err(|_| VerifyError("token signature is invalid: key is invalid".into()))?;
    mac.update(token.signing_input.as_bytes());
    if mac.verify_slice(&token.signature).is_err() {
        return Err(VerifyError(
            "token signature is invalid: signature is invalid".into(),
        ));
    }

    validate_claims(&registered, &token.api_key, now_unix)?;
    Ok((registered, grants))
}

/// `jwt.Validator.Validate`: errors are joined with ", " and wrapped in
/// "token has invalid claims".
pub fn validate_claims(
    c: &RegisteredClaims,
    expected_iss: &str,
    now: f64,
) -> Result<(), VerifyError> {
    let mut errs: Vec<String> = Vec::new();
    match c.exp {
        None => errs.push("token is missing required claim: exp claim is required".into()),
        Some(exp) => {
            if now.partial_cmp(&(exp + LEEWAY_SECS)) != Some(std::cmp::Ordering::Less) {
                errs.push("token is expired".into());
            }
        }
    }
    if let Some(nbf) = c.nbf
        && now < nbf - LEEWAY_SECS
    {
        errs.push("token is not valid yet".into());
    }
    // `WithIssuer(v.apiKey)`: only active when the expected issuer is non-empty.
    if !expected_iss.is_empty() {
        if c.iss.is_empty() {
            errs.push("token is missing required claim: iss claim is required".into());
        } else if c.iss != expected_iss {
            errs.push("token has invalid issuer".into());
        }
    }
    if errs.is_empty() {
        Ok(())
    } else {
        Err(VerifyError(format!(
            "token has invalid claims: {}",
            errs.join(", ")
        )))
    }
}

/// base64url (unpadded) encode, for tests and minting helpers.
pub fn encode_segment(bytes: &[u8]) -> String {
    RAW_URL.encode(bytes)
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;

    /// Build an HS256-style token with an arbitrary header and payload.
    pub(crate) fn make_token(header: &str, payload: &str, secret: &str) -> String {
        let h = encode_segment(header.as_bytes());
        let c = encode_segment(payload.as_bytes());
        let input = format!("{h}.{c}");
        let mut mac = <Hmac<Sha256> as Mac>::new_from_slice(secret.as_bytes()).unwrap();
        mac.update(input.as_bytes());
        let sig = mac.finalize().into_bytes();
        format!("{input}.{}", encode_segment(&sig))
    }

    const HS256: &str = r#"{"alg":"HS256","typ":"JWT"}"#;

    fn ok_grants(_: &Map<String, Value>) -> Result<(), String> {
        Ok(())
    }

    #[test]
    fn identity_falls_back_to_jti() {
        let t = make_token(HS256, r#"{"iss":"k","jti":"bob","exp":2000}"#, "s");
        let p = parse_unverified(&t).unwrap();
        assert_eq!(p.api_key, "k");
        assert_eq!(p.identity, "bob");
        let t = make_token(
            HS256,
            r#"{"iss":"k","sub":"alice","jti":"bob","exp":2000}"#,
            "s",
        );
        assert_eq!(parse_unverified(&t).unwrap().identity, "alice");
    }

    #[test]
    fn segments_and_alg() {
        assert!(parse_unverified("a.b").is_err());
        assert!(parse_unverified("a.b.c.d").is_err());
        let t = make_token(r#"{"alg":"XX"}"#, r#"{"iss":"k"}"#, "s");
        assert!(parse_unverified(&t).is_err());
        let t = make_token(r#"{"typ":"JWT"}"#, r#"{"iss":"k"}"#, "s");
        assert!(parse_unverified(&t).is_err());
        // exp of the wrong type fails the unverified parse
        let t = make_token(HS256, r#"{"iss":"k","exp":"10"}"#, "s");
        assert!(parse_unverified(&t).is_err());
    }

    #[test]
    fn verify_ok_and_leeway() {
        let t = make_token(
            HS256,
            r#"{"iss":"k","sub":"a","exp":1000,"nbf":900}"#,
            "secret",
        );
        let p = parse_unverified(&t).unwrap();
        assert!(verify(&p, "secret", 1000.0 + 59.0, ok_grants).is_ok());
        let e = verify(&p, "secret", 1000.0 + 60.0, ok_grants).unwrap_err();
        assert_eq!(e.0, "token has invalid claims: token is expired");
        assert!(verify(&p, "secret", 841.0, ok_grants).is_ok());
        let e = verify(&p, "secret", 839.0, ok_grants).unwrap_err();
        assert_eq!(e.0, "token has invalid claims: token is not valid yet");
    }

    #[test]
    fn verify_rejects() {
        let t = make_token(HS256, r#"{"iss":"k","sub":"a"}"#, "secret");
        let p = parse_unverified(&t).unwrap();
        assert_eq!(
            verify(&p, "secret", 0.0, ok_grants).unwrap_err().0,
            "token has invalid claims: token is missing required claim: exp claim is required"
        );
        let t = make_token(HS256, r#"{"iss":"k","exp":99999999999}"#, "secret");
        let p = parse_unverified(&t).unwrap();
        assert_eq!(
            verify(&p, "other", 0.0, ok_grants).unwrap_err().0,
            "token signature is invalid: signature is invalid"
        );
        let t = make_token(
            r#"{"alg":"HS512"}"#,
            r#"{"iss":"k","exp":99999999999}"#,
            "secret",
        );
        let p = parse_unverified(&t).unwrap();
        assert_eq!(
            verify(&p, "secret", 0.0, ok_grants).unwrap_err().0,
            "token signature is invalid: signing method HS512 is invalid"
        );
        let t = make_token(
            r#"{"alg":"none"}"#,
            r#"{"iss":"k","exp":99999999999}"#,
            "secret",
        );
        let p = parse_unverified(&t).unwrap();
        assert!(
            verify(&p, "secret", 0.0, ok_grants)
                .unwrap_err()
                .0
                .contains("signing method none")
        );
    }

    #[test]
    fn grant_decode_error_is_malformed() {
        let t = make_token(HS256, r#"{"iss":"k","exp":99999999999}"#, "secret");
        let p = parse_unverified(&t).unwrap();
        let e = verify(&p, "secret", 0.0, |_| Err::<(), _>("bad".to_string())).unwrap_err();
        assert_eq!(e.0, "token is malformed: could not JSON decode claim: bad");
    }

    #[test]
    fn case_insensitive_registered_claims() {
        let t = make_token(HS256, r#"{"ISS":"k","Sub":"a","EXP":99999999999}"#, "s");
        let p = parse_unverified(&t).unwrap();
        assert_eq!(p.api_key, "k");
        assert_eq!(p.identity, "a");
        assert!(verify(&p, "s", 0.0, ok_grants).is_ok());
    }
}
