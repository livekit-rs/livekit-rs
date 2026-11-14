//! Join request parsing and validation for `/rtc` (query) and `/rtc/v1`
//! (`join_request`), signaling.md §1.2–§1.4.

use std::collections::{BTreeMap, HashMap};
use std::io::Read;

use axum::http::{HeaderMap, StatusCode};
use base64::Engine as _;
use lk_protocol::livekit as pb;
#[cfg(test)]
use prost::Message as _;
use rtc_core::config::ServerConfig;
use rtc_core::ids::*;
use rtc_core::permission::Grants;
use rtc_core::session::{AddTrack, ClientMeta, JoinParams, SimulcastCodecRequest};

use crate::convert;

/// `http.DefaultMaxHeaderBytes`: limit for the decoded/decompressed join request.
const JOIN_REQUEST_MAX: usize = 1 << 20;

/// HTTP failure before the WebSocket upgrade: status + plain-text body.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HttpFailure {
    pub status: StatusCode,
    pub body: String,
}

impl HttpFailure {
    fn new(status: StatusCode, body: impl Into<String>) -> Self {
        Self {
            status,
            body: body.into(),
        }
    }
}

const NO_PERMISSION: &str = "no permissions to access the room";

/// Parsed join before the room and its owner have passed admission.
pub struct ParsedJoin {
    pub params: JoinParams,
    metadata_override: String,
    attribute_overrides: BTreeMap<String, String>,
}

impl ParsedJoin {
    /// The fixed server checks the room catalog and owner capacity before
    /// applying request metadata and attribute overrides.
    pub(super) fn finish(self) -> Result<JoinParams, HttpFailure> {
        let mut params = self.params;
        if (!self.metadata_override.is_empty() || !self.attribute_overrides.is_empty())
            && !params.grants.video.can_update_own_metadata.unwrap_or(false)
        {
            return Err(HttpFailure::new(StatusCode::UNAUTHORIZED, NO_PERMISSION));
        }
        if !self.metadata_override.is_empty() {
            params.metadata = self.metadata_override;
        }
        params.attributes.extend(
            self.attribute_overrides
                .into_iter()
                .filter(|(_, value)| !value.is_empty()),
        );
        Ok(params)
    }
}

fn bool_value(s: Option<&String>) -> bool {
    matches!(s.map(|s| s.as_str()), Some("1") | Some("true"))
}

fn opt_bool(s: Option<&String>) -> Option<bool> {
    s.map(|s| s == "1" || s == "true")
}

/// Build join params. `v1` requires `join_request`; its presence on either path
/// switches to single peer connection.
pub fn parse_join(
    cfg: &ServerConfig,
    grants: Option<Grants>,
    query: &HashMap<String, String>,
    headers: &HeaderMap,
    remote_addr: Option<std::net::SocketAddr>,
    v1: bool,
    strict: bool,
) -> Result<ParsedJoin, HttpFailure> {
    let Some(mut grants) = grants else {
        return Err(HttpFailure::new(StatusCode::UNAUTHORIZED, NO_PERMISSION));
    };

    let join_request = query.get("join_request").filter(|s| !s.is_empty());
    if v1 && join_request.is_none() {
        return Err(HttpFailure::new(
            StatusCode::BAD_REQUEST,
            "join_request is required",
        ));
    }

    let mut client = ClientMeta::default();
    let mut auto_subscribe = true;
    let auto_subscribe_data_track;
    let adaptive_stream;
    let disable_ice_lite;
    let subscriber_allow_pause;
    let reconnect;
    let reconnect_reason;
    let mut participant_sid = None;
    let mut metadata = String::new();
    let mut attributes: BTreeMap<String, String> = BTreeMap::new();
    let mut add_tracks = Vec::new();
    let mut publisher_offer = None;
    let single_pc = join_request.is_some();

    if let Some(raw) = join_request {
        let jr = decode_join_request(raw)?;
        let ci = jr.client_info.clone().unwrap_or_default();
        client = convert::client_info_to_meta(&ci);
        // v1 defaults: auto_subscribe false unless connection_settings says so.
        let cs = jr.connection_settings.unwrap_or_default();
        auto_subscribe = cs.auto_subscribe;
        adaptive_stream = cs.adaptive_stream;
        subscriber_allow_pause = cs.subscriber_allow_pause;
        disable_ice_lite = cs.disable_ice_lite;
        auto_subscribe_data_track = cs.auto_subscribe_data_track.unwrap_or(false);
        metadata = jr.metadata.clone();
        attributes = jr
            .participant_attributes
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();
        add_tracks = jr
            .add_track_requests
            .iter()
            .enumerate()
            .map(|(i, r)| add_track_from_proto(r, i as u32 + 1))
            .collect();
        publisher_offer = jr.publisher_offer.as_ref().map(|o| (o.sdp.clone(), o.id));
        reconnect = jr.reconnect;
        reconnect_reason = jr.reconnect_reason;
        if !jr.participant_sid.is_empty() {
            participant_sid = Some(ParticipantSid(jr.participant_sid.clone()));
        }
        // Upstream ignores JoinRequest.sync_state on this entry path.
    } else {
        if let Some(v) = query.get("auto_subscribe") {
            auto_subscribe = v == "1" || v == "true";
        }
        auto_subscribe_data_track = bool_value(query.get("auto_subscribe_data_track"));
        adaptive_stream = bool_value(query.get("adaptive_stream"));
        disable_ice_lite = bool_value(query.get("disable_ice_lite"));
        subscriber_allow_pause = opt_bool(query.get("subscriber_allow_pause"));
        reconnect = bool_value(query.get("reconnect"));
        reconnect_reason = query
            .get("reconnect_reason")
            .and_then(|s| s.parse().ok())
            .unwrap_or(0);
        if reconnect {
            participant_sid = query
                .get("sid")
                .filter(|s| !s.is_empty())
                .map(|s| ParticipantSid(s.clone()));
        }
        client.protocol = query
            .get("protocol")
            .and_then(|s| s.parse().ok())
            .unwrap_or(0);
        client.client_protocol = query
            .get("client_protocol")
            .and_then(|s| s.parse().ok())
            .unwrap_or(0);
        client.sdk = query
            .get("sdk")
            .map(|s| convert::sdk_from_query(s))
            .unwrap_or_default();
        let get = |k: &str| query.get(k).cloned().unwrap_or_default();
        client.version = get("version");
        client.os = get("os");
        client.os_version = get("os_version");
        client.browser = get("browser");
        client.browser_version = get("browser_version");
        client.device_model = get("device_model");
        client.network = get("network");
        client.capabilities = query
            .get("capabilities")
            .map(|c| convert::capabilities_from_query(c))
            .unwrap_or_default();
        client.packet_trailer = client
            .capabilities
            .contains(&(pb::client_info::Capability::CapPacketTrailer as i32));
        if let Some(enc) = query.get("attributes").filter(|s| !s.is_empty()) {
            match decode_attributes(enc) {
                Some(a) => attributes = a,
                None if strict => {
                    return Err(HttpFailure::new(
                        StatusCode::BAD_REQUEST,
                        "cannot decode attributes",
                    ));
                }
                None => {}
            }
        }
    }
    augment_client_info(&mut client, headers, remote_addr);

    if !grants.video.room_join {
        return Err(HttpFailure::new(
            StatusCode::UNAUTHORIZED,
            "permissions denied",
        ));
    }
    let limits = &cfg.limits;
    if grants.identity.is_empty() {
        return Err(HttpFailure::new(
            StatusCode::BAD_REQUEST,
            "identity cannot be empty",
        ));
    }
    let max = limits.max_participant_identity_length;
    if !rtc_core::config::signed_byte_length_allowed(max, grants.identity.len()) {
        return Err(HttpFailure::new(
            StatusCode::BAD_REQUEST,
            format!("participant identity length exceeds limits: max length {max}"),
        ));
    }
    let room_name = grants.video.room.clone();
    if room_name.is_empty() {
        return Err(HttpFailure::new(StatusCode::BAD_REQUEST, "no room name"));
    }
    let max = limits.max_room_name_length;
    if !rtc_core::config::signed_byte_length_allowed(max, room_name.len()) {
        return Err(HttpFailure::new(
            StatusCode::BAD_REQUEST,
            format!("room name length exceeds limits: max length {max}"),
        ));
    }

    // v0 `publish=<suffix>`: publish-only identity.
    let mut identity = grants.identity.clone();
    if !single_pc && let Some(suffix) = query.get("publish").filter(|s| !s.is_empty()) {
        if !grants.video.can_publish.unwrap_or(true) {
            return Err(HttpFailure::new(StatusCode::UNAUTHORIZED, NO_PERMISSION));
        }
        grants.video.can_subscribe = Some(false);
        identity = format!("{identity}#{suffix}");
    }

    let name = grants.name.clone();
    let params = JoinParams {
        room: RoomKey::new(ProjectId::local(), room_name),
        identity: Identity(identity),
        name,
        metadata: grants.metadata.clone(),
        attributes: grants.attributes.clone(),
        capabilities: crate::profile::capabilities(&client),
        client,
        auto_subscribe,
        auto_subscribe_data_track,
        adaptive_stream,
        single_pc,
        disable_ice_lite,
        subscriber_allow_pause,
        reconnect,
        reconnect_reason,
        participant_sid,
        add_tracks,
        publisher_offer,
        grants,
    };
    Ok(ParsedJoin {
        params,
        metadata_override: metadata,
        attribute_overrides: attributes,
    })
}

fn decode_join_request(raw: &str) -> Result<pb::JoinRequest, HttpFailure> {
    let bytes = base64::engine::general_purpose::URL_SAFE
        .decode(raw)
        .map_err(|_| {
            HttpFailure::new(
                StatusCode::BAD_REQUEST,
                "cannot base64 decode wrapped join request",
            )
        })?;
    let wrapped =
        lk_protocol::wire::decode::<pb::WrappedJoinRequest>(bytes.as_slice()).map_err(|_| {
            HttpFailure::new(
                StatusCode::BAD_REQUEST,
                "cannot unmarshal wrapped join request",
            )
        })?;
    let inner = if wrapped.compression == pb::wrapped_join_request::Compression::Gzip as i32 {
        let mut out = Vec::new();
        let mut reader = flate2::read::GzDecoder::new(wrapped.join_request.as_slice())
            .take(JOIN_REQUEST_MAX as u64 + 1);
        reader.read_to_end(&mut out).map_err(|_| {
            HttpFailure::new(
                StatusCode::BAD_REQUEST,
                "cannot read decompressed join request",
            )
        })?;
        if out.len() > JOIN_REQUEST_MAX {
            return Err(HttpFailure::new(
                StatusCode::BAD_REQUEST,
                "join request too large",
            ));
        }
        out
    } else {
        if wrapped.join_request.len() > JOIN_REQUEST_MAX {
            return Err(HttpFailure::new(
                StatusCode::BAD_REQUEST,
                "join request too large",
            ));
        }
        wrapped.join_request
    };
    lk_protocol::wire::decode::<pb::JoinRequest>(inner.as_slice())
        .map_err(|_| HttpFailure::new(StatusCode::BAD_REQUEST, "cannot unmarshal join request"))
}

/// `attributes` query param: base64URL(JSON object of strings).
fn decode_attributes(enc: &str) -> Option<BTreeMap<String, String>> {
    let bytes = base64::engine::general_purpose::URL_SAFE
        .decode(enc)
        .or_else(|_| base64::engine::general_purpose::URL_SAFE_NO_PAD.decode(enc))
        .ok()?;
    serde_json::from_slice(&bytes).ok()
}

/// `AugmentClientInfo`: client address from proxy headers, browser/OS from UA.
pub(crate) fn augment_client_info(
    client: &mut ClientMeta,
    headers: &HeaderMap,
    remote: Option<std::net::SocketAddr>,
) {
    let header = |name: &str| {
        headers
            .get(name)
            .and_then(|v| v.to_str().ok())
            .map(|s| s.trim().to_string())
    };
    client.address = header("CF-Connecting-IP")
        .or_else(|| {
            header("X-Forwarded-For").map(|s| s.split(',').next().unwrap_or("").trim().to_string())
        })
        .or_else(|| header("X-Real-IP"))
        .filter(|s| !s.is_empty())
        .or_else(|| remote.map(|r| r.ip().to_string()))
        .unwrap_or_default();

    use rtc_core::session::SdkKind;
    let web = matches!(
        client.sdk,
        SdkKind::Js | SdkKind::ReactNative | SdkKind::Flutter | SdkKind::Unity | SdkKind::Unknown
    );
    if web && let Some(ua) = header("User-Agent") {
        if client.browser.is_empty() {
            client.browser = browser_from_ua(&ua).to_string();
        }
        if client.os.is_empty() {
            client.os = os_from_ua(&ua).to_string();
        }
    }
}

fn browser_from_ua(ua: &str) -> &'static str {
    let l = ua.to_ascii_lowercase();
    if l.contains("firefox/") {
        "Firefox"
    } else if l.contains("edg/") {
        "Edge"
    } else if l.contains("chrome/") || l.contains("crios/") {
        "Chrome"
    } else if l.contains("safari/") {
        "Safari"
    } else {
        ""
    }
}

fn os_from_ua(ua: &str) -> &'static str {
    let l = ua.to_ascii_lowercase();
    if l.contains("android") {
        "Android"
    } else if l.contains("iphone") || l.contains("ipad") {
        "iOS"
    } else if l.contains("mac os x") {
        "macOS"
    } else if l.contains("windows") {
        "Windows"
    } else if l.contains("linux") {
        "Linux"
    } else {
        ""
    }
}

/// `AddTrackRequest` → engine `AddTrack`; `token` lets the session echo the
/// original request in a RequestResponse.
pub fn add_track_from_proto(r: &pb::AddTrackRequest, token: u32) -> AddTrack {
    AddTrack {
        token,
        cid: r.cid.clone(),
        name: r.name.clone(),
        kind: convert::track_kind_from_proto(r.r#type),
        width: r.width,
        height: r.height,
        muted: r.muted,
        disable_dtx: r.disable_dtx,
        source: convert::track_source_from_proto(r.source),
        layers: r
            .layers
            .iter()
            .map(convert::video_layer_from_proto)
            .collect(),
        simulcast_codecs: r
            .simulcast_codecs
            .iter()
            .map(|c| SimulcastCodecRequest {
                codec: c.codec.clone(),
                cid: c.cid.clone(),
                layers: c
                    .layers
                    .iter()
                    .map(convert::video_layer_from_proto)
                    .collect(),
                layer_mode: convert::layer_mode_from_proto(c.video_layer_mode),
            })
            .collect(),
        sid: (!r.sid.is_empty()).then(|| TrackSid(r.sid.clone())),
        stereo: r.stereo,
        disable_red: r.disable_red,
        encryption: convert::encryption_from_proto(r.encryption),
        packet_trailer_features: r.packet_trailer_features.clone(),
        stream: r.stream.clone(),
        backup_codec_policy: convert::backup_policy_from_proto(r.backup_codec_policy),
        audio_features: r
            .audio_features
            .iter()
            .map(|f| convert::audio_feature_from_proto(*f))
            .collect(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn join_request_round_trip_with_gzip() {
        use std::io::Write;
        let jr = pb::JoinRequest {
            metadata: String::new(),
            reconnect: true,
            ..Default::default()
        };
        let mut gz = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
        gz.write_all(&jr.encode_to_vec()).unwrap();
        let wrapped = pb::WrappedJoinRequest {
            compression: pb::wrapped_join_request::Compression::Gzip as i32,
            join_request: gz.finish().unwrap(),
        };
        let raw = base64::engine::general_purpose::URL_SAFE.encode(wrapped.encode_to_vec());
        let decoded = decode_join_request(&raw).unwrap();
        assert!(decoded.reconnect);
    }

    #[test]
    fn bad_base64_is_400() {
        let err = decode_join_request("@@@").unwrap_err();
        assert_eq!(err.status, StatusCode::BAD_REQUEST);
        assert_eq!(err.body, "cannot base64 decode wrapped join request");
    }

    #[test]
    fn ua_detection() {
        assert_eq!(
            browser_from_ua(
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Safari/605.1.15"
            ),
            "Safari"
        );
        assert_eq!(
            browser_from_ua(
                "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0 Safari/537.36"
            ),
            "Chrome"
        );
    }
}

#[cfg(test)]
mod capability_parse_tests {
    use super::*;
    use rtc_core::permission::VideoGrant;
    use rtc_core::session::SdkKind;

    fn grants() -> Grants {
        Grants {
            identity: "profile-user".into(),
            video: VideoGrant {
                room_join: true,
                room: "profile-room".into(),
                ..Default::default()
            },
            ..Default::default()
        }
    }

    #[test]
    fn legacy_and_v1_project_after_actual_user_agent_augmentation() {
        let cfg = crate::test_support::config();
        for (ua, expected_browser, ice_lite, red) in [
            ("Mozilla/5.0 Firefox/156.0", "Firefox", false, false),
            (
                "Mozilla/5.0 Version/18.0 Safari/605.1",
                "Safari",
                true,
                false,
            ),
            (
                "Mozilla/5.0 Chrome/156.0 Safari/537.36",
                "Chrome",
                true,
                true,
            ),
        ] {
            let mut headers = HeaderMap::new();
            headers.insert("User-Agent", ua.parse().unwrap());
            let query = HashMap::from([
                ("protocol".into(), "16".into()),
                ("sdk".into(), "js".into()),
            ]);
            let legacy =
                parse_join(&cfg, Some(grants()), &query, &headers, None, false, true).unwrap();
            let jr = pb::JoinRequest {
                client_info: Some(pb::ClientInfo {
                    sdk: pb::client_info::Sdk::Js as i32,
                    protocol: 16,
                    ..Default::default()
                }),
                ..Default::default()
            };
            let wrapped = pb::WrappedJoinRequest {
                compression: pb::wrapped_join_request::Compression::None as i32,
                join_request: jr.encode_to_vec(),
            };
            let query = HashMap::from([(
                "join_request".into(),
                base64::engine::general_purpose::URL_SAFE.encode(wrapped.encode_to_vec()),
            )]);
            let v1 = parse_join(&cfg, Some(grants()), &query, &headers, None, true, true).unwrap();
            assert_eq!(legacy.params.client.browser, expected_browser);
            assert_eq!(v1.params.client.browser, expected_browser);
            assert_eq!(legacy.params.client.sdk, SdkKind::Js);
            assert_eq!(legacy.params.capabilities, v1.params.capabilities);
            assert_eq!(v1.params.capabilities.ice_lite, ice_lite);
            assert_eq!(v1.params.capabilities.audio_red, red);
            assert_eq!(
                v1.params.capabilities.sync_stream_id,
                expected_browser != "Firefox"
            );
        }
    }

    #[test]
    fn actual_join_parser_preserves_unknown_protocol_zero_and_rust_request_rules() {
        let cfg = crate::test_support::config();
        for (sdk, protocol, request_response) in [
            ("unknown", "0", true),
            ("rust", "9", false),
            ("rust", "10", true),
        ] {
            let query = HashMap::from([
                ("sdk".into(), sdk.into()),
                ("protocol".into(), protocol.into()),
            ]);
            let parsed = parse_join(
                &cfg,
                Some(grants()),
                &query,
                &HeaderMap::new(),
                None,
                false,
                true,
            )
            .unwrap();
            assert_eq!(
                parsed.params.capabilities.request_response,
                request_response
            );
            assert_eq!(parsed.params.client.protocol.to_string(), protocol);
        }
    }
}

#[cfg(test)]
mod signed_identity_route_tests {
    use super::*;
    use axum::body::{Body, to_bytes};
    use axum::extract::ConnectInfo;
    use axum::http::Request;
    use rtc_core::events::NullSink;
    use rtc_runtime::net::{LocalInterface, NetConfig, NetRouter};
    use std::sync::Arc;
    use tower::ServiceExt;

    #[tokio::test]
    async fn real_validate_routes_preserve_signed_utf8_and_authentication_order() {
        for (limit, identity, expected_status, expected_body) in [
            (
                -1,
                "alice".to_string(),
                StatusCode::BAD_REQUEST,
                "participant identity length exceeds limits: max length -1",
            ),
            (
                i64::MIN,
                "alice".to_string(),
                StatusCode::BAD_REQUEST,
                "participant identity length exceeds limits: max length -9223372036854775808",
            ),
            (
                -1,
                String::new(),
                StatusCode::BAD_REQUEST,
                "identity cannot be empty",
            ),
            (0, "x".repeat(300), StatusCode::OK, "success"),
            (4, "éé".to_string(), StatusCode::OK, "success"),
            (
                4,
                "ééé".to_string(),
                StatusCode::BAD_REQUEST,
                "participant identity length exceeds limits: max length 4",
            ),
        ] {
            let mut config = crate::test_support::config();
            config.limits.max_participant_identity_length = limit;
            let key = config.keys[0].clone();
            let net = NetRouter::start(NetConfig {
                udp_enabled: false,
                udp_port: None,
                port_range: (0, 0),
                tcp_port: None,
                interfaces: vec![LocalInterface {
                    bind: std::net::Ipv4Addr::LOCALHOST.into(),
                    advertise: std::net::Ipv4Addr::LOCALHOST.into(),
                }],
            })
            .await
            .unwrap();
            let state = crate::state::CompatServices::new(Arc::new(config))
                .assemble(net.clone(), Arc::new(NullSink));
            let claims = serde_json::json!({"iss":key.0,"sub":identity,"exp":99999999999_u64,
                "video":{"roomJoin":true,"room":"signed-limit"}})
            .to_string();
            let token = crate::auth::jwt::tests::make_token(r#"{"alg":"HS256"}"#, &claims, &key.1);
            let join = pb::WrappedJoinRequest {
                compression: 0,
                join_request: pb::JoinRequest {
                    client_info: Some(pb::ClientInfo {
                        protocol: 16,
                        ..Default::default()
                    }),
                    ..Default::default()
                }
                .encode_to_vec(),
            };
            let encoded = base64::engine::general_purpose::URL_SAFE.encode(join.encode_to_vec());
            for route in [
                "/rtc/validate?protocol=16".to_string(),
                format!("/rtc/v1/validate?join_request={encoded}"),
            ] {
                let response = crate::http::router(state.clone())
                    .oneshot(
                        Request::get(&route)
                            .extension(ConnectInfo(
                                "127.0.0.1:50000".parse::<std::net::SocketAddr>().unwrap(),
                            ))
                            .header("authorization", format!("Bearer {token}"))
                            .body(Body::empty())
                            .unwrap(),
                    )
                    .await
                    .unwrap();
                assert_eq!(response.status(), expected_status, "{limit}, {route}");
                let body = to_bytes(response.into_body(), 16_384).await.unwrap();
                assert_eq!(std::str::from_utf8(&body).unwrap(), expected_body);
                // Failed preflight never creates a room, even with auto-create enabled.
                assert!(!state.manager().room_exists(&state.room_key("signed-limit")));
                let response = crate::http::router(state.clone())
                    .oneshot(
                        Request::get(&route)
                            .extension(ConnectInfo(
                                "127.0.0.1:50000".parse::<std::net::SocketAddr>().unwrap(),
                            ))
                            .body(Body::empty())
                            .unwrap(),
                    )
                    .await
                    .unwrap();
                assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
            }
            state.manager().close_all().await;
            net.shutdown();
        }
    }
}

#[cfg(test)]
mod binary_wire_regressions {
    use super::*;
    #[test]
    fn wrapped_and_nested_join_wrong_wire_is_unknown_not_http400() {
        let mut inner = pb::JoinRequest {
            metadata: "kept".into(),
            ..Default::default()
        }
        .encode_to_vec();
        inner.extend([0x18, 7]); // JoinRequest.metadata#3 wrong varint.
        let mut outer = pb::WrappedJoinRequest {
            join_request: inner,
            ..Default::default()
        }
        .encode_to_vec();
        outer.extend([0x10, 7]); // WrappedJoinRequest.join_request#2 wrong varint.
        assert!(pb::WrappedJoinRequest::decode(outer.as_slice()).is_err());
        let actual =
            decode_join_request(&base64::engine::general_purpose::URL_SAFE.encode(outer)).unwrap();
        assert_eq!(actual.metadata, "kept");
        for inner in [vec![0x1a, 1, 0xff], vec![0x1a, 2, b'x']] {
            let outer = pb::WrappedJoinRequest {
                join_request: inner,
                ..Default::default()
            }
            .encode_to_vec();
            let error =
                decode_join_request(&base64::engine::general_purpose::URL_SAFE.encode(outer))
                    .unwrap_err();
            assert_eq!(error.status, StatusCode::BAD_REQUEST);
            assert_eq!(error.body, "cannot unmarshal join request");
        }
    }
}

#[cfg(test)]
mod signed_name_order_tests {
    use super::*;
    #[test]
    fn rtc_connect_identity_precedes_room_and_display_name_is_not_new_join_limit() {
        let mut cfg = crate::test_support::config();
        cfg.limits.max_room_name_length = -1;
        cfg.limits.max_participant_name_length = -1;
        let mut grants = Grants::default();
        grants.video.room_join = true;
        grants.video.room = "a".into();
        let query = HashMap::new();
        let headers = HeaderMap::new();
        let error = parse_join(
            &cfg,
            Some(grants.clone()),
            &query,
            &headers,
            None,
            false,
            false,
        )
        .err()
        .unwrap();
        assert_eq!(error.body, "identity cannot be empty");
        grants.identity = "é".into();
        let error = parse_join(
            &cfg,
            Some(grants.clone()),
            &query,
            &headers,
            None,
            false,
            false,
        )
        .err()
        .unwrap();
        assert_eq!(error.body, "room name length exceeds limits: max length -1");
        cfg.limits.max_room_name_length = 0;
        grants.name = "display-name-must-not-be-validated-at-connect".into();
        assert!(parse_join(&cfg, Some(grants), &query, &headers, None, false, false).is_ok());
    }
}
