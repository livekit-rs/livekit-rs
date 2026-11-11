//! ICE servers, client configuration and server info for Join/Reconnect
//! responses (signaling.md §2.1, roommanager.go:1059-1154).

use base64::Engine as _;
use hmac::{Hmac, Mac};
use lk_protocol::livekit as pb;
use rtc_core::config::{IceServerProtocol, ServerConfig};
use rtc_core::ids::ParticipantSid;
use rtc_core::session::ClientMeta;

use crate::state::AppState;

/// Version reported on the wire. Clients gate behavior on the upstream server
/// version, so the compatibility target is advertised.
///
/// `debug_info` stays empty: the Go server never populates it on either the
/// signal or the agent-worker `ServerInfo` literal (verified against the
/// v1.13.10 oracle), so the field is always absent on the wire there.
pub const COMPAT_SERVER_VERSION: &str = "1.13.10";
pub const PROTOCOL_VERSION: i32 = lk_protocol::CURRENT_PROTOCOL;

const DEFAULT_STUN: [&str; 3] = [
    "stun:global.stun.twilio.com:3478",
    "stun:stun.l.google.com:19302",
    "stun:stun1.l.google.com:19302",
];

pub fn server_info(cfg: &ServerConfig) -> pb::ServerInfo {
    pb::ServerInfo {
        edition: pb::server_info::Edition::Standard as i32,
        version: COMPAT_SERVER_VERSION.to_string(),
        protocol: PROTOCOL_VERSION,
        region: cfg.region.clone(),
        node_id: cfg.node_id.clone(),
        debug_info: String::new(),
        agent_protocol: lk_protocol::CURRENT_AGENT_PROTOCOL,
    }
}

pub fn ice_servers(
    state: &AppState,
    api_key: &str,
    participant: &ParticipantSid,
) -> Vec<pb::IceServer> {
    ice_servers_for_preference(state, api_key, participant, false)
}

pub fn ice_servers_for_preference(
    state: &AppState,
    api_key: &str,
    participant: &ParticipantSid,
    mut tls_only: bool,
) -> Vec<pb::IceServer> {
    let cfg = &state.config;
    // Exact source: external TLS alone does not retain this embedded-UDP filter.
    if tls_only && cfg.turn.tls_port.is_none() {
        tls_only = false;
    }
    let mut out = Vec::new();
    let mut has_stun = false;

    // Embedded TURN.
    if cfg.turn.enabled {
        let mut urls = Vec::new();
        if let Some(port) = cfg.turn.udp_port.filter(|_| !tls_only) {
            has_stun = true;
            if let Some(ip) = cfg.rtc.node_ip.v4 {
                urls.push(format!("turn:{ip}:{port}?transport=udp"));
            }
            if let Some(ip) = cfg.rtc.node_ip.v6 {
                urls.push(format!("turn:[{ip}]:{port}?transport=udp"));
            }
        }
        if let Some(port) = cfg.turn.tls_port
            && !cfg.turn.domain.is_empty()
        {
            // Fixed RoomManager always advertises embedded TURN/TLS on 443,
            // independently of its internal listener or external termination.
            let advertised = 443;
            let _listener_port = port;
            urls.push(format!(
                "turns:{}:{advertised}?transport=tcp",
                cfg.turn.domain
            ));
        }
        if !urls.is_empty() {
            let (username, credential) =
                state
                    .turn_auth
                    .create_credentials(api_key, participant, cfg.turn.ttl);
            out.push(pb::IceServer {
                urls,
                username,
                credential,
            });
            // Fixed source marks embedded STUN only for its UDP listener.
        }
    }

    // Configured external TURN servers.
    for t in &cfg.rtc.turn_servers {
        let url = match t.protocol {
            IceServerProtocol::Udp => format!("turn:{}:{}?transport=udp", t.host, t.port),
            IceServerProtocol::Tcp => format!("turn:{}:{}?transport=tcp", t.host, t.port),
            IceServerProtocol::Tls => format!("turns:{}:{}?transport=tcp", t.host, t.port),
        };
        let (username, credential) = match &t.secret {
            Some(secret) => rest_credentials(secret, participant, t.ttl),
            None => (t.username.clone(), t.credential.clone()),
        };
        out.push(pb::IceServer {
            urls: vec![url],
            username,
            credential,
        });
        has_stun = true;
    }

    // The upstream list contains addresses and always prepends the scheme.
    if !cfg.rtc.stun_servers.is_empty() {
        let urls = cfg
            .rtc
            .stun_servers
            .iter()
            .map(|s| format!("stun:{s}"))
            .collect();
        out.push(pb::IceServer {
            urls,
            username: String::new(),
            credential: String::new(),
        });
        has_stun = true;
    }
    if !has_stun {
        out.push(pb::IceServer {
            urls: DEFAULT_STUN.iter().map(|s| s.to_string()).collect(),
            username: String::new(),
            credential: String::new(),
        });
    }
    out
}

/// TURN REST API credentials (draft-uberti-behave-turn-rest): username
/// `<expiry>:<id>`, credential base64(HMAC-SHA1(secret, username)).
fn rest_credentials(
    secret: &[u8],
    participant: &ParticipantSid,
    ttl: std::time::Duration,
) -> (String, String) {
    let ttl = if ttl.is_zero() {
        std::time::Duration::from_secs(14_400)
    } else {
        ttl
    };
    let expiry = rtc_core::util::unix_s() + ttl.as_secs() as i64;
    let username = format!("{expiry}:{participant}");
    let mut mac = <Hmac<sha1::Sha1> as Mac>::new_from_slice(secret).expect("hmac accepts any key");
    mac.update(username.as_bytes());
    let credential = base64::engine::general_purpose::STANDARD.encode(mac.finalize().into_bytes());
    (username, credential)
}

/// Exact fixed static configuration rules and merge order (conf.go), separate
/// from transport capability/profile browser matching.
pub fn client_configuration(
    client: &ClientMeta,
    force_relay: bool,
) -> Option<pb::ClientConfiguration> {
    let mut disabled = pb::DisabledCodecs::default();
    let codec = |mime: &str| pb::Codec {
        mime: mime.to_string(),
        fmtp_line: String::new(),
    };
    let safari = static_rule_eq(&client.browser, "safari");
    let firefox = static_rule_eq(&client.browser, "firefox")
        || static_rule_eq(&client.browser, "firefox mobile");
    if safari {
        disabled.codecs.push(codec("video/AV1"));
        if static_version_cmp(&client.browser_version, "18.3", ["18", "3", "0"]).is_gt() {
            disabled.publish.push(codec("video/VP9"));
        }
    }
    if firefox && static_version_cmp(&client.browser_version, "157", ["157", "0", "0"]).is_lt() {
        disabled.codecs.push(codec("video/AV1"));
    }
    if static_rule_eq(&client.os, "android")
        && (firefox || static_rule_eq(&client.device_model, "xiaomi 2201117ti"))
    {
        disabled.publish.push(codec("video/H264"));
    }
    let has_codec_rules = !disabled.codecs.is_empty() || !disabled.publish.is_empty();
    if !has_codec_rules && !force_relay {
        return None;
    }
    Some(pb::ClientConfiguration {
        video: None,
        screen: None,
        resume_connection: 0,
        disabled_codecs: has_codec_rules.then_some(disabled),
        force_relay: if force_relay {
            pb::ClientConfigSetting::Enabled as i32
        } else {
            pb::ClientConfigSetting::Unset as i32
        },
    })
}

// Fixed static rules compare Go strings.ToLower against ASCII literals. The
// only non-ASCII runes whose Go simple-lower result is ASCII are U+0130 and
// U+212A (Go1.26.3 unicode CaseRanges); full lowercase expansions are unwanted.
fn static_rule_eq(value: &str, expected: &str) -> bool {
    debug_assert!(expected.is_ascii());
    value
        .chars()
        .map(|c| match c {
            '\u{0130}' => 'i',
            '\u{212a}' => 'k',
            _ => c.to_ascii_lowercase(),
        })
        .eq(expected.chars())
}

// Source ruleSdkVersion.compare uses x/mod semver.IsValid("v"+value), falling
// back to original string comparison when invalid. Both static RHS versions
// are releases, so no arbitrary prerelease-to-prerelease ordering is needed.
fn static_version_cmp(value: &str, release: &str, release_parts: [&str; 3]) -> std::cmp::Ordering {
    fn valid_number(s: &str) -> bool {
        !s.is_empty()
            && s.bytes().all(|b| b.is_ascii_digit())
            && (s.len() == 1 || !s.starts_with('0'))
    }
    fn identifiers(s: &str, prerelease: bool) -> bool {
        s.split('.').all(|id| {
            !id.is_empty()
                && id.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'-')
                && !(prerelease
                    && id.len() > 1
                    && id.starts_with('0')
                    && id.bytes().all(|b| b.is_ascii_digit()))
        })
    }
    fn parse(value: &str) -> Option<([&str; 3], bool)> {
        let cut = value.find(['-', '+']).unwrap_or(value.len());
        let (core, suffix) = value.split_at(cut);
        let mut parts = ["0"; 3];
        let mut count = 0;
        for number in core.split('.') {
            if count == 3 || !valid_number(number) {
                return None;
            }
            parts[count] = number;
            count += 1;
        }
        if !suffix.is_empty() && count != 3 {
            return None;
        }
        let mut remaining = suffix;
        let prerelease = remaining.starts_with('-');
        if prerelease {
            let after = &remaining[1..];
            let end = after.find('+').unwrap_or(after.len());
            if !identifiers(&after[..end], true) {
                return None;
            }
            remaining = &after[end..];
        }
        if let Some(build) = remaining.strip_prefix('+') {
            if !identifiers(build, false) {
                return None;
            }
            remaining = "";
        }
        if !remaining.is_empty() {
            return None;
        }
        Some((parts, prerelease))
    }
    let Some((parts, prerelease)) = parse(value) else {
        return value.cmp(release);
    };
    for (a, b) in parts.into_iter().zip(release_parts) {
        let cmp = a.len().cmp(&b.len()).then_with(|| a.cmp(b));
        if !cmp.is_eq() {
            return cmp;
        }
    }
    if prerelease {
        std::cmp::Ordering::Less
    } else {
        std::cmp::Ordering::Equal
    }
}

#[cfg(test)]
#[path = "static_client_configuration_tests.rs"]
mod static_client_configuration_tests;

#[cfg(test)]
mod fallback_wire_tests {
    use super::*;
    use rtc_core::config::{ExternalTurnServer, IceServerProtocol};
    use std::sync::Arc;
    #[tokio::test]
    async fn tls_preference_filters_only_embedded_udp_and_preserves_source_stun_and_external_turn()
    {
        let (_, net) = crate::test_support::state().await;
        for (tls_port, tls_only) in [(Some(5349), false), (Some(5349), true), (None, true)] {
            let mut cfg = crate::test_support::config();
            cfg.turn.enabled = true;
            cfg.turn.domain = "turn.example.test".into();
            cfg.turn.udp_port = Some(3478);
            cfg.turn.tls_port = tls_port;
            cfg.rtc.stun_servers.clear();
            let state = crate::state::CompatServices::new(Arc::new(cfg.clone()))
                .assemble(net.clone(), Arc::new(rtc_core::events::NullSink));
            let participant = ParticipantSid::from("PA_fallback_wire");
            let urls = ice_servers_for_preference(&state, "devkey", &participant, tls_only)
                .into_iter()
                .flat_map(|server| server.urls)
                .collect::<Vec<_>>();
            let effective = tls_only && tls_port.is_some();
            assert_eq!(
                urls.iter().any(|url| url.contains("?transport=udp")),
                !effective
            );
            assert_eq!(
                urls.iter()
                    .any(|url| url == "turns:turn.example.test:443?transport=tcp"),
                tls_port.is_some()
            );
            assert_eq!(
                urls.iter().any(|url| url.starts_with("stun:")),
                effective,
                "embedded TLS alone does not replace STUN"
            );
            cfg.rtc.turn_servers.push(ExternalTurnServer {
                host: "external.example.test".into(),
                port: 3478,
                protocol: IceServerProtocol::Udp,
                username: "user".into(),
                credential: "credential".into(),
                secret: None,
                ttl: std::time::Duration::from_secs(300),
            });
            let state = crate::state::CompatServices::new(Arc::new(cfg))
                .assemble(net.clone(), Arc::new(rtc_core::events::NullSink));
            let urls = ice_servers_for_preference(&state, "devkey", &participant, tls_only)
                .into_iter()
                .flat_map(|server| server.urls)
                .collect::<Vec<_>>();
            assert!(
                urls.iter()
                    .any(|url| url == "turn:external.example.test:3478?transport=udp"),
                "external TURN is not filtered by TLS-only"
            );
            assert!(
                !urls.iter().any(|url| url.starts_with("stun:")),
                "actual external TURN satisfies source STUN flag"
            );
        }
        net.shutdown();
    }
}
