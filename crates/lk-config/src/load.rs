//! The load pipeline (`getConfig` + `NewConfig` + `ValidateKeys` +
//! `LoadTURNSecrets`, impl-notes §4), in upstream order:
//!
//! 1. YAML text: non-empty `--config-body`/`LIVEKIT_CONFIG` wins, else `--config`
//!    file, else defaults only (no implicit `config.yaml`).
//! 2. YAML decode onto defaults (strict unless `--disable-strict-config`);
//!    `livecloud:` split off and validated separately.
//! 3. Generated flags (root command only).
//! 4. Base flags (§4.3 order, including the ignored `--node-ip`/`--udp-port` errors).
//! 5. CLI-C08 unsupported checks (server / list-nodes).
//! 6. `rtc.Validate`: port defaults, node-IP discovery.
//! 7. congestion-control pacer validation.
//! 8. TURN TTL clamp.
//! 9. `key_file` `$VAR` / `~` expansion.
//! 10. TURN relay range defaults.
//! 11. log level (`log_level`, dev → debug), `pion_level`.
//! 12. legacy room limits.
//! 13. dev mode placeholder keys / loopback bind (only when no keys).
//! 14. server only: `ValidateKeys`, `LoadTURNSecrets`, TURN checks.
//! 15. normalize into `ServerConfig`.

use std::ffi::OsString;
use std::io::Read;
use std::net::IpAddr;
use std::path::{Path, PathBuf};
use std::time::Duration;

use rtc_core::config::{Deployment, ExtensionConfig};

use crate::LoadedConfig;
use crate::cli::{BaseValues, Invocation};
use crate::env::Env;
use crate::error::ConfigError;
use crate::generated;
use crate::keys::{
    load_join_token_key_file, load_key_file, load_turn_secret_file, new_node_id, parse_keys_flag,
    path_error,
};
use crate::netdetect::{NetProbe, determine_ip};
use crate::normalize::{Extras, to_server_config};
use crate::official::{Config, KeyMap, LivecloudSection, decode_yaml};
use crate::unsupported;

/// Which command the configuration is loaded for.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Mode {
    Run,
    Ports,
    ListNodes,
    CreateJoinToken,
}

const TURN_MAX_TTL_SECONDS: i64 = 24 * 60 * 60;
const DEFAULT_TURN_TTL_SECONDS: i64 = 300;

/// Step 1 (`getConfigString`, `main.go:324-335`).
fn config_bytes(base: &BaseValues) -> Result<Vec<u8>, ConfigError> {
    if let Some(body) = base.os_values.get("config-body").filter(|v| !v.is_empty()) {
        return Ok(body.as_encoded_bytes().to_vec());
    }
    let body = base.config_body.as_deref().unwrap_or("");
    if !body.is_empty() {
        return Ok(body.as_bytes().to_vec());
    }
    let path = base.os_values.get("config").map(PathBuf::from).or_else(|| {
        base.config
            .as_ref()
            .filter(|v| !v.is_empty())
            .map(PathBuf::from)
    });
    let Some(path) = path else {
        return Ok(Vec::new());
    };
    let mut file = std::fs::File::open(&path).map_err(|e| path_error("open", &path, &e))?;
    let mut bytes = Vec::new();
    file.read_to_end(&mut bytes)
        .map_err(|e| path_error("read", &path, &e))?;
    Ok(bytes)
}

/// `livecloud:` extension (spec §6.3). Only `schema_version: 1` and
/// `deployment: standalone` are implemented; anything else names the field.
fn validate_extension(s: Option<LivecloudSection>) -> Result<Option<ExtensionConfig>, ConfigError> {
    let Some(s) = s else { return Ok(None) };
    let schema_version = s.schema_version.unwrap_or(1);
    if schema_version != 1 {
        return Err(ConfigError::unsupported(
            "livecloud.schema_version",
            format!(
                "schema_version {schema_version} is not supported, this build understands schema_version 1"
            ),
        ));
    }
    let deployment = match s.deployment.as_deref() {
        None | Some("standalone") => Deployment::Standalone,
        Some(d @ ("cluster" | "cloud")) => {
            return Err(ConfigError::unsupported(
                "livecloud.deployment",
                format!("deployment {d:?} is not implemented, only \"standalone\" is supported"),
            ));
        }
        Some(other) => {
            return Err(ConfigError::msg(format!(
                "invalid configuration \"livecloud.deployment\": unknown deployment {other:?}, must be one of \"standalone\", \"cluster\", \"cloud\""
            )));
        }
    };
    let ingress_worker_protocol = s
        .ingress_worker_protocol
        .unwrap_or_else(|| "psrpc-v0.8.2".into());
    if !matches!(
        ingress_worker_protocol.as_str(),
        "psrpc-v0.8.2" | "psrpc-v0.7.1"
    ) {
        return Err(ConfigError::msg(
            "invalid livecloud.ingress_worker_protocol: expected psrpc-v0.8.2 or psrpc-v0.7.1",
        ));
    }
    let https = s.https.map(|h| -> Result<_, ConfigError> {
        let port = h.port.unwrap_or(8443);
        let max_connections = h.max_connections.unwrap_or(1024);
        let timeout = h.handshake_timeout_ms.unwrap_or(10_000);
        if port == 0 || max_connections == 0 || max_connections > 65_536 || timeout == 0 || timeout > 300_000 {
            return Err(ConfigError::msg("livecloud.https requires nonzero port, max_connections in 1..=65536 and handshake_timeout_ms in 1..=300000"));
        }
        let cert = h.cert_file.filter(|s| !s.trim().is_empty()).ok_or_else(|| ConfigError::msg("livecloud.https.cert_file is required"))?;
        let key = h.key_file.filter(|s| !s.trim().is_empty()).ok_or_else(|| ConfigError::msg("livecloud.https.key_file is required"))?;
        let binds = h.bind_addresses.unwrap_or_default().into_iter().map(|s| s.parse::<IpAddr>()
            .map_err(|_| ConfigError::msg(format!("invalid livecloud.https.bind_addresses address {s:?}"))))
            .collect::<Result<Vec<_>,_>>()?;
        Ok(rtc_core::config::HttpsConfig { port, cert_file: cert.into(), key_file: key.into(), bind_addresses: binds,
            http_enabled: h.http_enabled.unwrap_or(true), max_connections, handshake_timeout: Duration::from_millis(timeout) })
    }).transpose()?;
    Ok(Some(ExtensionConfig {
        schema_version,
        deployment,
        https,
        ingress_worker_protocol,
    }))
}

/// Step 4 (`config.go:1104-1142`).
fn apply_base_flags(
    cfg: &mut Config,
    b: &BaseValues,
    warnings: &mut Vec<String>,
) -> Result<(), ConfigError> {
    if let Some(dev) = b.dev {
        cfg.development = dev;
    }
    if let Some(v) = &b.key_file {
        cfg.key_file = v.clone();
    }
    if b.os_values.contains_key("keys") {
        return Err(ConfigError::msg(crate::keys::KEYS_PARSE_ERROR));
    }
    if let Some(v) = &b.keys {
        cfg.keys = parse_keys_flag(v)?;
    }
    if let Some(v) = &b.region {
        cfg.region = v.clone();
    }
    if let Some(v) = &b.redis_host {
        cfg.redis.address = v.clone();
    }
    if let Some(v) = &b.redis_password {
        cfg.redis.password = v.clone();
    }
    if let Some(v) = &b.turn_cert {
        cfg.turn.cert_file = v.clone();
    }
    if let Some(v) = &b.turn_key {
        cfg.turn.key_file = v.clone();
    }
    // Upstream ignores both errors (and may partially apply); we mirror that
    // and surface the error as a warning instead of dropping it.
    if let Some(v) = &b.node_ip
        && let Err(e) = cfg.rtc.node_ip.unmarshal_string(v)
    {
        warnings.push(format!("ignoring --node-ip/NODE_IP value {v:?}: {e}"));
    }
    if let Some(v) = &b.udp_port
        && let Err(e) = cfg.rtc.udp_port.unmarshal_string(v)
    {
        warnings.push(format!("ignoring --udp-port/UDP_PORT value {v:?}: {e}"));
    }
    if let Some(v) = &b.bind {
        cfg.bind_addresses = Some(v.clone());
    }
    Ok(())
}

/// Step 6 (`RTCConfig.Validate`). Returns whether the node IP was discovered.
fn rtc_validate(cfg: &mut Config, probe: &dyn NetProbe) -> Result<bool, String> {
    let r = &mut cfg.rtc;
    if !r.udp_port.valid() && r.port_range_start == 0 {
        if cfg.development {
            r.udp_port = crate::ports::PortRange {
                start: 7882,
                end: 0,
            };
        } else {
            r.port_range_start = 50000;
            r.port_range_end = 60000;
        }
    }
    if r.node_ip.is_empty() || r.use_external_ip {
        let ip = determine_ip(probe, r.use_external_ip, r.require_ipv4, &r.stun_servers)?;
        let is_v4 = matches!(ip.parse::<IpAddr>(), Ok(IpAddr::V4(_)));
        if is_v4 {
            r.node_ip.v4 = ip;
        } else {
            r.node_ip.v6 = ip;
        }
        return Ok(true);
    }
    Ok(false)
}

/// Step 8 (`NormalizeTURNTTLs`, `config.go:904-941`).
fn normalize_turn_ttls(cfg: &mut Config, warnings: &mut Vec<String>) {
    let ttl = cfg.turn.ttl_seconds;
    let clamped = if ttl <= 0 {
        Some(DEFAULT_TURN_TTL_SECONDS)
    } else if ttl > TURN_MAX_TTL_SECONDS {
        Some(TURN_MAX_TTL_SECONDS)
    } else {
        None
    };
    if let Some(c) = clamped {
        warnings.push(format!(
            "turn.ttl_seconds out of range, using safe value (configured {ttl}, clamped {c}, max {TURN_MAX_TTL_SECONDS})"
        ));
        cfg.turn.ttl_seconds = c;
    }
    for s in &mut cfg.rtc.turn_servers {
        if s.ttl > TURN_MAX_TTL_SECONDS {
            warnings.push(format!(
                "turn_servers ttl out of range, using safe value (host {}, configured {}, clamped {TURN_MAX_TTL_SECONDS})",
                s.host, s.ttl
            ));
            s.ttl = TURN_MAX_TTL_SECONDS;
        }
    }
}

fn is_shell_special(c: u8) -> bool {
    matches!(
        c,
        b'*' | b'#' | b'$' | b'@' | b'!' | b'?' | b'-' | b'0'..=b'9'
    )
}

fn is_alnum(c: u8) -> bool {
    c == b'_' || c.is_ascii_alphanumeric()
}

/// Go `getShellName`: returns (name, consumed width).
fn shell_name(s: &str) -> (&str, usize) {
    let b = s.as_bytes();
    if b[0] == b'{' {
        if b.len() > 2 && is_shell_special(b[1]) && b[2] == b'}' {
            return (&s[1..2], 3);
        }
        for i in 1..b.len() {
            if b[i] == b'}' {
                if i == 1 {
                    return ("", 2); // bad syntax: ${}
                }
                return (&s[1..i], i + 1);
            }
        }
        return ("", 1); // bad syntax: unterminated
    }
    if is_shell_special(b[0]) {
        return (&s[0..1], 1);
    }
    let mut i = 0;
    while i < b.len() && is_alnum(b[i]) {
        i += 1;
    }
    (&s[..i], i)
}

/// Go `os.ExpandEnv` over the injected environment (unset → "").
pub(crate) fn expand_env(s: &str, env: &Env) -> String {
    let b = s.as_bytes();
    let mut out = String::with_capacity(s.len());
    let mut i = 0; // start of pending literal
    let mut j = 0;
    while j < b.len() {
        if b[j] == b'$' && j + 1 < b.len() {
            out.push_str(&s[i..j]);
            let (name, w) = shell_name(&s[j + 1..]);
            if name.is_empty() && w > 0 {
                // invalid syntax: eat the characters
            } else if name.is_empty() {
                out.push('$');
            } else {
                out.push_str(env.get(name).unwrap_or(""));
            }
            j += w;
            i = j + 1;
        }
        j += 1;
    }
    if i < b.len() {
        out.push_str(&s[i..]);
    }
    out
}

/// mitchellh/go-homedir `Expand`.
pub(crate) fn expand_home(path: &str, env: &Env) -> Result<String, ConfigError> {
    if !path.starts_with('~') {
        return Ok(path.to_string());
    }
    let b = path.as_bytes();
    if b.len() > 1 && b[1] != b'/' && b[1] != b'\\' {
        return Err(ConfigError::msg("cannot expand user-specific home dir"));
    }
    let home = env
        .get("HOME")
        .filter(|h| !h.is_empty())
        .or_else(|| env.get("USERPROFILE").filter(|h| !h.is_empty()))
        .ok_or_else(|| ConfigError::msg("blank output when reading home directory"))?;
    #[cfg(unix)]
    {
        // Go homedir.Expand joins and cleans even Unicode home paths. The
        // Unicode branch cannot contain raw bytes, so this conversion is exact.
        Ok(
            crate::cli::os_input::join_home_posix(
                std::ffi::OsStr::new(home),
                &path.as_bytes()[1..],
            )
            .into_os_string()
            .into_string()
            .expect("Unicode POSIX lexical join"),
        )
    }
    #[cfg(not(unix))]
    {
        let rest = path[1..].trim_start_matches(['/', '\\']);
        let joined = if rest.is_empty() {
            PathBuf::from(home)
        } else {
            Path::new(home).join(rest)
        };
        Ok(joined.to_string_lossy().into_owned())
    }
}

/// Step 13 (`main.go:208-243`).
fn apply_dev_mode(cfg: &mut Config, notices: &mut Vec<String>) {
    if !cfg.development {
        return;
    }
    notices.push("starting in development mode".to_string());
    if !cfg.keys.is_empty() {
        return;
    }
    notices.push(
        "no keys provided, using placeholder keys (API Key: devkey, API Secret: secret)"
            .to_string(),
    );
    cfg.keys = KeyMap(vec![("devkey".to_string(), "secret".to_string())]);
    match &cfg.bind_addresses {
        None => {
            cfg.bind_addresses = Some(vec!["127.0.0.1".to_string(), "::1".to_string()]);
        }
        Some(binds) => {
            let should_match = binds.iter().any(|a| match a.parse::<IpAddr>() {
                Ok(ip) => !ip.is_loopback() && !ip.is_unspecified(),
                Err(_) => false,
            });
            if should_match {
                let extra: Vec<String> = binds.iter().map(|a| format!("{a}/24")).collect();
                cfg.rtc.ips.includes.extend(extra);
            }
        }
    }
}

/// `LoadTURNSecrets` (`config.go:857-898`).
fn load_turn_secrets(
    cfg: &mut Config,
    warnings: &mut Vec<String>,
) -> Result<Vec<(usize, Vec<u8>)>, ConfigError> {
    let mut binary_secrets = Vec::new();
    for (index, s) in cfg.rtc.turn_servers.iter_mut().enumerate() {
        let inline = s.secret.trim().to_string();
        if !s.secret_file.is_empty() && !inline.is_empty() {
            warnings.push(format!(
                "both secret and secret_file are set for TURN server, the hardcoded secret will be used (host {}, port {})",
                s.host, s.port
            ));
            s.secret = inline;
        } else if !s.secret_file.is_empty() {
            let secret = load_turn_secret_file(&s.host, Path::new(&s.secret_file))?;
            match String::from_utf8(secret) {
                Ok(secret) => s.secret = secret,
                Err(error) => {
                    s.secret.clear();
                    binary_secrets.push((index, error.into_bytes()));
                    continue;
                }
            }
        } else {
            s.secret = inline;
        }
        let has_dynamic = !s.secret.is_empty();
        let has_static = !s.username.is_empty() && !s.credential.is_empty();
        if !has_dynamic && !has_static {
            return Err(ConfigError::msg(format!(
                "turn server \"{}\": turn server has no usable credentials: set a non-empty secret/secret_file for dynamic auth, or username and credential for static auth",
                s.host
            )));
        }
    }
    Ok(binary_secrets)
}

pub(crate) fn load(
    inv: &Invocation,
    env: &Env,
    mode: Mode,
    probe: &dyn NetProbe,
) -> Result<LoadedConfig, ConfigError> {
    let command = load_command(inv, env, mode, probe)?;
    let cpus = std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(1);
    let (mut server, more) = to_server_config(
        &command.official,
        Extras {
            node_ip_auto_generated: command.node_ip_auto_generated,
            node_id: command.node_id,
            extension: command.extension,
            cpus,
        },
    )?;
    // Normalization preserves TURN server order. The YAML view only holds text,
    // so file keys with invalid UTF-8 go directly into the runtime config.
    for (index, secret) in command.binary_turn_secrets {
        server.rtc.turn_servers[index].secret = Some(secret);
    }
    let mut warnings = command.warnings;
    warnings.extend(more);
    Ok(LoadedConfig {
        server,
        key_file: command.key_file,
        warnings,
        warning_extras: command.warning_extras,
        notices: command.notices,
        cpu_profile: command.cpu_profile,
        mem_profile: command.mem_profile,
        node_ip_auto_generated: command.node_ip_auto_generated,
        official: command.official,
    })
}

pub(crate) fn load_command(
    inv: &Invocation,
    env: &Env,
    mode: Mode,
    probe: &dyn NetProbe,
) -> Result<crate::CommandConfig, ConfigError> {
    let mut warnings = Vec::new();
    let mut warning_extras = Vec::new();
    let mut notices = Vec::new();
    let base = &inv.base;

    // 1-2
    let input = config_bytes(base)?;
    if let Some((name, _)) = base
        .os_values
        .iter()
        .find(|(name, _)| !matches!(**name, "config" | "key-file" | "config-body" | "keys"))
    {
        return Err(ConfigError::unsupported(
            *name,
            "non-Unicode text consumer remains OPEN; no lossy or ignored projection",
        ));
    }
    let strict = !base.disable_strict_config.unwrap_or(false);
    let mut cfg = Config::default();
    let mut livecloud = None;
    if !input.is_empty() {
        livecloud = decode_yaml(&input, strict, &mut cfg).map_err(|e| {
            if e.starts_with(crate::yamlv3::PANIC_PREFIX) {
                ConfigError::process_exit(e, 2)
            } else {
                ConfigError::Parse(e)
            }
        })?;
    }
    if !base.https_overrides.is_empty() {
        let h = livecloud
            .get_or_insert_with(Default::default)
            .https
            .get_or_insert_with(Default::default);
        for (key, value) in &base.https_overrides {
            let invalid =
                || ConfigError::msg(format!("invalid livecloud.https.{key} value {value:?}"));
            match key.as_str() {
                "port" => h.port = Some(value.parse().map_err(|_| invalid())?),
                "cert_file" => h.cert_file = Some(value.clone()),
                "key_file" => h.key_file = Some(value.clone()),
                "bind_addresses" => {
                    h.bind_addresses = Some(value.split(',').map(|s| s.trim().to_owned()).collect())
                }
                "http_enabled" => {
                    h.http_enabled = Some(crate::strconv::parse_bool(value).map_err(|_| invalid())?)
                }
                "max_connections" => {
                    h.max_connections = Some(value.parse().map_err(|_| invalid())?)
                }
                "handshake_timeout_ms" => {
                    h.handshake_timeout_ms = Some(value.parse().map_err(|_| invalid())?)
                }
                _ => unreachable!("extension parser validates names"),
            }
        }
    }
    let extension = validate_extension(livecloud)?;

    // 3-4
    if mode == Mode::Run {
        // Keep the still-unimplemented consumers explicit; accepting a raw
        // string must not silently retain its previous YAML/default value.
        // SPEC-0074 C30: `rtc.node_ip.ipv4`/`ipv6` are consumed (SPEC-0074
        // §2.2). The dotted generated flag writes the struct leaf directly
        // (`config.go:96-115`), bypassing `NodeIP.UnmarshalString`, and neither
        // `RTC.Validate` nor the ICE gatherer rejects a non-empty non-IP value:
        // `nat1To1AddressRewriteRules` (webrtc_config.go:257-286) passes the raw
        // text into `ICEAddressRewriteRule.External` without parsing, so an
        // invalid literal simply never matches (ignored) and startup proceeds.
        // We therefore apply the raw string and defer the decision to the
        // consumer (normalize) rather than failing at early `ParseIP`.
        if let Some((spec, generated::Value::OsStr(raw))) =
            inv.generated.iter().find(|(spec, value)| {
                !matches!(
                    spec.path.as_str(),
                    "key_file"
                        | "log_level"
                        | "logging.level"
                        | "logging.pion_level"
                        | "rtc.node_ip.ipv4"
                        | "rtc.node_ip.ipv6"
                ) && matches!(value, generated::Value::OsStr(_))
            })
        {
            return Err(ConfigError::unsupported(
                spec.path.clone(),
                format!(
                    "raw string consumer remains OPEN: {}",
                    crate::cli::quote::quote_bytes(raw.as_encoded_bytes())
                ),
            ));
        }
        generated::apply(&mut cfg, &inv.generated);
        // ParseZapLevel starts at Info and ignores UnmarshalText errors. A
        // non-Unicode value cannot equal any accepted ASCII level name, so
        // these three actual consumers have a defined Info fallback. Preserve
        // invocation raw bytes; do not lossy-convert other String fields.
        for (spec, value) in &inv.generated {
            if matches!(value, generated::Value::OsStr(_)) {
                match spec.path.as_str() {
                    "log_level" => cfg.log_level = "info".into(),
                    "logging.level" => cfg.logging.level = "info".into(),
                    "logging.pion_level" => cfg.logging.pion_level = "info".into(),
                    _ => {}
                }
            }
        }
    }
    apply_base_flags(&mut cfg, base, &mut warnings)?;

    // 5
    if mode == Mode::Run {
        unsupported::check(&cfg)?;
    } else if mode == Mode::ListNodes {
        unsupported::check_node_listing_transport(&cfg)?;
    }

    // 6-7
    let node_ip_auto_generated = rtc_validate(&mut cfg, probe)
        .map_err(|e| ConfigError::msg(format!("could not validate RTC config: {e}")))?;
    match cfg.rtc.congestion_control.send_side_bwe_pacer.as_str() {
        "" | "pass-through" | "no-queue" | "leaky-bucket" => {}
        other => {
            return Err(ConfigError::msg(format!(
                "could not validate congestion control config: invalid send_side_bwe_pacer {}, must be one of \"pass-through\", \"no-queue\", \"leaky-bucket\"",
                crate::strconv::go_quote(other)
            )));
        }
    }

    // 8-9
    normalize_turn_ttls(&mut cfg, &mut warnings);
    let raw_key_input = base.os_values.get("key-file").or_else(|| {
        // Source order: generated fields first, then explicit base --key-file.
        // An explicitly empty or Unicode base value also suppresses generated
        // raw bytes; no phantom fallback to an overridden generated file.
        if mode != Mode::Run || base.key_file.is_some() {
            return None;
        }
        inv.generated.iter().find_map(|(spec, value)| match value {
            generated::Value::OsStr(raw) if spec.path == "key_file" => Some(raw),
            _ => None,
        })
    });
    #[cfg(unix)]
    let expanded_os = crate::cli::os_input::expand_path(
        raw_key_input
            .map(OsString::as_os_str)
            .unwrap_or_else(|| std::ffi::OsStr::new(&cfg.key_file)),
        env,
    )?;
    #[cfg(unix)]
    let os_key_file = if expanded_os.as_os_str().is_empty() {
        None
    } else {
        // Lexical parent cleanup can remove the sole non-Unicode HOME component.
        // Keep the actual expanded result even when that result is now Unicode;
        // running String expansion again would lose the original raw dependency.
        Some(expanded_os)
    };
    #[cfg(not(unix))]
    let os_key_file = raw_key_input
        .map(|path| crate::cli::os_input::expand_path(path, env))
        .transpose()?;
    // ASCII ~/ or $VAR may depend on raw environment bytes. Never run the
    // String-only expansion first and lose those bytes or report HOME absent.
    if let Some(path) = &os_key_file {
        if let Some(text) = path.to_str() {
            cfg.key_file = text.to_owned();
        }
    } else {
        cfg.key_file = expand_home(&expand_env(&cfg.key_file, env), env)?;
    }

    // 10
    if cfg.turn.relay_range_start == 0 || cfg.turn.relay_range_end == 0 {
        cfg.turn.relay_range_start = 30000;
        cfg.turn.relay_range_end = if cfg.development { 30002 } else { 40000 };
    }

    // 11
    if !cfg.log_level.is_empty() {
        cfg.logging.level = cfg.log_level.clone();
    }
    if cfg.logging.level.is_empty() && cfg.development {
        cfg.logging.level = "debug".to_string();
    }
    if !cfg.logging.pion_level.is_empty() {
        let lvl = cfg.logging.pion_level.clone();
        cfg.logging
            .component_levels
            .insert("transport.pion".to_string(), lvl.clone());
        cfg.logging.component_levels.insert("pion".to_string(), lvl);
    }

    // 12
    if cfg.room.max_metadata_size != 0 {
        cfg.limit.max_metadata_size = cfg.room.max_metadata_size;
    }
    if cfg.room.max_participant_identity_length != 0 {
        cfg.limit.max_participant_identity_length = cfg.room.max_participant_identity_length;
    }
    if cfg.room.max_room_name_length != 0 {
        cfg.limit.max_room_name_length = cfg.room.max_room_name_length;
    }

    // 13
    apply_dev_mode(&mut cfg, &mut notices);

    // 14
    let mut binary_turn_secrets = Vec::new();
    match mode {
        Mode::Run => {
            if let Some(path) = os_key_file
                .as_deref()
                .or_else(|| (!cfg.key_file.is_empty()).then_some(Path::new(&cfg.key_file)))
            {
                cfg.keys = load_key_file(path)?;
            }
            if cfg.keys.is_empty() {
                return Err(ConfigError::msg("one of key-file or keys must be provided"));
            }
            if !cfg.development {
                for (k, s) in &cfg.keys.0 {
                    if s.len() < 32 {
                        // Upstream logs this through zap before returning the same
                        // text; keep the field structured for the C32 renderer.
                        warning_extras.push(crate::NoticeExtra {
                            message:
                                "secret is too short, should be at least 32 characters for security"
                                    .to_string(),
                            field: "apiKey",
                            value: k.clone(),
                        });
                    }
                }
            }
            binary_turn_secrets = load_turn_secrets(&mut cfg, &mut warnings)?;
        }
        Mode::CreateJoinToken => {
            if cfg.keys.is_empty() {
                let p = os_key_file.as_deref().unwrap_or(Path::new(&cfg.key_file));
                cfg.keys = load_join_token_key_file(p)?;
                if cfg.keys.is_empty() {
                    return Err(ConfigError::msg("keys are not configured"));
                }
            }
        }
        Mode::Ports | Mode::ListNodes => {}
    }

    let key_file = if os_key_file.is_some() {
        os_key_file
    } else if cfg.key_file.is_empty() {
        None
    } else {
        Some(PathBuf::from(&cfg.key_file))
    };
    Ok(crate::CommandConfig {
        node_id: new_node_id(),
        extension,
        binary_turn_secrets,
        key_file,
        warnings,
        warning_extras,
        notices,
        cpu_profile: base
            .cpuprofile
            .as_deref()
            .filter(|s| !s.is_empty())
            .map(PathBuf::from),
        mem_profile: base
            .memprofile
            .as_deref()
            .filter(|s| !s.is_empty())
            .map(PathBuf::from),
        node_ip_auto_generated,
        official: Box::new(cfg),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cli::Command;
    use crate::generated::{Kind, parse_value, spec};
    use crate::netdetect::tests::FakeProbe;
    use rtc_core::config::UdpMode;

    fn inv(base: BaseValues) -> Invocation {
        Invocation {
            command: Command::Run,
            base,
            generated: Vec::new(),
        }
    }

    fn probe() -> FakeProbe {
        FakeProbe::local("10.0.0.5")
    }

    #[test]
    fn config_body_beats_file() {
        let base = BaseValues {
            config_body: Some("port: 1234\nkeys:\n  k: s\n".into()),
            config: Some("/definitely/not/here.yaml".into()),
            ..Default::default()
        };
        let c = load(&inv(base), &Env::default(), Mode::Ports, &probe()).unwrap();
        assert_eq!(c.server.http.port, 1234);
    }

    #[test]
    fn missing_file_error_names_path() {
        let base = BaseValues {
            config: Some("/definitely/not/here.yaml".into()),
            ..Default::default()
        };
        let err = load(&inv(base), &Env::default(), Mode::Ports, &probe())
            .unwrap_err()
            .to_string();
        assert!(err.starts_with("open /definitely/not/here.yaml: "), "{err}");
    }

    #[test]
    fn empty_body_falls_back_to_file() {
        let base = BaseValues {
            config_body: Some(String::new()),
            config: Some("/definitely/not/here.yaml".into()),
            ..Default::default()
        };
        assert!(load(&inv(base), &Env::default(), Mode::Ports, &probe()).is_err());
    }

    #[test]
    fn comment_only_body_is_eof() {
        let base = BaseValues {
            config_body: Some("# hi\n".into()),
            ..Default::default()
        };
        let err = load(&inv(base), &Env::default(), Mode::Ports, &probe()).unwrap_err();
        assert_eq!(err.to_string(), "could not parse config: EOF");
    }

    #[test]
    fn strict_unknown_key() {
        let base = BaseValues {
            config_body: Some("nope: 1\n".into()),
            ..Default::default()
        };
        let err = load(&inv(base), &Env::default(), Mode::Ports, &probe())
            .unwrap_err()
            .to_string();
        assert!(err.starts_with("could not parse config: "), "{err}");
        assert!(
            err.contains("field nope not found in type config.Config"),
            "{err}"
        );
        let base = BaseValues {
            config_body: Some("nope: 1\n".into()),
            disable_strict_config: Some(true),
            ..Default::default()
        };
        assert!(load(&inv(base), &Env::default(), Mode::Ports, &probe()).is_ok());
    }

    #[test]
    fn dev_defaults() {
        let base = BaseValues {
            dev: Some(true),
            ..Default::default()
        };
        let c = load(&inv(base), &Env::default(), Mode::Run, &probe()).unwrap();
        assert!(c.server.development);
        assert_eq!(
            c.server.keys,
            vec![("devkey".to_string(), "secret".to_string())]
        );
        let binds = c.server.http.bind_addresses.as_ref().unwrap();
        assert_eq!(binds, &["127.0.0.1", "::1"]);
        assert_eq!(
            c.official.rtc.udp_port,
            crate::ports::PortRange {
                start: 7882,
                end: 0
            }
        );
        assert!(matches!(c.server.rtc.udp, UdpMode::Mux { ref ports } if ports == &vec![7882]));
        assert_eq!(c.server.turn.relay_range, (30000, 30002));
        assert_eq!(c.server.logging.level, "debug");
        assert_eq!(c.server.rtc.node_ip.v4, Some("10.0.0.5".parse().unwrap()));
        assert!(c.node_ip_auto_generated);
        assert!(!c.server.rtc.use_node_ip_mapping);
    }

    #[test]
    fn dev_with_keys_keeps_bind_and_prod_ports() {
        let base = BaseValues {
            keys: Some("k: s\n".into()),
            ..Default::default()
        };
        let c = load(&inv(base), &Env::default(), Mode::Run, &probe()).unwrap();
        assert!(!c.server.development);
        assert_eq!(
            c.server.rtc.udp,
            UdpMode::Range {
                start: 50000,
                end: 60000
            }
        );
        assert!(c.server.http.bind_addresses.is_none());
        // C32: secret-too-short is a structured zap ERROR (apiKey field), not a
        // plain console warning line.
        assert!(
            c.warning_extras
                .iter()
                .any(|w| w.message.contains("secret is too short") && w.field == "apiKey"),
            "{:?}",
            c.warning_extras
        );
    }

    #[test]
    fn dev_non_loopback_bind_adds_ip_filters() {
        let base = BaseValues {
            dev: Some(true),
            bind: Some(vec!["192.168.1.10".into(), "::1".into()]),
            ..Default::default()
        };
        let c = load(&inv(base), &Env::default(), Mode::Run, &probe()).unwrap();
        assert_eq!(
            c.official.rtc.ips.includes,
            vec!["192.168.1.10/24", "::1/24"]
        );
    }

    #[test]
    fn no_keys_in_prod_is_error() {
        let err = load(
            &inv(BaseValues::default()),
            &Env::default(),
            Mode::Run,
            &probe(),
        )
        .unwrap_err();
        assert_eq!(err.to_string(), "one of key-file or keys must be provided");
    }

    #[test]
    fn generated_flag_applies_only_to_root() {
        let s = spec("rtc.udp_port.start").unwrap();
        let mut i = inv(BaseValues {
            keys: Some("k: s".into()),
            ..Default::default()
        });
        i.generated = vec![(s, parse_value(Kind::Int, "9000").unwrap())];
        let c = load(&i, &Env::default(), Mode::Run, &probe()).unwrap();
        assert_eq!(c.official.rtc.udp_port.start, 9000);
        // ports subcommand ignores generated flags
        let c = load(&i, &Env::default(), Mode::Ports, &probe()).unwrap();
        assert_eq!(c.official.rtc.udp_port.start, 0);
    }

    #[test]
    fn base_udp_port_partial_and_ignored_errors() {
        let base = BaseValues {
            config_body: Some("rtc:\n  udp_port: 7882-7892\n".into()),
            udp_port: Some("9000".into()),
            node_ip: Some("garbage".into()),
            ..Default::default()
        };
        let c = load(&inv(base), &Env::default(), Mode::Ports, &probe()).unwrap();
        assert_eq!(
            c.official.rtc.udp_port,
            crate::ports::PortRange {
                start: 9000,
                end: 7892
            }
        );
        assert!(c.warnings.iter().any(|w| w.contains("--node-ip")));
    }

    #[test]
    fn configured_rpc_tuning_has_no_ignored_warning() {
        let base = BaseValues {
            config_body: Some(
                concat!(
                    "keys: {devkey: public-rpc-config-fixture-0123456789}\n",
                    "signal_relay: {retry_timeout: 4s}\n",
                    "psrpc: {max_attempts: 5}\n",
                )
                .into(),
            ),
            ..Default::default()
        };
        let config = load(&inv(base), &Env::default(), Mode::Run, &probe()).unwrap();
        assert_eq!(
            config.official.signal_relay.retry_timeout.nanos(),
            4_000_000_000
        );
        assert_eq!(config.official.psrpc.max_attempts, 5);
        assert!(
            !config
                .warnings
                .iter()
                .any(|warning| { warning.contains("signal_relay.") || warning.contains("psrpc.") })
        );
    }

    #[test]
    fn redis_host_flag_reaches_shared_catalog_configuration() {
        let base = BaseValues {
            keys: Some("k: s".into()),
            redis_host: Some("localhost:6379".into()),
            ..Default::default()
        };
        let loaded = load(&inv(base), &Env::default(), Mode::Run, &probe()).unwrap();
        assert_eq!(loaded.official.redis.address, "localhost:6379");
        assert_eq!(loaded.official.redis.db, 0);
        // Actual connection/store consumption is checked by the process-level
        // Redis SIP API differential, not inferred from parsing this field.
    }

    #[test]
    fn livecloud_deployment() {
        let base = BaseValues {
            config_body: Some("livecloud:\n  deployment: cluster\n".into()),
            ..Default::default()
        };
        let err = load(&inv(base), &Env::default(), Mode::Ports, &probe())
            .unwrap_err()
            .to_string();
        assert!(
            err.starts_with("unsupported configuration \"livecloud.deployment\""),
            "{err}"
        );
        let base = BaseValues {
            config_body: Some("livecloud:\n  schema_version: 1\n  deployment: standalone\n".into()),
            ..Default::default()
        };
        let c = load(&inv(base), &Env::default(), Mode::Ports, &probe()).unwrap();
        assert_eq!(
            c.server.extension.unwrap().deployment,
            Deployment::Standalone
        );
    }

    #[test]
    fn env_expansion() {
        let env = Env::from_pairs([("HOME", "/home/u"), ("D", "keys")]);
        assert_eq!(expand_env("$D/a-${D}.yaml", &env), "keys/a-keys.yaml");
        assert_eq!(expand_env("x$", &env), "x$");
        assert_eq!(expand_env("$UNSET/x", &env), "/x");
        assert_eq!(expand_home("~/k.yaml", &env).unwrap(), "/home/u/k.yaml");
        assert!(expand_home("~bob/k", &env).is_err());
    }

    #[test]
    fn pacer_validation() {
        let base = BaseValues {
            config_body: Some(
                "rtc:\n  congestion_control:\n    enabled: false\n    send_side_bwe_pacer: \"bad\\nvalue\"\n"
                    .into(),
            ),
            ..Default::default()
        };
        let err = load(&inv(base), &Env::default(), Mode::Ports, &probe())
            .unwrap_err()
            .to_string();
        assert_eq!(
            err,
            r#"could not validate congestion control config: invalid send_side_bwe_pacer "bad\nvalue", must be one of "pass-through", "no-queue", "leaky-bucket""#
        );
    }
    #[test]
    fn native_https_extension_normalizes_without_altering_official_http() {
        let b = BaseValues { dev: Some(true), config_body: Some("port: 7880\nlivecloud:\n  https:\n    port: 8443\n    cert_file: server.pem\n    key_file: server-key.pem\n    http_enabled: false\n    max_connections: 8\n    handshake_timeout_ms: 500\n".into()), ..Default::default() };
        let cfg = load(&inv(b), &Env::default(), Mode::Run, &probe()).unwrap();
        assert_eq!(cfg.server.http.port, 7880);
        let h = cfg.server.extension.unwrap().https.unwrap();
        assert_eq!(h.port, 8443);
        assert!(!h.http_enabled);
        assert_eq!(h.max_connections, 8);
        assert_eq!(h.handshake_timeout, Duration::from_millis(500));
    }
    #[test]
    fn native_https_rejects_missing_credentials_unknown_fields_and_unbounded_limits() {
        for body in [
            "livecloud: {https: {port: 8443}}",
            "livecloud: {https: {cert_file: c, key_file: k, typo: true}}",
            "livecloud: {https: {cert_file: c, key_file: k, max_connections: 99999999}}",
        ] {
            let b = BaseValues {
                dev: Some(true),
                config_body: Some(body.into()),
                ..Default::default()
            };
            assert!(load(&inv(b), &Env::default(), Mode::Run, &probe()).is_err());
        }
    }
    #[test]
    fn explicit_ingress_worker_wire_profile_is_preserved_and_unknown_rejected() {
        for protocol in ["psrpc-v0.8.2", "psrpc-v0.7.1"] {
            let b = BaseValues {
                dev: Some(true),
                config_body: Some(format!(
                    "livecloud: {{ingress_worker_protocol: {protocol}}}\n"
                )),
                ..Default::default()
            };
            let cfg = load(&inv(b), &Env::default(), Mode::Run, &probe()).unwrap();
            assert_eq!(
                cfg.server.extension.unwrap().ingress_worker_protocol,
                protocol
            );
        }
        let b = BaseValues {
            dev: Some(true),
            config_body: Some(
                "livecloud: {ingress_worker_protocol: silently-change-compression}".into(),
            ),
            ..Default::default()
        };
        assert!(load(&inv(b), &Env::default(), Mode::Run, &probe()).is_err());
    }
}

#[cfg(test)]
mod disabled_turn_consumer_tests {
    use super::*;
    fn parsed(yaml: &str) -> Result<crate::EntryAction, ConfigError> {
        let args = ["livecloud", "--dev", "--config-body", yaml]
            .into_iter()
            .map(Into::into)
            .collect();
        crate::parse_with_probe(
            args,
            &Env::default(),
            &crate::ProgramInfo {
                binary_name: "livecloud".into(),
                product_version: "test".into(),
                compat_version: "1.13.10",
            },
            &crate::netdetect::tests::FakeProbe::local("127.0.0.1"),
        )
    }
    #[test]
    fn public_run_disabled_turn_does_not_activate_bind_port_or_peer_policy_consumers() {
        for tail in [
            "  bind_addresses: [not-an-ip]\n",
            "  tls_port: 70000\n",
            "  udp_port: 70000\n",
            "  allow_restricted_peer_cidrs: [not-a-cidr]\n",
            "  deny_peer_cidrs: [not-a-cidr]\n",
            "  bind_addresses: [not-an-ip]\n  tls_port: 70000\n  udp_port: 70000\n  allow_restricted_peer_cidrs: [not-a-cidr]\n  deny_peer_cidrs: [not-a-cidr]\n",
        ] {
            let input = format!("rtc:\n  node_ip: 127.0.0.1\nturn:\n  enabled: false\n{tail}");
            let action =
                parsed(&input).expect("disabled TURN is not a main-server startup requirement");
            let crate::EntryAction::RunServer(loaded) = action else {
                panic!("actual Run consumer required")
            };
            assert!(!loaded.server.turn.enabled);
            assert_eq!(loaded.server.turn.tls_port, None);
            assert_eq!(loaded.server.turn.udp_port, None);
            assert!(loaded.server.turn.bind_addresses.is_empty());
            assert!(loaded.server.turn.allow_restricted_peer_cidrs.is_empty());
            assert!(loaded.server.turn.deny_peer_cidrs.is_empty());
            // Original official input isn't sanitized or discarded.
            if tail.contains("not-an-ip") {
                assert_eq!(loaded.official.turn.bind_addresses, ["not-an-ip"]);
            }
            if tail.contains("tls_port: 70000") {
                assert_eq!(loaded.official.turn.tls_port, 70000);
            }
            if tail.contains("udp_port: 70000") {
                assert_eq!(loaded.official.turn.udp_port, 70000);
            }
        }
    }
    #[test]
    fn enabled_turn_still_checks_actual_ports_and_peer_policies() {
        let invalid = "rtc:\n  node_ip: 127.0.0.1\nturn:\n  enabled: true\n  bind_addresses: [127.0.0.1]\n  udp_port: 3478\n  deny_peer_cidrs: [not-a-cidr]\n";
        let error = match parsed(invalid) {
            Err(e) => e.to_string(),
            Ok(_) => panic!("active peer policy cannot be accepted"),
        };
        assert!(error.contains("invalid CIDR"), "{error}");
    }

    // PROXY trusted CIDRs are compiled only when a TCP (TLS-port) listener
    // exists and proxy_protocol is set, matching upstream newTURNTCPListener.
    #[test]
    fn proxy_protocol_trusted_cidrs_validated_only_with_tcp_listener() {
        let base = "rtc:\n  node_ip: 127.0.0.1\nturn:\n  enabled: true\n  bind_addresses: [127.0.0.1]\n  tls_port: 3479\n  domain: turn.example.com\n";
        // Valid trusted list is consumed into the runtime config.
        let loaded = match parsed(&format!(
            "{base}  proxy_protocol: true\n  proxy_protocol_trusted_cidrs: [127.0.0.0/8]\n"
        )) {
            Ok(crate::EntryAction::RunServer(l)) => l,
            other => panic!("valid proxy config must load, got {other:?}"),
        };
        assert!(loaded.server.turn.proxy_protocol);
        assert_eq!(loaded.server.turn.proxy_trusted_cidrs.len(), 1);

        // An empty trusted list aborts startup with the upstream message.
        let empty = match parsed(&format!(
            "{base}  proxy_protocol: true\n  proxy_protocol_trusted_cidrs: []\n"
        )) {
            Err(e) => e.to_string(),
            Ok(_) => panic!("empty trusted list must be rejected"),
        };
        assert!(
            empty.contains("turn.proxy_protocol requires at least one entry in turn.proxy_protocol_trusted_cidrs"),
            "{empty}"
        );

        // A malformed CIDR uses the upstream field-scoped text.
        let bad = match parsed(&format!(
            "{base}  proxy_protocol: true\n  proxy_protocol_trusted_cidrs: [nope]\n"
        )) {
            Err(e) => e.to_string(),
            Ok(_) => panic!("malformed trusted CIDR must be rejected"),
        };
        assert!(
            bad.contains("invalid CIDR \"nope\" in turn.proxy_protocol_trusted_cidrs"),
            "{bad}"
        );

        // UDP-only listeners skip the TCP-listener validation (no tls_port).
        let udp_only = "rtc:\n  node_ip: 127.0.0.1\nturn:\n  enabled: true\n  bind_addresses: [127.0.0.1]\n  udp_port: 3478\n  proxy_protocol: true\n  proxy_protocol_trusted_cidrs: []\n";
        match parsed(udp_only) {
            Ok(crate::EntryAction::RunServer(l)) => {
                assert!(l.server.turn.proxy_protocol);
                assert!(l.server.turn.proxy_trusted_cidrs.is_empty());
            }
            Err(e) => panic!("proxy without a TCP listener is not validated, got {e}"),
            other => panic!("expected a run-server action, got {other:?}"),
        }
    }
}
