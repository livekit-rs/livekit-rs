//! SPEC-0074 C10: real Redis TLS transport for `redis.use_tls` / `redis.tls.*`.
//!
//! Ports `protocol/redis/redis.go` `GetRedisClient` TLS selection and
//! `protocol/xtls` `ClientTLSConfig`: `tls.enabled` builds the config from the
//! certificate paths (`insecure`, `ca_cert_file`, `client_cert_file`/
//! `client_key_file`); otherwise the deprecated `use_tls` boolean yields a
//! verified TLS connection using the system trust store. The resulting
//! [`redis::ConnectionInfo`] carries a `TcpTls` address consumed by every
//! downstream Redis client in the running server.

use lk_compat::redis_endpoint::{RedisEndpoint, SentinelTarget, TlsSniOverride};
use lk_config::official::{RedisConfig, TlsConfig};
use redis::{ClientTlsConfig, ConnectionAddr, ConnectionInfo, TlsCertificates};
use rtc_tls::rustls::pki_types::PrivateKeyDer;
use std::sync::Arc;

/// True when the configuration requests a TLS Redis connection: an enabled
/// `xtls.Config` wins, otherwise the deprecated `use_tls` flag.
pub(crate) fn tls_requested(cfg: &RedisConfig) -> bool {
    cfg.tls.as_ref().is_some_and(|tls| tls.enabled) || cfg.use_tls
}

/// Loaded TLS material reused across every Sentinel master re-resolution dial.
/// `GetRedisClient` applies one `TLSConfig` to the simple, Sentinel and master
/// connections alike, so the same certificates serve all three here.
#[derive(Clone)]
struct TlsMaterial {
    insecure: bool,
    certificates: TlsCertificates,
    /// `xtls.ServerName` (may be empty).
    server_name: String,
    /// Raw `ca_cert_file` PEM, retained for the SNI-override dial (`None` when
    /// the trust store is used, i.e. no CA file configured).
    root_pem: Option<Vec<u8>>,
    /// Raw mTLS client key pair PEMs, retained for the SNI-override dial.
    client_cert_pem: Option<Vec<u8>>,
    client_key_pem: Option<Vec<u8>>,
}

/// `xtls.ClientTLSConfig` certificate material: an enabled block supplies the
/// configured paths; a bare `use_tls` (or a disabled block) mirrors Go's
/// `&tls.Config{MinVersion:1.2}` (system trust roots, verified, no client cert).
/// The raw PEMs are retained alongside the redis-rs `TlsCertificates` so the
/// same loaded material can drive the SNI-override dial when `server_name` is set.
fn load_tls_material(cfg: &RedisConfig) -> Result<TlsMaterial, String> {
    let enabled = cfg.tls.as_ref().filter(|tls| tls.enabled);
    let (insecure, server_name, root_pem, client_cert_pem, client_key_pem) = match enabled {
        Some(tls) => {
            let (cert, key) = read_client_pair(tls)?;
            (
                tls.insecure,
                tls.server_name.clone(),
                read_ca(&tls.ca_cert_file)?,
                cert,
                key,
            )
        }
        None => (false, String::new(), None, None, None),
    };
    let client_tls = match (&client_cert_pem, &client_key_pem) {
        (Some(cert), Some(key)) => Some(ClientTlsConfig {
            client_cert: cert.clone(),
            client_key: key.clone(),
        }),
        _ => None,
    };
    Ok(TlsMaterial {
        insecure,
        certificates: TlsCertificates {
            client_tls,
            root_cert: root_pem.clone(),
        },
        server_name,
        root_pem,
        client_cert_pem,
        client_key_pem,
    })
}

/// SPEC-0074 C10: the verified `xtls.ServerName` override material, or `None`
/// when the configuration has no TLS server-name role: empty `server_name`, or
/// `insecure`, which skips server-name verification entirely (Go's
/// `InsecureSkipVerify`) so an overridden name is meaningless. The CA bundle is
/// optional: without `ca_cert_file` the dial uses the system trust anchors,
/// exactly like Go's empty `RootCAs`.
fn sni_override_material(material: &TlsMaterial) -> Option<TlsSniOverride> {
    if material.insecure || material.server_name.is_empty() {
        return None;
    }
    Some(TlsSniOverride {
        server_name: material.server_name.clone(),
        root_pem: material.root_pem.clone(),
        client_cert_pem: material.client_cert_pem.clone(),
        client_key_pem: material.client_key_pem.clone(),
    })
}

/// SPEC-0074 C10: build the SNI-override dial material for a static
/// `redis.address`. Returns `None` (leave the redis-rs path unchanged) when the
/// configuration needs no override, including the case where `server_name`
/// already equals the dial host (redis-rs' own address-host SNI honours it).
fn build_sni_override(
    address: &str,
    material: &TlsMaterial,
) -> Result<Option<TlsSniOverride>, String> {
    let Some(override_) = sni_override_material(material) else {
        return Ok(None);
    };
    let host = match redis::IntoConnectionInfo::into_connection_info(format!("redis://{address}/"))
        .map_err(|e| e.to_string())?
        .addr()
    {
        ConnectionAddr::Tcp(host, _) | ConnectionAddr::TcpTls { host, .. } => host.clone(),
        _ => return Ok(None),
    };
    if host == override_.server_name {
        return Ok(None);
    }
    Ok(Some(override_))
}

/// Build `GetRedisClient`'s [`ConnectionInfo`] for one address with the given
/// credentials/database, folding in TLS when `tls` is present. The address may
/// be a `host:port` resolved by a Sentinel.
fn make_info(
    address: &str,
    username: &str,
    password: &str,
    db: i64,
    tls: Option<&TlsMaterial>,
) -> Result<ConnectionInfo, String> {
    let base = redis::IntoConnectionInfo::into_connection_info(format!("redis://{address}/"))
        .map_err(|e| e.to_string())?;
    let mut credentials = base.redis_settings().clone().set_db(db);
    if !username.is_empty() {
        credentials = credentials.set_username(username);
    }
    if !password.is_empty() {
        credentials = credentials.set_password(password);
    }
    let Some(tls) = tls else {
        return Ok(base.set_redis_settings(credentials));
    };
    // redis-rs binds the TLS SNI / verification name to the address host. A
    // configured `xtls.ServerName` that differs from the host is applied at the
    // endpoint layer ([`RedisEndpoint::simple_with_tls_sni`]), not here.
    let url = format!(
        "rediss://{address}{}",
        if tls.insecure { "#insecure" } else { "" }
    );
    let client =
        redis::Client::build_with_tls(url, tls.certificates.clone()).map_err(|e| e.to_string())?;
    Ok(client
        .get_connection_info()
        .clone()
        .set_redis_settings(credentials))
}

/// Builds `GetRedisClient`'s simple-client [`ConnectionInfo`], honouring TLS and
/// folding in `db`/`username`/`password`. Certificate load failures surface a
/// Go-style message (matching `xtls.ClientTLSConfig`). Production dials go
/// through [`redis_endpoint`] (which also carries the SNI override); this
/// connection-info view is used by the unit tests to assert the address shape.
#[cfg(test)]
pub(crate) fn connection_info(cfg: &RedisConfig) -> Result<ConnectionInfo, String> {
    let tls = if tls_requested(cfg) {
        Some(load_tls_material(cfg)?)
    } else {
        None
    };
    make_info(
        &cfg.address,
        &cfg.username,
        &cfg.password,
        cfg.db,
        tls.as_ref(),
    )
}

/// `GetRedisClient`'s mode log (`redis.go:101` Sentinel / `:132` Cluster / `:145`
/// simple). The branch is the one upstream takes — a non-empty `sentinel_addresses`
/// logs `"sentinel":true` with `masterName` even when that name is empty and the
/// client built from the same branch is therefore simple or cluster
/// (`UniversalEmptyMasterSelection`) — so the payload identifies the branch, and the
/// endpoint kind underneath it is a separate, later decision.
pub(crate) fn log_redis_mode(cfg: &RedisConfig) {
    let array = |values: &[String]| {
        crate::zap::ZapValue::Array(
            values
                .iter()
                .map(|value| crate::zap::ZapValue::Str(value.clone()))
                .collect(),
        )
    };
    if !cfg.sentinel_addresses.is_empty() {
        crate::logging::notify(
            crate::zap::INFO,
            "livekit",
            "protocol/redis/redis.go:101",
            "connecting to redis",
            &[
                ("sentinel", crate::zap::ZapValue::Bool(true)),
                ("addr", array(&cfg.sentinel_addresses)),
                (
                    "masterName",
                    crate::zap::ZapValue::Str(cfg.sentinel_master_name.clone()),
                ),
            ],
            None,
        );
    } else if !cfg.cluster_addresses.is_empty() {
        crate::logging::notify(
            crate::zap::INFO,
            "livekit",
            "protocol/redis/redis.go:132",
            "connecting to redis",
            &[
                ("cluster", crate::zap::ZapValue::Bool(true)),
                ("addr", array(&cfg.cluster_addresses)),
            ],
            None,
        );
    } else {
        crate::logging::notify(
            crate::zap::INFO,
            "livekit",
            "protocol/redis/redis.go:145",
            "connecting to redis",
            &[
                ("simple", crate::zap::ZapValue::Bool(true)),
                ("addr", crate::zap::ZapValue::Str(cfg.address.clone())),
            ],
            None,
        );
    }
}

/// SPEC-0074 C11: the single Redis connection source for every server-side
/// consumer. Upstream `GetRedisClient` order is Sentinel > Cluster > Address
/// (`references/protocol-stable/redis/redis.go:100-155`), so a non-empty
/// `sentinel_addresses` always wins over `address` here.
///
/// The mode log lives here because `GetRedisClient` logs it here: after the TLS
/// material is built (`redis.go:88-98`, `xtls.ClientTLSConfig` loads the client
/// pair before the CA, so an unloadable pair never reaches the log) and before the
/// client is constructed and Pinged. Every consumer — server start, `list-nodes`,
/// the SIP catalog — therefore prints the same line the reference prints, once per
/// client it builds.
pub(crate) fn redis_endpoint(cfg: &RedisConfig) -> Result<RedisEndpoint, String> {
    let tls = if tls_requested(cfg) {
        Some(Arc::new(load_tls_material(cfg)?))
    } else {
        None
    };
    log_redis_mode(cfg);
    // SPEC-0074 C12 + UniversalEmptyMasterSelection: Go's `NewUniversalClient`
    // selects Sentinel mode only when MasterName is non-empty. With empty
    // sentinel_master_name, sentinel_addresses become normal node addresses:
    // 1 address → simple, >1 → cluster. Upstream precedence:
    // Sentinel(non-empty name) > Cluster > Address.
    if !cfg.sentinel_addresses.is_empty() && !cfg.sentinel_master_name.is_empty() {
        // `GetRedisClient`'s Sentinel branch defaults dial/read to 2s/200ms
        // before building the failover client; discovery dials use this budget.
        let (discovery_dial_timeout, discovery_read_timeout) =
            lk_compat::redis_endpoint::sentinel_timeouts(cfg.dial_timeout, cfg.read_timeout);
        let sentinel_username = cfg.sentinel_username.clone();
        let sentinel_password = cfg.sentinel_password.clone();
        let sentinel_tls = tls.clone();
        // Sentinels authenticate with the Sentinel-only credentials on db 0
        // (`redis.Options.SentinelUsername`/`SentinelPassword`).
        let sentinel_info = Arc::new(move |address: &str| -> Result<ConnectionInfo, String> {
            make_info(
                address,
                &sentinel_username,
                &sentinel_password,
                0,
                sentinel_tls.as_deref(),
            )
        });
        let master_username = cfg.username.clone();
        let master_password = cfg.password.clone();
        let master_db = cfg.db;
        let master_tls = tls.clone();
        // The resolved master uses the ordinary Redis credentials/database.
        let master_info = Arc::new(move |host_port: String| -> Result<ConnectionInfo, String> {
            make_info(
                &host_port,
                &master_username,
                &master_password,
                master_db,
                master_tls.as_deref(),
            )
        });
        let target = SentinelTarget {
            master_name: cfg.sentinel_master_name.clone(),
            addresses: cfg.sentinel_addresses.clone(),
            sentinel_username: cfg.sentinel_username.clone(),
            sentinel_password: cfg.sentinel_password.clone(),
            discovery_dial_timeout,
            discovery_read_timeout,
            write_timeout: lk_compat::redis_endpoint::sentinel_write_timeout(cfg.write_timeout),
            sentinel_info,
            master_info,
        };
        // Upstream applies one `TLSConfig` (including `ServerName`) to the
        // Sentinel connections and the resolved master alike, so the override
        // wraps the whole discovery + master dial chain.
        let override_ = tls.as_deref().and_then(sni_override_material);
        return Ok(match override_ {
            Some(override_) => RedisEndpoint::sentinel_with_tls_sni(target, override_),
            None => RedisEndpoint::sentinel(target),
        });
    }
    // SPEC-0074 C12: upstream order Sentinel > Cluster > Address.
    // Cluster uses the same credentials/TLS as simple, but multiple seed nodes
    // and a slot-aware router (ClusterConnection).
    // CONFIG_BEHAVIOR:Redis.UniversalEmptyMasterSelection: when sentinel_addresses
    // is non-empty but master name is empty, NewUniversalClient treats them as
    // normal nodes: 1 address → simple, >1 → cluster.
    if !cfg.cluster_addresses.is_empty()
        || (!cfg.sentinel_addresses.is_empty() && cfg.sentinel_master_name.is_empty())
    {
        // `redis.go:142` sets `IsClusterMode: true` on the Cluster branch and
        // `universal.go:431` returns a ClusterClient for **any** address count, so a
        // single `cluster_addresses` entry is still a cluster client — that is what
        // makes one plain Redis seed a startup failure upstream (`ERR This instance
        // has cluster support disabled`) instead of a working simple client. The
        // empty-`sentinel_master_name` fall-through comes from the *Sentinel* branch
        // (`redis.go:116-130`), which never sets that flag, so only there does the
        // address count decide between a simple and a cluster client
        // (CONFIG_BEHAVIOR:Redis.UniversalEmptyMasterSelection). Upstream also tests
        // `sentinel_addresses` first (`redis.go:100`), so sentinel wins when both are set.
        let (addrs, cluster_forced): (&[String], bool) = if !cfg.sentinel_addresses.is_empty() {
            (&cfg.sentinel_addresses, false)
        } else {
            (&cfg.cluster_addresses, true)
        };
        if addrs.len() == 1 && !cluster_forced {
            // Single address: Go selects simple client
            let info = make_info(
                &addrs[0],
                &cfg.username,
                &cfg.password,
                cfg.db,
                tls.as_deref(),
            )?;
            return Ok(RedisEndpoint::simple_from_sentinel_branch(
                info,
                tls.as_deref().and_then(sni_override_material),
                cfg.dial_timeout,
                cfg.read_timeout,
                cfg.write_timeout,
            ));
        }
        let username = cfg.username.clone();
        let password = cfg.password.clone();
        let db = cfg.db;
        let mut nodes = Vec::with_capacity(addrs.len());
        for addr in addrs {
            nodes.push(make_info(addr, &username, &password, db, tls.as_deref())?);
        }
        if !cluster_forced {
            return Ok(RedisEndpoint::cluster_from_sentinel_branch(
                nodes,
                tls.as_deref().and_then(sni_override_material),
                cfg.dial_timeout,
                cfg.read_timeout,
                cfg.write_timeout,
            ));
        }
        // The explicit Cluster branch configures MaxRedirects itself.
        let max_redirects = cfg.max_redirects.unwrap_or(2).max(0) as u64;
        return Ok(match tls.as_deref().and_then(sni_override_material) {
            Some(override_) => RedisEndpoint::cluster_with_tls_sni(nodes, max_redirects, override_),
            None => RedisEndpoint::cluster(nodes, max_redirects),
        });
    }
    let info = make_info(
        &cfg.address,
        &cfg.username,
        &cfg.password,
        cfg.db,
        tls.as_deref(),
    )?;
    let override_ = match tls.as_deref() {
        Some(material) => build_sni_override(&cfg.address, material)?,
        None => None,
    };
    Ok(match override_ {
        Some(override_) => RedisEndpoint::simple_with_tls_sni(info, override_),
        None => RedisEndpoint::simple(info),
    })
}

/// Optional CA bundle in PEM form; an empty path defers to the trust store.
/// The two failure texts and their bare (un-wrapped) shape are the contract:
/// `GetRedisClient` returns `ClientTLSConfig`'s error before the Ping, so the CLI
/// prints it without the `unable to connect to redis:` prefix.
fn read_ca(path: &str) -> Result<Option<Vec<u8>>, String> {
    if path.is_empty() {
        return Ok(None);
    }
    let pem = std::fs::read(path)
        .map_err(|e| format!("could not read ca certificate: {}", go_file_error(path, &e)))?;
    // `x509.CertPool.AppendCertsFromPEM` skips blocks it cannot parse and
    // reports failure only when none of them landed, which `xtls` turns into
    // `ErrFailedToLoadCACert`. A present-but-unparsable bundle is therefore a
    // hard error, never a silently empty trust store.
    if parsable_certificates(&pem) == 0 {
        return Err("failed to load CACertificate".into());
    }
    Ok(Some(pem))
}

/// Counts the PEM certificates that parse as trust anchors (Go's
/// `AppendCertsFromPEM` success count).
fn parsable_certificates(pem: &[u8]) -> usize {
    use rtc_tls::rustls::pki_types::pem::PemObject;
    let mut roots = rtc_tls::rustls::RootCertStore::empty();
    rtc_tls::rustls::pki_types::CertificateDer::pem_slice_iter(pem)
        .filter_map(Result::ok)
        .fold(0usize, |count, der| {
            if roots.add(der).is_ok() {
                count + 1
            } else {
                count
            }
        })
}

/// Go's `*os.PathError` text for a failed read of `path`: `open {path}: {errno}`,
/// with the errno lowercased and without Rust's ` (os error N)` suffix.
fn go_file_error(path: &str, error: &std::io::Error) -> String {
    let raw = error.to_string();
    let errno = match raw.find(" (os error ") {
        Some(cut) => &raw[..cut],
        None => &raw,
    };
    let mut chars = errno.chars();
    let lowered = match chars.next() {
        Some(first) => first.to_lowercase().collect::<String>() + chars.as_str(),
        None => String::new(),
    };
    format!("open {path}: {lowered}")
}

/// mTLS client key pair (cert and key PEM) when one is configured.
type ClientPair = (Option<Vec<u8>>, Option<Vec<u8>>);

/// mTLS client key pair; loaded only when a certificate is configured, exactly
/// as `xtls` calls `tls.LoadX509KeyPair` (which requires both halves). Returns
/// the raw cert/key PEMs so the same material can serve both the redis-rs
/// `TlsCertificates` and the SNI-override dial.
fn read_client_pair(tls: &TlsConfig) -> Result<ClientPair, String> {
    // The upstream guard is the *certificate* path alone
    // (`references/protocol-stable/xtls/tls.go:39`): `if c.ClientCertFile != ""`.
    // A `client_key_file` with no `client_cert_file` is therefore never opened —
    // the client sends no certificate and the server decides whether that is
    // fatal. Reading the key half first would turn a configuration upstream
    // accepts into a dial failure here.
    if tls.client_cert_file.is_empty() {
        return Ok((None, None));
    }
    let pair_error = |reason: String| format!("could not load client key pair: {reason}");
    let client_cert = std::fs::read(&tls.client_cert_file)
        .map_err(|e| pair_error(go_file_error(&tls.client_cert_file, &e)))?;
    let client_key = std::fs::read(&tls.client_key_file)
        .map_err(|e| pair_error(go_file_error(&tls.client_key_file, &e)))?;
    check_key_pair_pem(&client_cert, &client_key).map_err(pair_error)?;
    check_key_pair_der(&client_cert, &client_key).map_err(pair_error)?;
    Ok((Some(client_cert), Some(client_key)))
}

/// The PEM block type names Go's `pem.Decode` would hand back, in order: a block needs
/// its own `-----END <type>-----` line before the next header, so a truncated or
/// re-labelled block is skipped rather than counted. A trailer whose type differs from
/// its header makes Go stop scanning altogether; that pathological shape is not
/// reproduced here, because no upstream configuration reaches it.
fn pem_block_types(pem: &[u8]) -> Vec<String> {
    let text = String::from_utf8_lossy(pem);
    let mut types = Vec::new();
    let mut open: Option<String> = None;
    for line in text.lines() {
        let line = line.trim();
        if let Some(rest) = line.strip_prefix("-----BEGIN ") {
            open = rest.strip_suffix("-----").map(str::to_owned);
            continue;
        }
        if let (Some(kind), Some(rest)) = (open.as_ref(), line.strip_prefix("-----END ")) {
            if rest
                .strip_suffix("-----")
                .is_some_and(|end| end == kind.as_str())
            {
                types.push(kind.clone());
                open = None;
            }
        }
    }
    types
}

/// `tls.X509KeyPair`'s input checks
/// (`references/go-stdlib-go1.26.3/src/crypto/tls/tls.go:266-306`), reproduced with
/// Go's own wording. `go_file_error` above covers the `os.ReadFile` half of
/// `LoadX509KeyPair`; this covers the parsing half. Only what Go's text actually
/// derives from is rendered here: a pair whose two halves each parse but do not
/// belong together still fails, and its reason comes from rustls, not from a
/// sentence invented below.
fn check_key_pair_pem(cert: &[u8], key: &[u8]) -> Result<(), String> {
    let cert_types = pem_block_types(cert);
    let certificates: Vec<&String> = cert_types
        .iter()
        .filter(|t| t.as_str() == "CERTIFICATE")
        .collect();
    if certificates.is_empty() {
        if cert_types.is_empty() {
            return Err("tls: failed to find any PEM data in certificate input".into());
        }
        if cert_types.len() == 1 && cert_types[0].ends_with("PRIVATE KEY") {
            return Err(
                "tls: failed to find certificate PEM data in certificate input, but did find a \
                 private key; PEM inputs may have been switched"
                    .into(),
            );
        }
        return Err(format!(
            "tls: failed to find \"CERTIFICATE\" PEM block in certificate input after skipping \
             PEM blocks of the following types: [{}]",
            cert_types.join(" ")
        ));
    }
    // Go stops scanning the key at the first usable private-key block and only
    // lists what it skipped before that point.
    let mut skipped: Vec<String> = Vec::new();
    for kind in pem_block_types(key) {
        if kind == "PRIVATE KEY" || kind.ends_with(" PRIVATE KEY") {
            return Ok(());
        }
        skipped.push(kind);
    }
    if skipped.is_empty() {
        return Err("tls: failed to find any PEM data in key input".into());
    }
    if skipped.len() == 1 && skipped[0] == "CERTIFICATE" {
        return Err(
            "tls: found a certificate rather than a key in the PEM for the private key".into(),
        );
    }
    Err(format!(
        "tls: failed to find PEM block with type ending in \"PRIVATE KEY\" in key input after \
         skipping PEM blocks of the following types: [{}]",
        skipped.join(" ")
    ))
}

/// One DER tag/length/value at the front of `data`: the tag, its contents and what
/// follows. Only the definite forms OpenSSL emits are read; anything else yields `None`
/// and the caller defers, which is what this file did before this walk existed.
fn der_tlv(data: &[u8]) -> Option<(u8, &[u8], &[u8])> {
    let (&tag, rest) = data.split_first()?;
    let (&length, rest) = rest.split_first()?;
    let (len, rest) = match length {
        0x00..=0x7f => (length as usize, rest),
        0x81 => {
            let (&one, rest) = rest.split_first()?;
            (one as usize, rest)
        }
        0x82 => {
            let (&hi, rest) = rest.split_first()?;
            let (&lo, rest) = rest.split_first()?;
            ((hi as usize) << 8 | lo as usize, rest)
        }
        _ => return None,
    };
    if rest.len() < len {
        return None;
    }
    Some((tag, &rest[..len], &rest[len..]))
}

/// The children of a constructed DER value.
fn der_children(mut data: &[u8]) -> Option<Vec<(u8, &[u8])>> {
    let mut children = Vec::new();
    while !data.is_empty() {
        let (tag, value, rest) = der_tlv(data)?;
        children.push((tag, value));
        data = rest;
    }
    Some(children)
}

/// The key families `tls.X509KeyPair` switches over
/// (`references/go-stdlib-go1.26.3/src/crypto/tls/tls.go:331-352`). A DER OID is
/// canonical, so its content octets identify the arcs without decoding them.
#[derive(Clone, Copy, PartialEq, Eq)]
enum KeyFamily {
    Rsa,
    Ec,
    Ed25519,
}

fn key_family(oid: &[u8]) -> Option<KeyFamily> {
    match oid {
        // 1.2.840.113549.1.1.1 rsaEncryption
        [0x2a, 0x86, 0x48, 0x86, 0xf7, 0x0d, 0x01, 0x01, 0x01] => Some(KeyFamily::Rsa),
        // 1.2.840.10045.2.1 ecPublicKey
        [0x2a, 0x86, 0x48, 0xce, 0x3d, 0x02, 0x01] => Some(KeyFamily::Ec),
        // 1.3.101.112 ed25519 (RFC 8410; 1.3.101.110 is X25519, which Go's switch
        // does not have an arm for)
        [0x2b, 0x65, 0x70] => Some(KeyFamily::Ed25519),
        _ => None,
    }
}

/// A public half in the shape Go's `PublicKey.Equal` compares it: RSA by
/// `(modulus, exponent)`, the other families by the key bytes themselves.
enum PublicKey {
    Rsa(Vec<u8>, Vec<u8>),
    Bits(Vec<u8>),
}

impl PublicKey {
    /// `rsa.PublicKey.Equal` compares values; a DER INTEGER may or may not carry the
    /// leading sign byte the other half happened to need.
    fn number(value: &[u8]) -> &[u8] {
        match value.split_first() {
            Some((0x00, rest)) => rest,
            _ => value,
        }
    }

    fn matches(&self, other: &PublicKey) -> bool {
        match (self, other) {
            (PublicKey::Rsa(cn, ce), PublicKey::Rsa(kn, ke)) => {
                Self::number(cn) == Self::number(kn) && Self::number(ce) == Self::number(ke)
            }
            (PublicKey::Bits(a), PublicKey::Bits(b)) => a == b,
            _ => false,
        }
    }
}

/// The certificate's `subjectPublicKeyInfo`. `X509KeyPair` parses
/// `cert.Certificate[0]`, so this is the first PEM certificate block.
fn certificate_public_key(der: &[u8]) -> Option<(KeyFamily, PublicKey)> {
    let (_, certificate, _) = der_tlv(der)?;
    let (_, tbs, _) = der_tlv(certificate)?;
    let mut fields = der_children(tbs)?;
    if fields.first()?.0 == 0xa0 {
        fields.remove(0); // `[0] version` is OPTIONAL
    }
    // serialNumber, signature, issuer, validity, subject, subjectPublicKeyInfo
    let (tag, spki) = *fields.get(5)?;
    if tag != 0x30 {
        return None;
    }
    let children = der_children(spki)?;
    let family = key_family(der_children(children.first()?.1)?.first()?.1)?;
    let (bits_tag, bits) = *children.get(1)?;
    if bits_tag != 0x03 {
        return None;
    }
    let bits = bits.get(1..)?.to_vec(); // past the BIT STRING's unused-bits octet
    match family {
        KeyFamily::Rsa => {
            // The RSA public key is itself a DER `SEQUENCE { modulus, publicExponent }`
            // (RFC 8017) inside the BIT STRING, so the walk descends one more level.
            let pair = der_children(der_tlv(&bits)?.1)?;
            let (n_tag, n) = *pair.first()?;
            let (e_tag, e) = *pair.get(1)?;
            if n_tag != 0x02 || e_tag != 0x02 {
                return None;
            }
            Some((family, PublicKey::Rsa(n.to_vec(), e.to_vec())))
        }
        KeyFamily::Ec | KeyFamily::Ed25519 => Some((family, PublicKey::Bits(bits))),
    }
}

/// `RSAPrivateKey` (RFC 8017) and `ECPrivateKey` (RFC 5915) carry the public half, so
/// both the traditional form and the same DER wrapped in PKCS#8 are readable here.
/// OpenSSL writes an Ed25519 PKCS#8 key as a bare seed, which cannot be compared
/// without deriving the public half, so that family reports `None` and defers.
fn traditional_public_key(family: KeyFamily, der: &[u8]) -> Option<(KeyFamily, Option<PublicKey>)> {
    if family == KeyFamily::Ed25519 {
        // RFC 8410's `Curve25519PrivateKey` is either a bare seed — a `OCTET STRING` TLV
        // with no public half anywhere, so only the family is knowable — or a SEQUENCE
        // whose primitive `[4]` carries the public key, with or without the BIT STRING's
        // unused-bits octet.
        let public = der_tlv(der)
            .filter(|(tag, _, _)| *tag == 0x30)
            .and_then(|(_, body, _)| der_children(body))
            .and_then(|fields| fields.iter().find(|(tag, _)| *tag == 0x84).map(|(_, v)| *v))
            .and_then(|value| match value {
                [0x00, rest @ ..] if rest.len() == 32 => Some(rest.to_vec()),
                bytes if bytes.len() == 32 => Some(bytes.to_vec()),
                _ => None,
            })
            .map(PublicKey::Bits);
        return Some((KeyFamily::Ed25519, public));
    }
    let (_, body, _) = der_tlv(der)?;
    let fields = der_children(body)?;
    match family {
        // SEQUENCE { version, modulus, publicExponent, privateExponent, ... }
        KeyFamily::Rsa if fields.get(1)?.0 == 0x02 && fields.get(2)?.0 == 0x02 => Some((
            KeyFamily::Rsa,
            Some(PublicKey::Rsa(fields[1].1.to_vec(), fields[2].1.to_vec())),
        )),
        // SEQUENCE { version, privateKey, [0] parameters, [1] publicKey }
        KeyFamily::Ec => {
            let public = fields
                .iter()
                .find(|(tag, _)| *tag == 0xa1)
                .and_then(|(_, value)| der_tlv(value))
                .and_then(|(tag, bits, _)| (tag == 0x03).then_some(bits))
                .and_then(|bits| bits.get(1..))
                .map(<[u8]>::to_vec)
                .map(PublicKey::Bits);
            Some((KeyFamily::Ec, public))
        }
        // A guarded RSA shape this walk cannot read is not a pairing verdict.
        _ => None,
    }
}

/// The private key's family and public half, from the block `pem.Decode` hands to Go's
/// `parsePrivateKey`. The family is read from the DER (PKCS#8's `AlgorithmIdentifier`,
/// otherwise the label rustls-pemfile recognised), so a pair of blocks that merely
/// parses differently from what it claims falls through to the crypto layer below.
fn private_key_public_key(der: &PrivateKeyDer<'_>) -> Option<(KeyFamily, Option<PublicKey>)> {
    match der {
        PrivateKeyDer::Pkcs8(wrapper) => {
            let (_, body, _) = der_tlv(wrapper.secret_pkcs8_der())?;
            let fields = der_children(body)?;
            if fields.len() != 3 || fields.first()?.0 != 0x02 || fields.get(2)?.0 != 0x04 {
                return None;
            }
            let family = key_family(der_children(fields[1].1)?.first()?.1)?;
            traditional_public_key(family, fields[2].1)
        }
        PrivateKeyDer::Pkcs1(body) => {
            traditional_public_key(KeyFamily::Rsa, body.secret_pkcs1_der())
        }
        PrivateKeyDer::Sec1(body) => traditional_public_key(KeyFamily::Ec, body.secret_sec1_der()),
        // A labelled key type Go's `parsePrivateKey` does not know (the enum is
        // non-exhaustive) is the crypto layer's to judge.
        _ => None,
    }
}

/// `tls.X509KeyPair`'s last step: parse the leaf, parse the key, then compare their
/// public halves. Go names the family disagreement and the value disagreement with two
/// different sentences, so both are reproduced here instead of being left to the crypto
/// layer, which reports them inside the dial in its own wording. A pair this walk cannot
/// classify — an unknown algorithm, an Ed25519 seed with no `[4]` public key, a block
/// whose DER does not match its label — is still left to that layer.
fn check_key_pair_der(cert: &[u8], key: &[u8]) -> Result<(), String> {
    use rtc_tls::rustls::pki_types::{CertificateDer, pem::PemObject};
    let Some(Ok(cert_der)) = CertificateDer::pem_slice_iter(cert).next() else {
        return Ok(());
    };
    let Ok(key_der) = PrivateKeyDer::from_pem_slice(key) else {
        return Ok(());
    };
    let Some((cert_family, cert_public)) = certificate_public_key(cert_der.as_ref()) else {
        return Ok(());
    };
    let Some((key_family, key_public)) = private_key_public_key(&key_der) else {
        return Ok(());
    };
    if cert_family != key_family {
        return Err("tls: private key type does not match public key type".into());
    }
    if let Some(key_public) = key_public {
        if !cert_public.matches(&key_public) {
            return Err("tls: private key does not match public key".into());
        }
    }
    Ok(())
}

/// The `xtls` client material for the byte-exact go-redis session dial
/// ([`cli_redis_session`](crate::cli_nodes::cli_redis_session)). The session owns
/// the TCP dial, the 5-attempt group and the `redis:` pool diagnostic, so the TLS
/// handshake happens there too rather than inside redis-rs; this struct is the
/// `tls.Config` that `GetRedisClient` hands to `tls.Client`.
#[derive(Clone, Debug)]
pub(crate) struct SessionTls {
    pub insecure: bool,
    pub server_name: String,
    pub roots_pem: Option<Vec<u8>>,
    pub client_cert_pem: Option<Vec<u8>>,
    pub client_key_pem: Option<Vec<u8>>,
}

impl From<&TlsMaterial> for SessionTls {
    fn from(material: &TlsMaterial) -> Self {
        Self {
            insecure: material.insecure,
            server_name: material.server_name.clone(),
            roots_pem: material.root_pem.clone(),
            client_cert_pem: material.client_cert_pem.clone(),
            client_key_pem: material.client_key_pem.clone(),
        }
    }
}

/// `GetRedisClient`'s TLS decision for the simple (single-address) client: `None`
/// for a plaintext dial, otherwise the loaded `xtls.Config` material. Errors are
/// the bare `ClientTLSConfig` texts, which upstream returns **before** the Ping so
/// they are never prefixed with `unable to connect to redis:`.
pub(crate) fn session_tls(cfg: &RedisConfig) -> Result<Option<SessionTls>, String> {
    if !tls_requested(cfg) {
        return Ok(None);
    }
    Ok(Some(SessionTls::from(&load_tls_material(cfg)?)))
}

/// The `tls.Config` behind a session TLS dial.
pub(crate) fn session_client_config(
    tls: &SessionTls,
) -> Result<rtc_tls::rustls::ClientConfig, String> {
    let pair = (
        tls.client_cert_pem.as_deref(),
        tls.client_key_pem.as_deref(),
    );
    if tls.insecure {
        // Go: `InsecureSkipVerify` still builds the config from the same paths,
        // so a client key pair is offered while the peer certificate is ignored.
        return rtc_tls::insecure_client_config(pair.0, pair.1).map_err(|e| e.to_string());
    }
    rtc_tls::verified_client_config(tls.roots_pem.as_deref(), pair.0, pair.1)
        .map_err(|e| e.to_string())
}

/// go-redis sets `ServerName` from the config, else from the dialled host; Go
/// sends no SNI for an IP literal and verifies IP SANs instead of DNS names.
pub(crate) fn session_server_name(tls: &SessionTls, host: &str) -> String {
    if tls.server_name.is_empty() {
        host.to_string()
    } else {
        tls.server_name.clone()
    }
}

/// The handshake failure text for a dial that surfaced its error as
/// `std::io::Error` (what tokio-rustls yields): the wrapped `rustls::Error` gets
/// Go's wording, anything else keeps its own.
pub(crate) fn go_handshake_text(error: &std::io::Error) -> String {
    match error
        .get_ref()
        .and_then(|inner| inner.downcast_ref::<rtc_tls::rustls::Error>())
    {
        Some(rustls_error) => go_tls_error_text(rustls_error),
        None => error.to_string(),
    }
}

/// Renders a failed client handshake the way Go's `crypto/tls` does, because that
/// text is what go-redis puts in both surfaces of a failed Ping:
/// `unable to connect to redis: <text>` and the
/// `redis: … pool.go:762: … failed to dial after 5 attempts: <text>` line.
///
/// Only the certificate-verification family has a Go counterpart that Rust's
/// error carries; anything else keeps its own text instead of inventing a
/// sentence Go would not print.
pub(crate) fn go_tls_error_text(error: &rtc_tls::rustls::Error) -> String {
    use rtc_tls::rustls::Error;
    match error {
        Error::InvalidCertificate(cert) => match go_x509_text(cert) {
            Some(text) => format!("tls: failed to verify certificate: {text}"),
            None => error.to_string(),
        },
        _ => error.to_string(),
    }
}

/// `crypto/x509` verification-failure texts. `None` for a reason whose Go wording
/// is not derivable from what Rust reports, so the caller keeps the Rust text
/// rather than inventing a sentence Go would not print.
fn go_x509_text(error: &rtc_tls::rustls::CertificateError) -> Option<String> {
    use rtc_tls::rustls::CertificateError as Cert;
    match error {
        Cert::UnknownIssuer => Some("x509: certificate signed by unknown authority".into()),
        Cert::Expired | Cert::ExpiredContext { .. } => Some("x509: certificate has expired".into()),
        Cert::NotValidYet | Cert::NotValidYetContext { .. } => {
            Some("x509: certificate is not yet valid".into())
        }
        Cert::Revoked => Some("x509: certificate revoked".into()),
        Cert::UnhandledCriticalExtension => Some("x509: unhandled critical extension".into()),
        Cert::InvalidPurpose | Cert::InvalidPurposeContext { .. } => {
            Some("x509: certificate specifies an incompatible key usage".into())
        }
        Cert::NotValidForNameContext {
            expected,
            presented,
        } => Some(go_hostname_error(expected, presented)),
        _ => None,
    }
}

/// `x509.HostnameError.Error()`: the certificate's own subject alternative names,
/// restricted to the kind being verified (a DNS host checks `DNSNames`, an IP host
/// checks `IPAddresses`), deduplicated in order and joined with ", ".
fn go_hostname_error(
    expected: &rtc_tls::rustls::pki_types::ServerName<'_>,
    presented: &[String],
) -> String {
    let want_ip = matches!(
        expected,
        rtc_tls::rustls::pki_types::ServerName::IpAddress(_)
    );
    let mut allowed: Vec<String> = Vec::new();
    for name in presented {
        if let Some((is_ip, value)) = san_kind_and_value(name)
            && is_ip == want_ip
            && !allowed.contains(&value)
        {
            allowed.push(value);
        }
    }
    format!(
        "x509: certificate is valid for {}, not {}",
        allowed.join(", "),
        expected.to_str()
    )
}

/// `CertificateError` carries the SANs as display strings, either in the
/// `ServerName` debug form (`DnsName("localhost")`, `IpAddress(127.0.0.1)`) or bare;
/// both shapes classify the same way.
fn san_kind_and_value(name: &str) -> Option<(bool, String)> {
    let unquote = |text: &str| -> String {
        text.strip_prefix('"')
            .and_then(|inner| inner.strip_suffix('"'))
            .unwrap_or(text)
            .to_string()
    };
    if let Some(rest) = name
        .strip_prefix("DnsName(")
        .and_then(|inner| inner.strip_suffix(')'))
    {
        return Some((false, unquote(rest)));
    }
    if let Some(rest) = name
        .strip_prefix("IpAddress(")
        .and_then(|inner| inner.strip_suffix(')'))
    {
        return Some((true, rest.to_string()));
    }
    let bare = unquote(name);
    if bare.parse::<std::net::IpAddr>().is_ok() {
        return Some((true, bare));
    }
    Some((false, bare))
}

#[cfg(test)]
mod tests {
    #[path = "empty_master_timeout_tests.rs"]
    mod empty_master_timeout_tests;
    use super::*;
    use redis::ConnectionAddr;

    fn fixture(name: &str) -> String {
        let manifest = std::path::Path::new(env!("CARGO_MANIFEST_DIR"));
        manifest
            .join("../rtc-tls/tests/fixtures")
            .join(name)
            .to_string_lossy()
            .into_owned()
    }

    #[test]
    fn plaintext_when_neither_tls_nor_use_tls() {
        let cfg = RedisConfig {
            address: "localhost:6379".into(),
            password: "secret".into(),
            db: 3,
            ..Default::default()
        };
        assert!(!tls_requested(&cfg));
        let info = connection_info(&cfg).unwrap();
        assert!(
            matches!(info.addr(), ConnectionAddr::Tcp(..)),
            "no TLS requested: {:?}",
            info.addr()
        );
    }

    #[test]
    fn deprecated_use_tls_builds_a_verified_tcp_tls_addr() {
        let cfg = RedisConfig {
            address: "redis.example:6380".into(),
            use_tls: true,
            ..Default::default()
        };
        assert!(tls_requested(&cfg));
        let info = connection_info(&cfg).unwrap();
        match info.addr() {
            ConnectionAddr::TcpTls {
                host,
                port,
                insecure,
                tls_params,
            } => {
                assert_eq!(host.as_str(), "redis.example");
                assert_eq!(*port, 6380);
                assert!(!insecure, "use_tls verifies by default");
                assert!(tls_params.is_some(), "TLS parameters are attached");
            }
            other => panic!("expected TcpTls, got {other:?}"),
        }
    }

    #[test]
    fn tls_enabled_insecure_marks_the_addr_insecure() {
        let cfg = RedisConfig {
            address: "localhost:6379".into(),
            tls: Some(TlsConfig {
                enabled: true,
                insecure: true,
                ..Default::default()
            }),
            ..Default::default()
        };
        let info = connection_info(&cfg).unwrap();
        assert!(
            matches!(info.addr(), ConnectionAddr::TcpTls { insecure: true, .. }),
            "insecure TLS: {:?}",
            info.addr()
        );
    }

    #[test]
    fn tls_enabled_with_a_ca_file_attaches_root_parameters() {
        let ca = fixture("root.pem");
        let cfg = RedisConfig {
            address: "localhost:6379".into(),
            tls: Some(TlsConfig {
                enabled: true,
                ca_cert_file: ca.clone(),
                ..Default::default()
            }),
            ..Default::default()
        };
        let info = connection_info(&cfg).unwrap();
        assert!(
            matches!(
                info.addr(),
                ConnectionAddr::TcpTls {
                    insecure: false,
                    ..
                }
            ),
            "verified TLS with a CA: {:?}",
            info.addr()
        );
    }

    #[test]
    fn missing_ca_file_reports_a_go_style_load_error() {
        let cfg = RedisConfig {
            address: "localhost:6379".into(),
            tls: Some(TlsConfig {
                enabled: true,
                ca_cert_file: "/nonexistent/ca.pem".into(),
                ..Default::default()
            }),
            ..Default::default()
        };
        let err = connection_info(&cfg).unwrap_err();
        assert!(err.starts_with("could not read ca certificate"), "{err}");
    }

    #[test]
    fn disabled_tls_block_falls_back_to_use_tls_semantics() {
        // `tls.enabled == false` with `use_tls == true`: Go ignores the disabled
        // block and uses the plain verified TLS config (no certificate paths).
        let cfg = RedisConfig {
            address: "localhost:6379".into(),
            use_tls: true,
            tls: Some(TlsConfig {
                enabled: false,
                ca_cert_file: "/nonexistent/ca.pem".into(),
                ..Default::default()
            }),
            ..Default::default()
        };
        let info = connection_info(&cfg).unwrap();
        assert!(
            matches!(
                info.addr(),
                ConnectionAddr::TcpTls {
                    insecure: false,
                    ..
                }
            ),
            "disabled block is skipped: {:?}",
            info.addr()
        );
    }

    fn tls_cfg(address: &str, tls: TlsConfig) -> RedisConfig {
        RedisConfig {
            address: address.into(),
            tls: Some(tls),
            ..Default::default()
        }
    }

    #[test]
    fn server_name_equal_to_host_leaves_the_redis_rs_path_unchanged() {
        // redis-rs already uses the address host as SNI; no override is needed.
        let material = load_tls_material(&tls_cfg(
            "redis.example:6380",
            TlsConfig {
                enabled: true,
                server_name: "redis.example".into(),
                ca_cert_file: fixture("root.pem"),
                ..Default::default()
            },
        ))
        .unwrap();
        assert!(
            build_sni_override("redis.example:6380", &material)
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn server_name_override_is_built_when_it_differs_from_the_host() {
        // Dial by IP but verify/SNI against DNS:redis.test.local.
        let material = load_tls_material(&tls_cfg(
            "127.0.0.1:7799",
            TlsConfig {
                enabled: true,
                server_name: "redis.test.local".into(),
                ca_cert_file: fixture("root.pem"),
                ..Default::default()
            },
        ))
        .unwrap();
        let override_ = build_sni_override("127.0.0.1:7799", &material)
            .unwrap()
            .expect("override must be built when the name differs from the host");
        assert_eq!(override_.server_name, "redis.test.local");
        assert!(
            override_
                .root_pem
                .as_ref()
                .is_some_and(|pem| !pem.is_empty()),
            "the configured CA bundle is carried into the override dial"
        );
        assert!(override_.client_cert_pem.is_none());
    }

    #[test]
    fn server_name_override_carries_the_mtls_client_pair() {
        let material = load_tls_material(&tls_cfg(
            "127.0.0.1:7799",
            TlsConfig {
                enabled: true,
                server_name: "redis.test.local".into(),
                ca_cert_file: fixture("root.pem"),
                client_cert_file: fixture("server.pem"),
                client_key_file: fixture("server-key.pem"),
                ..Default::default()
            },
        ))
        .unwrap();
        let override_ = build_sni_override("127.0.0.1:7799", &material)
            .unwrap()
            .expect("override present");
        assert!(override_.client_cert_pem.is_some());
        assert!(override_.client_key_pem.is_some());
    }

    #[test]
    fn server_name_override_without_a_ca_bundle_uses_the_system_trust_store() {
        // Go leaves `RootCAs` empty in this case and verifies against the system
        // trust anchors; the override dial does the same instead of failing.
        let material = load_tls_material(&tls_cfg(
            "127.0.0.1:7799",
            TlsConfig {
                enabled: true,
                server_name: "redis.test.local".into(),
                ..Default::default()
            },
        ))
        .unwrap();
        let override_ = build_sni_override("127.0.0.1:7799", &material)
            .unwrap()
            .expect("override present");
        assert!(override_.root_pem.is_none(), "system trust roots are used");
    }

    #[test]
    fn sentinel_tls_applies_the_server_name_override_to_discovery_and_master() {
        // Upstream applies one TLSConfig to sentinelOptions and masterOptions,
        // so the sentinel endpoint must wrap both dials in the override.
        let mut cfg = tls_cfg(
            "127.0.0.1:7799",
            TlsConfig {
                enabled: true,
                server_name: "redis.test.local".into(),
                ca_cert_file: fixture("root.pem"),
                ..Default::default()
            },
        );
        cfg.sentinel_addresses = vec!["127.0.0.1:26379".into()];
        cfg.sentinel_master_name = "mymaster".into();
        assert!(redis_endpoint(&cfg).is_ok());
    }

    #[test]
    fn insecure_server_name_stays_on_the_redis_rs_path() {
        // InsecureSkipVerify skips server-name verification, so an overridden
        // name has no verification role; redis-rs' insecure dial is used instead.
        let material = load_tls_material(&tls_cfg(
            "127.0.0.1:7799",
            TlsConfig {
                enabled: true,
                insecure: true,
                server_name: "redis.test.local".into(),
                ca_cert_file: fixture("root.pem"),
                ..Default::default()
            },
        ))
        .unwrap();
        assert!(
            build_sni_override("127.0.0.1:7799", &material)
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn redis_endpoint_applies_the_override_only_when_needed() {
        // Differing name + CA => override endpoint; equal name => plain endpoint.
        let differing = redis_endpoint(&tls_cfg(
            "127.0.0.1:7799",
            TlsConfig {
                enabled: true,
                server_name: "redis.test.local".into(),
                ca_cert_file: fixture("root.pem"),
                ..Default::default()
            },
        ));
        assert!(differing.is_ok(), "{differing:?}");
        let equal = redis_endpoint(&tls_cfg(
            "redis.test.local:7799",
            TlsConfig {
                enabled: true,
                server_name: "redis.test.local".into(),
                ca_cert_file: fixture("root.pem"),
                ..Default::default()
            },
        ));
        assert!(equal.is_ok(), "{equal:?}");
    }

    // ---- SPEC-0074 C12: Cluster + UniversalEmptyMasterSelection ----

    #[test]
    fn cluster_addresses_selects_cluster_endpoint() {
        let cfg = RedisConfig {
            cluster_addresses: vec!["127.0.0.1:7001".into(), "127.0.0.1:7002".into()],
            ..Default::default()
        };
        let ep = redis_endpoint(&cfg).expect("cluster config must succeed");
        assert!(ep.is_cluster());
    }

    #[test]
    fn a_single_cluster_seed_is_still_a_cluster_client() {
        // `redis.go:142` sets `IsClusterMode: true` and `universal.go:431` builds a
        // ClusterClient for any address count once that flag is set, so one seed must
        // be validated as a cluster node (and therefore reject a plain Redis instance)
        // rather than quietly downgraded to a working simple client.
        let cfg = RedisConfig {
            cluster_addresses: vec!["127.0.0.1:7001".into()],
            ..Default::default()
        };
        let ep = redis_endpoint(&cfg).expect("one cluster seed is still cluster mode");
        assert!(
            ep.is_cluster(),
            "IsClusterMode is not an address-count rule"
        );
    }

    #[test]
    fn sentinel_takes_precedence_over_cluster() {
        let cfg = RedisConfig {
            sentinel_addresses: vec!["127.0.0.1:26379".into()],
            sentinel_master_name: "mymaster".into(),
            cluster_addresses: vec!["127.0.0.1:7001".into()],
            ..Default::default()
        };
        let ep = redis_endpoint(&cfg).expect("sentinel wins");
        assert!(!ep.is_cluster(), "sentinel takes precedence over cluster");
    }

    #[test]
    fn universal_empty_master_single_addr_is_simple() {
        // Go NewUniversalClient: sentinel_addresses non-empty + empty masterName
        // with 1 address → simple client (not sentinel).
        let cfg = RedisConfig {
            sentinel_addresses: vec!["127.0.0.1:6379".into()],
            sentinel_master_name: String::new(),
            ..Default::default()
        };
        let ep = redis_endpoint(&cfg).expect("single addr → simple");
        assert!(!ep.is_cluster());
    }

    #[test]
    fn universal_empty_master_multiple_addrs_is_cluster() {
        // Go NewUniversalClient: sentinel_addresses non-empty + empty masterName
        // with >1 addresses → cluster client.
        let cfg = RedisConfig {
            sentinel_addresses: vec!["127.0.0.1:7001".into(), "127.0.0.1:7002".into()],
            sentinel_master_name: String::new(),
            ..Default::default()
        };
        let ep = redis_endpoint(&cfg).expect("multiple addrs → cluster");
        assert!(ep.is_cluster());
    }

    #[test]
    fn empty_address_no_sentinel_no_cluster_fails_gracefully() {
        // When all address fields are empty, Go's IsConfigured() returns false
        // and the server never calls GetRedisClient. Our factory returns an
        // error because make_info("") cannot parse a valid URL. This is
        // acceptable: startup validates Redis config before calling the factory.
        let cfg = RedisConfig::default();
        assert!(redis_endpoint(&cfg).is_err());
    }

    #[test]
    fn session_tls_is_none_without_a_tls_request() {
        let cfg = RedisConfig {
            address: "localhost:6379".into(),
            ..Default::default()
        };
        assert!(
            session_tls(&cfg).unwrap().is_none(),
            "a plain address must not dial a handshake"
        );
    }

    #[test]
    fn session_client_config_builds_the_insecure_and_verified_dials() {
        let insecure = SessionTls {
            insecure: true,
            server_name: String::new(),
            roots_pem: None,
            client_cert_pem: None,
            client_key_pem: None,
        };
        assert!(session_client_config(&insecure).is_ok());
        assert_eq!(session_server_name(&insecure, "127.0.0.1"), "127.0.0.1");

        let verified = SessionTls {
            insecure: false,
            server_name: "localhost".into(),
            roots_pem: Some(std::fs::read(fixture("root.pem")).unwrap()),
            client_cert_pem: Some(std::fs::read(fixture("server.pem")).unwrap()),
            client_key_pem: Some(std::fs::read(fixture("server-key.pem")).unwrap()),
        };
        assert!(session_client_config(&verified).is_ok());
        assert_eq!(session_server_name(&verified, "127.0.0.1"), "localhost");
    }

    #[test]
    fn unparsable_ca_bundle_is_a_hard_failure_not_an_empty_trust_store() {
        // Go appends only the blocks it can parse and reports
        // ErrFailedToLoadCACert when none landed; accepting the file and dialing
        // with no roots would be the forbidden "read the config, ignore it".
        let pem = std::fs::read(fixture("README.md")).unwrap();
        assert_eq!(parsable_certificates(&pem), 0);
        let cfg = RedisConfig {
            address: "localhost:6379".into(),
            tls: Some(TlsConfig {
                enabled: true,
                ca_cert_file: fixture("README.md"),
                ..Default::default()
            }),
            ..Default::default()
        };
        assert_eq!(
            session_tls(&cfg).unwrap_err(),
            "failed to load CACertificate".to_string()
        );
    }

    #[test]
    fn go_tls_error_text_uses_crypto_tls_wording_for_known_reasons() {
        use rtc_tls::rustls::{CertificateError as Cert, Error};
        let mapped = [
            (
                Cert::UnknownIssuer,
                "tls: failed to verify certificate: x509: certificate signed by unknown authority",
            ),
            (
                Cert::Expired,
                "tls: failed to verify certificate: x509: certificate has expired",
            ),
            (
                Cert::Revoked,
                "tls: failed to verify certificate: x509: certificate revoked",
            ),
            (
                Cert::UnhandledCriticalExtension,
                "tls: failed to verify certificate: x509: unhandled critical extension",
            ),
        ];
        for (reason, want) in mapped {
            assert_eq!(go_tls_error_text(&Error::InvalidCertificate(reason)), want);
        }
        // A reason with no derivable Go wording keeps Rust's own text.
        let unmapped = Error::InvalidCertificate(Cert::BadEncoding);
        let text = go_tls_error_text(&unmapped);
        assert_eq!(text, unmapped.to_string());
        assert!(
            !text.starts_with("tls: failed to verify certificate"),
            "invented Go wording for an unmapped reason: {text}"
        );
    }

    #[test]
    fn hostname_error_lists_only_the_san_kind_under_verification() {
        use rtc_tls::rustls::CertificateError as Cert;
        use rtc_tls::rustls::pki_types::ServerName;
        let presented = || {
            vec![
                "DnsName(\"localhost\")".to_string(),
                "IpAddress(127.0.0.1)".to_string(),
                "DnsName(\"localhost\")".to_string(),
            ]
        };
        let dns = ServerName::try_from("not.example").unwrap();
        assert_eq!(
            go_x509_text(&Cert::NotValidForNameContext {
                expected: dns,
                presented: presented(),
            })
            .unwrap(),
            "x509: certificate is valid for localhost, not not.example"
        );
        let ip = ServerName::try_from("10.0.0.1").unwrap();
        assert_eq!(
            go_x509_text(&Cert::NotValidForNameContext {
                expected: ip,
                presented: presented(),
            })
            .unwrap(),
            "x509: certificate is valid for 127.0.0.1, not 10.0.0.1"
        );
    }

    #[test]
    fn go_file_error_renders_a_go_path_error() {
        let error = std::io::Error::from_raw_os_error(2);
        assert_eq!(
            go_file_error("/nonexistent/ca.pem", &error),
            "open /nonexistent/ca.pem: no such file or directory"
        );
    }

    // ---- SPEC-0076 workline A: `xtls` client key pair guard and PEM failures ----

    fn pem(kind: &str, body: &str) -> String {
        format!("-----BEGIN {kind}-----\n{body}\n-----END {kind}-----\n")
    }

    fn tls_material(cert_file: &str, key_file: &str) -> Result<TlsMaterial, String> {
        load_tls_material(&tls_cfg(
            "localhost:6379",
            TlsConfig {
                enabled: true,
                client_cert_file: cert_file.into(),
                client_key_file: key_file.into(),
                ..Default::default()
            },
        ))
    }

    fn der(name: &str) -> String {
        fixture(&format!("expanded/{name}"))
    }

    fn pair_error(cert: &str, key: &str) -> Option<String> {
        check_key_pair_der(
            std::fs::read(cert).unwrap().as_slice(),
            std::fs::read(key).unwrap().as_slice(),
        )
        .err()
    }

    #[test]
    fn every_supported_key_family_pairs_with_its_own_certificate() {
        // A pair that belongs together must never be refused here: Go's `X509KeyPair`
        // only fails on the comparison, and these are the shapes its `switch` accepts.
        for (cert, key) in [
            ("rsa.pem", "rsa-key.pem"),        // PKCS#8
            ("rsa.pem", "rsa-pkcs1-key.pem"),  // PKCS#1 traditional
            ("p256.pem", "p256-key.pem"),      // PKCS#8 EC
            ("p256.pem", "p256-sec1-key.pem"), // SEC1 EC
            ("ed25519.pem", "ed25519-key.pem"),
        ] {
            let pair = format!("{cert} + {key}");
            assert_eq!(pair_error(&der(cert), &der(key)), None, "{pair}");
        }
    }

    #[test]
    fn a_key_of_the_other_family_gets_gos_type_sentence() {
        // `tls.go:335` and `:344`: the private key parses, but not into the Go type the
        // certificate's public key names.
        for (cert, key) in [
            ("rsa.pem", "p256-key.pem"),
            ("p256.pem", "rsa-key.pem"),
            ("rsa.pem", "ed25519-key.pem"),
            ("ed25519.pem", "p384-sec1-key.pem"),
        ] {
            assert_eq!(
                pair_error(&der(cert), &der(key)).as_deref(),
                Some("tls: private key type does not match public key type"),
                "{cert} + {key}"
            );
        }
    }

    #[test]
    fn a_same_family_but_unrelated_key_gets_gos_value_sentence() {
        // `tls.go:340`/`:349`: same family, `PublicKey.Equal` says no. The EC pair is
        // cross-curve, which Go also answers with the value sentence.
        assert_eq!(
            pair_error(&der("rsa.pem"), &der("rsa-2049-key.pem")).as_deref(),
            Some("tls: private key does not match public key")
        );
        assert_eq!(
            pair_error(&der("p256.pem"), &der("p384-sec1-key.pem")).as_deref(),
            Some("tls: private key does not match public key")
        );
    }

    #[test]
    fn the_pairing_failure_is_a_bare_client_tls_config_error() {
        // Upstream returns `ClientTLSConfig`'s error before the Ping, so a pair that
        // does not belong together must be refused while the material is loaded — not
        // inside the dial, where go-redis would wrap it in `unable to connect to redis`.
        let error = pair_load_error(&der("rsa.pem"), &der("rsa-2049-key.pem"));
        assert_eq!(
            error,
            "could not load client key pair: tls: private key does not match public key"
        );
        let error = pair_load_error(&der("rsa.pem"), &der("p256-key.pem"));
        assert_eq!(
            error,
            "could not load client key pair: tls: private key type does not match public key type"
        );
    }

    #[test]
    fn a_pair_this_walk_cannot_classify_is_left_to_the_crypto_layer() {
        // Go sniffs the DER and ignores the PEM label; this walk reads the label the way
        // rustls-pemfile does, so a block whose DER does not match its label is not
        // guessed at — it is deferred rather than refused with an invented sentence.
        let mislabelled = std::fs::read_to_string(der("p256-key.pem"))
            .unwrap()
            .replace("BEGIN PRIVATE KEY", "BEGIN EC PRIVATE KEY")
            .replace("END PRIVATE KEY", "END EC PRIVATE KEY");
        assert_eq!(
            check_key_pair_der(
                std::fs::read(der("p256.pem")).unwrap().as_slice(),
                mislabelled.as_bytes()
            ),
            Ok(())
        );
    }

    /// Rejection text for a client key pair, without needing `TlsMaterial: Debug`.
    fn pair_load_error(cert_file: &str, key_file: &str) -> String {
        match tls_material(cert_file, key_file) {
            Ok(_) => {
                panic!("expected the client key pair to be rejected: {cert_file} + {key_file}")
            }
            Err(err) => err,
        }
    }

    #[test]
    fn a_key_file_without_a_cert_file_is_never_opened() {
        // `xtls/tls.go:39` guards on `c.ClientCertFile != ""` alone, so a
        // `client_key_file` with no certificate is accepted upstream and the client
        // simply sends no certificate. Loading the key first would turn a
        // configuration Go accepts into a dial failure here.
        let material = tls_material("", &fixture("server-key.pem")).expect("upstream accepts this");
        assert!(material.client_cert_pem.is_none());
        assert!(material.client_key_pem.is_none());
        assert!(
            session_tls(&tls_cfg(
                "localhost:6379",
                TlsConfig {
                    enabled: true,
                    client_key_file: fixture("server-key.pem"),
                    ..Default::default()
                },
            ))
            .expect("no client cert, dial proceeds")
            .is_some(),
            "TLS is still requested"
        );
    }

    #[test]
    fn a_cert_file_with_a_missing_key_file_reproduces_the_path_error() {
        let err = pair_load_error(&fixture("server.pem"), "/nonexistent/client-key.pem");
        assert_eq!(
            err,
            "could not load client key pair: open /nonexistent/client-key.pem: no such file or directory"
        );
    }

    #[test]
    fn switched_cert_and_key_inputs_reproduce_go_switched_message() {
        let err = pair_load_error(&fixture("server-key.pem"), &fixture("server.pem"));
        assert_eq!(
            err,
            "could not load client key pair: tls: failed to find certificate PEM data in \
             certificate input, but did find a private key; PEM inputs may have been switched"
        );
    }

    #[test]
    fn a_non_pem_cert_input_reproduces_the_no_pem_data_message() {
        let err = pair_load_error(&fixture("README.md"), &fixture("server-key.pem"));
        assert_eq!(
            err,
            "could not load client key pair: tls: failed to find any PEM data in certificate input"
        );
    }

    #[test]
    fn a_cert_input_with_only_other_blocks_lists_the_skipped_types() {
        let err = check_key_pair_pem(
            pem("PUBLIC KEY", "MIIB").as_bytes(),
            pem("PRIVATE KEY", "AABB").as_bytes(),
        )
        .unwrap_err();
        assert_eq!(
            err,
            "tls: failed to find \"CERTIFICATE\" PEM block in certificate input after skipping \
             PEM blocks of the following types: [PUBLIC KEY]"
        );
    }

    #[test]
    fn a_key_input_with_a_cert_block_reproduces_the_certificate_instead_message() {
        let err = check_key_pair_pem(
            pem("CERTIFICATE", "AABB").as_bytes(),
            pem("CERTIFICATE", "CCDD").as_bytes(),
        )
        .unwrap_err();
        assert_eq!(
            err,
            "tls: found a certificate rather than a key in the PEM for the private key"
        );
    }

    #[test]
    fn a_key_input_with_no_private_key_block_lists_every_skipped_type_in_order() {
        // Go stops at the first usable private key and quotes only what it skipped
        // before that point, joined the way `%v` renders a []string.
        let key = format!(
            "{}{}",
            pem("PUBLIC KEY", "AAAA"),
            pem("CERTIFICATE", "BBBB")
        );
        let err =
            check_key_pair_pem(pem("CERTIFICATE", "AABB").as_bytes(), key.as_bytes()).unwrap_err();
        assert_eq!(
            err,
            "tls: failed to find PEM block with type ending in \"PRIVATE KEY\" in key input \
             after skipping PEM blocks of the following types: [PUBLIC KEY CERTIFICATE]"
        );
        let err =
            check_key_pair_pem(pem("CERTIFICATE", "AABB").as_bytes(), b"no pem here").unwrap_err();
        assert_eq!(err, "tls: failed to find any PEM data in key input");
    }

    #[test]
    fn an_unterminated_pem_block_is_skipped_like_pem_decode_leaves_it_out() {
        assert_eq!(
            pem_block_types(pem("CERTIFICATE", "AABB").as_bytes()),
            vec!["CERTIFICATE".to_string()]
        );
        assert_eq!(
            pem_block_types("-----BEGIN CERTIFICATE-----\nAABB\n".as_bytes()),
            Vec::<String>::new()
        );
    }

    #[test]
    fn an_ec_private_key_suffix_is_a_usable_key_block() {
        // Go accepts `EC PRIVATE KEY` (any type ending in "PRIVATE KEY").
        assert!(
            check_key_pair_pem(
                pem("CERTIFICATE", "AABB").as_bytes(),
                pem("EC PRIVATE KEY", "CCDD").as_bytes()
            )
            .is_ok()
        );
    }

    #[test]
    fn a_mismatched_pair_is_refused_at_load_and_by_the_crypto_layer() {
        // The PEM shape of this pair is valid, so `check_key_pair_pem` lets it through and
        // the refusal comes from the DER comparison at material-load time — which is where
        // upstream's `ClientTLSConfig` failure belongs, before the Ping. rustls reaches the
        // same verdict later and in its own words; that is kept as a second witness that the
        // pair really is inconsistent, not as the user-visible text.
        let cert = fixture("server.pem");
        let key = fixture("mismatch-key.pem");
        assert!(
            check_key_pair_pem(
                &std::fs::read(&cert).unwrap(),
                &std::fs::read(&key).unwrap()
            )
            .is_ok()
        );
        assert_eq!(
            pair_load_error(&cert, &key),
            "could not load client key pair: tls: private key does not match public key"
        );
        let err = session_client_config(&SessionTls {
            insecure: false,
            server_name: String::new(),
            roots_pem: None,
            client_cert_pem: Some(std::fs::read(&cert).unwrap()),
            client_key_pem: Some(std::fs::read(&key).unwrap()),
        })
        .err()
        .expect("an inconsistent pair must not yield a usable TLS config either");
        assert!(err.contains("KeyMismatch"), "rustls' own words: {err}");
    }

    #[test]
    fn a_valid_client_pair_is_carried_into_the_dial_material() {
        let material = tls_material(&fixture("server.pem"), &fixture("server-key.pem"))
            .expect("a real pair loads");
        assert!(
            material
                .client_cert_pem
                .as_ref()
                .is_some_and(|p| !p.is_empty())
        );
        assert!(
            material
                .client_key_pem
                .as_ref()
                .is_some_and(|p| !p.is_empty())
        );
    }
}
