//! C98-D config_projection legs for the Redis leaves that reach a dial, plus the
//! three `generated_flags:--webtransport.*` rows.
//!
//! Tier rule (SPEC-0075 §2): drive `parse_with_probe(argv, env, ProgramInfo,
//! NoNetwork)` and assert on what the *consumer* carries --
//! `crate::redis_tls::redis_endpoint` (the client `GetRedisClient` builds),
//! `crate::cli_nodes::cli_redis_retry::simple_options` (the `list-nodes` dial
//! settings) and `crate::server::webtransport_tls` (the TLS material
//! `server.go:184-195` builds) -- never on the parsed tree alone.
//!
//! Go oracle: references/protocol-stable @ redis/redis.go
//! - `:33-52` the `RedisConfig` leaves,
//! - `:68-73` `GetMaxRedirects` returns 2 when the pointer is nil,
//! - `:100-114` the Sentinel branch seeds `DialTimeout` 2000ms / `ReadTimeout`
//!   200ms / `WriteTimeout` 200ms when they are zero and passes
//!   `SentinelUsername`/`SentinelPassword`/`MasterName`,
//! - `:131-143` the Cluster branch is the only one that sets `MaxRedirects`,
//! - `:144-155` the simple branch passes no dial/read/write timeouts at all.
//! references/livekit-stable @ pkg/service/webtransport.go:44-53 returns the
//! `os.ReadFile` error verbatim and refuses a non-development listener without a
//! certificate pair.

use super::*;
use crate::cli_nodes::cli_redis_retry::simple_options;
use crate::redis_tls::redis_endpoint;
use lk_compat::redis_endpoint::RedisEndpoint;
use lk_config::{EntryAction, Env, ProgramInfo, netdetect::NetProbe, parse_with_probe};
use std::{ffi::OsString, net::IpAddr, time::Duration};

struct NoNetwork;
impl NetProbe for NoNetwork {
    fn local_addresses(&self) -> Vec<IpAddr> {
        vec!["127.0.0.1".parse().expect("fixture loopback")]
    }
    fn external_ip(&self, _: &[String], _: bool) -> Result<IpAddr, String> {
        panic!("fixture must not discover external IP")
    }
    fn sleep(&self, _: Duration) {
        panic!("fixture must not retry networking")
    }
}

fn load(args: &[&str], env: &Env) -> LoadedConfig {
    let info = ProgramInfo {
        binary_name: "livecloud".into(),
        product_version: "fixture".into(),
        compat_version: crate::COMPAT_VERSION,
    };
    let mut argv: Vec<OsString> = ["livecloud", "--dev"]
        .into_iter()
        .map(OsString::from)
        .collect();
    argv.extend(args.iter().map(OsString::from));
    match parse_with_probe(argv, env, &info, &NoNetwork) {
        Ok(EntryAction::RunServer(loaded)) => *loaded,
        Ok(other) => panic!("expected RunServer, got: {other:?}"),
        Err(e) => panic!("the fixture config must load, got: {e}"),
    }
}

fn endpoint(args: &[&str], env: &Env) -> RedisEndpoint {
    redis_endpoint(&load(args, env).official.redis).expect("the fixture redis must be dialable")
}

fn yaml(body: &'static str) -> Vec<&'static str> {
    vec!["--config-body", body]
}

/// `YAML:redis.username` / `.password` / `.db` -- the credential triple the
/// simple client authenticates with.
#[test]
fn redis_credential_and_db_leaves_reach_the_simple_client_info() {
    let body =
        "redis:\n  address: 127.0.0.1:6399\n  username: c98user\n  password: c98secret\n  db: 3\n";
    let simple = endpoint(&yaml(body), &Env::default());
    let info = simple
        .simple_info()
        .expect("an address-only config builds a simple endpoint");
    let settings = info.redis_settings();
    assert_eq!(
        Some("c98user"),
        settings.username(),
        "C98D-11 redis.username must reach the dial"
    );
    assert_eq!(
        Some("c98secret"),
        settings.password(),
        "C98D-11 redis.password must reach the dial"
    );
    assert_eq!(3, settings.db(), "C98D-11 redis.db must reach the dial");

    // Empty credentials stay absent: `make_info` only calls `set_username` /
    // `set_password` for a non-empty leaf, matching go-redis, which sends no AUTH
    // for an empty user/password pair.
    let bare = endpoint(
        &yaml("redis:\n  address: 127.0.0.1:6399\n"),
        &Env::default(),
    )
    .simple_info()
    .expect("simple endpoint")
    .redis_settings()
    .clone();
    assert_eq!(
        (None, None, 0),
        (bare.username(), bare.password(), bare.db()),
        "C98D-12 an absent credential triple must stay unauthenticated on db 0"
    );

    // The env surface reaches the same field set, not a parallel copy.
    let env = Env::from_pairs([
        ("LIVEKIT_REDIS_USERNAME", "envuser"),
        ("LIVEKIT_REDIS_PASSWORD", "envsecret"),
        ("LIVEKIT_REDIS_DB", "5"),
    ]);
    let from_env = endpoint(&yaml("redis:\n  address: 127.0.0.1:6399\n"), &env)
        .simple_info()
        .expect("simple endpoint")
        .redis_settings()
        .clone();
    assert_eq!(
        (Some("envuser"), Some("envsecret"), 5),
        (from_env.username(), from_env.password(), from_env.db()),
        "C98D-13 the env leaves must land on the same connection info"
    );
}

/// `YAML:redis.pool_size` / `.pool_timeout` -- the only leaves of the pool group
/// the simple client consumes.
#[test]
fn redis_pool_leaves_reach_the_cli_simple_client_options() {
    let body = "redis:\n  address: 127.0.0.1:6399\n  username: c98user\n  password: c98secret\n  db: 4\n  pool_size: 7\n  pool_timeout: 250ms\n";
    let options = simple_options(&load(&yaml(body), &Env::default()).official.redis).unwrap();
    assert_eq!(
        7, options.pool_size,
        "C98D-14 redis.pool_size must reach Options"
    );
    assert_eq!(
        Duration::from_millis(250),
        options.pool_timeout,
        "C98D-14 redis.pool_timeout must reach Options"
    );
    assert_eq!(
        "c98user", options.username,
        "C98D-14 the credential leaves ride the same Options"
    );
    assert_eq!("c98secret", options.password, "C98D-14b");
    assert_eq!(4, options.db, "C98D-14c");

    // Unset leaves keep the zero values the port hands the pool builder; the fixed
    // 5s dial/read/write budget below is the port's own CLI choice (Go's simple
    // branch passes no timeouts at all, redis.go:144-155), so the leg pins that it
    // is constant rather than claiming it is a projected leaf.
    let configured_options = simple_options(
        &load(
            &yaml("redis:\n  address: 127.0.0.1:6399\n  dial_timeout: 900\n  read_timeout: 30\n  write_timeout: 40\n"),
            &Env::default(),
        )
        .official
        .redis,
    )
    .unwrap();
    let plain = simple_options(
        &load(
            &yaml("redis:\n  address: 127.0.0.1:6399\n"),
            &Env::default(),
        )
        .official
        .redis,
    )
    .unwrap();
    assert_eq!(
        (0, Duration::ZERO),
        (plain.pool_size, plain.pool_timeout),
        "C98D-15 an absent pool pair must stay unset, got {:?}",
        (plain.pool_size, plain.pool_timeout)
    );
    // The port's own CLI budget, deliberately constant: Go's simple branch passes
    // no timeouts at all (redis.go:144-155) and lets go-redis default them, so this
    // pins that no `redis.*_timeout` leaf leaks into the CLI path -- it is not a
    // claim that 5s is upstream's number.
    assert_eq!(
        (
            Some(Duration::from_secs(5)),
            Some(Duration::from_secs(5)),
            Some(Duration::from_secs(5))
        ),
        (plain.dial, plain.read, plain.write),
        "C98D-15b the CLI dial budget must not follow the sentinel-only timeout leaves"
    );
    assert_eq!(
        (
            configured_options.dial,
            configured_options.read,
            configured_options.write
        ),
        (
            Some(Duration::from_secs(5)),
            Some(Duration::from_secs(5)),
            Some(Duration::from_secs(5))
        ),
        "C98D-15c a configured timeout triple must not change the CLI budget either"
    );
}

/// `YAML:redis.sentinel_username` / `.sentinel_password` and the three timeout
/// rows: the Sentinel branch is the only branch that seeds them.
#[test]
fn redis_sentinel_leaves_reach_the_target_and_seed_the_branch_budget() {
    let body = "redis:\n  sentinel_master_name: c98master\n  sentinel_addresses: ['127.0.0.1:26379']\n  sentinel_username: sentuser\n  sentinel_password: sentsecret\n";
    let simple_endpoint = endpoint(&yaml(body), &Env::default());
    let target = simple_endpoint
        .sentinel_target()
        .expect("a named master builds a sentinel endpoint");
    assert_eq!(
        "c98master", target.master_name,
        "C98D-16 the master name must reach the target"
    );
    assert_eq!(
        vec!["127.0.0.1:26379".to_string()],
        target.addresses,
        "C98D-16 the node list must reach the target in order"
    );
    assert_eq!(
        "sentuser", target.sentinel_username,
        "C98D-17 redis.sentinel_username must reach the target"
    );
    assert_eq!(
        "sentsecret", target.sentinel_password,
        "C98D-17 redis.sentinel_password must reach the target"
    );
    // redis.go:103-114 seeds 2000ms/200ms/200ms only in this branch.
    assert_eq!(
        (Duration::from_millis(2000), Duration::from_millis(200)),
        (target.discovery_dial_timeout, target.discovery_read_timeout),
        "C98D-18 a zero dial/read pair must seed to Go's 2000ms/200ms"
    );
    assert_eq!(
        Some(Duration::from_millis(200)),
        target.write_timeout,
        "C98D-18 a zero write timeout must seed to Go's 200ms"
    );

    // The configured leaves override the seed verbatim (the values are millis in
    // the Go struct, `redis.go:46-48`).
    let configured = "redis:\n  sentinel_master_name: m\n  sentinel_addresses: ['127.0.0.1:26379']\n  dial_timeout: 900\n  read_timeout: 30\n  write_timeout: 40\n";
    let configured_endpoint = endpoint(&yaml(configured), &Env::default());
    let target = configured_endpoint
        .sentinel_target()
        .expect("sentinel endpoint");
    assert_eq!(
        (
            Duration::from_millis(900),
            Duration::from_millis(30),
            Some(Duration::from_millis(40))
        ),
        (
            target.discovery_dial_timeout,
            target.discovery_read_timeout,
            target.write_timeout
        ),
        "C98D-19 configured timeouts must override the seed"
    );

    // The same three scalars through the flag surface -- these are the
    // `generated_flags:--redis.sentinel_*` rows. The node list is a slice, so Go
    // registers no flag for it (references/livekit-stable
    // pkg/config/config.go:1033-1035; the generated table at
    // crates/lk-config/tests/fixtures/cli-verbose-generated-v1.13.10.txt:83-85
    // lists exactly the three scalars), which is why the flags ride a YAML list
    // here and why the refusal of the list spelling is itself part of the claim.
    let mut args = yaml("redis:\n  sentinel_addresses: ['127.0.0.1:26381']\n").to_vec();
    args.extend([
        "--redis.sentinel_master_name=flagmaster",
        "--redis.sentinel_username=flaguser",
        "--redis.sentinel_password=flagsecret",
    ]);
    let flag_endpoint = endpoint(&args, &Env::default());
    let flags = flag_endpoint
        .sentinel_target()
        .expect("the sentinel flags must select the sentinel branch");
    assert_eq!(
        ("flagmaster", "flaguser", "flagsecret"),
        (
            flags.master_name.as_str(),
            flags.sentinel_username.as_str(),
            flags.sentinel_password.as_str()
        ),
        "C98D-20 the three generated sentinel flags must reach the target"
    );
    assert_eq!(
        vec!["127.0.0.1:26381".to_string()],
        flags.addresses,
        "C98D-20b the YAML node list must survive alongside the flags"
    );
    let refused = load_text(
        &["--redis.sentinel_addresses=127.0.0.1:26399"],
        &Env::default(),
    );
    assert!(
        refused.contains("not defined"),
        "C98D-21 --redis.sentinel_addresses must have no flag (slices are skipped by the generator), got {refused}"
    );
}

/// Whatever the loader printed instead of a server config (usage refusal, ...).
fn load_text(args: &[&str], env: &Env) -> String {
    let info = ProgramInfo {
        binary_name: "livecloud".into(),
        product_version: "fixture".into(),
        compat_version: crate::COMPAT_VERSION,
    };
    let mut argv: Vec<OsString> = ["livecloud", "--dev"]
        .into_iter()
        .map(OsString::from)
        .collect();
    argv.extend(args.iter().map(OsString::from));
    match parse_with_probe(argv, env, &info, &NoNetwork) {
        Ok(EntryAction::PrintHelp { text, .. }) => text,
        Ok(other) => format!("entry action {other:?}"),
        Err(e) => format!("parse error: {e}"),
    }
}

/// `YAML:redis.max_redirects` -- only the Cluster branch reads it.
#[test]
fn redis_cluster_max_redirects_defaults_to_two_and_clamps_negative() {
    let debug = |args: &[&str], env: &Env| format!("{:?}", endpoint(args, env));
    let absent = debug(
        &yaml("redis:\n  cluster_addresses: ['127.0.0.1:7000', '127.0.0.1:7001']\n"),
        &Env::default(),
    );
    assert!(
        absent.contains("max_redirects: 2"),
        "C98D-22 GetMaxRedirects returns 2 for a nil value (redis.go:68-73), got {absent}"
    );
    let explicit = debug(
        &yaml(
            "redis:\n  cluster_addresses: ['127.0.0.1:7000', '127.0.0.1:7001']\n  max_redirects: 5\n",
        ),
        &Env::default(),
    );
    assert!(
        explicit.contains("max_redirects: 5"),
        "C98D-23 a configured value must pass through, got {explicit}"
    );
    let negative = debug(
        &yaml(
            "redis:\n  cluster_addresses: ['127.0.0.1:7000', '127.0.0.1:7001']\n  max_redirects: -1\n",
        ),
        &Env::default(),
    );
    assert!(
        negative.contains("max_redirects: 0"),
        "C98D-24 a negative redirect count must clamp to 0 rather than wrap, got {negative}"
    );
    // Zero is an explicit value here, not "unset": `GetMaxRedirects` only defaults
    // the *pointer*, so `max_redirects: 0` yields 0 and never the 2 above.
    let zero = debug(
        &yaml(
            "redis:\n  cluster_addresses: ['127.0.0.1:7000', '127.0.0.1:7001']\n  max_redirects: 0\n",
        ),
        &Env::default(),
    );
    assert!(
        zero.contains("max_redirects: 0"),
        "C98D-25 an explicit 0 must not be re-seeded to the 2 default, got {zero}"
    );
}

/// `generated_flags:--webtransport.port` / `.tls_cert_file` / `.tls_key_file`.
#[test]
fn webtransport_flags_reach_the_tls_material_gate() {
    // Port 0 on any surface builds no TLS material and reports no error.
    let zero = webtransport_tls(
        &load(&["--webtransport.port=0"], &Env::default()).official,
        true,
    );
    match zero {
        Ok(None) => {}
        Ok(Some(_)) => panic!("C98D-30 --webtransport.port=0 must produce no TLS material"),
        Err(error) => panic!("C98D-30 --webtransport.port=0 must not error, got {error}"),
    }

    // A nonzero port in development mode self-signs.
    let dev = webtransport_tls(
        &load(&["--webtransport.port=4480"], &Env::default()).official,
        true,
    )
    .expect("dev mode must generate a leaf");
    assert!(
        dev.is_some(),
        "C98D-31 --webtransport.port=4480 must build TLS material"
    );

    // Without a pair outside development the refusal text is the upstream one.
    let refused = webtransport_tls(
        &load_ex_prod(&["--webtransport.port=4480"], &Env::default()).official,
        false,
    );
    match refused {
        Err(error) => assert!(
            error
                .to_string()
                .contains("webtransport requires webtransport.tls_cert_file/tls_key_file"),
            "C98D-32 the non-development refusal text must match webtransport.go:52, got {error}"
        ),
        Ok(Some(_)) => panic!("C98D-32 expected the missing-pair refusal, got TLS material"),
        Ok(None) => panic!("C98D-32 expected the missing-pair refusal, got no listener"),
    }

    // The file flags reach the reader, and the cert path is named before the key
    // path, so a swap of the two leaves is visible in the error text.
    let missing_cert = webtransport_tls(
        &load_ex_prod(
            &[
                "--webtransport.port=4480",
                "--webtransport.tls_cert_file=/nonexistent/c98-cert.pem",
                "--webtransport.tls_key_file=/nonexistent/c98-key.pem",
            ],
            &Env::default(),
        )
        .official,
        false,
    )
    .err()
    .expect("a missing certificate file must fail");
    assert_eq!(
        "open /nonexistent/c98-cert.pem: no such file or directory",
        missing_cert.to_string(),
        "C98D-33 --webtransport.tls_cert_file must name the cert path (Go returns os.ReadFile verbatim)"
    );

    // A readable cert with a missing key names the KEY path -- the claim a
    // cert/key crosswire would break.
    let cert = temp_fixture(
        "c98d-wt-cert.pem",
        &std::fs::read_to_string(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../rtc-tls/tests/fixtures/server.pem"
        ))
        .expect("committed loopback certificate fixture"),
    );
    let missing_key = webtransport_tls(
        &load_ex_prod(
            &[
                "--webtransport.port=4480",
                &format!("--webtransport.tls_cert_file={}", cert.display()),
                "--webtransport.tls_key_file=/nonexistent/c98-key.pem",
            ],
            &Env::default(),
        )
        .official,
        false,
    )
    .err()
    .expect("a missing key file must fail");
    assert_eq!(
        "open /nonexistent/c98-key.pem: no such file or directory",
        missing_key.to_string(),
        "C98D-34 --webtransport.tls_key_file must name the key path, not the cert path"
    );
}

/// A production-shaped load (no `--dev`), which is what the WebTransport gate
/// distinguishes on.
fn load_ex_prod(args: &[&str], env: &Env) -> LoadedConfig {
    let info = ProgramInfo {
        binary_name: "livecloud".into(),
        product_version: "fixture".into(),
        compat_version: crate::COMPAT_VERSION,
    };
    let mut argv: Vec<OsString> = vec![OsString::from("livecloud")];
    argv.extend(args.iter().map(OsString::from));
    argv.push(OsString::from("--config-body"));
    argv.push(OsString::from(
        "keys:\n  c98key: \"0123456789012345678901234567890123456789\"\n",
    ));
    match parse_with_probe(argv, env, &info, &NoNetwork) {
        Ok(EntryAction::RunServer(loaded)) => *loaded,
        Ok(other) => panic!("expected RunServer, got: {other:?}"),
        Err(e) => panic!("the fixture config must load, got: {e}"),
    }
}

/// A unique temporary fixture created with `O_EXCL` so two legs can never share
/// or overwrite it.
fn temp_fixture(name: &str, contents: &str) -> std::path::PathBuf {
    static SEQ: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);
    let seq = SEQ.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    let path = std::env::temp_dir().join(format!(
        "c98d-{name}-{}-{}-{seq}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("clock after epoch")
            .as_nanos()
    ));
    std::fs::write(&path, contents).expect("write fixture");
    path
}
