//! Scoped simple-client phase/retry implementation. Sentinel/cluster remain
//! independent unclosed consumers. TLS (`redis.use_tls`/`redis.tls`) is dialed by
//! this same simple client: the handshake runs inside the emulated go-redis dial,
//! so attempts, `redis:` diagnostics and error texts are shared with plaintext
//! (SPEC-0074 C10, SPEC-0076 workline A target 2).
use super::{
    NodeListError,
    cli_redis_session::{Options, Session},
    cli_redis_wire::{self as wire, Fault},
};
use lk_config::official::RedisConfig;
use std::time::Duration;
fn simple_options(settings: &RedisConfig) -> Result<Options, String> {
    Ok(Options {
        username: settings.username.clone(),
        password: settings.password.clone(),
        db: settings.db,
        pool_size: settings.pool_size,
        pool_timeout: settings.pool_timeout.to_std(),
        retries: 3,
        dial: Some(Duration::from_secs(5)),
        read: Some(Duration::from_secs(5)),
        write: Some(Duration::from_secs(5)),
        tls: crate::redis_tls::session_tls(settings)?,
    })
}

/// `redis.GetRedisClient` for the simple client: NewUniversalClient + Ping.
/// A TLS request (`redis.use_tls` / `redis.tls.enabled`) dials the same way, with
/// the handshake inside the go-redis dial so its attempts, diagnostics and error
/// texts stay identical to the plaintext path (SPEC-0076 workline A target 2).
async fn connect(settings: &RedisConfig) -> Result<Session, NodeListError> {
    let options = simple_options(settings).map_err(bare_failure)?;
    // The mode line belongs to building the client (`redis.go:145`), which is what
    // `GetRedisClient` is; `redis_endpoint` logs the same line for the clients that
    // go through it. Order matters: TLS material first, log second, dial last.
    crate::redis_tls::log_redis_mode(settings);
    ping_session(settings, options).await
}

/// The dial and `Ping` of the simple client over already-loaded TLS material.
async fn ping_session(settings: &RedisConfig, options: Options) -> Result<Session, NodeListError> {
    let mut session = Session::new(settings.address.clone(), options)?;
    session
        .query(wire::command("ping", &[]))
        .await
        .map_err(|e| command_error("unable to connect to redis: ", e))?;
    Ok(session)
}

/// Server start (`InitializeServer` -> `createRedisClient`): the same
/// GetRedisClient Ping over the endpoint the caller already built — `redis_endpoint`
/// loaded the TLS material and logged the mode, so this must not log a second time.
/// Upstream branches in the order Sentinel, Cluster, Address
/// (`references/protocol-stable/redis/redis.go:100-155`), so a cluster-only
/// configuration pings the cluster seeds instead of an empty `redis.address`.
/// The two non-simple branches differ in their timeout budget on purpose: the
/// Sentinel branch gets `redis.dial_timeout`/`read_timeout` (defaulting to 2s and
/// 200ms), while the Cluster branch passes no timeouts at all and therefore runs
/// on go-redis' own defaults, as upstream does. The budget follows that *branch*,
/// not the client the branch ended up building: with an empty `sentinel_master_name`
/// upstream still applies the Sentinel defaults to the simple/cluster client.
pub(crate) async fn ping_with(
    endpoint: &lk_compat::redis_endpoint::RedisEndpoint,
    settings: &RedisConfig,
) -> Result<(), NodeListError> {
    if !settings.sentinel_addresses.is_empty() {
        let (dial, read) = lk_compat::redis_endpoint::sentinel_timeouts(
            settings.dial_timeout,
            settings.read_timeout,
        );
        return ping_existing(endpoint, dial, read).await.map(drop);
    }
    if !settings.cluster_addresses.is_empty() {
        // `options.go:485-532`: an unset dial/read budget is go-redis' own 5s.
        return ping_existing(endpoint, Duration::from_secs(5), Duration::from_secs(5))
            .await
            .map(drop);
    }
    let options = simple_options(settings).map_err(bare_failure)?;
    ping_session(settings, options).await.map(drop)
}

/// `GetRedisClient` returns `xtls.ClientTLSConfig`'s error **before** the Ping, so
/// the CLI prints it with no `unable to connect to redis:` wrapper.
fn bare_failure(reason: String) -> NodeListError {
    NodeListError::OrdinaryBytes(reason.into_bytes())
}

/// The `Ping` of `GetRedisClient` over an endpoint the caller already built: the
/// Sentinel failover client, the Cluster client or the simple client chosen by
/// `redis_endpoint` (SPEC-0074 C11/C12).
///
/// Two failure shapes stay distinct here because upstream's are distinct: material
/// `xtls.ClientTLSConfig` cannot load is returned **before** the Ping and therefore
/// prints bare, while a dial that reached the wire is wrapped in
/// `unable to connect to redis:` (`references/protocol-stable/redis/redis.go:158-161`).
async fn ping_existing(
    endpoint: &lk_compat::redis_endpoint::RedisEndpoint,
    dial: Duration,
    read: Duration,
) -> Result<lk_compat::redis_endpoint::SharedConnection, NodeListError> {
    // SPEC-0074 §2.5-B: the aggregate Sentinel-unreachable family must surface
    // as go-redis' bare line (`redis: all sentinels … : <joined attempts>`),
    // not redis-rs' `"{trace} - Client: {detail}"` rendering.
    let fail = |reason: String| {
        NodeListError::OrdinaryBytes(format!("unable to connect to redis: {reason}").into_bytes())
    };
    let config = redis::AsyncConnectionConfig::new()
        .set_connection_timeout(Some(dial))
        .set_response_timeout(Some(read));
    // `connect`, not `dial`: a Cluster endpoint cannot yield a `MultiplexedConnection`
    // at all, so dialing one fails with `InvalidClientConfig` before a single byte is
    // written, even though `IsConfigured()` accepts a cluster-only configuration
    // (`redis.go:48-64`, `:131-143`). `SharedConnection::connect` is what every
    // server-side consumer already uses for the same endpoint.
    let connection = endpoint
        .connect(&config)
        .await
        .map_err(|e| fail(lk_compat::redis_endpoint::go_error_text(&e)))?;
    wire::command("ping", &[])
        .query_async::<()>(&mut connection.begin())
        .await
        .map_err(|e| fail(lk_compat::redis_endpoint::go_error_text(&e)))?;
    Ok(connection)
}

pub(crate) async fn read_nodes(settings: &RedisConfig) -> Result<Vec<Vec<u8>>, NodeListError> {
    // SPEC-0074 C10/C11/C12: Sentinel and Cluster node listing dial through the
    // same endpoint the server uses (Sentinel master discovery, cluster slot
    // routing). A TLS request is *not* an endpoint case any more: the simple
    // client's handshake belongs to the go-redis dial emulated below, which is
    // what makes its attempts, `redis:` diagnostics and error texts comparable to
    // the official binary at all (SPEC-0076 workline A target 2).
    if !settings.sentinel_addresses.is_empty() || !settings.cluster_addresses.is_empty() {
        return read_nodes_endpoint(settings).await;
    }
    let mut session = connect(settings).await?;
    let values = session
        .query(wire::command("hvals", &["nodes"]))
        .await
        .map_err(|e| command_error("could not list nodes: ", e))?;
    redis::from_redis_value(values)
        .map_err(|e| command_error("could not list nodes: ", Fault::Protocol(e.to_string())))
}

/// `HVALS nodes` over the endpoint transport (Sentinel or Cluster), mirroring the
/// plaintext session's `unable to connect to redis` / `could not list nodes` prefixes
/// and the same bare-vs-wrapped split as [`ping_existing`].
async fn read_nodes_endpoint(settings: &RedisConfig) -> Result<Vec<Vec<u8>>, NodeListError> {
    let endpoint = crate::redis_tls::redis_endpoint(settings).map_err(bare_failure)?;
    let (dial, read) = if settings.sentinel_addresses.is_empty() {
        (Duration::from_secs(5), Duration::from_secs(5))
    } else {
        lk_compat::redis_endpoint::sentinel_timeouts(settings.dial_timeout, settings.read_timeout)
    };
    let connection = ping_existing(&endpoint, dial, read).await?;
    let values: redis::Value = wire::command("hvals", &["nodes"])
        .query_async(&mut connection.begin())
        .await
        .map_err(|e| {
            command_error(
                "could not list nodes: ",
                Fault::Protocol(lk_compat::redis_endpoint::go_error_text(&e)),
            )
        })?;
    redis::from_redis_value(values)
        .map_err(|e| command_error("could not list nodes: ", Fault::Protocol(e.to_string())))
}
fn command_error(prefix: &str, error: Fault) -> NodeListError {
    let mut bytes = prefix.as_bytes().to_vec();
    bytes.extend(error.bytes());
    NodeListError::OrdinaryBytes(bytes)
}
pub(super) fn command_backoff(attempt: u32) -> Duration {
    let min = 10_000_000u64;
    let span = min
        .checked_shl(attempt)
        .filter(|n| *n >= min)
        .unwrap_or(u64::MAX);
    if span == u64::MAX {
        return Duration::from_secs(1);
    }
    Duration::from_nanos(
        min.saturating_add(rand::random_range(0..span))
            .min(1_000_000_000),
    )
}
pub(super) fn pool_diagnostic(cause: &[u8]) {
    let mut message = b"redis: connection pool: failed to dial after 5 attempts: ".to_vec();
    message.extend(cause);
    lk_compat::redis_endpoint::write_redis_diagnostic("pool.go:762", &message)
}
