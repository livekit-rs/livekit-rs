//! SPEC-0074 C11: Redis Sentinel discovery and master failover, porting the
//! observable `go-redis v9.22.0` `sentinelFailover` semantics used by
//! `protocol/redis.GetRedisClient` (`redis.go:100-130`).
//!
//! [`RedisEndpoint`] is the single connection source every server-side Redis
//! consumer uses (service bus, IO store, room registry/catalog, SIP catalog,
//! startup Ping, `list-nodes`). A `Simple` endpoint dials its static address and,
//! like go-redis' `pool.Pool`, re-dials it when a pooled command connection has
//! failed; a `Sentinel` endpoint resolves the current master through the
//! configured Sentinel nodes before every dial (`SENTINEL get-master-addr-by-name`,
//! first-success-wins like upstream's concurrent `MasterAddr` race) and re-dials
//! through that resolution on a dropped command connection, which is how go-redis
//! recovers a pooled connection after `+switch-master` demotes the old master.

use futures_util::stream::{FuturesUnordered, StreamExt};
use redis::aio::{ConnectionLike as AsyncConnectionLike, MultiplexedConnection};
use redis::cluster_async::ClusterConnection;
use redis::{
    AsyncConnectionConfig, ConnectionAddr, ConnectionInfo, ErrorKind, ProtocolVersion, RedisError,
    Value,
};
#[cfg(test)]
#[path = "redis_endpoint/cluster_tls_tests.rs"]
mod cluster_tls_tests;
#[path = "redis_endpoint/cluster_watch.rs"]
mod cluster_watch;
#[path = "redis_endpoint/diagnostics.rs"]
mod diagnostics;
#[path = "redis_endpoint/sentinel_watch.rs"]
mod sentinel_watch;
pub use diagnostics::write_redis_diagnostic;
use sentinel_watch::SentinelEvents;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::Duration;
use tokio::net::TcpStream as TokioTcpStream;

/// Upstream `sentinelFailover.MasterAddr` aggregates every failed Sentinel
/// attempt under this text (`sentinel.go:1051`).
const ALL_SENTINELS_UNREACHABLE: &str =
    "redis: all sentinels specified in configuration are unreachable";

/// Factory producing a Sentinel node's [`ConnectionInfo`] from one of the
/// configured `host:port` addresses (applies the Sentinel-only credentials).
type SentinelInfoFn = dyn Fn(&str) -> Result<ConnectionInfo, String> + Send + Sync;
/// Factory producing the master's [`ConnectionInfo`] from a resolved
/// `host:port` returned by `SENTINEL get-master-addr-by-name`.
type MasterInfoFn = dyn Fn(String) -> Result<ConnectionInfo, String> + Send + Sync;

/// Configuration of one Sentinel-monitored deployment, mirroring
/// `redis.UniversalOptions`' Sentinel fields: node addresses with the
/// Sentinel-only credentials, plus the monitored master name and the
/// credentials applied to the resolved master connection.
#[derive(Clone)]
pub struct SentinelTarget {
    /// `sentinel_master_name` (upstream `MasterName`).
    pub master_name: String,
    /// `sentinel_addresses`, in configured order. Upstream promotes the last
    /// answering Sentinel to the front of the list; the same preference is
    /// kept through [`SentinelTarget::cached_order`].
    pub addresses: Vec<String>,
    /// `sentinel_username`: applied only to Sentinel connections.
    pub sentinel_username: String,
    /// `sentinel_password`: applied only to Sentinel connections.
    pub sentinel_password: String,
    /// Connection settings for Sentinel nodes, including TLS when
    /// `redis.tls`/`redis.use_tls` is active (upstream applies the same
    /// `TLSConfig` to `sentinelOptions`).
    pub sentinel_info: Arc<SentinelInfoFn>,
    /// Master connection settings for a resolved `host:port` address.
    pub master_info: Arc<MasterInfoFn>,
    /// Per-Sentinel discovery budget from `GetRedisClient`'s Sentinel branch
    /// (`redis.go:103-114`: dial 2s by default). Discovery dials deliberately do
    /// not reuse the caller's [`AsyncConnectionConfig`]: that config can bind a
    /// RESP3 push sender meant for the master subscription, which redis-rs
    /// rejects on a RESP2 Sentinel node and whose disconnect pushes would fault
    /// the subscription. go-redis' Sentinel client connections are likewise
    /// plain pooled connections with these budgets.
    pub discovery_dial_timeout: Duration,
    /// Read budget for discovery and resolved master commands (200ms by
    /// default). Zero here represents a negative public read_timeout.
    pub discovery_read_timeout: Duration,
    /// Socket write deadline for Sentinel and resolved-master connections.
    pub write_timeout: Option<Duration>,
}

impl SentinelTarget {
    fn discovery_config(&self) -> AsyncConnectionConfig {
        self.connection_config(AsyncConnectionConfig::new())
    }

    fn connection_config(&self, config: AsyncConnectionConfig) -> AsyncConnectionConfig {
        SentinelBranchTimeouts {
            dial: self.discovery_dial_timeout,
            read: self.discovery_read_timeout,
            write: self.write_timeout,
        }
        .apply(config)
    }
}

#[derive(Clone, Copy)]
struct SentinelBranchTimeouts {
    dial: Duration,
    read: Duration,
    write: Option<Duration>,
}

impl SentinelBranchTimeouts {
    fn from_millis(dial_timeout_ms: i64, read_timeout_ms: i64, write_timeout_ms: i64) -> Self {
        let (dial, read) = sentinel_timeouts(dial_timeout_ms, read_timeout_ms);
        Self {
            dial,
            read,
            write: sentinel_write_timeout(write_timeout_ms),
        }
    }

    fn read(self) -> Option<Duration> {
        (!self.read.is_zero()).then_some(self.read)
    }

    fn apply(self, config: AsyncConnectionConfig) -> AsyncConnectionConfig {
        // The Sentinel factory sets these options before UniversalClient
        // selects Simple, Cluster or Failover. Negative read milliseconds
        // arrive as zero and disable the response deadline.
        config
            .set_connection_timeout(None)
            .set_transport_connection_timeout(Some(self.dial))
            .set_response_timeout(self.read())
            .set_write_timeout(self.write)
    }
}

impl std::fmt::Debug for SentinelTarget {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SentinelTarget")
            .field("master_name", &self.master_name)
            .field("addresses", &self.addresses)
            .field("sentinel_username", &self.sentinel_username)
            .field("sentinel_password", &"<redacted>")
            .finish_non_exhaustive()
    }
}

/// One Redis connection source for every server-side consumer.
#[derive(Clone)]
pub struct RedisEndpoint {
    /// Where to dial: a static address, or a Sentinel-monitored master resolved
    /// per dial. Kept separate from `tls_override` so a TLS SNI override applies
    /// uniformly to whichever branch dials.
    kind: EndpointKind,
    /// SPEC-0074 C10: when `redis.tls.server_name` is set, the TLS SNI and
    /// peer-certificate verification name is decoupled from the dial address
    /// host (redis-rs binds SNI to the address host and cannot express this).
    /// `None` keeps the redis-rs dial path byte-for-byte unchanged for every
    /// already-verified configuration (no `server_name`).
    tls_override: Option<Arc<TlsSniOverride>>,
}

#[derive(Clone)]
enum EndpointKind {
    /// `redis.address` (and TLS variants): the static connection info used by
    /// the simple client, unchanged from the pre-Sentinel implementation.
    /// Boxed so the kind stays `Sentinel`-sized (`ConnectionInfo` is ~232 B).
    Simple(Box<ConnectionInfo>, Option<SentinelBranchTimeouts>),
    /// `redis.sentinel_addresses` non-empty: upstream order is
    /// Sentinel > Cluster > Address, so the master is resolved per dial.
    Sentinel(Arc<SentinelShared>),
    /// `redis.cluster_addresses` non-empty and no Sentinel: upstream order is
    /// Sentinel > Cluster > Address. Uses `ClusterClient` for slot-aware routing.
    Cluster(Arc<ClusterShared>),
}

/// Shared state for a Redis Cluster deployment.
pub struct ClusterShared {
    /// Initial node addresses (parsed ConnectionInfo per address).
    initial_nodes: Vec<ConnectionInfo>,
    /// Maximum number of redirects per command (Go default 2).
    max_redirects: u64,
    /// Whether to force RESP3 on connections.
    resp3: bool,
    timeouts: Option<SentinelBranchTimeouts>,
}

/// SPEC-0074 C10 `xtls.ClientTLSConfig` material for a TLS Redis dial whose SNI
/// /verification name differs from the dial address host. Only the *verified*
/// paths live here: `insecure` and the no-`server_name` TLS paths stay on the
/// redis-rs dial (whose `tls-rustls-insecure` support is already verified),
/// because a Rust `insecure` handshake skips server-name verification and so
/// cannot use an overridden name anyway.
#[derive(Clone)]
pub struct TlsSniOverride {
    /// `xtls.ServerName`: the TLS SNI and certificate-verification identity.
    pub server_name: String,
    /// `xtls.CACertFile` contents (PEM). `None` mirrors Go's empty `RootCAs`
    /// and uses the system trust anchors.
    pub root_pem: Option<Vec<u8>>,
    /// `xtls.ClientCertFile` contents (PEM) for mTLS; `Some` iff `client_key_pem`
    /// is also `Some`.
    pub client_cert_pem: Option<Vec<u8>>,
    /// `xtls.ClientKeyFile` contents (PEM) for mTLS.
    pub client_key_pem: Option<Vec<u8>>,
}

impl std::fmt::Debug for TlsSniOverride {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TlsSniOverride")
            .field("server_name", &self.server_name)
            .field("root_pem", &self.root_pem.as_ref().map(Vec::len))
            .field("mtls", &self.client_cert_pem.is_some())
            .finish_non_exhaustive()
    }
}

/// Shared failover state of one Sentinel deployment: the cached preference
/// order (upstream pushes the answering Sentinel to the front) and the last
/// resolved master address (upstream keeps `c.sentinel` warm between calls).
pub struct SentinelShared {
    target: SentinelTarget,
    events: Arc<SentinelEvents>,
}

impl std::fmt::Debug for RedisEndpoint {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match &self.kind {
            EndpointKind::Simple(info, _) => f
                .debug_struct("RedisEndpoint")
                .field("kind", &"simple")
                .field("addr", info.addr())
                .field("tls_override", &self.tls_override.is_some())
                .finish(),
            EndpointKind::Sentinel(shared) => f
                .debug_struct("RedisEndpoint")
                .field("kind", &"sentinel")
                .field("target", &shared.target)
                .field("tls_override", &self.tls_override.is_some())
                .finish(),
            EndpointKind::Cluster(shared) => f
                .debug_struct("RedisEndpoint")
                .field("kind", &"cluster")
                .field("nodes", &shared.initial_nodes.len())
                .field("max_redirects", &shared.max_redirects)
                .finish(),
        }
    }
}

impl RedisEndpoint {
    pub fn simple(info: ConnectionInfo) -> Self {
        Self {
            kind: EndpointKind::Simple(Box::new(info), None),
            tls_override: None,
        }
    }

    /// SPEC-0074 C10: a `redis.address` TLS endpoint whose SNI and peer-name
    /// verification use `override_`s `server_name` instead of the address host.
    pub fn simple_with_tls_sni(info: ConnectionInfo, override_: TlsSniOverride) -> Self {
        Self {
            kind: EndpointKind::Simple(Box::new(info), None),
            tls_override: Some(Arc::new(override_)),
        }
    }

    /// A one-address Simple client selected by the upstream Sentinel config
    /// branch when the monitored master name is empty.
    pub fn simple_from_sentinel_branch(
        info: ConnectionInfo,
        tls_override: Option<TlsSniOverride>,
        dial_timeout_ms: i64,
        read_timeout_ms: i64,
        write_timeout_ms: i64,
    ) -> Self {
        let timeouts =
            SentinelBranchTimeouts::from_millis(dial_timeout_ms, read_timeout_ms, write_timeout_ms);
        Self {
            kind: EndpointKind::Simple(Box::new(info), Some(timeouts)),
            tls_override: tls_override.map(Arc::new),
        }
    }

    pub fn sentinel(target: SentinelTarget) -> Self {
        Self {
            kind: EndpointKind::Sentinel(Arc::new(SentinelShared {
                target,
                events: Arc::new(SentinelEvents::default()),
            })),
            tls_override: None,
        }
    }

    /// SPEC-0074 C10: a Sentinel endpoint whose Sentinel-node **and** resolved
    /// master dials use `override_`s `server_name` for SNI and peer-name
    /// verification (upstream applies one `TLSConfig.ServerName` to both).
    pub fn sentinel_with_tls_sni(target: SentinelTarget, override_: TlsSniOverride) -> Self {
        Self {
            kind: EndpointKind::Sentinel(Arc::new(SentinelShared {
                target,
                events: Arc::new(SentinelEvents::default()),
            })),
            tls_override: Some(Arc::new(override_)),
        }
    }

    /// SPEC-0074 C12: `redis.cluster_addresses` non-empty and no Sentinel.
    /// Uses `redis::cluster_async::ClusterConnection` for slot-aware routing.
    pub fn cluster(initial_nodes: Vec<ConnectionInfo>, max_redirects: u64) -> Self {
        Self {
            kind: EndpointKind::Cluster(Arc::new(ClusterShared {
                initial_nodes,
                max_redirects,
                resp3: false,
                timeouts: None,
            })),
            tls_override: None,
        }
    }

    /// An ordinary Cluster selected from nonempty Sentinel addresses and an
    /// empty master name retains the options set by that upstream branch.
    pub fn cluster_from_sentinel_branch(
        initial_nodes: Vec<ConnectionInfo>,
        tls_override: Option<TlsSniOverride>,
        dial_timeout_ms: i64,
        read_timeout_ms: i64,
        write_timeout_ms: i64,
    ) -> Self {
        Self {
            kind: EndpointKind::Cluster(Arc::new(ClusterShared {
                initial_nodes,
                // This UniversalOptions branch leaves MaxRedirects unset.
                // go-redis ClusterOptions.init applies its default of three.
                max_redirects: 3,
                resp3: false,
                timeouts: Some(SentinelBranchTimeouts::from_millis(
                    dial_timeout_ms,
                    read_timeout_ms,
                    write_timeout_ms,
                )),
            })),
            tls_override: tls_override.map(Arc::new),
        }
    }

    /// Apply one verified SNI/certificate identity to every Cluster dial,
    /// including discovered nodes and ASK/MOVED destinations.
    pub fn cluster_with_tls_sni(
        initial_nodes: Vec<ConnectionInfo>,
        max_redirects: u64,
        override_: TlsSniOverride,
    ) -> Self {
        let mut endpoint = Self::cluster(initial_nodes, max_redirects);
        endpoint.tls_override = Some(Arc::new(override_));
        endpoint
    }

    /// Dial a fresh multiplexed connection for subscriptions. Sentinel endpoints
    /// resolve the current master first, so every new connection (service-bus
    /// subscriptions, WATCH transactions) observes a completed failover.
    /// For cluster endpoints, use [`RedisEndpoint::dial_cluster`] instead.
    pub async fn dial(
        &self,
        config: &AsyncConnectionConfig,
    ) -> redis::RedisResult<MultiplexedConnection> {
        match &self.kind {
            EndpointKind::Simple(info, timeouts) => {
                let configured = timeouts.map(|timeouts| timeouts.apply(config.clone()));
                dial_with(
                    (**info).clone(),
                    configured.as_ref().unwrap_or(config),
                    self.tls_override.as_deref(),
                )
                .await
            }
            EndpointKind::Sentinel(shared) => {
                shared
                    .events
                    .start(shared.target.clone(), self.tls_override.clone());
                let info = resolve_master_info(shared, self.tls_override.as_deref()).await?;
                let config = shared.target.connection_config(config.clone());
                dial_with(info, &config, self.tls_override.as_deref()).await
            }
            EndpointKind::Cluster(_) => {
                // Cluster endpoints don't return MultiplexedConnection;
                // use dial_cluster() instead. Should not reach here.
                Err(RedisError::from((
                    ErrorKind::InvalidClientConfig,
                    "cluster endpoint: use dial_cluster()",
                )))
            }
        }
    }

    /// Dial a fresh `ClusterConnection` for subscriptions.
    pub async fn dial_cluster(&self) -> redis::RedisResult<ClusterConnection> {
        match &self.kind {
            EndpointKind::Cluster(shared) => {
                load_cluster_state(shared, self.tls_override.as_deref()).await?;
                let client = self.cluster_builder().expect("cluster endpoint").build()?;
                if shared.resp3 {
                    client
                        .get_async_connection_with_config(
                            redis::cluster::ClusterConfig::default()
                                .set_response_timeout(Duration::from_secs(5))
                                .set_connection_timeout(Duration::from_secs(5)),
                        )
                        .await
                } else {
                    client.get_async_connection().await
                }
            }
            _ => Err(RedisError::from((
                ErrorKind::InvalidClientConfig,
                "not a cluster endpoint",
            ))),
        }
    }

    /// Returns a `ClusterClientBuilder` pre-configured with initial nodes and retries,
    /// allowing the caller to add a push_sender before building.
    pub fn cluster_builder(&self) -> Option<redis::cluster::ClusterClientBuilder> {
        match &self.kind {
            EndpointKind::Cluster(shared) => {
                let mut builder =
                    redis::cluster::ClusterClient::builder(shared.initial_nodes.iter().cloned())
                        .retries(shared.max_redirects as u32)
                        .go_certificate_policy(true);
                if let Some(timeouts) = shared.timeouts {
                    builder = builder
                        .node_io_timeouts(timeouts.dial, timeouts.read(), timeouts.write)
                        .overall_response_timeout(None);
                }
                if let Some(tls) = self.tls_override.as_deref() {
                    let client_tls =
                        if tls.client_cert_pem.is_some() || tls.client_key_pem.is_some() {
                            Some(redis::ClientTlsConfig {
                                client_cert: tls.client_cert_pem.clone().unwrap_or_default(),
                                client_key: tls.client_key_pem.clone().unwrap_or_default(),
                            })
                        } else {
                            None
                        };
                    builder = builder
                        .certs(redis::TlsCertificates {
                            root_cert: tls.root_pem.clone(),
                            client_tls,
                        })
                        .tls_server_name(tls.server_name.clone());
                }
                Some(builder)
            }
            _ => None,
        }
    }

    /// Actual Sentinel master changes, shared by RESP2/RESP3 views. Receivers
    /// must capture the epoch before dialing and fence the subscription ACK.
    pub fn master_epoch(&self) -> Option<tokio::sync::watch::Receiver<u64>> {
        match &self.kind {
            EndpointKind::Sentinel(shared) => Some(shared.events.receiver()),
            _ => None,
        }
    }
    pub fn current_master_epoch(&self) -> u64 {
        match &self.kind {
            EndpointKind::Sentinel(shared) => shared.events.epoch(),
            _ => 0,
        }
    }

    /// Whether this is a cluster endpoint.
    pub fn is_cluster(&self) -> bool {
        matches!(&self.kind, EndpointKind::Cluster(_))
    }

    /// The long-lived command connection type used by every consumer.
    pub async fn connect(
        &self,
        config: &AsyncConnectionConfig,
    ) -> redis::RedisResult<SharedConnection> {
        SharedConnection::connect(self.clone(), config).await
    }

    /// A copy of this endpoint whose *master* connections negotiate RESP3, which
    /// the service bus requires for its bounded push subscription. The Sentinel
    /// nodes themselves are untouched (their reply is a plain array).
    pub fn with_resp3(&self) -> RedisEndpoint {
        match &self.kind {
            EndpointKind::Simple(info, timeouts) => RedisEndpoint {
                kind: EndpointKind::Simple(Box::new(resp3((**info).clone())), *timeouts),
                tls_override: self.tls_override.clone(),
            },
            EndpointKind::Sentinel(shared) => {
                let mut target = shared.target.clone();
                let inner = target.master_info.clone();
                target.master_info =
                    Arc::new(move |host_port: String| (inner)(host_port).map(resp3));
                let mut endpoint = RedisEndpoint::sentinel(target);
                if let EndpointKind::Sentinel(new) = &mut endpoint.kind {
                    Arc::get_mut(new).expect("new Sentinel view").events = shared.events.clone();
                }
                endpoint.tls_override = self.tls_override.clone();
                endpoint
            }
            EndpointKind::Cluster(shared) => RedisEndpoint {
                kind: EndpointKind::Cluster(Arc::new(ClusterShared {
                    initial_nodes: shared
                        .initial_nodes
                        .iter()
                        .map(|info| resp3(info.clone()))
                        .collect(),
                    max_redirects: shared.max_redirects,
                    resp3: true,
                    timeouts: shared.timeouts,
                })),
                tls_override: self.tls_override.clone(),
            },
        }
    }
}

/// Force a resolved connection to RESP3 (`redis.Settings.Protocol = 3`).
fn resp3(info: ConnectionInfo) -> ConnectionInfo {
    let settings = info
        .redis_settings()
        .clone()
        .set_protocol(ProtocolVersion::RESP3);
    info.set_redis_settings(settings)
}

impl From<ConnectionInfo> for RedisEndpoint {
    fn from(info: ConnectionInfo) -> Self {
        Self::simple(info)
    }
}

/// Dial a fresh multiplexed connection. When `tls_override` is present the TLS
/// handshake is performed here (SNI / peer-name = `server_name`, independent of
/// the address host); otherwise the redis-rs client dial path is used unchanged.
async fn dial_with(
    info: ConnectionInfo,
    config: &AsyncConnectionConfig,
    tls_override: Option<&TlsSniOverride>,
) -> redis::RedisResult<MultiplexedConnection> {
    let info = info.with_go_certificate_policy(true);
    if let Some(override_) = tls_override {
        return dial_tls_with_sni(info, config, override_).await;
    }
    redis::Client::open(info)?
        .get_multiplexed_async_connection_with_config(config)
        .await
}

/// `osscluster.go:1557-1605` (`ClusterClient.loadState`): a ClusterClient asks a
/// seed node for the slot map with `CLUSTER SLOTS` before it routes anything, and
/// the first seed error is what its caller receives. redis-rs happily builds a
/// usable connection from one seed that refuses the cluster protocol, so without
/// this probe a `redis.cluster_addresses` entry pointing at a plain Redis starts
/// upstream rejects with
/// `unable to connect to redis: ERR This instance has cluster support disabled`.
async fn load_cluster_state(
    shared: &ClusterShared,
    tls_override: Option<&TlsSniOverride>,
) -> redis::RedisResult<()> {
    // `redis.go:131-143` sets no timeouts on the Cluster branch, so the probe uses
    // go-redis' own dial/read defaults instead of the caller's budget.
    let config = AsyncConnectionConfig::new()
        .set_connection_timeout(Some(Duration::from_secs(5)))
        .set_response_timeout(Some(Duration::from_secs(5)));
    let config = match shared.timeouts {
        Some(timeouts) => timeouts.apply(config),
        None => config,
    };
    let mut first_error: Option<RedisError> = None;
    for info in &shared.initial_nodes {
        match probe_cluster_slots(info.clone(), &config, tls_override).await {
            Ok(()) => return Ok(()),
            Err(error) => {
                if first_error.is_none() {
                    first_error = Some(error);
                }
            }
        }
    }
    Err(first_error.unwrap_or_else(|| {
        RedisError::from((
            ErrorKind::InvalidClientConfig,
            "cluster endpoint has no seeds",
        ))
    }))
}

/// The `CLUSTER SLOTS` a go-redis seed receives as its first command.
async fn probe_cluster_slots(
    info: ConnectionInfo,
    config: &AsyncConnectionConfig,
    tls_override: Option<&TlsSniOverride>,
) -> redis::RedisResult<()> {
    let mut connection = dial_with(info, config, tls_override).await?;
    let _slots: Value = redis::cmd("CLUSTER")
        .arg("SLOTS")
        .query_async(&mut connection)
        .await?;
    Ok(())
}

/// `xtls.ServerName` dial: resolve the address host, open TCP to it, then run a
/// rustls handshake whose `ServerName` (SNI **and** WebPKI verification identity)
/// is the configured name rather than the dialed host. Verified paths only —
/// `insecure` never reaches here (see [`TlsSniOverride`]). The returned
/// multiplexed driver future is spawned so the connection is driven to
/// completion, exactly as redis-rs does internally for its own dials.
async fn dial_tls_with_sni(
    info: ConnectionInfo,
    config: &AsyncConnectionConfig,
    override_: &TlsSniOverride,
) -> redis::RedisResult<MultiplexedConnection> {
    let (host, port) = match info.addr() {
        ConnectionAddr::TcpTls { host, port, .. } | ConnectionAddr::Tcp(host, port) => {
            (host.clone(), *port)
        }
        _ => {
            return Err(RedisError::from((
                ErrorKind::InvalidClientConfig,
                "TLS SNI override requires a TCP/TLS address",
            )));
        }
    };
    let sni = rtc_tls::rustls::pki_types::ServerName::try_from(override_.server_name.clone())
        .map_err(|e| {
            RedisError::from((
                ErrorKind::InvalidClientConfig,
                "invalid redis.tls.server_name",
                e.to_string(),
            ))
        })?;
    let client_config = rtc_tls::verified_client_config(
        override_.root_pem.as_deref(),
        override_.client_cert_pem.as_deref(),
        override_.client_key_pem.as_deref(),
    )
    .map_err(|e| {
        RedisError::from((
            ErrorKind::InvalidClientConfig,
            "could not build Redis TLS config",
            e.to_string(),
        ))
    })?;
    let connector = rtc_tls::tokio_rustls::TlsConnector::from(Arc::new(client_config));
    let transport = async {
        let socket = resolve_first((host.as_str(), port)).await?;
        let tcp = TokioTcpStream::connect(socket).await.map_err(|e| {
            RedisError::from((
                ErrorKind::Io,
                "failed to connect to Redis for TLS SNI dial",
                e.to_string(),
            ))
        })?;
        connector.connect(sni, tcp).await.map_err(|e| {
            RedisError::from((
                ErrorKind::Io,
                "Redis TLS handshake (server_name override) failed",
                e.to_string(),
            ))
        })
    };
    let stream = match config.transport_connection_timeout() {
        Some(timeout) if timeout.is_zero() => {
            return Err(std::io::Error::from(std::io::ErrorKind::TimedOut).into());
        }
        Some(timeout) => tokio::time::timeout(timeout, transport)
            .await
            .map_err(|_| std::io::Error::from(std::io::ErrorKind::TimedOut))??,
        None => transport.await?,
    };
    let settings = info.redis_settings().clone();
    let (connection, driver) =
        MultiplexedConnection::new_with_config(&settings, stream, config.clone()).await?;
    tokio::spawn(driver);
    Ok(connection)
}

/// Resolve `host:port` to the first address, mirroring the default DNS path
/// redis-rs takes for a `TcpTls` host.
async fn resolve_first(host_port: (&str, u16)) -> redis::RedisResult<std::net::SocketAddr> {
    tokio::net::lookup_host(host_port)
        .await
        .map_err(|e| {
            RedisError::from((
                ErrorKind::Io,
                "failed to resolve Redis host for TLS SNI dial",
                e.to_string(),
            ))
        })?
        .next()
        .ok_or_else(|| {
            RedisError::from((
                ErrorKind::Io,
                "Redis host resolved to no address for TLS SNI dial",
            ))
        })
}

/// Resolve the master exactly like `sentinelFailover.MasterAddr`: prefer the
/// warm Sentinel node, fall through to a scan of every configured address,
/// and fail with the aggregated-unreachable text when no Sentinel answers.
/// The per-Sentinel discovery dial honours the Sentinel DialTimeout / read
/// budgets from `GetRedisClient` (see [`SentinelTarget`]), not the caller's
/// connection config, so a slow-but-healthy Sentinel is not judged unreachable
/// and no master push sender is bound to a discovery connection.
async fn resolve_master_info(
    shared: &Arc<SentinelShared>,
    tls_override: Option<&TlsSniOverride>,
) -> redis::RedisResult<ConnectionInfo> {
    let discovery = shared.target.discovery_config();
    let observed_epoch = shared.events.epoch();
    let accept = |master, selection: Option<(usize, &str)>| {
        let resolved = addr_string(&master);
        let master =
            shared
                .events
                .reconcile(resolved.clone(), observed_epoch, &shared.target.master_name);
        if let Some((index, address)) = selection {
            shared
                .events
                .select(index, address, &resolved, &shared.target.master_name);
        }
        (shared.target.master_info)(master).map_err(|reason| {
            RedisError::from((ErrorKind::Client, "sentinel master configuration", reason))
        })
    };
    let warm = async |address: &str, caller: &str| {
        let reason = match query_sentinel(&shared.target, address, &discovery, tls_override).await {
            Ok(Some(master)) => return Some(master),
            Ok(None) => "redis: nil".into(),
            Err(error) => go_attempt_text(&error, address),
        };
        let name = crate::twirp::error::go_quote(&shared.target.master_name);
        write_redis_diagnostic(
            caller,
            format!("sentinel: GetMasterAddrByName name={name} failed: {reason}").as_bytes(),
        );
        None
    };
    if let Some(index) = shared.events.selected_index() {
        let addresses = shared.events.addresses(&shared.target.addresses);
        if let Some(master) = warm(&addresses[index], "sentinel.go:973").await {
            return accept(master, None);
        }
    }

    // The Go resolver rechecks the currently selected Sentinel under an
    // exclusive discovery lock before closing it and racing the candidates.
    let _gate = shared.events.discovery_gate.lock().await;
    let addresses = shared.events.addresses(&shared.target.addresses);
    if let Some(index) = shared.events.selected_index() {
        if let Some(master) = warm(&addresses[index], "sentinel.go:991").await {
            return accept(master, None);
        }
        shared.events.clear_selection();
    }

    // After a failed warm lookup Go races the entire configured list again.
    // Keep the probes scoped to this lookup so returning a winner drops the rest.
    let discovery = &discovery;
    let mut probes: FuturesUnordered<_> = addresses
        .iter()
        .enumerate()
        .map(|(index, address)| async move {
            let outcome = query_sentinel(&shared.target, address, discovery, tls_override).await;
            (index, address, outcome)
        })
        .collect();
    let mut attempts: Vec<String> = Vec::new();
    while let Some((index, address, outcome)) = probes.next().await {
        let reason = match outcome {
            Ok(Some(master)) => return accept(master, Some((index, address))),
            // go-redis joins redis.Nil with the other failed discoveries.
            Ok(None) => "redis: nil".into(),
            Err(error) => go_attempt_text(&error, address),
        };
        let name = crate::twirp::error::go_quote(&shared.target.master_name);
        write_redis_diagnostic(
            "sentinel.go:1020",
            format!("sentinel: GetMasterAddrByName addr={address}, master={name} failed: {reason}")
                .as_bytes(),
        );
        attempts.push(reason);
    }
    // `errors.Join(errs...)` (sentinel.go:1051) renders every failed attempt
    // separated by newlines, in probe completion order.
    let detail = attempts.join("\n");
    Err(RedisError::from((
        ErrorKind::Client,
        ALL_SENTINELS_UNREACHABLE,
        detail,
    )))
}

/// Per-attempt text as go-redis reports it inside the aggregate: a dial/read
/// timeout surfaces the raw net timeout message (`i/o timeout`), a refused
/// connection surfaces Go's `net.OpError` line for the configured address
/// (`dial tcp <host:port>: connect: connection refused`), everything else
/// keeps the transport error's own text.
fn go_attempt_text(error: &RedisError, address: &str) -> String {
    if error.is_timeout() {
        "i/o timeout".into()
    } else if error.is_connection_refusal() {
        format!("dial tcp {address}: connect: connection refused")
    } else {
        go_error_text(error)
    }
}

/// Startup-surface text for [`RedisEndpoint::dial`] failures, matching what
/// upstream `GetRedisClient` wraps with `unable to connect to redis: %w`.
/// redis-rs renders `General` errors as `"{trace} - {kind}: {detail}"`, which
/// would duplicate the aggregate constant; the Sentinel-unreachable family is
/// rebuilt as the bare go-redis line (`detail` carries the joined attempts).
pub fn go_error_text(error: &RedisError) -> String {
    if error.kind() == ErrorKind::Client
        && error.to_string().starts_with(ALL_SENTINELS_UNREACHABLE)
        && let Some(detail) = error.detail()
        && !detail.is_empty()
    {
        return format!("{ALL_SENTINELS_UNREACHABLE}: {detail}");
    }
    // A Redis error reply is a different family: go-redis' `proto.Error` prints the
    // reply as the server sent it (`"ERR <message>"`), while redis-rs renders the
    // same reply as `"ResponseError: <message>"` and puts the code in `code()`.
    // Without this the cluster-probe failure would read
    // `unable to connect to redis: ResponseError: This instance has cluster support
    // disabled` where upstream prints the reply itself.
    if let Some(code) = error.code() {
        return match error.detail() {
            Some(detail) if !detail.is_empty() => format!("{code} {detail}"),
            _ => code.to_string(),
        };
    }
    error.to_string()
}

/// `SENTINEL get-master-addr-by-name <master>` against one node.
/// `Ok(None)` means the node answered successfully with no master registered
/// (redis-rs `ErrorKind::MasterNameNotFoundBySentinel` family).
async fn query_sentinel(
    target: &SentinelTarget,
    address: &str,
    config: &AsyncConnectionConfig,
    tls_override: Option<&TlsSniOverride>,
) -> redis::RedisResult<Option<ConnectionAddr>> {
    let info = (target.sentinel_info)(address).map_err(|reason| {
        RedisError::from((ErrorKind::Client, "sentinel configuration", reason))
    })?;
    let mut connection = dial_with(info, config, tls_override).await?;
    let values: Option<Vec<Option<String>>> = redis::cmd("SENTINEL")
        .arg("get-master-addr-by-name")
        .arg(&target.master_name)
        .query_async(&mut connection)
        .await?;
    let Some(values) = values else {
        return Ok(None);
    };
    let Some([Some(host), Some(port)]) = <[Option<String>; 2]>::try_from(values).ok() else {
        return Err(RedisError::from((
            ErrorKind::UnexpectedReturnType,
            "sentinel returned an invalid master address",
        )));
    };
    let port = port.parse::<u16>().map_err(|_| {
        RedisError::from((
            ErrorKind::UnexpectedReturnType,
            "sentinel returned an invalid master port",
        ))
    })?;
    Ok(Some(ip_addr(host, port)))
}

/// `net.JoinHostPort` of a resolved address, IPv6 bracketed like upstream.
fn addr_string(addr: &ConnectionAddr) -> String {
    let (host, port) = match addr {
        ConnectionAddr::Tcp(host, port) => (host.as_str(), *port),
        ConnectionAddr::TcpTls { host, port, .. } => (host.as_str(), *port),
        // `ConnectionAddr` is #[non_exhaustive]; Unix is unreachable here
        // because Sentinel only ever resolves TCP master addresses.
        _ => unreachable!("sentinel resolves TCP addresses"),
    };
    if host.contains(':') {
        format!("[{host}]:{port}")
    } else {
        format!("{host}:{port}")
    }
}

/// Upstream joins the Sentinel reply with `net.JoinHostPort`, so the host is
/// already an IP literal; a bare IPv6 has no port separator.
fn ip_addr(host: String, port: u16) -> ConnectionAddr {
    if let Some(literal) = host.strip_prefix('[').and_then(|h| h.strip_suffix(']')) {
        return ConnectionAddr::Tcp(literal.to_string(), port);
    }
    if host.contains(':') {
        return ConnectionAddr::Tcp(host, port);
    }
    ConnectionAddr::Tcp(host, port)
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

fn lock_mut<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

/// A long-lived command connection. Both non-cluster endpoints are
/// [`SharedConnection::Pooled`]: the live multiplexed connection is re-dialled
/// after a drop, which is what go-redis' `pool.Pool` does for a static address
/// (`internal/pool/pool.go` — a connection that failed is discarded and the next
/// `Get` dials a fresh one) and what the Sentinel branch already did for
/// `+switch-master`. The only difference between the two is *where* the next dial
/// goes: a static endpoint re-dials its own address, a Sentinel endpoint
/// re-resolves the current master first.
///
/// Before 21q the static branch was fail-closed (one dial, every later command on
/// a dead connection reporting the failure forever). Measured against the locked
/// reference on a real `redis-server` restart: the reference resumed
/// `RegisterNode` writes within a second of the server coming back, the candidate
/// never did (`could not register node: broken pipe` on every later ping). Pooling
/// is the upstream contract, so it is now the static contract too.
///
/// `Cluster` uses redis-rs `ClusterConnection`, which keeps its own per-node pool.
#[derive(Clone)]
pub enum SharedConnection {
    Pooled(PooledCommands),
    Cluster(ClusterConnection),
}

/// Re-dialling command pool: the live connection sits in shared state so an
/// in-flight lease can swap it while other clones of the same logical connection
/// keep observing the replacement.
#[derive(Clone)]
pub struct PooledCommands {
    /// Where the next dial goes. `dial` re-resolves a Sentinel master and applies
    /// the endpoint's TLS material, so a static and a Sentinel source share one
    /// path.
    endpoint: Option<RedisEndpoint>,
    config: Box<AsyncConnectionConfig>,
    live: Arc<Mutex<Option<Arc<MultiplexedConnection>>>>,
    dial_gate: Arc<tokio::sync::Mutex<()>>,
    live_epoch: Arc<AtomicU64>,
}

impl SharedConnection {
    pub(crate) async fn connect(
        endpoint: RedisEndpoint,
        config: &AsyncConnectionConfig,
    ) -> redis::RedisResult<Self> {
        match &endpoint.kind {
            EndpointKind::Simple(..) | EndpointKind::Sentinel(_) => {
                // Fail fast like go-redis: the first command connection is
                // established eagerly, so service assembly sees a resolution or
                // dial error at startup rather than on first use.
                let commands = PooledCommands {
                    endpoint: Some(endpoint),
                    config: Box::new(config.clone()),
                    live: Arc::new(Mutex::new(None)),
                    dial_gate: Arc::new(tokio::sync::Mutex::new(())),
                    live_epoch: Arc::new(AtomicU64::new(0)),
                };
                commands.acquire().await?;
                Ok(Self::Pooled(commands))
            }
            EndpointKind::Cluster(_) => {
                let connection = endpoint.dial_cluster().await?;
                Ok(Self::Cluster(connection))
            }
        }
    }

    /// Preserve an explicitly supplied connection's driver policy. There is no
    /// endpoint to redial, so this never pretends to add reconnect support.
    pub fn from_connection(connection: MultiplexedConnection) -> Self {
        Self::Pooled(PooledCommands {
            endpoint: None,
            config: Box::new(AsyncConnectionConfig::default()),
            live: Arc::new(Mutex::new(Some(Arc::new(connection)))),
            dial_gate: Arc::new(tokio::sync::Mutex::new(())),
            live_epoch: Arc::new(AtomicU64::new(0)),
        })
    }

    /// Borrow a command executor for one query/pipeline.
    pub fn lease(&mut self) -> LeasedConnection<'_> {
        match self {
            Self::Pooled(commands) => LeasedConnection::Pooled(commands),
            Self::Cluster(connection) => LeasedConnection::Cluster(connection),
        }
    }

    /// Shared-command sites keep their existing `&self` connection handles;
    /// this mirrors `MultiplexedConnection::clone` for the managed variants.
    pub fn begin(&self) -> ManagedLease<'_> {
        ManagedLease { inner: self }
    }

    /// Go ClusterClient routes ordinary pipeline commands by their own keys.
    /// redis-rs's packed pipeline requires one slot even without MULTI. Keep
    /// the original keys and first error, and attempt every independent command.
    pub(crate) async fn ordinary_pipeline(
        &self,
        pipeline: &redis::Pipeline,
    ) -> redis::RedisResult<()> {
        let mut connection = self.begin();
        if !matches!(self, Self::Cluster(_)) {
            return pipeline
                .query_async::<Vec<Value>>(&mut connection)
                .await
                .map(|_| ());
        }
        let mut failure = None;
        for command in pipeline.cmd_iter() {
            if let Err(error) = command.query_async::<Value>(&mut connection).await {
                failure.get_or_insert(error);
            }
        }
        failure.map_or(Ok(()), Err)
    }
}

/// `&self` façade over [`SharedConnection`] implementing [`ConnectionLike`], so
/// existing call sites (`query_async(&mut connection)` behind a mutex or a
/// cloned handle) keep their shape while Sentinel failover stays transparent.
pub struct ManagedLease<'a> {
    inner: &'a SharedConnection,
}

impl AsyncConnectionLike for ManagedLease<'_> {
    fn req_packed_command<'a>(&'a mut self, cmd: &'a redis::Cmd) -> redis::RedisFuture<'a, Value> {
        Box::pin(async move {
            match self.inner {
                SharedConnection::Pooled(commands) => {
                    let mut lease = LeasedConnection::Pooled(commands);
                    lease.req_packed_command(cmd).await
                }
                SharedConnection::Cluster(connection) => {
                    ClusterConnection::req_packed_command(&mut { connection.clone() }, cmd).await
                }
            }
        })
    }

    fn req_packed_commands<'a>(
        &'a mut self,
        pipeline: &'a redis::Pipeline,
        offset: usize,
        count: usize,
    ) -> redis::RedisFuture<'a, Vec<Value>> {
        Box::pin(async move {
            match self.inner {
                SharedConnection::Pooled(commands) => {
                    let mut lease = LeasedConnection::Pooled(commands);
                    lease.req_packed_commands(pipeline, offset, count).await
                }
                SharedConnection::Cluster(connection) => {
                    ClusterConnection::req_packed_commands(
                        &mut { connection.clone() },
                        pipeline,
                        offset,
                        count,
                    )
                    .await
                }
            }
        })
    }

    fn get_db(&self) -> i64 {
        match self.inner {
            SharedConnection::Pooled(commands) => lock(&commands.live)
                .as_ref()
                .map(|connection| connection.get_db())
                .unwrap_or(0),
            SharedConnection::Cluster(connection) => connection.get_db(),
        }
    }
}

impl AsyncConnectionLike for SharedConnection {
    fn req_packed_command<'a>(&'a mut self, cmd: &'a redis::Cmd) -> redis::RedisFuture<'a, Value> {
        Box::pin(async move {
            match self {
                SharedConnection::Pooled(commands) => {
                    let mut lease = LeasedConnection::Pooled(commands);
                    lease.req_packed_command(cmd).await
                }
                SharedConnection::Cluster(connection) => connection.req_packed_command(cmd).await,
            }
        })
    }

    fn req_packed_commands<'a>(
        &'a mut self,
        pipeline: &'a redis::Pipeline,
        offset: usize,
        count: usize,
    ) -> redis::RedisFuture<'a, Vec<Value>> {
        Box::pin(async move {
            match self {
                SharedConnection::Pooled(commands) => {
                    let mut lease = LeasedConnection::Pooled(commands);
                    lease.req_packed_commands(pipeline, offset, count).await
                }
                SharedConnection::Cluster(connection) => {
                    connection
                        .req_packed_commands(pipeline, offset, count)
                        .await
                }
            }
        })
    }

    fn get_db(&self) -> i64 {
        match self {
            SharedConnection::Pooled(commands) => lock(&commands.live)
                .as_ref()
                .map(|connection| connection.get_db())
                .unwrap_or(0),
            SharedConnection::Cluster(connection) => connection.get_db(),
        }
    }
}

impl PooledCommands {
    /// A live pooled connection, re-dialling (and for a Sentinel endpoint
    /// re-resolving the master) first when the pool was invalidated by a drop, an
    /// I/O failure or a recorded failover.
    async fn acquire(&self) -> redis::RedisResult<Arc<MultiplexedConnection>> {
        let endpoint_epoch = || {
            self.endpoint
                .as_ref()
                .map_or(0, RedisEndpoint::current_master_epoch)
        };
        {
            let mut live = lock_mut(&self.live);
            if self.live_epoch.load(Ordering::Acquire) != endpoint_epoch() {
                live.take();
            }
            if let Some(connection) = live.as_ref() {
                return Ok(connection.clone());
            }
        }
        let _dial = self.dial_gate.lock().await;
        loop {
            let epoch = endpoint_epoch();
            {
                let mut live = lock_mut(&self.live);
                if self.live_epoch.load(Ordering::Acquire) != epoch {
                    live.take();
                }
                if let Some(connection) = live.as_ref() {
                    return Ok(connection.clone());
                }
            }
            let endpoint = self.endpoint.as_ref().ok_or_else(|| {
                RedisError::from((
                    ErrorKind::InvalidClientConfig,
                    "supplied Redis connection has no endpoint",
                ))
            })?;
            let connection = Arc::new(endpoint.dial(&self.config).await?);
            // Do not install a dial that overlapped a newer switch event.
            if endpoint_epoch() != epoch {
                continue;
            }
            self.live_epoch.store(epoch, Ordering::Release);
            *lock_mut(&self.live) = Some(connection.clone());
            return Ok(connection);
        }
    }

    async fn transaction(
        &self,
        pipeline: &redis::Pipeline,
        offset: usize,
        count: usize,
    ) -> redis::RedisResult<Vec<Value>> {
        let mut first_write_error = None;
        for attempt in 0..=MAX_COMMAND_RETRIES {
            let generation = self.acquire().await?;
            let mut connection = (*generation).clone();
            let (result, incomplete_write) = connection
                .send_transaction_with_incomplete_write_state(pipeline, offset, count)
                .await;
            match result {
                Ok(values) => {
                    // Fixed Go retains the first writer error on the synthetic
                    // MULTI command after a subsequent successful transaction.
                    if let Some(error) = first_write_error
                        && matches!(values.as_slice(), [Value::Array(replies)]
                            if !matches!(replies.first(), Some(Value::ServerError(_))))
                    {
                        return Err(error);
                    }
                    return Ok(values);
                }
                Err(error) => {
                    if needs_reconnect(&error) {
                        self.invalidate(&generation);
                    }
                    // A read error, flush error or unattributed transport
                    // failure cannot prove that EXEC was never submitted.
                    if !incomplete_write || attempt == MAX_COMMAND_RETRIES {
                        return Err(error);
                    }
                    first_write_error.get_or_insert(error);
                    tokio::time::sleep(cluster_watch::retry_backoff(attempt + 1)?).await;
                }
            }
        }
        unreachable!("the last transaction attempt always returns")
    }

    /// A late failure from an old lease must not evict a replacement installed
    /// by another command. Arc identity is the connection generation fence.
    fn invalidate(&self, failed: &Arc<MultiplexedConnection>) {
        if self.endpoint.is_none() {
            return;
        }
        let mut live = lock_mut(&self.live);
        if live
            .as_ref()
            .is_some_and(|current| Arc::ptr_eq(current, failed))
        {
            live.take();
        }
    }
}

/// Executor returned by [`SharedConnection::lease`]. A managed single command
/// retries explicit transient Redis rejections with the fixed Go retry budget.
/// A READONLY rejection retires the old lease, so Sentinel resolves the new
/// master before retrying. Transport errors keep the existing one-reconnect
/// policy. A transaction retries only when the driver proves its own write
/// deadline expired before submitting every byte.
///
/// Explicitly supplied connections retain their caller-owned driver policy.
pub enum LeasedConnection<'a> {
    Pooled(&'a PooledCommands),
    Cluster(&'a mut ClusterConnection),
}

/// Whether a failed command proves the pooled connection may no longer reach
/// the current master, so the master must be re-resolved before a retry.
fn needs_reconnect(error: &RedisError) -> bool {
    error.is_connection_dropped() || error.is_io_error() || error.code() == Some("READONLY")
}

// Fixed go-redis v9.22.0 defaults to one command plus three retries. A
// redis-rs raw command preserves RESP error replies inside Value; callers
// of ConnectionLike expect the final reply in that same shape.
const MAX_COMMAND_RETRIES: u64 = 3;

fn retryable_command_rejection(error: &redis::ServerError) -> bool {
    matches!(
        error.code(),
        "LOADING" | "MASTERDOWN" | "CLUSTERDOWN" | "TRYAGAIN" | "NOREPLICAS" | "READONLY"
    )
}

impl AsyncConnectionLike for LeasedConnection<'_> {
    fn req_packed_command<'a>(&'a mut self, cmd: &'a redis::Cmd) -> redis::RedisFuture<'a, Value> {
        Box::pin(async move {
            match self {
                Self::Pooled(commands) => {
                    let managed = commands.endpoint.is_some();
                    let mut retries = 0;
                    let mut transport_retry_available = managed;
                    loop {
                        let generation = commands.acquire().await?;
                        let mut connection = (*generation).clone();
                        match connection.req_packed_command(cmd).await {
                            Ok(Value::ServerError(error))
                                if managed && retryable_command_rejection(&error) =>
                            {
                                if error.code() == "READONLY" {
                                    commands.invalidate(&generation);
                                }
                                if retries == MAX_COMMAND_RETRIES {
                                    return Ok(Value::ServerError(error));
                                }
                                retries += 1;
                                tokio::time::sleep(cluster_watch::retry_backoff(retries)?).await;
                            }
                            Ok(value) => return Ok(value),
                            Err(error) => {
                                if !needs_reconnect(&error) {
                                    return Err(error);
                                }
                                commands.invalidate(&generation);
                                if !transport_retry_available || retries == MAX_COMMAND_RETRIES {
                                    return Err(error);
                                }
                                retries += 1;
                                transport_retry_available = false;
                            }
                        }
                    }
                }
                Self::Cluster(connection) => connection.req_packed_command(cmd).await,
            }
        })
    }

    fn req_packed_commands<'a>(
        &'a mut self,
        pipeline: &'a redis::Pipeline,
        offset: usize,
        count: usize,
    ) -> redis::RedisFuture<'a, Vec<Value>> {
        Box::pin(async move {
            match self {
                Self::Pooled(commands)
                    if pipeline.is_transaction() && commands.endpoint.is_some() =>
                {
                    commands.transaction(pipeline, offset, count).await
                }
                Self::Pooled(commands) => {
                    let managed = commands.endpoint.is_some();
                    let is_transaction = pipeline.is_transaction();
                    let retry_server_reply = managed && !is_transaction && offset == 0;
                    let mut retries = 0;
                    let mut transport_retry_available = managed && !is_transaction;
                    loop {
                        let generation = commands.acquire().await?;
                        let mut connection = (*generation).clone();
                        match connection
                            .req_packed_commands(pipeline, offset, count)
                            .await
                        {
                            Ok(values) => {
                                // The fixed Go ordinary pipeline reads every reply, but
                                // only the first command's explicit rejection triggers a
                                // batch replay. A later failure keeps earlier successes.
                                let Some(Value::ServerError(error)) = values.first() else {
                                    return Ok(values);
                                };
                                if !retry_server_reply || !retryable_command_rejection(error) {
                                    return Ok(values);
                                }
                                if error.code() == "READONLY" {
                                    commands.invalidate(&generation);
                                }
                                if retries == MAX_COMMAND_RETRIES {
                                    return Ok(values);
                                }
                                retries += 1;
                                tokio::time::sleep(cluster_watch::retry_backoff(retries)?).await;
                            }
                            Err(error) => {
                                if !needs_reconnect(&error) {
                                    return Err(error);
                                }
                                commands.invalidate(&generation);
                                if !transport_retry_available || retries == MAX_COMMAND_RETRIES {
                                    return Err(error);
                                }
                                retries += 1;
                                transport_retry_available = false;
                            }
                        }
                    }
                }
                Self::Cluster(connection) => {
                    connection
                        .req_packed_commands(pipeline, offset, count)
                        .await
                }
            }
        })
    }

    fn get_db(&self) -> i64 {
        match self {
            Self::Pooled(commands) => lock(&commands.live)
                .as_ref()
                .map(|connection| connection.get_db())
                .unwrap_or(0),
            Self::Cluster(connection) => connection.get_db(),
        }
    }
}

/// Sentinel dial/read budgets from `GetRedisClient`'s Sentinel branch
/// (`redis.go:103-114`): dial defaults to 2s, read/write to 200ms.
pub fn sentinel_timeouts(dial_timeout_ms: i64, read_timeout_ms: i64) -> (Duration, Duration) {
    let dial = if dial_timeout_ms == 0 {
        Duration::from_millis(2000)
    } else {
        Duration::from_millis(dial_timeout_ms.max(0) as u64)
    };
    let read = if read_timeout_ms == 0 {
        Duration::from_millis(200)
    } else {
        Duration::from_millis(read_timeout_ms.max(0) as u64)
    };
    (dial, read)
}

/// Sentinel factory default is 200ms. Negative public milliseconds disable the
/// write deadline, as go-redis does for a negative timeout.
pub fn sentinel_write_timeout(timeout_ms: i64) -> Option<Duration> {
    match timeout_ms {
        0 => Some(Duration::from_millis(200)),
        n if n > 0 => Some(Duration::from_millis(n as u64)),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use redis::IntoConnectionInfo;

    #[test]
    fn reconnect_uses_server_error_code_not_arbitrary_error_text() {
        let readonly: RedisError = (
            ErrorKind::Server(redis::ServerErrorKind::ReadOnly),
            "replica refuses writes",
        )
            .into();
        assert!(needs_reconnect(&readonly));
        let logical: RedisError = (
            ErrorKind::Server(redis::ServerErrorKind::ResponseError),
            "script validation failed: value contains READONLY",
        )
            .into();
        assert!(!needs_reconnect(&logical));
        let io: RedisError = std::io::Error::from(std::io::ErrorKind::BrokenPipe).into();
        assert!(needs_reconnect(&io));
    }

    #[tokio::test]
    async fn cloned_command_pool_shares_reconnect_gate_and_live_generation() {
        let pooled = SharedConnection::Pooled(PooledCommands {
            endpoint: None,
            config: Box::new(AsyncConnectionConfig::default()),
            live: Arc::new(Mutex::new(None)),
            dial_gate: Arc::new(tokio::sync::Mutex::new(())),
            live_epoch: Arc::new(AtomicU64::new(0)),
        });
        let clone = pooled.clone();
        let (SharedConnection::Pooled(a), SharedConnection::Pooled(b)) = (&pooled, &clone) else {
            panic!("pooled views")
        };
        assert!(
            Arc::ptr_eq(&a.live, &b.live),
            "views must observe the same reconnect generation"
        );
        assert!(
            Arc::ptr_eq(&a.dial_gate, &b.dial_gate),
            "views must serialize the same dial"
        );
        let guard = a.dial_gate.lock().await;
        assert!(b.dial_gate.try_lock().is_err());
        drop(guard);
        assert!(b.dial_gate.try_lock().is_ok());
    }

    /// Run only against a coordinator-owned Redis deployment. Unlike the older
    /// optional-env tests this is explicitly ignored, never a silent pass.
    #[tokio::test]
    #[ignore = "requires LIVEKIT_REDIS_POOL_TEST_URL pointing to owned Redis"]
    async fn stale_command_failure_cannot_evict_reconnected_pool() {
        let url = std::env::var("LIVEKIT_REDIS_POOL_TEST_URL").expect("set owned Redis URL");
        let endpoint = RedisEndpoint::simple(url.into_connection_info().unwrap());
        let pool = SharedConnection::connect(endpoint, &AsyncConnectionConfig::default())
            .await
            .unwrap();
        let SharedConnection::Pooled(commands) = &pool else {
            panic!("simple pool")
        };
        let old = commands.acquire().await.unwrap();
        commands.invalidate(&old);
        let fresh = commands.acquire().await.unwrap();
        assert!(!Arc::ptr_eq(&old, &fresh));
        // Two commands held the old connection. The second finishes failing
        // only after the first has replaced it. That failure must be fenced.
        commands.invalidate(&old);
        let still_fresh = commands.acquire().await.unwrap();
        assert!(Arc::ptr_eq(&fresh, &still_fresh));
        commands.invalidate(&fresh);
        let (a, b) = tokio::join!(commands.acquire(), commands.acquire());
        assert!(Arc::ptr_eq(&a.unwrap(), &b.unwrap()));
        let mut lease = pool.begin();
        let pong: String = redis::cmd("PING").query_async(&mut lease).await.unwrap();
        assert_eq!(pong, "PONG");
        // Explicitly supplied connections have no dial factory. Preserve that
        // API and the original driver policy instead of deleting the caller's
        // constructor or inventing reconnect support for an unknown endpoint.
        let direct = commands
            .endpoint
            .as_ref()
            .unwrap()
            .dial(&AsyncConnectionConfig::default())
            .await
            .unwrap();
        let supplied = SharedConnection::from_connection(direct);
        let SharedConnection::Pooled(fixed) = &supplied else {
            panic!("fixed pool")
        };
        let fixed_generation = fixed.acquire().await.unwrap();
        fixed.invalidate(&fixed_generation);
        assert!(Arc::ptr_eq(
            &fixed_generation,
            &fixed.acquire().await.unwrap()
        ));
        let pong: String = redis::cmd("PING")
            .query_async(&mut supplied.begin())
            .await
            .unwrap();
        assert_eq!(pong, "PONG");
    }

    fn plaintext_target(addresses: Vec<String>) -> SentinelTarget {
        SentinelTarget {
            master_name: "mymaster".into(),
            addresses,
            sentinel_username: String::new(),
            sentinel_password: String::new(),
            discovery_dial_timeout: Duration::from_millis(2000),
            discovery_read_timeout: Duration::from_millis(200),
            write_timeout: sentinel_write_timeout(0),
            sentinel_info: Arc::new(|addr| {
                IntoConnectionInfo::into_connection_info(format!("redis://{addr}/"))
                    .map_err(|e| e.to_string())
            }),
            master_info: Arc::new(|host_port| {
                IntoConnectionInfo::into_connection_info(format!("redis://{host_port}/"))
                    .map_err(|e| e.to_string())
            }),
        }
    }

    mod sentinel_dial_timeout;
    mod sentinel_read_timeout;
    mod sentinel_write_timeout;

    #[test]
    fn sentinel_timeouts_apply_go_defaults() {
        // `GetRedisClient`'s Sentinel branch: dial 0 -> 2s, read 0 -> 200ms.
        assert_eq!(
            sentinel_timeouts(0, 0),
            (Duration::from_millis(2000), Duration::from_millis(200))
        );
        assert_eq!(
            sentinel_timeouts(1500, 250),
            (Duration::from_millis(1500), Duration::from_millis(250))
        );
    }

    #[test]
    fn addr_string_brackets_ipv6_like_joinhostport() {
        assert_eq!(
            addr_string(&ConnectionAddr::Tcp("127.0.0.1".into(), 7001)),
            "127.0.0.1:7001"
        );
        assert_eq!(
            addr_string(&ConnectionAddr::Tcp("::1".into(), 6379)),
            "[::1]:6379"
        );
    }

    #[test]
    fn ip_addr_strips_brackets_from_a_sentinel_ipv6_reply() {
        match ip_addr("::1".into(), 7001) {
            ConnectionAddr::Tcp(host, port) => {
                assert_eq!(host, "::1");
                assert_eq!(port, 7001);
            }
            other => panic!("expected Tcp, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn unreachable_sentinels_aggregate_the_go_unreachable_text() {
        // Ports 1/2 are refused immediately on loopback, so every attempt fails
        // and go-redis' aggregated Sentinel-unreachable message surfaces.
        let endpoint = RedisEndpoint::sentinel(plaintext_target(vec![
            "127.0.0.1:1".into(),
            "127.0.0.1:2".into(),
        ]));
        let config = AsyncConnectionConfig::new()
            .set_connection_timeout(Some(Duration::from_millis(200)))
            .set_response_timeout(Some(Duration::from_millis(200)));
        let error = endpoint.dial(&config).await.unwrap_err();
        let text = go_error_text(&error);
        assert!(
            text.starts_with(&format!("{ALL_SENTINELS_UNREACHABLE}: ")),
            "expected aggregate unreachable text, got: {text}"
        );
        // `errors.Join(errs...)` keeps every failed attempt on its own line;
        // the startup text must not carry redis-rs' `{trace} - {kind}: {detail}`
        // nesting (SPEC-0074 §2.5-B `struct.redis.sentinel`).
        assert!(
            text.contains('\n'),
            "both attempts must be joined, got: {text}"
        );
        assert!(
            !text.contains(" - Client"),
            "nested rendering leaked: {text}"
        );
        // Each refused attempt renders Go's `net.OpError` line with the
        // configured address, not Rust's std `Connection refused (os error 61)`.
        assert!(
            text.contains("dial tcp 127.0.0.1:1: connect: connection refused\ndial tcp 127.0.0.1:2: connect: connection refused"),
            "expected Go-style per-attempt lines, got: {text}"
        );
        assert!(!text.contains("os error"), "std error text leaked: {text}");
    }

    /// SPEC-0074 C10 real-deployment acceptance, run against a coordinator-owned
    /// TLS Redis (`rediss`). Proves the `xtls.ServerName` override performs a
    /// real handshake whose peer is verified against `server_name` rather than
    /// the dial address host, and that a wrong name is rejected by verification.
    /// Optional mTLS material is read from the env when the server requires it.
    #[tokio::test]
    async fn real_tls_server_name_override_and_verification() {
        let Ok(addr) = std::env::var("LIVEKIT_TLS_TEST_ADDR") else {
            return;
        };
        let Ok(ca_path) = std::env::var("LIVEKIT_TLS_TEST_CA") else {
            return;
        };
        let name = std::env::var("LIVEKIT_TLS_TEST_SERVER_NAME")
            .unwrap_or_else(|_| "redis.test.local".into());
        let pair = match (
            std::env::var("LIVEKIT_TLS_TEST_CLIENT_CERT").ok(),
            std::env::var("LIVEKIT_TLS_TEST_CLIENT_KEY").ok(),
        ) {
            (Some(c), Some(k)) => (
                Some(std::fs::read(c).expect("client cert")),
                Some(std::fs::read(k).expect("client key")),
            ),
            _ => (None, None),
        };
        let ca = std::fs::read(&ca_path).expect("CA bundle");
        let info = IntoConnectionInfo::into_connection_info(format!("rediss://{addr}/"))
            .expect("connection info");
        let config = AsyncConnectionConfig::new()
            .set_connection_timeout(Some(Duration::from_secs(3)))
            .set_response_timeout(Some(Duration::from_secs(3)));

        let endpoint = RedisEndpoint::simple_with_tls_sni(
            info.clone(),
            TlsSniOverride {
                server_name: name.clone(),
                root_pem: Some(ca.clone()),
                client_cert_pem: pair.0.clone(),
                client_key_pem: pair.1.clone(),
            },
        );
        let mut connection = endpoint
            .dial(&config)
            .await
            .expect("TLS handshake with server_name override");
        let pong: String = redis::cmd("PING")
            .query_async(&mut connection)
            .await
            .expect("PING over TLS");
        assert_eq!(pong, "PONG");

        // Negative: a name the certificate is not valid for must fail peer
        // verification, proving the override really drives verification and is
        // not silently ignored in favour of the dial address host.
        let wrong = RedisEndpoint::simple_with_tls_sni(
            info,
            TlsSniOverride {
                server_name: "not-the-certificate-name.invalid".into(),
                root_pem: Some(ca),
                client_cert_pem: pair.0,
                client_key_pem: pair.1,
            },
        );
        let error = wrong
            .dial(&config)
            .await
            .expect_err("verification must reject a wrong server name")
            .to_string();
        let lower = error.to_lowercase();
        assert!(
            lower.contains("certificate") || lower.contains("name"),
            "expected a certificate-name verification failure, got: {error}"
        );
    }

    /// SPEC-0074 C10 real-deployment acceptance for a TLS Sentinel deployment.
    /// Proves `xtls.ServerName` drives **both** the Sentinel discovery dial and
    /// the resolved-master dial (upstream applies one `TLSConfig` to both), and
    /// that a wrong name is rejected.
    #[tokio::test]
    async fn real_tls_sentinel_server_name_override() {
        let Ok(addresses) = std::env::var("LIVEKIT_TLS_SENTINEL_ADDR") else {
            return;
        };
        let Ok(ca_path) = std::env::var("LIVEKIT_TLS_SENTINEL_CA") else {
            return;
        };
        let master_name =
            std::env::var("LIVEKIT_TLS_SENTINEL_MASTER").unwrap_or_else(|_| "mymaster".into());
        let name = std::env::var("LIVEKIT_TLS_SENTINEL_NAME")
            .unwrap_or_else(|_| "redis.test.local".into());
        let ca = std::fs::read(ca_path).expect("CA bundle");
        let addresses: Vec<String> = addresses.split(',').map(|s| s.trim().to_string()).collect();
        let target = |addresses: Vec<String>, master_name: &str| SentinelTarget {
            master_name: master_name.to_string(),
            addresses,
            sentinel_username: String::new(),
            sentinel_password: String::new(),
            discovery_dial_timeout: Duration::from_millis(2000),
            discovery_read_timeout: Duration::from_millis(200),
            write_timeout: sentinel_write_timeout(0),
            sentinel_info: Arc::new(|addr: &str| {
                IntoConnectionInfo::into_connection_info(format!("rediss://{addr}/"))
                    .map_err(|e| e.to_string())
            }),
            master_info: Arc::new(|host_port: String| {
                IntoConnectionInfo::into_connection_info(format!("rediss://{host_port}/"))
                    .map_err(|e| e.to_string())
            }),
        };
        let config = AsyncConnectionConfig::new()
            .set_connection_timeout(Some(Duration::from_secs(3)))
            .set_response_timeout(Some(Duration::from_secs(3)));

        let endpoint = RedisEndpoint::sentinel_with_tls_sni(
            target(addresses.clone(), &master_name),
            TlsSniOverride {
                server_name: name,
                root_pem: Some(ca.clone()),
                client_cert_pem: None,
                client_key_pem: None,
            },
        );
        let mut connection = endpoint
            .dial(&config)
            .await
            .expect("TLS Sentinel discovery and master dial with server_name override");
        let pong: String = redis::cmd("PING")
            .query_async(&mut connection)
            .await
            .expect("PING on the resolved master over TLS");
        assert_eq!(pong, "PONG");

        let wrong = RedisEndpoint::sentinel_with_tls_sni(
            target(addresses, &master_name),
            TlsSniOverride {
                server_name: "not-the-certificate-name.invalid".into(),
                root_pem: Some(ca),
                client_cert_pem: None,
                client_key_pem: None,
            },
        );
        let error = wrong
            .dial(&config)
            .await
            .expect_err("verification must reject a wrong server name")
            .to_string();
        let lower = error.to_lowercase();
        assert!(
            lower.contains("certificate") || lower.contains("name"),
            "expected a certificate-name verification failure, got: {error}"
        );
    }

    fn real_sentinel_env() -> Option<(Vec<String>, String)> {
        let addrs = std::env::var("LIVEKIT_SENTINEL_TEST_ADDR").ok()?;
        let master =
            std::env::var("LIVEKIT_SENTINEL_TEST_MASTER").unwrap_or_else(|_| "mymaster".into());
        Some((
            addrs.split(',').map(|s| s.trim().to_string()).collect(),
            master,
        ))
    }

    /// SPEC-0074 C11 real-deployment acceptance, run against a coordinator-owned
    /// Redis Sentinel (master + replica + sentinel). Proves discovery, an actual
    /// command connection to the resolved master, a Sentinel-triggered failover
    /// and the endpoint's recovery onto the promoted master.
    #[tokio::test]
    #[ignore = "requires LIVEKIT_SENTINEL_TEST_ADDR and LIVEKIT_SENTINEL_TEST_REPLICA_ADDR"]
    async fn real_sentinel_discovery_command_and_failover_recovery() {
        let (addresses, master_name) = real_sentinel_env().expect("explicit Sentinel deployment");
        let replica_addr = std::env::var("LIVEKIT_SENTINEL_TEST_REPLICA_ADDR")
            .expect("explicit owned replica address");
        // Optional credentials exercise Sentinel authentication
        // (`redis.sentinel_username`/`sentinel_password`) and master
        // authentication (`redis.username`/`password`), exactly as the
        // production `redis_endpoint` closures do.
        let sentinel_user =
            std::env::var("LIVEKIT_SENTINEL_TEST_SENTINEL_USER").unwrap_or_default();
        let sentinel_pw = std::env::var("LIVEKIT_SENTINEL_TEST_SENTINEL_PW").unwrap_or_default();
        let master_user = std::env::var("LIVEKIT_SENTINEL_TEST_MASTER_USER").unwrap_or_default();
        let master_pw = std::env::var("LIVEKIT_SENTINEL_TEST_MASTER_PW").unwrap_or_default();
        let su = sentinel_user.clone();
        let sp = sentinel_pw.clone();
        let sentinel_info = Arc::new(move |addr: &str| {
            IntoConnectionInfo::into_connection_info(authority_url(addr, &su, &sp))
                .map_err(|e| e.to_string())
        });
        let mu = master_user.clone();
        let mp = master_pw.clone();
        let master_info = Arc::new(move |host_port: String| {
            IntoConnectionInfo::into_connection_info(authority_url(&host_port, &mu, &mp))
                .map_err(|e| e.to_string())
        });
        let target = SentinelTarget {
            master_name: master_name.clone(),
            addresses: addresses.clone(),
            sentinel_username: sentinel_user.clone(),
            sentinel_password: sentinel_pw.clone(),
            discovery_dial_timeout: Duration::from_millis(2000),
            discovery_read_timeout: Duration::from_millis(200),
            write_timeout: sentinel_write_timeout(0),
            sentinel_info,
            master_info,
        };
        let endpoint = RedisEndpoint::sentinel(target);
        let config = AsyncConnectionConfig::new()
            .set_connection_timeout(Some(Duration::from_secs(2)))
            .set_response_timeout(Some(Duration::from_secs(2)));

        // Discovery: resolve the master, connect and prove the resolved master
        // actually serves commands (SET/GET round-trip on the shared connection).
        let connection = endpoint
            .connect(&config)
            .await
            .expect("sentinel master connect");
        let first_master = resolved_master(&endpoint, &config)
            .await
            .expect("CONFIG GET port on the resolved master");
        let key = format!(
            "c11_key_{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        );
        let _: () = redis::cmd("SET")
            .arg(&key)
            .arg("c11_value")
            .query_async(&mut connection.begin())
            .await
            .expect("SET on resolved master");
        let value: String = redis::cmd("GET")
            .arg(&key)
            .query_async(&mut connection.begin())
            .await
            .expect("GET on resolved master");
        assert_eq!(value, "c11_value");

        use crate::service_bus::{
            channels,
            redis_transport::{RedisTransport, SubscriptionEvent, TransportLimits},
            wire::{self, CompressionPolicy, WireLimits},
        };
        let wire_limits = WireLimits {
            encoded_bytes: 65536,
            decompressed_bytes: 65536,
        };
        let bus = RedisTransport::connect_endpoint(
            endpoint.clone(),
            TransportLimits {
                wire: wire_limits,
                compression: CompressionPolicy::default(),
                inbox_messages: 4,
                subscriptions: 1,
                commands: 4,
                io_timeout: Duration::from_secs(2),
            },
        )
        .await
        .unwrap();
        let channel = channels::request("SentinelFixture", "Epoch", &["owned"], false);
        let mut subscription = bus.subscribe(&channel, false).await.unwrap();
        bus.publish(&channel, &lk_protocol::rpc::KeepalivePing { timestamp: 1 })
            .await
            .unwrap();
        match subscription.recv_recovering().await.unwrap() {
            SubscriptionEvent::Message(bytes) => assert_eq!(
                wire::decode::<lk_protocol::rpc::KeepalivePing>(&bytes, wire_limits)
                    .unwrap()
                    .message
                    .timestamp,
                1
            ),
            _ => panic!("initial acknowledged subscription"),
        }
        let mut epochs = endpoint.master_epoch().expect("Sentinel events");
        let baseline = *epochs.borrow_and_update();
        // Retain the old command socket, so success cannot be explained by a
        // failed write/READONLY retry or an explicit master-discovery poll.
        let SharedConnection::Pooled(pool) = &connection else {
            panic!("pooled")
        };
        let old = pool.acquire().await.unwrap();

        // Replication is asynchronous. Verify this run's key on the replica
        // itself before asking Sentinel to promote it.
        let replica_info = authority_url(&replica_addr, &master_user, &master_pw)
            .into_connection_info()
            .expect("valid replica connection info");
        let mut replica = dial_with(replica_info, &config, None)
            .await
            .expect("direct connection to the owned replica");
        let replica_port: Vec<String> = redis::cmd("CONFIG")
            .arg("GET")
            .arg("port")
            .query_async(&mut replica)
            .await
            .expect("CONFIG GET port on the replica");
        let replica_port = replica_port.last().expect("replica port");
        assert_ne!(
            replica_port, &first_master,
            "replica and master must differ"
        );
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                let value: Option<String> = redis::cmd("GET")
                    .arg(&key)
                    .query_async(&mut replica)
                    .await
                    .expect("GET on replica before promotion");
                if let Some(value) = value {
                    assert_eq!(value, "c11_value");
                    break;
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .expect("fixture must replicate the test key before Sentinel promotion");
        eprintln!("fixture replica port {replica_port} confirmed test key before promotion");
        trigger_failover(&addresses, &master_name, &sentinel_user, &sentinel_pw).await;
        tokio::time::timeout(Duration::from_secs(30), async {
            while *epochs.borrow_and_update() == baseline {
                epochs.changed().await.unwrap();
            }
        })
        .await
        .expect("actual +switch-master event");
        let port: Vec<String> = redis::cmd("CONFIG")
            .arg("GET")
            .arg("port")
            .query_async(&mut connection.begin())
            .await
            .expect("first read after switch");
        let new_master = port.last().unwrap().clone();
        assert_ne!(
            new_master, first_master,
            "read-only pooled path must migrate before any write"
        );
        assert_eq!(
            &new_master, replica_port,
            "the promoted master must be the replica that served the test key"
        );
        let mut old = (*old).clone();
        let old_port: Vec<String> = redis::cmd("CONFIG")
            .arg("GET")
            .arg("port")
            .query_async(&mut old)
            .await
            .expect("old master remains readable");
        assert_eq!(old_port.last(), Some(&first_master));
        assert!(matches!(
            tokio::time::timeout(Duration::from_secs(5), subscription.recv_recovering())
                .await
                .unwrap()
                .unwrap(),
            SubscriptionEvent::Disconnected
        ));
        assert!(matches!(
            tokio::time::timeout(Duration::from_secs(10), subscription.recv_recovering())
                .await
                .unwrap()
                .unwrap(),
            SubscriptionEvent::Restored
        ));
        bus.publish(&channel, &lk_protocol::rpc::KeepalivePing { timestamp: 2 })
            .await
            .unwrap();
        match tokio::time::timeout(Duration::from_secs(5), subscription.recv_recovering())
            .await
            .unwrap()
            .unwrap()
        {
            SubscriptionEvent::Message(bytes) => assert_eq!(
                wire::decode::<lk_protocol::rpc::KeepalivePing>(&bytes, wire_limits)
                    .unwrap()
                    .message
                    .timestamp,
                2
            ),
            _ => panic!("new-master publication after real SUBSCRIBE ACK"),
        }
        subscription.unsubscribe(&channel).await.unwrap();
        // Existing write-failure retry is still preserved after event migration.
        let _: () = redis::cmd("SET")
            .arg("c11_pooled")
            .arg("after")
            .query_async(&mut connection.begin())
            .await
            .expect("pooled command connection recovers after failover");
        let pooled_value: String = redis::cmd("GET")
            .arg("c11_pooled")
            .query_async(&mut connection.begin())
            .await
            .expect("pooled read after recovery");
        assert_eq!(pooled_value, "after");
        // A freshly dialled connection (re-resolution per dial) reaches the new
        // master and still returns the replicated value.
        let recovered = endpoint
            .connect(&config)
            .await
            .expect("reconnect after failover");
        let recovered_value: String = redis::cmd("GET")
            .arg(&key)
            .query_async(&mut recovered.begin())
            .await
            .expect("GET on promoted master");
        assert_eq!(recovered_value, "c11_value");
    }

    /// `redis://{user}:{password}@{addr}/`, or the bare authority when both
    /// credentials are empty (matching the production closures' behaviour).
    fn authority_url(addr: &str, user: &str, password: &str) -> String {
        if user.is_empty() && password.is_empty() {
            format!("redis://{addr}/")
        } else {
            format!("redis://{user}:{password}@{addr}/")
        }
    }

    async fn resolved_master(
        endpoint: &RedisEndpoint,
        config: &AsyncConnectionConfig,
    ) -> Option<String> {
        let mut connection = endpoint.dial(config).await.ok()?;
        let info = redis::cmd("CONFIG")
            .arg("GET")
            .arg("port")
            .query_async::<Vec<String>>(&mut connection)
            .await
            .ok()?;
        info.last().cloned()
    }

    async fn trigger_failover(addresses: &[String], master_name: &str, user: &str, password: &str) {
        for address in addresses {
            let Ok(info) =
                IntoConnectionInfo::into_connection_info(authority_url(address, user, password))
            else {
                continue;
            };
            let mut connection = match redis::Client::open(info) {
                Ok(client) => match client.get_multiplexed_async_connection().await {
                    Ok(connection) => connection,
                    Err(_) => continue,
                },
                Err(_) => continue,
            };
            let result: redis::RedisResult<String> = redis::cmd("SENTINEL")
                .arg("failover")
                .arg(master_name)
                .query_async(&mut connection)
                .await;
            if result.is_ok() {
                return;
            }
        }
    }

    // ---- SPEC-0074 C12: Redis Cluster integration tests ----

    /// Requires a running Redis Cluster with at least 3 master nodes.
    /// Set `REDIS_CLUSTER_TEST_NODES` to comma-separated addresses, e.g.
    /// "redis://127.0.0.1:7001,redis://127.0.0.1:7002,redis://127.0.0.1:7003"
    #[tokio::test]
    #[ignore = "requires REDIS_CLUSTER_TEST_NODES (running Redis Cluster)"]
    async fn cluster_endpoint_connect_and_key_value_round_trip() {
        let raw = std::env::var("REDIS_CLUSTER_TEST_NODES")
            .expect("comma-separated cluster node URLs required");
        let nodes: Vec<ConnectionInfo> = raw
            .split(',')
            .map(|s| IntoConnectionInfo::into_connection_info(s.trim().to_string()))
            .collect::<Result<Vec<_>, _>>()
            .expect("valid connection info per node");
        assert!(!nodes.is_empty(), "at least one node");
        let endpoint = RedisEndpoint::cluster(nodes, 2);
        assert!(endpoint.is_cluster());
        // connect() → SharedConnection::Cluster
        let config = AsyncConnectionConfig::new();
        let conn = endpoint.connect(&config).await.expect("cluster connect");
        // SET/GET via ClusterConnection
        let _: String = redis::cmd("SET")
            .arg("c12_test_key")
            .arg("c12_test_value")
            .query_async(&mut conn.begin())
            .await
            .expect("cluster SET");
        let val: String = redis::cmd("GET")
            .arg("c12_test_key")
            .query_async(&mut conn.begin())
            .await
            .expect("cluster GET");
        assert_eq!(val, "c12_test_value");
        // dial_cluster() for subscription connections
        let mut sub_conn = endpoint.dial_cluster().await.expect("dial_cluster");
        let _: String = redis::cmd("PING")
            .query_async(&mut sub_conn)
            .await
            .expect("PING on dialed cluster connection");
    }

    /// Verify RESP3 cluster endpoint path (used by service bus).
    #[tokio::test]
    #[ignore = "requires REDIS_CLUSTER_TEST_NODES (running Redis Cluster)"]
    async fn cluster_endpoint_resp3_with_push_sender() {
        let raw = std::env::var("REDIS_CLUSTER_TEST_NODES")
            .expect("comma-separated cluster node URLs required");
        let nodes: Vec<ConnectionInfo> = raw
            .split(',')
            .map(|s| IntoConnectionInfo::into_connection_info(s.trim().to_string()))
            .collect::<Result<Vec<_>, _>>()
            .expect("valid connection info per node");
        let endpoint = RedisEndpoint::cluster(nodes, 2);
        let resp3_endpoint = endpoint.with_resp3();
        assert!(resp3_endpoint.is_cluster());
        // Build via cluster_builder (used by service bus subscribe)
        let builder = resp3_endpoint
            .cluster_builder()
            .expect("cluster_builder on resp3 endpoint");
        use std::sync::atomic::{AtomicUsize, Ordering};
        let msg_count = Arc::new(AtomicUsize::new(0));
        let count_clone = msg_count.clone();
        let push = move |_info: redis::PushInfo| -> Result<(), ()> {
            count_clone.fetch_add(1, Ordering::Relaxed);
            Ok(())
        };
        let client = builder
            .push_sender(push)
            .build()
            .expect("build with push sender");
        let mut conn = client.get_async_connection().await.expect("async conn");
        // Subscribe to a test channel and publish
        let channel = "c12_push_test";
        conn.subscribe(channel).await.expect("subscribe");
        // Publish via a separate plain connection
        let first_node = raw.split(',').next().unwrap().trim();
        let pub_conn = redis::Client::open(first_node)
            .unwrap()
            .get_multiplexed_async_connection()
            .await
            .unwrap();
        let _: u64 = redis::cmd("PUBLISH")
            .arg(channel)
            .arg("hello_cluster")
            .query_async(&mut pub_conn.clone())
            .await
            .expect("publish");
        // Wait briefly for push
        tokio::time::sleep(Duration::from_millis(200)).await;
        assert!(
            msg_count.load(Ordering::Relaxed) > 0,
            "push sender received message"
        );
    }

    const CLUSTER_DISABLED: &[u8] = b"-ERR This instance has cluster support disabled\r\n";
    /// One shard owning the whole keyspace, as `CLUSTER SLOTS` answers in RESP2.
    const ONE_SHARD_SLOTS: &[u8] = b"*1\r\n*3\r\n:0\r\n:16383\r\n*3\r\n$9\r\n127.0.0.1\r\n\
        :7000\r\n$40\r\n0123456789012345678901234567890123456789\r\n";

    /// A TCP listener that answers every command with one canned reply, standing in
    /// for a Redis node whose cluster-protocol support is known in advance.
    async fn stub_node(reply: &'static [u8]) -> std::net::SocketAddr {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            loop {
                let Ok((mut socket, _)) = listener.accept().await else {
                    return;
                };
                tokio::spawn(async move {
                    use tokio::io::{AsyncReadExt, AsyncWriteExt};
                    let mut pending = Vec::new();
                    let mut chunk = [0u8; 512];
                    loop {
                        let Ok(read) = socket.read(&mut chunk).await else {
                            return;
                        };
                        if read == 0 {
                            return;
                        }
                        pending.extend_from_slice(&chunk[..read]);
                        while let Some(length) = first_command_len(&pending) {
                            pending.drain(..length);
                            if socket.write_all(reply).await.is_err() {
                                return;
                            }
                        }
                    }
                });
            }
        });
        addr
    }

    /// The byte length of the first complete RESP array in `buffer`, or `None` while
    /// more bytes are still on the way. Answers stay aligned one reply per command.
    fn first_command_len(buffer: &[u8]) -> Option<usize> {
        if *buffer.first()? != b'*' {
            return None;
        }
        let mut at = 1usize;
        let (digits, consumed) = split_line(buffer.get(at..)?)?;
        let count: usize = std::str::from_utf8(digits).ok()?.parse().ok()?;
        at += consumed;
        for _ in 0..count {
            if *buffer.get(at)? != b'$' {
                return None;
            }
            at += 1;
            let (digits, consumed) = split_line(buffer.get(at..)?)?;
            let len: usize = std::str::from_utf8(digits).ok()?.parse().ok()?;
            at += consumed + len + 2;
            if buffer.len() < at {
                return None;
            }
        }
        Some(at)
    }

    /// The bytes before the next CRLF, and how many bytes that CRLF consumed.
    fn split_line(buffer: &[u8]) -> Option<(&[u8], usize)> {
        let end = buffer.windows(2).position(|pair| pair == b"\r\n")?;
        Some((&buffer[..end], end + 2))
    }

    fn cluster_shared(addr: std::net::SocketAddr) -> ClusterShared {
        ClusterShared {
            initial_nodes: vec![
                IntoConnectionInfo::into_connection_info(format!("redis://{addr}/")).unwrap(),
            ],
            max_redirects: 2,
            resp3: false,
            timeouts: None,
        }
    }

    #[tokio::test]
    async fn a_plain_node_is_not_accepted_as_a_cluster_seed() {
        let addr = stub_node(CLUSTER_DISABLED).await;
        let endpoint = RedisEndpoint::cluster(
            vec![IntoConnectionInfo::into_connection_info(format!("redis://{addr}/")).unwrap()],
            2,
        );
        let Err(error) = endpoint.dial_cluster().await else {
            panic!("a seed that refuses CLUSTER SLOTS is not a cluster");
        };
        assert_eq!(
            go_error_text(&error),
            "ERR This instance has cluster support disabled"
        );
    }

    #[tokio::test]
    async fn a_seed_that_answers_the_slot_map_passes_the_probe() {
        let addr = stub_node(ONE_SHARD_SLOTS).await;
        load_cluster_state(&cluster_shared(addr), None)
            .await
            .expect("a seed that answers CLUSTER SLOTS is a cluster");
    }

    #[tokio::test]
    async fn the_first_seed_error_is_what_the_caller_sees_when_none_answer() {
        let answering = stub_node(CLUSTER_DISABLED).await;
        // A port with nothing behind it: the probe reports the *first* seed's error,
        // the same `firstErr` `loadState` keeps, not the last one it tried.
        let refused = {
            let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
            listener.local_addr().unwrap()
        };
        let mut shared = cluster_shared(answering);
        shared
            .initial_nodes
            .push(IntoConnectionInfo::into_connection_info(format!("redis://{refused}/")).unwrap());
        let error = load_cluster_state(&shared, None).await.unwrap_err();
        assert_eq!(
            go_error_text(&error),
            "ERR This instance has cluster support disabled"
        );
    }
}
