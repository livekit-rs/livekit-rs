//! Server assembly and lifecycle (config-cli.md §4.6, §5).

use std::net::{Ipv4Addr, Ipv6Addr, SocketAddr};
use std::sync::Arc;
use std::time::Duration;

use axum::serve::ListenerExt;
use lk_compat::CompatServices;
use lk_compat::auth::{KeyStore, TokenClaims, TokenMinter};
use lk_compat::webhook::WebhookNotifier;
use lk_config::LoadedConfig;
use rtc_core::config::ServerConfig;
use rtc_core::events::{EventSink, NullSink};
use rtc_core::permission::VideoGrant;
use rtc_runtime::RoomManager;
use rtc_runtime::net::NetRouter;
use tokio::sync::watch;

use crate::metrics::shutdown::Shutdown;
use crate::{https::HttpsListener, interfaces, logging, metrics, turn};

#[derive(Debug, thiserror::Error)]
pub enum ServerError {
    #[error("{0}")]
    Profile(#[from] crate::profiling::ProfileError),
    #[error("{0}")]
    Webhook(String),
    #[error("could not connect SIP configuration catalog: {0}")]
    SipCatalog(#[from] lk_compat::sip::catalog::CatalogError),
    #[error("could not start worker services: {0}")]
    Worker(#[from] lk_compat::service_bus::rpc::RpcError),
    #[error("could not configure HTTPS: {0}")]
    Https(#[from] rtc_tls::TlsError),
    #[error("TCP is forced but not configured")]
    ForcedTcpWithoutPort,
    /// Upstream's own listen error when the cause is a Go-format bind failure.
    #[error("{}", media_text(.0))]
    Media(std::io::Error),
    /// Go `*net.OpError` of `net.Listen("tcp", JoinHostPort(host, port))`.
    #[error("listen tcp {0}: bind: {errno}", errno = lk_config::go_errno_text(.1))]
    Listen(String, std::io::Error),
    #[error("listen tcp: {0}")]
    ListenResolve(String),
    #[error("{0}")]
    Turn(#[from] turn::TurnStartError),
    /// `unable to connect to redis: <go-redis error>` (GetRedisClient Ping).
    #[error("{0}")]
    RedisConnect(String),
    /// `router.RemoveDeadNodes()` at the tail of `InitializeServer`
    /// (`pkg/service/server.go:197`). Upstream returns the sweep's error unwrapped
    /// apart from `errors.Wrap(err, "could not list nodes")` on the HVALS itself.
    #[error("{}", .0.go_startup_prune_text())]
    NodePrune(#[from] lk_compat::room_control::registry::RegistryError),
    /// `routing.NewLocalNode` refusing an empty `rtc.node_ip`
    /// (`pkg/routing/node.go:53-55`), returned by `startServer` before
    /// `InitializeServer` even runs (`cmd/server/main.go:295-298`).
    #[error(transparent)]
    NodeIdentity(#[from] lk_compat::routing::LocalNodeError),
    /// `InitializeServer` construction errors (selector, WebRTC IP filter,
    /// embedded TURN). Upstream reaches these after `getConfig`/`ValidateKeys`/
    /// `LoadTURNSecrets`, and `main.go:190-192` prints `err.Error()` verbatim.
    #[error("{0}")]
    Wiring(lk_config::ConfigError),
    /// `server.Start()` failing at `RegisterNode`/`router.Start()`
    /// (`pkg/service/server.go:222-233`).
    #[error(transparent)]
    NodeStart(#[from] crate::node::NodeStartError),
    /// `rpc.NewKeepalivePubSub(clientParams)` (`pkg/service/wire_gen.go:57`),
    /// which runs while the router is being built — before `Start()`. The
    /// Rust equivalent dials the node's own pubsub connection there.
    #[error("could not create keepalive pubsub: {0}")]
    KeepaliveTransport(String),
    #[error("could not start cluster services: {0}")]
    ClusterStart(String),
    /// WebTransport TLS setup error (`pkg/service/webtransport.go`); the
    /// missing-cert message is surfaced verbatim as a fatal startup error.
    #[error("{0}")]
    WebTransport(String),
    /// `trace.jaeger_url` OTLP endpoint setup failure (`protocol` jaeger.
    /// Configure; `must.Get` on a bad exporter build is fatal upstream).
    #[error("{0}")]
    Jaeger(#[from] crate::jaeger::JaegerError),
}

/// Identical allocation policy for local and Redis-backed room ownership.
/// `pub` so the C96 config_projection legs can drive the real loading path
/// through this exact projection site instead of a hand-built `Config`.
pub fn allocation_selector_config(
    loaded: &LoadedConfig,
) -> lk_compat::room_control::selector::Config {
    let selector = &loaded.official.node_selector;
    lk_compat::room_control::selector::Config {
        kind: selector.kind.clone(),
        sort_by: selector.sort_by.clone(),
        algorithm: selector.algorithm.clone(),
        cpu_limit: selector.cpu_load_limit,
        sysload_limit: selector.sysload_limit,
        region: loaded.official.region.clone(),
        regions: selector
            .regions
            .iter()
            .map(|r| lk_compat::room_control::selector::Region {
                name: r.name.clone(),
                lat: r.lat,
                lon: r.lon,
            })
            .collect(),
        track_limit: loaded.official.limit.num_tracks,
        bytes_limit: loaded.official.limit.bytes_per_sec,
    }
}

/// `InitializeServer`'s agent-service parameters (`cmd/server/main.go:303` →
/// `agent.NewService`). `pub` so the C98-B2 config_projection legs assert the
/// `agents.*` leaves through this exact site instead of a hand-built `Config`.
pub fn agent_service_config(
    official: &lk_config::official::Config,
) -> lk_compat::agent::service::ServiceConfig {
    let agents = &official.agents;
    lk_compat::agent::service::ServiceConfig {
        signal_message_size_limit: official.limit.agent_signal_message_size_limit,
        enable_recording: agents.enable_user_data_recording,
        enable_redaction: agents.enable_user_data_redaction,
        target_load: agents.target_load,
        endpoints_disabled: agents.endpoints.disabled,
        endpoints_max_streams: agents.endpoints.max_streams,
        endpoints_max_tunnels: agents.endpoints.max_tunnels,
        endpoints_tunnel_drain_timeout_ns: agents.endpoints.tunnel_drain_timeout.nanos(),
    }
}

/// `rpc.GetBusOpts`' retry half: one `psrpc.*` group feeds the WHIP client and
/// the worker graph, so both sites must read the same policy.
pub fn psrpc_retry_policy(
    rpc: &lk_config::official::PsrpcConfig,
) -> lk_compat::workers::retry::RpcPolicy {
    lk_compat::workers::retry::RpcPolicy::from_raw(
        rpc.max_attempts,
        rpc.timeout.nanos(),
        rpc.backoff.nanos(),
        rpc.buffer_size,
    )
}

/// `rpc.CompressionConfig` → the bus's compression options and the
/// decompressed-size ceiling, shared by the local and the worker bus.
pub fn psrpc_compression(
    rpc: &lk_config::official::PsrpcConfig,
) -> (lk_compat::service_bus::wire::CompressionPolicy, usize) {
    let c = &rpc.compression;
    let unlimited = isize::MAX as usize - 1;
    (
        lk_compat::service_bus::wire::CompressionPolicy {
            quality: c.quality.clamp(0, 9) as i32,
            threshold: worker_compression_threshold(c.threshold),
        },
        if c.max_decompressed_size <= 0 {
            unlimited
        } else {
            usize::try_from(c.max_decompressed_size)
                .unwrap_or(unlimited)
                .min(unlimited)
        },
    )
}

/// `ingress.*` as the worker graph hands them to the ingress worker.
pub fn worker_ingress_config(
    ingress: &lk_config::official::IngressConfig,
) -> lk_compat::workers::ingress::IngressConfig {
    lk_compat::workers::ingress::IngressConfig {
        rtmp_base_url: ingress.rtmp_base_url.clone(),
        whip_base_url: ingress.whip_base_url.clone(),
        enable_udp_url_pull: ingress.enable_udp_url_pull,
    }
}

/// `node_stats.*` for the cluster node-state publisher.
pub fn node_stats_policy(
    official: &lk_config::official::Config,
) -> lk_compat::routing::NodeStatsPolicy {
    lk_compat::routing::NodeStatsPolicy {
        update_interval: official.node_stats.stats_update_interval.to_std(),
        max_delay: official.node_stats.stats_max_delay.to_std(),
    }
}

/// `node_stats.stats_update_interval` and `stats_rate_measurement_intervals` as
/// `node_stats::Sampler::new` receives them.
pub fn node_stats_sampler_intervals(
    official: &lk_config::official::Config,
) -> (std::time::Duration, Vec<std::time::Duration>) {
    (
        official.node_stats.stats_update_interval.to_std(),
        official
            .node_stats
            .stats_rate_measurement_intervals
            .iter()
            .map(|interval| interval.to_std())
            .collect(),
    )
}

/// `signal_relay.connect_attempts`, clamped to a count the retry loop accepts.
pub fn signal_connect_attempts(official: &lk_config::official::Config) -> usize {
    official.signal_relay.connect_attempts.max(0) as usize
}

/// Go `redis.IsConfigured` (`protocol-stable/redis/redis.go:48-64`): address OR
/// sentinel OR cluster, in that order of precedence.
pub fn redis_configured(redis: &lk_config::official::RedisConfig) -> bool {
    !redis.address.is_empty()
        || !redis.sentinel_addresses.is_empty()
        || !redis.cluster_addresses.is_empty()
}

/// `trace.jaeger_url`: an empty URL means no exporter is installed at all.
pub fn jaeger_endpoint(official: &lk_config::official::Config) -> Option<&str> {
    (!official.trace.jaeger_url.is_empty()).then_some(official.trace.jaeger_url.as_str())
}

/// `config.SignalRelayConfig` → the cluster signal relay's parameters, in the
/// order Go's `NewSignalRelayClient` reads them; the non-negative clamps are the
/// ones the port needs before `RelayConfig::validate` runs.
pub fn signal_relay_config(
    official: &lk_config::official::Config,
) -> lk_compat::cluster_signal::RelayConfig {
    let relay = &official.signal_relay;
    lk_compat::cluster_signal::RelayConfig {
        retry_timeout: relay.retry_timeout.to_std(),
        min_retry_interval: relay.min_retry_interval.to_std(),
        max_retry_interval: relay.max_retry_interval.to_std(),
        stream_buffer_size: relay.stream_buffer_size.max(1) as usize,
        connect_attempts: relay.connect_attempts.max(0) as usize,
    }
}

/// `server.go:184-195`: the WebTransport TLS material exists only while
/// `webtransport.port > 0`, and a port without a cert pair is fatal outside
/// development mode.
pub fn webtransport_tls(
    official: &lk_config::official::Config,
    development: bool,
) -> Result<Option<lk_webtransport::ServerTls>, crate::webtransport::SetupError> {
    let wt = &official.webtransport;
    if wt.port == 0 {
        return Ok(None);
    }
    Ok(Some(crate::webtransport::build_server_tls(
        &wt.tls_cert_file,
        &wt.tls_key_file,
        development,
    )?))
}

fn media_text(e: &std::io::Error) -> String {
    match e
        .get_ref()
        .and_then(|inner| inner.downcast_ref::<rtc_runtime::net::ListenError>())
    {
        Some(listen) => listen.to_string(),
        None => format!("could not start media sockets: {e}"),
    }
}

/// Upstream HTTP shutdown timeout.
const HTTP_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(5);

/// Media readers retain router clones. Every exit after their creation must
/// cancel the shared owner, including worker/bootstrap/embedded-TURN failures.
struct MediaShutdownGuard(NetRouter);
impl Drop for MediaShutdownGuard {
    fn drop(&mut self) {
        self.0.shutdown();
    }
}

fn worker_compression_threshold(raw: i64) -> usize {
    // Fixed psrpc/pkg/bus/options.go GetBusOpts applies the default *after*
    // options: both zero and negative are 1024, not compress-every-message.
    if raw <= 0 {
        1024
    } else {
        usize::try_from(raw).unwrap_or(usize::MAX)
    }
}

/// The node's public HTTP handler. Development mode serves `http.DefaultServeMux`,
/// which carries the debug handlers alongside the API routes
/// (`pkg/service/server.go:120-125`); production builds a fresh mux without them.
pub(crate) fn http_app(
    state: &lk_compat::AppState,
    manager: &RoomManager,
    development: bool,
) -> axum::Router {
    let mux = lk_compat::http::api_routes(state.clone());
    let mux = if development {
        mux.merge(crate::debug::api_routes(manager.clone()))
    } else {
        mux
    };
    lk_compat::http::api_router(state.clone(), mux)
}

pub async fn run(loaded: LoadedConfig) -> Result<(), ServerError> {
    // getConfig/ValidateKeys/LoadTURNSecrets already succeeded in lk-config.
    // Keep a real owner across every later service/startup error and shutdown.
    let _profiles = crate::profiling::Profiles::start(
        loaded.cpu_profile.as_deref(),
        loaded.mem_profile.as_deref(),
    )?;
    let allocation_selector = allocation_selector_config(&loaded);
    let cfg = Arc::new(loaded.server);
    logging::init(&cfg.logging);
    // SPEC-0074 C16: mirror upstream `cmd/server/main.go:253` — install the
    // global OTLP/Jaeger tracer provider before the rest of startup when
    // `trace.jaeger_url` is set. It emits no startup line, so the server runs
    // with the identical default startup output as the Go binary.
    let mut tracing_provider = match jaeger_endpoint(&loaded.official) {
        Some(url) => Some(crate::jaeger::configure(url, "livekit")?),
        None => None,
    };
    // Startup log lines (SPEC-0074 C32): emitted through the zap encoder in
    // upstream order — deprecation/config warnings first (some are logged and
    // swallowed upstream before its logger exists), then ValidateKeys' zap
    // ERROR line, then the dev-mode INFO pair.
    for w in &loaded.warnings {
        logging::notify(
            crate::zap::WARN,
            "livekit",
            "pkg/service/server.go:149",
            w,
            &[],
            None,
        );
    }
    for e in &loaded.warning_extras {
        logging::notify(
            crate::zap::ERROR,
            "livekit",
            "pkg/config/config.go:850",
            &e.message,
            &[(e.field, crate::zap::ZapValue::Str(e.value.clone()))],
            None,
        );
    }
    // Upstream logs the development-mode notices from `getConfig`, which every
    // command runs (`cmd/server/main.go:208-214`) — the same helper the CLI uses.
    logging::replay_config_notices(&loaded.notices);

    // `routing.NewLocalNode(conf)` (`cmd/server/main.go:295`) runs before
    // `InitializeServer`, so an empty `rtc.node_ip` is the earliest fatal startup
    // error — ahead of the Redis dial, the notifier, every socket bind and the
    // readiness line. The record it produces is what `Start()` HSETs and what
    // `/` reads: `id` from the process's generated `ND_` identity, `ip` from
    // `NodeIP.PrimaryIP()` as a raw string (never parsed, SPEC-0074 C30),
    // `num_cpus` from the runtime CPU count (`runtime.NumCPU()`), `region` from
    // `conf.Region`, and SERVING stats stamped with `time.Now().Unix()`.
    let current_node = Arc::new(
        lk_compat::routing::LocalNode::serving(
            cfg.node_id.clone(),
            official_node_ip_primary(&loaded.official),
            loaded.official.region.clone(),
            std::thread::available_parallelism()
                .map(|count| count.get() as u32)
                .unwrap_or(1),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|elapsed| elapsed.as_secs() as i64)
                .unwrap_or_default(),
        )
        .map_err(ServerError::NodeIdentity)?,
    );

    // `InitializeServer` (`cmd/server/main.go:303`) builds the room allocator's
    // selector, the WebRTC IP filter and the embedded TURN server. Those
    // constructions — not `getConfig` — own these refusals upstream.
    lk_config::validate_server_wiring(&loaded.official).map_err(ServerError::Wiring)?;

    // Webhooks (fail fast like upstream wire.go).
    let mut services =
        CompatServices::new(cfg.clone()).with_agent_config(agent_service_config(&loaded.official));
    services = services.with_signal_connect_attempts(signal_connect_attempts(&loaded.official));
    let rpc_policy = &loaded.official.psrpc;
    services = services.with_whip_policy(psrpc_retry_policy(rpc_policy));
    // Source getMessageBus applies PSRPC bus options to localbus as well as Redis.
    let (local_compression, local_max_decompressed) = psrpc_compression(rpc_policy);
    services = services.with_local_rpc_compression(local_compression, local_max_decompressed);
    let redis = &loaded.official.redis;
    // SPEC-0074 C11/C12: `redis.IsConfigured` is address OR sentinel OR cluster
    // (`references/protocol-stable/redis/redis.go:48-64`), so a cluster-only
    // configuration is a configured Redis deployment and must dial it. Upstream
    // branches in that same order — Sentinel, then Cluster, then the address —
    // and never reaches the cluster client while Sentinel addresses are present
    // (`redis.go:100-155`), so a shared client here means one of the three is set.
    let redis_enabled = redis_configured(redis);
    let mut redis_base: Option<Arc<lk_compat::service_bus::redis_transport::RedisTransport>> = None;
    let redis_endpoint = if redis_enabled {
        // Upstream `GetRedisClient` builds the `tls.Config` (`redis.go:88-98`, whose
        // failure is `xtls.ClientTLSConfig`'s own `could not load client key pair`)
        // *before* it logs the Redis mode (`redis.go:101`/`:132`/`:145`) and before it
        // dials. `redis_endpoint` is that one constructor, so it loads the material,
        // logs the mode and hands back the client: a key pair that cannot be loaded
        // ends startup with its bare message and no "connecting to redis" line at all.
        let endpoint =
            crate::redis_tls::redis_endpoint(redis).map_err(ServerError::RedisConnect)?;
        // One pooled command connection + one pub/sub multiplexer for the whole
        // process, mirroring upstream's single borrowed `redis.UniversalClient`
        // (`redisbus.New`): the SIP catalog, worker buses, IO store, node registry
        // and keepalive/cluster bus all ride this one identity instead of each
        // eagerly dialling its own pool. The base is dialled here because the SIP
        // catalog is assembled before the worker policy exists; every real bus
        // later derives its configured wire/inbox limits through `with_limits`,
        // so no compression or decompression setting is ignored.
        let limits = lk_compat::service_bus::redis_transport::TransportLimits {
            wire: lk_compat::service_bus::wire::WireLimits {
                encoded_bytes: isize::MAX as usize - 1,
                decompressed_bytes: isize::MAX as usize - 1,
            },
            compression: lk_compat::service_bus::wire::CompressionPolicy {
                quality: 0,
                threshold: 0,
            },
            inbox_messages: 100,
            subscriptions: 4096,
            commands: 1000,
            io_timeout: Duration::from_secs(5),
        };
        let topology = !redis.sentinel_addresses.is_empty() || !redis.cluster_addresses.is_empty();
        // Upstream `GetRedisClient` wraps a failed client dial/Ping with
        // `unable to connect to redis: %w` (`redis.go:158-161`). The eager
        // command-pool dial surfaces that failure as `TransportError::Connect`
        // carrying the Go `net.OpError` text; any other transport error keeps its
        // own message. The discovery Ping (`ping_with`/`ping_base`) already applies
        // the same wrapper internally, so this only governs the pool dial.
        let redis_connect_err =
            |error: lk_compat::service_bus::redis_transport::TransportError| match error {
                lk_compat::service_bus::redis_transport::TransportError::Connect(text) => {
                    ServerError::RedisConnect(format!("unable to connect to redis: {text}"))
                }
                other => ServerError::RedisConnect(other.to_string()),
            };
        let base = if topology {
            // SPEC-0074 C10/C11/C12: the Sentinel/Cluster `GetRedisClient` Ping
            // (`redis.go:158-161`) runs on the discovery client `redis_endpoint`
            // built, with that branch's own dial/read budget — never a second mode
            // log. Its discovery diagnostics and short budget are certified
            // (C08/C12), so the bus pool is dialled separately afterwards.
            crate::cli_nodes::cli_redis_retry::ping_with(&endpoint, redis)
                .await
                .map_err(|e| ServerError::RedisConnect(e.to_string()))?;
            lk_compat::service_bus::redis_transport::RedisTransport::connect_endpoint(
                endpoint.clone(),
                limits,
            )
            .await
            .map_err(redis_connect_err)?
        } else {
            // Simple/plaintext: upstream's `GetRedisClient` returns the ONE
            // `redis.UniversalClient` that `redisbus.New` then borrows, so the
            // startup Ping and every bus command ride a single pooled connection
            // (`redis.go:156-161`). Build that pool first and run the Ping on it,
            // so a successful start opens one command connection + one pub/sub
            // multiplexer — Go's two-connection shape — instead of a throwaway
            // Ping dial plus a separate bus pool.
            let base = lk_compat::service_bus::redis_transport::RedisTransport::connect_endpoint(
                endpoint.clone(),
                limits,
            )
            .await
            .map_err(redis_connect_err)?;
            crate::cli_nodes::cli_redis_retry::ping_base(base.command_connection(), redis)
                .await
                .map_err(|e| ServerError::RedisConnect(e.to_string()))?;
            base
        };
        let catalog = lk_compat::sip::catalog::SipCatalog::from_shared(
            base.command_connection(),
            Duration::from_secs(5),
            128,
        )?;
        services = services.with_sip_catalog(catalog);
        redis_base = Some(base);
        Some(endpoint)
    } else {
        None
    };
    let notifier = if cfg.webhook.urls.is_empty() && !redis_enabled {
        None
    } else {
        Some(Arc::new(
            WebhookNotifier::new(
                &cfg.webhook,
                services.keys().clone(),
                cfg.room.enable_full_room_in_webhooks,
            )
            .map_err(|e| ServerError::Webhook(e.to_string()))?,
        ))
    };
    let events: Arc<dyn EventSink> = match &notifier {
        Some(n) => n.clone(),
        None => Arc::new(NullSink),
    };

    // Validate certificates and reserve every HTTP/HTTPS/metrics address before
    // spawning any tasks. A failed bind drops all prior sockets transactionally.
    let https_cfg = cfg.extension.as_ref().and_then(|e| e.https.as_ref());
    let tls = https_cfg
        .map(|h| {
            rtc_tls::server_config(&h.cert_file, &h.key_file)
                .map(|c| tokio_rustls::TlsAcceptor::from(Arc::new(c)))
        })
        .transpose()?;
    // InitializeServer binds the RTC UDP/TCP media sockets before
    // LivekitServer.Start listens on HTTP/metrics.
    let net_cfg = interfaces::net_config(&cfg.rtc, cfg.development).map_err(ServerError::Media)?;
    let rtc = &loaded.official.rtc;
    let tcp_port = (rtc.tcp_port != 0).then_some(i64::from(rtc.tcp_port));
    if rtc.force_tcp && tcp_port.is_none() {
        return Err(ServerError::ForcedTcpWithoutPort);
    }
    let net = NetRouter::start_with_configured_ports(
        net_cfg,
        &interfaces::startup_mux_ports(&cfg.rtc, rtc.udp_port),
        interfaces::standalone_stun_mux_port(&cfg.rtc),
        tcp_port,
        cfg.rtc.advertise_internal_ip,
        (cfg.rtc.use_node_ip_mapping && !cfg.rtc.use_external_ip).then_some(&cfg.rtc.node_ip),
    )
    .await
    .map_err(ServerError::Media)?;
    if loaded.node_ip_auto_generated && !cfg.rtc.use_external_ip && !cfg.rtc.use_ice_lite {
        let servers = if cfg.rtc.stun_servers.is_empty() {
            lk_config::netdetect::DEFAULT_STUN_SERVERS
                .iter()
                .map(|endpoint| endpoint.to_string())
                .collect()
        } else {
            cfg.rtc.stun_servers.clone()
        };
        net.enable_server_stun(servers);
    }
    // SPEC-0074 C21: `rtc.batch_io.*` wraps the UDP mux in a batched writer
    // only when batch_size > 0 (webrtc_config.go:146). Disabled by default, so
    // this is a no-op for the common ephemeral-range configuration.
    net.enable_batch(cfg.rtc.batch_write_size, cfg.rtc.batch_write_interval)
        .await;
    let _media_shutdown = MediaShutdownGuard(net.clone());
    let bind_hosts = bind_hosts(&cfg);

    // SPEC-0074 C15: the WebTransport listener needs the assembled agent worker
    // control-plane state (api keys for the auth middleware plus the shared
    // registry/dispatch loop for the `/agent` data plane). It is read from
    // `state.agents()` after `assemble` and handed to the pumps at spawn time.

    let mut state = services.assemble(net.clone(), events);
    // `LivekitServer.Node()` is `s.currentNode.Clone()` (`server.go:204-206`), the
    // record the `/` health handler reads in *both* routing modes.
    state.enable_node(current_node.clone());
    if !redis_enabled {
        state.enable_local_allocation_selector(allocation_selector.clone())?;
    }
    // `CreateRouter` logs single-node routing where the router is built, before
    // any listener binds (pkg/routing/interfaces.go:181).
    if !redis_enabled {
        logging::notify(
            crate::zap::INFO,
            "livekit",
            "pkg/routing/interfaces.go:181",
            "using single-node routing",
            &[],
            None,
        );
    }
    // SPEC-0074 A10: the worker graph exists in both modes. Upstream derives
    // `getMessageBus` from the same client params — `psrpc.NewLocalMessageBus`
    // when the Redis client is nil — so only the transport and the store handles
    // differ; the policy does not.
    let rpc = &loaded.official.psrpc;
    let (compression, max_decompressed_size) = psrpc_compression(rpc);
    let worker_policy = lk_compat::worker_services::WorkerPolicy {
        rpc: psrpc_retry_policy(rpc),
        compression,
        max_decompressed_size,
        ingress: worker_ingress_config(&loaded.official.ingress),
        ingress_legacy_psrpc: cfg
            .extension
            .as_ref()
            .is_some_and(|e| e.ingress_worker_protocol == "psrpc-v0.7.1"),
    };
    // The bus options below are shared by the worker graph (which consumes
    // `worker_policy`) and the node's own keepalive pubsub, so the compression
    // policy is read out first.
    let keepalive_compression = worker_policy.compression;
    let whip_rpc_policy = worker_policy.rpc;
    // The startup sweep needs the same connection source the Ping used, but it
    // runs later — at the tail of construction — so keep a second handle.
    let prune_endpoint = redis_endpoint.clone();
    if let Some(endpoint) = redis_endpoint {
        // SPEC-0074 C10/C11: the resolved Redis endpoint (simple TLS client or
        // Sentinel master) flows to every downstream connection. A Sentinel
        // endpoint re-resolves the current master for each new dial, so worker
        // subscriptions and WATCH transactions follow `+switch-master`.
        state
            .enable_workers_shared(
                lk_compat::worker_services::WorkerConfig {
                    redis: endpoint,
                    rpc: worker_policy.rpc,
                    compression: worker_policy.compression,
                    max_decompressed_size: worker_policy.max_decompressed_size,
                    ingress: worker_policy.ingress,
                    ingress_legacy_psrpc: worker_policy.ingress_legacy_psrpc,
                },
                redis_base
                    .clone()
                    .expect("redis base identity is built whenever redis is enabled"),
                notifier
                    .as_deref()
                    .expect("Redis worker events have an owned notifier")
                    .clone(),
            )
            .await?;
    } else {
        state
            .enable_local_workers(worker_policy, notifier.as_deref().cloned())
            .await?;
    }
    state
        .agents()
        .enable_cluster_rpc(
            state
                .worker_message_bus()
                .expect("worker graph is assembled before agent discovery"),
        )
        .await
        .map_err(|e| ServerError::ClusterStart(e.to_string()))?;
    let manager = state.manager().clone();
    let (stats_interval, stats_intervals) = node_stats_sampler_intervals(&loaded.official);
    current_node.set_stats_provider(Arc::new(
        crate::node_stats::Sampler::new(manager.clone(), stats_interval, stats_intervals).map_err(
            |e| {
                ServerError::Media(std::io::Error::new(
                    e.kind(),
                    format!("could not initialize node statistics: {e}"),
                ))
            },
        )?,
    ));

    let turn_handle = turn::start(&cfg, &loaded.official.rtc, state.turn_auth().clone()).await?;

    // SPEC-0074 C15 / 21q: the WebTransport *TLS material* is built during
    // construction (`server.go:184-195`, `WebTransportTLS(...)` at :186) — before
    // `router.RemoveDeadNodes()` (:197) and long before `RegisterNode()` (:222) —
    // while the UDP listener is opened inside `Start()` (:272-276), after the HSET.
    // Keeping that split prevents a bad cert pair from leaving a node record.
    // A port without a cert pair is fatal in non-development mode. The TLS
    // startup error is printed on stdout with exit 1.
    let wt = &loaded.official.webtransport;
    let wt_tls = webtransport_tls(&loaded.official, cfg.development)
        .map_err(|e| ServerError::WebTransport(e.to_string()))?;
    // SPEC-0074 C12 / SPEC-0076 workline B: `InitializeServer` ends with
    // `router.RemoveDeadNodes()` (`pkg/service/server.go:197`), after the whole
    // component graph exists and before `Start()` binds anything. The sweep reads
    // the `nodes` hash through the client the startup Ping produced, so a Sentinel
    // deployment prunes the current master and a Cluster deployment the slot owner,
    // and it HDELs every entry `selector.IsAvailable` rejects — a stale stats
    // timestamp (`AvailableSeconds = 5`), while nil stats and a future timestamp
    // stay. A failure here is fatal and upstream logs nothing on success.
    //
    // The same client then becomes the node's own registration store: upstream has
    // one `redis.UniversalClient` in `RedisRouter` for the sweep, `RegisterNode`,
    // `UnregisterNode` and the ping-driven re-HSET, so the handle is kept rather
    // than dropped. `rpc.NewKeepalivePubSub(clientParams)`
    // (`pkg/service/wire_gen.go:57`) is built in that same construction phase,
    // before `CreateRouter`, which is why the pubsub connection is dialled here and
    // not inside `Start()`.
    let stats_policy = node_stats_policy(&loaded.official);
    let cluster_endpoint = prune_endpoint.clone();
    let (node_registry, keepalive_bus, cluster_bus) = match (prune_endpoint, redis_base.as_ref()) {
        (Some(_endpoint), Some(base)) => {
            // `ListNodes` (`redisrouter.go:137-151`) puts no size ceiling on the
            // stored node protobuf, so the startup sweep must not invent one either.
            // The sweep rides the same pooled command connection as the rest of the
            // process (upstream's single `RedisRouter` client), not a second pool.
            let registry = Arc::new(
                lk_compat::room_control::registry::RedisRegistry::from_connection(
                    base.command_connection(),
                    Duration::from_secs(5),
                    usize::MAX,
                ),
            );
            registry
                .remove_dead_nodes()
                .await
                .map_err(ServerError::NodePrune)?;
            // psrpc's own bus options apply to the keepalive client and server
            // (`getPSRPCClientParams`), so the transport limits match the worker
            // buses above; the inbox is the server-side handler channel. The
            // keepalive/cluster bus shares the worker transport's single command +
            // pub/sub connection identity instead of redialling a pair.
            let transport = base
                .with_limits(lk_compat::service_bus::redis_transport::TransportLimits {
                    wire: lk_compat::service_bus::wire::WireLimits {
                        encoded_bytes: isize::MAX as usize - 1,
                        // Plain bound stays unlimited; the source GZIP-only receive
                        // cap rides with_gzip_limit below (Go redisbus maxSize).
                        decompressed_bytes: isize::MAX as usize - 1,
                    },
                    compression: keepalive_compression,
                    inbox_messages: 100,
                    subscriptions: 4096,
                    commands: 1000,
                    io_timeout: Duration::from_secs(5),
                })
                .and_then(|t| t.with_gzip_limit(max_decompressed_size))
                .map_err(|e| ServerError::KeepaliveTransport(e.to_string()))?;
            let bus = Arc::new(lk_compat::routing::KeepaliveBus::new(
                transport.clone(),
                &current_node.id(),
            ));
            (Some(registry), Some(bus), Some(transport))
        }
        _ => (None, None, None),
    };
    let node_router = Arc::new(crate::node::NodeRouter::new(
        current_node.clone(),
        node_registry,
        keepalive_bus,
        stats_policy,
    ));
    // Subscribe owner and relay methods before announcing this node. The node
    // record remains exclusively owned by NodeRouter; managed RoomControl never
    // registers, refreshes or removes it independently.
    let mut cluster_guard = ClusterShutdownGuard::default();
    if let (Some(endpoint), Some(bus)) = (cluster_endpoint, cluster_bus) {
        let control = state
            .enable_managed_room_control_component(
                bus.clone(),
                lk_compat::room_control::ManagedConfig {
                    timeout: Duration::from_secs(5),
                    max_protobuf_bytes: usize::MAX,
                    max_pending: tokio::sync::Semaphore::MAX_PERMITS,
                    selector: allocation_selector,
                    limits: lk_compat::service_bus::rpc::ServerLimits {
                        concurrent: tokio::sync::Semaphore::MAX_PERMITS,
                        max_request_duration: Duration::MAX,
                        allow_skip_claim: false,
                    },
                },
                endpoint,
                current_node.clone(),
            )
            .await
            .map_err(|e| ServerError::ClusterStart(e.to_string()))?;
        cluster_guard.rooms = Some(control);
        let signal = state
            .enable_cluster_signal(bus, signal_relay_config(&loaded.official))
            .await
            .map_err(|e| ServerError::ClusterStart(e.to_string()))?;
        cluster_guard.signal = Some(signal);
    }
    // WHIP uses the same actual bus and owner authority as room/signal services.
    // Subscribe the original Create handler before the node is advertised ready.
    state
        .enable_whip_rpc(
            state
                .worker_message_bus()
                .expect("worker bus assembled before WHIP routing"),
            whip_rpc_policy,
        )
        .await?;

    // `signal.Notify` runs between `InitializeServer` and `server.Start()`
    // (`cmd/server/main.go:303-321`), so the handlers are installed before any
    // listener accepts. Registering them at the shutdown wait instead leaves a
    // window where the port is connectable but a signal still hits the default
    // disposition and kills the process without the shutdown log line.
    let mut signals = Signals::new();

    // `LivekitServer.Start()` opens with `RegisterNode()` then `router.Start()`
    // (`server.go:222-233`): the HSET lands and the keepalive subscription is
    // acknowledged before anything serves. Nothing is announced as ready before
    // this returns `Ok`, and a failure here leaves no task, subscription or node
    // record behind (the deferred `UnregisterNode` still runs upstream).
    node_router.start().await.map_err(ServerError::NodeStart)?;

    let (stop_tx, stop_rx) = watch::channel(false);
    let (force_tx, force_rx) = watch::channel(false);
    // `webrtc_config.go` logs the explicit-node-IP NAT rewrite when the node IP
    // was configured (not discovered) and external-IP discovery is off. Only
    // the `use_external_ip == false` branch reaches this line.
    if cfg.rtc.use_node_ip_mapping && !loaded.official.rtc.use_external_ip {
        let node = &loaded.official.rtc.node_ip;
        logging::notify(
            crate::zap::INFO,
            "livekit",
            "pkg/rtcconfig/webrtc_config.go:112",
            "using explicit node IP for NAT1To1Ips",
            &[
                (
                    "ip",
                    crate::zap::ZapValue::Object(vec![
                        ("V4".into(), crate::zap::ZapValue::Str(node.v4.clone())),
                        ("V6".into(), crate::zap::ZapValue::Str(node.v6.clone())),
                    ]),
                ),
                (
                    "advertiseInternalIP",
                    crate::zap::ZapValue::Bool(cfg.rtc.advertise_internal_ip),
                ),
            ],
            None,
        );
    }
    // Start opens the listeners after the initialization sweep and node
    // registration. An earlier bind lets TCP connect while initialization can
    // still fail and gives socket errors precedence over the sweep's error.
    let bound = async {
        let http_enabled = https_cfg.is_none_or(|h| h.http_enabled);
        let debug_port = loaded.official.debug_handler.port as u16;
        let mut http_sockets = Vec::new();
        let mut metric_sockets = Vec::new();
        let mut debug_sockets = Vec::new();
        for host in &bind_hosts {
            if http_enabled {
                http_sockets.push(listen(host, cfg.http.port).await?);
            }
            if let Some(port) = cfg.prometheus.port {
                metric_sockets.push(listen(host, port).await?);
            }
            if debug_port != 0 {
                debug_sockets.push(listen(host, debug_port).await?);
            }
        }
        let mut https_sockets = Vec::new();
        if let Some(h) = https_cfg {
            let explicit_hosts: Vec<String> =
                h.bind_addresses.iter().map(ToString::to_string).collect();
            let hosts = if explicit_hosts.is_empty() {
                &bind_hosts
            } else {
                &explicit_hosts
            };
            for host in hosts {
                https_sockets.push(listen(host, h.port).await?);
            }
        }
        let wt_listener = match wt_tls {
            Some(tls) => Some(
                crate::webtransport::bind(&tls, &bind_hosts, wt.port as u16)
                    .await
                    .map_err(|error| ServerError::WebTransport(error.to_string()))?,
            ),
            None => None,
        };
        Ok::<_, ServerError>((
            http_sockets,
            metric_sockets,
            debug_sockets,
            https_sockets,
            wt_listener,
        ))
    }
    .await;
    let (http_sockets, metric_sockets, debug_sockets, https_sockets, wt_listener) = match bound {
        Ok(listeners) => listeners,
        Err(error) => {
            // Go defers one unregister on a failed bind. stop() retires the
            // keepalive and removes the registration in that order.
            node_router.stop().await;
            cluster_guard.shutdown().await;
            state.agents().shutdown().await;
            state.shutdown_whip().await;
            if let Some(t) = turn_handle {
                t.shutdown().await;
            }
            state.shutdown_workers().await;
            manager.drain_recording_work().await;
            if let Some(n) = notifier {
                n.stop(false).await;
            }
            return Err(error);
        }
    };
    log_startup(&cfg, &loaded.official, net.config());
    let mut listeners = Vec::new();
    // `SignalServer.Start` registers relay topics right after the readiness
    // line and before HTTP serving (pkg/service/signal.go:120, DEBUG level).
    logging::notify(
        crate::zap::DEBUG,
        "livekit",
        "pkg/service/signal.go:120",
        "starting relay signal server",
        &[("topic", crate::zap::ZapValue::Str(cfg.node_id.clone()))],
        None,
    );
    // Go pkg/service/server.go:333-338: with no HTTP listeners httpGroup.Wait()
    // returns nil, so the watchdog logs "could not start server" (Errorw with a
    // nil error carries no field) and calls Stop(true). Stop returns early
    // because `running` is still false, so the process stays alive. Go builds
    // its listeners purely from BindAddresses, so the condition is an empty
    // bind list — not the Rust-only livecloud.https http_enabled toggle.
    if bind_hosts.is_empty() {
        logging::notify(
            crate::zap::ERROR,
            "livekit",
            "pkg/service/server.go:335",
            "could not start server",
            &[],
            None,
        );
    }
    for listener in http_sockets {
        let app = http_app(&state, &manager, cfg.development);
        let shutdown = Shutdown(stop_rx.clone());
        listeners.push(tokio::spawn(async move {
            if let Err(error) = axum::serve(
                listener,
                app.into_make_service_with_connect_info::<SocketAddr>(),
            )
            .with_graceful_shutdown(shutdown.wait())
            .await
            {
                logging::notify(
                    crate::zap::ERROR,
                    "livekit",
                    "pkg/service/server.go:335",
                    "could not start server",
                    &[(
                        "error",
                        crate::zap::ZapValue::Str(format!("HTTP server failed: {error}")),
                    )],
                    None,
                );
            }
        }));
    }
    if let (Some(h), Some(tls)) = (https_cfg, tls) {
        let slots = Arc::new(tokio::sync::Semaphore::new(h.max_connections));
        for socket in https_sockets {
            let listener = HttpsListener::new(
                socket,
                tls.clone(),
                slots.clone(),
                h.handshake_timeout,
                force_rx.clone(),
            );
            let app = http_app(&state, &manager, cfg.development)
                .layer(axum::Extension(lk_compat::agent::endpoint::TlsTransport));
            let shutdown = Shutdown(stop_rx.clone());
            listeners.push(tokio::spawn(async move {
                // TapIo's generic Connected implementation preserves the actual
                // remote SocketAddr required by the official adapter.
                if let Err(error) = axum::serve(
                    listener.tap_io(|_| {}),
                    app.into_make_service_with_connect_info::<SocketAddr>(),
                )
                .with_graceful_shutdown(shutdown.wait())
                .await
                {
                    logging::notify(
                        crate::zap::ERROR,
                        "livekit",
                        "pkg/service/server.go:335",
                        "could not start server",
                        &[(
                            "error",
                            crate::zap::ZapValue::Str(format!("HTTPS server failed: {error}")),
                        )],
                        None,
                    );
                }
            }));
        }
    }
    let auth = match (&cfg.prometheus.username, &cfg.prometheus.password) {
        (Some(u), Some(p)) => Some((u.clone(), p.clone())),
        _ => None,
    };
    for listener in metric_sockets {
        let app = metrics::router(manager.clone(), auth.clone(), cfg.node_id.clone());
        let shutdown = Shutdown(stop_rx.clone());
        listeners.push(tokio::spawn(async move {
            if let Err(error) = metrics::serve(listener, app, shutdown).await {
                logging::notify(
                    crate::zap::ERROR,
                    "livekit",
                    "pkg/service/server.go:335",
                    "could not start server",
                    &[(
                        "error",
                        crate::zap::ZapValue::Str(format!("metrics listener failed: {error}")),
                    )],
                    None,
                );
            }
        }));
    }

    for listener in debug_sockets {
        let app = crate::debug::router(manager.clone());
        let shutdown = Shutdown(stop_rx.clone());
        listeners.push(tokio::spawn(async move {
            if let Err(error) = axum::serve(
                listener,
                app.into_make_service_with_connect_info::<SocketAddr>(),
            )
            .with_graceful_shutdown(shutdown.wait())
            .await
            {
                logging::notify(
                    crate::zap::ERROR,
                    "livekit",
                    "pkg/service/server.go:335",
                    "could not start server",
                    &[(
                        "error",
                        crate::zap::ZapValue::Str(format!("debug server failed: {error}")),
                    )],
                    None,
                );
            }
        }));
    }

    // SPEC-0074 C15: start the WebTransport accept pumps after the readiness
    // line (Go starts serving after `logger.Infow("starting LiveKit server")`).
    // The pumps watch `stop_rx` and close their endpoints last, so peers still
    // receive the CONNECTION_CLOSE (webtransport.go Shutdown ordering).
    if let Some(listener) = wt_listener {
        listeners.extend(listener.spawn(
            state.agents().worker_state(),
            crate::webtransport::REGISTER_TIMEOUT,
            stop_rx.clone(),
        ));
    }

    wait_for_shutdown(&mut signals, &manager, &node_router, state.agents()).await;
    cluster_guard.shutdown().await;
    node_router.stop().await;
    state.agents().shutdown().await;
    state.shutdown_whip().await;

    let _ = stop_tx.send(true);
    drain_listener_tasks(listeners, &force_tx, HTTP_SHUTDOWN_TIMEOUT).await;
    if let Some(t) = turn_handle {
        t.shutdown().await;
    }
    state.shutdown_workers().await;
    manager.drain_recording_work().await;
    if let Some(n) = notifier {
        // Go stops the server without waiting for webhook retries or backlog.
        n.stop(false).await;
    }
    // `defer UnregisterNode()` of `LivekitServer.Start()` (`server.go:226-229`)
    // runs when `Start()` returns, i.e. after `Stop()` already made
    // `RedisRouter.Stop()` HDEL the same id. A clean shutdown therefore deletes
    // the key twice, and the second one is this line — not the earlier stop.
    node_router.final_unregister().await;
    if let Some(provider) = tracing_provider.as_mut() {
        provider.shutdown().await?;
    }
    Ok(())
}

/// Await each owned listener exactly once. A timeout may occur after earlier
/// handles completed: polling those completed JoinHandles a second time panics.
async fn drain_listener_tasks(
    mut listeners: Vec<tokio::task::JoinHandle<()>>,
    force: &watch::Sender<bool>,
    timeout: Duration,
) {
    let mut completed = 0;
    let drained = tokio::time::timeout(timeout, async {
        for listener in &mut listeners {
            let _ = listener.await;
            completed += 1;
        }
    })
    .await;
    let _ = force.send(true);
    if drained.is_err() {
        // The active await was cancelled by timeout. Retire it and every later
        // handle; the already-consumed prefix must never be polled again.
        for listener in listeners.into_iter().skip(completed) {
            listener.abort();
            let _ = listener.await;
        }
    }
}

#[derive(Default)]
struct ClusterShutdownGuard {
    signal: Option<Arc<lk_compat::cluster_signal::ClusterSignal>>,
    rooms: Option<Arc<lk_compat::room_control::RoomControl>>,
}
impl ClusterShutdownGuard {
    async fn shutdown(&mut self) {
        if let Some(signal) = self.signal.as_ref() {
            signal.shutdown().await;
        }
        if let Some(rooms) = self.rooms.as_ref()
            && let Err(error) = rooms.shutdown().await
        {
            tracing::error!(%error,"cluster owner shutdown failed");
        }
        self.signal.take();
        self.rooms.take();
    }
}
impl Drop for ClusterShutdownGuard {
    fn drop(&mut self) {
        let signal = self.signal.take();
        let rooms = self.rooms.take();
        if signal.is_none() && rooms.is_none() {
            return;
        }
        if let Ok(runtime) = tokio::runtime::Handle::try_current() {
            runtime.spawn(async move {
                if let Some(signal) = signal {
                    signal.shutdown().await;
                }
                if let Some(rooms) = rooms
                    && let Err(error) = rooms.shutdown().await
                {
                    tracing::error!(%error,"cancelled cluster shutdown failed");
                }
            });
        }
    }
}

/// First SIGINT/SIGTERM/SIGQUIT drains (wait for participants), the second
/// forces room closure; further signals are ignored. Upstream logs each of the
/// first two signals with `os.Signal.String()` and the force flag (main.go:316).
///
/// The router's own shutdown sits inside this sequence exactly where `Stop()`
/// puts it (`server.go:375-399`): `Drain()` as the first act after the log line
/// and before the participant wait, `Stop()` after the wait and before
/// `close(s.doneChan)` — which is what `stop_tx.send(true)` is in `run()`.
async fn wait_for_shutdown(
    signals: &mut Signals,
    manager: &RoomManager,
    node_router: &crate::node::NodeRouter,
    agents: &lk_compat::agent::service::AgentService,
) {
    let sig = signals.next().await;
    logging::notify(
        crate::zap::INFO,
        "livekit",
        "cmd/server/main.go:316",
        "exit requested, shutting down",
        &[
            ("signal", crate::zap::ZapValue::Str(sig.to_string())),
            ("force", crate::zap::ZapValue::Bool(false)),
        ],
        None,
    );
    agents.start_endpoint_drain();
    node_router.drain().await;
    tokio::select! {
        _ = manager.drain() => {}
        sig = signals.next() => {
            agents.force_endpoint_drain();
            logging::notify(
                crate::zap::INFO,
                "livekit",
                "cmd/server/main.go:316",
                "exit requested, shutting down",
                &[
                    ("signal", crate::zap::ZapValue::Str(sig.to_string())),
                    ("force", crate::zap::ZapValue::Bool(true)),
                ],
                None,
            );
        }
    }
    manager.close_all().await;
}

struct Signals {
    #[cfg(unix)]
    int: tokio::signal::unix::Signal,
    #[cfg(unix)]
    term: tokio::signal::unix::Signal,
    #[cfg(unix)]
    quit: tokio::signal::unix::Signal,
}

impl Signals {
    /// Registers all three handlers at once, as `signal.Notify(sigChan, SIGINT,
    /// SIGTERM, SIGQUIT)` does (`cmd/server/main.go:310`). Registering SIGINT here
    /// rather than through `ctrl_c()` inside `next()` matters: `ctrl_c()` installs
    /// its handler on first poll, which would leave SIGINT at the default
    /// disposition until the shutdown wait is reached.
    fn new() -> Self {
        #[cfg(unix)]
        {
            use tokio::signal::unix::{SignalKind, signal};
            Signals {
                int: signal(SignalKind::interrupt()).expect("SIGINT handler"),
                term: signal(SignalKind::terminate()).expect("SIGTERM handler"),
                quit: signal(SignalKind::quit()).expect("SIGQUIT handler"),
            }
        }
        #[cfg(not(unix))]
        {
            Signals {}
        }
    }

    async fn next(&mut self) -> &'static str {
        #[cfg(unix)]
        {
            tokio::select! {
                _ = self.int.recv() => "interrupt",
                _ = self.term.recv() => "terminated",
                _ = self.quit.recv() => "quit",
            }
        }
        #[cfg(not(unix))]
        {
            let _ = tokio::signal::ctrl_c().await;
            "interrupt"
        }
    }
}

/// Upstream `addresses := s.config.BindAddresses; if nil { [""] }`.
fn bind_hosts(cfg: &ServerConfig) -> Vec<String> {
    cfg.http
        .bind_addresses
        .clone()
        .unwrap_or_else(|| vec![String::new()])
}

/// `net.Listen("tcp", JoinHostPort(host, port))`. Go `favoriteAddrFamily`
/// (net/ipsock_posix.go:142) makes a "tcp" listen on any wildcard laddr — "" /
/// 0.0.0.0 / :: — dual-stack AF_INET6 (V6ONLY=0) wherever IPv4-mapping is
/// supported, falling back to IPv4-only. A concrete address keeps its family.
async fn listen(host: &str, port: u16) -> Result<tokio::net::TcpListener, ServerError> {
    // `text` preserves the original laddr spelling for the bind error (Go's
    // OpError.Addr): "" reports ":port", "0.0.0.0" reports "0.0.0.0:port".
    let text = match crate::bind_host::resolve(host, port)
        .await
        .map_err(ServerError::ListenResolve)?
    {
        Some(address) if !address.ip().is_unspecified() => {
            return tokio::net::TcpListener::bind(address)
                .await
                .map_err(|e| ServerError::Listen(address.to_string(), e));
        }
        Some(address) => address.to_string(),
        None => format!(":{port}"),
    };
    match tokio::net::TcpListener::bind((Ipv6Addr::UNSPECIFIED, port)).await {
        Ok(l) => Ok(l),
        Err(e) if e.kind() == std::io::ErrorKind::AddrInUse => Err(ServerError::Listen(text, e)),
        Err(_) => tokio::net::TcpListener::bind((Ipv4Addr::UNSPECIFIED, port))
            .await
            .map_err(|e| ServerError::Listen(text, e)),
    }
}

/// Go `NodeIP.PrimaryIP()` (config.go:280-285): the raw V4 string when set,
/// else the raw V6 string. Never parses, so a non-IP literal is returned
/// verbatim (SPEC-0074 C30).
fn official_node_ip_primary(official: &lk_config::official::Config) -> String {
    let node = &official.rtc.node_ip;
    if node.v4.is_empty() {
        node.v6.clone()
    } else {
        node.v4.clone()
    }
}

/// `logger.Infow("starting LiveKit server", values...)` (server.go:278-309),
/// reproduced field-by-field through the zap encoder (SPEC-0074 C32): same
/// keys, same order, same presence conditions. `portDebugHandler` (C14) and
/// `portWebTransport` (C15) are emitted after `portPrometheus`, before `region`.
fn log_startup(
    cfg: &ServerConfig,
    official: &lk_config::official::Config,
    net: &rtc_runtime::net::NetConfig,
) {
    let mut fields: Vec<(&str, crate::zap::ZapValue)> = vec![
        ("portHttp", crate::zap::ZapValue::Num(cfg.http.port as i64)),
        ("nodeID", crate::zap::ZapValue::Str(cfg.node_id.clone())),
        (
            "nodeIP",
            // `currentNode.NodeIP()` returns `NodeIP.PrimaryIP()` (node.go:70,
            // config.go:280-285), which hands back the *raw* V4-or-V6 string with
            // no parsing. A generated dotted flag can therefore leave a non-IP
            // literal here and the original still logs it verbatim (SPEC-0074
            // C30); reading the normalized typed value would wrongly print empty.
            crate::zap::ZapValue::Str(official_node_ip_primary(official)),
        ),
        (
            "version",
            crate::zap::ZapValue::Str(env!("CARGO_PKG_VERSION").to_string()),
        ),
    ];
    if let Some(binds) = &cfg.http.bind_addresses {
        fields.push((
            "bindAddresses",
            crate::zap::ZapValue::Array(
                binds
                    .iter()
                    .cloned()
                    .map(crate::zap::ZapValue::Str)
                    .collect(),
            ),
        ));
    }
    if let Some(port) = net.tcp_port
        && port != 0
    {
        fields.push(("rtc.portTCP", crate::zap::ZapValue::Num(port as i64)));
    }
    let rtc = &official.rtc;
    if !rtc.force_tcp && rtc.udp_port.valid() {
        // Go's PortRange struct dump keeps Go field casing inside the JSON.
        fields.push((
            "rtc.portUDP",
            crate::zap::ZapValue::Object(vec![
                (
                    "Start".into(),
                    crate::zap::ZapValue::Num(rtc.udp_port.start),
                ),
                ("End".into(), crate::zap::ZapValue::Num(rtc.udp_port.end)),
            ]),
        ));
    } else {
        fields.push((
            "rtc.portICERange",
            crate::zap::ZapValue::Array(vec![
                crate::zap::ZapValue::Num(rtc.port_range_start as i64),
                crate::zap::ZapValue::Num(rtc.port_range_end as i64),
            ]),
        ));
    }
    if let Some(port) = cfg.prometheus.port
        && port != 0
    {
        fields.push(("portPrometheus", crate::zap::ZapValue::Num(port as i64)));
    }
    // SPEC-0074 C14/C15: after Prometheus, upstream logs the debug and
    // WebTransport listener ports when non-zero (server.go:300-305).
    if official.debug_handler.port != 0 {
        fields.push((
            "portDebugHandler",
            crate::zap::ZapValue::Num(official.debug_handler.port as i64),
        ));
    }
    if official.webtransport.port != 0 {
        fields.push((
            "portWebTransport",
            crate::zap::ZapValue::Num(official.webtransport.port as i64),
        ));
    }
    if !cfg.region.is_empty() {
        fields.push(("region", crate::zap::ZapValue::Str(cfg.region.clone())));
    }
    logging::notify(
        crate::zap::INFO,
        "livekit",
        "pkg/service/server.go:309",
        "starting LiveKit server",
        &fields,
        None,
    );
}

/// `create-join-token`: first configured key, roomJoin, 30 days.
pub fn create_join_token(
    loaded: &impl lk_config::OfficialConfigView,
    room: &str,
    identity: &str,
    recorder: bool,
) -> Result<String, String> {
    // The command chooses a configured pair before AccessToken.ToJWT checks
    // both members. Do not apply KeyStore::first's refresh-token filtering:
    // that would skip an empty selected secret or sign with an empty issuer.
    // Go map iteration is unspecified; configured order chooses one member here.
    let (api_key, api_secret) = loaded
        .official_config()
        .keys
        .0
        .first()
        .ok_or_else(|| "missing API key or secret key".to_string())?;
    if api_key.is_empty() || api_secret.is_empty() {
        return Err("missing API key or secret key".to_string());
    }
    let api_key = api_key.clone();
    let keys = Arc::new(KeyStore::new(loaded.official_config().keys.0.clone()));
    let mut video = VideoGrant {
        room_join: true,
        room: room.to_string(),
        ..Default::default()
    };
    if recorder {
        video.hidden = true;
        video.recorder = true;
        video.can_publish = Some(false);
        video.can_publish_data = Some(false);
    }
    let claims = TokenClaims {
        identity: identity.to_string(),
        video: Some(video),
        ..Default::default()
    };
    TokenMinter::new(keys)
        .mint(
            &api_key,
            &claims,
            Duration::from_secs(30 * 24 * 3600),
            rtc_core::util::unix_s(),
        )
        .map_err(|e| e.to_string())
}

#[cfg(test)]
mod https_startup_tests {
    use super::*;
    use crate::COMPAT_VERSION;
    use lk_config::{EntryAction, Env, ProgramInfo};

    fn config(port: u16, key: &str) -> LoadedConfig {
        let fixtures =
            std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../rtc-tls/tests/fixtures");
        // This test exercises the HTTP/HTTPS bind. Avoid a separate TCP media
        // bind competing with ephemeral outbound connections during setup.
        let rtc_udp = std::net::UdpSocket::bind("127.0.0.1:0")
            .unwrap()
            .local_addr()
            .unwrap()
            .port();
        let body = format!(
            "port: {port}\nbind_addresses: [127.0.0.1]\nrtc:\n  node_ip: 127.0.0.1\n  tcp_port: 0\n  udp_port: {rtc_udp}\nlivecloud:\n  https:\n    port: {port}\n    cert_file: '{}'\n    key_file: '{}'\n",
            fixtures.join("server.pem").display(),
            fixtures.join(key).display()
        );
        let info = ProgramInfo {
            binary_name: "livecloud".into(),
            product_version: "test".into(),
            compat_version: COMPAT_VERSION,
        };
        match lk_config::parse(
            vec![
                "livecloud".into(),
                "--dev".into(),
                "--config-body".into(),
                body.into(),
            ],
            &Env::default(),
            &info,
        )
        .unwrap()
        {
            EntryAction::RunServer(cfg) => *cfg,
            _ => panic!("expected server"),
        }
    }
    fn free_port() -> u16 {
        std::net::TcpListener::bind("127.0.0.1:0")
            .unwrap()
            .local_addr()
            .unwrap()
            .port()
    }

    #[tokio::test]
    async fn duplicate_tls_bind_rolls_back_reserved_http_socket() {
        let port = free_port();
        let result = run(config(port, "server-key.pem")).await;
        assert!(
            matches!(&result, Err(ServerError::Listen(addr,_)) if addr.ends_with(&format!(":{port}"))),
            "{:?}",
            result.err()
        );
        assert!(
            tokio::net::TcpListener::bind((Ipv4Addr::LOCALHOST, port))
                .await
                .is_ok()
        );
    }

    #[tokio::test]
    async fn invalid_certificate_key_fails_before_any_http_listener_is_opened() {
        let port = free_port();
        assert!(matches!(
            run(config(port, "mismatch-key.pem")).await,
            Err(ServerError::Https(_))
        ));
        assert!(
            tokio::net::TcpListener::bind((Ipv4Addr::LOCALHOST, port))
                .await
                .is_ok()
        );
    }
}

#[cfg(test)]
mod bind_address_tests {
    use super::*;
    use crate::COMPAT_VERSION;
    use lk_config::{EntryAction, Env, ProgramInfo, netdetect::NetProbe};
    use std::ffi::OsString;
    use std::net::IpAddr;

    struct NoNetwork;
    impl NetProbe for NoNetwork {
        fn local_addresses(&self) -> Vec<IpAddr> {
            vec![Ipv4Addr::LOCALHOST.into()]
        }
        fn external_ip(&self, _: &[String], _: bool) -> Result<IpAddr, String> {
            panic!("bind fixture must not discover external IP")
        }
        fn sleep(&self, _: Duration) {
            panic!("bind fixture must not retry networking")
        }
    }

    // Production mode (no --dev): the dev-only nil→[127.0.0.1,::1] override must
    // not mask the nil-vs-empty distinction the launched MAIN consumes.
    fn server_config(extra_args: &[&str], body: &str) -> ServerConfig {
        let info = ProgramInfo {
            binary_name: "livecloud".into(),
            product_version: "fixture".into(),
            compat_version: COMPAT_VERSION,
        };
        let mut argv: Vec<OsString> = ["livecloud", "--config-body", body]
            .into_iter()
            .map(OsString::from)
            .collect();
        argv.extend(extra_args.iter().map(OsString::from));
        match lk_config::parse_with_probe(argv, &Env::default(), &info, &NoNetwork).unwrap() {
            EntryAction::RunServer(cfg) => cfg.server,
            _ => panic!("expected RunServer"),
        }
    }

    const BODY: &str = "port: 7880\nkeys:\n  devkey: secret\nrtc:\n  node_ip: 127.0.0.1\n  use_external_ip: false\n";

    async fn reachable(addr: SocketAddr) -> bool {
        tokio::time::timeout(
            Duration::from_millis(800),
            tokio::net::TcpStream::connect(addr),
        )
        .await
        .map(|r| r.is_ok())
        .unwrap_or(false)
    }

    #[test]
    fn bind_hosts_maps_nil_to_wildcard_and_preserves_empty_and_explicit() {
        let mut cfg = server_config(&[], BODY);
        cfg.http.bind_addresses = None;
        assert_eq!(bind_hosts(&cfg), vec![String::new()]);
        cfg.http.bind_addresses = Some(vec![]);
        assert!(bind_hosts(&cfg).is_empty());
        cfg.http.bind_addresses = Some(vec!["127.0.0.1".into(), "0.0.0.0".into()]);
        assert_eq!(
            bind_hosts(&cfg),
            vec!["127.0.0.1".to_string(), "0.0.0.0".to_string()]
        );
    }

    #[test]
    fn cli_bind_overrides_the_config_bind_addresses() {
        let body = "port: 7880\nbind_addresses: [0.0.0.0]\nkeys:\n  devkey: secret\nrtc:\n  node_ip: 127.0.0.1\n  use_external_ip: false\n";
        let cfg = server_config(&["--bind=127.0.0.1"], body);
        assert_eq!(bind_hosts(&cfg), vec!["127.0.0.1".to_string()]);
    }

    #[tokio::test]
    async fn wildcard_laddr_binds_dual_stack_like_the_empty_host() {
        // Go favoriteAddrFamily (net/ipsock_posix.go:142): "" / 0.0.0.0 / :: are
        // all wildcard → dual-stack AF_INET6. Pre-fix Rust bound 0.0.0.0 as
        // IPv4-only, so ::1 was refused while the "" host accepted it.
        let mut shapes = Vec::new();
        for host in ["", "0.0.0.0", "::"] {
            let listener = listen(host, 0).await.unwrap();
            let local = listener.local_addr().unwrap();
            let v4 = reachable(SocketAddr::new(Ipv4Addr::LOCALHOST.into(), local.port())).await;
            let v6 = reachable(SocketAddr::new(Ipv6Addr::LOCALHOST.into(), local.port())).await;
            shapes.push((local.is_ipv6(), v4, v6));
            drop(listener);
        }
        assert_eq!(
            shapes[0], shapes[1],
            "0.0.0.0 must match the empty-host wildcard"
        );
        assert_eq!(
            shapes[0], shapes[2],
            ":: must match the empty-host wildcard"
        );
        if shapes[0].0 {
            assert!(
                shapes[0].1 && shapes[0].2,
                "dual-stack wildcard must accept IPv4 and IPv6: {shapes:?}"
            );
        }
    }

    #[tokio::test]
    async fn concrete_ipv4_laddr_stays_ipv4_only() {
        let listener = listen("127.0.0.1", 0).await.unwrap();
        let local = listener.local_addr().unwrap();
        let port = local.port();
        assert!(
            local.is_ipv4(),
            "concrete 127.0.0.1 must bind IPv4, got {local}"
        );
        assert_eq!(local.ip(), IpAddr::from(Ipv4Addr::LOCALHOST));
        assert!(reachable(SocketAddr::new(Ipv4Addr::LOCALHOST.into(), port)).await);
        assert!(
            !reachable(SocketAddr::new(Ipv6Addr::LOCALHOST.into(), port)).await,
            "a concrete IPv4 listener must not accept IPv6"
        );
    }

    #[tokio::test]
    async fn wildcard_bind_error_preserves_the_original_laddr_spelling() {
        // Bind the holder on port 0 so the OS assigns a port it already owns;
        // a free-port probe would race the parallel wildcard tests.
        let hold = listen("", 0).await.unwrap();
        let port = hold.local_addr().unwrap().port();
        let e = listen("0.0.0.0", port).await.unwrap_err();
        assert!(
            matches!(&e, ServerError::Listen(a, _) if a == &format!("0.0.0.0:{port}")),
            "{e:?}"
        );
        let e2 = listen("", port).await.unwrap_err();
        assert!(
            matches!(&e2, ServerError::Listen(a, _) if a == &format!(":{port}")),
            "{e2:?}"
        );
        let e3 = listen("::", port).await.unwrap_err();
        assert!(
            matches!(&e3, ServerError::Listen(a, _) if a == &format!("[::]:{port}")),
            "{e3:?}"
        );
    }
}

/// The observable consequence of `--dev` on the public HTTP handler: upstream's
/// development branch registers the debug handlers on the mux the API middleware
/// chain serves (`pkg/service/server.go:120-125`), and that mux is
/// `http.DefaultServeMux`, which already carries the `net/http/pprof` subtree.
/// Without the flag the mux is fresh and has neither.
#[cfg(test)]
mod dev_api_mux_tests {
    use super::*;
    use crate::COMPAT_VERSION;
    use axum::body::{Body, to_bytes};
    use axum::http::Request;
    use lk_config::{EntryAction, Env, ProgramInfo, netdetect::NetProbe};
    use rtc_core::ids::{ProjectId, RoomKey};
    use rtc_runtime::CreateRoomOptions;
    use rtc_runtime::net::{LocalInterface, NetConfig};
    use std::ffi::OsString;
    use std::net::IpAddr;
    use tower::ServiceExt;

    const BODY: &str = "port: 7880\nkeys:\n  devkey: secret\nrtc:\n  node_ip: 127.0.0.1\n  use_external_ip: false\n";

    struct NoNetwork;
    impl NetProbe for NoNetwork {
        fn local_addresses(&self) -> Vec<IpAddr> {
            vec![Ipv4Addr::LOCALHOST.into()]
        }
        fn external_ip(&self, _: &[String], _: bool) -> Result<IpAddr, String> {
            panic!("router fixture must not discover external IP")
        }
        fn sleep(&self, _: Duration) {
            panic!("router fixture must not retry networking")
        }
    }

    fn config(flags: &[&str]) -> ServerConfig {
        let info = ProgramInfo {
            binary_name: "livecloud".into(),
            product_version: "fixture".into(),
            compat_version: COMPAT_VERSION,
        };
        let mut argv: Vec<OsString> = ["livecloud", "--config-body", BODY]
            .into_iter()
            .map(OsString::from)
            .collect();
        argv.extend(flags.iter().map(OsString::from));
        match lk_config::parse_with_probe(argv, &Env::default(), &info, &NoNetwork).unwrap() {
            EntryAction::RunServer(cfg) => cfg.server,
            action => panic!("expected RunServer, got {action:?}"),
        }
    }

    struct Graph {
        app: axum::Router,
        manager: RoomManager,
        net: NetRouter,
    }

    impl Graph {
        /// Assemble the public handler exactly as `run()` does, so the
        /// `--dev`→`config.development`→route-table chain is the one under test.
        async fn build(flags: &[&str]) -> Self {
            let cfg = Arc::new(config(flags));
            let local: IpAddr = Ipv4Addr::LOCALHOST.into();
            let net = NetRouter::start(NetConfig {
                udp_enabled: false,
                udp_port: None,
                port_range: (0, 0),
                tcp_port: None,
                interfaces: vec![LocalInterface {
                    bind: local,
                    advertise: local,
                }],
            })
            .await
            .unwrap();
            let state = CompatServices::new(cfg.clone()).assemble(net.clone(), Arc::new(NullSink));
            let manager = state.manager().clone();
            assert_eq!(
                cfg.development,
                flags.contains(&"--dev"),
                "--dev must normalize to the development flag the call site reads"
            );
            let app = http_app(&state, &manager, cfg.development);
            Self { app, manager, net }
        }

        async fn send(
            &self,
            method: &str,
            path: &str,
            headers: &[(&str, &str)],
        ) -> (u16, Vec<(String, String)>, Vec<u8>) {
            let mut builder = Request::builder().method(method).uri(path);
            for (name, value) in headers {
                builder = builder.header(*name, *value);
            }
            let response = self
                .app
                .clone()
                .oneshot(builder.body(Body::empty()).unwrap())
                .await
                .unwrap();
            let status = response.status().as_u16();
            let headers = response
                .headers()
                .iter()
                .map(|(name, value)| {
                    (
                        name.as_str().to_owned(),
                        value.to_str().unwrap_or_default().to_owned(),
                    )
                })
                .collect();
            let body = to_bytes(response.into_body(), 1 << 20)
                .await
                .unwrap()
                .to_vec();
            (status, headers, body)
        }

        async fn get(&self, path: &str) -> (u16, Vec<(String, String)>, Vec<u8>) {
            self.send("GET", path, &[]).await
        }

        async fn shutdown(self) {
            self.manager.close_all().await;
            self.net.shutdown_and_wait().await;
        }
    }

    const DEBUG_PATHS: &[&str] = &[
        "/debug/rooms",
        "/debug/goroutine",
        "/debug/pprof",
        "/debug/pprof/",
        "/debug/pprof/cmdline",
        "/debug/pprof/heap",
    ];

    #[tokio::test]
    async fn dev_flag_puts_debug_handlers_on_the_api_mux() {
        // `/debug/goroutine` drives the process-wide native sampler, so it needs
        // the same serialization the debug module's sampling tests use.
        let _serial = crate::profiling::TEST_SAMPLING.lock().await;
        let graph = Graph::build(&["--dev"]).await;
        graph
            .manager
            .create_room(
                RoomKey::new(ProjectId::local(), "dev-mux"),
                CreateRoomOptions::default(),
            )
            .await
            .unwrap();
        // The handlers merged into the API mux read the live graph, not a stub.
        let (status, _, body) = graph.get("/debug/rooms").await;
        assert_eq!(status, 200);
        let rooms: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(rooms[0]["Name"], "dev-mux", "actual manager behind the mux");
        let (status, headers, body) = graph.get("/debug/pprof/").await;
        assert_eq!(status, 200);
        assert!(
            headers.contains(&("content-type".into(), "text/html; charset=utf-8".into())),
            "{headers:?}"
        );
        assert!(body.starts_with(b"<html><head><title>livekit debug</title>"));
        let (status, headers, _) = graph.get("/debug/pprof/cmdline").await;
        assert_eq!(status, 200);
        assert!(
            headers.contains(&("x-content-type-options".into(), "nosniff".into())),
            "{headers:?}"
        );
        // `/debug/goroutine` is the handler Go registers on the same mux, and it
        // reports native samples rather than goroutine state.
        let (status, _, body) = graph.get("/debug/goroutine").await;
        assert_eq!(status, 200);
        assert!(
            body.starts_with(b"Rust native thread stack samples (not Go goroutine state)\n"),
            "{status}"
        );
        // `/debug/pprof` itself is not a pattern: it is the subtree's exact-path
        // hole, so ServeMux answers it with its trailing-slash 307, not the handler.
        let (status, headers, body) = graph.get("/debug/pprof").await;
        assert_eq!(status, 307);
        assert!(
            headers.contains(&("location".into(), "/debug/pprof/".into())),
            "{headers:?}"
        );
        assert_eq!(
            body,
            b"<a href=\"/debug/pprof/\">Temporary Redirect</a>.\n\n"
        );
        // An unmatched path under the debug prefix stays the API defaultHandler.
        // Go's `http.NotFound` and the debug mux's own 404 render identically, so
        // only the response is pinned, not which of the two produced it.
        let (status, _, body) = graph.get("/debug/nope").await;
        assert_eq!(status, 404);
        assert_eq!(body, b"404 page not found\n");
        // The debug routes sit inside the API middleware chain: Go's auth wrapper
        // covers them, so an unparseable token is refused before dispatch.
        let (status, _, _) = graph
            .send(
                "GET",
                "/debug/rooms",
                &[("authorization", "Bearer not-a-jwt")],
            )
            .await;
        assert_eq!(status, 401);
        graph.shutdown().await;
    }

    /// Measured on the locked oracle: net/http/pprof's init() registers its patterns on
    /// the shared DefaultServeMux with a `GET ` method prefix (or `HEAD ` implied by
    /// net/http's HEAD-shares-GET rule), while livekit registers its own two patterns
    /// with no prefix. A method that matches no pattern would make ServeMux answer 405,
    /// but the API mux also registers the rooted `/` catch-all (server.go:135), so the
    /// lookup finds that instead: `POST /debug/pprof/cmdline` is the plain 404 page on
    /// the dev-mode API port, while the same request on the dedicated debug mux is 200.
    #[tokio::test]
    async fn dev_api_pprof_patterns_reject_other_methods_than_the_livekit_ones_do_not() {
        // `/debug/goroutine` drives the process-wide native sampler on every method.
        let _serial = crate::profiling::TEST_SAMPLING.lock().await;
        let graph = Graph::build(&["--dev"]).await;
        for path in [
            "/debug/pprof",
            "/debug/pprof/",
            "/debug/pprof/cmdline",
            "/debug/pprof/heap",
        ] {
            for method in ["POST", "PUT", "OPTIONS"] {
                let (status, headers, body) = graph.send(method, path, &[]).await;
                assert_eq!(status, 404, "{method} {path} must reach the / catch-all");
                assert_eq!(body, b"404 page not found\n", "{method} {path}");
                assert!(
                    headers.contains(&("x-content-type-options".into(), "nosniff".into())),
                    "{method} {path}: {headers:?}"
                );
            }
            // HEAD is not a separate pattern: it reuses the GET registration, and only
            // the status and headers are pinned here because the wire body suppression
            // is a codec property the oneshot service does not exercise.
            let (status, headers, _) = graph.send("HEAD", path, &[]).await;
            if path == "/debug/pprof" {
                assert_eq!(status, 307, "HEAD {path}");
                assert!(
                    headers.contains(&("location".into(), "/debug/pprof/".into())),
                    "HEAD {path}: {headers:?}"
                );
            } else {
                assert_eq!(status, 200, "HEAD {path}");
            }
        }
        for path in ["/debug/goroutine", "/debug/rooms"] {
            for method in ["HEAD", "POST", "PUT", "OPTIONS"] {
                let (status, _, _) = graph.send(method, path, &[]).await;
                assert_eq!(
                    status, 200,
                    "{method} {path} is registered without a method filter"
                );
            }
        }
        // Outside the registered patterns the mode must be invisible.
        let prod = Graph::build(&[]).await;
        for path in ["/debug/nope", "/other", "/"] {
            for method in ["GET", "POST", "OPTIONS"] {
                let (a, _, _) = graph.send(method, path, &[]).await;
                let (b, _, _) = prod.send(method, path, &[]).await;
                assert_eq!(a, b, "{method} {path} must not depend on the mode");
            }
        }
        prod.shutdown().await;
        graph.shutdown().await;
    }

    #[tokio::test]
    async fn without_the_dev_flag_the_mux_has_no_debug_handlers() {
        let graph = Graph::build(&[]).await;
        for path in DEBUG_PATHS {
            let (status, headers, body) = graph.get(path).await;
            assert_eq!(status, 404, "{path} must not be served in production mode");
            assert_eq!(body, b"404 page not found\n", "{path}");
            assert!(
                headers.contains(&("x-content-type-options".into(), "nosniff".into())),
                "{path}: {headers:?}"
            );
        }
        graph.shutdown().await;
    }

    /// The flag only adds the debug patterns: every other dispatch, the middleware
    /// headers and the fallback bodies have to stay byte-identical.
    #[tokio::test]
    async fn dev_flag_leaves_the_api_surface_untouched() {
        let dev = Graph::build(&["--dev"]).await;
        let prod = Graph::build(&[]).await;
        for (path, headers) in [
            ("/", Vec::<(&str, &str)>::new()),
            ("/nope", Vec::new()),
            (
                "/twirp/livekit.RoomService/ListRooms",
                vec![("content-type", "application/json")],
            ),
            ("/nope", vec![("origin", "https://dashboard.livekit.cloud")]),
        ] {
            let a = dev.send("GET", path, &headers).await;
            let b = prod.send("GET", path, &headers).await;
            assert_eq!((a.0, &a.1, &a.2), (b.0, &b.1, &b.2), "{path} {headers:?}");
        }
        dev.shutdown().await;
        prod.shutdown().await;
    }
}

#[cfg(test)]
mod worker_lifecycle_tests {
    #[tokio::test]
    async fn listener_timeout_after_completed_prefix_never_repolls_and_collects_tail() {
        let first = tokio::spawn(async {});
        let (started, ready) = tokio::sync::oneshot::channel();
        let (dropped, collected) = tokio::sync::oneshot::channel();
        struct Owner(Option<tokio::sync::oneshot::Sender<()>>);
        impl Drop for Owner {
            fn drop(&mut self) {
                let _ = self.0.take().unwrap().send(());
            }
        }
        let tail = tokio::spawn(async move {
            let _owner = Owner(Some(dropped));
            let _ = started.send(());
            std::future::pending::<()>().await;
        });
        ready.await.unwrap();
        let (force, state) = tokio::sync::watch::channel(false);
        super::drain_listener_tasks(
            vec![first, tail],
            &force,
            std::time::Duration::from_millis(10),
        )
        .await;
        assert!(*state.borrow());
        collected
            .await
            .expect("timed-out listener owner must be dropped and joined");
    }

    use super::*;
    fn network_config() -> rtc_runtime::net::NetConfig {
        rtc_runtime::net::NetConfig {
            udp_enabled: true,
            udp_port: Some(0),
            port_range: (0, 0),
            tcp_port: Some(0),
            interfaces: vec![rtc_runtime::net::LocalInterface {
                bind: Ipv4Addr::LOCALHOST.into(),
                advertise: Ipv4Addr::LOCALHOST.into(),
            }],
        }
    }

    async fn exercise_exit(fail: bool) {
        use rtc_runtime::net::NetProto;

        // Let the real router bind both sockets atomically via port 0, then
        // get the actual physical addresses from its public ICE candidates.
        // Dropping a probe before rebinding its port races other listeners.
        let router = NetRouter::start(network_config()).await.unwrap();
        let (sink, _rx) = tokio::sync::mpsc::channel(1);
        let candidates = router
            .open_transport(rtc_core::ids::TransportId::next(), "shutdown", sink)
            .unwrap();
        assert_eq!(candidates.len(), 2);
        let tcp = candidates
            .iter()
            .find(|c| c.proto == NetProto::Tcp)
            .unwrap()
            .base;
        let udp = candidates
            .iter()
            .find(|c| c.proto == NetProto::Udp)
            .unwrap()
            .base;
        assert_ne!(tcp.port(), 0);
        assert_ne!(udp.port(), 0);
        let mut tcp_probe = network_config();
        tcp_probe.udp_enabled = false;
        tcp_probe.udp_port = None;
        tcp_probe.tcp_port = Some(tcp.port());
        // The advertised IPv4 candidate is not the physical dual-stack
        // wildcard bind. A competing router must use the same bind policy.
        match NetRouter::start(tcp_probe).await {
            Err(error) => assert_eq!(error.kind(), std::io::ErrorKind::AddrInUse),
            Ok(other) => {
                other.shutdown_and_wait().await;
                panic!("the actual wildcard TCP listener must own its port before teardown");
            }
        }
        assert_eq!(
            std::net::UdpSocket::bind(udp).unwrap_err().kind(),
            std::io::ErrorKind::AddrInUse
        );

        fn lifecycle(router: NetRouter, fail: bool) -> Result<(), ()> {
            // The exact guard used by run() encloses the real reader-owned
            // descriptors; the injected error represents a post-bind ?.
            let _guard = MediaShutdownGuard(router);
            if fail {
                Err(())?;
            }
            Ok(())
        }
        assert_eq!(lifecycle(router, fail).is_err(), fail);
        let mut same_ports = network_config();
        same_ports.tcp_port = Some(tcp.port());
        same_ports.udp_port = Some(udp.port());
        let rebound = tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                match NetRouter::start(same_ports.clone()).await {
                    Ok(router) => break router,
                    Err(error) if error.kind() == std::io::ErrorKind::AddrInUse => {
                        tokio::task::yield_now().await;
                    }
                    Err(error) => panic!("unexpected actual rebind error: {error}"),
                }
            }
        })
        .await
        .expect("real media sockets released after owner exit");
        rebound.shutdown_and_wait().await;
    }
    #[tokio::test]
    async fn post_bind_bootstrap_failure_releases_reader_owned_media_sockets() {
        exercise_exit(true).await;
    }
    #[tokio::test]
    async fn normal_return_releases_reader_owned_media_sockets() {
        exercise_exit(false).await;
    }
    #[test]
    fn compression_zero_and_negative_use_source_default_not_zero() {
        for raw in [i64::MIN, -1, 0] {
            assert_eq!(worker_compression_threshold(raw), 1024);
        }
        for raw in [1, 1023, 1024, i64::MAX] {
            assert_eq!(
                worker_compression_threshold(raw),
                usize::try_from(raw).unwrap_or(usize::MAX)
            );
        }
    }
}

#[cfg(test)]
mod signal_registration_tests {
    use super::Signals;

    /// `libc::raise` is process-wide and one arrival wakes every `Signal` that tokio's registry has
    /// for that kind (`registry.rs: register_listener` subscribes to the kind's watch channel, and
    /// `broadcast` sends on it), so two tests that raise while another is awaiting would steal each
    /// other's signal. The raises are serialized, and each test builds its `Signals` only after
    /// taking the guard: a subscription starts at the channel's current value, so a signal raised by
    /// an earlier test cannot leak into a later one. The lock is async-aware because
    /// the signal read is awaited while it is held.
    static SIGNAL_RAISE: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

    async fn raise_lock() -> tokio::sync::MutexGuard<'static, ()> {
        SIGNAL_RAISE.lock().await
    }

    /// `signal.Notify` covers SIGINT alongside SIGTERM/SIGQUIT before `Start()`
    /// (`cmd/server/main.go:310`). Raising SIGINT after `new()` but before the
    /// first `next()` poll must be captured, not taken at the default
    /// disposition — registering it lazily inside `next()` via `ctrl_c()` kills
    /// this process instead of returning "interrupt".
    #[tokio::test]
    async fn sigint_is_registered_before_the_first_poll() {
        let _guard = raise_lock().await;
        let mut signals = Signals::new();
        assert_eq!(unsafe { libc::raise(libc::SIGINT) }, 0);
        assert_eq!(signals.next().await, "interrupt");
    }

    /// `RUNTIME:Server.InterruptBeforeRunningRace`: the reason the candidate does not
    /// reproduce the upstream stall is that nothing here gates the shutdown on a
    /// "start completed" flag — the three handlers exist before the first listener accepts
    /// and the signal read is the last statement of `run()`. SIGTERM and SIGQUIT are covered
    /// alongside SIGINT because `signal.Notify` registers all three together
    /// (`cmd/server/main.go:310`); a handler missing for one kind takes the default
    /// disposition and kills this process instead of returning the name.
    #[tokio::test]
    async fn sigterm_and_sigquit_raised_before_the_first_poll_are_also_captured() {
        let _guard = raise_lock().await;
        let mut signals = Signals::new();
        assert_eq!(unsafe { libc::raise(libc::SIGTERM) }, 0);
        assert_eq!(
            tokio::time::timeout(std::time::Duration::from_secs(5), signals.next())
                .await
                .expect("a pre-poll SIGTERM must be remembered, not missed"),
            "terminated"
        );
        assert_eq!(unsafe { libc::raise(libc::SIGQUIT) }, 0);
        assert_eq!(
            tokio::time::timeout(std::time::Duration::from_secs(5), signals.next())
                .await
                .expect("a pre-poll SIGQUIT must be remembered, not missed"),
            "quit"
        );
    }

    /// The window itself: upstream sets `running` 100ms *after* the listeners already answer
    /// (`pkg/service/server.go:343-347`), and `Stop()` returns early when the flag was never
    /// set (`:375-399`), so an interrupt landing inside that window leaves `Start()` blocked
    /// on `<-s.doneChan` forever — measured at delays 0.00/0.03/0.08s after the first HTTP 200.
    /// Here the same arrival order (signal, then real startup work still awaiting, then the
    /// shutdown wait) must still reach the drain sequence instead of blocking. Timing out is a
    /// failure, not a hang: a design that drops the pending signal when entering the wait, or
    /// that refuses to stop before "running", would fail this assertion rather than stall the
    /// suite.
    #[tokio::test]
    async fn interrupt_during_the_startup_window_still_reaches_the_shutdown_wait() {
        let _guard = raise_lock().await;
        let mut signals = Signals::new();
        assert_eq!(unsafe { libc::raise(libc::SIGINT) }, 0);
        // Upstream's post-listener sleep, measured from the first HTTP 200. The task yields
        // here, so the shutdown wait below starts *after* the signal already arrived.
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        assert_eq!(
            tokio::time::timeout(std::time::Duration::from_secs(5), signals.next())
                .await
                .expect("an interrupt inside the startup window must not be lost"),
            "interrupt"
        );
    }
}

#[cfg(test)]
#[path = "cli_join_token_tests.rs"]
mod cli_join_token_tests;

#[cfg(test)]
#[path = "node_selector_projection_tests.rs"]
mod node_selector_projection_tests;

#[cfg(test)]
#[path = "boot_leaf_projection_tests.rs"]
mod boot_leaf_projection_tests;

#[cfg(test)]
#[path = "redis_webtransport_leaf_projection_tests.rs"]
mod redis_webtransport_leaf_projection_tests;
