//! The runtime half of the node producer: the `RegisterNode`/`UnregisterNode`
//! pairing around `LivekitServer.Start()` (`pkg/service/server.go:222-231,226-228`)
//! and `RedisRouter`'s two workers (`pkg/routing/redisrouter.go:168-252`), or the
//! single stats tick of `LocalRouter` (`pkg/routing/localrouter.go:128-155`).
//!
//! The record itself, the psrpc `Keepalive.Ping` transport and the readiness rule
//! live in [`lk_compat::routing`]; this module owns the tasks, the log lines and
//! the shutdown ordering, which is where the upstream caller strings live.
//!
//! Failures of the psrpc *bus* are silent here on purpose: psrpc's internal
//! logger is `logr.Discard()` (`references/psrpc/internal/logger/logger.go:21`)
//! and livekit-server never calls `SetLogger` for it, so the reference binary
//! prints nothing when a receive fails or a message cannot be decoded — it just
//! retries (`redisbus.go:141-156`) or drops (`bus/subscription.go:53-62`). The
//! observable consequence of those paths is the record ageing in Redis, which is
//! what the paired differential measures.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use lk_compat::room_control::registry::RedisRegistry;
use lk_compat::routing::{KeepaliveBus, LocalNode, NodeStatsPolicy, seconds_since};
use lk_protocol::livekit as pb;
use tokio::sync::{Mutex, OwnedMutexGuard, watch};
use tokio::task::JoinHandle;

/// How long `join_tasks()` waits for one worker before cutting it short. Every
/// Redis operation the workers issue already carries a 5 s response timeout, so a
/// task still running past this is wedged, not slow.
const JOIN_TIMEOUT: Duration = Duration::from_secs(10);

/// Caller for the diagnostics that exist only because the Rust runtime needs them.
/// A panicking Go goroutine kills the process and a Go goroutine cannot be
/// orphaned by a `return`, so upstream has no line to copy here.
const RUST_ONLY_WORKER_CALLER: &str = "crates/livecloud/src/node.rs (rust-only)";

/// A worker owned by the router, named so that a shutdown which has to cut one
/// short can say which one rather than silently dropping it.
struct Worker {
    name: &'static str,
    handle: JoinHandle<()>,
}

// Dropping a cancelled shutdown future must not detach the worker it was joining.
impl Drop for Worker {
    fn drop(&mut self) {
        self.handle.abort();
    }
}

/// Own the lifecycle gate until a cancelled operation has undone its registration.
/// In particular, a later start must not race an old attempt's asynchronous HDEL.
struct LifecycleAttempt {
    gate: Option<OwnedMutexGuard<()>>,
    node: Arc<LocalNode>,
    registry: Option<Arc<RedisRegistry>>,
    started: Arc<AtomicBool>,
    stop: watch::Sender<bool>,
    tasks: Arc<Mutex<Vec<Worker>>>,
    cleanup_failed: Arc<AtomicBool>,
    armed: bool,
}

impl LifecycleAttempt {
    fn complete(mut self) {
        self.armed = false;
    }
}

impl Drop for LifecycleAttempt {
    fn drop(&mut self) {
        if !self.armed {
            return;
        }
        self.started.store(false, Ordering::SeqCst);
        self.stop.send_replace(true);
        let Some(gate) = self.gate.take() else {
            return;
        };
        let node = self.node.clone();
        let registry = self.registry.clone();
        let tasks = self.tasks.clone();
        let cleanup_failed = self.cleanup_failed.clone();
        // An operation can only have registered/spawned under a Tokio runtime.
        // During runtime teardown its task-owned sockets are being dropped anyway.
        if let Ok(runtime) = tokio::runtime::Handle::try_current() {
            runtime.spawn(async move {
                let _gate = gate;
                let workers = std::mem::take(&mut *tasks.lock().await);
                for mut worker in workers {
                    worker.handle.abort();
                    let _ = (&mut worker.handle).await;
                }
                if let Some(registry) = registry {
                    match tokio::time::timeout(JOIN_TIMEOUT, registry.unregister_node(&node.id()))
                        .await
                    {
                        Ok(Ok(())) => {}
                        Ok(Err(error)) => {
                            cleanup_failed.store(true, Ordering::SeqCst);
                            error_log(
                                RUST_ONLY_WORKER_CALLER,
                                "could not unregister cancelled node operation",
                                &error.to_string(),
                            );
                        }
                        Err(_) => {
                            cleanup_failed.store(true, Ordering::SeqCst);
                            error_log(
                                RUST_ONLY_WORKER_CALLER,
                                "could not unregister cancelled node operation",
                                "Redis cleanup timed out; router restart blocked",
                            );
                        }
                    }
                }
            });
        } else {
            cleanup_failed.store(true, Ordering::SeqCst);
        }
    }
}

/// `RedisRouter.isStarted` / `LocalRouter.isStarted`: `Start()` is idempotent and
/// `Stop()` only tears down a router that had started.
pub struct NodeRouter {
    node: Arc<LocalNode>,
    /// `nil` in single-node routing, where `LocalRouter` registers nothing.
    registry: Option<Arc<RedisRegistry>>,
    /// `nil` in single-node routing, where there is no bus to publish on.
    keepalive: Option<Arc<KeepaliveBus>>,
    policy: NodeStatsPolicy,
    started: Arc<AtomicBool>,
    stop: watch::Sender<bool>,
    tasks: Arc<Mutex<Vec<Worker>>>,
    lifecycle: Arc<Mutex<()>>,
    cleanup_failed: Arc<AtomicBool>,
}

// The process assembly future can be cancelled after successful startup but
// before reaching Stop. The router is the final owner of its workers; dropping
// it must not detach the Arc-owned heartbeat loop and keep refreshing Redis.
impl Drop for NodeRouter {
    fn drop(&mut self) {
        if !self.started.swap(false, Ordering::SeqCst) {
            return; // an in-progress LifecycleAttempt already owns rollback
        }
        self.stop.send_replace(true);
        let node = self.node.clone();
        let registry = self.registry.clone();
        let tasks = self.tasks.clone();
        let lifecycle = self.lifecycle.clone();
        let cleanup_failed = self.cleanup_failed.clone();
        if let Ok(runtime) = tokio::runtime::Handle::try_current() {
            runtime.spawn(async move {
                let _gate = lifecycle.lock_owned().await;
                let workers = std::mem::take(&mut *tasks.lock().await);
                for mut worker in workers {
                    worker.handle.abort();
                    let _ = (&mut worker.handle).await;
                }
                if let Some(registry) = registry {
                    let result =
                        tokio::time::timeout(JOIN_TIMEOUT, registry.unregister_node(&node.id()))
                            .await;
                    if !matches!(result, Ok(Ok(()))) {
                        cleanup_failed.store(true, Ordering::SeqCst);
                        error_log(
                            RUST_ONLY_WORKER_CALLER,
                            "could not unregister dropped node router",
                            "node cleanup failed or timed out",
                        );
                    }
                }
            });
        } else {
            cleanup_failed.store(true, Ordering::SeqCst);
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum NodeStartError {
    #[error("previous node cleanup failed; cannot safely reuse this router identity")]
    CleanupIncomplete,
    /// `errors.Wrap(err, "could not register node")` (`redisrouter.go:75`), the
    /// first act of `LivekitServer.Start()` (`server.go:222`).
    #[error(transparent)]
    Register(#[from] lk_compat::room_control::registry::RegistryError),
    /// `RedisRouter.Start()` returning the `SubscribePing` error it got on
    /// `workerStarted` (`redisrouter.go:176-181`), wrapped by psrpc as
    /// `psrpc.NewError(psrpc.Internal, err)` (`pkg/client/subscription.go:39`).
    #[error("could not subscribe to keepalive pings: {0}")]
    KeepaliveSubscribe(&'static str),
    /// `routing.NewLocalNode` refusing an empty `rtc.node_ip` (`node.go:53-55`),
    /// which `cmd/server/main.go:294-297` returns as the fatal startup error.
    #[error(transparent)]
    Node(#[from] lk_compat::routing::LocalNodeError),
}

impl NodeRouter {
    pub fn new(
        node: Arc<LocalNode>,
        registry: Option<Arc<RedisRegistry>>,
        keepalive: Option<Arc<KeepaliveBus>>,
        policy: NodeStatsPolicy,
    ) -> Self {
        let (stop, _) = watch::channel(false);
        Self {
            node,
            registry,
            keepalive,
            policy,
            started: Arc::new(AtomicBool::new(false)),
            stop,
            tasks: Arc::new(Mutex::new(Vec::new())),
            lifecycle: Arc::new(Mutex::new(())),
            cleanup_failed: Arc::new(AtomicBool::new(false)),
        }
    }

    /// `LivekitServer.Start()`: register first, then start the router, and a
    /// router that cannot start must still undo the registration (upstream's
    /// `defer UnregisterNode` is installed right after the successful HSET).
    /// Nothing announces readiness before this returns `Ok`.
    pub async fn start(&self) -> Result<(), NodeStartError> {
        let gate = self.lifecycle.clone().lock_owned().await;
        if self.started.load(Ordering::SeqCst) {
            return Ok(());
        }
        if self.cleanup_failed.load(Ordering::SeqCst) {
            return Err(NodeStartError::CleanupIncomplete);
        }
        // A failed subscription/previous Stop sets the watch to true. Reusing that
        // signal would make a successful retry immediately lose all its workers.
        self.stop.send_replace(false);
        let attempt = self.attempt(gate);
        if let Some(registry) = &self.registry {
            if let Err(error) = registry.register_node(&self.node.wire_value()).await {
                self.rollback_start().await;
                attempt.complete();
                return Err(NodeStartError::Register(error));
            }
        }

        // Do not spawn the publishing worker until the subscription has actually
        // joined. Cancellation while joining is covered by `attempt`'s HDEL.
        let subscription = match &self.keepalive {
            Some(keepalive) => match keepalive.join().await {
                Ok(subscription) => Some(subscription),
                Err(error) => {
                    self.rollback_start().await;
                    attempt.complete();
                    return Err(NodeStartError::KeepaliveSubscribe(error_code(error)));
                }
            },
            None => None,
        };
        let mut tasks = self.tasks.lock().await;
        self.node.set_state(pb::NodeState::Serving as i32);
        self.started.store(true, Ordering::SeqCst);
        tasks.push(spawn_stats_worker(
            self.node.clone(),
            self.keepalive.clone(),
            self.policy,
            self.stop.subscribe(),
            self.started.clone(),
        ));
        if let (Some(keepalive), Some(subscription)) = (&self.keepalive, subscription) {
            tasks.push(spawn_keepalive_worker(
                self.node.clone(),
                self.registry.clone(),
                keepalive.clone(),
                self.policy,
                subscription,
                self.stop.subscribe(),
            ));
        }
        if self.node.has_stats_provider() {
            let node = self.node.clone();
            let mut stop = self.stop.subscribe();
            tasks.push(Worker {
                name: "cpu-stats-worker",
                handle: tokio::spawn(async move {
                    loop {
                        tokio::select! {
                            biased;
                            _=stop.changed()=>return,
                            _=tokio::time::sleep(Duration::from_secs(1))=>node.poll_system_stats(),
                        }
                    }
                }),
            });
        }
        // No await between spawning workers and disarming cancellation cleanup.
        drop(tasks);
        attempt.complete();
        Ok(())
    }

    async fn rollback_start(&self) {
        self.join_tasks().await;
        if let Some(registry) = &self.registry {
            if let Err(error) = registry.unregister_node(&self.node.id()).await {
                self.cleanup_failed.store(true, Ordering::SeqCst);
                error_log(
                    RUST_ONLY_WORKER_CALLER,
                    "could not unregister failed node start",
                    &error.to_string(),
                );
            }
        }
    }

    fn attempt(&self, gate: OwnedMutexGuard<()>) -> LifecycleAttempt {
        LifecycleAttempt {
            gate: Some(gate),
            node: self.node.clone(),
            registry: self.registry.clone(),
            started: self.started.clone(),
            stop: self.stop.clone(),
            tasks: self.tasks.clone(),
            cleanup_failed: self.cleanup_failed.clone(),
            armed: true,
        }
    }

    /// `RedisRouter.Drain()` (`redisrouter.go:185-190`) / `LocalRouter.Drain()`
    /// (`localrouter.go:137-139`): the state flip always happens, the re-HSET only
    /// where there is a store. Upstream runs this as the first act of `Stop()`,
    /// before waiting for participants, so a peer selector sees SHUTTING_DOWN while
    /// the room is still draining.
    pub async fn drain(&self) {
        let _gate = self.lifecycle.lock().await;
        if !self.started.load(Ordering::SeqCst) {
            return;
        }
        self.node.set_state(pb::NodeState::ShuttingDown as i32);
        let Some(registry) = &self.registry else {
            return;
        };
        if let Err(error) = registry.register_node(&self.node.wire_value()).await {
            // `logger.Errorw(msg, err, "nodeID", r.currentNode.NodeID())`.
            error_log_values(
                "pkg/routing/redisrouter.go:188",
                "failed to mark as draining",
                &[
                    ("error", crate::zap::ZapValue::Str(error.to_string())),
                    ("nodeID", crate::zap::ZapValue::Str(self.node.id())),
                ],
            );
        }
    }

    /// `RedisRouter.Stop()` (`redisrouter.go:193-200`): the guard, the debug line,
    /// an HDEL whose error is discarded (`_ = r.UnregisterNode()`). Rust first
    /// joins the workers so no asynchronous refresh can outlive that HDEL. `LocalRouter.Stop()` is empty, so in
    /// single-node routing only the task teardown below happens.
    pub async fn stop(&self) {
        let gate = self.lifecycle.clone().lock_owned().await;
        if !self.started.swap(false, Ordering::SeqCst) {
            return;
        }
        let attempt = self.attempt(gate);
        if self.registry.is_some() {
            debug_log("pkg/routing/redisrouter.go:196", "stopping RedisRouter");
        }
        // A worker may be inside HSET when Stop is requested. Join every writer
        // before HDEL, so no in-flight refresh can resurrect the removed record.
        self.join_tasks().await;
        if let Some(registry) = &self.registry {
            if let Err(error) = registry.unregister_node(&self.node.id()).await {
                self.cleanup_failed.store(true, Ordering::SeqCst);
                error_log(
                    RUST_ONLY_WORKER_CALLER,
                    "could not unregister stopped node",
                    &error.to_string(),
                );
            }
        }
        attempt.complete();
    }

    /// The `defer`red `UnregisterNode` of `LivekitServer.Start()`
    /// (`server.go:226-229`), which runs once `Start()` returns — after `Stop()`
    /// has already closed `doneChan`. Upstream therefore HDELs the same id twice on
    /// a clean shutdown, and this mirrors that second one with its own error log.
    pub async fn final_unregister(&self) {
        let gate = self.lifecycle.clone().lock_owned().await;
        // An earlier server run's deferred cleanup cannot remove a restarted run.
        if self.started.load(Ordering::SeqCst) {
            return;
        }
        let Some(registry) = &self.registry else {
            return;
        };
        let attempt = self.attempt(gate);
        if let Err(error) = registry.unregister_node(&self.node.id()).await {
            self.cleanup_failed.store(true, Ordering::SeqCst);
            error_log(
                "pkg/service/server.go:227",
                "could not unregister node",
                &error.to_string(),
            );
        }
        attempt.complete();
    }

    async fn join_tasks(&self) {
        // `Stop()` cancels the router context; both worker loops observe it and
        // return, and their subscription (which owns its own connection and the
        // transport permit) drops with the task. Upstream never calls
        // `Subscription.Close()` here, so no UNSUBSCRIBE is sent — the dedicated
        // connection simply goes away, which is what the reference's RESP tap
        // shows as well.
        //
        // The wait is bounded and every non-clean end is logged rather than
        // swallowed: a slow Redis command inside a worker must not block process
        // shutdown forever, and a task panic must never be reported as a full
        // release. The budget is above the 5 s Redis timeout every registry and
        // transport call in these loops already carries, so hitting it means the
        // worker is wedged rather than merely slow; `abort()` then drops its
        // subscription, which owns the dedicated connection, so no socket or
        // permit survives either branch and no later ping can re-HSET the record.
        self.stop.send_replace(true);
        let mut tasks = self.tasks.lock().await;
        // Keep handles in the shared collection across each await. If this future
        // is cancelled, LifecycleAttempt can still abort AND join every writer.
        while let Some(worker) = tasks.last_mut() {
            let name = worker.name;
            match tokio::time::timeout(JOIN_TIMEOUT, &mut worker.handle).await {
                Ok(Ok(())) => {}
                // `JoinError`: the worker panicked (or was cancelled elsewhere).
                // Upstream has no equivalent line — a panicking goroutine takes
                // the process down — so this is a Rust-side diagnostic and is
                // labelled as one rather than given a Go caller it does not have.
                Ok(Err(error)) => error_log_values(
                    RUST_ONLY_WORKER_CALLER,
                    "node worker ended abnormally",
                    &[
                        ("worker", crate::zap::ZapValue::Str(name.to_string())),
                        ("error", crate::zap::ZapValue::Str(error.to_string())),
                    ],
                ),
                Err(_) => {
                    worker.handle.abort();
                    // Wait for cancellation to run destructors before reporting a
                    // released subscription or allowing final_unregister.
                    let _ = (&mut worker.handle).await;
                    error_log_values(
                        RUST_ONLY_WORKER_CALLER,
                        "node worker did not exit before the shutdown budget",
                        &[
                            ("worker", crate::zap::ZapValue::Str(name.to_string())),
                            (
                                "budget_ms",
                                crate::zap::ZapValue::Num(JOIN_TIMEOUT.as_millis() as i64),
                            ),
                        ],
                    );
                }
            }
            tasks.pop();
        }
    }
}

/// `RedisRouter.statsWorker` (`redisrouter.go:202-227`) with a bus, and
/// `LocalRouter.statsWorker` (`localrouter.go:147-155`) without one. The two
/// upstream loops are genuinely different, so this keeps them separate rather
/// than fusing them: with Redis the tick *publishes* and the refresh happens on
/// the receive side; without Redis the tick refreshes the record in place.
fn spawn_stats_worker(
    node: Arc<LocalNode>,
    keepalive: Option<Arc<KeepaliveBus>>,
    policy: NodeStatsPolicy,
    stop: watch::Receiver<bool>,
    started: Arc<AtomicBool>,
) -> Worker {
    match keepalive {
        None => Worker {
            name: "stats-worker(local)",
            handle: spawn_local_stats_worker(node, policy, stop, started),
        },
        Some(keepalive) => Worker {
            name: "stats-worker(redis)",
            handle: spawn_redis_stats_worker(node, keepalive, policy, stop),
        },
    }
}

/// `LocalRouter.statsWorker` (`localrouter.go:147-155`): refresh the in-memory
/// record in place, no bus. Upstream only re-checks `isStarted`, which
/// `LocalRouter.Stop()` never clears, so the loop actually ends when the process
/// does; the stop signal here is the same thing expressed without a Go routine.
fn spawn_local_stats_worker(
    node: Arc<LocalNode>,
    policy: NodeStatsPolicy,
    mut stop: watch::Receiver<bool>,
    started: Arc<AtomicBool>,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        while started.load(Ordering::SeqCst) && !*stop.borrow_and_update() {
            if tokio::time::timeout(policy.update_interval, stop.changed())
                .await
                .is_ok()
            {
                return;
            }
            node.update_stats(now_unix());
        }
    })
}

/// `RedisRouter.statsWorker` (`redisrouter.go:202-227`): every interval, publish
/// this node's own ping and *discard* the result, then measure how long the
/// record has gone unrefreshed.
fn spawn_redis_stats_worker(
    node: Arc<LocalNode>,
    keepalive: Arc<KeepaliveBus>,
    policy: NodeStatsPolicy,
    mut stop: watch::Receiver<bool>,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        // `goroutineDumped`: the delayed diagnostic latches until one tick
        // measures a record that is fresh enough.
        let mut dumped = false;
        while !*stop.borrow_and_update() {
            // `select` on a freshly created `time.After` each pass, not a ticker.
            if tokio::time::timeout(policy.update_interval, stop.changed())
                .await
                .is_ok()
            {
                return;
            }
            // A ping that never leaves simply lets the record age past
            // `AvailableSeconds` (`redisrouter.go:207` ignores the error).
            let _ = keepalive.publish_ping(now_unix()).await;
            let delay = node.seconds_since_stats_update(SystemTime::now());
            if delay > policy.max_delay.as_secs_f64() {
                if !dumped {
                    dumped = true;
                    // `redisrouter.go:216`.
                    error_log_values(
                        "pkg/routing/redisrouter.go:216",
                        "status update delayed, possible deadlock",
                        &[
                            // `logger.Errorw(msg, err, "delay", delaySeconds,
                            //  "goroutines", string(dump))` — a float64 field.
                            ("delay", crate::zap::ZapValue::Float(delay)),
                            // The runtime is Rust: capture this delayed worker's
                            // actual native backtrace, without claiming a dump
                            // of every task or Go goroutine.
                            (
                                "goroutines",
                                crate::zap::ZapValue::Str(format!(
                                    "runtime=rust; scope=current-node-keepalive-worker\n{}",
                                    std::backtrace::Backtrace::force_capture()
                                )),
                            ),
                        ],
                    );
                }
            } else {
                dumped = false;
            }
        }
    })
}

/// `RedisRouter.keepaliveWorker` (`redisrouter.go:229-252`) past its started
/// handshake: refresh and re-HSET the own record for every ping that is not too
/// old, drop anything that does not decode as this schema, and keep the
/// subscription alive across a transport fault the way the bus read worker does.
fn spawn_keepalive_worker(
    node: Arc<LocalNode>,
    registry: Option<Arc<RedisRegistry>>,
    keepalive: Arc<KeepaliveBus>,
    policy: NodeStatsPolicy,
    mut subscription: lk_compat::service_bus::redis_transport::Subscription,
    mut stop: watch::Receiver<bool>,
) -> Worker {
    Worker {
        name: "keepalive-worker",
        handle: tokio::spawn(async move {
            // `redisbus.go:37-39,141-156`: 100 ms first, doubling, capped at 1 s, and
            // reset by any successful read. The log line it accompanies is discarded.
            const MIN_RETRY: Duration = Duration::from_millis(100);
            const MAX_RETRY: Duration = Duration::from_secs(1);
            let mut retry = Duration::ZERO;
            loop {
                if *stop.borrow_and_update() {
                    return;
                }
                let received = tokio::select! {
                    biased;
                    _ = stop.changed() => return,
                    bytes = subscription.recv() => bytes,
                };
                let bytes = match received {
                    Ok(bytes) => {
                        retry = Duration::ZERO;
                        bytes
                    }
                    Err(_) => {
                        // `redisbus.go:141-156`: sleep the current delay *first*
                        // (zero on the very first fault), then double it, capped.
                        if !retry.is_zero() {
                            tokio::select! {
                                biased;
                                _ = stop.changed() => return,
                                _ = tokio::time::sleep(retry) => {}
                            }
                        }
                        retry = if retry.is_zero() {
                            MIN_RETRY
                        } else {
                            (retry * 2).min(MAX_RETRY)
                        };
                        subscription = match retry_keepalive_join(&keepalive, &mut stop).await {
                            Some(next) => next,
                            None => return,
                        };
                        continue;
                    }
                };
                // `bus/subscription.go:53-62`: a deserialize failure or a payload whose
                // type is not this service's is dropped without a word.
                let Ok(ping) = keepalive.decode_ping(&bytes) else {
                    continue;
                };
                let now = SystemTime::now();
                if seconds_since(now, ping.timestamp) > policy.update_interval.as_secs_f64() {
                    info_log(
                        "pkg/routing/redisrouter.go:239",
                        "keep alive too old, skipping",
                        &[("timestamp", ping.timestamp)],
                    );
                    continue;
                }
                if !node.update_stats(now_unix()) {
                    continue;
                }
                let Some(registry) = &registry else {
                    continue;
                };
                if let Err(error) = registry.register_node(&node.wire_value()).await {
                    // `logger.Errorw("could not update node", err)` — the error field is
                    // the only payload upstream attaches.
                    error_log(
                        "pkg/routing/redisrouter.go:249",
                        "could not update node",
                        &error.to_string(),
                    );
                }
            }
        }),
    }
}

/// Re-join after a transport fault, honouring the stop signal while waiting. A
/// failed attempt leaves the previous (already dead) subscription to drop, so no
/// extra connection or permit survives either outcome.
async fn retry_keepalive_join(
    keepalive: &Arc<KeepaliveBus>,
    stop: &mut watch::Receiver<bool>,
) -> Option<lk_compat::service_bus::redis_transport::Subscription> {
    loop {
        tokio::select! {
            biased;
            _ = stop.changed() => return None,
            joined = keepalive.join() => match joined {
                Ok(subscription) => return Some(subscription),
                Err(_) => tokio::select! {
                    biased;
                    _ = stop.changed() => return None,
                    _ = tokio::time::sleep(Duration::from_secs(1)) => {}
                },
            },
        }
    }
}

fn now_unix() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|elapsed| elapsed.as_secs() as i64)
        .unwrap_or_default()
}

/// The stable part of a transport error text, for the one startup message that
/// carries it. `TransportError` has no source payload by design, so its Display
/// is already the whole message.
fn error_code(error: lk_compat::service_bus::redis_transport::TransportError) -> &'static str {
    match error {
        lk_compat::service_bus::redis_transport::TransportError::Configuration => {
            "invalid Redis transport limits or connection configuration"
        }
        lk_compat::service_bus::redis_transport::TransportError::Redis => "Redis operation failed",
        lk_compat::service_bus::redis_transport::TransportError::Timeout => {
            "Redis operation timed out"
        }
        lk_compat::service_bus::redis_transport::TransportError::Disconnected => {
            "Redis subscription disconnected"
        }
        lk_compat::service_bus::redis_transport::TransportError::Overflow => {
            "Redis subscription queue overflow"
        }
        lk_compat::service_bus::redis_transport::TransportError::Oversized => {
            "Redis message exceeds configured envelope limit"
        }
        lk_compat::service_bus::redis_transport::TransportError::Capacity => {
            "Redis transport capacity exhausted"
        }
        lk_compat::service_bus::redis_transport::TransportError::Codec => {
            "psrpc envelope encoding or decoding failed"
        }
    }
}

fn debug_log(caller: &str, msg: &str) {
    crate::logging::notify(crate::zap::DEBUG, "livekit", caller, msg, &[], None);
}

fn info_log(caller: &str, msg: &str, fields: &[(&str, i64)]) {
    crate::logging::notify(
        crate::zap::INFO,
        "livekit",
        caller,
        msg,
        &fields
            .iter()
            .map(|(key, value)| (*key, crate::zap::ZapValue::Num(*value)))
            .collect::<Vec<_>>(),
        None,
    );
}

fn error_log(caller: &str, msg: &str, error: &str) {
    crate::logging::notify(
        crate::zap::ERROR,
        "livekit",
        caller,
        msg,
        &[("error", crate::zap::ZapValue::Str(error.to_string()))],
        None,
    );
}

fn error_log_values(caller: &str, msg: &str, fields: &[(&str, crate::zap::ZapValue)]) {
    crate::logging::notify(crate::zap::ERROR, "livekit", caller, msg, fields, None);
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::UNIX_EPOCH;

    fn stale_node(seconds_ago: i64) -> Arc<LocalNode> {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs() as i64;
        Arc::new(
            LocalNode::serving(
                "ND_local_test".into(),
                "127.0.0.1".into(),
                String::new(),
                1,
                now - seconds_ago,
            )
            .unwrap(),
        )
    }

    fn policy() -> NodeStatsPolicy {
        NodeStatsPolicy {
            update_interval: Duration::from_millis(20),
            max_delay: Duration::from_secs(30),
        }
    }

    fn stamp(node: &LocalNode) -> i64 {
        node.snapshot().stats.unwrap().updated_at
    }

    /// Single-node routing (`LocalRouter`): no store, so nothing is registered and
    /// the in-memory record is refreshed in place (`localrouter.go:147-155`). The
    /// tick is what keeps `/` answering 200 in this mode.
    #[tokio::test]
    async fn local_router_ticks_refresh_the_record_in_place() {
        let node = stale_node(100);
        let before = stamp(&node);
        let router = NodeRouter::new(node.clone(), None, None, policy());
        router.start().await.unwrap();
        for _ in 0..50 {
            if stamp(&node) != before {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        let refreshed = stamp(&node);
        assert_ne!(refreshed, before, "the stats tick must advance the stamp");
        // A fresh stamp is what makes the node both available and ready.
        assert!(
            node.seconds_since_stats_update(SystemTime::now()) < 5.0,
            "within selector.AvailableSeconds"
        );

        // `LocalRouter.Drain()` only flips the state, and the tick preserves it.
        router.drain().await;
        tokio::time::sleep(Duration::from_millis(60)).await;
        let snapshot = node.snapshot();
        assert_eq!(snapshot.state, pb::NodeState::ShuttingDown as i32);
        assert!(snapshot.stats.unwrap().updated_at >= refreshed);

        // `Stop()` ends the worker: the stamp freezes, which is the observable
        // proof that no task outlives shutdown.
        router.stop().await;
        let frozen = stamp(&node);
        tokio::time::sleep(Duration::from_millis(120)).await;
        assert_eq!(stamp(&node), frozen, "no task may run after Stop()");
        // The deferred unregister has no store to act on, and a second `Stop()`
        // hits the `isStarted` guard.
        router.final_unregister().await;
        router.stop().await;
    }

    /// `Start()` is serialized with startup completion, so a repeated call
    /// does not stack a second worker; one `Stop()` still tears everything down.
    #[tokio::test]
    async fn start_is_idempotent_and_stop_joins_the_worker() {
        let node = stale_node(60);
        let router = NodeRouter::new(node.clone(), None, None, policy());
        router.start().await.unwrap();
        router.start().await.unwrap();
        router.stop().await;
        let frozen = stamp(&node);
        tokio::time::sleep(Duration::from_millis(80)).await;
        assert_eq!(stamp(&node), frozen);
    }

    /// `Stop()` without `Start()` is the `if !isStarted.Swap(false) { return }`
    /// guard: no HDEL, no task teardown, no state change.
    #[tokio::test]
    async fn stop_before_start_is_a_no_op() {
        let node = stale_node(60);
        let seeded = stamp(&node);
        let router = NodeRouter::new(node.clone(), None, None, policy());
        router.stop().await;
        let snapshot = node.snapshot();
        assert_eq!(snapshot.state, pb::NodeState::Serving as i32);
        assert_eq!(snapshot.stats.unwrap().updated_at, seeded);
    }
    #[tokio::test]
    async fn restart_after_drain_restores_serving_and_refreshes() {
        let node = stale_node(60);
        let router = NodeRouter::new(node.clone(), None, None, policy());
        router.start().await.unwrap();
        router.drain().await;
        router.stop().await;
        node.update_stats(1);
        router.start().await.unwrap();
        tokio::time::timeout(Duration::from_secs(1), async {
            while stamp(&node) == 1 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("restarted workers must observe a fresh stop signal");
        assert_eq!(node.snapshot().state, pb::NodeState::Serving as i32);
        assert_eq!(router.tasks.lock().await.len(), 1);
        router.stop().await;
        assert!(router.tasks.lock().await.is_empty());
    }

    struct Dropped(Arc<AtomicBool>);
    impl Drop for Dropped {
        fn drop(&mut self) {
            self.0.store(true, Ordering::SeqCst);
        }
    }

    #[tokio::test]
    async fn cancelled_stop_joins_old_workers_before_restart() {
        let router = Arc::new(NodeRouter::new(stale_node(60), None, None, policy()));
        router.start().await.unwrap();
        let dropped = Arc::new(AtomicBool::new(false));
        let witness = dropped.clone();
        let (entered, entered_rx) = tokio::sync::oneshot::channel();
        router.tasks.lock().await.push(Worker {
            name: "blocked-test-writer",
            handle: tokio::spawn(async move {
                let _witness = Dropped(witness);
                entered.send(()).unwrap();
                std::future::pending::<()>().await;
            }),
        });
        entered_rx.await.unwrap();
        let stopping = {
            let router = router.clone();
            tokio::spawn(async move {
                router.stop().await;
            })
        };
        tokio::time::timeout(Duration::from_secs(1), async {
            while router.started.load(Ordering::SeqCst) {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        stopping.abort();
        assert!(stopping.await.unwrap_err().is_cancelled());
        // Cancellation cleanup owns the lifecycle gate until all old writers
        // have actually dropped, so this new generation is safe to register.
        tokio::time::timeout(Duration::from_secs(1), router.start())
            .await
            .unwrap()
            .unwrap();
        assert!(dropped.load(Ordering::SeqCst));
        assert_eq!(router.tasks.lock().await.len(), 1);
        router.stop().await;
    }

    #[tokio::test]
    async fn concurrent_start_waits_for_in_progress_lifecycle() {
        let router = Arc::new(NodeRouter::new(stale_node(60), None, None, policy()));
        let gate = router.lifecycle.lock().await;
        let starting = {
            let router = router.clone();
            tokio::spawn(async move { router.start().await })
        };
        tokio::task::yield_now().await;
        assert!(
            !starting.is_finished(),
            "start cannot announce readiness before the gate"
        );
        drop(gate);
        starting.await.unwrap().unwrap();
        router.start().await.unwrap();
        assert_eq!(router.tasks.lock().await.len(), 1);
        router.stop().await;
    }
    #[tokio::test]
    async fn uncertain_cleanup_prevents_identity_reuse() {
        let router = NodeRouter::new(stale_node(60), None, None, policy());
        router.cleanup_failed.store(true, Ordering::SeqCst);
        assert!(matches!(
            router.start().await,
            Err(NodeStartError::CleanupIncomplete)
        ));
        assert!(!router.started.load(Ordering::SeqCst));
        assert!(router.tasks.lock().await.is_empty());
    }
    #[tokio::test]
    async fn dropping_started_router_retires_owned_worker() {
        let router = NodeRouter::new(stale_node(60), None, None, policy());
        router.start().await.unwrap();
        let dropped = Arc::new(AtomicBool::new(false));
        let witness = dropped.clone();
        let (entered, ready) = tokio::sync::oneshot::channel();
        router.tasks.lock().await.push(Worker {
            name: "drop-owned-writer",
            handle: tokio::spawn(async move {
                let _witness = Dropped(witness);
                entered.send(()).unwrap();
                std::future::pending::<()>().await;
            }),
        });
        ready.await.unwrap();
        drop(router);
        tokio::time::timeout(Duration::from_secs(1), async {
            while !dropped.load(Ordering::SeqCst) {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("final router drop must retire workers without explicit Stop");
    }
}
