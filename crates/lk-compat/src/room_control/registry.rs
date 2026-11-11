//! Fixed routing/redisrouter.go node/room assignment storage and RedisStore room
//! creation lock. A mapping is not an ownership lease and a lock is not a room.
//! No runtime room is created and no SDK signal is rerouted by this foundation.
use crate::redis_endpoint::{RedisEndpoint, SharedConnection};
use crate::workers::preserved::WireValue;
use lk_protocol::livekit as pb;
use std::time::Duration;

pub const NODES_KEY: &str = "nodes";
pub const ROOM_NODE_KEY: &str = "room_node_map";
pub const ROOM_LOCK_PREFIX: &str = "room_lock:";
const UNLOCK_SCRIPT: &str = "if redis.call('get', KEYS[1]) == ARGV[1] then return redis.call('del', KEYS[1]) else return 0 end";
const LIST_NODES_ERROR: &str = "could not list nodes";
const UNREGISTER_NODE_ERROR: &str = "could not unregister node";
#[derive(Debug, thiserror::Error)]
pub enum RegistryError {
    #[error("could not find object")]
    NotFound,
    #[error("could not lock room")]
    LockFailed,
    #[error("could not unlock room, lock token does not match")]
    UnlockFailed,
    #[error("{context}: {source}")]
    Redis {
        context: &'static str,
        source: redis::RedisError,
    },
    #[error("invalid node protobuf: {0}")]
    NodeWire(String),
    #[error("node protobuf exceeds explicit {0}-byte limit")]
    NodeSize(usize),
}

impl RegistryError {
    /// Go's startup sweep wraps HVALS, and returns HDEL and the decoded
    /// protobuf error unwrapped. Other registry callers retain their context.
    pub fn go_startup_prune_text(&self) -> String {
        match self {
            Self::NodeWire(message) => message.clone(),
            Self::Redis {
                context: LIST_NODES_ERROR,
                source,
            } => format!(
                "{LIST_NODES_ERROR}: {}",
                crate::redis_endpoint::go_error_text(source)
            ),
            Self::Redis {
                context: UNREGISTER_NODE_ERROR,
                source,
            } => crate::redis_endpoint::go_error_text(source),
            _ => self.to_string(),
        }
    }
}
#[derive(Clone, Debug)]
pub struct RoomLock {
    pub room_name: String,
    pub token: String,
}
pub struct RedisRegistry {
    connection: SharedConnection,
    max_node_bytes: usize,
}
impl RedisRegistry {
    /// Simple-address constructor. Every other topology has to go through
    /// [`RedisRegistry::connect_endpoint`], because a Sentinel master can change
    /// between two node-list reads and a Cluster deployment serves `nodes` from
    /// whichever node owns the slot.
    pub async fn connect(
        info: redis::ConnectionInfo,
        timeout: Duration,
        max_node_bytes: usize,
    ) -> Result<Self, RegistryError> {
        Self::connect_endpoint(RedisEndpoint::simple(info), timeout, max_node_bytes).await
    }

    /// SPEC-0074 C12: the startup node store rides the same
    /// [`RedisEndpoint`](crate::redis_endpoint::RedisEndpoint) as the client the
    /// server Pinged at startup, so a Sentinel deployment prunes and reads the
    /// *current* master's `nodes` hash and a Cluster deployment reaches the slot
    /// owner instead of one pinned connection.
    pub async fn connect_endpoint(
        endpoint: RedisEndpoint,
        timeout: Duration,
        max_node_bytes: usize,
    ) -> Result<Self, RegistryError> {
        let config = redis::AsyncConnectionConfig::new()
            .set_connection_timeout(Some(timeout))
            .set_response_timeout(Some(timeout));
        let connection =
            endpoint
                .connect(&config)
                .await
                .map_err(|source| RegistryError::Redis {
                    context: "connect registry",
                    source,
                })?;
        Ok(Self {
            connection,
            max_node_bytes,
        })
    }
    async fn query<T: redis::FromRedisValue + Send>(
        &self,
        command: redis::Cmd,
        context: &'static str,
    ) -> Result<T, RegistryError> {
        command
            .query_async(&mut self.connection.begin())
            .await
            .map_err(|source| RegistryError::Redis { context, source })
    }
    fn decode(&self, bytes: &[u8]) -> Result<WireValue<pb::Node>, RegistryError> {
        if bytes.len() > self.max_node_bytes {
            return Err(RegistryError::NodeSize(self.max_node_bytes));
        }
        WireValue::decode(bytes).map_err(|error| RegistryError::NodeWire(error.to_string()))
    }
    /// Source HSET, no TTL. Node statistics and liveness are separate inputs.
    pub async fn register_node(&self, node: &WireValue<pb::Node>) -> Result<(), RegistryError> {
        let bytes = node.encode();
        if bytes.len() > self.max_node_bytes {
            return Err(RegistryError::NodeSize(self.max_node_bytes));
        }
        let mut cmd = redis::cmd("HSET");
        cmd.arg(NODES_KEY).arg(&node.message.id).arg(bytes);
        let _: usize = self.query(cmd, "could not register node").await?;
        Ok(())
    }
    pub async fn unregister_node(&self, id: &str) -> Result<(), RegistryError> {
        let mut cmd = redis::cmd("HDEL");
        cmd.arg(NODES_KEY).arg(id);
        let _: usize = self.query(cmd, UNREGISTER_NODE_ERROR).await?;
        Ok(())
    }
    pub async fn get_node(&self, id: &str) -> Result<WireValue<pb::Node>, RegistryError> {
        let mut cmd = redis::cmd("HGET");
        cmd.arg(NODES_KEY).arg(id);
        let bytes: Option<Vec<u8>> = self.query(cmd, "could not get node").await?;
        self.decode(&bytes.ok_or(RegistryError::NotFound)?)
    }
    /// Source list is unordered. Corruption is an error, never silently skipped.
    pub async fn list_nodes(&self) -> Result<Vec<WireValue<pb::Node>>, RegistryError> {
        let mut cmd = redis::cmd("HVALS");
        cmd.arg(NODES_KEY);
        let values: Vec<Vec<u8>> = self.query(cmd, LIST_NODES_ERROR).await?;
        values
            .into_iter()
            .map(|bytes| self.decode(&bytes))
            .collect()
    }
    pub async fn get_node_for_room(
        &self,
        name: &str,
    ) -> Result<WireValue<pb::Node>, RegistryError> {
        let mut cmd = redis::cmd("HGET");
        cmd.arg(ROOM_NODE_KEY).arg(name);
        let id: Option<String> = self.query(cmd, "could not get node for room").await?;
        self.get_node(&id.ok_or(RegistryError::NotFound)?).await
    }
    /// Exact source operation; no HSETNX/TTL/implicit local fallback is invented.
    pub async fn set_node_for_room(&self, name: &str, id: &str) -> Result<(), RegistryError> {
        let mut cmd = redis::cmd("HSET");
        cmd.arg(ROOM_NODE_KEY).arg(name).arg(id);
        let _: usize = self.query(cmd, "could not set node for room").await?;
        Ok(())
    }
    pub async fn clear_room_state(&self, name: &str) -> Result<(), RegistryError> {
        let mut cmd = redis::cmd("HDEL");
        cmd.arg(ROOM_NODE_KEY).arg(name);
        let _: usize = self.query(cmd, "could not clear room state").await?;
        Ok(())
    }
    /// `RedisRouter.RemoveDeadNodes` (`routing/redisrouter.go:85-98`): read the
    /// node hash once, then HDEL every entry `selector.IsAvailable` rejects.
    /// The availability clock is sampled per node because the Go loop calls
    /// `time.Now()` inside `IsAvailable`, and the first HDEL failure aborts the
    /// sweep — upstream leaves the remaining dead entries behind too.
    pub async fn remove_dead_nodes(&self) -> Result<usize, RegistryError> {
        let mut removed = 0;
        for node in self.list_nodes().await? {
            if !node_available(&node.message, now_unix()) {
                self.unregister_node(&node.message.id).await?;
                removed += 1;
            }
        }
        Ok(removed)
    }
    /// Source StandardRoomAllocator uses a 5s creation lock, not a permanent
    /// assignment lease. It retries SET NX PX at 100ms until elapsed strictly>5s.
    /// Caller must explicitly unlock even if following creation/allocation fails.
    pub async fn lock_room(&self, name: &str) -> Result<RoomLock, RegistryError> {
        let lock = RoomLock {
            room_name: name.to_owned(),
            token: rtc_core::ids::new_sid("LOCK"),
        };
        let key = format!("{ROOM_LOCK_PREFIX}{name}");
        let start = tokio::time::Instant::now();
        loop {
            let mut cmd = redis::cmd("SET");
            cmd.arg(&key).arg(&lock.token).arg("NX").arg("PX").arg(5000);
            let accepted: Option<String> = self.query(cmd, "could not lock room").await?;
            if accepted.is_some() {
                return Ok(lock);
            }
            if start.elapsed() > Duration::from_secs(5) {
                return Err(RegistryError::LockFailed);
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }
    pub async fn unlock_room(&self, lock: &RoomLock) -> Result<(), RegistryError> {
        let mut cmd = redis::cmd("EVAL");
        cmd.arg(UNLOCK_SCRIPT)
            .arg(1)
            .arg(format!("{ROOM_LOCK_PREFIX}{}", lock.room_name))
            .arg(&lock.token);
        let deleted: i64 = self
            .query(cmd, "could not unlock room, lock token does not match")
            .await?;
        if deleted == 1 {
            Ok(())
        } else {
            Err(RegistryError::UnlockFailed)
        }
    }
}
/// Fixed selector.IsAvailable: missing stats is available; future timestamp is
/// available; exactly5s is unavailable. SERVING is a separate selection filter.
pub fn node_available(node: &pb::Node, now_unix: i64) -> bool {
    node.stats
        .as_ref()
        .is_none_or(|stats| now_unix.wrapping_sub(stats.updated_at) < 5)
}
/// `time.Now().Unix()` — the clock `selector.IsAvailable` reads.
pub(crate) fn now_unix() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("system wall clock is before the Unix epoch")
        .as_secs() as i64
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn exact_source_liveness_boundary_and_nil_future() {
        let mut n = pb::Node::default();
        assert!(node_available(&n, 100));
        n.stats = Some(pb::NodeStats {
            updated_at: 96,
            ..Default::default()
        });
        assert!(node_available(&n, 100));
        n.stats.as_mut().unwrap().updated_at = 95;
        assert!(!node_available(&n, 100));
        n.stats.as_mut().unwrap().updated_at = 101;
        assert!(node_available(&n, 100));
    }
    #[test]
    fn shutdown_state_not_confused_with_liveness() {
        let n = pb::Node {
            state: 1,
            stats: None,
            ..Default::default()
        };
        assert!(node_available(&n, 100));
    }
}
#[cfg(test)]
mod redis_tests {
    use super::*;
    #[tokio::test]
    #[ignore = "coordinator-owned dedicated real Redis; no cluster/SDK media acceptance"]
    async fn two_real_registry_connections_share_mapping_and_lock_tokens() {
        let url = std::env::var("CLUSTER_TEST_REDIS_URL").expect("dedicated Redis required");
        let info = redis::IntoConnectionInfo::into_connection_info(url).unwrap();
        let a = RedisRegistry::connect(info.clone(), Duration::from_secs(2), 1024 * 1024)
            .await
            .unwrap();
        let b = RedisRegistry::connect(info, Duration::from_secs(2), 1024 * 1024)
            .await
            .unwrap();
        let name = rtc_core::ids::new_sid("cluster-fixture-");
        let id = rtc_core::ids::new_sid("ND_");
        let node = WireValue::from_message(pb::Node {
            id: id.clone(),
            ..Default::default()
        });
        a.register_node(&node).await.unwrap();
        a.set_node_for_room(&name, &id).await.unwrap();
        assert_eq!(b.get_node_for_room(&name).await.unwrap().message.id, id);
        let lock = a.lock_room(&name).await.unwrap();
        let wrong = RoomLock {
            room_name: name.clone(),
            token: "wrong-token".into(),
        };
        assert!(matches!(
            b.unlock_room(&wrong).await,
            Err(RegistryError::UnlockFailed)
        ));
        a.unlock_room(&lock).await.unwrap();
        let next = b.lock_room(&name).await.unwrap();
        assert_ne!(next.token, lock.token);
        assert!(matches!(
            a.unlock_room(&lock).await,
            Err(RegistryError::UnlockFailed)
        ));
        b.unlock_room(&next).await.unwrap();
        a.clear_room_state(&name).await.unwrap();
        assert!(matches!(
            b.get_node_for_room(&name).await,
            Err(RegistryError::NotFound)
        ));
        a.unregister_node(&id).await.unwrap();
    }
    #[tokio::test]
    #[ignore = "coordinator-owned dedicated real Redis; no cluster/SDK media acceptance"]
    async fn startup_sweep_prunes_the_availability_window_and_ignores_node_state() {
        let url = std::env::var("CLUSTER_TEST_REDIS_URL").expect("dedicated Redis required");
        let info = redis::IntoConnectionInfo::into_connection_info(url).unwrap();
        let registry = RedisRegistry::connect_endpoint(
            RedisEndpoint::simple(info),
            Duration::from_secs(2),
            usize::MAX,
        )
        .await
        .unwrap();
        let now = now_unix();
        let namespace = rtc_core::ids::new_sid("prune-fixture-");
        let seed = |suffix: &str, updated_at: Option<i64>, state: pb::NodeState| {
            WireValue::from_message(pb::Node {
                id: format!("{namespace}{suffix}"),
                state: state as i32,
                stats: updated_at.map(|updated_at| pb::NodeStats {
                    started_at: updated_at,
                    updated_at,
                    ..Default::default()
                }),
                ..Default::default()
            })
        };
        let stale = seed("-stale", Some(now - 3600), pb::NodeState::Serving);
        let boundary = seed("-boundary", Some(now - 5), pb::NodeState::Serving);
        // The fixed-clock unit test covers the one-second boundary. Here the
        // Redis round trips must have time to finish before this row expires.
        let fresh = seed("-fresh", Some(now), pb::NodeState::Serving);
        let future = seed("-future", Some(now + 60), pb::NodeState::Serving);
        let nil_stats = seed("-nil-stats", None, pb::NodeState::Serving);
        let draining_fresh = seed("-draining-fresh", Some(now), pb::NodeState::ShuttingDown);
        let draining_stale = seed(
            "-draining-stale",
            Some(now - 3600),
            pb::NodeState::Suspended,
        );
        let seeded = [
            &stale,
            &boundary,
            &fresh,
            &future,
            &nil_stats,
            &draining_fresh,
            &draining_stale,
        ];
        for node in seeded {
            registry.register_node(node).await.unwrap();
        }
        let removed = registry.remove_dead_nodes().await.unwrap();
        // The instance-wide hash may hold other dead entries, so only the count is
        // a lower bound; the per-ID outcome below is the actual assertion.
        assert!(removed >= 3, "sweep removed {removed}, expected at least 3");
        for keep in [&fresh, &future, &nil_stats, &draining_fresh] {
            let id = &keep.message.id;
            assert!(
                registry.get_node(id).await.is_ok(),
                "{id} must survive the sweep"
            );
        }
        for gone in [&stale, &boundary, &draining_stale] {
            let id = &gone.message.id;
            assert!(
                matches!(registry.get_node(id).await, Err(RegistryError::NotFound)),
                "{id} must be pruned by the sweep"
            );
        }
        for node in seeded {
            let _ = registry.unregister_node(&node.message.id).await;
        }
    }
    #[tokio::test]
    #[ignore = "coordinator-owned dedicated real Redis; no cluster/SDK media acceptance"]
    async fn an_unparsable_node_entry_fails_the_sweep_instead_of_being_skipped() {
        let url = std::env::var("CLUSTER_TEST_REDIS_URL").expect("dedicated Redis required");
        let info = redis::IntoConnectionInfo::into_connection_info(url).unwrap();
        let registry = RedisRegistry::connect_endpoint(
            RedisEndpoint::simple(info.clone()),
            Duration::from_secs(2),
            usize::MAX,
        )
        .await
        .unwrap();
        let id = rtc_core::ids::new_sid("prune-corrupt-");
        let mut raw = redis::Client::open(info)
            .unwrap()
            .get_multiplexed_async_connection()
            .await
            .unwrap();
        let _: usize = redis::cmd("HSET")
            .arg(NODES_KEY)
            .arg(&id)
            .arg("not-a-protobuf")
            .query_async(&mut raw)
            .await
            .unwrap();
        // Upstream `ListNodes` returns the `proto.Unmarshal` error, so
        // `RemoveDeadNodes` aborts rather than pruning around the bad entry. Which
        // entries were already HDELed depends on HVALS order and is not asserted.
        let result = registry.remove_dead_nodes().await;
        assert!(
            matches!(result, Err(RegistryError::NodeWire(_))),
            "expected a wire failure, got {result:?}"
        );
        let _: usize = redis::cmd("HDEL")
            .arg(NODES_KEY)
            .arg(&id)
            .query_async(&mut raw)
            .await
            .unwrap();
    }
}
