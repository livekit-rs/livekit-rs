//! Fixed RedisStore room catalog. Runtime participant state belongs to one task.
use super::registry::{RedisRegistry, RegistryError, node_available};
use crate::redis_endpoint::{ManagedLease, RedisEndpoint, SharedConnection};
use crate::{convert, workers::preserved::WireValue};
use lk_protocol::livekit as pb;
use rtc_core::{config::ServerConfig, permission::RoomConfig};
use std::{sync::Arc, time::Duration};

type CachedParticipantRows = std::collections::BTreeMap<Vec<u8>, Vec<u8>>;
pub(super) type CachedParticipantIds = std::collections::BTreeSet<(String, String)>;

#[derive(Debug, thiserror::Error)]
pub enum CatalogError {
    #[error("requested room does not exist")]
    NotFound,
    #[error("participant does not exist")]
    ParticipantNotFound,
    #[error("unknown room configuration in create room request")]
    PresetUnknown,
    /// A nil preset (`name: ~`): applyNamedRoomConfiguration dereferences it.
    #[error("Internal service panic")]
    PresetNil,
    #[error("{0}")]
    Registry(#[from] RegistryError),
    #[error("{0}")]
    Redis(#[from] redis::RedisError),
    #[error("{context}: {}", redis_message(.source))]
    RedisContext {
        context: &'static str,
        source: redis::RedisError,
    },
    #[error("{0}")]
    Invalid(String),
    #[error("room projection changed during reconciliation")]
    ReconcileConflict,
    #[error("room protobuf exceeds explicit {0}-byte limit")]
    Size(usize),
}
#[derive(Clone, Debug)]
pub struct SharedRoom {
    pub room: WireValue<pb::Room>,
    pub internal: Option<WireValue<pb::RoomInternal>>,
}
pub struct RoomCatalog {
    connection: SharedConnection,
    max_bytes: usize,
}
impl RoomCatalog {
    pub async fn connect(
        info: redis::ConnectionInfo,
        timeout: Duration,
        max_bytes: usize,
    ) -> Result<Self, CatalogError> {
        Self::connect_endpoint(RedisEndpoint::simple(info), timeout, max_bytes).await
    }
    pub async fn connect_endpoint(
        endpoint: RedisEndpoint,
        timeout: Duration,
        max_bytes: usize,
    ) -> Result<Self, CatalogError> {
        if max_bytes == 0 {
            return Err(CatalogError::Size(0));
        }
        let config = redis::AsyncConnectionConfig::new()
            .set_connection_timeout(Some(timeout))
            .set_response_timeout(Some(timeout));
        Ok(Self {
            connection: endpoint.connect(&config).await?,
            max_bytes,
        })
    }
    fn decode<T: prost::Message + prost::Name + Default>(
        &self,
        raw: &[u8],
    ) -> Result<WireValue<T>, CatalogError> {
        if raw.len() > self.max_bytes {
            return Err(CatalogError::Size(self.max_bytes));
        }
        WireValue::decode(raw).map_err(|e| CatalogError::Invalid(e.to_string()))
    }
    pub async fn load(&self, name: &str) -> Result<SharedRoom, CatalogError> {
        let mut c = self.connection.begin();
        // Ordinary independent key commands, matching Go RedisStore's non-TX
        // pipeline. A Cluster executor must route each original key separately.
        let room: Option<Vec<u8>> = redis::cmd("HGET")
            .arg("rooms")
            .arg(name)
            .query_async(&mut c)
            .await?;
        let internal: Option<Vec<u8>> = redis::cmd("HGET")
            .arg("room_internal")
            .arg(name)
            .query_async(&mut c)
            .await?;
        Ok(SharedRoom {
            room: self.decode(&room.ok_or(CatalogError::NotFound)?)?,
            internal: internal.map(|v| self.decode(&v)).transpose()?,
        })
    }
    pub async fn public_room_exists(&self, name: &str) -> Result<bool, CatalogError> {
        let mut connection = self.connection.begin();
        let raw: Option<Vec<u8>> = redis::cmd("HGET")
            .arg("rooms")
            .arg(name)
            .query_async(&mut connection)
            .await?;
        if let Some(raw) = raw {
            self.decode::<pb::Room>(&raw)?;
            Ok(true)
        } else {
            Ok(false)
        }
    }
    pub async fn store(&self, room: &SharedRoom) -> Result<(), CatalogError> {
        let raw = room.room.encode();
        let inner = room.internal.as_ref().map(WireValue::encode);
        if raw.len() > self.max_bytes || inner.as_ref().is_some_and(|v| v.len() > self.max_bytes) {
            return Err(CatalogError::Size(self.max_bytes));
        }
        let mut pipeline = redis::pipe();
        pipeline
            .cmd("HSET")
            .arg("rooms")
            .arg(&room.room.message.name)
            .arg(raw);
        match inner {
            Some(inner) => {
                pipeline
                    .cmd("HSET")
                    .arg("room_internal")
                    .arg(&room.room.message.name)
                    .arg(inner);
            }
            None => {
                pipeline
                    .cmd("HDEL")
                    .arg("room_internal")
                    .arg(&room.room.message.name);
            }
        }
        self.connection
            .ordinary_pipeline(&pipeline)
            .await
            .map_err(|source| CatalogError::RedisContext {
                context: "could not create room",
                source,
            })
    }
    pub async fn list(
        &self,
        names: Option<&[String]>,
    ) -> Result<Vec<WireValue<pb::Room>>, CatalogError> {
        let mut command = if names.is_some() {
            redis::cmd("HMGET")
        } else {
            redis::cmd("HVALS")
        };
        command.arg("rooms");
        if let Some(names) = names {
            if names.is_empty() {
                return Ok(vec![]);
            }
            command.arg(names);
        }
        let mut c = self.connection.begin();
        let values: Vec<Option<Vec<u8>>> =
            command
                .query_async(&mut c)
                .await
                .map_err(|source| CatalogError::RedisContext {
                    context: if names.is_some() {
                        "could not get rooms by names"
                    } else {
                        "could not get rooms"
                    },
                    source,
                })?;
        values
            .into_iter()
            .flatten()
            .map(|v| self.decode(&v))
            .collect()
    }
    pub async fn store_participant(
        &self,
        name: &str,
        participant: &WireValue<pb::ParticipantInfo>,
    ) -> Result<(), CatalogError> {
        let raw = participant.encode();
        if raw.len() > self.max_bytes {
            return Err(CatalogError::Size(self.max_bytes));
        }
        let mut c = self.connection.begin();
        let _: usize = redis::cmd("HSET")
            .arg(format!("room_participants:{name}"))
            .arg(&participant.message.identity)
            .arg(raw)
            .query_async(&mut c)
            .await?;
        Ok(())
    }
    pub async fn has_participant(
        &self,
        name: &str,
        identity: &str,
    ) -> Result<bool, crate::service_bus::rpc::RpcError> {
        let mut c = self.connection.begin();
        redis::cmd("HEXISTS")
            .arg(format!("room_participants:{name}"))
            .arg(identity)
            .query_async(&mut c)
            .await
            .map_err(|e| crate::service_bus::rpc::RpcError::new("unknown", e.to_string()))
    }
    pub async fn load_participant(
        &self,
        name: &str,
        identity: &str,
    ) -> Result<WireValue<pb::ParticipantInfo>, CatalogError> {
        let mut c = self.connection.begin();
        let raw: Option<Vec<u8>> = redis::cmd("HGET")
            .arg(format!("room_participants:{name}"))
            .arg(identity)
            .query_async(&mut c)
            .await?;
        self.decode(&raw.ok_or_else(|| CatalogError::ParticipantNotFound)?)
    }
    pub async fn list_participants(
        &self,
        name: &str,
    ) -> Result<Vec<WireValue<pb::ParticipantInfo>>, CatalogError> {
        let mut c = self.connection.begin();
        let values: Vec<Vec<u8>> = redis::cmd("HVALS")
            .arg(format!("room_participants:{name}"))
            .query_async(&mut c)
            .await?;
        values.into_iter().map(|v| self.decode(&v)).collect()
    }
    pub async fn delete_participant(&self, name: &str, identity: &str) -> Result<(), CatalogError> {
        let mut c = self.connection.begin();
        let _: usize = redis::cmd("HDEL")
            .arg(format!("room_participants:{name}"))
            .arg(identity)
            .query_async(&mut c)
            .await?;
        Ok(())
    }
    /// Read-only same-generation ownership fence before exposing owner RPC
    /// topics. The later original per-key CAS still checks the actual bytes,
    /// revision and assignment again; these reads are not a cross-key snapshot.
    pub(super) async fn owns_generation(
        &self,
        name: &str,
        sid: &str,
        owner: &str,
    ) -> Result<bool, CatalogError> {
        let stored = self.load(name).await?;
        if stored.room.message.sid != sid {
            return Ok(false);
        }
        let mut connection = self.connection.begin();
        let assigned: Option<String> = redis::cmd("HGET")
            .arg("room_node_map")
            .arg(name)
            .query_async(&mut connection)
            .await?;
        Ok(assigned.as_deref() == Some(owner))
    }

    /// Reconcile the cached owner projection with source per-key write granularity.
    /// Room SID/bytes and participant inventory each have a single-key CAS; no
    /// cross-slot atomic snapshot or multi-key Redis transaction is assumed.
    pub async fn reconcile_generation(
        &self,
        projection: &rtc_core::events::RoomProjection,
        owner: &str,
        is_current: impl Fn() -> bool,
        prepare_cache: impl FnOnce(&CachedParticipantIds) -> Option<Vec<String>>,
    ) -> Result<bool, CatalogError> {
        let name = &projection.room.name.0;
        let mut c = self.connection.begin();
        let raw: Option<Vec<u8>> = redis::cmd("HGET")
            .arg("rooms")
            .arg(name)
            .query_async(&mut c)
            .await?;
        let observed: CachedParticipantRows = redis::cmd("HGETALL")
            .arg(format!("room_participants:{name}"))
            .query_async(&mut c)
            .await?;
        if observed.values().any(|v| v.len() > self.max_bytes) {
            return Err(CatalogError::Size(self.max_bytes));
        }
        let raw = raw.ok_or(CatalogError::NotFound)?;
        let current: WireValue<pb::Room> = self.decode(&raw)?;
        if current.message.sid != projection.room.sid.0 {
            return Ok(false);
        }
        let stored_version = current
            .message
            .version
            .as_ref()
            .map(|v| (v.unix_micro, v.ticks))
            .unwrap_or_default();
        if stored_version
            > (
                projection.room.version.unix_micro,
                projection.room.version.ticks,
            )
        {
            return Err(CatalogError::ReconcileConflict);
        }
        let mut updated = convert::room_info_to_proto(&projection.room);
        updated.turn_password = current.message.turn_password.clone();
        // The catalog's own reconcile ordering; API/wire rooms omit it.
        updated.version = Some(convert::timed_version_to_proto(&projection.room.version));
        let updated = current.with_message(updated).encode();
        if updated.len() > self.max_bytes {
            return Err(CatalogError::Size(self.max_bytes));
        }
        let mut rows = Vec::with_capacity(projection.participants.len());
        for participant in &projection.participants {
            let row =
                WireValue::from_message(convert::participant_info_to_proto(participant)).encode();
            if row.len() > self.max_bytes {
                return Err(CatalogError::Size(self.max_bytes));
            }
            rows.push((participant.identity.0.clone(), row));
        }
        if !is_current() {
            return Err(CatalogError::ReconcileConflict);
        }
        let observed_ids = observed
            .iter()
            .filter_map(|(identity, raw)| {
                let identity = std::str::from_utf8(identity).ok()?.to_owned();
                let sid = self.decode::<pb::ParticipantInfo>(raw).ok()?.message.sid;
                Some((identity, sid))
            })
            .collect();
        let retired = prepare_cache(&observed_ids).ok_or(CatalogError::ReconcileConflict)?;
        Self::commit_projection(
            &mut c, name, &raw, owner, &updated, &observed, &retired, &rows,
        )
        .await
    }
    /// The full observed hash is a CAS token too. A newer inline participant
    /// insert/update/delete after local revision validation cannot be erased.
    async fn commit_projection(
        c: &mut ManagedLease<'_>,
        name: &str,
        raw: &[u8],
        owner: &str,
        updated: &[u8],
        observed: &std::collections::BTreeMap<Vec<u8>, Vec<u8>>,
        retired: &[String],
        rows: &[(String, Vec<u8>)],
    ) -> Result<bool, CatalogError> {
        // Ownership is a router hash read, not a cross-slot transaction. The
        // upstream cache writes are ordinary per-key operations. Keep the CAS
        // only at each native key's boundary and let reconciliation retry after
        // a concurrent owner/projection update.
        let assigned: Option<String> = redis::cmd("HGET")
            .arg("room_node_map")
            .arg(name)
            .query_async(c)
            .await?;
        if assigned.as_deref() != Some(owner) {
            return Ok(false);
        }
        if !Self::replace_room(c, name, raw, updated).await? {
            return Err(CatalogError::ReconcileConflict);
        }
        // A rehydrated actor does not own participant rows from the former
        // process. Go keeps those rows visible through the cached APIs until
        // the identity is updated by a session or the room is deleted.
        const SCRIPT: &str = r"
            local count = tonumber(ARGV[1])
            if #redis.call('HGETALL', KEYS[1]) ~= count * 2 then return 0 end
            for i = 2, 1 + count * 2, 2 do
                if redis.call('HGET', KEYS[1], ARGV[i]) ~= ARGV[i + 1] then return 0 end
            end
            local removed = 2 + count * 2
            local stored = removed + tonumber(ARGV[removed]) + 1
            for i = removed + 1, stored - 1 do
                redis.call('HDEL', KEYS[1], ARGV[i])
            end
            for i = stored + 1, #ARGV, 2 do
                redis.call('HSET', KEYS[1], ARGV[i], ARGV[i + 1])
            end
            return 1
        ";
        let mut command = redis::cmd("EVAL");
        command
            .arg(SCRIPT)
            .arg(1)
            .arg(format!("room_participants:{name}"))
            .arg(observed.len());
        for (identity, row) in observed {
            command.arg(identity).arg(row);
        }
        command.arg(retired.len()).arg(retired).arg(rows.len());
        for (identity, row) in rows {
            command.arg(identity).arg(row);
        }
        let changed: i64 = command.query_async(c).await?;
        if changed != 1 {
            return Err(CatalogError::ReconcileConflict);
        }
        let assigned: Option<String> = redis::cmd("HGET")
            .arg("room_node_map")
            .arg(name)
            .query_async(c)
            .await?;
        Ok(assigned.as_deref() == Some(owner))
    }
    async fn clear_participant_cache(&self, name: &str) -> Result<(), CatalogError> {
        let mut connection = self.connection.begin();
        let _: usize = redis::cmd("DEL")
            .arg(format!("room_participants:{name}"))
            .query_async(&mut connection)
            .await?;
        Ok(())
    }
    async fn replace_room(
        c: &mut ManagedLease<'_>,
        name: &str,
        raw: &[u8],
        updated: &[u8],
    ) -> Result<bool, CatalogError> {
        const SCRIPT: &str = "if redis.call('HGET',KEYS[1],ARGV[1])~=ARGV[2] then return 0 end; redis.call('HSET',KEYS[1],ARGV[1],ARGV[3]); return 1";
        let changed: i64 = redis::cmd("EVAL")
            .arg(SCRIPT)
            .arg(1)
            .arg("rooms")
            .arg(name)
            .arg(raw)
            .arg(updated)
            .query_async(c)
            .await?;
        Ok(changed == 1)
    }
    /// Retry byte-level contention only while the exact SID and assignment remain
    /// ours. A projection from this same actor is not a generation replacement.
    pub async fn publish_owner_room(
        &self,
        room: &pb::Room,
        owner: &str,
    ) -> Result<(), crate::service_bus::rpc::RpcError> {
        self.publish_owner_fields(room, owner, false).await
    }
    pub async fn publish_owner_metadata(
        &self,
        room: &pb::Room,
        owner: &str,
    ) -> Result<(), crate::service_bus::rpc::RpcError> {
        self.publish_owner_fields(room, owner, true).await
    }
    async fn publish_owner_fields(
        &self,
        room: &pb::Room,
        owner: &str,
        metadata_only: bool,
    ) -> Result<(), crate::service_bus::rpc::RpcError> {
        use crate::service_bus::rpc::RpcError;
        let mut c = self.connection.begin();
        for _attempt in 0..32 {
            let raw: Option<Vec<u8>> = redis::cmd("HGET")
                .arg("rooms")
                .arg(&room.name)
                .query_async(&mut c)
                .await
                .map_err(|e| RpcError::new("unknown", e.to_string()))?;
            let raw = raw.ok_or_else(|| RpcError::from(CatalogError::NotFound))?;
            let current: WireValue<pb::Room> = self.decode(&raw).map_err(RpcError::from)?;
            if current.message.sid != room.sid {
                return Err(RpcError::new(
                    "failed_precondition",
                    "shared room generation does not match local owner",
                ));
            }
            let assigned: Option<String> = redis::cmd("HGET")
                .arg("room_node_map")
                .arg(&room.name)
                .query_async(&mut c)
                .await
                .map_err(|e| RpcError::new("unknown", e.to_string()))?;
            if assigned.as_deref() != Some(owner) {
                return Err(RpcError::new(
                    "aborted",
                    "room assignment changed during catalog publication",
                ));
            }
            // A retried CreateRoom snapshot must not roll back a newer actor
            // projection. Metadata updates change only their actual field;
            // participant counts/recording/unknown wire fields remain current.
            let next = if metadata_only {
                let mut next = current.message.clone();
                let version = |room: &pb::Room| {
                    room.version
                        .as_ref()
                        .map(|v| (v.unix_micro, v.ticks))
                        .unwrap_or_default()
                };
                if version(room) >= version(&current.message) {
                    next.metadata = room.metadata.clone();
                    next.version = room.version.clone();
                }
                next
            } else {
                // Creation already stores its policy before actor startup.
                // Re-publication only fences that exact generation; the live
                // projection is newer than the creation response snapshot.
                current.message.clone()
            };
            let updated = current.with_message(next).encode();
            if updated.len() > self.max_bytes {
                return Err(RpcError::from(CatalogError::Size(self.max_bytes)));
            }
            if Self::replace_room(&mut c, &room.name, &raw, &updated)
                .await
                .map_err(RpcError::from)?
            {
                return Ok(());
            }
            tokio::task::yield_now().await;
        }
        Err(RpcError::new(
            "aborted",
            "room catalog publication contention exceeded retry budget",
        ))
    }
    pub async fn finish_generation(
        &self,
        name: &str,
        sid: &str,
        owner: &str,
    ) -> Result<bool, CatalogError> {
        let mut c = self.connection.begin();
        let raw: Option<Vec<u8>> = redis::cmd("HGET")
            .arg("rooms")
            .arg(name)
            .query_async(&mut c)
            .await?;
        let Some(raw) = raw else {
            return Ok(false);
        };
        let current: WireValue<pb::Room> = self.decode(&raw)?;
        if current.message.sid != sid {
            return Ok(false);
        }
        Self::finish_observed_generation(&mut c, name, &raw, owner).await
    }
    async fn finish_observed_generation(
        c: &mut ManagedLease<'_>,
        name: &str,
        raw: &[u8],
        owner: &str,
    ) -> Result<bool, CatalogError> {
        let assigned: Option<String> = redis::cmd("HGET")
            .arg("room_node_map")
            .arg(name)
            .query_async(c)
            .await?;
        if assigned.as_deref() != Some(owner) {
            return Ok(false);
        }
        const REMOVE_ROOM: &str = "if redis.call('HGET',KEYS[1],ARGV[1])~=ARGV[2] then return 0 end; redis.call('HDEL',KEYS[1],ARGV[1]); return 1";
        let removed: i64 = redis::cmd("EVAL")
            .arg(REMOVE_ROOM)
            .arg(1)
            .arg("rooms")
            .arg(name)
            .arg(raw)
            .query_async(c)
            .await?;
        if removed != 1 {
            return Err(CatalogError::ReconcileConflict);
        }
        // Fixed RedisStore.DeleteRoom is a normal pipeline of original key
        // commands. Recheck a replacement between individual key retirements;
        // do not invent a multi-key transaction RedisCluster cannot execute.
        for key in [
            "room_internal".to_owned(),
            format!("room_participants:{name}"),
            format!("agent_dispatch:{name}"),
            format!("agent_job:{name}"),
        ] {
            let replacement: bool = redis::cmd("HEXISTS")
                .arg("rooms")
                .arg(name)
                .query_async(c)
                .await?;
            let assigned: Option<String> = redis::cmd("HGET")
                .arg("room_node_map")
                .arg(name)
                .query_async(c)
                .await?;
            if replacement || assigned.as_deref() != Some(owner) {
                return Ok(false);
            }
            let _: usize = if key == "room_internal" {
                redis::cmd("HDEL")
                    .arg(&key)
                    .arg(name)
                    .query_async(c)
                    .await?
            } else {
                redis::cmd("DEL").arg(&key).query_async(c).await?
            };
        }
        const REMOVE_MAPPING: &str = "if redis.call('HGET',KEYS[1],ARGV[1])~=ARGV[2] then return 0 end; redis.call('HDEL',KEYS[1],ARGV[1]); return 1";
        let _: usize = redis::cmd("EVAL")
            .arg(REMOVE_MAPPING)
            .arg(1)
            .arg("room_node_map")
            .arg(name)
            .arg(owner)
            .query_async(c)
            .await?;
        Ok(true)
    }
}
/// Adapter-owned strategy selected from native configuration; callers must not
/// silently substitute an unsupported strategy. All candidates supplied here
/// are actual SERVING and fresh NodeMap entries, never invented local defaults.
pub trait NodeSelector: Send + Sync {
    fn select(&self, candidates: &[pb::Node]) -> Result<String, CatalogError>;
    fn at_capacity(&self, node: &pb::Node) -> bool;
}
pub struct RoomAllocator {
    pub catalog: Arc<RoomCatalog>,
    pub registry: Arc<RedisRegistry>,
    pub config: Arc<ServerConfig>,
    pub selector: Arc<dyn NodeSelector>,
}
impl RoomAllocator {
    pub async fn create_for_owner(
        &self,
        request: pb::CreateRoomRequest,
        node: &str,
    ) -> Result<(SharedRoom, pb::CreateRoomRequest, bool), crate::service_bus::rpc::RpcError> {
        use crate::service_bus::rpc::RpcError;
        let lock = self
            .registry
            .lock_room(&request.name)
            .await
            .map_err(|e| RpcError::new("unknown", e.to_string()))?;
        let result = async {
            match self.registry.get_node_for_room(&request.name).await {
                Ok(actual) if actual.message.id != node => {
                    return Err(RpcError::new(
                        "failed_precondition",
                        "room is assigned to another node",
                    ));
                }
                Ok(_) => {}
                Err(RegistryError::NotFound) => self
                    .registry
                    .set_node_for_room(&request.name, node)
                    .await
                    .map_err(|e| RpcError::new("unknown", e.to_string()))?,
                Err(e) => return Err(RpcError::new("unknown", e.to_string())),
            }
            self.create_locked(request).await.map_err(RpcError::from)
        }
        .await;
        if self.registry.unlock_room(&lock).await.is_err() {
            tracing::warn!("room creation unlock failed");
        }
        result
    }
    pub async fn create(
        &self,
        req: pb::CreateRoomRequest,
    ) -> Result<(SharedRoom, pb::CreateRoomRequest), CatalogError> {
        let lock = self.registry.lock_room(&req.name).await?;
        let result = self.create_locked(req).await;
        if self.registry.unlock_room(&lock).await.is_err() {
            tracing::warn!("room creation unlock failed");
        }
        result.map(|(room, effective, _created)| (room, effective))
    }
    async fn create_locked(
        &self,
        mut req: pb::CreateRoomRequest,
    ) -> Result<(SharedRoom, pb::CreateRoomRequest, bool), CatalogError> {
        let (mut shared, created) = match self.catalog.load(&req.name).await {
            Ok(v) => (v, false),
            Err(CatalogError::NotFound) => {
                let now = rtc_core::util::unix_ms();
                let policy = &self.config.room;
                (
                    SharedRoom {
                        room: WireValue::from_message(pb::Room {
                            sid: rtc_core::ids::new_sid("RM_"),
                            name: req.name.clone(),
                            creation_time: now / 1000,
                            creation_time_ms: now,
                            turn_password: random_secret(),
                            empty_timeout: policy.empty_timeout.as_secs() as u32,
                            departure_timeout: policy.departure_timeout.as_secs() as u32,
                            max_participants: policy.max_participants,
                            enabled_codecs: policy
                                .enabled_codecs
                                .iter()
                                .map(convert::codec_spec_to_proto)
                                .collect(),
                            ..Default::default()
                        }),
                        internal: Some(WireValue::from_message(pb::RoomInternal {
                            sync_streams: policy.sync_streams,
                            playout_delay: Some(pb::PlayoutDelay {
                                enabled: policy.playout_delay.is_some(),
                                min: policy.playout_delay.map(|p| p.0).unwrap_or(0),
                                max: policy.playout_delay.map(|p| p.1).unwrap_or(0),
                            }),
                            ..Default::default()
                        })),
                    },
                    true,
                )
            }
            Err(e) => return Err(e),
        };
        let mut replace_agents = !req.agents.is_empty();
        if !req.room_preset.is_empty() {
            let preset = self
                .config
                .room
                .presets
                .get(&req.room_preset)
                .ok_or_else(|| CatalogError::PresetUnknown)?
                .as_ref()
                .ok_or(CatalogError::PresetNil)?;
            let mut projected = crate::twirp::room_service::room_config_from_request(&req);
            projected.inherit(preset);
            // The Go allocator materializes an empty slice for a named
            // preset even when its agent list is omitted.
            replace_agents = true;
            apply_config(&mut req, &projected);
        }
        let room = &mut shared.room.message;
        if req.empty_timeout > 0 {
            room.empty_timeout = req.empty_timeout;
        }
        if req.departure_timeout > 0 {
            room.departure_timeout = req.departure_timeout;
        }
        if req.max_participants > 0 {
            room.max_participants = req.max_participants;
        }
        if !req.metadata.is_empty() {
            room.metadata = req.metadata.clone();
        }
        let internal = shared
            .internal
            .get_or_insert_with(|| WireValue::from_message(pb::RoomInternal::default()));
        if let Some(egress) = &req.egress {
            if egress.participant.is_some() {
                internal.message.participant_egress = egress.participant.clone();
            }
            if egress.tracks.is_some() {
                internal.message.track_egress = egress.tracks.clone();
            }
        }
        if replace_agents {
            internal.message.agent_dispatches = req.agents.clone();
        }
        if req.min_playout_delay > 0 || req.max_playout_delay > 0 {
            internal.message.playout_delay = Some(pb::PlayoutDelay {
                enabled: true,
                min: req.min_playout_delay,
                max: req.max_playout_delay,
            });
        }
        if req.sync_streams {
            internal.message.sync_streams = true;
        }
        if shared.room.message.creation_time == 0 {
            let now = rtc_core::util::unix_ms();
            shared.room.message.creation_time = now / 1000;
            shared.room.message.creation_time_ms = now;
        }
        // The allocator lock serializes same-name creates. A fresh catalog
        // SID must never expose orphaned participant rows from an older SID.
        if created {
            self.catalog.clear_participant_cache(&req.name).await?;
        }
        self.catalog.store(&shared).await?;
        Ok((shared, req, created))
    }
    pub async fn select_owner(&self, name: &str, explicit: &str) -> Result<String, CatalogError> {
        let now = rtc_core::util::unix_ms() / 1000;
        match self.registry.get_node_for_room(name).await {
            Ok(existing) if node_available(&existing.message, now) => {
                if self.selector.at_capacity(&existing.message) {
                    return Err(CatalogError::Invalid(
                        "reached configured limit for node".into(),
                    ));
                }
                return Ok(existing.message.id);
            }
            Ok(_) | Err(RegistryError::NotFound) => {}
            Err(e) => return Err(e.into()),
        };
        let id = if explicit.is_empty() {
            let available: Vec<_> = self
                .registry
                .list_nodes()
                .await?
                .into_iter()
                .map(|v| v.message)
                .filter(|n| node_available(n, now) && n.state == pb::NodeState::Serving as i32)
                .collect();
            self.selector.select(&available)?
        } else {
            explicit.to_owned()
        };
        self.registry.set_node_for_room(name, &id).await?;
        Ok(id)
    }
}
fn apply_config(req: &mut pb::CreateRoomRequest, c: &RoomConfig) {
    if let Some(policy) = &c.automatic_recording {
        req.egress = Some(crate::workers::auto_egress_policy::room_egress_out(policy));
    }
    req.empty_timeout = c.empty_timeout_s.unwrap_or(0);
    req.departure_timeout = c.departure_timeout_s.unwrap_or(0);
    req.max_participants = c.max_participants.unwrap_or(0);
    req.metadata = c.metadata.clone().unwrap_or_default();
    req.min_playout_delay = c.min_playout_delay.unwrap_or(0);
    req.max_playout_delay = c.max_playout_delay.unwrap_or(0);
    req.sync_streams = c.sync_streams.unwrap_or(false);
    if let Some(agents) = &c.agent_dispatches {
        req.agents = agents
            .iter()
            .map(|a| pb::RoomAgentDispatch {
                agent_name: a.agent_name.clone(),
                metadata: a.metadata.clone(),
                restart_policy: a.restart_policy,
                deployment: a.deployment.clone(),
                attributes: a.attributes.clone().into_iter().collect(),
            })
            .collect();
    }
}
pub fn runtime_config(room: &SharedRoom) -> RoomConfig {
    let r = &room.room.message;
    let i = room.internal.as_ref().map(|v| &v.message);
    RoomConfig {
        tags: Default::default(),
        automatic_recording: i.and_then(|v| {
            if v.participant_egress.is_none() && v.track_egress.is_none() {
                None
            } else {
                Some(crate::workers::auto_egress_policy::room_egress_in(
                    &pb::RoomEgress {
                        participant: v.participant_egress.clone(),
                        tracks: v.track_egress.clone(),
                        ..Default::default()
                    },
                ))
            }
        }),
        empty_timeout_s: Some(r.empty_timeout),
        departure_timeout_s: Some(r.departure_timeout),
        max_participants: Some(r.max_participants),
        metadata: Some(r.metadata.clone()),
        min_playout_delay: i
            .and_then(|v| v.playout_delay.as_ref())
            .filter(|v| v.enabled)
            .map(|v| v.min),
        max_playout_delay: i
            .and_then(|v| v.playout_delay.as_ref())
            .filter(|v| v.enabled)
            .map(|v| v.max),
        sync_streams: i.map(|v| v.sync_streams),
        agent_dispatches: i.map(|v| {
            v.agent_dispatches
                .iter()
                .map(crate::auth::claims::room_agent_from_proto)
                .collect()
        }),
    }
}

fn random_secret() -> String {
    let mut bytes = [0u8; 32];
    rtc_core::util::fill_random(&mut bytes);
    crate::turn_auth::encode(&bytes)
}

impl From<CatalogError> for crate::service_bus::rpc::RpcError {
    fn from(error: CatalogError) -> Self {
        let code = match &error {
            CatalogError::NotFound | CatalogError::ParticipantNotFound => "not_found",
            CatalogError::PresetUnknown => "invalid_argument",
            CatalogError::PresetNil => "internal",
            _ => "unknown",
        };
        let message = match &error {
            CatalogError::Redis(value) => redis_message(value),
            _ => error.to_string(),
        };
        Self::new(code, message)
    }
}

fn redis_message(value: &redis::RedisError) -> String {
    // A packed pipeline reports per-command errors. The aggregate only exposes
    // the first code; Go's ordinary pipeline returns the first full error.
    let server_errors = value.clone().into_server_errors();
    let (code, detail) = server_errors
        .as_deref()
        .and_then(|errors| errors.first())
        .map_or((value.code(), value.detail()), |(_, error)| {
            (Some(error.code()), error.details())
        });
    match (code, detail) {
        (Some(code), Some(detail)) if !detail.is_empty() => format!("{code} {detail}"),
        (Some(code), _) => code.to_owned(),
        _ => value.to_string(),
    }
}
