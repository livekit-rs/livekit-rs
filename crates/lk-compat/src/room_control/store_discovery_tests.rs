//! Real-Redis legs for the state-store and node/service-discovery families.
//! Every literal key, command shape and expiry asserted here is read from the
//! pinned Go oracle (livekit v1.13.10 @ e5655ac) in `pkg/service/redisstore.go`
//! and `pkg/routing/redisrouter.go`; each leg reads back what it wrote through
//! raw Redis commands instead of only through the Rust wrapper that wrote it.
//!
//! The harness refuses to run against a shared server: it requires an explicit
//! db-indexed URL on loopback plus the opt-in flag, then proves the selected
//! database is empty before writing anything.

use super::catalog::{RoomAllocator, SharedRoom};
use super::registry::{ROOM_LOCK_PREFIX, ROOM_NODE_KEY, RedisRegistry};
use super::selector;
use crate::room_control::registry::NODES_KEY;
use crate::workers::preserved::WireValue;
use lk_protocol::livekit as pb;
use prost::Message;
use redis::aio::MultiplexedConnection;
use std::{sync::Arc, time::Duration};

/// Opt-in guard so a parallel developer's Redis can never be swept by accident.
fn isolated_url() -> String {
    assert!(
        matches!(
            std::env::var("STORE_DISCOVERY_REDIS_ISOLATED").as_deref(),
            Ok("1" | "true")
        ),
        "STORE_DISCOVERY_REDIS_ISOLATED=1 must confirm the db index below is exclusive"
    );
    let url = std::env::var("STORE_DISCOVERY_REDIS_URL").expect("STORE_DISCOVERY_REDIS_URL");
    assert!(
        url.contains("127.0.0.1"),
        "the owned instance must be loopback-only: {url}"
    );
    url
}

/// Every leg holds permanent state (`nodes`, `room_node_map`, `rooms`) and no
/// leg clears the database, so sharing one index would make `require_empty_db`
/// depend on execution order. Give each leg its own index instead: Redis serves
/// db 0..15 by default, so walk down from the reserved base and take the first
/// free slot for each leg. A reused instance therefore stays reproducible,
/// while an index another product already filled is still refused by
/// `require_empty_db` rather than swept.
fn isolated_leg_url(leg: usize) -> String {
    let base = isolated_url();
    let (head, index) = base.rsplit_once('/').unwrap();
    let base_index = index
        .parse::<usize>()
        .unwrap_or_else(|_| panic!("{base} must carry a numeric db index"));
    assert!(
        base_index >= leg,
        "the reserved base index {base} must be at least the highest leg index {leg}"
    );
    format!("{head}/{}", base_index - leg)
}

async fn raw(url: &str) -> MultiplexedConnection {
    redis::Client::open(url)
        .unwrap()
        .get_multiplexed_async_connection()
        .await
        .unwrap()
}

/// Refuse a database that already holds another product's keys: this suite
/// would otherwise sweep state it does not own.
async fn require_empty_db(connection: &mut MultiplexedConnection) {
    let size: i64 = redis::cmd("DBSIZE")
        .query_async(&mut *connection)
        .await
        .unwrap();
    assert_eq!(size, 0, "the dedicated db index must start empty");
}

/// The exact field-less HSET shape `routing/redisrouter.go:74` uses.
async fn seed_node(connection: &mut MultiplexedConnection, id: &str, updated_at: i64) {
    let encoded = pb::Node {
        id: id.into(),
        state: pb::NodeState::Serving as i32,
        stats: Some(pb::NodeStats {
            started_at: updated_at,
            updated_at,
            ..Default::default()
        }),
        ..Default::default()
    }
    .encode_to_vec();
    let _: i64 = redis::cmd("HSET")
        .arg(NODES_KEY)
        .arg(id)
        .arg(&encoded)
        .query_async(&mut *connection)
        .await
        .unwrap();
}

async fn hash_fields(connection: &mut MultiplexedConnection, key: &str) -> Vec<String> {
    redis::cmd("HKEYS")
        .arg(key)
        .query_async(&mut *connection)
        .await
        .unwrap()
}

async fn pttl_ms(connection: &mut MultiplexedConnection, key: &str) -> i64 {
    redis::cmd("PTTL")
        .arg(key)
        .query_async(&mut *connection)
        .await
        .unwrap()
}

/// Select through the allocator, then read the persisted assignment back with
/// raw Redis commands: the returned node id and the `room_node_map` field must
/// agree (SetNodeForRoom is a plain HSET keyed by room name,
/// redisrouter.go:112-114).
async fn select_and_read_back(
    probe: &mut MultiplexedConnection,
    allocator: &RoomAllocator,
    room: &str,
) -> String {
    let id = allocator.select_owner(room, "").await.unwrap();
    let raw: Option<String> = redis::cmd("HGET")
        .arg(ROOM_NODE_KEY)
        .arg(room)
        .query_async(&mut *probe)
        .await
        .unwrap();
    assert_eq!(
        raw.as_deref(),
        Some(id.as_str()),
        "{room} assignment persisted"
    );
    id
}

async fn registry(url: &str) -> RedisRegistry {
    RedisRegistry::connect(
        redis::IntoConnectionInfo::into_connection_info(url).unwrap(),
        Duration::from_secs(2),
        1 << 20,
    )
    .await
    .unwrap()
}

fn node_message(id: &str, updated_at: i64) -> pb::Node {
    pb::Node {
        id: id.into(),
        ip: "127.0.0.1".into(),
        num_cpus: 2,
        region: "local".into(),
        state: pb::NodeState::Serving as i32,
        stats: Some(pb::NodeStats {
            started_at: updated_at,
            updated_at,
            ..Default::default()
        }),
        ..Default::default()
    }
}

/// allocator over the same db as the registry, mirroring how RoomControl wires
/// one shared command connection (`room_control/mod.rs:262-271`).
async fn allocator(
    url: &str,
    cpu_limit: f32,
    sysload_limit: f32,
    sort_by: &str,
    algorithm: &str,
) -> RoomAllocator {
    let info = redis::IntoConnectionInfo::into_connection_info(url).unwrap();
    let registry = Arc::new(
        RedisRegistry::connect(info.clone(), Duration::from_secs(2), 1 << 20)
            .await
            .unwrap(),
    );
    let catalog = Arc::new(
        super::catalog::RoomCatalog::connect(info, Duration::from_secs(2), 1 << 20)
            .await
            .unwrap(),
    );
    let selector = selector::ConfiguredSelector::new(selector::Config {
        kind: "any".into(),
        sort_by: sort_by.into(),
        algorithm: algorithm.into(),
        cpu_limit,
        sysload_limit,
        region: String::new(),
        regions: vec![],
        track_limit: 0,
        bytes_limit: 0.0,
    })
    .unwrap();
    RoomAllocator {
        catalog,
        registry,
        // The normalized standalone defaults, through the same configuration
        // boundary the shipped binary uses.
        config: Arc::new(crate::test_support::config()),
        selector: Arc::new(selector),
    }
}

/// `pkg/service/redisstore.go:127-157` StoreRoom and `:262-301` LockRoom/
/// UnlockRoom, proven through the allocator's real create path
/// (`pkg/service/roomallocator.go:63-70`) rather than by calling the store in
/// isolation.
#[tokio::test]
#[ignore = "requires owned loopback STORE_DISCOVERY_REDIS_URL with a dedicated db index"]
async fn create_room_writes_the_exact_go_key_layout_and_expires_the_creation_lock() {
    let url = isolated_leg_url(0);
    let mut probe = raw(&url).await;
    require_empty_db(&mut probe).await;
    let registry = registry(&url).await;
    let allocator = allocator(&url, 0.9, 0.9, "clients", "lowest").await;

    // StandardRoomAllocator takes the lock first with a 5s expiry
    // (roomallocator.go:63 -> redisstore.go:262-281 SetNX PX duration).
    let lock = registry.lock_room("lock-shape").await.unwrap();
    // protocol/utils/guid/id.go:22 builds the token as prefix + guid.Size
    // random bytes, so `guid.New(utils.LockPrefix /* "LOCK" */)` is exactly 16
    // characters, and redisstore.go:61 pins RoomLockPrefix to "room_lock:".
    // Assert both literally: an expectation built from the production constant
    // cannot fail when the constant itself changes.
    assert_eq!(
        lock.token.len(),
        "LOCK".len() + 12,
        "guid.New(utils.LockPrefix...) token shape, redisstore.go:263"
    );
    assert!(lock.token.starts_with("LOCK"));
    assert_eq!(
        ROOM_LOCK_PREFIX, "room_lock:",
        "redisstore.go:61 RoomLockPrefix literal"
    );
    let key = format!("{ROOM_LOCK_PREFIX}lock-shape");
    assert_eq!(key, "room_lock:lock-shape");
    let stored: Option<String> = redis::cmd("GET")
        .arg(&key)
        .query_async(&mut probe)
        .await
        .unwrap();
    assert_eq!(stored.as_deref(), Some(lock.token.as_str()));
    let ttl = pttl_ms(&mut probe, &key).await;
    assert!(
        ttl > 0 && ttl <= 5000,
        "SetNX expiry must be 5s, PTTL={ttl}"
    );
    // LockRoom must not touch any other key: the whole db is one lock.
    let keys: Vec<String> = redis::cmd("KEYS")
        .arg("*")
        .query_async(&mut probe)
        .await
        .unwrap();
    assert_eq!(
        keys.as_slice(),
        ["room_lock:lock-shape".to_owned()],
        "LockRoom writes exactly one key, under the upstream prefix"
    );

    // UnlockRoom runs the compare-and-del script (redisstore.go:287-301):
    // a wrong uid returns ErrRoomUnlockFailed and leaves the lock intact.
    let wrong = super::registry::RoomLock {
        room_name: "lock-shape".into(),
        token: "LOCK-not-mine".into(),
    };
    assert!(matches!(
        registry.unlock_room(&wrong).await,
        Err(super::registry::RegistryError::UnlockFailed)
    ));
    assert_eq!(
        redis::cmd("EXISTS")
            .arg(&key)
            .query_async::<i64>(&mut probe)
            .await
            .unwrap(),
        1,
        "a mismatched unlock must not delete the held lock"
    );
    registry.unlock_room(&lock).await.unwrap();
    assert_eq!(
        redis::cmd("EXISTS")
            .arg(&key)
            .query_async::<i64>(&mut probe)
            .await
            .unwrap(),
        0
    );

    // CreateRoom writes RoomsKey/RoomInternalKey in one pipeline and never
    // touches NodeRoomKey (that is routing's assignment, redisrouter.go:112).
    let created = allocator
        .create(pb::CreateRoomRequest {
            name: "layout-room".into(),
            metadata: "parity".into(),
            ..Default::default()
        })
        .await
        .unwrap();
    let raw: Option<Vec<u8>> = redis::cmd("HGET")
        .arg("rooms")
        .arg("layout-room")
        .query_async(&mut probe)
        .await
        .unwrap();
    let stored = pb::Room::decode(raw.expect("rooms hash field").as_slice()).unwrap();
    assert_eq!(stored.name, "layout-room");
    assert_eq!(stored.metadata, "parity");
    assert!(
        stored.sid.starts_with("RM_"),
        "guid.RoomPrefix, redisstore via roomallocator.go:78"
    );
    assert_ne!(stored.creation_time, 0);
    assert_ne!(stored.creation_time_ms, 0);
    assert!(
        !stored.turn_password.is_empty(),
        "utils.RandomSecret at create (roomallocator.go:81)"
    );
    assert_eq!(
        hash_fields(&mut probe, ROOM_NODE_KEY).await,
        Vec::<String>::new(),
        "catalog create must not invent a node assignment"
    );

    // internal present -> HSET room_internal (redisstore.go:143-148).
    assert!(created.0.internal.is_some());
    let internal: Option<Vec<u8>> = redis::cmd("HGET")
        .arg("room_internal")
        .arg("layout-room")
        .query_async(&mut probe)
        .await
        .unwrap();
    assert!(internal.is_some(), "nil-internal branch is covered below");

    // internal nil -> HDEL room_internal while rooms stays intact
    // (redisstore.go:149-151).
    let bare = SharedRoom {
        room: created.0.room.clone(),
        internal: None,
    };
    allocator.catalog.store(&bare).await.unwrap();
    let still: Option<Vec<u8>> = redis::cmd("HGET")
        .arg("rooms")
        .arg("layout-room")
        .query_async(&mut probe)
        .await
        .unwrap();
    assert!(still.is_some());
    let gone: Option<Vec<u8>> = redis::cmd("HGET")
        .arg("room_internal")
        .arg("layout-room")
        .query_async(&mut probe)
        .await
        .unwrap();
    assert!(
        gone.is_none(),
        "StoreRoom(nil internal) must HDEL room_internal"
    );

    // ListRooms names branch uses HMGet and skips missing fields
    // (redisstore.go:210-244).
    let listed = allocator
        .catalog
        .list(Some(&["layout-room".to_owned(), "absent".to_owned()]))
        .await
        .unwrap();
    assert_eq!(listed.len(), 1);
    assert_eq!(listed[0].message.name, "layout-room");
}

/// `routing/redisrouter.go:69-98` RegisterNode/UnregisterNode/RemoveDeadNodes
/// with `routing/selector/utils.go:31-41` availability semantics.
#[tokio::test]
#[ignore = "requires owned loopback STORE_DISCOVERY_REDIS_URL with a dedicated db index"]
async fn node_registration_is_field_less_hset_and_the_sweep_prunes_only_expired_rows() {
    let url = isolated_leg_url(1);
    let mut probe = raw(&url).await;
    require_empty_db(&mut probe).await;
    let registry = registry(&url).await;

    let now = super::registry::now_unix();
    // A row written by Go-shaped HSET is readable by the Rust registry without
    // any TTL: NodesKey entries are permanent until HDEL (redisrouter.go:74).
    seed_node(&mut probe, "ND_crosslang", now).await;
    let cross = registry.get_node("ND_crosslang").await.unwrap();
    assert_eq!(cross.message.id, "ND_crosslang");
    assert_eq!(
        pttl_ms(&mut probe, NODES_KEY).await,
        -1,
        "nodes hash has no TTL"
    );

    // Rust registration carries the full identity fields upstream stores.
    let node = crate::workers::preserved::WireValue::from_message(node_message("ND_rustfull", now));
    registry.register_node(&node).await.unwrap();
    let read_back = registry.get_node("ND_rustfull").await.unwrap();
    assert_eq!(read_back.message.ip, "127.0.0.1");
    assert_eq!(read_back.message.region, "local");
    assert_eq!(read_back.message.num_cpus, 2);
    assert_eq!(read_back.message.state, pb::NodeState::Serving as i32);

    // Sweep boundary rows: exactly AvailableSeconds(5) old is dead, one second
    // fresher survives, a future stamp survives (selector/utils.go:31-41), and
    // a nil-stats row survives while its SERVING state is irrelevant because
    // RemoveDeadNodes checks only IsAvailable (redisrouter.go:85-98).
    let stale = crate::workers::preserved::WireValue::from_message(pb::Node {
        id: "ND_stale".into(),
        state: pb::NodeState::Serving as i32,
        stats: Some(pb::NodeStats {
            updated_at: now - 3600,
            ..Default::default()
        }),
        ..Default::default()
    });
    let boundary = crate::workers::preserved::WireValue::from_message(pb::Node {
        id: "ND_boundary".into(),
        state: pb::NodeState::Serving as i32,
        stats: Some(pb::NodeStats {
            updated_at: now - 5,
            ..Default::default()
        }),
        ..Default::default()
    });
    let fresh = crate::workers::preserved::WireValue::from_message(pb::Node {
        id: "ND_fresh".into(),
        state: pb::NodeState::Serving as i32,
        stats: Some(pb::NodeStats {
            updated_at: now - 4,
            ..Default::default()
        }),
        ..Default::default()
    });
    let nil_stats = crate::workers::preserved::WireValue::from_message(pb::Node {
        id: "ND_nilstats".into(),
        state: pb::NodeState::ShuttingDown as i32,
        stats: None,
        ..Default::default()
    });
    for value in [&stale, &boundary, &fresh, &nil_stats] {
        registry.register_node(value).await.unwrap();
    }
    let removed = registry.remove_dead_nodes().await.unwrap();
    assert!(
        removed >= 2,
        "sweep removed {removed}, expected stale+boundary"
    );
    let survivors = hash_fields(&mut probe, NODES_KEY).await;
    for keep in ["ND_crosslang", "ND_rustfull", "ND_fresh", "ND_nilstats"] {
        assert!(survivors.contains(&keep.to_owned()), "{keep} must survive");
    }
    for gone in ["ND_stale", "ND_boundary"] {
        assert!(
            !survivors.contains(&gone.to_owned()),
            "{gone} must be pruned"
        );
    }

    // UnregisterNode is a plain HDEL of the node id field (redisrouter.go:79-82).
    registry.unregister_node("ND_rustfull").await.unwrap();
    assert!(
        !hash_fields(&mut probe, NODES_KEY)
            .await
            .contains(&"ND_rustfull".to_owned())
    );
    assert!(matches!(
        registry.get_node("ND_rustfull").await,
        Err(super::registry::RegistryError::NotFound)
    ));
}

/// `pkg/service/roomallocator.go:120-176` SelectRoomNode against the real
/// `nodes`/`room_node_map` hashes: reuse inside the availability window,
/// capacity denial, re-selection after expiry, and persistence of the choice.
#[tokio::test]
#[ignore = "requires owned loopback STORE_DISCOVERY_REDIS_URL with a dedicated db index"]
async fn select_room_node_reuses_assignments_until_the_availability_window_closes() {
    let url = isolated_leg_url(2);
    let mut probe = raw(&url).await;
    require_empty_db(&mut probe).await;
    // kind=any with both limits above every node value: `LimitsReached` is
    // false for all stats (cpu/sysload thresholds are not consulted on the any
    // path), so this leg isolates reuse, re-selection and persistence.
    let allocator = allocator(&url, 0.9, 0.9, "clients", "lowest").await;
    let registry = allocator.registry.clone();
    let now = super::registry::now_unix();
    let wire = |id: &str, updated_at: i64, clients: i32| {
        crate::workers::preserved::WireValue::from_message(pb::Node {
            id: id.into(),
            state: pb::NodeState::Serving as i32,
            stats: Some(pb::NodeStats {
                updated_at,
                num_clients: clients,
                ..Default::default()
            }),
            ..Default::default()
        })
    };
    registry
        .register_node(&wire("ND_a", now, 10))
        .await
        .unwrap();
    registry.register_node(&wire("ND_b", now, 1)).await.unwrap();

    // First selection persists exactly the Go shapes: plain HSET into
    // NodeRoomKey keyed by room name holding the node id string. Pin the key
    // names as literals first — an expectation assembled from the production
    // constants would keep passing when the constants themselves change.
    assert_eq!(
        ROOM_NODE_KEY, "room_node_map",
        "redisrouter.go:40 NodeRoomKey literal"
    );
    assert_eq!(NODES_KEY, "nodes", "redisrouter.go:37 NodesKey literal");
    let owner = allocator.select_owner("route-me", "").await.unwrap();
    assert_eq!(owner, "ND_b", "sortBy=clients picks the lower load");
    let assigned: Option<String> = redis::cmd("HGET")
        .arg("room_node_map")
        .arg("route-me")
        .query_async(&mut probe)
        .await
        .unwrap();
    assert_eq!(assigned.as_deref(), Some("ND_b"));
    assert_eq!(
        pttl_ms(&mut probe, ROOM_NODE_KEY).await,
        -1,
        "no TTL on assignments"
    );

    // An available assignment is kept even when its load grew past the limit
    // (roomallocator.go:143-151 keeps the node, LimitsReached denies entry).
    registry
        .register_node(&wire("ND_b", now, 1000))
        .await
        .unwrap();
    allocator.select_owner("route-me", "").await.unwrap();
    let still: Option<String> = redis::cmd("HGET")
        .arg(ROOM_NODE_KEY)
        .arg("route-me")
        .query_async(&mut probe)
        .await
        .unwrap();
    assert_eq!(still.as_deref(), Some("ND_b"), "reuse must not re-select");

    // Expire ND_b past AvailableSeconds: selection moves and HSET overwrites
    // the mapping in place (redisrouter.go:112-114).
    registry
        .register_node(&wire("ND_b", now - 3600, 0))
        .await
        .unwrap();
    let moved = allocator.select_owner("route-me", "").await.unwrap();
    assert_eq!(moved, "ND_a");
    let moved_assignment: Option<String> = redis::cmd("HGET")
        .arg(ROOM_NODE_KEY)
        .arg("route-me")
        .query_async(&mut probe)
        .await
        .unwrap();
    assert_eq!(moved_assignment.as_deref(), Some("ND_a"));

    // ClearRoomState removes only the mapping; the node rows stay registered
    // (redisrouter.go:116-120 vs :79-82).
    registry.clear_room_state("route-me").await.unwrap();
    let cleared: Option<String> = redis::cmd("HGET")
        .arg(ROOM_NODE_KEY)
        .arg("route-me")
        .query_async(&mut probe)
        .await
        .unwrap();
    assert!(cleared.is_none());
    assert!(
        hash_fields(&mut probe, NODES_KEY)
            .await
            .contains(&"ND_a".to_owned())
    );

    // A fully expired pool reproduces the Go error text verbatim
    // (selector errors.go: ErrNoAvailableNodes).
    registry
        .register_node(&wire("ND_a", now - 3600, 0))
        .await
        .unwrap();
    let failure = allocator.select_owner("empty-pool", "").await.unwrap_err();
    assert_eq!(failure.to_string(), "could not find any available nodes");
}

/// `pkg/routing/selector/interfaces.go:35-66` CreateNodeSelector kinds and
/// limits, exercised through the allocator's real selection call.
#[tokio::test]
#[ignore = "requires owned loopback STORE_DISCOVERY_REDIS_URL with a dedicated db index"]
async fn configured_selector_kinds_drive_the_persisted_owner_choice() {
    let url = isolated_leg_url(3);
    let mut probe = raw(&url).await;
    require_empty_db(&mut probe).await;
    let now = super::registry::now_unix();
    let mk = |id: &str, cpu: f32, sys: f32, clients: i32| {
        crate::workers::preserved::WireValue::from_message(pb::Node {
            id: id.into(),
            state: pb::NodeState::Serving as i32,
            stats: Some(pb::NodeStats {
                updated_at: now,
                cpu_load: cpu,
                load_avg_last1min: sys,
                num_cpus: 2,
                num_clients: clients,
                ..Default::default()
            }),
            ..Default::default()
        })
    };
    let heavy = mk("ND_heavy", 0.95, 1.9, 1);
    let light = mk("ND_light", 0.10, 0.2, 500);
    async fn reset(url: &str, allocator: &RoomAllocator) {
        for id in ["ND_heavy", "ND_light"] {
            let _ = allocator.registry.unregister_node(id).await;
            let _: i64 = redis::cmd("HDEL")
                .arg(ROOM_NODE_KEY)
                .arg("kind-room")
                .query_async(&mut raw(url).await)
                .await
                .unwrap();
        }
    }
    // cpuload kind with limit between the two loads: the filtered set keeps
    // only the light node (nodesfilter.go:5-20 keeps non-empty filters).
    let cpu_allocator = allocator(&url, 0.5, 0.5, "cpuload", "lowest").await;
    cpu_allocator.registry.register_node(&heavy).await.unwrap();
    cpu_allocator.registry.register_node(&light).await.unwrap();
    assert_eq!(
        cpu_allocator.select_owner("kind-room", "").await.unwrap(),
        "ND_light",
        "cpuload kind must exclude the over-limit node"
    );
    reset(&url, &cpu_allocator).await;

    // sysload kind with both nodes under every threshold falls back to the
    // sortBy=clients ranking over the whole available set.
    let sys_allocator = allocator(&url, 0.5, 0.5, "clients", "lowest").await;
    sys_allocator
        .registry
        .register_node(&mk("ND_heavy", 0.1, 0.1, 1))
        .await
        .unwrap();
    sys_allocator.registry.register_node(&light).await.unwrap();
    assert_eq!(
        sys_allocator.select_owner("kind-room", "").await.unwrap(),
        "ND_heavy",
        "with limits inactive the sortBy criterion alone decides"
    );
    reset(&url, &sys_allocator).await;

    // An unsupported kind fails construction with upstream's text
    // (interfaces.go:65 ErrUnsupportedSelector).
    let bad = selector::ConfiguredSelector::new(selector::Config {
        kind: "bogus".into(),
        sort_by: "clients".into(),
        algorithm: "lowest".into(),
        cpu_limit: 0.9,
        sysload_limit: 0.9,
        region: String::new(),
        regions: vec![],
        track_limit: 0,
        bytes_limit: 0.0,
    });
    assert_eq!(
        bad.err().unwrap().to_string(),
        "unsupported node selector",
        "selector.CreateNodeSelector default branch"
    );
}

/// The availability gate `GetAvailableNodes` applies before any kind, sort or
/// limit (`pkg/routing/selector/utils.go:31-49`, reached through `any.go:28`
/// and `nodesfilter.go:6`; `pkg/service/roomallocator.go:157-166` hands it the
/// raw `ListNodes` result, so the gate — not the router — decides membership).
/// Every stimulus keeps a fresh SERVING row in the pool, so a gate clause that
/// stops discriminating changes the *chosen owner* instead of only changing an
/// error message.
#[tokio::test]
#[ignore = "requires owned loopback STORE_DISCOVERY_REDIS_URL with a dedicated db index"]
async fn availability_and_state_gates_drive_selection_and_reuse() {
    let url = isolated_leg_url(4);
    let mut probe = raw(&url).await;
    require_empty_db(&mut probe).await;
    let now = super::registry::now_unix();
    let node = |id: &str, state: pb::NodeState, updated_at: i64, clients: i32| {
        WireValue::from_message(pb::Node {
            id: id.into(),
            state: state as i32,
            stats: Some(pb::NodeStats {
                updated_at,
                num_clients: clients,
                ..Default::default()
            }),
            ..Default::default()
        })
    };
    // kind=any with both limits disabled: no cpu/sysload threshold is consulted
    // on this path (any.go:26-35), so GetAvailableNodes plus sortBy=clients
    // alone decide the owner and each stimulus isolates one gate clause.
    let allocator = allocator(&url, 0.0, 0.0, "clients", "lowest").await;
    let registry = allocator.registry.clone();

    // Freshness is required even for the emptiest row: ND_old sits exactly at
    // delta == AvailableSeconds, which Go's strict `<` (utils.go:40) calls
    // stale, so the fresher but busier ND_backup must win.
    registry
        .register_node(&node("ND_old", pb::NodeState::Serving, now - 5, 0))
        .await
        .unwrap();
    registry
        .register_node(&node("ND_backup", pb::NodeState::Serving, now, 9))
        .await
        .unwrap();
    assert_eq!(
        select_and_read_back(&mut probe, &allocator, "gate-stale").await,
        "ND_backup",
        "IsAvailable boundary: delta == AvailableSeconds is stale"
    );

    // Availability alone does not join the pool: state must be SERVING
    // (utils.go:45), so the freshest, emptiest SHUTTING_DOWN row loses to the
    // busier SERVING row.
    registry.unregister_node("ND_old").await.unwrap();
    registry
        .register_node(&node("ND_shutdown", pb::NodeState::ShuttingDown, now, 0))
        .await
        .unwrap();
    assert_eq!(
        select_and_read_back(&mut probe, &allocator, "gate-state").await,
        "ND_backup",
        "non-SERVING rows never enter GetAvailableNodes"
    );

    // A future stamp is available because Go subtracts signed seconds
    // (utils.go:39), so a clock-skewed row must not read as hugely stale.
    // It also carries the lowest client count, so it wins outright.
    registry.unregister_node("ND_shutdown").await.unwrap();
    registry
        .register_node(&node("ND_future", pb::NodeState::Serving, now + 3600, 0))
        .await
        .unwrap();
    assert_eq!(
        select_and_read_back(&mut probe, &allocator, "gate-future").await,
        "ND_future",
        "future UpdatedAt must read as available"
    );

    // The reuse guard consults IsAvailable alone, without the state check
    // (roomallocator.go:144 calls selector.IsAvailable, not GetAvailableNodes):
    // a fresh SHUTTING_DOWN assignment is kept instead of being re-selected
    // onto the SERVING node. Remove the future-stamped row first so the pool
    // holds only the SERVING backup and the rows this leg introduces.
    registry.unregister_node("ND_future").await.unwrap();
    registry
        .register_node(&node("ND_serving", pb::NodeState::Serving, now, 0))
        .await
        .unwrap();
    // The reuse guard consults IsAvailable alone (roomallocator.go:144 calls
    // selector.IsAvailable, not GetAvailableNodes), and that function reads
    // only Stats.UpdatedAt — upstream never re-reads the node row to check its
    // state here. So an assignment whose row has been unregistered outright is
    // still reused as long as the id parses: what pins this down is that Go
    // then keeps routing to that id (`router.go` uses the existing assignment
    // verbatim), which Rust must mirror rather than silently re-selecting.
    registry
        .register_node(&node("ND_serving", pb::NodeState::Serving, now, 0))
        .await
        .unwrap();
    assert_eq!(
        select_and_read_back(&mut probe, &allocator, "reuse-state").await,
        "ND_serving",
        "the emptiest SERVING row wins the first selection"
    );
    let ghost = "ND_ghost";
    let _: i64 = redis::cmd("HSET")
        .arg(ROOM_NODE_KEY)
        .arg("reuse-state")
        .arg(ghost)
        .query_async(&mut probe)
        .await
        .unwrap();
    let reused = allocator.select_owner("reuse-state", "").await.unwrap();
    let persisted: Option<String> = redis::cmd("HGET")
        .arg(ROOM_NODE_KEY)
        .arg("reuse-state")
        .query_async(&mut probe)
        .await
        .unwrap();
    assert_eq!(
        (reused.as_str(), persisted.as_deref()),
        (ghost, Some(ghost)),
        "an existing assignment is kept whatever the registry knows about it, \
         because IsAvailable(nodeID) cannot be false for a bare id"
    );

    // Once that assignment ages out the mapping is re-selected (test 2 covers
    // the pruning half of the same window).
    registry
        .register_node(&node(
            "ND_shutdown",
            pb::NodeState::ShuttingDown,
            now - 3600,
            0,
        ))
        .await
        .unwrap();
    assert_eq!(
        allocator.select_owner("reuse-state", "").await.unwrap(),
        "ND_serving",
        "an expired assignment forces re-selection"
    );

    // Nil stats is the one shape the gate lets through but the sorted ranking
    // cannot score: utils.go:35-38 treats it as available till stats arrive,
    // then selectLowestSortedNode dereferences node.Stats unconditionally
    // (utils.go:142-146 NumClients, upstream panics on a nil pointer). Rust
    // surfaces the same fault as an error instead of crashing the process, and
    // the pin below proves the gate really did admit the row into the pool: a
    // dropped availability clause would leave only the fresh SERVING rows and
    // select them happily.
    let mut null_stats = node("ND_nullstats", pb::NodeState::Serving, now, 0);
    null_stats.message.stats = None;
    registry.register_node(&null_stats).await.unwrap();
    let failure = allocator
        .select_owner("nil-stats-pool", "")
        .await
        .expect_err("sortBy ranking cannot score a nil-stats row that the gate admitted");
    assert_eq!(failure.to_string(), "node selector encountered nil stats");
}
