//! Measured verdict for the "direct_url" create-routing question: what does the
//! pinned server do when a CreateRoom arrives on a node that is *not* the room's
//! owner, with `CreateRoomRequest.node_id` (the CLI's direct-URL pin) set?
//!
//! The upstream contract this suite pins comes from re-reading the fixed Go
//! reference at e5655ac (livekit v1.13.10):
//! - `pkg/service/roomservice.go:100` calls `SelectRoomNode(ctx, req.Name, req.NodeId)`
//!   and then `pkg/service/roomservice.go:104` calls `s.router.CreateRoom`.
//! - `pkg/service/roomallocator.go:136-174` — the reuse branch keeps an available
//!   assignment (:144-152), otherwise an explicit `nodeID` short-circuits
//!   ListNodes/SelectNode entirely (:157-169) and is written back verbatim (:171).
//! - `pkg/routing/redisrouter.go:153-160` — `RedisRouter.CreateRoom` ignores the
//!   request's own `NodeId` and re-reads the durable mapping through
//!   `GetNodeForRoom`, i.e. it *routes to* the owner rather than creating locally.
//! - `pkg/routing/localrouter.go:100-102` — the RPC client sends CreateRoom to the
//!   resolved `nodeID` topic; there is no local fallback on a non-owner.
//!
//! So the answer is "forward to the owner", and the port already does exactly
//! that (`room_control/mod.rs:546-556` → `create_spec(node)` → real PSRPC). This
//! file therefore contains no invented behaviour: every leg asserts the routing
//! outcome plus the one place where a stale mapping makes our explicit-pin branch
//! differ observably from Go, which is why that branch now logs its authority.
//!
//! Runs only against an owned, empty, loopback Redis db index (see `isolated_leg_url`).

use super::registry::ROOM_NODE_KEY;
use super::store_discovery_tests::{allocator, isolated_leg_url, raw, require_empty_db};
use super::*;
use std::sync::OnceLock;

/// Register a fresh SERVING row carrying `clients` so `sortBy=clients` ranking is
/// deterministic between the two nodes.
async fn register(allocator: &RoomAllocator, id: &str, clients: i32) {
    allocator
        .registry
        .register_node(&WireValue::from_message(pb::Node {
            id: id.into(),
            ip: "127.0.0.1".into(),
            num_cpus: 2,
            region: "local".into(),
            state: pb::NodeState::Serving as i32,
            stats: Some(pb::NodeStats {
                started_at: super::registry::now_unix(),
                updated_at: super::registry::now_unix(),
                num_clients: clients,
                ..Default::default()
            }),
            ..Default::default()
        }))
        .await
        .unwrap();
}

/// Age one registered row past `AvailableSeconds` without touching its id, so the
/// reuse gate (`node_available`) alone decides whether the mapping is kept.
async fn age(allocator: &RoomAllocator, id: &str) {
    let mut node = allocator.registry.get_node(id).await.unwrap().message;
    node.stats
        .as_mut()
        .expect("registered row carries stats")
        .updated_at = super::registry::now_unix() - 3600;
    allocator
        .registry
        .register_node(&WireValue::from_message(node))
        .await
        .unwrap();
}

async fn assignment(url: &str, room: &str) -> Option<String> {
    let mut probe = raw(url).await;
    redis::cmd("HGET")
        .arg(ROOM_NODE_KEY)
        .arg(room)
        .query_async(&mut probe)
        .await
        .unwrap()
}

/// An explicit pin is adopted verbatim even when the pool would rank the other
/// node first, and the persisted mapping agrees with the returned owner
/// (roomallocator.go:157-171). A silent ignore of the pin fails here.
#[tokio::test]
#[ignore = "requires owned loopback STORE_DISCOVERY_REDIS_URL with a dedicated db index"]
async fn explicit_create_pin_overrides_the_ranking_and_persists() {
    let url = isolated_leg_url(11);
    require_empty_db(&mut raw(&url).await).await;
    let allocator = allocator(&url, 0.0, 0.0, "clients", "lowest").await;
    let capture = Capture::install();
    // ND_busy is deliberately the worse `clients` choice: selecting must pick ND_light.
    register(&allocator, "ND_light", 0).await;
    register(&allocator, "ND_busy", 500).await;

    assert_eq!(
        allocator.select_owner("pin-new", "ND_busy").await.unwrap(),
        "ND_busy",
        "an explicit pin must win over the ranking, as in Go"
    );
    assert_eq!(
        assignment(&url, "pin-new").await.as_deref(),
        Some("ND_busy"),
        "SetNodeForRoom persists the pinned id (roomallocator.go:171)"
    );
    // Control: the same pool without a pin ranks the emptier node, proving the
    // pin above actually changed the outcome instead of the fixture being degenerate.
    assert_eq!(
        allocator.select_owner("no-pin", "").await.unwrap(),
        "ND_light",
        "unpinned selection still uses the configured ranking"
    );

    // A pin to a node that is not registered at all is still adopted (Go performs
    // no availability check on the explicit path), which is precisely why the
    // downstream create can only fail as "no response from servers".
    assert_eq!(
        allocator
            .select_owner("pin-absent", "ND_absent")
            .await
            .unwrap(),
        "ND_absent",
        "Go SelectRoomNode never validates an explicit node id"
    );
    // Neither room had a mapping when it was pinned, so neither may claim an
    // override: this is the leg a mutant that logs unconditionally has to redden.
    let captured = capture.text();
    for room in ["pin-new", "pin-absent"] {
        assert_eq!(
            override_lines(&captured, room).len(),
            0,
            "a first pinned create replaces nothing: {captured}"
        );
    }
}

/// With a *fresh* assignment the pin is ignored and the existing owner is kept
/// (roomallocator.go:144-152 runs before the explicit branch at :157).
#[tokio::test]
#[ignore = "requires owned loopback STORE_DISCOVERY_REDIS_URL with a dedicated db index"]
async fn fresh_assignment_beats_a_conflicting_explicit_pin() {
    let url = isolated_leg_url(12);
    require_empty_db(&mut raw(&url).await).await;
    let allocator = allocator(&url, 0.0, 0.0, "clients", "lowest").await;
    register(&allocator, "ND_owner", 0).await;
    register(&allocator, "ND_other", 500).await;
    let capture = Capture::install();

    assert_eq!(
        allocator.select_owner("pin-reuse", "").await.unwrap(),
        "ND_owner"
    );
    assert_eq!(
        allocator
            .select_owner("pin-reuse", "ND_other")
            .await
            .unwrap(),
        "ND_owner",
        "reuse of an available owner outranks the caller's pin, like Go"
    );
    assert_eq!(
        assignment(&url, "pin-reuse").await.as_deref(),
        Some("ND_owner"),
        "the reuse branch must not rewrite the mapping to the pinned id"
    );
    let captured = capture.text();
    assert_eq!(
        override_lines(&captured, "pin-reuse").len(),
        0,
        "an available owner is kept, so nothing was overridden: {captured}"
    );
}

/// The one measured divergence class: when the existing mapping has aged out, Go's
/// reuse gate falls through and the explicit id is adopted; our port reaches the
/// same terminal owner but must say so, because the mapping it overrides was
/// never reported. The diagnostic is the deliverable, not invented routing.
#[tokio::test]
#[ignore = "requires owned loopback STORE_DISCOVERY_REDIS_URL with a dedicated db index"]
async fn stale_assignment_is_adopted_by_the_pin_with_an_authority_log() {
    let url = isolated_leg_url(13);
    require_empty_db(&mut raw(&url).await).await;
    let allocator = allocator(&url, 0.0, 0.0, "clients", "lowest").await;
    register(&allocator, "ND_stale", 0).await;
    register(&allocator, "ND_pin", 500).await;

    // Establish, then age, the mapping.
    assert_eq!(
        allocator.select_owner("pin-stale", "").await.unwrap(),
        "ND_stale"
    );
    age(&allocator, "ND_stale").await;

    let capture = Capture::install();
    let chosen = allocator.select_owner("pin-stale", "ND_pin").await.unwrap();
    let captured = capture.text();

    assert_eq!(
        chosen, "ND_pin",
        "a stale mapping must not strand the create on the aged-out owner"
    );
    assert_eq!(
        assignment(&url, "pin-stale").await.as_deref(),
        Some("ND_pin"),
        "the adopted pin overwrites the stale mapping in place"
    );
    let lines = override_lines(&captured, "pin-stale");
    assert_eq!(
        lines.len(),
        1,
        "the port must report that it replaced a stale assignment instead of \
         silently re-pinning (the honest-diagnostic half of this verdict): {captured}"
    );
    // The line must name both authorities, otherwise it is not actionable.
    assert!(
        lines[0].contains("pinned=\"ND_pin\""),
        "the diagnostic must carry the pinned node: {}",
        lines[0]
    );
}

/// A create with no prior mapping logs nothing: the diagnostic above is bound to
/// the override case only, so a mutant that logs unconditionally fails here.
#[tokio::test]
#[ignore = "requires owned loopback STORE_DISCOVERY_REDIS_URL with a dedicated db index"]
async fn first_pinned_create_reports_no_override_diagnostic() {
    let url = isolated_leg_url(14);
    require_empty_db(&mut raw(&url).await).await;
    let allocator = allocator(&url, 0.0, 0.0, "clients", "lowest").await;
    register(&allocator, "ND_pin", 500).await;

    let capture = Capture::install();
    let chosen = allocator.select_owner("pin-first", "ND_pin").await.unwrap();
    let captured = capture.text();
    assert_eq!(chosen, "ND_pin");
    assert_eq!(
        override_lines(&captured, "pin-first").len(),
        0,
        "an absent mapping is not an override: {captured}"
    );
}

/// Process-wide DEBUG sink for the legs that reason about log events.
///
/// A thread-scoped default cannot carry an absence claim here: a probe that
/// printed `std::thread::current().id()` showed the explicit-pin branch emitting
/// from `ThreadId(2)` while the leg that installed its capture guard ran on
/// `ThreadId(5)`, so the event missed the thread-local subscriber, the buffer
/// stayed empty and the leg passed for the wrong reason. One global subscriber
/// receives the event from whichever thread emits it; the `room` field, unique per
/// leg, is what attributes it, and `Capture::text` proves the sink is live before
/// any leg is allowed to reason about what it did not receive.
#[derive(Clone, Default)]
struct ErrorLog(Arc<std::sync::Mutex<Vec<u8>>>);

/// Ceiling on the shared buffer: these legs read what they need within one call,
/// so retaining the tail is enough while a full `--ignored` run cannot grow it.
const SINK_CAP: usize = 1 << 20;

impl ErrorLog {
    fn snapshot(&self) -> String {
        String::from_utf8_lossy(&self.0.lock().unwrap().clone()).into_owned()
    }
}

impl std::io::Write for ErrorLog {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        let mut log = self.0.lock().unwrap();
        log.extend_from_slice(buf);
        if log.len() > SINK_CAP {
            let cut = log.len() - SINK_CAP;
            log.drain(..cut);
        }
        Ok(buf.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for ErrorLog {
    type Writer = ErrorLog;
    fn make_writer(&'a self) -> Self::Writer {
        self.clone()
    }
}

static SINK: OnceLock<(
    ErrorLog,
    Result<(), tracing::subscriber::SetGlobalDefaultError>,
)> = OnceLock::new();
static CANARY_SEQ: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(1);

/// A leg's handle on the shared sink plus a token only this install could write.
struct Capture {
    sink: ErrorLog,
    canary: String,
}

impl Capture {
    /// Installs the process-wide sink once and emits a per-leg canary through it.
    fn install() -> Self {
        let (sink, install) = SINK.get_or_init(|| {
            let log = ErrorLog::default();
            let install = tracing::subscriber::set_global_default(
                tracing_subscriber::fmt()
                    .with_writer(log.clone())
                    .with_ansi(false)
                    .with_max_level(tracing::Level::DEBUG)
                    .finish(),
            );
            (log, install)
        });
        let canary = format!(
            "direct-pin-subscriber-canary-{}",
            CANARY_SEQ.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
        );
        let _ = install;
        tracing::debug!("{canary}");
        Self {
            sink: sink.clone(),
            canary,
        }
    }

    /// The lines the sink holds, with this leg's canary proven present.
    fn text(self) -> String {
        let captured = self.sink.snapshot();
        assert!(
            captured.contains(&self.canary),
            "the shared sink holds no canary for this leg, so any claim about the logs it \
             did or did not receive is void: {captured}"
        );
        captured
    }
}

/// The diagnostic under test, and the only line the override legs count.
const OVERRIDE: &str = "explicit create pin overrides an existing stale room assignment";

/// The override lines the sink holds for one room. Keying on the room is what
/// makes the count leg-local even though the sink is shared by the whole process.
fn override_lines<'a>(captured: &'a str, room: &str) -> Vec<&'a str> {
    let needle = format!("room=\"{room}\"");
    captured
        .lines()
        .filter(|line| line.contains(OVERRIDE) && line.contains(&needle))
        .collect()
}
