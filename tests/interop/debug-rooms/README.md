# Debug rooms identity-map candidate (not yet accepted)

Fixed source oracle: LiveKit v1.13.10 e5655ac20267fa5005dccaf3f0e3bfc3ea688c07:
`pkg/rtc/room.go:1846-1858`, `participant.go:3945-3970`,
`uptrackmanager.go:422-442`, `mediatrackreceiver.go:1148-1162`.

Implemented candidate:

- `/debug/rooms` preserves Name/Sid/CreatedAt, and Participants is keyed by actual
  participant identity. The previous NumParticipants/NumPublishers aggregate
  object was incompatible with Go's map.
- `RoomManager::debug_room_details` queries each captured room-generation actor.
  One actor turn returns RoomInfo and current participants together; it does not
  use the API participant-store cache. Deleted/closed actors produce no entry.
- ID and State use actual ParticipantInfo state. PublishedTracks maps actual
  published track SIDs to ID/Kind/PubMuted; empty published sets are real state.
- No PendingTracks, Migrated, DownTracks, or codec receiver debug state is invented.
  These fields remain **known DebugInfo compatibility gaps**. The endpoint remains
  partial and must not be promoted to whole-body compatibility.

When resource checks are authorized, run from livekit-rs-server:

```
cargo test -p livecloud --lib debug::tests::rooms_debug_uses_live_identity_state_and_drops_old_generation -- --exact
cargo test -p livecloud --lib debug::tests::rooms_json_lists_actual_created_rooms -- --exact
```

The new lifecycle check uses actual room actors to join two identities, inspect
live state, remove one participant, delete the room, and recreate the same room
name with an independent SID and empty identity map. No server process is started.
Tests have not yet been run. Remaining acceptance: actual pinned-Go paired HTTP
body comparison for joins/state transitions/published/muted/subscribed tracks;
pending-track/migration and receiver/downtrack data producers; concurrent room
close/recreation and endpoint response under a stalled owner. Existing pprof 501
collectors remain unchanged and open.
