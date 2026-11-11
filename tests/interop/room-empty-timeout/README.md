# Configured `room.empty_timeout` empty-room auto-close — paired Go oracle vs Rust

Real, deterministic HTTP-only acceptance for the global `room.empty_timeout`
(Go `config.go:267` uint32 seconds; `room.go:295` a room created without an
explicit value inherits `roomConfig.EmptyTimeout`; `room.go:789-808` `CloseIfEmpty`
closes a never-joined room once `now - CreationTime >= EmptyTimeout` with
`RoomCloseReasonIdleTimeout`; runs every second per `room.go:1435`). Fixed Go
`v1.13.10` (SHA `4bc2378e`) vs current Rust candidate `livecloud` (SHA `8ad74904`).

`run-paired.py --go <pinned livekit-server> --rust <livecloud> --output <new-owned-dir>`
starts, per profile, one fresh Go server (port 18400) and one fresh Rust server
(port 18410) sharing an identical YAML overlay (only HTTP/media ports differ),
rejects occupied ports, waits for real HTTP readiness, runs the same unchanged
node raw-JWT HTTP RoomService consumer, then SIGINT+waits both servers and
requires exit 0. A failed profile is retained, never counted partial.

`run.mjs` (env `REFERENCE_URL`, `LIVEKIT_URL`, `ET_PROFILE`, `ET_WINDOW`,
`ET_EXPECT_TIMEOUT`, `ET_OUTPUT`) creates an empty room (no join), asserts the
server reports the configured global `empty_timeout` on it, polls `ListRooms`
until the room disappears or the window elapses, and deep-equals the normalized
Rust observation `{reported_empty_timeout, closed_in_window}` against Go.

Profiles:
- `et-short` (`room.empty_timeout: 2`, 8s window): the empty room auto-closes.
- `et-long` (`room.empty_timeout: 600`, 4s window): the empty room persists.

Cross-profile control: the pinned Go oracle closes the short-timeout room and
persists the long-timeout room, so a config a server ignores could never
masquerade as agreement; Rust reproduces both byte-for-byte. Aggregate status
`PASS_CONFIGURED_EMPTY_TIMEOUT_EFFECT`.

Scope: the never-joined empty-from-creation branch only. Not the participant-
departure branch (`room.departure_timeout`), the CLI per-key startup flag, ENV
override, `room_configurations.{key}.emptytimeout` preset, or byte-exact close
latency (compared as a boolean within a bounded window) — those stay separate
ledger leaves. No SDK mutation, no browser, no media, no webhook, no global/100%
claim.
