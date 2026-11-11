# Generated integer CLI flag `--room.empty_timeout` parsed, applied and enforced — paired Go oracle vs Rust

Real, deterministic acceptance for the **integer flag input path** of the generated CLI
flag `--room.empty_timeout` (Go `config.go` `RoomConfig.EmptyTimeout uint32`
`yaml:"empty_timeout"`, upstream default 300 seconds). Go enforces the behavior at the
room-manager level: a room created without an explicit `empty_timeout` inherits the
global default, and after remaining empty past that threshold it is auto-closed. Rust
mirrors this on the same code path — the flag is parsed via
`crates/lk-config/src/generated.rs` (`parse_value` `Kind::Uint`) into
`config.room.empty_timeout`, and the auto-close timer on empty rooms is driven by that
value. Fixed Go `v1.13.10` (SHA `4bc2378e`) vs current Rust candidate `livecloud` (SHA
`8ad74904`).

This is the **flag** input path on the **same enforcement method** as the sibling YAML
value leaf fixture `room-empty-timeout` (which verified the YAML `room.empty_timeout`
path in an earlier batch). The two together cover both input paths for the empty-room
auto-close behavior.

`run-paired.py --go <pinned livekit-server> --rust <livecloud> --output <new-owned-dir>`
starts, per profile, one fresh Go server (port 20100) and one fresh Rust server (port
20110) sharing an identical base YAML plus the **same extra argv** (only HTTP/media
ports differ), rejects occupied ports, waits for real HTTP readiness, runs the same
unchanged node raw-JWT WS+timing consumer (`run.mjs` is byte-identical to the
`room-empty-timeout` fixture's consumer — same CreateRoom → ListRooms poll →
closed_in_window observations, only the launcher feeds the value via argv instead of
via YAML), then SIGINT+waits both servers and requires exit 0. A failed profile is
retained, never counted partial.

Profiles (all Go == Rust):
- `flag-et-short` — flag `--room.empty_timeout=2`, 8-second poll window: the room's
  `empty_timeout` on CreateRoom reports **2** on both Go and Rust (the flag value
  reaches the created room), and the room is **auto-closed** within the 8-second
  window on both sides.
- `flag-et-long` — flag `--room.empty_timeout=600`, 4-second poll window: the room's
  `empty_timeout` on CreateRoom reports **600** on both Go and Rust, and the room is
  **still open** within the 4-second window on both sides.

Cross-profile control: the pinned Go oracle **closes** `flag-et-short` and **persists**
`flag-et-long` — a server that ignores argv (retaining the upstream default 300) could
never produce that differentiation. The oracle-reported `empty_timeout` on the created
room must equal the flag value (2 vs 600), proving the flag value reaches enforcement
and is echoed on the created `Room` message. Rust matches Go on both the reported
`empty_timeout` and the `closed_in_window` boolean per profile. Aggregate status
`PASS_GENERATED_CLI_FLAG_ROOM_EMPTY_TIMEOUT_EFFECT`.

Scope: the generated `--room.empty_timeout` integer flag parse → apply → empty-room
auto-close behavior path only, for this one flag. Not the whole generated-flag catalog,
not the ENV single-key `LIVEKIT_ROOM_EMPTY_TIMEOUT` consumer, not the `room.empty_timeout`
YAML value leaf itself (separate verified fixture `room-empty-timeout`), not the
per-CreateRoom-request explicit `empty_timeout` override, not the
departure-timeout path (`room.departure_timeout`, separate fixture), not WS signaling
join, media, or webhook. No browser, no media/RTP, no SDK mutation, no global/100%
claim.
