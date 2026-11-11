# Generated integer CLI flag `--room.departure_timeout` parsed, applied and enforced — paired Go oracle vs Rust

Real, deterministic acceptance for the **integer flag input path** of the generated CLI
flag `--room.departure_timeout` (Go `config.go` `RoomConfig.DepartureTimeout uint32`
`yaml:"departure_timeout"`, upstream default 1800 seconds). Go enforces the behavior at
the room-manager level: after the last **independent** participant departs, if the room
was created without an explicit `departure_timeout`, it inherits the global default and
auto-closes after that many seconds of remaining empty. Rust mirrors this on the same
code path — the flag is parsed via `crates/lk-config/src/generated.rs` (`parse_value`
`Kind::Int`) into `config.room.departure_timeout`, and the auto-close timer on
last-participant-departure is driven by that value. Fixed Go `v1.13.10` (SHA `4bc2378e`)
vs current Rust candidate `livecloud` (SHA `8ad74904`).

This is the **flag** input path on the **same enforcement method** as the sibling YAML
value leaf fixture `room-departure-timeout` (which verified the YAML
`room.departure_timeout` path in an earlier batch). The two together cover both input
paths for the last-participant-departure auto-close behavior.

`run-paired.py --go <pinned livekit-server> --rust <livecloud> --output <new-owned-dir>`
starts, per profile, one fresh Go server (port 20200) and one fresh Rust server (port
20210) sharing an identical base YAML (which pins `room.empty_timeout: 600` in YAML so
only the `--room.departure_timeout` flag under test drives the departure branch, not
the from-creation empty branch) plus the **same extra argv** (only HTTP/media ports
differ), rejects occupied ports, waits for real HTTP readiness, runs the same unchanged
node raw-JWT WS+timing consumer (`run.mjs` is byte-identical to the
`room-departure-timeout` fixture's consumer — same real WS join → close → ListRooms
poll), then SIGINT+waits both servers and requires exit 0. A failed profile is retained,
never counted partial.

Profiles (all Go == Rust on both `reported_departure_timeout` and
`closed_after_departure`, and `reported_empty_timeout` pinned to 600 from YAML):
- `flag-dt-short` — flag `--room.departure_timeout=2`, 12-second poll window: the room's
  `departure_timeout` on CreateRoom reports **2** on both Go and Rust, `empty_timeout`
  reports **600** (YAML, not overridden by the flag), and the room is **auto-closed**
  within 12 seconds after the sole peer's real WS join then close.
- `flag-dt-long` — flag `--room.departure_timeout=600`, 6-second poll window: the room's
  `departure_timeout` reports **600**, `empty_timeout` reports **600**, and the room is
  **still open** within 6 seconds after departure on both sides.

Cross-profile control: the pinned Go oracle **closes** `flag-dt-short` and **persists**
`flag-dt-long` — a server that ignores argv (retaining the upstream default 1800 or a
YAML-only reading) could never produce that behavioral differentiation. The oracle's
`reported_departure_timeout` on the created Room must equal the flag value (2 vs 600),
proving the flag value reaches enforcement and is echoed on the created `Room` message.
Rust matches Go on both the reported `departure_timeout` and the `closed_after_departure`
boolean per profile. Aggregate status
`PASS_GENERATED_CLI_FLAG_ROOM_DEPARTURE_TIMEOUT_EFFECT`.

Scope: the generated `--room.departure_timeout` integer flag parse → apply →
last-participant-departure auto-close behavior path only, for this one flag. Not the
whole generated-flag catalog, not the ENV single-key `LIVEKIT_ROOM_DEPARTURE_TIMEOUT`
consumer, not the `room.departure_timeout` YAML value leaf itself (separate verified
fixture `room-departure-timeout`), not the per-CreateRoom-request explicit
`departure_timeout` override, not the empty-from-creation path
(`room.empty_timeout`, separate verified fixture), not WS signaling join beyond the
real-peer join/close the harness needs, media, or webhook. No browser, no media/RTP, no
SDK mutation, no global/100% claim.
