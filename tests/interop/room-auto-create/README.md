# Configured `room.auto_create` /rtc/validate effect — paired Go oracle vs Rust

Real, deterministic HTTP-only acceptance for the `room.auto_create` config gate
(Go `roomallocator.go` `if !r.config.Room.AutoCreate`; Rust `signal/mod.rs`
`may_create = auto_create || room_create`). Fixed Go `v1.13.10` (SHA `4bc2378e`)
vs current Rust candidate `livecloud` (SHA `8ad74904`).

`run-paired.py --go <pinned livekit-server> --rust <livecloud> --output <new-owned-dir>`
starts, per profile, one fresh Go server (port 18080) and one fresh Rust server
(port 18090) sharing an identical YAML overlay (only HTTP/media ports differ),
rejects occupied ports, waits for real HTTP readiness, runs the same unchanged
node raw-JWT `/rtc/validate` + RoomService consumer against both, then SIGINT+
waits both servers and requires exit 0. A failed profile is retained, never
counted partial.

Profiles: `ac-true` (`room.auto_create: true`), `ac-false` (`room.auto_create:
false`). `run.mjs` (env `REFERENCE_URL`, `LIVEKIT_URL`, `AC_PROFILE`,
`AC_OUTPUT`) captures, per side, the 5-case grant/existence matrix and requires
Rust to deep-equal Go byte-for-byte.

Observed (Go ≡ Rust, both profiles):
- join-only token → non-existent room: **200 when auto_create=true**, **404
  `requested room does not exist` when false** — the core gate.
- roomCreate grant → non-existent room: 200 under either profile.
- admin CreateRoom then join-existing: 200 under either profile.
- ListRooms confirms `/rtc/validate` did **not** materialize the room.

Cross-profile control: the two profiles' Go join-nonexistent codes must DIFFER
(200 vs 404), so a config a server ignores could never masquerade as agreement.
Aggregate status `PASS_CONFIGURED_AUTO_CREATE_EFFECT`.

Scope: the `/rtc/validate` HTTP effect and non-materialization only. Not the
WebSocket signaling join lifecycle after a 200, the CLI per-key startup flag,
ENV override, or `room_configurations.{key}` preset auto_create — those stay
separate ledger leaves. No SDK mutation, no media, no fake responder, no
global/100% claim.
