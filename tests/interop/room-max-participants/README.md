# Configured `room.max_participants` join gate — paired Go oracle vs Rust

Real, deterministic authenticated **WebSocket signaling join** acceptance for the
global `room.max_participants` default (Go `roomallocator.go:224` copies the
config default into each allocated room when unset; `room.go:458-466` returns
`ErrMaxParticipantsExceeded` when the count of independent participants already
in the room reaches the limit). Fixed Go `v1.13.10` (SHA `4bc2378e`) vs current
Rust candidate `livecloud` (SHA `8ad74904`).

`run-paired.py --go <pinned livekit-server> --rust <livecloud> --output <new-owned-dir>`
starts, per profile, one fresh Go server (port 18200) and one fresh Rust server
(port 18210) sharing an identical YAML overlay (only HTTP/media ports differ),
rejects occupied ports, waits for real HTTP readiness, runs the same unchanged
node raw-JWT consumer that opens a real `/rtc` WebSocket signaling join (reusing
the untouched `../signed-name-limits/raw-protocol.mjs` protobuf reader), then
SIGINT+waits both servers and requires exit 0. A failed profile is retained,
never counted partial.

`run.mjs` (env `REFERENCE_WS_URL`, `LIVEKIT_WS_URL`, `MP_PROFILE`, `MP_OUTPUT`)
holds peer A joined so the room is genuinely occupied, attempts peer B over the
limit, and reads `ListParticipants`, then deep-equals the normalized Rust
observations against Go (join outcome, join/leave frames received, WS close code,
transport error, room membership).

Profiles:
- `mp-1` (`room.max_participants: 1`): peer B is rejected — no `join` frame, the
  WS is torn down abnormally (close code 1006, clean=false) and B is NOT added to
  the room (`membership: ["peer-a"]`).
- `mp-off` (`room.max_participants: 0`, unlimited): peer B joins and appears in
  `ListParticipants` (`membership: ["peer-a","peer-b"]`).

Cross-profile control: the pinned Go oracle's peer-B outcome must DIFFER between
`mp-1` (rejected) and `mp-off` (joined), so a config a server ignores could never
masquerade as agreement; Rust reproduces both byte-for-byte. Aggregate status
`PASS_CONFIGURED_MAX_PARTICIPANTS_GATE`.

Scope: the real signaling join gate only. Not the CLI per-key startup flag, ENV
override, `room_configurations.{key}.maxparticipants` preset, per-room
CreateRoom `max_participants` param round-trip (already covered at the API layer
by functional-room-state), or dependent/agent participant-count nuances — those
stay separate ledger leaves. No SDK mutation, no browser, no media/RTP, no fake
RPC, no global/100% claim.
