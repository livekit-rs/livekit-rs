# Generated integer CLI flag `--room.max_participants` applied on the real WS join gate — paired Go oracle vs Rust

Real, deterministic acceptance for the **integer flag input path** of the generated CLI
flag `--room.max_participants` (Go `config.go` `Room.MaxParticipants`, YAML
`max_participants`, default `0` = unlimited) driving the authenticated WebSocket
signaling join gate. Go rejects the N+1th joiner in `roommanager`/`room` once the room is
at `MaxParticipants`; Rust mirrors the same gate reading `config.room.max_participants`,
sourced here through the generated flag path `crates/lk-config/src/generated.rs`
(`parse_value` `Kind::Int`). This is the **flag** path; the **YAML** value path is a
separate already-verified fixture (`room-max-participants`). Fixed Go `v1.13.10`
(SHA `4bc2378e`) vs current Rust candidate `livecloud` (SHA `8ad74904`).

`run-paired.py --go <pinned livekit-server> --rust <livecloud> --output <new-owned-dir>`
starts, per profile, one fresh Go server (port 19800) and one fresh Rust server (port
19810) sharing an identical base YAML plus the **same extra argv** (only HTTP/media ports
differ), rejects occupied ports, waits for real HTTP readiness, runs the **same unchanged
node raw-JWT WebSocket consumer** (identical logic to `room-max-participants/run.mjs`),
then SIGINT+waits both servers and requires exit 0. A failed profile is retained, never
counted partial.

`run.mjs` (env `REFERENCE_WS_URL`, `LIVEKIT_WS_URL`, `FMP_PROFILE`, `FMP_OUTPUT`) creates
a room, holds **peer A** joined over the real `/rtc` WS signaling endpoint so the room is
genuinely occupied, then attempts **peer B** — the `max_participants` gate — and reads the
observable join outcome (`joined`, got_join/got_leave, close code/clean) plus
`ListParticipants` room membership. It normalizes away the per-server room name and
requires Rust to deep-equal the pinned Go oracle.

Profiles (all Go == Rust):
- `flag-mp-1` — `--room.max_participants=1`. Peer A joins; peer B is **rejected**
  (`joined=false`); membership stays `[peer-a]`.
- `flag-mp-off` — `--room.max_participants=0` (unlimited). Both A and B join; membership
  `[peer-a, peer-b]`.

Cross-profile control: the SAME two-peer sequence leaves Go peer-B rejected under
`flag-mp-1` and joined under `flag-mp-off`, so the integer flag is genuinely parsed and
applied as the live join ceiling — a server ignoring argv (leaving the default unlimited)
could never produce the `flag-mp-1` rejection. Rust reproduces both membership lists and
join outcomes exactly. Aggregate status
`PASS_GENERATED_CLI_FLAG_ROOM_MAX_PARTICIPANTS_EFFECT`.

Scope: the generated `--room.max_participants` integer flag parse → apply → WS join gate
path only, for this one flag. Not the whole generated-flag catalog, not the ENV single-key
`LIVEKIT_ROOM_MAX_PARTICIPANTS` consumer, not the `room.max_participants` YAML value leaf
itself (separate verified fixture), not media/RTP, `/rtc/validate` status codes, webhook or
Redis. No browser, no SDK mutation, no global/100% claim.
