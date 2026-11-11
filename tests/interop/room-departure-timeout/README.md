# Configured `room.departure_timeout` last-participant-departure auto-close — paired Go oracle vs Rust

Real, deterministic acceptance for the global `room.departure_timeout`
(Go `config.go:268` uint32 seconds, default 20; `room.go:789-793` `CloseIfEmpty`
uses `elapsed = now - LastLeftAt` and `timeout = DepartureTimeout` for a room that
has had participants join and leave — the participant-departure branch — closing
on `RoomCloseReasonIdleTimeout`; runs every second). Fixed Go `v1.13.10`
(SHA `4bc2378e`) vs current Rust candidate `livecloud` (SHA `8ad74904`).

`run-paired.py --go <pinned livekit-server> --rust <livecloud> --output <new-owned-dir>`
starts, per profile, one fresh Go server (port 18600) and one fresh Rust server
(port 18610) sharing an identical YAML overlay (only HTTP/media ports differ),
rejects occupied ports, waits for real HTTP readiness, runs the same unchanged
node raw-JWT consumer, then SIGINT+waits both servers and requires exit 0. A
failed profile is retained, never counted partial.

`run.mjs` (env `REFERENCE_WS_URL`, `LIVEKIT_WS_URL`, `DT_PROFILE`, `DT_WINDOW`,
`DT_EXPECT_DEPARTURE_TIMEOUT`, `DT_OUTPUT`) creates a room that inherits the
global timeouts, joins one peer over the **real authenticated `/rtc` WebSocket
signaling** (reusing the untouched `../signed-name-limits/raw-protocol.mjs`
reader), asserts `ListParticipants==1`, closes the peer (departure), polls
`ListRooms` until the room disappears or the window elapses, and deep-equals the
normalized Rust observation `{reported_departure_timeout, reported_empty_timeout,
closed_after_departure}` against Go.

`room.empty_timeout` is pinned to 600 in both profiles so the departure branch
(not empty-from-creation) is what could fire.

Profiles:
- `dt-short` (`departure_timeout: 2`, 12s window): the room auto-closes after the
  sole peer departs.
- `dt-long` (`departure_timeout: 600`, 6s window): the room persists (control).

Cross-profile control: the pinned Go oracle closes dt-short and persists dt-long,
so a config a server ignores could never masquerade as agreement; Rust reproduces
both byte-for-byte. Aggregate status `PASS_CONFIGURED_DEPARTURE_TIMEOUT_EFFECT`.

Scope: the participant-departure auto-close branch only. Not the CLI per-key
startup flag, ENV override, `room_configurations.{key}.departuretimeout` preset,
dependent/agent exclusion in the last-left computation, reconnect-window
semantics, or byte-exact close latency (compared as a boolean within a bounded
window) — those stay separate ledger leaves. No browser, no media/RTP, no SDK
mutation, no webhook, no global/100% claim.
