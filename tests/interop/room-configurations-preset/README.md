# Configured `room.room_configurations.{key}` named presets applied to `CreateRoom` — paired Go oracle vs Rust

Real, deterministic acceptance for the global `room.room_configurations` named-preset
consumer (Go `roomallocator.go:206` `applyNamedRoomConfiguration`: when a
`CreateRoom` request carries `room_preset`, the matching preset fills **only the
fields the request left unset** — `EmptyTimeout`, `DepartureTimeout`,
`MaxParticipants`, `Metadata`, … — and an unknown preset name returns
`psrpc` `InvalidArgument` `unknown room configuration in create room request`; an
empty `room_preset` is treated as no preset). Rust mirrors it at `manager.rs:490-503`
(`presets.get(preset)` -> `ApiError::InvalidArgument` on miss, `RoomConfig::inherit`
in `permission.rs:162` fills unset fields). Fixed Go `v1.13.10` (SHA `4bc2378e`) vs
current Rust candidate `livecloud` (SHA `8ad74904`).

`run-paired.py --go <pinned livekit-server> --rust <livecloud> --output <new-owned-dir>`
starts, per profile, one fresh Go server (port 19000) and one fresh Rust server
(port 19010) sharing an identical YAML overlay (only HTTP/media ports differ),
rejects occupied ports, waits for real HTTP readiness, runs the same unchanged
node raw-JWT HTTP consumer, then SIGINT+waits both servers and requires exit 0. A
failed profile is retained, never counted partial.

`run.mjs` (env `REFERENCE_URL`, `LIVEKIT_URL`, `RC_PROFILE`, `RC_OUTPUT`) issues the
same six `CreateRoom` requests against each server and deep-equals the normalized
Rust observation against the pinned Go oracle. The public `Room` response echoes the
effective `empty_timeout` / `departure_timeout` / `max_participants` / `metadata`, so
those are compared exactly (server-generated `sid` / `turn_password` / timestamps and
JSON key order are normalized out); rejections are compared on the deterministic
`{code,msg}`.

Requests: `preset-applied` (`room_preset: presetA`), `no-preset-default` (no preset),
`request-override` (preset + explicit `empty_timeout: 5`), `unknown-preset`
(`preset: nosuch`), `empty-preset-name` (`preset: ""`), `request-wins-multi`
(preset + explicit `max_participants: 9` and `metadata: req-md`).

Profiles:
- `preset` — `room_configurations.presetA = {empty_timeout 41, departure_timeout 7,
  max_participants 3, metadata from-preset}`. The preset fills every unset field;
  explicit request fields win (only-when-unset); unnamed / empty-preset requests get
  the global defaults (300/20/0/""); the unknown preset 400s.
- `absent` — `room_configurations = {}`. Every `presetA` request 400s (the preset map
  is actually consumed), while unnamed requests still get the global defaults.

Cross-profile control: the SAME `preset-applied` request yields 200 with preset values
in `preset` and 400 in `absent` on the pinned oracle, so a server ignoring the preset
map could never reproduce the difference; Rust reproduces both profiles byte-for-byte.
Aggregate status `PASS_CONFIGURED_ROOM_PRESET_EFFECT`.

Scope: the named-preset application to the four HTTP-observable `CreateRoom` effective
fields (`empty_timeout`, `departure_timeout`, `max_participants`, `metadata`) only.
`sync_streams` and `agents` are `RoomConfiguration` preset fields but live in the
internal-only `RoomInternal`/dispatch paths (not the public `Room` response), so they
stay separate ledger leaves; likewise the CLI per-key startup flag, ENV override, the
`{key}` name-matching beyond the explicit `room_preset` request field, and the
"explicit 0 vs unset" boundary (Go treats request `0` as unset; only non-zero request
overrides are exercised here). No browser, no media/RTP, no SDK mutation, no webhook,
no global/100% claim.

## Redis room rehydration with named agent presets

`run-agent-rehydration.mjs` adds a separate Redis-backed comparison. Each
server gets an owned Redis and valid stored Room and RoomInternal messages
serialized by the locked official protocol package. The stored room has a
known SID and one configured agent, with no live owner. The probe sends
CreateRoom using a token minted by the official Server SDK and the named
preset field from the public protobuf request, then decodes the actual
RoomInternal bytes in Redis. The official Agent Dispatch SDK also lists the
live dispatches, including the unnamed default when the stored list is empty.
Five cases check absence of a preset,
omitted and empty agent lists in a named preset, populated preset lists,
and explicit request precedence. Every case checks the original room SID
and deletion of the durable room, internal configuration, and owner key.

```sh
node run-agent-rehydration.mjs /absolute/fixed-go-v1.13.10 \
  /absolute/rust-livekit-server /absolute/new-evidence-directory
```

Both real servers and both owned Redis processes must exit 0, and their
HTTP and Redis listeners must release. The test covers stored dispatch
configuration consumed by the allocator during room rehydration. It
does not exercise an external Agent worker or alter a live room's
dispatch jobs.
