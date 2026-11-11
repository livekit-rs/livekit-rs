# Configured `room.max_room_name_length` / `room.max_participant_identity_length` LEGACY ALIAS keys — paired Go oracle vs Rust

Real, deterministic acceptance for the two `room.*` legacy alias keys that Go
copies into `Limit.*` when non-zero (`config.go:743-747`; Rust mirrors at
`load.rs:650-654`). The alias feeds the same `limitConf` that enforces
`CreateRoom` room-name length (`roomservice.go` `CheckRoomNameLength`, error
`room name length exceeds limits: max length %d`) and `/rtc/validate` participant
identity length (`participant identity length exceeds limits: max length %d`).
Fixed Go `v1.13.10` (SHA `4bc2378e`) vs current Rust candidate `livecloud`
(SHA `8ad74904`).

`run-paired.py --go <pinned livekit-server> --rust <livecloud> --output <new-owned-dir>`
starts, per profile, one fresh Go server (port 18800) and one fresh Rust server
(port 18810) sharing an identical YAML overlay (only HTTP/media ports differ),
rejects occupied ports, waits for real HTTP readiness, runs the same unchanged
node raw-JWT HTTP consumer, then SIGINT+waits both servers and requires exit 0. A
failed profile is retained, never counted partial.

`run.mjs` (env `REFERENCE_URL`, `LIVEKIT_URL`, `NA_PROFILE`, `NA_EXPECT_LIMIT`,
`NA_OUTPUT`) exercises the affected endpoint across the length boundary — room
names / identities of length 1..limit+1 — and deep-equals the normalized Rust
observation against the pinned Go oracle byte-for-byte. Go checks `len()` (JS
string length), so ASCII inputs here are byte-identical to character count.

Profiles (each sets exactly ONE alias key to `3`):
- `name-alias` (`room.max_room_name_length: 3`): `CreateRoom` accepts names of
  length 1/2/3 and rejects length 4 with `room name length exceeds limits` +
  `max length 3`.
- `identity-alias` (`room.max_participant_identity_length: 3`): `/rtc/validate`
  accepts identities of length 1/3 and rejects length 4/5 with `participant
  identity length exceeds limits` + `max length 3`.

Config-effect control: the pinned oracle shows BOTH an accepted (length<=limit)
and a rejected (length>limit, with the alias value in the body) case per profile;
a server ignoring the alias and using the default limit could not reproduce that
boundary. Rust reproduces both profiles byte-for-byte. Aggregate status
`PASS_CONFIGURED_NAME_IDENTITY_ALIAS_EFFECT`.

Scope: the two `room.*` legacy ALIAS keys and their copy-into-`Limit.*` effect on
`CreateRoom` / `/rtc/validate` only. Not the `limit.max_room_name_length` /
`limit.max_participant_identity_length` canonical keys themselves, the CLI per-key
startup flag, ENV override, the length-encoding for non-ASCII (`len()` vs UTF-8
bytes), WS signaling join, media or webhook — those stay separate ledger leaves.
No browser, no media/RTP, no SDK mutation, no global/100% claim.
