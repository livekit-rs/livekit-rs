# Configured metadata-size enforcement — paired Go oracle vs Rust

Real, deterministic HTTP-only acceptance for the `limit.max_metadata_size`
config byte budget and its `room.max_metadata_size` legacy alias (fixed Go
`config.go:740-741` copies a non-zero `room.max_metadata_size` into
`Limit.MaxMetadataSize`; Rust `load.rs:647-648` mirrors it).

`run-paired.py --go <pinned livekit-server> --rust <livecloud> --output <new-owned-dir>`
starts, per profile, one fresh Go server (port 17880) and one fresh Rust server
(port 17980) sharing an identical YAML overlay (only HTTP/media ports differ),
rejects occupied ports, waits for real HTTP readiness, runs the same unchanged
node raw-JWT HTTP RoomService consumer against both, then SIGINT+waits both
servers and requires exit 0. A failed profile is retained, never counted partial.

`run.mjs` (env `REFERENCE_URL`, `LIVEKIT_URL`, `EXPECT_BYTE_LIMIT`,
`META_PROFILE`, `META_OUTPUT`) exercises CreateRoom and UpdateRoomMetadata at
empty / within / exactly-limit / over-limit / UTF8-byte-exact boundaries and
deep-equals Rust observations against Go, with an independent sanity gate that
within/exact are accepted and over is rejected with `invalid_argument` on both
sides (so a disabled guard on both cannot masquerade as agreement).

Profiles: `room-alias` (`room.max_metadata_size: 8`), `limit-key`
(`limit.max_metadata_size: 8`). Aggregate status
`PASS_CONFIGURED_METADATA_ENFORCEMENT_BOTH_PROFILES`.

Scope: HTTP RoomService CreateRoom/UpdateRoomMetadata only. Not the participant
`updateMetadata`/AddTrack signaling enforcement, CLI per-key startup flag, ENV
override or webhook payload limits — those stay separate ledger leaves. No SDK
mutation, no media, no fake responder, no global/100% claim.
