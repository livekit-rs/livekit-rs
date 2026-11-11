# Generated integer CLI flag `--limit.max_metadata_size` parsed, applied and enforced — paired Go oracle vs Rust

Real, deterministic acceptance for the **integer flag input path** of the generated CLI
flag `--limit.max_metadata_size` (Go `config.go:380` `LimitConfig.MaxMetadataSize`
`yaml:"max_metadata_size"`, upstream default `512*1024` at `config.go:628`, `0` =
unlimited). Go enforces the budget on `RoomService.CreateRoom` top-level metadata at
`roomservice.go:88` (`limitConf.CheckMetadataSize(req.Metadata)` →
`twirp.InvalidArgumentError("metadata size exceeds limits", strconv.Itoa(MaxMetadataSize))`,
with `config.go:420` rejecting `len(metadata) > MaxMetadataSize`). Rust mirrors this at
`crates/lk-compat/src/twirp/room_service.rs:214-222` `check_metadata`, reading
`state.config.limits.max_metadata_size`, produced through the generated flag path in
`crates/lk-config/src/generated.rs` (`parse_value` `Kind::Int`). Fixed Go `v1.13.10`
(SHA `4bc2378e`) vs current Rust candidate `livecloud` (SHA `8ad74904`).

This is the **flag** input path on a **different enforcement method** than the sibling
`flag-limit-attributes-size` fixture (CreateRoom top-level `metadata` vs agent
`attributes`), and pairs with the already-verified `limit.max_metadata_size` /
`room.max_metadata_size` **YAML** leaves.

`run-paired.py --go <pinned livekit-server> --rust <livecloud> --output <new-owned-dir>`
starts, per profile, one fresh Go server (port 19900) and one fresh Rust server (port
19910) sharing an identical base YAML plus the **same extra argv** (only HTTP/media ports
differ), rejects occupied ports, waits for real HTTP readiness, runs the same unchanged
node raw-JWT HTTP consumer issuing `CreateRoom`, then SIGINT+waits both servers and
requires exit 0. A failed profile is retained, never counted partial.

`run.mjs` (env `REFERENCE_URL`, `LIVEKIT_URL`, `AMD_PROFILE`, `AMD_OUTPUT`) issues two
`CreateRoom` calls per profile — `over` (metadata `m`*40 = 40 bytes) and `under`
(`m`*5 = 5 bytes) — with a `roomCreate/roomAdmin` token. Accepted cases compare only the
deterministic status + echoed room name (success bodies carry server-generated
sid/turn_password/creation_time and Go/Rust JSON key order); rejected cases compare the
deterministic Twirp error body `{code,msg,meta}` byte-for-byte. Rust is deep-equalled
against the pinned Go oracle per case.

Profiles (all Go == Rust):
- `metadata-10` — flag `--limit.max_metadata_size=10`. `over` → **400** with msg
  `"metadata size exceeds limits 10"` (the flag value reaches enforcement); `under` →
  **200**.
- `default` — no flag (upstream default 524288). `over` → **200**; `under` → **200**.

Cross-profile control: the SAME over-budget `CreateRoom` returns 400 under `metadata-10`
and 200 under `default`, so the integer flag is genuinely parsed and applied as a
threshold — not ignored, not a blanket reject (the `under` case still passes within
`metadata-10`), and not YAML-only. The oracle rejection `msg` embeds the flag value `10`.
Rust reproduces every status and the exact error body. Aggregate status
`PASS_GENERATED_CLI_FLAG_LIMIT_MAX_METADATA_SIZE_EFFECT`.

Scope: the generated `--limit.max_metadata_size` integer flag parse → apply → CreateRoom
metadata-size enforcement path only, for this one flag. Not the whole generated-flag
catalog, not the ENV single-key `LIVEKIT_LIMIT_MAX_METADATA_SIZE` consumer, not the
`UpdateRoomMetadata`/`UpdateParticipant` metadata gates (distinct code paths), not the
`limit.max_metadata_size` YAML value leaf itself (separate verified fixture), not WS
signaling join, media, or webhook. No browser, no media/RTP, no SDK mutation, no
global/100% claim.
