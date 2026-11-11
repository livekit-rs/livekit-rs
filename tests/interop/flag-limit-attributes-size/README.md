# Generated integer CLI flag `--limit.max_attributes_size` parsed, applied and enforced — paired Go oracle vs Rust

Real, deterministic acceptance for the **integer flag input path** of the generated CLI
flag `--limit.max_attributes_size` (Go `config.go:382` `LimitConfig.MaxAttributesSize`
`yaml:"max_attributes_size"`, upstream default `64*1024` at `config.go:629`). The flag
catalog name and default are locked in the Rust fixture
`crates/lk-config/tests/fixtures/cli-verbose-generated-v1.13.10.txt`. Go enforces the
budget on `RoomService.CreateRoom` agent attributes at
`roomservice.go:96-98` (`limitConf.CheckAttributesSize(ad.Attributes)` →
`twirp.InvalidArgumentError("attribute size exceeds limits", strconv.Itoa(MaxAttributesSize))`,
with `config.go:423-432` summing `len(k)+len(v)` and treating `0` as unlimited). Rust
mirrors this at `crates/lk-compat/src/twirp/room_service.rs:226-238` `check_attributes`,
reading `state.config.limits.max_attributes_size`, produced through the generated flag
path in `crates/lk-config/src/generated.rs` (`parse_value` `Kind::Int`). Fixed Go
`v1.13.10` (SHA `4bc2378e`) vs current Rust candidate `livecloud` (SHA `8ad74904`).

This complements the `room-auto-create-cli-flag` fixture (a boolean flag) with an
**integer** flag whose parsed value reaches the enforcement message, and pairs with the
already-verified `limit.max_attributes_size` **YAML** leaf.

`run-paired.py --go <pinned livekit-server> --rust <livecloud> --output <new-owned-dir>`
starts, per profile, one fresh Go server (port 19600) and one fresh Rust server (port
19610) sharing an identical base YAML plus the **same extra argv** (only HTTP/media ports
differ), rejects occupied ports, waits for real HTTP readiness, runs the same unchanged
node raw-JWT HTTP consumer issuing `CreateRoom` with an agent `attributes` map, then
SIGINT+waits both servers and requires exit 0. A failed profile is retained, never
counted partial.

`run.mjs` (env `REFERENCE_URL`, `LIVEKIT_URL`, `AF_PROFILE`, `AF_OUTPUT`) issues two
`CreateRoom` calls per profile — `over` (attributes `{k: 40 chars}`, total 41 bytes) and
`under` (`{k: 5 chars}`, total 6 bytes) — with a `roomCreate/roomAdmin` token. Accepted
cases compare only the deterministic status + echoed room name (success bodies carry
server-generated sid/turn_password/creation_time and Go/Rust JSON key order); rejected
cases compare the deterministic Twirp error body `{code,msg,meta}` byte-for-byte. Rust is
deep-equalled against the pinned Go oracle per case.

Profiles (all Go == Rust):
- `limit-10` — flag `--limit.max_attributes_size=10`. `over` → **400** with msg
  `"attribute size exceeds limits 10"` (the flag value reaches enforcement) and
  `meta.argument`; `under` → **200**.
- `default` — no flag (upstream default 65536). `over` → **200**; `under` → **200**.

Cross-profile control: the SAME over-limit `CreateRoom` returns 400 under `limit-10` and
200 under `default`, so the integer flag is genuinely parsed and applied as a threshold —
not ignored, not a blanket reject (the `under` case still passes within `limit-10`), and
not YAML-only. The oracle rejection `msg` embeds the flag value `10`. Rust reproduces
every status and the exact error body. Aggregate status
`PASS_GENERATED_CLI_FLAG_LIMIT_MAX_ATTRIBUTES_SIZE_EFFECT`.

Scope: the generated `--limit.max_attributes_size` integer flag parse → apply → agent
attributes size enforcement path only, for this one flag. Not the whole generated-flag
catalog, not the ENV single-key `LIVEKIT_LIMIT_MAX_ATTRIBUTES_SIZE` consumer, not the
`UpdateParticipant` attributes gate (a distinct code path with a different internal
message), not the metadata/agent-dispatch side effects, not WS signaling join, media, or
webhook. No browser, no media/RTP, no SDK mutation, no global/100% claim.
