# YAML value leaf `limit.max_attributes_size` parsed, applied and enforced — paired Go oracle vs Rust

Real, deterministic acceptance for the **YAML input path** of the leaf
`limit.max_attributes_size` (Go `config.go:382` `LimitConfig.MaxAttributesSize`
`yaml:"max_attributes_size"` with parent `Limit LimitConfig yaml:"limit"` at
`config.go:109`; upstream default `64*1024` at `config.go:629`; `0` = unlimited).
Go enforces the byte budget on `RoomService.CreateRoom` agent attributes at
`roomservice.go:96-98` (`limitConf.CheckAttributesSize(ad.Attributes)` →
`twirp.InvalidArgumentError("attribute size exceeds limits", strconv.Itoa(MaxAttributesSize))`),
with `config.go:423-432` summing `len(k)+len(v)` and rejecting `> MaxAttributesSize`.
Rust mirrors this at `crates/lk-compat/src/twirp/room_service.rs:226-238`
`check_attributes`, reading `state.config.limits.max_attributes_size` produced from the
YAML `limit:` block parsed at `crates/lk-config/src/official.rs:1040`. Fixed Go
`v1.13.10` (SHA `4bc2378e`) vs current Rust candidate `livecloud` (SHA `8ad74904`).

This is the **YAML value leaf** input path on the **same enforcement method** as the
sibling `flag-limit-attributes-size` fixture (which verified the CLI-flag input path).
The two together cover both input paths for the CreateRoom agent-attributes gate.

`run-paired.py --go <pinned livekit-server> --rust <livecloud> --output <new-owned-dir>`
starts, per profile, one fresh Go server (port 20000) and one fresh Rust server (port
20010) sharing an identical base YAML **with the `limit.max_attributes_size` leaf set in
YAML**, and **no extra argv** (only HTTP/media ports differ), rejects occupied ports,
waits for real HTTP readiness, runs the same unchanged node raw-JWT HTTP consumer
issuing `CreateRoom`, then SIGINT+waits both servers and requires exit 0. A failed
profile is retained, never counted partial.

`run.mjs` (env `REFERENCE_URL`, `LIVEKIT_URL`, `YAML_AF_PROFILE`, `YAML_AF_OUTPUT`)
issues two `CreateRoom` calls per profile — `over` (attribute value `x`*40, key `"k"`,
total 41 bytes) and `under` (`x`*5, total 6 bytes) — with a `roomCreate/roomAdmin/roomList`
token. Accepted cases compare only the deterministic status + echoed room name (success
bodies carry server-generated sid/turn_password/creation_time and Go/Rust JSON key
order); rejected cases compare the deterministic Twirp error body `{code,msg,meta}`
byte-for-byte. Rust is deep-equalled against the pinned Go oracle per case.

Profiles (all Go == Rust):
- `yaml-10` — YAML `limit: {max_attributes_size: 10}`. `over` → **400** with msg
  `"attribute size exceeds limits 10"` (the YAML value reaches enforcement); `under` →
  **200**.
- `default` — no YAML `limit:` (upstream default 65536). `over` → **200**; `under` → **200**.

Cross-profile control: the SAME over-budget `CreateRoom` returns 400 under `yaml-10` and
200 under `default`, so the integer YAML value is genuinely parsed and applied as a
threshold — not ignored, not a blanket reject (the `under` case still passes within
`yaml-10`). The oracle rejection `msg` embeds the YAML value `10`. Rust reproduces every
status and the exact error body. Aggregate status
`PASS_YAML_LIMIT_MAX_ATTRIBUTES_SIZE_EFFECT`.

Scope: the YAML leaf `limit.max_attributes_size` parse → apply → CreateRoom
agent-attributes-size enforcement path only, for this one leaf. Not the CLI flag
`--limit.max_attributes_size` input path (separate verified fixture), not the ENV
single-key `LIVEKIT_LIMIT_MAX_ATTRIBUTES_SIZE` consumer, not the metadata/agents
dispatch side effects (separate verified fixtures), not the whole `limit` block or
every YAML key, not WS signaling join, media, or webhook. No browser, no media/RTP, no
SDK mutation, no global/100% claim.
