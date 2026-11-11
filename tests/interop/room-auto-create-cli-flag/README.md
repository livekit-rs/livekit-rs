# Generated CLI flag `--room.auto_create` parsed, applied and precedence over YAML — paired Go oracle vs Rust

Real, deterministic acceptance for the **flag input path** and **flag-over-YAML
precedence** of the generated CLI flag `--room.auto_create` (Go `config.go:271`
`Room.AutoCreate` `yaml:"auto_create"`, default `true` at `config.go:608`; generated
flag catalog `--room.auto_create` `[$LIVEKIT_ROOM_AUTO_CREATE]` — the same flag name and
default are locked in the Rust fixture
`crates/lk-config/tests/fixtures/cli-verbose-generated-v1.13.10.txt:89`).

Go `roomallocator.go:56-57,179`: a join with `roomJoin` but no `roomCreate` grant is
rejected for a nonexistent room when `!Room.AutoCreate`. Rust mirrors the gate at
`crates/lk-compat/src/signal/mod.rs:158-161` (`may_create = state.config.room.auto_create
|| grants.video.room_create`; otherwise `404 requested room does not exist`). The flag is
parsed and stored by the generated flag path in `crates/lk-config/src/generated.rs`.
Fixed Go `v1.13.10` (SHA `4bc2378e`) vs current Rust candidate `livecloud` (SHA `8ad74904`).

This complements the `room-auto-create` fixture, which verified the **YAML value** effect;
here the value is supplied through the **CLI flag argv** and, in the third profile, the
flag deliberately **overrides** an opposite YAML value.

`run-paired.py --go <pinned livekit-server> --rust <livecloud> --output <new-owned-dir>`
starts, per profile, one fresh Go server (port 19300) and one fresh Rust server (port
19310) sharing an identical base YAML plus the **same extra argv** (only HTTP/media ports
differ), rejects occupied ports, waits for real HTTP readiness, runs the same unchanged
node raw-JWT HTTP consumer issuing `/rtc/validate` with a `roomJoin` token for a
nonexistent room, then SIGINT+waits both servers and requires exit 0. A failed profile is
retained, never counted partial.

`run.mjs` (env `REFERENCE_URL`, `LIVEKIT_URL`, `CA_PROFILE`, `CA_OUTPUT`) issues one
`/rtc/validate` (protocol 17, sdk js) for a fixed nonexistent room name — each server is
fresh per profile and `/rtc/validate` does not materialize the room, so both sides see it
as nonexistent — and deep-equals the Rust HTTP status against the pinned Go oracle. Only
the deterministic status is compared.

Profiles (all Go == Rust):
- `flag-false` — base YAML `auto_create` unset (default `true`), flag
  `--room.auto_create=false`. `/rtc/validate` → **404** (flag suppresses auto-create).
- `flag-true` — base YAML unset, flag `--room.auto_create=true`. → **200**.
- `yaml-true-flag-false` — YAML `room.auto_create: true`, flag
  `--room.auto_create=false`. → **404** (the flag wins over the YAML value).

Cross-profile control: the SAME `/rtc/validate` request returns 404 (flag false) vs 200
(flag true) on the pinned oracle, so the flag is genuinely parsed and applied — a server
ignoring argv could not reproduce the difference; and 404 when the flag overrides a YAML
`true`, proving flag-over-YAML precedence. Rust reproduces all three statuses. Aggregate
status `PASS_GENERATED_CLI_FLAG_ROOM_AUTO_CREATE_EFFECT`.

Scope: the generated `--room.auto_create` flag parse → apply → precedence path only, for
this one flag. Not the whole generated-flag catalog (`generated_flags:--*` per key), not
the ENV single-key consumer, not flag-vs-YAML for other fields, not the YAML value effect
itself (separate fixture), not WS signaling join, media, or webhook. No browser, no
media/RTP, no SDK mutation, no global/100% claim.
