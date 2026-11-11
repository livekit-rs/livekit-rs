# Configured `room.enabled_codecs` list projected into the room's reported codecs — paired Go oracle vs Rust

Real, deterministic acceptance for the global `room.enabled_codecs` list (Go
`config.go:265` `[]CodecSpec`, YAML `enabled_codecs`; default 10-codec set at
`config.go:602-612`). Go `roomallocator.go:192-197` `applyDefaultRoomConfig` appends
each configured codec `{Mime, FmtpLine}` into the created room's `enabled_codecs`,
which the public `Room` response echoes verbatim, in order. Rust projects the same
list at `normalize.rs:575-582` (`RoomDefaults.enabled_codecs`, ordered) into
`rtc-core` `model.rs:373`. Fixed Go `v1.13.10` (SHA `4bc2378e`) vs current Rust
candidate `livecloud` (SHA `8ad74904`).

`run-paired.py --go <pinned livekit-server> --rust <livecloud> --output <new-owned-dir>`
starts, per profile, one fresh Go server (port 19100) and one fresh Rust server
(port 19110) sharing an identical YAML overlay (only HTTP/media ports differ),
rejects occupied ports, waits for real HTTP readiness, runs the same unchanged
node raw-JWT HTTP consumer, then SIGINT+waits both servers and requires exit 0. A
failed profile is retained, never counted partial.

`run.mjs` (env `REFERENCE_URL`, `LIVEKIT_URL`, `EC_PROFILE`, `EC_OUTPUT`) creates one
room and normalizes the public response to the ORDERED `[{mime, fmtp_line}]` array
(JSON key order is Go/Rust formatting noise; the array order and per-entry values are
the contract), then deep-equals the Rust observation against the pinned Go oracle.

Profiles:
- `custom` — `room.enabled_codecs = [audio/opus, video/VP8 (fmtp_line x=7), video/H264
  (fmtp_line profile=42e01f)]`. The room reports exactly these 3 codecs in order,
  `fmtp_line` values intact.
- `default` — `room.enabled_codecs` unset. The room reports the pinned default
  10-codec list (PCMU/PCMA/opus/red/VP8/H264/VP9/AV1/H265/rtx), all with empty
  `fmtp_line`.

Cross-profile control: the SAME `CreateRoom` request yields 3 codecs (custom) vs 10
(default) on the pinned oracle, so a server ignoring the config could not reproduce
the difference; Rust reproduces both profiles byte-for-byte. Aggregate status
`PASS_CONFIGURED_ENABLED_CODECS_EFFECT`.

Scope: the `room.enabled_codecs` list projection (`.mime` and `.fmtp_line`) into the
created room's reported codecs only. Not codec runtime negotiation with a publisher,
the media/SDP effect of a reduced codec set, the CLI per-key startup flag, ENV
override, `room_configurations.{key}` preset `enabled_codecs`, or webhook — those stay
separate ledger leaves. No browser, no media/RTP, no SDK mutation, no global/100%
claim.
