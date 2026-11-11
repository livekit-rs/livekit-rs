# Generated CLI flag `--room.enable_remote_unmute` parsed, applied and enforced — paired Go oracle vs Rust

Real, deterministic acceptance for the **generated CLI flag input path** of
`--room.enable_remote_unmute` (Go `config.go` `RoomConfig.EnableRemoteUnmute bool`
`yaml:"enable_remote_unmute"`, upstream default `false`). Go enforces the behavior at
the admin `RoomService/MutePublishedTrack` RPC: when `muted=false` (an unmute request)
and the leaf is false, the RPC returns `ErrRemoteUnmuteNoteEnabled` (`psrpc.FailedPrecondition`
→ Twirp HTTP 412, code `failed_precondition`, message `remote unmute not enabled`)
**before** touching the track (roommanager.go:875). When the leaf is true, the same
unmute request is allowed through to track resolution. Rust mirrors this on the same
ordering — the flag is parsed via `crates/lk-config/src/generated.rs` (`parse_value`
`Kind::Bool`) into `config.room.enable_remote_unmute`, propagated into
`RoomPolicy.enable_remote_unmute` (`crates/rtc-core/src/config.rs:85`), and enforced at
`crates/rtc-core/src/room/api.rs:92` after the live `get_participant` presence check.
Fixed Go `v1.13.10` (SHA `4bc2378e`) vs current Rust candidate `livecloud` (SHA `8ad74904`).

This is the **flag** input path on the **same enforcement method** as the sibling YAML
value leaf fixture `room-enable-remote-unmute` (which verified the YAML
`room.enable_remote_unmute` path in an earlier batch). The two together cover both input
paths for the admin unmute gate.

`run-paired.py --go <pinned livekit-server> --rust <livecloud> --output <new-owned-dir>`
starts, per profile, one fresh Go server (port 20400) and one fresh Rust server (port
20410) sharing an identical base YAML (which leaves `room.enable_remote_unmute` at the
upstream default false) plus the **same extra argv** carrying the `--room.enable_remote_unmute`
flag under test (only HTTP/media ports differ), rejects occupied ports, waits for real
HTTP readiness, runs the same unchanged node raw-JWT WS+HTTP consumer (`run.mjs` is
byte-identical to the `room-enable-remote-unmute` fixture's consumer — same real WS
join → ListParticipants poll → admin MutePublishedTrack), then SIGINT+waits both servers
and requires exit 0. A failed profile is retained, never counted partial.

Profiles (all Go == Rust on the unmute response):
- `flag-false` — flag `--room.enable_remote_unmute=false`: the admin unmute against the
  present peer returns **HTTP 412** with body
  `{"code":"failed_precondition","msg":"remote unmute not enabled"}` on both Go and
  Rust.
- `flag-true` — flag `--room.enable_remote_unmute=true`: the same admin unmute returns
  **HTTP 200** with body `{"track":{}}` on both Go and Rust (gate bypassed).
- `yaml-true-flag-false` — YAML `room.enable_remote_unmute: true` + flag
  `--room.enable_remote_unmute=false`: the same admin unmute returns **HTTP 412** on
  both Go and Rust, proving the flag overrides the YAML value.

Cross-profile control: the pinned Go oracle gives 412 (flag-false) vs 200 (flag-true)
for the identical unmute request, and 412 when the flag overrides YAML true — a server
ignoring argv (retaining the upstream default false) could never produce 200 on
`flag-true`, and a server that always allowed unmute could never produce 412 on
`flag-false` or `yaml-true-flag-false`. The flag is genuinely parsed and applied as a
boolean that reaches the unmute gate, and the flag genuinely overrides the YAML value.
Rust matches Go on both the status and the error body per profile. Aggregate status
`PASS_GENERATED_CLI_FLAG_ROOM_ENABLE_REMOTE_UNMUTE_EFFECT`.

Scope: the generated `--room.enable_remote_unmute` boolean flag parse → apply → admin
MutePublishedTrack unmute gate path only, for this one flag. Not the whole generated
flag catalog, not the ENV single-key `LIVEKIT_ROOM_ENABLE_REMOTE_UNMUTE` consumer, not
the `room.enable_remote_unmute` YAML value leaf itself (separate verified fixture
`room-enable-remote-unmute`), not the real published-track mute effect (requires real
media track publication), not media, RTP, WebRTC, TURN, WebTransport, Redis routing,
worker lifecycles or webhook delivery. No browser, no media/RTP, no SDK mutation, no
global/100% claim.
