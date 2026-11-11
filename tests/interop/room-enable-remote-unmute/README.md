# YAML value leaf `room.enable_remote_unmute` parsed, applied and enforced — paired Go oracle vs Rust

Real, deterministic acceptance for the **YAML value leaf input path** of
`room.enable_remote_unmute` (Go `config.go` `RoomConfig.EnableRemoteUnmute bool`
`yaml:"enable_remote_unmute"`, upstream default `false`). Go enforces the behavior at
the admin `RoomService/MutePublishedTrack` RPC: when `muted=false` (an unmute request)
and the leaf is false, the RPC returns `ErrRemoteUnmuteNoteEnabled` (`psrpc.FailedPrecondition`
→ Twirp HTTP 412, code `failed_precondition`, message `remote unmute not enabled`)
**before** touching the track (roommanager.go:875). When the leaf is true, the same
unmute request is allowed through to track resolution. Rust mirrors this on the same
ordering — the YAML is parsed into `config.room.enable_remote_unmute`
(`crates/lk-config/src/official.rs:686`), propagated into `RoomPolicy.enable_remote_unmute`
(`crates/rtc-core/src/config.rs:85`), and enforced at `crates/rtc-core/src/room/api.rs:92`
after the live `get_participant` presence check. Fixed Go `v1.13.10` (SHA `4bc2378e`)
vs current Rust candidate `livecloud` (SHA `8ad74904`).

`run-paired.py --go <pinned livekit-server> --rust <livecloud> --output <new-owned-dir>`
starts, per profile, one fresh Go server (port 20300) and one fresh Rust server (port
20310) sharing an identical base YAML (which sets `room.enable_remote_unmute` to the
profile value; **no extra argv** — only HTTP/media ports differ), rejects occupied ports,
waits for real HTTP readiness, runs the same unchanged node raw-JWT WS+HTTP consumer
(`run.mjs`: hold one peer joined on real `/rtc` WS signaling, poll `ListParticipants`
until present, then admin `MutePublishedTrack` with `muted=false` on an unknown track
sid), then SIGINT+waits both servers and requires exit 0. A failed profile is retained,
never counted partial.

Profiles (both Go == Rust byte-for-byte on the unmute response):
- `eru-false` — YAML `room.enable_remote_unmute: false`: the admin unmute against the
  present peer returns **HTTP 412** with body
  `{"code":"failed_precondition","msg":"remote unmute not enabled"}` on both Go and
  Rust. The participant is genuinely present (confirmed by `ListParticipants`), so the
  response is the gate decision, not a present-vs-absent artifact.
- `eru-true` — YAML `room.enable_remote_unmute: true`: the same admin unmute against
  the present peer returns **HTTP 200** with body `{"track":{}}` on both Go and Rust
  (the gate is bypassed; the unknown track sid returns `Ok(None)` → empty track).

Cross-profile control: the pinned Go oracle gives 412 (false) vs 200 (true) for the
identical unmute request — a server ignoring the YAML (retaining the upstream default
false) could never produce 200 on `eru-true`, and a server that always allowed unmute
could never produce 412 on `eru-false`. The YAML leaf is genuinely parsed and applied as
a boolean that reaches the unmute gate. Rust matches Go on both the status and the
error body per profile. Aggregate status
`PASS_YAML_ROOM_ENABLE_REMOTE_UNMUTE_EFFECT`.

Scope: the YAML value leaf `room.enable_remote_unmute` parse → apply → admin
MutePublishedTrack unmute gate path only, for this one leaf. Not the whole room YAML
catalog, not the ENV single-key `LIVEKIT_ROOM_ENABLE_REMOTE_UNMUTE` consumer, not the
generated CLI flag `--room.enable_remote_unmute` (separate input path, sibling fixture),
not the real published-track mute effect (requires real media track publication), not
media, RTP, WebRTC, TURN, WebTransport, Redis routing, worker lifecycles or webhook
delivery. No browser, no media/RTP, no SDK mutation, no global/100% claim.
