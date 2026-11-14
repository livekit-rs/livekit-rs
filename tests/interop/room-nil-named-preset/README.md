# `room-nil-named-preset` — paired observation for `CONFIG:NilNamedPresetPointer`

An **observation** harness (not a parity claim). It boots one fresh locked-Go server and
one fresh Rust candidate sharing a YAML whose `room.room_configurations` contains a **NIL**
entry (`nilpreset: ~`), a VALID entry (`validpreset`) and no `absent-preset` key, then drives
the unchanged public `RoomService.CreateRoom` consumer against both sides for three cases.

## What it records

| case | request | Go oracle | Rust candidate |
|---|---|---|---|
| `unknown-preset` | `room_preset='absent-preset'` | 400 `unknown room configuration in create room request` | 400 identical (byte-parity control) |
| `valid-preset` | `room_preset='validpreset'` | 200, inherits `empty_timeout 30 / departure_timeout 20 / max_participants 3 / metadata 'valid-preset-md'` | 200 identical (byte-parity control) |
| `nil-preset` | `room_preset='nilpreset'` | **nil-deref → SIGSEGV, process exit 2, connection aborted** | **clean 500 internal `Internal service panic`, process exit 0** |

The two byte-equal controls prove the `room_configurations` map is genuinely consumed on
both sides — the nil case is a real dereference path, **not an ignored config**.

## The upstream crash (recorded, not invented)

Go `roomallocator.go:206-250 applyNamedRoomConfiguration`:
`:211 conf, ok := RoomConfigurations[req.RoomPreset]` returns `conf=nil, ok=true` for a nil
entry (the key exists); `:212 if !ok` is false; `:219 clone.EmptyTimeout = conf.EmptyTimeout`
dereferences nil → SIGSEGV. The psrpc `CreateRoom` chain installs no recovery middleware, so
the panic is unrecovered: the process exits 2 and the connection aborts
(`SocketError: other side closed`).

Rust `manager.rs:672-674` (the standalone local-owner path MAIN uses in `--dev`/no-Redis):
`preset.as_ref().ok_or_else(|| ApiError::Internal("Internal service panic".into()))?` returns
a clean structured 500 and the process survives. The distributed `catalog.rs:735 PresetNil`
is a **separate anchor** not exercised by this standalone harness.

## R03 decision

The nil case is a **recorded divergence**, not a parity claim. Keep Rust's clean structured
internal error and process survival; do **not** replicate Go's nil-deref crash. The harness
**records** Go's exit code and panic log verbatim (`go_exit` is not asserted — an unrecovered
panic may abort the connection or crash the process; either is the observable upstream
behavior). It **asserts** Rust survives (`rust_exit == 0`) and returns the clean 500.

## Run

```bash
/tmp/c09d-venv/bin/python3 tests/interop/room-nil-named-preset/run-paired.py \
  --go /tmp/c09d-livekit-server-go \
  --rust target/release/livecloud \
  --node node \
  --output <fresh-dir>
```

`run-paired.py` launches both servers (`--dev --config <yaml>`), waits for readiness, runs
`run.mjs` (Node ≥ 18, global `fetch`), then stops both and records exit codes. Ports: Go
19620 (media 19621/19622), Rust 19630 (media 19631/19632). Output: `execution.json`,
`nil-named-preset.pair.json`, `{go,rust}.yaml`, `{go,rust}.server.log`, `go.server.log.txt`
(verbatim panic stack), `runner.log`. Status `PASS_NIL_NAMED_PRESET_OBSERVED`. Failures are
retained; partials are never counted.

## Scope

Public `CreateRoom` named-preset nil pointer only. Not `sync_streams`/`agents` internal
fields, not the CLI per-key flag, not ENV, not WS signaling join, not media, not webhook,
not the distributed Redis router path.