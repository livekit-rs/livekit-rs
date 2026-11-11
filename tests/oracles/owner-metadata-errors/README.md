# Actual fixed-Go owner error oracle

This executable imports the three **actual** pinned `rtc/signalling` errors and the actual protocol `xtwirp.ToError`; no copies of error constants or hand-authored expected codes. Each JSONL row has id/code/msg and is compatible with the existing ignored Rust oracle test. The original server go.mod/go.sum are copied, keeping all versions and upstream replace directives; only the local test module and fixed local server/protocol replacements are added. No reference checkout is changed. Source hashes are verified before staging.

Coordinator only:

```sh
/usr/bin/python3 livekit-rs-server/tests/oracles/owner-metadata-errors/prepare.py --repository /Users/zoujiaqing/projects/livekit-rs --out /tmp/livekit-rs-f88-owner-go-module
cd /tmp/livekit-rs-f88-owner-go-module
GOTOOLCHAIN=go1.26.3 GOMAXPROCS=2 go run -mod=readonly . > /tmp/livekit-rs-f88-owner-go-results.jsonl
```

Then run the real Cargo ignored test `ordinary_owner_metadata_errors_keep_actual_go_unknown_origin` with `OWNER_METADATA_GO_RESULTS=/tmp/livekit-rs-f88-owner-go-results.jsonl`. Coordinator owns builds and cleanup. A module-resolution error is NOT a test verdict: preserve it and inspect fixed dependency paths rather than silently update versions.

## Scope

Exactly three ordinary error constructions and Twirp conversion. This does not execute `UpdateParticipant`, HTTP wrapping, PSRPC ownership/routing, real SDK delivery, or prove the runtime branch is reached. Public API prevalidation usually rejects the same limit before the owner; public nine-pair checks cannot substitute for owner tests. The isolated F88 RoomManager/RoomTask tests cover local actor consumption, not distributed PSRPC. Real owner SDK echo delivery remains pending.

Do not reuse these ordinary signalling errors to classify CreateRoom: its real `service/errors.go` error is typed InvalidArgument and is independently already corrected.

The coordinator has now executed all three Go conversions; the Rust owner consumer remains pending. See repository evidence f88-owner-go-oracle-actual.json. The frozen SOURCE status records preparation time and is not an execution claim.
