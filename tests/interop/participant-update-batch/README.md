# C02 actual participant-update batch target

Status: new consumer and test source, not yet executed or accepted.

Fixed Go `pkg/rtc/room.go:1633,2309-2348` passes
`room.update_batch_target_size` to `ChunkProtoBatch`. The helper in
`pkg/rtc/utils.go` sums **ParticipantInfo proto.Size**, excluding the surrounding
ParticipantUpdate or SignalResponse framing. It cuts when the next sum is
strictly greater than the signed target; a single oversized participant stays
intact, zero is not an unlimited sentinel, negative values remain negative.

The new candidate preserves the signed YAML/CLI/ENV target into RoomPolicy.
The protocol-neutral core receives a ParticipantUpdateSizer supplied by the
compatibility adapter, which converts the actual model to the real generated
protobuf and uses its encoded_len. No protobuf dependency or approximate byte
estimator is introduced in the core. CompatServices assembles this provider
before any room is created. Batch flush combines queued participant snapshots,
chunks first, then applies the existing recipient readiness, hidden and version
filters. The room source also receives the batch, as fixed Go does, after its
immediate self update. Ordinary non-batch update paths remain unchanged.

Coordinator checks, with nonzero executed count and retained exit status:

```sh
cargo test -p lk-compat --lib participant_updates::tests -- --nocapture
cargo test -p rtc-core --lib room::tests
cargo test -p lk-config --test config_leaf_parsing participant_update_batch_signed_target_survives_yaml_cli_env_to_room_policy -- --exact
```

The compatibility tests join real participants into the core room and inspect
actual encoded SignalResponse chunks at signed, zero, exact-boundary and
single-oversize targets; check hidden participants and no stale repeat at the
next tick; and join through actual CompatServices/RoomManager/RoomTask to prove
assembly consumes the configured target instead of merely storing it.

The primary independent oracle directly calls the exported function from the
actual fixed `github.com/livekit/livekit-server/pkg/rtc` package. Run the external
probe file from the reference checkout's module, without modifying that checkout
or its Go inputs:

```sh
cd /Users/zoujiaqing/projects/livekit-rs/references/livekit-stable
GOWORK=off go run -mod=readonly \
  /Users/zoujiaqing/projects/livekit-rs/livekit-rs-server/tests/interop/participant-update-batch/oracle/main.go \
  --source /Users/zoujiaqing/projects/livekit-rs/references/livekit-stable/pkg/rtc/utils.go \
  < /Users/zoujiaqing/projects/livekit-rs/livekit-rs-server/tests/interop/participant-update-batch/vectors.json
```

It uses the official generated ParticipantInfo, actual proto.Size and actual
`rtc.ChunkProtoBatch`; its observation explicitly names that upstream consumer.
The original function SHA-256 is also checked before processing:
`b8bd3cf807a7202b969072a9da7b12c46824260a5db6b9d2537ed6d372992353`.
The older copy-based probe in `goclient/cmd/participantbatch` is only a secondary
source extraction/checkpoint and must not replace this actual upstream call.
The vectors include a zero-size message and metadata at protobuf length-varint
boundaries, in addition to signed/zero/oversized target cases. Retain stdout,
exit status, module/source identity and source digest in final evidence.

This change does not certify the entire C02 family. Synthesized disconnect /
identity-replacement batching and full fixed Go / SDK signal timing still
require distinct acceptance. All-filtered empty chunks are not a source gap:
`rtc/signalling/signalling.go:48-51` returns nil when no valid participants
remain. A core regression preserves the existing no-empty-frame behavior. An absent protocol adapter keeps a neutral engine's logical batch
intact; it does not fabricate a guessed protobuf size.
