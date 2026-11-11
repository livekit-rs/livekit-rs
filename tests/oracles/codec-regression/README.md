# Original LiveKit codec regression observers

Fixed source: v1.13.10 / e5655ac20267fa5005dccaf3f0e3bfc3ea688c07. This observer stages the complete original source unchanged; only local module replacements and two added `_test.go` fixtures are added in the new stage. Original service/rtc algorithms and tests are not rewritten or replaced. `original-inputs.json` binds the consumer sources, original go.mod and go.sum; `STAGED.json` records observer and module-input hashes. No reference checkout is edited.

Stage (new absolute output path required):

```
/usr/bin/python3 tests/oracles/codec-regression/prepare.py --root /absolute/repository --out /absolute/new-stage
```

Coordinator execution inside `<stage>/source`, persisting both stdout and actual exit codes:

```
GOTOOLCHAIN=go1.26.3 GOFLAGS=-mod=mod go test ./pkg/service -run '^TestOriginalLiveKitCodecRoomGate$' -count=1 -v
GOTOOLCHAIN=go1.26.3 GOFLAGS=-mod=mod go test ./pkg/rtc -run '^TestOriginalLiveKitCodecRegressionPolicy$' -count=1 -v
```

On a host with no Docker daemon, add the upstream `-docker=false` flag to the `./pkg/service` command. `pkg/service/docker_test.go` defines that flag for exactly this case and its `TestMain` otherwise calls `log.Fatalf` before any test runs. `TestOriginalLiveKitCodecRoomGate` never calls `requireDocker`, so the observer still executes in full and still produces all 21 rows; only unrelated daemon-dependent tests are skipped. Record which invocation was used, and never treat a skipped observer as a pass.

The first fixture creates one original `rtc.Room` and joins six original `NewMockParticipant`s via its actual owner `Join`; the original `roomManagerParticipantHelper.ShouldRegressCodec` produces 21 count/threshold results. The second constructs original `MediaTrack` for each policy and both helper decisions and calls its actual `enableRegression` method, producing 6 rows. All rows have `SOURCE_CODEC_GATE ` prefix. Room/track cleanup uses their original Close methods.

After both Go commands succeed, combine their complete logs without removing failures and run the native supplemental ignored reader with `GO_CODEC_GATE_RESULTS` pointing to that combined file:

```
cargo test -p rtc-media --lib codec_receivers::original_go_observer::original_livekit_room_and_track_policy_results -- --exact --ignored
```

This proves original consumer policy decisions, not media interoperability. The separate nonignored dual-codec SRTP and backup-only-subscriber tests must also execute. These fixtures and reader have not yet been executed when added.

## Original receiver/buffer window, Invalid and generated-NACK controls

New stage with `--with-window-controls` adds three further fixtures under build tag `source_codec_observer`. Two tiny observers invoke the existing original `ConnectionStats.getStat()` through the real `WebRTCReceiver`, without replacing its delta algorithm, clock, ticker or shutdown worker. They let the test place deterministic snapshot barriers; native SRTP tests separately exercise actual five-second deadlines.

```
/usr/bin/python3 tests/oracles/codec-regression/prepare.py --with-window-controls --root /absolute/repository --out /absolute/new-window-stage
# In <stage>/source:
GOTOOLCHAIN=go1.26.3 GOFLAGS=-mod=mod go test -tags=source_codec_observer ./pkg/rtc -run '^TestOriginalLiveKitCodec(DeltaWindowAndClose|InvalidAndGeneratedNack)$' -count=1 -v
```

The fixture creates original MediaTrack/AddReceiver/WebRTCReceiver/Buffer objects using real Pion RTPReceiver registered codec configuration, writes actual marshaled RTP to original buffers, and records the actual telemetry listener callback. It verifies a discarded priority2 window cannot reappear, the current priority2 window written before target arrival is accepted after the flag opens, and Close flushes the remaining deltas. Another test writes an in-order new PT on the same original buffer/SSRC, waits for the original asynchronous Invalid→backup callback, and verifies targetReceived remains false. An actual missing RTP sequence exercises original GetNACKPairs accounting without any RTCP socket emission. No original library algorithm is replaced; the local metadata TrackRemote supplies an original accepted receiver boundary, and these controls do not claim encrypted Go interoperability.

Five `SOURCE_CODEC_WINDOW` rows are expected. After the original Go command succeeds, run the supplemental native receipt reader with `GO_CODEC_WINDOW_RESULTS`:

```
cargo test -p rtc-media --lib receiver_traffic::original_go_window_controls::original_go_window_invalid_and_generated_nack_receipts -- --exact --ignored
```

Required native nonignored tests remain the actual three-codec encrypted SRTP window/retirement ledger, generated-but-unemitted NACK, and same-MID PT-change backup forwarding tests. Added controls are initially NOT_RUN; preserve real failures and source identity receipts.

Original window attempt01 exited0 with NO_TESTS because the initial staged name ended in `_windows_test.go` (GOOS filtering). The staging destination is corrected to `source_codec_window_observer_test.go`; preserve that attempt and require both named tests and five rows on the new stage.
