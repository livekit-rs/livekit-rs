# Joining connection-callback sequence — real Go PCTransport observer

> The absolute paths below are the recorded batch 22e run on the measuring host. Substitute your own
> checkout for `/Users/zoujiaqing/projects/livekit-rs` and any writable directory for the
> `/private/tmp/...` staging paths; every driver takes them as flags.

Produces the 18 ordered JSONL rows (4 sequences) consumed by the Rust ignored
differential test
`regression::actual_go_joining_connection_callback_sequence_all_eighteen_stages`
(`crates/rtc-media/src/regression.rs:5148-5276`) via `JOINING_CALLBACK_GO_RESULTS`.

`stages.json` is TRANSCRIBED verbatim from the hardcoded sequence table in that
Rust test (regression.rs:5159-5194); it is the schedule input, not Go output.
Every row in `results.jsonl` is real execution output of the original pinned
Go server (`references/livekit-stable` @ `e5655ac20267fa5005dccaf3f0e3bfc3ea688c07`).

## Seam

The observer is dropped into `pkg/rtc` (in-package) and builds a bare
`PCTransport` with the original `NewPCTransport`, exactly as the original
`pkg/rtc/transport_test.go` does (`Config: &WebRTCConfig{}`,
`Handler: &transportfakes.FakeHandler{}` — the upstream counterfeiter fake whose
`CallCount` methods count real Handler invocations). No vnet pair is needed:

- connected stages drive the real registered PC callback
  `onPeerConnectionStateChange(webrtc.PeerConnectionStateConnected)`
  (transport.go:652/:884), which runs `clearConnTimer`,
  `setPeerConnectionConnectedAt` (:827), `Handler.OnInitialConnected` (:891)
  and `maybeNotifyFullyEstablished` (:893);
- `reset` runs the real `ResetShortConnOnICERestart()` (:2072) plus the real
  consumption `resetShortConnOnICERestart.CompareAndSwap(true, false)` →
  `resetShortConn()` exactly as transport.go:2970/:3060;
- channel-open stages set under `t.lock` the same `reliableDCOpened` /
  `lossyDCOpened` flags the real `onDataChannel` OnOpen sets (:953/:960, also
  exposed as `dcReady` pointers at :1296/:1301), then call the real
  `maybeNotifyFullyEstablished()` (:998);
- `eligible` is the real `isFullyEstablished()` (:1011) with both
  `UseOneShotSignallingMode` and `firstOfferNoDataChannel` false;
- `first`/`last` read the real `peerConnectionFirstConnectedAt` /
  `peerConnectionLastconnectedAt` timestamps.

Verified semantics worth noting: `resetShortConn()` zeroes only
`peerConnectionLastconnectedAt`, never `peerConnectionFirstConnectedAt`, so a
connected callback after a reset returns false from
`setPeerConnectionConnectedAt` and fires neither `OnInitialConnected` nor
`maybeNotifyFullyEstablished` (rows `again-connected` in the reset sequences
show `eligible=true` with `fully_callbacks=0` until a later channel-open
stage drains the notification). This matches the Rust mirror comment
"Source First is never cleared by resetShortConn; only Last is reset"
(`crates/rtc-media/src/transport.rs:403`).

The mandatory `source_functional_observer_init_test.go` shim initializes the
real prometheus collectors exactly as original `pkg/rtc/room_test.go:44`
(`prometheus.Init` is idempotent).

## Coordinator run commands

```sh
# 1. Stage (verifies every fixed-source.json sha256; requires a FRESH dir;
#    never executes Go; never modifies references; writes STAGED.json).
python3 tests/oracles/joining-callback-sequence/prepare.py \
  --root /Users/zoujiaqing/projects/livekit-rs \
  --out /private/tmp/b22e-callback-stage

# 2. Real Go observation (18 rows).
cd /private/tmp/b22e-callback-stage
JOINING_CALLBACK_STAGES=/private/tmp/b22e-callback-stage/joining-callback-stages.json \
JOINING_CALLBACK_GO_RESULTS=/private/tmp/b22e-callback-stage/results.jsonl \
GOTOOLCHAIN=local GOPROXY=off GOWORK=off \
"$HOME/go/pkg/mod/golang.org/toolchain@v0.0.1-go1.26.3.darwin-arm64/bin/go" \
  test -count=1 -run '^TestJoiningCallbackSequenceSource$' -v ./pkg/rtc

# 3. Rust differential consumer (prebuilt binary; no cargo).
JOINING_CALLBACK_GO_RESULTS=/private/tmp/b22e-callback-stage/results.jsonl \
/Users/zoujiaqing/projects/livekit-rs/target/debug/deps/rtc_media-8255c958dfdb0d70 \
  --ignored --exact regression::actual_go_joining_connection_callback_sequence_all_eighteen_stages \
  --nocapture --test-threads=1
```

Measured: Go test exit 0 (`--- PASS: TestJoiningCallbackSequenceSource`),
18 rows; Rust exit 0,
`test result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 420 filtered out`.
Hashes of the measured artifacts are recorded in `SOURCE.json`.

## Scope

Proven: the original Go `PCTransport` connection-state bookkeeping —
initial/fully-established callback firing and latching, short-connection reset
consumption, data-channel-open readiness flags, and the `isFullyEstablished`
predicate — across the 18 transcribed stages, and row-for-row agreement of the
Rust model with that real Go output.

NOT proven: no wire claim of any kind (no ICE connectivity checks, no DTLS
handshake, no SCTP association, no real detached DataChannel — the opened flags
are injected at the same seam the real OnOpen handler writes them); no live
server, room, signalling or SDK participation; no participant-level consumer
side effects (`PrimaryTransportHandler` → `onPrimaryTransportInitialConnected`
/ `onPrimaryTransportFullyEstablished` / `replayJoiningReliableMessages` are
not exercised — the upstream FakeHandler counts Handler invocations only); no
media, data-message delivery, negotiation or ICE-restart SDP flow (the reset
stage runs only the flag-consumption + `resetShortConn` portion of the real
restart paths). `fully_callbacks` is the raw cumulative Handler count, which
in these sequences never exceeds 1, matching the Rust latch.