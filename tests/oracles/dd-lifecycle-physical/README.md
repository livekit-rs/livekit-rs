# Original Go DD disappearance lifecycle physical observer

> The absolute paths below are the recorded batch 22e run on the measuring host. Substitute your own
> checkout for `/Users/zoujiaqing/projects/livekit-rs` and any writable directory for the
> `/private/tmp/...` staging paths; every driver takes them as flags.

NEWLY AUTHORED oracle producer for `GO_DD_LIFECYCLE_RESULTS`. The malformed
pre-roll, the seven-send schedule, the DD attachment / keyframe pattern, the coded
bodies and the dependency-descriptor shape are transcribed from the Rust consumer
`crates/rtc-media/src/regression/dd_disappearance.rs:55-101` (DD structure),
`:124-138` (coded body) and `:300-385` (source plan and retired-source phase).
**No expected value is hand-authored**: every emitted value is measured from real
Go execution over a real pion DTLS/SRTP transport on an in-memory `vnet`.

## What the observer actually runs

`physical_helpers_test.go` builds the real path, not a synthetic `ExtPacket`:

```
rtp.Packet -> buffer.NewBuffer(...).Bind(dd.ExtensionURI, codec params)
           -> Buffer.Write(raw)                       (real DD parser, real snRangeMap)
           -> reader goroutine: Buffer.ReadExtended()  (real ExtPacket)
           -> DownTrack.WriteRTP(ep, 0)                (real forwarder + RTP munger)
           -> pacer.NewPassThrough + null BWE
           -> real pion PeerConnection pair over vnettest.NewHosts
           -> far-side authenticated SRTP capture
```

`newDdLifecycleSink` registers payload type 98, negotiates the real
`dd.ExtensionURI`, reads back the negotiated extension ID from the sender
parameters, reuses the pinned `newFakeTrackReceiver` / `captureTrack` /
`downTrackHarness` helpers unchanged, waits for `IsWritableForTest` and pins the
target with `ForceForwardLayerForTest`. Sends are spaced 200 ms apart.

The DD bytes are produced by the real `dd.FrameDependencyStructure` /
`FrameDependencyTemplate` and the real `DependencyDescriptorExtension.Marshal()`
(DTI `NotPresent=0`, `Discardable=1`, `Switch=2`, `Required=3`, verified against
the pinned reader), then round-tripped through the real `Unmarshal` and asserted.
`output_dd` is decoded from the captured far-side packet with the real
`Unmarshal`, never copied from the input.

## Source plan

1. **Pre-roll**: sequence 999, timestamp 27000, marker true, keyframe body, DD
   extension bytes `[0x80, 0x00]` (a *present but malformed* descriptor). The
   original buffer must reject it, so it can never reach a subscriber and can
   never be confused with "extension not found". Executed and asserted; **no row**.
2. **Seven media sends** `sent = 0..6`: sequence `1000+sent`, timestamp
   `3000*(10+sent)`, marker true, body `payload(codec, 10+sent, key)` with
   `key = (sent == 0 || (startup_missing && sent == 1))`. DD is attached iff
   `has_dd`: `startup_missing` → `sent == 4` only; otherwise `sent ∈ {0,1,4,6}`.
   Descriptor `raw_chain_descriptor(10+sent, 0, sent==0||sent==4||sent==6, true, true)`.
   At `sent == 6` the original DownTrack is closed with
   `CloseWithFlush(false, true)` **before** the write — the Go analogue of the
   Rust consumer's `UnpublishTrack`, which mutes the original forwarder. That
   send is executed and asserted (`writes == 0`, no new receipt); **no row**.
3. **Trigger stage**: `0` when `startup_missing`, else `2`.

## Row contract (24 rows) — and a RECOMMENDATION

One group per `(codec, startup_missing)` filter: 4 groups × 6 rows, exactly 5 rows
per group with `writes == 1`. **`timing` is deliberately NOT a filter dimension** —
the Rust driver loops `timing` false/true over the same rows, so a group per
timing value would double every count and fail.

**RECOMMENDATION (this is a choice, not a parser constraint):** the identity of
the single `writes == 0` row is not constrained by the Rust parser. This producer
emits **one row per source stage 0..5, with the trigger stage carrying
`writes == 0`**, and gives the malformed pre-roll and the retired send no row.
That keeps rows in ascending source-stage order and makes the dropped slot
explicit. A different six-row identity that still yields five accepted rows would
also satisfy the parser, but would be a different corpus — do not mix receipts
from different row identities.

Fields: `codec` (MIME, exactly `video/VP9` / `video/AV1`), `startup_missing`
(bool), `writes` (read with **`as_i64()`** on the Rust side), `sequence` (u64),
`timestamp` (u64), `output_dd` (bool), `payload_hex` (lowercase hex). Extra keys
(`stage`, `source_sequence`, `input_dd`, `trigger`, `marker`, `emitted`) are
tolerated and ignored by the parser.

**`sequence` is the OUTPUT (post-munger, far-side captured) RTP sequence number,
never the source sequence number.** The Rust consumer asserts output offsets
`0,1,2,3,4` relative to the first accepted receipt; the dropped trigger slot is
compacted exactly once by the real RTP munger.

A `SOURCE_DD_LIFECYCLE_CASE ` diagnostic line is also printed per case
(pre-roll rejection, receipt counts, retired writes). Its prefix does not match
the consumer prefix and is ignored by the Rust parser. Rows are emitted **before**
the behavioural invariants are asserted, so a genuine divergence still leaves the
measured rows in the log.

## Coordinator run commands

Stage (never executes Go, never modifies the references):

```sh
python3 /Users/zoujiaqing/projects/livekit-rs/tests/oracles/dd-lifecycle-physical/prepare.py \
  --root /Users/zoujiaqing/projects/livekit-rs \
  --out  /private/tmp/ddlife-stage
```

`--out` must not already exist. `prepare.py` verifies every `SOURCE.json` hash,
copies the whole pinned tree, drops the two helpers into `pkg/sfu` as
`source_dd_lifecycle_*`, appends the two local `replace` directives and writes
`STAGED.json` with `status: STAGED_NOT_RUN`.

Run the observer inside the stage (offline toolchain, no network):

```sh
cd /private/tmp/ddlife-stage/source
GOTOOLCHAIN=local GOPROXY=off GOWORK=off \
  "$HOME/go/pkg/mod/golang.org/toolchain@v0.0.1-go1.26.3.darwin-arm64/bin/go" \
  test -count=1 -run '^TestOriginalDdLifecyclePhysicalPayloadRecovery$' \
  -v -timeout 20m ./pkg/sfu > /private/tmp/ddlife-stage/source/lifecycle.log 2>&1
```

Bind the actual log to the Rust consumer (prebuilt binary; no `cargo`):

```sh
GO_DD_LIFECYCLE_RESULTS=/private/tmp/ddlife-stage/source/lifecycle.log \
  /Users/zoujiaqing/projects/livekit-rs/target/debug/deps/rtc_media-8255c958dfdb0d70 \
  --ignored --exact \
  regression::dd_disappearance::actual_original_dd_disappearance_physical_matches_payload_recovery \
  --nocapture --test-threads=1
```

No rows, or no executed Go test, cannot pass: the consumer requires all 24 rows.

## Measured result (Go 1.26.3, darwin/arm64, commit e5655ac2)

`go test` exit 0, `ok github.com/livekit/livekit-server/pkg/sfu`, 4/4 subtests
PASS. Rust consumer exit 0,
`test result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 420 filtered out`.

* Accepted source stages: `startup_missing=false` → `[0,1,3,4,5]`;
  `startup_missing=true` → `[1,2,3,4,5]`.
* `output_dd`: `[true,true,false,false,false]` when `startup_missing=false`;
  all `false` when `startup_missing=true` (i.e. `!startup_missing && stage < 2`).
* Output sequences `1000..1004`, contiguous offsets `0..4`; all markers true.
* Malformed pre-roll rejected in all four groups; padding receipts 0 (the
  contiguous trigger is a real dropped slot, not invented padding); retired
  `writes == 0` with no extra far-side receipt.

No Go/Rust divergence was found.

## Scope

Proves the original public `Buffer.Write` -> `ReadExtended` -> `DownTrack.WriteRTP`
lifecycle — malformed-DD rejection, DD-disappearance retirement, contiguous
dropped-slot compaction, payload recovery and retired publication — plus the
far-side authenticated SRTP output sequence, timestamp, coded body, marker and DD
presence for the authored plan, using the original DD writer/reader and the
original VP9/AV1 layer selectors.

Does **not** prove:

* live server signalling, room/participant plumbing or SDK interoperability;
* real decoder pixel output — the coded bodies are tiny synthetic fixtures;
* playout-delay extension policy (the Rust-only `(100, 200)` ms assertion has no
  Go counterpart in this corpus);
* congestion control or pacer timing (PassThrough pacer + null BWE), and the
  Rust-side forward-stats timing histogram;
* RTX / NACK repair or FEC;
* stream restart / re-publication after the retired send;
* simulcast multi-SSRC layer allocation (a single source SSRC is used);
* any dependency-descriptor structure other than the single authored
  one-decode-target / one-chain, four-template S0T0 structure;
* `UnpublishTrack` beyond the `DownTrack.CloseWithFlush` forwarder mute.

Do not close the parent DD-disappearance capability on this corpus alone.

## Files

* `physical_observer_test.go` — the authored plan, row emission and invariants.
* `physical_helpers_test.go` — real Buffer/DownTrack/vnet harness, real DD marshal.
* `prepare.py`, `SOURCE.json` — pinned-input hashing and isolated staging.
* `LICENSE` — upstream Apache-2.0 (LiveKit).