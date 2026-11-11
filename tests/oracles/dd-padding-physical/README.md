# Original Go DD padding / dropped-slot physical observer

> The absolute paths below are the recorded batch 22e run on the measuring host. Substitute your own
> checkout for `/Users/zoujiaqing/projects/livekit-rs` and any writable directory for the
> `/private/tmp/...` staging paths; every driver takes them as flags.

NEWLY AUTHORED oracle producer for `GO_DD_PADDING_RESULTS`. The source packet
plan, the coded bodies and the dependency-descriptor shape are transcribed from
the Rust consumer `crates/rtc-media/src/regression/dd_padding.rs:102-116`
(coded body), `:333-345` (source plan) and `:397-425` (per-send RTP header / DD /
payload construction). **No expected value is hand-authored**: every emitted
number is measured from real Go execution over a real pion DTLS/SRTP transport
on an in-memory `vnet`.

## What the observer actually runs

`physical_helpers_test.go` builds the real path, not a synthetic `ExtPacket`:

```
rtp.Packet -> buffer.NewBuffer(...).Bind(dd.ExtensionURI, codec params)
           -> Buffer.Write(raw)                       (real snRangeMap, real DD parser)
           -> reader goroutine: Buffer.ReadExtended()  (real ExtPacket)
           -> DownTrack.WriteRTP(ep, 0)                (real forwarder + RTP munger)
           -> pacer.NewPassThrough + null BWE
           -> real pion PeerConnection pair over vnettest.NewHosts
           -> far-side authenticated SRTP capture
```

`newDdPaddingSink` registers the primary payload type 98 and, for the
`alternate_pt` variants, a second distinct payload type 99 in the real
`MediaEngine`; it negotiates the real `dd.ExtensionURI` extension, reads back the
negotiated extension ID from the sender parameters, uses the pinned
`newFakeTrackReceiver` / `captureTrack` / `downTrackHarness` helpers unchanged,
waits for `IsWritableForTest` and pins the target with
`ForceForwardLayerForTest`. Sends are spaced 200 ms apart.

The DD bytes are produced by the real
`dd.FrameDependencyStructure` / `FrameDependencyTemplate` and the real
`DependencyDescriptorExtension.Marshal()`, then round-tripped through the real
`Unmarshal` and asserted. `output_dd` is decoded from the captured far-side
packet with the real `Unmarshal`, never copied from the input.

## Row contract (40 rows)

One group per filter. **`timing` is deliberately NOT a filter dimension** — the
Rust driver loops `timing` false/true over the same rows, so emitting a group per
timing value would double every count and fail.

| prefix (single trailing space) | filter keys | rows/filter | filters | total |
| --- | --- | --- | --- | --- |
| `SOURCE_DD_OOO_PADDING ` | `codec` | 6 | VP9, AV1 | 12 |
| `SOURCE_DD_OOO_PADDING_ALT_PT ` | `codec` | 6 | VP9, AV1 | 12 |
| `SOURCE_DD_INORDER_PADDING ` | `codec`, `gap` | 2 | 4 | 8 |
| `SOURCE_DD_INORDER_PADDING_ALT_PT ` | `codec`, `gap` | 2 | 4 | 8 |

Out-of-order rows are one per source stage 0..5, strictly ascending. In-order
rows are only the two accepted media stages (0 and 2): the original buffer drops
an in-order padding-only input inside `Buffer.Write`, so it never reaches the
DownTrack and is not an observation. `writes` is the literal `WriteRTP` return
value read with `as_u64()` on the Rust side.

A `SOURCE_DD_PADDING_CASE ` diagnostic line is also printed per case (receipt
counts, padding offset/size, `output_dd` flags). Its prefix does not match any
consumer prefix and is ignored by the Rust parser.

## Coordinator run commands

Stage (never executes Go, never modifies the references):

```sh
python3 /Users/zoujiaqing/projects/livekit-rs/tests/oracles/dd-padding-physical/prepare.py \
  --root /Users/zoujiaqing/projects/livekit-rs \
  --out  /private/tmp/ddpadding-stage
```

`--out` must not already exist. `prepare.py` verifies every `SOURCE.json` hash,
copies the whole pinned tree, drops the two helpers into `pkg/sfu` as
`source_dd_padding_*`, appends the two local `replace` directives and writes
`STAGED.json` with `status: STAGED_NOT_RUN`.

Run the observer inside the stage (offline toolchain, no network):

```sh
cd /private/tmp/ddpadding-stage/source
GOTOOLCHAIN=local GOPROXY=off GOWORK=off \
  "$HOME/go/pkg/mod/golang.org/toolchain@v0.0.1-go1.26.3.darwin-arm64/bin/go" \
  test -count=1 -run '^TestOriginalDdPaddingPhysicalOffsetsAndLatches$' \
  -v -timeout 20m ./pkg/sfu > /private/tmp/ddpadding-stage/source/padding.log 2>&1
```

Bind the actual log to the Rust consumer (prebuilt binary; no `cargo`):

```sh
GO_DD_PADDING_RESULTS=/private/tmp/ddpadding-stage/source/padding.log \
  /Users/zoujiaqing/projects/livekit-rs/target/debug/deps/rtc_media-8255c958dfdb0d70 \
  --ignored --exact \
  regression::dd_padding::original_go_dd_padding_physical_offsets_and_latches_match_encrypted_packets \
  --nocapture --test-threads=1
```

No rows, or no executed Go test, cannot pass: the consumer requires all 40 rows.

## Measured result (Go 1.26.3, darwin/arm64, commit e5655ac2)

`go test` exit 0, `ok github.com/livekit/livekit-server/pkg/sfu`, 12/12 subtests
PASS. Rust consumer exit 0,
`test result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 420 filtered out`.

* OOO VP9 `writes` `[1,1,0,0,1,1]`; OOO AV1 `[1,1,0,1,1,1]`.
* Output sequence offsets: OOO VP9 `[0,2,4,5]`, OOO AV1 `[0,2,3,4,5]`.
* In-order output sequences: `gap=false` `[1000,1001]`, `gap=true` `[1000,1002]`.
* Padding receipts: 1 for out-of-order (offset +1 from the first media receipt,
  empty payload), 0 for in-order.
* `output_dd`: out-of-order `[true,true,false,false(,false)]`, in-order
  `[true,true]`.

No Go/Rust divergence was found.

## Scope

Proves the original public `Buffer.Write` -> `ReadExtended` -> `DownTrack.WriteRTP`
admission and dropped-slot-padding decision, plus the far-side authenticated SRTP
sequence numbers, timestamps, coded bodies, markers and DD presence, for the
authored in-order / out-of-order and alternate-payload-type plans, using the
original DD writer/reader and the original VP9/AV1 layer selectors.

Does **not** prove:

* live server signalling, room/participant plumbing or SDK interoperability;
* real decoder pixel output — the coded bodies are tiny synthetic fixtures;
* congestion control or pacer timing (PassThrough pacer + null BWE);
* RTX / NACK repair, FEC, or bandwidth-probe padding;
* simulcast multi-SSRC layer allocation (a single source SSRC is used);
* the Rust-side timing histogram, which has no Go counterpart here;
* any dependency-descriptor structure other than the single authored
  one-decode-target / one-chain, four-template S0T0 structure;
* `UnpublishTrack` semantics beyond `DownTrack.CloseWithFlush`.

Do not close the parent DD/padding capability on this corpus alone.

## Files

* `physical_observer_test.go` — the authored plan, row emission and invariants.
* `physical_helpers_test.go` — real Buffer/DownTrack/vnet harness, real DD marshal.
* `prepare.py`, `SOURCE.json` — pinned-input hashing and isolated staging.
* `LICENSE` — upstream Apache-2.0 (LiveKit).