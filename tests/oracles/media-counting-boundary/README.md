# media-counting-boundary — fixed-Go media counting boundary oracle

Paired differential for SPEC-0077 ledger item **[1066]
`RUNTIME:process_lifetime_source_telemetry`**. It pins the *source* half of that item's
question: for a physical RTP packet handed to the locked SFU counting objects, **which class
does the locked Go code file it into, over what window basis, and what does the SDK-visible
projection of that window look like** — and does the Rust reader agree row for row.

## What is compared

`boundary_observer_test.go` is staged into the locked server tree as
`pkg/sfu/connectionquality/source_media_counting_boundary_test.go`. It calls the untouched
`rtpstats.NewRTPStatsSender`/`RTPStatsSender.Update`/`NewSnapshotId`/`NewSenderSnapshotId`/
`DeltaInfoSender`/`GetPacketsSeenMinusPadding`, the untouched
`rtpstats.NewRTPStatsReceiver`/`Update`/`DeltaInfo`, the untouched
`connectionquality.toAnalyticsStream`/`toAnalyticsVideoLayer` and the untouched
`rtpstats.ReconcileRTPDeltaInfoWithRTX`, and prints the objects those calls actually returned —
one `SOURCE_MEDIA_BOUNDARY {json}` row per `(case, window)` pair, **18 cases / 31 rows**:

| field | meaning |
| --- | --- |
| `case` / `window` / `direction` / `is_rtx` | frozen row identity and which object produced it |
| `physical.sent_primary` / `sent_padding` / `sent_duplicate` / `sent_frames` / `logical_bytes` / `sn_span` | the stimulus *as handed over*, classified by the observer's own construction (`payload`/`padding`/`ooo`/`marker`) — not by Go |
| `sender_view` | `RTPDeltaInfo` from `DeltaInfoSender` (the feed-only sender path) |
| `receiver_view` | the same call's second return value (nil in every row: no RTCP report is fed) |
| `base_delta` | `RTPDeltaInfo` from `DeltaInfo` (the base snapshot path) |
| `stream_down` / `stream_down_unreconciled` / `stream_up` | `livekit.AnalyticsStream` from `toAnalyticsStream` |
| `layer_fold` | `livekit.AnalyticsVideoLayer` from `toAnalyticsVideoLayer` |
| `seen_minus_padding` | `GetPacketsSeenMinusPadding()` — a **lifetime** span, not a window count |
| `anchor_ext_highest_sn` / `ext_highest_sn` | the sequence numbers around the window, which is what makes the span arithmetic checkable |

The primary-path deltas of the reconcile row appear under `primary_*`/`rtx_*`/`reconciled_*` so
reconcile input and output stay comparable (the real function mutates its first argument, so the
observer copies each delta before printing it).

| case | rows | boundary it exercises |
| --- | --- | --- |
| `downstream_padding_only_before_start` | 2 | padding-only packet before the sender is initialised, then a real primary packet |
| `downstream_padding_inside_window` | 2 | primary → dropped-slot padding → primary inside one window |
| `downstream_sequence_gap` | 2 | a 3-sequence-number hole charged to `packetsLost` |
| `downstream_exact_duplicate` | 2 | the same SN twice (`gapSN == 0`) |
| `downstream_late_repair_in_window` | 2 | a late packet that repairs a hole instead of duplicating |
| `downstream_late_below_start` | 2 | a late packet **below** `extStartSN` |
| `downstream_late_beyond_cache` | 2 | a late packet older than the 4-slot metadata cache |
| `downstream_too_many_packets_expected` | 2 | a window spanning more than 65 536 sequence numbers |
| `downstream_zero_progress` | 2 | no packets between the two reads of one window |
| `downstream_feedback_counters` | 1 | `UpdateNack`/`UpdateNackProcessed`/`UpdatePliAndTime`/`UpdateFir` into both delta paths |
| `rtx_starts_on_padding_only` / `non_rtx_never_starts_on_padding` | 1 + 1 | the same padding-only stimulus on an `IsRTX` and a non-RTX sender |
| `downstream_padding_repairs_exceed_span` | 1 | out-of-order padding repairs for holes below the anchor, so padding > window span |
| `rtx_reconcile` | 1 | primary + RTX windows reconciled, then projected |
| `upstream_primary_and_padding`, `upstream_padding_only_first`, `upstream_gap_late_and_pre_start`, `upstream_duplicate` | 2 each | the receiver-side equivalents, including the lazily initialised anchor and the pre-start drop |

On the Rust side, `crates/rtc-media/src/regression/media_counting_boundary.rs` (ignored test
`original_media_counting_boundary_rows_match_locked_go_fold`) requires the exact 31-row set, then
checks each row against the rules below — as *decisions the locked source makes* and as
*arithmetic identities over the numbers the locked source printed*. It never re-implements a Go
algorithm.

## Measured locked-Go rules this table pins

* **Start guard.** A padding-only packet before initialisation returns before any state change
  unless the sender is RTX (`rtpstats_sender.go:476-480`). A never-initialised sender still reports
  a lifetime span of 1 from `GetPacketsSeenMinusPadding` (`rtpstats_base.go:459-466` with
  `rtpstats_base_lite.go:308-316`), which is why "no packet seen" is not the same as "window empty".
* **Window basis.** `packetsExpected = now.extStartSN − then.extStartSN`; a read stores
  `extHighestSN + 1` as the window start (`:1245`, base `:707`) while the id created after a packet
  stores `extHighestSN` itself (`:433-438` with `:454`, base `:311-324`), so the **first** read of a
  window covers `highest − anchor + 1` positions and the **second** read of the same id sees
  `packetsExpected == 0`. At zero expected the sender view is dropped outright (`:1072`) but the
  base path returns a **non-nil all-zero** delta (`rtpstats_base.go:504-510`) — the asymmetry the
  Rust window filter mirrors at `crates/rtc-media/src/receiver_traffic.rs:216`.
* **Over-long window.** Above `cNumSequenceNumbers = 65536` (`rtpstats_base_lite.go:33`) **both**
  paths return nil (`rtpstats_sender.go:1062`, `rtpstats_base.go:493-503`), and a nil delta
  projects no stream.
* **The two subtraction styles.** The sender view computes `Packets = expected − padding`
  **unguarded** in `uint32`, so padding above the span wraps (`rtpstats_sender.go:1096`); the base
  path has the `padding packets more than expected` guard, which zeroes **only** `Packets` and still
  books every padding packet (`rtpstats_base.go:517-533`), and the `DeltaInfo*` wrapper logs the
  error and returns that object anyway (`rtpstats_sender.go:1040-1044`,
  `rtpstats_receiver.go:896-900`). The reader asserts the wrapped value, the zeroed packet count
  *and* the surviving padding count in the same row.
* **Late packets.** Below-start moves `extStartSN` and re-anchors both snapshot kinds on the sender
  (`rtpstats_sender.go:523-535`); the receiver's wrap-around is built with `IsRestartAllowed: false`
  (`rtpstats_receiver.go:203` with `:490-523`), so the same packet is dropped outright there.
  `gapSN == 0` is a duplicate and never out-of-order on either side (`:559-567`, `:529-531`); a
  late packet older than the metadata cache lands in the duplicate series while still counting as
  out-of-order (`rtpstats_sender.go:559-571` with `:1317-1325`).
* **Projection.** `toAnalyticsStream` is nil-in/nil-out and otherwise mirrors 13 fields of the
  delta, with loss/rtt/jitter chosen by direction and `nackRepeated` **absent** from the SDK-visible
  series (`connectionstats.go:455-504`). `toAnalyticsVideoLayer` folds the three packet classes
  additively and drops the layer if any component is 0 (`:506-521`).
* **RTX reconciliation.** `ReconcileRTPDeltaInfoWithRTX` adds the RTX window into the primary
  duplicate and padding series and discounts loss by `rtx.Packets − rtx.PacketsLost −
  primary.NackRepeated`, never touching the primary packet/byte counts (`rtpstats_base.go:974-999`);
  the reader checks the delta, then the production-visible consequence — the RTX packets appear as
  `retransmit_packets` of the primary stream only after reconciliation.

## What is *not* claimed here

* **Nothing above `Update`.** No DTLS, no SRTP, no pacer, no down track, no RTCP: the packets are
  synthetic and handed straight to the counting objects, so these rows say nothing about whether a
  packet was authenticated or actually forwarded. The physical side lives elsewhere:
  `tests/oracles/pion-rtp-padding` pairs the locked-Go RTP padding *admission* table with the native
  header parser, and the real three-RID ICE/DTLS/SRTP session that carries padding telemetry and RTX
  output is `regression::node_padding_telemetry_matches_real_remote_feedback_rtx_srtp_output`
  (`crates/rtc-media/src/regression.rs:6075`).
* **The Prometheus series are deliberately not re-derived from these rows.** Feeding a Rust process
  accumulator the numbers Go just printed and comparing sums back can never fail, so it is not
  evidence. The direction-dependent fold (`pkg/telemetry/stats.go:83-92`: `Primary+Padding`, with
  upstream `Retransmit` folded into initial and downstream kept as its own series at `:103-108`) is
  pinned by the literal-value tests in `crates/livecloud/src/metrics.rs:298-320`, and the
  sampled-series differentials named in that file's comments are measured against the pinned
  official binary.
* **Byte-class exhaustiveness over the lifetime is verified but not asserted here.** Every
  non-dropped packet is booked into exactly one of `bytes`/`bytesPadding`/`bytesDuplicate`
  (`rtpstats_sender.go:642-660`, `rtpstats_receiver.go:597-612`), but each row of this table reads a
  snapshot id for the first time, so no row pins a lifetime byte total. That is a lifetime-consumer
  question, listed below.
* **The remaining [1066] levels stay open.** This oracle is component behaviour against the upstream
  source contract; `config_projection`, `main_server_consumer` and
  `official_differential_lifecycle` (the close-room process-lifetime consumer campaign) are not
  covered by it, and the ledger is not upgraded past the level this evidence supports.

## Run

From the repository root, with the pinned toolchain and a warm module cache (offline by design:
`GOTOOLCHAIN=local`, `GOPROXY=off`, private `GOCACHE` and `TMPDIR` inside the output directory):

```sh
python3 tests/oracles/media-counting-boundary/run.py --output "$(mktemp -d)"
```

`prepare.py` verifies the eight `SOURCE.json` `original_inputs` digests against
`references/livekit-stable` at commit `e5655ac20267fa5005dccaf3f0e3bfc3ea688c07`
(`pkg/sfu/rtpstats/rtpstats_base.go`, `rtpstats_base_lite.go`, `rtpstats_sender.go`,
`rtpstats_receiver.go`, `pkg/sfu/connectionquality/connectionstats.go`, `pkg/sfu/downtrack.go`,
`pkg/sfu/buffer/buffer_base.go`, `pkg/telemetry/stats.go`), stages a copy of that tree, adds the
observer and the filesystem `replace` directives for `github.com/livekit/protocol` →
`references/protocol-stable` and `github.com/livekit/mediatransportutil` →
`references/go-modules/mediatransportutil-f234b534b095`, and records `STAGED.json` as
`STAGED_NOT_RUN` with `algorithm_substitutions: []`.

`run.py` then runs

```sh
go test ./pkg/sfu/connectionquality/ -count=1 -v -run '^TestOriginalMediaCountingBoundary'
```

in that stage, keeps the actual rows, audits them structurally (the 31 `(case, window)` pairs
exactly once, every delta/stream/layer/physical field present and integral), writes
`boundary-rows.jsonl` and runs

```sh
cargo test --release --locked -p rtc-media \
  regression::media_counting_boundary::original_media_counting_boundary_rows_match_locked_go_fold \
  -- --ignored --exact
```

with `GO_MEDIA_COUNTING_BOUNDARY_RESULTS` pointed at the actual rows. Finally it re-runs that test
against `boundary-rows.mutant.jsonl`, where `downstream_padding_inside_window/1` has one primary
packet added to `sender_view.packets` — stimulus, padding series and `stream_down` untouched — and
requires the run to **fail**. That single byte of mutation breaks two independent identities at once
(the packets-plus-padding span no longer covers the window, and `stream_down.primary_packets` no
longer mirrors the delta it was built from), so a green report cannot come from a reader that only
parses.

The Rust reader is `#[ignore]`d and only meaningful through `run.py`; it is not part of
`tests/oracles/build-oracles.py`, which compiles the self-contained Go modules and does not stage the
locked tree.
