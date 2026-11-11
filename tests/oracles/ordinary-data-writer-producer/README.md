# Ordinary data writer Go oracle producer (ORDINARY_WRITER_RESULTS)

> The absolute paths below are the recorded batch 22e run on the measuring host. Substitute your own
> checkout for `/Users/zoujiaqing/projects/livekit-rs` and any writable directory for the
> `/private/tmp/...` staging paths; every driver takes them as flags.

Go producer for the Rust differential test
`ordinary_data_writer::tests::actual_go_ordinary_writer_all_twenty_results`
(`crates/rtc-media/src/ordinary_data_writer.rs`). It executes the REAL pinned
upstream `DataChannelWriter` (reliable and unreliable) plus `BitrateCalculator`
(BusyOnly) from `references/livekit-stable` @ `e5655ac20267fa5005dccaf3f0e3bfc3ea688c07`
and emits 20 JSONL observation rows (10 keys each, whole-object equality on the
Rust side). No expected values are hand-authored; every row comes from actual
Go execution.

## Corpus: COMMITTED / IMMUTABLE

`tests/oracles/ordinary-data-writer/cases.json` (12 cases, 20 calls) is a
pre-existing committed corpus. It is NOT part of this producer and must never
be edited. `prepare.py` hash-guards it (`corpus_sha256` in `SOURCE.json`) and
copies it verbatim into the fresh stage directory. Cases 11/12 carry an empty
third call `[]`: the writer must drop at admission before any raw write.

## What is staged

`prepare.py` copies exactly two upstream files into `<out>/source/`:
`pkg/sfu/datachannel/datachannel_writer.go` and `pkg/sfu/datachannel/bitrate.go`,
applying exactly two textual transforms: delete the
`github.com/livekit/protocol/utils/mono` import, and replace `mono.Now()` /
`time.Now()` with `sourceNow()` (the driver's explicit clock). No buffer,
branch, or arithmetic logic is changed. The driver is `package datachannel`
(in-package) so it can read the unexported `w.rate`. The upstream module/sums
and the pinned `references/protocol-stable` replacement are reused; the
upstream Apache-2.0 LICENSE is carried as `LICENSE-UPSTREAM`.

Driver behavior beyond the sibling `datachannel-writer` model:

- `NewDataChannelWriterReliable(d, d, slowThreshold)` for reliable cases
  (rate calculator exists only when `slowThreshold > 0`), and
  `NewDataChannelWriterUnreliable(d, d, targetLatency, 8*1024)` for lossy
  cases (minimum mirrors `pkg/rtc/transport.go:95 lossyDataChannelMinBufferedAmount`).
- A scripted action list per public `w.Write`: `writeReliable` loops internally
  and issues multiple raw `Write`+`SetWriteDeadline` attempts per public call;
  the fake advances an index inside `Write`.
- Clock: set to `base + carried_at_ns` before `w.Write` (deadline is set at the
  PREVIOUS completion instant), advanced to `base + action.at_ns` INSIDE the
  fake `Write` before returning. `base = time.Unix(1700000000, 0)` per case.
- `BufferedAmount()` returns the carried-in value at admission and
  `action.buffered` after each raw write. One writer instance per case is
  reused across calls, so the bitrate calculator accumulates. The deprecated
  MaxBuffered cap is disabled (max=0).
- Family mapping from the returned error: high-buffered drop -> `high`,
  `context.DeadlineExceeded` -> `deadline`, slow-reader drop -> `slow`,
  `io.ErrClosedPipe` -> `closed`, nil -> `ok`. `used` echoes consumed action
  objects verbatim (raw JSON); `deadlines_ns` records the actual
  `SetWriteDeadline` deltas (upstream `singleWriteTimeout` = 50ms).

## Coordinator run commands

```sh
cd /Users/zoujiaqing/projects/livekit-rs
/usr/bin/python3 tests/oracles/ordinary-data-writer-producer/prepare.py \
  --root "$PWD" --out /private/tmp/b22e-writer-stage
cd /private/tmp/b22e-writer-stage
GOTOOLCHAIN=local GOPROXY=off GOWORK=off \
  "$HOME/go/pkg/mod/golang.org/toolchain@v0.0.1-go1.26.3.darwin-arm64/bin/go" \
  run -mod=readonly . cases.json > results.jsonl
ORDINARY_WRITER_RESULTS=/private/tmp/b22e-writer-stage/results.jsonl \
  /Users/zoujiaqing/projects/livekit-rs/target/debug/deps/rtc_media-8255c958dfdb0d70 \
  --ignored --exact ordinary_data_writer::tests::actual_go_ordinary_writer_all_twenty_results \
  --nocapture --test-threads=1
```

`prepare.py` requires a fresh `--out` directory, verifies every manifest
sha256 before staging, never executes Go, and writes `STAGED.json`
(`STAGED_NOT_EXECUTED`). Expected: 20 rows, 12 unique ids; Rust log
`running 1 test` and `1 passed; 0 failed`, exit 0.

## Scope

Proven: the actual fixed upstream reliable/unreliable writer policy (admission,
retry, slow-reader drop, deadline, closed terminal) and BusyOnly bitrate
accumulation over the committed 20-call corpus, under an explicit scripted
clock and a controlled buffered-amount/raw-IO seam.

NOT proven: physical SCTP / pion datachannel transport behavior (the raw IO is
scripted, not a real association); the PCTransport-level deprecated MaxBuffered
cap (disabled here, max=0); concurrent-writer deadline/generation interactions
(single sequential caller per case); any negative-threshold/negative-target
invalid source domain (not in this corpus); whole-workspace acceptance. The
`SOURCE.json` status is the pre-execution manifest identity, not a live
acceptance flag; execution receipts are the measured `results.jsonl` and the
Rust consumer log retained under `/private/tmp` and reported to the coordinator.