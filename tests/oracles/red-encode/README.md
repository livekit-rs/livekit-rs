# red-encode — fixed-Go RED encoder oracle

Paired differential for SPEC-0077 item C68 (audio RED publisher side). It pins
one question: for a given sequence of forwarded Opus frames, what bytes does the
official Go SFU put on the wire — payload type, RFC 2198 header chain and block
contents — and does `vendor/str0m`'s `RedSender` produce exactly the same bytes?

## What is compared

`observer_test.go` is staged into the locked server tree as
`pkg/sfu/source_red_encode_observer_test.go` and calls the untouched
`RedReceiver.ForwardRTP` (`pkg/sfu/redreceiver.go`) once per frame of each frozen
vector, with one `dummyDowntrack` attached. Because `RedReceiver` reuses a single
outgoing packet, the row reads `dt.receivedPkts` immediately after the call, so
`out_payload_hex` is that call's actual egress. One
`SOURCE_RED_ENCODE {json}` row per frame:

| field | meaning |
| --- | --- |
| `case` / `index` | vector name and emission order within it |
| `seq` / `ts` / `payload_hex` / `marker` | the primary frame handed to `ForwardRTP` |
| `out_seq` / `out_ts` / `out_marker` | header fields of the forwarded packet |
| `out_pt` | `63` (`opusRedPT`) when RED wrapped, `111` (`opusPT`) when it did not |
| `out_payload_hex` | the complete encoded egress payload |

`generatePkts` and `dummyDowntrack` are the locked `redreceiver_test.go` helpers,
not reimplementations, and the vectors are that file's own subtests:

| vector | rows | what it exercises |
| --- | --- | --- |
| `normal` | 10 | window ramp to `maxRedCount = 2` |
| `packet_lost_and_jump` | 8 | even-index gaps, then a 10-seq / 10-step jump |
| `unorder_and_repeat` | 12 | an aged-out frame replayed, then the newest frame repeated |
| `encoding_exceed_space` | 5 | 1000-byte frames whose 3-block chain exceeds the 1500-byte buffer |
| `large_timestamp_gap` | 8 | 4 normal frames, then 4 frames stepping `40 × 480` |
| `oversized_frame` | 4 | 1024, 1023, 20, 20-byte frames across the `maxRedPayload` edge |

The observer first fails if the locked constants are not `maxRedCount == 2` and
`mtuSize == 1500`, which is what binds the Rust configuration
`cfg.set_red_distances(&[1, 2])` and the `GO_BUDGET = 1500` used in the
comparison.

On the Rust side, `vendor/str0m/src/packet/red.rs` runs a fresh
`RedSender::new(Pt(63), Pt(111), &[1, 2])` per vector — matching Go's fresh
`RedReceiver` per subtest — and asserts the payload type **and the whole encoded
payload byte-for-byte** for all 47 rows. `run.py` independently audits each actual
row structurally (lengths must cover the payload, block PT is always the fixed
Opus 111, the primary block must be the forwarded frame, every redundant block
must resolve to an earlier frame of the same case with exactly its timestamp gap,
a ≥ 1 KiB frame must be broadcast byte-identically on PT 111) so a green report
cannot come from both sides agreeing on something that is not RED.

## What is *not* claimed here

* This is the **encoder output only**, at one down track and below SRTP. The
  question of *whether* the SFU wraps at all — `audio.active_red_encoding`,
  `room.enabled_codecs`, the publisher's own RED capability, and encryption
  blocking the RED receiver — is the real ICE/DTLS/SRTP forwarding test
  `active_red_encoding_forwards_two_redundant_opus_levels_on_the_red_payload_type`
  and its `inactive_...` control in `crates/rtc-media/src/regression.rs`.
* Receiver-side recovery (splitting an RFC 2198 payload back into frames, the
  sticky consumer, dedup across redundant copies) is covered by the `RedDecoder`
  and `RedPrimary`-equivalent unit tests in this vendor crate and by
  `tests/oracles/audio-red-registration`, which deliberately left "actual
  RED-encoded packet production/FEC" open until this oracle.
* Go's `RedReceiver.ReadRTP` refuses NACK with `bucket.ErrPacketMismatch`; the
  Rust refusal lives in the RED-aware retransmit path and is not compared by byte
  equality here (an error value has no wire bytes to pair).
* The frames carry `Payload = header.Marshal()` like the upstream test, plus the
  `oversized_frame` vector's synthetic 1 KiB payloads. No real Opus codec is
  involved, so this says nothing about Opus frame sizes a given encoder emits.

## Run

From the repository root, with the pinned toolchain and a warm module cache
(offline by design: `GOTOOLCHAIN=local`, `GOPROXY=off`, private `GOCACHE` and
`TMPDIR` in the output directory):

```sh
python3 tests/oracles/red-encode/run.py --output "$(mktemp -d)"
```

`prepare.py` verifies the four `SOURCE.json` `original_inputs` digests against
`references/livekit-stable` at commit `e5655ac20267fa5005dccaf3f0e3bfc3ea688c07`
(`pkg/sfu/redreceiver.go`, `pkg/sfu/redreceiver_test.go`,
`pkg/sfu/receiver_base.go`, `go.mod`), stages a copy of that tree, adds the
observer and the filesystem `replace` directives for
`github.com/livekit/protocol` → `references/protocol-stable` and
`github.com/livekit/mediatransportutil`
→ `references/go-modules/mediatransportutil-f234b534b095`, and records
`STAGED.json` as `STAGED_NOT_RUN` with `algorithm_substitutions: []`.

`run.py` then runs

```sh
go test ./pkg/sfu/ -count=1 -v -run '^TestOriginalRedEncodeSource$'
```

in that stage, writes the rows to `red-encode.results`, and runs

```sh
cargo test --release --locked -p str0m \
  packet::red::test::original_go_red_encoder_vectors_match_red_sender_byte_for_byte \
  -- --ignored --exact
```

with `GO_RED_ENCODE_RESULTS` pointed at the actual rows. Finally it re-runs that
test against `red-encode.results.mutant`, where the first full-window row has one
redundant header removed — still a decodable two-block chain, one level less
history — and requires the run to **fail**. A report can therefore only be green
when the comparison is able to disagree.

The Rust test is `#[ignore]`d and only meaningful through `run.py`; it is not part
of `tests/oracles/build-oracles.py`, which compiles the self-contained Go modules
and does not stage the locked tree.
