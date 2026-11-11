# Actual Google protobuf container-wire oracle for negative zero

> The absolute paths below are the recorded batch 22e run on the measuring host. Substitute your own
> checkout for `/Users/zoujiaqing/projects/livekit-rs` and any writable directory for the
> `/private/tmp/...` staging paths; every driver takes them as flags.

Consumer: `crates/lk-protocol/tests/protobuf_negative_zero.rs:116`
(`actual_google_container_wire_matches_generated_encoder`, `#[ignore]`), which
reads `GO_NEGATIVE_ZERO_CONTAINER_RESULTS` as 4 JSONL rows
`{name, double_bits, single_bits, optional, repeated, map, wire_base64}` and
compares the bytes byte-for-byte against the Rust prost encoder
(`protobuf_negative_zero.rs:123-152`).

## What is actually executed

`float_containers.proto` is compiled by **protoc-gen-go built from the pinned
`google.golang.org/protobuf` v1.36.12 module graph** (not the `~/go/bin` copy, whose
version is unpinned), and `main.go` calls `proto.Marshal` on that real generated
message. No wire byte, field tag or packed-length prefix is hand-authored.

The `.proto` is the wire-shape counterpart of the Rust-only
`#[derive(Message)] FloatContainers` at `protobuf_negative_zero.rs:5-19`:
`float single = 1`, `double double = 2`, `repeated float repeated = 3`,
`map<string,double> map = 4`, `map<string,float> single_map = 5`,
`optional double optional = 6`. Field numbers, scalar kinds, labels, map key/value
kinds and proto3 optional presence are copied from that derive; nothing is added.

## Corpus provenance — newly authored, and labelled as such

`FloatContainers` has no upstream `.proto`: it is a Rust test-only struct. Both the
`.proto` and the 4-case matrix are therefore **newly authored for this oracle**,
not upstream data. The matrix is the sign × presence cross that the Rust consumer's
own flag handling makes observable:

| name | single / double | optional | repeated | map |
| --- | --- | --- | --- | --- |
| `negative-zero-all-containers` | -0 / -0 | yes | yes | yes |
| `positive-zero-all-containers` | +0 / +0 | yes | yes | yes |
| `negative-zero-scalars-only` | -0 / -0 | no | no | no |
| `positive-zero-implicit-defaults` | +0 / +0 | no | no | no |

The last row is the one that pins proto3 implicit presence: its real Go wire is
**empty** (`wire_len: 0`), so `+0` scalars must be omitted while `-0` scalars must
be emitted. The second row pins that an explicitly present `+0` optional survives.

## Measured map-order instability (recorded, not assumed away)

The Rust consumer stores both maps in a `BTreeMap`, i.e. sorted by key. Go's
default `proto.Marshal` leaves map iteration order unspecified, so `wire_base64`
is produced with `MarshalOptions{Deterministic: true}` — the comparable operand.

The producer does not stop there: it also runs 8 **default** marshals per case and
records `default_marshal_order_stable`. Measured result:

* both map cases → `false` (default Go map order really does vary between calls)
* both scalar-only cases → `true` (no map, nothing to reorder)

So the deterministic option is required for a meaningful byte comparison, and the
instability it avoids is persisted as evidence rather than hidden. The Rust
consumer ignores the extra field; it is not a comparison input.

## Run (coordinator only)

```sh
/usr/bin/python3 tests/oracles/protobuf-negative-zero-container/prepare.py \
  --repository /Users/zoujiaqing/projects/livekit-rs --out /private/tmp/b22e-stage/negzero-container
cd /private/tmp/b22e-stage/negzero-container
export GOTOOLCHAIN=local GOPROXY=off GOWORK=off
GO="$HOME/go/pkg/mod/golang.org/toolchain@v0.0.1-go1.26.3.darwin-arm64/bin/go"
"$GO" build -mod=readonly -o protoc-gen-go google.golang.org/protobuf/cmd/protoc-gen-go
protoc --plugin=protoc-gen-go=./protoc-gen-go --go_out=. --go_opt=paths=source_relative \
  -I. float_containers.proto
"$GO" build -mod=readonly -o negzero-container-oracle .
./negzero-container-oracle cases.json > results.jsonl
```

Then run the real Cargo ignored test with
`GO_NEGATIVE_ZERO_CONTAINER_RESULTS=/private/tmp/b22e-stage/negzero-container/results.jsonl`.

`protoc` itself is the host `libprotoc 29.3`; it only parses the `.proto` and drives
the pinned plugin, so the generated code and the runtime are both v1.36.12.

## Scope

Exactly the binary wire encoding of one 6-field container message for 4
sign/presence cases. This does **not** execute protojson, `descriptor_pool`,
`DynamicMessage`, the Twirp JSON dialects, `EncodingOptions`, or any live server
path — those are covered by the three non-ignored siblings in the same file. A
module-resolution or protoc error is not a test verdict: preserve the original exit
code and inspect the pinned paths rather than updating versions.