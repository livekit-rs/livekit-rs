# Supplemental typed failure observer for the stored `livekit.Node` decoder

> The absolute paths below are the recorded batch 22e run on the measuring host. Substitute your own
> checkout for `/Users/zoujiaqing/projects/livekit-rs` and any writable directory for the
> `/private/tmp/...` staging paths; every driver takes them as flags.

Consumer: `crates/livecloud/src/cli_nodes.rs:929`
(`actual_go_stored_node_typed_failure_and_projection_match16`, `#[ignore]`),
which reads `NODE_WIRE_TYPED_GO_RESULTS` as JSONL with exactly 16 unique rows
keyed by the ids of `tests/oracles/list-nodes-wire/cases.jsonl`.

The committed `tests/oracles/list-nodes-wire/main.go` already emits
`id / accepted / error / known_json`. The Rust consumer additionally requires, for
every rejected row, **both** `invalid_utf8` and `error_is_proto`
(`cli_nodes.rs:967-977`). That supplemental producer did not exist; this directory
is it. The original observer is left untouched so its existing PASS evidence is
not disturbed.

## What is actually executed

The same original fixed generated decoder: `proto.Unmarshal(raw, &livekit.Node{})`
from the pinned `references/protocol-stable`, then `protojson.Marshal` for accepted
rows. No wire reimplementation, no hand-authored admission decision.

## How the two typed flags are probed — by type, not by error text

This was the open design question in the batch 22c recipe. It is resolved from the
pinned `google.golang.org/protobuf` v1.36.12 source, not guessed:

* `internal/impl/codec_field.go:17-21` defines
  `type errInvalidUTF8 struct{}` with an **exported method**
  `func (errInvalidUTF8) InvalidUTF8() bool { return true }` and
  `Unwrap() error { return errors.Error }`. The family is therefore probed with
  `errors.As(err, &interface{ InvalidUTF8() bool })` — an interface assertion that
  never reads the message.
* `proto/proto.go:32-35` re-exports the internal sentinel as `proto.Error`, so
  `error_is_proto` is `errors.Is(err, proto.Error)`.

Both probes are independent of the deliberately destabilised `proto: ` /
`proto:\u00a0` prefix (`internal/errors/errors.go:26-34`, pinned excerpt at
`references/go-modules/protobuf-v1.36.12-error-prefix`). The Rust side classifies
schema-bound without reading error text either
(`crates/lk-protocol/src/json/wire_error.rs:1-27`), so this is a genuine
two-independent-methods differential rather than a shared string match.

The raw `error` string is still emitted as evidence. Note that the invalid-UTF-8
row's message is `string field contains invalid UTF-8` with **no** `proto: `
prefix, because `errInvalidUTF8` is returned directly by the field coder rather
than being wrapped by `errors.InvalidUTF8(name)`. It still satisfies
`errors.Is(err, proto.Error)` through `Unwrap()`.

## Corpus

`tests/oracles/list-nodes-wire/cases.jsonl` is reused verbatim — no new corpus is
authored, and the 16 ids are exactly the ones the Rust consumer iterates.

## Run (coordinator only)

```sh
/usr/bin/python3 tests/oracles/list-nodes-wire-typed/prepare.py \
  --repository /Users/zoujiaqing/projects/livekit-rs --out /private/tmp/b22e-stage/list-nodes-wire-typed
cd /private/tmp/b22e-stage/list-nodes-wire-typed
GOTOOLCHAIN=local GOPROXY=off GOWORK=off \
  "$HOME/go/pkg/mod/golang.org/toolchain@v0.0.1-go1.26.3.darwin-arm64/bin/go" \
  build -mod=readonly -o wire-typed-oracle .
./wire-typed-oracle < cases.jsonl > results.jsonl
```

Then run the real Cargo ignored test with
`NODE_WIRE_TYPED_GO_RESULTS=/private/tmp/b22e-stage/list-nodes-wire-typed/results.jsonl`.

## Scope

Exactly decoder admission, the protojson projection of accepted nodes, and the two
typed failure families over 16 committed wire inputs. This does **not** execute
`list-nodes`, table rendering, a Redis HGETALL, CLI stderr, or node selection. It
does not prove the runtime branch is reached in a live server. Whole-CLI stderr
exactness remains separately OPEN. A module-resolution error is not a test
verdict: preserve the original exit code and inspect the pinned paths.