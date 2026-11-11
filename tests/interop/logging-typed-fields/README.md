# Actual typed logging consumer

Status: SOURCE WRITTEN, NOT COMPILED/NOT RUN for this candidate. This is an
independent C11/C32 correction; existing global/item sampling is preserved.

The production tracing-to-zap layer previously handled only Debug and Str.
Tracing's default Visit methods therefore converted signed/unsigned counters,
floats and booleans to strings. A runtime warning such as the real RoomTask
`agent_index=index` consequently changed the structured log's field type.
The actual collector now implements primitive visitors. Unsigned 64-bit values
remain exact through u64::MAX; error trait objects use their display message.
Debug-formatted fields remain explicitly formatted strings.

The zap encoder now quotes NaN/+Inf/-Inf exactly as fixed zap1.28.0's
zapcore/json_encoder.go appendFloat. Finite fields use fixed decimal rendering
like buffer.Buffer.AppendFloat('f',-1), including tiny/large values and -0.
Console field JSON and production JSON share these value rules.

Coordinator checks (must report nonzero test counts):

```sh
cargo test -p livecloud --lib zap::tests::real_tracing_ -- --nocapture
```

The two tests emit actual tracing events through the production field collector
and full zap formatter. They parse real resulting JSON, check primitive types,
unsigned limits, multiline strings, error display, explicit Debug behavior,
nonfinite legal JSON and console behavior. They do not change global sampling
or treat a serializer helper alone as event producer coverage.

Fresh fixed-Go oracle, from the unchanged fixed server module graph:

```sh
ROOT=/absolute/livekit-rs
cd "$ROOT/references/livekit-stable"
GOWORK=off go run -mod=readonly \
 "$ROOT/livekit-rs-server/tests/interop/logging-typed-fields/oracle/main.go" \
 > /absolute/new/oracle.stdout 2> /absolute/new/go-json.log
cd "$ROOT/livekit-rs-server"
LOGGING_TYPED_GO_RESULTS=/absolute/new/go-json.log \
 cargo test -p livecloud --lib zap::tests::actual_fixed_go_typed_field_rows \
 -- --ignored --exact --nocapture
```

This invokes actual protocol.logger.InitFromConfig/Infow, not a copied encoder.
The matching Rust test compares both complete field maps, normalizing only
runtime timestamp and source caller. `console` as Go program argument captures
actual original console output separately. Source and executable/module input
hashes plus command exits must accompany any acceptance claim. No Go/Cargo or
collector service was started by the author of this source change.
