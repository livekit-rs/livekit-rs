# Pinned psrpc v0.8.2 wire oracle

Development-only Go tool; it is not a server dependency. The module path is a
subpath of github.com/livekit/psrpc, allowing the real upstream internal types
without changing or adding files to the immutable reference checkout. Its
`replace` points to the repository's fixed `references/psrpc` clone. The
coordinator owns every compiler execution and cleanup.

1. From this directory, generate and commit the **actual Go output**:

   `go run . -mode generate -file ../../../crates/lk-compat/src/service_bus/go-fixtures.json`

   This calls real `bus.New(...).Publish` and `bus.Deserialize`; no handwritten
   envelope generator substitutes for the upstream package. It calls all seven
   actual channel helpers and GetHandlerKey over 48 cases. Generated data is
   synthetic and contains no credentials or production identifiers.

2. From the server workspace, exercise Rust and optionally export fresh Rust
   bytes for the reverse direction:

   `PSRPC_RUST_FIXTURE_OUT=/tmp/livekit-rs-psrpc-rust.json cargo test --locked -p lk-compat service_bus -- --nocapture`

3. From this directory, have the real Go decoder read the exported Rust bytes:

   `go run . -mode verify -file /tmp/livekit-rs-psrpc-rust.json`

Compression output is compared through genuine decoding, not byte equality of
implementation-specific gzip streams. Protobuf map serialization is not assumed
to be ordered. Unknown protobuf fields are accepted and excluded from the
known-field semantic comparison (Rust prost does not retain them). The test
corpus includes both legacy Any and current raw payloads; current upstream
handlers execute raw payloads, not an invented Any fallback.

The manifest/source clone and schema checker bind the actual package version.
If Go's internal import rule rejects the submodule in a future toolchain, use a
task-owned copy of the whole frozen module under /tmp and place this tool there;
never modify the reference checkout. The coordinator must preserve execution
status/hashes in evidence before deleting temporary outputs.

This validates wire codecs and channels only. It does not validate Redis
subscriptions, queues, claim selection, worker requests, service persistence or
media/telephone side effects.

## Real Redis unary/claim development probes (F03b)

These commands are run only by the coordinator against a task-owned Redis >=6.
The Rust broker connection uses RESP3; the real Go psrpc transport may use RESP2,
but both share the same Pub/Sub messages and SHA256/base64 SETNX queue lease.
No public API or actual media/telephone worker is enabled by these examples.

Build the Rust development example through the coordinator's normal checks:
`cargo build --locked -p lk-compat --example psrpc_redis_probe`.

Run its production-code selftest on an explicitly chosen broker:
`target/debug/examples/psrpc_redis_probe selftest --redis-url redis://127.0.0.1:PORT/ --result /tmp/psrpc-selftest.json`.

Go server → Rust client (start server, await its readiness JSON, then call):

- `go run . -mode redis-serve -redis-url redis://127.0.0.1:PORT/ -ready /tmp/psrpc-go-ready.json -result /tmp/psrpc-go-stopped.json -lifetime 30s`
- `target/debug/examples/psrpc_redis_probe call --redis-url redis://127.0.0.1:PORT/ --expect-label go --result /tmp/psrpc-rust-call.json`

Rust server → Go client:

- `target/debug/examples/psrpc_redis_probe serve --redis-url redis://127.0.0.1:PORT/ --ready /tmp/psrpc-rust-ready.json --result /tmp/psrpc-rust-stopped.json --lifetime-ms 30000`
- `go run . -mode redis-call -redis-url redis://127.0.0.1:PORT/ -expect-label rust -result /tmp/psrpc-go-call.json`

Both tools default to the same synthetic CompatEcho/Echo service, Unicode topic,
metadata and claim policy. Use unique `--topic` / `-topic` values per independent
case to avoid accidental cross-case routing. Options include queue, claim,
affinity, response label, remote-error expectation, timeout, compressible payload
size and bounded server lifetime. `value=error` returns a genuine remote error;
`value=sleep:500` exercises real handler deadline behavior. The `call` command
exits nonzero if the actual response/code differs from the required expectation.
Use `queue=true` on both peers and `skip-claim=true` on the server for the actual
handling-announcement gate; affinity callbacks are absent for queue methods.

Readiness is condition-based, not a fixed startup sleep. Go's public API returns
before reconciliation, so its tool confirms broker subscriber-count increments
under the coordinator's controlled sequential startup. Rust `subscribe` awaits
the actual Redis command acknowledgement. Stop owned peers and Redis, persist
necessary safe evidence, then remove generated temporary files under root's
normal delivery cleanup rules. Do not operate a system Redis service.
