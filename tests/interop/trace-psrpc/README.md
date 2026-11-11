# PSRPC tracing: middleware and real constructor scope

Read [SOURCE-CONSTRUCTOR-PROFILES.md](SOURCE-CONSTRUCTOR-PROFILES.md) for the fixed Go0/1/2-layer factory map, new source-only patch, real acceptance commands and explicit limits.

Candidate20's original Agent trace fixture assumption was wrong: the fixed Go AgentInternal client and server constructors install no OTel options. Its actual failed four-pair receipt is retained unchanged. The corrected Agent campaign keeps every original signed assignment/availability/affinity/termination/cleanup business gate, requires AgentInternal trace absence in both implementations, and requires the real instrumented upper AgentDispatch two-client-layer exports. This does not waive real cross-node parent propagation: the actual native-WHIP four-pair campaign now has an optional --trace gate, and actual Linux SIP worker→backend IOInfo has --trace-ioinfo.

`observer.go` remains the original explicit-middleware oracle over real localbus calls, errors/retries/metadata/multi; the six Rust middleware tests remain explicit-option unit coverage. `factory-observer.go` additionally invokes original generated AgentInternal defaults, ClientParams.Options and original server observability options. It observes constructor policy, not a fake media worker.

All real campaigns retain raw OTLP requests/headers, decoded spans, side labels, exact instrumentation scope/kind/status/events/lifetime, immutable binary/runner identities, original business outcomes and real process/browser cleanup. No third probe is labelled as a real server. Existing disabled/default configuration behavior is source-driven rather than inferred from a globally configured provider.

The original notes below describe the pre-scope middleware implementation and acceptance requirements. Their old AgentInternal-edge assertion is historical and rejected by SOURCE-CONSTRUCTOR-PROFILES; it must never be used to upgrade candidate20 or waive new production tests.

<details><summary>Historical candidate20 middleware/fixture notes (not current constructor acceptance)</summary>

# C10 actual PSRPC spans / OTLP collector

Source checkpoint only: no Cargo, Go observer, collector or mixed servers have
been executed by the implementing agent. This is not a passing receipt.

Base: candidate13 source archive SHA256
`4a742b7c09b0b8ea680dd0590cdd0ba488e5f74362820d9c49010fda3a690ab1`.
`/tmp/livekit-trace-candidate14-20261105/BASE.json` records 2736 base file hashes.
Main production was not edited. The coordinator owns integration, build,
actual tests, commit/push and cleanup.

## Contracts

- Actual `otelpsrpc` scope `github.com/livekit/psrpc`, version `v0.8.0`.
- Each unary attempt has Sent CLIENT; accepted, decoded, claimed handler has
  Recv SERVER. Retry is outside telemetry. Multi clients have no default Sent;
  the actual multi server handlers still have Recv. No default stream spans.
- TraceContext (not baggage/global propagator) merges only injected keys into
  outgoing Request.metadata. Incoming application metadata is not forwarded.
- Poll-scoped async context restores the previous task's context; no TLS guard
  held across await. Explicitly spawned application tasks must still pass the
  context, just as Go goroutines must receive their caller context.
- Completion events follow original Outbound / Inbound sequence. PSRPC wire
  errors use message alone (not Rust `code: message` display) and fixed gRPC map.
- A forced native future drop retires the span without fabricating Inbound or
  success, distinguishing expiry from cancellation. Go does not force-kill
  noncooperative handlers: this native cancellation edge remains independently
  observable and must not be certified equal from cooperative-call tests.
- Default SDK0.33 batch exporter owns a std thread. The previous async HTTP
  client is unsupported by that processor. The blocking OTLP client is built by
  the exporter on a separate thread, and shutdown flush runs in spawn_blocking.
- Global provider is configured before RPC constructors capture it. Jaeger
  guard owns shutdown even on startup failure; normal shutdown happens after
  RPC/worker teardown. Actual exporter errors produce a warning.

## Coordinator execution

In the integrated next candidate's `livekit-rs-server/`:

```
cargo test -p lk-compat --lib service_bus::telemetry::tests::
cargo test -p livecloud --lib jaeger::tests::actual_otlp_batch_shutdown_exports_last_span_before_default_batch_interval
```

Require **6 telemetry tests + 1 actual HTTP exporter flush test**, all nonzero
and exit0. The tests construct independent providers; they do not reset or
replace global providers. The flush test uses the actual production builder,
real TCP/HTTP exporter request and immediate owned shutdown before the default
batch interval, not an in-memory helper as proof of export.

From `references/livekit-stable/`, with the absolute observer path:

```
go run -mod=readonly /absolute/server-inputs/tests/interop/trace-psrpc/observer.go > /new-evidence/go-observer.json
```

This calls the original fixed Go middleware over real localbus RPCs, with actual
PSRPC/plain/wrapped/gRPC errors, actual retry interceptor, metadata and multi.
It observes rather than copies the middleware. Match Go exception type/status
attributes against the Rust receipt before accepting exact diagnostic parity.
The wrapper exception types have no equivalent public Rust type identity;
those cannot be guessed from error strings.

Using the existing **actual fixed-protocol Agent worker probe** binary built
for `agent-job-mixed` (unaltered JWT/availability/assignment/termination gates):

```
python3 tests/interop/trace-psrpc/run-paired.py \
  --go-server /immutable/fixed-Go --rust-server /immutable/next-Rust \
  --probe /immutable/actual-agent-probe \
  --config-mode yaml --evidence-dir /new-evidence/trace-yaml-four-pairs
```

Repeat the entire four-pair matrix for `--config-mode cli` and `env` in new
evidence directories. Each owns Redis, two actual servers and loopback OTLP
collector; no build occurs in the runner. Each requires original business PASS,
actual exported JobRequest AND JobTerminate Sent(A)→Recv(B) same trace and
parent relationship, exact scope/kind/event order/OK and real process exit0.
Raw OTLP bodies, headers, decoded spans, hashes and collector errors are saved.
No token/private data is added to spans. Resource labels identify actual sides.
All input binary/probe/runner/collector identities are checked before/after.

The Go production main does not explicitly flush its global provider at exit:
the mixed observer permits its normal batch interval before stopping servers.
This is distinct from Rust's required owned immediate-shutdown flush test.
Do not retrofit Go main and call it original shutdown behavior.

## Remaining independently open surfaces

- The full cross-Go/Rust matrix and original observer have not run yet.
- HTTPS OTLP/custom trust still has the prior build's limitation; no widening of
  TLS/provider policy is included. Go container resource detector is still not
  ported. Neither is waived by the HTTP success matrix.
- Collector unavailable/500/recovery and bounded force-stop need actual final
  binary campaign receipts. A returned provider/config parse is not export proof.
- Incoming-parent sampled/unsampled/malformed/tracestate edges must be observed
  against the original Go tool. The six consumer tests are not a complete
  per-configuration source coverage claim.
- Public HTTP request→RPC and any application actor/spawn boundaries must follow
  fixed Go consumers, not an invented universal tracing requirement; this patch
  propagates context through the actual RPC handler future and nested awaits.

Do not close root C10 or any YAML parent until the corresponding actual receipts
and known differences are resolved on one final source identity.

</details>
