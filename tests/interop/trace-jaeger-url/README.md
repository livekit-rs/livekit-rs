# SPEC-0077 C27 — launched-MAIN `trace.jaeger_url` paired Go/Rust differential

This harness proves the **real main server** consumes `trace.jaeger_url` and
exports its WHIP lifecycle to an OTLP collector, identically on the locked Go
oracle and the Rust `livecloud` candidate. It names one ledger leaf:
`YAML:trace.jaeger_url`.

It is a thin differential wrapper around the self-contained
[`trace-https` observer](../trace-https/observe.go). The observer already:

- writes `trace.jaeger_url` into the daemon YAML (`observe.go:319`),
- launches one immutable daemon and drives an authenticated WHIP **Create** then
  **DELETE** with a Pion offer (requires `201` then `200`),
- owns a loopback OTLP/HTTP protobuf collector that records **only** spans whose
  resource carries `service.name=livekit` (`observe.go:82`),
- sets `OTEL_BSP_SCHEDULE_DELAY=250` so the batch exports inside its 6 s window,
- shuts the daemon down with SIGTERM and checks exclusive port rebinding,
- hashes the daemon before/after (anti-tamper) and writes `result.json`.

The observer's Pion driver configures **no tracer**, so every recorded span
originates from the **daemon** and depends on `jaeger_url` being consumed.

## Upstream consumer

Locked Go `cmd/server/main.go:253-254`:

```go
if url := conf.Trace.JaegerURL; url != "" { jaeger.Configure(ctx, url, "livekit") }
```

`jaeger.Configure` normalizes the endpoint (prepend `http://`, default port
`4318`, default path `/v1/traces`), builds a batched OTLP/HTTP exporter with
resource `service.name=livekit`, and installs it as the global tracer provider.
The Rust mirror is `livecloud/src/jaeger.rs::configure` called from
`server.rs:155-162` under the same `!jaeger_url.is_empty()` guard.

## Profiles

| Profile | Family | Collector observation |
| --- | --- | --- |
| `http` | export | Plaintext OTLP request carries `Recv.WHIP.Create` + `Sent.WHIP.Create`×2 + `Sent.WHIPParticipant.DeleteSession`×2. |
| `trusted` | export | The configured trace CA verifies the HTTPS collector; the span arrives over TLS **HTTP/2**. |
| `mtls` | export | The HTTPS collector also verifies the daemon's owned temporary client cert before receiving the span. |
| `untrusted` | suppress | A real TLS ClientHello is observed and **zero** HTTP requests (system trust rejects the fixture root). |
| `wrong-root` | suppress | The trace-specific `OTEL_EXPORTER_OTLP_TRACES_CERTIFICATE` (an unrelated CA) overrides the correct generic `OTEL_EXPORTER_OTLP_CERTIFICATE`; ClientHello observed, zero requests. |

Every profile also requires WHIP `201`/`200`, daemon exit `0`, and exclusive
rebinding of the daemon HTTP, media TCP and collector ports.

## Normalized contract (scope limit)

The harness compares the boolean export observables
`{exported, recv_whip_create, sent_whip_create, tls_hello, http2, client_auth,
server_clean, ports_released, whip, passed}` and the WHIP-lifecycle span-name
catalog (order-insensitive multiset) cross-side. It does **not** compare
byte-equal OTLP payloads: the Go resource detector merges `WithContainer`
(cgroup) attributes with no Rust equivalent, so resource attributes legitimately
differ. It does **not** claim the daemon's full telemetry surface is identical —
only the WHIP Create/Delete lifecycle this harness drives. No comparison field is
deleted; these are explicit scope bounds.

Each profile is asserted against a Go-oracle-derived contract on **each side
independently**, then compared cross-side, so the differential is non-vacuous.
`cross_profile_controls` add discriminating checks (positive export, negative
suppression, TLS HTTP/2 specificity, mTLS client-auth specificity, wrong-root
override, daemon-origin `Recv` span presence, WHIP lifecycle, observer exit
integrity).

## Build the observer

Build from the fixed upstream Go module with its pinned toolchain on PATH (the
observer imports `github.com/livekit/protocol/auth` and `pion/webrtc/v4`):

```sh
cd references/livekit-stable
GOTOOLCHAIN=go1.26.0 GOPROXY=off GOWORK=off go build -mod=readonly \
  -o /tmp/c27-build/trace-observer \
  /absolute/path/to/tests/interop/trace-https/observe.go
```

## Run the paired differential

```sh
python3 tests/interop/trace-jaeger-url/run-paired.py \
  --go /tmp/c27-build/livekit-server-go \
  --rust /absolute/path/to/target/release/livecloud \
  --observer /tmp/c27-build/trace-observer \
  --fixture /absolute/path/to/crates/rtc-tls/tests/fixtures \
  --output /tmp/c27-build/acceptance
```

All paths must be absolute; `--output` must not pre-exist. The harness runs the
10 observer invocations (5 profiles × 2 sides) **sequentially** (the daemon UDP
media range 50000–60000 is shared), writes `BATCH.json` (gate-recognized) and
`execution.json` (full per-side raw receipts), prints a summary, and exits `0`
iff every profile matches and every control is true, else `1`. Every original
observer exit code is preserved; no pipe filters a result.

## Falsifiability positive control

A mutant that accepts-but-ignores `jaeger_url` (`server.rs` forces
`tracing_provider = None`) exports nothing: the three export profiles flip
`exported` true→false, and the two suppress profiles lose even the TLS
ClientHello (no export attempt). Built in an independent target dir, it FAILS the
harness while the Go oracle stays correct on all five profiles. The mutant
receipt is nested under the evidence `positive-control/mutant/` and is **never**
cited as acceptance. See the evidence `positive-control.json`.

## Residual (honest, out of scope)

The `WithContainer`/cgroup resource-attribute gap, the broader non-WHIP telemetry
surface, and other platforms remain open. This harness proves the launched-MAIN
`trace.jaeger_url` OTLP-export consumer surface only. It is pure loopback — no
route/Clash/proxy mutation and no public network dependency.