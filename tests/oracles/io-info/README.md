# Actual fixed-Go / Rust IOInfo and Redis-store cross fixtures

Status: actual Go/Rust four-direction callback/store execution PASS in F06.
See `livekit-rs-docs/evidence/2026-10-25/f06-io-cross-pass.json`; earlier failures remain archived.
This is control-plane callback/state evidence, not external worker media acceptance.

The Go tool instantiates the actual fixed v1.13.10 IOInfoService, RedisStore and
PSRPC generated client/server. The Rust example uses production `IoInfo`,
`IoStore`, `CatalogSipCallbacks`, `RpcClient` and original-wire wrappers. Neither
implements a second store/codec/RPC algorithm or returns a fabricated worker reply.
The event observer records actual callback telemetry with ordinal/status/times.
It does not prove HTTP webhook delivery.

Coordinator builds the Go tool in this directory with its pinned module graph and
`io_store_probe` through the actual Cargo example target. It supplies two separately
owned empty loopback Redis DBs/brokers, then runs:

```sh
IO_REFERENCE_REDIS_URL=redis://127.0.0.1:PORT/DB \
IO_RUST_REDIS_URL=redis://127.0.0.1:OTHER_PORT/DB \
python3 run-cross.py --go /absolute/go-io-oracle \
  --rust /absolute/io_store_probe --dedicated-brokers \
  --output /tmp/actual-io-cross-evidence.json
```

Credentials are passed in process environments, never evidence/argv. Only one
IOInfo server runs at a time: Redis Pub/Sub is independent of logical DB selection.
The runner refuses pre-existing data or IOInfo subscribers. Every server must
write its unique readiness file only after real request/claim subscription ACKs.
Final Rust server disables SkipClaim; clients use the fixed non-affinity policy
AcceptFirstAvailable and default1s selection timeout, then actual grant. Original
3s RPC deadlines remain. Earlier skip-claim builds are diagnostics, not this gate.

Four independent campaigns exercise Go→Go, Rust→Rust, Go→Rust, Rust→Go. Their ordered
21 callbacks and9 real-store operations must match the actual Go baseline, with
explicit expected successful/error cases so matching failures cannot pass. Actual
Go generated decoding normalizes successful wire into complete known JSON plus
an unknown-field tree. No business fields are discarded or globally sorted.
Unknown raw values/order are checked literally; only the observer's tree paths
are sorted because protobuf reflection field iteration itself is unspecified.
Original wire is retained for actual hashes. The current lists contain0/1 elements;
this corpus does not invent a source order for Redis hash/set iteration.

Additional mandatory work in every campaign:

- Actual Egress/Ingress callbacks carry unknown varints, noncanonical unknown values,
  cross-tag order and known-number/wrong-wire fields; real callback/store replies
  must retain the literal future-field payload. The Go tool, not Rust's decoder,
  inspects all returned wire. Unknown ingress state is separately persisted.
- Actual Redis DUMP/RESTORE isolates WRONGTYPE on Egress/Ingress hashes and malformed
  ingress state. Every affected real callback must fail and match Go. Egress Update
  still emits its source event despite storage failure. No failure is fake-injected
  into a method return value.
- Deleting the real external ingress state key tests the nil-loaded-state callback
  path. This is a real missing-key precondition, not proof that deletion occurred
  between the service's HGET and GET.
- Two real PSRPC clients concurrently submit generations400/500. Generation500
  must succeed and be the final persisted state; generation400 may succeed first
  or fail as obsolete. All actual race events remain in evidence, while their
  declared concurrent phase is excluded only from ordered-event equivalence.
  This exercises concurrent delivery; WATCH collision/retry is not forced or
  claimed proven. Every other ordered callback/fault event remains compared.

Store callbacks retain several unintuitive upstream boundaries: CreateIngress's
reply may include supplied state while initial stored state is empty; a lower
UpdatedAt in the same generation is ignored by storage but still emits request-
state telemetry; an older StartedAt errors without telemetry; equal timestamps
are accepted. Direct store outputs and callback outputs remain separate paths.

All owned servers stop gracefully and actual return codes are recorded. Synthetic
keys are removed by exact known names; no FLUSH or wildcard deletion. Five-second
queue leases expire naturally; unexpected keys cause cleanup FAIL. Temporary raw
logs/configuration are removed after safe hashes/counts/status/event evidence is
written. Every failed comparison remains FAIL; later campaigns do not repair it.
External recording artifacts, decoded ingest media, SIP RTP, real signed webhook
sinks, forced WATCH conflicts, sweeper partial-failure retries, HA/reconnect and
multi-node room ownership remain separate OPEN gates.

### Controlled parser-fault diagnosis
`diagnose-corrupt-state.py --go <oracle> --rust <probe> --output <new-report> --dedicated-brokers` uses the same two broker environment variables. It actually creates a synthetic ingress, installs one `0xff` external-state byte and invokes IOInfo GetIngressInfo. It records only this fixed parser error's exact text and UTF-8 hex, never normalizes differences, and never overwrites the initial failed cross report. Both owned servers and exact synthetic state are cleaned in `finally`. Completion is `DIAGNOSTIC_COMPLETE`, not compatibility PASS. The full runner also reports this controlled error text alongside its unchanged strict comparison.

The sole comparison representation exception is SPEC-0018's complete invalid-wire parser error for `fault-corrupt-ingress-state`: the fixed Go protobuf deliberately permits one ASCII-space or U+00A0 separator. Original text/hex/hashes remain in results. All codes, wrappers, suffixes, other properties and other whitespace remain exact. Run `python3 -m unittest discover -s livekit-rs-server/tests/oracles/io-info -p 'test_*.py'` for deterministic positive and negative comparator regressions. No earlier failed report is rewritten.

## Actual executable wiring and unchanged SDK/HTTP webhooks

`run-main-http.mjs` invokes the genuine generated Go IOInfo client against each
already-running actual main executable. It creates an actual room, submits
explicit synthetic callback records, lists actual persisted results with the
unchanged locked EgressClient, and authenticates ordered extra webhooks with the
unchanged WebhookReceiver. It proves callback/store/main HTTP wiring, not real
worker media or a recording. Supply `IO_GO_ORACLE`, `REFERENCE_URL`, `LIVEKIT_URL`,
`IO_REFERENCE_REDIS_URL`, `IO_RUST_REDIS_URL`, and `IO_MAIN_OUTPUT`.
`IO_DEDICATED_BROKERS=1` is mandatory: synthetic Egress records have no public
delete operation and remain until the coordinator stops these disposable brokers.
The runner closes its real webhook listener and deletes its owned rooms; the
coordinator owns server/broker shutdown and temporary-output cleanup.
