# C20 bind-addresses paired differential harness

`run-paired.py` boots the **real launched MAIN** of the locked Go oracle and the Rust
candidate with identical config bytes / argv / ENV / reserved ports (single node, no Redis)
and compares the **bind contract** of `YAML:bind_addresses` / `base_flags:--bind` across 10
profiles. It is the SPEC-0077 C20 acceptance harness; the certifying receipt is `BATCH.json`.

## Run

```sh
# build the candidate (clean) and the locked oracle
cargo build --release --bin livecloud
cd references/livekit-stable && GOTOOLCHAIN=go1.26.0 GOPROXY=off GOWORK=off \
  go build -mod=readonly -o /tmp/c20-build/livekit-server-go ./cmd/server && cd -

python3 tests/interop/bind-addresses/run-paired.py \
  --official /tmp/c20-build/livekit-server-go \
  --rust     target/release/livecloud \
  --output   /tmp/c20-build/acceptance
```

The harness pins the Go oracle identity (`FIXED_GO_SHA256`, `e5655ac2` v1.13.10) and aborts
before execution on mismatch. It discovers the en0 IPv4 with `ipconfig getifaddr en0` (not the
UDP trick, which returns a Clash fake-IP on this host). Exit code is 0 only when
`bind_contract_status == PASS` and `input_identity_stable`.

## Two verdicts (no field or failure mode deleted)

- **`bind_contract_status`** — the item observable: listener creation. Per profile it compares
  the TCP reachability matrix (`127.0.0.1` / `::1` / en0 × http / prom / debug), the startup
  `bindAddresses` log field (present iff `BindAddresses != nil`, value verbatim), the empty-bind
  `ERROR service/server.go:335 could not start server` (no error field) + process-stays-alive
  lifecycle, the unbindable non-zero-exit-before-starting-log lifecycle, and CLI `--bind`
  precedence / repeatability. Each side must independently satisfy the structural contract
  (`go_matches_contract`, `rust_matches_contract`) **and** the normalized contracts must be
  deep-equal (`parity`). **This is the certifying verdict.**
- **`http_serving_status`** — supplementary userspace HTTP liveness (`http.client` GET `/`).
  Where v4/v6 are reachable both sides must serve 200 (a real falsifiable check). The en0
  userspace-accept divergence is emitted under `platform_residuals` (see below), never folded
  into a silent pass and never counted against the bind contract. `http_liveness` data is kept
  in every observation.

## Profiles

| id | bind input | CLI | expectation |
|---|---|---|---|
| loopback | `["127.0.0.1"]` | — | concrete IPv4: v4 reachable, v6/en0 refused |
| wildcard-v4 | `["0.0.0.0"]` | — | dual-stack: v4+v6+en0 reachable |
| wildcard-v6 | `["::"]` | — | dual-stack: v4+v6+en0 reachable |
| absent | nil (omitted) | — | default dual-stack; `bindAddresses` field absent |
| empty | `[]` | — | 0 listeners; could-not-start ERROR; process alive |
| multi | `["127.0.0.1","en0"]` | — | concrete: v4+en0, v6 refused |
| localhost | `["localhost"]` | — | resolves 127.0.0.1: v4 only |
| unbindable | `["10.255.255.1"]` | — | non-zero exit before starting log, no listener |
| cli-override | `["0.0.0.0"]` | `--bind 127.0.0.1` | CLI wins: concrete v4 only |
| cli-multi | absent | `--bind 127.0.0.1 --bind en0` | repeatable CLI replaces nil: v4+en0 |

## Normalization

Timestamps, nodeID, ephemeral ports and the Go zap stacktrace are side-specific and scrubbed
(Rust `logging::notify` passes `None`, so it emits no stack). The compared contract is the TCP
reachability matrix, the `bindAddresses` field shape, the could-not-start ERROR record (caller
`service/server.go:335`, no error field), the process lifecycle, and CLI precedence. Per-side
structural assertions are independent of the cross-side deep-equal, so normalization cannot
mask a real divergence (e.g. a v6 reachability drift or a missing ERROR).

## Falsifiability

Replacing the dual-stack `[::]` wildcard bind in `server.rs listen()` with the pre-fix IPv4-only
`0.0.0.0` bind (independent target dir) makes `bind_contract_status` FAIL on exactly
`wildcard-v4`, `wildcard-v6`, `absent` (Rust reach v6=False vs Go v6=True). The byte-identical
restore rebuilds to the honest binary and the acceptance PASSes again. See
`platform-residual/` for the separate en0 anomaly and the persisted evidence under
`livekit-docs/server/evidence/2026-11-14/spec0077-c20-bind-addresses/`.

## Platform residual (OPEN, split from the bind contract)

On this macOS dev host Rust's userspace `accept()` never fires for en0-destined connections, so
the listeners do not serve en0 while the kernel handshake completes (reach matches Go) and
loopback serves 200 on the same socket. Root-caused with a bare `std::net` reproducer (zero
livekit-rs code) — see `platform-residual/README.md`. It is below `std::net`, host-specific,
independent of `bind_addresses`, and not introduced by the C20 fixes. No network mutation is
performed. Production Linux nodes are unaffected.