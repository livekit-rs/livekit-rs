# SPEC-0077 C23 — launched-MAIN embedded TURN consumer paired differential

This harness proves that a **launched MAIN** server (not a module-scope oracle)
consumes the `turn.*` config and applies the upstream embedded-TURN policy
identically on the locked Go oracle and the Rust candidate. It closes the
`official_differential_lifecycle` residual for five TURN leaves on the **current
clean release candidate**, re-binding earlier component/candidate evidence to the
launched-MAIN paired-binary tier.

## What it covers (5 ledger leaves)

| leaf | surface |
|---|---|
| `YAML:turn.enabled` | yaml |
| `YAML:turn.bind_addresses` | yaml |
| `YAML:turn.proxy_protocol` | yaml |
| `YAML:turn.proxy_protocol_trusted_cidrs` | yaml |
| `CONFIG_ENABLED_CAPABILITY:turn.proxy_protocol` | enabled_capability |

## Relationship to the existing TURN fixtures

`tests/interop/turn-tls-proxy` and `tests/interop/turn-listener-relay` are
**pion-turn Go observers** that prove certificate-verified TLS termination,
authenticated TURN allocation and bidirectional relay (the 36/36 listener/relay
matrix) on earlier candidates. C23 is **complementary**: a light **raw-socket**
launched-MAIN paired harness (C21/C22 pattern) that re-proves the config consumer
and the PROXY **REQUIRE/REJECT** gate on the current clean candidate `d1dad0fc`,
with no media, no TLS certificate and no SDK change. Full TLS termination,
authenticated relay and NAT topology remain the scope of those observers and are
**not** claimed here.

## Stimulus (why it is deterministic on both sides)

`turn.external_tls: true` makes the TLS-port listener **plain TCP** (the listener
intended for a TLS terminator), so no certificate is needed. `probe.mjs` sends raw
STUN/PROXY wire bytes:

- **TCP test A** — a bare STUN Binding Request (no PROXY header).
- **TCP test B** — a PROXY v1 header naming source `203.0.113.9:55000`, then a
  STUN Binding Request.
- **UDP test** — a STUN Binding Request datagram.

The upstream gate (`pkg/service/turn.go:222-276` `newTURNTCPListener` /
`proxyProtocolPolicy`; Rust `crates/rtc-turn/src/tcp.rs:124-151` +
`crates/livecloud/src/turn.rs:145-149`) decides per connection:

| mode | test A (bare STUN) | test B (PROXY + STUN) |
|---|---|---|
| `proxy_protocol=false` | Binding Success Response | "PROXY…" is not a STUN frame → no reply |
| `proxy_protocol=true`, upstream **trusted** | REQUIRE reads STUN bytes as a PROXY header → fails → close | header parsed → Binding Success Response, **XOR-MAPPED-ADDRESS = 203.0.113.9:55000** |
| `proxy_protocol=true`, upstream **untrusted** | REJECT → close | REJECT → close |

The trusted-CIDR match uses the **actual TCP peer** (loopback `127.0.0.1` here):
`127.0.0.0/8` trusts it, `203.0.113.0/24` does not. The forwarded source
`203.0.113.9:55000` appearing in the Binding response is a **positive** observable
that the PROXY header was parsed and used (not merely accepted-and-ignored).

### Response/address normalization (scope limit)

A **direct** (non-PROXY) Binding response legitimately reflects the **ephemeral
loopback source port**, which differs per connection and per side; for those
replies only the mapped **IP** (`127.0.0.1`) and response presence are compared,
**not** the ephemeral port. The PROXY reply port **is** compared because it is the
fixed forwarded source port (`55000`). TCP close/reset/timeout timing details
legitimately differ between the Go and Rust listeners and are **not** compared —
response presence + mapped address are the observable. No comparison field is
deleted; these are explicit scope boundaries.

## Profiles (9)

| profile | turn config | observable | proves |
|---|---|---|---|
| `turn-disabled` | `enabled=false` | TCP refused, UDP no reply | enabled drives the listener (off) |
| `turn-udp-enabled` | `enabled=true, udp_port=P, tls_port=0` | UDP Binding reply, IP 127.0.0.1 | enabled (on, UDP) |
| `turn-tcp-plain` | `enabled=true, tls_port=P, external_tls, proxy=false` | A reply, B no reply | enabled (on, TCP) + proxy off |
| `turn-bind-loopback` | `bind_addresses=[127.0.0.1]`, proxy=false | A reply on 127.0.0.1 | bind_addresses |
| `turn-proxy-trusted` | `proxy=true, trusted=[127.0.0.0/8]` | A no reply, B reply mapped 203.0.113.9:55000 | proxy_protocol + trusted CIDR + enabled capability |
| `turn-proxy-untrusted` | `proxy=true, trusted=[203.0.113.0/24]` | A no reply, B no reply | trusted-CIDR REJECT |
| `turn-invalid-ports` | `enabled=true`, no port | exit 1, `invalid TURN ports` | enabled validation gate |
| `turn-proxy-empty-cidrs` | `proxy=true, trusted=[]` | exit 1, `turn.proxy_protocol requires at least one entry…` | trusted-CIDR empty gate |
| `turn-proxy-malformed-cidr` | `proxy=true, trusted=[nope]` | exit 1, `invalid CIDR "nope" …: invalid CIDR address: nope` | trusted-CIDR malformed gate |

All TCP profiles set `turn.domain=turn.example.com` (upstream requires a valid
domain whenever `tls_port>0`, `turn.go:73-80` == Rust `normalize.rs:878-884`) and
`rtc.node_ip=127.0.0.1`. Configs are emitted as **JSON** (a valid YAML subset both
loaders accept), so the harness needs no PyYAML dependency.

## Run

```bash
# build the locked Go oracle (v1.13.10 e5655ac2) and the Rust candidate first
python3 run-paired.py \
  --go   /path/to/livekit-server-go \
  --rust /path/to/livecloud \
  --output /path/to/evidence-dir
```

Each side runs a fresh server per profile on reserved loopback ports, first
SIGINT clean exit (alive profiles) or self-exit (startup_error). The manifest
`execution.json` records per-profile go/rust normalized observables, config
sha256, binary sha256, probe reports and captured stdout/stderr. Status
`PASS_LAUNCHED_MAIN_TURN` requires every profile to satisfy its contract on both
sides, the cross-side observables to be equal, and all 22 cross-profile controls
to hold.

## Falsifiability

A positive control disables the Rust PROXY gate (`turn.rs:145`
`proxy_trusted_cidrs` forced to `None`), built in an **independent** target dir so
the clean candidate is never overwritten. That mutant turns the two
`proxy_protocol` alive profiles into proxy-off behaviour (test A replies, test B
does not) and FAILS exactly `turn-proxy-trusted` + `turn-proxy-untrusted`, passing
the other seven profiles (the startup-error gates run in config validation before
the listener, so they are unaffected). The byte-identical restore rebuilds to the
honest candidate and the final acceptance PASSes. The mutant FAIL receipt is kept
under `positive-control/mutant/` and is **not** cited as acceptance evidence.