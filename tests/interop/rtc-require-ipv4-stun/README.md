# SPEC-0077 C26 — launched-MAIN rtc.require_ipv4 STUN-udp4 paired differential

This harness proves that a **launched MAIN** server consumes the
`rtc.require_ipv4` config (and its generated `--rtc.require_ipv4` CLI flag /
`LIVEKIT_RTC_REQUIRE_IPV4` ENV var) and applies the upstream STUN **address-family
selection** identically on the locked Go oracle and the Rust candidate. It closes
the `official_differential_lifecycle` residual for two `require_ipv4` leaves on
the **current clean release candidate** with a **gate-recognized** paired receipt.

## What it covers (2 ledger leaves)

| leaf | surface |
|---|---|
| `YAML:rtc.require_ipv4` | yaml |
| `generated_flags:--rtc.require_ipv4` | cli flag (also ENV `LIVEKIT_RTC_REQUIRE_IPV4`) |

## The consumer and the observable

`require_ipv4` lives in the **pinned external module**
`github.com/livekit/mediatransportutil@v0.0.0-20260821083140-f234b534b095/pkg/rtcconfig`
(`go.mod:23`), not in `livekit-stable` itself:

- `config.go:54` — `RequireIPv4 bool yaml:"require_ipv4,omitempty"`
  ("require a routable IPv4: dial STUN over udp4 and skip local IPv6 fallback").
- `RTCConfig.Validate` (`config.go:96-113`) → `resolveNodeIP` (`config.go:121-133`)
  → `determineIP` (`ip.go:35-77`). With `UseExternalIP`, `determineIP` selects
  `GetExternalIPv4` (dial network **"udp4"**) when `RequireIPv4` is set, else
  `GetExternalIP` (network **"udp"**). `findExternalIP` does
  `dialer.Dial(network, stunServer)`; `validateExternalIP` returns nil when
  `localAddr == nil` (both sides pass nil), so the mapped address is accepted
  without a routability check.

A resolution failure is wrapped by `livekit-stable` `NewConfig`
(`pkg/config/config.go:696-698`) as `could not validate RTC config: %v`, returned
through `getConfig` → `startServer`, and printed with `fmt.Println(err)` + exit 1
(`cmd/server/main.go:190-193`) — **bare upstream text on stdout**.

The Rust mirror is `lk-config::netdetect::determine_ip` (`netdetect.rs:232-268`) →
`external_ip` (`netdetect.rs:158-170`) → `stun_binding` (`netdetect.rs:413`), which
selects `StunNetwork::V4` when `ipv4_only` else `StunNetwork::Any`, called from
`load::rtc_validate` (`load.rs:194-218`) and wrapped at `load.rs:587-588` as
`could not validate RTC config: {e}` — **byte-identical** to the Go wrapping. The
`resolve` error for an IPv6 literal under a V4 network is
`dial udp4: address ::1: no suitable address found`.

### The load-bearing discriminator: STUN address family

A fake STUN binding responder is started on **IPv6 loopback (`::1`)** and **IPv4
loopback (`127.0.0.1`)**, each answering a Binding Request with
XOR-MAPPED-ADDRESS `192.0.2.1` (TEST-NET-1). This is **pure loopback** — no
route/Clash/proxy mutation, no public network dependency.

- `require_ipv4=true` + an IPv6-only STUN server: the **"udp4"** dial cannot
  resolve `::1` and fails **before any packet is sent**. The IPv6 responder
  records **ZERO** new hits and the server exits 1 with the exact text.
- `require_ipv4=false`: the **"udp"** dial reaches `::1`, the responder records
  hits, and the server serves HTTP and exits 0.

The per-side **hit delta** is captured around each launch, so the contact boolean
(`ipv6_hits_delta > 0`) is a deterministic, falsifiable observable independent of
the exit code. A consumer that ignored `ipv4_only` (forced `StunNetwork::Any`)
would reach `::1` even with `require_ipv4=true`, flipping **both** the exit/stdout
**and** the contact boolean.

## Profiles (10)

| profile | config | observable | proves |
|---|---|---|---|
| `req4-true-ipv6-fatal` | YAML `require_ipv4=true`, IPv6 STUN | exit 1, exact text, **0 IPv6 hits** | udp4 cannot reach `::1` |
| `req4-false-ipv6-serves` | YAML `require_ipv4=false`, IPv6 STUN | serves, exit 0, **IPv6 hit** | udp reaches `::1` |
| `req4-true-ipv4-serves` | YAML `require_ipv4=true`, IPv4 STUN | serves, exit 0, IPv4 hit | udp4 reaches `127.0.0.1` |
| `req4-false-ipv4-serves` | YAML `require_ipv4=false`, IPv4 STUN | serves, exit 0, IPv4 hit | udp reaches `127.0.0.1` |
| `cli-true-overrides-false-yaml` | YAML **false**, `--rtc.require_ipv4=true`, IPv6 STUN | exit 1, exact text, 0 IPv6 hits | **CLI override is load-bearing** |
| `cli-false-overrides-true-yaml` | YAML **true**, `--rtc.require_ipv4=false`, IPv6 STUN | serves, exit 0, IPv6 hit | CLI turns the gate OFF |
| `env-true-overrides-false-yaml` | YAML **false**, `LIVEKIT_RTC_REQUIRE_IPV4=true`, IPv6 STUN | exit 1, exact text, 0 IPv6 hits | **ENV override is load-bearing** |
| `env-false-overrides-true-yaml` | YAML **true**, `LIVEKIT_RTC_REQUIRE_IPV4=false`, IPv6 STUN | serves, exit 0, IPv6 hit | ENV turns the gate OFF |
| `local-path-no-stun` | `use_external_ip=false`, `require_ipv4=true`, IPv6 STUN listed | serves, exit 0, **no STUN contact** | local path bypasses STUN entirely |
| `default-unset-ipv6-serves` | `require_ipv4` key omitted, IPv6 STUN | serves, exit 0, IPv6 hit | default false → udp |

The CLI/ENV override profiles deliberately write the **OPPOSITE** value into the
YAML, so a consumer that ignored the flag/var would flip the exit+text and the
contact boolean. All profiles set `rtc.tcp_port=0` (the shared host may hold the
default ICE TCP port 7881), `bind_addresses=[127.0.0.1]`, and **omit `rtc.node_ip`**
so `use_external_ip` selects the STUN path and `use_external_ip=false` selects the
local-discovery path (`NodeIP.IsEmpty()`). Configs are emitted as **YAML strings**
(no PyYAML dependency); the STUN address is **quoted** (`- "[::1]:port"`) because a
bare `[::1]` is parsed as a YAML flow sequence.

### Deterministic ICE bind (loopback pin)

Every profile also sets `rtc.enable_loopback_candidate=true` and
`rtc.ips.includes=["127.0.0.1/32"]`, pinning the ICE media UDP mux to **IPv4
loopback** — exactly like the sibling `force-tcp-main` / `rtc-http-boundary`
launched-MAIN harnesses. Without this pin the mux enumerates **every** host
interface address (`interfaces.rs::media_interfaces_from` → Go `localInterfaces`)
and binds each one at `udp_port`. A rotating macOS **privacy global IPv6** can
deprecate between the Go launch and the Rust launch, so the later side aborts on a
now-unassignable address (`listen udp [2409:…]:port: bind: can't assign requested
address`). This is an **environment artifact, not a divergence**: Go
`createmux.go:58-84` (`return nil, err`) and Rust `udp.rs:55` **both** propagate the
bind error fatally with no graceful skip, so Go would abort identically had it
launched after the address deprecated. The pin removes the host-address
dependency and makes the alive rows deterministic regardless of host IPv6 state.

The pin does **not** touch the surface under test. `require_ipv4` is consumed in
`determine_ip → external_ip → stun_binding` during **config validation**, which
dials the configured STUN server directly and is independent of the ICE interface
filter; the alive-path contact boolean is guaranteed by that validation dial alone
(`udp` reaches `::1`, `udp4` reaches `127.0.0.1`). The ICE bind locality is a
separate surface (a different ledger item) and is explicitly **out of scope** here.

### Normalization (scope limit)

The **alive** observable is `{http reachable, exit==0, no SIGKILL}` plus the
deterministic STUN-contact boolean. The per-side shutdown timing
(`sigterm_to_exit_s`, second SIGTERM) and the **exact** STUN hit count
(retry/refresh dependent) legitimately differ between the Go and Rust event loops
and are **recorded but not compared**. The **refusal** observable is the exact
exit code, stdout bytes, and the zero-hit boolean. No comparison field is deleted;
these are explicit scope boundaries. `logging.level=error` suppresses the Go INFO
`found external IP via STUN` discovery log (which Rust does not emit) so it cannot
leak into the compared stdout/stderr.

### Shutdown calibration (measured, inherited from node-selector-main)

The alive rows settle `--settle` seconds (default 2) after the port first accepts
before signalling. `terminate()` sends SIGTERM, then a second SIGTERM, then
SIGKILL, and records which was needed.

## Run

```bash
# build the locked Go oracle (v1.13.10 e5655ac2) and the Rust candidate first
python3 run-paired.py \
  --go   /path/to/livekit-server-go \
  --rust /path/to/livecloud \
  --output /path/to/evidence-dir
```

Each side runs a fresh server per profile on reserved loopback ports against the
shared fake STUN responders: refusal profiles self-exit 1; alive profiles serve
HTTP then shut down on SIGTERM. The manifest `execution.json` records per-profile
go/rust observables, config sha256, binary sha256, captured stdout/stderr/log, and
the per-side IPv6/IPv4 hit deltas. Status `PASS_LAUNCHED_MAIN_REQUIRE_IPV4`
requires every profile to satisfy its contract on both sides, the cross-side
normalized observables **and contact booleans** to be equal, and all 26
cross-profile controls (13 per side) to hold.

## Falsifiability

Rust selects the STUN network in **one** place: `netdetect::stun_binding`
(`netdetect.rs:413`) chooses `StunNetwork::V4` when `ipv4_only` else
`StunNetwork::Any`. The positive control forces `StunNetwork::Any` regardless of
`ipv4_only` (the prohibited "accept-but-ignore" failure mode), built in an
**independent** target dir (`/tmp/c26-mutant-target`) so the clean candidate is
never overwritten.

That mutant makes the three refusal profiles genuinely **start**: with
`require_ipv4=true` the "udp" dial reaches the IPv6 responder, returns
`192.0.2.1`, and serves. It therefore FAILS exactly `req4-true-ipv6-fatal` +
`cli-true-overrides-false-yaml` + `env-true-overrides-false-yaml` on **three**
independent signals each (exit code, stdout bytes, and the IPv6-contact boolean
flipping `false→true`), passing the other seven alive profiles (they never
depended on a refusal). The byte-identical restore rebuilds to the honest
candidate and the final acceptance PASSes. The mutant FAIL receipt is kept under
`positive-control/mutant/` and is **not** cited as acceptance evidence.

## Out of scope (honest residual)

Real **public NAT traversal** / a routable external IPv4, **other platforms**, and
the **IPv6-only-host local path** (`require_ipv4=true` with `use_external_ip=false`
on a host with no local IPv4 → `no IPv4 address available`) are **not** claimed. On
this dual-stack host the local path prefers IPv4 for both true/false, so the local
profile here proves only that `use_external_ip=false` bypasses STUN entirely
(contact==none), not the IPv6-only local refusal. That residual stays recorded in
the ledger `accepted_scope` (with a lowercase `remains open` marker) and is not
absorbed into this batch's PASS. This harness proves the **startup
config-validation STUN-network-selection** surface only.