# Platform residual: en0 userspace-accept anomaly (SPEC-0077 C20)

**Status: OPEN — host-specific, below `std::net`, split from the C20 bind contract.**

This directory is reproducible evidence for a platform networking anomaly surfaced by the
C20 `bind_addresses` / `--bind` paired differential. It is **not** a `bind_addresses` defect
and is **not** introduced by the C20 changes. It is recorded here loudly so nothing is hidden
or deleted; the C20 harness (`../run-paired.py`) keeps the en0 HTTP-liveness data in every
observation and emits it under `platform_residuals`.

## What diverges

On this macOS dev host, a launched Rust MAIN binds the en0 IPv4 (`192.168.1.177`) correctly —
the TCP reachability matrix matches the locked Go oracle **exactly** on all 10 bind profiles
(the kernel completes the handshake on en0 for both sides). But Rust's **userspace `accept()`
never fires** for an en0-destined connection, so the HTTP / Prometheus / debug listeners never
serve it: `curl http://192.168.1.177:<port>/` hangs (rc 28, 0 bytes) while Go returns `200`.
Loopback (`127.0.0.1`, `::1`) serves normally on the **same** Rust socket.

## Root cause isolation (no livekit-rs code involved)

`reproducer.rs` is a bare `std::net` blocking-accept server (no tokio, no axum, no livekit
code). It binds five ways and parks; probe each printed port:

| bind mode | en0 dest | 127.0.0.1 dest |
|---|---|---|
| concrete en0, plain | TIMEOUT (no ACCEPT) | — |
| concrete en0, `SO_REUSEADDR` | TIMEOUT | — |
| concrete en0, `SO_REUSEPORT` | TIMEOUT | — |
| IPv4 wildcard `0.0.0.0` | TIMEOUT | **200 OK** |
| dual-stack `[::]` (V6ONLY=0) | TIMEOUT | **200 OK** (and `::1` 200) |

A separate bare **tokio 1.54.1** `TcpListener` reproducer behaves identically (en0 TIMEOUT, no
ACCEPT; loopback 200). So it is not tokio-specific and not an axum/router issue — it is Rust's
socket layer on this host.

Cross-checks run **simultaneously** on the same host, same en0 IP, different ports:

- **Python asyncio** en0 server (kqueue, `reuse_address=True`): **200 OK**.
- **Go** `net.Listen` en0 server (locked v1.13.10 oracle `4bc2378e`): **200 OK**.
- **Rust** en0 server: **TIMEOUT**.

`route -n get 192.168.1.177` → `interface: lo0`; `utun0..utun4` are present (Clash-style TUN).
The anomaly is specific to the non-loopback local destination IP and to the Rust process's
flows; Go and Python receive the same en0 flows fine. No network mutation was performed
(read-only diagnosis only; mutating routes/Clash/proxy is prohibited).

## Why this is split from C20, not a bind-contract failure

The `YAML:bind_addresses` / `base_flags:--bind` item observable is **listener creation**: which
`address:port` listeners exist, their family, the startup `bindAddresses` field, the empty-bind
ERROR + lifecycle, and CLI `--bind` precedence. The item's own `implemented_subscope` states it
is "not exhaustive resolver or network topology coverage". All of those bind observables match
Go on all 10 profiles (`bind_contract_status: PASS`). The en0 userspace-accept anomaly:

- is reproduced by a minimal bare-socket Rust program with **zero** livekit-rs code;
- affects **every** socket option and **all three** listener types equally;
- is independent of `bind_addresses` (the listener binds correctly; reach matches Go);
- is **not** introduced by the C20 fixes (FIX #1 wildcard dual-stack, FIX #2 empty-bind ERROR);
- is below `std::net`, so it is not fixable in livekit-rs application code;
- is host-specific (Go/Python serve en0 on the same host; production Linux nodes are unaffected).

It is therefore tracked as a distinct OPEN platform residual rather than counted against the
bind contract. The C20 items are certified VERIFIED **scoped to the bind contract**, with this
residual cross-referenced in their `accepted_scope`.

## Reproduce

```sh
mkdir -p /tmp/en0-repro/src
cp reproducer.rs /tmp/en0-repro/src/main.rs
cp Cargo.toml /tmp/en0-repro/Cargo.toml
cd /tmp/en0-repro
CARGO_NET_OFFLINE=true cargo build --release      # socket2 0.6.5 is in the workspace cache
./target/release/tokio-en0 "$(ipconfig getifaddr en0)"   # prints ports JSON on stdout, parks
# from another shell, probe each port:
curl --max-time 4 http://192.168.1.177:<en0_port>/        # hangs (000, rc 28)
curl --max-time 4 http://127.0.0.1:<v4wild_port>/         # 200 OK
```

`observed.json` records the actual results captured on this host. The full paired bind-contract
acceptance (with this residual reported per profile) is `../run-paired.py` → `BATCH.json`.