# Actual standalone server RPC policy and media RTT

Coordinator starts a fresh fixed Go server and a fresh candidate Rust server
separately with **no Redis configured**, default PSRPC3attempts/3s/2s timeout
backoff, explicit owned configuration/binary identities and actual media sockets.
Run this unchanged probe once per server, using its pinned interop dependencies:

```sh
INTEROP_PROJECT=/absolute/pinned/interop-project \
LIVEKIT_URL=ws://127.0.0.1:7880 RPC_DEPLOYMENT=standalone \
RPC_OUTPUT=/fresh/absolute/go-or-rust.json node run.mjs
```

No server, worker, broker or build is started by the probe. It owns only its own
Chromium, local HTTP SDK fixture, SDK participants and room. The fresh receipt
starts RUNNING before execution; interrupted or failed runs never count as PASS.

Two unchanged browser SDK2.23.0 participants exchange actual Opus audio. The
receiver must record over100actual inbound RTP packets. Actual server RPC handler
options are observed until a real nonzero RR media RTT reduces the10000ms wire
budget, bounded by the original20second observation budget; full configured
budget alone is not accepted as implementing the RTT reduction. This local
loopback profile can fail if the reference produces no nonzero RR observation;
retain that failure and add an actual owned media-delay subprofile rather than
fabricating RTT or replacing it with ICE, TWCC or signal RTT. Native/core tests
independently check exact RR calculation and5second gating.

The delayed original SDK method waits3500ms and records each actual request ID
and provided wire budget. Public PerformRpc has6000ms response budget: default
PSRPC's first3second attempt expires, second5second attempt succeeds. The probe
requires exactly2different actual IDs, both accepted SDK handlers finishing, a
correct final payload and genuine RR reductions on the wire. The backoff extends
per-attempt timeout; it is not a sleep. Root's paired receipt must preserve
process exits, all final SDK/browser cleanup, exact server/config/SDK/probe hashes
and the two separate Go/Rust results. This probe is prepared, not executed here.
