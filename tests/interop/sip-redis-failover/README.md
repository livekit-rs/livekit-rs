# Original SIP worker subscription recovery after live Redis promotion

Run the unmodified original SIP worker (v1.21.0,
`ee19da1a7ef178593e232a27365de7b4ba8f8829`) against the clean Go v1.13.10
server (`e5655ac20267fa5005dccaf3f0e3bfc3ea688c07`) and the candidate Rust server:

```sh
python3 tests/interop/sip-redis-failover/run.py \
  --go /absolute/path/to/fixed-go-server \
  --rust /absolute/path/to/livecloud \
  --worker /absolute/path/to/livekit-sip \
  --output /absolute/path/to/new-output
```

Use Python 3.11+, Redis Server and CLI with Cluster/Sentinel support, Node with
the committed interop lock installed, and the Chrome channel used by the
existing SIP media probe. The worker requires its normal native dependencies.
All servers are fixture-owned. The worker binds its SIP/RTP endpoints on
loopback, and the script runs its profiles serially to share the fixture's
local RTP port range.

Each profile creates and decodes audio on a real outbound SIP dialog before
fault injection, with the unchanged browser and Server SDKs. The orchestrator
locates the worker's actual per-call Redis `REQ` and `RCLAIM` channels. It
then performs one real promotion:

- Sentinel promotes its live replica, the former primary exits, and the
  channels must reappear on the promoted primary.
- Cluster promotes the replica for the shard hosting the worker's Pub/Sub
  socket. After the prior primary demotes, Redis closes the subscriptions on
  their original node using `CLIENT KILL TYPE PUBSUB`. The old primary stays
  reachable as a replica for stale slot maps and MOVED redirects. Legacy
  Redis Pub/Sub is global, so the same channel may reappear on the same
  address under a new connection, or on another node.

The original media process and worker must remain alive. The restored call
must decode 440 Hz and 880 Hz in opposite directions using newly received
packets and complete the same real REFER, target INVITE/ACK, NOTIFY, original
BYE, participant departure and cleanup assertions as the standard SIP probe.
The driver records the exact channel observations and exit codes, and
checks its TCP and UDP listener release.

This covers the original SIP worker's active per-call subscription and media.
Cluster hard primary loss, ingress/egress workers, TLS and active jobs outside
this profile require their own paired executions.

## Exact-slot hard-stop comparison

Use the same pinned Go and SIP binaries and unmodified SDK packages. Run the
two implementations serially with an owned Redis Cluster over verified
mTLS. The driver chooses a room whose participant catalog slot is on a
different primary from the fixed `rooms` slot. It stops the selected
primary with SIGKILL and checks its listener, the promoted owner on all
survivors, and restoration of the original worker's exact per-call
subscriptions before releasing the real media/SDK probe.

```sh
python3 tests/interop/sip-redis-failover/compare-hard-stop.py \
  --go /absolute/path/to/fixed-go-server \
  --rust /absolute/path/to/livecloud \
  --worker /absolute/path/to/livekit-sip \
  --output /absolute/path/to/new-output
```

Each repetition runs two immediate slot profiles and a separate 65-second
settled participant-slot control on each server. The output retains the
original strict exits, and reports attribution separately. An attributed
difference still produces a nonzero strict exit. Every trial records
SDK response, request digest and exact dedup slot, worker SET NX and RCLAIM,
actual REFER and target exchange, cleanup calls and independent reads on
promoted Redis. SIP dialog teardown after failed transfer is recorded
separately from successful REFER.

Fresh PSRPC request IDs vary the dedup slot. The immediate run retains
the actual slot and observed branch. The separate original PSRPC queue
control under the SPEC-0077 SIP evidence pins the dedup slot for both
branches. Only the real SDK trial can satisfy the end-to-end SIP assertions.
