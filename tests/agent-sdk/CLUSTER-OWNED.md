# Original official Agent SDK cross-node ownership matrix

Run on the already provisioned owned Linux VM as its nonroot fixture user. No build, install, protocol simulator or SDK modification is performed. Reuse the existing installed @livekit/agents **1.9.4**, @livekit/rtc-node **1.2.1** and livekit-server-sdk **2.19.2**. The existing OwnedProcessTree implementation is imported from source-root/tests/interop/egress-linux/run-owned-stack.py and its hash recorded.

```
python3 /absolute/script-overlay/tests/agent-sdk/run-owned-cluster.py \
 --source-root /home/fixture/fixture/source \
 --go /absolute/qualified/go --go-sha256 EXACT_GO_SHA \
 --rust /absolute/qualified/rust --rust-sha256 EXACT_RUST_SHA \
 --agent-sdk-root /absolute/installed/agent-sdk-project \
 --interop-project /absolute/installed/interop-project \
 --output /home/fixture/fixture/output/fresh-agent-cluster-matrix
```

The probe defaults to run-worker.mjs beside this runner. `--probe` explicitly selects another source-identified copy. `--only 0-go-rust` etc. requests one diagnostic case; its receipt is never a full12 completion claim. The complete matrix is each genuine ROOM/PUBLISHER/PARTICIPANT job × GoGo/RustRust/GoRust/RustGo. Every case creates new broker and two new servers with fresh private loopback ports and public fixture credentials; inherited LIVEKIT/Redis/runtime injection variables are absent.

Optional fixture variables added to run-worker.mjs:

- AGENT_WORKER_LIVEKIT_URL changes only AgentServer ServerOptions.wsURL to entry B. REST, native human participant and source RoomServiceClient remain on LIVEKIT_URL (A).
- AGENT_TARGET_NODE_ID pins original public SDK CreateRoom.nodeId to actual node A.
- AGENT_ROUTING_OBSERVER / AGENT_ROUTING_RELEASE pause before dispatch until actual ownership and worker admission evidence exists.
- AGENT_TERMINATION_RELEASE pauses after the actual job, child RTC join and all positive data/metadata assertions, before original DeleteDispatch. The runner first proves B's real JobTerminate admission.

These optional gates control fixture timing only. The official worker, generated Job, acceptance, signed token and RTC implementation are unchanged. Without optional variables the original single-node flow remains available.

Ownership proof is real Redis/process evidence: start servers one at a time, record exact HKEYS nodes differences with raw node SHA, record executable identity/PID/birth, and require room_node_map[actual_room] == node A. A passive MONITOR connection retains **only** actual AgentInternal JobRequest/JobTerminate SUBSCRIBE metadata and source-line hashes, never raw PSRPC payload/job/token frames. Its Redis client source port must match a TCP socket inode held by node B's /proc/PID/fd and the actual connected /proc/PID/net/tcp entry; Redis CLIENT LIST must identify that same active pubsub client. The resulting proof binds worker server B, its Redis node ID and actual registered named/job topic. It never writes a fake worker registry or implements a worker protocol. Node A and B IDs must differ.

The original probe still requires non-fake real child process, correct job type and target at request/child/retained-job layers, AGENT participant kind, SDK-signed join, exact attributes/metadata, one positive acceptance, actual publisher native audio RTP for job1, real termination callback, participant departure and PID ESRCH. Parent native RTC must dispose for job1/2. The owned tree must observe the real child PID and require zero surviving descendants without reclamation. Node/server/broker exits must all be0, configs removed and all owned ports released; emergency SIGKILL or reclaimed descendants fail acceptance. Original and new failures remain per-case and do not turn into success after cleanup.

Source preparation is **not** acceptance. Required execution is all12 with immutable qualified binary/source IDs and the complete current same-source regression; subset or historical passes cannot close the parent compatibility item. Source/SDK locks and binaries are hashed before/after. No Cloud Agent models or AI pipelines are claimed.


## Live owner / no-shadow proof (script22)

Every node additionally gets a distinct fresh `debug_handler.port`, included in
owned TCP port release gates. Before dispatch termination, after original SDK
positive assertions and actual child RTC join, the runner reads each independent
`/debug/rooms` endpoint. Each listener must belong to its exact server PID's
socket inode. Node A must contain exactly one room with the original created SID
and the real agent identity/participant SID; job1/2 also require its human.
Node B must have no same-name room. Both raw snapshots and their hashes are
persisted even when assertions fail. This rejects a shared Redis owner mapping
that conceals an entry B shadow room.

Pinned official field contracts: `pkg/service/server.go:426` emits an array of
local room manager entries; `pkg/rtc/room.go:1846` supplies Name/Sid and the
identity-keyed Participants map; `pkg/rtc/participant.go:3945` supplies ID/State.
Rust's real generation-bound producer is `crates/livecloud/src/debug.rs:493`.
This is the same independently checked surface as cluster-room-api/probe.mjs:54.
No SDK media or lifecycle requirement is reduced; these are added conjunctive
gates, and source preparation still supplies no execution credit.


## Ubuntu Redis multicall executable setup (script23)

Preserve the original `redis-server` launch entry and argv[0]. Ubuntu may ship
`/usr/bin/redis-server` as a symlink whose resolved executable is named
`redis-check-rdb`; Redis dispatches its mode from argv[0]. Resolving before exec
would start the RDB-check CLI rather than a broker. The runner now records both
original entry and canonical executable/SHA, launches the original entry, and
still verifies `/proc/PID/exe` against its canonical target. All actual PING,
registration, live owner/no-shadow, SDK and release gates remain strict.
The earlier Usage/readiness timeout is setup failure and supplies no Go runtime
compatibility result. Use `--redis-server /usr/bin/redis-server`, not the resolved
`redis-check-rdb` name.
