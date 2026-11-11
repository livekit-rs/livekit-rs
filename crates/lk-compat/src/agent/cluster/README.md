# AgentInternal production consumer candidate

Status: implementation and test source only; coordinator must compile and run it.
This is not evidence of passing Redis/official-worker compatibility.

Pinned sources: LiveKit v1.13.10 e5655ac20267fa5005dccaf3f0e3bfc3ea688c07,
protocol 9bb708cdb81918a6c63dd25e915b4644310b45b6. Exact unmodified
`protobufs/rpc/agent.proto` is now part of the generated schema lock.

The production `AgentService::enable_cluster_rpc(bus)` uses the existing node's
message bus, including the same local membership or Redis endpoint/connection/
admission slots. The generated-client inbox view is 100. Initial CheckEnabled
and WorkerRegistered subscriptions acknowledge readiness before activation.

Source mapping:

- `agent/client.go:NewAgentClient/getDispatcher/checkEnabled/GetAgentTopic`:
  CheckEnabled multi responses feed per-type namespace/agent-name discovery
  incrementally. Namespace launches do not wait out the five-second window.
  A completed result caches for one minute from fetch start and WorkerRegistered
  invalidates it. Concurrent cold discoveries currently perform separate bounded
  CheckEnabled calls; they do not create a shared incremental dispatcher.
- `service/agenthandler.go:registerWorker/deregisterWorker`: actual worker groups
  own JobRequest topics. WS/WT production registration uses `register_ready`
  so the actual broker subscription is ready before the Registered event. Group
  topic collisions cannot silently combine differently named worker groups.
- `JobRequestAffinity`: AVAILABLE workers contribute max(0,target_load-load),
  preserving NaN propagation. Draining is not an extra job-affinity condition.
  Local worker choice continues to use max(0,1-load), not a target-load cutoff.
- `AgentInternal.JobRequest` requires the real claim/grant affinity path. Actual
  Availability/Assignment writes and the original signed token minter are used.
  The running job's JobTerminate topic only becomes eligible after Assignment
  writer acknowledgement; pending writes cannot advertise a finished assignment.
- `JobTerminate` uses its actual job-id queue topic and actual worker owner.
  A room on another node commits the returned real state, and dispatch deletion
  routes termination back over that topic rather than fabricating a local worker.
- Registry changes drive handler retirement. Subscriptions, cache invalidation,
  incremental discoveries and retirement tasks have owners and shutdown joins.

Actual fixed policy applicability: NewAgentInternalClient/NewAgentInternalServer
are constructed without ClientParams/ServerParams options in the pinned Go graph.
Client/server default timeout is 3s, affinity selection is 1s with 200ms short
circuit, CheckEnabled explicitly uses 5s. Do not apply the separate IO/worker
ClientParams generic retry policy or claim these psrpc YAML fields affect this
service when the source does not consume them.

Coordinator commands from livekit-rs-server (require exit status and a nonzero
executed-test count for each selected filter):

```
cargo test -p lk-compat --lib agent::cluster::tests -- --nocapture
cargo test -p lk-compat --lib agent::service::tests
cargo test -p lk-compat --lib agent::registry::tests
cargo test -p lk-compat --lib agent::wt_control::tests
cargo test -p lk-compat --lib agent::websocket::tests
cargo test -p livecloud --lib webtransport::tests
```

New tests use real PSRPC envelopes over shared local transport, independent node
registries, actual claim selection and actual assignment token verification. One
starts from the public RoomManager dispatch API on node A, discovers a worker only
on node B, commits the genuine running state on A and routes dispatch deletion
back to B. Another completes CheckEnabled caching, observes WorkerRegistered
invalidation and retires the departed group's actual handler.

Remaining acceptance: unchanged official worker/control transcripts and fixed-Go
paired Redis multi-node CheckEnabled/claim/grant/JobRequest/JobTerminate; Redis
restart/rejoin, late cancellation/unknown delivery outcome, worker replacement,
namespace/type/deployment combinations and same final-source regression. The Go
source's own dispatch-state TODO on worker disappearance is not silently solved
by a fake job-state update. Endpoint cross-node fallback is not activated by the
pinned OSS graph and is not a prerequisite for this AgentInternal consumer.

## Root-executable real Redis variants

The same three-node claim/assignment/token/termination and public room-dispatch
scenarios now accept RpcBus fixtures. Explicitly ignored real Single and Cluster
variants have no environment fallback or silent skip. Use only an isolated,
coordinator-owned Redis deployment; these use the real canonical global
CheckEnabled/WorkerRegistered channels. No service-name rewrite, FLUSHDB, process
kill or shared-key cleanup is performed.

From livekit-rs-server, with the owned endpoints supplied by root:

```
LIVEKIT_AGENT_REDIS_SINGLE_URL='redis://127.0.0.1:PORT/0' cargo test -p lk-compat --lib agent::cluster::tests::actual_single_redis_agent_affinity_assignment_and_public_dispatch -- --ignored --exact --nocapture --test-threads=1
LIVEKIT_AGENT_REDIS_CLUSTER_URLS='redis://127.0.0.1:P1/0,redis://127.0.0.1:P2/0,redis://127.0.0.1:P3/0' cargo test -p lk-compat --lib agent::cluster::tests::actual_cluster_redis_agent_affinity_assignment_and_public_dispatch -- --ignored --exact --nocapture --test-threads=1
```

Each command must report exactly one executed test and exit0. Each creates fresh
random isolation values for agent name, namespace, deployment, worker IDs, job ID,
room and dispatch. The isolation value is printed without Redis credentials.
Real CheckEnabled discovery may legitimately report unrelated namespaces; the
fixture waits for its own namespace rather than falsely asserting a single
cluster-wide namespace. Job selection/assignment must still go to its low-load
worker, and the losing node must receive no Availability/Assignment side effect.

Success cleanup closes incremental discoveries, awaits every cluster controller,
RPC handler/retirement and fake worker actor, removes the owned room/dispatch,
stops the task-owned disabled-media routers and drops Redis handles. The external
Redis processes remain root-owned and running. A test panic is a failure; do not
reuse its result as acceptance. Root should inspect subscription/owned-process
cleanup after failed campaigns before rerunning. The worker writer is a controlled
ACK/Availability/Assignment fixture with real signed-token verification; this
proves real Redis AgentInternal business consumers, not an unchanged official
worker/SDK media campaign.

Existing fixed-Go transport harness to reuse:
`tests/interop/agent-wt-diff/run.py` plus
`goclient/cmd/wtdiff/main.go` already use canonical webtransport-go and framed
Register/ServerInfo/close probes. They do not currently drive AgentInternal
Availability/Assignment/termination or endpoint HTTP; those capabilities must not
be claimed from their existing register-only pass. `tests/interop/local-agent-dispatch/run.py`
checks fixed-Go/Rust API record/owner lifecycle but does not supply a real worker.

## Attempt 05 failure triage (not acceptance)

The coordinator's actual run exposed an invalid deployment in the affinity
fixture (`_` is reserved), plus empty discovery and dispatch timeout. The
fixture now uses a legal hyphen separator. CheckEnabled uses the fixed
`pkg/agent/client.go:CheckEnabledTimeout` five-second call, so that method's
server wire-deadline admission now accepts five seconds instead of rejecting
it through the ordinary three-second limit. Other methods retain three
seconds. Actual assignment, token, remote owner and termination assertions
remain unchanged. Fresh execution is required; Redis and Go paired acceptance
remain open.

## Checked recovery checkpoint

Coordinator workspace attempt 07 passed all library packages; the separate
`agent-redis-restart-attempt01` executed one actual test and passed (exit 0;
6.28 seconds; both owned Redis processes exit 0). Tested executable SHA-256:
`baebf8ebdd1067aa9108a0d7186bcccd961108fc5aa119504b062de480eb1cd1`.
The evidence directory is under
`livekit-rs-docs/evidence/2026-11-04/spec0077-implementation/`.
The business `google.protobuf.Empty` descriptor now selects the public schema
pool; internal envelopes keep their separate PSRPC pool. This fixes actual
local WorkerRegistered invalidation without inventing an internal payload.
Later Sentinel epoch hooks and mixed Go/Rust fixtures require fresh acceptance;
this checkpoint must not be reused as evidence for those source inputs.

## Mixed-language termination correction

Candidate 02's actual mixed campaign accepted Go/Go, Rust/Rust and
Go-owner/Rust-worker, but Rust-owner/Go-worker timed out specifically waiting
for JobTermination after public DeleteDispatch. The fixed generated
`protocol/rpc/agent.psrpc.go` declares
`RegisterMethod("JobTerminate", false, false, true, true)`; the argument order in
`psrpc/pkg/info/info.go` is affinity-enabled, multi, require-claim, queue.
Terminate therefore requires claim/grant even though it has no affinity
function. The candidate now sets claim=true and queue=true; default affinity
remains 1 and no JobRequest load function is applied to termination.
The previous Rust receiver's direct response let the inverse mixed direction
pass and concealed the incorrect method shape. The real Go receiver exposed it.
A generated-method-shape regression was added and the full unchanged remote
termination assertion remains. Fresh mixed execution is required for this fix.
Agent bridge launch, state commit and termination failures also now report the
actual job ID and error, without token contents, instead of silently dropping
failed work.

Candidate 03's four-way campaign then showed a second real consumer mismatch:
after enabling the correct termination claim, both Rust-owner pairings timed
out. The fixed PSRPC `pkg/client/options.go:57-65` selects the first available
claim for methods without an affinity function and applies the one-second
selection window. Terminate now explicitly consumes those defaults:
`accept_first=true`, affinity timeout one second, request timeout unchanged at
three seconds. The neutral generic CallOptions defaults are not evidence that
a generated consumer's defaults have been applied. Existing real local/Redis
termination and all four mixed directions must be rerun on the next candidate;
no previous mixed pass is reused for this input.
