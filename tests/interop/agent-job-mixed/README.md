# Fixed Go / Rust mixed AgentInternal job lifecycle

Status: harness source only, not compiled or executed. No ledger upgrade follows
from this document. Fixed source inputs are LiveKit Server v1.13.10
`e5655ac20267fa5005dccaf3f0e3bfc3ea688c07` and protocol
`9bb708cdb81918a6c63dd25e915b4644310b45b6`.

Build the probe with the existing pinned Go module and unchanged official
protocol, auth, protobuf, Twirp and WebSocket packages:

```sh
cd tests/interop/agent-wt-diff/goclient
go build -mod=readonly -o /absolute/path/to/agentjob ./cmd/agentjob
```

The Go program is a controlled worker harness using official wire definitions;
it is not an assertion that every official Agent SDK or actual AI provider has
been accepted. It registers over ordinary `/agent?protocol=1` WebSocket using
binary WorkerMessage/ServerMessage with the fixed official Go generated types.
It performs an ordered UpdateWorkerStatus then Ping/Pong barrier, responds to
actual Availability, verifies the real Assignment JWT using the official token
verifier and checks the actual worker receives JobTermination after deletion.

From `livekit-rs-server`, run all four source pairings:

```sh
python3 tests/interop/agent-job-mixed/run-paired.py \
  --go-server /absolute/path/to/pinned-livekit-server \
  --rust-server /absolute/path/to/current-livecloud \
  --probe /absolute/path/to/agentjob \
  --evidence-dir /absolute/path/to/new-evidence-directory
```

The runner starts two real server processes against one fresh owned Redis
broker per case: Go/Go, Rust/Rust, Go-owner/Rust-worker and
Rust-owner/Go-worker. Both actual workers share agent, namespace and deployment.
A high-load worker registers on node A; a lower-load worker registers on node B.
CreateRoom is directed to A's registered node ID, and public CreateDispatch is
sent to A. The losing high-load node must receive no availability/assignment;
the real lower-load remote node must complete the job. ListDispatch must expose
that actual worker ID and running job before public DeleteDispatch reaches it.

The fixed `pkg/agent/worker.go:AssignJob` sends Job and Token and leaves the
optional Assignment URL absent. The probe checks that exact absence; the real
worker's connection URL remains its fallback. It does not fabricate a room URL
field to claim an extra behavior. JWT checks include the real signature,
issuer, returned availability identity, room, RoomJoin and Agent grants.

All node IDs must differ. Every case has unique real room/agent/namespace/job
identity, a fresh Redis keyspace and independently selected ports. The runner
stores server/broker logs, normalized observations, exact commands, configuration
using only the committed public fixture credential, exit statuses, binary
SHA-256s and probe SHA-256. It does not log minted JWTs. It stops only its own
process handles, removes its temporary configs, and refuses acceptance when an
owned process requires forced kill or exits uncleanly. It runs no build or Git
command. A failing Go/Go baseline is a real harness/oracle failure, not permission
to accept the Rust pairings or relax assertions.

Remaining coverage includes full official Agent SDK campaigns, actual agent
media/participant sessions, Redis Sentinel/Cluster failover and provider workers.
This test covers AgentInternal discovery, node claim affinity, assignment and
remote termination through production API/WS entry points.
