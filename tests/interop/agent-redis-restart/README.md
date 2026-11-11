# Actual AgentInternal Redis restart acceptance

Status: test source and runner only; not yet executed or accepted.

Run after the coordinator has compiled the current candidate's `lk-compat --lib`
test executable. Pass the executable from that exact source input, not an older
binary. The runner records its SHA-256 and the exact test name. It requires local
`redis-server` and `redis-cli` binaries; it launches and stops only its own
loopback broker and removes its own temporary directory. The evidence directory
must not exist before invocation.

```sh
python3 tests/interop/agent-redis-restart/run.py \
  --test-binary /absolute/path/to/current/lk_compat-TESTHASH \
  --evidence-dir /absolute/path/to/new-evidence-directory
```

One actual ignored Rust test executes. The initial broker has persistence
explicitly disabled. Before restart, two existing Cluster instances discover an
actual worker, complete availability and signed assignment, and register the
running job's termination topic. The broker is actually stopped, then started
again at the same owned endpoint. Without reconstructing services, reconnecting
the worker or replacing the original job, the test requires:

- Cache invalidation on disconnect and after replacement SUBSCRIBE ACK.
- Existing CheckEnabled subscriptions returning the real worker namespace.
- Existing request/claim/grant subscriptions assigning a second actual job.
- Existing JobTerminate routing the first job's termination to the actual worker.
- Fresh WorkerRegistered publication still invalidating the discovery cache.
- A second outage followed by bounded shutdown and zero subscription permits.

The runner does not accept a zero-test success. It retains stdout, both Redis
process logs, exit status, binary identity and commands in the requested evidence
directory. It performs no Cargo command or Git operation. This test does not
replace fixed Go paired acceptance, Sentinel failover, Redis Cluster node failure
or real official worker/SDK campaigns.

The transport deliberately does not replay dropped Pub/Sub messages or invent
business ACKs. Existing in-flight RPC work retains its original deadline; lost
messages can cause a genuine timeout. Overflow and oversized messages still
fail closed. Recovery runs inside the owning receive future, and shutdown or
retirement cancels it by dropping that future and its connection.

## Coordinator execution checkpoint

The coordinator executed `agent-redis-restart-attempt01`: one actual test passed,
exit 0, in 6.28 seconds; both owned Redis processes exited 0. Evidence is
`livekit-rs-docs/evidence/2026-11-04/spec0077-implementation/agent-redis-restart-attempt01/`
(relative to repository root). The tested executable SHA-256 was
`baebf8ebdd1067aa9108a0d7186bcccd961108fc5aa119504b062de480eb1cd1`.
The source-only status above is the original pre-run checkpoint. This run
predates the later Sentinel epoch hook, so it is not evidence that the newer
candidate, Sentinel migration or Go/Rust mixed pairings have passed.
