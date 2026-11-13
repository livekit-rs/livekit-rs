# F05 Agent Worker control and room bridge implementation contract

Fixed server v1.13.10 e5655ac20267fa5005dccaf3f0e3bfc3ea688c07 and protocol9bb708cdb81918a6c63dd25e915b4644310b45b6. Apache-2.0 source notices retained. Registry, WebSocket codec/auth/token, service assembly and room bridge are integrated source. Coordinator execution is pending; this document makes no test-PASS claim.

## Actual source contract

- `/agent`: check WebSocket upgrade first (non-upgrade404), then actual `Video.Agent` authentication401. Allow any origin because bearer grants provide auth. Parse optional protocol query as signed Go int, default1 on parse failure. Honor `agent_signal_message_size_limit` on complete messages (positive bound, zero or negative disables).
- Handshake accepts ping/pong or register during fixed10-second read deadline. A register type must be0/1/2. Validate deployment as byte string; preserve explicit permissions versus default subscribe/publish/data/updateMetadata true. Clear registration read deadline only after registration response successfully writes. Duplicate register after handshake, unknown oneof or unsupported pre-register signal closes control. JSON text changes response codec to JSON; subsequent binary changes it back to protobuf.
- Actual WS path has no data-plane session, therefore ignores declared endpoints and does not return endpoint_settings. It must not invent HTTP endpoint support. WT/endpoint tunnels are a separate outstanding feature.
- Worker identity is connection-scoped generated `AW_`; group key is actual agent_name/namespace/job_type/deployment. Nonzero status seq<=last is ignored including status/load/draining; seq0 always applies without resetting last seq. Source selection uses statusAVAILABLE and weighted max(0,1-load), not a hard target_load cutoff; draining currently affects endpoint dispatch but is not an additional job selector condition.
- Assignment clones job, records RUNNING timestamps before availability request and waits10seconds after its successful write. Duplicate pending job is rejected. Unknown/duplicate availability is ignored. available=false declines; terminate=true is the explicit source-defined SUCCESS terminal response without assignment. Accepted availability supplies participant identity/name/metadata/attributes; token factory must issue real source-equivalent one-hour AGENT roomjoin token with lk.agent_name attribute and permissions. No placeholder token or fabricated successful participant.
- Insert running state before actual assignment write to permit immediate worker UpdateJob; roll back if real write fails or caller is cancelled before it completes. Queue acceptance alone is not a successful network write: the bounded outbound channel carries a one-shot actual writer result. Tokio/WS caller must complete that result only after real sink send success/failure.
- Worker UpdateJob status/end/error timestamps mirror source and remove SUCCESS/FAILED jobs. Unknown job update is handled nonfatally by connection handler. Termination writes frame then updates source terminal state even if the write failed; the operation still returns that failure. AGENT_LEFT_ROOM is FAILED with exact source error. Disconnect cancels all pending replies and removes running ownership without inventing job success; source itself has a TODO for dispatch-state persistence on disconnect.

## Ownership and integration

Registry owns only worker lifecycle and bounded pending/output channels, with no lock held across network waits. Pending and unsent-running RAII guards are generation-aware so cancellation cannot remove a later same-ID request. Internal explicit capacity errors bound resources; capacity policy needs coordinator review before claiming full-load source equivalence. HTTP auth, codec switching, actual message writer and room-dispatch effects are separate adapters under one AgentService owner. Runtime channels are bounded and preserve work. The room owner sends queued effects by reserving channel capacity in the same select loop as its control inbox and media input; a full agent channel cannot block owner RPC processing. Final effect drain also services its inbox. The bridge never awaits an owner RPC in its event-consumer loop: assignment/real-commit tasks share a 50-task limit, cleanup has a separate 50-task limit. When its launch queue reaches 4096, it pauses reading further effects (one effect's bounded namespace fanout may occupy up to 4096 additional entries); accepted launches are not silently discarded. This breaks the bounded-channel/owner-RPC cycle without replacing real assignment success with queued work. Small-capacity tests require every one of 80 queued launches to produce actual worker availability/assignment writer receipts and an owner-committed job.

Room bridge launches room jobs at dispatch creation, participant jobs at first non-resumed Join, publisher jobs once the first actual track publishes. Adding a dispatch backfills current participants and publishers. Dependent AGENT/EGRESS/agent-grant/recorder participants do not recursively launch jobs. Launch discovers current worker namespaces; registration does not retroactively retry historical dispatches. Only an actual accepted assignment with writer receipt is committed to dispatch jobs. Dispatch deletion and room close terminate accepted jobs, including late results, with room SID identity guards. Both already-owned and late accepted jobs use completed real termination before capturing the participant generation. Fixed three-second participant removal also checks participant SID, avoiding destruction of a replacement session.

Source `JobRequestAffinity` uses agents.target_load only for distributed routing. Local worker selection uses `max(0,1-load)` and must not add a target_load cutoff. ServiceConfig preserves target_load, and the distributed job-affinity consumer is now present in `cluster.rs`: a real `RequestAffinity` over `registry.affinity` (`max(0,target_load-load)` summed on AVAILABLE workers, NaN-preserving) registered per worker-key topic, with PSRPC cross-node worker discovery/dispatch — see the 2026-11-05 continuation below; it is production-wired (`server.rs:226` → `service.rs:130` → `cluster.rs:346-355`) and matches fixed Go `agenthandler.go:449-465` line-by-line, certified at component level by real single+cluster Redis cross-node affinity/dispatch/discovery tests on candidate `59b3546` (docs `evidence/2026-11-14/spec0077-a02-agent-affinity/`); the main-executable launch and paired locked-Go differential remain separate open gates. WT/data-plane endpoint (max-streams-weighted node) affinity, job retry/restart beyond the fixed local candidate loop, and the source disconnect dispatch-state TODO are not claimed implemented. WS declared endpoints intentionally remain ignored like the fixed WS source.

Internal limits (4096 workers, 256 pending and 4096 jobs per worker; 50 concurrent launch futures; bounded control channels) produce honest capacity failure. They are resource policy, not proof of matching upstream overload behavior. Shutdown first signals worker watches and closes registry state; bridge task abort cancels pending assignment RAII guards. Generation identity and actual network-write acknowledgements remain mandatory.

## Required proof before delivery

1. Actual same fixed Go and Rust WebSocket transcript: no-upgrade404; missing/invalid/agent grant; proto/JSON switch; ping before register; unknown type/deployment; fixed deadline; endpoint fields on WS; status seq0/stale. Compare actual messages, codes and closes, avoiding generated-writer-only assumptions.
2. Source-derived worker-state oracle: positive/negative availability, duplicate pending, unknown response, real assignment output, status transitions, termination, disconnect and backpressure cancellation. Time is injectable for exact source timestamp comparison.
3. Unmodified official Python or JS agent worker registration and accepted job; real assigned token connects an official RTC SDK as AGENT, then leaves/terminates. A wire probe alone is insufficient SDK evidence.
4. Actual room dispatch CRUD and close integration, generation/cancel negatives, and token claims source oracle. No worker/no availability must leave jobs pending or fail honestly.
5. All existing functional API/TLS/SVC regressions remain; ordinary SDK clients require no changes. Performance thresholds are not a completion gate for this functional slice, but bounded memory and lifecycle correctness remain required.

## 2026-11-04 unaccepted endpoint candidate

The preceding WT/data-plane exclusions describe the accepted baseline. New
`endpoint/` source and `wt_control::session_with_endpoints` now contain a local
HTTP-over-WT implementation candidate, including real session openers and stream
leases, manifest/settings negotiation, scoped endpoint grants and an independent
HTTP middleware chain. This candidate has **not been compiled or tested** because
the coordinator resource gate remains closed. See `endpoint/README.md` for exact
source contracts, required nonzero-test commands and retained gaps. It does not
close A03/W05, WebSocket tunnels, distributed endpoint or job affinity.

## 2026-11-05 AgentInternal / endpoint continuation candidate

`cluster.rs` now contains the real AgentInternal typed RPC graph and room-bridge
consumer, using exact rpc/agent.proto, actual multi CheckEnabled, claim affinity,
assignment writes/tokens and job-id termination topics. Production activates it
with the node's existing Local/Redis bus; `cluster/README.md` defines defaults,
readiness, lifecycle and the still-unverified acceptance scope. A `target_load`
consumer is therefore present in source, production-wired and component-certified
by real single+cluster Redis cross-node runs on candidate `59b3546`; the
main-executable launch and paired locked-Go differential remain separate open gates.

The endpoint continuation adds transparent WebSocket upgrades and raw frame
splicing, actual per-worker tunnel admission (default max_streams/2, minimum1),
frame-boundary1001 draining and owned early-start/force/shutdown drain tasks.
The ordinary HTTP max_streams cap stays soft. Informational responses are now
bounded to8 and invalid final statuses rejected; forwarding allowed1xx heads to
the final HTTP client remains a separate Hyper-server integration gap.
