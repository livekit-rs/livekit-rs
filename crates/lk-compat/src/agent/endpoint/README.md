# A03 / W05 local endpoint candidate — UNCOMPILED, NOT ACCEPTED

This candidate is source work, not a completed delivery. No Cargo build, test,
Go probe, server or real worker campaign has been run: the coordinator's resource
condition is not met. Formatting and static diff checks are the only checks run.
Do not upgrade any capability ledger item from this document alone.

Fixed source: LiveKit v1.13.10 e5655ac20267fa5005dccaf3f0e3bfc3ea688c07;
protocol 9bb708cdb81918a6c63dd25e915b4644310b45b6. Oracle files:

- `pkg/agent/endpoint/{manifest,registry,scope,routes,front,attempt,webtransport}.go`
- `pkg/agent/endpoint/router/template.go`, `wire/{protocol,exchange,streamcode}.go`
- `pkg/agent/worker.go:EndpointRegisterHandler`
- `pkg/service/{agentwt,agentendpoint,agenthandler,server}.go`
- `protocol/auth/grants.go:AgentEndpointGrant.Allows`

Candidate consumers:

1. A real WT session supplies `session_with_endpoints` an owned opener for that
   exact session. Registration validates the manifest/instance/protocol and
   returns protocol 1 only when a data-plane opener exists. Existing WebSocket
   registration and the old control-only entry point continue to withhold it.
2. Registry state is scoped by authenticated API key, agent name and deployment.
   Drain and disconnect consume the actual worker registry changes. Each opened
   HTTP stream holds one lease, released on driver completion/cancellation/drop.
   The session close fence rejects new opens, cancels pending stream/header opens
   and resets an allocated stream before returning a close error.
3. `/agents/` has its own enabled-only middleware dispatch. Its bodies stream
   without the API body limit, while all other paths and a disabled front keep
   the original API chain. Only this front adds PUT to CORS. Path normalization
   executes once. The actual HTTPS listener supplies the preamble scheme marker;
   an absolute request URI or forwarded header cannot claim TLS.
4. Real local HTTP is carried as a length-prefixed protobuf StreamPreamble and
   HTTP/1.1 on a server-opened WT bidi stream. Hyper drives upload/download
   concurrently; body collection is not used. Reserved signalling headers are
   stripped, bearer credentials do not reach the worker, and non-success body
   completion or truncated framing produces a body error. Head/preamble waiting
   has a bounded deadline and observes session cancellation.
5. `max_streams` zero takes 256. This is a soft headroom denominator, NOT a hard
   stream semaphore. Inflight stream counts and spare capacity are real state;
   local HTTP worker selection follows the fixed power-of-two choices by
   outstanding streams. Max-streams-weighted distributed node affinity is NOT
   activated here and remains open. No artificial local occupancy-ratio selector
   was added to create a configuration effect.

Coordinator checks after the resource gate opens (from livekit-rs-server; retain
command exit status and **nonzero executed test count**, no zero-test passes):

```
cargo test -p lk-compat --lib agent::endpoint::tests
cargo test -p lk-compat --lib agent::wt_control::tests
cargo test -p lk-compat --lib auth::claims::tests
cargo test -p lk-compat --lib http::tests
cargo test -p lk-compat --lib agent::service::tests
cargo test -p lk-webtransport --test webtransport
cargo test -p livecloud --lib webtransport::tests
```

New test source covers manifest converters/methods, exact JWT grant rejection,
soft-cap overcommit and lease release, reconnect epochs, actual in-memory HTTP/1
bidirectional streaming before the upload completes, response-drop cancellation,
real control framing/settings negotiation, independent CORS/body-limit chains,
and owned WT opening on the existing real QUIC session fixture. These are not
substitutes for a fresh unchanged official worker/SDK + pinned Go paired campaign.

Known remaining gaps / acceptance limits:

- No compiled result exists yet; all new tests and affected old tests must run.
- Actual pinned-Go/real WT endpoint worker body, cancellation and error/reset
  transcripts remain untested. Generic IO worker fixtures do not prove QUIC wire
  equivalence or session-close reset-code ordering for active endpoint streams.
- Advanced router over-budget fallback, exhaustive merged-route precedence,
  invalid UTF-8 percent paths, all ambiguity cases and exact diagnostics require
  the source conformance campaign. Regex matching is a candidate implementation.
- Completion/reset-aware safe retries after dispatch, informational-head count
  limits, exact response head boundary/header merge behavior remain open. Opening
  failures can select another local worker; this is not the full Go retry table.
- Client-side unfinished upload completion trailers and early worker responses
  need paired acceptance; response body errors must abort a response already
  committed, never become a synthesized success or JSON error body.
- WebSocket endpoint tunnels now have an additional unaccepted source candidate
  (raw frame splice, max_tunnels and drain timeout); they require the real paired
  campaign. Actual AgentInternal job affinity source is in ../cluster.rs. The
  pinned OSS front does not activate endpoint distributed fallback; do not invent
  a private relay protocol and label it upstream parity.
- `spare_streams` is a truthful local headroom producer, not evidence that the
  distributed max_streams consumer or a global W05 parent capability is complete.

## Attempt 05 failure triage (not acceptance)

The coordinator executed the candidate: the response-drop and raw WebSocket
upgrade cases failed with HTTP 502. The informational-head callback can be
dropped by Hyper on the final head, closing its watch channel. The front now
keeps a sender alive through final-head selection, so channel closure cannot
masquerade as the informational-count limit. Tests now consume a real request
head before emitting responses; zero and eight 103 heads must reach 200, nine
103 heads and final status 600 must fail. Existing cancellation, masked-frame,
101, drain-close and capacity assertions remain. These fixes require a new
coordinator run; the initial uncompiled status above is historical.
