# Redis transaction writer deadline and EXEC reply loss

The Go observer calls the pinned RedisStore DeleteIngress through
protocol/redis.GetRedisClient. Rust calls the corresponding production
IoStore method. Both clients use the one-address Sentinel configuration that
selects Simple Redis with explicit writer and reader deadlines. The proxy
stops reading after a partial SREM, holds the peer open past the 180 ms
writer deadline and never forwards the incomplete SREM or EXEC. The actual
broker records the successful fourth EXEC, or the original records on
four-fault exhaustion. It also checks an independent successful Egress write
using the same original store after every outcome.

Fixed Go can retry an actual writer error, yet retains that original error
on the synthetic MULTI command even when a subsequent retry commits. The
fixture compares the resulting state and the reported failure independently.
It keeps the two previous EXEC reply-loss controls, where the final outcome
does not authorize replay and concurrent recreation must survive. The
no-fault deletion is a positive control. Transport error wording differs.

Build the Go file as main.go in a private copy of the locked module from
tests/oracles/sip-callbacks, pointing its local LiveKit replacement at
references/livekit-stable. Build the Rust observer with cargo build --locked
-p lk-compat --example redis_transaction_prewrite_observer. Run
python3 run.py --go <go> --rust <rust> --expect after --output <new-directory>.
The same fixture accepts --expect before with an unchanged production
baseline executable built with the observer source.

Retry eligibility is scoped to the attributed write deadline while bytes
remain pending in poll_write. Unattributed transport errors, a deadline
flushing already submitted bytes, and a lost response remain fail closed.
This TCP fixture does not cover TLS transmission failures, Cluster atomic
routing, authentication errors, or broader Go writer error classes.

## Owned Cluster and Sentinel comparison

`run-topologies.py --go <go> --rust <rust> --output <new-directory>`
uses the same clients against an owned three-master Redis Cluster and a
named Sentinel deployment with a monitored Redis primary. The tap forwards
commands to the real slot owner or Sentinel-discovered primary. It rewrites
only discovery addresses to keep the tested connection on the observed
transport. Add `--tls` to verify the public RSA test CA and localhost SNI
on client-to-tap sockets.

The runner checks all four Ingress keys, each attempt and its socket,
and an independent Egress write on the original store after the outcome.
The default empty-master Cluster path attempts four writes. The fourth
write succeeds after three proven incomplete writes. Named Sentinel and
the original Simple path report the first write error after that commit.
All paths return an error and leave the surviving record alone on
unobserved EXEC completion. These profiles use a stable topology
during each run and do not exercise a live slot migration or promotion.

## Live slot migration and Sentinel promotion at the EXEC boundary

`run-live-transition.py --go <go> --rust <rust> --output <new-directory>`
performs an actual topology change while the transaction is in flight:
a completed slot migration before EXEC (MOVED), an importing/migrating slot
(ASK), a lost EXEC reply followed by migration and recreation on the new
owner, and a Sentinel promotion either with EXEC withheld or after a lost
EXEC reply. Both observers wait on `LK_REDIS_RECOVERY_GATE` until the fixture
finishes the transition, then perform an independent Egress write that must
land on the new owner or promoted primary. The oracle asserts the fixed Go
outcome; whether a client consumed the `+switch-master` notification is
recorded per implementation (`notification_observed`) rather than asserted,
because pinned Go has been observed to reach the promoted primary by
re-resolving the master instead.
