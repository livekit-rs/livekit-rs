# Node lifecycle campaign

Pinned Go server v1.13.10 source e5655ac20267fa5005dccaf3f0e3bfc3ea688c07.
The fixed Go binary SHA is asserted before execution (recorded in
`livekit-rs-docs/evidence/2026-10-30/c11-redis-sentinel`). A version string alone
is insufficient. Choose a fresh evidence output directory; old receipts are never
overwritten. Run only after the coordinator's resource gate qualifies.

```
python3 tests/interop/cli-redis-node-registration-lifecycle/run-paired.py --official /tmp/livekit-server-go --rust /absolute/final-candidate/livekit-server --output /absolute/new-evidence-directory
```

The candidate must be rebuilt from the intended source and its identity bound to
that source. This script checks binary/harness identity at campaign start/end;
it does not infer source provenance from an old executable on disk.

11 real vectors cover registration, keepalive, health, multi-server keyspace,
Redis restart/CLIENT KILL, startup failure cleanup and single-node mode. These
are node lifecycle tests, not cross-node room routing or NodeStats counter
acceptance. `--only` produces `PASS_SELECTED_VECTORS_ONLY`, never full PASS.
Interruptions, candidate mutation, mismatches and cleanup failures fail the run.

No-service receipt-policy regression (safe before compile/server resource gate):

```
PYTHONDONTWRITEBYTECODE=1 /usr/bin/python3 tests/interop/cli-redis-node-registration-lifecycle/test_receipts.py
```

Five mocked-policy tests exercise result classification and cleanup failure.
Their success does not mean a real Redis campaign or media test has passed.
