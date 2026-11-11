# Fixed Pion PR observer v2 — real original-send transition

FROZEN_INPUTS / NOT_RUN. Original F137 and its executed12 actual rows remain unchanged as history with an insufficient send prerequisite. Its direct pushNoCheck chunks never set _allInflight, so abandoned() could return false even after _abandoned=true. Those rows cannot certify PR behavior. All12 input lines remain byte-identical here.

The v2 observer uses real pendingQueue.push → movePendingDataChunkToInflightQueue. Original source performs setAllInflight, assigns TSN and nSent, applies initial PR and inserts/rack-registers the actual chunk. The observer asserts TSN1..5, nSent1 and allInflight before any fast call. It retains source initial abandonment, then uses the same independent2-hour-old send timestamp challenge and3 gap misses. Actual output includes initial_abandoned/raw_abandoned/all_inflight, plus nSent, abandoned(), true serialized packet sizes and DATA TSNs. No expected values are filled. No production algorithm/source clock is replaced.

Stage an independent fresh source directory using immutable F136 prepare_oracle.py first. Then:

```
/usr/bin/python3 /tmp/f137-sctp-fast-pr-source-v2/prepare.py --staged-source /tmp/livekit-rs-f137-v2-source-stage --source-reference REPO/references/go-modules/pion-sctp-v1.12.0
cd /tmp/livekit-rs-f137-v2-source-stage
GOTOOLCHAIN=go1.26.3 GOMAXPROCS=2 GO_SCTP_PR_INPUTS="$PWD/partial-reliability-cases.jsonl" GO_SCTP_PR_OUTPUT="$PWD/partial-reliability-actual.jsonl" go test -mod=readonly ./source -run '^TestSourceFastPartialReliabilityObserver$' -count=1
```

The supplementary stager fills any missing non-test example source only from actual hash-verified retained originals before enforcing all original Go source hashes. It never overwrites mismatched source or existing observer files. This corrects the prior actual FileNotFoundError setup defect, not an algorithm result. Parent alone executes; all runtime and physical/SDK claims remain OPEN.
