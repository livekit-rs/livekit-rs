# Independent original source RTO PR observer

FROZEN_INPUTS / NOT_RUN. Same12 original cases, real Stream.packetize3000B /3 original sends and5 stage rows per case; no original corpus edited. Only this separate observer uses original inflightQueue.markAllToRetrasmit and getDataPacketsToRetransmit + actual marshalPacket in place of fast selection. CWND/RWND are explicitly set20000 to isolate source PR/MTU selection from congestion credit. This is not an actual timer expiration, physical association or SDK claim. No algorithm replacement or authored expected output.

```
# First create a fresh original F136 source stage.
/usr/bin/python3 /tmp/f140-sctp-rto-pr-source/prepare.py --staged-source NEW_STAGE --source-reference REPO/references/go-modules/pion-sctp-v1.12.0
cd NEW_STAGE
GOTOOLCHAIN=go1.26.3 GOMAXPROCS=2 GO_SCTP_RTO_INPUTS="$PWD/rto-pr-cases.jsonl" GO_SCTP_RTO_OUTPUT="$PWD/rto-pr-actual.jsonl" go test -mod=readonly ./source -run '^TestSourceRtoPartialReliabilityObserver$' -count=1
```

This controls the RTO PR branch needed by an enabled source fast-window policy without changing native default0. All original F136/F137/F137v2/F139 evidence is retained. Coordinator alone executes; all60 rows and failures must be preserved.
