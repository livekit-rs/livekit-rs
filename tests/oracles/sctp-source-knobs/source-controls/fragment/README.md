# Original Pion fragmented PR / buffer source observer

FROZEN_INPUTS / NOT_RUN. Same12 immutable F137v2 case inputs; a separate observer calls actual Stream.packetize(3000B), source pending→inflight original sends, original fast selection and original cumulative SACK. It asserts real3 fragments and uses original generated fragment-head links, source stream buffer debt, all-inflight transition, and source nSent/TSN assignment. No authored expected output or replacement source methods. Twelve cases each produce5 stage rows (60 expected row count, outcomes unspecified).

Outputs distinguish raw head abandonment from effective abandonment before/after the final original fragment, retain original buffered/flight-byte debt through fast selection, and observe real SACK release. Real marshalled packet lengths and DATA TSNs are parsed from the source packet bytes. This is source component state and packetization, not a physical peer proof.

Use a fresh F136 stage, then:

```
/usr/bin/python3 /tmp/f139-sctp-fragment-pr-source/prepare.py --staged-source NEW_STAGE --source-reference REPO/references/go-modules/pion-sctp-v1.12.0
cd NEW_STAGE
GOTOOLCHAIN=go1.26.3 GOMAXPROCS=2 GO_SCTP_FRAGMENT_INPUTS="$PWD/fragment-pr-cases.jsonl" GO_SCTP_FRAGMENT_OUTPUT="$PWD/fragment-pr-actual.jsonl" go test -mod=readonly ./source -run '^TestSourceFragmentPartialReliabilityObserver$' -count=1
```

Original F13610+10/F13712/F137v2 actual12 remain unchanged. Coordinator alone executes. All actual outcomes/first failures must be preserved. Native defaults must remain unchanged unless a separate source-backed scope is explicitly adopted.
