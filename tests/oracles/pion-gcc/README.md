# Actual Pion source profiles

These are development-only Go source observers; no Go runtime or dependency enters the Rust product. The original404 stage cases/driver and original11-case/12-row adapter cases/driver are unchanged. All expected outputs must be produced by the coordinator from the locked source, never prefilled.

From repository root:

```sh
python3 livekit-rs-server/tests/oracles/pion-gcc/prepare.py --profile stages --source references/go-modules/pion-interceptor-v0.1.49 --server references/livekit-stable --out /tmp/owned-pion-source-stage
cd /tmp/owned-pion-source-stage
go list -mod=readonly -m github.com/pion/rtp github.com/pion/rtcp github.com/pion/transport/v5
PION_GCC_SOURCE_INPUT=/ABS/livekit-rs-server/tests/oracles/pion-gcc/stages/cases.jsonl PION_GCC_SOURCE_OUTPUT=/tmp/owned-pion-source-stage/actual.jsonl GOMAXPROCS=2 go test -mod=readonly ./pkg/gcc -run '^TestSourceOracle$' -count=1
```

Choose a NEW owned stage per profile. MVS must actually select RTP1.10.5, RTCP1.2.19, transport/v5 5.1.1. Clock-only transformations are recorded by the stager; estimator arithmetic is always the original source. Source178 hashes, fixed server mod/sum and all public observer inputs are checked. MIT license retained.

| Profile | Go test | Package | Input/output environment | Expected rows |
| --- | --- | --- | --- | --- |
| stages | TestSourceOracle | pkg/gcc | PION_GCC_SOURCE_INPUT / PION_GCC_SOURCE_OUTPUT |404|
| adapter | TestFeedbackSourceOracle | internal/cc | PION_ADAPTER_INPUT / PION_ADAPTER_OUTPUT |12|
| numeric | TestNumericInternalSourceOracle | pkg/gcc | PION_NUMERIC_SOURCE_INPUT / PION_NUMERIC_SOURCE_OUTPUT |34|
| extra | TestAdditionalSourceObserver | pkg/gcc | PION_GCC_EXTRA_INPUT / PION_GCC_EXTRA_OUTPUT |12|
| rfc8888 | TestRFC8888ReferenceSourceOracle | pkg/gcc | PION_RFC8888_INPUT / PION_RFC8888_OUTPUT |7|
| sent-size | TestSentSizeSourceOracle | internal/cc | PION_SENT_SIZE_INPUT / PION_SENT_SIZE_OUTPUT |8|

Use matching Rust ignored comparison RESULT environment variables documented in the candidate acceptance plan (source OUTPUT is renamed RESULTS there). Numeric states use IEEE754 bit equality, integer fields remain exact, negative zero is distinct, no tolerances. ARM64 default compiler fusion and GOAMD64=v1 separate operation results require their own actual runs. These profiles do not establish one numeric trajectory for Go's two racing delay goroutines; Rust explicitly serializes rate updates before grouping. Actual authenticated feedback, real send sizes, changed callbacks, default remote mode and independent publisher feedback each need their separate integration gate. RFC8888 observer proves synchronous actual WriteRTCP/reference arithmetic; its buffered controller channels do not claim a whole asynchronous target trajectory.
