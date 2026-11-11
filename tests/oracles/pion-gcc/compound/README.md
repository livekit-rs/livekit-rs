# Whole-compound RTCP admission — actual fixed Go source observer

Original fourteen inputs and driver are byte-for-byte the independent v1 review corpus. No expected results are embedded. Root already executed the original corpus against fixed server MVS RTCP1.2.19 / interceptor0.1.49; this public copy must reproduce it before delivery.

From repository root (source references restored):

```sh
cd references/livekit-stable
GOTOOLCHAIN=go1.26.3 GOMAXPROCS=2 go run -mod=readonly ../../livekit-rs-server/tests/oracles/pion-gcc/compound/main.go ../../livekit-rs-server/tests/oracles/pion-gcc/compound/cases.jsonl > /tmp/owned-pion-compound-actual.jsonl
```

This directly invokes Attributes.GetRTCPPackets on original bytes: no production transforms or fake estimator. It measures full-datagram admission/count/error only; it does not prove encryption or actual estimator callbacks. Rust test `actual_compound_admission_fourteen` requires PION_COMPOUND_INPUT and PION_COMPOUND_RESULTS and explicit --ignored.

Additional typed validators (SDES/APP/XR/REMB etc.) are source ports, not automatically certified by these fourteen examples. Their broader actual differential corpus remains an activation gate. Runtime encrypted pair and provider no-partial-mutation tests are separate.
