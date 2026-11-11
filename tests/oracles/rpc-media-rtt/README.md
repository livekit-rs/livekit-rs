# Original downlink RR producer controls

Status: FIXTURE_NOT_RUN. These tests execute the unchanged locked Go `RTPStatsSender.Update`, `GetRtcpSenderReport`, `UpdateFromReceiverReport` and original `mediatransportutil.GetRttMs`; no copied RTT algorithm is substituted.7 rows cover uninitialized/no SR/zero LSR/wrong LSR/anachronous delay and actual valid DLSR0/nonzero. Native source reader uses measured monotonic before/after brackets because original code samples time internally; no broad millisecond allowance replaces that measured interval.

Stage a fresh copy (root is repository root containing references):

```
python3 tests/oracles/rpc-media-rtt/prepare.py --root /absolute/livekit-rs --out /owned/new-stage
cd /owned/new-stage/source
go test -mod=mod ./pkg/sfu/rtpstats -run '^TestOriginalActualSenderReportRpcRttSource$' -count=1 -v
```

Save the complete actual log, exit code,7 nonzero rows, original source and resolved module hashes. Read via exact copied native vendor tests `receiver_report_rtt::tests::original_go_sender_rr_consumer_rows_match_native_marker_clock_brackets` with `GO_RPC_MEDIA_RTT_RESULTS` set, `--ignored --exact`. No test in this directory alone proves actual public RPC; the native Session encrypted SRTCP test, actual media bound-subscriber test and unchanged SDK RPC lifecycle runner verify that producer/consumer chain separately.
