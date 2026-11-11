# Pion actual sent-history and TWCC adapter oracle

Root stages the exact178-file original module, verifies all hashes, adds only this original-package development test. There are zero production source transformations and no authored expected ACKs. The driver invokes original FeedbackAdapter.OnSent with a real RTP header+real serialized transport extension; original Pion RTCP TransportLayerCC.Unmarshal decodes raw packet bytes, then original OnTransportCCFeedback returns actual ACK/default/loss/delta outcomes. Eleven inputs cover full run/vector tails, missing sent identity, negative deltas, wrap, duplicated feedback,250-key eviction/MRU, inconsistent/truncated wire. `actual_sent` captures actual original header MarshalSize plus payload.

Coordinator commands from repository root:

    python3 livekit-rs-server/tests/oracles/pion-feedback-adapter/prepare.py --source "$PWD/references/go-modules/pion-interceptor-v0.1.49" --out /tmp/pion-adapter-source
    cd /tmp/pion-adapter-source
    PION_ADAPTER_INPUT=/absolute/repository/livekit-rs-server/tests/oracles/pion-feedback-adapter/cases.jsonl PION_ADAPTER_OUTPUT=/tmp/pion-adapter-source/actual.jsonl GOTOOLCHAIN=go1.26.3 GOMAXPROCS=2 go test -count=1 ./internal/cc -run '^TestFeedbackSourceOracle$'

This is exact source adapter/wire parsing component evidence only. Rust actual encrypted RTP/SRTCP producer, callback scheduling and full LiveKit public consumer remain separate required gates; no claim of whole-path conformance. Input preserves legitimate malformed wire rejections instead of treating them as successful transport feedback.
