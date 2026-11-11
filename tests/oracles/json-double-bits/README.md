# Real fixed Go double consumers — NOT_RUN

Stage this directory using the repository public prepare-locked-server-oracle.py. Root runs unchanged fixed server/protocol go.mod/sum source, not a hand-written expected float parser. main.go invokes actual strconv.ParseFloat, encoding/json.Unmarshal float64, Google protojson.Unmarshal into generated livekit.EncodingOptions, and proto.Marshal. Twenty raw numeric lexemes include the actual Pion kalman failure plus strict representational boundaries. Output carries exact IEEE bits as hexadecimal strings, unaffected by JSON parser rounding. No expected values are prefilled.

Example coordinator commands from repository root:

    python3 livekit-rs-server/tests/oracles/prepare-locked-server-oracle.py --repository "$PWD" --oracle /tmp/pion-json-float-roundtrip/oracle --out /tmp/pion-json-double-source
    cd /tmp/pion-json-double-source
    GOTOOLCHAIN=go1.26.3 GOMAXPROCS=2 go run -mod=readonly . < /tmp/pion-json-float-roundtrip/oracle/cases.jsonl > actual.jsonl

Rust test is the real public lk_protocol::json::decode generated-message bridge; parent supplies GO_JSON_DOUBLE_RESULTS. Do test-only on old features first for actual RED, then add the narrowly scoped existing serde_json float_roundtrip feature; no package/version change, custom parser, tolerance, raw corpus edit or estimator arithmetic change. Integer lexical and default/presence regressions remain mandatory. Signed-zero differences at generated wire bridging, if discovered, are genuine separate counterexamples and must not be suppressed.
