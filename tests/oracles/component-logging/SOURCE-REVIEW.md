# F137 v2 component-level logging: isolated, NOT_RUN

This candidate changes the real `livecloud::logging::init` consumer. It does not
change loader precedence: existing `log_level` and development defaults remain;
nonempty `logging.pion_level` still overwrites the two parent keys `pion` and
`transport.pion` after YAML/CLI processing. More-specific component entries win.

The fixed source resolver performs literal exact lookup, then removes the last
`.` segment. It does not parse filter directives. Invalid levels resolve INFO;
`warn` and `warning` are accepted aliases; `trace`, `off` and whitespace-padded names are invalid. DPanic, Panic
and Fatal thresholds exceed ordinary ERROR; the Rust backend does not invent
Go panic/fatal calls. Pion Trace maps to Debug in the fixed source adapter.

Current production turns dots into underscores and interpolates keys into an
EnvFilter. Native ICE/DTLS targets do not match those directives. A syntactically
invalid directive key can also reset the entire filter to INFO. The candidate
uses a typed filter and consumes real tracing **and log-crate** producers. The
`log` bridge normalizes dynamic event target metadata before filtering; it must
not treat every log-crate event as the static `log` target.

Explicit native aliases are finite: `is::*` -> `transport.pion.ice`, `dimpl::*`
-> `transport.pion.dtls`, `str0m::bwe*` -> `transport.cc`, other `str0m::*` ->
`transport.pion`, `rtc_runtime::net::tcp*` -> `pion.tcp_mux`. These are documented
backend counterparts, not a claim of byte-identical Go messages, complete scope
attribution, Go runtime replacement, or native Pion execution. Other Rust targets
remain literal. More per-owner logging attribution remains a separate functional
leaf; this cut does not claim every Go component producer exists.

## Dependency boundary

Adds exact tracing-log 0.2.0, default-features=false, std+log-tracer; enables the
existing tracing-subscriber `tracing-log` feature. Uses already selected log
0.4.34 only as a dev dependency for the real process marker. No new allocator,
foreign runtime, unsafe implementation, services, locks, or timing policy.
Archive SHA256: ee855f1f400bd0e5c02d150ae5de3840039a3f54b025156404e34c23c03f47c3.
Published VCS 4161d8137d4f6117f17b110b0ec022d9350bf8e6 / tracing-log.
Package archive and selected primary-registry record are in source/. Root must
retain the accepted source in references/catalog before final delivery and
regenerate the package edge without changing unrelated versions.

## Actual execution gates (coordinator only)

Original v1 incorrectly treated warning as invalid; direct selected zap v1.28.0 source reinspection refutes that classification. V1 remains historical and must not be adopted. V2 restores the source alias, with cases unchanged.

No candidate, oracle, compiler, server or browser has been executed by author.
Python AST parsing and SHA guards are preparation, not compatibility PASS.

1. Preserve baseline production. Apply just the example and exact dev log edge,
   compile `cargo build --release --locked -p livecloud --example logging_policy_probe`.
   Save this old-consumer executable and binary/source identity before replacing
   production. The same example is used for RED and GREEN.
2. `python3 oracle/prepare.py --repo REPO --out FRESH_STAGE`. In stage, coordinator
   executes `GOTOOLCHAIN=go1.26.3 GOMAXPROCS=2 go build -p=2 -mod=mod -o OBSERVER .`
   and `OBSERVER cases.json > actual.jsonl`. The 42 rows call the *real*
   `logger.Config.ResolveComponentLevel`; no expected thresholds are synthesized.
   Save selected module graph/toolchain/input/output/binary identities.
3. `python3 run-processes.py --go OBSERVER --rust OLD_PROBE --out RED.json`.
   All nine profiles use genuine fixed source `config.NewConfig` and Rust
   `lk_config::parse`, followed by the real production logger. Controlled markers
   are a filtering/bridge test, not native networking proof.
4. Apply candidate with `apply.py --repo REPO/livekit-rs-server`, resolve only
   reviewed package edge, format, root full tests/Clippy/build. Execute same
   process fixture with candidate probe. Run ignored resolver comparison:
   `LOGGING_GO_RESULTS=... cargo test --release --locked -p livecloud actual_go_component_level_rows -- --ignored`.
   Example inclusion can cause the same module test in an example harness too;
   both must use the same actual output, never fabricated expected rows.
5. Genuine server/unchanged pinned SDK fresh-profile gates: actual media remains
   healthy while global DEBUG+pion ERROR suppresses real ICE DEBUG; global
   ERROR+pion DEBUG emits real ICE DEBUG; a transport.pion.ice FATAL override
   suppresses it; child DEBUG overrides parent ERROR; malformed literal component
   key never changes root ERROR policy. Capture actual producer JSON records,
   source identities, counts and SDK result. A successful controlled marker does
   not close this actual-native-producer gate. No thresholds/timeouts weakened.

All sampling leaves are out of scope. Fixed `logger.Config.SampleInitial` and
`SampleInterval` have serialization reads but no logger-production read in this
source revision. Global `Sample` actually uses `ItemSampleInitial/Interval`;
`WithItemSampler` uses ItemSampleSeconds plus those parameters. Current Rust has
no equivalent consumers. Do not invent consumption for the first two or mark
sampling supported because it parses. No canonical statuses are changed here.
