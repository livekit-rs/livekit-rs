# Original local RPC functional observers

Run prepare.py separately with `--kind constructor` (five actual typed-constructor/localbus cases) and `--kind phase` (twelve original wire/lifetime cases). Supply the fixed source repository root and two absent owned output directories. Original library code is unmodified; constructor mode only renames the observer entry to select its separate real constructor main.

In each staged directory build an actual executable using `GOTOOLCHAIN=go1.26.3 GOMAXPROCS=2 go build -mod=readonly -o <owned-executable> .`, then run that executable into owned JSONL. Bind actual executable, module/source, output hashes and exit codes. No expected result file is synthesized.

Use `LOCAL_RPC_CONSTRUCTOR_GO_RESULTS` and `LOCAL_RPC_V4_GO_RESULTS` for the complete Rust `service_bus::rpc::local_client::constructor_tests` and `service_bus::rpc::local_phase_tests` modules with `--include-ignored --test-threads=1 --nocapture`. Inventory-only selectors are insufficient. Real typed echo responses are transport proof, not agent worker assignment. Preserve explicit cancellation, a dropped external future, publication capacity and method deadlines as separate cases.
