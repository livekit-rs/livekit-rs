# Real fixed-source logger consumers

Stage `oracle/prepare.py --repo REPO --out FRESH_STAGE`, build observer using
Go1.26.3 in that stage, then run it on the unchanged42-case `cases.json`.
Compare real source loader+logger processes with `run-processes.py --go OBSERVER
--rust SERVER/target/release/examples/logging_policy_probe --out OWNED_JSON`.
Build the Rust example with locked release dependencies. Set LOGGING_GO_RESULTS
to actual observer output for the ignored `actual_go_component_level_rows` test.
Native SDK producer validation is separate; see SPEC0038 and archived evidence.
Public fixture credentials are development only; raw logs stay ignored/local.
