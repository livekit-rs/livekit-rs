# Isolated upstream-feature conformance

This independent tiny workspace uses the public WebSocket codec. Run both
`cargo test --locked --no-default-features` and `cargo test --locked
--no-default-features --features livekit-policy` with a task-owned target.
Each profile covers server/client roles,1000/1001/4001/empty Close inputs,
exact emitted masked/unmasked reply bytes, preserved received messages, and
unchanged invalid-code transformation. It does not certify Go's entire invalid
control-frame error text. Real authenticated HTTP Worker20 remains mandatory.
No runtime dependency is added to the production server by this test crate.
The coordinator cleans this additional target after preserving safe results.
