# Fixed Go logger sampling oracle

Source: LiveKit v1.13.10 e5655ac20267fa5005dccaf3f0e3bfc3ea688c07,
protocol 9bb708cdb81918a6c63dd25e915b4644310b45b6.

Run only after coordinator authorizes checks. From `references/protocol-stable`:

```
go run ../../livekit-rs-server/tests/interop/logging-sampling/probe.go > /absolute/owned-output/go-sampling.jsonl
```

Then from `livekit-rs-server`:

```
LOGGING_SAMPLING_GO_RESULTS=/absolute/owned-output/go-sampling.jsonl cargo test -p livecloud --lib logging::tests::actual_fixed_go_sampling_rows -- --ignored --exact
cargo test -p livecloud --lib log_sampling::tests
cargo test -p livecloud --lib logging::tests
cargo test -p lk-config --test config_leaf_parsing -- logging_sample_yaml_env_projection generated_flags_cli_coverage_audio_logging_redis_room_rtc
```

The probe calls the real fixed public logger with a private output sink. No server,
network, sleep or edits to reference inputs are needed. The Rust clock-injected
checks additionally cover exact window boundary, level separation, bounded XXH3
bucket collisions, signed values, and independent room generation ownership.

Important oracle behavior: `sample_initial` and `sample_interval` are accepted but
unused in this fixed logger. Root sampling uses `item_sample_initial/interval`,
with zero replaced by 20/100. WithItemSampler replaces root sampling, independently
of `sample`, when `item_sample_seconds != 0`; its values have no zero defaults.
Do not "correct" these upstream behaviors according to comments.

Scope limitation: the production room task explicitly owns a room logger scope;
logs inside it share that generation's sampler. This does not establish mapping
of every upstream room logger callsite, asynchronously spawned worker scope,
WithoutSampler, runtime config updates, or final server regression. Keep those
claims open until mapped and exercised.

Executed 2026-11-14 on Rust commit 012b002472bfc94e863e7785818b3467032e4ae0: the
`go run` module oracle produced 8 rows and `actual_fixed_go_sampling_rows`
matched all 8 (1 passed), 9 component tests passed, and the YAML/ENV + CLI
projection tests passed (2). This is a sampler-policy differential at module
scope, not a paired-binary full-server lifecycle. Raw identities, commands, exit
codes and per-row results: livekit-docs
`server/evidence/2026-11-14/spec0077-c11-logging-sampling/`. The residuals above
remain open; no full-server acceptance is claimed.
