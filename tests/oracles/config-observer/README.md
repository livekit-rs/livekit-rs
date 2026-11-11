# Config differential observer (SPEC-0073 M1 W1-G1/G2/G3)

This harness runs the same argv/ENV/YAML case matrix through upstream `getConfig`
and through lk-config `parse()`, then compares the canonical config trees and the
errors.

- `go/config_observer_test.go`: a Go test copied into a staged copy of
  `references/livekit-stable/cmd/server` (package main). For each case it clears
  `LIVEKIT_*`, `REDIS_*`, `NODE_IP` and `UDP_PORT`, applies the case env, writes the
  case files into a temp dir (`{dir}` placeholder), and builds a root
  `cli.Command` with main.go's `baseFlags + GenerateCLIFlags(baseFlags, true)`
  (fresh flag copies per case). It records:
  - `getconfig`: the real `getConfig(c)` result.
  - `run`: `getConfig` + `ValidateKeys` + `LoadTURNSecrets`. This is the outcome that
    gets compared.
  It also has a leaves mode, which writes the generated flag table and the
  canonical `DefaultConfig` tree.
- `crates/lk-config/src/observer.rs` (`#[cfg(test)]` only): the `Render` trait.
  `yaml_struct!` generates the impls under `#[cfg(test)]`. There is also an
  `#[ignore]` test `observer::tests::config_observer` that runs
  `parse()` → `EntryAction::RunServer` → `LoadedConfig.official`. It uses the real
  process env minus the observed variables, injected through `Env::from_os_pairs`,
  and the real `SystemProbe`.
- `gen_cases.py`: builds the case matrix from the Go leaves file. That covers
  baseline/dev, YAML, flag and ENV forms for every generated leaf, precedence,
  base flags, invalid values, strictness, file shapes, usage errors and
  structured fields.
- `compare.py`: compares the two result sets. It writes `report.json` and
  `summary.json`.

## Canonical rendering (both sides)

- Paths are dotted yaml-tag paths. `,inline` fields are flattened, `yaml:"-"` and
  unexported fields are skipped, and untagged fields use the lowercase field name.
- Scalars are text: bool `true`/`false`, ints in decimal, floats with
  `FormatFloat('g', -1, 32|64)`, `time.Duration.String()`, strings raw.
- Slices, maps and structs inside them become compact JSON at their path, with
  sorted keys, every nested scalar as a JSON string, and no HTML escaping. Nil
  pointers, slices and maps are rendered as `null`.
- `room.room_configurations` values are protobuf messages. They are rendered with
  protojson using `UseProtoNames`, `UseEnumNumbers` and `EmitUnpopulated`. Numbers
  and bools are then converted to strings and null members are removed, because
  an unset message or oneof is not emitted. On the Rust side, `None` and a nil
  repeated field render the same way.
- The comparator treats Go nil and Rust empty collections (`null` vs `[]`/`{}`) as
  benign and reports them separately, because Rust uses `Vec`/`BTreeMap` without
  nil.
- Node IP discovery runs for real on both sides, on this host.

## Commands (run from the repo root)

```sh
df -h /tmp                                   # keep > 5 GB free
W=/tmp/cfgobs-work; E=$PWD/livekit-rs-docs/evidence/2026-10-28/w1-config-observer
livekit-rs-server/tests/oracles/config-observer/stage_go.sh . $W     # GOTOOLCHAIN=go1.26.3, GOFLAGS=-mod=mod, replaces protocol/mediatransportutil
(cd $W && CONFIG_OBSERVER_LEAVES=$E/go-leaves.json ./config-observer.test -test.run TestConfigObserver)
python3 -I livekit-rs-server/tests/oracles/config-observer/gen_cases.py $E/go-leaves.json $E/cases.json
(cd $W && CONFIG_OBSERVER_CASES=$E/cases.json CONFIG_OBSERVER_OUT=$E/go-results.json ./config-observer.test -test.run TestConfigObserver -test.timeout 30m > $W/go-run.log 2>&1)
(cd livekit-rs-server && CONFIG_OBSERVER_CASES=$E/cases.json CONFIG_OBSERVER_OUT=$E/rust-results.json \
   cargo test -p lk-config --lib observer::tests::config_observer -- --ignored --exact)
python3 -I livekit-rs-server/tests/oracles/config-observer/compare.py $E/cases.json $E/go-results.json $E/rust-results.json $E/go-leaves.json $E/report.json
rm -rf $W
```

Run the Go side first; it must pass on its own as the baseline. The Rust results
depend on the host's network interfaces in the same way the Go results do.

## One-shot run and Go types

`run.sh [evidence-dir]` (repo root) stages Go, writes leaves, generates cases, runs
Go first, then Rust, then compares. It needs >5 GB free and removes its work dir.
The evidence keeps `go-results.json`/`rust-results.json` gzip-compressed
(`gzip -d` to inspect); `run.sh` regenerates them uncompressed.

`go-types.json` is `CONFIG_OBSERVER_TYPES=<out>`: the original `reflect.Type.String()`
of every yaml path (`[]` / `{}` / `{key}` for elements and keys). It is what
yaml.v3 prints in `cannot unmarshal ... into <type>`. The lk-config unit test
`observer::tests::go_types_match_original_reflection` requires every Rust field's
declared Go type (`yaml_struct!` `as "<type>"` overrides) to equal it exactly.
Regenerate after an upstream bump:
`(cd $W && CONFIG_OBSERVER_TYPES=$PWD/../livekit-rs-server/tests/oracles/config-observer/go-types.json ./config-observer.test -test.run TestConfigObserver)`.
