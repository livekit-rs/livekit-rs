# logging-json-level

Paired Go/Rust acceptance of the GENERATED CLI FLAGS `--logging.json` and `--logging.level`.

## What this verifies

- The `--logging.json` flag is parsed from argv (Kind::Bool in `crates/lk-config/src/generated.rs`), applied into `config.logging.json`, and selects the zap encoder:
  - `--logging.json=true` → JSON lines on stderr (Go: `zapcore.NewJSONEncoder`; Rust: `zap::set_json(true)` → `format_line` JSON branch)
  - `--logging.json=false` → console tab-separated lines on stderr (Go: `zapcore.NewConsoleEncoder`; Rust: `set_json(false)` → console branch)
- The `--logging.level` flag is parsed (Kind::Str), applied into `config.logging.level`, and filters stderr by severity:
  - `--logging.level=info` → no `debug` lines appear
  - `--logging.level=debug` → `debug` lines may appear

## Profiles

| Profile | Extra argv | Expect JSON | Expect debug possible |
|---------|-----------|-------------|----------------------|
| json-true-level-info | `--logging.json=true --logging.level=info` | yes | no |
| json-false-level-info | `--logging.json=false --logging.level=info` | no (console) | no |
| json-true-level-debug | `--logging.json=true --logging.level=debug` | yes | yes |

## Cross-profile control

- Go genuinely switches encoders: `json-true-level-info` produces all JSON lines, `json-false-level-info` produces all non-JSON console lines.
- Rust must match Go's structural behavior per profile.
- Level filtering: `json-true-level-debug` produces >= lines than `json-true-level-info` (debug is more verbose).

## Not verified here

- The YAML value leaf `logging.json` / `logging.level` (separate input path)
- The ENV single-key `LIVEKIT_LOGGING_JSON` / `LIVEKIT_LOGGING_LEVEL`
- Exact JSON field names beyond `level` (Go uses `ts` epoch float, Rust uses `time` RFC3339 string — a known structural difference)
- Component levels, sampling, pion level
- Media, RTP, WebRTC, TURN, WebTransport, Redis routing, worker lifecycles, webhook delivery

## Running

```bash
python3 run-paired.py \
  --go /path/to/livekit-server-v1.13.10 \
  --rust /path/to/livecloud \
  --output /path/to/evidence/dir
```

Exit 0 + stdout `PASS_GENERATED_CLI_FLAG_LOGGING_JSON_LEVEL_EFFECT` = both sides structurally match.
