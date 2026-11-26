# C100 lane A — launched-MAIN pairing for the config-source base flags

SPEC-0077 rows: `base_flags:--config`, `base_flags:--config-body`, `base_flags:--key-file`,
`base_flags:--keys`, `base_flags:--disable-strict-config`.

Tier: `main_server_consumer` (4). Nothing here is a projection claim — the projection legs for the
same leaves live in `crates/lk-config/tests/*` — and nothing here promotes a row to `VERIFIED`,
which needs the paired differential this lane runs.

## Why a launch is the only proof

`--config` / `--config-body` produce a byte string (`getConfigString`,
`references/livekit-stable/cmd/server/main.go:324`) that `config.NewConfig`
(`pkg/config/config.go:669`) strict-decodes, and `--keys` / `--key-file` only land in `conf.Keys` /
`conf.KeyFile` (the latter expanded with `homedir.Expand(os.ExpandEnv(...))` at
`pkg/config/config.go:707`). The consumer that proves a value was used is the HTTP authorizer fed by
`ValidateKeys` (`pkg/config/config.go:820-844`), so every start row signs an HS256 token with the
key the flag supplied and requires `POST /twirp/Livekit.RoomService/ListRooms` to answer, then
requires the same key with a wrong secret to be refused. The discriminator is the pair of statuses
the two probes produce on *both* binaries: with a key the authorizer knows the request gets past
`ValidateKeys` and reaches the mux (`http.StatusNotFound`, body `404 page not found`, because the
measured build does not route `ListRooms` on the HTTP port), and with a wrong secret it is rejected
before routing with **401 `invalid authorization token: token signature is invalid`**
(`pkg/service/auth.go:48`). Both halves are compared between the two binaries (`probe_*_agrees`), so
an "accepted" row cannot be an unauthenticated default and a "refused" row cannot be a crash.

Refusals are a contract too: upstream prints them with `fmt.Println(err)` and exits 1
(`cmd/server/main.go:190-192`), i.e. on **stdout**, and the four sentences this lane pins are

| case | upstream text | source |
| --- | --- | --- |
| keys string cannot parse | `Could not parse keys, it needs to be exactly, "key: secret", including the space` | `pkg/config/config.go:1114` |
| neither key source | `one of key-file or keys must be provided` | `:73` (`ErrKeysNotSet`), returned at `:844` |
| key file readable by others | `key file others permissions must be set to 0` | `:71` (`ErrKeyFileIncorrectPermission`), returned at `:827` |
| config body that is not YAML | `could not parse config: %v` | `:686` |

## Profiles (19)

| id | subject |
| --- | --- |
| A-01…A-05 | `--config` start / missing / directory / malformed / empty |
| A-06…A-08 | body beats file, `LIVEKIT_CONFIG` as the env spelling, empty `--config-body` falls back to the file |
| A-09, A-10 | unknown key refuses under strict parsing and reaches the keys check with `--disable-strict-config` |
| A-11…A-14 | `--keys` valid, env spelling, malformed, CLI beats env |
| A-15…A-18 | `--key-file` valid 0600, world-readable 0644 refusal, missing file, `$VAR` expansion |
| A-19 | neither key source outside development mode |

## Run

```bash
python3 tests/interop/base-flag-launch/run-paired.py \
  --official /tmp/c99-build/livekit-server-go \
  --rust     /Users/zoujiaqing/projects/livekit-rs/target/release/livecloud \
  --output   <fresh-dir>          # optional: --only A-06, --report <fresh-dir>/execution.json
python3 tests/interop/base-flag-launch/run-paired.py --selftest-gate <copy>/execution.json
```

The output directory must not exist: a lane that appends to an earlier slot would mix two
candidates in one receipt. `--report` re-derives every verdict from a recorded `execution.json`
without starting a daemon, so a filed receipt can be re-checked offline.
`--selftest-gate` re-derives the same verdicts under thirteen named in-memory mutations and fails
unless each mutation reddens the check it targets; it refuses a receipt that lacks a shape some
injection needs (no refusal row, no `expect`, no probes) rather than sweeping gates nothing reached.
All three read-only modes write beside the receipt they are given, so they take a **copy** — and
`refuse_filed_slot` stops any of them pointing at `server/evidence/`.

## A refusal has to end the process by itself

A refusal row is not allowed to be signalled into its exit code. `capture()` waits
`--refuse-wait` (20 s) for the child to leave on its own and records `self_exited`/`exit_wait_ms`;
only if it is still alive does the signal ladder run, and then `refusal_self_exits` reddens the row.
The first full run of this lane did the opposite — `port=None` skipped the settle wait, so the
harness SIGINTed every refusal about 1 ms after `Popen`, before either binary reached its
`fmt.Println(err)`. All ten refusal rows came back `exit=-2` with **zero-byte** streams, and because
two empty streams are equal in every bucket, `parity_flag_lines` and `parity_stdout` passed on them
while `full_streams_equal` counted ten rows that had no bytes in them. The row-level checks
(`refusal_exit_1`, `expect_*`) caught it and the lane exited 1, but the emptiness itself was
matching a check to nothing, so `refusal_printed` now requires a non-empty `flag` bucket on each
side, and `--selftest-gate` carries `refusal_survives_the_wait` and `refusal_prints_nothing` to
prove both new checks can fail.

## What a row actually compares

Each stream is bucketed by *who decides to print the line*, after normalizing timestamps, temp
paths, ports, hex and the per-launch `ND_` node id (`pkg/rtc/node_id.go` mints it, so it is noise
neither side was told to use):

| bucket | decided by | treatment |
| --- | --- | --- |
| `flag` | the flag under test (startup, config, refusal sentences) | must match line for line — `parity_flag_lines` |
| `request_log` | the serving process (`pkg/service/utils.go:79-81`) | reported per row and lane-wide |
| `shutdown` | the signal loop (`cmd/server/main.go:313-320`) | reported, plus the recorded signal sequence and stop latency |

`parity_line_order` keeps the scoping honest: if the whole streams differ while *every* bucket
matches, the difference is in the order the classes are printed, which no bucket accounts for, so
it is a finding instead of noise. `stdout` and `stderr` stay compared apart because the stream a
refusal lands on is itself part of the contract.

## Two service-surface differences this lane measures and does not assert

Both are systematic across the nine start rows, and both are recorded in `summary.scoped_observations`:

* **Go logs a refused API call, Rust is silent.** Every row whose probes include a wrong-secret
  request shows Go printing `WARN service/utils.go:85 error handling request {"status": 401,
  "path": "/twirp/Livekit.RoomService/ListRooms", ...}` and Rust printing nothing, while both
  answer 401 with the same body. The three rows that only send an accepted request
  (`A-08`, `A-12`, `A-18`) show no such line on either side, so the difference tracks the
  *refused* request rather than requests in general — `pkg/service/utils.go:79-81` is on the
  error path. The HTTP status parity the row claims is unaffected; the logging gap is an
  attribution item.
* **Rust stops on the first SIGINT; Go needs a second.** Rust exits 1–3 ms after the signal; Go
  logs `force: false`, is still running 8 s later, and only the harness's second SIGINT
  (`force: true`) ends it, at ~8.0 s, exit 0 on both sides. Upstream's `for i := range 2` loop is
  the calibration this harness was written against; the 8-second `Stop(false)` is the part worth
  naming. Both are recorded per row (`stop.signals`, `stop.latency_ms`), never averaged away.

## Safety

Pure loopback. `bind_addresses: [127.0.0.1]` in every config body and `rtc.tcp_port: 0`, and no row
ever names port 7881 or 7882: the harness asserts that per argv token and records `lsof` ownership of
both ports before and after (`doc["safety"]`), because the developer host runs its own
`livekit-server` there. A row that is *supposed* to refuse also gets its own ephemeral `--port`, so
an unexpected start cannot reach the upstream default either. No route, interface, proxy or alias
state is touched. Every original exit code is preserved (no pipe, no shell), and shutdown follows the
calibration in `tests/interop/config-wiring-startup`: SIGINT, wait, second SIGINT, then the process
group, with the signal that actually ended the run recorded.

## Honest residual

- Every refusal row in the measured run carries the *upstream* sentence on both sides, byte for byte
  after the normalizers above — the Rust binary reproduces the four quoted strings rather than
  paraphrasing them. That is what `expect_go` / `expect_rust` assert; a row where only one side says
  it is a finding, not a formatting difference.
- `--disable-strict-config` proves the *parse* gate moved, not that the unknown key was applied:
  a keyless relaxed run still refuses, so the row discriminates by which sentence the process prints.
- Only the two key-accepting API probes (`ListRooms`) are driven; no media, no room lifecycle.
- `~` expansion of `--key-file` is not exercised (it would point at the developer home); only `$VAR`.
- A refusal whose text is produced by the YAML library rather than by upstream (`could not parse
  config: <parser wording>`) can diverge for parser-internal reasons; such a row is fenced with the
  measured pair rather than asserted as parity.
- The two `request_log`/`shutdown` observations above are *measured*, not asserted: this lane has no
  claim about server logging or drain timing, and it does not disclaim them either. Their numbers are
  in `execution.json` under `verdicts[].service_log_divergences`, `verdicts[].stop` and
  `summary.scoped_observations`.
