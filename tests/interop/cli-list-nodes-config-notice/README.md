# C25 — launched-CLI `list-nodes` config-notice + Redis mode-log paired differential

SPEC-0077 workline C, batch C25. **Pure verification**: no production source change.
This harness re-binds the already-paired `list-nodes` stderr *shape* comparison
(c22/c27 `cli-redis-startup-tls-topology`) to the single named leaf
`CLI:list-nodes.ConfigNoticeAndModeLog` with a **per-vector item attribution** and a
dedicated falsifiability mutant — the precise gap the leaf's `accepted_scope`
recorded ("Not covered: a per-vector `ledger_items` attribution").

## Ledger leaf

| id | state before | tier-5 before |
|----|--------------|---------------|
| `CLI:list-nodes.ConfigNoticeAndModeLog` | PARTIAL | official_differential_lifecycle PARTIAL |

Tiers 1–4 (upstream_source_contract, config_projection, component_behavior,
main_server_consumer) were already MET. C25 supplies the item-named paired
locked-Go differential receipt that lifts tier 5 to MET and the leaf to VERIFIED.

## Upstream consumer chain (locked Go v1.13.10, e5655ac2)

`list-nodes` → `listNodes` (`cmd/server/commands.go:170`) → `getConfig`
(`cmd/server/main.go:196-246`):

- `config.InitLoggerFromConfig(&conf.Logging)` (`main.go:208`) installs the logger.
- `if conf.Development { logger.Infow("starting in development mode") }`
  (`main.go:211`).
- `if len(conf.Keys) == 0 { logger.Infow("no keys provided, using placeholder keys",
  "API Key", "devkey", "API Secret", "secret") }` (`main.go:214`).

Then `service.InitializeRouter` → `GetRedisClient` (`protocol/redis/redis.go`) logs
exactly one `connecting to redis` mode line **before** `rc.Ping()`:

| branch | caller | fields |
|--------|--------|--------|
| Sentinel (`len(SentinelAddresses) > 0`) | `redis/redis.go:101` | `sentinel:true`, `addr:[…]`, `masterName` |
| else Cluster (`len(ClusterAddresses) > 0`) | `redis/redis.go:132` | `cluster:true`, `addr:[…]` |
| else simple | `redis/redis.go:145` | `simple:true`, `addr:"…"` |

Precedence: **Sentinel > Cluster > Address**.

## Rust mirror (same CLI path)

`livecloud` `lib.rs:109-127` (`EntryAction::ListNodes`) runs `logging::init` +
`logging::replay_config_notices` (`logging.rs:306`, callers
`cmd/server/main.go:211/214`) before `cli_nodes::list_nodes`, which builds the Redis
client through `redis_tls::redis_endpoint` → `log_redis_mode` (`redis_tls.rs:186`,
callers `protocol/redis/redis.go:101/132/145`). The zap encoder shortens the caller
to its last two path components (`zap.rs:206 short_caller`, unit-tested at
`zap.rs:484`: `cmd/server/main.go:211` → `server/main.go:211`), byte-identical to the
Go `ShortCallerEncoder`.

## Vectors (10, both binaries, same config bytes)

All use `logging: {json: true, level: info}` and point Redis at closed loopback
ports so the Ping fails fast — the notice + mode line are emitted **before** the
dial, so no Redis infrastructure is needed and the observable is deterministic.

| vector | dev | keys | topology | expected notice + mode lines |
|--------|-----|------|----------|------------------------------|
| dev-simple-nokeys | ✓ | ✗ | address | 211 + 214 + simple(145) |
| dev-simple-withkeys | ✓ | ✓ | address | 211 + simple(145) |
| prod-simple | ✗ | ✓ | address | simple(145) |
| dev-sentinel-master | ✓ | ✗ | sentinel + masterName | 211 + 214 + sentinel(101, masterName="mymaster") |
| dev-sentinel-empty-master | ✓ | ✗ | sentinel, masterName="" | 211 + 214 + sentinel(101, masterName="") |
| dev-sentinel-precedence | ✓ | ✗ | sentinel + address | 211 + 214 + sentinel(101) — sentinel wins |
| dev-cluster | ✓ | ✗ | cluster | 211 + 214 + cluster(132) |
| dev-cluster-precedence | ✓ | ✗ | cluster + address | 211 + 214 + cluster(132) — cluster wins |
| prod-sentinel | ✗ | ✓ | sentinel + masterName | sentinel(101) |
| prod-cluster | ✗ | ✓ | cluster | cluster(132) |

## Normalization (scope limit)

Inherited from the proven c22 harness: only the **value** of zap's wall-clock field
(`ts`/`time`) is masked, keeping the key name and the JSON shape of the value
(number → `<number>`, string → `<string>`), so a key rename or a number-to-string
encoder change stays a real divergence. Two comparisons per vector:

1. the full clock-masked line **bytes** (zap field order / rendering / caller),
2. the **parsed** JSON payload with the clock key removed (semantic fields).

No comparison field is deleted. The dial-error stdout, the node table and the
go-redis internal `redis:` diagnostics never match the notice/mode msg keys, so
they are excluded by construction — they are **other items** (see residuals).

## Non-vacuity

Each vector declares the **exact** notice+mode payload sequence the Go oracle must
emit, derived from its development/keys/topology semantics (`expected_payloads`).
A side passes its contract only if it emits that sequence; `Go == Rust` alone would
be vacuous if both emitted nothing. 24 cross-profile controls (12/side) assert the
dev notice present/absent, the placeholder-keys notice present/absent, the three
mode branches, `masterName` set/empty, sentinel/cluster precedence, and that the
mode line is emitted even though the dial fails.

## Falsifiability (positive control)

The mutant no-ops **both** Rust consumers — `logging::replay_config_notices` and
`redis_tls::log_redis_mode` — in a separate `CARGO_TARGET_DIR`. The mutant then
drops the development-mode notices and the `connecting to redis` mode line, so the
notice/mode vectors diverge from the Go oracle and the run FAILS, while the Go side
stays correct. clean and mutant cite the **identical harness sha256** (anti-splicing).
See `positive-control.json` in the evidence directory.

## Out of scope (honest residuals, remain open)

- The Redis **dial outcome** and the seeded-node table (`Redis.TlsServerStartupConsumer`,
  the sentinel/cluster enabled-capability leaves) — covered by their own items.
- The go-redis internal `redis:` discovery diagnostics
  (`CONFIG_BEHAVIOR:Redis.SentinelDiscoveryDiagnostics`) — a separate open item.
- The psrpc message bus / node router over a Sentinel/Cluster endpoint and the
  running-server connection count — runtime surfaces the enabled-capability leaves
  still record as NOT covered.

This harness proves the **config-notice + mode-log CLI surface only**. It does not
claim 100% compatibility; the global status remains IN_PROGRESS_NOT_100.

## Run

```
python3 run-paired.py --go <locked-go-binary> --rust <livecloud-release> --output <fresh-dir>
```

Exit 0 iff every vector matches and every control passes; exit 1 otherwise. Every
original exit code is preserved; no pipe filters the result. Configs are emitted as
YAML strings (no PyYAML dependency).