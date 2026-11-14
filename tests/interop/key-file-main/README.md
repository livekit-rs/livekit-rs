# SPEC-0077 C22 — launched-MAIN YAML `key_file` consumer paired differential

Item: **`YAML:key_file`** (PARTIAL → VERIFIED, scoped).

Proves the YAML-surface `key_file` startup reader and the credentials it loads
reach the **assembled MAIN server's auth consumer**, with a paired locked-Go
oracle vs Rust candidate differential. Complementary to
`tests/interop/cli-generated-raw-keyfile` (the generated CLI/ENV raw-byte path,
mode-0600 files, `keys:{}`), which never exercises the YAML-surface
others-permission gate, file-over-inline REPLACEMENT, empty-file EOF, or simple
YAML `~`/`$VAR` expansion.

## Run

    python3 tests/interop/key-file-main/run-paired.py \
      --go   /tmp/c22-build/livekit-server-go \
      --rust target/release/livecloud \
      --output <evidence-dir>

`--interop-package` defaults to `tests/interop/package.json` (declares
`livekit-server-sdk` 2.19.2); `--node` defaults to `node`.

## Binaries (locked)

- Go oracle: livekit-server **1.13.10**, source `e5655ac20267fa5005dccaf3f0e3bfc3ea688c07`,
  binary sha256 `4bc2378e636740b29093061036249ffc9ebbef91f4ece1de12177c8dfee1e7dc`.
- Rust candidate: `livecloud`, binary sha256
  `d1dad0fcdeefd355dd01f553a7b1d300c0cca6abf3d4832791b4dc7920640554`.

## Profiles (7)

auth profiles start MAIN and drive the public `livekit.RoomService` Twirp surface
with the **unchanged official `livekit-server-sdk` 2.19.2** (`probe.mjs`, no
browser, no SDK modification). The probe mints and independently verifies HS256
JWTs, then:

- `accept` attempt: bounded **actual authenticated readiness** (≤20s, not sleep)
  on `ListRooms`, then Create/List/Delete a private room (real side effects).
- `reject` attempt: a single `ListRooms` that must fail **cleanly** with
  401/`unauthenticated` — a network error (server down) or a success is NOT a
  valid reject.

| profile | YAML `key_file` | inline `keys` | observable |
|---|---|---|---|
| `yaml-keyfile-valid` | `{filekey: FS}` mode 0600 | `{}` | file secret accepts + CRUD; wrong secret + unknown key reject |
| `yaml-keyfile-over-inline` | `{filekey: FS}` mode 0600 | `{inlinekey: IS}` | file secret accepts; **inline key is UNKNOWN** (file REPLACED inline); file+inline-secret rejects |
| `yaml-keyfile-env-expand` | `$C22_KEYDIR/keys.yaml` | `{}` | `$VAR` expansion reaches loader → file secret accepts |
| `yaml-keyfile-tilde-expand` | `~/keys.yaml` (HOME=tmp) | `{}` | `~` expansion reaches loader → file secret accepts |
| `yaml-keyfile-others-perm` | `{filekey: FS}` mode **0644** | `{}` | exit 1, stdout `key file others permissions must be set to 0` |
| `yaml-keyfile-empty` | empty file mode 0600 | `{}` | exit 1, stdout `EOF` |
| `yaml-no-keys-nondev` | (none) | `{}` | exit 1, stdout `one of key-file or keys must be provided` |

`FS`/`IS` are >32-char fixture secrets. The no-keys profile runs **without
`--dev`**: Go `getConfig` (`cmd/server/main.go:213-220`) injects a `devkey:secret`
placeholder in dev mode, so `ErrKeysNotSet` is only observable in non-dev. All
profiles run non-dev for a clean comparison (the file supplies the keys).

## Contract (falsifiable)

Each side must **independently** satisfy its profile contract, AND the two sides
must agree (auth: per-attempt `(label,expect,accepted)` tuples + CRUD;
startup_error: exact exit + stdout + stderr). Cross-profile controls assert the
consumer genuinely responds to `key_file` (file secret authenticates; inline key
replaced; `~`/`$VAR` expansion effective; permission gate / EOF / no-keys text
exact). A server that ignored `key_file`, kept inline keys, skipped the
permission gate, or dropped expansion would flip a boolean or an exit/text and
FAIL. See the file-over-inline merge-instead-of-replace positive-control mutant
(`positive-control/mutant/BATCH.json`, status FAIL): it fails exactly
`yaml-keyfile-over-inline` (the inline key stays known) and passes the other six
profiles, proving the harness is specific, not a fixed pass.

## Consumer chain (Rust)

`lk-config/src/load.rs:686` `cfg.keys = load_key_file(path)` (Mode::Run,
key_file preferred over inline; `load.rs:640` `expand_home(expand_env(...))`) →
`lk-config/src/normalize.rs:857` `ServerConfig.keys` →
`livecloud/src/server.rs:221` `CompatServices::new` →
`lk-compat/src/state.rs:78` `KeyStore::from_config` →
`lk-compat/src/auth/mod.rs:32` → auth middleware. Go counterpart:
`pkg/config/config.go:820-855 ValidateKeys` (key_file preference, `0o007`
permission gate, YAML decode REPLACING `conf.Keys`, `ErrKeysNotSet`),
`config.go:707` `homedir.Expand(os.ExpandEnv(conf.KeyFile))`,
`cmd/server/main.go:258`.

## Privacy / cleanup

JWTs, room IDs and the **file secret** never enter evidence: `probe.mjs` emits
only accept/reject booleans + SDK/Node identity, and the file secret lives only
in the removed temp key file (the config carries the key_file *path*, never its
contents). The `over-inline` config carries a synthetic inline fixture secret
(not a real credential), mirroring the C21 fixture-key practice. Key files live
in a private temp dir removed after the owned children exit. Owned servers
SIGINT/await actual exit; forced kill is failure. Server stdout/stderr are
captured to per-profile files. No SDK / `node_modules` is changed.