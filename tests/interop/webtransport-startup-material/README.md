# SPEC-0077 C99 — launched-MAIN WebTransport TLS material startup contract

**Batch:** C99 (verification **plus** one production parity fix in the material loader)
**Leafs:** `YAML:webtransport.tls_cert_file`, `YAML:webtransport.tls_key_file`,
`CONFIG_ENABLED_CAPABILITY:webtransport.port`
**Tier:** `main_server_consumer` (the committed production binary's own startup exit,
stdout diagnostics, listener state and node registration — not a config projection)
**Verdict:** `PASS_LAUNCHED_MAIN_WEBTRANSPORT_MATERIAL` — 15/15 profiles match cross-side,
19/19 cross-profile controls true, against Go `4bc2378e…` and Rust `26219632…`.

## Consumers under test

| side | anchor | semantics |
| --- | --- | --- |
| Go oracle (v1.13.10 @ `e5655ac`) | `pkg/service/webtransport.go` → `tls.LoadX509KeyPair(cert, key)` | both names must be set, then `os.ReadFile` (cert first, key second) and `tls.X509KeyPair`; any failure is fatal during `InitializeServer`, before the readiness line and before the node is registered |
| Go stdlib | `references/go-stdlib-go1.26.3/src/crypto/tls/tls.go:246-361` | the exact refusal vocabulary (`… in certificate input`, `… in key input`, `PEM inputs may have been switched`, `private key does not match public key`, `private key type does not match public key type`) and `os.ReadFile`'s syscall-naming (`open` vs `read`) |
| Rust | `crates/livecloud/src/webtransport.rs:105-151` (`build_server_tls` → `load_cert_files`) | same gate order; the PEM/DER taxonomy is the one already ported for the Redis client pair (`crates/livecloud/src/redis_tls.rs:479-536,757-786`), now shared by both callers |
| Rust | `crates/livecloud/src/server.rs:235-255` | the material step runs before `InitializeServer` completes, so a refusal must leave no listener and no node record |

## What this batch changed in production code

`load_cert_files` previously reported degenerate material with rustls's own wording, and
reported a directory as an `open` failure. Measured against the oracle on candidate
`babc0fab…`, four rows diverged:

| Profile | Go oracle | Rust before | Rust now |
| --- | --- | --- | --- |
| `wt-empty-cert-file` | `tls: failed to find any PEM data in certificate input` | `failed to parse private key: no certificates` | matches |
| `wt-junk-key-file` | `tls: failed to find any PEM data in key input` | `failed to parse private key: no items found` | matches |
| `wt-cert-key-mismatch` | `tls: private key does not match public key` | `webtransport tls config: … keys may not be consistent: KeyMismatch` (raised in `bind`, after material load) | matches, raised at the material step |
| `wt-cert-is-directory` | `read <path>: is a directory` | `open <path>: is a directory` | matches |

The fix reuses `redis_tls::check_key_pair_pem` / `check_key_pair_der` (now `pub(crate)`) and
splits the file read into `File::open` + `read_to_end` so Go's `os.ReadFile` op naming
survives. Unit coverage: `webtransport::tests::tls_material_refusals_use_go_key_pair_wording`
pins all six sentences (including `tls: found a certificate rather than a key in the PEM for
the private key` and `tls: private key type does not match public key type`) against the
committed `crates/rtc-tls` fixtures.

## Method

[`run-paired.py`](run-paired.py) launches the **committed** production binary and the fixed
Go oracle with **identical YAML** (same file, same bytes, sha recorded per side) under a
stripped env (`LIVEKIT_`/`LIVECLOUD_`/`OTEL_` prefixes removed). Certificates and keys are
fresh OpenSSL EC P-256 pairs generated into the output dir (`openssl version` recorded);
private material never leaves that dir and is deleted with the receipt.

Two stream files are kept per run (`stdout.txt` / `stderr.txt` for refusals,
`server.stdout` / `server.log` for starts) because Go writes the `LoadX509KeyPair` refusal to
**stdout** while the daemon log goes to stderr — comparing only one stream would hide it.

Per profile the harness records: raw stdout, raw stderr, exit code, whether the process
self-exited or needed the escalating SIGTERM → SIGTERM → SIGKILL sequence, whether the HTTP
port accepts, whether the WebTransport UDP endpoint is bindable (Go binds without
`SO_REUSEADDR`, so "bindable" is a real listener probe), the ordered `webtransport` log
messages around the readiness line, and — when a private Redis is running — the node hash
**while the daemon is alive** and again after exit.

That alive-sample is load-bearing: `RegisterNode` happens inside `Start()`, so reading Redis
only after shutdown reports the post-deregistration state on both sides and proves nothing.
The pre-fix run recorded exactly that vacuity (`nodes.count == 0` on both sides).

## Profiles (15)

| Profile | Stimulus | Contract asserted on each side |
| --- | --- | --- |
| `wt-disabled-port0` | `webtransport.port: 0` | daemon starts, serves, and logs **no** `webtransport listener started` |
| `wt-port-no-material` | port set, no cert, no key | exit 1, `webtransport requires webtransport.tls_cert_file/tls_key_file`, no listener |
| `wt-cert-only-no-key` | cert only | same both-names refusal (proves the gate is *both* names, not "cert present") |
| `wt-key-only-no-cert` | key only | same both-names refusal (proves the key alone does not open the loader) |
| `wt-missing-cert-file` | missing cert path + existing key | `open <cert path>: no such file or directory` — names the **cert** |
| `wt-missing-key-file` | existing cert + missing key path | `open <key path>: no such file or directory` — names the **key** (so the reader is not one combined check) |
| `wt-missing-both-files` | both missing | cert path named (read order is cert first) |
| `wt-empty-cert-file` | zero-byte cert | `tls: failed to find any PEM data in certificate input` |
| `wt-junk-key-file` | non-PEM key | `tls: failed to find any PEM data in key input` |
| `wt-cert-key-mismatch` | cert A + key B | `tls: private key does not match public key` |
| `wt-cert-is-directory` | cert path is a directory | `read <path>: is a directory` |
| `wt-valid-material` | cert A + key A | starts, listener line strictly **before** the readiness line, UDP held while alive, released on exit |
| `wt-dev-selfsigned` | `--dev`, no material | starts, dev WARN strictly before the listener line, UDP held |
| `wt-refusal-keeps-node-unregistered` | refusal + Redis | Redis node hash stays **empty** (registration ordering) |
| `wt-valid-registers-node` | valid material + Redis | hash has exactly 1 node with `ip=127.0.0.1` **while alive**, and 0 after exit |

`cert_read_before_key_*` and `both_names_gate_specificity_*` are the two controls that keep
the file-ordering and gate claims falsifiable; `pem_error_text_specificity_*` requires the
three PEM refusals to be three *different* strings.

## Falsifiability (positive control)

Mutant: the two shared taxonomy calls are removed from `load_cert_files`, i.e. the Go
`X509KeyPair` vocabulary is skipped and the shapes fall through to rustls (the pre-fix
behaviour). Built in its own target dir (`/tmp/c99-build/mutant-a`, sha256 `00e53a8c…`,
recorded as the mutant's identity; source tree left dirty during the run so
`rust_dirty_paths` shows the mutation).

- status **FAIL**, failing exactly `wt-empty-cert-file`, `wt-junk-key-file`,
  `wt-cert-key-mismatch` — the three rows the taxonomy owns;
- still green: both-names gate rows, both missing-file rows, `wt-cert-is-directory`
  (proved by the *other* half of the fix, measured on candidate `babc0fab…` where it printed
  `open … is a directory` against the oracle's `read … is a directory`), the two start rows,
  dev self-signed, and all four Redis ordering rows plus every control.

So neither refusal family can be satisfied by a generic error, and the directory row is
discriminated independently of the PEM row.

## Run

```bash
python3 /Users/zoujiaqing/projects/livekit-rs/tests/interop/webtransport-startup-material/run-paired.py \
  --go   /tmp/c99-build/livekit-server-go \
  --rust /Users/zoujiaqing/projects/livekit-rs/target/release/livecloud \
  --output <fresh-dir>            # --skip-redis drops the two ordering rows
```

## Out of scope (honest residual)

- Only the WebTransport **startup** contract is claimed: no QUIC handshake, no h3/WEBTRANSPORT
  session, no certificate SAN/expiry handling, and no client trust store. Data-plane parity is
  the C74/agent lanes.
- `webtransport.host`, mtu/discovery knobs, datagram queue size and multiple bind addresses
  are not driven here.
- RSA and Ed25519 listener material, and a PEM block whose label is `CERTIFICATE` but whose
  body does not base64-decode, are **not** exercised; the classification fall-through in that
  last shape stays rustls-worded (recorded in `execution.json`, not promoted).
- Generated flags (`--webtransport.tls_cert_file`) and the env channel are separate ledger
  rows; this batch drives YAML only.

Pure loopback: the daemon binds `127.0.0.1`, the Redis instance is a private child process,
and no route/interface/proxy state is touched.
