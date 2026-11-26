# SPEC-0077 C103 — generated flags and the key-file channel, judged on the running node

**Batch:** C103-20261127-generated-flag-channels (evidence only — **no production code change**)
**Leafs:** `generated_flags:--webtransport.port`, `generated_flags:--webtransport.tls_cert_file`,
`generated_flags:--webtransport.tls_key_file`, `generated_flags:--key_file`
**Tier:** 5 — both sides are the launched binaries, one locked Go oracle and the acceptance Rust image,
on identical YAML bytes; nothing reads a config struct.
**Verdict:** `PASS_LAUNCHED_GENERATED_FLAG_CHANNELS` — 28/28 profiles match cross-side, 312 legs,
46 controls, against Go `4bc2378e…` (source `e5655ac…`) and Rust `f6fe7f03…`.

C99 closed the **YAML** half of the WebTransport startup material contract and listed the residue
explicitly: *"Generated flags (`--webtransport.tls_cert_file`) and the env channel are separate ledger
rows; this batch drives YAML only."* This lane is that residue, plus the `key_file` field, which C22
closed as a YAML surface and C100 closed as the base flag `--key-file`, but whose **generated**
spelling `--key_file` had never been launched.

## Mechanism read from the oracle, then measured

| fact | upstream | measured here |
| --- | --- | --- |
| a generated flag name **is** the YAML path, and its env name is `LIVEKIT_` + that path upper-cased with dots → underscores | `pkg/config/config.go:940-1035` (`GenerateCLIFlags`, each with `Sources: cli.EnvVars(envVar)`) | `--webtransport.port` / `LIVEKIT_WEBTRANSPORT_PORT`, `--webtransport.tls_cert_file`, `--webtransport.tls_key_file`, `--key_file` / `LIVEKIT_KEY_FILE` all move a live effect |
| `--key-file` and `--key_file` are two **distinct** flags for one field | `ToCLIFlagNames` dedups only on an exact name, `config.go:803`; `cmd/server/main.go:52-101` registers the dash spelling | `key-file-dash-beats-underscore` passes both, and the file the dash names is the one the node signs with |
| argv order does not decide: the generated loop runs first, the explicit `if c.IsSet("key-file")` branch after | `config.go:1074-1120` (`updateFromCLI`) | `key-file-underscore-first` reverses argv order and lands on the same key as `key-file-dash-beats-underscore`; `key_precedence_pair_pinned` requires the two cells to be identical |
| `key_file` is consumed by `ValidateKeys`, which prefers the file over a `--keys` map | `config.go:820-855` | `key-file-beats-keys-flag`: the file's kid answers, the flag map's kid gets `401` |
| WebTransport material is loaded by `tls.LoadX509KeyPair` during `InitializeServer` | `pkg/service/webtransport.go` | the listener binds, or the process exits 1 before readiness |
| Rust mirrors the same table | `crates/lk-config/src/generated.rs:392`, `crates/lk-config/src/cli.rs:691`, `crates/lk-config/src/keys.rs:83-99`, `crates/livecloud/src/webtransport.rs:105-151` | every row above, same exit, same sentence, same cell |

## What makes a row here an effect rather than a projection

* **`webtransport.port`** — the UDP endpoint the *process* holds, read from the socket table
  (`lsof -nP -iUDP`) while it is alive and again after it exits. The C99 lane probed "can I bind this
  port instead"; on this host that answer was not trustworthy, so the lane reads the owner table.
  `webtransport listener started` must sit strictly before `starting LiveKit server`, on the raw log text.
* **`webtransport.tls_*_file`** — `wt-flag-material-beats-yaml` and `wt-flag-material-mismatch` are the
  same three inputs crossed: YAML carries a mismatched pair with the flags valid, then flags mismatched
  with YAML valid. A clean start on one and a refusal on the other is what proves the *flag* files were
  the ones opened; no error string alone can show that.
* **`key_file`** — which secret the **running node** accepts. `POST /rtc/v1/validate` with an HS256
  token: `400 join_request is required` means the key set signed a grant the handler parsed
  (`handler`), `401 invalid API key` means the node does not hold that kid (`bad-key`). Every key row
  records both cells; a row that only ever says `handler` would prove nothing, which is what
  `auth_claim_shows_both_classes` holds the lane to.
* **refusals** — exit code, whether the daemon self-exited (never SIGKILL), no TCP listener and no UDP
  endpoint left behind, and the sentence printed. `sentence_after()` compares the *line* carrying the
  needle, because both runtimes print a startup refusal as one bare line and the following lines
  legitimately differ.
* **registry** — the node hash is sampled **while the daemon is alive** and again after exit
  (`node_alive`, `node_row_gone_after_exit`). Read only after shutdown it is empty on both sides and
  proves nothing — the vacuity C99 recorded on its first pass.

## Profiles (28)

WebTransport generated flags (12): `wt-flags-valid`, `wt-flag-port-beats-yaml`,
`wt-flag-material-beats-yaml`, `wt-flag-material-mismatch`, `wt-env-port-and-material`,
`wt-env-beats-yaml`, `wt-cert-flag-without-key`, `wt-empty-material-flags`,
`wt-empty-cert-overrides-yaml`, `wt-port-empty-string`, `wt-port-zero-beats-yaml`,
`wt-missing-files-flags`.

| row | stimulus | shared outcome |
| --- | --- | --- |
| `wt-flags-valid` | port + cert + key on argv, YAML has no webtransport block | exit 0, `<WT1>` held, listener line before readiness, one node while alive |
| `wt-flag-port-beats-yaml` | flag port over a YAML port | flag port bound, YAML port never bound in either socket family |
| `wt-flag-material-beats-yaml` | flag pair valid, YAML pair mismatched | starts — the loader opened the flag files |
| `wt-flag-material-mismatch` | flag pair mismatched, YAML pair valid | exit 1, `tls: private key does not match public key`, no listener |
| `wt-env-port-and-material`, `wt-env-beats-yaml` | same through `LIVEKIT_WEBTRANSPORT_*` | same listener, YAML loses |
| `wt-cert-flag-without-key`, `wt-empty-material-flags`, `wt-empty-cert-overrides-yaml` | half a pair, empty strings | `webtransport requires webtransport.tls_cert_file/tls_key_file` — the empty string is a value, and an empty flag overrides a complete YAML pair |
| `wt-port-empty-string` | `--webtransport.port ''` | `invalid value "" for flag -webtransport.port: strconv.ParseUint: parsing "": invalid syntax` + `Incorrect Usage`, exit 1, nothing bound |
| `wt-port-zero-beats-yaml` | `0` over a YAML listener | starts serving with **no** UDP endpoint and no listener line |
| `wt-missing-files-flags` | both paths absent | the **certificate** path is named first — the read order, not one combined check |

`key_file` generated channel (16): `key-file-flag-auth`, `-underscore-flag`, `-env`,
`-dash-beats-underscore`, `-underscore-first`, `-flag-beats-env`, `-beats-keys-flag`, `-empty-value`,
`-env-expansion`, `-world-readable`, `-dir-755`, `-dir-700`, `-missing`, `-empty-file`, `-yaml-seq`,
`-short-secret`. The first nine are the channel/precedence set; the last seven are the `ValidateKeys`
failure vocabulary — `key file others permissions must be set to 0`,
`stat <path>: no such file or directory`, `EOF`, `cannot unmarshal !!seq into map[string]string`, and a
short secret that warns and still serves. `dir-755` versus `dir-700` is the ordering discriminator:
mode `0755` fails the `0o007` test before the read, mode `0700` passes it and then fails the read with
`read <path>: is a directory` — identical sentences on both sides, and `log_not` keeps each row from
borrowing the other's wording.

## Two divergences, pinned rather than averaged

1. **a WebTransport-material refusal leaves a key behind on Rust.** Go's store holds nothing after
   `wt-flag-material-mismatch`/`wt-cert-flag-without-key`/`wt-empty-material-flags`/
   `wt-empty-cert-overrides-yaml`/`wt-missing-files-flags`; Rust has already written `livekit_version`
   by then. Both register **no node**, so the rows claim `store_no_node_after` (the shared guarantee)
   and `webtransport_refusal_marker_split_pinned` pins the asymmetry in that direction. A key-file
   refusal, by contrast, happens before either side touches the store, and those rows keep
   `store_left_clean`.
2. **Go logs every failing validate call, Rust logs none.** The oracle prints one
   `error handling request` WARN per `/rtc/v1/validate` that ends in an HTTP error status — the `401`
   bad-key shape and the `400` join_request shape alike — while the Rust node answers byte-comparable
   statuses and prints nothing. `auth_rejection_logging_pinned` counts the lines against the number of
   probes on each row, so a side that logs only sometimes fails it. The parity claim is the answer
   status and the accepted key set, not the log volume.

## Falsifiability

Two axes, both required.

*Behaviour* is falsified by construction: every precedence row has its inverse (`flag beats YAML` vs
`flag mismatches while YAML is valid`; `--key-file a --key_file b` vs `--key_file b --key-file a`), the
auth channel says two different things on every key row, and `log_not` claims stop a generic
"something was printed" from passing.

*The receipt's own gates* are falsified by `--selftest-gate`, which re-runs `finish()` on in-memory
copies under 69 named mutations and requires each to redden the check it targets — unbound listener,
losing port bound, refusal→start, order swap, leaked socket, flipped auth cell, missing auth cell,
blanked refusal needle, flipped exit, collapsed bound/silent split, averaged logging split, truncated
inventory, store not flushed, SIGKILL used, node row leaked, version marker lost, machine/host address
leak, secret leak, off-limit port named, binary/oracle/reference/hash drift, empty raw stream, and the
untouched baseline. It launches nothing and proves it: the receipt digest is recorded before and after
(`sweep_writes_no_receipt`).

The audit also refuses to grade a target that does not exist: `FAILED_LEG`/`FAILED_CONTROL` cases are
checked against the unmutated leg ids and control names first, so a typo cannot read as
"discriminated". `--selftest-gate` on the filed receipt: 69/69.

## Identity discipline

`run-paired.py` **imports** the accepted C102 lane (`importlib` on
`tests/interop/base-flag-channels/run-paired.py`) for the launcher, port allocator, socket readers,
owned-store helpers and address masker instead of copying them; copying would leave the C102 harness
open to drift while its filed receipt still claimed that file. `imported_primitives_are_the_accepted_lane`
compares the recorded C102 digest with `fff0615a…` (the hash C102 filed), so this receipt states which
code it ran on. `record_*_matches_pinned` compares recorded hashes with the constants, `binaries_unchanged`
re-hashes the images at the end, and `harness_unchanged_since_run` binds the verdict to this file.

One owned `redis-server` on an allocated port, flushed and `DBSIZE`-checked before every launch
(`store_flushed_before_every_launch`). `7881`, `7882` and the host store `6379` are counted before and
after and never named in a profile; the receipt carries role tokens (`<HTTP> <STORE> <WT1> <WT2> <YRA>
<YRB>`), a `<WORK>` path alias, keyed IP tags and secret aliases, and no machine address. Loopback only:
no route, interface, proxy or alias is touched.

## Run

```bash
python3 tests/interop/generated-flag-channels/run-paired.py \
  --go   /tmp/c99-build/livekit-server-go \
  --rust target/release/livecloud \
  --out  /tmp/c103-lane --record                # 28 profiles, ~8 min

python3 tests/interop/generated-flag-channels/run-paired.py --report       /tmp/c103-lane/execution.json
python3 tests/interop/generated-flag-channels/run-paired.py --selftest-gate /tmp/c103-lane/execution.json
```

`--report` re-derives the verdict offline from the receipt; `--only` produces
`PARTIAL_SUBSET_NOT_ACCEPTANCE` and the identity-keyed controls read False rather than raise, because a
subset cannot claim a pair.

## Out of scope (honest residual)

* `--webtransport.port` is claimed on the **listener**, not the data path: no QUIC handshake, no h3
  session, no MTU/discovery/datagram-queue knobs (C99's residuals still stand).
* `key_file` **contents** beyond `kid: secret` pairs, an encrypted key file, and the
  `os.UserHomeDir` (`~`) expansion branch are not driven — `key-file-env-expansion` proves `$ENV`
  expansion only, because `~` would make the fixture depend on the host account.
* The generated **env** spelling of `key_file` is paired against argv and YAML, but a same-name
  collision between `LIVEKIT_KEY_FILE` and the base `KEY_FILE` env is not probed.
* The two divergences above are recorded, not fixed: neither is a wire-visible incompatibility, and
  imitating Go's log volume (or its write ordering) is a behaviour change this batch is not scoped to
  make. Both are listed for the attribution queue in the C103 spec section.
* Flags generated for `keys_file`, `key_pem` and `turn.key_file` (the other key-material surfaces) are
  separate rows, as are the CLI-side `--keys` map semantics beyond the precedence half measured here.
