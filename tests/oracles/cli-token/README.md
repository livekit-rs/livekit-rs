# F10 CLI create-join-token key-file boundary (isolated, NOT_RUN)

No repository Rust/Cargo input was modified. Preserve `/tmp/f08-config-functional`
independently. No Go/Rust compiler, executable fixture, server or browser was run.

## Selected actual gap

The fixed server command registry (`cmd/server/main.go`) includes generate-keys,
ports, hidden create-join-token, list-nodes, help-verbose and framework help/version.
Generate-keys/ports/list-nodes already have F05 real differential fixtures. This
slice addresses a concrete successful operation missing from create-join-token:
`commands.go:107–167` loads its key file with stat/open/YAML decoder directly.
It never calls the server's `ValidateKeys`. Current Rust reuses `load_key_file`
and refuses a readable 0644 file, so a valid official CLI operation fails.

`create-join-token-file-permissions.patch` changes only:

1. `lk-config/src/keys.rs`: extract the existing read/YAML logic unchanged and add
   command-specific stat+decode loader; strict server loader retains permission
   check. Existing assertions remain; new real-file assertions cover both modes.
2. `lk-config/src/load.rs`: only CreateJoinToken calls the new loader.
3. New `lk-config/tests/create_join_token.rs`: full public config action path,
   readable 0644 file, same-file server rejection, empty/empty-map failure and
   inline-key precedence over a missing file. Tests create owned temp files with
   scope cleanup; no server or network is started.

No auth/token minting algorithm, TTL, grants, runtime server permissions, package
version or dependency changes. Direct YAML decode semantics beyond these cases
remain measured by the official executable, not claimed correct from inspection.

## Independent source execution oracle

`compare-token-binaries.py` invokes the **unchanged fixed Go server binary** and
Rust CLI binary for the same 12 file/command cases. It copies no loader algorithm
or expected Go error strings. It does not start a server. The fixture requires
macOS/Unix permission semantics; Windows coverage is not implied.

For every successful token on each side it independently verifies HMAC-SHA256
against the fixture key selected by the actual issuer, exact HS256 header, exact
video grant including recorder false permissions, requested subject/identity,
`iat==nbf`, real invocation time bounds and exact 30-day expiry. It compares all
remaining actual claims. Map iteration can choose any genuinely configured key;
issuer nondeterminism is accepted only after validating the corresponding actual
signature. No token, signing secret, system environment or command config is
persisted. Stderr is retained for diagnostics but not declared equal (logger
identity/timestamps are outside stdout behavior). All actual errors/exit codes
are preserved and compared; no waived cases or expected-error rewrite.

Old candidate should demonstrate the 0644 permission mismatch before application.
Root must preserve this red evidence separately before green execution.

```sh
/usr/bin/python3 /tmp/f10-cli-functional-completeness/compare-token-binaries.py \
  --official /path/to/pinned-unmodified-livekit-v1.13.10 \
  --rust /path/to/old-livecloud \
  --output /tmp/f10-token-red.json
```

After root reviews/applies the patch (from repository root, `patch -p1` or exact
manual edits), it owns dependency/source checks and executes meaningful tests:

```sh
cd livekit-rs-server
cargo test --locked -p lk-config --lib keys::tests::key_file_permissions_and_replace
cargo test --locked -p lk-config --test create_join_token
# Root's full workspace/Clippy/build gates remain mandatory.
```

Then rerun the same executable oracle against the newly built candidate with a
new output file. No actual Rust or Go result exists yet; syntax checks are not
behavioral passes. No complete CLI100 claim follows from this 12-case slice.

## Known boundaries, not silent closures

- This does not claim all YAML malformed/type/coercion cases are byte-identical.
  Actual errors for these are a future source oracle expansion if needed.
- Multiple-key first selection is unspecified by Go map iteration.
- Recorder is a real signed permission restriction, not a synthetic client.
- Successful token verification here does not itself exercise an RTC room join.
- No real Redis nodes, interface selection, ports or other command gate is
  replaced by this isolated regression.
- CLI empty key/secret, explicit empty required flags, and environment-specific
  filesystem failures remain separate checks; no public token validation is
  weakened to accept them.

`SOURCE.json` locks the exact source and Rust bases. Check those base hashes
before integration, because other owners may change shared code later.
