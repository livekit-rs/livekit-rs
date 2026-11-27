# SPEC-0077 C105 — the TURN credential channels, judged on the `ice_servers` a real join returns

**Batch:** C105-20261127-turn-credential-join (evidence only — **no production code change**)
**Leafs:** 18 — nine `YAML:rtc.turn_servers*`, `YAML:turn.ttl_seconds`, `generated_flags:--turn.ttl_seconds`, plus the
join-response consumer half of `YAML:keys`, `YAML:turn.{domain,enabled,udp_port,tls_port}` and `YAML:rtc.{node_ip,stun_servers}`
**Tier:** 5 — both binaries launched on identical YAML bytes on allocated loopback ports, then **one real RFC 6455 join per
profile** (76 joins, 12 of them repeated against one live process). The credential is judged on what the daemon put in
`SignalResponse.join.ice_servers`, never on a config struct or a unit-tested helper.
**Verdict:** `PASS_LAUNCHED_TURN_CREDENTIAL_JOIN` — 33/33 profiles match cross-side, 1518 legs, 80 controls, all 14
`status_gates` true, against Go `4bc2378e…` (source `e5655ac…`) and Rust `f6fe7f03…`.

C104 closed the listeners and the material and named what it could not reach: *"The SDK join response (`ice_servers` /
`turns:<domain>:443?transport=tcp`) is the real consumer of `turn.domain` and is not observed by this lane"*, and
*"`ttl_seconds` has no launched consumer channel, so its two rows stay below `VERIFIED`"*. C9x had measured the URI shape in
a unit test and C23 had measured a relay, but nothing had said what credential string a joining participant actually receives,
whether it reproduces from the secret the YAML named, or how long it lives. That is the whole subject of this lane.

## Mechanism read from the oracle, then measured

| fact | upstream | measured here |
| --- | --- | --- |
| `LoadTURNSecrets` runs during startup and a failure aborts the process | `cmd/server/main.go:258-266` | six refusal rows: exit `1` on both sides, self-exited inside the grace window, no port bound in either family, store left at zero rows, and the upstream sentence verbatim |
| a list element with `secret` yields `username = "<expiry>:<participantSID>"` and `credential = base64(HMAC-SHA1(secret, username))` | `pkg/service/roommanager.go:1059-1155`, `crates/lk-compat/src/signal/ice.rs:145-161` | 14 `rest` rows, every credential 28 chars, each reproducing from **the secret that row configured** and from no other (9 inline, 4 file, 1 non-UTF-8; 14 `foreign=no` cells) |
| a list element with a literal pair is passed through unchanged | `pkg/config/config.go:201-217`, `ice.rs:62-141` | 5 `static` rows: the username comes back byte-for-byte (`<USR>`), the credential digest equals the YAML value's digest, and the class stays `static` even beside a blank secret |
| the embedded username is base62(`apiKey\|participantID\|expiry`), the password base62(sha256(`keysecret\|participantID\|expiry`)) | `pkg/service/turn.go:292-334`, `crates/lk-compat/src/turn_auth.rs:43-80` | 8 `embedded` rows. The password width is **not** constant — 43 or 44 chars depending on the digest's leading bits — so every embedded length leg is a band, and both widths were measured |
| external ttl: `0` → 14400, negative → 300 at generation, `>86400` capped at load; embedded ttl clamped both ways at load into `[120, 86400]`, default 300 | `pkg/config/config.go:52-75`, `:903-941`, `crates/lk-config/src/load.rs:221-245` | seven distinct credential lifetimes read off the wire — 120, 240, 300, 360, 600, 14400, 86400 — each in its own ±3 s band on both sides, plus `ttlshape` for the expiry form |
| an inline `secret` beats a configured `secret_file`, with a warning; `secret: ""` stays blank | `config.go:857-898` | `ts-secret-beats-file` (precedence line present on both sides, only the inline candidate reproduces) and `ts-blank-inline-file-wins` (no precedence line, the file signs) |
| `secret_file`: stat → others-permission filter → read → trim → empty is an error | `config.go:857-898`, `crates/lk-config/src/keys.rs:113-137` | `0600` works, `0604` refuses with `turn secret file others permissions must be set to 0`, whitespace-only with `turn secret is empty`, a directory with `is a directory`, a missing path with `no such file or directory`; a **non-UTF-8** file is used as the raw bytes and its HMAC matches |
| `hasSTUN` gates the default STUN fallback | `roommanager.go:1059-1155` | `no-turn-default-stun` advertises exactly upstream's own three URLs, and `ts-stun-list-append` puts the configured list ahead of them |
| the embedded block advertises `turn:<node_ip>:<udp_port>?transport=udp` plus `turns:<domain>:443?transport=tcp` | `roommanager.go:1059-1155` | `emb-uri-tls-443`: the advertised TLS URL says `:443` while the socket the process holds is `<TLS1>` — the quirk is kept, with `tcp-TLS1` held and `TU1`/`TU2` absent from both families |
| the embedded key id comes from `getFirstKeyPair()`, which ranges a **map** | `pkg/service/roommanager.go:324, 1203-1208` | `emb-two-keys`: over 12 joins of one Go process the username named `c105k` 11 times and `c105m` once; Rust named `c105m` 12 times. All 24 passwords reproduced from the secret of the key their own username named |

## What counts as an effect here

* **The join itself.** `GET /rtc/v1?join_request=…&protocol=15` with a bearer JWT over a raw loopback socket, `101 Switching
  Protocols`, then the first binary frame decoded by a field-number codec written for this file. There is no SDK in the
  path, which is what `join_client_is_the_harness_itself` and `sdk_substitution_is_explained` protect: the wire format is
  this lane's claim, not a library's. 54 `upgrade` cells (27 per side), 54 `frames`, 66 `exit`, 54 `node`, 54 `room-sid`,
  54 `join-sid`, and the 6 refusal rows claim `join: not-attempted` with `ice-count 0` so an empty stream cannot read as a
  pass.
* **The credential, not the log.** Every row stores digests (`username_sha256`, `cred_sha256`, `cred_len`) and recomputes
  the candidate forms — `rest_credential(secret, username)` and `embedded_password(secret, sid, expiry)` — against the
  **four** secrets the profile configured (`ext`, `file`, `bin`, `foreign`) or the three the embedded rows could use (`k`,
  `m`, `x`). A `match` on the foreign secret reddens `foreign_secret_never_matched`; a row where nothing reproduces is
  `no`, not skipped: 164 recorded verdict cells (82 per side over the 30 rows) and 82 cross-side equality legs over them.
* **The lifetime.** A `band` leg around the claimed seconds, derived from the expiry inside the username minus the clock at
  the moment the frame arrived, so the clamp is read as a duration rather than a string. 60 band legs, 30 `ttlshape` pairs.
* **The SID dependency.** `sid_ok` is recorded per row and `sid_ok_never_assented_for_another_participant` rejects a `true`
  attached to a credential that names someone else.
* **Repeat joins against one live process.** `emb-two-keys` joins 12 times without restarting, and `kids-count`,
  `kids-named`, `kids-known`, `kids-pinned` grade the *spread* — which is how a Go map-iteration divergence can be measured
  instead of asserted.
* **The sockets and the store.** `<HTTP>`/`<TLS1>`/`<TU1>`/`<TU2>` claimed present or absent by family while alive and after
  exit (154 socket legs: 64 `release-*`, 88 `not-*`, 2 present-port claims), the owned `redis-server` flushed and
  `DBSIZE`-checked before each of the 66 launches and re-read after exit (`node-gone` on all 6 refusal rows).

## Profiles (33)

External list, credential modes (5): `ts-secret-inline`, `ts-secret-file-0600`, `ts-secret-beats-file`,
`ts-blank-inline-file-wins`, `ts-static-pair`. Blank-over-static and list order (2): `ts-empty-secret-with-static`,
`ts-two-servers-order`. Protocol spellings (3): `ts-protocol-tls-static`, `ts-protocol-omitted`,
`ts-protocol-unknown-quic`. External ttl (4): `ts-ttl-zero-explicit`, `ts-ttl-600`, `ts-ttl-negative`, `ts-ttl-over-max`.
Per-row ttl (1): `ts-ttl-per-row`. Embedded ttl (6): `emb-ttl-default`, `emb-ttl-120`, `emb-ttl-clamp-high`,
`emb-ttl-clamp-negative`, `emb-ttl-flag-beats-yaml`, `emb-ttl-env-beats-yaml`. Embedded URI shape (3): `emb-two-keys`,
`emb-uri-tls-443`, `emb-disabled-with-ts`. STUN (2): `no-turn-default-stun`, `ts-stun-list-append`. Binary secret (1):
`ts-file-binary-bytes`. Refusals (6): username-only, no credentials, missing file, world-readable file, empty file, file is
a directory.

## Two divergences, recorded rather than averaged

1. **Which API key the embedded username names.** Go hands `iceServersForParticipant` the result of `getFirstKeyPair()`
   (`roommanager.go:324, 1203-1208`), which ranges `config.Keys` — so with two keys configured the advertised key is an
   arbitrary one and changes between joins of one running process. Rust names the key the join token was signed with, on
   every join. The **derivation** is identical: each of the 24 passwords reproduces from the secret belonging to the key its
   own username names, and neither side ever signs with a key that is not configured. The lane records this in
   `divergences` (`turn_embedded_key_selection`) and promotes only the derivation half of `YAML:keys`; the selection rule
   gets its own `OPEN_AUDIT` row `CONFIG_BEHAVIOR:Turn.EmbeddedCredentialKeySelection`.
2. **Clamp visibility.** Rust prints `turn.ttl_seconds out of range, using safe value …` and `turn_servers ttl out of range,
   using safe value …`; Go prints neither, because `NormalizeTURNTTLs` runs inside `config.NewConfig`
   (`cmd/server/main.go:204`) *before* `InitLoggerFromConfig` (`:208`). Three `log_all_rust` presences and three
   `log_not_go` absences pin the asymmetry in the measured direction. The rows still promote, because the credential
   lifetime they name is what the wire carries.

## Four claims this lane made and then measured false

1. *"An embedded TURN password is 43 base62 characters."* It is 43 or 44 — compact base62 drops the leading zero bits, so
   the width follows the digest. Pinning one number would have redened a real parity, so the embedded length legs are bands
   (the external HMAC rows stay exact at 28).
2. *"The embedded username names the key that signed the token."* True on Rust, false on Go — see divergence 1. The claim was
   written into `emb-two-keys` before the repeat joins measured it.
3. *"Go logs the ttl clamp, we just did not grep for it."* It does not: the clamp runs before the logger is attached. That
   turned a suspected harness bug into the `log_not_go` legs above.
4. *"The advertised `turns://` URL carries the port the listener owns."* It carries the constant 443, while the process holds
   `<TLS1>`. Flattening the two would erase the quirk, so the row claims the URL **and** the socket separately.

## Falsifiability

58 named receipt mutations plus one meta probe, `all_discriminated: true` against the filed receipt
`ca77919048950389dd3ebbc30d6576068f6fd77fa2686b961225c6605c215fa7`, with `receipt_sha256_before == after` proving the sweep
ran read-only. Expectations: 30 `FAILED_LEG`, 26 `FAILED_CONTROL`, 2 `HARNESS_REFUSAL`. Six cases failed their own audit and
were fixed rather than dropped: `foreign_secret_never_matched` was vacuously true because it keyed on the wrong prefix; a
duplicate `joins` field did not survive the JSON round-trip, so the repeats ride as `extra_joins`; `covers_row_left_unclaimed`
mutated a field the profiles no longer read; the `pair_leg_with_a_missing_side` case popped a key that the legs now `.get()`,
so it sets it to `null` instead; and two cases were retargeted onto the family-specific socket legs that replaced the
substring port scan (`tls_listener_not_held` → `tcp-TLS1`, `omitted_udp_port_bound_anyway` → `not-udp-TU1`). The 1518-leg,
80-control baseline the sweep grades against is re-derived purely from the receipt: `--report` recomputes every leg and
control from the recorded cells, and `verdict_stable_across_rederivation` ties the two runs together.

## Privacy

Role tokens (`<HTTP> <STORE> <TU1> <TU2> <TLS1>`), participant SIDs as `<PA>`, the two literal fixture values as `<USR>` /
`<CRD>`, the five signing secrets as `<SEC-K> <SEC-M> <SEC-X> <SEC-F> <SEC-G>`, paths as `<WORK>`, timestamps as `<TS>`,
addresses letter-keyed over the committed salt `c105-creds`. Credentials and usernames never appear as text — only as
sha256 prefixes and a length — and the non-UTF-8 secret file is stored as its digest
(`binary_secret_file_bytes_never_copied_in`). `no_machine_address_in_receipt`, `no_allocated_port_leaks_into_urls`,
`no_unmasked_loopback_ports`, `participant_sids_are_tokenised`, `fixture_secrets_never_raw`, `argv_never_raw`,
`off_limits_never_named`, `host_store_never_named` and `no_working_paths_in_profile_records` sweep the whole receipt JSON.
`7881`/`7882` counted 0→0 and never named; the host store `6379` counted 2→2 and never contacted. Everything is loopback:
no route, interface, proxy or alias is touched.

## Honest residual

* No TURN **allocation** is driven: this lane reads the credential a join is handed, not whether a relay allocation is
  created against it. That is the C23/`turn-listener-relay` half, and `ice_servers` delivery to a real peer over the
  original SDK is a separate SDK gate.
* `rtc.turn_servers[].tls_port`-style per-element TLS listeners are not bound by this lane: the external rows point at
  literal ports (`443`, `3478`, `3479`, `5349`, `7887`) that nothing in the lane owns, which is what upstream does too.
* `turn-static_auth_address`, `turn-secret`, `turn-secret_file` (the top-level embedded spellings), `turn.static_auth_addresses`,
  `keys_pem`, `keys_yaml` and the base flags `--key-file`/`--cert-file` remain separate rows.
* The embedded key-**selection** rule stays open as its own row (divergence 1); the `LIVEKIT_KEYS` env channel is C104's.
* `rtc.turn_servers[].ttl` is promoted for the values measured here; a ttl that would clamp into a band the lane does not
  exercise (e.g. exactly 86400 minus one second) is not claimed.

## Reproduce

The oracle image is built from the pinned checkout and reproduces byte for byte
(`4bc2378e636740b29093061036249ffc9ebbef91f4ece1de12177c8dfee1e7dc`):

```
cd references/livekit-stable && GOTOOLCHAIN=go1.26.0 GOPROXY=off GOWORK=off \
  go build -mod=readonly -o /tmp/c99-build/livekit-server-go ./cmd/server
python3 tests/interop/turn-credential-join/run-paired.py --record --out /tmp/c105-lane
python3 tests/interop/turn-credential-join/run-paired.py --report /tmp/c105-lane
python3 tests/interop/turn-credential-join/run-paired.py --selftest-gate /tmp/c105-lane
```

`--report` and `--selftest-gate` need no binaries at all and were re-run against the *filed* receipt in
`livekit-docs/server/evidence/2026-11-27/spec0077-c105-turn-credential-join/`, giving the same
`PASS_LAUNCHED_TURN_CREDENTIAL_JOIN` verdict, 59/59 discriminated cases and an unchanged receipt digest.

The harness refuses to record unless both binaries hash to the pinned images. It imports the C102 launcher/allocator/socket
observer (`fff0615a…`), the C103 certificate generator and JWT builder (`93abcd…`) and the C104 masker (`a4b5ea…`) rather than
copying them, so all three earlier lane files stay byte-identical and their filed receipts stay re-derivable;
`imported_primitives_are_the_accepted_lanes` re-hashes them at the end of the run.
