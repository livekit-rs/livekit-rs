# SPEC-0077 C104 — the TURN material, ports and key map, judged on the listeners they build

**Batch:** C104-20261127-turn-material-channels (evidence only — **no production code change**)
**Leafs:** 26 — twelve `YAML:turn.*` / `YAML:keys`, twelve `generated_flags:--turn.*`, one
`CONFIG_BEHAVIOR:Turn.DisabledSkipsInactiveRuntimeConsumers`
**Tier:** 5 — both binaries launched on identical YAML bytes on allocated loopback ports; the effect is read back from
the socket table, a real TLS handshake, a real STUN Binding, the running node's auth answers and the registry, never
from a config struct.
**Verdict:** `PASS_LAUNCHED_TURN_MATERIAL_CHANNELS` — 34/34 profiles match cross-side, 1078 legs, 65 controls,
against Go `4bc2378e…` (source `e5655ac…`) and Rust `f6fe7f03…`.

C103 ended by naming the TURN secret channels as residue, and C23/C98 had left these rows at
`IMPLEMENTED_UNVERIFIED`/`OPEN_AUDIT` with a Rust unit test plus a source reading behind them. `turn-main`,
`turn-listener-relay` and `turn-tls-proxy` measured other halves of the same feature. This lane is the part none of
them did: the certificate and key the TLS port actually presents, the port the UDP listener actually owns, the
gate that refuses a bad domain or CIDR before anything binds, and the key set the running node signs with.

## Mechanism read from the oracle, then measured

| fact | upstream | measured here |
| --- | --- | --- |
| `NewTurnServer` returns `nil, nil` before any consumer when `turn.enabled` is false | `pkg/service/turn.go:65-80` | `turn-disabled-skips-consumers`: the config still names TLS1/TU1 and carries a bad domain and a bad CIDR, and neither port appears in either socket family |
| the TURN server logs the range, the TLS port, the UDP port and the two booleans it was built with | `pkg/service/turn.go:215` | the same keys carry the configured numbers in `turn-relay-*`; the TLS key is **absent** from the Go line when the port is 0, while Rust logs `"tls_port": "None"` |
| a TLS listener is built only when `!ExternalTLS`, from `cert_file`/`key_file` | `pkg/service/turn.go newTURNTCPListener` | `handshake_leaves_der_on_the_wire`: a real 766-byte TLS 1.3 session whose leaf digest equals the fixture DER; with `external_tls: true` the handshake does not complete and the port answers a plaintext Binding instead |
| the domain is required and must be a valid domain, or the start is refused | `turn.go:73-80`, `pkg/service/utils.go:156-159` | `TURN domain required` / `TURN domain is not correct`, byte-identical on both sides, exit 1, nothing bound afterwards |
| the peer CIDR lists are parsed once at startup so a malformed entry fails loudly | `turn.go:84-95` | `invalid CIDR "not-a-cidr" in turn.deny_peer_cidrs: …` identical on both sides, and `turn-cidr-lists-valid` proves the same code path accepts documentation prefixes |
| a generated flag name **is** the YAML path and its env name is `LIVEKIT_` + that path | `pkg/config/config.go:940-1035` | `--turn.tls_port` / `LIVEKIT_TURN_TLS_PORT` move the live listener, and the port the YAML named instead is bound in neither family |
| `LIVEKIT_KEYS` is a live source for the same field the YAML block writes, and it **replaces** the map | `cmd/server/main.go:63` (`Sources: cli.EnvVars("LIVEKIT_KEYS")`), `config.go:1135-1158` | `turn-keys-env-replaces-yaml`: the env kid answers `handler`, the YAML kid drops to `bad-key` |
| `unmarshalKeys` keeps only string-valued entries | `config.go:1145-1157` | `turn-keys-env-numeric-secret`: `k104n: 12345` is refused, so the number was dropped rather than stringified |
| `turn.domain` is **not** the STUN realm; the realm is the constant `"livekit"` | `turn.go:44,95`, `crates/livekit-rs turn.rs:127` | the Binding Answer carries `0x8028` (SOFTWARE) and `0x20` (XOR-MAPPED-ADDRESS); no realm attribute appears, and the domain's real consumer is the join-response TURN URI |

## What counts as an effect here

* **Ports** — the TCP listener and the UDP endpoint the *process* owns, read from `lsof -a -p PID -i` while it is
  alive and again after exit (184 `release-*` legs). A losing port is claimed absent in **both** families
  (`losing_ports_never_bound`), which is how `--turn.tls_port 0` over a YAML port becomes a measurement rather than a
  log reading.
* **cert/key** — the leaf the TLS port hands a client, hashed over its DER and compared to the two fixture
  certificates. `turn-yaml-ports-and-leaf` presents A, `turn-yaml-material-b` presents B, and
  `turn-flag-material-beats-yaml` puts A on argv over a valid B in the YAML; the digest is what decides which file was
  opened. No log line about loading a certificate is used as a claim.
* **external_tls** — the same plaintext STUN Binding sent to the same TCP port. Under an internal TLS listener Go goes
  silent and Rust answers a 7-byte TLS alert (`alert=0232`, fatal handshake_failure); with `external_tls: true` both
  answer a Binding Success Response (Go 40 bytes, Rust 64). That split is the row's behaviour, so
  `plaintext_stun_over_tls_split_pinned` and `external_tls_answers_stun_over_tcp` read the recorded probe, not the flag.
* **domain / cidr / ports (gate)** — the refusal sentence, the raw exit, whether the daemon self-exited, that nothing
  is bound afterwards and that no node row was written. Sentences are compared with `sentence_after()`, line for line.
* **keys** — which secret the running node accepts on `POST /rtc/v1/validate`: `400 join_request is required`
  (`handler`) means the grant parsed under that key, `401 invalid API key` (`bad-key`) means the node does not hold
  the kid. 90 auth legs over the 23 rows that carry an `auth` claim, both cells recorded per row.
* **registry** — `node_ids` sampled **while the daemon lives** (68 reads, every started row claiming exactly one) and
  re-read after exit (all back to `[]`, plus 22 `node-gone` reads on the refusal rows), which is what keeps
  `version_marker_survives_clean_shutdown` from being a vacuous "empty equals empty".

## Profiles (34)

Listeners and material (7): `turn-yaml-ports-and-leaf`, `turn-yaml-material-b`, `turn-flag-ports-beat-yaml`,
`turn-env-ports-beat-yaml`, `turn-flag-zero-port-beats-yaml`, `turn-udp-only-no-tls-listener`,
`turn-flag-material-beats-yaml`.
external_tls (3): `turn-external-tls-yaml`, `turn-external-tls-flag`, `turn-external-tls-with-cert`.
Relay numbers (3), keys (3), enabled/flag precedence (5), TTL clamp (2), valid CIDR lists (1).
Refusals (11): both ports 0, crossed flag pair, missing files, domain missing, domain malformed, empty flag domain over
a valid YAML one, deny list, allow list, the enabled twin of the garbage block, an out-of-range port, and the proxy
flag without trusted CIDRs.

## Two divergences pinned instead of averaged

1. **`ttl_seconds` clamp visibility.** Rust logs `turn.ttl_seconds out of range, using safe value (configured 999999,
   clamped 86400, max 86400)`; Go clamps before its logger is attached and prints nothing. Both still start and both
   still answer a Binding, so the rows stay at component behaviour and `ttl_warning_visibility_pinned` asserts the
   asymmetry in the measured direction instead of claiming parity.
2. **Which gate speaks first.** Given one TURN block that is wrong in several ways, Go's first line is
   `TURN domain required` and Rust's is `turn.proxy_protocol requires at least one entry in
   turn.proxy_protocol_trusted_cidrs`. Both exit 1 and both bind nothing. `garbage_gate_first_sentence_pinned` names
   the two sentences, so aligning the order later reddens the control rather than passing unnoticed.

## Four claims this lane made and then measured false

1. *"`LIVEKIT_KEYS` is inert on the launched path."* It is a declared env source for the same field and it replaces
   the YAML map wholesale — the first profile design treated it as a no-op.
2. *"The clamp warning is missing because the clamp is not reached."* The clamp runs, on both sides; the oracle simply
   has no logger yet. The row is a visibility divergence, not a behaviour divergence.
3. *"`turn.domain` feeds the STUN realm."* Both hardcode the realm; the domain's consumer is the `turns:<domain>:443`
   URI in the join response, which needs an SDK join to observe and is therefore **not** claimed here.
4. *"`--turn.enabled false` switches TURN off."* It does not, on either binary: a bool flag does not consume the
   following word, so `false` becomes a positional argument and both listeners stay bound. The equals form does switch
   it off, in both directions (`turn-flag-disabled-equals-skips-consumers`,
   `turn-flag-enabled-true-beats-yaml-false`). An earlier draft read that refusal as "the domain gate runs before the
   enabled check", which the socket table disproves.

## Falsifiability

85 named receipt mutations plus one meta probe, `all_discriminated: true` against the filed receipt
(`receipt_sha256_before == after`, so the audit provably ran read-only). Expectations: 61 `FAILED_CONTROL`, 22
`FAILED_LEG`, 2 `HARNESS_REFUSAL`. Each case is anchored on a leg or control that exists in the unmutated baseline —
the rule that caught `daemon_ready_in_a_refusal`, which targeted a lifecycle control and therefore could not redden
when a readiness line was appended to a refusal: the append breaks the row's `log_not` leg, not that control, and the
case was retargeted rather than dropped.

## Privacy

Role tokens (`<HTTP> <STORE> <TLS1> <TLS2> <TU1> <TU2> <PU> <PT>`), letter-keyed addresses over the committed salt
`c104-turn`, `<WORK>` for every path, and the two fixture signing secrets only as `<SEC-Y>` / `<SEC-E>`. Unallocated
loopback ports are masked to `127.0.0.1:<EPHEM>`. `no_machine_address_in_receipt`, `no_unmasked_loopback_ports`,
`fixture_secrets_never_raw`, `argv_never_raw`, `off_limits_never_named` and `host_store_never_named` all sweep the
whole receipt JSON. `7881`/`7882` were 0 listeners before and after; the host store on `6379` was 2 before and 2 after
and is never named.

## Honest residual

* No TURN **allocation** is driven: the lane proves the listeners exist and answer a Binding, not that a relay
  allocation is created, permissioned or timed out. `peerip`, `realm`-side behaviour and the transport restrictions are
  out of scope here.
* `ttl_seconds` has no launched channel: neither binary exposes `GetTURNCredentials`, so credential expiry cannot be
  observed from a running node. Those two rows keep `main_server_consumer: NOT_RUN`.
* `turn-static_auth_address`, `turn-secret`, `turn-secret_file`, `turn.static_auth_addresses`, `keys_pem`,
  `keys_yaml` and the TLS-material `--key-file`/`--cert-file` base flags are separate rows, not covered here.
* `YAML:turn` as a group stays `OPEN_AUDIT`: the group row needs the secret channels above before it can be promoted.
* The SDK join response (`ice_servers`/`turns:<domain>:443?transport=tcp`) is the real consumer of `turn.domain` and is
  not observed by this lane.
* The livekit-platform/NAT scenario is not exercised; this is the embedded TURN server in Standalone mode.

## Reproduce

```
python3 tests/interop/turn-material-channels/run-paired.py --record --out /tmp/c104-lane
python3 tests/interop/turn-material-channels/run-paired.py --report /tmp/c104-lane/execution.json --out /tmp/c104-lane
python3 tests/interop/turn-material-channels/run-paired.py --selftest-gate /tmp/c104-lane/execution.json --out /tmp/c104-lane
```

The harness refuses to record unless both binaries hash to the pinned images. It imports the C102 launcher and the
C103 probes instead of copying them, so those two lane files stay byte-identical and their filed receipts stay
re-derivable.
