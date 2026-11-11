# Fixed official CLI source oracle

Pinned source/transformations are recorded in SOURCE.json. `main.go` copies the actual fixed commands.go 14-column renderer body (writer alone replaced with bytes.Buffer) and delegates secret encoding to the actual locked jxskiss/base62 module. It reads JSONL operations from cases.jsonl, writes named JSONL results. From this directory the coordinator runs `GOMAXPROCS=2 go run . < cases.jsonl > /tmp/livekit-rs-f05-cli-oracle.jsonl`, archives actual result/hash, then runs Rust fixture comparison. No compiler or executable was run by the implementing agent. `go.mod`/sum reuse the fixed official graph with a local pinned protocol replacement; no references are edited.

The source encoder has MIT copyright Shawn Wang (2019), humanize helpers MIT copyright Dustin Sallings (2005–2008), and copied server rendering logic Apache-2.0 LiveKit. Complete source licenses accompany inputs under references/go-cli; do not remove them. These pure inputs are not task temp files.

Real Redis CLI tests must separately launch the unmodified official executable and candidate against separately owned brokers with real protobuf HVALS records. Source renderer vectors do not prove command dispatch, authentication, selected DB, no mutations, malformed storage, error/exit behavior or runtime TLS/HA. Those remain pending until executed. No random IDs, timestamps or unknown error text should be globally normalized away.

## Real broker/binary corpus

Compile/run only under coordinator ownership. First obtain real wire fixtures with `GOMAXPROCS=2 go run ./seed-nodes < cases.jsonl > /tmp/livekit-rs-f05-cli-node-wire.jsonl` (actual Go protobuf encoder). Prestart two dedicated empty loopback Redis brokers, preferably authenticated. Run:

```
CLI_REFERENCE_REDIS_PASSWORD=... CLI_RUST_REDIS_PASSWORD=... \
python3 run-binaries.py --official /absolute/fixed-go --rust /absolute/livecloud \
 --reference-redis-port 16379 --rust-redis-port 16380 --db 3 \
 --unreachable-port 16381 --dedicated-brokers \
 --wire-fixtures /tmp/livekit-rs-f05-cli-node-wire.jsonl \
 --output /tmp/livekit-rs-f05-cli-real-results.json
```

`CLI_REFERENCE_REDIS_USERNAME` / `CLI_RUST_REDIS_USERNAME` support actual ACL users. Credentials are put only in temporary mode0600 configs and Redis CLI's authentication environment, not evidence/command arguments. The script starts no broker/compiler/server. It refuses pre-existing nodes keys in selected/alternate DB, changes only nodes in dedicated selected DB, deletes that owned key in finally and removes temporary configs. It preserves all failed comparisons and never treats skip/incomplete as PASS. Cases include empty, zero stats, >2^53 totals, selected/other DB, two nodes, nil stats, malformed protobuf, invalid known UTF8, WRONGTYPE, unreachable and invalid credentials. Invalid credentials remains NOT_RUN/incomplete unless both brokers require auth. Nil stats intentionally retains actual panic/exit differences: a safe Rust error is not automatically equivalent.

Exact stdout and exits are compared. Only complete body-row order may differ because HVALS does not promise order across independently seeded brokers; all cells, borders, widths and header remain exact, and both raw outputs stay in evidence. Diagnostic stderr is retained but timestamp/stack-frame equality is not claimed. Every invocation compares actual raw Redis DUMP bytes before/after in each dedicated broker; successful seeded cases additionally check HLEN. This proves this key remained unchanged in these sequential fixtures, not all routing side effects in arbitrary concurrent deployments.

`compare-ports.py` runs the additional11 real-process ports inputs from ports-cases.json; all original official_cli fixtures remain mandatory. This new corpus preserves stderr exactly, so timestamped logger differences remain actual reported differences until explicitly scoped (never silent global dropping). Nothing here proves full Redis routing/HA, every CLI flag, or whole-project compatibility.

## Actual generated credential authentication probe

The coordinator runs (this implementation has not executed it):

```sh
OFFICIAL_BIN=/absolute/fixed-livekit-server RUST_BIN=/absolute/livecloud \
CREDENTIAL_EVIDENCE=/tmp/credential-functional-result.json \
node generate-credentials.mjs
```

Optional `OFFICIAL_CREDENTIAL_PORT`, `RUST_CREDENTIAL_PORT` and their `_UDP_PORT`
counterparts select coordinator-reserved ports. Otherwise the script obtains
free loopback ports and then requires actual authenticated readiness; a bind race
is a failure, not a fabricated ready state. It starts only its own two isolated
servers sequentially, with no Redis or browser. Inherited `LIVEKIT_*` overrides
are removed from child environments so private generated-key configs govern.

Each actual binary executes `generate-keys` three times. The script checks exact
two-line output and spaces, the 15-character API key (API plus12 base57 chars),
43–52 alphanumeric characters from base62 encoding32 random bytes, and all six
keys/secrets distinct. This detects fixed output; it is not a cryptographic RNG
quality test. Every generated pair is then configured in both actual binaries:
the unchanged ServerSDK2.19.2 `AccessToken` signs an explicit admin grant, and
`RoomServiceClient` uses that token for actual create/list/delete and verifies
absence after deletion. A signature using the same API key but an altered secret
must receive authentication rejection. Thus a full pass contains12 real CRUD
cycles and12 incorrect-secret rejections, with no synthetic endpoint responses.

Credentials exist only in memory and mode0600 configs under a private temporary
directory. Child stdout/stderr is never copied into evidence. Reports contain
binary/script hashes, counts, booleans and actual child exit status, not API keys,
secrets, JWTs, room IDs, endpoints or logs. Failures persist a static stage label.
All owned rooms/children are cleaned in finally; private configs are removed only
after shutdown. Forced termination or incomplete cleanup makes the result FAIL;
there is no `process.exit()` success shortcut. Actual runtime evidence remains
NOT_RUN until the coordinator executes the complete script.

## Actual F05 execution

Coordinator executed the pinned Go oracle11 rows and explicit Rust comparisons, real authenticated Redis/process12 cases and ports11 exact-stream cases, and real generated credentials on both isolated servers. The final delivery records source and binary identities plus retained initial failures; scopes above remain bounded. Implementing-agent NOT_RUN comments describe authorship history, not the coordinator outcome. See `livekit-rs-docs/evidence/2026-10-25/f05-functional-delivery.json` and `validation/f05-oracles`.
