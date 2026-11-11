# F69v2 missing-HOME / raw ENV-name consumers — isolated NOT_RUN

Both new TODO leaves remain OPEN. No shared source, compiler, Go, server, browser,
git or cleanup ran here. Only source reading, hashing, isolated rustfmt and Python
AST syntax checks. This is independent of F60/F61 HOME-valued positive profiles;
those successes cannot certify missing HOME or raw environment **names**.

## Fixed source facts, not comment-based implementation

Pinned go-homedir1.1.0 homedir.go89–150:

* POSIX first uses nonempty HOME. USERPROFILE is Windows-only(dirWindows).
* Darwin: real sh -c `dscl -q . -read /Users/"$(whoami)" NFSHomeDirectory | sed ...`;
  successful nonempty trimmed stdout is the result. Failure or empty output then
  tries real sh -c `cd && pwd`. Pipeline success is shell status, not dscl status.
* Other Unix/Linux: real getent passwd actualUID; successful stdout parsed with
  SplitN(...,7), when length>5 returns field5 even if empty. Empty/malformed stdout
  may proceed to shell. Other command errors are returned.
* Important actual oracle case: the source compares err directly with
  exec.ErrNotFound, not errors.Is. Go exec.Command carries LookPath's *exec.Error,
  so the comment 'ignore ErrNotFound' may NOT mean missing getent reaches shell.
  Actual PATH-empty and PATH-only-real-sh rows decide this before production port.
* Shell failure returns actual typed process error; only successful empty stdout
  returns literal 'blank output when reading home directory'. Child stderr is
  discarded by the unmodified library, not forwarded as invented public output.
* Dir caches only successful nonempty result process-wide. The public CLI expands
  its keyfile once; oracle runs each case as a fresh process, so no fake cache reset
  or process-global fallback result crosses environments.

Current Rust incorrectly tries USERPROFILE on Unix and otherwise immediately
returns blank-output error, bypassing real OS lookup/failure ordering.

Raw name branch is separate: Go os.ExpandEnv→Getenv accepts raw-byte string names
inside ${...}; current Env::from_os_pairs discards those names and expand_path
only performs UTF8 name lookup. This is distinct from declaring an ASCII flag ENV:
LIVEKIT_PORT+ff must stay unknown to flags, but an explicit ${LIVEKIT_PORT+ff} path
reference should expand the real supplied variable.

## Actual oracle then OLD binary RED (root owns execution)

`oracle/main.go` imports the unmodified pinned homedir library and Go os.ExpandEnv.
It emits actual result bytes/error/type; no expected outputs are authored. Build
once using fixed Go (root only). Then fresh-process capture:

    python3 livekit-rs-server/tests/oracles/posix-home/oracle/capture.py \
      --go /actual/compiled-f69-oracle --profile darwin --output /private/f69-go.json

Use Linux independently. Ten cases cover actual platform HOME fallback, empty vs
absent HOME, USERPROFILE decoy, empty PATH, PATH containing only a symlink to real
/bin/sh, and raw byte variable names. It does not fake dscl/getent replies or mock
a home directory backend. All child processes execute real OS programs, and every
case begins with a private cleared environment. Captured function errors are
observations, never marked successful feature support. A wrapper/process failure
is not a Go function verdict. The fixture records binary/input hashes and rawhex.

Then actual CLI pairs (OLD Rust before patch):

    python3 livekit-rs-server/tests/oracles/posix-home/fixture/compare.py \
      --official /fixed/livekit-server --rust /actual/old/livecloud --profile darwin \
      --go-oracle /private/f69-go.json --output /private/f69-cli-red.json

Nine mandatory real cases. Three HOME-missing/empty positives use **actual Go
oracle HOME**, derive a relative ~/ route to an owned ASCII key file, and require
real signed JWT/issuer/identity/room/exact30-day lifetime. No file is written to a
user HOME, and no arbitrary HOME file is read: only the unique owned filename is
referenced. USERPROFILE is a decoy owned directory. Empty/only-sh PATH errors are
recorded and compared exactly, not guessed from comments. Raw-name positives use
owned prefix + ${rawname}/keys.yaml, so the old expansion cannot accidentally read
/keys.yaml or another external key. Both raw-name values and files are ASCII;
these tests are mandatory/runnable on Darwin too, no invalid filename creation.

The original F52/F61 byte helpers are retained as byte-identified byte_support.py.
All argv/env/stdout/stderr are preserved as hex/base64 in private output, including
owned JWTs: do not publish raw output or tokens. Only existing narrow VERSION
identity handling; no error/UTF8/path normalization. Actual JWT semantic comparison
has the exact existing owned key and source lifetime checks. Linux and Darwin are
separate real profiles, not cross-platform claims. Owned files/symlinks/processes
are removed by the fixture when coordinator executes; no reference input changes.

## Two narrow implementation seams, not one misleading 'fix'

`raw-name-candidate/` is a guarded two-file implementation draft plus a test-only
public consumer RED patch. Env gets a private OS-name sidecar and get_os_name.
Known ASCII snapshots/get/get_os remain unchanged; raw names never alias declared
ASCII flag sources. Explicit path expansion uses the actual OsStr name bytes.
One real parse_with_probe regression writes/loads only owned key files and checks
that LIVEKIT_PORT+ff still does not alter the actual default ASCII port consumer
without any CLI override masking a bad environment lookup. The public process
fixture includes that same default-port control. Existing
assertions are unchanged. This code is NOT_COMPILED/NOT_RUN, to apply only after
actual Go/public CLI RED and independent review.

Missing-HOME fallback implementation is intentionally not guessed ahead of the
actual source errors above. Proposed separate home resolver must use exact POSIX
command/lookup order with the captured Env (not silently getpwuid/std::env::home_dir
or USERPROFILE); preserve actual typed spawn/exit vs blank-output failures, stdout
byte trimming and filepath semantics. It must not invent timers, successful empty
answers or a broad 'not found' string fallback. See fallback-design.md. No runtime
fallback patch is presented as ready until these actual source observations land.

Root owns actual source/CLI oracles, red→green validation, all full gates,
spec/TODO promotion, source retention, commit/push and final cleanup. No feature
or parent family is closed by this static plan or finite fixture.

## v2 legitimate fixed-Go keyfile precondition

Root actual source/paired execution confirmed commands.go createToken may pass a
nil conf.Keys map to yaml.Decode, causing panic. Every keyfile token row in v2
therefore explicitly supplies global --config-body 'keys: {}'. This allocates an
empty map, adds no key/secret, and len0 still forces actual file reading. No fallback
credential can make a wrong expansion succeed. Original v1 remains unchanged; its
nine-case draft was NOT_RUN and is not retroactively marked PASS. Related F61
reference panic failures remain FAIL evidence, not parser-normalized away.

The equivalent real Rust consumer test includes the same empty-map premise. JWT
issuer/signature/source30-day/identity/grants checks are unchanged. No oracle home
lookup/input/error observations changed; no production algorithm changed in v2.

Current execution checkpoint: actual Darwin Go10 captured (f69-darwin-home-oracle-capture.json); no Rust or Linux comparison has passed in this source-preparation batch. Actual source is retained at references/deps/golang--go-1.26.3 and references/deps/mitchellh--go-homedir.
