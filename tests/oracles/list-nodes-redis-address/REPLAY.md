# v2 fixture safety/qualification delta, NOT_RUN

Original v1 immutable; actual baseline12 RED receipts belong to v1 and are not relabeled. No original Go12/Go18 inputs, public old-API assertions, stderr/stdout/error or binary execution bounds change.

Only cfg(test) source-port reader changes in redis_address.rs: exact original ordered 18 ID/address pairs are embedded from original JSON, no expected outputs. Count then per-index identity/input check precedes real parser/result comparison. A normal inventory validator regression rejects duplicate/reorder/truncate/changed-address synthetic metadata; these metadata rows are NOT source algorithm outputs. Runtime source prefix is byte-identical to v1.

run.py optional consumer command now owns a new process group, uses --consumer-timeout-seconds (default900, positive root execution budget only), inherits its original output streams, and on normal/timeout/exception exits terminates/reaps only its owned group. SIGTERM5s then SIGKILL + bounded wait; actual receipt retains exit/timed_out/cleanup_complete/group state. Timeout/non-reaped group cannot PASS. Original actual CLI8s/observer120s/request business deadlines and all comparisons unchanged. Parent may set a finite larger build budget according to compile resource use; this is not a functional timing threshold. Original12 main cases may be rerun with v2, identified as safety-improved fixture not retroactively changed v1 result.

From baseline: same apply-narrow.py --phase production --apply installs v2 production module plus stricter cfg(test) reader; test-only lib/public test payloads identical. From already adopted exactv1: apply-reader-only.py --root OWNED_REPO (preflight), then --apply installs only cfg(test) delta, refusing any other helper version. Run original public consumer and real Go18 reader as parent commands; do not reapply old whole files over drift.

Execute run.py as v1 README but from this v2 directory; optional --consumer-timeout-seconds 900. Optional old-public consumer remains inside live owned Redis/input/FD lifetime. All actual CLI/source captures retain exact streams and comparison; no normalization.

Original run.py/Go code/source cases are hash preserved in v1. build-package.py is historical author construction input; do not execute it to overwrite a sealed package or regenerate against a changed workspace. Canonical SOURCE.json and supplemental patches bind final v2 bytes.


## Final v2 refusal/stdout scope correction

Parent actual v1 observer succeeded12; main case6 timed out8s/exit-9 and reader never ran. This is FAILED_SETUP, not RED. No120s observer failure claim. V2 uses genuine listen->close RST with repeated actual ECONNREFUSED+lsof controls including original Go case; no held-bound drop endpoint.

Parent explicitly split functional stdout+exit parity from nondeterministic Go retry stderr. All original stderr bytes, full exact_equal and separate stderr_exact_equal remain unmodified/un-normalized observations, possibly false; status PASS_FUNCTIONAL_SCOPED_STDERR_OPEN cannot certify whole CLI. Use new own outputs old-v2/new-v2, preserve old-v1 history. Original12 IDs/expected errors and all18 input bytes unchanged.

Root optional reader command MUST include --lib (driver enforces); original ignored public API test unchanged. Reuse root actual Go18 exact inventory/hash in ACTUAL-HISTORY.json; all newv2 Go12/Rust main/reader execution remains NOT_RUN.
