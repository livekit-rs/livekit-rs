# Actual POSIX argv/ENV/path byte differential — currently RED

Run /usr/bin/python3 run.py --official /fixed/livekit-server --rust /candidate/livecloud --output /private/results.json. The default5s per-process bound remains unchanged. Both executables receive identical byte argv[0], raw argv/ENV and actual paths. Generated JWT claims and signature are validated separately; other output is byte-exact except the existing product/build identity normalization. No server or browser is launched.

All32 cases are mandatory on Linux, including real positive byte filenames and byte-HOME/path expansion. Darwin filesystem EILSEQ is explicitly NOT_RUN_PLATFORM; it never counts as support. Current F53 actual Darwin28 executed:5PASS/23FAIL,4filesystem exclusions. Preserve hashes/lengths/verdicts in safe evidence; raw reports include development JWTs and must stay private and be removed after archival. Fixture-owned files are cleaned internally. No runtime input implementation is certified by committing this fixture.


The expanded `run-expanded.py` keeps the original32 rows and adds five real key-file consumers: raw HOME parent cleanup, missing parent-expanded file, POSIX backslash filename, Unicode HOME parent cleanup and an independently named backslash-only file. All37 are mandatory on Linux. Darwin filesystem exclusions remain NOT_RUN_PLATFORM, never PASS. Use the same arguments as run.py; raw private output includes generated fixture tokens and must be sanitized before archival.


Actual fixed-Go nil-map counterexample is retained in the original profiles. `run-initialized.py` is a distinct37-case profile with empty `keys:{}` config for JWT cases only, not replacement of run.py/run-expanded.py. It supplies no credentials, still opens the actual key file and validates signatures, exact claims and source lifetime. All37 must execute on Linux. Reports never turn the original Go panic into PASS; follow SPEC0029 for the explicit remaining discrepancy.
