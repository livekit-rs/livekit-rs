# Actual POSIX HOME and raw ENV path consumers

Run compare.py with --official, --rust, --profile darwin|linux, --go-oracle (the actual fresh-process posix-home oracle capture), and a new --output path. These nine actual CLI pairs retain5 HOME/fallback rows and4 raw-name rows, original8s process bounds and strict output/JWT/keyfile checks. Actual Linux execution is required separately; Darwin cannot certify Linux.

The two raw-name positives use a raw-target-only filename. The working directory contains the HOME test keyfile under a different filename, so missing variable expansion cannot accidentally read that same keyfile and pass. The original F83 fixture used the same filename and credentials in both places: two apparent baseline positives were insufficient and remain historical, not support evidence. This strengthened fixture changes no required product behavior or threshold. Missing-key diagnostics and ASCII flag non-aliasing remain exact.

Raw reports include generated JWTs and are local-only. Persist safe case statuses, input/binary hashes and signature-validation results before cleanup. The runner removes only its own temporary directory and checks cleanup. No browser, all-CLI or full platform-family compatibility follows from these finite observations.
