# Fixed Go POSIX path oracle

This test-only program executes unmodified go-homedir v1.1.0 and Go os.ExpandEnv/filepath behavior for the eight public byte vectors. Reference clone: `references/deps/mitchellh--go-homedir`, commit af06845cf3004701891bf4fdb884bfe4920b3727. No production Go dependency is introduced.

Run with the fixed Go toolchain and readonly modules, redirecting cases.json to stdin. Set F61_HOME_CASES to the absolute cases.json path and F61_HOME_GO_RESULTS to actual JSONL output when running the ignored lk-config test actual_go_homedir_posix_vectors. The test compares all eight executed rows; merely generating Go output is not acceptance.

The byte cases contain synthetic public paths only. See LICENSES/go-homedir-MIT.txt for upstream attribution.
