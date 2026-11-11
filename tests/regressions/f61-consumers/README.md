# F61 actual consumer counterexamples

The three named consumer tests in cli_os.rs reproduce real loader/file/Run-stage failures, not new helper behavior. Original executed RED evidence is preserved. In an isolated checkout whose three file hashes match SOURCE.json, reverse production-fix.patch from the repository root, leaving all current tests intact; run `cargo test --release -p lk-config --lib os_input::tests`. The registered raw ENV, actual HOME/backslash file consumer and one-shot HTTPS consumption tests must fail against pre-fix production. A compiler failure or zero-test filter is not behavioral RED. Restore the same production patch before GREEN/checks. Never reverse in a shared checkout or overwrite unrelated changes.

This fixture is a historical regression input, not an alternate production implementation. It excludes changed/new test hunks and retains their real assertions. It does not reclassify original37 Go nil-map panic differences as PASS.
