# Actual Go quoting oracle

Run `GOTOOLCHAIN=go1.26.3 go run . cases.json > /tmp/owned-cli-quote.jsonl`
in this directory. Then from the server workspace run the opt-in
`lk-config actual_go_quote_vectors` release test with `CLI_QUOTE_GO_CASES`
and `CLI_QUOTE_GO_RESULTS` set to absolute paths and `-- --ignored`.
This independently executes strconv.Quote on 24 edge cases and every valid
Unicode scalar. The scalar digest includes code point, quoted byte length and
exact output bytes. It covers UTF-8 strings; arbitrary OS bytes need separate
process tests. Go source attribution and hashes are recorded in the F51 evidence
and production cli_quote modules. No Go runtime is linked into the Rust server.
