# yaml.v3 event-parser differential (lk-config `yamlv3::parser`)

`crates/lk-config/src/yamlv3/parser.rs` is a port of gopkg.in/yaml.v3 v3.0.1
`readerc.go`/`scannerc.go`/`parserc.go`. The lk-config unit test
`yamlv3::parser_differential_tests::differential_against_go_yaml_v3` replays
`corpus.txt` (719 inputs, 364 of them errors) and must reproduce `go_dump.txt`
exactly. That covers every event's type, anchor, resolved tag, value, style,
implicit flags, start/end marks and head/line/foot comments; for errors, the
kind, problem, context, both marks, offset and value plus the `(*parser).fail()`
text (cross-checked against `yaml.NewDecoder(...).Decode` in 342 error cases).
Event lines beyond 200 per case are folded into a hash.

Regenerate the Go dump (go1.26.3):

```sh
D=$(pwd); G=$(mktemp -d)
cp -R ~/go/pkg/mod/gopkg.in/yaml.v3@v3.0.1/. $G/ && chmod -R u+w $G
cp go/dump_test.go go/go.mod $G/
(cd $G && CORPUS=$D/corpus.txt DUMP=$D/go_dump.txt GOTOOLCHAIN=go1.26.3 GOFLAGS=-mod=mod GOPROXY=off \
   go test -run TestDumpEvents -count=1 .)
rm -rf "$G"
```

`gen_corpus.py` builds the corpus. `gen_fuzz*.py` produced the randomized
inputs, about 182,000 in total; those inputs were not committed, and all of
them matched. `YAMLV3_CORPUS`/`YAMLV3_DUMP` point the Rust test at other files.
Known modelled-but-unexercised branch: Go reallocating, not compacting, the
token queue inside `yaml_parser_parse_flow_sequence_entry_mapping_value` (the
in-place-compaction mark quirk itself is covered by 20 cases). The growth rules
are Go >= 1.22.
