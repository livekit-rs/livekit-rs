# Actual pinned `encoding/json` oracle for `workers::egress` layout metadata

> The absolute paths below are the recorded batch 22e run on the measuring host. Substitute your own
> checkout for `/Users/zoujiaqing/projects/livekit-rs` and any writable directory for the
> `/private/tmp/...` staging paths; every driver takes them as flags.

Consumer: `crates/lk-compat/src/workers/egress.rs:543`
(`layout_metadata_matches_actual_go_marshal_rows`, `#[ignore]`), which reads
`WORKERS_LAYOUT_GO_RESULTS` as a JSON **array** of exactly 7 rows
`{name, layout, metadata}` and compares `metadata` byte-for-byte against the
Rust `layout_metadata(layout)` (`egress.rs:508-523`).

## What is actually executed

`main.go` imports the real upstream `github.com/livekit/livekit-server/pkg/service`
and marshals `&service.LayoutMetadata{Layout: input}` — the exact expression at
`references/livekit-stable/pkg/service/egress.go:301`, using the exact upstream
struct at `egress.go:286-288`. The struct is **not** copied into the oracle, so
the JSON tag and Go's default `EscapeHTML` behaviour come from the pinned source.
No expected metadata string is hand-authored.

## Corpus provenance — newly authored, and labelled as such

Upstream has no committed 7-layout corpus: `UpdateLayout` takes `req.Layout` from
the caller, so there is no fixed input list to extract. `cases.json` is therefore
**newly authored for this oracle**, not upstream data. It is committed so the
comparison is reproducible, and it is chosen to cover every escape branch the
Rust port implements plus the branches it does not:

| name | layout | branch exercised |
| --- | --- | --- |
| `empty` | `""` | empty string, no escaping |
| `plain-ascii` | `grid` | printable ASCII passthrough |
| `html-trio` | `<>&` | the three `EscapeHTML` replacements |
| `javascript-separators` | U+2028 U+2029 | the two JS line separators |
| `quote-backslash-newline` | `"\\` + LF | quote, backslash, `\n` short form |
| `c0-controls` | NUL SOH BS TAB VT FF CR US | remaining C0 controls, both the short-form and the `\u00xx` branch |
| `unicode-non-ascii` | `日本語🎥e` + U+0301 | multi-byte UTF-8, astral plane, combining mark |

The `c0-controls` row was added to probe a suspected divergence: it was assumed
Go only had short forms for `\n`, `\r` and `\t` and would write `\u0008` /
`\u000c` where `serde_json` writes `\b` / `\f`. **That assumption was measured
and is false.** The pinned toolchain's
`src/encoding/json/encode.go:1009-1029` has explicit `case '\b'` and `case '\f'`
branches, and its own comment reads "This encodes bytes < 0x20 except for \b, \f,
\n, \r and \t". The real Go output for this row is
`{"layout":"\u0000\u0001\b\t\u000b\f\r\u001f"}` and the Rust port matches it
byte-for-byte. The row is kept because it pins that agreement; no defect was
found here.

## Run (coordinator only)

```sh
/usr/bin/python3 tests/oracles/workers-layout/prepare.py \
  --repository /Users/zoujiaqing/projects/livekit-rs --out /private/tmp/b22e-workers-layout
cd /private/tmp/b22e-workers-layout
GOTOOLCHAIN=local GOPROXY=off GOWORK=off \
  "$HOME/go/pkg/mod/golang.org/toolchain@v0.0.1-go1.26.3.darwin-arm64/bin/go" \
  build -mod=readonly -o workers-layout-oracle .
./workers-layout-oracle cases.json > /private/tmp/b22e-workers-layout/results.json
```

Then run the real Cargo ignored test with
`WORKERS_LAYOUT_GO_RESULTS=/private/tmp/b22e-workers-layout/results.json`.

## Scope

Exactly `json.Marshal` of the upstream `LayoutMetadata` for 7 inputs. This does
**not** execute `EgressService.UpdateLayout`, `EnsureRecordPermission`,
`io.GetEgress`, `roomService.UpdateParticipant`, grant mutation, or any SDK
delivery. It does not prove the runtime branch is reached in a live server, and
it says nothing about egress layout length limits (covered separately by
`tests/interop/egress-layout-limits`). A module-resolution error is not a test
verdict: preserve the original exit code and inspect the pinned paths rather
than updating versions.