# Fixed SCTP source observer

Inputs and source archive identities are in oracle-source-lock.json and pion-source-inputs.json; actual rows are never prefilled. From the repository root:

```
python3 livekit-rs-server/tests/oracles/sctp-source-knobs/prepare_oracle.py --root "$PWD" --sctp-source "$PWD/references/go-modules/pion-sctp-v1.12.0" --out /tmp/livekit-rs-sctp-source-observer
cd /tmp/livekit-rs-sctp-source-observer
GOTOOLCHAIN=go1.26.3 GOMAXPROCS=2 go run -mod=readonly ./settings cases.jsonl > settings-actual.jsonl
GO_SCTP_KNOB_INPUTS=$PWD/cases.jsonl GO_SCTP_KNOB_OUTPUT=$PWD/association-actual.jsonl GOTOOLCHAIN=go1.26.3 GOMAXPROCS=2 go test -mod=readonly ./source -run '^TestSourceKnobObserver$' -count=1 -v
```

Both independent original-source observers emit10 rows. See ORACLE-README.md for exact finite scope and original isolated commands. This package is a development oracle, not a Go production dependency or proof of physical SCTP/official SDK media. Pion MIT license is preserved as LICENSE-UPSTREAM.

Supplemental source observers are retained byte-for-byte under `source-controls/`.
`supplemental-observer-retention.json` records their input hashes and the actual
coordinator evidence names. Frozen `NOT_RUN` metadata describes the original
isolated artifact; later execution receipts are separate, never rewritten into
that original metadata. Use a fresh base stage per observer, then run its
`oracle/prepare.py --staged-source STAGE --source-reference REFERENCE`.

| Observer | Original test | Actual output variable | Boundary |
| --- | --- | --- | --- |
| `current-stream` | `TestSourceCurrentStreamPolicyObserver` | `GO_SCTP_POLICY_OUTPUT` (inputs: `GO_SCTP_POLICY_INPUTS`) | Current real stream policy; eight cases |
| `physical-timer` | `TestSourcePhysicalUDPDefaultFastZeroTimer` | `GO_SCTP_PHYSICAL_OUTPUT` | Real UDP/T3; all initial DATA withheld until timeout |
| `first-data-loss` | `TestSourcePhysicalUDPFirstDataLoss` | `GO_SCTP_FIRST_LOSS_OUTPUT` | One first DATA datagram lost; no fabricated ACK |
| `first-data-loss-branch` | `TestSourcePhysicalUDPFirstDataLoss` | `GO_SCTP_FIRST_LOSS_OUTPUT` | Separate run observing original ACK-RACK/timer/PTO trace formats |

The two loss patterns are distinct. A private selector test is not a timer
expiry, a source result is not Rust acceptance, and an observed internal branch
from one run must not be assigned retroactively to another run. See SPEC0040
for pending functional gates and the unchanged encrypted Engine regression.

## Later scoped functional controls

`actual-controls-2026-10-27/` retains actual source TSVs used by the native ignored readers for settings, per-message PR, fragmentation, RTO and live stream-policy changes. Assign the corresponding `GO_SCTP_{KNOB,PR,FRAGMENT,RTO,POLICY}_TSV` absolute paths when rerunning native consumers. ACK/adaptive/fractional/PTO/shutdown outputs and original failed-source fixtures remain separately in repository evidence; their observer code is retained in `source-controls/`. These finite actual controls are not complete API, SCTP or SDK certification. All stages derive from fixed reference inputs and must not alter those checkouts.
