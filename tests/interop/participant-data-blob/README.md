# Participant-owned Blob functionality

These tests use real servers and unchanged JS SDK2.23.0. There is no public SDK
Blob helper: known-field tests explicitly use its internal signaling send seam.
Public DataTrack acceptance remains a separate fixture.

Prepare the actual fixed-Go output using tests/oracles/participant-blob/README.md,
then prestart a server with the declared enable_participant_data_blob value.
From tests/interop run participant-data-blob/run.mjs with:

- PARTICIPANT_BLOB_ENABLED=true or false, matching actual server configuration.
- LIVEKIT_URL pointing to the owned server.
- BLOB_GO_CASES=../oracles/participant-blob/cases.json.
- BLOB_GO_RESULTS pointing to the actual Go JSONL output.
- BLOB_OUTPUT pointing to owned temporary evidence.

Both SDK connection modes retain exact bytes, overwrite/quota, actual signaling
resume and duplicate-identity replacement assertions. All26 raw cases instead
use independent authenticated WebSocket actors: the pinned SDK decoder rejects
one legal Go unknown-field shape. The conformance reader compares all known
semantics and every nested original unknown-byte cohort; it never drops fields.

From the workspace, limits-campaign.py accepts --official and --rust binary
paths, --origin yaml|env|cli and --output for an owned temporary directory. Run
all three input origins separately. It launches only its own loopback children,
stops each before continuing and compares complete paired observations. Default
dedicated ports are19880–19882 and19980–19982; choose unoccupied ports. Developer
keys are public test credentials. Generated configurations are private temporary
files; runtime evidence/logs remain in the specified owned output until cleanup.

F27 delivery evidence records actual results. Historical isolated contracts
marked NOT_RUN describe preintegration, not subsequent accepted campaigns.
Distributed relay and strict text JSON remain separate open leaves.
