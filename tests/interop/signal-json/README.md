# Real signaling and worker JSON sockets

Prestart task-owned fixed Go and Rust instances. Run run.mjs for each with
LIVEKIT_URL, LIVEKIT_API_KEY/SECRET, SIGNAL_JSON_GO_CASES/RESULTS and
SIGNAL_JSON_OUTPUT. Run worker.mjs with corresponding WORKER_JSON variables.
Cases/results are the independently executed oracle in tests/oracles/signal-json.
Use compare.mjs with Go report, Rust report and output arguments: comparison is
whole report equality including actual close code, clean flag and reason.

Signal: 31 real authenticated frame outcomes, four enum/trickle cases explicitly
component-only because actual ICE lifecycle is a separate test. Worker: 20 real
authenticated Register/Ping or closed socket cases. Registration is not a job
execution proof. Node transport errors after registration are retained and must
be followed by a bounded actual close, rather than manufactured into outcomes.
Official SDK media/job regressions remain separate. No SDK mutation is used.

The final Signal fixture sends no unrelated marker after an oracle-invalid frame.
For accepted frames it requires the correlated Pong, then sends a distinct
malformed sentinel and verifies actual server-initiated normal parse-error close.
This avoids using a client close handshake while unrelated participant responses
are pending. Code, clean flag, reason and original10s deadlines remain exact.
Earlier actual Go close1006 failures from the prior fixture remain archived;
this is not evidence that system load caused them or that all close races vanish.
