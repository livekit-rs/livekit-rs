# Official AgentServer functional fixture

`run-worker.mjs` loads the unchanged locked `@livekit/agents` 1.9.4 and
`@livekit/rtc-node` 1.2.1 from `AGENT_SDK_ROOT`. Set `LIVEKIT_URL`, fixture API
credentials, and `AGENT_SDK_EVIDENCE` to run against one actual server.
The coordinating test runner owns server processes and runs Go and Rust separately.

`AGENT_JOB_TYPE` selects one real job type (default `0`):

| Value | Actual trigger and assertions |
| --- | --- |
| `0` ROOM | Explicit dispatch; unchanged full room-agent lifecycle. |
| `1` PUBLISHER | A standard human participant joins through native RTC, publishes a microphone audio track, and captures 48 kHz mono sine PCM until termination. Actual outbound RTP packet and byte counters must advance. The offered and retained job must target this human. |
| `2` PARTICIPANT | A standard human participant joins through native RTC after dispatch creation. The offered and retained job must target this human. |

All modes assert the real job type on the request, child context and retained
Dispatch job, a non-fake child, AGENT participant kind, metadata/attributes,
exactly one acceptance, actual shutdown callback, participant disappearance,
OS child PID exit, worker shutdown and room cleanup. Dependent AGENT participants
must not recursively launch another job. No SDK source, generated worker protocol,
RTC source or original media/timeout acceptance standard is changed. The publisher
counters establish actual local RTP output, not end-to-end audio quality or a
performance result. Evidence contains counts and booleans, not tokens or raw IDs.

The SDK's `numIdleProcesses=0` branch retains a closed executor in `activeJobs`.
The fixture records that diagnostic cache state, but proves process termination
with the actual child PID (`ESRCH`), shutdown callback and room departure.

The locked Agent SDK sends `RunningJobInfo` through `child_process.fork` with
Node's default JSON serialization. Protobuf `Message.toJSON()` therefore turns
nonzero child job enums into names such as `JT_PARTICIPANT`. The child fixture
uses that same unchanged SDK's generated `Job.fromJson` to read its job type;
request-side and REST-client generated Jobs are still asserted numeric. The raw
child IPC enum is retained as a safe diagnostic. All three semantic type checks
remain mandatory. After closing the human room, tracks, source and worker, the
parent calls the public `rtc.dispose()` and awaits completion; it does not call
`process.exit` to mask native-runtime resources keeping the process alive.

`wire-control.mjs` compares 19 actual public worker observations. The approved
`ServerInfo.debugInfo` product identity exception is validated before normalizing
its presence: fixed Go must omit it or send an empty string; Rust must send exactly
`livecloud <workspace.package.version>`, matching `signal/ice.rs::server_info` and
the frozen Cargo manifest. Arbitrary nonempty values fail. Only after these checks
do both sides receive the same identity marker. Evidence records this exception;
this is functional protocol comparison, not byte-identical `ServerInfo`. All
other registration fields, HTTP auth/control results, codec switches and close
codes retain their comparisons. Historical registration-extra-field red evidence
remains a real fixture expectation failure and is not rewritten.
