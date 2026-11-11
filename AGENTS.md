# Repository delivery rules

- Complete the requested implementation, meaningful tests and matching spec updates before delivery.
- Commit the task changes and push them to the configured repository; verify the pushed commit identity.
- Stop task-owned test servers and browser processes, and remove task-generated temporary files, certificates, logs and ignored test output after persisting the necessary reproducible evidence in the repository.
- Run `cargo clean` at the Rust workspace root at the end of every completed task. Clean any additional task-created Cargo target directories as well. Do not delete reference checkouts, vendored sources or committed public test fixtures: these are project inputs, not temporary files.
- In parallel work, the coordinating agent owns commit, push and final cleanup. Subagents must not clean shared Cargo output or stop other agents' processes while work or checks are still running.

# NetonLive Cloud integration rules

- livekit-platform is the platform built specifically for livekit-rs, benchmarked against LiveKit Cloud. Any livekit-rs work touching cloud mode, platform integration, project isolation, quotas, metering, node lifecycle or the global mesh must follow the task book [SPEC-0078](https://github.com/livekit-rs/livekit-docs/blob/main/server/specs/0078-netonlive-cloud-node-task.md) and the specs it lists (livekit-rs SPEC-0002, livekit-platform SPEC-0001/0002/0004/0005).
- Official LiveKit compatibility comes first: MsgTrans is only the platform ↔ node control-plane transport and must not replace, change or weaken any official LiveKit protocol. Standalone parity checks must keep passing after cloud changes.
- When a change alters behaviour defined by those specs, update the specs on both sides in the same task before or together with the code.
- Do not modify the Go region agent's HTTP v1 control channel for MsgTrans work; both channels run in parallel until Rust nodes pass full acceptance.
