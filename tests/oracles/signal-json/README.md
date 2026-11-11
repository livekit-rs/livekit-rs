# Actual fixed LiveKit JSON wrapper

From this directory run `GOTOOLCHAIN=go1.26.3 go run -mod=readonly . "$PWD/cases.json"`
and similarly `go run -mod=readonly . "$PWD/worker-cases.json" worker`, saving
JSONL to task-owned temporary paths. Local protocol/server references are pinned
by the project reference lock. The imported LiveKit utils/protojson wrapper sets
AllowPartial and DiscardUnknown true. Plain Google protojson defaults are an
incorrect oracle for this service.

From the Rust workspace run the ignored tests actual_fixed_livekit_signal_json
and actual_fixed_livekit_worker_json with SIGNAL_JSON_GO_CASES/RESULTS or
WORKER_JSON_GO_CASES/RESULTS pointing to the corresponding absolute files.
Full frame EOF is required here, unlike the separate Twirp first-value reader.
Component acceptance is not proof that arbitrary control messages were consumed
by a real RTC session; the authenticated WebSocket fixtures provide that boundary.
