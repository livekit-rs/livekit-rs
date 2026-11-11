# Unknown-field binary/storage oracle

Coordinator-owned execution only. Compile this whole subpackage (`go build ./unknown`), not `unknown/main.go` by itself: `expanded.go` contains the additional families and legacy cases. Uses the fixed generated SIP Protobuf client, real Redis seeding/readback and real `ProtoReflect().GetUnknown()` observations. JSON comparisons cannot establish unknown-field preservation.

Invoke separately for the fixed Go and Rust endpoint with its corresponding isolated Redis database:

```
unknown-oracle -url <actual-server> -redis <its-owned-redis-db> -api-key <test-key> -api-secret <test-secret> -out <safe-report.json>
```

No server or broker is started by the oracle. It does not call telephone workers. Evidence contains case/source, unknown-field hashes and actual match booleans; generated entity identities, raw payload, endpoints and credentials are omitted. Exit is nonzero if any field-provenance observation differs from the declared source-derived expectation. Created records are cleaned by actual IDs.

The original eleven inbound observations are retained. Expanded coverage adds outbound lifecycle and repeated patch, dispatch-rule root/media/Rule/oneof/repeated RoomConfig.Agents subtrees, explicit child replacement, rule deletion response, cross-request isolation and legacy conversions. Actual total is computed from emitted observations; do not claim a result before executing both servers.

Expected provenance is not “always retain unknowns”: patch clones existing info; replace clones only replacement; an explicitly supplied nested Media/Rule replaces that child; unknowns on a request/patch envelope are not copied into record; legacy As* conversion constructs fresh known fields and drops source unknowns. Raw stored legacy data remains untouched when a typed patch creates the modern record.

The old F04 executable was deleted by the required cargo-clean delivery step. A future mechanism reproduction using the unchanged known-only `T::decode` catalog path must be labeled as such and tied to that frozen source hash; it cannot be reported as an old full-server binary execution. The current oracle is suitable for exact old-binary red only if the coordinator separately rebuilds and hashes that historical source.
