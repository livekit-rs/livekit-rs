# Ordinary pooled Redis command rejection differential

The observer executes an ordinary GET through LiveKit's production Redis
client on each implementation. A loopback RESP tap returns explicit error
replies before forwarding the affected commands, then forwards successful
attempts to an actual Redis instance. Every row gets a fresh owned broker.
The subsequent successful read proves the command pool is still usable.

The Go observer calls the pinned `protocol/redis.GetRedisClient`. To build it,
copy `original-go-observer.go` into an isolated temporary module with the
repository's `tests/oracles/sip-callbacks/go.mod` and `go.sum`. Point the copied
local LiveKit replacement at the repository's pinned `references/livekit-stable`
directory. Build with `GOTOOLCHAIN=go1.26.3 go build -mod=readonly`.

Build the Rust observer with `cargo build --locked -p lk-compat --example
redis_command_retry_observer` from the exact source under test. Then run
`python3 run.py --go <go-observer> --rust <rust-observer> --expect after
--output <new-directory>`. Redis server and Redis CLI must be on PATH.
The pre-fix witness uses the same observer with an older Rust production source
and `--expect before`.

The fixture covers six Redis rejection codes, the four-attempt ceiling,
ordinary errors containing a typed error word, ACL and wrong-type controls,
and the no-fault path. The wire records attempt times and physical connection
reuse. READONLY moves to a new socket, while the other covered errors keep the
socket. This fixture does not cover ambiguous writes, pipeline retry semantics,
or the source of server-generated faults in a deployment.
