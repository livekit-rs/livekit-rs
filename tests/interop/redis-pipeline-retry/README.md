# Egress Redis pipeline retry and Ingress transaction reply loss

The Go executable calls the pinned original RedisStore methods StoreEgress
and DeleteIngress through protocol/redis.GetRedisClient. The Rust executable
calls lk-compat's production IoStore methods. The wire tap forwards RESP
to an owned Redis broker and records actual replies, then checks durable
Egress and Ingress state through an independent Redis CLI connection.

Build Go with the locked module from tests/oracles/sip-callbacks, using the
same local pinned LiveKit replacement as the command retry observer. Use the
checked-in original-go-observer.go as main.go. Build Rust with
cargo build --locked -p lk-compat --example redis_pipeline_observer.
Run python3 run.py --go <go> --rust <rust> --expect transaction-closed --output <new-dir>.
The same script with --expect before and the Rust production baseline captures
the pre-fix behavior. The runner imports the checked-in redis-command-retry
fixture's RESP decoder and owned Redis setup.

The Egress tests distinguish explicit first-reply LOADING, its retry ceiling,
a similarly worded ordinary error, a later LOADING, and a physically executed
pipeline with a lost later reply. Earlier/later effects are checked even when
the overall response is an error.

The Ingress fixtures cover an ordinary successful deletion and two reply-loss
boundaries. Closing before forwarding EXEC and closing after actual EXEC both
make Go and the corrected Rust return an error without replay. After the
actual EXEC, the tap verifies all four successful deletions then recreates
the ingress through an independent Redis connection before closing the client.
The corrected operation preserves the recreation and recovers its command pool
for a positive Egress store. The earlier Rust control replays the batch and
deletes the recreation.

The current Rust connection abstraction cannot distinguish an authenticated
client writer failure before flush from a lost EXEC reply. Managed atomic
pipelines therefore fail closed on transport failure. Upstream's separate
pre-write retry semantics and exact transport error diagnostics are open.
