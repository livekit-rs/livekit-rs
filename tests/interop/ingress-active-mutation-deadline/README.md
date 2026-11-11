# Active Ingress Update/Delete after worker deadline

Build with the fixed LiveKit Go v1.13.10 source at e5655ac20267fa5005dccaf3f0e3bfc3ea688c07 and its locked protocol 9bb708cdb819 and PSRPC v0.8.2. The observer uses generated IngressHandler and IOInfo production clients on actual Redis. This exercises live worker-control and public catalog semantics. The observer supplies controlled worker state and does not ingest or decode media.

    cd /absolute/path/to/pinned-livekit-go
    GOTOOLCHAIN=go1.26.3 GOWORK=off GOPROXY=off GOCACHE=/path/to/owned-cache go build -mod=readonly -o /path/to/owned-observer /path/to/tests/interop/ingress-active-mutation-deadline/observer.go
    python3 /path/to/tests/interop/ingress-active-mutation-deadline/run-owned.py --go /path/to/pinned-Go --rust /path/to/exact-Rust --observer /path/to/owned-observer --redis /path/to/redis-server --output /path/to/new/receipt

Use distinct output directories for before and after. The runner starts its own Redis, Go or Rust server, and original generated handler observer per engine. It creates two actual public reusable RTMP endpoints, verifies their original Redis catalog/state/stream-key and room index, and switches both to PUBLISHING using actual IOInfo. Public UpdateIngress and DeleteIngress then send a 1600ms Twirp timeout. Each actual handler must be claimed on its exact IngressID, receive no more than the wire budget, and exit on its real context deadline before cleanup.

The fixed Go service logs the worker timeout and still performs the Redis mutation. Successful Update must retain the same StartedAt generation and stream key, move the room index and persist new metadata. Successful Delete must return INACTIVE and remove the physical catalog, stream key, room index and state. The next public lookup must return 404. A separate health request and clean owned process/port release are required. This covers the deadline header; it does not claim HTTP disconnect behavior or media-worker restart acceptance.

Repeat the fixed profile with --max-attempts 0 to exercise the accepted configuration where PSRPC has no attempt ceiling. The worker deadline must terminate retry processing and still allow the catalog mutation. The release checks are identical.
