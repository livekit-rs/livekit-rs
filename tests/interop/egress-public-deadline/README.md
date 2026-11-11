# Public Egress worker deadline

Build the observer with fixed Go LiveKit Server v1.13.10 (e5655ac20267fa5005dccaf3f0e3bfc3ea688c07), its locked protocol 9bb708cdb819 and psrpc v0.8.2. The observer uses the production generated EgressInternal server and the real Redis PSRPC transport. It is a worker-context probe and does not produce media.

    cd /absolute/path/to/pinned-livekit-go
    GOTOOLCHAIN=go1.26.3 GOWORK=off GOPROXY=off GOCACHE=/path/to/owned-cache go build -mod=readonly -o /path/to/owned-observer /path/to/tests/interop/egress-public-deadline/observer.go
    python3 /path/to/tests/interop/egress-public-deadline/run-owned.py --go /path/to/pinned-Go --rust /path/to/exact-Rust --observer /path/to/owned-observer --redis /path/to/redis-server --output /path/to/new/receipt

Use distinct new output directories for old and new binaries. In each engine the runner starts independent Redis, observer and server processes on owned ports. The held public StartWebEgress request carries a 1600ms X-Twirp-Timeout-Ms header. The real claimed handler must enter, see at most 1600ms between PSRPC SentAt and Expiry, and exit on that context deadline before fixture cleanup. The public 408 must match the fixed Go retry wrapper's canceled/request canceled result. An independent subsequent call must run the actual worker's typed response, return its real EgressID and persist it through public ListEgress. A final health request and clean process/port release are mandatory. JSON field-name styles are normalized only for positive EgressID extraction. This test does not assert protojson serialization parity generally.

Go/Twirp lifetime inputs are part of the receipt. The process that implements the controlled handler exits at the real request deadline. This does not imply a real recorder already launched by an external Egress service must be stopped when its HTTP caller times out.
