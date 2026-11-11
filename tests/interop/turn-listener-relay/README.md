# Embedded TURN listener and relay socket binding

The fixed Go server gives each TURN listener a Pion port-range relay
generator. The generator uses the configured listener address to open the
UDP relay socket. The address it advertises to clients is the configured
node IP for the listener family.

The observer launches one owned daemon per profile and drives an
authenticated TURN allocation with the pinned Pion client. It uses a real
local UDP peer inside the configured ICE range. The peer verifies the
datagram source and sends a reply to that source. The observer verifies the
reply through TURN and records the advertised allocation independently.

| Profile | Listener configuration | Actual UDP relay source | Advertised IP |
| --- | --- | --- | --- |
| explicit-v4 | An assigned local IPv4 distinct from loopback | That assigned local IPv4 | 127.0.0.1 |
| wildcard-v4 | Omitted (default wildcard), connecting to the assigned non-loopback IPv4 | 127.0.0.1 for the loopback peer | 127.0.0.1 |
| explicit-v6 | ::1 | ::1 | ::1 |

Each profile has UDP and TCP control connections. TCP uses
`turn.external_tls: true` to exercise the plain listener intended for a TLS
terminator. The observer also sends authenticated Allocate requests with an
explicit opposite address family and checks the wire error code 508. Positive
controls must return the expected payload in both directions, and every
daemon must exit zero and release the listener and relay ports.

Build from the pinned Go server module using the same pinned Go toolchain:

```sh
GOTOOLCHAIN=local GOPROXY=off go build -mod=readonly \
  -o /tmp/turn-listener-observer /absolute/path/to/observe.go
```

Run the matrix for each immutable Go or Rust daemon using fresh output
directories and an IPv4 already assigned to the local machine:

```sh
for profile in explicit-v4 wildcard-v4 explicit-v6; do
  for transport in udp tcp; do
    /tmp/turn-listener-observer -binary /absolute/path/to/livekit-server \
      -out "/tmp/turn-${profile}-${transport}-new" \
      -v4-bind "$ASSIGNED_LOCAL_IPV4" -profile "$profile" -transport "$transport"
  done
done
```

The observer creates its own ephemeral ports and fixture credentials. It does
not change host networking or trust. The output includes the actual daemon
SHA256, server log, generated YAML, wire observations and cleanup status.
