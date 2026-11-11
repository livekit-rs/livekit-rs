# Official listener host resolution

The observer starts one owned immutable server per profile. The same YAML
shape and CLI argument are used with the fixed Go and candidate Rust servers.

| Profile | Observation |
| --- | --- |
| `hostname`, `hostname-cli` | `localhost` actually serves HTTP, Prometheus and debug over IPv4 and negotiates a certificate-verified QUIC `h3` handshake. The TCP listeners do not bind IPv6 when localhost has an IPv4 result. The startup line retains the configured hostname. |
| `numeric` | The same endpoints are reachable with the IPv4 literal control. |
| `nil-list`, `wildcard` | The omitted list and the list containing the empty host keep wildcard listening. |
| `empty-list` | HTTP, metrics and debug refuse connections while WebTransport still binds a wildcard socket and negotiates QUIC. |
| `nonlocal`, `malformed` | A valid address is followed by a failing address. The daemon exits 1 without announcing readiness, retains the Go-formatted diagnostic and releases all reserved ports. |

For every successful startup the observer waits for clean SIGTERM exit. All
profiles require TCP and UDP port release. The test uses ephemeral local
ports, a public fixture CA and loopback. It does not rely on an external
resolver returning NXDOMAIN. QUIC acceptance here tests the WebTransport
listener and TLS, rather than the CONNECT/session behavior covered by the
separate WebTransport interoperability suite.

Build the observer from the fixed upstream Go module with its pinned
toolchain on PATH:

```sh
GOTOOLCHAIN=local GOPROXY=off go build -mod=readonly \
  -o /tmp/bind-host-observer /absolute/path/to/observe.go
```

Run each profile with a fresh output directory and the exact executable and
public `crates/rtc-tls/tests/fixtures` directory:

```sh
/tmp/bind-host-observer -binary /absolute/path/to/livekit-server \
  -fixture /absolute/path/to/crates/rtc-tls/tests/fixtures \
  -profile hostname -out /tmp/bind-host-hostname-new
```

Each output records the executable hash, generated YAML, raw daemon log,
real endpoint outcomes and shutdown status.
