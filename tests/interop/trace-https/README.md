# HTTPS Jaeger export from a running daemon

This observer opens an owned OTLP collector on loopback, launches one
immutable daemon and performs an authenticated WHIP POST with a Pion
WebRTC offer. It requires a 201 answer and a successful DELETE of the
created WHIP resource. Positive traces must contain an actual
`Sent.WHIP.Create` span with valid IDs and the `livekit` resource from
a decoded OTLP protobuf request.

| Profile | Collector observation |
| --- | --- |
| `http` | A real plaintext OTLP request contains the expected span. |
| `trusted` | The configured trace CA verifies the HTTPS collector, which receives the span over HTTP/2. |
| `mtls` | The HTTPS collector also verifies the daemon's owned temporary client certificate before receiving the span. |
| `untrusted` | The collector observes a real TLS ClientHello and no HTTP request. |
| `wrong-root` | A trace-specific unrelated CA overrides the correct generic CA. The collector observes a TLS ClientHello and no HTTP request. |

Each row checks normal daemon shutdown and exclusive rebinding of
the daemon HTTP, media TCP and collector ports. The mTLS row generates
a short-lived identity in its private output directory. Do not persist
that private key in a shared evidence directory.

Build this observer from the fixed upstream Go module with its pinned
Go toolchain on PATH:

```sh
GOTOOLCHAIN=local GOPROXY=off go build -mod=readonly \
  -o /tmp/trace-https-observer /absolute/path/to/observe.go
```

Run each profile with its exact daemon, public fixture and fresh output path:

```sh
/tmp/trace-https-observer -binary /absolute/path/to/livekit-server \
  -fixture /absolute/path/to/crates/rtc-tls/tests/fixtures \
  -profile trusted -out /tmp/trace-https-trusted-new
```

The observer does not modify system trust or require a separate worker,
Redis process or external collector.
