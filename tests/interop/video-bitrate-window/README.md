# Ordinary video bitrate report intervals

LiveKit Server v1.13.10 source is pinned at `e5655ac20267fa5005dccaf3f0e3bfc3ea688c07`. Run the Go observer from that reference module with Go 1.26 and its cached module dependencies. Set the source directory to the Rust checkout being tested.

```sh
cd "$fixed_livekit_go_source"
GOTOOLCHAIN=local GOPROXY=off GOSUMDB=off go run -mod=readonly \
  "$rust_server_source/tests/interop/video-bitrate-window/observe-go.go"
cd "$rust_server_source"
cargo test --locked -p rtc-media --lib video_bitrate_windows -- --nocapture
cargo test --locked -p lk-config --lib video_bitrate_report_intervals
```

The Go observer marshals RTP and feeds the original StreamTrackerManager's real worker. It checks the first positive bitrate report, absence of a premature report, and removal of the active tracker. A separate screen-share case clears the interval map with YAML null and checks that the original manager does not add a tracker.

The Rust media regression publishes VP8 through the production engine over authenticated ICE, DTLS and SRTP using an in-process datagram route. It checks that real decoded RTP yields a measurement at the configured camera or screen-share deadline and that an absent map suppresses bitrate reporting without rejecting video. Unpublishing and closing the transport releases the source. The configuration test covers merging supplied spatial-layer overrides with defaults and clearing the map.

This scope covers ordinary independently tracked spatial streams. Dependency-descriptor aggregate clocks, packet and frame tracker availability thresholds, temporal-layer accounting, and byte-exact bitrate equivalence need separate evidence. Nonpositive interval entries are treated as disabled by Rust. Go's worker uses `time.NewTicker` for such entries and has no normal reporting outcome.

## Aggregate dependency-descriptor report clock

Run `observe-dd-go.go` from the same pinned Go module. It marshals Pion RTP
with original Go dependency descriptors, parses each descriptor with the pinned
Go parser, then uses the real stream tracker manager's DD worker. The camera
and screen-share profiles deliberately assign 900 ms to spatial layer 1. Both
layers still report at the clock selected by layer 0. The null-map case creates
no aggregate tracker.

The real encrypted VP9 regression runs the production parser and media engine
through ICE, DTLS and SRTP. It sends two distinct spatial decode targets using
the fixed Go serializer's wire encoding. Both layers report together at 125 ms
or 450 ms, and a null map or absent base entry suppresses reporting while
the encrypted video still reaches the receiver. The test unpublishes the source
and checks that its track and transport are gone.

```sh
cd "$fixed_livekit_go_source"
GOTOOLCHAIN=local GOPROXY=off GOSUMDB=off go run -mod=readonly \
  "$rust_server_source/tests/interop/video-bitrate-window/observe-dd-go.go"
cd "$rust_server_source"
cargo test --locked -p rtc-media --lib video_dd_bitrate_windows -- --nocapture
```

This extends the interval consumer to sources with an actual parsed aggregate
descriptor. The ordinary video and ONE/RID paths keep their own clocks. The
tests compare the timing and positive/negative outcomes. Exact rate accounting,
tracker availability thresholds, and the Go pause/resume epoch are separate
behavior.

The aggregate observer also sends two equal-payload RTP packets with different
header extension lengths in one original Go tracker window. The reported rates
must have the ratio of the independently serialized complete RTP byte counts,
within integer rounding. The Rust encrypted VP9 test independently sums the
captured datagram lengths before the first report, removes the negotiated SRTP
tag and checks the exact per-spatial rates. It covers complete unpadded RTP
headers and extensions. The receiver already measures logical RTP bytes before
removing RTP padding and after excluding the two-byte RTX OSN. This byte count
now feeds the aggregate tracker. Mixed padded media and RTX rate differentials
remain open along with temporal-layer and pause/resume behavior.

## Aggregate pause and resume

The Go observer updates the track's mute state through the original stream
tracker manager while continuing to marshal and parse real DD RTP. The muted
worker drains its pending sample, ignores incoming media, and stops reporting
bytes. Unmute retires the prior generation. Bare continuation descriptors do
not restart reporting. A fresh attached descriptor starts a full new window.

The Rust regression drives the public media commands over a production
authenticated VP9 path. It independently records when the new attached
descriptor is delivered in the SRTP stream and checks the first resumed
report exactly one configured interval after that delivery. It checks that
the prior sample generation is retired and the publication and transport
are released. The fixture does not assert target availability notifications,
temporal-layer rates, or mixed padded/RTX byte accounting.

## Aggregate temporal target costs

Run the fixed Go temporal observer from the pinned module:

```sh
GOTOOLCHAIN=local GOPROXY=off GOSUMDB=off go run -mod=readonly \
  "$rust_server_source/tests/interop/video-bitrate-window/observe-dd-temporal-go.go"
```

The original Go DD parser and tracker count each present decode target into its
own temporal cell. Two targets in the same base cell each contribute their
packet bytes. The original forwarder defaults to the highest positive cell and
uses the base cell when it is the only measured one. The encrypted VP9 Rust
tests use the Go-generated DD bytes and independently count the actual SRTP
datagrams. They check the exact two projected costs over the configured
125 ms interval. This scope covers the default projection in the existing
spatial allocator. Subscriber FPS budgets, target availability, and mixed
padding/RTX comparisons need separate evidence.
