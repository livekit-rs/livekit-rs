# STUN family selection during config loading and daemon startup

This observer runs one immutable daemon or its real `ports` command against
owned UDP STUN sockets. In dual-stack profiles, IPv4 and IPv6 share a port
and return their actual source address in the XOR-MAPPED-ADDRESS reply.
The observer records which socket received each binding request.

| Profile | Observed behavior |
| --- | --- |
| `dual-default` | The `ports` command probes the system resolver's first address. On the tested host it is IPv6. |
| `dual-force-yaml` | `rtc.require_ipv4: true` sends one IPv4 probe. |
| `ports-ignore-root-cli`, `ports-ignore-root-env` | The fixed child command does not apply generated root options. The observed destination is the same as the default. |
| `v6-default` | An explicit IPv6 STUN endpoint works in the default configuration. |
| `v6-force` | The same endpoint with IPv4 required produces the fixed Go diagnostic, exit 1 and no STUN datagrams. |
| `daemon-force-cli`, `daemon-force-env` | The main daemon consumes the generated root option and sends IPv4 for node discovery. Its separate source-bound NAT probe goes to IPv6, the family of its configured local interface. Actual HTTP health, exit zero and owned port release are checked. |

Only the loopback resolver and socket pairs are required. No public STUN
service is contacted and no host networking changes are made.

Run with the same observer on each immutable Go and Rust binary:

```sh
python3 /absolute/path/to/observe.py \
  --binary /absolute/path/to/livekit-server \
  --output /tmp/stun-family-new
```

Each output records the binary and observer hashes, full command, relevant
generated environment override, wire observations, exit status and port
release. Main-daemon logs are retained alongside the receipt.
