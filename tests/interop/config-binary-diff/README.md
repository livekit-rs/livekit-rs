# W1-G3 real-binary config/CLI differential (SPEC-0073)

`run.py` runs every case of the config-observer matrix
(`livekit-rs-docs/evidence/<date>/w1-config-observer/cases.json`: argv, ENV,
files with modes) through the unmodified official `livekit-server` (built from
`references/livekit-stable` with go1.26.3 and the protocol/mediatransportutil
`replace`s) and through the Rust `livekit-server`. Each process runs in its own
temp dir with `HOME` set to that dir, observed variables cleared, one process
at a time (ports 7880/7881 must be free).

Per case:
- **Exited before readiness**: the exit status and stdout must be equal. The
  temp dir is substituted back to `{dir}`. No product-identity substitution is
  permitted (SPEC-0074 C31: the Rust binary prints `1.13.10` exactly). Stderr
  log messages are kept (prefix stripped) but not compared, because
  zap console/JSON byte format is the separate logging item in SPEC-0073 §7.
- **Started**: `starting LiveKit server` was logged (upstream logs it after
  every listener is bound). Compared are: survival through a grace period;
  whether a TCP connect to 127.0.0.1:`portHttp` succeeds; and the startup
  values (HTTP port, node IP, bind addresses, RTC TCP port, UDP port or ICE
  range, prometheus port, region). Go's nodeID/version are excluded.

```sh
python3 -I run.py --official /abs/livekit-server-go --rust /abs/target/debug/livekit-server \
  --cases .../cases.json --out results.json [--only PREFIX ...] [--resume]
```

`--resume` reuses rows already in `--out`. Never combine it with `--only` on
the same output file, because `--only` writes only the subset.
