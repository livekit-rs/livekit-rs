#!/usr/bin/env python3
"""SPEC-0074 C15 agent-wt online wire differential.

Runs the *same* agent control-stream contract against the unmodified official
Go `livekit-server` (v1.13.10 oracle) and the Rust `livekit-server`, one process
at a time, sharing one config (API keys, dev self-signed WebTransport TLS,
Redis) and one minted `video.agent` access token.

The comparison target is the externally visible outcome of the WebTransport
session, not internal state. Each probe mode exercises one part of the session
lifecycle and reports a fixed set of observation keys:

- `register` - the Extended CONNECT upgrade (status 200 + the response
  headers) plus the framed `RegisterWorkerResponse` decoded by the client:
  worker-id shape (`AW_` prefix, length, shortuuid alphabet), the `ServerInfo`
  the server hands back (edition/version/protocol/agent_protocol/region +
  node-id presence, and `debug_info`, which the Go oracle always leaves empty)
  and whether HTTP endpoint settings were negotiated;
- `idle` - the upgrade alone: the server must end the session on its
  `agent.RegisterTimeout` deadline. How the session was closed (a
  `WT_CLOSE_SESSION` capsule vs. a QUIC CONNECTION_CLOSE), with which code and
  reason, is the contract under test;
- `stream` - a second client-initiated WebTransport stream after the
  registration: the official listener accepts extra streams into the session's
  incoming queue and leaves them open, so the peer must not see a reset;
- `close` - the agent ends its control plane (a FIN on the control stream)
  after opening a second stream: the server must end the session, carry the
  `WT_CLOSE_SESSION` capsule and terminate the session's remaining streams
  with `WTSessionGoneErrorCode` (0x170d7b68);
- `datagram` - one RFC 9297 WebTransport datagram: the session must survive it;
- `data_integrity` - write 48 bytes (3 × 16-byte chunks) on a bidi stream and
  FIN the send side: verifies the transport preserves data without reset;
- `unidir` - open a unidirectional stream, write data, FIN: verifies the
  server accepts it without echoing;
- `reconnect` - register, close session, re-connect, register again: verifies
  the server cleans up the first session and accepts the second;
- `reliable_prefix` - write two chunks on a bidi stream, then reset it:
  verifies the transport delivers the contiguous prefix before the reset
  (RESET_STREAM_AT contract).

Two clients are used, one per WebTransport stack generation. The vendored
`h3 0.0.8` is patched to the *final* WebTransport framing
(`vendor/h3/webtransport-final-settings.patch`), so the Rust server speaks the
same framing as the official v1.13.10 server, which embeds
quic-go/webtransport-go v0.12.0. Neither side answers the Extended CONNECT
with a `sec-webtransport-http3-draft` header any more, so the upgrade
negotiation is compared field by field instead of being excluded from the
diff:

- `wtdiff`  - the canonical Go webtransport-go client (matches the Go oracle);
- `wt_probe` - the Rust lk-webtransport client (matches the Rust server).

Parity is asserted over the server-authored fields only. Every probe is run
against *both* servers, so each server is compared through the *same* client
stack (the Go `wtdiff` client first, the Rust `wt_probe` client second) and
the four combinations' results are recorded as evidence rather than hidden.

The two clients are independent implementations of the same protocol, so their
absolute observations are not expected to be byte-identical:
`webtransport-go` v0.12.0 absorbs `WTSessionGoneErrorCode` inside
`ReceiveStream.Read` and reports the session's own close error instead of the
stream error code, while quinn surfaces the code verbatim. The `cross_client`
section therefore publishes the absolute value of every compared field for
both clients next to each other and classifies it, so a client-library
difference is never mistaken for a server difference - and never hidden.

Usage: run.py --official GO_BIN --rust RUST_BIN --probe-go GODIFF_BIN \
       --probe-rust RUSTPROBE_BIN --out RESULTS [--ready-timeout S] [--retries N]
"""
import argparse
import base64
import hashlib
import hmac
import json
import os
import select
import signal
import socket
import subprocess
import sys
import tempfile
import time

MARK = b"starting LiveKit server"
API_KEY = "APIagentwtdiffkey"
SECRET = "agentwtdiffsecret0123456789abcdefghijklmnop"
REGION = "diff-region"

# The probe modes, in the order they are run against every server.
# `reliable_prefix` is a DEFAULT gate. It previously exposed a real Rust-server
# divergence (a client `RESET_STREAM` on an extra bidi stream tore down the
# whole h3 connection while the Go server kept the session alive). That
# divergence was root-caused and fixed - a per-stream reset is no longer
# connection-fatal in the lk-webtransport accept pump - so the mode now matches
# the locked Go oracle. It stays in the default matrix permanently: never drop a
# mode to force green.
MODES = ("register", "idle", "stream", "close", "datagram", "data_integrity", "unidir", "reconnect", "reliable_prefix")

# The two client stacks the matrix is driven with.
PROBES = ("wtdiff", "wt_probe")

# The observation fields compared for each mode between the two servers. The
# comparison is always made through the *same* client stack, so a client
# artifact can never be mistaken for a server difference. Wall-clock timings
# are recorded in the evidence but never compared verbatim; the `idle` mode
# compares the rounded second instead.
COMPARE_FIELDS = {
    "register": (
        "upgrade",
        "response_type",
        "worker_id",
        "server_info",
        "endpoint_settings_present",
        "session_closed",
        "close_kind",
        "close_code",
    ),
    "idle": (
        "upgrade",
        "session_closed",
        "close_kind",
        "close_code",
        "close_message",
        "closed_after_s",
    ),
    "stream": (
        "upgrade",
        "stream_opened",
        "stream_termination",
        "stream_reset",
        "stream_reset_code",
        "session_closed",
        "close_kind",
    ),
    "close": (
        "upgrade",
        "stream_opened",
        # `stream_termination` / `stream_reset` / `stream_reset_code` are
        # COMPARED, not excluded. The driver always compares the two servers
        # through the *same* client, so a client-library artifact cannot leak
        # in as a server difference; conversely a genuine server difference is
        # never hidden. Verified against the locked v1.13.10 oracle: BOTH
        # servers terminate the session's streams with the SAME code
        # (WTSessionGoneErrorCode 0x170d7b68), so these three fields carry no
        # server divergence (`server_agreement` is true). They classify as
        # `client_semantics_difference` only: the Go client (wtdiff) absorbs
        # the code inside webtransport-go and reports `session_gone`/no numeric
        # code for both servers, while the Rust client (wt_probe) surfaces the
        # code verbatim for both servers. The fields stay compared so any future
        # server divergence would be caught; per the restored-gate rule they are
        # never removed to hide a difference.
        "stream_termination",
        "stream_reset",
        "stream_reset_code",
        "session_closed",
        "close_kind",
        "close_code",
        "close_message",
    ),
    "datagram": (
        "upgrade",
        "datagram_sent",
        "datagram_echo",
        "session_closed",
        "close_kind",
    ),
    "data_integrity": (
        "upgrade",
        "stream_opened",
        "stream_write_bytes",
        "stream_fin_sent",
        "stream_termination",
        "stream_reset",
        "stream_reset_code",
        "session_closed",
        "close_kind",
    ),
    "unidir": (
        "upgrade",
        "unidir_opened",
        "unidir_write_bytes",
        "unidir_finish_ok",
        "session_closed",
        "close_kind",
    ),
    "reconnect": (
        "upgrade",
        "worker_id",
        "server_info",
        "first_session_closed",
        "first_close_kind",
        "reconnect_worker_id_shape_match",
        "reconnect_server_info_match",
        "second_session_closed",
        "second_close_kind",
    ),
    "reliable_prefix": (
        "upgrade",
        "stream_opened",
        "stream_write_bytes",
        "stream_reset_sent",
        "stream_reset_code",
        "session_closed",
        "close_kind",
    ),
}

# Where the two client libraries are known to report the same wire event
# differently. The attribution is *verified* by the cross-client section, not
# assumed: the value a client reports is identical against both servers, so a
# difference between the clients cannot have come from the server.
CLIENT_SEMANTICS_NOTES = {
    "stream_reset": (
        "webtransport-go v0.12.0 absorbs WTSessionGoneErrorCode inside "
        "ReceiveStream.Read (ReceiveStream.handleSessionGoneError) and reports "
        "the session's own close error, so the Go client cannot see the reset "
        "the Rust client observes; quinn surfaces the code verbatim."
    ),
    "stream_reset_code": (
        "same absorption: the Go client reports no numeric reset code, the "
        "Rust client reports the server's actual reset code. Verified against "
        "the locked oracle: BOTH servers send the same code "
        "(WTSessionGoneErrorCode 0x170d7b68), so this is purely a client-library "
        "difference, not a server divergence."
    ),
    "stream_termination": (
        "the termination *class* is comparable across the two clients; only "
        "the numeric reset code is client-library dependent. Verified against "
        "the locked oracle: in close mode BOTH clients report "
        "'session_gone' against BOTH servers, so this field is aligned and "
        "carries no client or server difference; the absorption only affects "
        "the numeric stream_reset/stream_reset_code fields."
    ),
    "stream_reset_sent": (
        "reliable_prefix mode: the client-initiated reset code is the same "
        "on both clients (0x170d7b68); the server's reaction (session close "
        "or continued operation) is the comparison target."
    ),
}

# The wall-clock budget of one probe run, per mode. The `idle` mode waits for
# the server's 10s registration deadline, so it needs the longest budget.
MODE_TIMEOUT = {
    "register": 45,
    "idle": 70,
    "stream": 45,
    "close": 45,
    "datagram": 45,
    "data_integrity": 45,
    "unidir": 45,
    "reconnect": 90,
    "reliable_prefix": 45,
}


def free_udp():
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    s.bind(("127.0.0.1", 0))
    port = s.getsockname()[1]
    s.close()
    return port


def port_free(port):
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    try:
        s.bind(("0.0.0.0", port))
    except OSError:
        return False
    finally:
        s.close()
    return True


def part(b):
    return base64.urlsafe_b64encode(b).rstrip(b"=")


def mint_token():
    now = int(time.time())
    head = part(b'{"alg":"HS256","typ":"JWT"}')
    body = part(json.dumps(
        {"iss": API_KEY, "sub": "probe-agent", "nbf": now - 10, "exp": now + 600,
         "video": {"agent": True, "kind": "standard"}}, separators=(",", ":")).encode())
    signing = head + b"." + body
    return (signing + b"." + part(hmac.new(SECRET.encode(), signing, hashlib.sha256).digest())).decode()


def write_config(home, wt_port, http_port):
    # Keys are supplied via the `--keys "key: secret"` argv flag (portable to
    # both binaries), so the YAML stays key-agnostic.
    cfg = os.path.join(home, "diff.yaml")
    with open(cfg, "w") as f:
        f.write(
            "development: true\n"
            f"port: {http_port}\n"
            "bind_addresses:\n"
            "  - 127.0.0.1\n"
            f"region: {REGION}\n"
            "webtransport:\n"
            f"  port: {wt_port}\n"
            "redis:\n"
            "  address: localhost:6379\n"
            "  db: 7\n"
            "rtc:\n"
            "  tcp_port: 0\n"
            "  udp_port: 0\n"
            "  use_external_ip: false\n"
            "logging:\n"
            "  level: info\n"
            "  json: false\n"
        )
    return cfg


def wait_ready(proc, fds, deadline):
    """Pump stderr until the `starting LiveKit server` line appears."""
    out = b""
    while time.time() < deadline:
        r, _, _ = select.select(list(fds), [], [], 0.1)
        for fd in r:
            try:
                chunk = os.read(fd, 65536)
            except BlockingIOError:
                continue
            if not chunk:
                fds.pop(fd, None)
                continue
            out += chunk
            if MARK in out:
                return True, out
        if proc.poll() is not None and not fds:
            return False, out
    return False, out


def probe_once(probe, addr, token, mode, retries, timeout=None):
    """Run one probe in `mode` against `addr`, retrying to absorb the
    accept-pump spawn race without treating a handshake timeout as a contract
    difference. Returns (observation|None, last_stderr)."""
    budget = timeout or MODE_TIMEOUT.get(mode, 45)
    last_err = ""
    for attempt in range(retries):
        try:
            p = subprocess.run([probe, "--addr", addr, "--token", token, "--mode", mode],
                               capture_output=True, text=True, timeout=budget)
        except subprocess.TimeoutExpired:
            last_err = "probe wall-clock timeout"
            time.sleep(0.4)
            continue
        if p.returncode == 0 and p.stdout.strip():
            try:
                return json.loads(p.stdout.strip().splitlines()[-1]), None
            except ValueError as e:
                last_err = f"unparseable probe output: {e}\n{p.stderr[-1500:]}"
        else:
            last_err = (p.stderr or "")[-1500:]
        time.sleep(0.4)
    return None, last_err


def run_binary(label, binary, cfg, home, wt_port, token, probes, ready_timeout, retries):
    argv = [binary, "--config", cfg, "--keys", f"{API_KEY}: {SECRET}"]
    env = {k: v for k, v in os.environ.items()
           if not k.startswith(("LIVEKIT_", "LIVECLOUD_", "REDIS_"))}
    env["HOME"] = home
    env["NO_COLOR"] = "1"
    proc = subprocess.Popen(argv, stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL,
                            stderr=subprocess.PIPE, env=env, cwd=home, start_new_session=True)
    fds = {proc.stderr.fileno(): "err"}
    os.set_blocking(proc.stderr.fileno(), False)
    ready, log = wait_ready(proc, fds, time.time() + ready_timeout)
    result = {"binary": label, "ready": ready, "wt_port": wt_port,
              "server_log_tail": log.decode("utf-8", "replace")[-2000:],
              "matrix": {}}
    try:
        if ready:
            addr = f"127.0.0.1:{wt_port}"
            for probe_name, probe_bin in probes.items():
                per_mode = {}
                for mode in MODES:
                    # The idle mode is slow by construction (it waits for the
                    # server's registration deadline), so it gets fewer
                    # retries: a failure there is a contract failure, not a
                    # startup race.
                    mode_retries = min(retries, 3) if mode == "idle" else retries
                    obs, err = probe_once(probe_bin, addr, token, mode, mode_retries)
                    per_mode[mode] = {"ok": obs is not None, "observation": obs, "error": err}
                    print(f"[{label}/{probe_name}/{mode}] ok={obs is not None}",
                          file=sys.stderr, flush=True)
                result["matrix"][probe_name] = per_mode
    finally:
        if proc.poll() is None:
            os.killpg(proc.pid, signal.SIGKILL)
        try:
            proc.wait(timeout=10)
        except subprocess.TimeoutExpired:
            pass
    return result


def compare(go, rust):
    """Compare the two servers through the same client stack, mode by mode."""
    if not (go["ready"] and rust["ready"]):
        return "readiness", {"go_ready": go["ready"], "rust_ready": rust["ready"]}
    by_probe_mode = {}
    failures = []
    for probe in go["matrix"]:
        for mode in MODES:
            g = go["matrix"].get(probe, {}).get(mode, {})
            r = rust["matrix"].get(probe, {}).get(mode, {})
            go_obs = g.get("observation")
            rust_obs = r.get("observation")
            key = f"{probe}/{mode}"
            if go_obs is None or rust_obs is None:
                by_probe_mode[key] = {
                    "status": "probe_failed",
                    "go_ok": g.get("ok"),
                    "rust_ok": r.get("ok"),
                    "go_error": g.get("error"),
                    "rust_error": r.get("error"),
                }
                failures.append(key)
                continue
            diffs = {}
            for field in COMPARE_FIELDS.get(mode, ()):
                if go_obs.get(field) != rust_obs.get(field):
                    diffs[field] = {"go": go_obs.get(field), "rust": rust_obs.get(field)}
            by_probe_mode[key] = {
                "status": "match" if not diffs else "divergence",
                "diff": diffs,
                "go": go_obs,
                "rust": rust_obs,
            }
            if diffs:
                failures.append(key)
    status = "match" if not failures else "divergence"
    return status, {"by_probe_mode": by_probe_mode, "failed": failures}


def cross_client(go, rust):
    """Publish the absolute observations of both clients next to each other.

    The two probes are independent implementations of the same protocol, so
    their absolute values may differ even when both servers behave identically.
    Every compared field is therefore published with its absolute value for
    both clients against both servers, and classified:

    - `aligned` - both clients report the same value against both servers;
    - `client_semantics_difference` - each client reports the *same* value
      against both servers (so no server difference exists), but the two
      clients report it differently;
    - `server_difference` - a client reports different values against the two
      servers, which is a real contract divergence;
    - `not_observed` - one of the four observations is missing, so the field
      carries no absolute value.

    The classification is derived, never assumed: `server_difference` is
    decided by comparing the two servers *within* one client.
    """
    by_mode = {}
    counts = {"aligned": 0, "client_semantics_difference": 0,
              "server_difference": 0, "not_observed": 0}
    for mode in MODES:
        observations = {}
        for probe in PROBES:
            go_obs = (go["matrix"].get(probe, {}).get(mode) or {}).get("observation") or {}
            rust_obs = (rust["matrix"].get(probe, {}).get(mode) or {}).get("observation") or {}
            observations[probe] = {"go": go_obs, "rust": rust_obs}
        fields = {}
        for field in COMPARE_FIELDS.get(mode, ()):
            values = {probe: {"go": observations[probe]["go"].get(field),
                              "rust": observations[probe]["rust"].get(field)}
                      for probe in PROBES}
            observed = all(observations[probe]["go"] and observations[probe]["rust"]
                           for probe in PROBES)
            if not observed:
                server_agreement = None
                client_agreement = None
                classification = "not_observed"
            else:
                server_agreement = all(values[probe]["go"] == values[probe]["rust"]
                                       for probe in PROBES)
                client_agreement = all(values["wtdiff"][server] == values["wt_probe"][server]
                                       for server in ("go", "rust"))
                if not server_agreement:
                    classification = "server_difference"
                elif not client_agreement:
                    classification = "client_semantics_difference"
                else:
                    classification = "aligned"
            counts[classification] += 1
            entry = {
                "values": values,
                "server_agreement": server_agreement,
                "client_agreement": client_agreement,
                "classification": classification,
            }
            note = CLIENT_SEMANTICS_NOTES.get(field)
            if note and classification == "client_semantics_difference":
                entry["note"] = note
            fields[field] = entry
        by_mode[mode] = fields
    return by_mode, counts


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--official", required=True)
    ap.add_argument("--rust", required=True)
    ap.add_argument("--probe-go", required=True)
    ap.add_argument("--probe-rust", required=True)
    ap.add_argument("--out", required=True)
    ap.add_argument("--ready-timeout", type=float, default=30)
    ap.add_argument("--retries", type=int, default=15)
    a = ap.parse_args()
    for b in (a.official, a.rust, a.probe_go, a.probe_rust):
        assert os.access(b, os.X_OK), f"not executable: {b}"

    token = mint_token()
    probes = {"wtdiff": os.path.abspath(a.probe_go), "wt_probe": os.path.abspath(a.probe_rust)}
    binaries = {"go": os.path.abspath(a.official), "rust": os.path.abspath(a.rust)}
    results = {"official": binaries["go"], "rust": binaries["rust"],
               "probe_go": probes["wtdiff"], "probe_rust": probes["wt_probe"],
               "modes": list(MODES), "runs": []}
    # One fresh process, one fresh config dir per binary; go first, then rust, on
    # distinct free ports so no socket is ever shared.
    for label, binary in (("go", binaries["go"]), ("rust", binaries["rust"])):
        for _ in range(50):
            http_port = free_udp()
            if port_free(http_port):
                break
        wt_port = free_udp()
        with tempfile.TemporaryDirectory(prefix=f"c15wt-{label}-") as home:
            cfg = write_config(home, wt_port, http_port)
            res = run_binary(label, binary, cfg, home, wt_port, token, probes,
                             a.ready_timeout, a.retries)
        res["http_port"] = http_port
        results["runs"].append(res)
        print(f"[{label}] ready={res['ready']}", file=sys.stderr, flush=True)

    go, rust = results["runs"]
    status, detail = compare(go, rust)
    cross, cross_counts = cross_client(go, rust)
    results["status"] = status
    results["result"] = detail
    results["cross_client"] = cross
    results["cross_client_summary"] = cross_counts
    json.dump(results, open(a.out, "w"), indent=1)
    summary = {key: value["status"] for key, value in detail.get("by_probe_mode", {}).items()}
    print(json.dumps({"status": status, "by_probe_mode": summary,
                      "cross_client_summary": cross_counts}, indent=1))
    sys.exit(0 if status == "match" else 1)


if __name__ == "__main__":
    main()
