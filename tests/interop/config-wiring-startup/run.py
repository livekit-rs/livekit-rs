#!/usr/bin/env python3
"""Paired startup-surface differential for the wiring-boundary validations.

SPEC-0077 / SPEC-0074 C24. Upstream performs four validations while *wiring*
services, not while loading config: `InitializeServer` runs after `getConfig`,
`ValidateKeys` and `LoadTURNSecrets` (`cmd/server/main.go:249-305`), and builds
the selector (`wire_gen.go:64`), the WebRTC IP filter (`wire_gen.go:148` ->
`roommanager.go:126`) and the embedded TURN server (`wire_gen.go:157`) in that
order. A construction error is printed with `fmt.Println(err)` and exits 1
(`main.go:190-192`), so the refusal is bare upstream text on stdout.

This harness asserts both halves of that contract against the real binaries:
  * refusal rows — identical stdout bytes and exit code 1 on both sides;
  * start rows   — the kinds upstream constructs without validation really
                   serve HTTP and shut down cleanly on both sides.

Shutdown calibration (measured, not assumed)
-------------------------------------------
The start rows settle `--settle` seconds after the port first accepts before
signalling. Signalling immediately is NOT a startup-acceptance measurement: the
official binary accepts TCP connections before `Start()` sets `s.running`
(`server.go:343-345`, after the log line at `:309`), and `Stop()` early-returns
on `!s.running.Swap(false)` (`server.go:390`) without closing `doneChan`, so
`Start()` blocks forever. `main.go:313` loops `for i := range 2`, so a *second*
signal releases it. The `early_signal_window` section below reproduces and
records that upstream race instead of letting it silently fail the start rows.

Every original exit code is preserved; no pipe filters the result.
"""
import argparse
import hashlib
import json
import os
import signal
import socket
import subprocess
import tempfile
import time
from pathlib import Path

# (id, config body, bare upstream stdout)
REFUSALS = [
    ("unsupported-kind", 'node_selector:\n  kind: "bogus-kind"\n',
     "unsupported node selector"),
    ("regionaware-blank-region", "node_selector:\n  kind: regionaware\n",
     "current region cannot be blank"),
    ("regionaware-unknown-latlon",
     "region: US-west\nnode_selector:\n  kind: regionaware\n  regions:\n"
     "  - name: US-east\n    lat: 1.0\n    lon: 2.0\n",
     "unknown lat and lon for the current region"),
    ("turn-enabled-no-ports", "turn:\n  enabled: true\n", "invalid TURN ports"),
    ("ips-includes-bad-cidr", "rtc:\n  ips:\n    includes: [nope]\n",
     "invalid CIDR address: nope"),
]

# Kinds `CreateNodeSelector` builds without validating; single-node local
# routing never invokes them, so they must start.
STARTS = [
    ("cpuload", "node_selector:\n  kind: cpuload\n"),
    ("sysload", "node_selector:\n  kind: sysload\n"),
    ("random", "node_selector:\n  kind: random\n"),
    ("regionaware-matching",
     "region: US-east\nnode_selector:\n  kind: regionaware\n  regions:\n"
     "  - name: US-east\n    lat: 1.0\n    lon: 2.0\n"),
]

DRIVEN_ITEMS = sorted({
    "YAML:node_selector.kind",
    "YAML:node_selector.regions",
    "YAML:region",
    "YAML:turn.enabled",
    "YAML:rtc.ips.includes",
    "YAML:port",
    "YAML:bind_addresses",
    "YAML:keys",
    "YAML:rtc.tcp_port",
    "base_flags:--config",
})


def identity(path):
    path = Path(path).resolve()
    return {"path": str(path), "sha256": hashlib.sha256(path.read_bytes()).hexdigest()}


def free_port():
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def base_config(port, body):
    """Disable the default ICE TCP port without duplicating a `rtc:` key.

    7881 may be held by an unrelated long-running server on the test host; a
    collision there is an environment artifact, not a config-surface result.
    """
    head = (f"port: {port}\nbind_addresses: [127.0.0.1]\n"
            "keys:\n  devkey: secretsecretsecretsecretsecretsecret\n")
    if body.startswith("rtc:") or "\nrtc:" in "\n" + body:
        lines = body.split("\n")
        for i, line in enumerate(lines):
            if line.rstrip() == "rtc:":
                lines.insert(i + 1, "  tcp_port: 0")
                break
        return head + "\n".join(lines)
    return head + "rtc:\n  tcp_port: 0\n" + body


def run_refusal(binary, body, work):
    cfg = work / "c.yaml"
    cfg.write_text(base_config(free_port(), body))
    r = subprocess.run([str(binary), "--config", str(cfg)],
                       capture_output=True, timeout=60)
    return {"exit_code": r.returncode,
            "stdout": r.stdout.decode(errors="replace"),
            "stderr": r.stderr.decode(errors="replace")}


def wait_http(proc, port, timeout):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if proc.poll() is not None:
            return False
        try:
            with socket.create_connection(("127.0.0.1", port), timeout=0.5):
                return True
        except OSError:
            time.sleep(0.02)
    return False


def terminate(proc, window, second_signal=False):
    """SIGTERM, then optionally a second SIGTERM, then SIGKILL. Returns details."""
    started = time.monotonic()
    proc.send_signal(signal.SIGTERM)
    second_sent = False
    try:
        code = proc.wait(timeout=window)
    except subprocess.TimeoutExpired:
        if second_signal:
            proc.send_signal(signal.SIGTERM)
            second_sent = True
            try:
                code = proc.wait(timeout=window)
            except subprocess.TimeoutExpired:
                os.killpg(proc.pid, signal.SIGKILL)
                code = proc.wait()
        else:
            os.killpg(proc.pid, signal.SIGKILL)
            code = proc.wait()
    return {"exit_code": code,
            "sigterm_to_exit_s": round(time.monotonic() - started, 3),
            "sigkill_required": code == -signal.SIGKILL,
            "second_sigterm_sent": second_sent}


def run_start(binary, body, work, settle, window):
    cfg = work / "c.yaml"
    port = free_port()
    cfg.write_text(base_config(port, body))
    log = (work / "server.log").open("wb")
    proc = subprocess.Popen([str(binary), "--config", str(cfg)], stdout=log,
                            stderr=subprocess.STDOUT, start_new_session=True)
    try:
        reached = wait_http(proc, port, 20)
        alive = proc.poll() is None
        if alive:
            time.sleep(settle)
            alive = proc.poll() is None
        details = terminate(proc, window) if alive else {
            "exit_code": proc.returncode, "sigterm_to_exit_s": None,
            "sigkill_required": False, "second_sigterm_sent": False}
    finally:
        if proc.poll() is None:
            os.killpg(proc.pid, signal.SIGKILL)
            proc.wait()
        log.close()
    return {"http_reachable": reached, "survived_grace": alive, **details,
            "log": (work / "server.log").read_text(errors="replace")}


def run_early_signal(binary, work, window, second_signal):
    """Signal at the first successful connect, with no settle."""
    cfg = work / "c.yaml"
    port = free_port()
    cfg.write_text(base_config(port, STARTS[0][1]))
    log = (work / "server.log").open("wb")
    proc = subprocess.Popen([str(binary), "--config", str(cfg)], stdout=log,
                            stderr=subprocess.STDOUT, start_new_session=True)
    try:
        reached = wait_http(proc, port, 20)
        details = terminate(proc, window, second_signal) if proc.poll() is None else {
            "exit_code": proc.returncode, "sigterm_to_exit_s": None,
            "sigkill_required": False, "second_sigterm_sent": False}
    finally:
        if proc.poll() is None:
            os.killpg(proc.pid, signal.SIGKILL)
            proc.wait()
        log.close()
    text = (work / "server.log").read_text(errors="replace")
    return {"http_reachable": reached, **details,
            "logged_exit_requested": "exit requested, shutting down" in text,
            "log": text}


def main():
    p = argparse.ArgumentParser(description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--official", type=Path, required=True)
    p.add_argument("--rust", type=Path, required=True)
    p.add_argument("--output", type=Path, required=True)
    p.add_argument("--settle", type=float, default=2.0,
                   help="seconds to wait after the port accepts before SIGTERM")
    p.add_argument("--shutdown-window", type=float, default=45.0)
    p.add_argument("--early-signal-trials", type=int, default=2)
    p.add_argument("--early-signal-window", type=float, default=8.0)
    a = p.parse_args()
    a.output = a.output.resolve()
    a.output.mkdir(parents=True, exist_ok=False)

    receipt = {
        "status": "RUNNING",
        "scope": ("startup surface of the four wiring-boundary validations: five "
                  "refusals compared byte-for-byte plus four kinds that must serve"),
        "official": identity(a.official),
        "rust": identity(a.rust),
        "driven_items": DRIVEN_ITEMS,
        "settle_s": a.settle,
        "shutdown_window_s": a.shutdown_window,
        "rows": [],
    }
    path = a.output / "results.json"

    def save():
        tmp = path.with_suffix(".tmp")
        tmp.write_text(json.dumps(receipt, indent=2, ensure_ascii=False) + "\n")
        tmp.replace(path)

    save()
    for name, body, expected in REFUSALS:
        row = {"id": name, "kind": "refusal", "expected_stdout": expected + "\n"}
        with tempfile.TemporaryDirectory(prefix="wiring-go-") as g, \
                tempfile.TemporaryDirectory(prefix="wiring-rs-") as r:
            row["go"] = run_refusal(a.official, body, Path(g))
            row["rust"] = run_refusal(a.rust, body, Path(r))
        row["match"] = (
            row["go"]["exit_code"] == row["rust"]["exit_code"] == 1
            and row["go"]["stdout"] == row["rust"]["stdout"] == expected + "\n"
        )
        receipt["rows"].append(row)
        save()
        print(f"refusal {name}: match={row['match']} "
              f"go_exit={row['go']['exit_code']} rust_exit={row['rust']['exit_code']}",
              flush=True)

    for name, body in STARTS:
        row = {"id": name, "kind": "start"}
        with tempfile.TemporaryDirectory(prefix="wiring-go-") as g, \
                tempfile.TemporaryDirectory(prefix="wiring-rs-") as r:
            row["go"] = run_start(a.official, body, Path(g), a.settle, a.shutdown_window)
            row["rust"] = run_start(a.rust, body, Path(r), a.settle, a.shutdown_window)
        row["match"] = all(
            row[s]["http_reachable"] and row[s]["survived_grace"]
            and row[s]["exit_code"] == 0 and not row[s]["sigkill_required"]
            for s in ("go", "rust")
        )
        receipt["rows"].append(row)
        save()
        print(f"start {name}: match={row['match']} "
              + " ".join(f"{s}(http={row[s]['http_reachable']} exit={row[s]['exit_code']})"
                         for s in ("go", "rust")), flush=True)

    # Characterization of the upstream readiness race that forces --settle.
    # The Rust requirement is asserted (a signal must be caught and logged, not
    # taken at the default disposition). Go's result is recorded, not asserted:
    # hanging here is upstream's own `running.Swap(false)` early-return.
    early = {
        "assertion": ("rust must catch the signal, log 'exit requested, shutting down' "
                      "and exit 0; go is recorded only — see module docstring"),
        "trials": a.early_signal_trials,
        "window_s": a.early_signal_window,
        "go": [], "rust": [], "go_second_signal_release": None,
    }
    for side, binary in (("go", a.official), ("rust", a.rust)):
        for i in range(a.early_signal_trials):
            with tempfile.TemporaryDirectory(prefix=f"wiring-early-{side}-") as d:
                r = run_early_signal(binary, Path(d), a.early_signal_window, False)
            r["trial"] = i
            early[side].append(r)
            save()
            print(f"early-signal {side} trial{i}: exit={r['exit_code']} "
                  f"logged={r['logged_exit_requested']} "
                  f"sigkill={r['sigkill_required']}", flush=True)
    with tempfile.TemporaryDirectory(prefix="wiring-early-go2-") as d:
        early["go_second_signal_release"] = run_early_signal(
            a.official, Path(d), a.early_signal_window, True)
    receipt["early_signal_window"] = early
    save()
    print("early-signal go second-signal release: "
          f"exit={early['go_second_signal_release']['exit_code']} "
          f"second_sent={early['go_second_signal_release']['second_sigterm_sent']}",
          flush=True)

    early_rust_ok = all(
        r["exit_code"] == 0 and r["logged_exit_requested"] and not r["sigkill_required"]
        for r in early["rust"]
    )
    receipt["early_signal_window"]["rust_requirement_met"] = early_rust_ok
    receipt["binary_unchanged"] = (identity(a.official) == receipt["official"]
                                   and identity(a.rust) == receipt["rust"])
    receipt["status"] = "PASS" if (
        receipt["binary_unchanged"] and early_rust_ok
        and all(r["match"] for r in receipt["rows"])
    ) else "FAIL"
    save()
    print(json.dumps({"status": receipt["status"], "rows": len(receipt["rows"]),
                      "matched": sum(1 for r in receipt["rows"] if r["match"]),
                      "early_rust_requirement_met": early_rust_ok}))
    return 0 if receipt["status"] == "PASS" else 1


if __name__ == "__main__":
    raise SystemExit(main())