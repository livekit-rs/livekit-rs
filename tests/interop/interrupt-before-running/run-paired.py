#!/usr/bin/env python3
"""SPEC-0077 [1146] RUNTIME:Server.InterruptBeforeRunningRace — the six-delay SIGINT sweep, paired.

Upstream `LivekitServer.Start()` opens the listeners, then sleeps 100ms before it flips its own
`running` flag (`references/livekit-stable/pkg/service/server.go:340-347`) and blocks on
`<-s.doneChan`. `Stop(force)` runs `router.Drain()` first, then returns **early** at
`if !s.running.Swap(false) { return }` (`server.go:390-392`) — before `router.Stop()` and
`close(s.doneChan)` at `:394-395`. An interrupt that lands inside that window therefore drains the
router, never closes `doneChan`, and leaves `Start()` blocked forever.

This harness measures that instead of assuming it: for each delay it times SIGINT from the
`starting LiveKit server` readiness line, waits with a fixed cap, and records the exit code, whether
SIGKILL was needed, the elapsed time, the achieved offset and the process's own `nodes` entry before
and after. Both binaries get identical config bytes (ports excepted) and the same six delays, and
the report keeps the two outcomes side by side rather than comparing them for equality: this item
exists precisely because the candidate does not reproduce the stall, and mirroring an upstream hang
is a product decision, not a compatibility one.

The anchor is a log line and not an HTTP 200 on purpose. `healthCheck` answers 406 until the node
stats are less than 4s stale (`server.go:451-464`), which is up to a full second after the sleep has
already ended — an HTTP-200 anchor would make every "inside the window" vector actually measure the
outside. The line at :309 is emitted 34 statements before the sleep, after `RegisterNode` (:222) and
after every listener has been created (:248-270). The hung process is probed over HTTP before it is
killed, so the receipt shows a node that answers requests while `Start()` is still parked on
`<-s.doneChan`: serving and unhurried to die at the same time.

The per-vector `nodes` observability is what makes the stall claim about the *unregister* path and
not about a missing entry: `router.Stop()` (`:394`, the HDEL) sits after the early return, so a
killed run must leave its own record behind while a graceful run must remove it.

The classifier itself gets a positive control: a dummy process that ignores SIGINT must be
classified as stalled with exit -9, so a green sweep cannot come from a stop path that silently
never waits.

Not covered here: the graceful-stop surfaces of the start-path items themselves (their harnesses
settle 0.5s before interrupting for exactly this reason), the *second* signal's force semantics
(`RUNTIME:Server.ForceShutdownOnSecondSignal`, covered by the drain/restart harnesses), and any
Windows/colima-specific signal delivery.
"""

import argparse
import hashlib
import json
import os
import pathlib
import re
import shutil
import signal
import socket
import subprocess
import sys
import tempfile
import time
import urllib.error
import urllib.request

OFFICIAL_VERSION = "livekit-server version 1.13.10"
ITEM = "RUNTIME:Server.InterruptBeforeRunningRace"
UPSTREAM_RUNNING_SECONDS = 0.100  # server.go:343-345
STALL_CAP_SECONDS = 20.0  # the cap the item records against
GRACEFUL_EXIT_BUDGET_SECONDS = 1.0
NODES_KEY = b"nodes"
# Readiness anchor: the `starting LiveKit server` line (server.go:309), which upstream emits a few
# statements before the post-listener sleep at :343. HTTP 200 is *not* usable here: the health
# handler answers 406 until the node stats are 4s-fresh (server.go:451-464), so a 200 would land
# roughly a second after the flag was already set and no inside-window vector could be measured.
READY_PATTERN = b"starting LiveKit server"
# The recorded measurement, timed from that anchor. Below 0.100s the reference is inside its own
# post-listener sleep; at and above it the flag is already set.
DELAYS = (0.00, 0.03, 0.08, 0.12, 0.20, 0.50)
INSIDE = [d for d in DELAYS if d < UPSTREAM_RUNNING_SECONDS]


def sha(path: pathlib.Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def free(port: int) -> bool:
    with socket.socket() as probe:
        probe.settimeout(0.2)
        return probe.connect_ex(("127.0.0.1", port)) != 0


def wait_port(port: int, timeout: float = 25.0) -> bool:
    end = time.monotonic() + timeout
    while time.monotonic() < end:
        if not free(port):
            return True
        time.sleep(0.05)
    return False


def run_checked(argv, **kw) -> subprocess.CompletedProcess:
    done = subprocess.run(argv, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, **kw)
    assert done.returncode == 0, (argv, done.stdout[-800:])
    return done


def start_process(argv, log: pathlib.Path) -> subprocess.Popen:
    with log.open("wb") as handle:
        return subprocess.Popen(argv, stdout=handle, stderr=subprocess.STDOUT,
                                start_new_session=True)


def http_status(port: int | None) -> int | None:
    """`GET /`'s status, or None when nothing answered.

    Any status counts as an answer: the health handler replies 406 while the node stats are staler
    than 4s and 200 once they are fresh (`pkg/service/server.go:451-464`), so either one proves the
    accept loop is live while `running` is still false."""
    if port is None:
        return None
    try:
        with urllib.request.urlopen(f"http://127.0.0.1:{port}/", timeout=2) as reply:
            return reply.status
    except urllib.error.HTTPError as answered:
        return answered.code
    except OSError:
        return None


def wait_for_anchor(log: pathlib.Path, proc: subprocess.Popen, needle: bytes) -> float:
    """Block until `needle` is in the process log and return that monotonic instant."""
    deadline = time.monotonic() + 60.0
    while time.monotonic() < deadline:
        if needle in log.read_bytes():
            return time.monotonic()
        if proc.poll() is not None:
            break
        time.sleep(0.002)
    raise AssertionError(f"{needle!r} never appeared in {log.name}")


def finish(proc: subprocess.Popen, delay: float, cap: float, log: pathlib.Path,
           http_port: int | None, on_ready=None) -> dict:
    """Deliver SIGINT `delay` after the readiness line and classify the outcome.

    The anchor is the `starting LiveKit server` line (server.go:309), 34 lines before the sleep at
    :343, so the requested delay is the distance into the startup window. The *achieved* offset is
    what gets recorded and classified, so a slow host shows up as a drifted vector instead of as a
    behaviour difference. A stalled run is probed over HTTP before it is killed: an answer there is
    the mechanism itself — the node accepts requests while `Start()` is still parked on
    `<-s.doneChan` with `running == false`.

    The control vector has no HTTP port and anchors on its own `ready` line.
    """
    anchor = wait_for_anchor(log, proc, READY_PATTERN if http_port else b"ready")
    ready_extra = on_ready() if on_ready else {}
    time.sleep(max(0.0, delay - (time.monotonic() - anchor)))
    interrupt_at = time.monotonic()
    achieved = round(interrupt_at - anchor, 4)
    exited_early = proc.poll() is not None
    sigkilled, exit_code, stop_seconds, status_while_hung = False, None, None, None
    if not exited_early:
        os.killpg(proc.pid, signal.SIGINT)
        try:
            exit_code = proc.wait(timeout=cap)
        except subprocess.TimeoutExpired:
            # Still alive: `Stop()` returned at :390-392 and never reached `close(s.doneChan)`.
            status_while_hung = http_status(http_port)
            sigkilled = True
            os.killpg(proc.pid, signal.SIGKILL)
            exit_code = proc.wait()
        stop_seconds = round(time.monotonic() - interrupt_at, 3)
    time.sleep(0.2)
    text = log.read_bytes().decode("utf-8", "replace")
    return {
        "anchored_on_startup_line": True,
        "interrupt_after_startup_line_seconds": achieved,
        "inside_upstream_running_window": achieved < UPSTREAM_RUNNING_SECONDS,
        "exited_early": exited_early,
        "exit": exit_code,
        "sigkilled": sigkilled,
        "stop_seconds": stop_seconds,
        "cap_seconds": cap,
        "http_status_while_hung": status_while_hung,
        "shutdown_log_lines": len(re.findall(r"exit requested, shutting down", text)),
        "log_bytes": len(text.encode()),
        **ready_extra,
    }


class Resp:
    def __init__(self, port: int):
        self.sock = socket.create_connection(("127.0.0.1", port), timeout=5)
        self.file = self.sock.makefile("rb")
        assert self.call(b"PING") == b"PONG"

    def read(self):
        line = self.file.readline(1 << 20)
        tag, data = line[:1], line[1:-2]
        if tag in (b"+", b"-"):
            return data
        if tag == b":":
            return int(data)
        if tag == b"$":
            size = int(data)
            body = self.file.read(size)
            assert len(body) == size and self.file.read(2) == b"\r\n"
            return body
        if tag == b"*":
            return [self.read() for _ in range(int(data))]
        raise AssertionError("unexpected RESP type " + repr(tag))

    def call(self, *args):
        self.sock.sendall(b"*" + str(len(args)).encode() + b"\r\n"
                          + b"".join(b"$" + str(len(v)).encode() + b"\r\n" + v + b"\r\n"
                                     for v in args))
        return self.read()

    def close(self):
        self.file.close()
        self.sock.close()


def node_entries(port: int) -> list[str]:
    conn = Resp(port)
    try:
        return sorted(v.decode(errors="replace")
                      for v in (conn.call(b"HKEYS", NODES_KEY) or []))
    finally:
        conn.close()


def clear_nodes(port: int) -> None:
    conn = Resp(port)
    try:
        conn.call(b"DEL", NODES_KEY)
        assert conn.call(b"HKEYS", NODES_KEY) == []
    finally:
        conn.close()


def redis_server(port: int, staging: pathlib.Path) -> subprocess.Popen:
    home = staging / f"redis-{port}"
    home.mkdir(parents=True, exist_ok=True)
    proc = start_process(["redis-server", "--port", str(port), "--bind", "127.0.0.1",
                          "--protected-mode", "no", "--save", "", "--appendonly", "no",
                          "--dir", str(home)], staging / f"redis-{port}.log")
    assert wait_port(port), f"redis-server never came up on {port}"
    return proc


def config_body(tcp_port: int, udp_port: int, redis_port: int) -> str:
    return ("keys: {devkey: secret}\nlogging: {level: info, json: true}\n"
            "bind_addresses: [127.0.0.1]\n"
            "rtc: {node_ip: 127.0.0.1, use_external_ip: false, "
            f"tcp_port: {tcp_port}, udp_port: {udp_port}, "
            "enable_loopback_candidate: true}\n"
            "turn: {enabled: false}\n"
            f"redis:\n  address: 127.0.0.1:{redis_port}\n")


def sweep(binary: pathlib.Path, tag: str, delay: float, redis_port: int, ports: dict,
          args: argparse.Namespace) -> dict:
    """One (side, delay) vector: empty keyspace, start, interrupt at the delay, classify."""
    clear_nodes(redis_port)
    path = args.output / f"{tag}-{delay:.2f}s.yaml"
    path.write_text(config_body(ports["tcp"], ports["udp"], redis_port))
    path.chmod(0o600)
    log = args.output / f"{tag}-{delay:.2f}s.server.log"
    proc = start_process([str(binary), "--dev", "--config", str(path),
                          "--port", str(ports["http"])], log)
    result = finish(proc, delay, STALL_CAP_SECONDS, log, ports["http"],
                    # Read the hash at the anchor, before anything is interrupted: both sides
                    # register before the readiness line (Go `server.go:222` inside `Start()`, Rust
                    # `node_router.start()` at crates/livecloud/src/server.rs:712 before
                    # `log_startup`), so the record must already be there. Without this the
                    # after-stop counts would only prove that an empty hash stayed empty.
                    on_ready=lambda: {"own_entries_while_running": len(node_entries(redis_port)),
                                      "nodes_fields_while_running": node_entries(redis_port)})
    # The entry is read again after the process is gone: a stalled run must still show it, which is
    # exactly the `router.Stop()` (`server.go:394`) that the early return skips.
    result["own_entries_after_stop"] = len(node_entries(redis_port))
    result["nodes_fields_after_stop"] = node_entries(redis_port)
    result["config_sha256"] = sha(path)
    return result


def stall_control(args: argparse.Namespace, staging: pathlib.Path) -> dict:
    """Positive control for the classifier: a process that ignores SIGINT must be reported as
    stalled with a SIGKILL exit, so `sigkilled`/`stop_seconds` are measurements and not defaults."""
    log = args.output / "control-ignores-sigint.log"
    proc = start_process([sys.executable, "-c",
                          "import signal, time\n"
                          "signal.signal(signal.SIGINT, signal.SIG_IGN)\n"
                          "print('ready', flush=True)\n"
                          "time.sleep(120)\n"], log)
    try:
        result = finish(proc, 0.05, 3.0, log, None)
    finally:
        if proc.poll() is None:
            os.killpg(proc.pid, signal.SIGKILL)
            proc.wait()
    assert result["sigkilled"], ("the classifier missed a process that ignores SIGINT", result)
    assert result["exit"] == -9, ("a SIGKILLed child must report -9", result["exit"])
    assert result["stop_seconds"] >= 3.0 - 0.5, ("the cap was not honoured", result)
    assert not result["exited_early"], result
    assert 0.0 <= result["interrupt_after_startup_line_seconds"] < 0.5, \
        ("the control interrupt did not land where it was asked", result)
    assert result["shutdown_log_lines"] == 0, \
        ("a process that ignores SIGINT must not log the shutdown line", result)
    result["kind"] = "control-ignores-sigint"
    return result


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--official", required=True, type=pathlib.Path)
    ap.add_argument("--rust", required=True, type=pathlib.Path)
    ap.add_argument("--output", required=True, type=pathlib.Path)
    ap.add_argument("--base-port", type=int, default=21900)
    cmd = ap.parse_args()

    official, rust = cmd.official.resolve(), cmd.rust.resolve()
    version = run_checked([str(official), "--version"]).stdout.strip()
    assert version == OFFICIAL_VERSION, ("locked reference binary required", version)
    redis_bin = shutil.which("redis-server")
    assert redis_bin, "redis-server is required to observe the unregister path"
    cmd.output.mkdir(parents=True, exist_ok=True)
    staging = pathlib.Path(tempfile.mkdtemp(prefix="interrupt-before-running-"))

    base = cmd.base_port
    used = {name: base + offset for name, offset in (
        ("redis", 0), ("go_http", 1), ("go_tcp", 2), ("go_udp", 3),
        ("rs_http", 4), ("rs_tcp", 5), ("rs_udp", 6))}
    for name, port in used.items():
        assert free(port), f"port {port} ({name}) is already in use"

    report = {
        "status": "RUNNING",
        "items": [ITEM],
        "scope": (
            "the signal-handling window only: SIGINT timed from the `starting LiveKit server` "
            f"readiness line at {', '.join(f'{d:.2f}s' for d in DELAYS)} against the upstream 100ms "
            "post-listener sleep (pkg/service/server.go:309-347), on both binaries with identical "
            "config bytes and an empty `nodes` hash per vector. Recorded per side: exit code, "
            "whether SIGKILL was needed after the 20s cap, the achieved interrupt offset, the "
            "elapsed stop time, the `exit requested, shutting down` line count, the HTTP status of "
            "a hung node, and the process's own `nodes` entry at the anchor and after it is gone. "
            "The two sides are NOT compared for equality: the difference is the finding."),
        "official_binary": {"path": str(official), "sha256": sha(official), "version": version},
        "rust_binary": {"path": str(rust), "sha256": sha(rust)},
        "upstream_running_seconds": UPSTREAM_RUNNING_SECONDS,
        "stall_cap_seconds": STALL_CAP_SECONDS,
        "delays": list(DELAYS),
        "delays_inside_window": INSIDE,
        "cases": [],
        "controls": [],
        "divergences": [],
    }

    def save() -> None:
        (cmd.output / "BATCH.json").write_text(json.dumps(report, indent=2) + "\n")

    procs: list[subprocess.Popen] = []
    code = 1
    try:
        procs.append(redis_server(used["redis"], staging))
        report["redis_version"] = run_checked([redis_bin, "--version"]).stdout.strip()

        report["controls"].append(stall_control(cmd, staging))
        save()

        for delay in DELAYS:
            go = sweep(official, "official", delay, used["redis"],
                       {"http": used["go_http"], "tcp": used["go_tcp"],
                        "udp": used["go_udp"]}, cmd)
            assert not go["exited_early"], (delay, "official died before the interrupt", go)
            rs = sweep(rust, "rust", delay, used["redis"],
                       {"http": used["rs_http"], "tcp": used["rs_tcp"],
                        "udp": used["rs_udp"]}, cmd)
            assert not rs["exited_early"], (delay, "candidate died before the interrupt", rs)
            # An interrupt that lands *later* than requested is not a parity failure, it is a vector
            # that never got measured. The inside-window ones are gated on the achieved offset so a
            # loaded host fails loudly here instead of quietly producing a "graceful" reference that
            # would then have to be explained away after the fact.
            for tag, side in (("official", go), ("rust", rs)):
                # Each side must have registered before being interrupted, otherwise "left behind"
                # and "removed" would both be trivially true about a hash that was never written.
                assert side["own_entries_while_running"] == 1, \
                    (delay, tag, "expected exactly this process's node entry at the anchor", side)
                # Whatever is still in the hash after the process is gone must be the record that
                # was registered during this very vector, identified by its field name.
                assert set(side["nodes_fields_after_stop"]) <= set(
                    side["nodes_fields_while_running"]), \
                    (delay, tag, "a node record that this run never registered appeared", side)
                assert side["interrupt_after_startup_line_seconds"] >= delay - 0.001, \
                    (delay, tag, "the interrupt landed before the requested offset", side)
            if delay < UPSTREAM_RUNNING_SECONDS:
                assert go["inside_upstream_running_window"], \
                    (delay, "the official interrupt drifted outside the 100ms window it was meant "
                            "to probe; re-run rather than re-label", go)

            rec = {"id": f"interrupt-{delay:.2f}s", "ledger_item": ITEM, "delay": delay,
                   "inside_upstream_running_window": delay < UPSTREAM_RUNNING_SECONDS,
                   "official": go, "rust": rs}
            report["cases"].append(rec)

            # --- the reference behaviour is asserted, not assumed -------------------------
            if go["sigkilled"]:
                assert go["nodes_fields_after_stop"] == go["nodes_fields_while_running"], \
                    (delay, "the record left behind must be the one registered at the anchor", go)
            if delay < UPSTREAM_RUNNING_SECONDS:
                assert go["sigkilled"], (delay, "the reference was expected to stall inside its "
                                         "own post-listener sleep", go)
                assert go["exit"] == -9, (delay, "a stalled reference must be SIGKILLed", go)
                assert go["stop_seconds"] >= STALL_CAP_SECONDS - 0.5, \
                    (delay, "the reference exited before the measurement cap", go)
                assert go["own_entries_after_stop"] == 1, \
                    (delay, "the skipped router.Stop() (server.go:394) must leave the node "
                            "record behind", go)
                # The mechanism, measured: still answering requests while `running` was never set.
                assert go["http_status_while_hung"] in (200, 406), \
                    (delay, "a hung node that stopped serving would be a different failure than "
                            "the one this item records", go)
            else:
                assert not go["sigkilled"], (delay, "the reference stalled after `running` was "
                                            "set", go)
                assert go["exit"] == 0, (delay, "the reference left uncleanly", go)
                assert go["stop_seconds"] <= GRACEFUL_EXIT_BUDGET_SECONDS, \
                    (delay, "the reference did not stop promptly", go)
                assert go["own_entries_after_stop"] == 0, \
                    (delay, "the graceful path must HDEL its own node record", go)

            # --- the candidate: no window reproduces the stall ----------------------------
            assert not rs["sigkilled"], (delay, "the candidate stalled; "
                                         "RUNTIME:Server.InterruptBeforeRunningRace would have "
                                         "become a real behaviour difference instead of a "
                                         "product decision", rs)
            assert rs["exit"] == 0, (delay, "the candidate left uncleanly", rs)
            assert rs["stop_seconds"] <= GRACEFUL_EXIT_BUDGET_SECONDS, \
                (delay, "the candidate did not stop promptly", rs)
            assert rs["own_entries_after_stop"] == 0, \
                (delay, "the candidate's unregister did not run", rs)
            assert rs["shutdown_log_lines"] >= 1, \
                (delay, "the candidate never reached the shutdown log line", rs)

            # The divergence is recorded per delay so the receipt shows the whole table, and any
            # flip of either side has already failed an assertion above.
            report["divergences"].append([
                f"{delay:.2f}s",
                {"official": "stalled-then-sigkilled" if go["sigkilled"] else "graceful",
                 "rust": "stalled-then-sigkilled" if rs["sigkilled"] else "graceful",
                 "official_exit": go["exit"], "rust_exit": rs["exit"],
                 "official_own_entries_after_stop": go["own_entries_after_stop"],
                 "rust_own_entries_after_stop": rs["own_entries_after_stop"]}])
            save()

        # Coverage of the two regimes, keyed on what was *achieved* rather than on the requested
        # delays: the sweep has to have measured a stall and a graceful exit, or the finding is not
        # in this receipt. (Re-deriving `len(INSIDE)` from `DELAYS` here would only restate the
        # constant, so every clause below reads the recorded cases.)
        assert len(report["cases"]) == len(DELAYS), "every requested delay must be recorded"
        stalled = [c for c in report["cases"] if c["official"]["sigkilled"]]
        graceful = [c for c in report["cases"] if not c["official"]["sigkilled"]]
        assert stalled and all(c["official"]["inside_upstream_running_window"] for c in stalled), \
            ("the reference must stall, and only inside the window",
             [c["official"]["interrupt_after_startup_line_seconds"] for c in stalled])
        assert graceful and all(not c["official"]["inside_upstream_running_window"]
                                for c in graceful), \
            ("the reference must stop gracefully, and only outside the window",
             [c["official"]["interrupt_after_startup_line_seconds"] for c in graceful])
        assert all(not c["rust"]["sigkilled"] for c in report["cases"]), \
            "the candidate must not stall at any delay"
        report["status"] = "PASS"
        code = 0
        report["finding"] = (
            "measured on this host: the reference ignores SIGINT at "
            + ", ".join(f"{d:.2f}s" for d in INSIDE)
            + " (each stayed alive to the 20s cap and was SIGKILLed with its own `nodes` entry "
              "left behind) and exits 0 at "
            + ", ".join(f"{d:.2f}s" for d in DELAYS if d >= UPSTREAM_RUNNING_SECONDS)
            + "; the candidate exits 0 and unregisters at all six. Whether to mirror an upstream "
              "hang is a product decision, not a compatibility one, so the difference is recorded "
              "here and the item stays open instead of being absorbed into a parity count.")
    except BaseException:
        report["status"] = "FAIL"
        raise
    finally:
        (cmd.output / "BATCH.json").write_text(json.dumps(report, indent=2) + "\n")
        for proc in procs:
            if proc.poll() is None:
                try:
                    os.killpg(proc.pid, signal.SIGTERM)
                    proc.wait(timeout=10)
                except (OSError, subprocess.TimeoutExpired):
                    try:
                        os.killpg(proc.pid, signal.SIGKILL)
                        proc.wait(timeout=10)
                    except (OSError, subprocess.TimeoutExpired):
                        pass
        shutil.rmtree(staging, ignore_errors=True)
    return code


if __name__ == "__main__":
    sys.exit(main())
