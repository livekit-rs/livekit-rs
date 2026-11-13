#!/usr/bin/env python3
"""Attribute the **server-start** Redis connection-count divergence by purpose.

`RUNTIME:Redis.ServerStartConnectionChurn` recorded 8 reference connections against
30 candidate connections in the TLS-relay witness of the 21o/21p start-path runs, but
a TLS handshake count cannot say *why* each connection exists. This harness puts a
RESP tap between each binary and an owned plain Redis, starts the real server, waits
for the HTTP API to answer, and categorizes **every** connection by the command text
the client actually wrote:

  * subscription  — the connection carries SUBSCRIBE (a psrpc message-bus reader)
  * command       — the connection carries pool commands (PING/HSET/HVALS/SET/...)
  * hello-only    — the connection only completed the RESP3 handshake (an idle dial)

The fixed Go contract (`references/psrpc/pkg/bus/redisbus/redisbus.go:63-83,262`)
multiplexes **all** subscriptions over one `ps` PubSub connection and shares one
`rc` pool for commands. The Rust transport
(`crates/lk-compat/src/service_bus/redis_transport.rs:205-238`) dials a **dedicated**
connection per subscription and each consumer owns its own command connection
(`redis_endpoint.rs:478-483,958-983`). This witness binds that attribution to the
current candidate with per-connection traces, and asserts the qualitative difference
so the recording is a check, not a note. It does **not** claim connection-count parity:
the count is a resource shape, and the functional startup (HTTP 200) is the gate.

Usage:
  python3 run-attribution.py --official /abs/livekit-server --rust /abs/livekit-server \
      --base-port 24460 --output DIR
"""
from __future__ import annotations

import argparse
import hashlib
import itertools
import json
import os
import pathlib
import shutil
import signal
import socket
import subprocess
import tempfile
import threading
import time
import urllib.request

LOCK = threading.Lock()
ITEM = "RUNTIME:Redis.ServerStartConnectionChurn"
COMMAND_NAMES = {
    "ping", "hset", "hvals", "hdel", "hget", "hgetall", "set", "get", "del", "exists",
    "publish", "eval", "evalsha", "multi", "exec", "watch", "unwatch", "expire",
    "sadd", "srem", "smembers", "incr", "decr", "lpush", "rpush", "zadd", "setnx",
}


def sha(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def free(port: int) -> bool:
    with socket.socket() as probe:
        probe.settimeout(0.2)
        return probe.connect_ex(("127.0.0.1", port)) != 0


def start_redis(port: int, staging: pathlib.Path) -> subprocess.Popen:
    home = staging / f"redis-{port}"
    home.mkdir(parents=True, exist_ok=True)
    log = (home / "redis.log").open("wb")
    proc = subprocess.Popen(
        ["redis-server", "--port", str(port), "--bind", "127.0.0.1",
         "--protected-mode", "no", "--save", "", "--appendonly", "no", "--dir", str(home)],
        stdout=log, stderr=subprocess.STDOUT, start_new_session=True)
    deadline = time.monotonic() + 20
    while time.monotonic() < deadline:
        if not free(port):
            return proc
        time.sleep(0.05)
    raise AssertionError("owned redis never came up on " + str(port))


class Tap:
    """Records every connection and every chunk the client wrote, verbatim."""

    def __init__(self, tap_port: int, upstream: tuple[str, int]):
        self.upstream = upstream
        self.connections: list[dict] = []
        self.sock = socket.socket()
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.sock.bind(("127.0.0.1", tap_port))
        self.sock.listen(64)
        self.stop = threading.Event()
        self.thread = threading.Thread(target=self._serve, daemon=True)
        self.thread.start()

    def _serve(self):
        self.sock.settimeout(0.5)
        while not self.stop.is_set():
            try:
                client, _ = self.sock.accept()
            except socket.timeout:
                continue
            except OSError:
                break
            threading.Thread(target=self._handle, args=(client,), daemon=True).start()

    def _handle(self, client: socket.socket):
        try:
            remote = socket.create_connection(self.upstream, timeout=10)
        except OSError:
            client.close()
            return
        entry = {"index": 0, "client_writes": []}
        with LOCK:
            entry["index"] = len(self.connections) + 1
            self.connections.append(entry)
        for src, dst, record in ((client, remote, True), (remote, client, False)):
            threading.Thread(target=self._pipe,
                             args=(src, dst, entry, record), daemon=True).start()

    @staticmethod
    def _pipe(src: socket.socket, dst: socket.socket, entry: dict, record: bool) -> None:
        # Record each chunk as it arrives, not only at close: a persistent server
        # keeps its connections open through the snapshot window, so deferring the
        # write until EOF would leave every live connection's trace empty and
        # misclassify it as hello-only.
        def store(chunk: bytes) -> None:
            if record and chunk:
                with LOCK:
                    entry["client_writes"].append(
                        chunk.decode("latin-1").replace("\r\n", "\\r\\n"))
        try:
            src.settimeout(10)
            first = src.recv(4096)
        except OSError:
            first = b""
        store(first)
        if first:
            try:
                dst.sendall(first)
            except OSError:
                pass
        while True:
            try:
                chunk = src.recv(65536)
            except OSError:
                break
            if not chunk:
                break
            store(chunk)
            try:
                dst.sendall(chunk)
            except OSError:
                break
        for handle in (src, dst):
            try:
                handle.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass
            handle.close()

    def reset(self) -> None:
        with LOCK:
            self.connections.clear()

    def snapshot(self) -> list[dict]:
        with LOCK:
            return [dict(c) for c in self.connections]

    def close(self) -> None:
        self.stop.set()
        try:
            self.sock.close()
        except OSError:
            pass


def command_traces(wire: str) -> list[list[str]]:
    raw = wire.replace("\\r\\n", "\r\n").encode("latin-1")
    traces: list[list[str]] = []
    i = 0
    while i < len(raw):
        if raw[i:i + 1] != b"*":
            break
        end = raw.find(b"\r\n", i)
        if end < 0:
            break
        try:
            count = int(raw[i + 1:end])
        except ValueError:
            break
        i = end + 2
        tokens = []
        for _ in range(count):
            if raw[i:i + 1] != b"$":
                return traces
            end = raw.find(b"\r\n", i)
            if end < 0:
                return traces
            length = int(raw[i + 1:end])
            i = end + 2
            tokens.append(raw[i:i + length].decode("latin-1"))
            i += length + 2
        if tokens:
            traces.append(tokens)
    return traces


def categorize(conn: dict) -> dict:
    traces = list(itertools.chain.from_iterable(command_traces(chunk)
                                                for chunk in conn["client_writes"]))
    names = [t[0].lower() for t in traces]
    sub_channels: list[str] = []
    for t in traces:
        if t[0].lower() == "subscribe":
            sub_channels.extend(t[1:])
    is_sub = bool(sub_channels)
    is_cmd = any(n in COMMAND_NAMES for n in names) and not is_sub
    category = "subscription" if is_sub else ("command" if is_cmd else "hello-only")
    return {"index": conn["index"], "category": category,
            "names": names, "subscribed_channels": sub_channels,
            "channel_count": len(set(sub_channels)), "traces": traces}


def start_server(binary: pathlib.Path, config: pathlib.Path, http_port: int,
                 log_path: pathlib.Path, wait: float = 45.0) -> tuple[subprocess.Popen, bool, int | None]:
    log = log_path.open("wb")
    proc = subprocess.Popen([str(binary), "--dev", "--config", str(config),
                             "--port", str(http_port)],
                            stdout=log, stderr=subprocess.STDOUT, start_new_session=True)
    reached = False
    deadline = time.monotonic() + wait
    while time.monotonic() < deadline:
        if proc.poll() is not None:
            break
        try:
            with urllib.request.urlopen(f"http://127.0.0.1:{http_port}/", timeout=1) as reply:
                reached = reply.status == 200
        except OSError:
            time.sleep(0.1)
            continue
        if reached:
            break
    return proc, reached, proc.poll()


def stop_server(proc: subprocess.Popen) -> int | None:
    if proc.poll() is not None:
        return proc.poll()
    time.sleep(0.5)  # let subscriptions settle into the startup window
    try:
        os.killpg(proc.pid, signal.SIGINT)
    except OSError:
        pass
    try:
        return proc.wait(timeout=30)
    except subprocess.TimeoutExpired:
        try:
            os.killpg(proc.pid, signal.SIGKILL)
        except OSError:
            pass
        return proc.wait()


def evaluate(official: dict, rust: dict) -> dict:
    """The attribution this witness exists to bind. Each assertion is falsifiable:
    if the candidate ever multiplexed like Go, or Go dedicated like Rust, it fails.
    Raised AssertionError carries the item id so a failure names the gap it guards."""
    assert rust["connections"] > official["connections"], \
        (ITEM, "candidate no longer opens more connections", rust["connections"], official["connections"])
    assert rust["subscription_connections"] > official["subscription_connections"], \
        (ITEM, "candidate no longer dedicates more subscription connections",
         rust["subscription_connections"], official["subscription_connections"])
    assert official["distinct_channel_count"] > official["subscription_connections"], \
        (ITEM, "reference no longer multiplexes multiple channels onto fewer connections",
         official["distinct_channel_count"], official["subscription_connections"])
    assert rust["subscription_connections"] >= rust["distinct_channel_count"], \
        (ITEM, "candidate no longer dedicates one connection per subscribed channel",
         rust["subscription_connections"], rust["distinct_channel_count"])
    return {
        "reference": (f"{official['connections']} connections: "
                      f"{official['subscription_connections']} multiplexed subscription connection(s) "
                      f"carrying {official['distinct_channel_count']} channels, "
                      f"{official['by_category'].get('command', 0)} command, "
                      f"{official['by_category'].get('hello-only', 0)} hello-only"),
        "candidate": (f"{rust['connections']} connections: "
                      f"{rust['subscription_connections']} dedicated subscription connections "
                      f"for {rust['distinct_channel_count']} channels, "
                      f"{rust['by_category'].get('command', 0)} command, "
                      f"{rust['by_category'].get('hello-only', 0)} hello-only"),
        "cause": ("Rust dials one dedicated connection per psrpc subscription and one command "
                  "connection per consumer; Go multiplexes all subscriptions over one PubSub "
                  "connection and shares one on-demand pool. Resource shape, not a functional "
                  "startup difference: both reach HTTP 200."),
    }


def resp_array(*tokens: str) -> str:
    """One RESP command array as the tap stores it (\\r\\n escaped)."""
    out = f"*{len(tokens)}\\r\\n"
    for tok in tokens:
        out += f"${len(tok)}\\r\\n{tok}\\r\\n"
    return out


def synth_run(connections: int, sub_conns: int, channels: int,
              command: int = 0, hello: int = 0) -> dict:
    by_cat = {}
    if sub_conns:
        by_cat["subscription"] = sub_conns
    if command:
        by_cat["command"] = command
    if hello:
        by_cat["hello-only"] = hello
    return {"connections": connections, "subscription_connections": sub_conns,
            "distinct_channel_count": channels, "by_category": by_cat}


def self_test() -> int:
    """Falsifiability probe: prove categorize and every evaluate assertion can fail.

    No Redis, no binaries — synthetic RESP writes and run summaries only. Each
    negative control isolates one assertion so a vacuous gate cannot hide here."""
    probes: list[dict] = []

    def record(name: str, ok: bool, detail: str) -> None:
        probes.append({"probe": name, "ok": ok, "detail": detail})

    # categorize: a SUBSCRIBE write is a subscription, a pool command is command,
    # a bare handshake is hello-only. Channels are counted distinctly.
    sub = categorize({"index": 1, "client_writes": [
        resp_array("hello", "3"), resp_array("subscribe", "A|CLAIM", "A|RES")]})
    record("categorize_subscription", sub["category"] == "subscription" and sub["channel_count"] == 2,
           f"category={sub['category']} channels={sub['channel_count']}")
    cmd = categorize({"index": 2, "client_writes": [
        resp_array("hello", "3"), resp_array("ping"), resp_array("hvals", "nodes")]})
    record("categorize_command", cmd["category"] == "command", f"category={cmd['category']}")
    hello = categorize({"index": 3, "client_writes": [resp_array("hello", "3")]})
    record("categorize_hello_only", hello["category"] == "hello-only", f"category={hello['category']}")
    mixed = categorize({"index": 4, "client_writes": [
        resp_array("subscribe", "A"), resp_array("ping")]})
    record("categorize_subscribe_wins_over_command", mixed["category"] == "subscription",
           f"category={mixed['category']}")

    # Positive control: the real observed shape (Go multiplex 63->1, Rust dedicate 35->35).
    official = synth_run(2, 1, 63, command=1)
    rust = synth_run(45, 35, 35, command=4, hello=6)
    try:
        evaluate(official, rust)
        record("positive_observed_shape", True, "evaluate accepted the multiplex-vs-dedicate shape")
    except AssertionError as exc:
        record("positive_observed_shape", False, f"evaluate wrongly rejected: {exc}")

    # Negative controls — each must raise. One per assertion, plus inverted/equal/zero.
    negatives = [
        ("inverted_shape", synth_run(45, 35, 35, command=4), synth_run(2, 1, 63, command=1),
         "candidate multiplexes, reference dedicates"),
        ("equal_no_divergence", synth_run(2, 1, 63, command=1), synth_run(2, 1, 63, command=1),
         "both sides identical"),
        ("zero_subscriptions", synth_run(2, 0, 0, command=2), synth_run(45, 0, 0, command=45),
         "neither side subscribed (the broken-tap shape)"),
        ("candidate_multiplexes_like_go", synth_run(2, 1, 63, command=1), synth_run(3, 1, 63, command=2),
         "candidate opens more connections but still one subscription"),
        ("reference_does_not_multiplex", synth_run(64, 63, 63, command=1), synth_run(100, 70, 70, command=30),
         "reference dedicates one connection per channel too"),
        ("candidate_channels_exceed_connections", synth_run(2, 1, 63, command=1), synth_run(45, 35, 40, command=4),
         "candidate claims more channels than subscription connections"),
    ]
    for name, off, rs, why in negatives:
        try:
            evaluate(off, rs)
            record(name, False, f"evaluate wrongly ACCEPTED ({why}) — assertion is vacuous")
        except AssertionError:
            record(name, True, f"evaluate correctly rejected ({why})")

    ok = all(p["ok"] for p in probes)
    print(json.dumps({"status": "PASS_SELF_TEST" if ok else "FAIL_SELF_TEST",
                      "probes": probes}, indent=2))
    return 0 if ok else 1


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--official", type=pathlib.Path)
    ap.add_argument("--rust", type=pathlib.Path)
    ap.add_argument("--base-port", type=int, default=24460)
    ap.add_argument("--output", type=pathlib.Path)
    ap.add_argument("--self-test", action="store_true",
                    help="run the falsifiability probe (no Redis/binaries) and exit")
    args = ap.parse_args()
    if args.self_test:
        return self_test()
    if not (args.official and args.rust and args.output):
        ap.error("--official, --rust and --output are required unless --self-test is set")
    args.output.mkdir(parents=True, exist_ok=True)

    plain, tap_port = args.base_port, args.base_port + 1
    http = {"official": args.base_port + 2, "rust": args.base_port + 3}
    tcp = {"official": args.base_port + 4, "rust": args.base_port + 5}
    udp = {"official": args.base_port + 6, "rust": args.base_port + 7}
    for port in (plain, tap_port, *http.values(), *tcp.values(), *udp.values()):
        assert free(port), f"fixture port {port} is already in use"
    staging = pathlib.Path(tempfile.mkdtemp(prefix="conn-attribution-"))
    redis_proc = None
    tap = None
    report = {"status": "RUNNING", "item": ITEM, "tap_port": tap_port, "upstream": plain,
              "binaries": {}, "runs": {},
              "contract": {
                  "go_multiplex": "references/psrpc/pkg/bus/redisbus/redisbus.go:63-83 (one ps), :262 (ps.Subscribe batches all channels)",
                  "go_pool": "references/protocol-stable/redis/redis.go:156-161 (one UniversalClient + Ping); go-redis options.go:497 PoolSize=10*GOMAXPROCS, MinIdleConns=0 (on demand)",
                  "rust_dedicated": "crates/lk-compat/src/service_bus/redis_transport.rs:205-238 (endpoint.dial per subscription)",
                  "rust_per_consumer_command": "crates/lk-compat/src/redis_endpoint.rs:478-483,958-983 (SharedConnection::connect eager per consumer)",
              },
              "normalization": "none: every connection's client writes recorded verbatim, \\r\\n escaped; categories parsed from those bytes"}
    try:
        redis_proc = start_redis(plain, staging)
        tap = Tap(tap_port, ("127.0.0.1", plain))
        time.sleep(0.3)
        for label, binary in (("official", args.official.resolve()),
                              ("rust", args.rust.resolve())):
            config = staging / f"{label}.yaml"
            config.write_text(
                "keys: {devkey: secret}\nlogging: {level: error, json: true}\n"
                "bind_addresses: [127.0.0.1]\n"
                "rtc: {node_ip: 127.0.0.1, use_external_ip: false, "
                f"tcp_port: {tcp[label]}, udp_port: {udp[label]}, "
                "enable_loopback_candidate: true}\n"
                "turn: {enabled: false}\n"
                f'redis:\n  address: "127.0.0.1:{tap_port}"\n')
            config.chmod(0o600)
            tap.reset()
            time.sleep(0.2)
            proc, reached, early = start_server(binary, config, http[label],
                                                args.output / f"{label}.server.log")
            # Snapshot the startup window before draining; the drain adds UNSUBSCRIBE.
            # Let the asynchronous psrpc writeWorker / per-consumer subscriptions
            # settle so their SUBSCRIBE writes are on the wire before the snapshot.
            time.sleep(3.0)
            seen = tap.snapshot()
            cats = [categorize(c) for c in seen]
            exit_code = stop_server(proc)
            report["binaries"][label] = {"path": str(binary), "sha256": sha(binary.read_bytes())}
            by_cat: dict[str, int] = {}
            for c in cats:
                by_cat[c["category"]] = by_cat.get(c["category"], 0) + 1
            sub_conns = [c for c in cats if c["category"] == "subscription"]
            distinct_channels = sorted({ch for c in sub_conns for ch in c["subscribed_channels"]})
            report["runs"][label] = {
                "reached_http": reached, "exited_early": early, "exit": exit_code,
                "connections": len(seen), "by_category": by_cat,
                "subscription_connections": len(sub_conns),
                "distinct_subscribed_channels": distinct_channels,
                "distinct_channel_count": len(distinct_channels),
                "max_channels_per_subscription_connection": max((c["channel_count"] for c in sub_conns), default=0),
                "per_connection": cats,
            }
            assert reached, (label, "server never reached HTTP 200", early)
            assert len(seen) > 0, (label, "the tap saw no connection")

        official = report["runs"]["official"]
        rust = report["runs"]["rust"]
        report["attribution"] = evaluate(official, rust)
        report["status"] = "PASS_SERVER_START_CONNECTION_ATTRIBUTION"
    except BaseException as exc:
        report["status"] = "FAIL"
        report["failure"] = f"{type(exc).__name__}: {exc}"
        return 1
    finally:
        if tap is not None:
            tap.close()
        if redis_proc is not None and redis_proc.poll() is None:
            redis_proc.terminate()
            try:
                redis_proc.wait(timeout=15)
            except subprocess.TimeoutExpired:
                redis_proc.kill()
        shutil.rmtree(staging, ignore_errors=True)
        (args.output / "attribution.json").write_text(json.dumps(report, indent=2) + "\n")
        print(json.dumps({"status": report["status"], "failure": report.get("failure"),
                          "official_connections": report["runs"].get("official", {}).get("connections"),
                          "rust_connections": report["runs"].get("rust", {}).get("connections"),
                          "official_by_category": report["runs"].get("official", {}).get("by_category"),
                          "rust_by_category": report["runs"].get("rust", {}).get("by_category")}))


if __name__ == "__main__":
    raise SystemExit(main())