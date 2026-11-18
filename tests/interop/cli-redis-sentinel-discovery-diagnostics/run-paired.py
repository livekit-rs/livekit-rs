#!/usr/bin/env python3
"""Paired Go-vs-Rust acceptance of go-redis' Sentinel **discovery diagnostics**
(CONFIG_BEHAVIOR:Redis.SentinelDiscoveryDiagnostics, SPEC-0077 batch C42).

The official binary resolves its Redis master through a Sentinel quorum using
go-redis v9.22.0's `NewFailoverClient`. That client prints its own `redis:`-prefixed
diagnostic lines to stderr while it discovers peers, selects a master and reacts to
`+switch-master` pubsub events. This harness certifies that the Rust port emits the
**same lines, byte for byte, in the same order**, after the wall-clock stamp is removed.

Unlike the start-path TLS/topology harness (cli-redis-startup-tls-topology), which
*diverted* the diagnostics surface to an open known-difference, this harness compares
diagnostics like every other surface: a divergence is a mismatch, not a waiver. Each
vector first asserts the **official reference** really emits the expected lines (so a
pass can never be vacuous — an empty or wrong reference fails the vector), then asserts
the Rust candidate's diagnostics equal the official's exactly.

Upstream contract, from the locked sources
(references/go-modules/go-redis-v9.22.0/sentinel.go):
  :961-1052  MasterAddr — cold race winner runs `once.Do`: swap the winning sentinel to
             index 0, `setSentinel` (which calls `discoverSentinels`), then print
             `sentinel.go:1032: sentinel: selected addr=<sentinel> masterAddr=<master>`.
             A failed per-sentinel query prints `sentinel.go:1020` (not driven here: every
             configured sentinel is reachable in these vectors).
  :1201-1210 setSentinel — calls discoverSentinels **once**, then Subscribe
             ("+switch-master","+replica-reconf-done") and `go listen`. `listen` never
             re-queries SENTINELS, so discoverSentinels is the sole appender to the peer
             list and the sole source of the `discovered` line.
  :1212-1236 discoverSentinels — `Sentinels(ctx, MasterName)`; on error print
             `sentinel.go:1215: sentinel: Sentinels master=%q failed: %s` and return.
             Per row, ip and port must both be present and non-empty; the peer address is
             `net.JoinHostPort(ip, port)` on the **string** port (no numeric parse). A peer
             not already in the list prints
             `sentinel.go:1230: sentinel: discovered new sentinel=%q for master=%q`.
  :1238-1261 listen — on `+switch-master`, split the payload; if parts[0] != MasterName
             print `sentinel.go:1250: sentinel: ignore addr for master=%q` and continue;
             otherwise trySwitchMaster, which prints
             `sentinel.go:1194: sentinel: new master=%q addr=%q`.

Vectors (both binaries; same config bytes apart from the ports they must differ in):
  stable-single-sentinel      list-nodes, 1 sentinel, no peers   -> 1032 + 1194
  discover-peers-subset       list-nodes, 3 sentinels, cfg=[S1]  -> 1230 x2 + 1032 + 1194
  sentinels-acl-denied        list-nodes, SENTINELS ACL-denied   -> 1215 + 1032 + 1194
  foreign-master-switch       server-start, real othermaster
                              failover while subscribed          -> 1032 + 1194 + 1250

Topology sharing: `discover-peers-subset` runs both sides against ONE live set of
sentinels, because the SENTINELS reply order is session-stable but not fixed across
fresh topologies — sharing it is what makes the two `discovered` lines comparable in
order. `foreign-master-switch` builds a FRESH topology per side (a real failover can
only happen once per topology), on identical ports, so its lines are port-deterministic.
The single-sentinel vectors are port-deterministic and run sequentially per side.

Not covered here (honestly out of scope, tracked separately, never silently dropped):
  - `Replicas` diagnostics (sentinel.go:1066/1084/1113/1146): livekit's client is
    NewFailoverClient, which never calls replicaAddrs, so those lines are not observable
    on this surface.
  - pool-creation panics (sentinel.go:584/588/615/719/723): an upstream crash path; a
    product decision (R03), not a compatibility line to reproduce.
  - internal DialerRetries/RetryTimeout/RetryBackoff (options.go:97-109): option field
    definitions, not a logged surface.

Usage:
  python3 run-paired.py --official /abs/livekit-server-go --rust /abs/livekit-server \
      --output DIR [--base-port 23450]
"""
from __future__ import annotations

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
import tempfile
import time
import urllib.request

DIAG_ITEM = "CONFIG_BEHAVIOR:Redis.SentinelDiscoveryDiagnostics"
NODE_ID = "owned-c42-node"
SETTLE_SECONDS = 1.0  # let the +switch-master subscription establish before the failover

STAMP = re.compile(r"^redis: \d{4}/\d{2}/\d{2} \d{2}:\d{2}:\d{2} ")
CLOCK_FIELD = re.compile(r'"(ts|time)":("(?:[^"\\]|\\.)*"|-?\d+(?:\.\d+)?(?:[eE][-+]?\d+)?)')
DISCOVERED_ADDR = re.compile(r'discovered new sentinel="([^"]+)"')
NOPERM_PREFIX = 'sentinel.go:1215: sentinel: Sentinels master="mymaster" failed: NOPERM'


def sha(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def varint(value: int) -> bytes:
    out = bytearray()
    while True:
        bits = value & 0x7F
        value >>= 7
        if value:
            out.append(bits | 0x80)
        else:
            out.append(bits)
            return bytes(out)


def field(number: int, wire: int, payload: bytes) -> bytes:
    return varint((number << 3) | wire) + payload


def vfield(number: int, value: int) -> bytes:
    return field(number, 0, varint(value))


def sfield(number: int, text: str) -> bytes:
    raw = text.encode()
    return field(number, 2, varint(len(raw)) + raw)


def ffield(number: int, value: float) -> bytes:
    import struct
    return field(number, 5, struct.pack("<f", value))


def node_proto(node_id: str) -> bytes:
    """Fixed-value livekit.Node so the rendered list-nodes table is byte-stable."""
    rate = (vfield(8, 1_048_576) + vfield(9, 2_097_152) + vfield(10, 3_000)
            + vfield(11, 4_000) + vfield(12, 5) + vfield(13, 6_000) + vfield(14, 7)
            + vfield(16, 8))
    stats = (vfield(1, 1_700_000_000) + vfield(2, 1_700_000_060) + vfield(3, 2)
             + vfield(4, 3) + vfield(5, 4) + vfield(6, 5) + vfield(7, 11_000)
             + vfield(8, 22_000) + vfield(9, 33) + vfield(10, 44) + vfield(11, 55)
             + vfield(17, 8) + ffield(18, 0.125) + ffield(19, 0.25) + ffield(20, 0.5)
             + ffield(21, 0.25) + vfield(22, 66) + vfield(23, 77) + vfield(28, 88)
             + vfield(29, 9) + vfield(34, 34_359_738_368) + vfield(35, 32_212_254_720)
             + field(50, 2, varint(len(rate)) + rate))
    return (sfield(1, node_id) + sfield(2, "127.0.0.1") + vfield(3, 8)
            + field(4, 2, varint(len(stats)) + stats) + vfield(5, 0) + vfield(6, 1)
            + sfield(7, "owned-c42"))


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


def wait_free(port: int, timeout: float = 15.0) -> bool:
    end = time.monotonic() + timeout
    while time.monotonic() < end:
        if free(port):
            return True
        time.sleep(0.1)
    return False


def start_process(argv, log: pathlib.Path) -> subprocess.Popen:
    with log.open("wb") as handle:
        return subprocess.Popen(argv, stdout=handle, stderr=subprocess.STDOUT,
                                start_new_session=True)


def stop_process(proc: subprocess.Popen, grace: float = 15.0) -> None:
    if proc.poll() is not None:
        return
    try:
        os.killpg(proc.pid, signal.SIGTERM)
        proc.wait(timeout=grace)
    except (OSError, subprocess.TimeoutExpired):
        try:
            os.killpg(proc.pid, signal.SIGKILL)
            proc.wait(timeout=grace)
        except (OSError, subprocess.TimeoutExpired):
            pass


def redis_server(port: int, staging: pathlib.Path, extra: list[str] = ()) -> subprocess.Popen:
    home = staging / f"redis-{port}"
    home.mkdir(parents=True, exist_ok=True)
    argv = ["redis-server", "--port", str(port), "--bind", "127.0.0.1",
            "--protected-mode", "no", "--save", "", "--appendonly", "no",
            "--dir", str(home), *extra]
    proc = start_process(argv, staging / f"redis-{port}.log")
    assert wait_port(port), f"redis-server never came up on {port}"
    return proc


def sentinel_server(port: int, monitors: list[tuple[str, int]], staging: pathlib.Path,
                    acl: str | None = None) -> subprocess.Popen:
    """A real redis-sentinel monitoring one or more masters. `monitors` is a list of
    (master_name, master_port); quorum 1, down-after 1000ms, failover-timeout 5000ms.
    `acl`, when given, is a `user ...` line applied to the sentinel itself (used to deny
    the SENTINELS subcommand while leaving get-master-addr-by-name allowed)."""
    home = staging / f"sentinel-{port}"
    home.mkdir(parents=True, exist_ok=True)
    conf = home / "sentinel.conf"
    lines = [f"port {port}", "bind 127.0.0.1", "protected-mode no", f'dir "{home}"']
    for name, mport in monitors:
        lines.append(f"sentinel monitor {name} 127.0.0.1 {mport} 1")
        lines.append(f"sentinel down-after-milliseconds {name} 1000")
        lines.append(f"sentinel parallel-syncs {name} 1")
        lines.append(f"sentinel failover-timeout {name} 5000")
    if acl:
        lines.append(acl)
    conf.write_text("\n".join(lines) + "\n")
    proc = start_process(["redis-server", str(conf), "--sentinel"], home / "sentinel.log")
    assert wait_port(port), f"sentinel never came up on {port}"
    return proc


class Resp:
    def __init__(self, host: str, port: int):
        self.sock = socket.create_connection((host, port), timeout=5)
        self.file = self.sock.makefile("rb")
        assert self.call(b"PING") in (b"PONG", b"+PONG")

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
            count = int(data)
            return [self.read() for _ in range(count)]
        raise AssertionError("unexpected RESP type " + repr(tag))

    def call(self, *args):
        self.sock.sendall(b"*" + str(len(args)).encode() + b"\r\n"
                          + b"".join(b"$" + str(len(v)).encode() + b"\r\n" + v + b"\r\n"
                                     for v in args))
        return self.read()

    def close(self):
        self.file.close()
        self.sock.close()


def diagnostics(text: str) -> list[str]:
    """The go-redis `redis:` internal-logger lines, wall-clock stamp removed. This is the
    C42 surface: compared byte for byte, in order, between the two binaries."""
    return [STAMP.sub("", line) for line in text.splitlines() if STAMP.match(line)]


def mask_clock(line: str) -> str:
    def repl(match):
        key, value = match.group(1), match.group(2)
        shape = "string" if value[0] == '"' else "number"
        return '"%s":<%s>' % (key, shape)
    return CLOCK_FIELD.sub(repl, line)


def stderr_shape(text: str) -> list[str]:
    """The whole CLI stderr, clock-masked, with the go-redis `redis:` lines taken out
    (those are compared on their own `diagnostics` surface). Everything else — the
    `starting in development mode` line and the `connecting to redis` mode-log JSON —
    has to agree byte for byte, so the Sentinel branch choice stays guarded too."""
    return [mask_clock(line.strip()) for line in text.splitlines()
            if line.strip() and not STAMP.match(line)]


def seed_node(master_port: int) -> None:
    """Write exactly one fixed-value node record to the master the Sentinel reports, so
    the list-nodes table is byte-stable and non-empty (an empty table would make the
    stdout comparison vacuous)."""
    conn = Resp("127.0.0.1", master_port)
    conn.call(b"DEL", b"nodes")
    assert conn.call(b"HSET", b"nodes", NODE_ID.encode(), node_proto(NODE_ID)) == 1, \
        "node record not written"
    conn.close()


def sel_line(sentinel_port: int, master_port: int) -> str:
    return (f"sentinel.go:1032: sentinel: selected addr=127.0.0.1:{sentinel_port} "
            f"masterAddr=127.0.0.1:{master_port}")


def new_line(master_port: int) -> str:
    return f'sentinel.go:1194: sentinel: new master="mymaster" addr="127.0.0.1:{master_port}"'


def disc_line(addr: str) -> str:
    return f'sentinel.go:1230: sentinel: discovered new sentinel="{addr}" for master="mymaster"'


def ignore_line(name: str) -> str:
    return f'sentinel.go:1250: sentinel: ignore addr for master="{name}"'


def list_nodes_cfg(sentinel_ports: list[int], staging: pathlib.Path, tag: str) -> pathlib.Path:
    cfg = staging / f"{tag}-cfg.yaml"
    addrs = ", ".join(f'"127.0.0.1:{p}"' for p in sentinel_ports)
    cfg.write_text(
        "keys: {devkey: secret}\nlogging: {level: info, json: true}\n"
        "bind_addresses: [127.0.0.1]\n"
        "rtc: {node_ip: 127.0.0.1, use_external_ip: false}\n"
        "turn: {enabled: false}\n"
        f"redis:\n  sentinel_addresses: [{addrs}]\n"
        "  sentinel_master_name: mymaster\n")
    cfg.chmod(0o600)
    return cfg


def invoke_list_nodes(binary: pathlib.Path, cfg: pathlib.Path, out: pathlib.Path,
                      tag: str) -> dict:
    proc = subprocess.run([str(binary), "--dev", "--config", str(cfg), "list-nodes"],
                          stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=120)
    (out / f"{tag}.stdout").write_bytes(proc.stdout)
    (out / f"{tag}.stderr").write_bytes(proc.stderr)
    err_text = proc.stderr.decode("utf-8", "replace")
    return {"kind": "list-nodes", "exit": proc.returncode,
            "stdout_text": proc.stdout.decode("utf-8", "replace"),
            "stderr_text": err_text, "diagnostics": diagnostics(err_text),
            "stderr_shape": stderr_shape(err_text)}


def teardown(procs: list[subprocess.Popen], ports: list[int]) -> None:
    for proc in procs:
        stop_process(proc)
    for port in ports:
        wait_free(port, 15)


def wait_sentinel_peers(sentinel_port: int, want: int, timeout: float = 45.0) -> int:
    """Block until the sentinel has discovered `want` peer sentinels via __sentinel__:hello,
    so the SENTINELS reply both binaries read is the full, session-stable peer set."""
    end = time.monotonic() + timeout
    peers = 0
    while time.monotonic() < end:
        conn = Resp("127.0.0.1", sentinel_port)
        rows = conn.call(b"SENTINEL", b"SENTINELS", b"mymaster")
        conn.close()
        peers = len(rows) if isinstance(rows, list) else 0
        if peers >= want:
            return peers
        time.sleep(1.0)
    return peers


def wait_replica_known(sentinel_port: int, name: str, timeout: float = 30.0) -> bool:
    end = time.monotonic() + timeout
    while time.monotonic() < end:
        conn = Resp("127.0.0.1", sentinel_port)
        rows = conn.call(b"SENTINEL", b"REPLICAS", name.encode())
        conn.close()
        if isinstance(rows, list) and rows:
            return True
        time.sleep(0.5)
    return False


def run_foreign_side(binary: pathlib.Path, ports: dict, staging: pathlib.Path,
                     out: pathlib.Path, tag: str) -> dict:
    """Build a FRESH mymaster+othermaster topology, start the server subscribed to
    mymaster, kill othermaster's master so the sentinel promotes its replica and publishes
    `+switch-master othermaster ...`, and capture the resulting `ignore addr` line. A real
    failover (not a PUBLISH injection, which sentinel mode rejects) is the only stimulus
    that reaches listen()'s foreign-master branch."""
    m1, s1, m2, r2 = ports["m1"], ports["s1"], ports["m2"], ports["r2"]
    http, tcp, udp = ports["http"], ports["tcp"], ports["udp"]
    procs: list[subprocess.Popen] = []
    all_ports = [m1, s1, m2, r2, http, tcp, udp]
    for port in all_ports:
        assert free(port), f"{tag}: port {port} not free at start"
    log = out / f"{tag}.server.log"
    try:
        procs.append(redis_server(m1, staging))
        m2proc = redis_server(m2, staging)
        procs.append(m2proc)
        procs.append(redis_server(r2, staging, ["--replicaof", "127.0.0.1", str(m2)]))
        procs.append(sentinel_server(s1, [("mymaster", m1), ("othermaster", m2)], staging))
        assert wait_replica_known(s1, "othermaster"), f"{tag}: sentinel never learned othermaster's replica"
        cfg = staging / f"{tag}-foreign-cfg.yaml"
        cfg.write_text(
            "keys: {devkey: secret}\nlogging: {level: info, json: true}\n"
            "bind_addresses: [127.0.0.1]\n"
            f"rtc: {{node_ip: 127.0.0.1, use_external_ip: false, tcp_port: {tcp}, "
            f"udp_port: {udp}, enable_loopback_candidate: true}}\n"
            "turn: {enabled: false}\n"
            f'redis:\n  sentinel_addresses: ["127.0.0.1:{s1}"]\n'
            "  sentinel_master_name: mymaster\n")
        cfg.chmod(0o600)
        proc = start_process([str(binary), "--dev", "--config", str(cfg), "--port", str(http)], log)
        procs.append(proc)
        reached_http = False
        status = None
        deadline = time.monotonic() + 45
        while time.monotonic() < deadline:
            if proc.poll() is not None:
                break
            try:
                with urllib.request.urlopen(f"http://127.0.0.1:{http}/", timeout=1) as reply:
                    status = reply.status
                    reached_http = reply.status == 200
            except OSError:
                time.sleep(0.1)
                continue
            if reached_http:
                break
        time.sleep(SETTLE_SECONDS)  # subscription established
        stop_process(m2proc)        # trigger the foreign-master failover
        procs.remove(m2proc)
        promoted = False
        deadline = time.monotonic() + 30
        while time.monotonic() < deadline:
            conn = Resp("127.0.0.1", s1)
            addr = conn.call(b"SENTINEL", b"get-master-addr-by-name", b"othermaster")
            conn.close()
            if isinstance(addr, list) and str(r2).encode() in addr:
                promoted = True
                break
            time.sleep(0.5)
        time.sleep(2.0)  # let listen() receive +switch-master and print the ignore line
        exited_early = proc.poll() is not None
        exit_code = proc.poll()
        sigkilled = False
        if not exited_early:
            os.killpg(proc.pid, signal.SIGINT)
            try:
                exit_code = proc.wait(timeout=30)
            except subprocess.TimeoutExpired:
                sigkilled = True
                os.killpg(proc.pid, signal.SIGKILL)
                exit_code = proc.wait()
        procs.remove(proc)
        time.sleep(0.3)
        text = log.read_bytes().decode("utf-8", "replace")
        return {"kind": "server-start", "exit": exit_code, "http_status": status,
                "reached_http": reached_http, "exited_early": exited_early,
                "sigkilled": sigkilled, "promoted": promoted,
                "stderr_text": text, "diagnostics": diagnostics(text)}
    finally:
        teardown(procs, all_ports)


def assert_official_reference(name: str, diag: list[str], ports: dict) -> None:
    """Demand the official binary really emit the lines this vector exists to certify.
    A reference that is empty, short, reordered or worded differently fails here, so the
    paired equality below can never pass vacuously."""
    m1, s1 = ports["m1"], ports["s1"]
    if name == "stable-single-sentinel":
        assert diag == [sel_line(s1, m1), new_line(m1)], (name, "official reference", diag)
    elif name == "discover-peers-subset":
        s2, s3 = ports["s2"], ports["s3"]
        assert len(diag) == 4, (name, "official reference length", diag)
        assert diag[0].startswith("sentinel.go:1230:") and diag[1].startswith("sentinel.go:1230:"), \
            (name, "official must discover two peers first", diag)
        found = {DISCOVERED_ADDR.search(d).group(1) for d in diag[:2]}
        assert found == {f"127.0.0.1:{s2}", f"127.0.0.1:{s3}"}, (name, "official peer set", found)
        assert diag[2] == sel_line(s1, m1) and diag[3] == new_line(m1), \
            (name, "official select/new-master after discovery", diag)
    elif name == "sentinels-acl-denied":
        assert len(diag) == 3, (name, "official reference length", diag)
        assert diag[0].startswith(NOPERM_PREFIX), (name, "official Sentinels-failed NOPERM", diag)
        assert diag[1] == sel_line(s1, m1) and diag[2] == new_line(m1), \
            (name, "official select/new-master after Sentinels failure", diag)
    elif name == "foreign-master-switch":
        assert diag == [sel_line(s1, m1), new_line(m1), ignore_line("othermaster")], \
            (name, "official reference", diag)
    else:
        raise AssertionError("unknown vector " + name)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--official", required=True, type=pathlib.Path)
    ap.add_argument("--rust", required=True, type=pathlib.Path)
    ap.add_argument("--output", required=True, type=pathlib.Path)
    ap.add_argument("--base-port", type=int, default=23450)
    args = ap.parse_args()

    official, rust = args.official.resolve(), args.rust.resolve()
    args.output.mkdir(parents=True, exist_ok=True)
    staging = pathlib.Path(tempfile.mkdtemp(prefix="redis-sentinel-c42-"))

    base = args.base_port
    ports = {name: base + offset for name, offset in (
        ("m1", 0), ("s1", 1), ("s2", 2), ("s3", 3), ("m2", 4), ("r2", 5),
        ("go_http", 6), ("go_tcp", 7), ("go_udp", 8),
        ("rs_http", 9), ("rs_tcp", 10), ("rs_udp", 11))}

    report = {
        "status": "RUNNING",
        "items": [DIAG_ITEM],
        "scope": (
            "go-redis v9.22.0 NewFailoverClient's `redis:`-prefixed Sentinel discovery "
            "diagnostics on the list-nodes CLI path and the server start path: peer "
            "discovery (sentinel.go:1230), Sentinels-query failure (sentinel.go:1215), "
            "master selection (sentinel.go:1032), new-master (sentinel.go:1194) and the "
            "foreign-master ignore (sentinel.go:1250). Each vector asserts the official "
            "reference emits the expected lines, then compares the Rust candidate's "
            "diagnostics byte for byte and in order. Not covered: Replicas diagnostics "
            "(not reachable via NewFailoverClient), pool-creation panics (R03 upstream "
            "crash product decision), internal retry/backoff option fields (not a logged "
            "surface)."),
        "binaries": {
            "official_go": {"path": str(official), "sha256": sha(official.read_bytes())},
            "rust": {"path": str(rust), "sha256": sha(rust.read_bytes())},
        },
        "go_binary_sha256": sha(official.read_bytes()),
        "rust_binary_sha256": sha(rust.read_bytes()),
        "normalization": (
            "the `redis:` diagnostics are compared after removing only the wall-clock "
            "stamp (redis: YYYY/MM/DD HH:MM:SS), byte for byte and in order — never "
            "diverted to a known-difference. On list-nodes the whole stdout (fixed-value "
            "node table), the exit code and the clock-masked non-redis stderr shape are "
            "also compared. On server-start the survival, reached_http and graceful-stop "
            "exit are compared; the full server log is not byte-comparable (timestamps, "
            "node ids, bound ports) and stays raw beside the receipt."),
        "ports": ports,
        "cases": [],
    }
    receipt = args.output / "BATCH.json"
    mismatches: list[tuple] = []

    def save():
        receipt.write_text(json.dumps(report, indent=2) + "\n")

    save()
    code = 0
    try:
        version = subprocess.run([str(official), "--version"], stdout=subprocess.PIPE,
                                 stderr=subprocess.PIPE, timeout=60)
        report["official_version"] = version.stdout.decode("utf-8", "replace").strip()
        assert report["official_version"] == "livekit-server version 1.13.10", \
            report["official_version"]
        save()

        # --- vector 1: stable-single-sentinel (list-nodes, no peers) ---
        name = "stable-single-sentinel"
        vports = [ports["m1"], ports["s1"]]
        for port in vports:
            assert free(port), f"port {port} in use"
        procs = [redis_server(ports["m1"], staging),
                 sentinel_server(ports["s1"], [("mymaster", ports["m1"])], staging)]
        try:
            seed_node(ports["m1"])
            cfg = list_nodes_cfg([ports["s1"]], staging, name)
            go = invoke_list_nodes(official, cfg, args.output, f"{name}-official")
            rs = invoke_list_nodes(rust, cfg, args.output, f"{name}-rust")
            assert_official_reference(name, go["diagnostics"], ports)
            rec = {"id": name, "path": "list-nodes", "ledger_items": [DIAG_ITEM],
                   "expected": [sel_line(ports["s1"], ports["m1"]), new_line(ports["m1"])],
                   "official": go, "rust": rs}
            for surface in ("diagnostics", "stdout_text", "stderr_shape", "exit"):
                if go[surface] != rs[surface]:
                    mismatches.append((name, surface + " differ", go[surface], rs[surface]))
            report["cases"].append(rec)
            save()
        finally:
            teardown(procs, vports)

        # --- vector 2: discover-peers-subset (list-nodes, SHARED 3-sentinel topology) ---
        name = "discover-peers-subset"
        vports = [ports["m1"], ports["s1"], ports["s2"], ports["s3"]]
        for port in vports:
            assert free(port), f"port {port} in use"
        procs = [redis_server(ports["m1"], staging)]
        for sp in (ports["s1"], ports["s2"], ports["s3"]):
            procs.append(sentinel_server(sp, [("mymaster", ports["m1"])], staging))
        try:
            peers = wait_sentinel_peers(ports["s1"], 2)
            assert peers >= 2, ("sentinels never discovered each other", peers)
            report["discover_peers_known"] = peers
            seed_node(ports["m1"])
            # Configure ONLY S1 so discoverSentinels must report S2/S3 as new peers.
            cfg = list_nodes_cfg([ports["s1"]], staging, name)
            go = invoke_list_nodes(official, cfg, args.output, f"{name}-official")
            rs = invoke_list_nodes(rust, cfg, args.output, f"{name}-rust")
            assert_official_reference(name, go["diagnostics"], ports)
            rec = {"id": name, "path": "list-nodes", "ledger_items": [DIAG_ITEM],
                   "expected": "1230 x2 (S2,S3 in SENTINELS reply order) + 1032 + 1194",
                   "official": go, "rust": rs}
            for surface in ("diagnostics", "stdout_text", "stderr_shape", "exit"):
                if go[surface] != rs[surface]:
                    mismatches.append((name, surface + " differ", go[surface], rs[surface]))
            report["cases"].append(rec)
            save()
        finally:
            teardown(procs, vports)

        # --- vector 3: sentinels-acl-denied (list-nodes, SENTINELS denied) ---
        name = "sentinels-acl-denied"
        vports = [ports["m1"], ports["s1"]]
        for port in vports:
            assert free(port), f"port {port} in use"
        acl = "user default on nopass ~* &* +@all -sentinel|sentinels"
        procs = [redis_server(ports["m1"], staging),
                 sentinel_server(ports["s1"], [("mymaster", ports["m1"])], staging, acl=acl)]
        try:
            # Confirm the ACL really denies SENTINELS but still allows master resolution,
            # so the 1215 line is a genuine permission failure, not a dial failure.
            probe = Resp("127.0.0.1", ports["s1"])
            denied = probe.call(b"SENTINEL", b"SENTINELS", b"mymaster")
            resolved = probe.call(b"SENTINEL", b"get-master-addr-by-name", b"mymaster")
            probe.close()
            assert isinstance(denied, bytes) and denied.startswith(b"NOPERM"), \
                ("ACL did not deny SENTINELS", denied)
            assert resolved == [b"127.0.0.1", str(ports["m1"]).encode()], \
                ("ACL broke master resolution", resolved)
            seed_node(ports["m1"])
            cfg = list_nodes_cfg([ports["s1"]], staging, name)
            go = invoke_list_nodes(official, cfg, args.output, f"{name}-official")
            rs = invoke_list_nodes(rust, cfg, args.output, f"{name}-rust")
            assert_official_reference(name, go["diagnostics"], ports)
            rec = {"id": name, "path": "list-nodes", "ledger_items": [DIAG_ITEM],
                   "expected": "1215 (NOPERM) + 1032 + 1194", "official": go, "rust": rs}
            for surface in ("diagnostics", "stdout_text", "stderr_shape", "exit"):
                if go[surface] != rs[surface]:
                    mismatches.append((name, surface + " differ", go[surface], rs[surface]))
            report["cases"].append(rec)
            save()
        finally:
            teardown(procs, vports)

        # --- vector 4: foreign-master-switch (server-start, FRESH topology per side) ---
        name = "foreign-master-switch"
        go_ports = {"m1": ports["m1"], "s1": ports["s1"], "m2": ports["m2"],
                    "r2": ports["r2"], "http": ports["go_http"], "tcp": ports["go_tcp"],
                    "udp": ports["go_udp"]}
        rs_ports = {"m1": ports["m1"], "s1": ports["s1"], "m2": ports["m2"],
                    "r2": ports["r2"], "http": ports["rs_http"], "tcp": ports["rs_tcp"],
                    "udp": ports["rs_udp"]}
        go = run_foreign_side(official, go_ports, staging, args.output, f"{name}-official")
        rs = run_foreign_side(rust, rs_ports, staging, args.output, f"{name}-rust")
        assert go["promoted"] and rs["promoted"], ("othermaster never failed over", go, rs)
        assert_official_reference(name, go["diagnostics"], ports)
        rec = {"id": name, "path": "server-start", "ledger_items": [DIAG_ITEM],
               "expected": [sel_line(ports["s1"], ports["m1"]), new_line(ports["m1"]),
                            ignore_line("othermaster")],
               "official": go, "rust": rs}
        if go["diagnostics"] != rs["diagnostics"]:
            mismatches.append((name, "diagnostics differ", go["diagnostics"], rs["diagnostics"]))
        for surface in ("reached_http", "exited_early", "sigkilled"):
            if go[surface] != rs[surface]:
                mismatches.append((name, surface + " differ", go[surface], rs[surface]))
        if not go["exited_early"] and not rs["exited_early"] and go["exit"] != rs["exit"]:
            mismatches.append((name, "graceful stop exit differs", go["exit"], rs["exit"]))
        report["cases"].append(rec)
        save()

        report["mismatches"] = mismatches
        assert not mismatches, f"{len(mismatches)} paired divergence(s): {mismatches}"
        assert {c["id"] for c in report["cases"]} == {
            "stable-single-sentinel", "discover-peers-subset", "sentinels-acl-denied",
            "foreign-master-switch"}, "the vector set changed"
        # Every certified line must have been exercised by at least one vector's official
        # reference, so the receipt cannot pass while quietly skipping a diagnostic.
        exercised = set()
        for case in report["cases"]:
            for line in case["official"]["diagnostics"]:
                exercised.add(line.split(": ", 1)[0])  # the sentinel.go:<line> caller
        assert {"sentinel.go:1032", "sentinel.go:1194", "sentinel.go:1230",
                "sentinel.go:1215", "sentinel.go:1250"} <= exercised, sorted(exercised)
        report["exercised_callers"] = sorted(exercised)
        report["aggregate_status"] = "PASS_CLI_REDIS_SENTINEL_DISCOVERY_DIAGNOSTICS_PAIRED"
        report["status"] = report["aggregate_status"]
    except BaseException as exc:
        report["status"] = "FAIL"
        report["failure"] = f"{type(exc).__name__}: {exc}"
        code = 1
    finally:
        shutil.rmtree(staging, ignore_errors=True)
        save()
    print(json.dumps({"status": report["status"], "cases": len(report["cases"]),
                      "mismatches": [[m[0], m[1]] for m in mismatches],
                      "failure": report.get("failure")}, indent=2))
    return code


if __name__ == "__main__":
    raise SystemExit(main())