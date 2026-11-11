#!/usr/bin/env python3
"""SPEC-0074 C12 / SPEC-0076 workline B: the server-start dead-node sweep, paired.

Upstream prunes the `nodes` hash inside `InitializeServer`
(`references/livekit-stable/pkg/service/server.go:197` calls
`router.RemoveDeadNodes()`, `pkg/routing/redisrouter.go:85-98`), which HDELs every
record `selector.IsAvailable` rejects (`pkg/routing/selector/utils.go:30-41`:
`AvailableSeconds = 5`, nil stats available, a future timestamp available, node state
NOT consulted). It runs on the client `GetRedisClient` built at startup, so on a
Sentinel deployment it prunes the resolved master and on a Cluster deployment the slot
owner — which is what makes this a topology consumer and not a CLI reader.

Every vector therefore seeds the keyspace itself, starts the server, snapshots the
hash WHILE the server runs (so a sweep that only happened at shutdown would show up),
stops it and snapshots again. Both binaries are run on identical config bytes; the
official one first, so the reference behaviour is asserted rather than assumed.

The stop is a SIGINT 0.5s after the first HTTP 200, not immediately: upstream flips its
own `running` flag 100ms *after* the listeners answer, and `Stop()` returns without
closing `doneChan` when the flag was never set, so an interrupt inside that window leaves
the reference hung forever (measured; recorded as
`RUNTIME:Server.InterruptBeforeRunningRace`).

Not covered here: node *registration* and the stats heartbeat that keeps a live node
inside the window (`RUNTIME:Redis.NodeRegistrationLifecycle`, opened by this run), the
prune's error-message wording (`CONFIG_BEHAVIOR:Redis.SentinelDiscoveryDiagnostics`
tracks the library-diagnostic family), Redis-over-TLS on this path (certified for the
same startup client by tests/interop/cli-redis-startup-tls-topology), and the full
multi-node room routing.
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
import tempfile
import time
import urllib.request

OFFICIAL_VERSION = "livekit-server version 1.13.10"
PRUNE_ITEM = "CONFIG_BEHAVIOR:Redis.StartupDeadNodePrune"
REG_ITEM = "RUNTIME:Redis.NodeRegistrationLifecycle"
ADDR = "YAML:redis.address"
SENTINEL_ADDRS = "YAML:redis.sentinel_addresses"
SENTINEL_NAME = "YAML:redis.sentinel_master_name"
CLUSTER_ADDRS = "YAML:redis.cluster_addresses"
PRUNE_TEXT_ITEM = "CONFIG_BEHAVIOR:Redis.NodePruneFailureText"
STARTUP_RACE_ITEM = "RUNTIME:Server.InterruptBeforeRunningRace"
# Measured upstream window: `Start()` sleeps 100ms after the listeners answer, then sets
# `running`; an interrupt that lands earlier makes `Stop()` return without closing
# `doneChan`. 0.5s clears it with margin on this host.
SETTLE_SECONDS = 0.5
ITEMS = sorted({PRUNE_ITEM, REG_ITEM, ADDR, SENTINEL_ADDRS, SENTINEL_NAME,
                CLUSTER_ADDRS, PRUNE_TEXT_ITEM, STARTUP_RACE_ITEM})
NODES_KEY = b"nodes"


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


def node_proto(node_id: str, updated_at=None, state: int = 1) -> bytes:
    """`livekit.Node` (`protobufs/livekit_internal.proto:49-56`): id=1, ip=2,
    num_cpus=3, stats=4, type=5, state=6, region=7. `updated_at=None` omits the whole
    stats message, which upstream treats as available until stats exist."""
    body = sfield(1, node_id) + sfield(2, "127.0.0.1") + vfield(3, 8) + vfield(5, 0)
    if updated_at is not None:
        stats = vfield(1, updated_at) + vfield(2, updated_at) + vfield(3, 1) + vfield(4, 2)
        body += field(4, 2, varint(len(stats)) + stats)
    return body + vfield(6, state) + sfield(7, "prune-21p")


class Resp:
    def __init__(self, host: str, port: int):
        self.sock = socket.create_connection((host, port), timeout=5)
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


def stop_process(proc: subprocess.Popen, grace: float = 20.0) -> None:
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


def sentinel_server(port: int, master_port: int, staging: pathlib.Path) -> subprocess.Popen:
    conf = staging / f"sentinel-{port}.conf"
    conf.write_text(
        f"port {port}\nbind 127.0.0.1\nprotected-mode no\ndir \"{staging}\"\n"
        f"sentinel monitor mymaster 127.0.0.1 {master_port} 1\n"
        "sentinel down-after-milliseconds mymaster 1000\n"
        "sentinel parallel-syncs mymaster 1\n"
        "sentinel failover-timeout mymaster 5000\n")
    proc = start_process(["redis-server", str(conf), "--sentinel"],
                         staging / f"sentinel-{port}.log")
    assert wait_port(port), f"sentinel never came up on {port}"
    return proc


def make_cluster(ports: list[int], staging: pathlib.Path) -> tuple[list[subprocess.Popen], str]:
    procs = [redis_server(port, staging, ["--cluster-enabled", "yes",
                                          "--cluster-config-file",
                                          str(staging / f"cluster-{port}.conf")])
             for port in ports]
    # Formed with plain RESP: `redis-cli --cluster create` aborts on this host with a
    # setsockopt error before touching the cluster bus. Same observable outcome: three
    # nodes, every slot covered, `cluster_state:ok`.
    conns = {port: Resp("127.0.0.1", port) for port in ports}
    for port in ports:
        for other in ports:
            if other != port:
                assert conns[port].call(b"CLUSTER", b"MEET", b"127.0.0.1",
                                        str(other).encode()) == b"OK", (port, other)
    time.sleep(1.0)
    for port, (lo, hi) in zip(ports, [(0, 5460), (5461, 10922), (10923, 16383)]):
        assert conns[port].call(b"CLUSTER", b"ADDSLOTS",
                                *(str(slot).encode() for slot in range(lo, hi + 1))) == b"OK"
    state = "unknown"
    for _ in range(30):
        states = []
        for port in ports:
            info = conns[port].call(b"CLUSTER", b"INFO").decode(errors="replace")
            match = re.search(r"cluster_state:(\w+)", info)
            states.append(match.group(1) if match else "unknown")
        state = "ok" if all(s == "ok" for s in states) else ",".join(states)
        if state == "ok":
            break
        time.sleep(1.0)
    for conn in conns.values():
        conn.close()
    return procs, state


def cluster_command(ports: list[int], *args: bytes) -> object:
    """A key-carrying command against the cluster, following one `-MOVED <slot> <host:port>`
    hop so the record lands on (and is read from) the node that owns the slot."""
    conn = Resp("127.0.0.1", ports[0])
    try:
        answer = conn.call(*args)
        if isinstance(answer, bytes) and answer.startswith(b"MOVED "):
            target = answer.split()[-1].decode().rsplit(":", 1)
            conn.close()
            conn = Resp("127.0.0.1", int(target[1]))
            answer = conn.call(*args)
        return answer
    finally:
        conn.close()


class Store:
    """The `nodes` hash of the deployment a vector targets, read the same way on both
    sides: through the address the topology resolves to, MOVED-aware for a cluster."""

    def __init__(self, ports: list[int]):
        self.ports = ports

    def call(self, *args: bytes):
        if len(self.ports) > 1:
            return cluster_command(self.ports, *args)
        conn = Resp("127.0.0.1", self.ports[0])
        try:
            return conn.call(*args)
        finally:
            conn.close()

    def field_names(self) -> list[str]:
        answer = self.call(b"HKEYS", NODES_KEY)
        return sorted(v.decode(errors="replace") for v in (answer or []))

    def seed(self, records: dict[str, bytes]) -> None:
        self.call(b"DEL", NODES_KEY)
        for field_name, value in records.items():
            assert self.call(b"HSET", NODES_KEY, field_name.encode(), value) == 1, \
                ("node record not written", field_name)
        assert sorted(self.field_names()) == sorted(records), \
            ("keyspace does not match what was seeded", self.field_names())


CLOCK_FIELD = re.compile(r'"(ts|time)":("(?:[^"\\]|\\.)*"|-?\d+(?:\.\d+)?(?:[eE][-+]?\d+)?)')


def mask_clock(line: str) -> str:
    """Replace only the *value* of zap's clock field, keeping the key name and the shape of
    the value. Two runs of two binaries can never share a wall clock, so the literal value
    carries no signal — masking this way still makes a key rename or an encoder change a
    visible divergence instead of hiding it."""
    def repl(match):
        key, value = match.group(1), match.group(2)
        shape = "string" if value[0] == '"' else "number"
        return '"%s":<%s>' % (key, shape)

    return CLOCK_FIELD.sub(repl, line)


def startup_lines(text: str) -> list[str]:
    """The `connecting to redis` mode lines, clock-masked. Which branch client the sweep
    ran on is observable here and nowhere else in this receipt: `sentinel` with the
    Sentinel address list and `masterName`, `cluster` with the seed list, or `simple`."""
    return [mask_clock(line.strip()) for line in text.splitlines()
            if "connecting to redis" in line]


def records(prefix: str, now: int) -> dict[str, bytes]:
    """The seeded keyspace: two records outside the window (one plain stale, one
    SHUTTING_DOWN so the sweep's state-agnosticism is tested) and three inside
    (fresh, future, stats-less)."""
    return {
        f"{prefix}-stale": node_proto(f"{prefix}-stale", now - 3600),
        f"{prefix}-draining-stale": node_proto(f"{prefix}-draining-stale", now - 3600, state=2),
        f"{prefix}-fresh": node_proto(f"{prefix}-fresh", now),
        f"{prefix}-future": node_proto(f"{prefix}-future", now + 60),
        f"{prefix}-nil-stats": node_proto(f"{prefix}-nil-stats", None),
    }


def keep_names(prefix: str) -> set[str]:
    return {f"{prefix}-fresh", f"{prefix}-future", f"{prefix}-nil-stats"}


def dead_names(prefix: str) -> set[str]:
    return {f"{prefix}-stale", f"{prefix}-draining-stale"}


def start_and_snapshot(binary: pathlib.Path, argv: list[str], log_path: pathlib.Path,
                       http_port: int, store: Store,
                       seeded: set[str], wait: float = 60.0) -> dict:
    """Run the start path, read the keyspace while the server holds it open, then stop."""
    proc = start_process([str(binary), *argv], log_path)
    reached_http = False
    status = None
    deadline = time.monotonic() + wait
    while time.monotonic() < deadline:
        if proc.poll() is not None:
            break
        try:
            with urllib.request.urlopen(f"http://127.0.0.1:{http_port}/", timeout=1) as reply:
                status = reply.status
                reached_http = reply.status == 200
        except OSError:
            time.sleep(0.1)
            continue
        if reached_http:
            break
    exited_early = proc.poll() is not None
    # The sweep is a startup step, so it has to be visible before the process is stopped.
    while_running = store.field_names()
    own_running = [name for name in while_running if name not in seeded]
    # Settle before interrupting. Upstream sets `running` 100ms after the listeners are
    # already answering (`server.go:343-347`), and `Stop()` returns early at
    # `if !s.running.Swap(false)` (`server.go:391`) without ever closing `doneChan`, so an
    # interrupt that lands inside that window leaves the reference hanging forever: with
    # the first-200-relative delays 0.00/0.03/0.08s all three official runs were still
    # alive at 20s and needed SIGKILL, while 0.12/0.20/0.50s all exited 0 within 0.01s (the
    # candidate exited 0 at every delay, including 0.00s — see the startup-race entry in
    # `known_differences`). Interrupting at the instant HTTP turns green would therefore
    # measure that race, not the graceful stop this step claims to observe.
    time.sleep(SETTLE_SECONDS)
    exit_code = None
    drain_seconds = None
    sigkilled = False
    if not exited_early:
        os.killpg(proc.pid, signal.SIGINT)
        drain_started = time.monotonic()
        try:
            exit_code = proc.wait(timeout=30)
        except subprocess.TimeoutExpired:
            sigkilled = True
            os.killpg(proc.pid, signal.SIGKILL)
            exit_code = proc.wait()
        drain_seconds = round(time.monotonic() - drain_started, 3)
    time.sleep(0.3)
    after_stop = store.field_names()
    text = log_path.read_bytes().decode("utf-8", "replace")
    return {
        "kind": "server-start", "exit": exit_code, "http_status": status,
        "reached_http": reached_http, "exited_early": exited_early,
        "sigkilled": sigkilled, "drain_seconds": drain_seconds,
        "keyspace_while_running": while_running, "keyspace_after_stop": after_stop,
        "seeded_kept_while_running": sorted(set(while_running) & seeded),
        "seeded_kept_after_stop": sorted(set(after_stop) & seeded),
        # A live node registered by the process itself, if any. Counted, never named:
        # the id is a random guid, so it cannot be compared literally.
        "own_node_entries_while_running": len(own_running),
        "own_node_entries_after_stop": len([n for n in after_stop if n not in seeded]),
        "redis_error_lines": [line for line in text.splitlines()
                              if "unable to connect to redis" in line
                              or "could not list nodes" in line
                              or "invalid wire-format" in line
                              or "cannot parse invalid" in line],
        "startup_mode_lines": startup_lines(text),
        "started_line": [line for line in text.splitlines()
                         if "starting LiveKit server" in line],
        "log_bytes": len(text.encode()),
    }


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--official", required=True, type=pathlib.Path)
    ap.add_argument("--rust", required=True, type=pathlib.Path)
    ap.add_argument("--output", required=True, type=pathlib.Path)
    ap.add_argument("--base-port", type=int, default=21850)
    args = ap.parse_args()

    official, rust = args.official.resolve(), args.rust.resolve()
    version = run_checked([str(official), "--version"]).stdout.strip()
    assert version == OFFICIAL_VERSION, ("locked reference binary required", version)
    args.output.mkdir(parents=True, exist_ok=True)
    staging = pathlib.Path(tempfile.mkdtemp(prefix="redis-prune-21p-"))

    base = args.base_port
    used = {name: base + offset for name, offset in (
        ("master", 0), ("sentinel", 1), ("c1", 2), ("c2", 3), ("c3", 4),
        ("go_http", 5), ("go_tcp", 6), ("go_udp", 7),
        ("rs_http", 8), ("rs_tcp", 9), ("rs_udp", 10))}
    for name, port in used.items():
        assert free(port), f"port {port} ({name}) is already in use"
    master_port, sentinel_port = used["master"], used["sentinel"]
    cluster_ports = [used["c1"], used["c2"], used["c3"]]

    report = {
        "status": "RUNNING",
        "items": ITEMS,
        "scope": (
            f"real paired server-start consumption of the upstream dead-node sweep "
            f"(`server.go:197` `router.RemoveDeadNodes`) for {PRUNE_ITEM}: the seeded "
            "`nodes` hash is pruned by the `selector.IsAvailable` 5s window, node state "
            "is not consulted, nil stats and a future timestamp survive, and the sweep "
            "runs on the same Redis client the startup Ping built — so the same "
            "assertions hold over a resolved Sentinel master and a Cluster slot owner. "
            "An unparsable record fails startup instead of being skipped. The graceful "
            "interrupt is deliberately delayed past upstream's 100ms post-listener "
            f"`running` window ({STARTUP_RACE_ITEM}). Since batch 21q both binaries also "
            "produce a record here, so this vector additionally pins that the sweep never "
            "consumes or clobbers the *live* self-registration: exactly one own entry while "
            "serving and none after the interrupt, on both sides, alongside the seeded "
            "records the sweep is about ({REG_ITEM}, recorded below). Not covered: the "
            "stats payload and heartbeat cadence inside that record, the exact wording of "
            f"the sweep's failure line ({PRUNE_TEXT_ITEM}, recorded below), Redis "
            "over TLS on this path (certified for the same startup client by tests/"
            "interop/cli-redis-startup-tls-topology), and cross-node room routing."),
        "binaries": {"official_go": {"path": str(official), "sha256": sha(official.read_bytes())},
                     "rust": {"path": str(rust), "sha256": sha(rust.read_bytes())}},
        # The candidate binary is itself named `livekit-server`, so the two sides cannot
        # be told apart from the `binaries` paths alone; name each digest with the side
        # it belongs to.
        "go_binary_sha256": sha(official.read_bytes()),
        "rust_binary_sha256": sha(rust.read_bytes()),
        "official_version": version,
        "normalization": (
            "exit status, whether SIGINT was honoured within the 30s window (`sigkilled`), "
            "`reached_http`, the surviving *seeded* records (read while the server runs and "
            "again after the graceful stop) and the sweep failure lines are compared "
            "exactly; `drain_seconds` (SIGINT to process exit) is recorded per side but not "
            "compared, since it is a timing. The process's own node entries are counted, "
            "not named — the id is a random guid — and the count is asserted against each "
            "side's documented behaviour (official registers at Start and HDELs on graceful "
            "stop, the candidate does neither yet). Raw logs and full keyspaces stay "
            "beside this receipt."),
        "cases": [],
        "mismatches": [],
    }

    def save() -> None:
        (args.output / "BATCH.json").write_text(json.dumps(report, indent=2) + "\n")

    procs: list[subprocess.Popen] = []
    code = 1
    try:
        procs.append(redis_server(master_port, staging))
        procs.append(sentinel_server(sentinel_port, master_port, staging))
        cluster_procs, cluster_state = make_cluster(cluster_ports, staging)
        procs.extend(cluster_procs)
        report["cluster_state"] = cluster_state
        assert cluster_state == "ok", ("cluster never formed", cluster_state)
        master_addr = f"127.0.0.1:{master_port}"
        cluster_list = ", ".join(f"\"127.0.0.1:{port}\"" for port in cluster_ports)

        # (id, redis yaml, store ports, expectation, ledger leaves this vector consumes)
        vectors = [
            ("prune-simple-address", f"  address: {master_addr}\n", [master_port], "success",
             [PRUNE_ITEM, ADDR]),
            ("prune-over-sentinel",
             (f"  address: {master_addr}\n"
              f"  sentinel_addresses: [\"127.0.0.1:{sentinel_port}\"]\n"
              "  sentinel_master_name: mymaster\n"), [master_port], "success",
             [PRUNE_ITEM, SENTINEL_ADDRS, SENTINEL_NAME]),
            ("prune-over-cluster", f"  cluster_addresses: [{cluster_list}]\n",
             cluster_ports, "success", [PRUNE_ITEM, CLUSTER_ADDRS]),
            ("prune-empty-nodes-hash", f"  address: {master_addr}\n", [master_port], "empty",
             [PRUNE_ITEM, ADDR]),
            ("prune-corrupt-record-aborts", f"  address: {master_addr}\n", [master_port],
             "corrupt", [PRUNE_ITEM, ADDR, PRUNE_TEXT_ITEM]),
            ("prune-skipped-without-redis", "", [master_port], "no-redis", [PRUNE_ITEM]),
        ]
        sentinel_probe = Resp("127.0.0.1", sentinel_port)
        resolved = sentinel_probe.call(b"SENTINEL", b"get-master-addr-by-name", b"mymaster")
        sentinel_probe.close()
        assert resolved == [b"127.0.0.1", str(master_port).encode()], \
            ("sentinel does not report the seeded master", resolved)

        for name, redis_yaml, store_ports, expectation, leaves in vectors:
            store = Store(store_ports)
            prefix = f"ND_{name.replace('-', '_')}"

            def run_side(binary: pathlib.Path, tag: str, http_port: int,
                         tcp_port: int, udp_port: int) -> dict:
                now = int(time.time())
                seeded: set[str] = set()
                if expectation == "success":
                    seeded = set(records(prefix, now))
                    store.seed(records(prefix, now))
                elif expectation == "corrupt":
                    seeded = {f"{prefix}-fresh", f"{prefix}-corrupt"}
                    store.seed({f"{prefix}-fresh": node_proto(f"{prefix}-fresh", now),
                                f"{prefix}-corrupt": b"not-a-protobuf"})
                elif expectation == "no-redis":
                    seeded = {f"{prefix}-stale"}
                    store.seed({f"{prefix}-stale": node_proto(f"{prefix}-stale", now - 3600)})
                else:  # empty
                    store.seed({})
                path = staging / f"{name}-{tag}.yaml"
                body = ("keys: {devkey: secret}\nlogging: {level: info, json: true}\n"
                        "bind_addresses: [127.0.0.1]\n"
                        "rtc: {node_ip: 127.0.0.1, use_external_ip: false, "
                        f"tcp_port: {tcp_port}, udp_port: {udp_port}, "
                        "enable_loopback_candidate: true}\n"
                        "turn: {enabled: false}\n"
                        + (f"redis:\n{redis_yaml}" if redis_yaml else ""))
                path.write_text(body)
                path.chmod(0o600)
                result = start_and_snapshot(
                    binary, ["--dev", "--config", str(path), "--port", str(http_port)],
                    args.output / f"{name}-{tag}.server.log", http_port, store, seeded)
                return {"config_body": body, "config_sha256": sha(path.read_bytes()),
                        "seeded": sorted(seeded), "result": result}

            go = run_side(official, "official", used["go_http"], used["go_tcp"], used["go_udp"])
            rs = run_side(rust, "rust", used["rs_http"], used["rs_tcp"], used["rs_udp"])
            normalize = lambda text, tcp, udp: (
                text.replace(f"tcp_port: {tcp}", "tcp_port: <T>")
                    .replace(f"udp_port: {udp}", "udp_port: <U>"))
            go_norm = normalize(go["config_body"], used["go_tcp"], used["go_udp"])
            rs_norm = normalize(rs["config_body"], used["rs_tcp"], used["rs_udp"])
            assert go_norm == rs_norm, (name, "both binaries must be fed the same config",
                                       go_norm, rs_norm)
            res_go, res_rs = go["result"], rs["result"]
            # Every vector that reaches the graceful interrupt contributes the startup-race
            # leaf too: that is where the settle-before-SIGINT rule is exercised.
            case_items = list(leaves)
            if expectation != "corrupt":
                case_items.append(STARTUP_RACE_ITEM)
            rec = {"id": name, "path": "server-start", "expectation": expectation,
                   "ledger_items": case_items, "config": go_norm,
                   "seeded": go["seeded"],
                   "config_sha256": {"official": go["config_sha256"],
                                     "rust": rs["config_sha256"]},
                   "official": res_go, "rust": res_rs}
            report["cases"].append(rec)

            # --- the reference behaviour is asserted on the official binary first ---
            if expectation in ("success", "empty", "no-redis"):
                assert res_go["reached_http"], (name, "official never served HTTP",
                                                res_go["redis_error_lines"],
                                                res_go["keyspace_while_running"])
                assert not res_go["sigkilled"], (name, "official ignored SIGINT for 30s",
                                                 res_go["drain_seconds"],
                                                 res_go["keyspace_after_stop"])
                assert res_go["exit"] == 0, (name, "official left uncleanly",
                                             res_go["exit"], res_go["redis_error_lines"],
                                             res_go["keyspace_while_running"])
            else:
                assert res_go["exit"] != 0, (name, "official started on a corrupt record",
                                             res_go["keyspace_while_running"])
                assert not res_go["reached_http"], (name, "official served HTTP anyway")
            if expectation == "success":
                assert set(res_go["seeded_kept_while_running"]) == keep_names(prefix), \
                    ("official sweep kept the wrong records", res_go["seeded_kept_while_running"])
                assert not (dead_names(prefix) & set(res_go["keyspace_while_running"])), \
                    ("official left a dead record behind", res_go["keyspace_while_running"])
                assert set(res_go["seeded_kept_after_stop"]) == keep_names(prefix), \
                    ("official keyspace changed at shutdown", res_go["keyspace_after_stop"])
                # Which branch client carried the sweep is only observable in the mode line.
                branch = ("sentinel" if SENTINEL_ADDRS in leaves else
                          "cluster" if CLUSTER_ADDRS in leaves else "simple")
                assert any(f'"{branch}":true' in line for line in res_go["startup_mode_lines"]), \
                    (name, f"official did not dial through the {branch} client",
                     res_go["startup_mode_lines"])
                assert res_go["startup_mode_lines"], (name, "no Redis mode line printed")
                assert res_go["own_node_entries_while_running"] == 1, \
                    ("official registers its own node at Start (server.go:222)",
                     res_go["keyspace_while_running"])
                assert res_go["own_node_entries_after_stop"] == 0, \
                    ("official unregisters on graceful stop (server.go:226)",
                     res_go["keyspace_after_stop"], res_go["exit"],
                     res_go["drain_seconds"])
                assert res_rs["own_node_entries_while_running"] == 1, \
                    (REG_ITEM, "the candidate must hold exactly its own record while it "
                     "serves, as `Start()` does at server.go:222",
                     res_rs["keyspace_while_running"])
                assert res_rs["own_node_entries_after_stop"] == 0, \
                    (REG_ITEM, "the candidate record survived a graceful stop; the deferred "
                     "UnregisterNode of server.go:226 has no Rust-side counterpart running",
                     res_rs["keyspace_after_stop"])
            elif expectation == "empty":
                assert res_go["seeded_kept_while_running"] == [], res_go["keyspace_while_running"]
                assert res_go["own_node_entries_while_running"] == 1, \
                    res_go["keyspace_while_running"]
                assert res_rs["own_node_entries_while_running"] == 1, \
                    (REG_ITEM, res_rs["keyspace_while_running"])
            elif expectation == "no-redis":
                # `redis.IsConfigured` is false, so no sweep — and no dial — happens.
                assert res_go["keyspace_while_running"] == [f"{prefix}-stale"], \
                    ("official touched the nodes hash without a Redis config",
                     res_go["keyspace_while_running"])

            # --- the two binaries ---
            fields = [("exit", res_go["exit"], res_rs["exit"]),
                      ("reached_http", res_go["reached_http"], res_rs["reached_http"]),
                      ("sigkilled", res_go["sigkilled"], res_rs["sigkilled"]),
                      ("own-entries-while-running",
                       res_go["own_node_entries_while_running"],
                       res_rs["own_node_entries_while_running"]),
                      ("own-entries-after-stop", res_go["own_node_entries_after_stop"],
                       res_rs["own_node_entries_after_stop"]),
                      ("seeded_kept_while_running", res_go["seeded_kept_while_running"],
                       res_rs["seeded_kept_while_running"]),
                      ("seeded_kept_after_stop", res_go["seeded_kept_after_stop"],
                       res_rs["seeded_kept_after_stop"]),
                      ("exited_early", res_go["exited_early"], res_rs["exited_early"]),
                      ("startup_mode_lines", res_go["startup_mode_lines"],
                       res_rs["startup_mode_lines"])]
            for label, left, right in fields:
                if left != right:
                    report["mismatches"].append([name, label, left, right])
            if expectation == "corrupt":
                # Both must refuse to start; the sentence they print is a separate,
                # open diagnostic-text item and is recorded per side, not compared.
                rec["official_error_lines"] = res_go["redis_error_lines"]
                rec["rust_error_lines"] = res_rs["redis_error_lines"]
                if bool(res_go["redis_error_lines"]) != bool(res_rs["redis_error_lines"]):
                    report["mismatches"].append(
                        [name, "a-sweep-failure-line-was-printed",
                         bool(res_go["redis_error_lines"]), bool(res_rs["redis_error_lines"])])
            if expectation == "success":
                assert res_rs["exit"] == 0, (name, "candidate failed to start",
                                             res_rs["redis_error_lines"])
                assert not res_rs["sigkilled"], (name, "candidate ignored SIGINT for 30s",
                                                 res_rs["drain_seconds"])
                assert res_rs["reached_http"], (name, "candidate never served HTTP")
                assert set(res_rs["seeded_kept_while_running"]) == keep_names(prefix), \
                    ("candidate sweep kept the wrong records",
                     res_rs["seeded_kept_while_running"], res_rs["keyspace_while_running"])
                assert not (dead_names(prefix) & set(res_rs["keyspace_while_running"])), \
                    ("candidate left a dead record behind", res_rs["keyspace_while_running"])
            if expectation == "no-redis":
                assert res_rs["keyspace_while_running"] == [f"{prefix}-stale"], \
                    ("candidate pruned without a Redis config", res_rs["keyspace_while_running"])
            rec["own_node_count"] = {"official": res_go["own_node_entries_while_running"],
                                     "rust": res_rs["own_node_entries_while_running"]}
            rec["graceful_stop"] = {
                "official": {"exit": res_go["exit"], "sigkilled": res_go["sigkilled"],
                             "drain_seconds": res_go["drain_seconds"],
                             "own_entries_after_stop": res_go["own_node_entries_after_stop"]},
                "rust": {"exit": res_rs["exit"], "sigkilled": res_rs["sigkilled"],
                         "drain_seconds": res_rs["drain_seconds"],
                         "own_entries_after_stop": res_rs["own_node_entries_after_stop"]}}
            save()

        success_vectors = [c["id"] for c in report["cases"]
                           if c["expectation"] in ("success", "empty", "no-redis")]
        assert len(success_vectors) >= 5, success_vectors
        by_leaf: dict[str, list[str]] = {}
        for case in report["cases"]:
            for leaf in case["ledger_items"]:
                by_leaf.setdefault(leaf, []).append(case["id"])
        report["ledger_item_vectors"] = by_leaf
        assert set(by_leaf) >= {PRUNE_ITEM, ADDR, SENTINEL_ADDRS, SENTINEL_NAME,
                                CLUSTER_ADDRS, STARTUP_RACE_ITEM}, sorted(by_leaf)
        assert len(by_leaf[PRUNE_ITEM]) == len(vectors), by_leaf[PRUNE_ITEM]
        assert len({c["id"] for c in report["cases"]}) == len(vectors), "duplicate case id"
        reg_difference = {
            "id": REG_ITEM,
            "scope_of_this_item": (
                "the producer side of the `nodes` hash on the start path: upstream "
                "`Start()` HSETs its own `livekit.Node` (`server.go:222` -> "
                "`redisrouter.go:69-78`) and defers `UnregisterNode` (`server.go:226`), "
                "and `keepaliveWorker` re-writes it with fresh stats on every ping "
                "(`redisrouter.go:229-252`), which is what keeps a live node inside the "
                "5s window the sweep above reads."),
            "difference": (
                "none on these two keyspace facts: both binaries hold exactly one "
                "self-named entry while they serve and none after the interrupt, on every "
                "Redis leaf this vector drives. HISTORY — at batch 21p the candidate held "
                "0 entries at both points because no server-start code wrote the local node "
                "at all; that receipt is kept unedited at "
                "evidence/2026-11-04/c26-redis-startup-dead-node-prune-paired, where this "
                "harness deliberately asserted the 0 so that the day a registration "
                "appeared it had to be re-scoped here rather than pass silently. Batch 21q "
                "wired `NodeRouter::start/stop` into the real start path, and this "
                "expectation moved to the reference contract."),
            "observed_in_this_run": [
                {"case": c["id"], "official_own_entries": c["own_node_count"]["official"],
                 "rust_own_entries": c["own_node_count"]["rust"],
                 "official_after_stop": c["graceful_stop"]["official"]["own_entries_after_stop"],
                 "rust_after_stop": c["graceful_stop"]["rust"]["own_entries_after_stop"]}
                for c in report["cases"] if c["expectation"] in ("success", "empty")],
            "handling": (
                "the start/stop pair this vector can observe now matches, and is asserted "
                "as a comparison rather than as a candidate-side zero. The item stays OPEN: "
                "its full scope — the stats payload riding in the record, the ping cadence "
                "and disconnect recovery — is wider than what a single-process pruning "
                "vector proves, and a matching count is not a matching record."),
        }
        report["known_differences"] = [reg_difference, {
            "id": PRUNE_TEXT_ITEM,
            "scope_of_this_item": (
                "the sentence printed when the startup sweep cannot read the node hash — "
                "either an unparsable record (`redisrouter.go:145-147` returns the raw "
                "`proto.Unmarshal` error) or a failed HVALS/HDEL. What the sweep *does* "
                "on failure (abort startup, never serve HTTP) is certified by "
                "`prune-corrupt-record-aborts`; only the wording is in question."),
            "difference": (
                "the reference prints protobuf-go's own decode sentence; the candidate "
                "prints the registry's `{context}: {source}` rendering of the same "
                "failure. The candidate additionally opens its own command connection at "
                "this step, so a dial-layer failure would surface with redis-rs' words "
                "where go-redis reuses its pool."),
            "observed_in_this_run": [
                {"case": c["id"], "official_lines": c["official_error_lines"],
                 "rust_lines": c["rust_error_lines"]}
                for c in report["cases"] if c["expectation"] == "corrupt"],
            "handling": (
                "the presence of a failure line and the exit status are compared on this "
                "vector, so a silent-success regression still fails here; the string "
                "itself stays open rather than being closed by pasting a Go-shaped "
                "message with no producer. This is deliberately not the same item as the "
                "go-redis library diagnostics c22 tracks."),
        }, {
            "id": STARTUP_RACE_ITEM,
            "scope_of_this_item": (
                "SIGINT landing while `Start()` is still in its post-listener sleep: "
                "`server.go:343-347` waits 100ms *after* the HTTP listeners answer before "
                "`s.running.Store(true)`, and `Stop()` (`server.go:375-399`) bails out at "
                "`if !s.running.Swap(false) { return }` without ever reaching "
                "`close(s.doneChan)`, so `Start()` keeps blocking on `<-s.doneChan`."),
            "difference": (
                "measured against the locked binary with the interrupt timed from the "
                "first HTTP 200: at 0.00s, 0.03s and 0.08s the official process was still "
                "alive at the 20s cap and had to be SIGKILLed (exit -9, its own `nodes` "
                "entry left behind), while at 0.12s, 0.20s and 0.50s it exited 0 within "
                "0.01s after HDELing its entry. The candidate exited 0 within 0.01s at "
                "every one of those six delays, including 0.00s — it does not reproduce "
                "the stall. This harness therefore sleeps `SETTLE_SECONDS` (0.5s) between "
                "the first 200 and the interrupt, so the graceful-stop assertions observe "
                "the unregister path rather than the race; without that settle the "
                "reference side fails 30s later for a reason this batch does not claim to "
                "port."),
            "observed_in_this_run": [
                {"case": c["id"], "settle_seconds": SETTLE_SECONDS,
                 "official": c["graceful_stop"]["official"],
                 "rust": c["graceful_stop"]["rust"]}
                for c in report["cases"] if c["expectation"] != "corrupt"],
            "handling": (
                "recorded, not closed. Whether to mirror an upstream hang is a product "
                "decision, not a compatibility one: the candidate's clean exit is strictly "
                "more useful, and no official API/CLI output depends on the hang. The item "
                "stays OPEN so the difference is never mistaken for parity, and so a "
                "future signal-handling change is written against both behaviours."),
        }]
        assert not report["mismatches"], report["mismatches"]
        report["aggregate_status"] = "PASS_CLI_REDIS_STARTUP_DEAD_NODE_PRUNE_PAIRED"
        report["status"] = report["aggregate_status"]
        code = 0
    except BaseException as exc:
        report["status"] = "FAIL"
        report["failure"] = f"{type(exc).__name__}: {exc}"
        code = 1
    finally:
        for proc in procs:
            stop_process(proc)
        shutil.rmtree(staging, ignore_errors=True)
        save()
    print(json.dumps({"status": report["status"], "cases": len(report["cases"]),
                      "mismatches": [[m[0], m[1]] for m in report["mismatches"]],
                      "failure": report.get("failure")}, indent=2))
    return code


if __name__ == "__main__":
    raise SystemExit(main())
