#!/usr/bin/env python3
"""Exercise actual slot moves and Sentinel promotions at Redis transaction boundaries."""
import argparse
from contextlib import ExitStack, contextmanager
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import socket
import socketserver
import ssl
import subprocess
import tempfile
import threading
import time

spec = importlib.util.spec_from_file_location("topologies", Path(__file__).with_name("run-topologies.py"))
top = importlib.util.module_from_spec(spec)
spec.loader.exec_module(top)
case, frame, cli = top.case, top.wire.frame, top.cli
MASTER = "transaction-fixture"
FIXTURES = Path(__file__).resolve().parents[3] / "crates/rtc-tls/tests/fixtures"
# Set by --tls: every client-to-tap socket (commands and Sentinel discovery) is a verified
# TLS server socket with the public RSA test chain; SNI names are recorded per run.
TLS = None
HANDSHAKES = []


def secure(sock):
    return TLS.wrap_socket(sock, server_side=True) if TLS is not None else sock
KEYS = (b"ingress", b"room_{ingress}:pppppppp", b"{ingress}_stream_key", b"{ingress}_state:IN_fixture")


class Tap(socketserver.ThreadingTCPServer):
    daemon_threads = False

    def __init__(self, backend, handler, label, events, translate, transition=None):
        self.backend, self.label, self.events = backend, label, events
        self.translate, self.transition = translate, transition
        self.serial = 0
        self.lock = threading.Lock()
        self.errors = []
        super().__init__(("127.0.0.1", 0), handler)

    def handle_error(self, request, client_address):
        import traceback
        self.errors.append(traceback.format_exc())

    def addr(self):
        return f"127.0.0.1:{self.server_address[1]}"


class Commands(socketserver.BaseRequestHandler):
    def handle(self):
        tap = self.server
        with tap.lock:
            tap.serial += 1
            serial = tap.serial
        try:
            self.request = secure(self.request)
        except (ssl.SSLError, OSError):
            return
        with socket.create_connection(("127.0.0.1", tap.backend), timeout=22) as peer:
            with self.request.makefile("rb") as client, peer.makefile("rb") as redis:
                while True:
                    try:
                        request, args = frame(client)
                    except (EOFError, ConnectionError):
                        return
                    op = args[0].upper()
                    hook = None
                    if tap.transition is not None and op == b"EXEC":
                        with tap.lock:
                            hook, tap.transition = tap.transition, None
                        if hook is not None and hook("before", b""):
                            return
                    try:
                        peer.sendall(request)
                        raw, value = frame(redis)
                    except (EOFError, ConnectionError):
                        return
                    if op in (b"ASKING", b"MULTI", b"SREM", b"HDEL", b"DEL", b"EXEC", b"HSET", b"SADD"):
                        tap.events.append({"node": tap.label, "socket": serial, "command": op.decode(),
                                           "reply": raw.decode()})
                    if hook is not None and hook("after", raw):
                        return
                    try:
                        self.request.sendall(tap.translate(args, raw, value))
                    except ConnectionError:
                        return


class Discovery(socketserver.BaseRequestHandler):
    # A Sentinel connection can also be a subscription. Translate live
    # +switch-master notifications in addition to the discovery response.
    def handle(self):
        tap = self.server
        try:
            self.request = secure(self.request)
        except (ssl.SSLError, OSError):
            return
        with socket.create_connection(("127.0.0.1", tap.backend), timeout=5) as peer:
            peer.settimeout(None)

            def relay_requests():
                try:
                    while data := self.request.recv(16384):
                        peer.sendall(data)
                except OSError:
                    pass
                finally:
                    try:
                        peer.shutdown(socket.SHUT_RDWR)
                    except OSError:
                        pass

            sender = threading.Thread(target=relay_requests)
            sender.start()
            try:
                with peer.makefile("rb") as stream:
                    while True:
                        raw, value = frame(stream)
                        self.request.sendall(tap.translate(None, raw, value))
            except (EOFError, ConnectionError):
                pass
            finally:
                try:
                    self.request.shutdown(socket.SHUT_RDWR)
                except OSError:
                    pass
                sender.join(timeout=3)
                assert not sender.is_alive()


@contextmanager
def observe(tap):
    with tap:
        thread = threading.Thread(target=tap.serve_forever, kwargs={"poll_interval": .05})
        thread.start()
        try:
            yield tap
        finally:
            tap.shutdown()
            thread.join(timeout=3)
            assert not thread.is_alive()
            assert not tap.errors, tap.errors


def require(port, *args):
    result = cli(port, *args)
    assert result in (b"OK", b"NOKEY"), (args, result)
    return result


def state(port):
    return {
        "entry": cli(port, "HGET", "ingress", "IN_fixture").decode(),
        "room": cli(port, "SISMEMBER", KEYS[1], "IN_fixture").decode(),
        "stream": cli(port, "HGET", KEYS[2], "stream").decode(),
        "state": cli(port, "GET", KEYS[3]).decode(),
    }


@contextmanager
def cluster(root, logs, mode, events, gate):
    used = set()
    ports = [top.port_pair(used) for _ in range(3)]
    with ExitStack() as stack:
        for i, port in enumerate(ports):
            stack.enter_context(top.broker(
                root / str(i), port, logs / f"broker-{i}.log", "--cluster-enabled", "yes",
                "--cluster-config-file", "nodes.conf", "--cluster-node-timeout", "1000",
                "--cluster-announce-ip", "127.0.0.1",
            ))
        subprocess.run(["redis-cli", "--cluster", "create", *(f"127.0.0.1:{p}" for p in ports),
                        "--cluster-replicas", "0", "--cluster-yes"],
                       capture_output=True, timeout=30, check=True)
        for port in ports:
            top.wait_for(lambda: b"cluster_state:ok" in cli(port, "CLUSTER", "INFO"))
        _, slots = case.command(ports[0], frame, b"CLUSTER", b"SLOTS")

        def route(key):
            slot = int(cli(ports[0], "CLUSTER", "KEYSLOT", key))
            return next(int(node[1]) for lo, hi, node, *_ in slots if int(lo) <= slot <= int(hi))

        old = route("ingress")
        new = next(p for p in ports if p != old)
        slot = str(int(cli(old, "CLUSTER", "KEYSLOT", "ingress")))
        ids = {p: cli(p, "CLUSTER", "MYID").decode() for p in ports}
        case.seed(old, frame, b"pppppppp", b"original")

        def move(finalize):
            require(new, "CLUSTER", "SETSLOT", slot, "IMPORTING", ids[old])
            require(old, "CLUSTER", "SETSLOT", slot, "MIGRATING", ids[new])
            require(old, "MIGRATE", "127.0.0.1", str(new), "", "0", "5000", "KEYS", *KEYS)
            if finalize:
                for p in (new, old, *(p for p in ports if p not in (old, new))):
                    require(p, "CLUSTER", "SETSLOT", slot, "NODE", ids[new])
            events.append({"transition": "slot-owner-changed" if finalize else "slot-importing",
                           "slot": int(slot), "source": "old", "target": "new"})

        def hook(phase, reply):
            if phase == "before" and mode != "lost":
                move(mode == "moved")
                gate.touch()
            if phase == "after" and mode == "lost":
                assert reply == b"*4\r\n:1\r\n:1\r\n:1\r\n:1\r\n"
                move(True)
                case.seed(new, frame, b"pppppppp", b"recreated")
                events.append({"transition": "recreated-on-new-owner"})
                gate.touch()
                return True
            return False

        def translate(args, raw, value):
            if args[:2] in ([b"CLUSTER", b"SLOTS"], [b"cluster", b"slots"]):
                return top.resp([[int(lo), int(hi), *[[b"127.0.0.1", taps[int(node[1])].server_address[1], node[2]]
                                                       for node in nodes]] for lo, hi, *nodes in value])
            if raw.startswith((b"-MOVED ", b"-ASK ")):
                code, number, address = raw.strip().split()
                port = int(address.rsplit(b":", 1)[1])
                return b" ".join([code, number, taps[port].addr().encode()]) + b"\r\n"
            return raw

        taps = {p: stack.enter_context(observe(Tap(
            p, Commands, "old" if p == old else "new" if p == new else "other",
            events, translate, hook if p == old else None,
        ))) for p in ports}
        def collect():
            if mode == "ask":
                for p in (new, old, *(p for p in ports if p not in (old, new))):
                    require(p, "CLUSTER", "SETSLOT", slot, "NODE", ids[new])
            return state(new), cli(route("egress"), "HEXISTS", "egress", "EG_recovery").decode()
        yield ",".join(taps[p].addr() for p in ports), collect


@contextmanager
def sentinel(root, logs, mode, events, gate):
    used = set()
    old, new, sent = (top.port_pair(used) for _ in range(3))
    with ExitStack() as stack:
        stack.enter_context(top.broker(root / "old", old, logs / "old.log", "--repl-diskless-sync-delay", "0"))
        stack.enter_context(top.broker(root / "new", new, logs / "new.log", "--replicaof", "127.0.0.1", str(old)))
        case.seed(old, frame, b"pppppppp", b"original")
        top.wait_for(lambda: cli(new, "HGET", "ingress", "IN_fixture") == b"original")
        directory = root / "sentinel"
        directory.mkdir()
        config = directory / "sentinel.conf"
        config.write_text(
            f"bind 127.0.0.1\nport {sent}\ndir {directory}\n"
            f"sentinel monitor {MASTER} 127.0.0.1 {old} 1\n"
            f"sentinel down-after-milliseconds {MASTER} 1000\n"
            f"sentinel failover-timeout {MASTER} 5000\n"
        )
        log = stack.enter_context((logs / "sentinel.log").open("wb"))
        proc = subprocess.Popen(["redis-server", config, "--sentinel"], stdout=log, stderr=subprocess.STDOUT)

        def stop():
            proc.terminate()
            assert proc.wait(timeout=7) == 0
        stack.callback(stop)
        top.wait_for(lambda: b"slave" in cli(sent, "SENTINEL", "REPLICAS", MASTER))

        def promote():
            require(sent, "SENTINEL", "FAILOVER", MASTER)
            top.wait_for(lambda: cli(sent, "SENTINEL", "get-master-addr-by-name", MASTER).endswith(str(new).encode()))
            assert cli(new, "ROLE").startswith(b"master\n")
            events.append({"transition": "promoted-replica", "new_role": "master"})

        def hook(phase, reply):
            if phase == "before" and mode == "before":
                events.append({"transition": "exec-withheld-before-promotion"})
                promote()
                # A real acknowledged promotion has occurred. This fixture
                # cuts the old transaction without submitting its EXEC.
                gate.touch()
                return True
            if phase == "after" and mode == "lost":
                assert reply == b"*4\r\n:1\r\n:1\r\n:1\r\n:1\r\n"
                top.wait_for(lambda: cli(new, "HEXISTS", "ingress", "IN_fixture") == b"0")
                promote()
                case.seed(new, frame, b"pppppppp", b"recreated")
                events.append({"transition": "recreated-on-promoted-primary"})
                gate.touch()
                return True
            return False

        taps = {p: stack.enter_context(observe(Tap(
            p, Commands, label, events, lambda _, raw, value: raw, hook if p == old else None,
        ))) for p, label in ((old, "old"), (new, "new"))}

        def translate(_, raw, value):
            if isinstance(value, list) and len(value) == 2 and value[0] == b"127.0.0.1":
                return top.resp([value[0], str(taps[int(value[1])].server_address[1]).encode()])
            if isinstance(value, list) and len(value) == 3 and value[:2] == [b"message", b"+switch-master"]:
                master, old_host, old_port, new_host, new_port = value[2].split()
                events.append({"transition": "observed-switch-master-notification"})
                return top.resp(value[:2] + [b" ".join([
                    master, old_host, str(taps[int(old_port)].server_address[1]).encode(),
                    new_host, str(taps[int(new_port)].server_address[1]).encode(),
                ])])
            return raw
        discovery = stack.enter_context(observe(Tap(sent, Discovery, "sentinel", events, translate)))
        yield discovery.addr(), lambda: (state(new), cli(new, "HEXISTS", "egress", "EG_recovery").decode())


def run(binary, kind, topology, mode, out):
    out.mkdir()
    events = []
    with tempfile.TemporaryDirectory(prefix="lk-redis-transition-brokers-") as tmp:
        gate = Path(tmp) / "transition-complete"
        with {"cluster": cluster, "sentinel": sentinel}[topology](Path(tmp), out, mode, events, gate) as (addr, collect):
            if topology == "cluster" and kind == "rust":
                scheme = "rediss://" if TLS is not None else "redis://"
                addr = ",".join(scheme + node for node in addr.split(","))
            extra = [str(FIXTURES / "expanded/root-rsa.pem")] if TLS is not None else []
            HANDSHAKES.clear()
            started = time.monotonic()
            child = subprocess.run([binary, addr, "8", topology, *extra], capture_output=True, timeout=40,
                                   env=dict(os.environ, LK_REDIS_RECOVERY_GATE=str(gate)))
            (out / "stdout").write_bytes(child.stdout)
            (out / "stderr").write_bytes(child.stderr)
            (out / "events.json").write_text(json.dumps(events, indent=2) + "\n")
            assert child.returncode == 0, (child.returncode, child.stderr)
            row = json.loads(child.stdout)
            final, recovery = collect()
            if TLS is not None:
                assert HANDSHAKES and set(HANDSHAKES) == {"localhost"}, HANDSHAKES
            return {**row, "store": final, "recovery_entry": recovery, "events": events,
                    "exit": child.returncode, "seconds": time.monotonic() - started,
                    "verified_sni": sorted(set(HANDSHAKES))}


def signature(row):
    return {k: row[k] for k in ("success", "recovery", "store", "recovery_entry")} | {
        "commands": [(e["node"], e["command"]) for e in row["events"]
                     if e.get("command") in ("ASKING", "MULTI", "EXEC")],
    }


def assert_oracle(profile, row):
    result = signature(row)
    assert result["recovery"] and result["recovery_entry"] == "1", row
    old = [("old", "MULTI"), ("old", "EXEC")]
    if profile in ("cluster-moved", "cluster-ask"):
        code = "-MOVED " if profile.endswith("moved") else "-ASK "
        assert any(e.get("node") == "old" and e.get("command") == "EXEC"
                   and e["reply"].startswith(code) for e in row["events"]), row
        redirect = [("new", "ASKING")] if code == "-ASK " else []
        assert result["commands"] == old + redirect + [("new", "MULTI"), ("new", "EXEC")], row
        assert result["success"] and result["store"] == {"entry": "", "room": "0", "stream": "", "state": ""}, row
    else:
        before = profile == "sentinel-before"
        assert not result["success"] and result["commands"] == (old[:1] if before else old), row
        value = "original" if before else "recreated"
        assert result["store"] == {"entry": value, "room": "1", "stream": "IN_fixture", "state": value}, row
    if profile.startswith("sentinel"):
        # Observable outcome: the recovery write lands on the promoted primary. Whether
        # the client learned of the promotion from the +switch-master notification or by
        # re-resolving the master after the failed transaction is an internal discovery
        # path; pinned Go has been observed to reach the new primary without consuming
        # the notification, so it is recorded (notification_observed) but not required.
        assert any(e.get("node") == "new" and e.get("command") == "HSET"
                   and e["reply"] == ":1\r\n" for e in row["events"]), row


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--go", type=Path, required=True)
    p.add_argument("--rust", type=Path)
    p.add_argument("--profile", action="append", choices=("cluster-moved", "cluster-ask", "cluster-lost", "sentinel-before", "sentinel-lost"))
    p.add_argument("--output", type=Path, required=True)
    p.add_argument("--tls", action="store_true", help="require a verified TLS socket to every tap")
    args = p.parse_args()
    if args.tls:
        global TLS
        TLS = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        TLS.load_cert_chain(FIXTURES / "expanded/rsa.pem", FIXTURES / "expanded/rsa-key.pem")
        TLS.set_servername_callback(lambda _, name, __: HANDSHAKES.append(name))
    args.output.mkdir()
    bins = [(kind, path.resolve()) for kind, path in (("go", args.go), ("rust", args.rust)) if path]
    report = {"tls": args.tls, "binaries": {k: hashlib.sha256(p.read_bytes()).hexdigest() for k, p in bins}, "profiles": {}}
    for profile in args.profile or ("cluster-moved", "cluster-ask", "cluster-lost", "sentinel-before", "sentinel-lost"):
        topology, mode = profile.split("-")
        rows = report["profiles"][profile] = {}
        for kind, path in bins:
            rows[kind] = run(path, kind, topology, mode, args.output / f"{profile}-{kind}")
            rows[kind]["notification_observed"] = any(
                e.get("transition") == "observed-switch-master-notification" for e in rows[kind]["events"])
            (args.output / "execution.json").write_text(json.dumps(report, indent=2) + "\n")
            print(profile, kind, signature(rows[kind]), rows[kind]["error"], flush=True)
        assert_oracle(profile, rows["go"])
        if args.rust:
            rows["match"] = signature(rows["go"]) == signature(rows["rust"])
            (args.output / "execution.json").write_text(json.dumps(report, indent=2) + "\n")
            if rows["match"]:
                assert_oracle(profile, rows["rust"])
    return int(args.rust is not None and not all(r["match"] for r in report["profiles"].values()))


if __name__ == "__main__":
    raise SystemExit(main())
