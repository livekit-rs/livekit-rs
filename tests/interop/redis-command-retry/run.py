#!/usr/bin/env python3
"""Fixed Go vs pooled Rust retry profile through a real Redis RESP wire."""
import argparse
from contextlib import contextmanager
import hashlib
import json
from pathlib import Path
import shutil
import socket
import socketserver
import subprocess
import tempfile
import threading
import time

CODE_PROFILES = {
    **{x.lower(): (x, 3, 4) for x in ("LOADING", "MASTERDOWN", "CLUSTERDOWN", "TRYAGAIN", "NOREPLICAS", "READONLY")},
    "tryagain-exhausted": ("TRYAGAIN", 4, 4),
    "ordinary-mentions-typed-code": ("ERR", 5, 1),
    "wrongtype": ("WRONGTYPE", 5, 1),
    "noperm": ("NOPERM", 5, 1),
    "positive-without-error": ("ERR", 0, 1),
}

def frame(stream):
    line = stream.readline()
    if not line:
        raise EOFError
    mark = line[:1]
    if mark in (b"$", b"!", b"="):
        size = int(line[1:])
        if size >= 0:
            body = stream.read(size + 2)
            if len(body) != size + 2:
                raise EOFError
            return line + body, body[:-2]
    if mark in (b"*", b"~", b">", b"%", b"|"):
        count = int(line[1:])
        data = []
        parts = []
        for _ in range(max(count, 0) * (2 if mark in (b"%", b"|") else 1)):
            raw, parsed = frame(stream)
            data.append(raw)
            parts.append(parsed)
        return line + b"".join(data), parts
    return line, line[1:-2]

@contextmanager
def owned_redis(output):
    redis_server = shutil.which("redis-server")
    redis_cli = shutil.which("redis-cli")
    if not redis_server or not redis_cli:
        raise RuntimeError("redis-server and redis-cli must be available")
    with tempfile.TemporaryDirectory(prefix="lk-redis-command-retry-") as temp:
        with socket.socket() as reservation:
            reservation.bind(("127.0.0.1", 0))
            port = reservation.getsockname()[1]
        with output.open("wb") as log:
            proc = subprocess.Popen(
                [redis_server, "--bind", "127.0.0.1", "--port", str(port),
                 "--save", "", "--appendonly", "no", "--dir", temp],
                stdout=log, stderr=subprocess.STDOUT,
            )
            cli = [redis_cli, "-h", "127.0.0.1", "-p", str(port), "--raw"]
            try:
                deadline = time.monotonic() + 5
                while True:
                    if proc.poll() is not None:
                        raise RuntimeError("owned Redis startup failed")
                    p = subprocess.run([*cli, "PING"], capture_output=True, timeout=2)
                    if p.stdout.strip() == b"PONG":
                        break
                    if time.monotonic() > deadline:
                        raise RuntimeError("owned Redis not ready")
                    time.sleep(.05)
                for key, value in (("retry_target", "actual-value"), ("recovery", "healthy")):
                    p = subprocess.run([*cli, "SET", key, value], capture_output=True, check=True, timeout=2)
                    assert p.stdout.strip() == b"OK"
                yield port
            finally:
                proc.terminate()
                try:
                    status = proc.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    proc.kill()
                    proc.wait()
                    raise
                assert status == 0

class Tap(socketserver.ThreadingTCPServer):
    daemon_threads = False
    allow_reuse_address = True

    def __init__(self, backend, code, faults):
        self.backend = backend
        self.code = code
        self.faults = faults
        self.events = []
        self.lock = threading.Lock()
        self.counter = 0
        super().__init__(("127.0.0.1", 0), Handler)

    def command(self, connection_id, key):
        with self.lock:
            fault = key == "retry_target" and self.faults > 0
            if fault:
                self.faults -= 1
            self.events.append({"key": key, "socket": connection_id, "fault": self.code if fault else None,
                                "monotonic_ns": time.monotonic_ns()})
            return fault

class Handler(socketserver.BaseRequestHandler):
    def handle(self):
        tap = self.server
        with tap.lock:
            tap.counter += 1
            connection_id = tap.counter
        with socket.create_connection(("127.0.0.1", tap.backend), timeout=6) as upstream:
            with self.request.makefile("rb") as client, upstream.makefile("rb") as server:
                self.request.settimeout(6)
                while True:
                    try:
                        raw, values = frame(client)
                    except (EOFError, ConnectionError):
                        return
                    assert isinstance(values, list)
                    fault = False
                    if values[0].upper() == b"GET":
                        key = values[1].decode()
                        fault = tap.command(connection_id, key)
                    if fault:
                        description = ("ordinary mentions TRYAGAIN LOADING" if tap.code == "ERR"
                                       else "injected before forwarding fixture GET")
                        reply = f"-{tap.code} {description}\r\n".encode()
                    else:
                        upstream.sendall(raw)
                        reply, _ = frame(server)
                    try:
                        self.request.sendall(reply)
                    except ConnectionError:
                        return

def one(binary, kind, code, failures, output):
    with owned_redis(output/"redis.log") as port, Tap(port, code, failures) as tap:
        worker = threading.Thread(target=tap.serve_forever)
        worker.start()
        try:
            address=f"127.0.0.1:{tap.server_address[1]}"
            arg=address if kind=="go" else "redis://"+address
            proc=subprocess.run([binary,arg], capture_output=True,timeout=12)
            (output/"stdout").write_bytes(proc.stdout)
            (output/"stderr").write_bytes(proc.stderr)
            parsed=[json.loads(line) for line in proc.stdout.splitlines() if line.startswith(b"{")]
            return {"exit":proc.returncode,"observed":parsed,"wire_events":tap.events}
        finally:
            tap.shutdown()
            worker.join(timeout=3)
            assert not worker.is_alive()

def verify(profile, row, expected_attempts):
    code, failures, _ = CODE_PROFILES[profile]
    assert row["exit"]==0
    events=row["wire_events"]
    target=[e for e in events if e["key"]=="retry_target"]
    recovery=[e for e in events if e["key"]=="recovery"]
    assert len(target)==expected_attempts and len(recovery)==1 and recovery[0]["fault"] is None
    assert all(e["fault"]==code for e in target[:failures])
    if len(target)>1:
        assert all((b["monotonic_ns"]-a["monotonic_ns"])>=8_000_000 for a,b in zip(target,target[1:]))
        if code=="READONLY":
            assert len({x["socket"] for x in target})==len(target)
        else:
            assert len({x["socket"] for x in target})==1
    assert row["observed"][1]=={"key":"recovery","value":"healthy"}
    expected=({"value":"actual-value"} if failures<expected_attempts else {"error":code})
    assert row["observed"][0]=={"key":"retry_target",**expected}

def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument("--go",type=Path,required=True)
    p.add_argument("--rust",type=Path,required=True)
    p.add_argument("--output",type=Path,required=True)
    p.add_argument("--expect",choices=("before","after"),required=True)
    args=p.parse_args()
    args.output.mkdir()
    results={"expect":args.expect, "rows":[]}
    for kind,binary in (("go",args.go),("rust",args.rust)):
        results[kind+"_sha256"]=hashlib.sha256(binary.read_bytes()).hexdigest()
        for name,(code,failures,expected) in CODE_PROFILES.items():
            out=args.output/(kind+"-"+name)
            out.mkdir()
            row=one(binary,kind,code,failures,out)
            row.update(kind=kind,profile=name)
            results["rows"].append(row)
            (args.output/"execution.json").write_text(json.dumps(results,indent=2)+"\n")
            attempts=expected
            if kind=="rust" and args.expect=="before" and expected>1:
                attempts=1
            verify(name,row,attempts)
    results["status"]="DIFFERENCE_REPRODUCED" if args.expect=="before" else "SCOPED_WIRE_MATCH"
    (args.output/"execution.json").write_text(json.dumps(results,indent=2)+"\n")

if __name__=="__main__":
    main()
