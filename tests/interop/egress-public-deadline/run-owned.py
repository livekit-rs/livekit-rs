#!/usr/bin/env python3
"""Public Egress deadline → actual claimed PSRPC worker context on owned Redis."""
import argparse
import base64
import hashlib
import hmac
import http.client
import json
import os
import signal
import socket
import subprocess
import time
from pathlib import Path

KEY = "devkey"
SECRET = "owned-egress-deadline-secret-0123456789"
HELD_URL = "http://127.0.0.1/owned-held"
POSITIVE_URL = "http://127.0.0.1/owned-positive"
HEADER_MS = 1600
NS_PER_MS = 1_000_000


def sha(path):
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def token():
    def encode(value):
        return base64.urlsafe_b64encode(json.dumps(value, separators=(",", ":")).encode()).rstrip(b"=")
    value = encode({"alg": "HS256", "typ": "JWT"}) + b"." + encode({
        "iss": KEY, "sub": "owned-egress-deadline", "exp": int(time.time()) + 300,
        "video": {"roomRecord": True},
    })
    return (value + b"." + base64.urlsafe_b64encode(
        hmac.digest(SECRET.encode(), value, "sha256")
    ).rstrip(b"=")).decode()


def owned_ports():
    reservations = []
    try:
        for kind in [socket.SOCK_STREAM] * 3 + [socket.SOCK_DGRAM]:
            sock = socket.socket(socket.AF_INET, kind)
            reservations.append(sock)
            sock.bind(("127.0.0.1", 0))
        return [sock.getsockname()[1] for sock in reservations]
    finally:
        for sock in reservations:
            sock.close()


def assert_free(ports):
    for port, kind in zip(ports, [socket.SOCK_STREAM] * 3 + [socket.SOCK_DGRAM]):
        with socket.socket(socket.AF_INET, kind) as sock:
            if kind == socket.SOCK_STREAM:
                sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            sock.bind(("127.0.0.1", port))


def wait(check, label, timeout=8):
    until = time.monotonic() + timeout
    while time.monotonic() < until:
        value = check()
        if value:
            return value
        time.sleep(.025)
    raise RuntimeError(label + " deadline")


def case(binary, label, args):
    output = args.output / label
    output.mkdir(mode=0o700)
    redis_port, http_port, tcp_port, udp_port = ports = owned_ports()
    config = output / "main.json"
    config.write_text(json.dumps({
        "port": http_port, "bind_addresses": ["127.0.0.1"],
        "keys": {KEY: SECRET}, "redis": {"address": f"127.0.0.1:{redis_port}"},
        "rtc": {"tcp_port": tcp_port, "udp_port": udp_port, "node_ip": "127.0.0.1",
                "use_external_ip": False, "enable_loopback_candidate": True,
                "ips": {"includes": ["127.0.0.1/32"]}},
        "turn": {"enabled": False}, "psrpc": {"compression": {"quality": 0}},
        "logging": {"level": "info"},
    }))
    result = {"server": label, "status": "RUNNING", "config_sha256": sha(config),
              "binaries": {}, "owned": {}}
    bins = {"main": binary, "observer": args.observer, "redis": args.redis.resolve(strict=True)}
    result["binaries"] = {name: {"path": str(path), "sha256": sha(path)} for name, path in bins.items()}
    processes = []
    env = {k: v for k, v in os.environ.items() if not k.startswith("LIVEKIT_")
           and k not in ("REDIS_HOST", "REDIS_PASSWORD")}
    events_path = output / "observer.jsonl"

    def launch(name, command, log_path):
        log = log_path.open("wb")
        proc = subprocess.Popen(command, env=env, stdout=log,
                                stderr=subprocess.STDOUT, stdin=subprocess.DEVNULL)
        processes.append((name, proc, log))
        result["owned"][name] = {"pid": proc.pid, "argv": [str(arg) for arg in command]}
        return proc

    def events():
        rows = []
        for line in events_path.read_text().splitlines():
            if line.startswith("{"):
                rows.append(json.loads(line))
        return rows

    def mode_events(mode, phase):
        return [row for row in events() if row.get("mode") == mode and row["phase"] == phase]

    def alive():
        assert all(proc.poll() is None for _, proc, _ in processes)

    def redis_ready():
        alive()
        try:
            with socket.create_connection(("127.0.0.1", redis_port), timeout=.5) as sock:
                sock.sendall(b"*1\r\n$4\r\nPING\r\n")
                return sock.recv(64) == b"+PONG\r\n"
        except OSError:
            return False

    def http_request(method=None, data=None, timeout_ms=None):
        connection = http.client.HTTPConnection("127.0.0.1", http_port, timeout=6)
        try:
            if method is None:
                connection.request("GET", "/")
            else:
                headers = {"Content-Type": "application/json", "Authorization": "Bearer " + token()}
                if timeout_ms is not None:
                    headers["X-Twirp-Timeout-Ms"] = str(timeout_ms)
                connection.request("POST", "/twirp/livekit.Egress/" + method, json.dumps(data), headers)
            response = connection.getresponse()
            body = response.read()
            return response.status, json.loads(body) if method else body.decode()
        finally:
            connection.close()

    def egress_id(value):
        fields = [value[name] for name in ("egressId", "egress_id") if name in value]
        assert len(fields) == 1 and fields[0].startswith("EG_"), value
        return fields[0]

    def http_ready():
        alive()
        try:
            return http_request()[0] == 200
        except OSError:
            return False

    try:
        launch("redis", [str(args.redis), "--bind", "127.0.0.1", "--port", str(redis_port),
                         "--save", "", "--appendonly", "no", "--dir", str(output)], output / "redis.log")
        wait(redis_ready, "owned Redis ready")
        launch("observer", [str(args.observer), f"127.0.0.1:{redis_port}"], events_path)
        wait(lambda: any(row["phase"] == "ready" for row in events()), "pinned generated worker ready")
        launch("main", [str(binary), "--config", str(config)], output / "main.log")
        wait(http_ready, "owned HTTP ready")
        # Go publishes its running flag after the listener is already active.
        time.sleep(.25)

        began = time.monotonic()
        status, error = http_request("StartWebEgress", {"url": HELD_URL, "audioOnly": True}, HEADER_MS)
        elapsed = time.monotonic() - began
        result["held_http"] = {"status": status, "body": error, "elapsed_s": elapsed}
        assert status == 408, result["held_http"]
        assert 1.1 <= elapsed < 2.6, result["held_http"]
        held = wait(lambda: mode_events(HELD_URL, "entered"), "held request admitted by real generated claim")
        assert len(held) == 1 and held[0]["egress_id"].startswith("EG_"), held
        result["held_wire"] = held[0]
        try:
            finished = wait(lambda: mode_events(HELD_URL, "finished"), "held worker actual context exit", timeout=.8)
        except RuntimeError:
            finished = []
        result["held_finish_before_cleanup"] = finished

        status, positive = http_request("StartWebEgress", {"url": POSITIVE_URL, "audioOnly": True}, 3000)
        result["positive_http"] = {"status": status, "body": positive}
        assert status == 200, positive
        positive_id = egress_id(positive)
        assert positive["web"]["url"] == POSITIVE_URL, positive
        done = mode_events(POSITIVE_URL, "finished")
        assert len(done) == 1 and done[0]["egress_id"] == positive_id, done
        assert done[0]["cause"] == "typed positive response", done
        status, listed = http_request("ListEgress", {"egressId": positive_id})
        assert status == 200 and [egress_id(row) for row in listed["items"]] == [positive_id], listed
        assert positive_id != held[0]["egress_id"]
        result["positive_catalog_persisted"] = True
        assert http_request()[0] == 200
        assert 0 < held[0]["wire_budget_ns"] <= HEADER_MS * NS_PER_MS, "worker wire budget exceeded the public deadline"
        assert len(finished) == 1 and finished[0]["cause"] == "context deadline exceeded", "worker handler did not exit on public deadline"
        assert finished[0]["egress_id"] == held[0]["egress_id"]
        assert error == {"code": "canceled", "msg": "request canceled"}, result["held_http"]
        result["status"] = "PASS"
    except Exception as error:
        result["status"] = "FAIL"
        result["error"] = f"{type(error).__name__}: {error}"
    finally:
        for name, proc, log in reversed(processes):
            try:
                if proc.poll() is None:
                    proc.send_signal(signal.SIGINT if name == "main" else signal.SIGTERM)
                result["owned"][name]["exit"] = proc.wait(timeout=45)
            except subprocess.TimeoutExpired:
                proc.kill()
                result["owned"][name]["exit"] = proc.wait()
                result["owned"][name]["forced"] = True
            log.close()
            if result["owned"][name]["exit"] != 0:
                result["status"] = "FAIL"
        if events_path.exists():
            result["observer_events_after_cleanup"] = events()
        try:
            assert_free(ports)
            result["ports_released"] = True
        except OSError as error:
            result["status"] = "FAIL"
            result["release_error"] = str(error)
        result["binary_identity_stable"] = all(sha(bins[name]) == item["sha256"]
                                               for name, item in result["binaries"].items())
        if not result["binary_identity_stable"]:
            result["status"] = "FAIL"
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ("go", "rust", "redis", "observer", "output"):
        parser.add_argument("--" + name, type=Path, required=True)
    args = parser.parse_args()
    args.output.mkdir(mode=0o700, parents=True, exist_ok=False)
    cases = [case(path.resolve(strict=True), label, args)
             for label, path in [("go", args.go), ("rust", args.rust)]]
    passed = all(case["status"] == "PASS" for case in cases)
    (args.output / "execution.json").write_text(json.dumps({
        "status": "PASS" if passed else "FAIL", "fixture_sha256": sha(Path(__file__)), "cases": cases,
        "scope": "Actual generated Go worker and Redis claim/context/typed response; no media recording credit",
    }, indent=2) + "\n")
    return not passed


if __name__ == "__main__":
    raise SystemExit(main())
