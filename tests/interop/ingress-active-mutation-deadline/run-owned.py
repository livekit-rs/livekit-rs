#!/usr/bin/env python3
"""Active Ingress worker timeout followed by public catalog mutation on owned Redis."""
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
SECRET = "owned-ingress-mutation-secret-0123456789"
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
        "iss": KEY, "sub": "owned-ingress-mutation", "exp": int(time.time()) + 300,
        "video": {"ingressAdmin": True},
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
    settings = {
        "port": http_port, "bind_addresses": ["127.0.0.1"],
        "keys": {KEY: SECRET}, "redis": {"address": f"127.0.0.1:{redis_port}"},
        "rtc": {"tcp_port": tcp_port, "udp_port": udp_port, "node_ip": "127.0.0.1",
                "use_external_ip": False, "enable_loopback_candidate": True,
                "ips": {"includes": ["127.0.0.1/32"]}},
        "turn": {"enabled": False}, "psrpc": {"compression": {"quality": 0}},
        "ingress": {"rtmp_base_url": "rtmp://127.0.0.1/owned-fixture"},
        "logging": {"level": "info"},
    }
    if args.max_attempts is not None:
        settings["psrpc"]["max_attempts"] = args.max_attempts
    config.write_text(json.dumps(settings))
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

    def mode_events(method, phase):
        return [row for row in events() if row.get("method") == method and row["phase"] == phase]

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
                connection.request("POST", "/twirp/livekit.Ingress/" + method, json.dumps(data), headers)
            response = connection.getresponse()
            body = response.read()
            return response.status, json.loads(body) if method else body.decode()
        finally:
            connection.close()

    def field(value, snake, camel):
        fields = [value[name] for name in (snake, camel) if name in value]
        assert len(fields) == 1, (snake, camel, list(value))
        return fields[0]

    def ingress_id(value):
        ident = field(value, "ingress_id", "ingressId")
        assert ident.startswith("IN_")
        return ident

    def redis(*values):
        return subprocess.check_output([str(args.redis.with_name("redis-cli")), "--raw", "-p",
                                        str(redis_port), *values], timeout=5).decode().rstrip("\n")

    def lookup(ident):
        return http_request("ListIngress", {"ingressId": ident})

    def status_of(value):
        return value["state"]["status"]

    def active(ident):
        code, body = lookup(ident)
        assert code == 200 and len(body["items"]) == 1, (code, body)
        value = body["items"][0]
        assert ingress_id(value) == ident
        return value

    def contents(ident, stream_key, room, new_room):
        return {
            "catalog": redis("HEXISTS", "ingress", ident) == "1",
            "stream_key": redis("HGET", "{ingress}_stream_key", stream_key) == ident,
            "old_room_index": redis("SISMEMBER", "room_{ingress}:" + room, ident) == "1",
            "new_room_index": redis("SISMEMBER", "room_{ingress}:" + new_room, ident) == "1",
            "state": redis("EXISTS", "{ingress}_state:" + ident) == "1",
        }

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
        launch("main", [str(binary), "--config", str(config)], output / "main.log")
        wait(http_ready, "owned HTTP ready")
        # Go publishes its running flag after the listener is already active.
        time.sleep(.25)
        controls = {}
        for action in ("UpdateIngress", "DeleteIngress"):
            room = "owned-old-" + action
            code, item = http_request("CreateIngress", {
                "inputType": "RTMP_INPUT", "name": "owned-rtmp", "roomName": room,
                "participantIdentity": "owned-publisher", "participantMetadata": "original",
            })
            assert code == 200, (code, item)
            assert status_of(item) == "ENDPOINT_INACTIVE"
            ident = ingress_id(item)
            stream = field(item, "stream_key", "streamKey")
            assert stream
            assert contents(ident, stream, room, "owned-next") == {
                "catalog": True, "stream_key": True, "old_room_index": True,
                "new_room_index": False, "state": True,
            }
            controls[action] = (ident, stream, room)
        launch("observer", [str(args.observer), f"127.0.0.1:{redis_port}",
                            *[c[0] for c in controls.values()]], events_path)
        wait(lambda: any(row["phase"] == "ready" for row in events()), "pinned generated worker and IOInfo ready")
        result["profiles"] = {}
        for action, (ident, stream, room) in controls.items():
            before = active(ident)
            assert status_of(before) == "ENDPOINT_PUBLISHING"
            before_start = field(before["state"], "started_at", "startedAt")
            assert int(before_start) > 0
            request = {"ingressId": ident}
            if action == "UpdateIngress":
                request.update(roomName="owned-next", participantMetadata="after-held-worker")
            began = time.monotonic()
            code, body = http_request(action, request, HEADER_MS)
            elapsed = time.monotonic() - began
            entered = wait(lambda: mode_events(action, "entered"), "actual claimed handler")
            finished = wait(lambda: mode_events(action, "finished"), "actual worker deadline")
            current = contents(ident, stream, room, "owned-next")
            list_code, listed = lookup(ident)
            response = {"status": code, "elapsed_s": elapsed, "wire": entered,
                        "worker_finished": finished, "physical_catalog": current,
                        "public_list_status": list_code}
            result["profiles"][action] = response
            assert len(entered) == len(finished) == 1
            assert entered[0]["ingress_id"] == finished[0]["ingress_id"] == ident
            assert 0 < entered[0]["wire_budget_ns"] <= HEADER_MS * NS_PER_MS
            assert finished[0]["cause"] == "context deadline exceeded"
            assert 1.1 <= elapsed < 2.6, response
            if action == "UpdateIngress":
                assert entered[0]["room"] == "owned-next"
                assert entered[0]["details"] == "after-held-worker"
                if list_code == 200:
                    item = listed["items"][0]
                    response["public_list_room"] = field(item, "room_name", "roomName")
                    response["public_list_metadata"] = field(item, "participant_metadata", "participantMetadata")
                    response["actual_worker_generation_unchanged"] = (
                        status_of(item) == "ENDPOINT_PUBLISHING"
                        and field(item["state"], "started_at", "startedAt") == before_start
                    )
                response["parity"] = (code == 200 and ingress_id(body) == ident
                    and status_of(body) == "ENDPOINT_PUBLISHING"
                    and response.get("public_list_room") == "owned-next"
                    and response.get("public_list_metadata") == "after-held-worker"
                    and response.get("actual_worker_generation_unchanged") is True
                    and current == {"catalog": True, "stream_key": True, "old_room_index": False,
                                    "new_room_index": True, "state": True})
            else:
                response["parity"] = (code == 200 and ingress_id(body) == ident
                    and status_of(body) == "ENDPOINT_INACTIVE" and list_code == 404
                    and not any(current.values()))
            if code != 200:
                response["twirp_error"] = body
            assert http_request()[0] == 200
        assert all(row["parity"] for row in result["profiles"].values()), "public mutation lost after worker timeout"
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
    parser.add_argument("--max-attempts", type=int)
    args = parser.parse_args()
    args.output.mkdir(mode=0o700, parents=True, exist_ok=False)
    cases = [case(path.resolve(strict=True), label, args)
             for label, path in [("go", args.go), ("rust", args.rust)]]
    passed = all(case["status"] == "PASS" for case in cases)
    (args.output / "execution.json").write_text(json.dumps({
        "status": "PASS" if passed else "FAIL", "fixture_sha256": sha(Path(__file__)),
        "configured_max_attempts": args.max_attempts, "cases": cases,
        "scope": "Actual generated IngressHandler and IOInfo callbacks, Redis indexes and public API; no media ingest credit",
    }, indent=2) + "\n")
    return not passed


if __name__ == "__main__":
    raise SystemExit(main())
