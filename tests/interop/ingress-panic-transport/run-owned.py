#!/usr/bin/env python3
"""Physical no-Redis Ingress Twirp panic connection parity on owned sockets."""
import argparse
import base64
import hashlib
import hmac
import json
import os
import signal
import socket
import subprocess
import time
from pathlib import Path

EXPECTED_BODY = b'{"code":"internal","msg":"Internal service panic"}'
PANIC_METHODS = ("UpdateIngress", "DeleteIngress")


def sha(path):
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def compact(value):
    return json.dumps(value, separators=(",", ":")).encode()


def token():
    def encode(value):
        return base64.urlsafe_b64encode(compact(value)).rstrip(b"=")
    data = encode({"alg": "HS256", "typ": "JWT"}) + b"." + encode({
        "iss": "devkey", "sub": "owned-ingress-panic-fixture", "exp": int(time.time()) + 900,
        "video": {"ingressAdmin": True},
    })
    return data + b"." + base64.urlsafe_b64encode(
        hmac.digest(b"owned-ingress-fixture-secret-0123456789", data, "sha256")
    ).rstrip(b"=")


def request(method=None, auth=None):
    if method is None:
        return b"GET / HTTP/1.1\r\nHost: 127.0.0.1\r\n\r\n"
    path = "/twirp/livekit.Ingress/" + method
    body = b'{"ingressId":"IN_owned_no_store"}'
    parts = [f"POST {path} HTTP/1.1", "Host: 127.0.0.1",
             "Content-Type: application/json", f"Content-Length: {len(body)}"]
    if auth is not None:
        parts.append("Authorization: Bearer " + auth.decode())
    return "\r\n".join(parts).encode() + b"\r\n\r\n" + body


class Wire:
    def __init__(self, sock):
        self.sock = sock
        self.buf = b""

    def fill(self):
        chunk = self.sock.recv(4096)
        if not chunk:
            raise EOFError("HTTP response ended before boundary")
        self.buf += chunk
        assert len(self.buf) <= 65536

    def until(self, delimiter):
        while delimiter not in self.buf:
            self.fill()
        value, self.buf = self.buf.split(delimiter, 1)
        return value

    def length(self, length):
        assert 0 <= length <= 65536
        while len(self.buf) < length:
            self.fill()
        value, self.buf = self.buf[:length], self.buf[length:]
        return value

    def response(self):
        head = self.until(b"\r\n\r\n").split(b"\r\n")
        assert head[0].startswith(b"HTTP/1.1 "), head[0]
        status = int(head[0].split(b" ")[1])
        headers = {}
        for row in head[1:]:
            name, value = row.split(b":", 1)
            headers.setdefault(name.decode().lower(), []).append(value.decode().strip())
        if "content-length" in headers:
            assert len(headers["content-length"]) == 1
            body = self.length(int(headers["content-length"][0]))
        elif headers.get("transfer-encoding") == ["chunked"]:
            body = bytearray()
            while True:
                count = int(self.until(b"\r\n").split(b";", 1)[0], 16)
                if count == 0:
                    while self.until(b"\r\n"):
                        pass
                    break
                body.extend(self.length(count))
                assert self.length(2) == b"\r\n"
                assert len(body) <= 65536
            body = bytes(body)
        else:
            raise AssertionError("no explicit HTTP body framing")
        return status, headers, body

    def retired(self, following=None):
        assert not self.buf, "second request returned bytes"
        try:
            if following is not None:
                self.sock.sendall(following)
            chunk = self.sock.recv(1)
        except ConnectionResetError:
            return "reset"
        except BrokenPipeError:
            return "broken_pipe"
        assert not chunk, "second request returned bytes"
        return "eof"


def reserve(kind):
    sock = socket.socket(socket.AF_INET, kind)
    sock.bind(("127.0.0.1", 0))
    return sock


def assert_free(port, kind):
    with socket.socket(socket.AF_INET, kind) as sock:
        if kind == socket.SOCK_STREAM:
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.bind(("127.0.0.1", port))


def case(binary, label, output):
    tcp = [reserve(socket.SOCK_STREAM) for _ in range(2)]
    udp = reserve(socket.SOCK_DGRAM)
    port, rtc_tcp, rtc_udp = [sock.getsockname()[1] for sock in [*tcp, udp]]
    for sock in [*tcp, udp]:
        sock.close()
    config = output / (label + ".json")
    config.write_bytes(compact({
        "port": port, "bind_addresses": ["127.0.0.1"],
        "keys": {"devkey": "owned-ingress-fixture-secret-0123456789"},
        "rtc": {"tcp_port": rtc_tcp, "udp_port": rtc_udp, "node_ip": "127.0.0.1",
                "use_external_ip": False, "enable_loopback_candidate": True,
                "ips": {"includes": ["127.0.0.1/32"]}},
        "turn": {"enabled": False}, "logging": {"level": "info"},
    }))
    env = {k: v for k, v in os.environ.items() if not k.startswith("LIVEKIT_")
           and k not in ("REDIS_HOST", "REDIS_PASSWORD")}
    result = {"server": label, "binary": str(binary), "binary_sha256": sha(binary),
              "config_sha256": sha(config), "cases": [], "status": "RUNNING"}
    log = (output / (label + ".log")).open("wb")
    proc = None
    def connect():
        return socket.create_connection(("127.0.0.1", port), timeout=4)
    def health():
        with connect() as sock:
            sock.sendall(request())
            return Wire(sock).response()[0] == 200
    try:
        proc = subprocess.Popen([str(binary), "--config", str(config)], env=env,
                                stdout=log, stderr=subprocess.STDOUT)
        result["owned_pid"] = proc.pid
        deadline = time.monotonic() + 20
        while True:
            assert proc.poll() is None, "owned server exited during startup"
            try:
                if health():
                    break
            except (OSError, EOFError):
                pass
            assert time.monotonic() < deadline, "owned health deadline"
            time.sleep(.1)

        # Go starts its listener before publishing running=true after 100ms.
        # Do not signal Stop during that startup window.
        time.sleep(.25)
        auth = token()
        with connect() as sock:
            wire = Wire(sock)
            sock.sendall(request("UpdateIngress"))
            unauthenticated = wire.response()[0]
            assert unauthenticated == 401, unauthenticated
            sock.sendall(request())
            assert wire.response()[0] == 200, "ordinary failure must allow reuse"
        result["ordinary_error_reused"] = True
        for method in PANIC_METHODS:
            for followup in ("sequential", "pipelined"):
                with connect() as sock:
                    wire = Wire(sock)
                    sock.sendall(request())
                    assert wire.response()[0] == 200
                    sock.sendall(request(method, auth) + (request() if followup == "pipelined" else b""))
                    status, headers, body = wire.response()
                    row = {"method": method, "followup": followup, "status": status,
                           "connection_headers": headers.get("connection", []),
                           "content_type": headers.get("content-type", []),
                           "body": body.decode(), "content_length": headers.get("content-length", [])}
                    result["cases"].append(row)
                    assert status == 500 and body == EXPECTED_BODY, row
                    assert headers.get("content-length") == [str(len(EXPECTED_BODY))], row
                    assert headers.get("content-type") == ["application/json"], row
                    row["same_connection_retirement"] = wire.retired(
                        request() if followup == "sequential" else None)
                    row["fresh_health"] = health()
                    assert row["fresh_health"]
        result["silent_retirement"] = all(not row["connection_headers"] for row in result["cases"])
        result["status"] = "PASS" if result["silent_retirement"] else "FAIL"
        if not result["silent_retirement"]:
            result["error"] = "Twirp panic retired the wire but advertised Connection"
    except Exception as exc:
        result["status"] = "FAIL"
        result["error"] = f"{type(exc).__name__}: {exc}"
    finally:
        if proc is not None:
            if proc.poll() is None:
                proc.send_signal(signal.SIGINT)
            try:
                result["server_exit"] = proc.wait(timeout=45)
            except subprocess.TimeoutExpired:
                proc.kill()
                result["server_exit"] = proc.wait()
                result["forced"] = True
            if result["server_exit"] != 0:
                result["status"] = "FAIL"
        log.close()
        try:
            for owned in (port, rtc_tcp):
                assert_free(owned, socket.SOCK_STREAM)
            assert_free(rtc_udp, socket.SOCK_DGRAM)
            result["ports_released"] = True
        except OSError as exc:
            result["status"] = "FAIL"
            result["release_error"] = str(exc)
        result["binary_identity_stable"] = result["binary_sha256"] == sha(binary)
        if not result["binary_identity_stable"]:
            result["status"] = "FAIL"
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--go", type=Path, required=True)
    parser.add_argument("--rust", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    args.output.mkdir(mode=0o700, parents=True, exist_ok=False)
    cases = [case(binary.resolve(strict=True), label, args.output)
             for label, binary in [("go", args.go), ("rust", args.rust)]]
    result = {"cases": cases, "script_sha256": sha(Path(__file__)),
              "status": "PASS" if all(row["status"] == "PASS" for row in cases) else "FAIL"}
    (args.output / "execution.json").write_text(json.dumps(result, indent=2) + "\n")
    return result["status"] != "PASS"


if __name__ == "__main__":
    raise SystemExit(main())
