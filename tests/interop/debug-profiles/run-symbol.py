#!/usr/bin/env python3
"""Compare pprof symbol request framing on the fixed Go and Rust listeners."""
import argparse
from http.client import HTTPConnection
from importlib.util import module_from_spec, spec_from_file_location
import json
from pathlib import Path

SPEC = spec_from_file_location("debug_wire", Path(__file__).with_name("run-wire.py"))
wire = module_from_spec(SPEC)
SPEC.loader.exec_module(wire)

SYMBOL = wire.PREFIX + "/symbol"
normal = "num_symbols: 1\n"
overflow = normal + "reading request: bufio: buffer full\n"
cases = [
    ("query_short", "GET", SYMBOL + "?0x0+garbage+0", None, normal),
    ("query_long_word", "GET", SYMBOL + "?" + "x" * 4097, None, overflow),
    ("post_non_utf8", "POST", SYMBOL + "?unused", b"\xff+0x0", normal),
    ("post_long_word", "POST", SYMBOL + "?long", b"x" * 4097 + b"+0", overflow),
    ("post_boundary_delimiter", "POST", SYMBOL + "?boundary", b"x" * 4095 + b"+0", normal),
    ("post_many_short_words", "POST", SYMBOL + "?many", b"0+" * 1050000, normal),
    ("post_ignores_query", "POST", SYMBOL + "?" + "x" * 4097, b"0", normal),
    ("put_ignores_body", "PUT", SYMBOL + "?0", b"x" * 4097, normal),
]
case_bodies = {path: body for _, method, path, body, _ in cases if method == "POST"}
wire.CASES = [(name, method, path) for name, method, path, _, _ in cases]


def request(port, method, path):
    conn = HTTPConnection("127.0.0.1", port, timeout=15)
    try:
        body = case_bodies.get(path) if method == "POST" else None
        if method == "PUT":
            body = b"x" * 4097
        conn.request(method, path, body)
        response = conn.getresponse()
        return response.status, {key.lower(): value for key, value in response.getheaders()}, response.read()
    finally:
        conn.close()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--go", type=Path, required=True)
    parser.add_argument("--rust", type=Path)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    args.output.mkdir()
    report = {}
    wire.request = request
    try:
        for side, binary in (("go", args.go), ("rust", args.rust)):
            if binary is None:
                continue
            wire.run(str(binary.resolve()), args.output, side, report.setdefault(side, {}))
            assert report[side]["server_exit"] == 0 and not report[side].get("forced_kill"), side
        report["checks"] = {}
        for side in ("go", "rust"):
            if side not in report:
                continue
            rows = report[side]["cases"]
            report["checks"][side] = []
            for row, (name, _, _, _, expected) in zip(rows, cases, strict=True):
                passed = (row["case"] == name and row["status"] == 200 and row["body"] == expected
                          and row["headers"]["content-type"] == "text/plain; charset=utf-8"
                          and row["headers"]["x-content-type-options"] == "nosniff")
                report["checks"][side].append({"case": name, "pass": passed})
        return int(not all(row["pass"] for rows in report["checks"].values() for row in rows))
    finally:
        (args.output / "execution.json").write_text(json.dumps(report, indent=2) + "\n")


if __name__ == "__main__":
    raise SystemExit(main())
