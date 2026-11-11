#!/usr/bin/env python3
"""Compare pprof parameter precedence and real allocation delta profiles."""
import argparse
from http.client import HTTPConnection
from importlib.util import module_from_spec, spec_from_file_location
import json
from pathlib import Path
import subprocess

SPEC = spec_from_file_location(
    "debug_wire",
    Path("tests/interop/debug-profiles/run-wire.py"),
)
wire = module_from_spec(SPEC)
SPEC.loader.exec_module(wire)
PREFIX = wire.PREFIX
BAD_SECONDS = 'invalid value for "seconds" - must be a positive integer\n'
BAD_DEBUG = "seconds and debug params are incompatible\n"
UNKNOWN = "Unknown profile\n"
BAD_PATH = "400 Bad Request"
FORM = "application/x-www-form-urlencoded"

# Case, method, target, request body, content type, expected error or delta filename.
CASES = [
    ("seconds_first", "GET", "/heap?seconds=broken&seconds=1", None, None, BAD_SECONDS),
    ("debug_first", "GET", "/heap?seconds=1&debug=2&debug=0", None, None, BAD_DEBUG),
    ("bad_escape_skipped", "GET", "/heap?seconds=%GG&seconds=1&debug=1", None, None, BAD_DEBUG),
    ("bad_semicolon_skipped", "GET", "/heap?seconds=bad;name=x&seconds=1&debug=1", None, None, BAD_DEBUG),
    ("escaped_key", "GET", "/heap?sec%6Fnds=broken&seconds=1", None, None, BAD_SECONDS),
    ("post_form_first", "POST", "/heap?seconds=1", "seconds=broken&seconds=1", FORM, BAD_SECONDS),
    ("put_form_first", "PUT", "/allocs?seconds=1&debug=0", "debug=2", FORM, BAD_DEBUG),
    ("patch_form_type", "PATCH", "/allocs?seconds=1", "seconds=broken", FORM.upper() + "; charset=UTF-8", BAD_SECONDS),
    ("get_body_ignored", "GET", "/allocs?seconds=broken", "seconds=1", FORM, BAD_SECONDS),
    ("wrong_media_type_ignored", "POST", "/allocs?seconds=broken", "seconds=1", "text/plain", BAD_SECONDS),
    ("unknown_non_utf8", "GET", "/%FF", None, None, UNKNOWN),
    ("unknown_suffix_non_utf8", "GET", "/heap%FF", None, None, UNKNOWN),
    ("unknown_escaped_slash", "GET", "/%2F", None, None, UNKNOWN),
    ("malformed_path_escape", "GET", "/%GG", None, None, BAD_PATH),
    ("heap_delta", "GET", "/heap?seconds=1", None, None, "heap-delta"),
    ("allocs_delta", "POST", "/allocs?seconds=broken&debug=1", "seconds=1&debug=0", FORM, "allocs-delta"),
]


def profile_fields(profile):
    tool = subprocess.run(
        ["go", "tool", "pprof", "-raw", str(profile)],
        capture_output=True, text=True, timeout=10, check=False,
    )
    fields = {
        "go_pprof_exit": tool.returncode,
        "has_samples_section": "Samples:" in tool.stdout,
        "alloc_types": any("alloc_objects/count alloc_space/bytes inuse_objects/count inuse_space/bytes" in line
                           for line in tool.stdout.splitlines()),
        "duration": next((line[10:] for line in tool.stdout.splitlines() if line.startswith("Duration: ")), None),
    }
    return fields


def run(binary, output, side, report):
    cases = {(method, PREFIX + path): (name, body, media_type)
             for name, method, path, body, media_type, _ in CASES}

    def request(port, method, path):
        name, body, media_type = cases.get((method, path), (None, None, None))
        headers = {"Content-Type": media_type} if media_type else {}
        connection = HTTPConnection("127.0.0.1", port, timeout=12)
        try:
            connection.request(method, path, body, headers)
            response = connection.getresponse()
            result = response.read()
            if name and result.startswith(b"\x1f\x8b"):
                profile = output / f"{side}-{name}.pb.gz"
                profile.write_bytes(result)
                result = json.dumps(profile_fields(profile), sort_keys=True).encode()
                profile.unlink()
            return response.status, {key.lower(): value for key, value in response.getheaders()}, result
        finally:
            connection.close()

    wire.CASES = [(name, method, PREFIX + path) for name, method, path, *_ in CASES]
    wire.request = request
    wire.run(str(binary.resolve(strict=True)), output, side, report)


def validate(report):
    checks = []
    for (_, _, _, _, _, expected), row in zip(CASES, report["cases"], strict=True):
        headers = row["headers"]
        if expected.endswith("-delta"):
            try:
                fields = json.loads(row["body"])
            except ValueError:
                fields = {}
            duration = fields.get("duration") or ""
            match = (
                row["status"] == 200 and headers["content-type"] == "application/octet-stream"
                and headers["content-disposition"] == f'attachment; filename="{expected}"'
                and headers["x-content-type-options"] == "nosniff"
                and fields.get("go_pprof_exit") == 0 and fields.get("has_samples_section")
                and fields.get("alloc_types") and bool(duration)
                and .9 <= float(duration) <= 6
            )
        else:
            status = 404 if expected == UNKNOWN else 400
            rejected_by_parser = expected == BAD_PATH
            match = (
                row["status"] == status and row["body"] == expected
                and headers["x-go-pprof"] == (None if rejected_by_parser else "1")
                and headers["content-type"] == "text/plain; charset=utf-8"
                and headers["x-content-type-options"] == (None if rejected_by_parser else "nosniff")
                and headers["content-disposition"] is None
            )
        checks.append({"case": row["case"], "match": bool(match)})
    return checks


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--go", type=Path, required=True)
    parser.add_argument("--rust", type=Path)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    output = args.output.resolve()
    output.mkdir(parents=True, exist_ok=False)
    report = {}
    try:
        for side, binary in [("go", args.go), ("rust", args.rust)]:
            if binary is None:
                continue
            result = report.setdefault(side, {})
            run(binary, output, side, result)
            assert result["server_exit"] == 0 and not result.get("forced_kill"), result
            result["checks"] = validate(result)
        return int(not all(check["match"] for result in report.values() for check in result["checks"]))
    finally:
        (output / "execution.json").write_text(json.dumps(report, indent=2) + "\n")


if __name__ == "__main__":
    raise SystemExit(main())
