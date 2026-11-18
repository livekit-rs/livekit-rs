#!/usr/bin/env python3
"""Exercise the native reader using fresh files and deliberately corrupted inputs."""
import argparse
import copy
import hashlib
import json
import os
from pathlib import Path
import subprocess


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", type=Path, required=True)
    parser.add_argument("--go-results", type=Path, required=True)
    parser.add_argument("--out", type=Path, required=True)
    args = parser.parse_args()
    args.out.mkdir(exist_ok=False)
    original = args.go_results.read_bytes()
    rows = [json.loads(line) for line in original.splitlines()]
    assert len(rows) == 90
    results = []

    def run(name, data, expected, existing=False):
        source = args.out / (name + ".input.jsonl")
        target = args.out / (name + ".actual.jsonl")
        source.write_bytes(data)
        if existing:
            target.write_bytes(b"preserve-existing-output\n")
        env = dict(os.environ, C54_GO_JSONL=str(source.resolve()),
                   C54_RUST_JSONL=str(target.resolve()))
        command = [str(args.binary.resolve()), "--exact",
                   "downtrack::tests::c54_reference_source::reference_layer_matrix",
                   "--ignored", "--nocapture"]
        result = subprocess.run(command, env=env, capture_output=True)
        output = result.stdout + result.stderr
        (args.out / (name + ".log")).write_bytes(output)
        passed = result.returncode == expected and b"running 1 test" in output
        if existing:
            passed &= target.read_bytes() == b"preserve-existing-output\n"
        if expected == 0:
            actual = [json.loads(line) for line in target.read_bytes().splitlines()]
            passed &= actual == rows
        results.append({"name": name, "command": command, "exit": result.returncode,
                        "expected_exit": expected, "control_passed": passed})

    def encode(value):
        return ("\n".join(json.dumps(row) for row in value) + "\n").encode()

    run("valid", original, 0)
    run("existing-output", original, 101, True)
    run("empty", b"", 101)
    run("missing-row", encode(rows[:-1]), 101)
    run("duplicate-row", encode(rows + [rows[0]]), 101)
    run("reordered", encode([rows[1], rows[0]] + rows[2:]), 101)
    for field in rows[0]["observations"][0]:
        corrupt = copy.deepcopy(rows)
        observation = corrupt[0]["observations"][0]
        value = observation[field]
        observation[field] = (not value if isinstance(value, bool) else
                              0 if value is None else
                              value + 1 if isinstance(value, int) else "fresh")
        run("changed-" + field, encode(corrupt), 101)
        corrupt = copy.deepcopy(rows)
        del corrupt[0]["observations"][0][field]
        run("missing-" + field, encode(corrupt), 101)
    for name, value in [("scenario", {"reference": 7}), ("unknown", {"extra": True})]:
        corrupt = copy.deepcopy(rows)
        (corrupt[0]["scenario"] if name == "scenario" else corrupt[0]).update(value)
        run(name, encode(corrupt), 101)
    corrupt = copy.deepcopy(rows)
    corrupt[0]["observations"][0]["max_spatial"] = "-1"
    run("wrong-type", encode(corrupt), 101)
    first, *rest = original.splitlines()
    run("duplicate-key", b'{"schema":1,' + first[1:] + b"\n" + b"\n".join(rest) + b"\n", 101)
    assert args.go_results.read_bytes() == original
    receipt = {"binary_sha256": hashlib.sha256(args.binary.read_bytes()).hexdigest(),
               "go_results_sha256": hashlib.sha256(original).hexdigest(),
               "controls": results, "original_unchanged": True}
    (args.out / "results.json").write_text(json.dumps(receipt, indent=2) + "\n")
    print(json.dumps({"controls": len(results), "passed": sum(r["control_passed"] for r in results)}))
    assert all(r["control_passed"] for r in results), results


if __name__ == "__main__":
    main()
