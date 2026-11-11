#!/usr/bin/env python3
"""Reproduce the Redis TLS 1.2 ECDSA SHA-512 interoperability failure."""
import argparse
import hashlib
import importlib.util
import json
from pathlib import Path
import ssl
from types import SimpleNamespace

HERE = Path(__file__).resolve().parent


def tls12_context(protocol):
    context = ssl.SSLContext(protocol)
    context.minimum_version = ssl.TLSVersion.TLSv1_2
    context.maximum_version = ssl.TLSVersion.TLSv1_2
    return context


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--go", type=Path, required=True)
    parser.add_argument("--rust", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--expect", choices=("before", "after"), default="after")
    args = parser.parse_args()
    if not __debug__:
        parser.error("assertions must remain enabled")
    if args.expect == "before" and ssl.OPENSSL_VERSION != "LibreSSL 2.8.3":
        parser.error("--expect before requires the observed LibreSSL 2.8.3 server")

    spec = importlib.util.spec_from_file_location("topologies", HERE / "run-topologies.py")
    topology = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(topology)
    topology.ssl = SimpleNamespace(
        SSLContext=tls12_context, PROTOCOL_TLS_SERVER=ssl.PROTOCOL_TLS_SERVER
    )

    fixtures = topology.FIXTURES
    cases = {
        "rsa": ("expanded/rsa.pem", "expanded/rsa-key.pem", "expanded/root-rsa.pem"),
        "p256": ("server.pem", "server-key.pem", "root.pem"),
        "p256-leaf-p384-issuer": ("p384-chain-server.pem", "server-key.pem", "p384-root.pem"),
    }
    binaries = {"go": args.go.resolve(strict=True), "rust": args.rust.resolve(strict=True)}
    report = {
        "python_tls": ssl.OPENSSL_VERSION,
        "expected_phase": args.expect,
        "binary_sha256": {
            kind: hashlib.sha256(path.read_bytes()).hexdigest()
            for kind, path in binaries.items()
        },
        "cases": [],
    }
    args.output.mkdir()
    for suite, (cert, key, ca) in cases.items():
        overlay = args.output / suite
        (overlay / "expanded").mkdir(parents=True)
        for target, source in zip(
            ("rsa.pem", "rsa-key.pem", "root-rsa.pem"), (cert, key, ca)
        ):
            (overlay / "expanded" / target).symlink_to(fixtures / source)
        topology.FIXTURES = overlay
        for name in ("cluster", "sentinel"):
            for kind, binary in binaries.items():
                directory = overlay / f"TLSv1_2-{name}-{kind}"
                result = {
                    "suite": suite, "certificate": cert, "ca": ca,
                    "version": "TLSv1_2", "topology": name, "kind": kind,
                }
                try:
                    row = topology.run(binary, kind, name, "normal", directory, True)
                    topology.check_oracle(row)
                    result.update(pass_oracle=True, observation=row)
                except (AssertionError, RuntimeError) as error:
                    stderr_path = directory / "stderr"
                    result.update(
                        pass_oracle=False, error=str(error),
                        stderr=stderr_path.read_text() if stderr_path.exists() else "",
                    )
                report["cases"].append(result)
                print(suite, name, kind, result["pass_oracle"], flush=True)
                (args.output / "execution.json").write_text(json.dumps(report, indent=2) + "\n")

    for result in report["cases"]:
        expected_failure = (
            args.expect == "before" and result["kind"] == "rust" and result["suite"] != "rsa"
        )
        assert result["pass_oracle"] != expected_failure, result
        if expected_failure:
            assert "UnsupportedSignatureAlgorithmForPublicKeyContext" in result["stderr"], result


if __name__ == "__main__":
    main()
