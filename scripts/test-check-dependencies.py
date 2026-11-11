#!/usr/bin/env python3
"""Regression checks for dependency gates using synthetic Cargo metadata."""
import importlib.util
from pathlib import Path
import unittest
import sys
sys.dont_write_bytecode = True

spec = importlib.util.spec_from_file_location("dependency_gate", Path(__file__).with_name("check-dependencies.py"))
gate = importlib.util.module_from_spec(spec)
spec.loader.exec_module(gate)


def fixture(edges):
    names = {"rtc-core", "rtc-media", "rtc-runtime", "livecloud"}
    names.update(edges)
    for children in edges.values():
        names.update(child for child, _ in children)
    return {
        "packages": [{"id": name, "name": name, "version": "1.0.0", "source": None} for name in sorted(names)],
        "workspace_members": ["rtc-core", "rtc-media", "rtc-runtime", "livecloud"],
        "resolve": {"nodes": [
            {"id": name, "features": [], "deps": [
                {"pkg": child, "dep_kinds": [{"kind": kind, "target": None}]} for child, kind in edges.get(name, [])
            ]} for name in sorted(names)
        ]},
    }


class DependencyGateTests(unittest.TestCase):
    def test_inactive_optional_crypto_is_excluded_but_active_crypto_is_rejected(self):
        metadata = fixture({"livecloud": [("rustls-webpki", None)], "rustls-webpki": [("ring", None)]})
        graphs = {name: gate.activated_tree_edges(metadata, f"0{name} v1.0.0")
                  for name in gate.ARCHITECTURE_ROOTS}
        graphs["livecloud"] = gate.activated_tree_edges(metadata,
            "0livecloud v1.0.0\n1rustls-webpki v1.0.0\n")
        self.assertTrue(gate.check_metadata(metadata, active_graphs=graphs)["passed"])
        graphs["livecloud"] = gate.activated_tree_edges(metadata,
            "0livecloud v1.0.0\n1rustls-webpki v1.0.0\n2ring v1.0.0\n")
        self.assertFalse(gate.check_metadata(metadata, active_graphs=graphs)["passed"])

    def test_tree_rejects_invalid_depth_and_unknown_identity(self):
        metadata = fixture({})
        for output in ["", "2livecloud v1.0.0", "0unknown v1.0.0", "not a package"]:
            with self.assertRaises(ValueError):
                gate.activated_tree_edges(metadata, output)

    def test_dev_only_native_crypto_is_excluded(self):
        metadata = fixture({"livecloud": [("ring", "dev"), ("rtc-runtime", None)], "rtc-runtime": [("rtc-core", None)]})
        self.assertTrue(gate.check_metadata(metadata)["passed"])

    def test_build_dependencies_are_production_dependencies(self):
        metadata = fixture({"livecloud": [("builder", "build")], "builder": [("aws-lc-sys", None)]})
        result = gate.check_metadata(metadata)
        self.assertFalse(result["passed"])
        self.assertEqual(result["checks"][-1]["violations"][0]["path"], ["livecloud@1.0.0", "builder@1.0.0", "aws-lc-sys@1.0.0"])

    def test_transitive_protocol_dependency_fails_a01(self):
        result = gate.check_metadata(fixture({"rtc-media": [("helper", None)], "helper": [("lk-protocol", None)]}))
        self.assertFalse(result["passed"])
        self.assertEqual(result["checks"][1]["violations"][0]["package"], "lk-protocol@1.0.0")

    def test_os_ffi_and_dev_protocol_dependency_are_allowed(self):
        result = gate.check_metadata(fixture({"rtc-runtime": [("libc", None), ("lk-config", "dev")]}))
        self.assertTrue(result["passed"])

    def test_cycles_terminate(self):
        self.assertTrue(gate.check_metadata(fixture({"rtc-core": [("helper", None)], "helper": [("rtc-core", None)]}))["passed"])


if __name__ == "__main__":
    unittest.main()
