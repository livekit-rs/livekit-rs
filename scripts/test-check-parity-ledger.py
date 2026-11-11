#!/usr/bin/env python3
"""Unit tests for the SPEC-0075 parity-ledger checker.

Run: python3 livekit-rs-server/scripts/test-check-parity-ledger.py
"""

from __future__ import annotations

import importlib.util
import json
import os
import subprocess
import sys
import tempfile
import unittest

sys.dont_write_bytecode = True
HERE = os.path.dirname(os.path.abspath(__file__))

_spec = importlib.util.spec_from_file_location(
    "parity_ledger_checker", os.path.join(HERE, "check-parity-ledger.py"))
cpl = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(cpl)


def write_ledger(root: str, items: list[dict]) -> str:
    path = os.path.join(root, "ledger.json")
    with open(path, "w") as handle:
        json.dump({"items": items}, handle)
    return path


def ids(out: dict, kind: str) -> set[str]:
    """Item ids reported under `kind`, detail suffix stripped."""
    return {entry.split(" :: ")[0] for entry in out["findings"].get(kind, [])}


# Identity fields must carry real digests, so the fixtures use 64-hex values.
# A placeholder like "aa" is exactly what the checker is supposed to reject.
GO_SHA = "ab" * 32
RUST_SHA = "cd" * 32


def paired_receipt(item_id: str) -> str:
    """A batch receipt: both binary identities plus the item it covered."""
    return json.dumps({"status": "PASS_SCOPED", "cases": [item_id],
                       "oracle_binary_sha256": GO_SHA,
                       "candidate_binary_sha256": RUST_SHA})


def make_repo(entries: dict[str, str]) -> str:
    """Create a repo tree with the given files, plus a test dir."""
    root = tempfile.mkdtemp()
    for rel, body in entries.items():
        full = os.path.join(root, rel)
        os.makedirs(os.path.dirname(full), exist_ok=True)
        with open(full, "w") as handle:
            handle.write(body)
    return root


class EvidenceResolution(unittest.TestCase):
    def test_resolves_repo_relative_and_server_relative(self):
        root = make_repo({
            "livekit-rs-docs/evidence/x/README.md": "ok",
            "livekit-rs-server/crates/foo/src/lib.rs": "fn a()",
        })
        self.assertTrue(cpl.is_evidence_path(
            "livekit-rs-docs/evidence/x/README.md", root)[0])
        # server-relative form used by some batches
        self.assertTrue(cpl.is_evidence_path(
            "crates/foo/src/lib.rs::some_test", root)[0])
        self.assertFalse(cpl.is_evidence_path("livekit-rs-docs/evidence/x/Nope.json", root)[0])

    def test_prose_is_not_evidence(self):
        root = make_repo({})
        for ref in ["All 21 DATA_PACKET children VERIFIED",
                    "Existing tests: lk-compat HTTP body limit tests"]:
            self.assertFalse(cpl.is_evidence_path(ref, root)[0], ref)


class TierRules(unittest.TestCase):
    def test_verified_without_declared_tiers_is_a_finding(self):
        root = make_repo({"livekit-rs-docs/evidence/a.json": "{}"})
        ledger = write_ledger(root, [{
            "id": "API:Thing", "state": "VERIFIED", "surface": "twirp",
            "evidence": ["livekit-rs-docs/evidence/a.json"],
        }])
        out = cpl.check(ledger, root)
        self.assertIn("API:Thing", ids(out, "VERIFIED_WITHOUT_DECLARED_TIERS"))
        self.assertEqual(out["verified_supported"], 0)

    def test_full_tiers_with_consumer_evidence_is_clean(self):
        root = make_repo({"livekit-rs-docs/evidence/a/paired.json": paired_receipt("API:Thing")})
        ledger = write_ledger(root, [{
            "id": "API:Thing", "state": "VERIFIED", "surface": "twirp", "evidence": [
                "livekit-rs-docs/evidence/a/paired.json"],
            "verification_levels": {t: "MET" for t in cpl.TIERS},
        }])
        out = cpl.check(ledger, root)
        self.assertEqual(out["findings"], {})
        self.assertEqual(out["verified_supported"], 1)

    def test_parse_only_support_cannot_be_verified(self):
        root = make_repo({
            "livekit-rs-server/crates/lk-config/tests/config_leaf_parsing.rs": "fn t()",
        })
        ledger = write_ledger(root, [{
            "id": "YAML:port", "state": "VERIFIED", "surface": "yaml", "evidence": [
                "livekit-rs-server/crates/lk-config/tests/config_leaf_parsing.rs::yaml_broad"],
        }])
        out = cpl.check(ledger, root)
        self.assertIn("YAML:port", ids(out, "VERIFIED_PARSE_TEST_ONLY"))

    def test_source_audit_alone_cannot_attest_consumer_tier(self):
        root = make_repo({"livekit-rs-docs/evidence/f62-config-source-audit.json": "{}"})
        ledger = write_ledger(root, [{
            "id": "YAML:foo", "state": "VERIFIED", "surface": "yaml", "evidence": [
                "livekit-rs-docs/evidence/f62-config-source-audit.json"],
            "verification_levels": {t: "MET" for t in cpl.TIERS},
        }])
        out = cpl.check(ledger, root)
        self.assertIn("YAML:foo", ids(out, "CONSUMER_TIER_WITHOUT_CONSUMER_EVIDENCE"))

    def test_verified_with_missing_main_consumer_flagged(self):
        root = make_repo({"livekit-rs-docs/evidence/a/paired.json": '{"status": "PASS_SCOPED", "oracle_binary_sha256": GO_SHA, "candidate_binary_sha256": RUST_SHA}'})
        levels = {t: "MET" for t in cpl.TIERS}
        levels["main_server_consumer"] = "NOT_RUN"
        ledger = write_ledger(root, [{
            "id": "API:Nope", "state": "VERIFIED", "surface": "twirp",
            "evidence": ["livekit-rs-docs/evidence/a/paired.json"],
            "verification_levels": levels,
        }])
        out = cpl.check(ledger, root)
        self.assertIn("API:Nope", ids(out, "VERIFIED_MISSING_MAIN_CONSUMER"))

    def test_not_applicable_differential_still_allows_verified(self):
        root = make_repo({"livekit-rs-docs/evidence/a/paired.json": '{"status": "PASS_SCOPED", "oracle_binary_sha256": GO_SHA, "candidate_binary_sha256": RUST_SHA}'})
        levels = {t: "MET" for t in cpl.TIERS}
        levels["official_differential_lifecycle"] = "NOT_APPLICABLE"
        ledger = write_ledger(root, [{
            "id": "CLI:gen-keys", "state": "VERIFIED", "surface": "cli",
            "evidence": ["livekit-rs-docs/evidence/a/paired.json"],
            "verification_levels": levels,
        }])
        out = cpl.check(ledger, root)
        self.assertNotIn("CLI:gen-keys", ids(out, "VERIFIED_MISSING_OFFICIAL_DIFFERENTIAL"))

    def test_bad_tier_value_rejected(self):
        root = make_repo({"livekit-rs-docs/evidence/a/paired.json": '{"status": "PASS_SCOPED", "oracle_binary_sha256": GO_SHA, "candidate_binary_sha256": RUST_SHA}'})
        levels = {t: "MET" for t in cpl.TIERS}
        levels["component_behavior"] = "PROBABLY"
        ledger = write_ledger(root, [{
            "id": "API:Bad", "state": "VERIFIED", "surface": "twirp",
            "evidence": ["livekit-rs-docs/evidence/a/paired.json"],
            "verification_levels": levels,
        }])
        out = cpl.check(ledger, root)
        self.assertIn("API:Bad", ids(out, "BAD_TIER_VALUE"))


class ContradictionRules(unittest.TestCase):
    def test_verified_whose_todo_still_says_open(self):
        root = make_repo({"livekit-rs-docs/evidence/a/paired.json": '{"status": "PASS_SCOPED", "oracle_binary_sha256": GO_SHA, "candidate_binary_sha256": RUST_SHA}'})
        ledger = write_ledger(root, [{
            "id": "API:Contr", "state": "VERIFIED", "surface": "twirp",
            "evidence": ["livekit-rs-docs/evidence/a/paired.json"],
            "verification_levels": {t: "MET" for t in cpl.TIERS},
            "todo": "Real worker lifecycle remains OPEN.",
        }])
        out = cpl.check(ledger, root)
        self.assertIn("API:Contr", ids(out, "VERIFIED_TEXT_CONTRADICTS_STATE"))

    def test_declared_scope_legitimises_a_named_remainder(self):
        root = make_repo({"livekit-rs-docs/evidence/a/paired.json": '{"status": "PASS_SCOPED", "oracle_binary_sha256": GO_SHA, "candidate_binary_sha256": RUST_SHA}'})
        ledger = write_ledger(root, [{
            "id": "API:Scoped", "state": "VERIFIED", "surface": "twirp",
            "evidence": ["livekit-rs-docs/evidence/a/paired.json"],
            "verification_levels": {t: "MET" for t in cpl.TIERS},
            "todo": "Worker lifecycle remains OPEN.",
            "accepted_scope": "Covers auth+routing only; worker lifecycle is OPEN in a separate item.",
        }])
        out = cpl.check(ledger, root)
        self.assertNotIn("API:Scoped", ids(out, "VERIFIED_TEXT_CONTRADICTS_STATE"))


class ParentRules(unittest.TestCase):
    def test_parent_needs_coverage_map(self):
        root = make_repo({"livekit-rs-docs/evidence/a/paired.json": '{"status": "PASS_SCOPED", "oracle_binary_sha256": GO_SHA, "candidate_binary_sha256": RUST_SHA}'})
        ledger = write_ledger(root, [{
            "id": "FUNC:DATATRACK", "state": "VERIFIED", "surface": "signal",
            "evidence": ["livekit-rs-docs/evidence/a/paired.json"],
            "verification_levels": {t: "MET" for t in cpl.TIERS},
        }])
        out = cpl.check(ledger, root)
        self.assertIn("FUNC:DATATRACK", ids(out, "PARENT_CLOSED_WITHOUT_COVERAGE_MAP"))

    def test_coverage_map_with_a_gap_is_a_finding(self):
        root = make_repo({"livekit-rs-docs/evidence/a/paired.json": '{"status": "PASS_SCOPED", "oracle_binary_sha256": GO_SHA, "candidate_binary_sha256": RUST_SHA}'})
        ledger = write_ledger(root, [{
            "id": "FUNC:DATATRACK", "state": "VERIFIED", "surface": "signal",
            "evidence": ["livekit-rs-docs/evidence/a/paired.json"],
            "verification_levels": {t: "MET" for t in cpl.TIERS},
            "coverage_map": [{"id": "rpc.timeout", "status": "open"}],
        }])
        out = cpl.check(ledger, root)
        self.assertIn("FUNC:DATATRACK", ids(out, "PARENT_COVERAGE_MAP_HAS_GAPS"))

    def test_full_coverage_map_closes_parent(self):
        root = make_repo({"livekit-rs-docs/evidence/a/paired.json":
                          paired_receipt("FUNC:DATATRACK")})
        ledger = write_ledger(root, [{
            "id": "FUNC:DATATRACK", "state": "VERIFIED", "surface": "signal",
            "evidence": ["livekit-rs-docs/evidence/a/paired.json"],
            "verification_levels": {t: "MET" for t in cpl.TIERS},
            "coverage_map": [{"id": "rpc.timeout", "status": "covered"},
                             {"id": "cloud.only", "status": "not_applicable"}],
        }])
        out = cpl.check(ledger, root)
        self.assertEqual(out["findings"], {})

    def test_child_all_verified_does_not_close_parent(self):
        root = make_repo({"livekit-rs-docs/evidence/a/paired.json":
                          paired_receipt("DATA_PACKET:x")})
        child = {"id": "DATA_PACKET:x", "state": "VERIFIED", "surface": "signal",
                 "evidence": ["livekit-rs-docs/evidence/a/paired.json"],
                 "verification_levels": {t: "MET" for t in cpl.TIERS}}
        ledger = write_ledger(root, [child])
        out = cpl.check(ledger, root)
        self.assertEqual(out["findings"], {})
        # a parent with only prose child-count evidence is still rejected
        ledger2 = write_ledger(root, [child, {
            "id": "FUNC:DATATRACK", "state": "VERIFIED", "surface": "signal",
            "evidence": ["All 1 DATA_PACKET children VERIFIED"],
            "verification_levels": {t: "MET" for t in cpl.TIERS},
        }])
        out2 = cpl.check(ledger2, root)
        self.assertIn("FUNC:DATATRACK", ids(out2, "VERIFIED_PROSE_ONLY_EVIDENCE"))
        self.assertIn("FUNC:DATATRACK", ids(out2, "PARENT_CLOSED_WITHOUT_COVERAGE_MAP"))


class PairedBinaryEvidence(unittest.TestCase):
    """Tier 5 needs an artifact carrying both the Go and the Rust identity."""

    def _artifact(self, root, body, name="livekit-rs-docs/evidence/a/BATCH.json"):
        full = os.path.join(root, name)
        os.makedirs(os.path.dirname(full), exist_ok=True)
        with open(full, "w") as handle:
            json.dump(body, handle)
        return name

    def test_both_identities_counts_as_paired(self):
        root = make_repo({})
        ref = self._artifact(root, {
            "status": "PASS_SCOPED",
            "oracle_binary_sha256": GO_SHA, "candidate_binary_sha256": RUST_SHA})
        self.assertTrue(cpl.paired_differential_evidence([ref], root))
        self.assertFalse(cpl.failing_evidence([ref], root))

    def test_rust_identity_alone_is_not_paired(self):
        root = make_repo({})
        ref = self._artifact(root, {"status": "PASS", "rust_binary_sha256": RUST_SHA})
        self.assertFalse(cpl.paired_differential_evidence([ref], root))

    def test_hand_written_expectation_is_not_paired(self):
        root = make_repo({})
        ref = self._artifact(root, {"status": "PASS",
                                    "expectation": "read from livekit config.go"})
        self.assertFalse(cpl.paired_differential_evidence([ref], root))

    def test_declared_tier5_without_paired_artifact_is_flagged(self):
        root = make_repo({})
        ref = self._artifact(root, {"status": "PASS", "rust_binary_sha256": RUST_SHA})
        ledger = write_ledger(root, [{
            "id": "API:Claim5", "state": "VERIFIED", "surface": "twirp", "evidence": [ref],
            "verification_levels": {t: "MET" for t in cpl.TIERS},
        }])
        out = cpl.check(ledger, root)
        self.assertIn("API:Claim5", ids(out, "TIER5_WITHOUT_PAIRED_BINARY_ARTIFACT"))

    def test_declared_tier5_with_paired_artifact_is_clean(self):
        root = make_repo({})
        ref = self._artifact(root, {"status": "PASS_SCOPED",
                                   "cases": ["API:Claim5ok"],
                                   "oracle_binary_sha256": GO_SHA,
                                   "candidate_binary_sha256": RUST_SHA})
        ledger = write_ledger(root, [{
            "id": "API:Claim5ok", "state": "VERIFIED", "surface": "twirp", "evidence": [ref],
            "verification_levels": {t: "MET" for t in cpl.TIERS},
        }])
        out = cpl.check(ledger, root)
        self.assertEqual(out["findings"], {})
        self.assertEqual(out["verified_with_paired_binary_evidence"], 1)

    def test_paired_total_that_never_names_the_item_is_a_finding(self):
        """f06-style aggregate: both binaries hashed, but no item identified."""
        root = make_repo({})
        ref = self._artifact(root, {
            "status": "PASS_SLICE",
            "binaries": {"livekit-rs-server/target/debug/livecloud": RUST_SHA,
                         "/tmp/livekit-rs-f06-official": GO_SHA},
            "public_differential": {"registered_rpc_decode_auth_cases": 464,
                                    "differences": 0},
        })
        ledger = write_ledger(root, [{
            "id": "API:Egress.ListEgress", "state": "VERIFIED", "surface": "twirp",
            "evidence": [ref],
            "verification_levels": {t: "MET" for t in cpl.TIERS},
        }])
        out = cpl.check(ledger, root)
        self.assertIn("API:Egress.ListEgress",
                      ids(out, "TIER5_ARTIFACT_DOES_NOT_NAME_ITEM"))
        self.assertIn("API:Egress.ListEgress",
                      ids(out, "CONSUMER_TIER_ARTIFACT_DOES_NOT_NAME_ITEM"))
        self.assertEqual(out["verified_with_item_named_paired_evidence"], 0)

    def test_batch_receipt_in_the_cited_directory_names_the_item(self):
        """Citing one case file inside a slot reaches that slot's receipt."""
        root = make_repo({})
        self._artifact(root, {"status": "PASS_PAIRED",
                             "batch": "W1-CLI-CONFIG/generated_flags:--room.empty_timeout",
                             "oracle_binary_sha256": GO_SHA,
                             "candidate_binary_sha256": RUST_SHA})
        ref = "livekit-rs-docs/evidence/a/flag-et-long.pair.json"
        full = os.path.join(root, ref)
        with open(full, "w") as handle:
            json.dump({"profile": "flag-et-long", "go": {}, "rust": {}}, handle)
        self.assertEqual(cpl.item_paired_evidence([ref], root,
                                                  "generated_flags:--room.empty_timeout"),
                         [ref])
        self.assertEqual(cpl.item_paired_evidence([ref], root, "YAML:room.unknown_leaf"),
                         [])

    def test_failing_artifact_cited_as_verified(self):
        root = make_repo({})
        ref = self._artifact(root, {"status": "FAIL",
                                    "oracle_binary_sha256": GO_SHA,
                                    "candidate_binary_sha256": RUST_SHA})
        ledger = write_ledger(root, [{
            "id": "API:CitesFail", "state": "VERIFIED", "surface": "twirp", "evidence": [ref],
            "verification_levels": {t: "MET" for t in cpl.TIERS},
        }])
        out = cpl.check(ledger, root)
        self.assertIn("API:CitesFail", ids(out, "VERIFIED_CITES_FAILING_EVIDENCE"))

    def test_delivery_receipt_binaries_map_counts_as_paired(self):
        """f06-style receipts hash both servers under a `binaries` path map."""
        root = make_repo({})
        ref = self._artifact(root, {
            "status": "PASS_SCOPED_IOINFO_STORE_MAIN_HTTP_WEBHOOK_SLICE",
            "binaries": {
                "livekit-rs-server/target/debug/livecloud": RUST_SHA,
                "/tmp/livekit-rs-f06-io-go": "6e72",
                "/tmp/livekit-rs-f06-official": GO_SHA,
            },
            "public_differential": {"registered_rpc_decode_auth_cases": 464,
                                    "differences": 0},
        })
        self.assertTrue(cpl.paired_differential_evidence([ref], root))

    def test_rust_only_binaries_map_is_tier4_not_paired(self):
        root = make_repo({})
        ref = self._artifact(root, {"status": "PASS", "binaries": {
            "livekit-rs-server/target/debug/livecloud": RUST_SHA}})
        self.assertFalse(cpl.paired_differential_evidence([ref], root))
        self.assertTrue(cpl.rust_binary_evidence([ref], root))

    def test_go_markers_win_over_shared_livekit_rs_prefix(self):
        """/tmp/livekit-rs-f06-io-go is the Go side, not the Rust candidate."""
        rust, go = cpl.identities_from_binaries_map(
            {"/tmp/livekit-rs-f06-io-go": GO_SHA})
        self.assertTrue(go)
        self.assertFalse(rust)

    def test_non_json_receipt_reads_text_markers(self):
        root = make_repo({})
        ref = "livekit-rs-docs/evidence/a/README.md"
        full = os.path.join(root, ref)
        os.makedirs(os.path.dirname(full), exist_ok=True)
        with open(full, "w") as handle:
            handle.write(f"oracle_binary_sha256: {GO_SHA}\n"
                         f"candidate_binary_sha256: {RUST_SHA}\n")
        self.assertTrue(cpl.paired_differential_evidence([ref], root))

    # --- negative controls requested in the 2026-11-03 review -----------------

    def test_marker_names_without_digests_are_not_a_paired_run(self):
        """Free-form prose that says the words but hashes nothing proves nothing."""
        root = make_repo({})
        ref = "livekit-rs-docs/evidence/a/README.md"
        full = os.path.join(root, ref)
        os.makedirs(os.path.dirname(full), exist_ok=True)
        with open(full, "w") as handle:
            handle.write("oracle_binary_sha256: not-recorded\n candidate_binary_sha256: todo\n")
        self.assertFalse(cpl.paired_differential_evidence([ref], root))

    def test_empty_identity_field_is_not_paired(self):
        root = make_repo({})
        ref = self._artifact(root, {"status": "PASS", "oracle_binary_sha256": "",
                                    "candidate_binary_sha256": ""})
        self.assertFalse(cpl.paired_differential_evidence([ref], root))

    def test_truncated_identity_digest_is_not_paired(self):
        root = make_repo({})
        ref = self._artifact(root, {"status": "PASS",
                                    "oracle_binary_sha256": GO_SHA[:16],
                                    "candidate_binary_sha256": RUST_SHA[:16]})
        self.assertFalse(cpl.paired_differential_evidence([ref], root))

    def test_binaries_map_with_empty_hashes_is_not_paired(self):
        root = make_repo({})
        ref = self._artifact(root, {"status": "PASS", "binaries": {
            "/tmp/livekit-server-go": "", "livekit-rs-server/target/debug/livecloud": ""}})
        self.assertFalse(cpl.paired_differential_evidence([ref], root))

    def test_nested_binaries_entry_with_real_digest_is_paired(self):
        """21l shape: {side: {path, sha256}} rather than a flat path map."""
        root = make_repo({})
        ref = self._artifact(root, {"status": "PASS_PAIRED", "binaries": {
            "official_go": {"path": "/tmp/livekit-server-go", "sha256": GO_SHA},
            "rust": {"path": "livekit-rs-server/target/debug/livecloud", "sha256": RUST_SHA}}})
        self.assertTrue(cpl.paired_differential_evidence([ref], root))

    def test_nested_binaries_entry_without_a_digest_is_not_paired(self):
        root = make_repo({})
        ref = self._artifact(root, {"status": "PASS_PAIRED", "binaries": {
            "official_go": {"path": "/tmp/livekit-server-go", "sha256": ""},
            "rust": {"path": "livekit-rs-server/target/debug/livecloud",
                     "sha256": "cfdbc"}}})
        self.assertFalse(cpl.paired_differential_evidence([ref], root))

    def test_paired_artifact_that_never_reports_success_is_not_acceptance(self):
        root = make_repo({})
        ref = self._artifact(root, {"oracle_binary_sha256": GO_SHA,
                                    "candidate_binary_sha256": RUST_SHA,
                                    "cases": ["API:ClaimNoStatus"]})
        ledger = write_ledger(root, [{
            "id": "API:ClaimNoStatus", "state": "VERIFIED", "surface": "twirp",
            "evidence": [ref],
            "verification_levels": {t: "MET" for t in cpl.TIERS}}])
        out = cpl.check(ledger, root)
        self.assertIn("API:ClaimNoStatus", ids(out, "TIER5_PAIRED_ARTIFACT_NOT_REPORTED_PASS"))

    def test_paired_passing_receipt_with_an_empty_case_list_is_flagged(self):
        root = make_repo({})
        ref = self._artifact(root, {"status": "PASS_SCOPED", "cases": [],
                                    "oracle_binary_sha256": GO_SHA,
                                    "candidate_binary_sha256": RUST_SHA,
                                    "batch": "W1/API:ClaimEmptyCases"})
        ledger = write_ledger(root, [{
            "id": "API:ClaimEmptyCases", "state": "VERIFIED", "surface": "twirp",
            "evidence": [ref],
            "verification_levels": {t: "MET" for t in cpl.TIERS}}])
        out = cpl.check(ledger, root)
        self.assertIn("API:ClaimEmptyCases",
                      ids(out, "TIER5_PAIRED_RECEIPT_REPORTS_NO_CASES"))

    def test_receipt_naming_the_item_and_its_cases_is_clean(self):
        root = make_repo({})
        ref = self._artifact(root, {"status": "PASS_SCOPED", "cases": ["API:ClaimOk"],
                                    "oracle_binary_sha256": GO_SHA,
                                    "candidate_binary_sha256": RUST_SHA})
        ledger = write_ledger(root, [{
            "id": "API:ClaimOk", "state": "VERIFIED", "surface": "twirp", "evidence": [ref],
            "verification_levels": {t: "MET" for t in cpl.TIERS}}])
        out = cpl.check(ledger, root)
        self.assertEqual(out["findings"], {})

    def test_unparsable_and_missing_files_are_ignored_not_crashing(self):
        root = make_repo({"livekit-rs-docs/evidence/a/broken.json": "{not json"})
        self.assertEqual(cpl.paired_differential_evidence(
            ["livekit-rs-docs/evidence/a/broken.json", "nope/missing.json"], root), [])


class MiscRules(unittest.TestCase):
    def test_no_evidence_at_all(self):
        root = make_repo({})
        ledger = write_ledger(root, [{"id": "API:E", "state": "VERIFIED",
                                      "surface": "twirp", "evidence": []}])
        out = cpl.check(ledger, root)
        self.assertIn("API:E", ids(out, "VERIFIED_WITHOUT_EVIDENCE"))

    def test_duplicate_id(self):
        root = make_repo({})
        ledger = write_ledger(root, [{"id": "A:x", "state": "OPEN", "surface": "s",
                                      "evidence": []},
                                     {"id": "A:x", "state": "OPEN", "surface": "s",
                                      "evidence": []}])
        out = cpl.check(ledger, root)
        self.assertIn("A:x", ids(out, "DUPLICATE_ID"))

    def test_non_verified_states_are_not_tier_checked(self):
        root = make_repo({})
        ledger = write_ledger(root, [{"id": "API:P", "state": "PARTIAL", "surface": "twirp",
                                      "evidence": []}])
        out = cpl.check(ledger, root)
        self.assertEqual(out["findings"], {})

    def test_cli_exit_code_is_one_when_findings_exist(self):
        root = make_repo({})
        ledger = write_ledger(root, [{"id": "API:E", "state": "VERIFIED",
                                      "surface": "twirp", "evidence": []}])
        script = os.path.join(HERE, "check-parity-ledger.py")
        done = subprocess.run([sys.executable, script, "--ledger", ledger,
                               "--root", root], capture_output=True, text=True)
        self.assertEqual(done.returncode, 1)
        self.assertIn("VERIFIED_WITHOUT_EVIDENCE", done.stdout)


if __name__ == "__main__":
    unittest.main(verbosity=2)
