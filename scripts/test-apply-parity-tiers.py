#!/usr/bin/env python3
"""Unit tests for the SPEC-0075 §9.1 tier inference and downgrade migration."""

from __future__ import annotations

import importlib.util
import json
import os
import sys
import tempfile
import unittest

sys.dont_write_bytecode = True
HERE = os.path.dirname(os.path.abspath(__file__))
_spec = importlib.util.spec_from_file_location(
    "apply_parity_tiers", os.path.join(HERE, "apply-parity-tiers.py"))
ap = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(ap)
# the applier delegates identity rules to the checker; the tests use the same objects
cpl = ap.cpl

ITEM = "API:RoomService.ListEgress"

# Identity fields must carry real digests; short placeholders are rejected now.
GO_SHA = "ab" * 32
RUST_SHA = "cd" * 32


def paired_receipt(item: str) -> str:
    return json.dumps({"status": "PASS_SCOPED", "oracle_binary_sha256": GO_SHA,
                       "candidate_binary_sha256": RUST_SHA, "cases": [item]})


def rust_only_receipt(item: str) -> str:
    return json.dumps({"status": "PASS_SCOPED", "candidate_binary_sha256": RUST_SHA,
                       "cases": [item]})


PAIRED = paired_receipt(ITEM)
RUST_ONLY = rust_only_receipt(ITEM)
SOURCE_AUDIT = json.dumps({"status": "STATIC_SOURCE_CLASSIFICATION_NOT_RUNTIME_PASS",
                           "upstream": "livekit protocol/config/config.go:109"})


def repo(files: dict[str, str]) -> str:
    root = tempfile.mkdtemp()
    for rel, body in files.items():
        full = os.path.join(root, rel)
        os.makedirs(os.path.dirname(full), exist_ok=True)
        with open(full, "w") as handle:
            handle.write(body)
    return root


class TierInference(unittest.TestCase):
    def test_no_evidence_is_tier_zero(self):
        root = repo({})
        got = ap.tiers_for({"id": "A", "evidence": []}, root)
        self.assertEqual(got["max_tier"], 0)
        self.assertEqual(got["levels"]["upstream_source_contract"], "NOT_RUN")

    def test_source_audit_reaches_tier_one_only(self):
        root = repo({"livekit-rs-docs/evidence/s.json": SOURCE_AUDIT})
        got = ap.tiers_for({"id": "A", "evidence": ["livekit-rs-docs/evidence/s.json"]}, root)
        self.assertEqual(got["max_tier"], 1)
        self.assertEqual(got["levels"]["config_projection"], "NOT_RUN")

    def test_parse_test_reaches_tier_two_not_three(self):
        root = repo({"crates/lk-config/tests/config_leaf_parsing.rs": "fn t() {}",
                     "livekit-rs-docs/evidence/s.json": SOURCE_AUDIT})
        got = ap.tiers_for({"id": "A", "evidence": [
            "livekit-rs-docs/evidence/s.json",
            "crates/lk-config/tests/config_leaf_parsing.rs::yaml_broad"]}, root)
        self.assertEqual(got["max_tier"], 2, "parsing alone must not attest behavior")
        self.assertEqual(got["levels"]["component_behavior"], "NOT_RUN")

    def test_component_test_reaches_tier_three(self):
        root = repo({"crates/livecloud/src/foo.rs": "mod tests { fn real_behavior() {} }",
                     "livekit-rs-docs/evidence/s.json": SOURCE_AUDIT})
        got = ap.tiers_for({"id": "A", "evidence": [
            "livekit-rs-docs/evidence/s.json",
            "crates/livecloud/src/foo.rs::real_behavior"]}, root)
        self.assertEqual(got["max_tier"], 3)
        self.assertEqual(got["levels"]["main_server_consumer"], "NOT_RUN")

    def test_rust_binary_identity_reaches_tier_four(self):
        root = repo({"livekit-rs-docs/evidence/r.json": RUST_ONLY})
        got = ap.tiers_for({"id": ITEM, "evidence": ["livekit-rs-docs/evidence/r.json"]}, root)
        self.assertEqual(got["max_tier"], 4)
        self.assertEqual(got["levels"]["main_server_consumer"], "MET")
        self.assertEqual(got["levels"]["official_differential_lifecycle"], "NOT_RUN")

    def test_both_identities_reach_tier_five(self):
        root = repo({"livekit-rs-docs/evidence/p.json": PAIRED})
        got = ap.tiers_for({"id": ITEM, "evidence": ["livekit-rs-docs/evidence/p.json"]}, root)
        self.assertEqual(got["max_tier"], 5)
        self.assertEqual(got["levels"]["official_differential_lifecycle"], "MET")

    def test_interop_fixture_counts_as_main_consumer(self):
        root = repo({"tests/interop/x/run-paired.py": "leaf = 'limit.max_attributes_size'"})
        got = ap.tiers_for({"id": "YAML:limit.max_attributes_size",
                            "evidence": ["tests/interop/x/run-paired.py"]}, root)
        self.assertGreaterEqual(got["max_tier"], 4)

    def test_interop_fixture_that_never_names_the_leaf_gives_no_consumer_tier(self):
        root = repo({"tests/interop/x/run-paired.py": "print('unrelated leaf')",
                     "livekit-rs-docs/evidence/s.json": SOURCE_AUDIT})
        got = ap.tiers_for({"id": "YAML:limit.max_attributes_size",
                            "evidence": ["tests/interop/x/run-paired.py",
                                         "livekit-rs-docs/evidence/s.json"]}, root)
        self.assertEqual(got["max_tier"], 1)

    def test_failing_receipt_gives_no_tier_credit(self):
        root = repo({"livekit-rs-docs/evidence/f.json": json.dumps(
            {"status": "FAIL", "cases": [ITEM],
             "oracle_binary_sha256": "a", "candidate_binary_sha256": "b"})})
        got = ap.tiers_for({"id": ITEM, "evidence": ["livekit-rs-docs/evidence/f.json"]}, root)
        self.assertEqual(got["max_tier"], 0)

    def test_bare_file_reference_does_not_attest_behavior(self):
        """Citing a source file without naming a test proves nothing ran."""
        root = repo({"crates/livecloud/src/foo.rs": "fn unrelated() {}",
                     "livekit-rs-docs/evidence/s.json": SOURCE_AUDIT})
        got = ap.tiers_for({"id": "A", "evidence": [
            "livekit-rs-docs/evidence/s.json", "crates/livecloud/src/foo.rs"]}, root)
        self.assertEqual(got["max_tier"], 1)

    def test_nonexistent_test_name_does_not_attest_behavior(self):
        root = repo({"crates/livecloud/src/foo.rs": "fn real_one() {}",
                     "livekit-rs-docs/evidence/s.json": SOURCE_AUDIT})
        got = ap.tiers_for({"id": "A", "evidence": [
            "livekit-rs-docs/evidence/s.json",
            "crates/livecloud/src/foo.rs::never_written_test"]}, root)
        self.assertEqual(got["max_tier"], 1)

    def test_f06_style_receipt_reaches_tier_five(self):
        """The real batch-f06 shape: a `binaries` path map hashing both servers."""
        receipt = json.dumps({
            "status": "PASS_SCOPED_IOINFO_STORE_MAIN_HTTP_WEBHOOK_SLICE",
            "binaries": {"livekit-rs-server/target/debug/livecloud": RUST_SHA,
                         "/tmp/livekit-rs-f06-official": GO_SHA},
            "public_differential": {"registered_rpc_decode_auth_cases": 464,
                                    "differences": 0},
            "cases": ["Egress.ListEgress"],
        })
        root = repo({"livekit-rs-docs/evidence/f06.json": receipt})
        got = ap.tiers_for({"id": "API:Egress.ListEgress",
                            "evidence": ["livekit-rs-docs/evidence/f06.json"]}, root)
        self.assertEqual(got["max_tier"], 5)

    def test_aggregate_receipt_without_item_name_does_not_attest_consumption(self):
        """A paired total that never says which leaf ran supports no item."""
        receipt = json.dumps({
            "status": "PASS_SLICE",
            "binaries": {"livekit-rs-server/target/debug/livecloud": RUST_SHA,
                         "/tmp/livekit-rs-f06-official": GO_SHA},
            "public_differential": {"registered_rpc_decode_auth_cases": 464,
                                    "differences": 0},
        })
        root = repo({"livekit-rs-docs/evidence/f06-agg.json": receipt})
        got = ap.tiers_for({"id": "API:Egress.ListEgress",
                            "evidence": ["livekit-rs-docs/evidence/f06-agg.json"]}, root)
        self.assertLess(got["max_tier"], 4)
        self.assertEqual(got["levels"]["main_server_consumer"], "NOT_RUN")
        self.assertEqual(got["levels"]["official_differential_lifecycle"], "NOT_RUN")

    def test_batch_receipt_naming_the_leaf_reaches_tier_five(self):
        """2026-11-02 BATCH.json form: `batch` carries workstream + item id."""
        receipt = json.dumps({
            "batch": "W1-CLI-CONFIG/generated_flags:--room.empty_timeout",
            "oracle_binary_sha256": GO_SHA, "candidate_binary_sha256": RUST_SHA,
            "harness": {"runner": "livekit-rs-server/tests/interop/x/run-paired.py"},
            "paired_cases": 3,
        })
        root = repo({"livekit-rs-docs/evidence/b/BATCH.json": receipt})
        got = ap.tiers_for({"id": "generated_flags:--room.empty_timeout",
                            "evidence": ["livekit-rs-docs/evidence/b/BATCH.json"]}, root)
        self.assertEqual(got["max_tier"], 5)

    def test_flag_run_does_not_name_the_yaml_leaf_item(self):
        """`--logging.level` and `YAML:logging.level` are different ledger items."""
        receipt = json.dumps({
            "batch": "W1-CLI-CONFIG/generated_flags:--logging.json+--logging.level",
            "oracle_binary_sha256": GO_SHA, "candidate_binary_sha256": RUST_SHA,
            "paired_cases": 3,
        })
        root = repo({"livekit-rs-docs/evidence/logging-json-level/BATCH.json": receipt})
        ref = "livekit-rs-docs/evidence/logging-json-level/BATCH.json"
        self.assertEqual(ap.tiers_for(
            {"id": "generated_flags:--logging.level", "evidence": [ref]},
            root)["max_tier"], 5)
        self.assertLess(ap.tiers_for(
            {"id": "YAML:logging.level", "evidence": [ref]}, root)["max_tier"], 5)

    def test_naming_a_longer_id_does_not_name_its_prefix(self):
        receipt = json.dumps({"cases": ["YAML:room.enable_remote_unmute"],
                              "oracle_binary_sha256": "a",
                              "candidate_binary_sha256": "b"})
        root = repo({"livekit-rs-docs/evidence/r.json": receipt})
        self.assertFalse(cpl.artifact_names_item(
            os.path.join(root, "livekit-rs-docs/evidence/r.json"),
            cpl.item_match_keys("YAML:room")))

    def test_rust_only_receipt_reaches_tier_four(self):
        receipt = json.dumps({"status": "PASS", "checks": {"build-bound": {}},
                              "cases": [ITEM],
                              "binaries": {"livekit-rs-server/target/debug/livecloud": RUST_SHA}})
        root = repo({"livekit-rs-docs/evidence/r.json": receipt})
        got = ap.tiers_for({"id": ITEM, "evidence": ["livekit-rs-docs/evidence/r.json"]}, root)
        self.assertEqual(got["max_tier"], 4)
        self.assertEqual(got["levels"]["component_behavior"], "MET")

    def test_monotone_fill(self):
        root = repo({"livekit-rs-docs/evidence/p.json": PAIRED})
        got = ap.tiers_for({"id": ITEM, "evidence": ["livekit-rs-docs/evidence/p.json"]}, root)
        self.assertEqual(list(got["levels"].values()), ["MET"] * 5)


class RefNormalization(unittest.TestCase):
    def test_resolvable_ref_is_left_alone(self):
        root = repo({"crates/a/foo.rs": "fn my_test_name() {}"})
        ref = "crates/a/foo.rs::my_test_name"
        self.assertEqual(ap.normalize_ref(ref, root), (ref, None))

    def test_colon_space_form_rewrites_when_identifier_exists(self):
        root = repo({"crates/a/foo.rs": "mod t { fn room_service_api_method_tests() {} }"})
        new, original = ap.normalize_ref(
            "crates/a/foo.rs: room_service_api_method_tests (auth+success)", root)
        self.assertTrue(new.endswith("::room_service_api_method_tests"), new)
        self.assertEqual(original, "crates/a/foo.rs: room_service_api_method_tests (auth+success)")

    def test_invented_identifier_is_not_normalized(self):
        """The identifier must literally be in the file, or we keep the old ref."""
        root = repo({"crates/a/foo.rs": "fn something_else() {}"})
        new, original = ap.normalize_ref("crates/a/foo.rs: never_written_test (x)", root)
        self.assertIsNotNone(original)
        self.assertNotIn("never_written_test", new)

    def test_pure_prose_stays_prose(self):
        root = repo({})
        ref = "All 21 DATA_PACKET children VERIFIED"
        self.assertEqual(ap.normalize_ref(ref, root), (ref, None))

    def test_crate_relative_path_is_repaired(self):
        root = repo({"livekit-rs-server/crates/lk-config/tests/leaves.rs":
                     "fn profile_options_are_real() {}"})
        new, original = ap.normalize_ref(
            "lk-config/tests/leaves.rs::profile_options_are_real", root)
        self.assertEqual(new, "livekit-rs-server/crates/lk-config/tests/leaves.rs"
                              "::profile_options_are_real")
        self.assertIsNotNone(original)

    def test_bare_basename_with_line_number_is_repaired_when_unique(self):
        root = repo({"livekit-rs-server/crates/lk-config/tests/only_one.rs":
                     "fn raw_key_loader() {}"})
        new, _ = ap.normalize_ref(
            "see only_one.rs L48 raw_key_loader on linux", root)
        self.assertEqual(new, "livekit-rs-server/crates/lk-config/tests/only_one.rs"
                              "::raw_key_loader")

    def test_ambiguous_basename_is_not_repaired(self):
        """Two files share the name, so the citation cannot be resolved safely."""
        root = repo({"livekit-rs-server/crates/a/encode.rs": "fn t_one() {}",
                     "livekit-rs-server/crates/b/encode.rs": "fn t_two() {}"})
        new, original = ap.normalize_ref("encode.rs::t_one", root)
        self.assertEqual(new, "encode.rs::t_one")
        self.assertIsNone(original)

    def test_normalize_records_audit_and_counts(self):
        root = repo({"crates/a/foo.rs": "mod t { fn real_test_name_here() {} }"})
        items = [{"id": "A:x", "state": "PARTIAL", "surface": "s",
                  "evidence": ["crates/a/foo.rs: real_test_name_here (covers it)"]}]
        self.assertEqual(ap.normalize_evidence(items, root), 1)
        self.assertTrue(items[0]["evidence"][0].endswith("::real_test_name_here"))
        self.assertEqual(items[0]["tier_audit"]["ref_normalized"][0]["from"],
                         "crates/a/foo.rs: real_test_name_here (covers it)")


class Migration(unittest.TestCase):
    def _ledger(self, root, items):
        path = os.path.join(root, "ledger.json")
        with open(path, "w") as handle:
            json.dump({"items": items}, handle)
        return path

    def test_downgrades_verified_to_attested_state(self):
        root = repo({"crates/lk-config/tests/config_leaf_parsing.rs": "fn y() {}"})
        ledger = self._ledger(root, [{
            "id": "YAML:port", "state": "VERIFIED", "surface": "yaml",
            "evidence": ["crates/lk-config/tests/config_leaf_parsing.rs::yaml_broad"]}])
        out = ap.migrate(ledger, root, apply=False)
        self.assertEqual(out["state_counts_after"]["IMPLEMENTED_UNVERIFIED"], 1)
        self.assertEqual(out["state_counts_after"].get("VERIFIED", 0), 0)

    def test_never_raises_an_open_item(self):
        root = repo({"livekit-rs-docs/evidence/p.json": paired_receipt("API:Later")})
        ledger = self._ledger(root, [{
            "id": "API:Later", "state": "OPEN", "surface": "twirp",
            "evidence": ["livekit-rs-docs/evidence/p.json"]}])
        out = ap.migrate(ledger, root, apply=False)
        self.assertEqual(out["state_counts_after"].get("OPEN"), 1)
        self.assertEqual(out["state_counts_after"].get("VERIFIED", 0), 0)

    def test_preserves_prior_state_label(self):
        root = repo({})
        ledger = self._ledger(root, [{"id": "API:V", "state": "VERIFIED",
                                      "surface": "twirp", "evidence": []}])
        with open(ledger) as handle:
            before = json.load(handle)
        ap.migrate(ledger, root, apply=True)
        with open(ledger) as handle:
            after = json.load(handle)
        self.assertEqual(before["items"][0]["state"], "VERIFIED")
        self.assertEqual(after["items"][0]["state_before_tier_audit"], "VERIFIED")
        self.assertEqual(after["items"][0]["tier_audit"]["rule"], "SPEC-0075 §9.1")

    def test_hand_declared_tiers_are_not_reinferred(self):
        root = repo({"crates/livecloud/src/foo.rs": "fn a_test_here() {}"})
        declared = {t: "MET" for t in ap.TIERS}
        declared["official_differential_lifecycle"] = "PARTIAL"
        ledger = self._ledger(root, [{
            "id": "CONFIG_BEHAVIOR:Pilot", "state": "PARTIAL", "surface": "config",
            "evidence": ["crates/livecloud/src/foo.rs::a_test_here"],
            "verification_levels": declared}])
        ap.migrate(ledger, root, apply=True)
        with open(ledger) as handle:
            after = json.load(handle)
        self.assertEqual(after["items"][0]["verification_levels"], declared)
        self.assertEqual(after["items"][0]["state"], "PARTIAL")

    def test_dry_run_does_not_touch_the_file(self):
        root = repo({})
        ledger = self._ledger(root, [{"id": "API:V", "state": "VERIFIED",
                                      "surface": "twirp", "evidence": []}])
        raw = open(ledger).read()
        ap.migrate(ledger, root, apply=False)
        self.assertEqual(open(ledger).read(), raw)

    def test_upstream_not_applicable_is_untouched(self):
        root = repo({})
        ledger = self._ledger(root, [{"id": "API:NA", "state": "UPSTREAM_NOT_APPLICABLE",
                                      "surface": "twirp", "evidence": []}])
        ap.migrate(ledger, root, apply=True)
        with open(ledger) as handle:
            after = json.load(handle)
        self.assertEqual(after["items"][0]["state"], "UPSTREAM_NOT_APPLICABLE")


class ReceiptAttribution(unittest.TestCase):
    """Existing paired evidence gets wired to the item it already names."""

    def _receipts(self, root):
        return ap.paired_receipt_index(root)

    def test_links_receipt_that_names_the_item(self):
        root = repo({"livekit-rs-docs/evidence/x/BATCH.json": json.dumps(
            {"batch": "W1-CLI-CONFIG/YAML:logging.level",
             "oracle_binary_sha256": GO_SHA, "candidate_binary_sha256": RUST_SHA})})
        items = [{"id": "YAML:logging.level", "state": "PARTIAL", "surface": "yaml",
                  "evidence": []}]
        self.assertEqual(ap.attribute_paired_receipts(items, self._receipts(root)), 1)
        self.assertEqual(items[0]["evidence"],
                         ["livekit-rs-docs/evidence/x/BATCH.json"])
        self.assertEqual(
            items[0]["tier_audit"]["receipt_attributed"],
            ["livekit-rs-docs/evidence/x/BATCH.json"])

    def test_flag_run_is_not_attributed_to_the_yaml_leaf(self):
        root = repo({"livekit-rs-docs/evidence/x/BATCH.json": json.dumps(
            {"batch": "W1-CLI-CONFIG/generated_flags:--logging.level",
             "oracle_binary_sha256": GO_SHA, "candidate_binary_sha256": RUST_SHA})})
        items = [{"id": "YAML:logging.level", "state": "PARTIAL", "surface": "yaml",
                  "evidence": []}]
        self.assertEqual(ap.attribute_paired_receipts(items, self._receipts(root)), 0)
        self.assertEqual(items[0]["evidence"], [])

    def test_failing_receipt_is_never_attributed(self):
        root = repo({"livekit-rs-docs/evidence/x/BATCH.json": json.dumps(
            {"status": "FAIL", "batch": "API:RoomService.ListRooms",
             "oracle_binary_sha256": GO_SHA, "candidate_binary_sha256": RUST_SHA})})
        items = [{"id": "API:RoomService.ListRooms", "state": "PARTIAL",
                  "surface": "twirp", "evidence": []}]
        self.assertEqual(ap.attribute_paired_receipts(items, self._receipts(root)), 0)

    def test_already_cited_receipt_is_not_duplicated(self):
        rel = "livekit-rs-docs/evidence/x/BATCH.json"
        root = repo({rel: json.dumps({"batch": "API:RoomService.ListRooms",
                                      "oracle_binary_sha256": GO_SHA,
                                      "candidate_binary_sha256": RUST_SHA})})
        items = [{"id": "API:RoomService.ListRooms", "state": "PARTIAL",
                  "surface": "twirp", "evidence": [rel]}]
        self.assertEqual(ap.attribute_paired_receipts(items, self._receipts(root)), 0)
        self.assertEqual(items[0]["evidence"], [rel])


if __name__ == "__main__":
    unittest.main(verbosity=2)
