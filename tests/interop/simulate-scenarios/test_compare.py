import copy
import unittest

from run import compare


class CompareTests(unittest.TestCase):
    def setUp(self):
        evidence = {
            "sdkSha256": "same-sdk",
            "scenarios": {
                "force-tcp": {
                    "audioBefore": True,
                    "dataBefore": True,
                    "roomDeleted": True,
                    "finalState": "connected",
                    "audioAfter": True,
                    "dataAfter": True,
                    "protocols": ["tcp"],
                }
            },
        }
        self.runs = {
            name: {
                "binary_sha256": name,
                "probe_exit": 0,
                "server_exit": 0,
                "ports_released": True,
                "evidence": copy.deepcopy(evidence),
            }
            for name in ("go", "rust")
        }

    def check(self):
        return compare(self.runs, ["force-tcp"], "true")["match"]

    def test_equal_complete_results_match(self):
        self.assertTrue(self.check())

    def test_requested_scenario_must_be_present_for_both(self):
        for run in self.runs.values():
            run["evidence"]["scenarios"].clear()
        self.assertFalse(self.check())
        self.assertFalse(compare(self.runs, [], "true")["match"])

    def test_nonzero_probe_exit_cannot_match(self):
        self.runs["rust"]["probe_exit"] = 1
        self.assertFalse(self.check())

    def test_unreleased_ports_cannot_match(self):
        self.runs["go"]["ports_released"] = False
        self.assertFalse(self.check())

    def test_different_sdk_bytes_cannot_match(self):
        self.runs["rust"]["evidence"]["sdkSha256"] = "different-sdk"
        self.assertFalse(self.check())

    def test_equal_bad_baselines_and_cleanup_cannot_match(self):
        for field in ("audioBefore", "dataBefore", "roomDeleted"):
            with self.subTest(field=field):
                rows = [run["evidence"]["scenarios"]["force-tcp"] for run in self.runs.values()]
                for row in rows:
                    row[field] = False
                self.assertFalse(self.check())
                for row in rows:
                    row[field] = True

    def test_missing_or_duplicate_scenarios_cannot_match(self):
        self.assertFalse(compare(self.runs, ["force-tcp", ""], "true")["match"])
        self.assertFalse(compare(self.runs, ["force-tcp", "force-tcp"], "true")["match"])

    def test_equal_connected_states_without_recovered_media_cannot_match(self):
        for run in self.runs.values():
            run["evidence"]["scenarios"]["force-tcp"]["audioAfter"] = False
        self.assertFalse(self.check())

    def test_coalesced_profile_requires_the_wire_delivery_receipt(self):
        for run in self.runs.values():
            scenarios = run["evidence"]["scenarios"]
            scenarios["force-tls"] = scenarios.pop("force-tcp")
            scenarios["force-tls"]["signalDelivery"] = [
                {"connection": 1},
                {"connection": 2, "flush": "pair", "descriptionKinds": ["answer", "offer"]},
            ]
        self.assertTrue(compare(self.runs, ["force-tls"], "false", True)["match"])
        delivery = self.runs["rust"]["evidence"]["scenarios"]["force-tls"]["signalDelivery"]
        delivery[1]["flush"] = "bounded-timeout"
        self.assertFalse(compare(self.runs, ["force-tls"], "false", True)["match"])


if __name__ == "__main__":
    unittest.main()
