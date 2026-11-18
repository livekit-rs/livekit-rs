#!/usr/bin/env python3
"""Exercise the compiled Rust reader using actual Go rows and corrupted copies."""
import argparse
import copy
import json
import os
from pathlib import Path
import subprocess
import tempfile
import unittest

TEST = 'layers::rid_normalization_source_differential::normalized_rid_matrix'


class ReaderTests(unittest.TestCase):
    def run_rows(self, rows, output_exists=False):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source, output = root / 'go.jsonl', root / 'rust.jsonl'
            source.write_text(''.join(json.dumps(row) + '\n' for row in rows))
            if output_exists:
                output.write_text('do not overwrite\n')
            env = {**os.environ, 'VIDEO_RID_GO_RESULTS': str(source),
                   'VIDEO_RID_RUST_RESULTS': str(output)}
            result = subprocess.run([self.binary, '--exact', TEST, '--ignored', '--nocapture'],
                                    env=env, capture_output=True, text=True, timeout=30)
            text = output.read_text() if output.exists() else None
            return result, text

    def reject(self, rows):
        result, _ = self.run_rows(rows)
        self.assertEqual(result.returncode, 101, result.stdout + result.stderr)
        self.assertIn('1 failed', result.stdout)

    def test_original_rows_pass_and_output_all_observations(self):
        result, text = self.run_rows(self.rows)
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertIn('1 passed', result.stdout)
        self.assertEqual([json.loads(line) for line in text.splitlines()], self.rows)

    def test_missing_case_rejected(self):
        self.reject(self.rows[:-1])

    def test_duplicate_case_rejected(self):
        rows = copy.deepcopy(self.rows)
        rows[-1] = rows[0]
        self.reject(rows)

    def test_reordered_cases_rejected(self):
        self.reject(list(reversed(self.rows)))

    def test_wrong_input_rejected(self):
        rows = copy.deepcopy(self.rows)
        rows[0]['input'][1] = 'q'
        self.reject(rows)

    def test_each_normalized_slot_is_compared(self):
        for index in range(3):
            with self.subTest(index=index):
                rows = copy.deepcopy(self.rows)
                rows[0]['normalized'][index] = 'corrupted'
                self.reject(rows)

    def test_every_presence_and_quality_mapping_is_compared(self):
        for mask in range(8):
            for quality in range(3):
                with self.subTest(mask=mask, quality=quality):
                    rows = copy.deepcopy(self.rows)
                    rows[0]['mappings'][mask][quality] = 'corrupted'
                    self.reject(rows)

    def test_unexpected_field_rejected(self):
        rows = copy.deepcopy(self.rows)
        rows[0]['extra'] = True
        self.reject(rows)

    def test_missing_field_rejected(self):
        rows = copy.deepcopy(self.rows)
        del rows[0]['normalized']
        self.reject(rows)

    def test_existing_output_is_not_overwritten(self):
        result, text = self.run_rows(self.rows, output_exists=True)
        self.assertEqual(result.returncode, 101)
        self.assertEqual(text, 'do not overwrite\n')


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--binary', type=Path, required=True)
    parser.add_argument('--go-results', type=Path, required=True)
    args = parser.parse_args()
    ReaderTests.binary = str(args.binary.resolve(strict=True))
    ReaderTests.rows = [json.loads(line) for line in args.go_results.read_text().splitlines()]
    assert len(ReaderTests.rows) == 512
    unittest.main(argv=[__file__], verbosity=2)
