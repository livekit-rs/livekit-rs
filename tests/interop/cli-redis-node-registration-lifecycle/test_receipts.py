"""Receipt policy tests only: no Redis, binary execution, socket or media process."""
import contextlib
import importlib.util
import io
import json
import pathlib
import tempfile
import types
import unittest
from unittest.mock import patch

RUNNER = pathlib.Path(__file__).with_name('run-paired.py')
spec = importlib.util.spec_from_file_location('node_lifecycle_runner', RUNNER)
runner = importlib.util.module_from_spec(spec)
spec.loader.exec_module(runner)


class ReceiptPolicy(unittest.TestCase):
    def run_campaign(self, only=None, failure=None, mutate=False, cleanup_failure=False):
        with tempfile.TemporaryDirectory(prefix='node-receipt-policy-') as directory:
            root = pathlib.Path(directory)
            official, rust = root / 'official', root / 'rust'
            official.write_bytes(b'reference-fixture')
            rust.write_bytes(b'candidate-fixture')
            output = root / 'evidence'
            argv = ['run-paired.py', '--official', str(official), '--rust', str(rust),
                    '--output', str(output)]
            if only:
                argv += ['--only', only]
            def vector(*_):
                if failure:
                    raise failure
                if mutate:
                    rust.write_bytes(b'changed-input')
                return {'mocked_execution': True}
            with contextlib.ExitStack() as stack:
                stack.enter_context(patch('sys.argv', argv))
                stack.enter_context(patch.object(runner, 'OFFICIAL_SHA256', runner.sha(official.read_bytes())))
                stack.enter_context(patch.object(runner, 'run_checked', return_value=types.SimpleNamespace(stdout=runner.OFFICIAL_VERSION)))
                stack.enter_context(patch.object(runner, 'free', return_value=True))
                stack.enter_context(patch.object(runner, 'redis_server', return_value=None))
                stop = stack.enter_context(patch.object(runner, 'stop_process'))
                if cleanup_failure:
                    stop.side_effect = RuntimeError('owned cleanup failed')
                for name in vars(runner):
                    if name.startswith('vector_'):
                        stack.enter_context(patch.object(runner, name, vector))
                stack.enter_context(contextlib.redirect_stdout(io.StringIO()))
                code = runner.main()
            self.assertFalse(output.joinpath('BATCH.json.pending').exists())
            report = json.loads(output.joinpath('BATCH.json').read_text())
            self.assertIsNotNone(report['finished_at_unix'])
            return code, report

    def test_full_campaign_and_selected_scope_are_distinct(self):
        code, full = self.run_campaign()
        self.assertEqual(code, 0)
        self.assertTrue(full['complete_campaign'])
        self.assertEqual(full['status'], 'PASS_CLI_REDIS_NODE_REGISTRATION_LIFECYCLE_PAIRED')
        self.assertEqual(len(full['cases']), 11)
        code, subset = self.run_campaign(only='register-refresh-revoke')
        self.assertEqual(code, 0)
        self.assertFalse(subset['complete_campaign'])
        self.assertEqual(subset['status'], 'PASS_SELECTED_VECTORS_ONLY')
        self.assertEqual(len(subset['cases']), 1)

    def test_interruption_is_failed_receipt(self):
        code, report = self.run_campaign(failure=InterruptedError('mock termination'))
        self.assertEqual(code, 1)
        self.assertEqual(report['status'], 'FAIL')
        self.assertFalse(report['complete_campaign'])
        self.assertIn('InterruptedError', report['failure'])
        self.assertFalse(report['cases'])

    def test_candidate_mutation_cannot_be_promoted(self):
        code, report = self.run_campaign(mutate=True)
        self.assertEqual(code, 1)
        self.assertEqual(report['status'], 'FAIL')
        self.assertIn('input changed', report['failure'])
        self.assertFalse(report['complete_campaign'])

    def test_cleanup_failure_revokes_aggregate_pass(self):
        code, report = self.run_campaign(cleanup_failure=True)
        self.assertEqual(code, 1)
        self.assertEqual(report['status'], 'FAIL')
        self.assertFalse(report['complete_campaign'])
        self.assertNotIn('aggregate_status', report)
        self.assertTrue(report['cleanup_errors'])

    def test_version_label_cannot_replace_locked_reference_identity(self):
        with tempfile.TemporaryDirectory(prefix='node-receipt-identity-') as directory:
            root = pathlib.Path(directory)
            binary = root / 'untrusted-reference'
            binary.write_bytes(b'untrusted')
            with patch('sys.argv', ['runner', '--official', str(binary), '--rust', str(binary), '--output', str(root / 'evidence')]), patch.object(runner, 'run_checked') as execute:
                with self.assertRaisesRegex(AssertionError, 'locked reference build'):
                    runner.main()
                execute.assert_not_called()
            self.assertFalse(root.joinpath('evidence').exists())


if __name__ == '__main__':
    unittest.main()
