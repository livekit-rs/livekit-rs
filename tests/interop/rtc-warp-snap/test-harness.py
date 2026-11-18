#!/usr/bin/env python3
import ast
import contextlib
import hashlib
import io
import json
import os
import time
from pathlib import Path
import runpy
import signal
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

HERE = Path(__file__).resolve().parent
SCRIPT = HERE / 'run-paired.py'
REAL_SHA = hashlib.sha256
FIXED_GO_SHA = '4bc2378e636740b29093061036249ffc9ebbef91f4ece1de12177c8dfee1e7dc'


class HarnessTests(unittest.TestCase):
    def campaign(self, corruption=None, oracle_valid=True):
        with tempfile.TemporaryDirectory(prefix='c51-harness-test-') as tmp:
            root = Path(tmp)
            for name in ('go', 'rust', 'observer'):
                (root / name).write_bytes(name.encode())
            output = root / 'output'
            argv = [str(SCRIPT), '--go', str(root / 'go'), '--rust', str(root / 'rust'),
                    '--observer', str(root / 'observer'), '--output', str(output)]

            def digest(data):
                if data == b'go' and oracle_valid:
                    class Fixed:
                        def hexdigest(self):
                            return FIXED_GO_SHA
                    return Fixed()
                return REAL_SHA(data)

            class Observer:
                def __init__(self, cmd, **kwargs):
                    self.returncode = 0
                    side = Path(cmd[cmd.index('--binary') + 1]).name
                    profile = cmd[cmd.index('--profile') + 1]
                    path = Path(cmd[cmd.index('--output') + 1])
                    path.mkdir()
                    identity = digest(side.encode()).hexdigest()
                    result = dict(refused=False, server_exit=0, signal_upgrade_status=101,
                                  join_received=True, offer_received=True,
                                  application_present=True, sctp_init_present=profile == 'warp-on',
                                  server_ports_released=True, binary_unchanged=True,
                                  binary_sha256_before=identity, binary_sha256_after=identity)
                    if corruption:
                        corruption(result, side, profile)
                    (path / 'result.json').write_text(json.dumps(result))

                def communicate(self, timeout=None):
                    return '', ''

            with patch.object(sys, 'argv', argv), patch('subprocess.Popen', Observer), \
                    patch('hashlib.sha256', side_effect=digest), patch('signal.signal'), \
                    contextlib.redirect_stdout(io.StringIO()):
                try:
                    runpy.run_path(str(SCRIPT), run_name='__main__')
                except SystemExit as exc:
                    status = exc.code
            receipt = json.loads((output / 'BATCH.json').read_text()) if (output / 'BATCH.json').exists() else None
            return status, receipt

    def test_valid_campaign(self):
        status, receipt = self.campaign()
        self.assertEqual(status, 0)
        self.assertTrue(all(receipt['cross_profile_controls'].values()))

    def test_wrong_oracle_rejected_before_launch(self):
        status, receipt = self.campaign(oracle_valid=False)
        self.assertEqual(status, 'fixed Go oracle identity mismatch')
        self.assertIsNone(receipt)

    def test_between_profile_binary_replacement_rejected(self):
        def corrupt(result, side, profile):
            if side == 'rust' and profile == 'warp-off':
                result['binary_sha256_before'] = 'b' * 64
                result['binary_sha256_after'] = 'b' * 64
        status, receipt = self.campaign(corrupt)
        self.assertEqual(status, 1)
        self.assertEqual(receipt['failed_profiles'], ['warp-off'])

    def test_missing_identity_rejected(self):
        status, receipt = self.campaign(lambda r, s, p: r.pop('binary_sha256_before'))
        self.assertEqual(status, 1)
        self.assertEqual(len(receipt['failed_profiles']), 2)

    def test_accept_but_ignore_is_specific(self):
        def corrupt(result, side, profile):
            if side == 'rust':
                result['sctp_init_present'] = False
        status, receipt = self.campaign(corrupt)
        self.assertEqual(status, 1)
        self.assertEqual(receipt['failed_profiles'], ['warp-on'])
        self.assertTrue(receipt['profiles']['warp-off']['match'])

    def test_missing_application_is_not_a_pass(self):
        status, receipt = self.campaign(lambda r, s, p: r.update(application_present=False))
        self.assertEqual(status, 1)
        self.assertEqual(len(receipt['failed_profiles']), 2)

    @unittest.skipUnless(os.environ.get('OBSERVER_BIN'), 'set OBSERVER_BIN for real cancellation test')
    def test_observer_sigterm_reaps_daemon(self):
        with tempfile.TemporaryDirectory(prefix='c51-cancel-') as tmp:
            root = Path(tmp)
            pid_file = root / 'daemon.pid'
            daemon = root / 'daemon'
            daemon.write_text(f'#!{sys.executable}\nimport os, time\nfrom pathlib import Path\nPath({str(pid_file)!r}).write_text(str(os.getpid()))\ntime.sleep(120)\n')
            daemon.chmod(0o700)
            proc = subprocess.Popen([os.environ['OBSERVER_BIN'], '--binary', str(daemon),
                                     '--output', str(root / 'result'), '--profile', 'cancel'])
            try:
                deadline = time.monotonic() + 10
                while not pid_file.exists() and time.monotonic() < deadline:
                    time.sleep(0.02)
                self.assertTrue(pid_file.exists(), 'daemon was never launched')
                pid = int(pid_file.read_text())
                proc.send_signal(signal.SIGTERM)
                self.assertEqual(proc.wait(timeout=25), 1)
                with self.assertRaises(ProcessLookupError):
                    os.kill(pid, 0)
                result = json.loads((root / 'result' / 'result.json').read_text())
                self.assertTrue(result['server_ports_released'])
                self.assertIn('context canceled', result['error'])
            finally:
                if proc.poll() is None:
                    proc.send_signal(signal.SIGTERM)
                    proc.wait(timeout=25)

    @unittest.skipUnless(os.environ.get('GO_BIN'), 'set GO_BIN for real wrapper cancellation test')
    def test_wrapper_sigterm_reaps_process_group(self):
        with tempfile.TemporaryDirectory(prefix='c51-wrapper-cancel-') as tmp:
            root = Path(tmp)
            pid_file = root / 'child.pid'
            observer = root / 'observer'
            observer.write_text(f'#!{sys.executable}\nimport subprocess, sys\nfrom pathlib import Path\np = subprocess.Popen([sys.executable, "-c", "import time; time.sleep(120)"])\nPath({str(pid_file)!r}).write_text(str(p.pid))\np.wait()\n')
            observer.chmod(0o700)
            proc = subprocess.Popen([sys.executable, str(SCRIPT), '--go', os.environ['GO_BIN'],
                                     '--rust', str(observer), '--observer', str(observer),
                                     '--output', str(root / 'result')])
            try:
                deadline = time.monotonic() + 10
                while not pid_file.exists() and time.monotonic() < deadline:
                    time.sleep(0.02)
                self.assertTrue(pid_file.exists(), 'observer child was never launched')
                pid = int(pid_file.read_text())
                proc.send_signal(signal.SIGTERM)
                self.assertEqual(proc.wait(timeout=35), 143)
                deadline = time.monotonic() + 5
                while time.monotonic() < deadline:
                    try:
                        os.kill(pid, 0)
                    except ProcessLookupError:
                        break
                    time.sleep(0.02)
                else:
                    self.fail('observer child survived cancellation')
            finally:
                if proc.poll() is None:
                    proc.send_signal(signal.SIGTERM)
                    proc.wait(timeout=35)

    def test_private_ip_redaction(self):
        tree = ast.parse(SCRIPT.read_text())
        selected = []
        for node in tree.body:
            if isinstance(node, (ast.Import, ast.FunctionDef)):
                selected.append(node)
            elif isinstance(node, ast.Assign) and any(isinstance(t, ast.Name) and t.id == '_PRIVATE_IP' for t in node.targets):
                selected.append(node)
        namespace = {}
        exec(compile(ast.Module(body=selected, type_ignores=[]), str(SCRIPT), 'exec'), namespace)
        redact = namespace['redact_private_ips']
        for address in ('10.23.45.67', '172.16.45.67', '172.31.45.67', '192.168.45.67'):
            self.assertEqual(redact(address), '192.0.2.67')
        for address in ('127.0.0.1', '192.0.2.67', '172.15.45.67', '172.32.45.67'):
            self.assertEqual(redact(address), address)


if __name__ == '__main__':
    unittest.main()
