#!/usr/bin/env python3
"""Offline fixture checks: synthetic data/mocked processes, not differential evidence."""
import contextlib
import copy
import importlib.util
import io
import json
from pathlib import Path
import signal
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import Mock, patch

HERE = Path(__file__).resolve().parent
SPEC = importlib.util.spec_from_file_location('layer_harness', HERE / 'run-paired.py')
HARNESS = importlib.util.module_from_spec(SPEC)
# Execute source without producing __pycache__ in the fixture directory.
exec(compile((HERE / 'run-paired.py').read_text(), str(HERE / 'run-paired.py'), 'exec'), HARNESS.__dict__)


def synthetic_row(case_id):
    return {'id': case_id, 'status': 'PASS', 'expected': {'layers': [], 'codecs': []},
            'observed': {'layers': [], 'codecs': []}, 'differences': [], 'join_owner_ok': True,
            'response': {'type': 'trackPublished', 'track_sid_present': True},
            'track_published_count': 1, 'ws_healthy': True, 'ws_close_code': 1000,
            'barrier': {'reason': 0, 'owner_metadata_matches': True}}


def synthetic_records():
    return {side: {'result': {'side': side, 'status': 'PASS_SCOPED', 'case_count': len(HARNESS.CASE_IDS),
                             'cleanup_errors': [], 'cases': [synthetic_row(key) for key in HARNESS.CASE_IDS]}}
            for side in ('go', 'rust')}


class HarnessTests(unittest.TestCase):
    def test_log_redaction_retains_failures_without_secrets(self):
        raw = b'failure marker 10.1.2.3 192.168.7.9 172.31.2.3 127.0.0.1 secret-value eyJabc.def.ghi'
        result = HARNESS.redact_log(raw, 'secret-value')
        self.assertEqual(result, 'failure marker <redacted-private-ip> <redacted-private-ip> <redacted-private-ip> 127.0.0.1 <redacted-secret> <redacted-jwt>')

    def test_original_protocol_and_metadata_mutations_without_network(self):
        script = r'''
import assert from 'node:assert/strict';
import { createRequire } from 'node:module';
const p = await import(process.argv[1]);
const loader = createRequire(process.argv[1]);
const { SignalResponse } = loader('@livekit/protocol');
assert.deepEqual(p.CASES.map(c => c.id), JSON.parse(process.argv[2]));
const seen = new Set();
for (const spec of p.CASES) {
  const before = JSON.stringify(spec);
  p.addTrack(spec); // Original protocol signed enum binary roundtrip.
  const expected = p.expectedMetadata(spec);
  const response = SignalResponse.fromJson({ trackPublished: {
    cid: spec.cid, track: { sid: 'synthetic-unit-test-only', ...expected },
  }});
  const decoded = SignalResponse.fromBinary(response.toBinary()).message.value.track;
  const observed = p.observedMetadata(decoded);
  assert.deepEqual(observed, expected);
  assert.deepEqual(p.differences(expected, observed), []);
  for (const group of [spec.layers, ...spec.simulcastCodecs.map(c => c.layers)]) {
    for (const layer of group) {
      assert(!seen.has(layer.rid));
      seen.add(layer.rid);
    }
  }
  for (const select of [v => v.layers, ...expected.codecs.map((_, i) => v => v.codecs[i].layers)]) {
    if (!select(expected).length) continue;
    for (const field of Object.keys(select(expected)[0])) {
      const corrupt = structuredClone(observed);
      const layer = select(corrupt)[0];
      layer[field] = typeof layer[field] === 'number' ? layer[field] + 1 : `${layer[field]}-lost`;
      assert(p.differences(expected, corrupt).some(d => d.path.endsWith(`.${field}`)), field);
    }
    const dropped = structuredClone(observed);
    select(dropped).pop();
    assert(p.differences(expected, dropped).length > 0);
    if (select(observed).length > 1) {
      const wrongOrder = structuredClone(observed);
      select(wrongOrder).reverse();
      assert(p.differences(expected, wrongOrder).length > 0);
    }
  }
  assert.equal(JSON.stringify(spec), before, 'oracle must not mutate input');
}
const signed = p.expectedMetadata(p.CASES.find(c => c.id === 'signed-codec-specific'));
for (const group of [signed.layers, ...signed.codecs.map(c => c.layers)]) {
  assert.deepEqual(group.map(l => l.quality), [-2147483648, -1, 0, 2, 77, 2147483647]);
}
const specific = p.CASES.find(c => c.id === 'vp8-specific-unordered');
const unsorted = p.expectedMetadata(specific);
unsorted.codecs[0].layers = structuredClone(specific.simulcastCodecs[0].layers);
assert(p.differences(p.expectedMetadata(specific), p.observedMetadata(unsorted)).length > 0);
const dual = p.expectedMetadata(p.CASES.find(c => c.id === 'dual-codec-distinct'));
const swapped = structuredClone(dual);
[swapped.codecs[0].layers, swapped.codecs[1].layers] = [swapped.codecs[1].layers, swapped.codecs[0].layers];
assert(p.differences(dual, swapped).length > 0);
'''
        result = subprocess.run(['node', '--input-type=module', '-e', script,
                                 (HERE / 'probe.mjs').as_uri(), json.dumps(HARNESS.CASE_IDS)],
                                capture_output=True, text=True, timeout=20)
        self.assertEqual(result.returncode, 0, result.stderr)

    def test_cross_equal_does_not_override_independent_contracts(self):
        records = synthetic_records()
        for side in ('go', 'rust'):
            records[side]['result']['cases'][0]['observed']['layers'] = [{'quality': 77}]
        cases, valid = HARNESS.paired_cases(records)
        self.assertTrue(all(valid.values()))
        self.assertTrue(cases[0]['cross_side_equal'])
        self.assertFalse(cases[0]['go_contract_ok'])
        self.assertFalse(cases[0]['rust_contract_ok'])
        self.assertEqual(cases[0]['status'], 'FAIL')
        self.assertTrue(all(case['status'] == 'PASS' for case in cases[1:]))

    def test_missing_and_duplicate_cases_are_not_synthesized(self):
        for duplicate in (False, True):
            records = synthetic_records()
            rows = records['rust']['result']['cases']
            if duplicate:
                rows.append(copy.deepcopy(rows[0]))
            else:
                rows.pop(0)
            cases, valid = HARNESS.paired_cases(records)
            self.assertFalse(valid['rust'])
            self.assertEqual(len(cases), len(HARNESS.CASE_IDS))
            self.assertIsNone(cases[0]['rust'])
            self.assertEqual(cases[0]['status'], 'FAIL')

    def test_owner_barrier_and_ws_cleanup_are_required(self):
        for mutation in ('barrier', 'ws_healthy', 'ws_close_code', 'track_published_count'):
            row = synthetic_row('test')
            row[mutation] = {} if mutation == 'barrier' else False
            self.assertFalse(HARNESS.independent_case_ok(row), mutation)

    def test_hash_and_port_release_are_required(self):
        identity = {'binary_sha256': 'a' * 64, 'executable_matches': True,
                    'owned_loopback_listener': True, 'owned_loopback_udp': True}
        cleanup = {'exit': 0, 'forced_kill': False, 'process_group_gone': True}
        record = {'probe_exit': 0, 'binary_sha256_before': 'a' * 64, 'binary_sha256_after': 'a' * 64,
                  'launch_before': copy.deepcopy(identity), 'launch_after': copy.deepcopy(identity),
                  'ports_rebound': {'http': True, 'udp': True},
                  'probe_cleanup': copy.deepcopy(cleanup), 'server_cleanup': copy.deepcopy(cleanup)}
        self.assertTrue(HARNESS.lifecycle_ok(record, 'a' * 64))
        for key in ('binary_sha256_after', 'launch_after', 'ports_rebound', 'server_cleanup'):
            corrupt = copy.deepcopy(record)
            corrupt.pop(key)
            self.assertFalse(HARNESS.lifecycle_ok(corrupt, 'a' * 64), key)

    def test_owned_process_timeout_escalates_and_reaps(self):
        proc = Mock(pid=23456)
        proc.wait.side_effect = [subprocess.TimeoutExpired('synthetic', 20), -signal.SIGKILL]
        with patch.object(HARNESS.os, 'killpg', side_effect=[None, None, ProcessLookupError()]) as kill:
            result = HARNESS.stop_owned(proc)
        self.assertTrue(result['forced_kill'])
        self.assertTrue(result['process_group_gone'])
        self.assertEqual(result['exit'], -signal.SIGKILL)
        self.assertEqual([call.args for call in kill.call_args_list],
                         [(proc.pid, signal.SIGTERM), (proc.pid, signal.SIGKILL), (proc.pid, 0)])

    def test_launch_cancellation_runs_finally_cleanup_without_real_process(self):
        with tempfile.TemporaryDirectory() as tmp:
            output = Path(tmp)
            binary = output / 'synthetic-binary'
            binary.write_bytes(b'not executed')
            reservations = [Mock(), Mock()]
            ports = {'http': 18001, 'udp': 18003}
            proc, record = Mock(pid=23456), {}
            with patch.object(HARNESS, 'reserve_ports', return_value=(ports, reservations)), \
                    patch.object(HARNESS.subprocess, 'Popen', return_value=proc), \
                    patch.object(HARNESS, 'wait_ready', side_effect=HARNESS.CheckFailure('cancelled')), \
                    patch.object(HARNESS, 'stop_owned', return_value={'exit': 0}) as stop, \
                    patch.object(HARNESS, 'rebind_ports', return_value={key: True for key in ports}) as rebind:
                HARNESS.run_side('go', binary, HARNESS.sha(binary), output, 'node', 'lsof', record)
            stop.assert_called_once_with(proc)
            rebind.assert_called_once_with(ports)
            self.assertEqual(record['failure']['check'], 'cancelled')
            self.assertEqual(record['binary_sha256_before'], record['binary_sha256_after'])

    def test_reference_cannot_impersonate_rust(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            binary = root / 'synthetic-reference'
            binary.write_bytes(b'\x7fELFnot-a-real-executable')
            binary.chmod(0o700)
            output = root / 'output'
            argv = ['run-paired.py', '--go', str(binary), '--rust', str(binary), '--output', str(output)]
            with patch.object(sys, 'argv', argv), patch.object(HARNESS, 'FIXED_GO_SHA', HARNESS.sha(binary)), \
                    patch.object(HARNESS.subprocess, 'Popen') as launch, contextlib.redirect_stdout(io.StringIO()):
                self.assertEqual(HARNESS.main(), 1)
            launch.assert_not_called()
            batch = json.loads((output / 'BATCH.json').read_text())
            self.assertEqual(batch['failure']['check'], 'rust_is_reference_binary')

    def test_wrong_go_identity_fails_before_any_launch(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            binary = root / 'synthetic-native'
            binary.write_bytes(b'\x7fELFnot-a-real-executable')
            binary.chmod(0o700)
            output = root / 'output'
            argv = ['run-paired.py', '--go', str(binary), '--rust', str(binary), '--output', str(output)]
            with patch.object(sys, 'argv', argv), patch.object(HARNESS.subprocess, 'Popen') as launch, \
                    contextlib.redirect_stdout(io.StringIO()):
                self.assertEqual(HARNESS.main(), 1)
            launch.assert_not_called()
            batch = json.loads((output / 'BATCH.json').read_text())
            self.assertEqual(batch['failure']['check'], 'fixed_go_sha_mismatch')
            self.assertEqual(batch['covers'], HARNESS.ITEMS)
            self.assertTrue(all(case['go'] is None and case['rust'] is None for case in batch['cases']))
            with patch.object(sys, 'argv', argv), self.assertRaises(FileExistsError):
                HARNESS.main()


if __name__ == '__main__':
    unittest.main()
