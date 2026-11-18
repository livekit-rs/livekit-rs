#!/usr/bin/env python3
"""Pure synthetic unit checks; no MAIN, browser, sockets or differential evidence."""
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
SPEC = importlib.util.spec_from_file_location('quality_runner', HERE / 'run-paired.py')
RUNNER = importlib.util.module_from_spec(SPEC)
exec(compile((HERE / 'run-paired.py').read_text(), str(HERE / 'run-paired.py'), 'exec'), RUNNER.__dict__)


def dependencies():
    return {'packages': [{'name': name, 'version': version, 'package_sha256': 'a' * 64, 'entry_sha256': 'b' * 64}
                         for name, version in RUNNER.PINS.items()],
            'package_lock_sha256': 'c' * 64, 'pins_sha256': 'd' * 64,
            'umd_entry': 'livekit-client/dist/livekit-client.umd.js'}


def row(case_id):
    return {'id': case_id, 'status': 'PASS',
            'observed': {'contract': {'vector': True, 'owner': True}, 'enabled': False,
                         'requested': {'qualities': ['low', 'high']}},
            'samples': [{'timestamp': 1000, 'bytesReceived': 6400, 'framesDecoded': 5}]}


def record(side):
    digest = RUNNER.FIXED_GO_SHA if side == 'go' else 'e' * 64
    identity = {'pid': 12345, 'binary_sha256': digest, 'config_sha256': 'f' * 64,
                'main_alive': True, 'executable_matches': True, 'owned_loopback_listener': True,
                'owned_loopback_udp': True, 'method': 'lsof_text_path_inode'}
    cleanup = {'exit': 0, 'forced_kill': False, 'process_group_gone': True}
    return {'pid': 12345, 'probe_exit': 0, 'binary_sha256_before': digest, 'binary_sha256_after': digest,
            'config_sha256': 'f' * 64, 'launch_before': copy.deepcopy(identity), 'launch_after': copy.deepcopy(identity),
            'probe_cleanup': copy.deepcopy(cleanup), 'server_cleanup': copy.deepcopy(cleanup),
            'ports_rebound': {'http': True, 'udp': True},
            'dependencies_before': dependencies(), 'dependencies_after': dependencies(),
            'fixture_sha256': {'client': '7' * 64, 'probe': '8' * 64},
            'probe_validation': {'exit': 0, 'result': {'ok': True, 'errors': []}},
            'chromium_sha256_before': '9' * 64, 'chromium_sha256_after': '9' * 64,
            'result': {'side': side, 'status': 'PASS_SCOPED', 'case_count': 12,
                       'cases': [row(case_id) for case_id in RUNNER.CASE_IDS],
                       'dependencies': {'before': dependencies(), 'after': dependencies(), 'chromium_sha256': '9' * 64,
                                        'fixture_sha256': {'client': '7' * 64, 'probe': '8' * 64}},
                       'cleanup_errors': [], 'browser_cleanup': {'failures': [], 'contextsAfter': 0,
                           'exit': {'code': 0, 'signal': None, 'observedBy': 'child-exit-event'}}}}


def manifest():
    return {'sides': {side: record(side) for side in ('go', 'rust')},
            'binaries': {'go': {'sha256': RUNNER.FIXED_GO_SHA}, 'rust': {'sha256': 'e' * 64}},
            'inputs_unchanged': True, 'binary_identities_stable': True, 'chromium_identity_stable': True}


class RunnerTests(unittest.TestCase):
    def setUp(self):
        RUNNER.CANCELLED.clear()

    def assert_fail(self, value):
        RUNNER.finalize(value)
        self.assertEqual(value['status'], 'FAIL')

    def test_complete_synthetic_manifest_and_false_enabled(self):
        value = manifest()
        RUNNER.finalize(value)
        self.assertEqual(value['status'], 'PASS_SCOPED')
        self.assertTrue(all(value['side_lifecycle_ok'].values()))
        self.assertEqual(value['observed_case_count'], {'go': 12, 'rust': 12})

    def test_samples_counts_and_timestamps_retained_not_compared(self):
        value = manifest()
        value['sides']['rust']['result']['cases'][0]['samples'] = [
            {'timestamp': 7000, 'bytesReceived': 18000, 'framesDecoded': 19},
            {'timestamp': 7001, 'bytesReceived': 19000, 'framesDecoded': 20}]
        original = copy.deepcopy(value['sides'])
        RUNNER.finalize(value)
        self.assertEqual(value['status'], 'PASS_SCOPED')
        self.assertEqual(value['cases'][0]['rust']['samples'], original['rust']['result']['cases'][0]['samples'])
        self.assertEqual(value['sides'], original)

    def test_empty_missing_duplicate_unknown_and_reordered_rows(self):
        for mutation in ('empty', 'missing', 'duplicate', 'unknown', 'reorder', 'null', 'malformed'):
            with self.subTest(mutation=mutation):
                value = manifest()
                rows = value['sides']['rust']['result']['cases']
                if mutation == 'empty':
                    rows.clear()
                elif mutation == 'missing':
                    rows.pop(0)
                elif mutation == 'duplicate':
                    rows.append(copy.deepcopy(rows[0]))
                elif mutation == 'unknown':
                    rows.append(row('unknown'))
                elif mutation == 'reorder':
                    rows.reverse()
                elif mutation == 'null':
                    value['sides']['rust']['result']['cases'] = None
                else:
                    rows.append(None)
                self.assert_fail(value)
                self.assertFalse(value['side_manifests_ok']['rust'])

    def test_bad_observation_and_samples_fail_both_equal_sides(self):
        for key, bad in [('observed', {}), ('observed', None), ('observed', {'contract': {}}),
                         ('observed', {'contract': {'vector': False}}), ('observed', {'contract': {'vector': 1}}),
                         ('observed', {'contract': {'vector': True}, 'n': float('nan')}),
                         ('samples', []), ('samples', {}), ('samples', [None]),
                         ('samples', [{'n': float('inf')}]), ('samples', [{'n': float('-inf')}]),
                         ('samples', [{'only': True}]), ('status', 'FAIL'), ('failure', {'check': 'failed'})]:
            with self.subTest(key=key, bad=bad):
                value = manifest()
                for side in ('go', 'rust'):
                    value['sides'][side]['result']['cases'][0][key] = bad
                self.assert_fail(value)
                self.assertFalse(value['cases'][0]['go_contract_ok'])
                self.assertFalse(value['cases'][0]['rust_contract_ok'])

    def test_full_deterministic_observed_compared_not_just_checks(self):
        value = manifest()
        value['sides']['rust']['result']['cases'][0]['observed']['requested']['qualities'].reverse()
        self.assert_fail(value)
        self.assertTrue(value['cases'][0]['rust_contract_ok'])
        self.assertFalse(value['cases'][0]['cross_side_equal'])
        self.assertFalse(RUNNER.deep_equal({'enabled': False}, {'enabled': 0}))
        self.assertFalse(RUNNER.deep_equal([1], [True]))

    def test_wrong_side_count_status_cleanup_and_empty_result(self):
        for key, bad in [('side', 'go'), ('case_count', 11), ('case_count', True), ('status', 'PASS'),
                         ('cleanup_errors', ['failed']), ('failure', {}), ('dependencies', {})]:
            with self.subTest(key=key):
                value = manifest()
                value['sides']['rust']['result'][key] = bad
                self.assert_fail(value)
        for bad in (None, [], {}, 'PASS'):
            value = manifest()
            value['sides']['rust']['result'] = bad
            self.assert_fail(value)

    def test_browser_cleanup_requires_real_exit_and_no_remaining_contexts(self):
        mutations = [('failures', ['owned-browser-exit']), ('contextsAfter', 1), ('contextsAfter', False),
                     ('exit', None), ('exit', {'code': 0}), ('exit', {'code': False, 'signal': None, 'observedBy': 'child-state'}),
                     ('exit', {'code': 0, 'signal': 'SIGTERM', 'observedBy': 'child-state'}),
                     ('exit', {'code': 0, 'signal': None, 'observedBy': 'disconnected'}),
                     ('exit', {'code': 0, 'signal': None, 'observedBy': 'child-state', 'timeout': True})]
        for key, bad in mutations:
            with self.subTest(key=key, bad=bad):
                value = manifest()
                value['sides']['rust']['result']['browser_cleanup'][key] = bad
                self.assert_fail(value)

    def test_lifecycle_exit_identity_config_ports_and_cleanup(self):
        for key, bad in [('probe_exit', 1), ('probe_exit', -15), ('probe_exit', None), ('probe_exit', False),
                         ('binary_sha256_after', '0' * 64), ('config_sha256', '0' * 64),
                         ('launch_before', {}), ('launch_after', {}), ('cleanup_errors', ['failed']),
                         ('ports_rebound', {'http': True, 'udp': False}),
                         ('server_cleanup', {'exit': 0, 'forced_kill': True, 'process_group_gone': True}),
                         ('server_cleanup', {'exit': -15, 'forced_kill': False, 'process_group_gone': True}),
                         ('probe_cleanup', {'exit': 0, 'forced_kill': False, 'process_group_gone': False}),
                         ('chromium_sha256_after', '0' * 64)]:
            with self.subTest(key=key, bad=bad):
                value = manifest()
                value['sides']['rust'][key] = bad
                self.assert_fail(value)
                self.assertFalse(value['side_lifecycle_ok']['rust'])
        for key in ('pid', 'main_alive', 'executable_matches', 'owned_loopback_listener', 'owned_loopback_udp'):
            value = manifest()
            value['sides']['rust']['launch_after'][key] = False
            self.assert_fail(value)

    def test_different_and_changed_dependencies_cannot_pass(self):
        for field in ('package_lock_sha256', 'pins_sha256'):
            value = manifest()
            for key in ('dependencies_before', 'dependencies_after'):
                value['sides']['rust'][key][field] = '8' * 64
            for phase in ('before', 'after'):
                value['sides']['rust']['result']['dependencies'][phase][field] = '8' * 64
            self.assert_fail(value)
            self.assertFalse(value['dependencies_equal'])
        for location in ('dependencies_before', 'dependencies_after', 'result'):
            value = manifest()
            dep = value['sides']['rust'][location]
            if location == 'result':
                dep = dep['dependencies']['before']
            dep['packages'][0]['entry_sha256'] = '0' * 64
            self.assert_fail(value)
        for name in RUNNER.PINS:
            dep = dependencies()
            next(item for item in dep['packages'] if item['name'] == name)['version'] = '0.0.0'
            self.assertFalse(RUNNER.dependencies_ok(dep))

    def test_vector_validator_failure_cannot_be_overridden_by_equal_observations(self):
        for bad in (None, {'exit': 1, 'result': {'ok': True, 'errors': []}},
                    {'exit': 0, 'result': {'ok': False, 'errors': ['low-high:observed-evidence']}},
                    {'exit': 0, 'result': {'ok': True, 'errors': ['low-high:sender_progress']}}):
            with self.subTest(bad=bad):
                value = manifest()
                for side in ('go', 'rust'):
                    value['sides'][side]['probe_validation'] = bad
                self.assert_fail(value)
                self.assertTrue(value['cases'][0]['cross_side_equal'])
                self.assertFalse(any(value['side_manifests_ok'].values()))

    def test_validator_runs_only_exported_offline_function_with_original_result(self):
        expected = {'ok': False, 'errors': ['low-high:observed-evidence']}
        with patch.object(RUNNER.subprocess, 'run', return_value=Mock(returncode=9, stdout=json.dumps(expected))) as execute:
            result = RUNNER.validate_probe_result('/synthetic/node', {'synthetic': 'unchanged'})
        self.assertEqual(result, {'exit': 9, 'result': expected})
        self.assertEqual(json.loads(execute.call_args.kwargs['input']), {'synthetic': 'unchanged'})
        self.assertEqual(execute.call_args.args[0][-1], (HERE / 'probe.mjs').as_uri())
        self.assertIn('validateReport', execute.call_args.args[0][3])
        self.assertEqual(execute.call_args.kwargs['timeout'], 30)

    def test_strict_json_rejects_duplicates_nonfinite_and_overflow(self):
        for text in ('{"n": NaN}', '{"n": Infinity}', '{"n": -Infinity}', '{"n": 1e999}', '{"n": 0, "n": 1}'):
            with self.subTest(text=text), self.assertRaises(RUNNER.CheckFailure):
                RUNNER.strict_json(text)
        self.assertEqual(RUNNER.strict_json('{"enabled":false,"n":0}'), {'enabled': False, 'n': 0})

    def test_config_loopback_no_stun_turn_only_vp8_opus(self):
        config = RUNNER.server_config({'http': 18001, 'udp': 18003}, 'fixture', 'secret')
        self.assertEqual(config['rtc']['tcp_port'], 0)
        self.assertEqual(config['rtc']['stun_servers'], [])
        self.assertEqual(config['rtc']['ips']['includes'], ['127.0.0.1/32'])
        self.assertEqual(config['bind_addresses'], ['127.0.0.1'])
        self.assertFalse(config['turn']['enabled'])
        self.assertEqual(config['video'], {'dynacast_pause_delay': '1s'})
        self.assertEqual(config['room']['enabled_codecs'], [{'mime': 'audio/opus'}, {'mime': 'video/vp8'}])
        self.assertEqual(RUNNER.PROBE_TIMEOUT, 600)

    def test_owned_process_timeout_records_force_kill_and_original_exit(self):
        proc = Mock(pid=23456)
        proc.wait.side_effect = [subprocess.TimeoutExpired('synthetic', 20), -signal.SIGKILL]
        with patch.object(RUNNER.os, 'killpg', side_effect=[None, None, ProcessLookupError()]) as kill:
            result = RUNNER.stop_owned(proc)
        self.assertEqual(result, {'exit': -signal.SIGKILL, 'forced_kill': True, 'process_group_gone': True})
        self.assertEqual([call.args for call in kill.call_args_list],
                         [(proc.pid, signal.SIGTERM), (proc.pid, signal.SIGKILL), (proc.pid, 0)])

    def test_darwin_inode_path_and_owned_loopback_listeners(self):
        proc = Mock(pid=23456)
        proc.poll.return_value = None
        binary, config = Path('/synthetic/main'), Path('/synthetic/server.yaml')
        mapped = 'p23456\nftxt\ni5678\nn/synthetic/main\n'
        valid = [mapped, 'p23456\nn127.0.0.1:18001\n', 'p23456\nn127.0.0.1:18003\n']
        for outputs, succeeds in [(valid, True), ([mapped.replace('5678', '9876')], False),
                                  ([mapped.replace('/synthetic/main', '/synthetic/other')], False),
                                  ([mapped, 'p9999\nn127.0.0.1:18001\n'], False),
                                  ([mapped, 'p23456\nn*:18001\n'], False),
                                  ([*valid[:2], 'p23456\nn*:18003\n'], False)]:
            with self.subTest(outputs=outputs), patch.object(Path, 'exists', return_value=False), \
                    patch.object(Path, 'stat', return_value=Mock(st_ino=5678)), \
                    patch.object(RUNNER, 'sha', side_effect=['a' * 64, 'b' * 64]), \
                    patch.object(RUNNER.subprocess, 'check_output', side_effect=outputs):
                if succeeds:
                    result = RUNNER.live_identity(proc, binary, 'a' * 64, config, 'b' * 64,
                                                  {'http': 18001, 'udp': 18003}, '/synthetic/lsof')
                    self.assertEqual(result['method'], 'lsof_text_path_inode')
                    self.assertTrue(result['main_alive'])
                else:
                    with self.assertRaises(RUNNER.CheckFailure):
                        RUNNER.live_identity(proc, binary, 'a' * 64, config, 'b' * 64,
                                             {'http': 18001, 'udp': 18003}, '/synthetic/lsof')

    def test_mock_side_preserves_exit_stdin_and_cleans_after_probe(self):
        with tempfile.TemporaryDirectory() as tmp:
            output = Path(tmp).resolve()
            binary = output / 'fake-main'
            binary.write_bytes(b'never executed')
            server, probe = Mock(pid=23456), Mock(pid=23457, returncode=7)
            server.poll.return_value = None
            probe.poll.return_value = 7
            ports, reservations = {'http': 18001, 'udp': 18003}, [Mock(), Mock()]
            value = {}
            with patch.object(RUNNER, 'dependency_snapshot', side_effect=[dependencies(), dependencies()]), \
                    patch.object(RUNNER, 'reserve_ports', return_value=(ports, reservations)), \
                    patch.object(RUNNER.subprocess, 'Popen', side_effect=[server, probe]) as launch, \
                    patch.object(RUNNER, 'wait_ready'), patch.object(RUNNER, 'live_identity', return_value={}), \
                    patch.object(RUNNER, 'stop_owned', return_value={'exit': 0}) as stop, \
                    patch.object(RUNNER, 'rebind_ports', return_value={'http': True, 'udp': True}):
                RUNNER.run_side('go', binary, RUNNER.sha(binary), output, '/synthetic/node',
                                '/synthetic/lsof', binary, value)
            self.assertEqual(value['probe_exit'], 7)
            self.assertEqual([call.args[0] for call in stop.call_args_list], [probe, server])
            options = json.loads(probe.stdin.write.call_args.args[0])
            self.assertEqual(options['side'], 'go')
            self.assertEqual(options['url'], 'http://127.0.0.1:18001')
            self.assertEqual(options['chromium'], str(binary))
            self.assertEqual(set(options), {'side', 'url', 'key', 'secret', 'chromium'})
            self.assertTrue(all(call.kwargs['start_new_session'] for call in launch.call_args_list))
            self.assertNotIn(options['secret'], (output / 'go.log').read_text())

    def test_cancellation_cleans_only_owned_server_and_rebinds(self):
        with tempfile.TemporaryDirectory() as tmp:
            output = Path(tmp).resolve()
            binary = output / 'fake-main'
            binary.write_bytes(b'never executed')
            server = Mock(pid=23456)
            value = {}
            with patch.object(RUNNER, 'dependency_snapshot', return_value=dependencies()), \
                    patch.object(RUNNER, 'reserve_ports', return_value=({'http': 18001, 'udp': 18003}, [Mock(), Mock()])), \
                    patch.object(RUNNER.subprocess, 'Popen', return_value=server), \
                    patch.object(RUNNER, 'wait_ready', side_effect=RUNNER.CheckFailure('cancelled')), \
                    patch.object(RUNNER, 'stop_owned', return_value={'exit': 0}) as stop, \
                    patch.object(RUNNER, 'rebind_ports', return_value={'http': True, 'udp': True}) as rebind:
                RUNNER.run_side('go', binary, RUNNER.sha(binary), output, 'node', 'lsof', binary, value)
            stop.assert_called_once_with(server)
            rebind.assert_called_once()
            self.assertEqual(value['failure']['check'], 'cancelled')

    def test_probe_timeout_keeps_unobserved_original_exit_and_cleanup_failure(self):
        with tempfile.TemporaryDirectory() as tmp:
            output = Path(tmp).resolve()
            binary = output / 'fake-main'
            binary.write_bytes(b'never executed')
            server, probe = Mock(pid=23456), Mock(pid=23457)
            server.poll.return_value = probe.poll.return_value = None
            value = {}
            with patch.object(RUNNER, 'dependency_snapshot', return_value=dependencies()), \
                    patch.object(RUNNER, 'reserve_ports', return_value=({'http': 18001, 'udp': 18003}, [Mock(), Mock()])), \
                    patch.object(RUNNER.subprocess, 'Popen', side_effect=[server, probe]), \
                    patch.object(RUNNER, 'wait_ready'), patch.object(RUNNER, 'live_identity', return_value={}), \
                    patch.object(RUNNER.time, 'monotonic', side_effect=[0, 601]), \
                    patch.object(RUNNER, 'stop_owned', side_effect=[OSError('private cleanup text'), {'exit': 0}]) as stop, \
                    patch.object(RUNNER, 'rebind_ports', return_value={'http': True, 'udp': True}):
                RUNNER.run_side('go', binary, RUNNER.sha(binary), output, 'node', 'lsof', binary, value)
            self.assertEqual(value['failure']['check'], 'probe_timeout')
            self.assertIsNone(value['probe_exit'])
            self.assertEqual(value['cleanup_errors'][0]['stage'], 'probe_cleanup')
            self.assertEqual(value['cleanup_errors'][0]['check'], 'operation_failed')
            self.assertEqual([call.args[0] for call in stop.call_args_list], [probe, server])
            self.assertNotIn('private cleanup text', json.dumps(value))

    def test_dependency_snapshot_checks_complete_identity_without_executing_node(self):
        with patch.object(RUNNER.subprocess, 'check_output', return_value=json.dumps(dependencies())) as execute:
            self.assertEqual(RUNNER.dependency_snapshot('/synthetic/node'), dependencies())
        command = execute.call_args.args[0]
        self.assertEqual(command[0:3], ['/synthetic/node', '--input-type=module', '-e'])
        self.assertEqual(json.loads(command[-1]), RUNNER.PINS)
        self.assertEqual(execute.call_args.kwargs['env'], {'PATH': RUNNER.os.environ.get('PATH', '/usr/bin:/bin')})
        with patch.object(RUNNER.subprocess, 'check_output', return_value='{}'), self.assertRaises(RUNNER.CheckFailure):
            RUNNER.dependency_snapshot('/synthetic/node')

    def test_log_privacy_keeps_failure_marker(self):
        result = RUNNER.redact_log(b'failed secret-value 10.1.2.3 192.168.7.9 172.31.2.3 127.0.0.1 eyJabc.def.ghi', 'secret-value')
        self.assertEqual(result, 'failed <redacted-secret> <redacted-private-ip> <redacted-private-ip> <redacted-private-ip> 127.0.0.1 <redacted-jwt>')

    def test_wrong_binary_and_fresh_output_fail_before_launch(self):
        for same_reference in (False, True):
            with self.subTest(same_reference=same_reference), tempfile.TemporaryDirectory() as tmp:
                root = Path(tmp).resolve()
                binary = root / 'synthetic-native'
                binary.write_bytes(b'\x7fELFnever executed')
                binary.chmod(0o700)
                output = root / 'output'
                argv = ['run-paired.py', '--go', str(binary), '--rust', str(binary),
                        '--chromium', str(binary), '--output', str(output)]
                fixed = RUNNER.sha(binary) if same_reference else RUNNER.FIXED_GO_SHA
                with patch.object(sys, 'argv', argv), patch.object(RUNNER, 'FIXED_GO_SHA', fixed), \
                        patch.object(RUNNER.subprocess, 'Popen') as launch, contextlib.redirect_stdout(io.StringIO()):
                    self.assertEqual(RUNNER.main(), 1)
                launch.assert_not_called()
                batch = json.loads((output / 'BATCH.json').read_text())
                self.assertEqual(batch['failure']['check'], 'rust_is_reference_binary' if same_reference else 'fixed_go_sha_mismatch')
                self.assertEqual(batch['covers'], RUNNER.ITEMS)
                with patch.object(sys, 'argv', argv), self.assertRaises(FileExistsError):
                    RUNNER.main()

    def test_go_failure_still_records_rust_without_retry(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp).resolve()
            binaries = [root / name for name in ('go', 'rust', 'chromium')]
            for i, binary in enumerate(binaries):
                binary.write_bytes(b'\x7fELFnever executed' + bytes([i]))
                binary.chmod(0o700)
            argv = ['run-paired.py', '--go', str(binaries[0]), '--rust', str(binaries[1]),
                    '--chromium', str(binaries[2]), '--output', str(root / 'output')]
            original_sha = RUNNER.sha

            def fake_sha(path):
                return original_sha(path) if Path(path) in binaries else 'a' * 64

            def run(side, binary, digest, output, node, lsof, chromium, value):
                value['failure'] = {'check': f'{side}_failed'}

            with patch.object(sys, 'argv', argv), patch.object(RUNNER, 'FIXED_GO_SHA', original_sha(binaries[0])), \
                    patch.object(RUNNER, 'sha', side_effect=fake_sha), \
                    patch.object(RUNNER.shutil, 'which', side_effect=lambda name: '/synthetic/' + name), \
                    patch.object(RUNNER, 'run_side', side_effect=run) as run_mock, \
                    contextlib.redirect_stdout(io.StringIO()):
                self.assertEqual(RUNNER.main(), 1)
            self.assertEqual([call.args[0] for call in run_mock.call_args_list], ['go', 'rust'])
            execution = json.loads((root / 'output/execution.json').read_text())
            self.assertEqual(set(execution['sides']), {'go', 'rust'})
            self.assertEqual(execution['sides']['go']['failure']['check'], 'go_failed')
            self.assertEqual(execution['sides']['rust']['failure']['check'], 'rust_failed')


if __name__ == '__main__':
    unittest.main()
