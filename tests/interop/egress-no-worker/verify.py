#!/usr/bin/env python3
"""复核 A10 原始证据：完整响应、真实重试、回归日志及全量台账统计。"""
import argparse
import collections
import copy
import importlib.util
import json
from pathlib import Path
import re
import sys


def load_module(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


HERE = Path(__file__).resolve().parent
PROBE = load_module('a10_probe', HERE / 'probe.py')
COMPARE = load_module('twirp_compare', HERE.parent / 'twirp-diff' / 'compare.py')


def read_json(path):
    return json.loads(path.read_text())


def check_probe(rows):
    expected = {f'{service}.{method}' for service, method, _ in PROBE.METHODS}
    assert len(rows) == len(expected) == 16, '必须是完整的 16 方法，不能空集通过'
    assert {row['id'] for row in rows} == expected, '方法缺失或重复'
    PROBE.validate(rows)


def negative_controls(rows):
    # 相同客户端超时、正文尾缀、立即失败、伪造录制等都不能通过。
    mutations = [
        lambda x: x.clear(),
        lambda x: x[0].update(error='timed out'),
        lambda x: x[0].update(status=500),
        lambda x: x[0].update(elapsed=0.0),
        lambda x: x[0].update(body=x[0]['body'] + 'PANIC: runtime error'),
        lambda x: x[0].update(body='{"code":"unavailable","msg":"wrong"}'),
        lambda x: x[-1].update(body=json.dumps({**json.loads(x[-1]['body']), 'active_recording': True})),
    ]
    for mutation in mutations:
        invalid = copy.deepcopy(rows)
        mutation(invalid)
        try:
            check_probe(invalid)
        except (AssertionError, ValueError):
            continue
        raise AssertionError('反例被错误接受')
    return len(mutations)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('evidence', type=Path)
    parser.add_argument('--ledger', type=Path, required=True)
    args = parser.parse_args()
    root = args.evidence
    official = read_json(root / 'go-probe-no-worker.json')
    rust = read_json(root / 'rust-probe-no-worker.json')
    for rows in (official, rust):
        check_probe(rows)
    for a, b in zip(official, rust):
        assert a['id'] == b['id'] and a['status'] == b['status'], (a, b)
        if a['id'] == 'RoomService.CreateRoom':
            assert COMPARE.norm(json.loads(a['body']), {}) == COMPARE.norm(json.loads(b['body']), {})
        else:
            assert a['body'] == b['body'], (a, b)

    report = read_json(root / 'full-diff' / 'report.json')
    raw = read_json(root / 'full-diff' / 'raw.json')['results']
    assert len(raw) == len(report['rows']) == 538
    assert len({row['id'] for row in raw}) == 538
    ids_a, ids_b = {}, {}
    for item, row in zip(raw, report['rows']):
        a, b = COMPARE.view(item['a'], ids_a), COMPARE.view(item['b'], ids_b)
        verdict = 'match' if a == b else 'status' if a.get('status') != b.get('status') else 'body'
        assert row == {'id': item['id'], 'status': verdict, 'a': a, 'b': b}
    starts = [row for row in report['rows'] if row['id'].startswith('Egress.Start')]
    assert len(starts) == 60
    for row in starts:
        assert row['status'] == 'match', row
        for side in ('a', 'b'):
            assert 'transport_error' not in row[side], row
    actual_503 = {side: sum(row[side]['status'] == 503 for row in starts) for side in ('a', 'b')}
    assert actual_503 == {'a': 18, 'b': 18}, actual_503
    hooks = report['webhooks']
    assert hooks['match'] and len(hooks['official']) == len(hooks['rust']) == 12
    assert all(event['auth'] == 'ok' for side in ('official', 'rust') for event in hooks[side])

    tests = (root / 'workspace-tests.log').read_text()
    summaries = re.findall(r'test result: ok\. (\d+) passed; (\d+) failed; (\d+) ignored;', tests)
    assert summaries and 'test result: FAILED' not in tests
    totals = [sum(int(row[index]) for row in summaries) for index in range(3)]
    assert totals[1] == 0
    names = [
        'single_node_graph_keeps_nil_store_handles_and_their_not_connected_errors',
        'single_node_start_waits_on_the_local_bus_instead_of_reporting_not_connected',
        'local_bus_start_round_trips_a_registered_egress_handler',
        'single_node_clients_reach_the_registered_io_graph',
        'single_node_gzip_limit_is_consumed_by_real_io_subscriptions',
        'client_views_share_membership_but_not_capacity_and_keep_root_isolation',
        'public_panic_boundary_sends_only_the_flushed_twirp_json',
    ]
    assert all(f'::{name} ... ok' in tests for name in names)
    assert 'Finished `dev` profile' in (root / 'clippy.log').read_text()
    ledger = read_json(args.ledger)
    items = ledger['items']
    assert len(items) == len({item['id'] for item in items}), '台账 ID 重复'
    api_ids = {f'API:{service}.{method}' for service, method, _ in PROBE.METHODS}
    scoped = [item for item in items if item['id'] in api_ids]
    assert len(scoped) == 16 and all(item['state'] == 'PARTIAL' for item in scoped)
    assert all(item.get('a10_standalone_scope') for item in scoped)
    result = {
        'scope': 'A10 单机无 Redis 切片通过；完整 API/外部 worker/其他契约未完成',
        'probe_methods_per_binary': 16,
        'strict_equal_error_bodies': 15,
        'room_comparison': '沿用现有比较器，只规范化运行时 ID/时钟/随机密码',
        'negative_controls_rejected': negative_controls(official),
        'twirp_counts': report['counts'],
        'remaining_differences': [row for row in report['rows'] if row['status'] != 'match'],
        'transport_errors': [row for row in report['rows'] if any('transport_error' in row[side] for side in ('a', 'b'))],
        'egress_start_rows': len(starts),
        'actual_503_per_side': actual_503,
        'signed_webhooks_per_side': 12,
        'workspace': dict(zip(('passed', 'failed', 'ignored'), totals), suites=len(summaries)),
        'a10_regressions': names,
        'ledger_inventory': {
            'total': len(items),
            'states': dict(collections.Counter(item['state'] for item in items)),
            'milestones': dict(collections.Counter(item.get('milestone', 'UNASSIGNED') for item in items)),
            'workstreams': dict(collections.Counter(item.get('workstream', 'UNASSIGNED') for item in items)),
            'a10_parent_states_unchanged': len(scoped),
            'exhaustive_parity_proof': False,
        },
    }
    json.dump(result, sys.stdout, ensure_ascii=False, indent=2)
    print()


if __name__ == '__main__':
    main()
