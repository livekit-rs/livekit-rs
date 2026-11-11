#!/usr/bin/env python3
"""Run both SDKs through exact-slot faults and retain strict and attributed outcomes."""
import argparse
import json
from pathlib import Path
import subprocess
import sys


def classify(label, profile, delay, result):
    stages = result.get('transfer_stages', {})
    fault = result.get('fault_slot', {})
    transition = next(iter(result.get('transitions', [])), {})
    catalog = result.get('catalog_slots', {})
    exact = (
        fault.get('profile') == profile and fault.get('slot') is not None
        and fault.get('live_field_survived_promotion')
        and fault.get('old_primary') == transition.get('previous_primary')
        and transition.get('old_primary_listener_released') and transition.get('restored_ports')
        and result.get('old_primary_still_down') and result.get('ports_released')
        and catalog.get('rooms', {}).get('old_primary') != catalog.get('room_participants', {}).get('old_primary')
    )
    request = stages.get('sdk_transfer', {})
    issued_after_release = request.get('issuedUnix', 0) - result.get('probe_released_unix', 0)
    strict = result.get('status') == 'PASS'
    attributed = 'UNEXPLAINED'
    if exact and issued_after_release >= delay:
        old_port = f':{fault["old_primary"]}:'
        preflight = not stages['requests'] and stages['refer_count'] == 0 and old_port in request.get('error', '')
        worker = (stages['requests'] and not stages['dedup_set_nx'] and not stages['claim_count']
                  and stages['refer_count'] == 0 and stages['stale_worker_slot'] and not request.get('ok'))
        room_delete = next((row for row in stages['sdk_cleanup'] if row['operation'] == 'owned-room-delete'), {})
        room_remains = any(row['bucket'] == 'rooms' and row['exists'] for row in result['post_cleanup_redis'])
        room_cleanup = label == 'go' and profile == 'room' and old_port in room_delete.get('error', '') and room_remains
        if delay:
            if strict and stages['healthy_transfer'] and stages['stores_empty']:
                attributed = 'STRICT_PASS'
        elif label == 'go' and profile == 'participant' and preflight:
            attributed = 'GO_STALE_PARTICIPANT_PREFLIGHT'
        elif room_cleanup and stages['healthy_transfer']:
            attributed = 'GO_STALE_ROOM_CLEANUP_AFTER_REFER'
        elif room_cleanup and worker:
            attributed = 'ORIGINAL_WORKER_STALE_DEDUP_AND_GO_ROOM_CLEANUP'
        elif label == 'rust' and worker and stages['stores_empty']:
            attributed = 'ORIGINAL_WORKER_STALE_DEDUP'
        elif label == 'rust' and strict:
            attributed = 'STRICT_PASS'
    return dict(strict=strict, attribution=attributed, seconds_after_release=round(issued_after_release, 3))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ('go', 'rust', 'worker', 'output'):
        parser.add_argument(f'--{name}', type=Path, required=True)
    parser.add_argument('--repeat', type=int, default=2)
    args = parser.parse_args()
    if args.repeat < 1:
        parser.error('--repeat must be at least 1')
    output = args.output.resolve()
    output.mkdir()
    driver = Path(__file__).resolve().with_name('run.py')
    rows = []
    for index in range(args.repeat):
        for label in ('go', 'rust'):
            for profile, delay in (('participant', 0), ('room', 0), ('participant', 65)):
                name = f'{index + 1}-{label}-{profile}-{delay}s'
                cmd = [
                    sys.executable, '-B', str(driver), '--go', str(args.go.resolve()),
                    '--rust', str(args.rust.resolve()), '--worker', str(args.worker.resolve()),
                    '--output', str(output / name), '--only', label, '--topology', 'cluster',
                    '--tls', '--hard-stop', '--worker-cpu-ceiling', '1',
                    '--slot-profile', profile, '--settle-seconds', str(delay),
                ]
                with (output / f'{name}.driver.log').open('wb') as log:
                    exit_code = subprocess.run(cmd, stdout=log, stderr=subprocess.STDOUT).returncode
                receipt = output / name / f'cluster-{label}' / 'execution.json'
                try:
                    result = json.loads(receipt.read_text())
                    verdict = classify(label, profile, delay, result)
                except (OSError, ValueError, KeyError):
                    verdict = dict(strict=False, attribution='UNEXPLAINED')
                row = dict(name=name, implementation=label, profile=profile, wait_seconds=delay,
                           driver_exit=exit_code, receipt=str(receipt.relative_to(output)), **verdict)
                rows.append(row)
                print(f'{name}: {verdict["attribution"]}, strict exit {exit_code}', flush=True)
                (output / 'comparison.json').write_text(json.dumps(rows, indent=2) + '\n')
    return 0 if all(row['strict'] and row['driver_exit'] == 0 for row in rows) else 1


if __name__ == '__main__':
    raise SystemExit(main())
