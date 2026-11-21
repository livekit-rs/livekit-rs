#!/usr/bin/env python3
"""Run the fixed-Go media counting boundary observer and pair it with the pinned Rust reader.

Stages the locked livekit-server tree, runs one original observer over the real
`RTPStatsSender`/`RTPStatsReceiver`/`toAnalyticsStream` code paths offline, keeps the actual rows,
binds them to the ignored Rust differential reader, and finally requires that reader to FAIL on a
mutated row file so the comparison cannot pass vacuously.
"""
import argparse
import hashlib
import json
import os
import pathlib
import subprocess

ROOT = pathlib.Path(__file__).resolve().parents[2]
HERE = pathlib.Path(__file__).resolve().parent
PREFIX = 'SOURCE_MEDIA_BOUNDARY '
TEST = ('regression::media_counting_boundary::'
        'original_media_counting_boundary_rows_match_locked_go_fold')

# (case, window) pairs the observer must emit, exactly once each.
EXPECTED = (
    [('downstream_padding_only_before_start', 1),
     ('downstream_padding_only_before_start', 2)]
    + [(name, window) for name in (
        'downstream_padding_inside_window',
        'downstream_sequence_gap',
        'downstream_exact_duplicate',
        'downstream_late_repair_in_window',
        'downstream_late_below_start',
        'downstream_late_beyond_cache',
        'downstream_too_many_packets_expected',
        'downstream_zero_progress') for window in (1, 2)]
    + [('downstream_feedback_counters', 1),
       ('rtx_starts_on_padding_only', 1),
       ('non_rtx_never_starts_on_padding', 1),
       ('downstream_padding_repairs_exceed_span', 1),
       ('rtx_reconcile', 1)]
    + [(name, window) for name in (
        'upstream_primary_and_padding',
        'upstream_padding_only_first',
        'upstream_gap_late_and_pre_start',
        'upstream_duplicate') for window in (1, 2)]
)

PHYSICAL = ('sent_primary', 'sent_padding', 'sent_duplicate', 'sent_frames',
            'logical_bytes', 'sn_span')
DELTA = ('packets', 'bytes', 'header_bytes', 'packets_duplicate', 'bytes_duplicate',
         'packets_padding', 'bytes_padding', 'packets_lost', 'packets_missing',
         'packets_out_of_order', 'frames', 'rtt_max', 'jitter_max', 'nacks',
         'nack_repeated', 'plis', 'firs')
STREAM = ('ssrc', 'primary_packets', 'primary_bytes', 'retransmit_packets',
          'retransmit_bytes', 'padding_packets', 'padding_bytes', 'packets_lost',
          'out_of_order', 'frames', 'rtt', 'jitter', 'nacks', 'plis', 'firs')
LAYER = ('packets', 'bytes', 'frames')

# Every row field holding an *RTPDeltaInfo / *AnalyticsStream, including the reconcile row, which
# reports the same shapes under distinct names so reconcile input and output stay comparable.
DELTA_FIELDS = ('sender_view', 'receiver_view', 'base_delta',
                'primary_sender_view', 'primary_receiver_view', 'primary_base_delta',
                'rtx_sender_view', 'rtx_base_delta',
                'reconciled_sender_view', 'reconciled_receiver_view', 'reconciled_base_delta')
STREAM_FIELDS = ('stream_down', 'stream_down_unreconciled', 'stream_up')


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def rows_of(text):
    rows = []
    for line in text.splitlines():
        if line.startswith(PREFIX):
            rows.append(json.loads(line[len(PREFIX):]))
    return rows


def check_shape(row, keys):
    for key in keys:
        if key not in row:
            raise RuntimeError('%s/%d: missing key %s' % (row['case'], row['window'], key))


def validate(rows):
    index = {}
    for row in rows:
        key = (row['case'], row['window'])
        if key in index:
            raise RuntimeError('repeated actual row %s/%d' % key)
        index[key] = row
    if sorted(index) != sorted(EXPECTED):
        raise RuntimeError('actual row set drifted: %s' % (
            sorted(set(index) ^ set(EXPECTED)) if len(index) != len(EXPECTED)
            else 'duplicate case names'))
    for key in EXPECTED:
        row = index[key]
        if 'physical' in row:
            check_shape(row['physical'], PHYSICAL)
        for field in DELTA_FIELDS:
            if field in row and row[field] is not None:
                check_shape(row[field], DELTA)
        for field in STREAM_FIELDS:
            if field in row and row[field] is not None:
                check_shape(row[field], STREAM)
        if 'layer_fold' in row and row['layer_fold'] is not None:
            check_shape(row['layer_fold'], LAYER)
    return {'rows': len(rows), 'cases': len(EXPECTED)}


def cargo(env, out, label, results, expect_success):
    log = out / (label + '.log')
    command = ['cargo', 'test', '--release', '--locked', '-p', 'rtc-media', TEST,
               '--', '--ignored', '--exact']
    with log.open('xb') as handle:
        proc = subprocess.run(command, cwd=ROOT,
                              env=dict(env, GO_MEDIA_COUNTING_BOUNDARY_RESULTS=str(results)),
                              stdout=handle, stderr=subprocess.STDOUT)
    passed = proc.returncode == 0
    if passed != expect_success:
        raise RuntimeError('%s: exit %d, expected %s (see %s)'
                           % (label, proc.returncode,
                              'success' if expect_success else 'failure', log))
    return {'exit_code': proc.returncode, 'log': str(log), 'log_sha256': sha(log)}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output', required=True, help='New private output directory')
    parser.add_argument('--go', default='/Users/zoujiaqing/go/pkg/mod/'
                                        'golang.org/toolchain@v0.0.1-go1.26.3.darwin-arm64/bin/go')
    args = parser.parse_args()
    out = pathlib.Path(args.output).resolve()
    out.mkdir(mode=0o700, parents=True, exist_ok=False)
    go = pathlib.Path(args.go)
    if not go.is_file():
        go = pathlib.Path('go')
    stage_out = out / 'stage'
    results = out / 'boundary-rows.jsonl'
    report = {
        'scope': 'locked pkg/sfu/rtpstats delta boundary + pkg/sfu/connectionquality '
                 'toAnalyticsStream class partition only; the telemetry fold is anchored to '
                 'pkg/telemetry/stats.go:83-92 and is compared by the livecloud Prometheus '
                 'series tests, and the authenticated physical SRTP boundary is compared by the '
                 'rtc-media real-session regressions',
        'status': 'RUNNING',
        'fixed_go_commit': json.loads((HERE / 'SOURCE.json').read_text())['commit'],
        'observer_sha256': sha(HERE / 'boundary_observer_test.go'),
        'source_manifest_sha256': sha(HERE / 'SOURCE.json'),
        'go_binary': str(go),
    }
    env = dict(os.environ, GOTOOLCHAIN='local', GOPROXY='off', GOSUMDB='off', GOWORK='off',
               GOENV='off', GOTELEMETRY='off', GOFLAGS='-mod=readonly -buildvcs=false',
               GOCACHE=str(out / 'gocache'), GOTMPDIR=str(out / 'tmp'), TMPDIR=str(out / 'tmp'))
    if go.is_absolute():
        env['GOROOT'] = str(go.parents[1])
    (out / 'gocache').mkdir()
    (out / 'tmp').mkdir()
    try:
        subprocess.run(['python3', str(HERE / 'prepare.py'), '--root', str(ROOT),
                        '--out', str(stage_out)], check=True, capture_output=True, text=True)
        staged = json.loads((stage_out / 'STAGED.json').read_text())
        report['go_mod_staged_sha256'] = staged['go_mod_staged_sha256']
        report['staged_observer_sha256'] = sha(
            stage_out / 'source' / staged['source']['staged_as'])
        report['go_version'] = subprocess.run([str(go), 'version'], env=env, check=True,
                                              capture_output=True, text=True).stdout.strip()
        with (out / 'observer.log').open('xb') as handle:
            proc = subprocess.run(
                [str(go), 'test', './pkg/sfu/connectionquality/', '-count=1', '-v',
                 '-run', '^TestOriginalMediaCountingBoundary'],
                cwd=stage_out / 'source', env=env, stdout=handle, stderr=subprocess.STDOUT)
        if proc.returncode != 0:
            raise RuntimeError('fixed-Go observer exited %d (see observer.log)'
                               % proc.returncode)
        observer_text = (out / 'observer.log').read_text()
        if '--- FAIL' in observer_text or 'PASS' not in observer_text:
            raise RuntimeError('fixed-Go observer did not report a clean pass')
        rows = rows_of(observer_text)
        report.update(validate(rows))
        with results.open('xb') as handle:
            for row in rows:
                handle.write(PREFIX + json.dumps(row, sort_keys=True) + '\n')
        report['results_sha256'] = sha(results)
        report['cargo'] = cargo(env, out, 'paired', results, True)

        # Mutate the in-window padding row by adding one primary packet to the sender view while
        # leaving the stimulus, the padding series and the AnalyticsStream mirror untouched. The
        # reader must reject it twice over: packets+padding no longer spans the sequence numbers
        # the window covers, and stream_down.primary_packets no longer mirrors the delta it was
        # built from. Nothing else in the row changes, so a passing reader would prove the
        # comparison was only parsing.
        target = ('downstream_padding_inside_window', 1)
        mutated, changed = [], False
        for row in rows:
            if (row['case'], row['window']) == target and row.get('sender_view'):
                row = dict(row)
                row['sender_view'] = dict(row['sender_view'],
                                          packets=row['sender_view']['packets'] + 1)
                changed = True
            mutated.append(row)
        if not changed:
            raise RuntimeError('no row available for the mutation control')
        mutant = out / 'boundary-rows.mutant.jsonl'
        with mutant.open('xb') as handle:
            for row in mutated:
                handle.write(PREFIX + json.dumps(row, sort_keys=True) + '\n')
        report['mutant'] = cargo(env, out, 'mutant', mutant, False)
        report['mutant_row'] = '%s/%d sender_view.packets+1 only' % target
        report['status'] = 'PASS'
    except BaseException:
        report['status'] = 'FAIL'
        raise
    finally:
        (out / 'report.json').write_text(json.dumps(report, indent=2) + '\n')


if __name__ == '__main__':
    main()
