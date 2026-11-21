#!/usr/bin/env python3
"""Actual fixed-Go RED encoder rows paired byte-for-byte with the Rust RED sender.

Stages the locked server tree with one in-package observer, runs the real
`RedReceiver.ForwardRTP` over the frozen vectors, validates every emitted row
structurally, binds the rows to the ignored Rust differential, and finally
requires that differential to fail on a mutated row so the comparison cannot
pass vacuously.
"""
import argparse
import hashlib
import json
import os
import pathlib
import subprocess

ROOT = pathlib.Path(__file__).resolve().parents[2]
HERE = pathlib.Path(__file__).resolve().parent
TEST = 'packet::red::test::original_go_red_encoder_vectors_match_red_sender_byte_for_byte'
PREFIX = 'SOURCE_RED_ENCODE '
# Row counts of the vectors in observer_test.go; a truncated stage must not pass.
EXPECTED = {
    'normal': 10,
    'packet_lost_and_jump': 8,
    'unorder_and_repeat': 12,
    'encoding_exceed_space': 5,
    'large_timestamp_gap': 8,
    'oversized_frame': 4,
}
MAX_RED_PAYLOAD = 1 << 10
OPUS_PT = 111
RED_PT = 63


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def rows_of(text):
    rows = []
    for line in text.splitlines():
        if not line.strip():
            continue
        if not line.startswith(PREFIX):
            continue
        rows.append(json.loads(line[len(PREFIX):]))
    return rows


def blocks_of(payload):
    """Walk the RFC 2198 header chain of one actual Go payload."""
    blocks = []
    index = 0
    while True:
        first = payload[index]
        index += 1
        if not first & 0x80:
            blocks.append({'pt': first & 0x7F, 'offset': 0, 'primary': True})
            break
        word = int.from_bytes(payload[index - 1:index + 3], 'big')
        blocks.append({'pt': (word >> 24) & 0x7F, 'offset': (word >> 10) & 0x3FFF,
                       'length': word & 0x3FF, 'primary': False})
        index += 3
    body = payload[index:]
    used = 0
    for block in blocks:
        size = len(body) - used if block['primary'] else block['length']
        block['payload'] = body[used:used + size]
        used += size
    return blocks, used == len(body)


def validate(rows):
    """Structural audit of the actual Go output, independent of the Rust algorithm."""
    by_case = {}
    for row in rows:
        by_case.setdefault(row['case'], []).append(row)
    if set(by_case) != set(EXPECTED):
        raise RuntimeError('actual cases %r != frozen vectors %r'
                           % (sorted(by_case), sorted(EXPECTED)))
    seen_payloads = {}
    wrapped = 0
    bypassed = 0
    for name, case_rows in by_case.items():
        if len(case_rows) != EXPECTED[name]:
            raise RuntimeError('%s: %d actual rows != %d frozen steps'
                               % (name, len(case_rows), EXPECTED[name]))
        if [r['index'] for r in case_rows] != list(range(len(case_rows))):
            raise RuntimeError('%s: actual row indexes are not the emission order' % name)
        # Frames this case has already fed in, so a redundant block must resolve to one of them.
        origins = {}
        for row in case_rows:
            if (row['out_seq'], row['out_ts']) != (row['seq'], row['ts']):
                raise RuntimeError('%s[%d]: forwarding changed seq/timestamp'
                                   % (name, row['index']))
            if row['out_marker'] != row['marker']:
                raise RuntimeError('%s[%d]: forwarding changed the marker bit'
                                   % (name, row['index']))
            payload = bytes.fromhex(row['payload_hex'])
            out = bytes.fromhex(row['out_payload_hex'])
            if len(payload) >= MAX_RED_PAYLOAD:
                if row['out_pt'] != OPUS_PT or out != payload:
                    raise RuntimeError('%s[%d]: a frame of %d bytes must be broadcast unwrapped'
                                       % (name, row['index'], len(payload)))
                bypassed += 1
                continue
            if row['out_pt'] != RED_PT:
                raise RuntimeError('%s[%d]: frame of %d bytes left the RED payload type'
                                   % (name, row['index'], len(payload)))
            wrapped += 1
            blocks, exact = blocks_of(out)
            if not exact:
                raise RuntimeError('%s[%d]: RED block lengths do not cover the payload'
                                   % (name, row['index']))
            if not 1 <= len(blocks) <= 3:
                raise RuntimeError('%s[%d]: %d RED blocks, want the primary plus at most two'
                                   % (name, row['index'], len(blocks)))
            for block in blocks:
                if block['pt'] != OPUS_PT:
                    raise RuntimeError('%s[%d]: block payload type %d, want the fixed opus %d'
                                       % (name, row['index'], block['pt'], OPUS_PT))
            primary = blocks[-1]
            if primary['payload'] != payload:
                raise RuntimeError('%s[%d]: the primary block is not the forwarded frame'
                                   % (name, row['index']))
            for block in blocks[:-1]:
                candidates = origins.get(block['payload'], [])
                if not candidates:
                    raise RuntimeError('%s[%d]: redundant block is not an earlier input frame of '
                                       'this case' % (name, row['index']))
                if not any(block['offset'] == (row['ts'] - ts) & 0x3FFF for _, ts in candidates):
                    raise RuntimeError('%s[%d]: block offset %d disagrees with the timestamp gap '
                                       'to every earlier frame carrying that payload (frames %r)'
                                       % (name, row['index'], block['offset'],
                                          [index for index, _ in candidates]))
            origins.setdefault(payload, []).append((row['index'], row['ts']))
        seen_payloads[name] = len(origins)
    if bypassed != 1:
        raise RuntimeError('%d bypassed rows, want exactly the oversized frame' % bypassed)
    if wrapped < 40:
        raise RuntimeError('%d wrapped rows cannot exercise the redundancy window' % wrapped)
    return {'rows': len(rows), 'wrapped_rows': wrapped, 'bypassed_rows': bypassed,
            'distinct_frames': seen_payloads,
            'cases': {name: len(case) for name, case in sorted(by_case.items())}}


def cargo(env, out, label, results, expect_success):
    log = out / (label + '.cargo.log')
    command = ['cargo', 'test', '--release', '--locked', '-p', 'str0m', TEST,
               '--', '--ignored', '--exact']
    with log.open('xb') as handle:
        proc = subprocess.run(command, cwd=ROOT,
                              env=dict(env, GO_RED_ENCODE_RESULTS=str(results)),
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
    go = pathlib.Path(args.go).resolve()
    stage_out = out / 'stage'
    results = out / 'red-encode.results'
    report = {
        'scope': 'locked pkg/sfu RedReceiver.ForwardRTP encodeRedForPrimary output only; the '
                 'active_red_encoding wire gate is the rtc-media real-SRTP regression test and '
                 'the receiver-side recovery is the str0m RED decoder unit tests',
        'status': 'RUNNING',
        'fixed_go_commit': json.loads((HERE / 'SOURCE.json').read_text())['commit'],
        'observer_sha256': sha(HERE / 'observer_test.go'),
        'go_binary': str(go),
    }
    env = dict(os.environ, GOTOOLCHAIN='local', GOPROXY='off', GOSUMDB='off', GOWORK='off',
               GOENV='off', GOTELEMETRY='off', GOROOT=str(go.parents[1]),
               GOFLAGS='-mod=mod -buildvcs=false',
               GOCACHE=str(out / 'gocache'), GOTMPDIR=str(out / 'tmp'), TMPDIR=str(out / 'tmp'))
    (out / 'gocache').mkdir()
    (out / 'tmp').mkdir()
    try:
        subprocess.run(['python3', str(HERE / 'prepare.py'), '--root', str(ROOT),
                        '--out', str(stage_out)], check=True, capture_output=True, text=True)
        staged = json.loads((stage_out / 'STAGED.json').read_text())
        report['source_manifest_sha256'] = sha(HERE / 'SOURCE.json')
        report['go_mod_staged_sha256'] = staged['go_mod_staged_sha256']
        with (out / 'observer.log').open('xb') as handle:
            proc = subprocess.run(
                [str(go), 'test', './pkg/sfu/', '-count=1', '-v',
                 '-run', '^TestOriginalRedEncodeSource$'],
                cwd=stage_out / 'source', env=env, stdout=handle, stderr=subprocess.STDOUT)
        if proc.returncode != 0:
            raise RuntimeError('fixed-Go observer exited %d (see observer.log)'
                               % proc.returncode)
        observer_text = (out / 'observer.log').read_text()
        if 'SOURCE_RED_ENCODE' not in observer_text:
            raise RuntimeError('observer produced no rows (see observer.log)')
        if '--- FAIL' in observer_text:
            raise RuntimeError('fixed-Go observer reported a failure (see observer.log)')
        rows = rows_of(observer_text)
        report.update(validate(rows))
        with results.open('xb') as handle:
            for row in rows:
                handle.write(PREFIX + json.dumps(row, sort_keys=True) + '\n')
        report['results_sha256'] = sha(results)
        report['cargo'] = cargo(env, out, 'paired', results, True)

        # Mutate the first full-window row: deleting one redundant header keeps the payload a
        # decodable RFC 2198 chain, so a failing differential proves the byte comparison and not
        # a parse error.
        target = next(r for r in rows
                      if r['out_pt'] == RED_PT
                      and len(blocks_of(bytes.fromhex(r['out_payload_hex']))[0]) == 3)
        mutated = []
        mutant_blocks = None
        for row in rows:
            if row is target:
                row = dict(row)
                payload = bytes.fromhex(row['out_payload_hex'])
                row['out_payload_hex'] = (payload[:4] + payload[8:]).hex()
                mutant_blocks = blocks_of(bytes.fromhex(row['out_payload_hex']))[0]
            mutated.append(row)
        if mutated == rows:
            raise RuntimeError('mutation control changed nothing')
        source_blocks = blocks_of(bytes.fromhex(target['out_payload_hex']))[0]
        if [b['payload'] for b in mutant_blocks] == [b['payload'] for b in source_blocks]:
            raise RuntimeError('mutation control is not observable in the block chain')
        mutant = out / 'red-encode.results.mutant'
        with mutant.open('xb') as handle:
            for row in mutated:
                handle.write(PREFIX + json.dumps(row, sort_keys=True) + '\n')
        report['mutant'] = cargo(env, out, 'mutant', mutant, False)
        report['mutant_row'] = '%s[%d]' % (target['case'], target['index'])
        report['status'] = 'PASS'
    except BaseException:
        report['status'] = 'FAIL'
        raise
    finally:
        (out / 'report.json').write_text(json.dumps(report, indent=2) + '\n')
        print(out / 'report.json')


if __name__ == '__main__':
    main()
