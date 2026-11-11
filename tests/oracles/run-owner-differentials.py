#!/usr/bin/env python3
"""Run the three room owner producers against the pinned Go observers and compare.

The Rust tests only WRITE their actual owner rows; a green producer exit is not
acceptance. This driver stages both Go observer trees, compiles each Go test
binary once, runs every producer with a fresh O_EXCL/create_new path, authors the
execution receipts read-go11.py demands, and then runs the strict readers. Every
step keeps its own exit code.
"""
import argparse
import hashlib
import json
import os
import pathlib
import shutil
import subprocess
import sys
import time

ROOT = pathlib.Path(__file__).resolve().parents[2]
AUDIO = ROOT / 'tests/oracles/audio-update-owner'
VIDEO = ROOT / 'tests/oracles/open-data-kind-video-owner'
RUST_SOURCE = ROOT / 'crates/rtc-core/src/room/tests.rs'
OVERLAYS = [('github.com/livekit/protocol', 'references/protocol-stable'),
            ('github.com/livekit/psrpc', 'references/psrpc'),
            ('github.com/livekit/mediatransportutil',
             'references/go-modules/mediatransportutil-f234b534b095')]
AUDIO_OBSERVERS = ['update_audio_owner_observer_test.go',
                   'update_audio_compact_tail_observer_test.go']

PROFILES = {
    'go11': {
        'tree': 'audio', 'go_test': 'TestUpdateAudioOwnerSourceObserver',
        'go_env': 'UPDATE_AUDIO_SOURCE_RESULT', 'go_out': 'go11.jsonl',
        'go_kind': 'original_go11', 'observer': AUDIO_OBSERVERS[0],
        'rust_test': 'room::tests::old_public_update_audio_actual_go11_owner_output',
        'rust_env': 'UPDATE_AUDIO_RUST_RESULT', 'rust_out': 'rust11.jsonl',
        'rust_kind': 'rust_owner11', 'reader': 'go11',
    },
    'compact9': {
        'tree': 'audio', 'go_test': 'TestUpdateAudioCompactTailSourceObserver',
        'go_env': 'UPDATE_AUDIO_COMPACT_SOURCE_RESULT', 'go_out': 'go-compact9.jsonl',
        'go_kind': 'original_go_compact9', 'observer': AUDIO_OBSERVERS[1],
        'rust_test': 'room::tests::old_public_update_audio_actual_compact9_owner_output',
        'rust_env': 'UPDATE_AUDIO_RUST_COMPACT_RESULT', 'rust_out': 'rust-compact9.jsonl',
        'rust_kind': 'rust_owner_compact9', 'reader': 'go11',
    },
    'video10': {
        'tree': 'video', 'go_test': 'TestUpdateVideoOwnerSourceObserver',
        'go_env': 'UPDATE_VIDEO_SOURCE_RESULT', 'go_out': 'go10.jsonl',
        'rust_test': 'room::tests::old_public_update_video_actual_owner10_output',
        'rust_env': 'UPDATE_VIDEO_RUST_RESULT', 'rust_out': 'rust10.jsonl',
        'reader': 'video10',
    },
}


def sha(path):
    return hashlib.sha256(pathlib.Path(path).read_bytes()).hexdigest()


class Driver:
    def __init__(self, work, go, timeout):
        self.work = work
        self.go = go
        self.timeout = timeout
        self.steps = []
        self.goenv = dict(os.environ, GOTOOLCHAIN='local', GOPROXY='off', GOWORK='off')

    def run(self, label, argv, cwd=ROOT, env=None, stdout=None):
        started = time.time()
        handle = open(stdout, 'wb') if stdout else subprocess.PIPE
        proc = subprocess.run([str(a) for a in argv], cwd=str(cwd), env=env,
                              stdout=handle, stderr=subprocess.PIPE, timeout=self.timeout)
        if stdout:
            handle.close()
            out = b''
        else:
            out = proc.stdout
        log = self.work / (label + '.log')
        log.write_bytes(out + proc.stderr)
        row = {'step': label, 'exit_code': proc.returncode, 'log': log.name,
               'seconds': round(time.time() - started, 2),
               'command': ' '.join(str(a) for a in argv)}
        self.steps.append(row)
        print('%s EXIT=%s' % (label, proc.returncode), flush=True)
        return proc.returncode, row

    def stage_audio(self):
        out = self.work / 'audio-src'
        shutil.copytree(ROOT / 'references/livekit-stable', out,
                        ignore=shutil.ignore_patterns('.git'))
        for name in AUDIO_OBSERVERS:
            shutil.copy2(AUDIO / name, out / 'pkg/rtc' / name)
        mod = out / 'go.mod'
        text = mod.read_text()
        for name, rel in OVERLAYS:
            text += '\nreplace %s => %s\n' % (name, json.dumps(str((ROOT / rel).resolve())))
        mod.write_text(text)
        return out

    def stage_video(self):
        out = self.work / 'video-src'
        code, _ = self.run('stage-video',
                           [sys.executable, VIDEO / 'prepare.py', '--root', ROOT, '--out', out])
        return out if code == 0 else None

    def compile_go(self, tree, label):
        binary = self.work / (tree + '-rtc.test')
        code, row = self.run('compile-' + label,
                             [self.go, 'test', '-c', '-mod=readonly', '-o', binary, './pkg/rtc'],
                             cwd=self.work / (tree + '-src'), env=self.goenv)
        if code != 0:
            return None, row
        row['binary_sha256'] = sha(binary)
        return binary, row

    def receipt(self, path, kind, exit_code, actual, binary, source):
        doc = {'kind': kind, 'exit_code': exit_code,
               'actual_sha256': sha(actual), 'binary_sha256': sha(binary),
               'source_sha256': sha(source)}
        path.write_text(json.dumps(doc, indent=2) + '\n')
        return doc

    def finish(self, selected, results, overall):
        counts = {}
        for entry in results.values():
            counts[entry['status']] = counts.get(entry['status'], 0) + 1
        summary = {'status': 'PASS' if overall == 0 and len(results) == len(selected) else 'FAIL',
                   'profiles': selected, 'status_counts': counts,
                   'rust_source_sha256': sha(RUST_SOURCE), 'steps': self.steps,
                   'results': results,
                   'scope': 'actual owner metadata/flags, callback/version and missing core '
                            'signal; Go receiver pointer/telemetry mapped to Rust metadata '
                            'field change, not a native Rust receiver/telemetry observation'}
        (self.work / 'summary.json').write_text(json.dumps(summary, indent=2) + '\n')
        print('OWNER_DIFFERENTIAL_COUNTS %s' % json.dumps(counts), flush=True)
        print('OWNER_DIFFERENTIAL_EXIT=%d' % overall, flush=True)
        return overall


def main():
    p = argparse.ArgumentParser()
    p.add_argument('--work', type=pathlib.Path, required=True)
    p.add_argument('--rust-binary', type=pathlib.Path, required=True)
    p.add_argument('--go', type=pathlib.Path, required=True)
    p.add_argument('--profiles', default='go11,compact9,video10')
    p.add_argument('--timeout', type=int, default=1800)
    a = p.parse_args()
    if a.work.exists():
        print('work dir already exists; refusing to reuse stale create_new outputs', flush=True)
        return 1
    if not a.rust_binary.is_file():
        print('rust binary missing: %s' % a.rust_binary, flush=True)
        return 1
    a.work.mkdir(parents=True)
    d = Driver(a.work, a.go, a.timeout)
    selected = [s.strip() for s in a.profiles.split(',') if s.strip()]
    for name in selected:
        if name not in PROFILES:
            print('unknown profile %s' % name, flush=True)
            return 1

    trees = {}
    if any(PROFILES[n]['tree'] == 'audio' for n in selected):
        trees['audio'] = d.stage_audio()
    if any(PROFILES[n]['tree'] == 'video' for n in selected):
        trees['video'] = d.stage_video()
        if trees['video'] is None:
            return d.finish(selected, {}, 1)

    compiled = {}
    for tree in trees:
        binary, row = d.compile_go(tree, tree)
        if binary is None:
            return d.finish(selected, {}, 1)
        compiled[tree] = (binary, row['binary_sha256'])

    results = {}
    overall = 0
    for name in selected:
        spec = PROFILES[name]
        tree = spec['tree']
        go_binary, go_sha = compiled[tree]
        go_out = a.work / spec['go_out']
        rust_out = a.work / spec['rust_out']
        for path in (go_out, rust_out):
            if path.exists():
                print('%s: output already exists %s' % (name, path), flush=True)
                return d.finish(selected, results, 1)

        go_code, _ = d.run('go-' + name,
                           [go_binary, '-test.run', '^%s$' % spec['go_test'],
                            '-test.count=1', '-test.timeout=180s'],
                           cwd=a.work / (tree + '-src'),
                           env=dict(d.goenv, **{spec['go_env']: str(go_out)}))
        rust_code, rust_row = d.run(
            'rust-' + name,
            [a.rust_binary, '--ignored', '--exact', spec['rust_test'],
             '--nocapture', '--test-threads=1'],
            env=dict(os.environ, **{spec['rust_env']: str(rust_out)}))
        rust_row['binary_sha256'] = sha(a.rust_binary)
        rust_row['binary_path'] = str(a.rust_binary)
        entry = {'rust_test': spec['rust_test'],
                 'go_exit_code': go_code, 'rust_exit_code': rust_code,
                 'rust_binary_sha256': sha(a.rust_binary),
                 'go_binary_sha256': go_sha}
        if go_code != 0 or rust_code != 0:
            entry['status'] = 'FAIL'
            entry['reason'] = 'producer process failed'
            results[name] = entry
            overall = 1
            continue
        if not go_out.is_file() or not rust_out.is_file():
            entry['status'] = 'FAIL'
            entry['reason'] = 'producer wrote no output'
            results[name] = entry
            overall = 1
            continue

        if spec['reader'] == 'go11':
            go_receipt = a.work / (name + '-go-receipt.json')
            rust_receipt = a.work / (name + '-rust-receipt.json')
            d.receipt(go_receipt, spec['go_kind'], go_code, go_out, go_binary,
                      AUDIO / spec['observer'])
            d.receipt(rust_receipt, spec['rust_kind'], rust_code, rust_out,
                      a.rust_binary, RUST_SOURCE)
            out = a.work / (name + '-comparison.json')
            code, _ = d.run('compare-' + name,
                            [sys.executable, AUDIO / 'read-go11.py', '--profile', name,
                             '--go', go_out, '--rust', rust_out,
                             '--go-receipt', go_receipt, '--rust-receipt', rust_receipt,
                             '--rust-source', RUST_SOURCE, '--out', out])
        else:
            out = a.work / (name + '-comparison.json')
            code, _ = d.run('compare-' + name,
                            [sys.executable, VIDEO / 'read-video10.py',
                             '--go', go_out, '--rust', rust_out], stdout=out)
        entry['comparison_exit_code'] = code
        entry['comparison'] = str(out)
        if code == 0 and out.is_file():
            report = json.loads(out.read_text())
            entry['status'] = 'PASS'
            entry['comparison_status'] = report.get('status')
            entry['rows'] = report.get('rows')
            entry['scope'] = report.get('scope')
            entry['open'] = report.get('open')
        else:
            entry['status'] = 'FAIL'
            overall = 1
        results[name] = entry

    return d.finish(selected, results, overall)


if __name__ == '__main__':
    sys.exit(main())