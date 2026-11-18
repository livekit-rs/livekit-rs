#!/usr/bin/env python3
"""Offline original-Go public observer. Preserve every attempt, including failures."""
import argparse
import json
import os
from pathlib import Path
import shutil
import signal
import subprocess
import sys
import tempfile
import traceback

sys.dont_write_bytecode = True
from prepare import FIXED_HEADS, HERE, checked_parent, git, mkdir, require, sha256

GO = Path('/Users/zoujiaqing/go/pkg/mod/golang.org/toolchain@v0.0.1-go1.26.0.darwin-arm64/bin/go')
MODULE_CACHE = Path('/Users/zoujiaqing/go/pkg/mod')


def escape(value):
    return ''.join('!' + c.lower() if c.isupper() else c for c in value)


def seed_cache(stage, dest):
    # C62 staging pattern: private APFS copies, not writable shared modules or hardlinks.
    identities = set()
    for relative in FIXED_HEADS:
        for line in (stage / 'sources' / relative / 'go.sum').read_text().splitlines():
            name, version, _ = line.split()
            identities.add((escape(name), escape(version.removesuffix('/go.mod'))))
    copied = []
    for name, version in sorted(identities):
        relative = name + '@' + version
        source = MODULE_CACHE / relative
        if source.is_dir():
            target = dest / relative
            mkdir(target.parent)
            checked_parent(target)
            subprocess.run(['/bin/cp', '-cR', str(source), str(target)], check=True, timeout=60)
            copied.append(relative)
        metadata = Path('cache/download') / name / '@v'
        for suffix in ('.mod', '.info', '.ziphash'):
            source = MODULE_CACHE / metadata / (version + suffix)
            if source.is_file():
                target = dest / metadata / source.name
                mkdir(target.parent)
                checked_parent(target)
                shutil.copyfile(source, target)
    return copied


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--fixed-source-root', type=Path, default=HERE.parents[2])
    args = parser.parse_args()
    checked_parent(Path('/private/tmp/video-publication-metadata-go-new'))
    work = Path(tempfile.mkdtemp(prefix='video-publication-metadata-go-', dir='/private/tmp')).resolve()
    print(work, flush=True)
    stage = work / 'stage'
    for name in ('cache', 'tmp', 'modcache', 'gopath'):
        mkdir(work / name)
    overrides = {
        'GOTOOLCHAIN': 'local', 'GOWORK': 'off', 'GOPROXY': 'off', 'GOSUMDB': 'off',
        'GOENV': 'off', 'GOFLAGS': '-mod=readonly -p=4 -buildvcs=false',
        'GOCACHE': str(work / 'cache'), 'GOMODCACHE': str(work / 'modcache'),
        'GOPATH': str(work / 'gopath'), 'GOTMPDIR': str(work / 'tmp'),
        'TMPDIR': str(work / 'tmp'), 'GOTELEMETRY': 'off', 'GOMAXPROCS': '2',
        'GOROOT': str(GO.parent.parent), 'GOOS': 'darwin', 'GOARCH': 'arm64',
        'CGO_ENABLED': '1', 'GOEXPERIMENT': '', 'GODEBUG': '',
        'PYTHONDONTWRITEBYTECODE': '1',
    }
    env = dict(os.environ)
    env.pop('VIDEO_PUBLICATION_METADATA_GO_JSONL', None)
    env.update(overrides)
    commands, dependencies = [], {}
    provenance = {'environment': overrides, 'outcome': 'INCOMPLETE'}
    error_text, exit_code = None, 1

    def save():
        (work / 'commands.json').write_text(json.dumps(commands, indent=2) + '\n')
        (work / 'provenance.json').write_text(json.dumps(provenance, indent=2, sort_keys=True) + '\n')

    def run(name, command, timeout=600, extra_env=None, allow_failure=False):
        checked_parent(work / (name + '.stdout'))
        entry = dict(name=name, command=list(map(str, command)), environment_override=extra_env or {},
                     timeout_seconds=timeout, cwd=str(stage if stage.is_dir() else work))
        commands.append(entry)
        save()
        try:
            with (work / (name + '.stdout')).open('xb') as out, (work / (name + '.stderr')).open('xb') as err:
                # Kill only this task-created process group on deadline; no leaked
                # go compiler/test children, and no signalling unrelated processes.
                proc = subprocess.Popen(entry['command'], cwd=entry['cwd'], env=dict(env, **(extra_env or {})),
                                        stdout=out, stderr=err, start_new_session=True)
                entry['pid'] = proc.pid
                try:
                    entry['exit_code'] = proc.wait(timeout=timeout)
                except subprocess.TimeoutExpired:
                    entry['timed_out'] = True
                    os.killpg(proc.pid, signal.SIGKILL)
                    entry['exit_code'] = proc.wait()
                    raise
            if not allow_failure and entry['exit_code'] != 0:
                raise subprocess.CalledProcessError(entry['exit_code'], entry['command'])
        except OSError as error:
            entry['launch_error'] = str(error)
            raise
        finally:
            entry['output_sha256'] = {s: sha256(work / (name + s)) for s in ('.stdout', '.stderr') if (work / (name + s)).is_file()}
            with (work / (name + '.command.json')).open('x') as receipt:
                json.dump(entry, receipt, indent=2)
                receipt.write('\n')
            save()
        return (work / (name + '.stdout')).read_text()

    try:
        run('prepare', ['/usr/bin/python3', '-B', HERE / 'prepare.py', '--fixed-source-root', args.fixed_source_root.resolve(), '--out', stage])
        provenance.update(json.loads((stage / 'provenance.json').read_text()))
        require(sha256(GO) == 'ae2238c208f2f1f5a8faa3287acf5ced5d6b3c396dbf75e8f446e47905b0a75d', 'Go binary identity drift')
        provenance['go_binary'] = dict(path=str(GO), sha256=sha256(GO))
        version = run('go-version', [GO, 'version']).strip()
        require(version == 'go version go1.26.0 darwin/arm64', 'unexpected Go version')
        provenance['go_binary']['version'] = version
        provenance['seeded_modules'] = seed_cache(stage, work / 'modcache')
        run('go-env', [GO, 'env', '-json'])
        require(not run('gofmt-check', [GO.parent / 'gofmt', '-l', stage / 'main_test.go']).strip(), 'fixture not formatted')
        run('validator-self-test', ['/usr/bin/python3', '-B', stage / 'assets/validate.py', '--self-test'])
        packages = run('dependencies', [GO, 'list', '-deps', '-test', '-json', '.'])
        decoder, offset = json.JSONDecoder(), 0
        while offset < len(packages):
            while offset < len(packages) and packages[offset].isspace():
                offset += 1
            if offset == len(packages):
                break
            package, offset = decoder.raw_decode(packages, offset)
            for key in ('GoFiles', 'TestGoFiles', 'XTestGoFiles', 'CgoFiles', 'CFiles', 'HFiles', 'SFiles', 'SysoFiles', 'EmbedFiles'):
                for name in package.get(key, []):
                    path = Path(package['Dir']) / name
                    dependencies[str(path)] = sha256(path)
        (work / 'dependency-source-sha256.json').write_text(json.dumps(dependencies, indent=2, sort_keys=True) + '\n')
        run('build-tests', [GO, 'test', '-race', '-c', '-o', work / 'observer.test', '.'])
        provenance['binary_sha256'] = sha256(work / 'observer.test')
        run('build-info', [GO, 'version', '-m', work / 'observer.test'])
        for name in ('observations', 'repeat'):
            output = work / (name + '.jsonl')
            run(name, [work / 'observer.test', '-test.run=^TestObserve$', '-test.v', '-test.count=1', '-test.timeout=90s'],
                timeout=100, extra_env={'VIDEO_PUBLICATION_METADATA_GO_JSONL': str(output)}, allow_failure=True)
            run(name + '-validate', ['/usr/bin/python3', '-B', stage / 'assets/validate.py', '--input', output], allow_failure=True)
        first, second = [(work / (n + '.jsonl')).read_bytes() for n in ('observations', 'repeat')]
        provenance['deterministic_repeat'] = [json.loads(x) for x in first.splitlines()] == [json.loads(x) for x in second.splitlines()]
        output = work / 'observations.jsonl'
        original_hash = sha256(output)
        run('existing-output-control', [work / 'observer.test', '-test.run=^TestObserve$', '-test.count=1', '-test.timeout=10s'],
            timeout=15, extra_env={'VIDEO_PUBLICATION_METADATA_GO_JSONL': str(output)}, allow_failure=True)
        require(commands[-1]['exit_code'] == 1 and 'file exists' in (work / 'existing-output-control.stdout').read_text(), 'missing O_EXCL rejection')
        require(sha256(output) == original_hash, 'exclusive control changed raw evidence')
        success = provenance['deterministic_repeat'] and all(c.get('exit_code') == 0 for c in commands if c['name'] != 'existing-output-control')
        exit_code = 0 if success else 1
        provenance['outcome'] = 'PASS' if success else 'FAIL: see original command exits; no evidence rewritten'
    except BaseException:
        error_text = traceback.format_exc()
        print(error_text, file=sys.stderr)
        provenance['outcome'] = 'FAIL: ' + error_text.splitlines()[-1]
    finally:
        # Audit both successes and failures. Original and staged references, all
        # compiler inputs, fixtures and executable bytes must remain unchanged.
        audit_errors = []
        def audit(condition, message):
            if not condition:
                audit_errors.append(message)
        try:
            for relative, expected in provenance.get('source_sha256', {}).items():
                for root in (args.fixed_source_root.resolve(), stage / 'sources'):
                    audit(sha256(root / relative) == expected, 'source changed: ' + str(root / relative))
            for path, expected in dependencies.items():
                audit(sha256(Path(path)) == expected, 'compiled dependency changed: ' + path)
            for name, expected in provenance.get('staged_sha256', {}).items():
                audit(sha256(stage / name) == expected, 'staged input changed: ' + name)
            for name, expected in provenance.get('asset_sha256', {}).items():
                audit(sha256(HERE / name) == expected and sha256(stage / 'assets' / name) == expected, 'fixture changed: ' + name)
            for relative, expected in FIXED_HEADS.items():
                for root in (args.fixed_source_root.resolve(), stage / 'sources'):
                    if (root / relative).exists():
                        audit(git(root / relative, 'rev-parse', 'HEAD') == expected, 'HEAD changed: ' + relative)
                        audit(not git(root / relative, 'status', '--porcelain', '--untracked-files=all'), 'reference dirtied: ' + relative)
            if 'binary_sha256' in provenance:
                audit(sha256(work / 'observer.test') == provenance['binary_sha256'], 'observer binary changed')
            if 'go_binary' in provenance:
                audit(sha256(GO) == provenance['go_binary']['sha256'], 'Go binary changed')
        except BaseException:
            audit_errors.append(traceback.format_exc())
        provenance['immutability_audit_errors'] = audit_errors
        if audit_errors:
            exit_code = 1
            provenance['outcome'] = 'FAIL: immutability audit'
        provenance['evidence_sha256'] = {p.name: sha256(p) for p in work.iterdir() if p.is_file() and p.name != 'provenance.json'}
        save()
        (work / 'runner.command.json').write_text(json.dumps(dict(argv=sys.argv, exit_code=exit_code, exception=error_text), indent=2) + '\n')
        print(provenance['outcome'], flush=True)
        print(work, flush=True)
    return exit_code


if __name__ == '__main__':
    raise SystemExit(main())
