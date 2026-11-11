#!/usr/bin/env python3
"""Preserve frozen source bytes, then actually restore every mapped file.

UTF-8 text uses a checked/applied Git patch; opaque/binary input uses SHA256
blobs. Recovery maps are authoritative subsets of a pushed base, not overlays
on a full checkout. No index, commit, reference, or candidate archive is changed.
"""
import argparse
import difflib
import hashlib
import json
import os
from pathlib import Path, PurePosixPath
import re
import stat
import subprocess
import tempfile


class PreservationError(RuntimeError):
    pass


def digest(data):
    return hashlib.sha256(data).hexdigest()


def require(condition, message):
    if not condition:
        raise PreservationError(message)


def relative_path(value):
    require(isinstance(value, str) and value, 'relative path must be a nonempty string')
    path = PurePosixPath(value)
    require(not path.is_absolute() and str(path) == value,
            'noncanonical/absolute relative path: ' + repr(value))
    require(all(part not in ('.', '..') and part.lower() != '.git' for part in path.parts),
            'unsafe path component: ' + repr(value))
    require('\\' not in value and not any(ord(c) < 32 or ord(c) == 127 for c in value),
            'control/backslash in relative path: ' + repr(value))
    return path


def paths_without_collisions(files):
    for name in files:
        relative_path(name)
        for parent in PurePosixPath(name).parents:
            require(str(parent) == '.' or str(parent) not in files,
                    'file/directory collision: ' + repr(name))


def git_environment():
    env = dict(os.environ)
    for key in ('GIT_DIR', 'GIT_WORK_TREE', 'GIT_INDEX_FILE', 'GIT_COMMON_DIR'):
        env.pop(key, None)
    env['GIT_OPTIONAL_LOCKS'] = '0'
    env['GIT_TERMINAL_PROMPT'] = '0'
    return env


class BaseTree:
    """One read-only batch process avoids thousands of Git show subprocesses."""
    def __init__(self, repository, revision):
        self.repository = repository.resolve(strict=True)
        self.env = git_environment()
        result = subprocess.run(
            ['git', 'rev-parse', '--verify', '--end-of-options', revision + '^{commit}'],
            cwd=self.repository, env=self.env, capture_output=True, check=True)
        self.commit = result.stdout.decode('ascii').strip()
        def list_tree(*paths):
            return subprocess.run(
                ['git', 'ls-tree', '-r', '-z', self.commit, '--', *paths],
                cwd=self.repository, env=self.env, capture_output=True, check=True).stdout

        prefix = 'livekit-rs-server/'
        listing = list_tree(prefix)
        if not listing:
            prefix = ''
            listing = list_tree()
        self.entries = {}
        for entry in listing.split(b'\0'):
            if not entry:
                continue
            header, path = entry.split(b'\t', 1)
            mode, kind, oid = header.split()
            if kind != b'blob':
                continue
            name = path.decode('utf8')
            relative = name.removeprefix(prefix)
            self.entries[relative] = (oid, mode)
        self.cache = {}
        self.reader = subprocess.Popen(
            ['git', 'cat-file', '--batch'], cwd=self.repository, env=self.env,
            stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

    def get(self, relative):
        entry = self.entries.get(relative)
        if entry is None:
            return None
        oid, mode = entry
        require(mode != b'120000', 'base symlink cannot be restored as source bytes: ' + relative)
        if oid not in self.cache:
            self.reader.stdin.write(oid + b'\n')
            self.reader.stdin.flush()
            header = self.reader.stdout.readline().split()
            require(len(header) == 3 and header[0] == oid and header[1] == b'blob',
                    'unexpected git cat-file response for ' + relative)
            size = int(header[2])
            data = self.reader.stdout.read(size)
            require(len(data) == size and self.reader.stdout.read(1) == b'\n',
                    'truncated git blob for ' + relative)
            self.cache[oid] = data
        return self.cache[oid]

    def close(self):
        if self.reader.stdin:
            self.reader.stdin.close()
        code = self.reader.wait(timeout=30)
        error = self.reader.stderr.read()
        self.reader.stdout.close()
        self.reader.stderr.close()
        require(code == 0, 'git cat-file failed: ' + repr(error))


def patch_text(data):
    if b'\0' in data:
        return None
    try:
        return data.decode('utf8')
    except UnicodeDecodeError:
        return None


def lf_lines(text):
    # Git patches split at LF only: str.splitlines also splits valid Unicode
    # separators inside source literals, which would corrupt an otherwise UTF-8 diff.
    chunks = text.split('\n')
    return [line + '\n' for line in chunks[:-1]] + ([chunks[-1]] if chunks[-1] else [])


def text_change(relative, before, data):
    label = 'livekit-rs-server/' + relative
    lines = ['diff --git a/' + label + ' b/' + label + '\n']
    if before is None:
        lines.append('new file mode 100644\n')
    old = lf_lines((before or b'').decode('utf8'))
    new = lf_lines(data.decode('utf8'))
    for line in difflib.unified_diff(
            old, new, fromfile='a/' + label if before is not None else '/dev/null',
            tofile='b/' + label):
        if line.endswith('\n'):
            lines.append(line)
        else:
            lines.extend([line + '\n', '\\ No newline at end of file\n'])
    return ''.join(lines)


def owned_artifact(root, name):
    path = root.joinpath(*relative_path(name).parts)
    require(path.resolve().is_relative_to(root.resolve()), 'artifact escapes output: ' + name)
    require(path.is_file() and not path.is_symlink(), 'missing/nonregular artifact: ' + name)
    return path


def store_blob(output, data):
    sha = digest(data)
    name = 'blobs/sha256/' + sha[:2] + '/' + sha[2:]
    path = output / name
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists():
        require(digest(path.read_bytes()) == sha, 'existing content-addressed blob is corrupt')
    else:
        with path.open('xb') as file:
            file.write(data)
    return name


def restore_mapping(mapping_path, base, tree, diagnostics):
    mapping = json.loads(mapping_path.read_text())
    require(mapping.get('schema') == 'frozen-source-recovery-v2', 'unknown recovery schema')
    require(mapping['base_commit'] == base.commit, 'recovery base commit mismatch')
    files = mapping['files']
    paths_without_collisions(files)
    patch = owned_artifact(mapping_path.parent, mapping['patch'])
    require(digest(patch.read_bytes()) == mapping['patch_sha256'], 'text patch digest mismatch')
    tree.mkdir(parents=True, exist_ok=False)
    server = tree / 'livekit-rs-server'
    server.mkdir()
    for relative, row in files.items():
        storage = row['storage']
        require(storage in ('base', 'text-patch', 'sha256-blob'), 'unknown storage: ' + relative)
        if storage == 'sha256-blob':
            continue
        before = base.get(relative)
        require((digest(before) if before is not None else None) == row['base_sha256'],
                'base file identity mismatch: ' + relative)
        if before is not None:
            target = server / relative
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_bytes(before)
    if patch.stat().st_size:
        for phase, extra in (('check', ['--check']), ('apply', [])):
            command = ['git', 'apply'] + extra + [str(patch.resolve())]
            result = subprocess.run(command, cwd=tree, env=base.env, capture_output=True)
            diagnostics.append({'phase': phase, 'command': command, 'exit': result.returncode,
                                'stdout': result.stdout.decode('utf8', errors='replace'),
                                'stderr': result.stderr.decode('utf8', errors='replace')})
            require(result.returncode == 0, 'git apply ' + phase + ' failed; see diagnostics')
    restored = {}
    binary_files = 0
    for relative, row in files.items():
        target = server / relative
        if row['storage'] == 'sha256-blob':
            blob = owned_artifact(mapping_path.parent, row['blob'])
            data = blob.read_bytes()
            require(digest(data) == row['sha256'], 'binary blob digest mismatch: ' + relative)
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_bytes(data)
            binary_files += 1
        require(target.is_file() and not target.is_symlink(), 'restored file missing: ' + relative)
        actual = digest(target.read_bytes())
        require(actual == row['sha256'], 'rehydrated byte mismatch: ' + relative)
        mode = row['source_mode']
        require(isinstance(mode, int) and 0 <= mode <= 0o777, 'unsafe source mode')
        target.chmod(mode)
        restored[relative] = actual
    actual_paths = {str(p.relative_to(server)) for p in server.rglob('*') if p.is_file()}
    require(actual_paths == set(files), 'extra or missing files in reconstructed source inventory')
    return {'verified_source_files': len(restored), 'restored_binary_or_opaque_blobs': binary_files,
            'all_files_sha256': restored, 'status': 'EXACT_SOURCE_REHYDRATION_PASS'}


def preserve_one(manifest, output, base, diagnostics):
    record = json.loads(manifest.read_bytes())
    files = record['source_files']
    paths_without_collisions(files)
    source = Path(record['source_directory']).resolve(strict=True)
    name = manifest.stem
    require(re.fullmatch(r'[A-Za-z0-9_.-]+', name), 'unsafe manifest artifact basename')
    patch = output / (name + '.patch')
    mapping_path = output / (name + '.recovery.json')
    require(not patch.exists() and not mapping_path.exists(), 'duplicate manifest artifact name')
    changes = []
    mapping = {'schema': 'frozen-source-recovery-v2', 'base_commit': base.commit,
               'manifest': name + '.input-manifest.json', 'manifest_sha256': digest(manifest.read_bytes()),
               'source_directory_at_capture': str(source),
               'inventory': 'Authoritative source subset; seed only listed base files into a NEW empty tree.',
               'patch': patch.name, 'files': {}}
    for relative, wanted in files.items():
        require(isinstance(wanted, str) and re.fullmatch(r'[0-9a-f]{64}', wanted),
                'invalid source SHA256: ' + relative)
        path = source / relative
        require(path.resolve().is_relative_to(source) and not path.is_symlink(),
                'source symlink/path escapes frozen tree: ' + relative)
        data = path.read_bytes()
        require(digest(data) == wanted, 'frozen input changed: ' + relative)
        before = base.get(relative)
        row = {'sha256': wanted, 'source_mode': stat.S_IMODE(path.stat().st_mode) & 0o777,
               'base_sha256': digest(before) if before is not None else None}
        if before == data:
            row['storage'] = 'base'
        elif (patch_text(data) is None or (before is not None and patch_text(before) is None)
              or not re.fullmatch(r'[A-Za-z0-9_.@/+,=-]+', relative) or (before is None and not data)):
            row.update(storage='sha256-blob', blob=store_blob(output, data))
            row['reason'] = 'binary/opaque UTF8, nonplain patch path, or new empty file; exact bytes retained'
        else:
            row['storage'] = 'text-patch'
            changes.append(text_change(relative, before, data))
        mapping['files'][relative] = row
    patch.write_bytes(''.join(changes).encode('utf8'))
    mapping['patch_sha256'] = digest(patch.read_bytes())
    (output / mapping['manifest']).write_bytes(manifest.read_bytes())
    mapping_path.write_text(json.dumps(mapping, indent=2) + '\n')
    with tempfile.TemporaryDirectory(prefix='livekit-rehydrate-owned-') as work:
        receipt = restore_mapping(mapping_path, base, Path(work) / 'tree', diagnostics)
    receipt.update(manifest=str(manifest), manifest_sha256=mapping['manifest_sha256'],
                   recovery_map=mapping_path.name, recovery_map_sha256=digest(mapping_path.read_bytes()),
                   patch=patch.name, patch_sha256=mapping['patch_sha256'],
                   temporary_rehydration_tree_removed=True)
    return receipt


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--repository', type=Path, required=True)
    parser.add_argument('--base', help='pushed base commit; recovery defaults to the map commit')
    parser.add_argument('--manifest', type=Path, action='append')
    parser.add_argument('--output', type=Path)
    parser.add_argument('--rehydrate', type=Path, help='replay an existing recovery map without old candidate directories')
    parser.add_argument('--restore-to', type=Path, help='NEW empty parent tree for --rehydrate')
    args = parser.parse_args()
    if args.rehydrate:
        require(args.restore_to is not None and not args.manifest and args.output is None,
                '--rehydrate requires --restore-to and excludes --manifest/--output')
        mapping = json.loads(args.rehydrate.read_bytes())
        base = BaseTree(args.repository, args.base or mapping['base_commit'])
        diagnostics = []
        try:
            receipt = restore_mapping(args.rehydrate.resolve(), base, args.restore_to.resolve(), diagnostics)
            receipt['git_apply_diagnostics'] = diagnostics
            print(json.dumps(receipt, indent=2))
        finally:
            base.close()
        return
    require(args.base and args.manifest and args.output is not None and args.restore_to is None,
            'capture requires --base, --manifest and --output')
    output = args.output.resolve()
    output.mkdir(parents=True, exist_ok=False)
    result = {'status': 'FAILED', 'requested_base': args.base,
              'scope': 'Exact source preservation/recovery only; no test or functional acceptance credit.',
              'runner_sha256': digest(Path(__file__).read_bytes()), 'candidates': [], 'git_apply_diagnostics': []}
    base = None
    try:
        base = BaseTree(args.repository, args.base)
        result['base_commit'] = base.commit
        for manifest in args.manifest:
            result['current_manifest'] = str(manifest)
            diagnostics = []
            try:
                receipt = preserve_one(manifest.resolve(strict=True), output, base, diagnostics)
                result['candidates'].append(receipt)
            finally:
                result['git_apply_diagnostics'].append({'manifest': str(manifest), 'operations': diagnostics})
        result.pop('current_manifest', None)
        result['status'] = 'ALL_EXACT_SOURCE_REHYDRATIONS_PASS'
        result['content_addressed_blob_files'] = len(list((output / 'blobs').rglob('*'))) if (output / 'blobs').exists() else 0
        result['content_addressed_blob_files'] = sum(p.is_file() for p in (output / 'blobs').rglob('*')) if (output / 'blobs').exists() else 0
    except Exception as error:
        result['error'] = {'type': type(error).__name__, 'message': str(error)}
        raise
    finally:
        if base is not None:
            try:
                base.close()
            except Exception as error:
                result['status'] = 'FAILED'
                result['base_reader_error'] = str(error)
        (output / 'receipt.json').write_text(json.dumps(result, indent=2) + '\n')
    require(result['status'] == 'ALL_EXACT_SOURCE_REHYDRATIONS_PASS', 'base reader failed; see receipt')
    print(json.dumps({'status': result['status'], 'candidates': len(result['candidates']),
                      'content_addressed_blobs': result['content_addressed_blob_files']}))


if __name__ == '__main__':
    main()
