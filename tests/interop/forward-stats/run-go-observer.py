#!/usr/bin/env python3
"""Stage unchanged fixed server package and call original ForwardStats summaries.
No Cargo, builds of server binaries, global services, or reference mutation.
"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import shutil
import signal
import stat
import subprocess
import tempfile


def digest(path): return hashlib.sha256(path.read_bytes()).hexdigest()
def inventory(root):
    return {str(p.relative_to(root)): digest(p) for p in root.rglob('*') if p.is_file() and '.git' not in p.parts}


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--go-binary',type=Path,help='direct fixed Go1.26.3 binary; no toolchain loader download')
    parser.add_argument('--fixed-server-source', type=Path, required=True)
    parser.add_argument('--evidence-dir', type=Path, required=True)
    args = parser.parse_args()
    go_source = args.fixed_server_source.resolve(strict=True)
    mtu = go_source
    observer = Path(__file__).with_name('original_observer_test.go')
    go_binary = args.go_binary.resolve(strict=True) if args.go_binary else Path(shutil.which('go') or 'missing-go').resolve(strict=True)
    output = args.evidence_dir.resolve(); output.mkdir(parents=True, exist_ok=False)
    before = inventory(mtu)
    result = {'status': 'FAILED', 'reference_server_files_before': before,
              'observer_sha256': digest(observer), 'runner_sha256': digest(Path(__file__)),
              'go_binary':str(go_binary),'go_binary_sha256':digest(go_binary),
              'fixed_go_mod_sha256': digest(go_source/'go.mod'), 'fixed_go_sum_sha256': digest(go_source/'go.sum')}
    try:
        with tempfile.TemporaryDirectory(prefix='owned-server-original-observer-') as temp:
            staging = Path(temp); copied = staging/'livekit-server'
            shutil.copytree(mtu, copied, ignore=shutil.ignore_patterns('.git','target','node_modules'))
            if inventory(copied) != before: raise RuntimeError('staged source copy identity mismatch')
            # Reference inputs are intentionally read-only. copytree preserves
            # those directory modes; grant owner-write ONLY to our private copy
            # so adding the observer and final TemporaryDirectory cleanup work.
            # Original files remain byte-identical and read-only.
            copied.chmod(copied.stat().st_mode | stat.S_IWUSR | stat.S_IXUSR)
            for directory in copied.rglob('*'):
                if directory.is_dir() and not directory.is_symlink():
                    directory.chmod(directory.stat().st_mode | stat.S_IWUSR | stat.S_IXUSR)
            result['staged_directory_permissions'] = 'task-owned directories owner-write; reference modes unchanged'
            shutil.copy2(observer, copied/'pkg/sfu/zz_livekit_forward_observer_test.go')
            env = dict(os.environ, GOPROXY='off', GOSUMDB='off', GOWORK='off', GOTOOLCHAIN='local', FORWARD_STATS_GO_OBSERVATION=str(output/'observation.json'))
            version = subprocess.run([str(go_binary),'version'],cwd=copied,env=env,capture_output=True,timeout=15)
            result['go_version_exit'] = version.returncode
            result['go_version'] = version.stdout.decode('utf8',errors='replace').strip()
            (output/'go-version.stderr').write_bytes(version.stderr)
            if version.returncode!=0 or not result['go_version'].startswith('go version go1.26.3 '):
                raise RuntimeError('original observer requires the fixed Go1.26.3 toolchain')
            command = [str(go_binary),'test','-mod=readonly',
                       './pkg/sfu',
                       '-run','^TestOriginalForwardStatsObserver$','-count=1','-v']
            result['command'] = command
            proc = subprocess.Popen(command,cwd=copied,env=env,stdout=subprocess.PIPE,stderr=subprocess.PIPE,start_new_session=True)
            result['owned_process_group'] = proc.pid
            try:
                stdout,stderr = proc.communicate(timeout=90)
            except subprocess.TimeoutExpired:
                result['forced_timeout_cleanup'] = True
                try: os.killpg(proc.pid,signal.SIGTERM)
                except ProcessLookupError: pass
                try: stdout,stderr = proc.communicate(timeout=5)
                except subprocess.TimeoutExpired:
                    try: os.killpg(proc.pid,signal.SIGKILL)
                    except ProcessLookupError: pass
                    stdout,stderr = proc.communicate()
            (output/'stdout.log').write_bytes(stdout); (output/'stderr.log').write_bytes(stderr)
            result['exit'] = proc.returncode
            result['actual_test_pass_lines'] = stdout.count(b'--- PASS: TestOriginalForwardStatsObserver')
            observation = json.loads((output/'observation.json').read_text())
            result['actual_rows'] = len(observation['rows'])
            result['status'] = 'PASS' if proc.returncode == 0 and not result.get('forced_timeout_cleanup') and result['actual_test_pass_lines'] == 1 and result['actual_rows'] == 12 else 'FAILED'
    except Exception as error:
        result['error'] = str(error)
    finally:
        result['reference_server_files_after'] = inventory(mtu)
        result['reference_identity_unchanged'] = result['reference_server_files_after'] == before
        result['go_binary_identity_unchanged'] = digest(go_binary) == result['go_binary_sha256']
        result['runner_identity_unchanged'] = digest(Path(__file__)) == result['runner_sha256'] and digest(observer) == result['observer_sha256']
        result['accepted'] = result['status'] == 'PASS' and result['go_binary_identity_unchanged'] and result['reference_identity_unchanged'] and result['runner_identity_unchanged']
        (output/'result.json').write_text(json.dumps(result,indent=2)+'\n')
    raise SystemExit(0 if result['accepted'] else 1)


if __name__ == '__main__': main()
