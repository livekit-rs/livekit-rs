#!/usr/bin/env python3
"""Execute unchanged official and candidate CLI commands, never start servers.
Oracle is the actual pinned official Go executable, not a reconstructed loader.
No complete JWTs/secrets are persisted; verified claims and token digests remain.
"""
import argparse
import base64
import hashlib
import hmac
import json
import os
from pathlib import Path
import subprocess
import tempfile
import time

SECRET = 'public-fixture-only-secret-0123456789'

def b64(value):
    return base64.urlsafe_b64decode(value + '=' * (-len(value) % 4))

def run(binary, args, keys, room, identity, recorder):
    start = int(time.time())
    result = subprocess.run([str(binary), *args], capture_output=True, timeout=20,
                            env={'PATH': os.environ.get('PATH', '/usr/bin:/bin')})
    end = int(time.time())
    stdout = result.stdout.decode('utf-8', 'replace')
    stderr = result.stderr.decode('utf-8', 'replace')
    observation = {'exit': result.returncode, 'stderr': stderr}
    if result.returncode:
        observation['stdout'] = stdout
        return observation
    # Exact successful CLI framing, followed by independent cryptographic check.
    assert stdout.startswith('Token: ') and stdout.endswith('\n'), repr(stdout[:32])
    token = stdout[len('Token: '):-1]
    assert '\n' not in token
    parts = token.split('.')
    assert len(parts) == 3
    header, claims = [json.loads(b64(part)) for part in parts[:2]]
    assert header == {'alg': 'HS256', 'typ': 'JWT'}, header
    issuer = claims['iss']
    assert issuer in keys, issuer
    assert hmac.compare_digest(b64(parts[2]), hmac.digest(keys[issuer].encode(), '.'.join(parts[:2]).encode(), 'sha256'))
    assert start <= claims['iat'] <= end
    assert claims['nbf'] == claims['iat']
    assert claims['exp'] - claims['iat'] == 30 * 24 * 3600
    assert claims.get('sub', '') == identity
    assert claims.get('identity', '') == identity
    expected_video = {'roomJoin': True}
    if room:
        expected_video['room'] = room
    if recorder:
        expected_video.update(hidden=True, recorder=True, canPublish=False, canPublishData=False)
    assert claims['video'] == expected_video, claims['video']
    observation.update(signature_valid=True, ttl_seconds=claims['exp']-claims['iat'],
                       issued_within_actual_invocation=True, header=header,
                       claims_without_clock={k:v for k,v in claims.items() if k not in ('iat', 'nbf', 'exp')},
                       token_sha256=hashlib.sha256(token.encode()).hexdigest())
    return observation

def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--official', type=Path, required=True)
    parser.add_argument('--rust', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    options = parser.parse_args()
    cases = [
        ('public-readable-0644', f'fixture: {SECRET}\n', 0o644, {}, False, 'fixture-room', 'fixture-user'),
        ('private-0600', f'fixture: {SECRET}\n', 0o600, {}, False, 'fixture-room', 'fixture-user'),
        ('group-readable-0640', f'fixture: {SECRET}\n', 0o640, {}, False, 'fixture-room', 'fixture-user'),
        ('recorder-0644', f'fixture: {SECRET}\n', 0o644, {}, True, 'fixture-room', 'recorder-user'),
        ('unicode-room-and-identity', f'fixture: {SECRET}\n', 0o644, {}, False, '房间', '参与者'),
        ('multiple-valid-keys', f'fixture: {SECRET}\nalternate: {SECRET}-alt\n', 0o644, {}, False, 'fixture-room', 'fixture-user'),
        ('inline-precedes-missing-file', None, 0o600, {'inline': SECRET}, False, 'fixture-room', 'fixture-user'),
        ('empty-document', '', 0o644, {}, False, 'fixture-room', 'fixture-user'),
        ('comment-only-document', '# fixture\n', 0o644, {}, False, 'fixture-room', 'fixture-user'),
        ('empty-mapping', '{}\n', 0o644, {}, False, 'fixture-room', 'fixture-user'),
        ('null-mapping', 'null\n', 0o644, {}, False, 'fixture-room', 'fixture-user'),
        ('missing-key-file', None, 0o600, {}, False, 'fixture-room', 'fixture-user'),
    ]
    rows = []
    with tempfile.TemporaryDirectory(prefix='lk-token-cli-') as directory:
        directory = Path(directory)
        for name, contents, mode, inline, recorder, room, identity in cases:
            keyfile = directory / (name + '.yaml')
            if contents is not None:
                keyfile.write_text(contents)
                keyfile.chmod(mode)
            config = {'rtc': {'node_ip': '127.0.0.1'}, 'key_file': str(keyfile), 'keys': inline}
            args = ['create-join-token', '--config-body', json.dumps(config), '--room', room, '--identity', identity]
            if recorder:
                args += ['--recorder']
            keys = inline or {'fixture': SECRET, 'alternate': SECRET+'-alt'}
            pair = {}
            for side, binary in (('official', options.official), ('rust', options.rust)):
                try:
                    pair[side] = run(binary, args, keys, room, identity, recorder)
                except Exception as error:
                    pair[side] = {'fixture_failure': type(error).__name__, 'message': str(error)}
            def comparable(observation):
                value = {k:v for k,v in observation.items() if k not in ('token_sha256', 'stderr')}
                if 'claims_without_clock' in value:
                    value['claims_without_clock'] = {k:v for k,v in value['claims_without_clock'].items() if k != 'iss'}
                return value
            equal = comparable(pair['official']) == comparable(pair['rust']) and not any('fixture_failure' in value for value in pair.values())
            rows.append({'case': name, **pair, 'equal': equal,
                         'comparison_scope': 'exit/error stdout or verified complete grant claims; invocation-clock values excluded, multiple valid issuers accepted, stderr retained diagnostic-only'})
        result = {'status': 'PASS' if all(row['equal'] for row in rows) else 'FAIL', 'cases': rows,
                  'binary_sha256': {name: hashlib.sha256(path.read_bytes()).hexdigest() for name,path in [('official', options.official), ('rust', options.rust)]},
                  'oracle': 'actual unchanged pinned Go create-join-token command; no reconstructed mock'}
        options.output.write_text(json.dumps(result, ensure_ascii=False, indent=2)+'\n')
    raise SystemExit(0 if result['status'] == 'PASS' else 1)

if __name__ == '__main__':
    main()
