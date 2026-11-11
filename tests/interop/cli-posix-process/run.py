#!/usr/bin/env python3
"""Root-run real byte argv/environment differential. No service is required."""
import argparse, base64, errno, hashlib, hmac, json, os, platform, signal, subprocess, tempfile, time

MAX_OUTPUT = 1024 * 1024
KEY = b'APIf52owned'
SECRET = b'f52-owned-not-production-signing-secret'

def encoded(value):
    return {'hex': value.hex(), 'base64': base64.b64encode(value).decode('ascii'), 'bytes': len(value),
            'sha256': hashlib.sha256(value).hexdigest()}

def execute(binary, args, env, cwd, timeout):
    # argv[0] is the same actual basename, not a later output text substitution.
    started = time.time()
    p = subprocess.Popen([b'livekit-server'] + args, executable=os.fsencode(binary), cwd=cwd,
                         env=env, stdout=subprocess.PIPE, stderr=subprocess.PIPE, start_new_session=True)
    timed_out = False
    try:
        stdout, stderr = p.communicate(timeout=timeout)
    except subprocess.TimeoutExpired:
        timed_out = True
        os.killpg(p.pid, signal.SIGTERM)
        try:
            stdout, stderr = p.communicate(timeout=1)
        except subprocess.TimeoutExpired:
            os.killpg(p.pid, signal.SIGKILL)
            stdout, stderr = p.communicate()
    overflow = len(stdout) > MAX_OUTPUT or len(stderr) > MAX_OUTPUT
    # Full bounded result or explicit overflow failure; never silently truncate.
    result = {'exit': p.returncode, 'timed_out': timed_out, 'overflow': overflow,
              'elapsed_ms': round((time.time() - started) * 1000, 3)}
    if overflow:
        result['stdout'] = {'bytes': len(stdout), 'sha256': hashlib.sha256(stdout).hexdigest()}
        result['stderr'] = {'bytes': len(stderr), 'sha256': hashlib.sha256(stderr).hexdigest()}
    else:
        result['stdout'] = encoded(stdout)
        result['stderr'] = encoded(stderr)
    return result, stdout, stderr

def jwt_semantics(stdout):
    # Generated time claims are checked, not compared as fabricated static bytes.
    prefix = b'Token: '
    if not stdout.startswith(prefix) or not stdout.endswith(b'\n'):
        raise ValueError('actual Token output framing absent')
    token = stdout[len(prefix):].strip()
    parts = token.split(b'.')
    if len(parts) != 3:
        raise ValueError('actual token is not a JWS')
    def decode(part): return base64.urlsafe_b64decode(part + b'=' * (-len(part) % 4))
    header = json.loads(decode(parts[0]))
    claims = json.loads(decode(parts[1]))
    if header.get('alg') != 'HS256' or not hmac.compare_digest(decode(parts[2]), hmac.new(SECRET, b'.'.join(parts[:2]), hashlib.sha256).digest()):
        raise ValueError('actual owned key-file signature invalid')
    if claims.get('iss') != KEY.decode() or claims.get('sub') != 'f52-identity' or claims.get('video') != {'roomJoin': True, 'room': 'f52-room'}:
        raise ValueError('actual keyfile issuer/identity/room grants differ')
    now = int(time.time())
    # Official auth.NewAccessToken uses nbf/exp; absence of iat is not invented.
    nbf, exp = claims.get('nbf'), claims.get('exp')
    if not isinstance(nbf, int) or not isinstance(exp, int) or not now - 30 <= nbf <= now + 2 or exp - nbf != 30 * 24 * 3600:
        raise ValueError('actual source30-day lifetime not preserved')
    # Preserve every non-generated claim and validate optional generated iat.
    if 'iat' in claims and (not isinstance(claims['iat'], int) or abs(claims['iat'] - nbf) > 1):
        raise ValueError('generated iat inconsistent')
    return {'signature_valid': True, 'source_lifetime_seconds': exp - nbf,
            'claims_without_generated_times': {k:v for k,v in claims.items() if k not in ('iat','nbf','exp')},
            'generated_time_claims': {k:v for k,v in claims.items() if k in ('iat','nbf','exp')}}

def original_24_cases(config, keys, missing, missingkey, ascii_config):
    return [
      ('positive-ascii-ports', [b'--config', ascii_config, b'--port=43117', b'ports'], {}, 'exact'),
      ('positive-byte-config-ports', [b'--config', config, b'ports'], {}, 'exact'),
      ('positive-byte-keyfile-token', [b'--key-file', keys, b'create-join-token', b'--room=f52-room', b'--identity=f52-identity'], {}, 'jwt'),
      ('byte-config-missing', [b'--config', missing, b'ports'], {}, 'exact'),
      ('byte-keyfile-missing', [b'--key-file', missingkey, b'create-join-token', b'--room=f52-room', b'--identity=f52-identity'], {}, 'exact'),
      ('byte-bool-inline-before-help', [b'--dev=\xff', b'--help'], {}, 'exact'),
      ('byte-bool-separated-is-positional', [b'--dev', b'\xff', b'--help'], {}, 'exact'),
      ('byte-uint-inline-before-help', [b'--port=\xfe', b'--help'], {}, 'exact'),
      ('byte-uint-separated-before-help', [b'--port', b'\xff', b'--help'], {}, 'exact'),
      ('byte-uint-with-quote', [b'--port=\xff"', b'--help'], {}, 'exact'),
      ('byte-flagname-before-help', [b'--\xff', b'--help'], {}, 'exact'),
      ('byte-value-consumed-before-help', [b'--config', b'\xff', b'--help'], {}, 'exact'),
      ('byte-positional-before-command', [b'--config', missing, b'\xff', b'ports', b'--help'], {}, 'exact'),
      ('byte-env-uint-before-child-help', [b'ports', b'--help'], {b'LIVEKIT_PORT':b'\xff'}, 'exact'),
      ('byte-env-uint-before-ports', [b'ports'], {b'LIVEKIT_PORT':b'\xff'}, 'exact'),
      ('byte-env-bool-before-child-help', [b'ports', b'--help'], {b'LIVEKIT_RTC_USE_EXTERNAL_IP':b'\xfe'}, 'exact'),
      ('byte-env-config-body', [b'ports'], {b'LIVEKIT_CONFIG':b'port: \xff\n'}, 'exact'),
      ('byte-env-keys-body', [b'create-join-token', b'--room=f52-room', b'--identity=f52-identity'], {b'LIVEKIT_KEYS':b'x: \xff'}, 'exact'),
      ('root-help-preempts-byte-env', [b'--help'], {b'LIVEKIT_PORT':b'\xff'}, 'exact'),
      ('root-cli-overrides-byte-env', [b'--port=43117', b'ports'], {b'LIVEKIT_PORT':b'\xff'}, 'exact'),
      ('child-cli-cannot-mask-root-byte-env', [b'ports', b'--port=43117', b'--help'], {b'LIVEKIT_PORT':b'\xff'}, 'exact'),
      ('unknown-byte-env-name', [b'--port=43117', b'ports'], {b'LIVEKIT_PORT\xff':b'bad'}, 'exact'),
      ('invalid-trailing-byte-sequence', [b'--dev=\xc3', b'--help'], {}, 'exact'),
      ('valid-utf8-replacement-is-not-invalid-byte', [b'--dev=\xef\xbf\xbd', b'--help'], {}, 'exact'),
    ]

def cases(config, keys, missing, missingkey, ascii_config, byte_home):
    rows=original_24_cases(config,keys,missing,missingkey,ascii_config)
    rows.extend([
      ('builtin-help-byte-topic',[b'help',b'\xff'],{},'exact'),
      ('builtin-alias-byte-topic',[b'h',b'\xff'],{},'exact'),
      ('nested-builtin-help-byte-topic',[b'ports',b'help',b'\xff'],{},'exact'),
      ('flag-help-byte-topic',[b'--help',b'\xff'],{},'exact'),
      ('ascii-tilde-byte-home-keyfile',[b'--key-file',b'~/keys',b'create-join-token',b'--room=f52-room',b'--identity=f52-identity'],{b'HOME':byte_home},'jwt'),
      ('ascii-variable-byte-dir-keyfile',[b'--key-file',b'$RAW_DIR/keys',b'create-join-token',b'--room=f52-room',b'--identity=f52-identity'],{b'RAW_DIR':byte_home},'jwt'),
      ('ascii-tilde-byte-home-missing',[b'--key-file',b'~/missing',b'create-join-token',b'--room=f52-room',b'--identity=f52-identity'],{b'HOME':byte_home},'exact'),
      ('ascii-variable-byte-dir-missing',[b'--key-file',b'$RAW_DIR/missing',b'create-join-token',b'--room=f52-room',b'--identity=f52-identity'],{b'RAW_DIR':byte_home},'exact'),
    ])
    return rows

def canonical_identity(raw):
    # SPEC-0074 C31: exact version parity, so the F47 VERSION stanza
    # substitution is removed; raw bytes are compared unchanged.
    return raw

def create_owned_file(path, body):
    try:
        fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        with os.fdopen(fd, 'wb') as file:
            file.write(body)
        return {'status':'SUPPORTED', 'operation':'os.open(O_CREAT|O_EXCL,0600)', 'path':encoded(path)}
    except OSError as error:
        return {'status':'UNSUPPORTED' if error.errno == errno.EILSEQ else 'SETUP_ERROR',
                'operation':'os.open(O_CREAT|O_EXCL,0600)', 'path':encoded(path),
                'errno':error.errno, 'errno_name':errno.errorcode.get(error.errno, 'UNKNOWN')}

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--official', required=True)
    parser.add_argument('--rust', required=True)
    parser.add_argument('--output', required=True)
    parser.add_argument('--timeout', type=float, default=5)
    parser.add_argument('--profile', choices=['auto','darwin','linux'], default='auto')
    opt = parser.parse_args()
    if os.name != 'posix': raise SystemExit('POSIX bytes ABI required; Windows scope NOT_RUN')
    if not 0 < opt.timeout <= 30: raise SystemExit('timeout must be positive and bounded<=30s')
    actual_platform=platform.system()
    detected={'Darwin':'darwin','Linux':'linux'}.get(actual_platform)
    profile=detected if opt.profile=='auto' else opt.profile
    if profile is None or profile != detected: raise SystemExit('profile must match actual Darwin/Linux host; other platforms NOT_RUN')
    binaries = {'official': os.path.abspath(opt.official), 'rust': os.path.abspath(opt.rust)}
    evidence = {'status':'RUNNING', 'scope':'F56 original24 cases retained plus8 actual raw-help/ASCII-path byte-ENV regressions; no server/browser/profile/runtime acceptance',
                'platform':platform.platform(), 'profile':profile, 'positive_byte_files_mandatory':profile=='linux', 'normalization':'Byte-preserving F47 exact product VERSION stanza only; same argv[0]. JWT generated times validated separately; raw bytes always retained and all other output exact.',
                'binary_sha256':{k:hashlib.sha256(open(v,'rb').read()).hexdigest() for k,v in binaries.items()}, 'cases':[]}
    with tempfile.TemporaryDirectory(prefix='livekit-rs-f52-owned-') as name:
        cwd=os.fsencode(name)
        config=cwd+b'/config-\xff.yaml'; keys=cwd+b'/keys-\xfe.yaml'
        missing=cwd+b'/absent-config-\xff.yaml'; missingkey=cwd+b'/absent-key-\xfe.yaml'
        ascii_config=cwd+b'/config-ascii.yaml'
        config_body=b'port: 43117\nrtc:\n  tcp_port: 43118\n  udp_port: 43119\n'
        probes={
            'ascii_config':create_owned_file(ascii_config,config_body),
            'positive-byte-config-ports':create_owned_file(config,config_body),
            'positive-byte-keyfile-token':create_owned_file(keys,KEY+b': '+SECRET+b'\n'),
        }
        evidence['filesystem_capabilities']=probes
        # This probes the actual filesystem, not the executable's Unicode policy.
        # A failed file creation cannot be a Go/Rust process comparison result.
        setup_error=any(p['status']=='SETUP_ERROR' for p in probes.values()) or probes['ascii_config']['status']!='SUPPORTED'
        byte_home=cwd+b'/home-\xff'
        try:
            os.mkdir(byte_home,0o700)
            home_probe=create_owned_file(byte_home+b'/keys',KEY+b': '+SECRET+b'\n')
        except OSError as error:
            home_probe={'status':'UNSUPPORTED' if error.errno==errno.EILSEQ else 'SETUP_ERROR',
                        'operation':'os.mkdir(byte HOME,0700)', 'path':encoded(byte_home),
                        'errno':error.errno,'errno_name':errno.errorcode.get(error.errno,'UNKNOWN')}
        for label in ['ascii-tilde-byte-home-keyfile','ascii-variable-byte-dir-keyfile']:
            probes[label]=home_probe
        # Negative paths don't require directory creation and must still execute.
        setup_error=setup_error or home_probe['status']=='SETUP_ERROR'
        env={b'PATH':os.fsencode(os.defpath),b'HOME':cwd,b'TZ':b'UTC',b'LC_ALL':b'C'}
        for label,args,extra,mode in cases(config,keys,missing,missingkey,ascii_config,byte_home):
            row={'name':label,'argv':[encoded(a) for a in args], 'env':[{ 'key':encoded(k),'value':encoded(v)} for k,v in extra.items()], 'mode':mode, 'raw':{}}
            probe=probes.get(label)
            if setup_error:
                row.update(status='NOT_RUN_SETUP', reason='owned filesystem setup failed', comparison_performed=False)
                evidence['cases'].append(row)
                continue
            if probe is not None and probe['status']=='UNSUPPORTED':
                row.update(status='NOT_RUN_PLATFORM', reason='actual filesystem rejected creation of this positive byte filename',
                           capability=probe, comparison_performed=False,
                           raw={k:{'status':'NOT_RUN_PLATFORM'} for k in binaries})
                evidence['cases'].append(row)
                continue
            outputs={}
            for target,binary in binaries.items():
                result,stdout,stderr=execute(binary,args,env|extra,cwd,opt.timeout)
                row['raw'][target]=result;outputs[target]=(result,stdout,stderr)
            a,b=outputs['official'],outputs['rust']
            if mode=='jwt':
                semantic={}
                for target,(result,stdout,stderr) in outputs.items():
                    try:
                        if result['exit']!=0 or stderr or result['timed_out'] or result['overflow']:raise ValueError('keyfile command did not cleanly complete')
                        semantic[target]=jwt_semantics(stdout)
                    except Exception as error:semantic[target]={'validation_error':str(error)}
                row['semantic']=semantic
                row['equal']=all('validation_error' not in v for v in semantic.values()) and semantic['official'].get('claims_without_generated_times')==semantic['rust'].get('claims_without_generated_times')
            else:
                row['raw_equal']=a[0]['exit']==b[0]['exit'] and a[1:]==b[1:]
                row['equal']=a[0]['exit']==b[0]['exit'] and tuple(map(canonical_identity,a[1:]))==tuple(map(canonical_identity,b[1:])) and not any(x[0]['timed_out'] or x[0]['overflow'] for x in [a,b])
            row['status']='PASS' if row['equal'] else 'FAIL';row['comparison_performed']=True;evidence['cases'].append(row)
    evidence['owned_files_cleaned']=True
    evidence['configured_cases']=len(evidence['cases'])
    evidence['executed_cases']=sum(c['comparison_performed'] for c in evidence['cases'])
    evidence['not_run_platform']=sum(c['status']=='NOT_RUN_PLATFORM' for c in evidence['cases'])
    evidence['not_run_setup']=sum(c['status']=='NOT_RUN_SETUP' for c in evidence['cases'])
    evidence['passed']=sum(c['status']=='PASS' for c in evidence['cases'])
    evidence['failed']=sum(c['status']=='FAIL' for c in evidence['cases'])
    evidence['completed_scope_status']='FAIL' if evidence['failed'] else ('PASS' if evidence['executed_cases'] else 'NOT_RUN')
    if evidence['not_run_setup']:evidence['status']='FAIL_SETUP'
    elif evidence['failed']:evidence['status']='FAIL'
    elif evidence['not_run_platform']:evidence['status']='FAIL_PLATFORM_REQUIRED' if profile=='linux' else 'INCOMPLETE_PLATFORM'
    else:evidence['status']='PASS'
    with open(opt.output,'w',encoding='utf-8') as file:json.dump(evidence,file,indent=2);file.write('\n')
    # Incomplete platform coverage is deliberately not an overall PASS.
    raise SystemExit(0 if evidence['status']=='PASS' else (1 if evidence['status']=='FAIL' else 2))
if __name__=='__main__':main()
