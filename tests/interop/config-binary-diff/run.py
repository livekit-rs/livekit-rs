#!/usr/bin/env python3
"""W1-G3 real-binary differential (SPEC-0073).

Runs every config-observer case (argv / ENV / files) through the unmodified
official `livekit-server` and the Rust `livekit-server`, one process at a time,
and compares the externally observable outcome:

- exited before readiness: exit status and stdout exactly; stderr log *messages*
  (timestamps/levels/callers stripped) are reported but zap byte format is a
  separate logging item (see README);
- started: the `starting LiveKit server` line was logged (upstream logs it after
  every listener is bound). The startup values (HTTP port, node IP, RTC TCP/UDP
  ports or ICE range, bind addresses, prometheus/debug/webtransport ports,
  region) are compared; then the process must survive a short grace period.

Usage: run.py --official BIN --rust BIN --cases cases.json --out results.json
       [--only PREFIX ...] [--ready-timeout S] [--grace S]
"""
import argparse, json, os, re, select, signal, socket, subprocess, sys, tempfile, time

ANSI = re.compile(r'\x1b\[[0-9;]*m')
MARK = 'starting LiveKit server'
OBSERVED_PREFIXES = ('LIVEKIT_', 'REDIS_')
OBSERVED_NAMES = {'NODE_IP', 'UDP_PORT'}


def port_free(port):
    for fam, addr in ((socket.AF_INET, '0.0.0.0'),):
        s = socket.socket(fam, socket.SOCK_STREAM)
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        try:
            s.bind((addr, port))
        except OSError:
            return False
        finally:
            s.close()
    return True


def norm_text(b):
    return b.decode('utf-8', 'replace')


def subst(v, d):
    return v.replace('{dir}', d) if isinstance(v, str) else v


def base_env(home):
    env = {k: v for k, v in os.environ.items()
           if not k.startswith(OBSERVED_PREFIXES) and k not in OBSERVED_NAMES}
    env['HOME'] = home
    env.pop('GOTRACEBACK', None)
    env['NO_COLOR'] = '1'
    return env


def run_one(binary, case, ready_timeout, grace):
    with tempfile.TemporaryDirectory(prefix='w1g3-') as d:
        for name, body in (case.get('files') or {}).items():
            p = os.path.join(d, name)
            with open(p, 'w') as f:
                f.write(subst(body, d))
            mode = (case.get('file_modes') or {}).get(name)
            os.chmod(p, int(mode, 8) if mode else 0o600)
        env = base_env(d)
        for k, v in (case.get('env') or {}).items():
            env[k] = subst(v, d)
        argv = [binary] + [subst(a, d) for a in case.get('argv') or []]
        t0 = time.time()
        proc = subprocess.Popen(argv, stdin=subprocess.DEVNULL, stdout=subprocess.PIPE,
                                stderr=subprocess.PIPE, env=env, cwd=d, start_new_session=True)
        out, err = b'', b''
        started = False
        deadline = t0 + ready_timeout
        fds = {proc.stdout.fileno(): 'out', proc.stderr.fileno(): 'err'}
        for fd in fds:
            os.set_blocking(fd, False)
        def pump(timeout):
            nonlocal out, err
            r, _, _ = select.select(list(fds), [], [], timeout)
            for fd in r:
                try:
                    chunk = os.read(fd, 65536)
                except BlockingIOError:
                    continue
                if not chunk:
                    fds.pop(fd, None)
                elif fds[fd] == 'out':
                    out += chunk
                else:
                    err += chunk
        while time.time() < deadline:
            pump(0.1)
            if MARK.encode() in out + err:
                started = True
                break
            if proc.poll() is not None and not fds:
                break
            if proc.poll() is not None:
                pump(0.05)
                if not fds or proc.poll() is not None and time.time() > deadline:
                    break
        survived = None
        http_reachable = None
        if started:
            pump(0.2)
            text = norm_text(out + err)
            port = (go_startup(text) or {}).get('portHttp') or (rust_startup(text) or {}).get('portHttp')
            st = {'portHttp': int(port)} if port else {}
            if 'portHttp' in st:
                try:
                    socket.create_connection(('127.0.0.1', st['portHttp']), timeout=2).close()
                    http_reachable = True
                except OSError:
                    http_reachable = False
            end = time.time() + grace
            while time.time() < end and proc.poll() is None:
                pump(0.1)
            survived = proc.poll() is None
        timed_out = not started and proc.poll() is None
        if proc.poll() is None:
            os.killpg(proc.pid, signal.SIGKILL)
        proc.wait()
        while fds:
            pump(0.2)
        real = os.path.realpath(d)
        def norm(b):
            t = b.decode('utf-8', 'replace')
            for v in (real, d):
                t = t.replace(v, '{dir}')
            # SPEC-0074 C31: the Rust binary prints the upstream version, so no
            # product-identity substitution remains permitted.
            return t
        return {
            'started': started, 'survived_grace': survived, 'http_reachable': http_reachable, 'timed_out': timed_out,
            'exit': None if started or timed_out else proc.returncode,
            'stdout': norm(out), 'stderr': norm(err),
            'seconds': round(time.time() - t0, 2),
        }


GO_JSON_DROP = {'level', 'ts', 'logger', 'caller', 'msg', 'nodeID', 'version'}


def go_startup(text):
    for line in text.splitlines():
        if MARK in line and line.lstrip().startswith('{'):
            try:
                v = json.loads(line)
            except ValueError:
                return {'unparsed': line}
            return {k: x for k, x in v.items() if k not in GO_JSON_DROP}
        if MARK in line:
            i = line.find('{', line.find(MARK))
            v = json.loads(line[i:]) if i >= 0 else {}
            v.pop('nodeID', None)
            v.pop('version', None)
            return v
    return None


# C32: the Rust binary now emits the same zap console/JSON encoder as Go. The
# startup object is flat; metadata keys (level/ts/logger/caller/msg plus node
# identity) are stripped so only externally meaningful startup values compare.
STARTUP_META = GO_JSON_DROP | {'time'}


def rust_startup(text):
    # SPEC-0074 C32: `starting LiveKit server` is byte-identical zap console in
    # both binaries, so parse it exactly like Go's and drop encoder metadata.
    v = go_startup(ANSI.sub('', text))
    if isinstance(v, dict) and 'unparsed' not in v:
        v = {k: x for k, x in v.items() if k not in STARTUP_META}
    return v


LOG_PREFIX_GO = re.compile(r'^\d{4}-\d\d-\d\dT[\d:.]+[+-]\d{4}\t(\w+)\t\S+\t\S+\t(.*)$')
# C32: the Rust binary emits the same zap console encoder, so Go's prefix applies.
LOG_PREFIX_RUST = LOG_PREFIX_GO


def messages(text, go):
    # C32: both binaries now use the zap console encoder; parse identically and
    # strip the trailing JSON context object from the message column.
    msgs = []
    for line in ANSI.sub('', text).splitlines():
        m = LOG_PREFIX_GO.match(line)
        if m:
            msg = m.group(2).split('\t{', 1)[0]
            msgs.append(f'{m.group(1).upper()} {msg.strip()}')
        elif line.strip():
            msgs.append('RAW ' + line)
    return msgs


def compare(g, r):
    if g['timed_out'] or r['timed_out']:
        return 'timeout' if g['timed_out'] == r['timed_out'] else 'outcome'
    if g['started'] != r['started']:
        return 'outcome'
    if g['started']:
        if g['survived_grace'] != r['survived_grace']:
            return 'grace'
        if g['http_reachable'] != r['http_reachable']:
            return 'http_reachable'
        gs, rs = go_startup(g['stdout'] + g['stderr']), rust_startup(r['stdout'] + r['stderr'])
        return 'match' if gs == rs else 'startup_values'
    if g['exit'] != r['exit']:
        return 'exit_code'
    if g['stdout'] != r['stdout']:
        return 'stdout'
    return 'match'


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--official', required=True)
    ap.add_argument('--rust', required=True)
    ap.add_argument('--cases', required=True)
    ap.add_argument('--out', required=True)
    ap.add_argument('--only', nargs='*')
    ap.add_argument('--ready-timeout', type=float, default=25)
    ap.add_argument('--grace', type=float, default=1.5)
    ap.add_argument('--resume', action='store_true')
    a = ap.parse_args()
    a.official, a.rust = os.path.abspath(a.official), os.path.abspath(a.rust)
    cases = json.load(open(a.cases))['cases']
    if a.only:
        cases = [c for c in cases if c['id'].startswith(tuple(a.only))]
    rows, counts = [], {}
    done = {}
    if a.resume and os.path.exists(a.out):
        for row in json.load(open(a.out))['rows']:
            done[row['id']] = row
    for i, c in enumerate(cases):
        if c['id'] in done:
            row = done[c['id']]
            rows.append(row)
            counts[row['status']] = counts.get(row['status'], 0) + 1
            continue
        for port in (7880, 7881):
            for _ in range(50):
                if port_free(port):
                    break
                time.sleep(0.2)
        g = run_one(a.official, c, a.ready_timeout, a.grace)
        r = run_one(a.rust, c, a.ready_timeout, a.grace)
        status = compare(g, r)
        counts[status] = counts.get(status, 0) + 1
        row = {'id': c['id'], 'status': status, 'go': g, 'rust': r,
               'go_startup': go_startup(g['stdout'] + g['stderr']) if g['started'] else None,
               'rust_startup': rust_startup(r['stdout'] + r['stderr']) if r['started'] else None,
               'go_messages': messages(g['stderr'], True), 'rust_messages': messages(r['stderr'], False)}
        rows.append(row)
        print(f'[{i+1}/{len(cases)}] {status:14} {c["id"]}', flush=True)
        if (i + 1) % 25 == 0:
            json.dump({'counts': counts, 'rows': rows}, open(a.out, 'w'), indent=1)
    json.dump({'official': a.official, 'rust': a.rust, 'counts': counts, 'rows': rows},
              open(a.out, 'w'), indent=1)
    print(json.dumps(counts))


if __name__ == '__main__':
    main()
