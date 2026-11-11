#!/usr/bin/env python3
"""W2-G1 / W5 driver: start the official and the Rust server with the same
single-node config (distinct ports), run the Go client over the step matrix,
stop both, then compare.

run.py --official BIN --rust BIN --client twirpdiff --steps steps.json --out DIR
"""
import argparse, base64, hashlib, hmac, http.server, json, os, signal, subprocess, sys, tempfile, threading, time

KEY, SECRET = 'devkey', 'twirpdiffsecretsecretsecretsecret00'


class Receiver:
    """Webhook sink: records every POST (path, headers, body) in arrival order."""

    def __init__(self):
        self.events = []
        rec = self

        class H(http.server.BaseHTTPRequestHandler):
            def do_POST(self):
                body = self.rfile.read(int(self.headers.get('Content-Length') or 0))
                rec.events.append({'path': self.path, 'headers': {k.lower(): v for k, v in self.headers.items()},
                                   'body': body.decode('utf-8', 'replace'), 'at': time.time()})
                self.send_response(200)
                self.end_headers()

            def log_message(self, *a):
                pass

        self.srv = http.server.ThreadingHTTPServer(('127.0.0.1', 0), H)
        self.port = self.srv.server_address[1]
        threading.Thread(target=self.srv.serve_forever, daemon=True).start()


def verify_webhook(event):
    """Upstream webhook auth: JWT (HS256, our key) whose sha256 claim is the
    base64 SHA-256 of the body. Returns a verdict string, never raises."""
    auth = event['headers'].get('authorization', '')
    try:
        h, p, sig = auth.split('.')
        want = base64.urlsafe_b64encode(hmac.new(SECRET.encode(), f'{h}.{p}'.encode(), hashlib.sha256)
                                        .digest()).rstrip(b'=').decode()
        claims = json.loads(base64.urlsafe_b64decode(p + '=' * (-len(p) % 4)))
        digest = base64.b64encode(hashlib.sha256(event['body'].encode()).digest()).decode()
        return 'ok' if sig == want and claims.get('sha256') == digest and claims.get('iss') == KEY else 'bad'
    except Exception as e:  # noqa: BLE001 - verdict only
        return f'unparsable: {e}'


def start(binary, port, d, tag, hook=None):
    cfg = os.path.join(d, f'{tag}.yaml')
    with open(cfg, 'w') as f:
        f.write(f"port: {port}\nrtc:\n  tcp_port: {port + 1}\n  udp_port: {port + 2}\n  node_ip: 127.0.0.1\n"
                f"keys:\n  {KEY}: {SECRET}\nlogging:\n  level: info\n"
                + (f"webhook:\n  api_key: {KEY}\n  urls: [http://127.0.0.1:{hook}/hook]\n" if hook else ""))
    log = open(os.path.join(d, f'{tag}.log'), 'wb')
    p = subprocess.Popen([binary, '--config', cfg], stdout=log, stderr=subprocess.STDOUT,
                         stdin=subprocess.DEVNULL, start_new_session=True, cwd=d,
                         env={**{k: v for k, v in os.environ.items() if not k.startswith('LIVEKIT_')},
                              'HOME': d, 'NO_COLOR': '1'})
    for _ in range(300):
        time.sleep(0.1)
        if b'starting LiveKit server' in open(os.path.join(d, f'{tag}.log'), 'rb').read():
            time.sleep(0.5)
            return p
        if p.poll() is not None:
            break
    raise SystemExit(f'{tag} did not start: ' + open(os.path.join(d, f'{tag}.log')).read()[-2000:])


def main():
    ap = argparse.ArgumentParser()
    for k in ('official', 'rust', 'client', 'steps', 'out'):
        ap.add_argument('--' + k, required=True)
    ap.add_argument('--port-a', type=int, default=18880)
    ap.add_argument('--port-b', type=int, default=28880)
    ap.add_argument('--only', default='')
    ap.add_argument('--timeout', default='20s')
    a = ap.parse_args()
    os.makedirs(a.out, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix='twirpdiff-') as d:
        ha, hb = Receiver(), Receiver()
        pa = start(os.path.abspath(a.official), a.port_a, d, 'official', ha.port)
        pb = start(os.path.abspath(a.rust), a.port_b, d, 'rust', hb.port)
        try:
            subprocess.run([os.path.abspath(a.client), '-cases', os.path.abspath(a.steps),
                            '-a', f'http://127.0.0.1:{a.port_a}', '-b', f'http://127.0.0.1:{a.port_b}',
                            '-key', KEY, '-secret', SECRET, '-out', os.path.join(a.out, 'raw.json'),
                            '-only', a.only, '-timeout', a.timeout], check=True)
            time.sleep(4)  # webhook queue flush
        finally:
            for p in (pa, pb):
                os.killpg(p.pid, signal.SIGKILL)
                p.wait()
        for r in (ha, hb):
            r.srv.shutdown()
            for e in r.events:
                e['auth_verdict'] = verify_webhook(e)
        json.dump({'official': ha.events, 'rust': hb.events},
                  open(os.path.join(a.out, 'webhooks.json'), 'w'), indent=1)
    subprocess.run([sys.executable, '-I', os.path.join(os.path.dirname(__file__), 'compare.py'),
                    os.path.join(a.out, 'raw.json'), os.path.join(a.out, 'report.json'),
                    os.path.join(a.out, 'webhooks.json')], check=True)


if __name__ == '__main__':
    main()
