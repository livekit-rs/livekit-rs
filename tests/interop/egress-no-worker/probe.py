#!/usr/bin/env python3
"""A10 probe: Egress/Ingress/SIP/RoomService calls against a single-node server
with no worker and no Redis. Records status, body and elapsed time per method.

    python3 -I probe.py --binary /abs/livekit-server --port 18890 \
        [--timeout 90] [--only Egress.StartEgress] [--out out.json]

The goal is the *timing* dimension the twirp differential cannot express: with
no Redis the original still owns a psrpc message bus (localbus) and no worker
subscriber, so an Egress start waits out the retry schedule before answering.
"""
import argparse, base64, hashlib, hmac, json, os, signal, subprocess, sys, tempfile, time
import urllib.error, urllib.request

KEY, SECRET = 'devkey', 'twirpdiffsecretsecretsecretsecret00'
FULL = {"video": {"roomAdmin": True, "roomCreate": True, "roomList": True, "roomRecord": True,
                  "ingressAdmin": True, "room": "r1"},
        "sip": {"admin": True, "call": True}}
METHODS = [
    ("Egress", "StartEgress", {"room_name": "r1"}),
    ("Egress", "StartRoomCompositeEgress", {"room_name": "r1", "file": {"filepath": "x.mp4"}}),
    ("Egress", "StartWebEgress", {"url": "https://example.com"}),
    ("Egress", "StartParticipantEgress", {"room_name": "r1", "identity": "nobody"}),
    ("Egress", "StartTrackCompositeEgress", {"room_name": "r1"}),
    ("Egress", "StartTrackEgress", {"room_name": "r1", "track_id": "TR_x"}),
    ("Egress", "UpdateLayout", {"egress_id": "EG_x", "layout": "grid"}),
    ("Egress", "UpdateStream", {"egress_id": "EG_x"}),
    ("Egress", "ListEgress", {"room_name": "r1"}),
    ("Egress", "StopEgress", {"egress_id": "EG_x"}),
    ("Ingress", "CreateIngress",
     {"input_type": "RTMP_INPUT", "room_name": "r1", "participant_identity": "ing"}),
    ("Ingress", "UpdateIngress", {"ingress_id": "IN_x"}),
    ("Ingress", "ListIngress", {"room_name": "r1"}),
    ("Ingress", "DeleteIngress", {"ingress_id": "IN_x"}),
    ("SIP", "CreateSIPParticipant",
     {"sip_trunk_id": "ST_x", "sip_call_to": "+1555", "room_name": "r1"}),
    ("RoomService", "CreateRoom", {"name": "r-egress", "egress": {"room": {"filepath": "x.mp4"}}}),
]


def b64(raw):
    return base64.urlsafe_b64encode(raw).rstrip(b'=').decode()


def token():
    head = b64(json.dumps({"alg": "HS256", "typ": "JWT"}, separators=(',', ':')).encode())
    claims = {"exp": int(time.time()) + 3600, "iss": KEY, "sub": "a10",
              "video": FULL["video"], "sip": FULL["sip"]}
    body = b64(json.dumps(claims, separators=(',', ':')).encode())
    sig = b64(hmac.new(SECRET.encode(), f'{head}.{body}'.encode(), hashlib.sha256).digest())
    return f'{head}.{body}.{sig}'


def start(binary, port, d, tag):
    cfg = os.path.join(d, f'{tag}.yaml')
    with open(cfg, 'w') as f:
        f.write(f"port: {port}\nrtc:\n  tcp_port: {port + 1}\n  udp_port: {port + 2}\n"
                f"  node_ip: 127.0.0.1\nkeys:\n  {KEY}: {SECRET}\nlogging:\n  level: info\n")
    log_path = os.path.join(d, f'{tag}.log')
    with open(log_path, 'wb') as log:
        p = subprocess.Popen([binary, '--config', cfg], stdout=log, stderr=subprocess.STDOUT,
                             stdin=subprocess.DEVNULL, start_new_session=True, cwd=d,
                             env={**{k: v for k, v in os.environ.items() if not k.startswith('LIVEKIT_')},
                                  'HOME': d, 'NO_COLOR': '1'})
    try:
        for _ in range(300):
            time.sleep(0.1)
            with open(log_path, 'rb') as log:
                content = log.read()
            if b'starting LiveKit server' in content:
                time.sleep(0.5)
                return p
            if p.poll() is not None:
                break
        raise RuntimeError(f'{tag} 未启动：' + content.decode(errors='replace')[-2000:])
    except BaseException:
        stop(p)
        raise


def stop(p):
    if p.poll() is None:
        os.killpg(p.pid, signal.SIGTERM)
        try:
            p.wait(timeout=5)
        except subprocess.TimeoutExpired:
            os.killpg(p.pid, signal.SIGKILL)
    p.wait()


def call(port, svc, method, body, timeout):
    url = f'http://127.0.0.1:{port}/twirp/livekit.{svc}/{method}'
    req = urllib.request.Request(url, data=json.dumps(body).encode(), method='POST',
                                 headers={'Content-Type': 'application/json',
                                          'Authorization': 'Bearer ' + token()})
    at = time.monotonic()
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return {'status': r.status, 'body': r.read().decode(),
                    'elapsed': round(time.monotonic() - at, 2)}
    except urllib.error.HTTPError as e:
        return {'status': e.code, 'body': e.read().decode(), 'elapsed': round(time.monotonic() - at, 2)}
    except Exception as e:  # noqa: BLE001 - transport verdict is the datum
        return {'error': str(e), 'elapsed': round(time.monotonic() - at, 2)}


def validate(rows):
    for row in rows:
        method = row['id']
        assert 'error' not in row, row
        body = json.loads(row['body'])  # 不能截掉 PANIC 尾缀来伪造 JSON 一致。
        if method.startswith('Egress.Start'):
            assert row['status'] == 503, row
            assert body == {'code': 'unavailable', 'msg': 'no response from servers'}, row
            assert 17 <= row['elapsed'] <= 40, row
        elif method == 'RoomService.CreateRoom':
            assert row['status'] == 200 and body['name'] == 'r-egress', row
            assert 9 <= row['elapsed'] <= 15, row
            assert body['active_recording'] is False, row
        else:
            assert row['status'] == 500, row
            service = method.split('.')[0].lower()
            message = ('Internal service panic' if method in
                       ('Ingress.UpdateIngress', 'Ingress.DeleteIngress') else
                       f'{service} not connected (redis required)')
            assert body == {'code': 'internal', 'msg': message}, row
            if method in ('Egress.UpdateStream', 'Egress.StopEgress'):
                assert 2.5 <= row['elapsed'] <= 6, row
            else:
                assert row['elapsed'] < 5, row


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--binary', required=True)
    ap.add_argument('--port', type=int, default=18890)
    ap.add_argument('--timeout', type=float, default=90)
    ap.add_argument('--only', default='')
    ap.add_argument('--out', default='')
    ap.add_argument('--check', action='store_true', help='校验默认无 worker 配置的状态、完整 JSON 和时序')
    a = ap.parse_args()
    binary = os.path.abspath(a.binary)
    with tempfile.TemporaryDirectory(prefix='a10probe-') as d:
        p = start(binary, a.port, d, 'srv')
        try:
            # The original resolves a start request's room through its store
            # before it ever publishes to the bus, so the room has to exist.
            setup = call(a.port, 'RoomService', 'CreateRoom',
                         {'name': 'r1', 'empty_timeout': 600}, a.timeout)
            print(json.dumps(dict(setup, id='RoomService.CreateRoom(setup)')), flush=True)
            assert setup.get('status') == 200, setup
            out = []
            for svc, method, body in METHODS:
                if a.only and f'{svc}.{method}' not in a.only.split(','):
                    continue
                r = call(a.port, svc, method, body, a.timeout)
                r['id'] = f'{svc}.{method}'
                out.append(r)
                print(json.dumps(r), flush=True)
            health = call(a.port, 'RoomService', 'ListRooms', {'names': ['r1']}, a.timeout)
            assert health.get('status') == 200, health
            assert any(room['name'] == 'r1' for room in json.loads(health['body'])['rooms']), health
        finally:
            stop(p)
    if a.out:
        with open(a.out, 'w') as f:
            json.dump(out, f, indent=1)
    if a.check:
        validate(out)
        print(json.dumps({'checked': len(out), 'health_after_failures': 'pass'}), flush=True)


if __name__ == '__main__':
    main()
