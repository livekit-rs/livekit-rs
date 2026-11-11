#!/usr/bin/env python3
"""A3 调查：定位原版 Ingress.UpdateIngress 偶发 connection reset 的连接层根因。

只用官方 Go 二进制 + Python 标准库，不构建 Rust。通过精确控制 HTTP 连接复用，
区分「panic 响应本身」与「keep-alive 池复用到已被服务端关闭的连接」两种来源。

用法：
  python3 reset_probe.py --binary /private/tmp/livekit-server-go --out 目录
"""

import argparse
import base64
import http.client
import hmac
import hashlib
import json
import os
from pathlib import Path
import signal
import socket
import shutil
import subprocess
import tempfile
import time

KEY, SECRET = 'devkey', 'secretsecretsecretsecretsecret12'  # >=32B
PANIC_BODY = {'code': 'internal', 'msg': 'Internal service panic'}


def _b64url(raw: bytes) -> str:
    return base64.urlsafe_b64encode(raw).rstrip(b'=').decode()


def token():
    """手工签 HS256 JWT，避免依赖 PyJWT（sha256 在本机 hashlib 可用）。"""
    now = int(time.time())
    header = {'alg': 'HS256', 'typ': 'JWT'}
    payload = {'iss': KEY, 'sub': 'reset-probe', 'nbf': now - 5, 'exp': now + 600,
               'video': {'roomAdmin': True, 'roomCreate': True, 'roomList': True,
                         'ingressAdmin': True, 'room': 'r1'}}
    signing_input = (_b64url(json.dumps(header, separators=(',', ':')).encode())
                     + '.'
                     + _b64url(json.dumps(payload, separators=(',', ':')).encode()))
    sig = hmac.new(SECRET.encode(), signing_input.encode(), hashlib.sha256).digest()
    return signing_input + '.' + _b64url(sig)


def free_port(kind):
    s = socket.socket(socket.AF_INET, kind)
    s.bind(('127.0.0.1', 0))
    p = s.getsockname()[1]
    s.close()
    return p


def request(conn, method, body, tok, reuse):
    """在给定连接上发一次 Twirp 请求；返回结果与连接是否被服务端关闭。

    method 形如 'Ingress.UpdateIngress'，Twirp 路径为
    /twirp/livekit.<Service>/<Method>（Service 与 Method 之间是斜杠）。
    """
    payload = json.dumps(body).encode()
    headers = {
        'Content-Type': 'application/json',
        'Authorization': 'Bearer ' + tok,
        'Content-Length': str(len(payload)),
    }
    if not reuse:
        headers['Connection'] = 'close'
    service, _, m = method.partition('.')
    path = '/twirp/livekit.' + service + '/' + m
    out = {'method': method, 'reuse': reuse}
    try:
        conn.request('POST', path, body=payload, headers=headers)
        resp = conn.getresponse()
        raw = resp.read()
        out['status'] = resp.status
        out['headers'] = {k.lower(): v for k, v in resp.getheaders()}
        out['body'] = raw.decode('utf-8', 'replace')
        out['will_close'] = resp.will_close
    except (http.client.HTTPException, OSError) as e:
        out['transport_error'] = f'{type(e).__name__}: {e}'
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--binary', required=True)
    ap.add_argument('--out', required=True)
    ap.add_argument('--rounds', type=int, default=30)
    args = ap.parse_args()
    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)

    http_port, tcp, udp = free_port(socket.SOCK_STREAM), free_port(socket.SOCK_STREAM), free_port(socket.SOCK_DGRAM)
    tmp = Path(tempfile.mkdtemp(prefix='a3-reset-', dir='/private/tmp'))
    cfg = tmp / 'config.yaml'
    cfg.write_text(
        f'port: {http_port}\nbind_addresses: ["127.0.0.1"]\n'
        f'rtc:\n  tcp_port: {tcp}\n  udp_port: {udp}\n  use_external_ip: false\n'
        f'  node_ip: 127.0.0.1\nkeys:\n  {KEY}: {SECRET}\n'
        f'logging:\n  level: error\n  json: true\n')
    log = (tmp / 'server.log').open('wb')
    proc = subprocess.Popen(
        [args.binary, '--config', str(cfg)],
        stdout=log, stderr=subprocess.STDOUT, start_new_session=True)
    rows = []
    try:
        deadline = time.monotonic() + 15
        while True:
            try:
                c = http.client.HTTPConnection('127.0.0.1', http_port, timeout=2)
                c.request('GET', '/')
                c.getresponse().read()
                c.close()
                break
            except OSError:
                if time.monotonic() > deadline:
                    raise SystemExit('server did not start')
                time.sleep(0.05)
        tok = token()

        # 实验1：每次新建连接（Connection: close），panic 响应是否稳定为裸 JSON。
        fresh = []
        for i in range(args.rounds):
            c = http.client.HTTPConnection('127.0.0.1', http_port, timeout=10)
            fresh.append(request(c, 'Ingress.UpdateIngress', {}, tok, reuse=False))
            c.close()
        rows.append({'experiment': 'fresh_connection_close', 'results': fresh})

        # 实验2：单条持久连接，normal -> panic -> normal，观察 panic 后连接是否被关闭。
        persistent = []
        c = http.client.HTTPConnection('127.0.0.1', http_port, timeout=10)
        seq = ['RoomService.ListRooms', 'Ingress.UpdateIngress',
               'RoomService.ListRooms', 'Ingress.DeleteIngress',
               'RoomService.ListRooms', 'Ingress.UpdateIngress',
               'RoomService.ListRooms']
        for method in seq:
            r = request(c, method, {}, tok, reuse=True)
            persistent.append(r)
            if r.get('will_close') or 'transport_error' in r:
                # 连接已被关闭：重连继续，模拟连接池拿到死连接后的重试。
                try:
                    c.close()
                except OSError:
                    pass
                c = http.client.HTTPConnection('127.0.0.1', http_port, timeout=10)
        c.close()
        rows.append({'experiment': 'persistent_connection_sequence', 'results': persistent})

        # 实验3：复用「刚处理过 panic 且未声明 close」的连接立即再发，统计 reset 频率。
        reuse_after_panic = []
        for i in range(args.rounds):
            c = http.client.HTTPConnection('127.0.0.1', http_port, timeout=10)
            first = request(c, 'Ingress.UpdateIngress', {}, tok, reuse=True)
            second = None
            if 'transport_error' not in first and not first.get('will_close'):
                second = request(c, 'RoomService.ListRooms', {}, tok, reuse=True)
            reuse_after_panic.append({'first': first, 'second': second})
            try:
                c.close()
            except OSError:
                pass
        rows.append({'experiment': 'reuse_after_panic', 'results': reuse_after_panic})
    finally:
        try:
            os.killpg(proc.pid, signal.SIGTERM)
            try:
                proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                os.killpg(proc.pid, signal.SIGKILL)
                proc.wait(timeout=5)
        except ProcessLookupError:
            pass
        log.close()
        # 自清理本脚本创建的 /private/tmp 工作目录，避免新增外部残留。
        shutil.rmtree(tmp, ignore_errors=True)

    (out / 'reset-probe.json').write_text(json.dumps(rows, indent=1) + '\n')

    # 汇总
    def summarize():
        fresh = rows[0]['results']
        fresh_panic_json = sum(
            1 for r in fresh
            if r.get('status') == 500 and json.loads(r.get('body', '{}')) == PANIC_BODY)
        fresh_reset = sum(1 for r in fresh if 'transport_error' in r)
        seq = rows[1]['results']
        panic_will_close = [
            {'method': r['method'], 'will_close': r.get('will_close'),
             'connection_header': r.get('headers', {}).get('connection'),
             'transport_error': r.get('transport_error')}
            for r in seq if r['method'].startswith('Ingress.')]
        reuse = rows[2]['results']
        reuse_second_reset = sum(
            1 for r in reuse
            if r['second'] and 'transport_error' in r['second'])
        reuse_first_will_close = sum(
            1 for r in reuse if r['first'].get('will_close'))
        return {
            'rounds': args.rounds,
            'fresh_panic_json': fresh_panic_json,
            'fresh_reset': fresh_reset,
            'persistent_panic_connection': panic_will_close,
            'reuse_first_will_close': reuse_first_will_close,
            'reuse_second_reset': reuse_second_reset,
        }
    summary = summarize()
    (out / 'reset-summary.json').write_text(json.dumps(summary, indent=1) + '\n')
    print(json.dumps(summary, ensure_ascii=False))


if __name__ == '__main__':
    main()