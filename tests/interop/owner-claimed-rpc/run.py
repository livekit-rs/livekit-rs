#!/usr/bin/env python3
"""Fixed Go/Rust owner faults after a real cross-node PSRPC claim and SDK callback."""
import argparse
import ast
import gzip
import hashlib
import json
import os
from pathlib import Path
import random
import re
import signal
import socket
import subprocess
import tempfile
import time
import urllib.error
import urllib.request
import uuid

HOST = '127.0.0.1'
KEY = 'heldrpc'
SECRET = 'public-owner-held-rpc-fixture-secret-0123456789'
GO_SHA256 = '4bc2378e636740b29093061036249ffc9ebbef91f4ece1de12177c8dfee1e7dc'
HERE = Path(__file__).resolve().parent
PAYLOAD = 'fault-barrier-request'


def sha(path):
    with path.open('rb') as file:
        return hashlib.file_digest(file, 'sha256').hexdigest()


def save(path, value):
    path.write_text(json.dumps(value, indent=2) + '\n')


def wait(check, label, seconds=20):
    until = time.monotonic() + seconds
    while time.monotonic() < until:
        if value := check():
            return value
        time.sleep(.025)
    raise TimeoutError(label)


def live_file(path, process):
    if path.is_file():
        try:
            return json.loads(path.read_bytes())
        except ValueError:
            pass
    if process.poll() is not None:
        raise RuntimeError(f'{path.name} missing, SDK probe exit={process.returncode}')
    return None


def ports(kinds):
    # Stay out of the ephemeral source-port range used by the SDK/Redis.
    sockets = []
    try:
        for kind in kinds:
            listener = socket.socket(socket.AF_INET, kind)
            sockets.append(listener)
            for _ in range(150):
                try:
                    listener.bind(('0.0.0.0', random.randint(20000, 41000)))
                    break
                except OSError:
                    pass
            else:
                raise OSError('no available fixture port')
        return [listener.getsockname()[1] for listener in sockets]
    finally:
        for listener in sockets:
            listener.close()


def released(port, kind):
    with socket.socket(socket.AF_INET, kind) as listener:
        listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        try:
            listener.bind(('0.0.0.0', port))
            return True
        except OSError:
            return False


def protobuf_fields(data):
    """Read just the fixed PSRPC field numbers used in the receipt."""
    def varint(index):
        value = 0
        for shift in range(0, 70, 7):
            item = data[index]
            index += 1
            value |= (item & 127) << shift
            if item < 128:
                return value, index
        raise ValueError('long varint')

    fields = {}
    i = 0
    while i < len(data):
        key, i = varint(i)
        wire = key & 7
        if wire == 0:
            value, i = varint(i)
        elif wire == 2:
            size, i = varint(i)
            value = data[i:i+size]
            i += size
        elif wire in (1, 5):
            i += 8 if wire == 1 else 4
            continue
        else:
            raise ValueError('unsupported protobuf wire type')
        if i > len(data):
            raise ValueError('truncated protobuf')
        fields[key >> 3] = value
    return fields


def owner_wire(path, topic, payloads):
    # Redis MONITOR records native messages. Drop their payload and unrelated
    # records after decoding request and peer IDs to correlate the handshake.
    records = []
    calls = {}
    for line in path.read_text(errors='replace').splitlines():
        prefix = re.match(r'^([\d.]+) \[[^]]+\] ', line)
        if not prefix or '"publish" ' not in line.lower():
            continue
        try:
            parts = [ast.literal_eval('b' + token)
                     for token in re.findall(r'"(?:[^"\\]|\\.)*"', line[prefix.end():])]
            if len(parts) != 3 or parts[0].upper() != b'PUBLISH':
                continue
            _, channel, payload = parts
            if not channel.startswith(b'Participant|'):
                continue
            envelope = protobuf_fields(payload)
            wire = envelope[2]
            if envelope.get(4, 0) == 1:
                wire = gzip.decompress(wire)
            fields = protobuf_fields(wire)
            kind = channel.rsplit(b'|', 1)[-1]
            request_id = fields[1].decode()
            peer = fields[2].decode()
            if kind == b'REQ' and channel == topic+b'|REQ':
                request = protobuf_fields(fields[8] if 8 in fields else protobuf_fields(fields[6])[2])
                name = request.get(4, b'').decode()
                if request.get(3) != b'cluster.echo' or name not in payloads:
                    continue
                calls[request_id] = (peer, name)
            elif request_id not in calls:
                continue
            elif kind in (b'CLAIM', b'RES') and channel != b'Participant|' + calls[request_id][0].encode() + b'|' + kind:
                continue
            elif kind == b'RCLAIM' and channel != topic+b'|RCLAIM':
                continue
            records.append(dict(
                timestamp=float(prefix[1]), kind=kind.decode(), request_id=request_id, call=calls[request_id][1],
                peer_id=peer, type_url=envelope[1].decode(), sha256=hashlib.sha256(payload).hexdigest(),
            ))
        except (ValueError, KeyError, IndexError, TypeError, OSError):
            continue
    return records


def http_ready(port, process):
    if process.poll() is not None:
        raise RuntimeError(f'HTTP process exited before readiness: {process.returncode}')
    try:
        with urllib.request.urlopen(f'http://{HOST}:{port}/', timeout=.5) as response:
            return response.status == 200
    except (OSError, urllib.error.URLError):
        return False


def run_case(args, kind, fault, output):
    output.mkdir()
    tcp, udp = socket.SOCK_STREAM, socket.SOCK_DGRAM
    selected = [tcp, tcp, tcp, udp, tcp, tcp, udp]
    redis_port, owner_http, owner_tcp, owner_udp, front_http, front_tcp, front_udp = ports(selected)
    env = {**os.environ, 'LIVEKIT_API_KEY':KEY, 'LIVEKIT_API_SECRET':SECRET}
    env['TEST_ROOM'] = 'heldrpc' + uuid.uuid4().hex[:10]
    payloads = [PAYLOAD] if args.concurrency==1 else [f'{PAYLOAD}-{i}' for i in range(args.concurrency)]
    room = env['TEST_ROOM']
    topic = f'Participant|PerformRpc|{room}_observer'.encode()
    result = {'implementation':kind, 'fault':fault, 'status':'FAIL', 'commands':[], 'processes':[],
              'concurrency':args.concurrency, 'requested_single_pc':args.single_pc}
    processes = []
    with tempfile.TemporaryDirectory(prefix='lk-held-rpc-') as temp:
        temp = Path(temp)
        monitor_path = temp/'redis-monitor.log'
        def start(name, command, path=None, options=None):
            path = path or output/f'{name}.log'
            command = list(map(str,command))
            result['commands'].append(command)
            with path.open('wb') as log:
                process = subprocess.Popen(command, stdout=log, stderr=subprocess.STDOUT,
                                           env=options or env)
            processes.append((name, process))
            return process

        def redis(*arguments):
            return subprocess.check_output(['redis-cli','--raw','-h',HOST,'-p',str(redis_port),
                                            *map(str,arguments)], stderr=subprocess.DEVNULL, timeout=3)

        def state(node):
            raw = redis('HGET','nodes',node)
            return protobuf_fields(raw[:-1]).get(6) if raw != b'\n' else None

        def readback():
            stored = redis('HGET','rooms',room)
            room_sid = protobuf_fields(stored[:-1])[1].decode() if stored!=b'\n' else None
            def subscribers(channel):
                return int(redis('PUBSUB','NUMSUB',channel).splitlines()[-1])
            return {
                'room_owner':redis('HGET','room_node_map',room).decode().strip(),
                'room':room_sid is not None,
                'room_sid':room_sid,
                'internal':redis('HEXISTS','room_internal',room).strip()==b'1',
                'participants':int(redis('HLEN','room_participants:'+room)),
                'nodes':redis('HKEYS','nodes').decode().split(),
                'participant_rpc_subscribers':subscribers((topic+b'|REQ').decode()),
            }

        try:
            broker = start('redis',['redis-server','--bind',HOST,'--port',redis_port,
                                    '--save','','--appendonly','no','--dir',temp])
            def ping():
                if broker.poll() is not None:
                    raise RuntimeError(f'owned broker exited: {broker.returncode}')
                try:
                    return redis('PING').strip()==b'PONG'
                except subprocess.CalledProcessError:
                    return False
            wait(ping, 'owned Redis ready')
            binary = getattr(args,kind)
            for side, http, rtc_tcp, rtc_udp in [('owner',owner_http,owner_tcp,owner_udp),
                                                ('front',front_http,front_tcp,front_udp)]:
                config = output/f'{side}.json'
                save(config, {'port':http, 'bind_addresses':[HOST], 'keys':{KEY:SECRET},
                              'turn':{'enabled':False}, 'logging':{'level':'info'},
                              'redis':{'address':f'{HOST}:{redis_port}'},
                              'rtc':{'node_ip':HOST,'tcp_port':rtc_tcp,'udp_port':rtc_udp,
                                     'use_external_ip':False,'enable_loopback_candidate':True,
                                     'ips':{'includes':[HOST+'/32']}}})
                process = start(side,[binary,'--config',config])
                wait(lambda:http_ready(http,process),f'{side} HTTP ready')
                nodes = redis('HKEYS','nodes').decode().split()
                if side == 'owner':
                    assert len(nodes)==1, nodes
                    owner, owner_node = process, nodes[0]
                else:
                    assert len(nodes)==2 and owner_node in nodes, nodes
                    front, front_node = process, next(node for node in nodes if node!=owner_node)
            monitor = start('monitor',['redis-cli','--raw','-h',HOST,'-p',redis_port,'MONITOR'], monitor_path)
            wait(lambda:monitor_path.read_bytes().startswith(b'OK'), 'MONITOR subscribed')
            probe = start('probe',['node',HERE/'probe.mjs'], options={
                **env, 'FRONT_URL':f'http://{HOST}:{front_http}', 'OWNER_URL':f'http://{HOST}:{owner_http}',
                'OWNER_NODE':owner_node, 'CASE_DIRECTORY':str(output), 'OWNER_FAULT':fault,
                'HELD_PAYLOADS':json.dumps(payloads), 'SINGLE_PC':args.single_pc,
            })
            ready = wait(lambda:live_file(output/'ready.json',probe),'actual SDK media ready')
            assert ready['roomSid'].startswith('RM_'), ready
            result['room_sid'] = ready['roomSid']
            result['connected_peer_connections'] = ready['connectedPeerConnections']
            result['owner_node'] = owner_node
            result['front_node'] = front_node
            result['before'] = before = readback()
            assert before['room_owner']==owner_node and before['room'] and before['participants']==2, before
            assert before['participant_rpc_subscribers']==1, before
            clients = {
                payload: start(f'rpc-{index}',[args.client,f'http://{HOST}:{front_http}',room,payload])
                for index,payload in enumerate(payloads)
            }
            wait(lambda:live_file(output/'accepted.json',probe),'SDK callback entered')
            result['accepted_sdk'] = json.loads((output/'accepted.json').read_text())

            def claimed():
                rows = owner_wire(monitor_path,topic,payloads)
                return rows if all([r['kind'] for r in rows if r['call']==payload] ==
                                   ['REQ','CLAIM','RCLAIM'] for payload in payloads) else None

            result['pre_fault_wire'] = claim = wait(claimed, 'matched REQ, CLAIM and RCLAIM for each call', 4)
            grants = {}
            for payload in payloads:
                request, accepted, grant = [row for row in claim if row['call']==payload]
                assert request['request_id']==accepted['request_id']==grant['request_id'], claim
                assert accepted['peer_id']==grant['peer_id'], claim
                grants[payload] = grant
            assert len({row['request_id'] for row in grants.values()}) == len(payloads), claim
            assert all(rpc.poll() is None for rpc in clients.values()) and owner.poll() is None, 'RPCs must remain held on live owner'
            assert http_ready(front_http,front)
            if fault == 'drain':
                owner.send_signal(signal.SIGTERM)
                wait(lambda:state(owner_node)==2, 'owner entered SHUTTING_DOWN', 3)
                assert owner.poll() is None and all(rpc.poll() is None for rpc in clients.values()), 'accepted calls must outlive drain transition'
                result['owner_state_after_fault'] = 2
            else:
                owner.kill()
                assert owner.wait(timeout=3)==-9
                owner_killed_at = time.monotonic()
                result['owner_ports_after_fault'] = [
                    released(port,kind) for port,kind in [(owner_http,tcp),(owner_tcp,tcp),(owner_udp,udp)]
                ]
                assert all(result['owner_ports_after_fault'])
            save(output/'fault.json',{'fault':fault,'owner_exit':owner.poll(),'timestamp':time.time()})
            if fault == 'drain':
                result['drain_media'] = wait(lambda:live_file(output/'drain-media.json',probe),'media advances during drain')
            result['calls'] = {}
            for release_index,payload in enumerate(reversed(payloads)):
                if fault == 'drain':
                    earlier = payloads[:len(payloads)-release_index-1]
                    assert all(clients[key].poll() is None for key in earlier)
                    save(output/f'release-{release_index}.json',{'payload':payload})
                exit_code = clients[payload].wait(timeout=22)
                rpc = json.loads((output/f'rpc-{payloads.index(payload)}.log').read_text())
                result['calls'][payload] = {'exit':exit_code,'response':rpc}
                assert exit_code==0, rpc
                if fault == 'drain':
                    assert all(clients[key].poll() is None for key in earlier), 'a different call finished while still held'
            if len(payloads)==1:
                result['rpc_exit'] = result['calls'][PAYLOAD]['exit']
                result['rpc'] = result['calls'][PAYLOAD]['response']
            save(output/'rpc-done.json',result['calls'])
            result['probe_exit'] = probe.wait(timeout=15)
            result['probe'] = json.loads((output/'probe.json').read_text())
            result['wire'] = rows = owner_wire(monitor_path,topic,payloads)
            result['front_healthy_after_fault'] = http_ready(front_http,front)
            assert result['front_healthy_after_fault']
            request_rows = [r for r in rows if r['kind']=='REQ']
            response_rows = [r for r in rows if r['kind']=='RES']
            result['request_count'] = len(request_rows)
            result['response_count'] = len(response_rows)
            if fault == 'drain':
                assert [r['call'] for r in response_rows]==list(reversed(payloads)), rows
                for payload in payloads:
                    rpc = result['calls'][payload]['response']
                    assert rpc['http_status']==200 and rpc['payload']=='remote:'+payload, rpc
                    selected_rows = [row for row in rows if row['call']==payload]
                    assert [r['kind'] for r in selected_rows]==['REQ','CLAIM','RCLAIM','RES'], rows
                    response = selected_rows[-1]
                    assert response['request_id']==grants[payload]['request_id'], rows
                    assert response['peer_id']==grants[payload]['peer_id'], rows
                assert owner.wait(timeout=15)==0, 'owner did not shut down gracefully'
                result['after'] = after = readback()
                assert owner_node not in after['nodes'] and after['nodes']==[front_node], after
                assert not (after['room_owner'] or after['room'] or after['internal'] or after['participants']
                            or after['participant_rpc_subscribers']), after
            else:
                for payload in payloads:
                    rpc = result['calls'][payload]['response']
                    assert {key:value for key,value in rpc.items() if key!='elapsed_ms'} == {
                        'http_status':503, 'code':'unavailable', 'message':'no response from servers', 'metadata':None
                    }, rpc
                    assert [r['kind'] for r in rows if r['call']==payload]==['REQ','CLAIM','RCLAIM','REQ','REQ'], rows
                assert len(request_rows)==len({r['request_id'] for r in request_rows})==3*len(payloads), rows
                result['after'] = after = readback()
                assert set(after['nodes'])=={front_node,owner_node} and after['participant_rpc_subscribers']==0, after
                assert after['room_owner']==owner_node and after['room'] and after['internal'] and after['participants']==2, after
            assert result['probe_exit']==0 and result['probe']['status']=='PASS', 'SDK/browser teardown failed'
            if args.recreate:
                if fault=='kill':
                    # The fixed selector ages a node out at five seconds.
                    # Preserve the stale Redis state above before waiting.
                    time.sleep(max(0,owner_killed_at+5.1-time.monotonic()))
                url = f'http://{HOST}:{front_http}'
                create = start('recreate',['node',HERE/'room-control.mjs','create',url,room,front_node])
                assert create.wait(timeout=22)==0, 'concurrent CreateRoom SDK requests failed'
                result['recreate'] = replay = json.loads((output/'recreate.log').read_text())
                assert len(replay['created'])==2 and replay['created'][0]==replay['created'][1], replay
                assert replay['create_http_overlap']==2, replay
                assert replay['listed']==[replay['created'][0]], replay
                recreated_sid = replay['created'][0]['sid']
                assert recreated_sid.startswith('RM_'), replay
                assert (recreated_sid==result['room_sid']) == (fault=='kill'), replay
                assert replay['created'][0]['metadata']=='concurrent-fault-create', replay
                result['recreate_redis'] = recreated = readback()
                assert recreated['room_owner']==front_node and recreated['room_sid']==recreated_sid, recreated
                delete = start('recreated-delete',['node',HERE/'room-control.mjs','delete',url,room])
                assert delete.wait(timeout=22)==0, 'DeleteRoom SDK request failed'
                assert json.loads((output/'recreated-delete.log').read_text())=={'listed':[]}

                def cleaned():
                    values = readback()
                    absent = ('room_owner','room','internal','participants','participant_rpc_subscribers')
                    return values if not any(values[key] for key in absent) else None

                result['recreate_deleted_redis'] = wait(cleaned, 'recreated room Redis and topic cleanup')
            result['status'] = 'OBSERVED'
        except Exception as error:
            result['error'] = f'{type(error).__name__}: {error}'
            if monitor_path.is_file():
                result['wire'] = owner_wire(monitor_path,topic,payloads)
        finally:
            for name,process in reversed(processes):
                forced = False
                if process.poll() is None:
                    process.send_signal(signal.SIGTERM)
                    try:
                        process.wait(timeout=16)
                    except subprocess.TimeoutExpired:
                        forced = True
                        process.kill()
                        process.wait()
                expected = -9 if name=='owner' and fault=='kill' and 'owner_ports_after_fault' in result else (
                    -15 if name=='monitor' else 0)
                result['processes'].append(dict(name=name,pid=process.pid,exit=process.returncode,
                                                expected=expected,forced=forced))
                if forced or process.returncode != expected:
                    result['status']='FAIL'
            result['ports_released'] = [
                released(port,kind) for port,kind in zip(
                    [redis_port,owner_http,owner_tcp,owner_udp,front_http,front_tcp,front_udp], selected, strict=True)
            ]
            if not all(result['ports_released']):
                result['status']='FAIL'
            pid_file = output/'browser-pid.json'
            if pid_file.is_file():
                pid = json.loads(pid_file.read_text())['pid']
                try:
                    os.kill(pid,0)
                except ProcessLookupError:
                    result['browser_exited']=True
                else:
                    result['browser_exited']=False
                    result['status']='FAIL'
                    os.kill(pid,signal.SIGTERM)
            save(output/'execution.json',result)
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ('go','rust','client','output'):
        parser.add_argument('--'+name,type=lambda value:Path(value).resolve(),required=True)
    parser.add_argument('--fault',choices=('drain','kill'),action='append')
    parser.add_argument('--engine',choices=('go','rust'),action='append')
    parser.add_argument('--concurrency',type=int,choices=(1,2),default=1)
    parser.add_argument('--single-pc',choices=('default','true','false'),default='default')
    parser.add_argument('--recreate',action='store_true',help='test two concurrent SDK creates on the survivor after fault readback')
    args = parser.parse_args()
    assert sha(args.go)==GO_SHA256, 'fixed Go v1.13.10 binary required'
    args.output.mkdir()
    rows = []
    evidence = {'binaries':{name:sha(getattr(args,name)) for name in ('go','rust','client')},
                'inputs':{str(path.relative_to(HERE.parent)):sha(path) for path in (
                    Path(__file__).resolve(), HERE/'probe.mjs', HERE/'rpc-client.go', HERE/'room-control.mjs',
                    HERE.parent/'cluster-room-api/client.html', HERE.parent/'package-lock.json')},
                'cases':rows}
    for fault in args.fault or ('drain','kill'):
        for kind in args.engine or ('go','rust'):
            row = run_case(args,kind,fault,args.output/f'{fault}-{kind}')
            rows.append(row)
            print(fault,kind,row['status'],row.get('calls'),row.get('error'),flush=True)
            save(args.output/'comparison.json',evidence)
    pairs = {}
    for fault in {row['fault'] for row in rows}:
        sides = [row for row in rows if row['fault']==fault]
        if len(sides)==2 and all(row['status']=='OBSERVED' for row in sides):
            def outcome(row):
                return {
                    call:{key:value for key,value in item['response'].items() if key!='elapsed_ms'}
                    for call,item in row['calls'].items()
                }
            pairs[fault]=dict(rpc_equal=outcome(sides[0])==outcome(sides[1]),
                              request_count_equal=sides[0]['request_count']==sides[1]['request_count'],
                              sdk_equal=all(sides[0]['probe'][field]==sides[1]['probe'][field]
                                            for field in ('sdkSha256','sdkVersion')))
            if args.recreate:
                pairs[fault]['recreated_participant_count_equal'] = (
                    sides[0]['recreate_redis']['participants']==sides[1]['recreate_redis']['participants'])
    evidence['paired_outcomes'] = pairs
    save(args.output/'comparison.json',evidence)
    return int(any(row['status']!='OBSERVED' for row in rows)
               or any(not all(pair.values()) for pair in pairs.values()))


if __name__=='__main__':
    raise SystemExit(main())
