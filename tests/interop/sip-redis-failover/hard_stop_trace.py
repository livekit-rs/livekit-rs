"""Sanitized Redis MONITOR attribution for the real SIP SDK probe."""
import ast
import base64
import binascii
import hashlib
import re

def redis_slot(key):
    tagged = re.search(rb'{([^}]*)}', key)
    if tagged and tagged[1]:
        key = tagged[1]
    return binascii.crc_hqx(key, 0) % 16384

def trace_summary(stage, call_id, key_owner, details):
    # Redis MONITOR uses escaped quoted byte strings. Emit only route,
    # timing and digest data, never raw request or application content.
    room = details.get('room', '').encode()
    trunk = details.get('trunk', '').encode()
    topic_prefix = f'SIPInternal|TransferSIPParticipant|{call_id}|'.encode()
    cleanup_keys = {
        b'sip_trunk', b'sip_inbound_trunk', b'sip_outbound_trunk', b'rooms', b'room_internal',
        b'room_participants:' + room, b'agent_dispatch:' + room, b'agent_job:' + room,
    }
    records, keys = [], {}
    for path in sorted(stage.glob('monitor-*.raw')):
        port = int(path.stem.split('-')[1])
        for line in path.read_text(errors='replace').splitlines():
            prefix = re.match(r'^([\d.]+) \[[^]]+\] ', line)
            if not prefix:
                continue
            try:
                parts = [ast.literal_eval('b' + token)
                         for token in re.findall(r'"(?:[^"\\]|\\.)*"', line[prefix.end():])]
            except (SyntaxError, ValueError):
                continue
            if len(parts) < 2:
                continue
            command, key, *args = parts
            command = command.upper()
            if command == b'PUBLISH' and key.startswith(topic_prefix) and len(args) == 1:
                digest = hashlib.sha256(args[0]).digest()
                queue = base64.b64encode(digest)
                if key.endswith(b'|REQ'):
                    keys[queue] = digest.hex()
                records.append(dict(time=float(prefix[1]), port=port, command='PUBLISH',
                                    channel_kind=key.split(b'|')[-1].decode(),
                                    payload_sha256=digest.hex(), queue_slot=redis_slot(queue),
                                    queue_old_owner=key_owner[redis_slot(queue)]))
            elif command == b'SET' and any(a.upper() == b'NX' for a in args):
                records.append(dict(time=float(prefix[1]), port=port, command='SET NX',
                                    key=key, slot=redis_slot(key)))
            elif key in cleanup_keys and (room in key or room in args or trunk in args) and command in (
                b'HGET', b'HDEL', b'DEL'
            ):
                bucket = key.split(b':')[0].decode()
                slot = redis_slot(key)
                records.append(dict(time=float(prefix[1]), port=port, command=command.decode(),
                                    store_bucket=bucket, slot=slot, old_owner=key_owner[slot]))
            elif command == b'CLUSTER' and key.upper() in (b'SLOTS', b'SHARDS'):
                records.append(dict(time=float(prefix[1]), port=port,
                                    command='CLUSTER ' + key.decode().upper()))
    selected = []
    for record in records:
        key = record.pop('key', None)
        if key is None:
            selected.append(record)
        elif key in keys:
            selected.append(dict(record, request_payload_sha256=keys[key]))
    return sorted(selected, key=lambda row: row['time'])


def slot_owners(redis, port):
    owners = {}
    for line in redis(port, 'CLUSTER', 'NODES').splitlines():
        row = line.split()
        flags = set(row[2].split(','))
        if 'master' not in flags or flags.intersection({'fail', 'fail?'}):
            continue
        owner = int(row[1].split('@')[0].rsplit(':', 1)[1])
        for slot_range in row[8:]:
            first, _, last = slot_range.partition('-')
            for slot in range(int(first), int(last or first) + 1):
                if slot in owners:
                    raise ValueError(f'conflicting owners for slot {slot}')
                owners[slot] = owner
    return owners


def cleanup_readback(redis, owners, details):
    room = details['room']
    trunk = details['trunk']
    keys = [
        ('sip_trunk', trunk), ('sip_inbound_trunk', trunk), ('sip_outbound_trunk', trunk),
        ('rooms', room), ('room_internal', room), ('room_participants:' + room, None),
        ('agent_dispatch:' + room, None), ('agent_job:' + room, None),
    ]
    rows = []
    for key, field in keys:
        slot = redis_slot(key.encode())
        port = owners[slot]
        command = ('HEXISTS', key, field) if field is not None else ('EXISTS', key)
        exists = redis(port, *command)
        if exists not in {'0', '1'}:
            raise ValueError(f'cleanup readback failed for {key.split(":")[0]}: {exists}')
        rows.append(dict(bucket=key.split(':')[0], slot=slot, port=port, exists=exists == '1'))
    return rows


def transfer_stages(records, probe, transition, readback):
    details = probe.get('transfer', {})
    requests = [row for row in records if row.get('channel_kind') == 'REQ']
    claims = [row for row in records if row.get('channel_kind') == 'RCLAIM']
    saves = [row for row in records if row['command'] == 'SET NX']
    sdk_cleanup = probe.get('cleanup', [])
    return dict(
        sdk_transfer=details,
        requests=requests,
        dedup_set_nx=saves,
        claim_count=len(claims),
        stale_worker_slot=any(row['queue_old_owner'] == transition['previous_primary'] for row in requests),
        sip=probe.get('sipTransfer', {}),
        refer_count=(probe.get('afterTransfer') or probe.get('failureSnapshots') or {})
        .get('peer', {}).get('referReceived'),
        fixture_teardown_bye_accepted=probe.get('fixtureOriginalDialogByeAccepted'),
        sdk_cleanup=sdk_cleanup,
        stores_empty=bool(readback) and all(not row['exists'] for row in readback),
        healthy_transfer=bool(len(requests) == len(saves) == len(claims) == 1
                              and requests[0]['payload_sha256'] == saves[0]['request_payload_sha256']
                              and requests[0]['time'] <= saves[0]['time'] <= claims[0]['time']
                              and details.get('ok') and probe.get('sipTransfer', {}).get('rtc_left')),
    )
