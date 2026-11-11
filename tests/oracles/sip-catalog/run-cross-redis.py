#!/usr/bin/env python3
"""Actual fixed-Go RedisStore versus production Rust SipCatalog. No broker startup.
All raw subprocess input/output is private and in memory. Report only counts/hashes.
"""
import argparse, base64, hashlib, json, pathlib, subprocess, time, uuid


def varint(v):
    out = bytearray()
    while v > 127:
        out.append((v & 127) | 128)
        v >>= 7
    return bytes(out + bytes([v]))


def field(n, value):
    if isinstance(value, int):
        return varint(n << 3) + varint(value)
    if isinstance(value, str):
        value = value.encode()
    return varint((n << 3) | 2) + varint(len(value)) + value


def message(*fields):
    return b''.join(field(*v) for v in fields)


def sha(value):
    return hashlib.sha256(value).hexdigest()


def semantic(row):
    if row.get('error'):
        return {'error': row['error']}
    return {'nil': row.get('nil', False), 'json': row.get('json')}


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--go-bin', required=True)
    parser.add_argument('--rust-bin', required=True)
    parser.add_argument('--redis', required=True, help='dedicated owned Redis database URL; never logged')
    parser.add_argument('--out', required=True)
    args = parser.parse_args()
    run = uuid.uuid4().hex
    rows = []

    def command(op, raw=b'', ident=''):
        return {'run_id': run, 'op': op, 'id': ident, 'protobuf': base64.b64encode(raw).decode()}

    def invoke(binary, commands):
        data = b''.join(json.dumps(v, separators=(',', ':')).encode() + b'\n' for v in commands)
        proc = subprocess.run([binary, '--redis', args.redis, '--run-id', run], input=data,
                              stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=75)
        if proc.returncode:
            raise RuntimeError('oracle process failed (raw diagnostics withheld): exit=%s stderr_sha256=%s' % (proc.returncode, sha(proc.stderr)))
        output = [json.loads(line) for line in proc.stdout.splitlines()]
        if len(output) != len(commands):
            raise RuntimeError('oracle response cardinality mismatch')
        for c, r in zip(commands, output):
            if r.get('run_id') != run or r.get('op') != c['op']:
                raise RuntimeError('stale or mismatched oracle result identity')
        return output

    for direction, writer in [('go-write', args.go_bin), ('rust-write', args.rust_bin)]:
        prefix = 'ST_' + run + '_' + direction + '_'
        ids = [prefix + v for v in ['a', 'b', 'c', 'd', 'e', 'f']]
        ruleid = 'SD_' + run + '_' + direction
        # Actual wire field numbers from the locked public .proto. Values are
        # synthetic; reader expectation comes from real Go helpers, not this encoder.
        stores = [
            command('store_legacy', message((1, ids[0]), (4, '+123456'), (11, 'legacy'), (14, 0))),
            command('store_inbound', message((1, ids[0]), (2, 'modern-preferred'), (4, '123456'), (7, 'synthetic-user'), (8, 'synthetic-pass'))),
            command('store_outbound', message((1, ids[1]), (2, 'outbound'), (4, 'example.invalid'), (5, 2), (6, '123456'))),
            command('store_legacy', message((1, ids[2]), (14, 2))),
            command('store_legacy', message((1, ids[3]), (14, 1))),
            command('store_inbound', message((1, ids[4]), (2, 'wildcard'))),
            command('store_rule', message((1, ruleid), (3, ids[0]), (5, 'rule-fixture'))),
        ]
        # More than default store page size, with a later matching filter hole.
        bulkids = [prefix + 'p%04d' % i for i in range(1005)]
        stores += [command('store_inbound', message((1, ident), (4, '+2' if i == 1004 else '+1')))
                   for i, ident in enumerate(bulkids)]
        output = invoke(writer, stores)
        if any(r.get('error') for r in output):
            raise RuntimeError('actual catalog write failed')
        reads = [command('encoding_nil_inbound'), command('encoding_nil_get')]
        reads += [command(op, ident=ident) for ident in ids
                  for op in ['load_legacy', 'load_inbound', 'load_outbound']]
        reads += [command('load_rule', ident=ruleid), command('load_rule', ident=ruleid + '_missing')]
        reads += [command('list_legacy'), command('list_inbound'), command('list_outbound'), command('list_rules')]
        # Store-page request field is 3 for inbound/outbound, 1 for legacy.
        for page in [message((1, prefix), (2, 0)), message((1, bulkids[999]), (2, 0)),
                     message((1, bulkids[1000] + 'x'), (2, 1)), message((1, prefix), (2, 1001))]:
            reads += [command('list_inbound', message((3, page))),
                      command('list_inbound', message((2, '+2'), (3, page)))]
        reads += [command('list_inbound', message((1, ids[0]), (2, '+1 (234) 56'))),
                  command('list_outbound', message((1, ids[1]), (2, '+1 (234) 56')))]
        expected = invoke(args.go_bin, reads)
        actual = invoke(args.rust_bin, reads)
        for index, (e, a) in enumerate(zip(expected, actual)):
            want = json.dumps(semantic(e), sort_keys=True, separators=(',', ':')).encode()
            got = json.dumps(semantic(a), sort_keys=True, separators=(',', ':')).encode()
            rows.append({'direction': direction, 'case_ordinal': index, 'operation': reads[index]['op'],
                         'status': 'PASS' if want == got else 'FAIL',
                         'expected_semantics_sha256': sha(want), 'actual_semantics_sha256': sha(got)})
            if want != got:
                raise RuntimeError('real cross-store semantic mismatch at %s ordinal %d' % (direction, index))
        # Public delete-rule pre-load is intentionally outside this Store oracle.
        deletes = [command('delete_trunk', ident=i) for i in ids + bulkids]
        deletes += [command('delete_trunk', ident=ids[0]), command('delete_rule', ident=ruleid), command('delete_rule', ident=ruleid)]
        deleted = invoke(writer, deletes)
        if any(r.get('error') for r in deleted):
            raise RuntimeError('actual idempotent store deletion failed')
        checks = [command('load_legacy', ident=ids[0]), command('load_rule', ident=ruleid)]
        e = invoke(args.go_bin, checks)
        a = invoke(args.rust_bin, checks)
        if [semantic(v) for v in e] != [semantic(v) for v in a] or not all(v.get('error') for v in a):
            raise RuntimeError('actual deleted catalog state mismatch')
        rows.append({'direction': direction, 'operation': 'delete-and-reload', 'status': 'PASS',
                     'actual_writes': len(stores), 'actual_deletes': len(deletes)})

    report = {'status': 'PASS', 'scope': 'real catalog storage only; not SIP domain or worker acceptance',
              'run_identity_sha256': sha(run.encode()), 'go_binary_sha256': sha(pathlib.Path(args.go_bin).read_bytes()),
              'rust_binary_sha256': sha(pathlib.Path(args.rust_bin).read_bytes()), 'cases': rows,
              'passed': len(rows), 'finished_unix_ns': time.time_ns()}
    pathlib.Path(args.out).write_text(json.dumps(report, indent=2) + '\n')
    print(json.dumps({'status': 'PASS', 'passed': len(rows)}))


if __name__ == '__main__':
    main()
