
# ------------------------------------------------------------------------------- driver
def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument('--record', action='store_true')
    ap.add_argument('--report', help='re-derive the verdict from a filed execution.json')
    ap.add_argument('--selftest-gate', help='apply named mutations to a recorded execution.json')
    ap.add_argument('--out', default='/tmp/c105-lane')
    ap.add_argument('--go', default='/tmp/c99-build/livekit-server-go')
    ap.add_argument('--rust', default=str(REPO / 'target/release/livecloud'))
    ap.add_argument('--only', help='comma-separated profile ids; a subset is never an acceptance')
    ap.add_argument('--hold', type=float, default=0.6)
    a = ap.parse_args()
    out = Path(a.out)
    (out / 'logs').mkdir(parents=True, exist_ok=True)
    harness_sha = C102.sha256_file(Path(__file__).resolve())
    c102_sha = C102.sha256_file(C102_DIR / 'run-paired.py')
    c103_sha = C102.sha256_file(C103_DIR / 'run-paired.py')
    c104_sha = C102.sha256_file(C104_DIR / 'run-paired.py')
    if a.report or a.selftest_gate:
        src = Path(a.report or a.selftest_gate)
        if src.is_dir():
            src = src / 'execution.json'
        doc = json.loads(src.read_text())
        if a.only:
            wanted = set(a.only.split(','))
            doc['profiles'] = [e for e in doc['profiles'] if e['profile'] in wanted]
            doc['subset'] = a.only
            print(f'development subset: {sorted(wanted)}', flush=True)
        if a.report:
            result = finish(doc, out=None, live=False, write=False)
            (out / 'report-rederivation.txt').write_text(json.dumps(result, indent=2) + '\n')
            print(json.dumps(result['summary'], indent=2))
            return 0 if result['all_ok'] else 1
        report = selftest_gate(doc, out, src)
        print(json.dumps({k: report[k] for k in ('cases', 'baseline_legs', 'baseline_controls',
                                                 'all_discriminated', 'undiscriminated')}, indent=2))
        return 0 if report['all_discriminated'] else 1
    if not a.record:
        ap.error('choose --record, --report or --selftest-gate')
    bins = {}
    for side, path in (('go', a.go), ('rust', a.rust)):
        pth = Path(path)
        if not pth.is_file():
            print(f'missing {side} binary: {pth}', file=sys.stderr)
            return 2
        bins[side] = {'path': str(pth), 'sha256': C102.sha256_file(pth)}
    for side, pinned in (('go', ORACLE_SHA256), ('rust', ACCEPTANCE_SHA256)):
        if bins[side]['sha256'] != pinned:
            print(f'refusing: the {side} image is {bins[side]["sha256"]}, pinned {pinned}',
                  file=sys.stderr)
            return 2
    bounds = {str(port): {'before': C102.listeners_on(port), 'after': None}
              for port in list(OFF_LIMITS) + [HOST_STORE_PORT]}
    ctx = build_ctx(out, a.hold)
    masker = Masker(ctx['port_roles'], {str(ctx['work']): '<WORK>', str(REPO): '<REPO>',
                                        str(Path.home()): '<HOME>'})
    profs = profiles(ctx)
    if len(profs) != PROFILE_COUNT:
        print(f'refusing: the profile table has {len(profs)} entries, the file claims '
              f'{PROFILE_COUNT}', file=sys.stderr)
        return 2
    full_table = list(profs)
    if a.only:
        wanted = set(a.only.split(','))
        missing = sorted(wanted - {p['id'] for p in profs})
        if missing:
            print(f'refusing: unknown profile ids {missing}', file=sys.stderr)
            return 2
        profs = [p for p in profs if p['id'] in wanted]
        print(f'development subset: {sorted(wanted)}', flush=True)
    store = C102.start_store(ctx['store_port'])
    doc = None
    try:
        deadline = time.monotonic() + 15
        while time.monotonic() < deadline and C102.redis_cli(ctx['store_port'], 'PING') != 'PONG':
            time.sleep(0.2)
        if C102.dbsize(ctx['store_port']) != 0:
            print('owned store is not empty at startup', file=sys.stderr)
            return 2
        doc = new_doc(bins, ctx, harness_sha, c102_sha, c103_sha, c104_sha, bounds, a.hold, a.only)
        # The lane-design controls grade the table, not the subset that happened to run, so a
        # development subset still gets a real answer about covers/ids instead of a false red.
        doc['table_ids'] = [p['id'] for p in full_table]
        doc['table_row_claims'] = {p['id']: list(p['rows']) for p in full_table}
        binaries = {side: info['path'] for side, info in bins.items()}
        for prof in profs:
            doc['profiles'].append(run_entry(ctx, prof, masker, binaries))
            e = doc['profiles'][-1]
            shape = {side: [(r['class'], r.get('ttl_delta'), r.get('url_count'))
                            for r in (e['runs'][side].get('join') or {}).get('rows') or []]
                     for side in ('go', 'rust')}
            print(f"  ran {prof['id']} exits={e['runs']['go']['exit']}/{e['runs']['rust']['exit']} "
                  f"ice={shape}", flush=True)
        doc['redis_final'] = {'plain': C102.dbsize(ctx['store_port'])}
        C102.stop_store(store, 0)
        doc['store_final'] = {'dbsize_after_stop': C102.dbsize(ctx['store_port']),
                              'listeners_after_stop': C102.listeners_on(ctx['store_port'])}
        for key in list(bounds):
            bounds[key]['after'] = C102.listeners_on(int(key))
        result = finish(doc, out=out, live=True)
        (out / 'harness-sha256.txt').write_text(harness_sha + '\n')
        for entry in doc['profiles']:
            for side in ('go', 'rust'):
                log = entry['runs'][side].get('log_masked') or ''
                (out / 'logs' / f"{side}-{entry['profile']}.log").write_text(log)
        (out / 'profiles.json').write_text(json.dumps(
            [{'profile': e['profile'], 'rows': e['rows'], 'claims': e['claims'], 'note': e['note'],
              'expect_exit': e['expect_exit'], 'refuse_wait': e['refuse_wait'], 'ready': e['ready'],
              'ice': e['ice'], 'match': e['match'], 'problems': e['problems'],
              'argv': [r['argv_masked'] for r in (e['runs']['go'], e['runs']['rust'])],
              'join': {side: {k: v for k, v in (e['runs'][side].get('join') or {}).items()
                              if k != 'rows'} for side in ('go', 'rust')},
              'ice_rows': {side: join_rows(e['runs'][side]) for side in ('go', 'rust')},
              'sockets': {side: e['runs'][side].get('sockets_masked') for side in ('go', 'rust')},
              'store_after': {side: (e['runs'][side].get('store') or {}).get('after')
                              for side in ('go', 'rust')}}
             for e in doc['profiles']], indent=2, ensure_ascii=False, sort_keys=True) + '\n')
        print(json.dumps(result['summary'], indent=2))
        return 0 if result['all_ok'] else 1
    finally:
        if doc is not None:
            for entry in doc.get('profiles') or []:
                for side in ('go', 'rust'):
                    log = entry['runs'][side].get('log_masked') or ''
                    if log:
                        (out / 'logs' / f"{side}-{entry['profile']}.log").write_text(log)
        try:
            C102.stop_store(store, 0)
        except Exception:  # noqa: BLE001
            pass
        if C102.listeners_on(ctx['store_port']):
            print('warning: the owned store port still has a listener', file=sys.stderr)


if __name__ == '__main__':
    raise SystemExit(main())
