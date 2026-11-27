#!/usr/bin/env python3
"""C106 limit-gate channels - part 7: CLI, report and the falsification harness.

    --record            launch every profile on both sides, write receipt.json
    --report FILE       recompute all verdicts from the stored cells and print them
    --selftest-gate F   mutate named cells in a copy and require each named target to turn red
    --only a,b          grade a subset (never an acceptance)
"""
from __future__ import annotations

from part1 import *  # noqa: F401,F403
from part3 import leg
from part5 import profiles
from part6 import (EXPECT, PARTS, SCHEMA, SIG_FIXED, grade, leg_map, privacy_sweep, record,
                   run_probe)

LANE = 'tests/interop/limit-gate-channels'


def harness_hashes() -> dict:
    out = {}
    for name in PARTS:
        path = HERE / name
        out[name] = sha256_file(path) if path.is_file() else 'MISSING'
    for label, rel in (('c102', 'base-flag-channels/run-paired.py'),
                       ('c103', 'generated-flag-channels/run-paired.py'),
                       ('c104', 'turn-material-channels/run-paired.py'),
                       ('c105', 'turn-credential-join/run-paired.py')):
        path = INTEROP / rel
        out[label] = sha256_file(path) if path.is_file() else 'MISSING'
    return out


def check_identities(binaries: dict, sides: list) -> list:
    problems = []
    want = {'go': ORACLE_SHA256, 'rust': ACCEPTANCE_SHA256}
    for side in sides:
        path = Path(binaries[side])
        if not path.is_file():
            problems.append(f'{side} binary missing: {path} (rebuild it before recording)')
            continue
        got = sha256_file(path)
        if got != want[side]:
            problems.append(f'{side} sha256 {got[:12]} != pinned {want[side][:12]}')
    if REFERENCE.is_dir():
        head = git_output(REFERENCE, 'rev-parse', 'HEAD')
        if head and head != FIXED_GO_COMMIT:
            problems.append(f'reference checkout {head[:12]} != pinned {FIXED_GO_COMMIT[:12]}')
    else:
        problems.append('references/livekit-stable missing')
    return problems


def git_output(cwd: Path, *args) -> str:
    try:
        return subprocess.run(['git', '-C', str(cwd), *args], capture_output=True, text=True,
                              timeout=20).stdout.strip()
    except Exception:  # noqa: BLE001
        return ''


def third_party_modules() -> list:
    """The lane must run on the stdlib alone: a stray import is a provenance hole."""
    bad = []
    for name in PARTS:
        path = HERE / name
        if not path.is_file():
            continue
        for line in path.read_text(encoding='utf-8').splitlines():
            m = re.match(r'\s*(?:import|from)\s+([A-Za-z_][\w.]*)', line)
            if not m:
                continue
            top = m.group(1).split('.')[0]
            if top in {'part1', 'part2', 'part3', 'part4', 'part5', 'part6', '__future__'}:
                continue
            if top not in sys.stdlib_module_names:
                bad.append(f'{name}: {top}')
    return bad


# ---------------------------------------------------------------------------------- printing
def w(text=''):
    print(text)


def report(doc: dict, args, P=None, work=None) -> int:
    graded = grade(doc)
    subset = bool(args.only or getattr(args, 'profiles', ''))
    w(f'# {BATCH} receipt {doc.get("schema")} recorded {doc.get("recorded_at")}')
    for side in ('go', 'rust'):
        ident = doc['identities'].get(side, {})
        w(f'- {side}: {ident.get("path_alias")} sha256={str(ident.get("sha256"))[:16]}... '
          f'bytes={ident.get("bytes")}')
    w(f'- oracle commit {doc["identities"].get("go", {}).get("source_commit", "")[:12]}  '
      f'heads {doc["identities"].get("heads", {})}')
    w(f'- harness {json.dumps(doc.get("harness", {}), sort_keys=True)[:220]}')
    total_legs = sum(len(p['legs']) for p in graded['profiles'])
    w(f'- profiles {[p["id"] for p in graded["profiles"]]}  graded legs {total_legs}')
    w('')
    diffs = []
    for entry in graded['profiles']:
        prof = next(p for p in doc['profiles'] if p['id'] == entry['id'])
        n_legs = len(entry['legs'])
        w(f'## profile {entry["id"]} (live={entry["live"]}, legs/side={n_legs}, '
          f'limits={json.dumps(prof["limit"], sort_keys=True)})')
        for one in entry['legs']:
            if one['pair'] != 'MATCH':
                diffs.append((entry['id'], one))
                cells = one['cells']
                keys = ', '.join(f"{k}: go={json.dumps((cells.get('go') or {}).get(k))} "
                                 f"rust={json.dumps((cells.get('rust') or {}).get(k))}"
                                 for k in one['grade_keys'] if k in one['diff'])
                w(f'  DIFF {one["id"]} keys={one["grade_keys"]} {keys}')
        if not n_legs:
            w('  NO LEGS RECORDED')
        for gid, fam in sorted(entry['families'].items()):
            if not fam.get('applicable', True):
                w(f'  n/a  family {gid} ({fam["rows"]}) {fam.get("reason", "")}')
                continue
            mark = 'ok  ' if fam['pass'] else 'FAIL'
            det = ' '.join(f"{s}:{v['detail']}" for s, v in sorted(fam['sides'].items()))
            w(f'  {mark} family {gid} ({fam["rows"]}) {det}')
        for one in graded['expectations']:
            if one['profile'] != entry['id']:
                continue
            mark = 'ok  ' if one['pass'] else 'FAIL'
            w(f'  {mark} expect[{one.get("kind", "probe")}] {one["leg"]} {one["side"]} '
              f'want={one["want"]} got={one["got"]} '
              f'{json.dumps(one.get("cell", {}), sort_keys=True)}')
        w('')
    all_side = [e for e in graded['expectations'] if e.get('kind') == 'side']
    if all_side:
        w('## per-side claims (not pairable)')
        for one in all_side:
            w(f'  {"ok  " if one["pass"] else "FAIL"} {one["leg"]} {one["side"]}: '
              f'want={one["want"]} got={one["got"]}')
        w('')
    ungraded = {}
    for entry in graded['profiles']:
        for one in entry['legs']:
            for k, pair in (one.get('other') or {}).items():
                ungraded.setdefault(k, {'legs': 0, 'values': []})
                ungraded[k]['legs'] += 1
                gone = json.dumps(pair[0], ensure_ascii=False)[:60]
                rgot = json.dumps(pair[1], ensure_ascii=False)[:60]
                if [gone, rgot] not in ungraded[k]['values']:
                    ungraded[k]['values'].append([gone, rgot])
    w('## ungraded keys that still differ between the sides')
    w('  (mechanism, not gate outcome: these cells stay out of the pair column on purpose, and the '
      'counts below are the record that they were measured)')
    for k in sorted(ungraded):
        one = ungraded[k]
        pairs = '; '.join(f'go={a} rust={b}' for a, b in one['values'][:6])
        w(f'  {k}: {one["legs"]} legs -> {pairs}')
    if not ungraded:
        w('  none')
    w('')
    w('## controls')
    for c in graded['controls']:
        extra = {k: v for k, v in c.items() if k not in ('id', 'profiles', 'pass')}
        w(f'  {"ok  " if c["pass"] else "FAIL"} {c["id"]}: {json.dumps(extra, sort_keys=True)[:200]}')
        w(f'       {json.dumps(c["profiles"], sort_keys=True)[:400]}')
    w('')
    if graded.get('rows_unexercised'):
        w('## flag channels excluded from profiles that did not carry the flag')
        w('  (a `generated_flags:` row is named only by a profile whose argv put that flag on the '
          'command line; the same legs still prove the YAML channel, so the exclusion is a scoping '
          'fact about the row, not a gap in the pair column)')
        by_row = {}
        for row, pid, leg_id in graded['rows_unexercised']:
            by_row.setdefault(row, []).append((pid, leg_id))
        for row in sorted(by_row):
            held = (graded['rows'].get(row) or {}).get('profiles') or []
            skipped = sorted({pid for pid, _ in by_row[row]})
            first = f'{by_row[row][0][0]}/{by_row[row][0][1]}'
            if held:
                w(f'  {row}: argv carried it in {len(held)} profile(s) {held}; '
                  f'{len(by_row[row])} legs in the other {len(skipped)} profile(s) excluded '
                  f'(first excluded {first})')
            else:
                w(f'  {row}: no profile put that flag on argv (first unseen leg {first})')
        w('')
    w('## rows')
    row_fail = []
    for row, agg in sorted(graded['rows'].items()):
        mark = 'PASS' if agg['pass'] else 'NOT-PASS'
        if not agg['pass']:
            row_fail.append(row)
        w(f'  {mark} {row} profiles={agg["profiles"]}')
        if agg['diffs']:
            for d in agg['diffs']:
                w(f'      diff in {d["profile"]}/{d["leg"]}: '
                  f'{json.dumps(d["diff"], sort_keys=True)[:400]}')
        if agg.get('gates'):
            w(f'      gates: {json.dumps(agg["gates"], sort_keys=True)}')
    w('')
    privacy = []
    if P and work:
        privacy = privacy_sweep(doc, P, work)
    fam_all = [f for e in graded['profiles'] for f in e['families'].values() if f.get('applicable', True)]
    w(f'- graded legs {total_legs}, pair diffs {len(diffs)}, '
      f'families {sum(1 for f in fam_all if f["pass"])}/{len(fam_all)} pass, '
      f'expectations {sum(1 for e in graded["expectations"] if e["pass"])}/'
      f'{len(graded["expectations"])} pass, rows {len(graded["rows"])} '
      f'({len(row_fail)} not passing), privacy violations {len(privacy)}')
    if privacy:
        for v in privacy:
            w(f'  PRIVACY {v}')
    exits = [(r['profile'], r['side'], r['ready'], r['exit']) for r in doc.get('runs', [])]
    w(f'- launches {exits}')
    live = [p for p in graded['profiles'] if p['live']]
    fam_all_ok = all(f['pass'] for e in graded['profiles']
                     for f in e['families'].values() if f.get('applicable', True))
    accepted = (not diffs and not row_fail and not privacy and fam_all_ok
                and all(c['pass'] for c in graded['controls'])
                and all(e['pass'] for e in graded['expectations'])
                and len(live) == 7 and total_legs >= 20 * len(live))
    if subset:
        w('VERDICT: PARTIAL_SUBSET_NOT_ACCEPTANCE')
        return 0
    w(f'VERDICT: {"ACCEPTED" if accepted else "NOT_ACCEPTED"}')
    return 0 if accepted else 1


# --------------------------------------------------------------------------- falsification
def mutate(doc: dict, name: str) -> str:
    """Apply one named corruption to the receipt copy; return what it touched."""
    prof = {p['id']: p for p in doc['profiles']}
    if name == 'body_over_code':
        for side in ('go', 'rust'):
            for one in prof['tight']['sides'][side]['legs']:
                if one['id'] == 'body_over':
                    one['cells']['http_code'] = '200'
        return 'tight/body_over http_code := 200'
    if name == 'metadata_msg_neutral':
        for side in ('go', 'rust'):
            for one in prof['tight']['sides'][side]['legs']:
                if one['id'] == 'update_room_meta_over':
                    one['cells']['msg'] = 'requested room does not exist'
        return 'tight/update_room_meta_over msg := a text the gate never prints'
    if name == 'signal_close_code':
        for one in prof['tight']['sides']['rust']['legs']:
            if one['id'] == 'sig_payload_over':
                one['cells']['close_code'] = 1000
        return 'tight/rust sig_payload_over close_code := 1000'
    if name == 'signal_frame_counting':
        for one in prof['tight']['sides']['rust']['legs']:
            if one['id'] in ('sig_frame_over', 'sig_payload_at'):
                one['cells'] = {'verdict': 'close:1009', 'close_code': 1009, 'close_reason': '',
                                'payload': one['cells'].get('payload'),
                                'frame': one['cells'].get('frame'), 'msgs': 'close:1009'}
        return 'tight/rust: header-counting guard (sig_frame_over and sig_payload_at now refused)'
    if name == 'zeros_body_refuses':
        for one in prof['zeros']['sides']['go']['legs']:
            if one['id'] == 'body_fixed_4k':
                one['cells']['http_code'] = '413'
                one['cells']['text'] = 'request body too large'
        return 'zeros/go body_fixed_4k http_code := 413'
    if name == 'drop_agent_legs':
        for side in ('go', 'rust'):
            prof['tight']['sides'][side]['legs'] = [
                one for one in prof['tight']['sides'][side]['legs']
                if not one['id'].startswith('agent_')]
        return 'tight: every agent_* leg removed'
    if name == 'leak_port':
        prof['tight']['sides']['rust']['legs'][0]['cells']['note_port'] = str(P_LEAK_PORT)
        return 'tight/rust: an unmasked allocated port written into a cell'
    if name == 'startup_agrees_with_rust':
        for one in prof['unsigned-neg']['sides']['rust']['legs']:
            one['cells'] = dict(one['cells'], ready=1, alive=1, self_exited=0,
                                exit_class='ran-and-was-stopped')
        return 'unsigned-neg/rust startup cells := as if the config loaded'
    if name == 'wipe_legs':
        for p in doc['profiles']:
            for side in ('go', 'rust'):
                p['sides'][side]['legs'] = []
        return 'every profile: legs := []'
    if name == 'flips_every_accept':
        for prof in doc['profiles']:
            for side in ('go', 'rust'):
                for one in prof['sides'][side]['legs']:
                    cells = one['cells']
                    if 'http_code' in cells:
                        cells['http_code'] = '413'
                    if 'verdict' in cells:
                        cells['verdict'] = 'close:1009'
                        cells['close_code'] = 1009
                        cells['rr_reason'] = 'LIMIT_EXCEEDED'
                    if cells.get('gate_class') in ('answered', 'other'):
                        # the graded cell for the worker socket is the gate outcome, so the corruption
                        # has to reach it and not only the mechanism text the pair column no longer
                        # compares
                        cells['gate_class'] = 'refused'
                        cells['stream_end'] = 'frame-seen'
                    if 'http' in cells:
                        cells['http'] = '403'
                        cells['closed_after_join'] = 1
                    if cells.get('reg_ok') is not None:
                        cells['reg_ok'] = 0
        return 'every side: each answer rewritten as a refusal'
    if name == 'headers_copied_from_the_other_stack':
        for prof in doc['profiles']:
            for one in prof['sides']['rust']['legs']:
                if 'hdr_case' in one['cells']:
                    one['cells']['hdr_case'] = 'canonical'
        return 'rust: header casing rewritten to the one only Go emits'
    if name == 'mid_bound_never_refuses':
        for x in doc['profiles']:
            for side in ('go', 'rust'):
                for one in x['sides'][side]['legs']:
                    if one['id'] in ('agent_mid_after_ok', 'agent_huge_after_ok'):
                        one['cells']['gate_class'] = 'answered'
        return 'every side: the post-registration worker messages claimed to pass'
    if name == 'agent_class_diverges':
        for one in prof['tight']['sides']['rust']['legs']:
            if one['id'] in ('agent_reg_payload_over', 'agent_mid_after_ok', 'agent_huge_after_ok'):
                one['cells']['gate_class'] = 'answered'
        return 'tight/rust: worker-socket size gates claimed to have answered instead of refusing'
    if name == 'identity_join_never_refuses':
        for x in doc['profiles']:
            for side in ('go', 'rust'):
                for one in x['sides'][side]['legs']:
                    if one['id'] == 'tok_identity_over':
                        one['cells']['http'] = '101'
                        one['cells']['closed_after_join'] = 0
                        one['cells']['has_join'] = 1
        return 'every side: the over-long identity claim asserted to have joined'
    if name == 'identical_cells':
        for p in doc['profiles']:
            base = {one['id']: one['cells'] for one in p['sides']['go']['legs']}
            for one in p['sides']['rust']['legs']:
                if one['id'] in base:
                    one['cells'] = dict(base[one['id']])
        return 'rust cells := go cells for every profile'
    raise KeyError(name)


MUTATIONS = [
    ('body_over_code', 'a body limit that stops refusing fails its family and its claim',
     lambda d: (_leg_pair(d, 'tight', 'body_over') == 'DIFF'
                or _family(d, 'tight', 'body_gate_talks') is False
                or _expect_fail(d, 'tight', 'body_over', 'go'))),
    ('metadata_msg_neutral', 'tight metadata_gate_talks fails (the gate no longer speaks)',
     lambda d: _family(d, 'tight', 'metadata_gate_talks') is False),
    ('signal_close_code', 'tight/rust signal close code no longer matches, gate fails',
     lambda d: (_leg_pair(d, 'tight', 'sig_payload_over') == 'DIFF'
                or _family(d, 'tight', 'signal_gate_talks') is False)),
    ('signal_frame_counting', 'a header-counting side breaks the accept half of the signal family',
     lambda d: _family(d, 'tight', 'signal_gate_talks') is False),
    ('zeros_body_refuses', 'zeros body expectation fails and the control stops flipping',
     lambda d: _expect_fail(d, 'zeros', 'body_fixed_4k', 'go')
     or not all(c['pass'] for c in d['controls'] if 'body_fixed' in c['id'])),
    ('drop_agent_legs', 'deleting the worker-socket legs empties the evidence that claims them',
     lambda d: _family(d, 'tight', 'agent_gate_talks') is False),
    ('leak_port', 'the privacy sweep catches an unmasked port',
     lambda d: bool(d['privacy'])),
    ('startup_agrees_with_rust', 'the startup refusal pair goes DIFF',
     lambda d: _leg_pair(d, 'unsigned-neg', 'startup_refusal') == 'DIFF'),
    ('wipe_legs', 'no legs means no rows, no gates and no acceptance',
     lambda d: (not d['rows']) or all(not r['pass'] for r in d['rows'].values())),
    ('identical_cells', 'copying the oracle cells goes against the per-side claims the lane pins',
     lambda d: not all(e['pass'] for e in d['expectations'] if e['kind'] == 'side')),
    ('flips_every_accept', 'a receipt where nothing is ever accepted fails its own families',
     lambda d: (not all(f['pass'] for e in d['profiles'] for f in e['families'].values()
                        if f.get('applicable', True)))
     and not all(c['pass'] for c in d['controls'])),
    ('mid_bound_never_refuses', 'the worker-socket bound control needs a refusal to discriminate',
     lambda d: not [c for c in d['controls'] if c['id'] == 'agent_mid_sits_on_one_bound'][0]['pass']),
    ('agent_class_diverges', 'the worker-socket gate class is what the agent row is graded on',
     lambda d: _leg_pair(d, 'tight', 'agent_reg_payload_over') == 'DIFF'
     and _family(d, 'tight', 'agent_gate_talks') is False),
    ('identity_join_never_refuses',
     'the identity row\'s gate rests on a join that is actually refused',
     lambda d: _family(d, 'tight', 'identity_gate_talks') is False
     or not (d['rows'].get('YAML:limit.max_participant_identity_length') or {}).get('gate_pass')),
    ('headers_copied_from_the_other_stack',
     'rust cannot be claimed to write go\'s header casing',
     lambda d: not all(e['pass'] for e in d['expectations'] if e['kind'] == 'side'
                       and e['side'] == 'rust')),
]

P_LEAK_PORT = 0


def _leg_pair(graded, pid, leg_id):
    for entry in graded['profiles']:
        if entry['id'] == pid:
            for one in entry['legs']:
                if one['id'] == leg_id:
                    return one['pair']
    return 'ABSENT'


def _family(graded, pid, gid):
    for entry in graded['profiles']:
        if entry['id'] == pid:
            fam = entry['families'].get(gid)
            if fam is None:
                return None
            # not applicable is not a pass: a mutation that removes the legs must read as red
            return fam.get('pass') if fam.get('applicable', True) else False
    return None


def _expect_fail(graded, pid, leg_id, side):
    return any(not e['pass'] for e in graded['expectations']
               if e['profile'] == pid and e['leg'] == leg_id and e['side'] == side)


def selftest(doc: dict, P: dict, work: Path) -> int:
    global P_LEAK_PORT
    P_LEAK_PORT = P['HTTP']
    base = grade(doc)
    base['privacy'] = privacy_sweep(doc, P, work)
    w('# C106 gate falsification')
    w(f'- baseline: profiles {len(base["profiles"])}, legs '
      f'{sum(len(p["legs"]) for p in base["profiles"])}, rows {len(base["rows"])}, '
      f'gates {sum(1 for g in base["gates"] if g["pass"])}/{len(base["gates"])}, '
      f'privacy {len(base["privacy"])}')
    if not base['profiles'] or not base['gates']:
        w('SELFTEST: BASELINE_EMPTY_NOT_A_TEST')
        return 1
    failures = []
    for name, why, probe in MUTATIONS:
        copy_doc = copy.deepcopy(doc)
        touched = mutate(copy_doc, name)
        graded = grade(copy_doc)
        graded['privacy'] = privacy_sweep(copy_doc, P, work)
        try:
            reddened = bool(probe(graded))
        except Exception as exc:  # noqa: BLE001
            reddened = False
            why += f' (probe error {type(exc).__name__})'
        w(f'- {"RED" if reddened else "STILL-GREEN"} {name}: {touched} -> {why}')
        if not reddened:
            failures.append(name)
    w(f'SELFTEST: {"OK" if not failures else "FAILURES " + ",".join(failures)} '
      f'({len(MUTATIONS)} mutations)')
    return 0 if not failures else 1


# --------------------------------------------------------------------------------------- main
def main() -> int:
    ap = argparse.ArgumentParser(description='C106 limit-gate channels paired differential')
    ap.add_argument('--record', action='store_true')
    ap.add_argument('--report', metavar='RECEIPT')
    ap.add_argument('--selftest-gate', metavar='RECEIPT')
    ap.add_argument('--out', default='/tmp/c106-run')
    ap.add_argument('--go', default=str(ORACLE))
    ap.add_argument('--rust', default=str(CANDIDATE))
    ap.add_argument('--profiles', default='')
    ap.add_argument('--sides', default='go,rust')
    ap.add_argument('--only', default='', help='comma list of leg ids (subset: never an acceptance)')
    ap.add_argument('--hold', type=float, default=0.25)
    ap.add_argument('--force', action='store_true')
    args = ap.parse_args()

    work = Path(args.out)
    work.mkdir(parents=True, exist_ok=True)
    used = set()
    P = {'HTTP': alloc_port(used), 'STORE': alloc_port(used)}
    P['roles'] = {P['HTTP']: 'HTTP', P['STORE']: 'STORE'}
    sides = [s for s in args.sides.split(',') if s]
    binaries = {'go': args.go, 'rust': args.rust}
    only_profiles = [p for p in args.profiles.split(',') if p]

    bad = third_party_modules()
    if bad:
        w(f'non-stdlib imports: {bad}')
        return 2
    if args.record:
        problems = check_identities(binaries, sides)
        if problems and not args.force:
            w('identity check failed: ' + '; '.join(problems))
            w('the lane only certifies against the pinned pair; rebuild or pass --force to explore')
            return 2
        store = start_store(P['STORE'])
        try:
            heads = {'rs': git_output(REPO, 'rev-parse', 'HEAD'),
                     'docs': git_output(REPO.parent / 'livekit-docs', 'rev-parse', 'HEAD')}
            doc = record(P, work, binaries, sides, only_profiles, args.hold, harness_hashes(), heads)
            path = work / 'receipt.json'
            path.write_text(json.dumps(doc, indent=1, sort_keys=True, ensure_ascii=False) + '\n',
                            encoding='utf-8')
            w(f'receipt written {path} ({path.stat().st_size} bytes)')
        finally:
            w(f'store stopped: {json.dumps(stop_store(store, P["STORE"]))}')
        args.profiles = ','.join(only_profiles)
        args.only = args.only or ''
        return report(json.loads((work / 'receipt.json').read_text()), args, P, work)
    if args.report:
        doc = json.loads(Path(args.report).read_text())
        return report(doc, args, P, work)
    if args.selftest_gate:
        doc = json.loads(Path(args.selftest_gate).read_text())
        return selftest(doc, P, work)
    ap.print_help()
    return 2


if __name__ == '__main__':
    sys.exit(main())
