

# ------------------------------------------------------------------------------- controls
ALLOWED_ROW_KEYS = {'urls', 'class', 'username_sha256', 'cred_sha256', 'cred_len', 'ttl_delta',
                    'sid_ok', 'kid', 'url_count', 'static_text'}
VERDICT_KEYS = {f'hmac_{k}' for k in ('ext', 'file', 'bin', 'foreign')} | \
               {f'pw_{k}' for k in ('k', 'm', 'x')}


def by_side(entry, side) -> dict:
    return entry['runs'][side]


def join_rows(rec) -> list:
    return (rec.get('join') or {}).get('rows') or []


def all_rows(rec) -> list:
    return [r for j in [rec.get('join') or {}] + list(rec.get('extra_joins') or [])
            for r in (j.get('rows') or [])]


def verdict_cells(rec, prefix: str) -> dict:
    out = {}
    for i, r in enumerate(join_rows(rec)):
        for k, v in r.items():
            if k.startswith(prefix):
                out[f'{i}-{k[len(prefix):]}'] = v
    return out


def first_row(rec):
    rows = join_rows(rec)
    return rows[0] if rows else {}


def profile_map(doc):
    return {e['profile']: e for e in doc['profiles']}


def build_controls(doc, live=False) -> dict:
    C = {}
    entries = doc['profiles']
    sub = bool(doc.get('subset'))

    def graded(value):
        """None means the receipt does not contain the material this control grades. A development
        subset that skipped the profile gets no verdict in either direction, which is honest; the
        full run always grades it."""
        return None if sub else value

    table_ids = doc.get('table_ids')
    rows_by_id = doc.get('table_row_claims')
    if not table_ids or not rows_by_id:
        raise RuntimeError('receipt carries no table_ids/table_row_claims, so the lane-design '
                           'controls cannot be separated from the profiles that happened to run')
    runs = [(e['profile'], s, e['runs'][s]) for e in entries for s in ('go', 'rust')]
    ready = [e for e in entries if e['ready']]
    refusals = [e for e in entries if not e['ready']]
    names = [e['profile'] for e in entries]
    if doc.get('subset'):
        C['profile_count_matches_the_table'] = 0 < len(entries) <= PROFILE_COUNT
    else:
        C['profile_count_matches_the_table'] = (len(entries) == PROFILE_COUNT ==
                                                doc['expected_profile_count'])
    C['every_profile_ran_both_binaries'] = all(set(e['runs']) == {'go', 'rust'} for e in entries)
    C['every_profile_carries_a_ledger_row'] = all(e['rows'] for e in entries)
    C['every_row_is_in_covers'] = all(r in doc['covers'] for e in entries for r in e['rows'])
    C['every_covers_row_is_claimed_by_a_profile'] = all(
        any(row in rows for rows in rows_by_id.values()) for row in doc['covers'])
    C['every_ran_profile_is_in_the_table'] = all(n in table_ids for n in names)
    C['every_profile_has_a_note'] = all(len(e['note']) > 24 for e in entries)
    C['lane_has_positive_controls'] = all(n in table_ids for n in ('no-turn-default-stun',
                                                                  'ts-stun-list-append'))
    stun_controls = ('no-turn-default-stun', 'ts-stun-list-append')
    C['both_default_stun_controls_joined'] = all(
        by_side(e, s)['join']['kind'] == 'joined' for e in entries
        if e['profile'] in stun_controls for s in ('go', 'rust')) if set(stun_controls) & set(
        names) else graded(False)
    C['default_stun_list_is_upstreams_own'] = all(
        first_row(by_side(e, s)).get('urls') == DEFAULT_STUN_URLS
        for e in entries if e['profile'] == 'no-turn-default-stun'
        for s in ('go', 'rust')) if 'no-turn-default-stun' in names else graded(False)
    C['every_join_row_carries_only_digest_fields'] = all(
        set(r) <= (ALLOWED_ROW_KEYS | VERDICT_KEYS)
        for _, _, rec in runs for r in all_rows(rec))
    C['every_row_digests_its_credential'] = all(
        re.fullmatch(r'[0-9a-f]{16}', r['cred_sha256']) and r['cred_len'] is not None
        for _, _, rec in runs for r in all_rows(rec))
    hmac_cells = {e['profile']: {s: verdict_cells(by_side(e, s), 'hmac_')
                                 for s in ('go', 'rust')} for e in entries}
    pw_cells = {e['profile']: {s: verdict_cells(by_side(e, s), 'pw_')
                               for s in ('go', 'rust')} for e in entries}
    matched = [(p, s, c) for p, cells in hmac_cells.items() for s, c in cells.items()
               if any(v == 'match' for v in c.values())]
    C['at_least_one_secret_reproduced_a_credential'] = bool(matched)
    C['every_secret_match_is_discriminated'] = all(
        sum(1 for v in c.values() if v == 'no') >= 1 for _, _, c in matched)
    pw_matched = [(p, s, c) for p, cells in pw_cells.items() for s, c in cells.items()
                  if any(v == 'match' for v in c.values())]
    C['at_least_one_derived_password_reproduced'] = bool(pw_matched)
    C['every_password_match_is_discriminated'] = all(
        sum(1 for v in c.values() if v == 'no') >= 2 for _, _, c in pw_matched)
    C['foreign_secret_never_matched'] = not any(
        v == 'match' for cells in hmac_cells.values() for side in cells.values()
        for k, v in side.items() if k.split('-', 1)[1] == 'foreign')
    def declared_hmac(e, i, name):
        rows = (e['ice'] or {}).get('rows') or []
        return (rows[i].get('hmac') or {}).get(name) if i < len(rows) else None

    C['file_secret_signs_only_the_file_rows'] = all(
        r[f'hmac_{name}'] == ('match' if declared_hmac(e, i, name) == 'match' else 'no')
        for e in ready for s in ('go', 'rust') for i, r in enumerate(join_rows(by_side(e, s)))
        for name in ('ext', 'file', 'bin', 'foreign') if f'hmac_{name}' in r)
    C['sid_ok_never_assented_for_another_participant'] = all(
        r.get('sid_ok') is not False for _, _, rec in runs for r in join_rows(rec))
    buckets = sorted({ttl_minute(r['ttl_delta']) for _, _, rec in runs for r in join_rows(rec)
                      if r.get('ttl_delta') is not None})
    C['ttl_buckets_recorded'] = buckets
    C['ttl_legs_span_five_distinct_lifetimes'] = graded(len(buckets) >= 5)
    url_seen = sorted({u for _, _, rec in runs for r in join_rows(rec) for u in r['urls']})
    C['advertised_urls_are_recorded'] = bool(url_seen)
    C['protocol_urls_are_distinguishable'] = graded(
        len({u for u in url_seen if 'c105.test' in u}) >= 3)
    C['advertised_tls_port_is_443'] = graded(
        'turns:turn.c105.test:443?transport=tcp' in url_seen)
    C['embedded_urls_use_the_loopback_node_ip'] = graded(
        any(u.startswith('turn:127.0.0.1:<TU1>?transport=udp') for u in url_seen))
    C['no_public_stun_leaked_into_a_turn_row'] = not any(
        u.startswith('stun:') and 'turn' in u for u in url_seen)
    static_profiles = ('ts-static-pair', 'ts-empty-secret-with-static', 'ts-protocol-tls-static',
                       'ts-protocol-omitted', 'ts-protocol-unknown-quic')
    statics = [e for e in entries if e['profile'] in static_profiles]
    C['static_rows_are_not_dynamic'] = all(
        r['class'] == 'static' for e in statics for s in ('go', 'rust')
        for r in join_rows(by_side(e, s))) if statics else graded(False)
    C['every_static_profile_returned_a_static_row'] = all(
        join_rows(by_side(e, s)) for e in statics for s in ('go', 'rust')) if statics else \
        graded(False)

    def one(pid):
        return profile_map(doc).get(pid)

    C['static_credential_digest_is_the_yaml_value'] = all(
        first_row(by_side(one('ts-static-pair'), s)).get('cred_sha256') ==
        hashlib.sha256(STATIC_CRED.encode()).hexdigest()[:16]
        for s in ('go', 'rust')) if one('ts-static-pair') else graded(False)
    C['static_username_comes_back_verbatim'] = all(
        first_row(by_side(one('ts-static-pair'), s)).get('static_text') == '<USR>'
        for s in ('go', 'rust')) if one('ts-static-pair') else graded(False)
    two = one('ts-two-servers-order')
    C['two_servers_keep_their_own_secrets_and_order'] = all(
        [r['class'] for r in join_rows(by_side(two, s))] == ['rest', 'rest'] and
        join_rows(by_side(two, s))[0]['hmac_ext'] == 'match' and
        join_rows(by_side(two, s))[1]['hmac_file'] == 'match' and
        join_rows(by_side(two, s))[0]['hmac_file'] == 'no' and
        join_rows(by_side(two, s))[1]['hmac_ext'] == 'no' and
        join_rows(by_side(two, s))[0]['urls'] != join_rows(by_side(two, s))[1]['urls']
        for s in ('go', 'rust')) if two else graded(False)
    def joins_of(rec):
        return [rec.get('join') or {}] + list(rec.get('extra_joins') or [])

    twokey = one('emb-two-keys')
    repeats = (twokey['claims'].get('join_repeat') or 0) if twokey else 0
    C['two_key_probe_repeats_recorded'] = all(
        len(joins_of(by_side(twokey, s))) == repeats for s in ('go', 'rust')) if twokey \
        else graded(False)
    C['every_repeat_password_follows_its_own_username_key'] = all(
        len(joins_of(by_side(twokey, s))) == repeats and
        all((signed := [n for n in ('k', 'm')
                        if ((j.get('rows') or [{}])[0]).get(f'pw_{n}') == 'match']) and
            len(signed) == 1 and KEY_ALIAS.get(((j.get('rows') or [{}])[0]).get('kid')) == signed[0]
            for j in joins_of(by_side(twokey, s)))
        for s in ('go', 'rust')) if twokey else graded(False)
    pinned = {(e['profile'], l['side']): l['actual'] for e in entries for l in e['legs']
              if l['leg'].endswith(':kids-pinned')}
    C['key_selection_divergence_is_measured'] = (
        pinned.get(('emb-two-keys', 'go')) is False and
        pinned.get(('emb-two-keys', 'rust')) is True) if ('emb-two-keys', 'go') in pinned else \
        graded(False)
    binp = one('ts-file-binary-bytes')
    C['binary_secret_file_is_the_key_on_both_sides'] = all(
        first_row(by_side(binp, s)).get('hmac_bin') == 'match' and
        first_row(by_side(binp, s)).get('hmac_file') == 'no'
        for s in ('go', 'rust')) if binp else graded(False)
    tls443 = one('emb-uri-tls-443')
    C['tls_only_join_advertises_443_then_default_stun'] = all(
        [len(join_rows(by_side(tls443, s))),
         [r['class'] for r in join_rows(by_side(tls443, s))]] == [2, ['embedded', 'empty']]
        for s in ('go', 'rust')) if tls443 else graded(False)
    C['refusals_are_only_the_refusal_profiles'] = all(
        (by_side(e, s)['exit'] != 0) == (not e['ready']) for e in entries for s in ('go', 'rust'))
    C['lane_covers_both_modes'] = (len(refusals) >= 5 and len(ready) >= 20) if not sub else None
    C['every_refusal_self_exited_before_the_timeout'] = all(
        by_side(e, s).get('self_exited') for e in refusals for s in ('go', 'rust')) if refusals \
        else graded(False)
    # log_any is a list of acceptable wordings, not a checklist: one refusal sentence may be shared
    # by both binaries and another may be phrased differently on each side. Requiring every
    # alternative to appear is what made a correct refusal read as a missing one.
    C['every_refusal_needle_is_visible_on_at_least_one_side'] = all(
        any(n in by_side(e, s)['log_masked'] for n in (e['claims'].get('log_any') or [])
            for s in ('go', 'rust'))
        for e in refusals) if refusals else graded(False)
    C['every_refusal_left_the_store_untouched'] = all(
        by_side(e, s)['store']['after']['keys'] == [] for e in refusals
        for s in ('go', 'rust')) if refusals else graded(False)
    C['every_start_claimed_a_live_node'] = all('node_alive' in e['claims']
                                                              for e in ready)
    C['store_was_empty_before_every_launch'] = all(rec['store']['before_dbsize'] == 0
                                                   for _, _, rec in runs)
    C['ready_profiles_received_frames'] = all((by_side(e, s)['join']['frames'] or 0) >= 1
                                              for e in ready for s in ('go', 'rust'))
    C['legs_are_uniquely_named'] = len(doc['legs']) == len({l['leg'] for l in doc['legs']})
    C['no_leg_compares_a_missing_value'] = not any(l['kind'] == 'eq' and l.get('expect') is None
                                                   for l in doc['legs'])
    C['no_pair_leg_has_a_missing_side'] = not any(l['kind'] == 'same' and
                                                  any(v is None for v in l['values'])
                                                  for l in doc['legs'])
    C['every_leg_kind_is_rederivable'] = all(
        l['kind'] in ('has', 'hasnt', 'any', 'nonzero', 'eq', 'band', 'same') for l in doc['legs'])
    C['leg_count_is_healthy'] = len(doc['legs']) >= 3 * len(entries)
    C['profile_verdicts_match_the_legs'] = all(
        e['match'] == all(l['ok'] for l in e['legs']) for e in entries)
    C['join_client_is_the_harness_itself'] = (
        doc['join_client']['harness_sha256'] == doc['harness_sha256'] and
        doc['join_client']['third_party_modules'] == [])
    C['sdk_substitution_is_explained'] = len(doc['join_client']['sdk_note']) > 80
    C['both_binaries_are_different_files'] = (doc['binaries']['go']['sha256'] !=
                                              doc['binaries']['rust']['sha256'])
    C['launch_labels_are_unique'] = len({f'{p}-{s}' for p, s, _ in runs}) == len(runs)
    C['every_profile_config_was_written_and_masked'] = all(by_side(e, s).get('cfg_masked')
                                                           for e in entries for s in ('go', 'rust'))
    return C


def sha256_file_now(path: str):
    p = Path(path)
    return C102.sha256_file(p) if p.is_file() else None


def build_provenance_controls(doc, live=False) -> dict:
    bins = doc['binaries']
    C = {}
    now = {side: (sha256_file_now(bins[side].get('path', '')) if bins[side].get('path') else None)
           for side in ('go', 'rust')}
    doc['binary_hash_now'] = now
    C['binaries_unchanged'] = all(now[s] is not None and now[s] == bins[s]['sha256']
                                 for s in ('go', 'rust'))
    bins['go']['is_fixed_oracle'] = now['go'] == ORACLE_SHA256
    bins['rust']['is_acceptance_candidate'] = now['rust'] == ACCEPTANCE_SHA256
    C['go_is_fixed_oracle'] = bool(bins['go']['is_fixed_oracle'])
    C['rust_is_acceptance_candidate'] = bool(bins['rust']['is_acceptance_candidate'])
    C['recorded_oracle_matches_pinned'] = bins['go']['sha256'] == ORACLE_SHA256
    C['recorded_candidate_matches_pinned'] = bins['rust']['sha256'] == ACCEPTANCE_SHA256
    C['reference_commit_matches'] = doc['reference']['go_checkout_commit'] == FIXED_GO_COMMIT
    C['imported_primitives_are_the_accepted_lanes'] = (
        doc['c102_harness_sha256'] == C102_FILED_HARNESS_SHA and
        doc['c103_harness_sha256'] == C103_FILED_HARNESS_SHA and
        doc['c104_harness_sha256'] == C104_FILED_HARNESS_SHA)
    harness_now = C102.sha256_file(Path(__file__).resolve())
    doc['harness_sha256_now'] = harness_now
    C['harness_unchanged_during_run'] = (
        harness_now == doc['harness_sha256_at_start'] if live
        else doc['harness_sha256'] == doc['harness_sha256_at_start'])
    C['harness_unchanged_since_run'] = doc['harness_sha256'] == harness_now
    blob = json.dumps(doc['profiles'], ensure_ascii=False, sort_keys=True)
    whole = json.dumps(doc, ensure_ascii=False, sort_keys=True)
    C['off_limits_never_named'] = not re.search(r'\b788[12]\b', blob)
    C['host_store_never_named'] = ':6379' not in blob
    C['no_machine_address_in_receipt'] = not C102.forbidden_addresses(whole)
    C['no_unmasked_loopback_ports'] = not re.search(r'127\.0\.0\.1:\d', blob)
    C['no_allocated_port_leaks_into_urls'] = not any(
        f':{port}' in blob for port in doc['ports'].values())
    C['no_working_paths_in_profile_records'] = not any(
        needle in blob for needle in (str(REPO), str(Path.home()),
                                      str(Path(doc.get('work_dir') or '\x00')), '/private/tmp/'))
    C['fixture_secrets_never_raw'] = not any(real in whole for real, _ in
                                            SECRET_ALIASES + STATIC_ALIASES)
    C['binary_secret_file_bytes_never_copied_in'] = b'BINc105' not in whole.encode('utf-8',
                                                                                  'surrogateescape')
    C['participant_sids_are_tokenised'] = not re.search(r'\bPA_[0-9A-Za-z]{6,16}\b', whole)
    C['argv_never_raw'] = not any('argv_raw' in r or ('argv' in r and 'argv_masked' not in r)
                                  for _, _, r in runs_of(doc))
    C['raw_streams_recorded'] = all(
        re.fullmatch(r'[0-9a-f]{64}', r['raw']['stdout_sha256']) and r['raw']['bytes'] > 0
        for _, _, r in runs_of(doc))
    C['every_launch_produced_log_output'] = all(r['raw']['lines'] >= 1 for _, _, r in runs_of(doc))
    bounds = doc.get('listeners_bounds') or {}
    C['off_limit_ports_untouched'] = bool(bounds) and all(
        v['before'] == v['after'] for v in bounds.values())
    C['owned_store_stopped_cleanly'] = bool(doc.get('store_final')) and \
        doc['store_final'].get('listeners_after_stop') == 0
    C['store_count_recorded'] = doc.get('redis_final', {}).get('plain') is not None
    return C


def runs_of(doc):
    return [(e['profile'], side, e['runs'][side]) for e in doc['profiles']
            for side in ('go', 'rust')]


# ------------------------------------------------------------------------------- verdict
def finish(doc, out=None, live=False, write=True) -> dict:
    for entry in doc['profiles']:
        legs = evaluate(entry)
        for item in legs:
            item['ok'] = rederive_leg(item)
        entry['legs'] = legs
        entry['problems'] = sorted(l['leg'] for l in legs if not l['ok'])
        entry['match'] = not entry['problems']
    doc['legs'] = [l for e in doc['profiles'] for l in e['legs']]
    doc['leg_count'] = len(doc['legs'])
    verdicts = {l['leg']: l['ok'] for l in doc['legs']}
    if live or 'leg_verdicts_at_record' not in doc:
        doc['leg_verdicts_at_record'] = verdicts
    controls = build_controls(doc, live=live)
    controls.update(build_provenance_controls(doc, live=live))
    controls['verdict_stable_across_rederivation'] = (verdicts == doc['leg_verdicts_at_record'])
    doc['cross_profile_controls'] = controls
    doc['failed_profiles'] = sorted(e['profile'] for e in doc['profiles'] if not e['match'])
    doc['failed_legs'] = sorted(l['leg'] for l in doc['legs'] if not l['ok'])
    # None is "this receipt holds no material for that claim", which a development subset produces
    # whenever it skips the driving profile. Counting it as a failure would hide the real reds;
    # counting it as a pass would be vacuous. It is listed instead.
    doc['failed_controls'] = sorted(k for k, v in controls.items() if v is False)
    doc['unassessed_controls'] = sorted(k for k, v in controls.items() if v is None)
    doc['row_profile_map'] = {row: sorted(e['profile'] for e in doc['profiles']
                                          if row in e['rows']) for row in doc['covers']}
    doc['status_gates'] = {
        'binaries_unchanged': controls['binaries_unchanged'],
        'go_is_fixed_oracle': controls['go_is_fixed_oracle'],
        'rust_is_acceptance_candidate': controls['rust_is_acceptance_candidate'],
        'reference_commit_matches': controls['reference_commit_matches'],
        'imported_primitives_are_the_accepted_lanes':
            controls['imported_primitives_are_the_accepted_lanes'],
        'harness_unchanged_during_run': controls['harness_unchanged_during_run'],
        'harness_unchanged_since_run': controls['harness_unchanged_since_run'],
        'join_client_is_the_harness_itself': controls['join_client_is_the_harness_itself'],
        'every_secret_match_is_discriminated': controls['every_secret_match_is_discriminated'],
        'rows_store_digests_only': controls['every_join_row_carries_only_digest_fields'],
        'lane_covers_both_modes': controls['lane_covers_both_modes'],
        'no_failed_profiles': not doc['failed_profiles'],
        'no_failed_controls': not doc['failed_controls'],
        'no_failed_legs': not doc['failed_legs'],
    }
    doc['unmet_gates'] = sorted(k for k, v in doc['status_gates'].items() if not v)
    doc['live_run'] = live
    doc['status'] = ('PARTIAL_SUBSET_NOT_ACCEPTANCE' if doc.get('subset')
                     else STATUS_PASS if not doc['unmet_gates'] else 'FAIL')
    if write and out is not None:
        (Path(out) / 'execution.json').write_text(
            json.dumps(doc, indent=2, ensure_ascii=False, sort_keys=True) + '\n')
        batch = {'status': doc['status'], 'batch': doc['batch'], 'covers': doc['covers'],
                 'profiles': len(doc['profiles']), 'legs': doc['leg_count'],
                 'controls': len(controls), 'failed_profiles': doc['failed_profiles'],
                 'failed_legs': doc['failed_legs'], 'failed_controls': doc['failed_controls'],
                 'unassessed_controls': doc['unassessed_controls'],
                 'divergences': doc.get('divergences') or [],
                 'unmet_gates': doc['unmet_gates'], 'status_gates': doc['status_gates'],
                 'row_profile_map': doc['row_profile_map'],
                 'ttl_buckets': controls.get('ttl_buckets_recorded'),
                 'advertised_urls': sorted({u for _, _, r in runs_of(doc)
                                            for row in join_rows(r) for u in row['urls']}),
                 'profile_verdicts': {
                     e['profile']: {'match': e['match'], 'rows': e['rows'], 'ready': e['ready'],
                                    'go_exit': e['runs']['go']['exit'],
                                    'rust_exit': e['runs']['rust']['exit'],
                                    'ice_shape': {
                                        side: [{k: v for k, v in row.items()
                                                 if k in ('class', 'ttl_delta', 'cred_len',
                                                          'static_text', 'kid', 'urls')}
                                                for row in join_rows(e['runs'][side])]
                                        for side in ('go', 'rust')},
                                    'problems': e['problems']}
                     for e in doc['profiles']},
                 'control_values': controls, 'binary_hash_now': doc['binary_hash_now'],
                 'harness_sha256_now': doc['harness_sha256_now'], 'binaries': doc['binaries'],
                 'reference': doc['reference'], 'harness_sha256': doc['harness_sha256'],
                 'c102_harness_sha256': doc['c102_harness_sha256'],
                 'c103_harness_sha256': doc['c103_harness_sha256'],
                 'c104_harness_sha256': doc['c104_harness_sha256']}
        (Path(out) / 'BATCH.json').write_text(json.dumps(batch, indent=2, ensure_ascii=False,
                                                        sort_keys=True) + '\n')
    return {'all_ok': not doc['unmet_gates'],
            'summary': {k: doc[k] for k in ('status', 'leg_count', 'failed_profiles',
                                            'failed_controls', 'failed_legs', 'unmet_gates')}}
