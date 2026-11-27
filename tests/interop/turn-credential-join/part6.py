

# ------------------------------------------------------------------------------- falsification
# Each case is a plausible misrecord applied to a deep copy of the filed receipt. A case that changes
# nothing, or that names a leg the receipt does not contain, is reported as undiscriminated rather
# than quietly passing.
def _r(doc, pid, side):
    for e in doc['profiles']:
        if e['profile'] == pid:
            return e['runs'][side]
    raise KeyError(pid)


def _j(doc, pid, side) -> dict:
    return _r(doc, pid, side).setdefault('join', {})


def _row(doc, pid, side, i=0) -> dict:
    rows = _j(doc, pid, side).setdefault('rows', [])
    while len(rows) <= i:
        rows.append({})
    return rows[i]


def _all_joins(doc, pid, side) -> list:
    """The primary join and its repeats. rec['join'] and rec['extra_joins'] are separate objects once
    the receipt has been through JSON, so a mutation has to walk both to be seen everywhere."""
    rec = _r(doc, pid, side)
    return [rec.setdefault('join', {})] + list(rec.setdefault('extra_joins', []))


def _socks(rec, fam):
    return rec.setdefault('sockets_masked', {}).setdefault(fam, [])


GATE_INJECTIONS = (
    # ------------------------------------------------------------- the secret -> credential channel
    {'case': 'inline_secret_never_used', 'expect': 'FAILED_LEG',
     'target': 'ts-secret-inline::go:ice0-hmac-ext',
     'apply': lambda d: _row(d, 'ts-secret-inline', 'go').update({'hmac_ext': 'no'}),
     'why': 'the row claims the credential reproduces from the configured secret; if it does not, the '
            'secret row has no evidence at all'},
    {'case': 'foreign_secret_signs_the_credential', 'expect': 'FAILED_CONTROL',
     'target': 'foreign_secret_never_matched',
     'apply': lambda d: _row(d, 'ts-secret-inline', 'go').update({'hmac_foreign': 'match'}),
     'why': 'a secret that nobody configured must not be able to produce the wire credential, or the '
            'match leg means nothing'},
    {'case': 'file_secret_signs_an_inline_row', 'expect': 'FAILED_CONTROL',
     'target': 'file_secret_signs_only_the_file_rows',
     'apply': lambda d: _row(d, 'ts-secret-inline', 'go').update({'hmac_file': 'match'}),
     'why': 'the file fixture is configured in three profiles only; a fourth match means the digest '
            'check is not tied to the config that named it'},
    {'case': 'password_derived_from_another_key', 'expect': 'FAILED_LEG',
     'target': 'emb-two-keys::rust:ice0-kid-signs',
     'apply': lambda d: _row(d, 'emb-two-keys', 'rust').update({'kid': KID}),
     'why': 'the key-map row is only proven if the password follows the key the username names'},
    {'case': 'go_key_choice_actually_stable', 'expect': 'FAILED_LEG',
     'target': 'emb-two-keys::go:kids-pinned',
     'apply': lambda d: [j['rows'][0].update({'kid': KID2}) for j in _all_joins(d, 'emb-two-keys', 'go')
                         if j.get('rows')],
     'why': 'the claim about Go is that the advertised key is not pinned; if every join named the '
            'same one, the divergence being recorded would not be there'},
    {'case': 'rust_key_choice_wanders', 'expect': 'FAILED_LEG',
     'target': 'emb-two-keys::rust:kids-pinned',
     'apply': lambda d: _all_joins(d, 'emb-two-keys', 'rust')[1]['rows'][0].update({'kid': KID}),
     'why': 'Rust is claimed to name the token key on every join of one launch'},
    {'case': 'a_repeat_returned_no_row', 'expect': 'FAILED_LEG',
     'target': 'emb-two-keys::rust:kids-count',
     'apply': lambda d: (_r(d, 'emb-two-keys', 'rust')['extra_joins'].pop()
                         if len(_r(d, 'emb-two-keys', 'rust').get('extra_joins') or []) > 1 else None),
     'why': 'a spread claim measured over one join is not a spread claim'},
    {'case': 'username_names_an_unconfigured_key', 'expect': 'FAILED_LEG',
     'target': 'emb-two-keys::go:kids-known',
     'apply': lambda d: _all_joins(d, 'emb-two-keys', 'go')[0]['rows'][0]
                         .update({'kid': 'c105nope'}),
     'why': 'a credential signed by a key nobody configured would make the key-map claim meaningless'},
    {'case': 'another_participants_sid_accepted', 'expect': 'FAILED_CONTROL',
     'target': 'sid_ok_never_assented_for_another_participant',
     'apply': lambda d: _row(d, 'ts-secret-inline', 'rust').update({'sid_ok': False}),
     'why': 'if a credential can carry someone else SID, the join proves nothing about this one'},
    {'case': 'static_pair_treated_as_dynamic', 'expect': 'FAILED_LEG',
     'target': 'ts-static-pair::go:ice0-class',
     'apply': lambda d: _row(d, 'ts-static-pair', 'go').update({'class': 'rest', 'static_text': None}),
     'why': 'the username/credential row is about pass-through; an HMAC-shaped username is a different '
            'mode'},
    {'case': 'blank_secret_treated_as_set', 'expect': 'FAILED_LEG',
     'target': 'ts-empty-secret-with-static::rust:ice0-class',
     'apply': lambda d: _row(d, 'ts-empty-secret-with-static', 'rust').update(
         {'class': 'rest', 'static_text': None}),
     'why': 'secret: "" must stay blank; if one side reads it as Some(""), the static pair is replaced '
            'by an HMAC over an empty key'},
    {'case': 'binary_secret_file_not_the_key', 'expect': 'FAILED_LEG',
     'target': 'ts-file-binary-bytes::go:ice0-hmac-bin',
     'apply': lambda d: _row(d, 'ts-file-binary-bytes', 'go').update({'hmac_bin': 'no'}),
     'why': 'this row is the non-UTF-8 file case; a miss means the bytes were transcoded, not used'},
    {'case': 'two_servers_order_swapped', 'expect': 'FAILED_LEG',
     'target': 'ts-two-servers-order::go:ice0-hmac-ext',
     'apply': lambda d: _j(d, 'ts-two-servers-order', 'go')['rows'].reverse(),
     'why': 'the list order is the wire order and each row carries its own secret'},
    {'case': 'secret_file_bytes_copied_into_receipt', 'expect': 'FAILED_CONTROL',
     'target': 'binary_secret_file_bytes_never_copied_in',
     'apply': lambda d: _r(d, 'ts-file-binary-bytes', 'go')['log_tail_masked'].append('BINc105'),
     'why': 'the file content is a secret fixture; only its digest may be stored'},
    {'case': 'raw_credential_stored', 'expect': 'FAILED_CONTROL',
     'target': 'every_join_row_carries_only_digest_fields',
     'apply': lambda d: _row(d, 'ts-secret-inline', 'go').update({'credential': 'AAAAAAAAAAAAAAAAAAAAAAAAAAAA='}),
     'why': 'a stored credential would put a usable TURN password in the committed evidence'},
    {'case': 'participant_sid_stored_raw', 'expect': 'FAILED_CONTROL',
     'target': 'participant_sids_are_tokenised',
     'apply': lambda d: _row(d, 'ts-secret-inline', 'go').update({'kid': 'PA_abcdefghijklmnop'}),
     'why': 'SIDs are per-connection identifiers and must not reach the receipt'},
    {'case': 'machine_address_in_an_advertised_url', 'expect': 'FAILED_CONTROL',
     'target': 'no_machine_address_in_receipt',
     'apply': lambda d: _row(d, 'emb-ttl-120', 'rust').update(
         {'urls': ['turn:10.113.4.21:<TU1>?transport=udp']}),
     'why': 'the node IP is the one thing a committed receipt must never carry'},
    {'case': 'allocated_port_left_unmasked', 'expect': 'FAILED_CONTROL',
     'target': 'no_allocated_port_leaks_into_urls',
     'apply': lambda d: _row(d, 'emb-ttl-default', 'go').update(
         {'urls': [f'turn:127.0.0.1:{d["ports"]["http"]}?transport=udp']}),
     'why': 'a bare port number in the receipt cannot be tied back to a role and varies per run'},

    # ------------------------------------------------------------------------- the ttl channel
    {'case': 'configured_ttl_ignored', 'expect': 'FAILED_LEG',
     'target': 'emb-ttl-120::go:ice0-ttl',
     'apply': lambda d: _row(d, 'emb-ttl-120', 'go').update({'ttl_delta': 3600}),
     'why': 'the ttl_seconds row is only about the lifetime the credential actually has'},
    {'case': 'flag_ttl_loses_to_yaml', 'expect': 'FAILED_LEG',
     'target': 'emb-ttl-flag-beats-yaml::rust:ice0-ttl',
     'apply': lambda d: _row(d, 'emb-ttl-flag-beats-yaml', 'rust').update({'ttl_delta': 120}),
     'why': 'this is the launched consumer for the generated flag; the YAML value means the flag lost'},
    {'case': 'env_ttl_loses_to_yaml', 'expect': 'FAILED_LEG',
     'target': 'emb-ttl-env-beats-yaml::go:ice0-ttl',
     'apply': lambda d: _row(d, 'emb-ttl-env-beats-yaml', 'go').update({'ttl_delta': 120}),
     'why': 'the env spelling is claimed as a separate channel and has to fail separately'},
    {'case': 'external_default_ttl_wrong', 'expect': 'FAILED_LEG',
     'target': 'ts-ttl-zero-explicit::go:ice0-ttl',
     'apply': lambda d: _row(d, 'ts-ttl-zero-explicit', 'go').update({'ttl_delta': 300}),
     'why': 'a zero external ttl means 14400, not the embedded 300 - the two defaults are the claim'},
    {'case': 'over_max_ttl_not_capped', 'expect': 'FAILED_LEG',
     'target': 'ts-ttl-over-max::rust:ice0-ttl',
     'apply': lambda d: _row(d, 'ts-ttl-over-max', 'rust').update({'ttl_delta': 999999}),
     'why': '999999 s must arrive as 86400 s, which is the cap and not the configured number'},
    {'case': 'ttl_legs_all_the_same_value', 'expect': 'FAILED_CONTROL',
     'target': 'ttl_legs_span_five_distinct_lifetimes',
     'apply': lambda d: [row.update({'ttl_delta': 300}) for _, _, rec in runs_of(d)
                         for row in join_rows(rec)],
     'why': 'if every row expired at the same second, the five ttl profiles would be one profile'},

    # --------------------------------------------------------------------- the URL shape channel
    {'case': 'tls_scheme_lost', 'expect': 'FAILED_LEG',
     'target': 'ts-protocol-tls-static::rust:ice0-url',
     'apply': lambda d: _row(d, 'ts-protocol-tls-static', 'rust').update(
         {'urls': [f'turn:{HOST}:3478?transport=tcp']}),
     'why': 'protocol tls is the only spelling that changes the scheme'},
    {'case': 'omitted_protocol_becomes_udp', 'expect': 'FAILED_LEG',
     'target': 'ts-protocol-omitted::go:ice0-url',
     'apply': lambda d: _row(d, 'ts-protocol-omitted', 'go').update(
         {'urls': [f'turn:{HOST}:3478?transport=udp']}),
     'why': 'the zero value is the fall-through turn/tcp; reading it as udp is a different default'},
    {'case': 'unknown_protocol_produces_no_row', 'expect': 'FAILED_LEG',
     'target': 'ts-protocol-unknown-quic::go:ice-count',
     'apply': lambda d: _j(d, 'ts-protocol-unknown-quic', 'go')['rows'].clear(),
     'why': 'an unlisted protocol must still advertise a row; if it vanishes, the fall-through is being '
            'asserted by absence rather than by a URL'},
    {'case': 'advertised_443_becomes_the_listener', 'expect': 'FAILED_LEG',
     'target': 'emb-uri-tls-443::ice0-urls',
     'apply': lambda d: _row(d, 'emb-uri-tls-443', 'rust').update(
         {'urls': [f'turns:{DOMAIN}:<TLS1>?transport=tcp']}),
     'why': 'the URL says 443 while the socket is on <TLS1>; flattening the two erases the quirk'},
    {'case': 'tls_only_advertises_no_stun_fallback', 'expect': 'FAILED_LEG',
     'target': 'emb-uri-tls-443::rust:ice-count',
     'apply': lambda d: _j(d, 'emb-uri-tls-443', 'rust')['rows'].__delitem__(slice(1, 2)),
     'why': 'hasSTUN stays false when only the TLS branch runs, so the default STUN row must follow'},
    {'case': 'default_stun_list_shortened', 'expect': 'FAILED_CONTROL',
     'target': 'default_stun_list_is_upstreams_own',
     'apply': lambda d: _row(d, 'no-turn-default-stun', 'go').update(
         {'urls': DEFAULT_STUN_URLS[:2], 'url_count': 2}),
     'why': 'the control is only a control if it returns the list upstream actually ships'},
    {'case': 'enabled_false_still_advertises_embedded', 'expect': 'FAILED_LEG',
     'target': 'emb-disabled-with-ts::go:ice-count',
     'apply': lambda d: _j(d, 'emb-disabled-with-ts', 'go')['rows'].append(dict(
         _row(d, 'emb-disabled-with-ts', 'go'))),
     'why': 'a disabled block must contribute no row of its own'},
    {'case': 'node_ip_missing_from_embedded_url', 'expect': 'FAILED_CONTROL',
     'target': 'embedded_urls_use_the_loopback_node_ip',
     'apply': lambda d: [row.update({'urls': [u.replace('127.0.0.1', 'other.c105.test')
                                              for u in row['urls']]})
                         for _, _, rec in runs_of(d) for row in join_rows(rec)],
     'why': 'rtc.node_ip is what puts an addressable host in the embedded URL; losing it makes the '
            'row unparseable by the client'},

    # ------------------------------------------------------------------------- the launch itself
    {'case': 'refusal_actually_started', 'expect': 'FAILED_LEG',
     'target': 'ts-file-empty-refused::rust:exit',
     'apply': lambda d: _r(d, 'ts-file-empty-refused', 'rust').update({'exit': 0}),
     'why': 'the refusal rows are claims about the raw exit code'},
    {'case': 'refusal_needle_absent_everywhere', 'expect': 'FAILED_LEG',
     'target': 'ts-username-only-refused::go:any',
     'apply': lambda d: _r(d, 'ts-username-only-refused', 'go').update({'log_masked': 'startup'}),
     'why': 'a refusal that names no reason is indistinguishable from some other failure'},
    {'case': 'precedence_warning_missing', 'expect': 'FAILED_LEG',
     'target': f'ts-secret-beats-file::go:log_all-{needle_slug(SECRET_PRECEDENCE)}',
     'apply': lambda d: _r(d, 'ts-secret-beats-file', 'go').update({'log_masked': ''}),
     'why': 'the precedence branch is claimed in the log as well as in the digest'},
    {'case': 'refusal_wrote_the_store', 'expect': 'FAILED_LEG',
     'target': 'ts-no-credentials-refused::go:store-clean',
     'apply': lambda d: _r(d, 'ts-no-credentials-refused', 'go')['store']['after'].update(
         {'keys': ['livekit_version']}),
     'why': 'a start that fails after touching the store leaves state the next launch inherits'},
    {'case': 'store_not_flushed_before_launch', 'expect': 'FAILED_CONTROL',
     'target': 'store_was_empty_before_every_launch',
     'apply': lambda d: _r(d, 'ts-static-pair', 'go')['store'].update({'before_dbsize': 4}),
     'why': 'a launch that inherited rows cannot attribute them to itself'},
    {'case': 'tls_listener_not_held', 'expect': 'FAILED_LEG',
     'target': 'emb-uri-tls-443::go:tcp-TLS1',
     'apply': lambda d: (_socks(_r(d, 'emb-uri-tls-443', 'go'), 'tcp').remove('<TLS1>')
                         if '<TLS1>' in _socks(_r(d, 'emb-uri-tls-443', 'go'), 'tcp') else None),
     'why': 'the 443 row is about a TLS listener that exists'},
    {'case': 'omitted_udp_port_bound_anyway', 'expect': 'FAILED_LEG',
     'target': 'emb-uri-tls-443::rust:not-udp-TU1',
     'apply': lambda d: _socks(_r(d, 'emb-uri-tls-443', 'rust'), 'udp').append('<TU1>'),
     'why': 'an absent udp_port must bind nothing; otherwise the two-URL shape is an accident'},
    {'case': 'no_join_frame_but_rows_claimed', 'expect': 'FAILED_LEG',
     'target': 'ts-secret-inline::rust:ice-count',
     'apply': lambda d: _j(d, 'ts-secret-inline', 'rust').update({'rows': [], 'kind': 'joined'}),
     'why': 'the count leg is what stops an empty stream reading as a pass'},
    {'case': 'join_reported_without_frames', 'expect': 'FAILED_CONTROL',
     'target': 'ready_profiles_received_frames',
     'apply': lambda d: [_j(d, e['profile'], 'go').update({'frames': 0}) for e in d['profiles']
                         if e['ready']],
     'why': 'a profile that claims a joined node but received no frame never joined'},
    {'case': 'upgrade_never_switched', 'expect': 'FAILED_LEG',
     'target': 'no-turn-default-stun::go:upgrade',
     'apply': lambda d: _j(d, 'no-turn-default-stun', 'go').update({'status': '400 Bad Request'}),
     'why': 'the RFC 6455 handshake returning 400 means the probe read nothing from a session'},
    {'case': 'room_sid_never_returned', 'expect': 'FAILED_LEG',
     'target': 'ts-ttl-600::rust:room-sid',
     'apply': lambda d: _j(d, 'ts-ttl-600', 'rust').update({'room': None}),
     'why': 'a response with no room is not a join'},
    {'case': 'refusal_survived_the_timeout', 'expect': 'FAILED_CONTROL',
     'target': 'every_refusal_self_exited_before_the_timeout',
     'apply': lambda d: _r(d, 'ts-file-missing-refused', 'go').update({'self_exited': False}),
     'why': 'a process the harness had to signal is not a startup refusal'},
    {'case': 'off_limit_port_touched', 'expect': 'FAILED_CONTROL',
     'target': 'off_limit_ports_untouched',
     'apply': lambda d: d['listeners_bounds'][str(list(d['listeners_bounds'])[0])].update(
         {'after': 1}),
     'why': 'the user-owned listeners must be exactly as found'},
    {'case': 'raw_stream_empty', 'expect': 'FAILED_CONTROL',
     'target': 'raw_streams_recorded',
     'apply': lambda d: _r(d, 'emb-ttl-120', 'go')['raw'].update({'bytes': 0}),
     'why': 'a launch with no recorded bytes cannot be re-examined later'},
    {'case': 'working_path_in_profile_record', 'expect': 'FAILED_CONTROL',
     'target': 'no_working_paths_in_profile_records',
     'apply': lambda d: _r(d, 'emb-ttl-120', 'rust').update(
         {'cfg_masked': f'secret_file: {REPO}/x.yaml'}),
     'why': 'committed evidence must not name the machine layout'},

    # ------------------------------------------------------------------------- the harness itself
    {'case': 'oracle_identity_replaced', 'expect': 'FAILED_CONTROL',
     'target': 'recorded_oracle_matches_pinned',
     'apply': lambda d: d['binaries']['go'].update({'sha256': 'f' * 64}),
     'why': 'the fixed Go e5655ac image is the differential reference'},
    {'case': 'reference_commit_drift', 'expect': 'FAILED_CONTROL',
     'target': 'reference_commit_matches',
     'apply': lambda d: d['reference'].update({'go_checkout_commit': 'e5655ac20267fa5005dccaf3f0e3bfc'
                                                                    '3ea688c06'[:-4] + 'dead'}),
     'why': 'the upstream anchors are only valid for the commit they were read from'},
    {'case': 'imported_primitive_swapped', 'expect': 'FAILED_CONTROL',
     'target': 'imported_primitives_are_the_accepted_lanes',
     'apply': lambda d: d.update({'c104_harness_sha256': '0' * 64}),
     'why': 'the launcher, masker and probes are accepted-lane code; a different file is a different '
            'harness'},
    {'case': 'harness_rewritten_after_record', 'expect': 'FAILED_CONTROL',
     'target': 'harness_unchanged_since_run',
     'apply': lambda d: d.update({'harness_sha256': '1' * 64}),
     'why': 'the legs in this receipt were derived by the bytes the record names'},
    {'case': 'profile_table_silently_short', 'expect': 'FAILED_CONTROL',
     'target': 'profile_count_matches_the_table',
     'apply': lambda d: d['profiles'].__delitem__(slice(0, 1)),
     'why': 'a run that dropped a profile is not the run the file describes'},
    {'case': 'row_claim_dropped', 'expect': 'FAILED_CONTROL',
     'target': 'every_profile_carries_a_ledger_row',
     'apply': lambda d: d['profiles'][0].update({'rows': []}),
     'why': 'a profile that maps to no ledger row cannot move evidence'},
    {'case': 'covers_row_left_unclaimed', 'expect': 'FAILED_CONTROL',
     'target': 'every_covers_row_is_claimed_by_a_profile',
     'apply': lambda d: [rows.remove('YAML:turn.ttl_seconds')
                         for rows in d['table_row_claims'].values()
                         if 'YAML:turn.ttl_seconds' in rows],
     'why': 'the batch advertises the ttl rows; nothing may quietly stop covering them'},
    {'case': 'third_party_client_claimed', 'expect': 'FAILED_CONTROL',
     'target': 'join_client_is_the_harness_itself',
     'apply': lambda d: d['join_client'].update({'third_party_modules': ['websockets']}),
     'why': 'if the probe needed a library, the wire format would be that library s claim, not ours'},
    {'case': 'unknown_claim_key_silently_ignored', 'expect': 'HARNESS_REFUSAL',
     'target': 'CLAIM_KEYS',
     'apply': lambda d: d['profiles'][0]['claims'].update({'turns_up_a_bell': True}),
     'why': 'a claim nothing evaluates is a pass that asserts nothing'},
    {'case': 'unknown_row_expectation_field', 'expect': 'HARNESS_REFUSAL',
     'target': 'ROW_KEYS',
     'apply': lambda d: d['profiles'][0]['ice']['rows'][0].update({'not_a_field': 1}),
     'why': 'an expectation naming a field evaluate ignores makes a leg disappear silently'},
    {'case': 'pair_leg_with_a_missing_side', 'expect': 'FAILED_CONTROL',
     'target': 'no_pair_leg_has_a_missing_side',
     'apply': lambda d: _row(d, 'ts-secret-inline', 'rust').update({'cred_len': None}),
     'why': 'a cross-binary comparison where one side is absent is not a comparison'},
)

STALE_VERDICT_FIELDS = ('legs', 'leg_count', 'failed_profiles', 'failed_legs', 'failed_controls',
                        'cross_profile_controls', 'status', 'unmet_gates', 'status_gates',
                        'row_profile_map', 'live_run', 'binary_hash_now', 'harness_sha256_now')


def _clean(doc):
    for key in STALE_VERDICT_FIELDS:
        doc.pop(key, None)
    for e in doc['profiles']:
        for key in ('legs', 'problems', 'match'):
            e.pop(key, None)
    return doc


def selftest_gate(doc, out, receipt=None):
    """Apply each named mutation to a deep copy of a real receipt and require the gate to redden."""
    import copy as _copy
    out = Path(out)
    out.mkdir(parents=True, exist_ok=True)
    before = C102.sha256_file(Path(receipt)) if receipt else None
    baseline = _clean(_copy.deepcopy(doc))
    finish(baseline, out=None, live=False, write=False)
    leg_ids = {l['leg'] for l in baseline['legs']}
    control_ids = set(baseline['cross_profile_controls'])
    results = []
    for inj in GATE_INJECTIONS:
        target = inj['target']
        # A refusal case names a vocabulary (CLAIM_KEYS/ROW_KEYS), not a leg or control id, so the
        # anchor check only applies to the two verdict expectations.
        anchor = True if inj['expect'] == 'HARNESS_REFUSAL' else (
            (target in leg_ids) if inj['expect'] == 'FAILED_LEG' else target in control_ids)
        if not anchor:
            results.append({'case': inj['case'], 'expect': inj['expect'], 'target': target,
                            'discriminated': False, 'why': inj['why'],
                            'detail': 'the target does not exist in the unmutated receipt, so this '
                                      'case could only ever appear to pass'})
            continue
        mutated = _clean(_copy.deepcopy(doc))
        detail = ''
        try:
            inj['apply'](mutated)
            finish(mutated, out=None, live=False, write=False)
        except Exception as exc:  # noqa: BLE001 - a harness refusal is itself a verdict
            detail = f'{type(exc).__name__}: {exc}'
        if inj['expect'] == 'FAILED_LEG':
            hit = target in (mutated.get('failed_legs') or [])
        elif inj['expect'] == 'FAILED_CONTROL':
            hit = target in (mutated.get('failed_controls') or [])
        elif inj['expect'] == 'HARNESS_REFUSAL':
            hit = bool(detail)
        else:
            raise ValueError(f'unknown expectation {inj["expect"]}')
        results.append({'case': inj['case'], 'expect': inj['expect'], 'target': target,
                        'discriminated': bool(hit), 'why': inj['why'], 'detail': detail or None,
                        'failed_legs_under_case': len(mutated.get('failed_legs') or []),
                        'failed_controls_under_case': len(mutated.get('failed_controls') or [])})
    after = C102.sha256_file(Path(receipt)) if receipt else None
    results.append({'case': 'sweep_writes_no_receipt', 'expect': 'RECEIPT_HASH_UNCHANGED',
                    'target': str(receipt) if receipt else None,
                    'discriminated': bool(before) and before == after,
                    'why': 'the audit runs against the filed artifact and may not alter it',
                    'detail': f'{before} -> {after}'})
    report = {'batch': doc['batch'], 'cases': len(results),
              'gate_injections': len(GATE_INJECTIONS),
              'receipt_sha256_before': before, 'receipt_sha256_after': after,
              'baseline_legs': len(leg_ids), 'baseline_controls': len(control_ids),
              'all_discriminated': all(r['discriminated'] for r in results),
              'undiscriminated': [r['case'] for r in results if not r['discriminated']],
              'results': results}
    (out / f'{BATCH}-gate-falsification.json').write_text(
        json.dumps(report, indent=2, ensure_ascii=False, sort_keys=True) + '\n')
    return report


# ------------------------------------------------------------------------------- manifest
def git_output(cwd, *args) -> str:
    out = subprocess.run(['git', '-C', str(cwd), *args], capture_output=True, text=True)
    return out.stdout.strip()


def third_party_modules() -> list:
    """Every import this file makes that is not the standard library. The join probe has to be ours,
    or the wire format it reports is whatever that package decided to parse."""
    bad = []
    src = (HERE / 'run-paired.py').read_text()
    for match in re.finditer(r'^\s*(?:import|from)\s+([a-zA-Z_][\w.]*)', src, re.M):
        root = match.group(1).split('.')[0]
        if root in sys.stdlib_module_names or root in ('__future__',):
            continue
        bad.append(root)
    return sorted(set(bad))


def new_doc(bins, ctx, harness_sha, c102_sha, c103_sha, c104_sha, bounds, hold, subset) -> dict:
    # Tier 5: both sides are the launched production binaries, paired on identical YAML bytes, and the
    # effect is read back from the ice_servers a real websocket join returns - not from a config
    # struct, not from a log line about a secret.
    return {'schema': SCHEMA, 'batch': BATCH, 'covers': COVERS, 'tier': 5, 'live_run': True,
            'skip_launch': False, 'subset': bool(subset),
            'expected_profile_count': PROFILE_COUNT,
            'platform': {'system': sys.platform, 'arch': os.uname().machine,
                         'python': sys.version.split()[0]},
            'binaries': bins,
            'join_client': {
                'kind': 'stdlib-rfc6455-and-protobuf-codec-in-this-file',
                'harness_sha256': harness_sha,
                'third_party_modules': third_party_modules(),
                'client_note': 'the join is a raw loopback websocket upgrade with a bearer token and '
                               'the first binary SignalResponse frame decoded by the field-number '
                               'codec above; nothing between the daemon and the assertion is '
                               'third-party code',
                'sdk_note': 'the original LiveKit SDK is deliberately not the client here. ice_servers '
                            'is delivered in the first response frame, before any PeerConnection or '
                            'ICE gathering exists, so an SDK client would consume the row rather than '
                            'expose it - and the claim is about the bytes the node emitted, which the '
                            'SDK re-encodes into its own config object. #386 pairs the SDK where the '
                            'claim is candidate behaviour on a live connection; here the SDK would add '
                            'a translation layer between the daemon and the digest being compared.'},
            'divergences': [
                {'id': 'turn_embedded_key_selection', 'profiles': ['emb-two-keys'],
                 'go': 'iceServersForParticipant is handed getFirstKeyPair() (roommanager.go:324, '
                       '1203-1208), which ranges the config.Keys map: the API key inside an embedded '
                       'TURN username is an arbitrary configured key, and with two keys configured it '
                       'changes between joins of one running process.',
                 'rust': 'the username names the key the join token was signed with, the same on every '
                         'join of one launch.',
                 'measured': 'emb-two-keys::go:kids-pinned False and emb-two-keys::rust:kids-pinned '
                             'True over 12 joins each; both sides sign the password from the secret of '
                             'the key the username actually names.',
                 'ledger': 'the derivation half of YAML:keys is promoted; the selection rule stays '
                           'OPEN as a follow-up row.'}],
            'reference': {'go_checkout': 'references/livekit-stable',
                          'go_commit': FIXED_GO_COMMIT,
                          'go_checkout_commit': git_output(REFERENCE, 'rev-parse', 'HEAD'),
                          'upstream_reads': {
                              'pkg/config/config.go:201-217': 'TURNServer{host,port,protocol,username,'
                                                              'credential,secret,secret_file,ttl} is the '
                                                              'external list element; the embedded '
                                                              'TURNConfig has no secret field',
                              'pkg/config/config.go:52-75': 'DefaultTURNTTLSeconds 300, '
                                                            'DefaultExternalTURNTTLSeconds 14400, '
                                                            'TURNMaxTTLSeconds 86400 and the four '
                                                            'credential errors',
                              'pkg/config/config.go:857-898': 'LoadTURNSecrets: trim the inline secret, '
                                                              'precedence with a warning, stat then the '
                                                              '0o007 other-permission filter, read, '
                                                              'trim, ErrTURNSecretEmpty, then the '
                                                              'hasDynamic/hasStatic union',
                              'pkg/config/config.go:903-941': 'ClampTURNTTLSeconds and NormalizeTURNTTLs '
                                                              '- the embedded ttl is clamped both ways at '
                                                              'load, the external one only at the top',
                              'pkg/service/roommanager.go:1059-1155': 'iceServersForParticipant: the '
                                                                      'embedded turn:/turns: pair, '
                                                                      'CreateUsername/CreatePassword, '
                                                                      'the external HMAC-SHA1 branch, '
                                                                      'hasSTUN and the default STUN '
                                                                      'fallback',
                              'pkg/service/turn.go:292-334': 'CreateUsername clamps then base62-encodes '
                                                             '"apiKey|participantID|expiry"; '
                                                             'computePassword is base62 of the sha256 of '
                                                             '"secret|participantID|expiry" using the '
                                                             'key that signed the token',
                              'cmd/server/main.go:258-266': 'LoadTURNSecrets runs during server startup '
                                                             'and a failure aborts the process',
                              'crates/lk-config/src/load.rs:384-420': 'the Rust mirror, including the '
                                                                      'binary_secrets vector for a file '
                                                                      'that is not valid UTF-8',
                              'crates/lk-config/src/load.rs:221-245': 'normalize_turn_ttls',
                              'crates/lk-config/src/keys.rs:113-137': 'load_turn_secret_file: stat, the '
                                                                      'others-bits filter, read, trim, '
                                                                      'the empty error',
                              'crates/lk-compat/src/signal/ice.rs:62-141': 'the same three URL shapes '
                                                                          'and the Some(secret)/static '
                                                                          'split on the Rust side',
                              'crates/lk-compat/src/signal/ice.rs:145-161': 'rest_credentials: zero '
                                                                           'means 14400, then '
                                                                           'base64(HMAC-SHA1)',
                              'crates/lk-compat/src/turn_auth.rs:43-80': 'create_credentials: the base62 '
                                                                        'username and password'}},
            'harness': str(HERE / 'run-paired.py'),
            'harness_sha256_at_start': harness_sha, 'harness_sha256': harness_sha,
            'imports_from': [str(C102_DIR / 'run-paired.py'), str(C103_DIR / 'run-paired.py'),
                             str(C104_DIR / 'run-paired.py')],
            'c102_harness_sha256_at_start': c102_sha, 'c102_harness_sha256': c102_sha,
            'c103_harness_sha256_at_start': c103_sha, 'c103_harness_sha256': c103_sha,
            'c104_harness_sha256_at_start': c104_sha, 'c104_harness_sha256': c104_sha,
            'imports_reason': 'the launcher, port allocator, socket observers, owned store and address '
                              'masker are the accepted C102 primitives; the certificate generator, the '
                              'JWT builder and the port listeners are C103 s; the loopback-port masking '
                              'is C104 s. Importing (not copying) leaves all three files byte-identical, '
                              'so their filed receipts stay re-derivable',
            'work_dir': str(ctx['work']),
            'ports': {k[:-5]: v for k, v in ctx.items() if k.endswith('_port')},
            'port_roles': {str(k): v for k, v in ctx['port_roles'].items()},
            'ip_salt': IP_SALT, 'off_limits': list(OFF_LIMITS), 'host_store_port': HOST_STORE_PORT,
            'hold_seconds': hold, 'listeners_bounds': bounds, 'store_flush_ok': True,
            'redis': {'plain_role': 'STORE'},
            'fixtures': {'key_ids': [KID, KID2], 'secrets': ['<SEC-K>', '<SEC-M>', '<SEC-X>',
                                                            '<SEC-F>', '<SEC-G>'],
                         'static_pair': ['<USR>', '<CRD>'], 'domain': DOMAIN, 'host': HOST,
                         'room': ROOM, 'literal_turn_ports': sorted(LITERAL_TURN_PORTS),
                         'default_stun_urls': DEFAULT_STUN_URLS,
                         'secret_files': {'secret-600': '0600', 'secret-world': '0604',
                                          'secret-empty': '0600 (whitespace)',
                                          'secret-bin': '0600 (not UTF-8)',
                                          'secret-dir': '0700 (a directory)'}},
            'ttl_constants': {'embedded_default': TTL_DEFAULT_EMBEDDED,
                              'external_default': TTL_DEFAULT_EXTERNAL, 'max': TTL_MAX,
                              'min': TTL_MIN},
            'profiles': []}




def profiles(ctx) -> list:
    out = []
    clash = sorted(LITERAL_TURN_PORTS & set(ctx['port_roles']))
    if clash:
        raise RuntimeError(f'a fixture port collides with an allocated role port: {clash}')
    probe_ports = [('http', ctx['http_port']), ('tu1', ctx['tu1_port']),
                   ('tu2', ctx['tu2_port']), ('tls1', ctx['tls1_port'])]
    joined = {'count': 1}

    def add(pid, rows, claims, cfg_text, argv=(), env=None, expect_exit=0, refuse_wait=None,
            ready=True, join_kid=KID, join_secret=KSEC, secrets=None, note='', ice=None,
            check_store=True, extra_ports=()):
        out.append({'id': pid, 'rows': rows, 'claims': claims, 'argv': list(argv),
                    'env': dict(env or {}), 'cfg': cfg_text, 'expect_exit': expect_exit,
                    'refuse_wait': refuse_wait, 'ready': ready, 'note': note,
                    'join_kid': join_kid, 'join_secret': join_secret,
                    'secrets': dict(secrets or {}), 'ice': ice, 'check_store': check_store,
                    'probe_ports': probe_ports + list(extra_ports)})

    cand = {'ext': EXT_SECRET, 'file': FILE_SECRET, 'bin': BIN_SECRET, 'foreign': FOREIGN_SECRET}
    emb_cand = {'k': KSEC, 'm': KSEC2, 'x': EXT_SECRET}

    def ts(pid, rows, ice, items, turn='', claims=None, **kw):
        """A profile whose YAML is `rtc.turn_servers` (plus an optional embedded block) joined with
        every secret this lane configured, so each row also records which candidates did NOT match."""
        add(pid, rows, dict(claims or {}), cfg(ctx, ts_items=ts_block(*items), turn=turn),
            ice=ice, secrets=cand, **kw)

    # ------------------------------------------------------------ what a join returns with no TURN
    # The lane's positive control: the probe really does read ice_servers, and the shape it sees when
    # nothing is configured is upstream's own default STUN list with an empty credential pair.
    add('no-turn-default-stun', ['YAML:rtc.turn_servers', 'YAML:rtc.stun_servers'],
        {'log_not': [TURN_START], 'node_alive': 1, 'room_sid_present': True},
        cfg(ctx), ready=True, ice={'count': 1, 'rows': [
            row(urls=DEFAULT_STUN_URLS, cls='empty', url_count=3, cred_len=0,
                hmac={'ext': 'skip', 'file': 'skip', 'bin': 'skip', 'foreign': 'skip'})]},
        secrets=cand,
        note='the control: with no TURN config the join still carries ice_servers, and the credential '
             'pair on that row is empty on both binaries')

    add('ts-stun-list-append', ['YAML:rtc.stun_servers'],
        {'node_alive': 1, 'room_sid_present': True, 'log_not': [NO_CREDENTIALS]},
        cfg(ctx, stun='    - stun.c105.test:3478\n'), ready=True,
        ice={'count': 1, 'rows': [row(url='stun:stun.c105.test:3478', cls='empty', url_count=1,
                                      cred_len=0)]},
        note='rtc.stun_servers reaches the same list, and the default STUN fallback stays silent once '
             'a configured server sets hasSTUN')

    # --------------------------------------------------------------------- secret -> HMAC-SHA1
    ts('ts-secret-inline',
       ['YAML:rtc.turn_servers[].secret', 'YAML:rtc.turn_servers[].host',
        'YAML:rtc.turn_servers[].port', 'YAML:rtc.turn_servers[].protocol',
        'YAML:rtc.turn_servers[].ttl', 'YAML:rtc.turn_servers'],
       {'count': 1, 'rows': [
           row(url=f'turn:{HOST}:3478?transport=udp', cls='rest', ttl=TTL_DEFAULT_EXTERNAL,
               sid_ok=True, url_count=1, cred_len=28,
               hmac={'ext': 'match', 'file': 'no', 'bin': 'no', 'foreign': 'no'})]},
       [ts_item(secret=EXT_SECRET)],
       claims={'log_not': [SECRET_PRECEDENCE, NO_CREDENTIALS], 'node_alive': 1,
               'room_sid_present': True},
       note='the inline secret is the HMAC key: the credential on the wire reproduces from these bytes '
            'and from none of the other three')

    sf600 = secret_file(ctx, 'secret-600', FILE_SECRET.encode() + b'\n', 0o600)
    ts('ts-secret-file-0600',
       ['YAML:rtc.turn_servers[].secret_file', 'YAML:rtc.turn_servers[].secret'],
       {'count': 1, 'rows': [
           row(url=f'turn:{HOST}:3478?transport=udp', cls='rest', ttl=TTL_DEFAULT_EXTERNAL,
               sid_ok=True, url_count=1, cred_len=28,
               hmac={'file': 'match', 'ext': 'no', 'bin': 'no', 'foreign': 'no'})]},
       [ts_item(secret_file_path=sf600)],
       claims={'log_not': [SECRET_PRECEDENCE, PERMS_NOT_ZERO], 'node_alive': 1,
               'room_sid_present': True},
       note='the file bytes are the key and the trailing newline is trimmed: the digest says which of '
            'the two fixtures signed the credential')

    ts('ts-secret-beats-file', ['YAML:rtc.turn_servers[].secret',
                                'YAML:rtc.turn_servers[].secret_file'],
       {'count': 1, 'rows': [
           row(url=f'turn:{HOST}:3478?transport=udp', cls='rest', ttl=TTL_DEFAULT_EXTERNAL,
               sid_ok=True, url_count=1, cred_len=28,
               hmac={'ext': 'match', 'file': 'no', 'bin': 'no', 'foreign': 'no'})]},
       [ts_item(secret=EXT_SECRET, secret_file_path=sf600)],
       claims={'log_all': [SECRET_PRECEDENCE], 'node_alive': 1, 'room_sid_present': True},
       note='both fields set: the warning is printed and the inline value wins, which is the whole '
            'precedence branch and it is visible in the digest as well as the log')

    ts('ts-blank-inline-file-wins', ['YAML:rtc.turn_servers[].secret',
                                     'YAML:rtc.turn_servers[].secret_file'],
       {'count': 1, 'rows': [
           row(url=f'turn:{HOST}:3478?transport=udp', cls='rest', ttl=TTL_DEFAULT_EXTERNAL,
               sid_ok=True, url_count=1, cred_len=28,
               hmac={'file': 'match', 'ext': 'no', 'bin': 'no', 'foreign': 'no'})]},
       [ts_item(secret='   ', secret_file_path=sf600)],
       claims={'log_not': [SECRET_PRECEDENCE, NO_CREDENTIALS], 'node_alive': 1,
               'room_sid_present': True},
       note='a whitespace inline secret is trimmed before the switch, so the file branch runs and no '
            'precedence warning is printed')

    ts('ts-static-pair', ['YAML:rtc.turn_servers[].username',
                          'YAML:rtc.turn_servers[].credential',
                          'YAML:rtc.turn_servers[].host', 'YAML:rtc.turn_servers[].port'],
       {'count': 1, 'rows': [
           row(url=f'turn:{HOST}:3479?transport=tcp', cls='static', url_count=1,
               static_text='<USR>', cred_len=len(STATIC_CRED),
               cred_sha256=hashlib.sha256(STATIC_CRED.encode()).hexdigest()[:16])]},
       [ts_item(port=3479, protocol='tcp', username=STATIC_USER, credential=STATIC_CRED)],
       claims={'log_not': [NO_CREDENTIALS], 'node_alive': 1, 'room_sid_present': True},
       note='the static pair comes back byte-identical (the digest is of the YAML value itself) and the '
            'username keeps no expiry shape at all')

    ts('ts-empty-secret-with-static', ['YAML:rtc.turn_servers[].secret',
                                       'YAML:rtc.turn_servers[].username'],
       {'count': 1, 'rows': [
           row(url=f'turn:{HOST}:3478?transport=udp', cls='static', url_count=1,
               static_text='<USR>', cred_len=len(STATIC_CRED),
               cred_sha256=hashlib.sha256(STATIC_CRED.encode()).hexdigest()[:16])]},
       [ts_item(secret='', username=STATIC_USER, credential=STATIC_CRED)],
       claims={'log_not': [SECRET_PRECEDENCE, NO_CREDENTIALS], 'node_alive': 1,
               'room_sid_present': True},
       note='secret: "" is a present-but-blank field: hasDynamic is false, so the static pair wins. '
            'This is the row where a Some(\"\")/None split in the loader would show up as a dynamic '
            'credential on one side')

    ts('ts-two-servers-order',
       ['YAML:rtc.turn_servers[].host', 'YAML:rtc.turn_servers[].port',
        'YAML:rtc.turn_servers[].secret', 'YAML:rtc.turn_servers[].secret_file',
        'YAML:rtc.turn_servers'],
       {'count': 2, 'rows': [
           row(url=f'turn:{HOST}:3478?transport=udp', cls='rest', ttl=TTL_DEFAULT_EXTERNAL,
               sid_ok=True, url_count=1, cred_len=28,
               hmac={'ext': 'match', 'file': 'no', 'bin': 'no', 'foreign': 'no'}),
           row(url=f'turns:second.c105.test:443?transport=tcp', cls='rest',
               ttl=TTL_DEFAULT_EXTERNAL, sid_ok=True, url_count=1, cred_len=28,
               hmac={'file': 'match', 'ext': 'no', 'bin': 'no', 'foreign': 'no'})]},
       [ts_item(secret=EXT_SECRET),
        ts_item(host='second.c105.test', port=443, protocol='tls',
                secret_file_path=sf600)],
       claims={'log_not': [NO_CREDENTIALS], 'node_alive': 1, 'room_sid_present': True},
       note='two servers, two hosts, two ports, two protocols, two secret sources - the list order is '
            'the wire order and each row is signed by its own secret')

    ts('ts-ttl-per-row', ['YAML:rtc.turn_servers[].ttl'],
       {'count': 2, 'rows': [
           row(url=f'turn:{HOST}:3478?transport=udp', cls='rest', ttl=600, sid_ok=True,
               url_count=1, cred_len=28,
               hmac={'ext': 'match', 'file': 'no', 'bin': 'no', 'foreign': 'no'}),
           row(url=f'turn:{HOST}:3479?transport=tcp', cls='rest', ttl=TTL_DEFAULT_EXTERNAL,
               sid_ok=True, url_count=1, cred_len=28,
               hmac={'file': 'match', 'ext': 'no', 'bin': 'no', 'foreign': 'no'})]},
       [ts_item(secret=EXT_SECRET, ttl=600),
        ts_item(port=3479, protocol='tcp', secret_file_path=sf600, ttl=0)],
       claims={'node_alive': 1, 'room_sid_present': True},
       note='ttl is per entry, not per server: one row expires in 600 s and the other in the 14400 s '
            'that a zero means')

    # --------------------------------------------------------------------- protocol -> URL
    ts('ts-protocol-tls-static', ['YAML:rtc.turn_servers[].protocol'],
       {'count': 1, 'rows': [
           row(url=f'turns:{HOST}:3478?transport=tcp', cls='static', url_count=1,
               static_text='<USR>', cred_len=len(STATIC_CRED),
               cred_sha256=hashlib.sha256(STATIC_CRED.encode()).hexdigest()[:16])]},
       [ts_item(protocol='tls', username=STATIC_USER, credential=STATIC_CRED)],
       claims={'node_alive': 1, 'room_sid_present': True},
       note='"tls" is the only value that changes the scheme, and it also flips the transport to tcp')

    ts('ts-protocol-omitted', ['YAML:rtc.turn_servers[].protocol'],
       {'count': 1, 'rows': [
           row(url=f'turn:{HOST}:3478?transport=tcp', cls='static', url_count=1,
               static_text='<USR>', cred_len=len(STATIC_CRED),
               cred_sha256=hashlib.sha256(STATIC_CRED.encode()).hexdigest()[:16])]},
       [ts_item(protocol=None, username=STATIC_USER, credential=STATIC_CRED)],
       claims={'node_alive': 1, 'room_sid_present': True},
       note='no protocol key at all: the zero value is not "udp", it is the fall-through turn/tcp')

    ts('ts-protocol-unknown-quic', ['YAML:rtc.turn_servers[].protocol'],
       {'count': 1, 'rows': [
           row(url=f'turn:{HOST}:3478?transport=tcp', cls='static', url_count=1,
               static_text='<USR>', cred_len=len(STATIC_CRED),
               cred_sha256=hashlib.sha256(STATIC_CRED.encode()).hexdigest()[:16])]},
       [ts_item(protocol='quic', username=STATIC_USER, credential=STATIC_CRED)],
       claims={'log_not': [NO_CREDENTIALS], 'node_alive': 1, 'room_sid_present': True},
       note='an unlisted protocol is neither rejected nor honoured: it advertises turn/tcp, the same '
            'URL an omitted one produces')

    # ------------------------------------------------------------------------ external ttl bands
    ts('ts-ttl-zero-explicit', ['YAML:rtc.turn_servers[].ttl'],
       {'count': 1, 'rows': [
           row(url=f'turn:{HOST}:3478?transport=udp', cls='rest', ttl=TTL_DEFAULT_EXTERNAL,
               sid_ok=True, url_count=1, cred_len=28,
               hmac={'ext': 'match', 'file': 'no', 'bin': 'no', 'foreign': 'no'})]},
       [ts_item(secret=EXT_SECRET, ttl=0)],
       claims={'node_alive': 1, 'room_sid_present': True},
       note='an explicit 0 is the same as an absent ttl: the external default of 14400 s')

    ts('ts-ttl-600', ['YAML:rtc.turn_servers[].ttl'],
       {'count': 1, 'rows': [
           row(url=f'turn:{HOST}:3478?transport=udp', cls='rest', ttl=600, sid_ok=True,
               url_count=1, cred_len=28,
               hmac={'ext': 'match', 'file': 'no', 'bin': 'no', 'foreign': 'no'})]},
       [ts_item(secret=EXT_SECRET, ttl=600)],
       claims={'node_alive': 1, 'room_sid_present': True},
       note='a configured ttl inside the range is used verbatim')

    ts('ts-ttl-negative', ['YAML:rtc.turn_servers[].ttl'],
       {'count': 1, 'rows': [
           row(url=f'turn:{HOST}:3478?transport=udp', cls='rest', ttl=TTL_DEFAULT_EMBEDDED,
               sid_ok=True, url_count=1, cred_len=28,
               hmac={'ext': 'match', 'file': 'no', 'bin': 'no', 'foreign': 'no'})]},
       [ts_item(secret=EXT_SECRET, ttl=-5)],
       claims={'log_not': [TS_TTL_CLAMPED], 'node_alive': 1, 'room_sid_present': True},
       note='a negative ttl is not clamped at load (only the upper bound is), it survives to '
            'credential generation where both binaries land on the 300 s default, not the 14400 s that '
            'a zero gets')

    ts('ts-ttl-over-max', ['YAML:rtc.turn_servers[].ttl'],
       {'count': 1, 'rows': [
           row(url=f'turn:{HOST}:3478?transport=udp', cls='rest', ttl=TTL_MAX, sid_ok=True,
               url_count=1, cred_len=28,
               hmac={'ext': 'match', 'file': 'no', 'bin': 'no', 'foreign': 'no'})]},
       [ts_item(secret=EXT_SECRET, ttl=999999)],
       claims={'log_all_rust': [TS_TTL_CLAMPED], 'log_not_go': [TS_TTL_CLAMPED], 'node_alive': 1,
               'room_sid_present': True},
       note='999999 s is capped at 86400 before the credential is ever made, so the wire cannot tell '
            'a cap from a configured value; only the warning does, and Go prints it before its logger '
            'exists')

    # ------------------------------------------------------------------- embedded credentials
    def emb(pid, rows, ice, turn='', claims=None, argv=(), env=None, keys=None, note='',
            join_kid=KID, join_secret=KSEC, secrets=None, extra_claims=None):
        add(pid, rows, dict({'node_alive': 1, 'room_sid_present': True}, **(extra_claims or {})),
            cfg(ctx, keys=keys, turn=turn or turn_block(ctx, tu='tu1')), argv=argv, env=env,
            ice=ice, join_kid=join_kid, join_secret=join_secret,
            secrets=secrets or emb_cand, note=note)

    def emb_row(ttl, kid=KID, urls=None, kid_signs=None, pw=None):
        return row(urls=urls or [f'turn:127.0.0.1:<TU1>?transport=udp'], cls='embedded', ttl=ttl,
                   sid_ok=True, kid=kid, kid_signs=kid_signs, url_count=len(urls or ['x']),
                   cred_band=[40, 48], pw=pw or {'k': 'match', 'm': 'no', 'x': 'no'})

    emb('emb-ttl-default', ['YAML:turn.ttl_seconds', 'YAML:turn.udp_port', 'YAML:turn.domain',
                            'YAML:keys', 'YAML:rtc.node_ip'],
        {'count': 1, 'rows': [emb_row(TTL_DEFAULT_EMBEDDED)]},
        note='no ttl_seconds in the YAML: the base62 username carries the 300 s default and the '
             'password reproduces from the joining key\'s secret')

    emb('emb-ttl-120', ['YAML:turn.ttl_seconds'],
        {'count': 1, 'rows': [emb_row(120)]},
        turn=turn_block(ctx, tu='tu1', ttl_seconds=120),
        note='the configured value arrives verbatim')

    emb('emb-ttl-clamp-high', ['YAML:turn.ttl_seconds'],
        {'count': 1, 'rows': [emb_row(TTL_MAX)]},
        turn=turn_block(ctx, tu='tu1', ttl_seconds=999999),
        extra_claims={'log_all_rust': [EMB_TTL_CLAMPED], 'log_not_go': [EMB_TTL_CLAMPED]},
        note='999999 is clamped to 86400 at load on both sides, and the asymmetry is measured: Rust '
             'logs the upstream sentence, Go never does because NormalizeTURNTTLs runs before '
             'InitLoggerFromConfig (main.go:204 before 208)')

    emb('emb-ttl-clamp-negative', ['YAML:turn.ttl_seconds'],
        {'count': 1, 'rows': [emb_row(TTL_DEFAULT_EMBEDDED)]},
        turn=turn_block(ctx, tu='tu1', ttl_seconds=-5),
        extra_claims={'log_all_rust': [EMB_TTL_CLAMPED], 'log_not_go': [EMB_TTL_CLAMPED]},
        note='unlike the external field, a negative embedded ttl is clamped at load and the wire shows '
             'the 300 s default')

    emb('emb-ttl-flag-beats-yaml', ['generated_flags:--turn.ttl_seconds', 'YAML:turn.ttl_seconds'],
        {'count': 1, 'rows': [emb_row(240)]},
        turn=turn_block(ctx, tu='tu1', ttl_seconds=120), argv=['--turn.ttl_seconds', '240'],
        note='the flag spelling moves the expiry that the join hands out, which is the launched '
             'consumer C104 withheld for this row')

    emb('emb-ttl-env-beats-yaml', ['generated_flags:--turn.ttl_seconds'],
        {'count': 1, 'rows': [emb_row(360)]},
        turn=turn_block(ctx, tu='tu1', ttl_seconds=120),
        env={'LIVEKIT_TURN_TTL_SECONDS': '360'},
        note='the env form of the same generated flag, attested on the credential instead of the '
             'projection')

    emb('emb-two-keys', ['YAML:keys', 'YAML:turn.ttl_seconds'],
        {'count': 1, 'rows': [emb_row(TTL_DEFAULT_EMBEDDED, kid=None, kid_signs=True,
                                      pw={'x': 'no'})]},
        keys={KID: KSEC, KID2: KSEC2}, join_kid=KID2, join_secret=KSEC2,
        extra_claims={'join_repeat': TWO_KEY_PROBES, 'kids_allowed': [KID, KID2],
                      'kids_pinned': {'go': False, 'rust': True}},
        note='twelve joins with a token signed by the second key. Rust names that key every time; Go '
             'takes getFirstKeyPair() (roommanager.go:324, 1203-1208), which ranges a map, so the key '
             'it advertises is not the token\'s and is not stable across joins of one process. Either '
             'way the password reproduces from the secret of the key the username actually names, '
             'and never from the unconfigured third secret - the selection rule is the divergence, not '
             'the derivation.')

    # ---------------------------------------- the advertised 443 and the hasSTUN fallback, one launch
    ts443 = turn_block(ctx, tls='tls1', tu=None)
    add('emb-uri-tls-443',
        ['YAML:turn.tls_port', 'YAML:turn.domain', 'YAML:turn.enabled', 'YAML:keys'],
        {'log_all': [TURN_START, SERVER_READY], 'tcp_has': ['TLS1'], 'udp_not': ['TU1', 'TU2'],
         'tcp_not': ['TU1', 'TU2'], 'release_ports': True, 'node_alive': 1, 'room_sid_present': True},
        cfg(ctx, turn=ts443), ready=True,
        ice={'count': 2, 'rows': [
            row(urls=['turns:turn.c105.test:443?transport=tcp'], cls='embedded',
                ttl=TTL_DEFAULT_EMBEDDED, sid_ok=True, kid=KID, url_count=1,
                cred_band=[40, 48], pw={'k': 'match', 'm': 'no', 'x': 'no'}),
            row(urls=DEFAULT_STUN_URLS, cls='empty', url_count=3, cred_len=0)]},
        secrets=emb_cand, extra_ports=[],
        note='the listener is on <TLS1> and the URL says 443, which is the hardcoding; and because '
             'only the TLS branch ran, hasSTUN stayed false, so the default STUN row follows')

    add('emb-disabled-with-ts', ['YAML:turn.enabled', 'YAML:rtc.turn_servers[].secret'],
        {'log_not': [TURN_START], 'udp_not': ['TU1', 'TU2'], 'tcp_not': ['TLS1', 'TU1'],
         'release_ports': True, 'node_alive': 1, 'room_sid_present': True},
        cfg(ctx, ts_items=ts_block(ts_item(secret=EXT_SECRET)),
            turn=turn_block(ctx, enabled=False, tu='tu1')), ready=True,
        ice={'count': 1, 'rows': [
            row(url=f'turn:{HOST}:3478?transport=udp', cls='rest', ttl=TTL_DEFAULT_EXTERNAL,
                sid_ok=True, url_count=1, cred_len=28,
                hmac={'ext': 'match', 'file': 'no', 'bin': 'no', 'foreign': 'no'})]},
        secrets=cand,
        note='turn.enabled false removes the embedded row and nothing else; the external list is a '
             'separate consumer and still advertises')

    # ------------------------------------------------------------------------- secret-file refusals
    def refuse(pid, rows, items, needles, kind, turn=''):
        add(pid, rows, {'log_any': needles, 'store_left_clean': True, 'store_no_node_after': True,
                        'udp_not': ['TU1', 'TU2'], 'tcp_not': ['TLS1', 'HTTP', 'TU1', 'TU2'],
                        'release_ports': True},
            cfg(ctx, ts_items=ts_block(*items), turn=turn), ready=False, expect_exit='nonzero',
            refuse_wait=6.0, ice={'count': 0, 'rows': []}, secrets=cand,
            note=f'{kind}: the daemon never becomes ready, so nothing is advertised and the store is '
                 f'untouched')

    refuse('ts-username-only-refused', ['YAML:rtc.turn_servers[].username'],
           [ts_item(username=STATIC_USER)], [NO_CREDENTIALS],
           'a username without a credential is not a static pair')

    refuse('ts-no-credentials-refused',
           ['YAML:rtc.turn_servers[].credential', 'YAML:rtc.turn_servers[].secret'],
           [ts_item()], [NO_CREDENTIALS], 'host and port alone are neither mode')

    refuse('ts-file-missing-refused', ['YAML:rtc.turn_servers[].secret_file'],
           [ts_item(secret_file_path=str(ctx['fx'] / 'nowhere'))],
           [NO_SUCH_FILE, NO_SUCH_FILE_RUST], 'the stat happens before any secret is read')

    world = secret_file(ctx, 'secret-world', FILE_SECRET.encode(), 0o604)
    refuse('ts-file-world-readable-refused', ['YAML:rtc.turn_servers[].secret_file'],
           [ts_item(secret_file_path=world)], [PERMS_NOT_ZERO],
           'other-writable (0o604) trips the 0o007 filter')

    empty = secret_file(ctx, 'secret-empty', b'  \n\t\n', 0o600)
    refuse('ts-file-empty-refused', ['YAML:rtc.turn_servers[].secret_file'],
           [ts_item(secret_file_path=empty)], [SECRET_EMPTY],
           'whitespace-only content trims to nothing')

    dirpath = ctx['fx'] / 'secret-dir'
    dirpath.mkdir(exist_ok=True)
    os.chmod(dirpath, 0o700)
    refuse('ts-file-is-directory-refused', ['YAML:rtc.turn_servers[].secret_file'],
           [ts_item(secret_file_path=str(dirpath))],
           [IS_A_DIRECTORY, IS_A_DIRECTORY_RUST],
           'a directory passes os.Stat, and the mode check too at 0700, so the read is what fails')

    # ------------------------------------------------------------- a secret file that is not text
    binpath = secret_file(ctx, 'secret-bin', BIN_SECRET, 0o600)
    ts('ts-file-binary-bytes', ['YAML:rtc.turn_servers[].secret_file'],
       {'count': 1, 'rows': [
           row(url=f'turn:{HOST}:3478?transport=udp', cls='rest', ttl=TTL_DEFAULT_EXTERNAL,
               sid_ok=True, url_count=1, cred_len=28,
               hmac={'bin': 'match', 'file': 'no', 'ext': 'no', 'foreign': 'no'})]},
       [ts_item(secret_file_path=binpath)],
       claims={'log_not': [SECRET_EMPTY, PERMS_NOT_ZERO], 'node_alive': 1, 'room_sid_present': True},
       note='the file bytes are not valid UTF-8. Go uses them as the key because string([]byte) is '
            'lossless; Rust cannot put them in a String, so it carries the raw bytes in a separate '
            'vector and the digest says both arrived at the same key')
    return out
