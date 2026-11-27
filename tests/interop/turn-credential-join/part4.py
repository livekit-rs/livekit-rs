

# --------------------------------------------------------------------------- runner
def mask_store(snap: dict, masker) -> dict:
    """Store keys are names the daemon chose, and a participant key carries the SID the credential
    embeds. The snapshot goes through the same masker as the logs, or the receipt is a SID list."""
    if not snap:
        return {}
    return {'keys': [masker.text(k) for k in snap.get('keys') or []],
            'count': snap.get('count'),
            'node_ids': [masker.text(n) for n in snap.get('node_ids') or []],
            'nodes_key_present': snap.get('nodes_key_present')}


def run_entry(ctx, prof, masker, binaries) -> dict:
    claimed_ports = set(prof['claims'].get('tcp_has', [])) | set(prof['claims'].get('tcp_not', [])) \
        | set(prof['claims'].get('udp_has', [])) | set(prof['claims'].get('udp_not', []))
    unknown_roles = claimed_ports - set(ctx['port_roles'].values())
    if unknown_roles:
        raise RuntimeError(f'{prof["id"]} claims ports never allocated: {sorted(unknown_roles)}')
    entry = {'profile': prof['id'], 'rows': prof['rows'], 'claims': prof['claims'],
             'note': prof['note'], 'expect_exit': prof['expect_exit'],
             'refuse_wait': prof['refuse_wait'], 'ready': prof['ready'],
             'ice': prof['ice'], 'runs': {}}
    plain = ctx['store_port']
    for side in ('go', 'rust'):
        C102.redis_cli(plain, 'FLUSHALL')
        before = C102.dbsize(plain)
        if before != 0:
            raise RuntimeError(f'store not clean entering {prof["id"]}/{side}: {before}')

        def on_alive(proc, cfg_path, prof=prof):
            observed = {}
            if prof['ready']:
                # The credential is read while the node is up, and the registry row is sampled after
                # the join returns: a participant that reached the room is what makes the row real.
                joins = [mask_join(
                    join_probe(ctx['http_port'], prof['join_kid'], prof['join_secret'],
                               prof['secrets']), masker)
                    for _ in range(max(1, int(prof['claims'].get('join_repeat') or 1)))]
                observed['join'] = joins[0]
                observed['extra_joins'] = joins[1:]
            observed['store'] = mask_store(C102.store_snapshot(plain), masker)
            return observed

        rec = C102.launch_pair(Path(binaries[side]), list(prof['argv']), prof['env'], prof['cfg'],
                               ctx['work'], f'{side}-{prof["id"]}', side, masker,
                               ctx['http_port'] if prof['ready'] else None, ctx['hold'],
                               prof['refuse_wait'], on_alive=on_alive)
        obs = rec.pop('observed', {}) or {}
        rec['join'] = obs.get('join') or {'kind': 'not-attempted', 'status': '', 'frames': 0,
                                          'rows': [], 'sid': None, 'room': None, 'version': None,
                                          'join_ms': 0}
        rec['extra_joins'] = obs.get('extra_joins') or []
        rec['store'] = {'before_dbsize': before, 'alive': obs.get('store') or {},
                        'after': mask_store(C102.store_snapshot(plain), masker)}
        rec['post_ports'] = {name: {'tcp': C103.tcp_listeners(port), 'udp': C103.udp_holders(port)}
                             for name, port in prof['probe_ports']}
        entry['runs'][side] = rec
    return entry


# ------------------------------------------------------------------------------- claims/legs
# The one vocabulary evaluate understands. A claim key outside this set would be recorded and then
# silently ignored, which is how a receipt grows a field that asserts nothing.
CLAIM_KEYS = {'log_all', 'log_all_go', 'log_all_rust', 'log_not', 'log_not_go', 'log_not_rust',
              'log_any', 'tcp_has', 'tcp_not', 'udp_has', 'udp_not', 'node_alive',
              'store_left_clean', 'store_no_node_after', 'release_ports', 'room_sid_present',
              'join_repeat', 'kids_pinned', 'kids_allowed'}

# Which of the per-row fields an expectation is allowed to name, so a typo in a profile cannot make a
# leg vanish: an expectation key outside this set raises instead of being skipped.
ROW_KEYS = {'class', 'url', 'urls', 'ttl', 'hmac', 'pw', 'sid_ok', 'kid', 'url_count',
            'static_text', 'cred_len', 'cred_band', 'cred_sha256', 'kid_signs'}


def needle_slug(needle: str) -> str:
    return re.sub(r'[^0-9a-z]', '', str(needle).lower())[:16] or 'x'


def sock_token(role: str) -> str:
    """An allocated port reaches the receipt as the masker's own <ROLE> token, never as the bare
    role name the claim uses. Comparing the bare name against the masked list makes every tcp_has
    leg fail and every tcp_not leg pass, which is the shape of a vacuous socket claim."""
    return role if role.startswith('<') else f'<{role}>'


def leg(name, side, kind, why='', **fields):
    item = {'leg': name, 'side': side, 'kind': kind, 'why': why}
    item.update(fields)
    return item


def rederive_leg(item) -> bool:
    kind = item['kind']
    if kind == 'has':
        return all(needle in item['haystack'] for needle in item['needles'])
    if kind == 'hasnt':
        return not any(needle in item['haystack'] for needle in item['needles'])
    if kind == 'any':
        return bool(item['needles']) and any(needle in item['haystack'] for needle in item['needles'])
    if kind == 'nonzero':
        return bool(item['actual'])
    if kind == 'eq':
        return item['expect'] == item['actual']
    if kind == 'band':
        return (isinstance(item['actual'], int) and not isinstance(item['actual'], bool)
                and item['expect'][0] <= item['actual'] <= item['expect'][1])
    if kind == 'same':
        vals = item['values']
        return len(vals) == 2 and vals[0] is not None and vals[0] == vals[1]
    raise RuntimeError(f'leg {item["leg"]} has kind {kind!r}, which this re-deriver cannot check')


def ttl_minute(delta):
    """Nearest-minute bucket for a pair comparison. Every fixture TTL is a multiple of 60 s, so the
    bucket is stable across the sub-second skew between two joins while 120/300/600/14400/86400 stay
    five different values."""
    return None if delta is None else int(round(delta / 60.0))


def evaluate(entry) -> list:
    """Pure over the receipt, so --record, --report and --selftest-gate grade the same legs."""
    unknown = set(entry['claims']) - CLAIM_KEYS
    if unknown:
        raise RuntimeError(f'{entry["profile"]} carries claims nothing evaluates: {sorted(unknown)}')
    legs = []
    claims = entry['claims']
    pid = entry['profile']
    ice = entry['ice'] or {}
    exp_rows = ice.get('rows') or []
    for exp in exp_rows:
        stray = set(exp) - ROW_KEYS
        if stray:
            raise RuntimeError(f'{pid} row expectation uses fields evaluate ignores: {sorted(stray)}')

    for side in ('go', 'rust'):
        rec = entry['runs'][side]
        log = rec.get('log_masked') or ''
        sockets = rec.get('sockets_masked') or {}
        tcp = sockets.get('tcp') or []
        udp = sockets.get('udp') or []
        join = rec.get('join') or {}
        rows = join.get('rows') or []

        def emit(suffix, kind, **fields):
            legs.append(leg(f'{pid}::{side}:{suffix}', side, kind, **fields))

        for group, sides in (('log_all', ('go', 'rust')), ('log_all_go', ('go',)),
                             ('log_all_rust', ('rust',))):
            if side in sides:
                for needle in claims.get(group, []):
                    emit(f'{group}-{needle_slug(needle)}', 'has', needles=[needle], haystack=log,
                         why='the effect has to be visible in the daemon log')
        for group, sides in (('log_not', ('go', 'rust')), ('log_not_go', ('go',)),
                             ('log_not_rust', ('rust',))):
            if side in sides:
                for needle in claims.get(group, []):
                    emit(f'{group}-{needle_slug(needle)}', 'hasnt', needles=[needle], haystack=log,
                         why='a branch that did not run must stay silent')
        if claims.get('log_any'):
            emit('any', 'any', needles=claims['log_any'], haystack=log,
                 why='a refusal has to name at least one plausible reason')
        want = entry.get('expect_exit')
        if want == 'nonzero':
            emit('exit', 'nonzero', actual=rec['exit'], why='the raw exit code, not a wrapper verdict')
        elif want is not None:
            emit('exit', 'eq', expect=want, actual=rec['exit'], why='the raw exit code of the launch')
        for role in claims.get('tcp_has', []):
            emit(f'tcp-{role}', 'eq', expect=True, actual=sock_token(role) in tcp,
                 why='the TCP listener the config named is one the live process owns')
        for role in claims.get('udp_has', []):
            emit(f'udp-{role}', 'eq', expect=True, actual=sock_token(role) in udp,
                 why='the UDP endpoint the config named is one the live process owns')
        for family, lst in (('tcp', tcp), ('udp', udp)):
            for role in claims.get(f'{family}_not', []):
                emit(f'not-{family}-{role}', 'eq', expect=False, actual=sock_token(role) in lst,
                     why=f'no {family.upper()} endpoint was opened for a port the config never named')
        if 'node_alive' in claims:
            alive = (rec.get('store') or {}).get('alive') or {}
            emit('node', 'eq', expect=claims['node_alive'], actual=len(alive.get('node_ids') or []),
                 why='the registry row exists while the daemon is alive')
        if claims.get('room_sid_present'):
            emit('room-sid', 'nonzero', actual=join.get('room'),
                 why='the join response named a room, so a participant really was admitted')
            emit('join-sid', 'nonzero', actual=join.get('sid'),
                 why='the join response named the participant whose SID the credential carries')
        if claims.get('kids_pinned') is not None:
            kids = [((j.get('rows') or [{}])[0]).get('kid')
                    for j in [join] + list(rec.get('extra_joins') or [])]
            emit('kids-count', 'eq', expect=claims.get('join_repeat') or 0, actual=len(kids),
                 why='every repeat has to have returned its own credential row')
            emit('kids-named', 'nonzero', actual=[k for k in kids if k],
                 why='a repeat whose row named no key says nothing about which key is chosen')
            emit('kids-pinned', 'eq', expect=claims['kids_pinned'][side],
                 actual=bool(kids) and all(k == kids[0] for k in kids),
                 why='whether one launch advertises the same API key on every join')
            emit('kids-known', 'eq', expect=True,
                 actual=bool(kids) and all(k in (claims.get('kids_allowed') or []) for k in kids),
                 why='a username can only name a key this node actually has in its map')
        if claims.get('store_left_clean'):
            emit('store-clean', 'eq', expect=[],
                 actual=((rec.get('store') or {}).get('after') or {}).get('keys'),
                 why='a refused start writes nothing it then has to unwind')
        if claims.get('store_no_node_after'):
            emit('node-gone', 'eq', expect=[],
                 actual=((rec.get('store') or {}).get('after') or {}).get('node_ids'),
                 why='a refused start never registers a node')
        if claims.get('release_ports'):
            post = rec.get('post_ports') or {}
            for name in sorted(post):
                emit(f'release-{name}', 'eq', expect={'tcp': 0, 'udp': 0}, actual=post[name],
                     why='the socket is released when the daemon is gone')

        emit('join', 'eq', expect='joined' if entry['ready'] else 'not-attempted',
             actual=join.get('kind'),
             why='the websocket upgrade either produced a JoinResponse or was never attempted')
        emit('ice-count', 'eq', expect=ice.get('count'), actual=len(rows),
             why='the number of ice_servers the running node put on the wire')
        if entry['ready']:
            emit('upgrade', 'eq', expect='101 Switching Protocols', actual=join.get('status'),
                 why='the pair is only real if the handshake itself switched protocols')
            emit('frames', 'nonzero', actual=join.get('frames'),
                 why='at least one binary frame has to come back before any ICE row can exist')
        for i, exp in enumerate(exp_rows):
            r = rows[i] if i < len(rows) else {}

            def actual(field, default=None):
                return r.get(field, default)

            emit(f'ice{i}-class', 'eq', expect=exp['class'], actual=actual('class'),
                 why='whether the username is an expiry pair, a base62 blob, a literal or empty')
            if 'url' in exp:
                emit(f'ice{i}-url', 'eq', expect=exp['url'],
                     actual=(actual('urls') or [None])[0],
                     why='the scheme, host, port and transport of the advertised URL')
            if 'urls' in exp:
                emit(f'ice{i}-urls', 'eq', expect=exp['urls'], actual=actual('urls'),
                     why='the whole URL list of this ICE server, in order')
            if 'url_count' in exp:
                emit(f'ice{i}-urlcount', 'eq', expect=exp['url_count'], actual=actual('url_count'),
                     why='one embedded UDP listener plus one TLS URL is two entries in one row')
            if 'ttl' in exp:
                emit(f'ice{i}-ttl', 'band', expect=exp['ttl'], actual=actual('ttl_delta'),
                     why='the expiry inside the username, minus the second the join happened')
            if 'cred_len' in exp:
                emit(f'ice{i}-credlen', 'eq', expect=exp['cred_len'], actual=actual('cred_len'),
                     why='the credential length is a property of the algorithm, not of the input')
            if 'cred_band' in exp:
                emit(f'ice{i}-credlen', 'band', expect=exp['cred_band'], actual=actual('cred_len'),
                     why='the derived password is a fixed-width encoding of a digest')
            if 'cred_sha256' in exp:
                emit(f'ice{i}-cred', 'eq', expect=exp['cred_sha256'], actual=actual('cred_sha256'),
                     why='a static credential is passed through byte for byte')
            if 'static_text' in exp:
                emit(f'ice{i}-user', 'eq', expect=exp['static_text'], actual=actual('static_text'),
                     why='a static username comes back verbatim (masked, like everything stored)')
            if exp.get('sid_ok') is not None:
                emit(f'ice{i}-sid', 'eq', expect=True, actual=actual('sid_ok'),
                     why='the participant SID inside the credential is the one this join got')
            if 'kid' in exp:
                emit(f'ice{i}-kid', 'eq', expect=exp['kid'], actual=actual('kid'),
                     why='the base62 username names the API key the token was signed with')
            if 'kid_signs' in exp:
                signed = [name for name in ('k', 'm') if actual(f'pw_{name}') == 'match']
                named = KEY_ALIAS.get(actual('kid') or '')
                emit(f'ice{i}-kid-signs', 'eq', expect=exp['kid_signs'],
                     actual=len(signed) == 1 and signed[0] == named,
                     why='the password reproduces from exactly the secret of the key the username names')
            for name, want in sorted((exp.get('hmac') or {}).items()):
                emit(f'ice{i}-hmac-{name}', 'eq', expect=want, actual=actual(f'hmac_{name}', 'skip'),
                     why='the credential either reproduces from this configured secret or it does not')
            for name, want in sorted((exp.get('pw') or {}).items()):
                emit(f'ice{i}-pw-{name}', 'eq', expect=want, actual=actual(f'pw_{name}', 'skip'),
                     why='the derived password either reproduces from this key secret or it does not')

    if entry['ready']:
        g = (entry['runs']['go'].get('join') or {}).get('rows') or []
        r2 = (entry['runs']['rust'].get('join') or {}).get('rows') or []

        def emit_pair(suffix, **fields):
            legs.append(leg(f'{pid}::{suffix}', 'pair', **fields))

        emit_pair('count', kind='same', values=[len(g), len(r2)],
                  why='both binaries advertise the same number of ICE servers to the same join')
        for i in range(min(len(g), len(r2))):
            emit_pair(f'ice{i}-urls', kind='same', values=[g[i].get('urls'), r2[i].get('urls')],
                      why='the advertised URL list is identical modulo role-token masking')
            emit_pair(f'ice{i}-class', kind='same', values=[g[i].get('class'), r2[i].get('class')],
                      why='the username shape agrees')
            emit_pair(f'ice{i}-urlcount', kind='same',
                      values=[g[i].get('url_count'), r2[i].get('url_count')],
                      why='the number of URLs in the row agrees')
            emit_pair(f'ice{i}-ttlshape', kind='same',
                      values=[g[i].get('ttl_delta') is None, r2[i].get('ttl_delta') is None],
                      why='either both usernames carry an expiry or neither does')
            if g[i].get('ttl_delta') is not None and r2[i].get('ttl_delta') is not None:
                emit_pair(f'ice{i}-ttl', kind='same',
                          values=[ttl_minute(g[i]['ttl_delta']), ttl_minute(r2[i]['ttl_delta'])],
                          why='the credential lifetime agrees to the nearest minute')
            # The embedded password is a variable-width base62 encoding of a digest, and the digest
            # covers the participant SID, which differs between the two launches by construction. A
            # pair comparison of the raw width would grade that, so each side is checked against the
            # band the profile declares and only the non-embedded widths are compared directly.
            if g[i].get('class') == r2[i].get('class') and g[i].get('class') != 'embedded':
                emit_pair(f'ice{i}-credlen', kind='same',
                          values=[g[i].get('cred_len'), r2[i].get('cred_len')],
                          why='the credential width says both used the same derivation')
            if g[i].get('class') == 'static':
                emit_pair(f'ice{i}-cred', kind='same',
                          values=[g[i].get('cred_sha256'), r2[i].get('cred_sha256')],
                          why='a passed-through secret is byte-identical on both sides')
            exp = exp_rows[i] if i < len(exp_rows) else {}
            for key in sorted((exp.get('hmac') or {})):
                a, b = g[i].get(f'hmac_{key}', 'skip'), r2[i].get(f'hmac_{key}', 'skip')
                emit_pair(f'ice{i}-hmac-{key}', kind='same', values=[a, b],
                          why='both binaries signed the same shape against the same candidate secrets')
            for key in sorted((exp.get('pw') or {})):
                a, b = g[i].get(f'pw_{key}', 'skip'), r2[i].get(f'pw_{key}', 'skip')
                emit_pair(f'ice{i}-pw-{key}', kind='same', values=[a, b],
                          why='both derived the password from the same key secret and neither from '
                               'another')
    seen = {}
    for item in legs:
        seen[item['leg']] = seen.get(item['leg'], 0) + 1
        if seen[item['leg']] > 1:
            item['leg'] = f"{item['leg']}#{seen[item['leg']] - 1}"
    return legs
