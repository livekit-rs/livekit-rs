

class Masker(C104.Masker):
    """C104's masker (paths, ports, store password, key secrets, timestamps, letter addresses, and the
    unallocated `127.0.0.1:<port>` -> `<EPHEM>` sweep) plus the two vocabularies this lane puts on the
    wire: the participant SIDs a join generates, and the static credential pair from the YAML."""

    def secrets(self, text: str) -> str:
        text = super().secrets(text)
        for real, alias in SECRET_ALIASES:
            text = text.replace(real, f'<{alias}>')
        return text

    def text(self, text: str, keep_node_ids: bool = True) -> str:
        out = super().text(text, keep_node_ids=keep_node_ids)
        out = re.sub(r'\bPA_[0-9A-Za-z]{6,16}\b', '<PA>', out)
        for real, alias in STATIC_ALIASES:
            out = out.replace(real, f'<{alias}>')
        return out


# --------------------------------------------------------------------------- the join probe
def join_probe(port: int, kid: str, key_secret: str, secrets: dict, timeout: float = 6.0) -> dict:
    """One real RFC 6455 join against the live daemon, decoded with the standard library only.

    `secrets` names every candidate this profile configured - the external secret(s) a REST credential
    could have been signed with, and the API-key secret(s) an embedded password could have been derived
    from. Each is tried and its own verdict stored, so "matched nothing" stays distinguishable from
    "nobody looked", and the foreign key is on the list too: a row that matches everything is a row
    whose digest was never compared to anything.
    """
    tok = token(kid, key_secret)
    path = f'/rtc/v1?join_request={JOIN_REQUEST_PARAM}&protocol=15'
    try:
        sock, first, rest = ws_open(port, path, {'Authorization': 'Bearer ' + tok}, timeout=timeout)
    except OSError as exc:
        return {'kind': err_kind(exc), 'status': '', 'frames': 0, 'rows': [], 'join_ms': 0,
                'sid': None, 'room': None}
    started = time.time()
    state = {'buf': rest}
    out = {'kind': 'no-join-frame', 'status': first.split(' ', 1)[1] if ' ' in first else first,
           'frames': 0, 'rows': [], 'sid': None, 'room': None, 'version': None}
    try:
        if ' 101 ' not in first:
            out['kind'] = 'upgrade-refused'
            return out
        for _ in range(8):
            try:
                op, payload = ws_read_frame(sock, state)
            except (ConnectionError, OSError):
                break
            out['frames'] += 1
            if op != 2:
                continue
            try:
                outer = pb_decode(payload)
            except ValueError:
                continue
            for _, join in outer.get(1, []):
                try:
                    d = pb_decode(join)
                except ValueError:
                    continue
                if 5 not in d:
                    continue
                now = int(time.time())
                sid = (pb_strings(join, 2) and pb_strings(d[2][0][1], 1) or [None])[0] \
                    if 2 in d else None
                out['sid'] = sid
                out['room'] = (pb_strings(d[1][0][1], 1) or [None])[0] if 1 in d else None
                out['version'] = (pb_strings(d[4][0][1], 1) or [None])[0] if 4 in d else None
                for _, ice in d[5]:
                    urls = pb_strings(ice, 1)
                    username = (pb_strings(ice, 2) or [''])[0]
                    credential = (pb_strings(ice, 3) or [''])[0]
                    shape = row_class(username)
                    row = {'urls': urls, 'class': shape['class'],
                           'username_sha256': hashlib.sha256(username.encode()).hexdigest()[:16],
                           'cred_sha256': hashlib.sha256(credential.encode()).hexdigest()[:16],
                           'cred_len': len(credential), 'ttl_delta': None, 'sid_ok': None,
                           'kid': shape.get('kid'), 'url_count': len(urls)}
                    if shape['class'] == 'static':
                        # A literal username is the only one that is not derived from something the
                        # node knows, so it can be carried in the clear and still be a claim: the
                        # masker turns the fixture value into <USR> before the row is stored.
                        row['static_text'] = shape['text']
                    if shape['expiry'] is not None:
                        row['ttl_delta'] = shape['expiry'] - now
                        row['sid_ok'] = (shape['sid'] == sid)
                    for name, candidate in secrets.items():
                        data = candidate.encode() if isinstance(candidate, str) else candidate
                        if shape['class'] == 'rest':
                            row[f'hmac_{name}'] = 'match' if rest_credential(
                                data, username) == credential else 'no'
                        elif shape['class'] == 'embedded':
                            row[f'pw_{name}'] = 'match' if embedded_password(
                                candidate, shape['sid'], shape['expiry']) == credential else 'no'
                    out['rows'].append(row)
                out['kind'] = 'joined'
                return out
        return out
    finally:
        out['join_ms'] = int((time.time() - started) * 1000)
        try:
            sock.close()
        except OSError:
            pass


# --------------------------------------------------------------------------- context and configs
def build_ctx(out_dir: Path, hold: float) -> dict:
    work = out_dir / 'work'
    fx = work / 'fixtures'
    (work / 'configs').mkdir(parents=True, exist_ok=True)
    (work / 'logs').mkdir(parents=True, exist_ok=True)
    fx.mkdir(parents=True, exist_ok=True)
    used: set = set()
    ctx = {}
    port_roles = {}
    for role in ('http', 'store', 'tu1', 'tu2', 'tls1'):
        port = C102.alloc_port(used)
        ctx[f'{role}_port'] = port
        port_roles[port] = role.upper()
    certs = {'A': C103.gen_cert(fx, 'c105a')}
    return dict(work=work, fx=fx, hold=hold, port_roles=port_roles, used=used, certs=certs, **ctx)


def secret_file(ctx, name: str, data: bytes, mode: int) -> str:
    path = ctx['fx'] / name
    path.write_bytes(data)
    os.chmod(path, mode)
    return str(path)


def ts_item(host=HOST, port=3478, protocol='udp', username=None, credential=None, secret=None,
            secret_file_path=None, ttl=None) -> str:
    """One `rtc.turn_servers` list entry. A None field is left out of the YAML entirely, which is how
    the credential-mode union gets tested instead of assumed: `secret: ""` and an omitted `secret` are
    different YAML, and only the second one is the blank the upstream branch actually checks."""
    out = f'    - host: {host}\n'
    out += f'      port: {port}\n'
    if protocol is not None:
        out += f'      protocol: {protocol}\n'
    if username is not None:
        out += f'      username: "{username}"\n'
    if credential is not None:
        out += f'      credential: "{credential}"\n'
    if secret is not None:
        out += f'      secret: "{secret}"\n'
    if secret_file_path is not None:
        out += f'      secret_file: {secret_file_path}\n'
    if ttl is not None:
        out += f'      ttl: {ttl}\n'
    return out


def ts_block(*items: str) -> str:
    return '  turn_servers:\n' + ''.join(items)


def cfg(ctx, keys: dict | None = None, ts_items: str = '', turn: str = '',
        node_ip: str = '127.0.0.1', stun: str = '') -> str:
    """The whole YAML: fixed loopback node, owned store, this lane's key map, and whichever of the two
    TURN surfaces the profile is about. `rtc.tcp_port: 0` keeps the TCP mux off, so the only listener
    the lane owns besides HTTP is the one a profile names."""
    ks = keys if keys is not None else {KID: KSEC}
    body = (f'port: {ctx["http_port"]}\n'
            f'bind_addresses: [127.0.0.1]\n'
            f'redis:\n  address: 127.0.0.1:{ctx["store_port"]}\n')
    body += 'keys:\n' + ''.join(f'  {k}: {v}\n' for k, v in ks.items())
    body += 'rtc:\n  tcp_port: 0\n'
    if node_ip:
        body += f'  node_ip: {node_ip}\n'
    if stun:
        body += f'  stun_servers:\n{stun}'
    if ts_items:
        body += ts_items
    if turn:
        body += turn
    return body


def turn_block(ctx, enabled=True, domain=DOMAIN, tls=None, tu=None, ttl_seconds=None,
               material='A') -> str:
    """The embedded `turn:` section - top level, not under `rtc:`. Ports are named by role, so the
    receipt carries `<TU1>`/`<TLS1>` instead of a number that would collide with the masker."""
    out = 'turn:\n'
    out += f'  enabled: {"true" if enabled else "false"}\n'
    if domain is not None:
        out += f'  domain: {domain}\n'
    if tls is not None:
        out += f'  tls_port: {ctx[f"{tls.lower()}_port"] if isinstance(tls, str) else tls}\n'
    if tu is not None:
        out += f'  udp_port: {ctx[f"{tu.lower()}_port"] if isinstance(tu, str) else tu}\n'
    if ttl_seconds is not None:
        out += f'  ttl_seconds: {ttl_seconds}\n'
    if material is not None and tls is not None:
        out += f'  cert_file: {ctx["certs"][material]["cert"]}\n'
        out += f'  key_file: {ctx["certs"][material]["key"]}\n'
    return out
