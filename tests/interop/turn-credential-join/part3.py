

# --------------------------------------------------------------------------- row expectations
def mask_join(raw: dict, masker) -> dict:
    """The join record before it can be stored: the URLs carry the node's own loopback port, and a
    static credential pair is a literal from the YAML, so both go through the masker. Digests,
    lengths and verdicts stay - they are the claims."""
    out = {k: v for k, v in raw.items() if k not in ('rows', 'sid')}
    out['sid'] = masker.text(raw['sid']) if raw.get('sid') else raw.get('sid')
    rows = []
    for r in raw['rows']:
        m = dict(r)
        m['urls'] = [masker.text(u) for u in r['urls']]
        if m.get('kid') is None:
            m.pop('kid', None)
        if 'static_text' in m:
            m['static_text'] = masker.text(m['static_text'])
        rows.append(m)
    out['rows'] = rows
    return out


def band(value: int, slack: int = 3) -> list:
    """A TTL is a clock difference, so every ttl claim is a band. Three seconds either side still
    separates 120 from 300 from 600 from 86400, which is the whole discrimination being claimed."""
    return [value - slack, value + slack]


DEFAULT_STUN_URLS = ['stun:global.stun.twilio.com:3478', 'stun:stun.l.google.com:19302',
                     'stun:stun1.l.google.com:19302']


def row(url=None, urls=None, cls='rest', ttl=None, hmac=None, pw=None, sid_ok=None, kid=None,
        url_count=None, static_text=None, cred_len=None, cred_band=None, cred_sha256=None,
        kid_signs=None) -> dict:
    r = {'class': cls}
    if url is not None:
        r['url'] = url
    if urls is not None:
        r['urls'] = list(urls)
    if ttl is not None:
        r['ttl'] = band(ttl)
    if cred_band is not None:
        r['cred_band'] = cred_band
    if cred_sha256 is not None:
        r['cred_sha256'] = cred_sha256
    if hmac is not None:
        r['hmac'] = dict(hmac)
    if pw is not None:
        r['pw'] = dict(pw)
    if sid_ok is not None:
        r['sid_ok'] = sid_ok
    if kid is not None:
        r['kid'] = kid
    if kid_signs is not None:
        r['kid_signs'] = kid_signs
    if url_count is not None:
        r['url_count'] = url_count
    if static_text is not None:
        r['static_text'] = static_text
    if cred_len is not None:
        r['cred_len'] = cred_len
    return r


# --------------------------------------------------------------------------- profiles
