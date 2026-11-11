// Normalize ephemeral values while preserving their cross-message relationships.
export function normalizer() {
  const identifiers = new Map();
  const id = value => {
    if (!identifiers.has(value)) identifiers.set(value, `<id-${identifiers.size + 1}>`);
    return identifiers.get(value);
  };
  const text = value => value
    .replace(/\b(?:PA|TR|RM|ND|EV|CO)_[A-Za-z0-9]+\b/g, id)
    .replace(/\b[0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12}\b/gi, id)
    .replace(/(^|\r?\n)(a=ice-(?:ufrag|pwd):)[^\r\n]+/g, '$1$2<redacted>')
    .replace(/(^|\r?\n)(a=fingerprint:)[^\r\n]+/g, '$1$2<fingerprint>')
    .replace(/(^|\r?\n)(o=[^ ]+ )\d+ \d+ (IN IP[46]) [^\r\n]+/g, '$1$2<session> <version> $3 <address>')
    .replace(/(^|\r?\n)(c=IN IP[46]) [^\r\n]+/g, '$1$2 <address>')
    .replace(/(^|\r?\n)((?:a=)?candidate:)([^\r\n]+)/g, (_, newline, prefix, rest) => {
      const fields = rest.split(' '); fields[0] = '<foundation>'; fields[4] = '<address>'; fields[5] = '<port>';
      for (let i = 6; i < fields.length - 1; i++) {
        if (fields[i] === 'raddr') fields[i + 1] = '<address>';
        if (fields[i] === 'rport') fields[i + 1] = '<port>';
        if (fields[i] === 'ufrag') fields[i + 1] = '<redacted>';
      }
      return newline + prefix + fields.join(' ');
    })
    .replace(/(^|\r?\n)(m=\w+) \d+ /g, '$1$2 <port> ');
  function normalize(value, key = '') {
    if (typeof value === 'string') {
      if (/token|credential|password|secret/i.test(key)) return '<redacted>';
      if (/address$|^ip$/i.test(key)) return '<address>';
      if (key === 'candidate' && value.startsWith('{')) {
        try { return JSON.stringify(normalize(JSON.parse(value))); } catch { return '<invalid candidate>'; }
      }
      return text(value);
    }
    if (Array.isArray(value)) return value.map(item => normalize(item, key));
    if (value && typeof value === 'object') return Object.fromEntries(Object.entries(value).map(([k, v]) => [k, normalize(v, k)]));
    return value;
  }
  return normalize;
}
