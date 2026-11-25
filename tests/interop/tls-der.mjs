// Minimal DER reader shared by the post-failure attribution probe and its unit
// test. Kept free of side effects so `node --test` can import it without running
// a probe.
export function parseDer(buffer) {
  let offset = 0;
  function readLength() {
    const first = buffer[offset++];
    if (first === undefined) throw new Error('der: length byte past end of buffer');
    if (first < 0x80) return first;
    const count = first & 0x7f;
    let length = 0;
    for (let i = 0; i < count; i++) length = (length << 8) | buffer[offset++];
    return length;
  }
  function readNode() {
    const nodeStart = offset;
    const tag = buffer[offset++];
    const length = readLength();
    const start = offset;
    const end = offset + length;
    if (end > buffer.length) throw new Error('der: child extends past its parent');
    const node = { tag, bytes: buffer.subarray(start, end), full: buffer.subarray(nodeStart, end), value: null, children: [] };
    if ((tag & 0x20) !== 0) {
      while (offset < end) node.children.push(readNode());
    } else {
      node.value = buffer.subarray(start, end);
      offset = end;
    }
    return node;
  }
  return readNode();
}

// SubjectPublicKeyInfo DER is the openssl `pkey -pubout -outform DER` blob that
// generate-tls-fixture.mjs pins, so both sides produce the same base64 SHA-256.
// That pin covers the whole TLV, tag and length included, not the SEQUENCE content.
// TBSCertificate is version [0] EXPLICIT (optional), serialNumber INTEGER, signature,
// issuer, validity, subject, subjectPublicKeyInfo, so the SPKI is the fifth SEQUENCE
// after the serial - the first SEQUENCE is `signature`, which is a different key.
export function leafSpki(leafDer) {
  try {
    const tbs = parseDer(leafDer)?.children?.find(child => child.tag === 0x30);
    if (!tbs) return null;
    let index = tbs.children[0]?.tag === 0xa0 ? 1 : 0;
    if (tbs.children[index++]?.tag !== 0x02) return null;
    for (let field = 0; field < 4; field++) {
      if (tbs.children[index++]?.tag !== 0x30) return null;
    }
    const spki = tbs.children[index];
    return spki?.tag === 0x30 ? spki.full : null;
  } catch {
    return null;
  }
}
