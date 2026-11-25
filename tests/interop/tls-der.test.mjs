import test from 'node:test';
import assert from 'node:assert/strict';
import { createHash } from 'node:crypto';
import { mkdtemp, readFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { spawnSync } from 'node:child_process';
import { leafSpki, parseDer } from './tls-der.mjs';

// The attribution probe pins the leaf's SubjectPublicKeyInfo to the same base64
// SHA-256 that generate-tls-fixture.mjs writes from `openssl pkey -pubout -outform
// DER`. These tests hold that equivalence, because the probe is otherwise the only
// reader of the field and its first execution was on a runner, where it threw.
function der(tag, contents) {
  const length = contents.length;
  const header = length < 0x80 ? [length] : [0x80 | String(length.toString(16)).length + 1, ...Buffer.from(length.toString(16).padStart(2, '0'), 'hex')];
  return Buffer.concat([Buffer.from([tag, ...header]), contents]);
}

const oid = (bytes) => der(0x06, Buffer.from(bytes));
const oidEcPublicKey = oid([0x2a, 0x86, 0x48, 0xce, 0x3d, 0x02, 0x01]); // 1.2.840.10045.2.1
const oidRsaEncryption = oid([0x2a, 0x86, 0x48, 0x86, 0xf7, 0x0d, 0x01, 0x01, 0x01]);
const emptySeq = der(0x30, Buffer.alloc(0));

function certificate({ version, spki }) {
  const signatureAlg = der(0x30, Buffer.concat([oidEcPublicKey, oid([0x2b, 0x81, 0x04, 0x00, 0x22])]));
  const tbs = der(0x30, Buffer.concat([
    ...(version ? [der(0xa0, der(0x02, Buffer.from([0x02])))] : []),
    der(0x02, Buffer.from([0x01, 0x23])),
    signatureAlg,
    emptySeq, // issuer
    emptySeq, // validity
    emptySeq, // subject
    spki,
    der(0xa3, emptySeq), // extensions
  ]));
  return der(0x30, Buffer.concat([tbs, signatureAlg, der(0x03, Buffer.from([0x00, 0x04, 0x05, 0x06]))]));
}

function spkiVector(algOid, keyBits) {
  return der(0x30, Buffer.concat([der(0x30, Buffer.concat([algOid])), der(0x03, Buffer.concat([Buffer.from([0x00]), keyBits]))]));
}

for (const version of [true, false]) {
  test(`leafSpki returns the SubjectPublicKeyInfo, not the signature AlgorithmIdentifier (version ${version ? 'present' : 'absent'})`, () => {
    const spki = spkiVector(oidRsaEncryption, Buffer.from([0xfe, 0xdc, 0xba]));
    const leaf = certificate({ version, spki });
    const found = leafSpki(leaf);
    assert.ok(found, 'the SPKI must be located');
    assert.deepEqual(Buffer.from(found), spki);
    // The naive "first SEQUENCE inside TBSCertificate" is `signature`; pinning that
    // would report a stable but wrong value, so it is asserted apart here.
    const signatureAlg = der(0x30, Buffer.concat([oidEcPublicKey, oid([0x2b, 0x81, 0x04, 0x00, 0x22])]));
    assert.notDeepEqual(Buffer.from(found), signatureAlg);
  });
}

test('leafSpki reports nothing rather than throwing on malformed input', () => {
  assert.equal(leafSpki(Buffer.alloc(0)), null);
  assert.equal(leafSpki(Buffer.from([0x30, 0x04, 0x02, 0x01, 0x05])), null);
  // Truncated: the outer SEQUENCE claims more bytes than the buffer holds.
  assert.equal(leafSpki(Buffer.from([0x30, 0x82, 0x04, 0x00, 0x30, 0x00])), null);
  assert.equal(leafSpki(Buffer.from('not-a-certificate', 'utf8')), null);
  assert.throws(() => parseDer(Buffer.from([0x30, 0x82, 0x04, 0x00])), /past its parent|end of buffer/);
});

test('leafSpki reproduces the openssl public-key pin for EC and RSA leaves', async () => {
  const work = await mkdtemp(join(tmpdir(), 'tls-der-'));
  const openssl = args => {
    const run = spawnSync('openssl', args, { encoding: 'utf8' });
    if (run.error) throw run.error;
    if (run.status !== 0) throw new Error(`openssl ${args.join(' ')}: ${run.stderr}`);
    return run.stdout;
  };
  for (const [label, keygen] of [['ec', ['-algorithm', 'EC', '-pkeyopt', 'ec_paramgen_curve:P-256']], ['rsa', ['-algorithm', 'RSA', '-pkeyopt', 'rsa_keygen_bits:2048']]]) {
    const key = join(work, `${label}-key.pem`);
    const cert = join(work, `${label}-cert.pem`);
    const certDer = join(work, `${label}-cert.der`);
    const keyDer = join(work, `${label}-spki.der`);
    openssl(['genpkey', ...keygen, '-out', key]);
    openssl(['req', '-x509', '-new', '-key', key, '-days', '1', '-subj', '/CN=probe.test', '-out', cert]);
    openssl(['x509', '-in', cert, '-outform', 'DER', '-out', certDer]);
    openssl(['pkey', '-pubout', '-outform', 'DER', '-in', key, '-out', keyDer]);
    const derBytes = await readFile(certDer);
    const expected = createHash('sha256').update(await readFile(keyDer)).digest('base64');
    const found = leafSpki(derBytes);
    assert.ok(found, `${label}: the SPKI must be located in an openssl-generated leaf`);
    assert.equal(createHash('sha256').update(found).digest('base64'), expected, `${label}: pin must match openssl pkey -pubout`);
  }
});
