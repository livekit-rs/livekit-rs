// Development certificates only. Output and private keys are ignored by Git.
import { execFileSync } from 'node:child_process';
import { mkdir, writeFile, chmod } from 'node:fs/promises';
import { createHash } from 'node:crypto';
import { fileURLToPath } from 'node:url';
const keyType = process.env.TLS_KEY_TYPE || 'ec';
if (!['ec', 'rsa'].includes(keyType)) throw new Error('TLS_KEY_TYPE must be ec or rsa');
const fixtureName = process.env.TLS_FIXTURE_NAME || 'tls';
if (!/^[a-zA-Z0-9_-]+$/.test(fixtureName)) throw new Error('Invalid fixture directory name');
const output = fileURLToPath(new URL(`./output/${fixtureName}/`, import.meta.url));
await mkdir(output, { recursive: true, mode: 0o700 });
const openssl = (...args) => execFileSync(process.env.OPENSSL || 'openssl', args, { cwd: output, stdio: ['ignore', 'pipe', 'pipe'] });
for (const name of ['root', 'server']) {
  if (keyType === 'rsa') openssl('genpkey', '-algorithm', 'RSA', '-pkeyopt', 'rsa_keygen_bits:2048', '-out', `${name}-key.pem`);
  else openssl('genpkey', '-algorithm', 'EC', '-pkeyopt', 'ec_paramgen_curve:P-256', '-out', `${name}-key.pem`);
  await chmod(output + `${name}-key.pem`, 0o600);
}
openssl('req', '-new', '-x509', '-key', 'root-key.pem', '-out', 'root.pem', '-days', '7', '-sha256',
  '-subj', '/CN=livekit-rs local test root', '-addext', 'basicConstraints=critical,CA:TRUE,pathlen:0', '-addext', 'keyUsage=critical,keyCertSign,cRLSign');
openssl('req', '-new', '-key', 'server-key.pem', '-out', 'server.csr', '-subj', '/CN=turn.localhost');
await writeFile(output + 'server.ext', 'basicConstraints=critical,CA:FALSE\nkeyUsage=critical,digitalSignature\nextendedKeyUsage=serverAuth\nsubjectAltName=DNS:turn.localhost,DNS:localhost,IP:127.0.0.1\n');
openssl('x509', '-req', '-in', 'server.csr', '-CA', 'root.pem', '-CAkey', 'root-key.pem', '-CAcreateserial', '-out', 'server.pem', '-days', '7', '-sha256', '-extfile', 'server.ext');
if (keyType === 'rsa') {
  openssl('pkey', '-in', 'server-key.pem', '-traditional', '-out', 'server-rsa-pkcs1-key.pem');
  await chmod(output + 'server-rsa-pkcs1-key.pem', 0o600);
}
const spki = openssl('pkey', '-in', 'server-key.pem', '-pubout', '-outform', 'DER');
const pin = createHash('sha256').update(spki).digest('base64');
await writeFile(output + 'spki-sha256.txt', pin + '\n');
console.log(`Generated 7-day local-only ${keyType.toUpperCase()} fixtures in ${fixtureName}. TLS_SPKI=${pin}`);
