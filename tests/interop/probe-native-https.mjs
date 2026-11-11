// Production listener certificate checks. This does not modify the LiveKit SDK.
import assert from 'node:assert/strict';
import https from 'node:https';
import net from 'node:net';
import { readFile, writeFile, mkdir } from 'node:fs/promises';
import { createHash, X509Certificate } from 'node:crypto';
import { fileURLToPath } from 'node:url';
const output = fileURLToPath(new URL('./output/', import.meta.url));
const caPath = process.env.TLS_CA_FILE || fileURLToPath(new URL('./output/tls/root.pem', import.meta.url));
const target = new URL((process.env.LIVEKIT_URL || 'wss://127.0.0.1:8443').replace(/^wss:/, 'https:'));
assert.equal(target.protocol, 'https:');
const ca = await readFile(caPath);
const label = process.env.PROBE_LABEL || '';
assert.match(label, /^[a-zA-Z0-9_-]*$/);
function request(options = {}) {
  return new Promise((resolve, reject) => {
    const req = https.get(target, { ca, rejectUnauthorized: true, agent: false, ...options }, res => {
      const socket = res.socket;
      const certificate = new X509Certificate(socket.getPeerCertificate().raw);
      const report = { status: res.statusCode, authorized: socket.authorized,
        tlsVersion: socket.getProtocol(), cipher: socket.getCipher().standardName,
        endpointKeyType: certificate.publicKey.asymmetricKeyType, fingerprint256: certificate.fingerprint256 };
      res.resume(); res.on('end', () => resolve(report));
    });
    req.setTimeout(5000, () => req.destroy(new Error('HTTPS probe timed out')));
    req.on('error', reject);
  });
}
await mkdir(output, { recursive: true });
const evidence = { scenario: 'native-tokio-axum-https-certificate-validation', status: 'RUNNING',
  caSha256: createHash('sha256').update(ca).digest('hex'), startedAt: new Date().toISOString() };
try {
  evidence.valid = await request();
  assert.equal(evidence.valid.status, 200); assert.equal(evidence.valid.authorized, true);
  await assert.rejects(request({ ca: [] }), /certificate|issuer|self.signed|verify/i);
  evidence.untrustedRejected = true;
  await assert.rejects(request({ servername: 'wrong-hostname.invalid' }), /hostname|altnames|certificate/i);
  evidence.wrongNameRejected = true;
  if (process.env.PLAIN_HTTP_PORT) {
    await assert.rejects(new Promise((resolve, reject) => {
      const socket = net.connect({ host: target.hostname, port: Number(process.env.PLAIN_HTTP_PORT) });
      socket.on('connect', () => { socket.destroy(); resolve(); });
      socket.on('error', reject); socket.setTimeout(2000, () => { socket.destroy(); reject(new Error('plaintext check timed out')); });
    }), error => error.code === 'ECONNREFUSED');
    evidence.disabledPlaintextPortClosed = true;
  }
  evidence.status = 'PASS'; console.log('PASS: native HTTPS, trusted CA, rejection of wrong name/untrusted CA');
} catch (error) {
  evidence.status = 'FAIL'; evidence.error = String(error.stack || error); console.error(error); process.exitCode = 1;
} finally {
  evidence.finishedAt = new Date().toISOString();
  await writeFile(output + (label ? label + '-' : '') + 'native-https-probe.json', JSON.stringify(evidence, null, 2));
}
