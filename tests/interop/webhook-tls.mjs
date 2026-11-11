// Actual binary -> verified HTTPS -> unmodified official WebhookReceiver.
import assert from 'node:assert/strict';
import { createHash } from 'node:crypto';
import { createServer } from 'node:https';
import { spawn } from 'node:child_process';
import { once } from 'node:events';
import { createWriteStream } from 'node:fs';
import { readFile, writeFile, mkdir } from 'node:fs/promises';
import { resolve, dirname } from 'node:path';
import { fileURLToPath } from 'node:url';
import { RoomServiceClient, WebhookReceiver } from 'livekit-server-sdk';

const root = dirname(fileURLToPath(import.meta.url));
const output = resolve(root, 'output'); await mkdir(output, { recursive: true });
const fixture = resolve(output, 'tls');
const serverRoot = resolve(root, '../..');
const binary = resolve(process.env.LIVEKIT_BIN || resolve(serverRoot, 'target/debug/livecloud'));
const apiPort = Number(process.env.WEBHOOK_TEST_PORT || 8280);
const receiver = new WebhookReceiver('devkey', 'secret');
const attempts = [], errors = [];
const report = { scenario: 'binary-https-webhook-official-receiver-retry', status: 'RUNNING',
  binarySha256: createHash('sha256').update(await readFile(binary)).digest('hex'), attempts, errors };
let rejectedFirst = false;
// Keep the original AES-128 regression deterministic as the client adds more suites.
// The independent provider matrix covers all expanded algorithms.
const endpoint = createServer({ key: await readFile(resolve(fixture, 'server-key.pem')), cert: await readFile(resolve(fixture, 'server.pem')), minVersion: 'TLSv1.3', maxVersion: 'TLSv1.3', ciphers: 'TLS_AES_128_GCM_SHA256' }, async (req, res) => {
  try {
    const chunks = [];
    for await (const chunk of req) chunks.push(chunk);
    const event = await receiver.receive(Buffer.concat(chunks).toString(), req.headers.authorization);
    const retry = !rejectedFirst; rejectedFirst = true;
    attempts.push({ event: event.event, id: event.id, room: event.room?.name, verifiedByOfficialSDK: true,
      protocol: req.socket.getProtocol(), cipher: req.socket.getCipher().standardName, status: retry ? 503 : 200 });
    res.writeHead(retry ? 503 : 200); res.end();
  } catch (error) { errors.push(String(error)); res.writeHead(400); res.end(); }
});
await new Promise(resolve => endpoint.listen(0, '127.0.0.1', resolve));
const webhookPort = endpoint.address().port;
const config = resolve(output, 'webhook-tls-config.yaml');
await writeFile(config, `port: ${apiPort}\nbind_addresses: [127.0.0.1]\nrtc:\n  tcp_port: ${apiPort + 1}\n  udp_port: ${apiPort + 2}\nwebhook:\n  api_key: devkey\n  urls: ["https://localhost:${webhookPort}/webhook"]\nlogging:\n  level: info\n`);
const log = createWriteStream(resolve(output, 'webhook-tls-server.log'));
const child = spawn(binary, ['--dev', '--config', config], { cwd: serverRoot,
  env: { ...process.env, SSL_CERT_FILE: resolve(fixture, 'root.pem') }, stdio: ['ignore', 'pipe', 'pipe'] });
child.stdout.pipe(log); child.stderr.pipe(log);
const sleep = ms => new Promise(resolve => setTimeout(resolve, ms));
async function wait(predicate, what, ms = 20000) {
  const started = Date.now();
  while (!predicate()) {
    if (Date.now() - started > ms) throw new Error(`timeout waiting for ${what}`);
    if (child.exitCode !== null) throw new Error(`server exited: ${child.exitCode}`);
    await sleep(100);
  }
}
const api = new RoomServiceClient(`http://127.0.0.1:${apiPort}`, 'devkey', 'secret');
const room = `tls-webhook-${Date.now()}`;
try {
  let ready = false;
  for (let i = 0; i < 100; i++) {
    try { await api.listRooms(); ready = true; break; } catch { await sleep(100); }
  }
  assert.ok(ready, 'server readiness failed');
  await api.createRoom({ name: room });
  await wait(() => attempts.filter(a => a.event === 'room_started' && a.room === room).length >= 2, 'signed retry');
  const started = attempts.filter(a => a.event === 'room_started' && a.room === room);
  assert.equal(started[0].status, 503); assert.equal(started[1].status, 200);
  assert.equal(started[0].id, started[1].id, 'retry changed event identity');
  await api.deleteRoom(room);
  await wait(() => attempts.some(a => a.event === 'room_finished' && a.room === room), 'room_finished delivery');
  assert.equal(errors.length, 0);
  assert.ok(attempts.every(a => a.protocol === 'TLSv1.3' && a.cipher === 'TLS_AES_128_GCM_SHA256'));
  report.status = 'PASS'; console.log('PASS: verified TLS 1.3 webhook + official SDK signature/hash + retry + room event order');
} catch (error) {
  report.status = 'FAIL'; report.error = String(error.stack || error); process.exitCode = 1; console.error(error);
} finally {
  await api.deleteRoom(room).catch(() => {});
  const ended = once(child, 'exit'); child.kill('SIGTERM');
  const timer = setTimeout(() => child.kill('SIGKILL'), 5000);
  await ended; clearTimeout(timer);
  await new Promise(resolve => endpoint.close(resolve)); log.end();
  report.finishedAt = new Date().toISOString();
  await writeFile(resolve(output, 'webhook-tls.json'), JSON.stringify(report, null, 2));
}
