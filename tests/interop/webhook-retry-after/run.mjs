import assert from 'node:assert/strict';
import { createHash } from 'node:crypto';
import { spawn } from 'node:child_process';
import { once } from 'node:events';
import { createWriteStream } from 'node:fs';
import { mkdir, readFile, writeFile } from 'node:fs/promises';
import { createServer as createHttpsServer } from 'node:https';
import { createServer as createHttpServer } from 'node:http';
import { createServer as createNetServer } from 'node:net';
import { createSocket } from 'node:dgram';
import { createRequire } from 'node:module';
import { resolve } from 'node:path';
import { performance } from 'node:perf_hooks';

const [repository, binary, destination] = process.argv.slice(2).map(p => resolve(p));
await mkdir(destination);
const require = createRequire(resolve(repository, 'tests/interop/package.json'));
const { RoomServiceClient, WebhookReceiver } = require('livekit-server-sdk');
const useTls = process.env.WEBHOOK_TEST_TLS === '1';
const useLongRetry = process.env.WEBHOOK_TEST_LONG_RETRY === '1';
const pendingShutdown = process.env.WEBHOOK_TEST_PENDING_SHUTDOWN === '1';
assert.ok(!(useLongRetry && pendingShutdown), 'choose one extended profile');
const fixture = resolve(repository, 'crates/rtc-tls/tests/fixtures');
const sha = value => createHash('sha256').update(value).digest('hex');
const binarySha256 = sha(await readFile(binary));
const receiver = new WebhookReceiver('devkey', 'secret');
const cases = [
  { name: 'accepted-then-tcp-closed', action: 'disconnect' },
  { name: '503-past-date', status: 503, header: 'Sun, 06 Nov 1994 08:49:37 GMT' },
  { name: '429-future-date', status: 429, future: true },
  { name: '429-negative-delta', status: 429, header: '-1' },
  { name: '429-plus-zero-delta', status: 429, header: '+0' },
  { name: '503-integer-outside-i64', status: 503, header: '9223372036854775808' },
  { name: '503-multiplied-negative-duration', status: 503, header: '9223372037' },
  { name: '500-ignores-past-date', status: 500, header: 'Sun, 06 Nov 1994 08:49:37 GMT' },
];
if (useLongRetry) {
  cases.push({ name: '429-over-300-seconds', status: 429, header: '302' });
}
if (pendingShutdown) {
  cases.splice(0, cases.length, { name: 'shutdown-during-retry', status: 429, header: '302' });
}
const prefix = `webhook-oracle-${process.pid}-`;
const byRoom = new Map(cases.map(c => [prefix + c.name, { ...c, attempts: [] }]));
const errors = [];
const sockets = new Set();
const options = {
  cert: await readFile(resolve(fixture, 'server.pem')),
  key: await readFile(resolve(fixture, 'server-key.pem')),
  minVersion: 'TLSv1.3',
  maxVersion: 'TLSv1.3',
};
const handleRequest = async (req, res) => {
  try {
    const pieces = [];
    let length = 0;
    for await (const piece of req) {
      length += piece.length;
      assert.ok(length < 65536);
      pieces.push(piece);
    }
    const body = Buffer.concat(pieces);
    const event = await receiver.receive(body.toString('utf8'), req.headers.authorization);
    const row = byRoom.get(event.room?.name);
    if (!row || event.event !== 'room_started') {
      res.writeHead(200);
      res.end();
      return;
    }
    const first = row.attempts.length === 0;
    const header = row.future ? new Date(Date.now() + 6000).toUTCString() : row.header;
    const action = first ? row.action ?? 'response' : 'response';
    const status = first ? row.status ?? null : 200;
    row.attempts.push({
      at: performance.now(),
      bodySha256: sha(body),
      jwtSha256: sha(req.headers.authorization),
      event: event.event,
      id: event.id,
      verifiedByOfficialSDK: true,
      tls: req.socket.getProtocol?.() ?? null,
      status,
      action,
      sentRetryAfter: first ? header ?? null : null,
      expectedWaitMsFromDate: first && row.future ? Date.parse(header) - Date.now() : null,
    });
    if (action === 'disconnect') {
      req.socket.destroy();
      return;
    }
    if (first && header) res.setHeader('Retry-After', header);
    res.writeHead(status);
    res.end();
  } catch (error) {
    errors.push(error.message);
    if (!res.headersSent) res.writeHead(400);
    res.end();
  }
};
const endpoint = useTls ? createHttpsServer(options, handleRequest) : createHttpServer(handleRequest);
endpoint.on('connection', s => {
  sockets.add(s);
  s.once('close', () => sockets.delete(s));
});
const sleep = ms => new Promise(resolve => setTimeout(resolve, ms));
await new Promise(resolve => endpoint.listen(0, '127.0.0.1', resolve));
const tmp = createNetServer();
await new Promise(resolve => tmp.listen(0, '127.0.0.1', resolve));
const apiPort = tmp.address().port;
await new Promise(resolve => tmp.close(resolve));
const udp = createSocket('udp4');
await new Promise(resolve => udp.bind(0, '127.0.0.1', resolve));
const udpPort = udp.address().port;
await new Promise(resolve => udp.close(resolve));
const config = resolve(destination, 'config.yaml');
await writeFile(config, `port: ${apiPort}
bind_addresses: [127.0.0.1]
rtc:
  tcp_port: 0
  udp_port: ${udpPort}
webhook:
  api_key: devkey
  urls: ["${useTls ? "https" : "http"}://127.0.0.1:${endpoint.address().port}/webhook"]
logging:
  level: warn
`);
const output = createWriteStream(resolve(destination, 'server.log'));
const child = spawn(binary, ['--dev', '--config', config], {
  cwd: repository, env: { ...process.env, SSL_CERT_FILE: resolve(fixture, 'root.pem') },
  stdio: ['ignore', 'pipe', 'pipe'],
});
child.stdout.pipe(output, { end: false });
child.stderr.pipe(output, { end: false });
const exited = once(child, 'exit');
const api = new RoomServiceClient(`http://127.0.0.1:${apiPort}`, 'devkey', 'secret');
let exit = null, error = null;
let secondSignalSent = false;
try {
  let ready = false;
  for (let i = 0; i < 100 && child.exitCode === null; i++) {
    try {
      await api.listRooms();
      ready = true;
      break;
    } catch { await sleep(60); }
  }
  assert.ok(ready, 'server readiness failed');
  // Go starts serving HTTP before entering the running state used by Stop.
  if (pendingShutdown) await sleep(500);
  for (const room of byRoom.keys()) {
    const options = { name: room };
    if (useLongRetry) options.emptyTimeout = 600;
    await api.createRoom(options);
  }
  const observationDeadline = performance.now() + (useLongRetry ? 307000 : 7000);
  const requiredAttempts = pendingShutdown ? 1 : 2;
  while (performance.now() < observationDeadline &&
    !Array.from(byRoom.values()).every(r => r.attempts.length >= requiredAttempts)) {
    if (child.exitCode !== null) throw new Error(`daemon exited ${child.exitCode}`);
    await sleep(25);
  }
  for (const row of byRoom.values()) {
    assert.ok(row.attempts.length > 0, `no verified attempts for ${row.name}`);
    if (row.attempts.length !== 2) continue;
    const [first, second] = row.attempts;
    row.delayMs = second.at - first.at;
    assert.equal(second.bodySha256, first.bodySha256);
    assert.equal(second.jwtSha256, first.jwtSha256);
    assert.equal(second.id, first.id);
    assert.equal(second.status, 200);
  }
  for (const room of byRoom.keys()) await api.deleteRoom(room);
  assert.equal(errors.length, 0);
} catch (e) {
  error = e.message;
} finally {
  // Close owned receiver after the measurement to release any future retry
  // connections. Terminate only the child started by this process.
  child.kill('SIGTERM');
  const secondSignal = pendingShutdown ? setTimeout(() => {
    secondSignalSent = child.kill('SIGTERM');
  }, 1000) : null;
  const timer = setTimeout(() => child.kill('SIGKILL'), 6500);
  exit = await exited;
  clearTimeout(timer);
  clearTimeout(secondSignal);
  for (const socket of sockets) socket.destroy();
  await new Promise(resolve => endpoint.close(resolve));
  await new Promise(resolve => output.end(resolve));
  await writeFile(resolve(destination, 'execution.json'), JSON.stringify({
    status: error || errors.length ? 'ERROR' : 'OBSERVED',
    protocol: useTls ? "https" : "http",
    binary,
    binarySha256,
    process: { pid: child.pid, exit: exit[0], signal: exit[1] },
    ...(pendingShutdown ? { secondSignalSent } : {}),
    fixtureSha256: sha(await readFile(new URL(import.meta.url))),
    error, errors,
    rows: Array.from(byRoom.values()),
  }, null, 2) + '\n');
}
if (error || errors.length) process.exitCode = 1;
