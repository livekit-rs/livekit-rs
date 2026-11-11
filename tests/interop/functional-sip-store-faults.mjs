/** Real dedicated-Redis SIP store fault differential. Never mock CatalogError. */
import assert from 'node:assert/strict';
import { spawn } from 'node:child_process';
import { createRequire } from 'node:module';
import { mkdir, writeFile } from 'node:fs/promises';
import { join, dirname } from 'node:path';
import { fileURLToPath } from 'node:url';

const interop = process.env.LIVEKIT_INTEROP_DIR || dirname(fileURLToPath(import.meta.url));
const require = createRequire(join(interop, 'package.json'));
const { AccessToken } = require('livekit-server-sdk');
const http = [process.env.REFERENCE_URL, process.env.LIVEKIT_URL];
function parsed(value) { try { return value ? new URL(value) : null; } catch { throw new Error('Fixture URL is invalid'); } }
const broker = [process.env.REFERENCE_REDIS_URL, process.env.LIVEKIT_REDIS_URL].map(parsed);
assert.ok(http.every(Boolean) && broker.every(Boolean), 'two real HTTP and two explicit Redis URLs required');
assert.equal(process.env.SIP_FAULT_DEDICATED, 'true', 'dedicated disposable broker namespaces must be explicitly acknowledged');
assert.ok(http[0] !== http[1], 'two distinct HTTP endpoints required');
for (const value of http) { const url = parsed(value); assert.ok(!url.username && !url.password, 'HTTP URL must not embed credentials'); }
for (const url of broker) assert.equal(url.protocol, 'redis:', 'this fixture covers native Redis, not TLS/Sentinel/Cluster');
const namespace = url => `${url.hostname}:${url.port || '6379'}${url.pathname || '/0'}`;
assert.notEqual(namespace(broker[0]), namespace(broker[1]), 'official and Rust Redis namespaces must be separate');
const key = 'sip_inbound_trunk';
const field = `ST_f04_fault_${process.pid}`;
const marker = `f04-owned-wrongtype-${process.pid}`;
const owned = [false, false];
const evidence = { profile: 'sip-config-real-store-faults', status: 'RUNNING', startedAt: new Date().toISOString(),
  source: { server: 'e5655ac20267fa5005dccaf3f0e3bfc3ea688c07', protocol: '9bb708cdb81918a6c63dd25e915b4644310b45b6' },
  scope: 'Actual dedicated Redis WRONGTYPE and corrupt protobuf through public SIP HTTP; error parity, cleanup and healthy follow-up only. No mock store, no routing/worker/call success claim.',
  cases: [], cleanup: [], differences: [] };

async function redis(server, command, input) {
  const url = broker[server];
  const db = url.pathname.slice(1) || '0';
  assert.match(db, /^\d+$/);
  const args = ['--raw', '-h', url.hostname, '-p', url.port || '6379', '-n', db];
  if (url.username) args.push('--user', decodeURIComponent(url.username));
  if (input !== undefined) args.push('-x');
  args.push(...command);
  // Password is environment-only, never command-line argument or evidence.
  const env = { ...process.env };
  if (url.password) env.REDISCLI_AUTH = decodeURIComponent(url.password); else delete env.REDISCLI_AUTH;
  return new Promise((resolve, reject) => {
    const child = spawn(process.env.REDIS_CLI || 'redis-cli', args, { env, stdio: ['pipe', 'pipe', 'pipe'] });
    let output = Buffer.alloc(0), failed = false;
    const timer = setTimeout(() => { failed = true; child.kill('SIGTERM'); reject(new Error('Redis fixture command timed out')); }, 5000);
    child.on('error', () => { clearTimeout(timer); reject(new Error('Redis fixture process could not start')); });
    child.stdout.on('data', data => {
      if (output.length + data.length > 8192) { failed = true; child.kill('SIGTERM'); reject(new Error('Redis fixture output bound exceeded')); }
      else output = Buffer.concat([output, data]);
    });
    // Do not persist arbitrary stderr: it can include connection endpoints.
    child.stderr.on('data', () => {});
    child.on('close', code => {
      clearTimeout(timer);
      if (failed) return;
      if (code !== 0) return reject(new Error('Redis fixture command failed'));
      const result = output.toString('utf8').trim();
      if (/^(ERR|WRONGTYPE|NOAUTH|WRONGPASS|NOPERM)\b/.test(result)) return reject(new Error('Redis fixture command returned a server error'));
      resolve(result);
    });
    child.stdin.on('error', () => {});
    child.stdin.end(input);
  });
}
async function jwt(server) {
  const apiKey = server === 0 ? process.env.REFERENCE_API_KEY || process.env.LIVEKIT_API_KEY || 'devkey' : process.env.LIVEKIT_API_KEY || 'devkey';
  const secret = server === 0 ? process.env.REFERENCE_API_SECRET || process.env.LIVEKIT_API_SECRET || 'secret' : process.env.LIVEKIT_API_SECRET || 'secret';
  const token = new AccessToken(apiKey, secret, { identity: 'sip-store-fault-fixture' }); token.addSIPGrant({ admin: true });
  return token.toJwt();
}
async function call(server, method, body) {
  const response = await fetch(`${http[server].replace(/\/$/, '')}/twirp/livekit.SIP/${method}`, {
    method: 'POST', headers: { 'content-type': 'application/json', authorization: `Bearer ${await jwt(server)}` },
    body: JSON.stringify(body), signal: AbortSignal.timeout(5000),
  });
  const text = await response.text();
  let bodyValue; try { bodyValue = JSON.parse(text); } catch { throw new Error('SIP fault response is not JSON'); }
  return { status: response.status, type: response.headers.get('content-type'), body: bodyValue };
}
function record(name, reference, rust) {
  evidence.cases.push({ name, reference, rust });
  try { assert.deepEqual(rust, reference); } catch { evidence.differences.push({ name, reference, rust }); }
}
async function absent(server) {
  assert.equal(await redis(server, ['EXISTS', key]), '0', 'fixture refuses to modify any existing broker key');
}
async function removeOwned(server) {
  if (!owned[server]) return;
  const type = await redis(server, ['TYPE', key]);
  if (type === 'none') { owned[server] = false; evidence.cleanup.push({ server, restoredOriginalAbsence: true, removed: 0 }); return; }
  // Guard every deletion against unexpected concurrent ownership changes.
  if (owned[server] === 'wrongtype') {
    assert.equal(type, 'string', 'cleanup refuses a foreign key type');
    assert.equal(await redis(server, ['GET', key]), marker, 'cleanup refuses a foreign string value');
  } else {
    assert.equal(type, 'hash', 'cleanup refuses a foreign key type');
    assert.equal(await redis(server, ['HLEN', key]), '1', 'cleanup refuses extra fields');
    assert.equal(await redis(server, ['HSTRLEN', key, field]), '1', 'cleanup requires its own exact field');
  }
  const removed = await redis(server, ['DEL', key]);
  owned[server] = false;
  evidence.cleanup.push({ server, restoredOriginalAbsence: (await redis(server, ['EXISTS', key])) === '0', removed: Number(removed) });
  assert.equal(await redis(server, ['EXISTS', key]), '0');
}
try {
  for (const server of [0, 1]) { assert.equal(await redis(server, ['PING']), 'PONG'); await absent(server); }
  const baseline = await Promise.all([0, 1].map(server => call(server, 'ListSIPInboundTrunk', {})));
  record('healthy-empty-before', ...baseline);
  for (const result of baseline) { assert.equal(result.status, 200); assert.deepEqual(result.body.items, []); }
  for (const kind of ['wrongtype', 'corrupt-protobuf']) {
    try {
      for (const server of [0, 1]) {
        await absent(server);
        // Mark before I/O: a command can succeed at Redis but lose its reply.
        owned[server] = kind;
        if (kind === 'wrongtype') assert.equal(await redis(server, ['SET', key, marker, 'NX']), 'OK');
        else assert.equal(await redis(server, ['HSET', key, field], Buffer.from([255])), '1');
      }
      const responses = await Promise.all([0, 1].map(server => call(server,
        kind === 'wrongtype' ? 'ListSIPInboundTrunk' : 'GetSIPInboundTrunk',
        kind === 'wrongtype' ? {} : { sip_trunk_id: field })));
      record(kind, ...responses);
      // The exact message comes from the real official run, not a guessed
      // Redis/protobuf error string or a fabricated Rust CatalogError.
      assert.equal(responses[0].status, 500, 'official backend fault must actually occur');
      assert.ok(typeof responses[0].body.code === 'string' && responses[0].body.code.length > 0, 'actual official Twirp error code required');
    } finally { for (const server of [0, 1]) await removeOwned(server); }
    const healthy = await Promise.all([0, 1].map(server => call(server, 'ListSIPInboundTrunk', {})));
    record(`healthy-after-${kind}`, ...healthy);
    for (const result of healthy) { assert.equal(result.status, 200); assert.deepEqual(result.body.items, []); }
  }
  assert.equal(evidence.differences.length, 0, 'actual fault envelopes must match exactly');
  evidence.status = 'PASS';
} catch (error) {
  evidence.status = 'FAIL'; evidence.error = { name: error.name, message: error.message }; process.exitCode = 1;
} finally {
  for (const server of [0, 1]) {
    try { await removeOwned(server); } catch { evidence.status = 'FAIL'; process.exitCode = 1; evidence.cleanup.push({ server, restoredOriginalAbsence: false, error: 'Redis cleanup failed' }); }
  }
  evidence.finishedAt = new Date().toISOString();
  const output = process.env.SIP_FAULT_EVIDENCE || join(interop, 'output/functional-sip-store-faults.json');
  await mkdir(dirname(output), {recursive:true});
  await writeFile(output, JSON.stringify(evidence, null, 2) + '\n');
  console.log(JSON.stringify({ status: evidence.status, cases: evidence.cases.length, differences: evidence.differences.length, error: evidence.error }));
}
