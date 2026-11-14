/** SPEC-0077 C21 — launched-MAIN root-logger sampling stimulus.
 *
 * Drives ONE launched MAIN (side under test) with N back-to-back real /rtc WebSocket
 * joins whose JWT carries a top-level `metadata` claim larger than limits.max_metadata_size.
 * The size gate runs at the RTC owner stage AFTER auth + the 101 upgrade decision
 * (Rust signal/mod.rs:194 check_owner_profile_sizes -> start.rs:46 root WARN
 * "error starting signal session"; Go rtcservice.go startConnection -> utils.go:79
 * root WARN "error handling request"). With signal_relay.connect_attempts=1 each request
 * produces exactly ONE root WARN on either side, so N requests inside one 1-second
 * sampler window exercise the root sampling policy (logging.sample +
 * logging.item_sample_initial / item_sample_interval).
 *
 * This probe ONLY emits the stimulus and records client-side transport observations.
 * The launcher counts the side's root WARN lines from captured stderr and asserts the
 * sampled count against first + floor((N-first)/thereafter). The message text and HTTP
 * status legitimately differ between Go and Rust and belong to the SIGNAL/HTTP items,
 * so they are NOT compared here (count-only sampling observable, scoped in the ledger).
 */
import assert from 'node:assert/strict';
import { createHmac, randomBytes } from 'node:crypto';
import http from 'node:http';
import { writeFile } from 'node:fs/promises';

const url = process.env.RTC_URL;
const side = process.env.SIDE;
const profile = process.env.PROFILE;
const outPath = process.env.OUTPUT;
const N = Number(process.env.N || '12');
const METADATA_BYTES = Number(process.env.METADATA_BYTES || '64');
assert.ok(url && side && profile && outPath, 'RTC_URL/SIDE/PROFILE/OUTPUT required');
assert.ok(Number.isInteger(N) && N > 0, 'N must be a positive integer');
assert.ok(Number.isInteger(METADATA_BYTES) && METADATA_BYTES > 0, 'METADATA_BYTES positive');

const KEY = 'devkey';
const SECRET = 'owned-logging-sampling-fixture-secret-0123456789';
const JOIN_ROOM = 'sampling-room';
const base = url.replace(/\/$/, '');

function b64url(x) { return Buffer.from(JSON.stringify(x)).toString('base64url'); }
function token(identity, metadata) {
  const head = b64url({ alg: 'HS256', typ: 'JWT' });
  const body = b64url({
    iss: KEY, sub: identity, exp: Math.floor(Date.now() / 1000) + 3600,
    metadata,
    video: { room: JOIN_ROOM, roomJoin: true, identity, canPublish: false, canSubscribe: false },
  });
  const sig = createHmac('sha256', SECRET).update(`${head}.${body}`).digest('base64url');
  return `${head}.${body}.${sig}`;
}

// One real /rtc WS join. Resolves with the client-side transport outcome; the server
// emits its root WARN regardless of whether the client sees a 101+close or an HTTP error.
function joinWs(t) {
  return new Promise((resolve) => {
    const u = new URL(base);
    const path = `/rtc?access_token=${encodeURIComponent(t)}&protocol=17&sdk=js`;
    const req = http.request({
      hostname: u.hostname, port: u.port, path, method: 'GET',
      headers: {
        Connection: 'Upgrade', Upgrade: 'websocket',
        'Sec-WebSocket-Key': randomBytes(16).toString('base64'), 'Sec-WebSocket-Version': '13',
      },
    });
    let settled = false;
    const done = (obs) => { if (!settled) { settled = true; resolve(obs); } };
    req.on('upgrade', (res, socket) => { socket.destroy(); done({ transport: 'upgrade', status: 101 }); });
    req.on('response', (res) => {
      let b = ''; res.on('data', (c) => { b += c; });
      res.on('end', () => done({ transport: 'response', status: res.statusCode }));
    });
    req.on('error', (e) => done({ transport: 'error', error: String(e.code || e.message) }));
    req.setTimeout(10000, () => { req.destroy(); done({ transport: 'timeout' }); });
    req.end();
  });
}

const report = { status: 'RUNNING', side, profile, n: N, metadata_bytes: METADATA_BYTES, observations: [], errors: [] };
try {
  const t = token('u', 'm'.repeat(METADATA_BYTES));
  // Fire all N concurrently so every root WARN lands inside one 1-second sampler window.
  const obs = await Promise.all(Array.from({ length: N }, () => joinWs(t)));
  report.observations = obs;
  report.diagnostics = {
    sent: obs.length,
    upgrades: obs.filter((o) => o.transport === 'upgrade').length,
    responses: obs.filter((o) => o.transport === 'response').length,
    errors: obs.filter((o) => o.transport === 'error' || o.transport === 'timeout').length,
  };
  assert.equal(obs.length, N, 'must settle all N requests');
  // The stimulus must actually reach the server: at least one upgrade or HTTP response
  // (a pure all-error run would mean the server never processed the joins).
  assert.ok(report.diagnostics.upgrades + report.diagnostics.responses > 0,
    'no request reached the server transport; stimulus did not fire');
  report.status = 'PASS';
} catch (error) {
  report.status = 'FAIL';
  report.errors.push({ name: error.name, message: String(error.message).replace(/eyJ[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+/g, '<token>') });
  process.exitCode = 1;
} finally {
  await writeFile(outPath, JSON.stringify(report, null, 2) + '\n');
  console.log(JSON.stringify({ status: report.status, side, profile, sent: report.observations.length, diagnostics: report.diagnostics }));
}