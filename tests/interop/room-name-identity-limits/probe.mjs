/** SPEC-0077 C16 — launched-MAIN room-name / participant-identity length-limit gate.
 *
 * Drives ONE launched MAIN (side under test) over the real HTTP surfaces that consume
 * limit.max_room_name_length / limit.max_participant_identity_length:
 *   - CreateRoom            official livekit-server-sdk 2.19.2 RoomServiceClient (unchanged)
 *                           -> roomservice.go:83 CheckRoomNameLength (Rust room_service.rs:322)
 *   - /rtc/validate         server validation preflight (non-WS), room-name AND identity
 *                           -> utils.go:369/387 (Rust signal/params.rs:219/230)
 *   - /rtc real WS join     actual signaling join with WebSocket upgrade headers; the gate
 *                           runs BEFORE the 101 upgrade (Go rtcservice.go:379 HandleError,
 *                           Rust signal/mod.rs prepare->failure), over-limit => HTTP 400.
 *
 * The effective limit is delivered by the launcher through a specific projection path
 * (YAML room.* legacy alias nonzero-override / zero-preserve, YAML limit.*, CLI --room.* /
 * --limit.* flags, default, or limit=0 unlimited). The probe asserts the boundary structurally
 * (accept len<=limit, reject len>limit with the EXACT upstream error text echoing the configured
 * limit) so a server that ignores the alias/flag and falls back to the default 256 could never
 * satisfy a small-limit profile. UTF-8 multibyte cases prove BYTE counting (Go len(string)),
 * not JS char counting. Each side produces a normalized contract; the launcher deep-compares.
 *
 * Normalization convention (same as the room-name-identity-alias oracle): an ACCEPTED request
 * compares only its status (success bodies carry server-generated sid/turn_password/creation_time
 * and JSON key order, outside the length-gate contract); a REJECTED request compares the
 * deterministic error payload byte-for-byte (CreateRoom {code,msg}; validate/join error text),
 * which is where the configured limit value is echoed.
 */
import assert from 'node:assert/strict';
import { createHmac, randomBytes } from 'node:crypto';
import http from 'node:http';
import { writeFile } from 'node:fs/promises';
import { RoomServiceClient, TwirpError } from 'livekit-server-sdk';

const url = process.env.RTC_URL;
const side = process.env.SIDE;
const profile = process.env.PROFILE;
const nameLimit = process.env.NAME_LIMIT === 'unlimited' ? null : Number(process.env.NAME_LIMIT);
const idLimit = process.env.IDENTITY_LIMIT === 'unlimited' ? null : Number(process.env.IDENTITY_LIMIT);
const outPath = process.env.OUTPUT;
assert.ok(url && side && profile && outPath, 'RTC_URL/SIDE/PROFILE/OUTPUT required');
assert.ok(nameLimit === null || Number.isFinite(nameLimit), 'NAME_LIMIT must be integer or unlimited');
assert.ok(idLimit === null || Number.isFinite(idLimit), 'IDENTITY_LIMIT must be integer or unlimited');

const KEY = 'devkey';
const SECRET = 'owned-room-service-fixture-secret-0123456789';
const JOIN_ROOM = 'join-room'; // 9 bytes, within every finite profile limit (>=10)
const base = url.replace(/\/$/, '');
const client = new RoomServiceClient(base, KEY, SECRET);

const report = {
  status: 'RUNNING', side, profile,
  name_limit: nameLimit, identity_limit: idLimit,
  contract: [], errors: [], diagnostics: {},
};

function b64url(x) { return Buffer.from(JSON.stringify(x)).toString('base64url'); }
function token(identity, video) {
  const head = b64url({ alg: 'HS256', typ: 'JWT' });
  const body = b64url({ iss: KEY, sub: identity || 'u', exp: Math.floor(Date.now() / 1000) + 3600, video });
  const sig = createHmac('sha256', SECRET).update(`${head}.${body}`).digest('base64url');
  return `${head}.${body}.${sig}`;
}
const bytes = (s) => Buffer.byteLength(s, 'utf8');

// Boundary cases for a finite limit (BYTE counting) or unlimited.
function boundaryCases(limit) {
  if (limit === null) {
    return [
      { label: 'ascii_long', str: 'a'.repeat(500), expect: 'accept' },
      { label: 'utf8_long', str: '中'.repeat(200), expect: 'accept' }, // 600 bytes
    ];
  }
  const out = [
    { label: 'ascii_at', str: 'a'.repeat(limit), expect: 'accept' },
    { label: 'ascii_over', str: 'a'.repeat(limit + 1), expect: 'reject' },
  ];
  const k = Math.floor(limit / 3);
  if (k >= 1) {
    out.push({ label: 'utf8_at', str: '中'.repeat(k), expect: 'accept' }); // 3k <= limit bytes
    if (3 * (k + 1) > limit) out.push({ label: 'utf8_over', str: '中'.repeat(k + 1), expect: 'reject' });
  }
  return out;
}

// --- endpoint drivers (normalized observations) ---
async function createRoom(name) {
  try {
    const room = await client.createRoom({ name });
    assert.equal(room.name, name, `accepted CreateRoom must echo requested name (side ${side})`);
    await client.deleteRoom(name).catch(() => {});
    return { status: 200 };
  } catch (e) {
    if (e instanceof TwirpError || (e && e.code !== undefined && e.status !== undefined)) {
      return { status: e.status, code: e.code, msg: e.message };
    }
    throw e;
  }
}

async function validate({ identity, room }) {
  const u = new URL(`${base}/rtc/validate`);
  u.searchParams.set('access_token', token(identity, { identity, room, roomJoin: true, canPublish: false, canSubscribe: false }));
  u.searchParams.set('protocol', '17');
  u.searchParams.set('sdk', 'js');
  const r = await fetch(u, { signal: AbortSignal.timeout(10000) });
  const body = await r.text();
  return r.status === 200 ? { status: 200 } : { status: r.status, body };
}

// Real /rtc WebSocket join: the length gate runs before the 101 upgrade. An over-limit
// identity yields an ordinary HTTP 400 response; an at-limit identity yields a 101 upgrade
// (socket immediately destroyed). Non-WS requests are rejected 404 by both servers, so the
// upgrade headers are mandatory to reach ValidateConnectRequest / parse_join.
function joinWs(identity) {
  return new Promise((resolve, reject) => {
    const u = new URL(base);
    const t = token(identity, { room: JOIN_ROOM, roomJoin: true, identity, canPublish: false, canSubscribe: false });
    const path = `/rtc?access_token=${encodeURIComponent(t)}&protocol=17&sdk=js`;
    const req = http.request({
      hostname: u.hostname, port: u.port, path, method: 'GET',
      headers: {
        Connection: 'Upgrade', Upgrade: 'websocket',
        'Sec-WebSocket-Key': randomBytes(16).toString('base64'), 'Sec-WebSocket-Version': '13',
      },
    });
    req.on('upgrade', (res, socket) => { socket.destroy(); resolve({ status: 101 }); });
    req.on('response', (res) => {
      let b = ''; res.on('data', (c) => { b += c; });
      res.on('end', () => resolve(res.statusCode === 200 ? { status: 200 } : { status: res.statusCode, body: b }));
    });
    req.on('error', reject);
    req.setTimeout(10000, () => req.destroy(new Error('join timeout')));
    req.end();
  });
}

function record(caseId, endpoint, kind, c, obs) {
  const rec = { case: caseId, endpoint, kind, bytes: bytes(c.str), expect: c.expect, status: obs.status };
  if (c.expect === 'reject') {
    if (obs.code !== undefined) { rec.code = obs.code; rec.msg = obs.msg; }
    if (obs.body !== undefined) { rec.body = obs.body; }
  }
  return rec;
}

function assertBoundary(rec, limit, subject) {
  if (rec.expect === 'accept') {
    const okStatus = rec.endpoint === 'join' ? 101 : 200;
    assert.equal(rec.status, okStatus,
      `${subject} ${rec.case} (${rec.bytes} bytes <= ${limit}) must be accepted; limit ignored? (side ${side})`);
  } else {
    assert.equal(rec.status, 400, `${subject} ${rec.case} (${rec.bytes} bytes > ${limit}) must reject 400 (side ${side})`);
    const text = rec.msg !== undefined ? rec.msg : rec.body;
    assert.ok(text && text.includes(`${subject} length exceeds limits`),
      `${subject} ${rec.case} rejection must echo upstream sentinel (side ${side}): ${text}`);
    assert.ok(text && text.includes(`max length ${limit}`),
      `${subject} ${rec.case} rejection must echo configured limit ${limit} (side ${side}): ${text}`);
    if (rec.endpoint === 'create-room') assert.equal(rec.code, 'invalid_argument',
      `CreateRoom rejection code must be invalid_argument (side ${side})`);
  }
}

async function scenario() {
  const contract = [];
  // ROOM NAME: CreateRoom (official SDK) + /rtc/validate room-name.
  for (const c of boundaryCases(nameLimit)) {
    const cr = record(`name-${c.label}`, 'create-room', 'room name', c, await createRoom(c.str));
    assertBoundary(cr, nameLimit, 'room name');
    contract.push(cr);
    const vr = record(`name-${c.label}`, 'validate-room', 'room name', c,
      await validate({ identity: 'u', room: c.str }));
    assertBoundary(vr, nameLimit, 'room name');
    contract.push(vr);
  }
  // PARTICIPANT IDENTITY: /rtc/validate identity + real /rtc WS join.
  for (const c of boundaryCases(idLimit)) {
    const vr = record(`identity-${c.label}`, 'validate-identity', 'participant identity', c,
      await validate({ identity: c.str, room: JOIN_ROOM }));
    assertBoundary(vr, idLimit, 'participant identity');
    contract.push(vr);
    const jr = record(`identity-${c.label}`, 'join', 'participant identity', c, await joinWs(c.str));
    assertBoundary(jr, idLimit, 'participant identity');
    contract.push(jr);
  }
  await client.deleteRoom(JOIN_ROOM).catch(() => {});
  return contract;
}

try {
  report.contract = await scenario();
  report.diagnostics.case_count = report.contract.length;
  report.diagnostics.accepted = report.contract.filter((r) => r.expect === 'accept').length;
  report.diagnostics.rejected = report.contract.filter((r) => r.expect === 'reject').length;
  // Structural sanity: a finite-limit profile MUST contain both accepts and rejects, otherwise
  // the boundary is not actually exercised (guards against a vacuous all-accept contract).
  if (nameLimit !== null || idLimit !== null) {
    assert.ok(report.diagnostics.rejected > 0, 'finite-limit profile must exercise at least one rejection');
    assert.ok(report.diagnostics.accepted > 0, 'finite-limit profile must exercise at least one acceptance');
  }
  report.status = 'PASS';
} catch (error) {
  report.status = 'FAIL';
  report.errors.push({ name: error.name, message: String(error.message).replace(/eyJ[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+/g, '<token>') });
  process.exitCode = 1;
} finally {
  await writeFile(outPath, JSON.stringify(report, null, 2) + '\n');
  console.log(JSON.stringify({ status: report.status, side, profile, cases: report.contract.length }));
}