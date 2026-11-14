/** SPEC-0077 C17 — launched-MAIN participant display-name length-limit gate.
 *
 * Drives ONE launched MAIN (side under test) over the real UpdateParticipant API that consumes
 * limit.max_participant_name_length:
 *   - UpdateParticipant   official livekit-server-sdk 2.19.2 RoomServiceClient.updateParticipant
 *                         (unchanged) on a REAL WebSocket-joined participant.
 *                         -> roomservice.go:265 CheckParticipantNameLength(req.Name), the FIRST
 *                            check (before metadata/attributes/permission/existence), returning
 *                            twirp.InvalidArgumentError("name length exceeds limits", limit).
 *                            Rust room_service.rs:582 check_participant_name_limit ->
 *                            invalid_argument_arg reproduces the identical wire envelope.
 *
 * Wire envelope (twirp v8.1.3 InvalidArgumentError(argument, validationMsg)):
 *   msg = argument + " " + validationMsg = "name length exceeds limits {limit}",
 *   code = invalid_argument, meta.argument = "name length exceeds limits". Both sides match.
 *
 * The effective limit is delivered by the launcher through a specific projection path
 * (CLI --limit.max_participant_name_length positive / zero-unlimited / signed-negative,
 * YAML limit.*, or default 256). The probe asserts the boundary structurally per side
 * (accept len<=limit -> 200 + name echo; reject len>limit -> 400 + the EXACT upstream error
 * text echoing the configured limit), so a server that ignores the flag and falls back to the
 * default 256 could never satisfy a small-limit profile. UTF-8 multibyte cases prove BYTE
 * counting (Go len(string)), not JS char counting. The signed-negative limit rejects EVERY
 * name (even one byte). Each side produces a normalized contract; the launcher deep-compares.
 *
 * Normalization convention: an ACCEPTED update compares its status + a name_echo witness
 * (success bodies carry server-generated sid/version/joined_at and JSON key order, outside the
 * length-gate contract); a REJECTED update compares the deterministic {code,msg} error envelope
 * byte-for-byte, which is where the configured limit value is echoed.
 */
import assert from 'node:assert/strict';
import { createHmac } from 'node:crypto';
import { writeFile } from 'node:fs/promises';
import { RoomServiceClient, TwirpError } from 'livekit-server-sdk';
import { SignalResponse } from '@livekit/protocol';

const url = process.env.RTC_URL;
const side = process.env.SIDE;
const profile = process.env.PROFILE;
const nameLimit = process.env.NAME_LIMIT === 'unlimited' ? null : Number(process.env.NAME_LIMIT);
const outPath = process.env.OUTPUT;
assert.ok(url && side && profile && outPath, 'RTC_URL/SIDE/PROFILE/OUTPUT required');
assert.ok(nameLimit === null || Number.isFinite(nameLimit), 'NAME_LIMIT must be integer or unlimited');

const KEY = 'devkey';
const SECRET = 'owned-room-service-fixture-secret-0123456789';
const base = url.replace(/\/$/, '');
const client = new RoomServiceClient(base, KEY, SECRET);
const bytes = (s) => Buffer.byteLength(s, 'utf8');

const report = {
  status: 'RUNNING', side, profile, name_limit: nameLimit,
  contract: [], errors: [], diagnostics: {},
};

function b64url(x) { return Buffer.from(JSON.stringify(x)).toString('base64url'); }
function token(identity, video) {
  const head = b64url({ alg: 'HS256', typ: 'JWT' });
  const body = b64url({ iss: KEY, sub: identity, exp: Math.floor(Date.now() / 1000) + 3600, video });
  const sig = createHmac('sha256', SECRET).update(`${head}.${body}`).digest('base64url');
  return `${head}.${body}.${sig}`;
}

// Boundary cases for a finite limit (BYTE counting), unlimited (null), or signed-negative.
function boundaryCases(limit) {
  if (limit === null) {
    return [
      { label: 'ascii_long', str: 'a'.repeat(500), expect: 'accept' },
      { label: 'utf8_long', str: '中'.repeat(200), expect: 'accept' }, // 600 bytes
    ];
  }
  if (limit < 0) {
    // Signed negative is NOT zero/unlimited: limit==0 || len<=negative is false for every
    // string, so even a one-byte name is rejected. No acceptance exists for this profile.
    return [
      { label: 'ascii_one', str: 'a', expect: 'reject' },
      { label: 'utf8_one', str: '中', expect: 'reject' }, // 3 bytes
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

// Minimal real /rtc WebSocket join: the participant becomes present in the owner room store
// once the server emits the Join SignalResponse. No SDP/ICE/RTC/media is claimed; this is the
// signaling join only, exactly as the upstream UpdateParticipant existence check requires.
async function joinPeer(room, identity) {
  const u = new URL('/rtc', base);
  u.protocol = u.protocol.replace('http', 'ws');
  u.searchParams.set('access_token', token(identity, { roomJoin: true, room, canPublish: false, canSubscribe: false }));
  u.searchParams.set('protocol', '17');
  u.searchParams.set('sdk', 'js');
  u.searchParams.set('auto_subscribe', '0');
  const ws = new WebSocket(u.toString());
  ws.binaryType = 'arraybuffer';
  const events = []; const waiters = new Set(); let error;
  const closed = new Promise((done) => ws.addEventListener('close', (e) => done(e.code)));
  ws.addEventListener('error', () => { error = new Error('actual WS transport error'); for (const w of waiters) w(); });
  ws.addEventListener('message', (ev) => {
    try {
      assert.ok(ev.data instanceof ArrayBuffer, 'binary signal frame');
      const m = SignalResponse.fromBinary(new Uint8Array(ev.data)).message;
      events.push({ type: m.case, value: m.value });
      for (const w of waiters) w();
    } catch (e) { error = e; for (const w of waiters) w(); }
  });
  function wait(pred, label) {
    const read = () => { if (error) throw error; return events.find(pred); };
    const v = read(); if (v) return Promise.resolve(v);
    return new Promise((done, reject) => {
      const timer = setTimeout(() => { waiters.delete(wake); reject(new Error(`${label} timeout`)); }, 10000);
      const wake = () => {
        try { const x = read(); if (x) { clearTimeout(timer); waiters.delete(wake); done(x); } }
        catch (e) { clearTimeout(timer); waiters.delete(wake); reject(e); }
      };
      waiters.add(wake);
    });
  }
  const join = await wait((e) => e.type === 'join', 'actual Join');
  assert.equal(join.value.participant.identity, identity, 'join echoes identity');
  assert.ok(join.value.participant.sid, 'join carries sid');
  return { sid: join.value.participant.sid, async close() { if (ws.readyState !== WebSocket.CLOSED) ws.close(1000, 'fixture complete'); await closed; } };
}

async function updateName(room, identity, name) {
  try {
    const info = await client.updateParticipant(room, identity, { name });
    assert.equal(info.name, name, `accepted UpdateParticipant must echo requested name (side ${side})`);
    return { status: 200, name_echo: true };
  } catch (e) {
    if (e instanceof TwirpError || (e && e.code !== undefined && e.status !== undefined)) {
      return { status: e.status, code: e.code, msg: e.message };
    }
    throw e;
  }
}

function assertBoundary(rec, limit) {
  if (rec.expect === 'accept') {
    assert.equal(rec.status, 200,
      `${rec.case} (${rec.bytes} bytes <= ${limit}) must be accepted; limit ignored? (side ${side})`);
    assert.equal(rec.name_echo, true, `${rec.case} accepted name must echo (side ${side})`);
  } else {
    assert.equal(rec.status, 400, `${rec.case} (${rec.bytes} bytes > ${limit}) must reject 400 (side ${side})`);
    assert.equal(rec.code, 'invalid_argument', `${rec.case} rejection code must be invalid_argument (side ${side})`);
    assert.equal(rec.msg, `name length exceeds limits ${limit}`,
      `${rec.case} rejection must echo configured limit ${limit} (side ${side}): ${rec.msg}`);
  }
}

async function scenario() {
  const contract = [];
  const room = `c17-name-${profile}`;
  const identity = 'p';
  await client.createRoom({ name: room });
  const peer = await joinPeer(room, identity);
  try {
    // Confirm the participant is present in the owner store before the admin update path.
    const deadline = Date.now() + 10000;
    for (;;) {
      try { await client.getParticipant(room, identity); break; }
      catch (e) {
        if (e.code !== 'not_found') throw e;
        assert.ok(Date.now() < deadline, 'participant presence convergence');
        await new Promise((r) => setTimeout(r, 20));
      }
    }
    for (const c of boundaryCases(nameLimit)) {
      const obs = await updateName(room, identity, c.str);
      const rec = { case: `name-${c.label}`, endpoint: 'update-participant', bytes: bytes(c.str), expect: c.expect, status: obs.status };
      if (c.expect === 'accept') { rec.name_echo = obs.name_echo; }
      else { rec.code = obs.code; rec.msg = obs.msg; }
      assertBoundary(rec, nameLimit);
      contract.push(rec);
    }
  } finally {
    await peer.close();
    await client.deleteRoom(room).catch(() => {});
  }
  return contract;
}

try {
  report.contract = await scenario();
  report.diagnostics.case_count = report.contract.length;
  report.diagnostics.accepted = report.contract.filter((r) => r.expect === 'accept').length;
  report.diagnostics.rejected = report.contract.filter((r) => r.expect === 'reject').length;
  // Structural sanity (profile-aware): the boundary must actually be exercised, guarding
  // against a vacuous contract. Unlimited accepts long names and rejects none; signed-negative
  // rejects every name and accepts none; a positive finite limit must do BOTH.
  if (nameLimit === null) {
    assert.ok(report.diagnostics.accepted > 0, 'unlimited profile must accept long names');
    assert.ok(report.diagnostics.rejected === 0, 'unlimited profile must not reject');
  } else if (nameLimit < 0) {
    assert.ok(report.diagnostics.rejected > 0, 'negative profile must reject');
    assert.ok(report.diagnostics.accepted === 0, 'negative profile rejects every name');
  } else {
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