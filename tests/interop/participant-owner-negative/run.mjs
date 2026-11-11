// Parent-run only: actual public HTTP source oracle, no expected error guessed here.
import assert from 'node:assert/strict';
import { createRequire } from 'node:module';
import { readFile, writeFile } from 'node:fs/promises';
import { randomUUID } from 'node:crypto';
import { performance } from 'node:perf_hooks';
const require = createRequire(process.env.INTEROP_PACKAGE_JSON || `${process.cwd()}/tests/interop/package.json`);
const { AccessToken } = require('livekit-server-sdk');
const url = (process.env.LIVEKIT_URL || 'http://127.0.0.1:7880').replace(/^ws/, 'http');
const key = process.env.LIVEKIT_API_KEY || 'devkey', secret = process.env.LIVEKIT_API_SECRET || 'secret';
const mode = process.env.API_PARTICIPANT_VIEW;
assert.ok(['cached', 'live-psrpc'].includes(mode), 'set explicit configured cached|live-psrpc mode');
const prefix = `f26-${randomUUID()}`, missing = `${prefix}-missing`, existing = `${prefix}-existing`, removed = `${prefix}-removed`;
const report = { status: 'RUNNING', mode, scope: 'Actual public missing-room/store-vs-topic semantics; active room participant absence and deleted-owner cases. No distributed C3 claim.', observations: [] };
const owned = new Set();
async function call(method, body, auth = true) {
  const token = new AccessToken(key, secret);
  token.addGrant(auth ? { roomAdmin: true, roomCreate: true, roomList: true, room: body.room || body.name } : {});
  const jwt = await token.toJwt(), start = performance.now();
  const response = await fetch(`${url}/twirp/livekit.RoomService/${method}`, { method: 'POST', headers: { Authorization: `Bearer ${jwt}`, 'Content-Type': 'application/json' }, body: JSON.stringify(body), signal: AbortSignal.timeout(15000) });
  const raw = await response.text();
  assert.ok(!raw.includes(secret) && !raw.includes(jwt), 'response contains a credential');
  let value; try { value = JSON.parse(raw); } catch { value = { nonJsonBody: raw }; }
  return { status: response.status, body: value, elapsedMs: performance.now() - start };
}
async function observe(id, method, body, auth = true) { const value = await call(method, body, auth); report.observations.push({ id, method, ...value }); return value; }
async function pair(scope, room) {
  await observe(`${scope}-list`, 'ListParticipants', { room });
  await observe(`${scope}-get`, 'GetParticipant', { room, identity: 'absent-participant' });
}
try {
  await pair('never-created-room', missing);
  await observe('no-admin-no-owner-list', 'ListParticipants', { room: missing }, false);
  await observe('no-admin-no-owner-get', 'GetParticipant', { room: missing, identity: 'absent-participant' }, false);
  const created = await call('CreateRoom', { name: existing }); assert.equal(created.status, 200); owned.add(existing);
  // An API-created store entry is not silently assumed to imply an active RTC owner.
  await pair('api-created-empty-room', existing);
  const another = await call('CreateRoom', { name: removed }); assert.equal(another.status, 200); owned.add(removed);
  const deleted = await call('DeleteRoom', { room: removed }); assert.equal(deleted.status, 200); owned.delete(removed);
  await pair('deleted-room', removed);
  if (process.env.PARTICIPANT_NEGATIVE_REFERENCE_RESULTS) {
    const expected = JSON.parse(await readFile(process.env.PARTICIPANT_NEGATIVE_REFERENCE_RESULTS, 'utf8'));
    assert.equal(expected.status, 'REFERENCE_CAPTURED'); assert.equal(expected.mode, mode);
    assert.equal(expected.observations.length, report.observations.length);
    for (let i = 0; i < report.observations.length; i++) {
      const a = report.observations[i], b = expected.observations[i];
      assert.equal(a.id, b.id); assert.equal(a.status, b.status, `${a.id} actual HTTP status`);
      assert.deepEqual(a.body, b.body, `${a.id} exact actual Go error/body/meta`);
    }
    report.status = 'PASS';
  } else report.status = 'REFERENCE_CAPTURED';
} catch (error) { report.status = 'FAIL'; report.failure = String(error.message).replaceAll(secret, '<secret>').replaceAll(prefix, '<owned-prefix>'); process.exitCode = 1; }
finally {
  const failures = [];
  for (const room of owned) try { const r = await call('DeleteRoom', { room }); if (r.status !== 200) failures.push('owned-room-delete'); } catch { failures.push('owned-room-delete'); }
  if (failures.length) { report.cleanupFailures = failures; report.status = 'FAIL'; process.exitCode = 1; }
  await writeFile(process.env.PARTICIPANT_NEGATIVE_OUTPUT || '/tmp/f26-participant-negative.json', JSON.stringify(report, null, 2) + '\n');
}
