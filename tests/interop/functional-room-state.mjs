/** Actual RoomService state transitions against the fixed official HTTP server. */
import assert from 'node:assert/strict';
import { readFile, writeFile, mkdir } from 'node:fs/promises';
import { fileURLToPath } from 'node:url';
import { dirname, join } from 'node:path';
import { AccessToken } from 'livekit-server-sdk';
const root = dirname(fileURLToPath(import.meta.url));
const urls = [process.env.REFERENCE_URL, process.env.LIVEKIT_URL];
assert.ok(urls.every(Boolean), 'Both actual reference and Rust URLs required');
const key = process.env.LIVEKIT_API_KEY || 'devkey', secret = process.env.LIVEKIT_API_SECRET || 'secret';
const evidence = { profile: 'functional-room-state', status: 'RUNNING', startedAt: new Date().toISOString(),
  scope: 'Actual empty-room Create/List/metadata/delete and missing-participant semantics. Not all14 methods, participant migration, media or worker execution.', observations: [] };
function roomView(room) {
  assert.match(room.sid, /^RM_/);
  assert.ok(BigInt(room.creation_time) > 0n);
  assert.ok(BigInt(room.creation_time_ms) >= BigInt(room.creation_time) * 1000n);
  return { name: 'ROOM_MAPPED', empty_timeout: room.empty_timeout,
    departure_timeout: room.departure_timeout, max_participants: room.max_participants,
    num_participants: room.num_participants, num_publishers: room.num_publishers,
    active_recording: room.active_recording, metadata: room.metadata };
}
async function scenario(server) {
  const name = `functional-room-state-${process.pid}-${server}`;
  const access = new AccessToken(key, secret);
  access.addGrant({ room: name, roomAdmin: true, roomCreate: true, roomList: true });
  const token = await access.toJwt();
  const out = [];
  async function invoke(method, body) {
    const r = await fetch(`${urls[server].replace(/\/$/, '')}/twirp/livekit.RoomService/${method}`, {
      method: 'POST', headers: { 'content-type': 'application/json', authorization: `Bearer ${token}` },
      body: JSON.stringify(body),
    });
    return { status: r.status, type: r.headers.get('content-type'), body: await r.json() };
  }
  try {
    const created = await invoke('CreateRoom', { name, empty_timeout: 30, departure_timeout: 4, max_participants: 2, metadata: '{"step":1}' });
    assert.equal(created.status, 200); out.push({ step: 'create', room: roomView(created.body) });
    const originalSid = created.body.sid;
    const listed = await invoke('ListRooms', { names: [name, `${name}-missing`] });
    assert.equal(listed.status, 200); assert.equal(listed.body.rooms.length, 1);
    assert.equal(listed.body.rooms[0].sid, originalSid);
    out.push({ step: 'list-filtered', room: roomView(listed.body.rooms[0]) });
    const participants = await invoke('ListParticipants', { room: name });
    assert.equal(participants.status, 200); assert.deepEqual(participants.body.participants, []);
    out.push({ step: 'empty-participants', result: participants });
    const metadata = await invoke('UpdateRoomMetadata', { room: name, metadata: '{"step":2,"unicode":"中文"}' });
    assert.equal(metadata.status, 200); assert.equal(metadata.body.sid, originalSid);
    assert.equal(metadata.body.metadata, '{"step":2,"unicode":"中文"}');
    out.push({ step: 'metadata-update', room: roomView(metadata.body) });
    const after = await invoke('ListRooms', { names: [name] });
    assert.equal(after.body.rooms[0].metadata, metadata.body.metadata);
    out.push({ step: 'metadata-persisted', room: roomView(after.body.rooms[0]) });
    for (const method of ['GetParticipant', 'RemoveParticipant']) {
      const result = await invoke(method, { room: name, identity: 'missing-participant' });
      assert.notEqual(result.status, 200);
      out.push({ step: method, result });
    }
    const removed = await invoke('DeleteRoom', { room: name });
    assert.equal(removed.status, 200); out.push({ step: 'delete', result: removed });
    const absent = await invoke('ListRooms', { names: [name] });
    assert.equal(absent.status, 200); assert.deepEqual(absent.body.rooms, []);
    out.push({ step: 'deleted-not-listed', result: absent });
    const recreated = await invoke('CreateRoom', { name, empty_timeout: 40, metadata: 'new-generation' });
    assert.equal(recreated.status, 200); assert.notEqual(recreated.body.sid, originalSid);
    assert.equal(recreated.body.metadata, 'new-generation');
    out.push({ step: 'new-generation', room: roomView(recreated.body) });
    return out;
  } finally { await invoke('DeleteRoom', { room: name }); }
}
try {
  const reference = await scenario(0), rust = await scenario(1);
  evidence.observations = { reference, rust };
  assert.deepEqual(rust, reference, 'actual RoomService lifecycle and error contracts');
  evidence.status = 'PASS';
} catch (error) {
  evidence.status = 'FAIL'; evidence.error = { name: error.name, message: error.message }; process.exitCode = 1;
} finally {
  evidence.finishedAt = new Date().toISOString();
  await mkdir(join(root, 'output'), { recursive: true });
  await writeFile(join(root, 'output/functional-room-state.json'), JSON.stringify(evidence, null, 2) + '\n');
  console.log(JSON.stringify({ status: evidence.status, error: evidence.error }));
}
