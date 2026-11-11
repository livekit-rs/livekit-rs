/** Public HTTP differential; no browser, worker or successful backend execution inferred. */
import assert from 'node:assert/strict';
import { readFile, mkdir, writeFile } from 'node:fs/promises';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';
import { AccessToken } from 'livekit-server-sdk';
import { duplicateJsonCases } from './service-json-corpus.mjs';

const root = dirname(fileURLToPath(import.meta.url));
const contracts = JSON.parse(await readFile(join(root, 'service-contracts.json')));
const urls = [process.env.REFERENCE_URL, process.env.LIVEKIT_URL];
assert.ok(urls.every(Boolean), 'REFERENCE_URL and LIVEKIT_URL must identify running pinned reference and Rust HTTP servers');
const key = process.env.LIVEKIT_API_KEY || 'devkey';
const secret = process.env.LIVEKIT_API_SECRET || 'secret';
const evidence = { profile: 'functional-services', status: 'RUNNING', protocol: contracts.protocol_commit,
  startedAt: new Date().toISOString(), transport: [], dispatch: [],
  scope: '47 registered RPC error/decode contracts and real AgentDispatch room CRUD/defaults. No worker jobs, Redis/Ingress/Egress/SIP execution or full RoomService functionality inferred.' };
async function call(server, service, method, body, { protobuf = false, token } = {}) {
  const response = await fetch(`${urls[server].replace(/\/$/, '')}/twirp/${service}/${method}`, {
    method: 'POST', headers: { 'content-type': protobuf ? 'application/protobuf' : 'application/json',
      ...(token ? { authorization: `Bearer ${token}` } : {}) },
    body: protobuf ? body : typeof body === 'string' ? body : JSON.stringify(body),
  });
  const bytes = await response.text();
  return { status: response.status, type: response.headers.get('content-type'), body: JSON.parse(bytes) };
}
async function token(room) {
  const value = new AccessToken(key, secret);
  value.addGrant({ room, roomAdmin: true, roomCreate: true, roomList: true });
  return value.toJwt();
}
function dispatch(record) {
  assert.match(record.id, /^AD_/);
  const state = record.state;
  assert.ok(BigInt(state.created_at) > 1_000_000_000_000_000_000n);
  return { ...record, id: 'AD_MAPPED', state: { ...state, created_at: 'UNIX_NS_POSITIVE',
    deleted_at: BigInt(state.deleted_at) === 0n ? '0' : 'UNIX_NS_POSITIVE' } };
}
async function dispatchScenario(server) {
  const room = `functional-api-dispatch-${process.pid}-${server}`;
  const auth = await token(room);
  const invoke = async (service, method, body) => call(server, service, method, body, { token: auth });
  const rooms = 'livekit.RoomService', agents = 'livekit.AgentDispatchService';
  const observations = [];
  try {
    assert.equal((await invoke(rooms, 'CreateRoom', { name: room })).status, 200);
    const initial = await invoke(agents, 'ListDispatch', { room });
    assert.equal(initial.status, 200); assert.equal(initial.body.agent_dispatches.length, 1);
    assert.equal(initial.body.agent_dispatches[0].agent_name, '');
    const normalize = value => { const result = dispatch(value); result.room = 'ROOM_MAPPED'; return result; };
    observations.push({ step: 'default-unnamed', value: normalize(initial.body.agent_dispatches[0]) });
    const created = await invoke(agents, 'CreateDispatch', { room, agent_name: 'assistant', metadata: 'context',
      attributes: { locale: 'zh' }, restart_policy: 'JRP_NEVER', deployment: 'blue' });
    assert.equal(created.status, 200);
    const id = created.body.id;
    observations.push({ step: 'create', value: normalize(created.body) });
    const list = await invoke(agents, 'ListDispatch', { room });
    assert.equal(list.body.agent_dispatches.length, 2);
    const filtered = await invoke(agents, 'ListDispatch', { room, dispatch_id: id });
    assert.deepEqual(filtered.body.agent_dispatches, [created.body]);
    const missing = await invoke(agents, 'ListDispatch', { room, dispatch_id: 'AD_missing' });
    assert.deepEqual(missing.body.agent_dispatches, []);
    const removed = await invoke(agents, 'DeleteDispatch', { room, dispatch_id: id });
    assert.equal(removed.status, 200);
    observations.push({ step: 'delete', value: normalize(removed.body) });
    observations.push({ step: 'delete-missing', value: await invoke(agents, 'DeleteDispatch', { room, dispatch_id: id }) });
    assert.equal((await invoke(rooms, 'DeleteRoom', { room })).status, 200);
    assert.equal((await invoke(rooms, 'CreateRoom', { name: room })).status, 200);
    const recreated = await invoke(agents, 'ListDispatch', { room });
    assert.equal(recreated.body.agent_dispatches.length, 1);
    assert.notEqual(recreated.body.agent_dispatches[0].id, initial.body.agent_dispatches[0].id);
    observations.push({ step: 'recreate', value: normalize(recreated.body.agent_dispatches[0]) });
    return observations;
  } finally { await invoke(rooms, 'DeleteRoom', { room }); }
}
try {
  const corpus = [ ['truncated-json', '{'], ['json-scalar', '"invalid"'], ['json-array', '[]'],
    ['json-null', 'null'], ['invalid-protobuf', new Uint8Array([255])], ['empty-unauthenticated', '{}'] ];
  for (const contract of contracts.methods) {
    const enumCases = contract.service === 'livekit.Ingress' && contract.method === 'CreateIngress'
      ? [{ name: 'unknown-symbolic-enum-before-auth', body: '{"inputType":"NOT_AN_INPUT"}', expectCode: 'unauthenticated' }]
      : contract.service === 'livekit.SIP' && contract.method === 'CreateSIPParticipant'
        ? [{ name: 'unknown-symbolic-enum-before-auth', body: '{"includeHeaders":"NOT_A_HEADER_OPTION"}', expectCode: 'unauthenticated' }] : [];
    const cases = [...corpus.map(([name, body]) => ({ name, body })), ...duplicateJsonCases(contract), ...enumCases];
    for (const { name, body, expectMalformed, expectCode, fieldPath } of cases) {
      const options = { protobuf: body instanceof Uint8Array };
      const reference = await call(0, contract.service, contract.method, body, options);
      const rust = await call(1, contract.service, contract.method, body, options);
      evidence.transport.push({ service: contract.service, method: contract.method, case: name,
        ...(fieldPath ? { descriptorFieldPath: fieldPath } : {}), reference, rust });
      if (expectMalformed) {
        assert.equal(reference.status, 400, `official ${contract.method} ${name} must fail before auth`);
        assert.equal(reference.body.code, 'malformed');
        assert.equal(reference.body.msg, 'the json request could not be decoded');
      }
      if (expectCode) {
        assert.equal(reference.status, 401);
        assert.equal(reference.body.code, expectCode);
      }
      assert.deepEqual(rust, reference, `${contract.service}/${contract.method} ${name}`);
    }
  }
  const reference = await dispatchScenario(0), rust = await dispatchScenario(1);
  evidence.dispatch = { reference, rust };
  assert.deepEqual(rust, reference, 'dispatch effects and response semantics');
  evidence.status = 'PASS';
} catch (error) {
  evidence.status = 'FAIL'; evidence.error = { name: error.name, message: error.message }; process.exitCode = 1;
} finally {
  evidence.finishedAt = new Date().toISOString();
  await mkdir(join(root, 'output'), { recursive: true });
  await writeFile(join(root, 'output/functional-services.json'), JSON.stringify(evidence, null, 2) + '\n');
  console.log(JSON.stringify({ status: evidence.status, transportCases: evidence.transport.length, error: evidence.error }));
}
