/** Draft F04a real HTTP/stateful configuration differential. No store mocks. */
import assert from 'node:assert/strict';
import { createRequire } from 'node:module';
import { mkdir, writeFile } from 'node:fs/promises';
import { join, dirname } from 'node:path';
import { fileURLToPath } from 'node:url';

const interop = dirname(fileURLToPath(import.meta.url));
const require = createRequire(join(interop, 'package.json'));
const { AccessToken } = require('livekit-server-sdk');
const urls = [process.env.REFERENCE_URL, process.env.LIVEKIT_URL];
assert.ok(urls.every(Boolean), 'two real configured HTTP servers required');
assert.notEqual(urls[0], urls[1], 'reference/Rust servers must be distinct');
const keys = [process.env.REFERENCE_API_KEY || process.env.LIVEKIT_API_KEY || 'devkey', process.env.LIVEKIT_API_KEY || 'devkey'];
const secrets = [process.env.REFERENCE_API_SECRET || process.env.LIVEKIT_API_SECRET || 'secret', process.env.LIVEKIT_API_SECRET || 'secret'];
const evidence = { profile: 'functional-sip-config', status: 'RUNNING', startedAt: new Date().toISOString(),
  source: { serverCommit: 'e5655ac20267fa5005dccaf3f0e3bfc3ea688c07', protocolCommit: '9bb708cdb81918a6c63dd25e915b4644310b45b6' },
  steps: [], differences: [], isolation: [], cleanup: [],
  scope: '14 SIP configuration RPCs, real separate stores and HTTP effects. CreateSIPParticipant/TransferSIPParticipant, worker calls, SIP bridge/media/telephony remain OPEN.' };
const ids = [new Map(), new Map()];
const owned = [[], []];
const visited = new Set();
const timestampFields = new Set(['created_at', 'updated_at']);
function normalize(value, server, field = '') {
  if (Array.isArray(value)) return value.map(v => normalize(v, server));
  if (value && typeof value === 'object') return Object.fromEntries(Object.entries(value).map(([k, v]) => [k, normalize(v, server, k)]));
  if (typeof value === 'string') {
    for (const [actual, label] of ids[server]) value = value.replaceAll(actual, label);
    // Only real nondefault generated timestamps; null/default timestamps stay exact.
    if (timestampFields.has(field) && /^\d{4}-\d\d-\d\dT/.test(value) && Date.parse(value) > 946684800000) return 'GENERATED_TIMESTAMP';
  }
  return value;
}
async function jwt(server, grant) {
  const token = new AccessToken(keys[server], secrets[server], { identity: 'sip-config-fixture' });
  if (grant === 'admin') token.addSIPGrant({ admin: true });
  if (grant === 'call') token.addSIPGrant({ call: true });
  if (grant === 'video-admin') token.addGrant({ room: 'fixture', roomAdmin: true });
  return token.toJwt();
}
async function call(server, method, body, grant = 'admin') {
  const authorization = grant === 'none' ? {} : { authorization: `Bearer ${await jwt(server, grant)}` };
  const result = await fetch(`${urls[server].replace(/\/$/, '')}/twirp/livekit.SIP/${method}`, {
    method: 'POST', headers: { 'content-type': 'application/json', ...authorization }, body: JSON.stringify(body),
  });
  const text = await result.text();
  let data;
  try { data = JSON.parse(text); } catch { throw new Error(`${method} response is not JSON (status ${result.status})`); }
  return { status: result.status, type: result.headers.get('content-type'), body: data };
}
function compare(label, method, responses) {
  const pair = responses.map((r, index) => normalize(r, index));
  evidence.steps.push({ label, method, reference: pair[0], rust: pair[1] });
  if (!isEqual(pair[0], pair[1])) evidence.differences.push({ label, method, reference: pair[0], rust: pair[1] });
}
function isEqual(a, b) { try { assert.deepEqual(a, b); return true; } catch { return false; } }
async function pair(label, method, bodies, { grant = 'admin', status } = {}) {
  visited.add(method);
  const responses = await Promise.all([0, 1].map(server => call(server, method, typeof bodies === 'function' ? bodies(server) : bodies, grant)));
  compare(label, method, responses);
  if (status !== undefined) for (const result of responses) assert.equal(result.status, status, `${label} must exhibit real expected effect`);
  return responses;
}
function id(server, label) {
  const found = [...ids[server]].find(([, value]) => value === label);
  assert.ok(found, `prerequisite real create missing for ${label}`); return found[0];
}
async function create(label, method, body, field, kind) {
  visited.add(method);
  const reference = await call(0, method, body(0));
  assert.equal(reference.status, 200, `${label}: official create precondition`);
  const referenceId = reference.body[field];
  assert.ok(typeof referenceId === 'string' && referenceId.length > 4);
  ids[0].set(referenceId, label); owned[0].push({ kind, id: referenceId });
  // Real cross-server read verifies no shared store namespace, independently of environment labels.
  if (kind === 'inbound') {
    const isolated = await call(1, 'GetSIPInboundTrunk', { sip_trunk_id: referenceId });
    evidence.isolation.push({ label, otherServerStatus: isolated.status, otherServerCode: isolated.body.code });
    assert.equal(isolated.status, 404, 'Rust must not see reference store writes');
  }
  const rust = await call(1, method, body(1));
  (evidence.prerequisites ??= []).push({ label, reference: normalize(reference, 0), rust: normalize(rust, 1) });
  assert.equal(rust.status, 200, `${label}: Rust must create, not return disconnected`);
  const rustId = rust.body[field]; assert.ok(typeof rustId === 'string' && rustId.length > 4);
  ids[1].set(rustId, label); owned[1].push({ kind, id: rustId });
  compare(label, method, [reference, rust]);
}
const inbound = extra => ({ name: 'inbound-config', numbers: ['+12025550101'], allowed_addresses: ['192.0.2.0/24'], ...extra });
const outbound = extra => ({ name: 'outbound-config', address: 'sip.example.invalid', numbers: ['+12025550111'], ...extra });
const direct = room => ({ dispatch_rule_direct: { room_name: room } });
try {
  for (const method of ['ListSIPTrunk', 'ListSIPInboundTrunk', 'ListSIPOutboundTrunk', 'ListSIPDispatchRule']) {
    const results = await pair(`empty-${method}`, method, {}, { status: 200 });
    for (const result of results) assert.deepEqual(result.body.items, [], 'fresh isolated store required');
  }
  // All 14 configuration routes must enforce SIP admin, regardless of broader video or SIP-call grants.
  const authMethods = ['ListSIPTrunk', 'CreateSIPInboundTrunk', 'CreateSIPOutboundTrunk', 'UpdateSIPInboundTrunk',
    'UpdateSIPOutboundTrunk', 'GetSIPInboundTrunk', 'GetSIPOutboundTrunk', 'ListSIPInboundTrunk', 'ListSIPOutboundTrunk',
    'DeleteSIPTrunk', 'CreateSIPDispatchRule', 'UpdateSIPDispatchRule', 'ListSIPDispatchRule', 'DeleteSIPDispatchRule'];
  for (const method of authMethods) for (const grant of ['none', 'call', 'video-admin']) {
    await pair(`auth-${method}-${grant}`, method, {}, { grant, status: 401 });
  }
  await pair('create-inbound-missing-trunk', 'CreateSIPInboundTrunk', {}, { status: 400 });
  await pair('create-inbound-unrestricted-hard-error', 'CreateSIPInboundTrunk', { trunk: {} }, { status: 400 });
  await pair('create-outbound-no-address', 'CreateSIPOutboundTrunk', { trunk: { numbers: ['+12025550111'] } }, { status: 400 });
  await pair('create-with-id-hard-error', 'CreateSIPInboundTrunk', { trunk: inbound({ sip_trunk_id: 'ST_forbidden' }) }, { status: 400 });
  await pair('invalid-header-hard-error', 'CreateSIPInboundTrunk', { trunk: inbound({ headers: { 'Bad Header': 'x' } }) }, { status: 400 });
  await create('INBOUND', 'CreateSIPInboundTrunk', () => ({ trunk: inbound({ headers: { To: '<sip:soft@example.invalid>' } }) }), 'sip_trunk_id', 'inbound');
  const legacy = await pair('legacy-list-created', 'ListSIPTrunk', {}, { status: 200 });
  for (const server of [0, 1]) {
    assert.equal(legacy[server].body.items.length, 1, 'legacy list must project the actual modern inbound store');
    assert.equal(legacy[server].body.items[0].sip_trunk_id, id(server, 'INBOUND'));
  }
  await create('OUTBOUND', 'CreateSIPOutboundTrunk', () => ({ trunk: outbound({}) }), 'sip_trunk_id', 'outbound');
  await pair('get-inbound-after-create', 'GetSIPInboundTrunk', s => ({ sip_trunk_id: id(s, 'INBOUND') }), { status: 200 });
  await pair('get-outbound-after-create', 'GetSIPOutboundTrunk', s => ({ sip_trunk_id: id(s, 'OUTBOUND') }), { status: 200 });
  const filteredIn = await pair('inbound-filter-id', 'ListSIPInboundTrunk', s => ({ trunk_ids: [id(s, 'INBOUND')] }), { status: 200 });
  for (const server of [0, 1]) {
    assert.equal(filteredIn[server].body.items.length, 1);
    assert.equal(filteredIn[server].body.items[0].sip_trunk_id, id(server, 'INBOUND'));
  }
  const noNumbers = await pair('inbound-filter-number-miss', 'ListSIPInboundTrunk', { numbers: ['+12025550888'] }, { status: 200 });
  for (const result of noNumbers) assert.deepEqual(result.body.items, []);
  await pair('outbound-filter-number', 'ListSIPOutboundTrunk', { numbers: ['+12025550111'] }, { status: 200 });
  const updateIn = await pair('listupdate-inbound-add-remove', 'UpdateSIPInboundTrunk', s => ({ sip_trunk_id: id(s, 'INBOUND'), update: {
    numbers: { add: ['+12025550102', '+12025550102'], remove: ['+12025550101'] }, name: '', metadata: 'updated' } }), { status: 200 });
  for (const r of updateIn) assert.deepEqual(r.body.numbers, ['+12025550102']);
  await pair('listupdate-inbound-allowed-set', 'UpdateSIPInboundTrunk', s => ({ sip_trunk_id: id(s, 'INBOUND'), update: {
    allowed_numbers: { set: ['+12025550999'] } } }), { status: 200 });
  const cleared = await pair('listupdate-inbound-clear', 'UpdateSIPInboundTrunk', s => ({ sip_trunk_id: id(s, 'INBOUND'), update: {
    allowed_numbers: { clear: true } } }), { status: 200 });
  for (const result of cleared) assert.deepEqual(result.body.allowed_numbers, []);
  await pair('listupdate-outbound-set', 'UpdateSIPOutboundTrunk', s => ({ sip_trunk_id: id(s, 'OUTBOUND'), update: {
    numbers: { set: ['+12025550112'] }, auth_username: '', metadata: '' } }), { status: 200 });
  await pair('replace-inbound', 'UpdateSIPInboundTrunk', s => ({ sip_trunk_id: id(s, 'INBOUND'), replace: inbound({ name: 'replaced-inbound', numbers: ['+12025550103'] }) }), { status: 200 });
  await pair('replace-outbound', 'UpdateSIPOutboundTrunk', s => ({ sip_trunk_id: id(s, 'OUTBOUND'), replace: outbound({ sip_trunk_id: id(s, 'OUTBOUND'), name: 'replaced-outbound' }) }), { status: 200 });
  const persistedIn = await pair('get-persisted-inbound-replace', 'GetSIPInboundTrunk', s => ({ sip_trunk_id: id(s, 'INBOUND') }), { status: 200 });
  for (const result of persistedIn) { assert.equal(result.body.trunk.name, 'replaced-inbound'); assert.deepEqual(result.body.trunk.numbers, ['+12025550103']); }
  const persistedOut = await pair('get-persisted-outbound-replace', 'GetSIPOutboundTrunk', s => ({ sip_trunk_id: id(s, 'OUTBOUND') }), { status: 200 });
  for (const result of persistedOut) assert.equal(result.body.trunk.name, 'replaced-outbound');
  for (const method of ['GetSIPInboundTrunk', 'GetSIPOutboundTrunk', 'DeleteSIPTrunk']) await pair(`missing-id-${method}`, method, {}, { status: 400 });
  await pair('inbound-update-no-action', 'UpdateSIPInboundTrunk', s => ({ sip_trunk_id: id(s, 'INBOUND') }), { status: 400 });
  await pair('outbound-replace-id-mismatch', 'UpdateSIPOutboundTrunk', s => ({ sip_trunk_id: id(s, 'OUTBOUND'), replace: outbound({ sip_trunk_id: 'ST_other' }) }), { status: 400 });
  await pair('get-missing-inbound', 'GetSIPInboundTrunk', { sip_trunk_id: 'ST_missing_fixture' }, { status: 404 });
  await create('RULE', 'CreateSIPDispatchRule', s => ({ dispatch_rule: { name: 'rule-new-shape', trunk_ids: [id(s, 'INBOUND')], rule: direct('fixture-room') } }), 'sip_dispatch_rule_id', 'rule');
  const filteredRule = await pair('rule-filter-id', 'ListSIPDispatchRule', s => ({ dispatch_rule_ids: [id(s, 'RULE')] }), { status: 200 });
  for (const server of [0, 1]) { assert.equal(filteredRule[server].body.items.length, 1); assert.equal(filteredRule[server].body.items[0].sip_dispatch_rule_id, id(server, 'RULE')); }
  await pair('rule-filter-trunk', 'ListSIPDispatchRule', s => ({ trunk_ids: [id(s, 'INBOUND')] }), { status: 200 });
  await pair('rule-listupdate', 'UpdateSIPDispatchRule', s => ({ sip_dispatch_rule_id: id(s, 'RULE'), update: {
    trunk_ids: { set: [id(s, 'OUTBOUND')] }, name: '', metadata: 'rule-updated', attributes: { locale: 'zh' } } }), { status: 200 });
  await pair('rule-replace', 'UpdateSIPDispatchRule', s => ({ sip_dispatch_rule_id: id(s, 'RULE'), replace: {
    name: 'rule-replaced', trunk_ids: [id(s, 'INBOUND')], rule: direct('fixture-replaced') } }), { status: 200 });
  await pair('rule-delete', 'DeleteSIPDispatchRule', s => ({ sip_dispatch_rule_id: id(s, 'RULE') }), { status: 200 });
  await pair('rule-delete-again-notfound', 'DeleteSIPDispatchRule', s => ({ sip_dispatch_rule_id: id(s, 'RULE') }), { status: 404 });
  await create('LEGACY_RULE', 'CreateSIPDispatchRule', s => ({ name: 'legacy-rule', trunk_ids: [id(s, 'INBOUND')], rule: direct('fixture-legacy') }), 'sip_dispatch_rule_id', 'rule');
  await pair('legacy-rule-read', 'ListSIPDispatchRule', s => ({ dispatch_rule_ids: [id(s, 'LEGACY_RULE')] }), { status: 200 });
  await pair('legacy-rule-delete', 'DeleteSIPDispatchRule', s => ({ sip_dispatch_rule_id: id(s, 'LEGACY_RULE') }), { status: 200 });
  for (const label of ['INBOUND', 'OUTBOUND']) {
    await pair(`delete-${label}`, 'DeleteSIPTrunk', s => ({ sip_trunk_id: id(s, label) }), { status: 200 });
    await pair(`delete-${label}-again-idempotent`, 'DeleteSIPTrunk', s => ({ sip_trunk_id: id(s, label) }), { status: 200 });
  }
  await create('UNKNOWN_HEADER_ENUM', 'CreateSIPInboundTrunk', () => ({ trunk: { numbers: ['+12025550770'], include_headers: 999 } }), 'sip_trunk_id', 'inbound');
  const unknownRead = await pair('unknown-header-enum-retained', 'GetSIPInboundTrunk', x => ({ sip_trunk_id: id(x, 'UNKNOWN_HEADER_ENUM') }), { status: 200 });
  for (const response of unknownRead) assert.equal(response.body.trunk.include_headers, 999);
  await pair('unknown-header-enum-delete', 'DeleteSIPTrunk', x => ({ sip_trunk_id: id(x, 'UNKNOWN_HEADER_ENUM') }), { status: 200 });
  // A patch may pass preflight yet fail final cloned-info validation. Preserve
  // upstream's typed inbound InvalidArgument versus ordinary outbound Internal.
  await create('SOLE_INBOUND', 'CreateSIPInboundTrunk', () => ({ trunk: { numbers: ['+12025550771'] } }), 'sip_trunk_id', 'inbound');
  await create('SOLE_OUTBOUND', 'CreateSIPOutboundTrunk', () => ({ trunk: { address: 'sip.example.invalid', numbers: ['+12025550772'] } }), 'sip_trunk_id', 'outbound');
  for (const [label, suffix, status, number] of [['SOLE_INBOUND', 'Inbound', 400, '+12025550771'], ['SOLE_OUTBOUND', 'Outbound', 500, '+12025550772']]) {
    await pair(`clear-sole-${label}-typed-error`, `UpdateSIP${suffix}Trunk`, x => ({ sip_trunk_id: id(x, label), update: { numbers: { clear: true } } }), { status });
    const unchanged = await pair(`clear-sole-${label}-no-store-mutation`, `GetSIP${suffix}Trunk`, x => ({ sip_trunk_id: id(x, label) }), { status: 200 });
    for (const response of unchanged) assert.deepEqual(response.body.trunk.numbers, [number]);
    await pair(`delete-${label}`, 'DeleteSIPTrunk', x => ({ sip_trunk_id: id(x, label) }), { status: 200 });
  }
  for (const method of ['ListSIPTrunk', 'ListSIPInboundTrunk', 'ListSIPOutboundTrunk', 'ListSIPDispatchRule']) {
    const results = await pair(`final-empty-${method}`, method, {}, { status: 200 });
    for (const result of results) assert.deepEqual(result.body.items, []);
  }
  assert.equal(visited.size, 14);
  assert.equal(evidence.differences.length, 0, 'all HTTP/state differences must be fixed, not ignored');
  evidence.status = 'PASS';
} catch (error) {
  evidence.status = 'FAIL'; evidence.error = { name: error.name, message: error.message }; process.exitCode = 1;
} finally {
  for (const server of [0, 1]) for (const value of owned[server].toReversed()) {
    try { const result = await call(server, value.kind === 'rule' ? 'DeleteSIPDispatchRule' : 'DeleteSIPTrunk',
      value.kind === 'rule' ? { sip_dispatch_rule_id: value.id } : { sip_trunk_id: value.id });
      evidence.cleanup.push({ server, kind: value.kind, id: ids[server].get(value.id), status: result.status });
      if (!(result.status === 200 || (value.kind === 'rule' && result.status === 404))) {
        evidence.status = 'FAIL'; process.exitCode = 1;
      }
    } catch (error) { evidence.cleanup.push({ server, kind: value.kind, error: error.name }); evidence.status = 'FAIL'; process.exitCode = 1; }
  }
  evidence.visitedMethods = [...visited].sort(); evidence.finishedAt = new Date().toISOString();
  const output = process.env.SIP_CONFIG_EVIDENCE || join(interop, 'output/functional-sip-config.json');
  await mkdir(dirname(output), { recursive: true });
  await writeFile(output, JSON.stringify(evidence, null, 2) + '\n');
  console.log(JSON.stringify({ status: evidence.status, methods: visited.size, steps: evidence.steps.length, differences: evidence.differences.length, error: evidence.error }));
}
