import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import test from 'node:test';
import { duplicateJsonCases } from './service-json-corpus.mjs';
const contracts = JSON.parse(await readFile(new URL('./service-contracts.json', import.meta.url)));

test('all 47 official request descriptors generate actual known-field duplicates', () => {
  assert.equal(contracts.methods.length, 47);
  for (const contract of contracts.methods) {
    const cases = duplicateJsonCases(contract);
    const known = cases.find(c => c.name === 'duplicate-known-scalar');
    assert.ok(known.expectMalformed);
    assert.ok(known.fieldPath.length >= 1);
    assert.ok(Object.keys(JSON.parse(known.body)).length > 0);
    assert.ok(cases.some(c => c.name === 'duplicate-unknown-field' && !c.expectMalformed));
  }
});
test('nested messages, repeated scalars and both alias directions retain duplicate tokens', () => {
  const find = name => duplicateJsonCases(contracts.methods.find(c => c.request === name));
  const nested = find('CreateSIPInboundTrunkRequest')[0];
  assert.equal(nested.fieldPath[0], 'trunk');
  assert.match(nested.body, /"sipTrunkId":"first","sipTrunkId":"second"/);
  assert.equal(find('ListRoomsRequest')[0].body, '{"names":["first"],"names":["second"]}');
  const aliases = find('CreateAgentDispatchRequest');
  assert.ok(aliases.find(c => c.name === 'duplicate-snake-camel').body.includes('"agent_name":"first","agentName":"second"'));
  assert.ok(aliases.find(c => c.name === 'duplicate-camel-snake').body.includes('"agentName":"first","agent_name":"second"'));
});
