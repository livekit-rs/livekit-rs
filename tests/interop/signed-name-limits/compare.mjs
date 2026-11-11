import assert from 'node:assert/strict';import{readFile,writeFile}from'node:fs/promises';
const [goPath,rustPath,out]=process.argv.slice(2);assert.ok(goPath&&rustPath&&out);
const go=JSON.parse(await readFile(goPath)),rust=JSON.parse(await readFile(rustPath));
assert.equal(go.status,'PASS_SCOPED');assert.equal(rust.status,'PASS_SCOPED');
assert.deepEqual(rust.limits,go.limits);assert.equal(rust.profile,go.profile);
assert.deepEqual(rust.observations,go.observations,'strict actual configured HTTP/signal outcomes/state');
assert.deepEqual(go.cleanupErrors,[]);assert.deepEqual(rust.cleanupErrors,[]);
await writeFile(out,JSON.stringify({status:'PASS_SCOPED',profile:go.profile,cases:go.observations.length,scope:go.scope},null,2)+'\n');
