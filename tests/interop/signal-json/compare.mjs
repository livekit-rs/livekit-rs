/** Strict actual Go/Rust consumer outcome comparison, including close metadata. */
import assert from 'node:assert/strict';
import {readFile,writeFile} from 'node:fs/promises';
import {createHash} from 'node:crypto';
const [left,right,output]=process.argv.slice(2);assert.ok(left&&right&&output);
const raw=await Promise.all([left,right].map(p=>readFile(p))),[go,rust]=raw.map(b=>JSON.parse(b));
const result={status:'RUNNING',scope:'actual same-input local WS JSON outcomes and close code/clean/reason',inputSha256:raw.map(b=>createHash('sha256').update(b).digest('hex'))};
try{assert.equal(go.status,'PASS_SCOPED');assert.equal(rust.status,'PASS_SCOPED');assert.deepEqual(go.cleanupErrors,[]);assert.deepEqual(rust.cleanupErrors,[]);assert.deepEqual(rust,go);result.status='PASS_SCOPED';result.cases=go.cases.length;}
catch(error){result.status='FAIL';result.error=String(error);process.exitCode=1;}
finally{await writeFile(output,JSON.stringify(result,null,2)+'\n');}
