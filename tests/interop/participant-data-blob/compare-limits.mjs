import assert from 'node:assert/strict';
import {readFile,writeFile} from 'node:fs/promises';
import {createHash} from 'node:crypto';
const [leftPath,rightPath,output]=process.argv.slice(2);assert.ok(leftPath&&rightPath&&output);
const raw=await Promise.all([leftPath,rightPath].map(p=>readFile(p)));const [go,rust]=raw.map(b=>JSON.parse(b));
const result={status:'RUNNING',scope:'actual configured local Go/Rust Blob profile observations',profile:go.profile,inputs:raw.map(b=>createHash('sha256').update(b).digest('hex'))};
try{
 assert.equal(go.status,'PASS_SCOPED');assert.equal(rust.status,'PASS_SCOPED');assert.deepEqual(go.cleanupErrors,[]);assert.deepEqual(rust.cleanupErrors,[]);
 // The optional full-server config digest can differ due to each local listen
 // address. The declared functional config itself is compared byte-for-byte
 // via the parsed tree, and every result/step/error/payload/cohort is retained.
 const a=structuredClone(go),b=structuredClone(rust);delete a.declaredServerConfigSha256;delete b.declaredServerConfigSha256;
 assert.deepEqual(a,b);result.status='PASS_SCOPED';result.steps=go.steps.length;
}catch(error){result.status='FAIL';result.error=String(error);process.exitCode=1;}
finally{await writeFile(output,JSON.stringify(result,null,2)+'\n');}
