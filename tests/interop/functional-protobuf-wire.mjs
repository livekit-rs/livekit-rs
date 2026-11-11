// Genuine public HTTP parser-order differential. Authentication failures prove
// only successful request decoding, not backend execution or feature support.
import {readFile,writeFile} from 'node:fs/promises';
import assert from 'node:assert/strict';
const input=JSON.parse(await readFile(new URL('../oracles/protobuf-wire/http-cases.json',import.meta.url),'utf8'));
const reference=process.env.REFERENCE_URL, rust=process.env.LIVEKIT_URL;
assert(reference&&rust,'both real server URLs required');
const rows=[];let failed=false;
for(const c of input.cases){
 const pair={name:c.name,purpose:c.purpose};
 for(const [name,url] of [['reference',reference],['rust',rust]]){
  try{const r=await fetch(`${url}/twirp/livekit.${c.service}/${c.method}`,{method:'POST',headers:{'content-type':c.content_type},body:Buffer.from(c.body_base64,'base64')});pair[name]={status:r.status,content_type:r.headers.get('content-type'),body:await r.json()};}
  catch(e){pair[name]={transport_error:e.message};failed=true;}
 }
 try{assert.deepEqual(pair.rust,pair.reference);}catch{failed=true;}
 rows.push(pair);
}
const result={status:failed?'FAIL':'PASS',scope:'two actual fixed Go/Rust public Twirp binary decode-before-auth cases; no successful backend/worker claim',cases:rows};
if(process.env.WIRE_HTTP_EVIDENCE)await writeFile(process.env.WIRE_HTTP_EVIDENCE,JSON.stringify(result,null,2)+'\n');
console.log(`${result.status} ${rows.length} public binary preflight comparisons`);if(failed)process.exitCode=1;
