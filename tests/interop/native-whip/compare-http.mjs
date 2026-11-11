// Paired actual full-server result comparator, no handwritten Rust expectations.
import assert from 'node:assert/strict';import{readFile,writeFile}from'node:fs/promises';
const[goPath,rustPath,output]=process.argv.slice(2);assert(goPath&&rustPath&&output);
const go=JSON.parse(await readFile(goPath,'utf8')),rust=JSON.parse(await readFile(rustPath,'utf8'));
assert.equal(go.count,rust.count);let checked=0;const mismatches=[];
for(let i=0;i<go.results.length;i++){const a=go.results[i],b=rust.results[i];assert.equal(a.id,b.id);for(const key of ['status','headers','body']){try{assert.deepEqual(b[key],a[key]);}catch{mismatches.push({id:a.id,field:key,expected:a[key],actual:b[key]});}}checked++;}
await writeFile(output,JSON.stringify({status:mismatches.length?'FAIL':'PASS',scope:'actual fixed official server versus actual Rust public WHIP HTTP results, exact fields; no native success implied',checked,mismatches},null,2)+'\n');if(mismatches.length)process.exitCode=1;
