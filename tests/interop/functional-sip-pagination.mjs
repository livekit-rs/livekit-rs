/** Real catalog fixtures + original public ListPageIter behavior, including EOF holes. */
import assert from 'node:assert/strict';
import {createRequire} from 'node:module';
import {spawnSync} from 'node:child_process';
import {randomUUID,createHash} from 'node:crypto';
import {mkdir,writeFile} from 'node:fs/promises';
import {join,dirname} from 'node:path';
import {fileURLToPath} from 'node:url';
const here=dirname(fileURLToPath(import.meta.url));
const {AccessToken}=createRequire(join(here,'package.json'))('livekit-server-sdk');
const urls=[process.env.REFERENCE_URL,process.env.LIVEKIT_URL],brokers=[process.env.REFERENCE_REDIS_URL,process.env.LIVEKIT_REDIS_URL];
assert.ok(urls.every(Boolean)&&brokers.every(Boolean)&&process.env.SIP_CATALOG_GO_BIN,'actual endpoints, dedicated brokers and fixed Go catalog oracle required');assert.equal(process.env.SIP_PAGINATION_DEDICATED,'true');
const run=randomUUID(), prefix='ST_f04_'+run+'_';const ids=Array.from({length:1005},(_,i)=>prefix+String(i).padStart(4,'0'));
const vari=n=>{const a=[];while(n>127){a.push((n&127)|128);n=Math.floor(n/128)}return Buffer.from([...a,n])};
const field=(n,s)=>{const b=Buffer.from(s);return Buffer.concat([vari(n*8+2),vari(b.length),b])};
const digest=v=>createHash('sha256').update(JSON.stringify(v)).digest('hex');
const evidence={profile:'functional-sip-public-pagination',status:'RUNNING',scope:'Real public ListPageIter over1005 fixed Go RedisStore-written records, including source-defined EOF-before-append filtering behavior. Not worker/telephony.',cases:[],cleanup:[]};
const owned=[false,false];
function store(server,commands){const data=commands.map(c=>JSON.stringify({...c,run_id:run})+'\n').join('');const p=spawnSync(process.env.SIP_CATALOG_GO_BIN,['--redis',brokers[server],'--run-id',run],{input:data,encoding:'utf8',timeout:75000,maxBuffer:8*1024*1024});assert.equal(p.status,0,'actual fixed Go catalog process must execute');const rows=p.stdout.trim().split('\n').map(JSON.parse);assert.equal(rows.length,commands.length);for(let i=0;i<rows.length;i++){assert.equal(rows[i].run_id,run);assert.equal(rows[i].op,commands[i].op);assert.ok(!rows[i].error,'actual catalog operation failed')}return rows;}
async function call(server,method,body){const token=new AccessToken(server===0?process.env.REFERENCE_API_KEY||'devkey':process.env.LIVEKIT_API_KEY||'devkey',server===0?process.env.REFERENCE_API_SECRET||'secret':process.env.LIVEKIT_API_SECRET||'secret');token.addSIPGrant({admin:true});const r=await fetch(urls[server]+'/twirp/livekit.SIP/'+method,{method:'POST',headers:{'content-type':'application/json',authorization:'Bearer '+await token.toJwt()},body:JSON.stringify(body)});return {status:r.status,body:await r.json()};}
try{
 for(let server=0;server<2;server++){
  const before=await call(server,'ListSIPInboundTrunk',{});assert.equal(before.status,200);assert.deepEqual(before.body.items,[],'fresh owned SIP namespace required');owned[server]=true;
  store(server,ids.map((id,i)=>({op:'store_inbound',id,protobuf:Buffer.concat([field(1,id),field(2,'page-fixture'),field(4,i===1004?'+12025559999':'+1202555'+String(i).padStart(4,'0'))]).toString('base64')})));
 }
 const cases=[['all','ListSIPInboundTrunk',{},1005],['default-page','ListSIPInboundTrunk',{page:{limit:0}},1005],['two-pages','ListSIPInboundTrunk',{page:{limit:1000}},1005],['five-pages','ListSIPInboundTrunk',{page:{limit:250}},1005],['cursor','ListSIPInboundTrunk',{page:{after_id:ids[999],limit:2}},5],['number-match-unpaged','ListSIPInboundTrunk',{numbers:['+12025559999']},1],['number-filter-empty-first-page','ListSIPInboundTrunk',{numbers:['+12025559999'],page:{limit:1000}},0],['id-match-unpaged','ListSIPInboundTrunk',{trunk_ids:[ids[1004]]},1],['id-filter-empty-first-page','ListSIPInboundTrunk',{trunk_ids:[ids[1004]],page:{limit:1000}},0],['legacy-paged','ListSIPTrunk',{page:{limit:1000}},1005]];
 for(const [name,method,body,count]of cases){const results=[];for(let server=0;server<2;server++)results.push(await call(server,method,body));assert.equal(results[0].status,200);assert.equal(results[1].status,200);assert.equal(results[0].body.items.length,count,'actual upstream public iterator cardinality');assert.deepEqual(results[1].body,results[0].body,'all public fields/order must match');evidence.cases.push({name,status:'PASS',actual_count:count,canonical_body_sha256:digest(results[0].body)});}
 evidence.status='PASS';
}catch(error){evidence.status='FAIL';evidence.error={name:error.name,message:error.message};process.exitCode=1;}
finally{
 for(let server=0;server<2;server++)if(owned[server]){try{store(server,ids.map(id=>({op:'delete_trunk',id,protobuf:''})));const after=await call(server,'ListSIPInboundTrunk',{});assert.equal(after.status,200);assert.deepEqual(after.body.items,[]);evidence.cleanup.push({server,owned_records_removed:true})}catch(error){evidence.status='FAIL';evidence.cleanup.push({server,error:error.name});process.exitCode=1;}}
 const output=process.env.SIP_PAGINATION_EVIDENCE||join(here,'output/functional-sip-pagination.json');await mkdir(dirname(output),{recursive:true});await writeFile(output,JSON.stringify(evidence,null,2)+'\n');console.log(JSON.stringify({status:evidence.status,cases:evidence.cases.length,error:evidence.error}));
}
