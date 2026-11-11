// Actual unchanged ServerSDK -> real Go/Rust HTTP -> actual Go RedisStore read.
// No external worker is faked. This fixture proves management only, not media.
import assert from 'node:assert/strict';
import {createRequire} from 'node:module';
import {resolve} from 'node:path';
import {spawn} from 'node:child_process';
import {writeFile,readFile} from 'node:fs/promises';
import {createHash} from 'node:crypto';
assert.equal(process.env.INGRESS_DEDICATED_BROKERS,'1');
assert(process.env.INTEROP_PROJECT,'INTEROP_PROJECT must point to the existing locked SDK project');
const require=createRequire(resolve(process.env.INTEROP_PROJECT,'package.json'));
const {IngressClient}=require('livekit-server-sdk');
const source=JSON.parse(await readFile(resolve(process.env.INTEROP_PROJECT,'node_modules/livekit-server-sdk/package.json'),'utf8'));
assert.equal(source.version,'2.19.2','unchanged frozen SDK required');
const urls=[process.env.REFERENCE_URL,process.env.LIVEKIT_URL];
const brokers=[process.env.IO_REFERENCE_REDIS_URL,process.env.IO_RUST_REDIS_URL];
assert(urls.every(Boolean)&&brokers.every(Boolean)&&process.env.IO_GO_ORACLE&&process.env.INGRESS_MANAGEMENT_OUTPUT);
const key=process.env.LIVEKIT_API_KEY??'devkey',secret=process.env.LIVEKIT_API_SECRET??'secret';
const audit=[],runs=[],transcripts=[],allCleanup=[];
const sha=x=>createHash('sha256').update(x).digest('hex');
async function oracle(index,mode,commands){
 const child=spawn(process.env.IO_GO_ORACLE,['--mode',mode],{env:{...process.env,IO_REDIS_URL:brokers[index]},stdio:['pipe','pipe','pipe']});
 let stdout='',stderr='';child.stdout.on('data',b=>{stdout+=b;if(stdout.length>4*1024*1024)child.kill('SIGTERM');});child.stderr.on('data',b=>{stderr+=b;});
 const result=new Promise((resolve,reject)=>{child.once('error',reject);child.once('close',(code,signal)=>resolve({code,signal}));});
 child.stdin.end(commands.map(x=>JSON.stringify(x)).join('\n')+'\n');
 const timer=setTimeout(()=>child.kill('SIGTERM'),15000);
 const status=await result;clearTimeout(timer);audit.push({mode,actual_exit_code:status.code,signal:status.signal,stdout_sha256:sha(stdout),stderr_sha256:sha(stderr)});
 assert.equal(status.code,0,'actual fixed Go store process must exit0');
 const rows=stdout.trim().split('\n').filter(Boolean).map(JSON.parse);assert.equal(rows.length,commands.length);
 for(let n=0;n<rows.length;n++)assert.equal(rows[n].case,commands[n].case);
 return rows;
}
async function decode(index,response){return (await oracle(index,'decode',[response]))[0];}
function semantic(row){return {known_json:row.known_json,unknown_tree:row.unknown_tree};}
async function compareDisk(index,info){
 const actual=(await oracle(index,'store',[{case:'actual-persisted-object',op:'GetIngress',id:info.ingressId}]))[0];assert.equal(actual.status,'OK');
 const [http,disk]=await Promise.all([
  decode(index,{case:'http-sdk-known-object',proto_name:'livekit.IngressInfo',wire_base64:Buffer.from(info.toBinary()).toString('base64')}),
  decode(index,actual),
 ]);
 assert.deepEqual(semantic(http),semantic(disk),'full official known projection and unknown tree compare');
 return structuredClone(http.known_json);
}
const report={status:'RUNNING',scope:'actual four public reusable-ingress management APIs and fixed Go RedisStore persistence; active worker/URL pull/RTMP/WHIP media NOT_RUN',runs,processes:audit,cleanup_errors:allCleanup};
try{
 for(let index=0;index<2;index++){
  const client=new IngressClient(urls[index],key,secret,{requestTimeout:15,failover:false});
  assert.equal((await client.listIngress()).length,0,'dedicated initial ingress catalog must be empty');
  const owned=[],records=[],streamKeys=new Set(),run={server:index===0?'fixed-go':'rust',completed_steps:[],management_only:true};runs.push(run);
  try{
   for(const type of [0,1]){
    const info=await client.createIngress(type,{name:'f08-management',roomName:'f08-owned-room',participantIdentity:'f08-controlled-source',participantName:'controlled publisher',participantMetadata:'fixture metadata',enabled:true});owned.push(info.ingressId);
    assert.match(info.ingressId,/^IN_/);assert(info.streamKey.length>0);assert(!streamKeys.has(info.streamKey));streamKeys.add(info.streamKey);
    assert.equal(info.inputType,type);assert.equal(info.reusable,true);assert.equal(info.state.status,0);assert.equal(info.enabled,true);
    assert.equal(info.enableTranscoding,type===0);assert.equal(info.bypassTranscoding,type===1);
    const created=await compareDisk(index,info);
    const updated=await client.updateIngress(info.ingressId,{name:'f08-updated',enabled:false,participantMetadata:'updated metadata é'});
    assert.equal(updated.enabled,false);assert.equal(updated.name,'f08-updated');assert.equal(updated.participantMetadata,'updated metadata é');
    const stored=await compareDisk(index,updated);
    const listed=await client.listIngress({ingressId:info.ingressId});assert.equal(listed.length,1);assert.deepEqual(listed[0].toJson(),updated.toJson());
    const sanitize=value=>({...value,ingress_id:'<created-ordinal-'+type+'>',stream_key:'<unique-random-stream-key>'});
    records.push({type,created:sanitize(created),updated:sanitize(stored)});run.completed_steps.push({type,create:true,actual_store_read:true,update_optional_false:true,list_exact_object:true});
   }
   const roomItems=await client.listIngress({roomName:'f08-owned-room'});assert.equal(roomItems.length,2);assert.deepEqual(new Set(roomItems.map(x=>x.ingressId)),new Set(owned));
   // Fixed source RedisStore.ListIngress does not promise order. Membership and
   // count are asserted; no business-field contents are removed from ID reads.
   for(const id of [...owned]){
    const result=await client.deleteIngress(id);assert.equal(result.ingressId,id);assert.equal(result.state.status,0);
    const missing=(await oracle(index,'store',[{case:'actual-delete-removes-store-object',op:'GetIngress',id}]))[0];assert.equal(missing.status,'ERROR');assert.equal(missing.code,'not_found');
    await assert.rejects(client.listIngress({ingressId:id}),error=>error.code==='not_found');
    owned.splice(owned.indexOf(id),1);
   }
   assert.equal((await client.listIngress()).length,0);run.room_membership_count=2;run.deleted_objects=2;run.final_catalog_empty=true;transcripts.push(records);
  }finally{
   for(const id of owned){
    try{await client.deleteIngress(id);}catch{allCleanup.push({server:index,kind:'owned public ingress cleanup failed'});}
    try{const left=(await oracle(index,'store',[{case:'owned-final-cleanup-read',op:'GetIngress',id}]))[0];
     if(left.status==='OK'){const removed=(await oracle(index,'store',[{case:'owned-final-real-store-cleanup',op:'DeleteIngress',id}]))[0];assert.equal(removed.status,'OK');allCleanup.push({server:index,kind:'public delete left object; exact owned store cleanup required'});}
    }catch{allCleanup.push({server:index,kind:'owned store cleanup could not be verified'});}
   }
  }
 }
 assert.deepEqual(transcripts[1],transcripts[0],'Go/Rust full management object semantics after declared random identity substitution');
 assert.equal(allCleanup.length,0);report.status='PASS';report.normalization='Only validated random ingressID/unique streamKey replaced; fixed-source unordered room membership compared as exact set+count';
 report.transcript_sha256=sha(JSON.stringify(transcripts[0]));
}catch(error){report.status='FAIL';report.failure={name:error.name,code:error.code??null};}
finally{if(allCleanup.length)report.status='FAIL';await writeFile(process.env.INGRESS_MANAGEMENT_OUTPUT,JSON.stringify(report,null,2)+'\n');}
if(report.status!=='PASS')process.exitCode=1;
console.log(report.status+' actual ingress management/Go-store scope');
