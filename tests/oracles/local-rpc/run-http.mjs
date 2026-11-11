// Coordinator executes against an already running isolated actual server.
// This fixture never starts servers, fabricates worker callbacks, or logs tokens.
import assert from 'node:assert/strict';
import {createRequire} from 'node:module';
import {readFile,writeFile} from 'node:fs/promises';
import {dirname,join,resolve} from 'node:path';
import {createHash,randomUUID} from 'node:crypto';
const interop=process.env.INTEROP_ROOT;assert(interop,'INTEROP_ROOT required');
const require=createRequire(join(resolve(interop),'package.json'));
const sdkEntry=require.resolve('livekit-server-sdk');
let packagePath=dirname(sdkEntry),actualPackage;
for(let i=0;i<6;i++){try{const p=join(packagePath,'package.json');const parsed=JSON.parse(await readFile(p,'utf8'));if(parsed.name==='livekit-server-sdk'){actualPackage={path:p,value:parsed};break;}}catch{}packagePath=dirname(packagePath);}
assert(actualPackage,'actual resolved SDK package not found');assert.equal(actualPackage.value.version,'2.19.2');
const {AccessToken}=require('livekit-server-sdk');
const base=process.env.LIVEKIT_URL,apiKey=process.env.LIVEKIT_API_KEY,secret=process.env.LIVEKIT_API_SECRET;
assert(base&&apiKey&&secret&&process.env.RESULT&&process.env.CONFIG_FILE,'URL, credentials, RESULT, CONFIG_FILE required');
assert(/^https?:\/\/127\.0\.0\.1:\d+$/.test(base),'owned loopback server only');
const profile=process.env.PROFILE;assert(['normal','observe-tiny','auto-create-disabled'].includes(profile));
const referencePath=process.env.REFERENCE_RESULT;const reference=referencePath?JSON.parse(await readFile(referencePath,'utf8')):undefined;
if(reference){assert.equal(profile,'auto-create-disabled');assert.equal(reference.profile,profile);assert.equal(reference.status,'OBSERVED_NOT_ACCEPTANCE');assert.equal(reference.rows.length,3);assert.deepEqual(reference.cleanupErrors,[]);}

const sha=b=>createHash('sha256').update(b).digest('hex');
const run=randomUUID();const rooms=new Set();const rows=[];const cleanupErrors=[];
const report={status:'RUNNING',profile,scope:'actual public AgentDispatch local auto-create; no worker assignment claim',
 sdk:{version:actualPackage.value.version,entrySha256:sha(await readFile(sdkEntry)),packageSha256:sha(await readFile(actualPackage.path)),lockSha256:sha(await readFile(join(interop,'package-lock.json')))},
 configSha256:sha(await readFile(process.env.CONFIG_FILE)),referenceSha256:referencePath?sha(await readFile(referencePath)):null,rows,cleanupErrors};
async function token(room,cleanup=false){const t=new AccessToken(apiKey,secret);t.addGrant(cleanup?{roomCreate:true,roomList:true,roomAdmin:true,room}:{roomAdmin:true,room,roomList:true});return t.toJwt();}
async function rpc(service,method,body,room,cleanup=false){const res=await fetch(`${base}/twirp/livekit.${service}/${method}`,{method:'POST',headers:{authorization:`Bearer ${await token(room,cleanup)}`,'content-type':'application/json'},body:JSON.stringify(body),signal:AbortSignal.timeout(15000)});const text=await res.text();let value;try{value=JSON.parse(text);}catch{throw Error(`non-JSON ${service}/${method} status=${res.status} bodyHash=${sha(text)}`);}return{status:res.status,value};}
async function listRoom(room){const out=await rpc('RoomService','ListRooms',{names:[room]},room,true);assert.equal(out.status,200);return out.value.rooms??[];}
function outcome(out){return out.status===200?{status:200}:{status:out.status,code:out.value.code,msg:out.value.msg};}
try{
 const iterations=profile==='normal'?1:3;
 for(let ordinal=0;ordinal<iterations;ordinal++){
  const room=`agent-policy-${run}-${ordinal}`;rooms.add(room);
  const body={room,agent_name:'controlled-policy-worker',metadata:'real request metadata',attributes:{controlled:'actual-http'}};
  const invalid=await rpc('AgentDispatchService','CreateDispatch',{...body,deployment:'bad_name'},room);
  assert.equal(invalid.status,400);assert.equal(invalid.value.code,'invalid_argument');assert.equal(invalid.value.msg,"deployment contains reserved character '_'");
  assert.equal((await listRoom(room)).length,0,'validation must precede owner creation');
  const created=await rpc('AgentDispatchService','CreateDispatch',body,room);
  const row={ordinal,create:outcome(created),roomPresenceAfterCalls:[]};rows.push(row);
  // Preserve every observation. No retry of the business request to hide a
  // tiny-deadline failure; no prefilled Go expected result for that profile.
  for(let observation=0;observation<3;observation++)row.roomPresenceAfterCalls.push((await listRoom(room)).length);
  if(profile==='observe-tiny'){
   if(created.status===200){assert.equal(created.value.agent_name,'controlled-policy-worker');assert.equal(created.value.metadata,body.metadata);assert.equal((created.value.state?.jobs??[]).length,0);row.successHasRealRecord=true;}
   continue;
  }
  if(profile==='auto-create-disabled'){
   // Source-first capture: do not canonize the inherited Rust404 behavior.
   // Auth/deployment and actual ListRooms controls above still must succeed.
   assert.notEqual(created.status,200,'disabled missing-room call cannot fabricate dispatch success');
   assert.equal(typeof created.value.code,'string');assert.equal(typeof created.value.msg,'string');
   assert.deepEqual(row.roomPresenceAfterCalls,[0,0,0]);
   if(reference){const expected=reference.rows[ordinal];assert.equal(expected.ordinal,ordinal);assert.deepEqual(row.create,expected.create,'exact actual fixedGo status/code/msg');assert.deepEqual(row.roomPresenceAfterCalls,expected.roomPresenceAfterCalls,'exact real room effects');}
   continue;
  }
  assert.equal(created.status,200);const first=created.value;assert(first.id);
  assert.equal(first.agent_name,body.agent_name);assert.equal(first.metadata,body.metadata);assert.deepEqual(first.attributes,body.attributes);assert.equal((first.state?.jobs??[]).length,0);
  const before=await listRoom(room);assert.equal(before.length,1);const sid=before[0].sid;assert(sid);
  const second=await rpc('AgentDispatchService','CreateDispatch',{...body,metadata:'second actual record'},room);assert.equal(second.status,200);assert.notEqual(second.value.id,first.id);
  const listed=await rpc('AgentDispatchService','ListDispatch',{room},room);assert.equal(listed.status,200);
  const dispatches=listed.value.agent_dispatches??[];assert.equal(dispatches.length,3);assert.equal(dispatches.filter(x=>!x.agent_name).length,1);assert.equal(dispatches.filter(x=>x.agent_name===body.agent_name).length,2);
  const filtered=await rpc('AgentDispatchService','ListDispatch',{room,dispatch_id:first.id},room);assert.equal(filtered.status,200);assert.equal(filtered.value.agent_dispatches.length,1);assert.equal(filtered.value.agent_dispatches[0].metadata,body.metadata);
  const removed=await rpc('AgentDispatchService','DeleteDispatch',{room,dispatch_id:first.id},room);assert.equal(removed.status,200);assert.equal(removed.value.id,first.id);
  const absent=await rpc('AgentDispatchService','ListDispatch',{room,dispatch_id:first.id},room);assert.equal(absent.status,200);assert.equal((absent.value.agent_dispatches??[]).length,0);
  const again=await rpc('AgentDispatchService','DeleteDispatch',{room,dispatch_id:first.id},room);assert.equal(again.status,404);assert.equal(again.value.code,'not_found');
  const after=await listRoom(room);assert.equal(after.length,1);assert.equal(after[0].sid,sid);
  row.normalConsumer={sameRoomSid:true,explicitRecords:2,unnamedDefaults:1,metadataPreserved:true,attributesPreserved:true,filteredRealRecord:true,deletedRealRecord:true,noFabricatedJobs:true};
 }
 report.status=profile==='observe-tiny'||(profile==='auto-create-disabled'&&!reference)?'OBSERVED_NOT_ACCEPTANCE':'PASS';
}catch(e){report.status='FAIL';report.failure={kind:e?.name??'Error',message:e?.message??'fixture failed'};process.exitCode=1;}
finally{
 for(const room of rooms){try{if((await listRoom(room)).length){const out=await rpc('RoomService','DeleteRoom',{room},room,true);assert.equal(out.status,200);}assert.equal((await listRoom(room)).length,0);}catch(e){cleanupErrors.push({ordinal:[...rooms].indexOf(room),kind:e?.name??'Error'});}}
 if(cleanupErrors.length){report.status='FAIL';process.exitCode=1;}
 await writeFile(process.env.RESULT,JSON.stringify(report,null,2)+'\n');
}
