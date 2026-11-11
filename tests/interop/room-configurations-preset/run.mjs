/** Real paired effect of the YAML `room.room_configurations.{key}` NAMED PRESET
 * consumer: against a prestarted fixed-Go reference and Rust server sharing one
 * YAML profile, issue CreateRoom with `room_preset` and require Rust to deep-equal
 * the fixed Go on the EFFECTIVE room fields the public Room response echoes
 * (empty_timeout / departure_timeout / max_participants / metadata). No SDK, no
 * media, no WS signaling, no fake responder. */
import assert from 'node:assert/strict';
import {createHmac} from 'node:crypto';
import {writeFile} from 'node:fs/promises';
const urls=[process.env.REFERENCE_URL,process.env.LIVEKIT_URL];
assert.ok(urls.every(Boolean),'both actual reference and Rust HTTP URLs required');
const key='devkey',secret='secret';
const profile=process.env.RC_PROFILE;
assert.ok(profile,'RC_PROFILE required');
const report={status:'RUNNING',profile,scope:'Actual configured room.room_configurations.{key} named-preset application to CreateRoom effective room empty_timeout/departure_timeout/max_participants/metadata (Go roomallocator.go applyNamedRoomConfiguration == Rust manager.rs inherit); not sync_streams/agents (internal-only), CLI per-key flag, ENV, media or webhook.',observations:[]};
function tk(identity,video){const e=x=>Buffer.from(JSON.stringify(x)).toString('base64url');const s=`${e({alg:'HS256',typ:'JWT'})}.${e({iss:key,sub:identity||'u',exp:Math.floor(Date.now()/1000)+3600,video})}`;return s+'.'+createHmac('sha256',secret).update(s).digest('base64url');}
async function createRoom(server,name,body){const r=await fetch(`${urls[server].replace(/\/$/,'')}/twirp/livekit.RoomService/CreateRoom`,{method:'POST',headers:{'content-type':'application/json',authorization:`Bearer ${tk(name,{room:name,roomCreate:true,roomAdmin:true,roomList:true})}`},body:JSON.stringify({name,...body}),signal:AbortSignal.timeout(10000)});return{status:r.status,body:await r.text()};}
async function del(server,name){await fetch(`${urls[server].replace(/\/$/,'')}/twirp/livekit.RoomService/DeleteRoom`,{method:'POST',headers:{'content-type':'application/json',authorization:`Bearer ${tk(name,{room:name,roomCreate:true,roomAdmin:true,roomList:true})}`},body:JSON.stringify({room:name})});}
// Same request sequence against each server; each name unique so no room is reused.
const CASES=[
 {case:'preset-applied',name:'cfg-preset',body:{room_preset:'presetA'}},
 {case:'no-preset-default',name:'cfg-default',body:{}},
 {case:'request-override',name:'cfg-override',body:{room_preset:'presetA',empty_timeout:5}},
 {case:'unknown-preset',name:'cfg-bogus',body:{room_preset:'nosuch'}},
 {case:'empty-preset-name',name:'cfg-emptypreset',body:{room_preset:''}},
 {case:'request-wins-multi',name:'cfg-both',body:{room_preset:'presetA',max_participants:9,metadata:'req-md'}},
];
async function scenario(server){
 const out=[];
 for(const c of CASES){
  const r=await createRoom(server,c.name,c.body);
  const rec={case:c.case,req:c.body,status:r.status};
  if(r.status===200){const j=JSON.parse(r.body);rec.room={empty_timeout:j.empty_timeout,departure_timeout:j.departure_timeout,max_participants:j.max_participants,metadata:j.metadata,name:j.name};await del(server,c.name);}
  else{const j=JSON.parse(r.body);rec.error={code:j.code,msg:j.msg};}
  out.push(rec);
 }
 const by=Object.fromEntries(out.map(o=>[o.case,o]));
 if(profile==='preset'){
  // Named preset present: fills every UNSET field; the request itself wins where set.
  assert.equal(by['preset-applied'].room?.empty_timeout,41,'preset empty_timeout not applied');
  assert.equal(by['preset-applied'].room?.departure_timeout,7,'preset departure_timeout not applied');
  assert.equal(by['preset-applied'].room?.max_participants,3,'preset max_participants not applied');
  assert.equal(by['preset-applied'].room?.metadata,'from-preset','preset metadata not applied');
  assert.equal(by['no-preset-default'].room?.empty_timeout,300,'global default must apply when unnamed');
  assert.equal(by['no-preset-default'].room?.metadata,'','global default metadata empty');
  assert.equal(by['request-override'].room?.empty_timeout,5,'explicit request must win over preset');
  assert.equal(by['request-override'].room?.departure_timeout,7,'unset field still from preset');
  assert.equal(by['empty-preset-name'].room?.empty_timeout,300,'empty preset name == no preset');
  assert.equal(by['request-wins-multi'].room?.max_participants,9,'request max_participants wins');
  assert.equal(by['request-wins-multi'].room?.metadata,'req-md','request metadata wins');
  assert.equal(by['request-wins-multi'].room?.empty_timeout,41,'unset still preset');
  assert.equal(by['unknown-preset'].status,400,'unknown preset must reject');
  assert.equal(by['unknown-preset'].error?.msg,'unknown room configuration in create room request','unknown-preset message');
 }else{
  // Absent preset map: every presetA request 400s (config actually consumed), while
  // unnamed requests still get the global defaults.
  assert.equal(by['preset-applied'].status,400,'undefined preset must 400 (config consumed)');
  assert.equal(by['preset-applied'].error?.msg,'unknown room configuration in create room request');
  assert.equal(by['request-override'].status,400,'presetA still unknown even with an explicit field');
  assert.equal(by['request-wins-multi'].status,400);
  assert.equal(by['no-preset-default'].room?.empty_timeout,300,'global default still applies');
  assert.equal(by['empty-preset-name'].room?.empty_timeout,300);
  assert.equal(by['unknown-preset'].status,400);
 }
 return out;
}
try{
 const reference=await scenario(0);
 const rust=await scenario(1);
 report.observations={reference,rust};
 assert.deepEqual(rust,reference,'Rust room_configurations preset application diverged from fixed Go');
 report.status='PASS_SCOPED';
}catch(error){
 report.status='FAIL';report.error={name:error.name,message:String(error.message).replace(/eyJ[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+/g,'<token>')};process.exitCode=1;
}finally{
 await writeFile(process.env.RC_OUTPUT,JSON.stringify(report,null,2)+'\n');
 console.log(JSON.stringify({status:report.status,profile,cases:report.observations?.reference?.length}));
}
