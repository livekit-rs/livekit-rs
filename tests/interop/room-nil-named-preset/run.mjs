/** OBSERVATION harness for CONFIG:NilNamedPresetPointer. A named preset whose map
 * value is nil (`room_configurations: {nilpreset: ~}`) is a genuine upstream edge:
 * Go roomallocator.go:211-219 looks the key up (ok==true) then dereferences
 * conf.EmptyTimeout while conf==nil. This harness does NOT assume the outcome — it
 * RECORDS what the fixed Go actually does (connection abort from an unrecovered
 * panic, or a recovered 5xx) and what Rust does (manager.rs:672-674 standalone
 * local-owner nil path -> a clean structured 500 internal "Internal service panic",
 * server survives; the distributed catalog.rs:735 PresetNil is a separate anchor not
 * exercised here). Two parity
 * controls (unknown preset key -> 400 identical message; valid preset -> 200 with
 * inherited fields) prove the preset map is genuinely consumed on both sides, so the
 * nil case is a real dereference path and not an ignored config. No SDK, no media, no
 * WS signaling, no fake responder. The nil case is a recorded divergence (R03), not a
 * parity claim. */
import assert from 'node:assert/strict';
import {createHmac} from 'node:crypto';
import {writeFile} from 'node:fs/promises';
const urls=[process.env.REFERENCE_URL,process.env.LIVEKIT_URL];
assert.ok(urls.every(Boolean),'both actual reference and Rust HTTP URLs required');
const key='devkey',secret='secret';
const scrub=s=>String(s).replace(/eyJ[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+/g,'<token>');
const report={status:'RUNNING',scope:'Actual fixed-Go vs Rust observable behavior of a NIL named-preset pointer (room_configurations.nilpreset=~) on public RoomService.CreateRoom, plus unknown-preset and valid-preset parity controls. Records the Go nil-deref outcome verbatim; Rust returns a clean internal error instead of crashing (R03 decision). Not a parity claim on the nil case; not sync_streams/agents internal fields, CLI per-key flag, ENV, media or webhook.',observations:{}};
function tk(name){const e=x=>Buffer.from(JSON.stringify(x)).toString('base64url');const s=`${e({alg:'HS256',typ:'JWT'})}.${e({iss:key,sub:'u',exp:Math.floor(Date.now()/1000)+3600,video:{room:name,roomCreate:true,roomAdmin:true,roomList:true}})}`;return s+'.'+createHmac('sha256',secret).update(s).digest('base64url');}
async function createRoom(server,name,body){
 try{
  const r=await fetch(`${urls[server].replace(/\/$/,'')}/twirp/livekit.RoomService/CreateRoom`,{method:'POST',headers:{'content-type':'application/json',authorization:`Bearer ${tk(name)}`},body:JSON.stringify({name,...body}),signal:AbortSignal.timeout(10000)});
  const text=await r.text();let parsed=null;try{parsed=JSON.parse(text);}catch{}
  return {status:r.status,parsed,network_error:null};
 }catch(e){
  // A Go nil-deref panic that net/http does not recover aborts the connection: fetch
  // rejects (ECONNRESET / socket hang up / EOF). Record it as the observable outcome.
  return {status:null,parsed:null,network_error:scrub(e&&e.cause?e.cause:e)};
 }
}
// nil-preset LAST: if the Go process dies on the dereference, the two parity controls
// are already captured. Each name is unique so no room is reused.
const CASES=[
 {case:'unknown-preset',name:'np-unknown',body:{room_preset:'absent-preset'}},
 {case:'valid-preset',name:'np-valid',body:{room_preset:'validpreset'}},
 {case:'nil-preset',name:'np-nil',body:{room_preset:'nilpreset'}},
];
async function scenario(server){
 const out=[];
 for(const c of CASES){
  const r=await createRoom(server,c.name,c.body);
  const rec={case:c.case,req:c.body,status:r.status,network_error:r.network_error};
  if(r.status===200&&r.parsed){rec.room={name:r.parsed.name,empty_timeout:r.parsed.empty_timeout,departure_timeout:r.parsed.departure_timeout,max_participants:r.parsed.max_participants,metadata:r.parsed.metadata};}
  else if(r.parsed){rec.error={code:r.parsed.code,msg:r.parsed.msg,meta:r.parsed.meta};}
  out.push(rec);
 }
 return out;
}
try{
 const reference=await scenario(0);
 const rust=await scenario(1);
 report.observations={reference,rust};
 const g=Object.fromEntries(reference.map(o=>[o.case,o]));
 const r=Object.fromEntries(rust.map(o=>[o.case,o]));
 // Parity control 1: an absent preset key is a clean 400 with the identical message.
 assert.equal(g['unknown-preset'].status,400,'oracle unknown-preset must 400');
 assert.equal(g['unknown-preset'].error?.msg,'unknown room configuration in create room request','oracle unknown-preset message');
 assert.deepEqual(r['unknown-preset'],g['unknown-preset'],'unknown-preset must be byte-parity');
 // Parity control 2: a valid preset is consumed and its unset fields inherited (200).
 assert.equal(g['valid-preset'].status,200,'oracle valid-preset must 200');
 assert.equal(r['valid-preset'].status,200,'Rust valid-preset must 200');
 assert.deepEqual(r['valid-preset'].room,g['valid-preset'].room,'valid-preset inherited fields must be parity');
 assert.equal(g['valid-preset'].room?.empty_timeout,30,'preset empty_timeout not inherited');
 assert.equal(g['valid-preset'].room?.metadata,'valid-preset-md','preset metadata not inherited');
 // Divergence (R03): Rust must return a CLEAN structured internal error and survive.
 assert.equal(r['nil-preset'].status,500,'Rust nil-preset must be a clean 500');
 assert.equal(r['nil-preset'].network_error,null,'Rust must NOT abort the connection on a nil preset');
 assert.equal(r['nil-preset'].error?.code,'internal','Rust nil-preset code');
 assert.equal(r['nil-preset'].error?.msg,'Internal service panic','Rust nil-preset message');
 // Go must exhibit a genuine failure (the nil pointer IS dereferenced): either an
 // unrecovered panic aborts the connection, or a recovered 5xx. Never a silent 200.
 assert.notEqual(g['nil-preset'].status,200,'Go nil-preset must not silently succeed');
 assert.ok(g['nil-preset'].network_error!==null||(g['nil-preset'].status!==null&&g['nil-preset'].status>=500),'Go nil-preset must fail (dereferenced), not 200/400');
 report.nil_preset_divergence={go:g['nil-preset'],rust:r['nil-preset']};
 report.status='PASS_NIL_NAMED_PRESET_OBSERVED';
}catch(error){
 report.status='FAIL';report.error={name:error.name,message:scrub(error.message)};process.exitCode=1;
}finally{
 await writeFile(process.env.NP_OUTPUT,JSON.stringify(report,null,2)+'\n');
 const nil=o=>o?.find(x=>x.case==='nil-preset');
 console.log(JSON.stringify({status:report.status,nil_go:nil(report.observations?.reference),nil_rust:nil(report.observations?.rust)}));
}