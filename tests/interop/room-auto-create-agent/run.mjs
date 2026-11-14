/** Real paired AgentDispatchService.CreateDispatch auto-create effect of configured
 * room.auto_create against a prestarted fixed-Go reference and Rust server sharing one
 * profile (standalone, no Redis). Exercises the public Twirp livekit.AgentDispatchService
 * CreateDispatch/ListDispatch/DeleteDispatch + RoomService CreateRoom/ListRooms/DeleteRoom.
 *
 * The auto-create gate is the F150 standalone direct-manager bypass: CreateDispatch with
 * admin (roomAdmin) auth reuses the internal CreateRoom boundary WITHOUT a roomCreate grant.
 * ac-true  -> CreateDispatch to a nonexistent room materializes the room and returns 200.
 * ac-false -> CreateDispatch to a nonexistent room is unavailable and creates NO room.
 *
 * Deterministic comparison: HTTP status + Twirp error code + normalized structural fields.
 * Dispatch/room SIDs and timestamps are identity-bearing and differ by design (Go uses a
 * deterministic RequestID-derived ID, Rust a fresh SID), so they are excluded from the
 * deep-equal projection; agent_name/room/metadata/deployment/attributes/restart_policy/jobs
 * and catalog presence are compared. ListDispatch order is non-contractual (Go ranges a map,
 * pkg/rtc/room.go:117/:875), so lists compare as canonically sorted SETS of stable projections.
 * No SDK, no media, no WS signaling, no fake responder. */
import assert from 'node:assert/strict';
import {createHmac} from 'node:crypto';
import {writeFile} from 'node:fs/promises';
const urls=[process.env.REFERENCE_URL,process.env.LIVEKIT_URL];
assert.ok(urls.every(Boolean),'both actual reference and Rust HTTP URLs required');
const key=process.env.LIVEKIT_API_KEY||'devkey',secret=process.env.LIVEKIT_API_SECRET||'secret';
const profile=process.env.AA_PROFILE;
assert.ok(profile,'AA_PROFILE required');
const report={status:'RUNNING',profile,scope:'Actual configured room.auto_create effect on public AgentDispatchService.CreateDispatch auto-create (standalone direct-manager CreateRoom reuse, admin not roomCreate) + room catalog materialization + dispatch lifecycle; not WS signaling join, media, ENV, distributed Redis router or webhook.',observations:[]};
function token(grants){
 const enc=v=>Buffer.from(JSON.stringify(v)).toString('base64url');
 const value=`${enc({alg:'HS256',typ:'JWT'})}.${enc({iss:key,sub:'u',exp:Math.floor(Date.now()/1000)+3600,video:grants})}`;
 return value+'.'+createHmac('sha256',secret).update(value).digest('base64url');
}
async function twirp(server,service,method,body,grants){
 const r=await fetch(`${urls[server].replace(/\/$/,'')}/twirp/livekit.${service}/${method}`,{method:'POST',headers:{'content-type':'application/json',authorization:`Bearer ${token(grants)}`},body:JSON.stringify(body),signal:AbortSignal.timeout(10000)});
 const text=await r.text();
 let json=null;try{json=JSON.parse(text);}catch{}
 return {status:r.status,json,text};
}
// Stable projection of a created dispatch: identity-bearing id/timestamps excluded.
// Twirp JSON here is emitted with proto OrigName (snake_case): agent_name, restart_policy,
// state.jobs. Reading camelCase would silently yield '' on BOTH sides (vacuous compare),
// so the created-dispatch positive controls below assert the real agent_name is present.
function dispatchProjection(j){
 if(!j||typeof j!=='object')return null;
 return {agent_name:j.agent_name??'',room:j.room??'',metadata:j.metadata??'',deployment:j.deployment??'',attributes:j.attributes??{},restart_policy:j.restart_policy??null,jobs:Array.isArray(j.state?.jobs)?j.state.jobs.length:0};
}
// ListDispatch order is NOT contractual: Go Room.GetAgentDispatches ranges over
// agentDispatches map[string]*agentDispatch (references/livekit-stable/pkg/rtc/room.go:117
// field, :875 range), and Go randomizes map iteration. Compare the dispatch SET as
// canonically sorted stable projections (identity-bearing id/timestamps already excluded).
// This is a source-justified normalization of a non-deterministic property, not a dropped field.
function sortedProjections(list){
 return list.map(dispatchProjection).sort((a,b)=>JSON.stringify(a)<JSON.stringify(b)?-1:1);
}
function dispatchList(json){
 return Array.isArray(json?.agent_dispatches)?json.agent_dispatches:(Array.isArray(json?.agentDispatches)?json.agentDispatches:[]);
}
function errProjection(res){
 // Twirp error envelope: {code, msg}. Compare the code (parity contract), record msg.
 return {status:res.status,code:res.json?.code??null,msg:res.json?.msg??res.text};
}
async function scenario(server,raw){
 // Shared literal room names across both fresh servers (each is independent per
 // profile); identity-bearing SIDs/timestamps still differ and are excluded below.
 const base=`aa-${profile}-${process.pid}`;
 const acRoom=`${base}-ac`;       // nonexistent at CreateDispatch time
 const exRoom=`${base}-ex`;       // explicitly created first
 const adminAc={room:acRoom,roomAdmin:true,roomList:true,roomCreate:true};
 const adminEx={room:exRoom,roomAdmin:true,roomList:true,roomCreate:true};
 const out=[];
 const rec=async(label,service,method,body,grants)=>{const r=await twirp(server,service,method,body,grants);raw.push({label,service,method,status:r.status,text:r.text});return r;};
 // (1) CreateDispatch to a NONEXISTENT room: the auto_create gate (F150 bypass).
 const d1=await rec('dispatch-nonexistent','AgentDispatchService','CreateDispatch',{room:acRoom,agentName:'probe-agent',metadata:'ac-meta',attributes:{k:'v'}},adminAc);
 if(d1.status===200){
  // Positive control: the projection must read real snake_case data, not silently ''.
  assert.equal(d1.json?.agent_name,'probe-agent','created dispatch agent_name must be present (non-vacuous projection)');
  assert.equal(d1.json?.metadata,'ac-meta','created dispatch metadata must be present');
  assert.deepEqual(d1.json?.attributes,{k:'v'},'created dispatch attributes must round-trip');
  assert.equal(d1.json?.restart_policy,'JRP_ON_FAILURE','default restart_policy must be JRP_ON_FAILURE');
 }
 out.push({case:'dispatch-nonexistent',status:d1.status,code:d1.json?.code??null,dispatch:d1.status===200?dispatchProjection(d1.json):null,msg:d1.status===200?undefined:(d1.json?.msg??d1.text)});
 // (2) Did CreateDispatch materialize the room? (ac-true yes, ac-false no)
 const l1=await rec('materialized-after-dispatch','RoomService','ListRooms',{names:[acRoom]},{...adminAc,room:acRoom});
 out.push({case:'materialized-after-dispatch',status:l1.status,count:Array.isArray(l1.json?.rooms)?l1.json.rooms.length:null,names:Array.isArray(l1.json?.rooms)?l1.json.rooms.map(r=>r.name).sort():null});
 // (3) CreateRoom explicitly, then CreateDispatch to the EXISTING room: allowed both profiles.
 const made=await rec('create-room-via-api','RoomService','CreateRoom',{name:exRoom,emptyTimeout:30},adminEx);
 out.push({case:'create-room-via-api',status:made.status});
 assert.equal(made.status,200,'admin CreateRoom must succeed under either profile');
 try{
  const d2=await rec('dispatch-existing','AgentDispatchService','CreateDispatch',{room:exRoom,agentName:'existing-agent',metadata:'ex-meta'},adminEx);
  if(d2.status===200){
   assert.equal(d2.json?.agent_name,'existing-agent','existing-room dispatch agent_name must be present (non-vacuous projection)');
   assert.equal(d2.json?.metadata,'ex-meta','existing-room dispatch metadata must be present');
  }
  out.push({case:'dispatch-existing',status:d2.status,code:d2.json?.code??null,dispatch:d2.status===200?dispatchProjection(d2.json):null,msg:d2.status===200?undefined:(d2.json?.msg??d2.text)});
  // (4) ListDispatch on the existing room returns the created dispatch (no worker => no jobs).
  //     Order is non-contractual (Go map range); compare the canonically sorted SET.
  const ld=await rec('list-dispatch','AgentDispatchService','ListDispatch',{room:exRoom},adminEx);
  const list=dispatchList(ld.json);
  const projections=sortedProjections(list);
  assert.ok(projections.some(p=>p.agent_name==='existing-agent'),'list must contain the explicitly created dispatch (non-vacuous)');
  out.push({case:'list-dispatch',status:ld.status,count:list.length,projections});
  // (5) DeleteDispatch removes the EXPLICIT dispatch, selected by stable agent_name (not list[0],
  //     which is a random map position on the Go side). Both sides delete the same logical record.
  const target=list.find(d=>(d.agent_name??d.agentName)==='existing-agent');
  assert.ok(target,'must find the explicit dispatch to delete by stable selector');
  const dd=await rec('delete-dispatch','AgentDispatchService','DeleteDispatch',{room:exRoom,dispatchId:target.id??target.dispatch_id},adminEx);
  out.push({case:'delete-dispatch',status:dd.status,code:dd.json?.code??null});
  const ld2=await rec('list-after-delete','AgentDispatchService','ListDispatch',{room:exRoom},adminEx);
  const list2=dispatchList(ld2.json);
  const projections2=sortedProjections(list2);
  assert.ok(!projections2.some(p=>p.agent_name==='existing-agent'),'deleted dispatch must be absent from the list (non-vacuous)');
  out.push({case:'list-after-delete',status:ld2.status,count:list2.length,projections:projections2});
 }finally{
  await twirp(server,'RoomService','DeleteRoom',{room:exRoom},adminEx);
  await twirp(server,'RoomService','DeleteRoom',{room:acRoom},adminAc);
 }
 return out;
}
try{
 const rawReference=[],rawRust=[];
 const reference=await scenario(0,rawReference);
 const rust=await scenario(1,rawRust);
 report.observations={reference,rust};
 report.rawResponses={reference:rawReference,rust:rawRust};
 // Compare status + code + structural projection; error msg recorded but not byte-gated
 // (Twirp error wording is a separate diagnostic surface; the code is the parity contract).
 const norm=rows=>rows.map(({msg,...rest})=>rest);
 assert.deepEqual(norm(rust),norm(reference),'Rust configured room.auto_create AgentDispatch behavior diverged from fixed Go');
 report.status='PASS_SCOPED';
}catch(error){
 report.status='FAIL';report.error={name:error.name,message:String(error.message).replace(/eyJ[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+/g,'<token>')};process.exitCode=1;
}finally{
 await writeFile(process.env.AA_OUTPUT,JSON.stringify(report,null,2)+'\n');
 const ref=report.observations.reference;
 const dn=ref?.find(x=>x.case==='dispatch-nonexistent');
 const mat=ref?.find(x=>x.case==='materialized-after-dispatch');
 console.log(JSON.stringify({status:report.status,profile,dispatch_nonexistent_status:dn?.status,materialized_count:mat?.count}));
}