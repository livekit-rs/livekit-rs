/** Real paired acceptance of the YAML value leaf `room.enable_remote_unmute` on the
 * admin RoomService/MutePublishedTrack UNMUTE gate. Against a fixed Go reference and a
 * Rust server launched with the SAME base YAML (which sets room.enable_remote_unmute to
 * the profile value) and NO extra argv. A single peer performs a real authenticated raw
 * WebSocket /rtc signaling join and is held open so the participant is genuinely present
 * in the room; an admin token then calls MutePublishedTrack with muted=false (unmute)
 * against that present participant and an unknown track sid. Go checks participant
 * presence (roomAndParticipantForReq), THEN the unmute gate, BEFORE touching the track
 * (roommanager.go:875), so the observable response is entirely the enable_remote_unmute
 * decision: denied (failed_precondition "remote unmute not enabled") when the leaf is
 * false, and allowed when it is true. Rust mirrors this on the same ordering
 * (crates/rtc-core/src/room/api.rs:92 after the live get_participant presence check).
 * No SDK mutation, no browser, no media/RTP, no fake RPC. Requires Rust to byte-equal the
 * fixed Go reference. This is the YAML INPUT PATH; the sibling generated CLI flag
 * --room.enable_remote_unmute is a separate input path. */
import assert from 'node:assert/strict';
import {createHmac} from 'node:crypto';
import {writeFile} from 'node:fs/promises';
import {readResponse} from '../signed-name-limits/raw-protocol.mjs';
const wsUrls=[process.env.REFERENCE_WS_URL,process.env.LIVEKIT_WS_URL];
assert.ok(wsUrls.every(Boolean),'both actual reference and Rust WS URLs required');
const httpUrls=wsUrls.map(u=>u.replace(/^ws/,'http').replace(/\/$/,''));
const key=process.env.LIVEKIT_API_KEY||'devkey',secret=process.env.LIVEKIT_API_SECRET||'secret';
const profile=process.env.ERU_PROFILE;
assert.ok(profile,'ERU_PROFILE required');
const expectUnmute=process.env.ERU_EXPECT_UNMUTE==='true';
const sleep=ms=>new Promise(r=>setTimeout(r,ms));
const report={status:'RUNNING',profile,expect_unmute_allowed:expectUnmute,scope:'YAML value leaf room.enable_remote_unmute applied on the admin MutePublishedTrack unmute gate (real WS-joined present peer, admin unmute on unknown track sid); not the CLI flag/ENV input path, not a real published-track mute effect, media, RTP or webhook.',observations:[]};
function token(identity,video){
 const e=v=>Buffer.from(JSON.stringify(v)).toString('base64url');
 const v=`${e({alg:'HS256',typ:'JWT'})}.${e({iss:key,sub:identity,exp:Math.floor(Date.now()/1000)+3600,video})}`;
 return v+'.'+createHmac('sha256',secret).update(v).digest('base64url');
}
const admin=(room)=>token('admin',{room,roomCreate:true,roomAdmin:true,roomList:true});
async function api(server,method,body){const r=await fetch(`${httpUrls[server]}/twirp/livekit.RoomService/${method}`,{method:'POST',headers:{authorization:`Bearer ${admin(body.room||body.name)}`,'content-type':'application/json'},body:JSON.stringify(body),signal:AbortSignal.timeout(10000)});const t=await r.text();let v;try{v=JSON.parse(t);}catch{v=t;}return{status:r.status,value:v};}
// Open a peer on the real /rtc WS signaling endpoint; resolves once it receives a join
// response or the connection terminates; the socket stays open (participant present)
// until close(). Mirrors the signaling join used by the max-participants fixture.
function openPeer(server,room,identity){
 return new Promise(res=>{
  const ep=new URL(wsUrls[server]);ep.pathname='/rtc';ep.search='';
  ep.searchParams.set('access_token',token(identity,{roomJoin:true,room,canPublish:false,canSubscribe:false}));
  ep.searchParams.set('protocol','17');ep.searchParams.set('sdk','js');
  const h={identity,joined:false,got_join:false,got_leave:false,close_clean:null,close_code:null,transport_error:false};
  const sock=new WebSocket(ep);sock.binaryType='arraybuffer';
  let settled=false;const finish=()=>{if(!settled){settled=true;res(h);}};
  setTimeout(finish,9000);
  sock.addEventListener('message',ev=>{try{const r=readResponse(Buffer.from(ev.data));
   if(r.type==='join'){h.joined=true;h.got_join=true;}
   if(r.type==='leave'){h.got_leave=true;}
   if(h.joined||r.type==='leave')finish();
  }catch{/* unrelated frames do not affect presence */}});
  sock.addEventListener('error',()=>{h.transport_error=true;finish();});
  sock.addEventListener('close',ev=>{h.close_clean=ev.wasClean;h.close_code=ev.code;finish();});
  h._close=()=>{try{sock.close();}catch{}};
 });
}
async function scenario(server){
 const room=`eru-${profile}-${server===0?'ref':'rs'}-${process.pid}`;
 const made=await api(server,'CreateRoom',{name:room,empty_timeout:60});
 assert.equal(made.status,200,'admin CreateRoom must succeed');
 const out=[];
 // Hold the peer joined so the participant is genuinely present for the gate.
 const a=await openPeer(server,room,'unmute-peer');
 out.push({case:'peer-join',joined:a.joined});
 assert.equal(a.joined,true,'peer must join under either profile');
 // Gate only fires once the participant is present: poll ListParticipants until it is,
 // on BOTH servers, so the observed unmute outcome is the enable_remote_unmute decision
 // and not a present-vs-absent artifact.
 let present=false;
 for(let i=0;i<60 && !present;i++){
  const lp=await api(server,'ListParticipants',{room});
  assert.equal(lp.status,200,'ListParticipants must succeed');
  present=(lp.value.participants||[]).map(p=>p.identity).includes('unmute-peer');
  if(!present)await sleep(150);
 }
 out.push({case:'participant-present',present});
 assert.equal(present,true,'WS-joined peer must be a present participant for the gate');
 // The gate under test: admin UNMUTE (muted=false) against the present peer on an
 // unknown track sid. Go evaluates the gate before track resolution.
 const un=await api(server,'MutePublishedTrack',{room,identity:'unmute-peer',track_sid:'TR_unknownsid',muted:false});
 out.push({case:'unmute',status:un.status,error:un.value&&un.value.code?un.value:null});
 a._close();
 await api(server,'DeleteRoom',{room});
 return {room_present:present,unmute:out.find(o=>o.case==='unmute')};
}
try{
 const reference=await scenario(0);
 const rust=await scenario(1);
 report.observations={reference,rust};
 assert.deepEqual(rust,reference,'Rust room.enable_remote_unmute unmute gate diverged from fixed Go');
 // Per-side expected oracle outcome; Rust must equal Go byte-for-byte.
 if(!expectUnmute){
  assert.equal(reference.unmute.status,412,'oracle must DENY remote unmute when disabled',reference.unmute);
  assert.equal(reference.unmute.error.code,'failed_precondition','oracle denial code',reference.unmute.error);
  assert.equal(reference.unmute.error.msg,'remote unmute not enabled','oracle denial msg',reference.unmute.error);
 }else{
  assert.equal(reference.unmute.status,200,'oracle must ALLOW remote unmute when enabled',reference.unmute);
 }
 assert.equal(rust.unmute.status,reference.unmute.status,'Rust unmute-gate status != Go');
 report.unmute={reference:reference.unmute,rust:rust.unmute};
 report.status='PASS_SCOPED';
}catch(error){
 report.status='FAIL';report.error={name:error.name,message:String(error.message).replace(/eyJ[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+/g,'<token>')};process.exitCode=1;
}finally{
 await writeFile(process.env.ERU_OUTPUT,JSON.stringify(report,null,2)+'\n');
 console.log(JSON.stringify({status:report.status,profile,unmute:{reference:report.observations?.reference?.unmute?.status,rust:report.observations?.rust?.unmute?.status}}));
}
