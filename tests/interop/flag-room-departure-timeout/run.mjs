/** Real paired effect of configured room.departure_timeout: against a prestarted
 * fixed-Go reference and Rust server sharing one YAML profile, create a room,
 * join one peer over the real authenticated /rtc WebSocket signaling, hold until
 * joined, then close it (departure) and poll ListRooms to observe whether the room
 * auto-closes within the window. empty_timeout is set high (600) in the profile so
 * the participant-departure branch (LastLeftAt -> DepartureTimeout) is what fires.
 * Rust must deep-equal the fixed Go reference. No browser, no media/RTP, no SDK. */
import assert from 'node:assert/strict';
import {createHmac,randomUUID} from 'node:crypto';
import {writeFile} from 'node:fs/promises';
import {readResponse} from '../signed-name-limits/raw-protocol.mjs';
const wsUrls=[process.env.REFERENCE_WS_URL,process.env.LIVEKIT_WS_URL];
assert.ok(wsUrls.every(Boolean),'both actual reference and Rust WS URLs required');
const httpUrls=wsUrls.map(u=>u.replace(/^ws/,'http').replace(/\/$/,''));
const key='devkey',secret='secret';
const profile=process.env.DT_PROFILE,windowMs=Number(process.env.DT_WINDOW),expectTo=Number(process.env.DT_EXPECT_DEPARTURE_TIMEOUT);
assert.ok(profile&&Number.isFinite(windowMs)&&windowMs>0&&Number.isFinite(expectTo),'DT_PROFILE, DT_WINDOW, DT_EXPECT_DEPARTURE_TIMEOUT required');
const report={status:'RUNNING',profile,window_ms:windowMs,scope:'Actual configured room.departure_timeout auto-closing a room after its last independent participant departs (real WS join then close), observed via ListRooms; empty_timeout pinned high so the departure branch fires. Not media/RTP or webhook.',observations:[]};
function token(video){const e=v=>Buffer.from(JSON.stringify(v)).toString('base64url');const v=`${e({alg:'HS256',typ:'JWT'})}.${e({iss:key,sub:'admin',exp:Math.floor(Date.now()/1000)+3600,video})}`;return v+'.'+createHmac('sha256',secret).update(v).digest('base64url');}
async function api(server,method,body){const r=await fetch(`${httpUrls[server]}/twirp/livekit.RoomService/${method}`,{method:'POST',headers:{authorization:`Bearer ${token({room:body.room||body.name,roomCreate:true,roomAdmin:true,roomList:true})}`,'content-type':'application/json'},body:JSON.stringify(body),signal:AbortSignal.timeout(10000)});const t=await r.text();let v;try{v=JSON.parse(t);}catch{v=t;}return{status:r.status,value:v};}
const sleep=ms=>new Promise(r=>setTimeout(r,ms));
async function present(server,name){const r=await api(server,'ListRooms',{names:[name]});assert.equal(r.status,200);return (r.value.rooms||[]).some(x=>x.name===name);}
function joinHold(server,name){return new Promise(res=>{const ep=new URL(wsUrls[server]);ep.pathname='/rtc';ep.search='';ep.searchParams.set('access_token',token({roomJoin:true,room:name,canPublish:false,canSubscribe:false}));ep.searchParams.set('protocol','17');ep.searchParams.set('sdk','js');const sock=new WebSocket(ep);sock.binaryType='arraybuffer';let done=false;const j=setTimeout(()=>{if(!done){done=true;res({joined:false,sock});}},9000);sock.addEventListener('message',ev=>{try{const r=readResponse(Buffer.from(ev.data));if(r.type==='join'&&!done){done=true;clearTimeout(j);res({joined:true,sock});}}catch{}});sock.addEventListener('error',()=>{if(!done){done=true;res({joined:false,sock});}});});}
async function scenario(server){
 const name=`dt-${profile}-${server===0?'ref':'rs'}-${randomUUID().slice(0,8)}`;
 const made=await api(server,'CreateRoom',{name});
 assert.equal(made.status,200,'admin CreateRoom must succeed');
 assert.equal(made.value.departure_timeout,expectTo,'server must report the configured global departure_timeout on a room created without an explicit one');
 assert.equal(made.value.empty_timeout,600,'profile pins empty_timeout=600 so the departure branch is what fires');
 const h=await joinHold(server,name);
 assert.equal(h.joined,true,'peer must join over the real WS signaling path under either profile');
 const lp=await api(server,'ListParticipants',{room:name});
 assert.equal((lp.value.participants||[]).length,1,'exactly one participant present before departure');
 // Departure: close the sole participant's socket.
 try{h.sock.close(1000);}catch{}
 let closed=false;const t0=Date.now();
 while(Date.now()-t0<windowMs){await sleep(250);const p=await present(server,name);if(!p){closed=true;break;}}
 await api(server,'DeleteRoom',{room:name});
 return {reported_departure_timeout:made.value.departure_timeout,reported_empty_timeout:made.value.empty_timeout,closed_after_departure:closed};
}
try{
 const reference=await scenario(0);
 const rust=await scenario(1);
 report.observations={reference,rust};
 assert.deepEqual(rust,reference,'Rust configured room.departure_timeout auto-close behavior diverged from fixed Go');
 report.status='PASS_SCOPED';
}catch(error){
 report.status='FAIL';report.error={name:error.name,message:String(error.message).replace(/eyJ[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+/g,'<token>')};process.exitCode=1;
}finally{
 await writeFile(process.env.DT_OUTPUT,JSON.stringify(report,null,2)+'\n');
 console.log(JSON.stringify({status:report.status,profile,closed_after_departure:{reference:report.observations?.reference?.closed_after_departure,rust:report.observations?.rust?.closed_after_departure}}));
}
