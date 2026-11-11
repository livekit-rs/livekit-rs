/** Real paired /rtc/validate + RoomService effect of configured room.auto_create
 * against a prestarted Go reference and Rust server sharing one profile.
 * No SDK, no media, no signaling WS, no fake responder. Captures HTTP status and
 * raw body for the grant/existence matrix and requires Rust to equal Go byte-for-byte. */
import assert from 'node:assert/strict';
import {createHmac} from 'node:crypto';
import {writeFile} from 'node:fs/promises';
const urls=[process.env.REFERENCE_URL,process.env.LIVEKIT_URL];
assert.ok(urls.every(Boolean),'both actual reference and Rust HTTP URLs required');
const key=process.env.LIVEKIT_API_KEY||'devkey',secret=process.env.LIVEKIT_API_SECRET||'secret';
const report={status:'RUNNING',profile:process.env.AC_PROFILE,scope:'Actual configured room.auto_create effect on /rtc/validate (join-only vs roomCreate grant, nonexistent vs existing room) + CreateRoom; not signaling WS join, media or webhook.',observations:[]};
function token(grants){
 const enc=v=>Buffer.from(JSON.stringify(v)).toString('base64url');
 const value=`${enc({alg:'HS256',typ:'JWT'})}.${enc({iss:key,sub:'u',exp:Math.floor(Date.now()/1000)+3600,video:grants})}`;
 return value+'.'+createHmac('sha256',secret).update(value).digest('base64url');
}
async function validate(server,grants){
 const u=new URL(`${urls[server]}/rtc/validate`);u.searchParams.set('access_token',token(grants));u.searchParams.set('protocol','17');u.searchParams.set('sdk','js');
 const r=await fetch(u,{signal:AbortSignal.timeout(10000)});
 return {status:r.status,body:await r.text()};
}
async function api(server,method,body,grants){
 const r=await fetch(`${urls[server].replace(/\/$/,'')}/twirp/livekit.RoomService/${method}`,{method:'POST',headers:{'content-type':'application/json',authorization:`Bearer ${token(grants)}`},body:JSON.stringify(body),signal:AbortSignal.timeout(10000)});
 return {status:r.status,body:await r.text()};
}
async function scenario(server){
 const base=`ac-${process.env.AC_PROFILE}-${server}-${process.pid}`;
 const admin={room:base,roomCreate:true,roomAdmin:true,roomList:true};
 const join={room:`${base}-nope`,roomJoin:true,canPublish:false,canSubscribe:false};
 const create={room:`${base}-nope`,roomJoin:true,roomCreate:true,canPublish:false,canSubscribe:false};
 const out=[];
 // (1) join-only token to a room that does not exist: the auto_create gate.
 out.push({case:'join-nonexistent',...await validate(server,join)});
 // (2) roomCreate grant to a non-existent room: allowed regardless of auto_create.
 out.push({case:'create-grant-nonexistent',...await validate(server,create)});
 // (3) existing room via real CreateRoom then join-only validate: allowed both profiles.
 const made=await api(server,'CreateRoom',{name:base,empty_timeout:30},{...admin,room:base});
 out.push({case:'create-room-via-api',status:made.status});
 assert.equal(made.status,200,'admin CreateRoom must succeed under either profile');
 try{
  out.push({case:'join-existing',...await validate(server,{room:base,roomJoin:true,canPublish:false,canSubscribe:false})});
  // (4) the (2) validate must NOT have created the -nope room (validate does not materialize).
  const listed=await api(server,'ListRooms',{names:[`${base}-nope`]},{...admin,room:`${base}-nope`});
  out.push({case:'validate-did-not-create',absent:(()=>{try{return JSON.parse(listed.body).rooms.length===0;}catch{return 'unparsed';}})()});
 }finally{ await api(server,'DeleteRoom',{room:base},{...admin,room:base}); }
 return out;
}
try{
 const reference=await scenario(0);
 const rust=await scenario(1);
 report.observations={reference,rust};
 assert.deepEqual(rust,reference,'Rust configured room.auto_create /rtc/validate behavior diverged from fixed Go');
 report.status='PASS_SCOPED';
}catch(error){
 report.status='FAIL';report.error={name:error.name,message:String(error.message)};process.exitCode=1;
}finally{
 await writeFile(process.env.AC_OUTPUT,JSON.stringify(report,null,2)+'\n');
 const jc=report.observations.reference?report.observations.reference.find(x=>x.case==='join-nonexistent'):null;
 console.log(JSON.stringify({status:report.status,profile:report.profile,join_nonexistent_status:jc&&jc.status}));
}
