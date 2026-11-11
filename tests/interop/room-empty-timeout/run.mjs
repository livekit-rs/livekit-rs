/** Real paired effect of configured room.empty_timeout: against a prestarted
 * fixed-Go reference and Rust server sharing one YAML profile, create an EMPTY
 * room (no join) that inherits the global empty_timeout and poll ListRooms to
 * observe whether the server auto-closes it within the window. HTTP API only,
 * no SDK/media/webhook. Requires Rust to deep-equal the fixed Go reference. */
import assert from 'node:assert/strict';
import {createHmac,randomUUID} from 'node:crypto';
import {writeFile} from 'node:fs/promises';
const urls=[process.env.REFERENCE_URL,process.env.LIVEKIT_URL];
assert.ok(urls.every(Boolean),'both actual reference and Rust HTTP URLs required');
const key='devkey',secret='secret';
const profile=process.env.ET_PROFILE,windowMs=Number(process.env.ET_WINDOW);
assert.ok(profile&&Number.isFinite(windowMs)&&windowMs>0,'ET_PROFILE and ET_WINDOW required');
const report={status:'RUNNING',profile,window_ms:windowMs,scope:'Actual configured room.empty_timeout auto-closing an empty (never-joined) room, observed via ListRooms; not participant-departure timing, media or webhook.',observations:[]};
function token(video){const e=v=>Buffer.from(JSON.stringify(v)).toString('base64url');const v=`${e({alg:'HS256',typ:'JWT'})}.${e({iss:key,sub:'admin',exp:Math.floor(Date.now()/1000)+3600,video})}`;return v+'.'+createHmac('sha256',secret).update(v).digest('base64url');}
async function api(server,method,body){const r=await fetch(`${urls[server].replace(/\/$/,'')}/twirp/livekit.RoomService/${method}`,{method:'POST',headers:{authorization:`Bearer ${token({room:body.room||body.name,roomCreate:true,roomAdmin:true,roomList:true})}`,'content-type':'application/json'},body:JSON.stringify(body),signal:AbortSignal.timeout(10000)});const t=await r.text();let v;try{v=JSON.parse(t);}catch{v=t;}return{status:r.status,value:v};}
const sleep=ms=>new Promise(r=>setTimeout(r,ms));
async function present(server,name){const r=await api(server,'ListRooms',{names:[name]});assert.equal(r.status,200);return (r.value.rooms||[]).some(x=>x.name===name);}
async function scenario(server){
 const name=`et-${profile}-${server===0?'ref':'rs'}-${randomUUID().slice(0,8)}`;
 const made=await api(server,'CreateRoom',{name});
 assert.equal(made.status,200,'admin CreateRoom must succeed');
 const reported=made.value.empty_timeout;
 assert.equal(reported,Number(process.env.ET_EXPECT_TIMEOUT),'server must report the configured global empty_timeout on a room created without an explicit one');
 // Poll until the room disappears or the window elapses.
 let closed=false;const t0=Date.now();
 while(Date.now()-t0<windowMs){const p=await present(server,name);if(!p){closed=true;break;}await sleep(250);}
 // Ensure cleanup regardless of outcome (DeleteRoom on an already-closed room is tolerated).
 await api(server,'DeleteRoom',{room:name});
 return {reported_empty_timeout:reported,closed_in_window:closed};
}
try{
 const reference=await scenario(0);
 const rust=await scenario(1);
 report.observations={reference,rust};
 assert.deepEqual(rust,reference,'Rust configured room.empty_timeout auto-close behavior diverged from fixed Go');
 report.status='PASS_SCOPED';
}catch(error){
 report.status='FAIL';report.error={name:error.name,message:String(error.message).replace(/eyJ[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+/g,'<token>')};process.exitCode=1;
}finally{
 await writeFile(process.env.ET_OUTPUT,JSON.stringify(report,null,2)+'\n');
 console.log(JSON.stringify({status:report.status,profile,closed_in_window:{reference:report.observations?.reference?.closed_in_window,rust:report.observations?.rust?.closed_in_window}}));
}
