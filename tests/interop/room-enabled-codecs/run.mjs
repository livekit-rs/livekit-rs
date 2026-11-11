/** Real paired effect of the YAML `room.enabled_codecs` list on the codecs a room
 * reports: against a prestarted fixed-Go reference and Rust server sharing one YAML
 * profile, create a room and require Rust to deep-equal the fixed Go on the ORDERED
 * enabled_codecs [{mime, fmtp_line}] the public Room response echoes (Go
 * roomallocator.go applyDefaultRoomConfig appends conf.EnabledCodecs verbatim == Rust
 * normalize.rs:575 projects them in order). No SDK, no media, no WS signaling, no fake
 * responder. */
import assert from 'node:assert/strict';
import {createHmac} from 'node:crypto';
import {writeFile} from 'node:fs/promises';
const urls=[process.env.REFERENCE_URL,process.env.LIVEKIT_URL];
assert.ok(urls.every(Boolean),'both actual reference and Rust HTTP URLs required');
const key='devkey',secret='secret';
const profile=process.env.EC_PROFILE;
assert.ok(profile==='custom'||profile==='default','EC_PROFILE must be custom|default');
const report={status:'RUNNING',profile,scope:'Actual configured room.enabled_codecs list projected into the created room\'s public Room enabled_codecs [{mime,fmtp_line}] (Go applyDefaultRoomConfig append == Rust normalize.rs ordered projection); not codec runtime negotiation, media, CLI per-key flag, ENV or webhook.',observations:[]};
function tk(room){const e=x=>Buffer.from(JSON.stringify(x)).toString('base64url');const s=`${e({alg:'HS256',typ:'JWT'})}.${e({iss:key,sub:room,exp:Math.floor(Date.now()/1000)+3600,video:{room,roomCreate:true,roomAdmin:true,roomList:true}})}`;return s+'.'+createHmac('sha256',secret).update(s).digest('base64url');}
async function createRoom(server,name){const r=await fetch(`${urls[server].replace(/\/$/,'')}/twirp/livekit.RoomService/CreateRoom`,{method:'POST',headers:{'content-type':'application/json',authorization:`Bearer ${tk(name)}`},body:JSON.stringify({name}),signal:AbortSignal.timeout(10000)});return{status:r.status,body:await r.text()};}
async function del(server,name){await fetch(`${urls[server].replace(/\/$/,'')}/twirp/livekit.RoomService/DeleteRoom`,{method:'POST',headers:{'content-type':'application/json',authorization:`Bearer ${tk(name)}`},body:JSON.stringify({room:name})});}
// Normalize to the ordered [{mime, fmtp_line}] array (JSON key order is Go/Rust
// formatting noise; the array ORDER and per-entry values are the contract).
async function scenario(server){
 const r=await createRoom(server,'ec-room');
 assert.equal(r.status,200,'CreateRoom must succeed');
 const j=JSON.parse(r.body);
 const codecs=(j.enabled_codecs||[]).map(c=>({mime:c.mime,fmtp_line:c.fmtp_line}));
 await del(server,'ec-room');
 if(profile==='custom'){
  assert.deepEqual(codecs,[{mime:'audio/opus',fmtp_line:''},{mime:'video/VP8',fmtp_line:'x=7'},{mime:'video/H264',fmtp_line:'profile=42e01f'}],'custom enabled_codecs not applied verbatim/in-order');
 }else{
  assert.equal(codecs.length,10,'default codec list length drifted');
  const mimes=codecs.map(c=>c.mime);
  for(const m of ['audio/PCMU','audio/opus','video/VP8','video/VP9','video/AV1','video/rtx'])assert.ok(mimes.includes(m),`default list missing ${m}`);
  assert.ok(codecs.every(c=>c.fmtp_line===''),'default codecs must have empty fmtp_line');
 }
 return {profile,codecs};
}
try{
 const reference=await scenario(0);
 const rust=await scenario(1);
 report.observations={reference,rust};
 assert.deepEqual(rust,reference,'Rust room.enabled_codecs projection diverged from fixed Go');
 report.status='PASS_SCOPED';
}catch(error){
 report.status='FAIL';report.error={name:error.name,message:String(error.message).replace(/eyJ[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+/g,'<token>')};process.exitCode=1;
}finally{
 await writeFile(process.env.EC_OUTPUT,JSON.stringify(report,null,2)+'\n');
 console.log(JSON.stringify({status:report.status,profile,codecCount:report.observations?.reference?.codecs?.length}));
}
