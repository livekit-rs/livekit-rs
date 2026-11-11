/** Real paired effect of the room.* legacy ALIAS keys for name/identity limits:
 * against a prestarted fixed-Go reference and Rust server sharing one YAML
 * profile, exercise CreateRoom (room.max_room_name_length) and /rtc/validate
 * (room.max_participant_identity_length) at the limit boundary and require Rust to
 * deep-equal the fixed Go byte-for-byte. No SDK, no media, no WS signaling, no fake
 * responder. */
import assert from 'node:assert/strict';
import {createHmac} from 'node:crypto';
import {writeFile} from 'node:fs/promises';
const urls=[process.env.REFERENCE_URL,process.env.LIVEKIT_URL];
assert.ok(urls.every(Boolean),'both actual reference and Rust HTTP URLs required');
const key='devkey',secret='secret';
const profile=process.env.NA_PROFILE,limit=Number(process.env.NA_EXPECT_LIMIT);
assert.ok(profile&&Number.isFinite(limit),'NA_PROFILE and NA_EXPECT_LIMIT required');
const report={status:'RUNNING',profile,expected_limit:limit,scope:'Actual configured room.max_room_name_length / room.max_participant_identity_length LEGACY ALIAS effect on CreateRoom and /rtc/validate (copied into Limit.* by Go config.go:743-747 == Rust load.rs:650-654); not the limit.* key itself, WS signaling join, media or webhook.',observations:[]};
function tk(identity,video){const e=x=>Buffer.from(JSON.stringify(x)).toString('base64url');const s=`${e({alg:'HS256',typ:'JWT'})}.${e({iss:key,sub:identity||'u',exp:Math.floor(Date.now()/1000)+3600,video})}`;return s+'.'+createHmac('sha256',secret).update(s).digest('base64url');}
async function createRoom(server,name){const r=await fetch(`${urls[server].replace(/\/$/,'')}/twirp/livekit.RoomService/CreateRoom`,{method:'POST',headers:{'content-type':'application/json',authorization:`Bearer ${tk(name,{room:name,roomCreate:true,roomAdmin:true,roomList:true})}`},body:JSON.stringify({name}),signal:AbortSignal.timeout(10000)});return{status:r.status,body:await r.text()};}
async function del(server,name){await fetch(`${urls[server].replace(/\/$/,'')}/twirp/livekit.RoomService/DeleteRoom`,{method:'POST',headers:{'content-type':'application/json',authorization:`Bearer ${tk(name,{room:name,roomCreate:true,roomAdmin:true,roomList:true})}`},body:JSON.stringify({room:name})});}
async function validate(server,identity){const u=new URL(`${urls[server].replace(/\/$/,'')}/rtc/validate`);u.searchParams.set('access_token',tk(identity,{identity,room:'na-shared',roomJoin:true,canPublish:false,canSubscribe:false}));u.searchParams.set('protocol','17');u.searchParams.set('sdk','js');const r=await fetch(u,{signal:AbortSignal.timeout(10000)});return{status:r.status,body:await r.text()};}
// Room names / identities at len 1..limit+1. Go checks len() (JS string length),
// so ASCII here is byte-identical to char count; 'abc' (len 3) is accepted, 'abcd'
// (len 4) rejected for limit 3.
// Normalized, deterministic observation per case: for an ACCEPTED (length<=limit)
// request compare only the status (the raw success body carries server-generated
// sid / turn_password / creation_time_ms and JSON key order, which are not part of
// this length-gate contract -- exactly the convention the signed-name-limits oracle
// uses). For a REJECTED (length>limit) request compare the deterministic error
// payload byte-for-byte (CreateRoom {code,msg}; /rtc/validate error text), which is
// where the configured ALIAS limit value is echoed.
async function scenario(server){
 const out=[];
 if(profile==='name-alias'){
  for(const name of ['a','ab','abc','abcd']){
   const r=await createRoom(server,name);
   const rec={case:`CreateRoom-${name.length}`,name,status:r.status};
   if(r.status===200){const j=JSON.parse(r.body);assert.equal(j.name,name,`accepted CreateRoom must echo the requested name (server ${server})`);await del(server,name);}
   else{const j=JSON.parse(r.body);rec.error={code:j.code,msg:j.msg};}
   out.push(rec);
  }
  const accepted=out.filter(o=>o.name.length<=limit),rejected=out.filter(o=>o.name.length>limit);
  assert.ok(accepted.every(o=>o.status===200),'names at/below limit must be accepted (alias not applied?)');
  assert.ok(rejected.every(o=>o.status===400&&o.error.code==='invalid_argument'&&o.error.msg.includes('room name length exceeds limits')&&o.error.msg.includes(`max length ${limit}`)),'over-limit names must reject with alias limit');
 }else{
  for(const id of ['a','abc','abcd','abcde']){
   const r=await validate(server,id);
   const rec={case:`validate-${id.length}`,identity:id,status:r.status};
   if(r.status!==200)rec.body=r.body;
   out.push(rec);
  }
  const accepted=out.filter(o=>o.identity.length<=limit),rejected=out.filter(o=>o.identity.length>limit);
  assert.ok(accepted.every(o=>o.status===200),'identities at/below limit must validate (alias not applied?)');
  assert.ok(rejected.every(o=>o.status===400&&o.body.includes('participant identity length exceeds limits')&&o.body.includes(`max length ${limit}`)),'over-limit identities must reject with alias limit');
 }
 return out;
}
try{
 const reference=await scenario(0);
 const rust=await scenario(1);
 report.observations={reference,rust};
 assert.deepEqual(rust,reference,'Rust room.* name/identity ALIAS enforcement diverged from fixed Go');
 report.status='PASS_SCOPED';
}catch(error){
 report.status='FAIL';report.error={name:error.name,message:String(error.message).replace(/eyJ[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+/g,'<token>')};process.exitCode=1;
}finally{
 await writeFile(process.env.NA_OUTPUT,JSON.stringify(report,null,2)+'\n');
 console.log(JSON.stringify({status:report.status,profile,cases:report.observations?.reference?.length}));
}
