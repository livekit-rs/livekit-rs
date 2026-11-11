/** Real paired effect of the GENERATED CLI FLAG `--room.auto_create` (and its
 * precedence over the YAML value): against a prestarted fixed-Go reference and Rust
 * server launched with the same base YAML + the same extra argv, issue /rtc/validate
 * for a nonexistent room with a roomJoin token and require Rust to deep-equal the
 * fixed Go on the HTTP status. This exercises the FLAG input path and flag-over-YAML
 * priority, complementing the YAML-value effect (room-auto-create). No SDK, no media,
 * no WS signaling, no fake responder. */
import assert from 'node:assert/strict';
import {createHmac} from 'node:crypto';
import {writeFile} from 'node:fs/promises';
const urls=[process.env.REFERENCE_URL,process.env.LIVEKIT_URL];
assert.ok(urls.every(Boolean),'both actual reference and Rust HTTP URLs required');
const key='devkey',secret='secret';
const profile=process.env.CA_PROFILE;
assert.ok(profile,'CA_PROFILE required');
const report={status:'RUNNING',profile,scope:'Generated CLI flag --room.auto_create parsed and applied via /rtc/validate join-nonexistent effect, and its precedence over the room.auto_create YAML value (Go generated flag -> config -> roomallocator AutoCreate == Rust generated.rs flag apply -> may_create); not the YAML value effect itself, WS signaling join, media, ENV or webhook.',observations:[]};
function tk(room){const e=x=>Buffer.from(JSON.stringify(x)).toString('base64url');const s=`${e({alg:'HS256',typ:'JWT'})}.${e({iss:key,sub:'u',exp:Math.floor(Date.now()/1000)+3600,video:{room,identity:'u',roomJoin:true,canPublish:false,canSubscribe:false}})}`;return s+'.'+createHmac('sha256',secret).update(s).digest('base64url');}
async function validate(server,room){const u=new URL(`${urls[server].replace(/\/$/,'')}/rtc/validate`);u.searchParams.set('access_token',tk(room));u.searchParams.set('protocol','17');u.searchParams.set('sdk','js');const r=await fetch(u,{signal:AbortSignal.timeout(8000)});await r.text();return r.status;}
// One shared literal room name; each server is fresh per profile and /rtc/validate
// does not materialize the room, so both see it as nonexistent. Only the status is
// observed (deterministic).
async function scenario(server){
 const validate_status=await validate(server,'ca-flag-nonexistent');
 assert.ok([200,400,404].includes(validate_status),`unexpected validate status ${validate_status}`);
 return {profile,validate_status};
}
try{
 const reference=await scenario(0);
 const rust=await scenario(1);
 report.observations={reference,rust};
 assert.deepEqual(rust,reference,'Rust --room.auto_create CLI-flag effect diverged from fixed Go');
 report.status='PASS_SCOPED';
}catch(error){
 report.status='FAIL';report.error={name:error.name,message:String(error.message).replace(/eyJ[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+/g,'<token>')};process.exitCode=1;
}finally{
 await writeFile(process.env.CA_OUTPUT,JSON.stringify(report,null,2)+'\n');
 console.log(JSON.stringify({status:report.status,profile,validate_status:report.observations?.reference?.validate_status}));
}
