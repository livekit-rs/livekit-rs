/** Real paired effect of the GENERATED CLI FLAG `--limit.max_attributes_size` (an
 * integer flag) on the Twirp RoomService.CreateRoom agents path: against a prestarted
 * fixed-Go reference and a Rust server launched with the same base YAML plus the same
 * extra argv, create rooms whose agent attributes are over/under the configured byte
 * budget and require Rust to deep-equal the fixed Go on status and, for rejections, on
 * the deterministic Twirp error body. This exercises the integer FLAG input path
 * (parse_value Int -> limits.max_attributes_size -> check_attributes) and its real
 * side effect, complementing the room.auto_create boolean-flag fixture. No SDK, no
 * media, no WS signaling, no fake responder. */
import assert from 'node:assert/strict';
import {createHmac} from 'node:crypto';
import {writeFile} from 'node:fs/promises';
const urls=[process.env.REFERENCE_URL,process.env.LIVEKIT_URL];
assert.ok(urls.every(Boolean),'both actual reference and Rust HTTP URLs required');
const key='devkey',secret='secret';
const profile=process.env.AF_PROFILE;
assert.ok(profile,'AF_PROFILE required');
const report={status:'RUNNING',profile,scope:'Generated integer CLI flag --limit.max_attributes_size parsed and applied on the CreateRoom agent-attributes size gate (Go roomservice.go:96-98 limitConf.CheckAttributesSize == Rust room_service.rs check_attributes reading config.limits.max_attributes_size); not the metadata/agents dispatch side effects, WS signaling, media, ENV or webhook.',observations:[]};
function tk(){const e=x=>Buffer.from(JSON.stringify(x)).toString('base64url');const s=`${e({alg:'HS256',typ:'JWT'})}.${e({iss:key,sub:'u',exp:Math.floor(Date.now()/1000)+3600,video:{room:'af',roomCreate:true,roomAdmin:true,roomList:true}})}`;return s+'.'+createHmac('sha256',secret).update(s).digest('base64url');}
// Two cases: total attribute byte size = len("k") + len(value). "over" -> 41 bytes,
// "under" -> 6 bytes. A fresh server per profile with the flag sets the threshold.
const CASES=[{case:'over',attr_len:40},{case:'under',attr_len:5}];
async function createRoom(server,c){
 const body=JSON.stringify({name:`af-${profile}-${c.case}`,agents:[{agent_name:'a',attributes:{k:'x'.repeat(c.attr_len)}}]});
 const r=await fetch(`${urls[server].replace(/\/$/,'')}/twirp/livekit.RoomService/CreateRoom`,{method:'POST',headers:{'content-type':'application/json',authorization:`Bearer ${tk()}`},body,signal:AbortSignal.timeout(10000)});
 return {status:r.status,value:await r.text()};
}
// Accepted cases compare only the deterministic status + echoed name (the success body
// carries server-generated sid/turn_password/creation_time and Go/Rust JSON key order).
// Rejected cases compare the deterministic Twirp error body {code,msg,meta}.
async function scenario(server){
 const out=[];
 for(const c of CASES){
  const r=await createRoom(server,c);
  assert.ok([200,400].includes(r.status),`unexpected CreateRoom status ${r.status}`);
  const rec={case:c.case,attr_len:c.attr_len,status:r.status};
  if(r.status===200){const j=JSON.parse(r.value);rec.name=j.name;rec.created=true;}
  else{const j=JSON.parse(r.value);rec.error={code:j.code,msg:j.msg,meta:j.meta};}
  out.push(rec);
 }
 return {profile,out};
}
try{
 const reference=await scenario(0);
 const rust=await scenario(1);
 report.observations={reference,rust};
 assert.deepEqual(rust,reference,'Rust --limit.max_attributes_size CLI-flag effect diverged from fixed Go');
 report.status='PASS_SCOPED';
}catch(error){
 report.status='FAIL';report.error={name:error.name,message:String(error.message).replace(/eyJ[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+/g,'<token>')};process.exitCode=1;
}finally{
 await writeFile(process.env.AF_OUTPUT,JSON.stringify(report,null,2)+'\n');
 console.log(JSON.stringify({status:report.status,profile,cases:report.observations?.reference?.out?.map(o=>({case:o.case,status:o.status}))}));
}
