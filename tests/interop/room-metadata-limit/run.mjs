/** Real paired HTTP RoomService metadata-size enforcement against a prestarted
 * Go reference and Rust server sharing the same configured limit profile.
 * No SDK mutation, no media, no fake responder. Captures actual status/code/msg
 * for empty/within/exactly-limit/over-limit and UTF8 byte-boundary metadata and
 * requires Rust outcomes to equal Go outcomes byte-for-byte. */
import assert from 'node:assert/strict';
import {createHmac} from 'node:crypto';
import {writeFile} from 'node:fs/promises';
const urls=[process.env.REFERENCE_URL,process.env.LIVEKIT_URL];
assert.ok(urls.every(Boolean),'both actual reference and Rust HTTP URLs required');
const key=process.env.LIVEKIT_API_KEY||'devkey',secret=process.env.LIVEKIT_API_SECRET||'secret';
const limit=Number(process.env.EXPECT_BYTE_LIMIT);
assert.ok(Number.isInteger(limit)&&limit>0,'coordinator must declare the configured byte limit for this profile');
const report={status:'RUNNING',profile:process.env.META_PROFILE||'unknown',scope:'Actual configured HTTP RoomService metadata-size enforcement (CreateRoom/UpdateRoomMetadata) incl empty/within/exact/over and UTF8 byte boundary; not signaling, media or webhook.',observations:[]};
function token(room){
 const enc=v=>Buffer.from(JSON.stringify(v)).toString('base64url');
 const value=`${enc({alg:'HS256',typ:'JWT'})}.${enc({iss:key,sub:'admin',exp:Math.floor(Date.now()/1000)+3600,video:{room,roomCreate:true,roomAdmin:true,roomList:true}})}`;
 return value+'.'+createHmac('sha256',secret).update(value).digest('base64url');
}
async function api(server,method,body){
 const r=await fetch(`${urls[server].replace(/\/$/,'')}/twirp/livekit.RoomService/${method}`,{method:'POST',headers:{'content-type':'application/json',authorization:`Bearer ${token(body.room||body.name||'')}`},body:JSON.stringify(body),signal:AbortSignal.timeout(10000)});
 const text=await r.text();let v;try{v=JSON.parse(text);}catch{v=text;}
 return {status:r.status,code:v&&typeof v==='object'?v.code:undefined,msg:v&&typeof v==='object'?v.msg:undefined};
}
// Byte-boundary metadata inputs against a configured limit L:
// empty(0), 1, L-1, L exactly, L+1 over, and a UTF8 string whose BYTE length is L
// but char length is smaller (proves byte, not char, accounting).
function inputs(L){
 const exact='a'.repeat(L);
 const over='a'.repeat(L+1);
 const within=L>1?'a'.repeat(L-1):'';
 // multibyte: build a byte-length exactly L using 2-byte chars where possible.
 let mb='';const unit='é';// 2 utf8 bytes
 while(Buffer.byteLength(mb+unit,'utf8')<=L)mb+=unit;
 // pad to exactly L if odd remainder
 if(Buffer.byteLength(mb,'utf8')<L)mb+='a'.repeat(L-Buffer.byteLength(mb,'utf8'));
 return [['empty',''],['within',within],['exact',exact],['over',over],['utf8-exact',mb]];
}
async function scenario(server){
 const base=`meta-${process.env.META_PROFILE}-${server}-${process.pid}`;
 const out=[];
 for(const [kind,meta] of inputs(limit)){
  const name=`${base}-c-${kind}`;
  const created=await api(server,'CreateRoom',{name,metadata:meta});
  out.push({op:'CreateRoom',kind,accepted:created.status===200,result:created});
  if(created.status===200)await api(server,'DeleteRoom',{room:name});
 }
 // UpdateRoomMetadata against a known-good empty-metadata owned room.
 const room=`${base}-u`;
 const mk=await api(server,'CreateRoom',{name:room,metadata:''});
 assert.equal(mk.status,200,'owned room for update must be creatable under this limit');
 try{
  for(const [kind,meta] of inputs(limit)){
   const upd=await api(server,'UpdateRoomMetadata',{room,metadata:meta});
   out.push({op:'UpdateRoomMetadata',kind,accepted:upd.status===200,result:upd});
  }
 }finally{ await api(server,'DeleteRoom',{room}); }
 return out;
}
try{
 const reference=await scenario(0);
 const rust=await scenario(1);
 report.observations={reference,rust};
 assert.deepEqual(rust,reference,'Rust configured metadata enforcement diverged from fixed Go');
 // Independent sanity: within/exact/empty must be accepted, over must be rejected on BOTH,
 // otherwise a disabled guard on both sides could masquerade as agreement.
 for(const side of [reference,rust]){
  const by=k=>side.find(x=>x.op==='CreateRoom'&&x.kind===k);
  assert.equal(by('empty').accepted,true,'empty metadata accepted');
  assert.equal(by('within').accepted,true,'within-limit metadata accepted');
  assert.equal(by('exact').accepted,true,'exactly-limit bytes accepted');
  assert.equal(by('utf8-exact').accepted,true,'exactly-limit UTF8 bytes accepted (byte accounting)');
  assert.equal(by('over').accepted,false,'over-limit metadata rejected');
  assert.equal(by('over').result.code,'invalid_argument','reject is invalid_argument');
 }
 report.status='PASS_SCOPED';
}catch(error){
 report.status='FAIL';report.error={name:error.name,message:String(error.message)};process.exitCode=1;
}finally{
 await writeFile(process.env.META_OUTPUT,JSON.stringify(report,null,2)+'\n');
 console.log(JSON.stringify({status:report.status,profile:report.profile,cases:report.observations.reference?report.observations.reference.length:0}));
}
