/** Real paired max_participants join-gate via the GENERATED integer CLI FLAG
 * `--room.max_participants`: authenticated raw WebSocket signaling join against a
 * prestarted fixed-Go reference and Rust server launched with the same base YAML plus the
 * same extra argv. Holds peer A joined so the room is genuinely at the flag-configured
 * limit, then attempts peer B and captures the observable join outcome + room membership.
 * No SDK mutation, no browser, no media/RTP, no fake RPC. Requires Rust to byte-equal the
 * fixed Go reference. This is the same unchanged consumer as room-max-participants (YAML
 * value path); only the launcher sources the value from the CLI flag argv. */
import assert from 'node:assert/strict';
import {createHmac} from 'node:crypto';
import {writeFile} from 'node:fs/promises';
import {readResponse} from '../signed-name-limits/raw-protocol.mjs';
const wsUrls=[process.env.REFERENCE_WS_URL,process.env.LIVEKIT_WS_URL];
assert.ok(wsUrls.every(Boolean),'both actual reference and Rust WS URLs required');
const httpUrls=wsUrls.map(u=>u.replace(/^ws/,'http').replace(/\/$/,''));
const key=process.env.LIVEKIT_API_KEY||'devkey',secret=process.env.LIVEKIT_API_SECRET||'secret';
const profile=process.env.FMP_PROFILE;
assert.ok(profile,'FMP_PROFILE required');
const report={status:'RUNNING',profile,scope:'Generated integer CLI flag --room.max_participants applied on the real authenticated WS signaling join gate (hold A, attempt B over the flag-configured limit) + ListParticipants room state; not the YAML value path, /rtc/validate, media, RTP or webhook.',observations:[]};
function token(identity,video){
 const e=v=>Buffer.from(JSON.stringify(v)).toString('base64url');
 const v=`${e({alg:'HS256',typ:'JWT'})}.${e({iss:key,sub:identity,exp:Math.floor(Date.now()/1000)+3600,video})}`;
 return v+'.'+createHmac('sha256',secret).update(v).digest('base64url');
}
const admin=(room)=>token('admin',{room,roomCreate:true,roomAdmin:true,roomList:true});
async function api(server,method,body){const r=await fetch(`${httpUrls[server]}/twirp/livekit.RoomService/${method}`,{method:'POST',headers:{authorization:`Bearer ${admin(body.room||body.name)}`,'content-type':'application/json'},body:JSON.stringify(body),signal:AbortSignal.timeout(10000)});const t=await r.text();let v;try{v=JSON.parse(t);}catch{v=t;}return{status:r.status,value:v};}
// Open a peer on the real /rtc WS signaling endpoint; resolves when it either
// receives a join response or the connection terminates; socket stays open until close().
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
  }catch{/* keep reading; parse of unrelated frames does not affect the gate */}});
  sock.addEventListener('error',()=>{h.transport_error=true;finish();});
  sock.addEventListener('close',ev=>{h.close_clean=ev.wasClean;h.close_code=ev.code;finish();});
  h._close=()=>{try{sock.close();}catch{}};
 });
}
async function scenario(server){
 const room=`fmp-${profile}-${server===0?'ref':'rs'}-${process.pid}`;
 const made=await api(server,'CreateRoom',{name:room,empty_timeout:60});
 assert.equal(made.status,200,'admin CreateRoom must succeed');
 const out=[];
 // Hold peer A joined so the room is genuinely occupied.
 const a=await openPeer(server,room,'peer-a');
 a.case='peer-a-hold';out.push(a);
 assert.equal(a.joined,true,'peer A must join under either profile');
 // Attempt peer B while A is present: this is the --room.max_participants gate.
 const b=await openPeer(server,room,'peer-b');
 b.case='peer-b-attempt';out.push(b);
 // Room membership after the over-limit attempt (fixed identities -> deterministic).
 const lp=await api(server,'ListParticipants',{room});
 assert.equal(lp.status,200);
 const ids=(lp.value.participants||[]).map(p=>p.identity).sort();
 out.push({case:'room-membership',identities:ids,b_present:ids.includes('peer-b')});
 a._close();b._close();
 await api(server,'DeleteRoom',{room});
 // Normalized comparable view (drops per-server room name).
 return {peer_a:{joined:a.joined,got_join:a.got_join,close_clean:a.close_clean},peer_b:{joined:b.joined,got_join:b.got_join,got_leave:b.got_leave,transport_error:b.transport_error,close_clean:b.close_clean,close_code:b.close_code},membership:ids.map(x=>x)};
}
try{
 const reference=await scenario(0);
 const rust=await scenario(1);
 report.observations={reference,rust};
 assert.deepEqual(rust,reference,'Rust --room.max_participants CLI-flag join gate diverged from fixed Go');
 report.peer_b={reference:reference.peer_b,rust:rust.peer_b};
 report.membership={reference:reference.membership,rust:rust.membership};
 report.status='PASS_SCOPED';
}catch(error){
 report.status='FAIL';report.error={name:error.name,message:String(error.message).replace(/eyJ[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+/g,'<token>')};process.exitCode=1;
}finally{
 await writeFile(process.env.FMP_OUTPUT,JSON.stringify(report,null,2)+'\n');
 console.log(JSON.stringify({status:report.status,profile,peer_b_joined:{reference:report.observations?.reference?.peer_b?.joined,rust:report.observations?.rust?.peer_b?.joined}}));
}
