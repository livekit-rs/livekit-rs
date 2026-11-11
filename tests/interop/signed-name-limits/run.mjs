/** Real prestarted fixed-Go/Rust HTTP+authenticated SignalRequest profile.
 * No SDK mutation, listener launcher, browser, fake RPC or media claim.
 */
import assert from 'node:assert/strict';
import {createHmac,randomUUID} from 'node:crypto';
import {writeFile} from 'node:fs/promises';
import {readResponse} from './raw-protocol.mjs';
import {SignalRequest} from '@livekit/protocol';
const profiles={
 'room-negative':[-1,256,256],'room-zero':[0,256,256],'room-three-utf8':[3,256,256],
 'name-negative':[256,256,-1],'name-zero':[256,256,0],'name-three-utf8':[256,256,3],
 'identity-negative':[256,-1,256],'identity-zero':[256,0,256],'identity-three-utf8':[256,3,256],
};
const profile=process.env.SIGNED_LIMIT_PROFILE,limits=profiles[profile];assert.ok(limits,'declare exact prestarted YAML profile');
const url=process.env.LIVEKIT_URL||'ws://127.0.0.1:7880',http=url.replace(/^ws/,'http').replace(/\/$/,'');
const key=process.env.LIVEKIT_API_KEY||'devkey',secret=process.env.LIVEKIT_API_SECRET||'secret';
const report={status:'RUNNING',profile,limits:{room:limits[0],identity:limits[1],name:limits[2]},scope:'Actual prestarted HTTP API/validate/WHIP preflight and authenticated metadata SignalRequest with participant snapshots. No RTC/media or unchanged Room SDK API claim.',observations:[],cleanupErrors:[]};
const ownedRooms=new Set();let peer;
const sleep=ms=>new Promise(r=>setTimeout(r,ms));
function token(identity,video,name=''){
 const encode=v=>Buffer.from(JSON.stringify(v)).toString('base64url');
 const value=`${encode({alg:'HS256',typ:'JWT'})}.${encode({iss:key,sub:identity,name,exp:Math.floor(Date.now()/1000)+3600,video})}`;
 return value+'.'+createHmac('sha256',secret).update(value).digest('base64url');
}
const admin=room=>token('admin',{room,roomCreate:true,roomAdmin:true,roomList:true});
async function api(method,body,auth=admin(body.room||body.name||'')){
 const response=await fetch(`${http}/twirp/livekit.RoomService/${method}`,{method:'POST',headers:{authorization:`Bearer ${auth}`,'content-type':'application/json'},body:JSON.stringify(body),signal:AbortSignal.timeout(10000)});
 const text=await response.text();let value;try{value=JSON.parse(text);}catch{value=text;}
 return {status:response.status,type:response.headers.get('content-type'),value};
}
async function absent(name){const r=await api('ListRooms',{names:[name]},admin(name));assert.equal(r.status,200);assert.deepEqual(r.value.rooms||[],[]);}
async function createOwned(name){await absent(name);const result=await api('CreateRoom',{name});if(result.status===200)ownedRooms.add(name);return result;}
async function validate(identity,room,name=''){
 const endpoint=new URL(http+'/rtc/validate');endpoint.searchParams.set('access_token',token(identity,{roomJoin:true,room,canPublish:false,canSubscribe:false},name));endpoint.searchParams.set('protocol','17');
 const r=await fetch(endpoint,{signal:AbortSignal.timeout(10000)});return {status:r.status,body:await r.text()};
}
async function whipPreflight(identity,room){const r=await fetch(http+'/whip',{method:'POST',headers:{authorization:'Bearer '+token(identity,{roomJoin:true,room,canPublish:false,canSubscribe:false}),'content-type':'application/sdp'},body:'invalid SDP after authentication',signal:AbortSignal.timeout(10000)});return{status:r.status,body:await r.text()};}
class Peer{
 constructor(room,identity,canUpdate){
  this.pending=new Map();this.count=0;this.room=room;this.identity=identity;
  const endpoint=new URL(url);endpoint.pathname='/rtc';endpoint.search='';endpoint.searchParams.set('access_token',token(identity,{roomJoin:true,room,canPublish:false,canSubscribe:false,canUpdateOwnMetadata:canUpdate},'original'));endpoint.searchParams.set('protocol','17');endpoint.searchParams.set('sdk','js');
  this.socket=new WebSocket(endpoint);this.socket.binaryType='arraybuffer';
  this.ready=new Promise((resolve,reject)=>{this.joinResolve=resolve;this.joinReject=reject;});
  this.closed=new Promise(resolve=>this.closeResolve=resolve);
  this.joinTimer=setTimeout(()=>this.fail(new Error('actual Join timeout')),10000);
  this.socket.addEventListener('message',event=>{try{
   assert.ok(event.data instanceof ArrayBuffer);assert.ok(++this.count<=2048);const response=readResponse(Buffer.from(event.data));
   if(response.type==='join'){assert.equal(response.tree.values.participant.values.identity,identity);clearTimeout(this.joinTimer);this.joinResolve();}
   else if(response.type==='requestResponse'){const pending=this.pending.get(response.requestId);assert.ok(pending,'correlated actual metadata response');this.pending.delete(response.requestId);clearTimeout(pending.timer);pending.resolve({reason:response.tree.values.reason||0,message:response.tree.values.message||''});}
  }catch(error){this.fail(error);}});
  this.socket.addEventListener('error',()=>this.fail(new Error('actual SignalRequest transport error')));
  this.socket.addEventListener('close',event=>{(report.transportClosures??=[]).push({identity:this.identity,closing:!!this.closing,code:event.code,clean:event.wasClean,reason:event.reason});this.closeResolve(event);if(!this.closing)this.fail(new Error('unexpected actual signal close'));});
 }
 fail(error){this.failure=error;clearTimeout(this.joinTimer);this.joinReject(error);for(const p of this.pending.values()){clearTimeout(p.timer);p.reject(error);}this.pending.clear();}
 async update(requestId,name,metadata){if(this.failure)throw this.failure;const response=new Promise((resolve,reject)=>{const timer=setTimeout(()=>{this.pending.delete(requestId);reject(new Error('actual metadata RequestResponse timeout'));},10000);this.pending.set(requestId,{resolve,reject,timer});});this.socket.send(SignalRequest.fromJson({updateMetadata:{requestId,name,metadata}}).toBinary());return response;}
 async close(){
  clearTimeout(this.joinTimer);this.closing=true;
  // This profile tests metadata, not Leave. Initiate the normal peer close
  // handshake, then delete the owned room through the real API. Fixed Go may
  // close TCP without a WS close frame after Leave; waiting for a clean
  // server-initiated Leave close would impose a contract it does not provide.
  if(this.socket.readyState!==WebSocket.CLOSED)this.socket.close(1000);
  let timer;try{const event=await Promise.race([this.closed,new Promise((_,reject)=>{timer=setTimeout(()=>reject(new Error('owned signal cleanup timeout')),10000);})]);assert.equal(event.code,1000);assert.equal(event.wasClean,true);}finally{clearTimeout(timer);}
  if(this.failure)throw this.failure;
 }
}
function nameAllowed(limit,value){return limit===0||(limit>0&&Buffer.byteLength(value)<=limit);}
async function snapshot(room,identity){const r=await api('GetParticipant',{room,identity});assert.equal(r.status,200);return{name:r.value.name||'',metadata:r.value.metadata||''};}
try{
 if(profile.startsWith('room-')){
  for(const name of ['', 'a','éa','éé']){
   const r=await createOwned(name),accepted=nameAllowed(limits[0],name);
   assert.equal(r.status===200,accepted);report.observations.push({case:`CreateRoom-${Buffer.from(name).toString('hex')}`,status:r.status,error:accepted?undefined:r.value});
   if(!accepted){
    // Actual fixed service returns fmt.Errorf, not a typed InvalidArgument.
    // Check envelope as well as the signed predicate and no owner mutation.
    assert.equal(r.status,400);assert.equal(r.value.code,'invalid_argument');
    assert.equal(r.value.msg,`room name length exceeds limits: max length ${limits[0]}`);
    await absent(name);
   }
  }
  for(const [id,identity,room]of [['empty-identity','', 'a'],['room-only','i','a'],['empty-room','i','']]){
   report.observations.push({case:`rtc-${id}`,...await validate(identity,room)});
   report.observations.push({case:`whip-${id}`,...await whipPreflight(identity,room)});
  }
 }else if(profile.startsWith('identity-')){
  const room='f63-identity-'+randomUUID();assert.equal((await createOwned(room)).status,200);
  for(const [i,identity]of ['', 'a','éa','éé','i'.repeat(300)].entries())report.observations.push({case:`rtc-identity-${i}`,...await validate(identity,room)});
 }else{
  const room='f63-names-'+randomUUID();assert.equal((await createOwned(room)).status,200);
  // A long display name is not checked by source ValidateConnectRequest.
  report.observations.push({case:'connect-display-name-not-a-limit',...await validate('long-display',room,'n'.repeat(300))});
  for(const canUpdate of [false,true]){
   const identity=canUpdate?'permitted':'denied';peer=new Peer(room,identity,canUpdate);await peer.ready;
   let previous=await snapshot(room,identity);
   for(const [i,name]of ['', 'éa','éé','n'.repeat(300)].entries()){
    const requestId=200+i,response=await peer.update(requestId,name,`step-${i}`);
    const accepted=canUpdate&&nameAllowed(limits[2],name);
    assert.equal(response.reason===0,accepted);
    const expected=accepted?{name:name||previous.name,metadata:`step-${i}`}:previous;
    let actual;const until=Date.now()+10000;
    do{actual=await snapshot(room,identity);if(JSON.stringify(actual)===JSON.stringify(expected))break;assert.ok(Date.now()<until,'actual metadata state convergence');await sleep(20);}while(true);
    assert.deepEqual(actual,expected);
    report.observations.push({case:`signal-${canUpdate}-${i}`,response,state:actual});previous=actual;
   }
   await peer.close();peer=undefined;
  }
  // Authenticated but non-admin JWT: source checks even omitted name before grant.
  for(const [i,name]of ['', 'éa','éé'].entries()){
   const result=await api('UpdateParticipant',{room,identity:'missing',name},token('not-admin',{room,roomJoin:true}));
   assert.notEqual(result.status,200);report.observations.push({case:`api-prepermission-${i}`,result});
  }
 }
 report.status='PASS_SCOPED';
}catch(error){report.status='FAIL';report.error={name:error.name,message:String(error.message).replace(/eyJ[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+/g,'<token>')};process.exitCode=1;}
finally{
 if(peer)try{await peer.close();}catch(error){report.cleanupErrors.push(String(error.message));}
 for(const room of ownedRooms)try{const r=await api('DeleteRoom',{room});assert.equal(r.status,200);await absent(room);}catch(error){report.cleanupErrors.push(String(error.message));}
 if(report.cleanupErrors.length){report.status='FAIL';process.exitCode=1;}
 assert.ok(process.env.SIGNED_LIMIT_OUTPUT,'coordinator-owned output required');await writeFile(process.env.SIGNED_LIMIT_OUTPUT,JSON.stringify(report,null,2)+'\n');console.log(JSON.stringify({status:report.status,profile,cases:report.observations.length}));
}
