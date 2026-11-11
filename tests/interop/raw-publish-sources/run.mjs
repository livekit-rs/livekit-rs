// Coordinator-run only. Real ServerSDK HTTP + authenticated binary WS; NO SDP/RTC claim.
import assert from 'node:assert/strict';
import {createRequire} from 'node:module';
import {readFile,writeFile,realpath} from 'node:fs/promises';
import {dirname,resolve,basename} from 'node:path';
import {randomUUID,createHash,createHmac,timingSafeEqual} from 'node:crypto';
import {execFile} from 'node:child_process';
import {promisify} from 'node:util';
const exec=promisify(execFile),hash=b=>createHash('sha256').update(b).digest('hex');
const env=process.env,required=['INTEROP_PACKAGE','REFERENCE_URL','LIVEKIT_URL','REFERENCE_PID','RUST_PID','REFERENCE_BIN','RUST_BIN','REFERENCE_CONFIG','RUST_CONFIG','RESULT'];
for(const k of required)assert(env[k],`required ${k}`);
const loader=createRequire(resolve(env.INTEROP_PACKAGE));
async function identity(name,version){const entry=await realpath(loader.resolve(name));let dir=dirname(entry);for(;;){try{const p=resolve(dir,'package.json'),b=await readFile(p),j=JSON.parse(b);if(j.name===name){assert.equal(j.version,version);return {entry,entry_sha256:hash(await readFile(entry)),package_sha256:hash(b),version:j.version};}}catch(e){if(e.code!=='ENOENT')throw e;}const parent=dirname(dir);assert.notEqual(parent,dir);dir=parent;}}
const sdk=await identity('livekit-server-sdk','2.19.2'),protocol=await identity('@livekit/protocol','1.52.1');
const {AccessToken,RoomServiceClient}=loader(sdk.entry),{SignalRequest,SignalResponse}=loader(protocol.entry);
const key=env.LIVEKIT_API_KEY||'devkey',secret=env.LIVEKIT_API_SECRET||'secret',TIMEOUT=10000;
const report={status:'RUNNING',scope:'unchanged official ServerSDK HTTP with independently HS256-signed legal protocol source vectors, actual WS refresh/reservation/resume; not SDK raw token mint or RTC/media',sdk,protocol,lock_sha256:hash(await readFile(resolve(dirname(env.INTEROP_PACKAGE),'package-lock.json'))),campaigns:[],cleanup_errors:[]};
let stage='setup';async function save(){await writeFile(env.RESULT,JSON.stringify(report,null,2)+'\n',{mode:0o600});}
async function processProof(pidText,binary,config){
 const pid=Number(pidText);assert(Number.isSafeInteger(pid)&&pid>1);process.kill(pid,0);const expected=await realpath(binary),configPath=await realpath(config);let observed,argv;
 try{observed=await realpath(`/proc/${pid}/exe`);assert.equal(observed,expected);argv=(await readFile(`/proc/${pid}/cmdline`)).toString('utf8').split('\0');}
 catch(e){if(e.code!=='ENOENT')throw e;const {stdout}=await exec('/bin/ps',['-p',String(pid),'-o','comm='],{timeout:TIMEOUT,maxBuffer:65536});const value=stdout.trim();assert(value,'actual server PID missing');if(value.startsWith('/'))assert.equal(await realpath(value),expected);else assert.equal(value,basename(expected));observed=value;argv=(await exec('/bin/ps',['-p',String(pid),'-o','command='],{timeout:TIMEOUT,maxBuffer:65536})).stdout.trim().split(/\s+/);}
 // Read actual startup argv privately: never persist it or possible credentials.
 assert(argv.some((arg,i)=>arg==='--config'&&argv[i+1]===configPath||arg==='--config='+configPath),'actual owned process must use this config file');
 const bytes=await readFile(configPath),lines=bytes.toString('utf8').split(/\r?\n/),apiStarts=lines.flatMap((line,i)=>line==='api:'?[i]:[]);assert.equal(apiStarts.length,1,'dedicated canonical api section required');
 let section=[];for(let i=apiStarts[0]+1;i<lines.length;i++){const line=lines[i];if(line&&!line.startsWith(' ')&&!line.startsWith('#'))break;if(line.trim()&&!line.trim().startsWith('#'))section.push(line);}
 assert.deepEqual(section,['  enable_psrpc_for_get_list_participants: true'],'actual profile requires owner get, no stale cache acceptance');
 return {pid_alive:true,pid,executable_observed:observed,binary_sha256:hash(await readFile(expected)),actual_startup_config_sha256:hash(bytes),owner_get_profile:true};
}
function bounded(p,label){let timer;return Promise.race([p,new Promise((_,reject)=>timer=setTimeout(()=>reject(Error(label+' timeout')),TIMEOUT))]).finally(()=>clearTimeout(timer));}
function claims(token){const a=token.split('.');assert.equal(a.length,3);assert.equal(JSON.parse(Buffer.from(a[0],'base64url')).alg,'HS256');const actual=Buffer.from(a[2],'base64url'),expected=createHmac('sha256',secret).update(a[0]+'.'+a[1]).digest();assert.equal(actual.length,expected.length);assert(timingSafeEqual(actual,expected),'actual refreshed token signature');return JSON.parse(Buffer.from(a[1],'base64url'));}
class Peer{
 constructor(url,token,identity,room,resumeSid){this.resumeSid=resumeSid;this.identity=identity;this.room=room;const initial=claims(token);assert.equal(initial.sub,identity);assert.equal(initial.video.room,room);this.video=initial.video;this.allowedAttributes=[initial.attributes??{}];this.events=[];this.waiters=new Set();this.error=undefined;this.closing=false;this.count=0;
  const u=new URL('/rtc',url);u.protocol=u.protocol.replace('http','ws');u.searchParams.set('access_token',token);u.searchParams.set('protocol','17');u.searchParams.set('sdk','unknown');u.searchParams.set('auto_subscribe','0');if(resumeSid){u.searchParams.set('reconnect','1');u.searchParams.set('sid',resumeSid);}
  this.ws=new WebSocket(u);this.ws.binaryType='arraybuffer';this.closed=new Promise(done=>this.ws.addEventListener('close',event=>{if(!this.closing)this.fail(Error('unexpected WS close'));done(event.code);}));
  this.ws.addEventListener('error',()=>this.fail(Error('actual WS error')));
  this.ws.addEventListener('message',event=>{try{assert(event.data instanceof ArrayBuffer);assert(++this.count<=2048);const m=SignalResponse.fromBinary(new Uint8Array(event.data)).message;if(this.resumeSid&&m.case==='join')throw Error('resume must not create new Join');let value=m.value;if(m.case==='refreshToken'){value=claims(m.value);assert.equal(value.sub,this.identity,'every refreshed subject');assert.equal(value.video.room,this.room,'every refreshed room');assert.deepEqual(value.video,this.video,'every refreshed full original video grant');assert(this.allowedAttributes.some(attrs=>{try{assert.deepEqual(value.attributes??{},attrs);return true;}catch{return false;}}),'every refreshed full attributes');}const record={type:m.case,value};this.events.push(record);if(m.case==='leave')throw Error('actual server Leave');for(const waiter of this.waiters)waiter();}catch(e){this.fail(e);}});
 }
 fail(e){this.error??=e;for(const waiter of this.waiters)waiter();}
 async wait(predicate,label){const read=()=>{if(this.error)throw this.error;return this.events.find(predicate);};let value=read();if(value)return value;let wake;try{return await bounded(new Promise((done,reject)=>{wake=()=>{try{const x=read();if(x)done(x);}catch(e){reject(e);}};this.waiters.add(wake);wake();}),label);}finally{this.waiters.delete(wake);}}
 send(json){assert.equal(this.ws.readyState,WebSocket.OPEN);this.ws.send(SignalRequest.fromJson(json).toBinary());}
 async liveness(){const n=String(Date.now());const start=this.events.length;this.send({pingReq:{timestamp:n}});await this.wait(e=>this.events.indexOf(e)>=start&&e.type==='pongResp'&&String(e.value.lastPingTimestamp)===n,'actual PingReq/PongResp response');}
 async join(){if(this.resumeSid){await this.wait(e=>e.type==='reconnect','actual Reconnect');assert.equal(this.events[0].type,'reconnect','original owner Reconnect must be first');assert.equal(this.events.filter(e=>e.type==='join').length,0);this.sid=this.resumeSid;await this.liveness();return;}const join=await this.wait(e=>e.type==='join','actual Join');assert.equal(join.value.participant.identity,this.identity);assert(join.value.participant.sid);this.sid=join.value.participant.sid;await this.liveness();}
 async close(){this.closing=true;if(this.ws.readyState!==WebSocket.CLOSED)this.ws.close(1000,'fixture complete');return bounded(this.closed,'actual owned WS close');}
}
function refreshCount(peer){return peer.events.filter(e=>e.type==='refreshToken').length;}
async function ownerParticipant(api,room,identity,predicate){const deadline=Date.now()+TIMEOUT;for(;;){try{const value=await api.getParticipant(room,identity);if(predicate(value)){assert(Date.now()<=deadline,'actual owner view deadline');return value;}}catch(e){if(e.code!=='not_found')throw e;}assert(Date.now()<deadline,'actual owner view did not converge');await new Promise(done=>setTimeout(done,10));}}

const casesPath=resolve(dirname(new URL(import.meta.url).pathname),'cases.json');
const actualPath=resolve(dirname(new URL(import.meta.url).pathname),'fixed-go16-actual.jsonl');
const caseBytes=await readFile(casesPath),actualBytes=await readFile(actualPath),cases=JSON.parse(caseBytes),sourceRows=actualBytes.toString('utf8').split('\n').filter(Boolean).map(line=>JSON.parse(line));
assert.equal(cases.length,16);assert.equal(sourceRows.length,16);assert.equal(new Set(cases.map(c=>c.id)).size,16);assert.equal(new Set(sourceRows.map(c=>c.id)).size,16);assert.deepEqual(sourceRows.map(c=>c.id),cases.map(c=>c.id));
report.source_cases_sha256=hash(caseBytes);report.actual_fixed_go16_sha256=hash(actualBytes);
function expectedVideo(actual,row,room,changed){const base=structuredClone(changed?actual.changed_refresh_video:actual.refresh_video);assert.equal(base.room,'owned');base.room=room;base.canSubscribe=false;base.canPublishData=false;base.canUpdateOwnMetadata=true;assert.equal(base.roomJoin,true);return base;}
function safeVideo(video,room){assert.equal(video.room,room);return {...video,room:'<actual-owned-room>'};}
function independentProtocolToken(baselineToken,rawSources){
 const parts=baselineToken.split('.');assert.equal(parts.length,3);const baseline=claims(baselineToken); // true original SDK signature already verified
 const header=JSON.parse(Buffer.from(parts[0],'base64url'));assert.deepEqual(header,{alg:'HS256'},'actual SDK2.19.2 protected header uses alg only');
 assert.equal(Object.hasOwn(baseline.video,'canPublishSources'),false,'SDK canonical baseline must not receive raw source vector');
 const payload=structuredClone(baseline);payload.video.canPublishSources=structuredClone(rawSources);
 const encodedHeader=parts[0],encodedPayload=Buffer.from(JSON.stringify(payload)).toString('base64url');
 const signed=encodedHeader+'.'+encodedPayload,token=signed+'.'+createHmac('sha256',secret).update(signed).digest('base64url');
 const verified=claims(token);assert.deepEqual(verified,payload);assert.equal(verified.sub,baseline.sub);assert.equal(verified.video.room,baseline.video.room);
 assert.equal(verified.exp,baseline.exp);assert.equal(verified.nbf,baseline.nbf);return token;
}

async function reserveFive(peer,actual,phase,raw){const results=[];const allowed=phase==='before'?actual.admission_before:actual.admission_after;assert.equal(allowed.length,5);
 for(let source=0;source<5;source++){
  const cid=`raw-source-${phase}-${source}`,start=peer.events.length;peer.send({addTrack:{cid,name:'owned-source-reservation',type:source===1||source===3?'VIDEO':'AUDIO',source:['UNKNOWN','CAMERA','MICROPHONE','SCREEN_SHARE','SCREEN_SHARE_AUDIO'][source]}});
  // Both sides require an explicit real source-shaped response, not silence or
  // frontend Pong; no negative absence timing argument is used.
  const answer=await peer.wait(event=>peer.events.indexOf(event)>=start&&(event.type==='trackPublished'&&event.value.cid===cid||event.type==='requestResponse'&&event.value.request?.case==='addTrack'&&event.value.request.value.cid===cid),'actual AddTrack correlated response');
  if(allowed[source]){assert.equal(answer.type,'trackPublished');assert(answer.value.track?.sid);assert.equal(answer.value.track.source,source);results.push({source,result:'track-reservation-accepted'});}
  else{assert.equal(answer.type,'requestResponse');assert.equal(answer.value.reason,2);results.push({source,result:'NOT_ALLOWED'});}
 }
 return results;
}
async function run(side,url,pid,binary,config){
 const api=new RoomServiceClient(url.replace(/^ws/,'http'),key,secret,{requestTimeout:10,failover:false});const row={side,process:await processProof(pid,binary,config),cases:[],cleanup_errors:[]};report.campaigns.push(row);let owned=false;const peers=[];const room='raw-source-'+randomUUID();
 try{
  const made=await api.createRoom({name:room});owned=true;assert.equal(made.name,room);const roomSid=made.sid;
  for(let index=0;index<cases.length;index++){
   const raw=cases[index],actual=sourceRows[index];assert.equal(actual.id,raw.id);assert.equal(actual.same_matches,true);assert.equal(actual.changed_matches,false);
   const identity='source-'+randomUUID(),mint=new AccessToken(key,secret,{identity,ttl:'2m'}),grant={roomJoin:true,room,canSubscribe:false,canPublishData:false,canUpdateOwnMetadata:true};if(raw.can_publish!==null)grant.canPublish=raw.can_publish;mint.addGrant(grant);
   const baselineToken=await mint.toJwt();claims(baselineToken);const token=independentProtocolToken(baselineToken,raw.sources),initial=claims(token);assert.deepEqual(initial.video.canPublishSources,raw.sources,'independent legal protocol vector retains exact array/null');
   let peer=new Peer(url,token,identity,room);peer.video=expectedVideo(actual,raw,room,false);peers.push(peer);await peer.join();
   let owner=await ownerParticipant(api,room,identity,p=>p.sid===peer.sid);assert.equal(owner.sid,peer.sid);assert.deepEqual(owner.permission.canPublishSources,actual.permission_sources,'actual owner API ordered enum projection incl duplicates/unknown');assert.equal(owner.permission.canUpdateMetadata,true);assert.equal(owner.permission.canSubscribe,false);assert.equal(owner.permission.canPublishData,false);assert.equal(owner.permission.canPublish,raw.can_publish===null?true:raw.can_publish);
   const observed={id:raw.id,input_signer:'standard HS256 protocol vector derived from verified unchanged SDK canonical baseline',baseline_sdk_signature_verified:true,independent_vector_signature_verified:true,raw_sources:raw.sources,owner_permission_sources:[...owner.permission.canPublishSources],steps:[]};
   // Metadata + equivalent public permission: real SetPermission must preserve
   // original raw source strings when the ordered projected permission matches.
   let start=peer.events.length,count=refreshCount(peer),previous=owner;
   owner=await api.updateParticipant(room,identity,{metadata:'raw-admin-'+raw.id,permission:previous.permission});
   assert.equal(owner.sid,peer.sid);assert.equal(owner.version,(previous.version+1)>>>0);assert.deepEqual(owner.permission.canPublishSources,actual.permission_sources);await peer.wait(e=>peer.events.indexOf(e)>=start&&e.type==='refreshToken','actual admin equal-permission raw refresh');await peer.liveness();assert.equal(refreshCount(peer)-count,1);const adminToken=peer.events.slice(start).filter(e=>e.type==='refreshToken').at(-1);assert.deepEqual(adminToken.value.video,peer.video);
   observed.steps.push({entry:'admin-equal-permission',version_delta:owner.version-previous.version,claims:refreshCount(peer)-count,video:safeVideo(adminToken.value.video,room)});
   previous=await ownerParticipant(api,room,identity,p=>p.version===owner.version);start=peer.events.length;count=refreshCount(peer);const requestId=index+101;peer.send({updateMetadata:{metadata:'raw-self-'+raw.id,requestId}});
   const self=await peer.wait(e=>peer.events.indexOf(e)>=start&&e.type==='requestResponse'&&e.value.requestId===requestId,'actual self metadata response');assert.equal(self.value.reason,0);await peer.liveness();owner=await ownerParticipant(api,room,identity,p=>p.version===((previous.version+1)>>>0));assert.equal(owner.metadata,'raw-self-'+raw.id);assert.deepEqual(owner.permission.canPublishSources,actual.permission_sources);assert.equal(refreshCount(peer)-count,1);const selfToken=peer.events.slice(start).filter(e=>e.type==='refreshToken').at(-1);assert.deepEqual(selfToken.value.video,peer.video);
   observed.steps.push({entry:'self',version_delta:owner.version-previous.version,claims:refreshCount(peer)-count,video:safeVideo(selfToken.value.video,room)});
   observed.admission_before=await reserveFive(peer,actual,'before',raw);
   // Real differing SetPermission canonicalizes raw strings exactly as original
   // source UpdateFromPermission. Reversal and toggled field match fixed Go16.
   const changedPermission={...owner.permission,canPublishSources:[...owner.permission.canPublishSources].reverse(),canManageAgentSession:!owner.permission.canManageAgentSession};assert.equal(changedPermission.canManageAgentSession,true);
   peer.video=expectedVideo(actual,raw,room,true);start=peer.events.length;count=refreshCount(peer);
   owner=await api.updateParticipant(room,identity,{permission:changedPermission});assert.equal(owner.sid,peer.sid);assert.deepEqual(owner.permission.canPublishSources,changedPermission.canPublishSources);assert.equal(owner.permission.canManageAgentSession,true);await peer.wait(e=>peer.events.indexOf(e)>=start&&e.type==='refreshToken','actual differing-permission canonical refresh');await peer.liveness();assert.equal(refreshCount(peer)-count,1);const changedToken=peer.events.slice(start).filter(e=>e.type==='refreshToken').at(-1);assert.deepEqual(changedToken.value.video,peer.video);
   observed.steps.push({entry:'admin-differing-permission',claims:refreshCount(peer)-count,video:safeVideo(changedToken.value.video,room)});
   observed.admission_after=await reserveFive(peer,actual,'after',raw);
   // Real source ResumeParticipant swaps the existing signal sink and retains
   // existing owner grants. Use the ORIGINAL older SDK-minted JWT, not latest
   // refresh token, while the current owner has genuinely changed permission.
   const original=peer;original.closing=true;
   const resumed=new Peer(url,token,identity,room,original.sid);resumed.video=expectedVideo(actual,raw,room,true);peers.push(resumed);await resumed.join();await bounded(original.closed,'actual old signal sink closed on resume');peers.splice(peers.indexOf(original),1);peer=resumed;
   assert.equal(original.events.filter(e=>e.type==='join').length,1);assert.equal(resumed.events.filter(e=>e.type==='reconnect').length,1);assert.equal(resumed.events.filter(e=>e.type==='join').length,0);
   const resumeBefore=await ownerParticipant(api,room,identity,p=>p.sid===original.sid);assert.equal(resumeBefore.sid,original.sid);assert.deepEqual(resumeBefore.permission.canPublishSources,changedPermission.canPublishSources);assert.equal(resumeBefore.permission.canManageAgentSession,true,'old JWT cannot replace current owner grants');
   start=peer.events.length;count=refreshCount(peer);const resumeRequestId=index+1001;peer.send({updateMetadata:{metadata:'raw-resumed-'+raw.id,requestId:resumeRequestId}});const resumeResponse=await peer.wait(e=>peer.events.indexOf(e)>=start&&e.type==='requestResponse'&&e.value.requestId===resumeRequestId,'actual old-JWT-resume self response');assert.equal(resumeResponse.value.reason,0);await peer.liveness();const resumeAfter=await ownerParticipant(api,room,identity,p=>p.version===((resumeBefore.version+1)>>>0));assert.equal(resumeAfter.sid,original.sid);assert.equal(resumeAfter.metadata,'raw-resumed-'+raw.id);assert.deepEqual(resumeAfter.permission.canPublishSources,changedPermission.canPublishSources);assert.equal(refreshCount(peer)-count,1);const resumedToken=peer.events.slice(start).filter(e=>e.type==='refreshToken').at(-1);assert.deepEqual(resumedToken.value.video,peer.video);
   observed.steps.push({entry:'old-original-JWT-actual-resume',same_sid:true,new_join_frames:0,reconnect_frames:1,old_sink_closed:true,version_delta:resumeAfter.version-resumeBefore.version,claims:refreshCount(peer)-count,video:safeVideo(resumedToken.value.video,room)});
   row.cases.push(observed);await save();
   await peer.close();peers.pop();assert.equal((await api.listRooms([room]))[0].sid,roomSid);
  }
  assert.equal(row.cases.length,16);assert.deepEqual(row.cases.map(c=>c.id),cases.map(c=>c.id));row.status='PASS';
 }finally{
  for(const peer of peers)try{await peer.close();}catch{row.cleanup_errors.push('owned raw source WS close failed');}
  if(owned)try{await api.deleteRoom(room);}catch{row.cleanup_errors.push('owned raw source room delete failed');}
  report.cleanup_errors.push(...row.cleanup_errors);await save();
 }
}
try{await writeFile(env.RESULT,'',{flag:'wx',mode:0o600});await save();stage='official';await run('reference',env.REFERENCE_URL,env.REFERENCE_PID,env.REFERENCE_BIN,env.REFERENCE_CONFIG);stage='rust';await run('rust',env.LIVEKIT_URL,env.RUST_PID,env.RUST_BIN,env.RUST_CONFIG);assert.deepEqual(report.campaigns[1].cases,report.campaigns[0].cases,'all actual public owner/raw refresh/reservation business observations');assert.equal(report.cleanup_errors.length,0);report.status='PASS_SCOPED';}
catch(error){report.status='FAIL';report.failure={stage,type:error?.constructor?.name??'Error',code:error?.code,source_positions:[...String(error?.stack??'').matchAll(/run\.mjs:(\d+):(\d+)/g)].slice(0,4).map(m=>({line:Number(m[1]),column:Number(m[2])}))};process.stderr.write(JSON.stringify(report.failure)+'\n');process.exitCode=1;}
finally{await save();}
